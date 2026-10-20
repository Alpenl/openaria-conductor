"""Explicit database maintenance. Media files are never modified here."""

from __future__ import annotations

import fcntl
import os
import sqlite3
import stat
import subprocess
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from rp_ylx.persistence import (
    APPLICATION_ID,
    SCHEMA_VERSION,
    PersistenceError,
    StateDatabase,
    _require_regular_database_file,
    _sync_parent,
    validate_state_schema,
)
from rp_ylx.recording.catalog import SessionCatalog


def require_offline() -> None:
    units = [
        "rp-ylx.service",
        "rp-ylx-network-control.service",
        "rp-ylx-recording-button.service",
        "rp-ylx-recover.service",
    ]
    result = subprocess.run(
        ["systemctl", "show", "--property=ActiveState", "--value", *units],
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    )
    states = result.stdout.split()
    if len(states) != len(units) or any(s not in {"inactive", "failed"} for s in states):
        raise PersistenceError(
            "stop capture, network-control, recording-button and recover services first"
        )


def inspect(path: Path) -> dict:
    _require_regular_database_file(path)
    db = sqlite3.connect(path.absolute().as_uri() + "?mode=ro", uri=True)
    try:
        if (
            db.execute("PRAGMA application_id").fetchone()[0] != APPLICATION_ID
            or db.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION
        ):
            raise PersistenceError("unsupported database identity or schema")
        validate_state_schema(db)
        checks = [row[0] for row in db.execute("PRAGMA integrity_check")]
        if checks != ["ok"]:
            raise PersistenceError("database integrity check failed")
        return {
            "integrity": "ok",
            "schema_version": SCHEMA_VERSION,
            "documents": [
                {"name": r[0], "revision": r[1]}
                for r in db.execute("SELECT name,revision FROM documents ORDER BY name")
            ],
        }
    finally:
        db.close()


@contextmanager
def _restore_lock(destination: Path) -> Iterator[None]:
    # Keep this inode after release. Unlinking a lock file permits waiters and
    # new callers to lock different inodes for the same destination.
    lock = destination.with_suffix(destination.suffix + ".restore.lock")
    fd = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise PersistenceError("database restore lock must be a regular file")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise PersistenceError("database restore is busy; retry after it finishes") from None
        yield
    finally:
        os.close(fd)


def restore(source: Path, destination: Path) -> dict:
    # SQLite locks protect transactions, not renames of database and journal
    # files. Serialize maintenance through final inspection and cleanup.
    with _restore_lock(destination):
        return _restore_locked(source, destination)


def _restore_locked(source: Path, destination: Path) -> dict:
    require_offline()
    info = inspect(source)
    required = {
        "state.sqlite3": "capture",
        "controller.sqlite3": "controller",
        "settings.sqlite3": "device",
    }.get(destination.name)
    if required and required not in {d["name"] for d in info["documents"]}:
        raise PersistenceError("backup does not contain the required authoritative document")
    if source.resolve() == destination.resolve():
        raise ValueError("source and destination must differ")
    token = uuid.uuid4().hex
    prepared = destination.with_name(destination.name + ".restore-" + token)
    with StateDatabase.readonly_snapshot(source, prepared):
        return _restore_prepared(prepared, destination, required, token)


def _restore_prepared(prepared: Path, destination: Path, required: str | None, token: str) -> dict:
    archived = []
    # The source can change after its initial inspection. Only the copied
    # snapshot will be published, so admit that exact database before any
    # mutation of the existing authority or its marker.
    prepared_info = inspect(prepared)
    if required and required not in {d["name"] for d in prepared_info["documents"]}:
        raise PersistenceError("backup does not contain the required authoritative document")
    # Preserve even corrupt databases and their journals for diagnosis.
    # Never remove the authority marker: interruption must fail closed.
    marker = destination.with_suffix(destination.suffix + ".initialized")
    if not marker.exists():
        fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as stream:
            stream.write(str(SCHEMA_VERSION))
            stream.flush()
            os.fsync(stream.fileno())
    # Make the authority marker durable before any old file moves. A
    # missing database after interruption must not bootstrap empty state.
    _sync_parent(marker)
    for suffix in ("", "-wal", "-shm"):
        old = Path(str(destination) + suffix)
        if os.path.lexists(old):
            saved = old.with_name(old.name + ".before-restore-" + token)
            old.rename(saved)
            archived.append(str(saved))
    # Old database and journals must be durably named before publication.
    # On failure, retain their archived names and the authority marker.
    _sync_parent(destination)
    prepared.replace(destination)
    # The private snapshot's contents were synced before admission; its new name
    # needs its own barrier. A failure preserves both new and archived data.
    _sync_parent(destination)
    StateDatabase(destination)
    return {"restored": str(destination), "preserved": archived, **inspect(destination)}


def manage(path: Path, action: str, *, source: Path | None, destination: Path | None) -> dict:
    if action == "check":
        return inspect(path)
    if action == "backup":
        if destination is None:
            raise ValueError("--output is required")
        StateDatabase.backup_readonly(path, destination)
        return {"backup": str(destination), **inspect(destination)}
    if action == "restore":
        if source is None:
            raise ValueError("--input is required")
        return restore(source, path)
    if action == "rebuild-catalog":
        require_offline()
        if path.name != "catalog.sqlite3":
            raise ValueError("only catalog.sqlite3 is rebuildable; restore authority from backup")
        temporary = path.with_name("catalog-rebuild-" + uuid.uuid4().hex + ".sqlite3")
        try:
            SessionCatalog(temporary)
            result = restore(temporary, path)
            result["rebuild"] = "session manifests will be indexed on the next list request"
            return result
        finally:
            temporary.unlink(missing_ok=True)
            temporary.with_suffix(".sqlite3.initialized").unlink(missing_ok=True)
            temporary.with_suffix(".sqlite3.init.lock").unlink(missing_ok=True)
    raise ValueError("unknown database operation")
