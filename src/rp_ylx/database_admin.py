"""Explicit database maintenance. Media files are never modified here."""

from __future__ import annotations

import os
import subprocess
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from rp_ylx.core.fs import O_CLOEXEC, O_NOFOLLOW, chown_to_parent
from rp_ylx.core.locks import acquire_flock
from rp_ylx.persistence import (
    SCHEMA_VERSION,
    PersistenceError,
    StateDatabase,
    readonly_connection,
    sync_parent,
)
from rp_ylx.recording.catalog import SessionCatalog

__all__ = ["inspect", "manage", "require_offline", "restore"]


def require_offline() -> None:
    units = [
        "rp-ylx.service",
        "rp-ylx-network-control.service",
        "rp-ylx-network-control.socket",
        "rp-ylx-recording-button.service",
        "rp-ylx-recover.service",
        "openaria-update.service",
        "rp-ylx-wifi-watchdog.service",
        "rp-ylx-wifi-watchdog.timer",
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
            "stop capture, network-control, recording-button, update, "
            "watchdog and recover services first"
        )


def inspect(path: Path) -> dict:
    with readonly_connection(path) as db:
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


@contextmanager
def _maintenance_restore_lease() -> Iterator[None]:
    lock_path = Path(os.environ.get("OPENARIA_MAINTENANCE_LOCK", "/run/rp-ylx/maintenance.lock"))
    if (
        os.geteuid() != 0
        and "OPENARIA_MAINTENANCE_LOCK" not in os.environ
        and not lock_path.exists()
    ):
        yield
        return
    try:
        with acquire_flock(lock_path, nonblocking=True):
            yield
    except BlockingIOError as error:
        raise PersistenceError(
            "maintenance lock is busy; retry after active operation finishes"
        ) from error


@contextmanager
def _restore_lock(destination: Path) -> Iterator[None]:
    # Keep this inode after release. Unlinking a lock file permits waiters and
    # new callers to lock different inodes for the same destination.
    lock = destination.with_suffix(destination.suffix + ".restore.lock")
    try:
        with acquire_flock(lock, mode=0o600, group_name=None, nonblocking=True):
            yield
    except BlockingIOError:
        raise PersistenceError("database restore is busy; retry after it finishes") from None
    except ValueError as error:
        raise PersistenceError("database restore lock must be a regular file") from error


def restore(source: Path, destination: Path) -> dict:
    # SQLite locks protect transactions, not renames of database and journal
    # files. Serialize maintenance through final inspection and cleanup.
    with _restore_lock(destination), _maintenance_restore_lease():
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
    if not os.path.lexists(marker):
        fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0o600)
        with os.fdopen(fd, "w") as stream:
            stream.write(str(SCHEMA_VERSION))
            stream.flush()
            os.fsync(stream.fileno())
        chown_to_parent(marker)
    # Make the authority marker durable before any old file moves. A
    # missing database after interruption must not bootstrap empty state.
    sync_parent(marker)
    # A hot rollback journal belongs to the old database just like WAL/SHM.
    # Leaving it at the live name can replay old pages into the restored copy.
    for suffix in ("", "-wal", "-shm", "-journal"):
        old = Path(str(destination) + suffix)
        if os.path.lexists(old):
            saved = old.with_name(old.name + ".before-restore-" + token)
            old.rename(saved)
            archived.append(str(saved))
    # Old database and journals must be durably named before publication.
    # On failure, retain their archived names and the authority marker.
    sync_parent(destination)
    prepared.replace(destination)
    # The private snapshot's contents were synced before admission; its new name
    # needs its own barrier. A failure preserves both new and archived data.
    sync_parent(destination)
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
            for suffix in ("", "-wal", "-shm", "-journal"):
                Path(str(temporary) + suffix).unlink(missing_ok=True)
            temporary.with_suffix(".sqlite3.initialized").unlink(missing_ok=True)
            temporary.with_suffix(".sqlite3.init.lock").unlink(missing_ok=True)
    raise ValueError("unknown database operation")
