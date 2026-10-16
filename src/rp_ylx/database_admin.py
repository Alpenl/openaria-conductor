"""Explicit database maintenance. Media files are never modified here."""

from __future__ import annotations

import os
import sqlite3
import subprocess
import uuid
from pathlib import Path

from rp_ylx.persistence import APPLICATION_ID, SCHEMA_VERSION, PersistenceError, StateDatabase
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
    db = sqlite3.connect(path.absolute().as_uri() + "?mode=ro", uri=True)
    try:
        if (
            db.execute("PRAGMA application_id").fetchone()[0] != APPLICATION_ID
            or db.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION
        ):
            raise PersistenceError("unsupported database identity or schema")
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


def restore(source: Path, destination: Path) -> dict:
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
    StateDatabase(source).backup(prepared)
    archived = []
    try:
        # Preserve even corrupt databases and their journals for diagnosis.
        # Never remove the authority marker: interruption must fail closed.
        marker = destination.with_suffix(destination.suffix + ".initialized")
        if not marker.exists():
            fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w") as stream:
                stream.write(str(SCHEMA_VERSION))
                stream.flush()
                os.fsync(stream.fileno())
        for suffix in ("", "-wal", "-shm"):
            old = Path(str(destination) + suffix)
            if os.path.lexists(old):
                saved = old.with_name(old.name + ".before-restore-" + token)
                old.rename(saved)
                archived.append(str(saved))
        prepared.replace(destination)
        StateDatabase(destination)
        return {"restored": str(destination), "preserved": archived, **inspect(destination)}
    finally:
        prepared.unlink(missing_ok=True)


def manage(path: Path, action: str, *, source: Path | None, destination: Path | None) -> dict:
    if action == "check":
        return inspect(path)
    if action == "backup":
        if destination is None:
            raise ValueError("--output is required")
        StateDatabase(path).backup(destination)
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
