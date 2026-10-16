"""Device-local SQLite authority; media and recovery journals remain independent files.

Databases use WAL/FULL, bounded lock waits and short transactions. A migration marker
prevents a missing database from silently resurrecting obsolete JSON state. No payload
verification proof is reconstructed from this database.
"""

from __future__ import annotations

import json
import os
import sqlite3
import stat
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
APPLICATION_ID = 0x594C5801


class PersistenceError(RuntimeError):
    """Persistent authority cannot safely be loaded or updated."""


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sync_parent(path: Path) -> None:
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class StateDatabase:
    """Versioned local state with explicit, one-time JSON imports."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.marker = self.path.with_suffix(self.path.suffix + ".initialized")
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if not self.path.exists() and self.marker.exists():
            raise PersistenceError(f"missing authoritative database: {self.path}")
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise PersistenceError("database must be a regular file")
            os.fchmod(fd, 0o600)
            if os.geteuid() == 0:
                parent = self.path.parent.stat()
                os.fchown(fd, parent.st_uid, parent.st_gid)
        finally:
            os.close(fd)
        try:
            with self.connection() as db:
                version = db.execute("PRAGMA user_version").fetchone()[0]
                application = db.execute("PRAGMA application_id").fetchone()[0]
                tables = db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                if self.marker.exists() and version == 0:
                    raise PersistenceError("initialized database has lost its schema")
                if application != APPLICATION_ID and (application or version or tables):
                    raise PersistenceError("not an OpenAria state database")
                if version not in (0, SCHEMA_VERSION):
                    raise PersistenceError(f"unsupported database schema: {version}")
                if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise PersistenceError("state database integrity check failed")
                db.execute("PRAGMA journal_mode=WAL")
                with db:
                    db.execute("BEGIN IMMEDIATE")
                    db.execute(
                        "CREATE TABLE IF NOT EXISTS documents ("
                        "name TEXT PRIMARY KEY, body TEXT NOT NULL, "
                        "revision INTEGER NOT NULL, updated_at TEXT NOT NULL)"
                    )
                    db.execute(
                        "CREATE TABLE IF NOT EXISTS imports ("
                        "name TEXT PRIMARY KEY, source TEXT NOT NULL)"
                    )
                    db.execute(
                        "CREATE TABLE IF NOT EXISTS changes ("
                        "id INTEGER PRIMARY KEY, name TEXT NOT NULL, "
                        "revision INTEGER NOT NULL, body TEXT NOT NULL, "
                        "updated_at TEXT NOT NULL)"
                    )
                    db.execute(
                        "CREATE TABLE IF NOT EXISTS capture_commands ("
                        "principal TEXT NOT NULL, operation TEXT NOT NULL, "
                        "idempotency_key TEXT NOT NULL, body TEXT NOT NULL, "
                        "PRIMARY KEY(principal,operation,idempotency_key))"
                    )
                    db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                    db.execute(f"PRAGMA application_id={APPLICATION_ID}")
            if not self.marker.exists():
                try:
                    fd = os.open(self.marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                except FileExistsError:
                    pass  # Another initializer committed the same schema.
                else:
                    with os.fdopen(fd, "w") as stream:
                        if os.geteuid() == 0:
                            parent = self.path.parent.stat()
                            os.fchown(stream.fileno(), parent.st_uid, parent.st_gid)
                        stream.write(str(SCHEMA_VERSION))
                        stream.flush()
                        os.fsync(stream.fileno())
                _sync_parent(self.marker)
        except (sqlite3.Error, OSError) as error:
            raise PersistenceError(f"cannot open state database {self.path}: {error}") from error

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        # mode=rw prevents a removed authority from being silently recreated.
        db = sqlite3.connect(self.path.absolute().as_uri() + "?mode=rw", uri=True, timeout=5)
        try:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA foreign_keys=ON")
            yield db
        finally:
            db.close()

    def initialize_document(self, name: str, value: Mapping[str, Any], source: str) -> None:
        """Import bootstrap defaults once, atomically with the migration receipt."""
        with self.connection() as db, db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT 1 FROM documents WHERE name=?", (name,)).fetchone():
                return
            if db.execute("SELECT 1 FROM imports WHERE name=?", (name,)).fetchone():
                raise PersistenceError(f"missing authoritative document: {name}")
            body = _json(value)
            now = datetime.now(UTC).isoformat()
            db.execute("INSERT INTO documents VALUES (?,?,1,?)", (name, body, now))
            db.execute("INSERT INTO imports VALUES (?,?)", (name, source))
            db.execute(
                "INSERT INTO changes(name,revision,body,updated_at) VALUES (?,1,?,?)",
                (name, body, now),
            )

    def update(self, name: str, transform) -> None:
        """Serialize validated read/modify/write and its audit record."""
        with self.connection() as db, db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT body,revision FROM documents WHERE name=?", (name,)).fetchone()
            if old is None:
                raise PersistenceError(f"missing authoritative document: {name}")
            body = _json(transform(json.loads(old[0])))
            if body == old[0]:
                return
            revision = old[1] + 1
            now = datetime.now(UTC).isoformat()
            db.execute(
                "UPDATE documents SET body=?,revision=?,updated_at=? WHERE name=?",
                (body, revision, now, name),
            )
            db.execute(
                "INSERT INTO changes(name,revision,body,updated_at) VALUES (?,?,?,?)",
                (name, revision, body, now),
            )

    def get(self, name: str) -> dict[str, Any] | None:
        with self.connection() as db:
            row = db.execute("SELECT body FROM documents WHERE name=?", (name,)).fetchone()
        if row is None:
            return None
        value = json.loads(row[0])
        if not isinstance(value, dict):
            raise PersistenceError(f"invalid state document: {name}")
        return value

    def capture_state(self) -> dict[str, Any]:
        value = self.get("capture") or {}
        if "commands" not in value:
            with self.connection() as db:
                value["commands"] = [
                    json.loads(row[0])
                    for row in db.execute("SELECT body FROM capture_commands ORDER BY rowid")
                ]
        return value

    def put_capture_state(
        self, value: Mapping[str, Any], commands: list[dict], removed: set
    ) -> None:
        """Commit lifecycle and new receipts together; do not rewrite old receipts."""
        body = _json(value)
        now = datetime.now(UTC).isoformat()
        with self.connection() as db, db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT body,revision FROM documents WHERE name='capture'").fetchone()
            if old is None or body != old[0]:
                revision = 1 if old is None else old[1] + 1
                db.execute(
                    "INSERT INTO documents VALUES ('capture',?,?,?) ON CONFLICT(name) "
                    "DO UPDATE SET body=excluded.body,revision=excluded.revision,"
                    "updated_at=excluded.updated_at",
                    (body, revision, now),
                )
            db.executemany(
                "DELETE FROM capture_commands WHERE principal=? AND operation=? "
                "AND idempotency_key=?",
                list(removed),
            )
            db.executemany(
                "INSERT OR REPLACE INTO capture_commands VALUES (?,?,?,?)",
                [
                    (c["principal_id"], c["operation"], c["idempotency_key"], _json(c))
                    for c in commands
                ],
            )

    def put(self, name: str, value: Mapping[str, Any], *, history: bool = False) -> None:
        body = _json(value)
        now = datetime.now(UTC).isoformat()
        with self.connection() as db, db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT body,revision FROM documents WHERE name=?", (name,)).fetchone()
            if old is not None and old[0] == body:
                return
            revision = 1 if old is None else old[1] + 1
            db.execute(
                "INSERT INTO documents VALUES (?,?,?,?) ON CONFLICT(name) "
                "DO UPDATE SET body=excluded.body,revision=excluded.revision,"
                "updated_at=excluded.updated_at",
                (name, body, revision, now),
            )
            if history:
                db.execute(
                    "INSERT INTO changes(name,revision,body,updated_at) VALUES (?,?,?,?)",
                    (name, revision, body, now),
                )

    def import_json(self, name: str, source: Path, *, validate=None) -> dict[str, Any] | None:
        """Import only once; retain source bytes as legacy evidence, never a live mirror."""
        with self.connection() as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT body FROM documents WHERE name=?", (name,)).fetchone()
            if row is not None:
                return json.loads(row[0])
            if db.execute("SELECT 1 FROM imports WHERE name=?", (name,)).fetchone():
                return None
            value = None
            if source.exists():
                raw = source.read_bytes()
                value = json.loads(raw)
                if not isinstance(value, dict):
                    raise PersistenceError(f"legacy state must be an object: {source}")
                if validate is not None:
                    validate(value)
                backup = source.with_suffix(source.suffix + ".pre-sqlite")
                if not backup.exists():
                    fd = os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                    with os.fdopen(fd, "wb") as stream:
                        stream.write(raw)
                        stream.flush()
                        os.fsync(stream.fileno())
                    _sync_parent(backup)
                db.execute(
                    "INSERT INTO documents VALUES (?,?,1,?)",
                    (name, _json(value), datetime.now(UTC).isoformat()),
                )
            db.execute("INSERT INTO imports VALUES (?,?)", (name, str(source)))
            return value

    def backup(self, destination: Path) -> None:
        """Online backup includes committed WAL data; destination must be new."""
        fd = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        try:
            with self.connection() as source:
                target = sqlite3.connect(destination)
                try:
                    source.backup(target)
                    if target.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                        raise PersistenceError("backup integrity check failed")
                finally:
                    target.close()
            with destination.open("rb") as stream:
                os.fsync(stream.fileno())
            _sync_parent(destination)
        except BaseException:
            destination.unlink(missing_ok=True)
            raise
