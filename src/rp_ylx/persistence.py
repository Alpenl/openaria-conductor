"""Device-local SQLite authority; media and recovery journals remain independent files.

Databases use WAL/FULL, bounded lock waits and short transactions. A migration marker
prevents a missing database from silently resurrecting obsolete JSON state. No payload
verification proof is reconstructed from this database.
"""

from __future__ import annotations

import errno
import fcntl
import json
import os
import sqlite3
import stat
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager, closing, contextmanager, suppress
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
APPLICATION_ID = 0x594C5801
BACKUP_TIMEOUT_SECONDS = 30

_STATE_TABLES = {
    "documents": "name TEXT PRIMARY KEY, body TEXT NOT NULL, "
    "revision INTEGER NOT NULL, updated_at TEXT NOT NULL",
    "imports": "name TEXT PRIMARY KEY, source TEXT NOT NULL",
    "changes": "id INTEGER PRIMARY KEY, name TEXT NOT NULL, "
    "revision INTEGER NOT NULL, body TEXT NOT NULL, updated_at TEXT NOT NULL",
    "capture_commands": "principal TEXT NOT NULL, operation TEXT NOT NULL, "
    "idempotency_key TEXT NOT NULL, body TEXT NOT NULL, "
    "PRIMARY KEY(principal,operation,idempotency_key)",
    "transfers": "id TEXT PRIMARY KEY, volume TEXT NOT NULL, session TEXT NOT NULL, "
    "artifact TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT, "
    "status TEXT NOT NULL, bytes_read INTEGER NOT NULL",
}


class PersistenceError(RuntimeError):
    """Persistent authority cannot safely be loaded or updated."""


@cache
def _schema_columns() -> dict[str, tuple]:
    # Derive SQLite's column/primary-key metadata from the same declarations
    # used for a fresh database. Do not duplicate a second schema definition.
    with closing(sqlite3.connect(":memory:")) as db:
        for name, columns in _STATE_TABLES.items():
            db.execute(f"CREATE TABLE {name} ({columns})")
        return {name: tuple(db.execute(f"PRAGMA table_xinfo({name})")) for name in _STATE_TABLES}


def validate_state_schema(db: sqlite3.Connection) -> None:
    """Reject incomplete version-1 authority; extra catalog tables are allowed."""
    objects = dict(db.execute("SELECT name,type FROM sqlite_master"))
    for name, columns in _schema_columns().items():
        if (
            objects.get(name) != "table"
            or tuple(db.execute(f"PRAGMA table_xinfo({name})")) != columns
        ):
            raise PersistenceError(f"incompatible or missing authoritative table: {name}")


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _require_regular_database_file(path: Path) -> None:
    # Inspect metadata only: opening and closing another descriptor for this
    # inode could release SQLite's process-wide POSIX locks. This rejects
    # existing special files and final-component links, not hostile concurrent
    # path replacement; database directories must remain trusted.
    if not stat.S_ISREG(path.lstat().st_mode):
        raise PersistenceError("database must be a regular file")


def _sync_parent(path: Path) -> None:
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _preserve_import_source(source: Path, raw: bytes) -> None:
    """Publish an exact recovery copy without replacing any existing evidence."""
    backup = source.with_suffix(source.suffix + ".pre-sqlite")
    if not os.path.lexists(backup):
        temporary = backup.with_name(f".{backup.name}.{uuid.uuid4().hex}.tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            with os.fdopen(fd, "wb") as stream:
                if stream.write(raw) != len(raw):
                    raise OSError("short legacy import backup write")
                stream.flush()
                os.fsync(stream.fileno())
            # Unlike replace(), link() cannot overwrite a competing backup.
            # A crash before publication leaves only a private temporary;
            # after publication the final name already holds complete bytes.
            with suppress(FileExistsError):
                os.link(temporary, backup, follow_symlinks=False)
            # Independently verify a competing winning copy below.
        finally:
            temporary.unlink(missing_ok=True)
    fd = os.open(backup, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != len(raw):
            raise PersistenceError("legacy import backup does not match the source")
        offset = 0
        while offset < len(raw):
            chunk = os.read(fd, min(64 * 1024, len(raw) - offset))
            if not chunk or chunk != raw[offset : offset + len(chunk)]:
                raise PersistenceError("legacy import backup does not match the source")
            offset += len(chunk)
        if os.read(fd, 1):
            raise PersistenceError("legacy import backup changed during verification")
        # A previous attempt may have failed before either durability barrier.
        # Existence alone must never turn that failure into a successful import.
        os.fsync(fd)
    finally:
        os.close(fd)
    _sync_parent(backup)


class StateDatabase:
    """Versioned local state with explicit, one-time JSON imports."""

    def __init__(self, path: Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        lock = path.with_suffix(path.suffix + ".init.lock")
        fd = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise PersistenceError("database initialization lock must be a regular file")
            if os.geteuid() == 0:
                parent = path.parent.stat()
                os.fchown(fd, parent.st_uid, parent.st_gid)
            deadline = time.monotonic() + 5
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise PersistenceError("database initialization is busy") from None
                    time.sleep(0.01)
            self._initialize(path)
        finally:
            os.close(fd)

    def _initialize(self, path: Path) -> None:
        self.path = Path(path)
        self.marker = self.path.with_suffix(self.path.suffix + ".initialized")
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if not self.path.exists() and self.marker.exists():
            raise PersistenceError(f"missing authoritative database: {self.path}")
        try:
            metadata = self.path.lstat()
        except FileNotFoundError:
            fd = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            os.close(fd)
            metadata = self.path.lstat()
        if not stat.S_ISREG(metadata.st_mode):
            raise PersistenceError("database must be a regular file")
        # Do not open/close an unrelated descriptor on an existing SQLite file:
        # closing it could release another connection's process-wide POSIX locks.
        self.path.chmod(0o600)
        if os.geteuid() == 0:
            parent = self.path.parent.stat()
            os.chown(self.path, parent.st_uid, parent.st_gid, follow_symlinks=False)
        try:
            with self.connection() as db:
                version = db.execute("PRAGMA user_version").fetchone()[0]
                application = db.execute("PRAGMA application_id").fetchone()[0]
                objects = db.execute("SELECT name FROM sqlite_master").fetchall()
                if self.marker.exists() and version == 0:
                    raise PersistenceError("initialized database has lost its schema")
                if application != APPLICATION_ID and (application or version or objects):
                    raise PersistenceError("not an OpenAria state database")
                if version not in (0, SCHEMA_VERSION):
                    raise PersistenceError(f"unsupported database schema: {version}")
                if version == SCHEMA_VERSION:
                    validate_state_schema(db)
                elif objects:
                    # Schema creation and its version commit together. Existing
                    # tables with version zero cannot be an interrupted creation.
                    raise PersistenceError("unversioned state database contains existing schema")
                if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise PersistenceError("state database integrity check failed")
                db.execute("PRAGMA journal_mode=WAL")
                with db:
                    if version == 0:
                        db.execute("BEGIN IMMEDIATE")
                        for name, columns in _STATE_TABLES.items():
                            db.execute(f"CREATE TABLE {name} ({columns})")
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
        # Recheck on every open; the path may have changed since initialization.
        _require_regular_database_file(self.path)
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
        # Lifecycle and receipts are committed together. Read both from the
        # same WAL snapshot even if a checkpoint writer commits between SELECTs.
        # A deferred read transaction does not reserve the SQLite writer.
        with self.connection() as db:
            db.execute("BEGIN")
            row = db.execute("SELECT body FROM documents WHERE name='capture'").fetchone()
            if row is None:
                raise PersistenceError("missing authoritative document: capture")
            value = json.loads(row[0])
            if not isinstance(value, dict):
                raise PersistenceError("invalid state document: capture")
            if "commands" not in value:
                value["commands"] = []
                invalid = value.get("invalid_commands", [])
                if not isinstance(invalid, list):
                    raise PersistenceError("invalid quarantined command identities")
                for principal, operation, key, raw in db.execute(
                    "SELECT principal,operation,idempotency_key,body "
                    "FROM capture_commands ORDER BY rowid"
                ):
                    scope = [principal, operation, key]
                    if not all(isinstance(part, str) and part for part in scope):
                        raise PersistenceError("invalid command table identity")
                    try:
                        if not isinstance(raw, str):
                            raise ValueError("command row body is not text")
                        item = json.loads(raw)
                        if (
                            not isinstance(item, dict)
                            or [
                                item.get("principal_id"),
                                item.get("operation"),
                                item.get("idempotency_key"),
                            ]
                            != scope
                        ):
                            raise ValueError("command body differs from table identity")
                    except (ValueError, TypeError, RecursionError):
                        # Preserve the original row and use its SQL primary key,
                        # never a potentially forged identity inside the body.
                        evidence = raw if isinstance(raw, str) else repr(raw)
                        invalid.append({"scope": scope, "receipt": evidence})
                    else:
                        value["commands"].append(item)
                if invalid:
                    value["invalid_commands"] = invalid
            return value

    def begin_transfer(self, volume: str, session: str, artifact: str) -> str:
        identity = str(uuid.uuid4())
        with self.connection() as db, db:
            db.execute(
                "INSERT INTO transfers VALUES (?,?,?,?,?,NULL,'open',0)",
                (identity, volume, session, artifact, datetime.now(UTC).isoformat()),
            )
        return identity

    def finish_transfer(self, identity: str, status: str, bytes_read: int) -> None:
        with self.connection() as db, db:
            db.execute(
                "UPDATE transfers SET finished_at=?,status=?,bytes_read=? WHERE id=?",
                (datetime.now(UTC).isoformat(), status, bytes_read, identity),
            )
            db.execute(
                "DELETE FROM transfers WHERE status!='open' AND rowid NOT IN "
                "(SELECT rowid FROM transfers ORDER BY rowid DESC LIMIT 4096)"
            )

    def interrupt_transfers(self) -> None:
        with self.connection() as db, db:
            db.execute(
                "UPDATE transfers SET status='interrupted',finished_at=? WHERE status='open'",
                (datetime.now(UTC).isoformat(),),
            )

    def put_capture_state(
        self,
        value: Mapping[str, Any],
        commands: list[dict],
        removed: set,
        *,
        camera_focus: Mapping[str, Any] | None = None,
    ) -> None:
        """Commit lifecycle, new receipts and an optional focus preference together."""
        # Keep large corrupt receipt evidence in the receipt table, once. The
        # lifecycle document only needs identities to keep replay fail-closed.
        # This also migrates the previous inline quarantine representation.
        quarantined: set[tuple[str, str, str]] = set()
        evidence = []
        if "invalid_commands" in value:
            invalid = value["invalid_commands"]
            if not isinstance(invalid, list):
                raise PersistenceError("invalid quarantined command identities")
            compact = []
            for entry in invalid:
                if not isinstance(entry, dict) or not isinstance(entry.get("scope"), list):
                    raise PersistenceError("invalid quarantined command identity")
                scope = tuple(entry["scope"])
                if len(scope) != 3 or not all(isinstance(part, str) and part for part in scope):
                    raise PersistenceError("invalid quarantined command identity")
                quarantined.add(scope)
                compact.append({"scope": list(scope)})
                raw = entry.get("receipt")
                if raw is not None:
                    # Escaped text preserves invalid Unicode from old parsed
                    # receipts without making the SQLite TEXT binding fail.
                    if not isinstance(raw, str):
                        raw = json.dumps(raw, ensure_ascii=True)
                    evidence.append((*scope, raw))
            value = {**value, "invalid_commands": compact}
        if any(
            (c["principal_id"], c["operation"], c["idempotency_key"]) in quarantined
            for c in commands
        ):
            raise PersistenceError("cannot replace a quarantined command receipt")
        body = _json(value)
        now = datetime.now(UTC).isoformat()
        with self.connection() as db, db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT body,revision FROM documents WHERE name='capture'").fetchone()
            if old is None and db.execute("SELECT 1 FROM imports WHERE name='capture'").fetchone():
                raise PersistenceError("missing authoritative document: capture")
            # Publish evidence and compact state atomically. A failed insert
            # rolls back the document too; existing SQL evidence is never replaced.
            db.executemany("INSERT OR IGNORE INTO capture_commands VALUES (?,?,?,?)", evidence)
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
                list(set(removed) - quarantined),
            )
            db.executemany(
                "INSERT OR REPLACE INTO capture_commands VALUES (?,?,?,?)",
                [
                    (c["principal_id"], c["operation"], c["idempotency_key"], _json(c))
                    for c in commands
                ],
            )
            if camera_focus is not None:
                self._put_document(db, "camera_focus", _json(camera_focus), now, history=True)

    @staticmethod
    def _put_document(
        db: sqlite3.Connection, name: str, body: str, now: str, *, history: bool
    ) -> None:
        old = db.execute("SELECT body,revision FROM documents WHERE name=?", (name,)).fetchone()
        if old is None and db.execute("SELECT 1 FROM imports WHERE name=?", (name,)).fetchone():
            raise PersistenceError(f"missing authoritative document: {name}")
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

    def put(self, name: str, value: Mapping[str, Any], *, history: bool = False) -> None:
        body = _json(value)
        now = datetime.now(UTC).isoformat()
        with self.connection() as db, db:
            db.execute("BEGIN IMMEDIATE")
            self._put_document(db, name, body, now, history=history)

    def import_json(
        self,
        name: str,
        source: Path,
        *,
        validate=None,
        default_factory: Callable[[], Mapping[str, Any]] = dict,
    ) -> dict[str, Any]:
        """Commit the first state and receipt together; never reset a missing authority."""
        with self.connection() as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT body FROM documents WHERE name=?", (name,)).fetchone()
            if row is not None:
                return json.loads(row[0])
            if db.execute("SELECT 1 FROM imports WHERE name=?", (name,)).fetchone():
                raise PersistenceError(f"missing authoritative document: {name}")
            raw = None
            if source.exists():
                raw = source.read_bytes()
                value = json.loads(raw)
            else:
                value = default_factory()
            if not isinstance(value, Mapping):
                raise PersistenceError(f"initial state must be an object: {name}")
            value = dict(value)
            if validate is not None:
                validate(value)
            body = _json(value)
            if raw is not None:
                _preserve_import_source(source, raw)
            db.execute(
                "INSERT INTO documents VALUES (?,?,1,?)",
                (name, body, datetime.now(UTC).isoformat()),
            )
            db.execute("INSERT INTO imports VALUES (?,?)", (name, str(source)))
            return value

    def backup(self, destination: Path) -> None:
        """Copy committed WAL data, then publish a complete, non-overwriting backup."""
        self._backup(destination, self.connection)

    @staticmethod
    def backup_readonly(source_path: Path, destination: Path) -> None:
        """Copy existing authority without initializing or changing source permissions."""

        @contextmanager
        def connection() -> Iterator[sqlite3.Connection]:
            _require_regular_database_file(source_path)
            db = sqlite3.connect(source_path.absolute().as_uri() + "?mode=ro", uri=True, timeout=5)
            try:
                if (
                    db.execute("PRAGMA application_id").fetchone()[0] != APPLICATION_ID
                    or db.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION
                ):
                    raise PersistenceError("unsupported backup source identity or schema")
                validate_state_schema(db)
                yield db
            finally:
                db.close()

        StateDatabase._backup(destination, connection)

    @staticmethod
    def _backup(
        destination: Path,
        connection: Callable[[], AbstractContextManager[sqlite3.Connection]],
    ) -> None:
        if os.path.lexists(destination):
            raise FileExistsError(errno.EEXIST, "backup destination already exists", destination)
        temporary = destination.with_name(f".{destination.name}.backup-{uuid.uuid4().hex}.tmp")
        fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        os.close(fd)
        try:
            with connection() as source:
                target = sqlite3.connect(temporary)
                try:
                    # SQLite's backup API retries BUSY/LOCKED independently of
                    # connect(timeout=...). Check one deadline between bounded
                    # page batches, including while writers keep restarting it.
                    source.execute("PRAGMA busy_timeout=0")
                    deadline = time.monotonic() + BACKUP_TIMEOUT_SECONDS

                    def check_deadline(_status, _remaining, _total):
                        if time.monotonic() >= deadline:
                            raise PersistenceError("state database backup copy deadline exceeded")

                    source.backup(target, pages=128, progress=check_deadline, sleep=0.05)
                    if (
                        target.execute("PRAGMA application_id").fetchone()[0] != APPLICATION_ID
                        or target.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION
                    ):
                        raise PersistenceError("unsupported backup snapshot identity or schema")
                    validate_state_schema(target)
                    if target.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                        raise PersistenceError("backup integrity check failed")
                    # Only the private copy changes journal mode. The source
                    # may be live WAL; its committed pages are included above.
                    # A standalone backup must not need writable WAL/SHM files
                    # merely to inspect or restore it from a read-only location.
                    if target.execute("PRAGMA journal_mode=DELETE").fetchone()[0] != "delete":
                        raise PersistenceError("backup snapshot is not self-contained")
                finally:
                    target.close()
            with temporary.open("rb") as stream:
                os.fsync(stream.fileno())
            # A competing destination must never be overwritten. Unlike a
            # placeholder final file, only this complete, synced copy is visible.
            os.link(temporary, destination, follow_symlinks=False)
        except BaseException:
            # The final name may belong to a concurrent producer. Clean up only
            # our private staging name and preserve the original exception.
            with suppress(OSError):
                temporary.unlink(missing_ok=True)
            raise
        else:
            temporary.unlink()
            # If this barrier fails, report failure but retain the complete
            # published copy; never unlink a final name after publication.
            _sync_parent(destination)
