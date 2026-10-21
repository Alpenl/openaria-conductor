"""Rebuildable metadata index. Never an authority for artifact integrity."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
import sys
import threading
from collections import OrderedDict
from collections.abc import Callable, Iterator, Mapping
from contextlib import ExitStack, contextmanager
from itertools import groupby
from pathlib import Path

from rp_ylx.manifest_limits import MAX_MANIFEST_BYTES
from rp_ylx.persistence import StateDatabase


class SessionCatalog(StateDatabase):
    # Bound retained serialized summaries separately from dictionary overhead.
    _VALIDATION_CACHE_ENTRIES = 8192
    _VALIDATION_CACHE_BYTES = 8 * 1024 * 1024

    def __init__(self, path: Path) -> None:
        self._scan_state = threading.local()
        self._validation_lock = threading.Lock()
        self._validated_summaries: OrderedDict[
            tuple[str, str], tuple[str, str | bytes, Callable[[object], None]]
        ] = OrderedDict()
        self._validated_summary_bytes = 0
        super().__init__(path)
        self._admitted: set[tuple[str, str, str]] = set()
        with self.connection() as db, db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS verifications ("
                "id INTEGER PRIMARY KEY, volume TEXT NOT NULL, session TEXT NOT NULL, "
                "manifest_sha256 TEXT NOT NULL, verified_at TEXT NOT NULL, body TEXT NOT NULL, "
                "UNIQUE(volume,session,manifest_sha256,verified_at))"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS sessions ("
                "volume TEXT NOT NULL, session TEXT NOT NULL, signature TEXT NOT NULL, "
                "validator TEXT NOT NULL, started_at TEXT NOT NULL, take_id TEXT NOT NULL, "
                "summary TEXT NOT NULL, PRIMARY KEY(volume,session))"
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS session_time ON sessions"
                "(volume,started_at DESC,session DESC)"
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS session_take ON sessions"
                "(volume,take_id,started_at DESC,session DESC)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS artifacts ("
                "volume TEXT NOT NULL, session TEXT NOT NULL, path TEXT NOT NULL, "
                "sha256 TEXT NOT NULL, bytes INTEGER NOT NULL, role TEXT, "
                "PRIMARY KEY(volume,session,path), FOREIGN KEY(volume,session) "
                "REFERENCES sessions(volume,session) ON DELETE CASCADE)"
            )

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        current = getattr(self._scan_state, "connection", None)
        if current is not None:
            yield current
        else:
            with super().connection() as db:
                yield db

    @contextmanager
    def scan(self) -> Iterator[None]:
        """Commit a metadata discovery pass once, keeping FULL durability."""
        if getattr(self._scan_state, "connection", None) is not None:
            raise RuntimeError("catalog scans must not be nested")
        with super().connection() as db:
            # Reserve the writer before reading: another process must not turn
            # the later read/write upgrade into SQLITE_BUSY_SNAPSHOT.
            db.execute("BEGIN IMMEDIATE")
            self._scan_state.connection = db
            self._scan_state.validations_seen = set()
            try:
                yield
                db.commit()
            except BaseException:
                db.rollback()
                self._admitted.clear()
                with self._validation_lock:
                    self._validated_summaries.clear()
                    self._validated_summary_bytes = 0
                raise
            finally:
                self._scan_state.connection = None
                self._scan_state.validations_seen = None

    @contextmanager
    def _write_transaction(self) -> Iterator[sqlite3.Connection]:
        with self.connection() as db:
            if getattr(self._scan_state, "connection", None) is None:
                with db:
                    yield db
                return
            # A malformed session can be skipped by discovery without leaving
            # half of its cache update in the outer scan transaction.
            db.execute("SAVEPOINT catalog_entry")
            try:
                yield db
            except BaseException:
                # SQLITE_FULL can roll back the whole transaction itself.
                # Do not replace that first error with "no such savepoint".
                if db.in_transaction:
                    db.execute("ROLLBACK TO catalog_entry")
                    db.execute("RELEASE catalog_entry")
                raise
            else:
                db.execute("RELEASE catalog_entry")

    @staticmethod
    def signature(directory: Path) -> str:
        # SQLite caches metadata, but stat equality cannot establish that the
        # manifest is unchanged. Hash only its bounded bytes, never the media.
        parent = directory.lstat()
        if not stat.S_ISDIR(parent.st_mode):
            raise OSError("catalog directory must not be a symbolic link")
        parent_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
        try:
            opened_parent = os.fstat(parent_fd)
            if (parent.st_dev, parent.st_ino) != (opened_parent.st_dev, opened_parent.st_ino):
                raise OSError("catalog directory changed during open")
            fd = os.open(
                "manifest.json",
                os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
                dir_fd=parent_fd,
            )
            # Keep descriptor ownership here: stream construction and context
            # entry can fail before the stream can run its own cleanup.
            try:
                with os.fdopen(fd, "rb", closefd=False) as stream:
                    metadata = os.fstat(stream.fileno())
                    if (
                        not stat.S_ISREG(metadata.st_mode)
                        or metadata.st_nlink != 1
                        or metadata.st_size > MAX_MANIFEST_BYTES
                    ):
                        raise OSError("catalog manifest must be a bounded exclusively linked file")

                    def fields(value):
                        return (
                            value.st_dev,
                            value.st_ino,
                            value.st_mode,
                            value.st_nlink,
                            value.st_size,
                            value.st_mtime_ns,
                            value.st_ctime_ns,
                        )

                    digest = hashlib.sha256()
                    remaining = metadata.st_size
                    while remaining:
                        block = stream.read(min(remaining, 1024 * 1024))
                        if not block:
                            raise OSError("catalog manifest truncated during read")
                        digest.update(block)
                        remaining -= len(block)
                    if stream.read(1) or fields(os.fstat(stream.fileno())) != fields(metadata):
                        raise OSError("catalog manifest changed during read")
                    current = os.stat("manifest.json", dir_fd=parent_fd, follow_symlinks=False)
                    if fields(current) != fields(metadata):
                        raise OSError("catalog manifest replaced during read")
                    return json.dumps(
                        [parent.st_dev, parent.st_ino, *fields(metadata), digest.hexdigest()]
                    )
            finally:
                os.close(fd)
        finally:
            os.close(parent_fd)

    def _forget_summary_validation(self, key: tuple[str, str]) -> None:
        # The caller holds _validation_lock.
        previous = self._validated_summaries.pop(key, None)
        if previous is not None:
            self._validated_summary_bytes -= sys.getsizeof(previous[1])

    def _summary_was_validated(self, key, validator, raw, validate) -> bool:
        with self._validation_lock:
            previous = self._validated_summaries.get(key)
            if (
                previous is not None
                and previous[0] == validator
                and previous[1] == raw
                and previous[2] is validate
            ):
                self._validated_summaries.move_to_end(key)
                seen = getattr(self._scan_state, "validations_seen", None)
                if seen is not None and len(seen) < self._VALIDATION_CACHE_ENTRIES:
                    seen.add(key)
                return True
            self._forget_summary_validation(key)
            return False

    def _remember_summary_validation(self, key, validator, raw, validate) -> None:
        size = sys.getsizeof(raw)
        with self._validation_lock:
            self._forget_summary_validation(key)
            if size > self._VALIDATION_CACHE_BYTES or self._VALIDATION_CACHE_ENTRIES <= 0:
                return
            seen = getattr(self._scan_state, "validations_seen", None)
            if seen is not None and key not in seen and len(seen) >= self._VALIDATION_CACHE_ENTRIES:
                return
            while self._validated_summaries and (
                len(self._validated_summaries) >= self._VALIDATION_CACHE_ENTRIES
                or self._validated_summary_bytes + size > self._VALIDATION_CACHE_BYTES
            ):
                oldest = next(iter(self._validated_summaries))
                # A full scan visits every row. Evicting entries already used
                # in this scan would make a directory larger than the cache
                # miss on every subsequent page. Keep that working subset;
                # unused entries (including another volume) remain evictable.
                if seen is not None and oldest in seen:
                    return
                self._forget_summary_validation(oldest)
            self._validated_summaries[key] = (validator, raw, validate)
            self._validated_summary_bytes += size
            if seen is not None:
                seen.add(key)

    def cached(
        self,
        volume: str,
        session: str,
        signature: str,
        validator: str,
        *,
        directory: Path | None = None,
        required_artifact_role: str | None = None,
        validate_summary: Callable[[object], None] | None = None,
        cache_validation: bool = False,
    ):
        # Opt in only for a pure summary validator. Each call still reads the
        # current SQLite row and decodes a fresh response; no mutable value is
        # shared, and filesystem admission is independent of this cache.
        query = (
            "SELECT summary FROM sessions WHERE volume=? AND session=? "
            "AND signature=? AND validator=?"
        )
        parameters = (volume, session, signature, validator)
        if required_artifact_role is not None:
            # Bind the publication marker to the same indexed manifest row.
            # This admits metadata only; payload integrity remains unverified.
            query += (
                " AND EXISTS (SELECT 1 FROM artifacts AS a WHERE "
                "a.volume=sessions.volume AND a.session=sessions.session AND a.role=?)"
            )
            parameters += (required_artifact_role,)
        with self.connection() as db:
            row = db.execute(query, parameters).fetchone()
        if row is None:
            return None
        key = (volume, session, signature)
        try:
            value = json.loads(row[0])
            if not isinstance(value, dict) or value.get("session_id") != session:
                raise ValueError("cached summary does not identify the indexed session")
            # A persisted digest is historical metadata, not proof about current payloads.
            value["verification"] = None
            if validate_summary is not None and not (
                cache_validation
                and self._summary_was_validated(
                    (volume, session), validator, row[0], validate_summary
                )
            ):
                validate_summary(value)
                if cache_validation:
                    self._remember_summary_validation(
                        (volume, session), validator, row[0], validate_summary
                    )
        except (TypeError, ValueError, RecursionError):
            # Only this rebuildable row is invalid. Leave database I/O/schema
            # failures visible; never replace or reinitialize state authority.
            self._admitted.discard(key)
            with self._validation_lock:
                self._forget_summary_validation((volume, session))
            return None
        if directory is not None and key not in self._admitted:
            with self.connection() as db:
                artifacts = db.execute(
                    "SELECT path,bytes FROM artifacts WHERE volume=? AND session=? ORDER BY path",
                    (volume, session),
                ).fetchall()
            if not self._artifacts_match(directory, artifacts):
                return None
            # Bind the admission to the manifest inspected before the scan,
            # including replacement of the session root during that scan.
            if self.signature(directory) != signature:
                return None
            self._admitted.add(key)
        return value

    @staticmethod
    def _artifacts_match(directory: Path, artifacts: list[tuple[str, int]]) -> bool:
        def paths():
            for relative, expected_bytes in artifacts:
                if not isinstance(relative, str) or "\x00" in relative:
                    raise OSError("invalid indexed artifact path")
                path = Path(relative)
                if path.is_absolute() or ".." in path.parts or not path.parts:
                    raise OSError("invalid indexed artifact path")
                yield path.parts, expected_bytes

        def identity(metadata):
            return (
                metadata.st_dev,
                metadata.st_ino,
                metadata.st_mode,
                metadata.st_mtime_ns,
                metadata.st_ctime_ns,
            )

        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
        with ExitStack() as root_stack:
            root_fd = os.open(directory, flags)
            root_stack.callback(os.close, root_fd)
            root_identity = identity(os.fstat(root_fd))
            # Retain descriptors only for one parent chain at a time, not one
            # per artifact or per unique directory in a long recording.
            for parent, entries in groupby(paths(), key=lambda entry: entry[0][:-1]):
                with ExitStack() as parent_stack:
                    parent_fd = root_fd
                    opened = []
                    for part in parent:
                        child_fd = os.open(part, flags, dir_fd=parent_fd)
                        parent_stack.callback(os.close, child_fd)
                        opened.append((parent_fd, part, identity(os.fstat(child_fd))))
                        parent_fd = child_fd
                    for parts, expected_bytes in entries:
                        metadata = os.stat(parts[-1], dir_fd=parent_fd, follow_symlinks=False)
                        if (
                            not stat.S_ISREG(metadata.st_mode)
                            or metadata.st_size != expected_bytes
                            or metadata.st_nlink != 1
                        ):
                            return False
                    # An open directory remains readable after a rename; do not
                    # admit its artifacts if the indexed path changed meanwhile.
                    for ancestor_fd, name, expected_identity in opened:
                        current = os.stat(name, dir_fd=ancestor_fd, follow_symlinks=False)
                        if identity(current) != expected_identity:
                            return False
            return identity(directory.lstat()) == root_identity

    def remember(
        self,
        volume: str,
        directory: Path,
        validator: str,
        summary: Mapping[str, object],
        manifest: Mapping[str, object],
        *,
        expected_signature: str | None = None,
    ) -> None:
        signature = self.signature(directory)
        if expected_signature is not None and signature != expected_signature:
            raise OSError("manifest changed during catalog inspection")

        def artifacts(value):
            if isinstance(value, dict):
                if all(k in value for k in ("path", "sha256", "bytes")):
                    yield value
                else:
                    for child in value.values():
                        yield from artifacts(child)
            elif isinstance(value, list):
                for child in value:
                    yield from artifacts(child)

        sid = str(summary["session_id"])
        with self._write_transaction() as db:
            db.execute(
                "INSERT INTO sessions VALUES (?,?,?,?,?,?,?) ON CONFLICT(volume,session) "
                "DO UPDATE SET signature=excluded.signature,validator=excluded.validator,"
                "started_at=excluded.started_at,take_id=excluded.take_id,summary=excluded.summary",
                (
                    volume,
                    sid,
                    signature,
                    validator,
                    summary["started_at"],
                    summary["take_id"],
                    json.dumps(dict(summary)),
                ),
            )
            db.execute("DELETE FROM artifacts WHERE volume=? AND session=?", (volume, sid))
            db.executemany(
                "INSERT INTO artifacts VALUES (?,?,?,?,?,?)",
                [
                    (volume, sid, a["path"], a["sha256"], a["bytes"], a.get("role"))
                    for a in artifacts(manifest)
                ],
            )
            verification = summary.get("verification")
            if isinstance(verification, Mapping):
                db.execute(
                    "INSERT OR IGNORE INTO verifications "
                    "(volume,session,manifest_sha256,verified_at,body) VALUES (?,?,?,?,?)",
                    (
                        volume,
                        sid,
                        verification["manifest_sha256"],
                        verification["verified_at"],
                        json.dumps(dict(verification)),
                    ),
                )

    def discard_missing(self, volume: str, discovered: set[str]) -> None:
        self._admitted = {key for key in self._admitted if key[0] != volume or key[1] in discovered}
        with self._validation_lock:
            for key in list(self._validated_summaries):
                if key[0] == volume and key[1] not in discovered:
                    self._forget_summary_validation(key)
        with self._write_transaction() as db:
            existing = db.execute(
                "SELECT session FROM sessions WHERE volume=?", (volume,)
            ).fetchall()
            db.executemany(
                "DELETE FROM sessions WHERE volume=? AND session=?",
                [(volume, row[0]) for row in existing if row[0] not in discovered],
            )

    def summaries(
        self, volume: str, *, take_id: str | None = None, limit: int | None = None, offset: int = 0
    ):
        sql = "SELECT summary FROM sessions WHERE volume=?"
        args: list[object] = [volume]
        if take_id is not None:
            sql += " AND take_id=?"
            args.append(take_id)
        sql += " ORDER BY started_at DESC,session DESC"
        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            args.extend([limit, offset])
        with self.connection() as db:
            return [json.loads(row[0]) for row in db.execute(sql, args)]
