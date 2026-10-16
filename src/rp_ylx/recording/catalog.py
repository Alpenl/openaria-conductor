"""Rebuildable metadata index. Never an authority for artifact integrity."""

from __future__ import annotations

import json
import stat
from collections.abc import Mapping
from pathlib import Path

from rp_ylx.persistence import StateDatabase


class SessionCatalog(StateDatabase):
    def __init__(self, path: Path) -> None:
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

    @staticmethod
    def signature(directory: Path) -> str:
        # Include ctime and inode so same-size/restored-mtime replacement invalidates.
        parent = directory.lstat()
        metadata = (directory / "manifest.json").lstat()
        if not stat.S_ISDIR(parent.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise OSError("catalog paths must not be symbolic links")
        return json.dumps(
            [
                parent.st_dev,
                parent.st_ino,
                metadata.st_dev,
                metadata.st_ino,
                metadata.st_size,
                metadata.st_mtime_ns,
                metadata.st_ctime_ns,
                metadata.st_nlink,
            ]
        )

    def cached(
        self,
        volume: str,
        session: str,
        signature: str,
        validator: str,
        *,
        directory: Path | None = None,
    ):
        with self.connection() as db:
            row = db.execute(
                "SELECT summary FROM sessions WHERE volume=? AND session=? "
                "AND signature=? AND validator=?",
                (volume, session, signature, validator),
            ).fetchone()
        if row is None:
            return None
        key = (volume, session, signature)
        if directory is not None and key not in self._admitted:
            with self.connection() as db:
                artifacts = db.execute(
                    "SELECT path,bytes FROM artifacts WHERE volume=? AND session=?",
                    (volume, session),
                ).fetchall()
            for relative, expected_bytes in artifacts:
                path = Path(relative)
                if path.is_absolute() or ".." in path.parts:
                    raise OSError("invalid indexed artifact path")
                current = directory
                for part in path.parts[:-1]:
                    current = current / part
                    if not stat.S_ISDIR(current.lstat().st_mode):
                        raise OSError("indexed artifact directory is not a regular directory")
                metadata = (directory / path).lstat()
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or metadata.st_size != expected_bytes
                    or metadata.st_nlink != 1
                ):
                    return None
            self._admitted.add(key)
        value = json.loads(row[0])
        # A persisted digest is historical metadata, not proof about current payloads.
        value["verification"] = None
        return value

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
        with self.connection() as db, db:
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
        with self.connection() as db, db:
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
