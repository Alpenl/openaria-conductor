"""Maintenance must not initialize input backups or require writable snapshots."""

import hashlib
import multiprocessing
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from rp_ylx import database_admin
from rp_ylx.persistence import PersistenceError, StateDatabase


class ReadonlyBackupTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.source_dir = self.root / "inputs"
        self.source_dir.mkdir()
        self.producer = StateDatabase(self.root / "producer.sqlite3")
        self.producer.put("capture", {"revision": 7})
        self.source = self.source_dir / "backup.sqlite3"
        self.producer.backup(self.source)
        self.destination = self.root / "state.sqlite3"
        target = StateDatabase(self.destination)
        target.put("capture", {"revision": 42})
        self.addCleanup(lambda: self.source_dir.chmod(0o700))
        self.offline = patch.object(database_admin, "require_offline")
        self.offline.start()
        self.addCleanup(self.offline.stop)

    def standalone(self):
        # Create the portable input explicitly so source protection tests are
        # independent of whether the producer format itself has been fixed.
        with closing(sqlite3.connect(self.source)) as db:
            self.assertEqual(db.execute("PRAGMA journal_mode=DELETE").fetchone()[0], "delete")

    def signature(self):
        return {
            p.name: (p.stat().st_mode & 0o777, hashlib.sha256(p.read_bytes()).hexdigest())
            for p in self.source_dir.iterdir()
            if p.is_file() and not p.name.endswith("-shm")
        }

    def restore(self):
        return database_admin.restore(self.source, self.destination)

    def test_new_backup_is_self_contained_and_readable_in_a_readonly_directory(self):
        self.source.chmod(0o400)
        self.source_dir.chmod(0o500)
        before = self.signature()
        info = database_admin.inspect(self.source)
        self.assertEqual(info["integrity"], "ok")
        self.assertEqual(self.source.read_bytes()[18:20], bytes([1, 1]))
        self.assertEqual(self.signature(), before)
        self.assertEqual(sorted(p.name for p in self.source_dir.iterdir()), [self.source.name])

    def test_restore_keeps_source_permissions_bytes_and_no_authority_files(self):
        self.standalone()
        self.source.chmod(0o444)
        before = self.signature()
        self.restore()
        self.assertEqual(StateDatabase(self.destination).get("capture"), {"revision": 7})
        self.assertEqual(self.signature(), before)
        self.assertEqual(sorted(p.name for p in self.source_dir.iterdir()), [self.source.name])

    def test_restore_from_readonly_source_directory_succeeds_without_source_writes(self):
        self.standalone()
        self.source.chmod(0o400)
        self.source_dir.chmod(0o500)
        before = self.signature()
        self.restore()
        self.assertEqual(StateDatabase(self.destination).get("capture"), {"revision": 7})
        self.assertEqual(self.signature(), before)
        self.assertEqual(sorted(p.name for p in self.source_dir.iterdir()), [self.source.name])

    def test_backup_command_accepts_readonly_source_without_initialization(self):
        self.standalone()
        self.source.chmod(0o400)
        self.source_dir.chmod(0o500)
        before = self.signature()
        output = self.root / "copy.sqlite3"
        result = database_admin.manage(self.source, "backup", source=None, destination=output)
        self.assertEqual(result["integrity"], "ok")
        self.assertEqual(StateDatabase(output).get("capture"), {"revision": 7})
        self.assertEqual(self.signature(), before)

    def test_readonly_source_with_committed_wal_keeps_latest_data(self):
        self.source.unlink()

        def writer():
            source = StateDatabase(self.source)
            source.put("capture", {"revision": 7})
            with source.connection() as reader:
                reader.execute("PRAGMA wal_autocheckpoint=0")
                reader.execute("BEGIN")
                reader.execute("SELECT * FROM documents").fetchall()
                source.put("capture", {"revision": 99})
                os._exit(79)

        process = multiprocessing.get_context("fork").Process(target=writer)
        process.start()
        try:
            process.join(10)
            self.assertEqual(process.exitcode, 79)
        finally:
            if process.is_alive():
                process.kill()
            process.join(5)
            process.close()
        self.assertGreater(Path(str(self.source) + "-wal").stat().st_size, 0)
        for path in self.source_dir.iterdir():
            path.chmod(0o400)
        self.source_dir.chmod(0o500)
        before = self.signature()
        self.restore()
        self.assertEqual(StateDatabase(self.destination).get("capture"), {"revision": 99})
        self.assertEqual(self.signature(), before)

    def test_source_removed_after_inspect_is_not_recreated_empty(self):
        self.standalone()
        original = database_admin.inspect

        def inspect(path):
            result = original(path)
            if path == self.source:
                self.source.unlink()
            return result

        with (
            patch.object(database_admin, "inspect", inspect),
            self.assertRaises((PersistenceError, sqlite3.Error, FileNotFoundError)),
        ):
            self.restore()
        self.assertFalse(self.source.exists())
        self.assertEqual(list(self.source_dir.iterdir()), [])
        self.assertEqual(StateDatabase(self.destination).get("capture"), {"revision": 42})

    def test_existing_backup_destination_does_not_modify_source(self):
        self.standalone()
        self.source.chmod(0o444)
        output = self.root / "existing.sqlite3"
        output.write_bytes(b"existing bytes")
        before = self.signature()
        with self.assertRaises(FileExistsError):
            database_admin.manage(self.source, "backup", source=None, destination=output)
        self.assertEqual(output.read_bytes(), b"existing bytes")
        self.assertEqual(self.signature(), before)

    def test_foreign_source_is_rejected_without_publishing_or_initializing_it(self):
        self.source.unlink()
        with closing(sqlite3.connect(self.source)) as db, db:
            db.execute("CREATE TABLE foreign_data(value TEXT)")
        self.source.chmod(0o444)
        before = self.signature()
        output = self.root / "foreign-copy.sqlite3"
        with self.assertRaises(PersistenceError):
            database_admin.manage(self.source, "backup", source=None, destination=output)
        self.assertFalse(output.exists())
        self.assertEqual(self.signature(), before)
