"""Private restore snapshots are owned even when preparation fails."""

import json
import os
import sqlite3
import stat
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from rp_ylx import database_admin, persistence
from rp_ylx.persistence import StateDatabase


class RestoreSnapshotCleanupTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.source = self.root / "source.sqlite3"
        self.target = self.root / "state.sqlite3"
        StateDatabase(self.source).put("capture", {"revision": 7})
        StateDatabase(self.target).put("capture", {"revision": 42})
        self.offline = patch.object(database_admin, "require_offline")
        self.offline.start()
        self.addCleanup(self.offline.stop)

    def value(self, path):
        with closing(sqlite3.connect(path.absolute().as_uri() + "?mode=ro", uri=True)) as db:
            return json.loads(
                db.execute("SELECT body FROM documents WHERE name='capture'").fetchone()[0]
            )["revision"]

    def restore(self):
        return database_admin.restore(self.source, self.target)

    def assert_no_snapshot(self):
        self.assertFalse(list(self.root.glob("*.restore-*")))
        self.assertFalse(list(self.root.glob(".*.backup-*.tmp*")))

    def assert_original(self):
        self.assertEqual(self.value(self.target), 42)
        self.assertEqual(self.value(self.source), 7)
        self.assertFalse(list(self.root.glob("*.before-restore-*")))

    def directory_failure(self, fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError("injected directory sync failure")
        return self.fsync(fd)

    def test_directory_sync_failure_never_leaves_a_private_restore_copy(self):
        self.fsync = os.fsync
        with (
            patch.object(persistence.os, "fsync", self.directory_failure),
            self.assertRaisesRegex(OSError, "directory sync failure"),
        ):
            self.restore()
        self.assert_original()
        self.assert_no_snapshot()
        self.restore()
        self.assertEqual(self.value(self.target), 7)

    def test_file_sync_failure_cleans_preparation_and_preserves_original(self):
        fsync = os.fsync

        def fail(fd):
            if ".restore-" in os.readlink(f"/proc/self/fd/{fd}"):
                raise OSError("injected snapshot file sync failure")
            return fsync(fd)

        with (
            patch.object(persistence.os, "fsync", fail),
            self.assertRaisesRegex(OSError, "snapshot file sync failure"),
        ):
            self.restore()
        self.assert_original()
        self.assert_no_snapshot()

    def test_rejected_snapshot_cleans_all_private_sqlite_journals(self):
        inspect = database_admin.inspect

        def reject(path):
            if ".restore-" in path.name:
                for suffix in ("-wal", "-shm", "-journal"):
                    Path(str(path) + suffix).write_bytes(b"injected private journal")
                raise OSError("snapshot validation failed")
            return inspect(path)

        with (
            patch.object(database_admin, "inspect", reject),
            self.assertRaisesRegex(OSError, "snapshot validation failed"),
        ):
            self.restore()
        self.assert_original()
        self.assert_no_snapshot()

    def test_preexisting_snapshot_collision_never_deletes_unowned_files(self):
        prepared = self.target.with_name(self.target.name + ".restore-collision")
        outside = self.root / "outside"
        outside.write_bytes(b"outside")
        for kind in ("file", "symlink", "fifo"):
            with self.subTest(kind=kind):
                if kind == "file":
                    prepared.write_bytes(b"existing snapshot")
                elif kind == "symlink":
                    prepared.symlink_to(outside)
                else:
                    os.mkfifo(prepared)
                before = prepared.lstat()
                try:
                    with (
                        patch.object(
                            database_admin.uuid,
                            "uuid4",
                            return_value=SimpleNamespace(hex="collision"),
                        ),
                        self.assertRaises(FileExistsError),
                    ):
                        self.restore()
                    after = prepared.lstat()
                    self.assertEqual((before.st_ino, before.st_mode), (after.st_ino, after.st_mode))
                    self.assertEqual(outside.read_bytes(), b"outside")
                    if kind == "file":
                        self.assertEqual(prepared.read_bytes(), b"existing snapshot")
                    self.assert_original()
                finally:
                    prepared.unlink()

    def test_existing_private_journal_collision_is_not_consumed_or_deleted(self):
        prepared = self.target.with_name(self.target.name + ".restore-collision")
        for suffix in ("-wal", "-shm", "-journal"):
            for kind in ("file", "dangling-link"):
                with self.subTest(suffix=suffix, kind=kind):
                    journal = Path(str(prepared) + suffix)
                    if kind == "file":
                        journal.write_bytes(b"existing journal evidence")
                    else:
                        journal.symlink_to(self.root / "absent")
                    before = journal.lstat()
                    try:
                        with (
                            patch.object(
                                database_admin.uuid,
                                "uuid4",
                                return_value=SimpleNamespace(hex="collision"),
                            ),
                            self.assertRaises(FileExistsError),
                        ):
                            self.restore()
                        self.assertFalse(prepared.exists())
                        after = journal.lstat()
                        self.assertEqual(
                            (before.st_ino, before.st_mode), (after.st_ino, after.st_mode)
                        )
                        if kind == "file":
                            self.assertEqual(journal.read_bytes(), b"existing journal evidence")
                        self.assert_original()
                    finally:
                        journal.unlink(missing_ok=True)

    def test_cleanup_failure_does_not_mask_original_sync_error(self):
        self.fsync = os.fsync
        unlink = Path.unlink

        def deny(path, *args, **kwargs):
            if ".restore-" in path.name:
                raise OSError("cleanup denied")
            return unlink(path, *args, **kwargs)

        with (
            patch.object(persistence.os, "fsync", self.directory_failure),
            patch.object(Path, "unlink", deny),
            self.assertRaisesRegex(OSError, "directory sync failure"),
        ):
            self.restore()
        self.assert_original()
        for path in self.root.glob("*.restore-*"):
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_base_exception_during_snapshot_admission_still_cleans_private_copy(self):
        inspect = database_admin.inspect

        def interrupted(path):
            if ".restore-" in path.name:
                raise KeyboardInterrupt("injected interruption")
            return inspect(path)

        with (
            patch.object(database_admin, "inspect", interrupted),
            self.assertRaises(KeyboardInterrupt),
        ):
            self.restore()
        self.assert_original()
        self.assert_no_snapshot()

    def test_normal_backup_still_retains_complete_copy_on_directory_sync_failure(self):
        output = self.root / "output.sqlite3"
        self.fsync = os.fsync
        with (
            patch.object(persistence.os, "fsync", self.directory_failure),
            self.assertRaisesRegex(OSError, "directory sync failure"),
        ):
            StateDatabase.backup_readonly(self.source, output)
        self.assertEqual(self.value(output), 7)
        self.assertEqual(database_admin.inspect(output)["integrity"], "ok")
        self.assert_no_snapshot()
