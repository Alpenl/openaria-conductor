"""A backup's final name must only expose a complete copy owned by the caller."""

import multiprocessing
import os
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing, contextmanager
from pathlib import Path
from unittest.mock import patch

from rp_ylx import persistence


class BackupPublicationTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.store = persistence.StateDatabase(self.root / "state.sqlite3")
        self.store.put("capture", {"revision": 42})
        self.destination = self.root / "copy.sqlite3"

    @contextmanager
    def copy_hook(self, hook):
        original = self.store.connection

        class Source:
            def __init__(self, db):
                self.db = db

            def execute(self, *args, **kwargs):
                return self.db.execute(*args, **kwargs)

            def backup(self, *args, **kwargs):
                return hook(self.db, *args, **kwargs)

        @contextmanager
        def connection():
            with original() as db:
                yield Source(db)

        with patch.object(self.store, "connection", connection):
            yield

    def assert_copy(self):
        with closing(
            sqlite3.connect(self.destination.absolute().as_uri() + "?mode=ro", uri=True)
        ) as db:
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(
                db.execute("SELECT body FROM documents WHERE name='capture'").fetchone()[0],
                '{"revision":42}',
            )
        self.assertEqual(self.destination.stat().st_mode & 0o777, 0o600)

    def run_child(self, child, code):
        process = multiprocessing.get_context("fork").Process(target=child)
        process.start()
        try:
            process.join(10)
            self.assertEqual(process.exitcode, code)
        finally:
            if process.is_alive():
                process.kill()
            process.join(5)
            process.close()

    def test_final_name_is_absent_until_copy_is_complete(self):
        entered, release = threading.Event(), threading.Event()
        errors = []

        def hook(db, *args, **kwargs):
            entered.set()
            if not release.wait(5):
                raise TimeoutError("copy barrier not released")
            return db.backup(*args, **kwargs)

        def backup():
            try:
                self.store.backup(self.destination)
            except BaseException as error:
                errors.append(error)

        worker = threading.Thread(target=backup)
        try:
            with self.copy_hook(hook):
                worker.start()
                self.assertTrue(entered.wait(2))
                self.assertFalse(os.path.lexists(self.destination))
                release.set()
                worker.join(5)
                self.assertFalse(worker.is_alive())
                self.assertEqual(errors, [])
        finally:
            release.set()
            worker.join(6)
        self.assert_copy()

    def test_exit_before_copy_never_leaves_a_final_backup(self):
        def child():
            with patch.object(self.store, "connection", side_effect=lambda: os._exit(71)):
                self.store.backup(self.destination)
            os._exit(72)

        self.run_child(child, 71)
        self.assertFalse(os.path.lexists(self.destination))
        self.assertEqual(self.store.get("capture"), {"revision": 42})
        self.store.backup(self.destination)
        self.assert_copy()

    def test_competing_final_file_is_not_reported_as_our_backup(self):
        winner = b"other producer owns this file"

        def hook(db, *args, **kwargs):
            db.backup(*args, **kwargs)
            self.destination.unlink(missing_ok=True)
            self.destination.write_bytes(winner)

        with self.copy_hook(hook), self.assertRaises(FileExistsError):
            self.store.backup(self.destination)
        self.assertEqual(self.destination.read_bytes(), winner)
        self.assertFalse(list(self.root.glob(".*.backup-*.tmp")))

    def test_failure_cleanup_never_deletes_replacement_destination(self):
        winner = b"replacement belongs to another producer"

        def hook(db, *args, **kwargs):
            db.backup(*args, **kwargs)
            self.destination.unlink(missing_ok=True)
            self.destination.write_bytes(winner)
            raise RuntimeError("injected copy interruption")

        with self.copy_hook(hook), self.assertRaisesRegex(RuntimeError, "copy interruption"):
            self.store.backup(self.destination)
        self.assertTrue(self.destination.exists())
        self.assertEqual(self.destination.read_bytes(), winner)
        self.assertFalse(list(self.root.glob(".*.backup-*.tmp")))

    def test_existing_file_symlink_and_fifo_are_unchanged(self):
        target = self.root / "original"
        target.write_bytes(b"original bytes")
        for kind in ("file", "symlink", "fifo"):
            with self.subTest(kind=kind):
                if kind == "file":
                    self.destination.write_bytes(b"existing")
                elif kind == "symlink":
                    self.destination.symlink_to(target)
                else:
                    os.mkfifo(self.destination)
                before = self.destination.lstat()
                try:
                    with self.assertRaises(FileExistsError):
                        self.store.backup(self.destination)
                    after = self.destination.lstat()
                    self.assertEqual((before.st_ino, before.st_mode), (after.st_ino, after.st_mode))
                    self.assertEqual(target.read_bytes(), b"original bytes")
                finally:
                    self.destination.unlink()

    def test_directory_sync_failure_preserves_complete_published_copy(self):
        with (
            patch.object(persistence, "_sync_parent", side_effect=OSError("directory sync failed")),
            self.assertRaisesRegex(OSError, "directory sync failed"),
        ):
            self.store.backup(self.destination)
        self.assertTrue(self.destination.exists())
        self.assert_copy()
        self.assertFalse(list(self.root.glob(".*.backup-*.tmp")))
        with self.assertRaises(FileExistsError):
            self.store.backup(self.destination)

    def test_exit_after_publication_exposes_only_complete_data(self):
        def child():
            original = os.link

            def link_then_exit(*args, **kwargs):
                original(*args, **kwargs)
                os._exit(73)

            with patch.object(persistence.os, "link", link_then_exit):
                self.store.backup(self.destination)
            os._exit(74)

        self.run_child(child, 73)
        self.assert_copy()
        self.assertEqual(self.store.get("capture"), {"revision": 42})

    def test_file_sync_failure_has_no_final_copy_or_descriptor_leak(self):
        descriptors = len(os.listdir("/proc/self/fd"))
        with (
            patch.object(persistence.os, "fsync", side_effect=OSError("file sync failed")),
            self.assertRaisesRegex(OSError, "file sync failed"),
        ):
            self.store.backup(self.destination)
        self.assertFalse(os.path.lexists(self.destination))
        self.assertFalse(list(self.root.glob(".*.backup-*.tmp")))
        self.assertEqual(len(os.listdir("/proc/self/fd")), descriptors)
        self.store.backup(self.destination)
        self.assert_copy()

    def test_cleanup_failure_preserves_original_error_and_private_staging_only(self):
        def hook(db, *args, **kwargs):
            raise RuntimeError("original backup failure")

        with (
            self.copy_hook(hook),
            patch.object(Path, "unlink", side_effect=OSError("cleanup denied")),
            self.assertRaisesRegex(RuntimeError, "original backup failure"),
        ):
            self.store.backup(self.destination)
        self.assertFalse(os.path.lexists(self.destination))
        staging = list(self.root.glob(".*.backup-*.tmp"))
        self.assertEqual(len(staging), 1)
        self.assertEqual(staging[0].stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.store.get("capture"), {"revision": 42})
