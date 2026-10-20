"""Restore must durably preserve authority before acknowledging publication."""

import multiprocessing
import os
import shutil
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx import database_admin, persistence
from rp_ylx.persistence import PersistenceError, StateDatabase


class RestorePublicationTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.source = StateDatabase(self.root / "source.sqlite3")
        self.source.put("capture", {"revision": 7})
        self.destination = self.root / "state.sqlite3"
        self.store = StateDatabase(self.destination)
        self.store.put("capture", {"revision": 42})
        self.offline = patch.object(database_admin, "require_offline")
        self.offline.start()
        self.addCleanup(self.offline.stop)

    def restore(self):
        return database_admin.restore(self.source.path, self.destination)

    def archives(self):
        return list(self.root.glob("state.sqlite3.before-restore-*"))

    def phase(self):
        if self.archives():
            return "published" if self.destination.exists() else "archived"
        return "original"

    def revision(self, path):
        return StateDatabase(path).get("capture")["revision"]

    def assert_old_preserved(self):
        archives = self.archives()
        self.assertEqual(len(archives), 1)
        self.assertEqual(self.revision(archives[0]), 42)

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

    def test_new_marker_archives_and_publication_have_ordered_directory_barriers(self):
        self.store.marker.unlink()
        events = []
        fsync, rename, replace = os.fsync, Path.rename, Path.replace

        def sync(fd):
            mode = os.fstat(fd).st_mode
            if stat.S_ISDIR(mode):
                events.append("directory-" + self.phase())
            elif os.readlink(f"/proc/self/fd/{fd}") == str(self.store.marker):
                events.append("marker-data")
            return fsync(fd)

        def archive(path, target):
            if path == self.destination:
                events.append("archive")
            return rename(path, target)

        def publish(path, target):
            if target == self.destination:
                events.append("publish")
            return replace(path, target)

        with (
            patch.object(persistence.os, "fsync", sync),
            patch.object(Path, "rename", archive),
            patch.object(Path, "replace", publish),
        ):
            self.restore()
        marker = events.index("marker-data")
        archive_at, publish_at = events.index("archive"), events.index("publish")
        self.assertIn("directory-original", events[marker + 1 : archive_at])
        self.assertIn("directory-archived", events[archive_at + 1 : publish_at])
        self.assertIn("directory-published", events[publish_at + 1 :])
        self.assertEqual(self.revision(self.destination), 7)
        self.assert_old_preserved()

    def test_new_marker_directory_failure_leaves_original_authority_in_place(self):
        self.store.marker.unlink()
        marker_synced = False
        original = os.fsync

        def sync(fd):
            nonlocal marker_synced
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                if marker_synced:
                    raise OSError("marker directory failure")
            elif os.readlink(f"/proc/self/fd/{fd}") == str(self.store.marker):
                marker_synced = True
            return original(fd)

        with (
            patch.object(persistence.os, "fsync", sync),
            self.assertRaisesRegex(OSError, "marker directory failure"),
        ):
            self.restore()
        self.assertEqual(self.archives(), [])
        self.assertEqual(self.revision(self.destination), 42)
        self.assertFalse(list(self.root.glob("*.restore-*")))

    def test_archive_directory_failure_retains_old_data_and_blocks_empty_reinitialization(self):
        original = os.fsync

        def sync(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode) and self.phase() == "archived":
                raise OSError("archive directory failure")
            return original(fd)

        with (
            patch.object(persistence.os, "fsync", sync),
            self.assertRaisesRegex(OSError, "archive directory failure"),
        ):
            self.restore()
        self.assertFalse(self.destination.exists())
        self.assertTrue(self.store.marker.exists())
        with self.assertRaises(PersistenceError):
            StateDatabase(self.destination)
        self.assert_old_preserved()
        self.assertEqual(self.revision(self.source.path), 7)
        self.assertFalse(list(self.root.glob("*.restore-*")))

    def test_published_directory_failure_is_not_reported_as_success_or_rolled_back(self):
        original = os.fsync

        def sync(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode) and self.phase() == "published":
                raise OSError("published directory failure")
            return original(fd)

        with (
            patch.object(persistence.os, "fsync", sync),
            self.assertRaisesRegex(OSError, "published directory failure"),
        ):
            self.restore()
        self.assertEqual(self.revision(self.destination), 7)
        self.assert_old_preserved()
        self.assertEqual(self.revision(self.source.path), 7)

    def test_exit_after_archive_barrier_retains_recovery_and_authority_marker(self):
        def child():
            original = os.fsync

            def sync(fd):
                original(fd)
                if stat.S_ISDIR(os.fstat(fd).st_mode) and self.phase() == "archived":
                    os._exit(73)

            with patch.object(persistence.os, "fsync", sync):
                self.restore()
            os._exit(74)

        self.run_child(child, 73)
        self.assertTrue(self.store.marker.exists())
        self.assertFalse(self.destination.exists())
        with self.assertRaises(PersistenceError):
            StateDatabase(self.destination)
        self.assert_old_preserved()
        self.restore()
        self.assertEqual(self.revision(self.destination), 7)

    def test_exit_after_publish_barrier_keeps_new_database_and_old_recovery_copy(self):
        def child():
            original = os.fsync

            def sync(fd):
                original(fd)
                if stat.S_ISDIR(os.fstat(fd).st_mode) and self.phase() == "published":
                    os._exit(75)

            with patch.object(persistence.os, "fsync", sync):
                self.restore()
            os._exit(76)

        self.run_child(child, 75)
        self.assertEqual(self.revision(self.destination), 7)
        self.assert_old_preserved()

    def test_committed_old_wal_and_shm_are_preserved_byte_for_byte(self):
        def writer():
            old = StateDatabase(self.destination)
            with old.connection() as reader:
                reader.execute("PRAGMA wal_autocheckpoint=0")
                reader.execute("BEGIN")
                reader.execute("SELECT * FROM documents").fetchall()
                old.put("capture", {"revision": 99})
                os._exit(77)

        self.run_child(writer, 77)
        originals = {
            suffix: Path(str(self.destination) + suffix).read_bytes()
            for suffix in ("", "-wal", "-shm")
        }
        result = self.restore()
        self.assertEqual(len(result["preserved"]), 3)
        recovered = self.root / "recover-old"
        recovered.mkdir()
        for saved in map(Path, result["preserved"]):
            original_name = saved.name.split(".before-restore-", 1)[0]
            suffix = original_name.removeprefix(self.destination.name)
            self.assertEqual(saved.read_bytes(), originals[suffix])
            shutil.copy2(saved, recovered / original_name)
        self.assertEqual(self.revision(recovered / self.destination.name), 99)
        self.assertEqual(self.revision(self.destination), 7)
