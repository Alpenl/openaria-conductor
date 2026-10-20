"""Database restores must not interleave archival and publication."""

import json
import multiprocessing
import os
import re
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from rp_ylx import database_admin
from rp_ylx.persistence import PersistenceError, StateDatabase


class RestoreConcurrencyTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.target = self.root / "state.sqlite3"
        StateDatabase(self.target).put("capture", {"revision": 42})
        self.sources = []
        for revision in (7, 99):
            path = self.root / f"source-{revision}.sqlite3"
            StateDatabase(path).put("capture", {"revision": revision})
            self.sources.append(path)
        self.offline = patch.object(database_admin, "require_offline")
        self.offline.start()
        self.addCleanup(self.offline.stop)

    def value(self, path):
        with closing(sqlite3.connect(path.absolute().as_uri() + "?mode=ro", uri=True)) as db:
            body = db.execute("SELECT body FROM documents WHERE name='capture'").fetchone()[0]
            return json.loads(body)["revision"]

    def archives(self):
        return sorted(
            self.value(p)
            for p in self.root.glob("state.sqlite3.before-restore-*")
            if re.fullmatch(r"state\.sqlite3\.before-restore-[0-9a-f]{32}", p.name)
        )

    def gated_restore(self):
        context = multiprocessing.get_context("fork")
        ready, resume = context.Event(), context.Event()

        def run():
            original = Path.replace

            def publish(path, destination):
                if destination == self.target:
                    ready.set()
                    if not resume.wait(15):
                        raise RuntimeError("restore test gate timed out")
                return original(path, destination)

            with patch.object(Path, "replace", publish):
                database_admin.restore(self.sources[0], self.target)

        process = context.Process(target=run)
        process.start()

        def cleanup():
            if process.is_alive():
                resume.set()
            process.join(5)
            if process.is_alive():
                process.kill()
            process.join(5)
            process.close()

        self.addCleanup(cleanup)
        self.assertTrue(ready.wait(10), "restore did not reach publication")
        return process, resume

    def test_competing_restore_is_rejected_then_retry_preserves_each_previous_database(self):
        process, resume = self.gated_restore()
        with self.assertRaisesRegex(PersistenceError, "restore.*busy"):
            database_admin.restore(self.sources[1], self.target)
        self.assertFalse(self.target.exists())
        self.assertEqual(self.archives(), [42])
        resume.set()
        process.join(10)
        self.assertEqual(process.exitcode, 0)
        self.assertEqual(self.value(self.target), 7)
        database_admin.restore(self.sources[1], self.target)
        self.assertEqual(self.value(self.target), 99)
        self.assertEqual(self.archives(), [7, 42])

    def test_busy_restore_does_not_read_source_or_create_staging(self):
        process, resume = self.gated_restore()
        before = set(self.root.iterdir())
        with (
            patch.object(database_admin, "inspect", side_effect=AssertionError("source was read")),
            self.assertRaisesRegex(PersistenceError, "restore.*busy"),
        ):
            database_admin.restore(self.sources[1], self.target)
        self.assertEqual(set(self.root.iterdir()), before)
        resume.set()
        process.join(10)
        self.assertEqual(process.exitcode, 0)

    def test_killed_restore_releases_lock_and_retry_preserves_original_archive(self):
        process, _resume = self.gated_restore()
        process.kill()
        process.join(10)
        self.assertEqual(process.exitcode, -9)
        self.assertFalse(self.target.exists())
        self.assertTrue(self.target.with_suffix(".sqlite3.initialized").exists())
        database_admin.restore(self.sources[1], self.target)
        self.assertEqual(self.value(self.target), 99)
        self.assertEqual(self.archives(), [42])

    def test_failed_copy_releases_lock_for_same_target_retry(self):
        with (
            patch.object(StateDatabase, "readonly_snapshot", side_effect=OSError("copy denied")),
            self.assertRaisesRegex(OSError, "copy denied"),
        ):
            database_admin.restore(self.sources[0], self.target)
        self.assertEqual(self.value(self.target), 42)
        database_admin.restore(self.sources[1], self.target)
        self.assertEqual(self.value(self.target), 99)
        self.assertEqual(self.archives(), [42])

    def test_different_destination_is_not_blocked_by_running_restore(self):
        process, resume = self.gated_restore()
        second = self.root / "second.sqlite3"
        StateDatabase(second).put("capture", {"revision": 21})
        database_admin.restore(self.sources[1], second)
        self.assertEqual(self.value(second), 99)
        self.assertFalse(self.target.exists())
        resume.set()
        process.join(10)
        self.assertEqual(process.exitcode, 0)
        self.assertEqual(self.value(self.target), 7)

    def test_linked_or_nonregular_restore_lock_fails_before_mutation(self):
        lock = self.target.with_suffix(".sqlite3.restore.lock")
        outside = self.root / "outside"
        outside.write_bytes(b"do not modify")
        for kind in ("symlink", "fifo", "directory"):
            with self.subTest(kind=kind):
                if kind == "symlink":
                    lock.symlink_to(outside)
                elif kind == "fifo":
                    os.mkfifo(lock)
                else:
                    lock.mkdir()
                try:
                    with self.assertRaises((OSError, PersistenceError)):
                        database_admin.restore(self.sources[0], self.target)
                    self.assertEqual(self.value(self.target), 42)
                    self.assertEqual(outside.read_bytes(), b"do not modify")
                    self.assertEqual(self.archives(), [])
                    self.assertFalse(list(self.root.glob("*.restore-*")))
                finally:
                    if kind == "directory":
                        lock.rmdir()
                    else:
                        lock.unlink()

    def test_offline_failure_releases_lock_without_mutating_database(self):
        with (
            patch.object(database_admin, "require_offline", side_effect=PersistenceError("online")),
            self.assertRaisesRegex(PersistenceError, "online"),
        ):
            database_admin.restore(self.sources[0], self.target)
        self.assertEqual(self.value(self.target), 42)
        self.assertEqual(self.archives(), [])
        database_admin.restore(self.sources[0], self.target)
        self.assertEqual(self.value(self.target), 7)
