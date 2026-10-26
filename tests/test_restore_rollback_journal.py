"""Archive rollback journals before a restored database can replay old pages."""

import json
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


class RestoreRollbackJournalTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.source = self.root / "source.sqlite3"
        self.target = self.root / "state.sqlite3"
        source = StateDatabase(self.source)
        source.put("capture", {"revision": 7})
        target = StateDatabase(self.target)
        target.put("capture", {"revision": 42})
        for index in range(32):
            target.put(f"filler-{index}", {"revision": 42, "payload": "x" * 4096})
        with closing(sqlite3.connect(self.target)) as db:
            self.assertEqual(db.execute("PRAGMA journal_mode=DELETE").fetchone()[0], "delete")
        self.journal = Path(str(self.target) + "-journal")
        self.offline = patch.object(database_admin, "require_offline")
        self.offline.start()
        self.addCleanup(self.offline.stop)

    def child(self, run, expected):
        process = multiprocessing.get_context("fork").Process(target=run)
        process.start()
        try:
            process.join(10)
            self.assertEqual(process.exitcode, expected)
        finally:
            if process.is_alive():
                process.kill()
            process.join(5)
            process.close()

    def dirty_transaction(self):
        def write_and_exit():
            db = sqlite3.connect(self.target)
            db.execute("PRAGMA cache_size=1")
            db.execute("PRAGMA cache_spill=ON")
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE documents SET body=?",
                (json.dumps({"revision": 99, "payload": "y" * 4096}),),
            )
            os._exit(77)

        self.child(write_and_exit, 77)
        self.assertGreater(self.journal.stat().st_size, 512)
        self.assertEqual(self.journal.read_bytes()[:8].hex(), "d9d505f920a163d7")
        return {"": self.target.read_bytes(), "-journal": self.journal.read_bytes()}

    def restore(self):
        return database_admin.restore(self.source, self.target)

    def value(self, path):
        # RW is intentional for the reconstructed control copy: SQLite must
        # actually roll back the uncommitted transaction, not ignore its log.
        with closing(sqlite3.connect(path)) as db:
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            return json.loads(
                db.execute("SELECT body FROM documents WHERE name='capture'").fetchone()[0]
            )["revision"]

    def recover_original(self, originals, label):
        root = self.root / label
        root.mkdir()
        for suffix, raw in originals.items():
            (root / (self.target.name + suffix)).write_bytes(raw)
        self.assertEqual(self.value(root / self.target.name), 42)

    def archived_bytes(self):
        result = {}
        for suffix in ("", "-journal"):
            matches = list(self.root.glob(self.target.name + suffix + ".before-restore-*"))
            self.assertEqual(len(matches), 1, (suffix, matches))
            result[suffix] = matches[0].read_bytes()
        return result

    def test_hot_journal_is_preserved_with_old_main_and_never_replayed_on_new_database(self):
        originals = self.dirty_transaction()
        self.recover_original(originals, "control")
        receipt = self.restore()
        self.assertEqual(self.value(self.target), 7)
        self.assertEqual(len(receipt["preserved"]), 2)
        self.assertFalse(self.journal.exists())
        self.assertEqual(self.archived_bytes(), originals)
        self.recover_original(self.archived_bytes(), "recovered")

    def test_leftover_hot_journal_with_missing_main_is_archived_before_publication(self):
        originals = self.dirty_transaction()
        self.target.unlink()
        with self.assertRaises(PersistenceError):
            StateDatabase(self.target)
        receipt = self.restore()
        self.assertEqual(self.value(self.target), 7)
        self.assertEqual(len(receipt["preserved"]), 1)
        saved = Path(receipt["preserved"][0])
        self.assertTrue(saved.name.startswith(self.journal.name + ".before-restore-"))
        self.assertEqual(saved.read_bytes(), originals["-journal"])
        self.recover_original({"": originals[""], "-journal": saved.read_bytes()}, "recovered")

    def test_inactive_and_zero_length_journals_are_preserved_without_interpretation(self):
        for raw in (b"", b"inactive journal evidence"):
            with self.subTest(raw=raw):
                self.journal.write_bytes(raw)
                receipt = self.restore()
                journals = [
                    Path(name)
                    for name in receipt["preserved"]
                    if "-journal.before-restore-" in name
                ]
                self.assertEqual(len(journals), 1)
                self.assertEqual(journals[0].read_bytes(), raw)
                self.assertFalse(self.journal.exists())
                self.assertEqual(self.value(self.target), 7)

    def test_journal_archive_failure_does_not_publish_and_retry_keeps_old_material(self):
        originals = self.dirty_transaction()
        rename = Path.rename

        def fail(path, destination):
            if path == self.journal:
                raise OSError("journal archive denied")
            return rename(path, destination)

        with (
            patch.object(Path, "rename", fail),
            self.assertRaisesRegex(OSError, "journal archive denied"),
        ):
            self.restore()
        self.assertFalse(self.target.exists())
        self.assertTrue(self.target.with_suffix(".sqlite3.initialized").exists())
        self.assertEqual(self.journal.read_bytes(), originals["-journal"])
        self.assertFalse(list(self.root.glob("*.restore-*")))
        self.restore()
        self.assertEqual(self.value(self.target), 7)
        self.assertEqual(self.archived_bytes(), originals)
        self.recover_original(self.archived_bytes(), "recovered")

    def test_exit_after_archive_barrier_keeps_both_recovery_files_and_allows_retry(self):
        originals = self.dirty_transaction()

        def interrupted():
            sync = database_admin._sync_parent

            def after_sync(path):
                sync(path)
                if list(self.root.glob(self.journal.name + ".before-restore-*")):
                    os._exit(79)

            with patch.object(database_admin, "_sync_parent", after_sync):
                self.restore()
            os._exit(80)

        self.child(interrupted, 79)
        self.assertFalse(self.target.exists())
        self.assertTrue(self.target.with_suffix(".sqlite3.initialized").exists())
        self.assertEqual(self.archived_bytes(), originals)
        self.recover_original(self.archived_bytes(), "recovered")
        self.restore()
        self.assertEqual(self.value(self.target), 7)
