"""Validate the actual restore snapshot before moving existing authority."""

import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from rp_ylx import database_admin
from rp_ylx.persistence import APPLICATION_ID, SCHEMA_VERSION, PersistenceError, StateDatabase


class RestoreAdmissionTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.source = StateDatabase(self.root / "source.sqlite3")
        for name in ("capture", "controller", "device"):
            self.source.put(name, {"revision": 7})
        self.offline = patch.object(database_admin, "require_offline")
        self.offline.start()
        self.addCleanup(self.offline.stop)

    def target(self, filename, document):
        store = StateDatabase(self.root / filename)
        store.put(document, {"revision": 42})
        return store

    def assert_unchanged(self, target, document):
        self.assertEqual(target.get(document), {"revision": 42})
        self.assertTrue(target.marker.exists())
        self.assertFalse(list(self.root.glob("*.before-restore-*")))
        self.assertFalse(list(self.root.glob("*.restore-*")))

    def test_required_document_removed_after_source_inspect_is_rejected_before_mutation(self):
        inspect = database_admin.inspect
        for filename, document in (
            ("state.sqlite3", "capture"),
            ("controller.sqlite3", "controller"),
            ("settings.sqlite3", "device"),
        ):
            with self.subTest(filename=filename):
                target = self.target(filename, document)
                self.source.put(document, {"revision": 7})
                changed = False

                def inspect_and_change(path, document=document):
                    nonlocal changed
                    result = inspect(path)
                    if path == self.source.path and not changed:
                        with self.source.connection() as db, db:
                            db.execute("DELETE FROM documents WHERE name=?", (document,))
                        changed = True
                    return result

                with (
                    patch.object(database_admin, "inspect", inspect_and_change),
                    self.assertRaisesRegex(PersistenceError, "required authoritative document"),
                ):
                    database_admin.restore(self.source.path, target.path)
                self.assertTrue(changed)
                self.assert_unchanged(target, document)

    def test_schema_damage_in_actual_copy_is_rejected_before_archival(self):
        target = self.target("state.sqlite3", "capture")
        original = StateDatabase.backup_readonly

        def corrupted_backup(source, destination):
            original(source, destination)
            with closing(sqlite3.connect(destination)) as db, db:
                db.execute("DROP TABLE transfers")

        with (
            patch.object(StateDatabase, "backup_readonly", corrupted_backup),
            self.assertRaises(PersistenceError),
        ):
            database_admin.restore(self.source.path, target.path)
        self.assert_unchanged(target, "capture")

    def test_wrong_identity_of_actual_copy_is_rejected_before_archival(self):
        target = self.target("state.sqlite3", "capture")
        original = StateDatabase.backup_readonly
        for pragma, value in (
            ("application_id", APPLICATION_ID + 1),
            ("user_version", SCHEMA_VERSION + 1),
        ):
            with self.subTest(pragma=pragma):

                def wrong_identity(source, destination, pragma=pragma, value=value):
                    original(source, destination)
                    with closing(sqlite3.connect(destination)) as db, db:
                        db.execute(f"PRAGMA {pragma}={value}")

                with (
                    patch.object(StateDatabase, "backup_readonly", wrong_identity),
                    self.assertRaises(PersistenceError),
                ):
                    database_admin.restore(self.source.path, target.path)
                self.assert_unchanged(target, "capture")

    def test_copy_inspection_io_failure_keeps_original_and_cleans_staging(self):
        target = self.target("state.sqlite3", "capture")
        original = database_admin.inspect

        def inspect(path):
            if ".restore-" in path.name:
                raise OSError("injected copy read failure")
            return original(path)

        with (
            patch.object(database_admin, "inspect", inspect),
            self.assertRaisesRegex(OSError, "copy read failure"),
        ):
            database_admin.restore(self.source.path, target.path)
        self.assert_unchanged(target, "capture")

    def test_valid_changed_snapshot_is_accepted_and_reported(self):
        target = self.target("state.sqlite3", "capture")
        original = database_admin.inspect
        changed = False

        def inspect(path):
            nonlocal changed
            result = original(path)
            if path == self.source.path and not changed:
                self.source.put("capture", {"revision": 8})
                self.source.put("new_config", {"enabled": True})
                changed = True
            return result

        with patch.object(database_admin, "inspect", inspect):
            result = database_admin.restore(self.source.path, target.path)
        self.assertEqual(target.get("capture"), {"revision": 8})
        self.assertIn("new_config", {entry["name"] for entry in result["documents"]})
        self.assertEqual(
            StateDatabase(Path(result["preserved"][0])).get("capture"), {"revision": 42}
        )

    def test_catalog_copy_without_capture_document_remains_supported(self):
        target = self.target("catalog.sqlite3", "catalog-probe")
        with self.source.connection() as db, db:
            db.execute("DELETE FROM documents")
        result = database_admin.restore(self.source.path, target.path)
        self.assertEqual(result["documents"], [])
        self.assertIsNone(target.get("catalog-probe"))
        self.assertEqual(
            StateDatabase(Path(result["preserved"][0])).get("catalog-probe"), {"revision": 42}
        )

    def test_cleanup_failure_does_not_hide_original_copy_inspection_error(self):
        target = self.target("state.sqlite3", "capture")
        original_inspect = database_admin.inspect
        original_unlink = Path.unlink

        def inspect(path):
            if ".restore-" in path.name:
                raise RuntimeError("original inspection failure")
            return original_inspect(path)

        def unlink(path, *args, **kwargs):
            if ".restore-" in path.name and ".backup-" not in path.name:
                raise OSError("cleanup denied")
            return original_unlink(path, *args, **kwargs)

        with (
            patch.object(database_admin, "inspect", inspect),
            patch.object(Path, "unlink", unlink),
            self.assertRaisesRegex(RuntimeError, "original inspection failure"),
        ):
            database_admin.restore(self.source.path, target.path)
        self.assertEqual(target.get("capture"), {"revision": 42})
        self.assertFalse(list(self.root.glob("*.before-restore-*")))
        staging = list(self.root.glob("*.restore-*"))
        self.assertEqual(len(staging), 1)
        self.assertEqual(staging[0].stat().st_mode & 0o777, 0o600)
