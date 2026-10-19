import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx import database_admin
from rp_ylx.persistence import PersistenceError, StateDatabase
from rp_ylx.recording.catalog import SessionCatalog

TABLES = ("documents", "imports", "changes", "capture_commands", "transfers")


class DatabaseSchemaTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def database(self, name):
        return StateDatabase(self.root / (name + ".sqlite3"))

    def dump(self, path):
        db = sqlite3.connect(path.absolute().as_uri() + "?mode=ro", uri=True)
        try:
            return tuple(db.iterdump())
        finally:
            db.close()

    def test_missing_required_tables_are_rejected_without_recreation(self):
        for marker in (True, False):
            for table in TABLES:
                with self.subTest(marker=marker, table=table):
                    store = self.database(f"missing-{marker}-{table}")
                    store.initialize_document("device", {"fps": 60}, "bootstrap")
                    with store.connection() as db, db:
                        db.execute(f"DROP TABLE {table}")
                    if not marker:
                        store.marker.unlink()
                    before = self.dump(store.path)
                    with self.assertRaises(PersistenceError):
                        StateDatabase(store.path)
                    self.assertEqual(self.dump(store.path), before)
                    self.assertEqual(store.marker.exists(), marker)

    def test_invalid_table_shapes_are_rejected_without_repair(self):
        replacements = (
            "CREATE TABLE documents(name TEXT, body TEXT NOT NULL, "
            "revision INTEGER NOT NULL, updated_at TEXT NOT NULL)",
            "CREATE TABLE documents(name TEXT PRIMARY KEY, body INTEGER NOT NULL, "
            "revision INTEGER NOT NULL, updated_at TEXT NOT NULL)",
            "CREATE TABLE documents(name TEXT PRIMARY KEY, body TEXT, "
            "revision INTEGER NOT NULL, updated_at TEXT NOT NULL)",
            "CREATE TABLE documents(name TEXT PRIMARY KEY, body TEXT NOT NULL, "
            "revision INTEGER NOT NULL, updated_at TEXT NOT NULL, extra TEXT)",
            "CREATE VIEW documents AS SELECT 'capture' AS name, '{}' AS body, "
            "1 AS revision, 'now' AS updated_at",
        )
        for index, statement in enumerate(replacements):
            with self.subTest(statement=statement):
                store = self.database(f"shape-{index}")
                with store.connection() as db, db:
                    db.execute("DROP TABLE documents")
                    db.execute(statement)
                before = self.dump(store.path)
                with self.assertRaises(PersistenceError):
                    StateDatabase(store.path)
                self.assertEqual(self.dump(store.path), before)

    def test_version_zero_with_existing_schema_is_not_completed_implicitly(self):
        store = self.database("version-zero")
        with store.connection() as db, db:
            db.execute("PRAGMA user_version=0")
        store.marker.unlink()
        before = self.dump(store.path)
        with self.assertRaises(PersistenceError):
            StateDatabase(store.path)
        self.assertEqual(self.dump(store.path), before)
        self.assertFalse(store.marker.exists())

    def test_missing_marker_after_committed_schema_is_recoverable(self):
        store = self.database("committed")
        store.put("capture", {"source_revision": 42})
        store.marker.unlink()
        before = self.dump(store.path)
        reopened = StateDatabase(store.path)
        self.assertEqual(reopened.get("capture"), {"source_revision": 42})
        self.assertEqual(self.dump(store.path), before)
        self.assertTrue(store.marker.exists())

    def test_empty_interrupted_creation_can_initialize(self):
        path = self.root / "empty.sqlite3"
        path.touch()
        store = StateDatabase(path)
        store.initialize_document("device", {"fps": 60}, "bootstrap")
        self.assertEqual(store.get("device"), {"fps": 60})
        self.assertEqual(database_admin.inspect(path)["integrity"], "ok")

    def test_extra_catalog_tables_remain_supported(self):
        catalog = SessionCatalog(self.root / "catalog.sqlite3")
        catalog.put("probe", {"ok": True})
        before = self.dump(catalog.path)
        self.assertEqual(StateDatabase(catalog.path).get("probe"), {"ok": True})
        SessionCatalog(catalog.path)
        self.assertEqual(self.dump(catalog.path), before)
        self.assertEqual(database_admin.inspect(catalog.path)["integrity"], "ok")

    def test_admin_check_and_restore_reject_incomplete_backups_before_mutation(self):
        target = self.database("state")
        target.put("capture", {"source_revision": 42})
        original = self.dump(target.path)
        for table in ("imports", "changes", "capture_commands", "transfers"):
            with self.subTest(table=table):
                source = self.database("source-" + table)
                source.put("capture", {"source_revision": 1})
                with source.connection() as db, db:
                    db.execute(f"DROP TABLE {table}")
                before = self.dump(source.path)
                with self.assertRaises(PersistenceError):
                    database_admin.inspect(source.path)
                with (
                    patch.object(database_admin, "require_offline"),
                    self.assertRaises(PersistenceError),
                ):
                    database_admin.restore(source.path, target.path)
                self.assertEqual(self.dump(source.path), before)
                self.assertEqual(self.dump(target.path), original)
                self.assertFalse(list(self.root.glob("*.before-restore-*")))
                self.assertFalse(list(self.root.glob("*.restore-*")))
