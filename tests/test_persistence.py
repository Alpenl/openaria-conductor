from __future__ import annotations

import random
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from jsonschema import Draft202012Validator

from rp_ylx.api.downloads import _ManifestValidator
from rp_ylx.persistence import PersistenceError, StateDatabase
from rp_ylx.recording.catalog import SessionCatalog
from rp_ylx.settings import effective, extract, overlay


class PersistenceTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.path = self.root / "state.sqlite3"

    def test_import_once_retains_exact_legacy_and_missing_authority_fails_closed(self):
        legacy = self.root / "old.json"
        legacy.write_bytes(b'{"answer": 42}\n')
        store = StateDatabase(self.path)
        self.assertEqual(store.import_json("state", legacy), {"answer": 42})
        self.assertEqual(legacy.with_suffix(".json.pre-sqlite").read_bytes(), legacy.read_bytes())
        legacy.write_text('{"answer": 0}')
        self.assertEqual(StateDatabase(self.path).import_json("state", legacy), {"answer": 42})
        self.path.unlink()
        with self.assertRaises(PersistenceError):
            StateDatabase(self.path)

    def test_missing_bootstrap_document_does_not_reset_settings(self):
        store = StateDatabase(self.path)
        store.initialize_document("device", {"v": 1}, "bootstrap")
        with store.connection() as db, db:
            db.execute("DELETE FROM documents")
        with self.assertRaises(PersistenceError):
            store.initialize_document("device", {"v": 0}, "bootstrap")

    def test_restore_preserves_previous_authority_and_requires_offline(self):
        from rp_ylx.database_admin import restore

        store = StateDatabase(self.path)
        store.put("capture", {"revision": 1})
        backup = self.root / "backup.sqlite3"
        store.backup(backup)
        store.put("capture", {"revision": 2})
        with (
            patch("rp_ylx.database_admin.require_offline", side_effect=PersistenceError("active")),
            self.assertRaises(PersistenceError),
        ):
            restore(backup, self.path)
        self.assertEqual(store.get("capture"), {"revision": 2})
        with patch("rp_ylx.database_admin.require_offline"):
            report = restore(backup, self.path)
        self.assertEqual(StateDatabase(self.path).get("capture"), {"revision": 1})
        self.assertTrue(report["preserved"])
        self.assertEqual(
            StateDatabase(Path(report["preserved"][0])).get("capture"), {"revision": 2}
        )

    def test_concurrent_initializers_share_one_schema(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            stores = list(pool.map(lambda _: StateDatabase(self.path), range(8)))
        stores[0].put("value", {"n": 1})
        self.assertTrue(all(s.get("value") == {"n": 1} for s in stores))

    def test_corrupt_foreign_and_future_databases_are_rejected(self):
        for kind in ("corrupt", "foreign", "future", "truncated"):
            with self.subTest(kind=kind):
                path = self.root / (kind + ".sqlite3")
                if kind == "corrupt":
                    path.write_bytes(b"not a sqlite database")
                elif kind == "foreign":
                    with closing(sqlite3.connect(path)) as db, db:
                        db.execute("CREATE TABLE unrelated(x)")
                else:
                    StateDatabase(path)
                    if kind == "future":
                        with closing(sqlite3.connect(path)) as db, db:
                            db.execute("PRAGMA user_version=99")
                    else:
                        path.write_bytes(b"")
                with self.assertRaises(PersistenceError):
                    StateDatabase(path)

    def test_transactional_updates_no_lost_writes_and_validation_rolls_back(self):
        store = StateDatabase(self.path)
        store.initialize_document("counter", {"n": 0}, "test")

        def increment(_):
            store.update("counter", lambda value: {"n": value["n"] + 1})

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(increment, range(80)))
        self.assertEqual(store.get("counter"), {"n": 80})

        def invalid(value):
            value["n"] = 999
            raise ValueError("invalid")

        with self.assertRaises(ValueError):
            store.update("counter", invalid)
        self.assertEqual(store.get("counter"), {"n": 80})
        with store.connection() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM changes").fetchone()[0], 81)

    def test_online_backup_includes_committed_wal_and_preserves_target(self):
        store = StateDatabase(self.path)
        with store.connection() as reader:
            reader.execute("PRAGMA wal_autocheckpoint=0")
            reader.execute("BEGIN")
            reader.execute("SELECT * FROM documents").fetchall()
            store.put("state", {"n": 7})
            self.assertGreater(Path(str(self.path) + "-wal").stat().st_size, 0)
            backup = self.root / "backup.sqlite3"
            store.backup(backup)
            self.assertEqual(StateDatabase(backup).get("state"), {"n": 7})
            with self.assertRaises(FileExistsError):
                store.backup(backup)
        self.assertEqual(backup.stat().st_mode & 0o777, 0o600)

    def test_invalid_import_does_not_copy_secrets(self):
        legacy = self.root / "old.json"
        legacy.write_text('{"secret":"do-not-import"}')
        store = StateDatabase(self.path)

        def reject(_):
            raise ValueError("rejected")

        with self.assertRaises(ValueError):
            store.import_json("state", legacy, validate=reject)
        self.assertIsNone(store.get("state"))
        self.assertFalse(legacy.with_suffix(".json.pre-sqlite").exists())

    def test_settings_overlay_preserves_bootstrap_and_rejects_path_mutation(self):
        base = {"state_root": str(self.root), "camera": {"device": "/dev/video0", "fps": 30}}
        self.assertEqual(extract(base), {"camera": {"fps": 30}})
        self.assertEqual(overlay(base, {"camera": {"fps": 25}})["camera"]["device"], "/dev/video0")
        with self.assertRaises(PersistenceError):
            overlay(base, {"camera": {"device": "/tmp/wrong"}})
        self.assertEqual(effective(base), base)
        store = StateDatabase(self.root / "settings.sqlite3")
        store.initialize_document("device", {"camera": {"fps": 25}}, "test")
        self.assertEqual(effective(base)["camera"]["fps"], 25)

    def test_catalog_restart_invalidation_and_historical_proof(self):
        directory = self.root / "session"
        directory.mkdir()
        manifest = directory / "manifest.json"
        manifest.write_text("{}")
        store = SessionCatalog(self.root / "catalog.sqlite3")
        signature = store.signature(directory)
        summary = {
            "session_id": "session",
            "started_at": "2026",
            "take_id": "take",
            "verification": {
                "verdict": "usable",
                "manifest_sha256": "a" * 64,
                "verified_at": "2026",
            },
        }
        store.remember("volume", directory, "commit", summary, {}, expected_signature=signature)
        reopened = SessionCatalog(store.path)
        self.assertIsNone(reopened.cached("volume", "session", signature, "commit")["verification"])
        self.assertIsNone(reopened.cached("other-volume", "session", signature, "commit"))
        self.assertIsNone(reopened.cached("volume", "session", signature, "new-commit"))
        manifest.write_text("[]")
        self.assertIsNone(
            reopened.cached("volume", "session", store.signature(directory), "commit")
        )
        with self.assertRaises(OSError):
            store.remember("volume", directory, "commit", summary, {}, expected_signature=signature)
        reopened.discard_missing("volume", set())
        self.assertEqual(reopened.summaries("volume"), [])


class ManifestUniquenessTest(unittest.TestCase):
    def test_json_schema_equality_matches_reference(self):
        baseline = Draft202012Validator({"uniqueItems": True})
        optimized = _ManifestValidator({"uniqueItems": True})
        values = [
            None,
            True,
            False,
            0,
            1,
            1.0,
            -0.0,
            "1",
            [],
            {},
            [1],
            [True],
            {"a": 1, "b": [2]},
            {"b": [2.0], "a": 1.0},
            {"a": True},
        ]
        for left in values:
            for right in values:
                case = [left, right]
                self.assertEqual(optimized.is_valid(case), baseline.is_valid(case), repr(case))
        generator = random.Random(42)
        for _ in range(100):
            case = generator.choices(values, k=10)
            self.assertEqual(optimized.is_valid(case), baseline.is_valid(case))
