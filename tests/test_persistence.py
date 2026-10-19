from __future__ import annotations

import json
import os
import random
import sqlite3
import subprocess
import sys
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

    def test_updater_rejects_legacy_wheel_after_sqlite_migration(self):
        import zipfile

        from rp_ylx.update import require_sqlite_wheel

        wheel = self.root / "legacy.whl"
        with zipfile.ZipFile(wheel, "w") as archive:
            archive.writestr("rp_ylx/daemon.py", "")
        with (
            patch("rp_ylx.update.sqlite_state_required", return_value=True),
            self.assertRaises(ValueError),
        ):
            require_sqlite_wheel(wheel)
        with zipfile.ZipFile(wheel, "a") as archive:
            archive.writestr("rp_ylx/persistence.py", "SCHEMA_VERSION = 1")
        with patch("rp_ylx.update.sqlite_state_required", return_value=True):
            require_sqlite_wheel(wheel)

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

    def test_process_initializers_share_one_schema(self):
        code = (
            "import sys; from pathlib import Path; from rp_ylx.persistence import StateDatabase; "
            "StateDatabase(Path(sys.argv[1])).put(sys.argv[2], {'ok':True})"
        )
        processes = [
            subprocess.Popen([sys.executable, "-c", code, str(self.path), str(i)]) for i in range(8)
        ]
        for process in processes:
            self.assertEqual(process.wait(timeout=20), 0)
        store = StateDatabase(self.path)
        self.assertTrue(all(store.get(str(i)) == {"ok": True} for i in range(8)))

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

    def test_transfer_receipts_distinguish_interrupted_and_served(self):
        store = StateDatabase(self.path)
        interrupted = store.begin_transfer("volume", "session", "first")
        served = store.begin_transfer("volume", "session", "second")
        store.finish_transfer(served, "served", 100)
        store.interrupt_transfers()
        with store.connection() as db:
            rows = dict(db.execute("SELECT id,status FROM transfers"))
        self.assertEqual(rows, {interrupted: "interrupted", served: "served"})
        self.assertNotIn("backed_up", rows.values())

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

    def test_settings_patch_validates_and_survives_bootstrap_change(self):
        from rp_ylx.daemon import ProductionConfigError, load_production_config
        from rp_ylx.settings import manage

        base = {
            "schema": "ylx.production-config.v1",
            "listen": {"host": "127.0.0.1", "port": 8080},
            "camera": {
                "device": "/dev/video0",
                "width": 3840,
                "height": 1080,
                "fps": 60,
                "data_plane": "rust",
            },
            "storage": {
                "mountpoint": str(self.root / "data"),
                "minimum_available_bytes": 100,
                "minimum_available_inodes": 10,
            },
            "state_root": str(self.root),
            "device": {
                "device_id": "11111111-1111-4111-8111-111111111111",
                "device_label": "YLX-12AB34CD",
                "hardware_fingerprint": "sha256:" + "a" * 64,
            },
            "security": {"profile": "lab", "isolated_network": True},
        }
        config = self.root / "device.json"
        config.write_text(json.dumps(base))
        patchfile = self.root / "patch.json"
        patchfile.write_text(json.dumps({"storage": {"minimum_available_bytes": 200}}))
        manage(config, "set", source=patchfile)
        self.assertEqual(manage(config, "show")["settings"]["recording"]["codec"], "hevc")
        self.assertEqual(manage(config, "show")["settings"]["recording"]["bitrate_kbps"], 12288)
        base["storage"]["minimum_available_bytes"] = 999
        config.write_text(json.dumps(base))
        self.assertEqual(load_production_config(config).minimum_available_bytes, 200)
        patchfile.write_text(json.dumps({"camera": {"fps": -1}}))
        with self.assertRaises(ProductionConfigError):
            manage(config, "set", source=patchfile)
        self.assertEqual(load_production_config(config).fps, 60)

    def test_catalog_signature_rejects_linked_and_oversized_manifests(self):
        directory = self.root / "signature-boundaries"
        directory.mkdir()
        manifest = directory / "manifest.json"
        source = self.root / "source.json"
        source.write_text("{}")
        before = len(os.listdir("/proc/self/fd"))
        manifest.symlink_to(source)
        with self.assertRaises(OSError):
            SessionCatalog.signature(directory)
        manifest.unlink()
        os.link(source, manifest)
        with self.assertRaises(OSError):
            SessionCatalog.signature(directory)
        manifest.unlink()
        with manifest.open("wb") as stream:
            stream.truncate(16 * 1024 * 1024 + 1)
        with self.assertRaises(OSError):
            SessionCatalog.signature(directory)
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)

    def test_catalog_signature_rejects_fifo_without_waiting_for_writer(self):
        directory = self.root / "fifo-manifest"
        directory.mkdir()
        os.mkfifo(directory / "manifest.json")
        code = (
            "from pathlib import Path; import sys; "
            "from rp_ylx.recording.catalog import SessionCatalog; "
            "SessionCatalog.signature(Path(sys.argv[1]))"
        )
        result = subprocess.run(
            [sys.executable, "-c", code, str(directory)],
            capture_output=True,
            text=True,
            timeout=5,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("bounded exclusively linked file", result.stderr)

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

    def _catalog_entry(self, sid):
        directory = self.root / sid
        directory.mkdir()
        (directory / "manifest.json").write_text("{}")
        summary = {"session_id": sid, "started_at": "2026", "take_id": "take", "verification": None}
        return directory, summary

    def test_catalog_scan_has_one_durable_commit_and_no_partial_visibility(self):
        store = SessionCatalog(self.root / "catalog.sqlite3")
        observer = SessionCatalog(store.path)
        entries = [self._catalog_entry(f"session-{i}") for i in range(20)]
        statements = []
        connect = sqlite3.connect

        def traced_connect(*args, **kwargs):
            db = connect(*args, **kwargs)
            db.set_trace_callback(statements.append)
            return db

        with patch("sqlite3.connect", side_effect=traced_connect), store.scan():
            for directory, summary in entries:
                store.remember("volume", directory, "commit", summary, {})
                self.assertIsNotNone(
                    store.cached("volume", directory.name, store.signature(directory), "commit")
                )
            store.discard_missing("volume", {d.name for d, _ in entries})
            with ThreadPoolExecutor(max_workers=1) as pool:
                self.assertEqual(pool.submit(observer.summaries, "volume").result(timeout=2), [])
        self.assertEqual(statements.count("COMMIT"), 1)
        self.assertEqual(statements.count("BEGIN IMMEDIATE"), 1)
        self.assertIn("PRAGMA synchronous=FULL", statements)
        self.assertEqual(len(observer.summaries("volume")), 20)

    def test_catalog_scan_failure_rolls_back_and_releases_connection(self):
        store = SessionCatalog(self.root / "catalog.sqlite3")
        directory, summary = self._catalog_entry("rollback")
        with self.assertRaisesRegex(RuntimeError, "scan failed"), store.scan():
            store.remember("volume", directory, "commit", summary, {})
            self.assertIsNotNone(
                store.cached(
                    "volume",
                    directory.name,
                    store.signature(directory),
                    "commit",
                    directory=directory,
                )
            )
            raise RuntimeError("scan failed")
        self.assertEqual(store.summaries("volume"), [])
        self.assertEqual(store._admitted, set())
        store.remember("volume", directory, "commit", summary, {})
        self.assertEqual(len(store.summaries("volume")), 1)

    def test_catalog_scan_bad_entry_cannot_commit_half_of_its_update(self):
        store = SessionCatalog(self.root / "catalog.sqlite3")
        good, good_summary = self._catalog_entry("good")
        bad, bad_summary = self._catalog_entry("bad")
        artifact = {"path": "same", "bytes": 1, "sha256": "a" * 64}
        with store.scan():
            store.remember("volume", good, "commit", good_summary, {})
            with self.assertRaises(sqlite3.IntegrityError):
                store.remember("volume", bad, "commit", bad_summary, {"logs": [artifact, artifact]})
        self.assertEqual([x["session_id"] for x in store.summaries("volume")], ["good"])
        self.assertIsNone(store.cached("volume", bad.name, store.signature(bad), "commit"))

    def test_catalog_scan_disk_full_keeps_the_original_error_and_rolls_back(self):
        store = SessionCatalog(self.root / "catalog.sqlite3")
        directory, summary = self._catalog_entry("full")
        summary["take_id"] = "x" * 65536
        with (
            self.assertRaisesRegex(sqlite3.OperationalError, "database or disk is full"),
            store.scan(),
        ):
            with store.connection() as db:
                pages = db.execute("PRAGMA page_count").fetchone()[0]
                db.execute(f"PRAGMA max_page_count={pages}")
            store.remember("volume", directory, "commit", summary, {})
        self.assertEqual(store.summaries("volume"), [])
        summary["take_id"] = "take"
        store.remember("volume", directory, "commit", summary, {})
        self.assertEqual(len(store.summaries("volume")), 1)


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
