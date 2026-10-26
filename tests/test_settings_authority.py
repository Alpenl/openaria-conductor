import copy
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx import daemon, settings
from rp_ylx.persistence import PersistenceError, StateDatabase
from tests import test_settings_presets as fixtures


class SettingsAuthorityTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.SettingsPresetTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.config = self.fixture.config
        self.base = copy.deepcopy(self.fixture.base)

    def write(self, base):
        self.config.write_text(json.dumps(base))

    def test_stored_values_override_invalid_legacy_dynamic_values_without_rewrite(self):
        before = self.fixture.snapshot()
        for group, name, value in (
            ("camera", "fps", -1),
            ("storage", "minimum_available_bytes", -1),
            ("recording", "bitrate_kbps", False),
        ):
            with self.subTest(group=group):
                base = copy.deepcopy(self.base)
                base[group][name] = value
                self.write(base)
                raw = self.config.read_bytes()
                expected = daemon.load_production_config(self.config)
                settings.initialize(self.config)
                self.assertEqual(
                    settings.manage(self.config, "show")["settings"], self.fixture.initial
                )
                self.assertEqual(daemon.load_production_config(self.config), expected)
                self.assertEqual(self.fixture.snapshot(), before)
                self.assertEqual(self.config.read_bytes(), raw)

    def test_service_start_reaches_builder_with_authoritative_dynamic_values(self):
        self.base["camera"]["fps"] = -1
        self.write(self.base)

        class ReachedBuilder(Exception):
            pass

        with (
            patch.object(daemon, "build_production_service", side_effect=ReachedBuilder) as build,
            patch.object(daemon._OPERATIONAL_LOG, "event"),
            self.assertRaises(ReachedBuilder),
        ):
            daemon.run_production_service(self.config)
        self.assertEqual(build.call_count, 1)
        self.assertEqual(build.call_args.args[0].fps, 60)

    def test_updates_validate_against_stored_settings_not_stale_json(self):
        self.base["camera"]["fps"] = -1
        self.write(self.base)
        self.fixture.set_patch({"storage": {"minimum_available_bytes": 3000}})
        effective = daemon.load_production_config(self.config)
        self.assertEqual(effective.fps, 60)
        self.assertEqual(effective.minimum_available_bytes, 3000)
        before = self.fixture.snapshot()
        with self.assertRaises(daemon.ProductionConfigError):
            self.fixture.set_patch({"camera": {"fps": -1}})
        self.assertEqual(self.fixture.snapshot(), before)

    def test_invalid_bootstrap_identity_or_path_still_fails_without_state_changes(self):
        before = self.fixture.snapshot()
        for group, key, value in (
            ("camera", "device", "relative"),
            ("listen", "port", 0),
            ("security", "isolated_network", False),
            ("device", "device_id", "invalid"),
        ):
            with self.subTest(group=group):
                base = copy.deepcopy(self.base)
                base[group][key] = value
                self.write(base)
                with self.assertRaises(daemon.ProductionConfigError):
                    settings.initialize(self.config)
                self.assertEqual(self.fixture.snapshot(), before)

    def test_invalid_first_import_does_not_create_a_database(self):
        self.base["state_root"] = str(self.fixture.root / "never-created")
        self.base["camera"]["fps"] = -1
        self.write(self.base)
        with self.assertRaises(daemon.ProductionConfigError):
            settings.initialize(self.config)
        self.assertFalse(Path(self.base["state_root"]).exists())

    def test_interrupted_schema_only_initialization_can_finish_once(self):
        self.base["state_root"] = str(self.fixture.root / "schema-only")
        self.write(self.base)
        database = StateDatabase(settings.settings_path(self.base))
        self.assertIsNone(database.get("device"))
        settings.initialize(self.config)
        first = database.get("device")
        self.assertIsNotNone(first)
        settings.initialize(self.config)
        self.assertEqual(database.get("device"), first)
        with database.connection() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM changes").fetchone()[0], 1)

    def test_missing_or_corrupt_authority_never_reimports_bootstrap(self):
        for kind in ("document", "file", "corrupt", "future"):
            with self.subTest(kind=kind):
                base = copy.deepcopy(self.base)
                base["state_root"] = str(self.fixture.root / kind)
                self.write(base)
                settings.initialize(self.config)
                database = StateDatabase(settings.settings_path(base))
                if kind == "document":
                    with database.connection() as db, db:
                        db.execute("DELETE FROM documents WHERE name='device'")
                elif kind == "file":
                    database.path.unlink()
                elif kind == "corrupt":
                    database.path.write_bytes(b"not a database")
                else:
                    with database.connection() as db, db:
                        db.execute("PRAGMA user_version=99")
                with self.assertRaises(PersistenceError):
                    settings.initialize(self.config)
                if kind == "file":
                    self.assertFalse(database.path.exists())
                if kind == "document":
                    self.assertIsNone(database.get("device"))

    def test_invalid_stored_configuration_does_not_fall_back_to_json(self):
        self.fixture.database.put("device", {"camera": {"fps": -1}})
        before = self.fixture.snapshot()
        with self.assertRaises(daemon.ProductionConfigError):
            settings.initialize(self.config)
        self.assertEqual(self.fixture.snapshot(), before)

    def test_manage_uses_one_bootstrap_snapshot(self):
        original = Path.read_bytes
        observations = []

        def read(path):
            if path == self.config:
                observations.append(path)
            return original(path)

        with patch.object(Path, "read_bytes", read):
            self.assertEqual(settings.manage(self.config, "show")["settings"], self.fixture.initial)
        self.assertEqual(observations, [self.config])
