import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path

from rp_ylx.daemon import ProductionConfigError, load_production_config
from rp_ylx.persistence import StateDatabase
from rp_ylx.recording.encoding import RecordingEncoding
from rp_ylx.settings import manage


class SettingsPresetTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.config = self.root / "device.json"
        self.base = {
            "schema": "ylx.production-config.v1",
            "listen": {"host": "127.0.0.1", "port": 8080},
            "camera": {"device": "/dev/video0", "width": 3840, "height": 1080, "fps": 60},
            "storage": {
                "mountpoint": str(self.root / "data"),
                "minimum_available_bytes": 2147483648,
                "minimum_available_inodes": 1024,
            },
            "state_root": str(self.root / "state"),
            "device": {
                "device_id": "11111111-1111-4111-8111-111111111111",
                "device_label": "YLX-12AB34CD",
                "hardware_fingerprint": "sha256:" + "a" * 64,
            },
            "security": {"profile": "lab", "isolated_network": True},
            "recording": {"preset": "efficient", "gop_frames": 90, "rate_control": "avbr"},
        }
        self.config.write_text(json.dumps(self.base))
        self.initial = manage(self.config, "show")["settings"]
        self.database = StateDatabase(self.root / "state/settings.sqlite3")

    def set_patch(self, value, name="patch.json"):
        source = self.root / name
        source.write_text(json.dumps(value))
        return manage(self.config, "set", source=source)

    def snapshot(self):
        with self.database.connection() as db:
            return {
                name: db.execute("SELECT * FROM " + name).fetchall()
                for name in ("documents", "changes", "imports")
            }

    def test_explicit_preset_replaces_old_profile_and_survives_bootstrap_change(self):
        for preset in ("standard", "high", "efficient"):
            with self.subTest(preset=preset):
                response = self.set_patch({"recording": {"preset": preset}})
                self.assertTrue(response["saved"])
                expected = asdict(RecordingEncoding.from_mapping({"preset": preset}))
                saved = manage(self.config, "show")["settings"]
                self.assertEqual(saved["recording"], expected)
                self.assertEqual(
                    asdict(load_production_config(self.config).recording_encoding), expected
                )
                for group in ("camera", "audio", "storage"):
                    self.assertEqual(saved[group], self.initial[group])
                self.base["recording"] = {"preset": "high", "codec": "hevc", "bitrate_kbps": 5000}
                self.config.write_text(json.dumps(self.base))
                self.assertEqual(
                    asdict(load_production_config(self.config).recording_encoding), expected
                )

    def test_same_patch_overrides_preset_but_old_overrides_do_not_survive(self):
        patch = {"preset": "standard", "codec": "hevc", "bitrate_kbps": 9000, "max_qp": 40}
        self.set_patch({"recording": patch})
        expected = asdict(RecordingEncoding.from_mapping(patch))
        self.assertEqual(manage(self.config, "show")["settings"]["recording"], expected)
        self.assertEqual(expected["gop_frames"], 30)
        self.assertEqual(expected["rate_control"], "cbr")

    def test_patch_without_preset_preserves_other_values_and_export_is_resolved(self):
        self.set_patch({"recording": {"bitrate_kbps": 10000}})
        expected = {**self.initial["recording"], "bitrate_kbps": 10000}
        self.assertEqual(manage(self.config, "show")["settings"]["recording"], expected)
        self.set_patch({"recording": {"preset": "high", "gop_frames": 75}})
        expected = asdict(RecordingEncoding.from_mapping({"preset": "high", "gop_frames": 75}))
        destination = self.root / "export.json"
        manage(self.config, "export", destination=destination)
        self.assertEqual(json.loads(destination.read_text())["recording"], expected)
        self.assertNotIn("preset", self.database.get("device")["recording"])

    def test_invalid_preset_or_override_rolls_back_all_groups_and_history(self):
        before = self.snapshot()
        for recording in (
            {"preset": "unknown"},
            {"preset": []},
            {"preset": "standard", "min_qp": 50},
            {"preset": "high", "bitrate_kbps": True},
            {"preset": "efficient", "typo": 1},
        ):
            with self.subTest(recording=recording):
                with self.assertRaises(ProductionConfigError):
                    self.set_patch(
                        {"recording": recording, "storage": {"minimum_available_bytes": 1234}}
                    )
                self.assertEqual(self.snapshot(), before)

    def test_equivalent_preset_is_a_noop_in_revision_and_audit_history(self):
        self.set_patch({"recording": {"preset": "standard"}})
        before = self.snapshot()
        self.set_patch(
            {"recording": asdict(RecordingEncoding.from_mapping({"preset": "standard"}))}
        )
        self.assertEqual(self.snapshot(), before)

    def test_concurrent_preset_and_storage_updates_do_not_lose_each_other(self):
        barrier = threading.Barrier(2, timeout=5)

        def change(name, value):
            barrier.wait()
            return self.set_patch(value, name)

        with ThreadPoolExecutor(max_workers=2) as pool:
            a = pool.submit(change, "preset.json", {"recording": {"preset": "high"}})
            b = pool.submit(change, "space.json", {"storage": {"minimum_available_bytes": 3000}})
            self.assertTrue(a.result()["saved"] and b.result()["saved"])
        actual = manage(self.config, "show")["settings"]
        self.assertEqual(
            actual["recording"], asdict(RecordingEncoding.from_mapping({"preset": "high"}))
        )
        self.assertEqual(actual["storage"]["minimum_available_bytes"], 3000)
