from __future__ import annotations

import json
import threading
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import Mock, patch

from rp_ylx import firmware_control as control
from rp_ylx import update
from scripts import publish_rdk_x5_oss as publisher
from tests import test_firmware_control as fixtures


class ManifestCacheTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.FirmwareControlTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.supervisor = self.fixture.supervisor
        self.fetch = self.fixture.fetch
        self.manifest = dict(self.fixture.manifest)

    def assert_rejected(self, manifest):
        before = self.supervisor.check(force=True)
        cache = dict(before["available"])
        checked = self.supervisor._cache_checked_at
        self.fetch.return_value = manifest
        after = self.supervisor.check(force=True)
        self.assertIn("清单校验失败", after["warning"])
        self.assertEqual(after["available"], cache)
        self.assertEqual(after["checked_at"], before["checked_at"])
        self.assertEqual(self.supervisor._cache_checked_at, checked)
        self.assertEqual(self.supervisor.status()["available"], cache)
        control._encode_control_response({"ok": True, "body": after})
        self.fetch.return_value = self.manifest

    def test_lone_surrogates_in_all_display_fields_preserve_cache(self):
        for field in ("version", "commit", "release_notes", "published_at"):
            for character in ("\ud800", "\udfff"):
                with self.subTest(field=field, codepoint=ord(character)):
                    self.assert_rejected({**self.manifest, field: character})

    def test_notes_type_and_character_limit(self):
        for value in (None, [], {}, True, 42, "x" * 16001):
            with self.subTest(type=type(value).__name__):
                self.assert_rejected({**self.manifest, "release_notes": value})

    def test_published_at_type_and_utf8_limit(self):
        for value in ([], {}, True, 42, float("nan"), "", "x" * 129, "中" * 43):
            with self.subTest(type=type(value).__name__):
                self.assert_rejected({**self.manifest, "published_at": value})

    def test_version_and_commit_admission(self):
        for field, value in (
            ("version", "0.2." + "9" * 5000),
            ("version", ""),
            ("version", 2),
            ("version", "0.2.2\n"),
            ("commit", "C" * 40),
            ("commit", "c" * 41),
            ("commit", []),
        ):
            with self.subTest(field=field):
                self.assert_rejected({**self.manifest, field: value})

    def test_bad_root_and_missing_identity(self):
        for value in (None, [], "text", {}, {"version": "0.2.2"}):
            with self.subTest(type=type(value).__name__):
                self.assert_rejected(value)

    def test_unicode_and_json_escaping_count_toward_wire_budget(self):
        for notes in ("中" * 16000, "\x00" * 6000, "😀" * 9000):
            self.assert_rejected({**self.manifest, "release_notes": notes})

    def test_exact_byte_boundary_accepts_then_rejects(self):
        empty = update.manifest_display_metadata({**self.manifest, "release_notes": ""})
        overhead = len(json.dumps(empty, ensure_ascii=False).encode())
        count, padding = divmod(update.MAX_DISPLAY_METADATA - overhead, 3)
        value = {**self.manifest, "release_notes": "中" * count + "x" * padding}
        metadata = update.manifest_display_metadata(value)
        self.assertEqual(len(json.dumps(metadata, ensure_ascii=False).encode()), 32768)
        with self.assertRaises(ValueError):
            update.manifest_display_metadata(
                {**value, "release_notes": value["release_notes"] + "x"}
            )

    def test_valid_unicode_and_optional_fields(self):
        for notes in ("x" * 16000, "修复\n录制😀", "中" * 10000):
            self.fetch.return_value = {**self.manifest, "release_notes": notes}
            result = self.supervisor.check(force=True)
            self.assertIsNone(result["warning"])
            self.assertEqual(result["available"]["release_notes"], notes)
            control._encode_control_response({"ok": True, "body": result})
        self.fetch.return_value = {"version": "0.2.2", "commit": "c" * 40}
        result = self.supervisor.check(force=True)
        self.assertEqual(result["available"]["release_notes"], "")
        self.assertIsNone(result["available"]["published_at"])

    def test_bad_metadata_on_incompatible_channel_does_not_clear_cache(self):
        self.assert_rejected({**self.manifest, "version": "0.1.0", "published_at": {}})

    def test_valid_incompatible_channel_still_clears_cache(self):
        self.supervisor.check(force=True)
        self.fetch.return_value = {**self.manifest, "version": "0.2.2-rc.1"}
        result = self.supervisor.check(force=True)
        self.assertIsNone(result["available"])
        self.assertIn("已连接 OSS", result["warning"])

    def test_invalid_first_fetch_and_valid_recovery(self):
        self.fetch.return_value = {**self.manifest, "release_notes": "\ud800"}
        result = self.supervisor.check(force=True)
        self.assertIsNone(result["available"])
        self.assertIsNone(result["checked_at"])
        self.assertIsNone(self.supervisor._cache_checked_at)
        self.fetch.return_value = self.manifest
        result = self.supervisor.check(force=True)
        self.assertIsNone(result["warning"])
        self.assertTrue(result["has_update"])

    def test_deep_json_fetch_error_preserves_cache(self):
        before = self.supervisor.check(force=True)
        self.fetch.side_effect = RecursionError("deep manifest")
        result = self.supervisor.check(force=True)
        self.assertIn("清单校验失败", result["warning"])
        self.assertEqual(result["available"], before["available"])
        self.assertEqual(result["checked_at"], before["checked_at"])

    def test_control_cancellation_propagates_without_cache_change(self):
        before = self.supervisor.check(force=True)
        self.fetch.side_effect = KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            self.supervisor.check(force=True)
        self.assertEqual(self.supervisor.status()["available"], before["available"])

    def test_update_refuses_invalid_manifest_without_running_installer(self):
        runner = Mock()
        self.supervisor.runner = runner
        self.supervisor.check(force=True)
        self.fetch.return_value = {**self.manifest, "release_notes": "\ud800"}
        with self.assertRaises(control.FirmwareError) as error:
            self.supervisor.start("update", str(uuid.uuid4()), "c" * 40)
        self.assertEqual(error.exception.code, "update_check_failed")
        runner.assert_not_called()
        self.fixture.idle.assert_not_called()
        self.assertIsNone(self.supervisor.task)

    def test_real_socket_keeps_check_and_status_available_then_recovers(self):
        path = str(self.fixture.root / "control.sock")
        server = control.ControlServer(path, control.ControlHandler)
        server.supervisor = self.supervisor
        thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01))
        thread.start()
        try:
            with patch.object(control, "SOCKET", path):
                original = control.request_firmware("check", force=True)
                self.fetch.return_value = {**self.manifest, "release_notes": "\ud800"}
                for operation, fields in (("check", {"force": True}), ("status", {})):
                    result = control.request_firmware(operation, **fields)
                    self.assertEqual(result["available"], original["available"])
                    self.assertIn("清单校验失败", result["warning"])
                self.fetch.return_value = {**self.manifest, "version": "0.2.3"}
                result = control.request_firmware("check", force=True)
                self.assertIsNone(result["warning"])
                self.assertEqual(result["available"]["version"], "0.2.3")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(3)
        self.assertFalse(thread.is_alive())

    def test_max_metadata_with_max_escaped_task_message_fits_control_frame(self):
        self.supervisor.task = {
            "id": str(uuid.uuid4()),
            "action": "update",
            "target_commit": "c" * 40,
            "status": "failed",
            "message": "\x00" * 4096,
            "started_at": 2**63 - 1,
            "finished_at": 2**63 - 1,
        }
        value = {**self.manifest, "release_notes": ""}
        overhead = len(json.dumps(value, ensure_ascii=False).encode())
        count, padding = divmod(32768 - overhead, 3)
        self.fetch.return_value = {**value, "release_notes": "中" * count + "x" * padding}
        result = self.supervisor.check(force=True)
        self.assertIsNone(result["warning"])
        wire = control._encode_control_response({"ok": True, "body": result})
        self.assertLessEqual(len(wire), control.LIMIT)

    def test_fetched_dictionary_changes_do_not_change_cache(self):
        original = self.supervisor.check(force=True)["available"].copy()
        self.fetch.return_value["release_notes"] = "\ud800"
        self.assertEqual(self.supervisor.status()["available"], original)

    def test_concurrent_status_never_observes_unvalidated_metadata(self):
        original = self.supervisor.check(force=True)["available"].copy()
        self.fetch.return_value = {**self.manifest, "release_notes": "\ud800"}
        entered, release = threading.Event(), threading.Event()
        validate = update.manifest_display_metadata
        outcomes = []

        def slow_validate(value):
            entered.set()
            if not release.wait(3):
                raise AssertionError("validation gate timed out")
            return validate(value)

        def check():
            try:
                outcomes.append(self.supervisor.check(force=True))
            except BaseException as error:
                outcomes.append(error)

        with patch.object(update, "manifest_display_metadata", side_effect=slow_validate):
            thread = threading.Thread(target=check)
            thread.start()
            try:
                self.assertTrue(entered.wait(3))
                for _ in range(20):
                    result = self.supervisor.status()
                    self.assertEqual(result["available"], original)
                    control._encode_control_response({"ok": True, "body": result})
            finally:
                release.set()
                thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(outcomes), 1)
        self.assertIsInstance(outcomes[0], dict)
        self.assertIn("清单校验失败", outcomes[0]["warning"])

    def test_manifest_consumer_validates_display_fields(self):
        descriptor = {"sha256": "a" * 64, "bytes": 1, "url": "https://example.test/file"}
        value = {
            **self.manifest,
            "schema": update.RELEASE_SCHEMA,
            "platform": update.PLATFORM,
            "bundle": descriptor,
            "updater": descriptor,
        }
        self.assertEqual(update.validate_manifest(value), value)
        with self.assertRaises(ValueError):
            update.validate_manifest({**value, "published_at": []})

    def test_publisher_rejects_before_creating_output_or_verifying_bundle(self):
        bundle = SimpleNamespace(commit="c" * 40, version="0.2.2", root=self.fixture.root)
        output = self.fixture.root / "unpublished"
        with (
            patch.object(publisher, "load_bundle", return_value=bundle),
            patch.object(publisher, "verify_bundle") as verify,
        ):
            for notes in ("\ud800", "中" * 16000, "\x00" * 6000, [], 1):
                with self.assertRaises(ValueError):
                    publisher.prepare(self.fixture.root, output, {}, release_notes=notes)
                self.assertFalse(output.exists())
            verify.assert_not_called()
