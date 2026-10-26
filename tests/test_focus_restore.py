"""Saved focus failures must not lock users out of correcting focus."""

import copy
import json
import sqlite3
import unittest
from unittest.mock import patch

from rp_ylx.api import ProviderError
from rp_ylx.api.events import validate_capture_status
from rp_ylx.camera import CameraError
from rp_ylx.persistence import StateDatabase
from tests import test_capture_coordinator as fixtures


class FocusSources(fixtures.FakeSources):
    def __init__(self):
        super().__init__()
        self.focus = copy.deepcopy(fixtures.CAMERA_FOCUS_STATUS)
        self.calls = []
        self.failures = []

    def set_camera_focus(self, *, value=None, auto_enabled=None):
        self.calls.append((value, auto_enabled))
        if self.failures:
            raise self.failures.pop(0)
        if value is not None and (type(value) is not int or not 0 <= value <= 255):
            raise CameraError("invalid_focus_value", "fixture invalid focus")
        if auto_enabled is not None and type(auto_enabled) is not bool:
            raise CameraError("invalid_camera_focus", "fixture invalid auto mode")
        return super().set_camera_focus(value=value, auto_enabled=auto_enabled)


def focus_command(key, value):
    return fixtures.command(key, {"schema": "ylx.camera-focus-set.v1", "value": value})


class FocusRestoreTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        first = self.fixture.coordinator()
        first.close()
        self.database = first._state_db

    def store_raw(self, raw):
        self.database.put("camera_focus", {})
        with self.database.connection() as db, db:
            db.execute("UPDATE documents SET body=? WHERE name='camera_focus'", (raw,))

    def raw(self):
        with self.database.connection() as db:
            return db.execute("SELECT body FROM documents WHERE name='camera_focus'").fetchone()[0]

    def record(self, coordinator, key):
        coordinator.start_capture(fixtures.start_command(key))
        coordinator.submit_frame(fixtures.frame())
        coordinator.stop_capture(fixtures.stop_command(key + "-stop"))

    def test_bad_saved_settings_allow_inspection_and_explicit_correction(self):
        cases = [
            "{",
            "[]",
            "null",
            "[" * 2000 + "]" * 2000,
            json.dumps({"unknown": True}),
            json.dumps({"value": True}),
            json.dumps({"value": "77"}),
            json.dumps({"value": float("nan")}),
            json.dumps({"auto_enabled": "false"}),
            json.dumps({"value": None, "auto_enabled": None}),
        ]
        for index, raw in enumerate(cases):
            with self.subTest(case=index):
                self.store_raw(raw)
                sources = FocusSources()
                coordinator = self.fixture.coordinator(sources=sources)
                try:
                    self.assertEqual(coordinator.camera_focus_status()["value"], 42)
                    validate_capture_status(coordinator.capture_status())
                    self.assertEqual(sources.calls, [])
                    with self.assertRaises(ProviderError) as raised:
                        coordinator.start_capture(fixtures.start_command(f"bad-start-{index}"))
                    self.assertEqual(raised.exception.code, "camera_focus_restore_failed")
                    self.assertIsNone(coordinator._active)
                    self.assertEqual(self.raw(), raw)
                    response = coordinator.set_camera_focus(focus_command(f"correct-{index}", 77))
                    self.assertEqual(response.body["value"], 77)
                    self.assertEqual(sources.calls, [(77, None)])
                    self.assertEqual(self.database.get("camera_focus")["value"], 77)
                    self.record(coordinator, f"corrected-start-{index}")
                finally:
                    coordinator.close()

    def test_incompatible_valid_setting_can_be_overridden_without_reapplying_it(self):
        self.database.put("camera_focus", {"value": 999, "auto_enabled": False})
        sources = FocusSources()
        coordinator = self.fixture.coordinator(sources=sources)
        try:
            self.assertEqual(sources.calls, [(999, False)])
            self.assertEqual(coordinator.camera_focus_status()["value"], 42)
            self.assertEqual(sources.calls, [(999, False)])
            with self.assertRaises(ProviderError) as raised:
                coordinator.start_capture(fixtures.start_command("wrong-hardware"))
            self.assertEqual(raised.exception.code, "camera_focus_restore_failed")
            before = list(sources.calls)
            coordinator.set_camera_focus(focus_command("new-hardware-focus", 88))
            self.assertEqual(sources.calls, before + [(88, None)])
            self.record(coordinator, "new-hardware-record")
            self.assertEqual(sources.calls[-1], (88, None))
        finally:
            coordinator.close()

    def test_transient_restore_failure_retries_on_capture_and_valid_restore_runs_once(self):
        self.database.put("camera_focus", {"value": 77, "auto_enabled": False})
        sources = FocusSources()
        sources.failures = [CameraError("camera_focus_open_failed", "temporary", retryable=True)]
        coordinator = self.fixture.coordinator(sources=sources)
        try:
            self.assertEqual(sources.calls, [(77, False)])
            self.assertEqual(coordinator.camera_focus_status()["value"], 42)
            self.assertEqual(sources.calls, [(77, False)])
            self.record(coordinator, "retry-focus")
            self.assertEqual(sources.calls, [(77, False), (77, False)])
            self.assertEqual(coordinator.camera_focus_status()["value"], 77)
        finally:
            coordinator.close()
        sources = FocusSources()
        restarted = self.fixture.coordinator(sources=sources)
        try:
            self.assertEqual(sources.calls, [(77, False)])
            self.record(restarted, "restored-once")
            self.assertEqual(sources.calls, [(77, False)])
        finally:
            restarted.close()

    def test_failed_explicit_override_keeps_saved_intent_until_success(self):
        original = {"value": 999, "auto_enabled": False}
        self.database.put("camera_focus", original)
        sources = FocusSources()
        coordinator = self.fixture.coordinator(sources=sources)
        try:
            with self.assertRaises(ProviderError) as raised:
                coordinator.set_camera_focus(focus_command("bad-override", 1000))
            self.assertEqual(raised.exception.code, "invalid_focus_value")
            self.assertEqual(self.database.get("camera_focus"), original)
            coordinator.set_camera_focus(focus_command("good-override", 55))
            self.assertEqual(self.database.get("camera_focus")["value"], 55)
            self.record(coordinator, "good-override-record")
        finally:
            coordinator.close()

    def test_invalid_hardware_response_is_not_saved_or_replayed_as_success(self):
        self.database.put("camera_focus", {"value": 42, "auto_enabled": False})
        sources = FocusSources()
        coordinator = self.fixture.coordinator(sources=sources)
        command = focus_command("invalid-hardware-response", 66)
        try:
            before = self.database.get("camera_focus")
            with (
                patch.object(sources, "set_camera_focus", return_value={"value": None}),
                self.assertRaises(ProviderError) as raised,
            ):
                coordinator.set_camera_focus(command)
            self.assertEqual(raised.exception.code, "camera_focus_unavailable")
            self.assertEqual(self.database.get("camera_focus"), before)
            result = coordinator.set_camera_focus(command)
            self.assertFalse(result.replayed)
            self.assertEqual(result.body["value"], 66)
        finally:
            coordinator.close()

    def test_database_io_failure_is_not_treated_as_optional_setting_corruption(self):
        self.database.put("camera_focus", {"value": 77, "auto_enabled": False})
        original = StateDatabase.get
        sources = FocusSources()

        def broken(database, name):
            if name == "camera_focus":
                raise sqlite3.DatabaseError("injected storage failure")
            return original(database, name)

        with (
            patch.object(StateDatabase, "get", broken),
            self.assertRaises(sqlite3.DatabaseError),
        ):
            self.fixture.coordinator(sources=sources)
        self.assertEqual(sources.calls, [])
        self.assertEqual(self.database.get("camera_focus")["value"], 77)

    def test_fixed_focus_camera_keeps_saved_preference_without_blocking_capture(self):
        saved = {"value": 77, "auto_enabled": False}
        self.database.put("camera_focus", saved)
        sources = FocusSources()
        sources.focus = None
        with patch.object(
            sources,
            "set_camera_focus",
            side_effect=CameraError("camera_focus_unsupported", "fixed-focus camera"),
        ) as setter:
            coordinator = self.fixture.coordinator(sources=sources)
            try:
                setter.assert_called_once_with(value=77, auto_enabled=False)
                self.assertIsNone(coordinator.camera_focus_status())
                self.record(coordinator, "fixed-focus-record")
                setter.assert_called_once()
                self.assertEqual(self.database.get("camera_focus"), saved)
                with self.assertRaises(ProviderError) as raised:
                    coordinator.set_camera_focus(focus_command("fixed-focus-change", 88))
                self.assertEqual(raised.exception.code, "camera_focus_unsupported")
                self.assertEqual(raised.exception.status, 404)
            finally:
                coordinator.close()

    def test_preview_remains_available_to_correct_bad_saved_focus(self):
        self.database.put("camera_focus", {"value": 999, "auto_enabled": False})
        sources = FocusSources()
        coordinator = self.fixture.coordinator(sources=sources)
        try:
            coordinator._preview._thumbnails = None
            coordinator._preview.publish(fixtures.JPEG)
            before = list(sources.calls)
            response = coordinator.latest_preview(fps=None, accept="image/jpeg")
            self.assertEqual(response.body, fixtures.JPEG)
            self.assertEqual(response.content_length, len(fixtures.JPEG))
            self.assertEqual(sources.calls, before)
            sources.camera_state = "disconnected"
            with self.assertRaises(ProviderError) as raised:
                coordinator.latest_preview(fps=None, accept="image/jpeg")
            self.assertEqual(raised.exception.code, "camera_not_connected")
        finally:
            coordinator.close()
