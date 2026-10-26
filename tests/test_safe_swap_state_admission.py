"""Persisted handoff metadata must be validated before changing ownership."""

import copy
import unittest

from rp_ylx.api import ProviderError
from rp_ylx.api.events import validate_capture_status, validate_safe_swap_v3_receipt
from rp_ylx.recording.device_session import uuid7
from tests import test_async_stop_crash as crash_fixtures
from tests import test_capture_coordinator as fixtures


class SafeSwapStateAdmissionTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)

    def released_state(self):
        coordinator = self.fixture.coordinator()
        coordinator.start_capture(fixtures.start_command("release-start"))
        coordinator.submit_frame(fixtures.frame())
        coordinator.stop_capture(fixtures.stop_command("release-stop", reason="safe_swap"))
        receipt = coordinator.current_safe_swap_receipt()
        validate_safe_swap_v3_receipt(receipt["receipt"])
        coordinator.close()
        return coordinator._state_db, coordinator._state_db.capture_state(), receipt

    def test_bad_completed_receipts_do_not_release_volume_or_block_new_capture(self):
        database, durable, receipt = self.released_state()
        variants = [
            ("missing-session", lambda v: v["receipt"].pop("session_id")),
            ("nonzero-handles", lambda v: v["receipt"].update(open_handle_count=1)),
            ("bad-release", lambda v: v["receipt"].update(release_state="recording")),
            ("bad-date", lambda v: v["receipt"].update(released_at="yesterday")),
            ("bad-digest", lambda v: v["receipt"].update(manifest_sha256="invalid")),
            ("unknown-receipt-field", lambda v: v["receipt"].update(extra=True)),
            ("unknown-resource-field", lambda v: v.update(extra=True)),
            ("wrong-resource-schema", lambda v: v.update(schema="invalid")),
        ]
        for name, change in variants:
            with self.subTest(case=name):
                value = copy.deepcopy(durable)
                change(value["safe_swap_resource"])
                database.put_capture_state(value, [], set())
                coordinator = self.fixture.coordinator()
                try:
                    self.assertIsNone(coordinator.current_safe_swap_receipt())
                    self.assertFalse(coordinator._released)
                    self.assertIsNone(database.capture_state()["safe_swap_resource"])
                    validate_capture_status(coordinator.capture_status())
                    coordinator.start_capture(fixtures.start_command("new-" + name))
                    coordinator.submit_frame(fixtures.frame())
                    coordinator.stop_capture(fixtures.stop_command("finish-" + name))
                finally:
                    coordinator.close()

    def test_valid_completed_receipt_and_release_survive_two_restarts(self):
        database, durable, receipt = self.released_state()
        for _ in range(2):
            coordinator = self.fixture.coordinator()
            try:
                self.assertTrue(coordinator._released)
                self.assertEqual(coordinator.current_safe_swap_receipt(), receipt)
                self.assertEqual(database.capture_state()["safe_swap_resource"], receipt)
                validate_capture_status(coordinator.capture_status())
            finally:
                coordinator.close()

    def test_bad_pending_intents_do_not_leave_phantom_handoff_ownership(self):
        database, durable, receipt = self.released_state()
        pending = {
            k: v
            for k, v in receipt["receipt"].items()
            if k not in {"schema", "released_at", "release_state", "open_handle_count"}
        }
        variants = [
            ("missing-session", lambda v: v.pop("session_id")),
            ("missing-manifest", lambda v: v.pop("manifest_id")),
            ("bad-session", lambda v: v.update(session_id=[])),
            ("bad-digest", lambda v: v.update(manifest_sha256="invalid")),
            ("bad-date", lambda v: v.update(sealed_at="yesterday")),
            ("unknown-field", lambda v: v.update(extra=True)),
        ]
        for name, change in variants:
            with self.subTest(case=name):
                value = copy.deepcopy(durable)
                value["safe_swap_resource"] = None
                value["pending_safe_swap"] = copy.deepcopy(pending)
                change(value["pending_safe_swap"])
                database.put_capture_state(value, [], set())
                coordinator = self.fixture.coordinator()
                try:
                    self.assertIsNone(coordinator._pending_safe_swap)
                    self.assertIsNone(coordinator._pending_safe_swap_failure)
                    self.assertIsNone(coordinator.artifact_io_state())
                    self.assertIsNone(database.capture_state()["pending_safe_swap"])
                    validate_capture_status(coordinator.capture_status())
                finally:
                    coordinator.close()

    def test_valid_crash_pending_intent_still_resumes_original_receipt(self):
        crash = crash_fixtures.AsyncStopCrashTest()
        crash.setUp()
        self.addCleanup(crash.doCleanups)
        command, accepted = crash.kill_at("before_release", "safe_swap")
        coordinator = crash.fixture.coordinator()
        try:
            self.assertIsNotNone(coordinator._pending_safe_swap)
            self.assertFalse(coordinator._released)
            coordinator.request_stop_capture(command)
            crash.wait_finished(coordinator)
            receipt = coordinator.current_safe_swap_receipt()["receipt"]
            validate_safe_swap_v3_receipt(receipt)
            self.assertEqual(receipt["session_id"], accepted["session_id"])
        finally:
            coordinator.close()

    def test_validly_typed_pending_fields_must_match_verified_manifest(self):
        database, durable, receipt = self.released_state()
        for field, value in (("manifest_id", uuid7()), ("sealed_at", "2020-01-01T00:00:00Z")):
            with self.subTest(field=field):
                state = copy.deepcopy(durable)
                state["safe_swap_resource"] = None
                state["pending_safe_swap"] = {
                    k: v
                    for k, v in receipt["receipt"].items()
                    if k not in {"schema", "released_at", "release_state", "open_handle_count"}
                }
                state["pending_safe_swap"][field] = value
                database.put_capture_state(state, [], set())
                coordinator = self.fixture.coordinator()
                try:
                    coordinator.request_stop_capture(
                        fixtures.stop_command("mismatch-" + field, reason="safe_swap")
                    )
                    with coordinator._stop_condition:
                        self.assertTrue(
                            coordinator._stop_condition.wait_for(
                                lambda coordinator=coordinator: (
                                    coordinator._async_stop_worker is None
                                ),
                                timeout=4,
                            )
                        )
                    with self.assertRaises(ProviderError) as raised:
                        coordinator.current_safe_swap_receipt()
                    self.assertEqual(raised.exception.code, "safe_swap_unverified")
                    self.assertFalse(coordinator._released)
                    self.assertIsNone(database.capture_state()["safe_swap_resource"])
                finally:
                    coordinator.close()
