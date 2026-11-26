"""Persisted handoff metadata must be validated before changing ownership."""

import copy
import multiprocessing
import signal
import threading
import traceback
import unittest

from rp_ylx.api import ProviderError
from rp_ylx.api.events import validate_capture_status, validate_safe_swap_v3_receipt
from rp_ylx.recording.device_session import uuid7
from tests.testkit import CoordinatorHarness, frame, start_command, stop_command


def _kill_before_release(fixture: CoordinatorHarness):
    context = multiprocessing.get_context("fork")
    receive, send = context.Pipe(duplex=False)
    command = stop_command("killed-stop-before_release", reason="safe_swap")

    def child():
        lock = threading.Lock()

        def message(value):
            with lock:
                send.send(value)

        def boundary(*args, **kwargs):
            message({"boundary": "before_release"})
            threading.Event().wait()

        try:
            coordinator = fixture.coordinator()
            coordinator.start_capture(start_command("killed-start-before_release"))
            session_id = fixture.active_session_id(coordinator)
            if not coordinator.submit_frame(frame()):
                raise AssertionError("frame rejected")
            coordinator._publish_safe_swap = boundary
            accepted = coordinator.request_stop_capture(command)
            message(
                {"accepted": accepted.body, "status": accepted.status, "session_id": session_id}
            )
            threading.Event().wait()
        except BaseException:
            message({"error": traceback.format_exc()})
            raise

    process = context.Process(target=child)
    process.start()
    send.close()
    try:
        messages = []
        for _ in range(2):
            if not receive.poll(10):
                raise AssertionError("child did not reach durability boundary")
            item = receive.recv()
            if "error" in item:
                raise AssertionError(item["error"])
            messages.append(item)
        if {"boundary": "before_release"} not in messages:
            raise AssertionError("missing boundary message")
        accepted = next(item for item in messages if "accepted" in item)
        if accepted["status"] != 202:
            raise AssertionError(f"expected 202, got {accepted['status']}")
        process.kill()
        process.join(5)
        if process.exitcode != -signal.SIGKILL:
            raise AssertionError(f"expected SIGKILL, got {process.exitcode}")
        return command, accepted
    finally:
        if process.is_alive():
            process.kill()
            process.join(5)
        receive.close()
        process.close()


class SafeSwapStateAdmissionTest(unittest.TestCase):
    def setUp(self):
        self.fixture = CoordinatorHarness().setUp()
        self.addCleanup(self.fixture.tearDown)

    def released_state(self):
        coordinator = self.fixture.coordinator()
        coordinator.start_capture(start_command("release-start"))
        coordinator.submit_frame(frame())
        coordinator.stop_capture(stop_command("release-stop", reason="safe_swap"))
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
                    coordinator.start_capture(start_command("new-" + name))
                    coordinator.submit_frame(frame())
                    coordinator.stop_capture(stop_command("finish-" + name))
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
        command, accepted = _kill_before_release(self.fixture)
        coordinator = self.fixture.coordinator()
        try:
            self.assertIsNotNone(coordinator._pending_safe_swap)
            self.assertFalse(coordinator._released)
            coordinator.request_stop_capture(command)
            with coordinator._stop_condition:
                self.assertTrue(
                    coordinator._stop_condition.wait_for(
                        lambda: coordinator._async_stop_worker is None, timeout=4
                    )
                )
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
                        stop_command("mismatch-" + field, reason="safe_swap")
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
