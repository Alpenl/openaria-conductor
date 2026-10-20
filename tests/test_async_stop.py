from __future__ import annotations

import json
import threading
import unittest
from unittest.mock import patch

from rp_ylx.api import ProviderError
from rp_ylx.recording.device_session import DeviceRecordingError
from tests import test_capture_coordinator as fixtures
from tests import test_gateway as gateway_fixtures


class AsyncStopTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.release = threading.Event()
        self.entered = threading.Event()
        self.coordinator = self.fixture.coordinator()
        self.addCleanup(self.coordinator.close)
        self.addCleanup(self.release.set)
        self.coordinator.start_capture(fixtures.start_command("async-start"))
        self.assertTrue(self.coordinator.submit_frame(fixtures.frame()))

    def block_verification(self):
        original = self.coordinator._verify_exact_session_payload

        def verify(*args, **kwargs):
            self.entered.set()
            if not self.release.wait(5):
                raise TimeoutError("test did not release independent verification")
            return original(*args, **kwargs)

        context = patch.object(self.coordinator, "_verify_exact_session_payload", verify)
        context.start()
        self.addCleanup(context.stop)

    def wait_finished(self):
        with self.coordinator._stop_condition:
            self.assertTrue(
                self.coordinator._stop_condition.wait_for(
                    lambda: self.coordinator._async_stop_worker is None, timeout=4
                )
            )

    def test_request_returns_before_full_verification_and_replays_same_receipt(self):
        self.block_verification()
        done = threading.Event()
        results, errors = [], []
        command = fixtures.stop_command("async-stop")

        def request():
            try:
                method = getattr(
                    self.coordinator, "request_stop_capture", self.coordinator.stop_capture
                )
                results.append(method(command))
            except BaseException as error:
                errors.append(error)
            finally:
                done.set()

        caller = threading.Thread(target=request)
        caller.start()
        try:
            self.assertTrue(self.entered.wait(2))
            self.assertTrue(done.wait(0.5), "HTTP stop still waits for full verification")
            self.assertEqual(errors, [])
            self.assertEqual(results[0].status, 202)
            self.assertIsNotNone(self.coordinator._network_operation_lease)
            with self.assertRaises(ProviderError):
                self.coordinator.start_capture(fixtures.start_command("too-early"))
            replay = self.coordinator.request_stop_capture(command)
            self.assertTrue(replay.replayed)
            self.assertEqual(replay.body, results[0].body)
            self.release.set()
            self.wait_finished()
            replay = self.coordinator.request_stop_capture(command)
            self.assertTrue(replay.replayed)
            self.assertEqual(replay.body, results[0].body)
            self.assertIsNone(self.coordinator._network_operation_lease)
            self.assertEqual(self.coordinator.capture_status()["snapshot"]["device_state"], "idle")
        finally:
            self.release.set()
            caller.join(5)
            self.assertFalse(caller.is_alive())

    def test_multiple_keys_share_one_worker_and_conflicting_intent_is_rejected(self):
        self.block_verification()
        first = self.coordinator.request_stop_capture(fixtures.stop_command("owner"))
        self.assertEqual(first.status, 202)
        self.assertTrue(self.entered.wait(2))
        worker = self.coordinator._async_stop_worker
        for index in range(12):
            result = self.coordinator.request_stop_capture(fixtures.stop_command(f"alias-{index}"))
            self.assertEqual(result.status, 202)
            self.assertIs(self.coordinator._async_stop_worker, worker)
        with self.assertRaises(ProviderError) as raised:
            self.coordinator.request_stop_capture(
                fixtures.stop_command("different-intent", reason="safe_swap")
            )
        self.assertEqual(raised.exception.code, "stop_intent_conflict")
        self.release.set()
        self.wait_finished()

    def test_safe_swap_receipt_waits_for_full_verification(self):
        self.block_verification()
        result = self.coordinator.request_stop_capture(
            fixtures.stop_command("safe-async", reason="safe_swap")
        )
        self.assertEqual(result.status, 202)
        self.assertTrue(self.entered.wait(2))
        self.assertIsNone(self.coordinator.current_safe_swap_receipt())
        self.assertIsNotNone(self.coordinator._network_operation_lease)
        with self.assertRaises(ProviderError) as raised:
            self.coordinator._acquire_representation()
        self.assertEqual(raised.exception.code, "volume_releasing")
        self.release.set()
        self.wait_finished()
        receipt = self.coordinator.current_safe_swap_receipt()["receipt"]
        self.assertEqual(receipt["release_state"], "device-released")
        self.assertEqual(receipt["open_handle_count"], 0)

    def test_worker_start_failure_does_not_cache_success_or_reserve_the_recorder(self):
        command = fixtures.stop_command("thread-start-failed")
        with (
            patch("rp_ylx.recording.coordinator.threading.Thread.start", side_effect=RuntimeError),
            self.assertRaises(ProviderError) as raised,
        ):
            self.coordinator.request_stop_capture(command)
        self.assertEqual(raised.exception.code, "stop_worker_unavailable")
        self.assertIsNone(self.coordinator._stop_inflight)
        self.assertIsNone(self.coordinator._async_stop_worker)
        self.assertNotIn(
            (command.principal_id, "stop", command.idempotency_key), self.coordinator._commands
        )
        self.assertEqual(self.coordinator.capture_status()["snapshot"]["device_state"], "recording")
        result = self.coordinator.request_stop_capture(command)
        self.assertEqual(result.status, 202)
        self.wait_finished()

    def test_close_waits_for_background_integrity_check(self):
        self.block_verification()
        self.coordinator.request_stop_capture(fixtures.stop_command("close-wait"))
        self.assertTrue(self.entered.wait(2))
        closed = threading.Event()
        closer = threading.Thread(target=lambda: (self.coordinator.close(), closed.set()))
        closer.start()
        try:
            self.assertFalse(closed.wait(0.1))
            self.release.set()
            self.assertTrue(closed.wait(4))
        finally:
            self.release.set()
            closer.join(5)

    def test_failed_durable_acceptance_cancels_worker_and_same_key_can_retry(self):
        command = fixtures.stop_command("acceptance-write-failed")
        recorder = self.coordinator._active
        with (
            patch.object(
                self.coordinator, "_persist_local_state", side_effect=OSError("disk full")
            ),
            patch.object(recorder, "stop", wraps=recorder.stop) as stop,
        ):
            with self.assertRaisesRegex(OSError, "disk full"):
                self.coordinator.request_stop_capture(command)
            stop.assert_not_called()
        self.assertIsNone(self.coordinator._stop_inflight)
        self.assertIsNone(self.coordinator._async_stop_worker)
        self.assertIs(self.coordinator._active, recorder)
        self.assertEqual(self.coordinator.capture_status()["snapshot"]["device_state"], "recording")
        result = self.coordinator.request_stop_capture(command)
        self.assertFalse(result.replayed)
        self.wait_finished()

    def test_accepted_receipt_is_durable_before_verification_finishes(self):
        self.block_verification()
        command = fixtures.stop_command("durable-accepted")
        accepted = self.coordinator.request_stop_capture(command)
        self.assertTrue(self.entered.wait(2))
        scope = (command.principal_id, "stop", command.idempotency_key)
        self.assertIs(
            self.coordinator._commands[scope], self.coordinator._persisted_commands[scope]
        )
        self.release.set()
        self.wait_finished()
        self.coordinator.close()
        reopened = self.fixture.coordinator()
        try:
            replay = reopened.request_stop_capture(command)
            self.assertTrue(replay.replayed)
            self.assertEqual(replay.body, accepted.body)
            self.assertIsNone(reopened._async_stop_worker)
        finally:
            reopened.close()

    def test_old_accepted_request_does_not_stop_a_new_take(self):
        command = fixtures.stop_command("old-accepted")
        accepted = self.coordinator.request_stop_capture(command)
        self.wait_finished()
        self.coordinator.start_capture(fixtures.start_command("new-take"))
        session = self.fixture.active_session_id(self.coordinator)
        replay = self.coordinator.request_stop_capture(command)
        self.assertTrue(replay.replayed)
        self.assertEqual(replay.body, accepted.body)
        self.assertEqual(self.fixture.active_session_id(self.coordinator), session)
        self.assertIsNone(self.coordinator._async_stop_worker)

    def test_unexpected_verification_error_is_visible_in_retained_diagnostics(self):
        with patch.object(
            self.coordinator, "_verify_exact_session_payload", side_effect=OSError("injected read")
        ):
            self.coordinator.request_stop_capture(fixtures.stop_command("async-io-failure"))
            self.wait_finished()
        status = self.coordinator.capture_status()["snapshot"]
        self.assertIsNone(status["active_recording"])
        retained = status["retained_unsuccessful"]["recording_state"]
        self.assertTrue(
            any(row["code"] == "stop_finalization_failed" for row in retained["diagnostics"])
        )
        self.assertIsNone(self.coordinator.current_safe_swap_receipt())

    def test_failed_content_verification_never_publishes_safe_swap(self):
        original = self.coordinator._verify_exact_session_payload

        def invalid(*args, **kwargs):
            verified, _ = original(*args, **kwargs)
            return verified, DeviceRecordingError("artifact_corrupt", "injected content mismatch")

        with patch.object(self.coordinator, "_verify_exact_session_payload", invalid):
            self.coordinator.request_stop_capture(
                fixtures.stop_command("bad-safe-swap", reason="safe_swap")
            )
            self.wait_finished()
        self.assertIsNone(self.coordinator.current_safe_swap_receipt())
        self.assertIsNone(self.coordinator._pending_safe_swap)
        self.assertFalse(self.coordinator._released)

    def test_safe_swap_rejects_existing_reader_before_acceptance(self):
        release = self.coordinator._acquire_representation()
        command = fixtures.stop_command("reader-safe-swap", reason="safe_swap")
        try:
            with self.assertRaises(ProviderError) as raised:
                self.coordinator.request_stop_capture(command)
            self.assertEqual(raised.exception.code, "safe_swap_blocked")
            self.assertIsNone(self.coordinator._async_stop_worker)
            self.assertEqual(
                self.coordinator.capture_status()["snapshot"]["device_state"], "recording"
            )
        finally:
            release()
        self.coordinator.request_stop_capture(command)
        self.wait_finished()
        self.assertIsNotNone(self.coordinator.current_safe_swap_receipt())

    def test_real_http_returns_accepted_while_integrity_is_blocked(self):
        self.block_verification()
        gateway = gateway_fixtures.GatewayHttpTest()
        gateway.setUp()
        gateway.server.provider = self.coordinator
        try:
            headers = {"Origin": gateway.base, "Idempotency-Key": "http-async"}
            body = {"schema": "ylx.capture-stop.v2", "reason": "user"}
            status, payload, _ = gateway.request(
                "/api/v4/capture/stop", token="reader-token", headers=headers, body=body
            )
            self.assertEqual(status, 202)
            self.assertTrue(self.entered.wait(2))
            status, replay, response_headers = gateway.request(
                "/api/v4/capture/stop", token="reader-token", headers=headers, body=body
            )
            self.assertEqual(status, 202)
            self.assertEqual(json.loads(replay), json.loads(payload))
            self.assertEqual(response_headers["Idempotency-Replayed"], "true")
            status, pending, _ = gateway.request("/api/v4/capture/status", token="reader-token")
            self.assertEqual(status, 200)
            self.assertIsNotNone(json.loads(pending)["snapshot"]["active_recording"])
            self.release.set()
            self.wait_finished()
        finally:
            self.release.set()
            gateway.tearDown()

    def test_sync_stop_waits_for_async_error_recovery(self):
        original = self.coordinator._retain_active_failure
        recovering, recover = threading.Event(), threading.Event()
        sync_done = threading.Event()
        results = []

        def recovery(*args, **kwargs):
            recovering.set()
            if not recover.wait(4):
                raise TimeoutError("test did not release recovery")
            return original(*args, **kwargs)

        with (
            patch.object(self.coordinator, "_retain_active_failure", recovery),
            patch.object(
                self.coordinator,
                "_verify_exact_session_payload",
                side_effect=OSError("read failure"),
            ),
        ):
            self.coordinator.request_stop_capture(fixtures.stop_command("async-error"))
            self.assertTrue(recovering.wait(2))

            def sync_stop():
                results.append(self.coordinator.stop_capture(fixtures.stop_command("sync-wait")))
                sync_done.set()

            waiter = threading.Thread(target=sync_stop)
            waiter.start()
            try:
                self.assertFalse(sync_done.wait(0.1))
                recover.set()
                self.assertTrue(sync_done.wait(4))
                self.wait_finished()
                self.assertEqual(results[0].status, 204)
            finally:
                recover.set()
                waiter.join(5)


if __name__ == "__main__":
    unittest.main()
