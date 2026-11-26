from __future__ import annotations

import json
import multiprocessing
import signal
import threading
import traceback
from unittest.mock import patch

from rp_ylx.api import ProviderError
from rp_ylx.recording_layout import resolve_recording
from tests.testkit import (
    SAFE_SWAP_V3,
    CoordinatorMixin,
    GatewayHarness,
    frame,
    start_command,
    stop_command,
)


class SafeSwapFailureTest(CoordinatorMixin):
    def wait_finished(self, coordinator):
        with coordinator._stop_condition:
            self.assertTrue(
                coordinator._stop_condition.wait_for(
                    lambda: coordinator._async_stop_worker is None, timeout=4
                )
            )

    def kill_before_release(self):
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
                coordinator = self.harness.coordinator()
                coordinator.start_capture(start_command("killed-start-before_release"))
                session_id = self.harness.active_session_id(coordinator)
                self.assertTrue(coordinator.submit_frame(frame()))
                coordinator._publish_safe_swap = boundary
                accepted = coordinator.request_stop_capture(command)
                message(
                    {
                        "accepted": accepted.body,
                        "status": accepted.status,
                        "session_id": session_id,
                    }
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
                self.assertTrue(receive.poll(10), "child did not reach durability boundary")
                message = receive.recv()
                self.assertNotIn("error", message, message)
                messages.append(message)
            self.assertIn({"boundary": "before_release"}, messages)
            accepted = next(item for item in messages if "accepted" in item)
            self.assertEqual(accepted["status"], 202)
            process.kill()
            process.join(5)
            self.assertEqual(process.exitcode, -signal.SIGKILL)
            return command, accepted
        finally:
            if process.is_alive():
                process.kill()
                process.join(5)
            receive.close()
            process.close()

    def pending_with_changed_manifest(self):
        command, accepted = self.kill_before_release()
        final = resolve_recording(self.harness.mountpoint / "recordings", accepted["session_id"])
        manifest = json.loads((final / "manifest.json").read_bytes())
        artifact = final / "manifest.json"
        original = artifact.read_bytes()
        manifest["display_name"] += " changed"
        artifact.write_text(json.dumps(manifest))
        return command, accepted, artifact, original

    def failed_pending(self):
        command, accepted, artifact, original = self.pending_with_changed_manifest()
        coordinator = self.harness.coordinator()
        self.addCleanup(coordinator.close)
        coordinator.request_stop_capture(command)
        self.wait_finished(coordinator)
        return coordinator, command, accepted, artifact, original

    def assert_failure(self, coordinator, code="safe_swap_unverified"):
        with self.assertRaises(ProviderError) as raised:
            coordinator.current_safe_swap_receipt()
        self.assertEqual(raised.exception.code, code)
        self.assertTrue(raised.exception.retryable)
        self.assertFalse(coordinator._released)
        return raised.exception

    def test_failed_resume_has_typed_http_error_instead_of_missing_receipt(self):
        command, accepted, _, _ = self.pending_with_changed_manifest()
        with (
            self.harness.coordinator() as coordinator,
            GatewayHarness(provider=coordinator) as gateway,
        ):
            coordinator.request_stop_capture(command)
            self.wait_finished(coordinator)
            status, payload, _ = gateway.request("/api/v4/capture/safe-swap", token="reader-token")
            self.assertEqual(status, 500, "Known failed handoff is indistinguishable from pending")
            body = json.loads(payload)
            self.assertEqual(body["error"]["code"], "safe_swap_unverified")
            self.assertTrue(body["error"]["retryable"])
            self.assertEqual(body["error"]["details"]["session_id"], accepted["session_id"])

    def test_safe_swap_get_reports_unexpected_provider_failure_without_disconnect(self):
        with GatewayHarness() as gateway:
            gateway.provider.safe_swap = SAFE_SWAP_V3
            with patch.object(
                gateway.provider,
                "current_safe_swap_receipt",
                side_effect=OSError("private device path"),
            ):
                status, payload, _ = gateway.request(
                    "/api/v4/capture/safe-swap", token="reader-token"
                )
            body = json.loads(payload)
            self.assertEqual(status, 500)
            self.assertEqual(body["error"]["code"], "provider_failure")
            self.assertNotIn("private device path", payload.decode())
            status, _, _ = gateway.request("/api/v4/capture/safe-swap", token="reader-token")
            self.assertEqual(status, 200)

    def test_failure_survives_restart_and_successful_retry_clears_it(self):
        coordinator, command, accepted, artifact, original = self.failed_pending()
        self.assert_failure(coordinator)
        coordinator.close()
        restarted = self.harness.coordinator()
        self.addCleanup(restarted.close)
        failure = self.assert_failure(restarted)
        self.assertEqual(failure.details["session_id"], accepted["session_id"])
        artifact.write_bytes(original)
        entered, release = threading.Event(), threading.Event()
        verify = restarted._check_pending_safe_swap

        def blocked(pending):
            entered.set()
            if not release.wait(4):
                raise TimeoutError("test did not release verification")
            return verify(pending)

        with patch.object(restarted, "_check_pending_safe_swap", blocked):
            try:
                replay = restarted.request_stop_capture(command)
                self.assertEqual(replay.body, accepted["accepted"])
                self.assertTrue(entered.wait(2))
                self.assertIsNone(restarted.current_safe_swap_receipt())
                self.assertIsNone(restarted._read_local_state()["pending_safe_swap_failure"])
            finally:
                release.set()
            self.wait_finished(restarted)
        receipt = restarted.current_safe_swap_receipt()
        self.assertEqual(receipt["receipt"]["session_id"], accepted["session_id"])
        restarted.close()
        final = self.harness.coordinator()
        self.addCleanup(final.close)
        self.assertEqual(final.current_safe_swap_receipt(), receipt)

    def test_new_capture_does_not_inherit_previous_handoff_failure(self):
        coordinator, _, accepted, _, _ = self.failed_pending()
        coordinator.start_capture(start_command("new-after-handoff-failure"))
        self.assertNotEqual(self.harness.active_session_id(coordinator), accepted["session_id"])
        self.assertIsNone(coordinator.current_safe_swap_receipt())
        self.assertIsNone(coordinator._read_local_state()["pending_safe_swap_failure"])
        coordinator.submit_frame(frame())
        coordinator.stop_capture(stop_command("finish-new-after-failure"))

    def test_persisted_error_for_another_session_is_not_restored(self):
        coordinator, _, _, _, _ = self.failed_pending()
        coordinator.close()
        value = coordinator._state_db.capture_state()
        value["pending_safe_swap_failure"]["session_id"] = "unrelated-session"
        coordinator._state_db.put_capture_state(value, [], set())
        restarted = self.harness.coordinator()
        self.addCleanup(restarted.close)
        self.assertIsNone(restarted.current_safe_swap_receipt())
        self.assertIsNotNone(restarted._pending_safe_swap)
        self.assertFalse(restarted._released)

    def test_retry_receipt_persistence_failure_restores_failure_without_work(self):
        coordinator, command, _, _, _ = self.failed_pending()
        with (
            patch.object(
                coordinator, "_persist_local_state", side_effect=OSError("SQLite unavailable")
            ),
            patch.object(coordinator, "_check_pending_safe_swap") as verify,
        ):
            with self.assertRaises(OSError):
                coordinator.request_stop_capture(command)
            self.assert_failure(coordinator)
            verify.assert_not_called()
            self.assertIsNone(coordinator._network_operation_lease)
            self.assertIsNone(coordinator._async_stop_worker)

    def test_retry_thread_start_failure_preserves_known_failure(self):
        coordinator, command, _, _, _ = self.failed_pending()
        with (
            patch.object(threading.Thread, "start", side_effect=RuntimeError("no thread")),
            self.assertRaises(ProviderError) as raised,
        ):
            coordinator.request_stop_capture(command)
        self.assertEqual(raised.exception.code, "stop_worker_unavailable")
        self.assert_failure(coordinator)
        self.assertIsNone(coordinator._network_operation_lease)

    def test_unknown_worker_error_is_visible_even_when_failure_cannot_be_persisted(self):
        command, _, _, _ = self.pending_with_changed_manifest()
        coordinator = self.harness.coordinator()
        self.addCleanup(coordinator.close)
        with (
            patch.object(
                coordinator,
                "_check_pending_safe_swap",
                side_effect=OSError("private filesystem path"),
            ),
            patch.object(coordinator, "_persist_local_state", side_effect=OSError("disk failed")),
        ):
            coordinator.request_stop_capture(command)
            self.wait_finished(coordinator)
        failure = self.assert_failure(coordinator, "safe_swap_resume_failed")
        self.assertNotIn("private filesystem path", failure.message)
        self.assertIsNone(coordinator._network_operation_lease)
        self.assertIsNone(coordinator._stop_inflight)

    def test_fresh_async_publish_failure_is_reported_and_retryable(self):
        coordinator = self.harness.coordinator()
        self.addCleanup(coordinator.close)
        coordinator.start_capture(start_command("fresh-handoff"))
        coordinator.submit_frame(frame())
        command = stop_command("fresh-handoff-stop", reason="safe_swap")
        with patch.object(
            coordinator,
            "_publish_safe_swap",
            side_effect=ProviderError("safe_swap_publish_failed", "write failed", status=500),
        ):
            accepted = coordinator.request_stop_capture(command)
            self.wait_finished(coordinator)
        self.assert_failure(coordinator, "safe_swap_publish_failed")
        self.assertIsNotNone(coordinator._read_local_state()["pending_safe_swap_failure"])
        replay = coordinator.request_stop_capture(command)
        self.assertEqual(replay.body, accepted.body)
        self.wait_finished(coordinator)
        self.assertIsNotNone(coordinator.current_safe_swap_receipt())

    def test_fresh_changed_manifest_keeps_retryable_handoff_intent(self):
        coordinator = self.harness.coordinator()
        self.addCleanup(coordinator.close)
        coordinator.start_capture(start_command("fresh-integrity"))
        coordinator.submit_frame(frame())
        command = stop_command("fresh-integrity-stop", reason="safe_swap")
        with patch.object(
            coordinator,
            "_check_pending_safe_swap",
            side_effect=ProviderError("safe_swap_unverified", "manifest changed", status=409),
        ):
            coordinator.request_stop_capture(command)
            self.wait_finished(coordinator)
        self.assert_failure(coordinator)
        self.assertIsNotNone(coordinator._pending_safe_swap)
        coordinator.request_stop_capture(command)
        self.wait_finished(coordinator)
        self.assertIsNotNone(coordinator.current_safe_swap_receipt())
