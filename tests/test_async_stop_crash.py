"""Real SIGKILL at accepted-stop durability boundaries, with isolated fake capture."""

from __future__ import annotations

import json
import multiprocessing
import signal
import threading
import traceback
import unittest
from unittest.mock import patch

from rp_ylx.api import ProviderError
from rp_ylx.recording import validate_device_session_directory
from tests import test_capture_coordinator as fixtures


class AsyncStopCrashTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)

    def wait_finished(self, coordinator):
        with coordinator._stop_condition:
            self.assertTrue(
                coordinator._stop_condition.wait_for(
                    lambda: coordinator._async_stop_worker is None, timeout=4
                )
            )

    def kill_at(self, stage, reason="user"):
        context = multiprocessing.get_context("fork")
        receive, send = context.Pipe(duplex=False)
        command = fixtures.stop_command("killed-stop-" + stage, reason=reason)

        def child():
            lock = threading.Lock()

            def message(value):
                with lock:
                    send.send(value)

            def boundary(*args, **kwargs):
                message({"boundary": stage})
                threading.Event().wait()

            try:
                coordinator = self.fixture.coordinator()
                coordinator.start_capture(fixtures.start_command("killed-start-" + stage))
                session_id = self.fixture.active_session_id(coordinator)
                self.assertTrue(coordinator.submit_frame(fixtures.frame()))
                if stage == "before_seal":
                    coordinator._active.stop = boundary
                elif stage == "before_verification":
                    coordinator._verify_exact_session_payload = boundary
                elif stage == "before_release":
                    coordinator._publish_safe_swap = boundary
                elif stage == "after_release":
                    original = coordinator._publish_safe_swap

                    def published():
                        original()
                        boundary()

                    coordinator._publish_safe_swap = published
                else:
                    raise AssertionError(stage)
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
                self.assertTrue(receive.poll(10), "child did not reach durability boundary")
                message = receive.recv()
                self.assertNotIn("error", message, message)
                messages.append(message)
            self.assertIn({"boundary": stage}, messages)
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

    def replay_and_continue(self, stage):
        command, accepted = self.kill_at(stage)
        coordinator = self.fixture.coordinator()
        try:
            self.assertIsNone(coordinator.capture_status()["snapshot"]["active_recording"])
            replay = coordinator.request_stop_capture(command)
            self.assertEqual(replay.status, 202)
            self.assertTrue(replay.replayed)
            self.assertEqual(replay.body, accepted["accepted"])
            self.assertIsNone(coordinator._async_stop_worker)
            self.assertIsNone(coordinator.current_safe_swap_receipt())
            final = self.fixture.mountpoint / "recordings" / accepted["session_id"]
            if stage == "before_verification":
                validate_device_session_directory(final)
                listed = coordinator.list_sessions(cursor=None, limit=50, take_id=None)
                self.assertEqual(len(listed["items"]), 1)
            else:
                self.assertFalse(final.exists(), "unsealed stop was promoted as completed")
            coordinator.start_capture(fixtures.start_command("after-kill"))
            new_id = self.fixture.active_session_id(coordinator)
            self.assertNotEqual(new_id, accepted["session_id"])
            coordinator.request_stop_capture(command)
            self.assertEqual(self.fixture.active_session_id(coordinator), new_id)
            coordinator.submit_frame(fixtures.frame())
            coordinator.stop_capture(fixtures.stop_command("finish-after-kill"))
        finally:
            coordinator.close()

    def test_kill_after_acceptance_before_seal_preserves_receipt_and_recovers(self):
        self.replay_and_continue("before_seal")

    def test_kill_after_seal_before_verification_recovers_complete_content(self):
        self.replay_and_continue("before_verification")

    def test_kill_before_safe_release_replays_only_after_zero_handles(self):
        command, accepted = self.kill_at("before_release", "safe_swap")
        coordinator = self.fixture.coordinator()
        try:
            self.assertIsNone(coordinator.current_safe_swap_receipt())
            self.assertIsNotNone(coordinator._pending_safe_swap)
            self.assertEqual(coordinator.open_handle_count, 0)
            replay = coordinator.request_stop_capture(command)
            self.assertTrue(replay.replayed)
            self.assertEqual(replay.body, accepted["accepted"])
            self.wait_finished(coordinator)
            receipt = coordinator.current_safe_swap_receipt()["receipt"]
            self.assertEqual(receipt["session_id"], accepted["session_id"])
            self.assertEqual(receipt["release_state"], "device-released")
            self.assertEqual(receipt["open_handle_count"], 0)
        finally:
            coordinator.close()

    def test_kill_after_release_preserves_both_release_and_accepted_receipts(self):
        command, accepted = self.kill_at("after_release", "safe_swap")
        coordinator = self.fixture.coordinator()
        try:
            receipt = coordinator.current_safe_swap_receipt()["receipt"]
            self.assertEqual(receipt["session_id"], accepted["session_id"])
            replay = coordinator.request_stop_capture(command)
            self.assertTrue(replay.replayed)
            self.assertEqual(replay.body, accepted["accepted"])
            self.assertEqual(coordinator.current_safe_swap_receipt()["receipt"], receipt)
        finally:
            coordinator.close()

    def test_pending_release_rechecks_content_changed_while_process_was_dead(self):
        command, accepted = self.kill_at("before_release", "safe_swap")
        final = self.fixture.mountpoint / "recordings" / accepted["session_id"]
        manifest = json.loads((final / "manifest.json").read_bytes())
        artifact = final / manifest["video"]["segments"][0]["artifacts"]["left"]["path"]
        payload = artifact.read_bytes()
        artifact.write_bytes(bytes([payload[0] ^ 1]) + payload[1:])
        coordinator = self.fixture.coordinator()
        try:
            self.assertIsNone(coordinator.current_safe_swap_receipt())
            coordinator.request_stop_capture(command)
            self.wait_finished(coordinator)
            self.assertIsNone(coordinator.current_safe_swap_receipt())
            self.assertFalse(coordinator._released)
            self.assertNotIn(accepted["session_id"], coordinator._verified)
            # The synchronous internal path must enforce the same content rule.
            with self.assertRaises(ProviderError) as raised:
                coordinator.stop_capture(fixtures.stop_command("sync-invalid", reason="safe_swap"))
            self.assertEqual(raised.exception.code, "safe_swap_unverified")
            artifact.write_bytes(payload)
            replay = coordinator.request_stop_capture(command)
            self.assertTrue(replay.replayed)
            self.assertEqual(replay.body, accepted["accepted"])
            self.wait_finished(coordinator)
            self.assertEqual(
                coordinator.current_safe_swap_receipt()["receipt"]["session_id"],
                accepted["session_id"],
            )
        finally:
            coordinator.close()

    def test_pending_release_remains_bound_to_original_manifest_bytes(self):
        command, accepted = self.kill_at("before_release", "safe_swap")
        final = self.fixture.mountpoint / "recordings" / accepted["session_id"]
        path = final / "manifest.json"
        original = path.read_bytes()
        path.write_bytes(original + b"\n")
        coordinator = self.fixture.coordinator()
        try:
            coordinator.request_stop_capture(command)
            self.wait_finished(coordinator)
            self.assertIsNone(coordinator.current_safe_swap_receipt())
            path.write_bytes(original)
            coordinator.request_stop_capture(command)
            self.wait_finished(coordinator)
            self.assertIsNotNone(coordinator.current_safe_swap_receipt())
        finally:
            coordinator.close()

    def test_pending_release_reverification_does_not_block_http_acceptance_or_status(self):
        command, accepted = self.kill_at("before_release", "safe_swap")
        coordinator = self.fixture.coordinator()
        entered, release, returned, status_returned = (threading.Event() for _ in range(4))
        original = coordinator._verify_exact_session_payload
        results, errors = [], []

        def verify(*args, **kwargs):
            entered.set()
            if not release.wait(5):
                raise TimeoutError("reverification was not released")
            return original(*args, **kwargs)

        def request():
            try:
                results.append(coordinator.request_stop_capture(command))
            except BaseException as error:
                errors.append(error)
            finally:
                returned.set()

        def status():
            coordinator.capture_status()
            status_returned.set()

        caller = threading.Thread(target=request)
        reader = threading.Thread(target=status)
        try:
            with patch.object(coordinator, "_verify_exact_session_payload", verify):
                caller.start()
                self.assertTrue(entered.wait(2))
                reader.start()
                self.assertTrue(returned.wait(0.5), "pending replay still waits for full hashing")
                self.assertTrue(status_returned.wait(0.5), "pending verification blocks status")
                self.assertEqual(errors, [])
                self.assertEqual(results[0].body, accepted["accepted"])
                self.assertTrue(results[0].replayed)
                self.assertIsNone(coordinator.current_safe_swap_receipt())
                release.set()
                with coordinator._stop_condition:
                    self.assertTrue(
                        coordinator._stop_condition.wait_for(
                            lambda: coordinator._async_stop_worker is None, timeout=4
                        )
                    )
                self.assertIsNotNone(coordinator.current_safe_swap_receipt())
        finally:
            release.set()
            if caller.ident is not None:
                caller.join(5)
            if reader.ident is not None:
                reader.join(5)
            coordinator.close()

    def test_pending_new_key_worker_start_failure_can_retry_without_false_receipt(self):
        self.kill_at("before_release", "safe_swap")
        coordinator = self.fixture.coordinator()
        command = fixtures.stop_command("resume-new-key", reason="safe_swap")
        try:
            with (
                patch(
                    "rp_ylx.recording.coordinator.threading.Thread.start", side_effect=RuntimeError
                ),
                self.assertRaises(ProviderError) as raised,
            ):
                coordinator.request_stop_capture(command)
            self.assertEqual(raised.exception.code, "stop_worker_unavailable")
            self.assertIsNone(coordinator._async_stop_worker)
            self.assertIsNone(coordinator._network_operation_lease)
            self.assertIsNone(coordinator.current_safe_swap_receipt())
            self.assertNotIn(
                (command.principal_id, "stop", command.idempotency_key), coordinator._commands
            )
            result = coordinator.request_stop_capture(command)
            self.assertFalse(result.replayed)
            self.wait_finished(coordinator)
            self.assertIsNotNone(coordinator.current_safe_swap_receipt())
        finally:
            coordinator.close()

    def test_pending_new_key_acceptance_failure_cancels_worker_without_releasing(self):
        self.kill_at("before_release", "safe_swap")
        coordinator = self.fixture.coordinator()
        command = fixtures.stop_command("resume-write-failed", reason="safe_swap")
        try:
            with (
                patch.object(coordinator, "_persist_local_state", side_effect=OSError("disk full")),
                patch.object(coordinator, "_verify_pending_safe_swap") as verify,
            ):
                with self.assertRaisesRegex(OSError, "disk full"):
                    coordinator.request_stop_capture(command)
                verify.assert_not_called()
            self.assertIsNone(coordinator._async_stop_worker)
            self.assertIsNone(coordinator._network_operation_lease)
            self.assertIsNone(coordinator.current_safe_swap_receipt())
            self.assertIsNotNone(coordinator._pending_safe_swap)
            result = coordinator.request_stop_capture(command)
            self.assertFalse(result.replayed)
            self.wait_finished(coordinator)
            self.assertIsNotNone(coordinator.current_safe_swap_receipt())
        finally:
            coordinator.close()
