"""Real SIGKILL at accepted-stop durability boundaries, with isolated fake capture."""

from __future__ import annotations

import multiprocessing
import signal
import threading
import traceback
import unittest

from rp_ylx.recording import validate_device_session_directory
from tests import test_capture_coordinator as fixtures


class AsyncStopCrashTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)

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
