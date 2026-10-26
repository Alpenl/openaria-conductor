import multiprocessing
import threading
import traceback
import unittest
from unittest.mock import patch

from rp_ylx.api import ProviderError
from rp_ylx.recording.device_session import DeviceRecordingError
from tests import test_capture_coordinator as fixtures
from tests import test_stop_state_commit as helpers


class StopAcceptanceTest(unittest.TestCase):
    cleanup_workers = helpers.StopStateCommitTest.cleanup_workers
    launch = helpers.StopStateCommitTest.launch
    assert_responsive = helpers.StopStateCommitTest.assert_responsive
    assert_busy_start = helpers.StopStateCommitTest.assert_busy_start
    assert_lease_held = helpers.StopStateCommitTest.assert_lease_held

    def setUp(self):
        helpers.StopStateCommitTest.setUp(self)
        self.worker_release = threading.Event()
        self.addCleanup(self.worker_release.set)

    def wait_finished(self):
        with self.coordinator._stop_condition:
            self.assertTrue(
                self.coordinator._stop_condition.wait_for(
                    lambda: self.coordinator._async_stop_worker is None, timeout=5
                )
            )

    def slow_accept(self, failure=None):
        c = self.coordinator
        entered = threading.Event()
        original = c._state_db.put_capture_state
        worker_before = {t.ident for t in threading.enumerate()}

        def put(value, commands, *args, **kwargs):
            if threading.current_thread().name == "request" and any(
                x["operation"] == "stop" for x in commands
            ):
                entered.set()
                if not self.release.wait(10):
                    raise TimeoutError("acceptance commit test")
                if failure is not None:
                    raise failure
            return original(value, commands, *args, **kwargs)

        with (
            patch.object(c._state_db, "put_capture_state", side_effect=put),
            patch.object(c, "_finish_stop_capture", wraps=c._finish_stop_capture) as finish,
        ):
            self.launch("request", lambda: c.request_stop_capture(self.command))
            try:
                self.assertTrue(entered.wait(3))
                self.assert_responsive()
                self.assert_busy_start()
                self.assert_lease_held()
                finish.assert_not_called()
                self.assertFalse(
                    any(x["operation"] == "stop" for x in c._state_db.capture_state()["commands"])
                )
                for command in (self.command, fixtures.stop_command("other")):
                    with self.assertRaises(ProviderError) as raised:
                        c.request_stop_capture(command)
                    self.assertEqual(raised.exception.code, "capture_busy")
            finally:
                self.cleanup_workers()
            self.wait_finished()
        if failure is not None:
            self.assertIs(self.errors.pop("request"), failure)
            self.assertIs(c._active, self.recorder)
            self.assertIsNone(c._stop_inflight)
            self.assertIsNone(c._stop_acceptance_inflight)
            self.assertEqual(worker_before, {t.ident for t in threading.enumerate()})
            self.assertFalse(any(x[1] == "stop" for x in c._commands))
            self.assertEqual(c.request_stop_capture(self.command).status, 202)
            self.wait_finished()
        else:
            self.assertEqual(self.results["request"].status, 202)
        self.assertFalse(self.errors, self.errors)
        self.assertIsNone(c._active)
        self.assertIsNone(c._stop_acceptance_inflight)
        self.assertTrue(c.request_stop_capture(self.command).replayed)

    def test_slow_acceptance_keeps_status_available_and_worker_gated(self):
        self.slow_accept()

    def test_failed_acceptance_cancels_worker_and_allows_same_request_retry(self):
        self.slow_accept(OSError("acceptance disk full"))

    def test_cancelled_acceptance_reaps_worker_without_stopping_recording(self):
        self.slow_accept(KeyboardInterrupt())

    def source_failure_during_acceptance(self, commit_fails):
        c = self.coordinator
        entered = threading.Event()
        original = c._state_db.put_capture_state
        plan = c._active_plan

        def put(*args, **kwargs):
            if threading.current_thread().name == "request":
                entered.set()
                if not self.release.wait(10):
                    raise TimeoutError("source failure acceptance")
                if commit_fails:
                    raise OSError("acceptance failed after source callback")
            return original(*args, **kwargs)

        with patch.object(c._state_db, "put_capture_state", side_effect=put):
            self.launch("request", lambda: c.request_stop_capture(self.command))
            try:
                self.assertTrue(entered.wait(3))
                reported = self.launch(
                    "source-failure",
                    lambda: c._retain_active_failure(
                        DeviceRecordingError(
                            "camera_read_failed", "source failed during acceptance"
                        ),
                        plan,
                        defer_recovery=True,
                    ),
                )
                self.assertTrue(reported.wait(0.25))
                self.assert_responsive()
            finally:
                self.cleanup_workers()
            with c._stop_condition:
                self.assertTrue(
                    c._stop_condition.wait_for(
                        lambda: c._async_stop_worker is None and c._failure_inflight is None,
                        timeout=5,
                    )
                )
        if commit_fails:
            self.assertIsInstance(self.errors.pop("request"), OSError)
        self.assertFalse(self.errors, self.errors)
        self.assertIsNone(c._active, "deferred source failure was stranded by acceptance rollback")
        retained = c.capture_status()["snapshot"]["retained_unsuccessful"]
        self.assertIsNotNone(retained)
        self.assertIn(
            "camera_read_failed", [x["code"] for x in retained["recording_state"]["diagnostics"]]
        )

    def test_source_failure_during_successful_acceptance_is_retained(self):
        self.source_failure_during_acceptance(False)

    def test_source_failure_during_failed_acceptance_is_not_stranded(self):
        self.source_failure_during_acceptance(True)

    def duplicate(self, failure):
        c = self.coordinator
        working, entered = threading.Event(), threading.Event()
        original_finish = c._finish_stop_capture
        original_write = c._state_db.put_capture_state
        other = fixtures.stop_command("other-stop")

        def finish(*args, **kwargs):
            working.set()
            if not self.worker_release.wait(10):
                raise TimeoutError("test worker gate")
            return original_finish(*args, **kwargs)

        def put(*args, **kwargs):
            if threading.current_thread().name == "duplicate":
                entered.set()
                if not self.release.wait(10):
                    raise TimeoutError("duplicate acceptance write")
                if failure:
                    raise OSError("duplicate receipt unavailable")
            return original_write(*args, **kwargs)

        with patch.object(c, "_finish_stop_capture", side_effect=finish):
            accepted = c.request_stop_capture(self.command)
            self.assertTrue(working.wait(3))
            try:
                with patch.object(c._state_db, "put_capture_state", side_effect=put):
                    self.launch("duplicate", lambda: c.request_stop_capture(other))
                    try:
                        self.assertTrue(entered.wait(3))
                        self.assert_responsive()
                        self.assert_busy_start()
                        replay = c.request_stop_capture(self.command)
                        self.assertTrue(replay.replayed)
                        self.assertEqual(replay.body, accepted.body)
                    finally:
                        self.cleanup_workers()
                if failure:
                    self.assertIsInstance(self.errors.pop("duplicate"), OSError)
                    self.assertNotIn(
                        (other.principal_id, "stop", other.idempotency_key),
                        c._commands,
                    )
                    self.assertEqual(c.request_stop_capture(other).status, 202)
                else:
                    self.assertEqual(self.results["duplicate"].status, 202)
                self.assertFalse(self.errors, self.errors)
            finally:
                self.worker_release.set()
                self.wait_finished()
        self.assertIsNone(c._stop_acceptance_inflight)
        self.assertTrue(c.request_stop_capture(other).replayed)

    def test_duplicate_receipt_commit_keeps_status_and_durable_replay_available(self):
        self.duplicate(False)

    def test_duplicate_commit_failure_does_not_cancel_original_worker(self):
        self.duplicate(True)

    def resume(self, fail):
        c = self.coordinator
        command = fixtures.stop_command("handoff", reason="safe_swap")
        with patch.object(c, "_publish_safe_swap", side_effect=OSError("handoff unavailable")):
            original_accepted = c.request_stop_capture(command)
            self.wait_finished()
        with self.assertRaises(ProviderError):
            c.current_safe_swap_receipt()
        failure = c._pending_safe_swap_failure
        original = c._state_db.put_capture_state
        entered = threading.Event()

        def put(value, *args, **kwargs):
            if threading.current_thread().name == "resume":
                entered.set()
                if not self.release.wait(10):
                    raise TimeoutError("resume acceptance")
                if fail:
                    raise OSError("resume acceptance disk full")
            return original(value, *args, **kwargs)

        with (
            patch.object(c._state_db, "put_capture_state", side_effect=put),
            patch.object(c, "_publish_safe_swap", wraps=c._publish_safe_swap) as publish,
        ):
            self.launch("resume", lambda: c.request_stop_capture(command))
            try:
                self.assertTrue(entered.wait(3))
                self.assert_responsive()
                self.assert_busy_start()
                self.assert_lease_held()
                publish.assert_not_called()
                with self.assertRaises(ProviderError):
                    c.current_safe_swap_receipt()
                self.assertEqual(c._state_db.capture_state()["pending_safe_swap_failure"], failure)
            finally:
                self.cleanup_workers()
                self.wait_finished()
        if fail:
            self.assertIsInstance(self.errors.pop("resume"), OSError)
            self.assertIsNone(c._stop_acceptance_inflight)
            self.assertEqual(c._pending_safe_swap_failure, failure)
            c.request_stop_capture(command)
            self.wait_finished()
        else:
            self.assertEqual(self.results["resume"].body, original_accepted.body)
            self.assertTrue(self.results["resume"].replayed)
        self.assertFalse(self.errors, self.errors)
        self.assertIsNotNone(c.current_safe_swap_receipt())

    def test_resume_acceptance_keeps_failure_visible_until_commit(self):
        self.resume(False)

    def test_resume_acceptance_failure_retains_failure_and_allows_retry(self):
        self.resume(True)

    def test_idle_receipt_commit_keeps_status_available_and_close_waits(self):
        c = self.coordinator
        c.stop_capture(self.command)
        entered = threading.Event()
        original = c._state_db.put_capture_state

        def put(*args, **kwargs):
            if threading.current_thread().name == "idle":
                entered.set()
                if not self.release.wait(10):
                    raise TimeoutError("idle stop acceptance")
            return original(*args, **kwargs)

        with patch.object(c._state_db, "put_capture_state", side_effect=put):
            self.launch("idle", lambda: c.request_stop_capture(fixtures.stop_command("idle")))
            try:
                self.assertTrue(entered.wait(3))
                self.assert_responsive()
                self.assert_busy_start()
                closed = self.launch("close", c.close)
                self.assertFalse(closed.wait(0.05))
                self.assert_responsive()
            finally:
                self.cleanup_workers()
        self.assertFalse(self.errors, self.errors)
        self.assertEqual(self.results["idle"].status, 204)


class StopAcceptanceCrashTest(unittest.TestCase):
    def crash(self, after):
        fixture = fixtures.CaptureCoordinatorTest()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        context = multiprocessing.get_context("fork")
        receive, send = context.Pipe(duplex=False)
        self.addCleanup(receive.close)
        self.addCleanup(send.close)
        command = fixtures.stop_command("crash-acceptance")

        def child():
            try:
                c = fixture.coordinator()
                c.start_capture(fixtures.start_command("crash-start"))
                self.assertTrue(c.submit_frame(fixtures.frame()))
                original = c._state_db.put_capture_state

                def put(value, commands, *args, **kwargs):
                    if any(x["operation"] == "stop" for x in commands):
                        if after:
                            original(value, commands, *args, **kwargs)
                        send.send(
                            {
                                "accepted": next(
                                    x["body"] for x in commands if x["operation"] == "stop"
                                )
                            }
                        )
                        threading.Event().wait()
                    return original(value, commands, *args, **kwargs)

                with patch.object(c._state_db, "put_capture_state", side_effect=put):
                    c.request_stop_capture(command)
            except BaseException:
                send.send({"error": traceback.format_exc()})
                raise

        worker = context.Process(target=child)
        worker.start()
        try:
            self.assertTrue(receive.poll(5))
            event = receive.recv()
            self.assertNotIn("error", event, event)
            worker.kill()
            worker.join(5)
            self.assertEqual(worker.exitcode, -9)
        finally:
            if worker.is_alive():
                worker.kill()
                worker.join(5)
            worker.close()
        restarted = fixture.coordinator()
        try:
            self.assertIsNone(restarted._active)
            replay = restarted.request_stop_capture(command)
            self.assertEqual(replay.status, 202 if after else 204)
            self.assertEqual(replay.replayed, after)
            if after:
                self.assertEqual(replay.body, event["accepted"])
            self.assertIsNone(restarted._async_stop_worker)
        finally:
            restarted.close()

    def test_kill_before_acceptance_does_not_replay_success(self):
        self.crash(False)

    def test_kill_after_acceptance_replays_stable_receipt_without_new_worker(self):
        self.crash(True)
