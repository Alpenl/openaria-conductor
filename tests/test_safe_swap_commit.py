import multiprocessing
import sqlite3
import threading
import traceback
import unittest
from unittest.mock import patch

from rp_ylx.api import ProviderError
from tests import test_capture_coordinator as fixtures
from tests import test_stop_state_commit as helpers


class SafeSwapCommitTest(unittest.TestCase):
    cleanup_workers = helpers.StopStateCommitTest.cleanup_workers
    launch = helpers.StopStateCommitTest.launch
    assert_responsive = helpers.StopStateCommitTest.assert_responsive
    assert_busy_start = helpers.StopStateCommitTest.assert_busy_start
    assert_lease_held = helpers.StopStateCommitTest.assert_lease_held

    def setUp(self):
        helpers.StopStateCommitTest.setUp(self)
        self.command = fixtures.stop_command("safe-swap", reason="safe_swap")

    def delay_commit(self, stage):
        c = self.coordinator
        entered = threading.Event()
        original = c._state_db.put_capture_state

        def write(value, commands, *args, **kwargs):
            if threading.current_thread().name == "stop":
                phase = (
                    "intent"
                    if value["pending_safe_swap"] is not None
                    else "receipt"
                    if any(x["operation"] == "stop" for x in commands)
                    else "publication"
                )
                if c._active is None and phase == stage:
                    entered.set()
                    if not self.release.wait(10):
                        raise TimeoutError("safe-swap commit test")
            return original(value, commands, *args, **kwargs)

        with patch.object(c._state_db, "put_capture_state", side_effect=write):
            stopped = self.launch("stop", lambda: c.stop_capture(self.command))
            try:
                self.assertTrue(entered.wait(3))
                self.assert_responsive()
                self.assert_busy_start()
                self.assert_lease_held()
                got = self.launch("receipt", c.current_safe_swap_receipt)
                self.assertTrue(got.wait(0.25))
                if stage != "receipt":
                    self.assertIsNone(self.results["receipt"])
                    self.assertFalse(c._released)
                    self.assertIsNone(c._state_db.capture_state()["safe_swap_resource"])
                else:
                    self.assertEqual(self.results["receipt"]["receipt"]["session_id"], self.sid)
                duplicate = self.launch("duplicate", lambda: c.stop_capture(self.command))
                self.assertFalse(duplicate.wait(0.05))
                self.assertFalse(stopped.is_set())
                with self.assertRaises(ProviderError):
                    c._acquire_representation()
                self.assert_lease_held()
                if stage == "publication":
                    closed = self.launch("close", c.close)
                    self.assertFalse(closed.wait(0.05))
                    self.assert_responsive()
                    self.assert_lease_held()
            finally:
                self.cleanup_workers()
        self.assertFalse(self.errors, self.errors)
        self.assertEqual(self.results["stop"].status, 204)
        self.assertTrue(self.results["duplicate"].replayed)
        self.assertIsNone(c._stop_inflight)
        self.assertIsNone(c._network_operation_lease)
        self.assertTrue(c._released)
        self.assertEqual(
            c.current_safe_swap_receipt(),
            c._state_db.capture_state()["safe_swap_resource"],
        )

    def test_slow_intent_commit_keeps_status_responsive(self):
        self.delay_commit("intent")

    def test_slow_publication_does_not_expose_uncommitted_safe_release(self):
        self.delay_commit("publication")

    def test_slow_command_receipt_keeps_status_responsive_and_lease_held(self):
        self.delay_commit("receipt")

    def failed_publication(self, error):
        c = self.coordinator
        original = c._state_db.put_capture_state

        def fail(value, *args, **kwargs):
            if value["safe_swap_resource"] is not None:
                raise error
            return original(value, *args, **kwargs)

        expected = ProviderError if isinstance(error, (OSError, sqlite3.Error)) else type(error)
        with (
            patch.object(c._state_db, "put_capture_state", side_effect=fail),
            self.assertRaises(expected),
        ):
            c.stop_capture(self.command)
        self.assertIsNone(c.current_safe_swap_receipt())
        self.assertFalse(c._released)
        self.assertIsNone(c._safe_swap_to_persist)
        self.assertIsNotNone(c._pending_safe_swap)
        self.assertIsNone(c._state_db.capture_state()["safe_swap_resource"])
        self.assertIsNone(c._stop_inflight)
        self.assertIsNone(c._network_operation_lease)
        with patch.object(self.recorder, "stop", side_effect=AssertionError("must not reseal")):
            self.assertEqual(c.stop_capture(self.command).status, 204)
        self.assertIsNotNone(c.current_safe_swap_receipt())

    def test_sqlite_publication_failure_never_exposes_false_release(self):
        self.failed_publication(sqlite3.OperationalError("locked database"))

    def test_io_publication_failure_retains_retryable_intent(self):
        self.failed_publication(OSError("full device"))

    def test_publication_cancellation_does_not_expose_false_release(self):
        self.failed_publication(KeyboardInterrupt())

    def retry_completion(self, api):
        c = self.coordinator
        original = c._state_db.put_capture_state

        def failed(value, commands, *args, **kwargs):
            if value["safe_swap_resource"] is not None and any(
                x["operation"] == "stop" for x in commands
            ):
                raise OSError("completion receipt unavailable")
            return original(value, commands, *args, **kwargs)

        with (
            patch.object(c._state_db, "put_capture_state", side_effect=failed),
            self.assertRaises(OSError),
        ):
            c.stop_capture(self.command)
        self.assertIsNotNone(c.current_safe_swap_receipt())
        self.assertFalse(
            any(x["operation"] == "stop" for x in c._state_db.capture_state()["commands"])
        )
        entered = threading.Event()

        def slow(*args, **kwargs):
            entered.set()
            if not self.release.wait(10):
                raise TimeoutError("safe-swap completion retry")
            return original(*args, **kwargs)

        with (
            patch.object(c._state_db, "put_capture_state", side_effect=slow),
            patch.object(c, "_publish_safe_swap", side_effect=AssertionError("already published")),
        ):
            method = c.request_stop_capture if api else c.stop_capture
            self.launch("retry", lambda: method(self.command))
            try:
                self.assertTrue(entered.wait(3))
                self.assert_responsive()
                self.assert_busy_start()
            finally:
                self.cleanup_workers()
        self.assertFalse(self.errors, self.errors)
        self.assertTrue(self.results["retry"].replayed)
        self.assertEqual(self.results["retry"].status, 204)
        self.assertTrue(
            any(x["operation"] == "stop" for x in c._state_db.capture_state()["commands"])
        )

    def test_sync_completion_receipt_retry_does_not_block_status(self):
        self.retry_completion(False)

    def test_api_completion_receipt_retry_does_not_block_status(self):
        self.retry_completion(True)

    def delay_resume(self, asynchronous):
        c = self.coordinator
        with (
            patch.object(c, "_publish_safe_swap", side_effect=OSError("interrupted handoff")),
            self.assertRaises(OSError),
        ):
            c.stop_capture(self.command)
        entered = threading.Event()
        original = c._verify_pending_safe_swap

        def verify(pending):
            entered.set()
            if not self.release.wait(10):
                raise TimeoutError("safe-swap verification test")
            return original(pending)

        with patch.object(c, "_verify_pending_safe_swap", side_effect=verify):
            method = c.request_stop_capture if asynchronous else c.stop_capture
            stopped = self.launch("stop", lambda: method(self.command))
            try:
                self.assertTrue(entered.wait(3))
                self.assert_responsive()
                self.assert_busy_start()
                self.assert_lease_held()
                self.assertIsNone(c.current_safe_swap_receipt())
                if asynchronous:
                    self.assertTrue(stopped.wait(0.25))
                    self.assertEqual(self.results["stop"].status, 202)
                else:
                    self.assertFalse(stopped.is_set())
            finally:
                self.cleanup_workers()
                with c._stop_condition:
                    self.assertTrue(
                        c._stop_condition.wait_for(lambda: c._async_stop_worker is None, timeout=5)
                    )
        self.assertFalse(self.errors, self.errors)
        self.assertIsNotNone(c.current_safe_swap_receipt())

    def test_sync_resume_verification_keeps_status_responsive(self):
        self.delay_resume(False)

    def test_async_resume_verification_keeps_status_responsive(self):
        self.delay_resume(True)


class SafeSwapCommitCrashTest(unittest.TestCase):
    def crash(self, after):
        fixture = fixtures.CaptureCoordinatorTest()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        context = multiprocessing.get_context("fork")
        receive, send = context.Pipe(duplex=False)
        self.addCleanup(receive.close)
        self.addCleanup(send.close)
        command = fixtures.stop_command("crash-handoff", reason="safe_swap")

        def child():
            try:
                c = fixture.coordinator()
                c.start_capture(fixtures.start_command("crash-start"))
                sid = fixture.active_session_id(c)
                self.assertTrue(c.submit_frame(fixtures.frame()))
                original = c._state_db.put_capture_state

                def boundary(value, *args, **kwargs):
                    if value["safe_swap_resource"] is not None:
                        if after:
                            original(value, *args, **kwargs)
                        send.send({"session_id": sid})
                        threading.Event().wait()
                    return original(value, *args, **kwargs)

                with patch.object(c._state_db, "put_capture_state", side_effect=boundary):
                    c.stop_capture(command)
            except BaseException:
                send.send({"error": traceback.format_exc()})
                raise

        worker = context.Process(target=child)
        worker.start()
        try:
            self.assertTrue(receive.poll(5), "publication boundary was not reached")
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
            resource = restarted.current_safe_swap_receipt()
            if after:
                self.assertEqual(resource["receipt"]["session_id"], event["session_id"])
                self.assertTrue(restarted._released)
            else:
                self.assertIsNone(resource)
                self.assertIsNotNone(restarted._pending_safe_swap)
                self.assertFalse(restarted._released)
                self.assertEqual(restarted.stop_capture(command).status, 204)
                self.assertEqual(
                    restarted.current_safe_swap_receipt()["receipt"]["session_id"],
                    event["session_id"],
                )
        finally:
            restarted.close()

    def test_kill_before_publication_preserves_only_pending_intent(self):
        self.crash(False)

    def test_kill_after_publication_recovers_durable_safe_release(self):
        self.crash(True)
