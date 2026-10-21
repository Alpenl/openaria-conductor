import errno
import fcntl
import multiprocessing
import os
import threading
import traceback
import unittest
from unittest.mock import patch

from rp_ylx.api import ProviderError
from tests import test_capture_coordinator as fixtures


class StopStateCommitTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.coordinator = self.fixture.coordinator()
        self.addCleanup(self.coordinator.close)
        self.coordinator.start_capture(fixtures.start_command("start"))
        self.assertTrue(self.coordinator.submit_frame(fixtures.frame()))
        self.sid = self.fixture.active_session_id(self.coordinator)
        self.command = fixtures.stop_command("stop")
        self.recorder = self.coordinator._active
        self.workers = []
        self.results = {}
        self.errors = {}
        self.release = threading.Event()
        self.addCleanup(self.cleanup_workers)

    def cleanup_workers(self):
        self.release.set()
        for worker in self.workers:
            worker.join(5)
        self.assertTrue(all(not worker.is_alive() for worker in self.workers))

    def launch(self, name, function):
        done = threading.Event()

        def work():
            try:
                self.results[name] = function()
            except BaseException as error:
                self.errors[name] = error
            finally:
                done.set()

        worker = threading.Thread(target=work, name=name)
        self.workers.append(worker)
        worker.start()
        return done

    def assert_responsive(self):
        self.assertTrue(self.launch("status", self.coordinator.capture_status).wait(.25))
        self.assertNotIn("status", self.errors)
        fixtures.validate_capture_status(self.results["status"])

    def assert_busy_start(self):
        done = self.launch("new-start", lambda: self.coordinator.start_capture(
            fixtures.start_command("new-start"),
        ))
        self.assertTrue(done.wait(.25))
        error = self.errors.pop("new-start")
        self.assertIsInstance(error, ProviderError)
        self.assertEqual(error.code, "capture_busy")

    def assert_lease_held(self):
        fd = os.open(self.coordinator._network_operation_lock_path(), os.O_RDONLY)
        try:
            with self.assertRaises(BlockingIOError):
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(fd)

    def assert_committed(self):
        state = self.coordinator._state_db.capture_state()
        self.assertIsNone(state["active_session"])
        receipts = [c for c in state["commands"] if c["operation"] == "stop"
                    and c["idempotency_key"] == self.command.idempotency_key]
        self.assertEqual(len(receipts), 1)
        self.assertEqual(receipts[0]["status"], 202)
        self.assertIsNone(self.coordinator._stop_inflight)
        self.assertIsNone(self.coordinator._network_operation_lease)
        return receipts[0]

    def test_slow_commit_is_responsive_and_does_not_admit_new_mutations(self):
        coordinator = self.coordinator
        entered = threading.Event()
        original = coordinator._state_db.put_capture_state

        def write(value, *args, **kwargs):
            if threading.current_thread().name == "stop" and coordinator._active is None:
                entered.set()
                if not self.release.wait(10):
                    raise TimeoutError("test stop commit")
            return original(value, *args, **kwargs)

        with patch.object(coordinator._state_db, "put_capture_state", side_effect=write):
            stopped = self.launch("stop", lambda: coordinator.stop_capture(self.command))
            try:
                self.assertTrue(entered.wait(3))
                state = coordinator._state_db.capture_state()
                self.assertEqual(state["active_session"]["session_id"], self.sid)
                self.assertFalse(any(c["operation"] == "stop" for c in state["commands"]))
                self.assert_responsive()
                self.assert_busy_start()
                self.assert_lease_held()
                duplicate = self.launch("duplicate", lambda: coordinator.stop_capture(self.command))
                self.assertFalse(duplicate.wait(.05))
                for name, command in (("same-api", self.command),
                                      ("other-api", fixtures.stop_command("other-stop"))):
                    done = self.launch(name, lambda command=command:
                                       coordinator.request_stop_capture(command))
                    self.assertTrue(done.wait(.25))
                    error = self.errors.pop(name)
                    self.assertIsInstance(error, ProviderError)
                    self.assertEqual(error.code, "capture_busy")
                self.assertFalse(stopped.is_set())
                self.assert_lease_held()
            finally:
                self.cleanup_workers()
        self.assertFalse(self.errors, self.errors)
        self.assertEqual(self.results["stop"].status, 202)
        self.assertTrue(self.results["duplicate"].replayed)
        self.assertEqual(self.results["stop"].body, self.results["duplicate"].body)
        receipt = self.assert_committed()
        self.assertEqual(receipt["body"], self.results["stop"].body)

    def retry_failed_commit(self, api):
        coordinator = self.coordinator
        original = coordinator._state_db.put_capture_state

        def failed(value, *args, **kwargs):
            if coordinator._active is None:
                raise OSError(errno.ENOSPC, "final state unavailable")
            return original(value, *args, **kwargs)

        with patch.object(self.recorder, "stop", wraps=self.recorder.stop) as stop:
            with (
                patch.object(coordinator._state_db, "put_capture_state", side_effect=failed),
                self.assertRaises(OSError),
            ):
                coordinator.stop_capture(self.command)
            entered = threading.Event()

            def delayed(value, *args, **kwargs):
                entered.set()
                if not self.release.wait(10):
                    raise TimeoutError("test retry commit")
                return original(value, *args, **kwargs)

            call = coordinator.request_stop_capture if api else coordinator.stop_capture
            with patch.object(coordinator._state_db, "put_capture_state", side_effect=delayed):
                self.launch("retry", lambda: call(self.command))
                try:
                    self.assertTrue(entered.wait(3))
                    self.assert_responsive()
                    self.assert_busy_start()
                finally:
                    self.cleanup_workers()
            self.assertEqual(stop.call_count, 1)
        self.assertFalse(self.errors, self.errors)
        self.assertTrue(self.results["retry"].replayed)
        self.assertEqual(self.results["retry"].status, 202)
        self.assert_committed()

    def test_sync_retry_repeats_only_failed_state_commit(self):
        self.retry_failed_commit(False)

    def test_api_retry_repeats_only_failed_state_commit(self):
        self.retry_failed_commit(True)

    def retry_background_commit(self, different_key=False):
        coordinator = self.coordinator
        original = coordinator._state_db.put_capture_state

        def failed(value, *args, **kwargs):
            if (threading.current_thread().name == "rp-ylx-stop-finalization"
                    and coordinator._active is None):
                raise OSError(errno.ENOSPC, "final state unavailable")
            return original(value, *args, **kwargs)

        with patch.object(coordinator._state_db, "put_capture_state", side_effect=failed):
            accepted = coordinator.request_stop_capture(self.command)
            with coordinator._stop_condition:
                self.assertTrue(coordinator._stop_condition.wait_for(
                    lambda: coordinator._async_stop_worker is None, timeout=5,
                ))
        self.assertIsNotNone(coordinator._state_db.capture_state()["active_session"])
        self.assert_busy_start()
        replay = coordinator.request_stop_capture(
            fixtures.stop_command("retry-new-key") if different_key else self.command,
        )
        if different_key:
            self.assertEqual(replay.status, 204)
        else:
            self.assertTrue(replay.replayed)
            self.assertEqual(replay.body, accepted.body)
        with coordinator._stop_condition:
            self.assertTrue(coordinator._stop_condition.wait_for(
                lambda: coordinator._async_stop_worker is None, timeout=5,
            ))
        self.assert_committed()

    def test_background_final_commit_failure_can_be_retried(self):
        self.retry_background_commit()

    def test_different_stop_key_can_retry_failed_background_commit(self):
        self.retry_background_commit(different_key=True)

    def test_close_waits_for_slow_completed_state_commit(self):
        coordinator = self.coordinator
        entered = threading.Event()
        original = coordinator._state_db.put_capture_state

        def delayed(value, *args, **kwargs):
            if coordinator._active is None:
                entered.set()
                if not self.release.wait(10):
                    raise TimeoutError("test close wait")
            return original(value, *args, **kwargs)

        with patch.object(coordinator._state_db, "put_capture_state", side_effect=delayed):
            self.launch("stop", lambda: coordinator.stop_capture(self.command))
            try:
                self.assertTrue(entered.wait(3))
                closed = self.launch("close", coordinator.close)
                self.assertFalse(closed.wait(.05))
                self.assert_responsive()
                self.assert_lease_held()
            finally:
                self.cleanup_workers()
        self.assertFalse(self.errors, self.errors)
        self.assert_committed()


    def test_other_completed_state_commit_clears_pending_marker(self):
        coordinator = self.coordinator
        original = coordinator._state_db.put_capture_state

        def failed(value, *args, **kwargs):
            if coordinator._active is None:
                raise OSError(errno.ENOSPC, "commit unavailable")
            return original(value, *args, **kwargs)

        with (
            patch.object(coordinator._state_db, "put_capture_state", side_effect=failed),
            self.assertRaises(OSError),
        ):
            coordinator.stop_capture(self.command)
        self.assertIsNotNone(coordinator._pending_stop_state_commit)
        # The idle safe-swap request retains its historical no-op semantics;
        # its successful state write must also settle the completed recording.
        result = coordinator.stop_capture(fixtures.stop_command("idle-swap", reason="safe_swap"))
        self.assertEqual(result.status, 204)
        self.assertIsNone(coordinator._pending_stop_state_commit)
        self.assert_committed()
        coordinator.start_capture(fixtures.start_command("after-committed-stop"))
        self.assertIsNotNone(coordinator._active)


class StopStateCommitCrashTest(unittest.TestCase):
    def crash(self, after):
        fixture = fixtures.CaptureCoordinatorTest()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        context = multiprocessing.get_context("fork")
        receive, send = context.Pipe(duplex=False)
        self.addCleanup(receive.close)
        self.addCleanup(send.close)
        command = fixtures.stop_command("crash-completed-state")

        def child():
            try:
                coordinator = fixture.coordinator()
                coordinator.start_capture(fixtures.start_command("crash-start"))
                sid = fixture.active_session_id(coordinator)
                self.assertTrue(coordinator.submit_frame(fixtures.frame()))
                original = coordinator._state_db.put_capture_state

                def boundary(value, *args, **kwargs):
                    if coordinator._active is None:
                        if after:
                            original(value, *args, **kwargs)
                        send.send({"session_id": sid, "after": after})
                        threading.Event().wait()
                    return original(value, *args, **kwargs)

                with patch.object(coordinator._state_db, "put_capture_state", side_effect=boundary):
                    coordinator.stop_capture(command)
            except BaseException:
                send.send({"error": traceback.format_exc()})
                raise

        worker = context.Process(target=child)
        worker.start()
        try:
            self.assertTrue(receive.poll(5), "stop commit boundary was not reached")
            event = receive.recv()
            self.assertNotIn("error", event, event)
            worker.kill()
            worker.join(5)
            self.assertFalse(worker.is_alive())
            self.assertEqual(worker.exitcode, -9)
        finally:
            if worker.is_alive():
                worker.kill()
                worker.join(5)
        from rp_ylx.persistence import StateDatabase
        database = StateDatabase(fixture.state_root / "state.sqlite3")
        state = database.capture_state()
        receipts = [x for x in state["commands"] if x["operation"] == "stop"]
        self.assertEqual(len(receipts), 1 if after else 0)
        if after:
            self.assertIsNone(state["active_session"])
            self.assertEqual(receipts[0]["status"], 202)
        else:
            self.assertEqual(state["active_session"]["session_id"], event["session_id"])
        restarted = fixture.coordinator()
        try:
            page = restarted.list_sessions(cursor=None, limit=50, take_id=None, api_version="v4")
            self.assertIn(event["session_id"], [x["session_id"] for x in page["items"]])
            replay = restarted.stop_capture(command)
            self.assertEqual(replay.status, 202 if after else 204)
            self.assertEqual(replay.replayed, after)
            self.assertIsNone(restarted._state_db.capture_state()["active_session"])
        finally:
            restarted.close()

    def test_kill_before_commit_retains_old_state_without_success_receipt(self):
        self.crash(False)

    def test_kill_after_commit_keeps_completed_state_and_receipt_together(self):
        self.crash(True)
