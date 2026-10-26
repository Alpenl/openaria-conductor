"""A timed-out writer still owns its files until its thread exits."""

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.api import ProviderError
from rp_ylx.recording.device_session import DeviceRecordingError
from tests import test_capture_coordinator as capture_fixtures
from tests import test_split_eye_recording as fixtures


class WriterShutdownTests(unittest.TestCase):
    def check_shutdown(self, operation, existing_error=False):
        entered = threading.Event()
        release = threading.Event()
        deadline = threading.Event()
        done = threading.Event()
        errors = []

        def block(role, payload):
            if role == "frames.index":
                entered.set()
                if not release.wait(5):
                    raise TimeoutError("test did not release writer")

        with tempfile.TemporaryDirectory() as temporary:
            recorder, encoders, _ = fixtures.SplitEyeRecordingTest().build(
                Path(temporary), before_write=block
            )
            recorder.start()
            recorder.submit_frame(fixtures.frame(0))
            self.assertTrue(entered.wait(2))
            writer = recorder._writer
            original_join = writer.join
            prior = OSError("earlier failure") if existing_error else None
            recorder._writer_error = prior

            def fast_deadline(timeout=None):
                if timeout == 10:
                    deadline.set()
                    return
                original_join(timeout)

            def shutdown():
                try:
                    if operation == "fail":
                        recorder.fail("source_failed", "test source failure")
                    elif operation == "stop":
                        recorder.stop()
                    else:
                        recorder._stop_writer()
                except BaseException as error:
                    errors.append(error)
                finally:
                    done.set()

            worker = threading.Thread(target=shutdown)
            try:
                with patch.object(writer, "join", side_effect=fast_deadline):
                    worker.start()
                    self.assertTrue(deadline.wait(2))
                    self.assertFalse(done.wait(0.05), "shutdown returned with a live writer")
                    self.assertIs(recorder._writer, writer)
                    self.assertTrue(writer.is_alive())
                    self.assertTrue(recorder._files)
                    self.assertTrue(all(not stream.closed for stream in recorder._files.values()))
                    self.assertFalse(encoders[0].aborted)
                    self.assertFalse((recorder.partial_path / "recovery").exists())
                    if existing_error:
                        self.assertIs(recorder._writer_error, prior)
                    else:
                        self.assertIsInstance(recorder._writer_error, TimeoutError)
                    release.set()
                    worker.join(5)
                    self.assertFalse(worker.is_alive())
                    self.assertFalse(writer.is_alive())
                    self.assertIsNone(recorder._writer)
            finally:
                release.set()
                worker.join(5)
                original_join(5)
                recorder.abort()
            if operation == "stop":
                self.assertEqual(len(errors), 1)
                self.assertIsInstance(errors[0], DeviceRecordingError)
            else:
                self.assertEqual(errors, [])

    def test_stop_writer_retains_resources_until_exit(self):
        self.check_shutdown("writer")

    def test_failure_recovery_waits_for_writer_exit(self):
        self.check_shutdown("fail")

    def test_sealing_error_cleanup_waits_for_writer_exit(self):
        self.check_shutdown("stop")

    def test_existing_error_is_preserved_while_waiting(self):
        self.check_shutdown("writer", existing_error=True)


class CoordinatorShutdownTests(unittest.TestCase):
    def setUp(self):
        self.fixture = capture_fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.coordinator = self.fixture.coordinator(checkpoint_interval=0)
        self.addCleanup(self.coordinator.close)
        self.coordinator.start_capture(capture_fixtures.start_command("close-start"))

    def test_close_releases_revision_lock_and_serializes_other_close_callers(self):
        coordinator = self.coordinator
        active = coordinator._active
        entered, release = threading.Event(), threading.Event()
        errors = []
        abort = active.abort

        def blocked_abort():
            entered.set()
            if not release.wait(5):
                raise TimeoutError("test did not release close")
            return abort()

        def close():
            try:
                coordinator.close()
            except BaseException as error:
                errors.append(error)

        callers = [threading.Thread(target=close) for _ in range(2)]
        with patch.object(active, "abort", side_effect=blocked_abort) as called:
            callers[0].start()
            try:
                self.assertTrue(entered.wait(2))
                acquired = coordinator._lock.acquire(timeout=0.2)
                self.assertTrue(acquired, "close owns the checkpoint revision lock")
                if acquired:
                    coordinator._lock.release()
                coordinator._next_revision()
                with self.assertRaises(ProviderError):
                    coordinator.start_capture(capture_fixtures.start_command("during-close"))
                with self.assertRaises(ProviderError):
                    coordinator.request_stop_capture(capture_fixtures.stop_command("during-close"))
                callers[1].start()
            finally:
                release.set()
                for caller in callers:
                    if caller.ident is not None:
                        caller.join(5)
                        self.assertFalse(caller.is_alive())
            self.assertEqual(called.call_count, 1)
        self.assertEqual(errors, [])
        self.assertIsNone(coordinator._active)
        self.assertIsNone(coordinator._stop_inflight)
        self.assertIsNone(coordinator._network_operation_lease)

    def test_abort_failure_releases_close_reservation_and_allows_cleanup_retry(self):
        coordinator = self.coordinator
        active = coordinator._active
        with (
            patch.object(active, "abort", side_effect=OSError("storage failure")),
            self.assertRaisesRegex(OSError, "storage failure"),
        ):
            coordinator.close()
        self.assertIsNone(coordinator._stop_inflight)
        self.assertIsNone(coordinator._network_operation_lease)
        self.assertIs(coordinator._active, active)
        coordinator.close()
        self.assertIsNone(coordinator._active)
