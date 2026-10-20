"""A timed-out writer still owns its files until its thread exits."""

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.recording.device_session import DeviceRecordingError
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
