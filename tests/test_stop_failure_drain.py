"""A failed finalizing checkpoint must not close a live writer's resources."""

import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import rp_ylx.recording.device_session as session
from tests import test_split_eye_recording as fixtures


class StopFailureDrainTests(unittest.TestCase):
    def check_failure(self, failure_site):
        entered, release, cleanup, done = (threading.Event() for _ in range(4))
        errors = []

        def block(role, payload):
            if role == "frames.index":
                entered.set()
                if not release.wait(10):
                    raise TimeoutError("test writer gate was not released")

        with tempfile.TemporaryDirectory() as temporary:
            recorder, encoders, _ = fixtures.SplitEyeRecordingTest().build(
                Path(temporary), before_write=block
            )
            recorder.start()
            fixtures.SplitEyeRecordingTest.feed(recorder, 3)
            self.assertTrue(entered.wait(3))
            writer = recorder._writer
            join, close = writer.join, recorder._close_files
            write, sink = session.write_json_atomic, recorder._state_sink
            injected = OSError("injected finalizing checkpoint failure")

            def observed_join(timeout=None):
                cleanup.set()
                return join(timeout)

            def observed_close(**kwargs):
                cleanup.set()
                return close(**kwargs)

            def failed_write(path, document, **kwargs):
                if (
                    failure_site == "file"
                    and path.name == "recording.json"
                    and document["state"] == "finalizing"
                ):
                    raise injected
                return write(path, document, **kwargs)

            def failed_sink(document):
                if failure_site == "sink" and document["state"] == "finalizing":
                    raise injected
                return sink(document)

            def stop():
                try:
                    recorder.stop()
                except BaseException as error:
                    errors.append(error)
                finally:
                    done.set()

            worker = threading.Thread(target=stop)
            try:
                with (
                    patch.object(session, "write_json_atomic", side_effect=failed_write),
                    patch.object(recorder, "_state_sink", side_effect=failed_sink),
                    patch.object(recorder, "_close_files", side_effect=observed_close),
                    patch.object(writer, "join", side_effect=observed_join),
                ):
                    worker.start()
                    self.assertTrue(cleanup.wait(3))
                    self.assertFalse(
                        encoders[0].aborted, "encoder aborted while its writer was active"
                    )
                    self.assertTrue(recorder._files)
                    self.assertTrue(all(not f.closed for f in recorder._files.values()))
                    self.assertFalse(done.is_set())
                    release.set()
                    worker.join(5)
                self.assertFalse(worker.is_alive())
                self.assertFalse(writer.is_alive())
                self.assertIsNone(recorder._writer)
                self.assertIsNone(recorder._writer_error)
                self.assertEqual(recorder._frames_written, 3)
                self.assertEqual(len(errors), 1)
                self.assertIsInstance(errors[0], session.DeviceRecordingError)
                self.assertIs(errors[0].__cause__, injected)
                self.assertEqual(recorder._files, {})
                self.assertTrue(encoders[0].aborted)
                document = json.loads((recorder.partial_path / "recording.json").read_text())
                self.assertIn(document["state"], {"failed", "recoverable"})
                self.assertIn("injected finalizing", document["diagnostics"][0]["message"])
                self.assertFalse((recorder.partial_path / "manifest.json").exists())
            finally:
                release.set()
                if worker.ident is not None:
                    worker.join(5)
                recorder._stop_writer()
                recorder.abort()

    def test_checkpoint_file_failure_waits_for_inflight_writer(self):
        self.check_failure("file")

    def test_checkpoint_sink_failure_waits_for_inflight_writer(self):
        self.check_failure("sink")
