import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from rp_ylx.recording import device_session as d
from tests import test_audio_inventory as fixtures


class AudioInventoryPipelineTests(unittest.TestCase):
    def exercise(self, fail_second):
        helper = fixtures.AudioInventoryTests()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = helper.multiple_audio(root, 6)
            recorder = helper.recorder(root)
            recorder._audio_result = {"previous": True}
            recorder._audio_segment_records = ["previous"]
            recorder._audio_bytes = 123
            initial = threading.Barrier(2, timeout=5)
            third_started = threading.Event()
            release = threading.Event()
            returned = threading.Event()
            errors = []
            submitted = []
            original = d._verified_finalization

            class TrackedPool(ThreadPoolExecutor):
                def submit(self, fn, *args, **kwargs):
                    submitted.append(args[0].name)
                    return super().submit(fn, *args, **kwargs)

            def read(path, expected_bytes, *, code):
                index = int(path.stem.split("_")[-1])
                if index < 2:
                    initial.wait()
                if index == 1:
                    if fail_second:
                        if not third_started.wait(5):
                            raise AssertionError("third read never started")
                        raise d.DeviceRecordingError("audio_invalid", "second read failed")
                    if not release.wait(5):
                        raise AssertionError("second read not released")
                if index == 2:
                    third_started.set()
                    if fail_second and not release.wait(5):
                        raise AssertionError("inflight third read not released")
                return original(path, expected_bytes, code=code)

            def apply():
                try:
                    recorder._apply_audio_result(result)
                except BaseException as error:
                    errors.append(error)
                finally:
                    returned.set()

            with (
                patch.object(d, "_verified_finalization", side_effect=read),
                patch.object(d, "ThreadPoolExecutor", TrackedPool),
            ):
                caller = threading.Thread(target=apply)
                caller.start()
                try:
                    self.assertTrue(
                        third_started.wait(1),
                        "free worker must start the next read before the stalled second finishes",
                    )
                    self.assertFalse(returned.wait(0.05))
                    self.assertEqual(
                        submitted, ["audio_00000.wav", "audio_00001.wav", "audio_00002.wav"]
                    )
                    self.assertEqual(recorder._audio_result, {"previous": True})
                finally:
                    release.set()
                    caller.join(6)
            self.assertFalse(caller.is_alive())
            if fail_second:
                self.assertEqual(len(errors), 1)
                self.assertIsInstance(errors[0], d.DeviceRecordingError)
                self.assertEqual(len(submitted), 3)
                self.assertEqual(recorder._audio_segment_records, ["previous"])
                self.assertEqual(recorder._audio_bytes, 123)
                self.assertEqual(recorder._artifact_identities, {})
                self.assertEqual(recorder._verified_artifacts, {})
            else:
                self.assertEqual(errors, [])
                self.assertEqual(
                    [item["index"] for item in recorder._audio_segment_records], list(range(6))
                )
                self.assertEqual(recorder._audio_bytes, 360)
                self.assertEqual(len(recorder._verified_artifacts), 6)

    def test_replenishes_vacant_worker_without_unbounded_read_ahead(self):
        self.exercise(False)

    def test_failure_waits_for_replenished_read_without_mutating_state(self):
        self.exercise(True)


if __name__ == "__main__":
    unittest.main()
