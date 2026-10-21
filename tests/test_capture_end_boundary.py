"""Recording time follows capture, including real audio tail, not sealing I/O."""

import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from rp_ylx.recording.device_session import DeviceRecordingError
from tests import test_split_eye_recording as fixture


class CaptureEndBoundaryTest(unittest.TestCase):
    def build(self, root, *, audio=False):
        recorder, _, audios = fixture.SplitEyeRecordingTest().build(root, audio_enabled=audio)
        recorder.start()
        fixture.SplitEyeRecordingTest.feed(recorder, 4)
        self.addCleanup(
            lambda: recorder.abort() if recorder.state in {"recording", "finalizing"} else None
        )
        return recorder, audios

    def test_python_file_flush_does_not_extend_duration_or_wall_end(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder, _ = self.build(Path(directory))
            wall_stop = recorder._now()
            close = recorder._sync_and_close_files

            def delayed_close():
                close()
                time.sleep(0.03)
                recorder._now = lambda: wall_stop + timedelta(seconds=60)

            recorder._now = lambda: wall_stop
            with patch.object(recorder, "_sync_and_close_files", side_effect=delayed_close):
                sealed = recorder.stop()
            self.assertEqual(sealed.manifest["time"]["duration_seconds"], recorder._stopped_elapsed)
            self.assertEqual(datetime.fromisoformat(sealed.manifest["time"]["ended_at"]), wall_stop)
            self.assertEqual(
                datetime.fromisoformat(sealed.manifest["sealed_at"]),
                wall_stop + timedelta(seconds=60),
            )

    def test_real_audio_tail_extends_capture_but_later_file_flush_does_not(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder, audios = self.build(Path(directory), audio=True)
            wall_stop = recorder._now()
            recorder._now = lambda: wall_stop
            audio_stop = audios[0].stop
            audio_result = []

            def finishing_audio(*args, **kwargs):
                result = audio_stop(*args, **kwargs)
                audio_result.append(result)
                return result

            close = recorder._sync_and_close_files

            def delayed_close():
                close()
                time.sleep(0.03)
                recorder._now = lambda: wall_stop + timedelta(seconds=60)

            with (
                patch.object(audios[0], "stop", side_effect=finishing_audio),
                patch.object(recorder, "_sync_and_close_files", side_effect=delayed_close),
            ):
                sealed = recorder.stop()
            audio_end = (
                audio_result[0]["stopped_monotonic_ns"] - recorder._started_monotonic_ns
            ) / 1e9
            expected = max(recorder._stopped_elapsed, audio_end)
            self.assertEqual(sealed.manifest["time"]["duration_seconds"], expected)
            self.assertEqual(sealed.manifest["audio"]["sync"]["end_time_seconds"], audio_end)
            self.assertEqual(
                datetime.fromisoformat(sealed.manifest["time"]["ended_at"]),
                wall_stop + timedelta(seconds=expected - recorder._stopped_elapsed),
            )

    def test_future_audio_time_is_rejected_instead_of_inflating_duration(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder, audios = self.build(Path(directory), audio=True)
            audio_stop = audios[0].stop

            def future_audio(*args, **kwargs):
                result = audio_stop(*args, **kwargs)
                result["stopped_monotonic_ns"] += 60_000_000_000
                return result

            with (
                patch.object(audios[0], "stop", side_effect=future_audio),
                self.assertRaises(DeviceRecordingError) as raised,
            ):
                recorder.stop()
            self.assertEqual(raised.exception.code, "audio_invalid")
            self.assertFalse(recorder._final.exists())

    def test_accepted_frames_drained_after_stop_keep_their_enqueue_boundaries(self):
        with tempfile.TemporaryDirectory() as directory:
            entered, release = threading.Event(), threading.Event()

            def hold_first_frame(role, payload):
                if role == "frames.index" and not entered.is_set():
                    entered.set()
                    if not release.wait(5):
                        raise RuntimeError("test writer gate timed out")

            recorder, _, _ = fixture.SplitEyeRecordingTest().build(
                Path(directory), before_write=hold_first_frame
            )
            recorder.start()
            try:
                fixture.SplitEyeRecordingTest.feed(recorder, 4)
                self.assertTrue(entered.wait(2))
                persist = recorder._persist_state

                def delayed_finalizing(state, **kwargs):
                    if state == "finalizing":
                        time.sleep(0.03)
                        release.set()
                    return persist(state, **kwargs)

                with patch.object(recorder, "_persist_state", side_effect=delayed_finalizing):
                    sealed = recorder.stop()
                self.assertEqual(sealed.manifest["frames"]["count"], 4)
                self.assertEqual(
                    sealed.manifest["time"]["duration_seconds"], recorder._stopped_elapsed
                )
                self.assertLess(recorder._boundary_elapsed[3], recorder._stopped_elapsed)
                self.assertFalse(recorder._partial.exists())
            finally:
                release.set()
                if recorder.state in {"recording", "finalizing"}:
                    recorder.abort()


if __name__ == "__main__":
    unittest.main()
