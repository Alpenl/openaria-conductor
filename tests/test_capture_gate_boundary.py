"""Source draining cannot move the native capture gate's recorded boundary."""

import tempfile
import unittest
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from rp_ylx.recording.device_session import DeviceRecordingError
from tests import test_split_eye_recording as fixture


class CaptureGateBoundaryTest(unittest.TestCase):
    @contextmanager
    def recording(self, *, audio=False):
        with tempfile.TemporaryDirectory() as directory:
            store = fixture.FakeSessionStore()
            with (
                patch("rp_ylx.recording.device_session.native_session_store", return_value=store),
                patch(
                    "rp_ylx.recording.device_session.resolve_executable",
                    return_value=Path("/bin/true"),
                ),
            ):
                recorder, _, _ = fixture.SplitEyeRecordingTest().build(
                    Path(directory), native_data_plane=True, audio_enabled=audio
                )
                recorder.start()
                transaction = store.transaction
                transaction.advance(frames=4, imu_samples=2)
                try:
                    yield recorder, transaction
                finally:
                    if recorder.state in {"recording", "finalizing"}:
                        recorder.abort()

    def test_source_drain_excluded_from_duration_wall_end_and_video_boundaries(self):
        with self.recording() as (recorder, transaction):
            start = recorder._started_monotonic_ns
            transaction.capture_stopped_monotonic_ns = start + 10_000_000_000
            wall = datetime(2026, 10, 21, 5, 0, 0, tzinfo=UTC)
            with (
                patch(
                    "rp_ylx.recording.device_session.time.monotonic_ns",
                    return_value=start + 30_000_000_000,
                ),
                patch.object(recorder, "_now", return_value=wall),
            ):
                sealed = recorder.stop()
            self.assertEqual(sealed.manifest["time"]["duration_seconds"], 10.0)
            self.assertEqual(sealed.manifest["frames"]["count"], 4)
            self.assertEqual(
                datetime.fromisoformat(sealed.manifest["time"]["ended_at"]),
                wall - timedelta(seconds=20),
            )
            self.assertEqual(datetime.fromisoformat(sealed.manifest["sealed_at"]), wall)
            self.assertTrue(transaction.boundary_calls)
            self.assertEqual({duration for _, duration in transaction.boundary_calls}, {10.0})
            self.assertEqual(recorder.stop().manifest_sha256, sealed.manifest_sha256)

    def test_without_native_runtime_gate_direct_transaction_stop_uses_observed_time(self):
        with self.recording() as (recorder, transaction):
            self.assertIsNone(transaction.capture_stopped_monotonic_ns)
            with patch(
                "rp_ylx.recording.device_session.time.monotonic_ns",
                return_value=recorder._started_monotonic_ns + 30_000_000_000,
            ):
                sealed = recorder.stop()
            self.assertEqual(sealed.manifest["time"]["duration_seconds"], 30.0)
            self.assertTrue(transaction.finished)

    def test_invalid_gate_timestamp_aborts_instead_of_sealing_false_duration(self):
        for kind in ("bool", "float", "negative", "before_start", "future", "clock_failure"):
            with self.subTest(kind=kind), self.recording() as (recorder, transaction):
                start = recorder._started_monotonic_ns
                transaction.capture_stopped_monotonic_ns = {
                    "bool": True,
                    "float": float(start),
                    "negative": -1,
                    "before_start": start - 1,
                    "future": start + 31_000_000_000,
                    "clock_failure": 2**64 - 1,
                }[kind]
                with (
                    patch(
                        "rp_ylx.recording.device_session.time.monotonic_ns",
                        return_value=start + 30_000_000_000,
                    ),
                    self.assertRaises(DeviceRecordingError) as raised,
                ):
                    recorder.stop()
                self.assertEqual(raised.exception.code, "native_session_store_failed")
                self.assertTrue(transaction.aborted)
                self.assertFalse(transaction.finished)
                self.assertFalse(recorder._final.exists())

    def test_real_audio_tail_extends_early_gate_without_including_later_drain(self):
        with self.recording(audio=True) as (recorder, transaction):
            audio = fixture.FakeAudioRecorder(recorder._partial)
            audio.start()
            transaction.capture_stopped_monotonic_ns = audio.started_monotonic_ns
            audio_result = audio.stop()
            finish = transaction.finish

            def finish_with_audio(duration, timeout):
                result = finish(duration, timeout)
                result["audio"] = audio_result
                return result

            start = recorder._started_monotonic_ns
            with (
                patch.object(transaction, "finish", side_effect=finish_with_audio),
                patch(
                    "rp_ylx.recording.device_session.time.monotonic_ns",
                    return_value=audio_result["stopped_monotonic_ns"] + 20_000_000_000,
                ),
            ):
                sealed = recorder.stop()
            expected = max(
                (audio_result["stopped_monotonic_ns"] - start) / 1e9,
                sealed.manifest["audio"]["sync"]["end_time_seconds"],
            )
            self.assertEqual(sealed.manifest["time"]["duration_seconds"], expected)
            self.assertGreater(expected, recorder._stopped_elapsed)
            self.assertEqual(sealed.manifest["audio"]["sample_count"], 4800)
