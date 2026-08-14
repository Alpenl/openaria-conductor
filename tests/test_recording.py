from __future__ import annotations

import errno
import json
import tempfile
import threading
import unittest
from pathlib import Path

from rp_ylx.camera import FrameObservation, StereoFrame
from rp_ylx.contracts import SessionValidationError, validate_session
from rp_ylx.contracts.frame_stream import MAGIC, iter_frames
from rp_ylx.imu import ImuObservation, ImuSample, RawVector3
from rp_ylx.recording import RecordingConfig, RecordingError, SessionRecorder

SESSION_ID = "0198c9a8-7a3c-7000-8000-000000000010"
CONFIG = RecordingConfig(
    device_id="camera-test",
    software_version="test",
    width=2,
    height=1,
    fps=30.0,
    encoding="test-bytes",
    device_tick_hz=1_000_000,
)


def frame(sequence: int, *, dropped_before: int = 0) -> FrameObservation:
    return FrameObservation(
        StereoFrame(
            source_sequence=sequence,
            host_monotonic_ns=1_000_000 + sequence,
            left=f"left-{sequence}".encode(),
            right=f"right-{sequence}".encode(),
        ),
        dropped_before=dropped_before,
    )


def imu_observation(*, dropped_samples: int = 0) -> ImuObservation:
    packet_sequence = 0
    device_ticks = 10_000
    host_time = 2_000_000

    def sample(sample_index: int) -> ImuSample:
        return ImuSample(
            sequence=sample_index,
            packet_sequence=packet_sequence,
            sample_index=sample_index,
            device_timestamp_raw=device_ticks,
            device_ticks=device_ticks,
            host_read_start_ns=host_time - 10,
            host_read_end_ns=host_time + 10,
            host_monotonic_ns=host_time,
            accelerometer=RawVector3(1 + sample_index, 2, 3),
            gyroscope=RawVector3(4, 5 + sample_index, 6),
            sync_offset_ns=None,
            sync_residual_ns=None,
            sync_quality="insufficient",
        )

    return ImuObservation((sample(0), sample(1)), dropped_samples=dropped_samples)


def read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


class SessionRecorderTest(unittest.TestCase):
    def test_normal_stop_seals_a_valid_idempotent_session(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder = SessionRecorder(root, CONFIG)
            partial = recorder.start(session_id=SESSION_ID)
            self.assertEqual(partial.name, f"{SESSION_ID}.partial")
            with self.assertRaises(SessionValidationError) as incomplete:
                validate_session(partial)
            self.assertEqual(incomplete.exception.code, "not_sealed")

            self.assertTrue(recorder.submit_frame(frame(0)))
            self.assertTrue(recorder.submit_imu(imu_observation()))
            final = recorder.stop()

            manifest = validate_session(final)
            self.assertEqual(
                manifest["counts"],
                {
                    "frames": 1,
                    "imu_samples": 2,
                    "diagnostics": 2,
                    "dropped_frames": 0,
                    "dropped_imu_samples": 0,
                },
            )
            self.assertEqual(
                {artifact["role"] for artifact in manifest["artifacts"]},
                {
                    "session.metadata",
                    "video.left",
                    "video.right",
                    "frames.timeline",
                    "imu.samples",
                    "diagnostics.events",
                },
            )
            with (final / "video/left.bin").open("rb") as stream:
                self.assertEqual(list(iter_frames(stream)), [b"left-0"])
            with (final / "video/right.bin").open("rb") as stream:
                self.assertEqual(list(iter_frames(stream)), [b"right-0"])

            manifest_before = (final / "manifest.json").read_bytes()
            modified_before = (final / "manifest.json").stat().st_mtime_ns
            self.assertEqual(recorder.stop(), final)
            self.assertEqual((final / "manifest.json").read_bytes(), manifest_before)
            self.assertEqual((final / "manifest.json").stat().st_mtime_ns, modified_before)

    def test_zero_frame_and_zero_imu_session_is_explicitly_valid(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            recorder = SessionRecorder(directory, CONFIG)
            recorder.start(session_id=SESSION_ID)
            final = recorder.stop()
            manifest = validate_session(final)

            self.assertEqual(manifest["counts"]["frames"], 0)
            self.assertEqual(manifest["counts"]["imu_samples"], 0)
            with (final / "video/left.bin").open("rb") as stream:
                self.assertEqual(list(iter_frames(stream)), [])

    def test_pre_publish_rejection_keeps_an_interrupted_partial(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder = SessionRecorder(root, CONFIG)
            recorder.start(session_id=SESSION_ID)

            def reject(candidate: Path) -> None:
                self.assertEqual(candidate, root / f"{SESSION_ID}.partial")
                validate_session(candidate, allow_partial=True)
                raise RecordingError("product_validation_failed", "后置产品校验失败")

            with self.assertRaises(RecordingError) as raised:
                recorder.stop(before_publish=reject)

            self.assertEqual(raised.exception.code, "product_validation_failed")
            partial = root / f"{SESSION_ID}.partial"
            self.assertFalse((root / SESSION_ID).exists())
            self.assertEqual(recorder.state, "interrupted")
            self.assertEqual(
                read_json(partial / "session.json")["failure"],
                {
                    "code": "product_validation_failed",
                    "message": "后置产品校验失败",
                },
            )
            self.assertFalse((partial / "manifest.json").exists())
            with self.assertRaises(SessionValidationError) as invalid:
                validate_session(partial)
            self.assertEqual(invalid.exception.code, "not_sealed")

    def test_context_exception_keeps_interrupted_partial(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder = SessionRecorder(root, CONFIG)
            with self.assertRaisesRegex(RuntimeError, "模拟进程异常"), recorder:
                partial = recorder.start(session_id=SESSION_ID)
                self.assertTrue(recorder.submit_frame(frame(0)))
                raise RuntimeError("模拟进程异常")

            state = read_json(partial / "session.json")
            self.assertEqual(state["state"], "interrupted")
            self.assertEqual(state["failure"]["code"], "process_interrupted")
            self.assertFalse((partial / "manifest.json").exists())
            self.assertFalse((root / SESSION_ID).exists())
            with self.assertRaises(SessionValidationError) as incomplete:
                validate_session(partial)
            self.assertEqual(incomplete.exception.code, "not_sealed")

    def test_disk_full_keeps_failed_partial_without_manifest(self) -> None:
        def fail_data_write(role: str, payload: bytes) -> None:
            if role == "video.left" and payload != MAGIC:
                raise OSError(errno.ENOSPC, "模拟磁盘写满")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder = SessionRecorder(root, CONFIG, before_write=fail_data_write)
            partial = recorder.start(session_id=SESSION_ID)
            self.assertTrue(recorder.submit_frame(frame(0)))

            with self.assertRaises(RecordingError) as failed:
                recorder.stop()
            self.assertEqual(failed.exception.code, "write_failed")
            self.assertEqual(recorder.state, "failed")
            self.assertEqual(read_json(partial / "session.json")["state"], "failed")
            self.assertFalse((partial / "manifest.json").exists())
            self.assertFalse((root / SESSION_ID).exists())

    def test_context_does_not_overwrite_a_recorded_write_failure(self) -> None:
        def fail_data_write(role: str, payload: bytes) -> None:
            if role == "video.left" and payload != MAGIC:
                raise OSError(errno.ENOSPC, "模拟磁盘写满")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder = SessionRecorder(root, CONFIG, before_write=fail_data_write)
            with self.assertRaises(RecordingError), recorder:
                partial = recorder.start(session_id=SESSION_ID)
                self.assertTrue(recorder.submit_frame(frame(0)))
                recorder.stop()

            state = read_json(partial / "session.json")
            self.assertEqual(state["state"], "failed")
            self.assertEqual(state["failure"]["code"], "write_failed")
            self.assertEqual(recorder.abort(), partial)
            self.assertEqual(read_json(partial / "session.json"), state)

    def test_slow_disk_applies_backpressure_and_records_all_known_loss(self) -> None:
        writer_blocked = threading.Event()
        release_writer = threading.Event()

        def slow_data_write(role: str, payload: bytes) -> None:
            if role == "video.left" and payload != MAGIC:
                writer_blocked.set()
                if not release_writer.wait(timeout=2):
                    raise TimeoutError("测试没有释放模拟慢盘")

        with tempfile.TemporaryDirectory() as directory:
            recorder = SessionRecorder(
                directory,
                CONFIG,
                queue_capacity=1,
                enqueue_timeout=0,
                before_write=slow_data_write,
            )
            recorder.start(session_id=SESSION_ID)
            try:
                self.assertTrue(recorder.submit_frame(frame(10, dropped_before=2)))
                self.assertTrue(writer_blocked.wait(timeout=1))
                self.assertTrue(recorder.submit_frame(frame(13)))
                self.assertFalse(recorder.submit_frame(frame(14)))
                self.assertFalse(recorder.submit_imu(imu_observation(dropped_samples=4)))
            finally:
                release_writer.set()

            final = recorder.stop()
            manifest = validate_session(final)
            self.assertEqual(manifest["counts"]["frames"], 2)
            self.assertEqual(manifest["counts"]["imu_samples"], 0)
            self.assertEqual(manifest["counts"]["dropped_frames"], 3)
            self.assertEqual(manifest["counts"]["dropped_imu_samples"], 6)
            diagnostics = [
                json.loads(line)
                for line in (final / "diagnostics.ndjson").read_text(encoding="utf-8").splitlines()
            ]
            counts = {record["code"]: record["count"] for record in diagnostics}
            self.assertEqual(counts["frame_dropped"], 3)
            self.assertEqual(counts["imu_dropped"], 6)


if __name__ == "__main__":
    unittest.main()
