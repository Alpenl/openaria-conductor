"""边录边出左右眼 H.264 的录制器行为（issue #46）。"""

from __future__ import annotations

import hashlib
import json
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from rp_ylx.api.downloads import (
    ArtifactAccessError,
    iter_device_session_v1_artifacts,
    validate_device_session_manifest,
)
from rp_ylx.camera import FrameObservation, StereoFrame
from rp_ylx.imu import ImuObservation, ImuSample, RawVector3
from rp_ylx.recording import (
    DeviceSessionConfig,
    DeviceSessionRecorder,
    SessionPlan,
    StorageStatus,
    uuid7,
)
from rp_ylx.recording.device_session import DeviceRecordingError
from rp_ylx.recording.stereo_encoder import ClosedSegment, StereoEncoderError

FRAME = b"\xff\xd8split-eye\xff\xd9"


def frame(sequence: int) -> FrameObservation:
    return FrameObservation(
        StereoFrame(
            source_sequence=sequence,
            host_monotonic_ns=sequence * 33_000_000,
            left=b"",
            right=b"",
            raw_side_by_side=FRAME,
        ),
        dropped_before=0,
    )


def imu_observation() -> ImuObservation:
    sample = ImuSample(
        sequence=0,
        packet_sequence=0,
        sample_index=0,
        device_timestamp_raw=10_000,
        device_ticks=10_000,
        host_read_start_ns=2_000_000,
        host_read_end_ns=2_000_010,
        host_monotonic_ns=2_000_005,
        accelerometer=RawVector3(1, 2, 3),
        gyroscope=RawVector3(4, 5, 6),
        sync_offset_ns=None,
        sync_residual_ns=None,
        sync_quality="insufficient",
    )
    return ImuObservation((sample, sample), dropped_samples=0)


class FakeStereoEncoder:
    """替身助手：写出可校验的分段文件，不触碰真实 JPU/VPU。"""

    def __init__(
        self,
        out_dir: Path,
        *,
        segment_frames: int,
        fail_after: int | None = None,
        drop_tail_segment: bool = False,
    ) -> None:
        self._out_dir = out_dir
        self._segment_frames = segment_frames
        self._fail_after = fail_after
        self._drop_tail_segment = drop_tail_segment
        self._segments: list[ClosedSegment] = []
        self._submitted = 0
        self._started = False
        self.aborted = False

    @property
    def segments(self) -> tuple[ClosedSegment, ...]:
        return tuple(self._segments)

    @property
    def submitted_frames(self) -> int:
        return self._submitted

    def start(self) -> None:
        self._out_dir.mkdir(parents=True, exist_ok=True)
        self._started = True

    def submit(self, jpeg: bytes) -> None:
        if not self._started:
            raise StereoEncoderError("invalid_state", "助手未启动")
        if self._fail_after is not None and self._submitted >= self._fail_after:
            raise StereoEncoderError("encoder_failed", "助手拒绝该帧")
        self._submitted += 1
        if self._submitted % self._segment_frames == 0:
            self._close(self._submitted - self._segment_frames, self._submitted)

    def finish(self, *, timeout: float = 30.0) -> tuple[ClosedSegment, ...]:
        del timeout
        closed = len(self._segments) * self._segment_frames
        if closed < self._submitted and not self._drop_tail_segment:
            self._close(closed, self._submitted)
        return self.segments

    # 与助手一致：帧数正好落在分段边界时不产生空尾段。

    def abort(self) -> None:
        self.aborted = True

    def _close(self, start_frame: int, end_frame: int) -> None:
        index = len(self._segments)
        payloads: dict[str, tuple[str, int]] = {}
        for eye in ("left", "right"):
            name = f"{eye}_{index:05d}.mp4"
            body = f"{eye}-{index}-{start_frame}-{end_frame}".encode() * 8
            (self._out_dir / name).write_bytes(body)
            payloads[eye] = (f"video/{name}", len(body))
        self._segments.append(
            ClosedSegment(
                index=index,
                start_frame=start_frame,
                end_frame=end_frame,
                left_path=payloads["left"][0],
                left_bytes=payloads["left"][1],
                right_path=payloads["right"][0],
                right_bytes=payloads["right"][1],
            )
        )


class FakeAudioRecorder:
    def __init__(self, session_root: Path, *, fail_stop: bool = False) -> None:
        self._session_root = session_root
        self._fail_stop = fail_stop
        self.started = False
        self.aborted = False
        self.started_monotonic_ns = 0

    def start(self) -> None:
        self.started = True
        self.started_monotonic_ns = time.monotonic_ns()

    def stop(self, timeout_seconds: float = 5.0) -> dict[str, object]:
        del timeout_seconds
        if self._fail_stop:
            raise RuntimeError("audio_failed: fake audio stop failure")
        path = self._session_root / "audio/audio_00000.wav"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"RIFF" + b"\x00" * 40 + b"pcm")
        return {
            "device": "hw:0,0",
            "codec": "pcm_s16le",
            "container": "wav",
            "sample_rate_hz": 48_000,
            "channels": 2,
            "sample_format": "S16_LE",
            "sample_count": 4_800,
            "started_monotonic_ns": self.started_monotonic_ns,
            "stopped_monotonic_ns": self.started_monotonic_ns + 100_000_000,
            "segments": [
                {
                    "index": 0,
                    "path": "audio/audio_00000.wav",
                    "start_sample": 0,
                    "end_sample": 4_800,
                    "start_time_seconds": 0.0,
                    "end_time_seconds": 0.1,
                }
            ],
        }

    def abort(self) -> None:
        self.aborted = True

    def close(self) -> None:
        self.aborted = True


class SplitEyeRecordingTest(unittest.TestCase):
    segment_seconds = 0.1  # 30fps * 0.1s = 3 frames per segment

    def build(
        self,
        root: Path,
        *,
        queue_capacity: int = 128,
        enqueue_timeout: float = 0.05,
        before_write: object | None = None,
        audio_enabled: bool = False,
        audio_fail_stop: bool = False,
        **encoder_options: object,
    ) -> tuple[DeviceSessionRecorder, list[FakeStereoEncoder], list[FakeAudioRecorder]]:
        revision = 0
        encoders: list[FakeStereoEncoder] = []
        audios: list[FakeAudioRecorder] = []

        def allocate_revision() -> int:
            nonlocal revision
            revision += 1
            return revision

        config = DeviceSessionConfig(
            device_id=str(uuid.uuid4()),
            device_label="YLX-12AB34CD",
            hardware_fingerprint="sha256:" + "a" * 64,
            platform="D-Robotics RDK X5 V1.0 + YLX 2UQ2",
            software_version="0.5.0",
            commit="b" * 40,
            width=3840,
            height=1080,
            sensor_fps=60.0,
            frame_decimation=2,
            video_layout="split-eyes",
            segment_seconds=self.segment_seconds,
            audio_enabled=audio_enabled,
        )

        def factory(partial: Path) -> FakeStereoEncoder:
            encoder = FakeStereoEncoder(
                partial / "video",
                segment_frames=round(
                    config.segment_seconds * config.sensor_fps / config.frame_decimation
                ),
                **encoder_options,  # type: ignore[arg-type]
            )
            encoders.append(encoder)
            return encoder

        def audio_factory(partial: Path) -> FakeAudioRecorder:
            audio = FakeAudioRecorder(partial, fail_stop=audio_fail_stop)
            audios.append(audio)
            return audio

        recorder = DeviceSessionRecorder(
            root,
            config,
            SessionPlan(
                session_id=uuid7(),
                volume_id=str(uuid.uuid4()),
                generation_id=str(uuid.uuid4()),
                capture_mode="production",
                display_name="split-eye fixture",
                take_id=uuid7(),
                take_sequence=1,
                continuation_of=None,
            ),
            authority_epoch=str(uuid.uuid4()),
            allocate_revision=allocate_revision,
            storage_status=lambda: StorageStatus(1024 * 1024 * 1024, True),
            encoder_factory=factory,  # type: ignore[arg-type]
            checkpoint_interval=0.0,
            queue_capacity=queue_capacity,
            enqueue_timeout=enqueue_timeout,
            before_write=before_write,  # type: ignore[arg-type]
            audio_recorder_factory=audio_factory,  # type: ignore[arg-type]
        )
        return recorder, encoders, audios

    @staticmethod
    def feed(recorder: DeviceSessionRecorder, count: int) -> None:
        for index in range(count):
            recorder.submit_frame(
                FrameObservation(
                    StereoFrame(
                        source_sequence=index,
                        host_monotonic_ns=index * 33_000_000,
                        left=b"",
                        right=b"",
                        raw_side_by_side=FRAME,
                    ),
                    dropped_before=0,
                )
            )

    def test_sealed_session_declares_contiguous_split_eye_segments(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root)
            recorder.start()
            self.feed(recorder, 7)
            sealed = recorder.stop()

            manifest = sealed.manifest
            video = manifest["video"]
            self.assertEqual(video["layout"], "split-eyes")
            self.assertEqual(video["codec"], "h264")
            self.assertEqual(video["container"], "mp4")
            segments = video["segments"]
            self.assertEqual([segment["index"] for segment in segments], [0, 1, 2])
            self.assertEqual(segments[0]["start_frame"], 0)
            self.assertEqual(segments[-1]["end_frame"], 7)
            for previous, following in zip(segments, segments[1:], strict=False):
                self.assertEqual(previous["end_frame"], following["start_frame"])
                self.assertEqual(previous["end_time_seconds"], following["start_time_seconds"])
            self.assertNotIn(
                "raw-sbs.mjpeg", {item.name for item in (sealed.path / "video").iterdir()}
            )
            validate_device_session_manifest(manifest)

    def test_take_ending_on_a_segment_boundary_seals_without_an_empty_tail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root)
            recorder.start()
            self.feed(recorder, 6)  # 恰好两段，末尾不留空段
            sealed = recorder.stop()

            segments = sealed.manifest["video"]["segments"]
            self.assertEqual([segment["index"] for segment in segments], [0, 1])
            self.assertEqual(segments[-1]["end_frame"], 6)
            self.assertEqual(
                sorted(item.name for item in (sealed.path / "video").iterdir()),
                ["left_00000.mp4", "left_00001.mp4", "right_00000.mp4", "right_00001.mp4"],
            )
            validate_device_session_manifest(sealed.manifest)

    def test_segment_artifacts_carry_verified_digests(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root)
            recorder.start()
            self.feed(recorder, 6)
            sealed = recorder.stop()

            for segment in sealed.manifest["video"]["segments"]:
                for eye in ("left", "right"):
                    artifact = segment["artifacts"][eye]
                    self.assertEqual(artifact["role"], f"video.{eye}")
                    self.assertEqual(artifact["media_type"], "video/mp4")
                    body = (sealed.path / artifact["path"]).read_bytes()
                    self.assertEqual(artifact["bytes"], len(body))
                    self.assertEqual(artifact["sha256"], hashlib.sha256(body).hexdigest())
                    self.assertEqual(artifact["artifact_id"], artifact["sha256"])

    def test_frame_index_records_segment_placement(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root)
            recorder.start()
            self.feed(recorder, 4)
            sealed = recorder.stop()

            records = [
                json.loads(line)
                for line in (sealed.path / "frames.ndjson").read_text().splitlines()
            ]
            self.assertEqual([record["segment_index"] for record in records], [0, 0, 0, 1])
            self.assertEqual([record["segment_frame"] for record in records], [0, 1, 2, 0])
            self.assertNotIn("video_offset", records[0])

    def test_native_recording_codec_is_used_for_hot_path_records(self) -> None:
        class FakeRecordingCodec:
            def __init__(self) -> None:
                self.jpeg_payload_calls = 0
                self.frame_index_calls = 0
                self.imu_sample_calls = 0

            def jpeg_payload(self, payload: bytes) -> bytes:
                self.jpeg_payload_calls += 1
                return payload

            def encode_split_frame_index(
                self,
                session_id: str,
                frame: int,
                source_sequence: int,
                host_monotonic_ns: int,
                segment_index: int,
                segment_frame: int,
            ) -> bytes:
                self.frame_index_calls += 1
                return (
                    json.dumps(
                        {
                            "schema": "ylx.frame-index.v1",
                            "session_id": session_id,
                            "frame": frame,
                            "source_sequence": source_sequence,
                            "host_monotonic_ns": host_monotonic_ns,
                            "segment_index": segment_index,
                            "segment_frame": segment_frame,
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                    + "\n"
                ).encode()

            def encode_imu_sample(
                self,
                session_id: str,
                sequence: int,
                packet_sequence: int,
                sample_index: int,
                device_timestamp_raw: int,
                device_ticks: int,
                host_read_start_ns: int,
                host_read_end_ns: int,
                host_monotonic_ns: int,
                accelerometer: tuple[int, int, int],
                gyroscope: tuple[int, int, int],
                sync_offset_ns: int | None,
                sync_residual_ns: int | None,
                sync_quality: str,
            ) -> bytes:
                self.imu_sample_calls += 1
                return (
                    json.dumps(
                        {
                            "format": "ylx.imu.v0",
                            "session_id": session_id,
                            "sequence": sequence,
                            "packet_sequence": packet_sequence,
                            "sample_index": sample_index,
                            "device_timestamp_raw": device_timestamp_raw,
                            "device_ticks": device_ticks,
                            "host_read_start_ns": host_read_start_ns,
                            "host_read_end_ns": host_read_end_ns,
                            "host_monotonic_ns": host_monotonic_ns,
                            "raw": {
                                "accelerometer": list(accelerometer),
                                "gyroscope": list(gyroscope),
                            },
                            "sync": {
                                "offset_ns": sync_offset_ns,
                                "residual_ns": sync_residual_ns,
                                "quality": sync_quality,
                            },
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                    + "\n"
                ).encode()

        codec = FakeRecordingCodec()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch(
                "rp_ylx.recording.device_session.create_native_recording_codec",
                return_value=codec,
            ):
                recorder, _, _ = self.build(root)
            recorder.start()
            self.feed(recorder, 1)
            recorder.submit_imu(imu_observation())
            sealed = recorder.stop()

            self.assertEqual(codec.jpeg_payload_calls, 1)
            self.assertEqual(codec.frame_index_calls, 1)
            self.assertEqual(codec.imu_sample_calls, 2)
            validate_device_session_manifest(sealed.manifest)

    def test_open_handles_cover_the_encoder_until_the_take_ends(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root)
            recorder.start()
            self.feed(recorder, 4)
            # 助手在卷上持有分段文件，安全换盘不能在它退出前放行。
            self.assertGreater(recorder.open_handle_count, 0)
            recorder.stop()
            self.assertEqual(recorder.open_handle_count, 0)

    def test_encoder_failure_fails_the_take_instead_of_sealing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, encoders, _ = self.build(root, fail_after=3)
            recorder.start()
            self.feed(recorder, 3)
            with self.assertRaises(DeviceRecordingError):
                self.feed(recorder, 1)
                recorder.stop()
            self.assertNotEqual(recorder.state, "sealed")
            self.assertFalse((root / recorder.partial_path.name.removesuffix(".partial")).exists())
            self.assertTrue(encoders[0].aborted)

    def test_missing_tail_segment_blocks_sealing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root, drop_tail_segment=True)
            recorder.start()
            self.feed(recorder, 5)
            with self.assertRaises(DeviceRecordingError) as raised:
                recorder.stop()
            self.assertEqual(raised.exception.code, "segment_invalid")
            self.assertNotEqual(recorder.state, "sealed")

    def test_downstream_rejects_manifest_with_a_frame_domain_hole(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root)
            recorder.start()
            self.feed(recorder, 6)
            manifest = json.loads(json.dumps(recorder.stop().manifest))
            manifest["video"]["segments"][1]["start_frame"] += 1
            with self.assertRaises(ArtifactAccessError):
                validate_device_session_manifest(manifest)

    def test_split_eye_artifact_iterator_exposes_all_video_segments(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root)
            recorder.start()
            self.feed(recorder, 7)
            manifest = recorder.stop().manifest

            artifacts = list(iter_device_session_v1_artifacts(manifest))
            self.assertEqual(
                [artifact["path"] for artifact in artifacts],
                [
                    "video/left_00000.mp4",
                    "video/right_00000.mp4",
                    "video/left_00001.mp4",
                    "video/right_00001.mp4",
                    "video/left_00002.mp4",
                    "video/right_00002.mp4",
                    "frames.ndjson",
                    "imu.ndjson",
                ],
            )

    def test_audio_segments_are_declared_and_downloadable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, audios = self.build(root, audio_enabled=True)
            recorder.start()
            self.feed(recorder, 3)
            sealed = recorder.stop()

            self.assertTrue(audios[0].started)
            audio = sealed.manifest["audio"]
            self.assertEqual(audio["codec"], "pcm_s16le")
            self.assertEqual(audio["container"], "wav")
            self.assertEqual(audio["sample_rate_hz"], 48_000)
            self.assertEqual(audio["sample_count"], 4_800)
            self.assertEqual(audio["sync"]["clock"], "host_monotonic")
            artifact = audio["segments"][0]["artifact"]
            self.assertEqual(artifact["role"], "audio.wav")
            self.assertEqual(artifact["media_type"], "audio/wav")
            self.assertEqual((sealed.path / artifact["path"]).read_bytes()[:4], b"RIFF")
            self.assertIn(
                "audio/audio_00000.wav",
                [item["path"] for item in iter_device_session_v1_artifacts(sealed.manifest)],
            )
            validate_device_session_manifest(sealed.manifest)

    def test_audio_failure_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, audios = self.build(root, audio_enabled=True, audio_fail_stop=True)
            recorder.start()
            self.feed(recorder, 3)
            with self.assertRaises(DeviceRecordingError) as raised:
                recorder.stop()
            self.assertEqual(raised.exception.code, "audio_failed")
            self.assertNotEqual(recorder.state, "sealed")
            self.assertTrue(audios[0].aborted)

    def test_imu_backpressure_fails_closed(self) -> None:
        block_writer = threading.Event()
        writer_blocked = threading.Event()

        def before_write(role: str, content: bytes) -> None:
            del content
            if role == "frames.index":
                writer_blocked.set()
                if not block_writer.wait(timeout=2):
                    raise TimeoutError("测试没有释放 writer")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(
                root,
                queue_capacity=1,
                enqueue_timeout=0.0,
                before_write=before_write,
            )
            recorder.start()
            recorder.submit_frame(frame(0))
            self.assertTrue(writer_blocked.wait(timeout=1))
            self.assertTrue(recorder.submit_frame(frame(1)))
            with self.assertRaises(DeviceRecordingError) as raised:
                recorder.submit_imu(imu_observation())
            self.assertEqual(raised.exception.code, "imu_backpressure")
            block_writer.set()
            with self.assertRaises(DeviceRecordingError) as stopped:
                recorder.stop()
            self.assertEqual(stopped.exception.code, "imu_backpressure")


if __name__ == "__main__":
    unittest.main()
