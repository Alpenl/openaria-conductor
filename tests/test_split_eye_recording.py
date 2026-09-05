"""边录边出左右眼 H.264 的录制器行为（issue #46）。"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
import time
import unittest
import uuid
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from rp_ylx.api.downloads import (
    ArtifactAccessError,
    iter_device_session_v1_artifacts,
    validate_device_session_manifest,
)
from rp_ylx.camera import FrameObservation, StereoFrame
from rp_ylx.imu import ImuObservation, ImuSample, RawVector3
from rp_ylx.native import NativeRecordingPlan
from rp_ylx.recording import (
    DeviceSessionConfig,
    DeviceSessionRecorder,
    SessionPlan,
    StorageStatus,
    uuid7,
)
from rp_ylx.recording.device_session import (
    DeviceRecordingError,
    _finalize_artifact,
    inspect_device_session_directory,
    manifest_artifact_bytes_total,
    validate_device_session_directory,
)
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
        self.live_sample_count = 0
        self.live_bytes_written = 0

    def start(self) -> None:
        self.started = True
        self.started_monotonic_ns = time.monotonic_ns()

    def advance_live(self, *, sample_count: int, bytes_written: int) -> None:
        self.live_sample_count = sample_count
        self.live_bytes_written = bytes_written

    def snapshot(self) -> dict[str, object]:
        return {
            "sample_count": self.live_sample_count,
            "bytes_written": self.live_bytes_written,
        }

    def stop(self, timeout_seconds: float = 5.0) -> dict[str, object]:
        del timeout_seconds
        if self._fail_stop:
            raise RuntimeError("audio_failed: fake audio stop failure")
        target_stop = self.started_monotonic_ns + 100_000_000
        while time.monotonic_ns() < target_stop:
            time.sleep(0.001)
        stopped_monotonic_ns = time.monotonic_ns()
        path = self._session_root / "audio/audio_00000.wav"
        path.parent.mkdir(parents=True, exist_ok=True)
        sample_count = 4_800
        channels = 2
        bits_per_sample = 16
        pcm_payload = b"\x00" * (sample_count * channels * (bits_per_sample // 8))
        header = bytearray(44)
        header[0:4] = b"RIFF"
        header[4:8] = (36 + len(pcm_payload)).to_bytes(4, "little")
        header[8:12] = b"WAVE"
        header[12:16] = b"fmt "
        header[16:20] = (16).to_bytes(4, "little")
        header[20:22] = (1).to_bytes(2, "little")
        header[22:24] = channels.to_bytes(2, "little")
        header[24:28] = (48_000).to_bytes(4, "little")
        header[28:32] = (48_000 * channels * (bits_per_sample // 8)).to_bytes(4, "little")
        header[32:34] = (channels * (bits_per_sample // 8)).to_bytes(2, "little")
        header[34:36] = bits_per_sample.to_bytes(2, "little")
        header[36:40] = b"data"
        header[40:44] = len(pcm_payload).to_bytes(4, "little")
        path.write_bytes(bytes(header) + pcm_payload)
        return {
            "device": "hw:0,0",
            "codec": "pcm_s16le",
            "container": "wav",
            "sample_rate_hz": 48_000,
            "channels": 2,
            "sample_format": "S16_LE",
            "sample_count": sample_count,
            "started_monotonic_ns": self.started_monotonic_ns,
            "stopped_monotonic_ns": stopped_monotonic_ns,
            "segments": [
                {
                    "index": 0,
                    "path": "audio/audio_00000.wav",
                    "start_sample": 0,
                    "end_sample": sample_count,
                    "start_time_seconds": 0.0,
                    "end_time_seconds": 0.1,
                }
            ],
        }

    def abort(self) -> None:
        self.aborted = True

    def close(self) -> None:
        self.aborted = True


class FakeSessionTransaction:
    def __init__(self, plan: NativeRecordingPlan, *, fail_finish: bool = False) -> None:
        self.plan = plan
        self.root = Path(plan.session_root)
        self.session_id = plan.session_id
        self.recording_start_monotonic_ns = plan.recording_start_monotonic_ns
        self.segment_frames = plan.segment_frames
        self.frames_written = 0
        self.imu_samples_written = 0
        self._segments: list[dict[str, object]] = []
        self.boundary_calls: list[tuple[int, float]] = []
        self.seal_calls: list[tuple[str, str, str, list[str]]] = []
        self.fail_finish = fail_finish
        self.finished = False
        self.aborted = False
        self.sealed = False

    @staticmethod
    def _identity(path: Path) -> dict[str, int]:
        metadata = path.stat(follow_symlinks=False)
        return {
            "device": metadata.st_dev,
            "inode": metadata.st_ino,
            "size": metadata.st_size,
            "mtime_ns": metadata.st_mtime_ns,
        }

    def advance(self, *, frames: int, imu_samples: int = 0) -> None:
        if frames <= 0 or frames < self.frames_written:
            raise ValueError("fake transaction frames must increase")
        self.frames_written = frames
        self.imu_samples_written = imu_samples
        (self.root / "frames.ndjson").write_bytes(b'{"frame":0}\n' * frames)
        (self.root / "imu.ndjson").write_bytes(b'{"sample":0}\n' * imu_samples)
        self._segments = []
        for start in range(0, frames, self.segment_frames):
            end = min(frames, start + self.segment_frames)
            index = len(self._segments)
            paths: dict[str, tuple[str, int]] = {}
            for eye in ("left", "right"):
                relative = f"video/{eye}_{index:05d}.mp4"
                payload = f"{eye}-{index}-{start}-{end}".encode() * 8
                (self.root / relative).write_bytes(payload)
                paths[eye] = (relative, len(payload))
            self._segments.append(
                {
                    "index": index,
                    "start_frame": start,
                    "end_frame": end,
                    "left_path": paths["left"][0],
                    "left_bytes": paths["left"][1],
                    "right_path": paths["right"][0],
                    "right_bytes": paths["right"][1],
                }
            )

    def _active_take(self) -> dict[str, object]:
        return {
            "session_id": self.session_id,
            "frame_domain": self.frames_written,
            "frames_written": self.frames_written,
            "pending_frames": 0,
            "drop_events": [],
        }

    def _sink(self, *, artifacts: bool) -> dict[str, object]:
        selected: dict[str, object] = {}
        for role, relative in (
            ("frames.index", "frames.ndjson"),
            ("imu.samples", "imu.ndjson"),
        ):
            path = self.root / relative
            payload = path.read_bytes()
            identity = self._identity(path)
            selected[role] = {
                "role": role,
                "path": relative,
                "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
                "identity": identity,
            }
        return {
            "frames_written": self.frames_written,
            "imu_samples_written": self.imu_samples_written,
            "bytes_written": sum(
                int(item["bytes"]) for item in selected.values() if isinstance(item, dict)
            ),
            "artifacts": selected if artifacts else {},
        }

    def snapshot(self) -> dict[str, object]:
        return {
            "state": "finished" if self.finished else "recording",
            "active_take": self._active_take(),
            "sink": self._sink(artifacts=False),
            "segments": list(self._segments),
            "submitted_frames": self.frames_written,
            "audio": None,
        }

    def segments(self) -> list[dict[str, object]]:
        return list(self._segments)

    def finish(self, duration_seconds: float, timeout_seconds: float) -> dict[str, object]:
        del timeout_seconds
        if self.fail_finish:
            raise RuntimeError("encoder_failed: fake transaction finish failure")
        if self.frames_written == 0:
            raise RuntimeError("no_frames: fake transaction has no frames")
        self.finished = True
        return {
            "active_take": self._active_take(),
            "sink": self._sink(artifacts=True),
            "audio": None,
            "segments": list(self._segments),
            "encoder_stats": {"frames": self.frames_written},
            "planner": {
                "segment_frames": self.segment_frames,
                "frames_written": self.frames_written,
                "segment_count": len(self._segments),
                "covered_frames": self.frames_written,
                "boundary_count": len(self._segments) + 1,
                "duration_seconds": duration_seconds,
            },
        }

    def boundary(self, ordinal: int, duration_seconds: float) -> dict[str, object]:
        self.boundary_calls.append((ordinal, duration_seconds))
        return {
            "frame": ordinal,
            "time_seconds": duration_seconds * ordinal / self.frames_written,
        }

    def abort(self, reason: str) -> None:
        del reason
        self.aborted = True

    def open_handle_count(self) -> int:
        return 0 if self.finished or self.aborted else 1

    def seal(
        self,
        partial_path: str,
        final_path: str,
        session_id: str,
        manifest: bytes,
        expected_identities: dict[str, tuple[int, int, int, int]],
        control_names: list[str] | None = None,
    ) -> dict[str, object]:
        if not self.finished:
            raise RuntimeError("invalid_state: transaction must finish before seal")
        artifacts = list(iter_device_session_v1_artifacts(json.loads(manifest)))
        paths = [str(item["path"]) for item in artifacts]
        self.seal_calls.append((partial_path, final_path, session_id, paths))
        partial = Path(partial_path)
        final = Path(final_path)
        for artifact in artifacts:
            relative = str(artifact["path"])
            metadata = (partial / relative).stat(follow_symlinks=False)
            actual = (
                metadata.st_dev,
                metadata.st_ino,
                metadata.st_size,
                metadata.st_mtime_ns,
            )
            if actual != expected_identities[relative]:
                raise RuntimeError("digest_mismatch: artifact changed")
        (partial / "manifest.json").write_bytes(manifest)
        for control_name in control_names or ["recording.json", "capture.json"]:
            (partial / control_name).unlink(missing_ok=True)
        os.rename(partial, final)
        self.sealed = True
        return {
            "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
            "artifact_count": len(artifacts),
            "manifest_bytes": len(manifest),
        }


class FakeSessionStore:
    def __init__(self, *, fail_finish: bool = False) -> None:
        self.fail_finish = fail_finish
        self.begin_calls: list[NativeRecordingPlan] = []
        self.transaction: FakeSessionTransaction | None = None

    def begin_recording(self, plan: NativeRecordingPlan) -> FakeSessionTransaction:
        self.begin_calls.append(plan)
        self.transaction = FakeSessionTransaction(plan, fail_finish=self.fail_finish)
        return self.transaction


class FakeNativeSessionStoreIo:
    def __init__(self) -> None:
        self.open_calls: list[tuple[int, str]] = []
        self.read_calls: list[tuple[int, int]] = []
        self.verify_calls: list[tuple[int, int, str]] = []

    def open_relative_regular(self, root_descriptor: int, relative_path: str) -> int:
        self.open_calls.append((root_descriptor, relative_path))
        return os.open(
            relative_path,
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=root_descriptor,
        )

    def read_bounded_fd(self, descriptor: int, maximum_bytes: int) -> bytes:
        self.read_calls.append((descriptor, maximum_bytes))
        return os.pread(descriptor, maximum_bytes + 1, 0)

    def verify_fd(
        self,
        descriptor: int,
        expected_bytes: int,
        expected_sha256: str,
    ) -> dict[str, object]:
        self.verify_calls.append((descriptor, expected_bytes, expected_sha256))
        return {}


class SplitEyeRecordingTest(unittest.TestCase):
    segment_seconds = 0.1  # 30fps * 0.1s = 3 frames per segment

    def test_finalize_artifact_uses_native_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "artifact.bin"
            path.write_bytes(b"abc")
            native = SimpleNamespace(
                finalize_artifact=Mock(
                    return_value={
                        "sha256": "a" * 64,
                        "identity": {
                            "device": 1,
                            "inode": 2,
                            "size": 3,
                            "modified_ns": 4,
                            "nlink": 1,
                        },
                    }
                )
            )

            with patch(
                "rp_ylx.recording.device_session._session_store_or_none",
                return_value=native,
            ):
                finalized = _finalize_artifact(path, 3, code="segment_invalid")

            native.finalize_artifact.assert_called_once_with(str(path), 3)
            self.assertEqual(finalized.sha256, "a" * 64)
            self.assertEqual(finalized.bytes, 3)
            self.assertEqual(finalized.identity, (1, 2, 3, 4))

    def test_finalize_artifact_maps_native_errors_to_context_code(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "artifact.bin"
            path.write_bytes(b"abc")
            native = SimpleNamespace(
                finalize_artifact=Mock(side_effect=RuntimeError("artifact_invalid: size changed"))
            )

            with (
                patch(
                    "rp_ylx.recording.device_session._session_store_or_none",
                    return_value=native,
                ),
                self.assertRaises(DeviceRecordingError) as raised,
            ):
                _finalize_artifact(path, 3, code="segment_invalid")

            self.assertEqual(raised.exception.code, "segment_invalid")
            self.assertEqual(raised.exception.message, "size changed")

    def build(
        self,
        root: Path,
        *,
        queue_capacity: int = 128,
        enqueue_timeout: float = 0.05,
        before_write: object | None = None,
        audio_enabled: bool = False,
        audio_fail_stop: bool = False,
        native_data_plane: bool = False,
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
            encoder_factory=None if native_data_plane else factory,  # type: ignore[arg-type]
            checkpoint_interval=0.0,
            queue_capacity=queue_capacity,
            enqueue_timeout=enqueue_timeout,
            before_write=None if native_data_plane else before_write,  # type: ignore[arg-type]
            audio_recorder_factory=(None if native_data_plane else audio_factory),
            native_data_plane=native_data_plane,
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

    def test_session_transaction_owns_live_progress_finish_and_seal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = FakeSessionStore()
            with (
                patch(
                    "rp_ylx.recording.device_session.native_session_store",
                    return_value=store,
                ),
                patch(
                    "rp_ylx.recording.device_session.resolve_executable",
                    return_value=Path("/bin/true"),
                ),
            ):
                recorder, _, _ = self.build(root, native_data_plane=True)
                recorder.start()
                transaction = recorder.native_recording_transaction()
                self.assertIs(transaction, store.transaction)
                assert store.transaction is not None
                initial = recorder.current_recording_state
                assert initial is not None
                store.transaction.advance(frames=4, imu_samples=2)
                live = recorder.current_recording_state
                assert live is not None
                self.assertEqual(live["state_revision"], initial["state_revision"])
                self.assertEqual(live["progress"]["captured_frames"], 4)
                self.assertGreater(live["progress"]["bytes_written"], 0)
                sealed = recorder.stop()

            self.assertEqual(len(store.begin_calls), 1)
            self.assertTrue(store.transaction.finished)
            self.assertTrue(store.transaction.sealed)
            self.assertFalse(store.transaction.aborted)
            self.assertEqual(store.transaction.open_handle_count(), 0)
            self.assertEqual(len(store.transaction.seal_calls), 1)
            partial_path, final_path, session_id, paths = store.transaction.seal_calls[0]
            self.assertEqual(Path(final_path), sealed.path)
            self.assertFalse(Path(partial_path).exists())
            self.assertTrue(Path(final_path).is_dir())
            self.assertEqual(session_id, sealed.manifest["session_id"])
            self.assertEqual(
                paths,
                [
                    "video/left_00000.mp4",
                    "video/right_00000.mp4",
                    "video/left_00001.mp4",
                    "video/right_00001.mp4",
                    "frames.ndjson",
                    "imu.ndjson",
                ],
            )
            self.assertTrue((sealed.path / "manifest.json").is_file())
            self.assertFalse((sealed.path / "recording.json").exists())
            self.assertFalse((sealed.path / "capture.json").exists())
            self.assertEqual(sealed.manifest["frames"]["count"], 4)
            self.assertEqual(sealed.manifest["imu"]["sample_count"], 2)
            self.assertEqual(
                [ordinal for ordinal, _ in store.transaction.boundary_calls],
                [0, 3, 3, 4],
            )
            validate_device_session_manifest(sealed.manifest)

    def test_device_session_config_rejects_raw_sbs_writer(self) -> None:
        with self.assertRaisesRegex(ValueError, "Device Session"):
            DeviceSessionConfig(
                device_id=str(uuid.uuid4()),
                device_label="YLX-12AB34CD",
                hardware_fingerprint="sha256:" + "a" * 64,
                platform="D-Robotics RDK X5 V1.0 + YLX 2UQ2",
                software_version="0.5.0",
                commit="b" * 40,
                width=3840,
                height=1080,
                sensor_fps=60.0,
                video_layout="raw-side-by-side",
            )

    def test_native_data_plane_rejects_python_frame_and_imu_callbacks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = FakeSessionStore()
            with (
                patch(
                    "rp_ylx.recording.device_session.native_session_store",
                    return_value=store,
                ),
                patch(
                    "rp_ylx.recording.device_session.resolve_executable",
                    return_value=Path("/bin/true"),
                ),
            ):
                recorder, _, _ = self.build(root, native_data_plane=True)
                recorder.start()
                with self.assertRaises(DeviceRecordingError) as frame_error:
                    recorder.submit_frame(frame(0))
                with self.assertRaises(DeviceRecordingError) as imu_error:
                    recorder.submit_imu(imu_observation())
                recorder.fail("test_cleanup", "cleanup", recoverable=False)
            self.assertEqual(frame_error.exception.code, "invalid_state")
            self.assertEqual(imu_error.exception.code, "invalid_state")
            assert store.transaction is not None
            self.assertTrue(store.transaction.aborted)

    def test_session_transaction_failure_aborts_without_publishing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = FakeSessionStore(fail_finish=True)
            with (
                patch(
                    "rp_ylx.recording.device_session.native_session_store",
                    return_value=store,
                ),
                patch(
                    "rp_ylx.recording.device_session.resolve_executable",
                    return_value=Path("/bin/true"),
                ),
            ):
                recorder, _, _ = self.build(root, native_data_plane=True)
                recorder.start()
                assert store.transaction is not None
                store.transaction.advance(frames=3)
                with self.assertRaises(DeviceRecordingError) as raised:
                    recorder.stop()
            self.assertEqual(raised.exception.code, "encoder_failed")
            self.assertTrue(store.transaction.aborted)
            self.assertFalse((root / recorder._plan.session_id).exists())

    def test_live_progress_includes_audio_snapshot_bytes_before_stop(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, audios = self.build(root, audio_enabled=True)
            recorder.start()
            audios[0].advance_live(sample_count=4_800, bytes_written=47)

            live = recorder.current_recording_state

            assert live is not None
            self.assertEqual(live["progress"]["bytes_written"], 47)
            recorder.fail("test_cleanup", "cleanup", recoverable=False)

    def test_active_take_pending_allows_writer_inflight_plus_queue_capacity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root, queue_capacity=2)

            snapshot = {
                "session_id": recorder._plan.session_id,
                "frame_domain": 3,
                "frames_written": 0,
                "pending_frames": 3,
                "drop_events": [],
            }
            recorder._apply_active_take_snapshot(snapshot, expected_frames_written=0)

            self.assertEqual(recorder._frame_domain, 3)

            snapshot["pending_frames"] = 4
            with self.assertRaises(DeviceRecordingError) as raised:
                recorder._apply_active_take_snapshot(snapshot, expected_frames_written=0)
            self.assertEqual(raised.exception.code, "active_take_writer_failed")

    def test_native_recorder_exposes_only_one_session_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = FakeSessionStore()
            with (
                patch(
                    "rp_ylx.recording.device_session.native_session_store",
                    return_value=store,
                ),
                patch(
                    "rp_ylx.recording.device_session.resolve_executable",
                    return_value=Path("/bin/true"),
                ),
            ):
                recorder, _, _ = self.build(root, native_data_plane=True)
                recorder.start()
                transaction = recorder.native_recording_transaction()
                self.assertIs(transaction, store.transaction)
                self.assertFalse(hasattr(recorder, "native_split_sink_targets"))
                recorder.fail("test_cleanup", "cleanup", recoverable=False)

    def test_explicit_source_test_adapter_uses_python_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root)
            recorder.start()
            self.feed(recorder, 3)
            sealed = recorder.stop()

            self.assertEqual(sealed.manifest["frames"]["count"], 3)
            validate_device_session_manifest(sealed.manifest)

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

    def test_session_store_owns_safe_manifest_and_artifact_io(self) -> None:
        native = FakeNativeSessionStoreIo()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch(
                "rp_ylx.recording.device_session._session_store_or_none",
                return_value=native,
            ):
                recorder, _, _ = self.build(root)
                recorder.start()
                self.feed(recorder, 4)
                sealed = recorder.stop()
                inspected, _ = inspect_device_session_directory(sealed.path)
                validated = validate_device_session_directory(sealed.path)

        self.assertEqual(inspected["session_id"], sealed.manifest["session_id"])
        self.assertEqual(validated["session_id"], sealed.manifest["session_id"])
        open_paths = [relative for _, relative in native.open_calls]
        self.assertGreaterEqual(open_paths.count("manifest.json"), 2)
        for relative in (
            "video/left_00000.mp4",
            "video/right_00000.mp4",
            "frames.ndjson",
            "imu.ndjson",
        ):
            self.assertIn(relative, open_paths)
        self.assertGreaterEqual(len(native.read_calls), 2)
        self.assertEqual(len(native.verify_calls), 6)

    def test_audio_segments_are_declared_and_downloadable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, audios = self.build(root, audio_enabled=True)
            recorder.start()
            self.feed(recorder, 3)
            sealed = recorder.stop()

            self.assertTrue(audios[0].started)
            audio = sealed.manifest["audio"]
            self.assertEqual(sealed.manifest["schema"], "ylx.device-session.v2")
            self.assertEqual(sealed.manifest["imu"]["coordinate_frame"], "raw_device_axes")
            self.assertEqual(audio["state"], "recorded")
            self.assertEqual(audio["requested_mode"], "enabled")
            self.assertEqual(audio["resolved_mode"], "enabled")
            self.assertEqual(audio["codec"], "pcm_s16le")
            self.assertEqual(audio["container"], "wav")
            self.assertEqual(audio["sample_rate"], 48_000)
            self.assertEqual(audio["sample_count"], 4_800)
            self.assertEqual(audio["sync"]["time_base"], "host_monotonic")
            self.assertEqual(audio["sync"]["video_time_reference"], "session_time_seconds")
            self.assertGreaterEqual(audio["sync"]["start_time_seconds"], 0)
            self.assertGreater(
                audio["sync"]["end_time_seconds"],
                audio["sync"]["start_time_seconds"],
            )
            self.assertEqual(audio["segments"][0]["pcm_payload_bytes"], 4_800 * 2 * 2)
            self.assertEqual(audio["segments"][0]["wav_header_bytes"], 44)
            artifact = audio["segments"][0]["artifact"]
            self.assertEqual(artifact["role"], "audio.wav")
            self.assertEqual(artifact["media_type"], "audio/wav")
            self.assertEqual((sealed.path / artifact["path"]).read_bytes()[:4], b"RIFF")
            self.assertIn(
                "audio/audio_00000.wav",
                [item["path"] for item in iter_device_session_v1_artifacts(sealed.manifest)],
            )
            validate_device_session_manifest(sealed.manifest)

    def test_audio_disabled_is_declared_as_not_recorded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, _ = self.build(root, audio_enabled=False)
            recorder.start()
            self.feed(recorder, 3)
            sealed = recorder.stop()

        self.assertEqual(sealed.manifest["schema"], "ylx.device-session.v2")
        self.assertEqual(
            sealed.manifest["audio"],
            {
                "state": "not_recorded",
                "requested_mode": "disabled",
                "resolved_mode": "disabled",
                "reason": "user_disabled",
            },
        )
        self.assertNotIn(
            "audio/audio_00000.wav",
            [item["path"] for item in iter_device_session_v1_artifacts(sealed.manifest)],
        )
        validate_device_session_manifest(sealed.manifest)

    def test_final_progress_bytes_match_manifest_artifact_bytes_including_audio(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder, _, audios = self.build(root, audio_enabled=True)
            recorder.start()
            self.feed(recorder, 3)
            audios[0].advance_live(sample_count=4_800, bytes_written=47)
            verifying_states: list[dict[str, object]] = []

            sealed = recorder.stop(
                before_publish=lambda: verifying_states.append(
                    deepcopy(recorder.current_recording_state)
                )
            )

            self.assertEqual(len(verifying_states), 1)
            final_state = verifying_states[0]
            self.assertEqual(final_state["state"], "verifying")
            expected_bytes = manifest_artifact_bytes_total(
                sealed.manifest,
                manifest_bytes=sealed.manifest_bytes,
                session_id=sealed.manifest["session_id"],
                code="artifact_invalid",
            )
            self.assertEqual(final_state["progress"]["bytes_written"], expected_bytes)

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
