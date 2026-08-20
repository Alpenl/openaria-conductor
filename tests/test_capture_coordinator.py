from __future__ import annotations

import errno
import hashlib
import json
import os
import tempfile
import threading
import time
import unittest
import uuid
from dataclasses import replace
from pathlib import Path
from typing import BinaryIO
from unittest.mock import patch

from rp_ylx.api import CaptureCommand, ProviderError
from rp_ylx.api.downloads import (
    ArtifactAccessError,
    DirectorySessionStore,
    iter_device_session_v1_artifacts,
)
from rp_ylx.api.events import (
    EventReplayBuffer,
    validate_capture_status,
    validate_device_descriptor,
    validate_retained_unsuccessful_outcome,
    validate_safe_swap_v3_receipt,
    validate_session_list,
)
from rp_ylx.api.preview import PreviewFrameUnavailable
from rp_ylx.camera import FrameObservation, StereoFrame
from rp_ylx.imu import ImuObservation, ImuSample, RawVector3
from rp_ylx.recording import (
    CaptureCoordinator,
    CoordinatorConfig,
    DeviceRecordingError,
    DeviceSessionConfig,
    initialize_capture_volume,
    validate_device_session_directory,
)
from rp_ylx.recording.coordinator import _TrackedRepresentation
from rp_ylx.recording.stereo_encoder import ClosedSegment, StereoEncoderError

JPEG = b"\xff\xd8raw-side-by-side\xff\xd9"
COMMIT = "a" * 40


class _FaultingBinaryStream:
    def __init__(self, stream: BinaryIO, fault: str) -> None:
        self._stream = stream
        self._fault = fault

    def write(self, payload: bytes) -> int:
        if self._fault == "short_write":
            return self._stream.write(payload[:-1])
        return self._stream.write(payload)

    def flush(self) -> None:
        if self._fault == "flush":
            raise OSError("模拟 flush 失败")
        self._stream.flush()

    def fileno(self) -> int:
        return self._stream.fileno()

    def tell(self) -> int:
        return self._stream.tell()

    def close(self) -> None:
        self._stream.close()

    def __enter__(self) -> _FaultingBinaryStream:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


class _RecordingFaultInjection:
    def __init__(self, target_name: str, fault: str) -> None:
        self._target_name = target_name
        self._fault = fault
        self._descriptors: set[int] = set()
        self._real_open = Path.open
        self._real_fsync = os.fsync

    def open(self, path: Path, *args: object, **kwargs: object) -> BinaryIO:
        stream = self._real_open(path, *args, **kwargs)  # type: ignore[arg-type]
        if path.name != self._target_name:
            return stream
        if self._fault == "fsync":
            self._descriptors.add(stream.fileno())
        return _FaultingBinaryStream(
            stream,
            self._fault if self._fault != "fsync" else "none",
        )  # type: ignore[return-value]

    def fsync(self, descriptor: int) -> None:
        if descriptor in self._descriptors:
            self._descriptors.remove(descriptor)
            raise OSError("模拟 fsync 失败")
        self._real_fsync(descriptor)


def _path_open_fault(injection: _RecordingFaultInjection) -> object:
    def open_with_fault(path: Path, *args: object, **kwargs: object) -> BinaryIO:
        return injection.open(path, *args, **kwargs)

    return open_with_fault


class FakeSources:
    def __init__(self) -> None:
        self.open_handle_count = 0
        self.mode: str | None = None
        self.generation_id: str | None = None
        self.submit_frame: object | None = None
        self.submit_imu: object | None = None

    def start(
        self,
        *,
        mode: str,
        generation_id: str,
        submit_frame: object,
        submit_imu: object,
        on_failure: object,
        native_recorder: object | None = None,
    ) -> None:
        del native_recorder
        self.mode = mode
        self.generation_id = generation_id
        self.submit_frame = submit_frame
        self.submit_imu = submit_imu
        self.open_handle_count = 2

    def stop(self) -> None:
        self.open_handle_count = 0


class FakeSourcesWithLatestImu(FakeSources):
    def __init__(self, observation: ImuObservation) -> None:
        super().__init__()
        self.observation = observation

    def latest_imu_observation(self) -> ImuObservation:
        return self.observation


class FakeSplitEyeEncoder:
    def __init__(self, out_dir: Path, *, segment_frames: int, **unused: object) -> None:
        del unused
        self._out_dir = out_dir
        self._segment_frames = segment_frames
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
        del jpeg
        if not self._started:
            raise StereoEncoderError("invalid_state", "助手未启动")
        self._submitted += 1
        if self._submitted % self._segment_frames == 0:
            self._close(self._submitted - self._segment_frames, self._submitted)

    def finish(self, *, timeout: float = 30.0) -> tuple[ClosedSegment, ...]:
        del timeout
        closed = len(self._segments) * self._segment_frames
        if closed < self._submitted:
            self._close(closed, self._submitted)
        return self.segments

    def abort(self) -> None:
        self.aborted = True

    def _close(self, start_frame: int, end_frame: int) -> None:
        index = len(self._segments)
        paths: dict[str, tuple[str, int]] = {}
        for eye in ("left", "right"):
            name = f"{eye}_{index:05d}.mp4"
            body = f"{eye}-{index}-{start_frame}-{end_frame}".encode() * 8
            (self._out_dir / name).write_bytes(body)
            paths[eye] = (f"video/{name}", len(body))
        self._segments.append(
            ClosedSegment(
                index=index,
                start_frame=start_frame,
                end_frame=end_frame,
                left_path=paths["left"][0],
                left_bytes=paths["left"][1],
                right_path=paths["right"][0],
                right_bytes=paths["right"][1],
            )
        )


class FakeNativeSessionIo:
    def __init__(self) -> None:
        self.artifact_calls: list[tuple[str, list[str]]] = []

    def device_session_v1_artifacts(
        self,
        manifest: bytes,
        session_id: str,
    ) -> list[dict[str, object]]:
        decoded = json.loads(manifest)
        artifacts = list(iter_device_session_v1_artifacts(decoded))
        self.artifact_calls.append((session_id, [str(item["path"]) for item in artifacts]))
        return artifacts


def command(key: str, body: dict[str, object]) -> CaptureCommand:
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    return CaptureCommand("operator", key, body, canonical)


def start_command(
    key: str,
    *,
    mode: str = "production",
    display_name: str = "测试录制",
) -> CaptureCommand:
    return command(
        key,
        {
            "schema": "ylx.capture-start.v2",
            "mode": mode,
            "display_name": display_name,
            "take": {"kind": "new"},
        },
    )


def stop_command(key: str, *, reason: str = "user") -> CaptureCommand:
    return command(key, {"schema": "ylx.capture-stop.v2", "reason": reason})


def frame(
    sequence: int = 0,
    *,
    generation_payload: bytes = JPEG,
    dropped_before: int = 0,
) -> FrameObservation:
    return FrameObservation(
        StereoFrame(
            source_sequence=sequence,
            host_monotonic_ns=1_000_000 + sequence,
            left=b"left-preview",
            right=b"right-preview",
            raw_side_by_side=generation_payload,
        ),
        dropped_before=dropped_before,
    )


def imu_observation(
    sequence: int = 0,
    *,
    accelerometer: tuple[int, int, int] = (1, 2, 3),
    gyroscope: tuple[int, int, int] = (4, 5, 6),
    sync_quality: str = "insufficient",
) -> ImuObservation:
    samples = []
    for sample_index in range(2):
        sample_sequence = sequence + sample_index
        samples.append(
            ImuSample(
                sequence=sample_sequence,
                packet_sequence=sequence // 2,
                sample_index=sample_index,
                device_timestamp_raw=sample_sequence,
                device_ticks=sample_sequence,
                host_read_start_ns=10_000 + sample_sequence,
                host_read_end_ns=10_100 + sample_sequence,
                host_monotonic_ns=10_050 + sample_sequence,
                accelerometer=RawVector3(*accelerometer),
                gyroscope=RawVector3(*gyroscope),
                sync_offset_ns=None,
                sync_residual_ns=None,
                sync_quality=sync_quality,
            )
        )
    return ImuObservation((samples[0], samples[1]), dropped_samples=0)


class CaptureCoordinatorTest(unittest.TestCase):
    def test_tracked_representation_forwards_send_to_fast_path(self) -> None:
        calls: list[tuple[int, int, int | None]] = []
        releases: list[bool] = []

        class _Representation:
            etag = '"etag"'
            size = 4
            content_type = "application/octet-stream"

            def close(self) -> None:
                return None

            def read(self, offset: int = 0, length: int | None = None) -> bytes:
                del offset, length
                return b""

            def iter_chunks(
                self,
                offset: int = 0,
                length: int | None = None,
                *,
                chunk_size: int = 1024 * 1024,
            ) -> object:
                del offset, length, chunk_size
                raise AssertionError("send_to fast path was not used")

            def send_to(
                self,
                output_descriptor: int,
                offset: int = 0,
                length: int | None = None,
            ) -> int:
                calls.append((output_descriptor, offset, length))
                return 2

        tracked = _TrackedRepresentation(_Representation(), lambda: releases.append(True))

        self.assertEqual(tracked.send_to(7, 1, 2), 2)
        self.assertEqual(calls, [(7, 1, 2)])
        tracked.close()
        self.assertEqual(releases, [True])

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.mountpoint = self.root / "volume"
        self.state_root = self.root / "state"
        self.mountpoint.mkdir()
        self.volume_id = initialize_capture_volume(self.mountpoint)
        self.session_config = DeviceSessionConfig(
            device_id=str(uuid.uuid4()),
            device_label="YLX-12AB34CD",
            hardware_fingerprint="sha256:" + "b" * 64,
            platform="D-Robotics RDK X5 V1.0 + YLX 2UQ2",
            software_version="0.5.0",
            commit=COMMIT,
            width=3840,
            height=1080,
            sensor_fps=60.0,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def coordinator(
        self,
        *,
        before_write: object | None = None,
        minimum_available_bytes: int = 0,
        minimum_available_inodes: int = 0,
        checkpoint_interval: float = 1.0,
        queue_capacity: int = 128,
        enqueue_timeout: float = 0.05,
        sources: object | None = None,
    ) -> CaptureCoordinator:
        return CaptureCoordinator(
            CoordinatorConfig(
                self.mountpoint,
                self.state_root,
                self.session_config,
                minimum_available_bytes=minimum_available_bytes,
                minimum_available_inodes=minimum_available_inodes,
                checkpoint_interval=checkpoint_interval,
                queue_capacity=queue_capacity,
                enqueue_timeout=enqueue_timeout,
            ),
            mount_checker=lambda path: path == self.mountpoint.resolve(),
            sources=sources,  # type: ignore[arg-type]
            before_write=before_write,  # type: ignore[arg-type]
        )

    def active_session_id(self, coordinator: CaptureCoordinator) -> str:
        status = coordinator.capture_status()
        validate_capture_status(status)
        active = status["snapshot"]["active_recording"]
        return active["recording_state"]["session_id"]

    def seal_one(
        self,
        coordinator: CaptureCoordinator,
        *,
        prefix: str = "one",
        mode: str = "production",
        reason: str = "user",
    ) -> str:
        coordinator.start_capture(start_command(f"{prefix}-start", mode=mode))
        session_id = self.active_session_id(coordinator)
        self.assertTrue(coordinator.submit_frame(frame()))
        result = coordinator.stop_capture(stop_command(f"{prefix}-stop", reason=reason))
        expected_status = 204 if reason == "safe_swap" else 202
        self.assertEqual(result.status, expected_status)
        if expected_status == 202:
            self.assertEqual(result.body["snapshot"]["device_state"], "idle")
        return session_id

    def assert_storage_admission_failure(
        self,
        coordinator: CaptureCoordinator,
        expected_code: str,
    ) -> None:
        try:
            validate_capture_status(coordinator.capture_status())
            descriptor = coordinator.device_descriptor("v3", "customer")
            validate_device_descriptor(
                descriptor,
                api_version="v3",
                security_profile="customer",
            )
            self.assertFalse(descriptor["capabilities"]["capture"])
            self.assertEqual(
                descriptor["storage"],
                {
                    "volume_id": None,
                    "total_bytes": 0,
                    "available_bytes": 0,
                    "writable": False,
                },
            )

            with self.assertRaises(ProviderError) as rejected:
                coordinator.start_capture(start_command(expected_code))
            self.assertEqual(rejected.exception.code, expected_code)
            self.assertEqual(rejected.exception.status, 409)
        finally:
            coordinator.close()

    def test_volume_requires_explicit_marker_mount_and_capacity(self) -> None:
        missing = self.root / "missing-marker"
        missing.mkdir()
        self.assert_storage_admission_failure(
            CaptureCoordinator(
                CoordinatorConfig(missing, self.state_root, self.session_config),
                mount_checker=lambda path: path == missing.resolve(),
            ),
            "volume_not_admitted",
        )

        with patch(
            "rp_ylx.recording.coordinator._stat_capacity",
            return_value=(10_000, 9, 100),
        ):
            self.assert_storage_admission_failure(
                self.coordinator(minimum_available_bytes=10),
                "insufficient_space",
            )

        with patch(
            "rp_ylx.recording.coordinator._stat_capacity",
            return_value=(10_000, 100, 9),
        ):
            self.assert_storage_admission_failure(
                self.coordinator(minimum_available_inodes=10),
                "insufficient_inodes",
            )

        with patch("rp_ylx.recording.coordinator.os.access", return_value=False):
            self.assert_storage_admission_failure(
                self.coordinator(),
                "volume_read_only",
            )

    def test_unmounted_volume_keeps_control_plane_and_recovers_without_restart(self) -> None:
        mounted = False
        coordinator = CaptureCoordinator(
            CoordinatorConfig(
                self.mountpoint,
                self.state_root,
                self.session_config,
                minimum_available_bytes=0,
                minimum_available_inodes=0,
            ),
            mount_checker=lambda path: mounted and path == self.mountpoint.resolve(),
        )
        try:
            validate_capture_status(coordinator.capture_status())
            descriptor = coordinator.device_descriptor("v3", "customer")
            validate_device_descriptor(
                descriptor,
                api_version="v3",
                security_profile="customer",
            )
            self.assertFalse(descriptor["capabilities"]["capture"])
            self.assertEqual(
                descriptor["storage"],
                {
                    "volume_id": None,
                    "total_bytes": 0,
                    "available_bytes": 0,
                    "writable": False,
                },
            )

            with self.assertRaises(ProviderError) as unavailable:
                coordinator.start_capture(start_command("volume-missing"))
            self.assertEqual(unavailable.exception.code, "volume_not_mounted")
            self.assertEqual(unavailable.exception.status, 409)

            mounted = True
            validate_capture_status(coordinator.capture_status())
            automatically_restored = coordinator.device_descriptor("v3", "customer")
            self.assertTrue(automatically_restored["capabilities"]["capture"])
            self.assertEqual(automatically_restored["storage"]["volume_id"], self.volume_id)
            result = coordinator.start_capture(start_command("volume-restored"))
            self.assertEqual(result.status, 202)
            self.assertEqual(result.body["snapshot"]["device_state"], "recording")
            self.assertEqual(coordinator.volume_id, self.volume_id)
            restored = coordinator.device_descriptor("v3", "customer")
            self.assertTrue(restored["capabilities"]["capture"])
            self.assertEqual(restored["storage"]["volume_id"], self.volume_id)
            self.assertTrue(restored["storage"]["writable"])
        finally:
            coordinator.close()

    def test_start_rechecks_thresholds_and_rejects_wrong_volume(self) -> None:
        coordinator = self.coordinator(minimum_available_bytes=10, minimum_available_inodes=10)
        try:
            wrong = start_command("wrong-volume")
            wrong_body = {**wrong.body, "volume_id": str(uuid.uuid4())}
            with self.assertRaises(ProviderError) as mismatch:
                coordinator.start_capture(command("wrong-volume", wrong_body))
            self.assertEqual(mismatch.exception.code, "volume_mismatch")

            cases = (
                ((100, 9, 100), "insufficient_space"),
                ((100, 100, 9), "insufficient_inodes"),
            )
            for capacity, expected in cases:
                with (
                    self.subTest(expected=expected),
                    patch(
                        "rp_ylx.recording.coordinator._stat_capacity",
                        return_value=capacity,
                    ),
                ):
                    with self.assertRaises(ProviderError) as rejected:
                        coordinator.start_capture(start_command(expected))
                    self.assertEqual(rejected.exception.code, expected)
        finally:
            coordinator.close()

    def test_normal_v1_seal_is_manifest_last_and_gateway_readable(self) -> None:
        coordinator = self.coordinator()
        try:
            session_id = self.seal_one(coordinator)
            session = self.mountpoint / "recordings" / session_id
            manifest = validate_device_session_directory(session)
            self.assertEqual(manifest["schema"], "ylx.device-session.v1")
            self.assertEqual(manifest["video"]["layout"], "raw-side-by-side")
            self.assertEqual(manifest["frames"]["count"], 1)
            self.assertEqual(manifest["camera"]["nominal_fps"], 60.0)
            self.assertAlmostEqual(
                manifest["camera"]["effective_fps"],
                manifest["frames"]["count"] / manifest["time"]["duration_seconds"],
            )
            self.assertEqual(
                manifest["integrity"]["quality_policy"]["policy_id"],
                "rdk-x5-lossless-v1",
            )
            self.assertFalse((session / "recording.json").exists())
            self.assertFalse((session / "capture.json").exists())

            listed = coordinator.list_sessions(cursor=None, limit=50, take_id=None)
            validate_session_list(listed, limit=50, take_id=None)
            self.assertEqual([item["session_id"] for item in listed["items"]], [session_id])

            manifest_handle = coordinator.open_manifest(session_id, "v3")
            try:
                manifest_bytes = manifest_handle.read()
                self.assertEqual(json.loads(manifest_bytes)["session_id"], session_id)
            finally:
                manifest_handle.close()
            video = manifest["video"]["artifact"]
            artifact = coordinator.open_verified_artifact(session_id, video["artifact_id"], "v3")
            try:
                self.assertEqual(artifact.read(), JPEG)
            finally:
                artifact.close()
        finally:
            coordinator.close()

    def test_legacy_sessions_root_remains_listed_and_downloadable(self) -> None:
        first = self.coordinator()
        try:
            session_id = self.seal_one(first, prefix="legacy-sessions")
            current = self.mountpoint / "recordings" / session_id
            legacy_root = self.mountpoint / "sessions"
            legacy_root.mkdir()
            legacy = legacy_root / session_id
            current.rename(legacy)
            manifest = validate_device_session_directory(legacy)
        finally:
            first.close()

        restarted = self.coordinator()
        try:
            listed = restarted.list_sessions(cursor=None, limit=50, take_id=None)
            validate_session_list(listed, limit=50, take_id=None)
            self.assertEqual([item["session_id"] for item in listed["items"]], [session_id])

            manifest_handle = restarted.open_manifest(session_id, "v3")
            try:
                self.assertEqual(json.loads(manifest_handle.read())["session_id"], session_id)
            finally:
                manifest_handle.close()

            video = manifest["video"]["artifact"]
            artifact = restarted.open_verified_artifact(session_id, video["artifact_id"], "v3")
            try:
                self.assertEqual(artifact.read(), JPEG)
            finally:
                artifact.close()
        finally:
            restarted.close()

    def test_list_sessions_returns_newest_session_first(self) -> None:
        coordinator = self.coordinator()
        try:
            older = self.seal_one(coordinator, prefix="older")
            time.sleep(0.01)
            newer = self.seal_one(coordinator, prefix="newer")

            first_page = coordinator.list_sessions(cursor=None, limit=1, take_id=None)
            validate_session_list(first_page, limit=1, take_id=None)
            self.assertEqual([item["session_id"] for item in first_page["items"]], [newer])
            self.assertEqual(first_page["next_cursor"], newer)

            second_page = coordinator.list_sessions(cursor=newer, limit=1, take_id=None)
            validate_session_list(second_page, limit=1, take_id=None)
            self.assertEqual([item["session_id"] for item in second_page["items"]], [older])
            self.assertIsNone(second_page["next_cursor"])
        finally:
            coordinator.close()

    def test_raw_sbs_frame_is_used_for_preview_without_eye_materialization(self) -> None:
        coordinator = self.coordinator()
        try:
            coordinator.start_capture(start_command("raw-sbs-preview"))
            observation = FrameObservation(
                StereoFrame(
                    source_sequence=0,
                    host_monotonic_ns=1_000_000,
                    left=b"",
                    right=b"",
                    raw_side_by_side=JPEG,
                ),
                dropped_before=0,
            )

            self.assertTrue(coordinator.submit_frame(observation))
            preview = coordinator.latest_preview(fps=None, accept="image/jpeg")
            self.assertEqual(preview.body, JPEG)
            self.assertEqual(preview.content_length, len(JPEG))
            result = coordinator.stop_capture(stop_command("raw-sbs-preview-stop"))
            self.assertEqual(result.status, 202)
            with self.assertRaises(PreviewFrameUnavailable):
                coordinator.latest_preview(fps=None, accept="image/jpeg")
        finally:
            coordinator.close()

    def test_capture_status_exposes_latest_live_raw_imu_during_recording(self) -> None:
        coordinator = self.coordinator()
        try:
            coordinator.start_capture(start_command("live-imu"))
            session_id = self.active_session_id(coordinator)
            self.assertTrue(
                coordinator.submit_imu(
                    imu_observation(
                        20,
                        accelerometer=(12, -8, 979),
                        gyroscope=(1, -2, 5),
                        sync_quality="good",
                    )
                )
            )

            status = coordinator.capture_status()
            validate_capture_status(status)
            live_imu = status["snapshot"]["runtime"]["live_imu"]
            self.assertEqual(live_imu["session_id"], session_id)
            self.assertEqual(
                live_imu["clock"],
                {"time_base": "host_monotonic", "timestamp_ns": 10_071},
            )
            self.assertEqual(
                live_imu["raw"],
                {
                    "units": "raw_int16",
                    "accelerometer": {"x": 12, "y": -8, "z": 979},
                    "gyroscope": {"x": 1, "y": -2, "z": 5},
                },
            )
            self.assertEqual(live_imu["sync"], {"quality": "good"})

            self.assertTrue(coordinator.submit_frame(frame()))
            result = coordinator.stop_capture(stop_command("live-imu-stop"))
            self.assertEqual(result.status, 202)
            validate_capture_status(result.body)
            self.assertIsNone(result.body["snapshot"]["runtime"]["live_imu"])
        finally:
            coordinator.close()

    def test_capture_status_reads_live_raw_imu_from_sources_latest_snapshot(self) -> None:
        sources = FakeSourcesWithLatestImu(
            imu_observation(
                40,
                accelerometer=(-1, -2, -3),
                gyroscope=(7, 8, 9),
                sync_quality="degraded",
            )
        )
        coordinator = self.coordinator(sources=sources)
        try:
            coordinator.start_capture(start_command("sources-live-imu"))
            status = coordinator.capture_status()
            validate_capture_status(status)
            live_imu = status["snapshot"]["runtime"]["live_imu"]
            self.assertEqual(live_imu["clock"]["timestamp_ns"], 10_091)
            self.assertEqual(
                live_imu["raw"]["accelerometer"],
                {"x": -1, "y": -2, "z": -3},
            )
            self.assertEqual(live_imu["raw"]["gyroscope"], {"x": 7, "y": 8, "z": 9})
            self.assertEqual(live_imu["sync"], {"quality": "degraded"})
        finally:
            coordinator.close()

    def test_source_sequence_gap_fails_and_is_retained_without_manifest(self) -> None:
        coordinator = self.coordinator()
        try:
            coordinator.start_capture(start_command("source-gap-start"))
            session_id = self.active_session_id(coordinator)
            with self.assertRaises(DeviceRecordingError) as rejected:
                coordinator.submit_frame(frame(3, dropped_before=2))
            self.assertEqual(rejected.exception.code, "source_sequence_gap")
            retained = coordinator.retained_unsuccessful_outcome(session_id)
            state = retained["outcome"]["recording_state"]
            self.assertEqual(state["state"], "failed")
            self.assertEqual(state["diagnostics"][0]["code"], "source_sequence_gap")
            partial = self.mountpoint / "recordings" / f"{session_id}.partial"
            self.assertTrue(partial.is_dir())
            self.assertFalse((partial / "manifest.json").exists())
            with self.assertRaises(PreviewFrameUnavailable):
                coordinator.latest_preview(fps=None, accept="image/jpeg")
        finally:
            coordinator.close()

    def test_writer_drop_exceeding_lossless_policy_never_seals(self) -> None:
        writer_blocked = threading.Event()
        release_writer = threading.Event()

        def slow_writer(role: str, payload: bytes) -> None:
            if role == "video.raw-side-by-side" and payload:
                writer_blocked.set()
                if not release_writer.wait(timeout=2):
                    raise TimeoutError("test did not release writer")

        coordinator = self.coordinator(
            before_write=slow_writer,
            queue_capacity=1,
            enqueue_timeout=0,
        )
        try:
            coordinator.start_capture(start_command("drop-gate-start"))
            session_id = self.active_session_id(coordinator)
            self.assertTrue(coordinator.submit_frame(frame(0)))
            self.assertTrue(writer_blocked.wait(timeout=1))
            self.assertTrue(coordinator.submit_frame(frame(1)))
            self.assertFalse(coordinator.submit_frame(frame(2)))
            release_writer.set()
            with self.assertRaises(ProviderError) as rejected:
                coordinator.stop_capture(stop_command("drop-gate-stop"))
            self.assertEqual(rejected.exception.code, "drop_quality_exceeded")
            partial = self.mountpoint / "recordings" / f"{session_id}.partial"
            self.assertTrue(partial.is_dir())
            self.assertFalse((partial / "manifest.json").exists())
            retained = coordinator.retained_unsuccessful_outcome(session_id)
            state = retained["outcome"]["recording_state"]
            self.assertEqual(state["state"], "failed")
            self.assertEqual(state["diagnostics"][0]["code"], "drop_quality_exceeded")
            diagnostic = state["diagnostics"][0]["message"]
            self.assertIn("classification=write_backpressure", diagnostic)
            self.assertIn("contiguous=1", diagnostic)
            self.assertIn("window_drops=1", diagnostic)
            self.assertIn("limits=contiguous:0,total:0,fraction:0,window:0", diagnostic)
        finally:
            release_writer.set()
            coordinator.close()

    def test_production_provider_projections_pass_all_frozen_validators(self) -> None:
        coordinator = self.coordinator()
        try:
            for api_version in ("v2", "v3"):
                descriptor = coordinator.device_descriptor(api_version, "customer")
                validate_device_descriptor(
                    descriptor,
                    api_version=api_version,
                    security_profile="customer",
                )
            validate_capture_status(coordinator.capture_status())
            self.assertEqual(EventReplayBuffer().publish(coordinator.capture_snapshot_event()), "1")

            coordinator.start_capture(start_command("validator-start"))
            session_id = self.active_session_id(coordinator)
            validate_capture_status(coordinator.capture_status())
            self.assertEqual(EventReplayBuffer().publish(coordinator.capture_snapshot_event()), "1")
            coordinator.report_media_loss(generation_id=coordinator.generation_id)

            status = coordinator.capture_status()
            validate_capture_status(status)
            retained = coordinator.retained_unsuccessful_outcome(session_id)
            validate_retained_unsuccessful_outcome(retained, session_id=session_id)
            self.assertEqual(EventReplayBuffer().publish(coordinator.capture_snapshot_event()), "1")
            descriptor = coordinator.device_descriptor("v3", "customer")
            validate_device_descriptor(
                descriptor,
                api_version="v3",
                security_profile="customer",
            )
            self.assertEqual(descriptor["storage"]["available_bytes"], 0)
            self.assertFalse(descriptor["storage"]["writable"])
        finally:
            coordinator.close()

    def test_duplicate_control_and_modes_are_mutually_exclusive(self) -> None:
        coordinator = self.coordinator()
        try:
            start = start_command("same-key")
            first = coordinator.start_capture(start)
            replay = coordinator.start_capture(start)
            self.assertEqual(first.body, replay.body)
            self.assertTrue(replay.replayed)

            changed = start_command("same-key", mode="calibration")
            with self.assertRaises(ProviderError) as conflict:
                coordinator.start_capture(changed)
            self.assertEqual(conflict.exception.code, "idempotency_conflict")

            with self.assertRaises(ProviderError) as busy:
                coordinator.start_capture(start_command("other-key", mode="calibration"))
            self.assertEqual(busy.exception.code, "capture_busy")
            coordinator.submit_frame(frame())
            coordinator.stop_capture(stop_command("stop"))
        finally:
            coordinator.close()

    def test_recording_progress_checkpoints_and_revision_survives_restart(self) -> None:
        coordinator = self.coordinator(checkpoint_interval=0)
        coordinator.start_capture(start_command("checkpoint-start"))
        session_id = self.active_session_id(coordinator)
        coordinator.submit_frame(frame(0))
        deadline = time.monotonic() + 1
        recording_path = self.mountpoint / "recordings" / f"{session_id}.partial/recording.json"
        while time.monotonic() < deadline:
            state = json.loads(recording_path.read_bytes())
            if state["progress"]["captured_frames"] == 1:
                break
            time.sleep(0.01)
            coordinator.submit_frame(frame(1))
        self.assertGreaterEqual(state["progress"]["captured_frames"], 1)
        self.assertGreater(state["progress"]["bytes_written"], 0)
        checkpoint_revision = coordinator.capture_status()["source_revision"]
        revision_deadline = time.monotonic() + 1
        while (
            checkpoint_revision < state["state_revision"] and time.monotonic() < revision_deadline
        ):
            time.sleep(0.01)
            checkpoint_revision = coordinator.capture_status()["source_revision"]
        self.assertGreaterEqual(checkpoint_revision, state["state_revision"])
        coordinator.close()

        restarted = self.coordinator()
        try:
            recovered = restarted.retained_unsuccessful_outcome(session_id)
            recovered_revision = recovered["source_revision"]
            self.assertGreater(recovered_revision, checkpoint_revision)
            validate_retained_unsuccessful_outcome(recovered, session_id=session_id)
        finally:
            restarted.close()

    def test_idempotent_stop_replays_after_process_restart(self) -> None:
        first = self.coordinator()
        stop = stop_command("durable-stop")
        first.start_capture(start_command("durable-start"))
        first.submit_frame(frame())
        first_result = first.stop_capture(stop)
        self.assertEqual(first_result.status, 202)
        self.assertEqual(first_result.body["snapshot"]["device_state"], "idle")
        first.close()

        restarted = self.coordinator()
        try:
            replay = restarted.stop_capture(stop)
            self.assertEqual(replay.status, 202)
            self.assertEqual(replay.body, first_result.body)
            self.assertTrue(replay.replayed)
        finally:
            restarted.close()

    def test_stop_without_active_recording_is_idempotent_no_op(self) -> None:
        coordinator = self.coordinator()
        try:
            stop = stop_command("already-idle")
            result = coordinator.stop_capture(stop)
            self.assertEqual((result.status, result.body, result.replayed), (204, None, False))
            replay = coordinator.stop_capture(stop)
            self.assertEqual((replay.status, replay.body, replay.replayed), (204, None, True))
        finally:
            coordinator.close()

    def test_seal_does_not_rescan_artifact_bytes(self) -> None:
        coordinator = self.coordinator()
        try:
            coordinator.start_capture(start_command("incremental-digest-start"))
            coordinator.submit_frame(frame())
            with (
                patch(
                    "rp_ylx.recording.device_session._digest",
                    side_effect=AssertionError("seal rescanned an artifact"),
                ),
                patch(
                    "rp_ylx.recording.coordinator.validate_device_session_directory",
                    side_effect=AssertionError("coordinator repeated full validation"),
                ),
            ):
                result = coordinator.stop_capture(stop_command("incremental-digest-stop"))
            self.assertEqual(result.status, 202)
        finally:
            coordinator.close()

    def test_catalog_and_list_do_not_read_artifact_contents(self) -> None:
        first = self.coordinator()
        try:
            session_id = self.seal_one(first, prefix="lightweight-catalog")
        finally:
            first.close()

        with patch(
            "rp_ylx.recording.coordinator.validate_device_session_directory",
            side_effect=AssertionError("catalog/list performed full artifact validation"),
        ):
            restarted = self.coordinator()
            try:
                listed = restarted.list_sessions(cursor=None, limit=50, take_id=None)
            finally:
                restarted.close()
        self.assertEqual([item["session_id"] for item in listed["items"]], [session_id])
        manifest = json.loads(
            (self.mountpoint / "recordings" / session_id / "manifest.json").read_bytes()
        )
        expected_bytes = sum(
            int(manifest[section]["artifact"]["bytes"]) for section in ("video", "imu", "frames")
        )
        self.assertEqual(listed["items"][0]["total_bytes"], expected_bytes)

    def test_list_sessions_uses_native_artifact_scan_for_total_bytes(self) -> None:
        first = self.coordinator()
        try:
            session_id = self.seal_one(first, prefix="native-list-scan")
        finally:
            first.close()

        native = FakeNativeSessionIo()
        with patch("rp_ylx.recording.device_session._session_io_or_none", return_value=native):
            restarted = self.coordinator()
            try:
                listed = restarted.list_sessions(cursor=None, limit=50, take_id=None)
            finally:
                restarted.close()

        self.assertEqual([item["session_id"] for item in listed["items"]], [session_id])
        manifest = json.loads(
            (self.mountpoint / "recordings" / session_id / "manifest.json").read_bytes()
        )
        expected_paths = [str(item["path"]) for item in iter_device_session_v1_artifacts(manifest)]
        self.assertGreaterEqual(len(native.artifact_calls), 2)
        self.assertTrue(
            all(call_session == session_id for call_session, _ in native.artifact_calls)
        )
        self.assertIn(expected_paths, [paths for _, paths in native.artifact_calls])
        self.assertEqual(
            listed["items"][0]["total_bytes"],
            sum(int(item["bytes"]) for item in iter_device_session_v1_artifacts(manifest)),
        )

    def test_split_eye_session_is_listed_and_downloadable(self) -> None:
        split_config = replace(
            self.session_config,
            frame_decimation=2,
            video_layout="split-eyes",
            segment_seconds=0.1,
        )
        coordinator = CaptureCoordinator(
            CoordinatorConfig(
                self.mountpoint,
                self.state_root,
                split_config,
                minimum_available_bytes=0,
                minimum_available_inodes=0,
                checkpoint_interval=0.0,
            ),
            mount_checker=lambda path: path == self.mountpoint.resolve(),
        )
        try:
            with patch(
                "rp_ylx.recording.device_session.StereoEncoderProcess",
                FakeSplitEyeEncoder,
            ):
                coordinator.start_capture(start_command("split-list-start"))
                session_id = self.active_session_id(coordinator)
                for index in range(7):
                    self.assertTrue(
                        coordinator.submit_frame(
                            frame(index, generation_payload=b"\xff\xd8split\xff\xd9")
                        )
                    )
                result = coordinator.stop_capture(stop_command("split-list-stop"))
            self.assertEqual(result.status, 202)
            listed = coordinator.list_sessions(cursor=None, limit=50, take_id=None)
            validate_session_list(listed, limit=50, take_id=None)
            self.assertEqual([item["session_id"] for item in listed["items"]], [session_id])
            manifest_path = self.mountpoint / "recordings" / session_id / "manifest.json"
            manifest = json.loads(manifest_path.read_bytes())
            self.assertEqual(manifest["video"]["layout"], "split-eyes")
            expected_bytes = (
                sum(
                    int(segment["artifacts"][eye]["bytes"])
                    for segment in manifest["video"]["segments"]
                    for eye in ("left", "right")
                )
                + int(manifest["frames"]["artifact"]["bytes"])
                + int(manifest["imu"]["artifact"]["bytes"])
            )
            self.assertEqual(listed["items"][0]["total_bytes"], expected_bytes)
            left = manifest["video"]["segments"][0]["artifacts"]["left"]
            with coordinator.open_verified_artifact(
                session_id, left["artifact_id"], "v3"
            ) as artifact:
                expected = self.mountpoint / "recordings" / session_id / left["path"]
                self.assertEqual(artifact.read(), expected.read_bytes())
        finally:
            coordinator.close()

    def test_stop_does_not_hold_coordinator_lock_while_writer_checkpoints(self) -> None:
        checkpoint_started = threading.Event()
        release_checkpoint = threading.Event()
        split_config = replace(
            self.session_config,
            frame_decimation=2,
            video_layout="split-eyes",
            segment_seconds=0.1,
        )
        coordinator = CaptureCoordinator(
            CoordinatorConfig(
                self.mountpoint,
                self.state_root,
                split_config,
                minimum_available_bytes=0,
                minimum_available_inodes=0,
                checkpoint_interval=0.0,
            ),
            mount_checker=lambda path: path == self.mountpoint.resolve(),
        )
        original_next_revision = coordinator._next_revision

        def next_revision() -> int:
            if threading.current_thread().name.startswith("rp-ylx-v1-writer-"):
                checkpoint_started.set()
                if not release_checkpoint.wait(timeout=2):
                    raise TimeoutError("测试没有释放 checkpoint")
            return original_next_revision()

        try:
            with (
                patch("rp_ylx.recording.device_session.StereoEncoderProcess", FakeSplitEyeEncoder),
                patch.object(coordinator, "_next_revision", side_effect=next_revision),
            ):
                coordinator.start_capture(start_command("stop-checkpoint-start"))
                coordinator.submit_frame(frame(0, generation_payload=b"\xff\xd8split\xff\xd9"))
                self.assertTrue(checkpoint_started.wait(timeout=1))
                stopped: list[object] = []
                thread = threading.Thread(
                    target=lambda: stopped.append(
                        coordinator.stop_capture(stop_command("stop-checkpoint-stop"))
                    )
                )
                thread.start()
                release_checkpoint.set()
                thread.join(timeout=3)
                self.assertFalse(thread.is_alive())
                self.assertEqual(stopped[0].status, 202)
        finally:
            release_checkpoint.set()
            coordinator.close()

    def test_concurrent_same_stop_replays_after_inflight_stop_finishes(self) -> None:
        manifest_blocked = threading.Event()
        release_manifest = threading.Event()

        def before_write(role: str, payload: bytes) -> None:
            del payload
            if role == "manifest":
                manifest_blocked.set()
                if not release_manifest.wait(timeout=2):
                    raise TimeoutError("测试没有释放 manifest 写入")

        coordinator = self.coordinator(before_write=before_write)
        try:
            coordinator.start_capture(start_command("concurrent-stop-start"))
            self.assertTrue(coordinator.submit_frame(frame()))
            stop = stop_command("concurrent-stop")
            results: list[object] = []
            errors: list[BaseException] = []

            def stop_once() -> None:
                try:
                    results.append(coordinator.stop_capture(stop))
                except BaseException as error:  # pragma: no cover - surfaced below
                    errors.append(error)

            first = threading.Thread(target=stop_once)
            second = threading.Thread(target=stop_once)
            first.start()
            self.assertTrue(manifest_blocked.wait(timeout=1))
            second.start()
            time.sleep(0.05)
            self.assertTrue(second.is_alive())
            release_manifest.set()
            first.join(timeout=3)
            second.join(timeout=3)
            self.assertFalse(first.is_alive())
            self.assertFalse(second.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(sorted(result.status for result in results), [202, 202])
            self.assertEqual(sorted(result.replayed for result in results), [False, True])
        finally:
            release_manifest.set()
            coordinator.close()

    def test_remount_at_same_path_rotates_generation_and_rejects_stale_submitter(self) -> None:
        mount_identity = "mount-a"
        first = CaptureCoordinator(
            CoordinatorConfig(
                self.mountpoint,
                self.state_root,
                self.session_config,
                minimum_available_bytes=0,
                minimum_available_inodes=0,
            ),
            mount_checker=lambda path: path == self.mountpoint.resolve(),
            mount_identity=lambda path: mount_identity,
        )
        previous_generation = first.generation_id
        first.close()

        mount_identity = "mount-b"
        restarted = CaptureCoordinator(
            CoordinatorConfig(
                self.mountpoint,
                self.state_root,
                self.session_config,
                minimum_available_bytes=0,
                minimum_available_inodes=0,
            ),
            mount_checker=lambda path: path == self.mountpoint.resolve(),
            mount_identity=lambda path: mount_identity,
        )
        try:
            self.assertNotEqual(restarted.generation_id, previous_generation)
            restarted.start_capture(start_command("remount-start"))
            with self.assertRaises(DeviceRecordingError) as stale:
                restarted.submit_frame(frame(), generation_id=previous_generation)
            self.assertEqual(stale.exception.code, "stale_generation")
            self.assertEqual(restarted.capture_status()["snapshot"]["device_state"], "recording")
            restarted.submit_frame(frame())
            restarted.stop_capture(stop_command("remount-stop"))
        finally:
            restarted.close()

    def test_calibration_output_uses_the_same_python_read_contract(self) -> None:
        coordinator = self.coordinator()
        try:
            session_id = self.seal_one(coordinator, prefix="cal", mode="calibration")
            session = self.mountpoint / "recordings" / session_id
            manifest = validate_device_session_directory(session)
            self.assertEqual(manifest["capture_mode"], "calibration")
            digest = hashlib.sha256((session / "manifest.json").read_bytes()).hexdigest()
            with (
                DirectorySessionStore(
                    self.mountpoint / "recordings",
                    verified_manifests={session_id: digest},
                ) as store,
                store.open_manifest(session_id, "v3") as representation,
            ):
                self.assertEqual(json.loads(representation.read())["capture_mode"], "calibration")
        finally:
            coordinator.close()

    def test_capture_sources_receive_generation_callbacks_and_release_before_seal(self) -> None:
        sources = FakeSources()
        coordinator = CaptureCoordinator(
            CoordinatorConfig(
                self.mountpoint,
                self.state_root,
                self.session_config,
                minimum_available_bytes=0,
                minimum_available_inodes=0,
            ),
            mount_checker=lambda path: path == self.mountpoint.resolve(),
            sources=sources,
        )
        try:
            coordinator.start_capture(start_command("sources-start", mode="calibration"))
            self.assertEqual(sources.mode, "calibration")
            self.assertEqual(sources.generation_id, coordinator.generation_id)
            self.assertTrue(callable(sources.submit_frame))
            self.assertTrue(callable(sources.submit_imu))
            self.assertEqual(coordinator.open_handle_count, 6)
            sources.submit_frame(frame())  # type: ignore[operator]
            coordinator.stop_capture(stop_command("sources-stop"))
            self.assertEqual(sources.open_handle_count, 0)
            self.assertEqual(coordinator.open_handle_count, 0)
        finally:
            coordinator.close()

    def test_enospc_never_publishes_a_success_manifest(self) -> None:
        def disk_full(role: str, payload: bytes) -> None:
            if role == "video.raw-side-by-side" and payload:
                raise OSError(errno.ENOSPC, "模拟磁盘写满")

        coordinator = self.coordinator(before_write=disk_full)
        try:
            coordinator.start_capture(start_command("full-start"))
            session_id = self.active_session_id(coordinator)
            coordinator.submit_frame(frame())
            with self.assertRaises(ProviderError) as stopped:
                coordinator.stop_capture(stop_command("full-stop"))
            self.assertEqual(stopped.exception.code, "storage_full")
            partial = self.mountpoint / "recordings" / f"{session_id}.partial"
            self.assertTrue(partial.is_dir())
            self.assertFalse((partial / "manifest.json").exists())
            self.assertFalse((self.mountpoint / "recordings" / session_id).exists())
            with self.assertRaises(DeviceRecordingError) as invalid:
                validate_device_session_directory(partial)
            self.assertEqual(invalid.exception.code, "manifest_invalid")
            state = json.loads((partial / "recording.json").read_bytes())
            self.assertEqual(state["state"], "failed")
            self.assertEqual(state["diagnostics"][0]["code"], "storage_full")
            retained = coordinator.retained_unsuccessful_outcome(session_id)
            self.assertEqual(retained["outcome"]["recording_state"]["state"], "failed")
        finally:
            coordinator.close()

    def test_short_write_flush_and_fsync_cannot_publish_a_v1_session(self) -> None:
        cases = (
            ("data-short", "raw-sbs.mjpeg", "short_write"),
            ("manifest-short", "manifest.json", "short_write"),
            ("data-flush", "raw-sbs.mjpeg", "flush"),
            ("manifest-flush", "manifest.json", "flush"),
            ("data-fsync", "raw-sbs.mjpeg", "fsync"),
            ("manifest-fsync", "manifest.json", "fsync"),
        )

        for key, target_name, fault in cases:
            with self.subTest(target_name=target_name, fault=fault):
                injection = _RecordingFaultInjection(target_name, fault)
                coordinator = self.coordinator()
                try:
                    with (
                        patch("pathlib.Path.open", new=_path_open_fault(injection)),
                        patch(
                            "rp_ylx.recording.device_session.os.fsync",
                            side_effect=injection.fsync,
                        ),
                    ):
                        coordinator.start_capture(start_command(f"{key}-start"))
                        session_id = self.active_session_id(coordinator)
                        coordinator.submit_frame(frame())
                        with self.assertRaises(ProviderError) as stopped:
                            coordinator.stop_capture(stop_command(f"{key}-stop"))

                    self.assertEqual(stopped.exception.code, "write_failed")
                    partial = self.mountpoint / "recordings" / f"{session_id}.partial"
                    final = self.mountpoint / "recordings" / session_id
                    self.assertTrue(partial.is_dir())
                    self.assertFalse((partial / "manifest.json").exists())
                    self.assertFalse(final.exists())
                    state = json.loads((partial / "recording.json").read_bytes())
                    self.assertEqual(state["state"], "failed")
                    self.assertEqual(state["diagnostics"][0]["code"], "write_failed")
                    with self.assertRaises(DeviceRecordingError) as invalid:
                        validate_device_session_directory(partial)
                    self.assertEqual(invalid.exception.code, "manifest_invalid")
                finally:
                    coordinator.close()

    def test_media_loss_and_stale_generation_fail_closed(self) -> None:
        mounted = True
        coordinator = CaptureCoordinator(
            CoordinatorConfig(
                self.mountpoint,
                self.state_root,
                self.session_config,
                minimum_available_bytes=0,
                minimum_available_inodes=0,
            ),
            mount_checker=lambda path: mounted and path == self.mountpoint.resolve(),
        )
        try:
            coordinator.start_capture(start_command("media-start"))
            session_id = self.active_session_id(coordinator)
            with self.assertRaises(DeviceRecordingError) as stale:
                coordinator.submit_frame(frame(), generation_id=str(uuid.uuid4()))
            self.assertEqual(stale.exception.code, "stale_generation")
            self.assertEqual(coordinator.capture_status()["snapshot"]["device_state"], "recording")

            mounted = False
            with self.assertRaises(DeviceRecordingError) as lost:
                coordinator.submit_frame(frame())
            self.assertEqual(lost.exception.code, "media_lost")
            partial = self.mountpoint / "recordings" / f"{session_id}.partial"
            self.assertFalse((partial / "manifest.json").exists())
            state = json.loads((partial / "recording.json").read_bytes())
            self.assertEqual(state["state"], "recoverable")
            self.assertEqual(state["storage"]["status"], "media_lost")
            self.assertFalse(state["storage"]["writable"])
        finally:
            mounted = True
            coordinator.close()

    def test_restart_settles_interrupted_partial_without_promoting_it(self) -> None:
        first = self.coordinator()
        first.start_capture(start_command("restart-start"))
        session_id = self.active_session_id(first)
        recording = first.capture_status()["snapshot"]["active_recording"]["recording_state"]
        first.close()
        partial = self.mountpoint / "recordings" / f"{session_id}.partial"
        recording["state"] = "recording"
        recording["diagnostics"] = []
        (partial / "recording.json").write_text(json.dumps(recording), encoding="utf-8")
        (partial / "manifest.json").write_text("{}", encoding="utf-8")

        restarted = self.coordinator()
        try:
            self.assertFalse((partial / "manifest.json").exists())
            settled = json.loads((partial / "recording.json").read_bytes())
            self.assertEqual(settled["state"], "recoverable")
            self.assertEqual(settled["diagnostics"][0]["code"], "process_interrupted")
            self.assertFalse((self.mountpoint / "recordings" / session_id).exists())
            validate_capture_status(restarted.capture_status())
        finally:
            restarted.close()

    def test_restart_completes_only_the_manifest_durable_rename_window(self) -> None:
        first = self.coordinator()
        try:
            session_id = self.seal_one(first, prefix="rename")
        finally:
            first.close()
        final = self.mountpoint / "recordings" / session_id
        partial = final.with_name(f"{session_id}.partial")
        final.rename(partial)

        restarted = self.coordinator()
        try:
            self.assertTrue(final.is_dir())
            self.assertFalse(partial.exists())
            validate_device_session_directory(final)
        finally:
            restarted.close()

    def test_digest_error_is_not_listed_as_a_success(self) -> None:
        first = self.coordinator()
        try:
            session_id = self.seal_one(first, prefix="digest")
        finally:
            first.close()
        video = self.mountpoint / "recordings" / session_id / "video/raw-sbs.mjpeg"
        video.write_bytes(b"tampered")

        restarted = self.coordinator()
        try:
            listed = restarted.list_sessions(cursor=None, limit=50, take_id=None)
            validate_session_list(listed, limit=50, take_id=None)
            self.assertEqual(listed["items"], [])
            self.assertEqual(listed["diagnostics"][0]["code"], "manifest_invalid")
            with self.assertRaises(ArtifactAccessError):
                restarted.open_verified_artifact(session_id, "0" * 64, "v3")
        finally:
            restarted.close()

    def test_digest_change_before_manifest_publish_becomes_failed_partial(self) -> None:
        tampered = False

        def change_after_digest(role: str, payload: bytes) -> None:
            nonlocal tampered
            if role != "manifest" or tampered:
                return
            tampered = True
            active = next((self.mountpoint / "recordings").glob("*.partial"))
            (active / "video/raw-sbs.mjpeg").write_bytes(b"\xff\xd8tampered-content\xff\xd9")

        coordinator = self.coordinator(before_write=change_after_digest)
        try:
            coordinator.start_capture(start_command("digest-race-start"))
            session_id = self.active_session_id(coordinator)
            coordinator.submit_frame(frame())
            with self.assertRaises(ProviderError) as rejected:
                coordinator.stop_capture(stop_command("digest-race-stop"))
            self.assertEqual(rejected.exception.code, "digest_mismatch")
            partial = self.mountpoint / "recordings" / f"{session_id}.partial"
            self.assertTrue(partial.is_dir())
            self.assertFalse((partial / "manifest.json").exists())
            self.assertFalse((self.mountpoint / "recordings" / session_id).exists())
            with self.assertRaises(DeviceRecordingError) as invalid:
                validate_device_session_directory(partial)
            self.assertEqual(invalid.exception.code, "manifest_invalid")
            state = json.loads((partial / "recording.json").read_bytes())
            self.assertEqual(state["state"], "failed")
            self.assertEqual(state["diagnostics"][0]["code"], "digest_mismatch")
        finally:
            coordinator.close()

    def test_post_publish_readback_failure_never_reopens_final_session(self) -> None:
        real_rename = os.rename

        def corrupt_after_publish(source: object, target: object) -> None:
            real_rename(source, target)
            target_path = Path(target)  # type: ignore[arg-type]
            if target_path.parent == self.mountpoint / "recordings":
                video = target_path / "video/raw-sbs.mjpeg"
                payload = bytearray(video.read_bytes())
                payload[2] ^= 1
                video.write_bytes(payload)

        coordinator = self.coordinator()
        try:
            coordinator.start_capture(start_command("post-publish-start"))
            session_id = self.active_session_id(coordinator)
            coordinator.submit_frame(frame())
            with (
                patch("rp_ylx.recording.device_session.os.rename", corrupt_after_publish),
                self.assertRaises(ProviderError) as rejected,
            ):
                coordinator.stop_capture(stop_command("post-publish-stop"))
            self.assertEqual(rejected.exception.code, "digest_mismatch")
            final = self.mountpoint / "recordings" / session_id
            self.assertTrue(final.is_dir())
            self.assertTrue((final / "manifest.json").is_file())
            self.assertFalse(final.with_name(f"{session_id}.partial").exists())
            with self.assertRaises(DeviceRecordingError) as invalid:
                validate_device_session_directory(final)
            self.assertEqual(invalid.exception.code, "digest_mismatch")
            self.assertIsNotNone(coordinator.retained_unsuccessful_outcome(session_id))
            listed = coordinator.list_sessions(cursor=None, limit=50, take_id=None)
            self.assertEqual(listed["items"], [])
            self.assertEqual(listed["diagnostics"][0]["code"], "manifest_invalid")
        finally:
            coordinator.close()

    def test_safe_swap_waits_for_all_download_handles(self) -> None:
        coordinator = self.coordinator()
        try:
            previous_id = self.seal_one(coordinator, prefix="previous")
            held = coordinator.open_manifest(previous_id, "v3")
            coordinator.start_capture(start_command("swap-start"))
            swap_session = self.active_session_id(coordinator)
            coordinator.submit_frame(frame())
            stop = stop_command("swap-stop", reason="safe_swap")
            with self.assertRaises(ProviderError) as blocked:
                coordinator.stop_capture(stop)
            self.assertEqual(blocked.exception.code, "safe_swap_blocked")
            self.assertEqual(blocked.exception.details["open_handle_count"], 1)
            self.assertIsNone(coordinator.current_safe_swap_receipt())

            held.close()
            result = coordinator.stop_capture(stop)
            self.assertEqual(result.status, 204)
            receipt_resource = coordinator.current_safe_swap_receipt()
            receipt = receipt_resource["receipt"]
            validate_safe_swap_v3_receipt(receipt)
            self.assertEqual(receipt["session_id"], swap_session)
            self.assertEqual(receipt["open_handle_count"], 0)
            self.assertEqual(coordinator.artifact_io_state(), "device-released")
        finally:
            coordinator.close()

    def test_remount_after_safe_swap_invalidates_receipt_and_allows_new_capture(self) -> None:
        mount_identity = "swap-mount-a"
        coordinator = CaptureCoordinator(
            CoordinatorConfig(
                self.mountpoint,
                self.state_root,
                self.session_config,
                minimum_available_bytes=0,
                minimum_available_inodes=0,
            ),
            mount_checker=lambda path: path == self.mountpoint.resolve(),
            mount_identity=lambda path: mount_identity,
        )
        try:
            released_session = self.seal_one(
                coordinator,
                prefix="release",
                reason="safe_swap",
            )
            released_generation = coordinator.generation_id
            receipt_resource = coordinator.current_safe_swap_receipt()
            self.assertIsNotNone(receipt_resource)
            self.assertEqual(
                receipt_resource["receipt"]["session_id"],
                released_session,
            )
            self.assertEqual(coordinator.artifact_io_state(), "device-released")
        finally:
            coordinator.close()

        retained = CaptureCoordinator(
            CoordinatorConfig(
                self.mountpoint,
                self.state_root,
                self.session_config,
                minimum_available_bytes=0,
                minimum_available_inodes=0,
            ),
            mount_checker=lambda path: path == self.mountpoint.resolve(),
            mount_identity=lambda path: mount_identity,
        )
        try:
            self.assertEqual(retained.generation_id, released_generation)
            self.assertIsNotNone(retained.current_safe_swap_receipt())
            with self.assertRaises(ProviderError) as released:
                retained.start_capture(start_command("released-start"))
            self.assertEqual(released.exception.code, "volume_released")
        finally:
            retained.close()

        mount_identity = "swap-mount-b"
        reopened = CaptureCoordinator(
            CoordinatorConfig(
                self.mountpoint,
                self.state_root,
                self.session_config,
                minimum_available_bytes=0,
                minimum_available_inodes=0,
            ),
            mount_checker=lambda path: path == self.mountpoint.resolve(),
            mount_identity=lambda path: mount_identity,
        )
        try:
            self.assertNotEqual(reopened.generation_id, released_generation)
            self.assertIsNone(reopened.current_safe_swap_receipt())
            reopened_session = self.seal_one(reopened, prefix="after-release")
            self.assertIsNone(reopened.current_safe_swap_receipt())
            self.assertIsNone(reopened.artifact_io_state())
            with reopened.open_manifest(reopened_session, "v3") as manifest:
                self.assertGreater(manifest.size, 0)
        finally:
            reopened.close()

    def test_pending_safe_swap_survives_restart_and_requires_zero_handles(self) -> None:
        first = self.coordinator()
        previous_id = self.seal_one(first, prefix="pending-previous")
        held = first.open_manifest(previous_id, "v3")
        first.start_capture(start_command("pending-swap-start"))
        swap_session = self.active_session_id(first)
        first.submit_frame(frame())
        stop = stop_command("pending-swap-stop", reason="safe_swap")
        with self.assertRaises(ProviderError) as blocked:
            first.stop_capture(stop)
        self.assertEqual(blocked.exception.code, "safe_swap_blocked")
        held.close()
        first.close()

        restarted = self.coordinator()
        try:
            self.assertIsNone(restarted.current_safe_swap_receipt())
            completed = restarted.stop_capture(stop_command("resume-safe-swap", reason="safe_swap"))
            self.assertEqual(completed.status, 204)
            receipt = restarted.current_safe_swap_receipt()["receipt"]
            validate_safe_swap_v3_receipt(receipt)
            self.assertEqual(receipt["session_id"], swap_session)
        finally:
            restarted.close()


if __name__ == "__main__":
    unittest.main()
