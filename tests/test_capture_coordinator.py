from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import tempfile
import threading
import time
import unittest
import uuid
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from typing import BinaryIO
from unittest.mock import patch

import rp_ylx.api.downloads as downloads_module
import rp_ylx.recording.coordinator as coordinator_module
from rp_ylx.api import CaptureCommand, NetworkCommand, ProviderError
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
from rp_ylx.camera import CameraError, FrameObservation, StereoFrame
from rp_ylx.imu import ImuObservation, ImuSample, RawVector3
from rp_ylx.network_control import NetworkControlClientError
from rp_ylx.recording import (
    CaptureCoordinator,
    CoordinatorConfig,
    DeviceRecordingError,
    DeviceSessionConfig,
    initialize_capture_volume,
    validate_device_session_directory,
)
from rp_ylx.recording.coordinator import _TrackedRepresentation
from rp_ylx.recording.device_session import inspect_device_session_directory
from rp_ylx.recording.stereo_encoder import ClosedSegment, StereoEncoderError

JPEG = b"\xff\xd8raw-side-by-side\xff\xd9"
COMMIT = "a" * 40
CAMERA_FOCUS_STATUS = {
    "schema": "ylx.camera-focus.v1",
    "value": 42,
    "minimum": 0,
    "maximum": 255,
    "step": 1,
    "default": 32,
    "auto_supported": True,
    "auto_enabled": False,
}


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
    supports_calibration_capture = True

    def __init__(self) -> None:
        self.open_handle_count = 0
        self.mode: str | None = None
        self.generation_id: str | None = None
        self.submit_frame: object | None = None
        self.submit_imu: object | None = None
        self.focus: dict[str, object] | None = None
        self.camera_state = "connected"

    def camera_connection_status(self) -> dict[str, object]:
        return {
            "schema": "ylx.camera-connection.v1",
            "state": self.camera_state,
        }

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

    def camera_focus_status(self) -> dict[str, object] | None:
        return deepcopy(self.focus)

    def set_camera_focus(
        self,
        *,
        value: int | None = None,
        auto_enabled: bool | None = None,
    ) -> dict[str, object]:
        if self.focus is None:
            raise RuntimeError("焦距控制不可用")
        if value is not None:
            self.focus["value"] = value
            if self.focus["auto_supported"] is True:
                self.focus["auto_enabled"] = False
        if auto_enabled is not None:
            self.focus["auto_enabled"] = auto_enabled
        return deepcopy(self.focus)


class FakeSourcesWithLatestImu(FakeSources):
    def __init__(self, observation: ImuObservation) -> None:
        super().__init__()
        self.observation = observation

    def latest_imu_observation(self) -> ImuObservation:
        return self.observation


class FakeSourcesWithSequentialLatestImu(FakeSources):
    def __init__(self, observations: list[ImuObservation]) -> None:
        super().__init__()
        self._observations = observations
        self._index = 0

    def latest_imu_observation(self) -> ImuObservation:
        if self._index < len(self._observations):
            observation = self._observations[self._index]
            self._index += 1
            return observation
        return self._observations[-1]


class FakeSourcesWithScriptedLatestImu(FakeSources):
    def __init__(self, observations: list[ImuObservation | BaseException | None]) -> None:
        super().__init__()
        self._observations = observations
        self._index = 0

    def latest_imu_observation(self) -> ImuObservation | None:
        if self._index < len(self._observations):
            observation = self._observations[self._index]
            self._index += 1
        else:
            observation = self._observations[-1]
        if isinstance(observation, BaseException):
            raise observation
        return observation


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


class FakeNativeSessionStore:
    def __init__(self) -> None:
        self.verify_calls: list[tuple[int, int, str]] = []

    def verify_fd(
        self,
        descriptor: int,
        expected_bytes: int,
        expected_sha256: str,
    ) -> dict[str, object]:
        self.verify_calls.append((descriptor, expected_bytes, expected_sha256))
        payload = os.pread(descriptor, expected_bytes + 1, 0)
        if len(payload) != expected_bytes or hashlib.sha256(payload).hexdigest() != expected_sha256:
            raise ValueError("native fixture verification mismatch")
        return {}


def command(key: str, body: dict[str, object]) -> CaptureCommand:
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    return CaptureCommand("operator", key, body, canonical)


def network_command(key: str, body: dict[str, object]) -> NetworkCommand:
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    return NetworkCommand("operator", key, body, canonical)


def idle_network_control(operation: str, **unused: object) -> dict[str, object]:
    del unused
    if operation == "status":
        return {
            "ok": True,
            "operation": "status",
            "status": 200,
            "body": {"transaction": {"current": None}},
        }
    raise NetworkControlClientError("controller_unavailable", "network controller is unavailable")


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


def single_imu_observation(
    *,
    host_monotonic_ns: int,
    accelerometer: tuple[int, int, int],
    gyroscope: tuple[int, int, int],
    sync_quality: str = "degraded",
) -> ImuObservation:
    sample = ImuSample(
        sequence=host_monotonic_ns,
        packet_sequence=0,
        sample_index=0,
        device_timestamp_raw=host_monotonic_ns,
        device_ticks=host_monotonic_ns,
        host_read_start_ns=host_monotonic_ns,
        host_read_end_ns=host_monotonic_ns,
        host_monotonic_ns=host_monotonic_ns,
        accelerometer=RawVector3(*accelerometer),
        gyroscope=RawVector3(*gyroscope),
        sync_offset_ns=None,
        sync_residual_ns=None,
        sync_quality=sync_quality,
    )
    return ImuObservation((sample,), dropped_samples=0)


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
        self.network_operation_lock_path = self.root / "network-operation.lock"
        self.mountpoint.mkdir()
        self.environment_patcher = patch.dict(
            os.environ,
            {"RP_YLX_NETWORK_OPERATION_LOCK_PATH": str(self.network_operation_lock_path)},
        )
        self.environment_patcher.start()
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
        self.encoder_patcher = patch(
            "rp_ylx.recording.device_session.StereoEncoderProcess",
            FakeSplitEyeEncoder,
        )
        self.encoder_patcher.start()
        self.network_control_patcher = patch(
            "rp_ylx.recording.coordinator.request_network_control",
            side_effect=idle_network_control,
        )
        self.network_control_patcher.start()

    def tearDown(self) -> None:
        self.network_control_patcher.stop()
        self.encoder_patcher.stop()
        self.environment_patcher.stop()
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

    def test_disconnected_camera_is_reported_and_rejected_before_capture_side_effects(
        self,
    ) -> None:
        sources = FakeSources()
        sources.camera_state = "disconnected"
        coordinator = self.coordinator(sources=sources)
        try:
            descriptor = coordinator.device_descriptor("v4", "lab")
            self.assertEqual(
                descriptor["runtime"]["camera"],
                {
                    "schema": "ylx.camera-connection.v1",
                    "state": "disconnected",
                },
            )
            self.assertTrue(descriptor["capabilities"]["capture"])
            self.assertTrue(descriptor["capabilities"]["preview"])
            self.assertEqual(
                descriptor["capabilities"]["calibration_capture"],
                {
                    "supported": True,
                    "enabled": False,
                    "disabled_reason": "hardware_unavailable",
                    "required_video_layout": "split-eyes",
                },
            )
            self.assertEqual(
                coordinator.capture_status()["snapshot"]["runtime"]["camera"],
                descriptor["runtime"]["camera"],
            )

            with self.assertRaises(ProviderError) as rejected:
                coordinator.start_capture(start_command("camera-disconnected"))
            self.assertEqual(rejected.exception.code, "camera_not_connected")
            self.assertEqual(rejected.exception.status, 503)
            self.assertTrue(rejected.exception.retryable)
            self.assertIsNone(sources.mode)
            sessions_root = self.mountpoint / "sessions"
            self.assertTrue(not sessions_root.exists() or not any(sessions_root.iterdir()))

            with self.assertRaises(ProviderError) as preview:
                coordinator.latest_preview(fps=None, accept="image/jpeg")
            self.assertEqual(preview.exception.code, "camera_not_connected")
            self.assertEqual(preview.exception.status, 503)
        finally:
            coordinator.close()

    def test_v4_network_mutation_capability_follows_controller_for_both_profiles(self) -> None:
        coordinator = self.coordinator()
        try:
            with patch.object(coordinator, "_network_controller_available", return_value=True):
                for security_profile in ("lab", "customer"):
                    with self.subTest(api_version="v4", security_profile=security_profile):
                        descriptor = coordinator.device_descriptor("v4", security_profile)
                        self.assertTrue(descriptor["capabilities"]["network_mutation"])
                        self.assertEqual(
                            descriptor["capabilities"]["calibration_capture"],
                            {
                                "supported": True,
                                "enabled": True,
                                "disabled_reason": None,
                                "required_video_layout": "split-eyes",
                            },
                        )
                        validate_device_descriptor(
                            descriptor,
                            api_version="v4",
                            security_profile=security_profile,
                        )

                    with self.subTest(api_version="v3", security_profile=security_profile):
                        descriptor = coordinator.device_descriptor("v3", security_profile)
                        self.assertFalse(descriptor["capabilities"]["network_mutation"])
                        self.assertNotIn("calibration_capture", descriptor["capabilities"])
                        validate_device_descriptor(
                            descriptor,
                            api_version="v3",
                            security_profile=security_profile,
                        )
        finally:
            coordinator.close()

    def active_session_id(self, coordinator: CaptureCoordinator) -> str:
        status = coordinator.capture_status()
        validate_capture_status(status)
        active = status["snapshot"]["active_recording"]
        return active["recording_state"]["session_id"]

    def assert_network_operation_lock_available(self) -> None:
        descriptor = os.open(
            self.network_operation_lock_path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0),
            0o660,
        )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def assert_network_operation_lock_held(self) -> None:
        descriptor = os.open(
            self.network_operation_lock_path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0),
            0o660,
        )
        try:
            with self.assertRaises(OSError) as rejected:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertIn(rejected.exception.errno, {errno.EACCES, errno.EAGAIN})
        finally:
            os.close(descriptor)

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

    def test_capture_start_fails_closed_when_network_state_cannot_be_proven_idle(self) -> None:
        failures: tuple[object, ...] = (
            NetworkControlClientError("controller_unavailable", "offline"),
            {"ok": False, "error": {"code": "controller_failure"}, "retryable": True},
            {"ok": True, "operation": "status", "status": 200, "body": {}},
        )
        for index, failure in enumerate(failures):
            with self.subTest(failure=failure):
                coordinator = self.coordinator()
                try:
                    effect = failure if isinstance(failure, BaseException) else None
                    result = failure if isinstance(failure, dict) else None
                    with (
                        patch(
                            "rp_ylx.recording.coordinator.request_network_control",
                            side_effect=effect,
                            return_value=result,
                        ),
                        self.assertRaises(ProviderError) as rejected,
                    ):
                        coordinator.start_capture(start_command(f"network-unknown-{index}"))
                    self.assertEqual(rejected.exception.code, "network_state_unavailable")
                    self.assertEqual(rejected.exception.status, 500)
                    self.assertTrue(rejected.exception.retryable)
                    self.assertEqual(
                        coordinator.capture_status()["snapshot"]["device_state"],
                        "idle",
                    )
                    self.assert_network_operation_lock_available()
                finally:
                    coordinator.close()

    def test_capture_holds_network_operation_lease_from_idle_check_through_stop(self) -> None:
        coordinator = self.coordinator()
        status_checked = False

        def idle_status_while_locked(operation: str, **unused: object) -> dict[str, object]:
            nonlocal status_checked
            del unused
            self.assertEqual(operation, "status")
            self.assert_network_operation_lock_held()
            status_checked = True
            return idle_network_control(operation)

        try:
            with patch(
                "rp_ylx.recording.coordinator.request_network_control",
                side_effect=idle_status_while_locked,
            ):
                coordinator.start_capture(start_command("lease-lifecycle"))
            self.assertTrue(status_checked)
            self.assert_network_operation_lock_held()
            self.assertTrue(coordinator.submit_frame(frame()))
            coordinator.stop_capture(stop_command("lease-lifecycle-stop"))
            self.assert_network_operation_lock_available()
        finally:
            coordinator.close()

    def test_capture_start_rejects_network_operation_lock_contention_without_querying_root(
        self,
    ) -> None:
        descriptor = os.open(
            self.network_operation_lock_path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0),
            0o660,
        )
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        coordinator = self.coordinator()
        try:
            with (
                patch(
                    "rp_ylx.recording.coordinator.request_network_control",
                    side_effect=AssertionError("root status must not be queried without the lease"),
                ),
                self.assertRaises(ProviderError) as rejected,
            ):
                coordinator.start_capture(start_command("lease-contended"))
            self.assertEqual(rejected.exception.code, "network_mutation_active")
            self.assertEqual(rejected.exception.status, 409)
            self.assertTrue(rejected.exception.retryable)
            self.assertEqual(coordinator.capture_status()["snapshot"]["device_state"], "idle")
        finally:
            coordinator.close()
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def test_capture_start_rejects_an_active_network_transaction(self) -> None:
        coordinator = self.coordinator()
        try:
            with (
                patch(
                    "rp_ylx.recording.coordinator.request_network_control",
                    return_value={
                        "ok": True,
                        "operation": "status",
                        "status": 200,
                        "body": {"transaction": {"current": {"transaction_id": "active"}}},
                    },
                ),
                self.assertRaises(ProviderError) as rejected,
            ):
                coordinator.start_capture(start_command("network-active"))
            self.assertEqual(rejected.exception.code, "network_mutation_active")
            self.assertEqual(rejected.exception.status, 409)
            self.assertTrue(rejected.exception.retryable)
            self.assert_network_operation_lock_available()
        finally:
            coordinator.close()

    def test_network_controller_errors_are_mapped_to_openapi_typed_errors(self) -> None:
        transaction_id = "0198d2a0-41a0-7b7a-a751-0e86a39d4db1"
        apply_body = {
            "schema": "ylx.network-apply-request.v1",
            "desired": {
                "mode": "wifi-client",
                "wifi_client": {
                    "ssid": "studio-wifi",
                    "security": "wpa2-personal",
                    "credential_ref": "cred-test",
                },
                "ethernet": None,
            },
        }
        retry_body = {
            "schema": "ylx.network-retry-request.v1",
            "transaction_id": transaction_id,
        }
        cases = (
            (
                "apply",
                apply_body,
                "idempotency_conflict",
                409,
                "idempotency_conflict",
                False,
                {
                    "idempotency_scope": "network-mutation",
                    "original_transaction_id": None,
                },
            ),
            (
                "retry",
                retry_body,
                "transaction_not_found",
                404,
                "network_transaction_not_found",
                False,
                {"transaction_id": transaction_id},
            ),
            (
                "apply",
                apply_body,
                "credential_ref_expired",
                422,
                "invalid_network_desired_state",
                False,
                {
                    "field": "desired.wifi_client.credential_ref",
                    "reason": "credential_ref_expired",
                },
            ),
            (
                "apply",
                apply_body,
                "inline_secret_rejected",
                422,
                "invalid_network_desired_state",
                False,
                {"field": "desired", "reason": "unsafe_secret_field"},
            ),
            (
                "apply",
                apply_body,
                "controller_busy",
                503,
                "network_mutation_unavailable",
                True,
                {"reason": "recovery_required"},
            ),
            (
                "apply",
                apply_body,
                "network_manager_unavailable",
                503,
                "network_mutation_unavailable",
                True,
                {"reason": "network_manager_unavailable"},
            ),
            (
                "apply",
                apply_body,
                "capture_active",
                503,
                "network_mutation_unavailable",
                True,
                {"reason": "capture_active"},
            ),
        )
        coordinator = self.coordinator()
        try:
            for index, (
                operation,
                body,
                controller_code,
                expected_status,
                expected_code,
                expected_retryable,
                expected_details,
            ) in enumerate(cases):
                with self.subTest(controller_code=controller_code):
                    with (
                        patch(
                            "rp_ylx.recording.coordinator.request_network_control",
                            return_value={
                                "ok": False,
                                "operation": operation,
                                "error": {"code": controller_code},
                                "retryable": controller_code
                                in {
                                    "capture_active",
                                    "controller_busy",
                                    "network_manager_unavailable",
                                },
                            },
                        ),
                        self.assertRaises(ProviderError) as rejected,
                    ):
                        command_value = network_command(f"network-error-{index}", body)
                        if operation == "retry":
                            coordinator.retry_network_transaction(command_value)
                        else:
                            coordinator.apply_network_desired_state(command_value)
                    self.assertEqual(rejected.exception.status, expected_status)
                    self.assertEqual(rejected.exception.code, expected_code)
                    self.assertEqual(rejected.exception.retryable, expected_retryable)
                    self.assertEqual(rejected.exception.details, expected_details)
        finally:
            coordinator.close()

    def test_active_capture_blocks_network_mutation_with_typed_unavailable_error(self) -> None:
        coordinator = self.coordinator()
        try:
            coordinator.start_capture(start_command("capture-before-network"))
            body = {
                "schema": "ylx.network-forget-request.v1",
            }
            with (
                patch(
                    "rp_ylx.recording.coordinator.request_network_control",
                    side_effect=AssertionError("network controller must not be called"),
                ),
                self.assertRaises(ProviderError) as rejected,
            ):
                coordinator.forget_network_client_profile(network_command("blocked-network", body))
            self.assertEqual(rejected.exception.status, 503)
            self.assertEqual(rejected.exception.code, "network_mutation_unavailable")
            self.assertTrue(rejected.exception.retryable)
            self.assertEqual(rejected.exception.details, {"reason": "capture_active"})
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

    def test_normal_v2_seal_is_manifest_last_and_gateway_readable(self) -> None:
        coordinator = self.coordinator()
        try:
            session_id = self.seal_one(coordinator)
            session = self.mountpoint / "recordings" / session_id
            manifest = validate_device_session_directory(session)
            self.assertEqual(manifest["schema"], "ylx.device-session.v2")
            self.assertEqual(manifest["imu"]["coordinate_frame"], "raw_device_axes")
            self.assertEqual(manifest["audio"]["state"], "not_recorded")
            self.assertEqual(manifest["video"]["layout"], "split-eyes")
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
            video = manifest["video"]["segments"][0]["artifacts"]["left"]
            artifact = coordinator.open_verified_artifact(session_id, video["artifact_id"], "v3")
            try:
                self.assertEqual(artifact.read(), (session / video["path"]).read_bytes())
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

            video = manifest["video"]["segments"][0]["artifacts"]["left"]
            artifact = restarted.open_verified_artifact(session_id, video["artifact_id"], "v3")
            try:
                self.assertEqual(artifact.read(), (legacy / video["path"]).read_bytes())
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

    def test_v4_catalog_cursor_pages_items_and_quarantine_as_one_snapshot(self) -> None:
        coordinator = self.coordinator()
        try:
            source_session_id = self.seal_one(coordinator, prefix="catalog-v3-source")
            source = deepcopy(coordinator._session_summaries[source_session_id])
            take_id = "01989f69-f000-7c3d-ae4f-5061728394a5"
            summaries: dict[str, dict[str, object]] = {}
            for index in range(203):
                session_id = f"01989f{index:02x}-0000-7000-8000-{index:012x}"
                item = deepcopy(source)
                item["session_id"] = session_id
                item["take_id"] = take_id
                item["started_at"] = f"2026-08-08T02:{index // 60:02d}:{index % 60:02d}Z"
                item["ended_at"] = item["started_at"]
                item["verification"]["manifest_sha256"] = f"{index + 1:064x}"
                summaries[session_id] = item
            diagnostics = {
                "bad-a": {
                    "quarantine_id": "550e8400-e29b-41d4-a716-446655440000",
                    "code": "manifest_unreadable",
                    "observed_at": "2026-08-08T02:30:00Z",
                    "message": "会话清单无法安全读取",
                },
                "bad-b": {
                    "quarantine_id": "550e8400-e29b-41d4-a716-446655440001",
                    "code": "manifest_invalid",
                    "observed_at": "2026-08-08T02:30:01Z",
                    "message": "会话清单或 artifact 描述不符合 Device Session 合同",
                },
            }
            with coordinator._catalog_lock:
                coordinator._session_summaries = summaries
                coordinator._session_diagnostics = diagnostics

            with patch.object(coordinator, "_catalog_sessions", return_value=None):
                for limit in (1, 50, 51, 200):
                    with self.subTest(limit=limit):
                        cursor = None
                        catalog_revision = None
                        identities: list[str] = []
                        while True:
                            page = coordinator.list_sessions(
                                cursor=cursor,
                                limit=limit,
                                take_id=None,
                                api_version="v4",
                            )
                            validate_session_list(
                                page,
                                limit=limit,
                                take_id=None,
                                api_version="v4",
                            )
                            catalog_revision = catalog_revision or page["catalog_revision"]
                            self.assertEqual(page["catalog_revision"], catalog_revision)
                            identities.extend(
                                f"session:{item['session_id']}" for item in page["items"]
                            )
                            identities.extend(
                                f"diagnostic:{item['quarantine_id']}"
                                for item in page["diagnostics"]
                            )
                            cursor = page["next_cursor"]
                            if cursor is None:
                                break
                            self.assertNotIn(cursor, summaries)
                        self.assertEqual(len(identities), 205)
                        self.assertEqual(len(set(identities)), 205)

                first = coordinator.list_sessions(
                    cursor=None,
                    limit=1,
                    take_id=None,
                    api_version="v4",
                )
                cursor = first["next_cursor"]
                self.assertIsInstance(cursor, str)
                changed = next(iter(summaries.values()))
                changed["verification"]["manifest_sha256"] = "f" * 64
                with self.assertRaises(ProviderError) as rejected:
                    coordinator.list_sessions(
                        cursor=cursor,
                        limit=1,
                        take_id=None,
                        api_version="v4",
                    )
                self.assertEqual(rejected.exception.code, "catalog_changed")
                self.assertEqual(rejected.exception.status, 409)
                self.assertTrue(rejected.exception.retryable)

                with self.assertRaises(ProviderError) as wrong_filter:
                    coordinator.list_sessions(
                        cursor=first["next_cursor"],
                        limit=1,
                        take_id=take_id,
                        api_version="v4",
                    )
                self.assertEqual(wrong_filter.exception.code, "invalid_cursor")
        finally:
            coordinator.close()

    def test_v4_quarantine_only_catalog_is_fully_pageable(self) -> None:
        coordinator = self.coordinator()
        try:
            diagnostics = {
                f"bad-{index}": {
                    "quarantine_id": f"550e8400-e29b-41d4-a716-{index:012x}",
                    "code": "manifest_invalid",
                    "observed_at": f"2026-08-08T02:30:0{index}Z",
                    "message": "会话清单或 artifact 描述不符合 Device Session 合同",
                }
                for index in range(3)
            }
            with coordinator._catalog_lock:
                coordinator._session_summaries = {}
                coordinator._session_diagnostics = diagnostics
            with patch.object(coordinator, "_catalog_sessions", return_value=None):
                cursor = None
                seen: list[str] = []
                revisions: set[str] = set()
                while True:
                    page = coordinator.list_sessions(
                        cursor=cursor,
                        limit=1,
                        take_id=None,
                        api_version="v4",
                    )
                    revisions.add(page["catalog_revision"])
                    self.assertEqual(page["items"], [])
                    self.assertEqual(len(page["diagnostics"]), 1)
                    seen.append(page["diagnostics"][0]["quarantine_id"])
                    cursor = page["next_cursor"]
                    if cursor is None:
                        break
                self.assertEqual(len(seen), 3)
                self.assertEqual(len(set(seen)), 3)
                self.assertEqual(len(revisions), 1)
        finally:
            coordinator.close()

    def test_v4_quarantine_diagnostics_are_stable_categorized_and_safe(self) -> None:
        cases = (
            (OSError("cannot read /secret/device/manifest.json"), "manifest_unreadable"),
            (
                DeviceRecordingError(
                    "manifest_invalid",
                    "manifest schema unsupported at /secret/device/manifest.json",
                ),
                "unsupported_schema",
            ),
            (
                DeviceRecordingError(
                    "manifest_invalid",
                    "manifest sealed 必须为 true at /secret/device/manifest.json",
                ),
                "manifest_not_sealed",
            ),
            (
                DeviceRecordingError(
                    "manifest_invalid",
                    "manifest session identity leaked from /secret/device/manifest.json",
                ),
                "manifest_invalid",
            ),
        )
        expected_codes = {
            "manifest_unreadable",
            "unsupported_schema",
            "manifest_not_sealed",
            "manifest_invalid",
        }

        diagnostics = [
            CaptureCoordinator._session_diagnostic("opaque-candidate", error) for error, _ in cases
        ]
        self.assertEqual(
            {diagnostic["code"] for diagnostic in diagnostics},
            expected_codes,
        )
        for diagnostic, (_, expected_code) in zip(diagnostics, cases, strict=True):
            self.assertEqual(diagnostic["code"], expected_code)
            self.assertNotIn("/secret", diagnostic["message"])
        repeated = [
            CaptureCoordinator._session_diagnostic("opaque-candidate", error) for error, _ in cases
        ]
        self.assertEqual(
            [diagnostic["quarantine_id"] for diagnostic in diagnostics],
            [diagnostic["quarantine_id"] for diagnostic in repeated],
        )

    def test_v4_real_unknown_and_unsealed_manifests_are_separate_quarantine_codes(
        self,
    ) -> None:
        first = self.coordinator()
        try:
            unsealed_session_id = self.seal_one(first, prefix="quarantine-unsealed")
        finally:
            first.close()

        manifest_path = self.mountpoint / "recordings" / unsealed_session_id / "manifest.json"
        manifest = json.loads(manifest_path.read_bytes())
        manifest["sealed"] = False
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        unknown_session_id = "01989f69-f000-7c3d-ae4f-5061728394a5"
        unknown_directory = self.mountpoint / "recordings" / unknown_session_id
        unknown_directory.mkdir()
        (unknown_directory / "manifest.json").write_text(
            json.dumps(
                {
                    "schema": "ylx.device-session.v999",
                    "session_id": unknown_session_id,
                },
                separators=(",", ":"),
            ),
            encoding="utf-8",
        )

        restarted = self.coordinator()
        try:
            page = restarted.list_sessions(
                cursor=None,
                limit=50,
                take_id=None,
                api_version="v4",
            )
            validate_session_list(page, limit=50, take_id=None, api_version="v4")
            self.assertEqual(page["items"], [])
            self.assertEqual(
                {diagnostic["code"] for diagnostic in page["diagnostics"]},
                {"manifest_not_sealed", "unsupported_schema"},
            )
        finally:
            restarted.close()

    def test_list_sessions_builds_catalog_once_instead_of_reinspecting_artifacts(self) -> None:
        first = self.coordinator()
        try:
            session_id = self.seal_one(first, prefix="catalog-cache")
        finally:
            first.close()

        with patch(
            "rp_ylx.recording.coordinator.inspect_device_session_directory",
            side_effect=inspect_device_session_directory,
        ) as inspect_session:
            restarted = self.coordinator()
            try:
                self.assertEqual(inspect_session.call_count, 0)

                listed = restarted.list_sessions(cursor=None, limit=50, take_id=None)
                first_list_inspections = inspect_session.call_count
                self.assertGreater(first_list_inspections, 0)
                self.assertEqual(
                    [item["session_id"] for item in listed["items"]],
                    [session_id],
                )

                restarted.list_sessions(cursor=None, limit=50, take_id=None)
                self.assertEqual(inspect_session.call_count, first_list_inspections)
            finally:
                restarted.close()

    def test_cached_catalog_page_does_not_restat_every_verified_artifact(self) -> None:
        coordinator = self.coordinator()
        try:
            session_id = self.seal_one(coordinator, prefix="catalog-page-cache")
            first = coordinator.list_sessions(cursor=None, limit=50, take_id=None)
            self.assertEqual([item["session_id"] for item in first["items"]], [session_id])

            with patch.object(
                coordinator_module._MultiRootSessionStore,
                "snapshot_verified_identities",
                side_effect=AssertionError("cached list page must not rescan artifacts"),
            ):
                second = coordinator.list_sessions(cursor=None, limit=50, take_id=None)

            self.assertEqual(second, first)
        finally:
            coordinator.close()

    def test_restart_defers_catalog_content_verification_until_first_list(self) -> None:
        first = self.coordinator()
        try:
            session_id = self.seal_one(first, prefix="deferred-catalog")
        finally:
            first.close()

        with patch(
            "rp_ylx.recording.coordinator.validate_device_session_directory",
            wraps=validate_device_session_directory,
        ) as verify:
            restarted = self.coordinator()
            try:
                self.assertEqual(verify.call_count, 0)

                listed = restarted.list_sessions(cursor=None, limit=50, take_id=None)
                first_list_verifications = verify.call_count
                self.assertGreater(first_list_verifications, 0)
                self.assertEqual(
                    [item["session_id"] for item in listed["items"]],
                    [session_id],
                )

                restarted.list_sessions(cursor=None, limit=50, take_id=None)
                self.assertEqual(verify.call_count, first_list_verifications)
            finally:
                restarted.close()

    def test_cached_catalog_reads_each_manifest_once_for_all_artifact_identities(self) -> None:
        coordinator = self.coordinator()
        try:
            session_id = self.seal_one(coordinator, prefix="catalog-identity-cache")
            manifest = json.loads(
                (self.mountpoint / "recordings" / session_id / "manifest.json").read_bytes()
            )
            self.assertGreater(len(list(iter_device_session_v1_artifacts(manifest))), 1)

            with patch(
                "rp_ylx.api.downloads._read_exact_file",
                wraps=downloads_module._read_exact_file,
            ) as read_manifest:
                listed = coordinator.list_sessions(cursor=None, limit=50, take_id=None)

            self.assertEqual([item["session_id"] for item in listed["items"]], [session_id])
            self.assertEqual(read_manifest.call_count, 1)
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

    def test_capture_status_refreshes_native_source_imu_for_same_session_revision(self) -> None:
        sources = FakeSourcesWithSequentialLatestImu(
            [
                single_imu_observation(
                    host_monotonic_ns=100,
                    accelerometer=(1, 2, 3),
                    gyroscope=(4, 5, 6),
                ),
                single_imu_observation(
                    host_monotonic_ns=200,
                    accelerometer=(7, 8, 9),
                    gyroscope=(10, 11, 12),
                ),
                single_imu_observation(
                    host_monotonic_ns=300,
                    accelerometer=(13, 14, 15),
                    gyroscope=(16, 17, 18),
                ),
            ]
        )
        coordinator = self.coordinator(sources=sources)
        try:
            started = coordinator.start_capture(start_command("sources-fresh-imu"))

            first = started.body
            validate_capture_status(first)
            second = coordinator.capture_status()
            validate_capture_status(second)
            third = coordinator.capture_status()
            validate_capture_status(third)

            self.assertEqual(second["source_revision"], first["source_revision"])
            self.assertEqual(third["source_revision"], first["source_revision"])
            self.assertEqual(
                first["snapshot"]["runtime"]["live_imu"]["clock"]["timestamp_ns"],
                100,
            )
            self.assertEqual(
                second["snapshot"]["runtime"]["live_imu"]["clock"]["timestamp_ns"],
                200,
            )
            self.assertEqual(
                third["snapshot"]["runtime"]["live_imu"]["clock"]["timestamp_ns"],
                300,
            )
        finally:
            coordinator.close()

    def test_live_imu_source_miss_reuses_cache_only_within_bounded_freshness(self) -> None:
        observation = single_imu_observation(
            host_monotonic_ns=100,
            accelerometer=(1, 2, 3),
            gyroscope=(4, 5, 6),
        )
        sources = FakeSourcesWithScriptedLatestImu(
            [
                observation,
                None,
                RuntimeError("temporary native IMU failure"),
                None,
            ]
        )
        clock = {"now": 1.0}

        def monotonic() -> float:
            return clock["now"]

        coordinator = self.coordinator(sources=sources)
        try:
            with patch("rp_ylx.recording.coordinator.time.monotonic", side_effect=monotonic):
                coordinator.start_capture(start_command("sources-bounded-imu"))

                clock["now"] = 1.2
                short_miss = coordinator.capture_status()
                validate_capture_status(short_miss)
                self.assertEqual(
                    short_miss["snapshot"]["runtime"]["live_imu"]["clock"]["timestamp_ns"],
                    100,
                )

                clock["now"] = 1.4
                short_error = coordinator.capture_status()
                validate_capture_status(short_error)
                self.assertEqual(
                    short_error["snapshot"]["runtime"]["live_imu"]["clock"]["timestamp_ns"],
                    100,
                )

                clock["now"] = 11.0
                stale = coordinator.capture_status()
                validate_capture_status(stale)
                self.assertIsNone(stale["snapshot"]["runtime"]["live_imu"])
        finally:
            coordinator.close()

    def test_repeated_live_imu_sample_does_not_refresh_cache_age(self) -> None:
        observation = single_imu_observation(
            host_monotonic_ns=100,
            accelerometer=(1, 2, 3),
            gyroscope=(4, 5, 6),
        )
        sources = FakeSourcesWithScriptedLatestImu([observation, observation, observation])
        clock = {"now": 20.0}

        def monotonic() -> float:
            return clock["now"]

        coordinator = self.coordinator(sources=sources)
        try:
            with patch("rp_ylx.recording.coordinator.time.monotonic", side_effect=monotonic):
                coordinator.start_capture(start_command("sources-repeated-imu"))

                clock["now"] = 20.5
                repeated = coordinator.capture_status()
                validate_capture_status(repeated)
                self.assertEqual(
                    repeated["snapshot"]["runtime"]["live_imu"]["clock"]["timestamp_ns"],
                    100,
                )

                clock["now"] = 30.0
                stale = coordinator.capture_status()
                validate_capture_status(stale)
                self.assertIsNone(stale["snapshot"]["runtime"]["live_imu"])
        finally:
            coordinator.close()

    def test_live_imu_cache_is_cleared_between_sessions(self) -> None:
        first_observation = single_imu_observation(
            host_monotonic_ns=100,
            accelerometer=(1, 2, 3),
            gyroscope=(4, 5, 6),
        )
        sources = FakeSourcesWithScriptedLatestImu([first_observation, None])
        coordinator = self.coordinator(sources=sources)
        try:
            first = coordinator.start_capture(start_command("sources-session-one"))
            validate_capture_status(first.body)
            self.assertEqual(
                first.body["snapshot"]["runtime"]["live_imu"]["clock"]["timestamp_ns"],
                100,
            )
            self.assertTrue(coordinator.submit_frame(frame()))
            coordinator.stop_capture(stop_command("sources-session-one-stop"))

            second = coordinator.start_capture(start_command("sources-session-two"))
            validate_capture_status(second.body)
            self.assertIsNone(second.body["snapshot"]["runtime"]["live_imu"])
        finally:
            coordinator.close()

    def test_v4_device_descriptor_does_not_leak_live_imu_after_stop_or_source_miss(
        self,
    ) -> None:
        first_observation = single_imu_observation(
            host_monotonic_ns=100,
            accelerometer=(1, 2, 3),
            gyroscope=(4, 5, 6),
        )
        sources = FakeSourcesWithScriptedLatestImu([first_observation, None])
        coordinator = self.coordinator(sources=sources)
        try:
            first = coordinator.start_capture(start_command("descriptor-session-one"))
            validate_capture_status(first.body)
            self.assertEqual(
                first.body["snapshot"]["runtime"]["live_imu"]["clock"]["timestamp_ns"],
                100,
            )
            self.assertTrue(coordinator.submit_frame(frame()))
            coordinator.stop_capture(stop_command("descriptor-session-one-stop"))

            idle_descriptor = coordinator.device_descriptor("v4", "customer")
            validate_device_descriptor(
                idle_descriptor,
                api_version="v4",
                security_profile="customer",
            )
            self.assertIsNone(idle_descriptor["runtime"]["live_imu"])

            second = coordinator.start_capture(start_command("descriptor-session-two"))
            validate_capture_status(second.body)
            self.assertIsNone(second.body["snapshot"]["runtime"]["live_imu"])

            active_descriptor = coordinator.device_descriptor("v4", "customer")
            validate_device_descriptor(
                active_descriptor,
                api_version="v4",
                security_profile="customer",
            )
            self.assertIsNone(active_descriptor["runtime"]["live_imu"])
        finally:
            coordinator.close()

    def test_camera_focus_status_and_set_are_reflected_in_runtime_snapshot(self) -> None:
        sources = FakeSources()
        sources.focus = deepcopy(CAMERA_FOCUS_STATUS)
        coordinator = self.coordinator(sources=sources)
        try:
            self.assertEqual(coordinator.camera_focus_status(), CAMERA_FOCUS_STATUS)
            before = coordinator.capture_status()["source_revision"]

            focus_command = command(
                "focus-set-77",
                {
                    "schema": "ylx.camera-focus-set.v1",
                    "value": 77,
                    "auto_enabled": False,
                },
            )
            result = coordinator.set_camera_focus(focus_command)
            self.assertEqual(result.status, 200)
            self.assertEqual(result.body["value"], 77)
            self.assertFalse(result.replayed)

            status = coordinator.capture_status()
            validate_capture_status(status)
            self.assertGreater(status["source_revision"], before)
            self.assertEqual(status["snapshot"]["runtime"]["camera_focus"]["value"], 77)

            replayed = coordinator.set_camera_focus(focus_command)
            self.assertEqual(replayed.status, 200)
            self.assertTrue(replayed.replayed)
            self.assertEqual(replayed.body["value"], 77)

            with self.assertRaises(ProviderError) as invalid:
                coordinator.set_camera_focus(
                    command(
                        "focus-invalid",
                        {
                            "schema": "ylx.camera-focus-set.v1",
                            "auto_enabled": "false",
                        },
                    )
                )
            self.assertEqual(invalid.exception.code, "invalid_camera_focus")
            self.assertEqual(invalid.exception.status, 400)
        finally:
            coordinator.close()

    def test_camera_focus_capability_errors_match_the_v4_contract(self) -> None:
        sources = FakeSources()
        sources.focus = deepcopy(CAMERA_FOCUS_STATUS)
        coordinator = self.coordinator(sources=sources)
        try:
            cases = (
                ("camera_focus_unsupported", 404),
                ("camera_focus_auto_unsupported", 422),
            )
            for index, (code, expected_status) in enumerate(cases):
                with self.subTest(code=code):
                    with (
                        patch.object(
                            sources,
                            "set_camera_focus",
                            side_effect=CameraError(code, "unsupported"),
                        ),
                        self.assertRaises(ProviderError) as rejected,
                    ):
                        coordinator.set_camera_focus(
                            command(
                                f"focus-capability-{index}",
                                {
                                    "schema": "ylx.camera-focus-set.v1",
                                    "auto_enabled": True,
                                },
                            )
                        )
                    self.assertEqual(rejected.exception.code, code)
                    self.assertEqual(rejected.exception.status, expected_status)
                    self.assertFalse(rejected.exception.retryable)
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

    def test_later_success_hides_stale_retained_failure_without_revision_regression(
        self,
    ) -> None:
        coordinator = self.coordinator()
        try:
            coordinator.start_capture(start_command("failed-before-success"))
            failed_session_id = self.active_session_id(coordinator)
            with self.assertRaises(DeviceRecordingError):
                coordinator.submit_frame(frame(3, dropped_before=2))
            failed_status = coordinator.capture_status()
            failed_revision = failed_status["source_revision"]
            self.assertIsNotNone(failed_status["snapshot"]["retained_unsuccessful"])

            self.seal_one(coordinator, prefix="success-after-failure")
            status = coordinator.capture_status()

            validate_capture_status(status)
            self.assertGreater(status["source_revision"], failed_revision)
            self.assertIsNone(status["snapshot"]["retained_unsuccessful"])
            self.assertIsNotNone(coordinator.retained_unsuccessful_outcome(failed_session_id))
        finally:
            coordinator.close()

    def test_writer_drop_exceeding_lossless_policy_never_seals(self) -> None:
        writer_blocked = threading.Event()
        release_writer = threading.Event()

        def slow_writer(role: str, payload: bytes) -> None:
            if role == "frames.index" and payload:
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

    def test_calibration_active_blocks_production_recording(self) -> None:
        coordinator = self.coordinator()
        try:
            calibration = start_command("calibration-start", mode="calibration")
            first = coordinator.start_capture(calibration)
            replay = coordinator.start_capture(calibration)
            self.assertEqual(first.body, replay.body)
            self.assertTrue(replay.replayed)

            changed = start_command("calibration-start", mode="production")
            with self.assertRaises(ProviderError) as conflict:
                coordinator.start_capture(changed)
            self.assertEqual(conflict.exception.code, "idempotency_conflict")

            with self.assertRaises(ProviderError) as busy:
                coordinator.start_capture(start_command("production-start", mode="production"))
            self.assertEqual(busy.exception.code, "capture_busy")
            coordinator.submit_frame(frame())
            coordinator.stop_capture(stop_command("calibration-stop"))
        finally:
            coordinator.close()

    def test_unsupported_calibration_is_rejected_before_capture_side_effects(self) -> None:
        sources = FakeSources()
        sources.supports_calibration_capture = False
        coordinator = self.coordinator(sources=sources)
        try:
            descriptor = coordinator.device_descriptor("v4", "lab")
            self.assertEqual(
                descriptor["capabilities"]["calibration_capture"],
                {
                    "supported": False,
                    "enabled": False,
                    "disabled_reason": "capture_source_unsupported",
                    "required_video_layout": "split-eyes",
                },
            )
            with self.assertRaises(ProviderError) as rejected:
                coordinator.start_capture(start_command("unsupported-cal", mode="calibration"))
            self.assertEqual(rejected.exception.code, "calibration_unavailable")
            self.assertEqual(rejected.exception.status, 503)
            self.assertFalse(rejected.exception.retryable)
            self.assertEqual(
                rejected.exception.details,
                {"reason": "capture_source_unsupported"},
            )
            self.assertIsNone(sources.mode)
            self.assert_network_operation_lock_available()
            sessions_root = self.mountpoint / "recordings"
            self.assertTrue(not sessions_root.exists() or not any(sessions_root.iterdir()))
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

    def test_seal_independently_verifies_artifact_bytes_before_usable_verdict(self) -> None:
        coordinator = self.coordinator()
        try:
            coordinator.start_capture(start_command("incremental-digest-start"))
            coordinator.submit_frame(frame())
            with patch(
                "rp_ylx.recording.coordinator.validate_device_session_directory",
                wraps=validate_device_session_directory,
            ) as verify:
                result = coordinator.stop_capture(stop_command("incremental-digest-stop"))
            self.assertEqual(result.status, 202)
            self.assertEqual(verify.call_count, 1)
            listed = coordinator.list_sessions(
                cursor=None,
                limit=50,
                take_id=None,
                api_version="v4",
            )
            self.assertEqual(listed["items"][0]["verification"]["verdict"], "usable")
            session_id = listed["items"][0]["session_id"]
            manifest = json.loads(
                (self.mountpoint / "recordings" / session_id / "manifest.json").read_bytes()
            )
            verification = listed["items"][0]["verification"]
            self.assertEqual(
                verification["validator"]["name"],
                "openaria-conductor-device-session-v2-integrity",
            )
            self.assertNotEqual(
                verification["verified_at"],
                manifest["integrity"]["verified_at"],
            )
        finally:
            coordinator.close()

    def test_catalog_verifies_artifact_contents_on_first_list_then_reuses_verdict(self) -> None:
        first = self.coordinator()
        try:
            session_id = self.seal_one(first, prefix="lightweight-catalog")
        finally:
            first.close()

        with patch(
            "rp_ylx.recording.coordinator.validate_device_session_directory",
            wraps=validate_device_session_directory,
        ) as verify:
            restarted = self.coordinator()
            try:
                self.assertEqual(verify.call_count, 0)
                listed = restarted.list_sessions(cursor=None, limit=50, take_id=None)
                first_list_verifications = verify.call_count
                self.assertGreater(first_list_verifications, 0)
                restarted.list_sessions(cursor=None, limit=50, take_id=None)
                self.assertEqual(verify.call_count, first_list_verifications)
            finally:
                restarted.close()
        self.assertEqual([item["session_id"] for item in listed["items"]], [session_id])
        manifest = json.loads(
            (self.mountpoint / "recordings" / session_id / "manifest.json").read_bytes()
        )
        expected_bytes = sum(
            int(artifact["bytes"]) for artifact in iter_device_session_v1_artifacts(manifest)
        )
        self.assertEqual(listed["items"][0]["total_bytes"], expected_bytes)

    def test_catalog_marks_content_digest_mismatch_unusable_before_download(self) -> None:
        first = self.coordinator()
        try:
            session_id = self.seal_one(first, prefix="gateway-integrity")
        finally:
            first.close()

        session = self.mountpoint / "recordings" / session_id
        manifest = json.loads((session / "manifest.json").read_bytes())
        video = manifest["video"]["segments"][0]["artifacts"]["left"]
        artifact_path = session / video["path"]
        payload = bytearray(artifact_path.read_bytes())
        payload[0] ^= 0x01
        artifact_path.write_bytes(payload)

        restarted = self.coordinator()
        try:
            listed = restarted.list_sessions(
                cursor=None,
                limit=50,
                take_id=None,
                api_version="v4",
            )
            verification = listed["items"][0]["verification"]
            self.assertEqual(verification["verdict"], "unusable")
            self.assertEqual(
                verification["diagnostics"],
                [
                    {
                        "code": "artifact_digest_mismatch",
                        "summary": "artifact 内容 SHA-256 与清单声明不一致",
                    }
                ],
            )
            with self.assertRaises(ArtifactAccessError) as blocked:
                restarted.open_verified_artifact(
                    session_id,
                    video["artifact_id"],
                    "v4",
                )
            self.assertEqual(blocked.exception.code, "not_verified")
        finally:
            restarted.close()

    def test_cached_usable_same_size_tamper_is_rejected_before_direct_download(self) -> None:
        coordinator = self.coordinator()
        try:
            session_id = self.seal_one(coordinator, prefix="cached-direct-download")
            before = coordinator.list_sessions(
                cursor=None,
                limit=50,
                take_id=None,
                api_version="v4",
            )
            self.assertEqual(before["items"][0]["verification"]["verdict"], "usable")

            session = self.mountpoint / "recordings" / session_id
            manifest = json.loads((session / "manifest.json").read_bytes())
            video = manifest["video"]["segments"][0]["artifacts"]["left"]
            artifact_path = session / video["path"]
            payload = bytearray(artifact_path.read_bytes())
            payload[0] ^= 0x01
            artifact_path.write_bytes(payload)

            # No intervening list call: the download entry point itself must
            # invalidate the old usable snapshot before it opens the bytes.
            with self.assertRaises(ArtifactAccessError) as blocked:
                coordinator.open_verified_artifact(session_id, video["artifact_id"], "v4")
            self.assertEqual(blocked.exception.code, "not_verified")

            after = coordinator.list_sessions(
                cursor=None,
                limit=50,
                take_id=None,
                api_version="v4",
            )
            self.assertNotEqual(after["catalog_revision"], before["catalog_revision"])
            self.assertEqual(after["items"][0]["verification"]["verdict"], "unusable")
            self.assertEqual(
                after["items"][0]["verification"]["diagnostics"][0]["code"],
                "artifact_digest_mismatch",
            )
        finally:
            coordinator.close()

    def test_list_sessions_uses_session_store_for_artifact_verification(self) -> None:
        first = self.coordinator()
        try:
            session_id = self.seal_one(first, prefix="native-list-scan")
        finally:
            first.close()

        native = FakeNativeSessionStore()
        with patch("rp_ylx.recording.device_session._session_store_or_none", return_value=native):
            restarted = self.coordinator()
            try:
                listed = restarted.list_sessions(cursor=None, limit=50, take_id=None)
            finally:
                restarted.close()

        self.assertEqual([item["session_id"] for item in listed["items"]], [session_id])
        manifest = json.loads(
            (self.mountpoint / "recordings" / session_id / "manifest.json").read_bytes()
        )
        self.assertGreater(len(native.verify_calls), 0)
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
            if threading.current_thread().name.startswith("rp-ylx-session-writer-"):
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
        split_config = replace(self.session_config, video_layout="split-eyes")
        coordinator = CaptureCoordinator(
            CoordinatorConfig(
                self.mountpoint,
                self.state_root,
                split_config,
                minimum_available_bytes=0,
                minimum_available_inodes=0,
            ),
            mount_checker=lambda path: path == self.mountpoint.resolve(),
        )
        try:
            session_id = self.seal_one(coordinator, prefix="cal", mode="calibration")
            session = self.mountpoint / "recordings" / session_id
            manifest = validate_device_session_directory(session)
            self.assertEqual(manifest["capture_mode"], "calibration")
            self.assertEqual(manifest["video"]["layout"], "split-eyes")
            self.assertEqual(manifest["video"]["codec"], "h264")
            self.assertEqual(manifest["video"]["container"], "mp4")
            self.assertGreaterEqual(len(manifest["video"]["segments"]), 1)
            self.assertEqual(manifest["frames"]["artifact"]["role"], "frames.index")
            self.assertEqual(manifest["imu"]["artifact"]["role"], "imu.samples")
            for segment in manifest["video"]["segments"]:
                self.assertEqual(segment["artifacts"]["left"]["role"], "video.left")
                self.assertEqual(segment["artifacts"]["right"]["role"], "video.right")
            self.assertEqual(manifest["audio"]["state"], "not_recorded")
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
            self.assertEqual(coordinator.open_handle_count, 7)
            sources.submit_frame(frame())  # type: ignore[operator]
            coordinator.stop_capture(stop_command("sources-stop"))
            self.assertEqual(sources.open_handle_count, 0)
            self.assertEqual(coordinator.open_handle_count, 0)
        finally:
            coordinator.close()

    def test_enospc_never_publishes_a_success_manifest(self) -> None:
        def disk_full(role: str, payload: bytes) -> None:
            if role == "frames.index" and payload:
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

    def test_short_write_flush_and_fsync_cannot_publish_a_v2_session(self) -> None:
        cases = (
            ("data-short", "frames.ndjson", "short_write"),
            ("manifest-short", "manifest.json", "short_write"),
            ("data-flush", "frames.ndjson", "flush"),
            ("manifest-flush", "manifest.json", "flush"),
            ("data-fsync", "frames.ndjson", "fsync"),
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
        video = next((self.mountpoint / "recordings" / session_id / "video").glob("left_*.mp4"))
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
            next((active / "video").glob("left_*.mp4")).write_bytes(b"tampered-content")

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
                video = next((target_path / "video").glob("left_*.mp4"))
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
