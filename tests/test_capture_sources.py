from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import unittest
import uuid
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from rp_ylx.api import CaptureCommand
from rp_ylx.api.events import EventReplayBuffer
from rp_ylx.api.preview import LatestPreviewBuffer
from rp_ylx.camera import (
    CameraError,
    CameraMode,
    FrameObservation,
    StereoFrame,
)
from rp_ylx.imu import ImuObservation, ImuSample, RawVector3
from rp_ylx.recording import (
    CaptureCoordinator,
    ContinuousCaptureSources,
    CoordinatorConfig,
    DeviceSessionConfig,
    NativeContinuousCaptureSources,
    initialize_capture_volume,
    validate_device_session_directory,
)
from rp_ylx.recording.stereo_encoder import ClosedSegment, StereoEncoderError

JPEG = b"\xff\xd8threaded-sbs\xff\xd9"


class FakeSplitEyeEncoder:
    def __init__(self, out_dir: Path, *, segment_frames: int, **unused: object) -> None:
        del unused
        self._out_dir = out_dir
        self._segment_frames = segment_frames
        self._segments: list[ClosedSegment] = []
        self._submitted = 0
        self._started = False

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
            raise StereoEncoderError("invalid_state", "fake encoder is not started")
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
        return

    def _close(self, start_frame: int, end_frame: int) -> None:
        index = len(self._segments)
        artifacts: dict[str, tuple[str, int]] = {}
        for eye in ("left", "right"):
            name = f"{eye}_{index:05d}.mp4"
            payload = f"{eye}-{index}-{start_frame}-{end_frame}".encode() * 8
            (self._out_dir / name).write_bytes(payload)
            artifacts[eye] = (f"video/{name}", len(payload))
        self._segments.append(
            ClosedSegment(
                index=index,
                start_frame=start_frame,
                end_frame=end_frame,
                left_path=artifacts["left"][0],
                left_bytes=artifacts["left"][1],
                right_path=artifacts["right"][0],
                right_bytes=artifacts["right"][1],
            )
        )


class GatedSequenceCamera:
    def __init__(
        self,
        observations: tuple[FrameObservation, ...],
        *,
        terminal_error: CameraError | None = None,
    ) -> None:
        self._observations = list(observations)
        self._terminal_error = terminal_error or CameraError("exhausted", "fake camera exhausted")
        self._release = threading.Semaphore(0)
        self.closed = threading.Event()

    def open(self, mode: CameraMode, *, stable_id: str | None = None) -> object:
        del mode, stable_id
        return self

    def start(self) -> None:
        return None

    def release(self, count: int = 1) -> None:
        for _ in range(count):
            self._release.release()

    def read(self, *, timeout: float) -> FrameObservation:
        if not self._release.acquire(timeout=timeout):
            raise TimeoutError("fake camera timeout")
        if self.closed.is_set():
            raise CameraError("disconnected", "fake camera closed")
        try:
            return self._observations.pop(0)
        except IndexError:
            raise self._terminal_error from None

    def stop(self) -> None:
        self.closed.set()
        self._release.release()

    def close(self) -> None:
        self.closed.set()
        self._release.release()


class GatedGapCamera:
    def __init__(self) -> None:
        self.closed = threading.Event()
        self.release_gap = threading.Event()
        self._reads = 0

    def open(self, mode: CameraMode, *, stable_id: str | None = None) -> object:
        del mode, stable_id
        return self

    def start(self) -> None:
        return None

    def read(self, *, timeout: float) -> FrameObservation:
        if self._reads == 0:
            self._reads += 1
            return FrameObservation(
                StereoFrame(0, 1_000_000, b"", b"", True, JPEG),
                dropped_before=0,
            )
        if not self.release_gap.wait(timeout):
            raise TimeoutError("fake camera timeout")
        if self.closed.wait(0.005):
            raise CameraError("disconnected", "fake camera closed")
        sequence = self._reads + 2
        self._reads += 1
        return FrameObservation(
            StereoFrame(sequence, 2_000_000 + self._reads, b"", b"", True, JPEG),
            dropped_before=2 if sequence == 3 else 0,
        )

    def stop(self) -> None:
        self.closed.set()
        self.release_gap.set()

    def close(self) -> None:
        self.closed.set()
        self.release_gap.set()


class StreamingCamera:
    def __init__(self, *, period: float = 0.005) -> None:
        self.period = period
        self.closed = threading.Event()
        self.source_sequence = 0

    def open(self, mode: CameraMode, *, stable_id: str | None = None) -> object:
        del mode, stable_id
        return self

    def start(self) -> None:
        return None

    def read(self, *, timeout: float) -> FrameObservation:
        if self.closed.wait(min(self.period, timeout)):
            raise CameraError("disconnected", "fake camera closed")
        sequence = self.source_sequence
        self.source_sequence += 1
        return FrameObservation(
            StereoFrame(
                sequence,
                1_000_000 + sequence,
                b"",
                b"",
                True,
                JPEG,
            ),
            dropped_before=0,
        )

    def stop(self) -> None:
        self.closed.set()

    def close(self) -> None:
        self.closed.set()


class BlockingImu:
    def __init__(self) -> None:
        self.closed = threading.Event()

    def read(self, *, timeout: float) -> object:
        if not self.closed.wait(timeout):
            raise TimeoutError("fake imu timeout")
        raise OSError("fake imu closed")

    def close(self) -> None:
        self.closed.set()


class StreamingImu:
    def __init__(self, *, period: float = 0.005) -> None:
        self.period = period
        self.closed = threading.Event()
        self.sequence = 0

    def read(self, *, timeout: float) -> ImuObservation:
        if self.closed.wait(min(self.period, timeout)):
            raise OSError("fake imu closed")
        host = time.monotonic_ns()
        samples = []
        for sample_index in range(2):
            samples.append(
                ImuSample(
                    sequence=self.sequence,
                    packet_sequence=self.sequence // 2,
                    sample_index=sample_index,
                    device_timestamp_raw=self.sequence,
                    device_ticks=self.sequence,
                    host_read_start_ns=host,
                    host_read_end_ns=host,
                    host_monotonic_ns=host,
                    accelerometer=RawVector3(0, 0, 0),
                    gyroscope=RawVector3(0, 0, 0),
                    sync_offset_ns=None,
                    sync_residual_ns=None,
                    sync_quality="insufficient",
                )
            )
            self.sequence += 1
        return ImuObservation((samples[0], samples[1]), 0)

    def close(self) -> None:
        self.closed.set()


class FakeNativeContinuousRuntime:
    def __init__(self) -> None:
        self.preview_started = False
        self.closed = False
        self.stop_calls = 0
        self.submit_frame = None
        self.submit_imu = None
        self.imu = None
        self.imu_timeout_seconds = None
        self.on_failure = None
        self.active_take = None
        self.sink = None
        self.encoder = None
        self.segment_planner = None
        self.recording_start_monotonic_ns = None

    def start_preview(self) -> None:
        self.preview_started = True

    def start_recording(
        self,
        submit_frame: object,
        on_failure: object,
        imu: object | None = None,
        submit_imu: object | None = None,
        imu_timeout_seconds: float = 1.0,
    ) -> dict[str, object]:
        self.submit_frame = submit_frame
        self.submit_imu = submit_imu
        self.imu = imu
        self.imu_timeout_seconds = imu_timeout_seconds
        self.on_failure = on_failure
        return self.snapshot()

    def start_recording_split_sink(
        self,
        active_take: object,
        sink: object,
        encoder: object,
        segment_planner: object,
        recording_start_monotonic_ns: int,
        on_failure: object,
        imu: object | None = None,
        imu_timeout_seconds: float = 1.0,
    ) -> dict[str, object]:
        self.active_take = active_take
        self.sink = sink
        self.encoder = encoder
        self.segment_planner = segment_planner
        self.recording_start_monotonic_ns = recording_start_monotonic_ns
        self.on_failure = on_failure
        self.imu = imu
        self.imu_timeout_seconds = imu_timeout_seconds
        return self.snapshot()

    def emit_frame(
        self,
        source_sequence: int,
        host_monotonic_ns: int,
        dropped_before: int,
        left: bytes,
        right: bytes,
        raw_side_by_side: bytes,
    ) -> object:
        assert self.submit_frame is not None
        return self.submit_frame(
            source_sequence,
            host_monotonic_ns,
            dropped_before,
            left,
            right,
            raw_side_by_side,
        )

    def emit_imu(self, raw: object) -> object:
        assert self.submit_imu is not None
        return self.submit_imu(raw)

    def fail(self, code: str, message: str) -> None:
        assert self.on_failure is not None
        self.on_failure(code, message)

    def stop_recording(self, timeout_seconds: float = 3.0) -> dict[str, object]:
        del timeout_seconds
        self.stop_calls += 1
        return self.snapshot()

    def close(self, timeout_seconds: float = 5.0) -> dict[str, object]:
        del timeout_seconds
        self.closed = True
        return self.snapshot()

    def snapshot(self) -> dict[str, object]:
        return {
            "running": self.preview_started and not self.closed,
            "recording_present": self.submit_frame is not None or self.active_take is not None,
            "recording_active": self.submit_frame is not None or self.active_take is not None,
            "inflight_frames": 0,
            "observed_frames": 0,
            "failure_reported": False,
            "terminal_error": None,
            "last_preview_error": None,
        }


def capture_command(key: str, body: dict[str, object]) -> CaptureCommand:
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    return CaptureCommand("operator", key, body, canonical)


def frame_observation(sequence: int, *, dropped_before: int = 0) -> FrameObservation:
    return FrameObservation(
        StereoFrame(
            sequence,
            1_000_000 + sequence,
            f"left-{sequence}".encode(),
            f"right-{sequence}".encode(),
            True,
            JPEG,
        ),
        dropped_before=dropped_before,
    )


def native_imu_observation(sequence: int = 0) -> dict[str, object]:
    samples = []
    for sample_index in range(2):
        sample_sequence = sequence + sample_index
        samples.append(
            {
                "sequence": sample_sequence,
                "packet_sequence": sequence // 2,
                "sample_index": sample_index,
                "device_timestamp_raw": sample_sequence,
                "device_ticks": sample_sequence,
                "host_read_start_ns": 10_000 + sample_sequence,
                "host_read_end_ns": 10_100 + sample_sequence,
                "host_monotonic_ns": 10_050 + sample_sequence,
                "raw": {
                    "accelerometer": [1, 2, 3],
                    "gyroscope": [4, 5, 6],
                },
                "sync": {
                    "offset_ns": None,
                    "residual_ns": None,
                    "quality": "insufficient",
                },
            }
        )
    return {"samples": samples, "dropped_samples": 0}


class CaptureSourcesTest(unittest.TestCase):
    def test_all_raw_sources_declare_calibration_support(self) -> None:
        self.assertTrue(ContinuousCaptureSources.supports_calibration_capture)
        self.assertTrue(NativeContinuousCaptureSources.supports_calibration_capture)

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.environment_patcher = patch.dict(
            os.environ,
            {"RP_YLX_NETWORK_OPERATION_LOCK_PATH": str(self.root / "network-operation.lock")},
        )
        self.environment_patcher.start()
        self.network_control_patcher = patch(
            "rp_ylx.recording.coordinator.request_network_control",
            return_value={
                "ok": True,
                "operation": "status",
                "status": 200,
                "body": {"transaction": {"current": None}},
            },
        )
        self.network_control_patcher.start()
        self.encoder_patcher = patch(
            "rp_ylx.recording.device_session.StereoEncoderProcess",
            FakeSplitEyeEncoder,
        )
        self.encoder_patcher.start()
        self.volume = self.root / "volume"
        self.volume.mkdir()
        initialize_capture_volume(self.volume)
        self.config = CoordinatorConfig(
            self.volume,
            self.root / "state",
            DeviceSessionConfig(
                device_id=str(uuid.uuid4()),
                device_label="YLX-89ABCDEF",
                hardware_fingerprint="sha256:" + "c" * 64,
                platform="D-Robotics RDK X5 V1.0 + YLX 2UQ2",
                software_version="0.5.0",
                commit="d" * 40,
                width=3840,
                height=1080,
                sensor_fps=60.0,
            ),
            minimum_available_bytes=0,
            minimum_available_inodes=0,
        )

    def tearDown(self) -> None:
        self.encoder_patcher.stop()
        self.network_control_patcher.stop()
        self.environment_patcher.stop()
        self.temporary.cleanup()

    def start(self, coordinator: CaptureCoordinator, *, key: str) -> str:
        result = coordinator.start_capture(
            capture_command(
                key,
                {
                    "schema": "ylx.capture-start.v2",
                    "mode": "production",
                    "take": {"kind": "new"},
                },
            )
        )
        active = result.body["snapshot"]["active_recording"]
        return active["recording_state"]["session_id"]

    def stop(self, coordinator: CaptureCoordinator, *, key: str) -> None:
        coordinator.stop_capture(
            capture_command(
                key,
                {"schema": "ylx.capture-stop.v2", "reason": "user"},
            )
        )

    def wait_for_retained_failure(
        self,
        coordinator: CaptureCoordinator,
        session_id: str,
    ) -> dict[str, object]:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            retained = coordinator.retained_unsuccessful_outcome(session_id)
            if retained is not None:
                return retained
            time.sleep(0.01)
        self.fail(f"session {session_id} did not settle to a retained failure")

    def test_continuous_sources_keep_preview_available_outside_recording(self) -> None:
        camera = StreamingCamera()
        imu = StreamingImu()
        preview = LatestPreviewBuffer(stream_fps=15)
        sources = ContinuousCaptureSources(
            lambda: camera,
            lambda: imu,
            CameraMode(3840, 1080, 60.0, "mjpg"),
            publish_preview=preview.publish,
            read_timeout=0.2,
            warmup_frames=1,
        )
        coordinator = CaptureCoordinator(
            self.config,
            mount_checker=lambda path: path == self.volume.resolve(),
            preview=preview,
            sources=sources,
        )
        try:
            idle_preview = coordinator.latest_preview(fps=None, accept="image/jpeg")
            self.assertEqual(idle_preview.body, JPEG)

            session_id = self.start(coordinator, key="continuous-start")
            time.sleep(0.15)
            self.stop(coordinator, key="continuous-stop")
            retained = coordinator.capture_status()["snapshot"]["retained_unsuccessful"]
            self.assertIsNone(retained)
            manifest = validate_device_session_directory(self.volume / "recordings" / session_id)
            self.assertGreater(manifest["frames"]["count"], 0)
            self.assertEqual(manifest["integrity"]["dropped_frames"], 0)
            after_stop_preview = coordinator.latest_preview(fps=None, accept="image/jpeg")
            self.assertEqual(after_stop_preview.body, JPEG)
            self.assertEqual(sources.open_handle_count, 1)
        finally:
            coordinator.close()
        self.assertTrue(camera.closed.is_set())
        self.assertTrue(imu.closed.is_set())

    def test_continuous_sources_decimate_recording_without_decimating_preview(self) -> None:
        camera = GatedSequenceCamera(tuple(frame_observation(index) for index in range(6)))
        imu = BlockingImu()
        preview_payloads: list[bytes] = []
        submitted: list[FrameObservation] = []
        failures: list[tuple[str, str]] = []
        sources = ContinuousCaptureSources(
            lambda: camera,
            lambda: imu,
            CameraMode(3840, 1080, 60.0, "mjpg"),
            publish_preview=preview_payloads.append,
            read_timeout=1.0,
            frame_decimation=2,
        )
        try:
            sources.start(
                mode="production",
                generation_id=str(uuid.uuid4()),
                submit_frame=lambda observation: submitted.append(observation) or True,
                submit_imu=lambda observation: True,
                on_failure=lambda code, message: failures.append((code, message)),
            )
            camera.release(6)
            deadline = time.monotonic() + 1
            while len(preview_payloads) < 6 and time.monotonic() < deadline:
                time.sleep(0.01)
        finally:
            sources.close()

        self.assertFalse(failures)
        self.assertEqual(
            [observation.frame.source_sequence for observation in submitted],
            [0, 2, 4],
        )
        self.assertEqual(preview_payloads, [f"left-{index}".encode() for index in range(6)])

    def test_native_continuous_sources_convert_runtime_frames_for_recorder(self) -> None:
        imu = BlockingImu()
        runtime = FakeNativeContinuousRuntime()
        native_camera = object()
        preview = SimpleNamespace(native_owner=object())
        submitted: list[FrameObservation] = []
        failures: list[tuple[str, str]] = []
        sources = NativeContinuousCaptureSources(
            "/dev/video0",
            lambda: imu,
            CameraMode(3840, 1080, 60.0, "mjpg"),
            preview=preview,
            read_timeout=0.1,
            frame_decimation=2,
            require_native_imu=False,
        )
        try:
            with (
                patch(
                    "rp_ylx.recording.sources.create_native_camera",
                    return_value=native_camera,
                ) as create_camera,
                patch(
                    "rp_ylx.recording.sources.create_native_continuous_capture_runtime",
                    return_value=runtime,
                ) as create_runtime,
            ):
                sources.start(
                    mode="production",
                    generation_id=str(uuid.uuid4()),
                    submit_frame=lambda observation: submitted.append(observation) or True,
                    submit_imu=lambda observation: True,
                    on_failure=lambda code, message: failures.append((code, message)),
                )
            create_camera.assert_called_once_with(
                "/dev/video0",
                3840,
                1080,
                60,
                "mjpg",
                buffer_count=16,
                queue_capacity=64,
                split_eyes=False,
            )
            create_runtime.assert_called_once_with(
                native_camera,
                preview.native_owner,
                2,
                read_timeout_seconds=0.1,
            )
            self.assertTrue(runtime.preview_started)
            self.assertTrue(
                runtime.emit_frame(7, 123_456, 3, b"", b"", JPEG),
            )
        finally:
            sources.close()

        self.assertFalse(failures)
        self.assertEqual(len(submitted), 1)
        self.assertEqual(submitted[0].frame.source_sequence, 7)
        self.assertEqual(submitted[0].frame.host_monotonic_ns, 123_456)
        self.assertEqual(submitted[0].frame.raw_side_by_side, JPEG)
        self.assertEqual(submitted[0].dropped_before, 3)
        self.assertTrue(imu.closed.is_set())
        self.assertTrue(runtime.closed)

    def test_native_continuous_sources_use_open_camera_for_focus_controls(self) -> None:
        runtime = FakeNativeContinuousRuntime()
        preview = SimpleNamespace(native_owner=object())
        focus = {
            "schema": "ylx.camera-focus.v1",
            "value": 42,
            "minimum": 0,
            "maximum": 255,
            "step": 1,
            "default": 32,
            "auto_supported": True,
            "auto_enabled": False,
        }
        focus_commands: list[tuple[int | None, bool | None]] = []

        def set_focus(
            *,
            value: int | None,
            auto_enabled: bool | None,
        ) -> dict[str, object]:
            focus_commands.append((value, auto_enabled))
            return {**focus, "value": value}

        camera = SimpleNamespace(
            close=lambda: None,
            camera_focus_status=unittest.mock.Mock(return_value=focus),
            set_camera_focus=unittest.mock.Mock(side_effect=set_focus),
        )
        sources = NativeContinuousCaptureSources(
            "/dev/video0",
            lambda: BlockingImu(),
            CameraMode(3840, 1080, 60.0, "mjpg"),
            preview=preview,
            read_timeout=0.1,
        )
        try:
            with (
                patch("rp_ylx.recording.sources.create_native_camera", return_value=camera),
                patch(
                    "rp_ylx.recording.sources.create_native_continuous_capture_runtime",
                    return_value=runtime,
                ),
            ):
                sources.start_preview()
                self.assertEqual(sources.camera_focus_status(), focus)
                self.assertEqual(
                    sources.set_camera_focus(value=64, auto_enabled=False),
                    {**focus, "value": 64},
                )
            camera.camera_focus_status.assert_called_once_with()
            camera.set_camera_focus.assert_called_once_with(
                value=64,
                auto_enabled=False,
            )
            self.assertEqual(focus_commands, [(64, False)])
        finally:
            sources.close()

    def test_native_continuous_sources_rebuild_terminal_preview_after_hotplug(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            device = Path(directory) / "video0"
            preview = SimpleNamespace(native_owner=object())
            first_runtime = FakeNativeContinuousRuntime()
            second_runtime = FakeNativeContinuousRuntime()
            first_camera = unittest.mock.Mock()
            second_camera = unittest.mock.Mock()
            sources = NativeContinuousCaptureSources(
                str(device),
                lambda: BlockingImu(),
                CameraMode(3840, 1080, 60.0, "mjpg"),
                preview=preview,
                read_timeout=0.1,
            )
            try:
                self.assertEqual(
                    sources.camera_connection_status()["state"],
                    "disconnected",
                )
                device.touch()
                self.assertEqual(
                    sources.camera_connection_status()["state"],
                    "connected",
                )
                with (
                    patch(
                        "rp_ylx.recording.sources.create_native_camera",
                        side_effect=(first_camera, second_camera),
                    ) as create_camera,
                    patch(
                        "rp_ylx.recording.sources.create_native_continuous_capture_runtime",
                        side_effect=(first_runtime, second_runtime),
                    ),
                ):
                    sources.start_preview()
                    first_runtime.preview_started = False
                    device.unlink()
                    self.assertEqual(
                        sources.camera_connection_status()["state"],
                        "disconnected",
                    )
                    device.touch()
                    sources.start_preview()

                self.assertEqual(create_camera.call_count, 2)
                self.assertTrue(first_runtime.closed)
                first_camera.close.assert_called_once_with()
                self.assertTrue(second_runtime.preview_started)
                self.assertEqual(sources.open_handle_count, 1)
            finally:
                sources.close()

    def test_native_continuous_sources_treat_missing_v4l2_focus_as_unsupported(self) -> None:
        runtime = FakeNativeContinuousRuntime()
        camera = SimpleNamespace(
            close=lambda: None,
            camera_focus_status=unittest.mock.Mock(
                side_effect=RuntimeError("camera_focus_unsupported: 相机没有可读取的焦距控制")
            ),
            set_camera_focus=unittest.mock.Mock(
                side_effect=RuntimeError("camera_focus_unsupported: 相机没有可读取的焦距控制")
            ),
        )
        sources = NativeContinuousCaptureSources(
            "/dev/video0",
            lambda: BlockingImu(),
            CameraMode(3840, 1080, 60.0, "mjpg"),
            preview=SimpleNamespace(native_owner=object()),
            read_timeout=0.1,
        )
        try:
            with (
                patch("rp_ylx.recording.sources.create_native_camera", return_value=camera),
                patch(
                    "rp_ylx.recording.sources.create_native_continuous_capture_runtime",
                    return_value=runtime,
                ),
            ):
                sources.start_preview()
                self.assertIsNone(sources.camera_focus_status())
                with self.assertRaises(CameraError) as unsupported:
                    sources.set_camera_focus(value=42)
            self.assertEqual(unsupported.exception.code, "camera_focus_unsupported")
            self.assertFalse(unsupported.exception.retryable)
        finally:
            sources.close()

    def test_native_continuous_sources_require_native_imu_by_default(self) -> None:
        imu = BlockingImu()
        runtime = FakeNativeContinuousRuntime()
        preview = SimpleNamespace(native_owner=object())
        sources = NativeContinuousCaptureSources(
            "/dev/video0",
            lambda: imu,
            CameraMode(3840, 1080, 60.0, "mjpg"),
            preview=preview,
            read_timeout=0.1,
        )
        try:
            with (
                patch("rp_ylx.recording.sources.create_native_camera", return_value=object()),
                patch(
                    "rp_ylx.recording.sources.create_native_continuous_capture_runtime",
                    return_value=runtime,
                ),
                self.assertRaisesRegex(RuntimeError, "Rust IMU"),
            ):
                sources.start(
                    mode="production",
                    generation_id=str(uuid.uuid4()),
                    submit_frame=lambda observation: True,
                    submit_imu=lambda observation: True,
                    on_failure=lambda code, message: None,
                )
        finally:
            sources.close()

        self.assertTrue(imu.closed.is_set())
        self.assertEqual(sources.open_handle_count, 0)

    def test_native_continuous_sources_require_split_calibration_recorder(self) -> None:
        imu_factory_calls: list[bool] = []
        recorder = SimpleNamespace()
        sources = NativeContinuousCaptureSources(
            "/dev/video0",
            lambda: imu_factory_calls.append(True),  # type: ignore[arg-type,func-returns-value]
            CameraMode(3840, 1080, 60.0, "mjpg"),
            preview=SimpleNamespace(native_owner=object()),
            read_timeout=0.1,
        )
        try:
            with self.assertRaisesRegex(RuntimeError, "split-eyes"):
                sources.start(
                    mode="calibration",
                    generation_id=str(uuid.uuid4()),
                    submit_frame=lambda observation: True,
                    submit_imu=lambda observation: True,
                    on_failure=lambda code, message: None,
                    native_recorder=recorder,
                )
        finally:
            sources.close()

        self.assertFalse(imu_factory_calls)
        self.assertEqual(sources.open_handle_count, 0)

    def test_native_continuous_sources_reap_failed_native_worker_before_retry(self) -> None:
        class NativeOnlyImu:
            def __init__(self) -> None:
                self.native_owner = object()
                self.closed = False

            def read(self, *, timeout: float) -> ImuObservation:
                del timeout
                raise AssertionError("direct native IMU path must not use Python read loop")

            def close(self) -> None:
                self.closed = True

        class StaleWorkerRuntime(FakeNativeContinuousRuntime):
            def __init__(self) -> None:
                super().__init__()
                self.worker_active = False

            def start_recording_split_sink(
                self,
                active_take: object,
                sink: object,
                encoder: object,
                segment_planner: object,
                recording_start_monotonic_ns: int,
                on_failure: object,
                imu: object | None = None,
                imu_timeout_seconds: float = 1.0,
            ) -> dict[str, object]:
                if self.worker_active:
                    raise RuntimeError("capture runtime IMU worker is already running")
                self.worker_active = True
                return super().start_recording_split_sink(
                    active_take,
                    sink,
                    encoder,
                    segment_planner,
                    recording_start_monotonic_ns,
                    on_failure,
                    imu,
                    imu_timeout_seconds,
                )

            def stop_recording(self, timeout_seconds: float = 3.0) -> dict[str, object]:
                self.worker_active = False
                self.active_take = None
                self.sink = None
                return super().stop_recording(timeout_seconds)

        runtime = StaleWorkerRuntime()
        imus: list[NativeOnlyImu] = []
        failures: list[tuple[str, str]] = []

        def imu_factory() -> NativeOnlyImu:
            imu = NativeOnlyImu()
            imus.append(imu)
            return imu

        sources = NativeContinuousCaptureSources(
            "/dev/video0",
            imu_factory,
            CameraMode(3840, 1080, 60.0, "mjpg"),
            preview=SimpleNamespace(native_owner=object()),
            read_timeout=0.1,
        )
        recorder = SimpleNamespace(
            native_split_sink_targets=lambda: (
                object(),
                object(),
                object(),
                object(),
                123_456_789,
            )
        )
        try:
            with (
                patch("rp_ylx.recording.sources.create_native_camera", return_value=object()),
                patch(
                    "rp_ylx.recording.sources.create_native_continuous_capture_runtime",
                    return_value=runtime,
                ),
            ):
                sources.start(
                    mode="calibration",
                    generation_id=str(uuid.uuid4()),
                    submit_frame=lambda observation: True,
                    submit_imu=lambda observation: True,
                    on_failure=lambda code, message: failures.append((code, message)),
                    native_recorder=recorder,
                )
                runtime.fail("source_sequence_gap", "source frame sequence has a gap")

                sources.start(
                    mode="calibration",
                    generation_id=str(uuid.uuid4()),
                    submit_frame=lambda observation: True,
                    submit_imu=lambda observation: True,
                    on_failure=lambda code, message: failures.append((code, message)),
                    native_recorder=recorder,
                )

            self.assertEqual(
                failures,
                [("source_sequence_gap", "source frame sequence has a gap")],
            )
            self.assertEqual(len(imus), 2)
            self.assertTrue(imus[0].closed)
            self.assertFalse(imus[1].closed)
            self.assertTrue(runtime.worker_active)
        finally:
            sources.close()

    def test_native_continuous_sources_route_calibration_to_split_sink(self) -> None:
        class NativeOnlyImu:
            def __init__(self) -> None:
                self.native_owner = object()
                self.closed = False

            def read(self, *, timeout: float) -> ImuObservation:
                del timeout
                raise AssertionError("direct split native IMU path must not use Python read loop")

            def close(self) -> None:
                self.closed = True

        active_take = object()
        sink = object()
        encoder = object()
        segment_planner = object()
        started_monotonic_ns = 123_456_789
        recorder = SimpleNamespace(
            native_split_sink_targets=lambda: (
                active_take,
                sink,
                encoder,
                segment_planner,
                started_monotonic_ns,
            ),
        )
        imu = NativeOnlyImu()
        runtime = FakeNativeContinuousRuntime()
        preview = SimpleNamespace(native_owner=object())
        submitted: list[FrameObservation] = []
        submitted_imu: list[ImuObservation] = []
        sources = NativeContinuousCaptureSources(
            "/dev/video0",
            lambda: imu,
            CameraMode(3840, 1080, 60.0, "mjpg"),
            preview=preview,
            read_timeout=0.1,
        )
        try:
            with (
                patch("rp_ylx.recording.sources.create_native_camera", return_value=object()),
                patch(
                    "rp_ylx.recording.sources.create_native_continuous_capture_runtime",
                    return_value=runtime,
                ),
            ):
                sources.start(
                    mode="calibration",
                    generation_id=str(uuid.uuid4()),
                    submit_frame=lambda observation: submitted.append(observation) or True,
                    submit_imu=lambda observation: submitted_imu.append(observation) or True,
                    on_failure=lambda code, message: None,
                    native_recorder=recorder,
                )
            self.assertIs(runtime.active_take, active_take)
            self.assertIs(runtime.sink, sink)
            self.assertIs(runtime.encoder, encoder)
            self.assertIs(runtime.segment_planner, segment_planner)
            self.assertEqual(runtime.recording_start_monotonic_ns, started_monotonic_ns)
            self.assertIs(runtime.imu, imu.native_owner)
            self.assertIsNone(runtime.submit_frame)
            self.assertIsNone(runtime.submit_imu)
        finally:
            sources.close()

        self.assertFalse(submitted)
        self.assertFalse(submitted_imu)
        self.assertTrue(imu.closed)
        self.assertTrue(runtime.closed)

    def test_native_continuous_sources_reject_invalid_split_sink_start_time(self) -> None:
        imu_factory_calls: list[bool] = []
        recorder = SimpleNamespace(
            native_split_sink_targets=lambda: (
                object(),
                object(),
                object(),
                object(),
                0,
            ),
        )
        sources = NativeContinuousCaptureSources(
            "/dev/video0",
            lambda: imu_factory_calls.append(True),  # type: ignore[arg-type,func-returns-value]
            CameraMode(3840, 1080, 60.0, "mjpg"),
            preview=SimpleNamespace(native_owner=object()),
            read_timeout=0.1,
        )
        try:
            with self.assertRaisesRegex(RuntimeError, "起始时间"):
                sources.start(
                    mode="calibration",
                    generation_id=str(uuid.uuid4()),
                    submit_frame=lambda observation: True,
                    submit_imu=lambda observation: True,
                    on_failure=lambda code, message: None,
                    native_recorder=recorder,
                )
        finally:
            sources.close()

        self.assertFalse(imu_factory_calls)
        self.assertEqual(sources.open_handle_count, 0)

    def test_native_continuous_sources_pass_metrics_owner_to_runtime(self) -> None:
        runtime = FakeNativeContinuousRuntime()
        native_camera = object()
        native_metrics = object()
        preview = SimpleNamespace(native_owner=object())
        metrics = SimpleNamespace(native_owner=native_metrics)
        sources = NativeContinuousCaptureSources(
            "/dev/video0",
            lambda: BlockingImu(),
            CameraMode(3840, 1080, 60.0, "mjpg"),
            preview=preview,
            read_timeout=0.1,
            metrics=metrics,
        )
        try:
            with (
                patch(
                    "rp_ylx.recording.sources.create_native_camera",
                    return_value=native_camera,
                ),
                patch(
                    "rp_ylx.recording.sources.create_native_continuous_capture_runtime",
                    return_value=runtime,
                ) as create_runtime,
            ):
                sources.start_preview()
            create_runtime.assert_called_once_with(
                native_camera,
                preview.native_owner,
                1,
                read_timeout_seconds=0.1,
                metrics=native_metrics,
            )
        finally:
            sources.close()

    def test_native_continuous_sources_let_runtime_submit_native_imu(self) -> None:
        class NativeOnlyImu:
            def __init__(self) -> None:
                self.native_owner = object()
                self.closed = False
                self.read_calls = 0

            def read(self, *, timeout: float) -> ImuObservation:
                del timeout
                self.read_calls += 1
                raise AssertionError("native IMU path must not use Python read loop")

            def close(self) -> None:
                self.closed = True

        imu = NativeOnlyImu()
        runtime = FakeNativeContinuousRuntime()
        preview = SimpleNamespace(native_owner=object())
        submitted_imu: list[ImuObservation] = []
        sources = NativeContinuousCaptureSources(
            "/dev/video0",
            lambda: imu,
            CameraMode(3840, 1080, 60.0, "mjpg"),
            preview=preview,
            read_timeout=0.1,
        )
        try:
            with (
                patch("rp_ylx.recording.sources.create_native_camera", return_value=object()),
                patch(
                    "rp_ylx.recording.sources.create_native_continuous_capture_runtime",
                    return_value=runtime,
                ),
            ):
                sources.start(
                    mode="production",
                    generation_id=str(uuid.uuid4()),
                    submit_frame=lambda observation: True,
                    submit_imu=lambda observation: submitted_imu.append(observation) or True,
                    on_failure=lambda code, message: None,
                )
            self.assertIs(runtime.imu, imu.native_owner)
            self.assertEqual(runtime.imu_timeout_seconds, 0.1)
            self.assertTrue(runtime.emit_imu(native_imu_observation(10)))
        finally:
            sources.close()

        self.assertEqual(imu.read_calls, 0)
        self.assertTrue(imu.closed)
        self.assertEqual(len(submitted_imu), 1)
        self.assertEqual([sample.sequence for sample in submitted_imu[0].samples], [10, 11])

    def test_native_continuous_sources_detach_imu_on_runtime_failure(self) -> None:
        imu = BlockingImu()
        runtime = FakeNativeContinuousRuntime()
        preview = SimpleNamespace(native_owner=object())
        failures: list[tuple[str, str]] = []
        sources = NativeContinuousCaptureSources(
            "/dev/video0",
            lambda: imu,
            CameraMode(3840, 1080, 60.0, "mjpg"),
            preview=preview,
            read_timeout=0.1,
            require_native_imu=False,
        )
        try:
            with (
                patch("rp_ylx.recording.sources.create_native_camera", return_value=object()),
                patch(
                    "rp_ylx.recording.sources.create_native_continuous_capture_runtime",
                    return_value=runtime,
                ),
            ):
                sources.start(
                    mode="production",
                    generation_id=str(uuid.uuid4()),
                    submit_frame=lambda observation: True,
                    submit_imu=lambda observation: True,
                    on_failure=lambda code, message: failures.append((code, message)),
                )
            runtime.fail("camera_failed", "boom")
            deadline = time.monotonic() + 1
            while sources.open_handle_count != 1 and time.monotonic() < deadline:
                time.sleep(0.01)
        finally:
            sources.close()

        self.assertEqual(failures, [("camera_failed", "boom")])
        self.assertTrue(imu.closed.is_set())
        self.assertEqual(sources.open_handle_count, 0)
        self.assertTrue(runtime.closed)

    def test_continuous_sources_source_gap_on_decimated_frame_fails_closed(self) -> None:
        camera = GatedSequenceCamera(
            (
                frame_observation(0),
                frame_observation(2, dropped_before=1),
            )
        )
        imu = BlockingImu()
        preview = LatestPreviewBuffer(stream_fps=15)
        sources = ContinuousCaptureSources(
            lambda: camera,
            lambda: imu,
            CameraMode(3840, 1080, 60.0, "mjpg"),
            publish_preview=preview.publish,
            read_timeout=1.0,
            frame_decimation=2,
        )
        config = replace(
            self.config,
            session=replace(self.config.session, frame_decimation=2),
        )
        coordinator = CaptureCoordinator(
            config,
            mount_checker=lambda path: path == self.volume.resolve(),
            preview=preview,
            sources=sources,
        )
        try:
            session_id = self.start(coordinator, key="continuous-decimated-gap-start")
            camera.release(2)
            retained = self.wait_for_retained_failure(coordinator, session_id)
            state = retained["outcome"]["recording_state"]
            self.assertEqual(state["state"], "failed")
            self.assertEqual(state["diagnostics"][0]["code"], "source_sequence_gap")
            self.assertFalse((self.volume / "recordings" / session_id).exists())
        finally:
            coordinator.close()

    def test_continuous_sources_ignore_pre_recording_gap_on_first_recorded_frame(self) -> None:
        camera = GatedGapCamera()
        imu = StreamingImu()
        preview = LatestPreviewBuffer(stream_fps=15)
        sources = ContinuousCaptureSources(
            lambda: camera,
            lambda: imu,
            CameraMode(3840, 1080, 60.0, "mjpg"),
            publish_preview=preview.publish,
            read_timeout=0.2,
        )
        coordinator = CaptureCoordinator(
            self.config,
            mount_checker=lambda path: path == self.volume.resolve(),
            preview=preview,
            sources=sources,
        )
        try:
            coordinator.latest_preview(fps=None, accept="image/jpeg")
            session_id = self.start(coordinator, key="continuous-gap-start")
            camera.release_gap.set()
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline:
                active = coordinator.capture_status()["snapshot"]["active_recording"]
                if active is not None and active["recording_state"]["progress"]["captured_frames"]:
                    break
                time.sleep(0.01)
            self.stop(coordinator, key="continuous-gap-stop")
            retained = coordinator.capture_status()["snapshot"]["retained_unsuccessful"]
            self.assertIsNone(retained)
            manifest = validate_device_session_directory(self.volume / "recordings" / session_id)
            self.assertEqual(manifest["integrity"]["dropped_frames"], 0)
            self.assertGreaterEqual(manifest["frames"]["count"], 1)
        finally:
            coordinator.close()

    def test_camera_failure_closes_imu_and_retains_failed_outcome(self) -> None:
        camera = GatedSequenceCamera((), terminal_error=CameraError("disconnected", "模拟热拔"))
        imu = BlockingImu()
        sources = ContinuousCaptureSources(
            lambda: camera,
            lambda: imu,
            CameraMode(3840, 1080, 60.0, "mjpg"),
            publish_preview=lambda payload: None,
            read_timeout=1.0,
        )
        coordinator = CaptureCoordinator(
            self.config,
            mount_checker=lambda path: path == self.volume.resolve(),
            sources=sources,
        )
        try:
            session_id = self.start(coordinator, key="failure-start")
            camera.release()
            deadline = time.monotonic() + 2
            status = coordinator.capture_status()
            while status["snapshot"]["device_state"] != "idle" and time.monotonic() < deadline:
                time.sleep(0.01)
                status = coordinator.capture_status()
            self.assertEqual(status["snapshot"]["device_state"], "idle")
            retained = coordinator.retained_unsuccessful_outcome(session_id)
            state = retained["outcome"]["recording_state"]
            self.assertEqual(state["state"], "failed")
            self.assertEqual(state["diagnostics"][0]["code"], "disconnected")
            partial = self.volume / "recordings" / f"{session_id}.partial"
            self.assertFalse((partial / "manifest.json").exists())
        finally:
            coordinator.close()
        self.assertEqual(sources.open_handle_count, 0)
        self.assertTrue(imu.closed.is_set())

    def test_source_fault_matrix_is_retained_and_survives_restart(self) -> None:
        cases = ("source_sequence_gap", "sequence_regression", "bad_frame")

        for index, expected_code in enumerate(cases):
            with self.subTest(index=index, expected_code=expected_code):
                mode = CameraMode(3840, 1080, 60.0, "mjpg")
                camera = GatedSequenceCamera(
                    (), terminal_error=CameraError(expected_code, "模拟采集故障")
                )
                imu = BlockingImu()
                sources = ContinuousCaptureSources(
                    lambda camera=camera: camera,
                    lambda imu=imu: imu,
                    mode,
                    publish_preview=lambda payload: None,
                    read_timeout=1.0,
                )
                coordinator = CaptureCoordinator(
                    self.config,
                    mount_checker=lambda path: path == self.volume.resolve(),
                    sources=sources,
                )
                try:
                    session_id = self.start(coordinator, key=f"source-fault-{index}")
                    camera.release()
                    retained = self.wait_for_retained_failure(coordinator, session_id)
                    state = retained["outcome"]["recording_state"]
                    self.assertEqual(state["state"], "failed")
                    self.assertEqual(state["diagnostics"][0]["code"], expected_code)

                    status = coordinator.capture_status()
                    projected = status["snapshot"]["retained_unsuccessful"]
                    self.assertEqual(projected["recording_state"], state)
                    source_event = coordinator.capture_snapshot_event()
                    self.assertEqual(
                        source_event["data"]["retained_unsuccessful"]["recording_state"],
                        state,
                    )
                    event_buffer = EventReplayBuffer(capacity=1)
                    event_buffer.publish(source_event)
                    event_buffer.publish(source_event)
                    replayed = event_buffer.replay(
                        "1",
                        api_version="v3",
                        snapshot=coordinator.capture_snapshot_event,
                    )
                    replayed_state = replayed[0].source_event["data"]["retained_unsuccessful"][
                        "recording_state"
                    ]
                    self.assertEqual(replayed_state, state)
                    self.assertEqual(
                        coordinator.retained_unsuccessful_outcome(session_id)["outcome"][
                            "recording_state"
                        ],
                        state,
                    )

                    partial = self.volume / "recordings" / f"{session_id}.partial"
                    final = self.volume / "recordings" / session_id
                    self.assertTrue(partial.is_dir())
                    self.assertFalse((partial / "manifest.json").exists())
                    self.assertFalse(final.exists())
                finally:
                    coordinator.close()
                self.assertTrue(imu.closed.is_set())

                restarted = CaptureCoordinator(
                    self.config,
                    mount_checker=lambda path: path == self.volume.resolve(),
                )
                try:
                    recovered = restarted.retained_unsuccessful_outcome(session_id)
                    recovered_state = recovered["outcome"]["recording_state"]
                    self.assertEqual(recovered_state["state"], "failed")
                    self.assertEqual(recovered_state["diagnostics"][0]["code"], expected_code)
                    recovered_snapshot = restarted.capture_status()["snapshot"]
                    self.assertEqual(
                        recovered_snapshot["retained_unsuccessful"]["recording_state"][
                            "session_id"
                        ],
                        session_id,
                    )
                finally:
                    restarted.close()

        coordinator = CaptureCoordinator(
            self.config,
            mount_checker=lambda path: path == self.volume.resolve(),
        )
        try:
            session_id = self.start(coordinator, key="after-source-fault")
            self.assertTrue(
                coordinator.submit_frame(
                    FrameObservation(
                        StereoFrame(0, 1_000_000, b"left", b"right", True, JPEG),
                        dropped_before=0,
                    )
                )
            )
            self.stop(coordinator, key="after-source-fault-stop")
            manifest = validate_device_session_directory(self.volume / "recordings" / session_id)
            self.assertEqual(manifest["frames"]["count"], 1)
        finally:
            coordinator.close()


if __name__ == "__main__":
    unittest.main()
