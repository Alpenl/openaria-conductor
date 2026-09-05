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


class FakeNativeCaptureEngine:
    def __init__(self) -> None:
        self.preview_started = False
        self.closed = False
        self.stop_calls = 0
        self.transaction: object | None = None
        self.on_failure: object | None = None
        self.latest_imu: object | None = None
        self.focus_status: dict[str, object] | None = None
        self.focus_commands: list[tuple[int | None, bool | None]] = []

    def start_preview(self) -> None:
        self.preview_started = True

    def start_recording(self, transaction: object, on_failure: object) -> dict[str, object]:
        if self.transaction is not None:
            raise RuntimeError("invalid_state: capture engine is already recording")
        self.transaction = transaction
        self.on_failure = on_failure
        return self.snapshot()

    def stop_recording(self, timeout_seconds: float = 3.0) -> dict[str, object]:
        del timeout_seconds
        self.stop_calls += 1
        self.transaction = None
        return self.snapshot()

    def fail(self, code: str, message: str) -> None:
        callback = self.on_failure
        self.transaction = None
        assert callable(callback)
        callback(code, message)

    def latest_imu_observation(self) -> object | None:
        return self.latest_imu

    def camera_focus_status(self) -> dict[str, object] | None:
        return self.focus_status

    def set_camera_focus(
        self, value: int | None = None, auto_enabled: bool | None = None
    ) -> dict[str, object]:
        self.focus_commands.append((value, auto_enabled))
        if self.focus_status is None:
            raise RuntimeError("camera_focus_unsupported: focus is unavailable")
        return {**self.focus_status, "value": value}

    def close(self, timeout_seconds: float = 5.0) -> dict[str, object]:
        del timeout_seconds
        self.closed = True
        self.transaction = None
        return self.snapshot()

    def snapshot(self) -> dict[str, object]:
        active = self.transaction is not None
        return {
            "running": self.preview_started and not self.closed,
            "recording_present": active,
            "recording_active": active,
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

    def test_native_capture_engine_receives_one_immutable_plan(self) -> None:
        engine = FakeNativeCaptureEngine()
        native_preview = object()
        native_metrics = object()
        created: list[tuple[object, object, object | None]] = []

        def engine_factory(
            plan: object,
            preview: object,
            metrics: object | None,
        ) -> FakeNativeCaptureEngine:
            created.append((plan, preview, metrics))
            return engine

        sources = NativeContinuousCaptureSources(
            "/dev/video0",
            CameraMode(3840, 1080, 60.0, "mjpg"),
            preview=SimpleNamespace(native_owner=native_preview),
            read_timeout=0.25,
            frame_decimation=2,
            buffer_count=8,
            queue_capacity=24,
            metrics=SimpleNamespace(native_owner=native_metrics),
            engine_factory=engine_factory,
        )
        try:
            sources.start_preview()
            sources.start_preview()

            self.assertEqual(len(created), 1)
            plan, preview, metrics = created[0]
            self.assertEqual(
                (
                    plan.device,
                    plan.width,
                    plan.height,
                    plan.fps,
                    plan.encoding,
                    plan.buffer_count,
                    plan.queue_capacity,
                    plan.frame_decimation,
                    plan.read_timeout_seconds,
                    plan.imu_timeout_seconds,
                ),
                ("/dev/video0", 3840, 1080, 60, "mjpg", 8, 24, 2, 0.25, 0.25),
            )
            with self.assertRaises((AttributeError, TypeError)):
                plan.width = 1  # type: ignore[attr-defined]
            self.assertIs(preview, native_preview)
            self.assertIs(metrics, native_metrics)
            self.assertTrue(engine.preview_started)
            self.assertEqual(sources.open_handle_count, 1)
        finally:
            sources.close()

        self.assertTrue(engine.closed)
        self.assertEqual(sources.open_handle_count, 0)

    def test_native_capture_engine_uses_one_transaction_for_all_modes(self) -> None:
        for mode in ("production", "calibration"):
            with self.subTest(mode=mode):
                engine = FakeNativeCaptureEngine()
                transaction = object()
                submitted_frames: list[FrameObservation] = []
                submitted_imu: list[ImuObservation] = []

                def submit_frame(
                    observation: FrameObservation,
                    target: list[FrameObservation] = submitted_frames,
                ) -> bool:
                    target.append(observation)
                    return True

                def submit_imu(
                    observation: ImuObservation,
                    target: list[ImuObservation] = submitted_imu,
                ) -> bool:
                    target.append(observation)
                    return True

                sources = NativeContinuousCaptureSources(
                    "/dev/video0",
                    CameraMode(3840, 1080, 60.0, "mjpg"),
                    preview=SimpleNamespace(native_owner=object()),
                    read_timeout=0.1,
                    engine_factory=lambda plan, preview, metrics, engine=engine: engine,
                )
                try:
                    sources.start(
                        mode=mode,
                        generation_id=str(uuid.uuid4()),
                        submit_frame=submit_frame,
                        submit_imu=submit_imu,
                        on_failure=lambda code, message: self.fail(
                            f"unexpected native failure: {code}: {message}"
                        ),
                        native_recorder=SimpleNamespace(
                            native_recording_transaction=lambda transaction=transaction: transaction
                        ),
                    )
                    self.assertIs(engine.transaction, transaction)
                    self.assertFalse(submitted_frames)
                    self.assertFalse(submitted_imu)
                    self.assertEqual(sources.open_handle_count, 2)

                    sources.stop()
                    self.assertIsNone(engine.transaction)
                    self.assertEqual(sources.open_handle_count, 1)
                finally:
                    sources.close()

                self.assertTrue(engine.closed)
                self.assertEqual(sources.open_handle_count, 0)

    def test_native_capture_engine_requires_started_transaction(self) -> None:
        created: list[bool] = []
        sources = NativeContinuousCaptureSources(
            "/dev/video0",
            CameraMode(3840, 1080, 60.0, "mjpg"),
            preview=SimpleNamespace(native_owner=object()),
            read_timeout=0.1,
            engine_factory=lambda plan, preview, metrics: (
                created.append(True) or FakeNativeCaptureEngine()
            ),
        )
        try:
            for recorder, message in (
                (None, "SessionTransaction"),
                (
                    SimpleNamespace(native_recording_transaction=lambda: None),
                    "尚未启动",
                ),
            ):
                with self.subTest(message=message), self.assertRaisesRegex(RuntimeError, message):
                    sources.start(
                        mode="production",
                        generation_id=str(uuid.uuid4()),
                        submit_frame=lambda observation: True,
                        submit_imu=lambda observation: True,
                        on_failure=lambda code, detail: None,
                        native_recorder=recorder,
                    )
        finally:
            sources.close()

        self.assertFalse(created)
        self.assertEqual(sources.open_handle_count, 0)

    def test_native_capture_engine_releases_failed_recording_and_retries(self) -> None:
        engine = FakeNativeCaptureEngine()
        transactions = iter((object(), object()))
        failures: list[tuple[str, str]] = []
        recorder = SimpleNamespace(native_recording_transaction=lambda: next(transactions))
        sources = NativeContinuousCaptureSources(
            "/dev/video0",
            CameraMode(3840, 1080, 60.0, "mjpg"),
            preview=SimpleNamespace(native_owner=object()),
            read_timeout=0.1,
            engine_factory=lambda plan, preview, metrics: engine,
        )
        try:
            for attempt in range(2):
                sources.start(
                    mode="production",
                    generation_id=str(uuid.uuid4()),
                    submit_frame=lambda observation: True,
                    submit_imu=lambda observation: True,
                    on_failure=lambda code, message: failures.append((code, message)),
                    native_recorder=recorder,
                )
                self.assertEqual(sources.open_handle_count, 2)
                if attempt == 0:
                    engine.fail("source_sequence_gap", "source frame sequence has a gap")
                    self.assertEqual(sources.open_handle_count, 1)
                    self.assertEqual(
                        sources.last_preview_error,
                        ("source_sequence_gap", "source frame sequence has a gap"),
                    )
            sources.stop()
            self.assertEqual(sources.open_handle_count, 1)
        finally:
            sources.close()

        self.assertEqual(
            failures,
            [("source_sequence_gap", "source frame sequence has a gap")],
        )
        self.assertTrue(engine.closed)
        self.assertEqual(sources.open_handle_count, 0)

    def test_native_capture_engine_routes_focus_controls(self) -> None:
        engine = FakeNativeCaptureEngine()
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
        engine.focus_status = focus
        sources = NativeContinuousCaptureSources(
            "/dev/video0",
            CameraMode(3840, 1080, 60.0, "mjpg"),
            preview=SimpleNamespace(native_owner=object()),
            read_timeout=0.1,
            engine_factory=lambda plan, preview, metrics: engine,
        )
        try:
            self.assertEqual(sources.camera_focus_status(), focus)
            self.assertEqual(
                sources.set_camera_focus(value=64, auto_enabled=False),
                {**focus, "value": 64},
            )
            self.assertEqual(engine.focus_commands, [(64, False)])
        finally:
            sources.close()

    def test_native_capture_engine_maps_unsupported_focus(self) -> None:
        engine = FakeNativeCaptureEngine()
        sources = NativeContinuousCaptureSources(
            "/dev/video0",
            CameraMode(3840, 1080, 60.0, "mjpg"),
            preview=SimpleNamespace(native_owner=object()),
            read_timeout=0.1,
            engine_factory=lambda plan, preview, metrics: engine,
        )
        try:
            self.assertIsNone(sources.camera_focus_status())
            with self.assertRaises(CameraError) as unsupported:
                sources.set_camera_focus(value=42)
            self.assertEqual(unsupported.exception.code, "camera_focus_unsupported")
            self.assertFalse(unsupported.exception.retryable)
        finally:
            sources.close()

    def test_native_capture_engine_rebuilds_terminal_preview_after_hotplug(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            device = Path(directory) / "video0"
            engines = iter((FakeNativeCaptureEngine(), FakeNativeCaptureEngine()))
            created: list[FakeNativeCaptureEngine] = []

            def engine_factory(
                plan: object,
                preview: object,
                metrics: object | None,
            ) -> FakeNativeCaptureEngine:
                del plan, preview, metrics
                engine = next(engines)
                created.append(engine)
                return engine

            sources = NativeContinuousCaptureSources(
                str(device),
                CameraMode(3840, 1080, 60.0, "mjpg"),
                preview=SimpleNamespace(native_owner=object()),
                read_timeout=0.1,
                engine_factory=engine_factory,
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
                sources.start_preview()
                created[0].preview_started = False
                device.unlink()
                self.assertEqual(
                    sources.camera_connection_status()["state"],
                    "disconnected",
                )
                device.touch()
                sources.start_preview()

                self.assertEqual(len(created), 2)
                self.assertTrue(created[0].closed)
                self.assertTrue(created[1].preview_started)
                self.assertEqual(sources.open_handle_count, 1)
            finally:
                sources.close()

    def test_native_capture_engine_exposes_latest_imu_snapshot(self) -> None:
        engine = FakeNativeCaptureEngine()
        engine.latest_imu = native_imu_observation(10)
        sources = NativeContinuousCaptureSources(
            "/dev/video0",
            CameraMode(3840, 1080, 60.0, "mjpg"),
            preview=SimpleNamespace(native_owner=object()),
            read_timeout=0.1,
            engine_factory=lambda plan, preview, metrics: engine,
        )
        try:
            self.assertIsNone(sources.latest_imu_observation())
            sources.start_preview()
            observation = sources.latest_imu_observation()
            self.assertIsNotNone(observation)
            assert observation is not None
            self.assertEqual([sample.sequence for sample in observation.samples], [10, 11])
        finally:
            sources.close()

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
