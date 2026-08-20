"""Unified benchmark runner for fixture and target-hardware workloads."""

from __future__ import annotations

import hashlib
import platform
import resource
import sys
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from rp_ylx import __commit__, __version__
from rp_ylx.api.downloads import iter_device_session_v1_artifacts
from rp_ylx.api.mock_device import MockDevice
from rp_ylx.api.preview import LatestPreviewBuffer, PreviewFrameUnavailable
from rp_ylx.camera import CameraController, CameraDescriptor, CameraMode
from rp_ylx.camera.models import CameraBackend, CameraStream, StereoFrame
from rp_ylx.camera.v4l2 import (
    V4L2CameraStream,
    split_sbs_mjpeg,
    v4l2_production_stream_factory,
)
from rp_ylx.hardware import collect_hardware_facts
from rp_ylx.hardware.target import RDK_X5_BOARD_ID, YLX_2UQ2_CAMERA_ID
from rp_ylx.imu import NativeImuCollector
from rp_ylx.native import (
    NativeModuleError,
    create_native_splitter,
    native_capabilities,
)
from rp_ylx.performance.metrics import PerformanceMetrics
from rp_ylx.performance.report import validate_performance_report
from rp_ylx.recording import (
    DeviceSessionConfig,
    DeviceSessionRecorder,
    NativeContinuousCaptureSources,
    SessionPlan,
    StorageStatus,
    uuid7,
)

BenchmarkKind = Literal["fixed_trace", "preview", "recording", "concurrent"]
BenchmarkAdapter = Literal["python", "rust"]
TARGET_MODE = CameraMode(3840, 1080, 60.0, "mjpg")


class BenchmarkError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True, slots=True)
class BenchmarkConfig:
    kind: BenchmarkKind
    duration_seconds: float
    round: int
    wheel_sha256: str
    device: Path = Path("/dev/video0")
    trace: Path | None = None
    recording_root: Path | None = None
    adapter: BenchmarkAdapter = "python"

    def __post_init__(self) -> None:
        if self.kind not in {"fixed_trace", "preview", "recording", "concurrent"}:
            raise ValueError("benchmark kind is invalid")
        if self.adapter not in {"python", "rust"}:
            raise ValueError("benchmark adapter is invalid")
        if self.duration_seconds <= 0 or self.round <= 0:
            raise ValueError("duration and round must be positive")
        if len(self.wheel_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.wheel_sha256
        ):
            raise ValueError("wheel_sha256 must be 64 lowercase hex characters")
        if self.kind == "fixed_trace" and self.trace is None:
            raise ValueError("fixed_trace requires an explicit trace")
        if self.kind != "fixed_trace" and self.trace is not None:
            raise ValueError("hardware workloads cannot use a fixture trace")
        if self.kind in {"recording", "concurrent"} and self.recording_root is None:
            raise ValueError("recording workloads require recording_root")


class _ExactCameraBackend(CameraBackend):
    def __init__(
        self,
        device: Path,
        metrics: PerformanceMetrics,
        adapter: BenchmarkAdapter,
    ) -> None:
        self._device = device
        self._metrics = metrics
        self._adapter = adapter
        self._descriptor = CameraDescriptor(
            stable_id=f"benchmark:{device}",
            node=str(device),
            name="YLX 2UQ2",
            modes=(TARGET_MODE,),
        )

    def discover(self) -> tuple[CameraDescriptor, ...]:
        return (self._descriptor,)

    def open(self, descriptor: CameraDescriptor, mode: CameraMode) -> CameraStream:
        if descriptor != self._descriptor or mode != TARGET_MODE:
            raise BenchmarkError("target_mismatch", "benchmark camera selection changed")
        if self._adapter == "rust":
            return v4l2_production_stream_factory(
                self._device,
                mode,
                metrics=self._metrics,
            )
        return V4L2CameraStream(self._device, mode, metrics=self._metrics)


def _target_environment(config: BenchmarkConfig) -> tuple[str, dict[str, object]]:
    if config.kind == "fixed_trace":
        return "fixture", {"board": "not_applicable", "camera": "fixture", "supported": False}
    facts = collect_hardware_facts(storage_path=config.recording_root or config.device.parent)
    target = facts["target"]
    expected = {
        "board": RDK_X5_BOARD_ID,
        "camera": YLX_2UQ2_CAMERA_ID,
        "supported": True,
    }
    selected = {name: target.get(name) for name in expected}
    if selected != expected:
        raise BenchmarkError(
            "unsupported_target",
            "hardware benchmark requires D-Robotics RDK X5 V1.0 + YLX 2UQ2",
        )
    if config.device != Path("/dev/video0"):
        raise BenchmarkError("wrong_capture_node", "hardware benchmark must use /dev/video0")
    return "hardware", expected


def _rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


def _new_recorder(config: BenchmarkConfig, metrics: PerformanceMetrics) -> DeviceSessionRecorder:
    assert config.recording_root is not None
    config.recording_root.mkdir(parents=True, exist_ok=True)
    revision = 0

    def allocate_revision() -> int:
        nonlocal revision
        revision += 1
        return revision

    return DeviceSessionRecorder(
        config.recording_root,
        DeviceSessionConfig(
            device_id=str(uuid.uuid4()),
            device_label="YLX-00000000",
            hardware_fingerprint="sha256:" + "0" * 64,
            platform="D-Robotics RDK X5 V1.0 + YLX 2UQ2",
            software_version=__version__,
            commit=__commit__,
            width=3840,
            height=1080,
            sensor_fps=60.0,
        ),
        SessionPlan(
            session_id=uuid7(),
            volume_id=str(uuid.uuid4()),
            generation_id=str(uuid.uuid4()),
            capture_mode="production",
            display_name=f"性能测试 {config.kind} 第 {config.round} 轮",
            take_id=uuid7(),
            take_sequence=1,
            continuation_of=None,
        ),
        authority_epoch=str(uuid.uuid4()),
        allocate_revision=allocate_revision,
        storage_status=lambda: StorageStatus(None, True),
        checkpoint_interval=1.0,
        metrics=metrics,
    )


def _preview_pair_for_frame(frame: StereoFrame) -> tuple[bytes, bytes]:
    left = frame.left or frame.raw_side_by_side
    right = frame.right or frame.raw_side_by_side
    if not left or not right:
        raise BenchmarkError("preview_payload_unavailable", "benchmark preview payload is empty")
    return left, right


def _manifest_frame_count(manifest: dict[str, object]) -> int:
    frames = manifest.get("frames")
    if not isinstance(frames, dict):
        raise BenchmarkError("invalid_session_manifest", "sealed session is missing frame count")
    count = frames.get("count")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise BenchmarkError("invalid_session_manifest", "sealed session frame count is invalid")
    return count


def _manifest_dropped_frames(manifest: dict[str, object]) -> int:
    integrity = manifest.get("integrity")
    if not isinstance(integrity, dict):
        raise BenchmarkError("invalid_session_manifest", "sealed session is missing integrity")
    dropped = integrity.get("dropped_frames")
    if isinstance(dropped, bool) or not isinstance(dropped, int) or dropped < 0:
        raise BenchmarkError("invalid_session_manifest", "sealed session drop count is invalid")
    return dropped


def _run_native_continuous_hardware(
    config: BenchmarkConfig, metrics: PerformanceMetrics
) -> tuple[int, int, int]:
    """Run Rust recording workloads through the production continuous runtime."""

    if config.kind not in {"recording", "concurrent"}:
        raise BenchmarkError(
            "unsupported_native_continuous_workload",
            "native continuous benchmark only supports recording/concurrent",
        )
    assert config.recording_root is not None
    preview = LatestPreviewBuffer(stream_fps=15)
    sources = NativeContinuousCaptureSources(
        str(config.device),
        lambda: NativeImuCollector(config.device),
        TARGET_MODE,
        preview=preview,
        metrics=metrics,
    )
    recorder = _new_recorder(config, metrics)
    failures: list[tuple[str, str]] = []

    def on_failure(code: str, message: str) -> None:
        failures.append((code, message))

    try:
        recorder.start()
        sources.start(
            mode="production",
            generation_id=recorder._plan.generation_id,
            submit_frame=recorder.submit_frame,
            submit_imu=recorder.submit_imu,
            on_failure=on_failure,
            native_recorder=recorder,
        )
        deadline = time.monotonic() + config.duration_seconds
        while time.monotonic() < deadline:
            if failures:
                code, message = failures[0]
                raise BenchmarkError(code, message)
            if config.kind == "concurrent":
                with suppress(PreviewFrameUnavailable):
                    preview.jpeg_response()
            time.sleep(1 / 15)
        sources.stop()
        if failures:
            code, message = failures[0]
            raise BenchmarkError(code, message)
        sealed = recorder.stop()
        frames_output = _manifest_frame_count(sealed.manifest)
        dropped = _manifest_dropped_frames(sealed.manifest)
        bytes_written = sum(
            int(artifact["bytes"]) for artifact in iter_device_session_v1_artifacts(sealed.manifest)
        )
        return frames_output + dropped, frames_output, bytes_written
    except BaseException:
        with suppress(BaseException):
            sources.stop()
        if recorder.state in {"recording", "finalizing"}:
            with suppress(BaseException):
                recorder.fail("benchmark_failed", "性能测试未完成")
        raise
    finally:
        sources.close()


def _run_trace(config: BenchmarkConfig, metrics: PerformanceMetrics) -> tuple[int, int, int]:
    assert config.trace is not None
    payload = config.trace.read_bytes()
    if not payload:
        raise BenchmarkError("empty_trace", "fixed trace cannot be empty")
    splitter = None
    try:
        if config.adapter == "rust":
            try:
                splitter = create_native_splitter()
            except NativeModuleError as exc:
                raise BenchmarkError(exc.code, exc.message) from exc
        deadline = time.monotonic() + config.duration_seconds
        frames = 0
        while time.monotonic() < deadline:
            started = metrics.start()
            if splitter is None:
                left, right = split_sbs_mjpeg(payload, TARGET_MODE.width, TARGET_MODE.height)
            else:
                left, right = splitter.split(payload, TARGET_MODE.width, TARGET_MODE.height)
            metrics.finish("jpeg_split", started)
            metrics.record_copy("left_output", len(left))
            metrics.record_copy("right_output", len(right))
            frames += 1
    finally:
        if splitter is not None:
            splitter.close()
    return frames, frames, 0


def _run_hardware(config: BenchmarkConfig, metrics: PerformanceMetrics) -> tuple[int, int, int]:
    if config.adapter == "rust" and config.kind in {"recording", "concurrent"}:
        return _run_native_continuous_hardware(config, metrics)

    controller = CameraController(
        _ExactCameraBackend(config.device, metrics, config.adapter),
        metrics=metrics,
    )
    preview = MockDevice() if config.kind in {"preview", "concurrent"} else None
    recorder = (
        _new_recorder(config, metrics) if config.kind in {"recording", "concurrent"} else None
    )
    frames_input = 0
    frames_output = 0
    bytes_written = 0
    try:
        controller.open(TARGET_MODE)
        controller.start()
        if recorder is not None:
            recorder.start()
        deadline = time.monotonic() + config.duration_seconds
        while time.monotonic() < deadline:
            observation = controller.read(timeout=2.0)
            frames_input += observation.dropped_before + 1
            accepted = True
            if preview is not None:
                frame = observation.frame
                left, right = _preview_pair_for_frame(frame)
                started = metrics.start()
                preview.publish_preview_pair(
                    left,
                    right,
                    source_sequence=frame.source_sequence,
                    capture_monotonic_ns=frame.host_monotonic_ns,
                )
                metrics.finish("preview_publish", started)
            if recorder is not None:
                accepted = recorder.submit_frame(observation)
            if accepted:
                frames_output += 1
        if recorder is not None:
            sealed = recorder.stop()
            bytes_written = sum(
                int(artifact["bytes"])
                for artifact in iter_device_session_v1_artifacts(sealed.manifest)
            )
    except BaseException:
        if recorder is not None:
            recorder.fail("benchmark_failed", "性能测试未完成")
        raise
    finally:
        controller.close()
    return frames_input, frames_output, bytes_written


def run_benchmark(config: BenchmarkConfig) -> dict[str, object]:
    """Run one workload and return a strict ``ylx.performance-report.v0`` report."""

    evidence_kind, target = _target_environment(config)
    if len(__commit__) != 40 or any(
        character not in "0123456789abcdef" for character in __commit__
    ):
        raise BenchmarkError(
            "unbound_distribution",
            "benchmark must run from a wheel bound to an exact 40-character commit",
        )
    if config.adapter == "python":
        native_identity: dict[str, object] = {
            "adapter": "python",
            "module_available": False,
            "module_version": None,
            "abi": None,
        }
    else:
        try:
            capabilities = native_capabilities()
        except NativeModuleError as exc:
            raise BenchmarkError(exc.code, exc.message) from exc
        required = "turbojpeg_split" if config.kind == "fixed_trace" else "native_camera"
        if not capabilities.module_available or required not in capabilities.features:
            raise BenchmarkError(
                "native_adapter_unavailable",
                f"Rust {config.kind} benchmark requires {required}",
            )
        native_identity = capabilities.as_report_identity()
    metrics = PerformanceMetrics()
    cpu_started = time.process_time_ns()
    wall_started = time.monotonic_ns()
    if config.kind == "fixed_trace":
        frames_input, frames_output, bytes_written = _run_trace(config, metrics)
    else:
        frames_input, frames_output, bytes_written = _run_hardware(config, metrics)
    duration_ns = max(1, time.monotonic_ns() - wall_started)
    cpu_time_ns = max(0, time.process_time_ns() - cpu_started)
    snapshot = metrics.snapshot()
    application_drop = snapshot.loss["queue_rejected"] + snapshot.loss["write_failure"]
    unknown_gap = max(
        0,
        frames_input - frames_output - snapshot.loss["source_gap"] - application_drop,
    )
    report: dict[str, object] = {
        "format": "ylx.performance-report.v0",
        "observed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "identity": {
            "commit": __commit__,
            "wheel_sha256": config.wheel_sha256,
            "software_version": __version__,
        },
        "environment": {
            "evidence_kind": evidence_kind,
            "machine": platform.machine(),
            "kernel": platform.release(),
            "python": platform.python_version(),
            "target": target,
        },
        "workload": {
            "kind": config.kind,
            "round": config.round,
            "duration_ns": duration_ns,
            "frames_input": frames_input,
            "frames_output": frames_output,
            "mode": {"width": 3840, "height": 1080, "fps": 60, "encoding": "mjpg"},
        },
        "native": native_identity,
        "stages": list(snapshot.stages),
        "copies": list(snapshot.copies),
        "queue": snapshot.queue,
        "loss": {
            "source_gap": snapshot.loss["source_gap"],
            "application_drop": application_drop,
            "unknown_gap": snapshot.loss["unknown_gap"] + unknown_gap,
        },
        "resources": {
            "cpu_time_ns": cpu_time_ns,
            "rss_peak_bytes": _rss_bytes(),
            "bytes_written": bytes_written,
        },
        "result": {"effective_fps": frames_output * 1_000_000_000 / duration_ns},
    }
    return validate_performance_report(report)


def trace_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
