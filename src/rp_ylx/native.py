"""经过校验的进程内 Rust 数据面能力探针。"""

from __future__ import annotations

import importlib
import threading
from dataclasses import dataclass
from types import ModuleType
from typing import Protocol, cast

NATIVE_MODULE = "rp_ylx._native"
SUPPORTED_NATIVE_ABI = 4


class NativeModuleError(RuntimeError):
    """原生模块存在但不能满足稳定能力 interface。"""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True, slots=True)
class NativeCapabilities:
    module_available: bool
    module_version: str | None
    abi: int | None
    features: tuple[str, ...]

    @property
    def adapter(self) -> str:
        return "rust" if self.module_available else "python"

    def as_dict(self) -> dict[str, object]:
        return {
            "adapter": self.adapter,
            "module_available": self.module_available,
            "module_version": self.module_version,
            "abi": self.abi,
            "features": list(self.features),
        }

    def as_report_identity(self) -> dict[str, object]:
        return {
            "adapter": self.adapter,
            "module_available": self.module_available,
            "module_version": self.module_version,
            "abi": self.abi,
        }


def _validate_capabilities(module: ModuleType) -> NativeCapabilities:
    try:
        raw = module.capabilities()
    except Exception as exc:
        raise NativeModuleError("native_probe_failed", f"原生能力探针失败：{exc}") from exc
    if not isinstance(raw, dict) or set(raw) != {"module_version", "abi", "features"}:
        raise NativeModuleError("invalid_native_capabilities", "原生能力字段不完整或含未知字段")

    version = raw["module_version"]
    abi = raw["abi"]
    features = raw["features"]
    if not isinstance(version, str) or not version or len(version) > 64:
        raise NativeModuleError("invalid_native_capabilities", "原生模块版本无效")
    if isinstance(abi, bool) or not isinstance(abi, int) or abi <= 0:
        raise NativeModuleError("invalid_native_capabilities", "原生 ABI 必须是正整数")
    if abi != SUPPORTED_NATIVE_ABI:
        raise NativeModuleError(
            "unsupported_native_abi",
            f"原生 ABI 为 {abi}，Python adapter 只支持 {SUPPORTED_NATIVE_ABI}",
        )
    if (
        not isinstance(features, (list, tuple))
        or not features
        or any(not isinstance(item, str) or not item for item in features)
        or len(set(features)) != len(features)
    ):
        raise NativeModuleError("invalid_native_capabilities", "原生能力列表无效")
    if "capability_probe" not in features:
        raise NativeModuleError("missing_native_capability", "原生模块缺少 capability_probe")
    return NativeCapabilities(True, version, abi, tuple(features))


def native_capabilities() -> NativeCapabilities:
    """返回可用的 Rust 能力，源码兼容模式下明确返回 Python adapter。"""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except ModuleNotFoundError as exc:
        if exc.name != NATIVE_MODULE:
            raise NativeModuleError(
                "native_dependency_missing", f"原生模块依赖缺失：{exc.name}"
            ) from exc
        return NativeCapabilities(False, None, None, ())
    except ImportError as exc:
        raise NativeModuleError("native_import_failed", f"原生模块加载失败：{exc}") from exc
    return _validate_capabilities(module)


class NativeSplitter(Protocol):
    def split(self, payload: bytes, width: int, height: int) -> tuple[bytes, bytes]: ...

    def close(self) -> None: ...


class NativeCamera(Protocol):
    def start(self) -> None: ...

    def read(self, timeout_seconds: float) -> tuple[int, int, int, bytes, bytes, bytes]: ...

    def stop(self) -> None: ...

    def close(self) -> None: ...

    def stats(self) -> dict[str, int]: ...

    def camera_focus_status(self) -> dict[str, object] | None: ...

    def set_camera_focus(
        self,
        value: int | None = None,
        auto_enabled: bool | None = None,
    ) -> dict[str, object]: ...


class NativeAudioRecorder(Protocol):
    def start(self) -> None: ...

    def snapshot(self) -> dict[str, object]: ...

    def stop(self, timeout_seconds: float = 5.0) -> dict[str, object]: ...

    def abort(self) -> None: ...

    def close(self) -> None: ...


class NativeActiveTakeWriter(Protocol):
    def reserve_frame(
        self,
        source_sequence: int,
        host_monotonic_ns: int,
        source_gap: int,
    ) -> dict[str, object]: ...

    def finish_frame(
        self,
        record_sequence: int,
        source_sequence: int,
        host_monotonic_ns: int,
        bytes_written: int,
    ) -> dict[str, object]: ...

    def reject_frame(
        self,
        record_sequence: int,
        source_sequence: int,
        host_monotonic_ns: int,
        at_time_seconds: float,
    ) -> dict[str, object]: ...

    def snapshot(self) -> dict[str, object]: ...

    def finish(self) -> dict[str, object]: ...


class NativeImuCollector(Protocol):
    def read(self, timeout_seconds: float = 1.0) -> dict[str, object]: ...

    def latest_observation(self) -> dict[str, object] | None: ...

    def close(self) -> None: ...

    def unit(self) -> int: ...


class NativeRecordingSink(Protocol):
    def write_split_frame_index(
        self,
        frame: int,
        source_sequence: int,
        host_monotonic_ns: int,
        segment_index: int,
        segment_frame: int,
    ) -> int: ...

    def write_imu_sample(
        self,
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
    ) -> int: ...

    def write_imu_observation(self, observation: object) -> dict[str, int]: ...

    def snapshot(self) -> dict[str, object]: ...

    def flush_and_close(self) -> dict[str, object]: ...

    def close(self) -> None: ...


class NativeContinuousCaptureRuntime(Protocol):
    def start_preview(self) -> None: ...

    def start_recording(
        self,
        submit_frame: object,
        on_failure: object,
        imu: object | None = None,
        submit_imu: object | None = None,
        imu_timeout_seconds: float = 1.0,
    ) -> dict[str, object]: ...

    def start_recording_split_sink(
        self,
        active_take: NativeActiveTakeWriter,
        sink: NativeRecordingSink,
        encoder: NativeStereoEncoderProcess,
        segment_planner: NativeRecordingSegmentPlanner,
        recording_start_monotonic_ns: int,
        on_failure: object,
        imu: object | None = None,
        imu_timeout_seconds: float = 1.0,
    ) -> dict[str, object]: ...

    def stop_recording(self, timeout_seconds: float = 3.0) -> dict[str, object]: ...

    def close(self, timeout_seconds: float = 5.0) -> dict[str, object]: ...

    def snapshot(self) -> dict[str, object]: ...


class NativeRecordingSegmentPlanner(Protocol):
    def next_frame(self, record_sequence: int, elapsed_seconds: float) -> dict[str, object]: ...

    def register_segment(
        self,
        index: int,
        start_ordinal: int,
        end_ordinal: int,
    ) -> dict[str, object]: ...

    def finish(
        self,
        submitted_frames: int,
        frame_domain: int,
        duration_seconds: float,
    ) -> dict[str, object]: ...

    def boundary(self, ordinal: int, duration_seconds: float) -> dict[str, object]: ...

    def snapshot(self) -> dict[str, object]: ...


class NativeStereoEncoderProcess(Protocol):
    def start(self) -> None: ...

    def submit(self, jpeg: bytes) -> int: ...

    def finish(self, timeout_seconds: float = 30.0) -> list[dict[str, object]]: ...

    def abort(self) -> None: ...

    def segments(self) -> list[dict[str, object]]: ...

    def stats(self) -> dict[str, object]: ...

    def submitted_frames(self) -> int: ...


class NativeSessionIo(Protocol):
    def hash_file(self, path: str) -> dict[str, object]: ...

    def finalize_artifact(
        self,
        path: str,
        expected_bytes: int | None = None,
    ) -> dict[str, object]: ...

    def verify_fd(
        self,
        descriptor: int,
        expected_bytes: int,
        expected_sha256: str,
    ) -> dict[str, object]: ...

    def sendfile(
        self,
        output_descriptor: int,
        input_descriptor: int,
        offset: int,
        length: int,
    ) -> int: ...

    def write_encoder_frame(self, descriptor: int, jpeg: bytes) -> int: ...

    def open_relative_regular(self, root_descriptor: int, relative_path: str) -> int: ...

    def read_bounded_fd(self, descriptor: int, maximum_bytes: int) -> bytes: ...

    def device_session_v1_artifacts(
        self,
        manifest: bytes,
        session_id: str,
    ) -> list[dict[str, object]]: ...

    def device_session_v1_artifact(
        self,
        manifest: bytes,
        session_id: str,
        artifact_id: str,
    ) -> dict[str, object] | None: ...

    def device_session_v1_summary(
        self,
        manifest: bytes,
        session_id: str,
    ) -> dict[str, object]: ...

    def seal_device_session_v1(
        self,
        partial_path: str,
        final_path: str,
        session_id: str,
        manifest: bytes,
        expected_identities: dict[str, tuple[int, int, int, int]],
        control_names: list[str] | None = None,
    ) -> dict[str, object]: ...


class NativeMultipartPreview(Protocol):
    def __iter__(self) -> NativeMultipartPreview: ...

    def __next__(self) -> bytes: ...

    def close(self) -> None: ...


class NativePreviewBuffer(Protocol):
    def publish(self, jpeg: bytes) -> int: ...

    def clear(self) -> None: ...

    def jpeg(self) -> tuple[int, bytes]: ...

    def multipart_stream(self, fps: int | None = None) -> NativeMultipartPreview: ...

    def wake_streams(self) -> None: ...


class NativePerformanceMetrics(Protocol):
    def record_stage(self, name: str, elapsed_ns: int) -> None: ...

    def record_copy(self, name: str, size: int, count: int = 1) -> None: ...

    def change_payload(self, name: str, count_delta: int, bytes_delta: int) -> None: ...

    def observe_queue(
        self,
        depth: int,
        capacity: int,
        rejected: int = 0,
        peak_depth: int | None = None,
    ) -> None: ...

    def record_loss(self, kind: str, count: int = 1) -> None: ...

    def snapshot(self) -> dict[str, object]: ...


_SESSION_IO_LOCK = threading.Lock()
_SESSION_IO: NativeSessionIo | None = None
_SESSION_IO_UNAVAILABLE = False


def _require_native_feature(
    feature: str,
    unavailable_code: str,
    load_subject: str,
    missing_message: str,
) -> ModuleType:
    try:
        module = importlib.import_module(NATIVE_MODULE)
    except ImportError as exc:
        raise NativeModuleError(unavailable_code, f"无法加载{load_subject}：{exc}") from exc
    if feature not in _validate_capabilities(module).features:
        raise NativeModuleError(unavailable_code, missing_message)
    return module


def _parse_native_error(exc: Exception, fallback_code: str) -> NativeModuleError:
    raw = str(exc)
    code, separator, message = raw.partition(": ")
    if not separator or not code.replace("_", "").isalnum():
        code, message = fallback_code, raw
    return NativeModuleError(code, message)


def create_native_splitter() -> NativeSplitter:
    """Create the explicit Rust/TurboJPEG splitter or fail without fallback."""

    module = _require_native_feature(
        "turbojpeg_split",
        "native_splitter_unavailable",
        "原生拆分模块",
        "原生模块缺少可用的 TurboJPEG 拆分能力",
    )
    try:
        splitter = module.NativeSplitter()
    except Exception as exc:
        raise NativeModuleError(
            "native_splitter_init_failed", f"原生拆分器初始化失败：{exc}"
        ) from exc
    return splitter


def create_native_camera(
    device: str,
    width: int,
    height: int,
    fps: int,
    encoding: str,
    *,
    buffer_count: int = 4,
    queue_capacity: int = 4,
    split_eyes: bool = True,
) -> NativeCamera:
    """Create the complete Rust V4L2/TurboJPEG camera or fail explicitly."""

    module = _require_native_feature(
        "native_camera",
        "native_camera_unavailable",
        "原生相机模块",
        "原生模块缺少完整 V4L2/TurboJPEG 相机能力",
    )
    try:
        return module.NativeCameraStream(
            device,
            width,
            height,
            fps,
            encoding,
            buffer_count,
            queue_capacity,
            split_eyes,
        )
    except Exception as exc:
        raise _parse_native_error(exc, "native_camera_init_failed") from exc


def _validate_native_focus_status(status: object) -> dict[str, object] | None:
    if status is not None and (
        not isinstance(status, dict)
        or set(status)
        != {
            "schema",
            "value",
            "minimum",
            "maximum",
            "step",
            "default",
            "auto_supported",
            "auto_enabled",
        }
        or status["schema"] != "ylx.camera-focus.v1"
        or any(
            isinstance(status[key], bool) or not isinstance(status[key], int)
            for key in ("value", "minimum", "maximum", "step", "default")
        )
        or status["step"] <= 0
        or status["minimum"] > status["maximum"]
        or not status["minimum"] <= status["value"] <= status["maximum"]
        or (status["value"] - status["minimum"]) % status["step"] != 0
        or not status["minimum"] <= status["default"] <= status["maximum"]
        or type(status["auto_supported"]) is not bool
        or (status["auto_enabled"] is not None and type(status["auto_enabled"]) is not bool)
        or (not status["auto_supported"] and status["auto_enabled"] is not None)
    ):
        raise NativeModuleError("invalid_native_focus_status", "原生焦距状态无效")
    return None if status is None else cast(dict[str, object], status)


def native_camera_focus_status(device: str) -> dict[str, object] | None:
    """Read V4L2 focus controls through the Rust native control path."""

    module = _require_native_feature(
        "v4l2_focus_control",
        "native_focus_unavailable",
        "原生相机控制模块",
        "原生模块缺少 V4L2 焦距控制能力",
    )
    try:
        status = module.v4l2_focus_status(device)
    except Exception as exc:
        raise _parse_native_error(exc, "native_focus_status_failed") from exc
    return _validate_native_focus_status(status)


def native_stream_camera_focus_status(camera: NativeCamera) -> dict[str, object] | None:
    """Read focus controls through an already-open native camera stream."""

    try:
        status = camera.camera_focus_status()
    except Exception as exc:
        raise _parse_native_error(exc, "native_focus_status_failed") from exc
    return _validate_native_focus_status(status)


def set_native_stream_camera_focus(
    camera: NativeCamera,
    *,
    value: int | None = None,
    auto_enabled: bool | None = None,
) -> dict[str, object]:
    """Set focus controls through an already-open native camera stream."""

    if value is None and auto_enabled is None:
        raise NativeModuleError("invalid_camera_focus", "焦距请求必须包含 value 或 auto_enabled")
    if value is not None and type(value) is not int:
        raise NativeModuleError("invalid_camera_focus", "焦距 value 必须是整数")
    if auto_enabled is not None and type(auto_enabled) is not bool:
        raise NativeModuleError("invalid_camera_focus", "auto_enabled 必须是布尔值")
    try:
        status = camera.set_camera_focus(value=value, auto_enabled=auto_enabled)
    except Exception as exc:
        raise _parse_native_error(exc, "native_focus_set_failed") from exc
    validated = _validate_native_focus_status(status)
    if validated is None:
        raise NativeModuleError("camera_focus_unsupported", "相机没有可读取的焦距控制")
    return validated


def set_native_camera_focus(
    device: str,
    *,
    value: int | None = None,
    auto_enabled: bool | None = None,
) -> dict[str, object]:
    """Set V4L2 focus controls through the Rust native control path."""

    if value is None and auto_enabled is None:
        raise NativeModuleError("invalid_camera_focus", "焦距请求必须包含 value 或 auto_enabled")
    if value is not None and type(value) is not int:
        raise NativeModuleError("invalid_camera_focus", "焦距 value 必须是整数")
    if auto_enabled is not None and type(auto_enabled) is not bool:
        raise NativeModuleError("invalid_camera_focus", "auto_enabled 必须是布尔值")
    module = _require_native_feature(
        "v4l2_focus_control",
        "native_focus_unavailable",
        "原生相机控制模块",
        "原生模块缺少 V4L2 焦距控制能力",
    )
    try:
        module.v4l2_set_focus(device, value, auto_enabled)
    except Exception as exc:
        raise _parse_native_error(exc, "native_focus_set_failed") from exc
    try:
        status = module.v4l2_focus_status(device)
    except Exception as exc:
        raise _parse_native_error(exc, "native_focus_status_failed") from exc
    status = _validate_native_focus_status(status)
    if status is None:
        raise NativeModuleError("camera_focus_unsupported", "相机没有可读取的焦距控制")
    return status


def create_native_audio_recorder(
    session_root: str,
    *,
    device: str = "hw:0,0",
    sample_rate_hz: int = 48_000,
    channels: int = 2,
    segment_seconds: float = 30.0,
) -> NativeAudioRecorder:
    """Create the Rust/ALSA recorder or fail explicitly."""

    module = _require_native_feature(
        "native_audio",
        "native_audio_unavailable",
        "原生音频模块",
        "原生模块缺少 ALSA 音频录制能力",
    )
    try:
        return module.NativeAudioRecorder(
            session_root,
            device,
            sample_rate_hz,
            channels,
            segment_seconds,
        )
    except Exception as exc:
        raise _parse_native_error(exc, "native_audio_init_failed") from exc


def create_native_active_take_writer(session_id: str) -> NativeActiveTakeWriter:
    """Create the Rust active-take frame/domain/drop owner or fail explicitly."""

    module = _require_native_feature(
        "active_take_writer",
        "active_take_writer_unavailable",
        "原生 active take 模块",
        "原生模块缺少 active take 写入状态能力",
    )
    try:
        return module.NativeActiveTakeWriter(session_id)
    except Exception as exc:
        raise _parse_native_error(exc, "active_take_writer_init_failed") from exc


def create_native_imu_collector(
    device: str,
    *,
    unit: int | None = None,
    selector: int = 1,
    stale_poll_interval: float = 0.001,
) -> NativeImuCollector:
    """Create the Rust UVC XU IMU collector or fail explicitly."""

    module = _require_native_feature(
        "native_imu",
        "native_imu_unavailable",
        "原生 IMU 模块",
        "原生模块缺少 UVC XU IMU 采集能力",
    )
    try:
        return module.NativeImuCollector(device, unit, selector, stale_poll_interval)
    except Exception as exc:
        raise _parse_native_error(exc, "native_imu_init_failed") from exc


def create_native_recording_sink(
    session_root: str,
    session_id: str,
) -> NativeRecordingSink:
    """Create the Rust recording event sink or fail explicitly."""

    module = _require_native_feature(
        "recording_sink",
        "native_recording_sink_unavailable",
        "原生录制写入模块",
        "原生模块缺少录制写入热路径能力",
    )
    try:
        # Preserve the ABI-4 constructor arity while exposing only the supported layout.
        return module.NativeRecordingSink(session_root, session_id, True)
    except Exception as exc:
        raise _parse_native_error(exc, "native_recording_sink_init_failed") from exc


def create_native_continuous_capture_runtime(
    camera: NativeCamera,
    preview: NativePreviewBuffer,
    frame_decimation: int,
    *,
    read_timeout_seconds: float = 2.0,
    metrics: NativePerformanceMetrics | None = None,
) -> NativeContinuousCaptureRuntime:
    """Create the Rust continuous capture runtime or fail explicitly."""

    module = _require_native_feature(
        "continuous_capture_runtime",
        "native_continuous_capture_runtime_unavailable",
        "原生连续采集 runtime 模块",
        "原生模块缺少连续采集 runtime 能力",
    )
    try:
        if metrics is None:
            return module.NativeContinuousCaptureRuntime(
                camera,
                preview,
                frame_decimation,
                read_timeout_seconds,
            )
        return module.NativeContinuousCaptureRuntime(
            camera,
            preview,
            frame_decimation,
            read_timeout_seconds,
            metrics,
        )
    except Exception as exc:
        raise _parse_native_error(exc, "native_continuous_capture_runtime_init_failed") from exc


def create_native_recording_segment_planner(segment_frames: int) -> NativeRecordingSegmentPlanner:
    """Create the Rust recording segment planner or fail explicitly."""

    module = _require_native_feature(
        "recording_segment_planner",
        "native_recording_segment_planner_unavailable",
        "原生录制分段规划模块",
        "原生模块缺少录制分段规划热路径能力",
    )
    try:
        return module.NativeRecordingSegmentPlanner(segment_frames)
    except Exception as exc:
        raise _parse_native_error(exc, "native_recording_segment_planner_init_failed") from exc


def create_native_stereo_encoder_process(
    out_dir: str,
    executable: str,
    *,
    width: int,
    height: int,
    fps: int,
    bitrate_kbps: int = 8192,
    segment_frames: int = 900,
    path_prefix: str = "video/",
) -> NativeStereoEncoderProcess:
    """Create the Rust-owned stereo encoder helper process or fail explicitly."""

    module = _require_native_feature(
        "stereo_encoder_process",
        "native_stereo_encoder_process_unavailable",
        "原生编码助手进程模块",
        "原生模块缺少编码助手进程 owner 能力",
    )
    try:
        return module.NativeStereoEncoderProcess(
            out_dir,
            executable,
            width,
            height,
            fps,
            bitrate_kbps,
            segment_frames,
            path_prefix,
        )
    except Exception as exc:
        raise _parse_native_error(exc, "native_stereo_encoder_process_init_failed") from exc


def create_native_session_io() -> NativeSessionIo:
    """Create the Rust session file I/O helper or fail explicitly."""

    module = _require_native_feature(
        "session_io",
        "native_session_io_unavailable",
        "原生会话 I/O 模块",
        "原生模块缺少会话 I/O 能力",
    )
    try:
        return module.NativeSessionIo()
    except Exception as exc:
        raise _parse_native_error(exc, "native_session_io_init_failed") from exc


def native_session_io_or_none() -> NativeSessionIo | None:
    """Return the process-wide stateless session I/O helper when available."""

    global _SESSION_IO, _SESSION_IO_UNAVAILABLE
    if _SESSION_IO_UNAVAILABLE:
        return None
    if _SESSION_IO is not None:
        return _SESSION_IO
    with _SESSION_IO_LOCK:
        if _SESSION_IO_UNAVAILABLE:
            return None
        if _SESSION_IO is not None:
            return _SESSION_IO
        try:
            _SESSION_IO = create_native_session_io()
        except NativeModuleError:
            _SESSION_IO_UNAVAILABLE = True
            return None
        return _SESSION_IO


def create_native_preview_buffer(stream_fps: int) -> NativePreviewBuffer:
    """Create the Rust latest-only preview buffer or fail explicitly."""

    module = _require_native_feature(
        "preview_buffer",
        "native_preview_buffer_unavailable",
        "原生预览缓冲模块",
        "原生模块缺少预览缓冲能力",
    )
    try:
        return module.NativePreviewBuffer(stream_fps)
    except Exception as exc:
        raise _parse_native_error(exc, "native_preview_buffer_init_failed") from exc


def create_native_performance_metrics() -> NativePerformanceMetrics:
    """Create the Rust performance metrics accumulator or fail explicitly."""

    module = _require_native_feature(
        "performance_metrics",
        "native_metrics_unavailable",
        "原生性能指标模块",
        "原生模块缺少性能指标能力",
    )
    try:
        return module.NativePerformanceMetrics()
    except Exception as exc:
        raise _parse_native_error(exc, "native_metrics_init_failed") from exc
