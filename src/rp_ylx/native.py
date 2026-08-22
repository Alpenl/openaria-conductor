"""经过校验的进程内 Rust 数据面能力探针。"""

from __future__ import annotations

import importlib
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


class NativeCameraFrameValidator(Protocol):
    def validate_frame(
        self,
        source_sequence: int,
        host_monotonic_ns: int,
        valid: bool,
        has_left: bool,
        has_right: bool,
        has_raw_side_by_side: bool,
        application_dropped_before: int,
    ) -> dict[str, int]: ...

    def reset(self) -> None: ...


class NativeAudioRecorder(Protocol):
    def start(self) -> None: ...

    def snapshot(self) -> dict[str, object]: ...

    def stop(self, timeout_seconds: float = 5.0) -> dict[str, object]: ...

    def abort(self) -> None: ...

    def close(self) -> None: ...


class NativeTimeline(Protocol):
    @staticmethod
    def now_monotonic_ns() -> int: ...

    def start_monotonic_ns(self) -> int: ...

    def elapsed_ns(self) -> int: ...

    def elapsed_seconds(self) -> float: ...

    def offset_ns(self, monotonic_ns: int) -> int: ...

    def offset_seconds(self, monotonic_ns: int) -> float: ...

    def audio_sync(
        self,
        started_monotonic_ns: int,
        stopped_monotonic_ns: int,
        sample_rate_hz: int,
    ) -> dict[str, object]: ...


class NativeActiveTakeWriter(Protocol):
    def reserve_frame(
        self,
        source_sequence: int,
        host_monotonic_ns: int,
        source_gap: int,
    ) -> dict[str, object]: ...

    def raw_write_decision(
        self,
        record_sequence: int,
        source_sequence: int,
        host_monotonic_ns: int,
    ) -> dict[str, object]: ...

    def split_write_decision(
        self,
        record_sequence: int,
        source_sequence: int,
        host_monotonic_ns: int,
        segment_index: int,
        segment_frame: int,
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


class NativeRecordingCodec(Protocol):
    def jpeg_payload(self, payload: bytes) -> bytes: ...

    def encode_split_frame_index(
        self,
        session_id: str,
        frame: int,
        source_sequence: int,
        host_monotonic_ns: int,
        segment_index: int,
        segment_frame: int,
    ) -> bytes: ...

    def encode_raw_frame_index(
        self,
        session_id: str,
        frame: int,
        source_sequence: int,
        host_monotonic_ns: int,
        video_offset: int,
        video_bytes: int,
    ) -> bytes: ...

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
    ) -> bytes: ...


class NativeRecordingSink(Protocol):
    def write_split_frame_index(
        self,
        frame: int,
        source_sequence: int,
        host_monotonic_ns: int,
        segment_index: int,
        segment_frame: int,
    ) -> int: ...

    def write_raw_frame(
        self,
        frame: int,
        source_sequence: int,
        host_monotonic_ns: int,
        raw_side_by_side: bytes,
    ) -> dict[str, int]: ...

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


class NativeRecordingFrameGate(Protocol):
    def begin_frame(self, dropped_before: int) -> dict[str, object]: ...

    def finish_frame(self) -> int: ...

    def start_stopping(self) -> int: ...

    def snapshot(self) -> dict[str, object]: ...


class NativeRecordingTapState(Protocol):
    def begin_frame(self, dropped_before: int) -> dict[str, object]: ...

    def finish_frame(self) -> int: ...

    def start_stopping(self) -> int: ...

    def mark_failure(self) -> dict[str, object]: ...

    def snapshot(self) -> dict[str, object]: ...


class NativeCaptureFanoutState(Protocol):
    def start_recording(self) -> dict[str, object]: ...

    def begin_frame(self, dropped_before: int, has_preview: bool) -> dict[str, object]: ...

    def finish_frame(self) -> int: ...

    def start_stopping(self) -> int: ...

    def mark_failure(self) -> dict[str, object]: ...

    def snapshot(self) -> dict[str, object]: ...


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

    def start_recording_raw_sink(
        self,
        active_take: NativeActiveTakeWriter,
        sink: NativeRecordingSink,
        on_failure: object,
        imu: object | None = None,
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


class NativeRecordingEventQueue(Protocol):
    def put(self, item: object, timeout_seconds: float = 0.0) -> bool: ...

    def get(self) -> object: ...

    def qsize(self) -> int: ...

    def stats(self) -> dict[str, object]: ...

    def close_and_clear(self) -> None: ...


class NativeStereoEncoderEvents(Protocol):
    def parse(self, line: bytes) -> dict[str, object] | None: ...


class NativeStereoEncoderPipe(Protocol):
    def submit(self, jpeg: bytes) -> int: ...

    def submitted_frames(self) -> int: ...


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


def create_native_splitter() -> NativeSplitter:
    """Create the explicit Rust/TurboJPEG splitter or fail without fallback."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_splitter_unavailable", f"无法加载原生拆分模块：{exc}"
        ) from exc
    capabilities = _validate_capabilities(module)
    if "turbojpeg_split" not in capabilities.features:
        raise NativeModuleError(
            "native_splitter_unavailable", "原生模块缺少可用的 TurboJPEG 拆分能力"
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

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_camera_unavailable", f"无法加载原生相机模块：{exc}"
        ) from exc
    capabilities = _validate_capabilities(module)
    if "native_camera" not in capabilities.features:
        raise NativeModuleError(
            "native_camera_unavailable", "原生模块缺少完整 V4L2/TurboJPEG 相机能力"
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
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_camera_init_failed", raw
        raise NativeModuleError(code, message) from exc


def _parse_native_error(exc: Exception, fallback_code: str) -> NativeModuleError:
    raw = str(exc)
    code, separator, message = raw.partition(": ")
    if not separator or not code.replace("_", "").isalnum():
        code, message = fallback_code, raw
    return NativeModuleError(code, message)


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

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_focus_unavailable", f"无法加载原生相机控制模块：{exc}"
        ) from exc
    capabilities = _validate_capabilities(module)
    if "v4l2_focus_control" not in capabilities.features:
        raise NativeModuleError("native_focus_unavailable", "原生模块缺少 V4L2 焦距控制能力")
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
    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_focus_unavailable", f"无法加载原生相机控制模块：{exc}"
        ) from exc
    capabilities = _validate_capabilities(module)
    if "v4l2_focus_control" not in capabilities.features:
        raise NativeModuleError("native_focus_unavailable", "原生模块缺少 V4L2 焦距控制能力")
    try:
        module.v4l2_set_focus(device, value, auto_enabled)
    except Exception as exc:
        raise _parse_native_error(exc, "native_focus_set_failed") from exc
    status = native_camera_focus_status(device)
    if status is None:
        raise NativeModuleError("camera_focus_unsupported", "相机没有可读取的焦距控制")
    return status


def create_native_camera_frame_validator() -> NativeCameraFrameValidator:
    """Create the Rust camera continuity/drop-accounting validator or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_camera_frame_validator_unavailable",
            f"无法加载原生相机帧校验模块：{exc}",
        ) from exc
    capabilities = _validate_capabilities(module)
    if "camera_frame_validator" not in capabilities.features:
        raise NativeModuleError(
            "native_camera_frame_validator_unavailable",
            "原生模块缺少相机帧连续性校验能力",
        )
    try:
        return module.NativeCameraFrameValidator()
    except Exception as exc:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_camera_frame_validator_init_failed", raw
        raise NativeModuleError(code, message) from exc


def create_native_audio_recorder(
    session_root: str,
    *,
    device: str = "hw:0,0",
    sample_rate_hz: int = 48_000,
    channels: int = 2,
    segment_seconds: float = 30.0,
) -> NativeAudioRecorder:
    """Create the Rust/ALSA recorder or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError("native_audio_unavailable", f"无法加载原生音频模块：{exc}") from exc
    capabilities = _validate_capabilities(module)
    if "native_audio" not in capabilities.features:
        raise NativeModuleError("native_audio_unavailable", "原生模块缺少 ALSA 音频录制能力")
    try:
        return module.NativeAudioRecorder(
            session_root,
            device,
            sample_rate_hz,
            channels,
            segment_seconds,
        )
    except Exception as exc:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_audio_init_failed", raw
        raise NativeModuleError(code, message) from exc


def create_native_timeline(start_monotonic_ns: int | None = None) -> NativeTimeline:
    """Create the Rust take/session timeline owner or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_timeline_unavailable", f"无法加载原生时间线模块：{exc}"
        ) from exc
    capabilities = _validate_capabilities(module)
    if "native_timeline" not in capabilities.features:
        raise NativeModuleError("native_timeline_unavailable", "原生模块缺少统一时间线能力")
    try:
        return module.NativeTimeline(start_monotonic_ns)
    except Exception as exc:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_timeline_init_failed", raw
        raise NativeModuleError(code, message) from exc


def create_native_active_take_writer(session_id: str) -> NativeActiveTakeWriter:
    """Create the Rust active-take frame/domain/drop owner or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "active_take_writer_unavailable", f"无法加载原生 active take 模块：{exc}"
        ) from exc
    capabilities = _validate_capabilities(module)
    if "active_take_writer" not in capabilities.features:
        raise NativeModuleError(
            "active_take_writer_unavailable", "原生模块缺少 active take 写入状态能力"
        )
    try:
        return module.NativeActiveTakeWriter(session_id)
    except Exception as exc:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "active_take_writer_init_failed", raw
        raise NativeModuleError(code, message) from exc


def create_native_imu_collector(
    device: str,
    *,
    unit: int | None = None,
    selector: int = 1,
    stale_poll_interval: float = 0.001,
) -> NativeImuCollector:
    """Create the Rust UVC XU IMU collector or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError("native_imu_unavailable", f"无法加载原生 IMU 模块：{exc}") from exc
    capabilities = _validate_capabilities(module)
    if "native_imu" not in capabilities.features:
        raise NativeModuleError("native_imu_unavailable", "原生模块缺少 UVC XU IMU 采集能力")
    try:
        return module.NativeImuCollector(device, unit, selector, stale_poll_interval)
    except Exception as exc:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_imu_init_failed", raw
        raise NativeModuleError(code, message) from exc


def create_native_recording_codec() -> NativeRecordingCodec:
    """Create the Rust recording hot-path codec or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_recording_unavailable", f"无法加载原生录制编码模块：{exc}"
        ) from exc
    capabilities = _validate_capabilities(module)
    if "recording_codec" not in capabilities.features:
        raise NativeModuleError("native_recording_unavailable", "原生模块缺少录制热路径编码能力")
    try:
        return module.NativeRecordingCodec()
    except Exception as exc:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_recording_init_failed", raw
        raise NativeModuleError(code, message) from exc


def create_native_recording_sink(
    session_root: str,
    session_id: str,
    *,
    split_eyes: bool,
) -> NativeRecordingSink:
    """Create the Rust recording event sink or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_recording_sink_unavailable", f"无法加载原生录制写入模块：{exc}"
        ) from exc
    capabilities = _validate_capabilities(module)
    if "recording_sink" not in capabilities.features:
        raise NativeModuleError(
            "native_recording_sink_unavailable", "原生模块缺少录制写入热路径能力"
        )
    try:
        return module.NativeRecordingSink(session_root, session_id, split_eyes)
    except Exception as exc:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_recording_sink_init_failed", raw
        raise NativeModuleError(code, message) from exc


def create_native_recording_frame_gate(frame_decimation: int) -> NativeRecordingFrameGate:
    """Create the Rust recording frame fanout/decimation gate or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_recording_frame_gate_unavailable",
            f"无法加载原生录制帧门控模块：{exc}",
        ) from exc
    capabilities = _validate_capabilities(module)
    if "recording_frame_gate" not in capabilities.features:
        raise NativeModuleError(
            "native_recording_frame_gate_unavailable", "原生模块缺少录制帧门控热路径能力"
        )
    try:
        return module.NativeRecordingFrameGate(frame_decimation)
    except Exception as exc:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_recording_frame_gate_init_failed", raw
        raise NativeModuleError(code, message) from exc


def create_native_recording_tap_state(frame_decimation: int) -> NativeRecordingTapState:
    """Create the Rust recording tap state machine or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_recording_tap_state_unavailable",
            f"无法加载原生录制 tap 状态模块：{exc}",
        ) from exc
    capabilities = _validate_capabilities(module)
    if "recording_tap_state" not in capabilities.features:
        raise NativeModuleError(
            "native_recording_tap_state_unavailable",
            "原生模块缺少录制 tap 状态热路径能力",
        )
    try:
        return module.NativeRecordingTapState(frame_decimation)
    except Exception as exc:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_recording_tap_state_init_failed", raw
        raise NativeModuleError(code, message) from exc


def create_native_capture_fanout_state(frame_decimation: int) -> NativeCaptureFanoutState:
    """Create the Rust continuous capture fanout state machine or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_capture_fanout_unavailable",
            f"无法加载原生连续采集 fanout 模块：{exc}",
        ) from exc
    capabilities = _validate_capabilities(module)
    if "capture_fanout" not in capabilities.features:
        raise NativeModuleError(
            "native_capture_fanout_unavailable",
            "原生模块缺少连续采集 fanout 热路径能力",
        )
    try:
        return module.NativeCaptureFanoutState(frame_decimation)
    except Exception as exc:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_capture_fanout_init_failed", raw
        raise NativeModuleError(code, message) from exc


def create_native_continuous_capture_runtime(
    camera: NativeCamera,
    preview: NativePreviewBuffer,
    frame_decimation: int,
    *,
    read_timeout_seconds: float = 2.0,
    metrics: NativePerformanceMetrics | None = None,
) -> NativeContinuousCaptureRuntime:
    """Create the Rust continuous capture runtime or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_continuous_capture_runtime_unavailable",
            f"无法加载原生连续采集 runtime 模块：{exc}",
        ) from exc
    capabilities = _validate_capabilities(module)
    if "continuous_capture_runtime" not in capabilities.features:
        raise NativeModuleError(
            "native_continuous_capture_runtime_unavailable",
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
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_continuous_capture_runtime_init_failed", raw
        raise NativeModuleError(code, message) from exc


def create_native_recording_segment_planner(segment_frames: int) -> NativeRecordingSegmentPlanner:
    """Create the Rust recording segment planner or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_recording_segment_planner_unavailable",
            f"无法加载原生录制分段规划模块：{exc}",
        ) from exc
    capabilities = _validate_capabilities(module)
    if "recording_segment_planner" not in capabilities.features:
        raise NativeModuleError(
            "native_recording_segment_planner_unavailable",
            "原生模块缺少录制分段规划热路径能力",
        )
    try:
        return module.NativeRecordingSegmentPlanner(segment_frames)
    except Exception as exc:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_recording_segment_planner_init_failed", raw
        raise NativeModuleError(code, message) from exc


def create_native_recording_event_queue(capacity: int) -> NativeRecordingEventQueue:
    """Create the Rust recording event queue or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_recording_event_queue_unavailable",
            f"无法加载原生录制事件队列模块：{exc}",
        ) from exc
    capabilities = _validate_capabilities(module)
    if "recording_event_queue" not in capabilities.features:
        raise NativeModuleError(
            "native_recording_event_queue_unavailable",
            "原生模块缺少录制事件队列热路径能力",
        )
    try:
        return module.NativeRecordingEventQueue(capacity)
    except Exception as exc:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_recording_event_queue_init_failed", raw
        raise NativeModuleError(code, message) from exc


def create_native_stereo_encoder_events() -> NativeStereoEncoderEvents:
    """Create the Rust stereo encoder stdout event parser or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_stereo_encoder_events_unavailable",
            f"无法加载原生编码助手事件解析模块：{exc}",
        ) from exc
    capabilities = _validate_capabilities(module)
    if "stereo_encoder_events" not in capabilities.features:
        raise NativeModuleError(
            "native_stereo_encoder_events_unavailable",
            "原生模块缺少编码助手事件解析能力",
        )
    try:
        return module.NativeStereoEncoderEvents()
    except Exception as exc:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_stereo_encoder_events_init_failed", raw
        raise NativeModuleError(code, message) from exc


def create_native_stereo_encoder_pipe(descriptor: int) -> NativeStereoEncoderPipe:
    """Create the Rust stereo encoder frame pipe writer or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_stereo_encoder_pipe_unavailable",
            f"无法加载原生编码助手写入模块：{exc}",
        ) from exc
    capabilities = _validate_capabilities(module)
    if "stereo_encoder_pipe" not in capabilities.features:
        raise NativeModuleError(
            "native_stereo_encoder_pipe_unavailable",
            "原生模块缺少编码助手写入热路径能力",
        )
    try:
        return module.NativeStereoEncoderPipe(descriptor)
    except Exception as exc:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_stereo_encoder_pipe_init_failed", raw
        raise NativeModuleError(code, message) from exc


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

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_stereo_encoder_process_unavailable",
            f"无法加载原生编码助手进程模块：{exc}",
        ) from exc
    capabilities = _validate_capabilities(module)
    if "stereo_encoder_process" not in capabilities.features:
        raise NativeModuleError(
            "native_stereo_encoder_process_unavailable",
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
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_stereo_encoder_process_init_failed", raw
        raise NativeModuleError(code, message) from exc


def create_native_session_io() -> NativeSessionIo:
    """Create the Rust session file I/O helper or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_session_io_unavailable", f"无法加载原生会话 I/O 模块：{exc}"
        ) from exc
    capabilities = _validate_capabilities(module)
    if "session_io" not in capabilities.features:
        raise NativeModuleError("native_session_io_unavailable", "原生模块缺少会话 I/O 能力")
    try:
        return module.NativeSessionIo()
    except Exception as exc:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_session_io_init_failed", raw
        raise NativeModuleError(code, message) from exc


def parse_native_single_range(
    value: str | None,
    complete_size: int,
) -> tuple[int, int] | None:
    """Parse a single HTTP Range through the Rust helper or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_range_parser_unavailable", f"无法加载原生 Range 解析模块：{exc}"
        ) from exc
    capabilities = _validate_capabilities(module)
    if "range_parser" not in capabilities.features:
        raise NativeModuleError("native_range_parser_unavailable", "原生模块缺少 Range 解析能力")
    try:
        result = module.parse_single_range(value, complete_size)
    except ValueError:
        raise
    except Exception as exc:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_range_parser_failed", raw
        raise NativeModuleError(code, message) from exc
    if result is None:
        return None
    if (
        not isinstance(result, tuple)
        or len(result) != 2
        or any(isinstance(item, bool) or not isinstance(item, int) or item < 0 for item in result)
    ):
        raise NativeModuleError("native_range_parser_failed", "原生 Range 解析结果无效")
    return result


def evaluate_native_drop_quality_policy(
    drop_events: object,
    frames_written: int,
    *,
    max_contiguous_dropped_frames: int,
    max_total_dropped_frames: int,
    max_drop_fraction: float,
    window_seconds: float,
    max_dropped_frames_per_window: int,
) -> dict[str, object]:
    """Evaluate recording drop quality through the Rust helper or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_drop_quality_policy_unavailable",
            f"无法加载原生丢帧质量策略模块：{exc}",
        ) from exc
    capabilities = _validate_capabilities(module)
    if "drop_quality_policy" not in capabilities.features:
        raise NativeModuleError(
            "native_drop_quality_policy_unavailable",
            "原生模块缺少丢帧质量策略能力",
        )
    try:
        result = module.evaluate_drop_quality_policy(
            drop_events,
            frames_written,
            max_contiguous_dropped_frames,
            max_total_dropped_frames,
            max_drop_fraction,
            window_seconds,
            max_dropped_frames_per_window,
        )
    except Exception as exc:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_drop_quality_policy_failed", raw
        raise NativeModuleError(code, message) from exc
    expected = {
        "accepted",
        "dropped",
        "total",
        "fraction",
        "contiguous",
        "window_drops",
        "violations",
    }
    if (
        not isinstance(result, dict)
        or set(result) != expected
        or type(result["accepted"]) is not bool
        or any(
            isinstance(result[name], bool) or not isinstance(result[name], int) or result[name] < 0
            for name in ("dropped", "total", "contiguous", "window_drops")
        )
        or not isinstance(result["fraction"], float)
        or not isinstance(result["violations"], list)
        or any(not isinstance(item, str) for item in result["violations"])
    ):
        raise NativeModuleError("native_drop_quality_policy_failed", "原生丢帧质量策略结果无效")
    return result


def create_native_preview_buffer(stream_fps: int) -> NativePreviewBuffer:
    """Create the Rust latest-only preview buffer or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_preview_buffer_unavailable", f"无法加载原生预览缓冲模块：{exc}"
        ) from exc
    capabilities = _validate_capabilities(module)
    if "preview_buffer" not in capabilities.features:
        raise NativeModuleError("native_preview_buffer_unavailable", "原生模块缺少预览缓冲能力")
    try:
        return module.NativePreviewBuffer(stream_fps)
    except Exception as exc:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_preview_buffer_init_failed", raw
        raise NativeModuleError(code, message) from exc


def create_native_performance_metrics() -> NativePerformanceMetrics:
    """Create the Rust performance metrics accumulator or fail explicitly."""

    try:
        module = importlib.import_module(NATIVE_MODULE)
    except (ImportError, ModuleNotFoundError) as exc:
        raise NativeModuleError(
            "native_metrics_unavailable", f"无法加载原生性能指标模块：{exc}"
        ) from exc
    capabilities = _validate_capabilities(module)
    if "performance_metrics" not in capabilities.features:
        raise NativeModuleError("native_metrics_unavailable", "原生模块缺少性能指标能力")
    try:
        return module.NativePerformanceMetrics()
    except Exception as exc:
        raw = str(exc)
        code, separator, message = raw.partition(": ")
        if not separator or not code.replace("_", "").isalnum():
            code, message = "native_metrics_init_failed", raw
        raise NativeModuleError(code, message) from exc
