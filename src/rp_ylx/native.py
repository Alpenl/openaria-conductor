"""经过校验的进程内 Rust 数据面能力探针。"""

from __future__ import annotations

import importlib
import threading
from dataclasses import dataclass
from types import ModuleType
from typing import Protocol, cast

NATIVE_MODULE = "rp_ylx._native"
SUPPORTED_NATIVE_ABI = 5


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


@dataclass(frozen=True, slots=True)
class NativeCapturePlan:
    device: str
    width: int
    height: int
    fps: int
    encoding: str
    buffer_count: int
    queue_capacity: int
    frame_decimation: int
    read_timeout_seconds: float
    imu_unit: int | None = None
    imu_selector: int = 1
    imu_stale_poll_interval: float = 0.001
    imu_timeout_seconds: float = 2.0


@dataclass(frozen=True, slots=True)
class NativeRecordingPlan:
    session_root: str
    session_id: str
    encoder_executable: str
    width: int
    height: int
    fps: int
    bitrate_kbps: int
    segment_frames: int
    recording_start_monotonic_ns: int
    audio_enabled: bool
    audio_device: str
    audio_sample_rate_hz: int
    audio_channels: int
    audio_segment_seconds: float


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


class NativeSessionTransaction(Protocol):
    @property
    def recording_start_monotonic_ns(self) -> int: ...

    def snapshot(self) -> dict[str, object]: ...

    def segments(self) -> list[dict[str, object]]: ...

    def finish(
        self,
        duration_seconds: float,
        timeout_seconds: float = 30.0,
    ) -> dict[str, object]: ...

    def boundary(self, ordinal: int, duration_seconds: float) -> dict[str, object]: ...

    def abort(self, reason: str) -> None: ...

    def open_handle_count(self) -> int: ...

    def seal(
        self,
        partial_path: str,
        final_path: str,
        session_id: str,
        manifest: bytes,
        expected_identities: dict[str, tuple[int, int, int, int]],
        control_names: list[str] | None = None,
    ) -> dict[str, object]: ...


class NativeCaptureEngine(Protocol):
    def start_preview(self) -> None: ...

    def start_recording(
        self,
        transaction: NativeSessionTransaction,
        on_failure: object,
    ) -> dict[str, object]: ...

    def stop_recording(self, timeout_seconds: float = 3.0) -> dict[str, object]: ...

    def latest_imu_observation(self) -> dict[str, object] | None: ...

    def camera_focus_status(self) -> dict[str, object] | None: ...

    def set_camera_focus(
        self,
        value: int | None = None,
        auto_enabled: bool | None = None,
    ) -> dict[str, object]: ...

    def snapshot(self) -> dict[str, object]: ...

    def close(self, timeout_seconds: float = 5.0) -> dict[str, object]: ...


class NativeSessionStore(Protocol):
    def begin_recording(self, plan: NativeRecordingPlan) -> NativeSessionTransaction: ...

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

    def open_relative_regular(self, root_descriptor: int, relative_path: str) -> int: ...

    def open_verified_artifact(
        self,
        root_descriptor: int,
        relative_path: str,
        expected_bytes: int,
        expected_sha256: str,
    ) -> dict[str, object]: ...

    def read_bounded_fd(self, descriptor: int, maximum_bytes: int) -> bytes: ...


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


_SESSION_STORE_LOCK = threading.Lock()
_SESSION_STORE: NativeSessionStore | None = None
_SESSION_STORE_UNAVAILABLE = False


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


def create_native_capture_engine(
    plan: NativeCapturePlan,
    preview: NativePreviewBuffer,
    *,
    metrics: NativePerformanceMetrics | None = None,
) -> NativeCaptureEngine:
    """Create the Rust owner for camera, preview fanout, IMU, and capture lifecycle."""

    module = _require_native_feature(
        "capture_engine",
        "native_capture_engine_unavailable",
        "原生 CaptureEngine 模块",
        "原生模块缺少录制级 CaptureEngine 能力",
    )
    try:
        if metrics is None:
            return module.NativeCaptureEngine(plan, preview, None)
        return module.NativeCaptureEngine(plan, preview, metrics)
    except Exception as exc:
        raise _parse_native_error(exc, "native_capture_engine_init_failed") from exc


def create_native_session_store() -> NativeSessionStore:
    """Create the Rust transaction owner for recording and verified session I/O."""

    module = _require_native_feature(
        "session_store",
        "native_session_store_unavailable",
        "原生 SessionStore 模块",
        "原生模块缺少事务级 SessionStore 能力",
    )
    try:
        return module.NativeSessionStore()
    except Exception as exc:
        raise _parse_native_error(exc, "native_session_store_init_failed") from exc


def native_session_store() -> NativeSessionStore:
    """Return the process-wide SessionStore; production never falls back to Python I/O."""

    global _SESSION_STORE, _SESSION_STORE_UNAVAILABLE
    if _SESSION_STORE_UNAVAILABLE:
        raise NativeModuleError(
            "native_session_store_unavailable", "原生 SessionStore 已标记为不可用"
        )
    if _SESSION_STORE is not None:
        return _SESSION_STORE
    with _SESSION_STORE_LOCK:
        if _SESSION_STORE_UNAVAILABLE:
            raise NativeModuleError(
                "native_session_store_unavailable", "原生 SessionStore 已标记为不可用"
            )
        if _SESSION_STORE is None:
            try:
                _SESSION_STORE = create_native_session_store()
            except NativeModuleError:
                _SESSION_STORE_UNAVAILABLE = True
                raise
        return _SESSION_STORE


def native_session_store_or_none() -> NativeSessionStore | None:
    """Return the sole SessionStore owner in source-checkout/test environments."""

    global _SESSION_STORE, _SESSION_STORE_UNAVAILABLE
    if _SESSION_STORE_UNAVAILABLE:
        return None
    if _SESSION_STORE is not None:
        return _SESSION_STORE
    with _SESSION_STORE_LOCK:
        if _SESSION_STORE_UNAVAILABLE:
            return None
        if _SESSION_STORE is not None:
            return _SESSION_STORE
        try:
            _SESSION_STORE = create_native_session_store()
        except NativeModuleError:
            _SESSION_STORE_UNAVAILABLE = True
            return None
        return _SESSION_STORE


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
