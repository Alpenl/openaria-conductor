"""经过校验的进程内 Rust 数据面能力探针。"""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from types import ModuleType
from typing import Protocol

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


class NativeAudioRecorder(Protocol):
    def start(self) -> None: ...

    def stop(self, timeout_seconds: float = 5.0) -> dict[str, object]: ...

    def abort(self) -> None: ...

    def close(self) -> None: ...


class NativeImuCollector(Protocol):
    def read(self, timeout_seconds: float = 1.0) -> dict[str, object]: ...

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
