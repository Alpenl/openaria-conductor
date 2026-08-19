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

    def flush_and_close(self) -> dict[str, object]: ...

    def close(self) -> None: ...


class NativeRecordingFrameGate(Protocol):
    def begin_frame(self, dropped_before: int) -> dict[str, object]: ...

    def finish_frame(self) -> int: ...

    def start_stopping(self) -> int: ...

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
