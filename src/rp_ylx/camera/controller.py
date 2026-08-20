"""确定性设备选择、生命周期和帧连续性检查。"""

from __future__ import annotations

from contextlib import suppress

from rp_ylx.camera.models import (
    CameraBackend,
    CameraDescriptor,
    CameraError,
    CameraMode,
    CameraStream,
    FrameObservation,
    StereoFrame,
)
from rp_ylx.native import (
    NativeCameraFrameValidator,
    NativeModuleError,
    create_native_camera_frame_validator,
)
from rp_ylx.performance.metrics import PerformanceMetrics


class CameraController:
    def __init__(
        self, backend: CameraBackend, *, metrics: PerformanceMetrics | None = None
    ) -> None:
        self._backend = backend
        self._stream: CameraStream | None = None
        self._descriptor: CameraDescriptor | None = None
        self._mode: CameraMode | None = None
        self._state = "closed"
        self._last_source_sequence: int | None = None
        self._last_host_time: int | None = None
        self._metrics = metrics
        try:
            self._native_frame_validator: NativeCameraFrameValidator | None = (
                create_native_camera_frame_validator()
            )
        except NativeModuleError:
            self._native_frame_validator = None

    @property
    def state(self) -> str:
        return self._state

    @property
    def descriptor(self) -> CameraDescriptor | None:
        return self._descriptor

    @property
    def mode(self) -> CameraMode | None:
        return self._mode

    def discover(self) -> tuple[CameraDescriptor, ...]:
        devices = self._backend.discover()
        stable_ids = [device.stable_id for device in devices]
        if len(stable_ids) != len(set(stable_ids)):
            raise CameraError("duplicate_identity", "发现重复的稳定设备 ID")
        return tuple(sorted(devices, key=lambda device: device.stable_id))

    def open(self, mode: CameraMode, *, stable_id: str | None = None) -> CameraDescriptor:
        if self._stream is not None:
            raise CameraError("invalid_state", "相机已经打开")
        devices = self.discover()
        if not devices:
            raise CameraError("no_device", "未发现相机", retryable=True)
        if stable_id is None:
            if len(devices) != 1:
                raise CameraError("selection_required", "发现多个相机，必须指定 stable_id")
            descriptor = devices[0]
        else:
            try:
                descriptor = next(device for device in devices if device.stable_id == stable_id)
            except StopIteration as exc:
                raise CameraError("device_not_found", "指定相机不存在", retryable=True) from exc
        if mode not in descriptor.modes:
            raise CameraError("unsupported_mode", "相机不支持请求的精确模式")
        try:
            stream = self._backend.open(descriptor, mode)
        except CameraError:
            raise
        except PermissionError as exc:
            raise CameraError("permission_denied", "没有打开相机的权限") from exc
        except OSError as exc:
            raise CameraError("open_failed", f"打开相机失败：{exc}", retryable=True) from exc
        self._stream = stream
        self._descriptor = descriptor
        self._mode = mode
        self._state = "open"
        self._reset_frame_validation()
        return descriptor

    def start(self) -> None:
        if self._stream is None or self._state != "open":
            raise CameraError("invalid_state", "相机必须先打开")
        try:
            self._stream.start()
        except Exception:
            with suppress(Exception):
                self.close()
            raise
        self._state = "streaming"

    def read(self, *, timeout: float = 1.0) -> FrameObservation:
        if timeout <= 0:
            raise ValueError("timeout 必须大于零")
        if self._stream is None or self._state != "streaming":
            raise CameraError("invalid_state", "相机尚未开始采集")
        try:
            frame = self._stream.read(timeout)
            return FrameObservation(frame=frame, dropped_before=self._validate_frame(frame))
        except CameraError:
            with suppress(Exception):
                self.close()
            raise
        except TimeoutError as exc:
            with suppress(Exception):
                self.close()
            raise CameraError("frame_timeout", "等待相机帧超时", retryable=True) from exc
        except OSError as exc:
            with suppress(Exception):
                self.close()
            raise CameraError("disconnected", f"相机读取失败：{exc}", retryable=True) from exc

    def stop(self) -> None:
        if self._stream is None:
            return
        try:
            if self._state == "streaming":
                self._stream.stop()
                self._state = "open"
        except Exception:
            with suppress(Exception):
                self.close()
            raise

    def close(self) -> None:
        stream, self._stream = self._stream, None
        self._descriptor = None
        self._mode = None
        self._state = "closed"
        self._reset_frame_validation()
        if stream is not None:
            stream.close()

    def _reset_frame_validation(self) -> None:
        self._last_source_sequence = None
        self._last_host_time = None
        if self._native_frame_validator is not None:
            try:
                self._native_frame_validator.reset()
            except Exception:
                self._native_frame_validator = None

    def _validate_frame(self, frame: StereoFrame) -> int:
        if self._native_frame_validator is not None:
            return self._validate_frame_native(frame)
        return self._validate_frame_python(frame)

    def _validate_frame_native(self, frame: StereoFrame) -> int:
        raw = frame.raw_side_by_side
        try:
            result = self._native_frame_validator.validate_frame(
                frame.source_sequence,
                frame.host_monotonic_ns,
                frame.valid,
                bool(frame.left),
                bool(frame.right),
                raw is not None and bool(raw),
                frame._application_dropped_before,
            )
            dropped = _native_non_negative_int(result.get("dropped_before"), "dropped_before")
            queue_rejected = _native_non_negative_int(
                result.get("queue_rejected"), "queue_rejected"
            )
            source_gap = _native_non_negative_int(result.get("source_gap"), "source_gap")
        except CameraError:
            raise
        except Exception as exc:
            raise _camera_error_from_native(exc, "native_camera_frame_validator_failed") from exc
        if self._metrics is not None:
            if queue_rejected:
                self._metrics.record_loss("queue_rejected", queue_rejected)
            if source_gap:
                self._metrics.record_loss("source_gap", source_gap)
        return dropped

    def _validate_frame_python(self, frame: StereoFrame) -> int:
        if (
            not frame.valid
            or frame.source_sequence < 0
            or frame.host_monotonic_ns < 0
            or not ((frame.left and frame.right) or frame.raw_side_by_side)
        ):
            raise CameraError("bad_frame", "相机返回不完整或无效双目帧")
        dropped = 0
        application_dropped = frame._application_dropped_before
        if self._last_source_sequence is not None:
            if frame.source_sequence <= self._last_source_sequence:
                raise CameraError("sequence_regression", "相机帧序号重复或回退")
            dropped = frame.source_sequence - self._last_source_sequence - 1
            if application_dropped > dropped:
                raise CameraError(
                    "invalid_drop_accounting",
                    "应用丢帧计数超过相机源序列缺口",
                )
            if self._metrics is not None:
                if application_dropped:
                    self._metrics.record_loss("queue_rejected", application_dropped)
                source_dropped = dropped - application_dropped
                if source_dropped:
                    self._metrics.record_loss("source_gap", source_dropped)
        elif application_dropped:
            dropped = application_dropped
            if self._metrics is not None:
                self._metrics.record_loss("queue_rejected", application_dropped)
        if self._last_host_time is not None and frame.host_monotonic_ns <= self._last_host_time:
            raise CameraError("timestamp_regression", "相机主机时间戳重复或回退")
        self._last_source_sequence = frame.source_sequence
        self._last_host_time = frame.host_monotonic_ns
        return dropped

    def __enter__(self) -> CameraController:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


def _native_non_negative_int(value: object, name: str) -> int:
    if type(value) is not int or value < 0:
        raise CameraError(
            "native_camera_frame_validator_failed",
            f"原生相机帧校验结果字段无效：{name}",
        )
    return value


def _camera_error_from_native(error: BaseException, fallback_code: str) -> CameraError:
    raw = str(error)
    code, separator, message = raw.partition(": ")
    if not separator or not code.replace("_", "").isalnum():
        code, message = fallback_code, raw or fallback_code
    return CameraError(code, message)
