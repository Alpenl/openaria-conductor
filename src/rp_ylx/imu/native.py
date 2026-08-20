"""Rust UVC XU IMU collector adapter."""

from __future__ import annotations

import os
from contextlib import suppress
from pathlib import Path

from rp_ylx.imu.models import ImuError, ImuObservation, ImuSample, RawVector3
from rp_ylx.native import NativeModuleError, create_native_imu_collector

_RETRYABLE_NATIVE_CODES = frozenset(
    {
        "disconnected",
        "non_monotonic_time",
        "sensor_stalled",
        "timestamp_regression",
        "timestamp_stalled",
    }
)


def _to_imu_error(error: BaseException, *, fallback_code: str) -> ImuError:
    if isinstance(error, ImuError):
        return error
    if isinstance(error, NativeModuleError):
        return ImuError(
            error.code,
            error.message,
            retryable=error.code in _RETRYABLE_NATIVE_CODES,
        )
    raw = str(error)
    code, separator, message = raw.partition(": ")
    if separator and code.replace("_", "").isalnum():
        return ImuError(code, message, retryable=code in _RETRYABLE_NATIVE_CODES)
    return ImuError(fallback_code, raw or type(error).__name__)


def _int(value: object, field: str) -> int:
    if type(value) is not int:
        raise ImuError("invalid_native_observation", f"原生 IMU 字段 {field} 必须是整数")
    return value


def _optional_int(value: object, field: str) -> int | None:
    if value is None:
        return None
    return _int(value, field)


def _dict(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ImuError("invalid_native_observation", f"原生 IMU 字段 {field} 必须是对象")
    return value


def _vector(value: object, field: str) -> RawVector3:
    if (
        not isinstance(value, (list, tuple))
        or len(value) != 3
        or any(type(item) is not int for item in value)
    ):
        raise ImuError("invalid_native_observation", f"原生 IMU 字段 {field} 必须是 3 轴整数")
    return RawVector3(value[0], value[1], value[2])


def _sample(value: object) -> ImuSample:
    item = _dict(value, "samples[]")
    raw = _dict(item.get("raw"), "raw")
    sync = _dict(item.get("sync"), "sync")
    quality = sync.get("quality")
    if not isinstance(quality, str) or quality not in {"insufficient", "good", "degraded"}:
        raise ImuError("invalid_native_observation", "原生 IMU 同步质量无效")
    return ImuSample(
        sequence=_int(item.get("sequence"), "sequence"),
        packet_sequence=_int(item.get("packet_sequence"), "packet_sequence"),
        sample_index=_int(item.get("sample_index"), "sample_index"),
        device_timestamp_raw=_int(item.get("device_timestamp_raw"), "device_timestamp_raw"),
        device_ticks=_int(item.get("device_ticks"), "device_ticks"),
        host_read_start_ns=_int(item.get("host_read_start_ns"), "host_read_start_ns"),
        host_read_end_ns=_int(item.get("host_read_end_ns"), "host_read_end_ns"),
        host_monotonic_ns=_int(item.get("host_monotonic_ns"), "host_monotonic_ns"),
        accelerometer=_vector(raw.get("accelerometer"), "raw.accelerometer"),
        gyroscope=_vector(raw.get("gyroscope"), "raw.gyroscope"),
        sync_offset_ns=_optional_int(sync.get("offset_ns"), "sync.offset_ns"),
        sync_residual_ns=_optional_int(sync.get("residual_ns"), "sync.residual_ns"),
        sync_quality=quality,
    )


def _observation(value: object) -> ImuObservation:
    item = _dict(value, "observation")
    samples = item.get("samples")
    if not isinstance(samples, (list, tuple)) or len(samples) != 2:
        raise ImuError("invalid_native_observation", "原生 IMU 必须返回两个样本")
    return ImuObservation(
        samples=(_sample(samples[0]), _sample(samples[1])),
        dropped_samples=_int(item.get("dropped_samples"), "dropped_samples"),
    )


def decode_native_imu_observation(value: object) -> ImuObservation:
    """Convert the Rust IMU observation dict into the Python recording model."""

    return _observation(value)


class NativeImuCollector:
    """Capture decoded IMU samples through the Rust hot path."""

    def __init__(
        self,
        device: str | Path,
        *,
        unit: int | None = None,
        selector: int = 1,
        stale_poll_interval: float = 0.001,
        owner: object | None = None,
    ) -> None:
        self._closed = False
        if owner is None:
            try:
                owner = create_native_imu_collector(
                    os.fspath(device),
                    unit=unit,
                    selector=selector,
                    stale_poll_interval=stale_poll_interval,
                )
            except BaseException as error:
                raise _to_imu_error(error, fallback_code="native_imu_unavailable") from error
        self._owner = owner

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def unit(self) -> int:
        try:
            value = self._owner.unit()
        except BaseException as error:
            raise _to_imu_error(error, fallback_code="native_imu_failed") from error
        if type(value) is not int:
            raise ImuError("invalid_native_observation", "原生 IMU unit 必须是整数")
        return value

    @property
    def native_owner(self) -> object:
        return self._owner

    def read(self, *, timeout: float = 1.0) -> ImuObservation:
        if timeout <= 0:
            raise ValueError("timeout 必须大于零")
        if self._closed:
            raise ImuError("invalid_state", "IMU 采集器已经关闭")
        try:
            raw = self._owner.read(timeout)
            return _observation(raw)
        except BaseException as error:
            imu_error = _to_imu_error(error, fallback_code="native_imu_failed")
            with suppress(BaseException):
                self.close()
            raise imu_error from error

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._owner.close()

    def __enter__(self) -> NativeImuCollector:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()
