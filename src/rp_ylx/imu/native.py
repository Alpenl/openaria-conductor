"""Rust UVC XU IMU collector adapter."""

from __future__ import annotations

from rp_ylx.imu.models import ImuError, ImuObservation, ImuSample, RawVector3


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
