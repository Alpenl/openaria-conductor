"""IMU 采集数据类型。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class ImuError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        self.code = code
        self.message = message
        self.retryable = retryable
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True, slots=True)
class RawVector3:
    x: int
    y: int
    z: int

    def __post_init__(self) -> None:
        if any(value < -32768 or value > 32767 for value in (self.x, self.y, self.z)):
            raise ValueError("IMU 原始轴值必须是 int16")

    def as_list(self) -> list[int]:
        return [self.x, self.y, self.z]


@dataclass(frozen=True, slots=True)
class ImuPacketRead:
    payload: bytes
    host_read_start_ns: int
    host_read_end_ns: int
    lost_packets: int = 0

    def __post_init__(self) -> None:
        if (
            self.host_read_start_ns < 0
            or self.host_read_end_ns < self.host_read_start_ns
            or self.lost_packets < 0
        ):
            raise ValueError("IMU 读取区间或丢包数无效")


@dataclass(frozen=True, slots=True)
class ImuSample:
    sequence: int
    packet_sequence: int
    sample_index: int
    device_timestamp_raw: int
    device_ticks: int
    host_read_start_ns: int
    host_read_end_ns: int
    host_monotonic_ns: int
    accelerometer: RawVector3
    gyroscope: RawVector3
    sync_offset_ns: int | None
    sync_residual_ns: int | None
    sync_quality: str

    def as_record(self, session_id: str) -> dict[str, Any]:
        return {
            "format": "ylx.imu.v0",
            "session_id": session_id,
            "sequence": self.sequence,
            "packet_sequence": self.packet_sequence,
            "sample_index": self.sample_index,
            "device_timestamp_raw": self.device_timestamp_raw,
            "device_ticks": self.device_ticks,
            "host_read_start_ns": self.host_read_start_ns,
            "host_read_end_ns": self.host_read_end_ns,
            "host_monotonic_ns": self.host_monotonic_ns,
            "raw": {
                "accelerometer": self.accelerometer.as_list(),
                "gyroscope": self.gyroscope.as_list(),
            },
            "sync": {
                "offset_ns": self.sync_offset_ns,
                "residual_ns": self.sync_residual_ns,
                "quality": self.sync_quality,
            },
        }


@dataclass(frozen=True, slots=True)
class ImuObservation:
    samples: tuple[ImuSample, ImuSample]
    dropped_samples: int


class ImuSource(Protocol):
    def read_packet(self, timeout: float) -> ImuPacketRead: ...

    def close(self) -> None: ...
