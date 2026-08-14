"""YLX 2UQ2 27 字节 XU IMU 包解码。"""

from __future__ import annotations

import struct
from dataclasses import dataclass

from rp_ylx.imu.models import ImuError, RawVector3

PACKET_BYTES = 27
SAMPLES_PER_PACKET = 2
DEVICE_TIMESTAMP_BITS = 24
DEVICE_TIMESTAMP_MODULUS = 1 << DEVICE_TIMESTAMP_BITS
VENDOR_ID = 0x1BCF
PRODUCT_ID = 0x0B15
XU_GUID = "D5C2F42C-1808-4D9F-BE56-753E271C9244"
XU_SELECTOR = 1

_AXES = struct.Struct(">12h")


@dataclass(frozen=True, slots=True)
class DecodedRawSample:
    sample_index: int
    accelerometer: RawVector3
    gyroscope: RawVector3


@dataclass(frozen=True, slots=True)
class DecodedPacket:
    device_timestamp_raw: int
    samples: tuple[DecodedRawSample, DecodedRawSample]


def decode_packet(payload: bytes) -> DecodedPacket:
    if len(payload) != PACKET_BYTES:
        raise ImuError(
            "invalid_packet_length",
            f"XU IMU 包必须正好 {PACKET_BYTES} 字节，实际为 {len(payload)}",
        )
    timestamp = int.from_bytes(payload[:3], "big", signed=False)
    axes = _AXES.unpack(payload[3:])
    samples = []
    for index in range(SAMPLES_PER_PACKET):
        start = index * 6
        samples.append(
            DecodedRawSample(
                sample_index=index,
                accelerometer=RawVector3(*axes[start : start + 3]),
                gyroscope=RawVector3(*axes[start + 3 : start + 6]),
            )
        )
    return DecodedPacket(timestamp, (samples[0], samples[1]))
