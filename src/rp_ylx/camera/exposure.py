"""Standard UVC exposure controls, applied before streaming and read back."""

from __future__ import annotations

import fcntl
import os
import struct


def configure_exposure(device: str, absolute: int | None) -> dict[str, int] | None:
    """100 units = 10 ms. None leaves the operator's camera controls intact."""
    if absolute is None:
        return None
    if type(absolute) is not int or not 1 <= absolute <= 100:
        raise ValueError("camera.exposure_time_absolute must be null or an integer in 1..100")
    fd = os.open(device, os.O_RDWR | os.O_CLOEXEC)
    try:
        result = {}
        for name, control, value in (
            ("auto_exposure", 0x009A0901, 1),
            ("exposure_time_absolute", 0x009A0902, absolute),
        ):
            fcntl.ioctl(fd, 0xC008561C, struct.pack("=Ii", control, value))
            data = bytearray(struct.pack("=Ii", control, 0))
            fcntl.ioctl(fd, 0xC008561B, data, True)
            actual = struct.unpack("=Ii", data)[1]
            if actual != value:
                raise RuntimeError(
                    f"exposure_readback_failed: {name} requested={value}, actual={actual}"
                )
            result[name] = actual
        return result
    finally:
        os.close(fd)
