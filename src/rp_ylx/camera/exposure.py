"""Standard UVC exposure controls, applied before streaming and read back."""

from __future__ import annotations

import errno
import fcntl
import os
import struct
from collections.abc import Callable
from typing import Literal, TypeVar

from rp_ylx.native import NativeModuleError, configure_native_exposure

_ORIGINAL_OS_OPEN = os.open
_ORIGINAL_OS_CLOSE = os.close
_ORIGINAL_FCNTL_IOCTL = fcntl.ioctl

_T = TypeVar("_T")


def _retry_eintr(fn: Callable[[], _T]) -> _T:
    while True:
        try:
            return fn()
        except OSError as exc:
            if exc.errno != errno.EINTR:
                raise


def _configure_exposure_python(device: str, absolute: int | Literal["auto"]) -> dict[str, int]:
    fd = _retry_eintr(lambda: os.open(device, os.O_RDWR | os.O_CLOEXEC))
    try:
        result: dict[str, int] = {}
        controls = [("auto_exposure", 0x009A0901, 3 if absolute == "auto" else 1)]
        if absolute != "auto":
            controls.append(("exposure_time_absolute", 0x009A0902, absolute))
        for name, control, value in controls:
            packed = struct.pack("=Ii", control, value)
            _retry_eintr(lambda p=packed: fcntl.ioctl(fd, 0xC008561C, p))
            data = bytearray(struct.pack("=Ii", control, 0))
            _retry_eintr(lambda d=data: fcntl.ioctl(fd, 0xC008561B, d, True))
            actual = struct.unpack("=Ii", data)[1]
            if actual != value:
                raise RuntimeError(
                    f"exposure_readback_failed: {name} requested={value}, actual={actual}"
                )
            result[name] = actual
        return result
    finally:
        os.close(fd)


def configure_exposure(
    device: str, absolute: int | Literal["auto"] | None
) -> dict[str, int] | None:
    """Default auto adapts to lighting; numeric values explicitly select manual."""
    if absolute is None:
        return None
    if absolute != "auto" and (type(absolute) is not int or not 1 <= absolute <= 100):
        raise ValueError("camera.exposure_time_absolute must be auto, null or an integer in 1..100")
    unpatched = (
        os.open is _ORIGINAL_OS_OPEN
        and os.close is _ORIGINAL_OS_CLOSE
        and fcntl.ioctl is _ORIGINAL_FCNTL_IOCTL
    )
    if unpatched:
        try:
            return configure_native_exposure(device, absolute)
        except NativeModuleError as exc:
            if exc.code not in {
                "native_exposure_unavailable",
                "native_focus_unavailable",
                "native_module_unavailable",
            }:
                raise
    return _configure_exposure_python(device, absolute)
