"""Optional lossless SBS JPEG crops through the TurboJPEG C API."""

from __future__ import annotations

import ctypes
import ctypes.util
from functools import lru_cache
from typing import Any

TJXOP_NONE = 0
TJXOPT_CROP = 4


class TurboJpegError(RuntimeError):
    """TurboJPEG rejected or could not complete a lossless transform."""


class TJRegion(ctypes.Structure):
    _fields_ = [
        ("x", ctypes.c_int),
        ("y", ctypes.c_int),
        ("w", ctypes.c_int),
        ("h", ctypes.c_int),
    ]


class TJTransform(ctypes.Structure):
    _fields_ = [
        ("r", TJRegion),
        ("op", ctypes.c_int),
        ("options", ctypes.c_int),
        ("data", ctypes.c_void_p),
        ("custom_filter", ctypes.c_void_p),
    ]


def _configure_library(library: Any) -> Any:
    byte_pointer = ctypes.POINTER(ctypes.c_ubyte)
    library.tjInitTransform.argtypes = []
    library.tjInitTransform.restype = ctypes.c_void_p
    library.tjTransform.argtypes = [
        ctypes.c_void_p,
        byte_pointer,
        ctypes.c_ulong,
        ctypes.c_int,
        ctypes.POINTER(byte_pointer),
        ctypes.POINTER(ctypes.c_ulong),
        ctypes.POINTER(TJTransform),
        ctypes.c_int,
    ]
    library.tjTransform.restype = ctypes.c_int
    library.tjDestroy.argtypes = [ctypes.c_void_p]
    library.tjDestroy.restype = ctypes.c_int
    library.tjFree.argtypes = [byte_pointer]
    library.tjFree.restype = None
    library.tjGetErrorStr2.argtypes = [ctypes.c_void_p]
    library.tjGetErrorStr2.restype = ctypes.c_char_p
    return library


@lru_cache(maxsize=1)
def _load_library() -> Any | None:
    path = ctypes.util.find_library("turbojpeg")
    if path is None:
        return None
    try:
        return _configure_library(ctypes.CDLL(path))
    except (AttributeError, OSError):
        return None


def _error_message(library: Any, handle: object | None) -> str:
    message = library.tjGetErrorStr2(handle)
    if isinstance(message, bytes):
        return message.decode("utf-8", errors="replace")
    return "TurboJPEG transform failed"


def _transform_sbs_jpeg(
    library: Any, payload: bytes, width: int, height: int
) -> tuple[bytes, bytes]:
    eye_width = width // 2
    handle = library.tjInitTransform()
    if not handle:
        raise TurboJpegError(_error_message(library, None))

    source = (ctypes.c_ubyte * len(payload)).from_buffer_copy(payload)
    byte_pointer = ctypes.POINTER(ctypes.c_ubyte)
    outputs = (byte_pointer * 2)()
    sizes = (ctypes.c_ulong * 2)()
    transforms = (TJTransform * 2)(
        TJTransform(TJRegion(0, 0, eye_width, height), TJXOP_NONE, TJXOPT_CROP),
        TJTransform(
            TJRegion(eye_width, 0, eye_width, height),
            TJXOP_NONE,
            TJXOPT_CROP,
        ),
    )
    try:
        result = library.tjTransform(
            handle,
            source,
            len(payload),
            2,
            outputs,
            sizes,
            transforms,
            0,
        )
        if result != 0:
            raise TurboJpegError(_error_message(library, handle))
        return (
            ctypes.string_at(outputs[0], sizes[0]),
            ctypes.string_at(outputs[1], sizes[1]),
        )
    finally:
        for output in outputs:
            if output:
                library.tjFree(output)
        library.tjDestroy(handle)


def lossless_crop_sbs_jpeg(payload: bytes, width: int, height: int) -> tuple[bytes, bytes] | None:
    """Return two lossless JPEG crops, or None when TurboJPEG cannot be used."""

    library = _load_library()
    if library is None:
        return None
    try:
        return _transform_sbs_jpeg(library, payload, width, height)
    except (OSError, TurboJpegError, ValueError, ctypes.ArgumentError):
        return None
