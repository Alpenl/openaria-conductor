"""v0 左右眼帧流的最小二进制封装。"""

from __future__ import annotations

import struct
from collections.abc import Iterator
from typing import BinaryIO

MAGIC = b"YLXFRM0\n"
_LENGTH = struct.Struct(">I")
MAX_FRAME_BYTES = 64 * 1024 * 1024


class FrameStreamError(ValueError):
    pass


def write_header(stream: BinaryIO) -> None:
    stream.write(MAGIC)


def write_frame(stream: BinaryIO, payload: bytes) -> None:
    stream.write(encode_frame(payload))


def encode_frame(payload: bytes) -> bytes:
    if not payload or len(payload) > MAX_FRAME_BYTES:
        raise FrameStreamError("帧长度必须在 1 到 64 MiB 之间")
    return _LENGTH.pack(len(payload)) + payload


def iter_frames(stream: BinaryIO) -> Iterator[bytes]:
    if stream.read(len(MAGIC)) != MAGIC:
        raise FrameStreamError("帧流魔数不正确")
    while True:
        length_bytes = stream.read(_LENGTH.size)
        if not length_bytes:
            return
        if len(length_bytes) != _LENGTH.size:
            raise FrameStreamError("帧长度字段被截断")
        (length,) = _LENGTH.unpack(length_bytes)
        if length == 0 or length > MAX_FRAME_BYTES:
            raise FrameStreamError("帧长度超出允许范围")
        payload = stream.read(length)
        if len(payload) != length:
            raise FrameStreamError("帧数据被截断")
        yield payload
