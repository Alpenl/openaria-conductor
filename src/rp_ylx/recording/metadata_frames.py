"""Strict bounded reader for the project's independently compressed metadata blocks.

Only locate frame/block boundaries here; Zstandard still validates and decodes
all compressed content. Frame boundaries must be complete before decoding:
a stream reader can report EOF before the final checksum has arrived.
"""

from __future__ import annotations

import io

import zstandard


class MetadataFrames(io.RawIOBase):
    """Read concatenated metadata frames without owning the caller's source fd."""

    def __init__(self, source, block_bytes: int):
        super().__init__()
        self.source = source
        self.block_bytes = block_bytes
        self.decoder = zstandard.ZstdDecompressor(max_window_size=block_bytes)
        self.buffer = b""
        self.offset = 0
        self.frames = 0
        self.ended = False

    def readable(self):
        return True

    def _exact(self, count, *, boundary=False):
        data = bytearray()
        while len(data) < count:
            block = self.source.read(count - len(data))
            if not block:
                if boundary and not data and self.frames:
                    return None
                raise zstandard.ZstdError("incomplete metadata compression frame")
            data.extend(block)
        return data

    def _frame(self):
        # frame_header_size includes the 4-byte magic and needs only 5 bytes.
        encoded = self._exact(5, boundary=True)
        if encoded is None:
            self.ended = True
            return b""
        header_bytes = zstandard.frame_header_size(encoded)
        encoded.extend(self._exact(header_bytes - len(encoded)))
        parameters = zstandard.get_frame_parameters(encoded)
        if parameters.window_size > self.block_bytes or (
            parameters.content_size != zstandard.CONTENTSIZE_UNKNOWN
            and parameters.content_size > self.block_bytes
        ):
            raise zstandard.ZstdError("metadata compression frame exceeds block limit")
        # RFC 8878: 3-byte little-endian block header, low bit is Last_Block,
        # next two bits are type; RLE stores one byte, raw/compressed store size.
        # Our producer compresses <=1 MiB per frame. Bound encoded input too,
        # including pathological sequences of zero-length blocks.
        while True:
            header = self._exact(3)
            value = int.from_bytes(header, "little")
            kind, size = (value >> 1) & 3, value >> 3
            if kind == 3 or size > 128 * 1024:
                raise zstandard.ZstdError("invalid metadata compression block")
            stored = 1 if kind == 1 else size
            if len(encoded) + 3 + stored + 4 > 2 * self.block_bytes:
                raise zstandard.ZstdError("encoded metadata frame exceeds bounded size")
            encoded.extend(header)
            encoded.extend(self._exact(stored))
            if value & 1:
                break
        if parameters.has_checksum:
            encoded.extend(self._exact(4))
        # Use bounded streaming even for a known-size frame. The one-shot
        # decoder has an empty-content fast path that can skip checksum checking.
        # Framing above has already proved that the complete footer is present.
        decoded = bytearray()
        with self.decoder.stream_reader(io.BytesIO(encoded), read_across_frames=False) as reader:
            while block := reader.read(self.block_bytes + 1 - len(decoded)):
                decoded.extend(block)
                if len(decoded) > self.block_bytes:
                    raise zstandard.ZstdError("decoded metadata frame exceeds block limit")
        self.frames += 1
        return bytes(decoded)

    def readinto(self, target):
        if self.closed:
            raise ValueError("read from closed metadata stream")
        target = memoryview(target).cast("B")
        if not target:
            return 0
        while self.offset == len(self.buffer):
            if self.ended:
                return 0
            self.buffer = self._frame()
            self.offset = 0
        count = min(len(target), len(self.buffer) - self.offset)
        target[:count] = memoryview(self.buffer)[self.offset : self.offset + count]
        self.offset += count
        return count

    def close(self):
        self.buffer = b""
        self.offset = 0
        super().close()
