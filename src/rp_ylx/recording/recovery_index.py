"""Bounded derived-index writing with complete-frame rollback checkpoints.

The caller owns the temporary file and its atomic publication. Compression never
changes the acquisition journals, and every compressed result is independently
read back before it can replace a previously published derived index.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import zstandard

from rp_ylx.recording.storage import BLOCK_BYTES, _digest
from rp_ylx.recording.verified import open_regular


class RecoveryIndex:
    def __init__(self, compressed: bool):
        # Allocate fallible compression state before the caller acquires an fd.
        self.compressor = (
            zstandard.ZstdCompressor(level=1, write_checksum=True) if compressed else None
        )
        self.pending = bytearray()
        self.size = 0
        self.digest = hashlib.sha256()
        self.encoded_digest = hashlib.sha256()
        self.encoded_size = 0

    def _write(self, output, payload):
        if output.write(payload) != len(payload):
            raise OSError("short recovery index write")
        if self.compressor is not None:
            self.encoded_size += len(payload)
            self.encoded_digest.update(payload)

    def _flush(self, output):
        if self.pending:
            self._write(output, self.compressor.compress(self.pending))
            self.pending.clear()

    def write(self, output, payload: bytes):
        if self.compressor is None:
            self._write(output, payload)
            return
        self.size += len(payload)
        self.digest.update(payload)
        view = memoryview(payload)
        offset = 0
        while offset < len(view):
            take = min(BLOCK_BYTES - len(self.pending), len(view) - offset)
            self.pending.extend(view[offset : offset + take])
            offset += take
            if len(self.pending) == BLOCK_BYTES:
                self._flush(output)

    def checkpoint(self, output):
        # A video boundary ends a complete checksummed Zstandard frame, so an
        # incomplete next segment can be truncated without retaining its bytes.
        self._flush(output)
        return (
            output.tell(),
            self.size,
            self.digest.copy(),
            self.encoded_size,
            self.encoded_digest.copy(),
        )

    def rollback(self, output, checkpoint):
        offset, self.size, digest, self.encoded_size, encoded_digest = checkpoint
        self.encoded_digest = encoded_digest.copy()
        output.seek(offset)
        output.truncate()
        self.pending.clear()
        self.digest = digest.copy()

    def finish(self, output):
        if self.compressor is None:
            return
        self._flush(output)
        if self.size == 0:
            self._write(output, self.compressor.compress(b""))

    def verify(self, temporary: Path) -> dict | None:
        if self.compressor is None:
            return None
        with open_regular(temporary) as raw:
            if _digest(raw) != (self.encoded_size, self.encoded_digest.hexdigest()):
                raise ValueError("recovery index compressed-byte readback mismatch")
            raw.seek(0)
            with zstandard.ZstdDecompressor(max_window_size=BLOCK_BYTES).stream_reader(
                raw, read_across_frames=True
            ) as decoded:
                if _digest(decoded) != (self.size, self.digest.hexdigest()):
                    raise ValueError("recovery index compression round-trip mismatch")
        return {
            "codec": "zstd",
            "level": 1,
            "block_bytes": BLOCK_BYTES,
            "uncompressed_bytes": self.size,
            "uncompressed_sha256": self.digest.hexdigest(),
        }
