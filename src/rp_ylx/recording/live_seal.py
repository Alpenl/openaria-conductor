"""Bounded read-back compression and clock audit alongside acquisition.

Raw recovery journals and WAV files remain authoritative until normal sealing.
Only completed audio segments are read. Journal blocks are independently read
back and decoded as they are written; final native source digests must agree
with the complete stream consumed here before any compacted output is used.
"""

from __future__ import annotations

import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Event

import zstandard

from rp_ylx.recording.audit import capture_audit
from rp_ylx.recording.storage import BLOCK_BYTES, _digest, compact_audio
from rp_ylx.recording.verified import VerifiedArtifact, identity


@dataclass(frozen=True)
class PreparedArtifact:
    source: VerifiedArtifact
    target: Path
    output: VerifiedArtifact
    compression: dict

    def use(self, root: Path, descriptor: dict):
        path = root / descriptor["path"]
        if not self.source.matches(
            path.stat(follow_symlinks=False), descriptor["bytes"], descriptor["sha256"]
        ):
            raise ValueError("source changed after incremental lossless compaction")
        if not self.output.unchanged(self.target):
            raise ValueError("incremental compacted output changed")
        return self.target, self.compression, descriptor["path"], path.suffix == ".wav"


def _read_back(path: Path) -> VerifiedArtifact:
    with path.open("rb") as source:
        before = identity(os.fstat(source.fileno()))
        _, digest = _digest(source)
        if identity(os.fstat(source.fileno())) != before:
            raise ValueError("artifact changed during independent read-back")
    return VerifiedArtifact(before, digest)


class _Journal:
    def __init__(self, path: Path):
        self.path = path
        self.target = path.with_suffix(path.suffix + ".zst")
        self.temporary = self.target.with_suffix(self.target.suffix + ".tmp")
        self.source = path.open("rb")
        self.source_id = identity(os.fstat(self.source.fileno()))[:2]
        try:
            self.output = self.temporary.open("x+b")
        except BaseException:
            self.source.close()
            raise
        self.pending = b""
        self.digest = hashlib.sha256()
        self.encoded_digest = hashlib.sha256()
        self.size = 0
        self.compressor = zstandard.ZstdCompressor(level=1, write_checksum=True)

    def pump(self, *, final: bool = False) -> None:
        # Normally a few KB arrive per poll; bound a catch-up pass to 4 MiB.
        passes = 0
        while final or passes < 4:
            passes += 1
            data = self.source.read(BLOCK_BYTES - len(self.pending))
            self.pending += data
            if len(self.pending) == BLOCK_BYTES or (final and not data and self.pending):
                block, self.pending = self.pending, b""
                self.digest.update(block)
                self.size += len(block)
                encoded = self.compressor.compress(block)
                self.encoded_digest.update(encoded)
                offset = self.output.tell()
                if self.output.write(encoded) != len(encoded):
                    raise OSError("short incremental journal write")
                self.output.flush()
                stored = os.pread(self.output.fileno(), len(encoded), offset)
                if zstandard.ZstdDecompressor().decompress(stored) != block:
                    raise ValueError("incremental metadata round-trip mismatch")
            if not data:
                break

    def finish(self) -> PreparedArtifact:
        self.pump(final=True)
        if not self.size:
            encoded = self.compressor.compress(b"")
            self.output.write(encoded)
            self.encoded_digest.update(encoded)
        self.output.flush()
        os.fsync(self.output.fileno())
        source_identity = identity(os.fstat(self.source.fileno()))
        if source_identity[:2] != self.source_id or source_identity[4] != self.size:
            raise ValueError("incremental journal source changed")
        if identity(self.path.stat(follow_symlinks=False)) != source_identity:
            raise ValueError("incremental journal source was replaced")
        self.close()
        os.replace(self.temporary, self.target)
        digest = self.digest.hexdigest()
        output = _read_back(self.target)
        if output.sha256 != self.encoded_digest.hexdigest():
            raise ValueError("incremental compressed journal changed after block verification")
        return PreparedArtifact(
            VerifiedArtifact(source_identity, digest),
            self.target,
            output,
            {
                "codec": "zstd",
                "level": 1,
                "block_bytes": BLOCK_BYTES,
                "uncompressed_bytes": self.size,
                "uncompressed_sha256": digest,
            },
        )

    def close(self) -> None:
        self.source.close()
        self.output.close()


class LiveSealing:
    def __init__(self, root: Path, nominal_fps: float, frame_decimation: int):
        self.root = root
        self.finished = Event()
        self.cancelled = Event()
        self.prepared: dict[str, PreparedArtifact] = {}
        self.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="live-seal")
        self.audit = self.executor.submit(
            capture_audit, root, nominal_fps, frame_decimation, finished=self.finished
        )
        self.compaction = self.executor.submit(self._compact)

    def _audio(self) -> None:
        # Native checkpoint publication follows WAV close/fsync. Reading only
        # paths explicitly named in this atomic checkpoint avoids open tails.
        import json

        checkpoint = self.root / "audio/checkpoint.json"
        if not checkpoint.exists():
            return
        for segment in json.loads(checkpoint.read_bytes())["segments"]:
            relative = segment["path"]
            path = Path(relative)
            if path.parts != ("audio", f"audio_{segment['index']:05d}.wav"):
                raise ValueError("invalid incremental audio checkpoint path")
            if relative in self.prepared:
                continue
            source_path = self.root / path
            original = _read_back(source_path)
            target, compression = compact_audio(source_path)
            if not original.unchanged(source_path):
                raise ValueError("audio changed during incremental compaction")
            self.prepared[relative] = PreparedArtifact(
                original, target, _read_back(target), compression
            )

    def _compact(self) -> None:
        journals: dict[str, _Journal] = {}
        try:
            for name in ("frames.ndjson", "imu.ndjson"):
                journals[name] = _Journal(self.root / name)
            while not self.cancelled.is_set():
                for journal in journals.values():
                    journal.pump()
                self._audio()
                if self.finished.is_set():
                    for name, journal in journals.items():
                        self.prepared[name] = journal.finish()
                    return
                self.finished.wait(0.1)
        finally:
            for journal in journals.values():
                journal.close()

    def finish(self) -> dict:
        # The caller has already stopped and joined every producer.
        self.finished.set()
        try:
            self.compaction.result()
            return self.audit.result()
        finally:
            self.executor.shutdown(wait=True)

    def close(self) -> None:
        self.cancelled.set()
        self.finished.set()
        self.executor.shutdown(wait=True)
