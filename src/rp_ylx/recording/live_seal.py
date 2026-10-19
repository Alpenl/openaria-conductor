"""Bounded read-back compression and clock audit alongside acquisition.

Raw recovery journals and WAV files remain authoritative until normal sealing.
Only completed audio segments are read. Journal blocks are independently read
back and decoded as they are written; final native source digests must agree
with the complete stream consumed here before any compacted output is used.
"""

from __future__ import annotations

import hashlib
import os
import stat
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from threading import Event

import zstandard

from rp_ylx.native import audio_checkpoint_segments
from rp_ylx.recording.audit import capture_audit
from rp_ylx.recording.storage import BLOCK_BYTES, _digest, compact_audio
from rp_ylx.recording.verified import VerifiedArtifact, identity, open_regular


@dataclass(frozen=True)
class PreparedArtifact:
    source: VerifiedArtifact
    target: Path
    output: VerifiedArtifact
    compression: dict

    def use(self, root: Path, descriptor: dict):
        result = self.prepare_for_finalization(root, descriptor)
        if not self.output.unchanged(self.target):
            raise ValueError("incremental compacted output changed")
        return result

    def prepare_for_finalization(self, root: Path, descriptor: dict):
        """Check the source and sync output; caller must read and match output proof.

        compact_manifest performs the fresh final read on the owning thread,
        then compares bytes, digest and identity before updating its descriptor.
        This preparation alone never admits the output's current content.
        """
        path = root / descriptor["path"]
        if not self.source.matches(
            path.stat(follow_symlinks=False), descriptor["bytes"], descriptor["sha256"]
        ) or not self.source.unchanged(path):
            raise ValueError("source changed after incremental lossless compaction")
        if identity(self.target.stat(follow_symlinks=False)) != self.output.identity:
            raise ValueError("incremental compacted output changed")
        if path.suffix == ".wav":
            with open_regular(self.target) as stream:
                if identity(os.fstat(stream.fileno())) != self.output.identity:
                    raise ValueError("prepared audio replaced before durable admission")
                os.fsync(stream.fileno())
        return self.target, self.compression, descriptor["path"], path.suffix == ".wav"


def _read_back(path: Path) -> VerifiedArtifact:
    with open_regular(path) as source:
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
        self.source = open_regular(path)
        try:
            self.source_id = identity(os.fstat(self.source.fileno()))[:2]
            self.output = self.temporary.open("x+b")
        except BaseException:
            self.source.close()
            raise
        self.pending = bytearray()
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
            self.pending.extend(data)
            if len(self.pending) == BLOCK_BYTES or (final and not data and self.pending):
                block, self.pending = self.pending, bytearray()
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
        try:
            self.source.close()
        finally:
            self.output.close()


def _checkpoint_identity(metadata: os.stat_result, *, force: bool) -> tuple[int, ...] | None:
    # Atomic publication can unlink the old regular inode at any read boundary.
    # Skip it during capture; the forced post-producer read remains strict.
    if not force and stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 0:
        return None
    return identity(metadata)


def _read_audio_checkpoint(path: Path, expected: tuple[int, ...], *, force: bool) -> bytes | None:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        if force:
            raise
        return None
    try:
        source = os.fdopen(descriptor, "rb")
    except BaseException:
        os.close(descriptor)
        raise
    with source:
        metadata = os.fstat(source.fileno())
        before = _checkpoint_identity(metadata, force=force)
        if before is None:
            return None
        if force and before != expected:
            raise ValueError("audio checkpoint changed before final read")
        # Read at most the observed size plus one; concurrent growth cannot
        # keep this read chasing an ever-growing file.
        payload = source.read(metadata.st_size + 1)
        after = _checkpoint_identity(os.fstat(source.fileno()), force=force)
        if after is None:
            return None
        if after != before or len(payload) != metadata.st_size:
            raise ValueError("audio checkpoint changed during read")
        return payload


class LiveSealing:
    def __init__(self, root: Path, nominal_fps: float, frame_decimation: int):
        self.root = root
        self.finished = Event()
        self.cancelled = Event()
        self.prepared: dict[str, PreparedArtifact] = {}
        self._audio_checkpoint_identity: tuple[int, ...] | None = None
        self.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="live-seal")
        self.audit = self.executor.submit(
            capture_audit, root, nominal_fps, frame_decimation, finished=self.finished
        )
        self.compaction = self.executor.submit(self._compact)

    def _audio(self, *, force: bool = False) -> None:
        # Native checkpoint publication follows WAV close/fsync. Reading only
        # paths explicitly named in this atomic checkpoint avoids open tails.
        checkpoint = self.root / "audio/checkpoint.json"
        try:
            metadata = checkpoint.stat(follow_symlinks=False)
        except FileNotFoundError:
            self._audio_checkpoint_identity = None
            return
        # Native atomically replaces this checkpoint while capture is running.
        # stat() can resolve the old inode just before rename unlinks it, then
        # report nlink=0. Retry on the next poll without caching that snapshot.
        # The forced final poll runs after producers join and remains strict.
        checkpoint_identity = _checkpoint_identity(metadata, force=force)
        if checkpoint_identity is None:
            return
        if not force and checkpoint_identity == self._audio_checkpoint_identity:
            return
        payload = _read_audio_checkpoint(checkpoint, checkpoint_identity, force=force)
        if payload is None:
            return
        for segment in audio_checkpoint_segments(payload):
            relative = segment["path"]
            filename = f"audio_{segment['index']:05d}.wav"
            # Native emits canonical paths. Avoid rebuilding every historical
            # Path on each checkpoint, but still validate its index binding.
            # Keep the prior normalization rules for noncanonical spellings.
            if relative != f"audio/{filename}" and Path(relative).parts != ("audio", filename):
                raise ValueError("invalid incremental audio checkpoint path")
            if relative in self.prepared:
                continue
            source_path = self.root / relative
            original = _read_back(source_path)
            # WAV + its recovery checkpoint are already durable. This derived
            # cache is synced on admission after capture stops, not in parallel
            # with the acquisition commit worker on the same microSD.
            target, compression = compact_audio(source_path, durable=False)
            if not original.unchanged(source_path):
                raise ValueError("audio changed during incremental compaction")
            self.prepared[relative] = PreparedArtifact(
                original, target, _read_back(target), compression
            )
        # Cache the identity from before the read. If native publication races
        # it, the next poll sees the replacement and reads again; recording the
        # path's identity after the read could hide a newly published tail.
        self._audio_checkpoint_identity = checkpoint_identity

    def _compact(self) -> None:
        journals: dict[str, _Journal] = {}
        with ExitStack() as resources:
            for name in ("frames.ndjson", "imu.ndjson"):
                journals[name] = _Journal(self.root / name)
                resources.callback(journals[name].close)
            while not self.cancelled.is_set():
                for journal in journals.values():
                    journal.pump()
                self._audio()
                if self.finished.is_set():
                    # finish() may race the checkpoint read above. Producers
                    # are now joined, so include the final closed audio tail.
                    self._audio(force=True)
                    for name, journal in journals.items():
                        self.prepared[name] = journal.finish()
                    return
                self.finished.wait(0.1)

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
