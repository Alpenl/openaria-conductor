"""Bounded read-back compression and clock audit alongside acquisition.

Raw recovery journals and WAV files remain authoritative until normal sealing.
Only completed audio segments are read. Journal blocks are independently read
back and decoded as they are written; final native source digests must agree
with the complete stream consumed here before any compacted output is used.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, suppress
from dataclasses import dataclass
from pathlib import Path
from threading import Event

from rp_ylx.native import audio_checkpoint_segments
from rp_ylx.recording.audio_journal import SCHEMA as AUDIO_JOURNAL_SCHEMA
from rp_ylx.recording.audio_journal import AudioJournalReader
from rp_ylx.recording.audit import capture_audit
from rp_ylx.recording.fs_safety import (
    is_exfat_or_msdos_fd,
    make_zstd_compressor,
    write_zstd_frame_checked,
)
from rp_ylx.recording.storage import BLOCK_BYTES, compact_audio, digest
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
        target_stat = self.target.stat(follow_symlinks=False)
        if identity(target_stat, include_inode=self.output.include_inode) != self.output.identity:
            raise ValueError("incremental compacted output changed")
        if path.suffix == ".wav":
            with open_regular(self.target) as stream:
                if (
                    identity(os.fstat(stream.fileno()), include_inode=self.output.include_inode)
                    != self.output.identity
                ):
                    raise ValueError("prepared audio replaced before durable admission")
                os.fsync(stream.fileno())
        return self.target, self.compression, descriptor["path"], path.suffix == ".wav"


def _read_back(path: Path) -> VerifiedArtifact:
    with open_regular(path) as source:
        exfat = is_exfat_or_msdos_fd(source.fileno())
        pinned_before = identity(os.fstat(source.fileno()))
        _, computed_digest = digest(source)
        pinned_after = os.fstat(source.fileno())
        if identity(pinned_after) != pinned_before:
            raise ValueError("artifact changed during independent read-back")
        stored_identity = (
            pinned_before if not exfat else identity(pinned_after, include_inode=False)
        )
    return VerifiedArtifact(stored_identity, computed_digest, include_inode=not exfat)


class _Journal:
    def __init__(self, path: Path):
        self.path = path
        self.target = path.with_suffix(path.suffix + ".zst")
        self.temporary = self.target.with_suffix(self.target.suffix + ".tmp")
        # Allocation can fail and its traceback may be retained by a Future.
        # Do it before acquiring files or creating an exclusive retry barrier.
        self.pending = bytearray()
        self.digest = hashlib.sha256()
        self.encoded_digest = hashlib.sha256()
        self.size = 0
        self.compressor = make_zstd_compressor(level=1)
        self._owns_temporary = False
        self._published = False
        self.source = open_regular(path)
        self._exfat = is_exfat_or_msdos_fd(self.source.fileno())
        try:
            self.source_id = identity(os.fstat(self.source.fileno()))[:2]
            self.output = self.temporary.open("x+b")
            self._owns_temporary = True
        except BaseException:
            self.source.close()
            raise

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
                encoded = write_zstd_frame_checked(
                    self.output,
                    self.compressor,
                    block,
                    verify_pread=True,
                )
                self.encoded_digest.update(encoded)
            if not data:
                break

    def finish(self) -> PreparedArtifact:
        try:
            self.pump(final=True)
            if not self.size:
                encoded = write_zstd_frame_checked(self.output, self.compressor, b"")
                self.encoded_digest.update(encoded)
            self.output.flush()
            os.fsync(self.output.fileno())
            source_stat = os.fstat(self.source.fileno())
            source_identity = identity(source_stat)
            if source_identity[:2] != self.source_id or source_identity[4] != self.size:
                raise ValueError("incremental journal source changed")
            path_identity = identity(
                self.path.stat(follow_symlinks=False),
                include_inode=not self._exfat,
            )
            expected_path_identity = (
                source_identity if not self._exfat else identity(source_stat, include_inode=False)
            )
            if path_identity != expected_path_identity:
                raise ValueError("incremental journal source was replaced")
            self._close_streams()
            os.replace(self.temporary, self.target)
            self._published = True
            computed_digest = self.digest.hexdigest()
            output = _read_back(self.target)
            if output.sha256 != self.encoded_digest.hexdigest():
                raise ValueError("incremental compressed journal changed after block verification")
            return PreparedArtifact(
                VerifiedArtifact(
                    expected_path_identity,
                    computed_digest,
                    include_inode=not self._exfat,
                ),
                self.target,
                output,
                {
                    "codec": "zstd",
                    "level": 1,
                    "block_bytes": BLOCK_BYTES,
                    "uncompressed_bytes": self.size,
                    "uncompressed_sha256": computed_digest,
                },
            )
        except BaseException:
            self.close()
            raise

    def _close_streams(self) -> None:
        try:
            self.source.close()
        finally:
            self.output.close()

    def close(self) -> None:
        try:
            self._close_streams()
        finally:
            if self._owns_temporary and not self._published:
                with suppress(OSError):
                    self.temporary.unlink(missing_ok=True)


def _checkpoint_identity(
    metadata: os.stat_result, *, force: bool, include_inode: bool = True
) -> tuple[int, ...] | None:
    # Atomic publication can unlink the old regular inode at any read boundary.
    # Skip it during capture; the forced post-producer read remains strict.
    if not force and stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 0:
        return None
    return identity(metadata, include_inode=include_inode)


def _read_audio_checkpoint(
    path: Path, expected: tuple[int, ...], *, force: bool, include_inode: bool = True
) -> bytes | None:
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
        exfat = is_exfat_or_msdos_fd(source.fileno())
        metadata = os.fstat(source.fileno())
        before_pinned = _checkpoint_identity(metadata, force=force, include_inode=True)
        before_cmp = _checkpoint_identity(
            metadata, force=force, include_inode=include_inode and not exfat
        )
        if before_pinned is None or before_cmp is None:
            return None
        expected_cmp = (
            expected
            if (include_inode and not exfat) or len(expected) == 6
            else (expected[0], *expected[2:])
        )
        if force and before_cmp != expected_cmp:
            raise ValueError("audio checkpoint changed before final read")
        # Read at most the observed size plus one; concurrent growth cannot
        # keep this read chasing an ever-growing file.
        payload = source.read(metadata.st_size + 1)
        after = _checkpoint_identity(os.fstat(source.fileno()), force=force, include_inode=True)
        if after is None:
            return None
        if after != before_pinned or len(payload) != metadata.st_size:
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
        try:
            self.audit = self.executor.submit(
                capture_audit, root, nominal_fps, frame_decimation, finished=self.finished
            )
            self.compaction = self.executor.submit(self._compact)
        except BaseException:
            # A submit can enqueue work before failing to start its thread.
            # The caller cannot close an object whose constructor never returned.
            # Release any running audit first, then cancel queued work and join.
            self.cancelled.set()
            self.finished.set()
            self.executor.shutdown(wait=True, cancel_futures=True)
            raise

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
        head = json.loads(payload)
        if isinstance(head, dict) and head.get("schema") == AUDIO_JOURNAL_SCHEMA:
            reader = getattr(self, "_audio_journal", None)
            # A fresh final read verifies the entire prefix, even if a previously
            # consumed region was modified after live compaction read it.
            if reader is None or force:
                reader = AudioJournalReader(self.root)
            segments = reader.read(head)["segments"]
            self._audio_journal = reader
        else:
            segments = audio_checkpoint_segments(payload)
        for segment in segments:
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
