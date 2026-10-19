"""Verified, bounded lossless compaction and prepared-artifact admission.

Source files survive until the new manifest has been atomically sealed. Each
1 MiB Zstandard block is a complete checksummed frame; an interrupted compaction
never changes the acquisition journal used by recovery.
"""

from __future__ import annotations

import hashlib
import io
import os
import selectors
import subprocess
import time
import wave
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import zstandard

from rp_ylx.recording.verified import open_regular

BLOCK_BYTES = 1024 * 1024
AUDIO_CODEC_TIMEOUT_SECONDS = 120


@contextmanager
def open_metadata(path: Path):
    with open_regular(path) as raw:
        if path.suffix == ".zst":
            with (
                zstandard.ZstdDecompressor(max_window_size=BLOCK_BYTES).stream_reader(
                    raw, read_across_frames=True
                ) as reader,
                io.BufferedReader(reader) as buffered,
            ):
                yield buffered
        else:
            yield raw


def _digest(stream) -> tuple[int, str]:
    size = 0
    digest = hashlib.sha256()
    while block := stream.read(BLOCK_BYTES):
        size += len(block)
        digest.update(block)
    return size, digest.hexdigest()


def compact_metadata(path: Path) -> tuple[Path, dict]:
    target = path.with_suffix(path.suffix + ".zst")
    temporary = target.with_suffix(target.suffix + ".tmp")
    compressor = zstandard.ZstdCompressor(level=1, write_checksum=True)
    size, digest = 0, hashlib.sha256()
    with open_regular(path) as source, temporary.open("xb") as output:
        while block := source.read(BLOCK_BYTES):
            size += len(block)
            digest.update(block)
            output.write(compressor.compress(block))
        if size == 0:
            output.write(compressor.compress(b""))
        output.flush()
        os.fsync(output.fileno())
    # Name remains temporary until the complete byte-for-byte round trip passes.
    with (
        open_regular(temporary) as raw,
        zstandard.ZstdDecompressor().stream_reader(raw, read_across_frames=True) as decoded,
    ):
        if _digest(decoded) != (size, digest.hexdigest()):
            raise ValueError("metadata compression round-trip mismatch")
    os.replace(temporary, target)
    return target, {
        "codec": "zstd",
        "level": 1,
        "block_bytes": BLOCK_BYTES,
        "uncompressed_bytes": size,
        "uncompressed_sha256": digest.hexdigest(),
    }


def _pcm_readback(command: list[str], *, expected_bytes: int) -> tuple[int, str]:
    """Bound both pipe consumption and process exit with one wall-clock deadline."""
    timeout = AUDIO_CODEC_TIMEOUT_SECONDS
    deadline = time.monotonic() + timeout
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    size, digest = 0, hashlib.sha256()
    try:
        assert process.stdout is not None
        descriptor = process.stdout.fileno()
        os.set_blocking(descriptor, False)
        with selectors.DefaultSelector() as selector:
            selector.register(descriptor, selectors.EVENT_READ)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    raise subprocess.TimeoutExpired(command, timeout)
                try:
                    block = os.read(descriptor, BLOCK_BYTES)
                except BlockingIOError:
                    continue
                if not block:
                    break
                size += len(block)
                if size > expected_bytes:
                    raise ValueError("FLAC PCM readback exceeds source length")
                digest.update(block)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(command, timeout)
        if process.wait(timeout=remaining) != 0:
            raise ValueError("FLAC PCM decoder failed")
        return size, digest.hexdigest()
    finally:
        # Popen.__exit__ waits without a timeout, including on TimeoutExpired.
        # Explicitly terminate and reap our decoder before closing its pipe.
        try:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
        finally:
            if process.stdout is not None:
                process.stdout.close()


def compact_audio(path: Path, *, durable: bool = True) -> tuple[Path, dict]:
    target = path.with_suffix(".flac")
    temporary = target.with_suffix(".flac.tmp")
    with open_regular(path) as raw, wave.open(raw, "rb") as source:
        if source.getsampwidth() != 2 or source.getcomptype() != "NONE":
            raise ValueError("lossless audio requires signed 16-bit PCM")
        pcm = hashlib.sha256()
        pcm_bytes = 0
        while block := source.readframes(BLOCK_BYTES // (2 * source.getnchannels())):
            pcm.update(block)
            pcm_bytes += len(block)
    subprocess.run(
        [
            "ffmpeg",
            "-nostdin",
            "-v",
            "error",
            "-n",
            "-i",
            str(path),
            "-map",
            "0:a:0",
            "-c:a",
            "flac",
            "-compression_level",
            "5",
            "-threads",
            "1",
            "-f",
            "flac",
            str(temporary),
        ],
        check=True,
        timeout=AUDIO_CODEC_TIMEOUT_SECONDS,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    actual = _pcm_readback(
        [
            "ffmpeg",
            "-nostdin",
            "-v",
            "error",
            "-i",
            str(temporary),
            "-threads",
            "1",
            "-map",
            "0:a:0",
            "-f",
            "s16le",
            "-c:a",
            "pcm_s16le",
            "-",
        ],
        expected_bytes=pcm_bytes,
    )
    if actual != (pcm_bytes, pcm.hexdigest()):
        raise ValueError("FLAC PCM round-trip mismatch")
    if durable:
        with open_regular(temporary) as stream:
            os.fsync(stream.fileno())
    os.replace(temporary, target)
    return target, {"codec": "flac", "pcm_bytes": pcm_bytes, "pcm_sha256": pcm.hexdigest()}


def compact_manifest(
    root: Path, manifest: dict, finalize, *, progress=None, prepared=None
) -> tuple[dict, list[str]]:
    """Caller owns manifest, updates file identities, then seals before cleanup."""
    originals = []
    descriptors = [manifest["frames"]["artifact"], manifest["imu"]["artifact"]]
    descriptors += [
        segment["artifact"] for segment in manifest.get("audio", {}).get("segments", [])
    ]

    def compact(descriptor):
        relative = descriptor["path"]
        if prepared is not None and relative in prepared:
            return prepared[relative].prepare_for_finalization(root, descriptor)
        path = root / relative
        audio = descriptor["media_type"] == "audio/wav"
        with open_regular(path) as source:
            expected_source = _digest(source)
        if expected_source != (descriptor["bytes"], descriptor["sha256"]):
            raise ValueError("source changed before lossless compaction")
        target, compression = compact_audio(path) if audio else compact_metadata(path)
        with open_regular(path) as source:
            if _digest(source) != expected_source:
                raise ValueError("source changed during lossless compaction")
        if (
            not audio
            and (compression["uncompressed_bytes"], compression["uncompressed_sha256"])
            != expected_source
        ):
            raise ValueError("metadata compaction did not preserve the sealed source")
        return target, compression, relative, audio

    # Keep at most two results pending admission. Unlike eager Executor.map,
    # this cannot queue the whole recording before seeing the first failure.
    # Refill after ordered caller-side admission so a slow file does not stall
    # both workers, while a failed finalizer/progress callback submits no more.
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="lossless-seal") as executor:
        pending = deque(executor.submit(compact, descriptor) for descriptor in descriptors[:2])
        for completed, descriptor in enumerate(descriptors, 1):
            target, compression, relative, audio = pending.popleft().result()
            finalized = finalize(target)
            if prepared is not None and relative in prepared:
                proof = prepared[relative].output
                if not proof.matches(
                    target.stat(follow_symlinks=False),
                    finalized.get("bytes"),
                    finalized.get("sha256"),
                ):
                    raise ValueError("prepared compacted output changed before final admission")
            descriptor.update(finalized)
            descriptor["media_type"] = "audio/flac" if audio else "application/zstd"
            descriptor["storage_encoding"] = compression
            originals.append(relative)
            if progress is not None:
                progress(completed, len(descriptors))
            if completed + 1 < len(descriptors):
                pending.append(executor.submit(compact, descriptors[completed + 1]))
    manifest["schema"] = "ylx.device-session.v4"
    return manifest, originals
