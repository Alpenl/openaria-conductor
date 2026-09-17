"""Verified, bounded lossless compaction after acquisition has stopped.

Source files survive until the new manifest has been atomically sealed. Each
1 MiB Zstandard block is a complete checksummed frame; an interrupted compaction
never changes the acquisition journal used by recovery.
"""

from __future__ import annotations

import hashlib
import io
import os
import subprocess
import wave
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import zstandard

BLOCK_BYTES = 1024 * 1024


@contextmanager
def open_metadata(path: Path):
    with path.open("rb") as raw:
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
    with path.open("rb") as source, temporary.open("xb") as output:
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
        temporary.open("rb") as raw,
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


def compact_audio(path: Path) -> tuple[Path, dict]:
    target = path.with_suffix(".flac")
    temporary = target.with_suffix(".flac.tmp")
    with wave.open(str(path), "rb") as source:
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
        timeout=120,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    with subprocess.Popen(
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
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    ) as process:
        assert process.stdout is not None
        actual = _digest(process.stdout)
        if process.wait(timeout=120) != 0 or actual != (pcm_bytes, pcm.hexdigest()):
            raise ValueError("FLAC PCM round-trip mismatch")
    with temporary.open("rb") as stream:
        os.fsync(stream.fileno())
    os.replace(temporary, target)
    return target, {"codec": "flac", "pcm_bytes": pcm_bytes, "pcm_sha256": pcm.hexdigest()}


def compact_manifest(
    root: Path, manifest: dict, finalize, *, progress=None
) -> tuple[dict, list[str]]:
    """Caller owns manifest, updates file identities, then seals before cleanup."""
    originals = []
    descriptors = [manifest["frames"]["artifact"], manifest["imu"]["artifact"]]
    descriptors += [
        segment["artifact"] for segment in manifest.get("audio", {}).get("segments", [])
    ]

    def compact(descriptor):
        relative = descriptor["path"]
        path = root / relative
        audio = descriptor["media_type"] == "audio/wav"
        with path.open("rb") as source:
            expected_source = _digest(source)
        if expected_source != (descriptor["bytes"], descriptor["sha256"]):
            raise ValueError("source changed before lossless compaction")
        target, compression = compact_audio(path) if audio else compact_metadata(path)
        with path.open("rb") as source:
            if _digest(source) != expected_source:
                raise ValueError("source changed during lossless compaction")
        if (
            not audio
            and (compression["uncompressed_bytes"], compression["uncompressed_sha256"])
            != expected_source
        ):
            raise ValueError("metadata compaction did not preserve the sealed source")
        return target, compression, relative, audio

    # Two bounded workers overlap codec startup / disk waits. All descriptor
    # mutation and finalization stays on the calling thread. Originals survive
    # any failure, and executor shutdown joins outstanding work before recovery.
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="lossless-seal") as executor:
        for completed, (descriptor, result) in enumerate(
            zip(descriptors, executor.map(compact, descriptors), strict=True), 1
        ):
            target, compression, relative, audio = result
            descriptor.update(finalize(target))
            descriptor["media_type"] = "audio/flac" if audio else "application/zstd"
            descriptor["storage_encoding"] = compression
            originals.append(relative)
            if progress is not None:
                progress(completed, len(descriptors))
    manifest["schema"] = "ylx.device-session.v4"
    return manifest, originals
