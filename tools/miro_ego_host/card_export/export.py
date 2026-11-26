#!/usr/bin/env python3
"""Read-only Miro-Ego SD-card exporter for MIRO-EGO exFAT recording volumes.

Reads ``recordings/YYYY-MM-DD/HH-MM-SS/`` sessions with ``miro-ego.device-session.v1``
manifests and verifies every declared artifact's exact byte size and SHA-256 digest.
Also supports exporting ``/logs/`` diagnostics, restoring ownership under ``sudo``,
and inspecting raw MBR cards, raw exFAT images, or mounted ``MIRO-EGO`` directories
without ever writing to the source card.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import struct
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO

if __package__:
    from .. import disks
else:
    _parent = str(Path(__file__).resolve().parents[1])
    if (Path(_parent) / "disks.py").is_file() and _parent not in sys.path:
        sys.path.insert(0, _parent)
    import disks

BLOCK = 4 * 1024**2
ALIGN_BYTES = 4096
MAX_EXFAT_DIR_BYTES = 64 * 1024 * 1024
MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_EXPORT_FILES = 1_000_000
EXPORT_SPACE_HEADROOM = 64 * 1024**2

EXPECTED_VOLUME_LABEL = "MIRO-EGO"
SESSION_MANIFEST_SCHEMA = "miro-ego.device-session.v1"
EXPORT_REPORT_SCHEMA = "miro-ego.card-export-report.v1"

DAY_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TIME_DIR_RE = re.compile(r"^\d{2}-\d{2}-\d{2}(?:_\d{2,})?$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

_RESERVED_WINDOWS_NAMES = frozenset(
    {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        *(f"COM{i}" for i in range(1, 10)),
        *(f"LPT{i}" for i in range(1, 10)),
    }
)


def discover() -> list[dict[str, Any]]:
    """List eligible external removable cards on Linux, Windows, or macOS."""
    found = disks.discover(
        require_writable=False,
        require_safe_mounts=True,
        strict_device_names=False,
        exclude_apfs_boot=True,
    )
    return [
        {"path": d.path, "size": d.size, "model": d.model, "identity": d.identity} for d in found
    ]


def safe_name(name: str) -> str:
    """Validate that a filename is portable and safe across Windows, macOS, and Linux."""
    if (
        not name
        or name in {".", ".."}
        or name.endswith((".", " "))
        or re.search(r'[<>:"/\\|?*\x00-\x1f\ud800-\udfff]', name)
        or name.split(".")[0].upper() in _RESERVED_WINDOWS_NAMES
    ):
        raise RuntimeError("Filename cannot be safely exported: " + repr(name))
    return name


def safe_artifact_relpath(relpath: str) -> str:
    """Validate an artifact relative POSIX path from ``miro-ego.device-session.v1``."""
    if (
        not isinstance(relpath, str)
        or not relpath
        or len(relpath) > 1024
        or relpath.startswith("/")
        or "\\" in relpath
        or re.search(r"[\x00-\x1f\x7f-\x9f]", relpath)
    ):
        raise RuntimeError(f"Unsafe manifest artifact path: {relpath!r}")
    parts = relpath.split("/")
    if any(not part or part in {".", ".."} for part in parts):
        raise RuntimeError(f"Unsafe manifest artifact path: {relpath!r}")
    if parts[0] in {"manifest.json", "recording.json"}:
        raise RuntimeError(f"Manifest cannot declare {parts[0]!r} as an artifact")
    for part in parts:
        safe_name(part)
        if ".tmp" in part and re.search(r"\.tmp(?:[._-].*)?$", part):
            raise RuntimeError(f"Manifest artifact path references temporary file: {relpath!r}")
    return PurePosixPath(*parts).as_posix()


def get_optional(fs: Any, path: str) -> Any:
    try:
        return fs.get(path)
    except FileNotFoundError:
        return None


def is_exfat(stream: Any) -> bool:
    """Return True if *stream* starts with a valid exFAT Volume Boot Record."""
    stream.seek(0)
    boot = stream.read(512)
    return len(boot) == 512 and boot[3:11] == b"EXFAT   " and boot[510:512] == b"\x55\xaa"


def parse_mbr_partitions(stream: Any) -> list[tuple[int, int, int, int]]:
    """Return ``[(part_num, part_type, start_byte, length_bytes), ...]`` for non-empty MBR rows."""
    stream.seek(0)
    mbr = stream.read(512)
    if len(mbr) != 512 or mbr[510:512] != b"\x55\xaa":
        return []
    result: list[tuple[int, int, int, int]] = []
    for index in range(4):
        kind = mbr[450 + index * 16]
        start_lba, sectors = struct.unpack_from("<II", mbr, 454 + index * 16)
        if kind == 0 or kind == 0xEE or sectors == 0:
            continue
        result.append((index + 1, kind, start_lba * 512, sectors * 512))
    return result


class _AlignedSliceStream:
    """Read-only, sector-aligned slice view over a raw disk handle or image file."""

    def __init__(
        self,
        handle: BinaryIO,
        base_offset: int = 0,
        length: int | None = None,
        align: int = ALIGN_BYTES,
    ) -> None:
        self._handle = handle
        self._base = base_offset
        self._length = length
        self._align = align
        self._pos = 0
        self._cache_start = -1
        self._cache = b""

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        if whence == os.SEEK_SET:
            target = offset
        elif whence == os.SEEK_CUR:
            target = self._pos + offset
        elif whence == os.SEEK_END:
            if self._length is None:
                raise RuntimeError("Cannot seek from end on unbounded stream")
            target = self._length + offset
        else:
            raise ValueError(f"Unsupported whence: {whence}")
        if target < 0:
            raise ValueError("Negative seek position")
        self._pos = target
        return self._pos

    def tell(self) -> int:
        return self._pos

    def _read_aligned_window(self, abs_offset: int, want: int) -> bytes:
        aligned_start = (abs_offset // self._align) * self._align
        end_needed = abs_offset + want
        aligned_end = ((end_needed + self._align - 1) // self._align) * self._align
        # Read at least 64 KiB aligned chunks for fast directory/FAT traversal.
        min_window = max(self._align * 16, aligned_end - aligned_start)
        window_size = min(min_window, BLOCK)
        if aligned_end - aligned_start > window_size:
            window_size = aligned_end - aligned_start
        if (
            self._cache_start >= 0
            and self._cache_start <= abs_offset
            and abs_offset + want <= self._cache_start + len(self._cache)
        ):
            delta = abs_offset - self._cache_start
            return self._cache[delta : delta + want]
        self._handle.seek(aligned_start)
        raw = self._handle.read(window_size)
        self._cache_start = aligned_start
        self._cache = raw
        delta = abs_offset - aligned_start
        if delta >= len(raw):
            return b""
        return raw[delta : delta + want]

    def read(self, count: int = -1) -> bytes:
        if self._length is not None:
            if self._pos >= self._length:
                return b""
            remaining = (
                self._length - self._pos if count < 0 else min(count, self._length - self._pos)
            )
        else:
            if count < 0:
                raise RuntimeError("Unbounded read is not supported on raw device streams")
            remaining = count
        if remaining <= 0:
            return b""
        chunks: list[bytes] = []
        while remaining > 0:
            step = min(remaining, BLOCK)
            chunk = self._read_aligned_window(self._base + self._pos, step)
            if not chunk:
                break
            chunks.append(chunk)
            self._pos += len(chunk)
            remaining -= len(chunk)
            if len(chunk) < step:
                break
        return b"".join(chunks)


class _ExfatFileStream:
    """Bounded read-only stream over an exFAT cluster chain."""

    def __init__(
        self,
        stream: Any,
        cluster_offsets: list[int],
        cluster_size: int,
        size: int,
        valid_size: int | None = None,
    ) -> None:
        self._stream = stream
        self._cluster_offsets = cluster_offsets
        self._cluster_size = cluster_size
        self._size = size
        self._valid_size = size if valid_size is None else min(valid_size, size)
        self._pos = 0

    def __enter__(self) -> _ExfatFileStream:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, count: int = -1) -> bytes:
        if self._pos >= self._size:
            return b""
        remaining = (
            self._size - self._pos
            if count is None or count < 0
            else min(count, self._size - self._pos)
        )
        chunks: list[bytes] = []
        while remaining > 0:
            if self._pos < self._valid_size:
                cluster_index, intra = divmod(self._pos, self._cluster_size)
                if cluster_index >= len(self._cluster_offsets):
                    break
                take = min(remaining, self._valid_size - self._pos, self._cluster_size - intra)
                self._stream.seek(self._cluster_offsets[cluster_index] + intra)
                chunk = self._stream.read(take)
                if not chunk:
                    break
                chunks.append(chunk)
                self._pos += len(chunk)
                remaining -= len(chunk)
                if len(chunk) < take:
                    break
            else:
                zero_len = min(remaining, self._size - self._pos)
                chunks.append(b"\x00" * zero_len)
                self._pos += zero_len
                remaining -= zero_len
        return b"".join(chunks)


class _ExfatNode:
    def __init__(
        self,
        fs: _ExfatFS,
        filename: str,
        *,
        filetype: int,
        first_cluster: int,
        size: int,
        valid_size: int | None = None,
        no_fat_chain: bool,
        inum: tuple[int, ...],
    ) -> None:
        self._fs = fs
        self.filename = filename
        self.filetype = filetype
        self._first_cluster = first_cluster
        self.size = size
        self._valid_size = size if valid_size is None else min(valid_size, size)
        self._no_fat_chain = no_fat_chain
        self.inum = inum

    def iterdir(self) -> list[_ExfatNode]:
        if self.filetype != stat.S_IFDIR:
            return []
        entries, _ = self._fs._read_directory(
            self._first_cluster,
            self.size if self._no_fat_chain else None,
            self._no_fat_chain,
        )
        return entries

    def open(self) -> _ExfatFileStream:
        if self.filetype != stat.S_IFREG:
            raise RuntimeError("Cannot open non-regular exFAT entry")
        offsets = self._fs._cluster_offsets(
            self._first_cluster, self._valid_size, self._no_fat_chain
        )
        return _ExfatFileStream(
            self._fs._stream, offsets, self._fs._cluster_size, self.size, self._valid_size
        )


class _ExfatFS:
    """Minimal read-only pure-Python exFAT reader for Miro-Ego card export."""

    def __init__(self, stream: Any) -> None:
        self._stream = stream
        stream.seek(0)
        vbr = stream.read(512)
        if len(vbr) != 512 or vbr[3:11] != b"EXFAT   " or vbr[510:512] != b"\x55\xaa":
            raise RuntimeError("Invalid exFAT boot sector")
        self._fat_offset = struct.unpack_from("<I", vbr, 80)[0]
        self._fat_length = struct.unpack_from("<I", vbr, 84)[0]
        self._cluster_heap_offset = struct.unpack_from("<I", vbr, 88)[0]
        self._cluster_count = struct.unpack_from("<I", vbr, 92)[0]
        self._root_cluster = struct.unpack_from("<I", vbr, 96)[0]
        volume_flags = struct.unpack_from("<H", vbr, 106)[0]
        bps_shift = vbr[108]
        spc_shift = vbr[109]
        number_of_fats = vbr[110]
        if not (9 <= bps_shift <= 12) or bps_shift + spc_shift > 25:
            raise RuntimeError("Unsupported exFAT geometry")
        if (
            self._cluster_count == 0
            or self._fat_length == 0
            or self._root_cluster < 2
            or self._root_cluster >= self._cluster_count + 2
        ):
            raise RuntimeError("Invalid exFAT geometry")
        if number_of_fats not in (1, 2):
            raise RuntimeError("Unsupported exFAT FAT count")
        active_fat = (volume_flags & 0x0001) if number_of_fats == 2 else 0
        self._active_fat_offset = self._fat_offset + active_fat * self._fat_length
        self._sector_size = 1 << bps_shift
        self._cluster_size = self._sector_size << spc_shift
        self.dirty = bool(volume_flags & 0x0002)
        self._root = _ExfatNode(
            self,
            "",
            filetype=stat.S_IFDIR,
            first_cluster=self._root_cluster,
            size=0,
            no_fat_chain=False,
            inum=(self._root_cluster, 0),
        )
        _, self.volume_label = self._read_directory(self._root_cluster, None, False)

    def _cluster_offset(self, cluster: int) -> int:
        if cluster < 2 or cluster >= self._cluster_count + 2:
            raise RuntimeError(f"exFAT cluster out of bounds: {cluster}")
        return (self._cluster_heap_offset * self._sector_size) + (
            (cluster - 2) * self._cluster_size
        )

    def _cluster_offsets(
        self,
        first_cluster: int,
        data_length: int | None,
        no_fat_chain: bool,
    ) -> list[int]:
        if first_cluster < 2:
            return []
        max_clusters = (
            (data_length + self._cluster_size - 1) // self._cluster_size
            if data_length is not None
            else self._cluster_count
        )
        if max_clusters == 0:
            return []
        if no_fat_chain:
            return [self._cluster_offset(first_cluster + i) for i in range(max_clusters)]
        fat_base = self._active_fat_offset * self._sector_size
        fat_bytes = self._fat_length * self._sector_size
        offsets: list[int] = []
        seen: set[int] = set()
        cluster = first_cluster
        while 2 <= cluster < 0xFFFFFFF8 and len(offsets) < max_clusters:
            if cluster in seen:
                raise RuntimeError("Cyclic exFAT cluster chain")
            seen.add(cluster)
            offsets.append(self._cluster_offset(cluster))
            fat_pos = cluster * 4
            if fat_pos + 4 > fat_bytes:
                raise RuntimeError("exFAT FAT lookup out of bounds")
            self._stream.seek(fat_base + fat_pos)
            entry_raw = self._stream.read(4)
            if len(entry_raw) != 4:
                raise RuntimeError("Truncated exFAT FAT entry")
            cluster = struct.unpack_from("<I", entry_raw, 0)[0]
        return offsets

    def _read_directory(
        self,
        first_cluster: int,
        data_length: int | None,
        no_fat_chain: bool,
    ) -> tuple[list[_ExfatNode], str]:
        if data_length is not None and data_length > MAX_EXFAT_DIR_BYTES:
            raise RuntimeError("exFAT directory exceeds memory limit")
        offsets = self._cluster_offsets(first_cluster, data_length, no_fat_chain)
        if len(offsets) * self._cluster_size > MAX_EXFAT_DIR_BYTES:
            raise RuntimeError("exFAT directory exceeds memory limit")
        raw_chunks = []
        for offset in offsets:
            self._stream.seek(offset)
            raw_chunks.append(self._stream.read(self._cluster_size))
        raw = b"".join(raw_chunks)
        entries: list[_ExfatNode] = []
        volume_label = ""
        index = 0
        total_slots = len(raw) // 32
        while index < total_slots:
            entry_offset = index * 32
            entry_type = raw[entry_offset]
            if entry_type == 0x00:
                break
            if entry_type == 0x83:
                char_count = raw[entry_offset + 1]
                if char_count <= 11:
                    volume_label = raw[entry_offset + 2 : entry_offset + 2 + char_count * 2].decode(
                        "utf-16-le", errors="strict"
                    )
                index += 1
                continue
            if entry_type != 0x85:
                index += 1
                continue
            secondary_count = raw[entry_offset + 1]
            if secondary_count < 2 or index + secondary_count >= total_slots:
                raise RuntimeError("Corrupt exFAT file directory entry")
            attrs = struct.unpack_from("<H", raw, entry_offset + 4)[0]
            stream_offset = (index + 1) * 32
            if raw[stream_offset] != 0xC0:
                raise RuntimeError("Missing exFAT stream extension entry")
            stream_flags = raw[stream_offset + 1]
            name_len = raw[stream_offset + 3]
            valid_data_length = struct.unpack_from("<Q", raw, stream_offset + 8)[0]
            child_cluster = struct.unpack_from("<I", raw, stream_offset + 20)[0]
            child_size = struct.unpack_from("<Q", raw, stream_offset + 24)[0]
            name_bytes = bytearray()
            for slot in range(index + 2, index + 1 + secondary_count):
                slot_offset = slot * 32
                slot_type = raw[slot_offset]
                if slot_type == 0xC1:
                    name_bytes.extend(raw[slot_offset + 2 : slot_offset + 32])
                elif 0xE0 <= slot_type <= 0xFF:
                    continue
                else:
                    raise RuntimeError("Unexpected exFAT secondary entry")
            if name_len == 0 or len(name_bytes) < name_len * 2:
                raise RuntimeError("Corrupt exFAT filename length")
            filename = name_bytes[: name_len * 2].decode("utf-16-le", errors="strict")
            filetype = stat.S_IFDIR if attrs & 0x10 else stat.S_IFREG
            valid_size = (
                child_size if filetype == stat.S_IFDIR else min(valid_data_length, child_size)
            )
            entries.append(
                _ExfatNode(
                    self,
                    filename,
                    filetype=filetype,
                    first_cluster=child_cluster,
                    size=child_size,
                    valid_size=valid_size,
                    no_fat_chain=bool(stream_flags & 0x02),
                    inum=(first_cluster, index, child_cluster),
                )
            )
            index += 1 + secondary_count
        return entries, volume_label

    def get(self, path: str) -> _ExfatNode:
        parts = [part for part in path.split("/") if part]
        node = self._root
        for part in parts:
            if node.filetype != stat.S_IFDIR:
                raise FileNotFoundError(path)
            children = node.iterdir()
            match = next((c for c in children if c.filename == part), None)
            if match is None:
                folded = part.casefold()
                match = next((c for c in children if c.filename.casefold() == folded), None)
            if match is None:
                raise FileNotFoundError(path)
            node = match
        return node


class _DirectoryNode:
    """Read-only filesystem adapter node for an already-mounted MIRO-EGO directory."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self.filename = path.name
        info = path.lstat()
        self.filetype = stat.S_IFMT(info.st_mode)
        self.size = info.st_size if self.filetype == stat.S_IFREG else 0
        self.inum = (info.st_dev, info.st_ino)

    def iterdir(self) -> list[_DirectoryNode]:
        if self.filetype != stat.S_IFDIR:
            return []
        return [
            _DirectoryNode(child) for child in sorted(self._path.iterdir(), key=lambda p: p.name)
        ]

    def open(self) -> BinaryIO:
        if self.filetype != stat.S_IFREG:
            raise RuntimeError("Cannot open non-regular file")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_BINARY", 0)
        fd = os.open(self._path, flags)
        return os.fdopen(fd, "rb")


class _DirectoryFS:
    """Read-only directory-backed volume view for a mounted ``MIRO-EGO`` card."""

    def __init__(self, root: Path) -> None:
        self._root = root.resolve(strict=True)
        self.dirty = False
        self.volume_label = self._root.name or EXPECTED_VOLUME_LABEL

    def get(self, path: str) -> _DirectoryNode:
        parts = [part for part in path.split("/") if part]
        cursor = self._root
        for part in parts:
            candidate = cursor / part
            if not candidate.exists():
                raise FileNotFoundError(path)
            if candidate.is_symlink():
                raise RuntimeError(f"Symlink refused in source volume: {path}")
            cursor = candidate
        return _DirectoryNode(cursor)


@contextmanager
def volumes(source: str | Path):
    """Yield discovered Miro-Ego exFAT volume records ``(label, fs, dirty, volume_label)``."""
    source_path = Path(source)
    if source_path.is_dir():
        fs = _DirectoryFS(source_path)
        found: list[tuple[str, Any, bool, str]] = []
        if _has_miro_ego_roots(fs):
            found.append((fs.volume_label, fs, False, fs.volume_label))
        yield found
        return

    # Always open rb without buffering; never mount, repair, or write the source card.
    with open(source, "rb", buffering=0) as handle:
        raw = _AlignedSliceStream(handle, base_offset=0, length=None)
        if is_exfat(raw):
            streams: list[tuple[str, Any]] = [("exfat", raw)]
        else:
            parts = parse_mbr_partitions(raw)
            streams = [
                (
                    f"partition-{part_num}",
                    _AlignedSliceStream(handle, base_offset=start_byte, length=length_bytes),
                )
                for part_num, _kind, start_byte, length_bytes in parts
            ]
        found = []
        for label, stream in streams:
            if not is_exfat(stream):
                continue
            fs = _ExfatFS(stream)
            if _has_miro_ego_roots(fs) or fs.volume_label == EXPECTED_VOLUME_LABEL:
                found.append((label, fs, fs.dirty, fs.volume_label))
        yield found


def _has_miro_ego_roots(fs: Any) -> bool:
    for root_name in ("/recordings", "/logs"):
        entry = get_optional(fs, root_name)
        if entry is not None and entry.filetype == stat.S_IFDIR:
            return True
    return False


def _read_node_bytes(node: Any, *, max_bytes: int) -> bytes:
    if node.filetype != stat.S_IFREG:
        raise RuntimeError("Expected regular file")
    if node.size > max_bytes:
        raise RuntimeError(f"File {node.filename!r} exceeds size limit ({max_bytes} bytes)")
    with node.open() as src:
        raw = src.read(max_bytes + 1)
    if len(raw) != node.size:
        raise RuntimeError(f"Short read while reading {node.filename!r}")
    return raw


def manifest_artifacts(manifest: Any) -> dict[str, tuple[int, str]]:
    """Extract and validate all declared artifacts from a ``miro-ego.device-session.v1`` manifest.

    Returns ``{relative_posix_path: (expected_bytes, expected_sha256)}``.
    """
    if not isinstance(manifest, dict):
        raise RuntimeError("Session manifest must be a JSON object")
    if manifest.get("schema") != SESSION_MANIFEST_SCHEMA:
        raise RuntimeError(f"Unsupported session manifest schema: {manifest.get('schema')!r}")
    if manifest.get("sealed") is not True:
        raise RuntimeError("Session manifest is not marked sealed")

    declared: list[Any] = []

    video = manifest.get("video")
    if (
        not isinstance(video, dict)
        or not isinstance(video.get("segments"), list)
        or not video["segments"]
    ):
        raise RuntimeError("Session manifest is missing video.segments")
    for segment in video["segments"]:
        if not isinstance(segment, dict) or not isinstance(segment.get("artifacts"), dict):
            raise RuntimeError("Invalid video segment entry in manifest")
        artifacts = segment["artifacts"]
        declared.append(artifacts.get("left"))
        declared.append(artifacts.get("right"))

    imu = manifest.get("imu")
    if not isinstance(imu, dict) or not isinstance(imu.get("artifact"), dict):
        raise RuntimeError("Session manifest is missing imu.artifact")
    declared.append(imu["artifact"])

    frames = manifest.get("frames")
    if not isinstance(frames, dict) or not isinstance(frames.get("artifact"), dict):
        raise RuntimeError("Session manifest is missing frames.artifact")
    declared.append(frames["artifact"])

    audio = manifest.get("audio")
    if not isinstance(audio, dict):
        raise RuntimeError("Session manifest is missing audio section")
    audio_state = audio.get("state")
    if audio_state == "recorded":
        segments = audio.get("segments")
        if not isinstance(segments, list) or not segments:
            raise RuntimeError("Recorded audio manifest is missing audio.segments")
        for segment in segments:
            if not isinstance(segment, dict) or not isinstance(segment.get("artifact"), dict):
                raise RuntimeError("Invalid audio segment entry in manifest")
            declared.append(segment["artifact"])
    elif audio_state != "not_recorded":
        raise RuntimeError(f"Unknown audio state in manifest: {audio_state!r}")

    logs = manifest.get("logs")
    if not isinstance(logs, list):
        raise RuntimeError("Session manifest logs field must be a list")
    declared.extend(logs)

    expected: dict[str, tuple[int, str]] = {}
    for item in declared:
        if not isinstance(item, dict):
            raise RuntimeError("Malformed artifact entry in manifest")
        rel = safe_artifact_relpath(item.get("path", ""))
        byte_len = item.get("bytes")
        digest_hex = item.get("sha256")
        if type(byte_len) is not int or byte_len < 0:
            raise RuntimeError(f"Invalid artifact byte count for {rel!r}")
        if not isinstance(digest_hex, str) or SHA256_RE.fullmatch(digest_hex) is None:
            raise RuntimeError(f"Invalid artifact SHA-256 for {rel!r}")
        if rel in expected:
            raise RuntimeError(f"Duplicate artifact path in manifest: {rel!r}")
        expected[rel] = (byte_len, digest_hex)
    return expected


def recordings(found: list[tuple[str, Any, bool, str]]) -> list[dict[str, Any]]:
    """Discover ``recordings/YYYY-MM-DD/HH-MM-SS[_NN]/`` sessions on ``MIRO-EGO`` exFAT volumes."""
    result: list[dict[str, Any]] = []
    for label, fs, unclean, volume_label in found:
        rec_root = get_optional(fs, "/recordings")
        if rec_root is None or rec_root.filetype != stat.S_IFDIR:
            continue
        for day_node in sorted(rec_root.iterdir(), key=lambda n: n.filename):
            if day_node.filetype != stat.S_IFDIR:
                continue
            if DAY_DIR_RE.fullmatch(day_node.filename) is None:
                # Ignore non-canonical or legacy top-level folders (e.g. UUID directories).
                continue
            day_name = safe_name(day_node.filename)
            for session_node in sorted(day_node.iterdir(), key=lambda n: n.filename):
                if session_node.filetype != stat.S_IFDIR:
                    continue
                if TIME_DIR_RE.fullmatch(session_node.filename) is None:
                    continue
                time_name = safe_name(session_node.filename)
                manifest_node = get_optional(
                    fs, f"/recordings/{day_name}/{time_name}/manifest.json"
                )
                sealed = False
                session_id: str | None = None
                expected_artifacts: dict[str, tuple[int, str]] = {}
                manifest_error: str | None = None
                if manifest_node is not None:
                    try:
                        parsed = json.loads(
                            _read_node_bytes(manifest_node, max_bytes=MAX_MANIFEST_BYTES)
                        )
                        expected_artifacts = manifest_artifacts(parsed)
                        session_id = parsed.get("session_id")
                        sealed = True
                    except Exception as exc:
                        manifest_error = str(exc)
                result.append(
                    {
                        "id": f"{label}/recordings/{day_name}/{time_name}",
                        "session_id": session_id,
                        "volume_label": volume_label,
                        "node": session_node,
                        "dirty": unclean,
                        "sealed": sealed,
                        "manifest_present": manifest_node is not None,
                        "manifest_error": manifest_error,
                        "expected_artifacts": expected_artifacts,
                        "prefix": Path(label, "recordings", day_name, time_name),
                    }
                )
    return result


def diagnostic_logs(found: list[tuple[str, Any, bool, str]]) -> list[dict[str, Any]]:
    """Discover ``/logs/`` diagnostic directories on ``MIRO-EGO`` exFAT volumes."""
    result: list[dict[str, Any]] = []
    for label, fs, unclean, volume_label in found:
        logs_node = get_optional(fs, "/logs")
        if logs_node is None or logs_node.filetype != stat.S_IFDIR:
            continue
        result.append(
            {
                "id": f"{label}/logs",
                "volume_label": volume_label,
                "node": logs_node,
                "dirty": unclean,
                "prefix": Path(label, "logs"),
            }
        )
    return result


def walk(
    node: Any,
    relative: Path = Path(),
    ancestors: frozenset[Any] = frozenset(),
):
    """Yield ``(relative_path, file_node)`` recursively while blocking cycles and symlinks."""
    key = node.inum
    if key in ancestors or len(ancestors) > 32:
        raise RuntimeError("Cyclic or excessively deep directory tree")
    for child in sorted(node.iterdir(), key=lambda n: n.filename):
        if child.filename in {".", ".."}:
            continue
        path = relative / safe_name(child.filename)
        if child.filetype == stat.S_IFDIR:
            yield from walk(child, path, ancestors | {key})
        elif child.filetype == stat.S_IFREG:
            yield path, child
        else:
            raise RuntimeError("Symlink or special file refused during export: " + str(path))


def destination_check(source: str | Path, destination: str | Path) -> Path:
    """Verify *destination* is an existing local directory not residing on *source*."""
    dest = Path(destination).resolve(strict=True)
    if not dest.is_dir():
        raise RuntimeError("Destination must be an existing directory on the computer")
    source_path = Path(source)
    if source_path.is_dir():
        resolved_src = source_path.resolve(strict=True)
        if dest == resolved_src or resolved_src in dest.parents:
            raise RuntimeError("Do not export onto the source card")
        return dest
    if source_path.is_file():
        return dest
    disks.check_path_not_on_disk(
        str(source),
        dest,
        message="Do not export onto the source card",
    )
    return dest


def _invoking_sudo_ids() -> tuple[int, int] | None:
    if os.name != "posix" or os.geteuid() != 0:
        return None
    uid_raw = os.environ.get("SUDO_UID")
    gid_raw = os.environ.get("SUDO_GID")
    if not uid_raw or not gid_raw:
        return None
    try:
        uid = int(uid_raw)
        gid = int(gid_raw)
    except ValueError:
        return None
    return (uid, gid) if uid >= 0 and gid >= 0 else None


def _apply_export_permissions(
    path: Path,
    sudo_ids: tuple[int, int] | None,
    *,
    is_dir: bool,
) -> None:
    if os.name != "posix":
        return
    path.chmod(0o755 if is_dir else 0o644)
    if sudo_ids is not None:
        os.chown(path, sudo_ids[0], sudo_ids[1])


def _ensure_export_dir(directory: Path, root: Path, sudo_ids: tuple[int, int] | None) -> None:
    missing: list[Path] = []
    cursor = directory
    while cursor != root and not cursor.exists():
        missing.append(cursor)
        cursor = cursor.parent
    directory.mkdir(parents=True, exist_ok=True)
    for created in reversed(missing):
        _apply_export_permissions(created, sudo_ids, is_dir=True)


def export_selected(
    selected: list[dict[str, Any]],
    destination: str | Path,
    *,
    allow_dirty: bool = False,
    allow_unsealed: bool = False,
    log_sources: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
) -> Path:
    """Export selected recordings and/or diagnostic logs with full SHA-256 verification."""
    all_dirty = any(r["dirty"] for r in selected) or any(item["dirty"] for item in log_sources)
    if all_dirty and not allow_dirty:
        raise RuntimeError(
            "Filesystem was not cleanly shut down. Power down Miro-Ego normally first, "
            "or explicitly use --allow-dirty for best-effort read-only export."
        )

    for record in selected:
        if record.get("manifest_error") is not None:
            raise RuntimeError(
                f"Invalid session manifest in {record['id']}: {record['manifest_error']}"
            )
        if not record["sealed"] and not allow_unsealed:
            raise RuntimeError(
                f"Recording {record['id']} has no sealed {SESSION_MANIFEST_SCHEMA} manifest; "
                "pass --allow-unsealed to export unsealed directories."
            )

    # Plan files and validate no collisions or missing manifest artifacts up front.
    files: list[tuple[Path, Any, dict[str, Any] | None, str | None]] = []
    seen_names: set[str] = set()
    for record in selected:
        present_relpaths: set[str] = set()
        for rel_path, node in walk(record["node"]):
            rel_posix = rel_path.as_posix()
            present_relpaths.add(rel_posix)
            out_path = record["prefix"] / rel_path
            normalized = str(out_path).casefold()
            if normalized in seen_names:
                raise RuntimeError("Case-insensitive filename collision")
            seen_names.add(normalized)
            files.append((out_path, node, record, rel_posix))
            if len(files) > MAX_EXPORT_FILES:
                raise RuntimeError("Too many files")
        if record["sealed"]:
            missing = sorted(set(record["expected_artifacts"]) - present_relpaths)
            if missing:
                raise RuntimeError(
                    f"Recording {record['id']} is missing manifest artifact(s): "
                    + ", ".join(missing)
                )

    for log_item in log_sources:
        for rel_path, node in walk(log_item["node"]):
            out_path = log_item["prefix"] / rel_path
            normalized = str(out_path).casefold()
            if normalized in seen_names:
                raise RuntimeError("Case-insensitive filename collision")
            seen_names.add(normalized)
            files.append((out_path, node, None, None))
            if len(files) > MAX_EXPORT_FILES:
                raise RuntimeError("Too many files")

    if not files:
        raise RuntimeError("No recording or diagnostic log files selected")

    total = sum(node.size for _, node, _, _ in files)
    dest_path = Path(destination)
    if shutil.disk_usage(dest_path).free < total + EXPORT_SPACE_HEADROOM:
        raise RuntimeError("Insufficient destination space")

    output = dest_path / (
        "Miro-Ego-export-" + datetime.now().strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:8]
    )
    sudo_ids = _invoking_sudo_ids()
    output.mkdir(mode=0o755)
    _apply_export_permissions(output, sudo_ids, is_dir=True)

    report: dict[str, Any] = {
        "schema": EXPORT_REPORT_SCHEMA,
        "status": "incomplete",
        "source_dirty": all_dirty,
        "recordings": [r["id"] for r in selected],
        "logs": [item["id"] for item in log_sources],
        "verified_artifacts": 0,
        "files": [],
    }
    try:
        done = 0
        verified_count = 0
        for index, (out_path, node, record, rel_posix) in enumerate(files, 1):
            target = output / out_path
            _ensure_export_dir(target.parent, output, sudo_ids)
            temporary = target.with_name(target.name + ".export-partial")
            src_digest = hashlib.sha256()
            remaining = node.size
            with node.open() as src, temporary.open("xb") as dst:
                while remaining:
                    chunk = src.read(min(BLOCK, remaining))
                    if not chunk:
                        raise RuntimeError("Short source read: " + str(out_path))
                    dst.write(chunk)
                    src_digest.update(chunk)
                    remaining -= len(chunk)
                dst.flush()
                os.fsync(dst.fileno())
            readback_sha = disks.sha256_file(temporary)
            source_sha = src_digest.hexdigest()
            if readback_sha != source_sha:
                raise RuntimeError("Destination verification failed: " + str(out_path))

            manifest_verified = False
            if (
                record is not None
                and record["sealed"]
                and rel_posix in record["expected_artifacts"]
            ):
                expected_bytes, expected_sha = record["expected_artifacts"][rel_posix]
                if node.size != expected_bytes:
                    raise RuntimeError(
                        f"Manifest byte count mismatch for {record['id']}/{rel_posix}: "
                        f"expected {expected_bytes}, got {node.size}"
                    )
                if readback_sha != expected_sha:
                    raise RuntimeError(
                        f"Manifest SHA-256 mismatch for {record['id']}/{rel_posix}: "
                        f"expected {expected_sha}, got {readback_sha}"
                    )
                manifest_verified = True
                verified_count += 1

            os.replace(temporary, target)
            _apply_export_permissions(target, sudo_ids, is_dir=False)
            report["files"].append(
                {
                    "path": out_path.as_posix(),
                    "bytes": node.size,
                    "sha256": readback_sha,
                    "manifest_verified": manifest_verified,
                }
            )
            done += node.size
            print(
                f"Exported and verified {index}/{len(files)} files ({done}/{total} bytes)",
                flush=True,
            )

        report["verified_artifacts"] = verified_count
        report["status"] = "complete"
    except BaseException as exc:
        report["error"] = str(exc)
        raise
    finally:
        report_path = output / "export-report.json"
        with report_path.open("x", encoding="utf-8") as dst:
            json.dump(report, dst, ensure_ascii=True, indent=2)
            dst.write("\n")
            dst.flush()
            os.fsync(dst.fileno())
        _apply_export_permissions(report_path, sudo_ids, is_dir=False)
        print("Export directory: " + str(output), flush=True)
    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Miro-Ego read-only MIRO-EGO exFAT SD-card exporter"
    )
    parser.add_argument(
        "--source",
        help="whole-disk device path, raw exFAT/MBR .img file, or mounted MIRO-EGO directory",
    )
    parser.add_argument("--destination", type=Path, help="existing local destination directory")
    parser.add_argument("--list", action="store_true", help="list discovered recordings and logs")
    parser.add_argument("--recording", action="append", help="recording ID to export (repeatable)")
    parser.add_argument(
        "--include-logs",
        action="store_true",
        help="also export /logs diagnostic files alongside recordings",
    )
    parser.add_argument(
        "--logs-only",
        action="store_true",
        help="export only /logs diagnostic files without recordings",
    )
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help="allow best-effort read-only export from an uncleanly unmounted exFAT volume",
    )
    parser.add_argument(
        "--allow-unsealed",
        action="store_true",
        help="allow exporting recording directories without a sealed v1 manifest",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    interactive = args.source is None
    if interactive:
        devices = discover()
        if not devices:
            raise RuntimeError("No external SD card found; insert the card reader and retry")
        for i, d in enumerate(devices, 1):
            print(f"{i}. {d['path']}  {d['size'] / 1024**3:.1f} GiB  {d['model']}")
        index = int(input("Select SD card number: "))
        if not 1 <= index <= len(devices):
            raise RuntimeError("Invalid card selection")
        args.source = devices[index - 1]["path"]

    source_path = Path(args.source)
    if (
        not source_path.is_file()
        and not source_path.is_dir()
        and args.source not in {d["path"] for d in discover()}
    ):
        raise RuntimeError("Source is not an eligible external card, image file, or directory")

    with volumes(args.source) as found:
        if not found:
            raise RuntimeError("No MIRO-EGO exFAT recording volume found on source")
        records = recordings(found)
        logs = diagnostic_logs(found)
        for i, item in enumerate(records, 1):
            status = "sealed v1" if item["sealed"] else "unsealed"
            if item["manifest_error"]:
                status = f"invalid manifest: {item['manifest_error']}"
            dirty_note = " [dirty]" if item["dirty"] else ""
            print(f"{i}. {item['id']}  ({status}){dirty_note}")
        for item in logs:
            dirty_note = " [dirty]" if item["dirty"] else ""
            print(f"logs: {item['id']}{dirty_note}")

        if args.list:
            return

        if args.logs_only:
            selected_records: list[dict[str, Any]] = []
            selected_logs = logs
            if not selected_logs:
                raise RuntimeError("No /logs diagnostics directory found on MIRO-EGO volume")
        else:
            if not records:
                raise RuntimeError("No recordings found under /recordings/YYYY-MM-DD/HH-MM-SS/")
            selected_records = records
            if interactive:
                selection = input(
                    "Recording numbers to export (space-separated, Enter for all): "
                ).strip()
                if selection:
                    indices = [int(x) for x in selection.split()]
                    if any(i < 1 or i > len(records) for i in indices):
                        raise RuntimeError("Invalid recording number")
                    selected_records = [records[i - 1] for i in dict.fromkeys(indices)]
                if logs and not args.include_logs:
                    ans = input("Also export /logs diagnostic files? [y/N]: ").strip().lower()
                    args.include_logs = ans == "y"
            elif args.recording:
                unknown = set(args.recording) - {r["id"] for r in records}
                if unknown:
                    raise RuntimeError("Unknown recording ID: " + ", ".join(sorted(unknown)))
                selected_records = [r for r in records if r["id"] in args.recording]
            selected_logs = logs if args.include_logs else []

        if args.destination is None:
            if not interactive:
                parser.error("export requires --destination")
            args.destination = Path(input("Destination directory on computer: ").strip().strip('"'))

        destination = destination_check(args.source, args.destination)
        export_selected(
            selected_records,
            destination,
            allow_dirty=args.allow_dirty,
            allow_unsealed=args.allow_unsealed,
            log_sources=selected_logs,
        )
        print(
            "Export complete. Every file verified against readback and manifest SHA-256; "
            "source unchanged."
        )


if __name__ == "__main__":
    try:
        main()
    except (Exception, KeyboardInterrupt) as error:
        print("Export failed: " + str(error), file=sys.stderr)
        raise SystemExit(2) from None
