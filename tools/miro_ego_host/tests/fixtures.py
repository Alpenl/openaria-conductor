"""Shared synthetic fixtures for Miro-Ego host-tool tests (MBR, ELF64, bundles, exFAT)."""

from __future__ import annotations

import base64
import hashlib
import io
import json
import struct
import tarfile
from pathlib import Path
from typing import Any

DATA_VOLUME_SCRIPT = b"""#!/usr/bin/env bash
set -euo pipefail
MODE="${MIRO_EGO_DATA_VOLUME_MODE:-exfat}"
partuuid="${MIRO_EGO_DATA_VOLUME_PARTUUID:?missing}"
uuid="${MIRO_EGO_DATA_VOLUME_UUID:?missing}"
mount -t exfat "/dev/disk/by-uuid/$uuid" /data
"""

DATA_VOLUME_UNIT = b"""[Unit]
Description=Miro-Ego /data exFAT recording volume
[Service]
Type=oneshot
EnvironmentFile=-/etc/default/miro-ego-data-volume
ExecStart=/usr/local/sbin/miro-ego-data-volume ensure
"""


def vendor_mbr(*, disk_sig: bytes = bytes.fromhex("05722edb")) -> bytes:
    """Build a synthetic 512-byte official RDK OS 3.5.0 2-partition MBR."""
    raw = bytearray(512)
    raw[440:444] = disk_sig
    raw[450], raw[466] = 0x0C, 0x83
    struct.pack_into("<II", raw, 454, 8192, 524288)
    struct.pack_into("<II", raw, 470, 532480, 19767296)
    raw[510:512] = b"\x55\xaa"
    return bytes(raw)


def aarch64_elf_stub(
    *,
    elf_class: int = 2,
    elf_data: int = 1,
    elf_type: int = 3,
    elf_machine: int = 183,
    payload: bytes = b"miro-ego-native-stub",
) -> bytes:
    """Build a minimal 64+ byte ELF header stub (default: AArch64 ELF64 little-endian PIE)."""
    header = bytearray(64)
    header[:4] = b"\x7fELF"
    header[4] = elf_class
    header[5] = elf_data
    header[6] = 1
    struct.pack_into("<HH", header, 16, elf_type, elf_machine)
    return bytes(header) + payload


def factory_profile(
    *,
    board: str = "a" * 64,
    voice_volume: int = 42,
    voice_enabled: bool | None = True,
    extra_files: dict[str, bytes] | None = None,
) -> dict[str, Any]:
    """Build a valid ``miro-ego.factory-profile.v1`` profile dictionary."""
    device_cfg = {
        "schema": "miro-ego.device-config.v1",
        "state_root": "/var/lib/miro-ego",
        "security": {"profile": "lab", "isolated_network": True},
    }
    raw_files: dict[str, bytes] = {
        "etc/miro-ego/device.json": json.dumps(device_cfg).encode("utf-8"),
        "etc/machine-id": b"a" * 32,
    }
    if extra_files:
        raw_files.update(extra_files)
    encoded: dict[str, dict[str, str]] = {}
    for name, raw in raw_files.items():
        encoded[name] = {
            "data": base64.b64encode(raw).decode("ascii"),
            "sha256": hashlib.sha256(raw).hexdigest(),
        }
    voice: dict[str, Any] = {"volume": voice_volume}
    if voice_enabled is not None:
        voice["enabled"] = voice_enabled
    return {
        "schema": "miro-ego.factory-profile.v1",
        "board": board,
        "files": encoded,
        "voice": voice,
    }


def make_native_bundle(
    path: Path,
    *,
    include_encoder: bool = True,
    miro_ego_bytes: bytes | None = None,
    encoder_bytes: bytes | None = None,
    helper_bytes: bytes | None = DATA_VOLUME_SCRIPT,
    unit_bytes: bytes | None = DATA_VOLUME_UNIT,
    bundle_schema: str | None = "miro-ego.rdk-x5-bundle.v1",
    bundle_platform: str = "linux_aarch64_rdk_x5_v1",
    bundle_extra_fields: dict[str, Any] | None = None,
    include_sums: bool = True,
    extra_files: dict[str, bytes] | None = None,
) -> Path:
    """Create a synthetic native Miro-Ego ``bundle.tar`` archive matching ``BundleManifest``."""
    files: dict[str, tuple[bytes, int]] = {}
    if miro_ego_bytes is None:
        miro_ego_bytes = aarch64_elf_stub(payload=b"miro-ego-bin")
    if miro_ego_bytes:
        files["bin/miro-ego"] = (miro_ego_bytes, 0o755)
    if include_encoder:
        enc = encoder_bytes if encoder_bytes is not None else aarch64_elf_stub(payload=b"encoder")
        files["bin/miro-ego-encoder"] = (enc, 0o755)
    if helper_bytes is not None:
        files["deploy/miro-ego/miro-ego-data-volume"] = (helper_bytes, 0o755)
    if unit_bytes is not None:
        files["deploy/miro-ego/miro-ego-data-volume.service"] = (unit_bytes, 0o644)
    if bundle_schema is not None:
        binaries = [
            {
                "path": rel,
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
                "executable": True,
            }
            for rel, (data, _mode) in sorted(files.items())
            if rel.startswith("bin/")
        ]
        assets = [
            {
                "path": rel,
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
                "executable": bool(mode & 0o111),
            }
            for rel, (data, mode) in sorted(files.items())
            if rel.startswith("deploy/miro-ego/")
        ]
        manifest_obj: dict[str, Any] = {
            "schema": bundle_schema,
            "platform": bundle_platform,
            "version": "0.3.0",
            "commit": "1" * 40,
            "binaries": binaries,
            "assets": assets,
        }
        if bundle_extra_fields:
            manifest_obj.update(bundle_extra_fields)
        files["bundle.json"] = (json.dumps(manifest_obj).encode("utf-8"), 0o644)
    if extra_files:
        for name, data in extra_files.items():
            files[name] = (data, 0o644)

    if include_sums:
        lines = [
            f"{hashlib.sha256(data).hexdigest()}  {name}\n"
            for name, (data, _mode) in sorted(files.items())
        ]
        files["SHA256SUMS"] = ("".join(lines).encode("utf-8"), 0o644)

    with tarfile.open(path, "w") as tar:
        for name, (data, mode) in sorted(files.items()):
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mode = mode
            tar.addfile(info, io.BytesIO(data))
    return path


def build_session_manifest(
    files: dict[str, bytes],
    *,
    session_id: str = "0198c9a8-7a3c-7000-8000-000000000001",
    schema: str = "miro-ego.device-session.v1",
    sealed: bool = True,
    include_audio: bool = True,
    extra_log_paths: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Construct a ``miro-ego.device-session.v1`` manifest matching *files* byte-for-byte."""

    def artifact(rel: str, role: str, media_type: str) -> dict[str, Any]:
        raw = files[rel]
        digest = hashlib.sha256(raw).hexdigest()
        return {
            "artifact_id": digest,
            "role": role,
            "path": rel,
            "media_type": media_type,
            "bytes": len(raw),
            "sha256": digest,
        }

    audio_section: dict[str, Any]
    if include_audio and "audio/audio_00000.wav" in files:
        audio_section = {
            "state": "recorded",
            "requested_mode": "device_default",
            "resolved_mode": "enabled",
            "codec": "pcm_s16le",
            "container": "wav",
            "sample_format": "S16_LE",
            "sample_rate": 48000,
            "channels": 2,
            "sample_count": 48000,
            "sync": {
                "time_base": "host_monotonic",
                "start_time_seconds": 0.0,
                "end_time_seconds": 1.0,
                "video_time_reference": "session_time_seconds",
            },
            "segments": [
                {
                    "index": 0,
                    "start_sample": 0,
                    "end_sample": 48000,
                    "start_time_seconds": 0.0,
                    "end_time_seconds": 1.0,
                    "pcm_payload_bytes": max(1, len(files["audio/audio_00000.wav"]) - 44),
                    "wav_header_bytes": 44,
                    "artifact": artifact("audio/audio_00000.wav", "audio.wav", "audio/wav"),
                }
            ],
        }
    else:
        audio_section = {
            "state": "not_recorded",
            "requested_mode": "disabled",
            "resolved_mode": "disabled",
            "reason": "user_disabled",
        }

    return {
        "schema": schema,
        "manifest_id": "0198c9a8-7a3c-7000-8000-000000000002",
        "sealed": sealed,
        "sealed_at": "2026-11-26T14:30:10Z",
        "session_id": session_id,
        "volume_id": "12345678-1234-4234-8234-123456789abc",
        "capture_mode": "production",
        "display_name": "2026-11-26 14:30:05",
        "device": {
            "device_id": "12345678-1234-4234-8234-123456789abd",
            "device_label": "Miro-Ego-1234ABCD",
            "hardware_fingerprint": "sha256:" + ("a" * 64),
            "platform": "D-Robotics RDK X5 V1.0",
            "software_version": "0.3.0",
            "commit": "1" * 40,
        },
        "time": {
            "started_at": "2026-11-26T14:30:05Z",
            "ended_at": "2026-11-26T14:30:10Z",
            "timezone": "Asia/Shanghai",
            "duration_seconds": 5.0,
            "duration_clock": "host_monotonic",
        },
        "take": {
            "take_id": "0198c9a8-7a3c-7000-8000-000000000003",
            "sequence": 1,
            "continuation_of": None,
        },
        "camera": {
            "width": 3840,
            "height": 1080,
            "eye_width": 1920,
            "sensor_fps": 60,
            "frame_decimation": 2,
            "effective_fps": 30,
            "coordinate_frame": "opencv_optical",
        },
        "video": {
            "layout": "split-eyes",
            "codec": "hevc",
            "container": "mp4",
            "segments": [
                {
                    "index": 0,
                    "start_frame": 0,
                    "end_frame": 150,
                    "start_time_seconds": 0.0,
                    "end_time_seconds": 5.0,
                    "artifacts": {
                        "left": artifact("video/left_00000.mp4", "video.left", "video/mp4"),
                        "right": artifact("video/right_00000.mp4", "video.right", "video/mp4"),
                    },
                }
            ],
            "encoding": {
                "schema": "miro-ego.recording-encoding.v1",
                "codec": "hevc",
                "rate_control": "cbr",
                "profile": "main",
                "pixel_format": "yuv420p",
                "bit_depth": 8,
                "b_frames": 0,
                "bitrate_kbps": 12288,
                "min_qp": 18,
                "max_qp": 32,
                "intra_qp": 20,
                "initial_qp": 22,
                "gop_frames": 30,
                "vbv_ms": 3000,
            },
        },
        "imu": {
            "artifact": artifact("imu.ndjson", "imu.samples", "application/x-ndjson"),
            "sample_count": 100,
            "units": "raw_int16",
            "coordinate_frame": "raw_device_axes",
        },
        "frames": {
            "artifact": artifact("frames.ndjson", "frames.index", "application/x-ndjson"),
            "count": 150,
        },
        "audio": audio_section,
        "logs": [
            artifact(rel, "log.diagnostics", "application/x-ndjson") for rel in extra_log_paths
        ],
        "integrity": {
            "verified_at": "2026-11-26T14:30:10Z",
            "dropped_frames": 0,
            "drop_events": [],
            "fatal_errors": [],
        },
    }


def build_exfat_image(
    tree: dict[str, Any],
    *,
    label: str = "MIRO-EGO",
    dirty: bool = False,
    cluster_size: int = 4096,
    no_fat_chain_paths: set[str] | frozenset[str] = frozenset(),
    valid_length_overrides: dict[str, int] | None = None,
) -> bytes:
    """Build a minimal, spec-compliant exFAT filesystem image in pure Python.

    *tree* is a nested dict mapping entry names to either ``bytes`` (regular file)
    or a nested ``dict`` (directory). Supports multi-cluster FAT chains,
    ``NoFatChain`` contiguous files, volume label entry ``0x83``, dirty volume
    flags, and custom ``ValidDataLength`` overrides for testing.
    """
    sector_size = 512
    bps_shift = 9
    spc = cluster_size // sector_size
    spc_shift = spc.bit_length() - 1
    assert (1 << spc_shift) == spc

    fat_offset = 24
    fat_length = 16  # 16 * 512 / 4 = 2048 FAT entries
    heap_offset = fat_offset + fat_length
    valid_overrides = valid_length_overrides or {}

    fat_entries: dict[int, int] = {0: 0xFFFFFFF8, 1: 0xFFFFFFFF}
    clusters: dict[int, bytearray] = {}
    next_cluster = 2

    def alloc_data(raw: bytes, *, contiguous: bool) -> tuple[int, int]:
        nonlocal next_cluster
        if not raw:
            return 0, 0
        count = (len(raw) + cluster_size - 1) // cluster_size
        first = next_cluster
        for i in range(count):
            cid = next_cluster
            next_cluster += 1
            chunk = raw[i * cluster_size : (i + 1) * cluster_size]
            buf = bytearray(cluster_size)
            buf[: len(chunk)] = chunk
            clusters[cid] = buf
            if not contiguous:
                fat_entries[cid] = (cid + 1) if i + 1 < count else 0xFFFFFFFF
        return first, count * cluster_size

    def encode_entry_set(
        name: str,
        *,
        is_dir: bool,
        first_cluster: int,
        data_length: int,
        valid_length: int,
        no_fat_chain: bool,
    ) -> bytes:
        utf16 = name.encode("utf-16-le")
        name_chunks = [utf16[i : i + 30] for i in range(0, len(utf16), 30)]
        secondary_count = 1 + len(name_chunks)

        file_entry = bytearray(32)
        file_entry[0] = 0x85
        file_entry[1] = secondary_count
        attrs = 0x0010 if is_dir else 0x0020
        if name.startswith("."):
            attrs |= 0x0002  # ATTR_HIDDEN
        struct.pack_into("<H", file_entry, 4, attrs)

        stream_entry = bytearray(32)
        stream_entry[0] = 0xC0
        stream_entry[1] = 0x01 | (0x02 if no_fat_chain else 0x00)
        stream_entry[3] = len(name)
        struct.pack_into("<Q", stream_entry, 8, valid_length)
        struct.pack_into("<I", stream_entry, 20, first_cluster)
        struct.pack_into("<Q", stream_entry, 24, data_length)

        parts = [bytes(file_entry), bytes(stream_entry)]
        for chunk in name_chunks:
            name_entry = bytearray(32)
            name_entry[0] = 0xC1
            name_entry[2 : 2 + len(chunk)] = chunk
            parts.append(bytes(name_entry))
        return b"".join(parts)

    def build_dir(mapping: dict[str, Any], prefix: str, *, is_root: bool) -> tuple[int, int]:
        entries_raw = bytearray()
        if is_root and label:
            vol_entry = bytearray(32)
            vol_entry[0] = 0x83
            label_u16 = label[:11].encode("utf-16-le")
            vol_entry[1] = len(label[:11])
            vol_entry[2 : 2 + len(label_u16)] = label_u16
            entries_raw.extend(vol_entry)

        for name, value in sorted(mapping.items()):
            rel = f"{prefix}/{name}" if prefix else name
            if isinstance(value, dict):
                child_cluster, child_len = build_dir(value, rel, is_root=False)
                entries_raw.extend(
                    encode_entry_set(
                        name,
                        is_dir=True,
                        first_cluster=child_cluster,
                        data_length=child_len,
                        valid_length=child_len,
                        no_fat_chain=False,
                    )
                )
            elif isinstance(value, (bytes, bytearray)):
                raw = bytes(value)
                contiguous = rel in no_fat_chain_paths
                valid_len = valid_overrides.get(rel, len(raw))
                stored = raw[:valid_len] if valid_len < len(raw) else raw
                first_cluster, _ = alloc_data(stored, contiguous=contiguous)
                entries_raw.extend(
                    encode_entry_set(
                        name,
                        is_dir=False,
                        first_cluster=first_cluster,
                        data_length=len(raw),
                        valid_length=valid_len,
                        no_fat_chain=contiguous,
                    )
                )
            else:
                raise TypeError(f"Unsupported exFAT tree entry type for {rel}: {type(value)}")
        # End-of-directory 0x00 sentinel slot.
        entries_raw.extend(bytes(32))
        return alloc_data(bytes(entries_raw), contiguous=False)

    root_cluster, _ = build_dir(tree, "", is_root=True)
    cluster_count = max(next_cluster - 2 + 4, 16)
    volume_sectors = heap_offset + cluster_count * spc
    image = bytearray(volume_sectors * sector_size)

    # Boot sector (VBR)
    image[0:3] = b"\xeb\x76\x90"
    image[3:11] = b"EXFAT   "
    struct.pack_into("<Q", image, 72, volume_sectors)
    struct.pack_into("<I", image, 80, fat_offset)
    struct.pack_into("<I", image, 84, fat_length)
    struct.pack_into("<I", image, 88, heap_offset)
    struct.pack_into("<I", image, 92, cluster_count)
    struct.pack_into("<I", image, 96, root_cluster)
    struct.pack_into("<I", image, 100, 0x1234ABCD)
    struct.pack_into("<H", image, 104, 0x0100)
    struct.pack_into("<H", image, 106, 0x0002 if dirty else 0x0000)
    image[108] = bps_shift
    image[109] = spc_shift
    image[110] = 1  # single FAT
    image[111] = 0x80
    image[510:512] = b"\x55\xaa"

    # FAT table
    fat_base = fat_offset * sector_size
    for cid, nxt in fat_entries.items():
        struct.pack_into("<I", image, fat_base + cid * 4, nxt)

    # Cluster heap
    heap_base = heap_offset * sector_size
    for cid, buf in clusters.items():
        offset = heap_base + (cid - 2) * cluster_size
        image[offset : offset + cluster_size] = buf

    return bytes(image)


def wrap_mbr_disk(partitions_spec: dict[int, tuple[int, int, bytes]]) -> bytes:
    """Wrap ``{part_num (1..4): (part_type, start_lba, payload_bytes)}`` in an MBR disk image."""
    max_end = 512
    for _num, (_kind, start_lba, payload) in partitions_spec.items():
        sectors = (len(payload) + 511) // 512
        max_end = max(max_end, (start_lba + sectors) * 512)
    disk = bytearray(max_end)
    disk[440:444] = bytes.fromhex("05722edb")
    disk[510:512] = b"\x55\xaa"
    for part_num, (kind, start_lba, payload) in partitions_spec.items():
        assert 1 <= part_num <= 4
        sectors = (len(payload) + 511) // 512
        entry_offset = 446 + (part_num - 1) * 16
        disk[entry_offset + 4] = kind
        struct.pack_into("<II", disk, entry_offset + 8, start_lba, sectors)
        byte_start = start_lba * 512
        disk[byte_start : byte_start + len(payload)] = payload
    return bytes(disk)
