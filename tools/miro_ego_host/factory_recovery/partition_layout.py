"""3-partition MBR layout and offline .deb verification for Miro-Ego RDK X5 cards.

Canonical layout on top of the official RDK OS 3.5.0 vendor image:
- p1: CONFIG   (256 MiB FAT32, type 0x0C, start sector 8192, 524,288 sectors)
- p2: rootfs   (fixed 20 GiB ext4, type 0x83, start sector 532480, 41,943,040 sectors)
- p3: MIRO-EGO (remaining capacity exFAT, type 0x07, start sector 42475520,
                1 MiB aligned, >= 4 GiB, < 1 MiB tail slack)

No loop-volume fallback exists: recordings are strictly confined to exFAT p3.
"""

from __future__ import annotations

import hashlib
import os
import struct
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

GIB = 1024**3
MIB = 1024**2
SECTOR_SIZE = 512
ALIGNMENT_SECTORS = 2048  # 1 MiB alignment
SYSTEM_GIB = 20
MIN_DATA_GIB = 4
MIN_CARD_BYTES = 20 * GIB
MAX_CARD_BYTES = 512 * GIB

LAYOUT_SCHEMA = "miro-ego.storage.exfat.v1"
DATA_VOLUME_LABEL = "MIRO-EGO"

VENDOR_CONFIG_PARTITION = (0x0C, 8192, 524288)
VENDOR_ROOT_PARTITION = (0x83, 532480, 19767296)
VENDOR_PARTITIONS = [
    VENDOR_CONFIG_PARTITION,
    VENDOR_ROOT_PARTITION,
    (0, 0, 0),
    (0, 0, 0),
]

EXFATPROGS_PACKAGE = "exfatprogs_1.1.3-1ubuntu0.1_arm64.deb"
EXFATPROGS_URL = "https://ports.ubuntu.com/pool/universe/e/exfatprogs/" + EXFATPROGS_PACKAGE
EXFATPROGS_SHA256 = "5a9d0deec1f2b418de4a726ec1b8a9d19cb23880fe5fedc33b41d4e500b72f4e"

LOGROTATE_PACKAGE = "logrotate_3.19.0-1ubuntu1.1_arm64.deb"
LOGROTATE_URL = "https://ports.ubuntu.com/ubuntu-ports/pool/main/l/logrotate/" + LOGROTATE_PACKAGE
LOGROTATE_SHA256 = "bd11fab1ae9c442f188fa06abfbc400365babbe1778afa53dd922968d31da106"

# Short aliases for callers/tests.
PACKAGE = EXFATPROGS_PACKAGE
PACKAGE_URL = EXFATPROGS_URL
PACKAGE_SHA256 = EXFATPROGS_SHA256
LOGROTATE = LOGROTATE_PACKAGE

MAX_PACKAGE_BYTES = 16 * 1024**2


def partitions(mbr: bytes | bytearray) -> list[tuple[int, int, int]]:
    """Decode the 4 primary MBR partition entries as ``(type, start_lba, sector_count)``."""
    if len(mbr) != 512 or mbr[510:512] != b"\x55\xaa":
        raise RuntimeError("Expected 512-byte MBR partition table")
    return [(mbr[450 + i * 16], *struct.unpack_from("<II", mbr, 454 + i * 16)) for i in range(4)]


def plan(
    mbr: bytes | bytearray,
    card_bytes: int,
    system_gib: int = SYSTEM_GIB,
) -> tuple[bytes, dict[str, Any]]:
    """Compute the 3-partition Miro-Ego MBR and storage layout descriptor."""
    entries = partitions(mbr)
    if entries != VENDOR_PARTITIONS or any(mbr[478:510]):
        raise RuntimeError("MIRO-EGO exFAT layout requires the pinned official RDK OS 3.5.0 image")
    if type(system_gib) is not int or system_gib != SYSTEM_GIB:
        raise RuntimeError("System partition must be exactly 20 GiB")
    if (
        type(card_bytes) is not int
        or not MIN_CARD_BYTES <= card_bytes <= MAX_CARD_BYTES
        or card_bytes % SECTOR_SIZE != 0
    ):
        raise RuntimeError("MIRO-EGO exFAT layout requires actual card capacity (20-512 GiB)")
    root_start = entries[1][1]
    root_sectors = system_gib * GIB // SECTOR_SIZE
    data_start = root_start + root_sectors
    if data_start % ALIGNMENT_SECTORS != 0:
        raise RuntimeError("Recording partition start is not 1 MiB aligned")
    available_sectors = card_bytes // SECTOR_SIZE - data_start
    if available_sectors <= 0:
        raise RuntimeError("Leave at least 4 GiB for recordings")
    # 1 MiB alignment; strictly less than 1 MiB remains unused at the tail of the card.
    data_sectors = (available_sectors // ALIGNMENT_SECTORS) * ALIGNMENT_SECTORS
    if data_sectors * SECTOR_SIZE < MIN_DATA_GIB * GIB:
        raise RuntimeError("Leave at least 4 GiB for recordings")
    disk_id = struct.unpack_from("<I", mbr, 440)[0]
    if not disk_id:
        raise RuntimeError("Vendor MBR disk signature is missing")
    result = bytearray(mbr)
    # Keep boot code, 32-bit disk signature, p1 CONFIG, and p2 start unchanged.
    struct.pack_into("<I", result, 474, root_sectors)
    # Partition 3: exFAT/NTFS type 0x07, LBA CHS sentinel 0xFE 0xFF 0xFF.
    result[478:494] = struct.pack(
        "<B3sB3sII", 0, b"\xfe\xff\xff", 0x07, b"\xfe\xff\xff", data_start, data_sectors
    )
    layout = {
        "schema": LAYOUT_SCHEMA,
        "label": DATA_VOLUME_LABEL,
        "card_bytes": card_bytes,
        "system_gib": system_gib,
        "data_start_sector": data_start,
        "data_sectors": data_sectors,
        "partuuid": f"{disk_id:08x}-03",
        "mbr_sha256": hashlib.sha256(result).hexdigest(),
        "exfatprogs_sha256": EXFATPROGS_SHA256,
        "logrotate_sha256": LOGROTATE_SHA256,
    }
    return bytes(result), layout


def check_fit(image: str | Path, card_bytes: int) -> None:
    """Verify every non-empty MBR partition in *image* fits within *card_bytes*."""
    with Path(image).open("rb") as src:
        mbr = src.read(512)
    if len(mbr) != 512 or mbr[510:512] != b"\x55\xaa":
        return
    entries = partitions(mbr)
    if any(kind == 0xEE for kind, _, _ in entries):
        # Preserve compatibility with generic GPT images.
        return
    if any((start + count) * SECTOR_SIZE > card_bytes for kind, start, count in entries if kind):
        raise RuntimeError("Partition layout exceeds target card capacity")


def verify_deb(path: str | Path, expected_sha256: str) -> str:
    """Verify an offline ``.deb`` file exists, is bounded, and matches *expected_sha256*."""
    package_path = Path(path)
    if not package_path.is_file() or package_path.is_symlink():
        raise RuntimeError(f"Offline package missing or unsafe: {package_path.name}")
    if package_path.stat().st_size > MAX_PACKAGE_BYTES:
        raise RuntimeError(f"Offline package exceeds size limit: {package_path.name}")
    hasher = hashlib.sha256()
    with package_path.open("rb") as src:
        for chunk in iter(lambda: src.read(64 * 1024), b""):
            hasher.update(chunk)
    actual = hasher.hexdigest()
    if actual != expected_sha256.lower():
        raise RuntimeError(f"Offline package digest mismatch: {package_path.name}")
    return actual


def _download_package(urls: list[str], *, package_name: str) -> bytes:
    errors: list[str] = []
    for url in urls:
        try:
            with urllib.request.urlopen(url, timeout=60) as src:
                chunks: list[bytes] = []
                total = 0
                while True:
                    chunk = src.read(64 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > MAX_PACKAGE_BYTES:
                        raise RuntimeError("Offline package exceeds size limit")
                    chunks.append(chunk)
                return b"".join(chunks)
        except (urllib.error.URLError, OSError, RuntimeError) as exc:
            errors.append(f"{url}: {exc}")
            print(
                f"Warning: failed to download {package_name} from {url}: {exc}",
                file=sys.stderr,
            )
    if package_name == EXFATPROGS_PACKAGE:
        hint = " override via MIRO_EGO_EXFATPROGS_URL or"
    elif package_name == LOGROTATE_PACKAGE:
        hint = " override via MIRO_EGO_LOGROTATE_URL or"
    else:
        hint = ""
    raise RuntimeError(
        f"Failed to download {package_name};{hint} place the "
        f"verified .deb in tools directory ({'; '.join(errors)})"
    )


def prepare_tools(
    directory: str | Path,
    name: str = EXFATPROGS_PACKAGE,
    url: str = EXFATPROGS_URL,
    digest: str = EXFATPROGS_SHA256,
    *,
    package_url: str | None = None,
) -> Path:
    """Host-side download & SHA-256 verification of offline ARM64 ``.deb`` packages."""
    directory_path = Path(directory)
    directory_path.mkdir(parents=True, exist_ok=True)
    output = directory_path / name
    if not output.exists():
        if name == EXFATPROGS_PACKAGE:
            configured = package_url or os.environ.get("MIRO_EGO_EXFATPROGS_URL", "").strip()
            fallback = os.environ.get("MIRO_EGO_EXFATPROGS_FALLBACK_URL", "").strip()
            candidates = [c for c in (configured, url, fallback) if c]
        elif name == LOGROTATE_PACKAGE:
            configured = package_url or os.environ.get("MIRO_EGO_LOGROTATE_URL", "").strip()
            candidates = [c for c in (configured, url) if c]
        else:
            candidates = [c for c in (package_url, url) if c]
        urls = list(dict.fromkeys(candidates))
        temporary = output.with_suffix(".partial")
        try:
            raw = _download_package(urls, package_name=name)
            if hashlib.sha256(raw).hexdigest() != digest.lower():
                raise RuntimeError(f"Offline package digest mismatch: {name}")
            with temporary.open("xb") as dst:
                dst.write(raw)
                dst.flush()
                os.fsync(dst.fileno())
            os.replace(temporary, output)
        finally:
            temporary.unlink(missing_ok=True)
    verify_deb(output, digest)
    return output
