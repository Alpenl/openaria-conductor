"""Target-sized MBR layout and offline exFAT tools for freshly flashed RDK cards.

The image contains only the vendor payload. Partition table extents may exceed
the image file; the flasher must validate them against the actual target card.
No on-device partition resizing or in-place migration is performed.
"""

from __future__ import annotations

import hashlib
import os
import struct
import urllib.request
from pathlib import Path

GIB = 1024**3
PACKAGE = "exfatprogs_1.1.3-1ubuntu0.1_arm64.deb"
PACKAGE_URL = "https://ports.ubuntu.com/pool/universe/e/exfatprogs/" + PACKAGE
PACKAGE_SHA256 = "5a9d0deec1f2b418de4a726ec1b8a9d19cb23880fe5fedc33b41d4e500b72f4e"


def partitions(mbr):
    if len(mbr) != 512 or mbr[510:] != b"\x55\xaa":
        raise RuntimeError("Expected MBR partition table")
    return [(mbr[450 + i * 16], *struct.unpack_from("<II", mbr, 454 + i * 16)) for i in range(4)]


def plan(mbr, card_bytes, system_gib=16):
    entries = partitions(mbr)
    if entries != [(0xC, 8192, 524288), (0x83, 532480, 19767296), (0, 0, 0), (0, 0, 0)]:
        raise RuntimeError("exFAT layout requires the pinned official RDK OS 3.5.0 image")
    if type(system_gib) is not int or not 16 <= system_gib <= 64:
        raise RuntimeError("System partition must be 16–64 GiB")
    if type(card_bytes) is not int or not 20 * GIB <= card_bytes <= 512 * GIB:
        raise RuntimeError("exFAT layout requires the actual card size (20–512 GiB)")
    root_start = entries[1][1]
    root_sectors = system_gib * GIB // 512
    data_start = root_start + root_sectors
    # 1 MiB alignment; at most 1 MiB remains unused at the end of the card.
    data_sectors = (card_bytes // 512 - data_start) // 2048 * 2048
    if data_sectors * 512 < 4 * GIB:
        raise RuntimeError("Leave at least 4 GiB for recordings")
    disk_id = struct.unpack_from("<I", mbr, 440)[0]
    if not disk_id:
        raise RuntimeError("Vendor disk identity is missing")
    result = bytearray(mbr)
    struct.pack_into("<I", result, 474, root_sectors)
    # Keep boot code, disk ID, CONFIG and root start unchanged.
    result[478:494] = struct.pack(
        "<B3sB3sII", 0, b"\xfe\xff\xff", 7, b"\xfe\xff\xff", data_start, data_sectors
    )
    return bytes(result), {
        "schema": "openaria.storage.exfat.v1",
        "card_bytes": card_bytes,
        "system_gib": system_gib,
        "partuuid": f"{disk_id:08x}-03",
        "mbr_sha256": hashlib.sha256(result).hexdigest(),
        "package_sha256": PACKAGE_SHA256,
    }


def check_fit(image, card_bytes):
    with Path(image).open("rb") as src:
        mbr = src.read(512)
    # Preserve support for existing generic images, including GPT images.
    if len(mbr) != 512 or mbr[510:] != b"\x55\xaa":
        return
    entries = partitions(mbr)
    if any(kind == 0xEE for kind, _, _ in entries):
        return
    if any((start + count) * 512 > card_bytes for kind, start, count in entries if kind):
        raise RuntimeError("Partition layout exceeds target card capacity")


def prepare_tools(directory):
    """Host-side download only; first boot never requires a network connection."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    output = directory / PACKAGE
    if not output.exists():
        temporary = output.with_suffix(".partial")
        try:
            with urllib.request.urlopen(PACKAGE_URL, timeout=60) as src:
                raw = src.read(1024**2)
            if hashlib.sha256(raw).hexdigest() != PACKAGE_SHA256:
                raise RuntimeError("Offline exFAT package digest mismatch")
            with temporary.open("xb") as dst:
                dst.write(raw)
                dst.flush()
                os.fsync(dst.fileno())
            os.replace(temporary, output)
        finally:
            temporary.unlink(missing_ok=True)
    if hashlib.sha256(output.read_bytes()).hexdigest() != PACKAGE_SHA256:
        raise RuntimeError("Offline exFAT package digest mismatch")
    return output
