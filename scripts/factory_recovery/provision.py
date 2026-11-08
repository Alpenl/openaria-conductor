"""Compose an unattended image through the vendor's FAT boot hook, on any host OS."""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import shutil
import struct
import tarfile
import zipfile
from pathlib import Path

if __package__:
    from . import device, disks, partition_layout
else:
    import device
    import disks
    import partition_layout

MAGIC = b"OAB1"
HOOK = """#!/bin/bash -e
# OpenAria offline recovery; the vendor hobot-rc mounts this CONFIG partition.
/usr/bin/python3 /boot/config/openaria/firstboot.py --register
exit 0
"""


def seal_profile(profile):
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    device.validate(profile)
    if not re.fullmatch(r"[0-9a-f]{64}", profile.get("board", "")):
        raise RuntimeError("Invalid board binding")
    nonce = os.urandom(12)
    key = hashlib.sha256(b"openaria-board-v1:" + profile["board"].encode()).digest()
    raw = json.dumps(profile).encode()
    return MAGIC + nonce + AESGCM(key).encrypt(nonce, raw, MAGIC)


def open_profile(raw, board):
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    if raw[:4] != MAGIC or len(raw) > 16 * 1024**2:
        raise RuntimeError("Invalid board profile")
    key = hashlib.sha256(b"openaria-board-v1:" + board.encode()).digest()
    profile = json.loads(AESGCM(key).decrypt(raw[4:16], raw[16:], MAGIC))
    device.validate(profile)
    if profile["board"] != board:
        raise RuntimeError("Board mismatch")
    return profile


def boot_offset(image):
    with Path(image).open("rb") as src:
        mbr = src.read(512)
    if len(mbr) != 512 or mbr[510:] != b"\x55\xaa" or mbr[450] not in (0xB, 0xC):
        raise RuntimeError("Expected vendor MBR and FAT32 CONFIG partition")
    start, sectors = struct.unpack_from("<II", mbr, 454)
    if start < 2048 or sectors < 262144 or (start + sectors) * 512 > Path(image).stat().st_size:
        raise RuntimeError("Invalid boot partition bounds")
    return start * 512


def require_partition_firmware(bundle):
    """Never pair a new layout with an old installer that creates an 80 GiB loop."""
    with tarfile.open(bundle) as archive:
        wheels = [
            m
            for m in archive.getmembers()
            if Path(m.name).name.startswith("rp_ylx-") and m.name.endswith(".whl")
        ]
        if len(wheels) != 1 or not wheels[0].isfile() or wheels[0].size > 256 * 1024**2:
            raise RuntimeError("Missing partition-capable application wheel")
        with (
            archive.extractfile(wheels[0]) as src,
            zipfile.ZipFile(io.BytesIO(src.read())) as wheel,
        ):
            helper = wheel.read("rp_ylx/deploy/rp-ylx-data-volume")
            unit = wheel.read("rp_ylx/deploy/rp-ylx-data-volume.service")
            if b"RP_YLX_DATA_VOLUME_PARTUUID" not in helper or b"EnvironmentFile=" not in unit:
                raise RuntimeError("Firmware does not support exFAT recording partitions")


def copy_budget(base):
    """On Unix, reserve only nonzero blocks plus room for the FAT payload."""
    if os.name != "posix":
        return base.stat().st_size
    block_size = 4 * 1024**2
    zeros = bytes(block_size)
    required = 0
    with base.open("rb") as src:
        for block in iter(lambda: src.read(block_size), b""):
            if block != zeros[: len(block)]:
                required += block_size
    return required


def copy_image(src, dst):
    """Preserve exact raw image bytes while avoiding allocated zero-only extents."""
    if os.name != "posix":
        shutil.copyfileobj(src, dst, 4 * 1024**2)
        return
    zeros = bytes(4 * 1024**2)
    for block in iter(lambda: src.read(len(zeros)), b""):
        if block == zeros[: len(block)]:
            dst.seek(len(block), os.SEEK_CUR)
        else:
            dst.write(block)
    dst.truncate(src.tell())


def compose(
    base,
    base_sha,
    bundle,
    bundle_sha,
    profile,
    output,
    *,
    card_bytes=None,
    system_gib=16,
    tools_directory=None,
):
    from pyfatfs.PyFatFS import PyFatFS

    base, bundle, output = Path(base), Path(bundle), Path(output)
    if not base.is_file() or not bundle.is_file():
        raise RuntimeError("Image and firmware inputs must be regular files")
    sealed = seal_profile(profile)
    if disks.digest(base) != base_sha or disks.digest(bundle) != bundle_sha:
        raise RuntimeError("Input image or bundle digest mismatch")
    offset = boot_offset(base)
    layout = None
    if card_bytes is not None:
        require_partition_firmware(bundle)
        with base.open("rb") as src:
            mbr, layout = partition_layout.plan(src.read(512), card_bytes, system_gib)
        package = partition_layout.prepare_tools(tools_directory or output.parent / "tools")
    with PyFatFS(str(base), offset=offset, read_only=True) as fat:
        hook = fat.readtext("/hobot_config.sh")
        lines = [s.strip() for s in hook.splitlines() if s.strip() and not s.startswith("#")]
        if lines != ["exit 0"] or fat.exists("/openaria"):
            raise RuntimeError("Use an unmodified official image with an empty vendor hook")
    output.parent.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(output.parent).free < copy_budget(base) + 512 * 1024**2:
        raise RuntimeError("Not enough space for the composed image")
    # Exclusive creation: never replace a prior image, block device or symlink.
    fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as dst, base.open("rb") as src:
            copy_image(src, dst)
            dst.flush()
            os.fsync(dst.fileno())
        if layout is not None:
            with output.open("r+b") as dst:
                dst.write(mbr)
        source = Path(__file__).parent
        with PyFatFS(str(output), offset=offset) as fat:
            fat.makedir("/openaria")
            for name in (
                "firstboot.py",
                "device.py",
                "provision.py",
                "disks.py",
                "partition_layout.py",
                "storage_firstboot.py",
            ):
                fat.writebytes("/openaria/" + name, (source / name).read_bytes())
            if layout is not None:
                fat.writebytes("/openaria/" + package.name, package.read_bytes())
            fat.writebytes("/openaria/profile.bin", sealed)
            with bundle.open("rb") as src, fat.openbin("/openaria/bundle.tar", "w") as dst:
                shutil.copyfileobj(src, dst, 4 * 1024**2)
            fat.writebytes(
                "/openaria/manifest.json",
                json.dumps(
                    {
                        "schema": "openaria.autoinstall.v1",
                        "bundle_sha256": bundle_sha,
                        "profile_sha256": hashlib.sha256(sealed).hexdigest(),
                        **({"storage": layout} if layout is not None else {}),
                    }
                ).encode(),
            )
            fat.writetext("/hobot_config.sh", HOOK)
        with output.open("r+b") as src:
            os.fsync(src.fileno())
        result = {
            "image": str(output.resolve()),
            "sha256": disks.digest(output),
            "bytes": output.stat().st_size,
            "mode": "offline-firstboot",
            **({"storage": layout} if layout is not None else {}),
        }
        with output.with_suffix(".json").open("x") as dst:
            json.dump(result, dst, indent=2)
        return result
    except BaseException:
        output.unlink(missing_ok=True)
        raise
