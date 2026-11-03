"""Compose an unattended image through the vendor's FAT boot hook, on any host OS."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import struct
from pathlib import Path

if __package__:
    from . import device, disks
else:
    import device
    import disks

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


def compose(base, base_sha, bundle, bundle_sha, profile, output):
    from pyfatfs.PyFatFS import PyFatFS

    base, bundle, output = Path(base), Path(bundle), Path(output)
    if not base.is_file() or not bundle.is_file():
        raise RuntimeError("Image and firmware inputs must be regular files")
    sealed = seal_profile(profile)
    if disks.digest(base) != base_sha or disks.digest(bundle) != bundle_sha:
        raise RuntimeError("Input image or bundle digest mismatch")
    offset = boot_offset(base)
    with PyFatFS(str(base), offset=offset, read_only=True) as fat:
        hook = fat.readtext("/hobot_config.sh")
        lines = [s.strip() for s in hook.splitlines() if s.strip() and not s.startswith("#")]
        if lines != ["exit 0"] or fat.exists("/openaria"):
            raise RuntimeError("Use an unmodified official image with an empty vendor hook")
    output.parent.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(output.parent).free < base.stat().st_size + 512 * 1024**2:
        raise RuntimeError("Not enough space for the composed image")
    # Exclusive creation: never replace a prior image, block device or symlink.
    fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as dst, base.open("rb") as src:
            shutil.copyfileobj(src, dst, 4 * 1024**2)
            dst.flush()
            os.fsync(dst.fileno())
        source = Path(__file__).parent
        with PyFatFS(str(output), offset=offset) as fat:
            fat.makedir("/openaria")
            for name in ("firstboot.py", "device.py", "provision.py", "disks.py"):
                fat.writebytes("/openaria/" + name, (source / name).read_bytes())
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
        }
        with output.with_suffix(".json").open("x") as dst:
            json.dump(result, dst, indent=2)
        return result
    except BaseException:
        output.unlink(missing_ok=True)
        raise
