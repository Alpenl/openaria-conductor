"""Compose unattended Miro-Ego recovery images via the vendor FAT32 CONFIG boot hook."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import struct
import sys
import tarfile
from pathlib import Path, PurePosixPath
from typing import Any

if __package__:
    from .. import disks
    from . import device, partition_layout
else:
    _parent = str(Path(__file__).resolve().parents[1])
    if (Path(_parent) / "disks.py").is_file() and _parent not in sys.path:
        sys.path.insert(0, _parent)
    import device
    import disks
    import partition_layout

MAGIC = b"MEB1"
AUTOINSTALL_SCHEMA = "miro-ego.autoinstall.v1"
NATIVE_BUNDLE_SCHEMA = "miro-ego.rdk-x5-bundle.v1"
ELF_MACHINE_AARCH64 = 183
MAX_PROFILE_BYTES = 16 * 1024**2
MAX_BINARY_BYTES = 256 * 1024**2
MAX_DEPLOY_ASSET_BYTES = 1024**2
MAX_BUNDLE_MEMBERS = 10000
MAX_BUNDLE_TOTAL_BYTES = 4 * 1024**3

HOOK = """#!/bin/bash -e
# Miro-Ego offline factory recovery; the vendor hobot-rc mounts this CONFIG partition.
/usr/bin/python3 -B /boot/config/miro-ego/firstboot.py --register
exit 0
"""

CLEAN_HOOK = """#!/bin/bash -e
# Miro-Ego offline factory recovery completed.
exit 0
"""


def seal_profile(profile: dict[str, Any]) -> bytes:
    """Encrypt a validated Miro-Ego device profile bound to its physical board ID (``MEB1``)."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    device.validate(profile)
    board = profile.get("board", "")
    if not isinstance(board, str) or not re.fullmatch(r"[0-9a-f]{64}", board):
        raise RuntimeError("Invalid board binding")
    nonce = os.urandom(12)
    key = hashlib.sha256(b"miro-ego-board-v1:" + board.encode("utf-8")).digest()
    raw = json.dumps(profile, sort_keys=True).encode("utf-8")
    return MAGIC + nonce + AESGCM(key).encrypt(nonce, raw, MAGIC)


def open_profile(raw: bytes, board: str) -> dict[str, Any]:
    """Decrypt and authenticate a ``MEB1`` board-bound device profile."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    if (
        len(raw) < 4 + 12 + 16
        or raw[:4] != MAGIC
        or len(raw) > MAX_PROFILE_BYTES
        or not isinstance(board, str)
        or not re.fullmatch(r"[0-9a-f]{64}", board)
    ):
        raise RuntimeError("Invalid board profile")
    key = hashlib.sha256(b"miro-ego-board-v1:" + board.encode("utf-8")).digest()
    profile = json.loads(AESGCM(key).decrypt(raw[4:16], raw[16:], MAGIC))
    device.validate(profile)
    if profile.get("board") != board:
        raise RuntimeError("Board mismatch")
    return profile


def boot_offset(image: str | Path) -> int:
    """Locate and validate the byte offset of the vendor FAT32 CONFIG partition."""
    image_path = Path(image)
    with image_path.open("rb") as src:
        mbr = src.read(512)
    if len(mbr) != 512 or mbr[510:512] != b"\x55\xaa" or mbr[450] not in (0x0B, 0x0C):
        raise RuntimeError("Expected vendor MBR and FAT32 CONFIG partition")
    start, sectors = struct.unpack_from("<II", mbr, 454)
    if start < 2048 or sectors < 262144 or (start + sectors) * 512 > image_path.stat().st_size:
        raise RuntimeError("Invalid boot partition bounds")
    return start * 512


def validate_aarch64_elf(header: bytes, *, name: str) -> None:
    """Validate an ELF64 little-endian AArch64 executable or PIE binary header."""
    if len(header) < 64:
        raise RuntimeError(f"Native binary {name} is truncated")
    if header[:4] != b"\x7fELF":
        raise RuntimeError(f"Native binary {name} is not an ELF executable")
    if header[4] != 2 or header[5] != 1:
        raise RuntimeError(f"Native binary {name} must be 64-bit little-endian ELF")
    elf_type, elf_machine = struct.unpack_from("<HH", header, 16)
    if elf_type not in (2, 3):
        raise RuntimeError(f"Native binary {name} must be an ELF executable or PIE")
    if elf_machine != ELF_MACHINE_AARCH64:
        raise RuntimeError(f"Native binary {name} must target AArch64 (EM_AARCH64)")


def _normalize_tar_path(name: str) -> PurePosixPath | None:
    if name in (".", "./"):
        return None
    normalized = name[2:] if name.startswith("./") else name
    path = PurePosixPath(normalized)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise RuntimeError(f"Unsafe archive path: {name!r}")
    return path


def require_native_bundle(bundle: str | Path) -> None:
    """Validate a native AArch64 Miro-Ego bundle with exFAT ``miro-ego-data-volume`` support.

    Rejects Python wheel/CPython runtime bundles, non-AArch64 binaries, unsafe tar
    members, and any legacy loop-volume fallback assets.
    """
    with tarfile.open(bundle) as archive:
        members = archive.getmembers()
        if (
            len(members) > MAX_BUNDLE_MEMBERS
            or sum(m.size for m in members) > MAX_BUNDLE_TOTAL_BYTES
        ):
            raise RuntimeError("Oversized firmware bundle")
        by_path: dict[str, tarfile.TarInfo] = {}
        for member in members:
            rel = _normalize_tar_path(member.name)
            if rel is None:
                if not member.isdir():
                    raise RuntimeError("Unsafe archive root member")
                continue
            if not (member.isfile() or member.isdir()):
                raise RuntimeError(f"Unsafe bundle member type: {member.name!r}")
            key = rel.as_posix()
            if key in by_path:
                raise RuntimeError(f"Duplicate bundle member: {key!r}")
            filename = rel.name
            if filename.endswith(".whl") or (
                filename.startswith("cpython-") and filename.endswith(".tar.gz")
            ):
                raise RuntimeError(
                    "Legacy Python runtime/wheel bundles are unsupported; "
                    "supply a native Miro-Ego bundle"
                )
            by_path[key] = member

        miro_bin = by_path.get("bin/miro-ego")
        if miro_bin is None or not miro_bin.isfile() or not 64 <= miro_bin.size <= MAX_BINARY_BYTES:
            raise RuntimeError("Missing native AArch64 bin/miro-ego executable in bundle")
        extracted_bin = archive.extractfile(miro_bin)
        if extracted_bin is None:
            raise RuntimeError("Unable to read bin/miro-ego from bundle")
        with extracted_bin as src:
            validate_aarch64_elf(src.read(64), name="bin/miro-ego")

        encoder_bin = by_path.get("bin/miro-ego-encoder")
        if encoder_bin is not None:
            if not encoder_bin.isfile() or not 64 <= encoder_bin.size <= MAX_BINARY_BYTES:
                raise RuntimeError("Invalid bin/miro-ego-encoder executable in bundle")
            extracted_enc = archive.extractfile(encoder_bin)
            if extracted_enc is None:
                raise RuntimeError("Unable to read bin/miro-ego-encoder from bundle")
            with extracted_enc as src:
                validate_aarch64_elf(src.read(64), name="bin/miro-ego-encoder")

        bundle_meta = by_path.get("bundle.json")
        if bundle_meta is not None:
            if not bundle_meta.isfile() or bundle_meta.size > MAX_DEPLOY_ASSET_BYTES:
                raise RuntimeError("Invalid bundle.json in native bundle")
            extracted_meta = archive.extractfile(bundle_meta)
            if extracted_meta is None:
                raise RuntimeError("Unable to read bundle.json")
            with extracted_meta as src:
                metadata = json.loads(src.read())
            if not isinstance(metadata, dict) or metadata.get("schema") != NATIVE_BUNDLE_SCHEMA:
                raise RuntimeError(f"bundle.json schema must be {NATIVE_BUNDLE_SCHEMA}")

        sums = by_path.get("SHA256SUMS")
        if sums is None or not sums.isfile():
            raise RuntimeError("Missing SHA256SUMS manifest in native bundle")

        helper_member = by_path.get("deploy/miro-ego/miro-ego-data-volume")
        unit_member = by_path.get("deploy/miro-ego/miro-ego-data-volume.service")
        if (
            helper_member is None
            or unit_member is None
            or not helper_member.isfile()
            or not unit_member.isfile()
            or helper_member.size > MAX_DEPLOY_ASSET_BYTES
            or unit_member.size > MAX_DEPLOY_ASSET_BYTES
        ):
            raise RuntimeError("Missing Miro-Ego exFAT data-volume deployment assets in bundle")

        extracted_helper = archive.extractfile(helper_member)
        extracted_unit = archive.extractfile(unit_member)
        if extracted_helper is None or extracted_unit is None:
            raise RuntimeError("Unable to read data-volume assets from bundle")
        with extracted_helper as src:
            helper = src.read()
        with extracted_unit as src:
            unit = src.read()
        if (
            b"MIRO_EGO_DATA_VOLUME_PARTUUID" not in helper
            or b"MIRO_EGO_DATA_VOLUME_UUID" not in helper
            or b"exfat" not in helper
        ):
            raise RuntimeError("Firmware bundle does not support MIRO-EGO exFAT recording volumes")
        if b"losetup" in helper or b"data-volume.img" in helper:
            raise RuntimeError("Firmware bundle contains unsupported loop-volume fallback")
        if (
            b"EnvironmentFile=" not in unit
            or b"/etc/default/miro-ego-data-volume" not in unit
            or b"miro-ego-data-volume" not in unit
        ):
            raise RuntimeError("Data-volume systemd unit does not configure exFAT environment")


def copy_budget(base: Path) -> int:
    """On POSIX, reserve only non-zero blocks plus headroom for the FAT payload."""
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


def copy_image(src: Any, dst: Any) -> None:
    """Preserve exact raw image bytes while skipping zero-only extents on POSIX."""
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


def _disks_module_path(source_dir: Path) -> Path:
    parent_candidate = source_dir.parent / "disks.py"
    if parent_candidate.is_file():
        return parent_candidate
    sibling_candidate = source_dir / "disks.py"
    if sibling_candidate.is_file():
        return sibling_candidate
    raise RuntimeError("Unable to locate shared disks.py helper module")


def compose(
    base: str | Path,
    base_sha: str,
    bundle: str | Path,
    bundle_sha: str,
    profile: dict[str, Any],
    output: str | Path,
    *,
    card_bytes: int,
    system_gib: int = partition_layout.SYSTEM_GIB,
    tools_directory: str | Path | None = None,
) -> dict[str, Any]:
    """Compose an unattended 3-partition MIRO-EGO recovery image on any host OS."""
    from pyfatfs.PyFatFS import PyFatFS

    base_path, bundle_path, output_path = Path(base), Path(bundle), Path(output)
    if card_bytes is None:
        raise RuntimeError("Target card capacity (card_bytes) is required for MIRO-EGO layout")
    if (
        not base_path.is_file()
        or base_path.is_symlink()
        or not bundle_path.is_file()
        or bundle_path.is_symlink()
    ):
        raise RuntimeError("Image and firmware inputs must be regular files")

    sealed = seal_profile(profile)
    if (
        disks.sha256_file(base_path) != base_sha.lower()
        or disks.sha256_file(bundle_path) != bundle_sha.lower()
    ):
        raise RuntimeError("Input image or bundle digest mismatch")

    require_native_bundle(bundle_path)
    offset = boot_offset(base_path)
    with base_path.open("rb") as src:
        mbr, layout = partition_layout.plan(src.read(512), card_bytes, system_gib)

    tools = Path(tools_directory) if tools_directory is not None else output_path.parent / "tools"
    exfatprogs_deb = partition_layout.prepare_tools(
        tools,
        partition_layout.EXFATPROGS_PACKAGE,
        partition_layout.EXFATPROGS_URL,
        partition_layout.EXFATPROGS_SHA256,
    )
    logrotate_deb = partition_layout.prepare_tools(
        tools,
        partition_layout.LOGROTATE_PACKAGE,
        partition_layout.LOGROTATE_URL,
        partition_layout.LOGROTATE_SHA256,
    )

    with PyFatFS(str(base_path), offset=offset, read_only=True) as fat:
        hook = fat.readtext("/hobot_config.sh")
        lines = [s.strip() for s in hook.splitlines() if s.strip() and not s.startswith("#")]
        if lines != ["exit 0"] or fat.exists("/miro-ego"):
            raise RuntimeError("Use an unmodified official image with an empty vendor hook")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(output_path.parent).free < copy_budget(base_path) + 512 * 1024**2:
        raise RuntimeError("Not enough space for the composed image")

    # Exclusive creation: never replace a prior image, block device, or symlink.
    fd = os.open(output_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as dst, base_path.open("rb") as src:
            copy_image(src, dst)
            dst.flush()
            os.fsync(dst.fileno())

        with output_path.open("r+b") as dst:
            dst.write(mbr)
            dst.flush()
            os.fsync(dst.fileno())

        source_dir = Path(__file__).resolve().parent
        disks_path = _disks_module_path(source_dir)
        with PyFatFS(str(output_path), offset=offset) as fat:
            fat.makedir("/miro-ego")
            fat.writebytes("/miro-ego/disks.py", disks_path.read_bytes())
            for name in (
                "firstboot.py",
                "device.py",
                "provision.py",
                "partition_layout.py",
                "storage_firstboot.py",
            ):
                fat.writebytes("/miro-ego/" + name, (source_dir / name).read_bytes())
            fat.writebytes("/miro-ego/" + exfatprogs_deb.name, exfatprogs_deb.read_bytes())
            fat.writebytes("/miro-ego/" + logrotate_deb.name, logrotate_deb.read_bytes())
            fat.writebytes("/miro-ego/profile.bin", sealed)
            with bundle_path.open("rb") as src, fat.openbin("/miro-ego/bundle.tar", "w") as dst:
                shutil.copyfileobj(src, dst, 4 * 1024**2)
            fat.writebytes(
                "/miro-ego/manifest.json",
                json.dumps(
                    {
                        "schema": AUTOINSTALL_SCHEMA,
                        "bundle_sha256": bundle_sha.lower(),
                        "profile_sha256": hashlib.sha256(sealed).hexdigest(),
                        "storage": layout,
                    },
                    indent=2,
                ).encode("utf-8"),
            )
            fat.writetext("/hobot_config.sh", HOOK)

        with output_path.open("r+b") as handle:
            os.fsync(handle.fileno())

        result = {
            "image": str(output_path.resolve()),
            "sha256": disks.sha256_file(output_path),
            "bytes": output_path.stat().st_size,
            "mode": "offline-firstboot",
            "storage": layout,
        }
        with output_path.with_suffix(".json").open("x", encoding="utf-8") as dst:
            json.dump(result, dst, indent=2)
        return result
    except BaseException:
        output_path.unlink(missing_ok=True)
        raise
