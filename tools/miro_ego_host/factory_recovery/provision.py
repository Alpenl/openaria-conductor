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
BUNDLE_SIG_DOMAIN = b"miro-ego.rdk-x5-bundle-sig.v1\n"
DEFAULT_SIGNING_SEED_MATERIAL = b"miro-ego-0.3.0-default-release-signing-seed"
TARGET_PLATFORM = "linux_aarch64_rdk_x5_v1"
REQUIRED_NATIVE_BINARIES = ("bin/miro-ego", "bin/miro-ego-encoder")
BUNDLE_MANIFEST_KEYS = frozenset({"schema", "platform", "version", "commit", "binaries", "assets"})
BUNDLE_ENTRY_KEYS = frozenset({"path", "bytes", "sha256", "executable"})
ELF_MACHINE_AARCH64 = 183
MAX_PROFILE_BYTES = 16 * 1024**2
MAX_BUNDLE_FILES = 256
MAX_BINARY_BYTES = 256 * 1024**2
MAX_BUNDLE_MANIFEST_BYTES = 128 * 1024
MAX_BUNDLE_SIG_BYTES = 256
MAX_SHA256SUMS_BYTES = 256 * 1024
MAX_DEPLOY_ASSET_BYTES = 1024**2
MAX_VOICE_ASSET_BYTES = 4 * 1024**2
MAX_TOTAL_DECLARED_BYTES = 512 * 1024**2
MAX_BUNDLE_MEMBERS = 10000
MAX_BUNDLE_TOTAL_BYTES = 4 * 1024**3
_VOICE_STEM_RE = re.compile(r"^[A-Za-z0-9_-]+\Z")
_SHA256SUMS_LINE_RE = re.compile(r"^([0-9a-f]{64})  (.+)\Z")


def default_release_public_key_hex() -> str:
    """Return the compiled-in default 32-byte Ed25519 release public key as 64 lowercase hex."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    seed = hashlib.sha256(DEFAULT_SIGNING_SEED_MATERIAL).digest()
    pub = Ed25519PrivateKey.from_private_bytes(seed).public_key()
    return pub.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw).hex()


def sign_bundle_manifest(manifest_bytes: bytes, *, seed: bytes | None = None) -> str:
    """Sign ``BUNDLE_SIG_DOMAIN + manifest_bytes`` and return ``<128-lowercase-hex>\\n``."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    raw_seed = seed if seed is not None else hashlib.sha256(DEFAULT_SIGNING_SEED_MATERIAL).digest()
    if len(raw_seed) != 32:
        raise RuntimeError("Ed25519 signing seed must be exactly 32 bytes")
    sig = Ed25519PrivateKey.from_private_bytes(raw_seed).sign(BUNDLE_SIG_DOMAIN + manifest_bytes)
    return sig.hex() + "\n"


def _resolve_trusted_public_key_bytes(trusted_public_key_hex: str | None) -> bytes:
    candidate = trusted_public_key_hex
    if candidate is None:
        env_key = os.environ.get("MIRO_EGO_RELEASE_PUBLIC_KEY_HEX", "").strip()
        if env_key:
            candidate = env_key
    if candidate is not None:
        cleaned = candidate.strip()
        if not re.fullmatch(r"[0-9a-fA-F]{64}", cleaned):
            raise RuntimeError("Invalid trusted release public key hex (expected 64 hex chars)")
        return bytes.fromhex(cleaned)
    return bytes.fromhex(default_release_public_key_hex())


def _verify_bundle_signature(
    manifest_bytes: bytes,
    sig_raw: bytes,
    trusted_pubkey_bytes: bytes,
) -> None:
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    try:
        sig_text = sig_raw.decode("ascii").rstrip(" \t\r\n")
    except UnicodeDecodeError as exc:
        raise RuntimeError("bundle.json.sig is not valid ASCII hex") from exc
    if not re.fullmatch(r"[0-9a-f]{128}", sig_text):
        raise RuntimeError("bundle.json.sig must contain 128 lowercase hex characters")
    try:
        Ed25519PublicKey.from_public_bytes(trusted_pubkey_bytes).verify(
            bytes.fromhex(sig_text),
            BUNDLE_SIG_DOMAIN + manifest_bytes,
        )
    except InvalidSignature as exc:
        raise RuntimeError("Invalid or untrusted Ed25519 signature in bundle.json.sig") from exc


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


def _validate_bundle_manifest(
    metadata: Any,
    by_path: dict[str, tarfile.TarInfo],
) -> dict[str, tuple[int, str]]:
    if not isinstance(metadata, dict):
        raise RuntimeError("bundle.json must be a JSON object")
    unknown = set(metadata) - BUNDLE_MANIFEST_KEYS
    if unknown:
        raise RuntimeError(f"bundle.json contains unknown field(s): {', '.join(sorted(unknown))}")
    if metadata.get("schema") != NATIVE_BUNDLE_SCHEMA:
        raise RuntimeError(f"bundle.json schema must be {NATIVE_BUNDLE_SCHEMA}")
    if metadata.get("platform") != TARGET_PLATFORM:
        raise RuntimeError(f"bundle.json platform must be {TARGET_PLATFORM}")
    version = metadata.get("version")
    if (
        not isinstance(version, str)
        or not 1 <= len(version) <= 64
        or any(ord(c) < 0x20 or ord(c) == 0x7F for c in version)
    ):
        raise RuntimeError("bundle.json version is invalid")
    commit = metadata.get("commit")
    if not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise RuntimeError("bundle.json commit must be a 40-character lowercase hex SHA")
    binaries = metadata.get("binaries")
    assets = metadata.get("assets", [])
    if not isinstance(binaries, list) or not binaries or not isinstance(assets, list):
        raise RuntimeError("bundle.json must declare non-empty binaries list and valid assets list")

    total_entries = len(binaries) + len(assets)
    if total_entries > MAX_BUNDLE_FILES:
        raise RuntimeError(
            f"bundle.json entry count {total_entries} exceeds MAX_BUNDLE_FILES ({MAX_BUNDLE_FILES})"
        )

    declared_binaries: set[str] = set()
    declared_entries: dict[str, tuple[int, str]] = {}
    total_declared_bytes = 0
    for section_name, entries in (("binaries", binaries), ("assets", assets)):
        for entry in entries:
            if not isinstance(entry, dict):
                raise RuntimeError(f"bundle.json {section_name} entry must be an object")
            extra_keys = set(entry) - BUNDLE_ENTRY_KEYS
            if extra_keys:
                raise RuntimeError(f"bundle.json entry has unknown key(s): {sorted(extra_keys)}")
            rel = entry.get("path")
            size = entry.get("bytes")
            digest = entry.get("sha256")
            executable = entry.get("executable", False)
            if (
                not isinstance(rel, str)
                or type(size) is not int
                or size <= 0
                or size > MAX_BINARY_BYTES
                or not isinstance(digest, str)
                or not re.fullmatch(r"[0-9a-f]{64}", digest)
                or type(executable) is not bool
            ):
                raise RuntimeError(f"Malformed bundle.json entry in {section_name}")
            if (
                not rel
                or rel.startswith("/")
                or "\\" in rel
                or any(ord(c) < 0x20 or ord(c) == 0x7F for c in rel)
                or any(seg in ("", ".", "..") for seg in rel.split("/"))
            ):
                raise RuntimeError(f"Unsafe path in bundle.json: {rel!r}")
            if rel in declared_entries:
                raise RuntimeError(f"Duplicate entry path in bundle.json: {rel!r}")
            member = by_path.get(rel)
            if member is None or not member.isfile() or member.size != size:
                raise RuntimeError(f"bundle.json entry {rel!r} missing or size mismatch in tar")
            if section_name == "binaries":
                if not rel.startswith("bin/"):
                    raise RuntimeError(f"Binary entry must live under bin/: {rel}")
                declared_binaries.add(rel)
            elif rel.startswith("deploy/miro-ego/"):
                rest = rel.removeprefix("deploy/miro-ego/")
                if not rest or "/" in rest or size > MAX_DEPLOY_ASSET_BYTES:
                    raise RuntimeError(f"Invalid deploy asset in bundle.json: {rel}")
            elif rel.startswith("share/voice/"):
                rest = rel.removeprefix("share/voice/")
                stem = rest[:-4] if rest.endswith(".wav") else ""
                if (
                    not rest
                    or "/" in rest
                    or not rest.endswith(".wav")
                    or not _VOICE_STEM_RE.fullmatch(stem)
                    or executable
                    or size > MAX_VOICE_ASSET_BYTES
                ):
                    raise RuntimeError(f"Invalid voice asset in bundle.json: {rel}")
            else:
                raise RuntimeError(
                    f"bundle.json asset must live under deploy/miro-ego/ or share/voice/: {rel}"
                )
            declared_entries[rel] = (size, digest)
            total_declared_bytes += size
            if total_declared_bytes > MAX_TOTAL_DECLARED_BYTES:
                raise RuntimeError("bundle.json declared total bytes exceed limit")
    for required in REQUIRED_NATIVE_BINARIES:
        if required not in declared_binaries:
            raise RuntimeError(f"bundle.json missing required binary declaration: {required}")
    return declared_entries


def _parse_sha256sums(raw_text: str) -> dict[str, str]:
    sums_map: dict[str, str] = {}
    for line in raw_text.splitlines():
        if not line:
            raise RuntimeError("Empty line in SHA256SUMS")
        matched = _SHA256SUMS_LINE_RE.fullmatch(line)
        if matched is None:
            raise RuntimeError(f"Malformed SHA256SUMS line: {line!r}")
        digest_hex, rel = matched.group(1), matched.group(2)
        if rel in sums_map:
            raise RuntimeError(f"Duplicate path in SHA256SUMS: {rel!r}")
        sums_map[rel] = digest_hex
    if not sums_map:
        raise RuntimeError("SHA256SUMS is empty")
    return sums_map


def require_native_bundle(
    bundle: str | Path,
    *,
    trusted_public_key_hex: str | None = None,
) -> None:
    """Validate a signed native AArch64 Miro-Ego bundle with exFAT data-volume support.

    Strictly matches ``crates/miro-ego-deploy/src/bundle.rs``: requires both
    ``bin/miro-ego`` and ``bin/miro-ego-encoder`` (AArch64 ELF64), ``bundle.json``
    (``miro-ego.rdk-x5-bundle.v1``, ``platform == "linux_aarch64_rdk_x5_v1"``),
    detached Ed25519 signature ``bundle.json.sig``, ``SHA256SUMS``, full per-member
    SHA-256 digest verification, and exFAT-only ``deploy/miro-ego/miro-ego-data-volume*``
    assets.
    """
    trusted_pubkey_bytes = _resolve_trusted_public_key_bytes(trusted_public_key_hex)
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
            lower_key = key.lower()
            if (
                filename.endswith(".whl")
                or (filename.startswith("cpython-") and filename.endswith(".tar.gz"))
                or lower_key.startswith("runtime/")
                or "/site-packages/" in lower_key
                or lower_key.endswith("/python3")
            ):
                raise RuntimeError(
                    "Legacy Python runtime/wheel bundles are unsupported; "
                    "supply a native Miro-Ego bundle"
                )
            by_path[key] = member

        for binary_rel in REQUIRED_NATIVE_BINARIES:
            bin_member = by_path.get(binary_rel)
            if (
                bin_member is None
                or not bin_member.isfile()
                or not 64 <= bin_member.size <= MAX_BINARY_BYTES
            ):
                raise RuntimeError(f"Missing native AArch64 {binary_rel} executable in bundle")
            extracted_bin = archive.extractfile(bin_member)
            if extracted_bin is None:
                raise RuntimeError(f"Unable to read {binary_rel} from bundle")
            with extracted_bin as src:
                validate_aarch64_elf(src.read(64), name=binary_rel)

        bundle_meta = by_path.get("bundle.json")
        if (
            bundle_meta is None
            or not bundle_meta.isfile()
            or bundle_meta.size == 0
            or bundle_meta.size > MAX_BUNDLE_MANIFEST_BYTES
        ):
            raise RuntimeError("Missing or invalid bundle.json in native bundle")
        extracted_meta = archive.extractfile(bundle_meta)
        if extracted_meta is None:
            raise RuntimeError("Unable to read bundle.json")
        with extracted_meta as src:
            manifest_bytes = src.read(MAX_BUNDLE_MANIFEST_BYTES + 1)
        if len(manifest_bytes) != bundle_meta.size:
            raise RuntimeError("Short read on bundle.json")

        sig_member = by_path.get("bundle.json.sig")
        if (
            sig_member is None
            or not sig_member.isfile()
            or sig_member.size == 0
            or sig_member.size > MAX_BUNDLE_SIG_BYTES
        ):
            raise RuntimeError("Missing or invalid bundle.json.sig signature in native bundle")
        extracted_sig = archive.extractfile(sig_member)
        if extracted_sig is None:
            raise RuntimeError("Unable to read bundle.json.sig")
        with extracted_sig as src:
            sig_bytes = src.read(MAX_BUNDLE_SIG_BYTES + 1)
        if len(sig_bytes) != sig_member.size:
            raise RuntimeError("Short read on bundle.json.sig")
        _verify_bundle_signature(manifest_bytes, sig_bytes, trusted_pubkey_bytes)

        try:
            metadata = json.loads(manifest_bytes)
        except ValueError as exc:
            raise RuntimeError("bundle.json is not valid JSON") from exc
        declared_entries = _validate_bundle_manifest(metadata, by_path)

        sums = by_path.get("SHA256SUMS")
        if sums is None or not sums.isfile() or sums.size == 0 or sums.size > MAX_SHA256SUMS_BYTES:
            raise RuntimeError("Missing SHA256SUMS manifest in native bundle")
        extracted_sums = archive.extractfile(sums)
        if extracted_sums is None:
            raise RuntimeError("Unable to read SHA256SUMS")
        with extracted_sums as src:
            sums_bytes = src.read(MAX_SHA256SUMS_BYTES + 1)
        if len(sums_bytes) != sums.size:
            raise RuntimeError("Short read on SHA256SUMS")
        try:
            sums_map = _parse_sha256sums(sums_bytes.decode("utf-8"))
        except UnicodeDecodeError as exc:
            raise RuntimeError("SHA256SUMS is not valid UTF-8") from exc

        expected_sums = {rel: digest for rel, (_sz, digest) in declared_entries.items()}
        expected_sums["bundle.json"] = hashlib.sha256(manifest_bytes).hexdigest()
        if sums_map != expected_sums:
            raise RuntimeError("SHA256SUMS does not match signed bundle.json manifest")

        regular_files = {key for key, m in by_path.items() if m.isfile()}
        allowed_files = set(declared_entries) | {"bundle.json", "bundle.json.sig", "SHA256SUMS"}
        undeclared = sorted(regular_files - allowed_files)
        if undeclared:
            raise RuntimeError(
                f"Bundle archive contains undeclared file(s): {', '.join(undeclared)}"
            )

        for rel, (expected_size, expected_sha) in sorted(declared_entries.items()):
            member = by_path[rel]
            extracted = archive.extractfile(member)
            if extracted is None:
                raise RuntimeError(f"Unable to read {rel} from bundle")
            with extracted as src:
                actual_sha, actual_size = disks.sha256_stream(src, expected_size)
                extra = src.read(1)
            if extra or actual_size != expected_size or actual_sha != expected_sha:
                raise RuntimeError(f"Digest or byte-length mismatch for bundle member {rel}")
            if rel.startswith("bin/"):
                extracted_hdr = archive.extractfile(member)
                if extracted_hdr is None:
                    raise RuntimeError(f"Unable to inspect ELF header of {rel}")
                with extracted_hdr as src:
                    validate_aarch64_elf(src.read(64), name=rel)

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
            manifest_payload: dict[str, Any] = {
                "schema": AUTOINSTALL_SCHEMA,
                "bundle_sha256": bundle_sha.lower(),
                "profile_sha256": hashlib.sha256(sealed).hexdigest(),
                "storage": layout,
            }
            trusted_key = os.environ.get("MIRO_EGO_RELEASE_PUBLIC_KEY_HEX", "").strip()
            if trusted_key:
                manifest_payload["bundle"] = {
                    "trusted_public_key_hex": trusted_key.lower(),
                }
            fat.writebytes(
                "/miro-ego/manifest.json",
                json.dumps(manifest_payload, indent=2).encode("utf-8"),
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
