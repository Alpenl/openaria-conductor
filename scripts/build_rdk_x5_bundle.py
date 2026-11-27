"""Build a pure-native Miro-Ego RDK X5 release bundle (zero CPython, zero wheels).

Matches ``crates/miro-ego-deploy/src/bundle.rs`` (``miro-ego.rdk-x5-bundle.v1``) and
``tools/miro_ego_host/factory_recovery/provision.py`` (``require_native_bundle``) byte-for-byte.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import struct
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.release_version import project_version  # noqa: E402

BUNDLE_SCHEMA = "miro-ego.rdk-x5-bundle.v1"
BUNDLE_SIG_DOMAIN = b"miro-ego.rdk-x5-bundle-sig.v1\n"
DEFAULT_RELEASE_SIGNING_SEED_LABEL = b"miro-ego-0.3.0-default-release-signing-seed"
DEFAULT_SIGNING_SEED_MATERIAL = DEFAULT_RELEASE_SIGNING_SEED_LABEL
TARGET_PLATFORM = "linux_aarch64_rdk_x5_v1"
AARCH64_RUST_TARGET = "aarch64-unknown-linux-gnu"
AARCH64_ZIG_RUST_TARGET = "aarch64-unknown-linux-gnu.2.28"
AARCH64_ZIG_CC_TARGET = "aarch64-linux-gnu.2.28"
MOCK_ENCODER_MARKER = b"mock-hobot-sdk:"
REQUIRED_BINARIES = ("bin/miro-ego", "bin/miro-ego-encoder")
REQUIRED_DEPLOY_ASSETS = ("miro-ego-data-volume", "miro-ego-data-volume.service")
REQUIRED_VOICE_ASSETS = (
    "device_ready.wav",
    "recording_started.wav",
    "recording_stopped.wav",
    "storage_low.wav",
    "error_alert.wav",
)
ENCODER_C_SOURCES = (
    "miro_ego_stereo_encoder.c",
    "miro_ego_stereo_pipeline.c",
    "miro_ego_shm_ring.c",
    "miro_ego_sha256.c",
    "miro_ego_split.c",
    "miro_ego_mp4_meta.c",
)
MAX_BUNDLE_FILES = 256
MAX_BINARY_BYTES = 256 * 1024 * 1024
MAX_VOICE_WAV_BYTES = 4 * 1024 * 1024
MAX_TOTAL_BUNDLE_BYTES = 512 * 1024 * 1024

ELF_MAGIC = b"\x7fELF"
ELFCLASS64 = 2
ELFDATA2LSB = 1
ET_EXEC = 2
ET_DYN = 3
EM_AARCH64 = 183
COMMIT_RE = re.compile(r"^[0-9a-f]{40}\Z")
HEX_32B_RE = re.compile(r"^[0-9a-fA-F]{64}\Z")
VOICE_STEM_RE = re.compile(r"^[A-Za-z0-9_-]+\Z")


def resolve_signing_seed(signing_seed_hex: str | None = None) -> bytes:
    """Resolve 32-byte Ed25519 private signing seed from CLI arg, env var, or default."""
    candidate = signing_seed_hex
    if candidate is None:
        env_seed = os.environ.get("MIRO_EGO_RELEASE_SIGNING_SEED_HEX", "").strip()
        if env_seed:
            candidate = env_seed
    if candidate is not None:
        cleaned = candidate.strip()
        if not HEX_32B_RE.fullmatch(cleaned):
            raise ValueError("Ed25519 signing seed must be a 64-character hex string (32 bytes)")
        return bytes.fromhex(cleaned)
    return hashlib.sha256(DEFAULT_SIGNING_SEED_MATERIAL).digest()


def sign_manifest_bytes(manifest_bytes: bytes, seed: bytes) -> tuple[str, str]:
    """Sign ``BUNDLE_SIG_DOMAIN + manifest_bytes`` and return ``(sig_hex, public_key_hex)``."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    private_key = Ed25519PrivateKey.from_private_bytes(seed)
    public_key_hex = (
        private_key.public_key()
        .public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        .hex()
    )
    signature_hex = private_key.sign(BUNDLE_SIG_DOMAIN + manifest_bytes).hex()
    return signature_hex, public_key_hex


def sha256_file(path: Path) -> tuple[int, str, bytes]:
    """Stream-hash a regular non-symlink file and return ``(bytes, sha256, first_64_bytes)``."""
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"Expected a regular non-symlink file: {path}")
    hasher = hashlib.sha256()
    total = 0
    header = bytearray()
    with path.open("rb") as src:
        while True:
            chunk = src.read(64 * 1024)
            if not chunk:
                break
            if len(header) < 64:
                need = 64 - len(header)
                header.extend(chunk[:need])
            hasher.update(chunk)
            total += len(chunk)
    return total, hasher.hexdigest(), bytes(header)


def _file_contains_bytes(path: Path, needle: bytes) -> bool:
    if not needle:
        return False
    overlap = len(needle) - 1
    tail = b""
    with path.open("rb") as src:
        while chunk := src.read(64 * 1024):
            window = tail + chunk
            if needle in window:
                return True
            tail = window[-overlap:] if overlap > 0 else b""
    return False


def validate_aarch64_elf64(header: bytes, label: str) -> None:
    """Validate an ELF64 little-endian AArch64 executable or PIE binary header."""
    if len(header) < 64:
        raise ValueError(f"{label} is too small to be an ELF64 binary")
    if header[:4] != ELF_MAGIC:
        raise ValueError(f"{label} is not an ELF binary")
    if header[4] != ELFCLASS64 or header[5] != ELFDATA2LSB:
        raise ValueError(f"{label} must be 64-bit little-endian ELF64")
    elf_type, elf_machine = struct.unpack_from("<HH", header, 16)
    if elf_type not in (ET_EXEC, ET_DYN):
        raise ValueError(
            f"{label} has unsupported ELF type {elf_type} (expected ET_EXEC or ET_DYN)"
        )
    if elf_machine != EM_AARCH64:
        raise ValueError(
            f"{label} is not an AArch64 binary (e_machine={elf_machine}, expected {EM_AARCH64})"
        )


def validate_pcm16_wav(raw: bytes, label: str) -> None:
    """Validate an uncompressed RIFF/WAVE PCM16 voice cue asset."""
    if len(raw) < 44 or raw[:4] != b"RIFF" or raw[8:12] != b"WAVE":
        raise ValueError(f"{label} is not a valid RIFF/WAVE file")
    offset = 12
    fmt_ok = False
    data_ok = False
    while offset + 8 <= len(raw):
        chunk_id = raw[offset : offset + 4]
        chunk_size = struct.unpack_from("<I", raw, offset + 4)[0]
        data_start = offset + 8
        data_end = data_start + chunk_size
        if data_end > len(raw):
            raise ValueError(f"{label} has truncated WAV chunk {chunk_id!r}")
        if chunk_id == b"fmt ":
            if chunk_size < 16:
                raise ValueError(f"{label} has undersized WAV fmt chunk")
            audio_fmt = struct.unpack_from("<H", raw, data_start)[0]
            bits_per_sample = struct.unpack_from("<H", raw, data_start + 14)[0]
            if audio_fmt != 1 or bits_per_sample != 16:
                raise ValueError(f"{label} must be uncompressed PCM16 WAV")
            fmt_ok = True
        elif chunk_id == b"data":
            if not fmt_ok:
                raise ValueError(f"{label} has WAV data chunk before fmt chunk")
            if chunk_size == 0 or chunk_size % 2 != 0:
                raise ValueError(f"{label} must have non-empty even-byte PCM16 data")
            data_ok = True
        offset = data_start + chunk_size + (chunk_size & 1)
    if not fmt_ok or not data_ok:
        raise ValueError(f"{label} is missing required PCM16 fmt/data chunks")


def validate_relative_path(rel: str) -> None:
    """Reject traversal, backslashes, control characters, and legacy Python payloads."""
    if (
        not rel
        or rel.startswith("/")
        or "\\" in rel
        or any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in rel)
    ):
        raise ValueError(f"Unsafe bundle entry path: {rel!r}")
    lower = rel.lower()
    if (
        lower.endswith(".whl")
        or lower.startswith("runtime/")
        or "/site-packages/" in lower
        or lower.endswith("/python3")
    ):
        raise ValueError(f"Legacy Python runtime or wheel payload is forbidden: {rel}")
    for segment in rel.split("/"):
        if segment in ("", ".", ".."):
            raise ValueError(f"Path traversal or empty segment in bundle entry: {rel!r}")


def _validate_version(version: str) -> str:
    if not version or len(version) > 64 or any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in version):
        raise ValueError(f"Invalid bundle version string: {version!r}")
    return version


def _resolve_commit(commit: str | None) -> str:
    resolved = commit
    if resolved is None:
        resolved = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip()
    if not COMMIT_RE.fullmatch(resolved):
        raise ValueError(f"Commit must be a 40-character lowercase hex SHA, got {resolved!r}")
    return resolved


def resolve_hobot_sdk_paths(
    hb_sdk_root: Path | None = None,
    *,
    environ: dict[str, str] | None = None,
) -> tuple[Path, Path] | None:
    """Resolve ``(include_dir, lib_dir)`` for ``libmultimedia.so`` from ``HB_SDK_ROOT`` or env."""
    env = os.environ if environ is None else environ
    explicit_inc = env.get("HOBOT_SDK_INCLUDE", "").strip()
    explicit_lib = env.get("HOBOT_SDK_LIB", "").strip()
    if explicit_inc or explicit_lib:
        if not (explicit_inc and explicit_lib):
            raise ValueError(
                "Both HOBOT_SDK_INCLUDE and HOBOT_SDK_LIB must be set when overriding Hobot SDK"
            )
        inc_path = Path(explicit_inc)
        lib_path = Path(explicit_lib)
        if not inc_path.is_dir() or not (lib_path / "libmultimedia.so").is_file():
            raise ValueError(
                f"Invalid Hobot SDK paths: expected directory {inc_path} and "
                f"{lib_path / 'libmultimedia.so'}"
            )
        return inc_path, lib_path

    sdk_root = hb_sdk_root
    if sdk_root is None:
        raw_root = env.get("HB_SDK_ROOT", "").strip()
        if raw_root:
            sdk_root = Path(raw_root)
    if sdk_root is None:
        return None

    for inc_rel, lib_rel in (
        ("include", "lib"),
        ("usr/include", "usr/hobot/lib"),
        ("usr/hobot/include", "usr/hobot/lib"),
    ):
        inc_candidate = sdk_root / inc_rel
        lib_candidate = sdk_root / lib_rel
        if inc_candidate.is_dir() and (lib_candidate / "libmultimedia.so").is_file():
            return inc_candidate, lib_candidate

    raise ValueError(
        f"HB_SDK_ROOT ({sdk_root}) is missing Hobot multimedia headers or libmultimedia.so"
    )


def compile_miro_ego_binary(
    *,
    commit: str,
    public_key_hex: str | None = None,
    workspace_root: Path = ROOT,
) -> Path:
    """Compile AArch64 ``miro-ego`` with ``cargo --locked``."""
    cargo_bin = shutil.which("cargo")
    if cargo_bin is None:
        raise ValueError("cargo is required to compile miro-ego when --miro-ego-bin is omitted")
    env = dict(os.environ)
    env["MIRO_EGO_GIT_COMMIT"] = commit
    env.pop("MIRO_EGO_RELEASE_SIGNING_SEED_HEX", None)
    if public_key_hex:
        env["MIRO_EGO_RELEASE_PUBLIC_KEY_HEX"] = public_key_hex

    if shutil.which("cargo-zigbuild") is not None:
        cmd = [
            cargo_bin,
            "zigbuild",
            "--locked",
            "--release",
            "--target",
            AARCH64_ZIG_RUST_TARGET,
            "-p",
            "miro-ego-cli",
        ]
    else:
        cmd = [
            cargo_bin,
            "build",
            "--locked",
            "--release",
            "--target",
            AARCH64_RUST_TARGET,
            "-p",
            "miro-ego-cli",
        ]
    subprocess.run(cmd, cwd=workspace_root, env=env, check=True)
    built = workspace_root / "target" / AARCH64_RUST_TARGET / "release" / "miro-ego"
    if not built.is_file():
        raise ValueError(f"Expected compiled miro-ego binary at {built}")
    return built


def compile_encoder_binary(
    output_bin: Path,
    *,
    hb_sdk_root: Path | None = None,
    allow_mock_encoder: bool = False,
    workspace_root: Path = ROOT,
) -> Path:
    """Compile AArch64 ``miro-ego-encoder``, requiring real Hobot SDK unless explicitly mocked."""
    sdk_paths = resolve_hobot_sdk_paths(hb_sdk_root)
    if sdk_paths is None and not allow_mock_encoder:
        raise ValueError(
            "Refusing to compile miro-ego-encoder without Hobot SDK: set HB_SDK_ROOT "
            "(or HOBOT_SDK_INCLUDE + HOBOT_SDK_LIB), pass prebuilt --encoder-bin, "
            "or pass --allow-mock-encoder for CI smoke builds"
        )

    csrc_dir = workspace_root / "csrc" / "miro-ego-encoder"
    sources = [str(csrc_dir / name) for name in ENCODER_C_SOURCES]

    if zig_bin := shutil.which("zig"):
        cc_prefix = [zig_bin, "cc", "-target", AARCH64_ZIG_CC_TARGET]
    elif aarch64_gcc := shutil.which("aarch64-linux-gnu-gcc"):
        cc_prefix = [aarch64_gcc]
    else:
        raise ValueError(
            "zig or aarch64-linux-gnu-gcc is required to cross-compile miro-ego-encoder"
        )

    output_bin.parent.mkdir(parents=True, exist_ok=True)
    common_flags = ["-O2", "-Wall", "-Wextra", "-Werror", "-std=gnu11"]
    if sdk_paths is not None:
        inc_dir, lib_dir = sdk_paths
        cmd = [
            *cc_prefix,
            *common_flags,
            f"-I{inc_dir}",
            f"-I{csrc_dir}",
            *sources,
            f"-L{lib_dir}",
            "-Wl,-rpath,/usr/hobot/lib",
            f"-Wl,-rpath-link,{lib_dir}",
            "-lmultimedia",
            "-lpthread",
            "-o",
            str(output_bin),
        ]
    else:
        stubs_dir = csrc_dir / "tests" / "stubs"
        tests_dir = csrc_dir / "tests"
        mock_src = tests_dir / "mock_hobot_sdk.c"
        cmd = [
            *cc_prefix,
            *common_flags,
            f"-I{stubs_dir}",
            f"-I{csrc_dir}",
            f"-I{tests_dir}",
            *sources,
            str(mock_src),
            "-lpthread",
            "-o",
            str(output_bin),
        ]

    subprocess.run(cmd, cwd=workspace_root, check=True)
    return output_bin


def _is_executable_asset(path: Path) -> bool:
    with path.open("rb") as src:
        return src.read(2) == b"#!"


def _write_deterministic_archive(bundle_dir: Path, archive_path: Path) -> None:
    archive_path = archive_path.resolve()
    try:
        archive_path.relative_to(bundle_dir.resolve())
    except ValueError:
        pass
    else:
        raise ValueError("Archive output path must live outside the bundle directory")

    archive_path.parent.mkdir(parents=True, exist_ok=True)
    lower = archive_path.name.lower()
    mode = "w:gz" if lower.endswith((".tar.gz", ".tgz")) else "w"
    files = sorted(
        (p for p in bundle_dir.rglob("*") if p.is_file()),
        key=lambda p: p.relative_to(bundle_dir).as_posix(),
    )

    tmp_fd, tmp_name = tempfile.mkstemp(
        prefix=f".{archive_path.name}.", suffix=".tmp", dir=archive_path.parent
    )
    os.close(tmp_fd)
    tmp_path = Path(tmp_name)
    try:
        with tarfile.open(tmp_path, mode) as tar:
            for item in files:
                if item.is_symlink():
                    raise ValueError(f"Symlink forbidden in bundle archive: {item}")
                rel = item.relative_to(bundle_dir).as_posix()
                validate_relative_path(rel)
                info = tarfile.TarInfo(rel)
                info.size = item.stat().st_size
                info.mtime = 0
                info.uid = 0
                info.gid = 0
                info.uname = ""
                info.gname = ""
                is_exec = rel.startswith("bin/") or (
                    rel.startswith("deploy/miro-ego/") and _is_executable_asset(item)
                )
                info.mode = 0o755 if is_exec else 0o644
                with item.open("rb") as src:
                    tar.addfile(info, src)
        os.replace(tmp_path, archive_path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


def build_bundle(
    *,
    miro_ego_bin: Path,
    encoder_bin: Path,
    output: Path,
    version: str,
    commit: str,
    deploy_dir: Path = ROOT / "deploy" / "miro-ego",
    voice_dir: Path = ROOT / "share" / "voice",
    archive: Path | None = None,
    signing_seed_hex: str | None = None,
    allow_mock_encoder: bool = False,
) -> dict[str, Any]:
    """Assemble a verified pure-native Miro-Ego RDK X5 bundle directory (and optional tar)."""
    _validate_version(version)
    commit = _resolve_commit(commit)
    signing_seed = resolve_signing_seed(signing_seed_hex)

    resolved_miro = miro_ego_bin.resolve()
    resolved_encoder = encoder_bin.resolve()
    if resolved_encoder.is_symlink() or not resolved_encoder.is_file():
        raise ValueError(f"Expected a regular non-symlink file: {resolved_encoder}")
    if not allow_mock_encoder and _file_contains_bytes(resolved_encoder, MOCK_ENCODER_MARKER):
        raise ValueError(
            "Refusing to package mock-SDK miro-ego-encoder into a hardware bundle "
            "(pass --allow-mock-encoder only for non-production CI smoke bundles)"
        )

    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise ValueError(f"Output directory must be empty to avoid stale artifacts: {output}")

    deploy_dir = deploy_dir.resolve()
    if deploy_dir.is_symlink() or not deploy_dir.is_dir():
        raise ValueError(f"Deploy assets directory does not exist: {deploy_dir}")

    voice_dir = voice_dir.resolve()
    if voice_dir.is_symlink() or not voice_dir.is_dir():
        raise ValueError(f"Voice assets directory does not exist: {voice_dir}")

    binary_sources = {
        "bin/miro-ego": resolved_miro,
        "bin/miro-ego-encoder": resolved_encoder,
    }

    binaries: list[dict[str, Any]] = []
    assets: list[dict[str, Any]] = []
    sums_entries: list[tuple[str, str]] = []
    total_bytes = 0

    (output / "bin").mkdir(parents=True, exist_ok=True)
    for rel in REQUIRED_BINARIES:
        src = binary_sources[rel]
        size, digest, header = sha256_file(src)
        if size == 0 or size > MAX_BINARY_BYTES:
            raise ValueError(f"{rel} byte length {size} is out of bounds")
        validate_aarch64_elf64(header, rel)
        validate_relative_path(rel)

        dst = output / rel
        shutil.copyfile(src, dst)
        dst.chmod(0o755)
        binaries.append(
            {
                "path": rel,
                "bytes": size,
                "sha256": digest,
                "executable": True,
            }
        )
        sums_entries.append((rel, digest))
        total_bytes += size

    deploy_out = output / "deploy" / "miro-ego"
    deploy_out.mkdir(parents=True, exist_ok=True)
    deploy_files = sorted(deploy_dir.iterdir(), key=lambda p: p.name)
    seen_deploy_names: set[str] = set()
    for src in deploy_files:
        if src.is_symlink() or not src.is_file():
            raise ValueError(f"Deploy asset must be a regular non-symlink file: {src}")
        rel = f"deploy/miro-ego/{src.name}"
        validate_relative_path(rel)
        size, digest, _ = sha256_file(src)
        if size == 0 or size > MAX_BINARY_BYTES:
            raise ValueError(f"{rel} byte length {size} is out of bounds")
        executable = _is_executable_asset(src)
        dst = output / rel
        shutil.copyfile(src, dst)
        dst.chmod(0o755 if executable else 0o644)
        assets.append(
            {
                "path": rel,
                "bytes": size,
                "sha256": digest,
                "executable": executable,
            }
        )
        sums_entries.append((rel, digest))
        seen_deploy_names.add(src.name)
        total_bytes += size

    for required_asset in REQUIRED_DEPLOY_ASSETS:
        if required_asset not in seen_deploy_names:
            raise ValueError(f"Missing required deploy asset: deploy/miro-ego/{required_asset}")

    voice_out = output / "share" / "voice"
    voice_out.mkdir(parents=True, exist_ok=True)
    voice_files = sorted(voice_dir.iterdir(), key=lambda p: p.name)
    seen_voice_names: set[str] = set()
    for src in voice_files:
        if src.is_symlink() or not src.is_file():
            raise ValueError(f"Voice asset must be a regular non-symlink file: {src}")
        if src.suffix != ".wav" or not VOICE_STEM_RE.fullmatch(src.stem):
            raise ValueError(f"Voice asset must be a .wav file with [A-Za-z0-9_-] stem: {src}")
        rel = f"share/voice/{src.name}"
        validate_relative_path(rel)
        size, digest, _ = sha256_file(src)
        if size == 0 or size > MAX_VOICE_WAV_BYTES:
            raise ValueError(f"{rel} byte length {size} is out of bounds")
        validate_pcm16_wav(src.read_bytes(), rel)
        dst = output / rel
        shutil.copyfile(src, dst)
        dst.chmod(0o644)
        assets.append(
            {
                "path": rel,
                "bytes": size,
                "sha256": digest,
                "executable": False,
            }
        )
        sums_entries.append((rel, digest))
        seen_voice_names.add(src.name)
        total_bytes += size

    for required_voice in REQUIRED_VOICE_ASSETS:
        if required_voice not in seen_voice_names:
            raise ValueError(f"Missing required voice asset: share/voice/{required_voice}")

    assets.sort(key=lambda entry: entry["path"])

    total_entries = len(binaries) + len(assets)
    if total_entries > MAX_BUNDLE_FILES:
        raise ValueError(f"Bundle entry count {total_entries} exceeds cap {MAX_BUNDLE_FILES}")
    if total_bytes > MAX_TOTAL_BUNDLE_BYTES:
        raise ValueError(f"Bundle total bytes {total_bytes} exceeds cap {MAX_TOTAL_BUNDLE_BYTES}")

    manifest: dict[str, Any] = {
        "schema": BUNDLE_SCHEMA,
        "platform": TARGET_PLATFORM,
        "version": version,
        "commit": commit,
        "binaries": binaries,
        "assets": assets,
    }
    manifest_bytes = (json.dumps(manifest, indent=2) + "\n").encode("utf-8")
    manifest_path = output / "bundle.json"
    manifest_path.write_bytes(manifest_bytes)
    manifest_path.chmod(0o644)
    sums_entries.append(("bundle.json", hashlib.sha256(manifest_bytes).hexdigest()))

    signature_hex, public_key_hex = sign_manifest_bytes(manifest_bytes, signing_seed)
    sig_path = output / "bundle.json.sig"
    sig_path.write_text(f"{signature_hex}\n", encoding="ascii")
    sig_path.chmod(0o644)

    sums_lines = [f"{digest}  {rel}\n" for rel, digest in sorted(sums_entries, key=lambda x: x[0])]
    sums_path = output / "SHA256SUMS"
    sums_path.write_text("".join(sums_lines), encoding="utf-8")
    sums_path.chmod(0o644)

    if archive is not None:
        _write_deterministic_archive(output, archive)

    return {
        "schema": BUNDLE_SCHEMA,
        "platform": TARGET_PLATFORM,
        "version": version,
        "commit": commit,
        "public_key_hex": public_key_hex,
        "binaries": [entry["path"] for entry in binaries],
        "asset_count": len(assets),
        "total_bytes": total_bytes,
        "output": str(output),
        "archive": str(archive.resolve()) if archive is not None else None,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build pure-native Miro-Ego RDK X5 bundle (miro-ego.rdk-x5-bundle.v1)"
    )
    parser.add_argument(
        "--miro-ego-bin",
        type=Path,
        help=(
            "Path to prebuilt AArch64 ELF64 miro-ego binary "
            "(compiles with cargo --locked if omitted)"
        ),
    )
    parser.add_argument(
        "--encoder-bin",
        type=Path,
        help=(
            "Path to prebuilt AArch64 ELF64 miro-ego-encoder binary "
            "(compiles from csrc/miro-ego-encoder against HB_SDK_ROOT if omitted)"
        ),
    )
    parser.add_argument(
        "--hb-sdk-root",
        type=Path,
        help=(
            "Optional Hobot SDK sysroot containing include/ and lib/libmultimedia.so "
            "(or HB_SDK_ROOT)"
        ),
    )
    parser.add_argument(
        "--allow-mock-encoder",
        action="store_true",
        help=(
            "Explicitly permit building or packaging a non-production mock_hobot_sdk.c "
            "miro-ego-encoder binary for CI smoke verification"
        ),
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Empty destination directory for the native bundle",
    )
    parser.add_argument(
        "--archive",
        type=Path,
        help="Optional deterministic .tar or .tar.gz archive path outside --output",
    )
    parser.add_argument(
        "--deploy-dir",
        type=Path,
        default=ROOT / "deploy" / "miro-ego",
        help="Source directory containing Miro-Ego deployment assets",
    )
    parser.add_argument(
        "--voice-dir",
        type=Path,
        default=ROOT / "share" / "voice",
        help="Source directory containing Miro-Ego PCM16 WAV voice cue assets",
    )
    parser.add_argument(
        "--version",
        help="Bundle version string (defaults to scripts/release_version.py)",
    )
    parser.add_argument(
        "--commit",
        help="40-char lowercase Git commit SHA (defaults to git rev-parse HEAD)",
    )
    parser.add_argument(
        "--signing-seed-hex",
        help=(
            "Optional 64-char hex Ed25519 private signing seed "
            "(defaults to MIRO_EGO_RELEASE_SIGNING_SEED_HEX or built-in release seed)"
        ),
    )
    args = parser.parse_args(argv)

    version = args.version if args.version is not None else project_version()
    commit = _resolve_commit(args.commit)
    seed = resolve_signing_seed(args.signing_seed_hex)
    _, pub_hex = sign_manifest_bytes(b"", seed)

    with tempfile.TemporaryDirectory(prefix="miro-ego-bundle-compile-") as tmp_build:
        miro_bin = args.miro_ego_bin
        if miro_bin is None:
            miro_bin = compile_miro_ego_binary(commit=commit, public_key_hex=pub_hex)

        encoder_bin = args.encoder_bin
        if encoder_bin is None:
            encoder_bin = compile_encoder_binary(
                Path(tmp_build) / "miro-ego-encoder",
                hb_sdk_root=args.hb_sdk_root,
                allow_mock_encoder=args.allow_mock_encoder,
            )

        summary = build_bundle(
            miro_ego_bin=miro_bin,
            encoder_bin=encoder_bin,
            output=args.output,
            version=version,
            commit=commit,
            deploy_dir=args.deploy_dir,
            voice_dir=args.voice_dir,
            archive=args.archive,
            signing_seed_hex=args.signing_seed_hex,
            allow_mock_encoder=args.allow_mock_encoder,
        )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
