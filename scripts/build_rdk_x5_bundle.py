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
TARGET_PLATFORM = "linux_aarch64_rdk_x5_v1"
REQUIRED_BINARIES = ("bin/miro-ego", "bin/miro-ego-encoder")
REQUIRED_DEPLOY_ASSETS = ("miro-ego-data-volume", "miro-ego-data-volume.service")
MAX_BUNDLE_FILES = 256
MAX_BINARY_BYTES = 256 * 1024 * 1024
MAX_TOTAL_BUNDLE_BYTES = 512 * 1024 * 1024

ELF_MAGIC = b"\x7fELF"
ELFCLASS64 = 2
ELFDATA2LSB = 1
ET_EXEC = 2
ET_DYN = 3
EM_AARCH64 = 183
COMMIT_RE = re.compile(r"^[0-9a-f]{40}\Z")


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
    archive: Path | None = None,
) -> dict[str, Any]:
    """Assemble a verified pure-native Miro-Ego RDK X5 bundle directory (and optional tar)."""
    _validate_version(version)
    commit = _resolve_commit(commit)

    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise ValueError(f"Output directory must be empty to avoid stale artifacts: {output}")

    deploy_dir = deploy_dir.resolve()
    if deploy_dir.is_symlink() or not deploy_dir.is_dir():
        raise ValueError(f"Deploy assets directory does not exist: {deploy_dir}")

    binary_sources = {
        "bin/miro-ego": miro_ego_bin.resolve(),
        "bin/miro-ego-encoder": encoder_bin.resolve(),
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
        required=True,
        type=Path,
        help="Path to AArch64 ELF64 miro-ego binary",
    )
    parser.add_argument(
        "--encoder-bin",
        required=True,
        type=Path,
        help="Path to AArch64 ELF64 miro-ego-encoder binary",
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
        "--version",
        help="Bundle version string (defaults to scripts/release_version.py)",
    )
    parser.add_argument(
        "--commit",
        help="40-char lowercase Git commit SHA (defaults to git rev-parse HEAD)",
    )
    args = parser.parse_args(argv)

    version = args.version if args.version is not None else project_version()
    commit = _resolve_commit(args.commit)
    summary = build_bundle(
        miro_ego_bin=args.miro_ego_bin,
        encoder_bin=args.encoder_bin,
        output=args.output,
        version=version,
        commit=commit,
        deploy_dir=args.deploy_dir,
        archive=args.archive,
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
