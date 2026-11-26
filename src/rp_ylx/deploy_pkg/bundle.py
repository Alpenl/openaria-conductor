"""Bundle validation, wheel/runtime extraction, and stage assembly."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tarfile
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass
from email.parser import BytesParser
from pathlib import Path, PurePosixPath

from .schema import (
    BUNDLE_SCHEMA,
    MAX_BUNDLE_WHEELS,
    MAX_RUNTIME_FILES,
    MAX_UNPACKED_BYTES,
    MAX_WHEEL_BYTES,
    TARGET_PLATFORM,
    DeploymentError,
    _canonical_commit,
    _sha256,
)


def _validate_wheel_tags(name: str) -> None:
    try:
        _, python_tag, abi_tag, platform_tag = name.removesuffix(".whl").rsplit("-", 3)
    except ValueError as error:
        raise DeploymentError("bundle_invalid", f"wheel 文件名无效：{name}") from error
    python_tags = set(python_tag.split("."))
    abi_tags = set(abi_tag.split("."))
    platforms = set(platform_tag.split("."))
    pure = python_tags <= {"py3", "py311"} and abi_tags == {"none"} and platforms == {"any"}
    native = (
        python_tags == {"cp311"}
        and abi_tags <= {"cp311", "abi3", "none"}
        and all(
            candidate.startswith("manylinux") and candidate.endswith("_aarch64")
            for candidate in platforms
        )
    )
    if not pure and not native:
        raise DeploymentError(
            "bundle_platform_mismatch", f"wheel 不兼容 CPython 3.11/RDK X5：{name}"
        )


@dataclass(frozen=True, slots=True)
class Bundle:
    """Verified offline release bundle descriptor."""

    root: Path
    commit: str
    version: str
    wheels: tuple[Mapping[str, object], ...]
    application_wheel: str
    installer: Mapping[str, object]
    runtime: Mapping[str, object]


def _wheel_identity(wheel: Path) -> tuple[str, str]:
    try:
        with zipfile.ZipFile(wheel) as archive:
            if archive.namelist().count("rp_ylx/_build_info.py") != 1:
                raise DeploymentError("bundle_invalid", "application wheel 缺少唯一构建身份")
            content = archive.read("rp_ylx/_build_info.py").decode()
            metadata_names = [
                name
                for name in archive.namelist()
                if name.endswith(".dist-info/METADATA")
                and "/" not in name.removesuffix("/METADATA")
            ]
            if len(metadata_names) != 1:
                raise DeploymentError("bundle_invalid", "application wheel 缺少唯一 METADATA")
            metadata = BytesParser().parsebytes(archive.read(metadata_names[0]))
            native_names = [name for name in archive.namelist() if name == "rp_ylx/_native.abi3.so"]
            if len(native_names) != 1:
                raise DeploymentError(
                    "bundle_invalid", "application wheel 缺少唯一 rp_ylx/_native.abi3.so"
                )
            native = archive.read(native_names[0])
    except (OSError, UnicodeError, zipfile.BadZipFile) as error:
        raise DeploymentError("bundle_invalid", f"无法读取 application wheel：{error}") from error
    if (
        len(native) < 20
        or native[:4] != b"\x7fELF"
        or native[4:6] != b"\x02\x01"
        or int.from_bytes(native[18:20], "little") != 183
    ):
        raise DeploymentError(
            "bundle_platform_mismatch", "application wheel 原生扩展不是 AArch64 ELF64"
        )
    marker = '__commit__ = "'
    lines = [
        line for line in content.splitlines() if line.startswith(marker) and line.endswith('"')
    ]
    if len(lines) != 1:
        raise DeploymentError("bundle_invalid", "application wheel 构建身份无效")
    name = metadata.get("Name", "").casefold().replace("_", "-")
    version = metadata.get("Version", "")
    if name != "rp-ylx" or not version or "\n" in version or "\r" in version:
        raise DeploymentError("bundle_invalid", "application wheel METADATA 身份无效")
    return _canonical_commit(lines[0][len(marker) : -1]), version


def _wheel_commit(wheel: Path) -> str:
    return _wheel_identity(wheel)[0]


def load_bundle(path: str | Path) -> Bundle:
    root = Path(path).resolve()
    try:
        payload = (root / "bundle.json").read_bytes()
        if len(payload) > 128 * 1024:
            raise DeploymentError("bundle_invalid", "bundle manifest 过大")
        value = json.loads(payload)
    except (OSError, json.JSONDecodeError) as error:
        raise DeploymentError("bundle_invalid", f"无法读取 bundle manifest：{error}") from error
    required = {
        "schema",
        "platform",
        "commit",
        "version",
        "application_wheel",
        "installer",
        "runtime",
        "wheels",
    }
    if (
        not isinstance(value, dict)
        or set(value) != required
        or value["schema"] != BUNDLE_SCHEMA
        or value["platform"] != TARGET_PLATFORM
        or not isinstance(value["version"], str)
        or not value["version"]
        or not isinstance(value["application_wheel"], str)
        or not isinstance(value["installer"], dict)
        or not isinstance(value["runtime"], dict)
        or not isinstance(value["wheels"], list)
        or not value["wheels"]
        or len(value["wheels"]) > MAX_BUNDLE_WHEELS
    ):
        raise DeploymentError("bundle_invalid", "bundle manifest 字段无效")
    commit = _canonical_commit(value["commit"])
    seen: set[str] = set()
    wheels: list[Mapping[str, object]] = []
    for item in value["wheels"]:
        if not isinstance(item, dict) or set(item) != {"file", "bytes", "sha256"}:
            raise DeploymentError("bundle_invalid", "wheel descriptor 无效")
        name = item["file"]
        size = item["bytes"]
        digest = item["sha256"]
        if (
            not isinstance(name, str)
            or PurePosixPath(name).name != name
            or not name.endswith(".whl")
            or name in seen
            or type(size) is not int
            or size <= 0
            or size > MAX_WHEEL_BYTES
            or not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise DeploymentError("bundle_invalid", "wheel descriptor 值无效")
        _validate_wheel_tags(name)
        wheel = root / name
        if (
            wheel.is_symlink()
            or not wheel.is_file()
            or wheel.stat().st_size != size
            or _sha256(wheel) != digest
        ):
            raise DeploymentError("bundle_digest_mismatch", f"wheel 校验失败：{name}")
        seen.add(name)
        wheels.append(item)
    if value["application_wheel"] not in seen:
        raise DeploymentError("bundle_invalid", "application wheel 不在 bundle 中")
    application_wheels = [name for name in seen if name.casefold().startswith("rp_ylx-")]
    if len(application_wheels) != 1 or application_wheels[0] != value["application_wheel"]:
        raise DeploymentError("bundle_invalid", "bundle 必须包含唯一 Open Aria application wheel")
    wheel_commit, wheel_version = _wheel_identity(root / value["application_wheel"])
    if wheel_commit != commit or wheel_version != value["version"]:
        raise DeploymentError("bundle_invalid", "bundle commit 与 application wheel 不一致")
    installer = value["installer"]
    if set(installer) != {"file", "bytes", "sha256"}:
        raise DeploymentError("bundle_invalid", "installer descriptor 无效")
    installer_name = installer.get("file")
    installer_size = installer.get("bytes")
    installer_digest = installer.get("sha256")
    installer_path = root / str(installer_name)
    if (
        installer_name != "rdk_x5_install.py"
        or type(installer_size) is not int
        or installer_size <= 0
        or not isinstance(installer_digest, str)
        or len(installer_digest) != 64
        or any(character not in "0123456789abcdef" for character in installer_digest)
        or installer_path.is_symlink()
        or not installer_path.is_file()
        or installer_path.stat().st_size != installer_size
        or _sha256(installer_path) != installer_digest
    ):
        raise DeploymentError("bundle_digest_mismatch", "首次安装脚本校验失败")
    runtime = value["runtime"]
    runtime_fields = {
        "file",
        "bytes",
        "sha256",
        "implementation",
        "python_version",
        "platform",
        "executable",
    }
    if set(runtime) != runtime_fields:
        raise DeploymentError("bundle_invalid", "package-owned runtime descriptor 无效")
    runtime_name = runtime.get("file")
    runtime_size = runtime.get("bytes")
    runtime_digest = runtime.get("sha256")
    runtime_path = root / str(runtime_name)
    if (
        not isinstance(runtime_name, str)
        or PurePosixPath(runtime_name).name != runtime_name
        or not runtime_name.endswith(".tar.gz")
        or type(runtime_size) is not int
        or runtime_size <= 0
        or runtime_size > MAX_WHEEL_BYTES
        or not isinstance(runtime_digest, str)
        or len(runtime_digest) != 64
        or any(character not in "0123456789abcdef" for character in runtime_digest)
        or runtime.get("implementation") != "cpython"
        or not str(runtime.get("python_version", "")).startswith("3.11.")
        or runtime.get("platform") != "linux_aarch64"
        or runtime.get("executable") != "runtime/bin/python3"
        or runtime_path.is_symlink()
        or not runtime_path.is_file()
        or runtime_path.stat().st_size != runtime_size
        or _sha256(runtime_path) != runtime_digest
    ):
        raise DeploymentError("bundle_digest_mismatch", "package-owned runtime 校验失败")
    return Bundle(
        root,
        commit,
        value["version"],
        tuple(wheels),
        value["application_wheel"],
        installer,
        runtime,
    )


def _ensure_stage_dirs(directory: Path, root: Path) -> None:
    pending: list[Path] = []
    current = directory
    while current != root and not current.exists():
        pending.append(current)
        current = current.parent
    for entry in reversed(pending):
        entry.mkdir(exist_ok=True)
        entry.chmod(0o755)


def _extract_wheel(wheel: Path, target: Path, *, max_bytes: int) -> int:
    try:
        with zipfile.ZipFile(wheel) as archive:
            seen: set[PurePosixPath] = set()
            unpacked_bytes = 0
            for member in archive.infolist():
                path = PurePosixPath(member.filename)
                unpacked_bytes += member.file_size
                if (
                    path.is_absolute()
                    or not path.parts
                    or any(part in {"", ".", ".."} for part in path.parts)
                    or path in seen
                    or (member.external_attr >> 16) & 0o170000 == 0o120000
                    or any(part.endswith(".data") for part in path.parts)
                    or unpacked_bytes > max_bytes
                ):
                    raise DeploymentError("wheel_unsafe", f"wheel 成员不安全：{member.filename}")
                seen.add(path)
                destination = target.joinpath(*path.parts)
                if member.is_dir():
                    _ensure_stage_dirs(destination, target)
                    destination.chmod(0o755)
                    continue
                _ensure_stage_dirs(destination.parent, target)
                with archive.open(member) as source, destination.open("xb") as output:
                    shutil.copyfileobj(source, output)
                is_exec = bool((member.external_attr >> 16) & 0o111)
                destination.chmod(0o755 if is_exec else 0o644)
            return unpacked_bytes
    except zipfile.BadZipFile as error:
        raise DeploymentError("bundle_invalid", f"wheel 不是有效 ZIP：{wheel.name}") from error


def _validated_runtime_members(archive: tarfile.TarFile) -> list[tarfile.TarInfo]:
    members = archive.getmembers()
    if len(members) > MAX_RUNTIME_FILES:
        raise DeploymentError("runtime_unsafe", "runtime 文件数量超过限制")
    unpacked_bytes = 0
    names: set[str] = set()
    executable_found = False
    for member in members:
        path = PurePosixPath(member.name)
        raw_parts = member.name.split("/")
        unpacked_bytes += member.size
        if (
            path.is_absolute()
            or not path.parts
            or path.parts[0] != "runtime"
            or any(part in {"", ".", ".."} for part in raw_parts)
            or member.name in names
            or member.issym()
            or member.islnk()
            or not (member.isdir() or member.isfile())
            or unpacked_bytes > MAX_UNPACKED_BYTES
        ):
            raise DeploymentError("runtime_unsafe", f"runtime 成员不安全：{member.name}")
        names.add(member.name)
        if member.name == "runtime/bin/python3" and member.isfile() and member.size > 0:
            executable_found = True
    if not executable_found:
        raise DeploymentError("runtime_dependency_missing", "runtime 缺少 bin/python3")
    return members


def _inspect_runtime(archive_path: Path) -> None:
    try:
        with tarfile.open(archive_path, "r:gz") as archive:
            _validated_runtime_members(archive)
    except (OSError, tarfile.TarError) as error:
        raise DeploymentError(
            "bundle_invalid", f"无法读取 package-owned runtime：{error}"
        ) from error


def _extract_runtime(archive_path: Path, target: Path) -> None:
    try:
        with tarfile.open(archive_path, "r:gz") as archive:
            members = _validated_runtime_members(archive)
            archive.extractall(target, members=members, filter="data")
    except (OSError, tarfile.TarError) as error:
        raise DeploymentError(
            "bundle_invalid", f"无法解包 package-owned runtime：{error}"
        ) from error
    runtime_root = target / "runtime"
    if runtime_root.is_dir():
        runtime_root.chmod(0o755)
    for dirpath, dirnames, _ in os.walk(runtime_root):
        Path(dirpath).chmod(0o755)
        for dirname in dirnames:
            (Path(dirpath) / dirname).chmod(0o755)
    for member in members:
        if member.isfile():
            destination = target.joinpath(*PurePosixPath(member.name).parts)
            is_exec = bool(member.mode & 0o111)
            destination.chmod(0o755 if is_exec else 0o644)
    executable = target / "runtime/bin/python3"
    executable.chmod(0o755)


def _launcher(module: str) -> bytes:
    return f"""#!/bin/sh
set -eu
RELEASE_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export PYTHONPATH="$RELEASE_ROOT/site-packages"
exec "$RELEASE_ROOT/runtime/bin/python3" -m {module} "$@"
""".encode()


def _install_stage(bundle: Bundle, stage: Path) -> None:
    site_packages = stage / "site-packages"
    bin_directory = stage / "bin"
    site_packages.mkdir(parents=True)
    site_packages.chmod(0o755)
    bin_directory.mkdir()
    bin_directory.chmod(0o755)
    _extract_runtime(bundle.root / str(bundle.runtime["file"]), stage)
    unpacked_bytes = 0
    for descriptor in bundle.wheels:
        unpacked_bytes += _extract_wheel(
            bundle.root / str(descriptor["file"]),
            site_packages,
            max_bytes=MAX_UNPACKED_BYTES - unpacked_bytes,
        )
    launchers = (
        ("rp-ylx", "rp_ylx"),
        ("rp-ylx-deploy", "rp_ylx.deployment"),
        ("rp-ylx-spectacular-check", "rp_ylx.spectacular.check_cli"),
        ("rp-ylx-recording-button", "rp_ylx.recording_button"),
    )
    for name, module in launchers:
        launcher = bin_directory / name
        launcher.write_bytes(_launcher(module))
        launcher.chmod(0o755)


def _build_stereo_encoder(stage: Path) -> None:
    """编译边录边出左右眼 H.264 的助手。

    助手链接板上的 hobot-multimedia，只能在目标机上编译；没有它就没有生产成片，
    因此编译失败必须让安装失败，而不是留下一个不能录制的发行版。
    """
    source = stage / "site-packages/rp_ylx/hobot"
    bin_directory = stage / "bin"
    if not (source / "Makefile").is_file():
        raise DeploymentError("bundle_invalid", "wheel 缺少 ylx-stereo-encoder 源码")
    try:
        subprocess.run(  # noqa: S603 - 固定参数，无 shell
            ["make", "--silent", "install", f"DESTDIR={bin_directory.resolve()}"],
            cwd=source,
            check=True,
            capture_output=True,
            timeout=300,
        )
    except (OSError, subprocess.SubprocessError) as error:
        detail = getattr(error, "stderr", b"") or b""
        raise DeploymentError(
            "runtime_dependency_missing",
            f"无法编译 ylx-stereo-encoder：{error}；{detail[:512].decode(errors='replace')}",
        ) from error
    built = bin_directory / "ylx-stereo-encoder"
    if not built.is_file():
        raise DeploymentError("runtime_dependency_missing", "ylx-stereo-encoder 未生成")
    built.chmod(0o755)
