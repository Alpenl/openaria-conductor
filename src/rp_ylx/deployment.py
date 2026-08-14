"""RDK X5 单机安装、升级恢复与两版本回滚。"""

from __future__ import annotations

import argparse
import ctypes.util
import hashlib
import json
import os
import platform
import posixpath
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.request
import uuid
import zipfile
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from email.parser import BytesParser
from importlib.resources import files
from pathlib import Path, PurePosixPath

BUNDLE_SCHEMA = "ylx.rdk-x5-bundle.v1"
TRANSACTION_SCHEMA = "ylx.install-transaction.v1"
RELEASE_SCHEMA = "ylx.installed-release.v1"
TARGET_PLATFORM = "linux_aarch64_rdk_x5_v1"
PRODUCTION_CONFIG_SCHEMA = "ylx.production-config.v1"
RDK_X5_MODEL = "D-Robotics RDK X5 V1.0"
MAX_BUNDLE_WHEELS = 128
MAX_WHEEL_BYTES = 512 * 1024 * 1024
MAX_UNPACKED_BYTES = 4 * 1024 * 1024 * 1024
MAX_RUNTIME_FILES = 100_000
ACTIVATION_HEALTH_TIMEOUT_SECONDS = 300.0


class DeploymentError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_tree(root: Path) -> None:
    directories: list[Path] = []
    for directory, names, files_in_directory in os.walk(root):
        current = Path(directory)
        directories.append(current)
        for name in names:
            candidate = current / name
            if candidate.is_symlink():
                raise DeploymentError("release_unsafe", "release 目录不能包含符号链接")
        for name in files_in_directory:
            candidate = current / name
            if candidate.is_symlink() or not candidate.is_file():
                raise DeploymentError("release_unsafe", "release 只能包含普通文件")
            with candidate.open("rb") as stream:
                os.fsync(stream.fileno())
    for directory in reversed(directories):
        _fsync_directory(directory)


def _write_atomic(path: Path, value: object) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as stream:
            payload = _json_bytes(value)
            if stream.write(payload) != len(payload):
                raise OSError(f"{path} 发生短写")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        with suppress(OSError):
            temporary.unlink(missing_ok=True)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _canonical_commit(value: object) -> str:
    commit = str(value)
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise DeploymentError("bundle_invalid", "bundle commit 不是完整 Git SHA")
    return commit


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


def _default_device_identity(seed: bytes) -> Mapping[str, str]:
    device_id = str(uuid.uuid4())
    digest = hashlib.sha256(seed + device_id.encode()).hexdigest()
    return {
        "device_id": device_id,
        "device_label": f"YLX-{digest[:8].upper()}",
        "hardware_fingerprint": f"sha256:{digest}",
    }


@dataclass(frozen=True, slots=True)
class Bundle:
    root: Path
    commit: str
    version: str
    wheels: tuple[Mapping[str, object], ...]
    application_wheel: str
    installer: Mapping[str, object]
    runtime: Mapping[str, object]


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
        raise DeploymentError("bundle_invalid", "bundle 必须包含唯一 RP-YLX application wheel")
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
    except (OSError, UnicodeError, zipfile.BadZipFile) as error:
        raise DeploymentError("bundle_invalid", f"无法读取 application wheel：{error}") from error
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


def write_bundle_manifest(
    directory: str | Path,
    *,
    application_wheel: str,
    version: str,
) -> Mapping[str, object]:
    root = Path(directory).resolve()
    app = root / application_wheel
    commit, wheel_version = _wheel_identity(app)
    if wheel_version != version:
        raise DeploymentError("bundle_invalid", "指定版本与 application wheel 不一致")
    wheels = sorted(root.glob("*.whl"), key=lambda path: path.name)
    if app not in wheels:
        raise DeploymentError("bundle_invalid", "application wheel 不存在")
    installer = root / "rdk_x5_install.py"
    if not installer.is_file():
        raise DeploymentError("bundle_invalid", "缺少首次安装脚本")
    runtimes = sorted(root.glob("cpython-3.11.*-aarch64.tar.gz"))
    if len(runtimes) != 1:
        raise DeploymentError("bundle_invalid", "必须包含唯一 CPython 3.11 aarch64 runtime")
    runtime = runtimes[0]
    runtime_version = runtime.name.removeprefix("cpython-").removesuffix("-aarch64.tar.gz")
    _inspect_runtime(runtime)
    value: dict[str, object] = {
        "schema": BUNDLE_SCHEMA,
        "platform": TARGET_PLATFORM,
        "commit": commit,
        "version": version,
        "application_wheel": application_wheel,
        "installer": {
            "file": installer.name,
            "bytes": installer.stat().st_size,
            "sha256": _sha256(installer),
        },
        "runtime": {
            "file": runtime.name,
            "bytes": runtime.stat().st_size,
            "sha256": _sha256(runtime),
            "implementation": "cpython",
            "python_version": runtime_version,
            "platform": "linux_aarch64",
            "executable": "runtime/bin/python3",
        },
        "wheels": [
            {"file": wheel.name, "bytes": wheel.stat().st_size, "sha256": _sha256(wheel)}
            for wheel in wheels
        ],
    }
    _write_atomic(root / "bundle.json", value)
    load_bundle(root)
    return value


Runner = Callable[[Sequence[str]], None]
StageInstaller = Callable[[Bundle, Path], None]


def _run(arguments: Sequence[str]) -> None:
    try:
        subprocess.run(list(arguments), check=True, timeout=300)
    except (OSError, subprocess.SubprocessError) as error:
        raise DeploymentError("command_failed", f"命令失败：{' '.join(arguments)}") from error


def _launcher(module: str) -> bytes:
    return f"""#!/bin/sh
set -eu
RELEASE_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export PYTHONPATH="$RELEASE_ROOT/site-packages"
exec "$RELEASE_ROOT/runtime/bin/python3" -m {module} "$@"
""".encode()


def _system_launcher() -> bytes:
    return b"""#!/bin/sh
set -eu
RUNTIME=/opt/rp-ylx/current/runtime/bin/python3
if [ ! -x "$RUNTIME" ]; then
    RUNTIME=/opt/rp-ylx/previous/runtime/bin/python3
    if [ ! -x "$RUNTIME" ]; then
        echo '{"code":"runtime_dependency_missing",'\
'"message":"package-owned runtime unavailable"}' >&2
        exit 2
    fi
fi
exec "$RUNTIME" /usr/local/lib/rp-ylx/deployment.py "$@"
"""


def _asset_bytes(name: str) -> bytes:
    standalone = Path(__file__).parent / name
    if standalone.is_file():
        return standalone.read_bytes()
    packaged = Path(__file__).parent / "deploy" / name
    if packaged.is_file():
        return packaged.read_bytes()
    try:
        return files("rp_ylx.deploy").joinpath(name).read_bytes()
    except (ModuleNotFoundError, OSError, TypeError):
        pass
    raise DeploymentError("install_resource_missing", f"缺少部署资源：{name}")


def _module_bytes() -> bytes:
    module = Path(__file__)
    if module.is_file():
        return module.read_bytes()
    loader = globals().get("__loader__")
    get_data = getattr(loader, "get_data", None)
    if callable(get_data):
        try:
            return get_data(__file__)
        except OSError:
            pass
    raise DeploymentError("install_resource_missing", "无法读取部署模块自身")


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
                    destination.mkdir(parents=True, exist_ok=True)
                    continue
                destination.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(member) as source, destination.open("xb") as output:
                    shutil.copyfileobj(source, output)
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


def normalize_runtime_archive(source_path: Path, output_path: Path) -> None:
    """Convert a verified standalone CPython archive into the no-link bundle format."""

    try:
        with tarfile.open(source_path, "r:gz") as source:
            source_members = source.getmembers()
            if not source_members or len(source_members) > MAX_RUNTIME_FILES:
                raise DeploymentError("runtime_unsafe", "runtime 文件数量无效")
            by_name: dict[str, tarfile.TarInfo] = {}
            roots: set[str] = set()
            unpacked_bytes = 0
            for member in source_members:
                path = PurePosixPath(member.name)
                raw_parts = member.name.split("/")
                unpacked_bytes += member.size
                if (
                    path.is_absolute()
                    or len(path.parts) < 2
                    or path.parts[0] not in {"python", "runtime"}
                    or any(part in {"", ".", ".."} for part in raw_parts)
                    or member.name in by_name
                    or member.islnk()
                    or not (member.isdir() or member.isfile() or member.issym())
                    or unpacked_bytes > MAX_UNPACKED_BYTES
                ):
                    raise DeploymentError("runtime_unsafe", f"runtime 源成员不安全：{member.name}")
                roots.add(path.parts[0])
                by_name[member.name] = member
            if len(roots) != 1:
                raise DeploymentError("runtime_unsafe", "runtime 源归档根目录不唯一")
            source_root = next(iter(roots))

            def resolve(member: tarfile.TarInfo) -> tarfile.TarInfo:
                seen: set[str] = set()
                current = member
                while current.issym():
                    if current.name in seen or PurePosixPath(current.linkname).is_absolute():
                        raise DeploymentError(
                            "runtime_unsafe", f"runtime 链接不安全：{member.name}"
                        )
                    seen.add(current.name)
                    target_name = posixpath.normpath(
                        posixpath.join(posixpath.dirname(current.name), current.linkname)
                    )
                    target = PurePosixPath(target_name)
                    if (
                        not target.parts
                        or target.parts[0] != source_root
                        or target_name not in by_name
                    ):
                        raise DeploymentError(
                            "runtime_unsafe", f"runtime 链接越界或缺失：{member.name}"
                        )
                    current = by_name[target_name]
                if not current.isfile():
                    raise DeploymentError(
                        "runtime_unsafe", f"runtime 链接未指向普通文件：{member.name}"
                    )
                return current

            temporary = output_path.with_name(f".{output_path.name}.{uuid.uuid4().hex}.tmp")
            try:
                with tarfile.open(temporary, "w:gz", format=tarfile.PAX_FORMAT) as output:
                    for member in source_members:
                        path = PurePosixPath(member.name)
                        normalized_name = str(PurePosixPath("runtime", *path.parts[1:]))
                        if member.isdir():
                            normalized = tarfile.TarInfo(normalized_name)
                            normalized.type = tarfile.DIRTYPE
                            normalized.mode = 0o755
                            output.addfile(normalized)
                            continue
                        resolved = resolve(member)
                        stream = source.extractfile(resolved)
                        if stream is None:
                            raise DeploymentError(
                                "runtime_unsafe", f"runtime 成员不可读：{member.name}"
                            )
                        normalized = tarfile.TarInfo(normalized_name)
                        normalized.size = resolved.size
                        normalized.mode = 0o755 if resolved.mode & 0o111 else 0o644
                        with stream:
                            output.addfile(normalized, stream)
                _inspect_runtime(temporary)
                os.replace(temporary, output_path)
                _fsync_directory(output_path.parent)
            finally:
                temporary.unlink(missing_ok=True)
    except (OSError, tarfile.TarError) as error:
        raise DeploymentError(
            "bundle_invalid", f"无法规范化 package-owned runtime：{error}"
        ) from error


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
    executable = target / "runtime/bin/python3"
    executable.chmod(executable.stat().st_mode | 0o755)


def _install_stage(bundle: Bundle, stage: Path) -> None:
    site_packages = stage / "site-packages"
    bin_directory = stage / "bin"
    site_packages.mkdir(parents=True)
    bin_directory.mkdir()
    _extract_runtime(bundle.root / str(bundle.runtime["file"]), stage)
    unpacked_bytes = 0
    for descriptor in bundle.wheels:
        unpacked_bytes += _extract_wheel(
            bundle.root / str(descriptor["file"]),
            site_packages,
            max_bytes=MAX_UNPACKED_BYTES - unpacked_bytes,
        )
    for name, module in (("rp-ylx", "rp_ylx"), ("rp-ylx-deploy", "rp_ylx.deployment")):
        launcher = bin_directory / name
        launcher.write_bytes(_launcher(module))
        launcher.chmod(0o755)


class ReleaseManager:
    def __init__(
        self,
        *,
        install_root: Path = Path("/opt/rp-ylx"),
        config_root: Path = Path("/etc/rp-ylx"),
        state_root: Path = Path("/var/lib/rp-ylx"),
        system_root: Path = Path("/"),
        target_machine: str | None = None,
        target_model: str | None = None,
        target_model_path: Path = Path("/proc/device-tree/model"),
        python_version: tuple[int, int] | None = None,
        library_finder: Callable[[str], str | None] = ctypes.util.find_library,
        executable_finder: Callable[[str], str | None] = shutil.which,
        runner: Runner = _run,
        stage_installer: StageInstaller = _install_stage,
        health_checker: Callable[[], None] | None = None,
    ) -> None:
        self.install_root = install_root
        self.config_root = config_root
        self.state_root = state_root
        self.system_root = system_root
        self.releases = install_root / "releases"
        self.transaction = state_root / "install-transaction.json"
        self.target_machine = (target_machine or platform.machine()).casefold()
        self.target_model = target_model
        self.target_model_path = target_model_path
        self.python_version = python_version or sys.version_info[:2]
        self.library_finder = library_finder
        self.executable_finder = executable_finder
        self.runner = runner
        self.stage_installer = stage_installer
        self.health_checker = health_checker or self._wait_for_health

    def _require_target(self) -> None:
        self._require_platform()
        if self.library_finder("turbojpeg") is None:
            raise DeploymentError("runtime_dependency_missing", "正式 60 FPS 路径要求 libturbojpeg")
        required_commands = (
            "systemctl",
            "systemd-sysusers",
            "systemd-tmpfiles",
        )
        missing = [
            command for command in required_commands if self.executable_finder(command) is None
        ]
        if missing:
            raise DeploymentError(
                "runtime_dependency_missing", f"缺少系统命令：{', '.join(missing)}"
            )

    def _require_platform(self) -> None:
        if self.target_machine not in {"aarch64", "arm64"}:
            raise DeploymentError("unsupported_platform", "安装包只支持 RDK X5 aarch64")
        model = self.target_model
        if model is None:
            try:
                model = self.target_model_path.read_text(encoding="utf-8", errors="replace")
            except OSError as error:
                raise DeploymentError(
                    "unsupported_platform", "无法确认 RDK X5 V1.0 板卡型号"
                ) from error
        normalized = " ".join(model.strip("\x00\n ").split()).casefold()
        if normalized != RDK_X5_MODEL.casefold():
            raise DeploymentError("unsupported_platform", "安装包只支持 D-Robotics RDK X5 V1.0")

    def _ensure_layout(self) -> None:
        self.releases.mkdir(parents=True, exist_ok=True, mode=0o755)
        self.config_root.mkdir(parents=True, exist_ok=True, mode=0o750)
        self.state_root.mkdir(parents=True, exist_ok=True, mode=0o750)

    def _link_target(self, name: str) -> str | None:
        link = self.install_root / name
        if not link.is_symlink():
            return None
        target = PurePosixPath(os.readlink(link))
        if len(target.parts) != 2 or target.parts[0] != "releases":
            raise DeploymentError("install_state_invalid", f"{name} 链接越界")
        return _canonical_commit(target.parts[1])

    def _switch(self, name: str, release: str | None) -> None:
        link = self.install_root / name
        temporary = self.install_root / f".{name}.{uuid.uuid4().hex}.tmp"
        if release is None:
            link.unlink(missing_ok=True)
            _fsync_directory(self.install_root)
            return
        os.symlink(f"releases/{release}", temporary)
        os.replace(temporary, link)
        _fsync_directory(self.install_root)

    def _transaction_document(
        self, action: str, old_current: str | None, new_current: str, state: str
    ) -> Mapping[str, object]:
        return {
            "schema": TRANSACTION_SCHEMA,
            "transaction_id": str(uuid.uuid4()),
            "action": action,
            "old_current": old_current,
            "new_current": new_current,
            "state": state,
        }

    def _read_transaction(self) -> Mapping[str, object] | None:
        if not self.transaction.exists():
            return None
        try:
            value = json.loads(self.transaction.read_bytes())
        except (OSError, json.JSONDecodeError) as error:
            raise DeploymentError("install_state_invalid", "安装事务不可读") from error
        required = {"schema", "transaction_id", "action", "old_current", "new_current", "state"}
        if (
            not isinstance(value, dict)
            or set(value) != required
            or value["schema"] != TRANSACTION_SCHEMA
            or value["action"] not in {"install", "rollback"}
            or value["state"] not in {"prepared", "switched"}
        ):
            raise DeploymentError("install_state_invalid", "安装事务字段无效")
        _canonical_commit(value["new_current"])
        if value["old_current"] is not None:
            _canonical_commit(value["old_current"])
        return value

    def _settle(self, transaction: Mapping[str, object]) -> None:
        old = transaction["old_current"]
        new = str(transaction["new_current"])
        if not (self.releases / new / "release.json").is_file():
            raise DeploymentError("release_incomplete", "事务目标 release 不完整")
        if transaction["state"] == "prepared":
            self._switch("current", new)
            switched = dict(transaction)
            switched["state"] = "switched"
            _write_atomic(self.transaction, switched)
        self._switch("previous", None if old is None or old == new else str(old))
        keep = {new}
        if old is not None and old != new:
            keep.add(str(old))
        self._prune(keep)
        self.transaction.unlink(missing_ok=True)
        _fsync_directory(self.state_root)

    def recover(self) -> Mapping[str, object]:
        self._require_platform()
        self._ensure_layout()
        transaction = self._read_transaction()
        if transaction is not None:
            self._settle(transaction)
        for staging in self.releases.glob(".*.staging-*"):
            if staging.is_dir() and not staging.is_symlink():
                shutil.rmtree(staging)
        return self.status()

    def _prune(self, keep: set[str]) -> None:
        for candidate in self.releases.iterdir():
            if candidate.name in keep or not candidate.is_dir() or candidate.is_symlink():
                continue
            with suppress(DeploymentError):
                _canonical_commit(candidate.name)
                shutil.rmtree(candidate)
        _fsync_directory(self.releases)

    def _release_metadata(self, release: Path) -> Mapping[str, object]:
        try:
            value = json.loads((release / "release.json").read_bytes())
        except (OSError, json.JSONDecodeError) as error:
            raise DeploymentError("release_incomplete", "release metadata 不可读") from error
        if (
            not isinstance(value, dict)
            or value.get("schema") != RELEASE_SCHEMA
            or value.get("commit") != release.name
        ):
            raise DeploymentError("release_incomplete", "release metadata 无效")
        return value

    def _prepare_release(self, bundle: Bundle) -> Path:
        final = self.releases / bundle.commit
        if final.exists():
            self._release_metadata(final)
            return final
        stage = self.releases / f".{bundle.commit}.staging-{uuid.uuid4().hex}"
        stage.mkdir(mode=0o755)
        try:
            self.stage_installer(bundle, stage)
            _write_atomic(
                stage / "release.json",
                {
                    "schema": RELEASE_SCHEMA,
                    "commit": bundle.commit,
                    "version": bundle.version,
                    "bundle_sha256": _sha256(bundle.root / "bundle.json"),
                },
            )
            _fsync_tree(stage)
            os.rename(stage, final)
            _fsync_directory(self.releases)
        except BaseException:
            with suppress(OSError):
                shutil.rmtree(stage)
            raise
        return final

    def _install_assets(self) -> None:
        targets = {
            "rp-ylx.service": self.system_root / "usr/lib/systemd/system/rp-ylx.service",
            "rp-ylx.sysusers": self.system_root / "usr/lib/sysusers.d/rp-ylx.conf",
            "rp-ylx.tmpfiles": self.system_root / "usr/lib/tmpfiles.d/rp-ylx.conf",
        }
        for name, target in targets.items():
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
            temporary.write_bytes(_asset_bytes(name))
            temporary.chmod(0o644)
            os.replace(temporary, target)
            _fsync_directory(target.parent)
        bootstrap = self.system_root / "usr/local/sbin/rp-ylx-deploy"
        bootstrap.parent.mkdir(parents=True, exist_ok=True)
        temporary_bootstrap = bootstrap.with_name(f".{bootstrap.name}.{uuid.uuid4().hex}.tmp")
        temporary_bootstrap.write_bytes(_system_launcher())
        temporary_bootstrap.chmod(0o755)
        os.replace(temporary_bootstrap, bootstrap)
        _fsync_directory(bootstrap.parent)
        bootstrap_root = self.system_root / "usr/local/lib/rp-ylx"
        bootstrap_module = bootstrap_root / "deployment.py"
        bootstrap_root.mkdir(parents=True, exist_ok=True)
        temporary_module = bootstrap_module.with_name(
            f".{bootstrap_module.name}.{uuid.uuid4().hex}.tmp"
        )
        temporary_module.write_bytes(_module_bytes())
        temporary_module.chmod(0o644)
        os.replace(temporary_module, bootstrap_module)
        for name in targets:
            bootstrap_asset = bootstrap_root / name
            temporary_asset = bootstrap_asset.with_name(
                f".{bootstrap_asset.name}.{uuid.uuid4().hex}.tmp"
            )
            temporary_asset.write_bytes(_asset_bytes(name))
            temporary_asset.chmod(0o644)
            os.replace(temporary_asset, bootstrap_asset)
        _fsync_directory(bootstrap_root)
        if not (self.config_root / "device.json").exists():
            config = {
                "schema": PRODUCTION_CONFIG_SCHEMA,
                "listen": {"host": "0.0.0.0", "port": 8080},
                "camera": {
                    "device": "/dev/video0",
                    "width": 3840,
                    "height": 1080,
                    "fps": 60,
                },
                "storage": {
                    "mountpoint": "/mnt/ylx-recording",
                    "minimum_available_bytes": 2 * 1024 * 1024 * 1024,
                    "minimum_available_inodes": 1024,
                },
                "state_root": str(self.state_root),
                "device": dict(_default_device_identity(os.urandom(32))),
                "security": {"profile": "lab", "isolated_network": True},
            }
            _write_atomic(self.config_root / "device.json", config)

    def _wait_for_health(self) -> None:
        deadline = time.monotonic() + ACTIVATION_HEALTH_TIMEOUT_SECONDS
        last_error = "service is not active"
        while time.monotonic() < deadline:
            try:
                active = subprocess.run(
                    ["systemctl", "is-active", "--quiet", "rp-ylx.service"],
                    check=False,
                    timeout=5,
                )
            except (OSError, subprocess.SubprocessError) as error:
                active = None
                last_error = str(error)
            if active is not None and active.returncode == 0:
                try:
                    with urllib.request.urlopen(
                        "http://127.0.0.1:8080/api/v3/device", timeout=2
                    ) as response:
                        if response.status == 200:
                            return
                        last_error = f"Device API HTTP {response.status}"
                except (OSError, urllib.error.URLError) as error:
                    last_error = str(error)
            time.sleep(0.25)
        raise DeploymentError(
            "service_unhealthy", f"Conductor activation health check timed out: {last_error}"
        )

    def _record_activation_failure(self, error: DeploymentError, commit: str) -> None:
        _write_atomic(
            self.state_root / "activation-failure.json",
            {
                "schema": "ylx.activation-failure.v1",
                "release_commit": commit,
                "code": error.code,
                "message": error.message,
            },
        )

    def _activate(self, old_current: str | None, new_current: str) -> None:
        release_changed = old_current != new_current
        try:
            self.runner(["systemctl", "enable", "--now", "rp-ylx.service"])
            if old_current is not None and release_changed:
                self.runner(["systemctl", "restart", "rp-ylx.service"])
            self.health_checker()
            (self.state_root / "activation-failure.json").unlink(missing_ok=True)
        except DeploymentError as error:
            self._record_activation_failure(error, new_current)
            if old_current is None:
                with suppress(DeploymentError):
                    self.runner(["systemctl", "disable", "--now", "rp-ylx.service"])
                self._switch("current", None)
                self._switch("previous", new_current)
            elif release_changed:
                self._switch("current", old_current)
                self._switch("previous", new_current)
                with suppress(DeploymentError):
                    self.runner(["systemctl", "restart", "rp-ylx.service"])
            raise

    def install(self, bundle_path: str | Path, *, activate: bool = True) -> Mapping[str, object]:
        self._require_target()
        self._ensure_layout()
        self.recover()
        bundle = load_bundle(bundle_path)
        self._prepare_release(bundle)
        self._install_assets()
        self.runner(["systemd-sysusers"])
        self.runner(["systemd-tmpfiles", "--create", "rp-ylx.conf"])
        self.runner(["systemctl", "daemon-reload"])
        current = self._link_target("current")
        old_current = current
        if current != bundle.commit:
            transaction = self._transaction_document("install", current, bundle.commit, "prepared")
            _write_atomic(self.transaction, transaction)
            self._settle(transaction)
        if activate:
            self._activate(old_current, bundle.commit)
        return self.status()

    def rollback(self, *, activate: bool = True) -> Mapping[str, object]:
        self._require_target()
        self._ensure_layout()
        self.recover()
        current = self._link_target("current")
        previous = self._link_target("previous")
        if current is None or previous is None:
            raise DeploymentError("rollback_unavailable", "没有可回滚的上一版本")
        transaction = self._transaction_document("rollback", current, previous, "prepared")
        _write_atomic(self.transaction, transaction)
        self._settle(transaction)
        if activate:
            self._activate(current, previous)
        return self.status()

    def uninstall(self) -> Mapping[str, object]:
        self._require_platform()
        with suppress(DeploymentError):
            self.runner(["systemctl", "disable", "--now", "rp-ylx.service"])
        if self.install_root.exists() and not self.install_root.is_symlink():
            shutil.rmtree(self.install_root)
        self.transaction.unlink(missing_ok=True)
        for target in (
            self.system_root / "usr/lib/systemd/system/rp-ylx.service",
            self.system_root / "usr/lib/sysusers.d/rp-ylx.conf",
            self.system_root / "usr/lib/tmpfiles.d/rp-ylx.conf",
            self.system_root / "usr/local/sbin/rp-ylx-deploy",
        ):
            target.unlink(missing_ok=True)
        bootstrap_root = self.system_root / "usr/local/lib/rp-ylx"
        if bootstrap_root.exists() and not bootstrap_root.is_symlink():
            shutil.rmtree(bootstrap_root)
        self.runner(["systemctl", "daemon-reload"])
        return {"installed": False, "config_preserved": True, "state_preserved": True}

    def status(self) -> Mapping[str, object]:
        activation_failure: Mapping[str, object] | None = None
        diagnostic_path = self.state_root / "activation-failure.json"
        if diagnostic_path.exists():
            try:
                diagnostic = json.loads(diagnostic_path.read_bytes())
                if isinstance(diagnostic, dict):
                    activation_failure = diagnostic
            except (OSError, json.JSONDecodeError):
                activation_failure = {
                    "schema": "ylx.activation-failure.v1",
                    "code": "diagnostic_unreadable",
                    "message": "activation failure diagnostic is unreadable",
                }
        return {
            "installed": self._link_target("current") is not None,
            "current": self._link_target("current"),
            "previous": self._link_target("previous"),
            "transaction_pending": self.transaction.exists(),
            "activation_failure": activation_failure,
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rp-ylx-deploy", description="RP-YLX RDK X5 部署工具")
    parser.add_argument("--install-root", type=Path, default=Path("/opt/rp-ylx"))
    parser.add_argument("--config-root", type=Path, default=Path("/etc/rp-ylx"))
    parser.add_argument("--state-root", type=Path, default=Path("/var/lib/rp-ylx"))
    subcommands = parser.add_subparsers(dest="command", required=True)
    install = subcommands.add_parser("install")
    install.add_argument("bundle", type=Path)
    install.add_argument("--no-activate", action="store_true")
    rollback = subcommands.add_parser("rollback")
    rollback.add_argument("--no-activate", action="store_true")
    subcommands.add_parser("recover")
    subcommands.add_parser("status")
    subcommands.add_parser("uninstall")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manager = ReleaseManager(
        install_root=args.install_root,
        config_root=args.config_root,
        state_root=args.state_root,
    )
    try:
        if args.command == "install":
            result = manager.install(args.bundle, activate=not args.no_activate)
        elif args.command == "rollback":
            result = manager.rollback(activate=not args.no_activate)
        elif args.command == "recover":
            result = manager.recover()
        elif args.command == "uninstall":
            result = manager.uninstall()
        else:
            result = manager.status()
    except DeploymentError as error:
        print(
            json.dumps(
                {"ok": False, "error": {"code": error.code, "message": error.message}},
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps({"ok": True, **result}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
