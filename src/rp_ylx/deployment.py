"""RDK X5 单机安装、升级恢复与两版本回滚。"""

from __future__ import annotations

import argparse
import ctypes.util
import grp
import hashlib
import json
import os
import platform
import posixpath
import re
import secrets
import shutil
import socket
import ssl
import stat
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
NETWORK_CONTROL_PROTOCOL = "persistent-listener-notify-v1"
NETWORK_CONTROL_MAX_REQUEST_BYTES = 64 * 1024
LEGACY_NETWORK_STATE_RELATIVE = Path("var/lib/rp-ylx/network")
NETWORK_STATE_RELATIVE = Path("var/lib/rp-ylx-network")
NETWORK_STATE_MIGRATION_MARKER = ".legacy-state-migration-v1.json"
MAX_NETWORK_STATE_MIGRATION_FILES = 4096
MAX_NETWORK_STATE_MIGRATION_BYTES = 128 * 1024 * 1024
FORBIDDEN_NETWORK_STATE_KEYS = frozenset(
    {"credential_ref", "password", "passphrase", "psk", "secret", "token"}
)
NETWORKMANAGER_CONNECTIONS_RELATIVE = Path("etc/NetworkManager/system-connections")
MANAGED_WIFI_PROFILE = re.compile(r"rp-ylx-wifi-client-[0-9a-f]{12}\.nmconnection")
MAX_NETWORKMANAGER_PROFILE_BYTES = 1024 * 1024
CUSTOMER_TOKEN_NAME = "customer.token"
CUSTOMER_TLS_CERTIFICATE_RELATIVE = Path("tls/device.crt")
CUSTOMER_TLS_PRIVATE_KEY_RELATIVE = Path("tls/device.key")
DEVICE_CONFIG_SNAPSHOT_DIRECTORY = ".release-configs"
DEPLOYMENT_ASSETS: Mapping[str, tuple[str, int]] = {
    "rp-ylx-recover.service": (
        "usr/lib/systemd/system/rp-ylx-recover.service",
        0o644,
    ),
    "rp-ylx-network-control.socket": (
        "usr/lib/systemd/system/rp-ylx-network-control.socket",
        0o644,
    ),
    "rp-ylx-network-control.service": (
        "usr/lib/systemd/system/rp-ylx-network-control.service",
        0o644,
    ),
    "rp-ylx.service": ("usr/lib/systemd/system/rp-ylx.service", 0o644),
    # avahi only scans /etc/avahi/services, so the mDNS service definition is
    # installed under /etc rather than /usr/lib.
    "rp-ylx.avahi": ("etc/avahi/services/rp-ylx.service", 0o644),
    "rp-ylx-data-volume": ("usr/local/sbin/rp-ylx-data-volume", 0o755),
    "rp-ylx-data-volume.service": (
        "usr/lib/systemd/system/rp-ylx-data-volume.service",
        0o644,
    ),
    "rp-ylx-device-login": ("usr/local/sbin/rp-ylx-device-login", 0o700),
    "rp-ylx.sysusers": ("usr/lib/sysusers.d/rp-ylx.conf", 0o644),
    "rp-ylx.tmpfiles": ("usr/lib/tmpfiles.d/rp-ylx.conf", 0o644),
    "rp-ylx-wifi-watchdog": ("usr/local/sbin/rp-ylx-wifi-watchdog", 0o755),
    "rp-ylx-wifi-watchdog.service": (
        "usr/lib/systemd/system/rp-ylx-wifi-watchdog.service",
        0o644,
    ),
    "rp-ylx-wifi-watchdog.timer": (
        "usr/lib/systemd/system/rp-ylx-wifi-watchdog.timer",
        0o644,
    ),
    "rp-ylx-wifi-watchdog.default": ("etc/default/rp-ylx-wifi-watchdog", 0o644),
    "aic8800-rp-ylx.conf": ("etc/modprobe.d/aic8800-rp-ylx.conf", 0o644),
    "90-rp-ylx-wifi-powersave.conf": (
        "etc/NetworkManager/conf.d/90-rp-ylx-wifi-powersave.conf",
        0o644,
    ),
}
PRESERVED_DEPLOYMENT_ASSETS = {"rp-ylx-wifi-watchdog.default"}
SUPPORTING_DEPLOYMENT_ASSETS: Mapping[str, int] = {"rp-ylx-customer.avahi": 0o644}
CORE_SYSTEMD_UNITS = (
    "rp-ylx-data-volume.service",
    "rp-ylx-network-control.socket",
    "rp-ylx.service",
)
NETWORK_CONTROL_SERVICE = "rp-ylx-network-control.service"
RECOVER_SERVICE = "rp-ylx-recover.service"
WATCHDOG_SERVICE = "rp-ylx-wifi-watchdog.service"
WATCHDOG_TIMER = "rp-ylx-wifi-watchdog.timer"
NETWORK_CONTROL_SOCKET = "run/rp-ylx/network-control.sock"


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


def _write_bytes_atomic(path: Path, payload: bytes, mode: int) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0),
            mode,
        )
        try:
            os.fchmod(descriptor, mode)
            written = 0
            while written < len(payload):
                count = os.write(descriptor, payload[written:])
                if count <= 0:
                    raise OSError(f"{path} 发生短写")
                written += count
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        with suppress(OSError):
            temporary.unlink(missing_ok=True)


def _require_regular_file(path: Path, *, code: str) -> os.stat_result:
    try:
        metadata = path.stat(follow_symlinks=False)
    except OSError as error:
        raise DeploymentError(code, f"文件不可读：{path}") from error
    if not stat.S_ISREG(metadata.st_mode):
        raise DeploymentError(code, f"文件必须是普通文件且不能是符号链接：{path}")
    return metadata


def _default_tls_material_generator(certificate: Path, private_key: Path, common_name: str) -> None:
    certificate.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
    private_key.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
    if certificate.exists() and not private_key.exists():
        raise DeploymentError(
            "customer_identity_incomplete",
            "Customer TLS 证书存在但私钥缺失；拒绝替换已有证书身份",
        )

    if not private_key.exists():
        temporary_key = private_key.with_name(f".{private_key.name}.{uuid.uuid4().hex}.tmp")
        try:
            subprocess.run(
                [
                    "openssl",
                    "genpkey",
                    "-algorithm",
                    "RSA",
                    "-pkeyopt",
                    "rsa_keygen_bits:3072",
                    "-out",
                    str(temporary_key),
                ],
                check=True,
                capture_output=True,
                timeout=60,
            )
            _require_regular_file(temporary_key, code="customer_identity_generation_failed")
            temporary_key.chmod(0o640)
            os.replace(temporary_key, private_key)
            _fsync_directory(private_key.parent)
        except (OSError, subprocess.SubprocessError) as error:
            raise DeploymentError(
                "customer_identity_generation_failed",
                "无法生成 Customer TLS 私钥",
            ) from error
        finally:
            with suppress(OSError):
                temporary_key.unlink(missing_ok=True)

    if certificate.exists():
        return

    temporary_config = certificate.parent / f".openssl-{uuid.uuid4().hex}.cnf"
    temporary_certificate = certificate.with_name(f".{certificate.name}.{uuid.uuid4().hex}.tmp")
    config = f"""[req]
prompt = no
distinguished_name = subject
x509_extensions = extensions

[subject]
CN = {common_name}

[extensions]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @subject_alt_names

[subject_alt_names]
DNS.1 = rp-ylx.local
DNS.2 = localhost
DNS.3 = {common_name}
IP.1 = 127.0.0.1
IP.2 = 10.42.0.1
""".encode("ascii")
    try:
        _write_bytes_atomic(temporary_config, config, 0o600)
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-new",
                "-sha256",
                "-days",
                "3650",
                "-key",
                str(private_key),
                "-config",
                str(temporary_config),
                "-out",
                str(temporary_certificate),
            ],
            check=True,
            capture_output=True,
            timeout=60,
        )
        _require_regular_file(
            temporary_certificate,
            code="customer_identity_generation_failed",
        )
        temporary_certificate.chmod(0o644)
        os.replace(temporary_certificate, certificate)
        _fsync_directory(certificate.parent)
    except (OSError, subprocess.SubprocessError) as error:
        raise DeploymentError(
            "customer_identity_generation_failed",
            "无法生成 Customer TLS 证书",
        ) from error
    finally:
        with suppress(OSError):
            temporary_config.unlink(missing_ok=True)
        with suppress(OSError):
            temporary_certificate.unlink(missing_ok=True)


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
TlsMaterialGenerator = Callable[[Path, Path, str], None]
GroupResolver = Callable[[str], int]
UnitLoadStateReader = Callable[[str], str]


def _group_gid(name: str) -> int:
    try:
        return grp.getgrnam(name).gr_gid
    except KeyError as error:
        raise DeploymentError("service_identity_missing", f"系统组不存在：{name}") from error


def _run(arguments: Sequence[str]) -> None:
    try:
        subprocess.run(list(arguments), check=True, timeout=300)
    except (OSError, subprocess.SubprocessError) as error:
        raise DeploymentError("command_failed", f"命令失败：{' '.join(arguments)}") from error


def _systemd_unit_load_state(unit: str) -> str:
    try:
        completed = subprocess.run(
            ["systemctl", "show", "--property=LoadState", "--value", unit],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise DeploymentError(
            "unit_state_query_failed",
            f"无法查询 systemd unit 状态：{unit}",
        ) from error
    state = completed.stdout.strip()
    if completed.returncode != 0 or not state or "\n" in state:
        raise DeploymentError(
            "unit_state_query_failed",
            f"systemd unit 状态无效：{unit}",
        )
    return state


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


def _notify_systemd_ready(status: str) -> None:
    address = os.environ.get("NOTIFY_SOCKET")
    if not address:
        return
    if address.startswith("@"):
        address = "\0" + address[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as notifier:
            notifier.connect(address)
            notifier.sendall(f"READY=1\nSTATUS={status}".encode())
    except OSError as error:
        raise DeploymentError(
            "readiness_notification_failed",
            "无法通知 systemd 网络控制兼容 launcher 已就绪",
        ) from error


def _current_release_metadata(install_root: Path) -> tuple[Path, Mapping[str, object]]:
    current = install_root / "current"
    if not current.is_symlink():
        raise DeploymentError("release_incomplete", "current release 链接不存在")
    target = PurePosixPath(os.readlink(current))
    if len(target.parts) != 2 or target.parts[0] != "releases":
        raise DeploymentError("install_state_invalid", "current release 链接越界")
    commit = _canonical_commit(target.parts[1])
    release = install_root / "releases" / commit
    try:
        metadata = json.loads((release / "release.json").read_bytes())
    except (OSError, json.JSONDecodeError) as error:
        raise DeploymentError("release_incomplete", "current release metadata 不可读") from error
    if (
        not isinstance(metadata, dict)
        or metadata.get("schema") != RELEASE_SCHEMA
        or metadata.get("commit") != commit
    ):
        raise DeploymentError("release_incomplete", "current release metadata 无效")
    return release, metadata


def _control_compatibility_error(payload: bytes) -> bytes:
    operation: str | None = None
    if len(payload) <= NETWORK_CONTROL_MAX_REQUEST_BYTES:
        try:
            request = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            request = None
        if isinstance(request, Mapping) and isinstance(request.get("operation"), str):
            operation = str(request["operation"])
    response: dict[str, object] = {
        "schema": "ylx.network-control-response.v1",
        "ok": False,
        "error": {
            "code": "network_controller_release_incompatible",
            "message": "the active rollback release does not support network mutation",
        },
        "retryable": False,
    }
    if operation is not None:
        response["operation"] = operation
    return (
        json.dumps(response, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _read_control_compatibility_request(connection: socket.socket) -> bytes:
    payload = bytearray()
    connection.settimeout(2.0)
    while len(payload) <= NETWORK_CONTROL_MAX_REQUEST_BYTES:
        block = connection.recv(4096)
        if not block:
            break
        payload.extend(block)
        newline = payload.find(b"\n")
        if newline >= 0:
            return bytes(payload[:newline])
    return bytes(payload)


def serve_network_control_launcher(
    install_root: Path = Path("/opt/rp-ylx"),
    *,
    listener: socket.socket | None = None,
    executor: Callable[[str, Sequence[str]], object] = os.execv,
    notifier: Callable[[str], None] = _notify_systemd_ready,
    max_connections: int | None = None,
) -> int:
    """Run the current controller protocol or a persistent fail-closed rollback shim."""

    release, metadata = _current_release_metadata(install_root)
    executable = release / "bin/rp-ylx"
    if metadata.get("network_control_protocol") == NETWORK_CONTROL_PROTOCOL:
        if not executable.is_file() or not os.access(executable, os.X_OK):
            raise DeploymentError("release_incomplete", "network-control executable 不可执行")
        executor(
            str(executable),
            [str(executable), "network-control", "serve", "--stdio"],
        )
        return 0

    owns_listener = listener is None
    if listener is None:
        try:
            listener = socket.fromfd(sys.stdin.fileno(), socket.AF_UNIX, socket.SOCK_STREAM)
        except (AttributeError, OSError, ValueError) as error:
            raise DeploymentError(
                "socket_activation_invalid",
                "network-control launcher 没有收到监听 socket",
            ) from error
    try:
        if listener.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN) != 1:
            raise DeploymentError(
                "socket_activation_invalid",
                "network-control launcher 收到的不是监听 socket",
            )
        notifier("rollback release active; network mutation is fail-closed")
        served = 0
        while max_connections is None or served < max_connections:
            connection, _ = listener.accept()
            with connection:
                try:
                    payload = _read_control_compatibility_request(connection)
                    connection.sendall(_control_compatibility_error(payload))
                except (OSError, TimeoutError):
                    pass
            served += 1
        return 0
    finally:
        if owns_listener:
            listener.close()


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
        encoder_builder: Callable[[Path], None] = _build_stereo_encoder,
        health_checker: Callable[[], None] | None = None,
        tls_material_generator: TlsMaterialGenerator = _default_tls_material_generator,
        group_resolver: GroupResolver = _group_gid,
        unit_load_state_reader: UnitLoadStateReader = _systemd_unit_load_state,
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
        self.encoder_builder = encoder_builder
        self.health_checker = health_checker or self._wait_for_health
        self.tls_material_generator = tls_material_generator
        self.group_resolver = group_resolver
        self.unit_load_state_reader = unit_load_state_reader

    def _require_target(self) -> None:
        self._require_platform()
        if self.library_finder("turbojpeg") is None:
            raise DeploymentError("runtime_dependency_missing", "正式 60 FPS 路径要求 libturbojpeg")
        if self.library_finder("multimedia") is None:
            raise DeploymentError(
                "runtime_dependency_missing", "边录边出 H.264 要求 hobot-multimedia"
            )
        required_commands = (
            "systemctl",
            "systemd-sysusers",
            "systemd-tmpfiles",
            "nmcli",
            "openssl",
            "make",
            "cc",
            "getent",
            "useradd",
            "usermod",
            "chage",
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
            "config_snapshot_required": True,
        }

    def _read_transaction(self) -> Mapping[str, object] | None:
        if not self.transaction.exists():
            return None
        try:
            value = json.loads(self.transaction.read_bytes())
        except (OSError, json.JSONDecodeError) as error:
            raise DeploymentError("install_state_invalid", "安装事务不可读") from error
        required = {"schema", "transaction_id", "action", "old_current", "new_current", "state"}
        fields = frozenset(value) if isinstance(value, dict) else frozenset()
        allowed_fields = {
            frozenset(required),
            frozenset(required | {"config_snapshot_required"}),
        }
        if (
            not isinstance(value, dict)
            or fields not in allowed_fields
            or value["schema"] != TRANSACTION_SCHEMA
            or value["action"] not in {"install", "rollback"}
            or value["state"] not in {"prepared", "switched"}
            or "config_snapshot_required" in value
            and value["config_snapshot_required"] is not True
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
            new = str(transaction["new_current"])
            self._release_metadata(self.releases / new)
            if transaction.get("config_snapshot_required") is True:
                snapshot = self._device_config_snapshot(new)
                if os.path.lexists(snapshot):
                    self._restore_device_config_snapshot(new)
                    self._settle(transaction)
                else:
                    old = transaction["old_current"]
                    self._revert_release_switch(
                        None if old is None else str(old),
                        new,
                    )
                    if old is not None:
                        self._restore_device_config_snapshot(str(old))
            else:
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
            self.encoder_builder(stage)
            _write_atomic(
                stage / "release.json",
                {
                    "schema": RELEASE_SCHEMA,
                    "commit": bundle.commit,
                    "version": bundle.version,
                    "bundle_sha256": _sha256(bundle.root / "bundle.json"),
                    "network_control_protocol": NETWORK_CONTROL_PROTOCOL,
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

    def _deployment_owner(self) -> tuple[int, int]:
        if self.system_root == Path("/"):
            return 0, 0
        return os.getuid(), os.getgid()

    @staticmethod
    def _read_network_state_payload(path: Path) -> bytes:
        metadata = _require_regular_file(path, code="network_state_migration_unsafe")
        if metadata.st_mode & 0o077 or metadata.st_size > MAX_NETWORK_STATE_MIGRATION_BYTES:
            raise DeploymentError(
                "network_state_migration_unsafe",
                f"旧网络状态文件权限或大小无效：{path.name}",
            )
        try:
            payload = path.read_bytes()
            value = json.loads(payload)
        except (OSError, json.JSONDecodeError) as error:
            raise DeploymentError(
                "network_state_migration_unsafe",
                f"旧网络状态文件不是有效 JSON：{path.name}",
            ) from error
        if not isinstance(value, dict):
            raise DeploymentError(
                "network_state_migration_unsafe",
                f"旧网络状态文件必须是 JSON object：{path.name}",
            )
        if ReleaseManager._network_state_contains_secret(value):
            raise DeploymentError(
                "network_state_migration_unsafe",
                f"旧网络状态文件包含禁止迁移的凭据字段：{path.name}",
            )
        return payload

    @staticmethod
    def _network_state_contains_secret(value: object) -> bool:
        if isinstance(value, Mapping):
            return any(
                isinstance(key, str)
                and key.casefold() in FORBIDDEN_NETWORK_STATE_KEYS
                or ReleaseManager._network_state_contains_secret(item)
                for key, item in value.items()
            )
        if isinstance(value, list):
            return any(ReleaseManager._network_state_contains_secret(item) for item in value)
        return False

    def _network_state_payloads(
        self,
        root: Path,
        *,
        migration_target: bool,
    ) -> dict[str, bytes]:
        try:
            metadata = root.stat(follow_symlinks=False)
        except FileNotFoundError:
            return {}
        except OSError as error:
            raise DeploymentError(
                "network_state_migration_unsafe",
                "网络状态目录不可读",
            ) from error
        if not stat.S_ISDIR(metadata.st_mode) or metadata.st_mode & 0o077:
            raise DeploymentError(
                "network_state_migration_unsafe",
                "网络状态目录必须是禁止组和其他用户访问的普通目录",
            )

        allowed_root_files = {
            "rescue.json",
            "journal.json",
            "lkg-wlan0.json",
            "lkg-eth0.json",
            "controller-state.json",
        }
        transient_files = {".lock", "operation.lock"}
        payloads: dict[str, bytes] = {}
        try:
            entries = sorted(root.iterdir(), key=lambda path: path.name)
        except OSError as error:
            raise DeploymentError(
                "network_state_migration_unsafe",
                "无法枚举网络状态目录",
            ) from error
        for entry in entries:
            if entry.name == NETWORK_STATE_MIGRATION_MARKER and migration_target:
                continue
            if entry.name in transient_files:
                _require_regular_file(entry, code="network_state_migration_unsafe")
                continue
            if entry.name == ".idempotency-key":
                key_metadata = _require_regular_file(
                    entry,
                    code="network_state_migration_unsafe",
                )
                if key_metadata.st_mode & 0o077 or key_metadata.st_size != 32:
                    raise DeploymentError(
                        "network_state_migration_unsafe",
                        "旧网络幂等密钥权限或大小无效",
                    )
                try:
                    payloads[entry.name] = entry.read_bytes()
                except OSError as error:
                    raise DeploymentError(
                        "network_state_migration_unsafe",
                        "旧网络幂等密钥不可读",
                    ) from error
                continue
            if entry.name == "requests":
                request_metadata = entry.stat(follow_symlinks=False)
                if not stat.S_ISDIR(request_metadata.st_mode) or request_metadata.st_mode & 0o077:
                    raise DeploymentError(
                        "network_state_migration_unsafe",
                        "旧网络请求状态目录权限无效",
                    )
                for receipt in sorted(entry.iterdir(), key=lambda path: path.name):
                    if re.fullmatch(r"[0-9a-f]{64}\.json", receipt.name) is None:
                        raise DeploymentError(
                            "network_state_migration_unsafe",
                            f"旧网络请求状态包含未知文件：{receipt.name}",
                        )
                    payloads[f"requests/{receipt.name}"] = self._read_network_state_payload(receipt)
                continue
            if entry.name not in allowed_root_files:
                raise DeploymentError(
                    "network_state_migration_unsafe",
                    f"网络状态目录包含未知条目：{entry.name}",
                )
            payloads[entry.name] = self._read_network_state_payload(entry)
        if (
            len(payloads) > MAX_NETWORK_STATE_MIGRATION_FILES
            or sum(len(payload) for payload in payloads.values())
            > MAX_NETWORK_STATE_MIGRATION_BYTES
        ):
            raise DeploymentError(
                "network_state_migration_unsafe",
                "旧网络状态超过安全迁移上限",
            )
        return payloads

    def _migrate_legacy_network_state(self) -> None:
        legacy = self.system_root / LEGACY_NETWORK_STATE_RELATIVE
        target = self.system_root / NETWORK_STATE_RELATIVE
        marker = target / NETWORK_STATE_MIGRATION_MARKER
        uid, gid = self._deployment_owner()

        if os.path.lexists(marker):
            marker_metadata = _require_regular_file(
                marker,
                code="network_state_migration_unsafe",
            )
            if marker_metadata.st_mode & 0o077:
                raise DeploymentError(
                    "network_state_migration_unsafe",
                    "网络状态迁移标记权限无效",
                )
            try:
                value = json.loads(marker.read_bytes())
            except (OSError, json.JSONDecodeError) as error:
                raise DeploymentError(
                    "network_state_migration_unsafe",
                    "网络状态迁移标记无效",
                ) from error
            if not isinstance(value, dict):
                raise DeploymentError(
                    "network_state_migration_unsafe",
                    "网络状态迁移标记结构无效",
                )
            digests = value.get("files")
            if (
                set(value) != {"schema", "source", "files"}
                or value.get("schema") != "ylx.network-state-migration.v1"
                or value.get("source") != "/var/lib/rp-ylx/network"
                or not isinstance(digests, dict)
            ):
                raise DeploymentError(
                    "network_state_migration_unsafe",
                    "网络状态迁移标记结构无效",
                )
            for relative, digest in digests.items():
                valid_relative = isinstance(relative, str) and (
                    relative
                    in {
                        ".idempotency-key",
                        "rescue.json",
                        "journal.json",
                        "lkg-wlan0.json",
                        "lkg-eth0.json",
                        "controller-state.json",
                    }
                    or re.fullmatch(r"requests/[0-9a-f]{64}\.json", relative) is not None
                )
                if (
                    not isinstance(relative, str)
                    or not isinstance(digest, str)
                    or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                    or not valid_relative
                ):
                    raise DeploymentError(
                        "network_state_migration_unsafe",
                        "网络状态迁移标记包含无效文件摘要",
                    )
            self._network_state_payloads(target, migration_target=True)
            return

        legacy_payloads = self._network_state_payloads(legacy, migration_target=False)
        if not legacy_payloads:
            return
        if os.path.lexists(target):
            target_metadata = target.stat(follow_symlinks=False)
            if not stat.S_ISDIR(target_metadata.st_mode):
                raise DeploymentError(
                    "network_state_migration_unsafe",
                    "新网络状态路径必须是普通目录且不能是符号链接",
                )
        else:
            target.mkdir(parents=True, mode=0o700)
        target.chmod(0o700)
        os.chown(target, uid, gid)
        existing_payloads = self._network_state_payloads(target, migration_target=True)
        extras = set(existing_payloads) - set(legacy_payloads)
        conflicts = {
            name
            for name in set(existing_payloads) & set(legacy_payloads)
            if existing_payloads[name] != legacy_payloads[name]
        }
        if extras or conflicts:
            raise DeploymentError(
                "network_state_migration_conflict",
                "新旧网络状态同时存在且内容不一致；拒绝选择任一侧",
            )
        for relative, payload in legacy_payloads.items():
            destination = target / relative
            destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            destination.parent.chmod(0o700)
            os.chown(destination.parent, uid, gid)
            if not destination.exists():
                _write_bytes_atomic(destination, payload, 0o600)
            destination.chmod(0o600)
            os.chown(destination, uid, gid)
        _write_atomic(
            marker,
            {
                "schema": "ylx.network-state-migration.v1",
                "source": "/var/lib/rp-ylx/network",
                "files": {
                    name: hashlib.sha256(payload).hexdigest()
                    for name, payload in sorted(legacy_payloads.items())
                },
            },
        )
        marker.chmod(0o600)
        os.chown(marker, uid, gid)
        _fsync_directory(target)

    @staticmethod
    def _disable_profile_autoconnect(payload: bytes) -> bytes:
        if not payload or len(payload) > MAX_NETWORKMANAGER_PROFILE_BYTES or b"\0" in payload:
            raise DeploymentError(
                "network_profile_migration_unsafe",
                "受管 Wi-Fi profile 大小或编码无效",
            )
        lines = payload.splitlines(keepends=True)
        section: bytes | None = None
        connection_sections = 0
        autoconnect_lines: list[tuple[int, bytes, bytes]] = []
        for index, line in enumerate(lines):
            content = line.rstrip(b"\r\n")
            stripped = content.strip()
            if stripped.startswith(b"[") and stripped.endswith(b"]"):
                section = stripped[1:-1].strip().lower()
                if section == b"connection":
                    connection_sections += 1
                continue
            if section != b"connection" or b"=" not in content:
                continue
            key, value = content.split(b"=", 1)
            if key.strip().lower() == b"autoconnect":
                autoconnect_lines.append((index, key, value))
        if connection_sections != 1 or len(autoconnect_lines) != 1:
            raise DeploymentError(
                "network_profile_migration_unsafe",
                "受管 Wi-Fi profile 必须包含唯一 connection.autoconnect",
            )
        index, key, value = autoconnect_lines[0]
        normalized = value.strip().lower()
        if normalized in {b"false", b"no", b"0"}:
            return payload
        if normalized not in {b"true", b"yes", b"1"}:
            raise DeploymentError(
                "network_profile_migration_unsafe",
                "受管 Wi-Fi profile 的 autoconnect 值无效",
            )
        line = lines[index]
        newline = line[len(line.rstrip(b"\r\n")) :]
        leading = value[: len(value) - len(value.lstrip())]
        trailing = value[len(value.rstrip()) :]
        lines[index] = key + b"=" + leading + b"false" + trailing + newline
        return b"".join(lines)

    def _disable_managed_wifi_autoconnect(self) -> None:
        connections = self.system_root / NETWORKMANAGER_CONNECTIONS_RELATIVE
        try:
            metadata = connections.stat(follow_symlinks=False)
        except FileNotFoundError:
            return
        except OSError as error:
            raise DeploymentError(
                "network_profile_migration_unsafe",
                "NetworkManager profile 目录不可读",
            ) from error
        if not stat.S_ISDIR(metadata.st_mode) or metadata.st_mode & 0o022:
            raise DeploymentError(
                "network_profile_migration_unsafe",
                "NetworkManager profile 目录必须是不可组写和其他用户写入的普通目录",
            )
        uid, gid = self._deployment_owner()
        changed = False
        try:
            profiles = sorted(connections.iterdir(), key=lambda path: path.name)
        except OSError as error:
            raise DeploymentError(
                "network_profile_migration_unsafe",
                "无法枚举 NetworkManager profile",
            ) from error
        for profile in profiles:
            if MANAGED_WIFI_PROFILE.fullmatch(profile.name) is None:
                continue
            profile_metadata = _require_regular_file(
                profile,
                code="network_profile_migration_unsafe",
            )
            if stat.S_IMODE(profile_metadata.st_mode) != 0o600:
                raise DeploymentError(
                    "network_profile_migration_unsafe",
                    "受管 Wi-Fi profile 权限必须为 0600",
                )
            try:
                payload = profile.read_bytes()
            except OSError as error:
                raise DeploymentError(
                    "network_profile_migration_unsafe",
                    "受管 Wi-Fi profile 不可读",
                ) from error
            migrated = self._disable_profile_autoconnect(payload)
            if migrated == payload:
                continue
            _write_bytes_atomic(profile, migrated, 0o600)
            os.chown(profile, uid, gid)
            changed = True
        if changed:
            self.runner(["nmcli", "connection", "reload"])

    def _managed_identity_path(self, path: Path) -> bool:
        if not path.is_absolute() or ".." in path.parts:
            return False
        try:
            path.relative_to(self.config_root)
        except ValueError:
            return False
        return True

    def _prepare_identity_parent(self, path: Path, group_gid: int) -> None:
        if not self._managed_identity_path(path):
            raise DeploymentError(
                "customer_identity_path_unmanaged",
                f"拒绝在配置目录之外生成 Customer 身份文件：{path}",
            )
        uid, _ = self._deployment_owner()
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
        current = self.config_root
        root_metadata = current.stat(follow_symlinks=False)
        if not stat.S_ISDIR(root_metadata.st_mode):
            raise DeploymentError(
                "customer_identity_path_unsafe",
                f"Customer 配置目录不能是符号链接：{current}",
            )
        current.chmod(0o750)
        os.chown(current, uid, group_gid)
        relative_parent = path.parent.relative_to(self.config_root)
        for part in relative_parent.parts:
            current = current / part
            metadata = current.stat(follow_symlinks=False)
            if not stat.S_ISDIR(metadata.st_mode):
                raise DeploymentError(
                    "customer_identity_path_unsafe",
                    f"Customer 身份目录不能是符号链接：{current}",
                )
            current.chmod(0o750)
            os.chown(current, uid, group_gid)

    def _secure_identity_file(self, path: Path, mode: int, group_gid: int) -> bytes:
        metadata = _require_regular_file(path, code="customer_identity_invalid")
        if metadata.st_size <= 0 or metadata.st_size > 1024 * 1024:
            raise DeploymentError(
                "customer_identity_invalid",
                f"Customer 身份文件大小无效：{path}",
            )
        uid, _ = self._deployment_owner()
        path.chmod(mode)
        os.chown(path, uid, group_gid)
        try:
            return path.read_bytes()
        except OSError as error:
            raise DeploymentError(
                "customer_identity_invalid",
                f"Customer 身份文件不可读：{path}",
            ) from error

    @staticmethod
    def _valid_bearer_token(payload: bytes) -> bool:
        try:
            token = payload.rstrip(b"\r\n").decode("ascii")
        except UnicodeDecodeError:
            return False
        return (
            32 <= len(token) <= 512
            and all(0x21 <= ord(character) <= 0x7E for character in token)
            and payload.rstrip(b"\r\n") == payload.removesuffix(b"\n").removesuffix(b"\r")
        )

    def _ensure_customer_identity(
        self,
        config: Mapping[str, object],
    ) -> dict[str, object]:
        security_value = config.get("security")
        device_value = config.get("device")
        if not isinstance(security_value, Mapping) or not isinstance(device_value, Mapping):
            raise DeploymentError("production_config_invalid", "Customer 配置结构无效")
        security = dict(security_value)
        defaults = {
            "bearer_token_file": self.config_root / CUSTOMER_TOKEN_NAME,
            "tls_certificate_file": self.config_root / CUSTOMER_TLS_CERTIFICATE_RELATIVE,
            "tls_private_key_file": self.config_root / CUSTOMER_TLS_PRIVATE_KEY_RELATIVE,
        }
        paths: dict[str, Path] = {}
        for field, default in defaults.items():
            raw = security.get(field)
            if raw is None:
                path = default
                security[field] = str(path)
            elif isinstance(raw, str):
                path = Path(raw)
            else:
                raise DeploymentError(
                    "production_config_invalid",
                    f"Customer 配置字段必须是路径字符串：{field}",
                )
            if not path.is_absolute():
                raise DeploymentError(
                    "production_config_invalid",
                    f"Customer 身份路径必须是绝对路径：{field}",
                )
            if not self._managed_identity_path(path):
                raise DeploymentError(
                    "customer_identity_path_unmanaged",
                    f"Customer 身份路径必须位于受管配置目录：{path}",
                )
            paths[field] = path
        security.setdefault("principal_id", "device-owner")

        group_gid = self.group_resolver("rp-ylx")
        token_path = paths["bearer_token_file"]
        self._prepare_identity_parent(token_path, group_gid)
        if not token_path.exists():
            token = secrets.token_urlsafe(48).encode("ascii") + b"\n"
            _write_bytes_atomic(token_path, token, 0o640)
        token_payload = self._secure_identity_file(token_path, 0o640, group_gid)
        if not self._valid_bearer_token(token_payload):
            raise DeploymentError("customer_identity_invalid", "Bearer token 格式无效")

        certificate = paths["tls_certificate_file"]
        private_key = paths["tls_private_key_file"]
        for path in (certificate, private_key):
            self._prepare_identity_parent(path, group_gid)
            if path.exists():
                _require_regular_file(path, code="customer_identity_invalid")
        device_label = device_value.get("device_label")
        if (
            not isinstance(device_label, str)
            or re.fullmatch(r"YLX-[0-9A-F]{8}", device_label) is None
        ):
            raise DeploymentError("production_config_invalid", "Customer 设备标签无效")
        if not certificate.exists() or not private_key.exists():
            self.tls_material_generator(certificate, private_key, device_label)
        self._secure_identity_file(certificate, 0o644, group_gid)
        self._secure_identity_file(private_key, 0o640, group_gid)

        normalized = dict(config)
        normalized["security"] = security
        return normalized

    def _default_device_config(self) -> dict[str, object]:
        return {
            "schema": PRODUCTION_CONFIG_SCHEMA,
            "listen": {"host": "0.0.0.0", "port": 8080},
            "camera": {
                "device": "/dev/video0",
                "width": 3840,
                "height": 1080,
                "fps": 60,
                "data_plane": "rust",
            },
            "audio": {
                "enabled": True,
                "device": "hw:0,0",
                "sample_rate_hz": 48000,
                "channels": 2,
                "sample_format": "S16_LE",
            },
            "storage": {
                "mountpoint": "/data",
                "minimum_available_bytes": 2 * 1024 * 1024 * 1024,
                "minimum_available_inodes": 1024,
            },
            "state_root": str(self.state_root),
            "device": dict(_default_device_identity(os.urandom(32))),
            "security": {"profile": "customer", "isolated_network": True},
        }

    def _ensure_device_config(self) -> Mapping[str, object]:
        path = self.config_root / "device.json"
        if path.exists():
            _require_regular_file(path, code="production_config_invalid")
            try:
                value = json.loads(path.read_bytes())
            except (OSError, json.JSONDecodeError) as error:
                raise DeploymentError("production_config_invalid", "device.json 不可读") from error
            if not isinstance(value, dict):
                raise DeploymentError("production_config_invalid", "device.json 必须是 object")
            config: dict[str, object] = value
        else:
            config = self._default_device_config()
        security = config.get("security")
        profile = security.get("profile") if isinstance(security, Mapping) else None
        if profile == "customer":
            config = self._ensure_customer_identity(config)
        elif profile != "lab":
            raise DeploymentError("production_config_invalid", "security.profile 无效")
        self._write_active_device_config(_json_bytes(config))
        return config

    @staticmethod
    def _device_config_profile(payload: bytes) -> tuple[str, bool]:
        try:
            value = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise DeploymentError("production_config_invalid", "device.json 不可读") from error
        security = value.get("security") if isinstance(value, dict) else None
        profile = security.get("profile") if isinstance(security, Mapping) else None
        if profile not in {"lab", "customer"}:
            raise DeploymentError("production_config_invalid", "security.profile 无效")
        tls_enabled = (
            profile == "customer"
            and isinstance(security.get("tls_certificate_file"), str)
            and isinstance(security.get("tls_private_key_file"), str)
        )
        return str(profile), tls_enabled

    def _install_mdns_asset(self, *, tls_enabled: bool) -> None:
        mdns_target = self.system_root / DEPLOYMENT_ASSETS["rp-ylx.avahi"][0]
        mdns_asset = "rp-ylx-customer.avahi" if tls_enabled else "rp-ylx.avahi"
        payload = _asset_bytes(mdns_asset)
        if not mdns_target.exists() or mdns_target.read_bytes() != payload:
            mdns_target.parent.mkdir(parents=True, exist_ok=True)
            _write_bytes_atomic(mdns_target, payload, 0o644)

    def _write_active_device_config(self, payload: bytes) -> None:
        _, tls_enabled = self._device_config_profile(payload)
        path = self.config_root / "device.json"
        group_gid = self.group_resolver("rp-ylx")
        uid, _ = self._deployment_owner()
        root_metadata = self.config_root.stat(follow_symlinks=False)
        if not stat.S_ISDIR(root_metadata.st_mode):
            raise DeploymentError(
                "production_config_invalid",
                "配置目录必须是普通目录且不能是符号链接",
            )
        self.config_root.chmod(0o750)
        os.chown(self.config_root, uid, group_gid)
        _write_bytes_atomic(path, payload, 0o640)
        os.chown(path, uid, group_gid)
        self._install_mdns_asset(tls_enabled=tls_enabled)

    def _device_config_snapshot(self, commit: str) -> Path:
        return self.config_root / DEVICE_CONFIG_SNAPSHOT_DIRECTORY / f"{commit}.json"

    def _snapshot_device_config(self, commit: str) -> None:
        canonical = _canonical_commit(commit)
        source = self.config_root / "device.json"
        metadata = _require_regular_file(source, code="production_config_invalid")
        if metadata.st_size <= 0 or metadata.st_size > 128 * 1024:
            raise DeploymentError("production_config_invalid", "device.json 大小无效")
        try:
            payload = source.read_bytes()
        except OSError as error:
            raise DeploymentError("production_config_invalid", "device.json 不可读") from error
        self._device_config_profile(payload)

        directory = self.config_root / DEVICE_CONFIG_SNAPSHOT_DIRECTORY
        directory.mkdir(mode=0o700, exist_ok=True)
        directory_metadata = directory.stat(follow_symlinks=False)
        if not stat.S_ISDIR(directory_metadata.st_mode):
            raise DeploymentError(
                "production_config_invalid",
                "release 配置快照目录不能是符号链接",
            )
        uid, gid = self._deployment_owner()
        directory.chmod(0o700)
        os.chown(directory, uid, gid)
        snapshot = self._device_config_snapshot(canonical)
        _write_bytes_atomic(snapshot, payload, 0o600)
        os.chown(snapshot, uid, gid)

    def _restore_device_config_snapshot(self, commit: str) -> None:
        snapshot = self._device_config_snapshot(_canonical_commit(commit))
        metadata = _require_regular_file(snapshot, code="release_config_snapshot_missing")
        if metadata.st_mode & 0o077 or metadata.st_size <= 0 or metadata.st_size > 128 * 1024:
            raise DeploymentError(
                "release_config_snapshot_invalid",
                f"release {commit} 的配置快照权限或大小无效",
            )
        try:
            payload = snapshot.read_bytes()
        except OSError as error:
            raise DeploymentError(
                "release_config_snapshot_invalid",
                f"release {commit} 的配置快照不可读",
            ) from error
        self._write_active_device_config(payload)

    def _install_assets(self) -> None:
        targets = {
            name: (self.system_root / relative, mode)
            for name, (relative, mode) in DEPLOYMENT_ASSETS.items()
        }
        bootstrap_root = self.system_root / "usr/local/lib/rp-ylx"
        bootstrap_module = bootstrap_root / "deployment.py"
        bootstrap_root.mkdir(parents=True, exist_ok=True, mode=0o755)
        _write_bytes_atomic(bootstrap_module, _module_bytes(), 0o644)
        for name, (_, mode) in targets.items():
            bootstrap_asset = bootstrap_root / name
            _write_bytes_atomic(bootstrap_asset, _asset_bytes(name), mode)
        for name, mode in SUPPORTING_DEPLOYMENT_ASSETS.items():
            _write_bytes_atomic(bootstrap_root / name, _asset_bytes(name), mode)

        bootstrap = self.system_root / "usr/local/sbin/rp-ylx-deploy"
        bootstrap.parent.mkdir(parents=True, exist_ok=True)
        _write_bytes_atomic(bootstrap, _system_launcher(), 0o755)

        for name, (target, mode) in targets.items():
            if name == "rp-ylx.avahi":
                continue
            if name in PRESERVED_DEPLOYMENT_ASSETS and target.exists():
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            _write_bytes_atomic(target, _asset_bytes(name), mode)

    def _wait_for_health(self) -> None:
        request, context = self._health_request()
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
                    with urllib.request.urlopen(request, timeout=2, context=context) as response:
                        if response.status == 200:
                            return
                        last_error = f"Device API HTTP {response.status}"
                except (OSError, urllib.error.URLError) as error:
                    last_error = str(error)
            time.sleep(0.25)
        raise DeploymentError(
            "service_unhealthy", f"Conductor activation health check timed out: {last_error}"
        )

    def _health_request(self) -> tuple[urllib.request.Request, ssl.SSLContext | None]:
        path = self.config_root / "device.json"
        _require_regular_file(path, code="production_config_invalid")
        try:
            config = json.loads(path.read_bytes())
        except (OSError, json.JSONDecodeError) as error:
            raise DeploymentError("production_config_invalid", "device.json 不可读") from error
        listen = config.get("listen") if isinstance(config, dict) else None
        security = config.get("security") if isinstance(config, dict) else None
        port = listen.get("port") if isinstance(listen, Mapping) else None
        profile = security.get("profile") if isinstance(security, Mapping) else None
        if type(port) is not int or not 1 <= port <= 65535 or profile not in {"lab", "customer"}:
            raise DeploymentError("production_config_invalid", "健康检查配置无效")

        scheme = "https" if profile == "customer" else "http"
        headers: dict[str, str] = {}
        context: ssl.SSLContext | None = None
        if profile == "customer":
            certificate_raw = security.get("tls_certificate_file")
            token_raw = security.get("bearer_token_file")
            if not isinstance(certificate_raw, str) or not isinstance(token_raw, str):
                raise DeploymentError("production_config_invalid", "Customer 健康检查身份缺失")
            certificate = Path(certificate_raw)
            token_path = Path(token_raw)
            _require_regular_file(certificate, code="customer_identity_invalid")
            _require_regular_file(token_path, code="customer_identity_invalid")
            try:
                token_payload = token_path.read_bytes()
            except OSError as error:
                raise DeploymentError(
                    "customer_identity_invalid",
                    "Customer Bearer token 不可读",
                ) from error
            if not self._valid_bearer_token(token_payload):
                raise DeploymentError("customer_identity_invalid", "Bearer token 格式无效")
            token = token_payload.rstrip(b"\r\n").decode("ascii")
            headers["Authorization"] = f"Bearer {token}"
            try:
                context = ssl.create_default_context(cafile=str(certificate))
            except (OSError, ssl.SSLError) as error:
                raise DeploymentError(
                    "customer_identity_invalid",
                    "无法以设备证书建立 Customer 健康检查信任",
                ) from error
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            context.verify_mode = ssl.CERT_REQUIRED
            context.check_hostname = True
            partial_chain = getattr(ssl, "VERIFY_X509_PARTIAL_CHAIN", 0)
            if partial_chain:
                context.verify_flags |= partial_chain

        return (
            urllib.request.Request(
                f"{scheme}://127.0.0.1:{port}/api/v4/device",
                headers=headers,
            ),
            context,
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

    def _run_unit_commands(
        self,
        commands: Sequence[Sequence[str]],
        *,
        code: str,
        message: str,
    ) -> None:
        failures: list[str] = []
        for command in commands:
            try:
                self.runner(command)
            except DeploymentError as error:
                failures.append(f"{' '.join(command)} ({error.code})")
        if failures:
            raise DeploymentError(code, f"{message}：{'; '.join(failures)}")

    def _quiesce_release_services(self) -> None:
        try:
            recover_load_state = self.unit_load_state_reader(RECOVER_SERVICE)
        except DeploymentError as error:
            raise DeploymentError(
                "unit_quiesce_failed",
                f"无法确认部署相关服务状态；拒绝切换 release：{RECOVER_SERVICE} ({error.code})",
            ) from error
        if not recover_load_state or "\n" in recover_load_state:
            raise DeploymentError(
                "unit_quiesce_failed",
                f"无法确认部署相关服务状态；拒绝切换 release：{RECOVER_SERVICE}",
            )
        commands = [
            ["systemctl", "disable", "--now", WATCHDOG_TIMER],
            ["systemctl", "stop", WATCHDOG_SERVICE],
            ["systemctl", "stop", "rp-ylx.service"],
            ["systemctl", "stop", "rp-ylx-network-control.socket"],
            ["systemctl", "stop", NETWORK_CONTROL_SERVICE],
        ]
        if recover_load_state != "not-found":
            commands.append(["systemctl", "stop", RECOVER_SERVICE])
        self._run_unit_commands(
            commands,
            code="unit_quiesce_failed",
            message="无法确认部署相关服务全部停止；拒绝切换 release",
        )

    def _start_release_services(self, *, check_health: bool) -> None:
        for unit in CORE_SYSTEMD_UNITS:
            self.runner(["systemctl", "enable", "--now", unit])
        if check_health:
            self.health_checker()
        self.runner(["systemctl", "enable", "--now", WATCHDOG_TIMER])

    def _deactivate_systemd_units(self) -> None:
        self._run_unit_commands(
            (
                ["systemctl", "disable", "--now", WATCHDOG_TIMER],
                ["systemctl", "stop", WATCHDOG_SERVICE],
                ["systemctl", "disable", "--now", "rp-ylx.service"],
                ["systemctl", "disable", "--now", "rp-ylx-network-control.socket"],
                ["systemctl", "stop", NETWORK_CONTROL_SERVICE],
                ["systemctl", "stop", RECOVER_SERVICE],
                ["systemctl", "disable", "--now", "rp-ylx-data-volume.service"],
            ),
            code="unit_deactivation_failed",
            message="无法确认部署相关服务全部停用；保留安装内容",
        )
        try:
            (self.system_root / NETWORK_CONTROL_SOCKET).unlink(missing_ok=True)
        except OSError as error:
            raise DeploymentError(
                "unit_deactivation_failed",
                "服务已停用，但无法清理 network-control socket；保留安装内容",
            ) from error

    def _restore_release(self, old_current: str, failed_release: str) -> None:
        self._quiesce_release_services()
        transaction = self._transaction_document(
            "rollback",
            failed_release,
            old_current,
            "prepared",
        )
        _write_atomic(self.transaction, transaction)
        self._restore_device_config_snapshot(old_current)
        self._settle(transaction)
        self.runner(["systemctl", "daemon-reload"])
        self._start_release_services(check_health=False)

    def _revert_release_switch(
        self,
        old_current: str | None,
        new_current: str,
    ) -> None:
        current = self._link_target("current")
        if current not in {old_current, new_current}:
            raise DeploymentError(
                "install_state_invalid",
                "release 切换失败后 current 指向未知目标",
            )
        self._switch("current", old_current)
        self._switch("previous", new_current)
        self.transaction.unlink(missing_ok=True)
        _fsync_directory(self.state_root)

    def _activate(self, old_current: str | None, new_current: str) -> None:
        release_changed = old_current != new_current
        try:
            self._start_release_services(check_health=True)
            (self.state_root / "activation-failure.json").unlink(missing_ok=True)
        except DeploymentError as error:
            self._record_activation_failure(error, new_current)
            try:
                if old_current is None:
                    self._deactivate_systemd_units()
                    self._switch("current", None)
                    self._switch("previous", new_current)
                elif release_changed:
                    self._restore_release(old_current, new_current)
                else:
                    self._deactivate_systemd_units()
            except BaseException as cleanup_error:
                detail = getattr(cleanup_error, "message", str(cleanup_error))
                raise DeploymentError(
                    "activation_cleanup_failed",
                    f"release {new_current} 激活失败，且无法安全停止或恢复服务：{detail}",
                ) from cleanup_error
            raise

    def install(self, bundle_path: str | Path, *, activate: bool = True) -> Mapping[str, object]:
        self._require_target()
        self._ensure_layout()
        self.recover()
        bundle = load_bundle(bundle_path)
        self._prepare_release(bundle)
        current = self._link_target("current")
        old_current = current
        quiesced = old_current is not None and (activate or old_current != bundle.commit)
        if quiesced:
            self._quiesce_release_services()
        transaction_started = False
        try:
            self._install_assets()
            self.runner(["systemd-sysusers"])
            self.runner(["systemd-tmpfiles", "--create", "rp-ylx.conf"])
            self.runner(["/usr/local/sbin/rp-ylx-device-login"])
            if old_current is not None:
                self._snapshot_device_config(old_current)
            self._migrate_legacy_network_state()
            self._disable_managed_wifi_autoconnect()
            if current != bundle.commit:
                transaction = self._transaction_document(
                    "install",
                    current,
                    bundle.commit,
                    "prepared",
                )
                _write_atomic(self.transaction, transaction)
                transaction_started = True
            self._ensure_device_config()
            self._snapshot_device_config(bundle.commit)
            self.runner(["systemctl", "daemon-reload"])
            if current != bundle.commit:
                self._settle(transaction)
        except BaseException:
            try:
                if old_current is not None:
                    self._restore_device_config_snapshot(old_current)
                if transaction_started:
                    self._revert_release_switch(old_current, bundle.commit)
                if quiesced:
                    self.runner(["systemctl", "daemon-reload"])
                    self._start_release_services(check_health=False)
            except BaseException as recovery_error:
                detail = getattr(recovery_error, "message", str(recovery_error))
                raise DeploymentError(
                    "install_recovery_failed",
                    f"升级失败，且无法恢复原 release 服务或配置：{detail}",
                ) from recovery_error
            raise
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
        self._quiesce_release_services()
        transaction_started = False
        current_snapshot_written = False
        try:
            self._snapshot_device_config(current)
            current_snapshot_written = True
            transaction = self._transaction_document("rollback", current, previous, "prepared")
            _write_atomic(self.transaction, transaction)
            transaction_started = True
            self._restore_device_config_snapshot(previous)
            self._settle(transaction)
        except BaseException:
            try:
                if current_snapshot_written:
                    self._restore_device_config_snapshot(current)
                if transaction_started:
                    self._revert_release_switch(current, previous)
                self.runner(["systemctl", "daemon-reload"])
                self._start_release_services(check_health=False)
            except BaseException as recovery_error:
                detail = getattr(recovery_error, "message", str(recovery_error))
                raise DeploymentError(
                    "rollback_recovery_failed",
                    f"回滚切换失败，且无法恢复原 release 服务或配置：{detail}",
                ) from recovery_error
            raise
        if activate:
            self._activate(current, previous)
        return self.status()

    def uninstall(self) -> Mapping[str, object]:
        self._require_platform()
        self._deactivate_systemd_units()
        if self.install_root.exists() and not self.install_root.is_symlink():
            shutil.rmtree(self.install_root)
        self.transaction.unlink(missing_ok=True)
        for relative, _ in DEPLOYMENT_ASSETS.values():
            target = self.system_root / relative
            target.unlink(missing_ok=True)
        (self.system_root / "usr/local/sbin/rp-ylx-deploy").unlink(missing_ok=True)
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
    subcommands.add_parser("network-control-launch", help=argparse.SUPPRESS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "network-control-launch":
            return serve_network_control_launcher(args.install_root)
        manager = ReleaseManager(
            install_root=args.install_root,
            config_root=args.config_root,
            state_root=args.state_root,
        )
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
