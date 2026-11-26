"""Deployment schema constants, error types, and bounded atomic file/JSON helpers."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import stat
import sys
import uuid
from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path, PurePosixPath
from typing import TypeVar

BUNDLE_SCHEMA = "ylx.rdk-x5-bundle.v1"
TRANSACTION_SCHEMA = "ylx.install-transaction.v1"
RELEASE_SCHEMA = "ylx.installed-release.v1"
TARGET_PLATFORM = "linux_aarch64_rdk_x5_v1"
PRODUCTION_CONFIG_SCHEMA = "ylx.production-config.v1"
CUSTOMER_BEARER_TOKEN_MIN_LENGTH = 8
CUSTOMER_BEARER_TOKEN_MAX_LENGTH = 512
RDK_X5_MODEL = "D-Robotics RDK X5 V1.0"
MAX_BUNDLE_WHEELS = 128
MAX_WHEEL_BYTES = 512 * 1024 * 1024
MAX_UNPACKED_BYTES = 4 * 1024 * 1024 * 1024
MAX_RUNTIME_FILES = 100_000
MAX_RELEASE_METADATA_BYTES = 64 * 1024
MAX_INSTALL_TRANSACTION_BYTES = 64 * 1024
MAX_ACTIVATION_DIAGNOSTIC_BYTES = 64 * 1024
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
MANAGED_WIFI_PROFILE = re.compile(
    r"rp-ylx-(?:wifi-client|hotspot(?:-rescue)?)-[0-9a-f]{12}\.nmconnection"
)
MAX_NETWORKMANAGER_PROFILE_BYTES = 1024 * 1024
CUSTOMER_TOKEN_NAME = "customer.token"
CUSTOMER_TLS_CERTIFICATE_RELATIVE = Path("tls/device.crt")
CUSTOMER_TLS_PRIVATE_KEY_RELATIVE = Path("tls/device.key")
DEVICE_CONFIG_SNAPSHOT_DIRECTORY = ".release-configs"
DEPLOYMENT_ASSETS: Mapping[str, tuple[str, int]] = {
    "rp-ylx-idle-fstrim": ("usr/local/sbin/rp-ylx-idle-fstrim", 0o755),
    "openaria-idle-fstrim.conf": (
        "etc/systemd/system/fstrim.service.d/50-openaria-idle.conf",
        0o644,
    ),
    "openaria-update.service": ("usr/lib/systemd/system/openaria-update.service", 0o644),
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
    "rp-ylx-recording-button.service": (
        "usr/lib/systemd/system/rp-ylx-recording-button.service",
        0o644,
    ),
    "rp-ylx-recording-button.json": ("etc/rp-ylx/recording-button.json", 0o644),
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
PRESERVED_DEPLOYMENT_ASSETS = {"rp-ylx-wifi-watchdog.default", "rp-ylx-recording-button.json"}
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

_T = TypeVar("_T")


def facade_attr(name: str, default: _T) -> _T:
    """Resolve a symbol through the rp_ylx.deployment facade when imported so patches apply."""
    facade = sys.modules.get("rp_ylx.deployment")
    if facade is not None and hasattr(facade, name):
        return getattr(facade, name)
    return default


def valid_customer_bearer_token(token: str) -> bool:
    return CUSTOMER_BEARER_TOKEN_MIN_LENGTH <= len(
        token
    ) <= CUSTOMER_BEARER_TOKEN_MAX_LENGTH and all(
        0x21 <= ord(character) <= 0x7E for character in token
    )


class DeploymentError(RuntimeError):
    """Typed deployment failure carrying a stable machine-readable error code."""

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


def _write_atomic(path: Path, value: object, *, mode: int = 0o644) -> None:
    _write_bytes_atomic(path, _json_bytes(value), mode)


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


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _canonical_commit(value: object) -> str:
    commit = value
    if (
        not isinstance(commit, str)
        or len(commit) != 40
        or any(character not in "0123456789abcdef" for character in commit)
    ):
        raise DeploymentError("bundle_invalid", "bundle commit 不是完整 Git SHA")
    return commit


def _read_deployment_json(path: Path, *, maximum: int) -> object:
    """Read bounded, stable JSON using only standard-library bootstrap code."""
    directory = descriptor = None
    try:
        directory = os.open(
            path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        )
        parent = os.fstat(directory)

        def identity(info: os.stat_result) -> tuple[int, ...]:
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > maximum:
                raise ValueError("unsafe deployment JSON file")
            return (
                info.st_dev,
                info.st_ino,
                info.st_mode,
                info.st_nlink,
                info.st_size,
                info.st_mtime_ns,
                info.st_ctime_ns,
            )

        expected = identity(os.stat(path.name, dir_fd=directory, follow_symlinks=False))
        descriptor = os.open(
            path.name,
            os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
            dir_fd=directory,
        )
        if identity(os.fstat(descriptor)) != expected:
            raise ValueError("deployment JSON changed before read")
        raw = bytearray()
        while len(raw) <= maximum:
            block = os.read(descriptor, min(8192, maximum + 1 - len(raw)))
            if not block:
                break
            raw.extend(block)
        if len(raw) > maximum or identity(os.fstat(descriptor)) != expected:
            raise ValueError("deployment JSON changed during read")
        if identity(os.stat(path.name, dir_fd=directory, follow_symlinks=False)) != expected:
            raise ValueError("deployment JSON replaced during read")
        after = path.parent.lstat()
        if not stat.S_ISDIR(after.st_mode) or (after.st_dev, after.st_ino) != (
            parent.st_dev,
            parent.st_ino,
        ):
            raise ValueError("deployment JSON directory replaced during read")
    finally:
        primary = sys.exc_info()[1]
        cleanup_error = None
        for owned in (descriptor, directory):
            if owned is not None:
                try:
                    os.close(owned)
                except BaseException as error:
                    if cleanup_error is None:
                        cleanup_error = error
        if primary is None and cleanup_error is not None:
            raise cleanup_error

    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate deployment JSON key")
            value[key] = item
        return value

    def reject_constant(value: str) -> object:
        raise ValueError("nonfinite deployment JSON number")

    value = json.loads(
        raw.decode("utf-8"), object_pairs_hook=unique, parse_constant=reject_constant
    )
    pending: list[tuple[object, int]] = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if depth > 64:
            raise ValueError("deployment JSON nesting too deep")
        if isinstance(item, dict):
            for key, child in item.items():
                key.encode("utf-8")
                pending.append((child, depth + 1))
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
        elif isinstance(item, str):
            item.encode("utf-8")
        elif isinstance(item, float) and not math.isfinite(item):
            raise ValueError("nonfinite deployment JSON number")
    return value


def _read_release_metadata(release: Path) -> Mapping[str, object]:
    reader = facade_attr("_read_deployment_json", _read_deployment_json)
    try:
        value = reader(release / "release.json", maximum=MAX_RELEASE_METADATA_BYTES)
        if (
            not re.fullmatch(r"[0-9a-f]{40}", release.name)
            or not isinstance(value, dict)
            or value.get("schema") != RELEASE_SCHEMA
            or value.get("commit") != release.name
        ):
            raise ValueError("invalid release metadata identity")
        return value
    except (OSError, ValueError, TypeError, RecursionError) as error:
        raise DeploymentError("release_incomplete", "release metadata 不可读或无效") from error


def _current_release_metadata(install_root: Path) -> tuple[Path, Mapping[str, object]]:
    current = install_root / "current"
    if not current.is_symlink():
        raise DeploymentError("release_incomplete", "current release 链接不存在")
    target = PurePosixPath(os.readlink(current))
    if len(target.parts) != 2 or target.parts[0] != "releases":
        raise DeploymentError("install_state_invalid", "current release 链接越界")
    commit = _canonical_commit(target.parts[1])
    release = install_root / "releases" / commit
    return release, _read_release_metadata(release)
