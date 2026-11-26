"""Device configuration, TLS identity management, and release config snapshots."""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import stat
import subprocess
import uuid
from collections.abc import Callable, Mapping
from contextlib import suppress
from pathlib import Path

from .schema import (
    CUSTOMER_TLS_CERTIFICATE_RELATIVE,
    CUSTOMER_TLS_PRIVATE_KEY_RELATIVE,
    CUSTOMER_TOKEN_NAME,
    DEPLOYMENT_ASSETS,
    DEVICE_CONFIG_SNAPSHOT_DIRECTORY,
    PRODUCTION_CONFIG_SCHEMA,
    DeploymentError,
    _canonical_commit,
    _fsync_directory,
    _json_bytes,
    _require_regular_file,
    _write_bytes_atomic,
    valid_customer_bearer_token,
)
from .systemd import _asset_bytes, _ensure_parent_dirs


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


def _default_device_identity(seed: bytes) -> Mapping[str, str]:
    device_id = str(uuid.uuid4())
    digest = hashlib.sha256(seed + device_id.encode()).hexdigest()
    return {
        "device_id": device_id,
        "device_label": f"YLX-{digest[:8].upper()}",
        "hardware_fingerprint": f"sha256:{digest}",
    }


def managed_identity_path(config_root: Path, path: Path) -> bool:
    if not path.is_absolute() or ".." in path.parts:
        return False
    try:
        path.relative_to(config_root)
    except ValueError:
        return False
    return True


def prepare_identity_parent(config_root: Path, path: Path, uid: int, group_gid: int) -> None:
    if not managed_identity_path(config_root, path):
        raise DeploymentError(
            "customer_identity_path_unmanaged",
            f"拒绝在配置目录之外生成 Customer 身份文件：{path}",
        )
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
    current = config_root
    root_metadata = current.stat(follow_symlinks=False)
    if not stat.S_ISDIR(root_metadata.st_mode):
        raise DeploymentError(
            "customer_identity_path_unsafe",
            f"Customer 配置目录不能是符号链接：{current}",
        )
    current.chmod(0o750)
    os.chown(current, uid, group_gid)
    relative_parent = path.parent.relative_to(config_root)
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


def secure_identity_file(path: Path, mode: int, uid: int, group_gid: int) -> bytes:
    metadata = _require_regular_file(path, code="customer_identity_invalid")
    if metadata.st_size <= 0 or metadata.st_size > 1024 * 1024:
        raise DeploymentError(
            "customer_identity_invalid",
            f"Customer 身份文件大小无效：{path}",
        )
    path.chmod(mode)
    os.chown(path, uid, group_gid)
    try:
        return path.read_bytes()
    except OSError as error:
        raise DeploymentError(
            "customer_identity_invalid",
            f"Customer 身份文件不可读：{path}",
        ) from error


def valid_bearer_token(payload: bytes) -> bool:
    try:
        token = payload.rstrip(b"\r\n").decode("ascii")
    except UnicodeDecodeError:
        return False
    return valid_customer_bearer_token(token) and payload.rstrip(b"\r\n") == payload.removesuffix(
        b"\n"
    ).removesuffix(b"\r")


def ensure_customer_identity(
    config: Mapping[str, object],
    *,
    config_root: Path,
    uid: int,
    group_gid: int,
    tls_material_generator: Callable[[Path, Path, str], None],
) -> dict[str, object]:
    security_value = config.get("security")
    device_value = config.get("device")
    if not isinstance(security_value, Mapping) or not isinstance(device_value, Mapping):
        raise DeploymentError("production_config_invalid", "Customer 配置结构无效")
    security = dict(security_value)
    defaults = {
        "bearer_token_file": config_root / CUSTOMER_TOKEN_NAME,
        "tls_certificate_file": config_root / CUSTOMER_TLS_CERTIFICATE_RELATIVE,
        "tls_private_key_file": config_root / CUSTOMER_TLS_PRIVATE_KEY_RELATIVE,
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
        if not managed_identity_path(config_root, path):
            raise DeploymentError(
                "customer_identity_path_unmanaged",
                f"Customer 身份路径必须位于受管配置目录：{path}",
            )
        paths[field] = path
    security.setdefault("principal_id", "device-owner")

    token_path = paths["bearer_token_file"]
    prepare_identity_parent(config_root, token_path, uid, group_gid)
    if not token_path.exists():
        token = secrets.token_urlsafe(48).encode("ascii") + b"\n"
        _write_bytes_atomic(token_path, token, 0o640)
    token_payload = secure_identity_file(token_path, 0o640, uid, group_gid)
    if not valid_bearer_token(token_payload):
        raise DeploymentError("customer_identity_invalid", "Bearer token 格式无效")

    certificate = paths["tls_certificate_file"]
    private_key = paths["tls_private_key_file"]
    for path in (certificate, private_key):
        prepare_identity_parent(config_root, path, uid, group_gid)
        if path.exists():
            _require_regular_file(path, code="customer_identity_invalid")
    device_label = device_value.get("device_label")
    if not isinstance(device_label, str) or re.fullmatch(r"YLX-[0-9A-F]{8}", device_label) is None:
        raise DeploymentError("production_config_invalid", "Customer 设备标签无效")
    if not certificate.exists() or not private_key.exists():
        tls_material_generator(certificate, private_key, device_label)
    secure_identity_file(certificate, 0o644, uid, group_gid)
    secure_identity_file(private_key, 0o640, uid, group_gid)

    normalized = dict(config)
    normalized["security"] = security
    return normalized


def default_device_config(state_root: Path) -> dict[str, object]:
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
            "device": "hw:CARD=D2UQ2,DEV=0",
            "sample_rate_hz": 48000,
            "channels": 2,
            "sample_format": "S16_LE",
        },
        "storage": {
            "mountpoint": "/data",
            "minimum_available_bytes": 2 * 1024 * 1024 * 1024,
            "minimum_available_inodes": 1024,
        },
        "state_root": str(state_root),
        "device": dict(_default_device_identity(os.urandom(32))),
        "security": {"profile": "lab", "isolated_network": True},
    }


def device_config_profile(payload: bytes) -> tuple[str, bool]:
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


def install_mdns_asset(
    system_root: Path,
    *,
    tls_enabled: bool,
    release_package_root: Path | None = None,
) -> None:
    mdns_target = system_root / DEPLOYMENT_ASSETS["rp-ylx.avahi"][0]
    mdns_asset = "rp-ylx-customer.avahi" if tls_enabled else "rp-ylx.avahi"
    payload = _asset_bytes(mdns_asset, release_package_root=release_package_root)
    if not mdns_target.exists() or mdns_target.read_bytes() != payload:
        _ensure_parent_dirs(mdns_target.parent, system_root, 0o755)
        _write_bytes_atomic(mdns_target, payload, 0o644)


def write_active_device_config(
    payload: bytes,
    *,
    config_root: Path,
    system_root: Path,
    uid: int,
    group_gid: int,
    mdns_installer: Callable[[bool], None],
) -> Mapping[str, object]:
    profile, _ = device_config_profile(payload)
    config = json.loads(payload)
    if profile == "customer":
        # Device access is uniformly HTTP, including upgrades and old
        # configuration snapshots restored during rollback. Retain old
        # credential files on disk without using or rewriting them.
        config["security"] = {"profile": "lab", "isolated_network": True}
        payload = _json_bytes(config)
    path = config_root / "device.json"
    root_metadata = config_root.stat(follow_symlinks=False)
    if not stat.S_ISDIR(root_metadata.st_mode):
        raise DeploymentError(
            "production_config_invalid",
            "配置目录必须是普通目录且不能是符号链接",
        )
    config_root.chmod(0o750)
    os.chown(config_root, uid, group_gid)
    _write_bytes_atomic(path, payload, 0o640)
    os.chown(path, uid, group_gid)
    mdns_installer(False)
    return config


def device_config_snapshot(config_root: Path, commit: str) -> Path:
    return config_root / DEVICE_CONFIG_SNAPSHOT_DIRECTORY / f"{commit}.json"


def snapshot_device_config(config_root: Path, commit: str, owner: tuple[int, int]) -> None:
    canonical = _canonical_commit(commit)
    source = config_root / "device.json"
    metadata = _require_regular_file(source, code="production_config_invalid")
    if metadata.st_size <= 0 or metadata.st_size > 128 * 1024:
        raise DeploymentError("production_config_invalid", "device.json 大小无效")
    try:
        payload = source.read_bytes()
    except OSError as error:
        raise DeploymentError("production_config_invalid", "device.json 不可读") from error
    device_config_profile(payload)

    directory = config_root / DEVICE_CONFIG_SNAPSHOT_DIRECTORY
    directory.mkdir(mode=0o700, exist_ok=True)
    directory_metadata = directory.stat(follow_symlinks=False)
    if not stat.S_ISDIR(directory_metadata.st_mode):
        raise DeploymentError(
            "production_config_invalid",
            "release 配置快照目录不能是符号链接",
        )
    uid, gid = owner
    directory.chmod(0o700)
    os.chown(directory, uid, gid)
    snapshot = device_config_snapshot(config_root, canonical)
    _write_bytes_atomic(snapshot, payload, 0o600)
    os.chown(snapshot, uid, gid)


def restore_device_config_snapshot(
    config_root: Path,
    commit: str,
    writer: Callable[[bytes], Mapping[str, object]],
) -> None:
    snapshot = device_config_snapshot(config_root, _canonical_commit(commit))
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
    writer(payload)
