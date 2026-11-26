#!/usr/bin/env python3
"""Standalone, standard-library-only RDK X5 online installer (Python >= 3.10)."""

from __future__ import annotations

import argparse
import base64
import ctypes.util
import fcntl
import grp
import hashlib
import http.client
import json
import os
import platform
import re
import shlex
import shutil
import sqlite3
import ssl
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.parse
import urllib.request
import uuid
import zipfile
from contextlib import contextmanager, suppress
from pathlib import Path

DEFAULT_MANIFEST = "https://openaria-firmware-cn.oss-cn-beijing.aliyuncs.com/rdk-x5/latest.json"
RELEASE_SCHEMA = "openaria.rdk-x5-release.v1"
PLATFORM = "linux_aarch64_rdk_x5_v1"
MAX_ARCHIVE = 2 * 1024**3
MAX_UNPACKED = 4 * 1024**3
CONFIG = Path("/etc/rp-ylx/device.json")
CURRENT = Path("/opt/rp-ylx/current")
DEFAULT_CACHE = Path("/var/cache/openaria")
NETWORK_STATE = Path("/var/lib/rp-ylx-network")
NETWORK_PROFILES = Path("/etc/NetworkManager/system-connections")
UPDATER_DIRECTORY = Path("/usr/local/lib/openaria")
UPDATE_COMMAND = Path("/usr/local/sbin/openaria-update")
DATA_VOLUME_CONFIG = Path("/etc/default/rp-ylx-data-volume")
FACTORY_STORAGE_STATE = Path("/var/lib/openaria-factory/firstboot/storage.json")
RELEASE_SIGNING_PUBLIC_KEY = Path("/usr/local/lib/openaria/release-signing.pub")
OPENARIA_UPDATE_PUBLIC_KEY_ENV = "OPENARIA_UPDATE_PUBLIC_KEY"
_ED25519_SPKI_PREFIX = bytes.fromhex("302a300506032b6570032100")


class MaintenanceBusyError(ValueError):
    """A cooperating capture or maintenance process currently holds the lease."""


def _unique_json_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("JSON 包含重复键")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"JSON 包含非法数值常量：{value}")


def _loads_strict_json(raw: str | bytes) -> object:
    text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
    return json.loads(
        text,
        object_pairs_hook=_unique_json_pairs,
        parse_constant=_reject_json_constant,
    )


def _read_bounded_regular_file(
    path: Path,
    *,
    max_bytes: int,
    error_message: str,
    too_large_message: str | None = None,
    forbidden_mode: int = 0,
) -> bytes:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    try:
        fd = os.open(path, flags)
    except OSError as error:
        raise ValueError(error_message) from error
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError(error_message)
        if forbidden_mode and (stat.S_IMODE(info.st_mode) & forbidden_mode):
            raise ValueError(error_message)
        if info.st_size > max_bytes:
            raise ValueError(too_large_message or error_message)
        chunks = bytearray()
        while len(chunks) <= max_bytes:
            block = os.read(fd, min(8192, max_bytes + 1 - len(chunks)))
            if not block:
                break
            chunks.extend(block)
        if len(chunks) > max_bytes:
            raise ValueError(too_large_message or error_message)
        return bytes(chunks)
    finally:
        os.close(fd)


def atomic_json(path: Path, value: dict, *, mode: int = 0o600) -> None:
    """Publish a durable JSON file, cleaning only our unpublished temporary file."""
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    payload = json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
    _atomic_write_file(path, payload, mode)


def sqlite_state_required() -> bool:
    root = Path("/var/lib/rp-ylx")
    if os.path.lexists(CONFIG):
        value = _loads_strict_json(
            _read_bounded_regular_file(
                CONFIG,
                max_bytes=128 * 1024,
                error_message="设备配置文件无效",
                forbidden_mode=0o022,
            )
        )
        if not isinstance(value, dict):
            raise ValueError("设备配置文件无效")
        root = Path(value.get("state_root", str(root)))
    return any((root / (name + ".sqlite3.initialized")).exists() for name in ("state", "settings"))


def require_sqlite_wheel(path: Path) -> None:
    if sqlite_state_required():
        with zipfile.ZipFile(path) as wheel:
            if "rp_ylx/persistence.py" not in wheel.namelist():
                raise ValueError("设备已迁移 SQLite，目标固件不支持当前状态；禁止直接降级")


def _parse_data_volume_config() -> dict[str, str]:
    values: dict[str, str] = {}
    if os.path.lexists(DATA_VOLUME_CONFIG):
        raw = _read_bounded_regular_file(
            DATA_VOLUME_CONFIG,
            max_bytes=64 * 1024,
            error_message="录像分区配置不可读或无效",
        )
        for line in raw.decode("utf-8").splitlines():
            fields = shlex.split(line, comments=True)
            if not fields:
                continue
            if len(fields) != 1 or "=" not in fields[0]:
                raise ValueError("录像分区配置格式无效")
            key, value = fields[0].split("=", 1)
            if key in values:
                raise ValueError("录像分区配置项重复")
            values[key] = value
    return values


def exfat_storage_required() -> bool:
    if os.environ.get("RP_YLX_DATA_VOLUME_MODE") == "exfat" or os.path.lexists(
        FACTORY_STORAGE_STATE
    ):
        return True
    values = _parse_data_volume_config()
    return values.get("RP_YLX_DATA_VOLUME_MODE") == "exfat"


def release_supports_exfat(package_root: Path) -> bool:
    """Verify an unpacked rp_ylx release handles exFAT f_files == 0 and PARTUUID."""
    recording_candidates = (
        package_root / "recording" / "volume.py",
        package_root / "recording" / "fs_safety.py",
        package_root / "recording" / "coordinator.py",
    )
    has_recording_exfat = any(
        candidate.is_file() and "f_files" in candidate.read_text(encoding="utf-8", errors="ignore")
        for candidate in recording_candidates
    )
    data_volume_script = package_root / "deploy" / "rp-ylx-data-volume"
    has_deploy_exfat = False
    if data_volume_script.is_file():
        script_text = data_volume_script.read_text(encoding="utf-8", errors="ignore")
        has_deploy_exfat = "exfat" in script_text and "PARTUUID" in script_text
    return has_recording_exfat and has_deploy_exfat


def require_storage_compatible_wheel(path: Path) -> None:
    if not exfat_storage_required():
        return
    with zipfile.ZipFile(path) as wheel:
        names = set(wheel.namelist())

        def contains(member: str, token: str) -> bool:
            if member not in names:
                return False
            return token in wheel.read(member).decode("utf-8", errors="ignore")

        has_recording_exfat = any(
            contains(member, "f_files")
            for member in (
                "rp_ylx/recording/volume.py",
                "rp_ylx/recording/fs_safety.py",
                "rp_ylx/recording/coordinator.py",
            )
        )
        script_member = "rp_ylx/deploy/rp-ylx-data-volume"
        has_deploy_exfat = contains(script_member, "exfat") and contains(script_member, "PARTUUID")
        if not (has_recording_exfat and has_deploy_exfat):
            raise ValueError(
                "设备已启用 exFAT 独立录像分区，目标固件不支持 exFAT（f_files == 0）；禁止直接降级"
            )


@contextmanager
def maintenance_lock(*, shared: bool = False):
    """Capture holds a shared lease; an update holds an exclusive lease.

    The file lives outside the service's lifecycle and must never be unlinked
    while services restart. Stock Python can use this standalone module too.
    """
    path = Path(os.environ.get("OPENARIA_MAINTENANCE_LOCK", "/run/rp-ylx/maintenance.lock"))
    # Source/mock installations have no system-managed lock. Production tmpfiles
    # creates it before the capture service starts. Root updates always create it.
    if (
        shared
        and not path.exists()
        and not CURRENT.exists()
        and "OPENARIA_MAINTENANCE_LOCK" not in os.environ
    ):
        yield
        return
    flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    if not shared or "OPENARIA_MAINTENANCE_LOCK" in os.environ:
        # A pristine board or isolated test env may not have the parent directory yet.
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
        if os.geteuid() == 0 and (path.parent.stat().st_mode & 0o050) != 0o050:
            os.chmod(path.parent, 0o755)
        flags |= os.O_CREAT
    fd = os.open(path, flags, 0o660)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("更新维护锁不是普通文件")
        # Preserve group-writable permissions even under firmware_control's umask(0077).
        if os.geteuid() == 0 or info.st_uid == os.geteuid():
            os.fchmod(fd, 0o660)
        if os.geteuid() == 0:
            with suppress(KeyError):
                gid = grp.getgrnam("rp-ylx").gr_gid
                if info.st_gid != gid:
                    os.fchown(fd, -1, gid)
        try:
            fcntl.flock(fd, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise MaintenanceBusyError("设备正在录制或更新，请等待当前任务完成") from error
        yield
    finally:
        os.close(fd)


def prepare_first_install_network(
    *, state_dir: Path | None = None, profile_dir: Path | None = None
) -> None:
    """Retain an active factory Wi-Fi profile before the controller starts.

    The active source UUID lets the controller keep the working connection;
    the saved clone provides a reconnect target if that connection is lost.
    NetworkManager clones credentials internally; none enter JSON or logs.
    """
    state_dir = NETWORK_STATE if state_dir is None else state_dir
    profile_dir = NETWORK_PROFILES if profile_dir is None else profile_dir
    if any((state_dir / name).exists() for name in ("lkg-wlan0.json", "lkg-eth0.json")):
        return
    controller_path = state_dir / "controller-state.json"
    database = state_dir / "controller.sqlite3"
    if database.exists() or database.with_suffix(".sqlite3.initialized").exists():
        # This updater also runs standalone, before the application is importable.
        db = sqlite3.connect(database.absolute().as_uri() + "?mode=rw", uri=True, timeout=5)
        try:
            db.execute("PRAGMA busy_timeout=5000")
            if (
                db.execute("PRAGMA application_id").fetchone()[0] != 0x594C5801
                or db.execute("PRAGMA user_version").fetchone()[0] != 1
            ):
                raise ValueError("网络数据库版本无效")
            row = db.execute("SELECT body FROM documents WHERE name='controller'").fetchone()
            if row is None:
                raise ValueError("网络控制器状态缺失")
            controller = _loads_strict_json(row[0])
        finally:
            db.close()
    else:
        controller = (
            _loads_strict_json(controller_path.read_bytes()) if controller_path.exists() else None
        )
    if controller is not None and not isinstance(controller, dict):
        raise ValueError("网络控制器状态无效，停止继承 Wi-Fi")
    if controller is not None and not (
        controller.get("schema") == "ylx.network-controller-state.v1"
        and controller.get("desired", {}).get("mode") == "hotspot"
        and controller.get("transaction") == {"current": None, "latest": None}
        and controller.get("saved") is False
        and controller.get("verified") is False
        and controller.get("receipts") == {}
        and controller.get("work") == {}
    ):
        return

    def nmcli(*args: str) -> str:
        result = subprocess.run(
            ["nmcli", "--escape", "no", *args],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode:
            # Never include nmcli output: queries may contain credentials.
            raise ValueError("无法保存当前 Wi-Fi 配置，尚未激活固件")
        return result.stdout.rstrip("\n")

    connection = nmcli("-g", "GENERAL.CON-UUID", "device", "show", "wlan0")
    if connection in {"", "--"}:
        return
    if not re.fullmatch(r"[0-9a-fA-F-]{36}", connection):
        raise ValueError("当前 Wi-Fi 连接身份无效")

    def field(name: str) -> str:
        return nmcli("-g", name, "connection", "show", "uuid", connection)

    mode = field("802-11-wireless.mode")
    if mode == "ap":
        return
    if mode != "infrastructure":
        raise ValueError("首次安装需要现有 Wi-Fi 客户端连接或有线网络")
    ssid = field("802-11-wireless.ssid")
    security = {"wpa-psk": "wpa2-personal", "sae": "wpa3-personal", "": "open", "--": "open"}.get(
        field("802-11-wireless-security.key-mgmt")
    )
    if security is None or not 1 <= len(ssid.encode()) <= 32 or any(ord(c) < 32 for c in ssid):
        raise ValueError("当前 Wi-Fi 安全类型或 SSID 不支持自动继承，请通过有线网络安装")
    if not nmcli("-g", "IP4.GATEWAY", "device", "show", "wlan0").strip("- "):
        raise ValueError("当前 Wi-Fi 缺少 IPv4 网关，无法验证启动后的管理链路")
    secret = None
    if security != "open":
        if field("802-11-wireless-security.psk-flags") not in {"0", "0 (none)"}:
            raise ValueError("当前 Wi-Fi 凭据未由系统持久保存，请通过有线网络安装")
        secret = nmcli(
            "--show-secrets",
            "-g",
            "802-11-wireless-security.psk",
            "connection",
            "show",
            "uuid",
            connection,
        )
        if not secret:
            raise ValueError("当前 Wi-Fi 缺少已保存凭据，尚未激活固件")
    stable_id = field("connection.stable-id")
    if stable_id in {"", "--"}:
        stable_id = connection
    else:
        stable_id = stable_id.replace("${CONNECTION}", connection)
    profile = "rp-ylx-wifi-client-" + uuid.uuid4().hex[:12]
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    metadata = state_dir.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_mode & 0o077
    ):
        raise ValueError("网络状态目录权限不安全")
    nmcli("connection", "clone", "uuid", connection, profile)
    try:
        nmcli(
            "connection",
            "modify",
            "id",
            profile,
            "connection.autoconnect",
            "no",
            "connection.interface-name",
            "wlan0",
            "connection.permissions",
            "",
            "connection.stable-id",
            stable_id,
        )
        path = profile_dir / (profile + ".nmconnection")
        metadata = path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_uid != os.geteuid()
        ):
            raise ValueError("继承的 Wi-Fi profile 必须由 NetworkManager 保存为 0600 普通文件")
        if (
            secret is not None
            and nmcli(
                "--show-secrets",
                "-g",
                "802-11-wireless-security.psk",
                "connection",
                "show",
                "id",
                profile,
            )
            != secret
        ):
            raise ValueError("Wi-Fi 副本未完整保存凭据，尚未激活固件")
        del secret
        if nmcli("-g", "GENERAL.CON-UUID", "device", "show", "wlan0") != connection:
            raise ValueError("准备安装时 Wi-Fi 已切换，停止激活固件")
        record = {
            "format": "ylx.network-lkg.v0",
            "mode": "wifi-client",
            "interface": "wlan0",
            "profile": profile,
            "bootstrap_connection_uuid": connection,
            "config": {"mode": "wifi-client", "ssid": ssid, "security": security},
        }

        atomic_json(state_dir / "lkg-wlan0.json", record)
        if controller is not None:
            # Migrate only untouched first-boot hotspot defaults. An explicit
            # user transaction or previously saved network always takes priority.
            controller.update(
                desired={
                    "mode": "wifi-client",
                    "ethernet": None,
                    "wifi_client": {
                        "ssid": ssid,
                        "security": security,
                        "credential_state": "absent" if security == "open" else "stored",
                    },
                },
                saved=True,
                verified=True,
                source_revision=controller["source_revision"] + 1,
            )
            if database.exists():
                db = sqlite3.connect(
                    database.absolute().as_uri() + "?mode=rw",
                    uri=True,
                    timeout=5,
                    isolation_level=None,
                )
                try:
                    db.execute("PRAGMA busy_timeout=5000")
                    db.execute("PRAGMA synchronous=FULL")
                    db.execute("BEGIN IMMEDIATE")
                    try:
                        payload = json.dumps(
                            controller,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                            allow_nan=False,
                        )
                        cursor = db.execute(
                            "UPDATE documents SET body=?,revision=revision+1,"
                            "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                            "WHERE name='controller'",
                            (payload,),
                        )
                        if cursor.rowcount != 1:
                            raise ValueError("网络控制器状态行缺失")
                        db.execute("COMMIT")
                    except BaseException:
                        with suppress(sqlite3.Error):
                            db.execute("ROLLBACK")
                        raise
                finally:
                    db.close()
            else:
                atomic_json(controller_path, controller)
    except BaseException:
        with suppress(OSError):
            (state_dir / "lkg-wlan0.json").unlink(missing_ok=True)
        with suppress(OSError, ValueError, subprocess.SubprocessError):
            nmcli("connection", "delete", "id", profile)
        raise
    print("已保留当前 Wi-Fi 连接，并保存自动重连配置", file=sys.stderr, flush=True)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def https_url(url: object) -> str:
    if not isinstance(url, str):
        raise ValueError("下载地址必须是 HTTPS URL")
    parsed = urllib.parse.urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.fragment
        or parsed.query
    ):
        raise ValueError("下载地址必须是无凭据、无查询参数的 HTTPS URL")
    return url


class HTTPSRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        https_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def download(url: str, destination: Path, limit: int) -> None:
    https_url(url)
    opener = urllib.request.build_opener(HTTPSRedirect())
    for attempt in range(3):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "openaria-updater/1"})
            # Small metadata checks must return before the API's 75 second
            # deadline, including retries. Large firmware keeps a longer timeout.
            timeout = 10 if limit <= 128 * 1024 else 60
            with opener.open(request, timeout=timeout) as response, destination.open("wb") as out:
                count = 0
                for block in iter(lambda: response.read(1024 * 1024), b""):
                    count += len(block)
                    if count > limit:
                        raise ValueError("下载超过清单大小或安全上限")
                    out.write(block)
                out.flush()
                os.fsync(out.fileno())
            return
        except OSError:
            if attempt == 2:
                raise
            time.sleep(attempt + 1)


def validate_descriptor(value: dict, maximum: int) -> None:
    digest, size = value.get("sha256"), value.get("bytes")
    if (
        not isinstance(digest, str)
        or not re.fullmatch("[0-9a-f]{64}", digest)
        or type(size) is not int
        or not 0 < size <= maximum
    ):
        raise ValueError("文件大小或 SHA-256 无效")


# Reserve half of the 64 KiB control frame for installed releases, task state
# (including escaped diagnostic text), warnings and the response envelope.
MAX_DISPLAY_METADATA = 32 * 1024


def manifest_display_metadata(value: object) -> dict:
    """Validate detached display fields before publishing or caching a release."""
    if not isinstance(value, dict):
        raise ValueError("发布清单必须是对象")
    version, commit = value.get("version"), value.get("commit")
    if not isinstance(version, str) or not re.fullmatch(r"[!-~]{1,64}", version):
        raise ValueError("发布清单 version 无效")
    if not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("发布清单 commit 无效")
    notes, published = value.get("release_notes", ""), value.get("published_at")
    if not isinstance(notes, str) or len(notes) > 16000:
        raise ValueError("发布说明无效")
    if published is not None and (
        not isinstance(published, str) or not published or len(published.encode("utf-8")) > 128
    ):
        raise ValueError("发布时间无效")
    metadata = {
        "version": version,
        "commit": commit,
        "release_notes": notes,
        "published_at": published,
    }
    # Encoding also rejects lone surrogates. Count JSON escaping, not just
    # characters: control characters can expand to six bytes each on the wire.
    encoded = json.dumps(metadata, ensure_ascii=False, allow_nan=False).encode("utf-8")
    if len(encoded) > MAX_DISPLAY_METADATA:
        raise ValueError("发布元数据超过控制响应容量")
    return metadata


def _raw_ed25519_to_pem(raw_key: bytes) -> bytes:
    if len(raw_key) != 32:
        raise ValueError("Ed25519 公钥长度无效")
    der = _ED25519_SPKI_PREFIX + raw_key
    encoded = base64.encodebytes(der).decode("ascii")
    return f"-----BEGIN PUBLIC KEY-----\n{encoded}-----END PUBLIC KEY-----\n".encode("ascii")


def _configured_public_key_pem() -> bytes | None:
    configured = os.environ.get(OPENARIA_UPDATE_PUBLIC_KEY_ENV, "").strip()
    if configured:
        if "-----BEGIN PUBLIC KEY-----" in configured:
            raw = configured.encode("ascii")
            if len(raw) > 16 * 1024:
                raise ValueError("发布签名公钥过大")
            return raw
        raw = _read_bounded_regular_file(
            Path(configured),
            max_bytes=16 * 1024,
            error_message="发布签名公钥文件缺失或无效",
            too_large_message="发布签名公钥过大",
        )
    elif os.path.lexists(RELEASE_SIGNING_PUBLIC_KEY):
        raw = _read_bounded_regular_file(
            RELEASE_SIGNING_PUBLIC_KEY,
            max_bytes=16 * 1024,
            error_message="发布签名公钥文件缺失或无效",
            too_large_message="发布签名公钥过大",
        )
    else:
        return None
    if len(raw) > 16 * 1024:
        raise ValueError("发布签名公钥过大")
    if b"-----BEGIN PUBLIC KEY-----" in raw:
        return raw
    if len(raw) == 32:
        return _raw_ed25519_to_pem(raw)
    try:
        decoded = base64.b64decode(raw.strip(), validate=True)
    except ValueError as error:
        raise ValueError("发布签名公钥格式无效") from error
    return _raw_ed25519_to_pem(decoded)


def _decode_ed25519_signature(value: object) -> bytes:
    if isinstance(value, dict):
        if set(value) != {"alg", "sig"} or value.get("alg") != "ed25519":
            raise ValueError("发布清单签名字段无效")
        value = value.get("sig")
    if not isinstance(value, str) or not value.strip():
        raise ValueError("发布清单签名格式无效")
    try:
        decoded = base64.b64decode(value.strip(), validate=True)
    except ValueError as error:
        raise ValueError("发布清单签名 Base64 无效") from error
    if len(decoded) != 64:
        raise ValueError("发布清单 Ed25519 签名长度无效")
    return decoded


def _verify_ed25519_bytes(public_key_pem: bytes, message: bytes, signature: bytes) -> None:
    try:
        from cryptography.exceptions import InvalidSignature, UnsupportedAlgorithm
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
        from cryptography.hazmat.primitives.serialization import load_pem_public_key
    except ImportError:
        load_pem_public_key = None
    if load_pem_public_key is not None:
        try:
            key = load_pem_public_key(public_key_pem)
        except (UnsupportedAlgorithm, TypeError, ValueError) as error:
            raise ValueError("发布签名公钥无效") from error
        if not isinstance(key, Ed25519PublicKey):
            raise ValueError("发布签名公钥不是 Ed25519")
        try:
            key.verify(signature, message)
            return
        except InvalidSignature as error:
            raise ValueError("发布清单 Ed25519 签名校验失败") from error
    with tempfile.TemporaryDirectory(prefix=".sig-") as temp:
        root = Path(temp)
        key_file = root / "pub.pem"
        msg_file = root / "msg.bin"
        sig_file = root / "sig.bin"
        key_file.write_bytes(public_key_pem)
        msg_file.write_bytes(message)
        sig_file.write_bytes(signature)
        result = subprocess.run(
            [
                "openssl",
                "pkeyutl",
                "-verify",
                "-pubin",
                "-inkey",
                str(key_file),
                "-rawin",
                "-in",
                str(msg_file),
                "-sigfile",
                str(sig_file),
            ],
            capture_output=True,
            timeout=10,
            check=False,
        )
        if result.returncode != 0:
            raise ValueError("发布清单 Ed25519 签名校验失败")


def verify_manifest_signature(
    manifest: dict, *, raw_manifest: bytes | None = None, signature_bytes: bytes | None = None
) -> None:
    """Verify Ed25519 manifest signature when a signing public key is configured."""
    if "signature" in manifest:
        embedded_sig = _decode_ed25519_signature(manifest["signature"])
    else:
        embedded_sig = None
    public_key_pem = _configured_public_key_pem()
    if public_key_pem is None:
        return
    if embedded_sig is not None:
        canonical = json.dumps(
            {k: v for k, v in manifest.items() if k != "signature"},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        _verify_ed25519_bytes(public_key_pem, canonical, embedded_sig)
        return
    if signature_bytes is not None and raw_manifest is not None:
        if len(signature_bytes) == 64:
            detached_sig = signature_bytes
        else:
            detached_sig = _decode_ed25519_signature(signature_bytes.decode("ascii"))
        _verify_ed25519_bytes(public_key_pem, raw_manifest, detached_sig)
        return
    raise ValueError("发布清单缺少 Ed25519 签名")


def validate_manifest(value: object, *, verify_detached: bool = True) -> dict:
    if not isinstance(value, dict) or value.get("schema") != RELEASE_SCHEMA:
        raise ValueError("发布清单 schema 无效")
    if value.get("platform") != PLATFORM:
        raise ValueError("发布清单不支持 RDK X5")
    manifest_display_metadata(value)
    if not isinstance(value.get("bundle"), dict):
        raise ValueError("发布清单 bundle 无效")
    validate_descriptor(value["bundle"], MAX_ARCHIVE)
    https_url(value["bundle"].get("url"))
    if not isinstance(value.get("updater"), dict):
        raise ValueError("发布清单 updater 无效")
    validate_descriptor(value["updater"], 1024 * 1024)
    https_url(value["updater"].get("url"))
    if "signature" in value:
        _decode_ed25519_signature(value["signature"])
    if verify_detached:
        verify_manifest_signature(value)
    return value


def fetch_manifest(url: str, directory: Path) -> dict:
    target = directory / "manifest.json"
    download(url, target, 128 * 1024)
    raw = target.read_bytes()
    parsed = _loads_strict_json(raw)
    validate_manifest(parsed, verify_detached=False)
    if "signature" in parsed or _configured_public_key_pem() is None:
        verify_manifest_signature(parsed, raw_manifest=raw)
    else:
        sig_path = directory / "manifest.sig"
        download(url + ".sig", sig_path, 4096)
        verify_manifest_signature(parsed, raw_manifest=raw, signature_bytes=sig_path.read_bytes())
    return parsed


def fetch_bundle(manifest: dict, cache: Path) -> Path:
    descriptor = manifest["bundle"]
    target = cache / (descriptor["sha256"] + ".tar.gz")
    if target.is_symlink():
        raise ValueError("缓存文件不允许符号链接")
    if (
        target.is_file()
        and target.stat().st_size == descriptor["bytes"]
        and sha256(target) == descriptor["sha256"]
    ):
        return target
    with tempfile.TemporaryDirectory(prefix=".download-", dir=cache) as temp:
        partial = Path(temp) / "bundle.part"
        print("正在下载固件…", file=sys.stderr, flush=True)
        download(descriptor["url"], partial, descriptor["bytes"])
        if partial.stat().st_size != descriptor["bytes"] or sha256(partial) != descriptor["sha256"]:
            raise ValueError("固件大小或 SHA-256 校验失败，未执行安装")
        partial.replace(target)
    return target


def extract_bundle(archive: Path, target: Path) -> None:
    """Release archives are deliberately flat: only regular bundle files."""
    with tarfile.open(archive, "r:gz") as stream:
        seen = set()
        total = 0
        for member in stream:
            name = member.name
            total += member.size
            if (
                not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.+\-]*", name)
                or not member.isfile()
                or member.issparse()
                or name in seen
                or len(seen) >= 256
                or total > MAX_UNPACKED
            ):
                raise ValueError("固件归档含非法路径、特殊文件或超过上限")
            seen.add(name)
            source = stream.extractfile(member)
            if source is None:
                raise ValueError("固件归档文件不可读")
            with source, (target / name).open("xb") as out:
                shutil.copyfileobj(source, out, length=1024 * 1024)
            (target / name).chmod(0o644)


def verify_bundle(root: Path, manifest: dict) -> None:
    """Check every executable payload before the bundled Python is executed."""
    data = (root / "bundle.json").read_bytes()
    if len(data) > 128 * 1024:
        raise ValueError("bundle.json 过大")
    bundle = _loads_strict_json(data)
    if not isinstance(bundle, dict) or bundle.get("schema") != "ylx.rdk-x5-bundle.v1":
        raise ValueError("bundle.json schema 无效")
    for field in ("commit", "version", "platform"):
        if bundle.get(field) != manifest[field]:
            raise ValueError("固件身份与发布清单不一致")
    if not isinstance(bundle.get("wheels"), list) or not 1 <= len(bundle["wheels"]) <= 128:
        raise ValueError("wheel 列表无效")
    names = {"bundle.json", "SHA256SUMS", "build-info.txt"}
    descriptors = [bundle.get("installer"), bundle.get("runtime"), *bundle["wheels"]]
    for item in descriptors:
        if not isinstance(item, dict):
            raise ValueError("bundle 文件描述无效")
        name = item.get("file")
        if (
            not isinstance(name, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.+\-]*", name)
            or name in names
        ):
            raise ValueError("bundle 文件名无效或重复")
        names.add(name)
        validate_descriptor(item, 512 * 1024**2)
        path = root / name
        if path.is_symlink() or not path.is_file():
            raise ValueError("bundle 文件缺失或不是普通文件")
        if path.stat().st_size != item["bytes"] or sha256(path) != item["sha256"]:
            raise ValueError("bundle 内部完整性校验失败")
    if bundle["installer"]["file"] != "rdk_x5_install.py":
        raise ValueError("bundle 安装器名称无效")
    wheels = [item["file"] for item in bundle["wheels"]]
    if [name for name in wheels if name.startswith("rp_ylx-")] != [bundle.get("application_wheel")]:
        raise ValueError("bundle 应包含唯一应用 wheel")
    if {path.name for path in root.iterdir()} - names:
        raise ValueError("bundle 包含未声明文件")
    require_sqlite_wheel(root / bundle["application_wheel"])
    require_storage_compatible_wheel(root / bundle["application_wheel"])


def current_commit() -> str | None:
    if not CURRENT.is_symlink():
        return None
    name = CURRENT.resolve().name
    if not re.fullmatch("[0-9a-f]{40}", name):
        raise ValueError("已安装版本链接无效")
    return name


def require_target() -> None:
    if os.geteuid() != 0:
        raise ValueError("安装或更新需要 sudo；只查看更新可使用 --check")
    if platform.machine() not in {"aarch64", "arm64"}:
        raise ValueError("仅支持 RDK X5 aarch64；本机可以使用 --check 或 --download-only")
    model = Path("/proc/device-tree/model").read_text().strip("\x00\n ")
    if " ".join(model.split()).casefold() != "d-robotics rdk x5 v1.0":
        raise ValueError("仅支持 D-Robotics RDK X5 V1.0")
    if not Path("/run/systemd/system").is_dir():
        raise ValueError("必须在使用 systemd 的 RDK 系统中安装")


def _resolve_local_host(raw_host: object) -> str:
    if not isinstance(raw_host, str) or not raw_host:
        return "127.0.0.1"
    mapped = {"0.0.0.0": "127.0.0.1", "::": "::1"}.get(raw_host, raw_host)
    return f"[{mapped}]" if ":" in mapped and not mapped.startswith("[") else mapped


def require_idle() -> None:
    if not current_commit():
        return
    config = _loads_strict_json(
        _read_bounded_regular_file(
            CONFIG,
            max_bytes=128 * 1024,
            error_message="设备配置文件无效",
            forbidden_mode=0o022,
        )
    )
    if not isinstance(config, dict):
        raise ValueError("设备配置文件无效")
    security = config["security"]
    headers = {}
    scheme, context = "http", None
    if security["profile"] == "customer":
        scheme = "https"
        cafile = security.get("ca_file") or security["tls_certificate_file"]
        _read_bounded_regular_file(
            Path(cafile),
            max_bytes=1024 * 1024,
            error_message="设备证书文件无效",
            forbidden_mode=0o022,
        )
        context = ssl.create_default_context(cafile=cafile)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = False
        partial_chain = getattr(ssl, "VERIFY_X509_PARTIAL_CHAIN", 0)
        if partial_chain:
            context.verify_flags |= partial_chain
        token_raw = _read_bounded_regular_file(
            Path(security["bearer_token_file"]),
            max_bytes=64 * 1024,
            error_message="访问令牌文件无效",
            forbidden_mode=0o027,
        )
        headers["Authorization"] = "Bearer " + token_raw.decode("utf-8").strip()
    listen = config["listen"]
    host = _resolve_local_host(listen.get("host", "127.0.0.1"))
    port = listen["port"]
    request = urllib.request.Request(
        f"{scheme}://{host}:{port}/api/v4/capture/status", headers=headers
    )
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=context)
    )
    try:
        with opener.open(request, timeout=15) as response:
            value = _loads_strict_json(response.read(1024 * 1024))
    except (OSError, ValueError, http.client.HTTPException) as error:
        raise ValueError("无法确认设备空闲；请先检查 rp-ylx 服务") from error
    if not isinstance(value, dict):
        raise ValueError("无法确认设备空闲；请先检查 rp-ylx 服务")
    snapshot = value.get("snapshot", {})
    if snapshot.get("device_state") != "idle" or snapshot.get("active_recording") is not None:
        raise ValueError("设备尚未空闲，请先停止录制并等待封存完成")


def prepare_dependencies() -> None:
    packages = {
        "nmcli": "network-manager",
        "make": "make",
        "cc": "gcc",
        "openssl": "openssl",
        "mkfs.ext4": "e2fsprogs",
        "fsck.exfat": "exfatprogs",
        "avahi-daemon": "avahi-daemon",
        "sshd": "openssh-server",
        "sudo": "sudo",
        "runuser": "util-linux",
        "ffmpeg": "ffmpeg",
    }
    needed = {package for command, package in packages.items() if shutil.which(command) is None}
    for library, package in (
        ("turbojpeg", "libturbojpeg0"),
        ("asound", "libasound2"),
        ("multimedia", "hobot-multimedia"),
    ):
        if ctypes.util.find_library(library) is None:
            needed.add(package)
    if needed:
        print("安装系统依赖：" + ", ".join(sorted(needed)), file=sys.stderr, flush=True)
        env = {**os.environ, "DEBIAN_FRONTEND": "noninteractive"}
        subprocess.run(["apt-get", "update"], check=True, env=env, timeout=600)
        subprocess.run(
            ["apt-get", "install", "-y", "--no-install-recommends", *sorted(needed)],
            check=True,
            env=env,
            timeout=1200,
        )


def data_volume_size(free_bytes: int) -> int:
    # Leave at least 8 GiB or 20% for the OS, downloads and a second release.
    free_gib = free_bytes // 1024**3
    size = min(80, free_gib - max(8, (free_gib + 4) // 5))
    if size < 4:
        raise ValueError("首次安装至少需要 12 GiB 可用空间（4 GiB 数据区 + 8 GiB 系统预留）")
    return size


def require_configured_exfat_volume() -> bool:
    values = _parse_data_volume_config()
    mode = values.get("RP_YLX_DATA_VOLUME_MODE", "loop")
    if mode != "exfat":
        if mode != "loop" or FACTORY_STORAGE_STATE.exists():
            raise ValueError("独立录像分区配置缺失或无效；禁止回退创建loop卷")
        return False
    identity = values.get("RP_YLX_DATA_VOLUME_UUID", "")
    partuuid = values.get("RP_YLX_DATA_VOLUME_PARTUUID", "")
    if not re.fullmatch(r"[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}", identity) or not re.fullmatch(
        r"[0-9A-Fa-f]{8}-03", partuuid
    ):
        raise ValueError("录像分区身份配置无效")
    device = Path("/dev/disk/by-uuid") / identity
    if not device.is_block_device():
        raise ValueError("配置的录像分区不存在")
    for field, expected in (("TYPE", "exfat"), ("UUID", identity), ("PARTUUID", partuuid)):
        result = subprocess.run(
            ["blkid", "-s", field, "-o", "value", str(device)],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.stdout.strip() != expected:
            raise ValueError("录像分区身份与配置不一致")
    result = subprocess.run(
        ["findmnt", "--json", "--mountpoint", "/data", "-o", "SOURCE,FSTYPE"],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
    mounts = _loads_strict_json(result.stdout)
    if not isinstance(mounts, dict):
        raise ValueError("/data 挂载信息无效")
    filesystems = mounts.get("filesystems", [])
    if (
        len(filesystems) != 1
        or filesystems[0].get("fstype") != "exfat"
        or Path(filesystems[0].get("source", "")).resolve() != device.resolve()
    ):
        raise ValueError("/data挂载的不是配置的录像分区")
    return True


def configure_data_volume() -> None:
    if require_configured_exfat_volume():
        return
    image = Path("/var/lib/rp-ylx-data-volume/data-volume.img")
    override = Path("/etc/systemd/system/rp-ylx-data-volume.service.d/10-size.conf")
    if image.exists() or override.exists():
        return
    if os.path.ismount("/data"):
        raise ValueError("检测到已挂载 /data，需核对现有存储布局后再安装")
    data = Path("/data")
    if data.exists() and any(data.iterdir()):
        raise ValueError("/data 已有未挂载数据，请先核对存储布局")
    size = data_volume_size(shutil.disk_usage("/var/lib").free)
    override.parent.mkdir(parents=True, exist_ok=True)
    override.write_text(f"[Service]\nEnvironment=RP_YLX_DATA_VOLUME_SIZE={size}G\n")
    print(f"自动配置 {size} GiB 固定数据区，保留系统空间", file=sys.stderr)


def fetch_updater(manifest: dict, directory: Path) -> Path:
    item = manifest["updater"]
    target = directory / "update.py"
    download(item["url"], target, item["bytes"])
    if target.stat().st_size != item["bytes"] or sha256(target) != item["sha256"]:
        raise ValueError("更新器校验失败")
    return target


def _atomic_write_file(target: Path, payload: bytes, mode: int) -> None:
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    replaced = False
    fd = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
        mode,
    )
    try:
        try:
            os.fchmod(fd, mode)
            view = memoryview(payload)
            while view:
                written = os.write(fd, view)
                if written <= 0:
                    raise OSError(f"short write to {target}")
                view = view[written:]
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(temporary, target)
        replaced = True
        dir_fd = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except BaseException:
        if not replaced:
            with suppress(OSError):
                temporary.unlink(missing_ok=True)
        raise


def prune_bundle_cache(cache: Path, *, keep: int = 2, preserve: Path | None = None) -> None:
    """Keep only the newest verified *.tar.gz release bundles in cache."""
    candidates: list[tuple[int, Path]] = []
    for entry in cache.iterdir():
        if entry.name.startswith(".") or not entry.name.endswith(".tar.gz"):
            continue
        if entry.is_symlink() or not entry.is_file():
            continue
        if preserve is not None and entry == preserve:
            continue
        with suppress(OSError):
            candidates.append((entry.stat().st_mtime_ns, entry))
    candidates.sort(key=lambda item: item[0], reverse=True)
    allowed_remaining = max(0, keep - (1 if preserve is not None else 0))
    for _, stale in candidates[allowed_remaining:]:
        with suppress(OSError):
            stale.unlink(missing_ok=True)


def install_update_command(source: Path, *, firmware_control: Path | None = None) -> None:
    directory = UPDATER_DIRECTORY
    directory.mkdir(parents=True, exist_ok=True)
    directory.chmod(0o755)
    _atomic_write_file(directory / "update.py", source.read_bytes(), 0o644)
    fw_source = (
        firmware_control if firmware_control is not None else source.parent / "firmware_control.py"
    )
    if fw_source.is_file() and not fw_source.is_symlink():
        _atomic_write_file(directory / "firmware_control.py", fw_source.read_bytes(), 0o644)
    launcher = UPDATE_COMMAND
    launcher.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_file(
        launcher,
        b'#!/bin/sh\nexec /usr/bin/python3 /usr/local/lib/openaria/update.py "$@"\n',
        0o755,
    )


@contextmanager
def cache_lock(cache: Path):
    cache.mkdir(parents=True, exist_ok=True, mode=0o700)
    mode = cache.lstat()
    if not stat.S_ISDIR(mode.st_mode) or mode.st_uid != os.geteuid() or mode.st_mode & 0o022:
        raise ValueError("缓存目录必须归当前用户所有且不允许其他用户写入")
    fd = os.open(cache / ".lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError("另一个安装或更新任务正在运行") from error
        yield


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Open Aria 一键安装、检查更新、升级与回退")
    parser.add_argument("manifest", nargs="?", default=DEFAULT_MANIFEST, help="发布清单 HTTPS URL")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--check", action="store_true", help="仅检查版本，不安装")
    action.add_argument("--download-only", action="store_true", help="下载并校验固件，不安装")
    action.add_argument("--rollback", action="store_true", help="回退到设备上的上一版本（离线）")
    action.add_argument("--status", action="store_true", help="查看本机部署状态（离线）")
    parser.add_argument("--reinstall", action="store_true", help="同一 commit 也重新安装")
    parser.add_argument("--cache-dir", type=Path, default=None)
    parser.add_argument("--expected-commit", help="要求渠道仍指向用户确认的完整 commit")
    args = parser.parse_args(argv)
    try:
        if args.status:
            return subprocess.call(["/usr/local/sbin/rp-ylx-deploy", "status"])
        if not (args.check or args.download_only):
            require_target()
            if args.cache_dir is not None:
                raise ValueError(
                    "--cache-dir 只用于 --check / --download-only；安装共用固定锁和缓存"
                )
        cache = args.cache_dir or (
            DEFAULT_CACHE if os.geteuid() == 0 else Path.home() / ".cache/openaria"
        )
        from contextlib import nullcontext

        lease = nullcontext() if args.check or args.download_only else maintenance_lock()
        with (
            cache_lock(cache),
            lease,
            tempfile.TemporaryDirectory(prefix=".stage-", dir=cache) as temp,
        ):
            if args.rollback:
                require_idle()
                if args.expected_commit is not None:
                    previous = CURRENT.parent / "previous"
                    if not previous.is_symlink() or previous.resolve().name != args.expected_commit:
                        raise ValueError("上一版本已变化，请重新确认回退目标")
                return subprocess.call(["/usr/local/sbin/rp-ylx-deploy", "rollback"], umask=0o022)
            directory = Path(temp)
            manifest = fetch_manifest(args.manifest, directory)
            if args.expected_commit is not None and manifest["commit"] != args.expected_commit:
                raise ValueError("发布渠道已变化，请重新检查并确认更新版本")
            current = current_commit()
            result = {
                "current": current,
                "available": manifest["commit"],
                "version": manifest["version"],
            }
            if args.check:
                print(json.dumps({**result, "update_available": current != manifest["commit"]}))
                return 0
            updater = fetch_updater(manifest, directory)
            if current == manifest["commit"] and not (args.reinstall or args.download_only):
                fw_path = CURRENT / "site-packages" / "rp_ylx" / "firmware_control.py"
                install_update_command(
                    updater,
                    firmware_control=fw_path if fw_path.is_file() else None,
                )
                print(json.dumps({**result, "changed": False}))
                return 0
            archive = fetch_bundle(manifest, cache)
            bundle = directory / "bundle"
            bundle.mkdir()
            extract_bundle(archive, bundle)
            verify_bundle(bundle, manifest)
            prune_bundle_cache(cache, keep=2, preserve=archive)
            if args.download_only:
                print(json.dumps({**result, "archive": str(archive), "verified": True}))
                return 0
            require_idle()
            prepare_dependencies()
            configure_data_volume()
            require_idle()
            if current is None:
                prepare_first_install_network()
            print("校验通过，正在安装并等待服务就绪…", file=sys.stderr, flush=True)
            subprocess.run(
                ["/usr/bin/python3", str(bundle / "rdk_x5_install.py"), "install", str(bundle)],
                check=True,
                # The download bootstrap uses 077 for its private workspace.
                # Installed code must be readable/traversable by rp-ylx; secrets
                # are secured explicitly by the bundle installer.
                umask=0o022,
            )
            if current_commit() != manifest["commit"]:
                raise ValueError("安装后版本不匹配")
            fw_path = CURRENT / "site-packages" / "rp_ylx" / "firmware_control.py"
            install_update_command(
                updater,
                firmware_control=fw_path if fw_path.is_file() else None,
            )
            config = _loads_strict_json(
                _read_bounded_regular_file(
                    CONFIG,
                    max_bytes=128 * 1024,
                    error_message="设备配置文件无效",
                    forbidden_mode=0o022,
                )
            )
            if not isinstance(config, dict):
                raise ValueError("设备配置文件无效")
            scheme = "https" if config["security"]["profile"] == "customer" else "http"
            print(json.dumps({**result, "installed": True, "changed": True}))
            print(
                f"安装完成。浏览器访问 {scheme}://设备IP:{config['listen']['port']}/",
                file=sys.stderr,
            )
            print(
                "以后运行 sudo openaria-update 更新；sudo openaria-update --rollback 回退。",
                file=sys.stderr,
            )
            if scheme == "https":
                print(
                    "首次连接所需访问令牌保存在 /etc/rp-ylx/customer.token（sudo 可读）。",
                    file=sys.stderr,
                )
        return 0
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        RecursionError,
        http.client.HTTPException,
        sqlite3.Error,
        tarfile.TarError,
        subprocess.SubprocessError,
    ) as error:
        print(f"Open Aria 更新失败：{error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
