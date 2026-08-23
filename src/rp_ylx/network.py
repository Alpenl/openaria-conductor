"""RDK X5 上由 NetworkManager 管理的设备网络。"""

from __future__ import annotations

import fcntl
import hashlib
import hmac
import ipaddress
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager, suppress
from copy import deepcopy
from datetime import UTC, datetime
from importlib.resources import files as resource_files
from pathlib import Path
from typing import Any, TextIO

NETWORK_STATUS_FORMAT = "ylx.network-status.v0"
SUPPORTED_MODES = ["hotspot", "wifi-client", "ethernet-dhcp", "ethernet-static"]
WIFI_INTERFACE = "wlan0"
ETHERNET_INTERFACE = "eth0"
MDNS_HOSTNAME = "rp-ylx.local"
MDNS_SERVICE = "_ylx-capture._tcp"
MDNS_SERVICE_ALIASES = ["_http._tcp"]
MDNS_PORT = 8080
MDNS_ASSET_NAME = "rp-ylx.avahi"
MDNS_SERVICE_FILENAME = "rp-ylx.service"
NETWORK_ACTIVATION_WAIT_SECONDS = 10
NETWORK_ACTIVATION_TIMEOUT_SECONDS = 12
MAX_CONFIG_BYTES = 64 * 1024
JOURNAL_FORMAT = "ylx.network-journal.v0"
RESULT_FORMAT = "ylx.network-result.v0"
LKG_FORMAT = "ylx.network-lkg.v0"
REQUEST_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")


class NetworkError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        exit_code: int = 3,
        recovery: str | None = None,
    ) -> None:
        self.code = code
        self.message = message
        self.exit_code = exit_code
        self.recovery = recovery
        super().__init__(f"{code}: {message}")


def _config_error(message: str) -> NetworkError:
    return NetworkError("config_invalid", message, exit_code=2)


def _read_config(source: str, stdin: TextIO) -> Mapping[str, Any]:
    if source == "-":
        try:
            rendered = stdin.read(MAX_CONFIG_BYTES + 1)
        except (OSError, UnicodeError) as exc:
            raise NetworkError("config_unreadable", "无法读取网络配置", exit_code=2) from exc
    else:
        path = Path(source)
        try:
            descriptor = os.open(
                path,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0),
            )
        except OSError as exc:
            raise NetworkError("config_unreadable", "无法读取网络配置", exit_code=2) from exc
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise NetworkError("config_unreadable", "网络配置必须是普通文件", exit_code=2)
            if stat.S_IMODE(metadata.st_mode) & 0o077:
                raise NetworkError(
                    "config_permissions",
                    "网络配置不能允许组用户或其他用户访问",
                    exit_code=2,
                )
            if metadata.st_size > MAX_CONFIG_BYTES:
                raise _config_error("网络配置超过大小上限")
            with os.fdopen(descriptor, "r", encoding="utf-8") as stream:
                descriptor = -1
                rendered = stream.read(MAX_CONFIG_BYTES + 1)
        except (OSError, UnicodeError) as exc:
            raise NetworkError("config_unreadable", "无法读取网络配置", exit_code=2) from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
    try:
        rendered_size = len(rendered.encode("utf-8"))
    except UnicodeError as exc:
        raise NetworkError("config_unreadable", "无法读取网络配置", exit_code=2) from exc
    if rendered_size > MAX_CONFIG_BYTES:
        raise _config_error("网络配置超过大小上限")
    try:
        parsed = json.loads(rendered)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise _config_error("网络配置不是有效 JSON") from exc
    if not isinstance(parsed, dict):
        raise _config_error("网络配置必须是 JSON 对象")
    return parsed


def _text_field(config: Mapping[str, Any], name: str) -> str:
    value = config.get(name)
    if not isinstance(value, str) or not value or any(ord(character) < 32 for character in value):
        raise _config_error(f"字段 {name} 无效")
    return value


def _validate_config(config: Mapping[str, Any]) -> dict[str, Any]:
    mode = config.get("mode")
    if mode not in SUPPORTED_MODES:
        raise _config_error("mode 必须是受支持的网络模式")
    allowed: set[str]
    normalized: dict[str, Any] = {"mode": mode}
    if mode in {"hotspot", "wifi-client"}:
        allowed = {"mode", "ssid", "psk"}
        ssid = _text_field(config, "ssid")
        psk = _text_field(config, "psk")
        if len(ssid.encode("utf-8")) > 32:
            raise _config_error("SSID 必须为 1 至 32 字节")
        if not 8 <= len(psk.encode("utf-8")) <= 63:
            raise _config_error("Wi-Fi 密码必须为 8 至 63 字节")
        normalized.update({"ssid": ssid, "psk": psk})
    elif mode == "ethernet-dhcp":
        allowed = {"mode"}
    else:
        allowed = {"mode", "address", "gateway", "dns"}
        try:
            address = ipaddress.IPv4Interface(_text_field(config, "address"))
        except ValueError as exc:
            raise _config_error("address 必须是有效 IPv4 CIDR") from exc
        if address.network.overlaps(ipaddress.IPv4Network("10.42.0.0/24")):
            raise _config_error("有线静态地址不能与热点网段重叠")
        normalized["address"] = str(address)
        gateway = config.get("gateway")
        if gateway is not None:
            try:
                parsed_gateway = ipaddress.IPv4Address(_text_field(config, "gateway"))
            except ValueError as exc:
                raise _config_error("gateway 必须是有效 IPv4 地址") from exc
            if parsed_gateway not in address.network or parsed_gateway in {
                address.network.network_address,
                address.network.broadcast_address,
            }:
                raise _config_error("gateway 必须是同一子网中的可用主机地址")
            normalized["gateway"] = str(parsed_gateway)
        dns = config.get("dns", [])
        if not isinstance(dns, list) or len(dns) > 3:
            raise _config_error("dns 必须是最多三个 IPv4 地址的数组")
        try:
            normalized["dns"] = [str(ipaddress.IPv4Address(item)) for item in dns]
        except (TypeError, ValueError) as exc:
            raise _config_error("dns 必须是最多三个 IPv4 地址的数组") from exc
    if set(config) != allowed and not set(config).issubset(allowed):
        raise _config_error("网络配置包含未知字段")
    if mode == "ethernet-static" and not set(config).issubset(allowed):
        raise _config_error("网络配置包含未知字段")
    return normalized


def _state_dir() -> Path:
    return Path(os.environ.get("RP_YLX_NETWORK_STATE_DIR", "/var/lib/rp-ylx/network"))


def _profile_dir() -> Path:
    return Path(
        os.environ.get(
            "RP_YLX_NM_PROFILE_DIR",
            "/etc/NetworkManager/system-connections",
        )
    )


def _avahi_dir() -> Path:
    return Path(os.environ.get("RP_YLX_AVAHI_SERVICE_DIR", "/etc/avahi/services"))


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ensure_directory(path: Path, mode: int) -> None:
    existed = path.exists()
    path.mkdir(mode=mode, parents=True, exist_ok=True)
    os.chmod(path, mode)
    _fsync_directory(path)
    if not existed:
        _fsync_directory(path.parent)


def _write_atomic(path: Path, data: bytes, mode: int) -> None:
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        _fsync_directory(path.parent)
        _fsync_directory(path.parent.parent)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        with suppress(OSError):
            temporary.unlink()


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    rendered = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    _write_atomic(path, rendered.encode(), 0o600)


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise NetworkError("state_invalid", f"网络状态文件 {path.name} 无效") from exc
    if not isinstance(value, dict):
        raise NetworkError("state_invalid", f"网络状态文件 {path.name} 无效")
    return value


@contextmanager
def _network_lock(state_dir: Path) -> Iterator[None]:
    lock_path = state_dir / ".lock"
    descriptor = -1
    try:
        descriptor = os.open(
            lock_path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
    except OSError as exc:
        if descriptor >= 0:
            os.close(descriptor)
        raise NetworkError("state_lock_failed", "无法锁定网络配置状态") from exc
    try:
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _prepare_state_dir(state_dir: Path) -> None:
    try:
        _ensure_directory(state_dir, 0o700)
        _ensure_directory(state_dir / "requests", 0o700)
    except OSError as exc:
        raise NetworkError("state_unwritable", "无法创建网络状态目录") from exc


def _idempotency_key(state_dir: Path) -> bytes:
    path = state_dir / ".idempotency-key"
    try:
        key = path.read_bytes()
    except FileNotFoundError:
        key = os.urandom(32)
        try:
            _write_atomic(path, key, 0o600)
        except OSError as exc:
            raise NetworkError("state_unwritable", "无法创建网络幂等密钥") from exc
    except OSError as exc:
        raise NetworkError("state_invalid", "无法读取网络幂等密钥") from exc
    if len(key) != 32:
        raise NetworkError("state_invalid", "网络幂等密钥无效")
    return key


def _fingerprint(config: Mapping[str, Any], key: bytes) -> str:
    canonical = json.dumps(config, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hmac.new(key, canonical.encode(), hashlib.sha256).hexdigest()


def _request_path(state_dir: Path, request_id: str) -> Path:
    name = hashlib.sha256(request_id.encode()).hexdigest()
    return state_dir / "requests" / f"{name}.json"


def _remember_failed_request(state_dir: Path, journal: Mapping[str, Any]) -> None:
    request_id = journal.get("request_id")
    fingerprint = journal.get("request_fingerprint")
    if not isinstance(request_id, str) or not isinstance(fingerprint, str):
        raise NetworkError("journal_invalid", "失败网络事务身份无效")
    try:
        _write_json(
            _request_path(state_dir, request_id),
            {
                "format": "ylx.network-request-failure.v0",
                "request_id": request_id,
                "request_fingerprint": fingerprint,
                "outcome": "failed",
            },
        )
    except OSError as exc:
        raise NetworkError(
            "state_write_failed",
            "无法持久化失败网络请求身份",
            recovery=str(journal.get("recovery", "unavailable")),
        ) from exc


def _lkg_path(state_dir: Path, interface: str) -> Path:
    if interface not in {WIFI_INTERFACE, ETHERNET_INTERFACE}:
        raise NetworkError("state_invalid", "最后已验证网络配置的接口无效")
    return state_dir / f"lkg-{interface}.json"


def _safe_config(config: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in config.items() if key != "psk"}


def _network_static_ipv4(config: Mapping[str, Any]) -> dict[str, Any]:
    interface = ipaddress.IPv4Interface(str(config["address"]))
    gateway = config.get("gateway")
    dns = config.get("dns", [])
    return {
        "address": str(interface.ip),
        "prefix_length": interface.network.prefixlen,
        "gateway": str(gateway) if isinstance(gateway, str) else None,
        "dns": [str(item) for item in dns] if isinstance(dns, list) else [],
    }


def _desired_state_from_config(config: Mapping[str, Any] | None) -> dict[str, Any]:
    if config is None or config.get("mode") not in SUPPORTED_MODES:
        return {
            "mode": "hotspot",
            "wifi_client": None,
            "ethernet": None,
        }
    mode = str(config["mode"])
    wifi_client = None
    ethernet = None
    if mode == "wifi-client":
        ssid = config.get("ssid")
        wifi_client = {
            "ssid": ssid if isinstance(ssid, str) and ssid else "",
            "credential_state": "stored",
        }
    elif mode == "ethernet-dhcp":
        ethernet = {"addressing": "dhcp", "static_ipv4": None}
    elif mode == "ethernet-static":
        try:
            static_ipv4 = _network_static_ipv4(config)
        except (KeyError, ValueError):
            static_ipv4 = None
        ethernet = {"addressing": "static", "static_ipv4": static_ipv4}
    return {
        "mode": mode,
        "wifi_client": wifi_client,
        "ethernet": ethernet,
    }


def _desired_state_from_disk(state_dir: Path) -> dict[str, Any]:
    for path in (
        _lkg_path(state_dir, WIFI_INTERFACE),
        _lkg_path(state_dir, ETHERNET_INTERFACE),
        state_dir / "rescue.json",
    ):
        record = _read_json(path)
        if record is None:
            continue
        config = record.get("config")
        if isinstance(config, Mapping):
            return _desired_state_from_config(config)
    return _desired_state_from_config(None)


def _network_observed_state(
    runtime: Mapping[str, Any],
    legacy_status: Mapping[str, Any],
) -> dict[str, Any]:
    network = runtime.get("network")
    if not isinstance(network, Mapping):
        raise NetworkError("runtime_network_unavailable", "无法读取运行时网络状态")
    mdns = legacy_status.get("mdns")
    devices = legacy_status.get("devices")
    if not isinstance(mdns, Mapping) or not isinstance(devices, list):
        raise NetworkError("network_status_failed", "NetworkManager 状态无效")
    observed = deepcopy(dict(network))
    observed["mdns"] = deepcopy(dict(mdns))
    observed["devices"] = deepcopy(devices)
    return observed


def network_status_v1(
    runtime: Mapping[str, Any],
    *,
    legacy_status: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Project the v4 Device API network status without enabling mutation."""

    status = network_status() if legacy_status is None else legacy_status
    capabilities = status.get("capabilities")
    if not isinstance(capabilities, Mapping):
        raise NetworkError("network_status_failed", "NetworkManager 状态无效")
    second_wifi = capabilities.get("second_wifi") is True
    observed_at = runtime.get("observed_at")
    if not isinstance(observed_at, str) or not observed_at:
        observed_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    state_dir = _state_dir()
    return {
        "schema": "ylx.network-status.v1",
        "observed_at": observed_at,
        "desired": _desired_state_from_disk(state_dir),
        "observed": _network_observed_state(runtime, status),
        "transaction": {
            "current": None,
            "latest": None,
        },
        "mutation_capability": {
            "enabled": False,
            "operations": ["apply", "retry", "forget"],
            "idempotency_key_required": True,
            "secret_handling": "opaque_credential_reference_only",
            "active_state_policy": "idle_only",
        },
        "concurrency_capability": {
            "rescue_ap_required": True,
            "same_phy_ap_sta": "unverified",
            "exclusive_client_failure_timeout_seconds": NETWORK_ACTIVATION_WAIT_SECONDS,
            "max_managed_interfaces": 2 if second_wifi else 1,
            "max_ap_interfaces": 1,
        },
    }


def _profile_name(request_id: str, mode: str) -> str:
    digest = hashlib.sha256(request_id.encode()).hexdigest()[:12]
    return f"rp-ylx-{mode}-{digest}"


def _valid_saved_profile(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    if re.fullmatch(r"rp-ylx-[a-z-]+-[0-9a-f]{12}", value) is None:
        return None
    return value


def _interface(mode: str) -> str:
    return WIFI_INTERFACE if mode in {"hotspot", "wifi-client"} else ETHERNET_INTERFACE


def _keyfile_string(value: object) -> str:
    rendered = str(value).replace("\\", "\\\\")
    leading_spaces = len(rendered) - len(rendered.lstrip(" "))
    return "\\s" * leading_spaces + rendered[leading_spaces:]


def _network_manager_profile(name: str, config: Mapping[str, Any]) -> bytes:
    mode = str(config["mode"])
    interface = _interface(mode)
    priority = 100 if mode == "hotspot" else 800 if mode == "wifi-client" else 900
    profile_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, f"rp-ylx.network.{name}")
    lines = [
        "[connection]",
        f"id={name}",
        f"uuid={profile_uuid}",
        f"type={'wifi' if interface == WIFI_INTERFACE else 'ethernet'}",
        f"interface-name={interface}",
        "autoconnect=true",
        f"autoconnect-priority={priority}",
        "",
    ]
    if mode in {"hotspot", "wifi-client"}:
        lines.extend(
            [
                "[wifi]",
                f"mode={'ap' if mode == 'hotspot' else 'infrastructure'}",
                f"ssid={_keyfile_string(config['ssid'])}",
                "",
                "[wifi-security]",
                "key-mgmt=wpa-psk",
                f"psk={_keyfile_string(config['psk'])}",
                "",
            ]
        )
    lines.append("[ipv4]")
    if mode == "hotspot":
        lines.extend(
            [
                "method=shared",
                "address1=10.42.0.1/24",
                "never-default=true",
            ]
        )
    elif mode in {"wifi-client", "ethernet-dhcp"}:
        lines.extend(["method=auto", "may-fail=false", "route-metric=600"])
    else:
        address = str(config["address"])
        gateway = config.get("gateway")
        address_value = f"{address}{',' + str(gateway) if gateway else ''}"
        lines.extend(["method=manual", f"address1={address_value}"])
        dns = config.get("dns", [])
        if dns:
            lines.append(f"dns={';'.join(str(item) for item in dns)};")
        lines.append("never-default=true" if gateway is None else "never-default=false")
    lines.extend(["", "[ipv6]", "method=disabled", ""])
    return "\n".join(lines).encode()


def _avahi_service() -> bytes:
    """读取随包发布的 mDNS service 定义。

    同一份资产由安装器在首次安装时铺到 `/etc/avahi/services/`，因此默认安装无需先执行一次
    network apply 就已经广播；这里在每次 apply 时重写，用于自愈被删除或被改坏的文件。两条
    路径共用 `deploy/rp-ylx.avahi`，避免端口或服务类型出现两份真相。
    """
    packaged = Path(__file__).parent / "deploy" / MDNS_ASSET_NAME
    if packaged.is_file():
        return packaged.read_bytes()
    return resource_files("rp_ylx.deploy").joinpath(MDNS_ASSET_NAME).read_bytes()


def _run_nmcli(arguments: list[str], *, timeout: float = 35) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["nmcli", *arguments],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise NetworkError("network_timeout", "NetworkManager 操作超时") from exc
    except OSError as exc:
        raise NetworkError("network_manager_unavailable", "NetworkManager 不可用") from exc


def _device_snapshot(interface: str) -> dict[str, Any]:
    result = _run_nmcli(
        [
            "--terse",
            "--escape",
            "no",
            "--fields",
            "GENERAL.STATE,GENERAL.CONNECTION,IP4.ADDRESS,IP4.ROUTE",
            "device",
            "show",
            interface,
        ],
        timeout=10,
    )
    if result.returncode != 0:
        raise NetworkError("interface_unavailable", f"网络接口 {interface} 不可用")
    values: dict[str, list[str]] = {}
    for line in result.stdout.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        values.setdefault(key.split("[", 1)[0], []).append(value)
    state = values.get("GENERAL.STATE", [""])[0].split(" ", 1)[0]
    return {
        "connected": state == "100",
        "connection": values.get("GENERAL.CONNECTION", [""])[0],
        "addresses": [value for value in values.get("IP4.ADDRESS", []) if value],
        "routes": [value for value in values.get("IP4.ROUTE", []) if value],
    }


def _has_default_route(routes: object) -> bool:
    if not isinstance(routes, list):
        return False
    return any(
        isinstance(route, str)
        and (
            re.match(r"^\s*0\.0\.0\.0/0(?:\s|$)", route) is not None
            or re.search(r"(?:^|,\s*)dst\s*=\s*0\.0\.0\.0/0(?:\s*,|\s*$)", route) is not None
        )
        for route in routes
    )


def _activation_error(result: subprocess.CompletedProcess[str], mode: str) -> NetworkError:
    evidence = result.stderr.lower()
    if mode == "wifi-client" and any(
        marker in evidence for marker in ("secret", "password", "802-11-wireless-security")
    ):
        return NetworkError("wifi_auth_failed", "Wi-Fi 凭据被拒绝")
    if any(marker in evidence for marker in ("dhcp", "ip-config-unavailable", "timeout")):
        return NetworkError("dhcp_timeout", "未在期限内获得 DHCP 地址")
    return NetworkError("activation_failed", "NetworkManager 无法激活网络配置")


def _activate(
    profile: str,
    mode: str,
    *,
    expected_address: str | None = None,
    gateway_required: bool = False,
) -> dict[str, Any]:
    interface = _interface(mode)
    result = _run_nmcli(
        [
            "--wait",
            str(NETWORK_ACTIVATION_WAIT_SECONDS),
            "connection",
            "up",
            "id",
            profile,
            "ifname",
            interface,
        ],
        timeout=NETWORK_ACTIVATION_TIMEOUT_SECONDS,
    )
    if result.returncode != 0:
        raise _activation_error(result, mode)
    snapshot = _device_snapshot(interface)
    if not snapshot["connected"] or snapshot["connection"] != profile:
        raise NetworkError("activation_unverified", "NetworkManager 未确认目标连接")
    if mode == "hotspot":
        if "10.42.0.1/24" not in snapshot["addresses"]:
            raise NetworkError("hotspot_address_missing", "热点管理地址未生效")
    elif mode in {"wifi-client", "ethernet-dhcp"}:
        if not snapshot["addresses"]:
            raise NetworkError("dhcp_timeout", "未在期限内获得 DHCP 地址")
        if not _has_default_route(snapshot["routes"]):
            raise NetworkError("default_route_missing", "默认路由未生效")
    elif expected_address not in snapshot["addresses"]:
        raise NetworkError("static_address_mismatch", "有线静态地址未按配置生效")
    elif gateway_required and not _has_default_route(snapshot["routes"]):
        raise NetworkError("default_route_missing", "默认路由未生效")
    return snapshot


def _activate_saved(record: Mapping[str, Any], *, interface: str | None = None) -> None:
    profile = _valid_saved_profile(record.get("profile"))
    mode = record.get("mode")
    if profile is None or mode not in SUPPORTED_MODES:
        raise NetworkError("state_invalid", "已保存的网络配置无效")
    if interface is not None and _interface(str(mode)) != interface:
        raise NetworkError("state_invalid", "已保存的网络配置属于其他接口")
    profile_path = _profile_dir() / f"{profile}.nmconnection"
    try:
        metadata = profile_path.stat(follow_symlinks=False)
    except OSError as exc:
        raise NetworkError("profile_missing", "已保存的 NetworkManager profile 不存在") from exc
    if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise NetworkError("profile_permissions", "已保存的 NetworkManager profile 权限无效")
    config = record.get("config")
    expected_address = config.get("address") if isinstance(config, dict) else None
    gateway_required = isinstance(config, dict) and "gateway" in config
    _activate(
        profile,
        str(mode),
        expected_address=expected_address if isinstance(expected_address, str) else None,
        gateway_required=gateway_required,
    )


def _activate_previous(journal: Mapping[str, Any], interface: str) -> None:
    profile = journal.get("previous_profile")
    if not isinstance(profile, str) or not profile:
        raise NetworkError("previous_profile_missing", "事务前网络连接不存在")
    result = _run_nmcli(
        [
            "--wait",
            str(NETWORK_ACTIVATION_WAIT_SECONDS),
            "connection",
            "up",
            "id",
            profile,
            "ifname",
            interface,
        ],
        timeout=NETWORK_ACTIVATION_TIMEOUT_SECONDS,
    )
    if result.returncode != 0:
        raise NetworkError("previous_activation_failed", "无法恢复事务前网络连接")
    snapshot = _device_snapshot(interface)
    if not snapshot["connected"] or snapshot["connection"] != profile:
        raise NetworkError("previous_activation_unverified", "事务前网络连接恢复后未通过验证")
    previous_snapshot = journal.get("previous_snapshot")
    if previous_snapshot is None:
        return
    if not isinstance(previous_snapshot, dict):
        raise NetworkError("journal_invalid", "事务前网络状态无效")
    previous_addresses = previous_snapshot.get("addresses")
    previous_routes = previous_snapshot.get("routes")
    if not isinstance(previous_addresses, list) or not all(
        isinstance(item, str) for item in previous_addresses
    ):
        raise NetworkError("journal_invalid", "事务前网络地址状态无效")
    if not isinstance(previous_routes, list) or not all(
        isinstance(item, str) for item in previous_routes
    ):
        raise NetworkError("journal_invalid", "事务前网络路由状态无效")
    if previous_addresses and not snapshot["addresses"]:
        raise NetworkError("previous_address_missing", "事务前网络连接未恢复地址")
    had_default_route = _has_default_route(previous_routes)
    has_default_route = _has_default_route(snapshot["routes"])
    if had_default_route and not has_default_route:
        raise NetworkError("previous_route_missing", "事务前网络连接未恢复默认路由")


def _remove_candidate(path: Path) -> None:
    for candidate in [path, *path.parent.glob(f".{path.name}.*")]:
        with suppress(FileNotFoundError):
            candidate.unlink()
    _fsync_directory(path.parent)
    result = _run_nmcli(["connection", "reload"], timeout=10)
    if result.returncode != 0:
        raise NetworkError("reload_failed", "NetworkManager 无法清除候选连接")


def _recover_after_failure(
    error: NetworkError,
    *,
    state_dir: Path,
    journal_path: Path,
    profile_path: Path,
    journal: Mapping[str, Any],
) -> NetworkError:
    recovery = "unavailable"
    cleanup_failed = False
    try:
        _remove_candidate(profile_path)
    except (NetworkError, OSError):
        cleanup_failed = True

    interface = str(journal["interface"])
    lkg = _read_json(_lkg_path(state_dir, interface))
    if lkg is not None:
        try:
            _activate_saved(lkg, interface=interface)
            recovery = "lkg"
        except NetworkError:
            pass
    if recovery == "unavailable":
        try:
            _activate_previous(journal, interface)
            recovery = "previous"
        except NetworkError:
            pass
    if recovery == "unavailable" and interface == WIFI_INTERFACE:
        rescue = _read_json(state_dir / "rescue.json")
        if rescue is not None:
            try:
                _activate_saved(rescue)
                recovery = "rescue"
            except NetworkError:
                pass

    failed = dict(journal)
    failed.update(
        {
            "phase": "verifying",
            "outcome": "rolled_back"
            if recovery in {"lkg", "previous"}
            else "rescued"
            if recovery == "rescue"
            else "recovery_failed",
            "error_code": error.code,
            "recovery": recovery,
            "cleanup": "failed" if cleanup_failed else "complete",
        }
    )
    try:
        _write_json(journal_path, failed)
    except OSError as exc:
        raise NetworkError(
            "state_write_failed",
            "无法持久化网络恢复状态",
            recovery=recovery,
        ) from exc
    return NetworkError(
        error.code,
        error.message,
        exit_code=error.exit_code,
        recovery=recovery,
    )


def _journal_record(
    *,
    phase: str,
    request_id: str,
    fingerprint: str,
    mode: str,
    profile: str,
    previous_profile: str,
    previous_snapshot: Mapping[str, Any] | None = None,
    config: Mapping[str, Any] | None = None,
    outcome: str | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "format": JOURNAL_FORMAT,
        "phase": phase,
        "request_id": request_id,
        "request_fingerprint": fingerprint,
        "mode": mode,
        "interface": _interface(mode),
        "profile": profile,
        "previous_profile": previous_profile,
    }
    if previous_snapshot is not None:
        value["previous_snapshot"] = {
            "connected": bool(previous_snapshot.get("connected")),
            "connection": str(previous_snapshot.get("connection", "")),
            "addresses": list(previous_snapshot.get("addresses", [])),
            "routes": list(previous_snapshot.get("routes", [])),
        }
    if config is not None:
        value["config"] = _safe_config(config)
    if outcome is not None:
        value["outcome"] = outcome
    return value


def _materialize_commit(state_dir: Path, journal: Mapping[str, Any]) -> dict[str, Any]:
    request_id = journal.get("request_id")
    fingerprint = journal.get("request_fingerprint")
    mode = journal.get("mode")
    profile = _valid_saved_profile(journal.get("profile"))
    config = journal.get("config")
    if (
        journal.get("format") != JOURNAL_FORMAT
        or journal.get("phase") != "commit"
        or journal.get("outcome") != "committed"
        or not isinstance(request_id, str)
        or not isinstance(fingerprint, str)
        or mode not in SUPPORTED_MODES
        or profile is None
        or not isinstance(config, dict)
        or "psk" in config
    ):
        raise NetworkError("journal_invalid", "已提交网络事务内容无效")
    interface = _interface(str(mode))
    lkg = {
        "format": LKG_FORMAT,
        "mode": mode,
        "interface": interface,
        "profile": profile,
        "config": config,
    }
    result = {
        "format": RESULT_FORMAT,
        "ok": True,
        "request_id": request_id,
        "mode": mode,
        "replayed": False,
    }
    try:
        _write_json(_lkg_path(state_dir, interface), lkg)
        if mode == "hotspot":
            _write_json(
                state_dir / "rescue.json",
                {
                    "format": "ylx.network-rescue.v0",
                    "mode": mode,
                    "interface": interface,
                    "profile": profile,
                    "config": config,
                },
            )
        _write_json(
            _request_path(state_dir, request_id),
            {
                "format": "ylx.network-request-receipt.v0",
                "request_id": request_id,
                "request_fingerprint": fingerprint,
                "result": result,
            },
        )
    except OSError as exc:
        raise NetworkError(
            "commit_pending",
            "网络配置已提交，等待 network reconcile 完成持久化",
            recovery="reconcile",
        ) from exc
    return result


def _referenced_profiles(state_dir: Path, interface: str) -> set[str]:
    profiles: set[str] = set()
    paths = [_lkg_path(state_dir, interface)]
    if interface == WIFI_INTERFACE:
        paths.append(state_dir / "rescue.json")
    for path in paths:
        record = _read_json(path)
        if record is None:
            continue
        profile = _valid_saved_profile(record.get("profile"))
        if profile is None:
            raise NetworkError("state_invalid", f"网络状态文件 {path.name} 无效")
        profiles.add(profile)
    return profiles


def _profile_belongs_to_interface(path: Path, interface: str) -> bool:
    if interface == WIFI_INTERFACE:
        return path.stem.startswith(("rp-ylx-hotspot-", "rp-ylx-wifi-client-"))
    return path.stem.startswith(("rp-ylx-ethernet-dhcp-", "rp-ylx-ethernet-static-"))


def _unreferenced_profiles(state_dir: Path, interface: str) -> list[str]:
    profile_dir = _profile_dir()
    referenced = _referenced_profiles(state_dir, interface)
    return sorted(
        path.name
        for path in profile_dir.glob("rp-ylx-*.nmconnection")
        if _profile_belongs_to_interface(path, interface) and path.stem not in referenced
    )


def _prune_profiles(profile_names: list[str]) -> None:
    profile_dir = _profile_dir()
    for name in profile_names:
        if re.fullmatch(r"rp-ylx-[a-z-]+-[0-9a-f]{12}\.nmconnection", name) is None:
            raise NetworkError("journal_invalid", "待清理网络连接名称无效")
        with suppress(FileNotFoundError):
            (profile_dir / name).unlink()
    _fsync_directory(profile_dir)
    result = _run_nmcli(["connection", "reload"], timeout=10)
    if result.returncode != 0:
        raise NetworkError("reload_failed", "NetworkManager 无法清理旧连接")


def _settle_commit(
    state_dir: Path,
    journal_path: Path,
    journal: Mapping[str, Any],
) -> dict[str, Any]:
    result = _materialize_commit(state_dir, journal)
    if journal.get("cleanup") == "complete":
        return result
    interface = journal.get("interface")
    if interface not in {WIFI_INTERFACE, ETHERNET_INTERFACE}:
        raise NetworkError("journal_invalid", "已提交网络事务接口无效")
    cleanup_profiles = journal.get("cleanup_profiles")
    if cleanup_profiles is None:
        cleanup_profiles = _unreferenced_profiles(state_dir, str(interface))
        if not cleanup_profiles:
            try:
                _write_json(journal_path, {**journal, "cleanup": "complete"})
            except OSError as exc:
                raise NetworkError(
                    "cleanup_pending",
                    "旧网络连接清理状态尚未持久化，等待再次执行 network reconcile",
                    recovery="reconcile",
                ) from exc
            return result
        journal = {
            **journal,
            "cleanup": "pending",
            "cleanup_profiles": cleanup_profiles,
        }
        try:
            _write_json(journal_path, journal)
        except OSError as exc:
            raise NetworkError(
                "cleanup_pending",
                "旧网络连接清理清单尚未持久化，等待再次执行 network reconcile",
                recovery="reconcile",
            ) from exc
    if not isinstance(cleanup_profiles, list) or not all(
        isinstance(item, str) for item in cleanup_profiles
    ):
        raise NetworkError("journal_invalid", "待清理网络连接清单无效")
    try:
        _prune_profiles(cleanup_profiles)
        _write_json(journal_path, {**journal, "cleanup": "complete"})
    except (NetworkError, OSError) as exc:
        with suppress(OSError):
            _write_json(journal_path, {**journal, "cleanup": "failed"})
        raise NetworkError(
            "cleanup_pending",
            "旧网络连接尚未清理，等待再次执行 network reconcile",
            recovery="reconcile",
        ) from exc
    return result


def _settle_terminal_cleanup(
    journal_path: Path,
    journal: Mapping[str, Any],
) -> None:
    if journal.get("cleanup") != "failed":
        return
    profile = _valid_saved_profile(journal.get("profile"))
    mode = journal.get("mode")
    interface = journal.get("interface")
    if mode not in SUPPORTED_MODES or profile is None or interface != _interface(str(mode)):
        raise NetworkError("journal_invalid", "待清理网络事务内容无效")
    try:
        _remove_candidate(_profile_dir() / f"{profile}.nmconnection")
        _write_json(journal_path, {**journal, "cleanup": "complete"})
    except (NetworkError, OSError) as exc:
        raise NetworkError(
            "cleanup_pending",
            "候选网络连接尚未清理，等待再次执行 network reconcile",
            recovery="reconcile",
        ) from exc


def _settle_journal_before_apply(
    state_dir: Path,
    journal_path: Path,
    journal: Mapping[str, Any],
) -> None:
    if journal.get("format") != JOURNAL_FORMAT:
        raise NetworkError("journal_invalid", "网络事务 journal 格式无效")
    outcome = journal.get("outcome")
    if outcome in {"rolled_back", "rescued", "recovery_failed"}:
        _settle_terminal_cleanup(journal_path, journal)
        _remember_failed_request(state_dir, journal)
        return
    phase = journal.get("phase")
    if phase == "commit":
        _settle_commit(state_dir, journal_path, journal)
        return
    if phase not in {"prepared", "staging", "verifying"}:
        raise NetworkError("journal_invalid", "网络事务 journal 阶段无效")
    mode = journal.get("mode")
    profile = _valid_saved_profile(journal.get("profile"))
    interface = journal.get("interface")
    if mode not in SUPPORTED_MODES or profile is None or interface != _interface(str(mode)):
        raise NetworkError("journal_invalid", "网络事务 journal 内容无效")
    _recover_after_failure(
        NetworkError("interrupted", "网络事务在提交前中断"),
        state_dir=state_dir,
        journal_path=journal_path,
        profile_path=_profile_dir() / f"{profile}.nmconnection",
        journal=journal,
    )
    settled = _read_json(journal_path)
    if settled is None or settled.get("outcome") not in {
        "rolled_back",
        "rescued",
        "recovery_failed",
    }:
        raise NetworkError("state_write_failed", "无法确认网络恢复状态")
    _settle_terminal_cleanup(journal_path, settled)
    _remember_failed_request(state_dir, settled)


def _health_reason(record: Mapping[str, Any], snapshot: Mapping[str, Any]) -> str | None:
    profile = _valid_saved_profile(record.get("profile"))
    mode = record.get("mode")
    config = record.get("config")
    if profile is None or mode not in SUPPORTED_MODES or not isinstance(config, dict):
        raise NetworkError("state_invalid", "最后已验证网络配置无效")
    if not snapshot.get("connected") or snapshot.get("connection") != profile:
        return "profile_disconnected"
    addresses = snapshot.get("addresses")
    routes = snapshot.get("routes")
    if not isinstance(addresses, list) or not isinstance(routes, list):
        raise NetworkError("state_invalid", "NetworkManager 状态无效")
    if mode == "hotspot":
        return None if "10.42.0.1/24" in addresses else "hotspot_address_missing"
    if mode in {"wifi-client", "ethernet-dhcp"}:
        if not addresses:
            return "dhcp_timeout"
        return None if _has_default_route(routes) else "default_route_missing"
    expected_address = config.get("address")
    if expected_address not in addresses:
        return "static_address_mismatch"
    if "gateway" in config and not _has_default_route(routes):
        return "default_route_missing"
    return None


def _activate_rescue(state_dir: Path) -> dict[str, Any]:
    rescue = _read_json(state_dir / "rescue.json")
    if rescue is None:
        raise NetworkError("rescue_unconfigured", "设备尚未登记救援热点")
    try:
        _activate_saved(rescue, interface=WIFI_INTERFACE)
    except NetworkError as exc:
        raise NetworkError("rescue_failed", "无法激活救援热点") from exc
    return {
        "format": RESULT_FORMAT,
        "ok": True,
        "action": "rescue",
        "mode": "hotspot",
        "recovery": "rescue",
    }


def rescue_network() -> dict[str, Any]:
    state_dir = _state_dir()
    _prepare_state_dir(state_dir)
    with _network_lock(state_dir):
        return _activate_rescue(state_dir)


def _reconcile_lkg(state_dir: Path) -> dict[str, Any]:
    first_recovery: dict[str, Any] | None = None
    for interface in (WIFI_INTERFACE, ETHERNET_INTERFACE):
        lkg = _read_json(_lkg_path(state_dir, interface))
        if lkg is None:
            continue
        snapshot = _device_snapshot(interface)
        reason = _health_reason(lkg, snapshot)
        if reason is None:
            continue
        if interface == WIFI_INTERFACE and lkg.get("mode") == "wifi-client":
            rescue = _read_json(state_dir / "rescue.json")
            if rescue is not None and _health_reason(rescue, snapshot) is None:
                continue
            try:
                _activate_rescue(state_dir)
            except NetworkError as exc:
                raise NetworkError(
                    "reconcile_failed",
                    "Wi-Fi client 失联且救援热点无法激活",
                    recovery="unavailable",
                ) from exc
            recovery = "rescue"
        else:
            try:
                _activate_saved(lkg, interface=interface)
            except NetworkError as exc:
                raise NetworkError(
                    "reconcile_failed",
                    f"接口 {interface} 的最后已验证配置无法恢复",
                    recovery="unavailable",
                ) from exc
            recovery = "lkg"
        if first_recovery is None:
            first_recovery = {
                "format": RESULT_FORMAT,
                "ok": True,
                "action": "reconcile",
                "interrupted_phase": None,
                "recovery": recovery,
                "reason": reason,
            }
    return first_recovery or {
        "format": RESULT_FORMAT,
        "ok": True,
        "action": "reconcile",
        "interrupted_phase": None,
        "recovery": "unchanged",
    }


def _reconcile_locked(state_dir: Path) -> dict[str, Any]:
    journal_path = state_dir / "journal.json"
    journal = _read_json(journal_path)
    if journal is None:
        return _reconcile_lkg(state_dir)
    if journal.get("format") != JOURNAL_FORMAT:
        raise NetworkError("journal_invalid", "网络事务 journal 格式无效")
    if journal.get("outcome") == "recovery_failed":
        _settle_terminal_cleanup(journal_path, journal)
        raise NetworkError(
            "reconcile_failed",
            "上次网络事务未能恢复可访问连接",
            recovery="unavailable",
        )
    if journal.get("outcome") in {"rolled_back", "rescued"}:
        _settle_terminal_cleanup(journal_path, journal)
        return _reconcile_lkg(state_dir)
    phase = journal.get("phase")
    if phase == "commit":
        _settle_commit(state_dir, journal_path, journal)
        return _reconcile_lkg(state_dir)
    if phase not in {"prepared", "staging", "verifying"}:
        raise NetworkError("journal_invalid", "网络事务 journal 阶段无效")
    mode = journal.get("mode")
    profile = _valid_saved_profile(journal.get("profile"))
    interface = journal.get("interface")
    if mode not in SUPPORTED_MODES or profile is None or interface != _interface(str(mode)):
        raise NetworkError("journal_invalid", "网络事务 journal 内容无效")
    profile_path = _profile_dir() / f"{profile}.nmconnection"
    synthetic_error = NetworkError("interrupted", "网络事务在提交前中断")
    recovered = _recover_after_failure(
        synthetic_error,
        state_dir=state_dir,
        journal_path=journal_path,
        profile_path=profile_path,
        journal=journal,
    )
    if recovered.recovery == "unavailable":
        raise NetworkError(
            "reconcile_failed",
            "中断的网络事务无法恢复",
            recovery="unavailable",
        )
    return {
        "format": RESULT_FORMAT,
        "ok": True,
        "action": "reconcile",
        "interrupted_phase": phase,
        "recovery": recovered.recovery,
    }


def reconcile_network() -> dict[str, Any]:
    state_dir = _state_dir()
    _prepare_state_dir(state_dir)
    with _network_lock(state_dir):
        return _reconcile_locked(state_dir)


def apply_network(
    request_id: str,
    config_source: str,
    *,
    stdin: TextIO = sys.stdin,
) -> dict[str, Any]:
    if REQUEST_ID_PATTERN.fullmatch(request_id) is None:
        raise NetworkError("request_id_invalid", "request-id 格式无效", exit_code=2)
    config = _validate_config(_read_config(config_source, stdin))
    state_dir = _state_dir()
    _prepare_state_dir(state_dir)
    with _network_lock(state_dir):
        return _apply_network_locked(state_dir, request_id, config)


def _apply_network_locked(
    state_dir: Path,
    request_id: str,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    fingerprint = _fingerprint(config, _idempotency_key(state_dir))
    journal_path = state_dir / "journal.json"
    existing_journal = _read_json(journal_path)
    if existing_journal is not None:
        _settle_journal_before_apply(state_dir, journal_path, existing_journal)
        if (
            existing_journal.get("request_id") == request_id
            and existing_journal.get("request_fingerprint") != fingerprint
        ):
            raise NetworkError(
                "request_conflict",
                "同一 request-id 已用于不同网络配置",
                exit_code=2,
            )

    receipt_path = _request_path(state_dir, request_id)
    receipt = _read_json(receipt_path)
    if receipt is not None:
        if receipt.get("request_fingerprint") != fingerprint:
            raise NetworkError(
                "request_conflict",
                "同一 request-id 已用于不同网络配置",
                exit_code=2,
            )
        result = receipt.get("result")
        if isinstance(result, dict):
            return {**result, "replayed": True}

    mode = str(config["mode"])
    interface = _interface(mode)
    profile = _profile_name(request_id, mode)
    previous_snapshot = _device_snapshot(interface)
    previous = previous_snapshot["connection"]
    previous_profile = previous if isinstance(previous, str) else ""
    common = {
        "request_id": request_id,
        "fingerprint": fingerprint,
        "mode": mode,
        "profile": profile,
        "previous_profile": previous_profile,
        "previous_snapshot": previous_snapshot,
    }
    profile_path = _profile_dir() / f"{profile}.nmconnection"
    current = _journal_record(phase="prepared", **common)
    try:
        _write_json(journal_path, current)
        _write_atomic(profile_path, _network_manager_profile(profile, config), 0o600)
        _write_atomic(_avahi_dir() / MDNS_SERVICE_FILENAME, _avahi_service(), 0o644)
        current = _journal_record(phase="staging", **common)
        _write_json(journal_path, current)
        reload_result = _run_nmcli(["connection", "reload"], timeout=10)
        if reload_result.returncode != 0:
            raise NetworkError("reload_failed", "NetworkManager 无法重新加载连接")
        current = _journal_record(phase="verifying", **common)
        _write_json(journal_path, current)
        expected_address = config.get("address")
        _activate(
            profile,
            mode,
            expected_address=expected_address if isinstance(expected_address, str) else None,
            gateway_required="gateway" in config,
        )
    except (NetworkError, OSError) as exc:
        failure = (
            exc
            if isinstance(exc, NetworkError)
            else NetworkError("state_write_failed", "无法持久化候选网络配置")
        )
        recovered = _recover_after_failure(
            failure,
            state_dir=state_dir,
            journal_path=journal_path,
            profile_path=profile_path,
            journal=current,
        )
        failed = _read_json(journal_path)
        if failed is None:
            raise NetworkError(
                "state_write_failed",
                "无法确认网络恢复状态",
                recovery=recovered.recovery,
            ) from exc
        _remember_failed_request(state_dir, failed)
        raise recovered from exc
    committed = _journal_record(
        phase="commit",
        config=config,
        outcome="committed",
        **common,
    )
    committed["cleanup"] = "pending"
    try:
        _write_json(journal_path, committed)
    except OSError as exc:
        try:
            published = _read_json(journal_path)
        except NetworkError as read_error:
            raise NetworkError(
                "commit_pending",
                "无法确认网络配置提交点，已保留候选配置等待 network reconcile",
                recovery="reconcile",
            ) from read_error
        if published == committed:
            try:
                _fsync_directory(state_dir)
            except OSError as sync_error:
                raise NetworkError(
                    "commit_pending",
                    "网络配置提交点已发布，等待 network reconcile 确认持久化",
                    recovery="reconcile",
                ) from sync_error
            return _settle_commit(state_dir, journal_path, committed)
        if published != current:
            raise NetworkError(
                "commit_pending",
                "网络配置提交状态不明确，已保留候选配置等待 network reconcile",
                recovery="reconcile",
            ) from exc
        failure = NetworkError("state_write_failed", "无法持久化网络配置提交点")
        recovered = _recover_after_failure(
            failure,
            state_dir=state_dir,
            journal_path=journal_path,
            profile_path=profile_path,
            journal=current,
        )
        failed = _read_json(journal_path)
        if failed is None:
            raise NetworkError(
                "state_write_failed",
                "无法确认网络恢复状态",
                recovery=recovered.recovery,
            ) from exc
        _remember_failed_request(state_dir, failed)
        raise recovered from exc
    return _settle_commit(state_dir, journal_path, committed)


def network_status() -> dict[str, Any]:
    result = _run_nmcli(
        [
            "--terse",
            "--escape",
            "no",
            "--fields",
            "DEVICE,TYPE,STATE",
            "device",
            "status",
        ],
        timeout=10,
    )
    if result.returncode != 0:
        raise NetworkError("network_status_failed", "无法读取 NetworkManager 状态")
    devices = []
    for line in result.stdout.splitlines():
        fields = line.split(":", 2)
        if len(fields) == 3:
            devices.append({"interface": fields[0], "type": fields[1], "state": fields[2]})
    wifi_count = sum(device["type"] == "wifi" for device in devices)
    return {
        "format": NETWORK_STATUS_FORMAT,
        "capabilities": {
            "modes": SUPPORTED_MODES,
            "wifi_interface": WIFI_INTERFACE,
            "ethernet_interface": ETHERNET_INTERFACE,
            "second_wifi": wifi_count > 1,
        },
        "mdns": {
            "hostname": MDNS_HOSTNAME,
            "service": MDNS_SERVICE,
            "aliases": MDNS_SERVICE_ALIASES,
            "port": MDNS_PORT,
        },
        "devices": devices,
    }
