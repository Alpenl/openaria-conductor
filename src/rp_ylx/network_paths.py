"""Shared leaf helpers for network paths, OS locks, state bootstrapping, and RPC sealing."""

from __future__ import annotations

import errno
import fcntl
import grp
import ipaddress
import json
import os
import stat
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager, suppress
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from rp_ylx.clock_sync import valid_clock_response
from rp_ylx.core.fs import fsync_directory
from rp_ylx.core.ids import valid_uuid4
from rp_ylx.core.rpc import ControlRpcClient, RpcFramingError
from rp_ylx.network_validation import (
    NETWORK_CREDENTIAL_REF,
    WIFI_SECURITY,
    valid_desired_state,
    valid_transaction,
)

WIFI_INTERFACE = "wlan0"
ETHERNET_INTERFACE = "eth0"
SUPPORTED_MODES = ["hotspot", "wifi-client", "ethernet-dhcp", "ethernet-static"]
WIFI_SECURITY_LIST = ["open", "wpa2-personal", "wpa3-personal", "wpa2-wpa3-personal"]
NETWORK_OPERATION_LOCK_PATH = Path("/run/rp-ylx/network-operation.lock")
NETWORK_ACTIVATION_WAIT_SECONDS = 10
NETWORK_ACTIVATION_TIMEOUT_SECONDS = 12
LKG_FORMAT = "ylx.network-lkg.v0"

CONTROL_REQUEST_SCHEMA = "ylx.network-control-request.v1"
CONTROL_RESPONSE_SCHEMA = "ylx.network-control-response.v1"
MAX_CONTROL_REQUEST_BYTES = 64 * 1024
MAX_CONTROL_RESPONSE_BYTES = 64 * 1024
CONTROL_SOCKET_PATH = Path("/run/rp-ylx/network-control.sock")
CONTROL_SOCKET_TIMEOUT_SECONDS = 2.0
CONTROL_RESPONSE_TIMEOUT_SECONDS = (
    NETWORK_ACTIVATION_TIMEOUT_SECONDS + 4 * CONTROL_SOCKET_TIMEOUT_SECONDS
)
MUTATION_OPERATIONS = frozenset({"apply", "retry", "forget"})
ROOT_OPERATIONS = frozenset(
    {"create_credential", "health", "scan", "status", "clock_status", "clock_inspect", "clock_sync"}
)
SUPPORTED_OPERATIONS = MUTATION_OPERATIONS | ROOT_OPERATIONS
SECRET_FIELD_NAMES = frozenset({"password", "psk", "secret", "token"})
RESPONSE_SECRET_FIELD_NAMES = SECRET_FIELD_NAMES | {"credential", "passphrase"}

_OPERATION_LOCKS_GUARD = threading.Lock()
_OPERATION_LOCKS: dict[Path, threading.RLock] = {}
_OPERATION_LOCK_LOCAL = threading.local()

_RPC_CLIENT = ControlRpcClient(
    max_request_bytes=MAX_CONTROL_REQUEST_BYTES,
    max_response_bytes=MAX_CONTROL_RESPONSE_BYTES,
    default_timeout_seconds=CONTROL_SOCKET_TIMEOUT_SECONDS,
)


class NetworkError(RuntimeError):
    """Structured error raised by synchronous network state and NetworkManager operations."""

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


class NetworkControlClientError(RuntimeError):
    """Error raised when communicating with the privileged network-control socket."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


def state_dir() -> Path:
    """Return the persistent network controller state directory."""
    return Path(os.environ.get("RP_YLX_NETWORK_STATE_DIR", "/var/lib/rp-ylx-network"))


def profile_dir() -> Path:
    """Return the NetworkManager system-connections profile directory."""
    return Path(
        os.environ.get(
            "RP_YLX_NM_PROFILE_DIR",
            "/etc/NetworkManager/system-connections",
        )
    )


def avahi_dir() -> Path:
    """Return the Avahi static service definition directory."""
    return Path(os.environ.get("RP_YLX_AVAHI_SERVICE_DIR", "/etc/avahi/services"))


def ensure_directory(path: Path, mode: int) -> None:
    """Create directory with explicit permissions and fsync directory metadata."""
    existed = path.exists()
    path.mkdir(mode=mode, parents=True, exist_ok=True)
    os.chmod(path, mode)
    fsync_directory(path)
    if not existed:
        fsync_directory(path.parent)


def prepare_state_dir(path: Path) -> None:
    """Ensure the network state and requests directories exist with 0700 mode."""
    try:
        ensure_directory(path, 0o700)
        ensure_directory(path / "requests", 0o700)
    except OSError as exc:
        raise NetworkError("state_unwritable", "无法创建网络状态目录") from exc


def read_json(path: Path) -> dict[str, Any] | None:
    """Read a JSON object from path, returning None when absent."""
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
def network_lock(state_path: Path) -> Iterator[None]:
    """Acquire the exclusive advisory flock guarding state_path/.lock."""
    lock_path = state_path / ".lock"
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


def network_operation_lock_path() -> Path:
    """Return the cross-process lock path shared by capture and network mutation."""
    return Path(
        os.environ.get("RP_YLX_NETWORK_OPERATION_LOCK_PATH", str(NETWORK_OPERATION_LOCK_PATH))
    ).absolute()


@contextmanager
def _file_operation_lock(path: Path, *, blocking: bool) -> Iterator[None]:
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            0o660,
        )
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise OSError("network operation lock is not a regular file")
        if os.geteuid() == 0 or metadata.st_uid == os.geteuid():
            os.fchmod(descriptor, 0o660)
        if os.geteuid() == 0:
            with suppress(KeyError):
                gid = grp.getgrnam("rp-ylx").gr_gid
                if metadata.st_gid != gid:
                    os.fchown(descriptor, -1, gid)
    except OSError as exc:
        if descriptor >= 0:
            os.close(descriptor)
        raise NetworkError("operation_lock_failed", "无法锁定网络控制操作") from exc
    try:
        flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
        fcntl.flock(descriptor, flags)
    except BlockingIOError as exc:
        os.close(descriptor)
        raise NetworkError("capture_active", "录制期间不能修改网络") from exc
    except OSError as exc:
        os.close(descriptor)
        if not blocking and exc.errno in {errno.EAGAIN, errno.EWOULDBLOCK}:
            raise NetworkError("capture_active", "录制期间不能修改网络") from exc
        raise NetworkError("operation_lock_failed", "无法锁定网络控制操作") from exc
    try:
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


@contextmanager
def network_operation_lease(*, blocking: bool = True) -> Iterator[None]:
    """Hold the cross-process lease shared by capture and root network mutation."""
    path = network_operation_lock_path()
    try:
        path.parent.mkdir(mode=0o770, parents=True, exist_ok=True)
    except OSError as exc:
        raise NetworkError("operation_lock_failed", "无法创建网络控制锁目录") from exc
    with _OPERATION_LOCKS_GUARD:
        process_lock = _OPERATION_LOCKS.setdefault(path, threading.RLock())
    acquired = process_lock.acquire(blocking=blocking)
    if not acquired:
        raise NetworkError("capture_active", "录制期间不能修改网络")
    held_paths = getattr(_OPERATION_LOCK_LOCAL, "held_paths", None)
    if held_paths is None:
        held_paths = set()
        _OPERATION_LOCK_LOCAL.held_paths = held_paths
    try:
        if path in held_paths:
            yield
            return
        with _file_operation_lock(path, blocking=blocking):
            held_paths.add(path)
            try:
                yield
            finally:
                held_paths.remove(path)
    finally:
        process_lock.release()


@contextmanager
def network_operation_lock(
    state_path: Path | None = None,
    *,
    blocking: bool = True,
) -> Iterator[None]:
    """Acquire the shared network operation lease (state_path kept for compatibility)."""
    del state_path
    with network_operation_lease(blocking=blocking):
        yield


def interface_for_mode(mode: str) -> str:
    """Return the Linux network interface name managed for mode."""
    return WIFI_INTERFACE if mode in {"hotspot", "wifi-client"} else ETHERNET_INTERFACE


def lkg_path(state_path: Path, interface: str) -> Path:
    """Return the last-known-good JSON file path for interface."""
    if interface not in {WIFI_INTERFACE, ETHERNET_INTERFACE}:
        raise NetworkError("state_invalid", "最后已验证网络配置的接口无效")
    return state_path / f"lkg-{interface}.json"


def network_static_ipv4(config: Mapping[str, Any]) -> dict[str, Any]:
    """Project an internal static IPv4 config dict into v1 API static_ipv4 shape."""
    interface = ipaddress.IPv4Interface(str(config["address"]))
    gateway = config.get("gateway")
    dns = config.get("dns", [])
    return {
        "address": str(interface.ip),
        "prefix_length": interface.network.prefixlen,
        "gateway": str(gateway) if isinstance(gateway, str) else None,
        "dns": [str(item) for item in dns] if isinstance(dns, list) else [],
    }


def desired_state_from_config(config: Mapping[str, Any] | None) -> dict[str, Any]:
    """Project a legacy/on-disk config record into secret-free v1 desired state."""
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
        security = config.get("security", "wpa2-personal")
        if security not in WIFI_SECURITY_LIST:
            security = "wpa2-personal"
        wifi_client = {
            "ssid": ssid if isinstance(ssid, str) and ssid else "",
            "security": security,
            "credential_state": "absent" if security == "open" else "stored",
        }
    elif mode == "ethernet-dhcp":
        ethernet = {"addressing": "dhcp", "static_ipv4": None}
    elif mode == "ethernet-static":
        try:
            static_ipv4 = network_static_ipv4(config)
        except (KeyError, ValueError):
            static_ipv4 = None
        ethernet = {"addressing": "static", "static_ipv4": static_ipv4}
    return {
        "mode": mode,
        "wifi_client": wifi_client,
        "ethernet": ethernet,
    }


def desired_state_from_disk(state_path: Path) -> dict[str, Any]:
    """Read the highest-priority persisted desired state from LKG or rescue files."""
    for path in (
        lkg_path(state_path, WIFI_INTERFACE),
        lkg_path(state_path, ETHERNET_INTERFACE),
        state_path / "rescue.json",
    ):
        record = read_json(path)
        if record is None:
            continue
        config = record.get("config")
        if isinstance(config, Mapping):
            return desired_state_from_config(config)
    return desired_state_from_config(None)


def error_response(
    code: str,
    message: str,
    *,
    operation: str | None = None,
    retryable: bool = False,
) -> dict[str, Any]:
    """Construct a closed ylx.network-control-response.v1 error payload."""
    response: dict[str, Any] = {
        "schema": CONTROL_RESPONSE_SCHEMA,
        "ok": False,
        "error": {
            "code": code,
            "message": message,
        },
        "retryable": retryable,
    }
    if operation is not None:
        response["operation"] = operation
    return response


def response_contains_secret_field(value: object) -> bool:
    """Return True if value recursively contains any forbidden secret key name."""
    if isinstance(value, Mapping):
        return any(
            isinstance(key, str)
            and key.casefold() in RESPONSE_SECRET_FIELD_NAMES
            or response_contains_secret_field(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(response_contains_secret_field(item) for item in value)
    return False


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def valid_capability(value: object) -> bool:
    """Validate the controller mutation capability projection."""
    return (
        isinstance(value, Mapping)
        and set(value)
        == {
            "enabled",
            "disabled_reason",
            "operations",
            "idempotency_key_required",
            "secret_handling",
            "active_state_policy",
        }
        and type(value.get("enabled")) is bool
        and (
            value.get("disabled_reason") is None
            if value.get("enabled") is True
            else value.get("disabled_reason")
            in {
                "not_enabled",
                "auth_profile_unavailable",
                "controller_unavailable",
                "network_manager_unavailable",
                "rescue_ap_not_validated",
                "capture_active",
                "recovery_required",
                "maintenance_window_closed",
                "unsupported_concurrency",
            }
        )
        and value.get("operations") == ["apply", "retry", "forget"]
        and value.get("idempotency_key_required") is True
        and value.get("secret_handling") == "opaque_credential_reference_only"
        and value.get("active_state_policy") == "idle_only"
    )


def valid_scan_body(value: object) -> bool:
    """Validate a redacted ylx.network-scan.v1 response body."""
    if not isinstance(value, Mapping) or set(value) != {
        "schema",
        "authority_epoch",
        "source_revision",
        "scanned_at",
        "networks",
    }:
        return False
    networks = value.get("networks")
    if not isinstance(networks, list) or len(networks) > 256:
        return False
    for entry in networks:
        if not isinstance(entry, Mapping) or set(entry) != {
            "ssid",
            "hidden",
            "security",
            "signal_dbm",
            "credential_required",
        }:
            return False
        ssid = entry.get("ssid")
        hidden = entry.get("hidden")
        security = entry.get("security")
        signal = entry.get("signal_dbm")
        if (
            type(hidden) is not bool
            or security not in WIFI_SECURITY
            or type(signal) is not int
            or not -127 <= signal <= 0
            or type(entry.get("credential_required")) is not bool
            or entry["credential_required"] != (security != "open")
            or hidden
            and ssid is not None
            or not hidden
            and (not isinstance(ssid, str) or not 1 <= len(ssid.encode("utf-8")) <= 32)
        ):
            return False
    return (
        value.get("schema") == "ylx.network-scan.v1"
        and valid_uuid4(value.get("authority_epoch"))
        and type(value.get("source_revision")) is int
        and value["source_revision"] >= 0
        and isinstance(value.get("scanned_at"), str)
        and bool(value["scanned_at"])
    )


def valid_controller_success(response: Mapping[str, Any]) -> bool:
    """Validate a successful network-control response against its closed operation contract."""
    operation = response.get("operation")
    if operation in {"clock_status", "clock_inspect", "clock_sync"}:
        return (
            set(response) == {"schema", "ok", "operation", "status", "body"}
            and response.get("status") == 200
            and valid_clock_response(response.get("body"))
        )
    if operation == "health":
        capabilities = response.get("capabilities")
        return (
            set(response) == {"schema", "ok", "operation", "capabilities"}
            and isinstance(capabilities, Mapping)
            and set(capabilities) == {"mutation_enabled", "operations", "secret_handling"}
            and type(capabilities.get("mutation_enabled")) is bool
            and capabilities.get("operations") == ["apply", "forget", "retry"]
            and capabilities.get("secret_handling") == "opaque_credential_reference_only"
        )
    if operation == "create_credential":
        body = response.get("body")
        issued_at = _parse_timestamp(body.get("issued_at")) if isinstance(body, Mapping) else None
        expires_at = _parse_timestamp(body.get("expires_at")) if isinstance(body, Mapping) else None
        return (
            set(response) == {"schema", "ok", "operation", "status", "body"}
            and response.get("status") == 201
            and isinstance(body, Mapping)
            and set(body)
            == {
                "schema",
                "credential_ref",
                "issued_at",
                "expires_at",
                "ttl_seconds",
                "single_use",
            }
            and body.get("schema") == "ylx.network-credential-receipt.v1"
            and isinstance(body.get("credential_ref"), str)
            and NETWORK_CREDENTIAL_REF.fullmatch(str(body["credential_ref"])) is not None
            and issued_at is not None
            and expires_at is not None
            and type(body.get("ttl_seconds")) is int
            and 1 <= body["ttl_seconds"] <= 120
            and body.get("single_use") is True
            and expires_at - issued_at == timedelta(seconds=body["ttl_seconds"])
        )
    if operation == "status":
        body = response.get("body")
        transaction = body.get("transaction") if isinstance(body, Mapping) else None
        capability = body.get("capability") if isinstance(body, Mapping) else None
        return (
            set(response) == {"schema", "ok", "operation", "status", "body"}
            and response.get("status") == 200
            and isinstance(body, Mapping)
            and set(body)
            == {
                "schema",
                "authority_epoch",
                "source_revision",
                "saved",
                "verified",
                "desired",
                "transaction",
                "capability",
            }
            and body.get("schema") == "ylx.network-controller-status.v1"
            and valid_uuid4(body.get("authority_epoch"))
            and type(body.get("source_revision")) is int
            and body["source_revision"] >= 0
            and type(body.get("saved")) is bool
            and type(body.get("verified")) is bool
            and (not body["verified"] or body["saved"])
            and valid_desired_state(body.get("desired"))
            and isinstance(transaction, Mapping)
            and set(transaction) == {"current", "latest"}
            and all(
                item is None or valid_transaction(item)
                for item in (transaction.get("current"), transaction.get("latest"))
            )
            and all(
                item is None
                or item["authority_epoch"] == body["authority_epoch"]
                and item["source_revision"] <= body["source_revision"]
                for item in (transaction.get("current"), transaction.get("latest"))
            )
            and valid_capability(capability)
        )
    if operation == "scan":
        return (
            set(response) == {"schema", "ok", "operation", "status", "body"}
            and response.get("status") == 200
            and valid_scan_body(response.get("body"))
        )
    if operation in MUTATION_OPERATIONS:
        body = response.get("body")
        return (
            set(response) == {"schema", "ok", "operation", "status", "body", "replayed"}
            and response.get("status") == 202
            and isinstance(body, Mapping)
            and set(body) == {"schema", "accepted_at", "transaction"}
            and body.get("schema") == "ylx.network-transaction-receipt.v1"
            and valid_transaction(body.get("transaction"))
            and body.get("accepted_at") == body["transaction"]["accepted_at"]
            and type(response.get("replayed")) is bool
        )
    return False


def seal_response(response: dict[str, Any]) -> dict[str, Any]:
    """Validate and secret-redact a controller response, failing closed on violations."""
    if response.get("schema") != CONTROL_RESPONSE_SCHEMA or type(response.get("ok")) is not bool:
        return error_response("response_invalid", "network-control response was rejected")
    if response["ok"] is False:
        allowed = {"schema", "ok", "error", "retryable"}
        if "operation" in response:
            allowed.add("operation")
        error = response.get("error")
        valid = (
            set(response) == allowed
            and isinstance(error, Mapping)
            and set(error) == {"code", "message"}
            and isinstance(error.get("code"), str)
            and isinstance(error.get("message"), str)
            and type(response.get("retryable")) is bool
        )
    else:
        valid = valid_controller_success(response)
    if not valid or response_contains_secret_field(response):
        return error_response("response_invalid", "network-control response was rejected")
    return response


def request_control(
    operation: str,
    *,
    principal_id: str | None = None,
    idempotency_key: str | None = None,
    body: Mapping[str, object] | None = None,
    socket_path: Path = CONTROL_SOCKET_PATH,
    timeout_seconds: float = CONTROL_SOCKET_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Send one bounded NDJSON request to the privileged network-control socket."""
    request: dict[str, object] = {
        "schema": CONTROL_REQUEST_SCHEMA,
        "operation": operation,
    }
    if operation in MUTATION_OPERATIONS:
        if principal_id is None or idempotency_key is None or body is None:
            raise NetworkControlClientError(
                "request_invalid",
                "network-control mutation request is incomplete",
            )
        request.update(
            {
                "principal_id": principal_id,
                "idempotency_key": idempotency_key,
                "body": dict(body),
            }
        )
    elif operation in {"create_credential", "clock_status", "clock_inspect", "clock_sync"}:
        if principal_id is None or body is None:
            raise NetworkControlClientError(
                "request_invalid",
                "network credential request is incomplete",
            )
        request.update({"principal_id": principal_id, "body": dict(body)})
    try:
        response = _RPC_CLIENT.request(socket_path, request, timeout_seconds=timeout_seconds)
    except RpcFramingError as exc:
        if "size limit" in str(exc):
            raise NetworkControlClientError(
                "response_too_large",
                "network-control response exceeded the size limit",
            ) from exc
        raise NetworkControlClientError(
            "response_invalid",
            "network-control response is not valid JSON",
        ) from exc
    except OSError as exc:
        raise NetworkControlClientError(
            "network_controller_unavailable",
            "network-control socket is unavailable",
        ) from exc
    if response.get("schema") != CONTROL_RESPONSE_SCHEMA:
        raise NetworkControlClientError(
            "response_invalid",
            "network-control response schema is invalid",
        )
    return seal_response(response)


def now_iso() -> str:
    """Return current UTC timestamp formatted as ISO-8601 Z."""
    from rp_ylx.core.time import utc_now_iso

    return utc_now_iso()


def monotonic_deadline_timeout(
    deadline_ns: int, monotonic_ns: Any, maximum_seconds: float
) -> float:
    """Compute remaining seconds until deadline_ns, raising NetworkError if expired."""
    remaining_ns = deadline_ns - monotonic_ns()
    if remaining_ns <= 0:
        raise NetworkError("network_timeout", "网络激活超过十秒期限")
    return min(maximum_seconds, remaining_ns / 1_000_000_000)


__all__ = [
    "CONTROL_REQUEST_SCHEMA",
    "CONTROL_RESPONSE_SCHEMA",
    "CONTROL_RESPONSE_TIMEOUT_SECONDS",
    "CONTROL_SOCKET_PATH",
    "CONTROL_SOCKET_TIMEOUT_SECONDS",
    "ETHERNET_INTERFACE",
    "LKG_FORMAT",
    "MAX_CONTROL_REQUEST_BYTES",
    "MAX_CONTROL_RESPONSE_BYTES",
    "MUTATION_OPERATIONS",
    "NETWORK_ACTIVATION_TIMEOUT_SECONDS",
    "NETWORK_ACTIVATION_WAIT_SECONDS",
    "NETWORK_OPERATION_LOCK_PATH",
    "RESPONSE_SECRET_FIELD_NAMES",
    "ROOT_OPERATIONS",
    "SECRET_FIELD_NAMES",
    "SUPPORTED_MODES",
    "SUPPORTED_OPERATIONS",
    "WIFI_INTERFACE",
    "WIFI_SECURITY_LIST",
    "NetworkControlClientError",
    "NetworkError",
    "avahi_dir",
    "desired_state_from_config",
    "desired_state_from_disk",
    "ensure_directory",
    "error_response",
    "interface_for_mode",
    "lkg_path",
    "monotonic_deadline_timeout",
    "network_lock",
    "network_operation_lease",
    "network_operation_lock",
    "network_operation_lock_path",
    "network_static_ipv4",
    "now_iso",
    "prepare_state_dir",
    "profile_dir",
    "read_json",
    "request_control",
    "response_contains_secret_field",
    "seal_response",
    "state_dir",
    "valid_capability",
    "valid_controller_success",
    "valid_scan_body",
]
