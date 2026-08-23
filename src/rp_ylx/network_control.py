"""Root-owned, socket-activated network mutation controller."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import queue
import re
import socket
import sys
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import ExitStack, nullcontext, suppress
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, TextIO

from rp_ylx.network import (
    NetworkError,
    _network_operation_lock,
    _state_dir,
    activate_network_candidate,
    cleanup_orphan_network_candidates,
    commit_network_candidate,
    discard_network_candidate,
    ensure_rescue_ap,
    forget_network_client_profiles,
    prepare_network_candidate,
    rescue_network,
    saved_network_candidate,
    saved_network_is_healthy,
    scan_wifi_networks,
)
from rp_ylx.network_credentials import NetworkCredentialError, NetworkCredentialStore
from rp_ylx.network_state import (
    NetworkStateError,
    NetworkStateStore,
    valid_desired_state,
    valid_transaction,
)

CONTROL_REQUEST_SCHEMA = "ylx.network-control-request.v1"
CONTROL_RESPONSE_SCHEMA = "ylx.network-control-response.v1"
MAX_CONTROL_REQUEST_BYTES = 64 * 1024
MAX_CONTROL_RESPONSE_BYTES = 64 * 1024
CONTROL_SOCKET_PATH = Path("/run/rp-ylx/network-control.sock")
CONTROL_SOCKET_TIMEOUT_SECONDS = 2.0
NETWORK_HEALTH_POLL_SECONDS = 1.0
NETWORK_HEALTH_FAILURE_SECONDS = 10.0
NETWORK_RESCUE_REACHABILITY_SECONDS = 15.0
NETWORK_MODES = frozenset({"hotspot", "wifi-client", "ethernet-dhcp", "ethernet-static"})
MUTATION_OPERATIONS = frozenset({"apply", "retry", "forget"})
ROOT_OPERATIONS = frozenset({"create_credential", "health", "scan", "status"})
SUPPORTED_OPERATIONS = MUTATION_OPERATIONS | ROOT_OPERATIONS
SECRET_FIELD_NAMES = frozenset({"password", "psk", "secret", "token"})
RESPONSE_SECRET_FIELD_NAMES = SECRET_FIELD_NAMES | {"credential", "passphrase"}
NETWORK_CREDENTIAL_REF = re.compile(r"^cred-[A-Za-z0-9_.:-]+$")
UUID_V7 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
WIFI_SECURITY = frozenset({"open", "wpa2-personal", "wpa3-personal", "wpa2-wpa3-personal"})


class NetworkControlClientError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


def _error_response(
    code: str,
    message: str,
    *,
    operation: str | None = None,
    retryable: bool = False,
) -> dict[str, Any]:
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


def _contains_inline_secret(value: object) -> bool:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if isinstance(key, str) and key.casefold() in SECRET_FIELD_NAMES:
                return True
            if _contains_inline_secret(item):
                return True
    elif isinstance(value, list):
        return any(_contains_inline_secret(item) for item in value)
    return False


def _valid_idempotency_key(value: object) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value) <= 128
        and all(character != " " and 0x21 <= ord(character) <= 0x7E for character in value)
    )


def _valid_principal_id(value: object) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value) <= 128
        and all(0x21 <= ord(character) <= 0x7E for character in value)
    )


def _valid_ipv4(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        ipaddress.IPv4Address(value)
    except ValueError:
        return False
    return True


def _valid_network_static_ipv4(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "address",
        "prefix_length",
        "gateway",
        "dns",
    }:
        return False
    dns = value.get("dns")
    return (
        _valid_ipv4(value.get("address"))
        and type(value.get("prefix_length")) is int
        and 1 <= value["prefix_length"] <= 32
        and (value.get("gateway") is None or _valid_ipv4(value.get("gateway")))
        and isinstance(dns, list)
        and len(dns) <= 3
        and all(_valid_ipv4(item) for item in dns)
        and len(set(dns)) == len(dns)
    )


def _valid_network_ethernet(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {"addressing", "static_ipv4"}:
        return False
    addressing = value.get("addressing")
    static = value.get("static_ipv4")
    return (addressing == "dhcp" and static is None) or (
        addressing == "static" and _valid_network_static_ipv4(static)
    )


def _valid_network_apply_wifi_client(value: object) -> bool:
    if not isinstance(value, Mapping) or not {"ssid", "security"}.issubset(value):
        return False
    security = value.get("security")
    expected_keys = (
        {"ssid", "security"} if security == "open" else {"ssid", "security", "credential_ref"}
    )
    if set(value) != expected_keys or security not in WIFI_SECURITY:
        return False
    ssid = value.get("ssid")
    credential_ref = value.get("credential_ref")
    return (
        isinstance(ssid, str)
        and 1 <= len(ssid.encode("utf-8")) <= 32
        and (
            security == "open"
            or isinstance(credential_ref, str)
            and 1 <= len(credential_ref) <= 128
            and NETWORK_CREDENTIAL_REF.fullmatch(credential_ref) is not None
        )
    )


def _valid_network_apply_desired_state(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {"mode", "wifi_client", "ethernet"}:
        return False
    mode = value.get("mode")
    wifi = value.get("wifi_client")
    ethernet = value.get("ethernet")
    if mode not in NETWORK_MODES:
        return False
    if mode == "wifi-client":
        if not _valid_network_apply_wifi_client(wifi):
            return False
    elif wifi is not None:
        return False
    if ethernet is not None and not _valid_network_ethernet(ethernet):
        return False
    return not (mode == "ethernet-static" and ethernet is None)


def _valid_network_body(operation: str, body: object) -> bool:
    if not isinstance(body, Mapping):
        return False
    if operation == "apply":
        return (
            set(body) == {"schema", "desired"}
            and body.get("schema") == "ylx.network-apply-request.v1"
            and _valid_network_apply_desired_state(body.get("desired"))
        )
    if operation == "retry":
        return (
            set(body) == {"schema", "transaction_id"}
            and body.get("schema") == "ylx.network-retry-request.v1"
            and isinstance(body.get("transaction_id"), str)
            and UUID_V7.fullmatch(str(body["transaction_id"])) is not None
        )
    return set(body) == {"schema"} and body.get("schema") == "ylx.network-forget-request.v1"


def _valid_control_request(operation: str, request: Mapping[str, object]) -> bool:
    if operation in {"health", "scan", "status"}:
        return set(request) == {"schema", "operation"}
    if operation == "create_credential":
        body = request.get("body")
        return (
            set(request) == {"schema", "operation", "principal_id", "body"}
            and _valid_principal_id(request.get("principal_id"))
            and isinstance(body, Mapping)
            and set(body) == {"schema", "passphrase"}
            and body.get("schema") == "ylx.network-credential-request.v1"
            and isinstance(body.get("passphrase"), str)
            and 8 <= len(str(body["passphrase"]).encode("utf-8")) <= 63
            and not any(ord(character) < 32 for character in str(body["passphrase"]))
        )
    return (
        set(request) == {"schema", "operation", "principal_id", "idempotency_key", "body"}
        and _valid_principal_id(request.get("principal_id"))
        and _valid_idempotency_key(request.get("idempotency_key"))
        and _valid_network_body(operation, request.get("body"))
    )


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _uuid7() -> str:
    milliseconds = int(time.time_ns() // 1_000_000) & ((1 << 48) - 1)
    random_bits = int.from_bytes(os.urandom(10), "big")
    random_a = random_bits >> 68
    random_b = random_bits & ((1 << 62) - 1)
    value = milliseconds << 80 | 0x7 << 76 | random_a << 64 | 0x2 << 62 | random_b
    return str(uuid.UUID(int=value))


def _request_fingerprint(operation: str, body: Mapping[str, object]) -> str:
    rendered = json.dumps(
        {"operation": operation, "body": body},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(rendered.encode()).hexdigest()


def _response_contains_secret_field(value: object) -> bool:
    if isinstance(value, Mapping):
        return any(
            isinstance(key, str)
            and key.casefold() in RESPONSE_SECRET_FIELD_NAMES
            or _response_contains_secret_field(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_response_contains_secret_field(item) for item in value)
    return False


def _valid_transaction_shape(value: object) -> bool:
    return valid_transaction(value)


def _valid_uuid4(value: object) -> bool:
    try:
        parsed = uuid.UUID(str(value))
    except (AttributeError, ValueError):
        return False
    return parsed.version == 4 and str(parsed) == value


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _valid_capability(value: object) -> bool:
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


def _valid_scan_body(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "schema",
        "authority_epoch",
        "source_revision",
        "scanned_at",
        "networks",
    }:
        return False
    try:
        authority = uuid.UUID(str(value.get("authority_epoch")))
    except (AttributeError, ValueError):
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
        and authority.version == 4
        and str(authority) == value.get("authority_epoch")
        and type(value.get("source_revision")) is int
        and value["source_revision"] >= 0
        and isinstance(value.get("scanned_at"), str)
        and bool(value["scanned_at"])
    )


def _valid_controller_success(response: Mapping[str, Any]) -> bool:
    operation = response.get("operation")
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
            and _valid_uuid4(body.get("authority_epoch"))
            and type(body.get("source_revision")) is int
            and body["source_revision"] >= 0
            and type(body.get("saved")) is bool
            and type(body.get("verified")) is bool
            and (not body["verified"] or body["saved"])
            and valid_desired_state(body.get("desired"))
            and isinstance(transaction, Mapping)
            and set(transaction) == {"current", "latest"}
            and all(
                item is None or _valid_transaction_shape(item)
                for item in (transaction.get("current"), transaction.get("latest"))
            )
            and all(
                item is None
                or item["authority_epoch"] == body["authority_epoch"]
                and item["source_revision"] <= body["source_revision"]
                for item in (transaction.get("current"), transaction.get("latest"))
            )
            and _valid_capability(capability)
        )
    if operation == "scan":
        return (
            set(response) == {"schema", "ok", "operation", "status", "body"}
            and response.get("status") == 200
            and _valid_scan_body(response.get("body"))
        )
    if operation in MUTATION_OPERATIONS:
        body = response.get("body")
        return (
            set(response) == {"schema", "ok", "operation", "status", "body", "replayed"}
            and response.get("status") == 202
            and isinstance(body, Mapping)
            and set(body) == {"schema", "accepted_at", "transaction"}
            and body.get("schema") == "ylx.network-transaction-receipt.v1"
            and _valid_transaction_shape(body.get("transaction"))
            and body.get("accepted_at") == body["transaction"]["accepted_at"]
            and type(response.get("replayed")) is bool
        )
    return False


def _seal_response(response: dict[str, Any]) -> dict[str, Any]:
    if response.get("schema") != CONTROL_RESPONSE_SCHEMA or type(response.get("ok")) is not bool:
        return _error_response("response_invalid", "network-control response was rejected")
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
        valid = _valid_controller_success(response)
    if not valid or _response_contains_secret_field(response):
        return _error_response("response_invalid", "network-control response was rejected")
    return response


def _controller_device_id() -> str:
    path = Path(os.environ.get("RP_YLX_MACHINE_ID_PATH", "/etc/machine-id"))
    try:
        value = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError) as exc:
        raise NetworkError("device_id_unavailable", "无法读取设备身份") from exc
    if not value or len(value.encode()) > 256:
        raise NetworkError("device_id_unavailable", "设备身份无效")
    return value


def _controller_boot_id() -> str:
    path = Path(os.environ.get("RP_YLX_BOOT_ID_PATH", "/proc/sys/kernel/random/boot_id"))
    try:
        value = path.read_text(encoding="utf-8").strip()
        parsed = uuid.UUID(value)
    except (OSError, UnicodeError, ValueError) as exc:
        raise NetworkError("boot_id_unavailable", "无法读取 Linux boot ID") from exc
    if str(parsed) != value:
        raise NetworkError("boot_id_unavailable", "Linux boot ID 无效")
    return value


class NetworkController:
    """Root-owned exclusive-mode state machine around the synchronous NM writer."""

    def __init__(
        self,
        *,
        device_id: str | None = None,
        credential_store: NetworkCredentialStore | None = None,
        start_worker: bool = True,
        require_root: bool = True,
        defer_execution_until_response: bool = False,
        health_poll_seconds: float = NETWORK_HEALTH_POLL_SECONDS,
        health_failure_seconds: float = NETWORK_HEALTH_FAILURE_SECONDS,
        monotonic_ns: Callable[[], int] | None = None,
    ) -> None:
        if require_root and os.geteuid() != 0:
            raise NetworkError("root_required", "network controller must run as root")
        if health_poll_seconds <= 0 or health_failure_seconds <= 0:
            raise ValueError("network health intervals must be positive")
        self._credentials = credential_store or NetworkCredentialStore()
        self._boot_id = _controller_boot_id()
        self._state = NetworkStateStore(_state_dir())
        with _network_operation_lock():
            self._rescue = ensure_rescue_ap(device_id or _controller_device_id())
        self._submit_lock = threading.Lock()
        self._condition = threading.Condition()
        self._queue: queue.Queue[str | None] = queue.Queue()
        self._queued: set[str] = set()
        self._deferred: set[str] = set()
        self._pending = 0
        self._thread: threading.Thread | None = None
        self._monitor_thread: threading.Thread | None = None
        self._monitor_stop = threading.Event()
        self._health_poll_seconds = health_poll_seconds
        self._health_failure_seconds = health_failure_seconds
        self._monotonic_ns = monotonic_ns or time.monotonic_ns
        self._health_failure_started_ns: int | None = None
        self._health_fallback_latched = False
        self._closed = False
        self._defer_execution_until_response = defer_execution_until_response
        self._boot_reconcile()
        with _network_operation_lock():
            cleanup_orphan_network_candidates(self._state.retained_profiles())
        if start_worker:
            self.start()

    def _accept_system_transaction(
        self,
        *,
        desired: Mapping[str, Any],
        work: Mapping[str, Any],
        reason: str,
        saved: bool,
        verified: bool,
    ) -> str:
        transaction_id = _uuid7()
        idempotency_key = f"system-{reason}-{transaction_id}"
        request_fingerprint = hashlib.sha256(
            json.dumps(
                {"reason": reason, "desired": desired},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        receipt, replayed = self._state.accept_transaction(
            operation="retry",
            desired=desired,
            principal_id="system-network-controller",
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            transaction_id=transaction_id,
            accepted_at=_now(),
            work=work,
            publish_desired=True,
            saved=saved,
            verified=verified,
        )
        if replayed:
            transaction = receipt.get("transaction")
            if isinstance(transaction, Mapping):
                return str(transaction["transaction_id"])
        return transaction_id

    def _retained_retry_work(self, snapshot: Mapping[str, Any]) -> dict[str, Any] | None:
        latest = snapshot.get("transaction", {}).get("latest")
        if not isinstance(latest, Mapping) or latest.get("status") not in {"rescued", "failed"}:
            return None
        try:
            source = self._state.work_for(str(latest["transaction_id"]))
        except (KeyError, NetworkStateError):
            return None
        candidate = source.get("candidate")
        desired = source.get("desired")
        if not isinstance(candidate, Mapping) or not isinstance(desired, Mapping):
            return None
        cleanup = source.get("cleanup_work_ids", [])
        cleanup_ids = (
            [item for item in cleanup if isinstance(item, str)] if isinstance(cleanup, list) else []
        )
        return {
            "kind": "candidate",
            "candidate": deepcopy(dict(candidate)),
            "desired": deepcopy(dict(desired)),
            "cleanup_work_ids": [str(latest["transaction_id"]), *cleanup_ids],
        }

    def _boot_reconcile(self) -> None:
        snapshot = self._state.snapshot()
        current = snapshot["transaction"]["current"]
        desired = snapshot["desired"]
        mode = str(desired["mode"])
        new_boot = self._state.boot_requires_rescue(self._boot_id)
        rescue_healthy = False
        if new_boot:
            with _network_operation_lock():
                rescue_network()
                self._state.mark_boot_rescue_validated(self._boot_id)
            rescue_healthy = True
        if isinstance(current, Mapping):
            return
        try:
            with _network_operation_lock():
                if saved_network_is_healthy(mode):
                    return
        except NetworkError:
            pass
        if mode == "hotspot":
            if not rescue_healthy:
                with _network_operation_lock():
                    rescue_network()
            return

        latest = snapshot["transaction"]["latest"]
        if isinstance(latest, Mapping) and latest.get("status") in {"rescued", "failed"}:
            if not rescue_healthy:
                with _network_operation_lock():
                    rescue_network()
            return

        try:
            with _network_operation_lock():
                candidate = saved_network_candidate(mode)
        except NetworkError:
            if not rescue_healthy:
                with _network_operation_lock():
                    rescue_network()
            return
        work = {
            "kind": "candidate",
            "candidate": candidate,
            "desired": deepcopy(desired),
            "cleanup_work_ids": [],
            "rescue_ready": rescue_healthy,
        }

        transaction_id = self._accept_system_transaction(
            desired=desired,
            work=work,
            reason="boot-reconcile",
            saved=bool(snapshot["saved"]),
            verified=bool(snapshot["verified"]),
        )
        self._execute(transaction_id)

    def start(self) -> None:
        with self._condition:
            if self._closed:
                raise RuntimeError("network controller is closed")
            if self._thread is not None:
                return
            self._thread = threading.Thread(
                target=self._worker_loop,
                name="rp-ylx-network-control",
                daemon=True,
            )
            self._thread.start()
            self._monitor_thread = threading.Thread(
                target=self._monitor_loop,
                name="rp-ylx-network-health",
                daemon=True,
            )
            self._monitor_thread.start()
        current = self._state.snapshot()["transaction"]["current"]
        if isinstance(current, Mapping):
            transaction_id = str(current["transaction_id"])
            if self._state.execution_released(transaction_id):
                self._enqueue(transaction_id)
            else:
                with self._condition:
                    self._deferred.add(transaction_id)

    def close(self) -> None:
        with self._submit_lock, self._condition:
            if self._closed:
                return
            self._closed = True
            thread = self._thread
            monitor_thread = self._monitor_thread
            self._monitor_stop.set()
        if monitor_thread is not None:
            monitor_thread.join()
        if thread is not None:
            self._queue.put(None)
            thread.join()
        self._credentials.clear()

    def wait_for_idle(self, *, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        with self._condition:
            while self._pending or self._deferred:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return True

    def _enqueue(self, transaction_id: str) -> None:
        with self._condition:
            if self._closed:
                return
            if transaction_id in self._queued:
                return
            self._queued.add(transaction_id)
            self._pending += 1
            self._queue.put(transaction_id)

    def _schedule(self, transaction_id: str) -> None:
        if self._defer_execution_until_response:
            with self._condition:
                self._deferred.add(transaction_id)
            return
        self._enqueue(transaction_id)

    def release_response(self, response: Mapping[str, Any]) -> None:
        if response.get("ok") is not True or response.get("operation") not in MUTATION_OPERATIONS:
            return
        body = response.get("body")
        transaction = body.get("transaction") if isinstance(body, Mapping) else None
        transaction_id = (
            transaction.get("transaction_id") if isinstance(transaction, Mapping) else None
        )
        if not isinstance(transaction_id, str):
            return
        with self._condition:
            if transaction_id not in self._deferred:
                return
            if not self._state.release_execution(transaction_id):
                self._deferred.remove(transaction_id)
                self._condition.notify_all()
                return
            self._deferred.remove(transaction_id)
            self._enqueue(transaction_id)

    def _monitor_once(self, *, now_ns: int | None = None) -> None:
        snapshot = self._state.snapshot()
        transaction = snapshot["transaction"]
        if transaction["current"] is not None:
            self._health_failure_started_ns = None
            return
        desired = snapshot["desired"]
        mode = str(desired["mode"])
        if mode == "hotspot" or not snapshot["saved"]:
            self._health_failure_started_ns = None
            self._health_fallback_latched = False
            return
        try:
            with _network_operation_lock(blocking=False):
                healthy = saved_network_is_healthy(mode)
        except NetworkError as exc:
            if exc.code == "capture_active":
                self._health_failure_started_ns = None
                return
            healthy = False
        if healthy:
            self._health_failure_started_ns = None
            self._health_fallback_latched = False
            return
        latest = transaction["latest"]
        if (
            self._health_fallback_latched
            or isinstance(latest, Mapping)
            and latest.get("status") in {"rescued", "failed"}
        ):
            self._health_fallback_latched = True
            return
        observed_ns = self._monotonic_ns() if now_ns is None else now_ns
        if self._health_failure_started_ns is None:
            self._health_failure_started_ns = observed_ns
            return
        elapsed = (observed_ns - self._health_failure_started_ns) / 1_000_000_000
        if elapsed < self._health_failure_seconds:
            return
        failure_started_ns = self._health_failure_started_ns
        with self._submit_lock:
            if self._closed:
                return
            snapshot = self._state.snapshot()
            if snapshot["transaction"]["current"] is not None:
                self._health_failure_started_ns = None
                return
            desired = snapshot["desired"]
            mode = str(desired["mode"])
            try:
                with _network_operation_lock(blocking=False):
                    try:
                        candidate = saved_network_candidate(mode)
                    except NetworkError as exc:
                        candidate = None
                        fallback_error = self._transaction_error(exc)
                    else:
                        fallback_error = {
                            "code": "route_lost",
                            "message": "the verified client remained unhealthy for ten seconds",
                            "retryable": True,
                        }
                    work: dict[str, Any] = {
                        "kind": "fallback",
                        "desired": deepcopy(desired),
                        "cleanup_work_ids": [],
                        "fallback_error": fallback_error,
                        "rescue_deadline": {
                            "time_base": "device_monotonic",
                            "boot_id": self._boot_id,
                            "deadline_ns": failure_started_ns
                            + int(NETWORK_RESCUE_REACHABILITY_SECONDS * 1_000_000_000),
                        },
                    }
                    if candidate is not None:
                        work["candidate"] = candidate
                    transaction_id = self._accept_system_transaction(
                        desired=desired,
                        work=work,
                        reason="health-timeout",
                        saved=bool(snapshot["saved"]),
                        verified=bool(snapshot["verified"]),
                    )
            except NetworkError as exc:
                if exc.code == "capture_active":
                    self._health_failure_started_ns = None
                    return
                raise
            self._health_fallback_latched = True
            self._health_failure_started_ns = None
            self._enqueue(transaction_id)

    def _monitor_loop(self) -> None:
        while not self._monitor_stop.wait(self._health_poll_seconds):
            try:
                self._monitor_once()
            except (
                KeyError,
                NetworkError,
                NetworkStateError,
                OSError,
                RuntimeError,
                TypeError,
                ValueError,
            ):
                continue

    def _worker_loop(self) -> None:
        while True:
            transaction_id = self._queue.get()
            if transaction_id is None:
                self._queue.task_done()
                return
            try:
                self._execute(transaction_id)
            except (KeyError, OSError, RuntimeError, TypeError, ValueError):
                self._fallback_transaction(
                    transaction_id,
                    NetworkError("network_manager_unavailable", "网络控制器执行失败"),
                )
            finally:
                with self._condition:
                    self._queued.discard(transaction_id)
                    self._pending -= 1
                    self._condition.notify_all()
                self._queue.task_done()

    @staticmethod
    def _transaction_error(error: NetworkError) -> dict[str, Any]:
        code = {
            "wifi_auth_failed": "credential_rejected",
            "network_timeout": "dhcp_timeout",
            "default_route_missing": "route_lost",
            "rescue_failed": "rescue_ap_unavailable",
            "rescue_unconfigured": "rescue_ap_unavailable",
        }.get(error.code, error.code)
        if code not in {
            "rescue_ap_unavailable",
            "credential_rejected",
            "dhcp_timeout",
            "route_lost",
            "network_manager_unavailable",
            "concurrency_unsupported",
        }:
            code = "network_manager_unavailable"
        messages = {
            "rescue_ap_unavailable": "rescue AP could not be validated",
            "credential_rejected": "Wi-Fi credential was rejected",
            "dhcp_timeout": "network activation exceeded the ten second deadline",
            "route_lost": "the candidate did not establish a default route",
            "network_manager_unavailable": "NetworkManager could not apply the candidate",
            "concurrency_unsupported": "requested network concurrency is unsupported",
        }
        return {"code": code, "message": messages[code], "retryable": True}

    def _fallback_transaction(
        self,
        transaction_id: str,
        error: NetworkError,
        *,
        rescue_deadline_ns: int | None = None,
    ) -> None:
        with suppress(NetworkStateError):
            self._state.transition(
                transaction_id,
                status="running",
                stage="falling_back",
                updated_at=_now(),
                recovery_action="await_device",
            )
        try:
            rescue_network(
                deadline_ns=rescue_deadline_ns,
                monotonic_ns=self._monotonic_ns if rescue_deadline_ns is not None else None,
            )
            recovered = True
        except NetworkError:
            recovered = False
        try:
            self._state.transition(
                transaction_id,
                status="rescued" if recovered else "failed",
                stage="rescued" if recovered else "failed",
                updated_at=_now(),
                ap_validated=recovered,
                error=self._transaction_error(error),
                recovery_action=("reconnect_rescue_ap" if recovered else "service_required"),
                retain_work=True,
            )
        except NetworkStateError:
            return

    def _execute_forget(self, transaction_id: str, work: Mapping[str, Any]) -> None:
        desired = work.get("desired")
        if not isinstance(desired, Mapping):
            desired = {"mode": "hotspot", "wifi_client": None, "ethernet": None}
        try:
            self._state.transition(
                transaction_id,
                status="running",
                stage="forgetting",
                updated_at=_now(),
                recovery_action="await_device",
            )
            forget_network_client_profiles()
            self._credentials.clear()
            self._state.transition(
                transaction_id,
                status="committed",
                stage="forgotten",
                updated_at=_now(),
                ap_validated=True,
                desired=desired,
                publish_desired=True,
                recovery_action="reconnect_rescue_ap",
                saved=False,
                verified=False,
                retain_work=False,
                clear_all_work=True,
            )
        except (NetworkError, NetworkStateError) as exc:
            error = (
                exc
                if isinstance(exc, NetworkError)
                else NetworkError("network_manager_unavailable", "网络事务状态无效")
            )
            self._fallback_transaction(transaction_id, error)

    def _execute(self, transaction_id: str) -> None:
        with _network_operation_lock():
            self._execute_locked(transaction_id)

    def _rescue_deadline(self, work: Mapping[str, Any]) -> int | None:
        value = work.get("rescue_deadline")
        if value is None:
            return None
        if (
            not isinstance(value, Mapping)
            or set(value) != {"time_base", "boot_id", "deadline_ns"}
            or value.get("time_base") != "device_monotonic"
            or not isinstance(value.get("boot_id"), str)
            or type(value.get("deadline_ns")) is not int
            or value["deadline_ns"] < 0
        ):
            raise NetworkStateError("state_invalid", "network rescue deadline is invalid")
        if value["boot_id"] != self._boot_id:
            return None
        return int(value["deadline_ns"])

    def _execute_locked(self, transaction_id: str) -> None:
        work = self._state.work_for(transaction_id)
        if work.get("kind") == "forget":
            self._execute_forget(transaction_id, work)
            return
        desired = work.get("desired")
        if not isinstance(desired, Mapping):
            desired = self._state.snapshot()["desired"]
        rescue_deadline_ns: int | None = None
        try:
            rescue_deadline_ns = self._rescue_deadline(work)
            self._state.transition(
                transaction_id,
                status="running",
                stage="prepared",
                updated_at=_now(),
                desired=desired,
                publish_desired=True,
            )
            if work.get("rescue_ready") is not True:
                rescue_network(
                    deadline_ns=rescue_deadline_ns,
                    monotonic_ns=(self._monotonic_ns if rescue_deadline_ns is not None else None),
                )
            self._state.transition(
                transaction_id,
                status="running",
                stage="ap_ready",
                updated_at=_now(),
                ap_validated=True,
            )
            kind = work.get("kind")
            if kind == "fallback":
                fallback_error = work.get("fallback_error")
                if not isinstance(fallback_error, Mapping):
                    fallback_error = {
                        "code": "route_lost",
                        "message": "the verified client remained unhealthy for ten seconds",
                        "retryable": True,
                    }
                self._state.transition(
                    transaction_id,
                    status="running",
                    stage="falling_back",
                    updated_at=_now(),
                    ap_validated=True,
                    desired=desired,
                    publish_desired=True,
                    recovery_action="await_device",
                )
                self._state.transition(
                    transaction_id,
                    status="rescued",
                    stage="rescued",
                    updated_at=_now(),
                    ap_validated=True,
                    desired=desired,
                    publish_desired=True,
                    error=fallback_error,
                    recovery_action="reconnect_rescue_ap",
                    retain_work=True,
                )
                return
            if kind == "candidate":
                candidate = work.get("candidate")
                if not isinstance(candidate, Mapping):
                    raise NetworkError("candidate_invalid", "候选网络配置无效")
                deadline_ns = self._monotonic_ns() + 10_000_000_000
                rescue_deadline_ns = deadline_ns + 5_000_000_000
                deadline = {
                    "time_base": "device_monotonic",
                    "deadline_ns": deadline_ns,
                    "remaining_seconds": 10,
                }
                self._state.transition(
                    transaction_id,
                    status="running",
                    stage="activating",
                    updated_at=_now(),
                    ap_validated=True,
                    deadline=deadline,
                )
                activate_network_candidate(
                    candidate,
                    deadline_ns=deadline_ns,
                    monotonic_ns=self._monotonic_ns,
                )
                observed_ns = self._monotonic_ns()
                if observed_ns >= deadline_ns:
                    raise NetworkError("network_timeout", "网络激活超过十秒期限")
                remaining_seconds = max(
                    0.0,
                    min(10.0, (deadline_ns - observed_ns) / 1_000_000_000),
                )
                self._state.transition(
                    transaction_id,
                    status="running",
                    stage="verifying",
                    updated_at=_now(),
                    ap_validated=True,
                    deadline={**deadline, "remaining_seconds": remaining_seconds},
                )
                commit_network_candidate(candidate)
            elif kind != "rescue":
                raise NetworkError("candidate_invalid", "候选网络配置无效")
            is_wifi = desired.get("mode") == "wifi-client"
            cleanup_work = work.get("cleanup_work_ids", [])
            cleanup_ids = (
                tuple(item for item in cleanup_work if isinstance(item, str))
                if isinstance(cleanup_work, list)
                else ()
            )
            self._state.transition(
                transaction_id,
                status="committed",
                stage="committed",
                updated_at=_now(),
                ap_validated=True,
                recovery_action=(
                    "reconnect_target_lan" if kind == "candidate" else "reconnect_rescue_ap"
                ),
                saved=is_wifi,
                verified=is_wifi,
                retain_work=False,
                remove_work=cleanup_ids,
            )
        except (NetworkError, NetworkStateError) as exc:
            error = (
                exc
                if isinstance(exc, NetworkError)
                else NetworkError("network_manager_unavailable", "网络事务状态无效")
            )
            self._fallback_transaction(
                transaction_id,
                error,
                rescue_deadline_ns=rescue_deadline_ns,
            )

    @staticmethod
    def _desired_and_config(
        desired: Mapping[str, object],
        credential: str | None,
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        mode = str(desired["mode"])
        if mode == "wifi-client":
            wifi = desired["wifi_client"]
            assert isinstance(wifi, Mapping)
            ssid = str(wifi["ssid"])
            security = str(wifi["security"])
            if security != "open" and credential is None:
                raise NetworkCredentialError("credential_ref_invalid", "credential is missing")
            credential_state = "absent" if security == "open" else "stored"
            config = {"mode": mode, "ssid": ssid, "security": security}
            if credential is not None:
                config["psk"] = credential
            return (
                {
                    "mode": mode,
                    "wifi_client": {
                        "ssid": ssid,
                        "security": security,
                        "credential_state": credential_state,
                    },
                    "ethernet": deepcopy(desired.get("ethernet")),
                },
                config,
            )
        if mode == "hotspot":
            return ({"mode": mode, "wifi_client": None, "ethernet": None}, None)
        if mode == "ethernet-dhcp":
            return (
                {
                    "mode": mode,
                    "wifi_client": None,
                    "ethernet": {"addressing": "dhcp", "static_ipv4": None},
                },
                {"mode": mode},
            )
        ethernet = desired.get("ethernet")
        assert isinstance(ethernet, Mapping)
        normalized_desired = {
            "mode": mode,
            "wifi_client": None,
            "ethernet": deepcopy(dict(ethernet)),
        }
        static = ethernet["static_ipv4"]
        assert isinstance(static, Mapping)
        config: dict[str, Any] = {
            "mode": mode,
            "address": f"{static['address']}/{static['prefix_length']}",
            "dns": list(static["dns"]),
        }
        if static.get("gateway") is not None:
            config["gateway"] = static["gateway"]
        return normalized_desired, config

    def _accept_apply(self, request: Mapping[str, object]) -> dict[str, Any]:
        principal_id = str(request["principal_id"])
        idempotency_key = str(request["idempotency_key"])
        body = request["body"]
        assert isinstance(body, Mapping)
        fingerprint = _request_fingerprint("apply", body)
        with ExitStack() as stack:
            stack.enter_context(self._submit_lock)
            if self._closed:
                raise RuntimeError("network controller is closed")
            replay = self._state.replay_receipt(
                principal_id=principal_id,
                idempotency_key=idempotency_key,
                request_fingerprint=fingerprint,
            )
            if replay is not None:
                return {
                    "schema": CONTROL_RESPONSE_SCHEMA,
                    "ok": True,
                    "operation": "apply",
                    "status": 202,
                    "body": replay,
                    "replayed": True,
                }
            stack.enter_context(_network_operation_lock(blocking=False))
            if self._state.snapshot()["transaction"]["current"] is not None:
                raise NetworkStateError(
                    "controller_busy",
                    "another network transaction is still active",
                    retryable=True,
                )
            desired_input = body["desired"]
            assert isinstance(desired_input, Mapping)
            transaction_id = _uuid7()
            candidate: dict[str, Any] | None = None
            wifi = desired_input.get("wifi_client")
            protected_wifi = (
                desired_input.get("mode") == "wifi-client"
                and isinstance(wifi, Mapping)
                and wifi.get("security") != "open"
            )
            reservation_context = (
                self._credentials.reserve(str(wifi["credential_ref"]))
                if protected_wifi and isinstance(wifi, Mapping)
                else nullcontext(None)
            )
            try:
                with reservation_context as reservation:
                    credential = reservation.credential if reservation is not None else None
                    if desired_input.get("mode") == "wifi-client":
                        desired, config = self._desired_and_config(desired_input, credential)
                        assert config is not None
                        candidate = prepare_network_candidate(transaction_id, config)
                    else:
                        desired, config = self._desired_and_config(desired_input, None)
                        if config is not None:
                            candidate = prepare_network_candidate(transaction_id, config)
                    work = (
                        {"kind": "rescue", "desired": desired}
                        if candidate is None
                        else {"kind": "candidate", "candidate": candidate, "desired": desired}
                    )
                    accepted_desired = deepcopy(desired)
                    accepted_wifi = accepted_desired.get("wifi_client")
                    if isinstance(accepted_wifi, dict) and accepted_wifi.get("security") != "open":
                        accepted_wifi["credential_state"] = "pending_input"
                    receipt, replayed = self._state.accept_transaction(
                        operation="apply",
                        desired=accepted_desired,
                        principal_id=principal_id,
                        idempotency_key=idempotency_key,
                        request_fingerprint=fingerprint,
                        transaction_id=transaction_id,
                        accepted_at=_now(),
                        work=work,
                        execution_released=not self._defer_execution_until_response,
                    )
                    if reservation is not None:
                        reservation.commit()
            except (NetworkError, NetworkStateError, OSError):
                if candidate is not None:
                    with suppress(NetworkError, OSError), _network_operation_lock():
                        discard_network_candidate(candidate)
                raise
            self._schedule(transaction_id)
        return {
            "schema": CONTROL_RESPONSE_SCHEMA,
            "ok": True,
            "operation": "apply",
            "status": 202,
            "body": receipt,
            "replayed": replayed,
        }

    def _accept_retry(self, request: Mapping[str, object]) -> dict[str, Any]:
        principal_id = str(request["principal_id"])
        idempotency_key = str(request["idempotency_key"])
        body = request["body"]
        assert isinstance(body, Mapping)
        fingerprint = _request_fingerprint("retry", body)
        with ExitStack() as stack:
            stack.enter_context(self._submit_lock)
            if self._closed:
                raise RuntimeError("network controller is closed")
            replay = self._state.replay_receipt(
                principal_id=principal_id,
                idempotency_key=idempotency_key,
                request_fingerprint=fingerprint,
            )
            if replay is not None:
                return {
                    "schema": CONTROL_RESPONSE_SCHEMA,
                    "ok": True,
                    "operation": "retry",
                    "status": 202,
                    "body": replay,
                    "replayed": True,
                }
            stack.enter_context(_network_operation_lock(blocking=False))
            snapshot = self._state.snapshot()
            if snapshot["transaction"]["current"] is not None:
                raise NetworkStateError(
                    "controller_busy",
                    "another network transaction is still active",
                    retryable=True,
                )
            source_id = str(body["transaction_id"])
            latest = snapshot["transaction"]["latest"]
            if not isinstance(latest, Mapping) or latest.get("transaction_id") != source_id:
                raise NetworkStateError(
                    "transaction_not_found",
                    "network transaction was not retained",
                )
            if latest.get("status") not in {"rescued", "failed"}:
                raise NetworkStateError(
                    "transaction_not_retryable",
                    "network transaction cannot be retried",
                )
            source_work = self._state.work_for(source_id)
            desired = source_work.get("desired")
            if not isinstance(desired, Mapping):
                raise NetworkStateError(
                    "transaction_not_retryable",
                    "network transaction has no retained candidate",
                )
            cleanup_ids = source_work.get("cleanup_work_ids", [])
            existing_cleanup = (
                [item for item in cleanup_ids if isinstance(item, str)]
                if isinstance(cleanup_ids, list)
                else []
            )
            retry_work = deepcopy(source_work)
            if retry_work.get("kind") == "fallback":
                candidate = retry_work.get("candidate")
                if not isinstance(candidate, Mapping):
                    try:
                        candidate = saved_network_candidate(str(desired["mode"]))
                    except (KeyError, NetworkError) as exc:
                        raise NetworkStateError(
                            "transaction_not_retryable",
                            "network transaction has no retained candidate",
                        ) from exc
                    retry_work["candidate"] = candidate
                retry_work["kind"] = "candidate"
                retry_work.pop("fallback_error", None)
            retry_work["cleanup_work_ids"] = [source_id, *existing_cleanup]
            transaction_id = _uuid7()
            receipt, replayed = self._state.accept_transaction(
                operation="retry",
                desired=desired,
                principal_id=principal_id,
                idempotency_key=idempotency_key,
                request_fingerprint=fingerprint,
                transaction_id=transaction_id,
                accepted_at=_now(),
                work=retry_work,
                execution_released=not self._defer_execution_until_response,
                publish_desired=source_work.get("kind") != "forget",
                saved=snapshot["saved"],
                verified=snapshot["verified"],
            )
            self._schedule(transaction_id)
        return {
            "schema": CONTROL_RESPONSE_SCHEMA,
            "ok": True,
            "operation": "retry",
            "status": 202,
            "body": receipt,
            "replayed": replayed,
        }

    def _accept_forget(self, request: Mapping[str, object]) -> dict[str, Any]:
        principal_id = str(request["principal_id"])
        idempotency_key = str(request["idempotency_key"])
        body = request["body"]
        assert isinstance(body, Mapping)
        fingerprint = _request_fingerprint("forget", body)
        with ExitStack() as stack:
            stack.enter_context(self._submit_lock)
            if self._closed:
                raise RuntimeError("network controller is closed")
            replay = self._state.replay_receipt(
                principal_id=principal_id,
                idempotency_key=idempotency_key,
                request_fingerprint=fingerprint,
            )
            if replay is not None:
                return {
                    "schema": CONTROL_RESPONSE_SCHEMA,
                    "ok": True,
                    "operation": "forget",
                    "status": 202,
                    "body": replay,
                    "replayed": True,
                }
            stack.enter_context(_network_operation_lock(blocking=False))
            snapshot = self._state.snapshot()
            if snapshot["transaction"]["current"] is not None:
                raise NetworkStateError(
                    "controller_busy",
                    "another network transaction is still active",
                    retryable=True,
                )
            desired = {"mode": "hotspot", "wifi_client": None, "ethernet": None}
            transaction_id = _uuid7()
            receipt, replayed = self._state.accept_transaction(
                operation="forget",
                desired=desired,
                principal_id=principal_id,
                idempotency_key=idempotency_key,
                request_fingerprint=fingerprint,
                transaction_id=transaction_id,
                accepted_at=_now(),
                work={"kind": "forget", "desired": desired},
                execution_released=not self._defer_execution_until_response,
                publish_desired=False,
                saved=snapshot["saved"],
                verified=snapshot["verified"],
            )
            self._schedule(transaction_id)
        return {
            "schema": CONTROL_RESPONSE_SCHEMA,
            "ok": True,
            "operation": "forget",
            "status": 202,
            "body": receipt,
            "replayed": replayed,
        }

    def _scan(self) -> dict[str, Any]:
        with _network_operation_lock(blocking=False):
            networks = scan_wifi_networks()
        authority_epoch, source_revision = self._state.scan_stamp()
        return {
            "schema": "ylx.network-scan.v1",
            "authority_epoch": authority_epoch,
            "source_revision": source_revision,
            "scanned_at": _now(),
            "networks": networks,
        }

    def _status(self) -> dict[str, Any]:
        state = self._state.snapshot()
        return {
            "schema": "ylx.network-controller-status.v1",
            "authority_epoch": state["authority_epoch"],
            "source_revision": state["source_revision"],
            "saved": state["saved"],
            "verified": state["verified"],
            "desired": state["desired"],
            "transaction": state["transaction"],
            "capability": {
                "enabled": True,
                "disabled_reason": None,
                "operations": ["apply", "retry", "forget"],
                "idempotency_key_required": True,
                "secret_handling": "opaque_credential_reference_only",
                "active_state_policy": "idle_only",
            },
        }

    def _handle_validated(self, operation: str, request: Mapping[str, object]) -> dict[str, Any]:
        if operation == "health":
            return {
                "schema": CONTROL_RESPONSE_SCHEMA,
                "ok": True,
                "operation": "health",
                "capabilities": {
                    "mutation_enabled": True,
                    "operations": sorted(MUTATION_OPERATIONS),
                    "secret_handling": "opaque_credential_reference_only",
                },
            }
        if operation == "status":
            return {
                "schema": CONTROL_RESPONSE_SCHEMA,
                "ok": True,
                "operation": "status",
                "status": 200,
                "body": self._status(),
            }
        if operation == "scan":
            return {
                "schema": CONTROL_RESPONSE_SCHEMA,
                "ok": True,
                "operation": "scan",
                "status": 200,
                "body": self._scan(),
            }
        if operation == "create_credential":
            body = request["body"]
            assert isinstance(body, Mapping)
            issued = datetime.now(UTC)
            credential_ref = self._credentials.create(str(body["passphrase"]))
            ttl_seconds = int(self._credentials.ttl_seconds)
            return {
                "schema": CONTROL_RESPONSE_SCHEMA,
                "ok": True,
                "operation": "create_credential",
                "status": 201,
                "body": {
                    "schema": "ylx.network-credential-receipt.v1",
                    "credential_ref": credential_ref,
                    "issued_at": issued.isoformat().replace("+00:00", "Z"),
                    "expires_at": (issued + timedelta(seconds=ttl_seconds))
                    .isoformat()
                    .replace("+00:00", "Z"),
                    "ttl_seconds": ttl_seconds,
                    "single_use": True,
                },
            }
        if operation == "apply":
            return self._accept_apply(request)
        if operation == "retry":
            return self._accept_retry(request)
        if operation == "forget":
            return self._accept_forget(request)
        return _error_response(
            "operation_not_implemented",
            "network-control operation is not implemented",
            operation=operation,
            retryable=True,
        )

    def handle(self, request: object) -> dict[str, Any]:
        return handle_control_request(request, controller=self)


def handle_control_request(
    request: object,
    *,
    controller: NetworkController | None = None,
) -> dict[str, Any]:
    if not isinstance(request, Mapping):
        return _error_response("request_invalid", "request must be a JSON object")
    if request.get("schema") != CONTROL_REQUEST_SCHEMA:
        return _error_response("request_invalid", "unsupported network-control request schema")
    operation = request.get("operation")
    if operation not in SUPPORTED_OPERATIONS:
        return _error_response("operation_invalid", "unsupported network-control operation")
    if operation in MUTATION_OPERATIONS and _contains_inline_secret(request):
        return _error_response(
            "inline_secret_rejected",
            "network-control requests must use credential_ref, not inline secret material",
            operation=str(operation),
        )
    if not _valid_control_request(str(operation), request):
        return _error_response("request_invalid", "network-control request shape is invalid")
    if controller is not None:
        try:
            response = controller._handle_validated(str(operation), request)
        except NetworkCredentialError as exc:
            messages = {
                "credential_ref_invalid": "credential reference is invalid or already consumed",
                "credential_ref_expired": "credential reference has expired",
                "credential_invalid": "Wi-Fi credential is invalid",
                "credential_store_full": "credential store is temporarily full",
                "credential_reference_failed": "credential reference could not be allocated",
            }
            response = _error_response(
                exc.code,
                messages.get(exc.code, "credential operation failed"),
                operation=str(operation),
                retryable=exc.code == "credential_store_full",
            )
        except NetworkStateError as exc:
            response = _error_response(
                exc.code,
                "network transaction could not be accepted",
                operation=str(operation),
                retryable=exc.retryable,
            )
        except NetworkError as exc:
            response = _error_response(
                exc.code,
                (
                    "capture is active; network mutation was not started"
                    if exc.code == "capture_active"
                    else "NetworkManager operation failed"
                ),
                operation=str(operation),
                retryable=True,
            )
        except (OSError, RuntimeError, ValueError):
            response = _error_response(
                "controller_failure",
                "network controller failed closed",
                operation=str(operation),
                retryable=True,
            )
        return _seal_response(response)
    if operation == "health":
        return {
            "schema": CONTROL_RESPONSE_SCHEMA,
            "ok": True,
            "operation": "health",
            "capabilities": {
                "mutation_enabled": False,
                "operations": sorted(MUTATION_OPERATIONS),
                "secret_handling": "opaque_credential_reference_only",
            },
        }
    return _error_response(
        "network_controller_not_enabled",
        "network mutation controller is staged but not enabled",
        operation=str(operation),
        retryable=True,
    )


def handle_control_payload(
    payload: str,
    *,
    controller: NetworkController | None = None,
) -> dict[str, Any]:
    try:
        size = len(payload.encode("utf-8"))
    except UnicodeError:
        return _error_response("request_invalid", "request must be valid UTF-8 JSON")
    if size > MAX_CONTROL_REQUEST_BYTES:
        return _error_response("request_too_large", "network-control request is too large")
    try:
        request = json.loads(payload)
    except json.JSONDecodeError:
        return _error_response("request_invalid", "request must be valid JSON")
    return handle_control_request(request, controller=controller)


def _render_response(response: Mapping[str, Any]) -> str:
    return json.dumps(response, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


def _read_socket_request(connection: socket.socket) -> dict[str, Any] | str:
    chunks: list[bytes] = []
    size = 0
    while True:
        try:
            chunk = connection.recv(4096)
        except (OSError, TimeoutError):
            return _error_response("request_invalid", "network-control request was incomplete")
        if not chunk:
            break
        chunks.append(chunk)
        size += len(chunk)
        if size > MAX_CONTROL_REQUEST_BYTES:
            return _error_response("request_too_large", "network-control request is too large")
        if b"\n" in chunk:
            break
    payload, _, _ = b"".join(chunks).partition(b"\n")
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError:
        return _error_response("request_invalid", "request must be valid UTF-8 JSON")


def _serve_socket(
    listener: socket.socket,
    controller: NetworkController | None,
    *,
    max_connections: int | None,
) -> int:
    served = 0
    while max_connections is None or served < max_connections:
        try:
            connection, _ = listener.accept()
        except OSError:
            return 0
        with connection:
            connection.settimeout(CONTROL_SOCKET_TIMEOUT_SECONDS)
            payload = _read_socket_request(connection)
            response = (
                payload
                if isinstance(payload, dict)
                else handle_control_payload(payload, controller=controller)
            )
            try:
                connection.sendall(_render_response(response).encode("utf-8"))
            except OSError:
                pass
            else:
                if controller is not None:
                    controller.release_response(response)
        served += 1
    return 0


def _listening_socket(stream: TextIO) -> socket.socket | None:
    try:
        listener = socket.fromfd(stream.fileno(), socket.AF_UNIX, socket.SOCK_STREAM)
    except (AttributeError, OSError, ValueError):
        return None
    try:
        if listener.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN) == 1:
            return listener
    except OSError:
        pass
    listener.close()
    return None


def _notify_ready() -> None:
    address = os.environ.get("NOTIFY_SOCKET")
    if not address:
        return
    if address.startswith("@"):
        address = "\0" + address[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as notifier:
            notifier.connect(address)
            notifier.sendall(b"READY=1\nSTATUS=network rescue path reconciled")
    except OSError as exc:
        raise NetworkError(
            "readiness_notification_failed",
            "无法通知 systemd 网络控制器就绪",
        ) from exc


def serve_stdio(
    *,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
    controller: NetworkController | None = None,
    max_connections: int | None = None,
) -> int:
    if max_connections is not None and max_connections < 1:
        raise ValueError("max_connections must be positive")
    if stdin is None:
        stdin = sys.stdin
    if stdout is None:
        stdout = sys.stdout
    owns_controller = controller is None
    if controller is None:
        controller = NetworkController(defer_execution_until_response=True)
    try:
        _notify_ready()
        listener = _listening_socket(stdin)
        if listener is not None:
            with listener:
                return _serve_socket(
                    listener,
                    controller,
                    max_connections=max_connections,
                )
        handled = 0
        while max_connections is None or handled < max_connections:
            payload = stdin.readline(MAX_CONTROL_REQUEST_BYTES + 2)
            if payload == "":
                break
            response = handle_control_payload(payload, controller=controller)
            rendered = _render_response(response)
            if stdout.write(rendered) != len(rendered):
                raise OSError("network-control response was not fully written")
            stdout.flush()
            controller.release_response(response)
            handled += 1
        return 0
    finally:
        if owns_controller:
            controller.close()


def _read_socket_response(client: socket.socket) -> bytes:
    chunks: list[bytes] = []
    size = 0
    while True:
        chunk = client.recv(4096)
        if not chunk:
            break
        chunks.append(chunk)
        size += len(chunk)
        if size > MAX_CONTROL_RESPONSE_BYTES:
            raise NetworkControlClientError(
                "response_too_large",
                "network-control response exceeded the size limit",
            )
        if b"\n" in chunk:
            break
    line, _, _ = b"".join(chunks).partition(b"\n")
    return line


def request_control(
    operation: str,
    *,
    principal_id: str | None = None,
    idempotency_key: str | None = None,
    body: Mapping[str, object] | None = None,
    socket_path: Path = CONTROL_SOCKET_PATH,
    timeout_seconds: float = CONTROL_SOCKET_TIMEOUT_SECONDS,
) -> dict[str, Any]:
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
    elif operation == "create_credential":
        if principal_id is None or body is None:
            raise NetworkControlClientError(
                "request_invalid",
                "network credential request is incomplete",
            )
        request.update({"principal_id": principal_id, "body": dict(body)})
    payload = json.dumps(request, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(timeout_seconds)
            client.connect(str(socket_path))
            client.sendall(payload.encode("utf-8") + b"\n")
            client.shutdown(socket.SHUT_WR)
            rendered = _read_socket_response(client)
    except OSError as exc:
        raise NetworkControlClientError(
            "network_controller_unavailable",
            "network-control socket is unavailable",
        ) from exc
    try:
        response = json.loads(rendered.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NetworkControlClientError(
            "response_invalid",
            "network-control response is not valid JSON",
        ) from exc
    if not isinstance(response, dict):
        raise NetworkControlClientError(
            "response_invalid",
            "network-control response must be a JSON object",
        )
    if response.get("schema") != CONTROL_RESPONSE_SCHEMA:
        raise NetworkControlClientError(
            "response_invalid",
            "network-control response schema is invalid",
        )
    return _seal_response(response)
