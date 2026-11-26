"""Root-owned, socket-activated network mutation controller."""

from __future__ import annotations

import hashlib
import json
import os
import queue
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import ExitStack, nullcontext, suppress
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, TextIO

from rp_ylx.clock_sync import (
    ClockSyncError,
    ClockSynchronizer,
    valid_clock_request,
)
from rp_ylx.core.rpc import (
    ControlRpcServer,
    RpcFramingError,
    RpcTimeoutError,
    decode_ndjson_object,
    encode_ndjson_object,
    read_ndjson_line,
    write_all_with_deadline,
)
from rp_ylx.core.time import utc_now_iso
from rp_ylx.network import (
    activate_network_candidate,
    cleanup_orphan_network_candidates,
    commit_network_candidate,
    discard_network_candidate,
    ensure_rescue_ap,
    forget_network_client_profiles,
    prepare_network_candidate,
    register_controller_status_provider,
    rescue_network,
    saved_network_candidate,
    saved_network_is_healthy,
    scan_wifi_networks,
)
from rp_ylx.network_credentials import NetworkCredentialError, NetworkCredentialStore
from rp_ylx.network_paths import (
    CONTROL_REQUEST_SCHEMA,
    CONTROL_RESPONSE_SCHEMA,
    CONTROL_RESPONSE_TIMEOUT_SECONDS,
    CONTROL_SOCKET_PATH,
    CONTROL_SOCKET_TIMEOUT_SECONDS,
    MAX_CONTROL_REQUEST_BYTES,
    MAX_CONTROL_RESPONSE_BYTES,
    MUTATION_OPERATIONS,
    SECRET_FIELD_NAMES,
    SUPPORTED_OPERATIONS,
    NetworkControlClientError,
    NetworkError,
    desired_state_from_disk,
    lkg_path,
    network_operation_lock,
    profile_dir,
    read_json,
    seal_response,
    state_dir,
)
from rp_ylx.network_paths import (
    error_response as _error_response,
)
from rp_ylx.network_paths import (
    request_control as _paths_request_control,
)
from rp_ylx.network_state import (
    NetworkStateError,
    NetworkStateStore,
)
from rp_ylx.network_validation import (
    valid_network_request_shape,
    validate_desired_state_policy,
)
from rp_ylx.operational_logging import operational_logger
from rp_ylx.update import prepare_first_install_network

NETWORK_HEALTH_POLL_SECONDS = 1.0
NETWORK_HEALTH_FAILURE_SECONDS = 10.0
NETWORK_RESCUE_REACHABILITY_SECONDS = 15.0
NETWORK_MONITOR_ERROR_LOG_INTERVAL_SECONDS = 60.0
FAST_READ_OPERATIONS = frozenset({"health", "status", "clock_status", "clock_inspect", "scan"})
_OPERATIONAL_LOG = operational_logger("network-control")
_seal_response = seal_response


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


def _valid_control_request(operation: str, request: Mapping[str, object]) -> bool:
    if operation in {"clock_status", "clock_inspect", "clock_sync"}:
        return (
            set(request) == {"schema", "operation", "principal_id", "body"}
            and _valid_principal_id(request.get("principal_id"))
            and (
                request.get("body") == {}
                if operation in {"clock_status", "clock_inspect"}
                else valid_clock_request(request.get("body"))
            )
        )
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
        and valid_network_request_shape(operation, request.get("body"))
    )


def _now() -> str:
    return utc_now_iso()


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


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


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


@dataclass
class _ScanJob:
    """Single-generation Wi-Fi scan holder shared by concurrent waiters."""

    event: threading.Event = field(default_factory=threading.Event)
    result: list[dict[str, Any]] | None = None
    error: BaseException | None = None


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
        deferred_release_timeout_seconds: float = 30.0,
        health_poll_seconds: float = NETWORK_HEALTH_POLL_SECONDS,
        health_failure_seconds: float = NETWORK_HEALTH_FAILURE_SECONDS,
        monotonic_ns: Callable[[], int] | None = None,
    ) -> None:
        if require_root and os.geteuid() != 0:
            raise NetworkError("root_required", "network controller must run as root")
        if health_poll_seconds <= 0 or health_failure_seconds <= 0:
            raise ValueError("network health intervals must be positive")
        if deferred_release_timeout_seconds <= 0:
            raise ValueError("deferred release timeout must be positive")
        self._credentials = credential_store or NetworkCredentialStore()
        self._clock = ClockSynchronizer(state_dir() / "clock.json")
        self._boot_id = _controller_boot_id()
        try:
            with network_operation_lock():
                prepare_first_install_network(state_dir=state_dir(), profile_dir=profile_dir())
        except (ValueError, OSError, sqlite3.Error, subprocess.SubprocessError) as exc:
            _OPERATIONAL_LOG.event(
                "network_first_install_skipped",
                level="warning",
                exception_type=type(exc).__name__,
            )
        self._state = NetworkStateStore(state_dir())
        with network_operation_lock():
            self._rescue = ensure_rescue_ap(
                device_id or _controller_device_id(),
                preserve_existing_psk=True,
            )
        self._submit_lock = threading.Lock()
        self._scan_lock = threading.Lock()
        self._scan_job: _ScanJob | None = None
        self._condition = threading.Condition()
        self._queue: queue.Queue[str | None] = queue.Queue()
        self._queued: set[str] = set()
        self._deferred: set[str] = set()
        self._deferred_since_ns: dict[str, int] = {}
        self._deferred_release_timeout_seconds = float(deferred_release_timeout_seconds)
        self._pending = 0
        self._thread: threading.Thread | None = None
        self._monitor_thread: threading.Thread | None = None
        self._monitor_stop = threading.Event()
        self._health_poll_seconds = health_poll_seconds
        self._health_failure_seconds = health_failure_seconds
        self._monotonic_ns = monotonic_ns or time.monotonic_ns
        self._health_failure_started_ns: int | None = None
        self._health_fallback_latched = False
        self._monitor_error_type: str | None = None
        self._monitor_error_code: str | None = None
        self._monitor_error_last_logged_ns: int | None = None
        self._monitor_error_suppressed = 0
        self._closed = False
        self._defer_execution_until_response = defer_execution_until_response
        self._boot_reconcile()
        with network_operation_lock():
            cleanup_orphan_network_candidates(self._state.retained_profiles())
        if start_worker:
            self.start()
        snapshot = self._state.snapshot()
        _OPERATIONAL_LOG.event(
            "network_controller_started",
            desired_mode=str(snapshot["desired"]["mode"]),
            saved=bool(snapshot["saved"]),
            verified=bool(snapshot["verified"]),
            active_transaction=isinstance(snapshot["transaction"]["current"], Mapping),
            worker_enabled=start_worker,
            poll_seconds=self._health_poll_seconds,
        )

    @staticmethod
    def _error_code(error: BaseException, fallback: str) -> str:
        code = getattr(error, "code", fallback)
        return code if isinstance(code, str) else fallback

    def _log_transaction_accepted(
        self,
        operation: str,
        receipt: Mapping[str, Any],
        *,
        replayed: bool,
        reason: str | None = None,
    ) -> None:
        transaction = receipt.get("transaction")
        if not isinstance(transaction, Mapping):
            return
        context: dict[str, object] = {
            "operation": operation,
            "transaction_id": transaction.get("transaction_id"),
            "replayed": replayed,
        }
        desired = transaction.get("desired")
        if isinstance(desired, Mapping):
            context["desired_mode"] = desired.get("mode")
        if type(transaction.get("source_revision")) is int:
            context["source_revision"] = transaction["source_revision"]
        if reason is not None:
            context["reason"] = reason
        _OPERATIONAL_LOG.event("network_transaction_accepted", **context)

    def _transition(
        self,
        transaction_id: str,
        *,
        status: str,
        stage: str,
        **changes: Any,
    ) -> dict[str, Any]:
        transaction = self._state.transition(
            transaction_id,
            status=status,
            stage=stage,
            **changes,
        )
        context: dict[str, object] = {
            "transaction_id": transaction_id,
            "status": status,
            "stage": stage,
            "recovery_action": transaction.get("recovery_action"),
        }
        if type(transaction.get("source_revision")) is int:
            context["source_revision"] = transaction["source_revision"]
        rescue = transaction.get("rescue")
        if isinstance(rescue, Mapping) and type(rescue.get("ap_validated")) is bool:
            context["ap_validated"] = rescue["ap_validated"]
        desired = transaction.get("desired")
        if isinstance(desired, Mapping):
            context["desired_mode"] = desired.get("mode")
        error = transaction.get("error")
        if isinstance(error, Mapping) and isinstance(error.get("code"), str):
            context["error_code"] = error["code"]
        level = "error" if status == "failed" else "warning" if status == "rescued" else "info"
        _OPERATIONAL_LOG.event("network_transaction_transition", level=level, **context)
        return transaction

    def _log_control_failure(self, response: Mapping[str, Any]) -> None:
        if response.get("ok") is not False:
            return
        error = response.get("error")
        context: dict[str, object] = {}
        if isinstance(response.get("operation"), str):
            context["operation"] = response["operation"]
        if isinstance(error, Mapping) and isinstance(error.get("code"), str):
            context["error_code"] = error["code"]
        if type(response.get("retryable")) is bool:
            context["retryable"] = response["retryable"]
        _OPERATIONAL_LOG.event("network_control_request_rejected", level="warning", **context)

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
        self._log_transaction_accepted(
            "retry",
            receipt,
            replayed=replayed,
            reason=reason,
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

    def _abort_unreleased_transaction(self, transaction_id: str, *, reason: str) -> None:
        current = self._state.snapshot()["transaction"]["current"]
        operation = current.get("operation") if isinstance(current, Mapping) else None
        try:
            work = self._state.work_for(transaction_id)
        except NetworkStateError:
            work = {}
        candidate = work.get("candidate") if isinstance(work, Mapping) else None
        if operation == "apply" and isinstance(candidate, Mapping):
            current_state_dir = state_dir()
            lkg_profiles: set[str] = set()
            for iface in ("wlan0", "eth0"):
                with suppress(NetworkError):
                    record = read_json(lkg_path(current_state_dir, iface))
                    if isinstance(record, Mapping) and isinstance(record.get("profile"), str):
                        lkg_profiles.add(str(record["profile"]))
            if candidate.get("profile") not in lkg_profiles:
                with suppress(NetworkError, OSError), network_operation_lock():
                    discard_network_candidate(candidate)
        try:
            desired = desired_state_from_disk(state_dir())
            saved = desired.get("mode") != "hotspot"
        except NetworkError:
            desired = None
            saved = None
        error_message = (
            "network-control response delivery failed before execution release"
            if reason == "response_write_failed"
            else "stale unacknowledged network transaction was aborted"
        )
        with suppress(NetworkStateError):
            self._transition(
                transaction_id,
                status="failed",
                stage="failed",
                updated_at=_now(),
                desired=desired,
                publish_desired=desired is not None,
                saved=saved,
                verified=False if desired is not None else None,
                error={
                    "code": "network_manager_unavailable",
                    "message": error_message,
                    "retryable": True,
                },
                recovery_action="none",
                retain_work=False,
            )

    def _boot_reconcile(self) -> None:
        snapshot = self._state.snapshot()
        current = snapshot["transaction"]["current"]
        desired = snapshot["desired"]
        mode = str(desired["mode"])
        new_boot = self._state.boot_needs_reconcile(self._boot_id)
        _OPERATIONAL_LOG.event(
            "network_boot_reconcile_started",
            desired_mode=mode,
            new_boot=new_boot,
            active_transaction=isinstance(current, Mapping),
            saved=bool(snapshot["saved"]),
            verified=bool(snapshot["verified"]),
        )
        if isinstance(current, Mapping):
            tx_id = str(current["transaction_id"])
            if new_boot and not self._state.execution_released(tx_id):
                self._abort_unreleased_transaction(tx_id, reason="stale_boot_deferred")
                _OPERATIONAL_LOG.event(
                    "network_boot_stale_deferred_aborted",
                    level="warning",
                    transaction_id=tx_id,
                )
                snapshot = self._state.snapshot()
                current = snapshot["transaction"]["current"]
                desired = snapshot["desired"]
                mode = str(desired["mode"])
            else:
                self._state.mark_boot_reconciled(self._boot_id)
                _OPERATIONAL_LOG.event(
                    "network_boot_reconcile_completed",
                    desired_mode=mode,
                    outcome="active_transaction_retained",
                )
                return
        try:
            with network_operation_lock():
                if saved_network_is_healthy(mode):
                    self._state.mark_boot_reconciled(self._boot_id)
                    _OPERATIONAL_LOG.event(
                        "network_boot_reconcile_completed",
                        desired_mode=mode,
                        outcome="saved_network_healthy",
                    )
                    return
        except NetworkError as error:
            if mode != "hotspot":
                _OPERATIONAL_LOG.event(
                    "network_boot_health_check_failed",
                    level="warning",
                    desired_mode=mode,
                    error_code=error.code,
                )
        if mode == "hotspot":
            with network_operation_lock():
                rescue_network()
            self._state.mark_boot_reconciled(self._boot_id)
            _OPERATIONAL_LOG.event(
                "network_boot_reconcile_completed",
                desired_mode=mode,
                outcome="rescue_ap_active",
            )
            return

        latest = snapshot["transaction"]["latest"]
        terminal_retry = isinstance(latest, Mapping) and latest.get("status") in {
            "rescued",
            "failed",
        }
        if terminal_retry and not new_boot:
            with network_operation_lock():
                rescue_network()
            self._state.mark_boot_reconciled(self._boot_id)
            _OPERATIONAL_LOG.event(
                "network_boot_reconcile_completed",
                desired_mode=mode,
                outcome="terminal_outcome_retained",
            )
            return

        work = self._retained_retry_work(snapshot) if new_boot else None
        if work is None:
            try:
                with network_operation_lock():
                    candidate = saved_network_candidate(mode)
            except NetworkError as error:
                with network_operation_lock():
                    rescue_network()
                self._state.mark_boot_reconciled(self._boot_id)
                _OPERATIONAL_LOG.event(
                    "network_boot_reconcile_completed",
                    level="warning",
                    desired_mode=mode,
                    outcome="saved_candidate_unavailable",
                    error_code=error.code,
                )
                return
            cleanup_work_ids: list[str] = []
            if terminal_retry:
                source_id = str(latest["transaction_id"])
                cleanup_work_ids.append(source_id)
                try:
                    source = self._state.work_for(source_id)
                except NetworkStateError:
                    pass
                else:
                    inherited = source.get("cleanup_work_ids", [])
                    if isinstance(inherited, list):
                        cleanup_work_ids.extend(item for item in inherited if isinstance(item, str))
            work = {
                "kind": "candidate",
                "candidate": candidate,
                "desired": deepcopy(desired),
                "cleanup_work_ids": cleanup_work_ids,
            }
        transaction_id = self._accept_system_transaction(
            desired=work["desired"],
            work=work,
            reason="boot-reconcile",
            saved=bool(snapshot["saved"]),
            verified=bool(snapshot["verified"]),
        )
        self._state.mark_boot_reconciled(self._boot_id)
        _OPERATIONAL_LOG.event(
            "network_boot_reconcile_completed",
            desired_mode=mode,
            outcome="retry_submitted",
            transaction_id=transaction_id,
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
            elif self._defer_execution_until_response:
                with self._condition:
                    self._deferred.add(transaction_id)
                    self._deferred_since_ns[transaction_id] = self._monotonic_ns()
            else:
                self._abort_unreleased_transaction(transaction_id, reason="non_deferred_unreleased")

    def close(self) -> None:
        with self._submit_lock, self._condition:
            if self._closed:
                return
            self._closed = True
            thread = self._thread
            monitor_thread = self._monitor_thread
            self._monitor_stop.set()
        if monitor_thread is not None:
            monitor_thread.join(timeout=5.0)
        if thread is not None:
            self._queue.put(None)
            thread.join(timeout=5.0)
        self._credentials.clear()
        _OPERATIONAL_LOG.event("network_controller_stopped", outcome="closed")

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
                self._deferred_since_ns[transaction_id] = self._monotonic_ns()
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
                self._deferred_since_ns.pop(transaction_id, None)
                self._condition.notify_all()
                return
            self._deferred.remove(transaction_id)
            self._deferred_since_ns.pop(transaction_id, None)
            self._enqueue(transaction_id)

    def abort_deferred_response(self, response: Mapping[str, Any]) -> None:
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
            self._deferred.remove(transaction_id)
            self._deferred_since_ns.pop(transaction_id, None)
            self._condition.notify_all()
        self._abort_unreleased_transaction(transaction_id, reason="response_write_failed")

    def _expire_stale_deferred(self, *, now_ns: int | None = None) -> None:
        observed_ns = self._monotonic_ns() if now_ns is None else now_ns
        ttl_ns = int(self._deferred_release_timeout_seconds * 1_000_000_000)
        expired: list[str] = []
        with self._condition:
            for tx_id in list(self._deferred):
                started_ns = self._deferred_since_ns.get(tx_id)
                if started_ns is None:
                    self._deferred_since_ns[tx_id] = observed_ns
                elif observed_ns - started_ns >= ttl_ns:
                    self._deferred.discard(tx_id)
                    self._deferred_since_ns.pop(tx_id, None)
                    expired.append(tx_id)
            if expired:
                self._condition.notify_all()
        for tx_id in expired:
            if not self._state.execution_released(tx_id):
                self._abort_unreleased_transaction(tx_id, reason="deferred_release_timeout")
                _OPERATIONAL_LOG.event(
                    "network_stale_deferred_aborted",
                    level="warning",
                    transaction_id=tx_id,
                )

    def _monitor_once(self, *, now_ns: int | None = None) -> None:
        self._expire_stale_deferred(now_ns=now_ns)
        snapshot = self._state.snapshot()
        transaction = snapshot["transaction"]
        if transaction["current"] is not None:
            self._health_failure_started_ns = None
            return
        desired = snapshot["desired"]
        mode = str(desired["mode"])
        if mode == "hotspot" or (mode == "wifi-client" and not snapshot["saved"]):
            self._health_failure_started_ns = None
            self._health_fallback_latched = False
            return
        health_error_code: str | None = None
        try:
            with network_operation_lock(blocking=False):
                # The read-only health probe below must not block interactive requests.
                pass
        except NetworkError as exc:
            if exc.code == "capture_active":
                self._health_failure_started_ns = None
                return
            healthy = False
            health_error_code = exc.code
        else:
            try:
                healthy = saved_network_is_healthy(mode)
            except NetworkError as exc:
                healthy = False
                health_error_code = exc.code
        if healthy:
            was_degraded = self._health_failure_started_ns is not None
            self._health_failure_started_ns = None
            self._health_fallback_latched = False
            if was_degraded:
                _OPERATIONAL_LOG.event(
                    "network_health_recovered",
                    desired_mode=mode,
                    outcome="saved_network_healthy",
                )
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
            context: dict[str, object] = {
                "desired_mode": mode,
                "grace_seconds": self._health_failure_seconds,
            }
            if health_error_code is not None:
                context["error_code"] = health_error_code
            _OPERATIONAL_LOG.event(
                "network_health_degraded",
                level="warning",
                **context,
            )
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
            if mode == "hotspot" or (mode == "wifi-client" and not snapshot["saved"]):
                self._health_failure_started_ns = None
                self._health_fallback_latched = False
                return
            latest = snapshot["transaction"]["latest"]
            if (
                self._health_fallback_latched
                or isinstance(latest, Mapping)
                and latest.get("status") in {"rescued", "failed"}
            ):
                self._health_fallback_latched = True
                self._health_failure_started_ns = None
                return
            try:
                with network_operation_lock(blocking=False):
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

    def _record_monitor_failure(self, error: BaseException) -> None:
        observed_ns = self._monotonic_ns()
        exception_type = type(error).__name__
        error_code = self._error_code(error, "network_health_monitor_failed")
        interval_ns = int(NETWORK_MONITOR_ERROR_LOG_INTERVAL_SECONDS * 1_000_000_000)
        should_log = (
            exception_type != self._monitor_error_type
            or error_code != self._monitor_error_code
            or self._monitor_error_last_logged_ns is None
            or observed_ns - self._monitor_error_last_logged_ns >= interval_ns
        )
        if not should_log:
            self._monitor_error_suppressed += 1
            return
        context: dict[str, object] = {
            "exception_type": exception_type,
            "error_code": error_code,
        }
        if self._monitor_error_suppressed:
            context["suppressed_count"] = self._monitor_error_suppressed
        _OPERATIONAL_LOG.event(
            "network_health_monitor_failed",
            level="warning",
            **context,
        )
        self._monitor_error_type = exception_type
        self._monitor_error_code = error_code
        self._monitor_error_last_logged_ns = observed_ns
        self._monitor_error_suppressed = 0

    def _record_monitor_recovered(self) -> None:
        if self._monitor_error_type is None:
            return
        context: dict[str, object] = {"exception_type": self._monitor_error_type}
        if self._monitor_error_code is not None:
            context["error_code"] = self._monitor_error_code
        if self._monitor_error_suppressed:
            context["suppressed_count"] = self._monitor_error_suppressed
        _OPERATIONAL_LOG.event(
            "network_health_monitor_recovered",
            **context,
        )
        self._monitor_error_type = None
        self._monitor_error_code = None
        self._monitor_error_last_logged_ns = None
        self._monitor_error_suppressed = 0

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
            ) as error:
                self._record_monitor_failure(error)
                continue
            self._record_monitor_recovered()

    def _worker_loop(self) -> None:
        while True:
            transaction_id = self._queue.get()
            if transaction_id is None:
                self._queue.task_done()
                return
            try:
                self._execute(transaction_id)
            except (KeyError, OSError, RuntimeError, TypeError, ValueError) as error:
                _OPERATIONAL_LOG.event(
                    "network_transaction_worker_failed",
                    level="error",
                    transaction_id=transaction_id,
                    exception_type=type(error).__name__,
                    error_code=self._error_code(error, "network_manager_unavailable"),
                )
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
            self._transition(
                transaction_id,
                status="running",
                stage="falling_back",
                updated_at=_now(),
                recovery_action="await_device",
            )
        rescue_error_code: str | None = None
        try:
            rescue_network(
                deadline_ns=rescue_deadline_ns,
                monotonic_ns=self._monotonic_ns if rescue_deadline_ns is not None else None,
            )
            recovered = True
        except NetworkError as rescue_error:
            recovered = False
            rescue_error_code = rescue_error.code
        try:
            self._transition(
                transaction_id,
                status="rescued" if recovered else "failed",
                stage="rescued" if recovered else "failed",
                updated_at=_now(),
                ap_validated=recovered,
                error=self._transaction_error(error),
                recovery_action=("reconnect_rescue_ap" if recovered else "service_required"),
                retain_work=True,
            )
            if rescue_error_code is not None:
                _OPERATIONAL_LOG.event(
                    "network_rescue_failed",
                    level="error",
                    transaction_id=transaction_id,
                    fallback_error_code=error.code,
                    rescue_error_code=rescue_error_code,
                )
        except NetworkStateError as state_error:
            context: dict[str, object] = {
                "transaction_id": transaction_id,
                "error_code": state_error.code,
                "fallback_error_code": error.code,
            }
            if rescue_error_code is not None:
                context["rescue_error_code"] = rescue_error_code
            _OPERATIONAL_LOG.event(
                "network_transaction_fallback_state_failed",
                level="error",
                **context,
            )
            return

    def _execute_forget(self, transaction_id: str, work: Mapping[str, Any]) -> None:
        desired = work.get("desired")
        if not isinstance(desired, Mapping):
            desired = {"mode": "hotspot", "wifi_client": None, "ethernet": None}
        try:
            self._transition(
                transaction_id,
                status="running",
                stage="forgetting",
                updated_at=_now(),
                recovery_action="await_device",
            )
            forget_network_client_profiles()
            self._credentials.clear()
            self._transition(
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
        with network_operation_lock():
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
            self._transition(
                transaction_id,
                status="running",
                stage="prepared",
                updated_at=_now(),
                desired=desired,
                publish_desired=True,
            )
            kind = work.get("kind")
            if kind in {"fallback", "rescue"}:
                rescue_network(
                    deadline_ns=rescue_deadline_ns,
                    monotonic_ns=(self._monotonic_ns if rescue_deadline_ns is not None else None),
                )
                self._transition(
                    transaction_id,
                    status="running",
                    stage="ap_ready",
                    updated_at=_now(),
                    ap_validated=True,
                )
            if kind == "fallback":
                fallback_error = work.get("fallback_error")
                if not isinstance(fallback_error, Mapping):
                    fallback_error = {
                        "code": "route_lost",
                        "message": "the verified client remained unhealthy for ten seconds",
                        "retryable": True,
                    }
                self._transition(
                    transaction_id,
                    status="running",
                    stage="falling_back",
                    updated_at=_now(),
                    ap_validated=True,
                    desired=desired,
                    publish_desired=True,
                    recovery_action="await_device",
                )
                self._transition(
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
                self._transition(
                    transaction_id,
                    status="running",
                    stage="activating",
                    updated_at=_now(),
                    ap_validated=False,
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
                self._transition(
                    transaction_id,
                    status="running",
                    stage="verifying",
                    updated_at=_now(),
                    ap_validated=False,
                    deadline={**deadline, "remaining_seconds": remaining_seconds},
                )
                commit_network_candidate(candidate)
            elif kind != "rescue":
                raise NetworkError("candidate_invalid", "候选网络配置无效")
            is_client = desired.get("mode") != "hotspot"
            cleanup_work = work.get("cleanup_work_ids", [])
            cleanup_ids = (
                tuple(item for item in cleanup_work if isinstance(item, str))
                if isinstance(cleanup_work, list)
                else ()
            )
            self._transition(
                transaction_id,
                status="committed",
                stage="committed",
                updated_at=_now(),
                ap_validated=kind == "rescue",
                recovery_action=(
                    "reconnect_target_lan" if kind == "candidate" else "reconnect_rescue_ap"
                ),
                saved=is_client,
                verified=is_client,
                retain_work=False,
                remove_work=cleanup_ids,
            )
            if cleanup_ids or kind == "rescue":
                cleanup_orphan_network_candidates(self._state.retained_profiles())
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
                self._log_transaction_accepted("apply", replay, replayed=True)
                return {
                    "schema": CONTROL_RESPONSE_SCHEMA,
                    "ok": True,
                    "operation": "apply",
                    "status": 202,
                    "body": replay,
                    "replayed": True,
                }
            stack.enter_context(network_operation_lock(blocking=False))
            if self._state.snapshot()["transaction"]["current"] is not None:
                raise NetworkStateError(
                    "controller_busy",
                    "another network transaction is still active",
                    retryable=True,
                )
            prior_work_ids = list(self._state.work_ids())
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
                        {
                            "kind": "rescue",
                            "desired": desired,
                            "cleanup_work_ids": prior_work_ids,
                        }
                        if candidate is None
                        else {
                            "kind": "candidate",
                            "candidate": candidate,
                            "desired": desired,
                            "cleanup_work_ids": prior_work_ids,
                        }
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
                    with suppress(NetworkError, OSError), network_operation_lock():
                        discard_network_candidate(candidate)
                raise
            self._log_transaction_accepted("apply", receipt, replayed=replayed)
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
                self._log_transaction_accepted("retry", replay, replayed=True)
                return {
                    "schema": CONTROL_RESPONSE_SCHEMA,
                    "ok": True,
                    "operation": "retry",
                    "status": 202,
                    "body": replay,
                    "replayed": True,
                }
            stack.enter_context(network_operation_lock(blocking=False))
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
            # Rescue readiness and monotonic deadlines belong to the old execution.
            retry_work.pop("rescue_ready", None)
            retry_work.pop("rescue_deadline", None)
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
            self._log_transaction_accepted("retry", receipt, replayed=replayed)
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
                self._log_transaction_accepted("forget", replay, replayed=True)
                return {
                    "schema": CONTROL_RESPONSE_SCHEMA,
                    "ok": True,
                    "operation": "forget",
                    "status": 202,
                    "body": replay,
                    "replayed": True,
                }
            stack.enter_context(network_operation_lock(blocking=False))
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
            self._log_transaction_accepted("forget", receipt, replayed=replayed)
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
        with network_operation_lock(blocking=False):
            pass
        with self._scan_lock:
            job = self._scan_job
            is_leader = job is None
            if is_leader:
                job = _ScanJob()
                self._scan_job = job
        if not is_leader:
            job.event.wait()
            if job.error is not None:
                raise job.error
            assert job.result is not None
            networks = deepcopy(job.result)
        else:
            try:
                scanned = scan_wifi_networks()
                job.result = scanned
                networks = deepcopy(scanned)
            except BaseException as exc:
                job.error = exc
                raise
            finally:
                with self._scan_lock:
                    if self._scan_job is job:
                        self._scan_job = None
                job.event.set()
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
        if operation in {"clock_status", "clock_inspect", "clock_sync"}:
            principal_id = str(request["principal_id"])
            body = request["body"]
            assert isinstance(body, Mapping)
            if operation in {"clock_status", "clock_inspect"}:
                result = self._clock.status(principal_id, extended=operation == "clock_inspect")
            else:
                # Capture holds this same flock for its entire lifetime, including
                # finalization. It also protects against a queued, timed-out caller.
                with network_operation_lock(blocking=False):
                    result = self._clock.sync(principal_id, body)
            return {
                "schema": CONTROL_RESPONSE_SCHEMA,
                "ok": True,
                "operation": operation,
                "status": 200,
                "body": result,
            }
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
            _OPERATIONAL_LOG.event(
                "network_credential_issued",
                operation="create_credential",
                outcome="issued",
                ttl_seconds=ttl_seconds,
            )
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
    if operation == "apply":
        body = request.get("body")
        assert isinstance(body, Mapping)
        policy_error = validate_desired_state_policy(body.get("desired"))
        if policy_error is not None:
            code, message = policy_error
            return _error_response(code, message, operation="apply", retryable=False)
    if controller is not None:
        try:
            response = controller._handle_validated(str(operation), request)
        except ClockSyncError as exc:
            response = _error_response(
                exc.code,
                str(exc),
                operation=str(operation),
                retryable=True,
            )
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
            is_invalid = exc.code in {
                "config_invalid",
                "candidate_invalid",
                "network_config_invalid",
            }
            response = _error_response(
                exc.code,
                (
                    "capture is active; network mutation was not started"
                    if exc.code == "capture_active"
                    else exc.message
                    if is_invalid
                    else "NetworkManager operation failed"
                ),
                operation=str(operation),
                retryable=not is_invalid,
            )
        except (OSError, RuntimeError, ValueError):
            response = _error_response(
                "controller_failure",
                "network controller failed closed",
                operation=str(operation),
                retryable=True,
            )
        sealed = _seal_response(response)
        controller._log_control_failure(sealed)
        return sealed
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


def _is_concurrent_control_request(request: Mapping[str, Any]) -> bool:
    return request.get("operation") in FAST_READ_OPERATIONS


class _NetworkControlRpcServer(ControlRpcServer):
    """Concurrent bounded NDJSON Unix-socket server with deferred-response ownership."""

    def __init__(
        self,
        listener: socket.socket,
        controller: NetworkController | None,
        *,
        max_connections: int | None = None,
    ) -> None:
        self._controller = controller
        super().__init__(
            None,
            lambda req: handle_control_request(req, controller=controller),
            max_workers=8,
            max_pending=32,
            max_request_bytes=MAX_CONTROL_REQUEST_BYTES,
            max_response_bytes=MAX_CONTROL_RESPONSE_BYTES,
            request_timeout_seconds=CONTROL_RESPONSE_TIMEOUT_SECONDS,
            max_connections=max_connections,
            concurrent_predicate=_is_concurrent_control_request,
            listener=listener,
        )

    def _reject_busy(self, conn: socket.socket) -> None:
        response = _error_response(
            "controller_busy",
            "network-control server is busy",
            retryable=True,
        )
        try:
            payload = encode_ndjson_object(response, max_bytes=self.max_response_bytes)
            write_all_with_deadline(conn, payload, deadline=time.monotonic() + 0.1)
        except (OSError, RpcFramingError):
            pass
        finally:
            with suppress(OSError):
                conn.close()

    def _handle_connection(self, conn: socket.socket, deadline: float) -> None:
        read_deadline = min(deadline, time.monotonic() + CONTROL_SOCKET_TIMEOUT_SECONDS)
        try:
            with conn:
                response: dict[str, Any] | None = None
                try:
                    raw = read_ndjson_line(
                        conn,
                        max_bytes=self.max_request_bytes,
                        deadline=read_deadline,
                        allow_empty_eof=True,
                    )
                except RpcFramingError as exc:
                    code = "request_too_large" if "size limit" in str(exc) else "request_invalid"
                    response = _error_response(code, "network-control request was invalid")
                except RpcTimeoutError:
                    response = _error_response(
                        "request_invalid", "network-control request timed out"
                    )
                else:
                    if not raw:
                        return
                    try:
                        request = decode_ndjson_object(
                            raw,
                            max_bytes=self.max_request_bytes,
                            max_depth=self.max_depth,
                        )
                    except RpcFramingError:
                        response = _error_response("request_invalid", "request must be valid JSON")
                    else:
                        if _is_concurrent_control_request(request):
                            response = handle_control_request(request, controller=self._controller)
                        else:
                            with self._mutation_lock:
                                response = handle_control_request(
                                    request, controller=self._controller
                                )
                assert response is not None
                if not response.get("ok") and self._controller is not None:
                    self._controller._log_control_failure(response)
                encoded = encode_ndjson_object(response, max_bytes=self.max_response_bytes)
                try:
                    write_all_with_deadline(conn, encoded, deadline=deadline)
                except OSError as error:
                    _OPERATIONAL_LOG.event(
                        "network_control_response_write_failed",
                        level="warning",
                        transport="socket",
                        error_code="response_write_failed",
                        exception_type=type(error).__name__,
                    )
                    if self._controller is not None:
                        self._controller.abort_deferred_response(response)
                else:
                    if self._controller is not None:
                        self._controller.release_response(response)
        except (OSError, RpcFramingError):
            return


def _serve_socket(
    listener: socket.socket,
    controller: NetworkController | None,
    *,
    max_connections: int | None,
    stop_requested: threading.Event | None = None,
    on_server_ready: Callable[[_NetworkControlRpcServer], None] | None = None,
) -> int:
    server = _NetworkControlRpcServer(
        listener,
        controller,
        max_connections=max_connections,
    )
    if on_server_ready is not None:
        on_server_ready(server)
    try:
        if stop_requested is None or not stop_requested.is_set():
            server.serve_forever()
    finally:
        server.shutdown(timeout=CONTROL_RESPONSE_TIMEOUT_SECONDS)
    outcome = (
        "signal"
        if stop_requested is not None and stop_requested.is_set()
        else (
            "connection_limit_reached"
            if max_connections is not None and server._connections_accepted >= max_connections
            else "closed"
        )
    )
    _OPERATIONAL_LOG.event(
        "network_control_listener_stopped",
        transport="socket",
        connections_served=server._connections_accepted,
        outcome=outcome,
    )
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
            notifier.sendall(b"READY=1\nSTATUS=network startup reconciled")
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
    transport = "stdio"
    stop_requested = threading.Event()
    active_server: _NetworkControlRpcServer | None = None
    previous_handlers: dict[int, object] = {}

    def _on_shutdown_signal(_signum: int, _frame: object) -> None:
        stop_requested.set()
        server = active_server
        if server is not None:
            with suppress(OSError):
                os.write(server._wakeup_w, b"x")

    if threading.current_thread() is threading.main_thread():
        for sig in (signal.SIGTERM, signal.SIGINT):
            with suppress(OSError, ValueError):
                previous_handlers[sig] = signal.signal(sig, _on_shutdown_signal)

    def _register_server(server: _NetworkControlRpcServer) -> None:
        nonlocal active_server
        active_server = server
        if stop_requested.is_set():
            with suppress(OSError):
                os.write(server._wakeup_w, b"x")

    try:
        _notify_ready()
        listener = _listening_socket(stdin)
        if listener is not None:
            transport = "socket"
            _OPERATIONAL_LOG.event(
                "network_control_server_ready",
                transport=transport,
            )
            with listener:
                return _serve_socket(
                    listener,
                    controller,
                    max_connections=max_connections,
                    stop_requested=stop_requested,
                    on_server_ready=_register_server,
                )
        _OPERATIONAL_LOG.event(
            "network_control_server_ready",
            transport=transport,
        )
        handled = 0
        while not stop_requested.is_set() and (
            max_connections is None or handled < max_connections
        ):
            payload = stdin.readline(MAX_CONTROL_REQUEST_BYTES + 1)
            if payload == "":
                break
            if not payload.endswith("\n") and (
                len(payload) > MAX_CONTROL_REQUEST_BYTES
                or len(payload.encode("utf-8", errors="replace")) > MAX_CONTROL_REQUEST_BYTES
            ):
                while True:
                    discard = stdin.readline(MAX_CONTROL_REQUEST_BYTES + 1)
                    if not discard or discard.endswith("\n"):
                        break
                response = _error_response(
                    "request_too_large", "network-control request is too large"
                )
            else:
                response = handle_control_payload(payload, controller=controller)
            rendered = _render_response(response)
            try:
                if stdout.write(rendered) != len(rendered):
                    raise OSError("network-control response was not fully written")
                stdout.flush()
            except OSError as error:
                _OPERATIONAL_LOG.event(
                    "network_control_response_write_failed",
                    level="error",
                    transport=transport,
                    error_code="response_write_failed",
                    exception_type=type(error).__name__,
                )
                controller.abort_deferred_response(response)
                raise
            controller.release_response(response)
            handled += 1
        _OPERATIONAL_LOG.event(
            "network_control_listener_stopped",
            transport=transport,
            connections_served=handled,
            outcome="signal" if stop_requested.is_set() else "input_closed",
        )
        return 0
    finally:
        for sig, previous in previous_handlers.items():
            with suppress(OSError, ValueError):
                signal.signal(sig, previous)  # type: ignore[arg-type]
        if owns_controller:
            controller.close()
        _OPERATIONAL_LOG.event(
            "network_control_server_stopped",
            transport=transport,
            outcome="closed",
        )


def request_control(
    operation: str,
    *,
    principal_id: str | None = None,
    idempotency_key: str | None = None,
    body: Mapping[str, object] | None = None,
    socket_path: Path = CONTROL_SOCKET_PATH,
    timeout_seconds: float = CONTROL_SOCKET_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    return _paths_request_control(
        operation,
        principal_id=principal_id,
        idempotency_key=idempotency_key,
        body=body,
        socket_path=socket_path,
        timeout_seconds=timeout_seconds,
    )


def _project_controller_status() -> dict[str, Any] | None:
    try:
        response = request_control(
            "status",
            timeout_seconds=CONTROL_RESPONSE_TIMEOUT_SECONDS,
        )
    except NetworkControlClientError:
        return None
    sealed = seal_response(response)
    if sealed.get("ok") is not True or sealed.get("operation") != "status":
        return None
    body = sealed.get("body")
    return deepcopy(dict(body)) if isinstance(body, Mapping) else None


register_controller_status_provider(_project_controller_status)
