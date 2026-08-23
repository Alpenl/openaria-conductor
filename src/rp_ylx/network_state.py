"""Durable authority and transaction state for privileged network mutation."""

from __future__ import annotations

import hashlib
import re
import threading
import uuid
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any

from rp_ylx.network import (
    _desired_state_from_disk,
    _network_lock,
    _prepare_state_dir,
    _read_json,
    _write_json,
)

CONTROLLER_STATE_SCHEMA = "ylx.network-controller-state.v1"
TRANSACTION_SCHEMA = "ylx.network-transaction.v1"
RECEIPT_SCHEMA = "ylx.network-transaction-receipt.v1"
UUID_V7_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
FORBIDDEN_PERSISTED_KEYS = frozenset(
    {"credential_ref", "password", "passphrase", "psk", "secret", "token"}
)
TERMINAL_STATUSES = frozenset({"committed", "rescued", "failed"})
WIFI_SECURITY = frozenset({"open", "wpa2-personal", "wpa3-personal", "wpa2-wpa3-personal"})
TRANSACTION_STATUSES = frozenset({"accepted", "running", *TERMINAL_STATUSES})
TRANSACTION_STAGES = frozenset(
    {
        "accepted",
        "prepared",
        "ap_ready",
        "activating",
        "verifying",
        "committed",
        "falling_back",
        "rescued",
        "failed",
        "forgetting",
        "forgotten",
    }
)
RECOVERY_ACTIONS = frozenset(
    {
        "await_device",
        "reconnect_target_lan",
        "reconnect_rescue_ap",
        "retry",
        "service_required",
        "none",
    }
)
TRANSACTION_ERROR_CODES = frozenset(
    {
        "rescue_ap_unavailable",
        "credential_rejected",
        "dhcp_timeout",
        "route_lost",
        "network_manager_unavailable",
        "concurrency_unsupported",
    }
)
STAGE_STATUS = {
    "accepted": "accepted",
    "prepared": "running",
    "ap_ready": "running",
    "activating": "running",
    "verifying": "running",
    "committed": "committed",
    "falling_back": "running",
    "rescued": "rescued",
    "failed": "failed",
    "forgetting": "running",
    "forgotten": "committed",
}


class NetworkStateError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        self.code = code
        self.message = message
        self.retryable = retryable
        super().__init__(f"{code}: {message}")


def _contains_forbidden_key(value: object) -> bool:
    if isinstance(value, Mapping):
        return any(
            isinstance(key, str)
            and key.casefold() in FORBIDDEN_PERSISTED_KEYS
            or _contains_forbidden_key(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_contains_forbidden_key(item) for item in value)
    return False


def _valid_desired(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {"mode", "wifi_client", "ethernet"}:
        return False
    mode = value.get("mode")
    if mode not in {"hotspot", "wifi-client", "ethernet-dhcp", "ethernet-static"}:
        return False
    wifi = value.get("wifi_client")
    if mode == "wifi-client":
        if (
            not isinstance(wifi, Mapping)
            or set(wifi) != {"ssid", "security", "credential_state"}
            or not isinstance(wifi.get("ssid"), str)
            or not 1 <= len(str(wifi["ssid"]).encode("utf-8")) <= 32
            or wifi.get("security") not in WIFI_SECURITY
            or wifi.get("credential_state") not in {"absent", "pending_input", "stored"}
        ):
            return False
        if (wifi.get("security") == "open") != (wifi.get("credential_state") == "absent"):
            return False
    elif wifi is not None:
        return False
    ethernet = value.get("ethernet")
    if ethernet is not None:
        if not isinstance(ethernet, Mapping) or set(ethernet) != {"addressing", "static_ipv4"}:
            return False
        if ethernet.get("addressing") == "dhcp":
            if ethernet.get("static_ipv4") is not None:
                return False
        elif ethernet.get("addressing") == "static":
            static = ethernet.get("static_ipv4")
            if not isinstance(static, Mapping) or set(static) != {
                "address",
                "prefix_length",
                "gateway",
                "dns",
            }:
                return False
        else:
            return False
    return not (mode == "ethernet-static" and ethernet is None)


def _valid_uuid4(value: object) -> bool:
    try:
        parsed = uuid.UUID(str(value))
    except (ValueError, AttributeError):
        return False
    return parsed.version == 4 and str(parsed) == value


def _valid_boot_id(value: object) -> bool:
    if value is None:
        return True
    try:
        parsed = uuid.UUID(str(value))
    except (ValueError, AttributeError):
        return False
    return isinstance(value, str) and str(parsed) == value


def _valid_deadline(value: object) -> bool:
    if value is None:
        return True
    if not isinstance(value, Mapping) or set(value) != {
        "time_base",
        "deadline_ns",
        "remaining_seconds",
    }:
        return False
    remaining = value.get("remaining_seconds")
    return (
        value.get("time_base") == "device_monotonic"
        and type(value.get("deadline_ns")) is int
        and value["deadline_ns"] >= 0
        and isinstance(remaining, (int, float))
        and not isinstance(remaining, bool)
        and 0 <= remaining <= 10
    )


def _valid_transaction(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "schema",
        "authority_epoch",
        "source_revision",
        "transaction_id",
        "operation",
        "status",
        "stage",
        "desired",
        "accepted_at",
        "updated_at",
        "deadline",
        "recovery_action",
        "rescue",
        "error",
    }:
        return False
    transaction_id = value.get("transaction_id")
    rescue = value.get("rescue")
    error = value.get("error")
    return (
        value.get("schema") == TRANSACTION_SCHEMA
        and _valid_uuid4(value.get("authority_epoch"))
        and type(value.get("source_revision")) is int
        and value["source_revision"] >= 0
        and isinstance(transaction_id, str)
        and UUID_V7_PATTERN.fullmatch(transaction_id) is not None
        and value.get("operation") in {"apply", "retry", "forget"}
        and value.get("status") in TRANSACTION_STATUSES
        and value.get("stage") in TRANSACTION_STAGES
        and STAGE_STATUS.get(str(value.get("stage"))) == value.get("status")
        and _valid_desired(value.get("desired"))
        and isinstance(value.get("accepted_at"), str)
        and bool(value["accepted_at"])
        and isinstance(value.get("updated_at"), str)
        and bool(value["updated_at"])
        and _valid_deadline(value.get("deadline"))
        and value.get("recovery_action") in RECOVERY_ACTIONS
        and isinstance(rescue, Mapping)
        and set(rescue) == {"ap_validated", "fallback_mode", "failure_trigger_seconds"}
        and type(rescue.get("ap_validated")) is bool
        and rescue.get("fallback_mode") == "hotspot"
        and rescue.get("failure_trigger_seconds") == 10
        and (
            error is None
            or isinstance(error, Mapping)
            and set(error) == {"code", "message", "retryable"}
            and error.get("code") in TRANSACTION_ERROR_CODES
            and isinstance(error.get("message"), str)
            and bool(error["message"])
            and type(error.get("retryable")) is bool
        )
    )


def _valid_receipts(value: object, authority_epoch: str, source_revision: int) -> bool:
    if not isinstance(value, Mapping):
        return False
    for scope, entry in value.items():
        if (
            not isinstance(scope, str)
            or re.fullmatch(r"[0-9a-f]{64}", scope) is None
            or not isinstance(entry, Mapping)
            or set(entry) != {"request_fingerprint", "receipt"}
            or re.fullmatch(r"[0-9a-f]{64}", str(entry.get("request_fingerprint"))) is None
        ):
            return False
        receipt = entry.get("receipt")
        if not isinstance(receipt, Mapping) or set(receipt) != {
            "schema",
            "accepted_at",
            "transaction",
        }:
            return False
        transaction = receipt.get("transaction")
        if (
            receipt.get("schema") != RECEIPT_SCHEMA
            or not isinstance(receipt.get("accepted_at"), str)
            or not _valid_transaction(transaction)
            or transaction.get("status") != "accepted"
            or transaction.get("authority_epoch") != authority_epoch
            or transaction.get("source_revision", source_revision + 1) > source_revision
            or receipt.get("accepted_at") != transaction.get("accepted_at")
        ):
            return False
    return True


def _valid_state(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "schema",
        "authority_epoch",
        "source_revision",
        "saved",
        "verified",
        "desired",
        "transaction",
        "receipts",
        "work",
        "execution_release",
        "rescue_boot_id",
    }:
        return False
    transaction = value.get("transaction")
    if not isinstance(transaction, Mapping) or set(transaction) != {"current", "latest"}:
        return False
    current = transaction.get("current")
    latest = transaction.get("latest")
    authority_epoch = value.get("authority_epoch")
    source_revision = value.get("source_revision")
    execution_release = value.get("execution_release")
    return (
        value.get("schema") == CONTROLLER_STATE_SCHEMA
        and _valid_uuid4(authority_epoch)
        and type(source_revision) is int
        and source_revision >= 0
        and type(value.get("saved")) is bool
        and type(value.get("verified")) is bool
        and (not value["verified"] or value["saved"])
        and _valid_desired(value.get("desired"))
        and (current is None or _valid_transaction(current))
        and (latest is None or _valid_transaction(latest))
        and (current is None or current.get("status") in {"accepted", "running"})
        and (latest is None or latest.get("status") in TERMINAL_STATUSES)
        and all(
            item is None
            or item.get("authority_epoch") == authority_epoch
            and item.get("source_revision", source_revision + 1) <= source_revision
            for item in (current, latest)
        )
        and _valid_receipts(value.get("receipts"), str(authority_epoch), source_revision)
        and isinstance(value.get("work"), Mapping)
        and isinstance(execution_release, Mapping)
        and _valid_boot_id(value.get("rescue_boot_id"))
        and all(
            isinstance(transaction_id, str)
            and UUID_V7_PATTERN.fullmatch(transaction_id) is not None
            and type(released) is bool
            for transaction_id, released in execution_release.items()
        )
        and (
            not execution_release
            if current is None
            else set(execution_release) == {current.get("transaction_id")}
        )
        and not _contains_forbidden_key(value)
    )


def valid_desired_state(value: object) -> bool:
    return _valid_desired(value)


def valid_transaction(value: object) -> bool:
    return _valid_transaction(value)


class NetworkStateStore:
    """Persist one authoritative desired state and current/latest v1 transaction window."""

    def __init__(self, state_dir: Path) -> None:
        self._state_dir = Path(state_dir)
        self._path = self._state_dir / "controller-state.json"
        self._thread_lock = threading.RLock()
        _prepare_state_dir(self._state_dir)
        with self._thread_lock, _network_lock(self._state_dir):
            state = _read_json(self._path)
            if state is None:
                desired = _desired_state_from_disk(self._state_dir)
                saved = desired.get("mode") == "wifi-client"
                state = {
                    "schema": CONTROLLER_STATE_SCHEMA,
                    "authority_epoch": str(uuid.uuid4()),
                    "source_revision": 0,
                    "saved": saved,
                    "verified": saved,
                    "desired": desired,
                    "transaction": {"current": None, "latest": None},
                    "receipts": {},
                    "work": {},
                    "execution_release": {},
                    "rescue_boot_id": None,
                }
                self._write_locked(state)
            else:
                migrated = False
                if "execution_release" not in state:
                    current = state.get("transaction", {}).get("current")
                    state["execution_release"] = (
                        {str(current["transaction_id"]): True}
                        if isinstance(current, Mapping)
                        else {}
                    )
                    migrated = True
                if "rescue_boot_id" not in state:
                    state["rescue_boot_id"] = None
                    migrated = True
                if migrated:
                    self._validate(state)
                    self._write_locked(state)
            self._validate(state)

    @staticmethod
    def _validate(state: Mapping[str, Any]) -> None:
        if not _valid_state(state):
            raise NetworkStateError("state_invalid", "network controller state is invalid")

    def _load_locked(self) -> dict[str, Any]:
        state = _read_json(self._path)
        if state is None:
            raise NetworkStateError("state_invalid", "network controller state is missing")
        self._validate(state)
        return state

    def _write_locked(self, state: Mapping[str, Any]) -> None:
        """Treat a post-replace error as published only when read-back is exact."""

        try:
            _write_json(self._path, state)
        except OSError:
            published = _read_json(self._path)
            if published != state:
                raise

    def snapshot(self) -> dict[str, Any]:
        with self._thread_lock, _network_lock(self._state_dir):
            state = self._load_locked()
        return {
            "schema": CONTROLLER_STATE_SCHEMA,
            "authority_epoch": state["authority_epoch"],
            "source_revision": state["source_revision"],
            "saved": state["saved"],
            "verified": state["verified"],
            "desired": deepcopy(state["desired"]),
            "transaction": deepcopy(state["transaction"]),
        }

    @staticmethod
    def _scope(principal_id: str, idempotency_key: str) -> str:
        return hashlib.sha256(f"{principal_id}\0{idempotency_key}".encode()).hexdigest()

    def replay_receipt(
        self,
        *,
        principal_id: str,
        idempotency_key: str,
        request_fingerprint: str,
    ) -> dict[str, Any] | None:
        scope = self._scope(principal_id, idempotency_key)
        with self._thread_lock, _network_lock(self._state_dir):
            state = self._load_locked()
            existing = state["receipts"].get(scope)
            if existing is None:
                return None
            if (
                not isinstance(existing, Mapping)
                or existing.get("request_fingerprint") != request_fingerprint
            ):
                raise NetworkStateError(
                    "idempotency_conflict",
                    "idempotency key was already used for another request",
                )
            receipt = existing.get("receipt")
            if not isinstance(receipt, Mapping):
                raise NetworkStateError("state_invalid", "stored network receipt is invalid")
            return deepcopy(dict(receipt))

    def accept_transaction(
        self,
        *,
        operation: str,
        desired: Mapping[str, Any],
        principal_id: str,
        idempotency_key: str,
        request_fingerprint: str,
        transaction_id: str,
        accepted_at: str,
        work: Mapping[str, Any],
        execution_released: bool = True,
        publish_desired: bool = True,
        saved: bool | None = None,
        verified: bool | None = None,
    ) -> tuple[dict[str, Any], bool]:
        if (
            operation not in {"apply", "retry", "forget"}
            or not _valid_desired(desired)
            or UUID_V7_PATTERN.fullmatch(transaction_id) is None
            or not isinstance(accepted_at, str)
            or not accepted_at
            or re.fullmatch(r"[0-9a-f]{64}", request_fingerprint) is None
            or _contains_forbidden_key(work)
            or type(execution_released) is not bool
            or type(publish_desired) is not bool
            or (saved is not None and type(saved) is not bool)
            or (verified is not None and type(verified) is not bool)
            or verified is True
            and saved is False
        ):
            raise NetworkStateError("state_input_invalid", "network transaction input is invalid")
        scope = self._scope(principal_id, idempotency_key)
        with self._thread_lock, _network_lock(self._state_dir):
            state = self._load_locked()
            existing = state["receipts"].get(scope)
            if existing is not None:
                if (
                    not isinstance(existing, Mapping)
                    or existing.get("request_fingerprint") != request_fingerprint
                ):
                    raise NetworkStateError(
                        "idempotency_conflict",
                        "idempotency key was already used for another request",
                    )
                receipt = existing.get("receipt")
                if not isinstance(receipt, Mapping):
                    raise NetworkStateError("state_invalid", "stored network receipt is invalid")
                return deepcopy(dict(receipt)), True
            if state["transaction"]["current"] is not None:
                raise NetworkStateError(
                    "controller_busy",
                    "another network transaction is still active",
                    retryable=True,
                )
            next_revision = state["source_revision"] + 1
            transaction = {
                "schema": TRANSACTION_SCHEMA,
                "authority_epoch": state["authority_epoch"],
                "source_revision": next_revision,
                "transaction_id": transaction_id,
                "operation": operation,
                "status": "accepted",
                "stage": "accepted",
                "desired": deepcopy(dict(desired)),
                "accepted_at": accepted_at,
                "updated_at": accepted_at,
                "deadline": None,
                "recovery_action": "await_device",
                "rescue": {
                    "ap_validated": False,
                    "fallback_mode": "hotspot",
                    "failure_trigger_seconds": 10,
                },
                "error": None,
            }
            receipt = {
                "schema": RECEIPT_SCHEMA,
                "accepted_at": accepted_at,
                "transaction": deepcopy(transaction),
            }
            state["source_revision"] = next_revision
            if publish_desired:
                state["desired"] = deepcopy(dict(desired))
            if saved is not None:
                state["saved"] = saved
            elif publish_desired:
                state["saved"] = desired.get("mode") == "wifi-client"
            if verified is not None:
                state["verified"] = verified
            elif publish_desired:
                state["verified"] = False
            state["transaction"]["current"] = transaction
            state["execution_release"] = {transaction_id: execution_released}
            state["receipts"][scope] = {
                "request_fingerprint": request_fingerprint,
                "receipt": receipt,
            }
            state["work"][transaction_id] = deepcopy(dict(work))
            self._validate(state)
            self._write_locked(state)
        return deepcopy(receipt), False

    def execution_released(self, transaction_id: str) -> bool:
        with self._thread_lock, _network_lock(self._state_dir):
            state = self._load_locked()
            current = state["transaction"]["current"]
            return bool(
                isinstance(current, Mapping)
                and current.get("transaction_id") == transaction_id
                and state["execution_release"].get(transaction_id) is True
            )

    def boot_requires_rescue(self, boot_id: str) -> bool:
        if not _valid_boot_id(boot_id):
            raise NetworkStateError("state_input_invalid", "Linux boot ID is invalid")
        with self._thread_lock, _network_lock(self._state_dir):
            return self._load_locked()["rescue_boot_id"] != boot_id

    def mark_boot_rescue_validated(self, boot_id: str) -> None:
        if not _valid_boot_id(boot_id):
            raise NetworkStateError("state_input_invalid", "Linux boot ID is invalid")
        with self._thread_lock, _network_lock(self._state_dir):
            state = self._load_locked()
            if state["rescue_boot_id"] == boot_id:
                return
            state["rescue_boot_id"] = boot_id
            self._validate(state)
            self._write_locked(state)

    def release_execution(self, transaction_id: str) -> bool:
        with self._thread_lock, _network_lock(self._state_dir):
            state = self._load_locked()
            current = state["transaction"]["current"]
            if not isinstance(current, Mapping) or current.get("transaction_id") != transaction_id:
                return False
            if state["execution_release"].get(transaction_id) is not True:
                state["execution_release"][transaction_id] = True
                self._validate(state)
                self._write_locked(state)
            return True

    def work_for(self, transaction_id: str) -> dict[str, Any]:
        with self._thread_lock, _network_lock(self._state_dir):
            state = self._load_locked()
            work = state["work"].get(transaction_id)
            if not isinstance(work, Mapping):
                raise NetworkStateError(
                    "transaction_not_found", "network transaction was not found"
                )
            return deepcopy(dict(work))

    def retained_profiles(self) -> set[str]:
        retained: set[str] = set()
        with self._thread_lock, _network_lock(self._state_dir):
            state = self._load_locked()
            for work in state["work"].values():
                candidate = work.get("candidate") if isinstance(work, Mapping) else None
                profile = candidate.get("profile") if isinstance(candidate, Mapping) else None
                if isinstance(profile, str) and re.fullmatch(
                    r"rp-ylx-(?:wifi-client|ethernet-dhcp|ethernet-static)-[0-9a-f]{12}",
                    profile,
                ):
                    retained.add(profile)
        return retained

    def transition(
        self,
        transaction_id: str,
        *,
        status: str,
        stage: str,
        updated_at: str,
        ap_validated: bool | None = None,
        error: Mapping[str, Any] | None = None,
        desired: Mapping[str, Any] | None = None,
        publish_desired: bool = False,
        deadline: Mapping[str, Any] | None = None,
        recovery_action: str = "await_device",
        saved: bool | None = None,
        verified: bool | None = None,
        retain_work: bool = True,
        clear_all_work: bool = False,
        remove_work: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        if (
            status not in TRANSACTION_STATUSES
            or stage not in TRANSACTION_STAGES
            or not isinstance(updated_at, str)
            or not updated_at
            or (ap_validated is not None and type(ap_validated) is not bool)
            or (desired is not None and not _valid_desired(desired))
            or type(publish_desired) is not bool
            or not _valid_deadline(deadline)
            or recovery_action not in RECOVERY_ACTIONS
            or (saved is not None and type(saved) is not bool)
            or (verified is not None and type(verified) is not bool)
            or verified is True
            and saved is False
            or type(retain_work) is not bool
            or type(clear_all_work) is not bool
            or any(not isinstance(item, str) for item in remove_work)
        ):
            raise NetworkStateError("state_input_invalid", "network transition is invalid")
        with self._thread_lock, _network_lock(self._state_dir):
            state = self._load_locked()
            current = state["transaction"]["current"]
            if not isinstance(current, dict) or current.get("transaction_id") != transaction_id:
                raise NetworkStateError("transaction_not_found", "active transaction was not found")
            current["status"] = status
            current["stage"] = stage
            current["updated_at"] = updated_at
            state["source_revision"] += 1
            current["source_revision"] = state["source_revision"]
            current["deadline"] = None if deadline is None else deepcopy(dict(deadline))
            current["recovery_action"] = recovery_action
            if ap_validated is not None:
                current["rescue"]["ap_validated"] = ap_validated
            current["error"] = None if error is None else deepcopy(dict(error))
            if desired is not None:
                current["desired"] = deepcopy(dict(desired))
                if publish_desired:
                    state["desired"] = deepcopy(dict(desired))
            if saved is not None:
                state["saved"] = saved
            if verified is not None:
                state["verified"] = verified
            if status in TERMINAL_STATUSES:
                state["transaction"]["latest"] = deepcopy(current)
                state["transaction"]["current"] = None
                state["execution_release"].pop(transaction_id, None)
                if not retain_work:
                    state["work"].pop(transaction_id, None)
            if clear_all_work:
                state["work"] = {}
            else:
                for work_id in remove_work:
                    state["work"].pop(work_id, None)
            self._validate(state)
            self._write_locked(state)
            return deepcopy(current)

    def clear_work(self) -> None:
        with self._thread_lock, _network_lock(self._state_dir):
            state = self._load_locked()
            if state["work"]:
                state["work"] = {}
                state["source_revision"] += 1
                self._validate(state)
                self._write_locked(state)

    def scan_stamp(self) -> tuple[str, int]:
        with self._thread_lock, _network_lock(self._state_dir):
            state = self._load_locked()
            state["source_revision"] += 1
            self._validate(state)
            self._write_locked(state)
            return state["authority_epoch"], state["source_revision"]
