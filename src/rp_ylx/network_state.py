"""Durable authority and transaction state for privileged network mutation."""

from __future__ import annotations

import hashlib
import re
import sqlite3
import threading
import uuid
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any

from rp_ylx.core.ids import UUID_V7_RE, valid_sha256, valid_uuid4, valid_uuid7
from rp_ylx.network_paths import desired_state_from_disk, network_lock, prepare_state_dir
from rp_ylx.network_validation import (
    CONTROLLER_STATE_SCHEMA,
    RECEIPT_SCHEMA,
    RECOVERY_ACTIONS,
    TERMINAL_STATUSES,
    TRANSACTION_SCHEMA,
    TRANSACTION_STAGES,
    TRANSACTION_STATUSES,
    valid_deadline,
    valid_desired_state,
    valid_transaction,
)
from rp_ylx.network_validation import (
    STAGE_STATUS as STAGE_STATUS,
)
from rp_ylx.network_validation import (
    TRANSACTION_ERROR_CODES as TRANSACTION_ERROR_CODES,
)
from rp_ylx.network_validation import (
    WIFI_SECURITY as WIFI_SECURITY,
)
from rp_ylx.persistence import StateDatabase

MAX_RETAINED_RECEIPTS = 2048
UUID_V7_PATTERN = UUID_V7_RE
FORBIDDEN_PERSISTED_KEYS = frozenset(
    {"credential_ref", "password", "passphrase", "psk", "secret", "token"}
)


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


def _valid_boot_id(value: object) -> bool:
    if value is None:
        return True
    try:
        parsed = uuid.UUID(str(value))
    except (ValueError, AttributeError):
        return False
    return isinstance(value, str) and str(parsed) == value


def _valid_receipts(value: object, authority_epoch: str, source_revision: int) -> bool:
    if not isinstance(value, Mapping):
        return False
    for scope, entry in value.items():
        if (
            not valid_sha256(scope)
            or not isinstance(entry, Mapping)
            or set(entry) != {"request_fingerprint", "receipt"}
            or not valid_sha256(entry.get("request_fingerprint"))
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
            or not valid_transaction(transaction)
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
        and valid_uuid4(authority_epoch)
        and type(source_revision) is int
        and source_revision >= 0
        and type(value.get("saved")) is bool
        and type(value.get("verified")) is bool
        and (not value["verified"] or value["saved"])
        and valid_desired_state(value.get("desired"))
        and (current is None or valid_transaction(current))
        and (latest is None or valid_transaction(latest))
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
            valid_uuid7(transaction_id) and type(released) is bool
            for transaction_id, released in execution_release.items()
        )
        and (
            not execution_release
            if current is None
            else set(execution_release) == {current.get("transaction_id")}
        )
        and not _contains_forbidden_key(value)
    )


def _prune_receipts_locked(
    state: dict[str, Any], *, max_entries: int = MAX_RETAINED_RECEIPTS
) -> None:
    receipts = state.get("receipts")
    if not isinstance(receipts, dict) or len(receipts) <= max_entries:
        return
    protected: set[str] = set()
    transaction = state.get("transaction", {})
    for key in ("current", "latest"):
        item = transaction.get(key) if isinstance(transaction, Mapping) else None
        if isinstance(item, Mapping) and isinstance(item.get("transaction_id"), str):
            protected.add(str(item["transaction_id"]))
    work = state.get("work")
    if isinstance(work, Mapping):
        protected.update(str(work_id) for work_id in work)

    removable: list[tuple[int, str, str]] = []
    for scope, entry in receipts.items():
        tx = entry.get("receipt", {}).get("transaction", {}) if isinstance(entry, Mapping) else {}
        tx_id = tx.get("transaction_id") if isinstance(tx, Mapping) else None
        if isinstance(tx_id, str) and tx_id in protected:
            continue
        revision = tx.get("source_revision") if isinstance(tx, Mapping) else 0
        accepted_at = str(tx.get("accepted_at", "")) if isinstance(tx, Mapping) else ""
        removable.append((revision if type(revision) is int else 0, accepted_at, scope))
    removable.sort()
    excess = len(receipts) - max_entries
    for _, _, scope in removable[:excess]:
        receipts.pop(scope, None)


class NetworkStateStore:
    """Persist one authoritative desired state and current/latest v1 transaction window."""

    def __init__(self, state_dir: Path) -> None:
        self._state_dir = Path(state_dir)
        self._path = self._state_dir / "controller-state.json"
        self._thread_lock = threading.RLock()
        prepare_state_dir(self._state_dir)
        self._database = StateDatabase(self._state_dir / "controller.sqlite3")

        def validate_import(value):
            migrated = deepcopy(value)
            current = migrated.get("transaction", {}).get("current")
            migrated.setdefault(
                "execution_release",
                ({str(current["transaction_id"]): True} if isinstance(current, Mapping) else {}),
            )
            migrated.setdefault("rescue_boot_id", None)
            self._validate(migrated)

        def initial_state():
            desired = desired_state_from_disk(self._state_dir)
            saved = desired.get("mode") != "hotspot"
            return {
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

        with self._thread_lock, network_lock(self._state_dir):
            state = self._database.import_json(
                "controller", self._path, validate=validate_import, default_factory=initial_state
            )
            migrated = False
            if "execution_release" not in state:
                current = state.get("transaction", {}).get("current")
                state["execution_release"] = (
                    {str(current["transaction_id"]): True} if isinstance(current, Mapping) else {}
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
        state = self._database.get("controller")
        if state is None:
            raise NetworkStateError("state_invalid", "network controller state is missing")
        self._validate(state)
        return state

    def _write_locked(self, state: Mapping[str, Any]) -> None:
        """Resolve an ambiguous commit only against the committed database value."""
        try:
            self._database.put("controller", state)
        except (OSError, sqlite3.Error):
            if self._database.get("controller") != state:
                raise

    def snapshot(self) -> dict[str, Any]:
        with self._thread_lock, network_lock(self._state_dir):
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
        with self._thread_lock, network_lock(self._state_dir):
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
            or not valid_desired_state(desired)
            or not valid_uuid7(transaction_id)
            or not isinstance(accepted_at, str)
            or not accepted_at
            or not valid_sha256(request_fingerprint)
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
        with self._thread_lock, network_lock(self._state_dir):
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
                state["saved"] = desired.get("mode") != "hotspot"
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
            _prune_receipts_locked(state)
            self._validate(state)
            self._write_locked(state)
        return deepcopy(receipt), False

    def execution_released(self, transaction_id: str) -> bool:
        with self._thread_lock, network_lock(self._state_dir):
            state = self._load_locked()
            current = state["transaction"]["current"]
            return bool(
                isinstance(current, Mapping)
                and current.get("transaction_id") == transaction_id
                and state["execution_release"].get(transaction_id) is True
            )

    def boot_needs_reconcile(self, boot_id: str) -> bool:
        if not _valid_boot_id(boot_id):
            raise NetworkStateError("state_input_invalid", "Linux boot ID is invalid")
        with self._thread_lock, network_lock(self._state_dir):
            return self._load_locked()["rescue_boot_id"] != boot_id

    def mark_boot_reconciled(self, boot_id: str) -> None:
        # Keep the legacy on-disk key for compatibility with installed releases.
        # It tracks boot reconciliation, not whether an AP was activated.
        if not _valid_boot_id(boot_id):
            raise NetworkStateError("state_input_invalid", "Linux boot ID is invalid")
        with self._thread_lock, network_lock(self._state_dir):
            state = self._load_locked()
            if state["rescue_boot_id"] == boot_id:
                return
            state["rescue_boot_id"] = boot_id
            self._validate(state)
            self._write_locked(state)

    def release_execution(self, transaction_id: str) -> bool:
        with self._thread_lock, network_lock(self._state_dir):
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
        with self._thread_lock, network_lock(self._state_dir):
            state = self._load_locked()
            work = state["work"].get(transaction_id)
            if not isinstance(work, Mapping):
                raise NetworkStateError(
                    "transaction_not_found", "network transaction was not found"
                )
            return deepcopy(dict(work))

    def work_ids(self) -> tuple[str, ...]:
        with self._thread_lock, network_lock(self._state_dir):
            state = self._load_locked()
            return tuple(str(key) for key in state["work"])

    def retained_profiles(self) -> set[str]:
        retained: set[str] = set()
        with self._thread_lock, network_lock(self._state_dir):
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
            or (desired is not None and not valid_desired_state(desired))
            or type(publish_desired) is not bool
            or not valid_deadline(deadline)
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
        with self._thread_lock, network_lock(self._state_dir):
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
            _prune_receipts_locked(state)
            self._validate(state)
            self._write_locked(state)
            return deepcopy(current)

    def scan_stamp(self) -> tuple[str, int]:
        with self._thread_lock, network_lock(self._state_dir):
            state = self._load_locked()
            state["source_revision"] += 1
            self._validate(state)
            self._write_locked(state)
            return state["authority_epoch"], state["source_revision"]
