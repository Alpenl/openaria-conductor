"""Coordinator durable SQLite state, command idempotency receipts, and settings."""

from __future__ import annotations

import copy
import json
import os
import threading
import uuid
from collections.abc import Callable, Mapping
from http import HTTPStatus
from pathlib import Path

from rp_ylx.api.events import (
    validate_capture_status,
    validate_retained_unsuccessful_outcome,
    validate_safe_swap_v3_receipt,
)
from rp_ylx.api.gateway import CaptureCommand, CaptureCommandResult, ProviderError
from rp_ylx.camera_focus import valid_camera_focus_status
from rp_ylx.capture_timer import DEFAULT_TIMER, valid_timer_settings
from rp_ylx.operational_logging import operational_logger
from rp_ylx.persistence import PersistenceError, StateDatabase
from rp_ylx.recording.deletion import valid_delete_request
from rp_ylx.recording.device_session import SessionPlan
from rp_ylx.recording.volume import VolumeAdmission
from rp_ylx.voice_feedback import notify as notify_voice
from rp_ylx.voice_feedback import valid_volume

SAFE_SWAP_FAILURES = {
    "safe_swap_unverified": (
        HTTPStatus.INTERNAL_SERVER_ERROR,
        "待交接录像的信息已变化或无法读取，请重试",
    ),
    "safe_swap_publish_failed": (
        HTTPStatus.INTERNAL_SERVER_ERROR,
        "安全换盘回执未能持久发布，请重试",
    ),
    "safe_swap_blocked": (
        HTTPStatus.INTERNAL_SERVER_ERROR,
        "仍有读取句柄未释放，请稍后重试",
    ),
    "safe_swap_resume_failed": (
        HTTPStatus.INTERNAL_SERVER_ERROR,
        "存储交接复核失败，请检查设备日志后重试",
    ),
}


def validated_command_receipt(item: Mapping[str, object]) -> tuple[bytes, CaptureCommandResult]:
    """Validate a durable response before replaying it or deriving side effects."""
    if set(item) != {
        "principal_id",
        "operation",
        "idempotency_key",
        "canonical_body",
        "status",
        "body",
    }:
        raise ValueError("invalid command receipt fields")
    canonical = item["canonical_body"]
    if not isinstance(canonical, str) or not canonical:
        raise ValueError("invalid canonical request")
    payload = canonical.encode("utf-8")
    request = json.loads(payload)
    if not isinstance(request, dict):
        raise ValueError("invalid canonical request object")
    json.dumps(request, allow_nan=False, ensure_ascii=False).encode("utf-8")
    json.dumps(item["body"], allow_nan=False, ensure_ascii=False).encode("utf-8")
    status, body, operation = item["status"], item["body"], item["operation"]
    if type(status) is not int:
        raise ValueError("invalid command response status")
    if operation in {"start", "stop"} and status == HTTPStatus.ACCEPTED:
        validate_capture_status(body)
    elif operation == "stop" and status == HTTPStatus.NO_CONTENT and body is None:
        pass
    elif operation == "set_camera_focus" and status == HTTPStatus.OK:
        if not valid_camera_focus_status(body):
            raise ValueError("invalid focus receipt")
    elif operation == "delete" and status == HTTPStatus.OK:
        if (
            not valid_delete_request(request)
            or not isinstance(body, dict)
            or set(body) != {"schema", "deleted_session_ids", "failed_sessions"}
            or body["schema"] != "ylx.session-delete-result.v1"
            or not isinstance(body["deleted_session_ids"], list)
            or not all(isinstance(sid, str) for sid in body["deleted_session_ids"])
            or not isinstance(body["failed_sessions"], list)
        ):
            raise ValueError("invalid deletion receipt")
        sessions = list(body["deleted_session_ids"])
        for failure in body["failed_sessions"]:
            if (
                not isinstance(failure, dict)
                or set(failure) != {"session_id", "error"}
                or not isinstance(failure["session_id"], str)
                or not isinstance(failure["error"], str)
                or not failure["error"]
            ):
                raise ValueError("invalid deletion failure")
            sessions.append(failure["session_id"])
        expected = {entry["session_id"] for entry in request["sessions"]}
        if len(sessions) != len(set(sessions)) or set(sessions) != expected:
            raise ValueError("deletion receipt differs from request")
    else:
        raise ValueError("invalid command operation or status")
    return payload, CaptureCommandResult(status, copy.deepcopy(body), replayed=False)


class CoordinatorStateStore:
    """Owns SQLite state persistence, idempotency receipts, and local settings."""

    def __init__(
        self,
        state_root: Path,
        *,
        notify_fn: Callable[..., None] | None = None,
    ) -> None:
        self._notify_fn = notify_fn or notify_voice
        state_root.mkdir(parents=True, exist_ok=True, mode=0o750)
        self._state_path = state_root / "capture-coordinator.json"
        self._state_db = StateDatabase(state_root / "state.sqlite3")
        self._state_db.import_json("capture", self._state_path)
        self._state_db.interrupt_transfers()

        self._state_persist_lock = threading.Lock()
        self._state_write_sequence = 0
        self._state_persisted_sequence = 0

        self._retained: dict[str, dict[str, object]] = {}
        self._recovery_session: dict[str, str] | None = None
        self._pending_safe_swap: dict[str, object] | None = None
        self._pending_safe_swap_failure: dict[str, object] | None = None
        self._safe_swap_resource: dict[str, object] | None = None
        self._safe_swap_to_persist: dict[str, object] | None = None
        self._safe_swap_failure_clear_pending = False

        self._commands: dict[tuple[str, str, str], tuple[bytes, CaptureCommandResult]] = {}
        self._invalid_commands: dict[tuple[str, str, str], object] = {}
        self._persisted_commands: dict[
            tuple[str, str, str], tuple[bytes, CaptureCommandResult]
        ] = {}

        self._voice_settings = self._state_db.get("voice_feedback")
        if self._voice_settings is None:
            self._voice_settings = {"volume": 100}
        self._timer_settings = self._state_db.get("capture_timer")
        if self._timer_settings is None:
            self._timer_settings = dict(DEFAULT_TIMER)

        self._focus_restore_invalid = False
        self._focus_to_persist: dict[str, object] | None = None
        self._persisted_focus: dict[str, object] | None = None
        try:
            self._pending_focus = self._state_db.get("camera_focus")
        except (ValueError, TypeError, RecursionError, PersistenceError) as error:
            # Preserve corrupt optional settings for explicit correction. An
            # actual database I/O error still propagates instead of being ignored.
            self._pending_focus = None
            self._focus_restore_invalid = True
            operational_logger("recording").event(
                "camera_focus_restore_rejected",
                level="warning",
                exception_type=type(error).__name__,
            )

        persisted = self.read_local_state()
        self._authority_epoch = str(persisted.get("authority_epoch") or uuid.uuid4())
        self._revision = int(persisted.get("source_revision", 0))
        binding = persisted.get("volume_binding")
        self._last_volume_binding = copy.deepcopy(binding) if isinstance(binding, Mapping) else None
        self._initial_persisted = persisted

    def read_local_state(self) -> Mapping[str, object]:
        """Load the authoritative `capture` state document and command receipts."""
        return self._state_db.capture_state()

    def restore_local_state(
        self,
        persisted: Mapping[str, object],
        admission: VolumeAdmission | None,
        *,
        on_released: Callable[[], None],
        on_forget_deleted: Callable[[list[str]], None],
    ) -> None:
        """Validate and restore durable state after startup or late admission."""
        self._recovery_session = None
        identity = persisted.get("active_session")
        if isinstance(identity, dict) and set(identity) == {
            "session_id",
            "volume_id",
            "generation_id",
        }:
            try:
                for key, version in (("session_id", 7), ("volume_id", 4), ("generation_id", 4)):
                    parsed = uuid.UUID(identity[key])
                    if parsed.version != version or str(parsed) != identity[key]:
                        raise ValueError("invalid active session identity")
            except (ValueError, TypeError, AttributeError):
                pass
            else:
                self._recovery_session = dict(identity)
        retained = persisted.get("retained")
        if isinstance(retained, Mapping):
            self._retained = {}
            rejected = 0
            for key, value in retained.items():
                state = value.get("recording_state") if isinstance(value, dict) else None
                if not isinstance(key, str) or not isinstance(state, Mapping):
                    rejected += 1
                    continue
                try:
                    validate_retained_unsuccessful_outcome(
                        {
                            "schema": "ylx.retained-unsuccessful-session-resource.v2",
                            "authority_epoch": state.get("authority_epoch"),
                            "source_revision": state.get("state_revision"),
                            "outcome": value,
                        },
                        session_id=key,
                    )
                except (ValueError, KeyError, TypeError, RecursionError):
                    rejected += 1
                    continue
                self._retained[key] = copy.deepcopy(value)
            if rejected:
                operational_logger("recording").event(
                    "retained_outcomes_rejected", level="warning", count=rejected
                )
        safe_swap = persisted.get("safe_swap_resource")
        if safe_swap is not None:
            try:
                if (
                    not isinstance(safe_swap, dict)
                    or set(safe_swap) != {"schema", "receipt"}
                    or safe_swap["schema"] != "ylx.safe-swap-receipt-resource.v3"
                    or not isinstance(safe_swap["receipt"], dict)
                ):
                    raise ValueError("invalid persisted safe-swap resource")
                validate_safe_swap_v3_receipt(safe_swap["receipt"])
            except (ValueError, KeyError, TypeError, RecursionError):
                operational_logger("recording").event(
                    "safe_swap_state_rejected", level="warning", field="safe_swap_resource"
                )
                safe_swap = None
        if isinstance(safe_swap, dict):
            receipt = safe_swap.get("receipt")
            if (
                admission is not None
                and isinstance(receipt, Mapping)
                and receipt.get("volume_id") == admission.volume_id
                and receipt.get("generation_id") == admission.generation_id
            ):
                self._safe_swap_resource = copy.deepcopy(safe_swap)
                on_released()
        pending = persisted.get("pending_safe_swap")
        if pending is not None:
            try:
                if not isinstance(pending, dict) or set(pending) != {
                    "session_id",
                    "volume_id",
                    "generation_id",
                    "manifest_id",
                    "manifest_sha256",
                    "sealed_at",
                }:
                    raise ValueError("invalid persisted safe-swap intent")
                # Reuse the frozen receipt field rules for the six intent fields.
                # Publication rechecks the saved manifest and released handles.
                validate_safe_swap_v3_receipt(
                    {
                        **pending,
                        "schema": "ylx.safe-swap-receipt.v3",
                        "released_at": pending["sealed_at"],
                        "release_state": "device-released",
                        "open_handle_count": 0,
                    }
                )
            except (ValueError, KeyError, TypeError, RecursionError):
                operational_logger("recording").event(
                    "safe_swap_state_rejected", level="warning", field="pending_safe_swap"
                )
                pending = None
        if (
            admission is not None
            and isinstance(pending, dict)
            and pending.get("volume_id") == admission.volume_id
            and pending.get("generation_id") == admission.generation_id
        ):
            self._pending_safe_swap = copy.deepcopy(pending)
            failure = persisted.get("pending_safe_swap_failure")
            if (
                isinstance(failure, Mapping)
                and set(failure) == {"session_id", "manifest_sha256", "code"}
                and isinstance(failure.get("code"), str)
                and failure["code"] in SAFE_SWAP_FAILURES
                and failure["session_id"] == pending.get("session_id")
                and failure["manifest_sha256"] == pending.get("manifest_sha256")
            ):
                self._pending_safe_swap_failure = copy.deepcopy(dict(failure))
        invalid = persisted.get("invalid_commands", [])
        if not isinstance(invalid, list):
            raise PersistenceError("invalid quarantined command identities")
        for entry in invalid:
            if not isinstance(entry, dict) or not isinstance(entry.get("scope"), list):
                raise PersistenceError("invalid quarantined command identity")
            scope = tuple(entry["scope"])
            if len(scope) != 3 or not all(isinstance(part, str) and part for part in scope):
                raise PersistenceError("invalid quarantined command identity")
            self._invalid_commands[scope] = copy.deepcopy(entry.get("receipt"))
        commands = persisted.get("commands")
        if "commands" in persisted and not isinstance(commands, list):
            raise PersistenceError("invalid legacy command receipt collection")
        if isinstance(commands, list):
            for item in commands[-1024:]:
                if not isinstance(item, Mapping):
                    raise PersistenceError("unidentifiable legacy command receipt")
                scope = tuple(
                    item.get(key)
                    for key in (
                        "principal_id",
                        "operation",
                        "idempotency_key",
                    )
                )
                if not all(isinstance(part, str) and part for part in scope):
                    raise PersistenceError("unidentifiable legacy command receipt")
                try:
                    receipt = validated_command_receipt(item)
                except (ValueError, KeyError, TypeError, RecursionError, OverflowError):
                    self._invalid_commands[scope] = json.dumps(item, ensure_ascii=True)
                    self._commands.pop(scope, None)
                else:
                    if scope not in self._invalid_commands:
                        self._commands[scope] = receipt
        if self._invalid_commands:
            operational_logger("recording").event(
                "command_receipts_rejected", level="warning", count=len(self._invalid_commands)
            )

        for (_, operation, _), (_, result) in self._commands.items():
            body = result.body
            if (
                operation == "delete"
                and result.status == HTTPStatus.OK
                and isinstance(body, Mapping)
                and body.get("schema") == "ylx.session-delete-result.v1"
            ):
                deleted = body.get("deleted_session_ids")
                if isinstance(deleted, list) and all(isinstance(sid, str) for sid in deleted):
                    on_forget_deleted(deleted)

    def build_local_state_write(
        self,
        *,
        admission: VolumeAdmission | None,
        active: object | None,
        active_plan: SessionPlan | None,
        failure_pending_commit: object | None = None,
    ) -> tuple[
        int, dict[str, object], dict[tuple[str, str, str], tuple[bytes, CaptureCommandResult]]
    ]:
        """Serialize a consistent coordinator state snapshot under the lifecycle lock."""
        if admission is not None:
            self._last_volume_binding = {
                "volume_id": admission.volume_id,
                "generation_id": admission.generation_id,
                "device": admission.device,
                "mount_identity": admission.mount_identity,
            }
        # Never persist a non-null active_session for a failed take whose
        # recovery has already finished and is committing its retained outcome.
        active_identity = (
            {
                "session_id": active_plan.session_id,
                "volume_id": active_plan.volume_id,
                "generation_id": active_plan.generation_id,
            }
            if active is not None
            and active_plan is not None
            and failure_pending_commit is not active
            else self._recovery_session
        )
        value: dict[str, object] = {
            "schema": "ylx.capture-coordinator-state.v1",
            "invalid_commands": [
                {
                    "scope": list(scope),
                    **({"receipt": copy.deepcopy(receipt)} if receipt is not None else {}),
                }
                for scope, receipt in self._invalid_commands.items()
            ],
            "authority_epoch": self._authority_epoch,
            "source_revision": self._revision,
            "volume_binding": copy.deepcopy(self._last_volume_binding),
            "active_session": active_identity,
            "retained": self._retained,
            "pending_safe_swap": self._pending_safe_swap,
            "pending_safe_swap_failure": self._pending_safe_swap_failure,
            "safe_swap_resource": self._safe_swap_resource,
        }
        if self._safe_swap_failure_clear_pending:
            value["pending_safe_swap_failure"] = None
        if self._safe_swap_to_persist is not None:
            # The handoff owner stages a durable snapshot without exposing an
            # uncommitted success through status or the safe-swap endpoint.
            value["safe_swap_resource"] = self._safe_swap_to_persist
            value["pending_safe_swap"] = None
            value["pending_safe_swap_failure"] = None
        self._state_write_sequence += 1
        return (
            self._state_write_sequence,
            copy.deepcopy(value),
            dict(self._commands),
        )

    def persist_local_state(
        self,
        lock: threading.RLock,
        snapshot_builder: Callable[
            [],
            tuple[
                int,
                dict[str, object],
                dict[tuple[str, str, str], tuple[bytes, CaptureCommandResult]],
                dict[str, object] | None,
                tuple[str, str, str] | None,
            ],
        ],
        on_committed: Callable[[tuple[str, str, str] | None, dict[str, object]], None],
        lock_override: threading.Lock | None = None,
    ) -> None:
        """Snapshot under `lock`, persist under `_state_persist_lock`, then publish under `lock`."""
        with lock:
            sequence, value, commands, focus, pending_stop = snapshot_builder()
        persist_lock = lock_override if lock_override is not None else self._state_persist_lock
        with persist_lock:
            if sequence <= self._state_persisted_sequence:
                return
            new_commands = [
                {
                    "principal_id": scope[0],
                    "operation": scope[1],
                    "idempotency_key": scope[2],
                    "canonical_body": receipt[0].decode("utf-8"),
                    "status": int(receipt[1].status),
                    "body": receipt[1].body,
                }
                for scope, receipt in commands.items()
                if self._persisted_commands.get(scope) is not receipt
            ]
            removed = self._persisted_commands.keys() - commands.keys()
            if focus is not None and focus is not self._persisted_focus:
                self._state_db.put_capture_state(value, new_commands, removed, camera_focus=focus)
            else:
                self._state_db.put_capture_state(value, new_commands, removed)
            self._persisted_focus = focus
            self._persisted_commands = commands
            self._state_persisted_sequence = sequence
        with lock:
            on_committed(pending_stop, value)
            if self._invalid_commands:
                self._invalid_commands = dict.fromkeys(self._invalid_commands)

    def require_valid_command_receipt(self, scope: tuple[str, str, str]) -> None:
        if scope in self._invalid_commands:
            raise ProviderError(
                "command_receipt_invalid",
                "该请求的持久化回执损坏，无法安全重放；请先核对实际操作结果",
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
                retryable=False,
            )

    def check_command_idempotency(
        self,
        scope: tuple[str, str, str],
        canonical_body: bytes,
    ) -> tuple[bool, CaptureCommandResult | None]:
        """Return `(needs_persist, replay_result)` or raise on invalid/conflicting receipt."""
        self.require_valid_command_receipt(scope)
        previous = self._commands.get(scope)
        if previous is None:
            return False, None
        previous_body, result = previous
        if previous_body != canonical_body:
            raise ProviderError(
                "idempotency_conflict",
                "幂等键已经用于不同请求",
                status=HTTPStatus.CONFLICT,
            )
        replay = CaptureCommandResult(result.status, copy.deepcopy(result.body), replayed=True)
        return self._persisted_commands.get(scope) is not previous, replay

    def cache_command(
        self,
        scope: tuple[str, str, str],
        command: CaptureCommand,
        result: CaptureCommandResult,
    ) -> None:
        self._commands[scope] = (command.canonical_body, copy.deepcopy(result))
        while len(self._commands) > 1024:
            self._commands.pop(next(iter(self._commands)))

    def notify_voice(self, event: str, identity: str = "") -> None:
        volume = self._voice_settings.get("volume")
        self._notify_fn(event, identity, volume=volume if valid_volume(volume) else 0)

    def voice_status(self) -> dict[str, object]:
        volume = self._voice_settings.get("volume")
        if not valid_volume(volume):
            raise ProviderError("invalid_voice_settings", "提示音设置损坏，请重新保存", status=409)
        return {
            "schema": "ylx.voice.v1",
            "enabled": os.environ.get("OPENARIA_VOICE_ENABLED") == "1",
            "volume": volume,
        }

    def save_voice_volume(self, volume: int) -> dict[str, object]:
        value = {"volume": volume}
        self._state_db.put("voice_feedback", value, history=True)
        self._voice_settings = value
        return self.voice_status()

    def require_timer_settings(self) -> None:
        if not valid_timer_settings(self._timer_settings):
            raise ProviderError(
                "invalid_timer_settings",
                "定时设置损坏，请在待机时重新保存",
                status=HTTPStatus.CONFLICT,
            )

    def save_capture_timer(self, settings: Mapping[str, object]) -> dict[str, object]:
        value = dict(settings)
        self._state_db.put("capture_timer", value, history=True)
        self._timer_settings = value
        return dict(self._timer_settings)
