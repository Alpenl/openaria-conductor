"""Stop, async-stop acceptance, and safe-swap publication workflow."""

from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import threading
import time
from collections.abc import Callable, Mapping
from contextlib import closing
from datetime import UTC, datetime
from http import HTTPStatus
from typing import TYPE_CHECKING

from rp_ylx.api.events import validate_safe_swap_v3_receipt
from rp_ylx.api.gateway import CaptureCommand, CaptureCommandResult, ProviderError
from rp_ylx.operational_logging import operational_logger
from rp_ylx.persistence import PersistenceError
from rp_ylx.recording.device_session import DeviceRecordingError, DeviceSessionRecorder, SessionPlan
from rp_ylx.recording.state_store import SAFE_SWAP_FAILURES

if TYPE_CHECKING:
    from rp_ylx.recording.coordinator import CaptureCoordinator

_RETRY_AFTER_ADMISSION = object()


def _require_admission_for_stop(coordinator: CaptureCoordinator) -> None:
    try:
        coordinator._require_admission()
    except DeviceRecordingError as error:
        raise ProviderError(
            error.code,
            error.message,
            status=HTTPStatus.CONFLICT,
            retryable=True,
        ) from error


def request_stop_capture(
    coordinator: CaptureCoordinator,
    command: CaptureCommand,
) -> CaptureCommandResult:
    """Durably accept a stop without holding an HTTP request through sealing."""
    replay = retry_completed_stop_receipt(coordinator, command)
    if replay is not None:
        return replay
    while True:
        with coordinator._lock:
            focus_revision = coordinator._focus_revision
        runtime = coordinator._runtime_snapshot()
        with coordinator._stop_condition:
            coordinator._lifecycle.wait_while_starting_locked()
            if focus_revision != coordinator._focus_revision:
                continue
            prepared = prepare_stop_request(coordinator, command, runtime=runtime)
            if prepared is _RETRY_AFTER_ADMISSION:
                need_admission = True
            else:
                need_admission = False
                break
        if need_admission:
            _require_admission_for_stop(coordinator)
    return prepared() if callable(prepared) else prepared


def prepare_stop_request(
    coordinator: CaptureCoordinator,
    command: CaptureCommand,
    *,
    runtime: Mapping[str, object] | None = None,
) -> CaptureCommandResult | Callable[[], CaptureCommandResult] | object:
    scope = (command.principal_id, "stop", command.idempotency_key)
    reason = command.body.get("reason")
    retry_receipt: CaptureCommandResult | None = None
    if coordinator._closing:
        raise ProviderError(
            "capture_busy", "服务正在关闭", status=HTTPStatus.CONFLICT, retryable=True
        )
    if coordinator._lifecycle._deleting:
        raise ProviderError(
            "capture_busy", "正在清理录像", status=HTTPStatus.CONFLICT, retryable=True
        )
    coordinator._require_valid_command_receipt(scope)
    previous = coordinator._commands.get(scope)
    if coordinator._stop_acceptance_inflight is not None:
        if previous is not None and previous[0] != command.canonical_body:
            raise ProviderError("idempotency_conflict", "幂等键已经用于不同请求", status=409)
        if previous is not None and coordinator._persisted_commands.get(scope) is previous:
            result = previous[1]
            return CaptureCommandResult(
                result.status,
                copy.deepcopy(result.body),
                replayed=True,
            )
        raise ProviderError(
            "capture_busy",
            "停止请求正在持久接收，请重试",
            status=409,
            retryable=True,
        )
    if previous is not None:
        needs_persist, replay_result = coordinator._state.check_command_idempotency(
            scope, command.canonical_body
        )
        assert replay_result is not None
        if needs_persist:
            if coordinator._stop_inflight is not None or (
                coordinator._active is None
                and (reason != "safe_swap" or coordinator._pending_safe_swap is None)
            ):
                raise ProviderError(
                    "capture_busy",
                    "停止回执正在提交，请重试",
                    status=HTTPStatus.CONFLICT,
                    retryable=True,
                )
            return prepare_stop_receipt_commit(coordinator, command, scope, previous[1], retry=True)
        result = previous[1]
        if (
            reason == "safe_swap"
            and coordinator._async_stop_worker is None
            and coordinator._pending_safe_swap is not None
            and isinstance(result.body, Mapping)
            and isinstance(
                accepted_active := result.body.get("snapshot", {}).get("active_recording"),
                Mapping,
            )
            and accepted_active.get("recording_state", {}).get("session_id")
            == coordinator._pending_safe_swap["session_id"]
        ):
            return request_pending_safe_swap(
                coordinator, command, scope, replay=result, runtime=runtime
            )
        if (
            coordinator._failure_pending_commit is coordinator._active
            and coordinator._active_plan is not None
            and coordinator._async_stop_worker is None
            and coordinator._stop_inflight is None
            and coordinator._failure_inflight is None
            and result.status == HTTPStatus.ACCEPTED
            and isinstance(result.body, Mapping)
            and isinstance(
                retry_active := result.body.get("snapshot", {}).get("active_recording"),
                Mapping,
            )
            and retry_active.get("recording_state", {}).get("session_id")
            == coordinator._active_plan.session_id
        ):
            retry_receipt = result
        else:
            return replay_result
    if coordinator._stop_inflight is not None and coordinator._active is None:
        raise ProviderError(
            "capture_busy", "录制正在收尾", status=HTTPStatus.CONFLICT, retryable=True
        )
    if coordinator._async_stop_worker is not None:
        if reason != coordinator._async_stop_reason:
            raise ProviderError(
                "stop_intent_conflict",
                "已有不同目的的停止请求正在收尾",
                status=HTTPStatus.CONFLICT,
                retryable=True,
            )
        assert runtime is not None
        result = CaptureCommandResult(
            HTTPStatus.ACCEPTED, coordinator._build_capture_status_locked(runtime)
        )
        return prepare_stop_receipt_commit(coordinator, command, scope, result)
    if coordinator._stop_inflight is not None or coordinator._failure_inflight is not None:
        raise ProviderError(
            "capture_busy", "录制正在收尾", status=HTTPStatus.CONFLICT, retryable=True
        )
    if coordinator._active is None:
        if reason == "safe_swap" and coordinator._pending_safe_swap is not None:
            return request_pending_safe_swap(coordinator, command, scope, runtime=runtime)
        if coordinator._admission is None:
            return _RETRY_AFTER_ADMISSION
        return prepare_stop_receipt_commit(
            coordinator,
            command,
            scope,
            CaptureCommandResult(HTTPStatus.NO_CONTENT, None),
        )
    if reason == "safe_swap" and coordinator._open_representations:
        raise ProviderError(
            "safe_swap_blocked",
            "仍有读取句柄未释放",
            status=HTTPStatus.CONFLICT,
            retryable=True,
            details={"open_handle_count": coordinator._open_representations},
        )
    recorder, plan = coordinator._active, coordinator._active_plan
    assert plan is not None and runtime is not None
    result = (
        CaptureCommandResult(retry_receipt.status, copy.deepcopy(retry_receipt.body), replayed=True)
        if retry_receipt is not None
        else CaptureCommandResult(
            HTTPStatus.ACCEPTED, coordinator._build_capture_status_locked(runtime)
        )
    )
    old_commands = dict(coordinator._commands)
    ready = threading.Event()

    def finish() -> None:
        ready.wait()
        with coordinator._stop_condition:
            if coordinator._async_stop_worker is not threading.current_thread():
                return
        try:
            coordinator._finish_stop_capture(command, scope, recorder, plan, remember_result=False)
        except BaseException as error:
            operational_logger("recording").event(
                "stop_finalization_failed",
                level="error",
                transaction_id=plan.session_id,
                exception_type=type(error).__name__,
                code=getattr(error, "code", "stop_finalization_failed"),
            )
            with coordinator._stop_condition:
                still_owned = coordinator._active is recorder and coordinator._active_plan is plan
                pending = coordinator._pending_safe_swap
            if still_owned:
                coordinator._retain_active_failure(
                    DeviceRecordingError("stop_finalization_failed", str(error)),
                    plan,
                    from_stop=True,
                )
            elif (
                reason == "safe_swap"
                and pending is not None
                and pending["session_id"] == plan.session_id
            ):
                record_safe_swap_failure(coordinator, pending, error)
        finally:
            with coordinator._stop_condition:
                if coordinator._async_stop_worker is threading.current_thread():
                    coordinator._async_stop_worker = None
                    coordinator._async_stop_reason = None
                    if (
                        coordinator._active is None
                        and coordinator._pending_stop_state_commit is None
                    ):
                        coordinator._release_network_operation_lease()
                coordinator._sync_phase()
                coordinator._stop_condition.notify_all()

    try:
        worker = threading.Thread(target=finish, name="rp-ylx-stop-finalization", daemon=False)
        coordinator._stop_inflight = scope
        coordinator._async_stop_worker = worker
        coordinator._async_stop_reason = str(reason)
        coordinator._sync_phase()
        worker.start()
    except BaseException as error:
        coordinator._stop_inflight = None
        coordinator._async_stop_worker = None
        coordinator._async_stop_reason = None
        coordinator._sync_phase()
        coordinator._stop_condition.notify_all()
        ready.set()
        if not isinstance(error, (RuntimeError, MemoryError)):
            raise
        raise ProviderError(
            "stop_worker_unavailable",
            "无法启动录制收尾任务",
            status=HTTPStatus.SERVICE_UNAVAILABLE,
            retryable=True,
        ) from error

    def rollback() -> None:
        coordinator._commands = old_commands
        coordinator._stop_inflight = None
        coordinator._async_stop_worker = None
        coordinator._async_stop_reason = None
        coordinator._sync_phase()

    try:
        if retry_receipt is None:
            coordinator._cache_command(scope, command, result)
        coordinator._stop_acceptance_inflight = scope
        coordinator._sync_phase()
    except BaseException:
        ready.set()
        rollback()
        coordinator._stop_condition.notify_all()
        raise
    return lambda: finish_stop_acceptance(
        coordinator,
        scope,
        result,
        rollback,
        persist=retry_receipt is None,
        ready=ready,
        worker=worker,
    )


def prepare_stop_receipt_commit(
    coordinator: CaptureCoordinator,
    command: CaptureCommand,
    scope: tuple[str, str, str],
    result: CaptureCommandResult,
    *,
    retry: bool = False,
) -> Callable[[], CaptureCommandResult]:
    old_commands = dict(coordinator._commands)

    def rollback() -> None:
        coordinator._commands = old_commands

    try:
        if not retry:
            coordinator._cache_command(scope, command, result)
        coordinator._stop_acceptance_inflight = scope
        coordinator._sync_phase()
    except BaseException:
        rollback()
        raise

    def commit() -> CaptureCommandResult:
        committed = finish_stop_acceptance(coordinator, scope, result, rollback)
        return request_stop_capture(coordinator, command) if retry else committed

    return commit


def finish_stop_acceptance(
    coordinator: CaptureCoordinator,
    scope: tuple[str, str, str],
    result: CaptureCommandResult,
    rollback: Callable[[], None],
    *,
    persist: bool = True,
    ready: threading.Event | None = None,
    worker: threading.Thread | None = None,
    accepted: Callable[[], None] | None = None,
    before_accept: Callable[[], None] | None = None,
) -> CaptureCommandResult:
    try:
        if before_accept is not None:
            before_accept()
        if persist:
            coordinator._persist_local_state()
        with coordinator._stop_condition:
            if accepted is not None:
                accepted()
            coordinator._stop_acceptance_inflight = None
            coordinator._sync_phase()
            coordinator._stop_condition.notify_all()
    except BaseException as error:
        try:
            with coordinator._stop_condition:
                try:
                    rollback()
                except BaseException as cleanup_error:
                    error.add_note(
                        "Stop acceptance cleanup failed: " + type(cleanup_error).__name__,
                    )
                source_failure = coordinator._pending_source_failure
                failed_plan = coordinator._active_plan
            if source_failure is not None and failed_plan is not None:
                try:
                    coordinator._retain_active_failure(
                        source_failure,
                        failed_plan,
                        defer_recovery=True,
                    )
                except BaseException as recovery_error:
                    error.add_note(
                        "Deferred source recovery failed: " + type(recovery_error).__name__,
                    )
        finally:
            with coordinator._stop_condition:
                coordinator._stop_acceptance_inflight = None
                coordinator._sync_phase()
                coordinator._stop_condition.notify_all()
            if ready is not None:
                ready.set()
        if worker is not None:
            worker.join()
        raise
    finally:
        if ready is not None:
            ready.set()
    return result


def request_pending_safe_swap(
    coordinator: CaptureCoordinator,
    command: CaptureCommand,
    scope: tuple[str, str, str],
    *,
    replay: CaptureCommandResult | None = None,
    runtime: Mapping[str, object] | None = None,
) -> Callable[[], CaptureCommandResult]:
    """Prepare a gated handoff worker; acquire lease and commit acceptance outside the lock."""
    pending = coordinator._pending_safe_swap
    assert pending is not None and coordinator._async_stop_worker is None
    if replay is not None:
        result = CaptureCommandResult(replay.status, copy.deepcopy(replay.body), replayed=True)
    else:
        assert runtime is not None
        result = CaptureCommandResult(
            HTTPStatus.ACCEPTED, coordinator._build_capture_status_locked(runtime)
        )
    old_commands = dict(coordinator._commands)
    previous_failure = coordinator._pending_safe_swap_failure
    ready = threading.Event()

    def finish() -> None:
        ready.wait()
        with coordinator._stop_condition:
            if coordinator._async_stop_worker is not threading.current_thread():
                return
        try:
            coordinator._publish_safe_swap()
        except BaseException as error:
            operational_logger("recording").event(
                "safe_swap_resume_failed",
                level="error",
                transaction_id=str(pending["session_id"]),
                exception_type=type(error).__name__,
                code=getattr(error, "code", "safe_swap_resume_failed"),
            )
            record_safe_swap_failure(coordinator, pending, error)
        finally:
            with coordinator._stop_condition:
                if coordinator._async_stop_worker is threading.current_thread():
                    coordinator._async_stop_worker = None
                    coordinator._async_stop_reason = None
                    coordinator._stop_inflight = None
                    coordinator._release_network_operation_lease()
                coordinator._sync_phase()
                coordinator._stop_condition.notify_all()

    try:
        worker = threading.Thread(target=finish, name="rp-ylx-safe-swap-resume", daemon=False)
        coordinator._async_stop_worker = worker
        coordinator._async_stop_reason = "safe_swap"
        coordinator._stop_inflight = scope
        coordinator._sync_phase()
        worker.start()
    except BaseException as error:
        ready.set()
        coordinator._async_stop_worker = None
        coordinator._async_stop_reason = None
        coordinator._stop_inflight = None
        coordinator._sync_phase()
        coordinator._stop_condition.notify_all()
        if not isinstance(error, (RuntimeError, MemoryError)):
            raise
        raise ProviderError(
            "stop_worker_unavailable",
            "无法启动存储交接复核任务",
            status=HTTPStatus.SERVICE_UNAVAILABLE,
            retryable=True,
        ) from error

    def rollback() -> None:
        coordinator._commands = old_commands
        coordinator._safe_swap_failure_clear_pending = False
        coordinator._async_stop_worker = None
        coordinator._async_stop_reason = None
        coordinator._stop_inflight = None
        coordinator._release_network_operation_lease()
        coordinator._sync_phase()

    def accepted() -> None:
        coordinator._pending_safe_swap_failure = None
        coordinator._safe_swap_failure_clear_pending = False

    try:
        if replay is None:
            coordinator._cache_command(scope, command, result)
        coordinator._safe_swap_failure_clear_pending = previous_failure is not None
        coordinator._stop_acceptance_inflight = scope
        coordinator._sync_phase()
    except BaseException:
        ready.set()
        rollback()
        coordinator._stop_condition.notify_all()
        raise
    return lambda: finish_stop_acceptance(
        coordinator,
        scope,
        result,
        rollback,
        persist=replay is None or previous_failure is not None,
        ready=ready,
        worker=worker,
        accepted=accepted,
        before_accept=coordinator._acquire_network_operation_lease,
    )


def record_safe_swap_failure(
    coordinator: CaptureCoordinator,
    pending: Mapping[str, object],
    error: BaseException,
) -> None:
    code = getattr(error, "code", "safe_swap_resume_failed")
    if not isinstance(code, str) or code not in SAFE_SWAP_FAILURES:
        code = "safe_swap_resume_failed"
    with coordinator._lock:
        if coordinator._pending_safe_swap is not pending:
            return
        coordinator._pending_safe_swap_failure = {
            "session_id": pending["session_id"],
            "manifest_sha256": pending["manifest_sha256"],
            "code": code,
        }
        coordinator._next_revision()
    try:
        coordinator._persist_local_state()
    except BaseException as persist_error:
        operational_logger("recording").event(
            "safe_swap_failure_persist_failed",
            level="error",
            transaction_id=str(pending["session_id"]),
            exception_type=type(persist_error).__name__,
        )


def stop_capture(
    coordinator: CaptureCoordinator,
    command: CaptureCommand,
    *,
    expected_session_id: str | None = None,
) -> CaptureCommandResult:
    if expected_session_id is None:
        replay = retry_completed_stop_receipt(coordinator, command, wait=True)
        if replay is not None:
            return replay
    scope = (command.principal_id, "stop", command.idempotency_key)
    reason = command.body.get("reason")
    pending_replay: CaptureCommandResult | None = None
    resume_safe_swap = False
    idle_receipt_to_persist: CaptureCommandResult | None = None
    recorder: DeviceSessionRecorder | None = None
    plan: SessionPlan | None = None
    while True:
        with coordinator._stop_condition:
            coordinator._lifecycle.wait_while_starting_locked()
            requested_plan = coordinator._active_plan
            while True:
                if expected_session_id is not None and (
                    coordinator._active_plan is None
                    or coordinator._active_plan.session_id != expected_session_id
                ):
                    return CaptureCommandResult(HTTPStatus.NO_CONTENT, None)
                needs_persist, replay_result = coordinator._state.check_command_idempotency(
                    scope, command.canonical_body
                )
                if replay_result is not None:
                    if needs_persist:
                        if (
                            coordinator._lifecycle._starting
                            or coordinator._lifecycle._deleting
                            or coordinator._stop_inflight is not None
                            or coordinator._failure_inflight is not None
                            or coordinator._async_stop_worker is not None
                            or coordinator._stop_acceptance_inflight is not None
                        ):
                            coordinator._stop_condition.wait()
                            continue
                        pending_replay = replay_result
                        coordinator._stop_inflight = scope
                        coordinator._sync_phase()
                        break
                    return replay_result
                if (
                    coordinator._active_plan is not None
                    and coordinator._active_plan is not requested_plan
                ):
                    if reason == "safe_swap":
                        raise ProviderError(
                            "capture_state_changed",
                            "等待停止期间已开始新的录制，不能安全移除当前存储卷",
                            status=HTTPStatus.CONFLICT,
                        )
                    idle_receipt_to_persist = CaptureCommandResult(HTTPStatus.NO_CONTENT, None)
                    coordinator._cache_command(scope, command, idle_receipt_to_persist)
                    break
                if (
                    not coordinator._lifecycle._starting
                    and not coordinator._lifecycle._deleting
                    and coordinator._stop_inflight is None
                    and coordinator._failure_inflight is None
                    and coordinator._async_stop_worker is None
                    and coordinator._stop_acceptance_inflight is None
                ):
                    break
                coordinator._stop_condition.wait()
            if idle_receipt_to_persist is not None or pending_replay is not None:
                break
            if coordinator._active is None:
                if reason == "safe_swap" and coordinator._pending_safe_swap is not None:
                    coordinator._stop_inflight = scope
                    coordinator._sync_phase()
                    resume_safe_swap = True
                    break
                if coordinator._admission is None:
                    pass
                else:
                    idle_receipt_to_persist = CaptureCommandResult(HTTPStatus.NO_CONTENT, None)
                    coordinator._cache_command(scope, command, idle_receipt_to_persist)
                    break
            else:
                recorder = coordinator._active
                plan = coordinator._active_plan
                assert plan is not None
                coordinator._stop_inflight = scope
                coordinator._sync_phase()
                break
        _require_admission_for_stop(coordinator)

    if idle_receipt_to_persist is not None:
        coordinator._persist_local_state()
        return idle_receipt_to_persist
    if pending_replay is not None:
        return commit_stop_replay(coordinator, scope, pending_replay)
    if resume_safe_swap:
        try:
            coordinator._acquire_network_operation_lease()
            result = coordinator._publish_safe_swap_command()
            with coordinator._lock:
                coordinator._cache_command(scope, command, result)
            coordinator._persist_local_state()
            return result
        finally:
            with coordinator._stop_condition:
                if coordinator._stop_inflight == scope:
                    coordinator._stop_inflight = None
                    coordinator._release_network_operation_lease()
                coordinator._sync_phase()
                coordinator._stop_condition.notify_all()
    assert recorder is not None and plan is not None
    return coordinator._finish_stop_capture(command, scope, recorder, plan)


def commit_stop_replay(
    coordinator: CaptureCoordinator,
    scope: tuple[str, str, str],
    result: CaptureCommandResult,
) -> CaptureCommandResult:
    with coordinator._lock:
        pending = coordinator._pending_stop_state_commit
    try:
        coordinator._persist_local_state()
        with coordinator._lock:
            if coordinator._pending_stop_state_commit == pending:
                coordinator._pending_stop_state_commit = None
            coordinator._sync_phase()
        return result
    finally:
        with coordinator._stop_condition:
            if coordinator._stop_inflight == scope:
                coordinator._stop_inflight = None
            if coordinator._active is None and coordinator._pending_stop_state_commit is None:
                coordinator._release_network_operation_lease()
            coordinator._sync_phase()
            coordinator._stop_condition.notify_all()


def retry_completed_stop_receipt(
    coordinator: CaptureCoordinator,
    command: CaptureCommand,
    *,
    wait: bool = False,
) -> CaptureCommandResult | None:
    scope = (command.principal_id, "stop", command.idempotency_key)
    with coordinator._stop_condition:
        while True:
            coordinator._require_valid_command_receipt(scope)
            previous = coordinator._commands.get(scope)
            pending = coordinator._pending_stop_state_commit
            if pending is None and (
                previous is None or coordinator._persisted_commands.get(scope) is previous
            ):
                return None
            result = None if previous is None else previous[1]
            completed_normal_stop = command.body.get("reason") != "safe_swap" and (
                pending is not None
                or (
                    result is not None
                    and isinstance(result.body, Mapping)
                    and result.body.get("snapshot", {}).get("device_state") == "idle"
                )
            )
            completed_safe_swap = (
                command.body.get("reason") == "safe_swap"
                and result is not None
                and result.status == HTTPStatus.NO_CONTENT
                and coordinator._pending_safe_swap is None
                and coordinator._safe_swap_resource is not None
                and coordinator._released
            )
            if not (coordinator._active is None and (completed_normal_stop or completed_safe_swap)):
                return None
            if previous is not None and previous[0] != command.canonical_body:
                raise ProviderError(
                    "idempotency_conflict", "幂等键已经用于不同请求", status=HTTPStatus.CONFLICT
                )
            if coordinator._closing:
                raise ProviderError(
                    "capture_busy", "服务正在关闭", status=HTTPStatus.CONFLICT, retryable=True
                )
            if (
                coordinator._lifecycle._starting
                or coordinator._lifecycle._deleting
                or coordinator._stop_inflight is not None
                or coordinator._failure_inflight is not None
                or coordinator._async_stop_worker is not None
                or coordinator._stop_acceptance_inflight is not None
            ):
                if wait:
                    coordinator._stop_condition.wait()
                    continue
                raise ProviderError(
                    "capture_busy", "录制正在收尾", status=HTTPStatus.CONFLICT, retryable=True
                )
            replay = (
                CaptureCommandResult(result.status, copy.deepcopy(result.body), replayed=True)
                if result is not None
                else CaptureCommandResult(HTTPStatus.NO_CONTENT, None)
            )
            coordinator._stop_inflight = scope
            coordinator._sync_phase()
            break
    committed = commit_stop_replay(coordinator, scope, replay)
    return committed if previous is not None else None


def finish_stop_capture(
    coordinator: CaptureCoordinator,
    command: CaptureCommand,
    scope: tuple[str, str, str],
    recorder: DeviceSessionRecorder,
    plan: SessionPlan,
    *,
    remember_result: bool = True,
) -> CaptureCommandResult:
    reason = command.body.get("reason")
    stop_started = time.monotonic()
    try:
        if coordinator._sources is not None:
            try:
                coordinator._sources.stop()
            except BaseException as error:
                failure = DeviceRecordingError(
                    "source_stop_failed",
                    f"采集来源未能释放：{error}",
                )
                coordinator._retain_active_failure(failure, plan, from_stop=True)
                raise ProviderError(
                    "source_stop_failed",
                    f"采集来源未能释放：{error}",
                    status=HTTPStatus.CONFLICT,
                ) from error
        if not coordinator._sources_keep_preview():
            coordinator._preview.clear()
        operational_logger("recording").event(
            "stop_stage",
            stage="sources_stop",
            transaction_id=plan.session_id,
            duration_ms=(time.monotonic() - stop_started) * 1000,
        )

        with coordinator._lock:
            retry_failure_commit = coordinator._failure_pending_commit is recorder
        if retry_failure_commit:
            coordinator._commit_active_failure(recorder)
            result = CaptureCommandResult(HTTPStatus.NO_CONTENT, None)
            if remember_result:
                with coordinator._lock:
                    coordinator._cache_command(scope, command, result)
                coordinator._persist_local_state()
            return result

        try:
            with coordinator._lock:
                pending_failure = coordinator._pending_source_failure
            if pending_failure is not None:
                raise pending_failure
            sealed = recorder.stop(
                before_publish=lambda: coordinator._check_generation(
                    plan.generation_id,
                    force=True,
                )
            )
        except DeviceRecordingError as error:
            coordinator._retain_active_failure(error, plan, from_stop=True)
            raise ProviderError(error.code, error.message, status=HTTPStatus.CONFLICT) from error

        with coordinator._lock:
            if coordinator._active is not recorder:
                raise ProviderError(
                    "capture_state_changed",
                    "录制停止期间状态已变化",
                    status=HTTPStatus.CONFLICT,
                    retryable=True,
                )
        published_path = coordinator._publish_recording_directory(sealed.path, sealed.manifest)
        coordinator._catalog.remember_sealed_session(
            volume_id=plan.volume_id,
            published_path=published_path,
            root=sealed.path.parent,
            session_id=plan.session_id,
            manifest=sealed.manifest,
            manifest_bytes=sealed.manifest_bytes,
            manifest_sha256=None,
        )

        with coordinator._lock:
            late_failure = coordinator._pending_source_failure
            if late_failure is not None:
                coordinator._pending_source_failure = None
            if coordinator._active is not recorder:
                raise ProviderError(
                    "capture_state_changed",
                    "录制停止期间状态已变化",
                    status=HTTPStatus.CONFLICT,
                    retryable=True,
                )
            coordinator._active = None
            coordinator._active_plan = None
            coordinator._capture_timer = None
            coordinator._latest_imu = None
            coordinator._next_revision()
            if reason == "safe_swap":
                coordinator._pending_safe_swap = {
                    "session_id": plan.session_id,
                    "volume_id": plan.volume_id,
                    "generation_id": plan.generation_id,
                    "manifest_id": sealed.manifest["manifest_id"],
                    "manifest_sha256": sealed.manifest_sha256,
                    "sealed_at": sealed.manifest["sealed_at"],
                }
                coordinator._sync_phase()
            else:
                coordinator._pending_stop_state_commit = scope
                coordinator._sync_phase()
        if late_failure is not None:
            operational_logger("recording").event(
                "late_source_failure_ignored_after_seal",
                level="warning",
                transaction_id=plan.session_id,
                error_code=late_failure.code,
            )
        if reason == "safe_swap":
            coordinator._persist_local_state()
            result = coordinator._publish_safe_swap_command()
            if remember_result:
                with coordinator._lock:
                    coordinator._cache_command(scope, command, result)
                coordinator._persist_local_state()
            return result
        result = CaptureCommandResult(HTTPStatus.ACCEPTED, coordinator.capture_status())
        if remember_result:
            with coordinator._lock:
                coordinator._cache_command(scope, command, result)
        coordinator._persist_local_state()
        with coordinator._lock:
            if coordinator._pending_stop_state_commit == scope:
                coordinator._pending_stop_state_commit = None
            coordinator._release_network_operation_lease()
            coordinator._sync_phase()
        coordinator._notify_voice("saved", plan.session_id)
        return result
    except DeviceRecordingError as error:
        coordinator._retain_active_failure(error, plan, from_stop=True)
        raise ProviderError(error.code, error.message, status=HTTPStatus.CONFLICT) from error
    finally:
        with coordinator._stop_condition:
            if coordinator._stop_inflight == scope:
                coordinator._stop_inflight = None
                coordinator._pending_source_failure = None
            if (
                coordinator._active is None
                and coordinator._pending_stop_state_commit is None
                and coordinator._async_stop_worker is None
            ):
                coordinator._release_network_operation_lease()
            coordinator._sync_phase()
            coordinator._stop_condition.notify_all()


def publish_safe_swap(coordinator: CaptureCoordinator) -> None:
    with coordinator._lock:
        pending = coordinator._pending_safe_swap
        if pending is None:
            raise ProviderError("safe_swap_not_pending", "没有待发布的安全换盘", status=409)
        if coordinator.open_handle_count != 0:
            raise ProviderError(
                "safe_swap_blocked",
                "仍有核心录制或读取句柄未释放",
                status=HTTPStatus.CONFLICT,
                retryable=True,
                details={"open_handle_count": coordinator.open_handle_count},
            )
    coordinator._check_pending_safe_swap(pending)
    with coordinator._lock:
        if coordinator._pending_safe_swap is not pending:
            raise ProviderError("capture_state_changed", "待交接录制已变化", status=409)
        if coordinator.open_handle_count != 0:
            raise ProviderError(
                "safe_swap_blocked",
                "仍有核心录制或读取句柄未释放",
                status=HTTPStatus.CONFLICT,
                retryable=True,
                details={"open_handle_count": coordinator.open_handle_count},
            )
        receipt = {
            "schema": "ylx.safe-swap-receipt.v3",
            **pending,
            "released_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "release_state": "device-released",
            "open_handle_count": 0,
        }
        validate_safe_swap_v3_receipt(receipt)
        resource = {"schema": "ylx.safe-swap-receipt-resource.v3", "receipt": receipt}
        coordinator._safe_swap_to_persist = resource
        coordinator._next_revision()
    try:
        coordinator._persist_local_state()
    except BaseException as error:
        with coordinator._lock:
            coordinator._safe_swap_to_persist = None
        if not isinstance(error, (OSError, sqlite3.Error, PersistenceError)):
            raise
        raise ProviderError(
            "safe_swap_publish_failed",
            "安全换盘回执未能持久发布",
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
            retryable=True,
        ) from error
    with coordinator._lock:
        coordinator._released = True
        coordinator._safe_swap_resource = resource
        coordinator._pending_safe_swap = None
        coordinator._pending_safe_swap_failure = None
        coordinator._safe_swap_to_persist = None
        coordinator._next_revision()
        coordinator._sync_phase()


def check_pending_safe_swap(
    coordinator: CaptureCoordinator,
    pending: Mapping[str, object],
) -> None:
    session_id = str(pending["session_id"])
    with (
        closing(coordinator._open_store()) as store,
        store.open_manifest(session_id, "v4") as representation,
    ):
        payload = representation.read()
    manifest = json.loads(payload)
    if hashlib.sha256(payload).hexdigest() != pending["manifest_sha256"] or any(
        manifest[key] != pending[key] for key in ("manifest_id", "volume_id", "sealed_at")
    ):
        raise ProviderError(
            "safe_swap_unverified", "待交接录像已变化，请重试", status=409, retryable=True
        )
