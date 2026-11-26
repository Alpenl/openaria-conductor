"""Active capture failure retention, recovery orchestration, and quiescent reconciliation."""

from __future__ import annotations

import copy
import threading
import time
from contextlib import suppress
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from rp_ylx.operational_logging import operational_logger
from rp_ylx.recording.device_session import DeviceRecordingError, DeviceSessionRecorder, SessionPlan

if TYPE_CHECKING:
    from rp_ylx.recording.coordinator import CaptureCoordinator


def reconcile_capture_recovery(coordinator: CaptureCoordinator) -> None:
    """Retry only quiescent owners; never detach a live writer or replay a start."""
    with coordinator._stop_condition:
        if (
            coordinator._closing
            or coordinator._lifecycle._starting
            or coordinator._failure_inflight is not None
            or coordinator._stop_inflight is not None
            or coordinator._async_stop_worker is not None
            or coordinator._stop_acceptance_inflight is not None
            or time.monotonic() < coordinator._recovery_retry_at
        ):
            return
        coordinator._recovery_retry_at = time.monotonic() + 2.0
        retry = coordinator._failure_retry
        pending = coordinator._pending_stop_state_commit
        if retry is not None:
            recorder, plan, error = retry
            if coordinator._active is not recorder or coordinator._active_plan is not plan:
                coordinator._failure_retry = None
                coordinator._sync_phase()
                return
            coordinator._failure_inflight = recorder
            coordinator._sync_phase()
        elif coordinator._active is None and pending is not None:
            coordinator._stop_inflight = pending
            coordinator._sync_phase()
        else:
            return
    try:
        if retry is not None:
            if coordinator._failure_pending_commit is recorder:
                coordinator._commit_active_failure(recorder)
            else:
                coordinator._finish_active_failure(recorder, plan, error)
        else:
            coordinator._persist_local_state()
            with coordinator._lock:
                if coordinator._pending_stop_state_commit == pending:
                    coordinator._pending_stop_state_commit = None
                if coordinator._active is None and coordinator._pending_stop_state_commit is None:
                    coordinator._release_network_operation_lease()
                coordinator._sync_phase()
    finally:
        with coordinator._stop_condition:
            if retry is not None and coordinator._failure_inflight is recorder:
                coordinator._failure_inflight = None
                if coordinator._active is not recorder:
                    coordinator._failure_projection = None
                    coordinator._failure_retry = None
            elif retry is None and coordinator._stop_inflight == pending:
                coordinator._stop_inflight = None
            coordinator._sync_phase()
            coordinator._stop_condition.notify_all()


def source_failure(
    coordinator: CaptureCoordinator,
    generation_id: str,
    code: str,
    message: str,
    *,
    expected_plan: SessionPlan | None = None,
    defer_recovery: bool = False,
) -> None:
    with coordinator._lock:
        if (
            coordinator._active is None
            or coordinator._active_plan is None
            or coordinator._active_plan.generation_id != generation_id
            or (expected_plan is not None and coordinator._active_plan is not expected_plan)
        ):
            return
        plan = coordinator._active_plan
    coordinator._retain_active_failure(
        DeviceRecordingError(code, message), plan, defer_recovery=defer_recovery
    )


def retain_active_failure(
    coordinator: CaptureCoordinator,
    error: DeviceRecordingError,
    plan: SessionPlan | None,
    *,
    from_stop: bool = False,
    defer_recovery: bool = False,
) -> None:
    voice_session: str | None = None
    with coordinator._stop_condition:
        recorder = coordinator._active
        if recorder is None or plan is None or coordinator._active_plan is not plan:
            return
        if (
            coordinator._failure_inflight is not None
            or coordinator._failure_pending_commit is recorder
        ):
            return
        if coordinator._stop_inflight is not None and not from_stop:
            coordinator._pending_source_failure = coordinator._pending_source_failure or error
            return
        voice_session = plan.session_id
        error = coordinator._pending_source_failure or error
        coordinator._pending_source_failure = None
        projection = copy.deepcopy(coordinator._cached_active_state)
        now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        if projection is None:
            projection = dict(coordinator._local_failure_state(plan, error, media_lost=False))
            projection["diagnostics"][0]["recoverable"] = error.code == "media_lost"
        else:
            projection.setdefault("diagnostics", []).append(
                {
                    "code": error.code,
                    "severity": "error",
                    "message": error.message,
                    "at": now,
                    "recoverable": error.code == "media_lost",
                }
            )
        projection["state"] = "finalizing"
        projection["state_revision"] = coordinator._next_revision()
        projection["updated_at"] = now
        coordinator._failure_projection = projection
        coordinator._failure_inflight = recorder
        coordinator._failure_retry = (recorder, plan, error)
        coordinator._sync_phase()
    if voice_session is not None:
        coordinator._notify_voice("save_failed" if from_stop else "recording_failed", voice_session)

    def finish() -> None:
        try:
            coordinator._finish_active_failure(recorder, plan, error)
        finally:
            with coordinator._stop_condition:
                coordinator._failure_inflight = None
                if coordinator._active is not recorder:
                    coordinator._failure_projection = None
                    coordinator._failure_retry = None
                coordinator._sync_phase()
                coordinator._stop_condition.notify_all()

    if defer_recovery:

        def background_finish() -> None:
            try:
                finish()
            except Exception as failure:
                operational_logger("recording").event(
                    "recovery_failed",
                    level="error",
                    transaction_id=plan.session_id,
                    exception_type=type(failure).__name__,
                )

        try:
            worker = threading.Thread(
                target=background_finish, name="rp-ylx-failure-recovery", daemon=False
            )
            worker.start()
        except (RuntimeError, MemoryError):
            finish()
        return
    finish()


def finish_active_failure(
    coordinator: CaptureCoordinator,
    recorder: DeviceSessionRecorder,
    plan: SessionPlan,
    error: DeviceRecordingError,
) -> None:
    if coordinator._sources is not None:
        with suppress(Exception):
            coordinator._sources.stop()
    if not coordinator._sources_keep_preview():
        coordinator._preview.clear()
    media_lost = error.code == "media_lost"
    recorder.fail(
        error.code,
        error.message,
        recoverable=media_lost,
        media_lost=media_lost,
    )
    state = recorder.current_recording_state
    recovered = recorder.recovered_session
    if (
        not media_lost
        and recovered is None
        and not recorder.recovery_attempted
        and recorder.partial_path.is_dir()
    ):
        recovered = recorder.recover_completed_segments()
    if not media_lost and recovered is not None:
        published = coordinator._publish_recording_directory(recovered.path, recovered.manifest)
        recorder.recovered_session = replace(recovered, path=published)
        with coordinator._catalog_lock:
            coordinator._verified[plan.session_id] = recovered.manifest_sha256
    with coordinator._lock:
        if state is None or state.get("state") not in {"recoverable", "failed", "abandoned"}:
            state = coordinator._local_failure_state(plan, error, media_lost=media_lost)
        coordinator._retained[plan.session_id] = {
            "generation_id": plan.generation_id,
            "recording_state": copy.deepcopy(state),
        }
        if not media_lost and recovered is not None:
            coordinator._record_saved_prefix(plan.session_id, recovered.manifest)
        coordinator._failure_pending_commit = recorder
        coordinator._sync_phase()
    coordinator._commit_active_failure(recorder)


def commit_active_failure(
    coordinator: CaptureCoordinator,
    recorder: DeviceSessionRecorder,
) -> None:
    coordinator._persist_local_state()
    with coordinator._lock:
        assert coordinator._active is recorder and coordinator._failure_pending_commit is recorder
        coordinator._failure_pending_commit = None
        coordinator._failure_retry = None
        coordinator._failure_projection = None
        coordinator._active = None
        coordinator._active_plan = None
        coordinator._capture_timer = None
        coordinator._cached_active_state = None
        coordinator._latest_imu = None
        if coordinator._stop_inflight is None:
            coordinator._release_network_operation_lease()
        coordinator._sync_phase()
