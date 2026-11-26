"""Explicit capture lifecycle state machine (CapturePhase) and orchestration."""

from __future__ import annotations

import copy
import threading
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import replace
from datetime import UTC, datetime
from enum import StrEnum
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING

from rp_ylx.api.gateway import ProviderError
from rp_ylx.recording.device_session import (
    DeviceRecordingError,
    DeviceSessionRecorder,
    SealedDeviceSession,
    SessionPlan,
)

if TYPE_CHECKING:
    from rp_ylx.recording.hardware_proxies import CaptureLeaseManager, CaptureSources


class CapturePhase(StrEnum):
    """Authoritative capture lifecycle state replacing ad-hoc boolean/sentinel flags."""

    IDLE = "idle"
    STARTING = "starting"
    DELETING = "deleting"
    RECORDING = "recording"
    STOPPING = "stopping"
    RECOVERING_FAILURE = "recovering_failure"
    COMMITTING_STATE = "committing_state"
    SAFE_SWAP_PENDING = "safe_swap_pending"
    RELEASED = "released"
    CLOSING = "closing"


class CaptureLifecycle:
    """Owns the lifecycle RLock, Condition, CapturePhase, and active recorder state."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._stop_condition = threading.Condition(self._lock)
        self._close_lock = threading.Lock()

        self._phase = CapturePhase.IDLE
        self._closing = False
        self._starting = False
        self._deleting = False
        self._released = False
        self._settings_updating = False
        self._focus_inflight: tuple[tuple[str, str, str], bytes] | None = None

        self._active: DeviceSessionRecorder | None = None
        self._active_plan: SessionPlan | None = None
        self._capture_timer: tuple[SessionPlan, float, int] | None = None

        self._stop_inflight: tuple[str, str, str] | None = None
        self._pending_stop_state_commit: tuple[str, str, str] | None = None
        self._stop_acceptance_inflight: tuple[str, str, str] | None = None
        self._async_stop_worker: threading.Thread | None = None
        self._async_stop_reason: str | None = None

        self._failure_inflight: DeviceSessionRecorder | None = None
        self._failure_projection: dict[str, object] | None = None
        self._failure_pending_commit: DeviceSessionRecorder | None = None
        self._failure_retry: (
            tuple[DeviceSessionRecorder, SessionPlan, DeviceRecordingError] | None
        ) = None
        self._cached_active_state: Mapping[str, object] | None = None
        self._pending_source_failure: DeviceRecordingError | None = None

        self._open_representations = 0
        self._recovery_deferred = False
        self._recovery_retry_at = 0.0
        self._focus_revision = 0
        self._network_ipc_inflight = 0
        self._clock_sync_inflight = 0

    def sync_phase(self, *, pending_safe_swap: Mapping[str, object] | None = None) -> CapturePhase:
        """Compute and update `self._phase` from current lifecycle state (caller holds `_lock`)."""
        if self._closing:
            phase = CapturePhase.CLOSING
        elif self._starting:
            phase = CapturePhase.STARTING
        elif self._deleting:
            phase = CapturePhase.DELETING
        elif (
            self._failure_inflight is not None
            or self._failure_pending_commit is not None
            or self._failure_projection is not None
            or self._failure_retry is not None
        ):
            phase = CapturePhase.RECOVERING_FAILURE
        elif (
            self._stop_inflight is not None
            or self._stop_acceptance_inflight is not None
            or self._async_stop_worker is not None
        ):
            phase = CapturePhase.STOPPING
        elif self._pending_stop_state_commit is not None:
            phase = CapturePhase.COMMITTING_STATE
        elif self._active is not None:
            phase = CapturePhase.RECORDING
        elif pending_safe_swap is not None:
            phase = CapturePhase.SAFE_SWAP_PENDING
        elif self._released:
            phase = CapturePhase.RELEASED
        else:
            phase = CapturePhase.IDLE
        self._phase = phase
        return phase

    def is_idle(self, *, pending_safe_swap: Mapping[str, object] | None = None) -> bool:
        """Return True iff the coordinator is completely idle in `CapturePhase.IDLE`."""
        with self._lock:
            return (
                self.sync_phase(pending_safe_swap=pending_safe_swap) is CapturePhase.IDLE
                and not self._deleting
                and not self._settings_updating
            )

    def require_idle_for_settings(
        self,
        message: str,
        *,
        pending_safe_swap: Mapping[str, object] | None = None,
    ) -> None:
        """Enforce the unified `CapturePhase.IDLE` gate for voice/timer mutations."""
        if (
            self.sync_phase(pending_safe_swap=pending_safe_swap) is not CapturePhase.IDLE
            or self._deleting
            or self._settings_updating
        ):
            raise ProviderError("capture_busy", message, status=HTTPStatus.CONFLICT)

    def is_recovery_pending(self) -> bool:
        with self._lock:
            return (
                self._failure_retry is not None
                or self._failure_inflight is not None
                or self._failure_pending_commit is not None
                or self._pending_stop_state_commit is not None
            )

    def is_capture_busy(self) -> bool:
        with self._lock:
            return (
                self._starting
                or self._deleting
                or self._active is not None
                or self._stop_inflight is not None
                or self._stop_acceptance_inflight is not None
                or self._async_stop_worker is not None
            )

    def wait_while_starting_locked(self) -> None:
        while self._starting or self._settings_updating:
            self._stop_condition.wait()

    @staticmethod
    def local_failure_state(
        plan: SessionPlan,
        error: DeviceRecordingError,
        *,
        authority_epoch: str,
        next_revision: Callable[[], int],
        device_id: str,
        device_label: str,
        media_lost: bool,
    ) -> Mapping[str, object]:
        now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        return {
            "schema": "ylx.recording-state.v1",
            "state": "recoverable" if media_lost else "failed",
            "authority_epoch": authority_epoch,
            "state_revision": next_revision(),
            "updated_at": now,
            "session_id": plan.session_id,
            "take_id": plan.take_id,
            "display_name": plan.display_name,
            "device": {
                "device_id": device_id,
                "device_label": device_label,
            },
            "storage": {
                "volume_id": plan.volume_id,
                "status": "media_lost" if media_lost else "mounted",
                "writable": False,
                "remaining_bytes": None,
            },
            "progress": {
                "elapsed_seconds": 0.0,
                "captured_frames": 0,
                "bytes_written": 0,
            },
            "diagnostics": [
                {
                    "code": error.code,
                    "severity": "error",
                    "message": error.message,
                    "at": now,
                    "recoverable": media_lost,
                }
            ],
        }

    def close_capture(
        self,
        *,
        sources: CaptureSources | None,
        clear_preview: Callable[[], None],
        cancel_verification: Callable[[], None],
        is_verifying: Callable[[], bool],
        publish_recovered: Callable[[SealedDeviceSession], Path],
        record_saved_prefix: Callable[[str, Mapping[str, object]], None],
        record_verified: Callable[[str, str], None],
        retain_outcome: Callable[[str, Mapping[str, object]], None],
        persist_state: Callable[[], None],
        leases: CaptureLeaseManager,
        pending_safe_swap: Callable[[], Mapping[str, object] | None],
    ) -> None:
        """Drain active/background workers, abort active capture, publish prefix, and persist."""
        with self._stop_condition:
            self._closing = True
            self.sync_phase(pending_safe_swap=pending_safe_swap())
            cancel_verification()
            while (
                self._starting
                or self._deleting
                or self._failure_inflight is not None
                or self._stop_inflight is not None
                or self._async_stop_worker is not None
                or self._stop_acceptance_inflight is not None
                or self._network_ipc_inflight > 0
                or self._clock_sync_inflight > 0
                or is_verifying()
            ):
                self._stop_condition.wait()
            active = self._active
            active_plan = self._active_plan
            if active is not None:
                self._stop_inflight = ("internal", "close", "shutdown")
                self.sync_phase(pending_safe_swap=pending_safe_swap())
        if sources is not None:
            close_sources = getattr(sources, "close", None)
            if callable(close_sources):
                with suppress(Exception):
                    close_sources()
            else:
                with suppress(Exception):
                    sources.stop()
        clear_preview()
        try:
            if active is not None:
                active.abort()
                recovered = active.recovered_session
                if recovered is not None:
                    published = publish_recovered(recovered)
                    active.recovered_session = replace(recovered, path=published)
                    recovered = active.recovered_session
                    if active_plan is not None:
                        record_verified(active_plan.session_id, recovered.manifest_sha256)
                state = active.current_recording_state
                with self._stop_condition:
                    if state is not None and active_plan is not None:
                        retain_outcome(
                            active_plan.session_id,
                            {
                                "generation_id": active_plan.generation_id,
                                "recording_state": copy.deepcopy(state),
                            },
                        )
                    if recovered is not None and active_plan is not None:
                        record_saved_prefix(active_plan.session_id, recovered.manifest)
                    self._clear_active_state_locked(pending_safe_swap=pending_safe_swap())
            else:
                with self._stop_condition:
                    self._clear_active_state_locked(pending_safe_swap=pending_safe_swap())
            persist_state()
        finally:
            with self._stop_condition:
                self._stop_inflight = None
                leases.release()
                self.sync_phase(pending_safe_swap=pending_safe_swap())
                self._stop_condition.notify_all()

    def _clear_active_state_locked(
        self,
        *,
        pending_safe_swap: Mapping[str, object] | None,
    ) -> None:
        self._active = None
        self._active_plan = None
        self._capture_timer = None
        self._failure_pending_commit = None
        self._failure_projection = None
        self._failure_retry = None
        self.sync_phase(pending_safe_swap=pending_safe_swap)
