"""生产录制的卷准入、协调、恢复和 DeviceProvider 投影。"""

from __future__ import annotations

import copy
import os  # noqa: F401
import sqlite3
import threading
import time
from collections.abc import Callable, Mapping
from contextlib import ExitStack, closing, suppress
from dataclasses import replace
from datetime import UTC, datetime
from http import HTTPStatus
from pathlib import Path

import rp_ylx.recording.failure_recovery as failure_recovery
import rp_ylx.recording.recovery_orchestrator as recovery_orchestrator
import rp_ylx.recording.stop_workflow as stop_workflow
from rp_ylx.api.downloads import ArtifactAccessError
from rp_ylx.api.downloads import DirectorySessionStore as DirectorySessionStore
from rp_ylx.api.events import (
    project_device_descriptor,
    validate_capture_status,
    validate_session_summary,
)
from rp_ylx.api.gateway import (
    CaptureCommand,
    CaptureCommandResult,
    NetworkCommand,
    NetworkCommandResult,
    NetworkCredentialCommand,
    ProviderError,
)
from rp_ylx.api.preview import LatestPreviewBuffer, PreviewFrameUnavailable, PreviewResponse
from rp_ylx.camera import CameraError, FrameObservation
from rp_ylx.capture_readiness import system_capacity
from rp_ylx.capture_timer import valid_timer_settings
from rp_ylx.clock_sync import valid_clock_response
from rp_ylx.errors import ProviderError as CoreProviderError
from rp_ylx.imu import ImuObservation
from rp_ylx.network import network_operation_lock_path
from rp_ylx.network import network_status as collect_network_status
from rp_ylx.network_control import request_control as request_network_control
from rp_ylx.operational_logging import operational_logger
from rp_ylx.performance.metrics import PerformanceMetrics
from rp_ylx.persistence import PersistenceError
from rp_ylx.recording.backing_storage import backing_capacity
from rp_ylx.recording.deletion import delete_recordings, valid_delete_request
from rp_ylx.recording.device_session import (
    DeviceRecordingError,
    DeviceSessionRecorder,
    SealedDeviceSession,
    SessionPlan,
    StorageStatus,
    inspect_device_session_directory,
    uuid7,
    validate_device_session_directory,
)
from rp_ylx.recording.hardware_proxies import (
    LIVE_IMU_STALE_SECONDS,
    CameraFocusProxy,
    CaptureLeaseManager,
    CaptureSources,
    ClockSyncProxy,
    LiveImuTracker,
    NetworkControllerProxy,
)
from rp_ylx.recording.lifecycle import CaptureLifecycle, CapturePhase
from rp_ylx.recording.recovery import RECOVERY_ROLE, recover_device_session  # noqa: F401
from rp_ylx.recording.recovery_orchestrator import read_recovery_metadata
from rp_ylx.recording.session_catalog import (
    SESSION_CATALOG_REVISION_DOMAIN,
    CatalogService,
    _MultiRootSessionStore,
    _TrackedRepresentation,
    _VerifiedSessionSnapshot,
    canonical_json_bytes,
    encode_session_cursor,
    session_catalog_revision,
)
from rp_ylx.recording.state_store import SAFE_SWAP_FAILURES, CoordinatorStateStore
from rp_ylx.recording.volume import (
    LEGACY_SESSIONS_DIRECTORY,
    SESSIONS_DIRECTORY,
    VOLUME_MARKER,
    CoordinatorConfig,
    VolumeAdmission,
    VolumeManager,
    initialize_capture_volume,
    stat_capacity,
)
from rp_ylx.runtime import collect_linux_runtime
from rp_ylx.voice_feedback import notify as notify_voice
from rp_ylx.voice_feedback import valid_volume

CALIBRATION_CAPTURE_DISABLED_REASONS = {
    "capture_source_unsupported",
    "storage_unavailable",
    "hardware_unavailable",
    "maintenance_or_capture_busy",
}

# Preserve compatibility aliases for existing tests and imports.
_stat_capacity = stat_capacity
_canonical_json_bytes = canonical_json_bytes
_encode_session_cursor = encode_session_cursor
_session_catalog_revision = session_catalog_revision
_read_recovery_metadata = read_recovery_metadata


class CaptureCoordinator:
    """单卷单活动会话的生产 DeviceProvider。"""

    def __init__(
        self,
        config: CoordinatorConfig,
        *,
        mount_checker: Callable[[Path], bool] | None = None,
        mount_identity: Callable[[Path], str] | None = None,
        runtime: Callable[[], Mapping[str, object]] | None = None,
        preview: LatestPreviewBuffer | None = None,
        sources: CaptureSources | None = None,
        metrics: PerformanceMetrics | None = None,
        before_write: Callable[[str, bytes], None] | None = None,
    ) -> None:
        self._config = config
        self._runtime = runtime or collect_linux_runtime
        self._preview = preview or LatestPreviewBuffer(stream_fps=25, thumbnails=True)
        self._sources = sources
        self._metrics = metrics
        self._before_write = before_write
        self._admission_lock = threading.Lock()

        self._lifecycle = CaptureLifecycle()
        self._volume = VolumeManager(
            config,
            mount_checker=mount_checker,
            mount_identity=mount_identity,
            stat_capacity_fn=lambda path: _stat_capacity(path),
            system_capacity_fn=lambda path: system_capacity(path),
            backing_capacity_fn=lambda path: backing_capacity(path),
            monotonic_fn=lambda: time.monotonic(),
        )
        self._state = CoordinatorStateStore(
            config.state_root,
            notify_fn=lambda *args, **kwargs: notify_voice(*args, **kwargs),
        )
        self._catalog = CatalogService(
            config.state_root,
            commit=config.session.commit,
            inspect_fn=lambda *args, **kwargs: inspect_device_session_directory(*args, **kwargs),
            validate_fn=lambda *args, **kwargs: validate_device_session_directory(*args, **kwargs),
            validate_summary_fn=lambda value: validate_session_summary(value),
            store_cls=_MultiRootSessionStore,
        )
        self._leases = CaptureLeaseManager(self._network_operation_lock_path)
        self._network_proxy = NetworkControllerProxy(
            request_control_fn=lambda *args, **kwargs: request_network_control(*args, **kwargs),
            collect_network_status_fn=lambda: collect_network_status(),
        )
        self._clock_proxy = ClockSyncProxy(
            request_control_fn=lambda *args, **kwargs: request_network_control(*args, **kwargs)
        )
        self._camera_proxy = CameraFocusProxy()
        self._imu_tracker = LiveImuTracker(monotonic_fn=lambda: time.monotonic())

        persisted = self._state._initial_persisted
        try:
            self._volume._admission = self._admit(persisted)
        except DeviceRecordingError:
            self._volume._admission = None
        self._restore_local_state(persisted)
        if self._volume._admission is not None:
            self._recover_sessions()
        self._lifecycle.sync_phase(pending_safe_swap=self._state._pending_safe_swap)
        self._persist_local_state()
        if self._sources is not None:
            with suppress(CameraError, ProviderError):
                self._require_camera_connected()

    # -- Compatibility properties preserving test-inspectable attributes --

    @property
    def phase(self) -> CapturePhase:
        return self._lifecycle._phase

    @property
    def _lock(self) -> threading.RLock:
        override = self.__dict__.get("_lock_override")
        if override is not None:
            return override
        return self._lifecycle._lock

    @_lock.setter
    def _lock(self, value: threading.RLock) -> None:
        self.__dict__["_lock_override"] = value

    @_lock.deleter
    def _lock(self) -> None:
        self.__dict__.pop("_lock_override", None)

    def _get_lock(self) -> threading.RLock:
        override = self.__dict__.get("_lock_override")
        if override is not None:
            return override
        if "_lifecycle" in self.__dict__:
            return self._lifecycle._lock
        lock = self.__dict__.get("_lock")
        if lock is None:
            lock = threading.RLock()
            self.__dict__["_lock_override"] = lock
        return lock

    @property
    def _stop_condition(self) -> threading.Condition:
        return self._lifecycle._stop_condition

    @property
    def _close_lock(self) -> threading.Lock:
        return self._lifecycle._close_lock

    @property
    def _state_persist_lock(self) -> threading.Lock:
        override = self.__dict__.get("_state_persist_lock_override")
        return override if override is not None else self._state._state_persist_lock

    @_state_persist_lock.setter
    def _state_persist_lock(self, value: threading.Lock) -> None:
        self.__dict__["_state_persist_lock_override"] = value

    @_state_persist_lock.deleter
    def _state_persist_lock(self) -> None:
        self.__dict__.pop("_state_persist_lock_override", None)

    @property
    def _state_db(self):
        return self._state._state_db

    @property
    def _storage_lock(self) -> threading.Lock:
        return self._volume._storage_lock

    @property
    def _storage_checked_at(self) -> float:
        return self._volume._storage_checked_at

    @_storage_checked_at.setter
    def _storage_checked_at(self, value: float) -> None:
        self._volume._storage_checked_at = value

    @property
    def _storage_cache(self) -> StorageStatus | None:
        return self._volume._storage_cache

    @_storage_cache.setter
    def _storage_cache(self, value: StorageStatus | None) -> None:
        self._volume._storage_cache = value

    @property
    def _admission(self) -> VolumeAdmission | None:
        return self._volume._admission

    @_admission.setter
    def _admission(self, value: VolumeAdmission | None) -> None:
        self._volume._admission = value

    @property
    def _media_lost(self) -> bool:
        return self._volume._media_lost

    @_media_lost.setter
    def _media_lost(self, value: bool) -> None:
        self._volume._media_lost = value

    @property
    def _catalog_lock(self):
        return self._catalog._catalog_lock

    @_catalog_lock.setter
    def _catalog_lock(self, value) -> None:
        self._catalog._catalog_lock = value

    @property
    def _catalog_db(self):
        return self._catalog._catalog_db

    @property
    def _catalog_verification_lock(self) -> threading.Lock:
        if "_catalog" in self.__dict__:
            return self._catalog._catalog_verification_lock
        return self.__dict__["_catalog_verification_lock_override"]

    @_catalog_verification_lock.setter
    def _catalog_verification_lock(self, value: threading.Lock) -> None:
        if "_catalog" in self.__dict__:
            self._catalog._catalog_verification_lock = value
        else:
            self.__dict__["_catalog_verification_lock_override"] = value

    @_catalog_verification_lock.deleter
    def _catalog_verification_lock(self) -> None:
        self.__dict__.pop("_catalog_verification_lock_override", None)

    @property
    def _catalog_verification_cancel(self) -> threading.Event:
        if "_catalog" in self.__dict__:
            return self._catalog._catalog_verification_cancel
        return self.__dict__["_compat_catalog_verification_cancel"]

    @_catalog_verification_cancel.setter
    def _catalog_verification_cancel(self, value: threading.Event) -> None:
        if "_catalog" in self.__dict__:
            self._catalog._catalog_verification_cancel = value
        else:
            self.__dict__["_compat_catalog_verification_cancel"] = value

    @property
    def _catalog_verification_epoch(self) -> int:
        if "_catalog" in self.__dict__:
            return self._catalog._catalog_verification_epoch
        return self.__dict__.get("_compat_catalog_verification_epoch", 0)

    @_catalog_verification_epoch.setter
    def _catalog_verification_epoch(self, value: int) -> None:
        if "_catalog" in self.__dict__:
            self._catalog._catalog_verification_epoch = value
        else:
            self.__dict__["_compat_catalog_verification_epoch"] = value

    @property
    def _catalog_verification_session(self) -> str | None:
        return self._catalog._catalog_verification_session

    @_catalog_verification_session.setter
    def _catalog_verification_session(self, value: str | None) -> None:
        self._catalog._catalog_verification_session = value

    @property
    def _recording_paths(self) -> dict[Path, dict[str, Path]]:
        return self._catalog._recording_paths

    @_recording_paths.setter
    def _recording_paths(self, value: dict[Path, dict[str, Path]]) -> None:
        self._catalog._recording_paths = value

    @property
    def _verified(self) -> dict[str, str]:
        return self._catalog._verified

    @_verified.setter
    def _verified(self, value: dict[str, str]) -> None:
        self._catalog._verified = value

    @property
    def _session_snapshots(self) -> dict[str, _VerifiedSessionSnapshot]:
        return self._catalog._session_snapshots

    @_session_snapshots.setter
    def _session_snapshots(self, value: dict[str, _VerifiedSessionSnapshot]) -> None:
        self._catalog._session_snapshots = value

    @property
    def _session_summaries(self) -> dict[str, dict[str, object]]:
        return self._catalog._session_summaries

    @_session_summaries.setter
    def _session_summaries(self, value: dict[str, dict[str, object]]) -> None:
        self._catalog._session_summaries = value

    @property
    def _session_diagnostics(self) -> dict[str, dict[str, object]]:
        return self._catalog._session_diagnostics

    @_session_diagnostics.setter
    def _session_diagnostics(self, value: dict[str, dict[str, object]]) -> None:
        self._catalog._session_diagnostics = value

    @property
    def _active(self) -> DeviceSessionRecorder | None:
        if "_lifecycle" in self.__dict__:
            return self._lifecycle._active
        return self.__dict__.get("_compat_active")

    @_active.setter
    def _active(self, value: DeviceSessionRecorder | None) -> None:
        if "_lifecycle" in self.__dict__:
            self._lifecycle._active = value
        else:
            self.__dict__["_compat_active"] = value

    @property
    def _active_plan(self) -> SessionPlan | None:
        return self._lifecycle._active_plan

    @_active_plan.setter
    def _active_plan(self, value: SessionPlan | None) -> None:
        self._lifecycle._active_plan = value

    @property
    def _capture_timer(self) -> tuple[SessionPlan, float, int] | None:
        return self._lifecycle._capture_timer

    @_capture_timer.setter
    def _capture_timer(self, value: tuple[SessionPlan, float, int] | None) -> None:
        self._lifecycle._capture_timer = value

    @property
    def _closing(self) -> bool:
        return self._lifecycle._closing

    @_closing.setter
    def _closing(self, value: bool) -> None:
        self._lifecycle._closing = value

    @property
    def _released(self) -> bool:
        return self._lifecycle._released

    @_released.setter
    def _released(self, value: bool) -> None:
        self._lifecycle._released = value

    @_released.deleter
    def _released(self) -> None:
        self._lifecycle._released = False

    @property
    def _open_representations(self) -> int:
        return self._lifecycle._open_representations

    @_open_representations.setter
    def _open_representations(self, value: int) -> None:
        self._lifecycle._open_representations = value

    @property
    def _stop_inflight(self):
        return self._lifecycle._stop_inflight

    @_stop_inflight.setter
    def _stop_inflight(self, value) -> None:
        self._lifecycle._stop_inflight = value

    @property
    def _pending_stop_state_commit(self):
        return self._lifecycle._pending_stop_state_commit

    @_pending_stop_state_commit.setter
    def _pending_stop_state_commit(self, value) -> None:
        self._lifecycle._pending_stop_state_commit = value

    @property
    def _stop_acceptance_inflight(self):
        return self._lifecycle._stop_acceptance_inflight

    @_stop_acceptance_inflight.setter
    def _stop_acceptance_inflight(self, value) -> None:
        self._lifecycle._stop_acceptance_inflight = value

    @property
    def _async_stop_worker(self):
        return self._lifecycle._async_stop_worker

    @_async_stop_worker.setter
    def _async_stop_worker(self, value) -> None:
        self._lifecycle._async_stop_worker = value

    @property
    def _async_stop_reason(self):
        return self._lifecycle._async_stop_reason

    @_async_stop_reason.setter
    def _async_stop_reason(self, value) -> None:
        self._lifecycle._async_stop_reason = value

    @_async_stop_reason.deleter
    def _async_stop_reason(self) -> None:
        self._lifecycle._async_stop_reason = None

    @property
    def _failure_inflight(self):
        return self._lifecycle._failure_inflight

    @_failure_inflight.setter
    def _failure_inflight(self, value) -> None:
        self._lifecycle._failure_inflight = value

    @property
    def _failure_projection(self):
        return self._lifecycle._failure_projection

    @_failure_projection.setter
    def _failure_projection(self, value) -> None:
        self._lifecycle._failure_projection = value

    @property
    def _failure_pending_commit(self):
        return self._lifecycle._failure_pending_commit

    @_failure_pending_commit.setter
    def _failure_pending_commit(self, value) -> None:
        self._lifecycle._failure_pending_commit = value

    @property
    def _failure_retry(self):
        return self._lifecycle._failure_retry

    @_failure_retry.setter
    def _failure_retry(self, value) -> None:
        self._lifecycle._failure_retry = value

    @property
    def _cached_active_state(self):
        return self._lifecycle._cached_active_state

    @_cached_active_state.setter
    def _cached_active_state(self, value) -> None:
        self._lifecycle._cached_active_state = value

    @property
    def _pending_source_failure(self):
        return self._lifecycle._pending_source_failure

    @_pending_source_failure.setter
    def _pending_source_failure(self, value) -> None:
        self._lifecycle._pending_source_failure = value

    @property
    def _recovery_deferred(self) -> bool:
        return self._lifecycle._recovery_deferred

    @_recovery_deferred.setter
    def _recovery_deferred(self, value: bool) -> None:
        self._lifecycle._recovery_deferred = value

    @property
    def _recovery_retry_at(self) -> float:
        return self._lifecycle._recovery_retry_at

    @_recovery_retry_at.setter
    def _recovery_retry_at(self, value: float) -> None:
        self._lifecycle._recovery_retry_at = value

    @property
    def _focus_revision(self) -> int:
        return self._lifecycle._focus_revision

    @_focus_revision.setter
    def _focus_revision(self, value: int) -> None:
        self._lifecycle._focus_revision = value

    @property
    def _network_operation_lease(self) -> int | None:
        if "_leases" in self.__dict__:
            return self._leases.network_operation_lease
        return self.__dict__.get("_compat_network_operation_lease")

    @_network_operation_lease.setter
    def _network_operation_lease(self, value: int | None) -> None:
        if "_leases" in self.__dict__:
            self._leases.network_operation_lease = value
        else:
            self.__dict__["_compat_network_operation_lease"] = value

    @property
    def _maintenance_lease(self):
        if "_leases" in self.__dict__:
            return self._leases.maintenance_lease
        return self.__dict__.get("_compat_maintenance_lease")

    @_maintenance_lease.setter
    def _maintenance_lease(self, value) -> None:
        if "_leases" in self.__dict__:
            self._leases.maintenance_lease = value
        else:
            self.__dict__["_compat_maintenance_lease"] = value

    @property
    def _retained(self) -> dict[str, dict[str, object]]:
        return self._state._retained

    @_retained.setter
    def _retained(self, value: dict[str, dict[str, object]]) -> None:
        self._state._retained = value

    @property
    def _commands(self):
        return self._state._commands

    @_commands.setter
    def _commands(self, value) -> None:
        self._state._commands = value

    @property
    def _invalid_commands(self):
        return self._state._invalid_commands

    @_invalid_commands.setter
    def _invalid_commands(self, value) -> None:
        self._state._invalid_commands = value

    @property
    def _persisted_commands(self):
        return self._state._persisted_commands

    @_persisted_commands.setter
    def _persisted_commands(self, value) -> None:
        self._state._persisted_commands = value

    @property
    def _pending_safe_swap(self):
        return self._state._pending_safe_swap

    @_pending_safe_swap.setter
    def _pending_safe_swap(self, value) -> None:
        self._state._pending_safe_swap = value

    @_pending_safe_swap.deleter
    def _pending_safe_swap(self) -> None:
        self._state._pending_safe_swap = None

    @property
    def _pending_safe_swap_failure(self):
        return self._state._pending_safe_swap_failure

    @_pending_safe_swap_failure.setter
    def _pending_safe_swap_failure(self, value) -> None:
        self._state._pending_safe_swap_failure = value

    @property
    def _safe_swap_resource(self):
        return self._state._safe_swap_resource

    @_safe_swap_resource.setter
    def _safe_swap_resource(self, value) -> None:
        self._state._safe_swap_resource = value

    @property
    def _safe_swap_to_persist(self):
        return self._state._safe_swap_to_persist

    @_safe_swap_to_persist.setter
    def _safe_swap_to_persist(self, value) -> None:
        self._state._safe_swap_to_persist = value

    @property
    def _safe_swap_failure_clear_pending(self) -> bool:
        return self._state._safe_swap_failure_clear_pending

    @_safe_swap_failure_clear_pending.setter
    def _safe_swap_failure_clear_pending(self, value: bool) -> None:
        self._state._safe_swap_failure_clear_pending = value

    @property
    def _authority_epoch(self) -> str:
        return self._state._authority_epoch

    @property
    def _revision(self) -> int:
        return self._state._revision

    @_revision.setter
    def _revision(self, value: int) -> None:
        self._state._revision = value

    @property
    def _voice_settings(self):
        if "_state" in self.__dict__:
            return self._state._voice_settings
        return self.__dict__.get("_compat_voice_settings", {"volume": 100})

    @_voice_settings.setter
    def _voice_settings(self, value) -> None:
        if "_state" in self.__dict__:
            self._state._voice_settings = value
        else:
            self.__dict__["_compat_voice_settings"] = value

    @property
    def _timer_settings(self):
        if "_state" in self.__dict__:
            return self._state._timer_settings
        return self.__dict__.get(
            "_compat_timer_settings", {"enabled": False, "duration_seconds": 600}
        )

    @_timer_settings.setter
    def _timer_settings(self, value) -> None:
        if "_state" in self.__dict__:
            self._state._timer_settings = value
        else:
            self.__dict__["_compat_timer_settings"] = value

    @property
    def _pending_focus(self):
        if "_state" in self.__dict__:
            return self._state._pending_focus
        return self.__dict__.get("_compat_pending_focus")

    @_pending_focus.setter
    def _pending_focus(self, value) -> None:
        if "_state" in self.__dict__:
            self._state._pending_focus = value
        else:
            self.__dict__["_compat_pending_focus"] = value

    @property
    def _focus_restore_invalid(self) -> bool:
        if "_state" in self.__dict__:
            return self._state._focus_restore_invalid
        return bool(self.__dict__.get("_compat_focus_restore_invalid", False))

    @_focus_restore_invalid.setter
    def _focus_restore_invalid(self, value: bool) -> None:
        if "_state" in self.__dict__:
            self._state._focus_restore_invalid = value
        else:
            self.__dict__["_compat_focus_restore_invalid"] = value

    @property
    def _latest_imu(self):
        return self._imu_tracker.latest_imu

    @_latest_imu.setter
    def _latest_imu(self, value) -> None:
        self._imu_tracker.latest_imu = value

    def _sync_phase(self) -> CapturePhase:
        return self._lifecycle.sync_phase(pending_safe_swap=self._pending_safe_swap)

    # -- Public volume/handle/source metadata --

    @property
    def volume_id(self) -> str:
        return self._require_admission().volume_id

    @property
    def generation_id(self) -> str:
        return self._require_admission().generation_id

    @property
    def open_handle_count(self) -> int:
        with self._lock:
            if self._failure_projection is not None:
                return 1 + self._open_representations
            recorder_handles = 0 if self._active is None else self._active.open_handle_count
            source_handles = 0 if self._sources is None else self._sources.open_handle_count
            return (
                recorder_handles
                + source_handles
                + self._open_representations
                + int(self._catalog_verification_session is not None)
            )

    def _sources_keep_preview(self) -> bool:
        return bool(getattr(self._sources, "keeps_preview_after_stop", False))

    # -- State persistence & volume admission delegation --

    def _read_local_state(self) -> Mapping[str, object]:
        return self._state.read_local_state()

    def _admit(self, persisted: Mapping[str, object]) -> VolumeAdmission:
        return self._volume.admit(persisted)

    def _require_admission(self) -> VolumeAdmission:
        admission = self._volume._admission
        if admission is not None:
            return admission
        with self._admission_lock:
            admission = self._volume._admission
            if admission is not None:
                return admission
            persisted = self._read_local_state()
            admission = self._admit(persisted)
            with self._lock:
                self._volume._admission = admission
                self._volume._media_lost = False
                self._restore_local_state(persisted)
                self._next_revision()
                self._sync_phase()
            self._recover_sessions()
            self._persist_local_state()
            return admission

    def _restore_local_state(self, persisted: Mapping[str, object]) -> None:
        def mark_released() -> None:
            self._released = True

        self._state.restore_local_state(
            persisted,
            self._volume._admission,
            on_released=mark_released,
            on_forget_deleted=self._forget_deleted_outcomes,
        )

    def _forget_deleted_outcomes(self, session_ids: list[str]) -> None:
        removed = self._retained.keys() & set(session_ids)
        if not removed:
            return
        visible = self._recording_snapshot()[3]
        for session_id in removed:
            del self._retained[session_id]
        if visible is not None and visible["recording_state"]["session_id"] in removed:
            self._next_revision()

    def _persist_local_state(self) -> None:
        def snapshot_builder():
            sequence, value, commands = self._local_state_write()
            return (
                sequence,
                value,
                commands,
                self._state._focus_to_persist,
                self._pending_stop_state_commit,
            )

        def on_committed(
            pending_stop: tuple[str, str, str] | None, value: dict[str, object]
        ) -> None:
            if (
                pending_stop is not None
                and self._pending_stop_state_commit == pending_stop
                and value["active_session"] is None
            ):
                self._pending_stop_state_commit = None
                if (
                    self._active is None
                    and self._stop_inflight is None
                    and self._async_stop_worker is None
                ):
                    self._release_network_operation_lease()
                self._sync_phase()
                self._stop_condition.notify_all()

        self._state.persist_local_state(
            self._lock,
            snapshot_builder,
            on_committed,
            lock_override=self._state_persist_lock,
        )

    def _local_state_write(self):
        return self._state.build_local_state_write(
            admission=self._volume._admission,
            active=self._active,
            active_plan=self._active_plan,
            failure_pending_commit=self._failure_pending_commit,
        )

    def _recording_state_changed(self, plan: SessionPlan, state: Mapping[str, object]) -> None:
        with self._lock:
            recovering = self._active_plan is plan and self._failure_inflight is not None
            if self._active_plan is plan:
                self._cached_active_state = copy.deepcopy(state)
        try:
            self._persist_local_state()
        except (OSError, sqlite3.Error, PersistenceError) as error:
            if not recovering:
                raise
            operational_logger("recording").event(
                "failure_checkpoint_commit_deferred",
                level="warning",
                transaction_id=plan.session_id,
                exception_type=type(error).__name__,
            )

    def _next_revision(self) -> int:
        with self._lock:
            self._state._revision += 1
            return self._state._revision

    def _probe_storage_capacity(self, admission: VolumeAdmission) -> tuple[StorageStatus, int]:
        return self._volume.probe_storage_capacity(admission)

    def _check_generation(
        self,
        generation_id: str | None = None,
        *,
        force: bool = False,
    ) -> StorageStatus:
        admission = self._require_admission()
        return self._volume.check_generation(
            admission,
            generation_id,
            released=self._released,
            force=force,
        )

    def _require_capture_reserve(
        self,
        admission: VolumeAdmission,
        capacity: tuple[int, int, int] | None = None,
    ) -> None:
        self._volume.require_capture_reserve(admission, capacity)

    def _require_system_reserve(self) -> None:
        self._volume.require_system_reserve()

    def capture_readiness(self) -> dict[str, object]:
        camera_connected = self._camera_connection_status()["state"] == "connected"
        recovery_pending = self._lifecycle.is_recovery_pending()
        capture_busy = self._lifecycle.is_capture_busy()
        with self._lock:
            closing_state = self._closing
        return self._volume.capture_readiness(
            require_admission=self._require_admission,
            check_generation=self._check_generation,
            recovery_pending=recovery_pending,
            capture_busy=capture_busy,
            closing=closing_state,
            camera_connected=camera_connected,
        )

    def reconcile_capture_recovery(self) -> None:
        """Retry only quiescent owners; never detach a live writer or replay a start."""
        failure_recovery.reconcile_capture_recovery(self)

    # -- Crash recovery --

    def _can_recover_sessions(self, admission: VolumeAdmission) -> bool:
        return recovery_orchestrator.can_recover_sessions(self, admission)

    def _recover_sessions(self) -> None:
        recovery_orchestrator.recover_sessions(self)

    @staticmethod
    def _partial_directory_current(partial: Path, descriptor: int) -> bool:
        return recovery_orchestrator.partial_directory_current(partial, descriptor)

    def _recover_partial_directory(self, partial: Path, session_id: str, descriptor: int) -> None:
        recovery_orchestrator.recover_partial_directory(self, partial, session_id, descriptor)

    def _restore_published_failure(self, admission: VolumeAdmission) -> str | None:
        return recovery_orchestrator.restore_published_failure(self, admission)

    def _publish_recording_directory(self, path: Path, manifest: Mapping[str, object]) -> Path:
        return self._catalog.publish_recording_directory(path, manifest)

    def _recover_completed_prefix(self, partial: Path, session_id: str) -> None:
        recovery_orchestrator.recover_completed_prefix(self, partial, session_id)

    def _record_saved_prefix(self, session_id: str, manifest: Mapping[str, object]) -> None:
        recovery_orchestrator.record_saved_prefix(self, session_id, manifest)

    def _settle_partial(self, partial: Path, session_id: str, *, descriptor: int) -> None:
        recovery_orchestrator.settle_partial(self, partial, session_id, descriptor=descriptor)

    # -- Catalog & verification compatibility bridges --

    def _verify_exact_session_payload(
        self,
        session_path: Path,
        session_id: str,
        manifest: Mapping[str, object],
        manifest_payload: bytes,
        *,
        interrupt_check: Callable[[], None] | None = None,
        verified_artifacts=None,
        progress: Callable[[int, int], None] | None = None,
    ) -> tuple[_VerifiedSessionSnapshot, DeviceRecordingError | None]:
        return self._catalog.verify_exact_session_payload(
            self._require_admission(),
            session_path,
            session_id,
            manifest,
            manifest_payload,
            interrupt_check=interrupt_check,
            verified_artifacts=verified_artifacts,
            progress=progress,
        )

    def _invalidate_session_verification(self, session_id: str) -> None:
        self._catalog.invalidate_session_verification(session_id)

    def _session_manifest_is_current(
        self,
        session_id: str,
        snapshot: _VerifiedSessionSnapshot,
    ) -> bool:
        return self._catalog.session_manifest_is_current(
            self._require_admission(), session_id, snapshot
        )

    def _is_verification_busy(self) -> bool:
        with self._lock:
            return (
                self._active is not None
                or self._lifecycle._starting
                or self._lifecycle._deleting
                or self._closing
            )

    def _is_verification_releasing(self, allow_pending_safe_swap: bool) -> bool:
        with self._lock:
            return self._released or (
                not allow_pending_safe_swap
                and (self._pending_safe_swap is not None or self._async_stop_reason == "safe_swap")
            )

    def _retained_session_ids(self) -> set[str]:
        with self._lock:
            return set(self._retained)

    def _check_catalog_verification_idle(self) -> None:
        self._catalog.check_catalog_verification_idle(is_busy=self._is_verification_busy)

    def _verify_catalog_session(
        self,
        session_id: str,
        *,
        cancelled: Callable[[], bool] | None = None,
        allow_pending_safe_swap: bool = False,
    ) -> None:
        self._catalog.verify_catalog_session(
            session_id,
            stop_condition=self._stop_condition,
            is_busy=self._is_verification_busy,
            is_releasing=self._is_verification_releasing,
            require_admission=self._require_admission,
            retained_ids=self._retained_session_ids,
            verify_payload_fn=self._verify_exact_session_payload,
            cancelled=cancelled,
            allow_pending_safe_swap=allow_pending_safe_swap,
        )

    def _session_payload_is_current(
        self, session_id: str, snapshot: _VerifiedSessionSnapshot
    ) -> bool:
        return self._catalog.session_payload_is_current(
            self._require_admission(), session_id, snapshot
        )

    def _catalog_sessions(
        self,
        *,
        verify_session: str | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> None:
        admission = self._require_admission()
        self._catalog.catalog_sessions(
            admission,
            retained_ids=self._retained_session_ids(),
            check_manifest_current=self._session_manifest_is_current,
            verify_session_fn=self._verify_catalog_session,
            verify_session=verify_session,
            cancelled=cancelled,
        )

    @staticmethod
    def _session_diagnostic(candidate_identity: str, error: object) -> dict[str, object]:
        return CatalogService.session_diagnostic(candidate_identity, error)

    def _session_summary(
        self,
        session_id: str,
        manifest: Mapping[str, object],
        manifest_payload: bytes,
        *,
        verification_error: DeviceRecordingError | None = None,
        verification_current: bool = True,
    ) -> dict[str, object]:
        return self._catalog.session_summary(
            session_id,
            manifest,
            manifest_payload,
            verification_error=verification_error,
            verification_current=verification_current,
        )

    # -- Snapshots, runtime, IMU, camera --

    def _recording_snapshot(
        self,
    ) -> tuple[int, str, dict[str, object] | None, dict[str, object] | None]:
        if self._active is not None:
            recording_state = (
                copy.deepcopy(self._failure_projection)
                if self._failure_projection is not None
                else self._active.current_recording_state
            )
            if recording_state is None:
                return self._revision, "blocked", None, None
            if self._failure_projection is None:
                if recording_state["state"] == "recording":
                    storage = self._volume.cached_storage()
                    if storage is not None:
                        recording_state = {
                            **recording_state,
                            "storage": {
                                **recording_state["storage"],
                                "remaining_bytes": storage.remaining_bytes,
                                "status": storage.status,
                                "writable": storage.writable,
                            },
                        }
                self._cached_active_state = copy.deepcopy(recording_state)
            state = str(recording_state["state"])
            return (
                int(recording_state["state_revision"]),
                state,
                {
                    "generation_id": self._active_plan.generation_id,
                    "recording_state": recording_state,
                },
                None,
            )
        retained = None
        for candidate in reversed(self._retained.values()):
            state = candidate.get("recording_state")
            if (
                isinstance(state, Mapping)
                and state.get("authority_epoch") == self._authority_epoch
                and state.get("state_revision") == self._revision
            ):
                retained = copy.deepcopy(candidate)
                break
        return self._revision, "idle", None, retained

    def capture_recording_progress(self) -> tuple[str, int, Mapping[str, object] | None]:
        if self._admission is None:
            with suppress(DeviceRecordingError):
                self._require_admission()
        with self._lock:
            revision, _, active, _ = self._recording_snapshot()
            state = None if active is None else active["recording_state"]
            return self._authority_epoch, revision, state

    def _refresh_latest_imu(self) -> None:
        with self._lock:
            plan = self._active_plan
            sources = self._sources
        observed_at = time.monotonic()
        latest = getattr(sources, "latest_imu_observation", None)
        if not callable(latest):
            return
        try:
            observation = latest()
        except Exception as error:
            operational_logger("recording").event(
                "latest_imu_poll_failed",
                level="warning",
                exception_type=type(error).__name__,
            )
            return
        if observation is None or not observation.samples:
            return
        sample = observation.samples[-1]
        with self._lock:
            if self._active_plan is plan:
                self._imu_tracker.store_latest_imu_locked(
                    None if plan is None or self._active is None else plan.session_id,
                    sample,
                    observed_at_monotonic=observed_at,
                )

    def _live_imu_snapshot_locked(self) -> Mapping[str, object] | None:
        plan = self._active_plan
        session_id = None if self._active is None or plan is None else plan.session_id
        return self._imu_tracker.snapshot_locked(session_id)

    def _camera_focus_status(self, *, raise_errors: bool = False) -> dict[str, object] | None:
        return CameraFocusProxy.camera_focus_status(self._sources, raise_errors=raise_errors)

    def _camera_connection_status(self) -> dict[str, object]:
        return CameraFocusProxy.camera_connection_status(getattr(self, "_sources", None))

    @staticmethod
    def _camera_not_connected() -> ProviderError:
        return CameraFocusProxy.camera_not_connected()

    def _require_camera_connected(self, *, restore_focus: bool = True) -> None:
        def get_pending() -> tuple[dict[str, object] | None, bool]:
            return self._pending_focus, self._focus_restore_invalid

        def set_pending(pending: dict[str, object] | None, invalid: bool) -> None:
            self._pending_focus = pending
            self._focus_restore_invalid = invalid

        sources = getattr(self, "_sources", None)
        if "_camera_proxy" in self.__dict__:
            self._camera_proxy.require_camera_connected(
                sources,
                restore_focus=restore_focus,
                get_pending_focus=get_pending,
                set_pending_focus=set_pending,
            )
            return
        CameraFocusProxy().require_camera_connected(
            sources,
            restore_focus=restore_focus,
            get_pending_focus=get_pending,
            set_pending_focus=set_pending,
        )

    def _calibration_source_supported(self) -> bool:
        if self._sources is None:
            return True
        return getattr(self._sources, "supports_calibration_capture", False) is True

    def _calibration_capture_capability(
        self,
        *,
        admission: VolumeAdmission | None,
        writable: bool,
    ) -> dict[str, object]:
        supported = self._calibration_source_supported()
        reason: str | None = None
        if not supported:
            reason = "capture_source_unsupported"
        elif self._active is not None:
            reason = "maintenance_or_capture_busy"
        elif self._camera_connection_status()["state"] != "connected":
            reason = "hardware_unavailable"
        elif admission is None or not writable or self._released:
            reason = "storage_unavailable"
        return {
            "supported": supported,
            "enabled": reason is None,
            "disabled_reason": reason,
            "required_video_layout": "split-eyes",
        }

    @staticmethod
    def _calibration_unavailable(reason: str) -> ProviderError:
        if reason not in CALIBRATION_CAPTURE_DISABLED_REASONS:
            raise RuntimeError("标定录制禁用原因无效")
        return ProviderError(
            "calibration_unavailable",
            "当前采集源不能生成标定所需的分眼 H.264 分段会话",
            status=HTTPStatus.SERVICE_UNAVAILABLE,
            retryable=False,
            details={"reason": reason},
        )

    def _runtime_snapshot(self) -> Mapping[str, object]:
        runtime = dict(copy.deepcopy(self._runtime()))
        camera = self._camera_connection_status()
        runtime["camera"] = camera
        runtime["camera_focus"] = (
            None if camera["state"] == "disconnected" else self._camera_focus_status()
        )
        self._refresh_latest_imu()
        with self._lock:
            runtime["live_imu"] = self._live_imu_snapshot_locked()
        return runtime

    def _build_capture_status_locked(self, runtime: Mapping[str, object]) -> Mapping[str, object]:
        source_revision, state, active, retained = self._recording_snapshot()
        rt = dict(runtime)
        rt["live_imu"] = self._live_imu_snapshot_locked()
        if (
            (
                active is None
                or self._failure_projection is not None
                or (self._active is not None and self._active.state == "sealed")
            )
            and rt["live_imu"] is not None
            and rt["live_imu"]["session_id"] is not None
        ):
            rt["live_imu"] = None
        return {
            "schema": "ylx.capture-status.v2",
            "authority_epoch": self._authority_epoch,
            "source_revision": source_revision,
            "snapshot": {
                "schema": "ylx.capture-snapshot-event.v2",
                "device_state": state,
                "active_recording": active,
                "retained_unsuccessful": retained,
                "runtime": rt,
            },
        }

    def capture_status(self) -> Mapping[str, object]:
        if self._admission is None:
            with suppress(DeviceRecordingError):
                self._require_admission()
        while True:
            with self._lock:
                focus_revision = self._focus_revision
            runtime = self._runtime_snapshot()
            with self._lock:
                if focus_revision != self._focus_revision:
                    continue
                return self._build_capture_status_locked(runtime)

    def capture_snapshot_event(self) -> Mapping[str, object]:
        status = self.capture_status()
        snapshot = status["snapshot"]
        session_id = None
        if isinstance(snapshot, Mapping):
            active = snapshot.get("active_recording")
            if isinstance(active, Mapping):
                state = active.get("recording_state")
                if isinstance(state, Mapping):
                    session_id = state.get("session_id")
        return {
            "authority_epoch": self._authority_epoch,
            "source_revision": status["source_revision"],
            "type": "snapshot",
            "occurred_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "session_id": session_id,
            "data": snapshot,
        }

    def device_descriptor(self, api_version: str, security_profile: str) -> Mapping[str, object]:
        admission = self._admission
        capture_ready = False
        try:
            admission = self._require_admission()
            status = self._check_generation(force=True)
            capacity = _stat_capacity(admission.mountpoint)
            total_bytes, available_bytes, _ = capacity
            writable = status.writable
            with suppress(DeviceRecordingError):
                self._require_capture_reserve(admission, capacity)
                capture_ready = writable and not self._released
        except DeviceRecordingError:
            total_bytes = 0 if admission is None else admission.total_bytes
            available_bytes = 0
            writable = False
        commit = self._config.session.commit
        capabilities: dict[str, object] = {
            "capture": capture_ready,
            "preview": True,
            "range_download": True,
            "network_mutation": (api_version == "v4" and self._network_controller_available()),
        }
        if api_version == "v4":
            capabilities.update(
                {
                    "session_list": True,
                    "session_detail": True,
                    "artifact_download": True,
                    "capture_status": True,
                    "session_deletion": True,
                }
            )
            capabilities["calibration_capture"] = self._calibration_capture_capability(
                admission=admission,
                writable=capture_ready,
            )
        descriptor = {
            "schema": f"ylx.device.{api_version}",
            "device": {
                "device_id": self._config.session.device_id,
                "device_label": self._config.session.device_label,
            },
            "hardware_fingerprint": self._config.session.hardware_fingerprint,
            "api_version": f"{api_version.removeprefix('v')}.0",
            "build": {
                "package_version": self._config.session.software_version,
                "commit": commit,
                "build_id": f"rp-ylx-{commit[:12]}",
            },
            "security_profile": security_profile,
            "capabilities": capabilities,
            "storage": {
                "volume_id": None if admission is None else admission.volume_id,
                "total_bytes": total_bytes,
                "available_bytes": available_bytes,
                "writable": writable,
            },
            "runtime": self._runtime_snapshot(),
        }
        projected = project_device_descriptor(descriptor, api_version=api_version)
        assert isinstance(projected, Mapping)
        return projected

    # -- Hardware & network proxies --

    def camera_focus_status(self) -> dict[str, object] | None:
        self._require_camera_connected(restore_focus=False)
        try:
            return self._camera_focus_status(raise_errors=True)
        except CameraError as error:
            status = HTTPStatus.SERVICE_UNAVAILABLE if error.retryable else HTTPStatus.CONFLICT
            raise ProviderError(
                error.code,
                error.message,
                status=status,
                retryable=error.retryable,
            ) from error

    @staticmethod
    def _network_operation_lock_path() -> Path:
        return network_operation_lock_path()

    def _get_leases(self) -> CaptureLeaseManager:
        if "_leases" in self.__dict__:
            return self._leases
        leases = self.__dict__.get("_compat_leases")
        if leases is None:
            leases = CaptureLeaseManager(self._network_operation_lock_path)
            leases.network_operation_lease = self.__dict__.get("_compat_network_operation_lease")
            leases.maintenance_lease = self.__dict__.get("_compat_maintenance_lease")
            self.__dict__["_compat_leases"] = leases
        return leases

    def _sync_compat_leases(self, leases: CaptureLeaseManager) -> None:
        if "_leases" not in self.__dict__:
            self.__dict__["_compat_network_operation_lease"] = leases.network_operation_lease
            self.__dict__["_compat_maintenance_lease"] = leases.maintenance_lease

    def _acquire_network_operation_lease(self) -> None:
        with self._get_lock():
            if "_lifecycle" in self.__dict__ and self._lifecycle._network_ipc_inflight > 0:
                raise ProviderError(
                    "network_mutation_active",
                    "网络事务执行期间不能开始录制",
                    status=HTTPStatus.CONFLICT,
                    retryable=True,
                )
        leases = self._get_leases()
        try:
            leases.acquire()
        finally:
            self._sync_compat_leases(leases)

    def _release_network_operation_lease(self) -> None:
        leases = self._get_leases()
        try:
            leases.release()
        finally:
            self._sync_compat_leases(leases)

    @staticmethod
    def _close_capture_leases(maintenance, descriptor: int | None) -> None:
        CaptureLeaseManager.close_capture_leases(maintenance, descriptor)

    def network_status(self) -> Mapping[str, object]:
        runtime = self._runtime()
        with self._lock:
            capture_active = self._active is not None
        return self._network_proxy.network_status(runtime, capture_active=capture_active)

    @staticmethod
    def _network_controller_available() -> bool:
        return NetworkControllerProxy(
            request_control_fn=lambda *args, **kwargs: request_network_control(*args, **kwargs)
        ).controller_available()

    @staticmethod
    def _network_mutation_unavailable(
        reason: str = "controller_unavailable",
        message: str = "网络写入控制器暂时不可用",
    ) -> ProviderError:
        return NetworkControllerProxy.network_mutation_unavailable(reason, message)

    def clock_status(self, principal_id: str) -> Mapping[str, object]:
        return self._clock_control("clock_status", principal_id, {})

    def clock_inspect(self, principal_id: str) -> Mapping[str, object]:
        return self._clock_control("clock_inspect", principal_id, {})

    def sync_clock(self, principal_id: str, body: Mapping[str, object]) -> Mapping[str, object]:
        lock = self._get_lock()
        has_lifecycle = "_lifecycle" in self.__dict__
        with lock:
            if getattr(self, "_active", None) is not None or (
                has_lifecycle and self._lifecycle._phase is not CapturePhase.IDLE
            ):
                if has_lifecycle and self._lifecycle._phase in {
                    CapturePhase.SAFE_SWAP_PENDING,
                    CapturePhase.RELEASED,
                }:
                    pass
                else:
                    raise ProviderError(
                        "capture_active",
                        "录制结束后再校准设备日期",
                        status=HTTPStatus.CONFLICT,
                        retryable=True,
                    )
            if has_lifecycle:
                self._lifecycle._clock_sync_inflight += 1
        try:
            # Run clock IPC outside self._lock so concurrent status/catalog readers never block.
            before = self.clock_status(principal_id)
            result = self._clock_control("clock_sync", principal_id, body)
            if before["source"] == "unsynchronized" and result["source"] == "client":
                self._notify_voice("clock_ready")
            return result
        finally:
            if has_lifecycle:
                with lock:
                    self._lifecycle._clock_sync_inflight -= 1
                    self._stop_condition.notify_all()

    def _clock_control(
        self, operation: str, principal_id: str, body: Mapping[str, object]
    ) -> Mapping[str, object]:
        proxy = self.__dict__.get("_clock_proxy")
        if proxy is None:
            proxy = ClockSyncProxy(
                request_control_fn=lambda *args, **kwargs: request_network_control(*args, **kwargs)
            )
        return proxy.clock_control(operation, principal_id, body)

    def _get_network_proxy(self) -> NetworkControllerProxy:
        if "_network_proxy" in self.__dict__:
            return self._network_proxy
        return NetworkControllerProxy(
            request_control_fn=lambda *args, **kwargs: request_network_control(*args, **kwargs)
        )

    def scan_networks(self) -> Mapping[str, object]:
        return self._get_network_proxy().scan_networks()

    def create_network_credential(self, command: NetworkCredentialCommand) -> Mapping[str, object]:
        return self._get_network_proxy().create_network_credential(command)

    def _network_command(self, operation: str, command: NetworkCommand) -> NetworkCommandResult:
        lock = self._get_lock()
        has_lifecycle = "_lifecycle" in self.__dict__
        proxy = self._get_network_proxy()
        with lock:
            busy = (
                getattr(self, "_active", None) is not None
                or getattr(self, "_closing", False)
                or (
                    has_lifecycle
                    and self._lifecycle._phase
                    in {
                        CapturePhase.STARTING,
                        CapturePhase.RECORDING,
                        CapturePhase.STOPPING,
                        CapturePhase.RECOVERING_FAILURE,
                        CapturePhase.COMMITTING_STATE,
                        CapturePhase.CLOSING,
                    }
                )
            )
            if busy:
                raise self._network_mutation_unavailable(
                    "capture_active",
                    "录制期间不能变更设备网络",
                )
            if has_lifecycle:
                self._lifecycle._network_ipc_inflight += 1
        try:
            # R-07: Run up to 20s network controller IPC outside coordinator._lock.
            return proxy.network_command(operation, command)
        finally:
            if has_lifecycle:
                with lock:
                    self._lifecycle._network_ipc_inflight -= 1
                    self._stop_condition.notify_all()

    def apply_network_desired_state(self, command: NetworkCommand) -> NetworkCommandResult:
        return self._network_command("apply", command)

    def retry_network_transaction(self, command: NetworkCommand) -> NetworkCommandResult:
        return self._network_command("retry", command)

    def forget_network_client_profile(self, command: NetworkCommand) -> NetworkCommandResult:
        return self._network_command("forget", command)

    def set_camera_focus(self, command: CaptureCommand) -> CaptureCommandResult:
        scope = (command.principal_id, "set_camera_focus", command.idempotency_key)
        value, auto_enabled = CameraFocusProxy.validate_focus_body(command.body)
        need_persist = False
        replay_result: CaptureCommandResult | None = None
        acquired_inflight = False
        with self._stop_condition:
            while True:
                self._lifecycle.wait_while_starting_locked()
                need_persist, replay_result = self._state.check_command_idempotency(
                    scope, command.canonical_body
                )
                stop_pending = (
                    self._stop_inflight is not None
                    or self._pending_stop_state_commit is not None
                    or self._stop_acceptance_inflight is not None
                )
                if replay_result is not None:
                    if stop_pending and need_persist:
                        raise ProviderError(
                            "capture_busy",
                            "录制正在收尾",
                            status=HTTPStatus.CONFLICT,
                            retryable=True,
                        )
                    break
                if stop_pending:
                    raise ProviderError(
                        "capture_busy", "录制正在收尾", status=HTTPStatus.CONFLICT, retryable=True
                    )
                inflight = self._lifecycle._focus_inflight
                if inflight is not None:
                    if inflight[0] == scope and inflight[1] != command.canonical_body:
                        raise ProviderError(
                            "idempotency_conflict",
                            "幂等键已经用于不同请求",
                            status=HTTPStatus.CONFLICT,
                        )
                    self._stop_condition.wait()
                    continue
                if (
                    self._lifecycle._deleting
                    or self._failure_inflight is not None
                    or self._failure_projection is not None
                    or self._async_stop_worker is not None
                    or self._closing
                ):
                    raise ProviderError("capture_busy", "录制正在收尾", status=HTTPStatus.CONFLICT)
                if self._sources is None:
                    raise self._camera_not_connected()
                self._lifecycle._focus_inflight = (scope, command.canonical_body)
                acquired_inflight = True
                break
        if replay_result is not None:
            if need_persist:
                self._persist_local_state()
            return replay_result
        try:
            # R-07: Run V4L2 camera connection & focus write outside lifecycle._lock.
            self._require_camera_connected(restore_focus=False)
            with self._lock:
                sources = self._sources
            if sources is None:
                raise self._camera_not_connected()
            try:
                focus_result = self._camera_proxy.apply_camera_focus(
                    sources,
                    value=value,
                    auto_enabled=auto_enabled,
                )
            finally:
                with self._lock:
                    self._focus_revision += 1
            cmd_result = CaptureCommandResult(200, focus_result)
            with self._lock:
                self._state._focus_to_persist = {
                    "value": focus_result["value"],
                    "auto_enabled": focus_result["auto_enabled"],
                }
                self._pending_focus = None
                self._focus_restore_invalid = False
                self._next_revision()
                self._cache_command(scope, command, cmd_result)
            self._persist_local_state()
            return cmd_result
        finally:
            if acquired_inflight:
                with self._stop_condition:
                    self._lifecycle._focus_inflight = None
                    self._stop_condition.notify_all()

    # -- Idempotency, voice, and capture timer --

    def _require_valid_command_receipt(self, scope: tuple[str, str, str]) -> None:
        self._state.require_valid_command_receipt(scope)

    def _notify_voice(self, event: str, identity: str = "") -> None:
        if "_state" in self.__dict__:
            self._state.notify_voice(event, identity)
            return
        volume = self._voice_settings.get("volume")
        notify_voice(event, identity, volume=volume if valid_volume(volume) else 0)

    def voice_status(self) -> dict[str, object]:
        with self._lock:
            return self._state.voice_status()

    def set_voice_volume(self, volume: int) -> dict[str, object]:
        if not valid_volume(volume):
            raise ProviderError("invalid_request", "音量须为0至100的整数", status=400)
        with self._stop_condition:
            while self._lifecycle._settings_updating:
                self._stop_condition.wait()
            self._lifecycle.require_idle_for_settings(
                "请在录制结束后调整提示音量",
                pending_safe_swap=self._pending_safe_swap,
            )
            self._lifecycle._settings_updating = True
        value = {"volume": volume}
        try:
            self._state._state_db.put("voice_feedback", value, history=True)
            with self._stop_condition:
                self._state._voice_settings = value
                return self._state.voice_status()
        finally:
            with self._stop_condition:
                self._lifecycle._settings_updating = False
                self._stop_condition.notify_all()

    def capture_timer_status(self) -> dict[str, object]:
        with self._lock:
            self._require_timer_settings()
            active = None
            timer = self._capture_timer
            if timer is not None and self._active_plan is timer[0]:
                plan, deadline, duration = timer
                active = {
                    "session_id": plan.session_id,
                    "duration_seconds": duration,
                    "remaining_seconds": max(0.0, min(duration, deadline - time.monotonic())),
                }
            return {
                "schema": "ylx.capture-timer.v1",
                "settings": dict(self._timer_settings),
                "active": active,
            }

    def _require_timer_settings(self) -> None:
        if "_state" in self.__dict__:
            self._state.require_timer_settings()
            return
        if not valid_timer_settings(self._timer_settings):
            raise ProviderError(
                "invalid_timer_settings",
                "定时设置损坏，请在待机时重新保存",
                status=HTTPStatus.CONFLICT,
            )

    def set_capture_timer(self, settings: Mapping[str, object]) -> dict[str, object]:
        if not valid_timer_settings(settings):
            raise ProviderError("invalid_request", "时长须为1至86400秒的整数", status=400)
        with self._stop_condition:
            while self._lifecycle._settings_updating:
                self._stop_condition.wait()
            self._lifecycle.require_idle_for_settings(
                "请在录制结束后设置定时",
                pending_safe_swap=self._pending_safe_swap,
            )
            self._lifecycle._settings_updating = True
        value = dict(settings)
        try:
            self._state._state_db.put("capture_timer", value, history=True)
            with self._stop_condition:
                self._state._timer_settings = value
                return self.capture_timer_status()
        finally:
            with self._stop_condition:
                self._lifecycle._settings_updating = False
                self._stop_condition.notify_all()

    def enforce_capture_timer(self) -> None:
        with self._lock:
            timer = self._capture_timer
            if (
                timer is None
                or self._active_plan is not timer[0]
                or self._closing
                or self._failure_projection is not None
                or time.monotonic() < timer[1]
            ):
                return
            session_id = timer[0].session_id
        body = {"reason": "timer"}
        self.stop_capture(
            CaptureCommand("capture-timer", session_id, body, _canonical_json_bytes(body)),
            expected_session_id=session_id,
        )

    # -- Start / Stop / Failure / Safe-Swap Lifecycle --

    def start_capture(self, command: CaptureCommand) -> CaptureCommandResult:
        scope = (command.principal_id, "start", command.idempotency_key)
        replay_to_persist: CaptureCommandResult | None = None
        with self._stop_condition:
            self._lifecycle.wait_while_starting_locked()
            need_persist, replay_result = self._state.check_command_idempotency(
                scope, command.canonical_body
            )
            stop_pending = (
                self._stop_inflight is not None
                or self._pending_stop_state_commit is not None
                or self._stop_acceptance_inflight is not None
            )
            if replay_result is not None:
                if stop_pending and need_persist:
                    raise ProviderError(
                        "capture_busy", "录制正在收尾", status=HTTPStatus.CONFLICT, retryable=True
                    )
                if need_persist:
                    replay_to_persist = replay_result
                else:
                    return replay_result
            else:
                if stop_pending:
                    raise ProviderError(
                        "capture_busy", "录制正在收尾", status=HTTPStatus.CONFLICT, retryable=True
                    )
                if (
                    self._lifecycle._deleting
                    or self._failure_inflight is not None
                    or self._failure_projection is not None
                    or self._failure_pending_commit is not None
                    or self._failure_retry is not None
                    or self._async_stop_worker is not None
                    or self._closing
                ):
                    raise ProviderError("capture_busy", "录制正在收尾", status=HTTPStatus.CONFLICT)
                if self._active is not None:
                    raise ProviderError("capture_busy", "已有活动录制", status=HTTPStatus.CONFLICT)
                if self._lifecycle._network_ipc_inflight > 0:
                    raise ProviderError(
                        "network_mutation_active",
                        "网络事务执行期间不能开始录制",
                        status=HTTPStatus.CONFLICT,
                        retryable=True,
                    )
                if self._lifecycle._clock_sync_inflight > 0:
                    raise ProviderError(
                        "capture_busy",
                        "设备正在校时，请稍后重试",
                        status=HTTPStatus.CONFLICT,
                        retryable=True,
                    )
                self._lifecycle._starting = True
                self._sync_phase()
        if replay_to_persist is not None:
            self._persist_local_state()
            return replay_to_persist
        try:
            result = self._start_capture(command.body)
            with self._stop_condition:
                self._cache_command(scope, command, result)
                self._lifecycle._starting = False
                self._sync_phase()
                self._stop_condition.notify_all()
            self._persist_local_state()
            return result
        finally:
            with self._stop_condition:
                if self._lifecycle._starting:
                    self._lifecycle._starting = False
                    if self._active is None:
                        self._release_network_operation_lease()
                    self._sync_phase()
                    self._stop_condition.notify_all()

    def _start_capture(self, body: Mapping[str, object]) -> CaptureCommandResult:
        try:
            return self._start_capture_checked(body)
        except Exception as error:
            self._notify_voice(
                "clock_required"
                if getattr(error, "code", "") in {"clock_unsynchronized", "clock_sync_unavailable"}
                else "start_failed"
            )
            raise

    def _start_capture_checked(self, body: Mapping[str, object]) -> CaptureCommandResult:
        self._require_timer_settings()
        if self._active is not None:
            raise ProviderError("capture_busy", "已有活动录制", status=HTTPStatus.CONFLICT)
        if body.get("mode") == "calibration" and not self._calibration_source_supported():
            raise self._calibration_unavailable("capture_source_unsupported")
        self._require_camera_connected()
        with self._lock:
            self._catalog_verification_epoch += 1
            self._catalog_verification_cancel.set()
        self._acquire_network_operation_lease()
        try:
            return self._start_capture_with_network_lease(body)
        except BaseException:
            if self._active is None:
                self._release_network_operation_lease()
            raise

    def _start_capture_with_network_lease(self, body: Mapping[str, object]) -> CaptureCommandResult:
        NetworkControllerProxy(
            request_control_fn=lambda *args, **kwargs: request_network_control(*args, **kwargs)
        ).require_network_idle()
        clock = self.clock_inspect("capture-start")
        if (
            not valid_clock_response(clock)
            or clock.get("schema") != "ylx.clock-status.v2"
            or clock.get("source") == "unsynchronized"
        ):
            raise ProviderError(
                "clock_unsynchronized", "请打开手机控制页面校准时间", status=409, retryable=True
            )
        clock_provenance = {
            key: clock[key] for key in ("source", "boot_id", "synchronized_at_ms", "age_seconds")
        }
        try:
            admission = self._require_admission()
        except DeviceRecordingError as error:
            raise ProviderError(
                error.code,
                error.message,
                status=HTTPStatus.CONFLICT,
                retryable=True,
            ) from error
        requested_volume = body.get("volume_id")
        if requested_volume is not None and requested_volume != admission.volume_id:
            raise ProviderError("volume_mismatch", "请求的录制卷未准入", status=HTTPStatus.CONFLICT)
        try:
            self._check_generation(force=True)
            self._require_capture_reserve(admission)
            if self._recovery_deferred:
                self._recover_sessions()
                self._require_capture_reserve(admission)
                if self._recovery_deferred:
                    raise DeviceRecordingError("storage_unavailable", "录制恢复正在等待可用空间")
        except DeviceRecordingError as error:
            raise ProviderError(error.code, error.message, status=HTTPStatus.CONFLICT) from error
        take = body.get("take")
        if not isinstance(take, Mapping):
            raise ProviderError("invalid_request", "take 无效", status=HTTPStatus.BAD_REQUEST)
        continuation_of = take.get("continuation_of") if take.get("kind") == "continue" else None
        take_id = uuid7()
        take_sequence = 1
        if continuation_of is not None:
            try:
                previous = self._load_manifest(str(continuation_of))
                previous_take = previous["take"]
                if not isinstance(previous_take, Mapping):
                    raise ValueError("take 无效")
                take_id = str(previous_take["take_id"])
                take_sequence = int(previous_take["sequence"]) + 1
            except (
                ArtifactAccessError,
                DeviceRecordingError,
                KeyError,
                TypeError,
                ValueError,
            ) as error:
                raise ProviderError(
                    "continuation_not_found",
                    "无法继续指定的已密封会话",
                    status=HTTPStatus.CONFLICT,
                ) from error
        session_id = uuid7()
        display_name = body.get("display_name")
        if not isinstance(display_name, str):
            display_name = datetime.now().astimezone().strftime("录制 %Y-%m-%d %H:%M:%S")
        plan = SessionPlan(
            session_id=session_id,
            volume_id=admission.volume_id,
            generation_id=admission.generation_id,
            capture_mode=str(body.get("mode")),
            display_name=display_name,
            take_id=take_id,
            take_sequence=take_sequence,
            continuation_of=None if continuation_of is None else str(continuation_of),
            clock_provenance=clock_provenance,
        )
        session_config = self._config.session
        if plan.capture_mode == "calibration":
            session_config = replace(
                session_config,
                video_layout="split-eyes",
                audio_enabled=False,
                recording_encoding=(
                    None
                    if session_config.recording_encoding is None
                    else replace(session_config.recording_encoding, codec="h264")
                ),
            )
        recorder = DeviceSessionRecorder(
            admission.sessions_root,
            session_config,
            plan,
            authority_epoch=self._authority_epoch,
            allocate_revision=self._next_revision,
            storage_status=lambda: self._check_generation(plan.generation_id),
            state_sink=lambda state: self._recording_state_changed(plan, state),
            queue_capacity=self._config.queue_capacity,
            enqueue_timeout=self._config.enqueue_timeout,
            checkpoint_interval=self._config.checkpoint_interval,
            metrics=self._metrics,
            before_write=self._before_write,
            native_data_plane=bool(getattr(self._sources, "requires_native_transaction", False)),
        )
        with self._lock:
            self._active_plan = plan
            self._cached_active_state = None
            self._pending_source_failure = None
            self._latest_imu = None
            self._safe_swap_resource = None
            self._pending_safe_swap = None
            self._pending_safe_swap_failure = None
        if not self._sources_keep_preview():
            self._preview.clear()
        try:
            # R-07: Start recorder and hardware sources outside lifecycle._lock.
            recorder.start()
            with self._lock:
                self._active = recorder
            self._persist_local_state()
            if self._sources is not None:
                self._sources.start(
                    mode=plan.capture_mode,
                    generation_id=plan.generation_id,
                    submit_frame=lambda observation: self._submit_frame(
                        observation,
                        generation_id=plan.generation_id,
                        retain_failure=False,
                    ),
                    submit_imu=lambda observation: self._submit_imu(
                        observation,
                        generation_id=plan.generation_id,
                        retain_failure=False,
                    ),
                    on_failure=lambda code, message: self._source_failure(
                        plan.generation_id,
                        code,
                        message,
                        expected_plan=plan,
                        defer_recovery=True,
                    ),
                    native_recorder=recorder,
                )
        except BaseException as error:
            # R-07: Run recorder.fail() outside lifecycle._lock so writer.join() cannot deadlock.
            if recorder.state in {"recording", "finalizing"}:
                recorder.fail(
                    "source_start_failed",
                    str(error),
                    recoverable=False,
                )
            state = recorder.current_recording_state
            with self._lock:
                if state is not None:
                    self._retained[plan.session_id] = {
                        "generation_id": plan.generation_id,
                        "recording_state": state,
                    }
                self._active = None
                self._active_plan = None
                self._capture_timer = None
                self._latest_imu = None
                self._sync_phase()
            self._preview.clear()
            self._persist_local_state()
            code = getattr(error, "code", "source_start_failed")
            message = getattr(error, "message", str(error))
            raise ProviderError(str(code), str(message), status=HTTPStatus.CONFLICT) from error
        with self._lock:
            if self._timer_settings["enabled"]:
                duration = self._timer_settings["duration_seconds"]
                self._capture_timer = (plan, time.monotonic() + duration, duration)
            else:
                self._capture_timer = None
            self._sync_phase()
        self._notify_voice("started", plan.session_id)
        return CaptureCommandResult(HTTPStatus.ACCEPTED, self.capture_status())

    def _source_failure(
        self,
        generation_id: str,
        code: str,
        message: str,
        *,
        expected_plan: SessionPlan | None = None,
        defer_recovery: bool = False,
    ) -> None:
        failure_recovery.source_failure(
            self,
            generation_id,
            code,
            message,
            expected_plan=expected_plan,
            defer_recovery=defer_recovery,
        )

    def enforce_storage_reserve(self) -> None:
        """Stop normally before exhausting the finalization reserve; never from a source thread."""
        with self._lock:
            if (
                self._active is None
                or self._active_plan is None
                or self._lifecycle._starting
                or self._closing
                or self._stop_inflight is not None
                or self._failure_projection is not None
            ):
                return
            admission = self._volume._admission
        if admission is None:
            admission = self._require_admission()
        with self._lock:
            if (
                self._active is None
                or self._active_plan is None
                or self._lifecycle._starting
                or self._closing
                or self._stop_inflight is not None
                or self._failure_projection is not None
            ):
                return
            plan = self._active_plan
            admission = self._volume._admission or admission
            if self._released:
                raise DeviceRecordingError("volume_released", "录制卷已经进入安全移除状态")
            if self._media_lost:
                raise DeviceRecordingError("media_lost", "录制介质已移除或被替换")
        try:
            storage, observed_at, violation = self._volume.check_reserve_violation(admission, plan)
        except DeviceRecordingError as error:
            with self._lock:
                if self._active_plan is not plan or self._volume._admission is not admission:
                    return
                if error.code == "media_lost":
                    self._media_lost = True
            raise
        with self._lock:
            if (
                self._active_plan is not plan
                or self._volume._admission is not admission
                or self._closing
                or self._stop_inflight is not None
                or self._failure_inflight is not None
            ):
                return
            self._volume.update_storage_cache(storage, observed_at)
            session_id = plan.session_id
        if violation is None:
            return
        body = {"reason": "storage_reserve"}
        self.stop_capture(
            CaptureCommand("storage-monitor", session_id, body, _canonical_json_bytes(body)),
            expected_session_id=session_id,
        )

    def request_stop_capture(self, command: CaptureCommand) -> CaptureCommandResult:
        """Durably accept a stop without holding an HTTP request through sealing."""
        return stop_workflow.request_stop_capture(self, command)

    def _prepare_stop_request(
        self,
        command: CaptureCommand,
        *,
        runtime: Mapping[str, object] | None = None,
    ) -> CaptureCommandResult | Callable[[], CaptureCommandResult] | object:
        return stop_workflow.prepare_stop_request(self, command, runtime=runtime)

    def _prepare_stop_receipt_commit(
        self,
        command: CaptureCommand,
        scope: tuple[str, str, str],
        result: CaptureCommandResult,
        *,
        retry: bool = False,
    ) -> Callable[[], CaptureCommandResult]:
        return stop_workflow.prepare_stop_receipt_commit(self, command, scope, result, retry=retry)

    def _finish_stop_acceptance(
        self,
        scope: tuple[str, str, str],
        result: CaptureCommandResult,
        rollback: Callable[[], None],
        *,
        persist: bool = True,
        ready: threading.Event | None = None,
        worker: threading.Thread | None = None,
        accepted: Callable[[], None] | None = None,
    ) -> CaptureCommandResult:
        return stop_workflow.finish_stop_acceptance(
            self,
            scope,
            result,
            rollback,
            persist=persist,
            ready=ready,
            worker=worker,
            accepted=accepted,
        )

    def _request_pending_safe_swap(
        self,
        command: CaptureCommand,
        scope: tuple[str, str, str],
        *,
        replay: CaptureCommandResult | None = None,
        runtime: Mapping[str, object] | None = None,
    ) -> Callable[[], CaptureCommandResult]:
        """Prepare a gated handoff worker; commit its acceptance outside the lock."""
        return stop_workflow.request_pending_safe_swap(
            self, command, scope, replay=replay, runtime=runtime
        )

    def _record_safe_swap_failure(
        self, pending: Mapping[str, object], error: BaseException
    ) -> None:
        stop_workflow.record_safe_swap_failure(self, pending, error)

    def stop_capture(
        self, command: CaptureCommand, *, expected_session_id: str | None = None
    ) -> CaptureCommandResult:
        return stop_workflow.stop_capture(self, command, expected_session_id=expected_session_id)

    def _commit_stop_replay(
        self,
        scope: tuple[str, str, str],
        result: CaptureCommandResult,
    ) -> CaptureCommandResult:
        return stop_workflow.commit_stop_replay(self, scope, result)

    def _retry_completed_stop_receipt(
        self,
        command: CaptureCommand,
        *,
        wait: bool = False,
    ) -> CaptureCommandResult | None:
        return stop_workflow.retry_completed_stop_receipt(self, command, wait=wait)

    def _finish_stop_capture(
        self,
        command: CaptureCommand,
        scope: tuple[str, str, str],
        recorder: DeviceSessionRecorder,
        plan: SessionPlan,
        *,
        remember_result: bool = True,
    ) -> CaptureCommandResult:
        return stop_workflow.finish_stop_capture(
            self, command, scope, recorder, plan, remember_result=remember_result
        )

    def _publish_safe_swap_command(self) -> CaptureCommandResult:
        self._publish_safe_swap()
        return CaptureCommandResult(HTTPStatus.NO_CONTENT, None)

    def _cache_command(
        self,
        scope: tuple[str, str, str],
        command: CaptureCommand,
        result: CaptureCommandResult,
    ) -> None:
        self._state.cache_command(scope, command, result)

    def _retain_active_failure(
        self,
        error: DeviceRecordingError,
        plan: SessionPlan | None,
        *,
        from_stop: bool = False,
        defer_recovery: bool = False,
    ) -> None:
        failure_recovery.retain_active_failure(
            self, error, plan, from_stop=from_stop, defer_recovery=defer_recovery
        )

    def _finish_active_failure(
        self, recorder: DeviceSessionRecorder, plan: SessionPlan, error: DeviceRecordingError
    ) -> None:
        failure_recovery.finish_active_failure(self, recorder, plan, error)

    def _commit_active_failure(self, recorder: DeviceSessionRecorder) -> None:
        failure_recovery.commit_active_failure(self, recorder)

    def _local_failure_state(
        self,
        plan: SessionPlan,
        error: DeviceRecordingError,
        *,
        media_lost: bool,
    ) -> Mapping[str, object]:
        return CaptureLifecycle.local_failure_state(
            plan,
            error,
            authority_epoch=self._authority_epoch,
            next_revision=self._next_revision,
            device_id=self._config.session.device_id,
            device_label=self._config.session.device_label,
            media_lost=media_lost,
        )

    # -- Frame / IMU ingestion & media loss --

    def submit_frame(
        self,
        observation: FrameObservation,
        *,
        generation_id: str | None = None,
    ) -> bool:
        return self._submit_frame(
            observation,
            generation_id=generation_id,
            retain_failure=True,
        )

    def _submit_frame(
        self,
        observation: FrameObservation,
        *,
        generation_id: str | None,
        retain_failure: bool,
    ) -> bool:
        failure: DeviceRecordingError | None = None
        active_plan: SessionPlan | None = None
        while True:
            with self._lock:
                if self._active is None:
                    raise DeviceRecordingError("invalid_state", "当前没有活动录制")
                if self._failure_projection is not None:
                    raise DeviceRecordingError("invalid_state", "录制正在收尾")
                admission_ready = self._volume._admission is not None
                if admission_ready:
                    try:
                        self._check_generation(generation_id or self._active_plan.generation_id)
                        accepted = self._active.submit_frame(observation)
                        preview_jpeg = observation.frame.left or observation.frame.raw_side_by_side
                        if preview_jpeg is not None:
                            with suppress(ValueError):
                                self._preview.publish(preview_jpeg)
                        return accepted
                    except DeviceRecordingError as error:
                        if error.code == "stale_generation" or not retain_failure:
                            raise
                        failure = error
                        active_plan = self._active_plan
                    break
            self._require_admission()
        if self._sources is not None:
            with suppress(Exception):
                self._sources.stop()
        if failure is not None:
            self._retain_active_failure(failure, active_plan)
        assert failure is not None
        raise failure

    def submit_imu(
        self,
        observation: ImuObservation,
        *,
        generation_id: str | None = None,
    ) -> bool:
        return self._submit_imu(
            observation,
            generation_id=generation_id,
            retain_failure=True,
        )

    def _submit_imu(
        self,
        observation: ImuObservation,
        *,
        generation_id: str | None,
        retain_failure: bool,
    ) -> bool:
        failure: DeviceRecordingError | None = None
        active_plan: SessionPlan | None = None
        while True:
            with self._lock:
                if self._active is None:
                    raise DeviceRecordingError("invalid_state", "当前没有活动录制")
                if self._failure_projection is not None:
                    raise DeviceRecordingError("invalid_state", "录制正在收尾")
                admission_ready = self._volume._admission is not None
                if admission_ready:
                    try:
                        self._check_generation(generation_id or self._active_plan.generation_id)
                        accepted = self._active.submit_imu(observation)
                        if accepted:
                            self._imu_tracker.store_latest_imu_locked(
                                self._active_plan.session_id,
                                observation.samples[-1],
                                observed_at_monotonic=time.monotonic(),
                            )
                        return accepted
                    except DeviceRecordingError as error:
                        if error.code == "stale_generation" or not retain_failure:
                            raise
                        failure = error
                        active_plan = self._active_plan
                    break
            self._require_admission()
        if self._sources is not None:
            with suppress(Exception):
                self._sources.stop()
        if failure is not None:
            self._retain_active_failure(failure, active_plan)
        assert failure is not None
        raise failure

    def report_media_loss(self, *, generation_id: str) -> None:
        admission = self._require_admission()
        with self._lock:
            if generation_id != admission.generation_id:
                raise DeviceRecordingError("stale_generation", "介质事件来自过期挂载代次")
            if self._active is None:
                return
            self._media_lost = True
            active_plan = self._active_plan
        if self._sources is not None:
            with suppress(Exception):
                self._sources.stop()
        error = DeviceRecordingError("media_lost", "录制介质在安全交接前被移除")
        self._retain_active_failure(error, active_plan)

    # -- Safe swap & artifact downloads --

    def _publish_safe_swap(self) -> None:
        stop_workflow.publish_safe_swap(self)

    def _check_pending_safe_swap(self, pending: Mapping[str, object]) -> None:
        stop_workflow.check_pending_safe_swap(self, pending)

    def current_safe_swap_receipt(self) -> object | None:
        with self._lock:
            failure = self._pending_safe_swap_failure
            if self._pending_safe_swap is not None and failure is not None:
                status, message = SAFE_SWAP_FAILURES[str(failure["code"])]
                raise ProviderError(
                    str(failure["code"]),
                    message,
                    status=status,
                    retryable=True,
                    details={"session_id": failure["session_id"]},
                )
            return copy.deepcopy(self._safe_swap_resource)

    def artifact_io_state(self) -> str | None:
        with self._lock:
            if self._failure_projection is not None:
                return "finalizing"
            if self._active is not None:
                return self._active.state
            if self._released or self._pending_safe_swap is not None:
                return "device-released"
            return None

    def _acquire_representation(self) -> Callable[[], None]:
        with self._lock:
            if (
                self._released
                or self._pending_safe_swap is not None
                or self._async_stop_reason == "safe_swap"
                or self._lifecycle._deleting
            ):
                raise ProviderError(
                    "volume_releasing",
                    "录制卷正在安全释放",
                    status=HTTPStatus.LOCKED,
                    retryable=True,
                )
            self._open_representations += 1
        try:
            self._check_generation()
        except DeviceRecordingError as error:
            with self._lock:
                self._open_representations -= 1
            raise ProviderError(
                error.code,
                error.message,
                status=HTTPStatus.CONFLICT,
                retryable=True,
            ) from error
        except BaseException:
            with self._lock:
                self._open_representations -= 1
            raise
        released = False

        def release() -> None:
            nonlocal released
            with self._lock:
                if not released:
                    released = True
                    self._open_representations -= 1

        return release

    def _open_store(self) -> _MultiRootSessionStore:
        return self._catalog.open_store(self._require_admission())

    def open_manifest(self, session_id: str, api_version: str) -> object:
        with ExitStack() as cleanup:
            release = self._acquire_representation()
            cleanup.callback(release)
            with closing(self._open_store()) as store:
                representation = store.open_manifest(session_id, api_version)
                cleanup.callback(representation.close)
            tracked = _TrackedRepresentation(representation, release)
            cleanup.pop_all()
            return tracked

    def open_verified_artifact(self, session_id: str, artifact_id: str, api_version: str) -> object:
        return self.open_cancellable_artifact(session_id, artifact_id, api_version)

    def open_cancellable_artifact(
        self,
        session_id: str,
        artifact_id: str,
        api_version: str,
        *,
        cancelled: Callable[[], bool] | None = None,
    ) -> object:
        with ExitStack() as cleanup:
            if cancelled is not None and cancelled():
                raise ProviderError(
                    "download_cancelled", "下载请求已取消", status=503, retryable=True
                )
            release = self._acquire_representation()
            cleanup.callback(release)
            with closing(self._open_store()) as store:
                representation = store.open_artifact(session_id, artifact_id, api_version)
                cleanup.callback(representation.close)
                transfer = self._state_db.begin_transfer(self.volume_id, session_id, artifact_id)

                def record_transfer(status: str, bytes_read: int) -> None:
                    try:
                        self._state_db.finish_transfer(transfer, status, bytes_read)
                    except (OSError, sqlite3.Error, PersistenceError):
                        operational_logger("recording").event(
                            "transfer_receipt_failed", level="error", transaction_id=session_id
                        )

                cleanup.callback(record_transfer, "failed", 0)
            tracked = _TrackedRepresentation(representation, release, record_transfer)
            cleanup.pop_all()
            return tracked

    def _load_manifest(self, session_id: str) -> Mapping[str, object]:
        return self._catalog.load_manifest(self._require_admission(), session_id)

    def delete_sessions(self, command: CaptureCommand) -> CaptureCommandResult:
        if not valid_delete_request(command.body):
            raise ProviderError("invalid_request", "删除请求无效", status=HTTPStatus.BAD_REQUEST)

        scope = (command.principal_id, "delete", command.idempotency_key)
        replay_to_persist: CaptureCommandResult | None = None
        catalog_acquired = False
        with self._stop_condition:
            self._lifecycle.wait_while_starting_locked()
            need_persist, replay_result = self._state.check_command_idempotency(
                scope, command.canonical_body
            )
            stop_pending = (
                self._stop_inflight is not None
                or self._pending_stop_state_commit is not None
                or self._stop_acceptance_inflight is not None
            )
            if replay_result is not None:
                if stop_pending and need_persist:
                    raise ProviderError(
                        "capture_busy", "录制正在收尾", status=HTTPStatus.CONFLICT, retryable=True
                    )
                if need_persist:
                    replay_to_persist = replay_result
                else:
                    return replay_result
            else:
                if stop_pending:
                    raise ProviderError(
                        "capture_busy", "录制正在收尾", status=HTTPStatus.CONFLICT, retryable=True
                    )
                if (
                    self._lifecycle._deleting
                    or self._failure_inflight is not None
                    or self._failure_projection is not None
                    or self._async_stop_worker is not None
                    or self._closing
                ):
                    raise ProviderError("capture_busy", "录制正在收尾", status=HTTPStatus.CONFLICT)
                if self._active is not None or self._stop_inflight is not None:
                    raise ProviderError(
                        "capture_busy", "录制进行中，不能删除", status=HTTPStatus.CONFLICT
                    )
                if self._released or self._pending_safe_swap is not None:
                    raise ProviderError(
                        "volume_releasing", "录制卷已释放或正在释放", status=HTTPStatus.LOCKED
                    )
                if self._open_representations:
                    raise ProviderError(
                        "download_busy", "下载进行中，不能删除", status=HTTPStatus.CONFLICT
                    )
                if self._catalog_verification_session is not None:
                    self._catalog_verification_epoch += 1
                    self._catalog_verification_cancel.set()
                    raise ProviderError(
                        "verification_cancelling",
                        "正在停止录像校验，请稍后重试删除",
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                        retryable=True,
                    )
                if not self._catalog_lock.acquire(blocking=False):
                    raise ProviderError(
                        "catalog_busy",
                        "录像目录正在更新，请稍后重试删除",
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                        retryable=True,
                    )
                catalog_acquired = True
                self._lifecycle._deleting = True
                self._sync_phase()
        if replay_to_persist is not None:
            self._persist_local_state()
            return replay_to_persist

        # R-07: Run generation check, delete_recordings (rmtree+fsync), and catalog cleanup
        # outside self._lock.
        try:
            try:
                self._check_generation(force=True)
            except DeviceRecordingError as error:
                raise ProviderError(
                    error.code, error.message, status=HTTPStatus.CONFLICT
                ) from error
            admission = self._require_admission()
            expected = {
                item["session_id"]: item["manifest_sha256"] for item in command.body["sessions"]
            }
            deleted_ids: list[str] = []
            try:
                result = delete_recordings(admission.catalog_roots, expected)
                deleted_ids = list(result["deleted_session_ids"])
            except CoreProviderError as error:
                if isinstance(error, ProviderError):
                    raise
                raise ProviderError(
                    error.code,
                    error.message,
                    status=error.status,
                    retryable=error.retryable,
                    details=error.details,
                ) from error
            finally:
                self._catalog.prune_after_deletion(admission.volume_id, set(expected), deleted_ids)
                self._volume.invalidate_storage_cache()
            cmd_result = CaptureCommandResult(HTTPStatus.OK, result)
            with self._lock:
                self._forget_deleted_outcomes(result["deleted_session_ids"])
                self._cache_command(scope, command, cmd_result)
            self._persist_local_state()
            return cmd_result
        finally:
            if catalog_acquired:
                self._catalog_lock.release()
            with self._stop_condition:
                self._lifecycle._deleting = False
                self._sync_phase()
                self._stop_condition.notify_all()

    def retained_unsuccessful_outcome(self, session_id: str) -> object | None:
        with self._lock:
            if self._active_plan is not None and self._active_plan.session_id == session_id:
                return None
            outcome = self._retained.get(session_id)
            if outcome is None:
                return None
            state = outcome["recording_state"]
            return {
                "schema": "ylx.retained-unsuccessful-session-resource.v2",
                "authority_epoch": state["authority_epoch"],
                "source_revision": state["state_revision"],
                "outcome": copy.deepcopy(outcome),
            }

    def list_sessions(
        self,
        *,
        cursor: str | None,
        limit: int,
        take_id: str | None,
        api_version: str = "v3",
    ) -> Mapping[str, object]:
        try:
            self._check_generation(force=True)
        except DeviceRecordingError as error:
            raise ProviderError(
                error.code,
                error.message,
                status=HTTPStatus.CONFLICT,
                retryable=True,
            ) from error
        self._require_admission()
        self._catalog_sessions()
        return self._catalog.list_sessions_page(
            cursor=cursor,
            limit=limit,
            take_id=take_id,
            api_version=api_version,
        )

    def latest_preview(self, *, fps: int | None, accept: str) -> PreviewResponse:
        with self._lock:
            if (
                self._pending_safe_swap is not None
                or self._released
                or self._async_stop_reason == "safe_swap"
            ):
                raise ProviderError(
                    "volume_releasing",
                    "录制卷正在安全释放",
                    status=HTTPStatus.LOCKED,
                    retryable=True,
                )
        self._require_camera_connected(restore_focus=False)
        start_preview = getattr(self._sources, "start_preview", None)
        try:
            if callable(start_preview):
                start_preview()
            return self._preview.latest_preview(fps=fps, accept=accept)
        except PreviewFrameUnavailable as error:
            if self._camera_connection_status()["state"] == "disconnected":
                raise self._camera_not_connected() from error
            raise
        except CameraError as error:
            if self._camera_connection_status()["state"] == "disconnected":
                raise self._camera_not_connected() from error
            status = HTTPStatus.SERVICE_UNAVAILABLE if error.retryable else HTTPStatus.CONFLICT
            raise ProviderError(
                error.code,
                error.message,
                status=status,
                retryable=error.retryable,
            ) from error
        except Exception as error:
            if self._camera_connection_status()["state"] == "disconnected":
                raise self._camera_not_connected() from error
            raise ProviderError(
                "preview_unavailable",
                str(error) or "当前没有可用的预览帧",
                status=HTTPStatus.SERVICE_UNAVAILABLE,
                retryable=True,
            ) from error

    def close(self) -> None:
        with self._close_lock:
            self._close_capture()

    def _close_capture(self) -> None:
        def cancel_verification() -> None:
            self._catalog_verification_epoch += 1
            self._catalog_verification_cancel.set()

        def publish_recovered(recovered: SealedDeviceSession) -> Path:
            return self._publish_recording_directory(recovered.path, recovered.manifest)

        def record_verified(session_id: str, sha256: str) -> None:
            with self._catalog_lock:
                self._verified[session_id] = sha256

        def retain_outcome(session_id: str, outcome: Mapping[str, object]) -> None:
            self._retained[session_id] = dict(outcome)

        self._lifecycle.close_capture(
            sources=self._sources,
            clear_preview=self._preview.clear,
            cancel_verification=cancel_verification,
            is_verifying=lambda: self._catalog_verification_session is not None,
            publish_recovered=publish_recovered,
            record_saved_prefix=self._record_saved_prefix,
            record_verified=record_verified,
            retain_outcome=retain_outcome,
            persist_state=self._persist_local_state,
            leases=self._leases,
            pending_safe_swap=lambda: self._pending_safe_swap,
        )
        self._latest_imu = None

    def __enter__(self) -> CaptureCoordinator:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


__all__ = [
    "CALIBRATION_CAPTURE_DISABLED_REASONS",
    "CaptureCoordinator",
    "CapturePhase",
    "CaptureSources",
    "CoordinatorConfig",
    "LEGACY_SESSIONS_DIRECTORY",
    "LIVE_IMU_STALE_SECONDS",
    "SAFE_SWAP_FAILURES",
    "SESSION_CATALOG_REVISION_DOMAIN",
    "SESSIONS_DIRECTORY",
    "VOLUME_MARKER",
    "VolumeAdmission",
    "_MultiRootSessionStore",
    "_TrackedRepresentation",
    "_VerifiedSessionSnapshot",
    "_canonical_json_bytes",
    "_encode_session_cursor",
    "_read_recovery_metadata",
    "_session_catalog_revision",
    "_stat_capacity",
    "initialize_capture_volume",
    "validate_capture_status",
]
