//! Core `CaptureCoordinator` state machine (`CapturePhase` transitions), `startCapture`,
//! camera focus, capture timer, status/readiness snapshots, and graceful `close()`.

use super::events::{CaptureEventBroadcaster, CaptureEventSubscription};
use super::receipts::{InFlightRegistration, InFlightTracker, ReceiptStore};
use super::types::{
    ActiveCaptureHandle, ActiveCaptureTimer, BackendStartRequest, CAMERA_FOCUS_SET_SCHEMA,
    CAPTURE_DIAGNOSTIC_EVENT_SCHEMA, CAPTURE_READINESS_SCHEMA, CAPTURE_SNAPSHOT_EVENT_SCHEMA,
    CAPTURE_STATE_EVENT_SCHEMA, CAPTURE_STATUS_SCHEMA, CAPTURE_TIMER_SCHEMA,
    CAPTURE_TIMER_SET_SCHEMA, CLOCK_CALIBRATION_SCHEMA, CameraFocusSetRequest, CameraFocusStatus,
    CaptureBackend, CaptureBusyState, CaptureEventType, CaptureMode, CapturePhase,
    CaptureProgressSnapshot, CaptureReadiness, CaptureReadinessStorage, CaptureSnapshotEventData,
    CaptureSnapshotRecording, CaptureStartRequest, CaptureStatusSnapshot, CaptureTimerSetRequest,
    CaptureTimerSettings, CaptureTimerStatus, CapturedArtifact, ClockSyncProbe, CoordinatorConfig,
    IdempotentOutcome, MAX_CALIBRATION_FILE_BYTES, MAX_TIMER_DURATION_SECONDS,
    RECORDING_STATE_SCHEMA, RETAINED_UNSUCCESSFUL_SCHEMA, RecordingDiagnostic, RecordingState,
    RecordingStateProgress, RecordingStateStorage, RetainedUnsuccessfulSessionResource,
    SAFE_SWAP_RECEIPT_RESOURCE_SCHEMA, SafeSwapReceiptResource, StopReason, TakeRequest,
    VerificationCounter, map_start_volume_error, request_fingerprint, sanitize_diag_code,
    sanitize_diag_message, validate_idempotency_key, validate_start_request,
};
use crate::audit::AuditLogger;
use crate::catalog::BinarySessionCatalog;
use crate::recovery::quarantine_session_directory;
use crate::util::{VolumeHandleLease, now_rfc3339_utc, sha256_hex, sweep_stale_atomic_temps};
use crate::volume::{ExfatVolumeManager, VolumeAdmission};
use crate::{StorageError, StorageResult, VolumeManager};
use miro_ego_core::CoreError;
use miro_ego_core::ids::{is_valid_uuid_v7, new_uuid_v4, new_uuid_v7};
use miro_ego_core::locks::{FlockGuard, LockMode, acquire_maintenance_lock};
use miro_ego_core::sys::clock::monotonic_ns;
use miro_ego_core::sys::fs::{read_bounded_bytes, read_bounded_json, write_bytes_atomic};
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub(super) struct SessionMetadata {
    pub session_id: String,
    pub take_id: String,
    pub take_sequence: u64,
    pub continuation_of: Option<String>,
    pub mode: CaptureMode,
    pub display_name: String,
    pub started_at: String,
    pub staging_dir: PathBuf,
    pub admission: VolumeAdmission,
    pub diagnostics: Vec<RecordingDiagnostic>,
    pub extra_logs: Vec<CapturedArtifact>,
}

pub(super) struct ActiveSessionContext {
    pub session_id: String,
    pub take_id: String,
    pub take_sequence: u64,
    pub continuation_of: Option<String>,
    pub mode: CaptureMode,
    pub display_name: String,
    pub started_at: String,
    pub started_monotonic_ns: u64,
    pub staging_dir: PathBuf,
    pub admission: VolumeAdmission,
    pub recording_stage: String,
    pub stop_reason: Option<StopReason>,
    pub last_progress: CaptureProgressSnapshot,
    pub verification_progress: Option<VerificationCounter>,
    pub diagnostics: Vec<RecordingDiagnostic>,
    pub extra_logs: Vec<CapturedArtifact>,
    pub timer_deadline_ns: Option<u64>,
    pub timer_duration_seconds: Option<u32>,
    pub handle: Option<Box<dyn ActiveCaptureHandle>>,
    pub _maintenance_lock: Option<Arc<FlockGuard>>,
}

impl ActiveSessionContext {
    pub(super) fn metadata_snapshot(&self) -> SessionMetadata {
        SessionMetadata {
            session_id: self.session_id.clone(),
            take_id: self.take_id.clone(),
            take_sequence: self.take_sequence,
            continuation_of: self.continuation_of.clone(),
            mode: self.mode,
            display_name: self.display_name.clone(),
            started_at: self.started_at.clone(),
            staging_dir: self.staging_dir.clone(),
            admission: self.admission.clone(),
            diagnostics: self.diagnostics.clone(),
            extra_logs: self.extra_logs.clone(),
        }
    }
}

pub(super) struct CoordinatorState {
    pub phase: CapturePhase,
    pub phase_atomic: Arc<AtomicU8>,
    pub authority_epoch: String,
    pub source_revision: u64,
    pub admission: Option<VolumeAdmission>,
    pub active_volume_id: Option<String>,
    pub volume_released: bool,
    pub safe_swap_in_progress: bool,
    pub volume_admission_in_progress: bool,
    pub released_mount_signature: Option<(String, u64, String)>,
    pub saw_unmount_since_release: bool,
    pub active: Option<ActiveSessionContext>,
    pub retained_unsuccessful: Option<CaptureSnapshotRecording>,
    pub timer_settings: CaptureTimerSettings,
    pub last_failure_reason: Option<String>,
    pub recovery_failure_streak: u32,
    pub next_recovery_retry_at: Option<Instant>,
}

impl CoordinatorState {
    pub(super) fn transition(&mut self, next: CapturePhase) -> StorageResult<()> {
        if !self.phase.can_transition_to(next) {
            return Err(StorageError::conflict(
                "invalid_state_transition",
                format!(
                    "cannot transition capture phase from {} to {}",
                    self.phase.as_str(),
                    next.as_str()
                ),
            ));
        }
        self.phase = next;
        self.phase_atomic.store(next.to_u8(), Ordering::Release);
        self.source_revision = self.source_revision.saturating_add(1);
        Ok(())
    }

    pub(super) fn bump_revision(&mut self) -> u64 {
        self.source_revision = self.source_revision.saturating_add(1);
        self.source_revision
    }

    pub(super) fn is_busy_for_ops(&self) -> bool {
        self.phase.is_busy() || self.safe_swap_in_progress || self.volume_admission_in_progress
    }

    pub(super) fn busy_error(&self, action: &str) -> StorageError {
        if self.phase == CapturePhase::Idle {
            if self.safe_swap_in_progress {
                return StorageError::conflict(
                    "capture_busy",
                    format!("cannot {action} while safe swap is in progress"),
                );
            }
            if self.volume_admission_in_progress {
                return StorageError::conflict(
                    "capture_busy",
                    format!("cannot {action} while volume admission is in progress"),
                );
            }
        }
        let msg = format!(
            "cannot {action} while coordinator is in phase {}",
            self.phase.as_str()
        );
        match self.phase {
            CapturePhase::Starting | CapturePhase::Recording => {
                StorageError::capture_busy(CaptureBusyState::Recording, msg)
            }
            CapturePhase::Stopping => {
                let stage = self
                    .active
                    .as_ref()
                    .map(|ctx| CaptureBusyState::from_recording_stage(&ctx.recording_stage))
                    .unwrap_or(CaptureBusyState::Finalizing);
                StorageError::capture_busy(stage, msg)
            }
            _ => StorageError::conflict("capture_busy", msg),
        }
    }
}

pub(super) type SafeSwapReleaseHook = Arc<dyn Fn() + Send + Sync>;

/// Authoritative native Rust capture coordinator managing the capture FSM, volume reserve
/// enforcement, zero-reread sealing, catalog admission, batch deletion, and events.
pub struct CaptureCoordinator {
    pub(super) config: CoordinatorConfig,
    pub(super) volume: Arc<ExfatVolumeManager>,
    pub(super) catalog: Arc<BinarySessionCatalog>,
    pub(super) backend: Arc<dyn CaptureBackend>,
    pub(super) receipts: ReceiptStore,
    pub(super) inflight: InFlightTracker,
    pub(super) events: CaptureEventBroadcaster,
    pub(super) phase_atomic: Arc<AtomicU8>,
    pub(super) timer_write_lock: Mutex<()>,
    pub(super) audit_logger: Mutex<Option<Arc<AuditLogger>>>,
    pub(super) clock_probe: Mutex<Option<Arc<dyn ClockSyncProbe>>>,
    pub(super) release_hooks: Mutex<Vec<SafeSwapReleaseHook>>,
    pub(super) audio_requested_mode: Mutex<String>,
    pub(super) safe_swap_drain_timeout_ms: AtomicU64,
    pub(super) state: Mutex<CoordinatorState>,
    pub(super) phase_cv: Condvar,
}

impl CaptureCoordinator {
    pub fn new(
        config: CoordinatorConfig,
        volume: Arc<ExfatVolumeManager>,
        catalog: Arc<BinarySessionCatalog>,
        backend: Arc<dyn CaptureBackend>,
    ) -> Self {
        let timer_settings = config
            .timer_settings_path
            .as_deref()
            .and_then(|p| read_bounded_json::<CaptureTimerSettings>(p, 4096, 8).ok())
            .filter(|s| (1..=MAX_TIMER_DURATION_SECONDS).contains(&s.duration_seconds))
            .unwrap_or_default();

        volume.set_volume_released(false);
        let admission = volume.admit(None).ok();
        let volume_writable = admission.as_ref().is_some_and(|a| a.writable);
        let active_volume_id = admission.as_ref().map(|a| a.volume_id.clone());
        let receipts = ReceiptStore::open(
            volume.paths().clone(),
            volume_writable,
            active_volume_id.as_deref(),
        );

        // Invalidate stale safe-swap receipt if it does not match the current mount generation.
        if let Ok(Some(existing_receipt)) = receipts.get_safe_swap(volume_writable) {
            let matches_current = admission.as_ref().is_some_and(|a| {
                a.volume_id == existing_receipt.volume_id
                    && a.generation_id == existing_receipt.generation_id
            });
            if !matches_current {
                receipts.set_safe_swap(None, volume_writable);
            }
        }

        let phase_atomic = Arc::new(AtomicU8::new(CapturePhase::Idle.to_u8()));

        Self {
            config,
            volume,
            catalog,
            backend,
            receipts,
            inflight: InFlightTracker::new(),
            events: CaptureEventBroadcaster::new(),
            phase_atomic: Arc::clone(&phase_atomic),
            timer_write_lock: Mutex::new(()),
            audit_logger: Mutex::new(None),
            clock_probe: Mutex::new(None),
            release_hooks: Mutex::new(Vec::new()),
            audio_requested_mode: Mutex::new("device_default".to_owned()),
            safe_swap_drain_timeout_ms: AtomicU64::new(5_000),
            state: Mutex::new(CoordinatorState {
                phase: CapturePhase::Idle,
                phase_atomic,
                authority_epoch: new_uuid_v4(),
                source_revision: 0,
                admission,
                active_volume_id,
                volume_released: false,
                safe_swap_in_progress: false,
                volume_admission_in_progress: false,
                released_mount_signature: None,
                saw_unmount_since_release: false,
                active: None,
                retained_unsuccessful: None,
                timer_settings,
                last_failure_reason: None,
                recovery_failure_streak: 0,
                next_recovery_retry_at: None,
            }),
            phase_cv: Condvar::new(),
        }
    }

    /// Register an [`AuditLogger`] whose active `/data/logs/audit` handle is flushed and closed
    /// automatically before emitting a `SafeSwapReceipt`.
    pub fn with_audit_logger(self, audit: Arc<AuditLogger>) -> Self {
        self.set_audit_logger(audit);
        self
    }

    pub fn set_audit_logger(&self, audit: Arc<AuditLogger>) {
        if let Ok(mut guard) = self.audit_logger.lock() {
            *guard = Some(audit);
        }
    }

    /// Register a [`ClockSyncProbe`] used to enforce boot-scoped clock synchronization
    /// (when `config.require_clock_sync == true`) and emit `logs/clock-calibration.json`.
    pub fn with_clock_probe(self, probe: Arc<dyn ClockSyncProbe>) -> Self {
        self.set_clock_probe(probe);
        self
    }

    pub fn set_clock_probe(&self, probe: Arc<dyn ClockSyncProbe>) {
        if let Ok(mut guard) = self.clock_probe.lock() {
            *guard = Some(probe);
        }
    }

    /// Register an additional zero-handle release callback invoked during `StopReason::SafeSwap`.
    pub fn with_safe_swap_release_hook(self, hook: SafeSwapReleaseHook) -> Self {
        self.set_safe_swap_release_hook(hook);
        self
    }

    pub fn set_safe_swap_release_hook(&self, hook: SafeSwapReleaseHook) {
        if let Ok(mut guard) = self.release_hooks.lock() {
            guard.push(hook);
        }
    }

    /// Override the maximum time `StopReason::SafeSwap` waits for in-flight `VolumeHandleLease`s
    /// to drain to `0` before failing closed with `503 volume_busy` (default `5s`).
    pub fn set_safe_swap_drain_timeout(&self, timeout: Duration) {
        let ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
        self.safe_swap_drain_timeout_ms.store(ms, Ordering::Release);
    }

    /// Configure the default `audio_requested_mode` (`"device_default" | "disabled" | "enabled"`)
    /// persisted into staging `capture.json` for crash recovery provenance.
    pub fn with_audio_requested_mode(self, mode: impl Into<String>) -> Self {
        self.set_audio_requested_mode(mode);
        self
    }

    pub fn set_audio_requested_mode(&self, mode: impl Into<String>) {
        let candidate = mode.into();
        if matches!(
            candidate.as_str(),
            "device_default" | "disabled" | "enabled"
        ) && let Ok(mut guard) = self.audio_requested_mode.lock()
        {
            *guard = candidate;
        }
    }

    /// Shared open-handle counter for tracking active artifact/log file descriptors on `/data`.
    pub fn open_handle_counter(&self) -> Arc<AtomicUsize> {
        self.volume.handle_counter()
    }

    /// Current number of open file handles tracked on `/data` (including `AuditLogger` if open).
    pub fn open_handle_count(&self) -> usize {
        let audit_open = self
            .audit_logger
            .lock()
            .ok()
            .and_then(|g| g.as_ref().map(|a| usize::from(a.has_open_file())))
            .unwrap_or(0);
        self.volume.open_handle_count().saturating_add(audit_open)
    }

    /// Acquire an RAII [`VolumeHandleLease`] on the recording volume.
    pub fn acquire_open_handle_lease(&self) -> VolumeHandleLease {
        self.volume.acquire_handle_lease()
    }

    /// Atomically acquire an RAII [`VolumeHandleLease`] only when the volume is not released.
    pub fn try_acquire_open_handle_lease(&self) -> Option<VolumeHandleLease> {
        self.volume.try_acquire_handle_lease()
    }

    pub(super) fn close_all_volume_handles_for_safe_swap(&self) {
        let close_audit = || {
            if let Ok(guard) = self.audit_logger.lock()
                && let Some(audit) = guard.as_ref()
            {
                audit.close_active_file();
            }
        };
        close_audit();
        let hooks = self
            .release_hooks
            .lock()
            .ok()
            .map(|g| g.clone())
            .unwrap_or_default();
        for hook in hooks {
            hook();
        }
        close_audit();
    }

    pub fn volume(&self) -> &Arc<ExfatVolumeManager> {
        &self.volume
    }

    pub fn catalog(&self) -> &Arc<BinarySessionCatalog> {
        &self.catalog
    }

    pub fn events(&self) -> &CaptureEventBroadcaster {
        &self.events
    }

    pub fn subscribe_events(&self) -> CaptureEventSubscription {
        self.events.subscribe()
    }

    /// Lock-free read of the current [`CapturePhase`] FSM state (`AtomicU8` mirror).
    pub fn phase(&self) -> StorageResult<CapturePhase> {
        Ok(CapturePhase::from_u8(
            self.phase_atomic.load(Ordering::Acquire),
        ))
    }

    /// Return the typed [`CaptureBusyState`] if capture/finalization is currently active.
    pub fn current_busy_state(&self) -> StorageResult<Option<CaptureBusyState>> {
        let guard = self
            .state
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
        let state = match guard.phase {
            CapturePhase::Starting | CapturePhase::Recording => Some(CaptureBusyState::Recording),
            CapturePhase::Stopping => Some(
                guard
                    .active
                    .as_ref()
                    .map(|ctx| CaptureBusyState::from_recording_stage(&ctx.recording_stage))
                    .unwrap_or(CaptureBusyState::Finalizing),
            ),
            _ => None,
        };
        Ok(state)
    }

    /// Wait until the coordinator exits any busy phase (`Starting`/`Recording`/`Stopping`/`Recovering`/`Deleting`).
    pub fn wait_until_not_busy(&self, timeout: Duration) -> StorageResult<CapturePhase> {
        let deadline = Instant::now() + timeout;
        let mut guard = self
            .state
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
        while guard.is_busy_for_ops() {
            let now = Instant::now();
            if now >= deadline {
                return Ok(guard.phase);
            }
            let (next, _) = self
                .phase_cv
                .wait_timeout(guard, deadline.saturating_duration_since(now))
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            guard = next;
        }
        Ok(guard.phase)
    }

    pub(super) fn acquire_shared_maintenance_lock(&self) -> StorageResult<Option<Arc<FlockGuard>>> {
        let Some(run_dir) = &self.config.run_dir else {
            return Ok(None);
        };
        let deadline = Instant::now() + Duration::from_millis(150);
        loop {
            match acquire_maintenance_lock(run_dir, LockMode::Shared, true) {
                Ok(lock) => return Ok(Some(Arc::new(lock))),
                Err(CoreError::LockBusy { .. }) => {
                    if Instant::now() >= deadline {
                        return Err(StorageError::service_unavailable(
                            "maintenance_busy",
                            "maintenance operation holds exclusive lock; retry shortly",
                        ));
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                Err(other) => return Err(StorageError::from(other)),
            }
        }
    }

    fn current_clock_probe(&self) -> Option<Arc<dyn ClockSyncProbe>> {
        self.clock_probe.lock().ok().and_then(|g| g.clone())
    }

    fn is_clock_synchronized(&self) -> bool {
        self.current_clock_probe()
            .is_some_and(|p| p.snapshot().source.is_synchronized())
    }

    /// Return current authority-bearing capture status snapshot (`miro-ego.capture-status.v1`).
    pub fn capture_status(&self) -> StorageResult<CaptureStatusSnapshot> {
        let runtime = self.backend.runtime_observation().to_openapi_runtime_json();
        let guard = self
            .state
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
        Ok(self.build_status_snapshot_locked(&guard, runtime))
    }

    /// Evaluate read-only capture admission readiness (`miro-ego.capture-readiness.v1`).
    pub fn capture_readiness(&self) -> StorageResult<CaptureReadiness> {
        let (phase, volume_released, busy_ops) = {
            let guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            (guard.phase, guard.volume_released, guard.is_busy_for_ops())
        };

        let reserve = self.volume.check_reserve()?;
        let camera_connected = self.backend.is_camera_connected();

        let (code, message) = if phase == CapturePhase::Recovering {
            (
                "recovery_pending".to_owned(),
                "recovering interrupted capture; please wait".to_owned(),
            )
        } else if phase == CapturePhase::Closing {
            (
                "capture_busy".to_owned(),
                "capture coordinator is closing".to_owned(),
            )
        } else if busy_ops {
            (
                "capture_busy".to_owned(),
                "capture or finalization is active".to_owned(),
            )
        } else if phase == CapturePhase::Failed {
            (
                "coordinator_failed".to_owned(),
                "capture coordinator requires recovery before starting".to_owned(),
            )
        } else if volume_released {
            (
                "volume_released".to_owned(),
                "recording volume has been released for safe swap".to_owned(),
            )
        } else if !reserve.ok {
            (reserve.code, reserve.message)
        } else if !camera_connected {
            (
                "hardware_unavailable".to_owned(),
                "configured camera is not connected".to_owned(),
            )
        } else if self.config.require_clock_sync && !self.is_clock_synchronized() {
            (
                "clock_unsynchronized".to_owned(),
                "device clock must be synchronized in this boot before starting capture".to_owned(),
            )
        } else {
            ("ready".to_owned(), "ready to start capture".to_owned())
        };

        Ok(CaptureReadiness {
            schema: CAPTURE_READINESS_SCHEMA.to_owned(),
            allowed: code == "ready",
            code,
            message,
            storage: CaptureReadinessStorage {
                recording: reserve.recording,
                system: reserve.system,
                backing: None,
            },
        })
    }

    /// Read current V4L2 camera focus status.
    pub fn get_camera_focus(&self) -> StorageResult<CameraFocusStatus> {
        if !self.backend.is_camera_connected() {
            return Err(StorageError::camera_not_connected(
                "configured camera is not connected",
            ));
        }
        self.backend.camera_focus()?.ok_or_else(|| {
            StorageError::unsupported(
                "camera_focus_unsupported",
                "camera does not expose focus_absolute control",
            )
        })
    }

    /// Idempotently update manual or automatic camera focus.
    pub fn set_camera_focus(
        &self,
        req: CameraFocusSetRequest,
        idempotency_key: Option<&str>,
    ) -> StorageResult<IdempotentOutcome<CameraFocusStatus>> {
        self.set_camera_focus_for_principal(req, idempotency_key, "")
    }

    /// Idempotently update manual or automatic camera focus scoped to `principal_id`.
    pub fn set_camera_focus_for_principal(
        &self,
        req: CameraFocusSetRequest,
        idempotency_key: Option<&str>,
        principal_id: &str,
    ) -> StorageResult<IdempotentOutcome<CameraFocusStatus>> {
        if req.schema != CAMERA_FOCUS_SET_SCHEMA {
            return Err(StorageError::invalid_input(
                "invalid_request",
                format!("camera focus request schema must be {CAMERA_FOCUS_SET_SCHEMA}"),
            ));
        }
        if req.value.is_none() && req.auto_enabled.is_none() {
            return Err(StorageError::invalid_input(
                "invalid_request",
                "camera focus request must specify value, auto_enabled, or both",
            ));
        }

        let volume_writable = self.volume.is_mounted_and_writable();
        let req_hash = request_fingerprint(&req)?;
        let mut inflight_guard = None;
        if let Some(key) = idempotency_key {
            validate_idempotency_key(key)?;
            if let Some(cached) = self.receipts.lookup_idempotency(
                principal_id,
                "set_camera_focus",
                key,
                &req_hash,
                volume_writable,
            )? {
                let status: CameraFocusStatus =
                    serde_json::from_value(cached).map_err(|err| StorageError::Catalog {
                        message: format!("corrupt cached focus receipt: {err}"),
                    })?;
                return Ok(IdempotentOutcome {
                    value: status,
                    replayed: true,
                });
            }
            match self
                .inflight
                .acquire(principal_id, "set_camera_focus", key, &req_hash)?
            {
                InFlightRegistration::Replayed(cached) => {
                    let status: CameraFocusStatus =
                        serde_json::from_value(cached).map_err(|err| StorageError::Catalog {
                            message: format!("corrupt replayed focus receipt: {err}"),
                        })?;
                    return Ok(IdempotentOutcome {
                        value: status,
                        replayed: true,
                    });
                }
                InFlightRegistration::Leader(leader) => {
                    if let Some(cached) = self.receipts.lookup_idempotency(
                        principal_id,
                        "set_camera_focus",
                        key,
                        &req_hash,
                        volume_writable,
                    )? {
                        let status: CameraFocusStatus = serde_json::from_value(cached.clone())
                            .map_err(|err| StorageError::Catalog {
                                message: format!("corrupt cached focus receipt: {err}"),
                            })?;
                        leader.succeed(cached);
                        return Ok(IdempotentOutcome {
                            value: status,
                            replayed: true,
                        });
                    }
                    inflight_guard = Some(leader);
                }
            }
        }

        let exec_res: StorageResult<CameraFocusStatus> = (|| {
            if !self.backend.is_camera_connected() {
                return Err(StorageError::camera_not_connected(
                    "configured camera is not connected",
                ));
            }
            let current = self.backend.camera_focus()?.ok_or_else(|| {
                StorageError::unsupported(
                    "camera_focus_unsupported",
                    "camera does not expose focus_absolute control",
                )
            })?;

            if let Some(val) = req.value {
                if val < current.minimum || val > current.maximum {
                    return Err(StorageError::unprocessable(
                        "invalid_camera_focus",
                        format!(
                            "focus value {val} outside allowed range {}..={}",
                            current.minimum, current.maximum
                        ),
                    ));
                }
                if current.step > 1 && (val - current.minimum) % current.step != 0 {
                    return Err(StorageError::unprocessable(
                        "invalid_camera_focus",
                        format!(
                            "focus value {val} is not aligned with step {}",
                            current.step
                        ),
                    ));
                }
            }
            if req.auto_enabled.is_some() && !current.auto_supported {
                return Err(StorageError::unprocessable(
                    "invalid_camera_focus",
                    "camera does not expose focus_auto control",
                ));
            }

            self.backend.set_camera_focus(&req)
        })();

        let updated = match exec_res {
            Ok(u) => u,
            Err(err) => {
                if let Some(leader) = inflight_guard {
                    leader.fail(&err);
                }
                return Err(err);
            }
        };
        if let Some(key) = idempotency_key {
            let json_val = serde_json::to_value(&updated).unwrap_or(Value::Null);
            self.receipts.store_idempotency(
                principal_id,
                "set_camera_focus",
                key,
                &req_hash,
                json_val.clone(),
                self.volume.is_mounted_and_writable(),
            );
            if let Some(leader) = inflight_guard {
                leader.succeed(json_val);
            }
        }
        Ok(IdempotentOutcome {
            value: updated,
            replayed: false,
        })
    }

    /// Read persistent capture timer settings and active take countdown.
    pub fn get_capture_timer(&self) -> StorageResult<CaptureTimerStatus> {
        let now_ns = monotonic_ns().unwrap_or(0);
        let guard = self
            .state
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
        let active = if guard.phase == CapturePhase::Recording {
            guard.active.as_ref().and_then(|ctx| {
                let deadline = ctx.timer_deadline_ns?;
                let duration = ctx.timer_duration_seconds?;
                let remaining = if now_ns >= deadline {
                    0.0
                } else {
                    (deadline - now_ns) as f64 / 1_000_000_000.0
                };
                Some(ActiveCaptureTimer {
                    session_id: ctx.session_id.clone(),
                    duration_seconds: duration,
                    remaining_seconds: remaining.min(f64::from(duration)),
                })
            })
        } else {
            None
        };
        Ok(CaptureTimerStatus {
            schema: CAPTURE_TIMER_SCHEMA.to_owned(),
            settings: guard.timer_settings,
            active,
        })
    }

    /// Replace persistent capture timer settings while idle.
    pub fn set_capture_timer(
        &self,
        req: CaptureTimerSetRequest,
    ) -> StorageResult<CaptureTimerStatus> {
        if req.schema != CAPTURE_TIMER_SET_SCHEMA {
            return Err(StorageError::invalid_input(
                "invalid_request",
                format!("timer request schema must be {CAPTURE_TIMER_SET_SCHEMA}"),
            ));
        }
        if !(1..=MAX_TIMER_DURATION_SECONDS).contains(&req.duration_seconds) {
            return Err(StorageError::invalid_input(
                "invalid_timer",
                format!("duration_seconds must be in 1..={MAX_TIMER_DURATION_SECONDS}"),
            ));
        }

        let new_settings = CaptureTimerSettings {
            enabled: req.enabled,
            duration_seconds: req.duration_seconds,
        };

        let _timer_io_guard = self
            .timer_write_lock
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("timer_write_lock")))?;

        {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.is_busy_for_ops() || guard.phase == CapturePhase::Closing {
                return Err(guard.busy_error("change capture timer settings"));
            }
            if let Some(path) = &self.config.timer_settings_path {
                let mut bytes = serde_json::to_vec_pretty(&new_settings).map_err(|err| {
                    StorageError::invalid_input("timer_encode_failed", err.to_string())
                })?;
                bytes.push(b'\n');
                write_bytes_atomic(path, &bytes, 0o640, true).map_err(|err| {
                    StorageError::io("timer_save_failed", "persist capture timer settings", err)
                })?;
            }
            guard.timer_settings = new_settings;
        }

        self.get_capture_timer()
    }

    /// Idempotently start a new capture or continuation take.
    pub fn start_capture(
        &self,
        req: CaptureStartRequest,
        idempotency_key: Option<&str>,
    ) -> StorageResult<IdempotentOutcome<CaptureStatusSnapshot>> {
        self.start_capture_for_principal(req, idempotency_key, "")
    }

    /// Idempotently start a new capture or continuation take scoped to `principal_id`.
    pub fn start_capture_for_principal(
        &self,
        req: CaptureStartRequest,
        idempotency_key: Option<&str>,
        principal_id: &str,
    ) -> StorageResult<IdempotentOutcome<CaptureStatusSnapshot>> {
        validate_start_request(&req)?;
        let volume_writable_hint = self.volume.is_mounted_and_writable();
        let req_hash = request_fingerprint(&req)?;
        let mut inflight_guard = None;
        if let Some(key) = idempotency_key {
            validate_idempotency_key(key)?;
            if let Some(cached) = self.receipts.lookup_idempotency(
                principal_id,
                "start_capture",
                key,
                &req_hash,
                volume_writable_hint,
            )? {
                let snap: CaptureStatusSnapshot =
                    serde_json::from_value(cached).map_err(|e| StorageError::Catalog {
                        message: format!("corrupt cached start receipt: {e}"),
                    })?;
                return Ok(IdempotentOutcome {
                    value: snap,
                    replayed: true,
                });
            }
            match self
                .inflight
                .acquire(principal_id, "start_capture", key, &req_hash)?
            {
                InFlightRegistration::Replayed(cached) => {
                    let snap: CaptureStatusSnapshot =
                        serde_json::from_value(cached).map_err(|e| StorageError::Catalog {
                            message: format!("corrupt replayed start receipt: {e}"),
                        })?;
                    return Ok(IdempotentOutcome {
                        value: snap,
                        replayed: true,
                    });
                }
                InFlightRegistration::Leader(leader) => {
                    if let Some(cached) = self.receipts.lookup_idempotency(
                        principal_id,
                        "start_capture",
                        key,
                        &req_hash,
                        volume_writable_hint,
                    )? {
                        let snap: CaptureStatusSnapshot = serde_json::from_value(cached.clone())
                            .map_err(|e| StorageError::Catalog {
                                message: format!("corrupt cached start receipt: {e}"),
                            })?;
                        leader.succeed(cached);
                        return Ok(IdempotentOutcome {
                            value: snap,
                            replayed: true,
                        });
                    }
                    inflight_guard = Some(leader);
                }
            }
        }

        let exec_res =
            self.execute_start_capture_inner(&req, principal_id, idempotency_key, &req_hash);
        match exec_res {
            Ok(status_snapshot) => {
                if let Some(leader) = inflight_guard {
                    let json_val = serde_json::to_value(&status_snapshot).unwrap_or(Value::Null);
                    leader.succeed(json_val);
                }
                Ok(IdempotentOutcome {
                    value: status_snapshot,
                    replayed: false,
                })
            }
            Err(err) => {
                if let Some(leader) = inflight_guard {
                    leader.fail(&err);
                }
                Err(err)
            }
        }
    }

    fn execute_start_capture_inner(
        &self,
        req: &CaptureStartRequest,
        principal_id: &str,
        idempotency_key: Option<&str>,
        req_hash: &str,
    ) -> StorageResult<CaptureStatusSnapshot> {
        let prev_admission = {
            let guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase != CapturePhase::Idle || guard.is_busy_for_ops() {
                return Err(guard.busy_error("start capture"));
            }
            if guard.volume_released || self.volume.is_volume_released() {
                return Err(StorageError::conflict(
                    "volume_released",
                    "recording volume was released for safe swap",
                ));
            }
            guard.admission.clone()
        };

        if let TakeRequest::Continue { continuation_of } = &req.take {
            let _ = self.resolve_continuation_locked(continuation_of, None)?;
        }

        if !self.backend.is_camera_connected() {
            return Err(StorageError::camera_not_connected(
                "configured camera is not connected",
            ));
        }

        // Enforce boot-scoped clock synchronization guard before side effects.
        let clock_snapshot = self.current_clock_probe().map(|p| p.snapshot());
        if self.config.require_clock_sync
            && !clock_snapshot
                .as_ref()
                .is_some_and(|snap| snap.source.is_synchronized())
        {
            return Err(StorageError::conflict(
                "clock_unsynchronized",
                "device clock must be synchronized via NTP or /api/v1/clock/sync in the current boot before starting capture",
            ));
        }

        // Validate optional external camera/IMU calibration file before entering Starting.
        let external_calibration_bytes = if let Some(ref cal_path) = self.config.calibration_file {
            let raw = read_bounded_bytes(cal_path, MAX_CALIBRATION_FILE_BYTES).map_err(|err| {
                StorageError::service_unavailable(
                    "calibration_unavailable",
                    format!("unable to read configured calibration_file: {err}"),
                )
            })?;
            let parsed: Value = serde_json::from_slice(&raw).map_err(|err| {
                StorageError::service_unavailable(
                    "calibration_unavailable",
                    format!("configured calibration_file is not valid JSON: {err}"),
                )
            })?;
            let mut normalized = serde_json::to_vec_pretty(&parsed).map_err(|err| {
                StorageError::service_unavailable(
                    "calibration_unavailable",
                    format!("failed to normalize calibration_file JSON: {err}"),
                )
            })?;
            normalized.push(b'\n');
            Some(normalized)
        } else {
            None
        };

        let prev_binding = prev_admission.as_ref().map(|p| {
            (
                p.volume_id.as_str(),
                p.device_id,
                p.mount_identity.as_str(),
                p.generation_id.as_str(),
            )
        });
        let admission = self
            .volume
            .admit(prev_binding)
            .map_err(map_start_volume_error)?;
        self.receipts
            .bind_volume(Some(&admission.volume_id), admission.writable);

        if let Some(requested_vid) = &req.volume_id
            && requested_vid != &admission.volume_id
        {
            return Err(StorageError::unprocessable(
                "volume_mismatch",
                "requested volume_id does not match admitted recording volume",
            ));
        }
        let reserve = self.volume.check_reserve()?;
        if !reserve.ok {
            let reserve_code = match reserve.code.as_str() {
                "insufficient_space" => "insufficient_space",
                "insufficient_inodes" => "insufficient_inodes",
                "volume_invalid" => "volume_invalid",
                _ => "storage_unavailable",
            };
            return Err(StorageError::unprocessable(reserve_code, reserve.message));
        }

        let maintenance_lock = self.acquire_shared_maintenance_lock()?;
        let local_starting_lock = maintenance_lock.clone();

        let session_id = new_uuid_v7();
        let started_at = now_rfc3339_utc();
        let started_monotonic_ns = monotonic_ns().unwrap_or(1).max(1);
        let display_name = req
            .display_name
            .clone()
            .unwrap_or_else(|| format!("Miro-Ego {started_at}"));
        let staging_dir = self
            .volume
            .paths()
            .staging_dir()
            .join(format!("{session_id}.partial"));

        let (authority_epoch, take_id, take_sequence, continuation_of) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase != CapturePhase::Idle || guard.is_busy_for_ops() {
                return Err(guard.busy_error("start capture"));
            }
            if guard.volume_released || self.volume.is_volume_released() {
                return Err(StorageError::conflict(
                    "volume_released",
                    "recording volume was released for safe swap",
                ));
            }

            let (take_id, take_sequence, continuation_of) = match &req.take {
                TakeRequest::New => (new_uuid_v7(), 1_u64, None),
                TakeRequest::Continue { continuation_of } => {
                    let active_cont = guard
                        .active
                        .as_ref()
                        .and_then(|a| a.continuation_of.as_deref());
                    let (tid, seq) =
                        self.resolve_continuation_locked(continuation_of, active_cont)?;
                    (tid, seq, Some(continuation_of.clone()))
                }
            };

            let (timer_deadline_ns, timer_duration_seconds) = if guard.timer_settings.enabled {
                let duration_ns = u64::from(guard.timer_settings.duration_seconds) * 1_000_000_000;
                (
                    Some(started_monotonic_ns.saturating_add(duration_ns)),
                    Some(guard.timer_settings.duration_seconds),
                )
            } else {
                (None, None)
            };

            guard.active = Some(ActiveSessionContext {
                session_id: session_id.clone(),
                take_id: take_id.clone(),
                take_sequence,
                continuation_of: continuation_of.clone(),
                mode: req.mode,
                display_name: display_name.clone(),
                started_at: started_at.clone(),
                started_monotonic_ns,
                staging_dir: staging_dir.clone(),
                admission: admission.clone(),
                recording_stage: "recording".to_owned(),
                stop_reason: None,
                last_progress: CaptureProgressSnapshot::default(),
                verification_progress: None,
                diagnostics: Vec::new(),
                extra_logs: Vec::new(),
                timer_deadline_ns,
                timer_duration_seconds,
                handle: None,
                _maintenance_lock: maintenance_lock,
            });
            guard.active_volume_id = Some(admission.volume_id.clone());
            guard.admission = Some(admission.clone());
            guard.transition(CapturePhase::Starting)?;
            self.phase_cv.notify_all();
            (
                guard.authority_epoch.clone(),
                take_id,
                take_sequence,
                continuation_of,
            )
        };
        self.catalog.mark_mount_dirty();
        self.receipts.set_safe_swap(None, admission.writable);

        if let Err(err) = fs::create_dir_all(staging_dir.join("video")) {
            cleanup_empty_staging_attempt(&staging_dir);
            let storage_err = StorageError::io(
                "staging_create_failed",
                "create staging partial directory",
                err,
            );
            self.fail_start_attempt(
                &session_id,
                &take_id,
                &display_name,
                &admission,
                "staging_create_failed",
                &storage_err.to_string(),
            );
            drop(local_starting_lock);
            return Err(storage_err);
        }

        // Persist provenance log artifacts (`logs/clock-calibration.json`, `logs/camera-calibration.json`)
        // when configured.
        let mut staged_extra_logs: Vec<CapturedArtifact> = Vec::new();
        let provenance_res: StorageResult<()> = (|| {
            if clock_snapshot.is_some() || external_calibration_bytes.is_some() {
                fs::create_dir_all(staging_dir.join("logs")).map_err(|err| {
                    StorageError::io("staging_logs_create_failed", "create staging logs/", err)
                })?;
            }
            if let Some(ref clk) = clock_snapshot {
                let doc = json!({
                    "schema": CLOCK_CALIBRATION_SCHEMA,
                    "session_id": session_id,
                    "captured_at": started_at,
                    "boot_id": clk.boot_id,
                    "source": clk.source.as_str(),
                    "unix_time_ms": clk.unix_time_ms,
                    "monotonic_ns": clk.monotonic_ns,
                    "synchronized_at_ms": clk.synchronized_at_ms,
                });
                let mut clk_bytes =
                    serde_json::to_vec_pretty(&doc).map_err(|err| StorageError::Catalog {
                        message: format!("serialize clock-calibration.json failed: {err}"),
                    })?;
                clk_bytes.push(b'\n');
                let clk_sha = sha256_hex(&clk_bytes);
                write_bytes_atomic(
                    &staging_dir.join("logs/clock-calibration.json"),
                    &clk_bytes,
                    0o640,
                    true,
                )
                .map_err(|err| {
                    StorageError::io(
                        "clock_calibration_write_failed",
                        "write clock-calibration.json",
                        err,
                    )
                })?;
                staged_extra_logs.push(CapturedArtifact {
                    role: "log.clock-calibration".to_owned(),
                    path: "logs/clock-calibration.json".to_owned(),
                    media_type: "application/json".to_owned(),
                    bytes: clk_bytes.len() as u64,
                    sha256: Some(clk_sha),
                });
            }
            if let Some(ref cal_bytes) = external_calibration_bytes {
                let cal_sha = sha256_hex(cal_bytes);
                write_bytes_atomic(
                    &staging_dir.join("logs/camera-calibration.json"),
                    cal_bytes,
                    0o640,
                    true,
                )
                .map_err(|err| {
                    StorageError::io(
                        "camera_calibration_write_failed",
                        "write camera-calibration.json",
                        err,
                    )
                })?;
                staged_extra_logs.push(CapturedArtifact {
                    role: "log.camera-calibration".to_owned(),
                    path: "logs/camera-calibration.json".to_owned(),
                    media_type: "application/json".to_owned(),
                    bytes: cal_bytes.len() as u64,
                    sha256: Some(cal_sha),
                });
            }
            Ok(())
        })();
        if let Err(storage_err) = provenance_res {
            cleanup_empty_staging_attempt(&staging_dir);
            self.fail_start_attempt(
                &session_id,
                &take_id,
                &display_name,
                &admission,
                storage_err.code(),
                &storage_err.to_string(),
            );
            drop(local_starting_lock);
            return Err(storage_err);
        }

        let audio_req_mode = self
            .audio_requested_mode
            .lock()
            .ok()
            .map(|g| g.clone())
            .unwrap_or_else(|| "device_default".to_owned());
        let capture_marker = json!({
            "session_id": session_id,
            "take_id": take_id,
            "take_sequence": take_sequence,
            "continuation_of": continuation_of,
            "volume_id": admission.volume_id,
            "capture_mode": req.mode.as_str(),
            "display_name": display_name,
            "started_at": started_at,
            "timezone": self.config.timezone,
            "segment_frames": self.config.segment_frames,
            "audio_requested_mode": audio_req_mode,
            "device": {
                "device_id": self.config.device_id,
                "device_label": self.config.device_label,
                "hardware_fingerprint": self.config.hardware_fingerprint,
                "platform": self.config.platform,
                "software_version": self.config.software_version,
                "commit": self.config.commit,
            },
            "camera": {
                "width": self.config.camera_width,
                "height": self.config.camera_height,
                "sensor_fps": self.config.sensor_fps,
                "frame_decimation": self.config.frame_decimation,
            },
            "encoding": self.config.encoding,
        });
        let capture_marker_res: StorageResult<()> = (|| {
            let mut bytes = serde_json::to_vec_pretty(&capture_marker).map_err(|err| {
                StorageError::Catalog {
                    message: format!("serialize capture.json failed: {err}"),
                }
            })?;
            bytes.push(b'\n');
            write_bytes_atomic(&staging_dir.join("capture.json"), &bytes, 0o640, true).map_err(
                |err| StorageError::io("capture_marker_write_failed", "write capture.json", err),
            )
        })();
        if let Err(storage_err) = capture_marker_res {
            cleanup_empty_staging_attempt(&staging_dir);
            self.fail_start_attempt(
                &session_id,
                &take_id,
                &display_name,
                &admission,
                storage_err.code(),
                &storage_err.to_string(),
            );
            drop(local_starting_lock);
            return Err(storage_err);
        }

        let initial_rec_state = RecordingState {
            schema: RECORDING_STATE_SCHEMA.to_owned(),
            state: "recording".to_owned(),
            authority_epoch: authority_epoch.clone(),
            state_revision: 1,
            updated_at: started_at.clone(),
            session_id: session_id.clone(),
            take_id: take_id.clone(),
            display_name: display_name.clone(),
            device: self.config.device_identity(),
            storage: RecordingStateStorage {
                volume_id: admission.volume_id.clone(),
                status: "mounted".to_owned(),
                writable: admission.writable,
                remaining_bytes: Some(admission.available_bytes),
            },
            progress: RecordingStateProgress {
                elapsed_seconds: 0.0,
                captured_frames: 0,
                bytes_written: 0,
                verification: None,
            },
            diagnostics: Vec::new(),
        };
        let recording_marker_res: StorageResult<()> = (|| {
            let mut bytes = serde_json::to_vec_pretty(&initial_rec_state).map_err(|err| {
                StorageError::Catalog {
                    message: format!("serialize recording.json failed: {err}"),
                }
            })?;
            bytes.push(b'\n');
            write_bytes_atomic(&staging_dir.join("recording.json"), &bytes, 0o640, true).map_err(
                |err| {
                    StorageError::io("recording_marker_write_failed", "write recording.json", err)
                },
            )
        })();
        if let Err(storage_err) = recording_marker_res {
            cleanup_empty_staging_attempt(&staging_dir);
            self.fail_start_attempt(
                &session_id,
                &take_id,
                &display_name,
                &admission,
                storage_err.code(),
                &storage_err.to_string(),
            );
            drop(local_starting_lock);
            return Err(storage_err);
        }

        let pre_start_check: StorageResult<()> = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase == CapturePhase::Closing {
                if guard
                    .active
                    .as_ref()
                    .is_some_and(|c| c.session_id == session_id)
                {
                    let _ = guard.active.take();
                }
                Err(guard.busy_error("start capture"))
            } else if guard.phase != CapturePhase::Starting
                || guard
                    .active
                    .as_ref()
                    .is_none_or(|c| c.session_id != session_id)
            {
                Err(guard.busy_error("start capture"))
            } else {
                Ok(())
            }
        };
        if let Err(err) = pre_start_check {
            cleanup_empty_staging_attempt(&staging_dir);
            drop(local_starting_lock);
            return Err(err);
        }

        let backend_req = BackendStartRequest {
            session_id: session_id.clone(),
            take_id: take_id.clone(),
            mode: req.mode,
            staging_dir: staging_dir.clone(),
            started_at: started_at.clone(),
            started_monotonic_ns,
            encoding: self.config.encoding.clone(),
        };
        let handle = match self.backend.begin_capture(&backend_req) {
            Ok(h) => h,
            Err(err) => {
                cleanup_empty_staging_attempt(&staging_dir);
                if matches!(
                    err.code(),
                    "camera_not_connected" | "calibration_unavailable"
                ) {
                    let released = {
                        let mut guard = match self.state.lock() {
                            Ok(g) => g,
                            Err(poisoned) => poisoned.into_inner(),
                        };
                        if guard
                            .active
                            .as_ref()
                            .is_some_and(|c| c.session_id == session_id)
                        {
                            let r = guard.active.take();
                            let _ = guard.transition(CapturePhase::Idle);
                            self.phase_cv.notify_all();
                            r
                        } else {
                            None
                        }
                    };
                    drop(released);
                } else {
                    self.fail_start_attempt(
                        &session_id,
                        &take_id,
                        &display_name,
                        &admission,
                        err.code(),
                        &err.to_string(),
                    );
                }
                drop(local_starting_lock);
                return Err(err);
            }
        };

        // Persist `recording.json` while STILL in `CapturePhase::Starting` before transitioning
        // `Starting -> Recording` so concurrent `stopCapture` never races marker persistence.
        let started_progress = handle.progress();
        let next_rev = {
            let guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            guard.source_revision.saturating_add(1)
        };
        let started_elapsed = if started_progress.elapsed_seconds > 0.0 {
            started_progress.elapsed_seconds
        } else {
            monotonic_ns()
                .unwrap_or(started_monotonic_ns)
                .saturating_sub(started_monotonic_ns) as f64
                / 1_000_000_000.0
        };
        let started_rec_state = RecordingState {
            schema: RECORDING_STATE_SCHEMA.to_owned(),
            state: "recording".to_owned(),
            authority_epoch: authority_epoch.clone(),
            state_revision: next_rev,
            updated_at: now_rfc3339_utc(),
            session_id: session_id.clone(),
            take_id: take_id.clone(),
            display_name: display_name.clone(),
            device: self.config.device_identity(),
            storage: RecordingStateStorage {
                volume_id: admission.volume_id.clone(),
                status: "mounted".to_owned(),
                writable: admission.writable,
                remaining_bytes: Some(admission.available_bytes),
            },
            progress: RecordingStateProgress {
                elapsed_seconds: started_elapsed.max(0.0),
                captured_frames: started_progress.captured_frames,
                bytes_written: started_progress.bytes_written,
                verification: None,
            },
            diagnostics: Vec::new(),
        };
        let post_begin_marker_res: StorageResult<()> = (|| {
            let mut bytes = serde_json::to_vec_pretty(&started_rec_state).map_err(|err| {
                StorageError::Catalog {
                    message: format!("serialize recording.json failed: {err}"),
                }
            })?;
            bytes.push(b'\n');
            write_bytes_atomic(&staging_dir.join("recording.json"), &bytes, 0o640, true).map_err(
                |err| {
                    StorageError::io(
                        "recording_marker_write_failed",
                        "persist recording.json after start",
                        err,
                    )
                },
            )
        })();
        if let Err(storage_err) = post_begin_marker_res {
            handle.abort();
            cleanup_empty_staging_attempt(&staging_dir);
            self.fail_start_attempt(
                &session_id,
                &take_id,
                &display_name,
                &admission,
                storage_err.code(),
                &storage_err.to_string(),
            );
            drop(local_starting_lock);
            return Err(storage_err);
        }

        let runtime_json = self.backend.runtime_observation().to_openapi_runtime_json();
        let (status_snapshot, rev) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            let session_matches = guard
                .active
                .as_ref()
                .is_some_and(|c| c.session_id == session_id);
            if !session_matches || guard.phase == CapturePhase::Closing {
                let err = guard.busy_error("start capture");
                let released = if session_matches {
                    guard.active.take()
                } else {
                    None
                };
                self.phase_cv.notify_all();
                drop(guard);
                handle.abort();
                drop(released);
                cleanup_empty_staging_attempt(&staging_dir);
                drop(local_starting_lock);
                return Err(err);
            }
            if let Err(err) = guard.transition(CapturePhase::Recording) {
                let released = if guard
                    .active
                    .as_ref()
                    .is_some_and(|c| c.session_id == session_id)
                {
                    guard.active.take()
                } else {
                    None
                };
                self.phase_cv.notify_all();
                drop(guard);
                handle.abort();
                drop(released);
                cleanup_empty_staging_attempt(&staging_dir);
                drop(local_starting_lock);
                return Err(err);
            }
            guard.retained_unsuccessful = None;
            if let Some(ctx) = guard.active.as_mut() {
                ctx.extra_logs = staged_extra_logs;
                ctx.handle = Some(handle);
            }
            self.phase_cv.notify_all();
            let snap = self.build_status_snapshot_locked(&guard, runtime_json);
            (snap, guard.source_revision)
        };
        drop(local_starting_lock);

        self.events.emit(
            &authority_epoch,
            rev,
            CaptureEventType::State,
            Some(session_id.clone()),
            json!({
                "schema": CAPTURE_STATE_EVENT_SCHEMA,
                "state": "recording",
                "volume_id": admission.volume_id,
                "generation_id": admission.generation_id,
            }),
        );
        self.events.emit(
            &authority_epoch,
            rev,
            CaptureEventType::Snapshot,
            Some(session_id),
            serde_json::to_value(&status_snapshot.snapshot).unwrap_or(Value::Null),
        );

        if let Some(key) = idempotency_key
            && let Ok(json_val) = serde_json::to_value(&status_snapshot)
        {
            self.receipts.store_idempotency(
                principal_id,
                "start_capture",
                key,
                req_hash,
                json_val,
                admission.writable,
            );
        }

        Ok(status_snapshot)
    }

    fn resolve_continuation_locked(
        &self,
        continuation_of: &str,
        active_continuation: Option<&str>,
    ) -> StorageResult<(String, u64)> {
        if active_continuation == Some(continuation_of) {
            return Err(StorageError::unprocessable(
                "invalid_continuation",
                "predecessor session already has an active continuation take",
            ));
        }
        let pred = self
            .catalog
            .get_indexed_session(continuation_of)?
            .ok_or_else(|| {
                StorageError::unprocessable(
                    "invalid_continuation",
                    "predecessor session does not exist in catalog",
                )
            })?;
        if pred.device.device_id != self.config.device_id {
            return Err(StorageError::unprocessable(
                "invalid_continuation",
                "predecessor session belongs to a different device",
            ));
        }
        if !self.catalog.continuations_of(continuation_of)?.is_empty() {
            return Err(StorageError::unprocessable(
                "invalid_continuation",
                "predecessor session already has a continuation branch",
            ));
        }
        Ok((pred.take_id, pred.take_sequence.saturating_add(1)))
    }

    /// Recover the current successful safe-swap handoff receipt (`GET /api/v1/capture/safe-swap`).
    pub fn safe_swap_receipt(&self) -> StorageResult<Option<SafeSwapReceiptResource>> {
        let (volume_released, admission) = {
            let guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            (guard.volume_released, guard.admission.clone())
        };
        if !volume_released {
            return Ok(None);
        }
        let Some(adm) = admission else {
            return Ok(None);
        };
        let receipt = self
            .receipts
            .get_safe_swap(self.volume.is_mounted_and_writable())?;
        Ok(receipt
            .filter(|r| r.volume_id == adm.volume_id && r.generation_id == adm.generation_id)
            .map(|r| SafeSwapReceiptResource {
                schema: SAFE_SWAP_RECEIPT_RESOURCE_SCHEMA.to_owned(),
                receipt: r,
            }))
    }

    /// Read one per-session retained unsuccessful terminal outcome
    /// (`GET /api/v1/sessions/{session_id}/unsuccessful-outcome`).
    pub fn retained_unsuccessful_outcome(
        &self,
        session_id: &str,
    ) -> StorageResult<Option<RetainedUnsuccessfulSessionResource>> {
        if !is_valid_uuid_v7(session_id) {
            return Err(StorageError::invalid_input(
                "invalid_session_id",
                "session_id must be a canonical lowercase UUIDv7",
            ));
        }
        self.receipts
            .get_unsuccessful(session_id, self.volume.is_mounted_and_writable())
    }

    /// Gracefully close the coordinator (`Closing` terminal phase), waiting for any in-flight
    /// `Starting`, `Stopping`, `Recovering`, or `Deleting` work before releasing `_maintenance_lock`.
    pub fn close(&self) -> StorageResult<()> {
        let active_ctx = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase == CapturePhase::Closing {
                return Ok(());
            }
            let finish_deadline = Instant::now() + Duration::from_secs(30);
            while matches!(
                guard.phase,
                CapturePhase::Starting
                    | CapturePhase::Stopping
                    | CapturePhase::Recovering
                    | CapturePhase::Deleting
            ) || guard.safe_swap_in_progress
                || guard.volume_admission_in_progress
            {
                let now = Instant::now();
                if now >= finish_deadline {
                    return Err(guard.busy_error("close coordinator"));
                }
                let (next_guard, _) = self
                    .phase_cv
                    .wait_timeout(guard, finish_deadline.saturating_duration_since(now))
                    .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
                guard = next_guard;
            }
            if guard.phase == CapturePhase::Closing {
                return Ok(());
            }
            guard.transition(CapturePhase::Closing)?;
            self.phase_cv.notify_all();
            guard.active.take()
        };
        if let Some(mut ctx) = active_ctx
            && let Some(handle) = ctx.handle.take()
        {
            handle.abort();
        }
        Ok(())
    }

    pub(super) fn fail_start_attempt(
        &self,
        session_id: &str,
        take_id: &str,
        display_name: &str,
        admission: &VolumeAdmission,
        error_code: &str,
        error_message: &str,
    ) {
        let diag = RecordingDiagnostic {
            code: sanitize_diag_code(error_code),
            severity: "error".to_owned(),
            message: sanitize_diag_message(error_message),
            at: now_rfc3339_utc(),
            recoverable: false,
            details: None,
        };
        let (epoch, rev, outcome, released_ctx) = {
            let mut guard = match self.state.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            let released = if guard
                .active
                .as_ref()
                .is_some_and(|c| c.session_id == session_id)
            {
                let r = guard.active.take();
                let _ = guard.transition(CapturePhase::Idle);
                self.phase_cv.notify_all();
                r
            } else {
                None
            };
            let rev = guard.source_revision;
            let epoch = guard.authority_epoch.clone();
            let rec = CaptureSnapshotRecording {
                generation_id: admission.generation_id.clone(),
                recording_state: RecordingState {
                    schema: RECORDING_STATE_SCHEMA.to_owned(),
                    state: "failed".to_owned(),
                    authority_epoch: epoch.clone(),
                    state_revision: rev,
                    updated_at: now_rfc3339_utc(),
                    session_id: session_id.to_owned(),
                    take_id: take_id.to_owned(),
                    display_name: display_name.to_owned(),
                    device: self.config.device_identity(),
                    storage: RecordingStateStorage {
                        volume_id: admission.volume_id.clone(),
                        status: "mounted".to_owned(),
                        writable: admission.writable,
                        remaining_bytes: Some(admission.available_bytes),
                    },
                    progress: RecordingStateProgress {
                        elapsed_seconds: 0.0,
                        captured_frames: 0,
                        bytes_written: 0,
                        verification: None,
                    },
                    diagnostics: vec![diag.clone()],
                },
            };
            guard.retained_unsuccessful = Some(rec.clone());
            (epoch, rev, rec, released)
        };
        drop(released_ctx);

        self.receipts.record_unsuccessful(
            RetainedUnsuccessfulSessionResource {
                schema: RETAINED_UNSUCCESSFUL_SCHEMA.to_owned(),
                authority_epoch: epoch.clone(),
                source_revision: rev,
                outcome,
            },
            self.volume.is_mounted_and_writable(),
        );
        self.events.emit(
            &epoch,
            rev,
            CaptureEventType::Diagnostic,
            Some(session_id.to_owned()),
            json!({
                "schema": CAPTURE_DIAGNOSTIC_EVENT_SCHEMA,
                "diagnostic": diag,
            }),
        );
    }

    pub(super) fn record_unsuccessful_stop(
        &self,
        ctx: &SessionMetadata,
        progress: CaptureProgressSnapshot,
        terminal_state: &str,
        error_code: &str,
        error_message: &str,
        media_lost: bool,
    ) -> CaptureStatusSnapshot {
        let volume_writable = !media_lost && self.volume.is_mounted_and_writable();
        if volume_writable && terminal_state != "recoverable" && ctx.staging_dir.exists() {
            let base_name = ctx
                .staging_dir
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(&ctx.session_id);
            let _ = quarantine_session_directory(self.volume.paths(), &ctx.staging_dir, base_name);
        }

        let safe_msg = sanitize_diag_message(error_message);
        let diag = RecordingDiagnostic {
            code: if media_lost {
                "media_lost".to_owned()
            } else {
                sanitize_diag_code(error_code)
            },
            severity: "error".to_owned(),
            message: safe_msg.clone(),
            at: now_rfc3339_utc(),
            recoverable: terminal_state == "recoverable",
            details: None,
        };

        let runtime_json = self.backend.runtime_observation().to_openapi_runtime_json();
        let (epoch, rev, outcome, status_snapshot, released_ctx) = {
            let mut guard = match self.state.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            let released = guard.active.take();
            let target_phase = if media_lost {
                guard.last_failure_reason = Some(safe_msg);
                guard.saw_unmount_since_release = true;
                CapturePhase::Failed
            } else {
                CapturePhase::Idle
            };
            let _ = guard.transition(target_phase);
            self.phase_cv.notify_all();
            let rev = guard.source_revision;
            let epoch = guard.authority_epoch.clone();
            let mut diags = ctx.diagnostics.clone();
            diags.push(diag.clone());

            let rec = CaptureSnapshotRecording {
                generation_id: ctx.admission.generation_id.clone(),
                recording_state: RecordingState {
                    schema: RECORDING_STATE_SCHEMA.to_owned(),
                    state: terminal_state.to_owned(),
                    authority_epoch: epoch.clone(),
                    state_revision: rev,
                    updated_at: now_rfc3339_utc(),
                    session_id: ctx.session_id.clone(),
                    take_id: ctx.take_id.clone(),
                    display_name: ctx.display_name.clone(),
                    device: self.config.device_identity(),
                    storage: RecordingStateStorage {
                        volume_id: ctx.admission.volume_id.clone(),
                        status: if media_lost {
                            "media_lost".to_owned()
                        } else {
                            "mounted".to_owned()
                        },
                        writable: !media_lost && ctx.admission.writable,
                        remaining_bytes: if media_lost {
                            None
                        } else {
                            Some(ctx.admission.available_bytes)
                        },
                    },
                    progress: RecordingStateProgress {
                        elapsed_seconds: progress.elapsed_seconds.max(0.0),
                        captured_frames: progress.captured_frames,
                        bytes_written: progress.bytes_written,
                        verification: None,
                    },
                    diagnostics: diags,
                },
            };
            if !media_lost {
                guard.retained_unsuccessful = Some(rec.clone());
            } else {
                guard.retained_unsuccessful = None;
            }
            let snap = self.build_status_snapshot_locked(&guard, runtime_json);
            (epoch, rev, rec, snap, released)
        };
        drop(released_ctx);

        self.receipts.record_unsuccessful(
            RetainedUnsuccessfulSessionResource {
                schema: RETAINED_UNSUCCESSFUL_SCHEMA.to_owned(),
                authority_epoch: epoch.clone(),
                source_revision: rev,
                outcome,
            },
            volume_writable,
        );

        self.events.emit(
            &epoch,
            rev,
            CaptureEventType::Diagnostic,
            Some(ctx.session_id.clone()),
            json!({
                "schema": CAPTURE_DIAGNOSTIC_EVENT_SCHEMA,
                "diagnostic": diag,
            }),
        );
        self.events.emit(
            &epoch,
            rev,
            CaptureEventType::State,
            Some(ctx.session_id.clone()),
            json!({
                "schema": CAPTURE_STATE_EVENT_SCHEMA,
                "state": terminal_state,
                "volume_id": ctx.admission.volume_id,
                "generation_id": ctx.admission.generation_id,
            }),
        );
        self.events.emit(
            &epoch,
            rev,
            CaptureEventType::Snapshot,
            None,
            serde_json::to_value(&status_snapshot.snapshot).unwrap_or(Value::Null),
        );
        status_snapshot
    }

    pub(super) fn build_status_snapshot_locked(
        &self,
        guard: &CoordinatorState,
        runtime_json: Value,
    ) -> CaptureStatusSnapshot {
        let (device_state, active_recording, retained_unsuccessful) = match guard.phase {
            CapturePhase::Starting | CapturePhase::Recording => {
                let active_rec = guard.active.as_ref().map(|ctx| {
                    let progress = ctx
                        .handle
                        .as_ref()
                        .map(|h| h.progress())
                        .unwrap_or_default();
                    let elapsed_seconds = if progress.elapsed_seconds > 0.0 {
                        progress.elapsed_seconds
                    } else {
                        monotonic_ns()
                            .unwrap_or(ctx.started_monotonic_ns)
                            .saturating_sub(ctx.started_monotonic_ns) as f64
                            / 1_000_000_000.0
                    };
                    CaptureSnapshotRecording {
                        generation_id: ctx.admission.generation_id.clone(),
                        recording_state: RecordingState {
                            schema: RECORDING_STATE_SCHEMA.to_owned(),
                            state: ctx.recording_stage.clone(),
                            authority_epoch: guard.authority_epoch.clone(),
                            state_revision: guard.source_revision,
                            updated_at: now_rfc3339_utc(),
                            session_id: ctx.session_id.clone(),
                            take_id: ctx.take_id.clone(),
                            display_name: ctx.display_name.clone(),
                            device: self.config.device_identity(),
                            storage: RecordingStateStorage {
                                volume_id: ctx.admission.volume_id.clone(),
                                status: "mounted".to_owned(),
                                writable: ctx.admission.writable,
                                remaining_bytes: Some(ctx.admission.available_bytes),
                            },
                            progress: RecordingStateProgress {
                                elapsed_seconds: elapsed_seconds.max(0.0),
                                captured_frames: progress.captured_frames,
                                bytes_written: progress.bytes_written,
                                verification: ctx.verification_progress,
                            },
                            diagnostics: ctx.diagnostics.clone(),
                        },
                    }
                });
                let state_str = if active_rec.is_some() {
                    "recording"
                } else {
                    "blocked"
                };
                (state_str.to_owned(), active_rec, None)
            }
            CapturePhase::Stopping => {
                let stage = guard
                    .active
                    .as_ref()
                    .map(|ctx| match ctx.recording_stage.as_str() {
                        "encoding" | "verifying" => ctx.recording_stage.clone(),
                        _ => "finalizing".to_owned(),
                    })
                    .unwrap_or_else(|| "finalizing".to_owned());
                let active_rec = guard.active.as_ref().map(|ctx| {
                    let progress = ctx.last_progress;
                    let elapsed_seconds = if progress.elapsed_seconds > 0.0 {
                        progress.elapsed_seconds
                    } else {
                        monotonic_ns()
                            .unwrap_or(ctx.started_monotonic_ns)
                            .saturating_sub(ctx.started_monotonic_ns) as f64
                            / 1_000_000_000.0
                    };
                    let verification = if stage == "verifying" {
                        Some(ctx.verification_progress.unwrap_or(VerificationCounter {
                            completed: 0,
                            total: 0,
                        }))
                    } else {
                        ctx.verification_progress
                    };
                    CaptureSnapshotRecording {
                        generation_id: ctx.admission.generation_id.clone(),
                        recording_state: RecordingState {
                            schema: RECORDING_STATE_SCHEMA.to_owned(),
                            state: stage.clone(),
                            authority_epoch: guard.authority_epoch.clone(),
                            state_revision: guard.source_revision,
                            updated_at: now_rfc3339_utc(),
                            session_id: ctx.session_id.clone(),
                            take_id: ctx.take_id.clone(),
                            display_name: ctx.display_name.clone(),
                            device: self.config.device_identity(),
                            storage: RecordingStateStorage {
                                volume_id: ctx.admission.volume_id.clone(),
                                status: "sealing".to_owned(),
                                writable: ctx.admission.writable,
                                remaining_bytes: Some(ctx.admission.available_bytes),
                            },
                            progress: RecordingStateProgress {
                                elapsed_seconds: elapsed_seconds.max(0.0),
                                captured_frames: progress.captured_frames,
                                bytes_written: progress.bytes_written,
                                verification,
                            },
                            diagnostics: ctx.diagnostics.clone(),
                        },
                    }
                });
                if active_rec.is_some() {
                    (stage, active_rec, None)
                } else {
                    ("blocked".to_owned(), None, None)
                }
            }
            CapturePhase::Recovering | CapturePhase::Failed | CapturePhase::Closing => {
                ("blocked".to_owned(), None, None)
            }
            CapturePhase::Idle | CapturePhase::Deleting => {
                ("idle".to_owned(), None, guard.retained_unsuccessful.clone())
            }
        };

        CaptureStatusSnapshot {
            schema: CAPTURE_STATUS_SCHEMA.to_owned(),
            authority_epoch: guard.authority_epoch.clone(),
            source_revision: guard.source_revision,
            snapshot: CaptureSnapshotEventData {
                schema: CAPTURE_SNAPSHOT_EVENT_SCHEMA.to_owned(),
                device_state,
                active_recording,
                retained_unsuccessful,
                runtime: runtime_json,
            },
        }
    }
}

/// Safely clean up an aborted pre-recording staging directory without invoking `fs::remove_dir_all`
/// on populated recording directories.
pub(super) fn cleanup_empty_staging_attempt(staging_dir: &Path) {
    if !staging_dir.exists() {
        return;
    }
    for marker in [
        "capture.json",
        "recording.json",
        "checkpoint.json",
        "logs/clock-calibration.json",
        "logs/camera-calibration.json",
    ] {
        let _ = fs::remove_file(staging_dir.join(marker));
    }
    let _ = sweep_stale_atomic_temps(staging_dir);
    let _ = fs::remove_dir(staging_dir.join("logs"));
    let _ = fs::remove_dir(staging_dir.join("video"));
    let _ = fs::remove_dir(staging_dir.join("audio"));
    if fs::remove_dir(staging_dir).is_err()
        && let Some(meta_dir) = staging_dir.parent().and_then(Path::parent)
    {
        let quarantine_root = meta_dir.join("quarantine");
        if fs::create_dir_all(&quarantine_root).is_ok()
            && let Some(name) = staging_dir.file_name().and_then(|n| n.to_str())
        {
            for ordinal in 1..=999_u32 {
                let candidate = if ordinal == 1 {
                    format!("{name}.aborted")
                } else {
                    format!("{name}.aborted_{ordinal:02}")
                };
                let dst = quarantine_root.join(candidate);
                if !dst.exists() && fs::rename(staging_dir, &dst).is_ok() {
                    break;
                }
            }
        }
    }
}
