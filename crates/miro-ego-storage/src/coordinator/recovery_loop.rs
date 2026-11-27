//! Startup crash recovery, mid-delete `PendingDeleteIntent` crash replay, 1 Hz reserve
//! enforcement, idle `AuditLogger` descriptor eviction, and bounded exponential recovery
//! backoff (preventing 1 Hz `Failed <-> Recovering` thrash on persistent volume faults).

use super::fsm::{CaptureCoordinator, CoordinatorState};
use super::types::{
    CAPTURE_DIAGNOSTIC_EVENT_SCHEMA, CAPTURE_STATE_EVENT_SCHEMA, CaptureEventType, CapturePhase,
    CaptureStopRequest, DeleteSessionsResult, FailedDeleteSession, ReserveEnforcementOutcome,
    SESSION_DELETE_RESULT_SCHEMA,
};
use crate::recovery::StagingRecoveryManager;
use crate::util::{is_canonical_session_relative_dir, now_rfc3339_utc};
use crate::volume::VolumeAdmission;
use crate::{RecoveryManager, RecoveryReport, StorageError, StorageResult, VolumeManager};
use miro_ego_core::CoreError;
use miro_ego_core::ids::is_valid_uuid_v7;
use miro_ego_core::sys::clock::monotonic_ns;
use miro_ego_core::sys::fs::{fstat, fsync_directory, open_directory_safe, read_bounded_json};
use serde_json::{Value, json};
use std::fs;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

const RECOVERY_BACKOFF_BASE_SECS: u64 = 2;
const RECOVERY_BACKOFF_MAX_SECS: u64 = 30;

struct VolumeAdmissionBusyGuard<'a> {
    state: &'a Mutex<CoordinatorState>,
    cv: &'a Condvar,
    active: bool,
}

impl<'a> VolumeAdmissionBusyGuard<'a> {
    fn new(state: &'a Mutex<CoordinatorState>, cv: &'a Condvar) -> Self {
        Self {
            state,
            cv,
            active: true,
        }
    }

    fn release_now(&mut self) {
        if self.active {
            self.active = false;
            if let Ok(mut guard) = self.state.lock() {
                guard.volume_admission_in_progress = false;
                self.cv.notify_all();
            } else if let Err(poisoned) = self.state.lock() {
                let mut guard = poisoned.into_inner();
                guard.volume_admission_in_progress = false;
                self.cv.notify_all();
            }
        }
    }

    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for VolumeAdmissionBusyGuard<'_> {
    fn drop(&mut self) {
        self.release_now();
    }
}

impl CaptureCoordinator {
    /// Complete any interrupted `PendingDeleteIntent` left behind by power loss mid-batch-delete.
    fn finish_pending_delete_intent(&self) {
        let writable = self.volume.is_mounted_and_writable();
        if !writable {
            return;
        }
        let Some(intent) = self.receipts.peek_pending_delete_intent(writable) else {
            return;
        };
        let recordings_root = self.volume.paths().recordings_dir();
        let deleting_root = self.volume.paths().deleting_dir();
        let root_dev = match open_directory_safe(&recordings_root) {
            Ok(dir) => match fstat(dir.as_raw_fd()) {
                Ok(st) => st.st_dev,
                Err(_) => return,
            },
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                let Ok(del_dir) = open_directory_safe(&deleting_root) else {
                    return;
                };
                match fstat(del_dir.as_raw_fd()) {
                    Ok(st) => st.st_dev,
                    Err(_) => return,
                }
            }
            Err(_) => return,
        };

        let mut deleted_ids = Vec::with_capacity(intent.targets.len());
        let mut forgotten_rels = Vec::with_capacity(intent.targets.len());
        let mut failed_sessions = Vec::new();
        for (sid, rel) in intent.targets {
            if !is_valid_uuid_v7(&sid) || !is_canonical_session_relative_dir(&rel) {
                failed_sessions.push(FailedDeleteSession {
                    session_id: sid,
                    error: "refusing to delete invalid pending delete target".to_owned(),
                });
                continue;
            }
            let rec_dir = recordings_root.join(&rel);
            if let Some(parent) = rec_dir.parent()
                && let Ok(pmeta) = fs::symlink_metadata(parent)
                && (pmeta.file_type().is_symlink() || !pmeta.is_dir() || pmeta.dev() != root_dev)
            {
                failed_sessions.push(FailedDeleteSession {
                    session_id: sid,
                    error: "parent date directory failed symlink or st_dev check".to_owned(),
                });
                continue;
            }
            let rec_removed: bool;
            match fs::symlink_metadata(&rec_dir) {
                Ok(meta) => {
                    if meta.file_type().is_symlink() || !meta.is_dir() || meta.dev() != root_dev {
                        failed_sessions.push(FailedDeleteSession {
                            session_id: sid,
                            error: "session directory failed symlink or st_dev check".to_owned(),
                        });
                        continue;
                    }
                    match fs::remove_dir_all(&rec_dir) {
                        Ok(()) => {
                            rec_removed = true;
                            if let Some(parent) = rec_dir.parent()
                                && parent != recordings_root
                            {
                                let _ = fs::remove_dir(parent);
                            }
                        }
                        Err(err) if err.kind() == io::ErrorKind::NotFound => {
                            rec_removed = true;
                        }
                        Err(err) => {
                            failed_sessions.push(FailedDeleteSession {
                                session_id: sid,
                                error: format!("failed to remove session directory: {err}"),
                            });
                            continue;
                        }
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    rec_removed = true;
                }
                Err(err) => {
                    failed_sessions.push(FailedDeleteSession {
                        session_id: sid,
                        error: format!("failed to stat session directory: {err}"),
                    });
                    continue;
                }
            }

            let trash_dir = deleting_root.join(&sid);
            match fs::symlink_metadata(&trash_dir) {
                Ok(tmeta) => {
                    if tmeta.file_type().is_symlink() || !tmeta.is_dir() || tmeta.dev() != root_dev
                    {
                        if rec_removed {
                            forgotten_rels.push(rel);
                        }
                        failed_sessions.push(FailedDeleteSession {
                            session_id: sid,
                            error: "staged trash directory failed symlink or st_dev check"
                                .to_owned(),
                        });
                        continue;
                    }
                    match fs::remove_dir_all(&trash_dir) {
                        Ok(()) => {}
                        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                        Err(err) => {
                            if rec_removed {
                                forgotten_rels.push(rel);
                            }
                            failed_sessions.push(FailedDeleteSession {
                                session_id: sid,
                                error: format!("failed to remove staged trash directory: {err}"),
                            });
                            continue;
                        }
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => {
                    if rec_removed {
                        forgotten_rels.push(rel);
                    }
                    failed_sessions.push(FailedDeleteSession {
                        session_id: sid,
                        error: format!("failed to stat staged trash directory: {err}"),
                    });
                    continue;
                }
            }

            forgotten_rels.push(rel);
            deleted_ids.push(sid);
        }
        if recordings_root.is_dir() {
            let _ = fsync_directory(&recordings_root);
        }
        if deleting_root.is_dir() {
            let _ = fsync_directory(&deleting_root);
        }
        if !forgotten_rels.is_empty() && self.catalog.forget_paths(&forgotten_rels).is_err() {
            let _ = self.catalog.forget_paths_in_memory(&forgotten_rels);
        }
        let res = DeleteSessionsResult {
            schema: SESSION_DELETE_RESULT_SCHEMA.to_owned(),
            deleted_session_ids: deleted_ids,
            failed_sessions,
        };
        if let Ok(json_val) = serde_json::to_value(&res) {
            self.receipts.commit_delete_sessions_bundle(
                &intent.principal_id,
                &intent.key,
                &intent.request_hash,
                json_val,
                writable,
            );
        }
    }

    /// Run crash prefix recovery on startup (`Idle -> Recovering -> Idle`).
    pub fn recover_on_startup(&self) -> StorageResult<RecoveryReport> {
        let maintenance_lock = self.acquire_shared_maintenance_lock()?;
        {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if (guard.phase != CapturePhase::Idle && guard.phase != CapturePhase::Failed)
                || guard.safe_swap_in_progress
                || guard.volume_admission_in_progress
            {
                return Err(guard.busy_error("run crash recovery"));
            }
            if guard.volume_released || self.volume.is_volume_released() {
                return Err(StorageError::conflict(
                    "volume_released",
                    "cannot run crash recovery while recording volume is released for safe swap",
                ));
            }
            guard.transition(CapturePhase::Recovering)?;
            self.phase_cv.notify_all();
        }

        self.receipts
            .ensure_loaded_from_disk(self.volume.is_mounted_and_writable());
        self.finish_pending_delete_intent();

        let recovery_mgr = StagingRecoveryManager::new(
            self.volume.paths().clone(),
            self.config.recovery_defaults(),
            Some(Arc::clone(&self.catalog)),
        );
        let result = recovery_mgr.recover_staging();

        let (epoch, rev) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            match &result {
                Ok(_) => {
                    guard.last_failure_reason = None;
                    guard.recovery_failure_streak = 0;
                    guard.next_recovery_retry_at = None;
                    let _ = guard.transition(CapturePhase::Idle);
                }
                Err(err) => {
                    guard.last_failure_reason = Some(err.to_string());
                    let streak = guard.recovery_failure_streak.saturating_add(1);
                    guard.recovery_failure_streak = streak;
                    let delay_secs = RECOVERY_BACKOFF_BASE_SECS
                        .saturating_mul(1_u64 << streak.saturating_sub(1).min(4))
                        .min(RECOVERY_BACKOFF_MAX_SECS);
                    guard.next_recovery_retry_at =
                        Some(Instant::now() + Duration::from_secs(delay_secs));
                    let _ = guard.transition(CapturePhase::Failed);
                }
            }
            self.phase_cv.notify_all();
            (guard.authority_epoch.clone(), guard.source_revision)
        };
        drop(maintenance_lock);

        if let Ok(ref report) = result
            && (report.recovered > 0 || report.quarantined > 0)
        {
            self.events.emit(
                &epoch,
                rev,
                CaptureEventType::Diagnostic,
                None,
                json!({
                    "schema": CAPTURE_DIAGNOSTIC_EVENT_SCHEMA,
                    "diagnostic": {
                        "code": "staging_recovery_completed",
                        "severity": "info",
                        "message": format!(
                            "scanned {} interrupted captures: {} recovered, {} quarantined",
                            report.scanned, report.recovered, report.quarantined
                        ),
                        "at": now_rfc3339_utc(),
                        "recoverable": true,
                    }
                }),
            );
        }
        result
    }

    /// Explicitly recover the coordinator FSM from `Failed` back to `Idle` once faults clear.
    pub fn recover_from_failure(&self) -> StorageResult<CapturePhase> {
        let (prev_adm, prev_vid, was_released) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase != CapturePhase::Failed {
                return Ok(guard.phase);
            }
            if guard.safe_swap_in_progress || guard.volume_admission_in_progress {
                return Err(guard.busy_error("recover from failure"));
            }
            guard.volume_admission_in_progress = true;
            guard.recovery_failure_streak = 0;
            guard.next_recovery_retry_at = None;
            guard.saw_unmount_since_release = false;
            self.phase_cv.notify_all();
            (
                guard.admission.clone(),
                guard.active_volume_id.clone(),
                guard.volume_released,
            )
        };
        let mut busy_guard = VolumeAdmissionBusyGuard::new(&self.state, &self.phase_cv);

        self.volume.set_volume_released(false);
        let prev_binding = if was_released {
            None
        } else {
            prev_adm.as_ref().map(|p| {
                (
                    p.volume_id.as_str(),
                    p.device_id,
                    p.mount_identity.as_str(),
                    p.generation_id.as_str(),
                )
            })
        };
        let admitted = self.volume.admit(prev_binding).ok();
        let writable = admitted.as_ref().is_some_and(|a| a.writable);
        let new_vid = admitted.as_ref().map(|a| a.volume_id.clone());
        let volume_changed = match (&prev_vid, &new_vid) {
            (Some(old_v), Some(new_v)) => old_v != new_v,
            _ => false,
        };
        if let Some(ref vid) = new_vid {
            self.receipts.bind_volume(Some(vid), writable);
        } else {
            self.receipts.ensure_loaded_from_disk(writable);
        }
        if admitted.is_some() {
            let _ = self.catalog.reconcile();
        }

        {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase != CapturePhase::Failed {
                return Ok(guard.phase);
            }
            guard.last_failure_reason = None;
            guard.volume_released = false;
            guard.released_mount_signature = None;
            guard.saw_unmount_since_release = false;
            guard.volume_admission_in_progress = false;
            busy_guard.disarm();
            if volume_changed {
                guard.retained_unsuccessful = None;
            }
            if new_vid.is_some() {
                guard.active_volume_id = new_vid;
            }
            guard.admission = admitted;
            guard.transition(CapturePhase::Idle)?;
            self.phase_cv.notify_all();
        }

        if writable {
            let _ = self.recover_on_startup();
        }
        Ok(CapturePhase::Idle)
    }

    /// Re-admit a remounted or newly inserted volume, isolating cross-card state, reconciling
    /// the catalog, running crash recovery when recovering from a released/swapped/failed volume,
    /// and atomically invalidating any prior safe-swap handoff receipt.
    pub fn refresh_volume_admission(&self) -> StorageResult<VolumeAdmission> {
        {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.is_busy_for_ops() || guard.phase == CapturePhase::Closing {
                return Err(guard.busy_error("refresh volume admission"));
            }
            guard.recovery_failure_streak = 0;
            guard.next_recovery_retry_at = None;
        }
        self.reconcile_and_admit_volume(false)
    }

    fn reconcile_and_admit_volume(
        &self,
        run_recovery_always: bool,
    ) -> StorageResult<VolumeAdmission> {
        let (prev, prev_vid, was_released, was_failed) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.is_busy_for_ops() || guard.phase == CapturePhase::Closing {
                return Err(guard.busy_error("refresh volume admission"));
            }
            guard.volume_admission_in_progress = true;
            guard.saw_unmount_since_release = false;
            self.phase_cv.notify_all();
            (
                guard.admission.clone(),
                guard.active_volume_id.clone(),
                guard.volume_released,
                guard.phase == CapturePhase::Failed,
            )
        };
        let mut busy_guard = VolumeAdmissionBusyGuard::new(&self.state, &self.phase_cv);

        self.volume.set_volume_released(false);
        let prev_binding = if was_released || was_failed {
            None
        } else {
            prev.as_ref().map(|p| {
                (
                    p.volume_id.as_str(),
                    p.device_id,
                    p.mount_identity.as_str(),
                    p.generation_id.as_str(),
                )
            })
        };
        let admitted = match self.volume.admit(prev_binding) {
            Ok(adm) => adm,
            Err(err) => {
                if was_released {
                    self.volume.set_volume_released(true);
                }
                if was_failed && let Ok(mut guard) = self.state.lock() {
                    let streak = guard.recovery_failure_streak.saturating_add(1);
                    guard.recovery_failure_streak = streak;
                    let delay_secs = RECOVERY_BACKOFF_BASE_SECS
                        .saturating_mul(1_u64 << streak.saturating_sub(1).min(4))
                        .min(RECOVERY_BACKOFF_MAX_SECS);
                    guard.next_recovery_retry_at =
                        Some(Instant::now() + Duration::from_secs(delay_secs));
                }
                return Err(err);
            }
        };

        let volume_changed = prev_vid
            .as_deref()
            .is_some_and(|old_vid| old_vid != admitted.volume_id);
        self.receipts
            .bind_volume(Some(&admitted.volume_id), admitted.writable);

        let generation_changed = was_released
            || was_failed
            || prev.as_ref().is_none_or(|p| {
                p.volume_id != admitted.volume_id || p.generation_id != admitted.generation_id
            });
        if generation_changed {
            self.receipts.set_safe_swap(None, admitted.writable);
        }
        if generation_changed || volume_changed {
            let _ = self.catalog.reconcile();
        }

        // Observe runtime strictly OUTSIDE `self.state.lock()` so slow backend/RPC calls
        // never block concurrent status/focus/FSM operations!
        let runtime_json = if was_failed {
            Some(self.backend.runtime_observation().to_openapi_runtime_json())
        } else {
            None
        };

        let snap_info = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase.is_busy() || guard.phase == CapturePhase::Closing {
                return Err(guard.busy_error("commit volume admission"));
            }
            if volume_changed {
                guard.retained_unsuccessful = None;
            }
            guard.active_volume_id = Some(admitted.volume_id.clone());
            guard.admission = Some(admitted.clone());
            guard.volume_released = false;
            guard.released_mount_signature = None;
            guard.saw_unmount_since_release = false;
            guard.last_failure_reason = None;
            guard.volume_admission_in_progress = false;
            busy_guard.disarm();
            if guard.phase == CapturePhase::Failed {
                guard.transition(CapturePhase::Idle)?;
                self.phase_cv.notify_all();
                let rt = runtime_json.unwrap_or_else(|| json!({}));
                let snap = self.build_status_snapshot_locked(&guard, rt);
                Some((guard.authority_epoch.clone(), guard.source_revision, snap))
            } else {
                self.phase_cv.notify_all();
                guard.bump_revision();
                None
            }
        };

        if let Some((epoch, rev, snap)) = snap_info {
            self.events.emit(
                &epoch,
                rev,
                CaptureEventType::Snapshot,
                None,
                serde_json::to_value(&snap.snapshot).unwrap_or(Value::Null),
            );
        }

        if admitted.writable && (run_recovery_always || volume_changed || was_failed) {
            let _ = self.recover_on_startup();
        }

        Ok(admitted)
    }

    /// Poll the active capture handle for asynchronous background failures (encoder/camera/sink
    /// crash), immediately aborting the handle, releasing `maintenance.lock`, quarantining any
    /// partial staging directory, transitioning to `Idle` (or `Failed` on `media_lost`), and
    /// committing `retained_unsuccessful` + SSE events.
    pub fn poll_active_failure(&self) -> StorageResult<bool> {
        let extracted = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase != CapturePhase::Recording {
                return Ok(false);
            }
            let failure = guard
                .active
                .as_ref()
                .and_then(|ctx| ctx.handle.as_ref())
                .and_then(|h| h.take_failure());
            let Some((code, msg)) = failure else {
                return Ok(false);
            };
            let ctx = guard.active.as_mut().ok_or_else(|| {
                StorageError::conflict("invalid_state", "missing active session context")
            })?;
            let handle = ctx.handle.take();
            let progress = handle
                .as_ref()
                .map(|h| h.progress())
                .unwrap_or(ctx.last_progress);
            ctx.recording_stage = "finalizing".to_owned();
            ctx.last_progress = progress;
            let meta = ctx.metadata_snapshot();
            guard.transition(CapturePhase::Stopping)?;
            self.phase_cv.notify_all();
            let stop_epoch = guard.authority_epoch.clone();
            let stop_rev = guard.source_revision;
            (meta, handle, progress, code, msg, stop_epoch, stop_rev)
        };

        let (meta, handle, progress, code, msg, stop_epoch, stop_rev) = extracted;
        self.events.emit(
            &stop_epoch,
            stop_rev,
            CaptureEventType::State,
            Some(meta.session_id.clone()),
            json!({
                "schema": CAPTURE_STATE_EVENT_SCHEMA,
                "state": "finalizing",
                "volume_id": meta.admission.volume_id,
                "generation_id": meta.admission.generation_id,
            }),
        );
        if let Some(h) = handle {
            h.abort();
        }
        let media_lost = !self.volume.is_mounted_and_writable();
        let _ = self.record_unsuccessful_stop(&meta, progress, "failed", &code, &msg, media_lost);
        Ok(true)
    }

    /// Check whether an active capture timer has expired (or an active background failure occurred)
    /// and trigger normal stop/seal if expired.
    pub fn poll_capture_timer(&self) -> StorageResult<bool> {
        if self.poll_active_failure()? {
            return Ok(false);
        }
        let expired = {
            let now_ns = monotonic_ns().unwrap_or(0);
            let guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            guard.phase == CapturePhase::Recording
                && guard
                    .active
                    .as_ref()
                    .and_then(|ctx| ctx.timer_deadline_ns)
                    .is_some_and(|deadline| now_ns >= deadline)
        };
        if !expired {
            return Ok(false);
        }
        let _ = self.stop_capture(CaptureStopRequest::user(), None)?;
        Ok(true)
    }

    /// Enforce storage reserve thresholds during active capture (refreshing cached
    /// `remaining_bytes` at 1 Hz), evict idle `AuditLogger` descriptors, and auto-recover
    /// from `Failed` / `volume_released` with bounded exponential backoff on persistent faults.
    pub fn enforce_storage_reserve(&self) -> StorageResult<ReserveEnforcementOutcome> {
        if let Ok(guard) = self.audit_logger.lock()
            && let Some(audit) = guard.as_ref()
        {
            audit.evict_idle_handle();
        }

        let _ = self.poll_active_failure()?;

        let (
            phase,
            volume_released,
            busy_for_ops,
            released_sig,
            saw_unmount,
            has_admission,
            active_session_id,
            next_retry_at,
        ) = {
            let guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            (
                guard.phase,
                guard.volume_released || self.volume.is_volume_released(),
                guard.is_busy_for_ops(),
                guard.released_mount_signature.clone(),
                guard.saw_unmount_since_release,
                guard.admission.is_some(),
                guard.active.as_ref().map(|a| a.session_id.clone()),
                guard.next_recovery_retry_at,
            )
        };

        if phase != CapturePhase::Recording {
            if !busy_for_ops && phase != CapturePhase::Closing {
                if phase == CapturePhase::Failed || volume_released {
                    match self.volume.probe_raw_volume() {
                        Some(cap) if cap.mounted && cap.writable && !cap.read_only => {
                            let disk_vid = read_bounded_json::<crate::volume::VolumeMetadata>(
                                &self.volume.paths().volume_metadata_file(),
                                8 * 1024,
                                8,
                            )
                            .ok()
                            .map(|m| m.volume_id);
                            let mount_changed = match &released_sig {
                                None => false,
                                Some((old_vid, old_dev, old_mnt)) => {
                                    cap.device_id != *old_dev
                                        || cap.mount_identity != *old_mnt
                                        || disk_vid.as_deref().is_some_and(|v| v != old_vid)
                                }
                            };
                            let backoff_elapsed =
                                next_retry_at.is_none_or(|deadline| Instant::now() >= deadline);
                            if (phase == CapturePhase::Failed
                                && (saw_unmount || backoff_elapsed || mount_changed))
                                || (volume_released && (saw_unmount || mount_changed))
                            {
                                let _ = self.reconcile_and_admit_volume(true);
                            }
                        }
                        _ => {
                            if let Ok(mut guard) = self.state.lock() {
                                guard.saw_unmount_since_release = true;
                                guard.recovery_failure_streak = 0;
                                guard.next_recovery_retry_at = None;
                            }
                        }
                    }
                } else if phase == CapturePhase::Idle
                    && !has_admission
                    && self.volume.is_raw_mounted_and_writable()
                {
                    let backoff_elapsed =
                        next_retry_at.is_none_or(|deadline| Instant::now() >= deadline);
                    if backoff_elapsed {
                        let _ = self.reconcile_and_admit_volume(true);
                    }
                }
            }
            return Ok(ReserveEnforcementOutcome::Healthy);
        }

        let Some(session_id) = active_session_id else {
            return Ok(ReserveEnforcementOutcome::Healthy);
        };

        let reserve = self.volume.check_reserve()?;
        if let Some(rec_budget) = &reserve.recording
            && let Ok(mut guard) = self.state.lock()
        {
            if let Some(ctx) = guard.active.as_mut() {
                ctx.admission.available_bytes = rec_budget.available_bytes;
            }
            if let Some(adm) = guard.admission.as_mut() {
                adm.available_bytes = rec_budget.available_bytes;
            }
        }
        if reserve.ok {
            return Ok(ReserveEnforcementOutcome::Healthy);
        }

        if !reserve.writable {
            let extracted = {
                let mut guard = self
                    .state
                    .lock()
                    .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
                if guard.phase != CapturePhase::Recording {
                    None
                } else if let Some(ctx) = guard.active.as_mut() {
                    let handle = ctx.handle.take();
                    let progress = handle
                        .as_ref()
                        .map(|h| h.progress())
                        .unwrap_or(ctx.last_progress);
                    ctx.recording_stage = "finalizing".to_owned();
                    ctx.last_progress = progress;
                    let meta = ctx.metadata_snapshot();
                    let _ = guard.transition(CapturePhase::Stopping);
                    self.phase_cv.notify_all();
                    Some((meta, handle, progress))
                } else {
                    None
                }
            };
            if let Some((meta, handle, progress)) = extracted {
                if let Some(h) = handle {
                    h.abort();
                }
                let _ = self.record_unsuccessful_stop(
                    &meta,
                    progress,
                    "failed",
                    "media_lost",
                    &reserve.message,
                    true,
                );
            }
            return Ok(ReserveEnforcementOutcome::FailedMediaLost {
                session_id,
                reason: reserve.message,
            });
        }

        let _ = self.stop_capture(CaptureStopRequest::user(), None)?;
        Ok(ReserveEnforcementOutcome::StoppedForReserve {
            session_id,
            reason: reserve.message,
        })
    }
}
