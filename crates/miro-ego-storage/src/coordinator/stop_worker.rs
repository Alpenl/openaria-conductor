//! Synchronous `stopCapture` (including `Idle`-state `SafeSwap` and pre-release
//! `SafeSwapReceipt` persistence) plus non-blocking async `begin_stop_capture_for_principal`
//! background finalization worker.

use super::fsm::{CaptureCoordinator, SessionMetadata};
use super::manifest_builder::{count_captured_artifacts, sum_captured_artifact_bytes};
use super::receipts::InFlightRegistration;
use super::types::{
    ActiveCaptureHandle, CAPTURE_STATE_EVENT_SCHEMA, CAPTURE_STOP_SCHEMA, CaptureEventType,
    CapturePhase, CaptureProgressSnapshot, CaptureStatusSnapshot, CaptureStopRequest,
    IdempotentOutcome, SAFE_SWAP_RECEIPT_SCHEMA, SafeSwapReceipt, StopReason, VerificationCounter,
    request_fingerprint, validate_idempotency_key,
};
use crate::artifact_io::open_manifest;
use crate::recovery::is_transient_storage_error;
use crate::util::now_rfc3339_utc;
use crate::volume::VolumeAdmission;
use crate::{StorageError, StorageResult, VolumeManager};
use miro_ego_core::CoreError;
use miro_ego_core::ids::{is_valid_uuid_v7, new_uuid_v7};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::{Duration, Instant};

impl CaptureCoordinator {
    /// Idempotently stop, seal (zero-reread), and publish the active recording synchronously.
    ///
    /// Returns `Ok(IdempotentOutcome { value: None, .. })` when no session is active (`204`),
    /// and `Ok(IdempotentOutcome { value: Some(snapshot), .. })` (`202`) when finalizing or
    /// when finalization commits a terminal `sealed` or `failed` outcome.
    pub fn stop_capture(
        &self,
        req: CaptureStopRequest,
        idempotency_key: Option<&str>,
    ) -> StorageResult<IdempotentOutcome<Option<CaptureStatusSnapshot>>> {
        self.stop_capture_for_principal(req, idempotency_key, "")
    }

    /// Idempotently stop, seal (zero-reread), and publish the active recording scoped to `principal_id`.
    pub fn stop_capture_for_principal(
        &self,
        req: CaptureStopRequest,
        idempotency_key: Option<&str>,
        principal_id: &str,
    ) -> StorageResult<IdempotentOutcome<Option<CaptureStatusSnapshot>>> {
        if req.schema != CAPTURE_STOP_SCHEMA {
            return Err(StorageError::invalid_input(
                "invalid_request",
                format!("stop request schema must be {CAPTURE_STOP_SCHEMA}"),
            ));
        }

        let volume_writable_hint = self.volume.is_mounted_and_writable();
        let req_hash = request_fingerprint(&req)?;
        let mut inflight_guard = None;
        if let Some(key) = idempotency_key {
            validate_idempotency_key(key)?;
            if let Some(cached) = self.receipts.lookup_idempotency(
                principal_id,
                "stop_capture",
                key,
                &req_hash,
                volume_writable_hint,
            )? {
                let snap: Option<CaptureStatusSnapshot> =
                    serde_json::from_value(cached).map_err(|e| StorageError::Catalog {
                        message: format!("corrupt cached stop receipt: {e}"),
                    })?;
                return Ok(IdempotentOutcome {
                    value: snap,
                    replayed: true,
                });
            }
            match self
                .inflight
                .acquire(principal_id, "stop_capture", key, &req_hash)?
            {
                InFlightRegistration::Replayed(cached) => {
                    let snap: Option<CaptureStatusSnapshot> = serde_json::from_value(cached)
                        .map_err(|e| StorageError::Catalog {
                            message: format!("corrupt replayed stop receipt: {e}"),
                        })?;
                    return Ok(IdempotentOutcome {
                        value: snap,
                        replayed: true,
                    });
                }
                InFlightRegistration::Leader(leader) => {
                    if let Some(cached) = self.receipts.lookup_idempotency(
                        principal_id,
                        "stop_capture",
                        key,
                        &req_hash,
                        volume_writable_hint,
                    )? {
                        let snap: Option<CaptureStatusSnapshot> =
                            serde_json::from_value(cached.clone()).map_err(|e| {
                                StorageError::Catalog {
                                    message: format!("corrupt cached stop receipt: {e}"),
                                }
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
            self.execute_stop_capture_inner(&req, principal_id, idempotency_key, &req_hash);
        match exec_res {
            Ok(wrapped) => {
                if let Some(leader) = inflight_guard {
                    let json_val = serde_json::to_value(&wrapped).unwrap_or(Value::Null);
                    leader.succeed(json_val);
                }
                Ok(IdempotentOutcome {
                    value: wrapped,
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

    /// Non-blocking HTTP/GPIO `stopCapture` entry point:
    /// - For `StopReason::SafeSwap`, delegates to synchronous [`Self::stop_capture_for_principal`]
    ///   so open handles are drained before returning (`503 volume_busy` on timeout).
    /// - For `StopReason::User`, transitions atomically `Recording -> Stopping` (`finalizing`),
    ///   spawns a background sealing thread (`finalizing -> verifying -> idle`), and returns
    ///   `202 Accepted` (`Some(finalizing_snapshot)`) immediately without blocking HTTP callers.
    pub fn begin_stop_capture_for_principal(
        self: &Arc<Self>,
        req: CaptureStopRequest,
        idempotency_key: Option<&str>,
        principal_id: &str,
    ) -> StorageResult<IdempotentOutcome<Option<CaptureStatusSnapshot>>> {
        if req.schema != CAPTURE_STOP_SCHEMA {
            return Err(StorageError::invalid_input(
                "invalid_request",
                format!("stop request schema must be {CAPTURE_STOP_SCHEMA}"),
            ));
        }
        if req.reason == StopReason::SafeSwap {
            return self.stop_capture_for_principal(req, idempotency_key, principal_id);
        }

        let volume_writable_hint = self.volume.is_mounted_and_writable();
        let req_hash = request_fingerprint(&req)?;
        let mut inflight_guard = None;
        if let Some(key) = idempotency_key {
            validate_idempotency_key(key)?;
            if let Some(cached) = self.receipts.lookup_idempotency(
                principal_id,
                "stop_capture",
                key,
                &req_hash,
                volume_writable_hint,
            )? {
                let snap: Option<CaptureStatusSnapshot> =
                    serde_json::from_value(cached).map_err(|e| StorageError::Catalog {
                        message: format!("corrupt cached stop receipt: {e}"),
                    })?;
                return Ok(IdempotentOutcome {
                    value: snap,
                    replayed: true,
                });
            }
            match self
                .inflight
                .acquire(principal_id, "stop_capture", key, &req_hash)?
            {
                InFlightRegistration::Replayed(cached) => {
                    let snap: Option<CaptureStatusSnapshot> = serde_json::from_value(cached)
                        .map_err(|e| StorageError::Catalog {
                            message: format!("corrupt replayed stop receipt: {e}"),
                        })?;
                    return Ok(IdempotentOutcome {
                        value: snap,
                        replayed: true,
                    });
                }
                InFlightRegistration::Leader(leader) => {
                    if let Some(cached) = self.receipts.lookup_idempotency(
                        principal_id,
                        "stop_capture",
                        key,
                        &req_hash,
                        volume_writable_hint,
                    )? {
                        let snap: Option<CaptureStatusSnapshot> =
                            serde_json::from_value(cached.clone()).map_err(|e| {
                                StorageError::Catalog {
                                    message: format!("corrupt cached stop receipt: {e}"),
                                }
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

        type AsyncStopWorkerJob = (
            SessionMetadata,
            Box<dyn ActiveCaptureHandle>,
            CaptureProgressSnapshot,
            String,
            u64,
        );
        let runtime_json = self.backend.runtime_observation().to_openapi_runtime_json();
        let begin_res: StorageResult<(Option<CaptureStatusSnapshot>, Option<AsyncStopWorkerJob>)> =
            (|| {
                let mut guard = self
                    .state
                    .lock()
                    .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
                while guard.phase == CapturePhase::Starting {
                    let (next_guard, wait_res) = self
                        .phase_cv
                        .wait_timeout(guard, Duration::from_secs(10))
                        .map_err(|_| {
                            StorageError::Core(CoreError::poisoned("coordinator_state"))
                        })?;
                    guard = next_guard;
                    if wait_res.timed_out() {
                        break;
                    }
                }

                if matches!(
                    guard.phase,
                    CapturePhase::Idle | CapturePhase::Failed | CapturePhase::Deleting
                ) {
                    return Ok((None, None));
                }
                if guard.phase == CapturePhase::Stopping {
                    let snap = self.build_status_snapshot_locked(&guard, runtime_json);
                    return Ok((Some(snap), None));
                }
                if guard.phase != CapturePhase::Recording {
                    return Err(guard.busy_error("stop capture"));
                }

                let ctx = guard.active.as_mut().ok_or_else(|| {
                    StorageError::conflict("invalid_state", "missing active session context")
                })?;
                let handle = ctx.handle.take().ok_or_else(|| {
                    StorageError::conflict("invalid_state", "missing active capture backend handle")
                })?;
                let last_progress = handle.progress();
                ctx.recording_stage = "finalizing".to_owned();
                ctx.stop_reason = Some(StopReason::User);
                ctx.last_progress = last_progress;
                let meta = ctx.metadata_snapshot();
                guard.transition(CapturePhase::Stopping)?;
                self.phase_cv.notify_all();
                let snap = self.build_status_snapshot_locked(&guard, runtime_json);
                let epoch = guard.authority_epoch.clone();
                let rev = guard.source_revision;
                Ok((Some(snap), Some((meta, handle, last_progress, epoch, rev))))
            })();

        let (wrapped, worker_job) = match begin_res {
            Ok(pair) => pair,
            Err(err) => {
                if let Some(leader) = inflight_guard {
                    leader.fail(&err);
                }
                return Err(err);
            }
        };

        let json_val = serde_json::to_value(&wrapped).unwrap_or(Value::Null);
        if let Some(key) = idempotency_key {
            self.receipts.store_idempotency(
                principal_id,
                "stop_capture",
                key,
                &req_hash,
                json_val.clone(),
                self.volume.is_mounted_and_writable(),
            );
        }
        if let Some(leader) = inflight_guard {
            leader.succeed(json_val);
        }

        if let Some((meta, handle, last_progress, authority_epoch, stopping_rev)) = worker_job {
            self.events.emit(
                &authority_epoch,
                stopping_rev,
                CaptureEventType::State,
                Some(meta.session_id.clone()),
                json!({
                    "schema": CAPTURE_STATE_EVENT_SCHEMA,
                    "state": "finalizing",
                    "volume_id": meta.admission.volume_id,
                    "generation_id": meta.admission.generation_id,
                }),
            );
            let coord = Arc::clone(self);
            let spawn_res = thread::Builder::new()
                .name("miro-ego-stop-finalizer".to_owned())
                .spawn(move || {
                    let _ = coord.complete_stopping_session(
                        meta,
                        handle,
                        last_progress,
                        authority_epoch,
                        StopReason::User,
                        None,
                    );
                });
            if spawn_res.is_err() {
                // Fallback if OS refuses thread creation under extreme memory pressure:
                // we cannot recover `handle` because closure was dropped on error? No: `Builder::spawn`
                // drops the closure on Err, so let's ensure coordinator never stays stuck in `Stopping`!
                let mut guard = match self.state.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                if guard.phase == CapturePhase::Stopping {
                    let _ = guard.active.take();
                    let _ = guard.transition(CapturePhase::Idle);
                    self.phase_cv.notify_all();
                }
            }
        }

        Ok(IdempotentOutcome {
            value: wrapped,
            replayed: false,
        })
    }

    fn execute_stop_capture_inner(
        &self,
        req: &CaptureStopRequest,
        principal_id: &str,
        idempotency_key: Option<&str>,
        req_hash: &str,
    ) -> StorageResult<Option<CaptureStatusSnapshot>> {
        #[allow(clippy::large_enum_variant)]
        enum StopDecision {
            NoOpIdle {
                idle_safe_swap_admission: Option<(VolumeAdmission, String)>,
            },
            AlreadyStopping,
            StopActive {
                meta: SessionMetadata,
                handle: Box<dyn ActiveCaptureHandle>,
                final_progress: CaptureProgressSnapshot,
                authority_epoch: String,
                stopping_rev: u64,
            },
        }

        let decision = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            while guard.phase == CapturePhase::Starting {
                let (next_guard, wait_res) = self
                    .phase_cv
                    .wait_timeout(guard, Duration::from_secs(10))
                    .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
                guard = next_guard;
                if wait_res.timed_out() {
                    break;
                }
            }

            if guard.phase == CapturePhase::Deleting {
                if req.reason == StopReason::SafeSwap {
                    return Err(guard.busy_error("safe-swap volume during session deletion"));
                }
                StopDecision::NoOpIdle {
                    idle_safe_swap_admission: None,
                }
            } else if matches!(guard.phase, CapturePhase::Idle | CapturePhase::Failed) {
                let idle_safe_swap_admission = if req.reason == StopReason::SafeSwap
                    && guard.phase == CapturePhase::Idle
                    && !guard.volume_released
                    && !self.volume.is_volume_released()
                {
                    guard
                        .admission
                        .clone()
                        .map(|adm| (adm, guard.authority_epoch.clone()))
                } else {
                    None
                };
                StopDecision::NoOpIdle {
                    idle_safe_swap_admission,
                }
            } else if guard.phase == CapturePhase::Stopping {
                let in_flight_reason = guard.active.as_ref().and_then(|c| c.stop_reason);
                if req.reason == StopReason::SafeSwap
                    && in_flight_reason != Some(StopReason::SafeSwap)
                {
                    return Err(guard.busy_error("safe-swap while finalizing without safe_swap"));
                }
                StopDecision::AlreadyStopping
            } else if guard.phase != CapturePhase::Recording {
                return Err(guard.busy_error("stop capture"));
            } else {
                let ctx = guard.active.as_mut().ok_or_else(|| {
                    StorageError::conflict("invalid_state", "missing active session context")
                })?;
                let handle = ctx.handle.take().ok_or_else(|| {
                    StorageError::conflict("invalid_state", "missing active capture backend handle")
                })?;
                let last_progress = handle.progress();
                ctx.recording_stage = "finalizing".to_owned();
                ctx.stop_reason = Some(req.reason);
                ctx.last_progress = last_progress;
                let meta = ctx.metadata_snapshot();
                guard.transition(CapturePhase::Stopping)?;
                self.phase_cv.notify_all();
                StopDecision::StopActive {
                    meta,
                    handle,
                    final_progress: last_progress,
                    authority_epoch: guard.authority_epoch.clone(),
                    stopping_rev: guard.source_revision,
                }
            }
        };

        match decision {
            StopDecision::NoOpIdle {
                idle_safe_swap_admission: Some((adm, epoch)),
            } => {
                self.execute_idle_safe_swap(&adm, &epoch, principal_id, idempotency_key, req_hash)?;
                Ok(None)
            }
            StopDecision::NoOpIdle {
                idle_safe_swap_admission: None,
            } => {
                if let Some(key) = idempotency_key {
                    self.receipts.store_idempotency(
                        principal_id,
                        "stop_capture",
                        key,
                        req_hash,
                        Value::Null,
                        self.volume.is_mounted_and_writable(),
                    );
                }
                Ok(None)
            }
            StopDecision::AlreadyStopping => {
                let wrapped = Some(self.capture_status()?);
                if let Some(key) = idempotency_key
                    && let Ok(json_val) = serde_json::to_value(&wrapped)
                {
                    self.receipts.store_idempotency(
                        principal_id,
                        "stop_capture",
                        key,
                        req_hash,
                        json_val,
                        self.volume.is_mounted_and_writable(),
                    );
                }
                Ok(wrapped)
            }
            StopDecision::StopActive {
                meta,
                handle,
                final_progress,
                authority_epoch,
                stopping_rev,
            } => {
                self.events.emit(
                    &authority_epoch,
                    stopping_rev,
                    CaptureEventType::State,
                    Some(meta.session_id.clone()),
                    json!({
                        "schema": CAPTURE_STATE_EVENT_SCHEMA,
                        "state": "finalizing",
                        "volume_id": meta.admission.volume_id,
                        "generation_id": meta.admission.generation_id,
                    }),
                );
                let idem_ctx = idempotency_key.map(|k| (principal_id, k, req_hash));
                self.complete_stopping_session(
                    meta,
                    handle,
                    final_progress,
                    authority_epoch,
                    req.reason,
                    idem_ctx,
                )
            }
        }
    }

    fn execute_idle_safe_swap(
        &self,
        adm: &VolumeAdmission,
        authority_epoch: &str,
        principal_id: &str,
        idempotency_key: Option<&str>,
        req_hash: &str,
    ) -> StorageResult<()> {
        if !self.volume.is_raw_mounted_and_writable() {
            if let Some(key) = idempotency_key {
                self.receipts.store_idempotency(
                    principal_id,
                    "stop_capture",
                    key,
                    req_hash,
                    Value::Null,
                    false,
                );
            }
            return Ok(());
        }

        // Look up any latest sealed session on this volume BEFORE gating `/data` reads.
        let candidate_receipt = self.build_latest_session_safe_swap_receipt(adm);

        // Temporarily gate new `/data` opens and drain all active handles to 0.
        self.volume.set_volume_released(true);
        self.close_all_volume_handles_for_safe_swap();
        let drain_budget =
            Duration::from_millis(self.safe_swap_drain_timeout_ms.load(Ordering::Acquire));
        let drain_deadline = Instant::now() + drain_budget;
        while self.open_handle_count() > 0 && Instant::now() < drain_deadline {
            thread::sleep(Duration::from_millis(10));
            self.close_all_volume_handles_for_safe_swap();
        }
        let remaining = self.open_handle_count();
        if remaining > 0 {
            self.volume.set_volume_released(false);
            return Err(StorageError::service_unavailable(
                "volume_busy",
                format!(
                    "cannot release volume for safe swap: {remaining} open file handle(s) did not drain within timeout"
                ),
            ));
        }

        // Pre-release durable receipt commit while `/data` is still writable on disk!
        self.volume.set_volume_released(false);
        let _ = self.catalog.compact();
        let idem_tuple = idempotency_key.map(|k| (principal_id, k, req_hash, Value::Null));
        self.receipts.commit_safe_swap_bundle(
            idem_tuple,
            candidate_receipt.clone(),
            Some(&adm.volume_id),
            adm.writable,
        );
        self.close_all_volume_handles_for_safe_swap();
        self.volume.set_volume_released(true);

        let runtime_json = self.backend.runtime_observation().to_openapi_runtime_json();
        let (snap, rev) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            guard.volume_released = true;
            guard.released_mount_signature = Some((
                adm.volume_id.clone(),
                adm.device_id,
                adm.mount_identity.clone(),
            ));
            guard.saw_unmount_since_release = false;
            let rev = guard.bump_revision();
            let snap = self.build_status_snapshot_locked(&guard, runtime_json);
            (snap, rev)
        };

        if let Some(receipt) = candidate_receipt {
            let sid = receipt.session_id.clone();
            self.events.emit(
                authority_epoch,
                rev,
                CaptureEventType::SafeSwap,
                Some(sid),
                serde_json::to_value(&receipt).unwrap_or(Value::Null),
            );
        }
        self.events.emit(
            authority_epoch,
            rev,
            CaptureEventType::Snapshot,
            None,
            serde_json::to_value(&snap.snapshot).unwrap_or(Value::Null),
        );
        Ok(())
    }

    fn build_latest_session_safe_swap_receipt(
        &self,
        adm: &VolumeAdmission,
    ) -> Option<SafeSwapReceipt> {
        let page = self
            .catalog
            .list_sessions_page(Some(1), None, true, None)
            .ok()?;
        let latest = page.items.first()?;
        let indexed = self
            .catalog
            .get_indexed_session(&latest.session_id)
            .ok()??;
        let opened = open_manifest(
            &self.volume.paths().recordings_dir(),
            &indexed.relative_dir,
            Some(&indexed.session_id),
        )
        .ok()?;
        let manifest_vid = opened.parsed.get("volume_id").and_then(Value::as_str)?;
        if manifest_vid != adm.volume_id {
            return None;
        }
        let manifest_id = opened
            .parsed
            .get("manifest_id")
            .and_then(Value::as_str)
            .filter(|id| is_valid_uuid_v7(id))
            .map(str::to_owned)
            .unwrap_or_else(new_uuid_v7);
        let sealed_at = opened
            .parsed
            .get("sealed_at")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| indexed.ended_at.clone());
        Some(SafeSwapReceipt {
            schema: SAFE_SWAP_RECEIPT_SCHEMA.to_owned(),
            session_id: indexed.session_id,
            volume_id: adm.volume_id.clone(),
            generation_id: adm.generation_id.clone(),
            manifest_id,
            manifest_sha256: opened.sha256,
            sealed_at,
            released_at: now_rfc3339_utc(),
            release_state: "device-released".to_owned(),
            open_handle_count: 0,
        })
    }

    pub(super) fn complete_stopping_session(
        &self,
        meta: SessionMetadata,
        handle: Box<dyn ActiveCaptureHandle>,
        final_progress: CaptureProgressSnapshot,
        authority_epoch: String,
        stop_reason: StopReason,
        idem_ctx: Option<(&str, &str, &str)>,
    ) -> StorageResult<Option<CaptureStatusSnapshot>> {
        let session_id = meta.session_id.clone();
        let volume_id = meta.admission.volume_id.clone();
        let generation_id = meta.admission.generation_id.clone();

        if let Some((bg_code, bg_msg)) = handle.take_failure() {
            handle.abort();
            let media_lost = !self.volume.is_mounted_and_writable();
            let failed_snap = self.record_unsuccessful_stop(
                &meta,
                final_progress,
                "failed",
                &bg_code,
                &bg_msg,
                media_lost,
            );
            let wrapped = Some(failed_snap);
            if let Some((principal_id, key, req_hash)) = idem_ctx
                && let Ok(json_val) = serde_json::to_value(&wrapped)
            {
                self.receipts.store_idempotency(
                    principal_id,
                    "stop_capture",
                    key,
                    req_hash,
                    json_val,
                    !media_lost && self.volume.is_mounted_and_writable(),
                );
            }
            return Ok(wrapped);
        }

        let captured = match handle.finish(stop_reason) {
            Ok(artifacts) => artifacts,
            Err(err) => {
                let media_lost = !self.volume.is_mounted_and_writable();
                let terminal_state =
                    if !media_lost && is_transient_storage_error(&err) && meta.staging_dir.is_dir()
                    {
                        "recoverable"
                    } else {
                        "failed"
                    };
                let failed_snap = self.record_unsuccessful_stop(
                    &meta,
                    final_progress,
                    terminal_state,
                    err.code(),
                    &err.to_string(),
                    media_lost,
                );
                let wrapped = Some(failed_snap);
                if let Some((principal_id, key, req_hash)) = idem_ctx
                    && let Ok(json_val) = serde_json::to_value(&wrapped)
                {
                    self.receipts.store_idempotency(
                        principal_id,
                        "stop_capture",
                        key,
                        req_hash,
                        json_val,
                        !media_lost && self.volume.is_mounted_and_writable(),
                    );
                }
                return Ok(wrapped);
            }
        };

        let total_artifacts = count_captured_artifacts(&captured, &meta.extra_logs);
        let captured_bytes = sum_captured_artifact_bytes(&captured, &meta.extra_logs);
        let verify_progress = CaptureProgressSnapshot {
            elapsed_seconds: captured
                .duration_seconds
                .max(final_progress.elapsed_seconds)
                .max(0.0),
            captured_frames: captured.frames_count.max(final_progress.captured_frames),
            bytes_written: final_progress.bytes_written.max(captured_bytes),
        };
        let verify_rev = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if let Some(ctx) = guard.active.as_mut() {
                ctx.recording_stage = "verifying".to_owned();
                ctx.last_progress = verify_progress;
                ctx.verification_progress = Some(VerificationCounter {
                    completed: 0,
                    total: total_artifacts,
                });
            }
            guard.bump_revision()
        };
        self.events.emit(
            &authority_epoch,
            verify_rev,
            CaptureEventType::State,
            Some(session_id.clone()),
            json!({
                "schema": CAPTURE_STATE_EVENT_SCHEMA,
                "state": "verifying",
                "volume_id": volume_id,
                "generation_id": generation_id,
            }),
        );

        let sealed = match self.seal_and_publish(&meta, &captured) {
            Ok(s) => s,
            Err(err) => {
                let media_lost = !self.volume.is_mounted_and_writable();
                let terminal_state =
                    if !media_lost && is_transient_storage_error(&err) && meta.staging_dir.is_dir()
                    {
                        "recoverable"
                    } else {
                        "failed"
                    };
                let failed_snap = self.record_unsuccessful_stop(
                    &meta,
                    verify_progress,
                    terminal_state,
                    err.code(),
                    &err.to_string(),
                    media_lost,
                );
                let wrapped = Some(failed_snap);
                if let Some((principal_id, key, req_hash)) = idem_ctx
                    && let Ok(json_val) = serde_json::to_value(&wrapped)
                {
                    self.receipts.store_idempotency(
                        principal_id,
                        "stop_capture",
                        key,
                        req_hash,
                        json_val,
                        !media_lost && self.volume.is_mounted_and_writable(),
                    );
                }
                return Ok(wrapped);
            }
        };

        let admission_writable = meta.admission.writable;
        let released_lock = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            guard
                .active
                .as_mut()
                .and_then(|ctx| ctx._maintenance_lock.take())
        };
        drop(released_lock);

        let released_ctx = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            guard.active.take()
        };
        drop(released_ctx);

        if stop_reason == StopReason::SafeSwap {
            self.volume.set_volume_released(true);
            self.close_all_volume_handles_for_safe_swap();
            let drain_budget =
                Duration::from_millis(self.safe_swap_drain_timeout_ms.load(Ordering::Acquire));
            let drain_deadline = Instant::now() + drain_budget;
            while self.open_handle_count() > 0 && Instant::now() < drain_deadline {
                thread::sleep(Duration::from_millis(10));
                self.close_all_volume_handles_for_safe_swap();
            }
            let remaining = self.open_handle_count();
            if remaining > 0 {
                self.volume.set_volume_released(false);
                let runtime_json = self.backend.runtime_observation().to_openapi_runtime_json();
                let (idle_snap, idle_rev) = {
                    let mut guard = self.state.lock().map_err(|_| {
                        StorageError::Core(CoreError::poisoned("coordinator_state"))
                    })?;
                    guard.volume_released = false;
                    guard.released_mount_signature = None;
                    guard.saw_unmount_since_release = false;
                    guard.transition(CapturePhase::Idle)?;
                    self.phase_cv.notify_all();
                    let snap = self.build_status_snapshot_locked(&guard, runtime_json);
                    (snap, guard.source_revision)
                };
                self.events.emit(
                    &authority_epoch,
                    idle_rev,
                    CaptureEventType::Snapshot,
                    None,
                    serde_json::to_value(&idle_snap.snapshot).unwrap_or(Value::Null),
                );
                return Err(StorageError::service_unavailable(
                    "volume_busy",
                    format!(
                        "cannot release volume for safe swap: {remaining} open file handle(s) did not drain within timeout"
                    ),
                ));
            }
        }

        let runtime_json = self.backend.runtime_observation().to_openapi_runtime_json();
        let (final_snapshot, done_rev) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if stop_reason == StopReason::SafeSwap {
                guard.volume_released = true;
                guard.released_mount_signature = Some((
                    meta.admission.volume_id.clone(),
                    meta.admission.device_id,
                    meta.admission.mount_identity.clone(),
                ));
                guard.saw_unmount_since_release = false;
            }
            guard.transition(CapturePhase::Idle)?;
            self.phase_cv.notify_all();
            let snap = self.build_status_snapshot_locked(&guard, runtime_json);
            (snap, guard.source_revision)
        };

        let wrapped = Some(final_snapshot);

        let safe_swap_receipt = if stop_reason == StopReason::SafeSwap {
            // Persist SafeSwapReceipt + compacted catalog BEFORE marking volume released!
            self.volume.set_volume_released(false);
            let _ = self.catalog.compact();
            let receipt = SafeSwapReceipt {
                schema: SAFE_SWAP_RECEIPT_SCHEMA.to_owned(),
                session_id: session_id.clone(),
                volume_id: volume_id.clone(),
                generation_id: generation_id.clone(),
                manifest_id: sealed.manifest_id,
                manifest_sha256: sealed.manifest_sha256,
                sealed_at: sealed.sealed_at,
                released_at: now_rfc3339_utc(),
                release_state: "device-released".to_owned(),
                open_handle_count: 0,
            };
            let idem_json = serde_json::to_value(&wrapped).unwrap_or(Value::Null);
            let idem_tuple =
                idem_ctx.map(|(principal_id, k, req_hash)| (principal_id, k, req_hash, idem_json));
            self.receipts.commit_safe_swap_bundle(
                idem_tuple,
                Some(receipt.clone()),
                Some(&volume_id),
                admission_writable,
            );
            self.close_all_volume_handles_for_safe_swap();
            self.volume.set_volume_released(true);
            Some(receipt)
        } else {
            if let Some((principal_id, key, req_hash)) = idem_ctx
                && let Ok(json_val) = serde_json::to_value(&wrapped)
            {
                self.receipts.store_idempotency(
                    principal_id,
                    "stop_capture",
                    key,
                    req_hash,
                    json_val,
                    admission_writable,
                );
            }
            None
        };

        if let Some(receipt) = safe_swap_receipt {
            self.events.emit(
                &authority_epoch,
                done_rev,
                CaptureEventType::SafeSwap,
                Some(session_id),
                serde_json::to_value(&receipt).unwrap_or(Value::Null),
            );
        }
        if let Some(ref snap) = wrapped {
            self.events.emit(
                &authority_epoch,
                done_rev,
                CaptureEventType::Snapshot,
                None,
                serde_json::to_value(&snap.snapshot).unwrap_or(Value::Null),
            );
        }

        Ok(wrapped)
    }
}
