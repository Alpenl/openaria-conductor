//! Crash-atomic trash-staged `deleteSessions` (`recordings/<sid>` -> `.miro-ego/deleting/<sid>`)
//! with continuation DAG validation, single-batch `catalog.forget_paths` WAL commit, and
//! pre-unlink idempotency receipt persistence.

use super::fsm::CaptureCoordinator;
use super::receipts::{InFlightRegistration, PendingDeleteIntent};
use super::types::{
    CapturePhase, DeleteSessionsRequest, DeleteSessionsResult, FailedDeleteSession,
    IdempotentOutcome, MAX_DELETE_BATCH_SIZE, SESSION_DELETE_REQUEST_SCHEMA,
    SESSION_DELETE_RESULT_SCHEMA, request_fingerprint, validate_idempotency_key,
};
use crate::artifact_io::open_manifest;
use crate::catalog::IndexedSession;
use crate::util::{is_canonical_session_relative_dir, now_rfc3339_utc};
use crate::{StorageError, StorageResult, VolumeManager};
use miro_ego_core::CoreError;
use miro_ego_core::ids::{is_valid_sha256, is_valid_uuid_v7};
use miro_ego_core::sys::fs::{fstat, fsync_directory, open_directory_safe};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;

impl CaptureCoordinator {
    /// Read-only lookup of a previously persisted `delete_sessions` idempotency receipt
    /// (e.g. across daemon restarts) without mutating coordinator state or acquiring delete locks.
    pub fn lookup_delete_sessions_idempotency(
        &self,
        req: &DeleteSessionsRequest,
        idempotency_key: &str,
        principal_id: &str,
    ) -> StorageResult<Option<DeleteSessionsResult>> {
        validate_idempotency_key(idempotency_key)?;
        let volume_writable_hint = self.volume.is_mounted_and_writable();
        let req_hash = request_fingerprint(req)?;
        let Some(cached) = self.receipts.lookup_idempotency(
            principal_id,
            "delete_sessions",
            idempotency_key,
            &req_hash,
            volume_writable_hint,
        )?
        else {
            return Ok(None);
        };
        let result: DeleteSessionsResult =
            serde_json::from_value(cached).map_err(|e| StorageError::Catalog {
                message: format!("corrupt cached delete receipt: {e}"),
            })?;
        Ok(Some(result))
    }

    /// Batch-delete explicitly selected sealed recordings with idempotency, digest preflight,
    /// continuation dependency enforcement, active-capture refusal, and empty date-dir cleanup.
    pub fn delete_sessions(
        &self,
        req: DeleteSessionsRequest,
        idempotency_key: &str,
    ) -> StorageResult<IdempotentOutcome<DeleteSessionsResult>> {
        self.delete_sessions_for_principal(req, idempotency_key, "")
    }

    /// Batch-delete explicitly selected sealed recordings scoped to `principal_id`.
    pub fn delete_sessions_for_principal(
        &self,
        req: DeleteSessionsRequest,
        idempotency_key: &str,
        principal_id: &str,
    ) -> StorageResult<IdempotentOutcome<DeleteSessionsResult>> {
        validate_idempotency_key(idempotency_key)?;
        if req.schema != SESSION_DELETE_REQUEST_SCHEMA {
            return Err(StorageError::invalid_input(
                "invalid_request",
                format!("delete request schema must be {SESSION_DELETE_REQUEST_SCHEMA}"),
            ));
        }
        if req.sessions.is_empty() || req.sessions.len() > MAX_DELETE_BATCH_SIZE {
            return Err(StorageError::invalid_input(
                "invalid_request",
                format!("sessions list must contain 1..={MAX_DELETE_BATCH_SIZE} items"),
            ));
        }

        let mut requested_ids = HashSet::with_capacity(req.sessions.len());
        for item in &req.sessions {
            if !is_valid_uuid_v7(&item.session_id) || !is_valid_sha256(&item.manifest_sha256) {
                return Err(StorageError::invalid_input(
                    "invalid_request",
                    "delete target requires canonical UUIDv7 session_id and 64-char sha256",
                ));
            }
            if !requested_ids.insert(item.session_id.clone()) {
                return Err(StorageError::invalid_input(
                    "duplicate_session_target",
                    "duplicate session_id in delete request",
                ));
            }
        }

        let volume_writable_hint = self.volume.is_mounted_and_writable();
        let req_hash = request_fingerprint(&req)?;
        if let Some(cached) = self.receipts.lookup_idempotency(
            principal_id,
            "delete_sessions",
            idempotency_key,
            &req_hash,
            volume_writable_hint,
        )? {
            let result: DeleteSessionsResult =
                serde_json::from_value(cached).map_err(|e| StorageError::Catalog {
                    message: format!("corrupt cached delete receipt: {e}"),
                })?;
            return Ok(IdempotentOutcome {
                value: result,
                replayed: true,
            });
        }

        let inflight_leader = match self.inflight.acquire(
            principal_id,
            "delete_sessions",
            idempotency_key,
            &req_hash,
        )? {
            InFlightRegistration::Replayed(cached) => {
                let result: DeleteSessionsResult =
                    serde_json::from_value(cached).map_err(|e| StorageError::Catalog {
                        message: format!("corrupt replayed delete receipt: {e}"),
                    })?;
                return Ok(IdempotentOutcome {
                    value: result,
                    replayed: true,
                });
            }
            InFlightRegistration::Leader(leader) => {
                if let Some(cached) = self.receipts.lookup_idempotency(
                    principal_id,
                    "delete_sessions",
                    idempotency_key,
                    &req_hash,
                    volume_writable_hint,
                )? {
                    let result: DeleteSessionsResult = serde_json::from_value(cached.clone())
                        .map_err(|e| StorageError::Catalog {
                            message: format!("corrupt cached delete receipt: {e}"),
                        })?;
                    leader.succeed(cached);
                    return Ok(IdempotentOutcome {
                        value: result,
                        replayed: true,
                    });
                }
                leader
            }
        };

        let mut trash_dirs_to_unlink: Vec<PathBuf> = Vec::new();
        let run_res: StorageResult<(DeleteSessionsResult, Value)> =
            (|| {
                let maintenance_lock = self.acquire_shared_maintenance_lock()?;
                {
                    let mut guard = self.state.lock().map_err(|_| {
                        StorageError::Core(CoreError::poisoned("coordinator_state"))
                    })?;
                    if guard.phase != CapturePhase::Idle || guard.is_busy_for_ops() {
                        return Err(guard.busy_error("delete sessions"));
                    }
                    if guard.volume_released || self.volume.is_volume_released() {
                        return Err(StorageError::locked(
                            "volume_released",
                            "cannot delete sessions while recording volume is released",
                        ));
                    }
                    guard.transition(CapturePhase::Deleting)?;
                    self.phase_cv.notify_all();
                }

                let delete_outcome = self.execute_trash_staged_delete(
                    &req,
                    &requested_ids,
                    principal_id,
                    idempotency_key,
                    &req_hash,
                    &mut trash_dirs_to_unlink,
                );

                {
                    let mut guard = self.state.lock().map_err(|_| {
                        StorageError::Core(CoreError::poisoned("coordinator_state"))
                    })?;
                    if guard.phase == CapturePhase::Deleting {
                        let _ = guard.transition(CapturePhase::Idle);
                        self.phase_cv.notify_all();
                    }
                }
                drop(maintenance_lock);

                delete_outcome
            })();

        let (result, json_val) = match run_res {
            Ok(pair) => pair,
            Err(err) => {
                inflight_leader.fail(&err);
                return Err(err);
            }
        };

        // Phase C: physically unlink `.miro-ego/deleting/<sid>` outside `Deleting` phase & catalog lock!
        // Because `catalog.forget_paths` and `commit_delete_sessions_bundle` already committed on disk,
        // any crash mid-unlink replays the `200 OK` receipt and sweeps `.miro-ego/deleting/` on recovery.
        for trash_path in trash_dirs_to_unlink {
            let _ = fs::remove_dir_all(&trash_path);
        }
        let deleting_root = self.volume.paths().deleting_dir();
        if deleting_root.is_dir() {
            let _ = fsync_directory(&deleting_root);
        }

        inflight_leader.succeed(json_val);
        Ok(IdempotentOutcome {
            value: result,
            replayed: false,
        })
    }

    fn execute_trash_staged_delete(
        &self,
        req: &DeleteSessionsRequest,
        requested_ids: &HashSet<String>,
        principal_id: &str,
        idempotency_key: &str,
        req_hash: &str,
        trash_dirs_to_unlink: &mut Vec<PathBuf>,
    ) -> StorageResult<(DeleteSessionsResult, Value)> {
        let mut resolved: HashMap<String, IndexedSession> =
            HashMap::with_capacity(req.sessions.len());
        let recordings_root = self.volume.paths().recordings_dir();
        let root_fd =
            open_directory_safe(&recordings_root).map_err(|err| StorageError::Unavailable {
                message: format!("recordings directory unavailable: {err}"),
            })?;
        let root_dev = fstat(root_fd.as_raw_fd())
            .map_err(|err| StorageError::io("recordings_stat", "fstat recordings root", err))?
            .st_dev;

        for target in &req.sessions {
            let entry = self
                .catalog
                .get_indexed_session(&target.session_id)?
                .ok_or_else(|| StorageError::NotFound {
                    session_id: target.session_id.clone(),
                })?;

            if !is_valid_uuid_v7(&entry.session_id)
                || !is_valid_sha256(&entry.manifest_sha256)
                || !is_canonical_session_relative_dir(&entry.relative_dir)
            {
                return Err(StorageError::integrity(
                    "unsafe_delete_target",
                    format!(
                        "catalog entry for session {} has non-canonical relative_dir or digest",
                        target.session_id
                    ),
                ));
            }

            if entry.manifest_sha256 != target.manifest_sha256 {
                return Err(StorageError::conflict(
                    "manifest_digest_mismatch",
                    format!("manifest_sha256 mismatch for session {}", target.session_id),
                ));
            }
            let disk_manifest = open_manifest(
                &recordings_root,
                &entry.relative_dir,
                Some(&entry.session_id),
            )?;
            if disk_manifest.sha256 != target.manifest_sha256 {
                return Err(StorageError::conflict(
                    "manifest_digest_mismatch",
                    format!(
                        "on-disk manifest digest mismatch for session {}",
                        target.session_id
                    ),
                ));
            }

            for child in self.catalog.continuations_of(&entry.session_id)? {
                if !requested_ids.contains(&child.session_id) {
                    return Err(StorageError::conflict(
                        "continuation_dependency",
                        format!(
                            "session {} cannot be deleted while continuation {} remains",
                            entry.session_id, child.session_id
                        ),
                    ));
                }
            }

            resolved.insert(entry.session_id.clone(), entry);
        }

        // Order deletions so continuations (higher take_sequence) are removed BEFORE predecessors.
        let mut ordered: Vec<IndexedSession> = resolved.into_values().collect();
        ordered.sort_by(|a, b| {
            b.take_sequence
                .cmp(&a.take_sequence)
                .then_with(|| b.session_id.cmp(&a.session_id))
        });

        self.catalog.mark_mount_dirty();
        let deleting_root = self.volume.paths().deleting_dir();
        if self.volume.paths().hidden_meta_dir().is_dir() {
            let _ = fs::create_dir_all(&deleting_root);
        }

        let writable_now = self.volume.is_mounted_and_writable();
        self.receipts.set_pending_delete_intent(
            Some(PendingDeleteIntent {
                principal_id: principal_id.to_owned(),
                key: idempotency_key.to_owned(),
                request_hash: req_hash.to_owned(),
                targets: ordered
                    .iter()
                    .map(|e| (e.session_id.clone(), e.relative_dir.clone()))
                    .collect(),
                recorded_at: now_rfc3339_utc(),
            }),
            writable_now,
        );

        let mut deleted_session_ids = Vec::with_capacity(ordered.len());
        let mut deleted_relative_dirs = Vec::with_capacity(ordered.len());
        let mut failed_sessions = Vec::new();
        let mut touched_date_parents: HashSet<PathBuf> = HashSet::new();

        // Phase A: atomically rename each `recordings/<rel_dir>` into `.miro-ego/deleting/<sid>`.
        let mut iter = ordered.into_iter();
        while let Some(entry) = iter.next() {
            if !is_valid_uuid_v7(&entry.session_id)
                || !is_valid_sha256(&entry.manifest_sha256)
                || !is_canonical_session_relative_dir(&entry.relative_dir)
            {
                failed_sessions.push(FailedDeleteSession {
                    session_id: entry.session_id,
                    error: "refusing to delete non-canonical session path".to_owned(),
                });
                for remaining in iter {
                    failed_sessions.push(FailedDeleteSession {
                        session_id: remaining.session_id,
                        error: "aborted after earlier session deletion failure".to_owned(),
                    });
                }
                break;
            }
            let target_dir = recordings_root.join(&entry.relative_dir);
            if let Some(parent) = target_dir.parent()
                && let Ok(pmeta) = fs::symlink_metadata(parent)
                && (pmeta.file_type().is_symlink() || !pmeta.is_dir() || pmeta.dev() != root_dev)
            {
                failed_sessions.push(FailedDeleteSession {
                    session_id: entry.session_id,
                    error: "parent date directory failed symlink or st_dev check".to_owned(),
                });
                for remaining in iter {
                    failed_sessions.push(FailedDeleteSession {
                        session_id: remaining.session_id,
                        error: "aborted after earlier session deletion failure".to_owned(),
                    });
                }
                break;
            }
            match fs::symlink_metadata(&target_dir) {
                Ok(tmeta) => {
                    if tmeta.file_type().is_symlink() || !tmeta.is_dir() || tmeta.dev() != root_dev
                    {
                        failed_sessions.push(FailedDeleteSession {
                            session_id: entry.session_id,
                            error: "session directory failed symlink or st_dev check".to_owned(),
                        });
                        for remaining in iter {
                            failed_sessions.push(FailedDeleteSession {
                                session_id: remaining.session_id,
                                error: "aborted after earlier session deletion failure".to_owned(),
                            });
                        }
                        break;
                    }
                    let staged_trash = deleting_root.join(&entry.session_id);
                    if staged_trash.exists() {
                        let _ = fs::remove_dir_all(&staged_trash);
                    }
                    let moved_ok = if deleting_root.is_dir()
                        && fs::rename(&target_dir, &staged_trash).is_ok()
                    {
                        trash_dirs_to_unlink.push(staged_trash);
                        true
                    } else {
                        match fs::remove_dir_all(&target_dir) {
                            Ok(()) => true,
                            Err(err) if err.kind() == io::ErrorKind::NotFound => true,
                            Err(err) => {
                                failed_sessions.push(FailedDeleteSession {
                                    session_id: entry.session_id.clone(),
                                    error: format!("failed to remove session directory: {err}"),
                                });
                                false
                            }
                        }
                    };
                    if !moved_ok {
                        for remaining in iter {
                            failed_sessions.push(FailedDeleteSession {
                                session_id: remaining.session_id,
                                error: "aborted after earlier session deletion failure".to_owned(),
                            });
                        }
                        break;
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => {
                    failed_sessions.push(FailedDeleteSession {
                        session_id: entry.session_id,
                        error: format!("failed to stat session directory: {err}"),
                    });
                    for remaining in iter {
                        failed_sessions.push(FailedDeleteSession {
                            session_id: remaining.session_id,
                            error: "aborted after earlier session deletion failure".to_owned(),
                        });
                    }
                    break;
                }
            }

            if let Some(parent) = target_dir.parent()
                && parent != recordings_root
            {
                touched_date_parents.insert(parent.to_path_buf());
            }
            deleted_relative_dirs.push(entry.relative_dir);
            deleted_session_ids.push(entry.session_id);
        }

        for parent in &touched_date_parents {
            let _ = fs::remove_dir(parent);
        }
        if !touched_date_parents.is_empty() {
            let _ = fsync_directory(&recordings_root);
        }
        if deleting_root.is_dir() && !trash_dirs_to_unlink.is_empty() {
            let _ = fsync_directory(&deleting_root);
        }

        // Phase B: commit single-batch `forget_paths` in `BinarySessionCatalog` + persist final
        // `delete_sessions` receipt BEFORE unlinking `.miro-ego/deleting/<sid>`!
        if !deleted_relative_dirs.is_empty()
            && self.catalog.forget_paths(&deleted_relative_dirs).is_err()
        {
            let _ = self.catalog.forget_paths_in_memory(&deleted_relative_dirs);
        }

        let result = DeleteSessionsResult {
            schema: SESSION_DELETE_RESULT_SCHEMA.to_owned(),
            deleted_session_ids,
            failed_sessions,
        };
        let json_val = serde_json::to_value(&result).unwrap_or(Value::Null);
        self.receipts.commit_delete_sessions_bundle(
            principal_id,
            idempotency_key,
            req_hash,
            json_val.clone(),
            self.volume.is_mounted_and_writable(),
        );
        Ok((result, json_val))
    }
}
