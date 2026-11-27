//! Bounded receipt persistence (`/data/.miro-ego/receipts/coordinator-receipts.json`)
//! with unmounted RAM fallback, pre-release `SafeSwapReceipt` bundle commits, crash-safe
//! `PendingDeleteIntent` journal, and in-flight idempotent coalescing.

use super::types::{RetainedUnsuccessfulSessionResource, SafeSwapReceipt};
use crate::util::{now_rfc3339_utc, sweep_stale_atomic_temps};
use crate::{StorageError, StorageResult};
use miro_ego_core::CoreError;
use miro_ego_core::paths::DataVolumePaths;
use miro_ego_core::sys::fs::{read_bounded_json, write_bytes_atomic};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

const MAX_IDEMPOTENCY_ENTRIES: usize = 256;
const MAX_FOCUS_IDEMPOTENCY_ENTRIES: usize = 32;
const MAX_UNSUCCESSFUL_OUTCOMES: usize = 64;
const MAX_RECEIPT_FILE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IdempotencyRecord {
    #[serde(default)]
    principal_id: String,
    key: String,
    operation: String,
    request_hash: String,
    response_json: Value,
    recorded_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PendingDeleteIntent {
    pub principal_id: String,
    pub key: String,
    pub request_hash: String,
    pub targets: Vec<(String, String)>,
    pub recorded_at: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedReceipts {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    volume_id: Option<String>,
    idempotency: VecDeque<IdempotencyRecord>,
    safe_swap: Option<SafeSwapReceipt>,
    unsuccessful: VecDeque<RetainedUnsuccessfulSessionResource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_delete: Option<PendingDeleteIntent>,
}

struct ReceiptStoreState {
    receipts: PersistedReceipts,
    loaded_from_disk: bool,
    active_volume_id: Option<String>,
}

pub(super) struct ReceiptStore {
    paths: DataVolumePaths,
    state: Mutex<ReceiptStoreState>,
}

impl ReceiptStore {
    pub(super) fn open(
        paths: DataVolumePaths,
        volume_writable: bool,
        initial_volume_id: Option<&str>,
    ) -> Self {
        let (mut loaded, loaded_from_disk) = if volume_writable && paths.receipts_dir().is_dir() {
            let _ = sweep_stale_atomic_temps(&paths.receipts_dir());
            let file = paths.receipts_dir().join("coordinator-receipts.json");
            let data = read_bounded_json::<PersistedReceipts>(&file, MAX_RECEIPT_FILE_BYTES, 16)
                .unwrap_or_default();
            if let (Some(expected_vid), Some(disk_vid)) =
                (initial_volume_id, data.volume_id.as_deref())
            {
                if disk_vid != expected_vid {
                    (PersistedReceipts::default(), true)
                } else {
                    (data, true)
                }
            } else {
                (data, true)
            }
        } else {
            (PersistedReceipts::default(), false)
        };
        if let Some(vid) = initial_volume_id {
            loaded.volume_id = Some(vid.to_owned());
        }
        Self {
            paths,
            state: Mutex::new(ReceiptStoreState {
                receipts: loaded,
                loaded_from_disk,
                active_volume_id: initial_volume_id.map(str::to_owned),
            }),
        }
    }

    /// Bind the receipt store to `new_volume_id`: when a different volume/card is inserted,
    /// isolate RAM state from the prior card and load the new card's `coordinator-receipts.json`.
    pub(super) fn bind_volume(&self, new_volume_id: Option<&str>, volume_writable: bool) {
        let Some(vid) = new_volume_id else {
            return;
        };
        let Ok(mut guard) = self.state.lock() else {
            return;
        };
        let switching_cards = guard
            .active_volume_id
            .as_deref()
            .is_some_and(|prev| prev != vid);
        if switching_cards {
            guard.receipts = PersistedReceipts {
                volume_id: Some(vid.to_owned()),
                ..PersistedReceipts::default()
            };
            guard.loaded_from_disk = false;
            guard.active_volume_id = Some(vid.to_owned());
        } else if guard.active_volume_id.is_none() {
            guard.active_volume_id = Some(vid.to_owned());
            guard.receipts.volume_id = Some(vid.to_owned());
        }
        drop(guard);
        self.ensure_loaded_from_disk(volume_writable);
    }

    /// Reload and merge on-disk `coordinator-receipts.json` when transitioning from an
    /// unmounted boot into a mounted writable recording volume so prior receipts are never wiped.
    pub(super) fn ensure_loaded_from_disk(&self, volume_writable: bool) {
        if !volume_writable {
            return;
        }
        let Ok(mut guard) = self.state.lock() else {
            return;
        };
        if guard.loaded_from_disk {
            return;
        }
        let receipts_dir = self.paths.receipts_dir();
        if !receipts_dir.is_dir() {
            return;
        }
        let _ = sweep_stale_atomic_temps(&receipts_dir);
        let file = receipts_dir.join("coordinator-receipts.json");
        let had_in_ram_changes = !guard.receipts.idempotency.is_empty()
            || !guard.receipts.unsuccessful.is_empty()
            || guard.receipts.safe_swap.is_some()
            || guard.receipts.pending_delete.is_some();

        if let Ok(disk) = read_bounded_json::<PersistedReceipts>(&file, MAX_RECEIPT_FILE_BYTES, 16)
        {
            let same_volume = match (&guard.active_volume_id, &disk.volume_id) {
                (Some(active_vid), Some(disk_vid)) => active_vid == disk_vid,
                _ => true,
            };
            if same_volume {
                if guard.active_volume_id.is_none() {
                    guard.active_volume_id = disk.volume_id.clone();
                }
                if guard.receipts.volume_id.is_none() {
                    guard.receipts.volume_id =
                        guard.active_volume_id.clone().or(disk.volume_id.clone());
                }
                let mut merged_unsuccessful = disk.unsuccessful;
                for mem_item in guard.receipts.unsuccessful.drain(..) {
                    let sid = mem_item.outcome.recording_state.session_id.clone();
                    merged_unsuccessful
                        .retain(|existing| existing.outcome.recording_state.session_id != sid);
                    merged_unsuccessful.push_back(mem_item);
                }
                while merged_unsuccessful.len() > MAX_UNSUCCESSFUL_OUTCOMES {
                    merged_unsuccessful.pop_front();
                }
                guard.receipts.unsuccessful = merged_unsuccessful;

                let mut merged_idem = disk.idempotency;
                for mem_rec in guard.receipts.idempotency.drain(..) {
                    merged_idem.retain(|r| {
                        !(r.principal_id == mem_rec.principal_id
                            && r.operation == mem_rec.operation
                            && r.key == mem_rec.key)
                    });
                    merged_idem.push_back(mem_rec);
                }
                while merged_idem.len() > MAX_IDEMPOTENCY_ENTRIES {
                    merged_idem.pop_front();
                }
                guard.receipts.idempotency = merged_idem;

                if guard.receipts.safe_swap.is_none() {
                    guard.receipts.safe_swap = disk.safe_swap;
                }
                if guard.receipts.pending_delete.is_none() {
                    guard.receipts.pending_delete = disk.pending_delete;
                }
            }
        }

        guard.loaded_from_disk = true;
        if had_in_ram_changes {
            self.persist_locked(&mut guard.receipts);
        }
    }

    pub(super) fn lookup_idempotency(
        &self,
        principal_id: &str,
        operation: &str,
        key: &str,
        request_hash: &str,
        volume_writable: bool,
    ) -> StorageResult<Option<Value>> {
        self.ensure_loaded_from_disk(volume_writable);
        let guard = self
            .state
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("receipt_store")))?;
        if let Some(found) =
            guard.receipts.idempotency.iter().find(|r| {
                r.principal_id == principal_id && r.operation == operation && r.key == key
            })
        {
            if found.request_hash != request_hash {
                return Err(StorageError::conflict(
                    "idempotency_conflict",
                    "Idempotency-Key reused with different request parameters",
                ));
            }
            return Ok(Some(found.response_json.clone()));
        }
        Ok(None)
    }

    fn insert_idempotency_locked(
        receipts: &mut PersistedReceipts,
        principal_id: &str,
        operation: &str,
        key: &str,
        request_hash: &str,
        response_json: Value,
    ) {
        receipts.idempotency.retain(|r| {
            !(r.principal_id == principal_id && r.operation == operation && r.key == key)
        });
        if operation == "set_camera_focus" {
            let focus_count = receipts
                .idempotency
                .iter()
                .filter(|r| r.operation == "set_camera_focus")
                .count();
            if focus_count >= MAX_FOCUS_IDEMPOTENCY_ENTRIES
                && let Some(pos) = receipts
                    .idempotency
                    .iter()
                    .position(|r| r.operation == "set_camera_focus")
            {
                receipts.idempotency.remove(pos);
            }
        }
        while receipts.idempotency.len() >= MAX_IDEMPOTENCY_ENTRIES {
            receipts.idempotency.pop_front();
        }
        receipts.idempotency.push_back(IdempotencyRecord {
            principal_id: principal_id.to_owned(),
            key: key.to_owned(),
            operation: operation.to_owned(),
            request_hash: request_hash.to_owned(),
            response_json,
            recorded_at: now_rfc3339_utc(),
        });
    }

    pub(super) fn store_idempotency(
        &self,
        principal_id: &str,
        operation: &str,
        key: &str,
        request_hash: &str,
        response_json: Value,
        volume_writable: bool,
    ) {
        self.ensure_loaded_from_disk(volume_writable);
        let Ok(mut guard) = self.state.lock() else {
            return;
        };
        Self::insert_idempotency_locked(
            &mut guard.receipts,
            principal_id,
            operation,
            key,
            request_hash,
            response_json,
        );
        if volume_writable {
            self.persist_locked(&mut guard.receipts);
        }
    }

    /// Atomically persist `safe_swap` (when `Some`) and the optional `stop_capture` idempotency
    /// receipt in a single write BEFORE the recording volume is marked `released = true`.
    pub(super) fn commit_safe_swap_bundle(
        &self,
        idem_entry: Option<(&str, &str, &str, Value)>,
        receipt: Option<SafeSwapReceipt>,
        volume_id: Option<&str>,
        volume_writable: bool,
    ) {
        self.ensure_loaded_from_disk(volume_writable);
        let Ok(mut guard) = self.state.lock() else {
            return;
        };
        if let Some(vid) = volume_id.or_else(|| receipt.as_ref().map(|r| r.volume_id.as_str())) {
            guard.receipts.volume_id = Some(vid.to_owned());
            guard.active_volume_id = Some(vid.to_owned());
        }
        guard.receipts.safe_swap = receipt;
        if let Some((principal_id, key, req_hash, response_json)) = idem_entry {
            Self::insert_idempotency_locked(
                &mut guard.receipts,
                principal_id,
                "stop_capture",
                key,
                req_hash,
                response_json,
            );
        }
        if volume_writable {
            self.persist_locked(&mut guard.receipts);
        }
    }

    /// Roll back an aborted `SafeSwap` receipt (and optional `stop_capture` idempotency entry)
    /// when post-commit zero-handle verification detects lingering handles.
    pub(super) fn rollback_safe_swap_bundle(
        &self,
        idem_key: Option<(&str, &str)>,
        volume_writable: bool,
    ) {
        self.ensure_loaded_from_disk(volume_writable);
        let Ok(mut guard) = self.state.lock() else {
            return;
        };
        guard.receipts.safe_swap = None;
        if let Some((principal_id, key)) = idem_key {
            guard.receipts.idempotency.retain(|r| {
                !(r.principal_id == principal_id && r.operation == "stop_capture" && r.key == key)
            });
        }
        if volume_writable {
            self.persist_locked(&mut guard.receipts);
        }
    }

    pub(super) fn set_pending_delete_intent(
        &self,
        intent: Option<PendingDeleteIntent>,
        volume_writable: bool,
    ) {
        self.ensure_loaded_from_disk(volume_writable);
        let Ok(mut guard) = self.state.lock() else {
            return;
        };
        guard.receipts.pending_delete = intent;
        if volume_writable {
            self.persist_locked(&mut guard.receipts);
        }
    }

    /// Peek any uncompleted `PendingDeleteIntent` without clearing it from disk until
    /// `commit_delete_sessions_bundle` succeeds after unlink + `forget_paths`.
    pub(super) fn peek_pending_delete_intent(
        &self,
        volume_writable: bool,
    ) -> Option<PendingDeleteIntent> {
        self.ensure_loaded_from_disk(volume_writable);
        let Ok(guard) = self.state.lock() else {
            return None;
        };
        guard.receipts.pending_delete.clone()
    }

    /// Atomically clear `pending_delete` and commit the `delete_sessions` idempotency receipt
    /// BEFORE unlinking directories inside `.miro-ego/deleting/`.
    pub(super) fn commit_delete_sessions_bundle(
        &self,
        principal_id: &str,
        key: &str,
        request_hash: &str,
        response_json: Value,
        volume_writable: bool,
    ) {
        self.ensure_loaded_from_disk(volume_writable);
        let Ok(mut guard) = self.state.lock() else {
            return;
        };
        guard.receipts.pending_delete = None;
        Self::insert_idempotency_locked(
            &mut guard.receipts,
            principal_id,
            "delete_sessions",
            key,
            request_hash,
            response_json,
        );
        if volume_writable {
            self.persist_locked(&mut guard.receipts);
        }
    }

    pub(super) fn set_safe_swap(&self, receipt: Option<SafeSwapReceipt>, volume_writable: bool) {
        self.ensure_loaded_from_disk(volume_writable);
        let Ok(mut guard) = self.state.lock() else {
            return;
        };
        guard.receipts.safe_swap = receipt;
        if volume_writable {
            self.persist_locked(&mut guard.receipts);
        }
    }

    pub(super) fn get_safe_swap(
        &self,
        volume_writable: bool,
    ) -> StorageResult<Option<SafeSwapReceipt>> {
        self.ensure_loaded_from_disk(volume_writable);
        let guard = self
            .state
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("receipt_store")))?;
        Ok(guard.receipts.safe_swap.clone())
    }

    pub(super) fn record_unsuccessful(
        &self,
        resource: RetainedUnsuccessfulSessionResource,
        volume_writable: bool,
    ) {
        self.ensure_loaded_from_disk(volume_writable);
        let Ok(mut guard) = self.state.lock() else {
            return;
        };
        let sid = resource.outcome.recording_state.session_id.clone();
        guard
            .receipts
            .unsuccessful
            .retain(|item| item.outcome.recording_state.session_id != sid);
        while guard.receipts.unsuccessful.len() >= MAX_UNSUCCESSFUL_OUTCOMES {
            guard.receipts.unsuccessful.pop_front();
        }
        guard.receipts.unsuccessful.push_back(resource);
        if volume_writable {
            self.persist_locked(&mut guard.receipts);
        }
    }

    pub(super) fn get_unsuccessful(
        &self,
        session_id: &str,
        volume_writable: bool,
    ) -> StorageResult<Option<RetainedUnsuccessfulSessionResource>> {
        self.ensure_loaded_from_disk(volume_writable);
        let guard = self
            .state
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("receipt_store")))?;
        Ok(guard
            .receipts
            .unsuccessful
            .iter()
            .find(|item| item.outcome.recording_state.session_id == session_id)
            .cloned())
    }

    /// Persist receipts strictly onto `/data/.miro-ego/receipts/` when mounted and writable,
    /// evicting oldest idempotency entries until serialized size `<= MAX_RECEIPT_FILE_BYTES`.
    fn persist_locked(&self, receipts: &mut PersistedReceipts) {
        let receipts_dir = self.paths.receipts_dir();
        if !receipts_dir.is_dir() {
            return;
        }
        loop {
            let Ok(mut bytes) = serde_json::to_vec(receipts) else {
                return;
            };
            bytes.push(b'\n');
            if bytes.len() <= MAX_RECEIPT_FILE_BYTES {
                let _ = write_bytes_atomic(
                    &receipts_dir.join("coordinator-receipts.json"),
                    &bytes,
                    0o640,
                    true,
                );
                return;
            }
            if receipts.idempotency.pop_front().is_some() {
                continue;
            }
            if receipts.unsuccessful.len() > 1 {
                receipts.unsuccessful.pop_front();
                continue;
            }
            return;
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct InFlightKey {
    principal_id: String,
    operation: &'static str,
    key: String,
}

enum InFlightOutcome {
    Running,
    Succeeded(Value),
    Failed(StorageError),
}

struct InFlightSlot {
    request_hash: String,
    outcome: Mutex<InFlightOutcome>,
    cv: Condvar,
}

pub(super) struct InFlightTracker {
    entries: Mutex<HashMap<InFlightKey, Arc<InFlightSlot>>>,
}

pub(super) enum InFlightRegistration<'a> {
    Leader(InFlightLeaderGuard<'a>),
    Replayed(Value),
}

pub(super) struct InFlightLeaderGuard<'a> {
    tracker: &'a InFlightTracker,
    key: InFlightKey,
    slot: Arc<InFlightSlot>,
    completed: bool,
}

impl InFlightLeaderGuard<'_> {
    pub(super) fn succeed(mut self, response_json: Value) {
        if let Ok(mut guard) = self.slot.outcome.lock() {
            *guard = InFlightOutcome::Succeeded(response_json);
        }
        if let Ok(mut map) = self.tracker.entries.lock() {
            map.remove(&self.key);
        }
        self.slot.cv.notify_all();
        self.completed = true;
    }

    pub(super) fn fail(mut self, err: &StorageError) {
        if let Ok(mut guard) = self.slot.outcome.lock() {
            *guard = InFlightOutcome::Failed(clone_storage_error(err));
        }
        if let Ok(mut map) = self.tracker.entries.lock() {
            map.remove(&self.key);
        }
        self.slot.cv.notify_all();
        self.completed = true;
    }
}

impl Drop for InFlightLeaderGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            if let Ok(mut guard) = self.slot.outcome.lock() {
                *guard = InFlightOutcome::Failed(StorageError::conflict(
                    "idempotency_aborted",
                    "in-flight idempotent command aborted before completion",
                ));
            }
            if let Ok(mut map) = self.tracker.entries.lock() {
                map.remove(&self.key);
            }
            self.slot.cv.notify_all();
        }
    }
}

impl InFlightTracker {
    pub(super) fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub(super) fn acquire(
        &self,
        principal_id: &str,
        operation: &'static str,
        key: &str,
        request_hash: &str,
    ) -> StorageResult<InFlightRegistration<'_>> {
        let map_key = InFlightKey {
            principal_id: principal_id.to_owned(),
            operation,
            key: key.to_owned(),
        };
        let existing_slot = {
            let mut map = self
                .entries
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("inflight_idempotency")))?;
            if let Some(existing) = map.get(&map_key) {
                if existing.request_hash != request_hash {
                    return Err(StorageError::conflict(
                        "idempotency_conflict",
                        "Idempotency-Key is currently in flight with different request parameters",
                    ));
                }
                Some(Arc::clone(existing))
            } else {
                let slot = Arc::new(InFlightSlot {
                    request_hash: request_hash.to_owned(),
                    outcome: Mutex::new(InFlightOutcome::Running),
                    cv: Condvar::new(),
                });
                map.insert(map_key.clone(), Arc::clone(&slot));
                return Ok(InFlightRegistration::Leader(InFlightLeaderGuard {
                    tracker: self,
                    key: map_key,
                    slot,
                    completed: false,
                }));
            }
        };

        let Some(slot) = existing_slot else {
            return Err(StorageError::conflict(
                "idempotency_aborted",
                "missing in-flight idempotency slot",
            ));
        };
        let mut guard = slot
            .outcome
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("inflight_slot")))?;
        while matches!(*guard, InFlightOutcome::Running) {
            let (next, wait_res) = slot
                .cv
                .wait_timeout(guard, Duration::from_secs(30))
                .map_err(|_| StorageError::Core(CoreError::poisoned("inflight_slot")))?;
            guard = next;
            if wait_res.timed_out() && matches!(*guard, InFlightOutcome::Running) {
                return Err(StorageError::conflict(
                    "capture_busy",
                    "timed out waiting for concurrent idempotent operation to finish",
                ));
            }
        }
        match &*guard {
            InFlightOutcome::Succeeded(val) => Ok(InFlightRegistration::Replayed(val.clone())),
            InFlightOutcome::Failed(err) => Err(clone_storage_error(err)),
            InFlightOutcome::Running => Err(StorageError::conflict(
                "capture_busy",
                "concurrent idempotent operation is still running",
            )),
        }
    }
}

pub(super) fn clone_storage_error(err: &StorageError) -> StorageError {
    match err {
        StorageError::Unavailable { message } => StorageError::Unavailable {
            message: message.clone(),
        },
        StorageError::ServiceUnavailable { code, message } => StorageError::ServiceUnavailable {
            code,
            message: message.clone(),
        },
        StorageError::VolumeInvalid { message } => StorageError::VolumeInvalid {
            message: message.clone(),
        },
        StorageError::NotFound { session_id } => StorageError::NotFound {
            session_id: session_id.clone(),
        },
        StorageError::Catalog { message } => StorageError::Catalog {
            message: message.clone(),
        },
        StorageError::CatalogChanged { catalog_revision } => StorageError::CatalogChanged {
            catalog_revision: catalog_revision.clone(),
        },
        StorageError::InvalidInput { code, message } => StorageError::InvalidInput {
            code,
            message: message.clone(),
        },
        StorageError::Conflict { code, message } => StorageError::Conflict {
            code,
            message: message.clone(),
        },
        StorageError::CaptureBusy { state, message } => StorageError::CaptureBusy {
            state: *state,
            message: message.clone(),
        },
        StorageError::Unprocessable { code, message } => StorageError::Unprocessable {
            code,
            message: message.clone(),
        },
        StorageError::Locked { code, message } => StorageError::Locked {
            code,
            message: message.clone(),
        },
        StorageError::Unsupported { code, message } => StorageError::Unsupported {
            code,
            message: message.clone(),
        },
        StorageError::Integrity { code, message } => StorageError::Integrity {
            code,
            message: message.clone(),
        },
        StorageError::RangeNotSatisfiable { total_bytes } => StorageError::RangeNotSatisfiable {
            total_bytes: *total_bytes,
        },
        StorageError::Io {
            code,
            context,
            source,
        } => StorageError::Io {
            code,
            context: context.clone(),
            source: io::Error::new(source.kind(), source.to_string()),
        },
        StorageError::Core(core_err) => match core_err {
            CoreError::InvalidInput { code, message } => {
                StorageError::Core(CoreError::InvalidInput {
                    code,
                    message: message.clone(),
                })
            }
            CoreError::InvalidConfig { code, message } => {
                StorageError::Core(CoreError::InvalidConfig {
                    code,
                    message: message.clone(),
                })
            }
            CoreError::LockBusy { message } => StorageError::Core(CoreError::LockBusy {
                message: message.clone(),
            }),
            CoreError::Poisoned { resource } => StorageError::Core(CoreError::poisoned(resource)),
            CoreError::Io {
                code,
                context,
                source,
            } => StorageError::Core(CoreError::Io {
                code,
                context: context.clone(),
                source: io::Error::new(source.kind(), source.to_string()),
            }),
        },
    }
}
