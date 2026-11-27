//! High-speed in-memory session index (`RwLock<CatalogIndex>`) persisted via atomic
//! binary snapshots (`/data/.miro-ego/index/catalog.bin`).
//!
//! Zero SQLite WAL on exFAT! Binary framing:
//! - `[0..8]`   `MEGOCAT1` magic (`CATALOG_BINARY_MAGIC`)
//! - `[8..12]`  format version `u32` little-endian (`CATALOG_FORMAT_VERSION = 1`)
//! - `[12..16]` IEEE CRC32 checksum `u32` little-endian over payload bytes
//! - `[16..20]` payload length `u32` little-endian (`<= MAX_CATALOG_BIN_BYTES`)
//! - `[20..]`   UTF-8 serialized JSON snapshot payload
//!
//! If `catalog.bin` is missing or corrupt (bad magic, bad version, truncated, CRC mismatch,
//! or malformed payload), [`BinarySessionCatalog`] automatically rebuilds from
//! `/data/recordings/YYYY-MM-DD/HH-MM-SS/manifest.json` and writes a clean atomic snapshot.

use crate::artifact_io::{
    extract_manifest_artifacts, open_manifest_in_dir, open_pinned_directory,
    open_relative_regular_in_dir, unique_downloadable_artifact_bytes,
};
use crate::util::{
    UNKNOWN_DATE_DIR_NAME, base64url_decode, base64url_encode, crc32_ieee, domain_sha256,
    is_canonical_session_relative_dir, is_date_dir_name, now_rfc3339_utc, parse_rfc3339_utc_nanos,
    sha256_hex, sweep_stale_atomic_temps,
};
use crate::{
    CATALOG_BINARY_FORMAT_VERSION, CATALOG_BINARY_MAGIC, CATALOG_FORMAT_VERSION, CATALOG_WAL_MAGIC,
    CatalogEntry, SessionCatalog, SessionState, StorageError, StorageResult,
};
use miro_ego_core::CoreError;
use miro_ego_core::ids::{is_valid_sha256, is_valid_uuid_v4, is_valid_uuid_v7, new_uuid_v4};
use miro_ego_core::locks::{FlockGuard, LockMode, acquire_flock};
use miro_ego_core::paths::DataVolumePaths;
use miro_ego_core::sys::fs::{
    fstat, fstatat_nofollow, is_exfat_or_msdos, open_directory_safe, openat_retry,
    read_bounded_bytes, write_bytes_atomic,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::ffi::CString;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};

pub const SESSION_LIST_SCHEMA: &str = "miro-ego.session-list.v1";
pub const DEVICE_SESSION_SCHEMA: &str = "miro-ego.device-session.v1";
pub const DEFAULT_PAGE_LIMIT: usize = 50;
pub const MAX_PAGE_LIMIT: usize = 200;
/// Hard cap for `/data/.miro-ego/index/catalog.bin` (16 MiB).
pub const MAX_CATALOG_BIN_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_CATALOG_WAL_BYTES: usize = 8 * 1024 * 1024;
const CATALOG_HEADER_BYTES: usize = 20;
const WAL_COMPACT_THRESHOLD: usize = 64;
const WAL_OP_ADMIT: u8 = 1;
const WAL_OP_SET_VERIFICATION: u8 = 2;
const WAL_OP_FORGET_PATHS: u8 = 3;

/// Device identity snapshot copied verbatim from a sealed manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionDeviceIdentity {
    pub device_id: String,
    pub device_label: String,
}

/// Validator metadata embedded in [`GatewayVerification`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayValidatorInfo {
    pub name: String,
    pub version: String,
    pub build_sha256: String,
}

/// Gateway verification diagnostic matching OpenAPI `GatewayVerificationDiagnostic`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayVerificationDiagnostic {
    pub code: String,
    pub summary: String,
}

/// Gateway verification verdict matching OpenAPI `GatewayVerification`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayVerification {
    pub actor: String,
    pub validator: GatewayValidatorInfo,
    pub manifest_sha256: String,
    pub verified_at: String,
    pub verdict: String,
    pub diagnostics: Vec<GatewayVerificationDiagnostic>,
}

/// OpenAPI `SessionSummary` projection for `/api/v1/sessions`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovered_prefix: Option<bool>,
    pub session_id: String,
    pub producer_outcome: String,
    pub take_id: String,
    pub take_sequence: u64,
    pub continuation_of: Option<String>,
    pub display_name: String,
    pub device: SessionDeviceIdentity,
    pub started_at: String,
    pub ended_at: String,
    pub duration_seconds: f64,
    pub total_bytes: u64,
    pub verification: Option<GatewayVerification>,
}

/// OpenAPI `SessionDiscoveryDiagnostic` error categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryDiagnosticCode {
    ManifestUnreadable,
    UnsupportedSchema,
    ManifestInvalid,
    ManifestNotSealed,
}

/// OpenAPI `SessionDiscoveryDiagnostic` for unreadable/corrupt/duplicate candidates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionDiscoveryDiagnostic {
    pub quarantine_id: String,
    pub code: DiscoveryDiagnosticCode,
    pub observed_at: String,
    pub message: String,
}

/// OpenAPI `SessionList` response (`miro-ego.session-list.v1`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionListPage {
    pub schema: String,
    pub catalog_revision: String,
    pub items: Vec<SessionSummary>,
    pub diagnostics: Vec<SessionDiscoveryDiagnostic>,
    pub next_cursor: Option<String>,
}

/// Full indexed session record kept in RAM and serialized into `catalog.bin`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexedSession {
    pub session_id: String,
    pub take_id: String,
    pub take_sequence: u64,
    pub continuation_of: Option<String>,
    pub display_name: String,
    pub device: SessionDeviceIdentity,
    pub relative_dir: String,
    pub started_at: String,
    pub ended_at: String,
    pub duration_seconds: f64,
    pub total_bytes: u64,
    pub manifest_sha256: String,
    pub state: SessionState,
    #[serde(default)]
    pub recovered_prefix: bool,
    #[serde(default)]
    pub verification: Option<GatewayVerification>,
}

impl IndexedSession {
    pub fn to_catalog_entry(&self) -> CatalogEntry {
        CatalogEntry {
            session_id: self.session_id.clone(),
            display_name: self.display_name.clone(),
            relative_dir: self.relative_dir.clone(),
            started_at: self.started_at.clone(),
            duration_seconds: self.duration_seconds,
            total_bytes: self.total_bytes,
            manifest_sha256: self.manifest_sha256.clone(),
            state: self.state,
        }
    }

    pub fn to_summary(&self, include_recovery: bool) -> SessionSummary {
        SessionSummary {
            recovered_prefix: if include_recovery && self.recovered_prefix {
                Some(true)
            } else {
                None
            },
            session_id: self.session_id.clone(),
            producer_outcome: "sealed".to_owned(),
            take_id: self.take_id.clone(),
            take_sequence: self.take_sequence,
            continuation_of: self.continuation_of.clone(),
            display_name: self.display_name.clone(),
            device: self.device.clone(),
            started_at: self.started_at.clone(),
            ended_at: self.ended_at.clone(),
            duration_seconds: self.duration_seconds,
            total_bytes: self.total_bytes,
            verification: self.verification.clone(),
        }
    }

    pub fn from_basic_entry(entry: CatalogEntry) -> Self {
        Self {
            take_id: entry.session_id.clone(),
            take_sequence: 1,
            continuation_of: None,
            device: SessionDeviceIdentity {
                device_id: "00000000-0000-4000-8000-000000000000".to_owned(),
                device_label: "Miro-Ego-00000000".to_owned(),
            },
            ended_at: entry.started_at.clone(),
            session_id: entry.session_id,
            display_name: entry.display_name,
            relative_dir: entry.relative_dir,
            started_at: entry.started_at,
            duration_seconds: entry.duration_seconds,
            total_bytes: entry.total_bytes,
            manifest_sha256: entry.manifest_sha256,
            state: entry.state,
            recovered_prefix: false,
            verification: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiagnosticRecord {
    relative_dir: String,
    diagnostic: SessionDiscoveryDiagnostic,
}

/// Serializable snapshot body encoded inside `/data/.miro-ego/index/catalog.bin`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CatalogSnapshotPayload {
    revision_seq: u64,
    entries: Vec<IndexedSession>,
    diagnostics: Vec<DiagnosticRecord>,
}

/// In-memory index protected by `RwLock` inside [`BinarySessionCatalog`].
#[derive(Debug, Clone, Default)]
pub struct CatalogIndex {
    revision_seq: u64,
    catalog_revision: String,
    by_id: HashMap<String, IndexedSession>,
    by_path: HashMap<String, String>,
    started_at_ns: HashMap<String, Option<i128>>,
    sorted_ids: Vec<String>,
    diagnostics_by_path: BTreeMap<String, SessionDiscoveryDiagnostic>,
}

impl CatalogIndex {
    fn new_empty() -> Self {
        let mut idx = Self::default();
        idx.recompute_revision();
        idx
    }

    fn rebuild_sorted_cache(&mut self) {
        self.started_at_ns
            .retain(|sid, _| self.by_id.contains_key(sid));
        for (sid, entry) in &self.by_id {
            self.started_at_ns
                .entry(sid.clone())
                .or_insert_with(|| parse_rfc3339_utc_nanos(&entry.started_at));
        }
        let mut ids: Vec<String> = self.by_id.keys().cloned().collect();
        ids.sort_by(|a_id, b_id| {
            let a_utc = self.started_at_ns.get(a_id).copied().flatten();
            let b_utc = self.started_at_ns.get(b_id).copied().flatten();
            match (b_utc, a_utc) {
                (Some(bn), Some(an)) => bn.cmp(&an),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => {
                    let a_str = self
                        .by_id
                        .get(a_id)
                        .map(|e| e.started_at.as_str())
                        .unwrap_or("");
                    let b_str = self
                        .by_id
                        .get(b_id)
                        .map(|e| e.started_at.as_str())
                        .unwrap_or("");
                    b_str.cmp(a_str)
                }
            }
            .then_with(|| b_id.cmp(a_id))
        });
        self.sorted_ids = ids;
    }

    fn recompute_revision(&mut self) {
        self.rebuild_sorted_cache();
        let sorted_items = self.sorted_sessions();
        let sorted_diags: Vec<&SessionDiscoveryDiagnostic> =
            self.diagnostics_by_path.values().collect();
        let canonical = serde_json::json!({
            "revision_seq": self.revision_seq,
            "items": sorted_items,
            "diagnostics": sorted_diags,
        });
        let raw = serde_json::to_vec(&canonical).unwrap_or_default();
        self.catalog_revision = domain_sha256("miro-ego.catalog-revision.v1", &raw);
    }

    fn bump_revision(&mut self) {
        self.revision_seq = self.revision_seq.saturating_add(1);
        self.recompute_revision();
    }

    fn sorted_sessions(&self) -> Vec<&IndexedSession> {
        if self.sorted_ids.len() == self.by_id.len() {
            let mut out = Vec::with_capacity(self.sorted_ids.len());
            for sid in &self.sorted_ids {
                if let Some(entry) = self.by_id.get(sid) {
                    out.push(entry);
                }
            }
            if out.len() == self.by_id.len() {
                return out;
            }
        }
        let mut items: Vec<&IndexedSession> = self.by_id.values().collect();
        items.sort_by(|a, b| {
            let a_utc = parse_rfc3339_utc_nanos(&a.started_at);
            let b_utc = parse_rfc3339_utc_nanos(&b.started_at);
            match (b_utc, a_utc) {
                (Some(bn), Some(an)) => bn.cmp(&an),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => b.started_at.cmp(&a.started_at),
            }
            .then_with(|| b.session_id.cmp(&a.session_id))
        });
        items
    }

    fn to_payload(&self) -> CatalogSnapshotPayload {
        CatalogSnapshotPayload {
            revision_seq: self.revision_seq,
            entries: self.sorted_sessions().into_iter().cloned().collect(),
            diagnostics: self
                .diagnostics_by_path
                .iter()
                .map(|(rel, diag)| DiagnosticRecord {
                    relative_dir: rel.clone(),
                    diagnostic: diag.clone(),
                })
                .collect(),
        }
    }

    fn from_payload(payload: CatalogSnapshotPayload) -> StorageResult<Self> {
        let mut idx = Self {
            revision_seq: payload.revision_seq,
            catalog_revision: String::new(),
            by_id: HashMap::with_capacity(payload.entries.len()),
            by_path: HashMap::with_capacity(payload.entries.len()),
            started_at_ns: HashMap::with_capacity(payload.entries.len()),
            sorted_ids: Vec::with_capacity(payload.entries.len()),
            diagnostics_by_path: BTreeMap::new(),
        };
        for entry in payload.entries {
            validate_indexed_session_record(&entry)?;
            if idx.by_id.contains_key(&entry.session_id)
                || idx.by_path.contains_key(&entry.relative_dir)
            {
                return Err(StorageError::Catalog {
                    message: "corrupt catalog snapshot contains duplicate session_id or path"
                        .to_owned(),
                });
            }
            idx.by_path
                .insert(entry.relative_dir.clone(), entry.session_id.clone());
            idx.by_id.insert(entry.session_id.clone(), entry);
        }
        for rec in payload.diagnostics {
            if !is_canonical_session_relative_dir(&rec.relative_dir)
                || !is_valid_uuid_v4(&rec.diagnostic.quarantine_id)
            {
                return Err(StorageError::Catalog {
                    message: "corrupt catalog snapshot contains invalid diagnostic path or id"
                        .to_owned(),
                });
            }
            idx.diagnostics_by_path
                .insert(rec.relative_dir, rec.diagnostic);
        }
        idx.recompute_revision();
        Ok(idx)
    }

    /// Decode and strictly validate a framed `MEGOCAT1` binary catalog snapshot.
    pub fn decode_binary(raw: &[u8]) -> StorageResult<Self> {
        let payload = decode_catalog_binary(raw)?;
        Self::from_payload(payload)
    }
}

fn validate_indexed_session_record(entry: &IndexedSession) -> StorageResult<()> {
    if !is_valid_uuid_v7(&entry.session_id) || !is_valid_uuid_v7(&entry.take_id) {
        return Err(StorageError::Catalog {
            message: "catalog entry session_id and take_id must be canonical UUIDv7".to_owned(),
        });
    }
    if entry.take_sequence < 1 {
        return Err(StorageError::Catalog {
            message: "catalog entry take_sequence must be >= 1".to_owned(),
        });
    }
    match &entry.continuation_of {
        Some(cid) => {
            if !is_valid_uuid_v7(cid) || entry.take_sequence < 2 {
                return Err(StorageError::Catalog {
                    message: "catalog entry continuation_of requires UUIDv7 and take_sequence >= 2"
                        .to_owned(),
                });
            }
        }
        None => {
            if entry.take_sequence != 1 {
                return Err(StorageError::Catalog {
                    message: "catalog entry without continuation_of must have take_sequence == 1"
                        .to_owned(),
                });
            }
        }
    }
    if !is_canonical_session_relative_dir(&entry.relative_dir) {
        return Err(StorageError::Catalog {
            message: format!(
                "catalog entry has non-canonical relative_dir '{}'",
                entry.relative_dir
            ),
        });
    }
    if !is_valid_sha256(&entry.manifest_sha256) {
        return Err(StorageError::Catalog {
            message: "catalog entry manifest_sha256 must be a 64-char lowercase hex digest"
                .to_owned(),
        });
    }
    if !is_valid_uuid_v4(&entry.device.device_id) {
        return Err(StorageError::Catalog {
            message: "catalog entry device_id must be a canonical UUIDv4".to_owned(),
        });
    }
    if !entry.duration_seconds.is_finite() || entry.duration_seconds < 0.0 {
        return Err(StorageError::Catalog {
            message: "catalog entry duration_seconds must be finite and >= 0".to_owned(),
        });
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PageCursorToken {
    rev: String,
    offset: usize,
    take_id: Option<String>,
    include_recovery: bool,
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
enum WalMutation {
    Admit(IndexedSession),
    SetVerification {
        session_id: String,
        verification: Option<GatewayVerification>,
    },
    ForgetPaths(Vec<String>),
}

/// High-speed in-memory session catalog backed by `/data/.miro-ego/index/catalog.bin`
/// (`MEGOCAT1` compact binary v2) and append-only `/data/.miro-ego/index/catalog.wal`.
pub struct BinarySessionCatalog {
    paths: DataVolumePaths,
    write_lock: Mutex<()>,
    wal_ops: AtomicUsize,
    index: RwLock<CatalogIndex>,
}

impl BinarySessionCatalog {
    /// Open the catalog: loads `catalog.bin` (+ `catalog.wal`) when valid and reconciles it
    /// against `/data/recordings/`, or automatically rebuilds if `catalog.bin` is missing or corrupt.
    pub fn open(paths: DataVolumePaths) -> StorageResult<Self> {
        let catalog = Self {
            paths,
            write_lock: Mutex::new(()),
            wal_ops: AtomicUsize::new(0),
            index: RwLock::new(CatalogIndex::new_empty()),
        };
        let _flock = catalog.try_acquire_index_flock(LockMode::Exclusive);
        let _ = sweep_stale_atomic_temps(&catalog.paths.index_dir());
        let loaded_res = catalog.load_snapshot_and_wal_from_disk(true);
        drop(_flock);
        match loaded_res {
            Ok((loaded, wal_count)) => {
                catalog.wal_ops.store(wal_count, Ordering::Release);
                {
                    let mut guard = catalog
                        .index
                        .write()
                        .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
                    *guard = loaded;
                }
                let _ = catalog.reconcile()?;
                Ok(catalog)
            }
            Err(_) => {
                if catalog.paths.recordings_dir().is_dir() {
                    let _ = catalog.rebuild_index()?;
                }
                Ok(catalog)
            }
        }
    }

    /// Fast startup open: when `/data/.miro-ego/index/mount-clean.marker` is present, no
    /// `.partial` staging or interrupted `deleting/` directories exist, and `catalog.bin`
    /// (+ `catalog.wal`) decodes cleanly, binds the index in $O(N)$ without walking the entire
    /// `/data/recordings/` filesystem tree. Returns `(catalog, skipped_full_scan)`.
    pub fn open_fast_if_clean(paths: DataVolumePaths) -> StorageResult<(Self, bool)> {
        let is_clean_hint = paths.mount_clean_marker_file().is_file()
            && !dir_has_entries(&paths.staging_dir(), Some(".partial"))
            && !dir_has_entries(&paths.deleting_dir(), None);
        if is_clean_hint {
            let catalog = Self {
                paths: paths.clone(),
                write_lock: Mutex::new(()),
                wal_ops: AtomicUsize::new(0),
                index: RwLock::new(CatalogIndex::new_empty()),
            };
            let _flock = catalog.try_acquire_index_flock(LockMode::Shared);
            if let Ok((loaded, wal_count)) = catalog.load_snapshot_and_wal_from_disk(false) {
                drop(_flock);
                catalog.wal_ops.store(wal_count, Ordering::Release);
                let wrote = if let Ok(mut guard) = catalog.index.write() {
                    *guard = loaded;
                    true
                } else {
                    false
                };
                if wrote {
                    return Ok((catalog, true));
                }
            }
        }
        let catalog = Self::open(paths)?;
        Ok((catalog, false))
    }

    /// Strictly read-only catalog open for CLI `miro-ego status`: never sweeps temp files,
    /// never mutates `catalog.bin`/`catalog.wal`, and never contends destructively with a live daemon.
    pub fn open_read_only(paths: DataVolumePaths) -> StorageResult<Self> {
        let catalog = Self {
            paths,
            write_lock: Mutex::new(()),
            wal_ops: AtomicUsize::new(0),
            index: RwLock::new(CatalogIndex::new_empty()),
        };
        let _flock = catalog.try_acquire_index_flock(LockMode::Shared);
        match catalog.load_snapshot_and_wal_from_disk(false) {
            Ok((loaded, wal_count)) => {
                catalog.wal_ops.store(wal_count, Ordering::Release);
                {
                    let mut guard = catalog
                        .index
                        .write()
                        .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
                    *guard = loaded;
                }
                Ok(catalog)
            }
            Err(_) => {
                let recordings_root = catalog.paths.recordings_dir();
                if recordings_root.is_dir() {
                    let (sessions, diags) = scan_recordings_tree(&recordings_root, None)?;
                    let mut staged = CatalogIndex::new_empty();
                    for (rel, diag) in diags {
                        staged.diagnostics_by_path.insert(rel, diag);
                    }
                    for session in sessions {
                        if !staged.by_id.contains_key(&session.session_id) {
                            staged
                                .by_path
                                .insert(session.relative_dir.clone(), session.session_id.clone());
                            staged.by_id.insert(session.session_id.clone(), session);
                        }
                    }
                    staged.recompute_revision();
                    {
                        let mut guard = catalog.index.write().map_err(|_| {
                            StorageError::Core(CoreError::poisoned("catalog_index"))
                        })?;
                        *guard = staged;
                    }
                }
                Ok(catalog)
            }
        }
    }

    /// Create an empty catalog instance without triggering an initial scan.
    pub fn new_empty(paths: DataVolumePaths) -> Self {
        Self {
            paths,
            write_lock: Mutex::new(()),
            wal_ops: AtomicUsize::new(0),
            index: RwLock::new(CatalogIndex::new_empty()),
        }
    }

    /// Clear `mount-clean.marker` before mutating `/data/recordings/` or `/data/.miro-ego/staging/`.
    pub fn mark_mount_dirty(&self) {
        let _ = fs::remove_file(self.paths.mount_clean_marker_file());
    }

    /// Compact any pending `catalog.wal` operations into `catalog.bin` and record `mount-clean.marker`
    /// on graceful daemon shutdown when no staging/deleting work remains.
    pub fn mark_mount_clean(&self) {
        let _ = self.compact();
        if self.paths.index_dir().is_dir()
            && !dir_has_entries(&self.paths.staging_dir(), Some(".partial"))
            && !dir_has_entries(&self.paths.deleting_dir(), None)
        {
            let _ = write_bytes_atomic(
                &self.paths.mount_clean_marker_file(),
                b"clean\n",
                0o640,
                true,
            );
        }
    }

    /// Compact `catalog.bin` + `catalog.wal` into a single fresh `MEGOCAT1` v2 snapshot.
    pub fn compact(&self) -> StorageResult<()> {
        let _persist_guard = self
            .write_lock
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_write")))?;
        let _flock = self.try_acquire_index_flock(LockMode::Exclusive);
        let payload = {
            let guard = self
                .index
                .read()
                .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
            guard.to_payload()
        };
        let bytes = encode_catalog_binary(&payload)?;
        self.write_snapshot_and_clear_wal(&bytes)?;
        Ok(())
    }

    fn try_acquire_index_flock(&self, mode: LockMode) -> Option<FlockGuard> {
        let index_dir = self.paths.index_dir();
        if !index_dir.is_dir() {
            return None;
        }
        let nonblocking = matches!(mode, LockMode::Shared);
        acquire_flock(self.paths.catalog_lock_file(), mode, nonblocking).ok()
    }

    pub fn paths(&self) -> &DataVolumePaths {
        &self.paths
    }

    /// Current canonical domain-separated `sha256:...` catalog revision.
    pub fn catalog_revision(&self) -> StorageResult<String> {
        let guard = self
            .index
            .read()
            .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
        Ok(guard.catalog_revision.clone())
    }

    /// Current monotonically increasing catalog revision counter.
    pub fn revision_seq(&self) -> StorageResult<u64> {
        let guard = self
            .index
            .read()
            .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
        Ok(guard.revision_seq)
    }

    /// Look up the full [`IndexedSession`] record by `session_id`.
    pub fn get_indexed_session(&self, session_id: &str) -> StorageResult<Option<IndexedSession>> {
        let guard = self
            .index
            .read()
            .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
        Ok(guard.by_id.get(session_id).cloned())
    }

    /// Return the OpenAPI `SessionSummary` for `session_id`.
    pub fn session_summary(
        &self,
        session_id: &str,
        include_recovery: bool,
    ) -> StorageResult<Option<SessionSummary>> {
        let guard = self
            .index
            .read()
            .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
        Ok(guard
            .by_id
            .get(session_id)
            .map(|entry| entry.to_summary(include_recovery)))
    }

    /// Return all indexed sessions whose `continuation_of == Some(session_id)`.
    pub fn continuations_of(&self, session_id: &str) -> StorageResult<Vec<IndexedSession>> {
        let guard = self
            .index
            .read()
            .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
        let mut children: Vec<IndexedSession> = guard
            .by_id
            .values()
            .filter(|item| item.continuation_of.as_deref() == Some(session_id))
            .cloned()
            .collect();
        children.sort_by(|a, b| a.session_id.cmp(&b.session_id));
        Ok(children)
    }

    /// Admit a rich [`IndexedSession`] into RAM and persist `catalog.bin` atomically.
    ///
    /// Rejects duplicate `session_id` pointing to a different `relative_dir` to protect
    /// catalog integrity, and persists `catalog.bin` on disk before committing into RAM.
    pub fn admit_indexed_session(&self, entry: IndexedSession) -> StorageResult<()> {
        validate_indexed_session_record(&entry)?;
        self.mutate_and_persist(|staged| {
            if let Some(existing) = staged.by_id.get(&entry.session_id)
                && existing.relative_dir != entry.relative_dir
            {
                return Err(StorageError::conflict(
                    "duplicate_session_id",
                    format!(
                        "session_id {} already indexed at another path",
                        entry.session_id
                    ),
                ));
            }
            if let Some(existing_id) = staged.by_path.get(&entry.relative_dir)
                && existing_id != &entry.session_id
            {
                return Err(StorageError::conflict(
                    "duplicate_session_path",
                    "directory path already belongs to another session_id",
                ));
            }

            let wal_op = WalMutation::Admit(entry.clone());
            staged.diagnostics_by_path.remove(&entry.relative_dir);
            staged
                .by_path
                .insert(entry.relative_dir.clone(), entry.session_id.clone());
            staged.by_id.insert(entry.session_id.clone(), entry);
            staged.bump_revision();
            Ok(((), Some(wal_op)))
        })
    }

    /// Admit a rich [`IndexedSession`] into RAM without writing `catalog.bin`, used as a
    /// fallback when a recording is already durably sealed in `/data/recordings/` and
    /// persisting `catalog.bin` encounters a transient storage error (`ENOSPC`/`EROFS`/`EIO`).
    pub fn admit_in_memory(&self, entry: IndexedSession) -> StorageResult<()> {
        validate_indexed_session_record(&entry)?;
        let _persist_guard = self
            .write_lock
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_write")))?;
        let mut staged = {
            let guard = self
                .index
                .read()
                .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
            guard.clone()
        };
        if let Some(existing) = staged.by_id.get(&entry.session_id) {
            if existing.relative_dir != entry.relative_dir {
                return Err(StorageError::conflict(
                    "duplicate_session_id",
                    format!(
                        "session_id {} already indexed at another path",
                        entry.session_id
                    ),
                ));
            }
        }
        if let Some(existing_id) = staged.by_path.get(&entry.relative_dir) {
            if existing_id != &entry.session_id {
                return Err(StorageError::conflict(
                    "duplicate_session_path",
                    "directory path already belongs to another session_id",
                ));
            }
        }
        staged.diagnostics_by_path.remove(&entry.relative_dir);
        staged
            .by_path
            .insert(entry.relative_dir.clone(), entry.session_id.clone());
        staged.by_id.insert(entry.session_id.clone(), entry);
        staged.bump_revision();

        let mut guard = self
            .index
            .write()
            .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
        *guard = staged;
        Ok(())
    }

    /// Reconcile the in-memory catalog against `/data/recordings/`:
    /// - prunes ghost entries whose directories no longer exist on disk,
    /// - sweeps leftover control markers inside sealed directories,
    /// - indexes newly discovered `recordings/<day>/<time>/manifest.json` directories, and
    /// - preserves cached [`GatewayVerification`] for unchanged `(session_id, relative_dir, manifest_sha256)`
    ///   while computing fresh verification verdicts for unverified or new sessions.
    pub fn reconcile(&self) -> StorageResult<usize> {
        self.scan_and_commit_from_disk(false)
    }

    /// Parse and admit one sealed session directory under `/data/recordings/<relative_dir>`.
    pub fn admit_from_session_dir(&self, relative_dir: &str) -> StorageResult<IndexedSession> {
        let (dir_file, root_dev, exfat) =
            open_pinned_directory(&self.paths.recordings_dir(), relative_dir)?;
        let opened = open_manifest_in_dir(&dir_file, root_dev, exfat, None)?;
        sweep_leftover_control_markers_best_effort(
            &self.paths.recordings_dir().join(relative_dir),
            &opened.parsed,
        );
        let mut indexed =
            parse_indexed_session_from_manifest(relative_dir, &opened.parsed, &opened.sha256)?;
        indexed.verification = Some(verify_discovered_session_artifacts(
            &dir_file,
            root_dev,
            &opened.parsed,
            &opened.sha256,
        ));
        self.admit_indexed_session(indexed.clone())?;
        Ok(indexed)
    }

    /// Update or clear the gateway verification verdict for an indexed session.
    pub fn set_verification(
        &self,
        session_id: &str,
        verification: Option<GatewayVerification>,
    ) -> StorageResult<bool> {
        self.mutate_and_persist(|staged| {
            let Some(entry) = staged.by_id.get_mut(session_id) else {
                return Ok((false, None));
            };
            entry.verification = verification.clone();
            staged.bump_revision();
            Ok((
                true,
                Some(WalMutation::SetVerification {
                    session_id: session_id.to_owned(),
                    verification,
                }),
            ))
        })
    }

    /// Remove any indexed session or discovery diagnostic associated with `relative_dir`,
    /// monotonically bumping `catalog_revision` and appending an $O(1)$ WAL record.
    pub fn forget_path(&self, relative_dir: &str) -> StorageResult<bool> {
        let removed = self.forget_paths(&[relative_dir.to_owned()])?;
        Ok(removed > 0)
    }

    /// Batch-remove multiple session `relative_dir` paths in a single revision bump and
    /// single WAL append ($O(1)$ disk syncs instead of $O(N^2)$ full-catalog rewrites).
    pub fn forget_paths(&self, relative_dirs: &[String]) -> StorageResult<usize> {
        if relative_dirs.is_empty() {
            return Ok(0);
        }
        self.mutate_and_persist(|staged| {
            let mut removed_paths = Vec::new();
            for rel in relative_dirs {
                let removed_id = staged.by_path.remove(rel.as_str());
                let removed_entry = removed_id
                    .as_ref()
                    .and_then(|sid| staged.by_id.remove(sid))
                    .is_some();
                let removed_diag = staged.diagnostics_by_path.remove(rel.as_str()).is_some();
                if removed_entry || removed_diag {
                    removed_paths.push(rel.clone());
                }
            }
            if removed_paths.is_empty() {
                return Ok((0, None));
            }
            let count = removed_paths.len();
            staged.bump_revision();
            Ok((count, Some(WalMutation::ForgetPaths(removed_paths))))
        })
    }

    /// Deterministic cursor-paginated listing matching OpenAPI v1 `GET /api/v1/sessions`
    /// (`limit`, `cursor`, `Prefer: include-recovery`, optional `take_id` filter).
    pub fn list_sessions_page(
        &self,
        limit: Option<usize>,
        cursor: Option<&str>,
        include_recovery: bool,
        take_id: Option<&str>,
    ) -> StorageResult<SessionListPage> {
        let effective_limit = limit.unwrap_or(DEFAULT_PAGE_LIMIT);
        if !(1..=MAX_PAGE_LIMIT).contains(&effective_limit) {
            return Err(StorageError::invalid_input(
                "invalid_limit",
                format!("limit must be in 1..={MAX_PAGE_LIMIT}"),
            ));
        }
        if let Some(tid) = take_id {
            if !is_valid_uuid_v7(tid) {
                return Err(StorageError::invalid_input(
                    "invalid_take_id",
                    "take_id must be a canonical lowercase UUIDv7",
                ));
            }
        }

        let guard = self
            .index
            .read()
            .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;

        let offset = match cursor {
            Some(raw_token) => {
                let decoded = decode_cursor(raw_token)?;
                if decoded.take_id.as_deref() != take_id
                    || decoded.include_recovery != include_recovery
                {
                    return Err(StorageError::invalid_input(
                        "invalid_cursor",
                        "cursor filter parameters do not match request",
                    ));
                }
                if decoded.rev != guard.catalog_revision {
                    return Err(StorageError::CatalogChanged {
                        catalog_revision: guard.catalog_revision.clone(),
                    });
                }
                decoded.offset
            }
            None => 0,
        };

        // Filtered schema-valid items (newest started_at first, session_id desc tie-break).
        let matching_items: Vec<SessionSummary> = guard
            .sorted_sessions()
            .into_iter()
            .filter(|entry| take_id.is_none_or(|tid| entry.take_id == tid))
            .map(|entry| entry.to_summary(include_recovery))
            .collect();

        // Diagnostics always remain visible because their take identity is untrusted.
        let matching_diags: Vec<SessionDiscoveryDiagnostic> =
            guard.diagnostics_by_path.values().cloned().collect();

        let total_combined = matching_items.len() + matching_diags.len();
        if offset > total_combined {
            return Err(StorageError::invalid_input(
                "invalid_cursor",
                "cursor offset exceeds catalog length",
            ));
        }

        let end = (offset + effective_limit).min(total_combined);
        let mut page_items = Vec::new();
        let mut page_diags = Vec::new();

        for pos in offset..end {
            if pos < matching_items.len() {
                page_items.push(matching_items[pos].clone());
            } else {
                page_diags.push(matching_diags[pos - matching_items.len()].clone());
            }
        }

        let next_cursor = if end < total_combined {
            Some(encode_cursor(&PageCursorToken {
                rev: guard.catalog_revision.clone(),
                offset: end,
                take_id: take_id.map(str::to_owned),
                include_recovery,
            })?)
        } else {
            None
        };

        Ok(SessionListPage {
            schema: SESSION_LIST_SCHEMA.to_owned(),
            catalog_revision: guard.catalog_revision.clone(),
            items: page_items,
            diagnostics: page_diags,
            next_cursor,
        })
    }

    fn mutate_and_persist<T, F>(&self, mutator: F) -> StorageResult<T>
    where
        F: FnOnce(&mut CatalogIndex) -> StorageResult<(T, Option<WalMutation>)>,
    {
        let _persist_guard = self
            .write_lock
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_write")))?;
        let _flock = self.try_acquire_index_flock(LockMode::Exclusive);
        let mut staged = {
            let guard = self
                .index
                .read()
                .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
            guard.clone()
        };
        let (out, wal_op_opt) = mutator(&mut staged)?;
        let Some(wal_op) = wal_op_opt else {
            return Ok(out);
        };

        let cur_wal_ops = self.wal_ops.load(Ordering::Acquire);
        let can_append_wal =
            cur_wal_ops < WAL_COMPACT_THRESHOLD && self.paths.catalog_file().is_file();
        if can_append_wal && self.append_wal_mutation(&wal_op).is_ok() {
            self.wal_ops.fetch_add(1, Ordering::AcqRel);
        } else {
            let snapshot_bytes = encode_catalog_binary(&staged.to_payload())?;
            self.write_snapshot_and_clear_wal(&snapshot_bytes)?;
        }

        let mut guard = self
            .index
            .write()
            .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
        *guard = staged;
        Ok(out)
    }

    fn load_snapshot_and_wal_from_disk(
        &self,
        repair_torn_wal: bool,
    ) -> StorageResult<(CatalogIndex, usize)> {
        let path = self.paths.catalog_file();
        let raw = read_bounded_bytes(&path, MAX_CATALOG_BIN_BYTES).map_err(|err| {
            StorageError::Catalog {
                message: format!("unable to read catalog.bin: {err}"),
            }
        })?;
        let payload = decode_catalog_binary(&raw)?;
        let mut idx = CatalogIndex::from_payload(payload)?;
        let wal_replayed =
            replay_catalog_wal(&self.paths.catalog_wal_file(), &mut idx, repair_torn_wal)?;
        Ok((idx, wal_replayed))
    }

    fn append_wal_mutation(&self, op: &WalMutation) -> StorageResult<()> {
        let wal_path = self.paths.catalog_wal_file();
        let frame = encode_wal_frame(op)?;
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .mode(0o640)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&wal_path)
            .map_err(|err| StorageError::io("catalog_wal_open", "open catalog.wal", err))?;
        let st = fstat(file.as_raw_fd())
            .map_err(|err| StorageError::io("catalog_wal_stat", "fstat catalog.wal", err))?;
        if (st.st_mode & libc::S_IFMT) != libc::S_IFREG {
            return Err(StorageError::Catalog {
                message: "catalog.wal is not a regular file".to_owned(),
            });
        }
        let cur_len = u64::try_from(st.st_size).unwrap_or(0);
        if cur_len as usize + frame.len() + CATALOG_WAL_MAGIC.len() > MAX_CATALOG_WAL_BYTES {
            return Err(StorageError::Catalog {
                message: "catalog.wal exceeded compaction size threshold".to_owned(),
            });
        }
        if cur_len == 0 {
            file.write_all(&CATALOG_WAL_MAGIC)
                .map_err(|err| StorageError::io("catalog_wal_write", "write wal magic", err))?;
        }
        file.write_all(&frame)
            .map_err(|err| StorageError::io("catalog_wal_write", "append wal frame", err))?;
        file.sync_data()
            .map_err(|err| StorageError::io("catalog_wal_sync", "fdatasync catalog.wal", err))?;
        Ok(())
    }

    fn write_snapshot_and_clear_wal(&self, bytes: &[u8]) -> StorageResult<()> {
        let index_dir = self.paths.index_dir();
        if !index_dir.is_dir() {
            if !self.paths.hidden_meta_dir().is_dir() {
                return Err(StorageError::Unavailable {
                    message: "volume hidden metadata directory does not exist".to_owned(),
                });
            }
            fs::create_dir_all(&index_dir).map_err(|err| {
                StorageError::io("catalog_dir_create_failed", "create catalog index dir", err)
            })?;
        }
        write_bytes_atomic(&self.paths.catalog_file(), bytes, 0o640, true).map_err(|err| {
            StorageError::io("catalog_write_failed", "write catalog.bin atomically", err)
        })?;
        let _ = fs::remove_file(self.paths.catalog_wal_file());
        self.wal_ops.store(0, Ordering::Release);
        Ok(())
    }
    fn scan_and_commit_from_disk(&self, force_bump: bool) -> StorageResult<usize> {
        let _persist_guard = self
            .write_lock
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_write")))?;
        let _flock = self.try_acquire_index_flock(LockMode::Exclusive);
        let current = {
            let guard = self
                .index
                .read()
                .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
            guard.clone()
        };

        let recordings_root = self.paths.recordings_dir();
        let (scanned_sessions, scanned_diags) =
            scan_recordings_tree(&recordings_root, Some(&current))?;

        let mut by_id: HashMap<String, IndexedSession> = HashMap::new();
        let mut by_path: HashMap<String, String> = HashMap::new();
        let mut diagnostics_by_path: BTreeMap<String, SessionDiscoveryDiagnostic> = BTreeMap::new();
        for (rel, mut diag) in scanned_diags {
            if let Some(prev_diag) = current.diagnostics_by_path.get(&rel) {
                if prev_diag.code == diag.code && prev_diag.message == diag.message {
                    diag = prev_diag.clone();
                }
            }
            diagnostics_by_path.insert(rel, diag);
        }

        let mut duplicate_ids: HashMap<String, Vec<String>> = HashMap::new();
        for session in scanned_sessions {
            if let Some(prev) = by_id.get(&session.session_id) {
                duplicate_ids
                    .entry(session.session_id.clone())
                    .or_insert_with(|| vec![prev.relative_dir.clone()])
                    .push(session.relative_dir.clone());
            } else {
                by_path.insert(session.relative_dir.clone(), session.session_id.clone());
                by_id.insert(session.session_id.clone(), session);
            }
        }

        // Duplicate session_id safety: remove ambiguous duplicates from `items` and
        // surface deterministic discovery diagnostics instead of silently overwriting.
        for (dup_id, rel_paths) in duplicate_ids {
            by_id.remove(&dup_id);
            for rel in rel_paths {
                by_path.remove(&rel);
                let dup_msg =
                    "duplicate session identity detected across multiple recording directories"
                        .to_owned();
                let diag = match current.diagnostics_by_path.get(&rel) {
                    Some(prev_diag)
                        if prev_diag.code == DiscoveryDiagnosticCode::ManifestInvalid
                            && prev_diag.message == dup_msg =>
                    {
                        prev_diag.clone()
                    }
                    _ => SessionDiscoveryDiagnostic {
                        quarantine_id: new_uuid_v4(),
                        code: DiscoveryDiagnosticCode::ManifestInvalid,
                        observed_at: now_rfc3339_utc(),
                        message: dup_msg,
                    },
                };
                diagnostics_by_path.insert(rel, diag);
            }
        }

        let count = by_id.len();
        let unchanged = !force_bump
            && by_id == current.by_id
            && by_path == current.by_path
            && diagnostics_by_path == current.diagnostics_by_path;
        if unchanged {
            return Ok(count);
        }

        let mut staged = CatalogIndex {
            revision_seq: current.revision_seq.saturating_add(1),
            catalog_revision: String::new(),
            by_id,
            by_path,
            started_at_ns: HashMap::new(),
            sorted_ids: Vec::new(),
            diagnostics_by_path,
        };
        staged.recompute_revision();

        let snapshot_bytes = encode_catalog_binary(&staged.to_payload())?;
        if let Err(err) = self.write_snapshot_and_clear_wal(&snapshot_bytes)
            && !is_persist_ignorable_on_rebuild(&err)
        {
            return Err(err);
        }

        let mut guard = self
            .index
            .write()
            .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
        *guard = staged;
        Ok(count)
    }
}

fn is_persist_ignorable_on_rebuild(err: &StorageError) -> bool {
    match err {
        StorageError::Unavailable { .. } => true,
        StorageError::Io { source, .. } => matches!(
            source.raw_os_error(),
            Some(libc::EROFS | libc::ENOSPC | libc::EIO | libc::EDQUOT | libc::EACCES)
        ),
        _ => false,
    }
}

impl SessionCatalog for BinarySessionCatalog {
    fn list_sessions(&self) -> StorageResult<Vec<CatalogEntry>> {
        let guard = self
            .index
            .read()
            .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
        Ok(guard
            .sorted_sessions()
            .into_iter()
            .map(IndexedSession::to_catalog_entry)
            .collect())
    }

    fn get_session(&self, session_id: &str) -> StorageResult<Option<CatalogEntry>> {
        let guard = self
            .index
            .read()
            .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
        Ok(guard
            .by_id
            .get(session_id)
            .map(IndexedSession::to_catalog_entry))
    }

    fn admit_session(&self, entry: CatalogEntry) -> StorageResult<()> {
        self.admit_indexed_session(IndexedSession::from_basic_entry(entry))
    }

    fn remove_session(&self, session_id: &str) -> StorageResult<bool> {
        self.mutate_and_persist(|staged| {
            let Some(removed) = staged.by_id.remove(session_id) else {
                return Ok((false, None));
            };
            let rel = removed.relative_dir.clone();
            staged.by_path.remove(&rel);
            staged.bump_revision();
            Ok((true, Some(WalMutation::ForgetPaths(vec![rel]))))
        })
    }

    fn rebuild_index(&self) -> StorageResult<usize> {
        self.scan_and_commit_from_disk(true)
    }
}

fn dir_has_entries(dir: &Path, required_suffix: Option<&str>) -> bool {
    let Ok(it) = fs::read_dir(dir) else {
        return false;
    };
    for entry in it.flatten() {
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            return true;
        };
        if name.starts_with('.') {
            continue;
        }
        if required_suffix.is_none_or(|sfx| name.ends_with(sfx)) {
            return true;
        }
    }
    false
}

/// Encode a `CatalogSnapshotPayload` into the framed `MEGOCAT1` compact binary v2 format with CRC32.
fn encode_catalog_binary(payload: &CatalogSnapshotPayload) -> StorageResult<Vec<u8>> {
    let body_bytes = encode_binary_payload_v2(payload)?;
    let total_len = CATALOG_HEADER_BYTES.saturating_add(body_bytes.len());
    if total_len > MAX_CATALOG_BIN_BYTES {
        return Err(StorageError::Catalog {
            message: "catalog snapshot exceeds maximum 16 MiB size".to_owned(),
        });
    }
    let payload_len = u32::try_from(body_bytes.len()).map_err(|_| StorageError::Catalog {
        message: "catalog payload length overflow".to_owned(),
    })?;
    let crc = crc32_ieee(&body_bytes);

    let mut out = Vec::with_capacity(total_len);
    out.extend_from_slice(&CATALOG_BINARY_MAGIC);
    out.extend_from_slice(&CATALOG_BINARY_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&body_bytes);
    Ok(out)
}

/// Decode and strictly verify a `MEGOCAT1` binary catalog file (`v1` JSON or `v2` compact binary).
fn decode_catalog_binary(raw: &[u8]) -> StorageResult<CatalogSnapshotPayload> {
    if raw.len() < CATALOG_HEADER_BYTES {
        return Err(StorageError::Catalog {
            message: "catalog.bin truncated below header size".to_owned(),
        });
    }
    if raw[0..8] != CATALOG_BINARY_MAGIC {
        return Err(StorageError::Catalog {
            message: "catalog.bin has invalid magic bytes".to_owned(),
        });
    }
    let version = u32::from_le_bytes([raw[8], raw[9], raw[10], raw[11]]);
    let expected_crc = u32::from_le_bytes([raw[12], raw[13], raw[14], raw[15]]);
    let declared_len = u32::from_le_bytes([raw[16], raw[17], raw[18], raw[19]]) as usize;
    let payload_slice = &raw[CATALOG_HEADER_BYTES..];
    if payload_slice.len() != declared_len {
        return Err(StorageError::Catalog {
            message: "catalog.bin payload length mismatch".to_owned(),
        });
    }
    let actual_crc = crc32_ieee(payload_slice);
    if actual_crc != expected_crc {
        return Err(StorageError::Catalog {
            message: "catalog.bin CRC32 checksum mismatch".to_owned(),
        });
    }
    match version {
        v if v == CATALOG_FORMAT_VERSION => {
            serde_json::from_slice(payload_slice).map_err(|err| StorageError::Catalog {
                message: format!("catalog.bin v1 payload decode failed: {err}"),
            })
        }
        v if v == CATALOG_BINARY_FORMAT_VERSION => decode_binary_payload_v2(payload_slice),
        other => Err(StorageError::Catalog {
            message: format!("unsupported catalog.bin format version {other}"),
        }),
    }
}

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_str(out: &mut Vec<u8>, s: &str) -> StorageResult<()> {
    let len = u32::try_from(s.len()).map_err(|_| StorageError::Catalog {
        message: "catalog string exceeds u32 length".to_owned(),
    })?;
    put_u32(out, len);
    out.extend_from_slice(s.as_bytes());
    Ok(())
}

fn put_opt_str(out: &mut Vec<u8>, opt: Option<&str>) -> StorageResult<()> {
    match opt {
        Some(s) => {
            out.push(1);
            put_str(out, s)
        }
        None => {
            out.push(0);
            Ok(())
        }
    }
}

fn encode_verification_v2(
    out: &mut Vec<u8>,
    ver: Option<&GatewayVerification>,
) -> StorageResult<()> {
    let Some(v) = ver else {
        out.push(0);
        return Ok(());
    };
    out.push(1);
    put_str(out, &v.actor)?;
    put_str(out, &v.validator.name)?;
    put_str(out, &v.validator.version)?;
    put_str(out, &v.validator.build_sha256)?;
    put_str(out, &v.manifest_sha256)?;
    put_str(out, &v.verified_at)?;
    put_str(out, &v.verdict)?;
    let dlen = u32::try_from(v.diagnostics.len()).map_err(|_| StorageError::Catalog {
        message: "verification diagnostics count overflow".to_owned(),
    })?;
    put_u32(out, dlen);
    for d in &v.diagnostics {
        put_str(out, &d.code)?;
        put_str(out, &d.summary)?;
    }
    Ok(())
}

fn encode_indexed_session_v2(out: &mut Vec<u8>, e: &IndexedSession) -> StorageResult<()> {
    put_str(out, &e.session_id)?;
    put_str(out, &e.take_id)?;
    put_u64(out, e.take_sequence);
    put_opt_str(out, e.continuation_of.as_deref())?;
    put_str(out, &e.display_name)?;
    put_str(out, &e.device.device_id)?;
    put_str(out, &e.device.device_label)?;
    put_str(out, &e.relative_dir)?;
    put_str(out, &e.started_at)?;
    put_str(out, &e.ended_at)?;
    out.extend_from_slice(&e.duration_seconds.to_bits().to_le_bytes());
    put_u64(out, e.total_bytes);
    put_str(out, &e.manifest_sha256)?;
    let state_tag = match e.state {
        SessionState::Sealed => 0_u8,
        SessionState::Staging => 1_u8,
        SessionState::Quarantined => 2_u8,
    };
    out.push(state_tag);
    out.push(u8::from(e.recovered_prefix));
    encode_verification_v2(out, e.verification.as_ref())
}

fn encode_binary_payload_v2(payload: &CatalogSnapshotPayload) -> StorageResult<Vec<u8>> {
    let mut out = Vec::with_capacity(256 + payload.entries.len() * 320);
    put_u64(&mut out, payload.revision_seq);
    let ec = u32::try_from(payload.entries.len()).map_err(|_| StorageError::Catalog {
        message: "catalog entries count overflow".to_owned(),
    })?;
    put_u32(&mut out, ec);
    for entry in &payload.entries {
        encode_indexed_session_v2(&mut out, entry)?;
    }
    let dc = u32::try_from(payload.diagnostics.len()).map_err(|_| StorageError::Catalog {
        message: "catalog diagnostics count overflow".to_owned(),
    })?;
    put_u32(&mut out, dc);
    for diag in &payload.diagnostics {
        put_str(&mut out, &diag.relative_dir)?;
        put_str(&mut out, &diag.diagnostic.quarantine_id)?;
        let code_tag = match diag.diagnostic.code {
            DiscoveryDiagnosticCode::ManifestUnreadable => 0_u8,
            DiscoveryDiagnosticCode::UnsupportedSchema => 1_u8,
            DiscoveryDiagnosticCode::ManifestInvalid => 2_u8,
            DiscoveryDiagnosticCode::ManifestNotSealed => 3_u8,
        };
        out.push(code_tag);
        put_str(&mut out, &diag.diagnostic.observed_at)?;
        put_str(&mut out, &diag.diagnostic.message)?;
    }
    Ok(out)
}

struct BinCursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> BinCursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> StorageResult<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| StorageError::Catalog {
                message: "binary catalog cursor overflow".to_owned(),
            })?;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| StorageError::Catalog {
                message: "binary catalog unexpectedly truncated".to_owned(),
            })?;
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> StorageResult<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> StorageResult<u32> {
        let s = self.take(4)?;
        Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }

    fn u64(&mut self) -> StorageResult<u64> {
        let s = self.take(8)?;
        Ok(u64::from_le_bytes([
            s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
        ]))
    }

    fn str(&mut self) -> StorageResult<String> {
        let len = self.u32()? as usize;
        if len > 1024 * 1024 {
            return Err(StorageError::Catalog {
                message: "binary catalog string exceeds 1 MiB".to_owned(),
            });
        }
        let raw = self.take(len)?;
        std::str::from_utf8(raw)
            .map(str::to_owned)
            .map_err(|_| StorageError::Catalog {
                message: "invalid UTF-8 in binary catalog".to_owned(),
            })
    }

    fn opt_str(&mut self) -> StorageResult<Option<String>> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.str().map(Some),
            _ => Err(StorageError::Catalog {
                message: "invalid option tag in binary catalog".to_owned(),
            }),
        }
    }
}

fn decode_verification_v2(cur: &mut BinCursor<'_>) -> StorageResult<Option<GatewayVerification>> {
    match cur.u8()? {
        0 => Ok(None),
        1 => {
            let actor = cur.str()?;
            let name = cur.str()?;
            let version = cur.str()?;
            let build_sha256 = cur.str()?;
            let manifest_sha256 = cur.str()?;
            let verified_at = cur.str()?;
            let verdict = cur.str()?;
            let dcount = cur.u32()? as usize;
            if dcount > 4096 {
                return Err(StorageError::Catalog {
                    message: "excessive verification diagnostics in binary catalog".to_owned(),
                });
            }
            let mut diagnostics = Vec::with_capacity(dcount.min(64));
            for _ in 0..dcount {
                diagnostics.push(GatewayVerificationDiagnostic {
                    code: cur.str()?,
                    summary: cur.str()?,
                });
            }
            Ok(Some(GatewayVerification {
                actor,
                validator: GatewayValidatorInfo {
                    name,
                    version,
                    build_sha256,
                },
                manifest_sha256,
                verified_at,
                verdict,
                diagnostics,
            }))
        }
        _ => Err(StorageError::Catalog {
            message: "invalid verification tag in binary catalog".to_owned(),
        }),
    }
}

fn decode_indexed_session_v2(cur: &mut BinCursor<'_>) -> StorageResult<IndexedSession> {
    let session_id = cur.str()?;
    let take_id = cur.str()?;
    let take_sequence = cur.u64()?;
    let continuation_of = cur.opt_str()?;
    let display_name = cur.str()?;
    let device_id = cur.str()?;
    let device_label = cur.str()?;
    let relative_dir = cur.str()?;
    let started_at = cur.str()?;
    let ended_at = cur.str()?;
    let duration_seconds = f64::from_bits(cur.u64()?);
    let total_bytes = cur.u64()?;
    let manifest_sha256 = cur.str()?;
    let state = match cur.u8()? {
        0 => SessionState::Sealed,
        1 => SessionState::Staging,
        2 => SessionState::Quarantined,
        _ => {
            return Err(StorageError::Catalog {
                message: "invalid session state tag in binary catalog".to_owned(),
            });
        }
    };
    let recovered_prefix = match cur.u8()? {
        0 => false,
        1 => true,
        _ => {
            return Err(StorageError::Catalog {
                message: "invalid bool tag in binary catalog".to_owned(),
            });
        }
    };
    let verification = decode_verification_v2(cur)?;
    Ok(IndexedSession {
        session_id,
        take_id,
        take_sequence,
        continuation_of,
        display_name,
        device: SessionDeviceIdentity {
            device_id,
            device_label,
        },
        relative_dir,
        started_at,
        ended_at,
        duration_seconds,
        total_bytes,
        manifest_sha256,
        state,
        recovered_prefix,
        verification,
    })
}

fn decode_binary_payload_v2(raw: &[u8]) -> StorageResult<CatalogSnapshotPayload> {
    let mut cur = BinCursor::new(raw);
    let revision_seq = cur.u64()?;
    let ecount = cur.u32()? as usize;
    if ecount > 200_000 {
        return Err(StorageError::Catalog {
            message: "binary catalog entry count exceeds sanity limit".to_owned(),
        });
    }
    let mut entries = Vec::with_capacity(ecount.min(1024));
    for _ in 0..ecount {
        entries.push(decode_indexed_session_v2(&mut cur)?);
    }
    let dcount = cur.u32()? as usize;
    if dcount > 200_000 {
        return Err(StorageError::Catalog {
            message: "binary catalog diagnostic count exceeds sanity limit".to_owned(),
        });
    }
    let mut diagnostics = Vec::with_capacity(dcount.min(256));
    for _ in 0..dcount {
        let relative_dir = cur.str()?;
        let quarantine_id = cur.str()?;
        let code = match cur.u8()? {
            0 => DiscoveryDiagnosticCode::ManifestUnreadable,
            1 => DiscoveryDiagnosticCode::UnsupportedSchema,
            2 => DiscoveryDiagnosticCode::ManifestInvalid,
            3 => DiscoveryDiagnosticCode::ManifestNotSealed,
            _ => {
                return Err(StorageError::Catalog {
                    message: "invalid diagnostic code tag in binary catalog".to_owned(),
                });
            }
        };
        let observed_at = cur.str()?;
        let message = cur.str()?;
        diagnostics.push(DiagnosticRecord {
            relative_dir,
            diagnostic: SessionDiscoveryDiagnostic {
                quarantine_id,
                code,
                observed_at,
                message,
            },
        });
    }
    if cur.pos != raw.len() {
        return Err(StorageError::Catalog {
            message: "trailing bytes in binary v2 catalog payload".to_owned(),
        });
    }
    Ok(CatalogSnapshotPayload {
        revision_seq,
        entries,
        diagnostics,
    })
}

fn encode_wal_frame(op: &WalMutation) -> StorageResult<Vec<u8>> {
    let (tag, body) = match op {
        WalMutation::Admit(entry) => {
            let mut b = Vec::with_capacity(320);
            encode_indexed_session_v2(&mut b, entry)?;
            (WAL_OP_ADMIT, b)
        }
        WalMutation::SetVerification {
            session_id,
            verification,
        } => {
            let mut b = Vec::with_capacity(256);
            put_str(&mut b, session_id)?;
            encode_verification_v2(&mut b, verification.as_ref())?;
            (WAL_OP_SET_VERIFICATION, b)
        }
        WalMutation::ForgetPaths(paths) => {
            let mut b = Vec::with_capacity(64 * paths.len());
            let cnt = u32::try_from(paths.len()).map_err(|_| StorageError::Catalog {
                message: "wal forget batch overflow".to_owned(),
            })?;
            put_u32(&mut b, cnt);
            for p in paths {
                put_str(&mut b, p)?;
            }
            (WAL_OP_FORGET_PATHS, b)
        }
    };
    let len = u32::try_from(body.len()).map_err(|_| StorageError::Catalog {
        message: "wal frame length overflow".to_owned(),
    })?;
    let crc = crc32_ieee(&body);
    let mut frame = Vec::with_capacity(9 + body.len());
    frame.push(tag);
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&crc.to_le_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

fn replay_catalog_wal(
    wal_path: &Path,
    idx: &mut CatalogIndex,
    repair_torn_tail: bool,
) -> StorageResult<usize> {
    let raw = match read_bounded_bytes(wal_path, MAX_CATALOG_WAL_BYTES) {
        Ok(b) => b,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(_) => return Ok(0),
    };
    if raw.len() < CATALOG_WAL_MAGIC.len() || raw[0..8] != CATALOG_WAL_MAGIC {
        if repair_torn_tail {
            let _ = fs::remove_file(wal_path);
        }
        return Ok(0);
    }

    let mut offset = CATALOG_WAL_MAGIC.len();
    let mut applied = 0_usize;
    while offset < raw.len() {
        if raw.len() - offset < 9 {
            break;
        }
        let tag = raw[offset];
        let body_len = u32::from_le_bytes([
            raw[offset + 1],
            raw[offset + 2],
            raw[offset + 3],
            raw[offset + 4],
        ]) as usize;
        let expected_crc = u32::from_le_bytes([
            raw[offset + 5],
            raw[offset + 6],
            raw[offset + 7],
            raw[offset + 8],
        ]);
        let Some(frame_end) = (offset + 9).checked_add(body_len) else {
            break;
        };
        let Some(body) = raw.get(offset + 9..frame_end) else {
            break;
        };
        if crc32_ieee(body) != expected_crc {
            break;
        }
        let mut cur = BinCursor::new(body);
        let apply_ok = match tag {
            WAL_OP_ADMIT => match decode_indexed_session_v2(&mut cur) {
                Ok(entry)
                    if cur.pos == body.len() && validate_indexed_session_record(&entry).is_ok() =>
                {
                    idx.diagnostics_by_path.remove(&entry.relative_dir);
                    idx.by_path
                        .insert(entry.relative_dir.clone(), entry.session_id.clone());
                    idx.by_id.insert(entry.session_id.clone(), entry);
                    idx.revision_seq = idx.revision_seq.saturating_add(1);
                    true
                }
                _ => false,
            },
            WAL_OP_SET_VERIFICATION => match (cur.str(), decode_verification_v2(&mut cur)) {
                (Ok(sid), Ok(ver)) if cur.pos == body.len() => {
                    if let Some(existing) = idx.by_id.get_mut(&sid) {
                        existing.verification = ver;
                        idx.revision_seq = idx.revision_seq.saturating_add(1);
                    }
                    true
                }
                _ => false,
            },
            WAL_OP_FORGET_PATHS => match cur.u32() {
                Ok(cnt) if (cnt as usize) <= 10_000 => {
                    let mut paths = Vec::with_capacity((cnt as usize).min(256));
                    let mut ok = true;
                    for _ in 0..cnt {
                        match cur.str() {
                            Ok(s) => paths.push(s),
                            Err(_) => {
                                ok = false;
                                break;
                            }
                        }
                    }
                    if ok && cur.pos == body.len() {
                        for rel in paths {
                            if let Some(sid) = idx.by_path.remove(&rel) {
                                idx.by_id.remove(&sid);
                            }
                            idx.diagnostics_by_path.remove(&rel);
                        }
                        idx.revision_seq = idx.revision_seq.saturating_add(1);
                        true
                    } else {
                        false
                    }
                }
                _ => false,
            },
            _ => false,
        };
        if !apply_ok {
            break;
        }
        offset = frame_end;
        applied += 1;
    }

    if repair_torn_tail
        && offset < raw.len()
        && let Ok(file) = OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(wal_path)
        && let Ok(off) = libc::off_t::try_from(offset)
    {
        // SAFETY: `file` is a valid open regular descriptor and `off` is within bounds.
        let _ = unsafe { libc::ftruncate(file.as_raw_fd(), off) };
        let _ = file.sync_data();
    }
    if applied > 0 {
        idx.recompute_revision();
    }
    Ok(applied)
}

fn encode_cursor(token: &PageCursorToken) -> StorageResult<String> {
    let raw = serde_json::to_vec(token).map_err(|err| StorageError::Catalog {
        message: format!("encode pagination cursor failed: {err}"),
    })?;
    Ok(base64url_encode(&raw))
}

fn decode_cursor(token: &str) -> StorageResult<PageCursorToken> {
    let raw = base64url_decode(token)
        .map_err(|err| StorageError::invalid_input("invalid_cursor", err))?;
    serde_json::from_slice(&raw)
        .map_err(|_| StorageError::invalid_input("invalid_cursor", "malformed cursor token"))
}

type ScannedRecordings = (
    Vec<IndexedSession>,
    Vec<(String, SessionDiscoveryDiagnostic)>,
);

/// Scan `/data/recordings/` (`YYYY-MM-DD/HH-MM-SS[_NN]` and `unknown-date/recording_NN`).
fn scan_recordings_tree(
    recordings_root: &Path,
    previous: Option<&CatalogIndex>,
) -> StorageResult<ScannedRecordings> {
    let root_dir = match open_directory_safe(recordings_root) {
        Ok(dir) => dir,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            return Ok((Vec::new(), Vec::new()));
        }
        Err(err) => {
            return Err(StorageError::Unavailable {
                message: format!("cannot open recordings root: {err}"),
            });
        }
    };
    let root_stat = fstat(root_dir.as_raw_fd())
        .map_err(|err| StorageError::io("recordings_stat_failed", "fstat recordings root", err))?;
    let root_dev = root_stat.st_dev;
    let exfat = is_exfat_or_msdos(root_dir.as_raw_fd()).unwrap_or(false);

    let mut sessions = Vec::new();
    let mut diagnostics = Vec::new();

    let mut day_names: Vec<String> = fs::read_dir(recordings_root)
        .map_err(|err| StorageError::io("recordings_readdir_failed", "readdir recordings", err))?
        .flatten()
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|name| !name.starts_with('.') && !name.ends_with(".partial"))
        .collect();
    day_names.sort();

    for day_name in day_names {
        let is_dated = is_date_dir_name(&day_name);
        let is_unknown = day_name == UNKNOWN_DATE_DIR_NAME;
        if !is_dated && !is_unknown {
            continue;
        }
        let Ok(day_c) = CString::new(day_name.as_str()) else {
            continue;
        };
        let Ok(day_st) = fstatat_nofollow(root_dir.as_raw_fd(), &day_c) else {
            continue;
        };
        if (day_st.st_mode & libc::S_IFMT) != libc::S_IFDIR || day_st.st_dev != root_dev {
            continue;
        }
        let Ok(day_dir) = openat_retry(
            root_dir.as_raw_fd(),
            &day_c,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        ) else {
            continue;
        };

        let day_path = recordings_root.join(&day_name);
        let mut child_names: Vec<String> = match fs::read_dir(&day_path) {
            Ok(it) => it
                .flatten()
                .filter_map(|e| e.file_name().into_string().ok())
                .filter(|name| !name.starts_with('.') && !name.ends_with(".partial"))
                .collect(),
            Err(_) => continue,
        };
        child_names.sort();

        for child in child_names {
            let rel_dir = format!("{day_name}/{child}");
            if !is_canonical_session_relative_dir(&rel_dir) {
                continue;
            }
            let Ok(child_c) = CString::new(child.as_str()) else {
                continue;
            };
            let Ok(child_st) = fstatat_nofollow(day_dir.as_raw_fd(), &child_c) else {
                continue;
            };
            if (child_st.st_mode & libc::S_IFMT) != libc::S_IFDIR || child_st.st_dev != root_dev {
                diagnostics.push((
                    rel_dir,
                    SessionDiscoveryDiagnostic {
                        quarantine_id: new_uuid_v4(),
                        code: DiscoveryDiagnosticCode::ManifestUnreadable,
                        observed_at: now_rfc3339_utc(),
                        message: "session entry is not a valid local directory".to_owned(),
                    },
                ));
                continue;
            }
            let session_dir = match openat_retry(
                day_dir.as_raw_fd(),
                &child_c,
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            ) {
                Ok(dir) => dir,
                Err(_) => {
                    diagnostics.push((
                        rel_dir,
                        SessionDiscoveryDiagnostic {
                            quarantine_id: new_uuid_v4(),
                            code: DiscoveryDiagnosticCode::ManifestUnreadable,
                            observed_at: now_rfc3339_utc(),
                            message: "unable to open session directory safely".to_owned(),
                        },
                    ));
                    continue;
                }
            };

            match open_manifest_in_dir(&session_dir, root_dev, exfat, None) {
                Ok(opened) => {
                    match classify_and_parse_manifest(&rel_dir, &opened.parsed, &opened.sha256) {
                        Ok(mut indexed) => {
                            sweep_leftover_control_markers_best_effort(
                                &day_path.join(&child),
                                &opened.parsed,
                            );
                            let cached_verification = previous
                                .and_then(|prev| prev.by_id.get(&indexed.session_id))
                                .filter(|prev_entry| {
                                    prev_entry.relative_dir == indexed.relative_dir
                                        && prev_entry.manifest_sha256 == indexed.manifest_sha256
                                })
                                .and_then(|prev_entry| prev_entry.verification.as_ref())
                                .filter(|ver| ver.manifest_sha256 == indexed.manifest_sha256)
                                .cloned();

                            indexed.verification = Some(cached_verification.unwrap_or_else(|| {
                                verify_discovered_session_artifacts(
                                    &session_dir,
                                    root_dev,
                                    &opened.parsed,
                                    &opened.sha256,
                                )
                            }));
                            sessions.push(indexed);
                        }
                        Err((code, message)) => {
                            diagnostics.push((
                                rel_dir,
                                SessionDiscoveryDiagnostic {
                                    quarantine_id: new_uuid_v4(),
                                    code,
                                    observed_at: now_rfc3339_utc(),
                                    message,
                                },
                            ));
                        }
                    }
                }
                Err(_) => {
                    diagnostics.push((
                        rel_dir,
                        SessionDiscoveryDiagnostic {
                            quarantine_id: new_uuid_v4(),
                            code: DiscoveryDiagnosticCode::ManifestUnreadable,
                            observed_at: now_rfc3339_utc(),
                            message: "session manifest is missing or unreadable".to_owned(),
                        },
                    ));
                }
            }
        }
    }

    Ok((sessions, diagnostics))
}

fn sweep_leftover_control_markers_best_effort(session_path: &Path, manifest: &Value) {
    if manifest.get("sealed").and_then(Value::as_bool) != Some(true) {
        return;
    }
    let declared_capture_clock =
        manifest
            .get("logs")
            .and_then(Value::as_array)
            .is_some_and(|logs| {
                logs.iter().any(|item| {
                    item.get("path").and_then(Value::as_str) == Some("audio/capture-clock.json")
                })
            });

    for rel in [
        "recording.json",
        "capture.json",
        "checkpoint.json",
        "audio/checkpoint.json",
        "audio/checkpoint.ndjson",
    ] {
        let p = session_path.join(rel);
        if let Ok(st) = fs::symlink_metadata(&p) {
            if st.file_type().is_file() {
                let _ = fs::remove_file(&p);
            }
        }
    }
    let tmp_ckpt = session_path.join("audio/checkpoint.tmp");
    if let Ok(st) = fs::symlink_metadata(&tmp_ckpt) {
        if st.file_type().is_file() {
            let _ = fs::remove_file(&tmp_ckpt);
        } else if st.file_type().is_dir() {
            let _ = fs::remove_dir(&tmp_ckpt);
        }
    }
    if !declared_capture_clock {
        let cc = session_path.join("audio/capture-clock.json");
        if let Ok(st) = fs::symlink_metadata(&cc) {
            if st.file_type().is_file() {
                let _ = fs::remove_file(&cc);
            }
        }
    }
    let _ = sweep_stale_atomic_temps(session_path);
    let audio_dir = session_path.join("audio");
    if audio_dir.is_dir() {
        let _ = sweep_stale_atomic_temps(&audio_dir);
        let _ = fs::remove_dir(&audio_dir);
    }
}

/// Perform a pinned-descriptor stat + size + regular-file integrity check over all declared
/// manifest artifacts to populate [`GatewayVerification`] during catalog discovery/rebuild.
pub(crate) fn verify_discovered_session_artifacts(
    session_dir: &fs::File,
    root_dev: u64,
    manifest: &Value,
    manifest_sha256: &str,
) -> GatewayVerification {
    let device_obj = manifest.get("device").and_then(Value::as_object);
    let version = device_obj
        .and_then(|d| d.get("software_version"))
        .and_then(Value::as_str)
        .filter(|s| (1..=64).contains(&s.chars().count()))
        .unwrap_or(env!("CARGO_PKG_VERSION"))
        .to_owned();
    let commit = device_obj
        .and_then(|d| d.get("commit"))
        .and_then(Value::as_str)
        .unwrap_or("0000000000000000000000000000000000000000");
    let build_sha256 = sha256_hex(format!("miro-ego:{version}:{commit}").as_bytes());
    let verified_at = now_rfc3339_utc();

    let descriptors = match extract_manifest_artifacts(manifest) {
        Ok(list) => list,
        Err(_) => {
            return GatewayVerification {
                actor: "gateway".to_owned(),
                validator: GatewayValidatorInfo {
                    name: "miro-ego".to_owned(),
                    version,
                    build_sha256,
                },
                manifest_sha256: manifest_sha256.to_owned(),
                verified_at,
                verdict: "unusable".to_owned(),
                diagnostics: vec![GatewayVerificationDiagnostic {
                    code: "manifest_invalid".to_owned(),
                    summary: "manifest artifact inventory failed structural validation".to_owned(),
                }],
            };
        }
    };

    let mut diagnostics = Vec::new();
    for desc in &descriptors {
        let file = match open_relative_regular_in_dir(session_dir, root_dev, &desc.path) {
            Ok(f) => f,
            Err(_) => {
                diagnostics.push(GatewayVerificationDiagnostic {
                    code: "artifact_invalid".to_owned(),
                    summary: format!(
                        "declared artifact for role '{}' is missing or unsafe on disk",
                        desc.role
                    ),
                });
                continue;
            }
        };
        let Ok(st) = fstat(file.as_raw_fd()) else {
            diagnostics.push(GatewayVerificationDiagnostic {
                code: "verification_failed".to_owned(),
                summary: format!("unable to stat declared artifact for role '{}'", desc.role),
            });
            continue;
        };
        #[allow(clippy::useless_conversion)]
        let nlink = u64::from(st.st_nlink);
        let size_ok = u64::try_from(st.st_size).is_ok_and(|sz| sz == desc.bytes);
        if (st.st_mode & libc::S_IFMT) != libc::S_IFREG
            || st.st_dev != root_dev
            || nlink != 1
            || !size_ok
        {
            diagnostics.push(GatewayVerificationDiagnostic {
                code: "artifact_invalid".to_owned(),
                summary: format!(
                    "declared artifact for role '{}' failed size or regular-file identity check",
                    desc.role
                ),
            });
        }
    }

    let verdict = if diagnostics.is_empty() {
        "usable".to_owned()
    } else {
        "unusable".to_owned()
    };

    GatewayVerification {
        actor: "gateway".to_owned(),
        validator: GatewayValidatorInfo {
            name: "miro-ego".to_owned(),
            version,
            build_sha256,
        },
        manifest_sha256: manifest_sha256.to_owned(),
        verified_at,
        verdict,
        diagnostics,
    }
}

fn classify_and_parse_manifest(
    relative_dir: &str,
    parsed: &Value,
    manifest_sha256: &str,
) -> Result<IndexedSession, (DiscoveryDiagnosticCode, String)> {
    let Some(obj) = parsed.as_object() else {
        return Err((
            DiscoveryDiagnosticCode::ManifestInvalid,
            "manifest root is not a JSON object".to_owned(),
        ));
    };
    let Some(schema) = obj.get("schema").and_then(Value::as_str) else {
        return Err((
            DiscoveryDiagnosticCode::UnsupportedSchema,
            "manifest missing schema discriminator".to_owned(),
        ));
    };
    if schema != DEVICE_SESSION_SCHEMA {
        return Err((
            DiscoveryDiagnosticCode::UnsupportedSchema,
            format!("unsupported manifest schema '{schema}'"),
        ));
    }
    if obj.get("sealed").and_then(Value::as_bool) != Some(true) {
        return Err((
            DiscoveryDiagnosticCode::ManifestNotSealed,
            "manifest is not marked sealed=true".to_owned(),
        ));
    }

    parse_indexed_session_from_manifest(relative_dir, parsed, manifest_sha256)
        .map_err(|err| (DiscoveryDiagnosticCode::ManifestInvalid, err.to_string()))
}

/// Extract and validate an [`IndexedSession`] from a parsed `miro-ego.device-session.v1` JSON value.
pub fn parse_indexed_session_from_manifest(
    relative_dir: &str,
    parsed: &Value,
    manifest_sha256: &str,
) -> StorageResult<IndexedSession> {
    if !is_valid_sha256(manifest_sha256) {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "manifest_sha256 is invalid",
        ));
    }
    let obj = parsed
        .as_object()
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "manifest must be object"))?;

    ensure_allowed_keys(
        obj,
        &[
            "schema",
            "manifest_id",
            "sealed",
            "sealed_at",
            "session_id",
            "volume_id",
            "capture_mode",
            "display_name",
            "device",
            "time",
            "take",
            "camera",
            "video",
            "imu",
            "frames",
            "audio",
            "logs",
            "integrity",
            "capture_audit",
        ],
        "manifest",
    )?;

    if obj.get("schema").and_then(Value::as_str) != Some(DEVICE_SESSION_SCHEMA) {
        return Err(StorageError::integrity(
            "manifest_invalid",
            format!("manifest schema must be {DEVICE_SESSION_SCHEMA}"),
        ));
    }
    if !obj
        .get("manifest_id")
        .and_then(Value::as_str)
        .is_some_and(is_valid_uuid_v7)
    {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "manifest_id must be a valid UUIDv7",
        ));
    }
    if obj.get("sealed").and_then(Value::as_bool) != Some(true) {
        return Err(StorageError::integrity(
            "manifest_not_sealed",
            "manifest is not sealed",
        ));
    }
    if !obj
        .get("sealed_at")
        .and_then(Value::as_str)
        .is_some_and(|s| parse_rfc3339_utc_nanos(s).is_some())
    {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "sealed_at must be a valid RFC 3339 date-time",
        ));
    }

    let session_id = obj
        .get("session_id")
        .and_then(Value::as_str)
        .filter(|s| is_valid_uuid_v7(s))
        .ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "session_id must be a valid UUIDv7")
        })?
        .to_owned();

    if !obj
        .get("volume_id")
        .and_then(Value::as_str)
        .is_some_and(is_valid_uuid_v4)
    {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "volume_id must be a valid UUIDv4",
        ));
    }

    if !matches!(
        obj.get("capture_mode").and_then(Value::as_str),
        Some("production" | "calibration")
    ) {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "capture_mode must be 'production' or 'calibration'",
        ));
    }

    let display_name = obj
        .get("display_name")
        .and_then(Value::as_str)
        .filter(|s| {
            let count = s.chars().count();
            (1..=160).contains(&count)
        })
        .ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "display_name must be 1..=160 chars")
        })?
        .to_owned();

    let take = obj
        .get("take")
        .and_then(Value::as_object)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing take object"))?;
    ensure_allowed_keys(take, &["take_id", "sequence", "continuation_of"], "take")?;
    let take_id = take
        .get("take_id")
        .and_then(Value::as_str)
        .filter(|s| is_valid_uuid_v7(s))
        .ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "take.take_id must be a valid UUIDv7")
        })?
        .to_owned();
    let take_sequence = take
        .get("sequence")
        .and_then(Value::as_u64)
        .filter(|&seq| seq >= 1)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "take.sequence must be >= 1"))?;
    let continuation_of = match take.get("continuation_of") {
        Some(Value::Null) => {
            if take_sequence != 1 {
                return Err(StorageError::integrity(
                    "manifest_invalid",
                    "take.continuation_of is required when take.sequence >= 2",
                ));
            }
            None
        }
        Some(Value::String(prev)) if is_valid_uuid_v7(prev) => {
            if take_sequence < 2 {
                return Err(StorageError::integrity(
                    "manifest_invalid",
                    "take.sequence must be >= 2 when continuation_of is present",
                ));
            }
            Some(prev.clone())
        }
        _ => {
            return Err(StorageError::integrity(
                "manifest_invalid",
                "take.continuation_of must be present as null or UUIDv7",
            ));
        }
    };

    let device_obj = obj
        .get("device")
        .and_then(Value::as_object)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing device object"))?;
    ensure_allowed_keys(
        device_obj,
        &[
            "device_id",
            "device_label",
            "hardware_fingerprint",
            "platform",
            "software_version",
            "commit",
        ],
        "device",
    )?;
    let device_id = device_obj
        .get("device_id")
        .and_then(Value::as_str)
        .filter(|s| is_valid_uuid_v4(s))
        .ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "device.device_id must be UUIDv4")
        })?
        .to_owned();
    let device_label = device_obj
        .get("device_label")
        .and_then(Value::as_str)
        .filter(|s| miro_ego_core::config::is_valid_device_label(s))
        .ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "device.device_label is invalid")
        })?
        .to_owned();
    if !device_obj
        .get("hardware_fingerprint")
        .and_then(Value::as_str)
        .and_then(|s| s.strip_prefix("sha256:"))
        .is_some_and(is_valid_sha256)
    {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "device.hardware_fingerprint must match sha256:<64-lower-hex>",
        ));
    }
    if !device_obj
        .get("platform")
        .and_then(Value::as_str)
        .is_some_and(|s| (1..=128).contains(&s.chars().count()))
    {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "device.platform must be 1..=128 chars",
        ));
    }
    if !device_obj
        .get("software_version")
        .and_then(Value::as_str)
        .is_some_and(|s| (1..=64).contains(&s.chars().count()))
    {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "device.software_version must be 1..=64 chars",
        ));
    }
    if !device_obj
        .get("commit")
        .and_then(Value::as_str)
        .is_some_and(|s| {
            (40..=64).contains(&s.len())
                && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        })
    {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "device.commit must be 40..=64 lowercase hex characters",
        ));
    }

    let time_obj = obj
        .get("time")
        .and_then(Value::as_object)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing time object"))?;
    ensure_allowed_keys(
        time_obj,
        &[
            "started_at",
            "ended_at",
            "timezone",
            "duration_seconds",
            "duration_clock",
        ],
        "time",
    )?;
    let started_at = time_obj
        .get("started_at")
        .and_then(Value::as_str)
        .filter(|s| parse_rfc3339_utc_nanos(s).is_some())
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "invalid time.started_at"))?
        .to_owned();
    let ended_at = time_obj
        .get("ended_at")
        .and_then(Value::as_str)
        .filter(|s| parse_rfc3339_utc_nanos(s).is_some())
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "invalid time.ended_at"))?
        .to_owned();
    if !time_obj
        .get("timezone")
        .and_then(Value::as_str)
        .is_some_and(is_valid_iana_timezone)
    {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "invalid time.timezone",
        ));
    }
    let duration_seconds = time_obj
        .get("duration_seconds")
        .and_then(Value::as_f64)
        .filter(|d| d.is_finite() && *d >= 0.0)
        .ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "time.duration_seconds invalid")
        })?;
    if let Some(clock_val) = time_obj.get("duration_clock") {
        if clock_val.as_str() != Some("host_monotonic") {
            return Err(StorageError::integrity(
                "manifest_invalid",
                "time.duration_clock must be 'host_monotonic'",
            ));
        }
    }

    validate_manifest_camera_and_media(obj)?;

    let total_bytes = unique_downloadable_artifact_bytes(parsed)?;

    let recovered_prefix = if let Some(ca_val) = obj.get("capture_audit") {
        let ca_obj = ca_val.as_object().ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "capture_audit must be an object")
        })?;
        if ca_obj.get("schema").and_then(Value::as_str) != Some("miro-ego.capture-audit.v1") {
            return Err(StorageError::integrity(
                "manifest_invalid",
                "capture_audit.schema must be 'miro-ego.capture-audit.v1'",
            ));
        }
        ca_obj
            .get("recovery")
            .and_then(Value::as_object)
            .and_then(|rec| rec.get("recovered_prefix"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
    } else {
        false
    };

    Ok(IndexedSession {
        session_id,
        take_id,
        take_sequence,
        continuation_of,
        display_name,
        device: SessionDeviceIdentity {
            device_id,
            device_label,
        },
        relative_dir: relative_dir.to_owned(),
        started_at,
        ended_at,
        duration_seconds,
        total_bytes,
        manifest_sha256: manifest_sha256.to_owned(),
        state: SessionState::Sealed,
        recovered_prefix,
        verification: None,
    })
}

fn ensure_allowed_keys(
    obj: &Map<String, Value>,
    allowed: &[&str],
    context: &str,
) -> StorageResult<()> {
    for key in obj.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(StorageError::integrity(
                "manifest_invalid",
                format!("unexpected property '{key}' in {context}"),
            ));
        }
    }
    Ok(())
}

fn is_valid_iana_timezone(tz: &str) -> bool {
    let mut parts = tz.split('/');
    let Some(first) = parts.next() else {
        return false;
    };
    if first.is_empty() || !first.bytes().all(|b| b.is_ascii_alphabetic() || b == b'_') {
        return false;
    }
    let mut sub_count = 0_usize;
    for seg in parts {
        sub_count += 1;
        if seg.is_empty()
            || !seg
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'+' | b'.' | b'-'))
        {
            return false;
        }
    }
    sub_count >= 1
}

fn is_valid_artifact_role(role: &str) -> bool {
    if role.is_empty() || role.len() > 96 {
        return false;
    }
    let bytes = role.as_bytes();
    if !bytes[0].is_ascii_lowercase() {
        return false;
    }
    let mut saw_sep = false;
    let mut prev_sep = false;
    for &b in &bytes[1..] {
        if matches!(b, b'.' | b'_' | b'-') {
            if prev_sep {
                return false;
            }
            saw_sep = true;
            prev_sep = true;
        } else if b.is_ascii_lowercase() || b.is_ascii_digit() {
            prev_sep = false;
        } else {
            return false;
        }
    }
    saw_sep && !prev_sep
}

fn is_valid_media_type(mt: &str) -> bool {
    let Some((lhs, rhs)) = mt.split_once('/') else {
        return false;
    };
    let ok_part = |s: &str| {
        !s.is_empty()
            && !s.contains('/')
            && s.bytes().all(|b| {
                b.is_ascii_lowercase()
                    || b.is_ascii_digit()
                    || matches!(
                        b,
                        b'!' | b'#' | b'$' | b'&' | b'^' | b'_' | b'.' | b'+' | b'-'
                    )
            })
    };
    ok_part(lhs) && ok_part(rhs)
}

fn validate_closed_artifact(raw: &Value, expected_role: Option<&str>) -> StorageResult<()> {
    let obj = raw
        .as_object()
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "artifact must be object"))?;
    ensure_allowed_keys(
        obj,
        &[
            "artifact_id",
            "role",
            "path",
            "media_type",
            "bytes",
            "sha256",
            "storage_encoding",
        ],
        "artifact",
    )?;
    let role = obj
        .get("role")
        .and_then(Value::as_str)
        .filter(|r| is_valid_artifact_role(r))
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "invalid artifact.role"))?;
    if let Some(exp) = expected_role {
        if role != exp {
            return Err(StorageError::integrity(
                "manifest_invalid",
                format!("artifact.role '{role}' must be '{exp}'"),
            ));
        }
    }
    let media_type = obj
        .get("media_type")
        .and_then(Value::as_str)
        .filter(|m| is_valid_media_type(m))
        .ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "invalid artifact.media_type")
        })?;
    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing artifact.path"))?;
    crate::artifact_io::validate_artifact_relative_path(path)?;

    if let Some(se_val) = obj.get("storage_encoding") {
        let se = se_val.as_object().ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "storage_encoding must be object")
        })?;
        ensure_allowed_keys(
            se,
            &[
                "codec",
                "level",
                "block_bytes",
                "uncompressed_bytes",
                "uncompressed_sha256",
                "pcm_bytes",
                "pcm_sha256",
            ],
            "artifact.storage_encoding",
        )?;
        match se.get("codec").and_then(Value::as_str) {
            Some("zstd") => {
                if se.get("level").and_then(Value::as_u64) != Some(1)
                    || se.get("block_bytes").and_then(Value::as_u64) != Some(1_048_576)
                    || se
                        .get("uncompressed_bytes")
                        .and_then(Value::as_u64)
                        .is_none()
                    || !se
                        .get("uncompressed_sha256")
                        .and_then(Value::as_str)
                        .is_some_and(is_valid_sha256)
                {
                    return Err(StorageError::integrity(
                        "manifest_invalid",
                        "invalid zstd storage_encoding metadata",
                    ));
                }
            }
            Some("flac") => {
                if se.get("pcm_bytes").and_then(Value::as_u64).is_none()
                    || !se
                        .get("pcm_sha256")
                        .and_then(Value::as_str)
                        .is_some_and(is_valid_sha256)
                {
                    return Err(StorageError::integrity(
                        "manifest_invalid",
                        "invalid flac storage_encoding metadata",
                    ));
                }
            }
            _ => {
                return Err(StorageError::integrity(
                    "manifest_invalid",
                    "unsupported storage_encoding.codec",
                ));
            }
        }
    } else if media_type == "application/zstd" || media_type == "audio/flac" {
        return Err(StorageError::integrity(
            "manifest_invalid",
            format!("{media_type} artifact requires storage_encoding"),
        ));
    }

    if media_type == "application/zstd" && !path.ends_with(".ndjson.zst") {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "application/zstd artifact path must end with .ndjson.zst",
        ));
    }
    if media_type == "audio/flac" && !path.ends_with(".flac") {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "audio/flac artifact path must end with .flac",
        ));
    }
    Ok(())
}

fn validate_manifest_camera_and_media(obj: &Map<String, Value>) -> StorageResult<()> {
    // Camera section
    let cam = obj
        .get("camera")
        .and_then(Value::as_object)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing camera object"))?;
    ensure_allowed_keys(
        cam,
        &[
            "width",
            "height",
            "eye_width",
            "sensor_fps",
            "frame_decimation",
            "nominal_fps",
            "effective_fps",
            "coordinate_frame",
        ],
        "camera",
    )?;
    for int_key in ["width", "height", "eye_width", "frame_decimation"] {
        if cam.get(int_key).and_then(Value::as_u64).unwrap_or(0) < 1 {
            return Err(StorageError::integrity(
                "manifest_invalid",
                format!("camera.{int_key} must be an integer >= 1"),
            ));
        }
    }
    if !cam
        .get("sensor_fps")
        .and_then(Value::as_f64)
        .is_some_and(|v| v.is_finite() && v > 0.0)
    {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "camera.sensor_fps must be > 0",
        ));
    }
    if let Some(nom) = cam.get("nominal_fps") {
        if !nom.as_f64().is_some_and(|v| v.is_finite() && v > 0.0) {
            return Err(StorageError::integrity(
                "manifest_invalid",
                "camera.nominal_fps must be > 0",
            ));
        }
    }
    if !cam
        .get("effective_fps")
        .and_then(Value::as_f64)
        .is_some_and(|v| v.is_finite() && v >= 0.0)
    {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "camera.effective_fps must be >= 0",
        ));
    }
    if cam.get("coordinate_frame").and_then(Value::as_str) != Some("opencv_optical") {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "camera.coordinate_frame must be 'opencv_optical'",
        ));
    }

    // Video section
    let video = obj
        .get("video")
        .and_then(Value::as_object)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing video object"))?;
    ensure_allowed_keys(
        video,
        &["layout", "codec", "container", "segments", "encoding"],
        "video",
    )?;
    if video.get("layout").and_then(Value::as_str) != Some("split-eyes")
        || video.get("container").and_then(Value::as_str) != Some("mp4")
    {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "video must use split-eyes mp4 layout",
        ));
    }
    let video_codec = video
        .get("codec")
        .and_then(Value::as_str)
        .filter(|&c| c == "h264" || c == "hevc")
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "invalid video.codec"))?;

    let v_segments = video
        .get("segments")
        .and_then(Value::as_array)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            StorageError::integrity(
                "manifest_invalid",
                "video.segments must be a non-empty array",
            )
        })?;
    for seg in v_segments {
        let s_obj = seg.as_object().ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "video segment must be object")
        })?;
        ensure_allowed_keys(
            s_obj,
            &[
                "index",
                "start_frame",
                "end_frame",
                "start_time_seconds",
                "end_time_seconds",
                "artifacts",
            ],
            "video.segments[]",
        )?;
        if s_obj.get("index").and_then(Value::as_u64).is_none()
            || s_obj.get("start_frame").and_then(Value::as_u64).is_none()
            || s_obj.get("end_frame").and_then(Value::as_u64).unwrap_or(0) < 1
            || !s_obj
                .get("start_time_seconds")
                .and_then(Value::as_f64)
                .is_some_and(|v| v.is_finite() && v >= 0.0)
            || !s_obj
                .get("end_time_seconds")
                .and_then(Value::as_f64)
                .is_some_and(|v| v.is_finite() && v > 0.0)
        {
            return Err(StorageError::integrity(
                "manifest_invalid",
                "video segment frame/time bounds invalid",
            ));
        }
        let arts = s_obj
            .get("artifacts")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                StorageError::integrity("manifest_invalid", "missing video segment artifacts")
            })?;
        ensure_allowed_keys(arts, &["left", "right"], "video.segments[].artifacts")?;
        let left = arts.get("left").ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "missing video.left artifact")
        })?;
        let right = arts.get("right").ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "missing video.right artifact")
        })?;
        validate_closed_artifact(left, Some("video.left"))?;
        validate_closed_artifact(right, Some("video.right"))?;
        if left.get("media_type").and_then(Value::as_str) != Some("video/mp4")
            || right.get("media_type").and_then(Value::as_str) != Some("video/mp4")
        {
            return Err(StorageError::integrity(
                "manifest_invalid",
                "video segment media_type must be video/mp4",
            ));
        }
    }

    let enc = video
        .get("encoding")
        .and_then(Value::as_object)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing video.encoding"))?;
    ensure_allowed_keys(
        enc,
        &[
            "schema",
            "codec",
            "rate_control",
            "profile",
            "pixel_format",
            "bit_depth",
            "b_frames",
            "bitrate_kbps",
            "min_qp",
            "max_qp",
            "intra_qp",
            "initial_qp",
            "gop_frames",
            "vbv_ms",
        ],
        "video.encoding",
    )?;
    let expected_profile = if video_codec == "h264" {
        "high"
    } else {
        "main"
    };
    if enc.get("schema").and_then(Value::as_str) != Some("miro-ego.recording-encoding.v1")
        || enc.get("codec").and_then(Value::as_str) != Some(video_codec)
        || enc.get("profile").and_then(Value::as_str) != Some(expected_profile)
        || enc.get("pixel_format").and_then(Value::as_str) != Some("yuv420p")
        || enc.get("bit_depth").and_then(Value::as_u64) != Some(8)
        || enc.get("b_frames").and_then(Value::as_u64) != Some(0)
        || !enc
            .get("intra_qp")
            .and_then(Value::as_u64)
            .is_some_and(|v| v <= 51)
        || !enc
            .get("gop_frames")
            .and_then(Value::as_u64)
            .is_some_and(|v| (1..=300).contains(&v))
    {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "video.encoding fields violate schema constraints",
        ));
    }
    match enc.get("rate_control").and_then(Value::as_str) {
        Some("cbr" | "avbr") => {
            if !enc
                .get("bitrate_kbps")
                .and_then(Value::as_u64)
                .is_some_and(|v| (1..=100_000).contains(&v))
                || !enc
                    .get("min_qp")
                    .and_then(Value::as_u64)
                    .is_some_and(|v| v <= 51)
                || !enc
                    .get("max_qp")
                    .and_then(Value::as_u64)
                    .is_some_and(|v| v <= 51)
                || !enc
                    .get("initial_qp")
                    .and_then(Value::as_u64)
                    .is_some_and(|v| v <= 51)
                || !enc
                    .get("vbv_ms")
                    .and_then(Value::as_u64)
                    .is_some_and(|v| (1..=3000).contains(&v))
            {
                return Err(StorageError::integrity(
                    "manifest_invalid",
                    "cbr/avbr video.encoding parameters invalid",
                ));
            }
        }
        Some("vbr") => {
            if !matches!(enc.get("bitrate_kbps"), Some(Value::Null))
                || !matches!(enc.get("min_qp"), Some(Value::Null))
                || !matches!(enc.get("max_qp"), Some(Value::Null))
                || !matches!(enc.get("vbv_ms"), Some(Value::Null))
                || !matches!(enc.get("initial_qp"), Some(Value::Null))
            {
                return Err(StorageError::integrity(
                    "manifest_invalid",
                    "vbr video.encoding requires null rate-control bounds",
                ));
            }
        }
        Some("fixqp") => {
            if !matches!(enc.get("bitrate_kbps"), Some(Value::Null))
                || !matches!(enc.get("min_qp"), Some(Value::Null))
                || !matches!(enc.get("max_qp"), Some(Value::Null))
                || !matches!(enc.get("vbv_ms"), Some(Value::Null))
                || !enc
                    .get("initial_qp")
                    .and_then(Value::as_u64)
                    .is_some_and(|v| v <= 51)
            {
                return Err(StorageError::integrity(
                    "manifest_invalid",
                    "fixqp video.encoding parameters invalid",
                ));
            }
        }
        _ => {
            return Err(StorageError::integrity(
                "manifest_invalid",
                "invalid video.encoding.rate_control",
            ));
        }
    }

    // IMU & Frames sections
    let imu = obj
        .get("imu")
        .and_then(Value::as_object)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing imu object"))?;
    ensure_allowed_keys(
        imu,
        &["artifact", "sample_count", "units", "coordinate_frame"],
        "imu",
    )?;
    if imu.get("sample_count").and_then(Value::as_u64).is_none()
        || imu.get("units").and_then(Value::as_str) != Some("raw_int16")
        || imu.get("coordinate_frame").and_then(Value::as_str) != Some("raw_device_axes")
    {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "imu metadata invalid",
        ));
    }
    let imu_art = imu
        .get("artifact")
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing imu.artifact"))?;
    validate_closed_artifact(imu_art, Some("imu.samples"))?;
    if !matches!(
        imu_art.get("media_type").and_then(Value::as_str),
        Some("application/x-ndjson" | "application/zstd")
    ) {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "invalid imu.artifact.media_type",
        ));
    }

    let frames = obj
        .get("frames")
        .and_then(Value::as_object)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing frames object"))?;
    ensure_allowed_keys(frames, &["artifact", "count"], "frames")?;
    if frames.get("count").and_then(Value::as_u64).is_none() {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "frames.count must be integer >= 0",
        ));
    }
    let frames_art = frames
        .get("artifact")
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing frames.artifact"))?;
    validate_closed_artifact(frames_art, Some("frames.index"))?;
    if !matches!(
        frames_art.get("media_type").and_then(Value::as_str),
        Some("application/x-ndjson" | "application/zstd")
    ) {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "invalid frames.artifact.media_type",
        ));
    }

    // Audio section
    let audio = obj
        .get("audio")
        .and_then(Value::as_object)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing audio object"))?;
    match audio.get("state").and_then(Value::as_str) {
        Some("not_recorded") => {
            ensure_allowed_keys(
                audio,
                &["state", "requested_mode", "resolved_mode", "reason"],
                "audio",
            )?;
            if !matches!(
                audio.get("requested_mode").and_then(Value::as_str),
                Some("device_default" | "disabled" | "enabled")
            ) || audio.get("resolved_mode").and_then(Value::as_str) != Some("disabled")
                || !matches!(
                    audio.get("reason").and_then(Value::as_str),
                    Some(
                        "user_disabled"
                            | "device_default_disabled"
                            | "recording_interrupted"
                            | "audio_open_failed"
                    )
                )
            {
                return Err(StorageError::integrity(
                    "manifest_invalid",
                    "not_recorded audio fields invalid",
                ));
            }
        }
        Some("recorded") => {
            ensure_allowed_keys(
                audio,
                &[
                    "state",
                    "requested_mode",
                    "resolved_mode",
                    "codec",
                    "container",
                    "sample_format",
                    "sample_rate",
                    "channels",
                    "sample_count",
                    "sync",
                    "segments",
                    "capture_clock",
                ],
                "audio",
            )?;
            if !matches!(
                audio.get("requested_mode").and_then(Value::as_str),
                Some("device_default" | "enabled")
            ) || audio.get("resolved_mode").and_then(Value::as_str) != Some("enabled")
                || audio.get("codec").and_then(Value::as_str) != Some("pcm_s16le")
                || audio.get("container").and_then(Value::as_str) != Some("wav")
                || audio.get("sample_format").and_then(Value::as_str) != Some("S16_LE")
                || !audio
                    .get("sample_rate")
                    .and_then(Value::as_u64)
                    .is_some_and(|v| (8_000..=384_000).contains(&v))
                || !audio
                    .get("channels")
                    .and_then(Value::as_u64)
                    .is_some_and(|v| (1..=8).contains(&v))
                || audio
                    .get("sample_count")
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
                    < 1
            {
                return Err(StorageError::integrity(
                    "manifest_invalid",
                    "recorded audio metadata invalid",
                ));
            }
            let sync = audio
                .get("sync")
                .and_then(Value::as_object)
                .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing audio.sync"))?;
            ensure_allowed_keys(
                sync,
                &[
                    "time_base",
                    "start_time_seconds",
                    "end_time_seconds",
                    "video_time_reference",
                ],
                "audio.sync",
            )?;
            if sync.get("time_base").and_then(Value::as_str) != Some("host_monotonic")
                || sync.get("video_time_reference").and_then(Value::as_str)
                    != Some("session_time_seconds")
                || !sync
                    .get("start_time_seconds")
                    .and_then(Value::as_f64)
                    .is_some_and(|v| v.is_finite() && v >= 0.0)
                || !sync
                    .get("end_time_seconds")
                    .and_then(Value::as_f64)
                    .is_some_and(|v| v.is_finite() && v > 0.0)
            {
                return Err(StorageError::integrity(
                    "manifest_invalid",
                    "audio.sync invalid",
                ));
            }
            if let Some(cc) = audio.get("capture_clock") {
                if cc
                    .as_object()
                    .and_then(|o| o.get("schema"))
                    .and_then(Value::as_str)
                    != Some("miro-ego.audio-clock.v1")
                {
                    return Err(StorageError::integrity(
                        "manifest_invalid",
                        "invalid audio.capture_clock",
                    ));
                }
            }
            let a_segs = audio
                .get("segments")
                .and_then(Value::as_array)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    StorageError::integrity("manifest_invalid", "audio.segments must be non-empty")
                })?;
            for aseg in a_segs {
                let as_obj = aseg.as_object().ok_or_else(|| {
                    StorageError::integrity("manifest_invalid", "audio segment must be object")
                })?;
                ensure_allowed_keys(
                    as_obj,
                    &[
                        "index",
                        "start_sample",
                        "end_sample",
                        "start_time_seconds",
                        "end_time_seconds",
                        "pcm_payload_bytes",
                        "wav_header_bytes",
                        "artifact",
                    ],
                    "audio.segments[]",
                )?;
                if as_obj.get("index").and_then(Value::as_u64).is_none()
                    || as_obj.get("start_sample").and_then(Value::as_u64).is_none()
                    || as_obj
                        .get("end_sample")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                        < 1
                    || as_obj
                        .get("pcm_payload_bytes")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                        < 1
                    || !as_obj
                        .get("wav_header_bytes")
                        .and_then(Value::as_u64)
                        .is_some_and(|v| (44..=65536).contains(&v))
                    || !as_obj
                        .get("start_time_seconds")
                        .and_then(Value::as_f64)
                        .is_some_and(|v| v.is_finite() && v >= 0.0)
                    || !as_obj
                        .get("end_time_seconds")
                        .and_then(Value::as_f64)
                        .is_some_and(|v| v.is_finite() && v > 0.0)
                {
                    return Err(StorageError::integrity(
                        "manifest_invalid",
                        "audio segment bounds invalid",
                    ));
                }
                let art = as_obj.get("artifact").ok_or_else(|| {
                    StorageError::integrity("manifest_invalid", "missing audio segment artifact")
                })?;
                validate_closed_artifact(art, Some("audio.wav"))?;
                if !matches!(
                    art.get("media_type").and_then(Value::as_str),
                    Some("audio/wav" | "audio/flac")
                ) {
                    return Err(StorageError::integrity(
                        "manifest_invalid",
                        "invalid audio segment media_type",
                    ));
                }
            }
        }
        _ => {
            return Err(StorageError::integrity(
                "manifest_invalid",
                "audio.state must be 'recorded' or 'not_recorded'",
            ));
        }
    }

    // Logs section
    let logs = obj
        .get("logs")
        .and_then(Value::as_array)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing logs array"))?;
    for log_item in logs {
        validate_closed_artifact(log_item, None)?;
        let role = log_item
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !role.starts_with("log.") && !role.starts_with("log_") && !role.starts_with("log-") {
            return Err(StorageError::integrity(
                "manifest_invalid",
                "logs[].role must match log[._-]*",
            ));
        }
    }

    // Integrity section
    let integ = obj
        .get("integrity")
        .and_then(Value::as_object)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing integrity object"))?;
    ensure_allowed_keys(
        integ,
        &[
            "verified_at",
            "dropped_frames",
            "drop_events",
            "quality_policy",
            "media_write_throughput_bytes_per_second",
            "fatal_errors",
        ],
        "integrity",
    )?;
    if !integ
        .get("verified_at")
        .and_then(Value::as_str)
        .is_some_and(|s| parse_rfc3339_utc_nanos(s).is_some())
        || integ
            .get("dropped_frames")
            .and_then(Value::as_u64)
            .is_none()
        || !integ
            .get("fatal_errors")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
    {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "integrity fields invalid or fatal_errors non-empty",
        ));
    }
    let drop_events = integ
        .get("drop_events")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "missing integrity.drop_events")
        })?;
    for de in drop_events {
        let de_obj = de.as_object().ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "drop_event must be object")
        })?;
        ensure_allowed_keys(
            de_obj,
            &[
                "start_frame",
                "end_frame",
                "at_time_seconds",
                "reason",
                "dropped",
            ],
            "integrity.drop_events[]",
        )?;
        if de_obj.get("start_frame").and_then(Value::as_u64).is_none()
            || de_obj.get("end_frame").and_then(Value::as_u64).unwrap_or(0) < 1
            || !de_obj
                .get("at_time_seconds")
                .and_then(Value::as_f64)
                .is_some_and(|v| v.is_finite() && v >= 0.0)
            || de_obj.get("reason").and_then(Value::as_str) != Some("write_backpressure")
            || de_obj.get("dropped").and_then(Value::as_u64).unwrap_or(0) < 1
        {
            return Err(StorageError::integrity(
                "manifest_invalid",
                "invalid integrity.drop_events entry",
            ));
        }
    }
    if let Some(tput) = integ.get("media_write_throughput_bytes_per_second") {
        if tput.as_u64().is_none() {
            return Err(StorageError::integrity(
                "manifest_invalid",
                "media_write_throughput_bytes_per_second must be integer >= 0",
            ));
        }
    }
    if let Some(qp) = integ.get("quality_policy") {
        let qp_obj = qp.as_object().ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "quality_policy must be object")
        })?;
        ensure_allowed_keys(
            qp_obj,
            &[
                "policy_id",
                "max_contiguous_dropped_frames",
                "max_total_dropped_frames",
                "max_drop_fraction",
                "window_seconds",
                "max_dropped_frames_per_window",
            ],
            "integrity.quality_policy",
        )?;
        if qp_obj.get("policy_id").and_then(Value::as_str) != Some("rdk-x5-lossless-v1")
            || qp_obj
                .get("max_contiguous_dropped_frames")
                .and_then(Value::as_u64)
                != Some(0)
            || qp_obj
                .get("max_total_dropped_frames")
                .and_then(Value::as_u64)
                != Some(0)
            || qp_obj.get("max_drop_fraction").and_then(Value::as_f64) != Some(0.0)
            || !qp_obj
                .get("window_seconds")
                .and_then(Value::as_f64)
                .is_some_and(|v| v.is_finite() && v > 0.0)
            || qp_obj
                .get("max_dropped_frames_per_window")
                .and_then(Value::as_u64)
                != Some(0)
        {
            return Err(StorageError::integrity(
                "manifest_invalid",
                "integrity.quality_policy violates rdk-x5-lossless-v1 constraints",
            ));
        }
    }

    Ok(())
}
