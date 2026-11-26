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
    open_manifest_in_dir, open_pinned_directory, unique_downloadable_artifact_bytes,
};
use crate::util::{
    UNKNOWN_DATE_DIR_NAME, base64url_decode, base64url_encode, crc32_ieee, domain_sha256,
    is_date_dir_name, is_time_dir_name, is_unknown_time_dir_name, now_rfc3339_utc,
};
use crate::{
    CATALOG_BINARY_MAGIC, CATALOG_FORMAT_VERSION, CatalogEntry, SessionCatalog, SessionState,
    StorageError, StorageResult,
};
use miro_ego_core::CoreError;
use miro_ego_core::ids::{is_valid_sha256, is_valid_uuid_v4, is_valid_uuid_v7, new_uuid_v4};
use miro_ego_core::paths::DataVolumePaths;
use miro_ego_core::sys::fs::{
    fstat, fstatat_nofollow, is_exfat_or_msdos, open_directory_safe, openat_retry,
    read_bounded_bytes, write_bytes_atomic,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::ffi::CString;
use std::fs;
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::sync::RwLock;

pub const SESSION_LIST_SCHEMA: &str = "miro-ego.session-list.v1";
pub const DEVICE_SESSION_SCHEMA: &str = "miro-ego.device-session.v1";
pub const DEFAULT_PAGE_LIMIT: usize = 50;
pub const MAX_PAGE_LIMIT: usize = 200;
/// Hard cap for `/data/.miro-ego/index/catalog.bin` (16 MiB).
pub const MAX_CATALOG_BIN_BYTES: usize = 16 * 1024 * 1024;
const CATALOG_HEADER_BYTES: usize = 20;

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
#[derive(Debug, Default)]
pub struct CatalogIndex {
    revision_seq: u64,
    catalog_revision: String,
    by_id: HashMap<String, IndexedSession>,
    by_path: HashMap<String, String>,
    diagnostics_by_path: BTreeMap<String, SessionDiscoveryDiagnostic>,
}

impl CatalogIndex {
    fn new_empty() -> Self {
        let mut idx = Self::default();
        idx.recompute_revision();
        idx
    }

    fn recompute_revision(&mut self) {
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
        let mut items: Vec<&IndexedSession> = self.by_id.values().collect();
        // Deterministic newest-first ordering by started_at, then session_id descending.
        items.sort_by(|a, b| {
            b.started_at
                .cmp(&a.started_at)
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
            diagnostics_by_path: BTreeMap::new(),
        };
        for entry in payload.entries {
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
            idx.diagnostics_by_path
                .insert(rec.relative_dir, rec.diagnostic);
        }
        idx.recompute_revision();
        Ok(idx)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PageCursorToken {
    rev: String,
    offset: usize,
    take_id: Option<String>,
    include_recovery: bool,
}

/// High-speed in-memory session catalog backed by `/data/.miro-ego/index/catalog.bin`.
pub struct BinarySessionCatalog {
    paths: DataVolumePaths,
    index: RwLock<CatalogIndex>,
}

impl BinarySessionCatalog {
    /// Open the catalog: loads `catalog.bin` when valid, or automatically rebuilds from
    /// `/data/recordings/` if `catalog.bin` is missing or corrupt.
    pub fn open(paths: DataVolumePaths) -> StorageResult<Self> {
        let catalog = Self {
            paths,
            index: RwLock::new(CatalogIndex::new_empty()),
        };
        match catalog.load_snapshot_from_disk() {
            Ok(loaded) => {
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
                // Missing or corrupt catalog.bin -> auto-rebuild cleanly from recordings/.
                if catalog.paths.recordings_dir().is_dir() {
                    let _ = catalog.rebuild_index()?;
                }
                Ok(catalog)
            }
        }
    }

    /// Create an empty catalog instance without triggering an initial scan.
    pub fn new_empty(paths: DataVolumePaths) -> Self {
        Self {
            paths,
            index: RwLock::new(CatalogIndex::new_empty()),
        }
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
    /// catalog integrity.
    pub fn admit_indexed_session(&self, entry: IndexedSession) -> StorageResult<()> {
        let snapshot_bytes = {
            let mut guard = self
                .index
                .write()
                .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;

            if let Some(existing) = guard.by_id.get(&entry.session_id) {
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
            if let Some(existing_id) = guard.by_path.get(&entry.relative_dir) {
                if existing_id != &entry.session_id {
                    return Err(StorageError::conflict(
                        "duplicate_session_path",
                        "directory path already belongs to another session_id",
                    ));
                }
            }

            guard.diagnostics_by_path.remove(&entry.relative_dir);
            guard
                .by_path
                .insert(entry.relative_dir.clone(), entry.session_id.clone());
            guard.by_id.insert(entry.session_id.clone(), entry);
            guard.bump_revision();
            encode_catalog_binary(&guard.to_payload())?
        };

        self.write_snapshot_bytes(&snapshot_bytes)
    }

    /// Parse and admit one sealed session directory under `/data/recordings/<relative_dir>`.
    pub fn admit_from_session_dir(&self, relative_dir: &str) -> StorageResult<IndexedSession> {
        let (dir_file, root_dev, exfat) =
            open_pinned_directory(&self.paths.recordings_dir(), relative_dir)?;
        let opened = open_manifest_in_dir(&dir_file, root_dev, exfat, None)?;
        let indexed =
            parse_indexed_session_from_manifest(relative_dir, &opened.parsed, &opened.sha256)?;
        self.admit_indexed_session(indexed.clone())?;
        Ok(indexed)
    }

    /// Update or clear the gateway verification verdict for an indexed session.
    pub fn set_verification(
        &self,
        session_id: &str,
        verification: Option<GatewayVerification>,
    ) -> StorageResult<bool> {
        let snapshot_bytes = {
            let mut guard = self
                .index
                .write()
                .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
            let Some(entry) = guard.by_id.get_mut(session_id) else {
                return Ok(false);
            };
            entry.verification = verification;
            guard.bump_revision();
            encode_catalog_binary(&guard.to_payload())?
        };
        self.write_snapshot_bytes(&snapshot_bytes)?;
        Ok(true)
    }

    /// Remove any indexed session or discovery diagnostic associated with `relative_dir`,
    /// monotonically bumping `catalog_revision` and persisting `catalog.bin`.
    pub fn forget_path(&self, relative_dir: &str) -> StorageResult<bool> {
        let snapshot_bytes = {
            let mut guard = self
                .index
                .write()
                .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
            let removed_id = guard.by_path.remove(relative_dir);
            let removed_entry = removed_id
                .as_ref()
                .and_then(|sid| guard.by_id.remove(sid))
                .is_some();
            let removed_diag = guard.diagnostics_by_path.remove(relative_dir).is_some();
            if !removed_entry && !removed_diag {
                return Ok(false);
            }
            guard.bump_revision();
            encode_catalog_binary(&guard.to_payload())?
        };
        self.write_snapshot_bytes(&snapshot_bytes)?;
        Ok(true)
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

    fn load_snapshot_from_disk(&self) -> StorageResult<CatalogIndex> {
        let path = self.paths.catalog_file();
        let raw = read_bounded_bytes(&path, MAX_CATALOG_BIN_BYTES).map_err(|err| {
            StorageError::Catalog {
                message: format!("unable to read catalog.bin: {err}"),
            }
        })?;
        let payload = decode_catalog_binary(&raw)?;
        CatalogIndex::from_payload(payload)
    }

    fn write_snapshot_bytes(&self, bytes: &[u8]) -> StorageResult<()> {
        let index_dir = self.paths.index_dir();
        if !index_dir.is_dir() {
            // Only create `index/` if parent `.miro-ego/` already exists on the mounted volume.
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
        })
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
        let snapshot_bytes = {
            let mut guard = self
                .index
                .write()
                .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
            let Some(removed) = guard.by_id.remove(session_id) else {
                return Ok(false);
            };
            guard.by_path.remove(&removed.relative_dir);
            guard.bump_revision();
            encode_catalog_binary(&guard.to_payload())?
        };
        self.write_snapshot_bytes(&snapshot_bytes)?;
        Ok(true)
    }

    fn rebuild_index(&self) -> StorageResult<usize> {
        let recordings_root = self.paths.recordings_dir();
        let (scanned_sessions, scanned_diags) = scan_recordings_tree(&recordings_root)?;

        let (count, snapshot_bytes) = {
            let mut guard = self
                .index
                .write()
                .map_err(|_| StorageError::Core(CoreError::poisoned("catalog_index")))?;
            let next_rev_seq = guard.revision_seq.saturating_add(1);

            let mut by_id: HashMap<String, IndexedSession> = HashMap::new();
            let mut by_path: HashMap<String, String> = HashMap::new();
            let mut diagnostics_by_path: BTreeMap<String, SessionDiscoveryDiagnostic> =
                scanned_diags.into_iter().collect();
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
                    diagnostics_by_path.insert(
                        rel,
                        SessionDiscoveryDiagnostic {
                            quarantine_id: new_uuid_v4(),
                            code: DiscoveryDiagnosticCode::ManifestInvalid,
                            observed_at: now_rfc3339_utc(),
                            message: "duplicate session identity detected across multiple recording directories".to_owned(),
                        },
                    );
                }
            }

            // Preserve any previously verified gateway verdicts whose manifest_sha256 still matches.
            for (sid, session) in &mut by_id {
                if let Some(prev) = guard.by_id.get(sid) {
                    if prev.manifest_sha256 == session.manifest_sha256 {
                        session.verification = prev.verification.clone();
                    }
                }
            }

            let count = by_id.len();
            guard.revision_seq = next_rev_seq;
            guard.by_id = by_id;
            guard.by_path = by_path;
            guard.diagnostics_by_path = diagnostics_by_path;
            guard.recompute_revision();

            (count, encode_catalog_binary(&guard.to_payload())?)
        };

        self.write_snapshot_bytes(&snapshot_bytes)?;
        Ok(count)
    }
}

/// Encode a `CatalogSnapshotPayload` into the framed `MEGOCAT1` binary format with CRC32.
fn encode_catalog_binary(payload: &CatalogSnapshotPayload) -> StorageResult<Vec<u8>> {
    let json_bytes = serde_json::to_vec(payload).map_err(|err| StorageError::Catalog {
        message: format!("serialize catalog payload failed: {err}"),
    })?;
    let total_len = CATALOG_HEADER_BYTES.saturating_add(json_bytes.len());
    if total_len > MAX_CATALOG_BIN_BYTES {
        return Err(StorageError::Catalog {
            message: "catalog snapshot exceeds maximum 16 MiB size".to_owned(),
        });
    }
    let payload_len = u32::try_from(json_bytes.len()).map_err(|_| StorageError::Catalog {
        message: "catalog payload length overflow".to_owned(),
    })?;
    let crc = crc32_ieee(&json_bytes);

    let mut out = Vec::with_capacity(total_len);
    out.extend_from_slice(&CATALOG_BINARY_MAGIC);
    out.extend_from_slice(&CATALOG_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&json_bytes);
    Ok(out)
}

/// Decode and strictly verify a `MEGOCAT1` binary catalog file (magic, version, length, CRC32).
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
    if version != CATALOG_FORMAT_VERSION {
        return Err(StorageError::Catalog {
            message: format!("unsupported catalog.bin format version {version}"),
        });
    }
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
    serde_json::from_slice(payload_slice).map_err(|err| StorageError::Catalog {
        message: format!("catalog.bin payload decode failed: {err}"),
    })
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
fn scan_recordings_tree(recordings_root: &Path) -> StorageResult<ScannedRecordings> {
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
            let valid_child = if is_dated {
                is_time_dir_name(&child)
            } else {
                is_unknown_time_dir_name(&child)
            };
            if !valid_child {
                continue;
            }
            let rel_dir = format!("{day_name}/{child}");
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
                        Ok(indexed) => sessions.push(indexed),
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

    if obj.get("schema").and_then(Value::as_str) != Some(DEVICE_SESSION_SCHEMA) {
        return Err(StorageError::integrity(
            "manifest_invalid",
            format!("manifest schema must be {DEVICE_SESSION_SCHEMA}"),
        ));
    }
    if obj.get("sealed").and_then(Value::as_bool) != Some(true) {
        return Err(StorageError::integrity(
            "manifest_not_sealed",
            "manifest is not sealed",
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

    let display_name = obj
        .get("display_name")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && s.len() <= 160)
        .ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "display_name must be 1..=160 chars")
        })?
        .to_owned();

    let take = obj
        .get("take")
        .and_then(Value::as_object)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing take object"))?;
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
        Some(Value::Null) | None => {
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
                "take.continuation_of must be null or UUIDv7",
            ));
        }
    };

    let device_obj = obj
        .get("device")
        .and_then(Value::as_object)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing device object"))?;
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

    let time_obj = obj
        .get("time")
        .and_then(Value::as_object)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing time object"))?;
    let started_at = time_obj
        .get("started_at")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing time.started_at"))?
        .to_owned();
    let ended_at = time_obj
        .get("ended_at")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing time.ended_at"))?
        .to_owned();
    let duration_seconds = time_obj
        .get("duration_seconds")
        .and_then(Value::as_f64)
        .filter(|d| d.is_finite() && *d >= 0.0)
        .ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "time.duration_seconds invalid")
        })?;

    let total_bytes = unique_downloadable_artifact_bytes(parsed)?;

    let recovered_prefix = obj
        .get("capture_audit")
        .and_then(Value::as_object)
        .and_then(|ca| ca.get("recovery"))
        .and_then(Value::as_object)
        .and_then(|rec| rec.get("recovered_prefix"))
        .and_then(Value::as_bool)
        .unwrap_or(false);

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
