//! exFAT recording-volume management, binary session catalog, and crash recovery traits
//! for Miro-Ego 0.3.0.

use miro_ego_core::CoreError;
use miro_ego_core::paths::DataVolumePaths;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use thiserror::Error;

/// Binary catalog header magic stored in `/data/.miro-ego/index/catalog.bin`.
pub const CATALOG_BINARY_MAGIC: [u8; 8] = *b"MEGOCAT1";
pub const CATALOG_FORMAT_VERSION: u32 = 1;
pub const VOLUME_METADATA_SCHEMA: &str = "miro-ego.volume.v1";

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("storage unavailable: {message}")]
    Unavailable { message: String },

    #[error("volume verification failed: {message}")]
    VolumeInvalid { message: String },

    #[error("session not found: {session_id}")]
    NotFound { session_id: String },

    #[error("catalog error: {message}")]
    Catalog { message: String },

    #[error(transparent)]
    Core(#[from] CoreError),
}

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeInfo {
    pub label: String,
    pub mountpoint: PathBuf,
    pub uuid: String,
    pub partuuid: String,
    pub is_exfat: bool,
    pub total_bytes: u64,
    pub available_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Sealed,
    Staging,
    Quarantined,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub session_id: String,
    pub display_name: String,
    pub relative_dir: String,
    pub started_at: String,
    pub duration_seconds: f64,
    pub total_bytes: u64,
    pub manifest_sha256: String,
    pub state: SessionState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryReport {
    pub scanned: usize,
    pub recovered: usize,
    pub quarantined: usize,
}

/// Manages mounting/verification and canonical layout of the `MIRO-EGO` exFAT recording volume.
pub trait VolumeManager: Send + Sync {
    fn paths(&self) -> &DataVolumePaths;
    fn inspect_volume(&self) -> StorageResult<VolumeInfo>;
    fn ensure_layout(&self) -> StorageResult<bool>;
}

/// Binary index and query interface for sealed sessions under `/data/recordings/`.
pub trait SessionCatalog: Send + Sync {
    fn list_sessions(&self) -> StorageResult<Vec<CatalogEntry>>;
    fn get_session(&self, session_id: &str) -> StorageResult<Option<CatalogEntry>>;
    fn admit_session(&self, entry: CatalogEntry) -> StorageResult<()>;
    fn remove_session(&self, session_id: &str) -> StorageResult<bool>;
    fn rebuild_index(&self) -> StorageResult<usize>;
}

/// Recovers or quarantines interrupted captures in `/data/.miro-ego/staging/`.
pub trait RecoveryManager: Send + Sync {
    fn recover_staging(&self) -> StorageResult<RecoveryReport>;
}
