//! ExFAT recording-volume management, atomic binary session catalog (`catalog.bin`),
//! explicit-FSM capture coordinator, crash prefix recovery, safe artifact/log I/O,
//! and bounded rotating audit logging for Miro-Ego 0.3.0.

pub mod artifact_io;
pub mod audit;
pub mod catalog;
pub mod coordinator;
pub mod logs;
pub mod recovery;
pub mod util;
pub mod volume;

use miro_ego_core::CoreError;
use miro_ego_core::paths::DataVolumePaths;
use serde::{Deserialize, Serialize};
use std::io;
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

    #[error("{code}: {message}")]
    ServiceUnavailable { code: &'static str, message: String },

    #[error("volume verification failed: {message}")]
    VolumeInvalid { message: String },

    #[error("session not found: {session_id}")]
    NotFound { session_id: String },

    #[error("catalog error: {message}")]
    Catalog { message: String },

    #[error("catalog changed (current revision: {catalog_revision})")]
    CatalogChanged { catalog_revision: String },

    #[error("{code}: {message}")]
    InvalidInput { code: &'static str, message: String },

    #[error("{code}: {message}")]
    Conflict { code: &'static str, message: String },

    #[error("{code}: {message}")]
    Unprocessable { code: &'static str, message: String },

    #[error("{code}: {message}")]
    Locked { code: &'static str, message: String },

    #[error("{code}: {message}")]
    Unsupported { code: &'static str, message: String },

    #[error("{code}: {message}")]
    Integrity { code: &'static str, message: String },

    #[error("byte range not satisfiable for total length {total_bytes}")]
    RangeNotSatisfiable { total_bytes: u64 },

    #[error("{code} ({context}): {source}")]
    Io {
        code: &'static str,
        context: String,
        #[source]
        source: io::Error,
    },

    #[error(transparent)]
    Core(#[from] CoreError),
}

impl StorageError {
    pub fn service_unavailable(code: &'static str, message: impl Into<String>) -> Self {
        Self::ServiceUnavailable {
            code,
            message: message.into(),
        }
    }

    pub fn camera_not_connected(message: impl Into<String>) -> Self {
        Self::service_unavailable("camera_not_connected", message)
    }

    pub fn invalid_input(code: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidInput {
            code,
            message: message.into(),
        }
    }

    pub fn conflict(code: &'static str, message: impl Into<String>) -> Self {
        Self::Conflict {
            code,
            message: message.into(),
        }
    }

    pub fn unprocessable(code: &'static str, message: impl Into<String>) -> Self {
        Self::Unprocessable {
            code,
            message: message.into(),
        }
    }

    pub fn locked(code: &'static str, message: impl Into<String>) -> Self {
        Self::Locked {
            code,
            message: message.into(),
        }
    }

    pub fn unsupported(code: &'static str, message: impl Into<String>) -> Self {
        Self::Unsupported {
            code,
            message: message.into(),
        }
    }

    pub fn integrity(code: &'static str, message: impl Into<String>) -> Self {
        Self::Integrity {
            code,
            message: message.into(),
        }
    }

    pub fn io(code: &'static str, context: impl Into<String>, source: io::Error) -> Self {
        Self::Io {
            code,
            context: context.into(),
            source,
        }
    }

    pub fn code(&self) -> &str {
        match self {
            Self::Unavailable { .. } => "storage_unavailable",
            Self::ServiceUnavailable { code, .. } => code,
            Self::VolumeInvalid { .. } => "volume_invalid",
            Self::NotFound { .. } => "not_found",
            Self::Catalog { .. } => "catalog_error",
            Self::CatalogChanged { .. } => "catalog_changed",
            Self::InvalidInput { code, .. }
            | Self::Conflict { code, .. }
            | Self::Unprocessable { code, .. }
            | Self::Locked { code, .. }
            | Self::Unsupported { code, .. }
            | Self::Integrity { code, .. }
            | Self::Io { code, .. } => code,
            Self::RangeNotSatisfiable { .. } => "range_not_satisfiable",
            Self::Core(err) => err.code(),
        }
    }

    pub fn http_status(&self) -> u16 {
        match self {
            Self::Unavailable { .. } | Self::ServiceUnavailable { .. } => 503,
            Self::VolumeInvalid { .. } | Self::Catalog { .. } | Self::Io { .. } => 500,
            Self::NotFound { .. } | Self::Unsupported { .. } => 404,
            Self::CatalogChanged { .. } | Self::Conflict { .. } | Self::Integrity { .. } => 409,
            Self::InvalidInput { .. } => 400,
            Self::Unprocessable { .. } => 422,
            Self::Locked { .. } => 423,
            Self::RangeNotSatisfiable { .. } => 416,
            Self::Core(err) => match err {
                CoreError::InvalidInput { .. } | CoreError::InvalidConfig { .. } => 400,
                CoreError::LockBusy { .. } => 409,
                CoreError::Io { .. } | CoreError::Poisoned { .. } => 500,
            },
        }
    }

    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Self::Unavailable { .. }
                | Self::ServiceUnavailable { .. }
                | Self::CatalogChanged { .. }
                | Self::Locked { .. }
        )
    }
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
    fn is_volume_released(&self) -> bool {
        false
    }
    fn set_volume_released(&self, _released: bool) {}
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

// Convenient top-level re-exports for downstream crates (`miro-ego-api`, `miro-ego-cli`).
pub use artifact_io::{
    ByteRange, ManifestArtifactDescriptor, OpenedManifest, VerifiedArtifactFile, open_manifest,
    open_verified_artifact, open_verified_artifact_with_lease, parse_single_byte_range,
};
pub use audit::{ApiAuditEvent, AuditConfig, AuditLogger, AuditWriteTarget, BoundedRingBuffer};
pub use catalog::{
    BinarySessionCatalog, CatalogIndex, DiscoveryDiagnosticCode, GatewayVerification,
    IndexedSession, SessionDiscoveryDiagnostic, SessionListPage, SessionSummary,
};
pub use coordinator::{
    ActiveCaptureHandle, BackendStartRequest, CameraFocusSetRequest, CameraFocusStatus,
    CaptureBackend, CaptureCoordinator, CaptureEvent, CaptureEventBroadcaster, CaptureEventType,
    CaptureMode, CapturePhase, CaptureProgressSnapshot, CaptureReadiness, CaptureStartRequest,
    CaptureStatusSnapshot, CaptureStopRequest, CaptureTimerSetRequest, CaptureTimerSettings,
    CaptureTimerStatus, CapturedArtifact, CapturedAudioOutcome, CapturedAudioSegment,
    CapturedSessionArtifacts, CapturedVideoSegment, CoordinatorConfig, DeleteSessionTarget,
    DeleteSessionsRequest, DeleteSessionsResult, IdempotentOutcome, ReserveEnforcementOutcome,
    RetainedUnsuccessfulSessionResource, RuntimeObservation, SafeSwapReceipt,
    SafeSwapReceiptResource, StopReason, TakeRequest,
};
pub use logs::{LogFileEntry, LogList, LogStore, OpenedLogFile};
pub use recovery::{
    DetailedRecoveryReport, RecoveryDefaults, RecoveryItemOutcome, StagingRecoveryManager,
};
pub use util::VolumeHandleLease;
pub use volume::{
    CaptureStorageBudget, ExfatVolumeManager, FsCapacity, PosixVolumeProbe, ReserveStatus,
    VolumeAdmission, VolumeMetadata, VolumePolicy, VolumeProbe,
};
