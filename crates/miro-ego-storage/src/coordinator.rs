//! Full Rust `CaptureCoordinator` with explicit `CapturePhase` FSM, trait-abstracted
//! `CaptureBackend`, zero-reread sealing, readable collision-safe publish, bounded
//! receipt persistence with unmounted RAM fallback, and `miro-ego.capture-event.v1`
//! event broadcasting.

use crate::artifact_io::open_manifest;
use crate::audit::AuditLogger;
use crate::catalog::{
    BinarySessionCatalog, DEVICE_SESSION_SCHEMA, IndexedSession, SessionDeviceIdentity,
    parse_indexed_session_from_manifest,
};
use crate::recovery::{
    RecoveryDefaults, StagingRecoveryManager, build_manifest_encoding_json,
    cleanup_empty_date_parent, is_transient_storage_error, publish_staging_directory,
    quarantine_session_directory, remove_control_markers_with_logs,
};
use crate::util::{
    VolumeHandleLease, is_canonical_session_relative_dir, now_rfc3339_utc, sha256_hex,
    sweep_stale_atomic_temps,
};
use crate::volume::{CaptureStorageBudget, ExfatVolumeManager, VolumeAdmission};
use crate::{RecoveryManager, RecoveryReport, StorageError, StorageResult, VolumeManager};
use miro_ego_capture::session::session_io::{
    ExpectedArtifactIdentity, finalize_artifact_with_digest, verify_device_session_artifacts,
};
use miro_ego_capture::session::stereo_encoder::SegmentEvent;
use miro_ego_core::CoreError;
use miro_ego_core::config::{RecordingEncoding, VideoCodec};
use miro_ego_core::ids::{
    is_valid_sha256, is_valid_uuid_v4, is_valid_uuid_v7, new_uuid_v4, new_uuid_v7,
};
use miro_ego_core::locks::{FlockGuard, LockMode, acquire_maintenance_lock};
use miro_ego_core::paths::DataVolumePaths;
use miro_ego_core::sys::clock::monotonic_ns;
use miro_ego_core::sys::fs::{
    fstat, fsync_directory, open_directory_safe, read_bounded_json, write_bytes_atomic,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub const CAPTURE_START_SCHEMA: &str = "miro-ego.capture-start.v1";
pub const CAPTURE_STOP_SCHEMA: &str = "miro-ego.capture-stop.v1";
pub const CAPTURE_STATUS_SCHEMA: &str = "miro-ego.capture-status.v1";
pub const CAPTURE_READINESS_SCHEMA: &str = "miro-ego.capture-readiness.v1";
pub const CAPTURE_EVENT_SCHEMA: &str = "miro-ego.capture-event.v1";
pub const CAPTURE_SNAPSHOT_EVENT_SCHEMA: &str = "miro-ego.capture-snapshot-event.v1";
pub const CAPTURE_STATE_EVENT_SCHEMA: &str = "miro-ego.capture-state-event.v1";
pub const CAPTURE_PROGRESS_EVENT_SCHEMA: &str = "miro-ego.capture-progress-event.v1";
pub const CAPTURE_DIAGNOSTIC_EVENT_SCHEMA: &str = "miro-ego.capture-diagnostic-event.v1";
pub const RECORDING_STATE_SCHEMA: &str = "miro-ego.recording-state.v1";
pub const SAFE_SWAP_RECEIPT_SCHEMA: &str = "miro-ego.safe-swap-receipt.v1";
pub const SAFE_SWAP_RECEIPT_RESOURCE_SCHEMA: &str = "miro-ego.safe-swap-receipt-resource.v1";
pub const RETAINED_UNSUCCESSFUL_SCHEMA: &str = "miro-ego.retained-unsuccessful-session-resource.v1";
pub const CAMERA_CONNECTION_SCHEMA: &str = "miro-ego.camera-connection.v1";
pub const CAMERA_FOCUS_SCHEMA: &str = "miro-ego.camera-focus.v1";
pub const CAMERA_FOCUS_SET_SCHEMA: &str = "miro-ego.camera-focus-set.v1";
pub const CAPTURE_TIMER_SCHEMA: &str = "miro-ego.capture-timer.v1";
pub const CAPTURE_TIMER_SET_SCHEMA: &str = "miro-ego.capture-timer-set.v1";
pub const SESSION_DELETE_REQUEST_SCHEMA: &str = "miro-ego.session-delete-request.v1";
pub const SESSION_DELETE_RESULT_SCHEMA: &str = "miro-ego.session-delete-result.v1";

pub const DEFAULT_TIMER_DURATION_SECONDS: u32 = 600;
pub const MAX_TIMER_DURATION_SECONDS: u32 = 86_400;
pub const MAX_DELETE_BATCH_SIZE: usize = 200;
pub const EVENT_REPLAY_CAPACITY: usize = 128;
const MAX_IDEMPOTENCY_ENTRIES: usize = 256;
const MAX_UNSUCCESSFUL_OUTCOMES: usize = 64;
const SUBSCRIBER_CHANNEL_CAPACITY: usize = 128;
const MAX_RECEIPT_FILE_BYTES: usize = 1024 * 1024;

/// Explicit finite state machine phase for [`CaptureCoordinator`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapturePhase {
    Idle,
    Starting,
    Recording,
    Stopping,
    Recovering,
    Deleting,
    Failed,
    Closing,
}

impl CapturePhase {
    /// Validate an explicit FSM transition.
    pub fn can_transition_to(self, next: Self) -> bool {
        if self == next {
            return true;
        }
        match self {
            Self::Idle => matches!(
                next,
                Self::Starting | Self::Recovering | Self::Deleting | Self::Failed | Self::Closing
            ),
            Self::Starting => {
                matches!(
                    next,
                    Self::Recording | Self::Idle | Self::Failed | Self::Closing
                )
            }
            Self::Recording => matches!(next, Self::Stopping | Self::Failed | Self::Closing),
            Self::Stopping => matches!(next, Self::Idle | Self::Failed | Self::Closing),
            Self::Recovering => matches!(next, Self::Idle | Self::Failed | Self::Closing),
            Self::Deleting => matches!(next, Self::Idle | Self::Failed | Self::Closing),
            Self::Failed => matches!(next, Self::Idle | Self::Recovering | Self::Closing),
            Self::Closing => false,
        }
    }

    pub fn is_busy(self) -> bool {
        matches!(
            self,
            Self::Starting | Self::Recording | Self::Stopping | Self::Recovering | Self::Deleting
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Starting => "starting",
            Self::Recording => "recording",
            Self::Stopping => "stopping",
            Self::Recovering => "recovering",
            Self::Deleting => "deleting",
            Self::Failed => "failed",
            Self::Closing => "closing",
        }
    }

    const fn to_u8(self) -> u8 {
        match self {
            Self::Idle => 0,
            Self::Starting => 1,
            Self::Recording => 2,
            Self::Stopping => 3,
            Self::Recovering => 4,
            Self::Deleting => 5,
            Self::Failed => 6,
            Self::Closing => 7,
        }
    }

    const fn from_u8(raw: u8) -> Self {
        match raw {
            0 => Self::Idle,
            1 => Self::Starting,
            2 => Self::Recording,
            3 => Self::Stopping,
            4 => Self::Recovering,
            5 => Self::Deleting,
            6 => Self::Failed,
            _ => Self::Closing,
        }
    }
}

/// Capture mode matching OpenAPI `CaptureStartRequest.mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CaptureMode {
    Production,
    Calibration,
}

impl CaptureMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Production => "production",
            Self::Calibration => "calibration",
        }
    }
}

/// Take selector matching OpenAPI `CaptureStartRequest.take`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum TakeRequest {
    New,
    Continue { continuation_of: String },
}

/// Start request matching OpenAPI `CaptureStartRequest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureStartRequest {
    pub schema: String,
    pub mode: CaptureMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub take: TakeRequest,
}

impl CaptureStartRequest {
    pub fn new_production() -> Self {
        Self {
            schema: CAPTURE_START_SCHEMA.to_owned(),
            mode: CaptureMode::Production,
            volume_id: None,
            display_name: None,
            take: TakeRequest::New,
        }
    }
}

/// Stop reason matching OpenAPI `CaptureStopRequest.reason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    User,
    SafeSwap,
}

/// Stop request matching OpenAPI `CaptureStopRequest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureStopRequest {
    pub schema: String,
    pub reason: StopReason,
}

impl CaptureStopRequest {
    pub fn user() -> Self {
        Self {
            schema: CAPTURE_STOP_SCHEMA.to_owned(),
            reason: StopReason::User,
        }
    }

    pub fn safe_swap() -> Self {
        Self {
            schema: CAPTURE_STOP_SCHEMA.to_owned(),
            reason: StopReason::SafeSwap,
        }
    }
}

/// Camera focus control status matching OpenAPI `CameraFocusStatus`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CameraFocusStatus {
    pub schema: String,
    pub value: u32,
    pub minimum: u32,
    pub maximum: u32,
    pub step: u32,
    pub default: u32,
    pub auto_supported: bool,
    pub auto_enabled: Option<bool>,
}

/// Focus mutation request matching OpenAPI `CameraFocusSetRequest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CameraFocusSetRequest {
    pub schema: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_enabled: Option<bool>,
}

/// Persistent timer configuration matching OpenAPI `CaptureTimerStatus.settings`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureTimerSettings {
    pub enabled: bool,
    pub duration_seconds: u32,
}

impl Default for CaptureTimerSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            duration_seconds: DEFAULT_TIMER_DURATION_SECONDS,
        }
    }
}

/// Active take countdown matching OpenAPI `CaptureTimerStatus.active`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveCaptureTimer {
    pub session_id: String,
    pub duration_seconds: u32,
    pub remaining_seconds: f64,
}

/// Timer status response matching OpenAPI `CaptureTimerStatus`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureTimerStatus {
    pub schema: String,
    pub settings: CaptureTimerSettings,
    pub active: Option<ActiveCaptureTimer>,
}

/// Timer update request matching OpenAPI `setCaptureTimer`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureTimerSetRequest {
    pub schema: String,
    pub enabled: bool,
    pub duration_seconds: u32,
}

/// Target entry in `POST /api/v1/sessions/delete`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteSessionTarget {
    pub session_id: String,
    pub manifest_sha256: String,
}

/// Batch delete request matching OpenAPI `deleteSessions`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteSessionsRequest {
    pub schema: String,
    pub sessions: Vec<DeleteSessionTarget>,
}

/// Per-session error reported on partial deletion failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailedDeleteSession {
    pub session_id: String,
    pub error: String,
}

/// Batch delete result matching OpenAPI `miro-ego.session-delete-result.v1`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteSessionsResult {
    pub schema: String,
    pub deleted_session_ids: Vec<String>,
    pub failed_sessions: Vec<FailedDeleteSession>,
}

/// Storage budget block inside [`CaptureReadiness`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureReadinessStorage {
    pub recording: Option<CaptureStorageBudget>,
    pub system: Option<CaptureStorageBudget>,
    pub backing: Option<CaptureStorageBudget>,
}

/// Capture readiness response matching OpenAPI `miro-ego.capture-readiness.v1`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureReadiness {
    pub schema: String,
    pub allowed: bool,
    pub code: String,
    pub message: String,
    pub storage: CaptureReadinessStorage,
}

/// Minimal safe-swap handoff receipt matching OpenAPI `SafeSwapReceipt`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafeSwapReceipt {
    pub schema: String,
    pub session_id: String,
    pub volume_id: String,
    pub generation_id: String,
    pub manifest_id: String,
    pub manifest_sha256: String,
    pub sealed_at: String,
    pub released_at: String,
    pub release_state: String,
    pub open_handle_count: u32,
}

/// Wrapper returned by `GET /api/v1/capture/safe-swap`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafeSwapReceiptResource {
    pub schema: String,
    pub receipt: SafeSwapReceipt,
}

/// Diagnostic record matching `miro-ego.recording-state.v1#/$defs/diagnostic`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordingDiagnostic {
    pub code: String,
    pub severity: String,
    pub message: String,
    pub at: String,
    pub recoverable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

/// Storage section inside `miro-ego.recording-state.v1`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordingStateStorage {
    pub volume_id: String,
    pub status: String,
    pub writable: bool,
    pub remaining_bytes: Option<u64>,
}

/// Verification progress counter inside `miro-ego.recording-state.v1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationCounter {
    pub completed: u64,
    pub total: u64,
}

/// Progress section inside `miro-ego.recording-state.v1`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordingStateProgress {
    pub elapsed_seconds: f64,
    pub captured_frames: u64,
    pub bytes_written: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification: Option<VerificationCounter>,
}

/// Operational recording state matching `contracts/schemas/miro-ego-recording-state-v1.schema.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordingState {
    pub schema: String,
    pub state: String,
    pub authority_epoch: String,
    pub state_revision: u64,
    pub updated_at: String,
    pub session_id: String,
    pub take_id: String,
    pub display_name: String,
    pub device: SessionDeviceIdentity,
    pub storage: RecordingStateStorage,
    pub progress: RecordingStateProgress,
    pub diagnostics: Vec<RecordingDiagnostic>,
}

/// Mount generation + lossless [`RecordingState`] matching OpenAPI `CaptureSnapshotRecording`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureSnapshotRecording {
    pub generation_id: String,
    pub recording_state: RecordingState,
}

/// Retained per-session unsuccessful outcome matching OpenAPI `RetainedUnsuccessfulSessionResource`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedUnsuccessfulSessionResource {
    pub schema: String,
    pub authority_epoch: String,
    pub source_revision: u64,
    pub outcome: CaptureSnapshotRecording,
}

/// Runtime observation embedded in capture status snapshots.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeObservation {
    pub observed_at: String,
    pub connection_method: String,
    pub temperature_celsius: f64,
    pub network: Value,
    pub live_imu: Option<Value>,
    pub camera_connected: bool,
    pub camera_focus: Option<CameraFocusStatus>,
}

impl RuntimeObservation {
    pub fn default_with_camera(connected: bool, focus: Option<CameraFocusStatus>) -> Self {
        Self {
            observed_at: now_rfc3339_utc(),
            connection_method: "wifi_ap".to_owned(),
            temperature_celsius: 42.0,
            network: json!({
                "ap": { "state": "active", "interface": "wlan0", "addresses": ["10.42.0.1"], "peer_or_ssid": null },
                "wifi_client": { "state": "disconnected", "interface": null, "addresses": [], "peer_or_ssid": null },
                "wired": { "state": "disconnected", "interface": null, "addresses": [], "peer_or_ssid": null },
                "default_route": "none"
            }),
            live_imu: None,
            camera_connected: connected,
            camera_focus: focus,
        }
    }

    pub fn to_openapi_runtime_json(&self) -> Value {
        json!({
            "observed_at": self.observed_at,
            "connection_method": self.connection_method,
            "temperature_celsius": self.temperature_celsius,
            "network": self.network,
            "live_imu": self.live_imu,
            "camera": {
                "schema": CAMERA_CONNECTION_SCHEMA,
                "state": if self.camera_connected { "connected" } else { "disconnected" },
            },
            "camera_focus": self.camera_focus,
        })
    }
}

/// Closed snapshot payload matching OpenAPI `CaptureSnapshotEventData`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureSnapshotEventData {
    pub schema: String,
    pub device_state: String,
    pub active_recording: Option<CaptureSnapshotRecording>,
    pub retained_unsuccessful: Option<CaptureSnapshotRecording>,
    pub runtime: Value,
}

/// Authority-bearing capture status matching OpenAPI `CaptureStatusSnapshot`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureStatusSnapshot {
    pub schema: String,
    pub authority_epoch: String,
    pub source_revision: u64,
    pub snapshot: CaptureSnapshotEventData,
}

/// OpenAPI `CaptureEvent.type` discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureEventType {
    Snapshot,
    State,
    Progress,
    Diagnostic,
    SafeSwap,
}

/// Closed capture SSE/broadcast event matching OpenAPI `miro-ego.capture-event.v1`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureEvent {
    pub schema: String,
    pub sse_delivery_id: String,
    pub authority_epoch: String,
    pub source_revision: u64,
    #[serde(rename = "type")]
    pub event_type: CaptureEventType,
    pub occurred_at: String,
    pub session_id: Option<String>,
    pub data: Value,
}

/// Result wrapper carrying whether an idempotent command was replayed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IdempotentOutcome<T> {
    pub value: T,
    pub replayed: bool,
}

/// Outcome returned by [`CaptureCoordinator::enforce_storage_reserve`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ReserveEnforcementOutcome {
    Healthy,
    StoppedForReserve { session_id: String, reason: String },
    FailedMediaLost { session_id: String, reason: String },
}

// --- CaptureBackend abstraction for real hardware & deterministic testing ---

/// Parameters passed to [`CaptureBackend::begin_capture`].
#[derive(Debug, Clone)]
pub struct BackendStartRequest {
    pub session_id: String,
    pub take_id: String,
    pub mode: CaptureMode,
    pub staging_dir: PathBuf,
    pub started_at: String,
    pub started_monotonic_ns: u64,
    pub encoding: RecordingEncoding,
}

/// Live progress snapshot queried from an active capture handle.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct CaptureProgressSnapshot {
    pub elapsed_seconds: f64,
    pub captured_frames: u64,
    pub bytes_written: u64,
}

/// Video segment output from backend finish (compatible with `miro-ego-capture` `SegmentEvent`).
#[derive(Debug, Clone, PartialEq)]
pub struct CapturedVideoSegment {
    pub index: u64,
    pub start_frame: u64,
    pub end_frame: u64,
    pub start_time_seconds: f64,
    pub end_time_seconds: f64,
    pub left_path: String,
    pub left_bytes: u64,
    pub left_sha256: Option<String>,
    pub right_path: String,
    pub right_bytes: u64,
    pub right_sha256: Option<String>,
}

impl CapturedVideoSegment {
    /// Convert a `miro-ego-capture` encoder [`SegmentEvent`] with streaming SHA-256 digests.
    pub fn from_encoder_segment(
        event: &SegmentEvent,
        start_time_seconds: f64,
        end_time_seconds: f64,
    ) -> Self {
        Self {
            index: event.index,
            start_frame: event.start_frame,
            end_frame: event.end_frame,
            start_time_seconds,
            end_time_seconds,
            left_path: event.left_path.clone(),
            left_bytes: event.left_bytes,
            left_sha256: event.left_sha256.clone(),
            right_path: event.right_path.clone(),
            right_bytes: event.right_bytes,
            right_sha256: event.right_sha256.clone(),
        }
    }
}

/// Regular artifact output with optional precomputed streaming SHA-256 (zero-reread seal).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedArtifact {
    pub role: String,
    pub path: String,
    pub media_type: String,
    pub bytes: u64,
    pub sha256: Option<String>,
}

/// Audio segment output for recorded takes.
#[derive(Debug, Clone, PartialEq)]
pub struct CapturedAudioSegment {
    pub index: u64,
    pub start_sample: u64,
    pub end_sample: u64,
    pub start_time_seconds: f64,
    pub end_time_seconds: f64,
    pub pcm_payload_bytes: u64,
    pub wav_header_bytes: u64,
    pub artifact: CapturedArtifact,
}

/// Audio recording outcome for manifest construction.
#[derive(Debug, Clone, PartialEq)]
pub enum CapturedAudioOutcome {
    Recorded {
        requested_mode: String,
        sample_rate: u32,
        channels: u16,
        sample_count: u64,
        start_time_seconds: f64,
        end_time_seconds: f64,
        segments: Vec<CapturedAudioSegment>,
    },
    RecordedWithClock {
        requested_mode: String,
        sample_rate: u32,
        channels: u16,
        sample_count: u64,
        start_time_seconds: f64,
        end_time_seconds: f64,
        segments: Vec<CapturedAudioSegment>,
        capture_clock: Option<Value>,
    },
    NotRecorded {
        requested_mode: String,
        reason: String,
    },
}

impl CapturedAudioOutcome {
    /// Attach an optional `miro-ego.audio-clock.v1` `capture_clock` object to a recorded audio outcome.
    pub fn with_capture_clock(self, capture_clock: Option<Value>) -> Self {
        match self {
            Self::Recorded {
                requested_mode,
                sample_rate,
                channels,
                sample_count,
                start_time_seconds,
                end_time_seconds,
                segments,
            }
            | Self::RecordedWithClock {
                requested_mode,
                sample_rate,
                channels,
                sample_count,
                start_time_seconds,
                end_time_seconds,
                segments,
                ..
            } => Self::RecordedWithClock {
                requested_mode,
                sample_rate,
                channels,
                sample_count,
                start_time_seconds,
                end_time_seconds,
                segments,
                capture_clock,
            },
            other => other,
        }
    }

    pub fn capture_clock(&self) -> Option<&Value> {
        match self {
            Self::RecordedWithClock { capture_clock, .. } => capture_clock.as_ref(),
            _ => None,
        }
    }
}

/// Frame drop event matching `miro-ego.device-session.v1#/$defs/dropEvent`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropEventRecord {
    pub start_frame: u64,
    pub end_frame: u64,
    pub at_time_seconds: f64,
    pub reason: String,
    pub dropped: u64,
}

/// Complete set of staged artifacts and streaming digests returned by [`ActiveCaptureHandle::finish`].
#[derive(Debug, Clone)]
pub struct CapturedSessionArtifacts {
    pub segments: Vec<CapturedVideoSegment>,
    pub frames_artifact: CapturedArtifact,
    pub frames_count: u64,
    pub imu_artifact: CapturedArtifact,
    pub imu_sample_count: u64,
    pub audio: CapturedAudioOutcome,
    pub logs: Vec<CapturedArtifact>,
    pub dropped_frames: u64,
    pub drop_events: Vec<DropEventRecord>,
    pub duration_seconds: f64,
    pub effective_fps: f64,
}

/// Active recording session owned by [`CaptureCoordinator`] between start and stop.
pub trait ActiveCaptureHandle: Send + Sync {
    fn progress(&self) -> CaptureProgressSnapshot;
    fn finish(self: Box<Self>, reason: StopReason) -> StorageResult<CapturedSessionArtifacts>;
    fn abort(self: Box<Self>);
    /// Poll and take any asynchronous mid-recording failure `(error_code, error_message)`
    /// detected by background camera, encoder, audio, or sink workers.
    fn take_failure(&self) -> Option<(String, String)> {
        None
    }
}

/// Pluggable hardware capture backend (`miro-ego-capture` in production, deterministic mock in tests).
pub trait CaptureBackend: Send + Sync {
    fn is_camera_connected(&self) -> bool;
    fn camera_focus(&self) -> StorageResult<Option<CameraFocusStatus>>;
    fn set_camera_focus(&self, req: &CameraFocusSetRequest) -> StorageResult<CameraFocusStatus>;
    fn begin_capture(
        &self,
        req: &BackendStartRequest,
    ) -> StorageResult<Box<dyn ActiveCaptureHandle>>;
    fn runtime_observation(&self) -> RuntimeObservation;
}

// --- Broadcast channel for `miro-ego.capture-event.v1` ---

struct BroadcasterInner {
    next_delivery_id: u64,
    replay: VecDeque<(u64, CaptureEvent)>,
    subscribers: Vec<SyncSender<CaptureEvent>>,
}

/// Non-blocking bounded event broadcaster with 128-slot replay ring for SSE reconnection.
pub struct CaptureEventBroadcaster {
    inner: Mutex<BroadcasterInner>,
}

impl CaptureEventBroadcaster {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(BroadcasterInner {
                next_delivery_id: 1,
                replay: VecDeque::with_capacity(EVENT_REPLAY_CAPACITY),
                subscribers: Vec::new(),
            }),
        }
    }

    pub fn subscribe(&self) -> CaptureEventSubscription {
        let (tx, rx) = mpsc::sync_channel(SUBSCRIBER_CHANNEL_CAPACITY);
        if let Ok(mut guard) = self.inner.lock() {
            guard.subscribers.push(tx);
        }
        CaptureEventSubscription { rx }
    }

    pub fn emit(
        &self,
        authority_epoch: &str,
        source_revision: u64,
        event_type: CaptureEventType,
        session_id: Option<String>,
        data: Value,
    ) -> CaptureEvent {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let delivery_id = guard.next_delivery_id;
        guard.next_delivery_id = guard.next_delivery_id.saturating_add(1);
        let event = CaptureEvent {
            schema: CAPTURE_EVENT_SCHEMA.to_owned(),
            sse_delivery_id: delivery_id.to_string(),
            authority_epoch: authority_epoch.to_owned(),
            source_revision,
            event_type,
            occurred_at: now_rfc3339_utc(),
            session_id,
            data,
        };
        if guard.replay.len() >= EVENT_REPLAY_CAPACITY {
            guard.replay.pop_front();
        }
        guard.replay.push_back((delivery_id, event.clone()));
        guard
            .subscribers
            .retain(|sub| match sub.try_send(event.clone()) {
                Ok(()) | Err(mpsc::TrySendError::Full(_)) => true,
                Err(mpsc::TrySendError::Disconnected(_)) => false,
            });
        event
    }

    /// Return `(needs_snapshot, events)` for an SSE client reconnecting with `Last-Event-ID`.
    pub fn replay_since(&self, last_delivery_id: Option<u64>) -> (bool, Vec<CaptureEvent>) {
        let guard = match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let Some(last_id) = last_delivery_id else {
            return (true, Vec::new());
        };
        let Some(&(oldest_id, _)) = guard.replay.front() else {
            return (true, Vec::new());
        };
        let latest_id = guard.next_delivery_id.saturating_sub(1);
        if last_id > latest_id || last_id.saturating_add(1) < oldest_id {
            return (true, Vec::new());
        }
        let events = guard
            .replay
            .iter()
            .filter(|(id, _)| *id > last_id)
            .map(|(_, ev)| ev.clone())
            .collect();
        (false, events)
    }
}

impl Default for CaptureEventBroadcaster {
    fn default() -> Self {
        Self::new()
    }
}

/// Subscriber handle receiving live [`CaptureEvent`] messages without blocking capture.
pub struct CaptureEventSubscription {
    rx: Receiver<CaptureEvent>,
}

impl CaptureEventSubscription {
    pub fn try_recv(&self) -> Result<CaptureEvent, TryRecvError> {
        self.rx.try_recv()
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<CaptureEvent, RecvTimeoutError> {
        self.rx.recv_timeout(timeout)
    }
}

// --- Bounded Receipt Store (`/data/.miro-ego/receipts/` with RAM fallback) ---

const MAX_FOCUS_IDEMPOTENCY_ENTRIES: usize = 32;

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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedReceipts {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    volume_id: Option<String>,
    idempotency: VecDeque<IdempotencyRecord>,
    safe_swap: Option<SafeSwapReceipt>,
    unsuccessful: VecDeque<RetainedUnsuccessfulSessionResource>,
}

struct ReceiptStoreState {
    receipts: PersistedReceipts,
    loaded_from_disk: bool,
    active_volume_id: Option<String>,
}

struct ReceiptStore {
    paths: DataVolumePaths,
    state: Mutex<ReceiptStoreState>,
}

impl ReceiptStore {
    fn open(
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
    fn bind_volume(&self, new_volume_id: Option<&str>, volume_writable: bool) {
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
    fn ensure_loaded_from_disk(&self, volume_writable: bool) {
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
            || guard.receipts.safe_swap.is_some();

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
                // Merge older on-disk unsuccessful outcomes behind any newer in-RAM outcomes.
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

                // Merge older on-disk idempotency records behind any newer in-RAM records.
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
            }
        }

        guard.loaded_from_disk = true;
        if had_in_ram_changes {
            self.persist_locked(&mut guard.receipts);
        }
    }

    fn lookup_idempotency(
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
            if focus_count >= MAX_FOCUS_IDEMPOTENCY_ENTRIES {
                if let Some(pos) = receipts
                    .idempotency
                    .iter()
                    .position(|r| r.operation == "set_camera_focus")
                {
                    receipts.idempotency.remove(pos);
                }
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

    fn store_idempotency(
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

    /// Atomically persist both `safe_swap` and the optional `stop_capture` idempotency receipt
    /// in a single write BEFORE the recording volume is marked `released = true`.
    fn commit_safe_swap_bundle(
        &self,
        idem_entry: Option<(&str, &str, &str, Value)>,
        receipt: SafeSwapReceipt,
        volume_writable: bool,
    ) {
        self.ensure_loaded_from_disk(volume_writable);
        let Ok(mut guard) = self.state.lock() else {
            return;
        };
        guard.receipts.volume_id = Some(receipt.volume_id.clone());
        guard.active_volume_id = Some(receipt.volume_id.clone());
        guard.receipts.safe_swap = Some(receipt);
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

    fn set_safe_swap(&self, receipt: Option<SafeSwapReceipt>, volume_writable: bool) {
        self.ensure_loaded_from_disk(volume_writable);
        let Ok(mut guard) = self.state.lock() else {
            return;
        };
        guard.receipts.safe_swap = receipt;
        if volume_writable {
            self.persist_locked(&mut guard.receipts);
        }
    }

    fn get_safe_swap(&self, volume_writable: bool) -> StorageResult<Option<SafeSwapReceipt>> {
        self.ensure_loaded_from_disk(volume_writable);
        let guard = self
            .state
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("receipt_store")))?;
        Ok(guard.receipts.safe_swap.clone())
    }

    fn record_unsuccessful(
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

    fn get_unsuccessful(
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
    /// Never creates `/data` on the root system disk!
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

// --- In-flight idempotency deduplication & conflict detection ---

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

struct InFlightTracker {
    entries: Mutex<HashMap<InFlightKey, Arc<InFlightSlot>>>,
}

enum InFlightRegistration<'a> {
    Leader(InFlightLeaderGuard<'a>),
    Replayed(Value),
}

struct InFlightLeaderGuard<'a> {
    tracker: &'a InFlightTracker,
    key: InFlightKey,
    slot: Arc<InFlightSlot>,
    completed: bool,
}

impl InFlightLeaderGuard<'_> {
    fn succeed(mut self, response_json: Value) {
        if let Ok(mut guard) = self.slot.outcome.lock() {
            *guard = InFlightOutcome::Succeeded(response_json);
        }
        if let Ok(mut map) = self.tracker.entries.lock() {
            map.remove(&self.key);
        }
        self.slot.cv.notify_all();
        self.completed = true;
    }

    fn fail(mut self, err: &StorageError) {
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
    fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn acquire(
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

fn clone_storage_error(err: &StorageError) -> StorageError {
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

// --- Coordinator configuration & active session state ---

/// Configuration for [`CaptureCoordinator`].
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    pub device_id: String,
    pub device_label: String,
    pub hardware_fingerprint: String,
    pub platform: String,
    pub software_version: String,
    pub commit: String,
    pub timezone: String,
    pub camera_width: u32,
    pub camera_height: u32,
    pub sensor_fps: f64,
    pub frame_decimation: u32,
    pub segment_frames: u64,
    pub encoding: RecordingEncoding,
    pub run_dir: Option<PathBuf>,
    pub timer_settings_path: Option<PathBuf>,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            device_id: new_uuid_v4(),
            device_label: "Miro-Ego-00000000".to_owned(),
            hardware_fingerprint: format!("sha256:{}", "0".repeat(64)),
            platform: "rdk-x5".to_owned(),
            software_version: env!("CARGO_PKG_VERSION").to_owned(),
            commit: "0".repeat(40),
            timezone: "Etc/UTC".to_owned(),
            camera_width: 3840,
            camera_height: 1080,
            sensor_fps: 60.0,
            frame_decimation: 2,
            segment_frames: 150,
            encoding: RecordingEncoding::default(),
            run_dir: None,
            timer_settings_path: None,
        }
    }
}

impl CoordinatorConfig {
    pub fn recovery_defaults(&self) -> RecoveryDefaults {
        RecoveryDefaults {
            device_id: self.device_id.clone(),
            device_label: self.device_label.clone(),
            hardware_fingerprint: self.hardware_fingerprint.clone(),
            platform: self.platform.clone(),
            software_version: self.software_version.clone(),
            commit: self.commit.clone(),
            timezone: self.timezone.clone(),
            camera_width: self.camera_width,
            camera_height: self.camera_height,
            sensor_fps: self.sensor_fps,
            frame_decimation: self.frame_decimation,
            segment_frames: self.segment_frames,
            encoding: self.encoding.clone(),
        }
    }

    fn device_identity(&self) -> SessionDeviceIdentity {
        SessionDeviceIdentity {
            device_id: self.device_id.clone(),
            device_label: self.device_label.clone(),
        }
    }
}

#[derive(Clone)]
struct SessionMetadata {
    session_id: String,
    take_id: String,
    take_sequence: u64,
    continuation_of: Option<String>,
    mode: CaptureMode,
    display_name: String,
    started_at: String,
    staging_dir: PathBuf,
    admission: VolumeAdmission,
    diagnostics: Vec<RecordingDiagnostic>,
}

struct ActiveSessionContext {
    session_id: String,
    take_id: String,
    take_sequence: u64,
    continuation_of: Option<String>,
    mode: CaptureMode,
    display_name: String,
    started_at: String,
    started_monotonic_ns: u64,
    staging_dir: PathBuf,
    admission: VolumeAdmission,
    recording_stage: String,
    stop_reason: Option<StopReason>,
    last_progress: CaptureProgressSnapshot,
    verification_progress: Option<VerificationCounter>,
    diagnostics: Vec<RecordingDiagnostic>,
    timer_deadline_ns: Option<u64>,
    timer_duration_seconds: Option<u32>,
    handle: Option<Box<dyn ActiveCaptureHandle>>,
    _maintenance_lock: Option<Arc<FlockGuard>>,
}

impl ActiveSessionContext {
    fn metadata_snapshot(&self) -> SessionMetadata {
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
        }
    }
}

struct CoordinatorState {
    phase: CapturePhase,
    phase_atomic: Arc<AtomicU8>,
    authority_epoch: String,
    source_revision: u64,
    admission: Option<VolumeAdmission>,
    active_volume_id: Option<String>,
    volume_released: bool,
    released_mount_signature: Option<(String, u64, String)>,
    saw_unmount_since_release: bool,
    active: Option<ActiveSessionContext>,
    retained_unsuccessful: Option<CaptureSnapshotRecording>,
    timer_settings: CaptureTimerSettings,
    last_failure_reason: Option<String>,
}

impl CoordinatorState {
    fn transition(&mut self, next: CapturePhase) -> StorageResult<()> {
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

    fn bump_revision(&mut self) -> u64 {
        self.source_revision = self.source_revision.saturating_add(1);
        self.source_revision
    }
}

type SafeSwapReleaseHook = Arc<dyn Fn() + Send + Sync>;

/// Authoritative native Rust capture coordinator managing the capture FSM, volume reserve
/// enforcement, zero-reread sealing, catalog admission, batch deletion, and events.
pub struct CaptureCoordinator {
    config: CoordinatorConfig,
    volume: Arc<ExfatVolumeManager>,
    catalog: Arc<BinarySessionCatalog>,
    backend: Arc<dyn CaptureBackend>,
    receipts: ReceiptStore,
    inflight: InFlightTracker,
    events: CaptureEventBroadcaster,
    phase_atomic: Arc<AtomicU8>,
    timer_write_lock: Mutex<()>,
    audit_logger: Mutex<Option<Arc<AuditLogger>>>,
    release_hooks: Mutex<Vec<SafeSwapReleaseHook>>,
    audio_requested_mode: Mutex<String>,
    state: Mutex<CoordinatorState>,
    phase_cv: Condvar,
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
            release_hooks: Mutex::new(Vec::new()),
            audio_requested_mode: Mutex::new("device_default".to_owned()),
            state: Mutex::new(CoordinatorState {
                phase: CapturePhase::Idle,
                phase_atomic,
                authority_epoch: new_uuid_v4(),
                source_revision: 0,
                admission,
                active_volume_id,
                volume_released: false,
                released_mount_signature: None,
                saw_unmount_since_release: false,
                active: None,
                retained_unsuccessful: None,
                timer_settings,
                last_failure_reason: None,
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
        ) {
            if let Ok(mut guard) = self.audio_requested_mode.lock() {
                *guard = candidate;
            }
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

    fn close_all_volume_handles_for_safe_swap(&self) {
        if let Ok(guard) = self.audit_logger.lock() {
            if let Some(audit) = guard.as_ref() {
                audit.close_active_file();
            }
        }
        let hooks = self
            .release_hooks
            .lock()
            .ok()
            .map(|g| g.clone())
            .unwrap_or_default();
        for hook in hooks {
            hook();
        }
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

    fn acquire_shared_maintenance_lock(&self) -> StorageResult<Option<Arc<FlockGuard>>> {
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

    /// Run crash prefix recovery on startup (`Idle -> Recovering -> Idle`).
    pub fn recover_on_startup(&self) -> StorageResult<RecoveryReport> {
        let maintenance_lock = self.acquire_shared_maintenance_lock()?;
        {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase != CapturePhase::Idle && guard.phase != CapturePhase::Failed {
                return Err(StorageError::conflict(
                    "capture_busy",
                    "cannot run crash recovery while capture coordinator is busy",
                ));
            }
            guard.transition(CapturePhase::Recovering)?;
            self.phase_cv.notify_all();
        }

        self.receipts
            .ensure_loaded_from_disk(self.volume.is_mounted_and_writable());

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
                    let _ = guard.transition(CapturePhase::Idle);
                }
                Err(err) => {
                    guard.last_failure_reason = Some(err.to_string());
                    let _ = guard.transition(CapturePhase::Failed);
                }
            }
            self.phase_cv.notify_all();
            (guard.authority_epoch.clone(), guard.source_revision)
        };
        drop(maintenance_lock);

        if let Ok(ref report) = result {
            if report.recovered > 0 || report.quarantined > 0 {
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
        }
        result
    }

    /// Explicitly recover the coordinator FSM from `Failed` back to `Idle` once faults clear.
    pub fn recover_from_failure(&self) -> StorageResult<CapturePhase> {
        let (phase_now, prev_adm, prev_vid, was_released) = {
            let guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase != CapturePhase::Failed {
                return Ok(guard.phase);
            }
            (
                guard.phase,
                guard.admission.clone(),
                guard.active_volume_id.clone(),
                guard.volume_released,
            )
        };
        let _ = phase_now;

        // Perform volume admission I/O strictly outside `self.state.lock()`.
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
            if volume_changed {
                guard.retained_unsuccessful = None;
            }
            if new_vid.is_some() {
                guard.active_volume_id = new_vid;
            }
            guard.admission = admitted.clone();
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
        self.reconcile_and_admit_volume(false)
    }

    fn reconcile_and_admit_volume(
        &self,
        run_recovery_always: bool,
    ) -> StorageResult<VolumeAdmission> {
        let (prev, prev_vid, was_released, was_failed) = {
            let guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase.is_busy() || guard.phase == CapturePhase::Closing {
                return Err(StorageError::conflict(
                    "capture_busy",
                    "cannot refresh volume admission while capture is busy",
                ));
            }
            (
                guard.admission.clone(),
                guard.active_volume_id.clone(),
                guard.volume_released,
                guard.phase == CapturePhase::Failed,
            )
        };

        // Temporarily clear `released` to probe/admit; restore if admission fails.
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

        let (should_emit_snapshot, snap_info) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase.is_busy() || guard.phase == CapturePhase::Closing {
                return Err(StorageError::conflict(
                    "capture_busy",
                    "cannot commit volume admission while capture is busy",
                ));
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
            if guard.phase == CapturePhase::Failed {
                guard.transition(CapturePhase::Idle)?;
                self.phase_cv.notify_all();
                let runtime_json = self.backend.runtime_observation().to_openapi_runtime_json();
                let snap = self.build_status_snapshot_locked(&guard, runtime_json);
                (
                    true,
                    Some((guard.authority_epoch.clone(), guard.source_revision, snap)),
                )
            } else {
                guard.bump_revision();
                (false, None)
            }
        };

        if should_emit_snapshot {
            if let Some((epoch, rev, snap)) = snap_info {
                self.events.emit(
                    &epoch,
                    rev,
                    CaptureEventType::Snapshot,
                    None,
                    serde_json::to_value(&snap.snapshot).unwrap_or(Value::Null),
                );
            }
        }

        if admitted.writable && (run_recovery_always || volume_changed || was_failed) {
            let _ = self.recover_on_startup();
        }

        Ok(admitted)
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
        let (phase, volume_released) = {
            let guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            (guard.phase, guard.volume_released)
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
        } else if phase.is_busy() {
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
                    // Re-check persisted store after acquiring slot in case previous leader just committed.
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

        if self.phase()?.is_busy() {
            return Err(StorageError::conflict(
                "capture_busy",
                "cannot change capture timer settings while capture or finalization is active",
            ));
        }

        // Perform atomic timer file write outside `self.state.lock()`.
        if let Some(path) = &self.config.timer_settings_path {
            let mut bytes = serde_json::to_vec_pretty(&new_settings).map_err(|err| {
                StorageError::invalid_input("timer_encode_failed", err.to_string())
            })?;
            bytes.push(b'\n');
            write_bytes_atomic(path, &bytes, 0o640, true).map_err(|err| {
                StorageError::io("timer_save_failed", "persist capture timer settings", err)
            })?;
        }

        {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase.is_busy() {
                return Err(StorageError::conflict(
                    "capture_busy",
                    "cannot change capture timer settings while capture or finalization is active",
                ));
            }
            guard.timer_settings = new_settings;
        }

        self.get_capture_timer()
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
    ///
    /// Returns `Ok(true)` if the timer expired and stopped the capture, `Ok(false)` otherwise.
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
        // Fast preflight on coordinator state before hardware/filesystem probes.
        let prev_admission = {
            let guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.volume_released || self.volume.is_volume_released() {
                return Err(StorageError::conflict(
                    "volume_released",
                    "recording volume was released for safe swap",
                ));
            }
            if guard.phase != CapturePhase::Idle {
                return Err(StorageError::conflict(
                    "capture_busy",
                    format!(
                        "cannot start capture while coordinator is in phase {}",
                        guard.phase.as_str()
                    ),
                ));
            }
            guard.admission.clone()
        };

        // Pre-validate continuation predecessor before side effects (re-verified inside state lock below).
        if let TakeRequest::Continue { continuation_of } = &req.take {
            let _ = self.resolve_continuation_locked(continuation_of, None)?;
        }

        if !self.backend.is_camera_connected() {
            return Err(StorageError::camera_not_connected(
                "configured camera is not connected",
            ));
        }

        // Verify volume admission (preserving generation_id across unchanged mounts) and reserve budgets
        // BEFORE transitioning FSM or creating files, mapping storage errors to OpenAPI 422 codes.
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

        if let Some(requested_vid) = &req.volume_id {
            if requested_vid != &admission.volume_id {
                return Err(StorageError::unprocessable(
                    "volume_mismatch",
                    "requested volume_id does not match admitted recording volume",
                ));
            }
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

        // Acquire maintenance shared lock (retrying briefly on LockBusy) so idle fstrim / watchdog defer.
        let maintenance_lock = self.acquire_shared_maintenance_lock()?;
        // Keep a local clone across `Starting` so even if `close()` transitions `Starting -> Closing`
        // while `begin_capture` is blocked, `maintenance.lock` stays held until cleanup finishes!
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

        // Enter `Starting` phase atomically with `active` populated (so concurrent snapshots never
        // report `"blocked"`) and re-verify `continuation_of` under `state.lock()` to eliminate TOCTOU!
        let (authority_epoch, take_id, take_sequence, continuation_of) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.volume_released || self.volume.is_volume_released() {
                return Err(StorageError::conflict(
                    "volume_released",
                    "recording volume was released for safe swap",
                ));
            }
            if guard.phase != CapturePhase::Idle {
                return Err(StorageError::conflict(
                    "capture_busy",
                    format!(
                        "cannot start capture while coordinator is in phase {}",
                        guard.phase.as_str()
                    ),
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
        self.receipts.set_safe_swap(None, admission.writable);

        // Prepare staging directory `/data/.miro-ego/staging/<session_id>.partial`.
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

        // Write `capture.json` metadata marker (including encoding, camera, device, and audio mode)
        // and propagate write errors before starting hardware capture!
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

        // Write initial `recording.json` outside `self.state.lock()` and propagate write errors!
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

        // Start backend capture.
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
                    // Prerequisite refusal (`503`): restore Idle cleanly without polluting
                    // `retained_unsuccessful` or leaving an orphan `.partial` directory!
                    let released = {
                        let mut guard = match self.state.lock() {
                            Ok(g) => g,
                            Err(poisoned) => poisoned.into_inner(),
                        };
                        let r = guard.active.take();
                        let _ = guard.transition(CapturePhase::Idle);
                        self.phase_cv.notify_all();
                        r
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

        let runtime_json = self.backend.runtime_observation().to_openapi_runtime_json();
        let (status_snapshot, rev) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if let Err(err) = guard.transition(CapturePhase::Recording) {
                let released = guard.active.take();
                self.phase_cv.notify_all();
                drop(guard);
                handle.abort();
                drop(released);
                cleanup_empty_staging_attempt(&staging_dir);
                drop(local_starting_lock);
                return Err(err);
            }
            // Starting a new take removes retained_unsuccessful from current status,
            // while NEVER deleting per-session retained unsuccessful resources in receipts.
            guard.retained_unsuccessful = None;
            if let Some(ctx) = guard.active.as_mut() {
                ctx.handle = Some(handle);
            }
            self.phase_cv.notify_all();
            let snap = self.build_status_snapshot_locked(&guard, runtime_json);
            (snap, guard.source_revision)
        };
        drop(local_starting_lock);

        // Update `recording.json` with exact `state_revision` outside `self.state.lock()`.
        if let Some(active_rec) = &status_snapshot.snapshot.active_recording {
            if let Ok(mut bytes) = serde_json::to_vec_pretty(&active_rec.recording_state) {
                bytes.push(b'\n');
                if let Err(err) =
                    write_bytes_atomic(&staging_dir.join("recording.json"), &bytes, 0o640, true)
                {
                    // Abort the just-started recording cleanly if `recording.json` fails to persist.
                    let extracted = {
                        let mut guard = match self.state.lock() {
                            Ok(g) => g,
                            Err(poisoned) => poisoned.into_inner(),
                        };
                        let h = guard.active.as_mut().and_then(|c| c.handle.take());
                        let _ = guard.transition(CapturePhase::Stopping);
                        self.phase_cv.notify_all();
                        h
                    };
                    if let Some(h) = extracted {
                        h.abort();
                    }
                    cleanup_empty_staging_attempt(&staging_dir);
                    let storage_err = StorageError::io(
                        "recording_marker_write_failed",
                        "persist recording.json after start",
                        err,
                    );
                    self.fail_start_attempt(
                        &session_id,
                        &take_id,
                        &display_name,
                        &admission,
                        storage_err.code(),
                        &storage_err.to_string(),
                    );
                    return Err(storage_err);
                }
            }
        }

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

        if let Some(key) = idempotency_key {
            if let Ok(json_val) = serde_json::to_value(&status_snapshot) {
                self.receipts.store_idempotency(
                    principal_id,
                    "start_capture",
                    key,
                    req_hash,
                    json_val,
                    admission.writable,
                );
            }
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

    /// Idempotently stop, seal (zero-reread), and publish the active recording.
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

    fn execute_stop_capture_inner(
        &self,
        req: &CaptureStopRequest,
        principal_id: &str,
        idempotency_key: Option<&str>,
        req_hash: &str,
    ) -> StorageResult<Option<CaptureStatusSnapshot>> {
        let (meta, handle, final_progress, authority_epoch, stopping_rev) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            // Wait if `Starting` is currently mid-handshake so stop acts deterministically.
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

            // `Deleting` is NOT idle: only `Idle` and `Failed` return `204` (`None`);
            // `Deleting` falls through below and returns `409 capture_busy`.
            if matches!(guard.phase, CapturePhase::Idle | CapturePhase::Failed) {
                drop(guard);
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
                return Ok(None);
            }

            if guard.phase == CapturePhase::Stopping {
                let in_flight_reason = guard.active.as_ref().and_then(|c| c.stop_reason);
                if req.reason == StopReason::SafeSwap
                    && in_flight_reason != Some(StopReason::SafeSwap)
                {
                    return Err(StorageError::conflict(
                        "capture_busy",
                        "capture is already finalizing without safe_swap",
                    ));
                }
                drop(guard);
                let wrapped = Some(self.capture_status()?);
                if let Some(key) = idempotency_key {
                    if let Ok(json_val) = serde_json::to_value(&wrapped) {
                        self.receipts.store_idempotency(
                            principal_id,
                            "stop_capture",
                            key,
                            req_hash,
                            json_val,
                            self.volume.is_mounted_and_writable(),
                        );
                    }
                }
                return Ok(wrapped);
            }

            if guard.phase != CapturePhase::Recording {
                return Err(StorageError::conflict(
                    "capture_busy",
                    format!(
                        "cannot stop capture while coordinator is in phase {}",
                        guard.phase.as_str()
                    ),
                ));
            }
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
            (
                meta,
                handle,
                last_progress,
                guard.authority_epoch.clone(),
                guard.source_revision,
            )
        };

        let session_id = meta.session_id.clone();
        let volume_id = meta.admission.volume_id.clone();
        let generation_id = meta.admission.generation_id.clone();

        self.events.emit(
            &authority_epoch,
            stopping_rev,
            CaptureEventType::State,
            Some(session_id.clone()),
            json!({
                "schema": CAPTURE_STATE_EVENT_SCHEMA,
                "state": "finalizing",
                "volume_id": volume_id,
                "generation_id": generation_id,
            }),
        );

        // If the backend already recorded an asynchronous mid-recording crash, abort immediately
        // and converge on the committed terminal `failed` snapshot (`202 Accepted`).
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
            if let Some(key) = idempotency_key {
                if let Ok(json_val) = serde_json::to_value(&wrapped) {
                    self.receipts.store_idempotency(
                        principal_id,
                        "stop_capture",
                        key,
                        req_hash,
                        json_val,
                        !media_lost && self.volume.is_mounted_and_writable(),
                    );
                }
            }
            return Ok(wrapped);
        }

        let captured = match handle.finish(req.reason) {
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
                if let Some(key) = idempotency_key {
                    if let Ok(json_val) = serde_json::to_value(&wrapped) {
                        self.receipts.store_idempotency(
                            principal_id,
                            "stop_capture",
                            key,
                            req_hash,
                            json_val,
                            !media_lost && self.volume.is_mounted_and_writable(),
                        );
                    }
                }
                return Ok(wrapped);
            }
        };

        // Transition visible stage from "finalizing" to "verifying" while retaining active_recording.
        let total_artifacts = count_captured_artifacts(&captured);
        let captured_bytes = sum_captured_artifact_bytes(&captured);
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

        let seal_result = self.seal_and_publish(&meta, &captured);
        let sealed = match seal_result {
            Ok(s) => s,
            Err(err) => {
                let media_lost = !self.volume.is_mounted_and_writable();
                // Pre-publish transient storage error (`ENOSPC`) leaves `.partial` intact in
                // `.miro-ego/staging/` as `"recoverable"` instead of quarantining or deleting!
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
                if let Some(key) = idempotency_key {
                    if let Ok(json_val) = serde_json::to_value(&wrapped) {
                        self.receipts.store_idempotency(
                            principal_id,
                            "stop_capture",
                            key,
                            req_hash,
                            json_val,
                            !media_lost && self.volume.is_mounted_and_writable(),
                        );
                    }
                }
                return Ok(wrapped);
            }
        };

        // Release `_maintenance_lock` FIRST, and on `SafeSwap` close `AuditLogger` + invoke
        // release hooks, persist `stop_capture` idempotency + `safe_swap` TOGETHER in `ReceiptStore`,
        // and THEN mark the volume released before emitting `SafeSwapReceipt`!
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

        if req.reason == StopReason::SafeSwap {
            self.close_all_volume_handles_for_safe_swap();
        }

        let runtime_json = self.backend.runtime_observation().to_openapi_runtime_json();
        let (final_snapshot, done_rev, released_ctx) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            let released = guard.active.take();
            if req.reason == StopReason::SafeSwap {
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
            (snap, guard.source_revision, released)
        };
        drop(released_ctx);

        let wrapped = Some(final_snapshot);

        let safe_swap_receipt = if req.reason == StopReason::SafeSwap {
            self.close_all_volume_handles_for_safe_swap();
            let open_handles = u32::try_from(self.open_handle_count()).unwrap_or(u32::MAX);
            let receipt = SafeSwapReceipt {
                schema: SAFE_SWAP_RECEIPT_SCHEMA.to_owned(),
                session_id: session_id.clone(),
                volume_id: volume_id.clone(),
                generation_id: generation_id.clone(),
                manifest_id: sealed.manifest_id.clone(),
                manifest_sha256: sealed.manifest_sha256.clone(),
                sealed_at: sealed.sealed_at.clone(),
                released_at: now_rfc3339_utc(),
                release_state: "device-released".to_owned(),
                open_handle_count: open_handles,
            };
            let idem_json = serde_json::to_value(&wrapped).unwrap_or(Value::Null);
            let idem_tuple = idempotency_key.map(|k| (principal_id, k, req_hash, idem_json));
            // Persist BOTH `stop_capture` idempotency and `safe_swap` receipt BEFORE setting
            // `volume.set_volume_released(true)` so zero writes occur on `/data` after release!
            self.receipts
                .commit_safe_swap_bundle(idem_tuple, receipt.clone(), admission_writable);
            self.volume.set_volume_released(true);
            self.close_all_volume_handles_for_safe_swap();
            Some(receipt)
        } else {
            if let Some(key) = idempotency_key {
                if let Ok(json_val) = serde_json::to_value(&wrapped) {
                    self.receipts.store_idempotency(
                        principal_id,
                        "stop_capture",
                        key,
                        req_hash,
                        json_val,
                        admission_writable,
                    );
                }
            }
            None
        };

        if let Some(receipt) = safe_swap_receipt {
            self.events.emit(
                &authority_epoch,
                done_rev,
                CaptureEventType::SafeSwap,
                Some(session_id.clone()),
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

        let run_res: StorageResult<DeleteSessionsResult> =
            (|| {
                // Refuse deletion during active capture/finalization or when volume is released,
                // and hold shared `maintenance.lock` throughout `Deleting`!
                if self.volume.is_volume_released() {
                    return Err(StorageError::locked(
                        "volume_released",
                        "cannot delete sessions while recording volume is released",
                    ));
                }
                let maintenance_lock = self.acquire_shared_maintenance_lock()?;
                {
                    let mut guard = self.state.lock().map_err(|_| {
                        StorageError::Core(CoreError::poisoned("coordinator_state"))
                    })?;
                    if guard.volume_released || self.volume.is_volume_released() {
                        return Err(StorageError::locked(
                            "volume_released",
                            "cannot delete sessions while recording volume is released",
                        ));
                    }
                    if guard.phase != CapturePhase::Idle {
                        return Err(StorageError::conflict(
                            "capture_busy",
                            format!(
                                "cannot delete sessions while coordinator is in phase {}",
                                guard.phase.as_str()
                            ),
                        ));
                    }
                    guard.transition(CapturePhase::Deleting)?;
                    self.phase_cv.notify_all();
                }

                // Always restore `Idle` phase and drop `maintenance.lock` when deletion finishes.
                let delete_outcome = self.execute_delete_preflight_and_remove(&req, &requested_ids);

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

        let result = match run_res {
            Ok(r) => r,
            Err(err) => {
                inflight_leader.fail(&err);
                return Err(err);
            }
        };
        let json_val = serde_json::to_value(&result).unwrap_or(Value::Null);
        self.receipts.store_idempotency(
            principal_id,
            "delete_sessions",
            idempotency_key,
            &req_hash,
            json_val.clone(),
            self.volume.is_mounted_and_writable(),
        );
        inflight_leader.succeed(json_val);
        Ok(IdempotentOutcome {
            value: result,
            replayed: false,
        })
    }

    fn execute_delete_preflight_and_remove(
        &self,
        req: &DeleteSessionsRequest,
        requested_ids: &HashSet<String>,
    ) -> StorageResult<DeleteSessionsResult> {
        // Preflight ALL targets BEFORE deleting a single file.
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

            // Validate canonical catalog entry fields BEFORE constructing any deletion path!
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

            // Check catalog digest AND exact on-disk manifest digest.
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

            // Continuation dependency check: every continuation of this session must also be deleted.
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

        let mut deleted_session_ids = Vec::new();
        let mut failed_sessions = Vec::new();

        for entry in ordered {
            if !is_valid_uuid_v7(&entry.session_id)
                || !is_valid_sha256(&entry.manifest_sha256)
                || !is_canonical_session_relative_dir(&entry.relative_dir)
            {
                failed_sessions.push(FailedDeleteSession {
                    session_id: entry.session_id,
                    error: "refusing to delete non-canonical session path".to_owned(),
                });
                break;
            }
            let target_dir = recordings_root.join(&entry.relative_dir);
            if let Some(parent) = target_dir.parent() {
                if let Ok(pmeta) = fs::symlink_metadata(parent) {
                    if pmeta.file_type().is_symlink() || !pmeta.is_dir() || pmeta.dev() != root_dev
                    {
                        failed_sessions.push(FailedDeleteSession {
                            session_id: entry.session_id,
                            error: "parent date directory failed symlink or st_dev check"
                                .to_owned(),
                        });
                        break;
                    }
                }
            }
            match fs::symlink_metadata(&target_dir) {
                Ok(tmeta) => {
                    if tmeta.file_type().is_symlink() || !tmeta.is_dir() || tmeta.dev() != root_dev
                    {
                        failed_sessions.push(FailedDeleteSession {
                            session_id: entry.session_id,
                            error: "session directory failed symlink or st_dev check".to_owned(),
                        });
                        break;
                    }
                    if let Err(err) = fs::remove_dir_all(&target_dir) {
                        if err.kind() != io::ErrorKind::NotFound {
                            failed_sessions.push(FailedDeleteSession {
                                session_id: entry.session_id,
                                error: format!("failed to remove session directory: {err}"),
                            });
                            // Stop further removal on storage failure and return partial result.
                            break;
                        }
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => {
                    failed_sessions.push(FailedDeleteSession {
                        session_id: entry.session_id,
                        error: format!("failed to stat session directory: {err}"),
                    });
                    break;
                }
            }

            // Clean up empty parent date directory (never removing `recordings/` itself).
            if let Some(parent) = target_dir.parent() {
                if parent != recordings_root {
                    let _ = fs::remove_dir(parent);
                    let _ = fsync_directory(&recordings_root);
                }
            }

            if let Err(err) = self.catalog.forget_path(&entry.relative_dir) {
                failed_sessions.push(FailedDeleteSession {
                    session_id: entry.session_id,
                    error: format!("failed to update session catalog: {err}"),
                });
                break;
            }

            deleted_session_ids.push(entry.session_id);
        }

        Ok(DeleteSessionsResult {
            schema: SESSION_DELETE_RESULT_SCHEMA.to_owned(),
            deleted_session_ids,
            failed_sessions,
        })
    }

    /// Enforce storage reserve thresholds during active capture (refreshing cached
    /// `remaining_bytes` at 1 Hz) and auto-recover from `Failed` / `volume_released` when
    /// an unmounted or replaced recording volume becomes mounted and writable again.
    pub fn enforce_storage_reserve(&self) -> StorageResult<ReserveEnforcementOutcome> {
        let _ = self.poll_active_failure()?;

        let (phase, volume_released, released_sig, saw_unmount, has_admission, active_session_id) = {
            let guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            (
                guard.phase,
                guard.volume_released || self.volume.is_volume_released(),
                guard.released_mount_signature.clone(),
                guard.saw_unmount_since_release,
                guard.admission.is_some(),
                guard.active.as_ref().map(|a| a.session_id.clone()),
            )
        };

        if phase != CapturePhase::Recording {
            if !phase.is_busy() && phase != CapturePhase::Closing {
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
                                None => true,
                                Some((old_vid, old_dev, old_mnt)) => {
                                    cap.device_id != *old_dev
                                        || cap.mount_identity != *old_mnt
                                        || disk_vid.as_deref().is_some_and(|v| v != old_vid)
                                }
                            };
                            if phase == CapturePhase::Failed || saw_unmount || mount_changed {
                                let _ = self.reconcile_and_admit_volume(true);
                            }
                        }
                        _ => {
                            if let Ok(mut guard) = self.state.lock() {
                                guard.saw_unmount_since_release = true;
                            }
                        }
                    }
                } else if phase == CapturePhase::Idle
                    && !has_admission
                    && self.volume.is_raw_mounted_and_writable()
                {
                    let _ = self.reconcile_and_admit_volume(true);
                }
            }
            return Ok(ReserveEnforcementOutcome::Healthy);
        }

        let Some(session_id) = active_session_id else {
            return Ok(ReserveEnforcementOutcome::Healthy);
        };

        let reserve = self.volume.check_reserve()?;
        // Refresh cached `available_bytes` on active session at 1 Hz so `capture_status()`
        // reports live `remaining_bytes` throughout long recordings!
        if let Some(rec_budget) = &reserve.recording {
            if let Ok(mut guard) = self.state.lock() {
                if let Some(ctx) = guard.active.as_mut() {
                    ctx.admission.available_bytes = rec_budget.available_bytes;
                }
                if let Some(adm) = guard.admission.as_mut() {
                    adm.available_bytes = rec_budget.available_bytes;
                }
            }
        }
        if reserve.ok {
            return Ok(ReserveEnforcementOutcome::Healthy);
        }

        if !reserve.writable {
            // Unexpected media removal / remount-ro during capture: abort handle and record media_lost.
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

        // Volume is still writable but below reserve threshold: cleanly stop & seal take.
        let _ = self.stop_capture(CaptureStopRequest::user(), None)?;
        Ok(ReserveEnforcementOutcome::StoppedForReserve {
            session_id,
            reason: reserve.message,
        })
    }

    /// Recover the current successful safe-swap handoff receipt (`GET /api/v1/capture/safe-swap`).
    pub fn safe_swap_receipt(&self) -> StorageResult<Option<SafeSwapReceiptResource>> {
        let receipt = self
            .receipts
            .get_safe_swap(self.volume.is_mounted_and_writable())?;
        Ok(receipt.map(|r| SafeSwapReceiptResource {
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
    /// `Starting`, `Stopping`, `Recovering`, or `Deleting` work before releasing `_maintenance_lock`
    /// and NEVER mutating phase or dropping `_maintenance_lock` out from under an active sealer on timeout.
    pub fn close(&self) -> StorageResult<()> {
        let active_ctx = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase == CapturePhase::Closing {
                return Ok(());
            }
            // Wait briefly for `Starting` to finish handshake; if still blocked after 150ms,
            // `Starting -> Closing` is permitted (`start_capture` holds its own `Arc<FlockGuard>`).
            let starting_deadline = Instant::now() + Duration::from_millis(150);
            while guard.phase == CapturePhase::Starting {
                let now = Instant::now();
                if now >= starting_deadline {
                    break;
                }
                let (next_guard, wait_res) = self
                    .phase_cv
                    .wait_timeout(guard, starting_deadline.saturating_duration_since(now))
                    .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
                guard = next_guard;
                if wait_res.timed_out() {
                    break;
                }
            }
            // Wait up to 30s for `Stopping`, `Recovering`, or `Deleting`; NEVER pull phase or
            // `_maintenance_lock` out from under an active sealer/recovery/delete on timeout!
            let finish_deadline = Instant::now() + Duration::from_secs(30);
            while matches!(
                guard.phase,
                CapturePhase::Stopping | CapturePhase::Recovering | CapturePhase::Deleting
            ) {
                let now = Instant::now();
                if now >= finish_deadline {
                    return Err(StorageError::conflict(
                        "capture_busy",
                        format!(
                            "timed out waiting for {} operation to finish before close",
                            guard.phase.as_str()
                        ),
                    ));
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
        if let Some(mut ctx) = active_ctx {
            if let Some(handle) = ctx.handle.take() {
                handle.abort();
            }
        }
        Ok(())
    }

    // --- Internal zero-reread seal & state helpers ---

    fn update_verification_completed(&self, completed: u64) {
        if let Ok(mut guard) = self.state.lock() {
            if let Some(ctx) = guard.active.as_mut() {
                if let Some(ver) = ctx.verification_progress.as_mut() {
                    ver.completed = completed.min(ver.total);
                }
            }
        }
    }

    fn seal_and_publish(
        &self,
        ctx: &SessionMetadata,
        captured: &CapturedSessionArtifacts,
    ) -> StorageResult<SealedPublishOutput> {
        if captured.segments.is_empty() {
            return Err(StorageError::integrity(
                "empty_recording",
                "cannot seal recording with zero stereo segments",
            ));
        }

        let staging_root = &ctx.staging_dir;
        let declared_log_paths: Vec<String> =
            captured.logs.iter().map(|l| l.path.clone()).collect();
        let mut verified_count = 0_u64;
        let mut expected_identities: Vec<ExpectedArtifactIdentity> = Vec::new();
        let mut segments_json = Vec::with_capacity(captured.segments.len());

        for seg in &captured.segments {
            let left_digest = finalize_artifact_with_digest(
                &staging_root.join(&seg.left_path),
                Some(seg.left_bytes),
                seg.left_sha256.as_deref(),
            )
            .map_err(|e| StorageError::integrity("seal_artifact_invalid", e.message))?;
            let right_digest = finalize_artifact_with_digest(
                &staging_root.join(&seg.right_path),
                Some(seg.right_bytes),
                seg.right_sha256.as_deref(),
            )
            .map_err(|e| StorageError::integrity("seal_artifact_invalid", e.message))?;

            expected_identities.push(ExpectedArtifactIdentity {
                path: seg.left_path.clone(),
                identity: left_digest.identity,
            });
            expected_identities.push(ExpectedArtifactIdentity {
                path: seg.right_path.clone(),
                identity: right_digest.identity,
            });
            verified_count = verified_count.saturating_add(2);
            self.update_verification_completed(verified_count);

            segments_json.push(json!({
                "index": seg.index,
                "start_frame": seg.start_frame,
                "end_frame": seg.end_frame,
                "start_time_seconds": seg.start_time_seconds,
                "end_time_seconds": seg.end_time_seconds,
                "artifacts": {
                    "left": {
                        "artifact_id": left_digest.sha256,
                        "role": "video.left",
                        "path": seg.left_path,
                        "media_type": "video/mp4",
                        "bytes": left_digest.identity.size,
                        "sha256": left_digest.sha256,
                    },
                    "right": {
                        "artifact_id": right_digest.sha256,
                        "role": "video.right",
                        "path": seg.right_path,
                        "media_type": "video/mp4",
                        "bytes": right_digest.identity.size,
                        "sha256": right_digest.sha256,
                    }
                }
            }));
        }

        let frames_digest = finalize_artifact_with_digest(
            &staging_root.join(&captured.frames_artifact.path),
            Some(captured.frames_artifact.bytes),
            captured.frames_artifact.sha256.as_deref(),
        )
        .map_err(|e| StorageError::integrity("seal_artifact_invalid", e.message))?;
        expected_identities.push(ExpectedArtifactIdentity {
            path: captured.frames_artifact.path.clone(),
            identity: frames_digest.identity,
        });
        verified_count = verified_count.saturating_add(1);
        self.update_verification_completed(verified_count);

        let imu_digest = finalize_artifact_with_digest(
            &staging_root.join(&captured.imu_artifact.path),
            Some(captured.imu_artifact.bytes),
            captured.imu_artifact.sha256.as_deref(),
        )
        .map_err(|e| StorageError::integrity("seal_artifact_invalid", e.message))?;
        expected_identities.push(ExpectedArtifactIdentity {
            path: captured.imu_artifact.path.clone(),
            identity: imu_digest.identity,
        });
        verified_count = verified_count.saturating_add(1);
        self.update_verification_completed(verified_count);

        let audio_json = match &captured.audio {
            CapturedAudioOutcome::Recorded {
                requested_mode,
                sample_rate,
                channels,
                sample_count,
                start_time_seconds,
                end_time_seconds,
                segments,
            } => self.finalize_recorded_audio_json(
                staging_root,
                requested_mode,
                *sample_rate,
                *channels,
                *sample_count,
                *start_time_seconds,
                *end_time_seconds,
                segments,
                None,
                &mut expected_identities,
                &mut verified_count,
            )?,
            CapturedAudioOutcome::RecordedWithClock {
                requested_mode,
                sample_rate,
                channels,
                sample_count,
                start_time_seconds,
                end_time_seconds,
                segments,
                capture_clock,
            } => self.finalize_recorded_audio_json(
                staging_root,
                requested_mode,
                *sample_rate,
                *channels,
                *sample_count,
                *start_time_seconds,
                *end_time_seconds,
                segments,
                capture_clock.as_ref(),
                &mut expected_identities,
                &mut verified_count,
            )?,
            CapturedAudioOutcome::NotRecorded {
                requested_mode,
                reason,
            } => {
                // Clean/quarantine any leftover partial `audio/` files before publishing a
                // `NotRecorded` session (preserving any path explicitly declared in `logs`).
                clean_leftover_not_recorded_audio_files(
                    self.volume.paths(),
                    staging_root,
                    &ctx.session_id,
                    &declared_log_paths,
                );
                json!({
                    "state": "not_recorded",
                    "requested_mode": requested_mode,
                    "resolved_mode": "disabled",
                    "reason": reason,
                })
            }
        };

        let mut logs_json = Vec::with_capacity(captured.logs.len());
        for log_art in &captured.logs {
            let ldig = finalize_artifact_with_digest(
                &staging_root.join(&log_art.path),
                Some(log_art.bytes),
                log_art.sha256.as_deref(),
            )
            .map_err(|e| StorageError::integrity("seal_artifact_invalid", e.message))?;
            expected_identities.push(ExpectedArtifactIdentity {
                path: log_art.path.clone(),
                identity: ldig.identity,
            });
            verified_count = verified_count.saturating_add(1);
            self.update_verification_completed(verified_count);
            logs_json.push(json!({
                "artifact_id": ldig.sha256,
                "role": log_art.role,
                "path": log_art.path,
                "media_type": log_art.media_type,
                "bytes": ldig.identity.size,
                "sha256": ldig.sha256,
            }));
        }

        let sealed_at = now_rfc3339_utc();
        let manifest_id = new_uuid_v7();
        let profile_str = match self.config.encoding.codec {
            VideoCodec::H264 => "high",
            VideoCodec::Hevc => "main",
        };
        let manifest_value = json!({
            "schema": DEVICE_SESSION_SCHEMA,
            "manifest_id": manifest_id,
            "sealed": true,
            "sealed_at": sealed_at,
            "session_id": ctx.session_id,
            "volume_id": ctx.admission.volume_id,
            "capture_mode": ctx.mode.as_str(),
            "display_name": ctx.display_name,
            "device": {
                "device_id": self.config.device_id,
                "device_label": self.config.device_label,
                "hardware_fingerprint": self.config.hardware_fingerprint,
                "platform": self.config.platform,
                "software_version": self.config.software_version,
                "commit": self.config.commit,
            },
            "time": {
                "started_at": ctx.started_at,
                "ended_at": sealed_at,
                "timezone": self.config.timezone,
                "duration_seconds": captured.duration_seconds.max(0.0),
                "duration_clock": "host_monotonic",
            },
            "take": {
                "take_id": ctx.take_id,
                "sequence": ctx.take_sequence,
                "continuation_of": ctx.continuation_of,
            },
            "camera": {
                "width": self.config.camera_width,
                "height": self.config.camera_height,
                "eye_width": (self.config.camera_width / 2).max(1),
                "sensor_fps": self.config.sensor_fps,
                "frame_decimation": self.config.frame_decimation,
                "effective_fps": captured.effective_fps.max(0.0),
                "coordinate_frame": "opencv_optical",
            },
            "video": {
                "layout": "split-eyes",
                "codec": self.config.encoding.codec.as_str(),
                "container": "mp4",
                "segments": segments_json,
                "encoding": build_manifest_encoding_json(&self.config.encoding, profile_str),
            },
            "imu": {
                "artifact": {
                    "artifact_id": imu_digest.sha256,
                    "role": "imu.samples",
                    "path": captured.imu_artifact.path,
                    "media_type": captured.imu_artifact.media_type,
                    "bytes": imu_digest.identity.size,
                    "sha256": imu_digest.sha256,
                },
                "sample_count": captured.imu_sample_count,
                "units": "raw_int16",
                "coordinate_frame": "raw_device_axes",
            },
            "frames": {
                "artifact": {
                    "artifact_id": frames_digest.sha256,
                    "role": "frames.index",
                    "path": captured.frames_artifact.path,
                    "media_type": captured.frames_artifact.media_type,
                    "bytes": frames_digest.identity.size,
                    "sha256": frames_digest.sha256,
                },
                "count": captured.frames_count,
            },
            "audio": audio_json,
            "logs": logs_json,
            "integrity": {
                "verified_at": sealed_at,
                "dropped_frames": captured.dropped_frames,
                "drop_events": captured.drop_events,
                "fatal_errors": [],
            },
        });

        let mut manifest_bytes =
            serde_json::to_vec_pretty(&manifest_value).map_err(|err| StorageError::Catalog {
                message: format!("serialize session manifest failed: {err}"),
            })?;
        manifest_bytes.push(b'\n');
        let manifest_sha256 = sha256_hex(&manifest_bytes);

        // Zero-reread pre-publish identity verification via `miro-ego-capture::session_io`.
        verify_device_session_artifacts(
            staging_root,
            &manifest_bytes,
            &ctx.session_id,
            &expected_identities,
        )
        .map_err(|e| StorageError::integrity("seal_verify_failed", e.message))?;

        let manifest_path = staging_root.join("manifest.json");
        write_bytes_atomic(&manifest_path, &manifest_bytes, 0o640, true).map_err(|err| {
            StorageError::io("manifest_write_failed", "write sealed manifest.json", err)
        })?;
        let _ = fsync_directory(staging_root);

        let relative_dir =
            match publish_staging_directory(self.volume.paths(), staging_root, &ctx.started_at) {
                Ok(rel) => rel,
                Err(pub_err) => {
                    // Leave `.partial` intact in `.miro-ego/staging/` (unlinking candidate `manifest.json`
                    // so control markers remain authoritative for later crash recovery).
                    let _ = fs::remove_file(&manifest_path);
                    let _ = fsync_directory(staging_root);
                    return Err(pub_err);
                }
            };
        let final_root = self.volume.paths().recordings_dir().join(&relative_dir);

        // Step 1 post-publish: remove control markers and verify published artifacts on `final_root`.
        let verify_published_res: StorageResult<IndexedSession> = (|| {
            remove_control_markers_with_logs(&final_root, &declared_log_paths)?;
            let _ = fsync_directory(&final_root);

            // Post-publish exFAT-safe artifact identity verification (zero disk reread).
            verify_device_session_artifacts(
                &final_root,
                &manifest_bytes,
                &ctx.session_id,
                &expected_identities,
            )
            .map_err(|e| StorageError::integrity("post_publish_verify_failed", e.message))?;

            let mut indexed = parse_indexed_session_from_manifest(
                &relative_dir,
                &manifest_value,
                &manifest_sha256,
            )?;
            let build_sha256 = sha256_hex(
                format!(
                    "miro-ego:{}:{}",
                    self.config.software_version, self.config.commit
                )
                .as_bytes(),
            );
            indexed.verification = Some(crate::catalog::GatewayVerification {
                actor: "gateway".to_owned(),
                validator: crate::catalog::GatewayValidatorInfo {
                    name: "miro-ego".to_owned(),
                    version: self.config.software_version.clone(),
                    build_sha256,
                },
                manifest_sha256: manifest_sha256.clone(),
                verified_at: sealed_at.clone(),
                verdict: "usable".to_owned(),
                diagnostics: Vec::new(),
            });
            Ok(indexed)
        })();

        let indexed = match verify_published_res {
            Ok(idx) => idx,
            Err(post_err) => {
                // NEVER call `fs::remove_dir_all(&final_root)`! Only quarantine if non-transient.
                if !is_transient_storage_error(&post_err) {
                    let q_base = format!("{}.postpublish", ctx.session_id);
                    if quarantine_session_directory(self.volume.paths(), &final_root, &q_base)
                        .is_ok()
                    {
                        cleanup_empty_date_parent(
                            &self.volume.paths().recordings_dir(),
                            &final_root,
                        );
                    }
                }
                return Err(post_err);
            }
        };

        // Step 2 post-publish: `final_root` IS durably sealed & verified!
        // If `catalog.admit_indexed_session` fails due to transient storage error (`ENOSPC`/`EIO`/`EROFS`),
        // admit into the in-memory `BinarySessionCatalog` (`admit_in_memory`) and succeed!
        if let Err(admit_err) = self.catalog.admit_indexed_session(indexed.clone()) {
            if is_transient_storage_error(&admit_err) {
                self.catalog.admit_in_memory(indexed)?;
            } else {
                let q_base = format!("{}.postpublish", ctx.session_id);
                if quarantine_session_directory(self.volume.paths(), &final_root, &q_base).is_ok() {
                    cleanup_empty_date_parent(&self.volume.paths().recordings_dir(), &final_root);
                }
                return Err(admit_err);
            }
        }

        Ok(SealedPublishOutput {
            manifest_id,
            manifest_sha256,
            sealed_at,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn finalize_recorded_audio_json(
        &self,
        staging_root: &Path,
        requested_mode: &str,
        sample_rate: u32,
        channels: u16,
        sample_count: u64,
        start_time_seconds: f64,
        end_time_seconds: f64,
        segments: &[CapturedAudioSegment],
        capture_clock: Option<&Value>,
        expected_identities: &mut Vec<ExpectedArtifactIdentity>,
        verified_count: &mut u64,
    ) -> StorageResult<Value> {
        let mut audio_segs_json = Vec::with_capacity(segments.len());
        for aseg in segments {
            let adig = finalize_artifact_with_digest(
                &staging_root.join(&aseg.artifact.path),
                Some(aseg.artifact.bytes),
                aseg.artifact.sha256.as_deref(),
            )
            .map_err(|e| StorageError::integrity("seal_artifact_invalid", e.message))?;
            expected_identities.push(ExpectedArtifactIdentity {
                path: aseg.artifact.path.clone(),
                identity: adig.identity,
            });
            *verified_count = verified_count.saturating_add(1);
            self.update_verification_completed(*verified_count);
            audio_segs_json.push(json!({
                "index": aseg.index,
                "start_sample": aseg.start_sample,
                "end_sample": aseg.end_sample,
                "start_time_seconds": aseg.start_time_seconds,
                "end_time_seconds": aseg.end_time_seconds,
                "pcm_payload_bytes": aseg.pcm_payload_bytes,
                "wav_header_bytes": aseg.wav_header_bytes,
                "artifact": {
                    "artifact_id": adig.sha256,
                    "role": "audio.wav",
                    "path": aseg.artifact.path,
                    "media_type": aseg.artifact.media_type,
                    "bytes": adig.identity.size,
                    "sha256": adig.sha256,
                }
            }));
        }

        // Also check `staging_root/audio/capture-clock.json` on disk if caller didn't pass `capture_clock`.
        let disk_clock = if capture_clock.is_none() {
            read_bounded_json::<Value>(
                &staging_root.join("audio/capture-clock.json"),
                64 * 1024,
                16,
            )
            .ok()
            .filter(|v| v.get("schema").and_then(Value::as_str) == Some("miro-ego.audio-clock.v1"))
        } else {
            None
        };

        let mut obj = json!({
            "state": "recorded",
            "requested_mode": requested_mode,
            "resolved_mode": "enabled",
            "codec": "pcm_s16le",
            "container": "wav",
            "sample_format": "S16_LE",
            "sample_rate": sample_rate,
            "channels": channels,
            "sample_count": sample_count,
            "sync": {
                "time_base": "host_monotonic",
                "start_time_seconds": start_time_seconds,
                "end_time_seconds": end_time_seconds,
                "video_time_reference": "session_time_seconds",
            },
            "segments": audio_segs_json,
        });
        if let Some(clk) = capture_clock.or(disk_clock.as_ref()) {
            if clk.get("schema").and_then(Value::as_str) == Some("miro-ego.audio-clock.v1") {
                if let Some(map) = obj.as_object_mut() {
                    map.insert("capture_clock".to_owned(), clk.clone());
                }
            }
        }
        Ok(obj)
    }

    fn fail_start_attempt(
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
            let released = guard.active.take();
            let _ = guard.transition(CapturePhase::Idle);
            self.phase_cv.notify_all();
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

    fn record_unsuccessful_stop(
        &self,
        ctx: &SessionMetadata,
        progress: CaptureProgressSnapshot,
        terminal_state: &str,
        error_code: &str,
        error_message: &str,
        media_lost: bool,
    ) -> CaptureStatusSnapshot {
        // Isolate `.partial` into `quarantine/` ONLY for non-recoverable terminal failures (`failed`/`abandoned`)
        // when the volume is writable; NEVER call `fs::remove_dir_all(&ctx.staging_dir)` and NEVER
        // quarantine `"recoverable"` `.partial` directories left behind on `ENOSPC`!
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

    fn build_status_snapshot_locked(
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

struct SealedPublishOutput {
    manifest_id: String,
    manifest_sha256: String,
    sealed_at: String,
}

fn count_captured_artifacts(captured: &CapturedSessionArtifacts) -> u64 {
    let audio_count = match &captured.audio {
        CapturedAudioOutcome::Recorded { segments, .. }
        | CapturedAudioOutcome::RecordedWithClock { segments, .. } => segments.len() as u64,
        CapturedAudioOutcome::NotRecorded { .. } => 0,
    };
    (captured.segments.len() as u64)
        .saturating_mul(2)
        .saturating_add(2)
        .saturating_add(audio_count)
        .saturating_add(captured.logs.len() as u64)
}

fn sum_captured_artifact_bytes(captured: &CapturedSessionArtifacts) -> u64 {
    let mut total = captured
        .frames_artifact
        .bytes
        .saturating_add(captured.imu_artifact.bytes);
    for seg in &captured.segments {
        total = total
            .saturating_add(seg.left_bytes)
            .saturating_add(seg.right_bytes);
    }
    match &captured.audio {
        CapturedAudioOutcome::Recorded { segments, .. }
        | CapturedAudioOutcome::RecordedWithClock { segments, .. } => {
            for aseg in segments {
                total = total.saturating_add(aseg.artifact.bytes);
            }
        }
        CapturedAudioOutcome::NotRecorded { .. } => {}
    }
    for log in &captured.logs {
        total = total.saturating_add(log.bytes);
    }
    total
}

/// Remove or quarantine any leftover partial files inside `staging_root/audio` when sealing
/// `CapturedAudioOutcome::NotRecorded` (preserving any path explicitly declared in `manifest.logs`).
fn clean_leftover_not_recorded_audio_files(
    paths: &DataVolumePaths,
    staging_root: &Path,
    session_id: &str,
    declared_log_paths: &[String],
) {
    let audio_dir = staging_root.join("audio");
    if !audio_dir.is_dir() {
        return;
    }
    let has_declared_audio_log = declared_log_paths
        .iter()
        .any(|p| p == "audio" || p.starts_with("audio/"));
    if !has_declared_audio_log {
        let q_name = format!("{session_id}.not-recorded-audio");
        if quarantine_session_directory(paths, &audio_dir, &q_name).is_ok() {
            let _ = fsync_directory(staging_root);
            return;
        }
    }

    let Ok(entries) = fs::read_dir(&audio_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        let rel = format!("audio/{name}");
        if declared_log_paths.iter().any(|p| p == &rel) {
            continue;
        }
        let path = entry.path();
        if let Ok(meta) = fs::symlink_metadata(&path) {
            if meta.is_dir() && !meta.file_type().is_symlink() {
                let _ = quarantine_session_directory(
                    paths,
                    &path,
                    &format!("{session_id}.audio-{name}"),
                )
                .or_else(|_| fs::remove_dir(&path).map(|()| String::new()));
            } else {
                let _ = fs::remove_file(&path);
            }
        }
    }
    let _ = fs::remove_dir(&audio_dir);
    let _ = fsync_directory(staging_root);
}

/// Safely clean up an aborted pre-recording staging directory without invoking `fs::remove_dir_all`
/// on populated recording directories: unlinks known start markers (`capture.json`, `recording.json`,
/// `.tmp` files) and removes empty `video/`, `audio/`, and `<session_id>.partial` directories,
/// falling back to moving into `quarantine/` if the backend already wrote files during `begin_capture`.
fn cleanup_empty_staging_attempt(staging_dir: &Path) {
    if !staging_dir.exists() {
        return;
    }
    for marker in ["capture.json", "recording.json", "checkpoint.json"] {
        let _ = fs::remove_file(staging_dir.join(marker));
    }
    let _ = sweep_stale_atomic_temps(staging_dir);
    let _ = fs::remove_dir(staging_dir.join("video"));
    let _ = fs::remove_dir(staging_dir.join("audio"));
    if fs::remove_dir(staging_dir).is_err() {
        // Backend wrote files into `staging_dir` before failing `begin_capture`: move to quarantine!
        if let Some(meta_dir) = staging_dir.parent().and_then(Path::parent) {
            let quarantine_root = meta_dir.join("quarantine");
            if fs::create_dir_all(&quarantine_root).is_ok() {
                if let Some(name) = staging_dir.file_name().and_then(|n| n.to_str()) {
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
    }
}

fn validate_start_request(req: &CaptureStartRequest) -> StorageResult<()> {
    if req.schema != CAPTURE_START_SCHEMA {
        return Err(StorageError::invalid_input(
            "invalid_request",
            format!("capture start schema must be {CAPTURE_START_SCHEMA}"),
        ));
    }
    if let Some(vid) = &req.volume_id {
        if !is_valid_uuid_v4(vid) {
            return Err(StorageError::invalid_input(
                "invalid_request",
                "volume_id must be a canonical lowercase UUIDv4",
            ));
        }
    }
    if let Some(name) = &req.display_name {
        let char_count = name.chars().count();
        if !(1..=160).contains(&char_count) {
            return Err(StorageError::invalid_input(
                "invalid_request",
                "display_name must be 1..=160 characters",
            ));
        }
    }
    if let TakeRequest::Continue { continuation_of } = &req.take {
        if !is_valid_uuid_v7(continuation_of) {
            return Err(StorageError::invalid_input(
                "invalid_request",
                "take.continuation_of must be a canonical lowercase UUIDv7",
            ));
        }
    }
    Ok(())
}

fn map_start_volume_error(err: StorageError) -> StorageError {
    match err {
        StorageError::Unavailable { message } => {
            StorageError::unprocessable("storage_unavailable", message)
        }
        StorageError::VolumeInvalid { message } => {
            StorageError::unprocessable("volume_invalid", message)
        }
        other => other,
    }
}

fn validate_idempotency_key(key: &str) -> StorageResult<()> {
    if !(1..=128).contains(&key.len())
        || !key.bytes().all(|b| (0x21..=0x7e).contains(&b) && b != b'"')
    {
        return Err(StorageError::invalid_input(
            "invalid_idempotency_key",
            "Idempotency-Key must be 1..=128 visible ASCII chars excluding spaces and quotes",
        ));
    }
    Ok(())
}

fn request_fingerprint<T: Serialize>(value: &T) -> StorageResult<String> {
    let bytes = serde_json::to_vec(value)
        .map_err(|err| StorageError::invalid_input("request_encode_failed", err.to_string()))?;
    Ok(sha256_hex(&bytes))
}

/// Sanitize a diagnostic code to strictly satisfy `^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$` (`<= 96` chars):
/// collapses consecutive `[._-]` separators, strips leading/trailing separators, and falls back to `"capture_error"`.
pub fn sanitize_diag_code(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len().min(96));
    let mut pending_sep: Option<char> = None;

    for ch in raw.chars() {
        if out.len() >= 96 {
            break;
        }
        let mapped = if ch.is_ascii_lowercase() || ch.is_ascii_digit() {
            Some((ch, false))
        } else if ch.is_ascii_uppercase() {
            Some((ch.to_ascii_lowercase(), false))
        } else if matches!(ch, '.' | '_' | '-') {
            Some((ch, true))
        } else {
            Some(('_', true))
        };

        if let Some((c, is_sep)) = mapped {
            if is_sep {
                if !out.is_empty() && pending_sep.is_none() {
                    pending_sep = Some(c);
                }
            } else {
                if let Some(sep) = pending_sep.take() {
                    if out.len() + 2 > 96 {
                        break;
                    }
                    out.push(sep);
                }
                out.push(c);
            }
        }
    }

    while out.ends_with(['.', '_', '-']) {
        out.pop();
    }

    if out.is_empty() || !out.as_bytes()[0].is_ascii_lowercase() {
        "capture_error".to_owned()
    } else {
        out
    }
}

fn sanitize_diag_message(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        "capture failed".to_owned()
    } else {
        raw.chars().take(1024).collect()
    }
}
