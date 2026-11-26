//! Full Rust `CaptureCoordinator` with explicit `CapturePhase` FSM, trait-abstracted
//! `CaptureBackend`, zero-reread sealing, readable collision-safe publish, bounded
//! receipt persistence with unmounted RAM fallback, and `miro-ego.capture-event.v1`
//! event broadcasting.

use crate::artifact_io::open_manifest;
use crate::catalog::{
    BinarySessionCatalog, DEVICE_SESSION_SCHEMA, IndexedSession, SessionDeviceIdentity,
    parse_indexed_session_from_manifest,
};
use crate::recovery::{
    RecoveryDefaults, StagingRecoveryManager, build_manifest_encoding_json,
    publish_staging_directory, remove_control_markers,
};
use crate::util::{now_rfc3339_utc, sha256_hex};
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
use miro_ego_core::sys::fs::{fsync_directory, read_bounded_json, write_bytes_atomic};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
    NotRecorded {
        requested_mode: String,
        reason: String,
    },
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IdempotencyRecord {
    key: String,
    operation: String,
    request_hash: String,
    response_json: Value,
    recorded_at: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedReceipts {
    idempotency: VecDeque<IdempotencyRecord>,
    safe_swap: Option<SafeSwapReceipt>,
    unsuccessful: VecDeque<RetainedUnsuccessfulSessionResource>,
}

struct ReceiptStore {
    paths: DataVolumePaths,
    state: Mutex<PersistedReceipts>,
}

impl ReceiptStore {
    fn open(paths: DataVolumePaths, volume_writable: bool) -> Self {
        let loaded = if volume_writable {
            let file = paths.receipts_dir().join("coordinator-receipts.json");
            read_bounded_json::<PersistedReceipts>(&file, MAX_RECEIPT_FILE_BYTES, 16)
                .unwrap_or_default()
        } else {
            PersistedReceipts::default()
        };
        Self {
            paths,
            state: Mutex::new(loaded),
        }
    }

    fn lookup_idempotency(
        &self,
        operation: &str,
        key: &str,
        request_hash: &str,
    ) -> StorageResult<Option<Value>> {
        let guard = self
            .state
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("receipt_store")))?;
        if let Some(found) = guard
            .idempotency
            .iter()
            .find(|r| r.operation == operation && r.key == key)
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

    fn store_idempotency(
        &self,
        operation: &str,
        key: &str,
        request_hash: &str,
        response_json: Value,
        volume_writable: bool,
    ) {
        let snapshot = {
            let Ok(mut guard) = self.state.lock() else {
                return;
            };
            guard
                .idempotency
                .retain(|r| !(r.operation == operation && r.key == key));
            while guard.idempotency.len() >= MAX_IDEMPOTENCY_ENTRIES {
                guard.idempotency.pop_front();
            }
            guard.idempotency.push_back(IdempotencyRecord {
                key: key.to_owned(),
                operation: operation.to_owned(),
                request_hash: request_hash.to_owned(),
                response_json,
                recorded_at: now_rfc3339_utc(),
            });
            guard.clone()
        };
        if volume_writable {
            self.persist_best_effort(&snapshot);
        }
    }

    fn set_safe_swap(&self, receipt: Option<SafeSwapReceipt>, volume_writable: bool) {
        let snapshot = {
            let Ok(mut guard) = self.state.lock() else {
                return;
            };
            guard.safe_swap = receipt;
            guard.clone()
        };
        if volume_writable {
            self.persist_best_effort(&snapshot);
        }
    }

    fn get_safe_swap(&self) -> StorageResult<Option<SafeSwapReceipt>> {
        let guard = self
            .state
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("receipt_store")))?;
        Ok(guard.safe_swap.clone())
    }

    fn record_unsuccessful(
        &self,
        resource: RetainedUnsuccessfulSessionResource,
        volume_writable: bool,
    ) {
        let snapshot = {
            let Ok(mut guard) = self.state.lock() else {
                return;
            };
            let sid = resource.outcome.recording_state.session_id.clone();
            guard
                .unsuccessful
                .retain(|item| item.outcome.recording_state.session_id != sid);
            while guard.unsuccessful.len() >= MAX_UNSUCCESSFUL_OUTCOMES {
                guard.unsuccessful.pop_front();
            }
            guard.unsuccessful.push_back(resource);
            guard.clone()
        };
        if volume_writable {
            self.persist_best_effort(&snapshot);
        }
    }

    fn get_unsuccessful(
        &self,
        session_id: &str,
    ) -> StorageResult<Option<RetainedUnsuccessfulSessionResource>> {
        let guard = self
            .state
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("receipt_store")))?;
        Ok(guard
            .unsuccessful
            .iter()
            .find(|item| item.outcome.recording_state.session_id == session_id)
            .cloned())
    }

    /// Persist receipts strictly onto `/data/.miro-ego/receipts/` when mounted and writable.
    /// Never creates `/data` on the root system disk!
    fn persist_best_effort(&self, snapshot: &PersistedReceipts) {
        let receipts_dir = self.paths.receipts_dir();
        if !receipts_dir.is_dir() {
            return;
        }
        let Ok(mut bytes) = serde_json::to_vec(snapshot) else {
            return;
        };
        bytes.push(b'\n');
        if bytes.len() > MAX_RECEIPT_FILE_BYTES {
            return;
        }
        let _ = write_bytes_atomic(
            &receipts_dir.join("coordinator-receipts.json"),
            &bytes,
            0o640,
            true,
        );
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
    verification_progress: Option<VerificationCounter>,
    diagnostics: Vec<RecordingDiagnostic>,
    timer_deadline_ns: Option<u64>,
    timer_duration_seconds: Option<u32>,
    handle: Option<Box<dyn ActiveCaptureHandle>>,
    _maintenance_lock: Option<FlockGuard>,
}

struct CoordinatorState {
    phase: CapturePhase,
    authority_epoch: String,
    source_revision: u64,
    admission: Option<VolumeAdmission>,
    volume_released: bool,
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
        self.source_revision = self.source_revision.saturating_add(1);
        Ok(())
    }

    fn bump_revision(&mut self) -> u64 {
        self.source_revision = self.source_revision.saturating_add(1);
        self.source_revision
    }
}

/// Authoritative native Rust capture coordinator managing the capture FSM, volume reserve
/// enforcement, zero-reread sealing, catalog admission, batch deletion, and events.
pub struct CaptureCoordinator {
    config: CoordinatorConfig,
    volume: Arc<ExfatVolumeManager>,
    catalog: Arc<BinarySessionCatalog>,
    backend: Arc<dyn CaptureBackend>,
    receipts: ReceiptStore,
    events: CaptureEventBroadcaster,
    state: Mutex<CoordinatorState>,
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

        let admission = volume.admit(None).ok();
        let volume_writable = admission.as_ref().is_some_and(|a| a.writable);
        let receipts = ReceiptStore::open(volume.paths().clone(), volume_writable);

        // Invalidate stale safe-swap receipt if it does not match the current mount generation.
        if let Ok(Some(existing_receipt)) = receipts.get_safe_swap() {
            let matches_current = admission.as_ref().is_some_and(|a| {
                a.volume_id == existing_receipt.volume_id
                    && a.generation_id == existing_receipt.generation_id
            });
            if !matches_current {
                receipts.set_safe_swap(None, volume_writable);
            }
        }

        Self {
            config,
            volume,
            catalog,
            backend,
            receipts,
            events: CaptureEventBroadcaster::new(),
            state: Mutex::new(CoordinatorState {
                phase: CapturePhase::Idle,
                authority_epoch: new_uuid_v4(),
                source_revision: 0,
                admission,
                volume_released: false,
                active: None,
                retained_unsuccessful: None,
                timer_settings,
                last_failure_reason: None,
            }),
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

    pub fn phase(&self) -> StorageResult<CapturePhase> {
        let guard = self
            .state
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
        Ok(guard.phase)
    }

    /// Run crash prefix recovery on startup (`Idle -> Recovering -> Idle`).
    pub fn recover_on_startup(&self) -> StorageResult<RecoveryReport> {
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
        }

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
            (guard.authority_epoch.clone(), guard.source_revision)
        };

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
        let mut guard = self
            .state
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
        if guard.phase != CapturePhase::Failed {
            return Ok(guard.phase);
        }
        guard.last_failure_reason = None;
        guard.volume_released = false;
        guard.admission = self.volume.admit(None).ok();
        guard.transition(CapturePhase::Idle)?;
        Ok(guard.phase)
    }

    /// Re-admit a remounted or newly inserted volume, atomically invalidating any prior
    /// safe-swap handoff receipt (OpenAPI requirement: remounting creates a new `generation_id`).
    pub fn refresh_volume_admission(&self) -> StorageResult<VolumeAdmission> {
        let prev = {
            let guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase.is_busy() {
                return Err(StorageError::conflict(
                    "capture_busy",
                    "cannot refresh volume admission while capture is busy",
                ));
            }
            guard.admission.clone()
        };

        let admitted = self.volume.admit(None)?;
        let changed = prev.as_ref().is_none_or(|p| {
            p.volume_id != admitted.volume_id || p.generation_id != admitted.generation_id
        });
        if changed {
            self.receipts.set_safe_swap(None, admitted.writable);
        }

        let mut guard = self
            .state
            .lock()
            .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
        guard.admission = Some(admitted.clone());
        guard.volume_released = false;
        guard.bump_revision();
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
            return Err(StorageError::Unavailable {
                message: "camera is not connected".to_owned(),
            });
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

        let req_hash = request_fingerprint(&req)?;
        if let Some(key) = idempotency_key {
            validate_idempotency_key(key)?;
            if let Some(cached) =
                self.receipts
                    .lookup_idempotency("set_camera_focus", key, &req_hash)?
            {
                let status: CameraFocusStatus =
                    serde_json::from_value(cached).map_err(|err| StorageError::Catalog {
                        message: format!("corrupt cached focus receipt: {err}"),
                    })?;
                return Ok(IdempotentOutcome {
                    value: status,
                    replayed: true,
                });
            }
        }

        if !self.backend.is_camera_connected() {
            return Err(StorageError::Unavailable {
                message: "camera is not connected".to_owned(),
            });
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

        let updated = self.backend.set_camera_focus(&req)?;
        if let Some(key) = idempotency_key {
            if let Ok(json_val) = serde_json::to_value(&updated) {
                self.receipts.store_idempotency(
                    "set_camera_focus",
                    key,
                    &req_hash,
                    json_val,
                    self.volume.is_mounted_and_writable(),
                );
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
        let active = guard.active.as_ref().and_then(|ctx| {
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
        });
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

    /// Check whether an active capture timer has expired and trigger normal stop/seal if so.
    ///
    /// Returns `Ok(true)` if the timer expired and stopped the capture, `Ok(false)` otherwise.
    pub fn poll_capture_timer(&self) -> StorageResult<bool> {
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
        validate_start_request(&req)?;
        let req_hash = request_fingerprint(&req)?;
        if let Some(key) = idempotency_key {
            validate_idempotency_key(key)?;
            if let Some(cached) =
                self.receipts
                    .lookup_idempotency("start_capture", key, &req_hash)?
            {
                let snap: CaptureStatusSnapshot =
                    serde_json::from_value(cached).map_err(|e| StorageError::Catalog {
                        message: format!("corrupt cached start receipt: {e}"),
                    })?;
                return Ok(IdempotentOutcome {
                    value: snap,
                    replayed: true,
                });
            }
        }

        // Resolve take identity & validate continuation predecessor before side effects.
        let (take_id, take_sequence, continuation_of) = match &req.take {
            TakeRequest::New => (new_uuid_v7(), 1_u64, None),
            TakeRequest::Continue { continuation_of } => {
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
                (
                    pred.take_id,
                    pred.take_sequence.saturating_add(1),
                    Some(continuation_of.clone()),
                )
            }
        };

        if !self.backend.is_camera_connected() {
            return Err(StorageError::Unavailable {
                message: "camera_not_connected: configured camera is not connected".to_owned(),
            });
        }

        // Verify volume admission and reserve budgets BEFORE transitioning FSM or creating files.
        let admission = self.volume.admit(None)?;
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
            return Err(StorageError::unprocessable(
                "insufficient_space",
                reserve.message,
            ));
        }

        // Acquire maintenance shared lock when run_dir is configured so idle fstrim / watchdog defer.
        let maintenance_lock = match &self.config.run_dir {
            Some(run_dir) => Some(
                acquire_maintenance_lock(run_dir, LockMode::Shared, true)
                    .map_err(StorageError::from)?,
            ),
            None => None,
        };

        let session_id = new_uuid_v7();
        let started_at = now_rfc3339_utc();
        let started_monotonic_ns = monotonic_ns().unwrap_or(1).max(1);
        let display_name = req
            .display_name
            .clone()
            .unwrap_or_else(|| format!("Miro-Ego {started_at}"));

        // Enter `Starting` phase and atomically invalidate any previous safe-swap receipt.
        let (authority_epoch, timer_settings) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase != CapturePhase::Idle {
                return Err(StorageError::conflict(
                    "capture_busy",
                    format!(
                        "cannot start capture while coordinator is in phase {}",
                        guard.phase.as_str()
                    ),
                ));
            }
            if guard.volume_released {
                return Err(StorageError::locked(
                    "volume_released",
                    "recording volume was released for safe swap",
                ));
            }
            guard.transition(CapturePhase::Starting)?;
            guard.admission = Some(admission.clone());
            // Starting a new take removes retained_unsuccessful from current status,
            // while NEVER deleting per-session retained unsuccessful resources in receipts.
            guard.retained_unsuccessful = None;
            (guard.authority_epoch.clone(), guard.timer_settings)
        };
        self.receipts.set_safe_swap(None, admission.writable);

        // Prepare staging directory `/data/.miro-ego/staging/<session_id>.partial`.
        let staging_dir = self
            .volume
            .paths()
            .staging_dir()
            .join(format!("{session_id}.partial"));
        if let Err(err) = fs::create_dir_all(staging_dir.join("video")) {
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
            return Err(storage_err);
        }

        // Write `capture.json` metadata marker so crash recovery can reconstruct full provenance.
        let capture_marker = json!({
            "session_id": session_id,
            "take_id": take_id,
            "take_sequence": take_sequence,
            "continuation_of": continuation_of,
            "volume_id": admission.volume_id,
            "capture_mode": req.mode.as_str(),
            "display_name": display_name,
            "started_at": started_at,
            "segment_frames": self.config.segment_frames,
        });
        if let Ok(mut bytes) = serde_json::to_vec_pretty(&capture_marker) {
            bytes.push(b'\n');
            let _ = write_bytes_atomic(&staging_dir.join("capture.json"), &bytes, 0o640, true);
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
                self.fail_start_attempt(
                    &session_id,
                    &take_id,
                    &display_name,
                    &admission,
                    err.code(),
                    &err.to_string(),
                );
                return Err(err);
            }
        };

        let (timer_deadline_ns, timer_duration_seconds) = if timer_settings.enabled {
            let duration_ns = u64::from(timer_settings.duration_seconds) * 1_000_000_000;
            (
                Some(started_monotonic_ns.saturating_add(duration_ns)),
                Some(timer_settings.duration_seconds),
            )
        } else {
            (None, None)
        };

        let runtime_json = self.backend.runtime_observation().to_openapi_runtime_json();
        let (status_snapshot, rev) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            guard.transition(CapturePhase::Recording)?;
            guard.active = Some(ActiveSessionContext {
                session_id: session_id.clone(),
                take_id: take_id.clone(),
                take_sequence,
                continuation_of,
                mode: req.mode,
                display_name,
                started_at,
                started_monotonic_ns,
                staging_dir: staging_dir.clone(),
                admission: admission.clone(),
                recording_stage: "recording".to_owned(),
                verification_progress: None,
                diagnostics: Vec::new(),
                timer_deadline_ns,
                timer_duration_seconds,
                handle: Some(handle),
                _maintenance_lock: maintenance_lock,
            });
            let snap = self.build_status_snapshot_locked(&guard, runtime_json);
            if let Some(active_rec) = &snap.snapshot.active_recording {
                if let Ok(mut bytes) = serde_json::to_vec_pretty(&active_rec.recording_state) {
                    bytes.push(b'\n');
                    let _ = write_bytes_atomic(
                        &staging_dir.join("recording.json"),
                        &bytes,
                        0o640,
                        true,
                    );
                }
            }
            (snap, guard.source_revision)
        };

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
                    "start_capture",
                    key,
                    &req_hash,
                    json_val,
                    admission.writable,
                );
            }
        }

        Ok(IdempotentOutcome {
            value: status_snapshot,
            replayed: false,
        })
    }

    /// Idempotently stop, seal (zero-reread), and publish the active recording.
    ///
    /// Returns `Ok(IdempotentOutcome { value: None, .. })` when idle (OpenAPI 204 no-op).
    pub fn stop_capture(
        &self,
        req: CaptureStopRequest,
        idempotency_key: Option<&str>,
    ) -> StorageResult<IdempotentOutcome<Option<CaptureStatusSnapshot>>> {
        if req.schema != CAPTURE_STOP_SCHEMA {
            return Err(StorageError::invalid_input(
                "invalid_request",
                format!("stop request schema must be {CAPTURE_STOP_SCHEMA}"),
            ));
        }

        let req_hash = request_fingerprint(&req)?;
        if let Some(key) = idempotency_key {
            validate_idempotency_key(key)?;
            if let Some(cached) =
                self.receipts
                    .lookup_idempotency("stop_capture", key, &req_hash)?
            {
                let snap: Option<CaptureStatusSnapshot> =
                    serde_json::from_value(cached).map_err(|e| StorageError::Catalog {
                        message: format!("corrupt cached stop receipt: {e}"),
                    })?;
                return Ok(IdempotentOutcome {
                    value: snap,
                    replayed: true,
                });
            }
        }

        let (mut active_ctx, authority_epoch, stopping_rev) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase == CapturePhase::Idle {
                drop(guard);
                if let Some(key) = idempotency_key {
                    self.receipts.store_idempotency(
                        "stop_capture",
                        key,
                        &req_hash,
                        Value::Null,
                        self.volume.is_mounted_and_writable(),
                    );
                }
                return Ok(IdempotentOutcome {
                    value: None,
                    replayed: false,
                });
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
            guard.transition(CapturePhase::Stopping)?;
            let ctx = guard.active.take().ok_or_else(|| {
                StorageError::conflict("invalid_state", "missing active session context")
            })?;
            (ctx, guard.authority_epoch.clone(), guard.source_revision)
        };

        let session_id = active_ctx.session_id.clone();
        let volume_id = active_ctx.admission.volume_id.clone();
        let generation_id = active_ctx.admission.generation_id.clone();

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

        let handle = active_ctx.handle.take().ok_or_else(|| {
            StorageError::conflict("invalid_state", "missing active capture backend handle")
        })?;
        let final_progress = handle.progress();
        let captured = match handle.finish(req.reason) {
            Ok(artifacts) => artifacts,
            Err(err) => {
                self.record_unsuccessful_stop(
                    &active_ctx,
                    final_progress,
                    "failed",
                    err.code(),
                    &err.to_string(),
                    false,
                );
                return Err(err);
            }
        };

        // Emit verifying state transition before zero-reread seal & publish.
        let verify_rev = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
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

        let seal_result = self.seal_and_publish(&active_ctx, &captured);
        let sealed = match seal_result {
            Ok(s) => s,
            Err(err) => {
                self.record_unsuccessful_stop(
                    &active_ctx,
                    final_progress,
                    "failed",
                    err.code(),
                    &err.to_string(),
                    false,
                );
                return Err(err);
            }
        };

        // Drop active context (releasing maintenance lock and any staging handles) before emitting safe-swap receipt.
        let admission_writable = active_ctx.admission.writable;
        drop(active_ctx);

        let safe_swap_receipt = if req.reason == StopReason::SafeSwap {
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
                open_handle_count: 0,
            };
            self.receipts
                .set_safe_swap(Some(receipt.clone()), admission_writable);
            Some(receipt)
        } else {
            None
        };

        let runtime_json = self.backend.runtime_observation().to_openapi_runtime_json();
        let (final_snapshot, done_rev) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if req.reason == StopReason::SafeSwap {
                guard.volume_released = true;
            }
            guard.transition(CapturePhase::Idle)?;
            let snap = self.build_status_snapshot_locked(&guard, runtime_json);
            (snap, guard.source_revision)
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
        self.events.emit(
            &authority_epoch,
            done_rev,
            CaptureEventType::Snapshot,
            None,
            serde_json::to_value(&final_snapshot.snapshot).unwrap_or(Value::Null),
        );

        let wrapped = Some(final_snapshot);
        if let Some(key) = idempotency_key {
            if let Ok(json_val) = serde_json::to_value(&wrapped) {
                self.receipts.store_idempotency(
                    "stop_capture",
                    key,
                    &req_hash,
                    json_val,
                    admission_writable,
                );
            }
        }

        Ok(IdempotentOutcome {
            value: wrapped,
            replayed: false,
        })
    }

    /// Batch-delete explicitly selected sealed recordings with idempotency, digest preflight,
    /// continuation dependency enforcement, active-capture refusal, and empty date-dir cleanup.
    pub fn delete_sessions(
        &self,
        req: DeleteSessionsRequest,
        idempotency_key: &str,
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

        let req_hash = request_fingerprint(&req)?;
        if let Some(cached) =
            self.receipts
                .lookup_idempotency("delete_sessions", idempotency_key, &req_hash)?
        {
            let result: DeleteSessionsResult =
                serde_json::from_value(cached).map_err(|e| StorageError::Catalog {
                    message: format!("corrupt cached delete receipt: {e}"),
                })?;
            return Ok(IdempotentOutcome {
                value: result,
                replayed: true,
            });
        }

        // Refuse deletion during active capture/finalization or when volume is released.
        {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.volume_released {
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
        }

        // Always restore `Idle` phase when deletion finishes or preflight fails.
        let delete_outcome = self.execute_delete_preflight_and_remove(&req, &requested_ids);

        {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase == CapturePhase::Deleting {
                let _ = guard.transition(CapturePhase::Idle);
            }
        }

        let result = delete_outcome?;
        if let Ok(json_val) = serde_json::to_value(&result) {
            self.receipts.store_idempotency(
                "delete_sessions",
                idempotency_key,
                &req_hash,
                json_val,
                self.volume.is_mounted_and_writable(),
            );
        }
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

        for target in &req.sessions {
            let entry = self
                .catalog
                .get_indexed_session(&target.session_id)?
                .ok_or_else(|| StorageError::NotFound {
                    session_id: target.session_id.clone(),
                })?;

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
            let target_dir = recordings_root.join(&entry.relative_dir);
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

    /// Enforce storage reserve thresholds during active capture; stops & seals when space
    /// drops below `minimum_available_bytes` or marks media-lost if the volume disappears.
    pub fn enforce_storage_reserve(&self) -> StorageResult<ReserveEnforcementOutcome> {
        let active_session_id = {
            let guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase != CapturePhase::Recording {
                return Ok(ReserveEnforcementOutcome::Healthy);
            }
            guard.active.as_ref().map(|a| a.session_id.clone())
        };
        let Some(session_id) = active_session_id else {
            return Ok(ReserveEnforcementOutcome::Healthy);
        };

        let reserve = self.volume.check_reserve()?;
        if reserve.ok {
            return Ok(ReserveEnforcementOutcome::Healthy);
        }

        if !reserve.writable {
            // Unexpected media removal / remount-ro during capture: abort handle and record media_lost.
            let active_ctx = {
                let mut guard = self
                    .state
                    .lock()
                    .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
                guard.active.take()
            };
            if let Some(mut ctx) = active_ctx {
                let progress = ctx
                    .handle
                    .as_ref()
                    .map(|h| h.progress())
                    .unwrap_or_default();
                if let Some(handle) = ctx.handle.take() {
                    handle.abort();
                }
                self.record_unsuccessful_stop(
                    &ctx,
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
        let receipt = self.receipts.get_safe_swap()?;
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
        self.receipts.get_unsuccessful(session_id)
    }

    /// Gracefully close the coordinator (`Closing` terminal phase), aborting any active handle.
    pub fn close(&self) -> StorageResult<()> {
        let active_ctx = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| StorageError::Core(CoreError::poisoned("coordinator_state")))?;
            if guard.phase == CapturePhase::Closing {
                return Ok(());
            }
            guard.transition(CapturePhase::Closing)?;
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

    fn seal_and_publish(
        &self,
        ctx: &ActiveSessionContext,
        captured: &CapturedSessionArtifacts,
    ) -> StorageResult<SealedPublishOutput> {
        if captured.segments.is_empty() {
            return Err(StorageError::integrity(
                "empty_recording",
                "cannot seal recording with zero stereo segments",
            ));
        }

        let staging_root = &ctx.staging_dir;
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

        let audio_json = match &captured.audio {
            CapturedAudioOutcome::Recorded {
                requested_mode,
                sample_rate,
                channels,
                sample_count,
                start_time_seconds,
                end_time_seconds,
                segments,
            } => {
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
                json!({
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
                })
            }
            CapturedAudioOutcome::NotRecorded {
                requested_mode,
                reason,
            } => json!({
                "state": "not_recorded",
                "requested_mode": requested_mode,
                "resolved_mode": "disabled",
                "reason": reason,
            }),
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
            publish_staging_directory(self.volume.paths(), staging_root, &ctx.started_at)?;
        let final_root = self.volume.paths().recordings_dir().join(&relative_dir);
        remove_control_markers(&final_root)?;
        let _ = fsync_directory(&final_root);

        // Post-publish exFAT-safe artifact identity verification (zero disk reread).
        verify_device_session_artifacts(
            &final_root,
            &manifest_bytes,
            &ctx.session_id,
            &expected_identities,
        )
        .map_err(|e| StorageError::integrity("post_publish_verify_failed", e.message))?;

        let indexed =
            parse_indexed_session_from_manifest(&relative_dir, &manifest_value, &manifest_sha256)?;
        self.catalog.admit_indexed_session(indexed)?;

        Ok(SealedPublishOutput {
            manifest_id,
            manifest_sha256,
            sealed_at,
        })
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
            message: error_message.chars().take(1024).collect(),
            at: now_rfc3339_utc(),
            recoverable: false,
            details: None,
        };
        let (epoch, rev, outcome) = {
            let mut guard = match self.state.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            let _ = guard.transition(CapturePhase::Idle);
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
            (epoch, rev, rec)
        };

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
        ctx: &ActiveSessionContext,
        progress: CaptureProgressSnapshot,
        terminal_state: &str,
        error_code: &str,
        error_message: &str,
        media_lost: bool,
    ) {
        let diag = RecordingDiagnostic {
            code: if media_lost {
                "media_lost".to_owned()
            } else {
                sanitize_diag_code(error_code)
            },
            severity: "error".to_owned(),
            message: error_message.chars().take(1024).collect(),
            at: now_rfc3339_utc(),
            recoverable: terminal_state == "recoverable",
            details: None,
        };

        let (epoch, rev, outcome) = {
            let mut guard = match self.state.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            let target_phase = if media_lost {
                guard.last_failure_reason = Some(error_message.to_owned());
                CapturePhase::Failed
            } else {
                CapturePhase::Idle
            };
            let _ = guard.transition(target_phase);
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
            (epoch, rev, rec)
        };

        let volume_writable = !media_lost && self.volume.is_mounted_and_writable();
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
                let active_rec = guard.active.as_ref().map(|ctx| CaptureSnapshotRecording {
                    generation_id: ctx.admission.generation_id.clone(),
                    recording_state: RecordingState {
                        schema: RECORDING_STATE_SCHEMA.to_owned(),
                        state: "finalizing".to_owned(),
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
                            elapsed_seconds: 0.0,
                            captured_frames: 0,
                            bytes_written: 0,
                            verification: ctx.verification_progress,
                        },
                        diagnostics: ctx.diagnostics.clone(),
                    },
                });
                if active_rec.is_some() {
                    ("finalizing".to_owned(), active_rec, None)
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
        if name.is_empty() || name.len() > 160 {
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

fn sanitize_diag_code(raw: &str) -> String {
    let mut out = String::new();
    for ch in raw.chars().take(96) {
        if ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-') {
            out.push(ch);
        } else if ch.is_ascii_uppercase() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    if out.is_empty() || !out.as_bytes()[0].is_ascii_lowercase() {
        "capture_error".to_owned()
    } else {
        out
    }
}
