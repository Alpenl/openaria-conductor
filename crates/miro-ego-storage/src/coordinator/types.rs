//! Schema constants, FSM phase & busy enums, clock/calibration provenance traits,
//! request/response DTOs, and `CaptureBackend` traits for [`super::CaptureCoordinator`].

use crate::catalog::SessionDeviceIdentity;
use crate::recovery::RecoveryDefaults;
use crate::util::{now_rfc3339_utc, sha256_hex};
use crate::volume::CaptureStorageBudget;
use crate::{StorageError, StorageResult};
use miro_ego_core::config::RecordingEncoding;
use miro_ego_core::ids::{is_valid_uuid_v4, is_valid_uuid_v7, new_uuid_v4};
use miro_ego_core::media_types::{FragmentEvent, SealedMediaEvent, SegmentEvent};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fmt;
use std::path::PathBuf;

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
pub const CLOCK_CALIBRATION_SCHEMA: &str = "miro-ego.clock-calibration.v1";

pub const DEFAULT_TIMER_DURATION_SECONDS: u32 = 600;
pub const MAX_TIMER_DURATION_SECONDS: u32 = 86_400;
pub const MAX_DELETE_BATCH_SIZE: usize = 200;
pub const EVENT_REPLAY_CAPACITY: usize = 128;
pub(super) const MAX_CALIBRATION_FILE_BYTES: usize = 4 * 1024 * 1024;

/// Typed active capture state carried by [`StorageError::CaptureBusy`], matching
/// OpenAPI `CaptureBusyError.error.details.current_state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureBusyState {
    Recording,
    Finalizing,
    Encoding,
    Verifying,
}

impl CaptureBusyState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Recording => "recording",
            Self::Finalizing => "finalizing",
            Self::Encoding => "encoding",
            Self::Verifying => "verifying",
        }
    }

    pub fn from_recording_stage(stage: &str) -> Self {
        match stage {
            "encoding" => Self::Encoding,
            "verifying" => Self::Verifying,
            "finalizing" => Self::Finalizing,
            _ => Self::Recording,
        }
    }
}

impl fmt::Display for CaptureBusyState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Authoritative clock synchronization source for boot-scoped capture guard and provenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClockSyncSource {
    Ntp,
    Client,
    Unsynchronized,
}

impl ClockSyncSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ntp => "ntp",
            Self::Client => "client",
            Self::Unsynchronized => "unsynchronized",
        }
    }

    pub const fn is_synchronized(self) -> bool {
        matches!(self, Self::Ntp | Self::Client)
    }
}

/// Boot-scoped clock snapshot recorded into `<session>/logs/clock-calibration.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClockSyncSnapshot {
    pub boot_id: String,
    pub source: ClockSyncSource,
    pub unix_time_ms: u64,
    pub monotonic_ns: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synchronized_at_ms: Option<u64>,
}

/// Read-only probe allowing [`super::CaptureCoordinator`] to enforce boot-scoped clock
/// synchronization and record `logs/clock-calibration.json` provenance on `startCapture`.
pub trait ClockSyncProbe: Send + Sync {
    fn snapshot(&self) -> ClockSyncSnapshot;
}

/// Explicit finite state machine phase for [`super::CaptureCoordinator`].
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

    pub(super) const fn to_u8(self) -> u8 {
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

    pub(super) const fn from_u8(raw: u8) -> Self {
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

/// Outcome returned by [`super::CaptureCoordinator::enforce_storage_reserve`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ReserveEnforcementOutcome {
    Healthy,
    StoppedForReserve { session_id: String, reason: String },
    FailedMediaLost { session_id: String, reason: String },
}

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

/// Video segment output from backend finish (compatible with `SegmentEvent`).
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
    /// Convert an encoder [`SegmentEvent`] with streaming SHA-256 digests.
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

/// Metadata describing a sealed single-fMP4 (`session.mp4`, `v2`) capture outcome.
#[derive(Debug, Clone)]
pub struct SyncedFmp4Artifacts {
    pub sealed: SealedMediaEvent,
    pub fragments: Vec<FragmentEvent>,
    pub audio_sample_rate: Option<u32>,
    pub audio_channels: Option<u16>,
    pub audio_capture_clock: Option<Value>,
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
    pub synced_fmp4: Option<SyncedFmp4Artifacts>,
    pub logs: Vec<CapturedArtifact>,
    pub dropped_frames: u64,
    pub drop_events: Vec<DropEventRecord>,
    pub duration_seconds: f64,
    pub effective_fps: f64,
}

/// Active recording session owned by [`super::CaptureCoordinator`] between start and stop.
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

/// Configuration for [`super::CaptureCoordinator`].
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
    pub require_clock_sync: bool,
    pub calibration_file: Option<PathBuf>,
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
            require_clock_sync: false,
            calibration_file: None,
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

    pub(super) fn device_identity(&self) -> SessionDeviceIdentity {
        SessionDeviceIdentity {
            device_id: self.device_id.clone(),
            device_label: self.device_label.clone(),
        }
    }
}

pub(super) fn validate_start_request(req: &CaptureStartRequest) -> StorageResult<()> {
    if req.schema != CAPTURE_START_SCHEMA {
        return Err(StorageError::invalid_input(
            "invalid_request",
            format!("capture start schema must be {CAPTURE_START_SCHEMA}"),
        ));
    }
    if let Some(vid) = &req.volume_id
        && !is_valid_uuid_v4(vid)
    {
        return Err(StorageError::invalid_input(
            "invalid_request",
            "volume_id must be a canonical lowercase UUIDv4",
        ));
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
    if let TakeRequest::Continue { continuation_of } = &req.take
        && !is_valid_uuid_v7(continuation_of)
    {
        return Err(StorageError::invalid_input(
            "invalid_request",
            "take.continuation_of must be a canonical lowercase UUIDv7",
        ));
    }
    Ok(())
}

pub(super) fn map_start_volume_error(err: StorageError) -> StorageError {
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

pub(super) fn validate_idempotency_key(key: &str) -> StorageResult<()> {
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

pub(super) fn request_fingerprint<T: Serialize>(value: &T) -> StorageResult<String> {
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

pub(super) fn sanitize_diag_message(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        "capture failed".to_owned()
    } else {
        raw.chars().take(1024).collect()
    }
}
