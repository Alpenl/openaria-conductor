//! Explicit-FSM `CaptureCoordinator` decomposed into cohesive submodules:
//! - [`types`] — schemas, DTOs, `CapturePhase`, `CaptureBusyState`, `ClockSyncProbe`, `CaptureBackend`
//! - [`events`] — bounded 128-slot `CaptureEventBroadcaster`
//! - [`receipts`] — `ReceiptStore` (`coordinator-receipts.json`) + `InFlightTracker`
//! - [`fsm`] — core state transitions, `startCapture`, focus, timer, readiness, and `close`
//! - [`manifest_builder`] — zero-reread sealing, provenance logs, collision-safe publish, catalog admit
//! - [`stop_worker`] — synchronous `stopCapture`, `Idle` `SafeSwap`, and async background `begin_stop_capture_for_principal`
//! - [`deletion`] — trash-staged crash-atomic `deleteSessions` with batch WAL forget
//! - [`recovery_loop`] — startup crash recovery, `PendingDeleteIntent` replay, 1 Hz reserve & backoff

mod deletion;
mod events;
mod fsm;
mod manifest_builder;
mod receipts;
mod recovery_loop;
mod stop_worker;
mod types;

pub use events::{CaptureEventBroadcaster, CaptureEventSubscription};
pub use fsm::CaptureCoordinator;
pub use types::{
    ActiveCaptureHandle, ActiveCaptureTimer, BackendStartRequest, CAMERA_CONNECTION_SCHEMA,
    CAMERA_FOCUS_SCHEMA, CAMERA_FOCUS_SET_SCHEMA, CAPTURE_DIAGNOSTIC_EVENT_SCHEMA,
    CAPTURE_EVENT_SCHEMA, CAPTURE_PROGRESS_EVENT_SCHEMA, CAPTURE_READINESS_SCHEMA,
    CAPTURE_SNAPSHOT_EVENT_SCHEMA, CAPTURE_START_SCHEMA, CAPTURE_STATE_EVENT_SCHEMA,
    CAPTURE_STATUS_SCHEMA, CAPTURE_STOP_SCHEMA, CAPTURE_TIMER_SCHEMA, CAPTURE_TIMER_SET_SCHEMA,
    CLOCK_CALIBRATION_SCHEMA, CameraFocusSetRequest, CameraFocusStatus, CaptureBackend,
    CaptureBusyState, CaptureEvent, CaptureEventType, CaptureMode, CapturePhase,
    CaptureProgressSnapshot, CaptureReadiness, CaptureReadinessStorage, CaptureSnapshotEventData,
    CaptureSnapshotRecording, CaptureStartRequest, CaptureStatusSnapshot, CaptureStopRequest,
    CaptureTimerSetRequest, CaptureTimerSettings, CaptureTimerStatus, CapturedArtifact,
    CapturedAudioOutcome, CapturedAudioSegment, CapturedSessionArtifacts, CapturedVideoSegment,
    ClockSyncProbe, ClockSyncSnapshot, ClockSyncSource, CoordinatorConfig,
    DEFAULT_TIMER_DURATION_SECONDS, DeleteSessionTarget, DeleteSessionsRequest,
    DeleteSessionsResult, DropEventRecord, EVENT_REPLAY_CAPACITY, FailedDeleteSession,
    IdempotentOutcome, MAX_DELETE_BATCH_SIZE, MAX_TIMER_DURATION_SECONDS, RECORDING_STATE_SCHEMA,
    RETAINED_UNSUCCESSFUL_SCHEMA, RecordingDiagnostic, RecordingState, RecordingStateProgress,
    RecordingStateStorage, ReserveEnforcementOutcome, RetainedUnsuccessfulSessionResource,
    RuntimeObservation, SAFE_SWAP_RECEIPT_RESOURCE_SCHEMA, SAFE_SWAP_RECEIPT_SCHEMA,
    SESSION_DELETE_REQUEST_SCHEMA, SESSION_DELETE_RESULT_SCHEMA, SafeSwapReceipt,
    SafeSwapReceiptResource, StopReason, SyncedFmp4Artifacts, TakeRequest, VerificationCounter,
    sanitize_diag_code,
};
