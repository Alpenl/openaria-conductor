//! Decoupled `GatewayBackend` trait covering all 37 Miro-Ego v1 OpenAPI operations,
//! plus a complete stateful in-memory `MockGatewayBackend` for integration testing and dev mode.

use crate::downloads::{OpenRepresentation, open_safe_log_file};
use crate::error::{ApiError, ApiResult};
use crate::preview::{H264PreviewHub, MjpegPreviewHub};
use crate::range::{infer_log_media_type, is_valid_log_name};
use crate::security::{AuditEvent, SecurityProfile, format_unix_secs_rfc3339, rfc3339_now_utc};
use crate::sse::{EventReplayBuffer, NetworkEventReplayBuffer};
use async_trait::async_trait;
use bytes::Bytes;
use miro_ego_core::ids::{hex_encode, new_uuid_v4, new_uuid_v7};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Idempotent command envelope passed from HTTP handlers to [`GatewayBackend`].
#[derive(Debug, Clone)]
pub struct IdempotentCommand {
    pub principal_id: String,
    pub idempotency_key: String,
    pub body: Value,
    pub canonical_body: Vec<u8>,
}

/// Outcome returned by an idempotent mutation operation (`200`, `202`, or `204`).
#[derive(Debug, Clone)]
pub struct CommandOutcome {
    pub status_code: u16,
    pub body: Option<Value>,
    pub replayed: bool,
}

impl CommandOutcome {
    pub fn ok(body: Value, replayed: bool) -> Self {
        Self {
            status_code: 200,
            body: Some(body),
            replayed,
        }
    }

    pub fn accepted(body: Value, replayed: bool) -> Self {
        Self {
            status_code: 202,
            body: Some(body),
            replayed,
        }
    }

    pub fn no_content(replayed: bool) -> Self {
        Self {
            status_code: 204,
            body: None,
            replayed,
        }
    }
}

/// Query parameters for `GET /api/v1/sessions`.
#[derive(Debug, Clone, Default)]
pub struct ListSessionsQuery {
    pub cursor: Option<String>,
    pub limit: usize,
    pub take_id: Option<String>,
    pub include_recovery: bool,
}

/// Query parameters for `GET /api/v1/logs`.
#[derive(Debug, Clone, Default)]
pub struct ListLogsQuery {
    pub prefix: Option<String>,
    pub limit: usize,
}

/// Exact stored `manifest.json` bytes paired with their verified SHA-256 hex digest.
#[derive(Debug, Clone)]
pub struct RawManifest {
    pub bytes: Bytes,
    pub sha256: String,
}

impl RawManifest {
    pub fn from_bytes(bytes: Bytes) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let sha256 = hex_encode(&hasher.finalize());
        Self { bytes, sha256 }
    }

    pub fn etag(&self) -> String {
        format!("\"{}\"", self.sha256)
    }
}

/// Immutable in-memory static web console asset served at `/` and `/<asset>`.
#[derive(Debug, Clone)]
pub struct StaticAsset {
    pub name: String,
    pub content_type: String,
    pub bytes: Bytes,
    pub etag: String,
}

impl StaticAsset {
    pub fn new(name: impl Into<String>, content_type: impl Into<String>, bytes: Bytes) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let digest = hex_encode(&hasher.finalize());
        Self {
            name: name.into(),
            content_type: content_type.into(),
            bytes,
            etag: format!("\"{digest}\""),
        }
    }
}

/// Complete async backend trait decoupling the Miro-Ego v1 HTTP gateway from concrete
/// storage, capture, hardware, network, and firmware implementations.
#[async_trait]
pub trait GatewayBackend: Send + Sync + 'static {
    // --- Device ---
    async fn device_info(&self, security_profile: SecurityProfile) -> ApiResult<Value>;

    // --- Capture ---
    async fn capture_status(&self) -> ApiResult<Value>;
    async fn capture_snapshot_event(&self) -> ApiResult<Value>;
    async fn capture_readiness(&self) -> ApiResult<Value>;
    async fn capture_safe_swap(&self) -> ApiResult<Option<Value>>;
    async fn start_capture(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome>;
    async fn stop_capture(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome>;
    async fn capture_timer(&self) -> ApiResult<Value>;
    async fn set_capture_timer(&self, enabled: bool, duration_seconds: u64) -> ApiResult<Value>;
    async fn camera_focus(&self) -> ApiResult<Option<Value>>;
    async fn set_camera_focus(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome>;

    // --- Sessions & Artifacts ---
    async fn list_sessions(&self, query: ListSessionsQuery) -> ApiResult<Value>;
    async fn open_session_manifest(&self, session_id: &str) -> ApiResult<RawManifest>;
    async fn retained_unsuccessful_outcome(&self, session_id: &str) -> ApiResult<Option<Value>>;
    /// Return `Some("recording" | "finalizing" | "encoding" | "verifying")` when capture
    /// currently owns device I/O resources and artifact downloads must return `423 capture_busy`.
    async fn artifact_io_busy_state(&self) -> ApiResult<Option<String>>;
    async fn open_session_artifact(
        &self,
        session_id: &str,
        artifact_id: &str,
    ) -> ApiResult<OpenRepresentation>;
    async fn delete_sessions(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome>;

    // --- Diagnostic Logs ---
    async fn list_logs(&self, query: ListLogsQuery) -> ApiResult<Value>;
    async fn open_log_file(&self, name: &str) -> ApiResult<OpenRepresentation>;

    // --- Preview & SSE Hubs ---
    fn preview_mjpeg_hub(&self) -> Option<Arc<MjpegPreviewHub>>;
    fn preview_h264_hub(&self) -> Option<Arc<H264PreviewHub>>;
    async fn open_preview_mjpeg(&self, _fps: Option<u32>) -> ApiResult<Arc<MjpegPreviewHub>> {
        self.preview_mjpeg_hub()
            .ok_or_else(|| ApiError::preview_unavailable("preview stream is unavailable"))
    }
    async fn open_preview_h264(
        &self,
        _fps: Option<u32>,
        _bitrate_kbps: Option<u32>,
    ) -> ApiResult<Arc<H264PreviewHub>> {
        self.preview_h264_hub()
            .ok_or_else(ApiError::preview_h264_unsupported)
    }
    fn capture_event_buffer(&self) -> Arc<EventReplayBuffer>;
    fn network_event_buffer(&self) -> Arc<NetworkEventReplayBuffer>;

    // --- Clock ---
    async fn clock_status(&self, principal_id: &str) -> ApiResult<Value>;
    async fn clock_inspect(&self, principal_id: &str) -> ApiResult<Value>;
    async fn clock_sync(&self, principal_id: &str, body: Value) -> ApiResult<Value>;

    // --- Voice ---
    async fn voice_status(&self) -> ApiResult<Value>;
    async fn set_voice_volume(&self, volume: u8) -> ApiResult<Value>;

    // --- Network ---
    async fn network_status(&self) -> ApiResult<Value>;
    async fn network_scan(&self) -> ApiResult<Value>;
    async fn create_network_credential(
        &self,
        principal_id: &str,
        passphrase: &str,
    ) -> ApiResult<Value>;
    async fn apply_network_desired_state(
        &self,
        cmd: IdempotentCommand,
    ) -> ApiResult<CommandOutcome>;
    async fn retry_network_transaction(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome>;
    async fn forget_network_client_profile(
        &self,
        cmd: IdempotentCommand,
    ) -> ApiResult<CommandOutcome>;

    // --- Firmware ---
    async fn firmware_status(&self) -> ApiResult<Value>;
    async fn firmware_check(&self, force: bool) -> ApiResult<Value>;
    async fn firmware_update(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome>;
    async fn firmware_rollback(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome>;

    // --- Audit & Static Web Assets ---
    fn record_audit(&self, event: AuditEvent) -> ApiResult<()>;
    fn get_web_asset(&self, name: &str) -> Option<StaticAsset>;
}

// ---------------------------------------------------------------------------
// Full In-Memory MockGatewayBackend (Tests & Dev `serve-mock` Mode)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MockSession {
    pub session_id: String,
    pub take_id: String,
    pub primary_artifact_id: String,
    pub manifest: RawManifest,
    pub summary: Value,
    pub verified: bool,
    pub verification_reason: String,
    pub artifacts: HashMap<String, MockArtifact>,
}

#[derive(Debug, Clone)]
pub enum MockArtifactBacking {
    Memory(Bytes),
    FilePath(PathBuf),
}

#[derive(Debug, Clone)]
pub struct MockArtifact {
    pub artifact_id: String,
    pub media_type: String,
    pub backing: MockArtifactBacking,
}

#[derive(Debug, Clone)]
pub struct MockLogEntry {
    pub name: String,
    pub bytes: Bytes,
    pub modified_at: String,
    pub sha256: String,
}

#[derive(Debug, Clone)]
struct IdempotencyRecord {
    canonical_body: Vec<u8>,
    outcome: CommandOutcome,
}

struct MockState {
    device_id: String,
    device_label: String,
    hardware_fingerprint: String,
    volume_id: String,
    authority_epoch: String,
    boot_id: String,
    source_revision: u64,
    network_revision: u64,
    device_state: String,
    camera_connected: bool,
    active_session_id: Option<String>,
    active_take_id: Option<String>,
    active_display_name: Option<String>,
    active_generation_id: Option<String>,
    safe_swap_receipt: Option<Value>,
    timer_enabled: bool,
    timer_duration_seconds: u64,
    voice_volume: u8,
    focus_supported: bool,
    focus_value: u64,
    focus_auto: bool,
    clock_synced: bool,
    clock_unix_ms: u64,
    clock_synced_at_ms: Option<u64>,
    clock_challenges: HashMap<String, (String, bool)>, // challenge -> (principal_id, is_check)
    network_mode: String,
    network_saved: bool,
    network_verified: bool,
    network_ssid: Option<String>,
    network_security: Option<String>,
    network_credentials: HashMap<String, String>, // cred_ref -> principal_id
    network_transactions: HashMap<String, Value>,
    latest_network_tx: Option<Value>,
    firmware_current: (String, String),
    firmware_previous: Option<(String, String)>,
    firmware_available: Option<(String, String)>,
    firmware_task: Option<Value>,
    sessions: HashMap<String, MockSession>,
    session_order: Vec<String>,
    unsuccessful_outcomes: HashMap<String, Value>,
    logs_dir: Option<PathBuf>,
    in_memory_logs: HashMap<String, MockLogEntry>,
    web_assets: HashMap<String, StaticAsset>,
    idempotency: HashMap<(String, String, String), IdempotencyRecord>,
    audit_events: Vec<AuditEvent>,
}

/// Full in-memory implementation of [`GatewayBackend`] conforming to all 37 OpenAPI v1 routes.
pub struct MockGatewayBackend {
    state: Mutex<MockState>,
    mjpeg_hub: Arc<MjpegPreviewHub>,
    h264_hub: Option<Arc<H264PreviewHub>>,
    capture_events: Arc<EventReplayBuffer>,
    network_events: Arc<NetworkEventReplayBuffer>,
    fail_audit: AtomicBool,
}

impl Default for MockGatewayBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MockGatewayBackend {
    pub fn new() -> Self {
        Self::with_options(true)
    }

    pub fn with_options(enable_h264_preview: bool) -> Self {
        let device_id = new_uuid_v4();
        let device_label = "Miro-Ego-1234ABCD".to_owned();
        let hardware_fingerprint = format!("sha256:{}", "a".repeat(64));
        let volume_id = new_uuid_v4();
        let authority_epoch = new_uuid_v4();
        let boot_id = new_uuid_v4();
        let session_id = new_uuid_v7();
        let take_id = new_uuid_v7();
        let failed_session_id = new_uuid_v7();
        let failed_take_id = new_uuid_v7();
        let fw_commit = "1".repeat(40);

        let sample_session = build_mock_session(
            &session_id,
            &take_id,
            "2026-11-26 Sample Session",
            &device_id,
            &device_label,
            &hardware_fingerprint,
            &volume_id,
            &fw_commit,
        );
        let manifest_sha = sample_session.manifest.sha256.clone();

        let mut sessions = HashMap::new();
        sessions.insert(session_id.clone(), sample_session);

        let mut unsuccessful_outcomes = HashMap::new();
        let failed_gen_id = new_uuid_v4();
        unsuccessful_outcomes.insert(
            failed_session_id.clone(),
            json!({
                "schema": "miro-ego.retained-unsuccessful-session-resource.v1",
                "authority_epoch": authority_epoch,
                "source_revision": 1,
                "outcome": {
                    "generation_id": failed_gen_id,
                    "recording_state": build_mock_recording_state(
                        "failed",
                        &authority_epoch,
                        1,
                        &failed_session_id,
                        &failed_take_id,
                        "2026-11-26 Failed Session",
                        &device_id,
                        &device_label,
                        &volume_id,
                        "2026-11-26T10:01:00Z",
                    )
                }
            }),
        );

        let safe_swap_receipt = json!({
            "schema": "miro-ego.safe-swap-receipt-resource.v1",
            "receipt": {
                "schema": "miro-ego.safe-swap-receipt.v1",
                "session_id": session_id,
                "volume_id": volume_id,
                "generation_id": new_uuid_v4(),
                "manifest_id": new_uuid_v7(),
                "manifest_sha256": manifest_sha,
                "sealed_at": "2026-11-26T10:00:10Z",
                "released_at": "2026-11-26T10:00:12Z",
                "release_state": "unmounted",
                "open_handle_count": 0
            }
        });

        let mut in_memory_logs = HashMap::new();
        let sample_log_bytes = Bytes::from_static(b"2026-11-26T10:00:00Z INFO miro-ego boot ok\n");
        in_memory_logs.insert(
            "device.log".to_owned(),
            MockLogEntry {
                name: "device.log".to_owned(),
                sha256: sha256_hex(&sample_log_bytes),
                bytes: sample_log_bytes,
                modified_at: "2026-11-26T10:00:00Z".to_owned(),
            },
        );
        let wd_log_bytes = Bytes::from_static(b"{\"event\":\"wifi_ok\"}\n");
        in_memory_logs.insert(
            "wifi-watchdog/events.ndjson".to_owned(),
            MockLogEntry {
                name: "wifi-watchdog/events.ndjson".to_owned(),
                sha256: sha256_hex(&wd_log_bytes),
                bytes: wd_log_bytes,
                modified_at: "2026-11-26T10:05:00Z".to_owned(),
            },
        );

        let mut web_assets = HashMap::new();
        web_assets.insert(
            "index.html".to_owned(),
            StaticAsset::new(
                "index.html",
                "text/html; charset=utf-8",
                Bytes::from_static(b"<!doctype html><title>Miro-Ego</title><h1>Miro-Ego</h1>"),
            ),
        );
        web_assets.insert(
            "app.js".to_owned(),
            StaticAsset::new(
                "app.js",
                "application/javascript; charset=utf-8",
                Bytes::from_static(b"export const API_PREFIX = '/api/v1';\n"),
            ),
        );

        let mjpeg_hub = Arc::new(MjpegPreviewHub::new(10));
        let _ = mjpeg_hub.publish(Bytes::from_static(b"\xff\xd8\xff\xe0MOCK-JPEG\xff\xd9"));

        let h264_hub = if enable_h264_preview {
            let hub = Arc::new(H264PreviewHub::new_fmp4(64));
            hub.set_init_segment_blocking(Bytes::from_static(b"\x00\x00\x00\x18ftypisommoov"));
            Some(hub)
        } else {
            None
        };

        let tx_id = new_uuid_v7();
        let initial_tx = json!({
            "schema": "miro-ego.network-transaction.v1",
            "authority_epoch": authority_epoch,
            "source_revision": 1,
            "transaction_id": tx_id,
            "operation": "apply",
            "status": "rescued",
            "stage": "rescued",
            "desired": { "mode": "hotspot", "wifi_client": null, "ethernet": null },
            "accepted_at": "2026-11-26T10:00:00Z",
            "updated_at": "2026-11-26T10:00:10Z",
            "deadline": null,
            "recovery_action": "reconnect_rescue_ap",
            "rescue": {
                "ap_validated": true,
                "fallback_mode": "hotspot",
                "failure_trigger_seconds": 10
            },
            "error": {
                "code": "dhcp_timeout",
                "message": "DHCP lease timed out",
                "retryable": true
            }
        });
        let mut network_transactions = HashMap::new();
        network_transactions.insert(tx_id, initial_tx.clone());

        Self {
            state: Mutex::new(MockState {
                device_id,
                device_label,
                hardware_fingerprint,
                volume_id,
                authority_epoch,
                boot_id,
                source_revision: 1,
                network_revision: 1,
                device_state: "idle".to_owned(),
                camera_connected: true,
                active_session_id: None,
                active_take_id: None,
                active_display_name: None,
                active_generation_id: None,
                safe_swap_receipt: Some(safe_swap_receipt),
                timer_enabled: false,
                timer_duration_seconds: 600,
                voice_volume: 80,
                focus_supported: true,
                focus_value: 250,
                focus_auto: false,
                clock_synced: false,
                clock_unix_ms: 1_764_152_400_000,
                clock_synced_at_ms: None,
                clock_challenges: HashMap::new(),
                network_mode: "hotspot".to_owned(),
                network_saved: false,
                network_verified: false,
                network_ssid: None,
                network_security: None,
                network_credentials: HashMap::new(),
                network_transactions,
                latest_network_tx: Some(initial_tx),
                firmware_current: ("0.3.0".to_owned(), fw_commit),
                firmware_previous: Some(("0.2.9".to_owned(), "2".repeat(40))),
                firmware_available: Some(("0.3.1".to_owned(), "3".repeat(40))),
                firmware_task: None,
                sessions,
                session_order: vec![session_id],
                unsuccessful_outcomes,
                logs_dir: None,
                in_memory_logs,
                web_assets,
                idempotency: HashMap::new(),
                audit_events: Vec::new(),
            }),
            mjpeg_hub,
            h264_hub,
            capture_events: Arc::new(EventReplayBuffer::default()),
            network_events: Arc::new(NetworkEventReplayBuffer::default()),
            fail_audit: AtomicBool::new(false),
        }
    }

    pub fn set_fail_audit(&self, fail: bool) {
        self.fail_audit.store(fail, Ordering::SeqCst);
    }

    pub fn audit_events(&self) -> Vec<AuditEvent> {
        self.state
            .lock()
            .map(|s| s.audit_events.clone())
            .unwrap_or_default()
    }

    pub fn set_device_state(&self, state: impl Into<String>) {
        if let Ok(mut guard) = self.state.lock() {
            guard.device_state = state.into();
        }
    }

    pub fn set_camera_connected(&self, connected: bool) {
        if let Ok(mut guard) = self.state.lock() {
            guard.camera_connected = connected;
        }
    }

    pub fn set_logs_dir(&self, path: PathBuf) {
        if let Ok(mut guard) = self.state.lock() {
            guard.logs_dir = Some(path);
        }
    }

    pub fn insert_sample_session(
        &self,
        take_id: Option<String>,
    ) -> ApiResult<(String, String, String)> {
        let mut guard = self.lock_state()?;
        let sid = new_uuid_v7();
        let tid = take_id.unwrap_or_else(new_uuid_v7);
        let sess = build_mock_session(
            &sid,
            &tid,
            "2026-11-26 Additional Session",
            &guard.device_id,
            &guard.device_label,
            &guard.hardware_fingerprint,
            &guard.volume_id,
            &guard.firmware_current.1,
        );
        let aid = sess.primary_artifact_id.clone();
        let manifest_sha = sess.manifest.sha256.clone();
        guard.sessions.insert(sid.clone(), sess);
        guard.session_order.push(sid.clone());
        Ok((sid, aid, manifest_sha))
    }

    pub fn sample_session_and_artifact(&self) -> Option<(String, String, String)> {
        let guard = self.state.lock().ok()?;
        let sid = guard.session_order.first()?.clone();
        let sess = guard.sessions.get(&sid)?;
        Some((
            sid,
            sess.primary_artifact_id.clone(),
            sess.manifest.sha256.clone(),
        ))
    }

    pub fn sample_unsuccessful_session_id(&self) -> Option<String> {
        let guard = self.state.lock().ok()?;
        guard.unsuccessful_outcomes.keys().next().cloned()
    }

    pub fn sample_network_transaction_id(&self) -> Option<String> {
        let guard = self.state.lock().ok()?;
        guard.network_transactions.keys().next().cloned()
    }

    pub fn add_file_artifact(
        &self,
        session_id: &str,
        artifact_id: String,
        media_type: String,
        path: PathBuf,
    ) -> ApiResult<()> {
        let mut guard = self.lock_state()?;
        let session = guard
            .sessions
            .get_mut(session_id)
            .ok_or_else(|| ApiError::not_found("session not found"))?;
        session.artifacts.insert(
            artifact_id.clone(),
            MockArtifact {
                artifact_id,
                media_type,
                backing: MockArtifactBacking::FilePath(path),
            },
        );
        Ok(())
    }

    pub fn set_session_verified(&self, session_id: &str, verified: bool, reason: &str) {
        if let Ok(mut guard) = self.state.lock()
            && let Some(sess) = guard.sessions.get_mut(session_id)
        {
            sess.verified = verified;
            sess.verification_reason = reason.to_owned();
        }
    }

    fn lock_state(&self) -> ApiResult<std::sync::MutexGuard<'_, MockState>> {
        self.state
            .lock()
            .map_err(|_| ApiError::internal("state_poisoned", "mock backend mutex poisoned"))
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_encode(&hasher.finalize())
}

#[allow(clippy::too_many_arguments)]
fn build_mock_session(
    session_id: &str,
    take_id: &str,
    display_name: &str,
    device_id: &str,
    device_label: &str,
    hardware_fingerprint: &str,
    volume_id: &str,
    commit: &str,
) -> MockSession {
    let left_payload = Bytes::from(format!("MIRO-EGO-LEFT-VIDEO-MP4-{session_id}"));
    let right_payload = Bytes::from(format!("MIRO-EGO-RIGHT-VIDEO-MP4-{session_id}"));
    let imu_payload = Bytes::from_static(
        b"{\"t_ns\":0,\"ax\":0,\"ay\":0,\"az\":16384,\"gx\":0,\"gy\":0,\"gz\":0}\n",
    );
    let frames_payload = Bytes::from_static(b"{\"frame\":0,\"t_ns\":0}\n");

    let left_id = sha256_hex(&left_payload);
    let right_id = sha256_hex(&right_payload);
    let imu_id = sha256_hex(&imu_payload);
    let frames_id = sha256_hex(&frames_payload);
    let total_bytes =
        left_payload.len() + right_payload.len() + imu_payload.len() + frames_payload.len();

    let manifest_json = json!({
        "schema": "miro-ego.device-session.v1",
        "manifest_id": new_uuid_v7(),
        "sealed": true,
        "sealed_at": "2026-11-26T10:00:11Z",
        "session_id": session_id,
        "volume_id": volume_id,
        "capture_mode": "production",
        "display_name": display_name,
        "device": {
            "device_id": device_id,
            "device_label": device_label,
            "hardware_fingerprint": hardware_fingerprint,
            "platform": "rdk-x5-ubuntu22.04-aarch64",
            "software_version": "0.3.0",
            "commit": commit
        },
        "time": {
            "started_at": "2026-11-26T10:00:00Z",
            "ended_at": "2026-11-26T10:00:10Z",
            "timezone": "Etc/UTC",
            "duration_seconds": 10.0,
            "duration_clock": "host_monotonic"
        },
        "take": {
            "take_id": take_id,
            "sequence": 1,
            "continuation_of": null
        },
        "camera": {
            "width": 2560,
            "height": 800,
            "eye_width": 1280,
            "sensor_fps": 30.0,
            "frame_decimation": 1,
            "nominal_fps": 30.0,
            "effective_fps": 30.0,
            "coordinate_frame": "opencv_optical"
        },
        "video": {
            "layout": "split-eyes",
            "codec": "h264",
            "container": "mp4",
            "segments": [
                {
                    "index": 0,
                    "start_frame": 0,
                    "end_frame": 300,
                    "start_time_seconds": 0.0,
                    "end_time_seconds": 10.0,
                    "artifacts": {
                        "left": {
                            "artifact_id": left_id,
                            "role": "video.left",
                            "path": "video/seg_0000_left.mp4",
                            "media_type": "video/mp4",
                            "bytes": left_payload.len(),
                            "sha256": left_id
                        },
                        "right": {
                            "artifact_id": right_id,
                            "role": "video.right",
                            "path": "video/seg_0000_right.mp4",
                            "media_type": "video/mp4",
                            "bytes": right_payload.len(),
                            "sha256": right_id
                        }
                    }
                }
            ],
            "encoding": {
                "schema": "miro-ego.recording-encoding.v1",
                "codec": "h264",
                "rate_control": "cbr",
                "profile": "high",
                "pixel_format": "yuv420p",
                "bit_depth": 8,
                "b_frames": 0,
                "bitrate_kbps": 12000,
                "min_qp": 18,
                "max_qp": 38,
                "intra_qp": 24,
                "initial_qp": 26,
                "gop_frames": 30,
                "vbv_ms": 1000
            }
        },
        "imu": {
            "artifact": {
                "artifact_id": imu_id,
                "role": "imu.samples",
                "path": "imu/samples.ndjson",
                "media_type": "application/x-ndjson",
                "bytes": imu_payload.len(),
                "sha256": imu_id
            },
            "sample_count": 1,
            "units": "raw_int16",
            "coordinate_frame": "raw_device_axes"
        },
        "frames": {
            "artifact": {
                "artifact_id": frames_id,
                "role": "frames.index",
                "path": "frames/index.ndjson",
                "media_type": "application/x-ndjson",
                "bytes": frames_payload.len(),
                "sha256": frames_id
            },
            "count": 300
        },
        "audio": {
            "state": "not_recorded",
            "requested_mode": "disabled",
            "resolved_mode": "disabled",
            "reason": "user_disabled"
        },
        "logs": [],
        "integrity": {
            "verified_at": "2026-11-26T10:00:11Z",
            "dropped_frames": 0,
            "drop_events": [],
            "fatal_errors": []
        }
    });
    let manifest_bytes =
        Bytes::from(serde_json::to_vec(&manifest_json).unwrap_or_else(|_| b"{}".to_vec()));
    let manifest = RawManifest::from_bytes(manifest_bytes);

    let summary = json!({
        "session_id": session_id,
        "producer_outcome": "sealed",
        "take_id": take_id,
        "take_sequence": 1,
        "continuation_of": null,
        "display_name": display_name,
        "device": {
            "device_id": device_id,
            "device_label": device_label
        },
        "started_at": "2026-11-26T10:00:00Z",
        "ended_at": "2026-11-26T10:00:10Z",
        "duration_seconds": 10.0,
        "total_bytes": total_bytes,
        "verification": {
            "actor": "gateway",
            "validator": {
                "name": "miro-ego-gateway",
                "version": "0.3.0",
                "build_sha256": "b".repeat(64)
            },
            "manifest_sha256": manifest.sha256,
            "verified_at": "2026-11-26T10:00:11Z",
            "verdict": "usable",
            "diagnostics": []
        }
    });

    let mut artifacts = HashMap::new();
    artifacts.insert(
        left_id.clone(),
        MockArtifact {
            artifact_id: left_id.clone(),
            media_type: "video/mp4".to_owned(),
            backing: MockArtifactBacking::Memory(left_payload),
        },
    );
    artifacts.insert(
        right_id.clone(),
        MockArtifact {
            artifact_id: right_id,
            media_type: "video/mp4".to_owned(),
            backing: MockArtifactBacking::Memory(right_payload),
        },
    );
    artifacts.insert(
        imu_id.clone(),
        MockArtifact {
            artifact_id: imu_id,
            media_type: "application/x-ndjson".to_owned(),
            backing: MockArtifactBacking::Memory(imu_payload),
        },
    );
    artifacts.insert(
        frames_id.clone(),
        MockArtifact {
            artifact_id: frames_id,
            media_type: "application/x-ndjson".to_owned(),
            backing: MockArtifactBacking::Memory(frames_payload),
        },
    );

    MockSession {
        session_id: session_id.to_owned(),
        take_id: take_id.to_owned(),
        primary_artifact_id: left_id,
        manifest,
        summary,
        verified: true,
        verification_reason: "usable".to_owned(),
        artifacts,
    }
}

#[allow(clippy::too_many_arguments)]
fn build_mock_recording_state(
    state_name: &str,
    authority_epoch: &str,
    state_revision: u64,
    session_id: &str,
    take_id: &str,
    display_name: &str,
    device_id: &str,
    device_label: &str,
    volume_id: &str,
    updated_at: &str,
) -> Value {
    let mut progress = json!({
        "elapsed_seconds": 1.0,
        "captured_frames": 30,
        "bytes_written": 4096
    });
    if state_name == "verifying"
        && let Some(obj) = progress.as_object_mut()
    {
        obj.insert(
            "verification".to_owned(),
            json!({ "completed": 0, "total": 4 }),
        );
    }

    let diagnostics = match state_name {
        "failed" => json!([{
            "code": "encoder_fault",
            "severity": "error",
            "message": "hardware encoder fault terminated capture session",
            "at": updated_at,
            "recoverable": false
        }]),
        "recoverable" => json!([{
            "code": "power_interrupted",
            "severity": "warning",
            "message": "capture interrupted with recoverable prefix",
            "at": updated_at,
            "recoverable": true
        }]),
        _ => json!([]),
    };

    json!({
        "schema": "miro-ego.recording-state.v1",
        "state": state_name,
        "authority_epoch": authority_epoch,
        "state_revision": state_revision,
        "updated_at": updated_at,
        "session_id": session_id,
        "take_id": take_id,
        "display_name": display_name,
        "device": {
            "device_id": device_id,
            "device_label": device_label
        },
        "storage": {
            "volume_id": volume_id,
            "status": "mounted",
            "writable": true,
            "remaining_bytes": 96_000_000_000_u64
        },
        "progress": progress,
        "diagnostics": diagnostics
    })
}

fn mock_runtime_json(state: &MockState) -> Value {
    let focus = if state.camera_connected && state.focus_supported {
        json!({
            "schema": "miro-ego.camera-focus.v1",
            "value": state.focus_value,
            "minimum": 0,
            "maximum": 1023,
            "step": 1,
            "default": 250,
            "auto_supported": true,
            "auto_enabled": state.focus_auto
        })
    } else {
        Value::Null
    };
    json!({
        "observed_at": rfc3339_now_utc(),
        "connection_method": "wifi_ap",
        "temperature_celsius": 42.5,
        "network": {
            "ap": {
                "state": "active",
                "interface": "wlan0",
                "addresses": ["10.42.0.1"],
                "peer_or_ssid": "MiroEgo-1234ABCD"
            },
            "wifi_client": {
                "state": "disconnected",
                "interface": null,
                "addresses": [],
                "peer_or_ssid": null
            },
            "wired": {
                "state": "disconnected",
                "interface": "eth0",
                "addresses": [],
                "peer_or_ssid": null
            },
            "default_route": "none"
        },
        "live_imu": null,
        "camera": {
            "schema": "miro-ego.camera-connection.v1",
            "state": if state.camera_connected { "connected" } else { "disconnected" }
        },
        "camera_focus": focus
    })
}

fn mock_capture_snapshot_data(state: &MockState) -> Value {
    let active_recording = if matches!(
        state.device_state.as_str(),
        "recording" | "finalizing" | "encoding" | "verifying"
    ) {
        let sid = state.active_session_id.clone().unwrap_or_else(new_uuid_v7);
        let tid = state.active_take_id.clone().unwrap_or_else(new_uuid_v7);
        let disp = state
            .active_display_name
            .as_deref()
            .unwrap_or("Active Capture Session");
        let gen_id = state
            .active_generation_id
            .clone()
            .unwrap_or_else(new_uuid_v4);
        let now = rfc3339_now_utc();
        json!({
            "generation_id": gen_id,
            "recording_state": build_mock_recording_state(
                &state.device_state,
                &state.authority_epoch,
                state.source_revision,
                &sid,
                &tid,
                disp,
                &state.device_id,
                &state.device_label,
                &state.volume_id,
                &now,
            )
        })
    } else {
        Value::Null
    };
    json!({
        "schema": "miro-ego.capture-snapshot-event.v1",
        "device_state": state.device_state,
        "active_recording": active_recording,
        "retained_unsuccessful": null,
        "runtime": mock_runtime_json(state)
    })
}

fn mock_capture_status_json(state: &MockState) -> Value {
    json!({
        "schema": "miro-ego.capture-status.v1",
        "authority_epoch": state.authority_epoch,
        "source_revision": state.source_revision,
        "snapshot": mock_capture_snapshot_data(state)
    })
}

fn mock_network_status_json(state: &MockState) -> Value {
    let desired_wifi = if state.network_mode == "wifi-client" {
        json!({
            "ssid": state.network_ssid.clone().unwrap_or_else(|| "studio-wifi".to_owned()),
            "security": state.network_security.clone().unwrap_or_else(|| "wpa2-personal".to_owned()),
            "credential_state": if state.network_security.as_deref() == Some("open") { "absent" } else { "stored" }
        })
    } else {
        Value::Null
    };
    json!({
        "schema": "miro-ego.network-status.v1",
        "authority_epoch": state.authority_epoch,
        "source_revision": state.network_revision,
        "observed_at": rfc3339_now_utc(),
        "saved": state.network_saved,
        "verified": state.network_verified,
        "desired": {
            "mode": state.network_mode,
            "wifi_client": desired_wifi,
            "ethernet": null
        },
        "observed": {
            "ap": {
                "state": "active",
                "interface": "wlan0",
                "addresses": ["10.42.0.1"],
                "peer_or_ssid": "MiroEgo-1234ABCD"
            },
            "wifi_client": {
                "state": "disconnected",
                "interface": null,
                "addresses": [],
                "peer_or_ssid": null
            },
            "wired": {
                "state": "disconnected",
                "interface": "eth0",
                "addresses": [],
                "peer_or_ssid": null
            },
            "default_route": "none",
            "mdns": {
                "hostname": "miro-ego.local",
                "service": "_miro-ego._tcp",
                "aliases": ["miro-ego.local"],
                "port": 443
            },
            "devices": [
                { "interface": "wlan0", "type": "wifi", "state": "connected" },
                { "interface": "eth0", "type": "ethernet", "state": "disconnected" }
            ]
        },
        "transaction": {
            "current": null,
            "latest": state.latest_network_tx
        },
        "mutation_capability": {
            "enabled": true,
            "disabled_reason": null,
            "operations": ["apply", "retry", "forget"],
            "idempotency_key_required": true,
            "secret_handling": "opaque_credential_reference_only",
            "active_state_policy": "idle_only"
        },
        "concurrency_capability": {
            "rescue_ap_required": true,
            "same_phy_ap_sta": "supported",
            "exclusive_client_failure_timeout_seconds": 10,
            "max_managed_interfaces": 1,
            "max_ap_interfaces": 1
        }
    })
}

fn check_idempotency(
    state: &MockState,
    scope: &str,
    cmd: &IdempotentCommand,
) -> ApiResult<Option<CommandOutcome>> {
    let key = (
        scope.to_owned(),
        cmd.principal_id.clone(),
        cmd.idempotency_key.clone(),
    );
    if let Some(existing) = state.idempotency.get(&key) {
        if existing.canonical_body == cmd.canonical_body {
            let mut replayed = existing.outcome.clone();
            replayed.replayed = true;
            return Ok(Some(replayed));
        }
        if matches!(scope, "network_apply" | "network_retry" | "network_forget") {
            let orig_tx_id = existing
                .outcome
                .body
                .as_ref()
                .and_then(|b| b.get("transaction"))
                .and_then(|t| t.get("transaction_id"))
                .and_then(Value::as_str);
            return Err(ApiError::network_idempotency_conflict(orig_tx_id));
        }
        return Err(ApiError::conflict(
            "idempotency_conflict",
            "Idempotency-Key reused with different request payload",
        )
        .with_error_code_header());
    }
    Ok(None)
}

fn store_idempotency(
    state: &mut MockState,
    scope: &str,
    cmd: &IdempotentCommand,
    outcome: &CommandOutcome,
) {
    let key = (
        scope.to_owned(),
        cmd.principal_id.clone(),
        cmd.idempotency_key.clone(),
    );
    state.idempotency.insert(
        key,
        IdempotencyRecord {
            canonical_body: cmd.canonical_body.clone(),
            outcome: outcome.clone(),
        },
    );
}

#[async_trait]
impl GatewayBackend for MockGatewayBackend {
    async fn device_info(&self, security_profile: SecurityProfile) -> ApiResult<Value> {
        let state = self.lock_state()?;
        Ok(json!({
            "schema": "miro-ego.device.v1",
            "device": {
                "device_id": state.device_id,
                "device_label": state.device_label
            },
            "hardware_fingerprint": state.hardware_fingerprint,
            "api_version": "1.0",
            "build": {
                "package_version": "0.3.0",
                "commit": state.firmware_current.1,
                "build_id": "miro-ego-0.3.0-rust"
            },
            "security_profile": security_profile.as_str(),
            "capabilities": {
                "capture": true,
                "preview": true,
                "preview_h264": self.h264_hub.is_some(),
                "diagnostic_logs": true,
                "range_download": true,
                "network_mutation": true,
                "session_list": true,
                "session_detail": true,
                "artifact_download": true,
                "capture_status": true,
                "session_deletion": true,
                "calibration_capture": {
                    "supported": true,
                    "enabled": true,
                    "disabled_reason": null,
                    "required_video_layout": "split-eyes"
                }
            },
            "storage": {
                "volume_id": state.volume_id,
                "total_bytes": 128_000_000_000_u64,
                "available_bytes": 96_000_000_000_u64,
                "writable": true
            },
            "runtime": mock_runtime_json(&state)
        }))
    }

    async fn capture_status(&self) -> ApiResult<Value> {
        let state = self.lock_state()?;
        Ok(mock_capture_status_json(&state))
    }

    async fn capture_snapshot_event(&self) -> ApiResult<Value> {
        let state = self.lock_state()?;
        Ok(json!({
            "authority_epoch": state.authority_epoch,
            "source_revision": state.source_revision,
            "type": "snapshot",
            "occurred_at": rfc3339_now_utc(),
            "session_id": state.active_session_id,
            "data": mock_capture_snapshot_data(&state)
        }))
    }

    async fn capture_readiness(&self) -> ApiResult<Value> {
        Ok(json!({
            "schema": "miro-ego.capture-readiness.v1",
            "allowed": true,
            "code": "ready",
            "message": "storage and capture prerequisites satisfied",
            "storage": {
                "recording": {
                    "available_bytes": 96_000_000_000_u64,
                    "required_bytes": 2_147_483_648_u64
                },
                "system": {
                    "available_bytes": 10_000_000_000_u64,
                    "required_bytes": 536_870_912_u64
                },
                "backing": null
            }
        }))
    }

    async fn capture_safe_swap(&self) -> ApiResult<Option<Value>> {
        let state = self.lock_state()?;
        Ok(state.safe_swap_receipt.clone())
    }

    async fn start_capture(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        let (outcome, state_event) = {
            let mut state = self.lock_state()?;
            if let Some(replayed) = check_idempotency(&state, "capture_start", &cmd)? {
                return Ok(replayed);
            }
            if !state.camera_connected {
                return Err(ApiError::camera_not_connected(
                    "configured camera is not connected",
                ));
            }
            if state.device_state == "idle" {
                let sid = new_uuid_v7();
                let tid = new_uuid_v7();
                let gen_id = new_uuid_v4();
                let display_name = cmd
                    .body
                    .get("display_name")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                state.device_state = "recording".to_owned();
                state.active_session_id = Some(sid.clone());
                state.active_take_id = Some(tid);
                state.active_display_name = display_name;
                state.active_generation_id = Some(gen_id.clone());
                state.source_revision += 1;
                let ev = json!({
                    "authority_epoch": state.authority_epoch,
                    "source_revision": state.source_revision,
                    "type": "state",
                    "occurred_at": rfc3339_now_utc(),
                    "session_id": sid,
                    "data": {
                        "schema": "miro-ego.capture-state-event.v1",
                        "state": "recording",
                        "volume_id": state.volume_id,
                        "generation_id": gen_id
                    }
                });
                let outcome = CommandOutcome::accepted(mock_capture_status_json(&state), false);
                store_idempotency(&mut state, "capture_start", &cmd, &outcome);
                (outcome, Some(ev))
            } else {
                let outcome = CommandOutcome::accepted(mock_capture_status_json(&state), false);
                store_idempotency(&mut state, "capture_start", &cmd, &outcome);
                (outcome, None)
            }
        };
        if let Some(ev) = state_event {
            let _ = self.capture_events.publish(ev);
        }
        Ok(outcome)
    }

    async fn stop_capture(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        let (outcome, stop_event) = {
            let mut state = self.lock_state()?;
            if let Some(replayed) = check_idempotency(&state, "capture_stop", &cmd)? {
                return Ok(replayed);
            }
            if state.device_state == "idle" {
                let outcome = CommandOutcome::no_content(false);
                store_idempotency(&mut state, "capture_stop", &cmd, &outcome);
                (outcome, None)
            } else {
                state.device_state = "idle".to_owned();
                state.active_session_id = None;
                state.active_take_id = None;
                state.active_display_name = None;
                state.active_generation_id = None;
                state.source_revision += 1;
                let snap_data = mock_capture_snapshot_data(&state);
                let ev = json!({
                    "authority_epoch": state.authority_epoch,
                    "source_revision": state.source_revision,
                    "type": "snapshot",
                    "occurred_at": rfc3339_now_utc(),
                    "session_id": Value::Null,
                    "data": snap_data
                });
                let outcome = CommandOutcome::accepted(mock_capture_status_json(&state), false);
                store_idempotency(&mut state, "capture_stop", &cmd, &outcome);
                (outcome, Some(ev))
            }
        };
        if let Some(ev) = stop_event {
            let _ = self.capture_events.publish(ev);
        }
        Ok(outcome)
    }

    async fn capture_timer(&self) -> ApiResult<Value> {
        let state = self.lock_state()?;
        Ok(json!({
            "schema": "miro-ego.capture-timer.v1",
            "settings": {
                "enabled": state.timer_enabled,
                "duration_seconds": state.timer_duration_seconds
            },
            "active": null
        }))
    }

    async fn set_capture_timer(&self, enabled: bool, duration_seconds: u64) -> ApiResult<Value> {
        let mut state = self.lock_state()?;
        if state.device_state != "idle" {
            return Err(ApiError::conflict(
                "capture_active",
                "cannot modify capture timer while capture is active",
            ));
        }
        state.timer_enabled = enabled;
        state.timer_duration_seconds = duration_seconds;
        Ok(json!({
            "schema": "miro-ego.capture-timer.v1",
            "settings": {
                "enabled": state.timer_enabled,
                "duration_seconds": state.timer_duration_seconds
            },
            "active": null
        }))
    }

    async fn camera_focus(&self) -> ApiResult<Option<Value>> {
        let state = self.lock_state()?;
        if !state.camera_connected {
            return Err(ApiError::camera_not_connected(
                "configured camera is not connected",
            ));
        }
        if !state.focus_supported {
            return Ok(None);
        }
        Ok(Some(json!({
            "schema": "miro-ego.camera-focus.v1",
            "value": state.focus_value,
            "minimum": 0,
            "maximum": 1023,
            "step": 1,
            "default": 250,
            "auto_supported": true,
            "auto_enabled": state.focus_auto
        })))
    }

    async fn set_camera_focus(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        let mut state = self.lock_state()?;
        if !state.camera_connected {
            return Err(ApiError::camera_not_connected(
                "configured camera is not connected",
            ));
        }
        if !state.focus_supported {
            return Err(ApiError::camera_focus_unsupported());
        }
        if let Some(replayed) = check_idempotency(&state, "camera_focus", &cmd)? {
            return Ok(replayed);
        }
        if let Some(val) = cmd.body.get("value").and_then(Value::as_u64) {
            if val > 1023 {
                return Err(ApiError::unprocessable(
                    "invalid_camera_focus",
                    "focus value must be within 0..=1023",
                )
                .with_error_code_header());
            }
            state.focus_value = val;
        }
        if let Some(auto) = cmd.body.get("auto_enabled").and_then(Value::as_bool) {
            state.focus_auto = auto;
        }
        let body = json!({
            "schema": "miro-ego.camera-focus.v1",
            "value": state.focus_value,
            "minimum": 0,
            "maximum": 1023,
            "step": 1,
            "default": 250,
            "auto_supported": true,
            "auto_enabled": state.focus_auto
        });
        let outcome = CommandOutcome::ok(body, false);
        store_idempotency(&mut state, "camera_focus", &cmd, &outcome);
        Ok(outcome)
    }

    async fn list_sessions(&self, query: ListSessionsQuery) -> ApiResult<Value> {
        let state = self.lock_state()?;
        let all_summaries: Vec<&Value> = state
            .session_order
            .iter()
            .filter_map(|sid| state.sessions.get(sid).map(|s| &s.summary))
            .collect();
        let catalog_input = serde_json::to_vec(&all_summaries).unwrap_or_default();
        let catalog_revision = format!("sha256:{}", sha256_hex(&catalog_input));

        let take_tok = query.take_id.as_deref().unwrap_or("-");
        let rec_tok = if query.include_recovery { "1" } else { "0" };

        let offset = match query.cursor.as_deref() {
            None => 0usize,
            Some(tok) => {
                let parts: Vec<&str> = tok.splitn(4, '~').collect();
                if parts.len() != 4 {
                    return Err(
                        ApiError::new(400, "invalid_cursor", "invalid pagination cursor")
                            .with_retryable(false),
                    );
                }
                let parsed_off = parts[3].parse::<usize>().map_err(|_| {
                    ApiError::new(400, "invalid_cursor", "invalid pagination cursor offset")
                        .with_retryable(false)
                })?;
                if parts[0] != catalog_revision {
                    return Err(ApiError::conflict(
                        "catalog_changed",
                        "session catalog revision changed; restart listing without cursor",
                    )
                    .with_retryable(true)
                    .with_error_code_header()
                    .with_details(json!({ "catalog_revision": catalog_revision })));
                }
                if parts[1] != take_tok || parts[2] != rec_tok {
                    return Err(ApiError::new(
                        400,
                        "invalid_cursor",
                        "cursor filter does not match request filter",
                    )
                    .with_retryable(false));
                }
                parsed_off
            }
        };

        let mut items = Vec::new();
        for sid in &state.session_order {
            if let Some(sess) = state.sessions.get(sid) {
                if let Some(filter_take) = &query.take_id
                    && &sess.take_id != filter_take
                {
                    continue;
                }
                let mut item = sess.summary.clone();
                if query.include_recovery
                    && let Some(obj) = item.as_object_mut()
                {
                    obj.insert("recovered_prefix".to_owned(), Value::Bool(true));
                }
                items.push(item);
            }
        }

        let limit = query.limit.clamp(1, 200);
        let page: Vec<Value> = items.iter().skip(offset).take(limit).cloned().collect();
        let next_cursor = if offset + page.len() < items.len() {
            Some(format!(
                "{catalog_revision}~{take_tok}~{rec_tok}~{}",
                offset + page.len()
            ))
        } else {
            None
        };

        Ok(json!({
            "schema": "miro-ego.session-list.v1",
            "catalog_revision": catalog_revision,
            "items": page,
            "diagnostics": [],
            "next_cursor": next_cursor
        }))
    }

    async fn open_session_manifest(&self, session_id: &str) -> ApiResult<RawManifest> {
        let state = self.lock_state()?;
        state
            .sessions
            .get(session_id)
            .map(|s| s.manifest.clone())
            .ok_or_else(|| ApiError::not_found("session not found or not sealed"))
    }

    async fn retained_unsuccessful_outcome(&self, session_id: &str) -> ApiResult<Option<Value>> {
        let state = self.lock_state()?;
        Ok(state.unsuccessful_outcomes.get(session_id).cloned())
    }

    async fn artifact_io_busy_state(&self) -> ApiResult<Option<String>> {
        let state = self.lock_state()?;
        if matches!(
            state.device_state.as_str(),
            "recording" | "finalizing" | "encoding" | "verifying"
        ) {
            return Ok(Some(state.device_state.clone()));
        }
        Ok(None)
    }

    async fn open_session_artifact(
        &self,
        session_id: &str,
        artifact_id: &str,
    ) -> ApiResult<OpenRepresentation> {
        let artifact = {
            let state = self.lock_state()?;
            let session = state
                .sessions
                .get(session_id)
                .ok_or_else(|| ApiError::not_found("session not found"))?;
            if !session.verified {
                let reason = match session.verification_reason.as_str() {
                    "absent" | "unusable" | "stale" => session.verification_reason.as_str(),
                    _ => "absent",
                };
                return Err(ApiError::session_not_verified(reason));
            }
            session
                .artifacts
                .get(artifact_id)
                .cloned()
                .ok_or_else(|| ApiError::not_found("artifact not found"))?
        };
        let etag = format!("\"{}\"", artifact.artifact_id);
        match artifact.backing {
            MockArtifactBacking::Memory(bytes) => Ok(OpenRepresentation::from_memory(
                artifact.media_type,
                etag,
                bytes,
            )),
            MockArtifactBacking::FilePath(path) => {
                let media_type = artifact.media_type;
                tokio::task::spawn_blocking(move || {
                    let file =
                        miro_ego_core::sys::fs::open_readonly_safe(&path).map_err(|err| {
                            ApiError::provider_failure(format!("open artifact file: {err}"))
                        })?;
                    let meta = file.metadata().map_err(|err| {
                        ApiError::provider_failure(format!("stat artifact file: {err}"))
                    })?;
                    Ok(OpenRepresentation::from_file(
                        media_type,
                        etag,
                        file,
                        meta.len(),
                    ))
                })
                .await
                .map_err(|err| {
                    ApiError::internal(
                        "task_join_error",
                        format!("open artifact task failed: {err}"),
                    )
                })?
            }
        }
    }

    async fn delete_sessions(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        let mut state = self.lock_state()?;
        if let Some(replayed) = check_idempotency(&state, "sessions_delete", &cmd)? {
            return Ok(replayed);
        }
        if state.device_state != "idle" {
            return Err(ApiError::conflict(
                "capture_active",
                "cannot delete sessions while capture is active",
            ));
        }
        let Some(requested) = cmd.body.get("sessions").and_then(Value::as_array) else {
            return Err(ApiError::bad_request("missing sessions list"));
        };
        // Validate all digests before deleting anything.
        let mut targets = Vec::with_capacity(requested.len());
        for entry in requested {
            let sid = entry
                .get("session_id")
                .and_then(Value::as_str)
                .ok_or_else(|| ApiError::bad_request("missing session_id"))?;
            let digest = entry
                .get("manifest_sha256")
                .and_then(Value::as_str)
                .ok_or_else(|| ApiError::bad_request("missing manifest_sha256"))?;
            let Some(sess) = state.sessions.get(sid) else {
                return Err(ApiError::not_found(format!("session {sid} not found")));
            };
            if sess.manifest.sha256 != digest {
                return Err(ApiError::conflict(
                    "manifest_digest_mismatch",
                    "provided manifest_sha256 does not match stored sealed manifest",
                ));
            }
            targets.push(sid.to_owned());
        }
        for sid in &targets {
            state.sessions.remove(sid);
            state.session_order.retain(|item| item != sid);
        }
        let result = json!({
            "schema": "miro-ego.session-delete-result.v1",
            "deleted_session_ids": targets,
            "failed_sessions": []
        });
        let outcome = CommandOutcome::ok(result, false);
        store_idempotency(&mut state, "sessions_delete", &cmd, &outcome);
        Ok(outcome)
    }

    async fn list_logs(&self, query: ListLogsQuery) -> ApiResult<Value> {
        let (volume_id, logs_dir, mem_logs) = {
            let state = self.lock_state()?;
            (
                state.volume_id.clone(),
                state.logs_dir.clone(),
                state.in_memory_logs.clone(),
            )
        };
        let limit = query.limit.clamp(1, 500);
        let mut entries: Vec<(String, u64, String, String, Option<String>)> = Vec::new();

        if let Some(root) = logs_dir {
            let prefix = query.prefix.clone();
            entries = tokio::task::spawn_blocking(move || {
                let mut disk_entries = Vec::new();
                collect_disk_logs(&root, "", prefix.as_deref(), &mut disk_entries)?;
                Ok::<_, ApiError>(disk_entries)
            })
            .await
            .map_err(|err| {
                ApiError::internal("task_join_error", format!("list logs task failed: {err}"))
            })??;
        } else {
            for entry in mem_logs.values() {
                if let Some(prefix) = query.prefix.as_deref()
                    && !entry.name.starts_with(prefix)
                {
                    continue;
                }
                entries.push((
                    entry.name.clone(),
                    entry.bytes.len() as u64,
                    entry.modified_at.clone(),
                    infer_log_media_type(&entry.name).to_owned(),
                    Some(entry.sha256.clone()),
                ));
            }
        }

        entries.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
        let truncated = entries.len() > limit;
        let items: Vec<Value> = entries
            .into_iter()
            .take(limit)
            .map(|(name, bytes, modified_at, media_type, sha256)| {
                json!({
                    "name": name,
                    "bytes": bytes,
                    "modified_at": modified_at,
                    "media_type": media_type,
                    "sha256": sha256
                })
            })
            .collect();

        Ok(json!({
            "schema": "miro-ego.log-list.v1",
            "volume_id": volume_id,
            "observed_at": rfc3339_now_utc(),
            "items": items,
            "truncated": truncated
        }))
    }

    async fn open_log_file(&self, name: &str) -> ApiResult<OpenRepresentation> {
        if !is_valid_log_name(name) {
            return Err(ApiError::bad_request(
                "invalid diagnostic log path; traversal and hidden files are forbidden",
            ));
        }
        let (logs_dir, mem_entry) = {
            let state = self.lock_state()?;
            (
                state.logs_dir.clone(),
                state.in_memory_logs.get(name).cloned(),
            )
        };
        if let Some(root) = logs_dir {
            let owned_name = name.to_owned();
            return tokio::task::spawn_blocking(move || open_safe_log_file(&root, &owned_name))
                .await
                .map_err(|err| {
                    ApiError::internal(
                        "task_join_error",
                        format!("open log file task failed: {err}"),
                    )
                })?;
        }
        let entry =
            mem_entry.ok_or_else(|| ApiError::not_found("diagnostic log file not found"))?;
        let leaf = name.rsplit('/').next().unwrap_or(name).replace('"', "");
        Ok(OpenRepresentation::from_memory(
            infer_log_media_type(name),
            format!("\"{}\"", entry.sha256),
            entry.bytes,
        )
        .with_content_disposition(format!("attachment; filename=\"{leaf}\"")))
    }

    fn preview_mjpeg_hub(&self) -> Option<Arc<MjpegPreviewHub>> {
        Some(Arc::clone(&self.mjpeg_hub))
    }

    fn preview_h264_hub(&self) -> Option<Arc<H264PreviewHub>> {
        self.h264_hub.as_ref().map(Arc::clone)
    }

    async fn open_preview_mjpeg(&self, _fps: Option<u32>) -> ApiResult<Arc<MjpegPreviewHub>> {
        let state = self.lock_state()?;
        if state.device_state == "blocked" {
            return Err(ApiError::conflict(
                "preview_state_conflict",
                "preview is unavailable in the current device state",
            ));
        }
        if !state.camera_connected {
            return Err(ApiError::camera_not_connected(
                "configured camera is not connected",
            ));
        }
        Ok(Arc::clone(&self.mjpeg_hub))
    }

    async fn open_preview_h264(
        &self,
        _fps: Option<u32>,
        _bitrate_kbps: Option<u32>,
    ) -> ApiResult<Arc<H264PreviewHub>> {
        let Some(hub) = self.h264_hub.as_ref().map(Arc::clone) else {
            return Err(ApiError::preview_h264_unsupported());
        };
        let state = self.lock_state()?;
        if state.device_state == "blocked" {
            return Err(ApiError::conflict(
                "preview_state_conflict",
                "preview is unavailable in the current device state",
            ));
        }
        if !state.camera_connected {
            return Err(ApiError::camera_not_connected(
                "configured camera is not connected",
            ));
        }
        Ok(hub)
    }

    fn capture_event_buffer(&self) -> Arc<EventReplayBuffer> {
        Arc::clone(&self.capture_events)
    }

    fn network_event_buffer(&self) -> Arc<NetworkEventReplayBuffer> {
        Arc::clone(&self.network_events)
    }

    async fn clock_status(&self, principal_id: &str) -> ApiResult<Value> {
        let mut state = self.lock_state()?;
        let (challenge, expires_in_ms) = if state.clock_synced {
            (Value::Null, 0u64)
        } else {
            let tok = new_challenge_hex();
            state
                .clock_challenges
                .insert(tok.clone(), (principal_id.to_owned(), false));
            (Value::String(tok), 5000u64)
        };
        Ok(json!({
            "schema": "miro-ego.clock-status.v1",
            "source": if state.clock_synced { "client" } else { "unsynchronized" },
            "unix_time_ms": state.clock_unix_ms,
            "challenge": challenge,
            "expires_in_ms": expires_in_ms,
            "applied": state.clock_synced,
            "boot_id": state.boot_id,
            "synchronized_at_ms": state.clock_synced_at_ms,
            "age_seconds": if state.clock_synced { json!(0) } else { Value::Null }
        }))
    }

    async fn clock_inspect(&self, principal_id: &str) -> ApiResult<Value> {
        let mut state = self.lock_state()?;
        let tok = new_challenge_hex();
        state
            .clock_challenges
            .insert(tok.clone(), (principal_id.to_owned(), true));
        Ok(json!({
            "schema": "miro-ego.clock-status.v1",
            "source": if state.clock_synced { "client" } else { "unsynchronized" },
            "unix_time_ms": state.clock_unix_ms,
            "challenge": tok,
            "expires_in_ms": 5000,
            "applied": state.clock_synced,
            "boot_id": state.boot_id,
            "synchronized_at_ms": state.clock_synced_at_ms,
            "age_seconds": if state.clock_synced { json!(0) } else { Value::Null }
        }))
    }

    async fn clock_sync(&self, principal_id: &str, body: Value) -> ApiResult<Value> {
        let mut state = self.lock_state()?;
        if state.device_state != "idle" {
            return Err(ApiError::conflict(
                "capture_active",
                "clock sync is prohibited while capture is active",
            ));
        }
        let challenge = body
            .get("challenge")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::bad_request("missing clock challenge"))?;
        let unix_time_ms = body
            .get("unix_time_ms")
            .and_then(Value::as_u64)
            .ok_or_else(|| ApiError::bad_request("missing unix_time_ms"))?;
        let replace = body
            .get("replace")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let Some((owner, from_check)) = state.clock_challenges.remove(challenge) else {
            return Err(ApiError::conflict(
                "clock_challenge_invalid",
                "clock challenge is expired, already used, or unknown",
            ));
        };
        if owner != principal_id {
            return Err(ApiError::conflict(
                "clock_challenge_invalid",
                "clock challenge is bound to a different principal",
            ));
        }
        if replace && !from_check {
            return Err(ApiError::conflict(
                "clock_challenge_invalid",
                "replace=true requires a fresh /clock/check challenge",
            ));
        }
        let applied = if !state.clock_synced || replace {
            state.clock_synced = true;
            state.clock_unix_ms = unix_time_ms;
            state.clock_synced_at_ms = Some(unix_time_ms);
            true
        } else {
            false
        };
        Ok(json!({
            "schema": "miro-ego.clock-status.v1",
            "source": "client",
            "unix_time_ms": state.clock_unix_ms,
            "challenge": null,
            "expires_in_ms": 0,
            "applied": applied,
            "boot_id": state.boot_id,
            "synchronized_at_ms": state.clock_synced_at_ms,
            "age_seconds": 0
        }))
    }

    async fn voice_status(&self) -> ApiResult<Value> {
        let state = self.lock_state()?;
        Ok(json!({
            "schema": "miro-ego.voice.v1",
            "enabled": true,
            "volume": state.voice_volume
        }))
    }

    async fn set_voice_volume(&self, volume: u8) -> ApiResult<Value> {
        let mut state = self.lock_state()?;
        if state.device_state != "idle" {
            return Err(ApiError::conflict(
                "capture_active",
                "voice volume cannot be modified while capture is active",
            ));
        }
        state.voice_volume = volume;
        Ok(json!({
            "schema": "miro-ego.voice.v1",
            "enabled": true,
            "volume": state.voice_volume
        }))
    }

    async fn network_status(&self) -> ApiResult<Value> {
        let state = self.lock_state()?;
        Ok(mock_network_status_json(&state))
    }

    async fn network_scan(&self) -> ApiResult<Value> {
        let state = self.lock_state()?;
        Ok(json!({
            "schema": "miro-ego.network-scan.v1",
            "authority_epoch": state.authority_epoch,
            "source_revision": state.network_revision,
            "scanned_at": rfc3339_now_utc(),
            "networks": [
                {
                    "ssid": "studio-wifi",
                    "hidden": false,
                    "security": "wpa2-personal",
                    "signal_dbm": -52,
                    "credential_required": true
                },
                {
                    "ssid": "lab-open",
                    "hidden": false,
                    "security": "open",
                    "signal_dbm": -64,
                    "credential_required": false
                }
            ]
        }))
    }

    async fn create_network_credential(
        &self,
        principal_id: &str,
        _passphrase: &str,
    ) -> ApiResult<Value> {
        let mut state = self.lock_state()?;
        let cred_ref = format!("cred-{}", new_uuid_v4());
        state
            .network_credentials
            .insert(cred_ref.clone(), principal_id.to_owned());
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Ok(json!({
            "schema": "miro-ego.network-credential-receipt.v1",
            "credential_ref": cred_ref,
            "issued_at": format_unix_secs_rfc3339(now_secs),
            "expires_at": format_unix_secs_rfc3339(now_secs.saturating_add(120)),
            "ttl_seconds": 120,
            "single_use": true
        }))
    }

    async fn apply_network_desired_state(
        &self,
        cmd: IdempotentCommand,
    ) -> ApiResult<CommandOutcome> {
        let (outcome, updated_status) = {
            let mut state = self.lock_state()?;
            if let Some(replayed) = check_idempotency(&state, "network_apply", &cmd)? {
                return Ok(replayed);
            }
            let desired = cmd
                .body
                .get("desired")
                .ok_or_else(|| ApiError::bad_request("missing desired network state"))?;
            let mode = desired
                .get("mode")
                .and_then(Value::as_str)
                .ok_or_else(|| ApiError::bad_request("missing desired.mode"))?;
            let desired_out = if mode == "wifi-client" {
                let wifi = desired
                    .get("wifi_client")
                    .and_then(Value::as_object)
                    .ok_or_else(|| {
                        ApiError::bad_request("wifi-client requires wifi_client object")
                    })?;
                let ssid = wifi
                    .get("ssid")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ApiError::bad_request("missing wifi_client.ssid"))?;
                let security = wifi
                    .get("security")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ApiError::bad_request("missing wifi_client.security"))?;
                if security != "open" {
                    let cred_ref = wifi
                        .get("credential_ref")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            ApiError::unprocessable(
                                "invalid_network_desired_state",
                                "protected Wi-Fi requires credential_ref",
                            )
                            .with_error_code_header()
                            .with_details(json!({
                                "field": "desired.wifi_client.credential_ref",
                                "reason": "missing_credential_ref"
                            }))
                        })?;
                    if state.network_credentials.remove(cred_ref).is_none() {
                        return Err(ApiError::unprocessable(
                            "invalid_network_desired_state",
                            "credential_ref expired or already used",
                        )
                        .with_error_code_header()
                        .with_details(json!({
                            "field": "desired.wifi_client.credential_ref",
                            "reason": "credential_ref_already_used"
                        })));
                    }
                }
                state.network_mode = "wifi-client".to_owned();
                state.network_saved = true;
                state.network_ssid = Some(ssid.to_owned());
                state.network_security = Some(security.to_owned());
                json!({
                    "mode": "wifi-client",
                    "wifi_client": {
                        "ssid": ssid,
                        "security": security,
                        "credential_state": if security == "open" { "absent" } else { "stored" }
                    },
                    "ethernet": null
                })
            } else {
                state.network_mode = mode.to_owned();
                json!({
                    "mode": mode,
                    "wifi_client": null,
                    "ethernet": desired.get("ethernet").cloned().unwrap_or(Value::Null)
                })
            };

            state.network_revision += 1;
            let now = rfc3339_now_utc();
            let tx_id = new_uuid_v7();
            let tx = json!({
                "schema": "miro-ego.network-transaction.v1",
                "authority_epoch": state.authority_epoch,
                "source_revision": state.network_revision,
                "transaction_id": tx_id,
                "operation": "apply",
                "status": "accepted",
                "stage": "accepted",
                "desired": desired_out,
                "accepted_at": now,
                "updated_at": now,
                "deadline": null,
                "recovery_action": "await_device",
                "rescue": {
                    "ap_validated": true,
                    "fallback_mode": "hotspot",
                    "failure_trigger_seconds": 10
                },
                "error": null
            });
            state.network_transactions.insert(tx_id, tx.clone());
            state.latest_network_tx = Some(tx.clone());
            let receipt = json!({
                "schema": "miro-ego.network-transaction-receipt.v1",
                "accepted_at": now,
                "transaction": tx
            });
            let outcome = CommandOutcome::accepted(receipt, false);
            store_idempotency(&mut state, "network_apply", &cmd, &outcome);
            (outcome, mock_network_status_json(&state))
        };
        let _ = self.network_events.observe(updated_status);
        Ok(outcome)
    }

    async fn retry_network_transaction(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        let (outcome, updated_status) = {
            let mut state = self.lock_state()?;
            if let Some(replayed) = check_idempotency(&state, "network_retry", &cmd)? {
                return Ok(replayed);
            }
            let tx_id = cmd
                .body
                .get("transaction_id")
                .and_then(Value::as_str)
                .ok_or_else(|| ApiError::bad_request("missing transaction_id"))?;
            let Some(prev_tx) = state.network_transactions.get(tx_id).cloned() else {
                return Err(ApiError::network_transaction_not_found(tx_id));
            };
            state.network_revision += 1;
            let now = rfc3339_now_utc();
            let new_tx_id = new_uuid_v7();
            let tx = json!({
                "schema": "miro-ego.network-transaction.v1",
                "authority_epoch": state.authority_epoch,
                "source_revision": state.network_revision,
                "transaction_id": new_tx_id,
                "operation": "retry",
                "status": "accepted",
                "stage": "accepted",
                "desired": prev_tx.get("desired").cloned().unwrap_or(json!({"mode":"hotspot","wifi_client":null,"ethernet":null})),
                "accepted_at": now,
                "updated_at": now,
                "deadline": null,
                "recovery_action": "await_device",
                "rescue": {
                    "ap_validated": true,
                    "fallback_mode": "hotspot",
                    "failure_trigger_seconds": 10
                },
                "error": null
            });
            state.network_transactions.insert(new_tx_id, tx.clone());
            state.latest_network_tx = Some(tx.clone());
            let receipt = json!({
                "schema": "miro-ego.network-transaction-receipt.v1",
                "accepted_at": now,
                "transaction": tx
            });
            let outcome = CommandOutcome::accepted(receipt, false);
            store_idempotency(&mut state, "network_retry", &cmd, &outcome);
            (outcome, mock_network_status_json(&state))
        };
        let _ = self.network_events.observe(updated_status);
        Ok(outcome)
    }

    async fn forget_network_client_profile(
        &self,
        cmd: IdempotentCommand,
    ) -> ApiResult<CommandOutcome> {
        let (outcome, updated_status) = {
            let mut state = self.lock_state()?;
            if let Some(replayed) = check_idempotency(&state, "network_forget", &cmd)? {
                return Ok(replayed);
            }
            state.network_mode = "hotspot".to_owned();
            state.network_saved = false;
            state.network_verified = false;
            state.network_ssid = None;
            state.network_security = None;
            state.network_revision += 1;
            let now = rfc3339_now_utc();
            let tx_id = new_uuid_v7();
            let tx = json!({
                "schema": "miro-ego.network-transaction.v1",
                "authority_epoch": state.authority_epoch,
                "source_revision": state.network_revision,
                "transaction_id": tx_id,
                "operation": "forget",
                "status": "accepted",
                "stage": "accepted",
                "desired": { "mode": "hotspot", "wifi_client": null, "ethernet": null },
                "accepted_at": now,
                "updated_at": now,
                "deadline": null,
                "recovery_action": "await_device",
                "rescue": {
                    "ap_validated": true,
                    "fallback_mode": "hotspot",
                    "failure_trigger_seconds": 10
                },
                "error": null
            });
            state.network_transactions.insert(tx_id, tx.clone());
            state.latest_network_tx = Some(tx.clone());
            let receipt = json!({
                "schema": "miro-ego.network-transaction-receipt.v1",
                "accepted_at": now,
                "transaction": tx
            });
            let outcome = CommandOutcome::accepted(receipt, false);
            store_idempotency(&mut state, "network_forget", &cmd, &outcome);
            (outcome, mock_network_status_json(&state))
        };
        let _ = self.network_events.observe(updated_status);
        Ok(outcome)
    }

    async fn firmware_status(&self) -> ApiResult<Value> {
        let state = self.lock_state()?;
        Ok(mock_firmware_status(&state, None))
    }

    async fn firmware_check(&self, _force: bool) -> ApiResult<Value> {
        let state = self.lock_state()?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Ok(mock_firmware_status(&state, Some(now)))
    }

    async fn firmware_update(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        let mut state = self.lock_state()?;
        if let Some(replayed) = check_idempotency(&state, "firmware_update", &cmd)? {
            return Ok(replayed);
        }
        let commit = cmd
            .body
            .get("commit")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::bad_request("missing commit"))?
            .to_owned();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let task = json!({
            "id": new_uuid_v7(),
            "action": "update",
            "target_commit": commit,
            "status": "accepted",
            "message": "firmware update accepted",
            "started_at": now,
            "finished_at": null
        });
        state.firmware_task = Some(task.clone());
        let outcome = CommandOutcome::accepted(task, false);
        store_idempotency(&mut state, "firmware_update", &cmd, &outcome);
        Ok(outcome)
    }

    async fn firmware_rollback(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        let mut state = self.lock_state()?;
        if let Some(replayed) = check_idempotency(&state, "firmware_rollback", &cmd)? {
            return Ok(replayed);
        }
        let commit = cmd
            .body
            .get("commit")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::bad_request("missing commit"))?
            .to_owned();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let task = json!({
            "id": new_uuid_v7(),
            "action": "rollback",
            "target_commit": commit,
            "status": "accepted",
            "message": "firmware rollback accepted",
            "started_at": now,
            "finished_at": null
        });
        state.firmware_task = Some(task.clone());
        let outcome = CommandOutcome::accepted(task, false);
        store_idempotency(&mut state, "firmware_rollback", &cmd, &outcome);
        Ok(outcome)
    }

    fn record_audit(&self, event: AuditEvent) -> ApiResult<()> {
        if self.fail_audit.load(Ordering::SeqCst) {
            return Err(ApiError::audit_unavailable());
        }
        let mut state = self.lock_state()?;
        state.audit_events.push(event);
        Ok(())
    }

    fn get_web_asset(&self, name: &str) -> Option<StaticAsset> {
        self.state.lock().ok()?.web_assets.get(name).cloned()
    }
}

fn mock_firmware_status(state: &MockState, checked_at: Option<u64>) -> Value {
    json!({
        "schema": "miro-ego.firmware-status.v1",
        "supported": true,
        "current": {
            "version": state.firmware_current.0,
            "commit": state.firmware_current.1
        },
        "previous": state.firmware_previous.as_ref().map(|(v, c)| json!({
            "version": v,
            "commit": c
        })),
        "available": state.firmware_available.as_ref().map(|(v, c)| json!({
            "version": v,
            "commit": c,
            "release_notes": "Miro-Ego release",
            "published_at": "2026-11-26T10:00:00Z"
        })),
        "has_update": state.firmware_available.is_some(),
        "checked_at": checked_at,
        "warning": null,
        "task": state.firmware_task
    })
}

fn new_challenge_hex() -> String {
    let raw = new_uuid_v4().replace('-', "");
    raw.chars().take(32).collect()
}

fn collect_disk_logs(
    dir: &std::path::Path,
    rel_prefix: &str,
    filter_prefix: Option<&str>,
    out: &mut Vec<(String, u64, String, String, Option<String>)>,
) -> ApiResult<()> {
    let read_dir = std::fs::read_dir(dir).map_err(|err| {
        ApiError::storage_unavailable(format!("cannot list logs directory: {err}"))
    })?;
    for entry_res in read_dir {
        let Ok(entry) = entry_res else { continue };
        let Ok(ft) = entry.file_type() else { continue };
        if ft.is_symlink() {
            continue;
        }
        let Some(name_str) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if name_str.starts_with('.') {
            continue;
        }
        let rel = if rel_prefix.is_empty() {
            name_str
        } else {
            format!("{rel_prefix}/{name_str}")
        };
        if ft.is_dir() {
            let _ = collect_disk_logs(&entry.path(), &rel, filter_prefix, out);
        } else if ft.is_file() && is_valid_log_name(&rel) {
            if let Some(prefix) = filter_prefix
                && !rel.starts_with(prefix)
            {
                continue;
            }
            if let Ok(meta) = entry.metadata() {
                let modified_secs = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                out.push((
                    rel.clone(),
                    meta.len(),
                    format_unix_secs_rfc3339(modified_secs),
                    infer_log_media_type(&rel).to_owned(),
                    None,
                ));
            }
        }
    }
    Ok(())
}
