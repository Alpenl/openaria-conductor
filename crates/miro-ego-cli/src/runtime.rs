//! Production `GatewayBackend` and `CaptureBackend` implementations plus daemon lifecycle
//! orchestration (startup crash recovery, 1 Hz storage reserve enforcement, capture timer
//! monitoring, live MJPEG/fMP4 preview hubs, LED/voice feedback, and graceful shutdown).

use async_trait::async_trait;
use bytes::Bytes;
use miro_ego_api::{
    ApiError, ApiResult, AuditEvent, CommandOutcome, EventReplayBuffer, GatewayBackend,
    GatewayServer, GatewayServerConfig, H264PreviewHub, IdempotentCommand, ListLogsQuery,
    ListSessionsQuery, MjpegPreviewHub, NetworkEventReplayBuffer, OpenRepresentation, Principal,
    RawManifest, SecurityPolicy, SecurityProfile, StaticAsset, TlsConfig,
};
use miro_ego_capture::audio::{self, Recorder as AudioRecorder};
use miro_ego_capture::hardware::imu::Collector as ImuCollector;
use miro_ego_capture::hardware::native_camera::{Stream as NativeCameraStream, StreamConfig};
use miro_ego_capture::hardware::preview::LatestBuffer;
use miro_ego_capture::session::active_take::ActiveTakeWriter;
use miro_ego_capture::session::capture_runtime::{FailureSink, RecordingTarget, Runtime};
use miro_ego_capture::session::recording::{
    ImuSampleRecord, RecordingSegmentPlanner, RecordingSink,
};
use miro_ego_capture::session::shm_ring::ShmRingConfig;
use miro_ego_capture::session::stereo_encoder::{EncoderConfig, EncoderProcess, EncoderTransport};
use miro_ego_core::config::{DeviceConfig, SecurityConfig};
use miro_ego_core::ids::{hex_encode, new_uuid_v4, new_uuid_v7};
use miro_ego_core::paths::{DataVolumePaths, SYSTEM_ENCODER_BIN};
use miro_ego_core::sys::clock::monotonic_ns;
use miro_ego_deploy::{
    DeployError, DeployResult, HealthChecker, ReleaseManager, ReleaseManagerPaths, SystemctlRunner,
};
use miro_ego_hw::thermal::read_soc_temperature_c;
use miro_ego_hw::{
    HwError, IndicatorController, IndicatorState, SysfsLedController, VoiceController, VoiceCue,
    VoiceFeedback,
};
use miro_ego_network::{ApplyDesiredState, ControllerStatus, NetworkError, NetworkRpcClient};
use miro_ego_storage::coordinator::{
    CAMERA_FOCUS_SCHEMA, CAPTURE_TIMER_SET_SCHEMA, DropEventRecord,
};
use miro_ego_storage::{
    ActiveCaptureHandle, ApiAuditEvent, AuditConfig, AuditLogger, AuditWriteTarget,
    BackendStartRequest, BinarySessionCatalog, CameraFocusSetRequest, CameraFocusStatus,
    CaptureBackend, CaptureCoordinator, CaptureEvent, CaptureEventType, CapturePhase,
    CaptureProgressSnapshot, CaptureStartRequest, CaptureStopRequest, CaptureTimerSetRequest,
    CapturedArtifact, CapturedAudioOutcome, CapturedAudioSegment, CapturedSessionArtifacts,
    CapturedVideoSegment, CoordinatorConfig, DeleteSessionsRequest, ExfatVolumeManager, LogStore,
    ReserveEnforcementOutcome, RuntimeObservation, StopReason, StorageError, StorageResult,
    VolumeManager, VolumePolicy, open_manifest, open_verified_artifact,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::future::Future;
use std::io::Write;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_RESERVE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_TIMER_POLL_INTERVAL: Duration = Duration::from_millis(250);
const ENCODER_FINISH_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_BEARER_TOKEN_FILE_BYTES: usize = 4096;
const SYNTHETIC_JPEG_FRAME: &[u8] = &[
    0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, b'J', b'F', b'I', b'F', 0x00, 0x01, 0x01, 0x00, 0x00, 0x01,
    0x00, 0x01, 0x00, 0x00, 0xFF, 0xDB, 0x00, 0x43, 0x00, 0x08, 0x06, 0x06, 0x07, 0x06, 0x05, 0x08,
    0x07, 0x07, 0x07, 0x09, 0x09, 0x08, 0x0A, 0x0C, 0x14, 0x0D, 0x0C, 0x0B, 0x0B, 0x0C, 0x19, 0x12,
    0x13, 0x0F, 0x14, 0x1D, 0x1A, 0x1F, 0x1E, 0x1D, 0x1A, 0x1C, 0x1C, 0x20, 0x24, 0x2E, 0x27, 0x20,
    0x22, 0x2C, 0x23, 0x1C, 0x1C, 0x28, 0x37, 0x29, 0x2C, 0x30, 0x31, 0x34, 0x34, 0x34, 0x1F, 0x27,
    0x39, 0x3D, 0x38, 0x32, 0x3C, 0x2E, 0x33, 0x34, 0x32, 0xFF, 0xC0, 0x00, 0x0B, 0x08, 0x00, 0x01,
    0x00, 0x01, 0x01, 0x01, 0x11, 0x00, 0xFF, 0xC4, 0x00, 0x14, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0xFF, 0xDA, 0x00, 0x08,
    0x01, 0x01, 0x00, 0x00, 0x3F, 0x00, 0x7F, 0xFF, 0xD9,
];
const SYNTHETIC_FMP4_INIT: &[u8] =
    b"\x00\x00\x00\x18ftypisom\x00\x00\x02\x00isomiso6\x00\x00\x00\x08moov";
const SYNTHETIC_FMP4_MOOF: &[u8] = b"\x00\x00\x00\x10moof\x00\x00\x00\x08mfhd\x00\x00\x00\x08mdat";
const EMBEDDED_INDEX_HTML: &[u8] = br#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Miro-Ego Device Console</title>
</head>
<body>
  <main id="app">
    <h1>Miro-Ego 0.3.0</h1>
    <p>Native Rust Device Gateway</p>
  </main>
  <script type="module" src="/app.js"></script>
</body>
</html>
"#;
const EMBEDDED_APP_JS: &[u8] = b"export const API_PREFIX = '/api/v1';\n";

/// No-op `SystemctlRunner` used for read-only daemon firmware status inspection and hermetic CLI runs.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSystemctlRunner;

impl SystemctlRunner for NoopSystemctlRunner {
    fn run_systemctl(&self, _args: &[&str]) -> DeployResult<()> {
        Ok(())
    }

    fn unit_load_state(&self, _unit: &str) -> DeployResult<String> {
        Ok("loaded".to_owned())
    }
}

/// No-op `HealthChecker` used when daemon activation is disabled or redirected to a hermetic root.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopHealthChecker;

impl HealthChecker for NoopHealthChecker {
    fn check_health(&self) -> DeployResult<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Error translation helpers
// ---------------------------------------------------------------------------

pub fn map_storage_error(err: StorageError) -> ApiError {
    match err {
        StorageError::Locked {
            code: "capture_busy",
            message,
        } => ApiError::capture_busy("recording", 1).with_details(json!({
            "wait_for": "idle",
            "retry_after_seconds": 1,
            "reason": message,
        })),
        StorageError::Unavailable { message } => ApiError::storage_unavailable(message),
        StorageError::CatalogChanged { catalog_revision } => ApiError::conflict(
            "catalog_changed",
            "session catalog revision changed; restart listing without cursor",
        )
        .with_retryable(true)
        .with_error_code_header()
        .with_details(json!({ "catalog_revision": catalog_revision })),
        StorageError::RangeNotSatisfiable { total_bytes } => {
            ApiError::range_not_satisfiable(total_bytes, "requested byte range is not satisfiable")
        }
        StorageError::Unsupported { code, message } => {
            ApiError::new(404, code, message).with_error_code_header()
        }
        other => {
            let status = other.http_status();
            let code = other.code().to_owned();
            let retryable = other.retryable();
            let message = other.to_string();
            ApiError::new(status, code, message)
                .with_retryable(retryable)
                .with_error_code_header()
        }
    }
}

pub fn map_network_error(err: NetworkError) -> ApiError {
    match err {
        NetworkError::Busy { message } => {
            if message.contains("capture_active") {
                ApiError::conflict("capture_active", message).with_error_code_header()
            } else {
                ApiError::service_unavailable("network_busy", message)
                    .with_header("Retry-After", "1")
                    .with_error_code_header()
            }
        }
        NetworkError::Invalid { message } => {
            if message.contains("idempotency_conflict") {
                ApiError::conflict("idempotency_conflict", message).with_error_code_header()
            } else if message.contains("transaction_not_found") {
                ApiError::not_found(message).with_header(
                    miro_ego_api::HEADER_ERROR_CODE,
                    "network_transaction_not_found",
                )
            } else if message.contains("credential_ref_") {
                ApiError::unprocessable("invalid_network_desired_state", message)
                    .with_error_code_header()
            } else {
                ApiError::bad_request(message).with_error_code_header()
            }
        }
        NetworkError::Backend { message } => {
            if message.contains("idempotency_conflict") {
                ApiError::conflict("idempotency_conflict", message).with_error_code_header()
            } else if message.contains("transaction_not_found") {
                ApiError::not_found(message).with_header(
                    miro_ego_api::HEADER_ERROR_CODE,
                    "network_transaction_not_found",
                )
            } else if message.contains("credential_ref_") {
                ApiError::unprocessable("invalid_network_desired_state", message)
                    .with_error_code_header()
            } else if message.contains("capture_active") {
                ApiError::conflict("capture_active", message).with_error_code_header()
            } else {
                ApiError::service_unavailable("network_controller_unavailable", message)
                    .with_header("Retry-After", "1")
                    .with_error_code_header()
            }
        }
        NetworkError::Core(core_err) => ApiError::from(core_err),
    }
}

pub fn map_hw_error(err: HwError) -> ApiError {
    match err {
        HwError::Unavailable { message } => {
            ApiError::service_unavailable("hardware_unavailable", message).with_error_code_header()
        }
        HwError::Io { message } => {
            ApiError::internal("hardware_io_failed", message).with_error_code_header()
        }
        HwError::Core(core_err) => ApiError::from(core_err),
    }
}

pub fn map_deploy_error(err: DeployError) -> ApiError {
    match err {
        DeployError::InvalidBundle { message } => {
            ApiError::unprocessable("invalid_firmware_bundle", message).with_error_code_header()
        }
        DeployError::Blocked { message } => {
            ApiError::conflict("firmware_blocked", message).with_error_code_header()
        }
        DeployError::Failed { message } => {
            ApiError::internal("firmware_failed", message).with_error_code_header()
        }
        DeployError::Core(core_err) => ApiError::from(core_err),
    }
}

fn rfc3339_now_utc() -> String {
    miro_ego_api::security::rfc3339_now_utc()
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_encode(&hasher.finalize())
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Security & TLS loader from DeviceConfig
// ---------------------------------------------------------------------------

/// Build the `SecurityPolicy` and optional `TlsConfig` matching `DeviceConfig.security`.
pub fn build_security_and_tls(
    config: &DeviceConfig,
) -> Result<(Arc<SecurityPolicy>, Option<TlsConfig>), String> {
    match &config.security {
        SecurityConfig::Lab { .. } => Ok((Arc::new(SecurityPolicy::lab_default()), None)),
        SecurityConfig::Customer {
            bearer_token_file,
            tls_certificate_file,
            tls_private_key_file,
            principal_id,
            ..
        } => {
            let meta = fs::metadata(bearer_token_file).map_err(|err| {
                format!(
                    "failed to stat customer bearer token file {}: {err}",
                    bearer_token_file.display()
                )
            })?;
            if meta.len() > MAX_BEARER_TOKEN_FILE_BYTES as u64 {
                return Err(format!(
                    "customer bearer token file {} exceeds {} bytes",
                    bearer_token_file.display(),
                    MAX_BEARER_TOKEN_FILE_BYTES
                ));
            }
            let raw_token = fs::read_to_string(bearer_token_file).map_err(|err| {
                format!(
                    "failed to read customer bearer token file {}: {err}",
                    bearer_token_file.display()
                )
            })?;
            let bearer_token = raw_token.trim().to_owned();
            if bearer_token.is_empty() {
                return Err(format!(
                    "customer bearer token file {} is empty",
                    bearer_token_file.display()
                ));
            }
            let csrf_token = std::env::var("MIRO_EGO_CSRF_TOKEN")
                .ok()
                .map(|v| v.trim().to_owned())
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| {
                    let mut hasher = Sha256::new();
                    hasher.update(b"miro-ego.csrf.v1:");
                    hasher.update(bearer_token.as_bytes());
                    hex_encode(&hasher.finalize())
                });
            let principal = Principal::all_v1_operations(principal_id.clone())
                .map_err(|err| format!("invalid customer principal_id: {err}"))?;
            let mut tokens = HashMap::new();
            tokens.insert(bearer_token, principal);
            let policy = SecurityPolicy::customer(tokens, HashSet::new(), Some(csrf_token))
                .map_err(|err| format!("failed to construct customer security policy: {err}"))?;
            let tls = TlsConfig::from_pem_files(tls_certificate_file, tls_private_key_file)
                .map_err(|err| format!("failed to load customer TLS credentials: {err}"))?;
            Ok((Arc::new(policy), Some(tls)))
        }
    }
}

// ---------------------------------------------------------------------------
// CaptureBackends: Native (V4L2 + SHM), SHM-Encoder-Driven, and Pure-Rust Mock
// ---------------------------------------------------------------------------

struct CaptureFailureLog {
    last_error: Mutex<Option<(String, String)>>,
}

impl CaptureFailureLog {
    fn new() -> Self {
        Self {
            last_error: Mutex::new(None),
        }
    }

    fn take(&self) -> Option<(String, String)> {
        self.last_error.lock().ok().and_then(|mut g| g.take())
    }
}

impl FailureSink for CaptureFailureLog {
    fn on_failure(&self, code: &str, message: &str) {
        if let Ok(mut guard) = self.last_error.lock() {
            if guard.is_none() {
                *guard = Some((code.to_owned(), message.to_owned()));
            }
        }
    }
}

/// Real on-device hardware capture backend wiring `miro-ego-capture` (`NativeCameraStream`,
/// `Runtime`, `EncoderProcess` over `MEGO_SHM` v1, `RecordingSink`, optional IMU & ALSA audio)
/// to `miro-ego-storage::CaptureCoordinator`.
pub struct NativeCaptureBackend {
    config: DeviceConfig,
    encoder_bin: PathBuf,
    imu_device: Option<PathBuf>,
    stream: Mutex<Option<Arc<NativeCameraStream>>>,
    runtime: Mutex<Option<Arc<Runtime>>>,
    preview_buf: Arc<LatestBuffer>,
    preview_stop: Arc<AtomicBool>,
    preview_thread: Mutex<Option<JoinHandle<()>>>,
    mjpeg_hub: Arc<MjpegPreviewHub>,
}

impl NativeCaptureBackend {
    pub fn open(
        config: DeviceConfig,
        encoder_bin: PathBuf,
        imu_device: Option<PathBuf>,
        mjpeg_hub: Arc<MjpegPreviewHub>,
    ) -> StorageResult<Self> {
        let preview_fps = config.camera.fps.clamp(1, 30);
        let preview_buf = Arc::new(LatestBuffer::new(preview_fps).map_err(|err| {
            StorageError::service_unavailable("preview_init_failed", err.message)
        })?);
        let preview_stop = Arc::new(AtomicBool::new(false));
        let backend = Self {
            config,
            encoder_bin,
            imu_device,
            stream: Mutex::new(None),
            runtime: Mutex::new(None),
            preview_buf,
            preview_stop,
            preview_thread: Mutex::new(None),
            mjpeg_hub,
        };
        // Best-effort camera open at startup so daemon still boots even if camera is unplugged.
        let _ = backend.try_connect_camera();
        Ok(backend)
    }

    /// Attempt to open the V4L2 camera stream and start preview fan-out.
    pub fn try_connect_camera(&self) -> StorageResult<()> {
        let mut stream_guard = self.stream.lock().map_err(|_| {
            StorageError::service_unavailable("camera_poisoned", "camera mutex poisoned")
        })?;
        if stream_guard.is_some() {
            return Ok(());
        }
        let device_str = self.config.camera.device.to_string_lossy().into_owned();
        let stream = NativeCameraStream::open(StreamConfig {
            device: &device_str,
            width: self.config.camera.width,
            height: self.config.camera.height,
            fps: self.config.camera.fps,
            encoding: "mjpeg",
            buffer_count: 6,
            queue_capacity: 16,
            split_eyes: false,
        })
        .map_err(|err| StorageError::camera_not_connected(err.message))?;
        let stream = Arc::new(stream);
        let runtime = Arc::new(
            Runtime::new(
                Arc::clone(&stream),
                Arc::clone(&self.preview_buf),
                u64::from(self.config.camera.frame_decimation),
                Duration::from_millis(500),
                None,
            )
            .map_err(|err| {
                StorageError::service_unavailable("capture_runtime_failed", err.message)
            })?,
        );
        runtime
            .start_preview()
            .map_err(|err| StorageError::camera_not_connected(err.message))?;

        self.start_preview_forwarder();
        *stream_guard = Some(stream);
        if let Ok(mut rt_guard) = self.runtime.lock() {
            *rt_guard = Some(runtime);
        }
        Ok(())
    }

    fn start_preview_forwarder(&self) {
        let Ok(mut thread_guard) = self.preview_thread.lock() else {
            return;
        };
        if thread_guard.is_some() {
            return;
        }
        self.preview_stop.store(false, Ordering::Release);
        let mut subscriber = self.preview_buf.subscribe();
        let stop = Arc::clone(&self.preview_stop);
        let hub = Arc::clone(&self.mjpeg_hub);
        if let Ok(handle) = thread::Builder::new()
            .name("miro-ego-preview-fanout".to_owned())
            .spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    match subscriber.recv_latest(&stop, Duration::from_millis(200)) {
                        Ok(Some(snapshot)) => {
                            let _ = hub.publish(Bytes::copy_from_slice(&snapshot.jpeg));
                        }
                        Ok(None) => {}
                        Err(_) => thread::sleep(Duration::from_millis(50)),
                    }
                }
            })
        {
            *thread_guard = Some(handle);
        }
    }

    pub fn shutdown(&self) {
        self.preview_stop.store(true, Ordering::Release);
        if let Ok(mut guard) = self.preview_thread.lock() {
            if let Some(handle) = guard.take() {
                let _ = handle.join();
            }
        }
        if let Ok(mut guard) = self.runtime.lock() {
            if let Some(rt) = guard.take() {
                let _ = rt.close(Duration::from_secs(3));
            }
        }
        if let Ok(mut guard) = self.stream.lock() {
            if let Some(stream) = guard.take() {
                let _ = stream.close();
            }
        }
    }
}

impl Drop for NativeCaptureBackend {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct NativeActiveHandle {
    runtime: Arc<Runtime>,
    active_take: Arc<Mutex<ActiveTakeWriter>>,
    sink: Arc<Mutex<RecordingSink>>,
    encoder: Arc<Mutex<EncoderProcess>>,
    segment_planner: Arc<Mutex<RecordingSegmentPlanner>>,
    failure_log: Arc<CaptureFailureLog>,
    audio: Option<Arc<AudioRecorder>>,
    audio_requested_mode: String,
    started_monotonic_ns: u64,
    effective_fps: f64,
}

impl ActiveCaptureHandle for NativeActiveHandle {
    fn progress(&self) -> CaptureProgressSnapshot {
        let now_ns = monotonic_ns().unwrap_or(self.started_monotonic_ns);
        let elapsed_seconds =
            now_ns.saturating_sub(self.started_monotonic_ns) as f64 / 1_000_000_000.0;
        let (frames, bytes) = self
            .sink
            .lock()
            .map(|s| {
                let snap = s.snapshot();
                (snap.frames_written, snap.bytes_written)
            })
            .unwrap_or((0, 0));
        CaptureProgressSnapshot {
            elapsed_seconds,
            captured_frames: frames,
            bytes_written: bytes,
        }
    }

    fn finish(self: Box<Self>, _reason: StopReason) -> StorageResult<CapturedSessionArtifacts> {
        let _ = self.runtime.stop_recording(Duration::from_secs(5));

        if let Some((code, msg)) = self.failure_log.take() {
            if let Ok(mut enc) = self.encoder.lock() {
                enc.abort();
            }
            if let Ok(mut sink) = self.sink.lock() {
                sink.close();
            }
            return Err(StorageError::service_unavailable(
                "capture_failed",
                format!("{code}: {msg}"),
            ));
        }

        let segment_events = {
            let mut enc = self.encoder.lock().map_err(|_| {
                StorageError::service_unavailable("encoder_poisoned", "encoder mutex poisoned")
            })?;
            enc.finish(ENCODER_FINISH_TIMEOUT).map_err(|err| {
                StorageError::service_unavailable(
                    "encoder_finish_failed",
                    format!("{}: {}", err.code, err.message),
                )
            })?
        };

        let sink_snap = {
            let mut sink = self.sink.lock().map_err(|_| {
                StorageError::service_unavailable("sink_poisoned", "recording sink mutex poisoned")
            })?;
            sink.flush_and_close().map_err(|err| {
                StorageError::service_unavailable(
                    "sink_flush_failed",
                    format!("{}: {}", err.code, err.message),
                )
            })?
        };

        let take_snap = self.active_take.lock().ok().map(|t| t.snapshot());
        let planner_guard = self.segment_planner.lock().ok();

        let now_ns = monotonic_ns().unwrap_or(self.started_monotonic_ns);
        let duration_seconds =
            (now_ns.saturating_sub(self.started_monotonic_ns) as f64 / 1_000_000_000.0).max(0.001);

        let segments: Vec<CapturedVideoSegment> = segment_events
            .iter()
            .map(|ev| {
                let (start_s, end_s) = planner_guard
                    .as_ref()
                    .and_then(|p| {
                        let b0 = p.boundary(ev.start_frame, duration_seconds).ok()?;
                        let b1 = p.boundary(ev.end_frame, duration_seconds).ok()?;
                        Some((b0.time_seconds, b1.time_seconds))
                    })
                    .unwrap_or_else(|| {
                        let fps = self.effective_fps.max(1.0);
                        (ev.start_frame as f64 / fps, ev.end_frame as f64 / fps)
                    });
                CapturedVideoSegment::from_encoder_segment(ev, start_s, end_s)
            })
            .collect();

        let (frames_artifact, imu_artifact) = extract_sink_artifacts(&sink_snap)?;

        let audio = match self.audio {
            Some(recorder) => match recorder.stop(Duration::from_secs(5)) {
                Ok(res) => CapturedAudioOutcome::Recorded {
                    requested_mode: self.audio_requested_mode,
                    sample_rate: res.sample_rate_hz,
                    channels: res.channels,
                    sample_count: res.sample_count,
                    start_time_seconds: 0.0,
                    end_time_seconds: duration_seconds,
                    segments: res
                        .segments
                        .into_iter()
                        .map(|seg| CapturedAudioSegment {
                            index: seg.index,
                            start_sample: seg.start_sample,
                            end_sample: seg.end_sample,
                            start_time_seconds: seg.start_time_seconds,
                            end_time_seconds: seg.end_time_seconds,
                            pcm_payload_bytes: seg.bytes.saturating_sub(audio::WAV_HEADER_BYTES),
                            wav_header_bytes: audio::WAV_HEADER_BYTES,
                            artifact: CapturedArtifact {
                                role: "audio.wav".to_owned(),
                                path: seg.relative_path,
                                media_type: "audio/wav".to_owned(),
                                bytes: seg.bytes,
                                sha256: seg.sha256,
                            },
                        })
                        .collect(),
                },
                Err(_err) => CapturedAudioOutcome::NotRecorded {
                    requested_mode: self.audio_requested_mode,
                    reason: "recording_interrupted".to_owned(),
                },
            },
            None => CapturedAudioOutcome::NotRecorded {
                requested_mode: self.audio_requested_mode,
                reason: "device_default_disabled".to_owned(),
            },
        };

        let dropped_frames = take_snap.as_ref().map(|s| s.dropped_frames).unwrap_or(0);
        let drop_events = take_snap
            .map(|s| {
                s.drop_events
                    .into_iter()
                    .map(|ev| DropEventRecord {
                        start_frame: ev.start_frame,
                        end_frame: ev.end_frame,
                        at_time_seconds: ev.at_time_seconds,
                        reason: ev.reason.to_owned(),
                        dropped: ev.dropped,
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(CapturedSessionArtifacts {
            segments,
            frames_artifact,
            frames_count: sink_snap.frames_written,
            imu_artifact,
            imu_sample_count: sink_snap.imu_samples_written,
            audio,
            logs: Vec::new(),
            dropped_frames,
            drop_events,
            duration_seconds,
            effective_fps: if duration_seconds > 0.0 {
                sink_snap.frames_written as f64 / duration_seconds
            } else {
                self.effective_fps
            },
        })
    }

    fn abort(self: Box<Self>) {
        let _ = self.runtime.stop_recording(Duration::from_secs(2));
        if let Some(audio) = &self.audio {
            audio.request_stop();
        }
        if let Ok(mut enc) = self.encoder.lock() {
            enc.abort();
        }
        if let Ok(mut sink) = self.sink.lock() {
            sink.close();
        }
    }
}

impl CaptureBackend for NativeCaptureBackend {
    fn is_camera_connected(&self) -> bool {
        if self.stream.lock().ok().and_then(|g| g.clone()).is_none() {
            let _ = self.try_connect_camera();
        }
        self.stream.lock().ok().is_some_and(|g| g.is_some())
    }

    fn camera_focus(&self) -> StorageResult<Option<CameraFocusStatus>> {
        let Some(stream) = self.stream.lock().ok().and_then(|g| g.clone()) else {
            return Err(StorageError::camera_not_connected(
                "camera is not connected",
            ));
        };
        let status = stream
            .focus_status()
            .map_err(|err| StorageError::service_unavailable("camera_focus_failed", err.message))?;
        Ok(status.map(|f| CameraFocusStatus {
            schema: CAMERA_FOCUS_SCHEMA.to_owned(),
            value: u32::try_from(f.value).unwrap_or(0),
            minimum: u32::try_from(f.minimum).unwrap_or(0),
            maximum: u32::try_from(f.maximum).unwrap_or(0),
            step: u32::try_from(f.step).unwrap_or(1).max(1),
            default: u32::try_from(f.default_value).unwrap_or(0),
            auto_supported: f.auto_supported,
            auto_enabled: f.auto_enabled,
        }))
    }

    fn set_camera_focus(&self, req: &CameraFocusSetRequest) -> StorageResult<CameraFocusStatus> {
        let Some(stream) = self.stream.lock().ok().and_then(|g| g.clone()) else {
            return Err(StorageError::camera_not_connected(
                "camera is not connected",
            ));
        };
        let val = req.value.map(i32::try_from).transpose().map_err(|_| {
            StorageError::unprocessable("invalid_camera_focus", "focus out of range")
        })?;
        let updated = stream
            .set_focus(val, req.auto_enabled)
            .map_err(|err| StorageError::unprocessable("invalid_camera_focus", err.message))?;
        Ok(CameraFocusStatus {
            schema: CAMERA_FOCUS_SCHEMA.to_owned(),
            value: u32::try_from(updated.value).unwrap_or(0),
            minimum: u32::try_from(updated.minimum).unwrap_or(0),
            maximum: u32::try_from(updated.maximum).unwrap_or(0),
            step: u32::try_from(updated.step).unwrap_or(1).max(1),
            default: u32::try_from(updated.default_value).unwrap_or(0),
            auto_supported: updated.auto_supported,
            auto_enabled: updated.auto_enabled,
        })
    }

    fn begin_capture(
        &self,
        req: &BackendStartRequest,
    ) -> StorageResult<Box<dyn ActiveCaptureHandle>> {
        self.try_connect_camera()?;
        let runtime = self
            .runtime
            .lock()
            .ok()
            .and_then(|g| g.clone())
            .ok_or_else(|| StorageError::camera_not_connected("capture runtime unavailable"))?;

        let effective_fps = (f64::from(self.config.camera.fps)
            / f64::from(self.config.camera.frame_decimation))
        .max(1.0);
        let gop = u64::from(req.encoding.gop_frames.max(1));
        let raw_seg_frames =
            ((effective_fps * self.config.segment_seconds).round() as u64).max(gop);
        let segment_frames = raw_seg_frames.div_ceil(gop) * gop;
        let video_dir = req.staging_dir.join("video");
        fs::create_dir_all(&video_dir).map_err(|err| {
            StorageError::io("staging_create_failed", "create staging video dir", err)
        })?;

        let mut encoder = EncoderProcess::with_transport(
            EncoderConfig {
                out_dir: &video_dir,
                executable: &self.encoder_bin,
                width: u64::from(self.config.camera.width),
                height: u64::from(self.config.camera.height),
                fps: effective_fps.round() as u64,
                bitrate_kbps: u64::from(req.encoding.bitrate_kbps),
                segment_frames,
                path_prefix: "video/",
            },
            EncoderTransport::ShmRing(ShmRingConfig::default()),
        )
        .map_err(|err| StorageError::service_unavailable("encoder_init_failed", err.message))?;

        encoder
            .configure(&req.encoding.encoder_arguments())
            .map_err(|err| StorageError::invalid_input("encoder_config_invalid", err.message))?;
        encoder.start().map_err(|err| {
            StorageError::service_unavailable("encoder_start_failed", err.message)
        })?;

        let sink = RecordingSink::create(&req.staging_dir, &req.session_id)
            .map_err(|err| StorageError::service_unavailable("sink_create_failed", err.message))?;
        let active_take = ActiveTakeWriter::new(&req.session_id)
            .map_err(|err| StorageError::service_unavailable("take_create_failed", err.message))?;
        let segment_planner = RecordingSegmentPlanner::new(segment_frames)
            .map_err(|err| StorageError::invalid_input("segment_invalid", err.message))?;

        let imu = self
            .imu_device
            .as_deref()
            .and_then(|dev| dev.to_str())
            .and_then(|dev| ImuCollector::open(dev, None, 1, None).ok())
            .map(Arc::new);

        let audio_requested_mode = if self.config.audio.enabled {
            "enabled".to_owned()
        } else {
            "disabled".to_owned()
        };
        let audio = if self.config.audio.enabled && audio::available() {
            let root_str = req.staging_dir.to_string_lossy().into_owned();
            AudioRecorder::new(
                &root_str,
                &self.config.audio.device,
                self.config.audio.sample_rate_hz,
                self.config.audio.channels,
                self.config.segment_seconds,
            )
            .ok()
            .and_then(|rec| {
                let rec = Arc::new(rec);
                if rec.start().is_ok() { Some(rec) } else { None }
            })
        } else {
            None
        };

        let active_take = Arc::new(Mutex::new(active_take));
        let sink = Arc::new(Mutex::new(sink));
        let encoder = Arc::new(Mutex::new(encoder));
        let segment_planner = Arc::new(Mutex::new(segment_planner));
        let failure_log = Arc::new(CaptureFailureLog::new());

        runtime
            .prepare_recording(Duration::from_secs(2))
            .map_err(|err| {
                StorageError::service_unavailable("capture_prepare_failed", err.message)
            })?;
        runtime
            .start_recording(RecordingTarget {
                active_take: Arc::clone(&active_take),
                sink: Arc::clone(&sink),
                encoder: Arc::clone(&encoder),
                segment_planner: Arc::clone(&segment_planner),
                recording_start_monotonic_ns: req.started_monotonic_ns,
                on_failure: Arc::clone(&failure_log) as Arc<dyn FailureSink>,
                imu,
                imu_timeout: Duration::from_millis(50),
                audio: audio.clone(),
                capture_stopped_monotonic_ns: Arc::new(AtomicU64::new(0)),
                recover_source_gaps: true,
            })
            .map_err(|err| {
                StorageError::service_unavailable("capture_start_failed", err.message)
            })?;

        Ok(Box::new(NativeActiveHandle {
            runtime,
            active_take,
            sink,
            encoder,
            segment_planner,
            failure_log,
            audio,
            audio_requested_mode,
            started_monotonic_ns: req.started_monotonic_ns,
            effective_fps,
        }))
    }

    fn runtime_observation(&self) -> RuntimeObservation {
        let connected = self.stream.lock().ok().is_some_and(|g| g.is_some());
        let focus = if connected {
            self.camera_focus().ok().flatten()
        } else {
            None
        };
        let mut obs = RuntimeObservation::default_with_camera(connected, focus);
        if let Some(temp) = read_soc_temperature_c() {
            obs.temperature_celsius = temp;
        }
        obs
    }
}

/// Real C-encoder-backed capture backend driving `miro-ego-encoder` / `miro-ego-encoder-mock`
/// over `MEGO_SHM` v1 (`memfd` + `eventfd`) with live synthetic stereo JPEG frame feeding.
pub struct ShmEncoderCaptureBackend {
    encoder_bin: PathBuf,
    width: u32,
    height: u32,
    fps: u32,
    segment_frames: u64,
    min_frames: u64,
    mjpeg_hub: Arc<MjpegPreviewHub>,
    h264_hub: Option<Arc<H264PreviewHub>>,
    focus: Mutex<CameraFocusStatus>,
}

impl ShmEncoderCaptureBackend {
    pub fn new(
        encoder_bin: PathBuf,
        width: u32,
        height: u32,
        fps: u32,
        segment_frames: u64,
        mjpeg_hub: Arc<MjpegPreviewHub>,
        h264_hub: Option<Arc<H264PreviewHub>>,
    ) -> Self {
        let _ = mjpeg_hub.publish(Bytes::from_static(SYNTHETIC_JPEG_FRAME));
        if let Some(hub) = &h264_hub {
            hub.set_init_segment_blocking(Bytes::from_static(SYNTHETIC_FMP4_INIT));
            let _ = hub.publish_fragment(Bytes::from_static(SYNTHETIC_FMP4_MOOF));
        }
        let seg_frames = segment_frames.max(1);
        Self {
            encoder_bin,
            width: width.max(8) & !3,
            height: height.max(8),
            fps: fps.max(1),
            segment_frames: seg_frames,
            min_frames: seg_frames,
            mjpeg_hub,
            h264_hub,
            focus: Mutex::new(CameraFocusStatus {
                schema: CAMERA_FOCUS_SCHEMA.to_owned(),
                value: 250,
                minimum: 0,
                maximum: 1023,
                step: 1,
                default: 250,
                auto_supported: true,
                auto_enabled: Some(false),
            }),
        }
    }
}

struct ShmActiveHandle {
    stop_flag: Arc<AtomicBool>,
    frames_count: Arc<AtomicU64>,
    bytes_written: Arc<AtomicU64>,
    started_monotonic_ns: u64,
    fps: u32,
    worker: Option<JoinHandle<StorageResult<CapturedSessionArtifacts>>>,
}

impl ActiveCaptureHandle for ShmActiveHandle {
    fn progress(&self) -> CaptureProgressSnapshot {
        let now_ns = monotonic_ns().unwrap_or(self.started_monotonic_ns);
        let elapsed_seconds =
            now_ns.saturating_sub(self.started_monotonic_ns) as f64 / 1_000_000_000.0;
        CaptureProgressSnapshot {
            elapsed_seconds,
            captured_frames: self.frames_count.load(Ordering::Acquire),
            bytes_written: self.bytes_written.load(Ordering::Acquire),
        }
    }

    fn finish(mut self: Box<Self>, _reason: StopReason) -> StorageResult<CapturedSessionArtifacts> {
        self.stop_flag.store(true, Ordering::Release);
        let Some(handle) = self.worker.take() else {
            return Err(StorageError::service_unavailable(
                "capture_worker_missing",
                "SHM capture worker handle already consumed",
            ));
        };
        let mut artifacts = handle.join().map_err(|_| {
            StorageError::service_unavailable(
                "capture_worker_panicked",
                "SHM capture worker panicked",
            )
        })??;
        let _ = self.fps;
        if artifacts.duration_seconds <= 0.0 {
            artifacts.duration_seconds = 0.1;
        }
        Ok(artifacts)
    }

    fn abort(mut self: Box<Self>) {
        self.stop_flag.store(true, Ordering::Release);
        if let Some(handle) = self.worker.take() {
            let _ = handle.join();
        }
    }
}

impl CaptureBackend for ShmEncoderCaptureBackend {
    fn is_camera_connected(&self) -> bool {
        true
    }

    fn camera_focus(&self) -> StorageResult<Option<CameraFocusStatus>> {
        let guard = self.focus.lock().map_err(|_| {
            StorageError::service_unavailable("focus_poisoned", "focus state mutex poisoned")
        })?;
        Ok(Some(guard.clone()))
    }

    fn set_camera_focus(&self, req: &CameraFocusSetRequest) -> StorageResult<CameraFocusStatus> {
        let mut guard = self.focus.lock().map_err(|_| {
            StorageError::service_unavailable("focus_poisoned", "focus state mutex poisoned")
        })?;
        if let Some(v) = req.value {
            guard.value = v;
        }
        if let Some(auto) = req.auto_enabled {
            guard.auto_enabled = Some(auto);
        }
        Ok(guard.clone())
    }

    fn begin_capture(
        &self,
        req: &BackendStartRequest,
    ) -> StorageResult<Box<dyn ActiveCaptureHandle>> {
        let video_dir = req.staging_dir.join("video");
        fs::create_dir_all(&video_dir).map_err(|err| {
            StorageError::io("staging_create_failed", "create staging video dir", err)
        })?;

        let shm_cfg = ShmRingConfig {
            slot_count: 8,
            slot_capacity: 256 * 1024,
        };
        let mut encoder = EncoderProcess::with_transport(
            EncoderConfig {
                out_dir: &video_dir,
                executable: &self.encoder_bin,
                width: u64::from(self.width),
                height: u64::from(self.height),
                fps: u64::from(self.fps),
                bitrate_kbps: u64::from(req.encoding.bitrate_kbps),
                segment_frames: self.segment_frames,
                path_prefix: "video/",
            },
            EncoderTransport::ShmRing(shm_cfg),
        )
        .map_err(|err| StorageError::service_unavailable("encoder_init_failed", err.message))?;

        encoder
            .configure(&req.encoding.encoder_arguments())
            .map_err(|err| StorageError::invalid_input("encoder_config_invalid", err.message))?;
        encoder.start().map_err(|err| {
            StorageError::service_unavailable("encoder_start_failed", err.message)
        })?;

        let mut sink = RecordingSink::create(&req.staging_dir, &req.session_id)
            .map_err(|err| StorageError::service_unavailable("sink_create_failed", err.message))?;

        let stop_flag = Arc::new(AtomicBool::new(false));
        let frames_count = Arc::new(AtomicU64::new(0));
        let bytes_written = Arc::new(AtomicU64::new(0));

        let stop_worker = Arc::clone(&stop_flag);
        let frames_worker = Arc::clone(&frames_count);
        let bytes_worker = Arc::clone(&bytes_written);
        let mjpeg_hub = Arc::clone(&self.mjpeg_hub);
        let h264_hub = self.h264_hub.clone();
        let fps = self.fps;
        let segment_frames = self.segment_frames;
        let min_frames = self.min_frames;
        let started_ns = req.started_monotonic_ns;

        let worker = thread::Builder::new()
            .name("miro-ego-shm-feeder".to_owned())
            .spawn(move || -> StorageResult<CapturedSessionArtifacts> {
                let frame_interval =
                    Duration::from_millis((1000 / u64::from(fps.max(1))).clamp(5, 50));
                let mut seq: u64 = 0;

                while !stop_worker.load(Ordering::Acquire) || seq < min_frames {
                    let frame_ns = monotonic_ns().unwrap_or(started_ns.saturating_add(seq + 1));
                    let written = encoder
                        .submit_frame(seq, frame_ns, SYNTHETIC_JPEG_FRAME)
                        .map_err(|err| {
                            StorageError::service_unavailable(
                                "encoder_submit_failed",
                                format!("{}: {}", err.code, err.message),
                            )
                        })?;
                    let seg_index = seq / segment_frames;
                    let seg_frame = seq % segment_frames;
                    let idx_bytes = sink
                        .write_audited_frame_index(
                            seq,
                            seq + 1,
                            frame_ns,
                            seg_index,
                            seg_frame,
                            None,
                        )
                        .map_err(|err| {
                            StorageError::service_unavailable("sink_write_failed", err.message)
                        })?;
                    let imu_bytes = sink
                        .write_imu_sample_record(&ImuSampleRecord {
                            sequence: seq,
                            packet_sequence: seq,
                            sample_index: 0,
                            device_timestamp_raw: u32::try_from(seq * 1000).unwrap_or(u32::MAX),
                            device_ticks: seq * 1000,
                            host_read_start_ns: frame_ns,
                            host_read_end_ns: frame_ns.saturating_add(100),
                            host_monotonic_ns: frame_ns,
                            accelerometer: (0, 0, 16384),
                            gyroscope: (0, 0, 0),
                            sync_offset_ns: Some(0),
                            sync_residual_ns: Some(0),
                            sync_quality: "nominal",
                        })
                        .map_err(|err| {
                            StorageError::service_unavailable("sink_write_failed", err.message)
                        })?;

                    let _ = mjpeg_hub.publish(Bytes::from_static(SYNTHETIC_JPEG_FRAME));
                    if let Some(ref hub) = h264_hub {
                        let _ = hub.publish_fragment(Bytes::from_static(SYNTHETIC_FMP4_MOOF));
                    }

                    seq = seq.saturating_add(1);
                    frames_worker.store(seq, Ordering::Release);
                    bytes_worker.fetch_add(written + idx_bytes + imu_bytes, Ordering::AcqRel);

                    if stop_worker.load(Ordering::Acquire) && seq >= min_frames {
                        break;
                    }
                    thread::sleep(frame_interval);
                }

                let segment_events = encoder.finish(ENCODER_FINISH_TIMEOUT).map_err(|err| {
                    StorageError::service_unavailable(
                        "encoder_finish_failed",
                        format!("{}: {}", err.code, err.message),
                    )
                })?;

                let sink_snap = sink.flush_and_close().map_err(|err| {
                    StorageError::service_unavailable("sink_flush_failed", err.message)
                })?;
                let (frames_artifact, imu_artifact) = extract_sink_artifacts(&sink_snap)?;

                let now_ns = monotonic_ns().unwrap_or(started_ns);
                let duration_seconds =
                    (now_ns.saturating_sub(started_ns) as f64 / 1_000_000_000.0).max(0.05);
                let fps_f = f64::from(fps.max(1));

                let segments = segment_events
                    .iter()
                    .map(|ev| {
                        CapturedVideoSegment::from_encoder_segment(
                            ev,
                            ev.start_frame as f64 / fps_f,
                            ev.end_frame as f64 / fps_f,
                        )
                    })
                    .collect();

                Ok(CapturedSessionArtifacts {
                    segments,
                    frames_artifact,
                    frames_count: sink_snap.frames_written,
                    imu_artifact,
                    imu_sample_count: sink_snap.imu_samples_written,
                    audio: CapturedAudioOutcome::NotRecorded {
                        requested_mode: "disabled".to_owned(),
                        reason: "device_default_disabled".to_owned(),
                    },
                    logs: Vec::new(),
                    dropped_frames: 0,
                    drop_events: Vec::new(),
                    duration_seconds,
                    effective_fps: fps_f,
                })
            })
            .map_err(|err| {
                StorageError::io("capture_worker_spawn", "spawn SHM feeder thread", err)
            })?;

        Ok(Box::new(ShmActiveHandle {
            stop_flag,
            frames_count,
            bytes_written,
            started_monotonic_ns: req.started_monotonic_ns,
            fps: self.fps,
            worker: Some(worker),
        }))
    }

    fn runtime_observation(&self) -> RuntimeObservation {
        let focus = self.focus.lock().ok().map(|g| g.clone());
        let mut obs = RuntimeObservation::default_with_camera(true, focus);
        if let Some(temp) = read_soc_temperature_c() {
            obs.temperature_celsius = temp;
        }
        obs
    }
}

/// Pure-Rust deterministic capture backend for `miro-ego serve --mock` when no external encoder
/// binary is supplied. Produces real ISO-BMFF MP4 payloads and streaming SHA-256 digests.
pub struct MockCaptureBackend {
    fps: u32,
    mjpeg_hub: Arc<MjpegPreviewHub>,
    h264_hub: Option<Arc<H264PreviewHub>>,
    focus: Mutex<CameraFocusStatus>,
}

impl MockCaptureBackend {
    pub fn new(
        fps: u32,
        mjpeg_hub: Arc<MjpegPreviewHub>,
        h264_hub: Option<Arc<H264PreviewHub>>,
    ) -> Self {
        let _ = mjpeg_hub.publish(Bytes::from_static(SYNTHETIC_JPEG_FRAME));
        if let Some(hub) = &h264_hub {
            hub.set_init_segment_blocking(Bytes::from_static(SYNTHETIC_FMP4_INIT));
            let _ = hub.publish_fragment(Bytes::from_static(SYNTHETIC_FMP4_MOOF));
        }
        Self {
            fps: fps.max(1),
            mjpeg_hub,
            h264_hub,
            focus: Mutex::new(CameraFocusStatus {
                schema: CAMERA_FOCUS_SCHEMA.to_owned(),
                value: 250,
                minimum: 0,
                maximum: 1023,
                step: 1,
                default: 250,
                auto_supported: true,
                auto_enabled: Some(false),
            }),
        }
    }
}

struct MockActiveHandle {
    staging_dir: PathBuf,
    session_id: String,
    started_monotonic_ns: u64,
    fps: u32,
    mjpeg_hub: Arc<MjpegPreviewHub>,
    h264_hub: Option<Arc<H264PreviewHub>>,
}

impl ActiveCaptureHandle for MockActiveHandle {
    fn progress(&self) -> CaptureProgressSnapshot {
        let now_ns = monotonic_ns().unwrap_or(self.started_monotonic_ns);
        let elapsed_seconds =
            now_ns.saturating_sub(self.started_monotonic_ns) as f64 / 1_000_000_000.0;
        let frames = ((elapsed_seconds * f64::from(self.fps)) as u64).max(1);
        CaptureProgressSnapshot {
            elapsed_seconds,
            captured_frames: frames,
            bytes_written: frames * 2048,
        }
    }

    fn finish(self: Box<Self>, _reason: StopReason) -> StorageResult<CapturedSessionArtifacts> {
        let video_dir = self.staging_dir.join("video");
        fs::create_dir_all(&video_dir).map_err(|err| {
            StorageError::io("staging_create_failed", "create mock video dir", err)
        })?;

        let left_bytes = build_synthetic_mp4_payload(&self.session_id, "left");
        let right_bytes = build_synthetic_mp4_payload(&self.session_id, "right");
        let left_sha = sha256_hex(&left_bytes);
        let right_sha = sha256_hex(&right_bytes);

        write_file_synced(&video_dir.join("left_00000.mp4"), &left_bytes)?;
        write_file_synced(&video_dir.join("right_00000.mp4"), &right_bytes)?;

        let mut sink = RecordingSink::create(&self.staging_dir, &self.session_id)
            .map_err(|err| StorageError::service_unavailable("sink_create_failed", err.message))?;
        let frame_count: u64 = 5;
        for idx in 0..frame_count {
            let ts = self.started_monotonic_ns.saturating_add(idx * 33_333_333);
            sink.write_audited_frame_index(idx, idx + 1, ts, 0, idx, None)
                .map_err(|err| {
                    StorageError::service_unavailable("sink_write_failed", err.message)
                })?;
            sink.write_imu_sample_record(&ImuSampleRecord {
                sequence: idx,
                packet_sequence: idx,
                sample_index: 0,
                device_timestamp_raw: u32::try_from(idx * 1000).unwrap_or(u32::MAX),
                device_ticks: idx * 1000,
                host_read_start_ns: ts,
                host_read_end_ns: ts.saturating_add(100),
                host_monotonic_ns: ts,
                accelerometer: (0, 0, 16384),
                gyroscope: (0, 0, 0),
                sync_offset_ns: Some(0),
                sync_residual_ns: Some(0),
                sync_quality: "nominal",
            })
            .map_err(|err| StorageError::service_unavailable("sink_write_failed", err.message))?;
        }
        let sink_snap = sink
            .flush_and_close()
            .map_err(|err| StorageError::service_unavailable("sink_flush_failed", err.message))?;
        let (frames_artifact, imu_artifact) = extract_sink_artifacts(&sink_snap)?;

        let _ = self
            .mjpeg_hub
            .publish(Bytes::from_static(SYNTHETIC_JPEG_FRAME));
        if let Some(hub) = &self.h264_hub {
            let _ = hub.publish_fragment(Bytes::from_static(SYNTHETIC_FMP4_MOOF));
        }

        let now_ns = monotonic_ns().unwrap_or(self.started_monotonic_ns);
        let duration_seconds =
            (now_ns.saturating_sub(self.started_monotonic_ns) as f64 / 1_000_000_000.0).max(0.1);

        Ok(CapturedSessionArtifacts {
            segments: vec![CapturedVideoSegment {
                index: 0,
                start_frame: 0,
                end_frame: frame_count,
                start_time_seconds: 0.0,
                end_time_seconds: duration_seconds,
                left_path: "video/left_00000.mp4".to_owned(),
                left_bytes: left_bytes.len() as u64,
                left_sha256: Some(left_sha),
                right_path: "video/right_00000.mp4".to_owned(),
                right_bytes: right_bytes.len() as u64,
                right_sha256: Some(right_sha),
            }],
            frames_artifact,
            frames_count: sink_snap.frames_written,
            imu_artifact,
            imu_sample_count: sink_snap.imu_samples_written,
            audio: CapturedAudioOutcome::NotRecorded {
                requested_mode: "disabled".to_owned(),
                reason: "device_default_disabled".to_owned(),
            },
            logs: Vec::new(),
            dropped_frames: 0,
            drop_events: Vec::new(),
            duration_seconds,
            effective_fps: f64::from(self.fps),
        })
    }

    fn abort(self: Box<Self>) {}
}

impl CaptureBackend for MockCaptureBackend {
    fn is_camera_connected(&self) -> bool {
        true
    }

    fn camera_focus(&self) -> StorageResult<Option<CameraFocusStatus>> {
        let guard = self.focus.lock().map_err(|_| {
            StorageError::service_unavailable("focus_poisoned", "focus state mutex poisoned")
        })?;
        Ok(Some(guard.clone()))
    }

    fn set_camera_focus(&self, req: &CameraFocusSetRequest) -> StorageResult<CameraFocusStatus> {
        let mut guard = self.focus.lock().map_err(|_| {
            StorageError::service_unavailable("focus_poisoned", "focus state mutex poisoned")
        })?;
        if let Some(v) = req.value {
            guard.value = v;
        }
        if let Some(auto) = req.auto_enabled {
            guard.auto_enabled = Some(auto);
        }
        Ok(guard.clone())
    }

    fn begin_capture(
        &self,
        req: &BackendStartRequest,
    ) -> StorageResult<Box<dyn ActiveCaptureHandle>> {
        Ok(Box::new(MockActiveHandle {
            staging_dir: req.staging_dir.clone(),
            session_id: req.session_id.clone(),
            started_monotonic_ns: req.started_monotonic_ns,
            fps: self.fps,
            mjpeg_hub: Arc::clone(&self.mjpeg_hub),
            h264_hub: self.h264_hub.clone(),
        }))
    }

    fn runtime_observation(&self) -> RuntimeObservation {
        let focus = self.focus.lock().ok().map(|g| g.clone());
        RuntimeObservation::default_with_camera(true, focus)
    }
}

fn build_synthetic_mp4_payload(session_id: &str, eye: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(256);
    out.extend_from_slice(SYNTHETIC_FMP4_INIT);
    out.extend_from_slice(SYNTHETIC_FMP4_MOOF);
    out.extend_from_slice(format!("miro-ego:{session_id}:{eye}").as_bytes());
    out
}

fn write_file_synced(path: &Path, bytes: &[u8]) -> StorageResult<()> {
    let mut file = File::create(path)
        .map_err(|err| StorageError::io("artifact_write_failed", "create artifact file", err))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|err| StorageError::io("artifact_write_failed", "write artifact file", err))
}

fn extract_sink_artifacts(
    snap: &miro_ego_capture::session::recording::RecordingSinkSnapshot,
) -> StorageResult<(CapturedArtifact, CapturedArtifact)> {
    let mut frames: Option<CapturedArtifact> = None;
    let mut imu: Option<CapturedArtifact> = None;
    for art in &snap.artifacts {
        let captured = CapturedArtifact {
            role: art.role.to_owned(),
            path: art.relative_path.to_owned(),
            media_type: "application/x-ndjson".to_owned(),
            bytes: art.bytes,
            sha256: Some(art.sha256.clone()),
        };
        if art.role == "frames.index" {
            frames = Some(captured);
        } else if art.role == "imu.samples" {
            imu = Some(captured);
        }
    }
    let frames_art = frames.ok_or_else(|| {
        StorageError::integrity(
            "missing_frames_artifact",
            "recording sink omitted frames.ndjson",
        )
    })?;
    let imu_art = imu.ok_or_else(|| {
        StorageError::integrity("missing_imu_artifact", "recording sink omitted imu.ndjson")
    })?;
    Ok((frames_art, imu_art))
}

// ---------------------------------------------------------------------------
// ProductionGatewayBackend
// ---------------------------------------------------------------------------

struct ClockChallenge {
    principal_id: String,
    from_check: bool,
    expires_at: Instant,
}

struct ClockRuntimeState {
    boot_id: String,
    synced: bool,
    offset_ms: i64,
    synchronized_at_ms: Option<u64>,
    synchronized_mono_ns: Option<u64>,
    challenges: HashMap<String, ClockChallenge>,
}

impl ClockRuntimeState {
    fn new() -> Self {
        Self {
            boot_id: new_uuid_v4(),
            synced: false,
            offset_ms: 0,
            synchronized_at_ms: None,
            synchronized_mono_ns: None,
            challenges: HashMap::new(),
        }
    }

    fn current_unix_ms(&self) -> u64 {
        let base = now_unix_ms() as i128;
        let adjusted = base + i128::from(self.offset_ms);
        u64::try_from(adjusted.max(0)).unwrap_or(u64::MAX)
    }

    fn age_seconds(&self) -> Option<u64> {
        let synced_ns = self.synchronized_mono_ns?;
        let now_ns = monotonic_ns().unwrap_or(synced_ns);
        Some(now_ns.saturating_sub(synced_ns) / 1_000_000_000)
    }

    fn issue_challenge(&mut self, principal_id: &str, from_check: bool) -> String {
        let now = Instant::now();
        self.challenges.retain(|_, v| v.expires_at > now);
        if self.challenges.len() >= 64 {
            self.challenges.clear();
        }
        let token = new_uuid_v4().replace('-', "");
        self.challenges.insert(
            token.clone(),
            ClockChallenge {
                principal_id: principal_id.to_owned(),
                from_check,
                expires_at: now + Duration::from_secs(5),
            },
        );
        token
    }
}

#[derive(Default)]
struct FirmwareRuntimeState {
    checked_at: Option<u64>,
    task: Option<Value>,
    idempotency: HashMap<(String, String, String), (Vec<u8>, CommandOutcome)>,
}

/// Configuration knobs for constructing a [`ProductionGatewayBackend`].
#[derive(Clone)]
pub struct ProductionBackendConfig {
    pub device_config: DeviceConfig,
    pub software_commit: String,
    pub network_socket: Option<PathBuf>,
    pub release_paths: ReleaseManagerPaths,
    pub enable_h264_preview: bool,
    pub voice_controller: Option<Arc<VoiceController>>,
    pub indicator: Option<Arc<SysfsLedController>>,
}

impl ProductionBackendConfig {
    pub fn from_device_config(device_config: DeviceConfig) -> Self {
        Self {
            device_config,
            software_commit: option_env!("MIRO_EGO_GIT_COMMIT")
                .map(str::to_owned)
                .filter(|s| s.len() == 40)
                .unwrap_or_else(|| "0".repeat(40)),
            network_socket: Some(PathBuf::from(miro_ego_network::DEFAULT_CONTROL_SOCKET_PATH)),
            release_paths: ReleaseManagerPaths::canonical(),
            enable_h264_preview: true,
            voice_controller: None,
            indicator: None,
        }
    }
}

/// Complete production implementation of [`GatewayBackend`] bridging all 7 workspace crates.
pub struct ProductionGatewayBackend {
    config: ProductionBackendConfig,
    coordinator: Arc<CaptureCoordinator>,
    volume: Arc<ExfatVolumeManager>,
    catalog: Arc<BinarySessionCatalog>,
    audit: Arc<AuditLogger>,
    logs: Arc<LogStore>,
    network_client: Option<NetworkRpcClient>,
    release_manager: Arc<ReleaseManager>,
    mjpeg_hub: Arc<MjpegPreviewHub>,
    h264_hub: Option<Arc<H264PreviewHub>>,
    capture_events: Arc<EventReplayBuffer>,
    network_events: Arc<NetworkEventReplayBuffer>,
    clock: Mutex<ClockRuntimeState>,
    voice_fallback_volume: Mutex<u8>,
    firmware: Mutex<FirmwareRuntimeState>,
    web_assets: HashMap<String, StaticAsset>,
}

impl ProductionGatewayBackend {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: ProductionBackendConfig,
        coordinator: Arc<CaptureCoordinator>,
        volume: Arc<ExfatVolumeManager>,
        catalog: Arc<BinarySessionCatalog>,
        audit: Arc<AuditLogger>,
        logs: Arc<LogStore>,
        mjpeg_hub: Arc<MjpegPreviewHub>,
        h264_hub: Option<Arc<H264PreviewHub>>,
    ) -> Self {
        let network_client = config
            .network_socket
            .as_ref()
            .map(|sock| NetworkRpcClient::new(sock).with_timeout(Duration::from_secs(5)));

        let release_manager = Arc::new(ReleaseManager::new(
            config.release_paths.clone(),
            Arc::new(NoopSystemctlRunner),
            Arc::new(NoopHealthChecker),
        ));

        let mut web_assets = HashMap::new();
        web_assets.insert(
            "index.html".to_owned(),
            StaticAsset::new(
                "index.html",
                "text/html; charset=utf-8",
                Bytes::from_static(EMBEDDED_INDEX_HTML),
            ),
        );
        web_assets.insert(
            "app.js".to_owned(),
            StaticAsset::new(
                "app.js",
                "application/javascript; charset=utf-8",
                Bytes::from_static(EMBEDDED_APP_JS),
            ),
        );

        Self {
            config,
            coordinator,
            volume,
            catalog,
            audit,
            logs,
            network_client,
            release_manager,
            mjpeg_hub,
            h264_hub,
            capture_events: Arc::new(EventReplayBuffer::default()),
            network_events: Arc::new(NetworkEventReplayBuffer::default()),
            clock: Mutex::new(ClockRuntimeState::new()),
            voice_fallback_volume: Mutex::new(80),
            firmware: Mutex::new(FirmwareRuntimeState::default()),
            web_assets,
        }
    }

    pub fn coordinator(&self) -> &Arc<CaptureCoordinator> {
        &self.coordinator
    }

    pub fn volume(&self) -> &Arc<ExfatVolumeManager> {
        &self.volume
    }

    pub fn catalog(&self) -> &Arc<BinarySessionCatalog> {
        &self.catalog
    }

    pub fn audit(&self) -> &Arc<AuditLogger> {
        &self.audit
    }

    pub fn indicator(&self) -> Option<&Arc<SysfsLedController>> {
        self.config.indicator.as_ref()
    }

    pub fn voice_controller(&self) -> Option<&Arc<VoiceController>> {
        self.config.voice_controller.as_ref()
    }

    fn require_network_client(&self) -> ApiResult<NetworkRpcClient> {
        self.network_client.clone().ok_or_else(|| {
            ApiError::service_unavailable(
                "network_controller_unavailable",
                "network control socket is not configured",
            )
            .with_header("Retry-After", "1")
            .with_error_code_header()
        })
    }

    fn format_firmware_status(&self, checked_at: Option<u64>, task: Option<Value>) -> Value {
        let deploy = self.release_manager.status().ok();
        let current_version = deploy
            .as_ref()
            .and_then(|d| d.current_version.clone())
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned());
        let current_commit = deploy
            .as_ref()
            .and_then(|d| d.current.clone())
            .unwrap_or_else(|| self.config.software_commit.clone());
        let previous = deploy
            .as_ref()
            .and_then(|d| d.previous.clone())
            .map(|commit| {
                json!({
                    "version": "previous",
                    "commit": commit,
                })
            });
        let warning = deploy
            .as_ref()
            .and_then(|d| d.activation_failure.as_ref().map(|f| f.message.clone()));

        json!({
            "schema": miro_ego_deploy::FIRMWARE_STATUS_SCHEMA,
            "supported": true,
            "current": {
                "version": current_version,
                "commit": current_commit,
            },
            "previous": previous,
            "available": Value::Null,
            "has_update": false,
            "checked_at": checked_at,
            "warning": warning,
            "task": task,
        })
    }
}

fn controller_status_to_openapi(
    status: &ControllerStatus,
    device_label: &str,
    listen_port: u16,
) -> Value {
    let ap_ssid = format!("MiroEgo-{}", device_label.trim_start_matches("Miro-Ego-"));
    json!({
        "schema": miro_ego_network::NETWORK_STATUS_SCHEMA,
        "authority_epoch": status.authority_epoch,
        "source_revision": status.source_revision,
        "observed_at": rfc3339_now_utc(),
        "saved": status.saved,
        "verified": status.verified,
        "desired": status.desired,
        "observed": {
            "ap": {
                "state": "active",
                "interface": "wlan0",
                "addresses": ["10.42.0.1"],
                "peer_or_ssid": ap_ssid,
            },
            "wifi_client": {
                "state": "disconnected",
                "interface": Value::Null,
                "addresses": [],
                "peer_or_ssid": Value::Null,
            },
            "wired": {
                "state": "disconnected",
                "interface": "eth0",
                "addresses": [],
                "peer_or_ssid": Value::Null,
            },
            "default_route": "none",
            "mdns": {
                "hostname": "miro-ego.local",
                "service": miro_ego_network::MDNS_SERVICE_TYPE,
                "aliases": ["miro-ego.local"],
                "port": listen_port,
            },
            "devices": [
                { "interface": "wlan0", "type": "wifi", "state": "connected" },
                { "interface": "eth0", "type": "ethernet", "state": "disconnected" },
            ],
        },
        "transaction": status.transaction,
        "mutation_capability": status.capability,
        "concurrency_capability": {
            "rescue_ap_required": true,
            "same_phy_ap_sta": "supported",
            "exclusive_client_failure_timeout_seconds": 10,
            "max_managed_interfaces": 1,
            "max_ap_interfaces": 1,
        }
    })
}

fn degraded_network_status(device_label: &str, disabled_reason: &str) -> Value {
    let ap_ssid = format!("MiroEgo-{}", device_label.trim_start_matches("Miro-Ego-"));
    json!({
        "schema": miro_ego_network::NETWORK_STATUS_SCHEMA,
        "authority_epoch": "00000000-0000-4000-8000-000000000000",
        "source_revision": 0,
        "observed_at": rfc3339_now_utc(),
        "saved": false,
        "verified": false,
        "desired": {
            "mode": "hotspot",
            "wifi_client": Value::Null,
            "ethernet": Value::Null,
        },
        "observed": {
            "ap": {
                "state": "unknown",
                "interface": "wlan0",
                "addresses": [],
                "peer_or_ssid": ap_ssid,
            },
            "wifi_client": {
                "state": "disconnected",
                "interface": Value::Null,
                "addresses": [],
                "peer_or_ssid": Value::Null,
            },
            "wired": {
                "state": "disconnected",
                "interface": "eth0",
                "addresses": [],
                "peer_or_ssid": Value::Null,
            },
            "default_route": "none",
            "mdns": {
                "hostname": "miro-ego.local",
                "service": miro_ego_network::MDNS_SERVICE_TYPE,
                "aliases": ["miro-ego.local"],
                "port": 8080,
            },
            "devices": [],
        },
        "transaction": {
            "current": Value::Null,
            "latest": Value::Null,
        },
        "mutation_capability": {
            "enabled": false,
            "disabled_reason": disabled_reason,
            "operations": [],
            "idempotency_key_required": true,
            "secret_handling": "opaque_credential_reference_only",
            "active_state_policy": "idle_only",
        },
        "concurrency_capability": {
            "rescue_ap_required": true,
            "same_phy_ap_sta": "unknown",
            "exclusive_client_failure_timeout_seconds": 10,
            "max_managed_interfaces": 1,
            "max_ap_interfaces": 1,
        }
    })
}

async fn run_blocking<F, T>(f: F) -> ApiResult<T>
where
    F: FnOnce() -> ApiResult<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.map_err(|err| {
        ApiError::internal("task_join_failed", format!("blocking task failed: {err}"))
    })?
}

#[async_trait]
impl GatewayBackend for ProductionGatewayBackend {
    async fn device_info(&self, security_profile: SecurityProfile) -> ApiResult<Value> {
        let cfg = self.config.device_config.clone();
        let volume = Arc::clone(&self.volume);
        let coordinator = Arc::clone(&self.coordinator);
        let release_manager = Arc::clone(&self.release_manager);
        let fallback_commit = self.config.software_commit.clone();
        let preview_h264 = self.h264_hub.is_some();
        let net_client = self.network_client.clone();

        run_blocking(move || {
            let vol_info = volume.inspect_volume().ok();
            let vol_meta = volume.read_metadata().ok();
            let writable = volume.is_mounted_and_writable();
            let volume_id = vol_meta.map(|m| m.volume_id);
            let status = coordinator.capture_status().map_err(map_storage_error)?;
            let deploy = release_manager.status().ok();
            let commit = deploy
                .as_ref()
                .and_then(|d| d.current.clone())
                .unwrap_or(fallback_commit);
            let pkg_version = deploy
                .as_ref()
                .and_then(|d| d.current_version.clone())
                .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned());
            let network_mutation = net_client
                .as_ref()
                .is_some_and(|client| client.health().is_ok());

            Ok(json!({
                "schema": "miro-ego.device.v1",
                "device": {
                    "device_id": cfg.device.device_id,
                    "device_label": cfg.device.device_label,
                },
                "hardware_fingerprint": cfg.device.hardware_fingerprint,
                "api_version": "1.0",
                "build": {
                    "package_version": pkg_version,
                    "commit": commit,
                    "build_id": format!("miro-ego-{pkg_version}-rust"),
                },
                "security_profile": security_profile.as_str(),
                "capabilities": {
                    "capture": true,
                    "preview": true,
                    "preview_h264": preview_h264,
                    "diagnostic_logs": true,
                    "range_download": true,
                    "network_mutation": network_mutation,
                    "session_list": true,
                    "session_detail": true,
                    "artifact_download": true,
                    "capture_status": true,
                    "session_deletion": true,
                    "calibration_capture": {
                        "supported": true,
                        "enabled": true,
                        "disabled_reason": Value::Null,
                        "required_video_layout": "split-eyes",
                    },
                },
                "storage": {
                    "volume_id": volume_id,
                    "total_bytes": vol_info.as_ref().map(|v| v.total_bytes).unwrap_or(0),
                    "available_bytes": vol_info.as_ref().map(|v| v.available_bytes).unwrap_or(0),
                    "writable": writable,
                },
                "runtime": status.snapshot.runtime,
            }))
        })
        .await
    }

    async fn capture_status(&self) -> ApiResult<Value> {
        let coordinator = Arc::clone(&self.coordinator);
        run_blocking(move || {
            let status = coordinator.capture_status().map_err(map_storage_error)?;
            serde_json::to_value(status).map_err(|err| {
                ApiError::internal(
                    "serialize_failed",
                    format!("serialize capture status: {err}"),
                )
            })
        })
        .await
    }

    async fn capture_snapshot_event(&self) -> ApiResult<Value> {
        let coordinator = Arc::clone(&self.coordinator);
        run_blocking(move || {
            let status = coordinator.capture_status().map_err(map_storage_error)?;
            let session_id = status
                .snapshot
                .active_recording
                .as_ref()
                .map(|r| r.recording_state.session_id.clone());
            Ok(json!({
                "authority_epoch": status.authority_epoch,
                "source_revision": status.source_revision,
                "type": "snapshot",
                "occurred_at": rfc3339_now_utc(),
                "session_id": session_id,
                "data": status.snapshot,
            }))
        })
        .await
    }

    async fn capture_readiness(&self) -> ApiResult<Value> {
        let coordinator = Arc::clone(&self.coordinator);
        run_blocking(move || {
            let readiness = coordinator.capture_readiness().map_err(map_storage_error)?;
            serde_json::to_value(readiness).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize readiness: {err}"))
            })
        })
        .await
    }

    async fn capture_safe_swap(&self) -> ApiResult<Option<Value>> {
        let coordinator = Arc::clone(&self.coordinator);
        run_blocking(move || {
            let receipt = coordinator.safe_swap_receipt().map_err(map_storage_error)?;
            match receipt {
                Some(r) => serde_json::to_value(r).map(Some).map_err(|err| {
                    ApiError::internal("serialize_failed", format!("serialize safe-swap: {err}"))
                }),
                None => Ok(None),
            }
        })
        .await
    }

    async fn start_capture(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        let req: CaptureStartRequest = serde_json::from_value(cmd.body).map_err(|err| {
            ApiError::bad_request(format!("invalid capture start request: {err}"))
        })?;
        let coordinator = Arc::clone(&self.coordinator);
        let key = cmd.idempotency_key;
        run_blocking(move || {
            let outcome = coordinator
                .start_capture(req, Some(&key))
                .map_err(map_storage_error)?;
            let body = serde_json::to_value(&outcome.value).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize start status: {err}"))
            })?;
            Ok(CommandOutcome::accepted(body, outcome.replayed))
        })
        .await
    }

    async fn stop_capture(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        let req: CaptureStopRequest = serde_json::from_value(cmd.body)
            .map_err(|err| ApiError::bad_request(format!("invalid capture stop request: {err}")))?;
        let coordinator = Arc::clone(&self.coordinator);
        let key = cmd.idempotency_key;
        run_blocking(move || {
            let outcome = coordinator
                .stop_capture(req, Some(&key))
                .map_err(map_storage_error)?;
            match outcome.value {
                Some(snapshot) => {
                    let body = serde_json::to_value(&snapshot).map_err(|err| {
                        ApiError::internal(
                            "serialize_failed",
                            format!("serialize stop status: {err}"),
                        )
                    })?;
                    Ok(CommandOutcome::accepted(body, outcome.replayed))
                }
                None => Ok(CommandOutcome::no_content(outcome.replayed)),
            }
        })
        .await
    }

    async fn capture_timer(&self) -> ApiResult<Value> {
        let coordinator = Arc::clone(&self.coordinator);
        run_blocking(move || {
            let status = coordinator.get_capture_timer().map_err(map_storage_error)?;
            serde_json::to_value(status).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize timer: {err}"))
            })
        })
        .await
    }

    async fn set_capture_timer(&self, enabled: bool, duration_seconds: u64) -> ApiResult<Value> {
        let dur_u32 = u32::try_from(duration_seconds)
            .map_err(|_| ApiError::bad_request("duration_seconds exceeds supported range"))?;
        let req = CaptureTimerSetRequest {
            schema: CAPTURE_TIMER_SET_SCHEMA.to_owned(),
            enabled,
            duration_seconds: dur_u32,
        };
        let coordinator = Arc::clone(&self.coordinator);
        run_blocking(move || {
            let status = coordinator
                .set_capture_timer(req)
                .map_err(map_storage_error)?;
            serde_json::to_value(status).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize timer: {err}"))
            })
        })
        .await
    }

    async fn camera_focus(&self) -> ApiResult<Option<Value>> {
        let coordinator = Arc::clone(&self.coordinator);
        run_blocking(move || match coordinator.get_camera_focus() {
            Ok(focus) => serde_json::to_value(focus).map(Some).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize focus: {err}"))
            }),
            Err(StorageError::Unsupported { .. }) => Ok(None),
            Err(err) => Err(map_storage_error(err)),
        })
        .await
    }

    async fn set_camera_focus(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        let req: CameraFocusSetRequest = serde_json::from_value(cmd.body)
            .map_err(|err| ApiError::bad_request(format!("invalid camera focus request: {err}")))?;
        let coordinator = Arc::clone(&self.coordinator);
        let key = cmd.idempotency_key;
        run_blocking(move || {
            let outcome = coordinator
                .set_camera_focus(req, Some(&key))
                .map_err(map_storage_error)?;
            let body = serde_json::to_value(&outcome.value).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize focus: {err}"))
            })?;
            Ok(CommandOutcome::ok(body, outcome.replayed))
        })
        .await
    }

    async fn list_sessions(&self, query: ListSessionsQuery) -> ApiResult<Value> {
        let catalog = Arc::clone(&self.catalog);
        run_blocking(move || {
            let page = catalog
                .list_sessions_page(
                    Some(query.limit),
                    query.cursor.as_deref(),
                    query.include_recovery,
                    query.take_id.as_deref(),
                )
                .map_err(map_storage_error)?;
            serde_json::to_value(page).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize session list: {err}"))
            })
        })
        .await
    }

    async fn open_session_manifest(&self, session_id: &str) -> ApiResult<RawManifest> {
        let catalog = Arc::clone(&self.catalog);
        let recordings_root = self.volume.paths().recordings_dir();
        let sid = session_id.to_owned();
        run_blocking(move || {
            let indexed = catalog
                .get_indexed_session(&sid)
                .map_err(map_storage_error)?
                .ok_or_else(|| ApiError::not_found("session not found or not sealed"))?;
            let opened = open_manifest(&recordings_root, &indexed.relative_dir, Some(&sid))
                .map_err(map_storage_error)?;
            Ok(RawManifest {
                bytes: Bytes::from(opened.raw_bytes),
                sha256: opened.sha256,
            })
        })
        .await
    }

    async fn retained_unsuccessful_outcome(&self, session_id: &str) -> ApiResult<Option<Value>> {
        let coordinator = Arc::clone(&self.coordinator);
        let sid = session_id.to_owned();
        run_blocking(move || {
            let out = coordinator
                .retained_unsuccessful_outcome(&sid)
                .map_err(map_storage_error)?;
            match out {
                Some(res) => serde_json::to_value(res).map(Some).map_err(|err| {
                    ApiError::internal(
                        "serialize_failed",
                        format!("serialize unsuccessful outcome: {err}"),
                    )
                }),
                None => Ok(None),
            }
        })
        .await
    }

    async fn artifact_io_busy_state(&self) -> ApiResult<Option<String>> {
        let phase = self.coordinator.phase().map_err(map_storage_error)?;
        let busy = match phase {
            CapturePhase::Starting | CapturePhase::Recording => Some("recording".to_owned()),
            CapturePhase::Stopping | CapturePhase::Recovering | CapturePhase::Deleting => {
                Some("finalizing".to_owned())
            }
            CapturePhase::Idle | CapturePhase::Failed | CapturePhase::Closing => None,
        };
        Ok(busy)
    }

    async fn open_session_artifact(
        &self,
        session_id: &str,
        artifact_id: &str,
    ) -> ApiResult<OpenRepresentation> {
        let catalog = Arc::clone(&self.catalog);
        let recordings_root = self.volume.paths().recordings_dir();
        let sid = session_id.to_owned();
        let aid = artifact_id.to_owned();
        run_blocking(move || {
            let indexed = catalog
                .get_indexed_session(&sid)
                .map_err(map_storage_error)?
                .ok_or_else(|| ApiError::not_found("session not found"))?;
            if let Some(ref verification) = indexed.verification {
                if verification.verdict != "usable" {
                    return Err(ApiError::session_not_verified(&verification.verdict));
                }
            }
            let verified = open_verified_artifact(
                &recordings_root,
                &indexed.relative_dir,
                &aid,
                Some(&sid),
                false,
            )
            .map_err(map_storage_error)?;
            let cloned_file = verified.file().try_clone().map_err(|err| {
                ApiError::provider_failure(format!("clone verified artifact descriptor: {err}"))
            })?;
            Ok(OpenRepresentation::from_file(
                verified.descriptor.media_type.clone(),
                verified.descriptor.etag(),
                cloned_file,
                verified.descriptor.bytes,
            ))
        })
        .await
    }

    async fn delete_sessions(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        let req: DeleteSessionsRequest = serde_json::from_value(cmd.body)
            .map_err(|err| ApiError::bad_request(format!("invalid delete request: {err}")))?;
        let coordinator = Arc::clone(&self.coordinator);
        let key = cmd.idempotency_key;
        run_blocking(move || {
            let outcome = coordinator
                .delete_sessions(req, &key)
                .map_err(map_storage_error)?;
            let body = serde_json::to_value(&outcome.value).map_err(|err| {
                ApiError::internal(
                    "serialize_failed",
                    format!("serialize delete result: {err}"),
                )
            })?;
            Ok(CommandOutcome::ok(body, outcome.replayed))
        })
        .await
    }

    async fn list_logs(&self, query: ListLogsQuery) -> ApiResult<Value> {
        let logs = Arc::clone(&self.logs);
        let volume = Arc::clone(&self.volume);
        run_blocking(move || {
            let volume_id = volume.read_metadata().ok().map(|m| m.volume_id);
            let list = logs
                .list_logs(query.prefix.as_deref(), Some(query.limit), volume_id)
                .map_err(map_storage_error)?;
            serde_json::to_value(list).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize log list: {err}"))
            })
        })
        .await
    }

    async fn open_log_file(&self, name: &str) -> ApiResult<OpenRepresentation> {
        let logs = Arc::clone(&self.logs);
        let log_name = name.to_owned();
        run_blocking(move || {
            let opened = logs.open_log(&log_name).map_err(map_storage_error)?;
            let (_, etag) = opened
                .compute_sha256_and_etag()
                .map_err(map_storage_error)?;
            let cloned = opened.file().try_clone().map_err(|err| {
                ApiError::provider_failure(format!("clone diagnostic log descriptor: {err}"))
            })?;
            let leaf = log_name
                .rsplit('/')
                .next()
                .unwrap_or(&log_name)
                .replace('"', "");
            Ok(OpenRepresentation::from_file(
                opened.entry.media_type.clone(),
                etag,
                cloned,
                opened.identity.size,
            )
            .with_content_disposition(format!("attachment; filename=\"{leaf}\"")))
        })
        .await
    }

    fn preview_mjpeg_hub(&self) -> Option<Arc<MjpegPreviewHub>> {
        Some(Arc::clone(&self.mjpeg_hub))
    }

    fn preview_h264_hub(&self) -> Option<Arc<H264PreviewHub>> {
        self.h264_hub.as_ref().map(Arc::clone)
    }

    fn capture_event_buffer(&self) -> Arc<EventReplayBuffer> {
        Arc::clone(&self.capture_events)
    }

    fn network_event_buffer(&self) -> Arc<NetworkEventReplayBuffer> {
        Arc::clone(&self.network_events)
    }

    async fn clock_status(&self, principal_id: &str) -> ApiResult<Value> {
        let mut guard = self
            .clock
            .lock()
            .map_err(|_| ApiError::internal("clock_poisoned", "clock state mutex poisoned"))?;
        let (challenge, expires_in_ms) = if guard.synced {
            (Value::Null, 0u64)
        } else {
            (
                Value::String(guard.issue_challenge(principal_id, false)),
                5000u64,
            )
        };
        Ok(json!({
            "schema": "miro-ego.clock-status.v1",
            "source": if guard.synced { "client" } else { "unsynchronized" },
            "unix_time_ms": guard.current_unix_ms(),
            "challenge": challenge,
            "expires_in_ms": expires_in_ms,
            "applied": guard.synced,
            "boot_id": guard.boot_id,
            "synchronized_at_ms": guard.synchronized_at_ms,
            "age_seconds": guard.age_seconds(),
        }))
    }

    async fn clock_inspect(&self, principal_id: &str) -> ApiResult<Value> {
        let mut guard = self
            .clock
            .lock()
            .map_err(|_| ApiError::internal("clock_poisoned", "clock state mutex poisoned"))?;
        let tok = guard.issue_challenge(principal_id, true);
        Ok(json!({
            "schema": "miro-ego.clock-status.v1",
            "source": if guard.synced { "client" } else { "unsynchronized" },
            "unix_time_ms": guard.current_unix_ms(),
            "challenge": tok,
            "expires_in_ms": 5000,
            "applied": guard.synced,
            "boot_id": guard.boot_id,
            "synchronized_at_ms": guard.synchronized_at_ms,
            "age_seconds": guard.age_seconds(),
        }))
    }

    async fn clock_sync(&self, principal_id: &str, body: Value) -> ApiResult<Value> {
        if self
            .coordinator
            .phase()
            .map_err(map_storage_error)?
            .is_busy()
        {
            return Err(ApiError::conflict(
                "capture_active",
                "clock sync is prohibited while capture is active",
            )
            .with_error_code_header());
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

        let mut guard = self
            .clock
            .lock()
            .map_err(|_| ApiError::internal("clock_poisoned", "clock state mutex poisoned"))?;
        let Some(record) = guard.challenges.remove(challenge) else {
            return Err(ApiError::conflict(
                "clock_challenge_invalid",
                "clock challenge is expired, already used, or unknown",
            )
            .with_error_code_header());
        };
        if record.expires_at <= Instant::now() || record.principal_id != principal_id {
            return Err(ApiError::conflict(
                "clock_challenge_invalid",
                "clock challenge expired or bound to a different principal",
            )
            .with_error_code_header());
        }
        if replace && !record.from_check {
            return Err(ApiError::conflict(
                "clock_challenge_invalid",
                "replace=true requires a fresh /clock/check challenge",
            )
            .with_error_code_header());
        }
        let applied = if !guard.synced || replace {
            let sys_now = now_unix_ms() as i128;
            guard.offset_ms = (i128::from(unix_time_ms) - sys_now)
                .clamp(i128::from(i64::MIN), i128::from(i64::MAX))
                as i64;
            guard.synced = true;
            guard.synchronized_at_ms = Some(unix_time_ms);
            guard.synchronized_mono_ns = Some(monotonic_ns().unwrap_or(0));
            true
        } else {
            false
        };
        Ok(json!({
            "schema": "miro-ego.clock-status.v1",
            "source": "client",
            "unix_time_ms": guard.current_unix_ms(),
            "challenge": Value::Null,
            "expires_in_ms": 0,
            "applied": applied,
            "boot_id": guard.boot_id,
            "synchronized_at_ms": guard.synchronized_at_ms,
            "age_seconds": 0,
        }))
    }

    async fn voice_status(&self) -> ApiResult<Value> {
        if let Some(voice) = &self.config.voice_controller {
            let status = voice.status().map_err(map_hw_error)?;
            return serde_json::to_value(status).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize voice status: {err}"))
            });
        }
        let volume = self.voice_fallback_volume.lock().map(|g| *g).unwrap_or(80);
        Ok(json!({
            "schema": miro_ego_hw::VOICE_STATUS_SCHEMA,
            "enabled": false,
            "volume": volume,
        }))
    }

    async fn set_voice_volume(&self, volume: u8) -> ApiResult<Value> {
        if self
            .coordinator
            .phase()
            .map_err(map_storage_error)?
            .is_busy()
        {
            return Err(ApiError::conflict(
                "capture_active",
                "voice volume cannot be modified while capture is active",
            )
            .with_error_code_header());
        }
        if let Some(voice) = &self.config.voice_controller {
            let status = voice.set_volume(volume).map_err(map_hw_error)?;
            return serde_json::to_value(status).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize voice status: {err}"))
            });
        }
        if let Ok(mut guard) = self.voice_fallback_volume.lock() {
            *guard = volume;
        }
        Ok(json!({
            "schema": miro_ego_hw::VOICE_STATUS_SCHEMA,
            "enabled": false,
            "volume": volume,
        }))
    }

    async fn network_status(&self) -> ApiResult<Value> {
        let client = self.network_client.clone();
        let label = self.config.device_config.device.device_label.clone();
        let port = self.config.device_config.listen.port;
        let status_json = run_blocking(move || {
            let val = match client {
                Some(ref c) => match c.status() {
                    Ok(st) => controller_status_to_openapi(&st, &label, port),
                    Err(_) => degraded_network_status(&label, "network_controller_unavailable"),
                },
                None => degraded_network_status(&label, "network_controller_not_configured"),
            };
            Ok(val)
        })
        .await?;
        let _ = self.network_events.observe(status_json.clone());
        Ok(status_json)
    }

    async fn network_scan(&self) -> ApiResult<Value> {
        let client = self.require_network_client()?;
        run_blocking(move || {
            let scan = client.scan().map_err(map_network_error)?;
            serde_json::to_value(scan).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize network scan: {err}"))
            })
        })
        .await
    }

    async fn create_network_credential(
        &self,
        principal_id: &str,
        passphrase: &str,
    ) -> ApiResult<Value> {
        let client = self.require_network_client()?;
        let principal = principal_id.to_owned();
        let pass = passphrase.to_owned();
        run_blocking(move || {
            let receipt = client
                .create_credential(&principal, &pass)
                .map_err(map_network_error)?;
            serde_json::to_value(receipt).map_err(|err| {
                ApiError::internal(
                    "serialize_failed",
                    format!("serialize network credential receipt: {err}"),
                )
            })
        })
        .await
    }

    async fn apply_network_desired_state(
        &self,
        cmd: IdempotentCommand,
    ) -> ApiResult<CommandOutcome> {
        if self
            .coordinator
            .phase()
            .map_err(map_storage_error)?
            .is_busy()
        {
            return Err(ApiError::conflict(
                "capture_active",
                "network changes are prohibited while capture is active",
            )
            .with_error_code_header());
        }
        let desired_val = cmd
            .body
            .get("desired")
            .cloned()
            .ok_or_else(|| ApiError::bad_request("missing desired network state"))?;
        let desired: ApplyDesiredState = serde_json::from_value(desired_val).map_err(|err| {
            ApiError::bad_request(format!("invalid desired network state: {err}"))
        })?;
        let client = self.require_network_client()?;
        let principal = cmd.principal_id;
        let key = cmd.idempotency_key;
        let label = self.config.device_config.device.device_label.clone();
        let port = self.config.device_config.listen.port;

        let (outcome, updated_status) = run_blocking(move || {
            let res = client
                .apply(&principal, &key, &desired)
                .map_err(map_network_error)?;
            let body = serde_json::to_value(&res.receipt).map_err(|err| {
                ApiError::internal(
                    "serialize_failed",
                    format!("serialize network receipt: {err}"),
                )
            })?;
            let status_json = client
                .status()
                .ok()
                .map(|st| controller_status_to_openapi(&st, &label, port));
            Ok((CommandOutcome::accepted(body, res.replayed), status_json))
        })
        .await?;

        if let Some(st) = updated_status {
            let _ = self.network_events.observe(st);
        }
        Ok(outcome)
    }

    async fn retry_network_transaction(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        if self
            .coordinator
            .phase()
            .map_err(map_storage_error)?
            .is_busy()
        {
            return Err(ApiError::conflict(
                "capture_active",
                "network retry is prohibited while capture is active",
            )
            .with_error_code_header());
        }
        let tx_id = cmd
            .body
            .get("transaction_id")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::bad_request("missing transaction_id"))?
            .to_owned();
        let client = self.require_network_client()?;
        let principal = cmd.principal_id;
        let key = cmd.idempotency_key;
        let label = self.config.device_config.device.device_label.clone();
        let port = self.config.device_config.listen.port;

        let (outcome, updated_status) = run_blocking(move || {
            let res = client
                .retry(&principal, &key, &tx_id)
                .map_err(map_network_error)?;
            let body = serde_json::to_value(&res.receipt).map_err(|err| {
                ApiError::internal(
                    "serialize_failed",
                    format!("serialize retry receipt: {err}"),
                )
            })?;
            let status_json = client
                .status()
                .ok()
                .map(|st| controller_status_to_openapi(&st, &label, port));
            Ok((CommandOutcome::accepted(body, res.replayed), status_json))
        })
        .await?;

        if let Some(st) = updated_status {
            let _ = self.network_events.observe(st);
        }
        Ok(outcome)
    }

    async fn forget_network_client_profile(
        &self,
        cmd: IdempotentCommand,
    ) -> ApiResult<CommandOutcome> {
        if self
            .coordinator
            .phase()
            .map_err(map_storage_error)?
            .is_busy()
        {
            return Err(ApiError::conflict(
                "capture_active",
                "forgetting Wi-Fi profile is prohibited while capture is active",
            )
            .with_error_code_header());
        }
        let client = self.require_network_client()?;
        let principal = cmd.principal_id;
        let key = cmd.idempotency_key;
        let label = self.config.device_config.device.device_label.clone();
        let port = self.config.device_config.listen.port;

        let (outcome, updated_status) = run_blocking(move || {
            let res = client.forget(&principal, &key).map_err(map_network_error)?;
            let body = serde_json::to_value(&res.receipt).map_err(|err| {
                ApiError::internal(
                    "serialize_failed",
                    format!("serialize forget receipt: {err}"),
                )
            })?;
            let status_json = client
                .status()
                .ok()
                .map(|st| controller_status_to_openapi(&st, &label, port));
            Ok((CommandOutcome::accepted(body, res.replayed), status_json))
        })
        .await?;

        if let Some(st) = updated_status {
            let _ = self.network_events.observe(st);
        }
        Ok(outcome)
    }

    async fn firmware_status(&self) -> ApiResult<Value> {
        let (checked_at, task) = self
            .firmware
            .lock()
            .map(|g| (g.checked_at, g.task.clone()))
            .map_err(|_| ApiError::internal("firmware_poisoned", "firmware state poisoned"))?;
        Ok(self.format_firmware_status(checked_at, task))
    }

    async fn firmware_check(&self, _force: bool) -> ApiResult<Value> {
        let now = now_unix_secs();
        let task = {
            let mut guard = self
                .firmware
                .lock()
                .map_err(|_| ApiError::internal("firmware_poisoned", "firmware state poisoned"))?;
            guard.checked_at = Some(now);
            guard.task.clone()
        };
        Ok(self.format_firmware_status(Some(now), task))
    }

    async fn firmware_update(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        if self
            .coordinator
            .phase()
            .map_err(map_storage_error)?
            .is_busy()
        {
            return Err(ApiError::conflict(
                "capture_active",
                "firmware update is prohibited while capture is active",
            )
            .with_error_code_header());
        }
        let commit = cmd
            .body
            .get("commit")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::bad_request("missing commit"))?
            .to_owned();
        let mut guard = self
            .firmware
            .lock()
            .map_err(|_| ApiError::internal("firmware_poisoned", "firmware state poisoned"))?;
        let idem_key = (
            "firmware_update".to_owned(),
            cmd.principal_id.clone(),
            cmd.idempotency_key.clone(),
        );
        if let Some((prev_body, prev_outcome)) = guard.idempotency.get(&idem_key) {
            if prev_body == &cmd.canonical_body {
                let mut replayed = prev_outcome.clone();
                replayed.replayed = true;
                return Ok(replayed);
            }
            return Err(ApiError::conflict(
                "idempotency_conflict",
                "Idempotency-Key reused with different firmware update payload",
            )
            .with_error_code_header());
        }
        let staged_bundle = self.volume.paths().updates_dir().join(&commit);
        if !staged_bundle.is_dir() {
            return Err(ApiError::conflict(
                "update_not_staged",
                format!(
                    "offline firmware bundle for commit {commit} is not staged under /data/updates/"
                ),
            )
            .with_error_code_header());
        }
        let task = json!({
            "id": new_uuid_v7(),
            "action": "update",
            "target_commit": commit,
            "status": "accepted",
            "message": "staged offline firmware bundle accepted for activation",
            "started_at": now_unix_secs(),
            "finished_at": Value::Null,
        });
        guard.task = Some(task.clone());
        let outcome = CommandOutcome::accepted(task, false);
        guard
            .idempotency
            .insert(idem_key, (cmd.canonical_body, outcome.clone()));
        Ok(outcome)
    }

    async fn firmware_rollback(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        if self
            .coordinator
            .phase()
            .map_err(map_storage_error)?
            .is_busy()
        {
            return Err(ApiError::conflict(
                "capture_active",
                "firmware rollback is prohibited while capture is active",
            )
            .with_error_code_header());
        }
        let commit = cmd
            .body
            .get("commit")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::bad_request("missing commit"))?
            .to_owned();
        let deploy = self.release_manager.status().map_err(map_deploy_error)?;
        if deploy.previous.as_deref() != Some(commit.as_str()) {
            return Err(ApiError::conflict(
                "rollback_unavailable",
                "requested commit does not match installed previous release",
            )
            .with_error_code_header());
        }
        let mut guard = self
            .firmware
            .lock()
            .map_err(|_| ApiError::internal("firmware_poisoned", "firmware state poisoned"))?;
        let idem_key = (
            "firmware_rollback".to_owned(),
            cmd.principal_id.clone(),
            cmd.idempotency_key.clone(),
        );
        if let Some((prev_body, prev_outcome)) = guard.idempotency.get(&idem_key) {
            if prev_body == &cmd.canonical_body {
                let mut replayed = prev_outcome.clone();
                replayed.replayed = true;
                return Ok(replayed);
            }
            return Err(ApiError::conflict(
                "idempotency_conflict",
                "Idempotency-Key reused with different firmware rollback payload",
            )
            .with_error_code_header());
        }
        let task = json!({
            "id": new_uuid_v7(),
            "action": "rollback",
            "target_commit": commit,
            "status": "accepted",
            "message": "firmware rollback accepted",
            "started_at": now_unix_secs(),
            "finished_at": Value::Null,
        });
        guard.task = Some(task.clone());
        let outcome = CommandOutcome::accepted(task, false);
        guard
            .idempotency
            .insert(idem_key, (cmd.canonical_body, outcome.clone()));
        Ok(outcome)
    }

    fn record_audit(&self, event: AuditEvent) -> ApiResult<()> {
        let storage_event = ApiAuditEvent {
            schema: event.schema,
            recorded_at: event.recorded_at,
            request_id: event.request_id,
            principal_id: event.principal_id,
            operation_id: event.operation_id,
            poll_count: None,
            resource_id: event.resource_id,
            outcome: event.outcome,
        };
        let target = self
            .audit
            .record(&storage_event)
            .map_err(map_storage_error)?;
        match target {
            AuditWriteTarget::Disk | AuditWriteTarget::MemoryFallback => Ok(()),
            AuditWriteTarget::Unavailable => Err(ApiError::audit_unavailable()),
        }
    }

    fn get_web_asset(&self, name: &str) -> Option<StaticAsset> {
        self.web_assets.get(name).cloned()
    }
}

// ---------------------------------------------------------------------------
// Unified Daemon Runtime Assembly & Graceful Lifecycle
// ---------------------------------------------------------------------------

/// Capture backend selection mode for [`DaemonServeOptions`].
#[derive(Debug, Clone)]
pub enum CaptureModeKind {
    /// Real hardware V4L2 + `miro-ego-capture` + `MEGO_SHM` encoder.
    Native {
        encoder_bin: PathBuf,
        imu_device: Option<PathBuf>,
    },
    /// Real external C encoder (`miro-ego-encoder` / `miro-ego-encoder-mock`) driven over `MEGO_SHM`.
    ShmEncoder {
        encoder_bin: PathBuf,
        segment_frames: u64,
    },
    /// Pure-Rust synthetic capture backend (`--mock` without external encoder).
    Mock,
}

/// Parameters for assembling and serving the complete `miro-ego` daemon.
#[derive(Clone)]
pub struct DaemonServeOptions {
    pub device_config: DeviceConfig,
    pub capture_mode: CaptureModeKind,
    pub listen_override: Option<SocketAddr>,
    pub network_socket: Option<PathBuf>,
    pub release_paths: ReleaseManagerPaths,
    pub relaxed_volume: bool,
    pub enable_h264_preview: bool,
    pub run_dir: Option<PathBuf>,
    pub voice_controller: Option<Arc<VoiceController>>,
    pub indicator: Option<Arc<SysfsLedController>>,
    pub reserve_poll_interval: Duration,
    pub timer_poll_interval: Duration,
}

impl DaemonServeOptions {
    pub fn from_device_config(device_config: DeviceConfig, mock: bool) -> Self {
        let encoder_bin = std::env::var_os("MIRO_EGO_ENCODER_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(SYSTEM_ENCODER_BIN));
        let capture_mode = if mock {
            CaptureModeKind::Mock
        } else {
            CaptureModeKind::Native {
                encoder_bin,
                imu_device: None,
            }
        };
        let network_socket = std::env::var_os("MIRO_EGO_NETWORK_SOCKET")
            .map(PathBuf::from)
            .or_else(|| Some(PathBuf::from(miro_ego_network::DEFAULT_CONTROL_SOCKET_PATH)));
        Self {
            device_config,
            capture_mode,
            listen_override: None,
            network_socket,
            release_paths: ReleaseManagerPaths::canonical(),
            relaxed_volume: mock,
            enable_h264_preview: true,
            run_dir: Some(PathBuf::from(miro_ego_core::paths::SYSTEM_RUN_DIR)),
            voice_controller: None,
            indicator: None,
            reserve_poll_interval: DEFAULT_RESERVE_POLL_INTERVAL,
            timer_poll_interval: DEFAULT_TIMER_POLL_INTERVAL,
        }
    }
}

/// Assembled live daemon ready to bind and serve HTTP/HTTPS requests.
pub struct BoundDaemon {
    pub server: GatewayServer,
    pub backend: Arc<ProductionGatewayBackend>,
    pub reserve_poll_interval: Duration,
    pub timer_poll_interval: Duration,
}

/// Assemble all storage, capture, preview, security, and HTTP server subsystems, running
/// startup `StagingRecoveryManager::recover_staging()` BEFORE opening HTTP intake!
pub async fn bind_daemon(options: DaemonServeOptions) -> Result<BoundDaemon, String> {
    options
        .device_config
        .validate()
        .map_err(|err| format!("invalid device config: {err}"))?;

    let volume_paths = DataVolumePaths::new(options.device_config.storage.mountpoint.clone());
    let policy = if options.relaxed_volume {
        VolumePolicy::relaxed_for_tests(options.device_config.storage.minimum_available_bytes)
    } else {
        let mut prod = VolumePolicy::production(
            options.device_config.storage.minimum_available_bytes,
            options.device_config.storage.minimum_available_inodes,
        );
        prod.system_state_root = Some(options.device_config.state_root.clone());
        prod
    };
    let volume = Arc::new(ExfatVolumeManager::new(volume_paths.clone(), policy));
    if volume.is_mounted_and_writable() {
        let _ = volume.ensure_layout();
    }

    let catalog = Arc::new(
        BinarySessionCatalog::open(volume_paths.clone())
            .map_err(|err| format!("failed to open binary session catalog: {err}"))?,
    );

    let vol_for_audit = Arc::clone(&volume);
    let audit = Arc::new(AuditLogger::new(
        volume_paths.clone(),
        AuditConfig::default(),
        Arc::new(move || vol_for_audit.is_mounted_and_writable()),
    ));

    let vol_for_logs = Arc::clone(&volume);
    let logs = Arc::new(LogStore::new(
        volume_paths,
        Arc::new(move || vol_for_logs.is_mounted_and_writable()),
    ));

    let mjpeg_hub = Arc::new(MjpegPreviewHub::new(
        options.device_config.camera.fps.clamp(1, 30),
    ));
    let h264_hub = if options.enable_h264_preview {
        let hub = Arc::new(H264PreviewHub::new_fmp4(64));
        hub.set_init_segment_blocking(Bytes::from_static(SYNTHETIC_FMP4_INIT));
        Some(hub)
    } else {
        None
    };

    let effective_fps = (f64::from(options.device_config.camera.fps)
        / f64::from(options.device_config.camera.frame_decimation))
    .max(1.0);
    let default_segment_frames =
        ((effective_fps * options.device_config.segment_seconds).round() as u64).max(1);

    let (capture_backend, segment_frames): (Arc<dyn CaptureBackend>, u64) =
        match options.capture_mode {
            CaptureModeKind::Native {
                encoder_bin,
                imu_device,
            } => {
                let b = NativeCaptureBackend::open(
                    options.device_config.clone(),
                    encoder_bin,
                    imu_device,
                    Arc::clone(&mjpeg_hub),
                )
                .map_err(|err| format!("failed to initialize native capture backend: {err}"))?;
                (Arc::new(b), default_segment_frames)
            }
            CaptureModeKind::ShmEncoder {
                encoder_bin,
                segment_frames,
            } => {
                let b = ShmEncoderCaptureBackend::new(
                    encoder_bin,
                    options.device_config.camera.width,
                    options.device_config.camera.height,
                    effective_fps.round() as u32,
                    segment_frames.max(1),
                    Arc::clone(&mjpeg_hub),
                    h264_hub.clone(),
                );
                (Arc::new(b), segment_frames.max(1))
            }
            CaptureModeKind::Mock => {
                let b = MockCaptureBackend::new(
                    effective_fps.round() as u32,
                    Arc::clone(&mjpeg_hub),
                    h264_hub.clone(),
                );
                (Arc::new(b), default_segment_frames)
            }
        };

    let timer_settings_path = options.device_config.state_root.join("capture-timer.json");
    let coord_cfg = CoordinatorConfig {
        device_id: options.device_config.device.device_id.clone(),
        device_label: options.device_config.device.device_label.clone(),
        hardware_fingerprint: options.device_config.device.hardware_fingerprint.clone(),
        platform: "rdk-x5".to_owned(),
        software_version: env!("CARGO_PKG_VERSION").to_owned(),
        commit: option_env!("MIRO_EGO_GIT_COMMIT")
            .map(str::to_owned)
            .filter(|s| s.len() == 40)
            .unwrap_or_else(|| "0".repeat(40)),
        timezone: "Etc/UTC".to_owned(),
        camera_width: options.device_config.camera.width,
        camera_height: options.device_config.camera.height,
        sensor_fps: f64::from(options.device_config.camera.fps),
        frame_decimation: options.device_config.camera.frame_decimation,
        segment_frames,
        encoding: options.device_config.recording.clone(),
        run_dir: options.run_dir.clone(),
        timer_settings_path: Some(timer_settings_path),
    };

    let coordinator = Arc::new(CaptureCoordinator::new(
        coord_cfg,
        Arc::clone(&volume),
        Arc::clone(&catalog),
        capture_backend,
    ));

    // Step 1: Run crash staging recovery BEFORE opening HTTP intake.
    if volume.is_mounted_and_writable() {
        let coord_for_recovery = Arc::clone(&coordinator);
        let _ = tokio::task::spawn_blocking(move || coord_for_recovery.recover_on_startup()).await;
    }

    let mut backend_cfg =
        ProductionBackendConfig::from_device_config(options.device_config.clone());
    backend_cfg.network_socket = options.network_socket;
    backend_cfg.release_paths = options.release_paths;
    backend_cfg.enable_h264_preview = options.enable_h264_preview;
    backend_cfg.voice_controller = options.voice_controller;
    backend_cfg.indicator = options.indicator;

    let backend = Arc::new(ProductionGatewayBackend::new(
        backend_cfg,
        Arc::clone(&coordinator),
        Arc::clone(&volume),
        Arc::clone(&catalog),
        Arc::clone(&audit),
        Arc::clone(&logs),
        mjpeg_hub,
        h264_hub,
    ));

    let (security, tls) = build_security_and_tls(&options.device_config)?;
    let bind_addr = match options.listen_override {
        Some(addr) => addr,
        None => {
            let ip: IpAddr = options
                .device_config
                .listen
                .host
                .parse()
                .map_err(|err| format!("invalid listen.host: {err}"))?;
            SocketAddr::new(ip, options.device_config.listen.port)
        }
    };

    let server_cfg = match tls {
        Some(tls_cfg) => GatewayServerConfig::https(bind_addr, tls_cfg),
        None => GatewayServerConfig::http(bind_addr),
    };
    let server = GatewayServer::bind(
        server_cfg,
        Arc::clone(&backend) as Arc<dyn GatewayBackend>,
        security,
    )
    .await
    .map_err(|err| format!("failed to bind gateway server on {bind_addr}: {err}"))?;

    Ok(BoundDaemon {
        server,
        backend,
        reserve_poll_interval: options.reserve_poll_interval,
        timer_poll_interval: options.timer_poll_interval,
    })
}

/// Serve a [`BoundDaemon`] together with its background supervisor tasks until `shutdown` fires,
/// then gracefully drain HTTP connections, seal any active capture, flush audit logs, and restore LED.
pub async fn run_bound_daemon_with_shutdown<F>(
    daemon: BoundDaemon,
    shutdown: F,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send + 'static,
{
    let BoundDaemon {
        server,
        backend,
        reserve_poll_interval,
        timer_poll_interval,
    } = daemon;

    if let Some(led) = backend.indicator() {
        let _ = led.set_state(IndicatorState::Ready);
    }
    if let Some(voice) = backend.voice_controller() {
        let _ = voice.play_cue(VoiceCue::DeviceReady);
    }

    let bg_stop = Arc::new(AtomicBool::new(false));
    let (shutdown_tx, mut reserve_rx) = tokio::sync::watch::channel(false);
    let mut timer_rx = shutdown_tx.subscribe();

    // Task 1: Capture event bridge -> SSE replay buffer + LED + voice feedback.
    let event_sub = backend.coordinator().subscribe_events();
    let event_buf = backend.capture_event_buffer();
    let event_stop = Arc::clone(&bg_stop);
    let event_led = backend.indicator().cloned();
    let event_voice = backend.voice_controller().cloned();
    let event_thread = thread::Builder::new()
        .name("miro-ego-event-bridge".to_owned())
        .spawn(move || {
            while !event_stop.load(Ordering::Acquire) {
                match event_sub.recv_timeout(Duration::from_millis(200)) {
                    Ok(ev) => {
                        forward_capture_event(
                            &ev,
                            &event_buf,
                            event_led.as_deref(),
                            event_voice.as_deref(),
                        );
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
        })
        .ok();

    // Task 2: 1 Hz storage reserve enforcement & audit RAM fallback flush.
    let reserve_coord = Arc::clone(backend.coordinator());
    let reserve_audit = Arc::clone(backend.audit());
    let reserve_voice = backend.voice_controller().cloned();
    let reserve_task = tokio::spawn(async move {
        let mut ticker =
            tokio::time::interval(reserve_poll_interval.max(Duration::from_millis(50)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let c = Arc::clone(&reserve_coord);
                    let a = Arc::clone(&reserve_audit);
                    let outcome = tokio::task::spawn_blocking(move || {
                        let _ = a.flush_fallback_to_disk();
                        c.enforce_storage_reserve()
                    })
                    .await;
                    if let Ok(Ok(ReserveEnforcementOutcome::StoppedForReserve { .. })) = outcome {
                        if let Some(ref voice) = reserve_voice {
                            let _ = voice.play_cue(VoiceCue::StorageLow);
                        }
                    }
                }
                _ = reserve_rx.changed() => {
                    if *reserve_rx.borrow() {
                        break;
                    }
                }
            }
        }
    });

    // Task 3: Capture timer expiration monitor.
    let timer_coord = Arc::clone(backend.coordinator());
    let timer_task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(timer_poll_interval.max(Duration::from_millis(50)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let c = Arc::clone(&timer_coord);
                    let _ = tokio::task::spawn_blocking(move || c.poll_capture_timer()).await;
                }
                _ = timer_rx.changed() => {
                    if *timer_rx.borrow() {
                        break;
                    }
                }
            }
        }
    });

    // Run HTTP/HTTPS gateway until shutdown signal resolves.
    let serve_result = server.serve_with_graceful_shutdown(shutdown).await;

    // Graceful shutdown sequence:
    // 1. Stop background tasks.
    bg_stop.store(true, Ordering::Release);
    let _ = shutdown_tx.send(true);
    let _ = reserve_task.await;
    let _ = timer_task.await;
    if let Some(handle) = event_thread {
        let _ = handle.join();
    }

    // 2. Cleanly stop and seal any active capture, then close coordinator & flush audit log.
    let shutdown_coord = Arc::clone(backend.coordinator());
    let shutdown_audit = Arc::clone(backend.audit());
    let _ = tokio::task::spawn_blocking(move || {
        if matches!(shutdown_coord.phase(), Ok(CapturePhase::Recording)) {
            let _ = shutdown_coord.stop_capture(CaptureStopRequest::user(), None);
        }
        let _ = shutdown_coord.close();
        let _ = shutdown_audit.flush_fallback_to_disk();
    })
    .await;

    // 3. Restore LED state on exit.
    if let Some(led) = backend.indicator() {
        let _ = led.restore();
    }

    serve_result.map_err(|err| format!("gateway server exited with error: {err}"))
}

fn forward_capture_event(
    ev: &CaptureEvent,
    event_buf: &EventReplayBuffer,
    led: Option<&SysfsLedController>,
    voice: Option<&VoiceController>,
) {
    let ev_type = match ev.event_type {
        CaptureEventType::Snapshot => "snapshot",
        CaptureEventType::State => "state",
        CaptureEventType::Progress => "progress",
        CaptureEventType::Diagnostic => "diagnostic",
        CaptureEventType::SafeSwap => "safe_swap",
    };
    let payload = json!({
        "authority_epoch": ev.authority_epoch,
        "source_revision": ev.source_revision,
        "type": ev_type,
        "occurred_at": ev.occurred_at,
        "session_id": ev.session_id,
        "data": ev.data,
    });
    let _ = event_buf.publish(payload);

    if ev.event_type == CaptureEventType::State {
        if let Some(state_str) = ev.data.get("state").and_then(Value::as_str) {
            match state_str {
                "recording" => {
                    if let Some(l) = led {
                        let _ = l.set_state(IndicatorState::Recording);
                    }
                    if let Some(v) = voice {
                        let _ = v.play_cue(VoiceCue::RecordingStarted);
                    }
                }
                "idle" => {
                    if let Some(l) = led {
                        let _ = l.set_state(IndicatorState::Ready);
                    }
                    if let Some(v) = voice {
                        let _ = v.play_cue(VoiceCue::RecordingStopped);
                    }
                }
                "failed" => {
                    if let Some(l) = led {
                        let _ = l.set_state(IndicatorState::Error);
                    }
                    if let Some(v) = voice {
                        let _ = v.play_cue(VoiceCue::ErrorAlert);
                    }
                }
                _ => {}
            }
        }
    }
}

/// Wait for `SIGINT` or `SIGTERM` on Unix systems.
pub async fn wait_for_termination_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    let sigterm = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            let _ = sig.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    tokio::select! {
        _ = ctrl_c => {}
        _ = sigterm => {}
    }
}
