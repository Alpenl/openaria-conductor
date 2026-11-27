//! Comprehensive Wave B integration & E2E verification suite for `miro-ego-cli`.
//!
//! Covers all 8 Wave B cross-crate wiring groups:
//! 1. Production IMU default wiring (`imu_device = Some(camera.device)`), `enable_h264_preview = false`
//!    in `Native` mode, and `--run-dir` + non-root `--relaxed-volume` fallback.
//! 2. Mid-recording `ActiveCaptureHandle::take_failure()` propagation via supervisor
//!    `poll_active_failure()` (with capture timer disabled), `Drop`/`abort` cleanup,
//!    SSE event emission, lock release, principal-scoped idempotency isolation, and
//!    preview `503 camera_not_connected` admission.
//! 3. Customer HTTPS echoed-bearer `X-CSRF-Token`, `MIRO_EGO_CSRF_TOKEN` override,
//!    `miro-ego button` RDK X5 BOARD pin `37 -> gpio401` export, credential read error
//!    surfacing, and non-deadlocking error exit.
//! 4. Privileged `miro-ego deploy serve-rpc` (including systemd `StandardInput=socket` fd 0
//!    activation + `NOTIFY_SOCKET`) + `GET /api/v1/firmware/check` offline discovery +
//!    upfront `verify_bundle_dir` validation on `POST /api/v1/firmware/update` + live v1->v2
//!    install + `previous.version` parity + `POST /api/v1/firmware/rollback`.
//! 5. Audio `"audio_open_failed"` manifest/catalog/schema parity and `button.json` LED config.
//! 6. Live `ControllerStatus.observed` & `rescue_ssid`, Avahi Customer HTTPS `ControllerConfig`,
//!    and active-capture shared `network-operation.lock` blocking `network-control` & `network_scan`.
//! 7. OpenAPI v1 error/schema parity (`409 session_not_verified` `absent`/`stale`/`unusable`,
//!    `423 capture_busy` `current_state`, `404 network_transaction_not_found`,
//!    `409 idempotency_conflict` `network-mutation`, `degraded_network_status`, unmounted
//!    `device_info` `UuidV4` `volume_id`, and `Settings::validate_for_device`).

use bytes::Bytes;
use http::{HeaderMap, Method, Request};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use miro_ego_api::{
    GatewayBackend, GatewayServer, GatewayServerConfig, MjpegPreviewHub, Principal, SecurityPolicy,
};
use miro_ego_cli::commands::{
    apply_settings_to_device_config, build_button_capture_client, build_network_controller_config,
    load_and_apply_settings_for_device, resolve_serve_led_config, validate_session_dir,
};
use miro_ego_cli::runtime::{
    CaptureModeKind, DaemonServeOptions, EventFeedbackTracker, NetworkLockCaptureBackend,
    NoopHealthChecker, NoopSystemctlRunner, ProductionBackendConfig, ProductionGatewayBackend,
    bind_daemon, degraded_network_status, degraded_network_status_with_scheme,
    map_camera_focus_error, map_deploy_error, map_network_error, map_network_mutation_error,
    map_storage_error, run_bound_daemon_with_shutdown,
};
use miro_ego_core::CoreError;
use miro_ego_core::config::{
    AudioConfig, CameraConfig, DEVICE_CONFIG_SCHEMA, DeviceConfig, DeviceIdentity, ExposureSetting,
    ListenConfig, RecordingEncoding, SecurityConfig, Settings, StorageConfig, save_settings_atomic,
};
use miro_ego_core::ids::{hex_encode, is_valid_uuid_v4, new_uuid_v4, new_uuid_v7};
use miro_ego_core::locks::{LockMode, acquire_network_operation_lock};
use miro_ego_core::paths::{DataVolumePaths, RECORDING_VOLUME_LABEL, SYSTEM_RUN_DIR};
use miro_ego_core::sys::clock::monotonic_ns;
use miro_ego_core::sys::fs::write_bytes_atomic;
use miro_ego_deploy::{
    BUNDLE_SCHEMA, BUNDLE_SIGNATURE_FILENAME, BundleFileEntry, BundleManifest,
    DEFAULT_DEPLOY_SOCKET_PATH, DeployRpcServer, DeployRpcServerConfig, ReleaseManager,
    ReleaseManagerPaths, TARGET_PLATFORM, default_release_signing_seed,
    sign_bundle_manifest_with_seed, verify_bundle_dir,
};
use miro_ego_hw::{CaptureToggleAction, CaptureToggleClient};
use miro_ego_network::{
    ApplyDesiredState, AuthoritativeNetworkController, CommandOutput, CommandRunner,
    ControllerConfig, DEFAULT_AVAHI_SERVICES_DIR, DesiredMode, MdnsScheme, MockSystemClockBackend,
    NetworkError, NetworkResult, NetworkRpcClient, NetworkRpcServer, NetworkStateStore,
    NmcliBackend, RpcServerConfig, derive_rescue_identity,
};
use miro_ego_storage::catalog::{GatewayValidatorInfo, GatewayVerificationDiagnostic};
use miro_ego_storage::coordinator::CAMERA_FOCUS_SCHEMA;
use miro_ego_storage::{
    ActiveCaptureHandle, AuditConfig, AuditLogger, BackendStartRequest, BinarySessionCatalog,
    CameraFocusSetRequest, CameraFocusStatus, CaptureBackend, CaptureCoordinator,
    CaptureProgressSnapshot, CapturedArtifact, CapturedAudioOutcome, CapturedSessionArtifacts,
    CapturedVideoSegment, CoordinatorConfig, ExfatVolumeManager, GatewayVerification, LogStore,
    RuntimeObservation, SessionCatalog, StopReason, StorageError, StorageResult,
    VOLUME_METADATA_SCHEMA, VolumePolicy,
};
use rustls::pki_types::{CertificateDer, ServerName};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::os::linux::net::SocketAddrExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{SocketAddr as UnixSocketAddr, UnixDatagram, UnixListener};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_rustls::TlsConnector;

fn temp_test_dir(tag: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "miro-ego-wave-b-{tag}-{}-{unique}",
        std::process::id()
    ));
    fs::create_dir_all(&dir).expect("create temp test dir");
    dir
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex_encode(&h.finalize())
}

fn init_test_volume(data_dir: &Path) -> String {
    let paths = DataVolumePaths::new(data_dir.to_path_buf());
    paths.ensure_layout().expect("ensure volume layout");
    let volume_id = new_uuid_v4();
    let meta = json!({
        "schema": VOLUME_METADATA_SCHEMA,
        "volume_id": volume_id,
        "label": RECORDING_VOLUME_LABEL,
    });
    let mut raw = serde_json::to_vec(&meta).expect("serialize volume meta");
    raw.push(b'\n');
    write_bytes_atomic(&paths.volume_metadata_file(), &raw, 0o640, true)
        .expect("write volume.json");
    volume_id
}

fn make_lab_device_config(data_dir: &Path, state_dir: &Path) -> DeviceConfig {
    DeviceConfig {
        schema: DEVICE_CONFIG_SCHEMA.to_owned(),
        listen: ListenConfig {
            host: "127.0.0.1".to_owned(),
            port: 8080,
        },
        camera: CameraConfig {
            device: PathBuf::from("/dev/video7"),
            width: 64,
            height: 32,
            fps: 30,
            frame_decimation: 2,
            exposure_time_absolute: Some(ExposureSetting::default()),
        },
        storage: StorageConfig {
            mountpoint: data_dir.to_path_buf(),
            minimum_available_bytes: 1024 * 1024,
            minimum_available_inodes: 16,
        },
        state_root: state_dir.to_path_buf(),
        device: DeviceIdentity {
            device_id: new_uuid_v4(),
            device_label: "Miro-Ego-ABCDEF01".to_owned(),
            hardware_fingerprint: format!("sha256:{}", "b".repeat(64)),
        },
        security: SecurityConfig::Lab {
            isolated_network: true,
        },
        audio: AudioConfig {
            enabled: false,
            ..AudioConfig::default()
        },
        recording: RecordingEncoding {
            gop_frames: 15,
            ..RecordingEncoding::default()
        },
        segment_seconds: 1.0,
    }
}

async fn http_request(
    addr: SocketAddr,
    method: Method,
    path: &str,
    extra_headers: &[(&str, &str)],
    body: Option<Vec<u8>>,
) -> (u16, HeaderMap, Vec<u8>) {
    let stream = TcpStream::connect(addr)
        .await
        .expect("connect to gateway TCP");
    let (mut sender, conn) = http1::handshake(TokioIo::new(stream))
        .await
        .expect("HTTP/1.1 handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("Host", addr.to_string());
    for (k, v) in extra_headers {
        builder = builder.header(*k, *v);
    }
    let payload = body.unwrap_or_default();
    let req = builder
        .body(Full::new(Bytes::from(payload)))
        .expect("build HTTP request");
    let resp = sender.send_request(req).await.expect("send HTTP request");
    let status = resp.status().as_u16();
    let headers = resp.headers().clone();
    let collected = resp
        .into_body()
        .collect()
        .await
        .expect("read HTTP response body")
        .to_bytes()
        .to_vec();
    (status, headers, collected)
}

async fn https_request_full(
    addr: SocketAddr,
    cert_der: CertificateDer<'static>,
    method: Method,
    path: &str,
    extra_headers: &[(&str, &str)],
    body: Option<Vec<u8>>,
) -> (u16, HeaderMap, Vec<u8>) {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert_der).expect("add self-signed cert");
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let tls_cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .expect("TLS versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(tls_cfg));
    let tcp = TcpStream::connect(addr).await.expect("connect HTTPS TCP");
    let server_name = ServerName::try_from("localhost").expect("server name");
    let tls_stream = connector
        .connect(server_name, tcp)
        .await
        .expect("TLS handshake");
    let (mut sender, conn) = http1::handshake(TokioIo::new(tls_stream))
        .await
        .expect("HTTPS HTTP/1.1 handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("Host", format!("localhost:{}", addr.port()));
    for (k, v) in extra_headers {
        builder = builder.header(*k, *v);
    }
    let payload = body.unwrap_or_default();
    let req = builder
        .body(Full::new(Bytes::from(payload)))
        .expect("build HTTPS request");
    let resp = sender.send_request(req).await.expect("send HTTPS request");
    let status = resp.status().as_u16();
    let headers = resp.headers().clone();
    let collected = resp
        .into_body()
        .collect()
        .await
        .expect("read HTTPS body")
        .to_bytes()
        .to_vec();
    (status, headers, collected)
}

fn build_fake_aarch64_elf(tag: &str) -> Vec<u8> {
    let mut elf = vec![0u8; 128];
    elf[0..4].copy_from_slice(&[0x7f, b'E', b'L', b'F']);
    elf[4] = 2; // ELFCLASS64
    elf[5] = 1; // ELFDATA2LSB
    elf[6] = 1; // EV_CURRENT
    elf[16..18].copy_from_slice(&3u16.to_le_bytes()); // ET_DYN
    elf[18..20].copy_from_slice(&183u16.to_le_bytes()); // EM_AARCH64
    elf[20..24].copy_from_slice(&1u32.to_le_bytes()); // EV_CURRENT
    let tag_bytes = tag.as_bytes();
    let copy_len = tag_bytes.len().min(60);
    elf[64..64 + copy_len].copy_from_slice(&tag_bytes[..copy_len]);
    elf
}

fn create_synthetic_release_bundle(bundle_dir: &Path, version: &str, commit: &str) {
    let bin_dir = bundle_dir.join("bin");
    fs::create_dir_all(&bin_dir).unwrap();

    let miro_bin = build_fake_aarch64_elf(&format!("miro-ego:{version}:{commit}"));
    let enc_bin = build_fake_aarch64_elf(&format!("miro-ego-encoder:{version}:{commit}"));

    let miro_path = bin_dir.join("miro-ego");
    let enc_path = bin_dir.join("miro-ego-encoder");
    fs::write(&miro_path, &miro_bin).unwrap();
    fs::write(&enc_path, &enc_bin).unwrap();
    fs::set_permissions(&miro_path, fs::Permissions::from_mode(0o755)).unwrap();
    fs::set_permissions(&enc_path, fs::Permissions::from_mode(0o755)).unwrap();

    let manifest = BundleManifest {
        schema: BUNDLE_SCHEMA.to_owned(),
        platform: TARGET_PLATFORM.to_owned(),
        version: version.to_owned(),
        commit: commit.to_owned(),
        binaries: vec![
            BundleFileEntry {
                path: "bin/miro-ego".to_owned(),
                bytes: miro_bin.len() as u64,
                sha256: sha256_hex(&miro_bin),
                executable: true,
            },
            BundleFileEntry {
                path: "bin/miro-ego-encoder".to_owned(),
                bytes: enc_bin.len() as u64,
                sha256: sha256_hex(&enc_bin),
                executable: true,
            },
        ],
        assets: Vec::new(),
    };
    let mut raw = serde_json::to_vec_pretty(&manifest).unwrap();
    raw.push(b'\n');
    let sig_hex = sign_bundle_manifest_with_seed(&raw, &default_release_signing_seed());
    fs::write(bundle_dir.join("bundle.json"), &raw).unwrap();
    fs::write(
        bundle_dir.join(BUNDLE_SIGNATURE_FILENAME),
        format!("{sig_hex}\n").as_bytes(),
    )
    .unwrap();
    verify_bundle_dir(bundle_dir).expect("synthetic bundle must pass verify_bundle_dir");
}

// ---------------------------------------------------------------------------
// 1. Native defaults, `--run-dir` non-root fallback, LED config, & Settings validation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_defaults_h264_preview_disabled_and_relaxed_run_dir_fallback() {
    let root = temp_test_dir("native-defaults");
    let data_dir = root.join("data");
    let state_dir = root.join("state");
    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&state_dir).unwrap();
    let _ = init_test_volume(&data_dir);

    let cfg = make_lab_device_config(&data_dir, &state_dir);
    let prod_opts = DaemonServeOptions::from_device_config(cfg.clone(), false);
    assert!(
        !prod_opts.enable_h264_preview,
        "Native (!mock) mode must default enable_h264_preview to false"
    );
    match &prod_opts.capture_mode {
        CaptureModeKind::Native { imu_device, .. } => {
            assert_eq!(
                imu_device.as_deref(),
                Some(Path::new("/dev/video7")),
                "Native mode must default imu_device to Some(device_config.camera.device)"
            );
        }
        other => panic!("expected CaptureModeKind::Native, got {other:?}"),
    }
    assert_eq!(
        prod_opts.deploy_socket.as_deref(),
        Some(Path::new(DEFAULT_DEPLOY_SOCKET_PATH))
    );

    // Non-root `--relaxed-volume` with `/run/miro-ego` falls back to `<state_root>/run` and
    // `GET /api/v1/preview/h264` returns 404 `preview_h264_unsupported` when disabled.
    let mut opts = DaemonServeOptions::from_device_config(cfg, true);
    opts.enable_h264_preview = false;
    opts.relaxed_volume = true;
    opts.run_dir = Some(PathBuf::from(SYSTEM_RUN_DIR));
    opts.listen_override = Some("127.0.0.1:0".parse().unwrap());
    opts.network_socket = None;

    let bound = bind_daemon(opts)
        .await
        .expect("bind daemon with fallback run_dir");
    let addr = bound.server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        run_bound_daemon_with_shutdown(bound, async {
            let _ = shutdown_rx.await;
        })
        .await
    });

    let (code, headers, body) =
        http_request(addr, Method::GET, "/api/v1/preview/h264", &[], None).await;
    assert_eq!(code, 404);
    assert_eq!(
        headers
            .get("Miro-Ego-Error-Code")
            .and_then(|v| v.to_str().ok()),
        Some("preview_h264_unsupported")
    );
    let problem: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(problem["error"]["code"], "preview_h264_unsupported");

    let (dev_code, _, dev_body) =
        http_request(addr, Method::GET, "/api/v1/device", &[], None).await;
    assert_eq!(dev_code, 200);
    let dev_val: Value = serde_json::from_slice(&dev_body).unwrap();
    assert_eq!(dev_val["capabilities"]["preview_h264"], false);

    let _ = shutdown_tx.send(());
    task.await.unwrap().unwrap();
    let _ = fs::remove_dir_all(&root);
}

#[test]
fn settings_validate_for_device_and_button_led_config_wiring() {
    let root = temp_test_dir("settings-and-led");
    let data_dir = root.join("data");
    let state_dir = root.join("state");
    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&state_dir).unwrap();

    let mut cfg = make_lab_device_config(&data_dir, &state_dir);
    assert_eq!(cfg.camera.frame_decimation, 2);

    // fps = 25 is valid standalone (1..=60) but NOT divisible by frame_decimation = 2!
    let mut incompatible = Settings::from_device_config(&cfg);
    incompatible.camera.fps = 25;
    let bad_path = root.join("bad-settings.json");
    save_settings_atomic(&bad_path, &incompatible).unwrap();

    let err = apply_settings_to_device_config(&mut cfg, &incompatible)
        .expect_err("incompatible fps % frame_decimation must be rejected");
    assert!(err.contains("frame_decimation"), "unexpected error: {err}");
    assert!(
        load_and_apply_settings_for_device(&mut cfg, &bad_path).is_err(),
        "load_and_apply_settings_for_device must enforce validate_for_device"
    );

    // Compatible settings (fps = 60, frame_decimation = 2 -> encoded 30 fps) succeed.
    let mut good = Settings::from_device_config(&cfg);
    good.camera.fps = 60;
    good.audio.enabled = true;
    let good_path = root.join("good-settings.json");
    save_settings_atomic(&good_path, &good).unwrap();
    assert!(load_and_apply_settings_for_device(&mut cfg, &good_path).unwrap());
    assert_eq!(cfg.camera.fps, 60);
    assert!(cfg.audio.enabled);

    // LED resolution from button.json:
    let missing_led = resolve_serve_led_config(&root.join("missing-button.json"));
    assert_eq!(missing_led, Some(("ACT".to_owned(), false)));

    let custom_btn = root.join("button.json");
    fs::write(
        &custom_btn,
        serde_json::to_vec_pretty(&json!({
            "schema": "miro-ego.button-config.v1",
            "enabled": true,
            "physical_pin": 37,
            "active_low": true,
            "debounce_ms": 30,
            "double_press_window_ms": 350,
            "long_press_ms": 1200,
            "cooldown_ms": 200,
            "poll_ms": 10,
            "request_timeout_ms": 2000,
            "status_led": "green:status",
            "status_led_inverted": true
        }))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        resolve_serve_led_config(&custom_btn),
        Some(("green:status".to_owned(), true))
    );

    let disabled_led_btn = root.join("button-no-led.json");
    fs::write(
        &disabled_led_btn,
        serde_json::to_vec_pretty(&json!({
            "schema": "miro-ego.button-config.v1",
            "enabled": true,
            "physical_pin": 37,
            "status_led": null
        }))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(resolve_serve_led_config(&disabled_led_btn), None);

    let _ = fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// 2. Mid-recording failure propagation, Drop/abort cleanup, SSE, & lock release
// ---------------------------------------------------------------------------

struct CrashingCaptureBackend {
    camera_connected: Arc<AtomicBool>,
    inject_failure: Arc<Mutex<Option<(String, String)>>>,
    abort_count: Arc<AtomicUsize>,
    drop_cleanup_count: Arc<AtomicUsize>,
    audio_not_recorded_reason: String,
    audio_requested_mode: String,
}

struct CrashingHandle {
    staging_dir: PathBuf,
    session_id: String,
    started_ns: u64,
    inject_failure: Arc<Mutex<Option<(String, String)>>>,
    abort_count: Arc<AtomicUsize>,
    drop_cleanup_count: Arc<AtomicUsize>,
    audio_not_recorded_reason: String,
    audio_requested_mode: String,
    completed: bool,
}

impl Drop for CrashingHandle {
    fn drop(&mut self) {
        if !self.completed {
            self.completed = true;
            self.drop_cleanup_count.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl ActiveCaptureHandle for CrashingHandle {
    fn progress(&self) -> CaptureProgressSnapshot {
        CaptureProgressSnapshot {
            elapsed_seconds: 0.25,
            captured_frames: 5,
            bytes_written: 4096,
        }
    }

    fn take_failure(&self) -> Option<(String, String)> {
        self.inject_failure.lock().ok().and_then(|mut g| g.take())
    }

    fn finish(mut self: Box<Self>, _reason: StopReason) -> StorageResult<CapturedSessionArtifacts> {
        self.completed = true;
        let video_dir = self.staging_dir.join("video");
        fs::create_dir_all(&video_dir).unwrap();
        let mp4 = b"\x00\x00\x00\x18ftypisom\x00\x00\x02\x00isomiso6\x00\x00\x00\x08moov";
        let sha = sha256_hex(mp4);
        fs::write(video_dir.join("left_00000.mp4"), mp4).unwrap();
        fs::write(video_dir.join("right_00000.mp4"), mp4).unwrap();

        let frames_bytes = b"{\"sequence\":0,\"source_sequence\":1,\"timestamp_ns\":1000,\"segment_index\":0,\"segment_frame_index\":0}\n";
        let imu_bytes = b"{\"sequence\":0,\"packet_sequence\":0,\"sample_index\":0,\"device_timestamp_raw\":0,\"device_ticks\":0,\"host_read_start_ns\":1000,\"host_read_end_ns\":1100,\"host_monotonic_ns\":1050,\"accelerometer\":[0,0,16384],\"gyroscope\":[0,0,0],\"sync_offset_ns\":0,\"sync_residual_ns\":0,\"sync_quality\":\"good\"}\n";
        fs::write(self.staging_dir.join("frames.ndjson"), frames_bytes).unwrap();
        fs::write(self.staging_dir.join("imu.ndjson"), imu_bytes).unwrap();
        let _ = &self.session_id;
        let _ = self.started_ns;

        Ok(CapturedSessionArtifacts {
            segments: vec![CapturedVideoSegment {
                index: 0,
                start_frame: 0,
                end_frame: 1,
                start_time_seconds: 0.0,
                end_time_seconds: 0.1,
                left_path: "video/left_00000.mp4".to_owned(),
                left_bytes: mp4.len() as u64,
                left_sha256: Some(sha.clone()),
                right_path: "video/right_00000.mp4".to_owned(),
                right_bytes: mp4.len() as u64,
                right_sha256: Some(sha),
            }],
            frames_artifact: CapturedArtifact {
                role: "frames.index".to_owned(),
                path: "frames.ndjson".to_owned(),
                media_type: "application/x-ndjson".to_owned(),
                bytes: frames_bytes.len() as u64,
                sha256: Some(sha256_hex(frames_bytes)),
            },
            frames_count: 1,
            imu_artifact: CapturedArtifact {
                role: "imu.samples".to_owned(),
                path: "imu.ndjson".to_owned(),
                media_type: "application/x-ndjson".to_owned(),
                bytes: imu_bytes.len() as u64,
                sha256: Some(sha256_hex(imu_bytes)),
            },
            imu_sample_count: 1,
            audio: CapturedAudioOutcome::NotRecorded {
                requested_mode: self.audio_requested_mode.clone(),
                reason: self.audio_not_recorded_reason.clone(),
            },
            logs: Vec::new(),
            dropped_frames: 0,
            drop_events: Vec::new(),
            duration_seconds: 0.1,
            effective_fps: 15.0,
        })
    }

    fn abort(mut self: Box<Self>) {
        self.completed = true;
        self.abort_count.fetch_add(1, Ordering::SeqCst);
    }
}

impl CaptureBackend for CrashingCaptureBackend {
    fn is_camera_connected(&self) -> bool {
        self.camera_connected.load(Ordering::Acquire)
    }

    fn camera_focus(&self) -> StorageResult<Option<CameraFocusStatus>> {
        if !self.is_camera_connected() {
            return Err(StorageError::camera_not_connected("camera disconnected"));
        }
        Ok(Some(CameraFocusStatus {
            schema: CAMERA_FOCUS_SCHEMA.to_owned(),
            value: 100,
            minimum: 0,
            maximum: 1023,
            step: 1,
            default: 100,
            auto_supported: true,
            auto_enabled: Some(false),
        }))
    }

    fn set_camera_focus(&self, req: &CameraFocusSetRequest) -> StorageResult<CameraFocusStatus> {
        if !self.is_camera_connected() {
            return Err(StorageError::camera_not_connected("camera disconnected"));
        }
        Ok(CameraFocusStatus {
            schema: CAMERA_FOCUS_SCHEMA.to_owned(),
            value: req.value.unwrap_or(100),
            minimum: 0,
            maximum: 1023,
            step: 1,
            default: 100,
            auto_supported: true,
            auto_enabled: req.auto_enabled.or(Some(false)),
        })
    }

    fn begin_capture(
        &self,
        req: &BackendStartRequest,
    ) -> StorageResult<Box<dyn ActiveCaptureHandle>> {
        if !self.is_camera_connected() {
            return Err(StorageError::camera_not_connected("camera disconnected"));
        }
        Ok(Box::new(CrashingHandle {
            staging_dir: req.staging_dir.clone(),
            session_id: req.session_id.clone(),
            started_ns: req.started_monotonic_ns,
            inject_failure: Arc::clone(&self.inject_failure),
            abort_count: Arc::clone(&self.abort_count),
            drop_cleanup_count: Arc::clone(&self.drop_cleanup_count),
            audio_not_recorded_reason: self.audio_not_recorded_reason.clone(),
            audio_requested_mode: self.audio_requested_mode.clone(),
            completed: false,
        }))
    }

    fn runtime_observation(&self) -> RuntimeObservation {
        let connected = self.is_camera_connected();
        RuntimeObservation {
            observed_at: "2026-11-26T10:00:00Z".to_owned(),
            connection_method: "offline".to_owned(),
            temperature_celsius: 0.0,
            network: json!({
                "ap": { "state": "unavailable", "interface": null, "addresses": [], "peer_or_ssid": null },
                "wifi_client": { "state": "unavailable", "interface": null, "addresses": [], "peer_or_ssid": null },
                "wired": { "state": "unavailable", "interface": null, "addresses": [], "peer_or_ssid": null },
                "default_route": "none"
            }),
            live_imu: None,
            camera_connected: connected,
            camera_focus: None,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn supervisor_polls_mid_recording_failure_emits_sse_releases_locks_and_checks_preview_camera()
{
    let root = temp_test_dir("supervisor-failure");
    let data_dir = root.join("data");
    let state_dir = root.join("state");
    let run_dir = root.join("run");
    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&state_dir).unwrap();
    fs::create_dir_all(&run_dir).unwrap();
    let _ = init_test_volume(&data_dir);

    let cfg = make_lab_device_config(&data_dir, &state_dir);
    let volume_paths = DataVolumePaths::new(data_dir.clone());
    let volume = Arc::new(ExfatVolumeManager::new(
        volume_paths.clone(),
        VolumePolicy::relaxed_for_tests(1024 * 1024),
    ));
    let catalog = Arc::new(BinarySessionCatalog::open(volume_paths.clone()).unwrap());
    let vol_audit = Arc::clone(&volume);
    let audit = Arc::new(AuditLogger::new(
        volume_paths.clone(),
        AuditConfig::default(),
        Arc::new(move || vol_audit.is_mounted_and_writable()),
    ));
    let vol_logs = Arc::clone(&volume);
    let logs = Arc::new(LogStore::new(
        volume_paths,
        Arc::new(move || vol_logs.is_mounted_and_writable()),
    ));

    let camera_connected = Arc::new(AtomicBool::new(true));
    let inject_failure = Arc::new(Mutex::new(None));
    let abort_count = Arc::new(AtomicUsize::new(0));
    let drop_cleanup_count = Arc::new(AtomicUsize::new(0));

    let raw_backend: Arc<dyn CaptureBackend> = Arc::new(CrashingCaptureBackend {
        camera_connected: Arc::clone(&camera_connected),
        inject_failure: Arc::clone(&inject_failure),
        abort_count: Arc::clone(&abort_count),
        drop_cleanup_count: Arc::clone(&drop_cleanup_count),
        audio_not_recorded_reason: "audio_open_failed".to_owned(),
        audio_requested_mode: "enabled".to_owned(),
    });
    let locked_backend: Arc<dyn CaptureBackend> =
        Arc::new(NetworkLockCaptureBackend::new(raw_backend, run_dir.clone()));

    let coord_cfg = CoordinatorConfig {
        device_id: cfg.device.device_id.clone(),
        device_label: cfg.device.device_label.clone(),
        hardware_fingerprint: cfg.device.hardware_fingerprint.clone(),
        platform: "rdk-x5".to_owned(),
        software_version: "0.3.0".to_owned(),
        commit: "0".repeat(40),
        timezone: "Etc/UTC".to_owned(),
        camera_width: cfg.camera.width,
        camera_height: cfg.camera.height,
        sensor_fps: f64::from(cfg.camera.fps),
        frame_decimation: cfg.camera.frame_decimation,
        segment_frames: 15,
        encoding: cfg.recording.clone(),
        run_dir: Some(run_dir.clone()),
        timer_settings_path: Some(state_dir.join("capture-timer.json")),
    };
    let coordinator = Arc::new(CaptureCoordinator::new(
        coord_cfg,
        Arc::clone(&volume),
        Arc::clone(&catalog),
        Arc::clone(&locked_backend),
    ));

    let mjpeg_hub = Arc::new(MjpegPreviewHub::new(15));
    let _ = mjpeg_hub.publish(Bytes::from_static(&[0xFF, 0xD8, 0xFF, 0xD9]));
    let mut backend_cfg = ProductionBackendConfig::from_device_config(cfg.clone());
    backend_cfg.network_socket = None;
    let backend = Arc::new(
        ProductionGatewayBackend::new(
            backend_cfg,
            Arc::clone(&coordinator),
            Arc::clone(&volume),
            Arc::clone(&catalog),
            Arc::clone(&audit),
            Arc::clone(&logs),
            Arc::clone(&mjpeg_hub),
            None,
        )
        .with_capture_backend(Arc::clone(&locked_backend)),
    );

    let server = GatewayServer::bind(
        GatewayServerConfig::http("127.0.0.1:0".parse().unwrap()),
        Arc::clone(&backend) as Arc<dyn GatewayBackend>,
        Arc::new(SecurityPolicy::lab_default()),
    )
    .await
    .unwrap();
    let addr = server.local_addr();
    let origin = format!("http://{addr}");

    let daemon = miro_ego_cli::runtime::BoundDaemon {
        server,
        backend: Arc::clone(&backend),
        reserve_poll_interval: Duration::from_millis(200),
        timer_poll_interval: Duration::from_millis(60),
    };
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let daemon_task = tokio::spawn(async move {
        run_bound_daemon_with_shutdown(daemon, async {
            let _ = shutdown_rx.await;
        })
        .await
    });

    // Verify capture timer is DISABLED by default, yet supervisor still polls active failure!
    let (t_code, _, t_body) =
        http_request(addr, Method::GET, "/api/v1/capture/timer", &[], None).await;
    assert_eq!(t_code, 200);
    let timer_json: Value = serde_json::from_slice(&t_body).unwrap();
    assert_eq!(timer_json["settings"]["enabled"], false);

    // Start capture:
    let start_key = new_uuid_v4();
    let start_payload = serde_json::to_vec(&json!({
        "schema": "miro-ego.capture-start.v1",
        "mode": "production",
        "take": { "kind": "new" }
    }))
    .unwrap();
    let (s_code, _, s_body) = http_request(
        addr,
        Method::POST,
        "/api/v1/capture/start",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &start_key),
        ],
        Some(start_payload),
    )
    .await;
    assert_eq!(
        s_code,
        202,
        "start failed: {}",
        String::from_utf8_lossy(&s_body)
    );
    let started_status: Value = serde_json::from_slice(&s_body).unwrap();
    let crashed_sid =
        started_status["snapshot"]["active_recording"]["recording_state"]["session_id"]
            .as_str()
            .unwrap()
            .to_owned();

    // While active, exclusive `network-operation.lock` MUST be contended (`LockBusy`)!
    let lock_attempt = acquire_network_operation_lock(&run_dir, LockMode::Exclusive, true);
    assert!(
        matches!(lock_attempt, Err(CoreError::LockBusy { .. })),
        "shared network-operation.lock must be held during active capture"
    );

    // Inject mid-recording encoder crash into `take_failure()` without calling `/capture/stop`!
    *inject_failure.lock().unwrap() = Some((
        "encoder_crashed".to_owned(),
        "simulated mid-recording SHM encoder crash".to_owned(),
    ));

    // Wait for the daemon's periodic supervisor loop (`poll_active_failure()`) to settle to `idle`.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut final_status = Value::Null;
    while Instant::now() < deadline {
        let (code, _, body) =
            http_request(addr, Method::GET, "/api/v1/capture/status", &[], None).await;
        assert_eq!(code, 200);
        let st: Value = serde_json::from_slice(&body).unwrap();
        if st["snapshot"]["device_state"] == "idle" {
            final_status = st;
            break;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    assert_eq!(
        final_status["snapshot"]["device_state"], "idle",
        "supervisor loop must poll_active_failure() and return daemon to idle"
    );
    assert_eq!(
        final_status["snapshot"]["retained_unsuccessful"]["recording_state"]["session_id"],
        crashed_sid
    );
    assert_eq!(
        final_status["snapshot"]["retained_unsuccessful"]["recording_state"]["state"],
        "failed"
    );
    assert!(
        abort_count.load(Ordering::SeqCst) >= 1,
        "handle.abort() must be invoked on mid-recording failure"
    );

    // Exclusive `network-operation.lock` must now be released!
    let lock_after = acquire_network_operation_lock(&run_dir, LockMode::Exclusive, true)
        .expect("network-operation.lock must be released after failure cleanup");
    drop(lock_after);

    // SSE replay buffer must have recorded the state/diagnostic events!
    let sse_deadline = Instant::now() + Duration::from_secs(2);
    let mut saw_failure_sse = false;
    while Instant::now() < sse_deadline {
        let replayed_events = backend
            .capture_event_buffer()
            .replay_or_snapshot(Some(miro_ego_api::EventCursor::Exact(1)), || {
                Ok(json!({
                    "authority_epoch": new_uuid_v4(),
                    "source_revision": 0,
                    "type": "snapshot",
                    "occurred_at": "2026-11-26T10:00:00Z",
                    "session_id": Value::Null,
                    "data": {}
                }))
            })
            .unwrap();
        if replayed_events
            .iter()
            .any(|ev| ev.event_type == "diagnostic" || ev.event_type == "state")
        {
            saw_failure_sse = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        saw_failure_sse,
        "SSE replay buffer must contain supervisor-emitted failure events"
    );

    // Also verify Drop cleanup when an unconsumed handle is dropped directly:
    let dummy_req = BackendStartRequest {
        session_id: new_uuid_v7(),
        take_id: new_uuid_v7(),
        mode: miro_ego_storage::CaptureMode::Production,
        staging_dir: root.join("drop-staging"),
        started_at: "2026-11-26T10:00:00Z".to_owned(),
        encoding: cfg.recording.clone(),
        started_monotonic_ns: monotonic_ns().unwrap_or(1),
    };
    let unconsumed_handle = locked_backend.begin_capture(&dummy_req).unwrap();
    assert!(
        acquire_network_operation_lock(&run_dir, LockMode::Exclusive, true).is_err(),
        "handle holds shared network-operation.lock before Drop"
    );
    drop(unconsumed_handle);
    assert!(
        abort_count.load(Ordering::SeqCst) >= 2,
        "Drop on NetworkLockedActiveHandle must invoke inner.abort()"
    );
    let lock_after_drop = acquire_network_operation_lock(&run_dir, LockMode::Exclusive, true)
        .expect("Drop must release shared network-operation.lock");
    drop(lock_after_drop);

    // Verify `audio_open_failed` session seals cleanly AND passes `validate_session_dir` JSON Schema!
    let ok_start_key = new_uuid_v4();
    let (code, _, _) = http_request(
        addr,
        Method::POST,
        "/api/v1/capture/start",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &ok_start_key),
        ],
        Some(
            serde_json::to_vec(&json!({
                "schema": "miro-ego.capture-start.v1",
                "mode": "production",
                "take": { "kind": "new" }
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(code, 202);

    let ok_stop_key = new_uuid_v4();
    let (code, _, stop_body) = http_request(
        addr,
        Method::POST,
        "/api/v1/capture/stop",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &ok_stop_key),
        ],
        Some(
            serde_json::to_vec(&json!({
                "schema": "miro-ego.capture-stop.v1",
                "reason": "user"
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        code,
        202,
        "stop failed: {}",
        String::from_utf8_lossy(&stop_body)
    );

    let sessions = catalog.list_sessions().unwrap();
    assert_eq!(sessions.len(), 1);
    let sealed_sid = &sessions[0].session_id;
    let indexed = catalog.get_indexed_session(sealed_sid).unwrap().unwrap();
    assert_eq!(
        indexed.verification.as_ref().map(|v| v.verdict.as_str()),
        Some("usable"),
        "newly sealed session must carry usable gateway verification verdict"
    );
    let session_dir = data_dir.join("recordings").join(&indexed.relative_dir);
    let report = validate_session_dir(&session_dir)
        .expect("manifest with audio.reason == audio_open_failed must pass JSON Schema validation");
    assert_eq!(report["valid"], true);
    let manifest_raw: Value =
        serde_json::from_slice(&fs::read(session_dir.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest_raw["audio"]["reason"], "audio_open_failed");

    // Verify preview returns `503 camera_not_connected` when camera disconnects!
    camera_connected.store(false, Ordering::Release);
    let (p_code, p_headers, p_body) =
        http_request(addr, Method::GET, "/api/v1/preview", &[], None).await;
    assert_eq!(p_code, 503);
    assert_eq!(
        p_headers
            .get("Miro-Ego-Error-Code")
            .and_then(|v| v.to_str().ok()),
        Some("camera_not_connected")
    );
    let p_err: Value = serde_json::from_slice(&p_body).unwrap();
    assert_eq!(p_err["error"]["code"], "camera_not_connected");

    let _ = shutdown_tx.send(());
    daemon_task.await.unwrap().unwrap();
    let _ = fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// 3. Principal-scoped idempotency isolation across 2 Customer principals
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn principal_scoped_coordinator_calls_isolate_idempotency_keys_across_principals() {
    let root = temp_test_dir("principal-idempotency");
    let data_dir = root.join("data");
    let state_dir = root.join("state");
    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&state_dir).unwrap();
    let _ = init_test_volume(&data_dir);

    let cfg = make_lab_device_config(&data_dir, &state_dir);
    let mut options = DaemonServeOptions::from_device_config(cfg.clone(), true);
    options.listen_override = Some("127.0.0.1:0".parse().unwrap());
    options.network_socket = None;

    let bound = bind_daemon(options).await.unwrap();
    let backend = Arc::clone(&bound.backend);
    drop(bound.server);

    let mut tokens = HashMap::new();
    tokens.insert(
        "tok-alpha-secret-12345678".to_owned(),
        Principal::all_v1_operations("principal-alpha").unwrap(),
    );
    tokens.insert(
        "tok-beta-secret-87654321".to_owned(),
        Principal::all_v1_operations("principal-beta").unwrap(),
    );
    let policy = Arc::new(SecurityPolicy::customer(tokens, HashSet::new(), None).unwrap());
    let server = GatewayServer::bind(
        GatewayServerConfig::http("127.0.0.1:0".parse().unwrap()),
        backend as Arc<dyn GatewayBackend>,
        policy,
    )
    .await
    .unwrap();
    let addr = server.local_addr();
    let origin = format!("http://{addr}");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let srv_task = tokio::spawn(async move {
        server
            .serve_with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    // Shared Idempotency-Key used by BOTH principals with DIFFERENT focus values!
    let shared_key = new_uuid_v4();
    let (c1, _, b1) = http_request(
        addr,
        Method::POST,
        "/api/v1/camera/focus",
        &[
            ("Authorization", "Bearer tok-alpha-secret-12345678"),
            ("X-CSRF-Token", "tok-alpha-secret-12345678"),
            ("Origin", &origin),
            ("Content-Type", "application/json"),
            ("Idempotency-Key", &shared_key),
        ],
        Some(
            serde_json::to_vec(&json!({
                "schema": "miro-ego.camera-focus-set.v1",
                "value": 300
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        c1,
        200,
        "alpha focus failed: {}",
        String::from_utf8_lossy(&b1)
    );

    let (c2, _, b2) = http_request(
        addr,
        Method::POST,
        "/api/v1/camera/focus",
        &[
            ("Authorization", "Bearer tok-beta-secret-87654321"),
            ("X-CSRF-Token", "tok-beta-secret-87654321"),
            ("Origin", &origin),
            ("Content-Type", "application/json"),
            ("Idempotency-Key", &shared_key),
        ],
        Some(
            serde_json::to_vec(&json!({
                "schema": "miro-ego.camera-focus-set.v1",
                "value": 600
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        c2,
        200,
        "beta with same Idempotency-Key and different payload must NOT collide with alpha: {}",
        String::from_utf8_lossy(&b2)
    );
    let beta_focus: Value = serde_json::from_slice(&b2).unwrap();
    assert_eq!(beta_focus["value"], 600);

    // Same principal (`alpha`) reusing `shared_key` with different payload MUST get 409!
    let (c3, _, _) = http_request(
        addr,
        Method::POST,
        "/api/v1/camera/focus",
        &[
            ("Authorization", "Bearer tok-alpha-secret-12345678"),
            ("X-CSRF-Token", "tok-alpha-secret-12345678"),
            ("Origin", &origin),
            ("Content-Type", "application/json"),
            ("Idempotency-Key", &shared_key),
        ],
        Some(
            serde_json::to_vec(&json!({
                "schema": "miro-ego.camera-focus-set.v1",
                "value": 900
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(c3, 409);

    let _ = shutdown_tx.send(());
    srv_task.await.unwrap().unwrap();
    let _ = fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// 4. Customer HTTPS echoed-bearer CSRF + `miro-ego button` GPIO 37->401 & non-hanging error exit
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn customer_https_echoed_bearer_csrf_and_button_cli_gpio_mapping_and_fast_error_exit() {
    let root = temp_test_dir("customer-csrf-button");
    let data_dir = root.join("data");
    let state_dir = root.join("state");
    let sec_dir = root.join("security");
    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&state_dir).unwrap();
    fs::create_dir_all(&sec_dir).unwrap();
    let _ = init_test_volume(&data_dir);

    let certified =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
            .expect("generate self-signed cert");
    let cert_pem = certified.cert.pem();
    let key_pem = certified.key_pair.serialize_pem();
    let cert_der = certified.cert.der().clone();

    let cert_path = sec_dir.join("tls-cert.pem");
    let key_path = sec_dir.join("tls-key.pem");
    let token_path = sec_dir.join("bearer.token");
    let bearer = "miro-customer-secret-token-wave-b";
    fs::write(&cert_path, &cert_pem).unwrap();
    fs::write(&key_path, &key_pem).unwrap();
    fs::write(&token_path, format!("{bearer}\n")).unwrap();

    let mut cfg = make_lab_device_config(&data_dir, &state_dir);
    cfg.security = SecurityConfig::Customer {
        isolated_network: true,
        bearer_token_file: token_path.clone(),
        tls_certificate_file: cert_path.clone(),
        tls_private_key_file: key_path,
        principal_id: "customer-owner".to_owned(),
    };

    let mut options = DaemonServeOptions::from_device_config(cfg.clone(), true);
    options.listen_override = Some("127.0.0.1:0".parse().unwrap());
    options.network_socket = None;

    let bound = bind_daemon(options)
        .await
        .expect("bind Customer HTTPS daemon");
    assert!(bound.server.is_tls());
    let addr = bound.server.local_addr();
    cfg.listen.port = addr.port();
    let cfg_path = root.join("device.json");
    fs::write(&cfg_path, serde_json::to_vec_pretty(&cfg).unwrap()).unwrap();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let daemon_task = tokio::spawn(async move {
        run_bound_daemon_with_shutdown(bound, async {
            let _ = shutdown_rx.await;
        })
        .await
    });

    let origin = format!("https://localhost:{}", addr.port());
    let auth = format!("Bearer {bearer}");

    // 1. Customer POST without X-CSRF-Token -> 403
    let (c_missing, _, _) = https_request_full(
        addr,
        cert_der.clone(),
        Method::POST,
        "/api/v1/capture/start",
        &[
            ("Authorization", &auth),
            ("Origin", &origin),
            ("Content-Type", "application/json"),
            ("Idempotency-Key", &new_uuid_v4()),
        ],
        Some(
            serde_json::to_vec(&json!({
                "schema": "miro-ego.capture-start.v1",
                "mode": "production",
                "take": { "kind": "new" }
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(c_missing, 403);

    // 2. Customer POST echoing Bearer token in X-CSRF-Token -> 202 Accepted!
    let (c_ok, _, _) = https_request_full(
        addr,
        cert_der.clone(),
        Method::POST,
        "/api/v1/capture/start",
        &[
            ("Authorization", &auth),
            ("X-CSRF-Token", bearer),
            ("Origin", &origin),
            ("Content-Type", "application/json"),
            ("Idempotency-Key", &new_uuid_v4()),
        ],
        Some(
            serde_json::to_vec(&json!({
                "schema": "miro-ego.capture-start.v1",
                "mode": "production",
                "take": { "kind": "new" }
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        c_ok, 202,
        "Customer POST echoing bearer token as X-CSRF-Token must succeed"
    );

    // 3. `build_button_capture_client` with Customer TLS + echoed bearer toggles capture (`Stopped` -> `Started` -> `Stopped`)!
    let btn_client = build_button_capture_client(Some(&cfg_path), None, 3000)
        .expect("build_button_capture_client must succeed on valid Customer device.json");
    let stop_act = tokio::task::spawn_blocking({
        let c = btn_client.clone();
        move || c.toggle_capture()
    })
    .await
    .unwrap()
    .expect("button toggle_capture stop over pinned Customer HTTPS");
    assert_eq!(stop_act, CaptureToggleAction::Stopped);

    let start_act = tokio::task::spawn_blocking({
        let c = btn_client.clone();
        move || c.toggle_capture()
    })
    .await
    .unwrap()
    .expect("button toggle_capture start over pinned Customer HTTPS");
    assert_eq!(start_act, CaptureToggleAction::Started);

    let stop_act_2 = tokio::task::spawn_blocking(move || btn_client.toggle_capture())
        .await
        .unwrap()
        .expect("button toggle_capture second stop");
    assert_eq!(stop_act_2, CaptureToggleAction::Stopped);

    // 4. `build_button_capture_client` MUST surface missing `bearer_token_file` instead of `.ok()`!
    let broken_cfg_path = root.join("broken-device.json");
    let mut broken_cfg = cfg.clone();
    broken_cfg.security = SecurityConfig::Customer {
        isolated_network: true,
        bearer_token_file: sec_dir.join("does-not-exist.token"),
        tls_certificate_file: cert_path,
        tls_private_key_file: sec_dir.join("tls-key.pem"),
        principal_id: "customer-owner".to_owned(),
    };
    fs::write(
        &broken_cfg_path,
        serde_json::to_vec_pretty(&broken_cfg).unwrap(),
    )
    .unwrap();
    let missing_tok_err = build_button_capture_client(Some(&broken_cfg_path), None, 2000)
        .expect_err("unreadable bearer_token_file must fail closed");
    assert!(
        missing_tok_err.contains("does-not-exist.token") || missing_tok_err.contains("bearer"),
        "unexpected error: {missing_tok_err}"
    );

    // 5. Spawn real `miro-ego button` subprocess with fake sysfs GPIO root:
    //    - physical_pin = 37 -> RDK X5 SoC GPIO 401 (`gpio401`)
    //    - corrupt `gpio401/value` causes immediate non-zero exit WITHOUT `sig_thread.join()` hang!
    let fake_gpio_root = root.join("sysfs-gpio");
    let gpio401_dir = fake_gpio_root.join("gpio401");
    fs::create_dir_all(&gpio401_dir).unwrap();
    fs::write(fake_gpio_root.join("export"), "").unwrap();
    fs::write(gpio401_dir.join("direction"), "out\n").unwrap();
    // Write invalid GPIO value so `read_level()` errors immediately after opening pin 37 -> gpio401!
    fs::write(gpio401_dir.join("value"), "corrupt_gpio_payload\n").unwrap();

    let btn_cfg_path = root.join("button.json");
    fs::write(
        &btn_cfg_path,
        serde_json::to_vec_pretty(&json!({
            "schema": "miro-ego.button-config.v1",
            "enabled": true,
            "physical_pin": 37,
            "active_low": true,
            "debounce_ms": 20,
            "double_press_window_ms": 300,
            "long_press_ms": 1000,
            "cooldown_ms": 100,
            "poll_ms": 10,
            "request_timeout_ms": 1000
        }))
        .unwrap(),
    )
    .unwrap();

    let bin = env!("CARGO_BIN_EXE_miro-ego");
    let start_instant = Instant::now();
    let mut child = Command::new(bin)
        .args([
            "button",
            "--config",
            btn_cfg_path.to_str().unwrap(),
            "--device-config",
            cfg_path.to_str().unwrap(),
        ])
        .env("MIRO_EGO_GPIO_SYSFS_ROOT", &fake_gpio_root)
        .spawn()
        .expect("spawn miro-ego button");

    let mut exited = None;
    while start_instant.elapsed() < Duration::from_secs(4) {
        if let Some(status) = child.try_wait().unwrap() {
            exited = Some(status);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let exit_status = match exited {
        Some(status) => status,
        None => {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "miro-ego button hung on sig_thread.join() instead of exiting immediately on monitor error"
            );
        }
    };
    assert!(
        !exit_status.success(),
        "miro-ego button must exit with non-zero code on GPIO read error"
    );
    assert_eq!(
        fs::read_to_string(gpio401_dir.join("direction"))
            .unwrap()
            .trim(),
        "in",
        "SysfsGpioPin::open_rdk_x5_board_pin(37) must map BOARD pin 37 -> gpio401 and set direction=in"
    );

    let _ = shutdown_tx.send(());
    daemon_task.await.unwrap().unwrap();
    let _ = fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// 5. Privileged `miro-ego deploy serve-rpc` (fd 0 socket activation) + firmware API E2E
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deploy_serve_rpc_fd0_activation_and_firmware_check_update_rollback_e2e() {
    let root = temp_test_dir("deploy-rpc-e2e");
    let data_dir = root.join("data");
    let state_dir = root.join("state");
    let install_root = root.join("opt-miro-ego");
    let config_root = root.join("etc-miro-ego");
    let deploy_run = root.join("deploy-run");
    let sysroot = root.join("sysroot");
    for d in [
        &data_dir,
        &state_dir,
        &install_root,
        &config_root,
        &deploy_run,
        &sysroot,
    ] {
        fs::create_dir_all(d).unwrap();
    }
    let _ = init_test_volume(&data_dir);

    let commit_v1 = "1".repeat(40);
    let commit_v2 = "2".repeat(40);
    let commit_tampered = "3".repeat(40);

    // Build and install initial release v1 (`0.3.0`).
    let v1_src = root.join("bundle-v1");
    create_synthetic_release_bundle(&v1_src, "0.3.0", &commit_v1);

    let release_paths = ReleaseManagerPaths {
        install_root: install_root.clone(),
        config_root: config_root.clone(),
        state_root: state_dir.clone(),
        run_dir: deploy_run.clone(),
        system_root: sysroot.clone(),
    };
    let bin = env!("CARGO_BIN_EXE_miro-ego");
    let init_install = Command::new(bin)
        .args([
            "deploy",
            "install",
            v1_src.to_str().unwrap(),
            "--no-systemctl",
            "--install-root",
            install_root.to_str().unwrap(),
            "--config-root",
            config_root.to_str().unwrap(),
            "--state-root",
            state_dir.to_str().unwrap(),
            "--run-dir",
            deploy_run.to_str().unwrap(),
            "--system-root",
            sysroot.to_str().unwrap(),
        ])
        .output()
        .expect("run miro-ego deploy install v1");
    assert!(
        init_install.status.success(),
        "v1 install failed: {}",
        String::from_utf8_lossy(&init_install.stderr)
    );

    // Stage valid offline bundle v2 (`0.4.0`) AND a tampered bundle under `/data/updates/<commit>`.
    let updates_dir = DataVolumePaths::new(data_dir.clone()).updates_dir();
    let staged_v2 = updates_dir.join(&commit_v2);
    create_synthetic_release_bundle(&staged_v2, "0.4.0", &commit_v2);

    let staged_tampered = updates_dir.join(&commit_tampered);
    create_synthetic_release_bundle(&staged_tampered, "0.5.0", &commit_tampered);
    // Corrupt binary after manifest creation so `verify_bundle_dir` fails with `invalid_firmware_bundle`!
    fs::write(
        staged_tampered.join("bin/miro-ego"),
        b"corrupted-not-matching-sha256",
    )
    .unwrap();

    // Launch `miro-ego deploy serve-rpc` with systemd `StandardInput=socket` (fd 0) + `NOTIFY_SOCKET`!
    let deploy_sock = deploy_run.join("deploy-control.sock");
    let listener = UnixListener::bind(&deploy_sock).expect("bind deploy socket listener");
    let listener_fd = listener.as_raw_fd();

    let abs_name = format!(
        "miro-ego-deploy-notify-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let abs_addr = UnixSocketAddr::from_abstract_name(abs_name.as_bytes()).unwrap();
    let notify_rx = UnixDatagram::bind_addr(&abs_addr).unwrap();
    notify_rx
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let mut rpc_cmd = Command::new(bin);
    rpc_cmd
        .args([
            "deploy",
            "serve-rpc",
            "--no-systemctl",
            "--install-root",
            install_root.to_str().unwrap(),
            "--config-root",
            config_root.to_str().unwrap(),
            "--state-root",
            state_dir.to_str().unwrap(),
            "--run-dir",
            deploy_run.to_str().unwrap(),
            "--system-root",
            sysroot.to_str().unwrap(),
            "--updates-root",
            updates_dir.to_str().unwrap(),
        ])
        .env("NOTIFY_SOCKET", OsStr::new(&format!("@{abs_name}")))
        .env_remove("LISTEN_PID")
        .env_remove("LISTEN_FDS");

    // SAFETY: `pre_exec` calls async-signal-safe `dup2` onto `STDIN_FILENO` (fd 0).
    unsafe {
        rpc_cmd.pre_exec(move || {
            if libc::dup2(listener_fd, libc::STDIN_FILENO) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut rpc_child = rpc_cmd.spawn().expect("spawn miro-ego deploy serve-rpc");
    drop(listener);

    let mut notify_buf = [0u8; 64];
    let n = notify_rx
        .recv(&mut notify_buf)
        .expect("receive READY=1 from deploy serve-rpc");
    assert_eq!(&notify_buf[..n], b"READY=1");

    // Start live gateway daemon wired to `deploy_sock`.
    let cfg = make_lab_device_config(&data_dir, &state_dir);
    let mut options = DaemonServeOptions::from_device_config(cfg, true);
    options.listen_override = Some("127.0.0.1:0".parse().unwrap());
    options.network_socket = None;
    options.deploy_socket = Some(deploy_sock.clone());
    options.release_paths = release_paths;

    let bound = bind_daemon(options)
        .await
        .expect("bind daemon with deploy_socket");
    let addr = bound.server.local_addr();
    let origin = format!("http://{addr}");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let daemon_task = tokio::spawn(async move {
        run_bound_daemon_with_shutdown(bound, async {
            let _ = shutdown_rx.await;
        })
        .await
    });

    // 1. `GET /api/v1/firmware/check` discovers offline package `0.4.0` (`commit_v2`) and ignores tampered bundle!
    let (chk_code, _, chk_body) =
        http_request(addr, Method::GET, "/api/v1/firmware/check", &[], None).await;
    assert_eq!(
        chk_code,
        200,
        "firmware check failed: {}",
        String::from_utf8_lossy(&chk_body)
    );
    let chk: Value = serde_json::from_slice(&chk_body).unwrap();
    assert_eq!(chk["current"]["version"], "0.3.0");
    assert_eq!(chk["current"]["commit"], commit_v1);
    assert_eq!(chk["has_update"], true);
    assert_eq!(chk["available"]["version"], "0.4.0");
    assert_eq!(chk["available"]["commit"], commit_v2);

    // 2. `POST /api/v1/firmware/update` with tampered bundle MUST fail upfront with 422 `invalid_firmware_bundle`!
    let (bad_code, _, bad_body) = http_request(
        addr,
        Method::POST,
        "/api/v1/firmware/update",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &new_uuid_v4()),
        ],
        Some(serde_json::to_vec(&json!({ "commit": commit_tampered })).unwrap()),
    )
    .await;
    assert_eq!(
        bad_code,
        422,
        "tampered bundle must be rejected upfront by verify_bundle_dir: {}",
        String::from_utf8_lossy(&bad_body)
    );
    let bad_err: Value = serde_json::from_slice(&bad_body).unwrap();
    assert_eq!(bad_err["error"]["code"], "invalid_firmware_bundle");

    // 3. `POST /api/v1/firmware/update` with `commit_v2` installs v2 via `deploy serve-rpc`!
    let (up_code, _, up_body) = http_request(
        addr,
        Method::POST,
        "/api/v1/firmware/update",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &new_uuid_v4()),
        ],
        Some(serde_json::to_vec(&json!({ "commit": commit_v2 })).unwrap()),
    )
    .await;
    assert_eq!(
        up_code,
        202,
        "firmware update failed: {}",
        String::from_utf8_lossy(&up_body)
    );

    let wait_deadline = Instant::now() + Duration::from_secs(5);
    let mut after_update = Value::Null;
    while Instant::now() < wait_deadline {
        let (code, _, body) = http_request(addr, Method::GET, "/api/v1/firmware", &[], None).await;
        assert_eq!(code, 200);
        let fw: Value = serde_json::from_slice(&body).unwrap();
        if fw["task"]["status"] == "succeeded" && fw["current"]["commit"] == commit_v2 {
            after_update = fw;
            break;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    assert_eq!(after_update["current"]["version"], "0.4.0");
    assert_eq!(after_update["current"]["commit"], commit_v2);
    assert_eq!(
        after_update["previous"]["version"], "0.3.0",
        "previous.version must report actual installed 0.3.0, not placeholder 'previous'"
    );
    assert_eq!(after_update["previous"]["commit"], commit_v1);
    assert_eq!(after_update["has_update"], false);

    // 4. `POST /api/v1/firmware/rollback` rolls back to `commit_v1` (`0.3.0`) via `deploy serve-rpc`!
    let (rb_code, _, rb_body) = http_request(
        addr,
        Method::POST,
        "/api/v1/firmware/rollback",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &new_uuid_v4()),
        ],
        Some(serde_json::to_vec(&json!({ "commit": commit_v1 })).unwrap()),
    )
    .await;
    assert_eq!(
        rb_code,
        202,
        "firmware rollback failed: {}",
        String::from_utf8_lossy(&rb_body)
    );

    let rb_deadline = Instant::now() + Duration::from_secs(5);
    let mut after_rollback = Value::Null;
    while Instant::now() < rb_deadline {
        let (code, _, body) = http_request(addr, Method::GET, "/api/v1/firmware", &[], None).await;
        assert_eq!(code, 200);
        let fw: Value = serde_json::from_slice(&body).unwrap();
        if fw["task"]["action"] == "rollback"
            && fw["task"]["status"] == "succeeded"
            && fw["current"]["commit"] == commit_v1
        {
            after_rollback = fw;
            break;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    assert_eq!(after_rollback["current"]["version"], "0.3.0");
    assert_eq!(after_rollback["current"]["commit"], commit_v1);

    let _ = shutdown_tx.send(());
    daemon_task.await.unwrap().unwrap();

    // Send SIGTERM to `miro-ego deploy serve-rpc` and verify `STOPPING=1` + clean exit!
    unsafe {
        let _ = libc::kill(rpc_child.id() as libc::pid_t, libc::SIGTERM);
    }
    let n_stop = notify_rx
        .recv(&mut notify_buf)
        .expect("receive STOPPING=1 from deploy serve-rpc");
    assert_eq!(&notify_buf[..n_stop], b"STOPPING=1");
    let rpc_exit = rpc_child.wait().unwrap();
    assert!(rpc_exit.success());

    // Also sanity-check in-process `DeployRpcServer` constructor coverage:
    let inproc_sock = deploy_run.join("inproc-deploy.sock");
    let inproc_mgr = Arc::new(ReleaseManager::new(
        ReleaseManagerPaths {
            install_root,
            config_root,
            state_root: state_dir,
            run_dir: deploy_run,
            system_root: sysroot,
        },
        Arc::new(NoopSystemctlRunner),
        Arc::new(NoopHealthChecker),
    ));
    let inproc_srv =
        DeployRpcServer::bind(&inproc_sock, inproc_mgr, DeployRpcServerConfig::default()).unwrap();
    inproc_srv.shutdown();

    let _ = fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// 6. Network observed state, rescue_ssid, Avahi config, & active-capture lock contention
// ---------------------------------------------------------------------------

struct CustomObservedNmcliRunner;

impl CommandRunner for CustomObservedNmcliRunner {
    fn run(
        &self,
        _program: &str,
        args: &[&str],
        _timeout: Duration,
    ) -> NetworkResult<CommandOutput> {
        let joined = args.join(" ");
        let stdout = if joined.contains("device wifi list") {
            "LabWifi:WPA2:77\n".to_owned()
        } else if joined.contains("device show wlan0") {
            "GENERAL.DEVICE:wlan0\nGENERAL.TYPE:wifi\nGENERAL.STATE:100 (connected)\nGENERAL.CONNECTION:miro-ego-hotspot-rescue\nIP4.ADDRESS[1]:192.168.77.1/24\n".to_owned()
        } else if joined.contains("device show eth0") {
            "GENERAL.DEVICE:eth0\nGENERAL.TYPE:ethernet\nGENERAL.STATE:100 (connected)\nGENERAL.CONNECTION:Wired\nIP4.ADDRESS[1]:10.0.50.9/24\nIP4.GATEWAY:10.0.50.1\nIP4.ROUTE[1]:dst = 0.0.0.0/0, nh = 10.0.50.1, mt = 100\n".to_owned()
        } else if joined.contains("connection show") {
            "miro-ego-hotspot-rescue:11111111-2222-4333-8444-555555555555:802-11-wireless:wlan0\nWired:22222222-3333-4444-8555-666666666666:802-3-ethernet:eth0\n".to_owned()
        } else if joined.contains("device status") {
            "wlan0:wifi:connected:miro-ego-hotspot-rescue\neth0:ethernet:connected:Wired\n"
                .to_owned()
        } else {
            String::new()
        };
        Ok(CommandOutput {
            exit_code: 0,
            stdout,
            stderr: String::new(),
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn network_observed_mapping_avahi_config_and_active_capture_lock_blocks_scan_and_rpc() {
    let root = temp_test_dir("network-seams");
    let data_dir = root.join("data");
    let state_dir = root.join("state");
    let shared_run_dir = root.join("shared-run");
    let net_dir = root.join("net");
    for d in [&data_dir, &state_dir, &shared_run_dir, &net_dir] {
        fs::create_dir_all(d).unwrap();
    }
    let _ = init_test_volume(&data_dir);

    // 1. Verify `build_network_controller_config` configures Avahi + port + Customer Https!
    let mut cust_cfg = make_lab_device_config(&data_dir, &state_dir);
    cust_cfg.listen.port = 8443;
    cust_cfg.security = SecurityConfig::Customer {
        isolated_network: true,
        bearer_token_file: PathBuf::from("/etc/miro-ego/bearer.token"),
        tls_certificate_file: PathBuf::from("/etc/miro-ego/cert.pem"),
        tls_private_key_file: PathBuf::from("/etc/miro-ego/key.pem"),
        principal_id: "owner".to_owned(),
    };
    let cust_cfg_path = root.join("customer-device.json");
    fs::write(
        &cust_cfg_path,
        serde_json::to_vec_pretty(&cust_cfg).unwrap(),
    )
    .unwrap();

    let ctrl_cfg = build_network_controller_config(
        &cust_cfg_path,
        true,
        None,
        shared_run_dir.clone(),
        Path::new("/etc/NetworkManager/system-connections"),
        None,
    )
    .expect("build_network_controller_config from customer device.json");
    assert_eq!(ctrl_cfg.device_id, cust_cfg.device.device_id);
    assert_eq!(ctrl_cfg.mdns_port, 8443);
    assert_eq!(ctrl_cfg.mdns_scheme, MdnsScheme::Https);
    assert_eq!(
        ctrl_cfg.avahi_dir.as_deref(),
        Some(Path::new(DEFAULT_AVAHI_SERVICES_DIR))
    );

    // 2. Start `AuthoritativeNetworkController` sharing `shared_run_dir` with `bind_daemon`!
    let cfg = make_lab_device_config(&data_dir, &state_dir);
    let expected_rescue_ssid = derive_rescue_identity(&cfg.device.device_id).unwrap().0;
    assert_ne!(
        expected_rescue_ssid,
        format!(
            "MiroEgo-{}",
            cfg.device.device_label.trim_start_matches("Miro-Ego-")
        ),
        "derived rescue SSID uses SHA-256(device_id), distinct from device_label suffix"
    );

    let net_sock = net_dir.join("network-control.sock");
    let profiles_dir = net_dir.join("profiles");
    let avahi_dir = net_dir.join("avahi");
    fs::create_dir_all(&profiles_dir).unwrap();
    fs::create_dir_all(&avahi_dir).unwrap();

    let state_store = NetworkStateStore::open_or_init(net_dir.join("state.json")).unwrap();
    let nmcli = NmcliBackend::new(
        Arc::new(CustomObservedNmcliRunner),
        profiles_dir,
        "wlan0",
        "eth0",
    );
    let mut live_ctrl_cfg =
        ControllerConfig::new(cfg.device.device_id.clone(), shared_run_dir.clone());
    live_ctrl_cfg.avahi_dir = Some(avahi_dir.clone());
    live_ctrl_cfg.mdns_port = 8443;
    live_ctrl_cfg.mdns_scheme = MdnsScheme::Https;

    let controller =
        Arc::new(AuthoritativeNetworkController::new(nmcli, state_store, live_ctrl_cfg).unwrap());
    let net_server =
        NetworkRpcServer::bind(&net_sock, controller, RpcServerConfig::default()).unwrap();

    assert!(
        avahi_dir.join("miro-ego.service").is_file(),
        "Avahi service file must be written when avahi_dir is configured"
    );

    let mut options = DaemonServeOptions::from_device_config(cfg.clone(), true);
    options.listen_override = Some("127.0.0.1:0".parse().unwrap());
    options.network_socket = Some(net_sock.clone());
    options.run_dir = Some(shared_run_dir.clone());

    let bound = bind_daemon(options).await.unwrap();
    let addr = bound.server.local_addr();
    let origin = format!("http://{addr}");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let daemon_task = tokio::spawn(async move {
        run_bound_daemon_with_shutdown(bound, async {
            let _ = shutdown_rx.await;
        })
        .await
    });

    // Verify `GET /api/v1/network` maps live `ControllerStatus.observed` & `rescue_ssid` (no hardcoded 10.42.0.1)!
    let (n_code, _, n_body) = http_request(addr, Method::GET, "/api/v1/network", &[], None).await;
    assert_eq!(n_code, 200);
    let net_json: Value = serde_json::from_slice(&n_body).unwrap();
    assert_eq!(
        net_json["observed"]["ap"]["peer_or_ssid"],
        expected_rescue_ssid
    );
    assert_eq!(
        net_json["observed"]["ap"]["addresses"],
        json!(["192.168.77.1"]),
        "observed AP address must come from ControllerStatus.observed, not hardcoded 10.42.0.1"
    );
    assert_eq!(
        net_json["observed"]["wired"]["addresses"],
        json!(["10.0.50.9"])
    );
    assert_eq!(net_json["observed"]["default_route"], "wired");

    // Start capture -> holds shared `network-operation.lock`!
    let (s_code, _, _) = http_request(
        addr,
        Method::POST,
        "/api/v1/capture/start",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &new_uuid_v4()),
        ],
        Some(
            serde_json::to_vec(&json!({
                "schema": "miro-ego.capture-start.v1",
                "mode": "production",
                "take": { "kind": "new" }
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(s_code, 202);

    // Direct RPC mutation bypassing HTTP gateway is STILL blocked by OS shared `network-operation.lock`!
    let direct_rpc = NetworkRpcClient::new(&net_sock);
    let rpc_err = direct_rpc
        .apply(
            "root-cli",
            &new_uuid_v4(),
            &ApplyDesiredState {
                mode: DesiredMode::Hotspot,
                wifi_client: None,
                ethernet: None,
            },
        )
        .expect_err(
            "AuthoritativeNetworkController must be blocked by shared network-operation.lock",
        );
    assert!(
        rpc_err.to_string().contains("capture_active"),
        "expected capture_active from lock contention, got: {rpc_err}"
    );

    // `GET /api/v1/network/scan` is blocked with 503 `capture_active` while capture is busy!
    let (scan_busy_code, scan_busy_hdrs, scan_busy_body) =
        http_request(addr, Method::GET, "/api/v1/network/scan", &[], None).await;
    assert_eq!(scan_busy_code, 503);
    assert_eq!(
        scan_busy_hdrs
            .get("Miro-Ego-Error-Code")
            .and_then(|v| v.to_str().ok()),
        Some("capture_active")
    );
    let scan_busy_err: Value = serde_json::from_slice(&scan_busy_body).unwrap();
    assert_eq!(scan_busy_err["error"]["code"], "capture_active");

    // Stop capture -> lock released -> `GET /api/v1/network/scan` succeeds (`200 OK`)!
    let (stop_code, _, _) = http_request(
        addr,
        Method::POST,
        "/api/v1/capture/stop",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &new_uuid_v4()),
        ],
        Some(
            serde_json::to_vec(&json!({
                "schema": "miro-ego.capture-stop.v1",
                "reason": "user"
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(stop_code, 202);

    let (scan_ok_code, _, scan_ok_body) =
        http_request(addr, Method::GET, "/api/v1/network/scan", &[], None).await;
    assert_eq!(scan_ok_code, 200);
    let scan_ok: Value = serde_json::from_slice(&scan_ok_body).unwrap();
    assert_eq!(scan_ok["networks"][0]["ssid"], "LabWifi");

    // Verify `POST /api/v1/network/retry` with unknown UUIDv7 -> 404 `network_transaction_not_found` with `details.transaction_id`!
    let unknown_tx = new_uuid_v7();
    let (retry_code, retry_hdrs, retry_body) = http_request(
        addr,
        Method::POST,
        "/api/v1/network/retry",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &new_uuid_v4()),
        ],
        Some(
            serde_json::to_vec(&json!({
                "schema": "miro-ego.network-retry-request.v1",
                "transaction_id": unknown_tx
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(retry_code, 404);
    assert_eq!(
        retry_hdrs
            .get("Miro-Ego-Error-Code")
            .and_then(|v| v.to_str().ok()),
        Some("network_transaction_not_found")
    );
    let retry_err: Value = serde_json::from_slice(&retry_body).unwrap();
    assert_eq!(retry_err["error"]["code"], "network_transaction_not_found");
    assert_eq!(retry_err["error"]["details"]["transaction_id"], unknown_tx);

    let _ = shutdown_tx.send(());
    daemon_task.await.unwrap().unwrap();
    net_server.shutdown();
    let _ = fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// 7. OpenAPI error/schema parity (`session_not_verified`, `capture_busy`, `degraded_network_status`, unmounted `device_info`)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn openapi_error_and_degraded_schema_parity_for_sessions_network_and_unmounted_device() {
    let root = temp_test_dir("openapi-parity");
    let data_dir = root.join("data");
    let state_dir = root.join("state");
    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&state_dir).unwrap();

    // 1. Unit check error translators (`map_storage_error` & `map_network_error`) and `degraded_network_status`:
    let busy_api_err = map_storage_error(StorageError::Locked {
        code: "capture_busy",
        message: "recording is finalizing".to_owned(),
    });
    let busy_json = busy_api_err.to_problem_json(&new_uuid_v4());
    assert_eq!(busy_api_err.status_code, 423);
    assert_eq!(busy_json["error"]["code"], "capture_busy");
    assert_eq!(busy_json["error"]["details"]["wait_for"], "idle");
    assert_eq!(busy_json["error"]["details"]["retry_after_seconds"], 1);
    assert_eq!(
        busy_json["error"]["details"]["current_state"], "finalizing",
        "map_storage_error must preserve CaptureBusyError.details.current_state"
    );
    assert!(busy_json["error"]["details"].get("reason").is_none());

    let orig_tx = new_uuid_v7();
    let idem_net_err = map_network_error(NetworkError::Invalid {
        message: format!("idempotency_conflict: key already bound to {orig_tx}"),
    });
    let idem_json = idem_net_err.to_problem_json(&new_uuid_v4());
    assert_eq!(idem_net_err.status_code, 409);
    assert_eq!(idem_json["error"]["code"], "idempotency_conflict");
    assert_eq!(
        idem_json["error"]["details"]["idempotency_scope"],
        "network-mutation"
    );
    assert_eq!(
        idem_json["error"]["details"]["original_transaction_id"],
        orig_tx
    );

    let cred_err = map_network_error(NetworkError::Invalid {
        message: "credential_ref_expired: credential reference has expired".to_owned(),
    });
    let cred_json = cred_err.to_problem_json(&new_uuid_v4());
    assert_eq!(cred_err.status_code, 422);
    assert_eq!(
        cred_json["error"]["details"]["field"],
        "desired.wifi_client.credential_ref"
    );
    assert_eq!(
        cred_json["error"]["details"]["reason"],
        "credential_ref_expired"
    );

    let dev_id = new_uuid_v4();
    let degraded = degraded_network_status(&dev_id, 8443, "network_controller_unavailable");
    assert_eq!(degraded["observed"]["ap"]["state"], "unavailable");
    assert!(degraded["observed"]["ap"]["interface"].is_null());
    assert_eq!(degraded["observed"]["mdns"]["port"], 8443);
    assert_eq!(
        degraded["mutation_capability"]["disabled_reason"],
        "controller_unavailable"
    );
    assert_eq!(
        degraded["mutation_capability"]["operations"],
        json!(["apply", "retry", "forget"])
    );
    assert_eq!(
        degraded["concurrency_capability"]["same_phy_ap_sta"],
        "unverified"
    );

    // 2. Boot daemon WITHOUT `volume.json` (`unmounted`/uninitialized metadata) and `network_socket = None`:
    let cfg = make_lab_device_config(&data_dir, &state_dir);
    let mut options = DaemonServeOptions::from_device_config(cfg.clone(), true);
    options.listen_override = Some("127.0.0.1:0".parse().unwrap());
    options.network_socket = None;

    let bound = bind_daemon(options).await.unwrap();
    let catalog = Arc::clone(bound.backend.catalog());
    let addr = bound.server.local_addr();
    let origin = format!("http://{addr}");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let daemon_task = tokio::spawn(async move {
        run_bound_daemon_with_shutdown(bound, async {
            let _ = shutdown_rx.await;
        })
        .await
    });

    // `GET /api/v1/device` when `volume.json` is absent MUST still emit a valid non-null UuidV4 `storage.volume_id`!
    let (d_code, _, d_body) = http_request(addr, Method::GET, "/api/v1/device", &[], None).await;
    assert_eq!(d_code, 200);
    let dev_json: Value = serde_json::from_slice(&d_body).unwrap();
    let vol_id_str = dev_json["storage"]["volume_id"]
        .as_str()
        .expect("storage.volume_id must be a non-null string even when volume.json is absent");
    assert!(is_valid_uuid_v4(vol_id_str));

    // `GET /api/v1/network` when `network_socket = None` returns OpenAPI-valid degraded status (`disabled_reason = "not_enabled"`).
    let (n_code, _, n_body) = http_request(addr, Method::GET, "/api/v1/network", &[], None).await;
    assert_eq!(n_code, 200);
    let n_json: Value = serde_json::from_slice(&n_body).unwrap();
    assert_eq!(
        n_json["mutation_capability"]["disabled_reason"],
        "not_enabled"
    );
    assert_eq!(
        n_json["mutation_capability"]["operations"],
        json!(["apply", "retry", "forget"])
    );

    // Now initialize volume.json, record & seal one session, then test `open_session_artifact`
    // verification verdicts (`absent`, `stale`, `unusable`)!
    let _ = init_test_volume(&data_dir);
    let (s_code, _, _) = http_request(
        addr,
        Method::POST,
        "/api/v1/capture/start",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &new_uuid_v4()),
        ],
        Some(
            serde_json::to_vec(&json!({
                "schema": "miro-ego.capture-start.v1",
                "mode": "production",
                "take": { "kind": "new" }
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(s_code, 202);

    let (stop_code, _, _) = http_request(
        addr,
        Method::POST,
        "/api/v1/capture/stop",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &new_uuid_v4()),
        ],
        Some(
            serde_json::to_vec(&json!({
                "schema": "miro-ego.capture-stop.v1",
                "reason": "user"
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(stop_code, 202);

    let sessions = catalog.list_sessions().unwrap();
    let sid = sessions[0].session_id.clone();
    let (_, m_hdrs, m_body) = http_request(
        addr,
        Method::GET,
        &format!("/api/v1/sessions/{sid}"),
        &[],
        None,
    )
    .await;
    let manifest_sha = m_hdrs
        .get("Miro-Ego-Manifest-SHA256")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let manifest_val: Value = serde_json::from_slice(&m_body).unwrap();
    let aid = manifest_val["video"]["segments"][0]["artifacts"]["left"]["sha256"]
        .as_str()
        .unwrap()
        .to_owned();
    let art_path = format!("/api/v1/sessions/{sid}/artifacts/{aid}");

    // Freshly sealed -> `usable` -> 200 OK
    let (ok_code, _, _) = http_request(addr, Method::GET, &art_path, &[], None).await;
    assert_eq!(ok_code, 200);

    // Case A: `verification == None` -> 409 `session_not_verified` with `details.reason == "absent"`!
    catalog.set_verification(&sid, None).unwrap();
    let (abs_code, abs_hdrs, abs_body) =
        http_request(addr, Method::GET, &art_path, &[], None).await;
    assert_eq!(abs_code, 409);
    assert_eq!(
        abs_hdrs
            .get("Miro-Ego-Error-Code")
            .and_then(|v| v.to_str().ok()),
        Some("session_not_verified")
    );
    let abs_err: Value = serde_json::from_slice(&abs_body).unwrap();
    assert_eq!(abs_err["error"]["details"]["reason"], "absent");

    // Case B: mismatched `verification.manifest_sha256` -> 409 `session_not_verified` with `details.reason == "stale"`!
    catalog
        .set_verification(
            &sid,
            Some(GatewayVerification {
                actor: "gateway".to_owned(),
                validator: GatewayValidatorInfo {
                    name: "miro-ego".to_owned(),
                    version: "0.3.0".to_owned(),
                    build_sha256: "a".repeat(64),
                },
                manifest_sha256: "f".repeat(64),
                verified_at: "2026-11-26T10:00:00Z".to_owned(),
                verdict: "usable".to_owned(),
                diagnostics: Vec::new(),
            }),
        )
        .unwrap();
    let (stale_code, _, stale_body) = http_request(addr, Method::GET, &art_path, &[], None).await;
    assert_eq!(stale_code, 409);
    let stale_err: Value = serde_json::from_slice(&stale_body).unwrap();
    assert_eq!(stale_err["error"]["details"]["reason"], "stale");

    // Case C: non-usable verdict -> 409 `session_not_verified` mapped strictly to `"unusable"`!
    catalog
        .set_verification(
            &sid,
            Some(GatewayVerification {
                actor: "gateway".to_owned(),
                validator: GatewayValidatorInfo {
                    name: "miro-ego".to_owned(),
                    version: "0.3.0".to_owned(),
                    build_sha256: "a".repeat(64),
                },
                manifest_sha256: manifest_sha,
                verified_at: "2026-11-26T10:00:00Z".to_owned(),
                verdict: "unusable".to_owned(),
                diagnostics: vec![GatewayVerificationDiagnostic {
                    code: "artifact_digest_mismatch".to_owned(),
                    summary: "bitrot detected".to_owned(),
                }],
            }),
        )
        .unwrap();
    let (unusable_code, _, unusable_body) =
        http_request(addr, Method::GET, &art_path, &[], None).await;
    assert_eq!(unusable_code, 409);
    let unusable_err: Value = serde_json::from_slice(&unusable_body).unwrap();
    assert_eq!(unusable_err["error"]["details"]["reason"], "unusable");

    let _ = shutdown_tx.send(());
    daemon_task.await.unwrap().unwrap();
    let _ = fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// 8. LED/voice feedback state machine, focus/deploy/network error parity,
//    dynamic calibration_capture, clock challenge eviction, firmware fail-closed,
//    network SSE background poller, and `deploy serve-rpc --idle-timeout`
// ---------------------------------------------------------------------------

struct RecordingIndicator {
    states: Mutex<Vec<miro_ego_hw::IndicatorState>>,
}

impl RecordingIndicator {
    fn new() -> Self {
        Self {
            states: Mutex::new(Vec::new()),
        }
    }

    fn take(&self) -> Vec<miro_ego_hw::IndicatorState> {
        std::mem::take(&mut *self.states.lock().unwrap())
    }
}

impl miro_ego_hw::IndicatorController for RecordingIndicator {
    fn set_state(&self, state: miro_ego_hw::IndicatorState) -> miro_ego_hw::HwResult<()> {
        self.states.lock().unwrap().push(state);
        Ok(())
    }
}

struct RecordingVoice {
    cues: Mutex<Vec<miro_ego_hw::VoiceCue>>,
}

impl RecordingVoice {
    fn new() -> Self {
        Self {
            cues: Mutex::new(Vec::new()),
        }
    }

    fn take(&self) -> Vec<miro_ego_hw::VoiceCue> {
        std::mem::take(&mut *self.cues.lock().unwrap())
    }
}

impl miro_ego_hw::VoiceFeedback for RecordingVoice {
    fn play_cue(&self, cue: miro_ego_hw::VoiceCue) -> miro_ego_hw::HwResult<()> {
        self.cues.lock().unwrap().push(cue);
        Ok(())
    }
}

#[test]
fn event_feedback_tracker_and_error_translators_cover_stop_failure_focus_deploy_and_network() {
    use miro_ego_hw::{IndicatorState, VoiceCue};
    use miro_ego_storage::{CaptureEvent, CaptureEventType};

    let led = RecordingIndicator::new();
    let voice = RecordingVoice::new();
    let mut tracker = EventFeedbackTracker::default();
    let epoch = new_uuid_v4();
    let sid1 = new_uuid_v7();

    let mk_ev = |ty: CaptureEventType, sid: Option<&str>, data: Value| -> CaptureEvent {
        CaptureEvent {
            schema: "miro-ego.capture-event.v1".to_owned(),
            sse_delivery_id: "1".to_owned(),
            authority_epoch: epoch.clone(),
            source_revision: 1,
            event_type: ty,
            occurred_at: "2026-11-26T10:00:00Z".to_owned(),
            session_id: sid.map(str::to_owned),
            data,
        }
    };

    // 1. Start -> State("recording") + Snapshot("recording") -> single Recording LED & voice cue
    tracker.observe(
        &mk_ev(
            CaptureEventType::State,
            Some(&sid1),
            json!({ "state": "recording" }),
        ),
        Some(&led),
        Some(&voice),
    );
    tracker.observe(
        &mk_ev(
            CaptureEventType::Snapshot,
            Some(&sid1),
            json!({ "device_state": "recording" }),
        ),
        Some(&led),
        Some(&voice),
    );
    assert_eq!(led.take(), vec![IndicatorState::Recording]);
    assert_eq!(voice.take(), vec![VoiceCue::RecordingStarted]);

    // 2. Normal stop -> State("finalizing") -> State("verifying") -> Snapshot("idle") -> Ready + RecordingStopped!
    tracker.observe(
        &mk_ev(
            CaptureEventType::State,
            Some(&sid1),
            json!({ "state": "finalizing" }),
        ),
        Some(&led),
        Some(&voice),
    );
    tracker.observe(
        &mk_ev(
            CaptureEventType::State,
            Some(&sid1),
            json!({ "state": "verifying" }),
        ),
        Some(&led),
        Some(&voice),
    );
    tracker.observe(
        &mk_ev(
            CaptureEventType::Snapshot,
            None,
            json!({ "device_state": "idle" }),
        ),
        Some(&led),
        Some(&voice),
    );
    assert_eq!(led.take(), vec![IndicatorState::Ready]);
    assert_eq!(voice.take(), vec![VoiceCue::RecordingStopped]);

    // 3. Mid-recording failure -> State("recording") -> Diagnostic(error) -> State("failed") -> Snapshot("idle")
    //    Must emit Error + ErrorAlert once and NOT overwrite Error with Ready on trailing Snapshot("idle")!
    let sid2 = new_uuid_v7();
    tracker.observe(
        &mk_ev(
            CaptureEventType::State,
            Some(&sid2),
            json!({ "state": "recording" }),
        ),
        Some(&led),
        Some(&voice),
    );
    let _ = led.take();
    let _ = voice.take();

    tracker.observe(
        &mk_ev(
            CaptureEventType::Diagnostic,
            Some(&sid2),
            json!({ "diagnostic": { "severity": "error", "code": "enc_crash" } }),
        ),
        Some(&led),
        Some(&voice),
    );
    tracker.observe(
        &mk_ev(
            CaptureEventType::State,
            Some(&sid2),
            json!({ "state": "failed" }),
        ),
        Some(&led),
        Some(&voice),
    );
    tracker.observe(
        &mk_ev(
            CaptureEventType::Snapshot,
            None,
            json!({ "device_state": "idle" }),
        ),
        Some(&led),
        Some(&voice),
    );
    assert_eq!(led.take(), vec![IndicatorState::Error]);
    assert_eq!(voice.take(), vec![VoiceCue::ErrorAlert]);

    // 4. Start-attempt failure -> Diagnostic(error) while idle -> Error + ErrorAlert!
    let sid3 = new_uuid_v7();
    tracker.observe(
        &mk_ev(
            CaptureEventType::Diagnostic,
            Some(&sid3),
            json!({ "diagnostic": { "severity": "error", "code": "staging_failed" } }),
        ),
        Some(&led),
        Some(&voice),
    );
    assert_eq!(led.take(), vec![IndicatorState::Error]);
    assert_eq!(voice.take(), vec![VoiceCue::ErrorAlert]);

    // 5. Camera focus error mapping:
    let f_unsup = map_storage_error(map_camera_focus_error(
        "camera_focus_unsupported",
        "no focus".to_owned(),
    ));
    assert_eq!(f_unsup.status_code, 404);
    assert_eq!(f_unsup.code, "camera_focus_unsupported");

    let f_range = map_storage_error(map_camera_focus_error(
        "invalid_camera_focus",
        "out of range".to_owned(),
    ));
    assert_eq!(f_range.status_code, 422);
    assert_eq!(f_range.code, "invalid_camera_focus");

    let f_auto = map_storage_error(map_camera_focus_error(
        "camera_focus_auto_unsupported",
        "no focus_auto".to_owned(),
    ));
    assert_eq!(f_auto.status_code, 422);
    assert_eq!(f_auto.code, "invalid_camera_focus");

    let f_disc = map_storage_error(map_camera_focus_error(
        "invalid_state",
        "camera closed".to_owned(),
    ));
    assert_eq!(f_disc.status_code, 503);
    assert_eq!(f_disc.code, "camera_not_connected");

    let f_timeout = map_storage_error(map_camera_focus_error(
        "native_camera_control_timeout",
        "timed out".to_owned(),
    ));
    assert_eq!(f_timeout.status_code, 503);
    assert_eq!(f_timeout.code, "camera_focus_failed");

    // 6. Network mutation 503 + 422 transaction_not_retryable + mDNS aliases:
    let net_503 = map_network_mutation_error(NetworkError::Backend {
        message: "network_controller_unavailable: connect ENOENT".to_owned(),
    });
    let net_503_json = net_503.to_problem_json(&new_uuid_v4());
    assert_eq!(net_503.status_code, 503);
    assert_eq!(
        net_503_json["error"]["code"],
        "network_mutation_unavailable"
    );
    assert_eq!(
        net_503_json["error"]["details"]["reason"],
        "controller_unavailable"
    );

    let net_retry_422 = map_network_mutation_error(NetworkError::Invalid {
        message: "transaction_not_retryable: transaction is already committed".to_owned(),
    });
    let net_retry_json = net_retry_422.to_problem_json(&new_uuid_v4());
    assert_eq!(net_retry_422.status_code, 422);
    assert_eq!(
        net_retry_json["error"]["code"],
        "invalid_network_desired_state"
    );
    assert_eq!(
        net_retry_json["error"]["details"]["field"],
        "transaction_id"
    );
    assert_eq!(net_retry_json["error"]["details"]["reason"], "active_state");

    let deg_http = degraded_network_status(&new_uuid_v4(), 8080, "not_enabled");
    assert_eq!(
        deg_http["observed"]["mdns"]["aliases"],
        json!(["_http._tcp"])
    );
    let deg_https = degraded_network_status_with_scheme(
        &new_uuid_v4(),
        8443,
        MdnsScheme::Https,
        "controller_unavailable",
    );
    assert_eq!(
        deg_https["observed"]["mdns"]["aliases"],
        json!(["_https._tcp"])
    );

    // 7. Deploy error mapping (503 Retry-After on unreachable/timeout/maintenance_busy):
    for deploy_err in [
        miro_ego_deploy::DeployError::Failed {
            message: "deploy_controller_unavailable: ENOENT".to_owned(),
        },
        miro_ego_deploy::DeployError::Failed {
            message: "response_timeout: timed out".to_owned(),
        },
        miro_ego_deploy::DeployError::Blocked {
            message: "maintenance_busy: lock held".to_owned(),
        },
    ] {
        let mapped = map_deploy_error(deploy_err);
        assert_eq!(mapped.status_code, 503);
        assert!(mapped.retryable);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dynamic_calibration_capability_clock_eviction_firmware_fail_closed_voice_and_network_sse()
{
    let root = temp_test_dir("phase1-runtime-hardening");
    let data_dir = root.join("data");
    let state_dir = root.join("state");
    let net_dir = root.join("net");
    let run_dir = root.join("run");
    for d in [&data_dir, &state_dir, &net_dir, &run_dir] {
        fs::create_dir_all(d).unwrap();
    }
    let _ = init_test_volume(&data_dir);

    let cfg = make_lab_device_config(&data_dir, &state_dir);
    let net_sock = net_dir.join("network-control.sock");
    let profiles_dir = net_dir.join("profiles");
    fs::create_dir_all(&profiles_dir).unwrap();
    struct HotspotCommitNmcliRunner {
        active_conn: std::sync::Mutex<String>,
    }
    impl CommandRunner for HotspotCommitNmcliRunner {
        fn run(
            &self,
            _program: &str,
            args: &[&str],
            _timeout: Duration,
        ) -> NetworkResult<CommandOutput> {
            if let Some(pos) = args.iter().position(|&a| a == "id") {
                if args.contains(&"up") {
                    if let Some(name) = args.get(pos + 1) {
                        *self.active_conn.lock().unwrap() = (*name).to_owned();
                    }
                }
            }
            let conn = self.active_conn.lock().unwrap().clone();
            let joined = args.join(" ");
            let stdout = if joined.contains("device show wlan0") {
                format!(
                    "GENERAL.DEVICE:wlan0\nGENERAL.TYPE:wifi\nGENERAL.STATE:100 (connected)\nGENERAL.CONNECTION:{conn}\nIP4.ADDRESS[1]:10.42.0.1/24\n"
                )
            } else if joined.contains("connection show") {
                format!("{conn}:11111111-2222-4333-8444-555555555555:802-11-wireless:wlan0\n")
            } else if joined.contains("device status") {
                format!("wlan0:wifi:connected:{conn}\neth0:ethernet:disconnected:--\n")
            } else {
                String::new()
            };
            Ok(CommandOutput {
                exit_code: 0,
                stdout,
                stderr: String::new(),
            })
        }
    }

    let state_store = NetworkStateStore::open_or_init(net_dir.join("state.json")).unwrap();
    let nmcli = NmcliBackend::new(
        Arc::new(HotspotCommitNmcliRunner {
            active_conn: std::sync::Mutex::new("miro-ego-hotspot-rescue".to_owned()),
        }),
        profiles_dir,
        "wlan0",
        "eth0",
    );
    let mock_clock = Arc::new(MockSystemClockBackend::new(
        "11111111-2222-4333-8444-555555555555",
        1_764_150_000_000,
        5_000_000_000,
    ));
    let controller = Arc::new(
        AuthoritativeNetworkController::new(
            nmcli,
            state_store,
            ControllerConfig::new(cfg.device.device_id.clone(), run_dir.clone())
                .with_clock_backend(
                    Arc::clone(&mock_clock) as Arc<dyn miro_ego_network::SystemClockBackend>
                ),
        )
        .unwrap(),
    );
    let net_server =
        NetworkRpcServer::bind(&net_sock, controller, RpcServerConfig::default()).unwrap();

    let mut options = DaemonServeOptions::from_device_config(cfg.clone(), true);
    options.listen_override = Some("127.0.0.1:0".parse().unwrap());
    options.network_socket = Some(net_sock);
    options.deploy_socket = None;
    options.run_dir = Some(run_dir.clone());

    let bound = bind_daemon(options).await.unwrap();
    let net_events = bound.backend.network_event_buffer();
    let bound_audit = Arc::clone(bound.backend.audit());
    let bound_coord = Arc::clone(bound.backend.coordinator());
    let bound_vol = Arc::clone(bound.backend.volume());
    let addr = bound.server.local_addr();
    let origin = format!("http://{addr}");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let daemon_task = tokio::spawn(async move {
        run_bound_daemon_with_shutdown(bound, async {
            let _ = shutdown_rx.await;
        })
        .await
    });

    // 1. Real git commit from build.rs + dynamic `calibration_capture.enabled == true` while idle:
    let (d_code, _, d_body) = http_request(addr, Method::GET, "/api/v1/device", &[], None).await;
    assert_eq!(d_code, 200);
    let dev_idle: Value = serde_json::from_slice(&d_body).unwrap();
    assert_ne!(
        dev_idle["build"]["commit"],
        "0".repeat(40),
        "build.rs must stamp real git HEAD SHA into MIRO_EGO_GIT_COMMIT"
    );
    assert_eq!(
        dev_idle["capabilities"]["calibration_capture"]["enabled"],
        true
    );
    assert!(dev_idle["capabilities"]["calibration_capture"]["disabled_reason"].is_null());

    // 2. Mock voice volume reports `enabled: volume > 0`:
    let (v_code, _, v_body) = http_request(
        addr,
        Method::POST,
        "/api/v1/voice",
        &[("Content-Type", "application/json"), ("Origin", &origin)],
        Some(
            serde_json::to_vec(&json!({ "schema": "miro-ego.voice-set.v1", "volume": 55 }))
                .unwrap(),
        ),
    )
    .await;
    assert_eq!(v_code, 200);
    let v_json: Value = serde_json::from_slice(&v_body).unwrap();
    assert_eq!(v_json["enabled"], true);
    assert_eq!(v_json["volume"], 55);

    // 3. Firmware update/rollback with `deploy_socket = None` fails closed with `503` (never stuck 202!):
    let (fw_up_code, fw_up_hdrs, fw_up_body) = http_request(
        addr,
        Method::POST,
        "/api/v1/firmware/update",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &new_uuid_v4()),
        ],
        Some(serde_json::to_vec(&json!({ "commit": "a".repeat(40) })).unwrap()),
    )
    .await;
    assert_eq!(fw_up_code, 503);
    assert_eq!(
        fw_up_hdrs.get("Retry-After").and_then(|v| v.to_str().ok()),
        Some("1")
    );
    let fw_up_err: Value = serde_json::from_slice(&fw_up_body).unwrap();
    assert_eq!(fw_up_err["error"]["code"], "firmware_unavailable");

    // 4. Clock challenge bounded eviction (65 challenges issued: 1st evicted, 2nd..65th preserved!)
    //    AND `applied: false` returns non-null `age_seconds`!
    let mut challenges = Vec::with_capacity(65);
    for _ in 0..65 {
        let (c, _, b) = http_request(addr, Method::GET, "/api/v1/clock/check", &[], None).await;
        assert_eq!(c, 200);
        let doc: Value = serde_json::from_slice(&b).unwrap();
        challenges.push(doc["challenge"].as_str().unwrap().to_owned());
    }
    // Challenge #0 was the earliest-expiring and must have been evicted at 65:
    let (evicted_code, _, _) = http_request(
        addr,
        Method::POST,
        "/api/v1/clock/sync",
        &[("Content-Type", "application/json"), ("Origin", &origin)],
        Some(
            serde_json::to_vec(&json!({
                "schema": "miro-ego.clock-sync-request.v1",
                "challenge": challenges[0],
                "unix_time_ms": 1_764_152_400_000_u64
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        evicted_code, 409,
        "earliest challenge must be evicted at cap 64"
    );

    // Challenge #1 MUST still be valid (proving challenges.clear() was NOT called!):
    let (ok_sync_code, _, ok_sync_body) = http_request(
        addr,
        Method::POST,
        "/api/v1/clock/sync",
        &[("Content-Type", "application/json"), ("Origin", &origin)],
        Some(
            serde_json::to_vec(&json!({
                "schema": "miro-ego.clock-sync-request.v1",
                "challenge": challenges[1],
                "unix_time_ms": 1_764_152_400_000_u64
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ok_sync_code, 200);
    let ok_sync_doc: Value = serde_json::from_slice(&ok_sync_body).unwrap();
    assert_eq!(ok_sync_doc["applied"], true);
    assert_eq!(
        ok_sync_doc["boot_id"],
        "11111111-2222-4333-8444-555555555555"
    );
    assert_eq!(mock_clock.settime_calls(), 1);
    assert!(
        run_dir.join("clock-state.json").is_file(),
        "clock.sync RPC must persist boot-bound clock-state.json in run_dir"
    );

    mock_clock.advance_seconds(7);

    // Challenge #2 with `replace: false` must return `applied: false` with numeric `age_seconds`:
    let (second_code, _, second_body) = http_request(
        addr,
        Method::POST,
        "/api/v1/clock/sync",
        &[("Content-Type", "application/json"), ("Origin", &origin)],
        Some(
            serde_json::to_vec(&json!({
                "schema": "miro-ego.clock-sync-request.v1",
                "challenge": challenges[2],
                "unix_time_ms": 1_764_152_405_000_u64,
                "replace": false
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(second_code, 200);
    let second_doc: Value = serde_json::from_slice(&second_body).unwrap();
    assert_eq!(second_doc["applied"], false);
    assert_eq!(second_doc["age_seconds"], 7);
    assert_eq!(mock_clock.settime_calls(), 1);

    // 5. Background network SSE poller automatically feeds async `/network/apply` settlement into `net_events`!
    let (_, _, init_net_body) = http_request(addr, Method::GET, "/api/v1/network", &[], None).await;
    let init_snap = net_events
        .issue_resync_snapshot(serde_json::from_slice(&init_net_body).unwrap(), None)
        .unwrap();
    let (ap_code, _, _) = http_request(
        addr,
        Method::POST,
        "/api/v1/network/apply",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &new_uuid_v4()),
        ],
        Some(
            serde_json::to_vec(&json!({
                "schema": "miro-ego.network-apply-request.v1",
                "desired": {
                    "mode": "hotspot",
                    "wifi_client": null,
                    "ethernet": null
                }
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ap_code, 202);

    // Without calling `GET /api/v1/network`, wait for the background poller to publish a `transaction` SSE event!
    let poll_deadline = Instant::now() + Duration::from_secs(4);
    let mut saw_committed_tx = false;
    while Instant::now() < poll_deadline {
        let evs = net_events
            .try_events_after(init_snap.delivery_id)
            .unwrap()
            .unwrap_or_default();
        if evs.iter().any(|e| {
            (e.event_type == "transaction" && e.source_event["data"]["status"] == "committed")
                || (e.event_type == "snapshot"
                    && e.source_event["data"]["transaction"]["latest"]["status"] == "committed")
        }) {
            saw_committed_tx = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        saw_committed_tx,
        "background network SSE poller must observe async transaction settlement without manual GET /network"
    );

    // 6. Start capture -> `device_info` reports `calibration_capture.disabled_reason == "maintenance_or_capture_busy"`:
    let (s_code, _, _) = http_request(
        addr,
        Method::POST,
        "/api/v1/capture/start",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &new_uuid_v4()),
        ],
        Some(
            serde_json::to_vec(&json!({
                "schema": "miro-ego.capture-start.v1",
                "mode": "production",
                "take": { "kind": "new" }
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(s_code, 202);

    let (_, _, busy_dev_body) = http_request(addr, Method::GET, "/api/v1/device", &[], None).await;
    let busy_dev: Value = serde_json::from_slice(&busy_dev_body).unwrap();
    assert_eq!(
        busy_dev["capabilities"]["calibration_capture"]["enabled"],
        false
    );
    assert_eq!(
        busy_dev["capabilities"]["calibration_capture"]["disabled_reason"],
        "maintenance_or_capture_busy"
    );

    let (swap_stop_code, _, _) = http_request(
        addr,
        Method::POST,
        "/api/v1/capture/stop",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &new_uuid_v4()),
        ],
        Some(
            serde_json::to_vec(
                &json!({ "schema": "miro-ego.capture-stop.v1", "reason": "safe_swap" }),
            )
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(swap_stop_code, 202);
    assert!(
        !bound_audit.has_open_file(),
        "SafeSwap must close AuditLogger active fd before issuing SafeSwapReceipt"
    );
    assert_eq!(bound_coord.open_handle_count(), 0);
    assert!(bound_vol.is_volume_released());

    let (ss_code, _, ss_body) =
        http_request(addr, Method::GET, "/api/v1/capture/safe-swap", &[], None).await;
    assert_eq!(ss_code, 200);
    let ss_doc: Value = serde_json::from_slice(&ss_body).unwrap();
    assert_eq!(ss_doc["receipt"]["open_handle_count"], 0);
    assert_eq!(ss_doc["receipt"]["release_state"], "device-released");

    let _ = shutdown_tx.send(());
    daemon_task.await.unwrap().unwrap();
    net_server.shutdown();

    // 7. Verify `miro-ego deploy serve-rpc --idle-timeout 1` exits cleanly on idle!
    let bin = env!("CARGO_BIN_EXE_miro-ego");
    let idle_sock = root.join("idle-deploy.sock");
    let start_idle = Instant::now();
    let out = Command::new(bin)
        .args([
            "deploy",
            "serve-rpc",
            "--no-systemctl",
            "--idle-timeout",
            "1",
            "--socket",
            idle_sock.to_str().unwrap(),
            "--install-root",
            root.join("opt").to_str().unwrap(),
            "--config-root",
            root.join("etc").to_str().unwrap(),
            "--state-root",
            state_dir.to_str().unwrap(),
            "--run-dir",
            run_dir.to_str().unwrap(),
            "--system-root",
            root.join("sysroot").to_str().unwrap(),
        ])
        .output()
        .expect("run deploy serve-rpc --idle-timeout 1");
    assert!(
        out.status.success(),
        "deploy serve-rpc --idle-timeout 1 must exit 0: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(start_idle.elapsed() < Duration::from_secs(5));

    let _ = fs::remove_dir_all(&root);
}
