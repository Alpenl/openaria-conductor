//! Live full-stack Rust + C end-to-end integration tests for `miro-ego-cli`.
//!
//! Exercises:
//! 1. Real C `miro-ego-encoder-mock` over `memfd`+`eventfd` `MEGO_SHM` v1 driven by
//!    `CaptureCoordinator` + `ExfatVolumeManager` + `BinarySessionCatalog` + `AuditLogger` +
//!    `LogStore` + live `NetworkRpcServer` (Unix socket) + live `GatewayServer` (HTTP/1.1).
//! 2. Live HTTPS `rustls` Customer-profile check with `rcgen` self-signed certificates.
//! 3. Unified `miro-ego` binary subcommands (`--version`, `volume`, `validate`, `deploy`,
//!    `network-control watchdog-mode/status`, `probe`, `status`).

use bytes::Bytes;
use http::{HeaderMap, Method, Request};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use miro_ego_cli::DEVICE_SESSION_V2_SCHEMA;
use miro_ego_cli::runtime::{
    CaptureModeKind, DaemonServeOptions, ShmEncoderCaptureBackend, bind_daemon,
    run_bound_daemon_with_shutdown,
};
use miro_ego_core::config::{
    AudioConfig, CameraConfig, DEVICE_CONFIG_SCHEMA, DeviceConfig, DeviceIdentity, ExposureSetting,
    ListenConfig, RecordingEncoding, SecurityConfig, StorageConfig,
};
use miro_ego_core::ids::{hex_encode, new_uuid_v4};
use miro_ego_core::paths::{DataVolumePaths, RECORDING_VOLUME_LABEL};
use miro_ego_core::schema_validation::SchemaValidator;
use miro_ego_core::sys::fs::write_bytes_atomic;
use miro_ego_deploy::{
    BUNDLE_SIGNATURE_FILENAME, ReleaseManagerPaths, default_release_signing_seed,
    sign_bundle_manifest_with_seed,
};
use miro_ego_network::{
    AuthoritativeNetworkController, CommandOutput, CommandRunner, ControllerConfig,
    MockSystemClockBackend, NetworkResult, NetworkRpcClient, NetworkRpcServer, NetworkStateStore,
    NmcliBackend, RpcServerConfig,
};
use miro_ego_storage::{
    BinarySessionCatalog, CATALOG_BINARY_MAGIC, CaptureCoordinator, CaptureStartRequest,
    CaptureStopRequest, CoordinatorConfig, ExfatVolumeManager, VOLUME_METADATA_SCHEMA,
    VolumePolicy,
};
use rustls::pki_types::{CertificateDer, ServerName};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_rustls::TlsConnector;

static MOCK_ENCODER_BIN: OnceLock<PathBuf> = OnceLock::new();

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crate sits under <repo>/crates/miro-ego-cli")
        .to_path_buf()
}

fn ensure_mock_encoder_bin() -> PathBuf {
    MOCK_ENCODER_BIN
        .get_or_init(|| {
            let encoder_dir = repo_root().join("csrc/miro-ego-encoder");
            let status = Command::new("make")
                .arg("-C")
                .arg(&encoder_dir)
                .arg("tests/build/miro-ego-encoder-mock")
                .status()
                .expect("invoke make to compile miro-ego-encoder-mock");
            assert!(
                status.success(),
                "make -C csrc/miro-ego-encoder tests/build/miro-ego-encoder-mock failed with {status:?}"
            );
            let bin = encoder_dir.join("tests/build/miro-ego-encoder-mock");
            assert!(
                bin.is_file(),
                "expected compiled mock encoder at {}",
                bin.display()
            );
            bin
        })
        .clone()
}

fn temp_test_dir(tag: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "miro-ego-e2e-{tag}-{}-{unique}",
        std::process::id()
    ));
    fs::create_dir_all(&dir).expect("create temp test dir");
    dir
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

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex_encode(&h.finalize())
}

fn make_lab_device_config(data_dir: &Path, state_dir: &Path) -> DeviceConfig {
    DeviceConfig {
        schema: DEVICE_CONFIG_SCHEMA.to_owned(),
        listen: ListenConfig {
            host: "127.0.0.1".to_owned(),
            port: 8080,
        },
        camera: CameraConfig {
            device: PathBuf::from("/dev/video0"),
            width: 64,
            height: 32,
            fps: 30,
            frame_decimation: 1,
            exposure_time_absolute: Some(ExposureSetting::default()),
            calibration_file: None,
        },
        storage: StorageConfig {
            mountpoint: data_dir.to_path_buf(),
            minimum_available_bytes: 1024 * 1024,
            minimum_available_inodes: 16,
        },
        state_root: state_dir.to_path_buf(),
        device: DeviceIdentity {
            device_id: new_uuid_v4(),
            device_label: "Miro-Ego-1234ABCD".to_owned(),
            hardware_fingerprint: format!("sha256:{}", "a".repeat(64)),
        },
        security: SecurityConfig::Lab {
            isolated_network: true,
        },
        audio: AudioConfig {
            enabled: false,
            ..AudioConfig::default()
        },
        recording: RecordingEncoding {
            gop_frames: 3,
            ..RecordingEncoding::default()
        },
        segment_seconds: 1.0,
    }
}

/// Deterministic `nmcli` stub allowing a real `AuthoritativeNetworkController` +
/// `NetworkRpcServer` to run hermetically over a temp Unix socket in E2E tests.
struct StubNmcliRunner;

impl CommandRunner for StubNmcliRunner {
    fn run(
        &self,
        _program: &str,
        args: &[&str],
        _timeout: Duration,
    ) -> NetworkResult<CommandOutput> {
        let joined = args.join(" ");
        let stdout = if joined.contains("device wifi list") {
            "StudioNet:WPA2:82\nLabOpen::61\n".to_owned()
        } else if joined.contains("connection show") {
            "miro-ego-hotspot:802-11-wireless:wlan0\n".to_owned()
        } else if joined.contains("device status") {
            "wlan0:wifi:connected:miro-ego-hotspot\neth0:ethernet:disconnected:--\n".to_owned()
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

fn spawn_test_network_rpc(root: &Path, device_id: &str) -> (PathBuf, NetworkRpcServer) {
    let sock_path = root.join("network-control.sock");
    let state_path = root.join("network-state.json");
    let profiles_dir = root.join("nm-profiles");
    let run_dir = root.join("net-run");
    fs::create_dir_all(&profiles_dir).expect("create profiles dir");
    fs::create_dir_all(&run_dir).expect("create net run dir");

    let state_store = NetworkStateStore::open_or_init(&state_path).expect("init net state");
    let nmcli = NmcliBackend::new(Arc::new(StubNmcliRunner), profiles_dir, "wlan0", "eth0");
    let mock_clock = Arc::new(MockSystemClockBackend::new(
        new_uuid_v4(),
        1_764_150_000_000,
        1_000_000_000,
    ));
    let controller = Arc::new(
        AuthoritativeNetworkController::new(
            nmcli,
            state_store,
            ControllerConfig::new(device_id.to_owned(), run_dir).with_clock_backend(mock_clock),
        )
        .expect("init network controller"),
    );
    let server = NetworkRpcServer::bind(&sock_path, controller, RpcServerConfig::default())
        .expect("bind NetworkRpcServer");
    (sock_path, server)
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

async fn https_request(
    addr: SocketAddr,
    cert_der: CertificateDer<'static>,
    path: &str,
    auth_header: Option<&str>,
) -> (u16, Vec<u8>) {
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
        .method(Method::GET)
        .uri(path)
        .header("Host", format!("localhost:{}", addr.port()));
    if let Some(auth) = auth_header {
        builder = builder.header("Authorization", auth);
    }
    let req = builder
        .body(Full::new(Bytes::new()))
        .expect("build HTTPS request");
    let resp = sender.send_request(req).await.expect("send HTTPS request");
    let status = resp.status().as_u16();
    let body = resp
        .into_body()
        .collect()
        .await
        .expect("read HTTPS body")
        .to_bytes()
        .to_vec();
    (status, body)
}

async fn wait_for_capture_idle(addr: SocketAddr) -> Value {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        let (code, _, body) =
            http_request(addr, Method::GET, "/api/v1/capture/status", &[], None).await;
        assert_eq!(code, 200);
        let doc: Value = serde_json::from_slice(&body).expect("parse capture status");
        if doc["snapshot"]["device_state"] == "idle" {
            return doc;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for capture coordinator to reach idle state");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_stack_live_http_e2e_with_real_c_shm_encoder_and_network_rpc() {
    let encoder_bin = ensure_mock_encoder_bin();
    let root = temp_test_dir("full-stack");
    let data_dir = root.join("data");
    let system_dir = root.join("system-state");
    let net_dir = root.join("net");
    let run_dir = root.join("run");
    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&system_dir).unwrap();
    fs::create_dir_all(&net_dir).unwrap();
    fs::create_dir_all(&run_dir).unwrap();

    let volume_id = init_test_volume(&data_dir);
    let log_content = b"2026-11-26T10:00:00Z INFO miro-ego e2e boot ok\n";
    fs::write(data_dir.join("logs/device.log"), log_content).unwrap();
    fs::create_dir_all(data_dir.join("logs/wifi-watchdog")).unwrap();
    fs::write(
        data_dir.join("logs/wifi-watchdog/events.ndjson"),
        b"{\"event\":\"ap_active\"}\n",
    )
    .unwrap();

    let device_cfg = make_lab_device_config(&data_dir, &system_dir);
    let (net_sock, net_server) = spawn_test_network_rpc(&net_dir, &device_cfg.device.device_id);
    let direct_rpc_status = NetworkRpcClient::new(&net_sock)
        .status()
        .expect("direct RPC status check");

    let mut options = DaemonServeOptions::from_device_config(device_cfg.clone(), true);
    options.capture_mode = CaptureModeKind::ShmEncoder {
        encoder_bin,
        segment_frames: 3,
    };
    options.listen_override = Some("127.0.0.1:0".parse().unwrap());
    options.network_socket = Some(net_sock.clone());
    options.relaxed_volume = true;
    options.run_dir = Some(run_dir);
    options.release_paths = ReleaseManagerPaths {
        install_root: root.join("opt"),
        config_root: root.join("etc"),
        state_root: system_dir.clone(),
        deploy_state_root: root.join("var/lib/miro-ego-deploy"),
        run_dir: root.join("deploy-run"),
        system_root: root.join("sysroot"),
    };

    let bound = bind_daemon(options).await.expect("bind full-stack daemon");
    let addr = bound.server.local_addr();
    let origin = format!("http://{addr}");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let daemon_task = tokio::spawn(async move {
        run_bound_daemon_with_shutdown(bound, async {
            let _ = shutdown_rx.await;
        })
        .await
    });

    // 1. GET /api/v1/device
    let (code, _, body) = http_request(addr, Method::GET, "/api/v1/device", &[], None).await;
    assert_eq!(code, 200);
    let device_doc: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(device_doc["schema"], "miro-ego.device.v1");
    assert_eq!(
        device_doc["device"]["device_id"],
        device_cfg.device.device_id
    );
    assert_eq!(device_doc["storage"]["volume_id"], volume_id);
    assert_eq!(device_doc["storage"]["writable"], true);
    assert_eq!(device_doc["capabilities"]["network_mutation"], true);
    assert_eq!(device_doc["capabilities"]["preview_h264"], true);

    // 2. GET /api/v1/capture/status & /capture/readiness
    let (code, _, body) =
        http_request(addr, Method::GET, "/api/v1/capture/status", &[], None).await;
    assert_eq!(code, 200);
    let status_doc: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status_doc["snapshot"]["device_state"], "idle");

    let (code, _, body) =
        http_request(addr, Method::GET, "/api/v1/capture/readiness", &[], None).await;
    assert_eq!(code, 200);
    let ready_doc: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(ready_doc["allowed"], true);
    assert_eq!(ready_doc["code"], "ready");

    // 3. GET /api/v1/network & /network/scan (proving LIVE Unix socket RPC!)
    let (code, _, body) = http_request(addr, Method::GET, "/api/v1/network", &[], None).await;
    assert_eq!(code, 200);
    let net_doc: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(net_doc["schema"], "miro-ego.network-status.v1");
    assert_eq!(
        net_doc["authority_epoch"],
        direct_rpc_status.authority_epoch
    );
    assert_eq!(net_doc["mutation_capability"]["enabled"], true);
    assert_eq!(net_doc["desired"]["mode"], "hotspot");

    let (code, _, body) = http_request(addr, Method::GET, "/api/v1/network/scan", &[], None).await;
    assert_eq!(code, 200);
    let scan_doc: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(scan_doc["schema"], "miro-ego.network-scan.v1");
    assert_eq!(scan_doc["networks"].as_array().unwrap().len(), 2);

    // 4. GET /api/v1/clock + POST /api/v1/clock/sync, GET /api/v1/firmware
    let (code, _, body) = http_request(addr, Method::GET, "/api/v1/clock", &[], None).await;
    assert_eq!(code, 200);
    let clock_doc: Value = serde_json::from_slice(&body).unwrap();
    let challenge = clock_doc["challenge"].as_str().expect("clock challenge");
    let sync_body = serde_json::to_vec(&json!({
        "schema": "miro-ego.clock-sync-request.v1",
        "challenge": challenge,
        "unix_time_ms": 1_764_152_400_000_u64,
    }))
    .unwrap();
    let (code, _, body) = http_request(
        addr,
        Method::POST,
        "/api/v1/clock/sync",
        &[("Content-Type", "application/json"), ("Origin", &origin)],
        Some(sync_body),
    )
    .await;
    assert_eq!(code, 200);
    let synced_doc: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(synced_doc["applied"], true);

    let (code, _, body) = http_request(addr, Method::GET, "/api/v1/firmware", &[], None).await;
    assert_eq!(code, 200);
    let fw_doc: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(fw_doc["schema"], "miro-ego.firmware-status.v1");

    // 5. POST /api/v1/capture/start -> streams frames through MEGO_SHM to C miro-ego-encoder-mock!
    let start_key = new_uuid_v4();
    let start_payload = serde_json::to_vec(&json!({
        "schema": "miro-ego.capture-start.v1",
        "mode": "production",
        "display_name": "E2E Stereo SHM Take",
        "take": { "kind": "new" }
    }))
    .unwrap();
    let (code, _, body) = http_request(
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
        code,
        202,
        "capture/start body: {}",
        String::from_utf8_lossy(&body)
    );
    let start_doc: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(start_doc["snapshot"]["device_state"], "recording");
    let session_id = start_doc["snapshot"]["active_recording"]["recording_state"]["session_id"]
        .as_str()
        .expect("active session_id")
        .to_owned();

    // Let real frames flow through memfd+eventfd MEGO_SHM ring into C encoder.
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Verify 423 capture_busy gate during active recording.
    let dummy_sha = "0".repeat(64);
    let (busy_code, busy_headers, _) = http_request(
        addr,
        Method::GET,
        &format!("/api/v1/sessions/{session_id}/artifacts/{dummy_sha}"),
        &[],
        None,
    )
    .await;
    assert_eq!(busy_code, 423);
    assert_eq!(
        busy_headers
            .get("miro-ego-error-code")
            .and_then(|v| v.to_str().ok()),
        Some("capture_busy")
    );

    // 6. POST /api/v1/capture/stop -> zero-reread seal + readable publish + catalog.bin!
    let stop_key = new_uuid_v4();
    let stop_payload = serde_json::to_vec(&json!({
        "schema": "miro-ego.capture-stop.v1",
        "reason": "user"
    }))
    .unwrap();
    let (code, _, body) = http_request(
        addr,
        Method::POST,
        "/api/v1/capture/stop",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &stop_key),
        ],
        Some(stop_payload),
    )
    .await;
    assert_eq!(
        code,
        202,
        "capture/stop body: {}",
        String::from_utf8_lossy(&body)
    );
    let stop_doc: Value = serde_json::from_slice(&body).unwrap();
    assert!(
        matches!(
            stop_doc["snapshot"]["device_state"].as_str(),
            Some("finalizing" | "verifying" | "idle")
        ),
        "unexpected stop device_state: {}",
        stop_doc["snapshot"]["device_state"]
    );
    let idle_doc = wait_for_capture_idle(addr).await;
    assert_eq!(idle_doc["snapshot"]["device_state"], "idle");

    // Verify catalog.bin exists with MEGOCAT1 magic and session published under recordings/YYYY-MM-DD/HH-MM-SS/
    let catalog_bin_bytes =
        fs::read(data_dir.join(".miro-ego/index/catalog.bin")).expect("catalog.bin written");
    assert!(
        catalog_bin_bytes.starts_with(&CATALOG_BINARY_MAGIC),
        "catalog.bin must start with MEGOCAT1"
    );

    let catalog = BinarySessionCatalog::open(DataVolumePaths::new(data_dir.clone()))
        .expect("reopen catalog.bin");
    let indexed = catalog
        .get_indexed_session(&session_id)
        .expect("lookup session")
        .expect("session indexed in catalog.bin");
    let rel_parts: Vec<&str> = indexed.relative_dir.split('/').collect();
    assert_eq!(
        rel_parts.len(),
        2,
        "expected YYYY-MM-DD/HH-MM-SS layout, got {}",
        indexed.relative_dir
    );
    let session_abs_dir = data_dir.join("recordings").join(&indexed.relative_dir);
    assert!(session_abs_dir.join("manifest.json").is_file());
    assert!(session_abs_dir.join("session.mp4").is_file());
    assert!(!session_abs_dir.join("video").exists());

    // 7. GET /api/v1/sessions
    let (code, _, body) = http_request(addr, Method::GET, "/api/v1/sessions", &[], None).await;
    assert_eq!(code, 200);
    let list_doc: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(list_doc["schema"], "miro-ego.session-list.v1");
    let items = list_doc["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["session_id"], session_id);
    assert_eq!(items[0]["producer_outcome"], "sealed");

    // 8. GET /api/v1/sessions/{id} — verify Miro-Ego-Manifest-SHA256 & Draft 2020-12 Schema!
    let (code, headers, raw_manifest) = http_request(
        addr,
        Method::GET,
        &format!("/api/v1/sessions/{session_id}"),
        &[],
        None,
    )
    .await;
    assert_eq!(code, 200);
    let manifest_sha_header = headers
        .get("miro-ego-manifest-sha256")
        .and_then(|v| v.to_str().ok())
        .expect("Miro-Ego-Manifest-SHA256 header")
        .to_owned();
    assert_eq!(manifest_sha_header, sha256_hex(&raw_manifest));

    let validator = SchemaValidator::new(DEVICE_SESSION_V2_SCHEMA).expect("compile schema");
    assert!(
        validator
            .is_valid(std::str::from_utf8(&raw_manifest).unwrap())
            .expect("validate manifest"),
        "manifest must satisfy miro-ego.device-session.v2 schema"
    );

    let manifest_json: Value = serde_json::from_slice(&raw_manifest).unwrap();
    let media_art = &manifest_json["media"]["artifact"];
    let left_sha = media_art["sha256"]
        .as_str()
        .expect("media sha256")
        .to_owned();
    let left_len = media_art["bytes"].as_u64().expect("media bytes");
    assert!(left_len >= 64, "C encoder fMP4 must be non-trivial size");

    // 9. HEAD + GET full (200) + GET Range (206) /api/v1/sessions/{id}/artifacts/{sha256}
    let art_uri = format!("/api/v1/sessions/{session_id}/artifacts/{left_sha}");
    let (head_code, head_headers, head_body) =
        http_request(addr, Method::HEAD, &art_uri, &[], None).await;
    assert_eq!(head_code, 200);
    assert!(head_body.is_empty());
    assert_eq!(
        head_headers
            .get("content-length")
            .and_then(|v| v.to_str().ok()),
        Some(left_len.to_string().as_str())
    );
    assert_eq!(
        head_headers
            .get("accept-ranges")
            .and_then(|v| v.to_str().ok()),
        Some("bytes")
    );

    let (full_code, _, full_bytes) = http_request(addr, Method::GET, &art_uri, &[], None).await;
    assert_eq!(full_code, 200);
    assert_eq!(full_bytes.len() as u64, left_len);
    assert_eq!(sha256_hex(&full_bytes), left_sha);

    let (range_code, range_headers, range_bytes) = http_request(
        addr,
        Method::GET,
        &art_uri,
        &[("Range", "bytes=0-31")],
        None,
    )
    .await;
    assert_eq!(range_code, 206);
    assert_eq!(range_bytes.len(), 32);
    assert_eq!(&range_bytes[..], &full_bytes[..32]);
    assert_eq!(
        range_headers
            .get("content-range")
            .and_then(|v| v.to_str().ok()),
        Some(format!("bytes 0-31/{left_len}").as_str())
    );

    // 10. GET /api/v1/logs & GET /api/v1/logs/{name}
    let (code, _, body) = http_request(addr, Method::GET, "/api/v1/logs", &[], None).await;
    assert_eq!(code, 200);
    let logs_doc: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(logs_doc["schema"], "miro-ego.log-list.v1");
    assert_eq!(logs_doc["items"].as_array().unwrap().len(), 2);

    let (code, _, body) =
        http_request(addr, Method::GET, "/api/v1/logs/device.log", &[], None).await;
    assert_eq!(code, 200);
    assert_eq!(body.as_slice(), log_content);

    // 11. Verify audit NDJSON landed under <data>/.miro-ego/audit/ AND ZERO writes to system_dir!
    let audit_path = data_dir.join(".miro-ego/audit/api-audit.ndjson");
    assert!(
        audit_path.is_file(),
        "expected audit log at {}",
        audit_path.display()
    );
    let audit_raw = fs::read_to_string(&audit_path).unwrap();
    let audit_lines: Vec<Value> = audit_raw
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("valid audit NDJSON line"))
        .collect();
    assert!(
        !audit_lines.is_empty(),
        "audit log must record API operations"
    );
    assert!(
        audit_lines
            .iter()
            .all(|v| v["schema"] == "miro-ego.api-audit-event.v1")
    );
    let system_entries: Vec<_> = fs::read_dir(&system_dir)
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    assert!(
        system_entries.is_empty(),
        "expected ZERO writes to system_dir during capture/API/audit, found: {system_entries:?}"
    );

    // 12. POST /api/v1/sessions/delete
    let del_key = new_uuid_v4();
    let del_payload = serde_json::to_vec(&json!({
        "schema": "miro-ego.session-delete-request.v1",
        "sessions": [
            {
                "session_id": session_id,
                "manifest_sha256": manifest_sha_header,
            }
        ]
    }))
    .unwrap();
    let (code, _, body) = http_request(
        addr,
        Method::POST,
        "/api/v1/sessions/delete",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &del_key),
        ],
        Some(del_payload),
    )
    .await;
    assert_eq!(code, 200);
    let del_doc: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(del_doc["deleted_session_ids"], json!([session_id]));
    assert!(!session_abs_dir.exists());

    let (code, _, body) = http_request(addr, Method::GET, "/api/v1/sessions", &[], None).await;
    assert_eq!(code, 200);
    let after_del: Value = serde_json::from_slice(&body).unwrap();
    assert!(after_del["items"].as_array().unwrap().is_empty());

    // 13. Graceful shutdown
    let _ = shutdown_tx.send(());
    daemon_task
        .await
        .expect("daemon join")
        .expect("clean daemon exit");
    net_server.shutdown();
    let _ = fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_https_rustls_customer_profile_enforces_tls_and_bearer() {
    let root = temp_test_dir("https-customer");
    let data_dir = root.join("data");
    let state_dir = root.join("state");
    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&state_dir).unwrap();
    let _ = init_test_volume(&data_dir);

    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
            .expect("generate self-signed cert");
    let cert_der = cert.cert.der().clone();
    let cert_path = root.join("server.crt");
    let key_path = root.join("server.key");
    let token_path = root.join("bearer.token");
    let token = "customer-e2e-bearer-token-0123456789";

    fs::write(&cert_path, cert.cert.pem()).unwrap();
    fs::write(&key_path, cert.key_pair.serialize_pem()).unwrap();
    fs::write(&token_path, format!("{token}\n")).unwrap();

    let mut cfg = make_lab_device_config(&data_dir, &state_dir);
    cfg.security = SecurityConfig::Customer {
        isolated_network: true,
        bearer_token_file: token_path,
        tls_certificate_file: cert_path,
        tls_private_key_file: key_path,
        principal_id: "operator-e2e".to_owned(),
    };

    let mut options = DaemonServeOptions::from_device_config(cfg, true);
    options.listen_override = Some("127.0.0.1:0".parse().unwrap());
    options.network_socket = None;
    options.relaxed_volume = true;
    options.run_dir = None;

    let bound = bind_daemon(options).await.expect("bind HTTPS daemon");
    assert!(bound.server.is_tls(), "customer profile must enable TLS");
    let addr = bound.server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        run_bound_daemon_with_shutdown(bound, async {
            let _ = shutdown_rx.await;
        })
        .await
    });

    // Without bearer token -> 401 Unauthorized
    let (code_unauth, _) = https_request(addr, cert_der.clone(), "/api/v1/device", None).await;
    assert_eq!(code_unauth, 401);

    // With valid bearer token -> 200 OK + customer security profile
    let auth_val = format!("Bearer {token}");
    let (code_ok, body) = https_request(addr, cert_der, "/api/v1/device", Some(&auth_val)).await;
    assert_eq!(code_ok, 200);
    let doc: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(doc["security_profile"], "customer");

    let _ = shutdown_tx.send(());
    task.await
        .expect("join HTTPS task")
        .expect("HTTPS shutdown");
    let _ = fs::remove_dir_all(&root);
}

fn build_fake_aarch64_elf(tag: &[u8]) -> Vec<u8> {
    let mut elf = vec![0u8; 64];
    elf[0..4].copy_from_slice(&[0x7f, b'E', b'L', b'F']);
    elf[4] = 2; // ELFCLASS64
    elf[5] = 1; // ELFDATA2LSB
    elf[6] = 1; // EV_CURRENT
    elf[16..18].copy_from_slice(&3u16.to_le_bytes()); // ET_DYN
    elf[18..20].copy_from_slice(&183u16.to_le_bytes()); // EM_AARCH64
    elf.extend_from_slice(tag);
    elf
}

fn create_test_release_bundle(dir: &Path, commit: &str, version: &str) {
    fs::create_dir_all(dir.join("bin")).unwrap();
    fs::create_dir_all(dir.join("deploy/miro-ego")).unwrap();

    let bin_cli = build_fake_aarch64_elf(format!("miro-ego-{version}-{commit}").as_bytes());
    let bin_enc = build_fake_aarch64_elf(format!("miro-ego-encoder-{version}-{commit}").as_bytes());
    let cli_path = dir.join("bin/miro-ego");
    let enc_path = dir.join("bin/miro-ego-encoder");
    fs::write(&cli_path, &bin_cli).unwrap();
    fs::write(&enc_path, &bin_enc).unwrap();
    fs::set_permissions(&cli_path, fs::Permissions::from_mode(0o755)).unwrap();
    fs::set_permissions(&enc_path, fs::Permissions::from_mode(0o755)).unwrap();

    let asset_bytes =
        format!("# Miro-Ego journal limit ({version})\n[Journal]\nSystemMaxUse=64M\n").into_bytes();
    let asset_path = dir.join("deploy/miro-ego/miro-ego-journal.conf");
    fs::write(&asset_path, &asset_bytes).unwrap();

    let manifest = json!({
        "schema": "miro-ego.rdk-x5-bundle.v1",
        "platform": "linux_aarch64_rdk_x5_v1",
        "version": version,
        "commit": commit,
        "binaries": [
            {
                "path": "bin/miro-ego",
                "bytes": bin_cli.len(),
                "sha256": sha256_hex(&bin_cli),
                "executable": true
            },
            {
                "path": "bin/miro-ego-encoder",
                "bytes": bin_enc.len(),
                "sha256": sha256_hex(&bin_enc),
                "executable": true
            }
        ],
        "assets": [
            {
                "path": "deploy/miro-ego/miro-ego-journal.conf",
                "bytes": asset_bytes.len(),
                "sha256": sha256_hex(&asset_bytes),
                "executable": false
            }
        ]
    });
    let mut manifest_bytes = serde_json::to_vec_pretty(&manifest).unwrap();
    manifest_bytes.push(b'\n');
    let sig_hex = sign_bundle_manifest_with_seed(&manifest_bytes, &default_release_signing_seed());
    fs::write(dir.join("bundle.json"), &manifest_bytes).unwrap();
    fs::write(
        dir.join(BUNDLE_SIGNATURE_FILENAME),
        format!("{sig_hex}\n").as_bytes(),
    )
    .unwrap();
}

#[test]
fn cli_binary_subcommands_e2e() {
    let bin = env!("CARGO_BIN_EXE_miro-ego");
    let encoder_bin = ensure_mock_encoder_bin();
    let root = temp_test_dir("cli-subcmds");

    // 1. --version
    let out = Command::new(bin).arg("--version").output().unwrap();
    assert!(out.status.success());
    let ver_str = String::from_utf8_lossy(&out.stdout);
    assert!(ver_str.contains("miro-ego 0.3.0"), "got: {ver_str}");
    assert!(ver_str.contains("shm=v1"), "got: {ver_str}");
    assert!(ver_str.contains("catalog=v1"), "got: {ver_str}");

    // 2. volume init / hide / inspect (+ `--help` safety and strict exFAT enforcement)
    let vol_dir = root.join("vol");
    fs::create_dir_all(&vol_dir).unwrap();

    let help_out = Command::new(bin)
        .current_dir(&root)
        .args(["volume", "init", "--help"])
        .output()
        .unwrap();
    assert!(help_out.status.success(), "volume init --help must exit 0");
    assert!(
        !root.join("--help").exists(),
        "volume init --help must never create ./--help directory"
    );

    let strict_out = Command::new(bin)
        .args(["volume", "init"])
        .arg(&vol_dir)
        .output()
        .unwrap();
    assert!(
        !strict_out.status.success(),
        "strict volume init must refuse non-exFAT temp directory"
    );

    let out = Command::new(bin)
        .args(["volume", "init", "--relaxed-volume"])
        .arg(&vol_dir)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "volume init stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(vol_dir.join(".miro-ego/volume.json").is_file());

    let out = Command::new(bin)
        .args(["volume", "hide"])
        .arg(vol_dir.join(".miro-ego"))
        .output()
        .unwrap();
    assert!(out.status.success());

    let out = Command::new(bin)
        .args(["volume", "inspect"])
        .arg(&vol_dir)
        .output()
        .unwrap();
    assert!(out.status.success());
    let inspect_json: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(inspect_json["writable"], true);

    // 3. Seal a real session with C miro-ego-encoder-mock & verify `miro-ego validate` (pass & tampered fail)
    let paths = DataVolumePaths::new(vol_dir.clone());
    let volume = Arc::new(ExfatVolumeManager::new(
        paths.clone(),
        VolumePolicy::relaxed_for_tests(1024 * 1024),
    ));
    let catalog = Arc::new(BinarySessionCatalog::open(paths).unwrap());
    let mjpeg_hub = Arc::new(miro_ego_api::MjpegPreviewHub::new(15));
    let backend = Arc::new(ShmEncoderCaptureBackend::new(
        encoder_bin,
        64,
        32,
        30,
        3,
        mjpeg_hub,
        None,
    ));
    let coord = CaptureCoordinator::new(
        CoordinatorConfig {
            camera_width: 64,
            camera_height: 32,
            sensor_fps: 30.0,
            frame_decimation: 1,
            segment_frames: 3,
            encoding: RecordingEncoding {
                gop_frames: 3,
                ..RecordingEncoding::default()
            },
            ..CoordinatorConfig::default()
        },
        volume,
        Arc::clone(&catalog),
        backend,
    );
    let started = coord
        .start_capture(CaptureStartRequest::new_production(), Some("cli-val-start"))
        .unwrap();
    let sid = started
        .value
        .snapshot
        .active_recording
        .expect("active recording")
        .recording_state
        .session_id;
    std::thread::sleep(Duration::from_millis(180));
    coord
        .stop_capture(CaptureStopRequest::user(), Some("cli-val-stop"))
        .unwrap();
    let indexed = catalog.get_indexed_session(&sid).unwrap().unwrap();
    let session_dir = vol_dir.join("recordings").join(&indexed.relative_dir);

    let out = Command::new(bin)
        .arg("validate")
        .arg(&session_dir)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "validate stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let val_doc: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(val_doc["valid"], true);
    assert_eq!(val_doc["schema"], "miro-ego.device-session.v2");
    assert_eq!(val_doc["session_id"], sid);
    assert_eq!(val_doc["artifact_count"].as_u64().unwrap(), 1);

    // Tamper 1 byte of session.mp4 and verify `validate` rejects it!
    let mp4_path = session_dir.join("session.mp4");
    let mut mp4_bytes = fs::read(&mp4_path).unwrap();
    mp4_bytes[0] ^= 0xFF;
    fs::write(&mp4_path, &mp4_bytes).unwrap();
    let tampered_out = Command::new(bin)
        .arg("validate")
        .arg(&session_dir)
        .output()
        .unwrap();
    assert!(
        !tampered_out.status.success(),
        "validate must reject tampered artifact"
    );

    // 4. deploy install (v1 & v2) + status + rollback + recover + uninstall
    let bundle_v1 = root.join("bundle-v1");
    let bundle_v2 = root.join("bundle-v2");
    let commit_v1 = "1".repeat(40);
    let commit_v2 = "2".repeat(40);
    create_test_release_bundle(&bundle_v1, &commit_v1, "0.3.0");
    create_test_release_bundle(&bundle_v2, &commit_v2, "0.3.1");

    let install_root = root.join("deploy/opt");
    let config_root = root.join("deploy/etc");
    let state_root = root.join("deploy/state");
    let run_dir = root.join("deploy/run");
    let sys_root = root.join("deploy/sysroot");
    let root_flags = |cmd: &mut Command| {
        cmd.arg("--install-root")
            .arg(&install_root)
            .arg("--config-root")
            .arg(&config_root)
            .arg("--state-root")
            .arg(&state_root)
            .arg("--run-dir")
            .arg(&run_dir)
            .arg("--system-root")
            .arg(&sys_root)
            .arg("--no-systemctl");
    };

    let mut cmd = Command::new(bin);
    cmd.args(["deploy", "install"])
        .arg(&bundle_v1)
        .arg("--no-activate");
    root_flags(&mut cmd);
    let out = cmd.output().unwrap();
    assert!(
        out.status.success(),
        "deploy install v1 stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let mut cmd = Command::new(bin);
    cmd.args(["deploy", "install"])
        .arg(&bundle_v2)
        .arg("--no-activate");
    root_flags(&mut cmd);
    let out = cmd.output().unwrap();
    assert!(
        out.status.success(),
        "deploy install v2 stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let mut cmd = Command::new(bin);
    cmd.args(["deploy", "status"]);
    root_flags(&mut cmd);
    let out = cmd.output().unwrap();
    assert!(out.status.success());
    let deploy_st: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(deploy_st["installed"], true);
    assert_eq!(deploy_st["current"], commit_v2);
    assert_eq!(deploy_st["previous"], commit_v1);

    let mut cmd = Command::new(bin);
    cmd.args(["deploy", "rollback", "--no-activate"]);
    root_flags(&mut cmd);
    let out = cmd.output().unwrap();
    assert!(
        out.status.success(),
        "deploy rollback stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let rolled: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(rolled["current"], commit_v1);

    let mut cmd = Command::new(bin);
    cmd.args(["deploy", "recover", "--service-start"]);
    root_flags(&mut cmd);
    let out = cmd.output().unwrap();
    assert!(out.status.success());

    let mut cmd = Command::new(bin);
    cmd.args(["deploy", "uninstall"]);
    root_flags(&mut cmd);
    let out = cmd.output().unwrap();
    assert!(out.status.success());
    let uninstalled: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(uninstalled["installed"], false);

    // 5. network-control watchdog-mode & status against live NetworkRpcServer
    let net_dir = root.join("net-cli");
    fs::create_dir_all(&net_dir).unwrap();
    let (sock_path, net_srv) = spawn_test_network_rpc(&net_dir, &new_uuid_v4());

    let out = Command::new(bin)
        .args(["network-control", "watchdog-mode", "--socket"])
        .arg(&sock_path)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "watchdog-mode stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "hotspot");

    let out = Command::new(bin)
        .args(["network-control", "status", "--socket"])
        .arg(&sock_path)
        .output()
        .unwrap();
    assert!(out.status.success());
    let net_st: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(net_st["schema"], "miro-ego.network-controller-status.v1");

    // 6. probe & status
    let out = Command::new(bin).arg("probe").output().unwrap();
    assert!(out.status.success());
    let probe_doc: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(probe_doc["shm_version"], 1);

    let out = Command::new(bin)
        .arg("status")
        .arg("--network-socket")
        .arg(&sock_path)
        .output()
        .unwrap();
    assert!(out.status.success());
    let status_doc: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(status_doc["version"], "0.3.0");
    assert!(status_doc["network"].is_object());

    net_srv.shutdown();
    let _ = fs::remove_dir_all(&root);
}

/// Verifies systemd `StandardInput=socket` (fd 0, `Accept=no`) activation as configured in
/// `deploy/miro-ego/miro-ego-network.service`: passes a real listening Unix socket on fd 0
/// with `LISTEN_PID`/`LISTEN_FDS` unset, adopts it via `take_systemd_socket_fd()`, notifies
/// over an abstract `NOTIFY_SOCKET=@...`, and serves live `NetworkRpcClient` requests.
#[test]
fn systemd_standard_input_socket_fd0_activation_serves_network_rpc() {
    use miro_ego_cli::commands::{notify_systemd_to, take_systemd_socket_fd};
    use std::ffi::OsStr;
    use std::os::fd::AsRawFd;
    use std::os::linux::net::SocketAddrExt;
    use std::os::unix::net::{SocketAddr as UnixSocketAddr, UnixDatagram, UnixListener};
    use std::os::unix::process::CommandExt;

    // Child mode: spawned with listening Unix socket dup2'd onto fd 0 (`STDIN_FILENO`).
    if let Ok(child_root_str) = std::env::var("MIRO_EGO_E2E_FD0_CHILD_ROOT") {
        let child_root = PathBuf::from(child_root_str);
        let listener = take_systemd_socket_fd()
            .expect("take_systemd_socket_fd must adopt listening Unix socket on fd 0");
        let state_path = child_root.join("net-state.json");
        let profiles_dir = child_root.join("nm-profiles");
        let run_dir = child_root.join("net-run");
        fs::create_dir_all(&profiles_dir).unwrap();
        fs::create_dir_all(&run_dir).unwrap();

        let state_store = NetworkStateStore::open_or_init(&state_path).unwrap();
        let nmcli = NmcliBackend::new(Arc::new(StubNmcliRunner), profiles_dir, "wlan0", "eth0");
        let controller = Arc::new(
            AuthoritativeNetworkController::new(
                nmcli,
                state_store,
                ControllerConfig::new(new_uuid_v4(), run_dir),
            )
            .unwrap(),
        );
        let server =
            NetworkRpcServer::from_listener(listener, None, controller, RpcServerConfig::default())
                .expect("mount NetworkRpcServer on adopted fd 0 listener");

        let notify_sock = std::env::var_os("NOTIFY_SOCKET").expect("NOTIFY_SOCKET set by parent");
        assert!(notify_systemd_to(&notify_sock, "READY=1"));

        // Wait until parent touches the stop marker file, then shut down cleanly.
        let stop_marker = child_root.join("stop");
        for _ in 0..200 {
            if stop_marker.exists() {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        assert!(notify_systemd_to(&notify_sock, "STOPPING=1"));
        server.shutdown();
        return;
    }

    // Parent mode: bind listening Unix socket, bind abstract notify socket, spawn child with fd 0.
    let root = temp_test_dir("fd0-activation");
    let sock_path = root.join("network-control.sock");
    let listener = UnixListener::bind(&sock_path).expect("bind parent UnixListener");
    let listener_fd = listener.as_raw_fd();

    let abs_name = format!(
        "miro-ego-e2e-notify-{}-{}",
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

    let current_exe = std::env::current_exe().expect("locate current test executable");
    let mut cmd = Command::new(current_exe);
    cmd.args([
        "--exact",
        "systemd_standard_input_socket_fd0_activation_serves_network_rpc",
        "--nocapture",
    ])
    .env("MIRO_EGO_E2E_FD0_CHILD_ROOT", &root)
    .env("NOTIFY_SOCKET", OsStr::new(&format!("@{abs_name}")))
    .env_remove("LISTEN_PID")
    .env_remove("LISTEN_FDS");

    // SAFETY: `pre_exec` invokes only async-signal-safe `libc::dup2` to place the listening
    // socket onto `STDIN_FILENO` (fd 0), mirroring systemd `StandardInput=socket`.
    unsafe {
        cmd.pre_exec(move || {
            if libc::dup2(listener_fd, libc::STDIN_FILENO) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }

    let mut child = cmd.spawn().expect("spawn fd 0 socket activation child");
    // Drop parent listener handle to prove child owns an independent dup'd descriptor.
    drop(listener);

    let mut notify_buf = [0u8; 64];
    let n = notify_rx
        .recv(&mut notify_buf)
        .expect("receive READY=1 on abstract NOTIFY_SOCKET");
    assert_eq!(&notify_buf[..n], b"READY=1");

    // Talk to the child over the fd-0-adopted socket!
    let client = NetworkRpcClient::new(&sock_path).with_timeout(Duration::from_secs(3));
    let status = client.status().expect("RPC status over fd 0 socket");
    assert_eq!(status.schema, "miro-ego.network-controller-status.v1");
    let wd_mode = client
        .watchdog_mode()
        .expect("RPC watchdog-mode over fd 0 socket");
    assert_eq!(wd_mode.as_str(), "hotspot");

    fs::write(root.join("stop"), b"1").unwrap();
    let n_stop = notify_rx
        .recv(&mut notify_buf)
        .expect("receive STOPPING=1 on abstract NOTIFY_SOCKET");
    assert_eq!(&notify_buf[..n_stop], b"STOPPING=1");

    let exit = child.wait().expect("wait for fd 0 child");
    assert!(exit.success(), "fd 0 child exited with {exit:?}");
    let _ = fs::remove_dir_all(&root);
}

/// Regression test proving `miro-ego serve --mock` uses `<state_root>/run` (NOT `/run/miro-ego`)
/// by default so non-root `POST /api/v1/capture/start` succeeds without permission errors.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mock_serve_default_run_dir_allows_non_root_capture_start_and_stop() {
    let root = temp_test_dir("mock-nonroot-run");
    let data_dir = root.join("data");
    let state_dir = root.join("state");
    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&state_dir).unwrap();
    let _ = init_test_volume(&data_dir);

    let cfg = make_lab_device_config(&data_dir, &state_dir);
    let mut options = DaemonServeOptions::from_device_config(cfg, true);
    assert_eq!(
        options.run_dir.as_deref(),
        Some(state_dir.join("run").as_path()),
        "mock mode must default run_dir under state_root/run for non-root execution"
    );
    options.listen_override = Some("127.0.0.1:0".parse().unwrap());
    options.network_socket = None;

    let bound = bind_daemon(options)
        .await
        .expect("bind mock daemon with default run_dir");
    let addr = bound.server.local_addr();
    let origin = format!("http://{addr}");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let daemon_task = tokio::spawn(async move {
        run_bound_daemon_with_shutdown(bound, async {
            let _ = shutdown_rx.await;
        })
        .await
    });

    let start_key = new_uuid_v4();
    let start_payload = serde_json::to_vec(&json!({
        "schema": "miro-ego.capture-start.v1",
        "mode": "production",
        "take": { "kind": "new" }
    }))
    .unwrap();
    let (code, _, body) = http_request(
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
        code,
        202,
        "mock capture start must succeed non-root: {}",
        String::from_utf8_lossy(&body)
    );
    assert!(
        state_dir.join("run/maintenance.lock").is_file(),
        "maintenance.lock must be created inside state_root/run"
    );

    let stop_key = new_uuid_v4();
    let stop_payload = serde_json::to_vec(&json!({
        "schema": "miro-ego.capture-stop.v1",
        "reason": "user"
    }))
    .unwrap();
    let (code, _, body) = http_request(
        addr,
        Method::POST,
        "/api/v1/capture/stop",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &stop_key),
        ],
        Some(stop_payload),
    )
    .await;
    assert_eq!(
        code,
        202,
        "mock capture stop failed: {}",
        String::from_utf8_lossy(&body)
    );
    let _ = wait_for_capture_idle(addr).await;

    let (code, _, body) = http_request(addr, Method::GET, "/api/v1/sessions", &[], None).await;
    assert_eq!(code, 200);
    let list: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(list["items"].as_array().unwrap().len(), 1);

    let _ = shutdown_tx.send(());
    daemon_task
        .await
        .expect("join mock daemon")
        .expect("clean mock daemon exit");
    let _ = fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wave5_embedded_console_clock_guard_provenance_async_stop_network_busy_and_clean_mount() {
    let root = temp_test_dir("wave5-e2e");
    let data_dir = root.join("data");
    let system_dir = root.join("system-state");
    let net_dir = root.join("net");
    let run_dir = root.join("run");
    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&system_dir).unwrap();
    fs::create_dir_all(&net_dir).unwrap();
    fs::create_dir_all(&run_dir).unwrap();

    let _volume_id = init_test_volume(&data_dir);
    let cal_path = root.join("calibration.json");
    let cal_bytes = br#"{"schema":"miro-ego.camera-calibration.v1","baseline_m":0.063}"#;
    fs::write(&cal_path, cal_bytes).unwrap();

    let mut device_cfg = make_lab_device_config(&data_dir, &system_dir);
    device_cfg.camera.calibration_file = Some(cal_path);

    let (net_sock, net_server) = spawn_test_network_rpc(&net_dir, &device_cfg.device.device_id);

    let mut options = DaemonServeOptions::from_device_config(device_cfg.clone(), true);
    options.listen_override = Some("127.0.0.1:0".parse().unwrap());
    options.network_socket = Some(net_sock);
    options.relaxed_volume = true;
    options.run_dir = Some(run_dir.clone());
    options.require_clock_sync = true;
    options.mock_finish_delay = Duration::from_millis(250);

    let bound = bind_daemon(options).await.expect("bind wave5 daemon");
    let addr = bound.server.local_addr();
    let origin = format!("http://{addr}");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let daemon_task = tokio::spawn(async move {
        run_bound_daemon_with_shutdown(bound, async {
            let _ = shutdown_rx.await;
        })
        .await
    });

    // 1. Embedded Web Console assets (`/`, `/app.js`, `/styles.css`) match `web/assets.json`!
    let assets_manifest: Value = serde_json::from_str(include_str!("../web/assets.json")).unwrap();
    for entry in assets_manifest["files"].as_array().unwrap() {
        let asset_name = entry["path"].as_str().unwrap();
        let req_path = if asset_name == "index.html" {
            "/".to_owned()
        } else {
            format!("/{asset_name}")
        };
        let (c_code, c_hdrs, c_body) = http_request(addr, Method::GET, &req_path, &[], None).await;
        assert_eq!(c_code, 200, "GET {req_path} failed");
        assert_eq!(c_body.len() as u64, entry["bytes"].as_u64().unwrap());
        let expected_sha = entry["sha256"].as_str().unwrap();
        assert_eq!(sha256_hex(&c_body), expected_sha);
        assert_eq!(
            c_hdrs.get("etag").and_then(|v| v.to_str().ok()),
            Some(format!("\"{expected_sha}\"").as_str())
        );
        assert!(
            c_hdrs
                .get("content-security-policy")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|csp| csp.contains("default-src 'self'")),
            "missing strict CSP on {req_path}"
        );
    }

    // 2. Boot-scoped clock guard rejects `startCapture` (`409 clock_unsynchronized`) before sync!
    let start_payload = serde_json::to_vec(&json!({
        "schema": "miro-ego.capture-start.v1",
        "mode": "production",
        "display_name": "Wave 5 Clock Guarded Take",
        "take": { "kind": "new" }
    }))
    .unwrap();
    let (unsync_code, unsync_hdrs, unsync_body) = http_request(
        addr,
        Method::POST,
        "/api/v1/capture/start",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &new_uuid_v4()),
        ],
        Some(start_payload.clone()),
    )
    .await;
    assert_eq!(
        unsync_code,
        409,
        "expected 409 clock_unsynchronized, got {}: {}",
        unsync_code,
        String::from_utf8_lossy(&unsync_body)
    );
    assert_eq!(
        unsync_hdrs
            .get("miro-ego-error-code")
            .and_then(|v| v.to_str().ok()),
        Some("clock_unsynchronized")
    );

    // Synchronize clock via `/api/v1/clock` + `/api/v1/clock/sync`:
    let (clk_code, _, clk_body) = http_request(addr, Method::GET, "/api/v1/clock", &[], None).await;
    assert_eq!(clk_code, 200);
    let clk_doc: Value = serde_json::from_slice(&clk_body).unwrap();
    let challenge = clk_doc["challenge"].as_str().expect("clock challenge");
    let (sync_code, _, sync_body) = http_request(
        addr,
        Method::POST,
        "/api/v1/clock/sync",
        &[("Content-Type", "application/json"), ("Origin", &origin)],
        Some(
            serde_json::to_vec(&json!({
                "schema": "miro-ego.clock-sync-request.v1",
                "challenge": challenge,
                "unix_time_ms": 1_764_200_000_000_u64
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(sync_code, 200);
    let sync_doc: Value = serde_json::from_slice(&sync_body).unwrap();
    assert_eq!(sync_doc["applied"], true);
    assert!(
        net_dir.join("net-run/clock-state.json").is_file(),
        "clock-state.json must be persisted in controller run_dir, never state_root"
    );
    assert!(
        fs::read_dir(&system_dir).unwrap().next().is_none(),
        "system_dir must remain completely untouched"
    );

    // 3. `startCapture` now succeeds (`202 Accepted`)!
    let (start_code, _, start_resp_body) = http_request(
        addr,
        Method::POST,
        "/api/v1/capture/start",
        &[
            ("Content-Type", "application/json"),
            ("Origin", &origin),
            ("Idempotency-Key", &new_uuid_v4()),
        ],
        Some(start_payload),
    )
    .await;
    assert_eq!(start_code, 202);
    let started_doc: Value = serde_json::from_slice(&start_resp_body).unwrap();
    let session_id = started_doc["snapshot"]["active_recording"]["recording_state"]["session_id"]
        .as_str()
        .unwrap()
        .to_owned();

    // 4. Network mutations during active capture return `503 network_mutation_unavailable` (`reason: "capture_active"`)!
    let (cred_busy_code, cred_busy_hdrs, cred_busy_body) = http_request(
        addr,
        Method::POST,
        "/api/v1/network/credentials",
        &[("Content-Type", "application/json"), ("Origin", &origin)],
        Some(
            serde_json::to_vec(&json!({
                "schema": "miro-ego.network-credential-request.v1",
                "passphrase": "validpassword123"
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(cred_busy_code, 503);
    assert_eq!(
        cred_busy_hdrs
            .get("miro-ego-error-code")
            .and_then(|v| v.to_str().ok()),
        Some("network_mutation_unavailable")
    );
    let cred_err: Value = serde_json::from_slice(&cred_busy_body).unwrap();
    assert_eq!(cred_err["error"]["code"], "network_mutation_unavailable");
    assert_eq!(cred_err["error"]["details"]["reason"], "capture_active");

    let (apply_busy_code, _, apply_busy_body) = http_request(
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
                    "wifi_client": Value::Null,
                    "ethernet": Value::Null
                }
            }))
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(apply_busy_code, 503);
    let apply_err: Value = serde_json::from_slice(&apply_busy_body).unwrap();
    assert_eq!(apply_err["error"]["code"], "network_mutation_unavailable");
    assert_eq!(apply_err["error"]["details"]["reason"], "capture_active");

    // 5. Async `stopCapture` (`reason: "user"`) returns `202` immediately (`finalizing`) while 250ms drain runs in background!
    let stop_t0 = std::time::Instant::now();
    let (stop_code, _, stop_body) = http_request(
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
    let stop_latency = stop_t0.elapsed();
    assert_eq!(stop_code, 202);
    assert!(
        stop_latency < Duration::from_millis(180),
        "async stopCapture should return before 250ms mock drain, took {stop_latency:?}"
    );
    let stop_doc: Value = serde_json::from_slice(&stop_body).unwrap();
    assert_eq!(
        stop_doc["snapshot"]["device_state"], "finalizing",
        "async stopCapture immediate snapshot must report finalizing"
    );

    let idle_doc = wait_for_capture_idle(addr).await;
    assert_eq!(idle_doc["snapshot"]["device_state"], "idle");

    // 6. Verify sealed manifest embeds BOTH `logs/clock-calibration.json` and `logs/camera-calibration.json`!
    let (m_code, _, raw_manifest) = http_request(
        addr,
        Method::GET,
        &format!("/api/v1/sessions/{session_id}"),
        &[],
        None,
    )
    .await;
    assert_eq!(m_code, 200);
    let validator = SchemaValidator::new(DEVICE_SESSION_V2_SCHEMA).expect("compile schema");
    assert!(
        validator
            .is_valid(std::str::from_utf8(&raw_manifest).unwrap())
            .unwrap()
    );
    let manifest_doc: Value = serde_json::from_slice(&raw_manifest).unwrap();
    let logs_arr = manifest_doc["logs"]
        .as_array()
        .expect("manifest logs array");
    let clock_log = logs_arr
        .iter()
        .find(|l| l["path"] == "logs/clock-calibration.json")
        .expect("logs/clock-calibration.json must be embedded in sealed manifest");
    let cam_cal_log = logs_arr
        .iter()
        .find(|l| l["path"] == "logs/camera-calibration.json")
        .expect("logs/camera-calibration.json must be embedded in sealed manifest");
    let cam_cal_sha = cam_cal_log["sha256"].as_str().unwrap();
    let (cam_art_code, _, cam_art_bytes) = http_request(
        addr,
        Method::GET,
        &format!("/api/v1/sessions/{session_id}/artifacts/{cam_cal_sha}"),
        &[],
        None,
    )
    .await;
    assert_eq!(cam_art_code, 200);
    assert_eq!(sha256_hex(&cam_art_bytes), cam_cal_sha);
    let cam_cal_doc: Value = serde_json::from_slice(&cam_art_bytes).unwrap();
    assert_eq!(cam_cal_doc["schema"], "miro-ego.camera-calibration.v1");
    assert_eq!(cam_cal_doc["baseline_m"], 0.063);

    let clock_sha = clock_log["sha256"].as_str().unwrap();
    let (clk_art_code, _, clk_art_bytes) = http_request(
        addr,
        Method::GET,
        &format!("/api/v1/sessions/{session_id}/artifacts/{clock_sha}"),
        &[],
        None,
    )
    .await;
    assert_eq!(clk_art_code, 200);
    let clk_cal_doc: Value = serde_json::from_slice(&clk_art_bytes).unwrap();
    assert_eq!(clk_cal_doc["schema"], "miro-ego.clock-calibration.v1");
    assert_eq!(clk_cal_doc["source"], "client");

    // 7. Clean shutdown writes `.miro-ego/index/mount.clean` and `open_read_only` preserves it!
    let _ = shutdown_tx.send(());
    daemon_task
        .await
        .expect("join wave5 daemon")
        .expect("clean wave5 daemon exit");
    net_server.shutdown();

    let clean_marker = DataVolumePaths::new(data_dir.clone()).mount_clean_marker_file();
    assert!(
        clean_marker.is_file(),
        "clean daemon shutdown must persist mount-clean.marker"
    );
    let ro_cat =
        BinarySessionCatalog::open_read_only(DataVolumePaths::new(data_dir.clone())).unwrap();
    assert_eq!(
        ro_cat
            .list_sessions_page(Some(10), None, true, None)
            .unwrap()
            .items
            .len(),
        1
    );
    assert!(
        clean_marker.is_file(),
        "open_read_only must not consume mount.clean"
    );

    let _ = fs::remove_dir_all(&root);
}
