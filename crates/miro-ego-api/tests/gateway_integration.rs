//! Comprehensive live HTTP and `rustls` HTTPS integration tests for Miro-Ego Device API v1.

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1 as client_http1;
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use miro_ego_api::{
    CORS_EXPOSE_HEADERS, GatewayBackend, GatewayRuntimeConfig, GatewayServer, GatewayServerConfig,
    HEADER_AUDIT_STATUS, HEADER_ERROR_CODE, HEADER_MANIFEST_SHA256, HEADER_WAIT_STATE,
    MJPEG_CONTENT_TYPE, MockGatewayBackend, Principal, ROUTE_TABLE, SecurityPolicy, TlsConfig,
};
use miro_ego_core::ids::{hex_encode, new_uuid_v4, new_uuid_v7};
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, ServerName};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_rustls::TlsConnector;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

struct RunningServer {
    addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<()>,
    state: Arc<miro_ego_api::GatewayState>,
}

impl RunningServer {
    async fn start_http(
        backend: Arc<MockGatewayBackend>,
        security: Arc<SecurityPolicy>,
        runtime: GatewayRuntimeConfig,
    ) -> Self {
        let mut cfg = GatewayServerConfig::http("127.0.0.1:0".parse().unwrap());
        cfg.runtime = runtime;
        let server = GatewayServer::bind(cfg, backend, security).await.unwrap();
        let addr = server.local_addr();
        let state = server.state();
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let _ = server
                .serve_with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await;
        });
        Self {
            addr,
            shutdown_tx: Some(tx),
            handle,
            state,
        }
    }

    async fn start_https(
        backend: Arc<MockGatewayBackend>,
        security: Arc<SecurityPolicy>,
        tls: TlsConfig,
        handshake_timeout: Duration,
    ) -> Self {
        let mut cfg = GatewayServerConfig::https("127.0.0.1:0".parse().unwrap(), tls);
        cfg.tls_handshake_timeout = handshake_timeout;
        let server = GatewayServer::bind(cfg, backend, security).await.unwrap();
        let addr = server.local_addr();
        let state = server.state();
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let _ = server
                .serve_with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await;
        });
        Self {
            addr,
            shutdown_tx: Some(tx),
            handle,
            state,
        }
    }

    async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = tokio::time::timeout(Duration::from_secs(3), self.handle).await;
    }
}

async fn http_request(
    addr: SocketAddr,
    method: Method,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<Bytes>,
) -> (StatusCode, hyper::HeaderMap, Bytes) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) = client_http1::handshake(TokioIo::new(stream)).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut builder = Request::builder().method(method).uri(path);
    let mut has_host = false;
    for (k, v) in headers {
        if k.eq_ignore_ascii_case("host") {
            has_host = true;
        }
        builder = builder.header(*k, *v);
    }
    if !has_host {
        builder = builder.header("Host", format!("127.0.0.1:{}", addr.port()));
    }
    let req = builder.body(Full::new(body.unwrap_or_default())).unwrap();
    let resp = sender.send_request(req).await.unwrap();
    let status = resp.status();
    let resp_headers = resp.headers().clone();
    let collected = resp.into_body().collect().await.unwrap().to_bytes();
    (status, resp_headers, collected)
}

async fn https_request(
    addr: SocketAddr,
    connector: &TlsConnector,
    method: Method,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<Bytes>,
) -> (StatusCode, hyper::HeaderMap, Bytes) {
    let tcp = TcpStream::connect(addr).await.unwrap();
    let server_name = ServerName::try_from("localhost".to_owned()).unwrap();
    let tls_stream = connector.connect(server_name, tcp).await.unwrap();
    let (mut sender, conn) = client_http1::handshake(TokioIo::new(tls_stream))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut builder = Request::builder().method(method).uri(path);
    let mut has_host = false;
    for (k, v) in headers {
        if k.eq_ignore_ascii_case("host") {
            has_host = true;
        }
        builder = builder.header(*k, *v);
    }
    if !has_host {
        builder = builder.header("Host", format!("localhost:{}", addr.port()));
    }
    let req = builder.body(Full::new(body.unwrap_or_default())).unwrap();
    let resp = sender.send_request(req).await.unwrap();
    let status = resp.status();
    let resp_headers = resp.headers().clone();
    let collected = resp.into_body().collect().await.unwrap().to_bytes();
    (status, resp_headers, collected)
}

fn generate_test_tls() -> (TlsConfig, TlsConnector) {
    let certified =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
            .unwrap();
    let cert_pem = certified.cert.pem();
    let key_pem = certified.key_pair.serialize_pem();
    let server_tls = TlsConfig::from_pem_bytes(cert_pem.as_bytes(), key_pem.as_bytes()).unwrap();

    let mut roots = RootCertStore::empty();
    let cert_der = CertificateDer::from(certified.cert.der().to_vec());
    roots.add(cert_der).unwrap();

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let client_cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (server_tls, TlsConnector::from(Arc::new(client_cfg)))
}

/// Extract every `(path, method_upper, operationId)` declared in `contracts/miro-ego-device-v1.openapi.yaml`.
fn parse_openapi_operations(yaml: &str) -> BTreeMap<(String, String), String> {
    let mut ops = BTreeMap::new();
    let mut in_paths = false;
    let mut current_path: Option<String> = None;
    let mut current_method: Option<String> = None;

    for line in yaml.lines() {
        if !line.starts_with(' ') && !line.is_empty() {
            in_paths = line.trim_end() == "paths:";
            current_path = None;
            current_method = None;
            continue;
        }
        if !in_paths {
            continue;
        }
        if let Some(rest) = line.strip_prefix("  ")
            && !rest.starts_with(' ')
            && let Some(path) = rest.trim_end().strip_suffix(':')
            && path.starts_with('/')
        {
            current_path = Some(path.to_owned());
            current_method = None;
            continue;
        }
        if let Some(rest) = line.strip_prefix("    ")
            && !rest.starts_with(' ')
            && let Some(method) = rest.trim_end().strip_suffix(':')
            && matches!(method, "get" | "post" | "put" | "patch" | "delete" | "head")
        {
            current_method = Some(method.to_ascii_uppercase());
            continue;
        }
        if let Some(rest) = line.strip_prefix("      operationId:") {
            let op_id = rest.trim().to_owned();
            if let (Some(p), Some(m)) = (current_path.clone(), current_method.clone()) {
                ops.insert((p, m), op_id);
            }
        }
    }
    ops
}

#[tokio::test]
async fn all_37_openapi_operations_match_contract_and_respond_with_options_and_405() {
    let root = repo_root();
    let openapi_bytes = fs::read(root.join("contracts/miro-ego-device-v1.openapi.yaml")).unwrap();
    let support_text =
        fs::read_to_string(root.join("contracts/miro-ego-api-support.json")).unwrap();
    let support: Value = serde_json::from_str(&support_text).unwrap();

    // Verify OpenAPI SHA-256 & byte length match contracts/miro-ego-api-support.json.
    let mut hasher = Sha256::new();
    hasher.update(&openapi_bytes);
    let actual_sha = hex_encode(&hasher.finalize());
    assert_eq!(
        support["required_contracts"][0]["sha256"].as_str(),
        Some(actual_sha.as_str())
    );
    assert_eq!(
        support["required_contracts"][0]["bytes"].as_u64(),
        Some(openapi_bytes.len() as u64)
    );

    let openapi_str = std::str::from_utf8(&openapi_bytes).unwrap();
    let openapi_ops = parse_openapi_operations(openapi_str);
    assert_eq!(
        openapi_ops.len(),
        37,
        "OpenAPI v1 must define 37 operations"
    );

    // Cross-check ROUTE_TABLE against OpenAPI YAML and support JSON.
    let mut table_ops = BTreeMap::new();
    for spec in ROUTE_TABLE {
        table_ops.insert(
            (spec.path_template.to_owned(), spec.method.to_owned()),
            spec.operation_id.to_owned(),
        );
    }
    assert_eq!(
        table_ops, openapi_ops,
        "ROUTE_TABLE must match all 37 OpenAPI v1 operations exactly"
    );

    let support_routes = support["routes"].as_array().unwrap();
    assert_eq!(support_routes.len(), 37);

    // Start live HTTP server in Lab mode with an allowed CORS origin to test OPTIONS + CORS + 405 + all 37 routes.
    let backend = Arc::new(MockGatewayBackend::new());
    let (sample_sid, sample_aid, sample_manifest_sha) =
        backend.sample_session_and_artifact().unwrap();
    let failed_sid = backend.sample_unsuccessful_session_id().unwrap();
    let retry_tx_id = backend.sample_network_transaction_id().unwrap();

    let mut allowed_origins = HashSet::new();
    allowed_origins.insert("https://console.miro-ego.local".to_owned());
    let allowed_ops: HashSet<String> = miro_ego_api::ALL_V1_OPERATION_IDS
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
    let security = Arc::new(
        SecurityPolicy::lab(
            allowed_ops,
            allowed_origins,
            Some("lab-csrf-token".to_owned()),
        )
        .unwrap(),
    );
    let srv = RunningServer::start_http(
        Arc::clone(&backend),
        security,
        GatewayRuntimeConfig::default(),
    )
    .await;

    // 1. Verify OPTIONS preflight & 405 Allow across EVERY unique path template!
    let mut visited_paths = HashSet::new();
    for spec in ROUTE_TABLE {
        if !visited_paths.insert(spec.path_template) {
            continue;
        }
        let concrete = spec
            .path_template
            .replace("{session_id}", &sample_sid)
            .replace("{artifact_id}", &sample_aid)
            .replace("{name}", "device.log");
        let full_path = format!("/api/v1{concrete}");

        // Plain OPTIONS
        let (st, hdrs, body) = http_request(srv.addr, Method::OPTIONS, &full_path, &[], None).await;
        assert_eq!(st, StatusCode::NO_CONTENT, "OPTIONS {full_path}");
        assert!(body.is_empty());
        assert_eq!(
            hdrs.get("allow").and_then(|v| v.to_str().ok()),
            Some(spec.allow_methods.join(", ").as_str()),
            "Allow header mismatch for {full_path}"
        );

        // CORS preflight OPTIONS
        let (st_cors, hdrs_cors, _) = http_request(
            srv.addr,
            Method::OPTIONS,
            &full_path,
            &[
                ("Origin", "https://console.miro-ego.local"),
                ("Access-Control-Request-Method", spec.method),
            ],
            None,
        )
        .await;
        assert_eq!(st_cors, StatusCode::NO_CONTENT, "CORS OPTIONS {full_path}");
        assert_eq!(
            hdrs_cors
                .get("access-control-expose-headers")
                .and_then(|v| v.to_str().ok()),
            Some(CORS_EXPOSE_HEADERS)
        );

        // 405 Method Not Allowed on DELETE (no v1 route allows DELETE)
        let (st_405, hdrs_405, body_405) =
            http_request(srv.addr, Method::DELETE, &full_path, &[], None).await;
        assert_eq!(st_405, StatusCode::METHOD_NOT_ALLOWED, "DELETE {full_path}");
        assert_eq!(
            hdrs_405.get("allow").and_then(|v| v.to_str().ok()),
            Some(spec.allow_methods.join(", ").as_str())
        );
        let problem: Value = serde_json::from_slice(&body_405).unwrap();
        assert_eq!(problem["schema"], "miro-ego.api-error.v1");
        assert_eq!(problem["error"]["code"], "method_not_allowed");
    }

    // 2. Static Web assets (`/`, `/app.js`, `HEAD`, `OPTIONS`) and legacy `/api/v4` rejection
    let (st_root, hdr_root, body_root) = http_request(srv.addr, Method::GET, "/", &[], None).await;
    assert_eq!(st_root, StatusCode::OK);
    assert!(hdr_root.get("etag").is_some());
    assert!(String::from_utf8_lossy(&body_root).contains("Miro-Ego"));
    let (st_head_root, _, body_head_root) =
        http_request(srv.addr, Method::HEAD, "/", &[], None).await;
    assert_eq!(st_head_root, StatusCode::OK);
    assert!(body_head_root.is_empty());
    let (st_v4, _, _) = http_request(srv.addr, Method::GET, "/api/v4/device", &[], None).await;
    assert_eq!(st_v4, StatusCode::NOT_FOUND);

    // 3. Exercise all 37 live API v1 endpoints!
    let csrf_headers = [
        ("Content-Type", "application/json"),
        ("X-CSRF-Token", "lab-csrf-token"),
    ];
    let cmd_headers = |key: &'static str| -> Vec<(&'static str, &'static str)> {
        vec![
            ("Content-Type", "application/json"),
            ("X-CSRF-Token", "lab-csrf-token"),
            ("Idempotency-Key", key),
        ]
    };

    // GET /device
    let (st, _, b) = http_request(srv.addr, Method::GET, "/api/v1/device", &[], None).await;
    assert_eq!(st, StatusCode::OK);
    let dev: Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(dev["schema"], "miro-ego.device.v1");
    assert_eq!(dev["api_version"], "1.0");
    assert_eq!(dev["capabilities"]["preview_h264"], true);
    assert_eq!(dev["capabilities"]["diagnostic_logs"], true);

    // GET & POST /voice
    let (st, _, b) = http_request(srv.addr, Method::GET, "/api/v1/voice", &[], None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&b).unwrap()["schema"],
        "miro-ego.voice.v1"
    );
    let (st, _, b) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/voice",
        &csrf_headers,
        Some(Bytes::from(
            json!({"schema": "miro-ego.voice-set.v1", "volume": 65}).to_string(),
        )),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(serde_json::from_slice::<Value>(&b).unwrap()["volume"], 65);

    // GET /clock, GET /clock/check, POST /clock/sync
    let (st, _, b) = http_request(srv.addr, Method::GET, "/api/v1/clock", &[], None).await;
    assert_eq!(st, StatusCode::OK);
    let clk: Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(clk["schema"], "miro-ego.clock-status.v1");
    let (st, _, b) = http_request(srv.addr, Method::GET, "/api/v1/clock/check", &[], None).await;
    assert_eq!(st, StatusCode::OK);
    let chk: Value = serde_json::from_slice(&b).unwrap();
    let challenge = chk["challenge"].as_str().unwrap().to_owned();
    let (st, _, b) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/clock/sync",
        &csrf_headers,
        Some(Bytes::from(
            json!({
                "schema": "miro-ego.clock-sync-request.v1",
                "challenge": challenge,
                "unix_time_ms": 1_764_152_700_000_u64,
                "replace": true
            })
            .to_string(),
        )),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&b).unwrap()["applied"],
        true
    );

    // GET /firmware, GET /firmware/check, POST /firmware/update, POST /firmware/rollback
    let (st, _, b) = http_request(srv.addr, Method::GET, "/api/v1/firmware", &[], None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&b).unwrap()["schema"],
        "miro-ego.firmware-status.v1"
    );
    let (st, _, _) = http_request(
        srv.addr,
        Method::GET,
        "/api/v1/firmware/check?force=true",
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let commit_body = Bytes::from(json!({"commit": "3".repeat(40)}).to_string());
    let (st, _, _) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/firmware/update",
        &cmd_headers("fw-upd-1"),
        Some(commit_body.clone()),
    )
    .await;
    assert_eq!(st, StatusCode::ACCEPTED);
    // Replay idempotency key -> Idempotency-Replayed: true
    let (st, hdrs, _) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/firmware/update",
        &cmd_headers("fw-upd-1"),
        Some(commit_body),
    )
    .await;
    assert_eq!(st, StatusCode::ACCEPTED);
    assert_eq!(
        hdrs.get("idempotency-replayed")
            .and_then(|v| v.to_str().ok()),
        Some("true")
    );
    let (st, _, _) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/firmware/rollback",
        &cmd_headers("fw-rb-1"),
        Some(Bytes::from(json!({"commit": "2".repeat(40)}).to_string())),
    )
    .await;
    assert_eq!(st, StatusCode::ACCEPTED);

    // GET & POST /camera/focus
    let (st, _, b) = http_request(srv.addr, Method::GET, "/api/v1/camera/focus", &[], None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&b).unwrap()["schema"],
        "miro-ego.camera-focus.v1"
    );
    let (st, _, b) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/camera/focus",
        &cmd_headers("focus-1"),
        Some(Bytes::from(
            json!({"schema": "miro-ego.camera-focus-set.v1", "value": 400}).to_string(),
        )),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(serde_json::from_slice::<Value>(&b).unwrap()["value"], 400);

    // GET /capture/readiness, /status, /safe-swap, GET & POST /timer, POST /start & /stop
    let (st, _, b) = http_request(
        srv.addr,
        Method::GET,
        "/api/v1/capture/readiness",
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&b).unwrap()["schema"],
        "miro-ego.capture-readiness.v1"
    );
    let (st, _, b) = http_request(srv.addr, Method::GET, "/api/v1/capture/status", &[], None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&b).unwrap()["schema"],
        "miro-ego.capture-status.v1"
    );
    let (st, _, b) = http_request(
        srv.addr,
        Method::GET,
        "/api/v1/capture/safe-swap",
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&b).unwrap()["schema"],
        "miro-ego.safe-swap-receipt-resource.v1"
    );
    let (st, _, b) = http_request(srv.addr, Method::GET, "/api/v1/capture/timer", &[], None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&b).unwrap()["schema"],
        "miro-ego.capture-timer.v1"
    );
    let (st, _, _) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/capture/timer",
        &csrf_headers,
        Some(Bytes::from(
            json!({"schema": "miro-ego.capture-timer-set.v1", "enabled": true, "duration_seconds": 300}).to_string(),
        )),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    // Idle stop -> 204
    let (st, _, _) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/capture/stop",
        &cmd_headers("stop-idle-1"),
        Some(Bytes::from(
            json!({"schema": "miro-ego.capture-stop.v1", "reason": "user"}).to_string(),
        )),
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    // Start -> 202, then stop -> 202
    let (st, _, _) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/capture/start",
        &cmd_headers("start-1"),
        Some(Bytes::from(
            json!({"schema": "miro-ego.capture-start.v1", "mode": "production", "take": {"kind": "new"}}).to_string(),
        )),
    )
    .await;
    assert_eq!(st, StatusCode::ACCEPTED);
    let (st, _, _) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/capture/stop",
        &cmd_headers("stop-active-1"),
        Some(Bytes::from(
            json!({"schema": "miro-ego.capture-stop.v1", "reason": "user"}).to_string(),
        )),
    )
    .await;
    assert_eq!(st, StatusCode::ACCEPTED);

    // Network: GET /network, /network/scan, POST /network/credentials, /apply, /retry, /forget
    let (st, _, b) = http_request(srv.addr, Method::GET, "/api/v1/network", &[], None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&b).unwrap()["schema"],
        "miro-ego.network-status.v1"
    );
    let (st, _, b) = http_request(srv.addr, Method::GET, "/api/v1/network/scan", &[], None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&b).unwrap()["schema"],
        "miro-ego.network-scan.v1"
    );
    let (st, _, b) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/network/credentials",
        &csrf_headers,
        Some(Bytes::from(
            json!({"schema": "miro-ego.network-credential-request.v1", "passphrase": "supersecret123"}).to_string(),
        )),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED);
    let cred_ref = serde_json::from_slice::<Value>(&b).unwrap()["credential_ref"]
        .as_str()
        .unwrap()
        .to_owned();
    let (st, _, _) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/network/apply",
        &cmd_headers("net-apply-1"),
        Some(Bytes::from(
            json!({
                "schema": "miro-ego.network-apply-request.v1",
                "desired": {
                    "mode": "wifi-client",
                    "wifi_client": {
                        "ssid": "studio-wifi",
                        "security": "wpa2-personal",
                        "credential_ref": cred_ref
                    },
                    "ethernet": null
                }
            })
            .to_string(),
        )),
    )
    .await;
    assert_eq!(st, StatusCode::ACCEPTED);
    let (st, _, _) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/network/retry",
        &cmd_headers("net-retry-1"),
        Some(Bytes::from(
            json!({
                "schema": "miro-ego.network-retry-request.v1",
                "transaction_id": retry_tx_id
            })
            .to_string(),
        )),
    )
    .await;
    assert_eq!(st, StatusCode::ACCEPTED);
    let (st, _, _) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/network/forget",
        &cmd_headers("net-forget-1"),
        Some(Bytes::from(
            json!({"schema": "miro-ego.network-forget-request.v1"}).to_string(),
        )),
    )
    .await;
    assert_eq!(st, StatusCode::ACCEPTED);

    // Sessions: GET /sessions, /sessions/{id}, /unsuccessful-outcome, GET & HEAD artifact, POST /sessions/delete
    let (st, _, b) = http_request(
        srv.addr,
        Method::GET,
        "/api/v1/sessions?limit=10",
        &[("Prefer", "include-recovery")],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let list_json: Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(list_json["schema"], "miro-ego.session-list.v1");
    assert_eq!(list_json["items"][0]["recovered_prefix"], true);

    let (st, hdrs, _) = http_request(
        srv.addr,
        Method::GET,
        &format!("/api/v1/sessions/{sample_sid}"),
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        hdrs.get(HEADER_MANIFEST_SHA256)
            .and_then(|v| v.to_str().ok()),
        Some(sample_manifest_sha.as_str())
    );

    let (st, _, b) = http_request(
        srv.addr,
        Method::GET,
        &format!("/api/v1/sessions/{failed_sid}/unsuccessful-outcome"),
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&b).unwrap()["schema"],
        "miro-ego.retained-unsuccessful-session-resource.v1"
    );

    let artifact_path = format!("/api/v1/sessions/{sample_sid}/artifacts/{sample_aid}");
    let (st, _, b_head) = http_request(srv.addr, Method::HEAD, &artifact_path, &[], None).await;
    assert_eq!(st, StatusCode::OK);
    assert!(b_head.is_empty());
    let (st, _, b_get) = http_request(srv.addr, Method::GET, &artifact_path, &[], None).await;
    assert_eq!(st, StatusCode::OK);
    assert!(!b_get.is_empty());

    let (st, _, b_del) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/sessions/delete",
        &cmd_headers("del-1"),
        Some(Bytes::from(
            json!({
                "schema": "miro-ego.session-delete-request.v1",
                "sessions": [{
                    "session_id": sample_sid,
                    "manifest_sha256": sample_manifest_sha
                }]
            })
            .to_string(),
        )),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&b_del).unwrap()["deleted_session_ids"][0],
        sample_sid
    );

    // Logs & single JPEG preview
    let (st, _, b_logs) = http_request(srv.addr, Method::GET, "/api/v1/logs", &[], None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&b_logs).unwrap()["schema"],
        "miro-ego.log-list.v1"
    );
    let (st, _, _) =
        http_request(srv.addr, Method::GET, "/api/v1/logs/device.log", &[], None).await;
    assert_eq!(st, StatusCode::OK);
    let (st, hdrs_prev, b_prev) = http_request(
        srv.addr,
        Method::GET,
        "/api/v1/preview",
        &[("Accept", "image/jpeg")],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        hdrs_prev.get("content-type").and_then(|v| v.to_str().ok()),
        Some("image/jpeg")
    );
    assert!(!b_prev.is_empty());

    srv.shutdown().await;
}

#[tokio::test]
async fn live_rustls_https_enforces_bearer_auth_csrf_permissions_host_rebinding_and_timeout() {
    let (tls_cfg, connector) = generate_test_tls();
    let backend = Arc::new(MockGatewayBackend::new());

    let admin = Principal::all_v1_operations("admin-client").unwrap();
    let mut ro_perms = HashMap::new();
    ro_perms.insert("getDevice".to_owned(), None);
    let reader = Principal::new("readonly-client", ro_perms).unwrap();

    let mut tokens = HashMap::new();
    tokens.insert("admin-secret-token-9999".to_owned(), admin);
    tokens.insert("readonly-secret-token-1".to_owned(), reader);

    let mut origins = HashSet::new();
    origins.insert("https://app.miro-ego.local".to_owned());

    let security = Arc::new(
        SecurityPolicy::customer(tokens, origins, Some("customer-csrf-secret".to_owned())).unwrap(),
    );

    let srv = RunningServer::start_https(
        Arc::clone(&backend),
        security,
        tls_cfg,
        Duration::from_millis(250),
    )
    .await;

    let https_origin = format!("https://localhost:{}", srv.addr.port());

    // 1. Missing bearer -> 401 with WWW-Authenticate: Bearer
    let (st, hdrs, body) = https_request(
        srv.addr,
        &connector,
        Method::GET,
        "/api/v1/device",
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
    assert_eq!(
        hdrs.get("www-authenticate").and_then(|v| v.to_str().ok()),
        Some("Bearer")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"]["code"],
        "unauthorized"
    );

    // 2. Wrong bearer -> 401
    let (st, _, _) = https_request(
        srv.addr,
        &connector,
        Method::GET,
        "/api/v1/device",
        &[("Authorization", "Bearer wrong-token-value")],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    // 3. Valid readonly token can read /device (200) but is 403 Forbidden on /voice
    let (st, _, body) = https_request(
        srv.addr,
        &connector,
        Method::GET,
        "/api/v1/device",
        &[("Authorization", "Bearer readonly-secret-token-1")],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["security_profile"],
        "customer"
    );
    let (st, _, body) = https_request(
        srv.addr,
        &connector,
        Method::GET,
        "/api/v1/voice",
        &[("Authorization", "Bearer readonly-secret-token-1")],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"]["code"],
        "forbidden"
    );

    // 4. Customer mutation missing Origin -> 403 origin_forbidden
    let voice_payload =
        Bytes::from(json!({"schema": "miro-ego.voice-set.v1", "volume": 42}).to_string());
    let (st, _, body) = https_request(
        srv.addr,
        &connector,
        Method::POST,
        "/api/v1/voice",
        &[
            ("Authorization", "Bearer admin-secret-token-9999"),
            ("Content-Type", "application/json"),
            ("X-CSRF-Token", "customer-csrf-secret"),
        ],
        Some(voice_payload.clone()),
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"]["code"],
        "origin_forbidden"
    );

    // 5. Customer mutation with Origin but missing/wrong X-CSRF-Token -> 403 csrf_forbidden
    let (st, _, body) = https_request(
        srv.addr,
        &connector,
        Method::POST,
        "/api/v1/voice",
        &[
            ("Authorization", "Bearer admin-secret-token-9999"),
            ("Origin", &https_origin),
            ("Content-Type", "application/json"),
            ("X-CSRF-Token", "wrong-csrf"),
        ],
        Some(voice_payload.clone()),
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"]["code"],
        "csrf_forbidden"
    );

    // 6. Customer mutation with valid Bearer + same-origin Origin + X-CSRF-Token -> 200 OK
    let (st, _, body) = https_request(
        srv.addr,
        &connector,
        Method::POST,
        "/api/v1/voice",
        &[
            ("Authorization", "Bearer admin-secret-token-9999"),
            ("Origin", &https_origin),
            ("Content-Type", "application/json"),
            ("X-CSRF-Token", "customer-csrf-secret"),
        ],
        Some(voice_payload),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["volume"],
        42
    );

    // 7. Host DNS-rebinding rejection on BOTH HTTP GET and OPTIONS (0.0.0.0, [::], evil.example)
    for bad_host in ["0.0.0.0:443", "[::]:443", "attacker.example.com"] {
        let (st_get, _, b_get) = https_request(
            srv.addr,
            &connector,
            Method::GET,
            "/api/v1/device",
            &[
                ("Host", bad_host),
                ("Authorization", "Bearer admin-secret-token-9999"),
            ],
            None,
        )
        .await;
        assert_eq!(st_get, StatusCode::FORBIDDEN, "GET Host={bad_host}");
        assert_eq!(
            serde_json::from_slice::<Value>(&b_get).unwrap()["error"]["code"],
            "origin_forbidden"
        );

        let (st_opt, _, b_opt) = https_request(
            srv.addr,
            &connector,
            Method::OPTIONS,
            "/api/v1/device",
            &[("Host", bad_host)],
            None,
        )
        .await;
        assert_eq!(st_opt, StatusCode::FORBIDDEN, "OPTIONS Host={bad_host}");
        assert_eq!(
            serde_json::from_slice::<Value>(&b_opt).unwrap()["error"]["code"],
            "origin_forbidden"
        );
    }

    // 8. Bounded TLS handshake timeout: stalled TCP peer gets closed promptly by server
    let mut stalled = TcpStream::connect(srv.addr).await.unwrap();
    let mut scratch = [0_u8; 16];
    let read_res = tokio::time::timeout(Duration::from_secs(2), stalled.read(&mut scratch))
        .await
        .expect("server must close stalled TLS handshake within timeout");
    assert!(matches!(read_res, Ok(0) | Err(_)));

    // 9. Fail-closed audit sink -> 500 InternalError with Miro-Ego-Audit-Status: unavailable
    backend.set_fail_audit(true);
    let (st, hdrs, body) = https_request(
        srv.addr,
        &connector,
        Method::GET,
        "/api/v1/device",
        &[("Authorization", "Bearer admin-secret-token-9999")],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        hdrs.get(HEADER_AUDIT_STATUS).and_then(|v| v.to_str().ok()),
        Some("unavailable")
    );
    assert_eq!(
        hdrs.get(HEADER_ERROR_CODE).and_then(|v| v.to_str().ok()),
        Some("audit_unavailable")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"]["code"],
        "audit_unavailable"
    );

    srv.shutdown().await;
}

#[tokio::test]
async fn range_if_range_fadvise_file_download_gates_and_concurrency_throttling() {
    let backend = Arc::new(MockGatewayBackend::new());
    let (sid, _, _) = backend.sample_session_and_artifact().unwrap();

    // Create a 1.5 MiB real file artifact to exercise DownloadSource::File + fadvise_sequential/dontneed.
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let tmp_dir = std::env::temp_dir().join(format!(
        "miro-ego-api-range-test-{}-{unique}",
        std::process::id()
    ));
    fs::create_dir(&tmp_dir).unwrap();
    let file_path = tmp_dir.join("clip.mp4");
    let total_size = 1_500_000usize;
    let payload: Vec<u8> = (0..total_size).map(|i| (i % 251) as u8).collect();
    fs::write(&file_path, &payload).unwrap();
    let mut hasher = Sha256::new();
    hasher.update(&payload);
    let aid = hex_encode(&hasher.finalize());
    let etag = format!("\"{aid}\"");
    backend
        .add_file_artifact(&sid, aid.clone(), "video/mp4".to_owned(), file_path)
        .unwrap();

    let runtime = GatewayRuntimeConfig {
        max_concurrent_downloads: 1,
        download_chunk_bytes: 256 * 1024,
        ..GatewayRuntimeConfig::default()
    };
    let srv = RunningServer::start_http(
        Arc::clone(&backend),
        Arc::new(SecurityPolicy::lab_default()),
        runtime,
    )
    .await;

    let path = format!("/api/v1/sessions/{sid}/artifacts/{aid}");

    // 1. Full 200 GET over real file
    let (st, hdrs, body) = http_request(srv.addr, Method::GET, &path, &[], None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        hdrs.get("etag").and_then(|v| v.to_str().ok()),
        Some(etag.as_str())
    );
    assert_eq!(
        hdrs.get("accept-ranges").and_then(|v| v.to_str().ok()),
        Some("bytes")
    );
    assert_eq!(body.len(), total_size);
    assert_eq!(&body[..], &payload[..]);

    // 2. Single 206 Range with matching If-Range
    let (st, hdrs, body) = http_request(
        srv.addr,
        Method::GET,
        &path,
        &[("Range", "bytes=1000-4095"), ("If-Range", &etag)],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        hdrs.get("content-range").and_then(|v| v.to_str().ok()),
        Some(format!("bytes 1000-4095/{total_size}").as_str())
    );
    assert_eq!(&body[..], &payload[1000..=4095]);

    // 3. Mismatched If-Range ignores Range and returns full 200
    let wrong_etag = format!("\"{}\"", "0".repeat(64));
    let (st, hdrs, body) = http_request(
        srv.addr,
        Method::GET,
        &path,
        &[("Range", "bytes=0-10"), ("If-Range", &wrong_etag)],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(hdrs.get("content-range").is_none());
    assert_eq!(body.len(), total_size);

    // 4. Multi-range & out-of-bounds -> 416 with Content-Range: bytes */<size>
    for bad_range in ["bytes=0-10, 20-30", "bytes=99999999-"] {
        let (st, hdrs, body) =
            http_request(srv.addr, Method::GET, &path, &[("Range", bad_range)], None).await;
        assert_eq!(st, StatusCode::RANGE_NOT_SATISFIABLE, "Range={bad_range}");
        assert_eq!(
            hdrs.get("content-range").and_then(|v| v.to_str().ok()),
            Some(format!("bytes */{total_size}").as_str())
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap()["error"]["code"],
            "range_not_satisfiable"
        );
    }

    // 5. Admission gate: capture busy -> 423 with Miro-Ego-Wait-State: idle (on BOTH GET & HEAD)
    backend.set_device_state("recording");
    let (st_get, hdrs_get, body_get) = http_request(
        srv.addr,
        Method::GET,
        &path,
        &[("Range", "bytes=99999999-")],
        None,
    )
    .await;
    assert_eq!(st_get, StatusCode::LOCKED);
    assert_eq!(
        hdrs_get
            .get(HEADER_ERROR_CODE)
            .and_then(|v| v.to_str().ok()),
        Some("capture_busy")
    );
    assert_eq!(
        hdrs_get
            .get(HEADER_WAIT_STATE)
            .and_then(|v| v.to_str().ok()),
        Some("idle")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&body_get).unwrap()["error"]["details"]["current_state"],
        "recording"
    );
    let (st_head, hdrs_head, body_head) =
        http_request(srv.addr, Method::HEAD, &path, &[], None).await;
    assert_eq!(st_head, StatusCode::LOCKED);
    assert_eq!(
        hdrs_head
            .get(HEADER_WAIT_STATE)
            .and_then(|v| v.to_str().ok()),
        Some("idle")
    );
    assert!(body_head.is_empty());
    backend.set_device_state("idle");

    // 6. Admission gate: unverified session -> 409 session_not_verified
    backend.set_session_verified(&sid, false, "absent");
    let (st, hdrs, body) = http_request(srv.addr, Method::GET, &path, &[], None).await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(
        hdrs.get(HEADER_ERROR_CODE).and_then(|v| v.to_str().ok()),
        Some("session_not_verified")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"]["details"]["reason"],
        "absent"
    );
    backend.set_session_verified(&sid, true, "usable");

    // 7. Concurrent download throttling (semaphore limit = 1 -> 503 download_capacity_exhausted)
    let held_permit = Arc::clone(&srv.state.download_semaphore)
        .try_acquire_owned()
        .unwrap();
    let (st_503, hdrs_503, body_503) = http_request(srv.addr, Method::GET, &path, &[], None).await;
    assert_eq!(st_503, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        hdrs_503
            .get(HEADER_ERROR_CODE)
            .and_then(|v| v.to_str().ok()),
        Some("download_capacity_exhausted")
    );
    assert_eq!(
        hdrs_503.get("retry-after").and_then(|v| v.to_str().ok()),
        Some("1")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&body_503).unwrap()["error"]["code"],
        "download_capacity_exhausted"
    );
    drop(held_permit);

    // Permit released -> download works again!
    let (st_ok, _, _) = http_request(srv.addr, Method::HEAD, &path, &[], None).await;
    assert_eq!(st_ok, StatusCode::OK);

    srv.shutdown().await;
    let _ = fs::remove_dir_all(tmp_dir);
}

#[tokio::test]
async fn sse_replay_giant_last_event_id_dos_immunity_and_heartbeats() {
    let backend = Arc::new(MockGatewayBackend::new());
    let runtime = GatewayRuntimeConfig {
        sse_heartbeat_interval: Duration::from_millis(80),
        ..GatewayRuntimeConfig::default()
    };
    let srv = RunningServer::start_http(
        Arc::clone(&backend),
        Arc::new(SecurityPolicy::lab_default()),
        runtime,
    )
    .await;

    // 1. Untrusted giant Last-Event-ID must NOT advance internal monotonic delivery ID!
    let mut tcp = TcpStream::connect(srv.addr).await.unwrap();
    let raw_req = format!(
        "GET /api/v1/capture/events HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nLast-Event-ID: 99999999999999999999999999999999999999\r\n\r\n",
        srv.addr.port()
    );
    tcp.write_all(raw_req.as_bytes()).await.unwrap();

    let mut received = Vec::new();
    let mut chunk = [0_u8; 4096];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        let n = tokio::time::timeout(Duration::from_millis(400), tcp.read(&mut chunk))
            .await
            .unwrap()
            .unwrap();
        if n == 0 {
            break;
        }
        received.extend_from_slice(&chunk[..n]);
        let text = String::from_utf8_lossy(&received);
        if text.contains("event: snapshot") && text.contains(": heartbeat") {
            break;
        }
    }
    let text = String::from_utf8_lossy(&received);
    assert!(text.contains("HTTP/1.1 200 OK"), "got:\n{text}");
    assert!(text.contains("text/event-stream"), "got:\n{text}");
    assert!(
        text.contains("id: 1\n"),
        "giant cursor jumped delivery id:\n{text}"
    );
    assert!(text.contains("miro-ego.capture-event.v1"), "got:\n{text}");
    assert!(
        text.contains(": heartbeat\n\n"),
        "missing heartbeat:\n{text}"
    );
    drop(tcp);

    // Confirm internal counter is still small (2), not jumped to 999999...
    let next_id = backend.capture_event_buffer().next_delivery_id().unwrap();
    assert_eq!(next_id, 2);

    // 2. Publish contiguous events and verify replay after Last-Event-ID
    let epoch = new_uuid_v4();
    let sid = new_uuid_v7();
    let id_a = backend
        .capture_event_buffer()
        .publish(json!({
            "authority_epoch": epoch,
            "source_revision": 10,
            "type": "state",
            "occurred_at": "2026-11-26T10:10:00Z",
            "session_id": sid,
            "data": {"schema": "miro-ego.capture-state-event.v1", "state": "recording", "volume_id": new_uuid_v4(), "generation_id": new_uuid_v4()}
        }))
        .unwrap();
    let id_b = backend
        .capture_event_buffer()
        .publish(json!({
            "authority_epoch": epoch,
            "source_revision": 11,
            "type": "state",
            "occurred_at": "2026-11-26T10:10:01Z",
            "session_id": sid,
            "data": {"schema": "miro-ego.capture-state-event.v1", "state": "finalizing", "volume_id": new_uuid_v4(), "generation_id": new_uuid_v4()}
        }))
        .unwrap();

    let mut tcp2 = TcpStream::connect(srv.addr).await.unwrap();
    let req2 = format!(
        "GET /api/v1/capture/events HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nLast-Event-ID: {id_a}\r\n\r\n",
        srv.addr.port()
    );
    tcp2.write_all(req2.as_bytes()).await.unwrap();
    let mut rec2 = Vec::new();
    let deadline2 = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline2 {
        let n = tokio::time::timeout(Duration::from_millis(400), tcp2.read(&mut chunk))
            .await
            .unwrap()
            .unwrap();
        if n == 0 {
            break;
        }
        rec2.extend_from_slice(&chunk[..n]);
        if String::from_utf8_lossy(&rec2).contains(&format!("id: {id_b}\n")) {
            break;
        }
    }
    let text2 = String::from_utf8_lossy(&rec2);
    assert!(
        text2.contains(&format!("id: {id_b}\nevent: state")),
        "got:\n{text2}"
    );
    assert!(text2.contains("finalizing"), "got:\n{text2}");
    drop(tcp2);

    // 3. Network SSE giant Last-Event-ID immunity
    let mut tcp_net = TcpStream::connect(srv.addr).await.unwrap();
    let req_net = format!(
        "GET /api/v1/network/events HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nLast-Event-ID: 184467440737095516159999\r\n\r\n",
        srv.addr.port()
    );
    tcp_net.write_all(req_net.as_bytes()).await.unwrap();
    let mut rec_net = Vec::new();
    let deadline_net = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline_net {
        let n = tokio::time::timeout(Duration::from_millis(400), tcp_net.read(&mut chunk))
            .await
            .unwrap()
            .unwrap();
        if n == 0 {
            break;
        }
        rec_net.extend_from_slice(&chunk[..n]);
        if String::from_utf8_lossy(&rec_net).contains("miro-ego.network-event.v1") {
            break;
        }
    }
    let text_net = String::from_utf8_lossy(&rec_net);
    assert!(
        text_net.contains("id: 1\nevent: snapshot"),
        "got:\n{text_net}"
    );
    assert_eq!(
        backend.network_event_buffer().next_delivery_id().unwrap(),
        2
    );
    drop(tcp_net);

    // 4. Invalid Last-Event-ID rejected with 400
    let (st, _, _) = http_request(
        srv.addr,
        Method::GET,
        "/api/v1/capture/events",
        &[("Last-Event-ID", "not-a-number")],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    srv.shutdown().await;
}

#[tokio::test]
async fn mjpeg_and_h264_preview_streaming_and_unsupported_fallback() {
    let backend = Arc::new(MockGatewayBackend::with_options(true));
    let srv = RunningServer::start_http(
        Arc::clone(&backend),
        Arc::new(SecurityPolicy::lab_default()),
        GatewayRuntimeConfig::default(),
    )
    .await;

    // 1. Live MJPEG multipart/x-mixed-replace stream
    let mjpeg_hub = backend.preview_mjpeg_hub().unwrap();
    let mut tcp = TcpStream::connect(srv.addr).await.unwrap();
    let req = format!(
        "GET /api/v1/preview?fps=30 HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nAccept: multipart/x-mixed-replace\r\n\r\n",
        srv.addr.port()
    );
    tcp.write_all(req.as_bytes()).await.unwrap();

    // Publish a second frame shortly after connecting
    let hub_clone = Arc::clone(&mjpeg_hub);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(60)).await;
        let _ = hub_clone.publish(Bytes::from_static(b"\xff\xd8FRAME-TWO\xff\xd9"));
    });

    let mut received = Vec::new();
    let mut buf = [0_u8; 4096];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        let n = tokio::time::timeout(Duration::from_millis(500), tcp.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        if n == 0 {
            break;
        }
        received.extend_from_slice(&buf[..n]);
        if String::from_utf8_lossy(&received).contains("FRAME-TWO") {
            break;
        }
    }
    let text = String::from_utf8_lossy(&received);
    assert!(text.contains(MJPEG_CONTENT_TYPE), "got:\n{text}");
    assert!(text.contains("--miro-ego-preview\r\n"), "got:\n{text}");
    assert!(text.contains("MOCK-JPEG"), "got:\n{text}");
    assert!(text.contains("FRAME-TWO"), "got:\n{text}");
    drop(tcp);

    // 2. Live H.264 fMP4 preview stream
    let h264_hub = backend.preview_h264_hub().unwrap();
    let mut tcp_h264 = TcpStream::connect(srv.addr).await.unwrap();
    let req_h264 = format!(
        "GET /api/v1/preview/h264?fps=15&bitrate_kbps=2000 HTTP/1.1\r\nHost: 127.0.0.1:{}\r\n\r\n",
        srv.addr.port()
    );
    tcp_h264.write_all(req_h264.as_bytes()).await.unwrap();

    let h264_clone = Arc::clone(&h264_hub);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(60)).await;
        let _ = h264_clone.publish_fragment(Bytes::from_static(b"moof-mdat-fragment-1"));
    });

    let mut rec_h264 = Vec::new();
    let deadline2 = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline2 {
        let n = tokio::time::timeout(Duration::from_millis(500), tcp_h264.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        if n == 0 {
            break;
        }
        rec_h264.extend_from_slice(&buf[..n]);
        if String::from_utf8_lossy(&rec_h264).contains("moof-mdat-fragment-1") {
            break;
        }
    }
    let text_h264 = String::from_utf8_lossy(&rec_h264);
    assert!(text_h264.contains("video/mp4"), "got:\n{text_h264}");
    assert!(text_h264.contains("ftypisommoov"), "got:\n{text_h264}");
    assert!(
        text_h264.contains("moof-mdat-fragment-1"),
        "got:\n{text_h264}"
    );
    assert_eq!(h264_hub.last_stream_hints(), (Some(15), Some(2000)));
    drop(tcp_h264);
    srv.shutdown().await;

    // 3. Disabled H.264 preview -> 404 preview_h264_unsupported
    let backend_no_h264 = Arc::new(MockGatewayBackend::with_options(false));
    let srv2 = RunningServer::start_http(
        backend_no_h264,
        Arc::new(SecurityPolicy::lab_default()),
        GatewayRuntimeConfig::default(),
    )
    .await;
    let (st, hdrs, body) =
        http_request(srv2.addr, Method::GET, "/api/v1/preview/h264", &[], None).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_eq!(
        hdrs.get(HEADER_ERROR_CODE).and_then(|v| v.to_str().ok()),
        Some("preview_h264_unsupported")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"]["code"],
        "preview_h264_unsupported"
    );
    srv2.shutdown().await;
}

#[tokio::test]
async fn diagnostic_logs_disk_listing_range_read_and_traversal_symlink_rejection() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "miro-ego-api-logs-test-{}-{unique}",
        std::process::id()
    ));
    let logs_dir = root.join("logs");
    let sub_dir = logs_dir.join("wifi-watchdog");
    fs::create_dir_all(&sub_dir).unwrap();
    fs::write(logs_dir.join("device.log"), b"0123456789abcdefghij\n").unwrap();
    fs::write(sub_dir.join("events.ndjson"), b"{\"ok\":true}\n").unwrap();
    fs::write(logs_dir.join(".hidden.log"), b"SECRET-HIDDEN").unwrap();
    fs::write(root.join("outside.txt"), b"OUTSIDE-SECRET").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(root.join("outside.txt"), logs_dir.join("escape.log")).unwrap();

    let backend = Arc::new(MockGatewayBackend::new());
    backend.set_logs_dir(logs_dir);
    let srv = RunningServer::start_http(
        backend,
        Arc::new(SecurityPolicy::lab_default()),
        GatewayRuntimeConfig::default(),
    )
    .await;

    // 1. Listing exposes only regular visible logs (no dotfiles, no symlinks)
    let (st, _, body) = http_request(srv.addr, Method::GET, "/api/v1/logs", &[], None).await;
    assert_eq!(st, StatusCode::OK);
    let list: Value = serde_json::from_slice(&body).unwrap();
    let names: Vec<&str> = list["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"device.log"));
    assert!(names.contains(&"wifi-watchdog/events.ndjson"));
    assert!(!names.contains(&".hidden.log"));
    assert!(!names.contains(&"escape.log"));

    // 2. Prefix filter
    let (st, _, body) = http_request(
        srv.addr,
        Method::GET,
        "/api/v1/logs?prefix=wifi-watchdog/",
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let filtered: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(filtered["items"].as_array().unwrap().len(), 1);
    assert_eq!(filtered["items"][0]["name"], "wifi-watchdog/events.ndjson");

    // 3. Range download on diagnostic log file
    let (st, hdrs, body) = http_request(
        srv.addr,
        Method::GET,
        "/api/v1/logs/device.log",
        &[("Range", "bytes=10-19")],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        hdrs.get("content-range").and_then(|v| v.to_str().ok()),
        Some("bytes 10-19/21")
    );
    assert_eq!(&body[..], b"abcdefghij");

    // 4. Path traversal, percent-encoding, hidden dotfiles, and symlinks all fail closed!
    for bad_path in [
        "/api/v1/logs/../outside.txt",
        "/api/v1/logs/%2e%2e/outside.txt",
        "/api/v1/logs/wifi-watchdog/../../outside.txt",
        "/api/v1/logs/.hidden.log",
        "/api/v1/logs/wifi-watchdog%2fevents.ndjson",
    ] {
        let (st, _, _) = http_request(srv.addr, Method::GET, bad_path, &[], None).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "expected 400 for {bad_path}");
    }

    #[cfg(unix)]
    {
        let (st, _, _) =
            http_request(srv.addr, Method::GET, "/api/v1/logs/escape.log", &[], None).await;
        assert_eq!(st, StatusCode::NOT_FOUND, "symlink escape must fail closed");
    }

    srv.shutdown().await;
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn sse_live_lag_resync_fetches_fresh_backend_state() {
    let backend = Arc::new(MockGatewayBackend::new());
    let runtime = GatewayRuntimeConfig {
        sse_heartbeat_interval: Duration::from_secs(30),
        ..GatewayRuntimeConfig::default()
    };
    let srv = RunningServer::start_http(
        Arc::clone(&backend),
        Arc::new(SecurityPolicy::lab_default()),
        runtime,
    )
    .await;

    // 1. Capture SSE: connect while idle, receive initial "idle" snapshot.
    let mut tcp = TcpStream::connect(srv.addr).await.unwrap();
    let req = format!(
        "GET /api/v1/capture/events HTTP/1.1\r\nHost: 127.0.0.1:{}\r\n\r\n",
        srv.addr.port()
    );
    tcp.write_all(req.as_bytes()).await.unwrap();

    let mut received = Vec::new();
    let mut chunk = [0_u8; 8192];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        let n = tokio::time::timeout(Duration::from_millis(500), tcp.read(&mut chunk))
            .await
            .unwrap()
            .unwrap();
        assert!(n > 0, "unexpected EOF waiting for initial capture snapshot");
        received.extend_from_slice(&chunk[..n]);
        if String::from_utf8_lossy(&received).contains("\"device_state\":\"idle\"") {
            break;
        }
    }
    assert!(
        String::from_utf8_lossy(&received).contains("\"device_state\":\"idle\""),
        "initial snapshot must be idle"
    );

    // Mutate backend live state to "recording" and push a non-contiguous revision burst (>128 slots)
    // to force a live resync snapshot on the already-open SSE stream.
    backend.set_device_state("recording");
    let new_epoch = new_uuid_v4();
    let sid = new_uuid_v7();
    let vid = new_uuid_v4();
    let gid = new_uuid_v4();
    for rev in 500..640_u64 {
        backend
            .capture_event_buffer()
            .publish(json!({
                "authority_epoch": new_epoch,
                "source_revision": rev,
                "type": "state",
                "occurred_at": "2026-11-26T11:00:00Z",
                "session_id": sid,
                "data": {
                    "schema": "miro-ego.capture-state-event.v1",
                    "state": "recording",
                    "volume_id": vid,
                    "generation_id": gid
                }
            }))
            .unwrap();
    }

    let mut after = Vec::new();
    let deadline2 = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline2 {
        let n = tokio::time::timeout(Duration::from_millis(500), tcp.read(&mut chunk))
            .await
            .unwrap()
            .unwrap();
        assert!(
            n > 0,
            "unexpected EOF waiting for fresh capture resync snapshot"
        );
        after.extend_from_slice(&chunk[..n]);
        let s = String::from_utf8_lossy(&after);
        if s.contains("event: snapshot") && s.contains("\"device_state\":\"recording\"") {
            break;
        }
    }
    let after_str = String::from_utf8_lossy(&after);
    assert!(
        after_str.contains("event: snapshot")
            && after_str.contains("\"device_state\":\"recording\""),
        "lagged capture SSE resync must emit fresh 'recording' snapshot, got:\n{after_str}"
    );
    drop(tcp);

    // 2. Network SSE: connect in hotspot mode, mutate backend desired state + overflow 128-slot ring.
    let mut tcp_net = TcpStream::connect(srv.addr).await.unwrap();
    let req_net = format!(
        "GET /api/v1/network/events HTTP/1.1\r\nHost: 127.0.0.1:{}\r\n\r\n",
        srv.addr.port()
    );
    tcp_net.write_all(req_net.as_bytes()).await.unwrap();

    let mut net_init = Vec::new();
    let deadline3 = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline3 {
        let n = tokio::time::timeout(Duration::from_millis(500), tcp_net.read(&mut chunk))
            .await
            .unwrap()
            .unwrap();
        assert!(n > 0);
        net_init.extend_from_slice(&chunk[..n]);
        if String::from_utf8_lossy(&net_init).contains("\"mode\":\"hotspot\"") {
            break;
        }
    }
    assert!(String::from_utf8_lossy(&net_init).contains("\"mode\":\"hotspot\""));

    // Mutate backend desired mode via /api/v1/network/apply to ethernet-dhcp
    let (st_apply, _, _) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/network/apply",
        &[
            ("Content-Type", "application/json"),
            ("Idempotency-Key", "lag-net-apply-1"),
        ],
        Some(Bytes::from(
            json!({
                "schema": "miro-ego.network-apply-request.v1",
                "desired": {
                    "mode": "ethernet-dhcp",
                    "wifi_client": null,
                    "ethernet": { "addressing": "dhcp", "static_ipv4": null }
                }
            })
            .to_string(),
        )),
    )
    .await;
    assert_eq!(st_apply, StatusCode::ACCEPTED);

    // Overflow the 128-slot network replay ring with a fresh authority epoch so client must resync
    let mut base_status = backend.network_status().await.unwrap();
    let fresh_net_epoch = new_uuid_v4();
    for rev in 1..=140_u64 {
        if let Some(obj) = base_status.as_object_mut() {
            obj.insert("authority_epoch".to_owned(), json!(fresh_net_epoch));
            obj.insert("source_revision".to_owned(), json!(rev));
            obj.insert(
                "transaction".to_owned(),
                json!({"current": null, "latest": null}),
            );
        }
        let _ = backend.network_event_buffer().observe(base_status.clone());
    }

    let mut net_after = Vec::new();
    let deadline4 = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline4 {
        let n = tokio::time::timeout(Duration::from_millis(500), tcp_net.read(&mut chunk))
            .await
            .unwrap()
            .unwrap();
        assert!(n > 0);
        net_after.extend_from_slice(&chunk[..n]);
        let s = String::from_utf8_lossy(&net_after);
        if s.contains("\"mode\":\"ethernet-dhcp\"") {
            break;
        }
    }
    let net_after_str = String::from_utf8_lossy(&net_after);
    assert!(
        net_after_str.contains("\"mode\":\"ethernet-dhcp\""),
        "lagged network SSE resync must serve fresh 'ethernet-dhcp' state, got:\n{net_after_str}"
    );
    drop(tcp_net);

    srv.shutdown().await;
}

#[tokio::test]
async fn customer_mode_rejects_unauthenticated_invalid_paths_with_401_and_mirrors_error_code() {
    let (tls_cfg, connector) = generate_test_tls();
    let backend = Arc::new(MockGatewayBackend::new());

    let admin = Principal::all_v1_operations("admin-client").unwrap();
    let mut tokens = HashMap::new();
    tokens.insert("admin-secret-token-9999".to_owned(), admin);
    let security = Arc::new(
        SecurityPolicy::customer(tokens, HashSet::new(), Some("csrf-secret-123".to_owned()))
            .unwrap(),
    );

    let srv = RunningServer::start_https(
        Arc::clone(&backend),
        security,
        tls_cfg,
        Duration::from_secs(5),
    )
    .await;

    let unauth_invalid_paths = [
        (Method::GET, "/api/v1/sessions/not-a-uuid"),
        (Method::GET, "/api/v1/sessions/%ff"),
        (
            Method::GET,
            "/api/v1/sessions/not-a-uuid/unsuccessful-outcome",
        ),
        (Method::GET, "/api/v1/sessions/%ff/unsuccessful-outcome"),
        (Method::GET, "/api/v1/sessions/not-a-uuid/artifacts/bad"),
        (Method::GET, "/api/v1/sessions/%ff/artifacts/bad"),
        (Method::HEAD, "/api/v1/sessions/not-a-uuid/artifacts/bad"),
        (Method::GET, "/api/v1/logs/../etc/passwd"),
        (Method::GET, "/api/v1/logs/%2e%2e/secret"),
        (Method::GET, "/api/v1/logs/%ff"),
    ];

    for (method, path) in unauth_invalid_paths {
        let is_head = method == Method::HEAD;
        let (st, hdrs, body) = https_request(srv.addr, &connector, method, path, &[], None).await;
        assert_eq!(
            st,
            StatusCode::UNAUTHORIZED,
            "unauthenticated Customer request to {path} must return 401"
        );
        assert_eq!(
            hdrs.get("www-authenticate").and_then(|v| v.to_str().ok()),
            Some("Bearer"),
            "missing WWW-Authenticate on {path}"
        );
        assert_eq!(
            hdrs.get(HEADER_ERROR_CODE).and_then(|v| v.to_str().ok()),
            Some("unauthorized"),
            "missing Miro-Ego-Error-Code on {path}"
        );
        if is_head {
            assert!(body.is_empty());
        } else {
            let problem: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(problem["schema"], "miro-ego.api-error.v1");
            assert_eq!(problem["error"]["code"], "unauthorized");
        }
    }

    // Confirm unauthorized audit events were recorded for all 10 unauthenticated probes
    let unauth_audits = backend
        .audit_events()
        .into_iter()
        .filter(|ev| ev.outcome == "unauthorized")
        .count();
    assert_eq!(unauth_audits, 10);

    // Authenticated requests with invalid or non-UTF8 (%ff) path params still return JSON
    // miro-ego.api-error.v1 404 (sessions) / 400 (logs) and mirror Miro-Ego-Error-Code!
    let auth = [("Authorization", "Bearer admin-secret-token-9999")];
    for sess_bad_path in ["/api/v1/sessions/not-a-uuid", "/api/v1/sessions/%ff"] {
        let (st_sess, hdrs_sess, body_sess) = https_request(
            srv.addr,
            &connector,
            Method::GET,
            sess_bad_path,
            &auth,
            None,
        )
        .await;
        assert_eq!(st_sess, StatusCode::NOT_FOUND, "path={sess_bad_path}");
        assert_eq!(
            hdrs_sess
                .get(HEADER_ERROR_CODE)
                .and_then(|v| v.to_str().ok()),
            Some("not_found")
        );
        let problem: Value = serde_json::from_slice(&body_sess).unwrap();
        assert_eq!(problem["schema"], "miro-ego.api-error.v1");
    }

    for log_bad_path in ["/api/v1/logs/../etc/passwd", "/api/v1/logs/%ff"] {
        let (st_log, hdrs_log, body_log) =
            https_request(srv.addr, &connector, Method::GET, log_bad_path, &auth, None).await;
        assert_eq!(st_log, StatusCode::BAD_REQUEST, "path={log_bad_path}");
        assert_eq!(
            hdrs_log
                .get(HEADER_ERROR_CODE)
                .and_then(|v| v.to_str().ok()),
            Some("invalid_request")
        );
        let problem: Value = serde_json::from_slice(&body_log).unwrap();
        assert_eq!(problem["schema"], "miro-ego.api-error.v1");
    }

    srv.shutdown().await;
}

#[tokio::test]
async fn customer_echoed_bearer_csrf_and_resource_scoped_session_deletion() {
    let (tls_cfg, connector) = generate_test_tls();
    let backend = Arc::new(MockGatewayBackend::new());
    let (sid_a, _, sha_a) = backend.sample_session_and_artifact().unwrap();
    let (sid_b, _, sha_b) = backend.insert_sample_session(None).unwrap();

    let operator_a = Principal::all_v1_operations("operator-a").unwrap();
    let mut scoped_perms = HashMap::new();
    scoped_perms.insert(
        "deleteSessions".to_owned(),
        Some(HashSet::from([sid_a.clone()])),
    );
    scoped_perms.insert("setVoiceVolume".to_owned(), None);
    let operator_b = Principal::new("operator-b", scoped_perms).unwrap();

    let mut tokens = HashMap::new();
    tokens.insert("bearer-tok-alpha-1111".to_owned(), operator_a);
    tokens.insert("bearer-tok-bravo-2222".to_owned(), operator_b);

    // Multi-token Customer policy with csrf_token = None enforces X-CSRF-Token == caller's bearer token!
    let security = Arc::new(SecurityPolicy::customer(tokens, HashSet::new(), None).unwrap());
    let srv = RunningServer::start_https(
        Arc::clone(&backend),
        security,
        tls_cfg,
        Duration::from_secs(5),
    )
    .await;
    let origin = format!("https://localhost:{}", srv.addr.port());
    let voice_body = Bytes::from(json!({"schema": "miro-ego.voice-set.v1", "volume": 55}).to_string());

    // 1. Missing X-CSRF-Token -> 403 csrf_forbidden (NOT bypassed when csrf_token is None!)
    let (st, _, body) = https_request(
        srv.addr,
        &connector,
        Method::POST,
        "/api/v1/voice",
        &[
            ("Authorization", "Bearer bearer-tok-alpha-1111"),
            ("Origin", &origin),
            ("Content-Type", "application/json"),
        ],
        Some(voice_body.clone()),
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"]["code"],
        "csrf_forbidden"
    );

    // 2. Cross-principal bearer token in X-CSRF-Token -> 403 csrf_forbidden
    let (st, _, body) = https_request(
        srv.addr,
        &connector,
        Method::POST,
        "/api/v1/voice",
        &[
            ("Authorization", "Bearer bearer-tok-alpha-1111"),
            ("Origin", &origin),
            ("Content-Type", "application/json"),
            ("X-CSRF-Token", "bearer-tok-bravo-2222"),
        ],
        Some(voice_body.clone()),
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"]["code"],
        "csrf_forbidden"
    );

    // 3. Each principal echoing its own bearer token in X-CSRF-Token succeeds (200 OK)
    for tok in ["bearer-tok-alpha-1111", "bearer-tok-bravo-2222"] {
        let (st, _, _) = https_request(
            srv.addr,
            &connector,
            Method::POST,
            "/api/v1/voice",
            &[
                ("Authorization", &format!("Bearer {tok}")),
                ("Origin", &origin),
                ("Content-Type", "application/json"),
                ("X-CSRF-Token", tok),
            ],
            Some(voice_body.clone()),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "echoed bearer CSRF failed for {tok}");
    }

    // 4. Resource-scoped operator-b attempting to delete unauthorized sid_b -> 403 forbidden
    let (st_forbid, _, _) = https_request(
        srv.addr,
        &connector,
        Method::POST,
        "/api/v1/sessions/delete",
        &[
            ("Authorization", "Bearer bearer-tok-bravo-2222"),
            ("Origin", &origin),
            ("Content-Type", "application/json"),
            ("X-CSRF-Token", "bearer-tok-bravo-2222"),
            ("Idempotency-Key", "scoped-del-forbid"),
        ],
        Some(Bytes::from(
            json!({
                "schema": "miro-ego.session-delete-request.v1",
                "sessions": [{"session_id": sid_b, "manifest_sha256": sha_b}]
            })
            .to_string(),
        )),
    )
    .await;
    assert_eq!(st_forbid, StatusCode::FORBIDDEN);

    // 5. Resource-scoped operator-b deleting permitted sid_a -> 200 OK
    let (st_allow, _, body_allow) = https_request(
        srv.addr,
        &connector,
        Method::POST,
        "/api/v1/sessions/delete",
        &[
            ("Authorization", "Bearer bearer-tok-bravo-2222"),
            ("Origin", &origin),
            ("Content-Type", "application/json"),
            ("X-CSRF-Token", "bearer-tok-bravo-2222"),
            ("Idempotency-Key", "scoped-del-allow"),
        ],
        Some(Bytes::from(
            json!({
                "schema": "miro-ego.session-delete-request.v1",
                "sessions": [{"session_id": sid_a, "manifest_sha256": sha_a}]
            })
            .to_string(),
        )),
    )
    .await;
    assert_eq!(st_allow, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body_allow).unwrap()["deleted_session_ids"][0],
        sid_a
    );

    srv.shutdown().await;
}

#[tokio::test]
async fn open_artifact_download_blocks_session_deletion_until_stream_closes() {
    let backend = Arc::new(MockGatewayBackend::new());
    let (sid, _, manifest_sha) = backend.sample_session_and_artifact().unwrap();

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let tmp_dir = std::env::temp_dir().join(format!(
        "miro-ego-api-open-dl-del-{}-{unique}",
        std::process::id()
    ));
    fs::create_dir(&tmp_dir).unwrap();
    let file_path = tmp_dir.join("large_segment.mp4");
    let total_size = 8 * 1024 * 1024;
    let payload = vec![0x5a_u8; total_size];
    fs::write(&file_path, &payload).unwrap();
    let mut hasher = Sha256::new();
    hasher.update(&payload);
    let aid = hex_encode(&hasher.finalize());
    backend
        .add_file_artifact(&sid, aid.clone(), "video/mp4".to_owned(), file_path)
        .unwrap();

    let runtime = GatewayRuntimeConfig {
        max_concurrent_downloads: 4,
        download_chunk_bytes: 32 * 1024,
        ..GatewayRuntimeConfig::default()
    };
    let srv = RunningServer::start_http(
        Arc::clone(&backend),
        Arc::new(SecurityPolicy::lab_default()),
        runtime,
    )
    .await;

    // Open a streaming download with a small TCP receive buffer and pause reading body bytes
    // so the server's download body stream stays active and holds its shared delete lease.
    let sock = tokio::net::TcpSocket::new_v4().unwrap();
    let _ = sock.set_recv_buffer_size(4096);
    let tcp = sock.connect(srv.addr).await.unwrap();
    let (mut sender, conn) = client_http1::handshake(TokioIo::new(tcp)).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let dl_req = Request::builder()
        .method(Method::GET)
        .uri(format!("/api/v1/sessions/{sid}/artifacts/{aid}"))
        .header("Host", format!("127.0.0.1:{}", srv.addr.port()))
        .body(Full::new(Bytes::new()))
        .unwrap();
    let dl_resp = sender.send_request(dl_req).await.unwrap();
    assert_eq!(dl_resp.status(), StatusCode::OK);

    // Concurrent POST /api/v1/sessions/delete must be rejected with 409 open_downloads!
    let del_body = Bytes::from(
        json!({
            "schema": "miro-ego.session-delete-request.v1",
            "sessions": [{"session_id": sid, "manifest_sha256": manifest_sha}]
        })
        .to_string(),
    );
    let (st_conflict, hdrs_conflict, body_conflict) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/sessions/delete",
        &[
            ("Content-Type", "application/json"),
            ("Idempotency-Key", "del-while-open-1"),
        ],
        Some(del_body.clone()),
    )
    .await;
    assert_eq!(st_conflict, StatusCode::CONFLICT);
    assert_eq!(
        hdrs_conflict
            .get(HEADER_ERROR_CODE)
            .and_then(|v| v.to_str().ok()),
        Some("open_downloads")
    );
    let conflict_json: Value = serde_json::from_slice(&body_conflict).unwrap();
    assert_eq!(conflict_json["error"]["code"], "open_downloads");
    assert_eq!(conflict_json["error"]["retryable"], true);

    // Finish draining the download body so the server releases the delete lease.
    let downloaded = dl_resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(downloaded.len(), total_size);
    drop(sender);
    tokio::task::yield_now().await;

    // Now POST /api/v1/sessions/delete succeeds!
    let (st_ok, _, body_ok) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/sessions/delete",
        &[
            ("Content-Type", "application/json"),
            ("Idempotency-Key", "del-after-close-1"),
        ],
        Some(del_body),
    )
    .await;
    assert_eq!(st_ok, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body_ok).unwrap()["deleted_session_ids"][0],
        sid
    );

    srv.shutdown().await;
    let _ = fs::remove_dir_all(tmp_dir);
}

#[tokio::test]
async fn sse_async_snapshot_retry_prevents_swallowed_concurrent_event_and_bumps_cursor_collision() {
    use miro_ego_api::{EventCursor, EventReplayBuffer};
    use std::sync::atomic::{AtomicUsize, Ordering};

    // 1. Fresh buffer (next_delivery_id == 1) with Last-Event-ID: 1 must bump resync ID to 2 (> 1).
    let fresh_buf = EventReplayBuffer::new(16);
    let epoch = new_uuid_v4();
    let bumped = fresh_buf
        .replay_or_snapshot_async(Some(EventCursor::Exact(1)), || {
            let ep = epoch.clone();
            async move {
                Ok(json!({
                    "authority_epoch": ep,
                    "source_revision": 1,
                    "type": "snapshot",
                    "occurred_at": "2026-11-26T10:00:00Z",
                    "session_id": null,
                    "data": {"schema": "miro-ego.capture-snapshot-event.v1"}
                }))
            }
        })
        .await
        .unwrap();
    assert_eq!(bumped.len(), 1);
    assert_eq!(
        bumped[0].delivery_id, 2,
        "resync snapshot must advance past Last-Event-ID=1"
    );

    // 2. Concurrent publish firing DURING async snapshot fetch must cause replay_or_snapshot_async
    // to re-fetch fresh state rather than anchoring over the concurrent publish with a stale snapshot!
    let race_buf = Arc::new(EventReplayBuffer::new(16));
    let live_rev = Arc::new(AtomicUsize::new(10));
    let calls = Arc::new(AtomicUsize::new(0));

    let buf_for_cb = Arc::clone(&race_buf);
    let rev_for_cb = Arc::clone(&live_rev);
    let calls_for_cb = Arc::clone(&calls);
    let epoch2 = new_uuid_v4();

    let events = race_buf
        .replay_or_snapshot_async(None, || {
            let b = Arc::clone(&buf_for_cb);
            let r = Arc::clone(&rev_for_cb);
            let c = Arc::clone(&calls_for_cb);
            let ep = epoch2.clone();
            async move {
                let attempt = c.fetch_add(1, Ordering::SeqCst);
                let observed_rev = r.load(Ordering::SeqCst);
                if attempt == 0 {
                    // Simulate a concurrent state transition publishing while snapshot #0 was in flight.
                    r.store(11, Ordering::SeqCst);
                    b.publish(json!({
                        "authority_epoch": ep,
                        "source_revision": 11,
                        "type": "state",
                        "occurred_at": "2026-11-26T10:00:01Z",
                        "session_id": new_uuid_v7(),
                        "data": {"schema": "miro-ego.capture-state-event.v1", "state": "recording"}
                    }))
                    .unwrap();
                }
                Ok(json!({
                    "authority_epoch": ep,
                    "source_revision": observed_rev,
                    "type": "snapshot",
                    "occurred_at": "2026-11-26T10:00:02Z",
                    "session_id": null,
                    "data": {"schema": "miro-ego.capture-snapshot-event.v1", "rev": observed_rev}
                }))
            }
        })
        .await
        .unwrap();

    assert!(
        calls.load(Ordering::SeqCst) >= 2,
        "must retry when publish_count changes during async snapshot"
    );
    assert_eq!(
        events[0].source_event["source_revision"], 11,
        "retried snapshot must reflect concurrent revision 11, not stale revision 10"
    );
}

#[tokio::test]
async fn contract_parity_network_errors_cursor_binding_preview_guards_and_mock_schemas() {
    let backend = Arc::new(MockGatewayBackend::new());
    let (sid1, _, _) = backend.sample_session_and_artifact().unwrap();
    let failed_sid = backend.sample_unsuccessful_session_id().unwrap();
    let take_other = new_uuid_v7();
    let (_sid2, _, _) = backend
        .insert_sample_session(Some(take_other.clone()))
        .unwrap();

    let srv = RunningServer::start_http(
        Arc::clone(&backend),
        Arc::new(SecurityPolicy::lab_default()),
        GatewayRuntimeConfig::default(),
    )
    .await;

    // 1. Validate MockGatewayBackend sealed manifest & unsuccessful recording_state against contract schema requirements!
    let (st_man, _, man_bytes) = http_request(
        srv.addr,
        Method::GET,
        &format!("/api/v1/sessions/{sid1}"),
        &[],
        None,
    )
    .await;
    assert_eq!(st_man, StatusCode::OK);
    let manifest: Value = serde_json::from_slice(&man_bytes).unwrap();
    let man_obj = manifest.as_object().unwrap();
    assert!(!man_obj.contains_key("artifacts"));
    for req_key in [
        "schema",
        "manifest_id",
        "sealed",
        "sealed_at",
        "session_id",
        "volume_id",
        "capture_mode",
        "display_name",
        "device",
        "time",
        "take",
        "camera",
        "video",
        "imu",
        "frames",
        "audio",
        "logs",
        "integrity",
    ] {
        assert!(
            man_obj.contains_key(req_key),
            "mock manifest missing required schema key: {req_key}"
        );
    }

    let (st_out, _, out_bytes) = http_request(
        srv.addr,
        Method::GET,
        &format!("/api/v1/sessions/{failed_sid}/unsuccessful-outcome"),
        &[],
        None,
    )
    .await;
    assert_eq!(st_out, StatusCode::OK);
    let outcome_doc: Value = serde_json::from_slice(&out_bytes).unwrap();
    let rec_state = outcome_doc["outcome"]["recording_state"]
        .as_object()
        .unwrap();
    for req_key in [
        "schema",
        "state",
        "authority_epoch",
        "state_revision",
        "updated_at",
        "session_id",
        "take_id",
        "display_name",
        "device",
        "storage",
        "progress",
        "diagnostics",
    ] {
        assert!(
            rec_state.contains_key(req_key),
            "mock recording_state missing required schema key: {req_key}"
        );
    }
    assert_eq!(rec_state["diagnostics"][0]["severity"], "error");
    assert_eq!(rec_state["diagnostics"][0]["recoverable"], false);

    // 2. Unverified session returns 409 session_not_verified BEFORE 404 even for an unknown artifact_id!
    backend.set_session_verified(&sid1, false, "unusable");
    let unknown_aid = "f".repeat(64);
    let (st_unv, hdrs_unv, body_unv) = http_request(
        srv.addr,
        Method::GET,
        &format!("/api/v1/sessions/{sid1}/artifacts/{unknown_aid}"),
        &[],
        None,
    )
    .await;
    assert_eq!(st_unv, StatusCode::CONFLICT);
    assert_eq!(
        hdrs_unv.get(HEADER_ERROR_CODE).and_then(|v| v.to_str().ok()),
        Some("session_not_verified")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&body_unv).unwrap()["error"]["details"]["reason"],
        "unusable"
    );
    backend.set_session_verified(&sid1, true, "usable");

    // 3. list_sessions cursor binds take_id and preserves identical catalog_revision across filters!
    let (st_all, _, b_all) =
        http_request(srv.addr, Method::GET, "/api/v1/sessions?limit=1", &[], None).await;
    assert_eq!(st_all, StatusCode::OK);
    let page1: Value = serde_json::from_slice(&b_all).unwrap();
    let rev_all = page1["catalog_revision"].as_str().unwrap().to_owned();
    let cursor1 = page1["next_cursor"].as_str().unwrap().to_owned();

    let (st_filt, _, b_filt) = http_request(
        srv.addr,
        Method::GET,
        &format!("/api/v1/sessions?take_id={take_other}"),
        &[],
        None,
    )
    .await;
    assert_eq!(st_filt, StatusCode::OK);
    let filt_json: Value = serde_json::from_slice(&b_filt).unwrap();
    assert_eq!(
        filt_json["catalog_revision"].as_str(),
        Some(rev_all.as_str()),
        "catalog_revision must bind the complete catalog regardless of take_id filter"
    );

    let (st_bad_cur, hdrs_bad_cur, body_bad_cur) = http_request(
        srv.addr,
        Method::GET,
        &format!("/api/v1/sessions?cursor={cursor1}&take_id={take_other}"),
        &[],
        None,
    )
    .await;
    assert_eq!(st_bad_cur, StatusCode::BAD_REQUEST);
    assert_eq!(
        hdrs_bad_cur
            .get(HEADER_ERROR_CODE)
            .and_then(|v| v.to_str().ok()),
        Some("invalid_cursor")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&body_bad_cur).unwrap()["error"]["code"],
        "invalid_cursor"
    );

    // 4. POST /api/v1/network/retry unknown transaction -> 404 network_transaction_not_found in BOTH body & header
    let missing_tx = new_uuid_v7();
    let (st_nf, hdrs_nf, body_nf) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/network/retry",
        &[
            ("Content-Type", "application/json"),
            ("Idempotency-Key", "retry-nf-1"),
        ],
        Some(Bytes::from(
            json!({
                "schema": "miro-ego.network-retry-request.v1",
                "transaction_id": missing_tx
            })
            .to_string(),
        )),
    )
    .await;
    assert_eq!(st_nf, StatusCode::NOT_FOUND);
    assert_eq!(
        hdrs_nf.get(HEADER_ERROR_CODE).and_then(|v| v.to_str().ok()),
        Some("network_transaction_not_found")
    );
    let nf_json: Value = serde_json::from_slice(&body_nf).unwrap();
    assert_eq!(nf_json["error"]["code"], "network_transaction_not_found");
    assert_eq!(nf_json["error"]["details"]["transaction_id"], missing_tx);

    // 5. Network 409 idempotency_conflict includes details { idempotency_scope: "network-mutation", original_transaction_id }
    let (st_ap1, _, b_ap1) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/network/apply",
        &[
            ("Content-Type", "application/json"),
            ("Idempotency-Key", "net-idem-conflict-1"),
        ],
        Some(Bytes::from(
            json!({
                "schema": "miro-ego.network-apply-request.v1",
                "desired": { "mode": "hotspot", "wifi_client": null, "ethernet": null }
            })
            .to_string(),
        )),
    )
    .await;
    assert_eq!(st_ap1, StatusCode::ACCEPTED);
    let orig_tx_id = serde_json::from_slice::<Value>(&b_ap1).unwrap()["transaction"]
        ["transaction_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let (st_ap2, hdrs_ap2, b_ap2) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/network/apply",
        &[
            ("Content-Type", "application/json"),
            ("Idempotency-Key", "net-idem-conflict-1"),
        ],
        Some(Bytes::from(
            json!({
                "schema": "miro-ego.network-apply-request.v1",
                "desired": {
                    "mode": "ethernet-dhcp",
                    "wifi_client": null,
                    "ethernet": { "addressing": "dhcp", "static_ipv4": null }
                }
            })
            .to_string(),
        )),
    )
    .await;
    assert_eq!(st_ap2, StatusCode::CONFLICT);
    assert_eq!(
        hdrs_ap2.get(HEADER_ERROR_CODE).and_then(|v| v.to_str().ok()),
        Some("idempotency_conflict")
    );
    let conflict_doc: Value = serde_json::from_slice(&b_ap2).unwrap();
    assert_eq!(conflict_doc["error"]["code"], "idempotency_conflict");
    assert_eq!(
        conflict_doc["error"]["details"]["idempotency_scope"],
        "network-mutation"
    );
    assert_eq!(
        conflict_doc["error"]["details"]["original_transaction_id"],
        orig_tx_id
    );

    // 6. Duplicate static_ipv4.dns rejected with 400 (uniqueItems: true)
    let (st_dup_dns, _, _) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/network/apply",
        &[
            ("Content-Type", "application/json"),
            ("Idempotency-Key", "net-dup-dns-1"),
        ],
        Some(Bytes::from(
            json!({
                "schema": "miro-ego.network-apply-request.v1",
                "desired": {
                    "mode": "ethernet-static",
                    "wifi_client": null,
                    "ethernet": {
                        "addressing": "static",
                        "static_ipv4": {
                            "address": "192.168.10.20",
                            "prefix_length": 24,
                            "gateway": "192.168.10.1",
                            "dns": ["1.1.1.1", "1.1.1.1"]
                        }
                    }
                }
            })
            .to_string(),
        )),
    )
    .await;
    assert_eq!(st_dup_dns, StatusCode::BAD_REQUEST);

    // 7. Multi-byte UTF-8 display_name (100 CJK chars = 300 bytes <= 160 chars) accepted, and stop_capture emits SSE snapshot!
    let cjk_name = "镜".repeat(100);
    let mut sse_tcp = TcpStream::connect(srv.addr).await.unwrap();
    let sse_req = format!(
        "GET /api/v1/capture/events HTTP/1.1\r\nHost: 127.0.0.1:{}\r\n\r\n",
        srv.addr.port()
    );
    sse_tcp.write_all(sse_req.as_bytes()).await.unwrap();
    let mut sse_buf = [0_u8; 4096];
    let mut sse_acc = Vec::new();
    while !String::from_utf8_lossy(&sse_acc).contains("event: snapshot") {
        let n = tokio::time::timeout(Duration::from_secs(1), sse_tcp.read(&mut sse_buf))
            .await
            .unwrap()
            .unwrap();
        assert!(n > 0);
        sse_acc.extend_from_slice(&sse_buf[..n]);
    }

    let (st_start, _, _) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/capture/start",
        &[
            ("Content-Type", "application/json"),
            ("Idempotency-Key", "cjk-start-1"),
        ],
        Some(Bytes::from(
            json!({
                "schema": "miro-ego.capture-start.v1",
                "mode": "production",
                "display_name": cjk_name,
                "take": { "kind": "new" }
            })
            .to_string(),
        )),
    )
    .await;
    assert_eq!(st_start, StatusCode::ACCEPTED);
    sse_acc.clear();
    while !String::from_utf8_lossy(&sse_acc).contains("event: state") {
        let n = tokio::time::timeout(Duration::from_secs(1), sse_tcp.read(&mut sse_buf))
            .await
            .unwrap()
            .unwrap();
        assert!(n > 0);
        sse_acc.extend_from_slice(&sse_buf[..n]);
    }
    assert!(String::from_utf8_lossy(&sse_acc).contains("\"state\":\"recording\""));

    let (st_stop, _, _) = http_request(
        srv.addr,
        Method::POST,
        "/api/v1/capture/stop",
        &[
            ("Content-Type", "application/json"),
            ("Idempotency-Key", "cjk-stop-1"),
        ],
        Some(Bytes::from(
            json!({
                "schema": "miro-ego.capture-stop.v1",
                "reason": "user"
            })
            .to_string(),
        )),
    )
    .await;
    assert_eq!(st_stop, StatusCode::ACCEPTED);
    sse_acc.clear();
    while !String::from_utf8_lossy(&sse_acc).contains("event: snapshot") {
        let n = tokio::time::timeout(Duration::from_secs(1), sse_tcp.read(&mut sse_buf))
            .await
            .unwrap()
            .unwrap();
        assert!(n > 0);
        sse_acc.extend_from_slice(&sse_buf[..n]);
    }
    assert!(String::from_utf8_lossy(&sse_acc).contains("\"device_state\":\"idle\""));
    drop(sse_tcp);

    // 8. Disconnected camera returns 503 camera_not_connected on /preview & /preview/h264,
    // and H264PreviewHub without init_segment returns 503 preview_unavailable!
    backend.set_camera_connected(false);
    for prev_path in ["/api/v1/preview", "/api/v1/preview/h264"] {
        let (st_cam, hdrs_cam, body_cam) =
            http_request(srv.addr, Method::GET, prev_path, &[], None).await;
        assert_eq!(
            st_cam,
            StatusCode::SERVICE_UNAVAILABLE,
            "path={prev_path}"
        );
        assert_eq!(
            hdrs_cam
                .get(HEADER_ERROR_CODE)
                .and_then(|v| v.to_str().ok()),
            Some("camera_not_connected")
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&body_cam).unwrap()["error"]["code"],
            "camera_not_connected"
        );
    }
    backend.set_camera_connected(true);

    // Clear H.264 init_segment via a fresh custom hub check: verify /preview/h264 without init_segment -> 503 preview_unavailable
    struct UninitH264Backend {
        inner: MockGatewayBackend,
        empty_h264: Arc<miro_ego_api::H264PreviewHub>,
    }
    #[async_trait::async_trait]
    impl GatewayBackend for UninitH264Backend {
        async fn device_info(&self, sp: miro_ego_api::SecurityProfile) -> miro_ego_api::ApiResult<Value> { self.inner.device_info(sp).await }
        async fn capture_status(&self) -> miro_ego_api::ApiResult<Value> { self.inner.capture_status().await }
        async fn capture_snapshot_event(&self) -> miro_ego_api::ApiResult<Value> { self.inner.capture_snapshot_event().await }
        async fn capture_readiness(&self) -> miro_ego_api::ApiResult<Value> { self.inner.capture_readiness().await }
        async fn capture_safe_swap(&self) -> miro_ego_api::ApiResult<Option<Value>> { self.inner.capture_safe_swap().await }
        async fn start_capture(&self, c: miro_ego_api::IdempotentCommand) -> miro_ego_api::ApiResult<miro_ego_api::CommandOutcome> { self.inner.start_capture(c).await }
        async fn stop_capture(&self, c: miro_ego_api::IdempotentCommand) -> miro_ego_api::ApiResult<miro_ego_api::CommandOutcome> { self.inner.stop_capture(c).await }
        async fn capture_timer(&self) -> miro_ego_api::ApiResult<Value> { self.inner.capture_timer().await }
        async fn set_capture_timer(&self, e: bool, d: u64) -> miro_ego_api::ApiResult<Value> { self.inner.set_capture_timer(e, d).await }
        async fn camera_focus(&self) -> miro_ego_api::ApiResult<Option<Value>> { self.inner.camera_focus().await }
        async fn set_camera_focus(&self, c: miro_ego_api::IdempotentCommand) -> miro_ego_api::ApiResult<miro_ego_api::CommandOutcome> { self.inner.set_camera_focus(c).await }
        async fn list_sessions(&self, q: miro_ego_api::ListSessionsQuery) -> miro_ego_api::ApiResult<Value> { self.inner.list_sessions(q).await }
        async fn open_session_manifest(&self, s: &str) -> miro_ego_api::ApiResult<miro_ego_api::RawManifest> { self.inner.open_session_manifest(s).await }
        async fn retained_unsuccessful_outcome(&self, s: &str) -> miro_ego_api::ApiResult<Option<Value>> { self.inner.retained_unsuccessful_outcome(s).await }
        async fn artifact_io_busy_state(&self) -> miro_ego_api::ApiResult<Option<String>> { self.inner.artifact_io_busy_state().await }
        async fn open_session_artifact(&self, s: &str, a: &str) -> miro_ego_api::ApiResult<miro_ego_api::OpenRepresentation> { self.inner.open_session_artifact(s, a).await }
        async fn delete_sessions(&self, c: miro_ego_api::IdempotentCommand) -> miro_ego_api::ApiResult<miro_ego_api::CommandOutcome> { self.inner.delete_sessions(c).await }
        async fn list_logs(&self, q: miro_ego_api::ListLogsQuery) -> miro_ego_api::ApiResult<Value> { self.inner.list_logs(q).await }
        async fn open_log_file(&self, n: &str) -> miro_ego_api::ApiResult<miro_ego_api::OpenRepresentation> { self.inner.open_log_file(n).await }
        fn preview_mjpeg_hub(&self) -> Option<Arc<miro_ego_api::MjpegPreviewHub>> { self.inner.preview_mjpeg_hub() }
        fn preview_h264_hub(&self) -> Option<Arc<miro_ego_api::H264PreviewHub>> { Some(Arc::clone(&self.empty_h264)) }
        fn capture_event_buffer(&self) -> Arc<miro_ego_api::EventReplayBuffer> { self.inner.capture_event_buffer() }
        fn network_event_buffer(&self) -> Arc<miro_ego_api::NetworkEventReplayBuffer> { self.inner.network_event_buffer() }
        async fn clock_status(&self, p: &str) -> miro_ego_api::ApiResult<Value> { self.inner.clock_status(p).await }
        async fn clock_inspect(&self, p: &str) -> miro_ego_api::ApiResult<Value> { self.inner.clock_inspect(p).await }
        async fn clock_sync(&self, p: &str, b: Value) -> miro_ego_api::ApiResult<Value> { self.inner.clock_sync(p, b).await }
        async fn voice_status(&self) -> miro_ego_api::ApiResult<Value> { self.inner.voice_status().await }
        async fn set_voice_volume(&self, v: u8) -> miro_ego_api::ApiResult<Value> { self.inner.set_voice_volume(v).await }
        async fn network_status(&self) -> miro_ego_api::ApiResult<Value> { self.inner.network_status().await }
        async fn network_scan(&self) -> miro_ego_api::ApiResult<Value> { self.inner.network_scan().await }
        async fn create_network_credential(&self, p: &str, pw: &str) -> miro_ego_api::ApiResult<Value> { self.inner.create_network_credential(p, pw).await }
        async fn apply_network_desired_state(&self, c: miro_ego_api::IdempotentCommand) -> miro_ego_api::ApiResult<miro_ego_api::CommandOutcome> { self.inner.apply_network_desired_state(c).await }
        async fn retry_network_transaction(&self, c: miro_ego_api::IdempotentCommand) -> miro_ego_api::ApiResult<miro_ego_api::CommandOutcome> { self.inner.retry_network_transaction(c).await }
        async fn forget_network_client_profile(&self, c: miro_ego_api::IdempotentCommand) -> miro_ego_api::ApiResult<miro_ego_api::CommandOutcome> { self.inner.forget_network_client_profile(c).await }
        async fn firmware_status(&self) -> miro_ego_api::ApiResult<Value> { self.inner.firmware_status().await }
        async fn firmware_check(&self, f: bool) -> miro_ego_api::ApiResult<Value> { self.inner.firmware_check(f).await }
        async fn firmware_update(&self, c: miro_ego_api::IdempotentCommand) -> miro_ego_api::ApiResult<miro_ego_api::CommandOutcome> { self.inner.firmware_update(c).await }
        async fn firmware_rollback(&self, c: miro_ego_api::IdempotentCommand) -> miro_ego_api::ApiResult<miro_ego_api::CommandOutcome> { self.inner.firmware_rollback(c).await }
        fn record_audit(&self, e: miro_ego_api::AuditEvent) -> miro_ego_api::ApiResult<()> { self.inner.record_audit(e) }
        fn get_web_asset(&self, n: &str) -> Option<miro_ego_api::StaticAsset> { self.inner.get_web_asset(n) }
    }

    srv.shutdown().await;

    let uninit_backend: Arc<dyn GatewayBackend> = Arc::new(UninitH264Backend {
        inner: MockGatewayBackend::with_options(false),
        empty_h264: Arc::new(miro_ego_api::H264PreviewHub::new_fmp4(16)),
    });
    let cfg = GatewayServerConfig::http("127.0.0.1:0".parse().unwrap());
    let server = GatewayServer::bind(
        cfg,
        uninit_backend,
        Arc::new(SecurityPolicy::lab_default()),
    )
    .await
    .unwrap();
    let addr2 = server.local_addr();
    let (tx2, rx2) = oneshot::channel::<()>();
    let h2 = tokio::spawn(async move {
        let _ = server
            .serve_with_graceful_shutdown(async {
                let _ = rx2.await;
            })
            .await;
    });
    let (st_uninit, hdrs_uninit, body_uninit) =
        http_request(addr2, Method::GET, "/api/v1/preview/h264", &[], None).await;
    assert_eq!(st_uninit, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        hdrs_uninit
            .get(HEADER_ERROR_CODE)
            .and_then(|v| v.to_str().ok()),
        Some("preview_unavailable")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&body_uninit).unwrap()["error"]["code"],
        "preview_unavailable"
    );
    let _ = tx2.send(());
    let _ = tokio::time::timeout(Duration::from_secs(2), h2).await;
}
