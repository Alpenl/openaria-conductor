//! Miro-Ego v1 async `tokio` + `axum` + `rustls` HTTP/HTTPS device gateway.
//!
//! Implements all 37 OpenAPI v1 operations under `/api/v1/...`, zero-copy MJPEG & H.264 preview
//! streaming, RFC 9110 `Range`/`If-Range` downloads with `posix_fadvise` page-cache eviction,
//! bounded SSE replay buffers with DoS-safe `Last-Event-ID`, and strict Host/Origin/CORS/CSRF
//! security enforcement.

pub mod backend;
pub mod downloads;
pub mod error;
pub mod handlers;
pub mod middleware;
pub mod preview;
pub mod range;
pub mod router;
pub mod security;
pub mod server;
pub mod sse;

pub use backend::{
    CommandOutcome, GatewayBackend, IdempotentCommand, ListLogsQuery, ListSessionsQuery,
    MockArtifact, MockArtifactBacking, MockGatewayBackend, MockLogEntry, MockSession, RawManifest,
    StaticAsset,
};
pub use downloads::{DownloadSource, OpenRepresentation, ResolvedRange, open_safe_log_file};
pub use error::{
    API_ERROR_SCHEMA, ApiError, ApiResult, HEADER_AUDIT_STATUS, HEADER_ERROR_CODE,
    HEADER_MANIFEST_SHA256, HEADER_WAIT_STATE,
};
pub use middleware::{CORS_EXPOSE_HEADERS, GatewayRuntimeConfig, GatewayState, MAX_BODY_BYTES};
pub use preview::{
    H264PreviewHub, MJPEG_BOUNDARY, MJPEG_CONTENT_TYPE, MjpegPreviewHub, PreviewFrame,
};
pub use range::{
    ByteRange, UnsatisfiableRange, infer_log_media_type, is_valid_log_name, is_valid_log_prefix,
    parse_single_range,
};
pub use router::{API_V1_PREFIX, ROUTE_TABLE, RouteOperationSpec, build_router};
pub use security::{
    ALL_V1_OPERATION_IDS, AUDIT_EVENT_SCHEMA, AuditEvent, IDEMPOTENCY_OPERATION_IDS,
    MUTATION_OPERATION_IDS, Principal, SecurityPolicy, SecurityProfile, extract_host_hostname,
    is_allowed_gateway_host, is_valid_idempotency_key, normalize_allowed_hosts,
};
pub use server::{GatewayServer, GatewayServerConfig, TlsConfig};
pub use sse::{
    CAPTURE_EVENT_SCHEMA, DEFAULT_REPLAY_CAPACITY, EventCursor, EventReplayBuffer,
    NETWORK_EVENT_SCHEMA, NetworkEventReplayBuffer, SseEvent, parse_last_event_id,
};

use serde::{Deserialize, Serialize};

/// Legacy Wave 1/2 bootstrap server configuration kept for workspace compatibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiServerConfig {
    pub bind_host: String,
    pub bind_port: u16,
    pub api_prefix: String,
    pub enable_h264_preview: bool,
}

impl Default for ApiServerConfig {
    fn default() -> Self {
        Self {
            bind_host: "0.0.0.0".to_owned(),
            bind_port: 8080,
            api_prefix: API_V1_PREFIX.to_owned(),
            enable_h264_preview: true,
        }
    }
}

/// Summary of a user-visible diagnostic log file under `/data/logs/`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogFileSummary {
    pub name: String,
    pub bytes: u64,
    pub modified_epoch_secs: u64,
}

/// Synchronous summary control-plane trait preserved from Wave 1/2 workspace scaffolding.
pub trait ApiService: Send + Sync {
    fn health_json(&self) -> ApiResult<serde_json::Value>;
    fn device_info_json(&self) -> ApiResult<serde_json::Value>;
    fn capture_status_json(&self) -> ApiResult<serde_json::Value>;
    fn list_logs(&self) -> ApiResult<Vec<LogFileSummary>>;
}
