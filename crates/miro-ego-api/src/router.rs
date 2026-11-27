//! Declarative Miro-Ego v1 OpenAPI route table (37 operations across 33 path templates)
//! and Axum `Router` assembly with exact `OPTIONS` preflight and `405 Allow` semantics.

use crate::error::ApiError;
use crate::handlers::{capture, device, logs, network, sessions};
use crate::middleware::{
    GatewayState, RequestCtx, gateway_framing_and_host_middleware, handle_options_preflight,
};
use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Method, Request, Response};
use axum::middleware;
use axum::routing::{MethodRouter, get, post};
use std::sync::Arc;

pub const API_V1_PREFIX: &str = "/api/v1";

const GET_HEADERS: &[&str] = &["Authorization"];
const PREFER_GET_HEADERS: &[&str] = &["Authorization", "Prefer"];
const SSE_HEADERS: &[&str] = &["Authorization", "Last-Event-ID"];
const RANGE_HEADERS: &[&str] = &["Authorization", "If-Range", "Range"];
const POST_WRITE_HEADERS: &[&str] = &["Authorization", "Content-Type", "X-CSRF-Token"];
const POST_COMMAND_HEADERS: &[&str] = &[
    "Authorization",
    "Content-Type",
    "X-CSRF-Token",
    "Idempotency-Key",
];

/// Declarative entry describing one OpenAPI v1 operation in [`ROUTE_TABLE`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteOperationSpec {
    pub path_template: &'static str,
    pub method: &'static str,
    pub operation_id: &'static str,
    pub allow_methods: &'static [&'static str],
    pub allowed_headers: &'static [&'static str],
}

/// Authoritative compile-time table of all 37 Miro-Ego Device API v1 operations.
pub const ROUTE_TABLE: [RouteOperationSpec; 37] = [
    RouteOperationSpec {
        path_template: "/device",
        method: "GET",
        operation_id: "getDevice",
        allow_methods: &["GET", "OPTIONS"],
        allowed_headers: GET_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/voice",
        method: "GET",
        operation_id: "getVoice",
        allow_methods: &["GET", "POST", "OPTIONS"],
        allowed_headers: GET_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/voice",
        method: "POST",
        operation_id: "setVoiceVolume",
        allow_methods: &["GET", "POST", "OPTIONS"],
        allowed_headers: POST_WRITE_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/clock",
        method: "GET",
        operation_id: "getDeviceClock",
        allow_methods: &["GET", "OPTIONS"],
        allowed_headers: GET_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/clock/check",
        method: "GET",
        operation_id: "inspectDeviceClock",
        allow_methods: &["GET", "OPTIONS"],
        allowed_headers: GET_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/clock/sync",
        method: "POST",
        operation_id: "syncDeviceClock",
        allow_methods: &["POST", "OPTIONS"],
        allowed_headers: POST_WRITE_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/firmware",
        method: "GET",
        operation_id: "firmwareStatus",
        allow_methods: &["GET", "OPTIONS"],
        allowed_headers: GET_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/firmware/check",
        method: "GET",
        operation_id: "checkFirmware",
        allow_methods: &["GET", "OPTIONS"],
        allowed_headers: GET_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/firmware/update",
        method: "POST",
        operation_id: "firmwareUpdate",
        allow_methods: &["POST", "OPTIONS"],
        allowed_headers: POST_COMMAND_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/firmware/rollback",
        method: "POST",
        operation_id: "rollbackFirmware",
        allow_methods: &["POST", "OPTIONS"],
        allowed_headers: POST_COMMAND_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/camera/focus",
        method: "GET",
        operation_id: "getCameraFocus",
        allow_methods: &["GET", "POST", "OPTIONS"],
        allowed_headers: GET_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/camera/focus",
        method: "POST",
        operation_id: "setCameraFocus",
        allow_methods: &["GET", "POST", "OPTIONS"],
        allowed_headers: POST_COMMAND_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/capture/readiness",
        method: "GET",
        operation_id: "getCaptureReadiness",
        allow_methods: &["GET", "OPTIONS"],
        allowed_headers: GET_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/capture/status",
        method: "GET",
        operation_id: "getCaptureStatus",
        allow_methods: &["GET", "OPTIONS"],
        allowed_headers: GET_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/capture/timer",
        method: "GET",
        operation_id: "getCaptureTimer",
        allow_methods: &["GET", "POST", "OPTIONS"],
        allowed_headers: GET_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/capture/timer",
        method: "POST",
        operation_id: "setCaptureTimer",
        allow_methods: &["GET", "POST", "OPTIONS"],
        allowed_headers: POST_WRITE_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/capture/start",
        method: "POST",
        operation_id: "startCapture",
        allow_methods: &["POST", "OPTIONS"],
        allowed_headers: POST_COMMAND_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/capture/stop",
        method: "POST",
        operation_id: "stopCapture",
        allow_methods: &["POST", "OPTIONS"],
        allowed_headers: POST_COMMAND_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/capture/events",
        method: "GET",
        operation_id: "streamCaptureEvents",
        allow_methods: &["GET", "OPTIONS"],
        allowed_headers: SSE_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/capture/safe-swap",
        method: "GET",
        operation_id: "getCurrentSafeSwapReceipt",
        allow_methods: &["GET", "OPTIONS"],
        allowed_headers: GET_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/preview",
        method: "GET",
        operation_id: "getPreview",
        allow_methods: &["GET", "OPTIONS"],
        allowed_headers: GET_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/preview/h264",
        method: "GET",
        operation_id: "getPreviewH264",
        allow_methods: &["GET", "OPTIONS"],
        allowed_headers: GET_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/network",
        method: "GET",
        operation_id: "getNetworkStatus",
        allow_methods: &["GET", "OPTIONS"],
        allowed_headers: GET_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/network/scan",
        method: "GET",
        operation_id: "scanNetworks",
        allow_methods: &["GET", "OPTIONS"],
        allowed_headers: GET_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/network/events",
        method: "GET",
        operation_id: "streamNetworkEvents",
        allow_methods: &["GET", "OPTIONS"],
        allowed_headers: SSE_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/network/credentials",
        method: "POST",
        operation_id: "createNetworkCredentialReference",
        allow_methods: &["POST", "OPTIONS"],
        allowed_headers: POST_WRITE_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/network/apply",
        method: "POST",
        operation_id: "applyNetworkDesiredState",
        allow_methods: &["POST", "OPTIONS"],
        allowed_headers: POST_COMMAND_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/network/retry",
        method: "POST",
        operation_id: "retryNetworkTransaction",
        allow_methods: &["POST", "OPTIONS"],
        allowed_headers: POST_COMMAND_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/network/forget",
        method: "POST",
        operation_id: "forgetNetworkClientProfile",
        allow_methods: &["POST", "OPTIONS"],
        allowed_headers: POST_COMMAND_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/sessions",
        method: "GET",
        operation_id: "listSessions",
        allow_methods: &["GET", "OPTIONS"],
        allowed_headers: PREFER_GET_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/sessions/delete",
        method: "POST",
        operation_id: "deleteSessions",
        allow_methods: &["POST", "OPTIONS"],
        allowed_headers: POST_COMMAND_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/sessions/{session_id}",
        method: "GET",
        operation_id: "getSession",
        allow_methods: &["GET", "OPTIONS"],
        allowed_headers: GET_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/sessions/{session_id}/unsuccessful-outcome",
        method: "GET",
        operation_id: "getRetainedUnsuccessfulSessionOutcome",
        allow_methods: &["GET", "OPTIONS"],
        allowed_headers: GET_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/sessions/{session_id}/artifacts/{artifact_id}",
        method: "GET",
        operation_id: "getSessionArtifact",
        allow_methods: &["GET", "HEAD", "OPTIONS"],
        allowed_headers: RANGE_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/sessions/{session_id}/artifacts/{artifact_id}",
        method: "HEAD",
        operation_id: "headSessionArtifact",
        allow_methods: &["GET", "HEAD", "OPTIONS"],
        allowed_headers: RANGE_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/logs",
        method: "GET",
        operation_id: "listLogs",
        allow_methods: &["GET", "OPTIONS"],
        allowed_headers: GET_HEADERS,
    },
    RouteOperationSpec {
        path_template: "/logs/{name}",
        method: "GET",
        operation_id: "getLogFile",
        allow_methods: &["GET", "OPTIONS"],
        allowed_headers: RANGE_HEADERS,
    },
];

fn attach_options_and_405(
    mut router: MethodRouter<Arc<GatewayState>>,
    allow_methods: &'static [&'static str],
    headers_for_method: fn(&str) -> &'static [&'static str],
) -> MethodRouter<Arc<GatewayState>> {
    let allow_str = allow_methods.join(", ");
    if !allow_methods.contains(&"HEAD") {
        let head_allow = allow_str.clone();
        router = router.head(
            move |State(state): State<Arc<GatewayState>>, headers: HeaderMap| async move {
                let ctx = RequestCtx::new_with_state(&Method::HEAD, &headers, &state);
                ctx.error_response(ApiError::method_not_allowed(&head_allow))
            },
        );
    }
    router
        .options(
            move |State(state): State<Arc<GatewayState>>, headers: HeaderMap| async move {
                handle_options_preflight(&state, &headers, allow_methods, headers_for_method)
            },
        )
        .fallback(
            move |State(state): State<Arc<GatewayState>>,
                  method: Method,
                  headers: HeaderMap| async move {
                let ctx = RequestCtx::new_with_state(&method, &headers, &state);
                ctx.error_response(ApiError::method_not_allowed(&allow_str))
            },
        )
}

fn get_only_headers(_: &str) -> &'static [&'static str] {
    GET_HEADERS
}

fn prefer_get_headers(_: &str) -> &'static [&'static str] {
    PREFER_GET_HEADERS
}

fn sse_headers(_: &str) -> &'static [&'static str] {
    SSE_HEADERS
}

fn range_headers(_: &str) -> &'static [&'static str] {
    RANGE_HEADERS
}

fn post_write_headers(_: &str) -> &'static [&'static str] {
    POST_WRITE_HEADERS
}

fn post_command_headers(_: &str) -> &'static [&'static str] {
    POST_COMMAND_HEADERS
}

fn get_or_post_write_headers(method: &str) -> &'static [&'static str] {
    if method == "POST" {
        POST_WRITE_HEADERS
    } else {
        GET_HEADERS
    }
}

fn get_or_post_command_headers(method: &str) -> &'static [&'static str] {
    if method == "POST" {
        POST_COMMAND_HEADERS
    } else {
        GET_HEADERS
    }
}

/// Build the complete Axum `Router` for Miro-Ego Device API v1 (`/api/v1/...`)
/// plus static web assets (`/` and `/<asset>`), CORS preflights, `405 Allow`,
/// and outer Host/framing security middleware.
pub fn build_router(state: Arc<GatewayState>) -> Router {
    let api_router = Router::new()
        // --- Device, Voice, Clock, Firmware ---
        .route(
            "/device",
            attach_options_and_405(
                get(device::get_device),
                &["GET", "OPTIONS"],
                get_only_headers,
            ),
        )
        .route(
            "/voice",
            attach_options_and_405(
                get(device::get_voice).post(device::set_voice_volume),
                &["GET", "POST", "OPTIONS"],
                get_or_post_write_headers,
            ),
        )
        .route(
            "/clock",
            attach_options_and_405(
                get(device::get_device_clock),
                &["GET", "OPTIONS"],
                get_only_headers,
            ),
        )
        .route(
            "/clock/check",
            attach_options_and_405(
                get(device::inspect_device_clock),
                &["GET", "OPTIONS"],
                get_only_headers,
            ),
        )
        .route(
            "/clock/sync",
            attach_options_and_405(
                post(device::sync_device_clock),
                &["POST", "OPTIONS"],
                post_write_headers,
            ),
        )
        .route(
            "/firmware",
            attach_options_and_405(
                get(device::firmware_status),
                &["GET", "OPTIONS"],
                get_only_headers,
            ),
        )
        .route(
            "/firmware/check",
            attach_options_and_405(
                get(device::firmware_check),
                &["GET", "OPTIONS"],
                get_only_headers,
            ),
        )
        .route(
            "/firmware/update",
            attach_options_and_405(
                post(device::firmware_update),
                &["POST", "OPTIONS"],
                post_command_headers,
            ),
        )
        .route(
            "/firmware/rollback",
            attach_options_and_405(
                post(device::firmware_rollback),
                &["POST", "OPTIONS"],
                post_command_headers,
            ),
        )
        // --- Capture, Focus & Preview ---
        .route(
            "/camera/focus",
            attach_options_and_405(
                get(capture::get_camera_focus).post(capture::set_camera_focus),
                &["GET", "POST", "OPTIONS"],
                get_or_post_command_headers,
            ),
        )
        .route(
            "/capture/readiness",
            attach_options_and_405(
                get(capture::get_capture_readiness),
                &["GET", "OPTIONS"],
                get_only_headers,
            ),
        )
        .route(
            "/capture/status",
            attach_options_and_405(
                get(capture::get_capture_status),
                &["GET", "OPTIONS"],
                get_only_headers,
            ),
        )
        .route(
            "/capture/timer",
            attach_options_and_405(
                get(capture::get_capture_timer).post(capture::set_capture_timer),
                &["GET", "POST", "OPTIONS"],
                get_or_post_write_headers,
            ),
        )
        .route(
            "/capture/start",
            attach_options_and_405(
                post(capture::start_capture),
                &["POST", "OPTIONS"],
                post_command_headers,
            ),
        )
        .route(
            "/capture/stop",
            attach_options_and_405(
                post(capture::stop_capture),
                &["POST", "OPTIONS"],
                post_command_headers,
            ),
        )
        .route(
            "/capture/events",
            attach_options_and_405(
                get(capture::stream_capture_events),
                &["GET", "OPTIONS"],
                sse_headers,
            ),
        )
        .route(
            "/capture/safe-swap",
            attach_options_and_405(
                get(capture::get_safe_swap),
                &["GET", "OPTIONS"],
                get_only_headers,
            ),
        )
        .route(
            "/preview",
            attach_options_and_405(
                get(capture::get_preview),
                &["GET", "OPTIONS"],
                get_only_headers,
            ),
        )
        .route(
            "/preview/h264",
            attach_options_and_405(
                get(capture::get_preview_h264),
                &["GET", "OPTIONS"],
                get_only_headers,
            ),
        )
        // --- Network Control ---
        .route(
            "/network",
            attach_options_and_405(
                get(network::get_network_status),
                &["GET", "OPTIONS"],
                get_only_headers,
            ),
        )
        .route(
            "/network/scan",
            attach_options_and_405(
                get(network::scan_networks),
                &["GET", "OPTIONS"],
                get_only_headers,
            ),
        )
        .route(
            "/network/events",
            attach_options_and_405(
                get(network::stream_network_events),
                &["GET", "OPTIONS"],
                sse_headers,
            ),
        )
        .route(
            "/network/credentials",
            attach_options_and_405(
                post(network::create_network_credential),
                &["POST", "OPTIONS"],
                post_write_headers,
            ),
        )
        .route(
            "/network/apply",
            attach_options_and_405(
                post(network::apply_network_desired_state),
                &["POST", "OPTIONS"],
                post_command_headers,
            ),
        )
        .route(
            "/network/retry",
            attach_options_and_405(
                post(network::retry_network_transaction),
                &["POST", "OPTIONS"],
                post_command_headers,
            ),
        )
        .route(
            "/network/forget",
            attach_options_and_405(
                post(network::forget_network_client_profile),
                &["POST", "OPTIONS"],
                post_command_headers,
            ),
        )
        // --- Sessions & Artifacts ---
        .route(
            "/sessions",
            attach_options_and_405(
                get(sessions::list_sessions),
                &["GET", "OPTIONS"],
                prefer_get_headers,
            ),
        )
        .route(
            "/sessions/delete",
            attach_options_and_405(
                post(sessions::delete_sessions),
                &["POST", "OPTIONS"],
                post_command_headers,
            ),
        )
        .route(
            "/sessions/{session_id}",
            attach_options_and_405(
                get(sessions::get_session_manifest),
                &["GET", "OPTIONS"],
                get_only_headers,
            ),
        )
        .route(
            "/sessions/{session_id}/unsuccessful-outcome",
            attach_options_and_405(
                get(sessions::get_unsuccessful_outcome),
                &["GET", "OPTIONS"],
                get_only_headers,
            ),
        )
        .route(
            "/sessions/{session_id}/artifacts/{artifact_id}",
            attach_options_and_405(
                get(sessions::session_artifact).head(sessions::session_artifact),
                &["GET", "HEAD", "OPTIONS"],
                range_headers,
            ),
        )
        // --- Diagnostic Logs ---
        .route(
            "/logs",
            attach_options_and_405(get(logs::list_logs), &["GET", "OPTIONS"], get_only_headers),
        )
        .route(
            "/logs/{*name}",
            attach_options_and_405(get(logs::get_log_file), &["GET", "OPTIONS"], range_headers),
        );

    Router::new()
        .nest(API_V1_PREFIX, api_router)
        .route(
            "/",
            attach_options_and_405(
                get(static_root_handler).head(static_root_handler),
                &["GET", "HEAD", "OPTIONS"],
                get_only_headers,
            ),
        )
        .fallback(static_asset_or_not_found)
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            gateway_framing_and_host_middleware,
        ))
        .with_state(state)
}

async fn static_root_handler(
    State(state): State<Arc<GatewayState>>,
    method: Method,
    headers: HeaderMap,
) -> Response<Body> {
    device::serve_web_asset(state, method, headers, "index.html").await
}

async fn static_asset_or_not_found(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, _) = req.into_parts();
    let path = parts.uri.path();
    let method = parts.method;
    let headers = parts.headers;

    // Only single-segment non-dot filenames under `/` may resolve to static web assets.
    if let Some(asset_name) = path.strip_prefix('/')
        && !asset_name.is_empty()
        && !asset_name.contains('/')
        && !asset_name.starts_with('.')
        && state.backend.get_web_asset(asset_name).is_some()
    {
        return match method {
            Method::GET | Method::HEAD => {
                device::serve_web_asset(state, method, headers, asset_name).await
            }
            Method::OPTIONS => handle_options_preflight(
                &state,
                &headers,
                &["GET", "HEAD", "OPTIONS"],
                get_only_headers,
            ),
            _ => {
                let ctx = RequestCtx::new_with_state(&method, &headers, &state);
                ctx.error_response(ApiError::method_not_allowed("GET, HEAD, OPTIONS"))
            }
        };
    }

    let ctx = RequestCtx::new_with_state(&method, &headers, &state);
    ctx.error_response(ApiError::not_found("endpoint not found"))
}
