//! Device identity, voice volume, clock calibration/sync, firmware control, and static web assets.

use crate::backend::IdempotentCommand;
use crate::error::ApiError;
use crate::handlers::{is_lower_hex_exact, parse_strict_query, truncate_untrusted_field};
use crate::middleware::{GatewayState, RequestCtx, read_strict_json_object_with_timeout};
use axum::body::Body;
use axum::extract::State;
use axum::http::header;
use axum::http::{HeaderMap, Method, Request, Response, StatusCode};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

pub const WEB_CONTENT_SECURITY_POLICY: &str = "default-src 'self'; base-uri 'none'; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; img-src 'self' blob: data:; media-src 'self' blob:; object-src 'none'; script-src 'self'; worker-src 'self' blob:; style-src 'self'";

const CLOCK_MIN_MS: u64 = 1_704_067_200_000;
const CLOCK_MAX_MS: u64 = 4_102_444_799_999;

/// Serve an immutable embedded web console asset (`/` -> `index.html`, `/<asset>`).
pub async fn serve_web_asset(
    state: Arc<GatewayState>,
    method: Method,
    headers: HeaderMap,
    asset_name: &str,
) -> Response<Body> {
    let ctx = RequestCtx::new_with_state(&method, &headers, &state);
    let Some(asset) = state.backend.get_web_asset(asset_name) else {
        return ctx.error_response(ApiError::not_found("resource not found"));
    };
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, asset.content_type.as_str())
        .header(header::CONTENT_LENGTH, asset.bytes.len().to_string())
        .header(header::ETAG, asset.etag.as_str())
        .header(header::CACHE_CONTROL, "no-store")
        .header("Content-Security-Policy", WEB_CONTENT_SECURITY_POLICY)
        .header("Cross-Origin-Resource-Policy", "same-origin")
        .header("Referrer-Policy", "no-referrer")
        .header("X-Content-Type-Options", "nosniff")
        .header("X-Frame-Options", "DENY");
    builder = ctx.apply_common_headers(builder, &[]);
    let body = if method == Method::HEAD {
        Body::empty()
    } else {
        Body::from(asset.bytes)
    };
    builder.body(body).unwrap_or_else(|_| {
        ctx.error_response(ApiError::internal(
            "response_build_failed",
            "failed to build web asset response",
        ))
    })
}

/// `GET /api/v1/device` (`getDevice`)
pub async fn get_device(
    State(state): State<Arc<GatewayState>>,
    method: Method,
    headers: HeaderMap,
) -> Response<Body> {
    let mut ctx = RequestCtx::new_with_state(&method, &headers, &state);
    if let Err(resp) = ctx.authorize_read(&state, &headers, "getDevice", None) {
        return *resp;
    }
    match state.backend.device_info(state.security.profile()).await {
        Ok(body) => ctx.json_response(StatusCode::OK, &body, &[]),
        Err(err) => ctx.error_response(err),
    }
}

/// `GET /api/v1/voice` (`getVoice`)
pub async fn get_voice(
    State(state): State<Arc<GatewayState>>,
    method: Method,
    headers: HeaderMap,
) -> Response<Body> {
    let mut ctx = RequestCtx::new_with_state(&method, &headers, &state);
    if let Err(resp) = ctx.authorize_read(&state, &headers, "getVoice", None) {
        return *resp;
    }
    match state.backend.voice_status().await {
        Ok(body) => ctx.json_response(StatusCode::OK, &body, &[]),
        Err(err) => ctx.error_response(err),
    }
}

/// `POST /api/v1/voice` (`setVoiceVolume`)
pub async fn set_voice_volume(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, body) = req.into_parts();
    let mut ctx = RequestCtx::new_with_state(&parts.method, &parts.headers, &state);
    let principal = match ctx.authorize_write_headers(&state, &parts.headers, "setVoiceVolume") {
        Ok(p) => p,
        Err(resp) => return *resp,
    };
    let (json, _) = match read_strict_json_object_with_timeout(
        &parts.headers,
        body,
        state.config.request_body_timeout,
    )
    .await
    {
        Ok(v) => v,
        Err(err) => {
            ctx.audit(&state, "setVoiceVolume", None, &err.code, Some(&principal));
            return ctx.error_response(err);
        }
    };
    let volume = match validate_voice_set_body(&json) {
        Ok(v) => v,
        Err(err) => {
            ctx.audit(&state, "setVoiceVolume", None, &err.code, Some(&principal));
            return ctx.error_response(err);
        }
    };
    if let Err(resp) = ctx.record_allowed(&state, "setVoiceVolume", None, &principal) {
        return *resp;
    }
    match state.backend.set_voice_volume(volume).await {
        Ok(resp_body) => ctx.json_response(StatusCode::OK, &resp_body, &[]),
        Err(err) => ctx.error_response(err),
    }
}

fn validate_voice_set_body(body: &Value) -> Result<u8, ApiError> {
    let Some(obj) = body.as_object() else {
        return Err(ApiError::bad_request("voice request must be a JSON object"));
    };
    if obj.len() != 2 || obj.get("schema").and_then(Value::as_str) != Some("miro-ego.voice-set.v1")
    {
        return Err(ApiError::bad_request(
            "voice request must have schema=miro-ego.voice-set.v1 and volume only",
        ));
    }
    let Some(vol) = obj.get("volume").and_then(Value::as_u64) else {
        return Err(ApiError::bad_request(
            "volume must be an integer in 0..=100",
        ));
    };
    if vol > 100 {
        return Err(ApiError::bad_request(
            "volume must be an integer in 0..=100",
        ));
    };
    Ok(vol as u8)
}

/// `GET /api/v1/clock` (`getDeviceClock`)
pub async fn get_device_clock(
    State(state): State<Arc<GatewayState>>,
    method: Method,
    headers: HeaderMap,
) -> Response<Body> {
    let mut ctx = RequestCtx::new_with_state(&method, &headers, &state);
    let principal = match ctx.authorize_read(&state, &headers, "getDeviceClock", None) {
        Ok(p) => p,
        Err(resp) => return *resp,
    };
    match state.backend.clock_status(principal.principal_id()).await {
        Ok(body) => ctx.json_response(StatusCode::OK, &body, &[]),
        Err(err) => ctx.error_response(err),
    }
}

/// `GET /api/v1/clock/check` (`inspectDeviceClock`)
pub async fn inspect_device_clock(
    State(state): State<Arc<GatewayState>>,
    method: Method,
    headers: HeaderMap,
) -> Response<Body> {
    let mut ctx = RequestCtx::new_with_state(&method, &headers, &state);
    let principal = match ctx.authorize_read(&state, &headers, "inspectDeviceClock", None) {
        Ok(p) => p,
        Err(resp) => return *resp,
    };
    match state.backend.clock_inspect(principal.principal_id()).await {
        Ok(body) => ctx.json_response(StatusCode::OK, &body, &[]),
        Err(err) => ctx.error_response(err),
    }
}

/// `POST /api/v1/clock/sync` (`syncDeviceClock`)
pub async fn sync_device_clock(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, body) = req.into_parts();
    let mut ctx = RequestCtx::new_with_state(&parts.method, &parts.headers, &state);
    let principal = match ctx.authorize_write_headers(&state, &parts.headers, "syncDeviceClock") {
        Ok(p) => p,
        Err(resp) => return *resp,
    };
    let (json, _) = match read_strict_json_object_with_timeout(
        &parts.headers,
        body,
        state.config.request_body_timeout,
    )
    .await
    {
        Ok(v) => v,
        Err(err) => {
            ctx.audit(&state, "syncDeviceClock", None, &err.code, Some(&principal));
            return ctx.error_response(err);
        }
    };
    if let Err(err) = validate_clock_sync_body(&json) {
        ctx.audit(&state, "syncDeviceClock", None, &err.code, Some(&principal));
        return ctx.error_response(err);
    }
    if let Err(resp) = ctx.record_allowed(&state, "syncDeviceClock", None, &principal) {
        return *resp;
    }
    match state
        .backend
        .clock_sync(principal.principal_id(), json)
        .await
    {
        Ok(resp_body) => ctx.json_response(StatusCode::OK, &resp_body, &[]),
        Err(err) => ctx.error_response(err),
    }
}

fn validate_clock_sync_body(body: &Value) -> Result<(), ApiError> {
    let Some(obj) = body.as_object() else {
        return Err(ApiError::bad_request(
            "clock sync body must be a JSON object",
        ));
    };
    for key in obj.keys() {
        if !matches!(
            key.as_str(),
            "schema" | "challenge" | "unix_time_ms" | "replace"
        ) {
            return Err(ApiError::bad_request(format!(
                "unknown field in clock sync request: {}",
                truncate_untrusted_field(key)
            )));
        }
    }
    if obj.get("schema").and_then(Value::as_str) != Some("miro-ego.clock-sync-request.v1") {
        return Err(ApiError::bad_request(
            "clock sync schema must be miro-ego.clock-sync-request.v1",
        ));
    }
    let Some(challenge) = obj.get("challenge").and_then(Value::as_str) else {
        return Err(ApiError::bad_request("missing challenge string"));
    };
    if !is_lower_hex_exact(challenge, 32) {
        return Err(ApiError::bad_request(
            "challenge must be 32 lowercase hexadecimal characters",
        ));
    }
    let Some(ts) = obj.get("unix_time_ms").and_then(Value::as_u64) else {
        return Err(ApiError::bad_request("unix_time_ms must be an integer"));
    };
    if !(CLOCK_MIN_MS..=CLOCK_MAX_MS).contains(&ts) {
        return Err(ApiError::bad_request(
            "unix_time_ms must lie within [2024, 2100) UTC",
        ));
    }
    if let Some(replace) = obj.get("replace")
        && !replace.is_boolean()
    {
        return Err(ApiError::bad_request("replace must be a boolean"));
    }
    Ok(())
}

/// `GET /api/v1/firmware` (`firmwareStatus`)
pub async fn firmware_status(
    State(state): State<Arc<GatewayState>>,
    method: Method,
    headers: HeaderMap,
) -> Response<Body> {
    let mut ctx = RequestCtx::new_with_state(&method, &headers, &state);
    if let Err(resp) = ctx.authorize_read(&state, &headers, "firmwareStatus", None) {
        return *resp;
    }
    let Ok(_permit) = tokio::time::timeout(
        Duration::from_millis(100),
        state.firmware_semaphore.acquire(),
    )
    .await
    else {
        return ctx.error_response(
            ApiError::service_unavailable("firmware_busy", "firmware control channel is busy")
                .with_header("Retry-After", "1"),
        );
    };
    match state.backend.firmware_status().await {
        Ok(body) => ctx.json_response(StatusCode::OK, &body, &[]),
        Err(err) => ctx.error_response(err),
    }
}

/// `GET /api/v1/firmware/check` (`checkFirmware`)
pub async fn firmware_check(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, _) = req.into_parts();
    let mut ctx = RequestCtx::new_with_state(&parts.method, &parts.headers, &state);
    let principal = match ctx.authorize_read_headers(&state, &parts.headers, "checkFirmware", None)
    {
        Ok(p) => p,
        Err(resp) => return *resp,
    };
    let query = match parse_strict_query(&parts.uri) {
        Ok(q) => q,
        Err(err) => {
            ctx.audit(&state, "checkFirmware", None, &err.code, Some(&principal));
            return ctx.error_response(err);
        }
    };
    let mut force = false;
    for (k, v) in query {
        if k != "force" {
            ctx.audit(
                &state,
                "checkFirmware",
                None,
                "invalid_request",
                Some(&principal),
            );
            return ctx.error_response(ApiError::bad_request(
                "unexpected query parameter on /firmware/check",
            ));
        }
        match v.as_str() {
            "true" => force = true,
            "false" => force = false,
            _ => {
                ctx.audit(
                    &state,
                    "checkFirmware",
                    None,
                    "invalid_request",
                    Some(&principal),
                );
                return ctx.error_response(ApiError::bad_request(
                    "force query parameter must be true or false",
                ));
            }
        }
    }
    if let Err(resp) = ctx.record_allowed(&state, "checkFirmware", None, &principal) {
        return *resp;
    }
    let Ok(_permit) = tokio::time::timeout(
        Duration::from_millis(100),
        state.firmware_semaphore.acquire(),
    )
    .await
    else {
        return ctx.error_response(
            ApiError::service_unavailable("firmware_busy", "firmware control channel is busy")
                .with_header("Retry-After", "1"),
        );
    };
    match state.backend.firmware_check(force).await {
        Ok(body) => ctx.json_response(StatusCode::OK, &body, &[]),
        Err(err) => ctx.error_response(err),
    }
}

/// `POST /api/v1/firmware/update` (`firmwareUpdate`)
pub async fn firmware_update(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    firmware_mutation(state, req, false).await
}

/// `POST /api/v1/firmware/rollback` (`rollbackFirmware`)
pub async fn firmware_rollback(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    firmware_mutation(state, req, true).await
}

async fn firmware_mutation(
    state: Arc<GatewayState>,
    req: Request<Body>,
    rollback: bool,
) -> Response<Body> {
    let op_id = if rollback {
        "rollbackFirmware"
    } else {
        "firmwareUpdate"
    };
    let (parts, body) = req.into_parts();
    let mut ctx = RequestCtx::new_with_state(&parts.method, &parts.headers, &state);
    let (principal, key) = match ctx.authorize_command_headers(&state, &parts.headers, op_id) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };
    let (json, canonical) = match read_strict_json_object_with_timeout(
        &parts.headers,
        body,
        state.config.request_body_timeout,
    )
    .await
    {
        Ok(v) => v,
        Err(err) => {
            ctx.audit(&state, op_id, None, &err.code, Some(&principal));
            return ctx.error_response(err);
        }
    };
    if let Err(err) = validate_firmware_commit_body(&json) {
        ctx.audit(&state, op_id, None, &err.code, Some(&principal));
        return ctx.error_response(err);
    }
    if let Err(resp) = ctx.record_allowed(&state, op_id, None, &principal) {
        return *resp;
    }
    let principal_id = principal.principal_id().to_owned();
    let cmd = IdempotentCommand {
        principal_id: principal_id.clone(),
        idempotency_key: key.clone(),
        body: json,
        canonical_body: canonical.clone(),
    };
    let state_exec = Arc::clone(&state);
    let res = state
        .idempotency_coalescer
        .execute(
            &principal_id,
            op_id,
            op_id,
            &key,
            &canonical,
            || async move {
                let Ok(_permit) = tokio::time::timeout(
                    Duration::from_millis(100),
                    state_exec.firmware_semaphore.acquire(),
                )
                .await
                else {
                    return Err(ApiError::service_unavailable(
                        "firmware_busy",
                        "firmware control channel is busy",
                    )
                    .with_header("Retry-After", "1"));
                };
                if rollback {
                    state_exec.backend.firmware_rollback(cmd).await
                } else {
                    state_exec.backend.firmware_update(cmd).await
                }
            },
        )
        .await;
    match res {
        Ok(outcome) => {
            let headers = if outcome.replayed {
                vec![("Idempotency-Replayed", "true")]
            } else {
                Vec::new()
            };
            let status = StatusCode::from_u16(outcome.status_code).unwrap_or(StatusCode::ACCEPTED);
            match outcome.body {
                Some(ref val) => ctx.json_response(status, val, &headers),
                None => ctx.empty_response(status, &headers),
            }
        }
        Err(err) => ctx.error_response(err),
    }
}

fn validate_firmware_commit_body(body: &Value) -> Result<(), ApiError> {
    let Some(obj) = body.as_object() else {
        return Err(ApiError::bad_request(
            "firmware commit body must be a JSON object",
        ));
    };
    if obj.len() != 1 {
        return Err(ApiError::bad_request(
            "firmware commit request must contain only commit",
        ));
    }
    let Some(commit) = obj.get("commit").and_then(Value::as_str) else {
        return Err(ApiError::bad_request("missing commit string"));
    };
    if !is_lower_hex_exact(commit, 40) {
        return Err(ApiError::bad_request(
            "commit must be a 40-character lowercase SHA-1 hex string",
        ));
    }
    Ok(())
}
