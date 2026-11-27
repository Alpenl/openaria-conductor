//! Capture status, readiness, safe-swap, timer, start/stop commands, camera focus,
//! capture SSE events, and zero-copy MJPEG / H.264 preview streaming handlers.

use crate::backend::IdempotentCommand;
use crate::error::ApiError;
use crate::handlers::{parse_strict_query, truncate_untrusted_field};
use crate::middleware::{
    GatewayState, HeaderExtract, RequestCtx, read_strict_json_object_with_timeout,
    single_header_str,
};
use crate::preview::{MJPEG_CONTENT_TYPE, h264_stream_body_with_gop, mjpeg_stream_body};
use crate::sse::{EventCursor, capture_sse_body, parse_last_event_id};
use axum::body::Body;
use axum::extract::State;
use axum::http::header;
use axum::http::{HeaderMap, Method, Request, Response, StatusCode};
use miro_ego_core::ids::{is_valid_uuid_v4, is_valid_uuid_v7};
use serde_json::Value;
use std::sync::Arc;

/// `GET /api/v1/capture/status` (`getCaptureStatus`)
pub async fn get_capture_status(
    State(state): State<Arc<GatewayState>>,
    method: Method,
    headers: HeaderMap,
) -> Response<Body> {
    let mut ctx = RequestCtx::new_with_state(&method, &headers, &state);
    if let Err(resp) = ctx.authorize_read(&state, &headers, "getCaptureStatus", None) {
        return *resp;
    }
    match state.backend.capture_status().await {
        Ok(body) => ctx.json_response(StatusCode::OK, &body, &[]),
        Err(err) => ctx.error_response(err),
    }
}

/// `GET /api/v1/capture/readiness` (`getCaptureReadiness`)
pub async fn get_capture_readiness(
    State(state): State<Arc<GatewayState>>,
    method: Method,
    headers: HeaderMap,
) -> Response<Body> {
    let mut ctx = RequestCtx::new_with_state(&method, &headers, &state);
    if let Err(resp) = ctx.authorize_read(&state, &headers, "getCaptureReadiness", None) {
        return *resp;
    }
    match state.backend.capture_readiness().await {
        Ok(body) => ctx.json_response(StatusCode::OK, &body, &[]),
        Err(err) => ctx.error_response(err),
    }
}

/// `GET /api/v1/capture/safe-swap` (`getCurrentSafeSwapReceipt`)
pub async fn get_safe_swap(
    State(state): State<Arc<GatewayState>>,
    method: Method,
    headers: HeaderMap,
) -> Response<Body> {
    let mut ctx = RequestCtx::new_with_state(&method, &headers, &state);
    if let Err(resp) = ctx.authorize_read(&state, &headers, "getCurrentSafeSwapReceipt", None) {
        return *resp;
    }
    match state.backend.capture_safe_swap().await {
        Ok(Some(body)) => ctx.json_response(StatusCode::OK, &body, &[]),
        Ok(None) => ctx.error_response(ApiError::not_found(
            "no current safe-swap handoff receipt exists",
        )),
        Err(err) => ctx.error_response(err),
    }
}

/// `GET /api/v1/capture/timer` (`getCaptureTimer`)
pub async fn get_capture_timer(
    State(state): State<Arc<GatewayState>>,
    method: Method,
    headers: HeaderMap,
) -> Response<Body> {
    let mut ctx = RequestCtx::new_with_state(&method, &headers, &state);
    if let Err(resp) = ctx.authorize_read(&state, &headers, "getCaptureTimer", None) {
        return *resp;
    }
    match state.backend.capture_timer().await {
        Ok(body) => ctx.json_response(StatusCode::OK, &body, &[]),
        Err(err) => ctx.error_response(err),
    }
}

/// `POST /api/v1/capture/timer` (`setCaptureTimer`)
pub async fn set_capture_timer(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, body) = req.into_parts();
    let mut ctx = RequestCtx::new_with_state(&parts.method, &parts.headers, &state);
    let principal = match ctx.authorize_write_headers(&state, &parts.headers, "setCaptureTimer") {
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
            ctx.audit(&state, "setCaptureTimer", None, &err.code, Some(&principal));
            return ctx.error_response(err);
        }
    };
    let (enabled, duration_seconds) = match validate_timer_set_body(&json) {
        Ok(v) => v,
        Err(err) => {
            ctx.audit(&state, "setCaptureTimer", None, &err.code, Some(&principal));
            return ctx.error_response(err);
        }
    };
    if let Err(resp) = ctx.record_allowed(&state, "setCaptureTimer", None, &principal) {
        return *resp;
    }
    match state
        .backend
        .set_capture_timer(enabled, duration_seconds)
        .await
    {
        Ok(resp_body) => ctx.json_response(StatusCode::OK, &resp_body, &[]),
        Err(err) => ctx.error_response(err),
    }
}

fn validate_timer_set_body(body: &Value) -> Result<(bool, u64), ApiError> {
    let Some(obj) = body.as_object() else {
        return Err(ApiError::bad_request("timer request must be a JSON object"));
    };
    if obj.len() != 3
        || obj.get("schema").and_then(Value::as_str) != Some("miro-ego.capture-timer-set.v1")
    {
        return Err(ApiError::bad_request(
            "timer request must have schema=miro-ego.capture-timer-set.v1, enabled, duration_seconds",
        ));
    }
    let Some(enabled) = obj.get("enabled").and_then(Value::as_bool) else {
        return Err(ApiError::bad_request("enabled must be a boolean"));
    };
    let Some(duration_seconds) = obj.get("duration_seconds").and_then(Value::as_u64) else {
        return Err(ApiError::bad_request(
            "duration_seconds must be an integer in 1..=86400",
        ));
    };
    if !(1..=86_400).contains(&duration_seconds) {
        return Err(ApiError::bad_request(
            "duration_seconds must be an integer in 1..=86400",
        ));
    }
    Ok((enabled, duration_seconds))
}

/// `POST /api/v1/capture/start` (`startCapture`)
pub async fn start_capture(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, body) = req.into_parts();
    let mut ctx = RequestCtx::new_with_state(&parts.method, &parts.headers, &state);
    let (principal, key) =
        match ctx.authorize_command_headers(&state, &parts.headers, "startCapture") {
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
            ctx.audit(&state, "startCapture", None, &err.code, Some(&principal));
            return ctx.error_response(err);
        }
    };
    if let Err(err) = validate_capture_start_body(&json) {
        ctx.audit(&state, "startCapture", None, &err.code, Some(&principal));
        return ctx.error_response(err);
    }
    if let Err(resp) = ctx.record_allowed(&state, "startCapture", None, &principal) {
        return *resp;
    }
    let principal_id = principal.principal_id().to_owned();
    let cmd = IdempotentCommand {
        principal_id: principal_id.clone(),
        idempotency_key: key.clone(),
        body: json,
        canonical_body: canonical.clone(),
    };
    let backend = Arc::clone(&state.backend);
    let res = state
        .idempotency_coalescer
        .execute(
            &principal_id,
            "startCapture",
            "startCapture",
            &key,
            &canonical,
            || async move { backend.start_capture(cmd).await },
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

fn validate_capture_start_body(body: &Value) -> Result<(), ApiError> {
    let Some(obj) = body.as_object() else {
        return Err(ApiError::bad_request(
            "capture start request must be a JSON object",
        ));
    };
    for key in obj.keys() {
        if !matches!(
            key.as_str(),
            "schema" | "mode" | "volume_id" | "display_name" | "take"
        ) {
            return Err(ApiError::bad_request(format!(
                "unknown field in capture start request: {}",
                truncate_untrusted_field(key)
            )));
        }
    }
    if obj.get("schema").and_then(Value::as_str) != Some("miro-ego.capture-start.v1") {
        return Err(ApiError::bad_request(
            "schema must be miro-ego.capture-start.v1",
        ));
    }
    if !matches!(
        obj.get("mode").and_then(Value::as_str),
        Some("production" | "calibration")
    ) {
        return Err(ApiError::bad_request(
            "mode must be production or calibration",
        ));
    }
    if let Some(vid) = obj.get("volume_id")
        && !vid.as_str().is_some_and(is_valid_uuid_v4)
    {
        return Err(ApiError::bad_request(
            "volume_id must be a canonical UUIDv4",
        ));
    }
    if let Some(name) = obj.get("display_name")
        && !name
            .as_str()
            .is_some_and(|s| !s.is_empty() && s.chars().count() <= 160)
    {
        return Err(ApiError::bad_request(
            "display_name must be 1..=160 characters",
        ));
    }
    let Some(take) = obj.get("take").and_then(Value::as_object) else {
        return Err(ApiError::bad_request("take must be an object"));
    };
    match take.get("kind").and_then(Value::as_str) {
        Some("new") if take.len() == 1 => Ok(()),
        Some("continue")
            if take.len() == 2
                && take
                    .get("continuation_of")
                    .and_then(Value::as_str)
                    .is_some_and(is_valid_uuid_v7) =>
        {
            Ok(())
        }
        _ => Err(ApiError::bad_request("invalid take specification")),
    }
}

/// `POST /api/v1/capture/stop` (`stopCapture`)
pub async fn stop_capture(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, body) = req.into_parts();
    let mut ctx = RequestCtx::new_with_state(&parts.method, &parts.headers, &state);
    let (principal, key) =
        match ctx.authorize_command_headers(&state, &parts.headers, "stopCapture") {
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
            ctx.audit(&state, "stopCapture", None, &err.code, Some(&principal));
            return ctx.error_response(err);
        }
    };
    if let Err(err) = validate_capture_stop_body(&json) {
        ctx.audit(&state, "stopCapture", None, &err.code, Some(&principal));
        return ctx.error_response(err);
    }
    if let Err(resp) = ctx.record_allowed(&state, "stopCapture", None, &principal) {
        return *resp;
    }
    let principal_id = principal.principal_id().to_owned();
    let cmd = IdempotentCommand {
        principal_id: principal_id.clone(),
        idempotency_key: key.clone(),
        body: json,
        canonical_body: canonical.clone(),
    };
    let backend = Arc::clone(&state.backend);
    let res = state
        .idempotency_coalescer
        .execute(
            &principal_id,
            "stopCapture",
            "stopCapture",
            &key,
            &canonical,
            || async move { backend.stop_capture(cmd).await },
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

fn validate_capture_stop_body(body: &Value) -> Result<(), ApiError> {
    let Some(obj) = body.as_object() else {
        return Err(ApiError::bad_request(
            "capture stop request must be a JSON object",
        ));
    };
    if obj.len() != 2
        || obj.get("schema").and_then(Value::as_str) != Some("miro-ego.capture-stop.v1")
        || !matches!(
            obj.get("reason").and_then(Value::as_str),
            Some("user" | "safe_swap")
        )
    {
        return Err(ApiError::bad_request(
            "capture stop request must have schema=miro-ego.capture-stop.v1 and reason in {user, safe_swap}",
        ));
    }
    Ok(())
}

/// `GET /api/v1/camera/focus` (`getCameraFocus`)
pub async fn get_camera_focus(
    State(state): State<Arc<GatewayState>>,
    method: Method,
    headers: HeaderMap,
) -> Response<Body> {
    let mut ctx = RequestCtx::new_with_state(&method, &headers, &state);
    if let Err(resp) = ctx.authorize_read(&state, &headers, "getCameraFocus", None) {
        return *resp;
    }
    match state.backend.camera_focus().await {
        Ok(Some(body)) => ctx.json_response(StatusCode::OK, &body, &[]),
        Ok(None) => ctx.error_response(ApiError::camera_focus_unsupported()),
        Err(err) => ctx.error_response(err),
    }
}

/// `POST /api/v1/camera/focus` (`setCameraFocus`)
pub async fn set_camera_focus(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, body) = req.into_parts();
    let mut ctx = RequestCtx::new_with_state(&parts.method, &parts.headers, &state);
    let (principal, key) =
        match ctx.authorize_command_headers(&state, &parts.headers, "setCameraFocus") {
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
            ctx.audit(&state, "setCameraFocus", None, &err.code, Some(&principal));
            return ctx.error_response(err);
        }
    };
    if let Err(err) = validate_camera_focus_body(&json) {
        ctx.audit(&state, "setCameraFocus", None, &err.code, Some(&principal));
        return ctx.error_response(err);
    }
    if let Err(resp) = ctx.record_allowed(&state, "setCameraFocus", None, &principal) {
        return *resp;
    }
    let principal_id = principal.principal_id().to_owned();
    let cmd = IdempotentCommand {
        principal_id: principal_id.clone(),
        idempotency_key: key.clone(),
        body: json,
        canonical_body: canonical.clone(),
    };
    let backend = Arc::clone(&state.backend);
    let res = state
        .idempotency_coalescer
        .execute(
            &principal_id,
            "setCameraFocus",
            "setCameraFocus",
            &key,
            &canonical,
            || async move { backend.set_camera_focus(cmd).await },
        )
        .await;
    match res {
        Ok(outcome) => {
            let headers = if outcome.replayed {
                vec![("Idempotency-Replayed", "true")]
            } else {
                Vec::new()
            };
            let status = StatusCode::from_u16(outcome.status_code).unwrap_or(StatusCode::OK);
            match outcome.body {
                Some(ref val) => ctx.json_response(status, val, &headers),
                None => ctx.empty_response(status, &headers),
            }
        }
        Err(err) => ctx.error_response(err),
    }
}

fn validate_camera_focus_body(body: &Value) -> Result<(), ApiError> {
    let Some(obj) = body.as_object() else {
        return Err(ApiError::bad_request(
            "camera focus request must be a JSON object",
        ));
    };
    for key in obj.keys() {
        if !matches!(key.as_str(), "schema" | "value" | "auto_enabled") {
            return Err(ApiError::bad_request(format!(
                "unknown field in camera focus request: {}",
                truncate_untrusted_field(key)
            )));
        }
    }
    if obj.get("schema").and_then(Value::as_str) != Some("miro-ego.camera-focus-set.v1") {
        return Err(ApiError::bad_request(
            "schema must be miro-ego.camera-focus-set.v1",
        ));
    }
    let has_value = obj.contains_key("value");
    let has_auto = obj.contains_key("auto_enabled");
    if !has_value && !has_auto {
        return Err(ApiError::bad_request(
            "camera focus request must include value or auto_enabled",
        ));
    }
    if has_value && obj.get("value").and_then(Value::as_u64).is_none() {
        return Err(ApiError::bad_request(
            "focus value must be a non-negative integer",
        ));
    }
    if has_auto && obj.get("auto_enabled").and_then(Value::as_bool).is_none() {
        return Err(ApiError::bad_request("auto_enabled must be a boolean"));
    }
    Ok(())
}

/// `GET /api/v1/capture/events` (`streamCaptureEvents`)
pub async fn stream_capture_events(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, _) = req.into_parts();
    let mut ctx = RequestCtx::new_with_state(&parts.method, &parts.headers, &state);
    let principal =
        match ctx.authorize_read_headers(&state, &parts.headers, "streamCaptureEvents", None) {
            Ok(p) => p,
            Err(resp) => return *resp,
        };
    if parts.uri.query().is_some_and(|q| !q.is_empty()) {
        ctx.audit(
            &state,
            "streamCaptureEvents",
            None,
            "invalid_request",
            Some(&principal),
        );
        return ctx.error_response(ApiError::bad_request(
            "SSE endpoint does not accept query parameters",
        ));
    }
    let cursor = match single_header_str(&parts.headers, "last-event-id") {
        HeaderExtract::Multiple => {
            ctx.audit(
                &state,
                "streamCaptureEvents",
                None,
                "invalid_request",
                Some(&principal),
            );
            return ctx.error_response(ApiError::bad_request("Last-Event-ID may appear only once"));
        }
        HeaderExtract::Missing => None,
        HeaderExtract::Single(raw) => match parse_last_event_id(Some(raw)) {
            Ok(c) => c,
            Err(err) => {
                ctx.audit(
                    &state,
                    "streamCaptureEvents",
                    None,
                    &err.code,
                    Some(&principal),
                );
                return ctx.error_response(err);
            }
        },
    };
    if let Err(resp) = ctx.record_allowed(&state, "streamCaptureEvents", None, &principal) {
        return *resp;
    }
    let Ok(permit) = Arc::clone(&state.sse_semaphore).try_acquire_owned() else {
        return ctx.error_response(ApiError::stream_capacity_exhausted());
    };

    // Subscribe BEFORE replay_or_snapshot_async so concurrent publishes never miss a broadcast wakeup,
    // and evaluate capture_snapshot_event() INSIDE the publish_count loop to prevent stale anchors.
    let buffer = state.backend.capture_event_buffer();
    let sub = buffer.subscribe();

    let backend_for_snap = Arc::clone(&state.backend);
    let initial_events = match buffer
        .replay_or_snapshot_async(cursor, || {
            let b = Arc::clone(&backend_for_snap);
            async move { b.capture_snapshot_event().await }
        })
        .await
    {
        Ok(evs) => evs,
        Err(err) => return ctx.error_response(err),
    };
    let initial_cursor = initial_events
        .last()
        .map(|ev| ev.delivery_id)
        .or(match cursor {
            Some(EventCursor::Exact(id)) => Some(id),
            _ => None,
        })
        .unwrap_or(0);

    let body = capture_sse_body(
        buffer,
        sub,
        Arc::clone(&state.backend),
        initial_events,
        initial_cursor,
        state.config.sse_heartbeat_interval,
        permit,
        state.subscribe_shutdown(),
    );

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header("X-Accel-Buffering", "no");
    builder = ctx.apply_common_headers(builder, &[]);
    builder.body(body).unwrap_or_else(|_| {
        ctx.error_response(ApiError::internal(
            "response_build_failed",
            "failed to build SSE response",
        ))
    })
}

/// `GET /api/v1/preview` (`getPreview`)
pub async fn get_preview(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, _) = req.into_parts();
    let mut ctx = RequestCtx::new_with_state(&parts.method, &parts.headers, &state);
    let principal = match ctx.authorize_read_headers(&state, &parts.headers, "getPreview", None) {
        Ok(p) => p,
        Err(resp) => return *resp,
    };
    let query = match parse_strict_query(&parts.uri) {
        Ok(q) => q,
        Err(err) => {
            ctx.audit(&state, "getPreview", None, &err.code, Some(&principal));
            return ctx.error_response(err);
        }
    };
    let mut fps: Option<u32> = None;
    for (k, v) in query {
        if k != "fps" {
            ctx.audit(
                &state,
                "getPreview",
                None,
                "invalid_request",
                Some(&principal),
            );
            return ctx.error_response(ApiError::bad_request(
                "unexpected query parameter on /preview",
            ));
        }
        let parsed = v.parse::<u32>().ok().filter(|&n| n >= 1);
        let Some(valid_fps) = parsed else {
            ctx.audit(
                &state,
                "getPreview",
                None,
                "invalid_request",
                Some(&principal),
            );
            return ctx.error_response(ApiError::bad_request("fps must be a positive integer"));
        };
        fps = Some(valid_fps);
    }
    if let Err(resp) = ctx.record_allowed(&state, "getPreview", None, &principal) {
        return *resp;
    }

    // Acquire preview semaphore BEFORE calling `open_preview_mjpeg` so capacity-exhausted
    // requests never invoke backend preview initialization.
    let Ok(permit) = Arc::clone(&state.preview_semaphore).try_acquire_owned() else {
        return ctx.error_response(ApiError::preview_capacity_exhausted());
    };

    let hub = match state.backend.open_preview_mjpeg(fps).await {
        Ok(h) => h,
        Err(err) => return ctx.error_response(err),
    };
    let Some(latest) = hub.latest() else {
        return ctx.error_response(ApiError::preview_unavailable(
            "no preview frame available yet",
        ));
    };

    let wants_multipart = parts
        .headers
        .get_all(header::ACCEPT)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .any(|s| s.to_ascii_lowercase().contains("multipart/x-mixed-replace"));

    if !wants_multipart {
        drop(permit);
        let frame_bytes = (*latest.jpeg).clone();
        let len = frame_bytes.len();
        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "image/jpeg")
            .header(header::CONTENT_LENGTH, len.to_string())
            .header(header::CACHE_CONTROL, "no-store");
        builder = ctx.apply_common_headers(builder, &[]);
        return builder.body(Body::from(frame_bytes)).unwrap_or_else(|_| {
            ctx.error_response(ApiError::internal(
                "response_build_failed",
                "failed to build JPEG preview response",
            ))
        });
    }

    let body = mjpeg_stream_body(hub, fps, latest, permit, state.subscribe_shutdown());
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, MJPEG_CONTENT_TYPE)
        .header(header::CACHE_CONTROL, "no-store")
        .header("X-Accel-Buffering", "no");
    builder = ctx.apply_common_headers(builder, &[]);
    builder.body(body).unwrap_or_else(|_| {
        ctx.error_response(ApiError::internal(
            "response_build_failed",
            "failed to build MJPEG response",
        ))
    })
}

/// `GET /api/v1/preview/h264` (`getPreviewH264`)
pub async fn get_preview_h264(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, _) = req.into_parts();
    let mut ctx = RequestCtx::new_with_state(&parts.method, &parts.headers, &state);
    let principal = match ctx.authorize_read_headers(&state, &parts.headers, "getPreviewH264", None)
    {
        Ok(p) => p,
        Err(resp) => return *resp,
    };
    let query = match parse_strict_query(&parts.uri) {
        Ok(q) => q,
        Err(err) => {
            ctx.audit(&state, "getPreviewH264", None, &err.code, Some(&principal));
            return ctx.error_response(err);
        }
    };
    let mut fps: Option<u32> = None;
    let mut bitrate_kbps: Option<u32> = None;
    for (k, v) in query {
        match k.as_str() {
            "fps" => {
                let Some(parsed) = v.parse::<u32>().ok().filter(|n| (1..=60).contains(n)) else {
                    ctx.audit(
                        &state,
                        "getPreviewH264",
                        None,
                        "invalid_request",
                        Some(&principal),
                    );
                    return ctx
                        .error_response(ApiError::bad_request("fps must be an integer in 1..=60"));
                };
                fps = Some(parsed);
            }
            "bitrate_kbps" => {
                let Some(parsed) = v.parse::<u32>().ok().filter(|n| (256..=16_000).contains(n))
                else {
                    ctx.audit(
                        &state,
                        "getPreviewH264",
                        None,
                        "invalid_request",
                        Some(&principal),
                    );
                    return ctx.error_response(ApiError::bad_request(
                        "bitrate_kbps must be an integer in 256..=16000",
                    ));
                };
                bitrate_kbps = Some(parsed);
            }
            _ => {
                ctx.audit(
                    &state,
                    "getPreviewH264",
                    None,
                    "invalid_request",
                    Some(&principal),
                );
                return ctx.error_response(ApiError::bad_request(
                    "unexpected query parameter on /preview/h264",
                ));
            }
        }
    }
    if let Err(resp) = ctx.record_allowed(&state, "getPreviewH264", None, &principal) {
        return *resp;
    }

    // Acquire preview semaphore BEFORE calling `open_preview_h264`.
    let Ok(permit) = Arc::clone(&state.preview_semaphore).try_acquire_owned() else {
        return ctx.error_response(ApiError::preview_capacity_exhausted());
    };

    let hub = match state.backend.open_preview_h264(fps, bitrate_kbps).await {
        Ok(h) => h,
        Err(err) => return ctx.error_response(err),
    };

    let (gop_fragments, rx) = hub.subscribe_with_gop();
    let Some(init) = hub.init_segment().await else {
        return ctx.error_response(ApiError::preview_unavailable(
            "H.264 preview initialization segment is not available yet",
        ));
    };
    hub.record_stream_hints(fps, bitrate_kbps);
    let content_type = hub.content_type().to_owned();
    let body = h264_stream_body_with_gop(
        Some(init),
        gop_fragments,
        rx,
        permit,
        state.subscribe_shutdown(),
    );

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, "no-store")
        .header("X-Accel-Buffering", "no");
    builder = ctx.apply_common_headers(builder, &[]);
    builder.body(body).unwrap_or_else(|_| {
        ctx.error_response(ApiError::internal(
            "response_build_failed",
            "failed to build H.264 preview response",
        ))
    })
}
