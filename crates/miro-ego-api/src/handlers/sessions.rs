//! Sealed session listing, exact raw manifest delivery, retained unsuccessful outcomes,
//! verified artifact Range/HEAD streaming with page-cache eviction, and idempotent deletion.

use crate::backend::{IdempotentCommand, ListSessionsQuery};
use crate::downloads::{resolve_range_selection, stream_download_body};
use crate::error::{ApiError, HEADER_MANIFEST_SHA256};
use crate::handlers::{DownloadResponseMeta, build_download_response, parse_strict_query};
use crate::middleware::{
    GatewayState, HeaderExtract, RequestCtx, read_strict_json_object, single_header,
};
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::header;
use axum::http::{HeaderMap, Method, Request, Response, StatusCode};
use miro_ego_core::ids::{is_valid_sha256, is_valid_uuid_v7};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;

/// `GET /api/v1/sessions` (`listSessions`)
pub async fn list_sessions(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, _) = req.into_parts();
    let mut ctx = RequestCtx::new(&parts.method, &parts.headers, &state.security);
    if let Err(resp) = ctx.authorize_read(&state, &parts.headers, "listSessions", None) {
        return *resp;
    }
    let query = match parse_strict_query(&parts.uri) {
        Ok(q) => q,
        Err(err) => return ctx.error_response(err),
    };
    let mut cursor: Option<String> = None;
    let mut limit: usize = 50;
    let mut take_id: Option<String> = None;

    for (k, v) in query {
        match k.as_str() {
            "cursor" => {
                if v.is_empty() {
                    return ctx.error_response(ApiError::bad_request("cursor must not be empty"));
                }
                cursor = Some(v);
            }
            "limit" => {
                let parsed = v.parse::<usize>().ok().filter(|&n| (1..=200).contains(&n));
                let Some(n) = parsed else {
                    return ctx.error_response(ApiError::bad_request(
                        "limit must be an integer in 1..=200",
                    ));
                };
                limit = n;
            }
            "take_id" => {
                if !is_valid_uuid_v7(&v) {
                    return ctx.error_response(ApiError::bad_request("take_id must be a UUIDv7"));
                }
                take_id = Some(v);
            }
            _ => {
                return ctx.error_response(ApiError::bad_request(
                    "unexpected query parameter on /sessions",
                ));
            }
        }
    }

    let include_recovery = parts
        .headers
        .get_all("prefer")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(','))
        .any(|tok| tok.trim().eq_ignore_ascii_case("include-recovery"));

    let req_query = ListSessionsQuery {
        cursor,
        limit,
        take_id,
        include_recovery,
    };
    match state.backend.list_sessions(req_query).await {
        Ok(body) => ctx.json_response(StatusCode::OK, &body, &[]),
        Err(err) => ctx.error_response(err),
    }
}

/// `GET /api/v1/sessions/{session_id}` (`getSession`)
pub async fn get_session_manifest(
    State(state): State<Arc<GatewayState>>,
    Path(session_id): Path<String>,
    method: Method,
    headers: HeaderMap,
) -> Response<Body> {
    let mut ctx = RequestCtx::new(&method, &headers, &state.security);
    let principal = match ctx.authenticate_for_operation(&state, &headers, "getSession") {
        Ok(p) => p,
        Err(resp) => return *resp,
    };
    if !is_valid_uuid_v7(&session_id) {
        return ctx.error_response(ApiError::not_found("session not found"));
    }
    if let Err(resp) =
        ctx.authorize_resource_read(&state, &principal, "getSession", Some(&session_id))
    {
        return *resp;
    }
    let Ok(_permit) = Arc::clone(&state.download_semaphore).try_acquire_owned() else {
        return ctx.error_response(ApiError::download_capacity_exhausted());
    };
    let manifest = match state.backend.open_session_manifest(&session_id).await {
        Ok(m) => m,
        Err(err) => return ctx.error_response(err),
    };
    let etag = manifest.etag();
    let len = manifest.bytes.len();
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CONTENT_LENGTH, len.to_string())
        .header(header::ETAG, etag.as_str())
        .header(HEADER_MANIFEST_SHA256, manifest.sha256.as_str())
        .header(header::CACHE_CONTROL, "no-store");
    builder = ctx.apply_common_headers(builder, &[]);
    builder
        .body(Body::from(manifest.bytes))
        .unwrap_or_else(|_| {
            ctx.error_response(ApiError::internal(
                "response_build_failed",
                "failed to build manifest response",
            ))
        })
}

/// `GET /api/v1/sessions/{session_id}/unsuccessful-outcome` (`getRetainedUnsuccessfulSessionOutcome`)
pub async fn get_unsuccessful_outcome(
    State(state): State<Arc<GatewayState>>,
    Path(session_id): Path<String>,
    method: Method,
    headers: HeaderMap,
) -> Response<Body> {
    let mut ctx = RequestCtx::new(&method, &headers, &state.security);
    let principal = match ctx.authenticate_for_operation(
        &state,
        &headers,
        "getRetainedUnsuccessfulSessionOutcome",
    ) {
        Ok(p) => p,
        Err(resp) => return *resp,
    };
    if !is_valid_uuid_v7(&session_id) {
        return ctx.error_response(ApiError::not_found("session not found"));
    }
    if let Err(resp) = ctx.authorize_resource_read(
        &state,
        &principal,
        "getRetainedUnsuccessfulSessionOutcome",
        Some(&session_id),
    ) {
        return *resp;
    }
    match state
        .backend
        .retained_unsuccessful_outcome(&session_id)
        .await
    {
        Ok(Some(body)) => ctx.json_response(StatusCode::OK, &body, &[]),
        Ok(None) => ctx.error_response(ApiError::not_found(
            "no retained unsuccessful terminal outcome for this session",
        )),
        Err(err) => ctx.error_response(err),
    }
}

/// `GET` / `HEAD /api/v1/sessions/{session_id}/artifacts/{artifact_id}`
/// (`getSessionArtifact` / `headSessionArtifact`)
pub async fn session_artifact(
    State(state): State<Arc<GatewayState>>,
    Path((session_id, artifact_id)): Path<(String, String)>,
    method: Method,
    headers: HeaderMap,
) -> Response<Body> {
    let head = method == Method::HEAD;
    let op_id = if head {
        "headSessionArtifact"
    } else {
        "getSessionArtifact"
    };
    let mut ctx = RequestCtx::new(&method, &headers, &state.security);
    let principal = match ctx.authenticate_for_operation(&state, &headers, op_id) {
        Ok(p) => p,
        Err(resp) => return *resp,
    };
    if !is_valid_uuid_v7(&session_id) || !is_valid_sha256(&artifact_id) {
        return ctx.error_response(ApiError::not_found("artifact not found"));
    }
    if let Err(resp) = ctx.authorize_resource_read(&state, &principal, op_id, Some(&session_id)) {
        return *resp;
    }

    // Admission Gate 1 (evaluated BEFORE download slots, verification, or Range): capture busy -> 423.
    match state.backend.artifact_io_busy_state().await {
        Ok(Some(busy_state)) => {
            return ctx.error_response(ApiError::capture_busy(&busy_state, 1));
        }
        Ok(None) => {}
        Err(err) => return ctx.error_response(err),
    }

    // Bounded concurrent download semaphore.
    let Ok(permit) = Arc::clone(&state.download_semaphore).try_acquire_owned() else {
        return ctx.error_response(ApiError::download_capacity_exhausted());
    };

    // Admission Gate 2: open verified artifact (unverified -> 409 session_not_verified).
    let representation = match state
        .backend
        .open_session_artifact(&session_id, &artifact_id)
        .await
    {
        Ok(repr) => repr,
        Err(err) => return ctx.error_response(err),
    };

    let (range_header, range_is_single) = match single_header(&headers, header::RANGE) {
        HeaderExtract::Missing => (None, true),
        HeaderExtract::Single(v) => (Some(v), true),
        HeaderExtract::Multiple => (None, false),
    };
    let (if_range_header, if_range_is_single) = match single_header(&headers, header::IF_RANGE) {
        HeaderExtract::Missing => (None, true),
        HeaderExtract::Single(v) => (Some(v), true),
        HeaderExtract::Multiple => (None, false),
    };

    let resolved = match resolve_range_selection(
        range_header,
        range_is_single,
        if_range_header,
        if_range_is_single,
        &representation,
        head,
    ) {
        Ok(r) => r,
        Err(err) => return ctx.error_response(err),
    };

    let status = StatusCode::from_u16(resolved.status).unwrap_or(StatusCode::OK);
    let content_range = resolved
        .content_range
        .map(|r| r.content_range_header(representation.size));
    let body = if head {
        drop(permit);
        Body::empty()
    } else {
        stream_download_body(
            representation.source,
            resolved.offset,
            resolved.length,
            state.config.download_chunk_bytes,
            permit,
        )
    };

    build_download_response(
        &ctx,
        DownloadResponseMeta {
            status,
            content_type: &representation.content_type,
            etag: &representation.etag,
            content_length: resolved.length,
            content_range,
            content_disposition: representation.content_disposition.as_deref(),
        },
        body,
    )
}

/// `POST /api/v1/sessions/delete` (`deleteSessions`)
pub async fn delete_sessions(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, body) = req.into_parts();
    let mut ctx = RequestCtx::new(&parts.method, &parts.headers, &state.security);
    let (principal, key) = match ctx.authorize_command(&state, &parts.headers, "deleteSessions") {
        Ok(v) => v,
        Err(resp) => return *resp,
    };
    let (json, canonical) = match read_strict_json_object(&parts.headers, body).await {
        Ok(v) => v,
        Err(err) => return ctx.error_response(err),
    };
    if let Err(err) = validate_delete_sessions_body(&json) {
        return ctx.error_response(err);
    }
    let cmd = IdempotentCommand {
        principal_id: principal.principal_id().to_owned(),
        idempotency_key: key,
        body: json,
        canonical_body: canonical,
    };
    match state.backend.delete_sessions(cmd).await {
        Ok(outcome) => {
            let _ = ctx.audit(
                &state,
                "deleteSessions",
                None,
                "completed",
                Some(&principal),
            );
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

fn validate_delete_sessions_body(body: &Value) -> Result<(), ApiError> {
    let Some(obj) = body.as_object() else {
        return Err(ApiError::bad_request(
            "delete request must be a JSON object",
        ));
    };
    if obj.len() != 2
        || obj.get("schema").and_then(Value::as_str) != Some("miro-ego.session-delete-request.v1")
    {
        return Err(ApiError::bad_request(
            "delete request must have schema=miro-ego.session-delete-request.v1 and sessions",
        ));
    }
    let Some(items) = obj.get("sessions").and_then(Value::as_array) else {
        return Err(ApiError::bad_request("sessions must be a non-empty array"));
    };
    if items.is_empty() || items.len() > 200 {
        return Err(ApiError::bad_request(
            "sessions array must contain 1..=200 entries",
        ));
    }
    let mut seen = HashSet::with_capacity(items.len());
    for item in items {
        let Some(entry) = item.as_object() else {
            return Err(ApiError::bad_request(
                "each session deletion item must be an object",
            ));
        };
        if entry.len() != 2 {
            return Err(ApiError::bad_request(
                "each session item must contain only session_id and manifest_sha256",
            ));
        }
        let Some(sid) = entry.get("session_id").and_then(Value::as_str) else {
            return Err(ApiError::bad_request("missing session_id"));
        };
        let Some(digest) = entry.get("manifest_sha256").and_then(Value::as_str) else {
            return Err(ApiError::bad_request("missing manifest_sha256"));
        };
        if !is_valid_uuid_v7(sid) || !is_valid_sha256(digest) || !seen.insert(sid.to_owned()) {
            return Err(ApiError::bad_request(
                "duplicate or invalid session_id / manifest_sha256",
            ));
        }
    }
    Ok(())
}
