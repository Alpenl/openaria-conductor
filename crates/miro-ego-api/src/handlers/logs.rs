//! Diagnostic log listing (`GET /api/v1/logs`) and traversal-proof Range streaming (`GET /api/v1/logs/{name}`).

use crate::backend::ListLogsQuery;
use crate::downloads::{resolve_range_selection, stream_download_body_with_guard};
use crate::error::ApiError;
use crate::handlers::{DownloadResponseMeta, build_download_response, parse_strict_query};
use crate::middleware::{GatewayState, HeaderExtract, RequestCtx, single_header};
use crate::range::{is_valid_log_name, is_valid_log_prefix};
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::header;
use axum::http::{Request, Response, StatusCode};
use std::sync::Arc;

/// `GET /api/v1/logs` (`listLogs`)
pub async fn list_logs(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, _) = req.into_parts();
    let mut ctx = RequestCtx::new(&parts.method, &parts.headers, &state.security);
    if let Err(resp) = ctx.authorize_read(&state, &parts.headers, "listLogs", None) {
        return *resp;
    }
    let query = match parse_strict_query(&parts.uri) {
        Ok(q) => q,
        Err(err) => return ctx.error_response(err),
    };
    let mut prefix: Option<String> = None;
    let mut limit: usize = 200;
    for (k, v) in query {
        match k.as_str() {
            "prefix" => {
                if !is_valid_log_prefix(&v) {
                    return ctx.error_response(ApiError::bad_request(
                        "invalid diagnostic log prefix filter",
                    ));
                }
                prefix = Some(v);
            }
            "limit" => {
                let parsed = v.parse::<usize>().ok().filter(|&n| (1..=500).contains(&n));
                let Some(n) = parsed else {
                    return ctx.error_response(ApiError::bad_request(
                        "limit must be an integer in 1..=500",
                    ));
                };
                limit = n;
            }
            _ => {
                return ctx
                    .error_response(ApiError::bad_request("unexpected query parameter on /logs"));
            }
        }
    }
    match state
        .backend
        .list_logs(ListLogsQuery { prefix, limit })
        .await
    {
        Ok(body) => ctx.json_response(StatusCode::OK, &body, &[]),
        Err(err) => ctx.error_response(err),
    }
}

/// `GET /api/v1/logs/{*name}` (`getLogFile`)
pub async fn get_log_file(
    State(state): State<Arc<GatewayState>>,
    path_res: Result<Path<String>, axum::extract::rejection::PathRejection>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, _) = req.into_parts();
    let mut ctx = RequestCtx::new_with_state(&parts.method, &parts.headers, &state);

    // Authenticate BEFORE inspecting path syntax or UTF-8 decoding so unauthenticated Customer callers fail closed with 401.
    let principal = match ctx.authenticate_for_operation(&state, &parts.headers, "getLogFile") {
        Ok(p) => p,
        Err(resp) => return *resp,
    };

    let Ok(Path(decoded_name)) = path_res else {
        ctx.audit(
            &state,
            "getLogFile",
            None,
            "invalid_request",
            Some(&principal),
        );
        return ctx.error_response(ApiError::bad_request(
            "invalid diagnostic log path; traversal, percent-encoding, and hidden files are forbidden",
        ));
    };

    // Allow `%2F`/`%2f` segment separators per OpenAPI path encoding while strictly blocking
    // all other `%`-escapes (`%2e`, `%5c`, `%00`, `%25`, etc.), `..`, dotfiles, and empty segments.
    let raw_suffix = parts
        .uri
        .path()
        .strip_prefix("/api/v1/logs/")
        .or_else(|| parts.uri.path().strip_prefix("/logs/"));
    let normalized_raw = raw_suffix.and_then(decode_log_path_separators_only);
    let decoded_normalized = decode_log_path_separators_only(&decoded_name);
    let Some(canonical_name) = normalized_raw.filter(|norm| {
        is_valid_log_name(norm) && decoded_normalized.as_deref() == Some(norm.as_str())
    }) else {
        ctx.audit(
            &state,
            "getLogFile",
            None,
            "invalid_request",
            Some(&principal),
        );
        return ctx.error_response(ApiError::bad_request(
            "invalid diagnostic log path; traversal, percent-encoding, and hidden files are forbidden",
        ));
    };

    if let Err(resp) =
        ctx.authorize_resource_read(&state, &principal, "getLogFile", Some(&canonical_name))
    {
        return *resp;
    }

    let Ok(permit) = Arc::clone(&state.download_semaphore).try_acquire_owned() else {
        return ctx.error_response(ApiError::download_capacity_exhausted());
    };

    let representation = match state.backend.open_log_file(&canonical_name).await {
        Ok(repr) => repr,
        Err(err) => return ctx.error_response(err),
    };

    let (range_header, range_is_single) = match single_header(&parts.headers, header::RANGE) {
        HeaderExtract::Missing => (None, true),
        HeaderExtract::Single(v) => (Some(v), true),
        HeaderExtract::Multiple => (None, false),
    };
    let (if_range_header, if_range_is_single) =
        match single_header(&parts.headers, header::IF_RANGE) {
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
        false,
    ) {
        Ok(r) => r,
        Err(err) => return ctx.error_response(err),
    };

    let status = StatusCode::from_u16(resolved.status).unwrap_or(StatusCode::OK);
    let content_range = resolved
        .content_range
        .map(|r| r.content_range_header(representation.size));
    let body = stream_download_body_with_guard(
        representation.source,
        resolved.offset,
        resolved.length,
        state.config.download_chunk_bytes,
        permit,
        representation.lease_guard,
    );

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

/// Normalize `%2F` / `%2f` path segment separators to `/` in a single left-to-right pass while
/// rejecting any other `%` byte (e.g. `%2e`, `%5c`, `%00`, `%25`, `%252f`).
fn decode_log_path_separators_only(raw: &str) -> Option<String> {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 < bytes.len() && bytes[i + 1] == b'2' && matches!(bytes[i + 2], b'f' | b'F') {
                out.push(b'/');
                i += 3;
                continue;
            }
            return None;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).ok()
}
