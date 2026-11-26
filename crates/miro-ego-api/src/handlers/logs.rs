//! Diagnostic log listing (`GET /api/v1/logs`) and traversal-proof Range streaming (`GET /api/v1/logs/{name}`).

use crate::backend::ListLogsQuery;
use crate::downloads::{resolve_range_selection, stream_download_body};
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
    Path(decoded_name): Path<String>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, _) = req.into_parts();
    let mut ctx = RequestCtx::new(&parts.method, &parts.headers, &state.security);

    // Authenticate BEFORE inspecting path syntax so unauthenticated Customer callers fail closed with 401.
    let principal = match ctx.authenticate_for_operation(&state, &parts.headers, "getLogFile") {
        Ok(p) => p,
        Err(resp) => return *resp,
    };

    // Inspect the raw URI path as well as decoded path to block `%2e`, `%2f`, `%5c`, `..`, etc.
    let raw_suffix = parts
        .uri
        .path()
        .strip_prefix("/api/v1/logs/")
        .or_else(|| parts.uri.path().strip_prefix("/logs/"));
    let valid_raw = raw_suffix.is_some_and(|s| !s.contains('%') && is_valid_log_name(s));
    if !valid_raw || !is_valid_log_name(&decoded_name) {
        return ctx.error_response(ApiError::bad_request(
            "invalid diagnostic log path; traversal, percent-encoding, and hidden files are forbidden",
        ));
    }

    if let Err(resp) =
        ctx.authorize_resource_read(&state, &principal, "getLogFile", Some(&decoded_name))
    {
        return *resp;
    }

    let Ok(permit) = Arc::clone(&state.download_semaphore).try_acquire_owned() else {
        return ctx.error_response(ApiError::download_capacity_exhausted());
    };

    let representation = match state.backend.open_log_file(&decoded_name).await {
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
    let body = stream_download_body(
        representation.source,
        resolved.offset,
        resolved.length,
        state.config.download_chunk_bytes,
        permit,
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
