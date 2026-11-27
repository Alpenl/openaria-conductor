//! Modular Axum HTTP handlers for all 37 Miro-Ego v1 OpenAPI routes and static web assets.

pub mod capture;
pub mod device;
pub mod logs;
pub mod network;
pub mod sessions;

use crate::error::{ApiError, ApiResult};
use crate::middleware::RequestCtx;
use axum::body::Body;
use axum::http::header;
use axum::http::{Response, StatusCode, Uri};
use std::collections::HashMap;

/// Parse raw URI query parameters strictly: rejects duplicate keys and malformed percent-encoding.
pub fn parse_strict_query(uri: &Uri) -> ApiResult<HashMap<String, String>> {
    let mut map = HashMap::new();
    let Some(query) = uri.query() else {
        return Ok(map);
    };
    if query.is_empty() {
        return Ok(map);
    }
    for pair in query.split('&') {
        let (raw_key, raw_val) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        let key = percent_decode(raw_key)?;
        let val = percent_decode(raw_val)?;
        if key.is_empty() || map.contains_key(&key) {
            return Err(ApiError::bad_request(
                "duplicate or empty query parameter is not allowed",
            ));
        }
        map.insert(key, val);
    }
    Ok(map)
}

pub fn percent_decode(input: &str) -> ApiResult<String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' => {
                if i + 2 >= bytes.len() {
                    return Err(ApiError::bad_request("malformed percent-encoding in query"));
                }
                let hi = hex_nibble(bytes[i + 1])?;
                let lo = hex_nibble(bytes[i + 2])?;
                out.push((hi << 4) | lo);
                i += 3;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| ApiError::bad_request("query parameter is not valid UTF-8"))
}

fn hex_nibble(b: u8) -> ApiResult<u8> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(ApiError::bad_request(
            "invalid hex digit in percent-encoding",
        )),
    }
}

pub fn is_lower_hex_exact(value: &str, expected_len: usize) -> bool {
    value.len() == expected_len
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

const MAX_ECHOED_FIELD_CHARS: usize = 64;

/// Sanitize and truncate an untrusted field name or identifier before echoing it in an error message.
pub fn truncate_untrusted_field(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len().min(MAX_ECHOED_FIELD_CHARS + 3));
    for (idx, ch) in raw.chars().enumerate() {
        if idx >= MAX_ECHOED_FIELD_CHARS {
            out.push_str("...");
            break;
        }
        if ch.is_control() {
            out.push('?');
        } else {
            out.push(ch);
        }
    }
    out
}

/// Response headers metadata for streaming downloads (artifacts and diagnostic logs).
pub struct DownloadResponseMeta<'a> {
    pub status: StatusCode,
    pub content_type: &'a str,
    pub etag: &'a str,
    pub content_length: u64,
    pub content_range: Option<String>,
    pub content_disposition: Option<&'a str>,
}

/// Build an HTTP streaming download response (used by both artifacts and diagnostic logs).
pub fn build_download_response(
    ctx: &RequestCtx,
    meta: DownloadResponseMeta<'_>,
    body: Body,
) -> Response<Body> {
    let mut builder = Response::builder()
        .status(meta.status)
        .header(header::CONTENT_TYPE, meta.content_type)
        .header(header::CONTENT_LENGTH, meta.content_length.to_string())
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::ETAG, meta.etag)
        .header(header::CACHE_CONTROL, "no-store");
    if let Some(ref cr) = meta.content_range {
        builder = builder.header(header::CONTENT_RANGE, cr.as_str());
    }
    if let Some(cd) = meta.content_disposition {
        builder = builder.header(header::CONTENT_DISPOSITION, cd);
    }
    builder = ctx.apply_common_headers(builder, &[]);
    builder.body(body).unwrap_or_else(|_| {
        ctx.error_response(ApiError::internal(
            "response_build_failed",
            "failed to construct download response",
        ))
    })
}
