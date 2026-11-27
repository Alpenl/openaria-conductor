//! Network status, Wi-Fi scan, SSE event stream, opaque credential exchange,
//! and idempotent async network mutations (`/apply`, `/retry`, `/forget`).

use crate::backend::IdempotentCommand;
use crate::error::ApiError;
use crate::handlers::truncate_untrusted_field;
use crate::middleware::{
    GatewayState, HeaderExtract, RequestCtx, read_strict_json_object_with_timeout,
    single_header_str,
};
use crate::sse::{EventCursor, network_sse_body, parse_last_event_id};
use axum::body::Body;
use axum::extract::State;
use axum::http::header;
use axum::http::{HeaderMap, Method, Request, Response, StatusCode};
use miro_ego_core::ids::is_valid_uuid_v7;
use serde_json::Value;
use std::net::Ipv4Addr;
use std::sync::Arc;

/// `GET /api/v1/network` (`getNetworkStatus`)
pub async fn get_network_status(
    State(state): State<Arc<GatewayState>>,
    method: Method,
    headers: HeaderMap,
) -> Response<Body> {
    let mut ctx = RequestCtx::new(&method, &headers, &state.security);
    if let Err(resp) = ctx.authorize_read(&state, &headers, "getNetworkStatus", None) {
        return *resp;
    }
    match state.backend.network_status().await {
        Ok(body) => ctx.json_response(StatusCode::OK, &body, &[]),
        Err(err) => ctx.error_response(err),
    }
}

/// `GET /api/v1/network/scan` (`scanNetworks`)
pub async fn scan_networks(
    State(state): State<Arc<GatewayState>>,
    method: Method,
    headers: HeaderMap,
) -> Response<Body> {
    let mut ctx = RequestCtx::new(&method, &headers, &state.security);
    if let Err(resp) = ctx.authorize_read(&state, &headers, "scanNetworks", None) {
        return *resp;
    }
    match state.backend.network_scan().await {
        Ok(body) => ctx.json_response(StatusCode::OK, &body, &[]),
        Err(err) => ctx.error_response(err),
    }
}

/// `GET /api/v1/network/events` (`streamNetworkEvents`)
pub async fn stream_network_events(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, _) = req.into_parts();
    let mut ctx = RequestCtx::new(&parts.method, &parts.headers, &state.security);
    if let Err(resp) = ctx.authorize_read(&state, &parts.headers, "streamNetworkEvents", None) {
        return *resp;
    }
    if parts.uri.query().is_some_and(|q| !q.is_empty()) {
        return ctx.error_response(ApiError::bad_request(
            "SSE endpoint does not accept query parameters",
        ));
    }
    let cursor = match single_header_str(&parts.headers, "last-event-id") {
        HeaderExtract::Multiple => {
            return ctx.error_response(ApiError::bad_request("Last-Event-ID may appear only once"));
        }
        HeaderExtract::Missing => None,
        HeaderExtract::Single(raw) => match parse_last_event_id(Some(raw)) {
            Ok(c) => c,
            Err(err) => return ctx.error_response(err),
        },
    };
    let Ok(permit) = Arc::clone(&state.sse_semaphore).try_acquire_owned() else {
        return ctx.error_response(ApiError::stream_capacity_exhausted());
    };

    // Subscribe BEFORE replay_or_snapshot so concurrent publishes never miss a broadcast wakeup.
    let buffer = state.backend.network_event_buffer();
    let sub = buffer.subscribe();

    let current_status = match state.backend.network_status().await {
        Ok(s) => s,
        Err(err) => return ctx.error_response(err),
    };
    let initial_events = match buffer.replay_or_snapshot(cursor, current_status) {
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

    let body = network_sse_body(
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
            "failed to build network SSE response",
        ))
    })
}

/// `POST /api/v1/network/credentials` (`createNetworkCredentialReference`)
pub async fn create_network_credential(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, body) = req.into_parts();
    let mut ctx = RequestCtx::new_with_state(&parts.method, &parts.headers, &state);
    let principal =
        match ctx.authorize_write(&state, &parts.headers, "createNetworkCredentialReference") {
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
        Err(err) => return ctx.error_response(err),
    };
    let passphrase = match validate_credential_request_body(&json) {
        Ok(p) => p,
        Err(err) => return ctx.error_response(err),
    };
    match state
        .backend
        .create_network_credential(principal.principal_id(), passphrase)
        .await
    {
        Ok(receipt) => ctx.json_response(StatusCode::CREATED, &receipt, &[]),
        Err(err) => ctx.error_response(err),
    }
}

fn validate_credential_request_body(body: &Value) -> Result<&str, ApiError> {
    let Some(obj) = body.as_object() else {
        return Err(ApiError::bad_request(
            "credential request must be a JSON object",
        ));
    };
    if obj.len() != 2
        || obj.get("schema").and_then(Value::as_str)
            != Some("miro-ego.network-credential-request.v1")
    {
        return Err(ApiError::bad_request(
            "credential request must contain only schema=miro-ego.network-credential-request.v1 and passphrase",
        ));
    }
    let Some(pass) = obj.get("passphrase").and_then(Value::as_str) else {
        return Err(ApiError::bad_request("missing passphrase string"));
    };
    if pass.chars().count() < 8 || pass.len() > 63 || pass.bytes().any(|b| b < 0x20) {
        return Err(ApiError::bad_request(
            "passphrase must be at least 8 characters and at most 63 UTF-8 bytes without control characters",
        ));
    }
    Ok(pass)
}

/// `POST /api/v1/network/apply` (`applyNetworkDesiredState`)
pub async fn apply_network_desired_state(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, body) = req.into_parts();
    let mut ctx = RequestCtx::new_with_state(&parts.method, &parts.headers, &state);
    let (principal, key) =
        match ctx.authorize_command(&state, &parts.headers, "applyNetworkDesiredState") {
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
        Err(err) => return ctx.error_response(err),
    };
    if let Err(err) = validate_network_apply_body(&json) {
        return ctx.error_response(err);
    }
    let cmd = IdempotentCommand {
        principal_id: principal.principal_id().to_owned(),
        idempotency_key: key,
        body: json,
        canonical_body: canonical,
    };
    match state.backend.apply_network_desired_state(cmd).await {
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

/// `POST /api/v1/network/retry` (`retryNetworkTransaction`)
pub async fn retry_network_transaction(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, body) = req.into_parts();
    let mut ctx = RequestCtx::new_with_state(&parts.method, &parts.headers, &state);
    let (principal, key) =
        match ctx.authorize_command(&state, &parts.headers, "retryNetworkTransaction") {
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
        Err(err) => return ctx.error_response(err),
    };
    if let Err(err) = validate_network_retry_body(&json) {
        return ctx.error_response(err);
    }
    let cmd = IdempotentCommand {
        principal_id: principal.principal_id().to_owned(),
        idempotency_key: key,
        body: json,
        canonical_body: canonical,
    };
    match state.backend.retry_network_transaction(cmd).await {
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

/// `POST /api/v1/network/forget` (`forgetNetworkClientProfile`)
pub async fn forget_network_client_profile(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, body) = req.into_parts();
    let mut ctx = RequestCtx::new_with_state(&parts.method, &parts.headers, &state);
    let (principal, key) =
        match ctx.authorize_command(&state, &parts.headers, "forgetNetworkClientProfile") {
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
        Err(err) => return ctx.error_response(err),
    };
    if let Err(err) = validate_network_forget_body(&json) {
        return ctx.error_response(err);
    }
    let cmd = IdempotentCommand {
        principal_id: principal.principal_id().to_owned(),
        idempotency_key: key,
        body: json,
        canonical_body: canonical,
    };
    match state.backend.forget_network_client_profile(cmd).await {
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

fn validate_network_apply_body(body: &Value) -> Result<(), ApiError> {
    let Some(obj) = body.as_object() else {
        return Err(ApiError::bad_request(
            "network apply request must be a JSON object",
        ));
    };
    if obj.len() != 2
        || obj.get("schema").and_then(Value::as_str) != Some("miro-ego.network-apply-request.v1")
    {
        return Err(ApiError::bad_request(
            "network apply request must have schema=miro-ego.network-apply-request.v1 and desired",
        ));
    }
    let Some(desired) = obj.get("desired").and_then(Value::as_object) else {
        return Err(ApiError::bad_request("desired must be a JSON object"));
    };
    for key in desired.keys() {
        if !matches!(key.as_str(), "mode" | "wifi_client" | "ethernet") {
            return Err(ApiError::bad_request(
                "desired must contain only mode, wifi_client, and ethernet",
            ));
        }
    }
    let Some(mode) = desired.get("mode").and_then(Value::as_str) else {
        return Err(ApiError::bad_request("missing desired.mode"));
    };
    if !matches!(
        mode,
        "hotspot" | "wifi-client" | "ethernet-dhcp" | "ethernet-static"
    ) {
        return Err(ApiError::bad_request("unsupported desired.mode"));
    }

    let wifi = desired
        .get("wifi_client")
        .ok_or_else(|| ApiError::bad_request("desired must include wifi_client"))?;
    let ethernet = desired.get("ethernet").unwrap_or(&Value::Null);

    if mode == "wifi-client" {
        let Some(w) = wifi.as_object() else {
            return Err(ApiError::bad_request(
                "wifi-client mode requires wifi_client object",
            ));
        };
        for k in w.keys() {
            if !matches!(k.as_str(), "ssid" | "security" | "credential_ref") {
                return Err(ApiError::bad_request(format!(
                    "disallowed field in wifi_client: {}",
                    truncate_untrusted_field(k)
                )));
            }
        }
        let Some(ssid) = w.get("ssid").and_then(Value::as_str) else {
            return Err(ApiError::bad_request("missing wifi_client.ssid"));
        };
        if ssid.is_empty() || ssid.len() > 32 {
            return Err(ApiError::bad_request(
                "wifi_client.ssid must be 1..=32 UTF-8 bytes",
            ));
        }
        let Some(sec) = w.get("security").and_then(Value::as_str) else {
            return Err(ApiError::bad_request("missing wifi_client.security"));
        };
        if !matches!(
            sec,
            "open" | "wpa2-personal" | "wpa3-personal" | "wpa2-wpa3-personal"
        ) {
            return Err(ApiError::bad_request("invalid wifi_client.security"));
        }
        let has_ref = w.contains_key("credential_ref");
        if sec == "open" && has_ref {
            return Err(ApiError::bad_request(
                "open Wi-Fi must not include credential_ref",
            ));
        }
        if sec != "open" {
            let Some(cred_ref) = w.get("credential_ref").and_then(Value::as_str) else {
                return Err(ApiError::bad_request(
                    "protected Wi-Fi requires opaque credential_ref",
                ));
            };
            if !is_valid_credential_ref(cred_ref) {
                return Err(ApiError::bad_request("invalid credential_ref format"));
            }
        }
    } else if !wifi.is_null() {
        return Err(ApiError::bad_request(
            "wifi_client must be null unless mode is wifi-client",
        ));
    }

    match mode {
        "hotspot" | "wifi-client" => {
            if !ethernet.is_null() {
                return Err(ApiError::bad_request(
                    "ethernet must be null when mode is hotspot or wifi-client",
                ));
            }
        }
        "ethernet-dhcp" => {
            if !ethernet.is_null() {
                validate_desired_ethernet(ethernet)?;
                if ethernet.get("addressing").and_then(Value::as_str) != Some("dhcp") {
                    return Err(ApiError::bad_request(
                        "ethernet.addressing must be dhcp when mode is ethernet-dhcp",
                    ));
                }
            }
        }
        "ethernet-static" => {
            if ethernet.is_null() {
                return Err(ApiError::bad_request(
                    "ethernet-static mode requires ethernet configuration",
                ));
            }
            validate_desired_ethernet(ethernet)?;
            if ethernet.get("addressing").and_then(Value::as_str) != Some("static") {
                return Err(ApiError::bad_request(
                    "ethernet.addressing must be static when mode is ethernet-static",
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_desired_ethernet(value: &Value) -> Result<(), ApiError> {
    let Some(obj) = value.as_object() else {
        return Err(ApiError::bad_request("ethernet must be an object or null"));
    };
    if obj.len() != 2 {
        return Err(ApiError::bad_request(
            "ethernet must contain only addressing and static_ipv4",
        ));
    }
    let Some(addr_mode) = obj.get("addressing").and_then(Value::as_str) else {
        return Err(ApiError::bad_request("missing ethernet.addressing"));
    };
    let static_ipv4 = obj
        .get("static_ipv4")
        .ok_or_else(|| ApiError::bad_request("missing ethernet.static_ipv4"))?;
    match addr_mode {
        "dhcp" => {
            if !static_ipv4.is_null() {
                return Err(ApiError::bad_request(
                    "static_ipv4 must be null when addressing=dhcp",
                ));
            }
            Ok(())
        }
        "static" => {
            let Some(s) = static_ipv4.as_object() else {
                return Err(ApiError::bad_request(
                    "static addressing requires static_ipv4 object",
                ));
            };
            if s.len() != 4 {
                return Err(ApiError::bad_request(
                    "static_ipv4 must contain address, prefix_length, gateway, dns",
                ));
            }
            let addr_ok = s
                .get("address")
                .and_then(Value::as_str)
                .is_some_and(|ip| ip.parse::<Ipv4Addr>().is_ok());
            let prefix_ok = s
                .get("prefix_length")
                .and_then(Value::as_u64)
                .is_some_and(|p| (1..=32).contains(&p));
            let gw_ok = match s.get("gateway") {
                Some(Value::Null) => true,
                Some(Value::String(ip)) => ip.parse::<Ipv4Addr>().is_ok(),
                _ => false,
            };
            let dns_ok = s.get("dns").and_then(Value::as_array).is_some_and(|arr| {
                if arr.len() > 3 {
                    return false;
                }
                let mut seen = std::collections::HashSet::with_capacity(arr.len());
                arr.iter().all(|item| {
                    item.as_str()
                        .and_then(|ip| ip.parse::<Ipv4Addr>().ok())
                        .is_some_and(|parsed| seen.insert(parsed))
                })
            });
            if !(addr_ok && prefix_ok && gw_ok && dns_ok) {
                return Err(ApiError::bad_request("invalid static_ipv4 parameters"));
            }
            Ok(())
        }
        _ => Err(ApiError::bad_request(
            "ethernet.addressing must be dhcp or static",
        )),
    }
}

fn is_valid_credential_ref(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("cred-") else {
        return false;
    };
    !rest.is_empty()
        && value.len() <= 128
        && rest
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'-'))
}

fn validate_network_retry_body(body: &Value) -> Result<(), ApiError> {
    let Some(obj) = body.as_object() else {
        return Err(ApiError::bad_request(
            "network retry request must be a JSON object",
        ));
    };
    if obj.len() != 2
        || obj.get("schema").and_then(Value::as_str) != Some("miro-ego.network-retry-request.v1")
        || !obj
            .get("transaction_id")
            .and_then(Value::as_str)
            .is_some_and(is_valid_uuid_v7)
    {
        return Err(ApiError::bad_request(
            "network retry request must have schema=miro-ego.network-retry-request.v1 and valid UUIDv7 transaction_id",
        ));
    }
    Ok(())
}

fn validate_network_forget_body(body: &Value) -> Result<(), ApiError> {
    let Some(obj) = body.as_object() else {
        return Err(ApiError::bad_request(
            "network forget request must be a JSON object",
        ));
    };
    if obj.len() != 1
        || obj.get("schema").and_then(Value::as_str) != Some("miro-ego.network-forget-request.v1")
    {
        return Err(ApiError::bad_request(
            "network forget request must contain only schema=miro-ego.network-forget-request.v1",
        ));
    }
    Ok(())
}
