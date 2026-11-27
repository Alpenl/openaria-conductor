//! Gateway shared runtime state, Host/Origin/CORS/CSRF/Auth/Idempotency enforcement,
//! strict JSON body parsing, audit recording, and Problem Details HTTP response rendering.

use crate::backend::GatewayBackend;
use crate::downloads::{ArtifactDeleteGate, LogEtagCache};
use crate::error::{ApiError, ApiResult, HEADER_AUDIT_STATUS, HEADER_ERROR_CODE};
use crate::security::{
    AuditEvent, Principal, SecurityPolicy, SecurityProfile, is_allowed_gateway_host,
    is_same_gateway_origin, is_valid_idempotency_key,
};
use axum::body::Body;
use axum::extract::State;
use axum::http::header::{self, HeaderName, HeaderValue};
use axum::http::{HeaderMap, Method, Request, Response, StatusCode};
use axum::middleware::Next;
use miro_ego_core::ids::new_uuid_v4;
use miro_ego_core::sys::fs::{DEFAULT_IO_BUFFER_BYTES, parse_strict_json_value};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, watch};

pub const MAX_BODY_BYTES: usize = 64 * 1024;
pub const MAX_JSON_DEPTH: usize = 32;
pub const DEFAULT_REQUEST_BODY_TIMEOUT: Duration = Duration::from_secs(10);

/// Exact `Access-Control-Expose-Headers` value mandated by Miro-Ego Device API v1.
pub const CORS_EXPOSE_HEADERS: &str = "Accept-Ranges, Content-Range, ETag, Idempotency-Replayed, Retry-After, Miro-Ego-Audit-Status, Miro-Ego-Error-Code, Miro-Ego-Manifest-SHA256, Miro-Ego-Wait-State";

/// Runtime limits and parameters shared by all HTTP/HTTPS gateway handlers.
#[derive(Debug, Clone)]
pub struct GatewayRuntimeConfig {
    pub external_scheme: String,
    pub max_concurrent_downloads: usize,
    pub max_sse_connections: usize,
    pub max_preview_streams: usize,
    pub max_firmware_slots: usize,
    pub sse_heartbeat_interval: Duration,
    pub download_chunk_bytes: usize,
    pub request_body_timeout: Duration,
}

impl Default for GatewayRuntimeConfig {
    fn default() -> Self {
        Self {
            external_scheme: "http".to_owned(),
            max_concurrent_downloads: 8,
            max_sse_connections: 4,
            max_preview_streams: 2,
            max_firmware_slots: 4,
            sse_heartbeat_interval: Duration::from_secs(15),
            download_chunk_bytes: DEFAULT_IO_BUFFER_BYTES,
            request_body_timeout: DEFAULT_REQUEST_BODY_TIMEOUT,
        }
    }
}

/// Shared gateway state injected into every Axum handler and middleware layer.
pub struct GatewayState {
    pub backend: Arc<dyn GatewayBackend>,
    pub security: Arc<SecurityPolicy>,
    pub config: GatewayRuntimeConfig,
    pub allowed_hosts: Arc<HashSet<String>>,
    pub download_semaphore: Arc<Semaphore>,
    pub artifact_delete_gate: Arc<ArtifactDeleteGate>,
    pub log_etag_cache: Arc<LogEtagCache>,
    pub sse_semaphore: Arc<Semaphore>,
    pub preview_semaphore: Arc<Semaphore>,
    pub firmware_semaphore: Arc<Semaphore>,
    shutdown_tx: watch::Sender<bool>,
    pub shutdown_rx: watch::Receiver<bool>,
}

impl GatewayState {
    pub fn new(
        backend: Arc<dyn GatewayBackend>,
        security: Arc<SecurityPolicy>,
        allowed_hosts: HashSet<String>,
        config: GatewayRuntimeConfig,
    ) -> Arc<Self> {
        let max_downloads = config.max_concurrent_downloads.max(1);
        let max_sse = config.max_sse_connections.max(1);
        let max_preview = config.max_preview_streams.max(1);
        let max_fw = config.max_firmware_slots.max(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Arc::new(Self {
            backend,
            security,
            config,
            allowed_hosts: Arc::new(allowed_hosts),
            download_semaphore: Arc::new(Semaphore::new(max_downloads)),
            artifact_delete_gate: Arc::new(ArtifactDeleteGate::new()),
            log_etag_cache: Arc::new(LogEtagCache::default()),
            sse_semaphore: Arc::new(Semaphore::new(max_sse)),
            preview_semaphore: Arc::new(Semaphore::new(max_preview)),
            firmware_semaphore: Arc::new(Semaphore::new(max_fw)),
            shutdown_tx,
            shutdown_rx,
        })
    }

    /// Subscribe to the gateway-wide graceful shutdown signal.
    pub fn subscribe_shutdown(&self) -> watch::Receiver<bool> {
        self.shutdown_rx.clone()
    }

    /// Broadcast immediate graceful shutdown to all open SSE, preview, and HTTP/1.1 tasks.
    pub fn signal_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

/// Per-request security & audit context extracted in handlers.
pub struct RequestCtx {
    pub request_id: String,
    pub is_head: bool,
    pub audit_failed: bool,
    pub cors_origin: Option<String>,
}

impl RequestCtx {
    pub fn new(method: &Method, headers: &HeaderMap, security: &SecurityPolicy) -> Self {
        let cors_origin = match single_header(headers, header::ORIGIN) {
            HeaderExtract::Single(origin) if security.allowed_origins().contains(origin) => {
                Some(origin.to_owned())
            }
            _ => None,
        };
        Self {
            request_id: new_uuid_v4(),
            is_head: *method == Method::HEAD,
            audit_failed: false,
            cors_origin,
        }
    }

    pub fn new_with_state(method: &Method, headers: &HeaderMap, state: &GatewayState) -> Self {
        let mut ctx = Self::new(method, headers, &state.security);
        if ctx.cors_origin.is_none()
            && let HeaderExtract::Single(origin) = single_header(headers, header::ORIGIN)
            && let HeaderExtract::Single(host) = single_header(headers, header::HOST)
            && is_same_gateway_origin(
                origin,
                host,
                &state.config.external_scheme,
                &state.allowed_hosts,
            )
        {
            ctx.cors_origin = Some(origin.to_owned());
        }
        ctx
    }

    pub fn audit(
        &mut self,
        state: &GatewayState,
        operation_id: &str,
        resource_id: Option<&str>,
        outcome: &str,
        principal: Option<&Principal>,
    ) -> bool {
        let event = AuditEvent::new(
            &self.request_id,
            principal.map(|p| p.principal_id().to_owned()),
            operation_id,
            resource_id.map(str::to_owned),
            outcome,
        );
        if state.backend.record_audit(event).is_err() {
            self.audit_failed = true;
            return false;
        }
        true
    }

    /// Authorize a read operation (`Host`, `Origin`, `Authorization`, permission, and fail-closed audit).
    pub fn authorize_read(
        &mut self,
        state: &GatewayState,
        headers: &HeaderMap,
        operation_id: &str,
        resource_id: Option<&str>,
    ) -> Result<Principal, Box<Response<Body>>> {
        self.authorize_inner(state, headers, operation_id, resource_id, true)
    }

    /// Authorize a write operation (`Host`, `Origin`, `Authorization`, permission, `X-CSRF-Token`,
    /// and fail-closed audit).
    pub fn authorize_write(
        &mut self,
        state: &GatewayState,
        headers: &HeaderMap,
        operation_id: &str,
    ) -> Result<Principal, Box<Response<Body>>> {
        self.authorize_write_inner(state, headers, operation_id, true, true)
    }

    fn authorize_write_inner(
        &mut self,
        state: &GatewayState,
        headers: &HeaderMap,
        operation_id: &str,
        require_unscoped_permission: bool,
        emit_allowed_audit: bool,
    ) -> Result<Principal, Box<Response<Body>>> {
        let (principal, authenticated_bearer) =
            self.authenticate_caller(state, headers, operation_id, None)?;
        let permitted = if require_unscoped_permission {
            principal.permits(operation_id, None)
        } else {
            principal.has_operation_permission(operation_id)
        };
        if !permitted {
            self.audit(state, operation_id, None, "forbidden", Some(&principal));
            return Err(Box::new(self.error_response(ApiError::forbidden(
                "forbidden",
                "principal is not authorized for this operation or resource",
            ))));
        }

        let origin_ext = single_header(headers, header::ORIGIN);
        if state.security.profile() == SecurityProfile::Customer {
            match origin_ext {
                HeaderExtract::Single(_) => {}
                HeaderExtract::Missing | HeaderExtract::Multiple => {
                    self.audit(
                        state,
                        operation_id,
                        None,
                        "origin_forbidden",
                        Some(&principal),
                    );
                    return Err(Box::new(self.error_response(ApiError::forbidden(
                        "origin_forbidden",
                        "customer mutation request must provide a single allowed Origin header",
                    ))));
                }
            }
        }

        let host_str = match single_header(headers, header::HOST) {
            HeaderExtract::Single(h) => Some(h),
            _ => None,
        };
        let is_cross_origin = match origin_ext {
            HeaderExtract::Single(origin) => !host_str.is_some_and(|h| {
                is_same_gateway_origin(
                    origin,
                    h,
                    &state.config.external_scheme,
                    &state.allowed_hosts,
                )
            }),
            HeaderExtract::Missing => false,
            HeaderExtract::Multiple => true,
        };

        let csrf_required = state.security.profile() == SecurityProfile::Customer
            || state.security.csrf_token().is_some()
            || is_cross_origin;

        let csrf_header = single_header_str(headers, "x-csrf-token");
        let csrf_ok = match csrf_header {
            HeaderExtract::Multiple => false,
            HeaderExtract::Missing => !csrf_required,
            HeaderExtract::Single(val) => {
                !csrf_required
                    || state
                        .security
                        .verify_csrf_for_caller(Some(val), authenticated_bearer.as_deref())
            }
        };
        if !csrf_ok {
            self.audit(
                state,
                operation_id,
                None,
                "csrf_forbidden",
                Some(&principal),
            );
            return Err(Box::new(self.error_response(ApiError::forbidden(
                "csrf_forbidden",
                "X-CSRF-Token is missing or invalid",
            ))));
        }

        if emit_allowed_audit && !self.audit(state, operation_id, None, "allowed", Some(&principal))
        {
            return Err(Box::new(self.error_response(ApiError::audit_unavailable())));
        }
        Ok(principal)
    }

    /// Authorize an idempotent command operation (all write checks + required `Idempotency-Key`
    /// verified BEFORE emitting the fail-closed `"allowed"` audit event).
    pub fn authorize_command(
        &mut self,
        state: &GatewayState,
        headers: &HeaderMap,
        operation_id: &str,
    ) -> Result<(Principal, String), Box<Response<Body>>> {
        let principal = self.authorize_write_inner(state, headers, operation_id, true, false)?;
        let key = match single_header_str(headers, "idempotency-key") {
            HeaderExtract::Single(key) if is_valid_idempotency_key(key) => key.to_owned(),
            _ => {
                self.audit(
                    state,
                    operation_id,
                    None,
                    "invalid_request",
                    Some(&principal),
                );
                return Err(Box::new(self.error_response(ApiError::bad_request(
                    "Idempotency-Key header is missing or invalid",
                ))));
            }
        };
        if !self.audit(state, operation_id, None, "allowed", Some(&principal)) {
            return Err(Box::new(self.error_response(ApiError::audit_unavailable())));
        }
        Ok((principal, key))
    }

    /// Authenticate, CSRF-validate, and check `Idempotency-Key` for a resource-scoped idempotent
    /// command (`deleteSessions`) before per-target resource IDs are inspected in the JSON body.
    /// Emits `"invalid_request"` if `Idempotency-Key` is missing/invalid; `"allowed"` is emitted
    /// after body and resource-level authorization succeed.
    pub fn authorize_resource_command_headers(
        &mut self,
        state: &GatewayState,
        headers: &HeaderMap,
        operation_id: &str,
    ) -> Result<(Principal, String), Box<Response<Body>>> {
        let principal = self.authorize_write_inner(state, headers, operation_id, false, false)?;
        match single_header_str(headers, "idempotency-key") {
            HeaderExtract::Single(key) if is_valid_idempotency_key(key) => {
                Ok((principal, key.to_owned()))
            }
            _ => {
                self.audit(
                    state,
                    operation_id,
                    None,
                    "invalid_request",
                    Some(&principal),
                );
                Err(Box::new(self.error_response(ApiError::bad_request(
                    "Idempotency-Key header is missing or invalid",
                ))))
            }
        }
    }

    /// Authenticate a caller (`Host`, `Origin`, Bearer token) and verify operation-level permission
    /// BEFORE validating parameterized path segments. Guarantees unauthenticated Customer callers
    /// always fail closed with `401 Unauthorized` even for malformed session IDs or log traversal paths.
    pub fn authenticate_for_operation(
        &mut self,
        state: &GatewayState,
        headers: &HeaderMap,
        operation_id: &str,
    ) -> Result<Principal, Box<Response<Body>>> {
        let (principal, _) = self.authenticate_caller(state, headers, operation_id, None)?;
        if !principal.has_operation_permission(operation_id) {
            self.audit(state, operation_id, None, "forbidden", Some(&principal));
            return Err(Box::new(self.error_response(ApiError::forbidden(
                "forbidden",
                "principal is not authorized for this operation or resource",
            ))));
        }
        Ok(principal)
    }

    /// Enforce resource-scoped permission and fail-closed `"allowed"` audit after path parameters
    /// have been validated for an already-authenticated principal.
    pub fn authorize_resource_read(
        &mut self,
        state: &GatewayState,
        principal: &Principal,
        operation_id: &str,
        resource_id: Option<&str>,
    ) -> Result<(), Box<Response<Body>>> {
        if !principal.permits(operation_id, resource_id) {
            self.audit(
                state,
                operation_id,
                resource_id,
                "forbidden",
                Some(principal),
            );
            return Err(Box::new(self.error_response(ApiError::forbidden(
                "forbidden",
                "principal is not authorized for this operation or resource",
            ))));
        }
        if !self.audit(state, operation_id, resource_id, "allowed", Some(principal)) {
            return Err(Box::new(self.error_response(ApiError::audit_unavailable())));
        }
        Ok(())
    }

    fn authenticate_caller(
        &mut self,
        state: &GatewayState,
        headers: &HeaderMap,
        operation_id: &str,
        resource_id: Option<&str>,
    ) -> Result<(Principal, Option<String>), Box<Response<Body>>> {
        // 1. Host allowlist (anti-DNS-rebinding)
        let host_value = match single_header(headers, header::HOST) {
            HeaderExtract::Single(h) if is_allowed_gateway_host(h, &state.allowed_hosts) => Some(h),
            HeaderExtract::Missing => None,
            HeaderExtract::Single(_) | HeaderExtract::Multiple => {
                self.audit(state, operation_id, resource_id, "origin_forbidden", None);
                return Err(Box::new(self.error_response(ApiError::forbidden(
                    "origin_forbidden",
                    "request Host header is not allowed",
                ))));
            }
        };

        // 2. Origin allowlist
        match single_header(headers, header::ORIGIN) {
            HeaderExtract::Missing => {}
            HeaderExtract::Multiple => {
                self.audit(state, operation_id, resource_id, "origin_forbidden", None);
                return Err(Box::new(self.error_response(ApiError::forbidden(
                    "origin_forbidden",
                    "multiple Origin headers are not allowed",
                ))));
            }
            HeaderExtract::Single(origin) => {
                let same_origin = host_value.is_some_and(|h| {
                    is_same_gateway_origin(
                        origin,
                        h,
                        &state.config.external_scheme,
                        &state.allowed_hosts,
                    )
                });
                if !same_origin && !state.security.allowed_origins().contains(origin) {
                    self.audit(state, operation_id, resource_id, "origin_forbidden", None);
                    return Err(Box::new(self.error_response(ApiError::forbidden(
                        "origin_forbidden",
                        "request Origin is not allowed",
                    ))));
                }
            }
        }

        // 3. Bearer authentication
        let auth_header = match single_header(headers, header::AUTHORIZATION) {
            HeaderExtract::Single(val) => Some(val),
            HeaderExtract::Missing => None,
            HeaderExtract::Multiple => {
                self.audit(state, operation_id, resource_id, "unauthorized", None);
                return Err(Box::new(self.error_response(ApiError::unauthorized(
                    "multiple Authorization headers are not allowed",
                ))));
            }
        };
        let Some((principal, bearer_tok)) = state.security.authenticate_with_token(auth_header)
        else {
            self.audit(state, operation_id, resource_id, "unauthorized", None);
            return Err(Box::new(self.error_response(ApiError::unauthorized(
                "bearer credential is missing or invalid",
            ))));
        };
        Ok((principal.clone(), bearer_tok.map(str::to_owned)))
    }

    fn authorize_inner(
        &mut self,
        state: &GatewayState,
        headers: &HeaderMap,
        operation_id: &str,
        resource_id: Option<&str>,
        emit_allowed_audit: bool,
    ) -> Result<Principal, Box<Response<Body>>> {
        let (principal, _) = self.authenticate_caller(state, headers, operation_id, resource_id)?;

        // 4. Operation / resource permission
        if !principal.permits(operation_id, resource_id) {
            self.audit(
                state,
                operation_id,
                resource_id,
                "forbidden",
                Some(&principal),
            );
            return Err(Box::new(self.error_response(ApiError::forbidden(
                "forbidden",
                "principal is not authorized for this operation or resource",
            ))));
        }

        // 5. Fail-closed audit sink on allowed read requests
        if emit_allowed_audit
            && !self.audit(
                state,
                operation_id,
                resource_id,
                "allowed",
                Some(&principal),
            )
        {
            return Err(Box::new(self.error_response(ApiError::audit_unavailable())));
        }

        Ok(principal)
    }

    pub fn json_response(
        &self,
        status: StatusCode,
        body: &Value,
        extra_headers: &[(&str, &str)],
    ) -> Response<Body> {
        let payload = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
        let len = payload.len();
        let mut builder = Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
            .header(header::CONTENT_LENGTH, len.to_string())
            .header(header::CACHE_CONTROL, "no-store");
        builder = self.apply_common_headers(builder, extra_headers);
        let resp_body = if self.is_head {
            Body::empty()
        } else {
            Body::from(payload)
        };
        builder.body(resp_body).unwrap_or_else(|_| fallback_500())
    }

    pub fn empty_response(
        &self,
        status: StatusCode,
        extra_headers: &[(&str, &str)],
    ) -> Response<Body> {
        let mut builder = Response::builder()
            .status(status)
            .header(header::CACHE_CONTROL, "no-store");
        if self.is_head || !(status == StatusCode::NO_CONTENT || status.is_informational()) {
            builder = builder.header(header::CONTENT_LENGTH, "0");
        }
        builder = self.apply_common_headers(builder, extra_headers);
        builder
            .body(Body::empty())
            .unwrap_or_else(|_| fallback_500())
    }

    pub fn error_response(&self, error: ApiError) -> Response<Body> {
        let status =
            StatusCode::from_u16(error.status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let mut extra: Vec<(&str, &str)> = error
            .headers_slice()
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let has_error_code_header = extra
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case(HEADER_ERROR_CODE));
        if !has_error_code_header {
            extra.push((HEADER_ERROR_CODE, error.code.as_str()));
        }
        if matches!(
            status,
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN | StatusCode::REQUEST_TIMEOUT
        ) && !extra
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("connection"))
        {
            extra.push(("Connection", "close"));
        }
        if self.is_head {
            return self.empty_response(status, &extra);
        }
        let problem = error.to_problem_json(&self.request_id);
        let payload = serde_json::to_vec(&problem).unwrap_or_else(|_| b"{}".to_vec());
        let mut builder = Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/problem+json")
            .header(header::CONTENT_LENGTH, payload.len().to_string())
            .header(header::CACHE_CONTROL, "no-store");
        builder = self.apply_common_headers(builder, &extra);
        builder
            .body(Body::from(payload))
            .unwrap_or_else(|_| fallback_500())
    }

    pub fn apply_common_headers(
        &self,
        mut builder: axum::http::response::Builder,
        extra_headers: &[(&str, &str)],
    ) -> axum::http::response::Builder {
        if self.audit_failed {
            builder = builder.header(HEADER_AUDIT_STATUS, "unavailable");
        }
        for (name, value) in extra_headers {
            if let (Ok(h_name), Ok(h_val)) = (
                HeaderName::from_bytes(name.as_bytes()),
                HeaderValue::from_str(value),
            ) {
                builder = builder.header(h_name, h_val);
            }
        }
        if let Some(origin) = &self.cors_origin {
            builder = builder
                .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin.as_str())
                .header(header::ACCESS_CONTROL_EXPOSE_HEADERS, CORS_EXPOSE_HEADERS)
                .header(header::VARY, "Origin");
        }
        builder
    }
}

/// Result of inspecting a single-valued HTTP header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderExtract<'a> {
    Missing,
    Single(&'a str),
    Multiple,
}

pub fn single_header(headers: &HeaderMap, name: HeaderName) -> HeaderExtract<'_> {
    let mut iter = headers.get_all(name).iter();
    let Some(first) = iter.next() else {
        return HeaderExtract::Missing;
    };
    if iter.next().is_some() {
        return HeaderExtract::Multiple;
    }
    match first.to_str() {
        Ok(s) => HeaderExtract::Single(s),
        Err(_) => HeaderExtract::Multiple,
    }
}

pub fn single_header_str<'a>(headers: &'a HeaderMap, name: &str) -> HeaderExtract<'a> {
    let Ok(header_name) = HeaderName::from_bytes(name.as_bytes()) else {
        return HeaderExtract::Missing;
    };
    single_header(headers, header_name)
}

/// Strictly read and parse a bounded JSON object body (`<= 64 KiB`, `Content-Type: application/json`,
/// rejecting duplicate JSON keys and nesting depth > 32) using the default body read timeout.
pub async fn read_strict_json_object(
    headers: &HeaderMap,
    body: Body,
) -> ApiResult<(Value, Vec<u8>)> {
    read_strict_json_object_with_timeout(headers, body, DEFAULT_REQUEST_BODY_TIMEOUT).await
}

/// Strictly read and parse a bounded JSON object body (`<= 64 KiB`) within `body_timeout`
/// to defeat slowloris request-body trickle attacks.
pub async fn read_strict_json_object_with_timeout(
    headers: &HeaderMap,
    body: Body,
    body_timeout: Duration,
) -> ApiResult<(Value, Vec<u8>)> {
    match single_header(headers, header::CONTENT_TYPE) {
        HeaderExtract::Single(ct) => {
            let media = ct.split(';').next().unwrap_or("").trim();
            if !media.eq_ignore_ascii_case("application/json") {
                return Err(ApiError::bad_request(
                    "Content-Type must be application/json",
                ));
            }
        }
        HeaderExtract::Missing => {
            return Err(ApiError::bad_request(
                "Content-Type must be application/json",
            ));
        }
        HeaderExtract::Multiple => {
            return Err(ApiError::bad_request(
                "Content-Type header may appear only once",
            ));
        }
    }

    let effective_timeout = body_timeout.max(Duration::from_millis(20));
    let raw_res = tokio::time::timeout(
        effective_timeout,
        axum::body::to_bytes(body, MAX_BODY_BYTES),
    )
    .await
    .map_err(|_| ApiError::request_timeout("request body read timed out"))?;
    let raw = raw_res.map_err(|_| {
        ApiError::bad_request("request body exceeds 64 KiB limit or failed to read")
    })?;
    if raw.is_empty() {
        return Err(ApiError::bad_request("request body must not be empty"));
    }
    let parsed = parse_strict_json_value(&raw, MAX_JSON_DEPTH)
        .map_err(|err| ApiError::bad_request(format!("invalid JSON body: {err}")))?;
    if !parsed.is_object() {
        return Err(ApiError::bad_request("request body must be a JSON object"));
    }
    let canonical = canonicalize_json_bytes(&parsed)?;
    Ok((parsed, canonical))
}

/// Produce deterministic sorted-key compact JSON bytes for idempotency payload comparison.
pub fn canonicalize_json_bytes(value: &Value) -> ApiResult<Vec<u8>> {
    let sorted = sort_json_value(value);
    serde_json::to_vec(&sorted)
        .map_err(|err| ApiError::internal("serialization_failed", format!("{err}")))
}

fn sort_json_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            let mut out = serde_json::Map::with_capacity(entries.len());
            for (k, v) in entries {
                out.insert(k.clone(), sort_json_value(v));
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(sort_json_value).collect()),
        other => other.clone(),
    }
}

/// Handle `OPTIONS` preflight requests against one declarative route's allowed methods & headers.
pub fn handle_options_preflight(
    state: &GatewayState,
    headers: &HeaderMap,
    allowed_methods: &[&str],
    allowed_headers_fn: fn(&str) -> &'static [&'static str],
) -> Response<Body> {
    let mut ctx = RequestCtx::new_with_state(&Method::OPTIONS, headers, state);

    // Host allowlist is enforced on OPTIONS as well as all HTTP methods.
    let host_value = match single_header(headers, header::HOST) {
        HeaderExtract::Single(h) if is_allowed_gateway_host(h, &state.allowed_hosts) => Some(h),
        HeaderExtract::Missing => None,
        HeaderExtract::Single(_) | HeaderExtract::Multiple => {
            ctx.audit(state, "options", None, "origin_forbidden", None);
            return ctx.error_response(ApiError::forbidden(
                "origin_forbidden",
                "request Host header is not allowed",
            ));
        }
    };

    let allow_header = allowed_methods.join(", ");
    let origin = match single_header(headers, header::ORIGIN) {
        HeaderExtract::Missing => {
            return ctx.empty_response(StatusCode::NO_CONTENT, &[("Allow", &allow_header)]);
        }
        HeaderExtract::Multiple => {
            ctx.audit(state, "options", None, "origin_forbidden", None);
            return ctx.error_response(ApiError::forbidden(
                "origin_forbidden",
                "multiple Origin headers are not allowed",
            ));
        }
        HeaderExtract::Single(o) => o,
    };

    let same_origin = host_value.is_some_and(|h| {
        is_same_gateway_origin(
            origin,
            h,
            &state.config.external_scheme,
            &state.allowed_hosts,
        )
    });
    if !same_origin && !state.security.allowed_origins().contains(origin) {
        ctx.audit(state, "options", None, "origin_forbidden", None);
        return ctx.error_response(ApiError::forbidden(
            "origin_forbidden",
            "request Origin is not allowed",
        ));
    }

    let requested_method = match single_header(headers, header::ACCESS_CONTROL_REQUEST_METHOD) {
        HeaderExtract::Single(m) => m.to_ascii_uppercase(),
        HeaderExtract::Missing => {
            return ctx.error_response(ApiError::bad_request(
                "missing Access-Control-Request-Method header",
            ));
        }
        HeaderExtract::Multiple => {
            return ctx.error_response(ApiError::bad_request(
                "Access-Control-Request-Method may appear only once",
            ));
        }
    };

    if !allowed_methods.contains(&requested_method.as_str()) {
        return ctx.error_response(ApiError::method_not_allowed(&allow_header));
    }

    let allowed_headers = allowed_headers_fn(&requested_method);
    let allowed_lower: HashSet<String> = allowed_headers
        .iter()
        .map(|h| h.to_ascii_lowercase())
        .collect();
    for raw in headers.get_all(header::ACCESS_CONTROL_REQUEST_HEADERS) {
        let Ok(text) = raw.to_str() else {
            return ctx.error_response(ApiError::forbidden(
                "forbidden",
                "invalid Access-Control-Request-Headers value",
            ));
        };
        for item in text.split(',') {
            let token = item.trim().to_ascii_lowercase();
            if !token.is_empty() && !allowed_lower.contains(&token) {
                return ctx.error_response(ApiError::forbidden(
                    "forbidden",
                    "preflight requested header is not allowed",
                ));
            }
        }
    }

    let allow_headers_joined = allowed_headers.join(", ");
    ctx.empty_response(
        StatusCode::NO_CONTENT,
        &[
            ("Allow", &allow_header),
            ("Access-Control-Allow-Methods", &allow_header),
            ("Access-Control-Allow-Headers", &allow_headers_joined),
            ("Access-Control-Max-Age", "600"),
        ],
    )
}

/// Outer Axum middleware enforcing Host anti-DNS-rebinding and HTTP request framing invariants
/// on every request (including `OPTIONS`, `404`, and `405`).
pub async fn gateway_framing_and_host_middleware(
    State(state): State<Arc<GatewayState>>,
    request: Request<Body>,
    next: Next,
) -> Response<Body> {
    let method = request.method().clone();
    let version = request.version();
    let headers = request.headers();
    let mut ctx = RequestCtx::new(&method, headers, &state.security);

    // 1. HTTP/1.1 requires Host header
    if version == axum::http::Version::HTTP_11 && headers.get(header::HOST).is_none() {
        return ctx.error_response(ApiError::bad_request("HTTP/1.1 request must include Host"));
    }

    // 2. Host anti-DNS-rebinding check on ALL requests (including OPTIONS / unknown paths)
    match single_header(headers, header::HOST) {
        HeaderExtract::Missing => {}
        HeaderExtract::Single(h) if is_allowed_gateway_host(h, &state.allowed_hosts) => {}
        HeaderExtract::Single(_) | HeaderExtract::Multiple => {
            ctx.audit(&state, "gatewayHost", None, "origin_forbidden", None);
            return ctx.error_response(ApiError::forbidden(
                "origin_forbidden",
                "request Host header is not allowed",
            ));
        }
    }

    // 3. Reject Transfer-Encoding on incoming request payloads
    if headers.get(header::TRANSFER_ENCODING).is_some() {
        return ctx.error_response(ApiError::bad_request(
            "Transfer-Encoding request bodies are not supported",
        ));
    }

    // 4. Validate Content-Length framing and body-less methods
    let body_allowed = matches!(method, Method::POST | Method::PUT | Method::PATCH);
    let cl_values: Vec<_> = headers.get_all(header::CONTENT_LENGTH).iter().collect();
    if cl_values.len() > 1 {
        return ctx.error_response(ApiError::bad_request(
            "Content-Length header may appear only once",
        ));
    }
    if let Some(raw_cl) = cl_values.first() {
        let Ok(cl_str) = raw_cl.to_str() else {
            return ctx.error_response(ApiError::bad_request("invalid Content-Length header"));
        };
        let trimmed = cl_str.trim();
        let valid_decimal = trimmed == "0"
            || (!trimmed.is_empty()
                && !trimmed.starts_with('0')
                && trimmed.bytes().all(|b| b.is_ascii_digit()));
        if !valid_decimal {
            return ctx.error_response(ApiError::bad_request("invalid Content-Length format"));
        }
        let Ok(len) = trimmed.parse::<usize>() else {
            return ctx.error_response(ApiError::bad_request("request body exceeds maximum size"));
        };
        if len > MAX_BODY_BYTES {
            return ctx.error_response(ApiError::bad_request("request body exceeds 64 KiB limit"));
        }
        if !body_allowed && len != 0 {
            return ctx.error_response(ApiError::bad_request(
                "request method does not accept a request body",
            ));
        }
    }

    next.run(request).await
}

fn fallback_500() -> Response<Body> {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::empty())
        .unwrap_or_else(|_| Response::new(Body::empty()))
}
