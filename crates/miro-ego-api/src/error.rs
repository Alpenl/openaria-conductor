//! Structured Miro-Ego v1 API errors (`miro-ego.api-error.v1`) and Problem Details serialization.

use miro_ego_core::CoreError;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

pub const API_ERROR_SCHEMA: &str = "miro-ego.api-error.v1";
pub const MAX_ERROR_MESSAGE_CHARS: usize = 1024;

pub const HEADER_AUDIT_STATUS: &str = "Miro-Ego-Audit-Status";
pub const HEADER_ERROR_CODE: &str = "Miro-Ego-Error-Code";
pub const HEADER_MANIFEST_SHA256: &str = "Miro-Ego-Manifest-SHA256";
pub const HEADER_WAIT_STATE: &str = "Miro-Ego-Wait-State";

/// Clamp an error message to `1..=1024` Unicode scalar characters per OpenAPI `ErrorResponse`.
pub fn clamp_error_message(raw: &str) -> String {
    let non_empty = if raw.is_empty() {
        "request failed"
    } else {
        raw
    };
    if non_empty.chars().count() <= MAX_ERROR_MESSAGE_CHARS {
        return non_empty.to_owned();
    }
    let mut truncated: String = non_empty
        .chars()
        .take(MAX_ERROR_MESSAGE_CHARS - 3)
        .collect();
    truncated.push_str("...");
    truncated
}

/// Structured error carried between handlers, middleware, and [`crate::backend::GatewayBackend`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("{code}: {message}")]
pub struct ApiError {
    pub schema: String,
    pub status_code: u16,
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Box<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<Box<Vec<(String, String)>>>,
}

impl ApiError {
    pub fn new(status_code: u16, code: impl Into<String>, message: impl Into<String>) -> Self {
        let raw_msg = message.into();
        Self {
            schema: API_ERROR_SCHEMA.to_owned(),
            status_code,
            code: code.into(),
            message: clamp_error_message(&raw_msg),
            retryable: status_code >= 500
                || status_code == 429
                || status_code == 423
                || status_code == 408,
            details: None,
            headers: None,
        }
    }

    pub fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = Some(Box::new(details));
        self
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers
            .get_or_insert_with(|| Box::new(Vec::new()))
            .push((name.into(), value.into()));
        self
    }

    pub fn headers_slice(&self) -> &[(String, String)] {
        self.headers.as_deref().map_or(&[], Vec::as_slice)
    }

    pub fn with_error_code_header(self) -> Self {
        let code = self.code.clone();
        self.with_header(HEADER_ERROR_CODE, code)
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(400, "invalid_request", message).with_retryable(false)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(401, "unauthorized", message)
            .with_retryable(false)
            .with_header("WWW-Authenticate", "Bearer")
    }

    pub fn forbidden(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(403, code, message).with_retryable(false)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(404, "not_found", message).with_retryable(false)
    }

    pub fn method_not_allowed(allow: impl Into<String>) -> Self {
        Self::new(
            405,
            "method_not_allowed",
            "method not allowed for this endpoint",
        )
        .with_retryable(false)
        .with_header("Allow", allow)
    }

    pub fn request_timeout(message: impl Into<String>) -> Self {
        Self::new(408, "request_timeout", message)
            .with_retryable(true)
            .with_header("Connection", "close")
    }

    pub fn conflict(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(409, code, message).with_retryable(false)
    }

    pub fn range_not_satisfiable(complete_size: u64, message: impl Into<String>) -> Self {
        Self::new(416, "range_not_satisfiable", message)
            .with_retryable(false)
            .with_header("Content-Range", format!("bytes */{complete_size}"))
    }

    pub fn unprocessable(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(422, code, message).with_retryable(false)
    }

    pub fn too_many_requests(message: impl Into<String>, retry_after_secs: u64) -> Self {
        Self::new(429, "rate_limited", message)
            .with_retryable(true)
            .with_header("Retry-After", retry_after_secs.max(1).to_string())
    }

    pub fn internal(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(500, code, message).with_retryable(false)
    }

    pub fn provider_failure(message: impl Into<String>) -> Self {
        Self::new(500, "provider_failure", message).with_retryable(true)
    }

    pub fn service_unavailable(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(503, code, message).with_retryable(true)
    }

    pub fn capture_busy(current_state: &str, retry_after_secs: u64) -> Self {
        let secs = retry_after_secs.max(1);
        Self::new(
            423,
            "capture_busy",
            "capture resources are busy; wait for idle state",
        )
        .with_retryable(true)
        .with_header(HEADER_ERROR_CODE, "capture_busy")
        .with_header(HEADER_WAIT_STATE, "idle")
        .with_header("Retry-After", secs.to_string())
        .with_details(json!({
            "wait_for": "idle",
            "retry_after_seconds": secs,
            "current_state": current_state,
        }))
    }

    pub fn session_not_verified(reason: &str) -> Self {
        Self::new(
            409,
            "session_not_verified",
            "session does not have a current usable gateway verification verdict",
        )
        .with_retryable(false)
        .with_header(HEADER_ERROR_CODE, "session_not_verified")
        .with_details(json!({ "reason": reason }))
    }

    pub fn preview_h264_unsupported() -> Self {
        Self::new(
            404,
            "preview_h264_unsupported",
            "H.264 fragmented MP4 preview is not enabled on this device",
        )
        .with_retryable(false)
        .with_header(HEADER_ERROR_CODE, "preview_h264_unsupported")
    }

    pub fn camera_focus_unsupported() -> Self {
        Self::new(
            404,
            "camera_focus_unsupported",
            "camera does not expose focus_absolute control",
        )
        .with_retryable(false)
        .with_header(HEADER_ERROR_CODE, "camera_focus_unsupported")
    }

    pub fn storage_unavailable(message: impl Into<String>) -> Self {
        Self::new(503, "storage_unavailable", message)
            .with_retryable(true)
            .with_header(HEADER_ERROR_CODE, "storage_unavailable")
    }

    pub fn camera_not_connected(message: impl Into<String>) -> Self {
        Self::new(503, "camera_not_connected", message)
            .with_retryable(true)
            .with_header(HEADER_ERROR_CODE, "camera_not_connected")
    }

    pub fn preview_unavailable(message: impl Into<String>) -> Self {
        Self::new(503, "preview_unavailable", message)
            .with_retryable(true)
            .with_header(HEADER_ERROR_CODE, "preview_unavailable")
    }

    pub fn preview_capacity_exhausted() -> Self {
        Self::new(
            503,
            "preview_capacity_exhausted",
            "maximum concurrent preview streams reached",
        )
        .with_retryable(true)
        .with_header("Retry-After", "1")
        .with_header(HEADER_ERROR_CODE, "preview_capacity_exhausted")
    }

    pub fn stream_capacity_exhausted() -> Self {
        Self::new(
            503,
            "stream_capacity_exhausted",
            "maximum concurrent SSE connections reached",
        )
        .with_retryable(true)
        .with_header("Retry-After", "1")
        .with_header(HEADER_ERROR_CODE, "stream_capacity_exhausted")
    }

    pub fn network_transaction_not_found(transaction_id: &str) -> Self {
        Self::new(
            404,
            "network_transaction_not_found",
            "network transaction not found",
        )
        .with_retryable(false)
        .with_header(HEADER_ERROR_CODE, "network_transaction_not_found")
        .with_details(json!({ "transaction_id": transaction_id }))
    }

    pub fn network_idempotency_conflict(original_transaction_id: Option<&str>) -> Self {
        Self::new(
            409,
            "idempotency_conflict",
            "Idempotency-Key reused with different network mutation request",
        )
        .with_retryable(false)
        .with_header(HEADER_ERROR_CODE, "idempotency_conflict")
        .with_details(json!({
            "idempotency_scope": "network-mutation",
            "original_transaction_id": original_transaction_id,
        }))
    }

    pub fn network_mutation_unavailable(reason: &str, message: impl Into<String>) -> Self {
        Self::new(503, "network_mutation_unavailable", message)
            .with_retryable(true)
            .with_header(HEADER_ERROR_CODE, "network_mutation_unavailable")
            .with_details(json!({ "reason": reason }))
    }

    pub fn audit_unavailable() -> Self {
        Self::new(
            500,
            "audit_unavailable",
            "audit sink is temporarily unavailable; retry later",
        )
        .with_retryable(true)
        .with_header(HEADER_ERROR_CODE, "audit_unavailable")
    }

    pub fn download_capacity_exhausted() -> Self {
        Self::new(
            503,
            "download_capacity_exhausted",
            "concurrent session download limit reached; retry later",
        )
        .with_retryable(true)
        .with_header("Retry-After", "1")
        .with_header(HEADER_ERROR_CODE, "download_capacity_exhausted")
    }

    /// Format the exact OpenAPI `ErrorResponse` JSON payload (`miro-ego.api-error.v1`).
    pub fn to_problem_json(&self, request_id: &str) -> Value {
        let mut error_obj = serde_json::Map::new();
        error_obj.insert("code".to_owned(), Value::String(self.code.clone()));
        error_obj.insert(
            "message".to_owned(),
            Value::String(clamp_error_message(&self.message)),
        );
        error_obj.insert(
            "request_id".to_owned(),
            Value::String(request_id.to_owned()),
        );
        error_obj.insert("retryable".to_owned(), Value::Bool(self.retryable));
        if let Some(details) = &self.details {
            error_obj.insert("details".to_owned(), (**details).clone());
        }
        let mut root = serde_json::Map::new();
        root.insert(
            "schema".to_owned(),
            Value::String(API_ERROR_SCHEMA.to_owned()),
        );
        root.insert("error".to_owned(), Value::Object(error_obj));
        Value::Object(root)
    }
}

impl From<CoreError> for ApiError {
    fn from(error: CoreError) -> Self {
        let status_code = match &error {
            CoreError::InvalidInput { .. } | CoreError::InvalidConfig { .. } => 400,
            CoreError::LockBusy { .. } => 409,
            CoreError::Io { .. } | CoreError::Poisoned { .. } => 500,
        };
        Self::new(status_code, error.code(), error.to_string())
    }
}

pub type ApiResult<T> = Result<T, ApiError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn problem_json_matches_openapi_v1_schema_shape() {
        let err = ApiError::capture_busy("recording", 2);
        let req_id = miro_ego_core::ids::new_uuid_v4();
        let body = err.to_problem_json(&req_id);
        assert_eq!(body["schema"], "miro-ego.api-error.v1");
        assert_eq!(body["error"]["code"], "capture_busy");
        assert_eq!(body["error"]["request_id"], req_id);
        assert_eq!(body["error"]["retryable"], true);
        assert_eq!(body["error"]["details"]["wait_for"], "idle");
        assert_eq!(body["error"]["details"]["retry_after_seconds"], 2);
        assert_eq!(body["error"]["details"]["current_state"], "recording");
    }
}
