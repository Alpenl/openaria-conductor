//! Miro-Ego v1 HTTP API contract constants, structured error types, and service traits.

use miro_ego_core::CoreError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const API_V1_PREFIX: &str = "/api/v1";
pub const API_ERROR_SCHEMA: &str = "miro-ego.api-error.v1";

pub const HEADER_AUDIT_STATUS: &str = "Miro-Ego-Audit-Status";
pub const HEADER_ERROR_CODE: &str = "Miro-Ego-Error-Code";
pub const HEADER_MANIFEST_SHA256: &str = "Miro-Ego-Manifest-SHA256";
pub const HEADER_WAIT_STATE: &str = "Miro-Ego-Wait-State";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("{code}: {message}")]
pub struct ApiError {
    pub schema: String,
    pub status_code: u16,
    pub code: String,
    pub message: String,
}

impl ApiError {
    pub fn new(status_code: u16, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            schema: API_ERROR_SCHEMA.to_owned(),
            status_code,
            code: code.into(),
            message: message.into(),
        }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiServerConfig {
    pub bind_host: String,
    pub bind_port: u16,
    pub api_prefix: String,
    pub enable_h264_preview: bool,
}

impl Default for ApiServerConfig {
    fn default() -> Self {
        Self {
            bind_host: "0.0.0.0".to_owned(),
            bind_port: 8080,
            api_prefix: API_V1_PREFIX.to_owned(),
            enable_h264_preview: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogFileSummary {
    pub name: String,
    pub bytes: u64,
    pub modified_epoch_secs: u64,
}

/// Core control-plane service contract wired to `/api/v1/...` HTTP handlers in Wave 3.
pub trait ApiService: Send + Sync {
    fn health_json(&self) -> ApiResult<serde_json::Value>;
    fn device_info_json(&self) -> ApiResult<serde_json::Value>;
    fn capture_status_json(&self) -> ApiResult<serde_json::Value>;
    fn list_logs(&self) -> ApiResult<Vec<LogFileSummary>>;
}
