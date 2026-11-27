//! Typed error translation mapping `miro-ego-storage`, `miro-ego-network`, `miro-ego-hw`,
//! `miro-ego-deploy`, and V4L2 camera focus errors onto OpenAPI v1 `ApiError` responses.

use miro_ego_api::ApiError;
use miro_ego_core::CoreError;
use miro_ego_core::ids::is_valid_uuid_v7;
use miro_ego_deploy::DeployError;
use miro_ego_hw::HwError;
use miro_ego_network::NetworkError;
use miro_ego_storage::StorageError;
use serde_json::json;

pub fn map_storage_error(err: StorageError) -> ApiError {
    match err {
        StorageError::CaptureBusy { state, .. } => ApiError::capture_busy(state.as_str(), 1),
        StorageError::Locked {
            code: "capture_busy",
            message,
        } => {
            let current_state = if message.contains("finalizing") {
                "finalizing"
            } else if message.contains("verifying") {
                "verifying"
            } else if message.contains("encoding") {
                "encoding"
            } else {
                "recording"
            };
            ApiError::capture_busy(current_state, 1)
        }
        StorageError::Unavailable { message } => ApiError::storage_unavailable(message),
        StorageError::CatalogChanged { catalog_revision } => ApiError::conflict(
            "catalog_changed",
            "session catalog revision changed; restart listing without cursor",
        )
        .with_retryable(true)
        .with_error_code_header()
        .with_details(json!({ "catalog_revision": catalog_revision })),
        StorageError::RangeNotSatisfiable { total_bytes } => {
            ApiError::range_not_satisfiable(total_bytes, "requested byte range is not satisfiable")
        }
        StorageError::Unsupported { code, message } => {
            ApiError::new(404, code, message).with_error_code_header()
        }
        other => {
            let status = other.http_status();
            let code = other.code().to_owned();
            let retryable = other.retryable();
            let message = other.to_string();
            ApiError::new(status, code, message)
                .with_retryable(retryable)
                .with_error_code_header()
        }
    }
}

fn extract_uuid_v7_from_text(message: &str) -> Option<&str> {
    message
        .split(|c: char| c.is_whitespace() || matches!(c, ':' | ',' | ';' | '(' | ')' | '\'' | '"'))
        .find(|tok| is_valid_uuid_v7(tok))
}

fn map_network_classified_error(code: Option<&str>, message: String, is_backend: bool) -> ApiError {
    match code {
        Some("idempotency_conflict") => {
            let orig_tx = extract_uuid_v7_from_text(&message);
            ApiError::network_idempotency_conflict(orig_tx)
        }
        Some("transaction_not_found") => {
            let tx_id = extract_uuid_v7_from_text(&message)
                .unwrap_or("00000000-0000-7000-8000-000000000000");
            ApiError::network_transaction_not_found(tx_id)
        }
        Some("transaction_not_retryable") => {
            ApiError::unprocessable("invalid_network_desired_state", message)
                .with_error_code_header()
                .with_details(json!({
                    "field": "transaction_id",
                    "reason": "active_state",
                }))
        }
        Some("credential_ref_expired") => {
            ApiError::unprocessable("invalid_network_desired_state", message)
                .with_error_code_header()
                .with_details(json!({
                    "field": "desired.wifi_client.credential_ref",
                    "reason": "credential_ref_expired",
                }))
        }
        Some("credential_ref_not_allowed") => {
            ApiError::unprocessable("invalid_network_desired_state", message)
                .with_error_code_header()
                .with_details(json!({
                    "field": "desired.wifi_client.credential_ref",
                    "reason": "credential_ref_not_allowed",
                }))
        }
        Some("missing_credential_ref") => {
            ApiError::unprocessable("invalid_network_desired_state", message)
                .with_error_code_header()
                .with_details(json!({
                    "field": "desired.wifi_client.credential_ref",
                    "reason": "missing_credential_ref",
                }))
        }
        Some("credential_ref_already_used" | "credential_ref_invalid") => {
            ApiError::unprocessable("invalid_network_desired_state", message)
                .with_error_code_header()
                .with_details(json!({
                    "field": "desired.wifi_client.credential_ref",
                    "reason": "credential_ref_already_used",
                }))
        }
        Some("security_mismatch") => {
            ApiError::unprocessable("invalid_network_desired_state", message)
                .with_error_code_header()
                .with_details(json!({
                    "field": "desired.wifi_client.security",
                    "reason": "security_mismatch",
                }))
        }
        Some("ssid_too_long") => ApiError::unprocessable("invalid_network_desired_state", message)
            .with_error_code_header()
            .with_details(json!({
                "field": "desired.wifi_client.ssid",
                "reason": "ssid_too_long",
            })),
        Some("invalid_static_ipv4") => {
            ApiError::unprocessable("invalid_network_desired_state", message)
                .with_error_code_header()
                .with_details(json!({
                    "field": "desired.ethernet.static_ipv4",
                    "reason": "invalid_static_ipv4",
                }))
        }
        Some("unsupported_mode") => {
            ApiError::unprocessable("invalid_network_desired_state", message)
                .with_error_code_header()
                .with_details(json!({
                    "field": "desired.mode",
                    "reason": "unsupported_mode",
                }))
        }
        Some("capture_active") => {
            ApiError::conflict("capture_active", message).with_error_code_header()
        }
        _ => map_network_message_fallback(message, is_backend),
    }
}

fn map_network_message_fallback(message: String, is_backend: bool) -> ApiError {
    if message.contains("credential_ref_") {
        ApiError::unprocessable("invalid_network_desired_state", message)
            .with_error_code_header()
            .with_details(json!({
                "field": "desired.wifi_client.credential_ref",
                "reason": "credential_ref_already_used",
            }))
    } else if is_backend {
        ApiError::service_unavailable("network_controller_unavailable", message)
            .with_header("Retry-After", "1")
            .with_error_code_header()
    } else {
        ApiError::bad_request(message).with_error_code_header()
    }
}

pub fn map_network_error(err: NetworkError) -> ApiError {
    let classified = err.error_code().map(str::to_owned);
    match err {
        NetworkError::Busy { message } => {
            if classified.as_deref() == Some("capture_active") || message.contains("capture_active")
            {
                ApiError::conflict("capture_active", message).with_error_code_header()
            } else {
                ApiError::service_unavailable("network_busy", message)
                    .with_header("Retry-After", "1")
                    .with_error_code_header()
            }
        }
        NetworkError::Invalid { message } => {
            map_network_classified_error(classified.as_deref(), message, false)
        }
        NetworkError::Backend { message } => {
            map_network_classified_error(classified.as_deref(), message, true)
        }
        NetworkError::Core(core_err) => ApiError::from(core_err),
    }
}

pub(super) fn network_mutation_unavailable(reason: &str, message: impl Into<String>) -> ApiError {
    ApiError::service_unavailable("network_mutation_unavailable", message)
        .with_header("Retry-After", "1")
        .with_error_code_header()
        .with_details(json!({ "reason": reason }))
}

fn infer_network_mutation_reason(code: Option<&str>, message: &str) -> &'static str {
    match code {
        Some("capture_active") => return "capture_active",
        Some("network_manager_unavailable")
            if !message.contains("network_controller_unavailable")
                && !message.contains("response_timeout")
                && !message.contains("empty EOF") =>
        {
            return "network_manager_unavailable";
        }
        _ => {}
    }
    if message.contains("not_enabled") || message.contains("not configured") {
        "not_enabled"
    } else if message.contains("auth_profile_unavailable") {
        "auth_profile_unavailable"
    } else if message.contains("rescue_ap_not_validated") {
        "rescue_ap_not_validated"
    } else if message.contains("capture_active") {
        "capture_active"
    } else if message.contains("recovery_required") {
        "recovery_required"
    } else if message.contains("maintenance_window_closed") {
        "maintenance_window_closed"
    } else if message.contains("unsupported_concurrency") {
        "unsupported_concurrency"
    } else if message.contains("network_manager_unavailable")
        && !message.contains("network_controller_unavailable")
        && !message.contains("response_timeout")
        && !message.contains("empty EOF")
    {
        "network_manager_unavailable"
    } else {
        "controller_unavailable"
    }
}

/// Map a [`NetworkError`] returned from `/network/{credentials,apply,retry,forget}` into an
/// OpenAPI-compliant error (`503` always uses `code: "network_mutation_unavailable"` with
/// `details.reason` matching `NetworkMutationDisabledReason`).
pub fn map_network_mutation_error(err: NetworkError) -> ApiError {
    let classified = err.error_code().map(str::to_owned);
    match err {
        NetworkError::Busy { message } => {
            let reason = infer_network_mutation_reason(classified.as_deref(), &message);
            network_mutation_unavailable(reason, message)
        }
        NetworkError::Backend { message } => {
            let reason = infer_network_mutation_reason(classified.as_deref(), &message);
            network_mutation_unavailable(reason, message)
        }
        NetworkError::Core(CoreError::LockBusy { message }) => {
            let reason = if message.contains("network") {
                "capture_active"
            } else {
                "maintenance_window_closed"
            };
            network_mutation_unavailable(reason, message)
        }
        NetworkError::Core(CoreError::Io {
            context, source, ..
        }) => {
            network_mutation_unavailable("controller_unavailable", format!("{context}: {source}"))
        }
        other => {
            let mapped = map_network_error(other);
            if mapped.status_code == 503 {
                let reason = infer_network_mutation_reason(classified.as_deref(), &mapped.message);
                network_mutation_unavailable(reason, mapped.message)
            } else {
                mapped
            }
        }
    }
}

pub fn map_hw_error(err: HwError) -> ApiError {
    match err {
        HwError::Unavailable { message } => {
            ApiError::service_unavailable("hardware_unavailable", message).with_error_code_header()
        }
        HwError::Io { message } => {
            ApiError::internal("hardware_io_failed", message).with_error_code_header()
        }
        HwError::Core(core_err) => ApiError::from(core_err),
    }
}

pub fn map_deploy_error(err: DeployError) -> ApiError {
    match err {
        DeployError::InvalidBundle { message } => {
            ApiError::unprocessable("invalid_firmware_bundle", message).with_error_code_header()
        }
        DeployError::Blocked { message } => {
            if message.contains("rollback_unavailable") {
                ApiError::conflict("rollback_unavailable", message).with_error_code_header()
            } else if message.contains("idempotency_conflict") {
                ApiError::conflict("idempotency_conflict", message).with_error_code_header()
            } else if message.contains("maintenance_busy")
                || message.contains("server is busy")
                || message.contains("controller_busy")
            {
                ApiError::service_unavailable("firmware_busy", message)
                    .with_header("Retry-After", "1")
                    .with_error_code_header()
            } else {
                ApiError::conflict("firmware_blocked", message).with_error_code_header()
            }
        }
        DeployError::Failed { message } => {
            if message.contains("deploy_controller_unavailable")
                || message.contains("response_timeout")
                || message.contains("request_timeout")
                || message.contains("timed out")
                || message.contains("failed writing deploy RPC")
                || message.contains("failed configuring deploy RPC")
                || message.contains("empty EOF")
            {
                ApiError::service_unavailable("firmware_unavailable", message)
                    .with_header("Retry-After", "1")
                    .with_error_code_header()
            } else {
                ApiError::internal("firmware_failed", message).with_error_code_header()
            }
        }
        DeployError::Core(CoreError::LockBusy { message }) => {
            ApiError::service_unavailable("firmware_busy", message)
                .with_header("Retry-After", "1")
                .with_error_code_header()
        }
        DeployError::Core(core_err) => ApiError::from(core_err),
    }
}

/// Map low-level V4L2 / `NativeCameraStream` focus errors onto OpenAPI v1 status codes.
pub fn map_camera_focus_error(code: &str, message: String) -> StorageError {
    match code {
        "camera_focus_unsupported" => StorageError::Unsupported {
            code: "camera_focus_unsupported",
            message,
        },
        "invalid_camera_focus" | "camera_focus_auto_unsupported" => {
            StorageError::unprocessable("invalid_camera_focus", message)
        }
        "invalid_state" | "native_camera_closed" | "camera_not_connected" => {
            StorageError::camera_not_connected(message)
        }
        _ if message.contains("No such device") || message.contains("os error 19") => {
            StorageError::camera_not_connected(message)
        }
        _ => StorageError::service_unavailable("camera_focus_failed", message),
    }
}

pub(super) fn map_clock_rpc_error(err: NetworkError) -> ApiError {
    let code = err.error_code().map(str::to_owned);
    match err {
        NetworkError::Invalid { message } => {
            ApiError::bad_request(message).with_error_code_header()
        }
        NetworkError::Busy { message }
            if code.as_deref() == Some("capture_active") || message.contains("capture_active") =>
        {
            ApiError::conflict("capture_active", message).with_error_code_header()
        }
        NetworkError::Busy { message } | NetworkError::Backend { message } => {
            ApiError::service_unavailable("clock_unavailable", message)
                .with_header("Retry-After", "1")
                .with_error_code_header()
        }
        NetworkError::Core(core_err) => ApiError::from(core_err),
    }
}
