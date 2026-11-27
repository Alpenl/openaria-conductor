//! Structured error types and canonical error-code classification for `miro-ego-network`.

use miro_ego_core::CoreError;
use thiserror::Error;

/// Canonical error codes surfaced across the network control plane and RPC wire protocol.
const CANONICAL_ERROR_CODES: &[&str] = &[
    "idempotency_conflict",
    "transaction_not_found",
    "transaction_not_retryable",
    "credential_ref_expired",
    "credential_ref_not_allowed",
    "credential_ref_already_used",
    "credential_ref_invalid",
    "missing_credential_ref",
    "security_mismatch",
    "ssid_too_long",
    "invalid_static_ipv4",
    "unsupported_mode",
    "capture_active",
    "controller_busy",
    "credential_store_full",
    "inline_secret_rejected",
    "invalid_network_desired_state",
    "wifi_auth_failed",
    "dhcp_timeout",
    "network_timeout",
    "network_scan_failed",
    "network_controller_unavailable",
    "network_manager_unavailable",
    "rescue_ap_unavailable",
    "interface_unavailable",
    "activation_failed",
    "request_too_large",
    "request_timeout",
    "response_timeout",
    "response_too_large",
    "response_invalid",
    "operation_invalid",
    "request_invalid",
    "controller_failure",
];

#[derive(Debug, Error)]
pub enum NetworkError {
    #[error("network busy: {message}")]
    Busy { message: String },

    #[error("network validation error: {message}")]
    Invalid { message: String },

    #[error("network backend failed: {message}")]
    Backend { message: String },

    #[error(transparent)]
    Core(#[from] CoreError),
}

pub type NetworkResult<T> = Result<T, NetworkError>;

impl NetworkError {
    /// Return the canonical machine-readable error code when classified, while keeping
    /// `Display` and `message` strings backward-compatible for existing callers.
    pub fn error_code(&self) -> Option<&str> {
        match self {
            Self::Core(core_err) => Some(core_err.code()),
            Self::Busy { message } => classify_message_code(message, ErrorCategory::Busy),
            Self::Invalid { message } => classify_message_code(message, ErrorCategory::Invalid),
            Self::Backend { message } => classify_message_code(message, ErrorCategory::Backend),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ErrorCategory {
    Busy,
    Invalid,
    Backend,
}

fn normalize_alias(token: &str) -> Option<&'static str> {
    match token {
        "network_transaction_not_found" => Some("transaction_not_found"),
        "network_busy" => Some("controller_busy"),
        _ => CANONICAL_ERROR_CODES.iter().copied().find(|&c| c == token),
    }
}

fn classify_message_code(message: &str, category: ErrorCategory) -> Option<&'static str> {
    let trimmed = message.trim();
    if let Some((prefix, _)) = trimmed.split_once(':') {
        if let Some(canonical) = normalize_alias(prefix.trim()) {
            return Some(canonical);
        }
    }

    for &code in CANONICAL_ERROR_CODES {
        if trimmed.contains(code) {
            return Some(code);
        }
    }
    if trimmed.contains("network_transaction_not_found") {
        return Some("transaction_not_found");
    }
    if trimmed.contains("network_busy") {
        return Some("controller_busy");
    }

    if category == ErrorCategory::Invalid {
        if trimmed.contains("static IPv4")
            || trimmed.contains("DNS IPv4")
            || trimmed.contains("prefix_length")
            || trimmed.contains("DNS servers")
            || trimmed.contains("static_ipv4")
            || trimmed.contains("addressing=static")
        {
            return Some("invalid_static_ipv4");
        }
        if trimmed.contains("open Wi-Fi") && trimmed.contains("must not include") {
            return Some("security_mismatch");
        }
        if trimmed.contains("protected Wi-Fi") && trimmed.contains("requires") {
            return Some("missing_credential_ref");
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_canonical_network_error_codes() {
        let cases = [
            (
                NetworkError::Invalid {
                    message: "idempotency_conflict: key reused".to_owned(),
                },
                Some("idempotency_conflict"),
            ),
            (
                NetworkError::Invalid {
                    message: "transaction_not_found: missing".to_owned(),
                },
                Some("transaction_not_found"),
            ),
            (
                NetworkError::Invalid {
                    message: "transaction_not_retryable: committed".to_owned(),
                },
                Some("transaction_not_retryable"),
            ),
            (
                NetworkError::Invalid {
                    message: "credential_ref_expired: expired".to_owned(),
                },
                Some("credential_ref_expired"),
            ),
            (
                NetworkError::Invalid {
                    message: "credential_ref_not_allowed: wrong principal".to_owned(),
                },
                Some("credential_ref_not_allowed"),
            ),
            (
                NetworkError::Invalid {
                    message: "credential_ref_already_used: consumed".to_owned(),
                },
                Some("credential_ref_already_used"),
            ),
            (
                NetworkError::Invalid {
                    message: "missing_credential_ref: protected Wi-Fi requires credential_ref"
                        .to_owned(),
                },
                Some("missing_credential_ref"),
            ),
            (
                NetworkError::Invalid {
                    message: "security_mismatch: open Wi-Fi must not include credential_ref"
                        .to_owned(),
                },
                Some("security_mismatch"),
            ),
            (
                NetworkError::Invalid {
                    message: "ssid_too_long: Wi-Fi SSID exceeds 32 UTF-8 bytes".to_owned(),
                },
                Some("ssid_too_long"),
            ),
            (
                NetworkError::Invalid {
                    message: "invalid_static_ipv4: invalid static IPv4 address: 999.1.1.1"
                        .to_owned(),
                },
                Some("invalid_static_ipv4"),
            ),
            (
                NetworkError::Invalid {
                    message: "unsupported_mode: mesh".to_owned(),
                },
                Some("unsupported_mode"),
            ),
            (
                NetworkError::Busy {
                    message: "capture_active: recording in progress".to_owned(),
                },
                Some("capture_active"),
            ),
        ];

        for (err, expected) in cases {
            assert_eq!(err.error_code(), expected, "failed for {err}");
        }
    }
}
