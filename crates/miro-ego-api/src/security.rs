//! Deployment security profiles (`lab` / `customer`), constant-time token verification,
//! per-operation authorization, Host anti-DNS-rebinding validation, and audit events.

use crate::error::{ApiError, ApiResult};
use miro_ego_core::ids::CUSTOMER_BEARER_TOKEN_MAX_LENGTH;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv6Addr};
use std::time::{SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;

pub const AUDIT_EVENT_SCHEMA: &str = "miro-ego.api-audit-event.v1";

/// Exact list of all 37 Miro-Ego v1 OpenAPI `operationId`s.
pub const ALL_V1_OPERATION_IDS: [&str; 37] = [
    "getDevice",
    "getDeviceClock",
    "inspectDeviceClock",
    "syncDeviceClock",
    "firmwareStatus",
    "checkFirmware",
    "firmwareUpdate",
    "rollbackFirmware",
    "getCaptureReadiness",
    "getCaptureStatus",
    "getVoice",
    "setVoiceVolume",
    "getCaptureTimer",
    "setCaptureTimer",
    "startCapture",
    "stopCapture",
    "streamCaptureEvents",
    "getCurrentSafeSwapReceipt",
    "getPreview",
    "getPreviewH264",
    "getCameraFocus",
    "setCameraFocus",
    "getNetworkStatus",
    "scanNetworks",
    "streamNetworkEvents",
    "createNetworkCredentialReference",
    "applyNetworkDesiredState",
    "retryNetworkTransaction",
    "forgetNetworkClientProfile",
    "listSessions",
    "getSession",
    "getRetainedUnsuccessfulSessionOutcome",
    "getSessionArtifact",
    "headSessionArtifact",
    "deleteSessions",
    "listLogs",
    "getLogFile",
];

/// All 13 mutation operations subject to Origin and CSRF protection.
pub const MUTATION_OPERATION_IDS: [&str; 13] = [
    "startCapture",
    "stopCapture",
    "deleteSessions",
    "applyNetworkDesiredState",
    "retryNetworkTransaction",
    "forgetNetworkClientProfile",
    "createNetworkCredentialReference",
    "firmwareUpdate",
    "rollbackFirmware",
    "setVoiceVolume",
    "setCaptureTimer",
    "setCameraFocus",
    "syncDeviceClock",
];

/// Subset of 9 mutation operations requiring an `Idempotency-Key` header.
pub const IDEMPOTENCY_OPERATION_IDS: [&str; 9] = [
    "startCapture",
    "stopCapture",
    "deleteSessions",
    "applyNetworkDesiredState",
    "retryNetworkTransaction",
    "forgetNetworkClientProfile",
    "firmwareUpdate",
    "rollbackFirmware",
    "setCameraFocus",
];

const DEFAULT_ALLOWED_HOSTS: [&str; 4] = ["127.0.0.1", "localhost", "::1", "local"];
const WILDCARD_LISTEN_HOSTS: [&str; 4] = ["", "0.0.0.0", "::", "[::]"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SecurityProfile {
    Lab,
    Customer,
}

impl SecurityProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Lab => "lab",
            Self::Customer => "customer",
        }
    }
}

/// Authenticated caller and its per-operation resource permissions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    principal_id: String,
    permissions: HashMap<String, Option<HashSet<String>>>,
}

impl Principal {
    pub fn new(
        principal_id: impl Into<String>,
        permissions: HashMap<String, Option<HashSet<String>>>,
    ) -> ApiResult<Self> {
        let principal_id = principal_id.into();
        if principal_id.is_empty() || principal_id.len() > 128 {
            return Err(ApiError::bad_request(
                "principal_id must be 1..=128 characters",
            ));
        }
        Ok(Self {
            principal_id,
            permissions,
        })
    }

    pub fn all_v1_operations(principal_id: impl Into<String>) -> ApiResult<Self> {
        let permissions = ALL_V1_OPERATION_IDS
            .iter()
            .map(|op| ((*op).to_owned(), None))
            .collect();
        Self::new(principal_id, permissions)
    }

    pub fn principal_id(&self) -> &str {
        &self.principal_id
    }

    pub fn permissions(&self) -> &HashMap<String, Option<HashSet<String>>> {
        &self.permissions
    }

    pub fn permits(&self, operation_id: &str, resource_id: Option<&str>) -> bool {
        let Some(allowed_resources) = self.permissions.get(operation_id) else {
            return false;
        };
        match allowed_resources {
            None => true,
            Some(resources) => resource_id.is_some_and(|id| resources.contains(id)),
        }
    }
}

/// Secret-free per-request authorization audit record (`miro-ego.api-audit-event.v1`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    pub schema: String,
    pub recorded_at: String,
    pub request_id: String,
    pub principal_id: Option<String>,
    pub operation_id: String,
    pub resource_id: Option<String>,
    pub outcome: String,
}

impl AuditEvent {
    pub fn new(
        request_id: impl Into<String>,
        principal_id: Option<String>,
        operation_id: impl Into<String>,
        resource_id: Option<String>,
        outcome: impl Into<String>,
    ) -> Self {
        Self {
            schema: AUDIT_EVENT_SCHEMA.to_owned(),
            recorded_at: rfc3339_now_utc(),
            request_id: request_id.into(),
            principal_id,
            operation_id: operation_id.into(),
            resource_id,
            outcome: outcome.into(),
        }
    }
}

/// Immutable startup security policy (`lab` or `customer`).
#[derive(Debug, Clone)]
pub struct SecurityPolicy {
    profile: SecurityProfile,
    tokens: Vec<(String, Principal)>,
    allowed_origins: HashSet<String>,
    csrf_token: Option<String>,
    lab_principal: Option<Principal>,
}

impl SecurityPolicy {
    pub fn customer(
        tokens: HashMap<String, Principal>,
        allowed_origins: HashSet<String>,
        csrf_token: Option<String>,
    ) -> ApiResult<Self> {
        if tokens.is_empty() {
            return Err(ApiError::bad_request(
                "customer security profile requires at least one bearer token",
            ));
        }
        for token in tokens.keys() {
            if !valid_token_characters(token) {
                return Err(ApiError::bad_request(
                    "invalid customer bearer token format",
                ));
            }
        }
        if let Some(ref csrf) = csrf_token
            && !valid_token_characters(csrf)
        {
            return Err(ApiError::bad_request("invalid csrf_token format"));
        }
        let has_mutation = tokens.values().any(|principal| {
            MUTATION_OPERATION_IDS
                .iter()
                .any(|op| principal.permissions.contains_key(*op))
        });
        if has_mutation && csrf_token.is_none() {
            return Err(ApiError::bad_request(
                "customer profile with mutation permissions must configure csrf_token",
            ));
        }
        validate_origins(&allowed_origins)?;
        Ok(Self {
            profile: SecurityProfile::Customer,
            tokens: tokens.into_iter().collect(),
            allowed_origins,
            csrf_token,
            lab_principal: None,
        })
    }

    pub fn lab(
        allowed_operations: HashSet<String>,
        allowed_origins: HashSet<String>,
        csrf_token: Option<String>,
    ) -> ApiResult<Self> {
        if let Some(ref csrf) = csrf_token
            && !valid_token_characters(csrf)
        {
            return Err(ApiError::bad_request("invalid csrf_token format"));
        }
        validate_origins(&allowed_origins)?;
        let permissions = allowed_operations
            .into_iter()
            .map(|op| (op, None))
            .collect();
        let principal = Principal::new("isolated-lab-device", permissions)?;
        Ok(Self {
            profile: SecurityProfile::Lab,
            tokens: Vec::new(),
            allowed_origins,
            csrf_token,
            lab_principal: Some(principal),
        })
    }

    /// Convenience constructor for a lab deployment permitting all 37 v1 operations.
    pub fn lab_default() -> Self {
        let permissions = ALL_V1_OPERATION_IDS
            .iter()
            .map(|op| ((*op).to_owned(), None))
            .collect();
        Self {
            profile: SecurityProfile::Lab,
            tokens: Vec::new(),
            allowed_origins: HashSet::new(),
            csrf_token: None,
            lab_principal: Some(Principal {
                principal_id: "isolated-lab-device".to_owned(),
                permissions,
            }),
        }
    }

    pub fn profile(&self) -> SecurityProfile {
        self.profile
    }

    pub fn allowed_origins(&self) -> &HashSet<String> {
        &self.allowed_origins
    }

    pub fn csrf_token(&self) -> Option<&str> {
        self.csrf_token.as_deref()
    }

    /// Authenticate a request using constant-time UTF-8 byte comparison for Customer bearer tokens.
    pub fn authenticate(&self, authorization: Option<&str>) -> Option<&Principal> {
        if self.profile == SecurityProfile::Lab {
            return self.lab_principal.as_ref();
        }
        let header = authorization?;
        let candidate = header.strip_prefix("Bearer ")?;
        if candidate.is_empty() {
            return None;
        }
        let candidate_bytes = candidate.as_bytes();
        let mut matched: Option<&Principal> = None;
        for (token, principal) in &self.tokens {
            if constant_time_eq_bytes(candidate_bytes, token.as_bytes()) {
                matched = Some(principal);
            }
        }
        matched
    }

    pub fn verify_csrf(&self, candidate: Option<&str>) -> bool {
        match (self.csrf_token.as_deref(), candidate) {
            (Some(expected), Some(actual)) => constant_time_eq(actual, expected),
            _ => false,
        }
    }
}

pub fn constant_time_eq(a: &str, b: &str) -> bool {
    constant_time_eq_bytes(a.as_bytes(), b.as_bytes())
}

pub fn constant_time_eq_bytes(a: &[u8], b: &[u8]) -> bool {
    bool::from(a.ct_eq(b))
}

pub fn valid_token_characters(value: &str) -> bool {
    (1..=CUSTOMER_BEARER_TOKEN_MAX_LENGTH).contains(&value.len())
        && value.bytes().all(|b| (0x21..=0x7E).contains(&b))
}

/// Validate single-occurrence `Idempotency-Key` (`1..=128` visible ASCII excluding spaces and `"`).
pub fn is_valid_idempotency_key(key: &str) -> bool {
    (1..=128).contains(&key.len()) && key.bytes().all(|b| b != b'"' && (0x21..=0x7E).contains(&b))
}

fn validate_origins(origins: &HashSet<String>) -> ApiResult<()> {
    if origins.contains("*") {
        return Err(ApiError::bad_request(
            "wildcard '*' Origin is prohibited by Miro-Ego security policy",
        ));
    }
    Ok(())
}

fn is_valid_port(port: &str) -> bool {
    !port.is_empty()
        && port.len() <= 5
        && port.bytes().all(|b| b.is_ascii_digit())
        && port.parse::<u16>().is_ok_and(|p| p >= 1)
}

/// Extract normalized lowercase hostname from a single HTTP `Host` header.
pub fn extract_host_hostname(host_header: &str) -> Option<String> {
    let raw = host_header.trim();
    if raw.is_empty()
        || raw
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b < 0x20 || matches!(b, b'/' | b'@' | b'\\' | b'#'))
    {
        return None;
    }
    if let Some(after_open) = raw.strip_prefix('[') {
        let end = after_open.find(']')?;
        let addr = &after_open[..end];
        let rest = &after_open[end + 1..];
        if !rest.is_empty() {
            let port = rest.strip_prefix(':')?;
            if !is_valid_port(port) {
                return None;
            }
        }
        let ip: Ipv6Addr = addr.parse().ok()?;
        return Some(ip.to_string().to_ascii_lowercase());
    }
    let hostname = if let Some((host_part, port_part)) = raw.split_once(':') {
        if !is_valid_port(port_part) {
            return None;
        }
        host_part
    } else {
        raw
    };
    let normalized = hostname.trim_end_matches('.').to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }
    Some(normalized)
}

/// Build the normalized `Host` allowlist from the server's listen address and configured hosts.
/// Never admits wildcard addresses (`0.0.0.0`, `::`).
pub fn normalize_allowed_hosts<I, S>(listen_host: &str, configured: I) -> HashSet<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut hosts: HashSet<String> = DEFAULT_ALLOWED_HOSTS
        .iter()
        .map(|h| (*h).to_owned())
        .collect();
    let trimmed_listen = listen_host.trim();
    if !WILDCARD_LISTEN_HOSTS.contains(&trimmed_listen)
        && let Some(norm) = extract_host_hostname(trimmed_listen)
        && !is_unspecified_ip(&norm)
    {
        hosts.insert(norm);
    }
    for entry in configured {
        let text = entry.as_ref().trim();
        if let Some(extracted) = extract_host_hostname(text) {
            if !is_unspecified_ip(&extracted) {
                hosts.insert(extracted);
            }
        } else if let Ok(v6) = text.parse::<Ipv6Addr>()
            && !v6.is_unspecified()
        {
            hosts.insert(v6.to_string().to_ascii_lowercase());
        }
    }
    hosts
}

fn is_unspecified_ip(hostname: &str) -> bool {
    hostname
        .parse::<IpAddr>()
        .is_ok_and(|ip| ip.is_unspecified())
}

fn is_valid_mdns_token(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

fn is_private_or_local_ip(ip: IpAddr) -> bool {
    if ip.is_unspecified() {
        return false;
    }
    match ip {
        IpAddr::V4(v4) => v4.is_loopback() || v4.is_private() || v4.is_link_local(),
        IpAddr::V6(v6) => {
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return !mapped.is_unspecified()
                    && (mapped.is_loopback() || mapped.is_private() || mapped.is_link_local());
            }
            let seg0 = v6.segments()[0];
            let is_unique_local = (seg0 & 0xfe00) == 0xfc00;
            let is_link_local = (seg0 & 0xffc0) == 0xfe80;
            v6.is_loopback() || is_unique_local || is_link_local
        }
    }
}

/// Anti-DNS-rebinding check for HTTP `Host` header.
/// Rejects `0.0.0.0`, `::`, public domains, and public IP addresses.
pub fn is_allowed_gateway_host(host_header: &str, allowed_hosts: &HashSet<String>) -> bool {
    let Some(hostname) = extract_host_hostname(host_header) else {
        return false;
    };
    if is_unspecified_ip(&hostname) {
        return false;
    }
    if allowed_hosts.contains(&hostname) {
        return true;
    }
    if hostname.ends_with(".local")
        && hostname.len() > ".local".len()
        && is_valid_mdns_token(&hostname)
    {
        return true;
    }
    if let Ok(ip) = hostname.parse::<IpAddr>() {
        return is_private_or_local_ip(ip);
    }
    false
}

/// Return `true` when `Origin` exactly matches `<external_scheme>://<Host>` (no path/query/fragment)
/// and `<Host>` itself passes the gateway Host allowlist.
pub fn is_same_gateway_origin(
    origin: &str,
    host_header: &str,
    external_scheme: &str,
    allowed_hosts: &HashSet<String>,
) -> bool {
    if !is_allowed_gateway_host(host_header, allowed_hosts) {
        return false;
    }
    let Some((scheme, rest)) = origin.split_once("://") else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case(external_scheme) || rest.is_empty() {
        return false;
    }
    if rest
        .bytes()
        .any(|b| matches!(b, b'/' | b'?' | b'#' | b'@' | b'\\') || b <= 0x20)
    {
        return false;
    }
    rest.eq_ignore_ascii_case(host_header.trim())
}

/// Format current UTC wall-clock timestamp in RFC 3339 format (`YYYY-MM-DDTHH:MM:SSZ`).
pub fn rfc3339_now_utc() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format_unix_secs_rfc3339(secs)
}

/// Format an unsigned Unix timestamp (seconds since 1970-01-01T00:00:00Z) as RFC 3339 UTC.
pub fn format_unix_secs_rfc3339(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let hour = rem / 3_600;
    let minute = (rem % 3_600) / 60;
    let second = rem % 60;
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Howard Hinnant's civil-from-days algorithm (exact proleptic Gregorian calendar).
fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + i64::from(m <= 2);
    (year, m as u32, d as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_allowlist_blocks_dns_rebinding_and_wildcards() {
        let allowed = normalize_allowed_hosts("0.0.0.0", Vec::<String>::new());
        assert!(is_allowed_gateway_host("localhost:8080", &allowed));
        assert!(is_allowed_gateway_host("127.0.0.1:8080", &allowed));
        assert!(is_allowed_gateway_host("[::1]:8080", &allowed));
        assert!(is_allowed_gateway_host("10.42.0.1:443", &allowed));
        assert!(is_allowed_gateway_host("192.168.1.42", &allowed));
        assert!(is_allowed_gateway_host("miro-ego.local:8443", &allowed));

        assert!(!is_allowed_gateway_host("0.0.0.0:8080", &allowed));
        assert!(!is_allowed_gateway_host("[::]:8080", &allowed));
        assert!(!is_allowed_gateway_host("evil.example.com", &allowed));
        assert!(!is_allowed_gateway_host("8.8.8.8", &allowed));
        assert!(!is_allowed_gateway_host("localhost:99999", &allowed));
        assert!(!is_allowed_gateway_host("localhost/evil", &allowed));
    }

    #[test]
    fn customer_policy_enforces_constant_time_bearer_and_csrf_requirement() {
        let full = Principal::all_v1_operations("operator-1").unwrap();
        let mut tokens = HashMap::new();
        tokens.insert("super-secret-token-01".to_owned(), full.clone());

        // Missing CSRF token when mutation permissions exist must fail closed.
        assert!(SecurityPolicy::customer(tokens.clone(), HashSet::new(), None).is_err());

        // Wildcard origin must fail closed.
        let mut bad_origins = HashSet::new();
        bad_origins.insert("*".to_owned());
        assert!(
            SecurityPolicy::customer(tokens.clone(), bad_origins, Some("csrf-1234".to_owned()))
                .is_err()
        );

        let policy =
            SecurityPolicy::customer(tokens, HashSet::new(), Some("csrf-token-1".to_owned()))
                .unwrap();
        assert_eq!(
            policy
                .authenticate(Some("Bearer super-secret-token-01"))
                .map(Principal::principal_id),
            Some("operator-1")
        );
        assert!(policy.authenticate(Some("Bearer wrong-token")).is_none());
        assert!(policy.authenticate(Some("Basic abc")).is_none());
        assert!(policy.authenticate(None).is_none());
        assert!(policy.verify_csrf(Some("csrf-token-1")));
        assert!(!policy.verify_csrf(Some("csrf-token-2")));
        assert!(!policy.verify_csrf(None));
    }

    #[test]
    fn rfc3339_formatting_matches_epoch_and_known_dates() {
        assert_eq!(format_unix_secs_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(
            format_unix_secs_rfc3339(1_764_152_701),
            "2025-11-26T10:25:01Z"
        );
    }
}
