//! Fixed-size bounded persistent network state store (`/var/lib/miro-ego/network-state.json`).
//!
//! Enforces atomic writes, strict secret-key redaction, and a hard cap of `MAX_RETAINED_RECEIPTS`
//! (64 entries) so the 20 GiB system disk never experiences unbounded growth.

use crate::nmcli::{StaticIpv4Config, WifiSecurity};
use crate::{NetworkError, NetworkResult};
use miro_ego_core::ids::{hex_encode, new_uuid_v4, new_uuid_v7};
use miro_ego_core::sys::fs::{read_bounded_json, write_bytes_atomic};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub const DEFAULT_NETWORK_STATE_FILE: &str = "/var/lib/miro-ego/network-state.json";
pub const NETWORK_STATE_SCHEMA: &str = "miro-ego.network-state.v1";
pub const NETWORK_TRANSACTION_SCHEMA: &str = "miro-ego.network-transaction.v1";
pub const NETWORK_RECEIPT_SCHEMA: &str = "miro-ego.network-transaction-receipt.v1";
pub const MAX_RETAINED_RECEIPTS: usize = 64;
pub const MAX_STATE_FILE_BYTES: usize = 128 * 1024;
pub const MAX_STATE_JSON_DEPTH: usize = 32;

const FORBIDDEN_PERSISTED_KEYS: &[&str] = &[
    "credential_ref",
    "password",
    "passphrase",
    "psk",
    "secret",
    "token",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DesiredMode {
    #[serde(rename = "hotspot")]
    Hotspot,
    #[serde(rename = "wifi-client")]
    WifiClient,
    #[serde(rename = "ethernet-dhcp")]
    EthernetDhcp,
    #[serde(rename = "ethernet-static")]
    EthernetStatic,
}

impl DesiredMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hotspot => "hotspot",
            Self::WifiClient => "wifi-client",
            Self::EthernetDhcp => "ethernet-dhcp",
            Self::EthernetStatic => "ethernet-static",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialState {
    Absent,
    PendingInput,
    Stored,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DesiredWifiClient {
    pub ssid: String,
    pub security: WifiSecurity,
    pub credential_state: CredentialState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EthernetAddressing {
    Dhcp,
    Static,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DesiredEthernet {
    pub addressing: EthernetAddressing,
    pub static_ipv4: Option<StaticIpv4Config>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DesiredState {
    pub mode: DesiredMode,
    pub wifi_client: Option<DesiredWifiClient>,
    pub ethernet: Option<DesiredEthernet>,
}

impl DesiredState {
    pub fn hotspot() -> Self {
        Self {
            mode: DesiredMode::Hotspot,
            wifi_client: None,
            ethernet: None,
        }
    }

    pub fn wifi_client(
        ssid: impl Into<String>,
        security: WifiSecurity,
        credential_state: CredentialState,
    ) -> Self {
        Self {
            mode: DesiredMode::WifiClient,
            wifi_client: Some(DesiredWifiClient {
                ssid: ssid.into(),
                security,
                credential_state,
            }),
            ethernet: None,
        }
    }

    pub fn ethernet_dhcp() -> Self {
        Self {
            mode: DesiredMode::EthernetDhcp,
            wifi_client: None,
            ethernet: Some(DesiredEthernet {
                addressing: EthernetAddressing::Dhcp,
                static_ipv4: None,
            }),
        }
    }

    pub fn ethernet_static(cfg: StaticIpv4Config) -> Self {
        Self {
            mode: DesiredMode::EthernetStatic,
            wifi_client: None,
            ethernet: Some(DesiredEthernet {
                addressing: EthernetAddressing::Static,
                static_ipv4: Some(cfg),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionOperation {
    Apply,
    Retry,
    Forget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionStatus {
    Accepted,
    Running,
    Committed,
    Rescued,
    Failed,
}

impl TransactionStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Committed | Self::Rescued | Self::Failed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionStage {
    Accepted,
    Prepared,
    ApReady,
    Activating,
    Verifying,
    Committed,
    FallingBack,
    Rescued,
    Failed,
    Forgetting,
    Forgotten,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryAction {
    AwaitDevice,
    ReconnectTargetLan,
    ReconnectRescueAp,
    Retry,
    ServiceRequired,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RescueMetadata {
    pub ap_validated: bool,
    pub fallback_mode: String,
    pub failure_trigger_seconds: u32,
}

impl Default for RescueMetadata {
    fn default() -> Self {
        Self {
            ap_validated: false,
            fallback_mode: "hotspot".to_owned(),
            failure_trigger_seconds: 10,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkTransaction {
    pub schema: String,
    pub authority_epoch: String,
    pub source_revision: u64,
    pub transaction_id: String,
    pub operation: TransactionOperation,
    pub status: TransactionStatus,
    pub stage: TransactionStage,
    pub desired: DesiredState,
    pub accepted_at: String,
    pub updated_at: String,
    pub recovery_action: RecoveryAction,
    pub rescue: RescueMetadata,
    pub error: Option<TransactionError>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionWindow {
    pub current: Option<NetworkTransaction>,
    pub latest: Option<NetworkTransaction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionReceipt {
    pub schema: String,
    pub accepted_at: String,
    pub transaction: NetworkTransaction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredReceipt {
    pub scope: String,
    pub request_fingerprint: String,
    pub source_revision: u64,
    pub receipt: TransactionReceipt,
}

/// Retained secret-free candidate reference for retrying a rescued/failed transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedCandidate {
    pub transaction_id: String,
    pub profile_name: String,
    pub desired: DesiredState,
}

/// Complete persisted network controller state stored in a single fixed-size JSON file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistedNetworkState {
    pub schema: String,
    pub authority_epoch: String,
    pub source_revision: u64,
    pub saved: bool,
    pub verified: bool,
    pub desired: DesiredState,
    pub transaction: TransactionWindow,
    pub rescue_profile: Option<String>,
    pub rescue_ssid: Option<String>,
    pub lkg_wifi_profile: Option<String>,
    pub lkg_ethernet_profile: Option<String>,
    pub retained_candidate: Option<RetainedCandidate>,
    pub receipts: Vec<StoredReceipt>,
}

impl PersistedNetworkState {
    pub fn initial() -> Self {
        Self {
            schema: NETWORK_STATE_SCHEMA.to_owned(),
            authority_epoch: new_uuid_v4(),
            source_revision: 0,
            saved: false,
            verified: false,
            desired: DesiredState::hotspot(),
            transaction: TransactionWindow::default(),
            rescue_profile: None,
            rescue_ssid: None,
            lkg_wifi_profile: None,
            lkg_ethernet_profile: None,
            retained_candidate: None,
            receipts: Vec::new(),
        }
    }

    pub fn validate(&self) -> NetworkResult<()> {
        if self.schema != NETWORK_STATE_SCHEMA {
            return Err(NetworkError::Invalid {
                message: format!("unexpected network state schema: {}", self.schema),
            });
        }
        if self.verified && !self.saved {
            return Err(NetworkError::Invalid {
                message: "network state invariant violated: verified=true requires saved=true"
                    .to_owned(),
            });
        }
        if self.receipts.len() > MAX_RETAINED_RECEIPTS {
            return Err(NetworkError::Invalid {
                message: format!(
                    "network state receipt history {} exceeds cap {MAX_RETAINED_RECEIPTS}",
                    self.receipts.len()
                ),
            });
        }
        if let Some(current) = &self.transaction.current {
            if current.status.is_terminal() {
                return Err(NetworkError::Invalid {
                    message: "current transaction cannot have terminal status".to_owned(),
                });
            }
        }
        if let Some(latest) = &self.transaction.latest {
            if !latest.status.is_terminal() {
                return Err(NetworkError::Invalid {
                    message: "latest transaction must have terminal status".to_owned(),
                });
            }
        }
        Ok(())
    }
}

/// Public snapshot without internal receipt history or profile filenames.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateSnapshot {
    pub authority_epoch: String,
    pub source_revision: u64,
    pub saved: bool,
    pub verified: bool,
    pub desired: DesiredState,
    pub transaction: TransactionWindow,
}

/// Result of calling `accept_transaction`: either a fresh acceptance or an idempotent replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptOutcome {
    pub receipt: TransactionReceipt,
    pub replayed: bool,
}

/// Transition parameters for advancing the active transaction.
#[derive(Debug, Clone)]
pub struct TransactionUpdate {
    pub status: TransactionStatus,
    pub stage: TransactionStage,
    pub updated_at: String,
    pub recovery_action: RecoveryAction,
    pub ap_validated: Option<bool>,
    pub error: Option<TransactionError>,
    pub desired: Option<DesiredState>,
    pub publish_desired: bool,
    pub saved: Option<bool>,
    pub verified: Option<bool>,
    pub retained_candidate: Option<Option<RetainedCandidate>>,
}

/// Check recursively whether a JSON value contains any forbidden secret field name.
pub fn contains_forbidden_secret_key(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(map) => map.iter().any(|(k, v)| {
            let lower = k.to_ascii_lowercase();
            FORBIDDEN_PERSISTED_KEYS.iter().any(|&f| f == lower) || contains_forbidden_secret_key(v)
        }),
        serde_json::Value::Array(items) => items.iter().any(contains_forbidden_secret_key),
        _ => false,
    }
}

/// Compute idempotency scope key `sha256(principal_id + '\0' + idempotency_key)`.
pub fn receipt_scope(principal_id: &str, idempotency_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(principal_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(idempotency_key.as_bytes());
    hex_encode(hasher.finalize().as_slice())
}

/// Thread-safe, atomically persisted, bounded network state store.
pub struct NetworkStateStore {
    path: PathBuf,
    inner: Mutex<PersistedNetworkState>,
}

impl NetworkStateStore {
    pub fn open_or_init(path: impl Into<PathBuf>) -> NetworkResult<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|err| NetworkError::Backend {
                    message: format!(
                        "failed to create network state dir {}: {err}",
                        parent.display()
                    ),
                })?;
            }
        }

        let state = match fs::symlink_metadata(&path) {
            Ok(_) => {
                let loaded: PersistedNetworkState = read_bounded_json(
                    &path,
                    MAX_STATE_FILE_BYTES,
                    MAX_STATE_JSON_DEPTH,
                )
                .map_err(|err| NetworkError::Invalid {
                    message: format!("failed to load network state {}: {err}", path.display()),
                })?;
                loaded.validate()?;
                loaded
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let initial = PersistedNetworkState::initial();
                persist_state_atomic(&path, &initial)?;
                initial
            }
            Err(err) => {
                return Err(NetworkError::Backend {
                    message: format!("failed to stat network state {}: {err}", path.display()),
                });
            }
        };

        Ok(Self {
            path,
            inner: Mutex::new(state),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn snapshot(&self) -> NetworkResult<StateSnapshot> {
        let guard = self.lock_state()?;
        Ok(StateSnapshot {
            authority_epoch: guard.authority_epoch.clone(),
            source_revision: guard.source_revision,
            saved: guard.saved,
            verified: guard.verified,
            desired: guard.desired.clone(),
            transaction: guard.transaction.clone(),
        })
    }

    pub fn full_state(&self) -> NetworkResult<PersistedNetworkState> {
        Ok(self.lock_state()?.clone())
    }

    pub fn receipt_count(&self) -> NetworkResult<usize> {
        Ok(self.lock_state()?.receipts.len())
    }

    pub fn set_rescue_info(&self, profile: String, ssid: String) -> NetworkResult<()> {
        let mut state = self.lock_state()?;
        if state.rescue_profile.as_deref() == Some(profile.as_str())
            && state.rescue_ssid.as_deref() == Some(ssid.as_str())
        {
            return Ok(());
        }
        state.rescue_profile = Some(profile);
        state.rescue_ssid = Some(ssid);
        persist_state_atomic(&self.path, &state)
    }

    pub fn set_lkg_profile(&self, is_wifi: bool, profile: Option<String>) -> NetworkResult<()> {
        let mut state = self.lock_state()?;
        if is_wifi {
            state.lkg_wifi_profile = profile;
        } else {
            state.lkg_ethernet_profile = profile;
        }
        persist_state_atomic(&self.path, &state)
    }

    pub fn bump_scan_revision(&self) -> NetworkResult<(String, u64)> {
        let mut state = self.lock_state()?;
        state.source_revision = state.source_revision.saturating_add(1);
        persist_state_atomic(&self.path, &state)?;
        Ok((state.authority_epoch.clone(), state.source_revision))
    }

    /// Check for an existing idempotent receipt without mutating state.
    pub fn replay_receipt(
        &self,
        principal_id: &str,
        idempotency_key: &str,
        request_fingerprint: &str,
    ) -> NetworkResult<Option<TransactionReceipt>> {
        let scope = receipt_scope(principal_id, idempotency_key);
        let state = self.lock_state()?;
        if let Some(entry) = state.receipts.iter().find(|r| r.scope == scope) {
            if entry.request_fingerprint != request_fingerprint {
                return Err(NetworkError::Invalid {
                    message:
                        "idempotency_conflict: idempotency key already used for another request"
                            .to_owned(),
                });
            }
            return Ok(Some(entry.receipt.clone()));
        }
        Ok(None)
    }

    /// Accept a new transaction or replay an identical idempotent request.
    #[allow(clippy::too_many_arguments)]
    pub fn accept_transaction(
        &self,
        operation: TransactionOperation,
        desired: DesiredState,
        principal_id: &str,
        idempotency_key: &str,
        request_fingerprint: &str,
        accepted_at: String,
        publish_desired: bool,
        retained_candidate: Option<RetainedCandidate>,
    ) -> NetworkResult<AcceptOutcome> {
        let scope = receipt_scope(principal_id, idempotency_key);
        let mut state = self.lock_state()?;

        if let Some(existing) = state.receipts.iter().find(|r| r.scope == scope) {
            if existing.request_fingerprint != request_fingerprint {
                return Err(NetworkError::Invalid {
                    message:
                        "idempotency_conflict: idempotency key already used for another request"
                            .to_owned(),
                });
            }
            return Ok(AcceptOutcome {
                receipt: existing.receipt.clone(),
                replayed: true,
            });
        }

        if state.transaction.current.is_some() {
            return Err(NetworkError::Busy {
                message: "controller_busy: another network transaction is still active".to_owned(),
            });
        }

        let next_revision = state.source_revision.saturating_add(1);
        let transaction_id = new_uuid_v7();
        let tx = NetworkTransaction {
            schema: NETWORK_TRANSACTION_SCHEMA.to_owned(),
            authority_epoch: state.authority_epoch.clone(),
            source_revision: next_revision,
            transaction_id,
            operation,
            status: TransactionStatus::Accepted,
            stage: TransactionStage::Accepted,
            desired: desired.clone(),
            accepted_at: accepted_at.clone(),
            updated_at: accepted_at.clone(),
            recovery_action: RecoveryAction::AwaitDevice,
            rescue: RescueMetadata::default(),
            error: None,
        };
        let receipt = TransactionReceipt {
            schema: NETWORK_RECEIPT_SCHEMA.to_owned(),
            accepted_at,
            transaction: tx.clone(),
        };

        let mut next = state.clone();
        next.source_revision = next_revision;
        if publish_desired {
            next.desired = desired;
            next.saved = next.desired.mode != DesiredMode::Hotspot;
            next.verified = false;
        }
        next.transaction.current = Some(tx);
        if let Some(candidate) = retained_candidate {
            next.retained_candidate = Some(candidate);
        }
        next.receipts.push(StoredReceipt {
            scope,
            request_fingerprint: request_fingerprint.to_owned(),
            source_revision: next_revision,
            receipt: receipt.clone(),
        });
        prune_receipts(&mut next, MAX_RETAINED_RECEIPTS);
        next.validate()?;
        persist_state_atomic(&self.path, &next)?;
        *state = next;

        Ok(AcceptOutcome {
            receipt,
            replayed: false,
        })
    }

    /// Transition the active transaction and atomically commit the updated state.
    pub fn transition(
        &self,
        transaction_id: &str,
        update: TransactionUpdate,
    ) -> NetworkResult<NetworkTransaction> {
        let mut state = self.lock_state()?;
        let mut next = state.clone();

        let Some(current) = next.transaction.current.as_mut() else {
            return Err(NetworkError::Invalid {
                message: format!("transaction_not_found: {transaction_id}"),
            });
        };
        if current.transaction_id != transaction_id {
            return Err(NetworkError::Invalid {
                message: format!("transaction_not_found: {transaction_id}"),
            });
        }

        next.source_revision = next.source_revision.saturating_add(1);
        current.status = update.status;
        current.stage = update.stage;
        current.updated_at = update.updated_at;
        current.source_revision = next.source_revision;
        current.recovery_action = update.recovery_action;
        if let Some(ap_val) = update.ap_validated {
            current.rescue.ap_validated = ap_val;
        }
        current.error = update.error;
        if let Some(desired) = update.desired {
            current.desired = desired.clone();
            if update.publish_desired {
                next.desired = desired;
            }
        }
        let updated_tx = current.clone();

        if let Some(saved) = update.saved {
            next.saved = saved;
        }
        if let Some(verified) = update.verified {
            next.verified = verified;
        }
        if let Some(candidate_slot) = update.retained_candidate {
            next.retained_candidate = candidate_slot;
        }
        if update.status.is_terminal() {
            next.transaction.latest = Some(updated_tx.clone());
            next.transaction.current = None;
        }

        prune_receipts(&mut next, MAX_RETAINED_RECEIPTS);
        next.validate()?;
        persist_state_atomic(&self.path, &next)?;
        *state = next;

        Ok(updated_tx)
    }

    fn lock_state(&self) -> NetworkResult<std::sync::MutexGuard<'_, PersistedNetworkState>> {
        self.inner.lock().map_err(|_| NetworkError::Backend {
            message: "network state mutex poisoned".to_owned(),
        })
    }
}

/// Prune receipts down to `max_entries`, evicting oldest revisions first while protecting
/// receipts belonging to the current or latest transaction.
fn prune_receipts(state: &mut PersistedNetworkState, max_entries: usize) {
    if state.receipts.len() <= max_entries {
        return;
    }
    let mut protected = HashSet::new();
    if let Some(cur) = &state.transaction.current {
        protected.insert(cur.transaction_id.clone());
    }
    if let Some(latest) = &state.transaction.latest {
        protected.insert(latest.transaction_id.clone());
    }

    while state.receipts.len() > max_entries {
        let remove_idx = state
            .receipts
            .iter()
            .enumerate()
            .filter(|(_, r)| !protected.contains(&r.receipt.transaction.transaction_id))
            .min_by_key(|(_, r)| r.source_revision)
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        state.receipts.remove(remove_idx);
    }
}

fn persist_state_atomic(path: &Path, state: &PersistedNetworkState) -> NetworkResult<()> {
    state.validate()?;
    let value = serde_json::to_value(state).map_err(|err| NetworkError::Backend {
        message: format!("failed to serialize network state: {err}"),
    })?;
    if contains_forbidden_secret_key(&value) {
        return Err(NetworkError::Invalid {
            message: "refusing to persist network state containing secret key".to_owned(),
        });
    }
    let mut bytes = serde_json::to_vec_pretty(&value).map_err(|err| NetworkError::Backend {
        message: format!("failed to encode network state JSON: {err}"),
    })?;
    bytes.push(b'\n');
    if bytes.len() > MAX_STATE_FILE_BYTES {
        return Err(NetworkError::Invalid {
            message: format!(
                "network state size {} exceeds hard limit {MAX_STATE_FILE_BYTES}",
                bytes.len()
            ),
        });
    }
    write_bytes_atomic(path, &bytes, 0o600, true).map_err(|err| NetworkError::Backend {
        message: format!("failed to persist network state {}: {err}", path.display()),
    })
}

/// Format Unix epoch seconds as RFC 3339 / ISO-8601 UTC (`YYYY-MM-DDTHH:MM:SSZ`).
pub fn format_utc_iso8601(unix_secs: u64) -> String {
    let days = (unix_secs / 86_400) as i64;
    let rem = unix_secs % 86_400;
    let hour = rem / 3_600;
    let minute = (rem % 3_600) / 60;
    let second = rem % 60;

    // Howard Hinnant's civil-from-days algorithm.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    format!("{year:04}-{m:02}-{d:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Return current UTC timestamp formatted as `YYYY-MM-DDTHH:MM:SSZ`.
pub fn utc_now_iso8601() -> String {
    let secs = miro_ego_core::sys::clock::realtime_ns()
        .map(|ns| ns / 1_000_000_000)
        .unwrap_or(0);
    format_utc_iso8601(secs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_state_path(tag: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir()
            .join(format!(
                "miro-ego-state-{tag}-{}-{unique}",
                std::process::id()
            ))
            .join("network-state.json")
    }

    #[test]
    fn enforces_bounded_receipt_cap_of_64_and_reloads_cleanly() {
        let path = temp_state_path("cap64");
        let store = NetworkStateStore::open_or_init(&path).unwrap();

        for i in 0..80_u32 {
            let key = format!("idem-{i}");
            let fp = hex_encode(Sha256::digest(key.as_bytes()).as_slice());
            let accepted = store
                .accept_transaction(
                    TransactionOperation::Apply,
                    DesiredState::ethernet_dhcp(),
                    "principal-1",
                    &key,
                    &fp,
                    format_utc_iso8601(1_730_000_000 + u64::from(i)),
                    true,
                    None,
                )
                .unwrap();
            assert!(!accepted.replayed);

            // Replaying same key+fingerprint succeeds idempotently.
            let replayed = store
                .accept_transaction(
                    TransactionOperation::Apply,
                    DesiredState::ethernet_dhcp(),
                    "principal-1",
                    &key,
                    &fp,
                    format_utc_iso8601(1_730_000_000 + u64::from(i)),
                    true,
                    None,
                )
                .unwrap();
            assert!(replayed.replayed);
            assert_eq!(
                replayed.receipt.transaction.transaction_id,
                accepted.receipt.transaction.transaction_id
            );

            // Different fingerprint with same idempotency key fails with conflict.
            let bad_fp = "f".repeat(64);
            assert!(
                store
                    .accept_transaction(
                        TransactionOperation::Apply,
                        DesiredState::hotspot(),
                        "principal-1",
                        &key,
                        &bad_fp,
                        format_utc_iso8601(1_730_000_000 + u64::from(i)),
                        true,
                        None,
                    )
                    .is_err()
            );

            // Complete transaction so next one can run.
            store
                .transition(
                    &accepted.receipt.transaction.transaction_id,
                    TransactionUpdate {
                        status: TransactionStatus::Committed,
                        stage: TransactionStage::Committed,
                        updated_at: format_utc_iso8601(1_730_000_001 + u64::from(i)),
                        recovery_action: RecoveryAction::ReconnectTargetLan,
                        ap_validated: Some(false),
                        error: None,
                        desired: None,
                        publish_desired: false,
                        saved: Some(true),
                        verified: Some(true),
                        retained_candidate: Some(None),
                    },
                )
                .unwrap();
        }

        assert_eq!(store.receipt_count().unwrap(), MAX_RETAINED_RECEIPTS);
        let file_bytes = fs::metadata(&path).unwrap().len() as usize;
        assert!(file_bytes <= MAX_STATE_FILE_BYTES);

        // Reload from disk verifies persistence and bounds.
        let reloaded = NetworkStateStore::open_or_init(&path).unwrap();
        assert_eq!(reloaded.receipt_count().unwrap(), MAX_RETAINED_RECEIPTS);
        assert!(reloaded.snapshot().unwrap().verified);

        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn formats_known_utc_timestamps_accurately() {
        assert_eq!(format_utc_iso8601(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_utc_iso8601(1_709_210_096), "2024-02-29T12:34:56Z");
    }
}
