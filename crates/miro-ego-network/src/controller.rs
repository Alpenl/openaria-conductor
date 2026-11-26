//! Authoritative network controller, transaction state machine, rescue hotspot failover,
//! watchdog-mode evaluator, and ephemeral single-use Wi-Fi credential store.

use crate::mdns::{AVAHI_SERVICE_FILENAME, AvahiServiceRecord, DEFAULT_MDNS_PORT, MdnsScheme};
use crate::nmcli::{
    DEFAULT_ACTIVATION_WAIT_SECS, DEFAULT_HOTSPOT_IPV4_CIDR, NmcliBackend, ProfileSpec,
    RescueHotspotInfo, StaticIpv4Config, WifiScanEntry, WifiSecurity, managed_profile_name,
    validate_wifi_psk, validate_wifi_ssid,
};
use crate::state::{
    CredentialState, DesiredEthernet, DesiredMode, DesiredState, EthernetAddressing,
    NetworkDeviceDeadline, NetworkStateStore, RecoveryAction, RetainedCandidate, StateSnapshot,
    TransactionError, TransactionOperation, TransactionReceipt, TransactionStage,
    TransactionStatus, TransactionUpdate, TransactionWindow, format_utc_iso8601, utc_now_iso8601,
};
use crate::{NetworkController, NetworkError, NetworkResult, StationCredentials, WifiStatus};
use miro_ego_core::ids::{hex_encode, new_uuid_v4};
use miro_ego_core::locks::{LockMode, acquire_network_operation_lock};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub const NETWORK_CONTROLLER_STATUS_SCHEMA: &str = "miro-ego.network-controller-status.v1";
pub const NETWORK_SCAN_SCHEMA: &str = "miro-ego.network-scan.v1";
pub const NETWORK_CREDENTIAL_RECEIPT_SCHEMA: &str = "miro-ego.network-credential-receipt.v1";
pub const DEFAULT_CREDENTIAL_TTL_SECS: u64 = 60;
pub const MAX_CREDENTIAL_STORE_ENTRIES: usize = 32;

/// Watchdog mode decision consumed by `/usr/local/sbin/miro-ego-wifi-watchdog`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WatchdogMode {
    #[serde(rename = "wifi-client")]
    WifiClient,
    #[serde(rename = "hotspot")]
    Hotspot,
    #[serde(rename = "ethernet-dhcp")]
    EthernetDhcp,
    #[serde(rename = "ethernet-static")]
    EthernetStatic,
    #[serde(rename = "defer")]
    Defer,
}

impl WatchdogMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WifiClient => "wifi-client",
            Self::Hotspot => "hotspot",
            Self::EthernetDhcp => "ethernet-dhcp",
            Self::EthernetStatic => "ethernet-static",
            Self::Defer => "defer",
        }
    }
}

impl fmt::Display for WatchdogMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Evaluate `watchdog-mode` from an authoritative state snapshot.
///
/// Mirrors the `miro-ego-wifi-watchdog` contract:
/// - `hotspot`, `ethernet-dhcp`, `ethernet-static` are returned directly.
/// - `wifi-client` returns `defer` whenever a transaction is active OR the latest transaction
///   ended in `rescued`/`failed` so the watchdog does not tear down the rescue AP.
pub fn compute_watchdog_mode(snapshot: &StateSnapshot) -> WatchdogMode {
    match snapshot.desired.mode {
        DesiredMode::Hotspot => WatchdogMode::Hotspot,
        DesiredMode::EthernetDhcp => WatchdogMode::EthernetDhcp,
        DesiredMode::EthernetStatic => WatchdogMode::EthernetStatic,
        DesiredMode::WifiClient => {
            if snapshot.transaction.current.is_some() {
                return WatchdogMode::Defer;
            }
            if let Some(latest) = &snapshot.transaction.latest {
                if matches!(
                    latest.status,
                    TransactionStatus::Rescued | TransactionStatus::Failed
                ) {
                    return WatchdogMode::Defer;
                }
            }
            WatchdogMode::WifiClient
        }
    }
}

/// Receipt returned when storing a short-lived single-use Wi-Fi passphrase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialReceipt {
    pub schema: String,
    pub credential_ref: String,
    pub issued_at: String,
    pub expires_at: String,
    pub ttl_seconds: u64,
    pub single_use: bool,
}

struct StoredCredential {
    passphrase: String,
    expires_at: Instant,
}

/// In-memory ephemeral single-use credential vault (passphrases never hit disk).
pub struct EphemeralCredentialStore {
    ttl: Duration,
    entries: Mutex<HashMap<String, StoredCredential>>,
}

impl EphemeralCredentialStore {
    pub fn new(ttl_seconds: u64) -> Self {
        let clamped = ttl_seconds.clamp(1, 120);
        Self {
            ttl: Duration::from_secs(clamped),
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub fn issue(&self, passphrase: &str) -> NetworkResult<CredentialReceipt> {
        validate_wifi_psk(passphrase)?;
        let now_inst = Instant::now();
        let mut map = self.entries.lock().map_err(|_| NetworkError::Backend {
            message: "credential store mutex poisoned".to_owned(),
        })?;
        map.retain(|_, v| v.expires_at > now_inst);
        if map.len() >= MAX_CREDENTIAL_STORE_ENTRIES {
            return Err(NetworkError::Busy {
                message: "credential_store_full: too many pending credentials".to_owned(),
            });
        }
        let credential_ref = format!("cred-{}", new_uuid_v4().replace('-', ""));
        map.insert(
            credential_ref.clone(),
            StoredCredential {
                passphrase: passphrase.to_owned(),
                expires_at: now_inst + self.ttl,
            },
        );
        let now_secs = miro_ego_core::sys::clock::realtime_ns()
            .map(|ns| ns / 1_000_000_000)
            .unwrap_or(0);
        let ttl_secs = self.ttl.as_secs();
        Ok(CredentialReceipt {
            schema: NETWORK_CREDENTIAL_RECEIPT_SCHEMA.to_owned(),
            credential_ref,
            issued_at: format_utc_iso8601(now_secs),
            expires_at: format_utc_iso8601(now_secs.saturating_add(ttl_secs)),
            ttl_seconds: ttl_secs,
            single_use: true,
        })
    }

    /// Consume a single-use credential reference.
    pub fn consume(&self, credential_ref: &str) -> NetworkResult<String> {
        let now_inst = Instant::now();
        let mut map = self.entries.lock().map_err(|_| NetworkError::Backend {
            message: "credential store mutex poisoned".to_owned(),
        })?;
        let Some(entry) = map.remove(credential_ref) else {
            return Err(NetworkError::Invalid {
                message: "credential_ref_invalid: credential reference is unknown or consumed"
                    .to_owned(),
            });
        };
        if entry.expires_at <= now_inst {
            return Err(NetworkError::Invalid {
                message: "credential_ref_expired: credential reference has expired".to_owned(),
            });
        }
        Ok(entry.passphrase)
    }

    pub fn clear(&self) {
        if let Ok(mut map) = self.entries.lock() {
            map.clear();
        }
    }
}

/// Desired Wi-Fi client specification for `apply` requests (uses opaque `credential_ref`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplyWifiClient {
    pub ssid: String,
    pub security: WifiSecurity,
    #[serde(default)]
    pub credential_ref: Option<String>,
}

/// Desired target state submitted to `apply`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplyDesiredState {
    pub mode: DesiredMode,
    #[serde(default)]
    pub wifi_client: Option<ApplyWifiClient>,
    #[serde(default)]
    pub ethernet: Option<DesiredEthernet>,
}

/// Authoritative mutation capability projection matching OpenAPI `NetworkMutationCapability`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationCapability {
    pub enabled: bool,
    pub disabled_reason: Option<String>,
    pub operations: Vec<String>,
    pub idempotency_key_required: bool,
    pub secret_handling: String,
    pub active_state_policy: String,
}

impl MutationCapability {
    pub fn enabled_default() -> Self {
        Self {
            enabled: true,
            disabled_reason: None,
            operations: vec!["apply".to_owned(), "retry".to_owned(), "forget".to_owned()],
            idempotency_key_required: true,
            secret_handling: "opaque_credential_reference_only".to_owned(),
            active_state_policy: "idle_only".to_owned(),
        }
    }
}

/// Secret-free controller status returned by RPC `status`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControllerStatus {
    pub schema: String,
    pub authority_epoch: String,
    pub source_revision: u64,
    pub saved: bool,
    pub verified: bool,
    pub desired: DesiredState,
    pub transaction: TransactionWindow,
    pub capability: MutationCapability,
}

/// Redacted Wi-Fi scan response matching OpenAPI `NetworkScanResult`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanResponse {
    pub schema: String,
    pub authority_epoch: String,
    pub source_revision: u64,
    pub scanned_at: String,
    pub networks: Vec<WifiScanEntry>,
}

/// Outcome of an authoritative mutation (`apply`, `retry`, `forget`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MutationOutcome {
    pub receipt: TransactionReceipt,
    pub replayed: bool,
    pub final_status: TransactionStatus,
}

/// Configuration for [`AuthoritativeNetworkController`].
#[derive(Debug, Clone)]
pub struct ControllerConfig {
    pub device_id: String,
    pub run_dir: PathBuf,
    pub avahi_dir: Option<PathBuf>,
    pub mdns_port: u16,
    pub mdns_scheme: MdnsScheme,
    pub activation_wait_secs: u64,
}

impl ControllerConfig {
    pub fn new(device_id: impl Into<String>, run_dir: impl Into<PathBuf>) -> Self {
        Self {
            device_id: device_id.into(),
            run_dir: run_dir.into(),
            avahi_dir: None,
            mdns_port: DEFAULT_MDNS_PORT,
            mdns_scheme: MdnsScheme::Http,
            activation_wait_secs: DEFAULT_ACTIVATION_WAIT_SECS,
        }
    }
}

/// Authoritative privileged network controller coordinating `nmcli`, bounded state,
/// rescue hotspot failover, and `network-operation.lock`.
pub struct AuthoritativeNetworkController {
    backend: NmcliBackend,
    state: NetworkStateStore,
    credentials: EphemeralCredentialStore,
    config: ControllerConfig,
    rescue_info: Mutex<Option<RescueHotspotInfo>>,
}

impl AuthoritativeNetworkController {
    pub fn new(
        backend: NmcliBackend,
        state: NetworkStateStore,
        config: ControllerConfig,
    ) -> NetworkResult<Self> {
        let controller = Self {
            backend,
            state,
            credentials: EphemeralCredentialStore::new(DEFAULT_CREDENTIAL_TTL_SECS),
            config,
            rescue_info: Mutex::new(None),
        };
        controller.ensure_rescue_hotspot(true)?;
        Ok(controller)
    }

    pub fn backend(&self) -> &NmcliBackend {
        &self.backend
    }

    pub fn state_store(&self) -> &NetworkStateStore {
        &self.state
    }

    /// Ensure the device rescue hotspot (`MiroEgo-XXXXXXXX`) exists, preserving any custom PSK.
    pub fn ensure_rescue_hotspot(
        &self,
        preserve_existing_psk: bool,
    ) -> NetworkResult<RescueHotspotInfo> {
        let _lock = self.acquire_op_lock()?;
        let info = self
            .backend
            .ensure_rescue_hotspot(&self.config.device_id, preserve_existing_psk)?;
        self.write_avahi_if_configured()?;
        self.state
            .set_rescue_info(info.profile_name.clone(), info.ssid.clone())?;
        if let Ok(mut guard) = self.rescue_info.lock() {
            *guard = Some(info.clone());
        }
        Ok(info)
    }

    /// Issue a short-lived single-use opaque `credential_ref` for a Wi-Fi passphrase.
    pub fn create_credential(&self, passphrase: &str) -> NetworkResult<CredentialReceipt> {
        self.credentials.issue(passphrase)
    }

    /// Return the authoritative controller status projection.
    pub fn controller_status(&self) -> NetworkResult<ControllerStatus> {
        let snap = self.state.snapshot()?;
        Ok(ControllerStatus {
            schema: NETWORK_CONTROLLER_STATUS_SCHEMA.to_owned(),
            authority_epoch: snap.authority_epoch,
            source_revision: snap.source_revision,
            saved: snap.saved,
            verified: snap.verified,
            desired: snap.desired,
            transaction: snap.transaction,
            capability: MutationCapability::enabled_default(),
        })
    }

    /// Return the `watchdog-mode` decision for `miro-ego-wifi-watchdog`.
    pub fn watchdog_mode(&self) -> NetworkResult<WatchdogMode> {
        let snap = self.state.snapshot()?;
        Ok(compute_watchdog_mode(&snap))
    }

    /// Scan Wi-Fi networks with `network-operation.lock` check and request coalescing.
    pub fn scan(&self) -> NetworkResult<ScanResponse> {
        // Nonblocking check: do not scan while capture or another network mutation holds the lock.
        {
            let _lock = self.acquire_op_lock()?;
        }
        let networks = self.backend.scan_wifi()?;
        let (authority_epoch, source_revision) = self.state.bump_scan_revision()?;
        Ok(ScanResponse {
            schema: NETWORK_SCAN_SCHEMA.to_owned(),
            authority_epoch,
            source_revision,
            scanned_at: utc_now_iso8601(),
            networks,
        })
    }

    /// Apply a desired network mode transactionally with automatic rescue-hotspot failover.
    pub fn apply(
        &self,
        principal_id: &str,
        idempotency_key: &str,
        desired_input: &ApplyDesiredState,
    ) -> NetworkResult<MutationOutcome> {
        validate_principal_and_key(principal_id, idempotency_key)?;
        let fingerprint = compute_request_fingerprint("apply", desired_input)?;

        if let Some(receipt) =
            self.state
                .replay_receipt(principal_id, idempotency_key, &fingerprint)?
        {
            let final_status = self
                .state
                .snapshot()?
                .transaction
                .latest
                .as_ref()
                .map(|t| t.status)
                .unwrap_or(receipt.transaction.status);
            return Ok(MutationOutcome {
                receipt,
                replayed: true,
                final_status,
            });
        }

        let _lock = self.acquire_op_lock()?;
        let (normalized_desired, maybe_spec) = self.resolve_apply_spec(desired_input)?;

        let candidate_profile = maybe_spec.as_ref().map(|spec| {
            managed_profile_name(
                spec.mode_slug(),
                &format!("{idempotency_key}:{fingerprint}"),
            )
        });

        // Write candidate profile before accepting the transaction so validation errors fail fast.
        if let (Some(profile), Some(spec)) = (&candidate_profile, &maybe_spec) {
            self.backend.write_profile(profile, spec)?;
            self.write_avahi_if_configured()?;
        }

        let accepted = self.state.accept_transaction(
            TransactionOperation::Apply,
            normalized_desired.clone(),
            principal_id,
            idempotency_key,
            &fingerprint,
            utc_now_iso8601(),
            true,
            None,
        )?;
        if accepted.replayed {
            return Ok(MutationOutcome {
                final_status: accepted.receipt.transaction.status,
                receipt: accepted.receipt,
                replayed: true,
            });
        }

        let tx_id = accepted.receipt.transaction.transaction_id.clone();
        let final_status = match (candidate_profile, maybe_spec) {
            (None, None) => self.execute_rescue_commit(&tx_id)?,
            (Some(profile), Some(spec)) => {
                self.execute_candidate_activation(&tx_id, &profile, &spec, &normalized_desired)?
            }
            _ => {
                return Err(NetworkError::Backend {
                    message: "inconsistent candidate profile state".to_owned(),
                });
            }
        };

        Ok(MutationOutcome {
            receipt: accepted.receipt,
            replayed: false,
            final_status,
        })
    }

    /// Retry the latest `rescued` or `failed` transaction using its retained candidate profile.
    pub fn retry(
        &self,
        principal_id: &str,
        idempotency_key: &str,
        source_transaction_id: &str,
    ) -> NetworkResult<MutationOutcome> {
        validate_principal_and_key(principal_id, idempotency_key)?;
        let fingerprint = compute_request_fingerprint("retry", &source_transaction_id)?;

        if let Some(receipt) =
            self.state
                .replay_receipt(principal_id, idempotency_key, &fingerprint)?
        {
            let final_status = self
                .state
                .snapshot()?
                .transaction
                .latest
                .as_ref()
                .map(|t| t.status)
                .unwrap_or(receipt.transaction.status);
            return Ok(MutationOutcome {
                receipt,
                replayed: true,
                final_status,
            });
        }

        let _lock = self.acquire_op_lock()?;
        let full = self.state.full_state()?;
        let Some(latest) = &full.transaction.latest else {
            return Err(NetworkError::Invalid {
                message: "transaction_not_found: no previous transaction to retry".to_owned(),
            });
        };
        if latest.transaction_id != source_transaction_id {
            return Err(NetworkError::Invalid {
                message: "transaction_not_found: requested transaction is not retained".to_owned(),
            });
        }
        if !matches!(
            latest.status,
            TransactionStatus::Rescued | TransactionStatus::Failed
        ) {
            return Err(NetworkError::Invalid {
                message: "transaction_not_retryable: transaction did not fail or rescue".to_owned(),
            });
        }
        let Some(retained) = full.retained_candidate else {
            return Err(NetworkError::Invalid {
                message: "transaction_not_retryable: no retained candidate profile".to_owned(),
            });
        };

        let accepted = self.state.accept_transaction(
            TransactionOperation::Retry,
            retained.desired.clone(),
            principal_id,
            idempotency_key,
            &fingerprint,
            utc_now_iso8601(),
            true,
            Some(retained.clone()),
        )?;
        if accepted.replayed {
            return Ok(MutationOutcome {
                final_status: accepted.receipt.transaction.status,
                receipt: accepted.receipt,
                replayed: true,
            });
        }

        let tx_id = accepted.receipt.transaction.transaction_id.clone();
        let spec = spec_from_retained_desired(&retained.desired)?;
        let final_status = self.execute_candidate_activation(
            &tx_id,
            &retained.profile_name,
            &spec,
            &retained.desired,
        )?;
        Ok(MutationOutcome {
            receipt: accepted.receipt,
            replayed: false,
            final_status,
        })
    }

    /// Forget saved Wi-Fi client profiles and return safely to the rescue hotspot.
    pub fn forget(
        &self,
        principal_id: &str,
        idempotency_key: &str,
    ) -> NetworkResult<MutationOutcome> {
        validate_principal_and_key(principal_id, idempotency_key)?;
        let fingerprint = compute_request_fingerprint("forget", &serde_json::json!({}))?;

        if let Some(receipt) =
            self.state
                .replay_receipt(principal_id, idempotency_key, &fingerprint)?
        {
            let final_status = self
                .state
                .snapshot()?
                .transaction
                .latest
                .as_ref()
                .map(|t| t.status)
                .unwrap_or(receipt.transaction.status);
            return Ok(MutationOutcome {
                receipt,
                replayed: true,
                final_status,
            });
        }

        let _lock = self.acquire_op_lock()?;
        let accepted = self.state.accept_transaction(
            TransactionOperation::Forget,
            DesiredState::hotspot(),
            principal_id,
            idempotency_key,
            &fingerprint,
            utc_now_iso8601(),
            false,
            None,
        )?;
        if accepted.replayed {
            return Ok(MutationOutcome {
                final_status: accepted.receipt.transaction.status,
                receipt: accepted.receipt,
                replayed: true,
            });
        }

        let tx_id = accepted.receipt.transaction.transaction_id.clone();
        self.state.transition(
            &tx_id,
            TransactionUpdate {
                status: TransactionStatus::Running,
                stage: TransactionStage::Forgetting,
                updated_at: utc_now_iso8601(),
                deadline: None,
                recovery_action: RecoveryAction::AwaitDevice,
                ap_validated: None,
                error: None,
                desired: None,
                publish_desired: false,
                saved: None,
                verified: None,
                retained_candidate: None,
            },
        )?;

        let result = (|| -> NetworkResult<()> {
            self.activate_rescue_hotspot_locked()?;
            let _ = self.backend.remove_managed_wifi_client_profiles()?;
            self.credentials.clear();
            self.state.set_lkg_profile(true, None)?;
            Ok(())
        })();

        let final_status = match result {
            Ok(()) => {
                self.state.transition(
                    &tx_id,
                    TransactionUpdate {
                        status: TransactionStatus::Committed,
                        stage: TransactionStage::Forgotten,
                        updated_at: utc_now_iso8601(),
                        deadline: None,
                        recovery_action: RecoveryAction::ReconnectRescueAp,
                        ap_validated: Some(true),
                        error: None,
                        desired: Some(DesiredState::hotspot()),
                        publish_desired: true,
                        saved: Some(false),
                        verified: Some(false),
                        retained_candidate: Some(None),
                    },
                )?;
                TransactionStatus::Committed
            }
            Err(err) => self.fallback_to_rescue(&tx_id, &err, None)?,
        };

        Ok(MutationOutcome {
            receipt: accepted.receipt,
            replayed: false,
            final_status,
        })
    }

    fn execute_rescue_commit(&self, tx_id: &str) -> NetworkResult<TransactionStatus> {
        self.state.transition(
            tx_id,
            TransactionUpdate {
                status: TransactionStatus::Running,
                stage: TransactionStage::Prepared,
                updated_at: utc_now_iso8601(),
                deadline: None,
                recovery_action: RecoveryAction::AwaitDevice,
                ap_validated: None,
                error: None,
                desired: None,
                publish_desired: false,
                saved: None,
                verified: None,
                retained_candidate: None,
            },
        )?;

        match self.activate_rescue_hotspot_locked() {
            Ok(()) => {
                self.state.transition(
                    tx_id,
                    TransactionUpdate {
                        status: TransactionStatus::Running,
                        stage: TransactionStage::ApReady,
                        updated_at: utc_now_iso8601(),
                        deadline: None,
                        recovery_action: RecoveryAction::AwaitDevice,
                        ap_validated: Some(true),
                        error: None,
                        desired: None,
                        publish_desired: false,
                        saved: None,
                        verified: None,
                        retained_candidate: None,
                    },
                )?;
                self.state.transition(
                    tx_id,
                    TransactionUpdate {
                        status: TransactionStatus::Committed,
                        stage: TransactionStage::Committed,
                        updated_at: utc_now_iso8601(),
                        deadline: None,
                        recovery_action: RecoveryAction::ReconnectRescueAp,
                        ap_validated: Some(true),
                        error: None,
                        desired: Some(DesiredState::hotspot()),
                        publish_desired: true,
                        saved: Some(false),
                        verified: Some(false),
                        retained_candidate: Some(None),
                    },
                )?;
                Ok(TransactionStatus::Committed)
            }
            Err(err) => self.fallback_to_rescue(tx_id, &err, None),
        }
    }

    fn validate_rescue_profile_ready_locked(&self) -> NetworkResult<()> {
        let info = {
            let existing = self.rescue_info.lock().ok().and_then(|g| g.clone());
            match existing {
                Some(info) => info,
                None => {
                    let ensured = self
                        .backend
                        .ensure_rescue_hotspot(&self.config.device_id, true)?;
                    if let Ok(mut guard) = self.rescue_info.lock() {
                        *guard = Some(ensured.clone());
                    }
                    ensured
                }
            }
        };
        let path = self.backend.profile_path(&info.profile_name);
        let meta = std::fs::symlink_metadata(&path).map_err(|err| NetworkError::Backend {
            message: format!(
                "rescue_ap_unavailable: rescue profile {} missing: {err}",
                info.profile_name
            ),
        })?;
        if !meta.file_type().is_file() {
            return Err(NetworkError::Backend {
                message: format!(
                    "rescue_ap_unavailable: rescue profile {} is not a regular file",
                    info.profile_name
                ),
            });
        }
        Ok(())
    }

    fn execute_candidate_activation(
        &self,
        tx_id: &str,
        profile_name: &str,
        spec: &ProfileSpec,
        desired: &DesiredState,
    ) -> NetworkResult<TransactionStatus> {
        self.state.transition(
            tx_id,
            TransactionUpdate {
                status: TransactionStatus::Running,
                stage: TransactionStage::Prepared,
                updated_at: utc_now_iso8601(),
                deadline: None,
                recovery_action: RecoveryAction::AwaitDevice,
                ap_validated: Some(false),
                error: None,
                desired: None,
                publish_desired: false,
                saved: None,
                verified: None,
                retained_candidate: None,
            },
        )?;

        if let Err(err) = self.validate_rescue_profile_ready_locked() {
            let retained = RetainedCandidate {
                transaction_id: tx_id.to_owned(),
                profile_name: profile_name.to_owned(),
                desired: desired.clone(),
            };
            return self.fallback_to_rescue(tx_id, &err, Some(retained));
        }

        self.state.transition(
            tx_id,
            TransactionUpdate {
                status: TransactionStatus::Running,
                stage: TransactionStage::ApReady,
                updated_at: utc_now_iso8601(),
                deadline: None,
                recovery_action: RecoveryAction::AwaitDevice,
                ap_validated: Some(true),
                error: None,
                desired: None,
                publish_desired: false,
                saved: None,
                verified: None,
                retained_candidate: None,
            },
        )?;

        let wait_secs = self.config.activation_wait_secs.clamp(1, 10);
        let now_mono_ns = miro_ego_core::sys::clock::monotonic_ns().unwrap_or(0);
        let deadline_ns = now_mono_ns.saturating_add(wait_secs.saturating_mul(1_000_000_000));
        let activating_deadline =
            NetworkDeviceDeadline::device_monotonic(deadline_ns, wait_secs as f64);

        self.state.transition(
            tx_id,
            TransactionUpdate {
                status: TransactionStatus::Running,
                stage: TransactionStage::Activating,
                updated_at: utc_now_iso8601(),
                deadline: Some(activating_deadline),
                recovery_action: RecoveryAction::AwaitDevice,
                ap_validated: Some(true),
                error: None,
                desired: None,
                publish_desired: false,
                saved: None,
                verified: None,
                retained_candidate: None,
            },
        )?;

        let iface = spec.interface_name(
            self.backend.wifi_interface(),
            self.backend.ethernet_interface(),
        );
        let is_wifi_client = matches!(spec, ProfileSpec::WifiClient { .. });

        let outcome = (|| -> NetworkResult<()> {
            self.backend.activate_connection(
                profile_name,
                iface,
                self.config.activation_wait_secs,
                is_wifi_client,
            )?;
            let observed_ns = miro_ego_core::sys::clock::monotonic_ns().unwrap_or(now_mono_ns);
            let remaining_secs = (deadline_ns.saturating_sub(observed_ns) as f64) / 1_000_000_000.0;
            let verifying_deadline =
                NetworkDeviceDeadline::device_monotonic(deadline_ns, remaining_secs);
            self.state.transition(
                tx_id,
                TransactionUpdate {
                    status: TransactionStatus::Running,
                    stage: TransactionStage::Verifying,
                    updated_at: utc_now_iso8601(),
                    deadline: Some(verifying_deadline),
                    recovery_action: RecoveryAction::AwaitDevice,
                    ap_validated: Some(true),
                    error: None,
                    desired: None,
                    publish_desired: false,
                    saved: None,
                    verified: None,
                    retained_candidate: None,
                },
            )?;
            let _ = self.backend.verify_activated_profile(profile_name, spec)?;
            Ok(())
        })();

        match outcome {
            Ok(()) => {
                self.state
                    .set_lkg_profile(is_wifi_client, Some(profile_name.to_owned()))?;
                self.state.transition(
                    tx_id,
                    TransactionUpdate {
                        status: TransactionStatus::Committed,
                        stage: TransactionStage::Committed,
                        updated_at: utc_now_iso8601(),
                        deadline: None,
                        recovery_action: RecoveryAction::ReconnectTargetLan,
                        ap_validated: Some(true),
                        error: None,
                        desired: Some(desired.clone()),
                        publish_desired: true,
                        saved: Some(true),
                        verified: Some(true),
                        retained_candidate: Some(None),
                    },
                )?;
                Ok(TransactionStatus::Committed)
            }
            Err(err) => {
                let retained = RetainedCandidate {
                    transaction_id: tx_id.to_owned(),
                    profile_name: profile_name.to_owned(),
                    desired: desired.clone(),
                };
                self.fallback_to_rescue(tx_id, &err, Some(retained))
            }
        }
    }

    fn fallback_to_rescue(
        &self,
        tx_id: &str,
        cause: &NetworkError,
        retained: Option<RetainedCandidate>,
    ) -> NetworkResult<TransactionStatus> {
        let _ = self.state.transition(
            tx_id,
            TransactionUpdate {
                status: TransactionStatus::Running,
                stage: TransactionStage::FallingBack,
                updated_at: utc_now_iso8601(),
                deadline: None,
                recovery_action: RecoveryAction::AwaitDevice,
                ap_validated: None,
                error: None,
                desired: None,
                publish_desired: false,
                saved: None,
                verified: None,
                retained_candidate: None,
            },
        );

        let rescued = self.activate_rescue_hotspot_locked().is_ok();
        let (status, stage, recovery_action) = if rescued {
            (
                TransactionStatus::Rescued,
                TransactionStage::Rescued,
                RecoveryAction::ReconnectRescueAp,
            )
        } else {
            (
                TransactionStatus::Failed,
                TransactionStage::Failed,
                RecoveryAction::ServiceRequired,
            )
        };

        self.state.transition(
            tx_id,
            TransactionUpdate {
                status,
                stage,
                updated_at: utc_now_iso8601(),
                deadline: None,
                recovery_action,
                ap_validated: Some(rescued),
                error: Some(map_transaction_error(cause)),
                desired: None,
                publish_desired: false,
                saved: None,
                verified: Some(false),
                retained_candidate: Some(retained),
            },
        )?;
        Ok(status)
    }

    fn activate_rescue_hotspot_locked(&self) -> NetworkResult<()> {
        let info = {
            let existing = self.rescue_info.lock().ok().and_then(|g| g.clone());
            match existing {
                Some(info) => info,
                None => self
                    .backend
                    .ensure_rescue_hotspot(&self.config.device_id, true)?,
            }
        };
        let hotspot_spec = ProfileSpec::Hotspot {
            ssid: info.ssid.clone(),
            psk: "placeholder-preserved".to_owned(),
            ipv4_cidr: DEFAULT_HOTSPOT_IPV4_CIDR.to_owned(),
        };
        self.backend.activate_connection(
            &info.profile_name,
            self.backend.wifi_interface(),
            self.config.activation_wait_secs,
            false,
        )?;
        let _ = self
            .backend
            .verify_activated_profile(&info.profile_name, &hotspot_spec)?;
        Ok(())
    }

    fn resolve_apply_spec(
        &self,
        input: &ApplyDesiredState,
    ) -> NetworkResult<(DesiredState, Option<ProfileSpec>)> {
        match input.mode {
            DesiredMode::Hotspot => Ok((DesiredState::hotspot(), None)),
            DesiredMode::WifiClient => {
                let Some(wifi) = &input.wifi_client else {
                    return Err(NetworkError::Invalid {
                        message: "wifi-client mode requires wifi_client object".to_owned(),
                    });
                };
                validate_wifi_ssid(&wifi.ssid)?;
                let (psk, cred_state) = match wifi.security {
                    WifiSecurity::Open => {
                        if wifi.credential_ref.is_some() {
                            return Err(NetworkError::Invalid {
                                message: "open Wi-Fi must not include credential_ref".to_owned(),
                            });
                        }
                        (None, CredentialState::Absent)
                    }
                    _ => {
                        let Some(cred_ref) = wifi.credential_ref.as_deref() else {
                            return Err(NetworkError::Invalid {
                                message: "protected Wi-Fi requires credential_ref".to_owned(),
                            });
                        };
                        let passphrase = self.credentials.consume(cred_ref)?;
                        (Some(passphrase), CredentialState::Stored)
                    }
                };
                let desired =
                    DesiredState::wifi_client(wifi.ssid.clone(), wifi.security, cred_state);
                let spec = ProfileSpec::WifiClient {
                    ssid: wifi.ssid.clone(),
                    security: wifi.security,
                    psk,
                };
                Ok((desired, Some(spec)))
            }
            DesiredMode::EthernetDhcp => Ok((
                DesiredState::ethernet_dhcp(),
                Some(ProfileSpec::EthernetDhcp),
            )),
            DesiredMode::EthernetStatic => {
                let Some(eth) = &input.ethernet else {
                    return Err(NetworkError::Invalid {
                        message: "ethernet-static requires ethernet object".to_owned(),
                    });
                };
                if eth.addressing != EthernetAddressing::Static {
                    return Err(NetworkError::Invalid {
                        message: "ethernet-static requires addressing=static".to_owned(),
                    });
                }
                let Some(static_cfg) = eth.static_ipv4.clone() else {
                    return Err(NetworkError::Invalid {
                        message: "ethernet-static requires static_ipv4 configuration".to_owned(),
                    });
                };
                static_cfg.validate()?;
                Ok((
                    DesiredState::ethernet_static(static_cfg.clone()),
                    Some(ProfileSpec::EthernetStatic(static_cfg)),
                ))
            }
        }
    }

    fn acquire_op_lock(&self) -> NetworkResult<miro_ego_core::locks::FlockGuard> {
        acquire_network_operation_lock(&self.config.run_dir, LockMode::Exclusive, true).map_err(
            |err| match err {
                miro_ego_core::CoreError::LockBusy { message } => NetworkError::Busy {
                    message: format!("capture_active: {message}"),
                },
                other => NetworkError::Core(other),
            },
        )
    }

    fn write_avahi_if_configured(&self) -> NetworkResult<()> {
        if let Some(dir) = &self.config.avahi_dir {
            let record =
                AvahiServiceRecord::standard(self.config.mdns_port, self.config.mdns_scheme);
            record.write_atomic(&dir.join(AVAHI_SERVICE_FILENAME))?;
        }
        Ok(())
    }
}

impl NetworkController for AuthoritativeNetworkController {
    fn status(&self) -> NetworkResult<WifiStatus> {
        self.backend.wifi_status()
    }

    fn start_hotspot(&self, ssid: &str) -> NetworkResult<WifiStatus> {
        validate_wifi_ssid(ssid)?;
        let _lock = self.acquire_op_lock()?;
        let profile_name = managed_profile_name("hotspot", ssid);
        let existing_path = self.backend.profile_path(&profile_name);
        let spec = ProfileSpec::Hotspot {
            ssid: ssid.to_owned(),
            psk: crate::nmcli::DEFAULT_RESCUE_HOTSPOT_PSK.to_owned(),
            ipv4_cidr: DEFAULT_HOTSPOT_IPV4_CIDR.to_owned(),
        };
        // Preserve any existing hotspot profile & PSK rather than overwriting it.
        if !existing_path.exists() {
            self.backend.write_profile(&profile_name, &spec)?;
        }
        self.backend.activate_connection(
            &profile_name,
            self.backend.wifi_interface(),
            self.config.activation_wait_secs,
            false,
        )?;
        let _ = self
            .backend
            .verify_activated_profile(&profile_name, &spec)?;
        self.backend.wifi_status()
    }

    fn connect_station(&self, credentials: &StationCredentials) -> NetworkResult<WifiStatus> {
        validate_wifi_ssid(&credentials.ssid)?;
        let security = if credentials.passphrase.is_empty() {
            WifiSecurity::Open
        } else {
            validate_wifi_psk(&credentials.passphrase)?;
            WifiSecurity::Wpa2Personal
        };
        let cred_ref = if security.requires_credential() {
            Some(
                self.create_credential(&credentials.passphrase)?
                    .credential_ref,
            )
        } else {
            None
        };
        let idem_key = format!("station-{}", new_uuid_v4());
        let outcome = self.apply(
            "local-controller",
            &idem_key,
            &ApplyDesiredState {
                mode: DesiredMode::WifiClient,
                wifi_client: Some(ApplyWifiClient {
                    ssid: credentials.ssid.clone(),
                    security,
                    credential_ref: cred_ref,
                }),
                ethernet: None,
            },
        )?;
        if outcome.final_status != TransactionStatus::Committed {
            return Err(NetworkError::Backend {
                message: format!(
                    "station connection did not commit (status={:?}; fell back to rescue hotspot)",
                    outcome.final_status
                ),
            });
        }
        self.backend.wifi_status()
    }
}

fn spec_from_retained_desired(desired: &DesiredState) -> NetworkResult<ProfileSpec> {
    match desired.mode {
        DesiredMode::Hotspot => Err(NetworkError::Invalid {
            message: "hotspot does not use a retained client candidate".to_owned(),
        }),
        DesiredMode::WifiClient => {
            let Some(wifi) = &desired.wifi_client else {
                return Err(NetworkError::Invalid {
                    message: "retained wifi_client state missing".to_owned(),
                });
            };
            Ok(ProfileSpec::WifiClient {
                ssid: wifi.ssid.clone(),
                security: wifi.security,
                psk: None,
            })
        }
        DesiredMode::EthernetDhcp => Ok(ProfileSpec::EthernetDhcp),
        DesiredMode::EthernetStatic => {
            let Some(StaticIpv4Config { .. }) = desired
                .ethernet
                .as_ref()
                .and_then(|e| e.static_ipv4.as_ref())
            else {
                return Err(NetworkError::Invalid {
                    message: "retained static_ipv4 configuration missing".to_owned(),
                });
            };
            let cfg = desired
                .ethernet
                .as_ref()
                .and_then(|e| e.static_ipv4.clone())
                .ok_or_else(|| NetworkError::Invalid {
                    message: "retained static_ipv4 configuration missing".to_owned(),
                })?;
            Ok(ProfileSpec::EthernetStatic(cfg))
        }
    }
}

fn validate_principal_and_key(principal_id: &str, idempotency_key: &str) -> NetworkResult<()> {
    let valid_token =
        |s: &str| (1..=128).contains(&s.len()) && s.bytes().all(|b| (0x21..=0x7E).contains(&b));
    if !valid_token(principal_id) {
        return Err(NetworkError::Invalid {
            message: "principal_id must be 1..=128 visible ASCII characters".to_owned(),
        });
    }
    if !valid_token(idempotency_key) {
        return Err(NetworkError::Invalid {
            message: "idempotency_key must be 1..=128 visible ASCII characters".to_owned(),
        });
    }
    Ok(())
}

fn compute_request_fingerprint<T: Serialize>(operation: &str, body: &T) -> NetworkResult<String> {
    let payload = serde_json::to_vec(&serde_json::json!({
        "operation": operation,
        "body": body,
    }))
    .map_err(|err| NetworkError::Invalid {
        message: format!("failed to serialize request fingerprint: {err}"),
    })?;
    Ok(hex_encode(Sha256::digest(&payload).as_slice()))
}

fn map_transaction_error(error: &NetworkError) -> TransactionError {
    let text = error.to_string();
    let (code, message) = if text.contains("wifi_auth_failed") {
        ("credential_rejected", "Wi-Fi credential was rejected")
    } else if text.contains("dhcp_timeout") || text.contains("network_timeout") {
        ("dhcp_timeout", "network activation exceeded the deadline")
    } else if text.contains("default_route_missing") {
        ("route_lost", "candidate did not establish a default route")
    } else if text.contains("rescue") || text.contains("hotspot_address_missing") {
        ("rescue_ap_unavailable", "rescue AP could not be validated")
    } else {
        (
            "network_manager_unavailable",
            "NetworkManager could not apply the candidate",
        )
    };
    TransactionError {
        code: code.to_owned(),
        message: message.to_owned(),
        retryable: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nmcli::{CommandOutput, CommandRunner};
    use std::fs;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(tag: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "miro-ego-ctrl-{tag}-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    struct MockNmcliState {
        active_wlan_profile: Option<String>,
        active_wlan_ip: Option<String>,
        active_wlan_route: Option<String>,
        active_eth_profile: Option<String>,
        active_eth_ip: Option<String>,
        active_eth_route: Option<String>,
        fail_client_activation: bool,
        fail_rescue_activation: bool,
        calls: Vec<Vec<String>>,
    }

    struct MockNmcli {
        inner: Mutex<MockNmcliState>,
    }

    impl MockNmcli {
        fn new(fail_client_activation: bool) -> Arc<Self> {
            Arc::new(Self {
                inner: Mutex::new(MockNmcliState {
                    active_wlan_profile: None,
                    active_wlan_ip: None,
                    active_wlan_route: None,
                    active_eth_profile: None,
                    active_eth_ip: None,
                    active_eth_route: None,
                    fail_client_activation,
                    fail_rescue_activation: false,
                    calls: Vec::new(),
                }),
            })
        }
    }

    impl CommandRunner for MockNmcli {
        fn run(
            &self,
            _program: &str,
            args: &[&str],
            _timeout: Duration,
        ) -> NetworkResult<CommandOutput> {
            let mut st = self.inner.lock().unwrap();
            st.calls
                .push(args.iter().map(|s| (*s).to_owned()).collect());

            if args == ["connection", "reload"] {
                return Ok(CommandOutput {
                    exit_code: 0,
                    stdout: String::new(),
                    stderr: String::new(),
                });
            }

            if args.contains(&"connection") && args.contains(&"up") {
                let id_idx = args.iter().position(|a| *a == "id").unwrap();
                let if_idx = args.iter().position(|a| *a == "ifname").unwrap();
                let profile = args[id_idx + 1];
                let iface = args[if_idx + 1];

                if profile.contains("hotspot") {
                    if st.fail_rescue_activation {
                        return Ok(CommandOutput {
                            exit_code: 4,
                            stdout: String::new(),
                            stderr: "hotspot failed".to_owned(),
                        });
                    }
                    st.active_wlan_profile = Some(profile.to_owned());
                    st.active_wlan_ip = Some(DEFAULT_HOTSPOT_IPV4_CIDR.to_owned());
                    st.active_wlan_route = None;
                    return Ok(CommandOutput {
                        exit_code: 0,
                        stdout: String::new(),
                        stderr: String::new(),
                    });
                }

                if st.fail_client_activation {
                    return Ok(CommandOutput {
                        exit_code: 4,
                        stdout: String::new(),
                        stderr: "Error: Secrets were required, but not provided (password)"
                            .to_owned(),
                    });
                }

                if iface == "wlan0" {
                    st.active_wlan_profile = Some(profile.to_owned());
                    st.active_wlan_ip = Some("192.168.1.50/24".to_owned());
                    st.active_wlan_route =
                        Some("dst = 0.0.0.0/0, nh = 192.168.1.1, mt = 600".to_owned());
                } else {
                    st.active_eth_profile = Some(profile.to_owned());
                    st.active_eth_ip = Some("10.0.0.20/24".to_owned());
                    st.active_eth_route =
                        Some("dst = 0.0.0.0/0, nh = 10.0.0.1, mt = 100".to_owned());
                }
                return Ok(CommandOutput {
                    exit_code: 0,
                    stdout: String::new(),
                    stderr: String::new(),
                });
            }

            if args.contains(&"device") && args.contains(&"show") {
                let iface = *args.last().unwrap();
                let (conn, ip, route) = if iface == "wlan0" {
                    (
                        st.active_wlan_profile.clone(),
                        st.active_wlan_ip.clone(),
                        st.active_wlan_route.clone(),
                    )
                } else {
                    (
                        st.active_eth_profile.clone(),
                        st.active_eth_ip.clone(),
                        st.active_eth_route.clone(),
                    )
                };
                let mut stdout = String::new();
                if let Some(c) = conn {
                    stdout.push_str("GENERAL.STATE:100 (connected)\n");
                    stdout.push_str(&format!("GENERAL.CONNECTION:{c}\n"));
                    if let Some(a) = ip {
                        stdout.push_str(&format!("IP4.ADDRESS[1]:{a}\n"));
                    }
                    if let Some(r) = route {
                        stdout.push_str(&format!("IP4.ROUTE[1]:{r}\n"));
                    }
                } else {
                    stdout.push_str("GENERAL.STATE:30 (disconnected)\n");
                    stdout.push_str("GENERAL.CONNECTION:--\n");
                }
                return Ok(CommandOutput {
                    exit_code: 0,
                    stdout,
                    stderr: String::new(),
                });
            }

            if args.contains(&"wifi") && args.contains(&"list") {
                return Ok(CommandOutput {
                    exit_code: 0,
                    stdout: "LabWiFi:WPA2:76\n".to_owned(),
                    stderr: String::new(),
                });
            }

            Ok(CommandOutput {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
            })
        }
    }

    #[test]
    fn failed_station_apply_falls_back_to_rescue_hotspot_and_defers_watchdog() {
        let root = temp_root("failover");
        let mock = MockNmcli::new(true);
        let backend = NmcliBackend::new(mock.clone(), root.join("profiles"), "wlan0", "eth0");
        let state = NetworkStateStore::open_or_init(root.join("network-state.json")).unwrap();
        let cfg = ControllerConfig::new("rdk-x5-serial-42", root.join("run"));
        let ctrl = AuthoritativeNetworkController::new(backend, state, cfg).unwrap();

        assert_eq!(ctrl.watchdog_mode().unwrap(), WatchdogMode::Hotspot);

        let cred = ctrl.create_credential("wrong-passphrase").unwrap();
        let outcome = ctrl
            .apply(
                "client-1",
                "idem-failover-1",
                &ApplyDesiredState {
                    mode: DesiredMode::WifiClient,
                    wifi_client: Some(ApplyWifiClient {
                        ssid: "LabWiFi".to_owned(),
                        security: WifiSecurity::Wpa2Personal,
                        credential_ref: Some(cred.credential_ref),
                    }),
                    ethernet: None,
                },
            )
            .unwrap();
        assert_eq!(outcome.final_status, TransactionStatus::Rescued);

        // Watchdog MUST defer when desired is wifi-client and latest tx is rescued!
        assert_eq!(ctrl.watchdog_mode().unwrap(), WatchdogMode::Defer);

        // Active link must be rescue hotspot (10.42.0.1/24).
        let status = ctrl.status().unwrap();
        assert_eq!(status.mode, crate::NetworkMode::Hotspot);
        assert_eq!(status.ipv4.as_deref(), Some("10.42.0.1"));

        // Now fix upstream and retry the retained candidate!
        mock.inner.lock().unwrap().fail_client_activation = false;
        let retry_outcome = ctrl
            .retry(
                "client-1",
                "idem-retry-1",
                &outcome.receipt.transaction.transaction_id,
            )
            .unwrap();
        assert_eq!(retry_outcome.final_status, TransactionStatus::Committed);
        assert_eq!(ctrl.watchdog_mode().unwrap(), WatchdogMode::WifiClient);
        let committed_latest = ctrl
            .controller_status()
            .unwrap()
            .transaction
            .latest
            .unwrap();
        assert!(committed_latest.rescue.ap_validated);
        assert!(committed_latest.deadline.is_none());
        let tx_json = serde_json::to_value(&committed_latest).unwrap();
        assert!(tx_json.as_object().unwrap().contains_key("deadline"));
        assert!(tx_json["deadline"].is_null());

        // Forget restores hotspot and removes managed wifi-client profiles.
        let forget_outcome = ctrl.forget("client-1", "idem-forget-1").unwrap();
        assert_eq!(forget_outcome.final_status, TransactionStatus::Committed);
        assert_eq!(ctrl.watchdog_mode().unwrap(), WatchdogMode::Hotspot);
        assert!(!ctrl.controller_status().unwrap().saved);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn watchdog_mode_matrix_covers_all_five_modes() {
        let base =
            |mode: DesiredMode, current_active: bool, latest_status: Option<TransactionStatus>| {
                let make_tx = |status: TransactionStatus| crate::state::NetworkTransaction {
                    schema: crate::state::NETWORK_TRANSACTION_SCHEMA.to_owned(),
                    authority_epoch: new_uuid_v4(),
                    source_revision: 1,
                    transaction_id: miro_ego_core::ids::new_uuid_v7(),
                    operation: TransactionOperation::Apply,
                    status,
                    stage: TransactionStage::Committed,
                    desired: DesiredState::hotspot(),
                    accepted_at: "2026-01-01T00:00:00Z".to_owned(),
                    updated_at: "2026-01-01T00:00:01Z".to_owned(),
                    deadline: None,
                    recovery_action: RecoveryAction::None,
                    rescue: Default::default(),
                    error: None,
                };
                StateSnapshot {
                    authority_epoch: new_uuid_v4(),
                    source_revision: 1,
                    saved: mode != DesiredMode::Hotspot,
                    verified: mode != DesiredMode::Hotspot,
                    desired: DesiredState {
                        mode,
                        wifi_client: None,
                        ethernet: None,
                    },
                    transaction: TransactionWindow {
                        current: current_active.then(|| make_tx(TransactionStatus::Running)),
                        latest: latest_status.map(make_tx),
                    },
                }
            };

        assert_eq!(
            compute_watchdog_mode(&base(DesiredMode::Hotspot, false, None)),
            WatchdogMode::Hotspot
        );
        assert_eq!(
            compute_watchdog_mode(&base(DesiredMode::EthernetDhcp, false, None)),
            WatchdogMode::EthernetDhcp
        );
        assert_eq!(
            compute_watchdog_mode(&base(DesiredMode::EthernetStatic, false, None)),
            WatchdogMode::EthernetStatic
        );
        assert_eq!(
            compute_watchdog_mode(&base(
                DesiredMode::WifiClient,
                false,
                Some(TransactionStatus::Committed)
            )),
            WatchdogMode::WifiClient
        );
        assert_eq!(
            compute_watchdog_mode(&base(DesiredMode::WifiClient, true, None)),
            WatchdogMode::Defer
        );
        assert_eq!(
            compute_watchdog_mode(&base(
                DesiredMode::WifiClient,
                false,
                Some(TransactionStatus::Rescued)
            )),
            WatchdogMode::Defer
        );
        assert_eq!(
            compute_watchdog_mode(&base(
                DesiredMode::WifiClient,
                false,
                Some(TransactionStatus::Failed)
            )),
            WatchdogMode::Defer
        );
    }

    #[test]
    fn respects_network_operation_lock_when_held_by_capture() {
        let root = temp_root("oplock");
        let mock = MockNmcli::new(false);
        let backend = NmcliBackend::new(mock, root.join("profiles"), "wlan0", "eth0");
        let state = NetworkStateStore::open_or_init(root.join("network-state.json")).unwrap();
        let cfg = ControllerConfig::new("rdk-x5-lock", root.join("run"));
        let ctrl = AuthoritativeNetworkController::new(backend, state, cfg).unwrap();

        let _capture_guard =
            acquire_network_operation_lock(root.join("run"), LockMode::Exclusive, true).unwrap();

        let err = ctrl.scan().unwrap_err();
        assert!(matches!(err, NetworkError::Busy { .. }));

        let err_forget = ctrl.forget("principal-1", "idem-busy").unwrap_err();
        assert!(matches!(err_forget, NetworkError::Busy { .. }));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn applies_ethernet_static_and_dhcp_modes_cleanly() {
        let root = temp_root("eth-modes");
        let mock = MockNmcli::new(false);
        let backend = NmcliBackend::new(mock.clone(), root.join("profiles"), "wlan0", "eth0");
        let state = NetworkStateStore::open_or_init(root.join("network-state.json")).unwrap();
        let cfg = ControllerConfig::new("rdk-x5-eth", root.join("run"));
        let ctrl = AuthoritativeNetworkController::new(backend, state, cfg).unwrap();

        let dhcp = ctrl
            .apply(
                "client-eth",
                "idem-eth-dhcp",
                &ApplyDesiredState {
                    mode: DesiredMode::EthernetDhcp,
                    wifi_client: None,
                    ethernet: Some(DesiredEthernet {
                        addressing: EthernetAddressing::Dhcp,
                        static_ipv4: None,
                    }),
                },
            )
            .unwrap();
        assert_eq!(dhcp.final_status, TransactionStatus::Committed);
        assert_eq!(ctrl.watchdog_mode().unwrap(), WatchdogMode::EthernetDhcp);

        let static_res = ctrl
            .apply(
                "client-eth",
                "idem-eth-static",
                &ApplyDesiredState {
                    mode: DesiredMode::EthernetStatic,
                    wifi_client: None,
                    ethernet: Some(DesiredEthernet {
                        addressing: EthernetAddressing::Static,
                        static_ipv4: Some(StaticIpv4Config {
                            address: "10.0.0.20".to_owned(),
                            prefix_length: 24,
                            gateway: Some("10.0.0.1".to_owned()),
                            dns: vec!["1.1.1.1".to_owned()],
                        }),
                    }),
                },
            )
            .unwrap();
        assert_eq!(static_res.final_status, TransactionStatus::Committed);
        assert_eq!(ctrl.watchdog_mode().unwrap(), WatchdogMode::EthernetStatic);
        let eth_latest = ctrl
            .controller_status()
            .unwrap()
            .transaction
            .latest
            .unwrap();
        assert!(eth_latest.rescue.ap_validated);
        assert!(eth_latest.deadline.is_none());

        fs::remove_dir_all(root).unwrap();
    }
}
