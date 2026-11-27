//! Authoritative network controller, transaction state machine, rescue hotspot failover,
//! watchdog-mode evaluator, and ephemeral single-use Wi-Fi credential store.

use crate::clock::{
    ClockStatusSnapshot, LinuxSystemClockBackend, SystemClockBackend, evaluate_clock_status,
    execute_clock_sync,
};
use crate::mdns::{AVAHI_SERVICE_FILENAME, AvahiServiceRecord, DEFAULT_MDNS_PORT, MdnsScheme};
use crate::nmcli::{
    DEFAULT_ACTIVATION_WAIT_SECS, DEFAULT_HOTSPOT_IPV4_CIDR, NmcliBackend, ObservedNetworkState,
    ProfileSpec, RescueHotspotInfo, StaticIpv4Config, WifiScanEntry, WifiSecurity,
    derive_rescue_identity, managed_profile_name, validate_wifi_psk, validate_wifi_ssid,
};
use crate::state::{
    CredentialState, DesiredEthernet, DesiredMode, DesiredState, EthernetAddressing,
    NetworkDeviceDeadline, NetworkStateStore, NetworkTransaction, PersistedNetworkState,
    RecoveryAction, RetainedCandidate, StateSnapshot, TransactionError, TransactionOperation,
    TransactionReceipt, TransactionStage, TransactionStatus, TransactionUpdate, TransactionWindow,
    format_utc_iso8601, utc_now_iso8601,
};
use crate::{NetworkController, NetworkError, NetworkResult, StationCredentials, WifiStatus};
use miro_ego_core::ids::{hex_encode, new_uuid_v4};
use miro_ego_core::locks::{FlockGuard, LockMode, acquire_network_operation_lock};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread::JoinHandle;
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
            if !snapshot.verified || snapshot.transaction.current.is_some() {
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
    owner_principal: Option<String>,
    passphrase: Option<String>,
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
        self.issue_inner(None, passphrase)
    }

    /// Issue a principal-bound single-use `credential_ref` that can only be consumed by
    /// `principal_id`.
    pub fn issue_for_principal(
        &self,
        principal_id: &str,
        passphrase: &str,
    ) -> NetworkResult<CredentialReceipt> {
        let valid_principal = (1..=128).contains(&principal_id.len())
            && principal_id.bytes().all(|b| (0x21..=0x7E).contains(&b));
        if !valid_principal {
            return Err(NetworkError::Invalid {
                message: "request_invalid: principal_id must be 1..=128 visible ASCII characters"
                    .to_owned(),
            });
        }
        self.issue_inner(Some(principal_id.to_owned()), passphrase)
    }

    fn issue_inner(
        &self,
        owner_principal: Option<String>,
        passphrase: &str,
    ) -> NetworkResult<CredentialReceipt> {
        validate_wifi_psk(passphrase)?;
        let now_inst = Instant::now();
        let mut map = self.entries.lock().map_err(|_| NetworkError::Backend {
            message: "credential store mutex poisoned".to_owned(),
        })?;
        map.retain(|_, v| v.expires_at > now_inst);
        let unconsumed_count = map.values().filter(|v| v.passphrase.is_some()).count();
        if unconsumed_count >= MAX_CREDENTIAL_STORE_ENTRIES {
            return Err(NetworkError::Busy {
                message: "credential_store_full: too many pending credentials".to_owned(),
            });
        }
        if map.len() >= MAX_CREDENTIAL_STORE_ENTRIES.saturating_mul(2) {
            if let Some(oldest_used_key) = map
                .iter()
                .filter(|(_, v)| v.passphrase.is_none())
                .min_by_key(|(_, v)| v.expires_at)
                .map(|(k, _)| k.clone())
            {
                map.remove(&oldest_used_key);
            }
        }
        let credential_ref = format!("cred-{}", new_uuid_v4().replace('-', ""));
        map.insert(
            credential_ref.clone(),
            StoredCredential {
                owner_principal,
                passphrase: Some(passphrase.to_owned()),
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

    /// Consume an unscoped single-use credential reference.
    pub fn consume(&self, credential_ref: &str) -> NetworkResult<String> {
        self.consume_inner(None, credential_ref)
    }

    /// Consume a single-use credential reference bound to `principal_id`.
    pub fn consume_for_principal(
        &self,
        principal_id: &str,
        credential_ref: &str,
    ) -> NetworkResult<String> {
        self.consume_inner(Some(principal_id), credential_ref)
    }

    fn consume_inner(
        &self,
        principal_id: Option<&str>,
        credential_ref: &str,
    ) -> NetworkResult<String> {
        let now_inst = Instant::now();
        let mut map = self.entries.lock().map_err(|_| NetworkError::Backend {
            message: "credential store mutex poisoned".to_owned(),
        })?;
        let Some(entry) = map.get_mut(credential_ref) else {
            return Err(NetworkError::Invalid {
                message: "credential_ref_invalid: credential reference is unknown".to_owned(),
            });
        };
        if entry.expires_at <= now_inst {
            map.remove(credential_ref);
            return Err(NetworkError::Invalid {
                message: "credential_ref_expired: credential reference has expired".to_owned(),
            });
        }
        if let Some(owner) = &entry.owner_principal {
            if principal_id != Some(owner.as_str()) {
                return Err(NetworkError::Invalid {
                    message:
                        "credential_ref_not_allowed: credential reference belongs to another principal"
                            .to_owned(),
                });
            }
        }
        let Some(passphrase) = entry.passphrase.take() else {
            return Err(NetworkError::Invalid {
                message: "credential_ref_already_used: credential reference was already consumed"
                    .to_owned(),
            });
        };
        Ok(passphrase)
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
    #[serde(default)]
    pub rescue_ssid: Option<String>,
    #[serde(default)]
    pub observed: ObservedNetworkState,
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
#[derive(Clone)]
pub struct ControllerConfig {
    pub device_id: String,
    pub run_dir: PathBuf,
    pub avahi_dir: Option<PathBuf>,
    pub mdns_port: u16,
    pub mdns_scheme: MdnsScheme,
    pub activation_wait_secs: u64,
    pub mdns_hostname: Option<String>,
    pub synchronous_mutations: bool,
    pub clock_backend: Option<Arc<dyn SystemClockBackend>>,
}

impl fmt::Debug for ControllerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControllerConfig")
            .field("device_id", &self.device_id)
            .field("run_dir", &self.run_dir)
            .field("avahi_dir", &self.avahi_dir)
            .field("mdns_port", &self.mdns_port)
            .field("mdns_scheme", &self.mdns_scheme)
            .field("activation_wait_secs", &self.activation_wait_secs)
            .field("mdns_hostname", &self.mdns_hostname)
            .field("synchronous_mutations", &self.synchronous_mutations)
            .field("has_custom_clock_backend", &self.clock_backend.is_some())
            .finish()
    }
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
            mdns_hostname: None,
            synchronous_mutations: false,
            clock_backend: None,
        }
    }

    /// Configure mutations (`apply`, `retry`, `forget`) to wait synchronously for terminal
    /// completion instead of returning `Accepted` while the worker runs in the background.
    pub fn with_synchronous_mutations(mut self, synchronous: bool) -> Self {
        self.synchronous_mutations = synchronous;
        self
    }

    pub fn with_mdns_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.mdns_hostname = Some(hostname.into());
        self
    }

    pub fn with_clock_backend(mut self, backend: Arc<dyn SystemClockBackend>) -> Self {
        self.clock_backend = Some(backend);
        self
    }
}

enum BackgroundTask {
    RescueCommit {
        tx_id: String,
    },
    CandidateActivation {
        tx_id: String,
        profile_name: String,
        spec: Box<ProfileSpec>,
        desired: Box<DesiredState>,
    },
    Forget {
        tx_id: String,
    },
}

struct WorkerSync {
    state: Mutex<WorkerState>,
    idle_cv: Condvar,
    response_flushed_cv: Condvar,
}

struct WorkerState {
    active: bool,
    response_flushed: bool,
    handle: Option<JoinHandle<()>>,
}

impl WorkerSync {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(WorkerState {
                active: false,
                response_flushed: false,
                handle: None,
            }),
            idle_cv: Condvar::new(),
            response_flushed_cv: Condvar::new(),
        })
    }
}

struct ControllerCore {
    backend: NmcliBackend,
    state: NetworkStateStore,
    credentials: EphemeralCredentialStore,
    clock: Arc<dyn SystemClockBackend>,
    clock_lock: Mutex<()>,
    config: ControllerConfig,
    rescue_info: Mutex<Option<RescueHotspotInfo>>,
    worker: Arc<WorkerSync>,
}

struct NetworkWorkerDropGuard {
    core: Arc<ControllerCore>,
    completed_cleanly: bool,
}

impl Drop for NetworkWorkerDropGuard {
    fn drop(&mut self) {
        if !self.completed_cleanly {
            let _ = self
                .core
                .state
                .recover_interrupted_transaction(false, utc_now_iso8601());
        }
        let mut wguard = self
            .core
            .worker
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        wguard.active = false;
        self.core.worker.idle_cv.notify_all();
    }
}

/// Authoritative privileged network controller coordinating `nmcli`, bounded state,
/// rescue hotspot failover, and `network-operation.lock`.
pub struct AuthoritativeNetworkController {
    core: Arc<ControllerCore>,
}

impl AuthoritativeNetworkController {
    pub fn new(
        backend: NmcliBackend,
        state: NetworkStateStore,
        mut config: ControllerConfig,
    ) -> NetworkResult<Self> {
        let clock = config
            .clock_backend
            .take()
            .unwrap_or_else(|| Arc::new(LinuxSystemClockBackend::new()));
        let controller = Self {
            core: Arc::new(ControllerCore {
                backend,
                state,
                credentials: EphemeralCredentialStore::new(DEFAULT_CREDENTIAL_TTL_SECS),
                clock,
                clock_lock: Mutex::new(()),
                config,
                rescue_info: Mutex::new(None),
                worker: WorkerSync::new(),
            }),
        };
        controller.ensure_rescue_hotspot(true)?;
        // Perform startup recovery & link reconciliation; if physical radio hardware is
        // temporarily absent at boot, interrupted transactions are still settled cleanly
        // while keeping the RPC socket available.
        let _ = controller.reconcile_startup();
        Ok(controller)
    }

    pub fn backend(&self) -> &NmcliBackend {
        &self.core.backend
    }

    pub fn state_store(&self) -> &NetworkStateStore {
        &self.core.state
    }

    /// Inspect privileged OS clock status (`adjtimex`, `boot_id`, `<run_dir>/clock-state.json`).
    pub fn clock_status(&self) -> NetworkResult<ClockStatusSnapshot> {
        let _guard = self
            .core
            .clock_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        evaluate_clock_status(self.core.clock.as_ref(), &self.core.config.run_dir)
    }

    /// Synchronize OS wall clock via `clock_settime(CLOCK_REALTIME)` and persist boot-bound
    /// calibration state under `<run_dir>/clock-state.json`.
    pub fn clock_sync(
        &self,
        unix_time_ms: u64,
        replace: bool,
    ) -> NetworkResult<ClockStatusSnapshot> {
        let _guard = self
            .core
            .clock_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        execute_clock_sync(
            self.core.clock.as_ref(),
            &self.core.config.run_dir,
            unix_time_ms,
            replace,
        )
    }

    /// Reconcile interrupted transactions and active NetworkManager links on startup.
    ///
    /// Executed under `network-operation.lock`:
    /// 1. Transitions any non-terminal `transaction.current` (`Accepted` / `Running`) to
    ///    terminal `Rescued` (after bringing up the rescue AP) or `Failed` (`current = None`).
    /// 2. Otherwise reconciles links to match `state.desired.mode`:
    ///    - `WifiClient`: activates `lkg_wifi_profile` when verified/available, falling back
    ///      automatically to the rescue hotspot (marking `verified = false`) if absent,
    ///      rescued/failed, or unreachable.
    ///    - `Hotspot`: ensures the rescue hotspot is active on `wlan0`.
    ///    - `EthernetDhcp` / `EthernetStatic`: activates `lkg_ethernet_profile` if healthy and
    ///      present, falling back automatically to the rescue hotspot if missing or unreachable.
    pub fn reconcile_startup(&self) -> NetworkResult<()> {
        let _lock = self.core.acquire_op_lock()?;
        let _info = self.core.ensure_rescue_hotspot_locked(true)?;
        let full = self.core.state.full_state()?;

        if full.transaction.current.is_some() {
            let rescue_res = self.core.activate_rescue_hotspot_locked();
            let rescued = rescue_res.is_ok();
            let _ = self
                .core
                .state
                .recover_interrupted_transaction(rescued, utc_now_iso8601())?;
            return rescue_res;
        }

        let latest_unhealthy = full.transaction.latest.as_ref().is_some_and(|tx| {
            matches!(
                tx.status,
                TransactionStatus::Rescued | TransactionStatus::Failed
            )
        });

        match full.desired.mode {
            DesiredMode::Hotspot => self.core.activate_rescue_hotspot_locked(),
            DesiredMode::WifiClient => {
                if full.verified || !latest_unhealthy {
                    if let (Some(lkg_profile), Some(wifi)) =
                        (&full.lkg_wifi_profile, &full.desired.wifi_client)
                    {
                        if self.core.backend.profile_path(lkg_profile).is_file() {
                            let spec = ProfileSpec::WifiClient {
                                ssid: wifi.ssid.clone(),
                                security: wifi.security,
                                psk: None,
                            };
                            let connected_ok = self
                                .core
                                .backend
                                .activate_connection(
                                    lkg_profile,
                                    self.core.backend.wifi_interface(),
                                    self.core.config.activation_wait_secs,
                                    true,
                                )
                                .and_then(|()| {
                                    self.core
                                        .backend
                                        .verify_activated_profile(lkg_profile, &spec)
                                        .map(|_| ())
                                });
                            if connected_ok.is_ok() {
                                let _ = self
                                    .core
                                    .backend
                                    .promote_wifi_client_autoconnect(lkg_profile);
                                if full.verified && latest_unhealthy {
                                    let _ = self
                                        .core
                                        .state
                                        .record_startup_wifi_recovered(utc_now_iso8601());
                                }
                                return Ok(());
                            }
                            if full.verified {
                                let rescue_res = self.core.activate_rescue_hotspot_locked();
                                let _ = self.core.state.record_startup_rescue_preserving_verified(
                                    rescue_res.is_ok(),
                                    utc_now_iso8601(),
                                );
                                return rescue_res;
                            }
                        }
                    }
                }
                let _ = self.core.state.set_verified(false);
                self.core.activate_rescue_hotspot_locked()
            }
            DesiredMode::EthernetDhcp => {
                self.reconcile_ethernet_startup(&full, latest_unhealthy, &ProfileSpec::EthernetDhcp)
            }
            DesiredMode::EthernetStatic => {
                if let Some(cfg) = full
                    .desired
                    .ethernet
                    .as_ref()
                    .and_then(|e| e.static_ipv4.clone())
                {
                    return self.reconcile_ethernet_startup(
                        &full,
                        latest_unhealthy,
                        &ProfileSpec::EthernetStatic(cfg),
                    );
                }
                let _ = self.core.state.set_verified(false);
                self.core.activate_rescue_hotspot_locked()
            }
        }
    }

    fn reconcile_ethernet_startup(
        &self,
        full: &PersistedNetworkState,
        latest_unhealthy: bool,
        spec: &ProfileSpec,
    ) -> NetworkResult<()> {
        if full.verified || !latest_unhealthy {
            if let Some(lkg_eth) = &full.lkg_ethernet_profile {
                if self.core.backend.profile_path(lkg_eth).is_file() {
                    let connected_ok = self
                        .core
                        .backend
                        .activate_connection(
                            lkg_eth,
                            self.core.backend.ethernet_interface(),
                            self.core.config.activation_wait_secs,
                            false,
                        )
                        .and_then(|()| {
                            self.core
                                .backend
                                .verify_activated_profile(lkg_eth, spec)
                                .map(|_| ())
                        });
                    if connected_ok.is_ok() {
                        let _ = self
                            .core
                            .backend
                            .disconnect_device(self.core.backend.wifi_interface());
                        if full.verified && latest_unhealthy {
                            let _ = self
                                .core
                                .state
                                .record_startup_wifi_recovered(utc_now_iso8601());
                        }
                        return Ok(());
                    }
                    if full.verified {
                        let rescue_res = self.core.activate_rescue_hotspot_locked();
                        let _ = self.core.state.record_startup_rescue_preserving_verified(
                            rescue_res.is_ok(),
                            utc_now_iso8601(),
                        );
                        return rescue_res;
                    }
                }
            }
        }
        let _ = self.core.state.set_verified(false);
        self.core.activate_rescue_hotspot_locked()
    }

    /// Notify any newly accepted background mutation that the RPC server has flushed its
    /// `202 Accepted` response bytes to the client socket so interface changes can begin immediately.
    pub fn notify_rpc_response_flushed(&self) {
        if let Ok(mut guard) = self.core.worker.state.lock() {
            guard.response_flushed = true;
            self.core.worker.response_flushed_cv.notify_all();
        }
    }

    /// Wait until any in-flight asynchronous network transaction finishes and return
    /// `transaction.latest` once idle.
    pub fn wait_for_idle(&self, timeout: Duration) -> NetworkResult<Option<NetworkTransaction>> {
        self.notify_rpc_response_flushed();
        let deadline = Instant::now() + timeout;
        let mut handle_to_join = None;
        {
            let mut guard = self
                .core
                .worker
                .state
                .lock()
                .map_err(|_| NetworkError::Backend {
                    message: "network worker mutex poisoned".to_owned(),
                })?;
            while guard.active {
                let now = Instant::now();
                if now >= deadline {
                    return Err(NetworkError::Busy {
                        message:
                            "controller_busy: timed out waiting for active network transaction"
                                .to_owned(),
                    });
                }
                let wait_for = deadline.saturating_duration_since(now);
                let (next_guard, _) = self
                    .core
                    .worker
                    .idle_cv
                    .wait_timeout(guard, wait_for)
                    .map_err(|_| NetworkError::Backend {
                        message: "network worker condvar poisoned".to_owned(),
                    })?;
                guard = next_guard;
            }
            if guard
                .handle
                .as_ref()
                .is_some_and(std::thread::JoinHandle::is_finished)
            {
                handle_to_join = guard.handle.take();
            }
        }
        if let Some(h) = handle_to_join {
            let _ = h.join();
        }
        Ok(self.core.state.snapshot()?.transaction.latest)
    }

    /// Ensure the device rescue hotspot (`MiroEgo-XXXXXXXX`) exists, preserving any custom PSK.
    pub fn ensure_rescue_hotspot(
        &self,
        preserve_existing_psk: bool,
    ) -> NetworkResult<RescueHotspotInfo> {
        let _lock = self.core.acquire_op_lock()?;
        self.core
            .ensure_rescue_hotspot_locked(preserve_existing_psk)
    }

    /// Issue a short-lived single-use opaque `credential_ref` for a Wi-Fi passphrase.
    pub fn create_credential(&self, passphrase: &str) -> NetworkResult<CredentialReceipt> {
        self.core.credentials.issue(passphrase)
    }

    /// Issue a principal-bound short-lived single-use opaque `credential_ref` for a Wi-Fi passphrase.
    pub fn create_credential_for_principal(
        &self,
        principal_id: &str,
        passphrase: &str,
    ) -> NetworkResult<CredentialReceipt> {
        self.core
            .credentials
            .issue_for_principal(principal_id, passphrase)
    }

    /// Return the authoritative controller status projection, including live `observed` link state
    /// and canonical `rescue_ssid`.
    pub fn controller_status(&self) -> NetworkResult<ControllerStatus> {
        let full = self.core.state.full_state()?;
        let rescue_ssid = self
            .core
            .rescue_info
            .lock()
            .ok()
            .and_then(|g| g.as_ref().map(|i| i.ssid.clone()))
            .or_else(|| full.rescue_ssid.clone())
            .or_else(|| {
                derive_rescue_identity(&self.core.config.device_id)
                    .ok()
                    .map(|(ssid, _)| ssid)
            });
        let hostname = self.core.resolve_mdns_hostname();
        let observed = self.core.backend.observed_network_state(
            rescue_ssid.as_deref(),
            &hostname,
            self.core.config.mdns_port,
            self.core.config.mdns_scheme,
        );
        Ok(ControllerStatus {
            schema: NETWORK_CONTROLLER_STATUS_SCHEMA.to_owned(),
            authority_epoch: full.authority_epoch,
            source_revision: full.source_revision,
            saved: full.saved,
            verified: full.verified,
            desired: full.desired,
            rescue_ssid,
            observed,
            transaction: full.transaction,
            capability: MutationCapability::enabled_default(),
        })
    }

    /// Return the `watchdog-mode` decision for `miro-ego-wifi-watchdog`.
    pub fn watchdog_mode(&self) -> NetworkResult<WatchdogMode> {
        let snap = self.core.state.snapshot()?;
        let mode = compute_watchdog_mode(&snap);
        if mode == WatchdogMode::WifiClient && self.core.is_rescue_hotspot_active_on_wlan() {
            return Ok(WatchdogMode::Defer);
        }
        Ok(mode)
    }

    /// Scan Wi-Fi networks while holding a shared `network-operation.lock` across the scan,
    /// without mutating or fsyncing `network-state.json`.
    pub fn scan(&self) -> NetworkResult<ScanResponse> {
        if self.core.is_busy()? {
            return Err(NetworkError::Busy {
                message: "controller_busy: another network transaction is still active".to_owned(),
            });
        }
        let _lock =
            acquire_network_operation_lock(&self.core.config.run_dir, LockMode::Shared, true)
                .map_err(|err| self.core.map_lock_error(err))?;
        let networks = self.core.backend.scan_wifi()?;
        let snap = self.core.state.snapshot()?;
        Ok(ScanResponse {
            schema: NETWORK_SCAN_SCHEMA.to_owned(),
            authority_epoch: snap.authority_epoch,
            source_revision: snap.source_revision,
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

        if let Some(receipt) = self.core.state.replay_receipt_for_operation(
            principal_id,
            TransactionOperation::Apply,
            idempotency_key,
            &fingerprint,
        )? {
            let final_status = self
                .core
                .state
                .find_transaction_status(&receipt.transaction.transaction_id)?
                .unwrap_or(receipt.transaction.status);
            return Ok(MutationOutcome {
                receipt,
                replayed: true,
                final_status,
            });
        }

        // Validate shape and verify controller is idle BEFORE consuming any single-use credential_ref!
        validate_apply_input_shape(desired_input)?;
        if self.core.is_busy()? {
            return Err(NetworkError::Busy {
                message: "controller_busy: another network transaction is still active".to_owned(),
            });
        }
        let lock_guard = self.core.acquire_op_lock()?;
        if self.core.is_busy()? {
            return Err(NetworkError::Busy {
                message: "controller_busy: another network transaction is still active".to_owned(),
            });
        }

        let (normalized_desired, maybe_spec) =
            self.core.resolve_apply_spec(principal_id, desired_input)?;
        let candidate_profile = maybe_spec.as_ref().map(|spec| {
            managed_profile_name(
                spec.mode_slug(),
                &format!("{idempotency_key}:{fingerprint}"),
            )
        });

        // Stage candidate profile with autoconnect=false before accepting the transaction so
        // validation errors fail fast and unverified Ethernet candidates never auto-activate.
        if let (Some(profile), Some(spec)) = (&candidate_profile, &maybe_spec) {
            self.core.backend.write_profile_staged(profile, spec)?;
            if let Err(err) = self.core.write_avahi_if_configured() {
                let _ = self.core.backend.delete_connection(profile);
                return Err(err);
            }
        }

        let accepted = match self.core.state.accept_transaction(
            TransactionOperation::Apply,
            normalized_desired.clone(),
            principal_id,
            idempotency_key,
            &fingerprint,
            utc_now_iso8601(),
            true,
            None,
        ) {
            Ok(acc) => acc,
            Err(err) => {
                if let Some(profile) = &candidate_profile {
                    let _ = self.core.backend.delete_connection(profile);
                }
                return Err(err);
            }
        };
        if accepted.replayed {
            let final_status = self
                .core
                .state
                .find_transaction_status(&accepted.receipt.transaction.transaction_id)?
                .unwrap_or(accepted.receipt.transaction.status);
            return Ok(MutationOutcome {
                final_status,
                receipt: accepted.receipt,
                replayed: true,
            });
        }

        let tx_id = accepted.receipt.transaction.transaction_id.clone();
        let task = match (candidate_profile, maybe_spec) {
            (None, None) => BackgroundTask::RescueCommit { tx_id },
            (Some(profile_name), Some(spec)) => BackgroundTask::CandidateActivation {
                tx_id,
                profile_name,
                spec: Box::new(spec),
                desired: Box::new(normalized_desired),
            },
            _ => {
                return Err(NetworkError::Backend {
                    message: "inconsistent candidate profile state".to_owned(),
                });
            }
        };

        self.dispatch_mutation_task(accepted.receipt, lock_guard, task)
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

        if let Some(receipt) = self.core.state.replay_receipt_for_operation(
            principal_id,
            TransactionOperation::Retry,
            idempotency_key,
            &fingerprint,
        )? {
            let final_status = self
                .core
                .state
                .find_transaction_status(&receipt.transaction.transaction_id)?
                .unwrap_or(receipt.transaction.status);
            return Ok(MutationOutcome {
                receipt,
                replayed: true,
                final_status,
            });
        }

        if self.core.is_busy()? {
            return Err(NetworkError::Busy {
                message: "controller_busy: another network transaction is still active".to_owned(),
            });
        }
        let lock_guard = self.core.acquire_op_lock()?;
        if self.core.is_busy()? {
            return Err(NetworkError::Busy {
                message: "controller_busy: another network transaction is still active".to_owned(),
            });
        }

        let full = self.core.state.full_state()?;
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
        let spec = spec_from_retained_desired(&retained.desired)?;

        let accepted = self.core.state.accept_transaction(
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
            let final_status = self
                .core
                .state
                .find_transaction_status(&accepted.receipt.transaction.transaction_id)?
                .unwrap_or(accepted.receipt.transaction.status);
            return Ok(MutationOutcome {
                final_status,
                receipt: accepted.receipt,
                replayed: true,
            });
        }

        let tx_id = accepted.receipt.transaction.transaction_id.clone();
        let task = BackgroundTask::CandidateActivation {
            tx_id,
            profile_name: retained.profile_name,
            spec: Box::new(spec),
            desired: Box::new(retained.desired),
        };
        self.dispatch_mutation_task(accepted.receipt, lock_guard, task)
    }

    /// Forget saved Wi-Fi client profiles and return safely to the rescue hotspot.
    pub fn forget(
        &self,
        principal_id: &str,
        idempotency_key: &str,
    ) -> NetworkResult<MutationOutcome> {
        validate_principal_and_key(principal_id, idempotency_key)?;
        let fingerprint = compute_request_fingerprint("forget", &serde_json::json!({}))?;

        if let Some(receipt) = self.core.state.replay_receipt_for_operation(
            principal_id,
            TransactionOperation::Forget,
            idempotency_key,
            &fingerprint,
        )? {
            let final_status = self
                .core
                .state
                .find_transaction_status(&receipt.transaction.transaction_id)?
                .unwrap_or(receipt.transaction.status);
            return Ok(MutationOutcome {
                receipt,
                replayed: true,
                final_status,
            });
        }

        if self.core.is_busy()? {
            return Err(NetworkError::Busy {
                message: "controller_busy: another network transaction is still active".to_owned(),
            });
        }
        let lock_guard = self.core.acquire_op_lock()?;
        if self.core.is_busy()? {
            return Err(NetworkError::Busy {
                message: "controller_busy: another network transaction is still active".to_owned(),
            });
        }

        let accepted = self.core.state.accept_transaction(
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
            let final_status = self
                .core
                .state
                .find_transaction_status(&accepted.receipt.transaction.transaction_id)?
                .unwrap_or(accepted.receipt.transaction.status);
            return Ok(MutationOutcome {
                final_status,
                receipt: accepted.receipt,
                replayed: true,
            });
        }

        let tx_id = accepted.receipt.transaction.transaction_id.clone();
        let task = BackgroundTask::Forget { tx_id };
        self.dispatch_mutation_task(accepted.receipt, lock_guard, task)
    }

    fn dispatch_mutation_task(
        &self,
        receipt: TransactionReceipt,
        lock_guard: FlockGuard,
        task: BackgroundTask,
    ) -> NetworkResult<MutationOutcome> {
        if self.core.config.synchronous_mutations {
            let _held = lock_guard;
            let final_status = self.core.run_task(task)?;
            return Ok(MutationOutcome {
                receipt,
                replayed: false,
                final_status,
            });
        }

        let prev_handle = {
            let mut guard = self
                .core
                .worker
                .state
                .lock()
                .map_err(|_| NetworkError::Backend {
                    message: "network worker mutex poisoned".to_owned(),
                })?;
            guard.active = true;
            guard.response_flushed = false;
            guard.handle.take()
        };
        if let Some(prev) = prev_handle {
            let _ = prev.join();
        }

        let core_clone = Arc::clone(&self.core);
        let spawn_res = std::thread::Builder::new()
            .name("miro-ego-net-tx".to_owned())
            .spawn(move || {
                let _held_lock = lock_guard;
                let mut worker_guard = NetworkWorkerDropGuard {
                    core: Arc::clone(&core_clone),
                    completed_cleanly: false,
                };
                // Wait up to 500ms for the HTTP gateway to flush the 202 Accepted reply over
                // wlan0 before tearing down the AP (woken early by `notify_response_flushed`).
                {
                    let wguard = core_clone
                        .worker
                        .state
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    if !wguard.response_flushed {
                        let _ = core_clone
                            .worker
                            .response_flushed_cv
                            .wait_timeout(wguard, Duration::from_millis(500));
                    }
                }
                let _ = core_clone.run_task(task);
                worker_guard.completed_cleanly = true;
            });

        match spawn_res {
            Ok(handle) => {
                if let Ok(mut guard) = self.core.worker.state.lock() {
                    guard.handle = Some(handle);
                }
                Ok(MutationOutcome {
                    receipt,
                    replayed: false,
                    final_status: TransactionStatus::Accepted,
                })
            }
            Err(err) => {
                if let Ok(mut guard) = self.core.worker.state.lock() {
                    guard.active = false;
                    self.core.worker.idle_cv.notify_all();
                }
                let tx_id = receipt.transaction.transaction_id.clone();
                let spawn_err = NetworkError::Backend {
                    message: format!("failed to spawn network transaction worker: {err}"),
                };
                let _ = self.core.fallback_to_rescue(&tx_id, &spawn_err, None);
                Err(spawn_err)
            }
        }
    }
}

impl Drop for AuthoritativeNetworkController {
    fn drop(&mut self) {
        self.notify_rpc_response_flushed();
        let handle = self
            .core
            .worker
            .state
            .lock()
            .ok()
            .and_then(|mut g| g.handle.take());
        if let Some(h) = handle {
            let _ = h.join();
        }
    }
}

impl ControllerCore {
    fn is_busy(&self) -> NetworkResult<bool> {
        let worker_busy = self
            .worker
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .active;
        Ok(worker_busy || self.state.has_active_transaction()?)
    }

    fn is_rescue_hotspot_active_on_wlan(&self) -> bool {
        let Ok(wlan) = self.backend.inspect_device(self.backend.wifi_interface()) else {
            return false;
        };
        wlan.connected
            && (wlan
                .connection
                .as_deref()
                .is_some_and(|c| c.contains("hotspot"))
                || wlan
                    .ipv4_addresses
                    .iter()
                    .any(|a| a == DEFAULT_HOTSPOT_IPV4_CIDR))
    }

    fn ensure_rescue_hotspot_locked(
        &self,
        preserve_existing_psk: bool,
    ) -> NetworkResult<RescueHotspotInfo> {
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

    fn resolve_mdns_hostname(&self) -> String {
        if let Some(host) = &self.config.mdns_hostname {
            if !host.trim().is_empty() {
                return host.trim().to_owned();
            }
        }
        std::fs::read_to_string("/etc/hostname")
            .ok()
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "miro-ego".to_owned())
    }

    fn run_task(&self, task: BackgroundTask) -> NetworkResult<TransactionStatus> {
        match task {
            BackgroundTask::RescueCommit { tx_id } => self.execute_rescue_commit(&tx_id),
            BackgroundTask::CandidateActivation {
                tx_id,
                profile_name,
                spec,
                desired,
            } => self.execute_candidate_activation(&tx_id, &profile_name, &spec, &desired),
            BackgroundTask::Forget { tx_id } => self.execute_forget(&tx_id),
        }
    }

    fn execute_forget(&self, tx_id: &str) -> NetworkResult<TransactionStatus> {
        self.state.transition(
            tx_id,
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
            self.cleanup_managed_ethernet_if_leaving()?;
            let keep = HashSet::new();
            let _ = self.backend.prune_managed_profiles(&keep)?;
            self.credentials.clear();
            self.state.set_lkg_profile(true, None)?;
            self.state.set_lkg_profile(false, None)?;
            Ok(())
        })();

        match result {
            Ok(()) => {
                self.state.transition(
                    tx_id,
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
                Ok(TransactionStatus::Committed)
            }
            Err(err) => self.fallback_to_rescue(tx_id, &err, None),
        }
    }

    fn cleanup_managed_ethernet_if_leaving(&self) -> NetworkResult<()> {
        let had_lkg_eth = self
            .state
            .full_state()
            .ok()
            .and_then(|s| s.lkg_ethernet_profile)
            .is_some();
        let active_managed_eth = self
            .backend
            .inspect_device(self.backend.ethernet_interface())
            .ok()
            .and_then(|snap| snap.connection)
            .is_some_and(|conn| conn.starts_with("miro-ego-ethernet-"));
        if had_lkg_eth || active_managed_eth {
            let _ = self
                .backend
                .disconnect_device(self.backend.ethernet_interface());
            self.state.set_lkg_profile(false, None)?;
        }
        Ok(())
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
                let _ = self.cleanup_managed_ethernet_if_leaving();
                let keep = HashSet::new();
                let _ = self.backend.prune_managed_profiles(&keep);
                let _ = self.state.set_lkg_profile(true, None);
                let _ = self.state.set_lkg_profile(false, None);
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
            if is_wifi_client {
                self.backend.promote_wifi_client_autoconnect(profile_name)?;
            } else {
                self.backend
                    .promote_profile_autoconnect(profile_name, spec)?;
            }
            Ok(())
        })();

        match outcome {
            Ok(()) => {
                let mut keep = HashSet::new();
                keep.insert(profile_name.to_owned());
                if is_wifi_client {
                    let _ = self.cleanup_managed_ethernet_if_leaving();
                    self.state
                        .set_lkg_profile(true, Some(profile_name.to_owned()))?;
                } else {
                    let _ = self
                        .backend
                        .disconnect_device(self.backend.wifi_interface());
                    if let Some(existing_wifi) = self
                        .state
                        .full_state()
                        .ok()
                        .and_then(|s| s.lkg_wifi_profile)
                    {
                        keep.insert(existing_wifi);
                    }
                    self.state
                        .set_lkg_profile(false, Some(profile_name.to_owned()))?;
                }
                let _ = self.backend.prune_managed_profiles(&keep);
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
        let mut keep = HashSet::new();
        if let Ok(full) = self.state.full_state() {
            if let Some(lkg_wifi) = full.lkg_wifi_profile {
                keep.insert(lkg_wifi);
            }
            if let Some(lkg_eth) = full.lkg_ethernet_profile {
                keep.insert(lkg_eth);
            }
        }
        if let Some(ref cand) = retained {
            keep.insert(cand.profile_name.clone());
        }
        let _ = self.backend.prune_managed_profiles(&keep);

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
        principal_id: &str,
        input: &ApplyDesiredState,
    ) -> NetworkResult<(DesiredState, Option<ProfileSpec>)> {
        match input.mode {
            DesiredMode::Hotspot => Ok((DesiredState::hotspot(), None)),
            DesiredMode::WifiClient => {
                let Some(wifi) = &input.wifi_client else {
                    return Err(NetworkError::Invalid {
                        message:
                            "invalid_network_desired_state: wifi-client mode requires wifi_client object"
                                .to_owned(),
                    });
                };
                validate_wifi_ssid(&wifi.ssid)?;
                let (psk, cred_state) = match wifi.security {
                    WifiSecurity::Open => {
                        if wifi.credential_ref.is_some() {
                            return Err(NetworkError::Invalid {
                                message:
                                    "security_mismatch: open Wi-Fi must not include credential_ref"
                                        .to_owned(),
                            });
                        }
                        (None, CredentialState::Absent)
                    }
                    _ => {
                        let Some(cred_ref) = wifi.credential_ref.as_deref() else {
                            return Err(NetworkError::Invalid {
                                message:
                                    "missing_credential_ref: protected Wi-Fi requires credential_ref"
                                        .to_owned(),
                            });
                        };
                        let passphrase = self
                            .credentials
                            .consume_for_principal(principal_id, cred_ref)?;
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
                        message: "invalid_static_ipv4: ethernet-static requires ethernet object"
                            .to_owned(),
                    });
                };
                if eth.addressing != EthernetAddressing::Static {
                    return Err(NetworkError::Invalid {
                        message: "invalid_static_ipv4: ethernet-static requires addressing=static"
                            .to_owned(),
                    });
                };
                let Some(static_cfg) = eth.static_ipv4.clone() else {
                    return Err(NetworkError::Invalid {
                        message:
                            "invalid_static_ipv4: ethernet-static requires static_ipv4 configuration"
                                .to_owned(),
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

    fn acquire_op_lock(&self) -> NetworkResult<FlockGuard> {
        acquire_network_operation_lock(&self.config.run_dir, LockMode::Exclusive, true)
            .map_err(|err| self.map_lock_error(err))
    }

    fn map_lock_error(&self, err: miro_ego_core::CoreError) -> NetworkError {
        match err {
            miro_ego_core::CoreError::LockBusy { message } => {
                if self.is_busy().unwrap_or(false) {
                    NetworkError::Busy {
                        message: "controller_busy: another network transaction is still active"
                            .to_owned(),
                    }
                } else {
                    NetworkError::Busy {
                        message: format!("capture_active: {message}"),
                    }
                }
            }
            other => NetworkError::Core(other),
        }
    }

    fn write_avahi_if_configured(&self) -> NetworkResult<()> {
        let Some(dir) = &self.config.avahi_dir else {
            return Ok(());
        };
        match std::fs::symlink_metadata(dir) {
            Ok(meta) => {
                if !meta.file_type().is_dir() {
                    eprintln!(
                        "miro-ego-network: skipping Avahi write because {} is not a directory",
                        dir.display()
                    );
                    return Ok(());
                }
            }
            Err(err) if is_skippable_avahi_io_error(&err) => {
                eprintln!(
                    "miro-ego-network: skipping Avahi write at {} ({err})",
                    dir.display()
                );
                return Ok(());
            }
            Err(err) => {
                return Err(NetworkError::Backend {
                    message: format!("failed to inspect Avahi directory {}: {err}", dir.display()),
                });
            }
        }

        let record = AvahiServiceRecord::standard(self.config.mdns_port, self.config.mdns_scheme);
        let xml = record.render_xml()?;
        let target = dir.join(AVAHI_SERVICE_FILENAME);
        match miro_ego_core::sys::fs::write_bytes_atomic(&target, xml.as_bytes(), 0o644, true) {
            Ok(()) => Ok(()),
            Err(err) if is_skippable_avahi_io_error(&err) => {
                eprintln!(
                    "miro-ego-network: skipping Avahi write at {} ({err})",
                    target.display()
                );
                Ok(())
            }
            Err(err) => Err(NetworkError::Backend {
                message: format!("failed to write Avahi service {}: {err}", target.display()),
            }),
        }
    }
}

fn is_skippable_avahi_io_error(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::PermissionDenied
            | std::io::ErrorKind::ReadOnlyFilesystem
    ) || matches!(
        err.raw_os_error(),
        Some(libc::ENOENT | libc::EACCES | libc::EPERM | libc::EROFS)
    )
}

fn validate_apply_input_shape(input: &ApplyDesiredState) -> NetworkResult<()> {
    match input.mode {
        DesiredMode::Hotspot => Ok(()),
        DesiredMode::WifiClient => {
            let Some(wifi) = &input.wifi_client else {
                return Err(NetworkError::Invalid {
                    message:
                        "invalid_network_desired_state: wifi-client mode requires wifi_client object"
                            .to_owned(),
                });
            };
            validate_wifi_ssid(&wifi.ssid)?;
            match wifi.security {
                WifiSecurity::Open => {
                    if wifi.credential_ref.is_some() {
                        return Err(NetworkError::Invalid {
                            message:
                                "security_mismatch: open Wi-Fi must not include credential_ref"
                                    .to_owned(),
                        });
                    }
                }
                _ => {
                    if wifi.credential_ref.as_deref().is_none_or(str::is_empty) {
                        return Err(NetworkError::Invalid {
                            message:
                                "missing_credential_ref: protected Wi-Fi requires credential_ref"
                                    .to_owned(),
                        });
                    }
                }
            }
            Ok(())
        }
        DesiredMode::EthernetDhcp => Ok(()),
        DesiredMode::EthernetStatic => {
            let Some(eth) = &input.ethernet else {
                return Err(NetworkError::Invalid {
                    message: "invalid_static_ipv4: ethernet-static requires ethernet object"
                        .to_owned(),
                });
            };
            if eth.addressing != EthernetAddressing::Static {
                return Err(NetworkError::Invalid {
                    message: "invalid_static_ipv4: ethernet-static requires addressing=static"
                        .to_owned(),
                });
            };
            let Some(static_cfg) = &eth.static_ipv4 else {
                return Err(NetworkError::Invalid {
                    message:
                        "invalid_static_ipv4: ethernet-static requires static_ipv4 configuration"
                            .to_owned(),
                });
            };
            static_cfg.validate()
        }
    }
}

impl NetworkController for AuthoritativeNetworkController {
    fn status(&self) -> NetworkResult<WifiStatus> {
        self.core.backend.wifi_status()
    }

    fn start_hotspot(&self, ssid: &str) -> NetworkResult<WifiStatus> {
        validate_wifi_ssid(ssid)?;
        let _lock = self.core.acquire_op_lock()?;
        let profile_name = managed_profile_name("hotspot", ssid);
        let existing_path = self.core.backend.profile_path(&profile_name);
        let spec = ProfileSpec::Hotspot {
            ssid: ssid.to_owned(),
            psk: crate::nmcli::DEFAULT_RESCUE_HOTSPOT_PSK.to_owned(),
            ipv4_cidr: DEFAULT_HOTSPOT_IPV4_CIDR.to_owned(),
        };
        // Preserve any existing hotspot profile & PSK rather than overwriting it.
        if !existing_path.exists() {
            self.core.backend.write_profile(&profile_name, &spec)?;
        }
        self.core.backend.activate_connection(
            &profile_name,
            self.core.backend.wifi_interface(),
            self.core.config.activation_wait_secs,
            false,
        )?;
        let _ = self
            .core
            .backend
            .verify_activated_profile(&profile_name, &spec)?;
        self.core.backend.wifi_status()
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
                self.create_credential_for_principal("local-controller", &credentials.passphrase)?
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
        let terminal_status = if outcome.final_status.is_terminal() {
            outcome.final_status
        } else {
            let wait_timeout = Duration::from_secs(
                self.core
                    .config
                    .activation_wait_secs
                    .saturating_mul(3)
                    .max(15),
            );
            let _ = self.wait_for_idle(wait_timeout)?;
            self.core
                .state
                .find_transaction_status(&outcome.receipt.transaction.transaction_id)?
                .unwrap_or(outcome.final_status)
        };
        if terminal_status != TransactionStatus::Committed {
            return Err(NetworkError::Backend {
                message: format!(
                    "station connection did not commit (status={terminal_status:?}; fell back to rescue hotspot)"
                ),
            });
        }
        self.core.backend.wifi_status()
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

            if args.len() == 3 && args[0] == "device" && args[1] == "disconnect" {
                if args[2] == "eth0" {
                    st.active_eth_profile = None;
                    st.active_eth_ip = None;
                    st.active_eth_route = None;
                } else if args[2] == "wlan0" {
                    st.active_wlan_profile = None;
                    st.active_wlan_ip = None;
                    st.active_wlan_route = None;
                }
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
        let init_status = ctrl.controller_status().unwrap();
        assert!(
            init_status
                .rescue_ssid
                .as_deref()
                .unwrap()
                .starts_with("MiroEgo-")
        );
        assert_eq!(init_status.observed.ap.state, "active");
        assert_eq!(init_status.observed.ap.addresses, vec!["10.42.0.1"]);

        let cred = ctrl.create_credential("wrong-passphrase").unwrap();
        let cred_ref = cred.credential_ref.clone();
        let outcome = ctrl
            .apply(
                "client-1",
                "idem-failover-1",
                &ApplyDesiredState {
                    mode: DesiredMode::WifiClient,
                    wifi_client: Some(ApplyWifiClient {
                        ssid: "LabWiFi".to_owned(),
                        security: WifiSecurity::Wpa2Personal,
                        credential_ref: Some(cred_ref.clone()),
                    }),
                    ethernet: None,
                },
            )
            .unwrap();
        assert_eq!(outcome.final_status, TransactionStatus::Accepted);
        let settled = ctrl.wait_for_idle(Duration::from_secs(5)).unwrap().unwrap();
        assert_eq!(settled.status, TransactionStatus::Rescued);

        // Idempotent replay of the rescued request reflects terminal Rescued status!
        let replayed = ctrl
            .apply(
                "client-1",
                "idem-failover-1",
                &ApplyDesiredState {
                    mode: DesiredMode::WifiClient,
                    wifi_client: Some(ApplyWifiClient {
                        ssid: "LabWiFi".to_owned(),
                        security: WifiSecurity::Wpa2Personal,
                        credential_ref: Some(cred_ref),
                    }),
                    ethernet: None,
                },
            )
            .unwrap();
        assert!(replayed.replayed);
        assert_eq!(replayed.final_status, TransactionStatus::Rescued);
        assert_eq!(
            replayed.receipt.transaction.status,
            TransactionStatus::Rescued
        );

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
        assert_eq!(retry_outcome.final_status, TransactionStatus::Accepted);
        let retry_settled = ctrl.wait_for_idle(Duration::from_secs(5)).unwrap().unwrap();
        assert_eq!(retry_settled.status, TransactionStatus::Committed);
        assert_eq!(ctrl.watchdog_mode().unwrap(), WatchdogMode::WifiClient);
        let committed_status = ctrl.controller_status().unwrap();
        assert_eq!(committed_status.observed.wifi_client.state, "connected");
        assert_eq!(
            committed_status.observed.wifi_client.addresses,
            vec!["192.168.1.50"]
        );
        assert_eq!(
            committed_status.observed.default_route,
            crate::nmcli::ObservedDefaultRoute::WifiClient
        );
        let committed_latest = committed_status.transaction.latest.unwrap();
        assert!(committed_latest.rescue.ap_validated);
        assert!(committed_latest.deadline.is_none());
        let tx_json = serde_json::to_value(&committed_latest).unwrap();
        assert!(tx_json.as_object().unwrap().contains_key("deadline"));
        assert!(tx_json["deadline"].is_null());

        // Replay of retry idempotency key returns Committed (even after later transactions!).
        let retry_replay = ctrl
            .retry(
                "client-1",
                "idem-retry-1",
                &outcome.receipt.transaction.transaction_id,
            )
            .unwrap();
        assert!(retry_replay.replayed);
        assert_eq!(retry_replay.final_status, TransactionStatus::Committed);
        assert_eq!(
            retry_replay.receipt.transaction.status,
            TransactionStatus::Committed
        );

        // Forget restores hotspot and removes managed wifi-client profiles.
        let forget_outcome = ctrl.forget("client-1", "idem-forget-1").unwrap();
        assert_eq!(forget_outcome.final_status, TransactionStatus::Accepted);
        let forget_settled = ctrl.wait_for_idle(Duration::from_secs(5)).unwrap().unwrap();
        assert_eq!(forget_settled.status, TransactionStatus::Committed);
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
        let cfg =
            ControllerConfig::new("rdk-x5-eth", root.join("run")).with_synchronous_mutations(true);
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

        // Superseded ethernet-dhcp profile must have been pruned on EthernetStatic commit!
        let remaining_profiles: Vec<String> = fs::read_dir(root.join("profiles"))
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            !remaining_profiles
                .iter()
                .any(|n| n.starts_with("miro-ego-ethernet-dhcp-"))
        );
        assert!(
            remaining_profiles
                .iter()
                .any(|n| n.starts_with("miro-ego-ethernet-static-"))
        );

        // Switching back to hotspot disconnects managed eth0 and prunes ethernet-static!
        let back_to_ap = ctrl
            .apply(
                "client-eth",
                "idem-back-ap",
                &ApplyDesiredState {
                    mode: DesiredMode::Hotspot,
                    wifi_client: None,
                    ethernet: None,
                },
            )
            .unwrap();
        assert_eq!(back_to_ap.final_status, TransactionStatus::Committed);
        assert!(mock.inner.lock().unwrap().active_eth_profile.is_none());
        let after_ap_profiles: Vec<String> = fs::read_dir(root.join("profiles"))
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(after_ap_profiles.len(), 1);
        assert!(after_ap_profiles[0].starts_with("miro-ego-hotspot-rescue-"));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn startup_reconciliation_recovers_interrupted_tx_and_preserves_credential_when_busy() {
        let root = temp_root("startup-reconcile");
        let state_path = root.join("network-state.json");

        // Seed an interrupted `Running` transaction on disk to simulate SIGKILL/power-loss.
        {
            let store = NetworkStateStore::open_or_init(&state_path).unwrap();
            let _ = store
                .accept_transaction(
                    TransactionOperation::Apply,
                    DesiredState::wifi_client(
                        "DeadRouter",
                        WifiSecurity::Wpa2Personal,
                        CredentialState::Stored,
                    ),
                    "client-crash",
                    "idem-crashed",
                    "fp-crashed",
                    "2026-11-26T00:00:00Z".to_owned(),
                    true,
                    None,
                )
                .unwrap();
            assert!(store.has_active_transaction().unwrap());
        }

        // Constructing a new controller reconciles startup: brings up rescue AP and settles
        // the interrupted transaction to `Rescued` (`current = None`)!
        let mock = MockNmcli::new(false);
        let backend = NmcliBackend::new(mock.clone(), root.join("profiles"), "wlan0", "eth0");
        let state = NetworkStateStore::open_or_init(&state_path).unwrap();
        let cfg = ControllerConfig::new("rdk-x5-crash", root.join("run"));
        let ctrl = AuthoritativeNetworkController::new(backend, state, cfg).unwrap();

        let status = ctrl.controller_status().unwrap();
        assert!(status.transaction.current.is_none());
        let latest = status.transaction.latest.unwrap();
        assert_eq!(latest.status, TransactionStatus::Rescued);
        assert_eq!(latest.recovery_action, RecoveryAction::ReconnectRescueAp);
        assert_eq!(status.observed.ap.state, "active");

        // Scan must NOT increment source_revision or rewrite network-state.json!
        let rev_before = status.source_revision;
        let scan_res = ctrl.scan().unwrap();
        assert_eq!(scan_res.source_revision, rev_before);
        assert_eq!(
            ctrl.controller_status().unwrap().source_revision,
            rev_before
        );

        // When network-operation.lock is held, `apply` MUST reject with Busy BEFORE consuming
        // single-use `credential_ref`, allowing the same `credential_ref` to succeed once idle!
        let cred = ctrl.create_credential("valid-passphrase-123").unwrap();
        {
            let _ext_lock =
                acquire_network_operation_lock(root.join("run"), LockMode::Exclusive, true)
                    .unwrap();
            let busy_err = ctrl
                .apply(
                    "client-1",
                    "idem-after-lock",
                    &ApplyDesiredState {
                        mode: DesiredMode::WifiClient,
                        wifi_client: Some(ApplyWifiClient {
                            ssid: "LabWiFi".to_owned(),
                            security: WifiSecurity::Wpa2Personal,
                            credential_ref: Some(cred.credential_ref.clone()),
                        }),
                        ethernet: None,
                    },
                )
                .unwrap_err();
            assert!(matches!(busy_err, NetworkError::Busy { .. }));
            assert!(busy_err.to_string().contains("capture_active"));
        }

        // Now that the lock is released, the SAME single-use credential_ref succeeds!
        let ok_apply = ctrl
            .apply(
                "client-1",
                "idem-after-lock",
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
        assert_eq!(ok_apply.final_status, TransactionStatus::Accepted);
        let settled = ctrl.wait_for_idle(Duration::from_secs(5)).unwrap().unwrap();
        assert_eq!(settled.status, TransactionStatus::Committed);

        // Simulate reboot in WifiClient mode when upstream router is transiently down ->
        // activates rescue AP operationally (Rescued) WITHOUT clobbering persisted.verified=true,
        // defers wifi-watchdog while rescue AP is active, and reconnects cleanly on a later boot!
        let lkg_wifi_prof = ctrl
            .state_store()
            .full_state()
            .unwrap()
            .lkg_wifi_profile
            .unwrap();
        let wifi_keyfile = fs::read_to_string(ctrl.backend().profile_path(&lkg_wifi_prof)).unwrap();
        assert!(
            wifi_keyfile.contains("autoconnect=true"),
            "verified WifiClient profile must be promoted to autoconnect=true"
        );
        assert!(
            mock.inner.lock().unwrap().calls.iter().any(|c| {
                c == &[
                    "connection".to_owned(),
                    "modify".to_owned(),
                    lkg_wifi_prof.clone(),
                    "connection.autoconnect".to_owned(),
                    "yes".to_owned(),
                ]
            }),
            "verified WifiClient commit must run nmcli connection modify <profile> connection.autoconnect yes"
        );

        drop(ctrl);
        let mock_down = MockNmcli::new(true);
        let backend2 = NmcliBackend::new(mock_down, root.join("profiles"), "wlan0", "eth0");
        let state2 = NetworkStateStore::open_or_init(&state_path).unwrap();
        let mut cfg2 = ControllerConfig::new("rdk-x5-crash", root.join("run"));
        // Non-existent Avahi directory must gracefully skip without failing startup!
        cfg2.avahi_dir = Some(root.join("missing-etc-avahi-services"));
        let ctrl2 = AuthoritativeNetworkController::new(backend2, state2, cfg2).unwrap();
        assert_eq!(ctrl2.status().unwrap().mode, crate::NetworkMode::Hotspot);
        let st2 = ctrl2.controller_status().unwrap();
        assert!(
            st2.verified,
            "transient router outage at boot must NOT clobber persisted.verified=true"
        );
        assert_eq!(
            st2.transaction.latest.as_ref().map(|t| t.status),
            Some(TransactionStatus::Rescued)
        );
        assert_eq!(ctrl2.watchdog_mode().unwrap(), WatchdogMode::Defer);

        // Next boot when router comes back online -> reconnects automatically without rescue lock-in!
        drop(ctrl2);
        let mock_up_again = MockNmcli::new(false);
        let backend3 = NmcliBackend::new(mock_up_again, root.join("profiles"), "wlan0", "eth0");
        let state3 = NetworkStateStore::open_or_init(&state_path).unwrap();
        let ctrl3 = AuthoritativeNetworkController::new(
            backend3,
            state3,
            ControllerConfig::new("rdk-x5-crash", root.join("run")),
        )
        .unwrap();
        assert_eq!(ctrl3.status().unwrap().mode, crate::NetworkMode::Station);
        assert!(ctrl3.controller_status().unwrap().verified);
        assert_eq!(ctrl3.watchdog_mode().unwrap(), WatchdogMode::WifiClient);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn enforces_principal_bound_credentials_and_operation_scoped_idempotency() {
        let root = temp_root("principal-idem");
        let mock = MockNmcli::new(false);
        let backend = NmcliBackend::new(mock, root.join("profiles"), "wlan0", "eth0");
        let state = NetworkStateStore::open_or_init(root.join("network-state.json")).unwrap();
        let cfg =
            ControllerConfig::new("rdk-x5-sec", root.join("run")).with_synchronous_mutations(true);
        let ctrl = AuthoritativeNetworkController::new(backend, state, cfg).unwrap();

        let cred = ctrl
            .create_credential_for_principal("alice", "alice-secret-psk")
            .unwrap();

        // Different principal cannot consume alice's credential_ref, and alice's ref stays valid!
        let hijack_err = ctrl
            .apply(
                "bob",
                "shared-idem-key",
                &ApplyDesiredState {
                    mode: DesiredMode::WifiClient,
                    wifi_client: Some(ApplyWifiClient {
                        ssid: "LabWiFi".to_owned(),
                        security: WifiSecurity::Wpa2Personal,
                        credential_ref: Some(cred.credential_ref.clone()),
                    }),
                    ethernet: None,
                },
            )
            .unwrap_err();
        assert_eq!(hijack_err.error_code(), Some("credential_ref_not_allowed"));

        // Alice consumes her credential_ref successfully.
        let alice_apply = ctrl
            .apply(
                "alice",
                "shared-idem-key",
                &ApplyDesiredState {
                    mode: DesiredMode::WifiClient,
                    wifi_client: Some(ApplyWifiClient {
                        ssid: "LabWiFi".to_owned(),
                        security: WifiSecurity::Wpa2Personal,
                        credential_ref: Some(cred.credential_ref.clone()),
                    }),
                    ethernet: None,
                },
            )
            .unwrap();
        assert_eq!(alice_apply.final_status, TransactionStatus::Committed);

        // Reusing the already-consumed credential_ref under a new idempotency key returns credential_ref_already_used!
        let reuse_err = ctrl
            .apply(
                "alice",
                "second-idem-key",
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
            .unwrap_err();
        assert_eq!(reuse_err.error_code(), Some("credential_ref_already_used"));

        // Same principal + same idempotency_key on a DIFFERENT operation (`forget` vs `apply`)
        // does NOT collide because idempotency is scoped by (principal_id, operation, idempotency_key)!
        let alice_forget = ctrl.forget("alice", "shared-idem-key").unwrap();
        assert!(!alice_forget.replayed);
        assert_eq!(alice_forget.final_status, TransactionStatus::Committed);

        // Different principal using the same key on `apply` also does NOT collide!
        let bob_apply = ctrl
            .apply(
                "bob",
                "shared-idem-key",
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
        assert!(!bob_apply.replayed);
        assert_eq!(bob_apply.final_status, TransactionStatus::Committed);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn ethernet_stages_autoconnect_false_disconnects_wlan_on_commit_and_falls_back_on_boot() {
        let root = temp_root("eth-stage-boot");
        let state_path = root.join("network-state.json");
        let mock = MockNmcli::new(true);
        let backend = NmcliBackend::new(mock.clone(), root.join("profiles"), "wlan0", "eth0");
        let state = NetworkStateStore::open_or_init(&state_path).unwrap();
        let cfg = ControllerConfig::new("rdk-x5-eth-boot", root.join("run"))
            .with_synchronous_mutations(true);
        let ctrl = AuthoritativeNetworkController::new(backend, state, cfg).unwrap();

        // 1. Failed EthernetDhcp apply leaves retained candidate with autoconnect=false!
        let failed = ctrl
            .apply(
                "client-eth",
                "idem-eth-fail",
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
        assert_eq!(failed.final_status, TransactionStatus::Rescued);
        let retained_prof = ctrl
            .state_store()
            .full_state()
            .unwrap()
            .retained_candidate
            .unwrap()
            .profile_name;
        let staged_txt = fs::read_to_string(ctrl.backend().profile_path(&retained_prof)).unwrap();
        assert!(
            staged_txt.contains("autoconnect=false"),
            "unverified Ethernet candidate must remain autoconnect=false"
        );

        // 2. Retry succeeds -> promotes Ethernet profile to autoconnect=true and disconnects wlan0!
        mock.inner.lock().unwrap().fail_client_activation = false;
        let retried = ctrl
            .retry(
                "client-eth",
                "idem-eth-retry",
                &failed.receipt.transaction.transaction_id,
            )
            .unwrap();
        assert_eq!(retried.final_status, TransactionStatus::Committed);
        let promoted_txt = fs::read_to_string(ctrl.backend().profile_path(&retained_prof)).unwrap();
        assert!(
            promoted_txt.contains("autoconnect=true"),
            "verified Ethernet LKG profile must be promoted to autoconnect=true"
        );
        assert!(
            mock.inner.lock().unwrap().active_wlan_profile.is_none(),
            "committing Ethernet mode must disconnect wlan0"
        );

        // 3. Reboot with eth0 cable unplugged -> reconcile_startup falls back to rescue hotspot
        //    while preserving `verified = true` so subsequent reboots retry eth0!
        drop(ctrl);
        let mock_unplugged = MockNmcli::new(true);
        let backend2 = NmcliBackend::new(mock_unplugged, root.join("profiles"), "wlan0", "eth0");
        let state2 = NetworkStateStore::open_or_init(&state_path).unwrap();
        let ctrl2 = AuthoritativeNetworkController::new(
            backend2,
            state2,
            ControllerConfig::new("rdk-x5-eth-boot", root.join("run")),
        )
        .unwrap();
        assert_eq!(ctrl2.status().unwrap().mode, crate::NetworkMode::Hotspot);
        assert!(ctrl2.controller_status().unwrap().verified);

        // 4. Reboot with eth0 cable plugged back in -> recovers EthernetDhcp automatically!
        drop(ctrl2);
        let mock_plugged = MockNmcli::new(false);
        let backend3 = NmcliBackend::new(mock_plugged, root.join("profiles"), "wlan0", "eth0");
        let state3 = NetworkStateStore::open_or_init(&state_path).unwrap();
        let ctrl3 = AuthoritativeNetworkController::new(
            backend3,
            state3,
            ControllerConfig::new("rdk-x5-eth-boot", root.join("run")),
        )
        .unwrap();
        assert_eq!(ctrl3.status().unwrap().mode, crate::NetworkMode::Offline);
        assert!(ctrl3.controller_status().unwrap().verified);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn ethernet_static_startup_unplugged_preserves_verified_and_recovers() {
        let root = temp_root("eth-static-boot");
        let state_path = root.join("network-state.json");
        let mock = MockNmcli::new(false);
        let backend = NmcliBackend::new(mock, root.join("profiles"), "wlan0", "eth0");
        let state = NetworkStateStore::open_or_init(&state_path).unwrap();
        let cfg = ControllerConfig::new("rdk-x5-eth-static", root.join("run"))
            .with_synchronous_mutations(true);
        let ctrl = AuthoritativeNetworkController::new(backend, state, cfg).unwrap();

        let applied = ctrl
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
        assert_eq!(applied.final_status, TransactionStatus::Committed);

        // Reboot with cable unplugged -> Hotspot, verified preserved!
        drop(ctrl);
        let mock_unplugged = MockNmcli::new(true);
        let backend2 = NmcliBackend::new(mock_unplugged, root.join("profiles"), "wlan0", "eth0");
        let state2 = NetworkStateStore::open_or_init(&state_path).unwrap();
        let ctrl2 = AuthoritativeNetworkController::new(
            backend2,
            state2,
            ControllerConfig::new("rdk-x5-eth-static", root.join("run")),
        )
        .unwrap();
        assert_eq!(ctrl2.status().unwrap().mode, crate::NetworkMode::Hotspot);
        assert!(ctrl2.controller_status().unwrap().verified);

        // Reboot with cable plugged back in -> EthernetStatic restored!
        drop(ctrl2);
        let mock_plugged = MockNmcli::new(false);
        let backend3 = NmcliBackend::new(mock_plugged, root.join("profiles"), "wlan0", "eth0");
        let state3 = NetworkStateStore::open_or_init(&state_path).unwrap();
        let ctrl3 = AuthoritativeNetworkController::new(
            backend3,
            state3,
            ControllerConfig::new("rdk-x5-eth-static", root.join("run")),
        )
        .unwrap();
        assert_eq!(ctrl3.status().unwrap().mode, crate::NetworkMode::Offline);
        assert!(ctrl3.controller_status().unwrap().verified);

        fs::remove_dir_all(root).unwrap();
    }
}
