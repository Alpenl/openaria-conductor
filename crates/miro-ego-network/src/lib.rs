//! Wi-Fi hotspot/station management, privileged network control socket, bounded network
//! state store, and mDNS service configuration (`_miro-ego._tcp`) for Miro-Ego.

pub mod controller;
pub mod mdns;
pub mod nmcli;
pub mod rpc;
pub mod state;

use miro_ego_core::CoreError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use controller::{
    ApplyDesiredState, ApplyWifiClient, AuthoritativeNetworkController, ControllerConfig,
    ControllerStatus, CredentialReceipt, EphemeralCredentialStore, MutationCapability,
    MutationOutcome, ScanResponse, WatchdogMode, compute_watchdog_mode,
};
pub use mdns::{
    AVAHI_SERVICE_FILENAME, AvahiServiceRecord, DEFAULT_AVAHI_SERVICES_DIR, DEFAULT_MDNS_API_PATH,
    DEFAULT_MDNS_PORT, MdnsScheme, MdnsServiceConfig, MdnsStatus,
};
pub use nmcli::{
    CommandOutput, CommandRunner, ConnectionSummary, DEFAULT_ETHERNET_INTERFACE,
    DEFAULT_HOTSPOT_IPV4_CIDR, DEFAULT_RESCUE_HOTSPOT_PSK, DEFAULT_WIFI_INTERFACE,
    DeviceLinkSnapshot, NetworkDeviceInfo, NmcliBackend, ProfileSpec, RescueHotspotInfo,
    StaticIpv4Config, SystemCommandRunner, WifiScanEntry, WifiSecurity, derive_rescue_identity,
    managed_profile_name, parse_wifi_scan_output,
};
pub use rpc::{
    CONTROL_REQUEST_SCHEMA, CONTROL_RESPONSE_SCHEMA, DEFAULT_CONTROL_SOCKET_PATH,
    MAX_RPC_LINE_BYTES, NetworkRpcClient, NetworkRpcServer, RpcErrorBody, RpcRequest, RpcResponse,
    RpcServerConfig,
};
pub use state::{
    CredentialState, DEFAULT_NETWORK_STATE_FILE, DEVICE_MONOTONIC_TIME_BASE, DesiredEthernet,
    DesiredMode, DesiredState, DesiredWifiClient, EthernetAddressing, MAX_RETAINED_RECEIPTS,
    MAX_STATE_FILE_BYTES, NETWORK_RECEIPT_SCHEMA, NETWORK_STATE_SCHEMA, NETWORK_TRANSACTION_SCHEMA,
    NetworkDeviceDeadline, NetworkStateStore, NetworkTransaction, PersistedNetworkState,
    RecoveryAction, RescueMetadata, RetainedCandidate, StateSnapshot, TransactionError,
    TransactionOperation, TransactionReceipt, TransactionStage, TransactionStatus,
    TransactionUpdate, TransactionWindow,
};

pub const MDNS_SERVICE_TYPE: &str = "_miro-ego._tcp";
pub const HOTSPOT_SSID_PREFIX: &str = "MiroEgo-";
pub const NETWORK_STATUS_SCHEMA: &str = "miro-ego.network-status.v1";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkMode {
    Hotspot,
    Station,
    Offline,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WifiStatus {
    pub schema: String,
    pub mode: NetworkMode,
    pub interface: String,
    pub ssid: Option<String>,
    pub ipv4: Option<String>,
    pub signal_dbm: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StationCredentials {
    pub ssid: String,
    pub passphrase: String,
    pub country_code: Option<String>,
}

/// Privileged network control-plane trait implemented by [`AuthoritativeNetworkController`].
pub trait NetworkController: Send + Sync {
    fn status(&self) -> NetworkResult<WifiStatus>;
    fn start_hotspot(&self, ssid: &str) -> NetworkResult<WifiStatus>;
    fn connect_station(&self, credentials: &StationCredentials) -> NetworkResult<WifiStatus>;
}
