//! Wi-Fi hotspot/station management, privileged network control socket, and mDNS
//! service configuration (`_miro-ego._tcp`) for Miro-Ego.

use miro_ego_core::CoreError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MdnsServiceConfig {
    pub service_type: String,
    pub instance_name: String,
    pub port: u16,
}

impl MdnsServiceConfig {
    pub fn for_host(hostname: &str, port: u16) -> Self {
        Self {
            service_type: MDNS_SERVICE_TYPE.to_owned(),
            instance_name: format!("Miro-Ego on {hostname}"),
            port,
        }
    }
}

/// Privileged network control-plane trait implemented in Wave 3.
pub trait NetworkController: Send + Sync {
    fn status(&self) -> NetworkResult<WifiStatus>;
    fn start_hotspot(&self, ssid: &str) -> NetworkResult<WifiStatus>;
    fn connect_station(&self, credentials: &StationCredentials) -> NetworkResult<WifiStatus>;
}
