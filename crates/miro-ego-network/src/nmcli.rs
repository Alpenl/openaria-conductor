//! Pure-Rust `nmcli` backend with `CommandRunner` trait abstraction, scan request
//! coalescing, hotspot PSK preservation, Ethernet DHCP/static profiles, and link inspection.

use crate::{
    HOTSPOT_SSID_PREFIX, NETWORK_STATUS_SCHEMA, NetworkError, NetworkMode, NetworkResult,
    WifiStatus,
};
use miro_ego_core::ids::hex_encode;
use miro_ego_core::sys::fs::{fsync_directory, write_bytes_atomic};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::io::Read;
use std::net::Ipv4Addr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

pub const DEFAULT_WIFI_INTERFACE: &str = "wlan0";
pub const DEFAULT_ETHERNET_INTERFACE: &str = "eth0";
pub const DEFAULT_HOTSPOT_IPV4_CIDR: &str = "10.42.0.1/24";
pub const DEFAULT_RESCUE_HOTSPOT_PSK: &str = "12345678";
pub const DEFAULT_ACTIVATION_WAIT_SECS: u64 = 10;
pub const DEFAULT_NMCLI_TIMEOUT: Duration = Duration::from_secs(12);
pub const MAX_SCAN_ENTRIES: usize = 256;

/// Output captured from a subprocess invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
}

/// Subprocess execution trait allowing deterministic mocking of `nmcli`.
pub trait CommandRunner: Send + Sync {
    fn run(&self, program: &str, args: &[&str], timeout: Duration) -> NetworkResult<CommandOutput>;
}

/// Real OS subprocess runner with bounded execution timeout.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemCommandRunner;

impl CommandRunner for SystemCommandRunner {
    fn run(&self, program: &str, args: &[&str], timeout: Duration) -> NetworkResult<CommandOutput> {
        let mut child = Command::new(program)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|err| NetworkError::Backend {
                message: format!("failed to spawn {program}: {err}"),
            })?;

        let mut stdout_pipe = child.stdout.take();
        let mut stderr_pipe = child.stderr.take();
        let stdout_handle = std::thread::spawn(move || -> Vec<u8> {
            let mut buf = Vec::new();
            if let Some(ref mut pipe) = stdout_pipe {
                let _ = pipe.read_to_end(&mut buf);
            }
            buf
        });
        let stderr_handle = std::thread::spawn(move || -> Vec<u8> {
            let mut buf = Vec::new();
            if let Some(ref mut pipe) = stderr_pipe {
                let _ = pipe.read_to_end(&mut buf);
            }
            buf
        });

        let deadline = Instant::now() + timeout;
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) => {
                    if Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        let _ = stdout_handle.join();
                        let _ = stderr_handle.join();
                        return Err(NetworkError::Backend {
                            message: format!(
                                "network_timeout: {program} timed out after {}ms",
                                timeout.as_millis()
                            ),
                        });
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(err) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    let _ = stdout_handle.join();
                    let _ = stderr_handle.join();
                    return Err(NetworkError::Backend {
                        message: format!("failed waiting on {program}: {err}"),
                    });
                }
            }
        };

        let stdout_bytes = stdout_handle.join().unwrap_or_default();
        let stderr_bytes = stderr_handle.join().unwrap_or_default();

        Ok(CommandOutput {
            exit_code: status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&stdout_bytes).into_owned(),
            stderr: String::from_utf8_lossy(&stderr_bytes).into_owned(),
        })
    }
}

/// Supported Wi-Fi client security modes matching OpenAPI `NetworkWifiSecurity`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum WifiSecurity {
    #[serde(rename = "open")]
    Open,
    #[serde(rename = "wpa2-personal")]
    Wpa2Personal,
    #[serde(rename = "wpa3-personal")]
    Wpa3Personal,
    #[serde(rename = "wpa2-wpa3-personal")]
    Wpa2Wpa3Personal,
}

impl WifiSecurity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Wpa2Personal => "wpa2-personal",
            Self::Wpa3Personal => "wpa3-personal",
            Self::Wpa2Wpa3Personal => "wpa2-wpa3-personal",
        }
    }

    pub fn requires_credential(self) -> bool {
        !matches!(self, Self::Open)
    }
}

/// Redacted Wi-Fi scan result entry matching OpenAPI `NetworkScanEntry`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WifiScanEntry {
    pub ssid: Option<String>,
    pub hidden: bool,
    pub security: WifiSecurity,
    pub signal_dbm: i32,
    pub credential_required: bool,
}

/// Summary of an `nmcli connection show` entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionSummary {
    pub name: String,
    pub uuid: String,
    pub conn_type: String,
    pub device: Option<String>,
}

/// Summary of an `nmcli device status` entry matching OpenAPI `NetworkDeviceStatus`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkDeviceInfo {
    pub interface: String,
    #[serde(rename = "type")]
    pub device_type: String,
    pub state: String,
}

/// Interface class currently owning the default route (`NetworkObservedState.default_route`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ObservedDefaultRoute {
    WifiClient,
    Wired,
    #[default]
    None,
}

/// Live interface observation matching OpenAPI `NetworkInterfaceStatus`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedInterfaceStatus {
    pub state: String,
    pub interface: Option<String>,
    pub addresses: Vec<String>,
    pub peer_or_ssid: Option<String>,
}

impl Default for ObservedInterfaceStatus {
    fn default() -> Self {
        Self {
            state: "unavailable".to_owned(),
            interface: None,
            addresses: Vec::new(),
            peer_or_ssid: None,
        }
    }
}

/// Live network state snapshot matching OpenAPI `NetworkObservedState`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedNetworkState {
    pub ap: ObservedInterfaceStatus,
    pub wifi_client: ObservedInterfaceStatus,
    pub wired: ObservedInterfaceStatus,
    pub default_route: ObservedDefaultRoute,
    pub mdns: crate::mdns::MdnsStatus,
    pub devices: Vec<NetworkDeviceInfo>,
}

impl Default for ObservedNetworkState {
    fn default() -> Self {
        Self {
            ap: ObservedInterfaceStatus::default(),
            wifi_client: ObservedInterfaceStatus::default(),
            wired: ObservedInterfaceStatus::default(),
            default_route: ObservedDefaultRoute::None,
            mdns: crate::mdns::MdnsStatus::for_host(
                "miro-ego",
                crate::mdns::DEFAULT_MDNS_PORT,
                crate::mdns::MdnsScheme::Http,
            ),
            devices: Vec::new(),
        }
    }
}

/// Observed link, IPv4, route, and signal snapshot for an interface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceLinkSnapshot {
    pub interface: String,
    pub connected: bool,
    pub connection: Option<String>,
    pub ipv4_addresses: Vec<String>,
    pub routes: Vec<String>,
    pub ssid: Option<String>,
    pub signal_dbm: Option<i32>,
}

/// Static IPv4 configuration for `ethernet-static` mode matching OpenAPI `NetworkStaticIpv4`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticIpv4Config {
    pub address: String,
    pub prefix_length: u8,
    pub gateway: Option<String>,
    #[serde(default)]
    pub dns: Vec<String>,
}

impl StaticIpv4Config {
    pub fn cidr(&self) -> String {
        format!("{}/{}", self.address, self.prefix_length)
    }

    pub fn validate(&self) -> NetworkResult<()> {
        let addr: Ipv4Addr = self.address.parse().map_err(|_| NetworkError::Invalid {
            message: format!("invalid static IPv4 address: {}", self.address),
        })?;
        if !is_usable_ipv4(addr) {
            return Err(NetworkError::Invalid {
                message: format!("unusable static IPv4 address: {}", self.address),
            });
        }
        if !(1..=32).contains(&self.prefix_length) {
            return Err(NetworkError::Invalid {
                message: "prefix_length must be in 1..=32".to_owned(),
            });
        }
        if let Some(gw) = &self.gateway {
            let gw_addr: Ipv4Addr = gw.parse().map_err(|_| NetworkError::Invalid {
                message: format!("invalid static IPv4 gateway: {gw}"),
            })?;
            if !is_usable_ipv4(gw_addr) || gw_addr == addr {
                return Err(NetworkError::Invalid {
                    message: format!("unusable static IPv4 gateway: {gw}"),
                });
            }
        }
        if self.dns.len() > 3 {
            return Err(NetworkError::Invalid {
                message: "at most 3 DNS servers are supported".to_owned(),
            });
        }
        let mut seen = HashSet::new();
        for server in &self.dns {
            let ip: Ipv4Addr = server.parse().map_err(|_| NetworkError::Invalid {
                message: format!("invalid DNS IPv4 address: {server}"),
            })?;
            if !is_usable_ipv4(ip) {
                return Err(NetworkError::Invalid {
                    message: format!("unusable DNS IPv4 address: {server}"),
                });
            }
            if !seen.insert(ip) {
                return Err(NetworkError::Invalid {
                    message: format!("duplicate DNS IPv4 address: {server}"),
                });
            }
        }
        Ok(())
    }
}

/// Profile specification for writing a NetworkManager `.nmconnection` file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileSpec {
    Hotspot {
        ssid: String,
        psk: String,
        ipv4_cidr: String,
    },
    WifiClient {
        ssid: String,
        security: WifiSecurity,
        psk: Option<String>,
    },
    EthernetDhcp,
    EthernetStatic(StaticIpv4Config),
}

impl ProfileSpec {
    pub fn mode_slug(&self) -> &'static str {
        match self {
            Self::Hotspot { .. } => "hotspot",
            Self::WifiClient { .. } => "wifi-client",
            Self::EthernetDhcp => "ethernet-dhcp",
            Self::EthernetStatic(_) => "ethernet-static",
        }
    }

    pub fn interface_name<'a>(&self, wifi_iface: &'a str, eth_iface: &'a str) -> &'a str {
        match self {
            Self::Hotspot { .. } | Self::WifiClient { .. } => wifi_iface,
            Self::EthernetDhcp | Self::EthernetStatic(_) => eth_iface,
        }
    }

    pub fn validate(&self) -> NetworkResult<()> {
        match self {
            Self::Hotspot { ssid, psk, .. } => {
                validate_wifi_ssid(ssid)?;
                validate_wifi_psk(psk)?;
                Ok(())
            }
            Self::WifiClient {
                ssid,
                security,
                psk,
            } => {
                validate_wifi_ssid(ssid)?;
                match (security, psk) {
                    (WifiSecurity::Open, Some(_)) => Err(NetworkError::Invalid {
                        message: "open Wi-Fi network must not include a passphrase".to_owned(),
                    }),
                    (WifiSecurity::Open, None) => Ok(()),
                    (_, Some(pass)) => validate_wifi_psk(pass),
                    (_, None) => Err(NetworkError::Invalid {
                        message: "protected Wi-Fi network requires a passphrase".to_owned(),
                    }),
                }
            }
            Self::EthernetDhcp => Ok(()),
            Self::EthernetStatic(cfg) => cfg.validate(),
        }
    }
}

/// Rescue hotspot metadata returned after ensuring/migrating the rescue profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RescueHotspotInfo {
    pub profile_name: String,
    pub ssid: String,
    pub interface: String,
    pub preserved_existing_psk: bool,
}

/// Derive canonical rescue hotspot SSID `MiroEgo-XXXXXXXX` and profile name from `device_id`.
pub fn derive_rescue_identity(device_id: &str) -> NetworkResult<(String, String)> {
    if device_id.is_empty()
        || device_id.len() > 256
        || device_id.bytes().any(|b| b < 0x20 || b == 0x7f)
    {
        return Err(NetworkError::Invalid {
            message: "device_id is invalid for rescue hotspot derivation".to_owned(),
        });
    }
    let digest = hex_encode(Sha256::digest(device_id.as_bytes()).as_slice());
    let suffix_upper = digest[..8].to_ascii_uppercase();
    let profile_suffix = &digest[..12];
    Ok((
        format!("{HOTSPOT_SSID_PREFIX}{suffix_upper}"),
        format!("miro-ego-hotspot-rescue-{profile_suffix}"),
    ))
}

/// Derive deterministic managed profile name `miro-ego-<mode>-<12hex>`.
pub fn managed_profile_name(mode_slug: &str, seed: &str) -> String {
    let digest = hex_encode(Sha256::digest(seed.as_bytes()).as_slice());
    format!("miro-ego-{mode_slug}-{}", &digest[..12])
}

/// Validate a Wi-Fi SSID (1..=32 UTF-8 bytes, no ASCII control chars).
pub fn validate_wifi_ssid(ssid: &str) -> NetworkResult<()> {
    let len = ssid.len();
    if !(1..=32).contains(&len) || ssid.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return Err(NetworkError::Invalid {
            message: "Wi-Fi SSID must be 1..=32 UTF-8 bytes without control characters".to_owned(),
        });
    }
    Ok(())
}

/// Validate a WPA2/WPA3 personal passphrase (8..=63 UTF-8 bytes, no ASCII control chars).
pub fn validate_wifi_psk(psk: &str) -> NetworkResult<()> {
    let len = psk.len();
    if !(8..=63).contains(&len) || psk.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return Err(NetworkError::Invalid {
            message: "Wi-Fi passphrase must be 8..=63 UTF-8 bytes without control characters"
                .to_owned(),
        });
    }
    Ok(())
}

pub fn is_usable_ipv4(addr: Ipv4Addr) -> bool {
    !addr.is_unspecified()
        && !addr.is_loopback()
        && !addr.is_link_local()
        && !addr.is_multicast()
        && !addr.is_broadcast()
}

/// Escape leading whitespace and backslashes for NetworkManager keyfiles.
fn keyfile_escape(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\");
    let mut out = String::with_capacity(escaped.len() + 4);
    let mut chars = escaped.chars().peekable();
    while let Some(&ch) = chars.peek() {
        match ch {
            ' ' => {
                out.push_str("\\s");
                chars.next();
            }
            '\t' => {
                out.push_str("\\t");
                chars.next();
            }
            _ => break,
        }
    }
    out.extend(chars);
    out
}

/// Unescape a `GKeyFile` string value (stripping only unescaped leading horizontal whitespace
/// after `=` while preserving valid trailing spaces).
fn keyfile_unescape(raw_after_eq: &str) -> String {
    let trimmed_start = raw_after_eq
        .trim_end_matches('\r')
        .trim_start_matches([' ', '\t']);
    let mut out = String::with_capacity(trimmed_start.len());
    let mut chars = trimmed_start.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('s') => out.push(' '),
                Some('t') => out.push('\t'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('\\') => out.push('\\'),
                Some(other) => out.push(other),
                None => out.push('\\'),
            }
        } else {
            out.push(ch);
        }
    }
    out
}

/// Return `true` when `value` would be parsed by NetworkManager's `read_array_of_uint8`
/// (`g_key_file_get_integer_list` with all elements in `0..=255`), which would otherwise
/// misinterpret numeric/semicolon SSIDs such as `"101"` or `"10;20"` as raw byte indices.
fn is_nm_uint8_list_ambiguous(value: &str) -> bool {
    let body = value.strip_suffix(';').unwrap_or(value);
    if body.is_empty() {
        return false;
    }
    let mut count = 0usize;
    for part in body.split(';') {
        let token = part.trim();
        if token.is_empty() {
            return false;
        }
        let Ok(num) = token.parse::<i64>() else {
            return false;
        };
        if !(0..=255).contains(&num) {
            return false;
        }
        count += 1;
    }
    count > 0
}

/// Encode an SSID for `[wifi] ssid=` in `.nmconnection`, protecting integer-shaped SSIDs
/// from NetworkManager's `uint8` array ambiguity while keeping normal ASCII SSIDs readable.
fn format_nm_ssid_value(ssid: &str) -> String {
    if is_nm_uint8_list_ambiguous(ssid) {
        let mut encoded: Vec<String> = ssid.bytes().map(|b| b.to_string()).collect();
        encoded.push(String::new());
        encoded.join(";")
    } else {
        keyfile_escape(ssid)
    }
}

/// Parse `[wifi] ssid=` from `.nmconnection`, handling both `uint8;` byte lists and
/// `GKeyFile`-escaped UTF-8 strings (preserving valid trailing spaces).
fn parse_nm_ssid_value(raw_after_eq: &str) -> Option<String> {
    let trimmed = raw_after_eq
        .trim_end_matches('\r')
        .trim_start_matches([' ', '\t']);
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.contains(';') && is_nm_uint8_list_ambiguous(trimmed) {
        let body = trimmed.strip_suffix(';').unwrap_or(trimmed);
        let mut bytes = Vec::new();
        for part in body.split(';') {
            let b: u8 = part.trim().parse().ok()?;
            bytes.push(b);
        }
        if let Ok(decoded) = String::from_utf8(bytes) {
            if !decoded.is_empty() {
                return Some(decoded);
            }
        }
    }
    let unescaped = keyfile_unescape(raw_after_eq);
    (!unescaped.is_empty()).then_some(unescaped)
}

/// Generate a deterministic RFC 4122-shaped UUID from a profile name without external dependencies.
fn deterministic_profile_uuid(name: &str) -> String {
    let digest = Sha256::digest(format!("miro-ego.network.{name}").as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50; // version 5
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // RFC 4122 variant
    let h = hex_encode(&bytes);
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// Render a NetworkManager `.nmconnection` keyfile for the given spec.
pub fn render_nmconnection(
    profile_name: &str,
    spec: &ProfileSpec,
    wifi_iface: &str,
    eth_iface: &str,
) -> NetworkResult<String> {
    render_nmconnection_with_autoconnect(profile_name, spec, wifi_iface, eth_iface, None)
}

fn render_nmconnection_with_autoconnect(
    profile_name: &str,
    spec: &ProfileSpec,
    wifi_iface: &str,
    eth_iface: &str,
    autoconnect_override: Option<bool>,
) -> NetworkResult<String> {
    spec.validate()?;
    let iface = spec.interface_name(wifi_iface, eth_iface);
    let conn_type = if iface == wifi_iface {
        "wifi"
    } else {
        "ethernet"
    };
    let (default_autoconnect, priority) = match spec {
        ProfileSpec::Hotspot { .. } => (false, 100),
        ProfileSpec::WifiClient { .. } => (false, 800),
        ProfileSpec::EthernetDhcp | ProfileSpec::EthernetStatic(_) => (true, 900),
    };
    let autoconnect = if autoconnect_override.unwrap_or(default_autoconnect) {
        "true"
    } else {
        "false"
    };
    let uuid = deterministic_profile_uuid(profile_name);

    let mut lines = vec![
        "[connection]".to_owned(),
        format!("id={profile_name}"),
        format!("uuid={uuid}"),
        format!("type={conn_type}"),
        format!("interface-name={iface}"),
        format!("autoconnect={autoconnect}"),
        format!("autoconnect-priority={priority}"),
        String::new(),
    ];

    match spec {
        ProfileSpec::Hotspot {
            ssid,
            psk,
            ipv4_cidr,
        } => {
            lines.extend([
                "[wifi]".to_owned(),
                "mode=ap".to_owned(),
                format!("ssid={}", format_nm_ssid_value(ssid)),
                String::new(),
                "[wifi-security]".to_owned(),
                "key-mgmt=wpa-psk".to_owned(),
                format!("psk={}", keyfile_escape(psk)),
                String::new(),
                "[ipv4]".to_owned(),
                "method=shared".to_owned(),
                format!("address1={ipv4_cidr}"),
                "never-default=true".to_owned(),
            ]);
        }
        ProfileSpec::WifiClient {
            ssid,
            security,
            psk,
        } => {
            lines.extend([
                "[wifi]".to_owned(),
                "mode=infrastructure".to_owned(),
                format!("ssid={}", format_nm_ssid_value(ssid)),
                String::new(),
            ]);
            if *security != WifiSecurity::Open {
                if let Some(pass) = psk {
                    let key_mgmt = if *security == WifiSecurity::Wpa3Personal {
                        "sae"
                    } else {
                        "wpa-psk"
                    };
                    lines.extend([
                        "[wifi-security]".to_owned(),
                        format!("key-mgmt={key_mgmt}"),
                        format!("psk={}", keyfile_escape(pass)),
                        String::new(),
                    ]);
                }
            }
            lines.extend([
                "[ipv4]".to_owned(),
                "method=auto".to_owned(),
                "may-fail=false".to_owned(),
                "route-metric=600".to_owned(),
            ]);
        }
        ProfileSpec::EthernetDhcp => {
            lines.extend([
                "[ipv4]".to_owned(),
                "method=auto".to_owned(),
                "may-fail=false".to_owned(),
                "route-metric=600".to_owned(),
            ]);
        }
        ProfileSpec::EthernetStatic(cfg) => {
            let addr_entry = match &cfg.gateway {
                Some(gw) => format!("{},{gw}", cfg.cidr()),
                None => cfg.cidr(),
            };
            lines.extend([
                "[ipv4]".to_owned(),
                "method=manual".to_owned(),
                format!("address1={addr_entry}"),
            ]);
            if !cfg.dns.is_empty() {
                lines.push(format!("dns={};", cfg.dns.join(";")));
            }
            let never_default = if cfg.gateway.is_none() {
                "true"
            } else {
                "false"
            };
            lines.push(format!("never-default={never_default}"));
        }
    }

    lines.extend([
        String::new(),
        "[ipv6]".to_owned(),
        "method=disabled".to_owned(),
        String::new(),
    ]);
    Ok(lines.join("\n"))
}

/// Parsed subset of an existing `.nmconnection` file used to preserve existing hotspot PSK
/// and operator customizations.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ExistingHotspotProfile {
    existing_psk: Option<String>,
    existing_ssid: Option<String>,
    autoconnect_false: bool,
    customized_outside_psk: bool,
}

fn inspect_existing_hotspot_profile(content: &str, expected_ssid: &str) -> ExistingHotspotProfile {
    let mut section = String::new();
    let mut existing_psk: Option<String> = None;
    let mut existing_ssid: Option<String> = None;
    let mut key_mgmt: Option<String> = None;
    let mut ipv4_addr: Option<String> = None;
    let mut autoconnect_false = false;
    let mut has_timestamp_or_perms = false;

    for raw_line in content.lines() {
        let no_cr = raw_line.trim_end_matches('\r');
        let line = no_cr.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            section = line[1..line.len() - 1].to_owned();
            continue;
        }
        if let Some((k, v)) = no_cr.split_once('=') {
            let key = k.trim();
            let val_trimmed = v.trim();
            match (section.as_str(), key) {
                ("connection", "autoconnect") => autoconnect_false = val_trimmed == "false",
                ("connection", "timestamp") | ("connection", "permissions") => {
                    has_timestamp_or_perms = true;
                }
                ("wifi", "ssid") => {
                    if let Some(parsed_ssid) = parse_nm_ssid_value(v) {
                        existing_ssid = Some(parsed_ssid);
                    }
                }
                ("wifi-security", "psk") => {
                    let parsed_psk = keyfile_unescape(v);
                    if !parsed_psk.is_empty() {
                        existing_psk = Some(parsed_psk);
                    }
                }
                ("wifi-security", "key-mgmt") => key_mgmt = Some(val_trimmed.to_owned()),
                ("ipv4", "address1") => ipv4_addr = Some(val_trimmed.to_owned()),
                _ => {}
            }
        }
    }

    let customized_outside_psk = has_timestamp_or_perms
        || key_mgmt.as_deref().unwrap_or("wpa-psk") != "wpa-psk"
        || existing_ssid.as_deref() != Some(expected_ssid)
        || ipv4_addr.as_deref() != Some(DEFAULT_HOTSPOT_IPV4_CIDR);

    ExistingHotspotProfile {
        existing_psk,
        existing_ssid,
        autoconnect_false,
        customized_outside_psk,
    }
}

/// Rewrite only `autoconnect=false` inside `[connection]` while preserving PSK and all other settings.
fn rewrite_autoconnect_false(content: &str) -> String {
    let mut in_connection = false;
    let mut replaced = false;
    let mut out = String::with_capacity(content.len() + 24);

    for line in content.split_inclusive('\n') {
        let trimmed = line.trim();
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            if in_connection && !replaced {
                out.push_str("autoconnect=false\n");
                replaced = true;
            }
            in_connection = trimmed == "[connection]";
            out.push_str(line);
            continue;
        }
        if in_connection && trimmed.starts_with("autoconnect=") {
            if line.ends_with('\n') {
                out.push_str("autoconnect=false\n");
            } else {
                out.push_str("autoconnect=false");
            }
            replaced = true;
        } else {
            out.push_str(line);
        }
    }
    if in_connection && !replaced {
        if !out.ends_with('\n') && !out.is_empty() {
            out.push('\n');
        }
        out.push_str("autoconnect=false\n");
    }
    out
}

/// Split an `nmcli --terse --escape yes` line respecting `\` escapes.
pub fn split_nmcli_terse_escaped(line: &str) -> Option<Vec<String>> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut escaped = false;

    for ch in line.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == ':' {
            fields.push(std::mem::take(&mut current));
        } else {
            current.push(ch);
        }
    }
    if escaped {
        return None;
    }
    fields.push(current);
    Some(fields)
}

fn classify_scan_security(raw: &str) -> Option<WifiSecurity> {
    let normalized = raw.trim().to_ascii_uppercase();
    if normalized.is_empty() || normalized == "--" {
        return Some(WifiSecurity::Open);
    }
    if ["802.1X", "ENTERPRISE", "EAP", "WEP"]
        .iter()
        .any(|marker| normalized.contains(marker))
    {
        return None;
    }
    let has_wpa3 = normalized.contains("WPA3") || normalized.contains("SAE");
    let has_wpa2 =
        normalized.contains("WPA2") || normalized.contains("WPA1") || normalized == "WPA";
    match (has_wpa2, has_wpa3) {
        (true, true) => Some(WifiSecurity::Wpa2Wpa3Personal),
        (false, true) => Some(WifiSecurity::Wpa3Personal),
        (true, false) => Some(WifiSecurity::Wpa2Personal),
        (false, false) => None,
    }
}

/// Parse `nmcli --terse --escape yes --fields SSID,SECURITY,SIGNAL device wifi list` output.
pub fn parse_wifi_scan_output(stdout: &str) -> Vec<WifiScanEntry> {
    let mut strongest: BTreeMap<(Option<String>, WifiSecurity), WifiScanEntry> = BTreeMap::new();

    for line in stdout.lines() {
        let Some(fields) = split_nmcli_terse_escaped(line) else {
            continue;
        };
        if fields.len() != 3 {
            continue;
        }
        let raw_ssid = &fields[0];
        let Some(security) = classify_scan_security(&fields[1]) else {
            continue;
        };
        let Ok(signal_pct) = fields[2].trim().parse::<i32>() else {
            continue;
        };
        if !(0..=100).contains(&signal_pct) {
            continue;
        }

        let hidden = raw_ssid.is_empty();
        let ssid = if hidden {
            None
        } else {
            if validate_wifi_ssid(raw_ssid).is_err() {
                continue;
            }
            Some(raw_ssid.clone())
        };

        // Round half-to-even / nearest matching Python's `round(signal_percent / 2 - 100)`.
        let signal_dbm = ((signal_pct as f64) / 2.0 - 100.0).round() as i32;
        let entry = WifiScanEntry {
            ssid: ssid.clone(),
            hidden,
            security,
            signal_dbm,
            credential_required: security.requires_credential(),
        };
        let key = (ssid, security);
        let replace = match strongest.get(&key) {
            None => true,
            Some(prev) => entry.signal_dbm > prev.signal_dbm,
        };
        if replace {
            strongest.insert(key, entry);
        }
    }

    let mut entries: Vec<WifiScanEntry> = strongest.into_values().collect();
    entries.sort_by(|a, b| {
        b.signal_dbm
            .cmp(&a.signal_dbm)
            .then_with(|| a.ssid.is_none().cmp(&b.ssid.is_none()))
            .then_with(|| {
                a.ssid
                    .as_deref()
                    .unwrap_or("")
                    .cmp(b.ssid.as_deref().unwrap_or(""))
            })
            .then_with(|| a.security.as_str().cmp(b.security.as_str()))
    });
    entries.truncate(MAX_SCAN_ENTRIES);
    entries
}

/// Check whether an `IP4.ROUTE` list contains a default route (`0.0.0.0/0`).
pub fn has_default_route(routes: &[String]) -> bool {
    routes.iter().any(|route| {
        let trimmed = route.trim();
        if trimmed == "0.0.0.0/0"
            || trimmed.starts_with("0.0.0.0/0 ")
            || trimmed.starts_with("0.0.0.0/0\t")
        {
            return true;
        }
        trimmed.split(',').any(|part| {
            let p = part.trim();
            if let Some(val) = p.strip_prefix("dst") {
                val.trim_start()
                    .strip_prefix('=')
                    .is_some_and(|rhs| rhs.trim() == "0.0.0.0/0")
            } else {
                false
            }
        })
    })
}

/// Filter `IP4.ADDRESS` entries (e.g. `192.168.1.42/24`) to usable IPv4 addresses.
pub fn usable_ipv4_cidrs(addresses: &[String]) -> Vec<String> {
    let mut usable = Vec::new();
    for cidr in addresses {
        let Some((ip_part, prefix_part)) = cidr.split_once('/') else {
            continue;
        };
        let Ok(ip) = ip_part.parse::<Ipv4Addr>() else {
            continue;
        };
        let Ok(prefix) = prefix_part.parse::<u8>() else {
            continue;
        };
        if (1..=32).contains(&prefix) && is_usable_ipv4(ip) {
            usable.push(cidr.clone());
        }
    }
    usable
}

fn classify_activation_error(stderr: &str, is_wifi_client: bool) -> NetworkError {
    let lower = stderr.to_ascii_lowercase();
    if is_wifi_client
        && (lower.contains("secret")
            || lower.contains("password")
            || lower.contains("802-11-wireless-security"))
    {
        return NetworkError::Backend {
            message: "wifi_auth_failed: Wi-Fi credentials were rejected".to_owned(),
        };
    }
    if lower.contains("dhcp")
        || lower.contains("ip-config-unavailable")
        || lower.contains("timeout")
    {
        return NetworkError::Backend {
            message: "dhcp_timeout: IP configuration did not complete within deadline".to_owned(),
        };
    }
    NetworkError::Backend {
        message: format!("activation_failed: {}", stderr.trim()),
    }
}

/// In-flight shared scan state for request coalescing across concurrent callers.
struct ScanFlight {
    state: Mutex<Option<Result<Vec<WifiScanEntry>, String>>>,
    ready: Condvar,
}

impl ScanFlight {
    fn new() -> Self {
        Self {
            state: Mutex::new(None),
            ready: Condvar::new(),
        }
    }

    fn complete(&self, outcome: Result<Vec<WifiScanEntry>, String>) {
        if let Ok(mut guard) = self.state.lock() {
            *guard = Some(outcome);
            self.ready.notify_all();
        }
    }

    fn wait(&self) -> NetworkResult<Vec<WifiScanEntry>> {
        let mut guard = self.state.lock().map_err(|_| NetworkError::Backend {
            message: "scan coalescing lock poisoned".to_owned(),
        })?;
        while guard.is_none() {
            guard = self.ready.wait(guard).map_err(|_| NetworkError::Backend {
                message: "scan coalescing condvar poisoned".to_owned(),
            })?;
        }
        match guard.as_ref() {
            Some(Ok(entries)) => Ok(entries.clone()),
            Some(Err(msg)) => Err(NetworkError::Backend {
                message: msg.clone(),
            }),
            None => Err(NetworkError::Backend {
                message: "scan coalescing ended without result".to_owned(),
            }),
        }
    }
}

/// High-level NetworkManager `nmcli` backend with coalesced Wi-Fi scanning.
pub struct NmcliBackend {
    runner: Arc<dyn CommandRunner>,
    profile_dir: PathBuf,
    wifi_interface: String,
    ethernet_interface: String,
    inflight_scan: Mutex<Option<Arc<ScanFlight>>>,
}

impl NmcliBackend {
    pub fn new(
        runner: Arc<dyn CommandRunner>,
        profile_dir: impl Into<PathBuf>,
        wifi_interface: impl Into<String>,
        ethernet_interface: impl Into<String>,
    ) -> Self {
        Self {
            runner,
            profile_dir: profile_dir.into(),
            wifi_interface: wifi_interface.into(),
            ethernet_interface: ethernet_interface.into(),
            inflight_scan: Mutex::new(None),
        }
    }

    pub fn system_default() -> Self {
        Self::new(
            Arc::new(SystemCommandRunner),
            "/etc/NetworkManager/system-connections",
            DEFAULT_WIFI_INTERFACE,
            DEFAULT_ETHERNET_INTERFACE,
        )
    }

    pub fn wifi_interface(&self) -> &str {
        &self.wifi_interface
    }

    pub fn ethernet_interface(&self) -> &str {
        &self.ethernet_interface
    }

    pub fn profile_dir(&self) -> &Path {
        &self.profile_dir
    }

    pub fn profile_path(&self, profile_name: &str) -> PathBuf {
        self.profile_dir
            .join(format!("{profile_name}.nmconnection"))
    }

    /// Scan Wi-Fi networks, coalescing concurrent in-flight callers onto a single `nmcli` call.
    pub fn scan_wifi(&self) -> NetworkResult<Vec<WifiScanEntry>> {
        let (flight, is_leader) = {
            let mut slot = self
                .inflight_scan
                .lock()
                .map_err(|_| NetworkError::Backend {
                    message: "scan coalescer mutex poisoned".to_owned(),
                })?;
            if let Some(existing) = slot.as_ref() {
                (Arc::clone(existing), false)
            } else {
                let created = Arc::new(ScanFlight::new());
                *slot = Some(Arc::clone(&created));
                (created, true)
            }
        };

        if !is_leader {
            return flight.wait();
        }

        let outcome = self.execute_wifi_scan();
        {
            if let Ok(mut slot) = self.inflight_scan.lock() {
                if slot.as_ref().is_some_and(|cur| Arc::ptr_eq(cur, &flight)) {
                    *slot = None;
                }
            }
        }
        match &outcome {
            Ok(entries) => flight.complete(Ok(entries.clone())),
            Err(err) => flight.complete(Err(err.to_string())),
        }
        outcome
    }

    fn execute_wifi_scan(&self) -> NetworkResult<Vec<WifiScanEntry>> {
        let out = self.runner.run(
            "nmcli",
            &[
                "--terse",
                "--escape",
                "yes",
                "--fields",
                "SSID,SECURITY,SIGNAL",
                "device",
                "wifi",
                "list",
                "ifname",
                &self.wifi_interface,
                "--rescan",
                "yes",
            ],
            DEFAULT_NMCLI_TIMEOUT,
        )?;
        if out.exit_code != 0 {
            return Err(NetworkError::Backend {
                message: format!("network_scan_failed: {}", out.stderr.trim()),
            });
        }
        Ok(parse_wifi_scan_output(&out.stdout))
    }

    /// Reload NetworkManager keyfiles from disk.
    pub fn reload_connections(&self) -> NetworkResult<()> {
        let out = self
            .runner
            .run("nmcli", &["connection", "reload"], Duration::from_secs(10))?;
        if out.exit_code != 0 {
            return Err(NetworkError::Backend {
                message: format!("reload_failed: {}", out.stderr.trim()),
            });
        }
        Ok(())
    }

    /// List all NetworkManager connections.
    pub fn list_connections(&self) -> NetworkResult<Vec<ConnectionSummary>> {
        let out = self.runner.run(
            "nmcli",
            &[
                "--terse",
                "--escape",
                "yes",
                "--fields",
                "NAME,UUID,TYPE,DEVICE",
                "connection",
                "show",
            ],
            Duration::from_secs(10),
        )?;
        if out.exit_code != 0 {
            return Err(NetworkError::Backend {
                message: format!("connection_list_failed: {}", out.stderr.trim()),
            });
        }
        let mut items = Vec::new();
        for line in out.stdout.lines() {
            let Some(fields) = split_nmcli_terse_escaped(line) else {
                continue;
            };
            if fields.len() != 4 {
                continue;
            }
            let device = if fields[3].is_empty() || fields[3] == "--" {
                None
            } else {
                Some(fields[3].clone())
            };
            items.push(ConnectionSummary {
                name: fields[0].clone(),
                uuid: fields[1].clone(),
                conn_type: fields[2].clone(),
                device,
            });
        }
        Ok(items)
    }

    /// List NetworkManager devices (`DEVICE,TYPE,STATE`) using `--escape yes`.
    pub fn list_devices(&self) -> NetworkResult<Vec<NetworkDeviceInfo>> {
        let out = self.runner.run(
            "nmcli",
            &[
                "--terse",
                "--escape",
                "yes",
                "--fields",
                "DEVICE,TYPE,STATE",
                "device",
                "status",
            ],
            Duration::from_secs(10),
        )?;
        if out.exit_code != 0 {
            return Err(NetworkError::Backend {
                message: format!("network_status_failed: {}", out.stderr.trim()),
            });
        }
        let mut devices = Vec::new();
        for line in out.stdout.lines() {
            let Some(fields) = split_nmcli_terse_escaped(line) else {
                continue;
            };
            if fields.len() == 3 && !fields[0].is_empty() {
                devices.push(NetworkDeviceInfo {
                    interface: sanitize_device_token(&fields[0]),
                    device_type: sanitize_device_token(&fields[1]),
                    state: sanitize_device_state_token(&fields[2]),
                });
            }
        }
        devices.truncate(64);
        Ok(devices)
    }

    /// Disconnect a NetworkManager-managed interface (e.g. when leaving managed Ethernet mode).
    pub fn disconnect_device(&self, interface: &str) -> NetworkResult<()> {
        let out = self.runner.run(
            "nmcli",
            &["device", "disconnect", interface],
            Duration::from_secs(10),
        )?;
        if out.exit_code != 0 {
            return Err(NetworkError::Backend {
                message: format!(
                    "device_disconnect_failed: {interface}: {}",
                    out.stderr.trim()
                ),
            });
        }
        Ok(())
    }

    /// Write a `.nmconnection` profile atomically (`0600`) and reload NetworkManager.
    pub fn write_profile(&self, profile_name: &str, spec: &ProfileSpec) -> NetworkResult<PathBuf> {
        self.write_profile_inner(profile_name, spec, None)
    }

    /// Stage a candidate `.nmconnection` profile with `autoconnect=false` until its activation
    /// and routing are verified.
    pub fn write_profile_staged(
        &self,
        profile_name: &str,
        spec: &ProfileSpec,
    ) -> NetworkResult<PathBuf> {
        self.write_profile_inner(profile_name, spec, Some(false))
    }

    /// Promote a verified Ethernet profile to `autoconnect=true` and reload NetworkManager.
    pub fn promote_profile_autoconnect(
        &self,
        profile_name: &str,
        spec: &ProfileSpec,
    ) -> NetworkResult<()> {
        if matches!(
            spec,
            ProfileSpec::EthernetDhcp | ProfileSpec::EthernetStatic(_)
        ) {
            let _ = self.write_profile_inner(profile_name, spec, Some(true))?;
        }
        Ok(())
    }

    fn write_profile_inner(
        &self,
        profile_name: &str,
        spec: &ProfileSpec,
        autoconnect_override: Option<bool>,
    ) -> NetworkResult<PathBuf> {
        let rendered = render_nmconnection_with_autoconnect(
            profile_name,
            spec,
            &self.wifi_interface,
            &self.ethernet_interface,
            autoconnect_override,
        )?;
        fs::create_dir_all(&self.profile_dir).map_err(|err| NetworkError::Backend {
            message: format!(
                "failed to create profile dir {}: {err}",
                self.profile_dir.display()
            ),
        })?;
        let path = self.profile_path(profile_name);
        write_bytes_atomic(&path, rendered.as_bytes(), 0o600, true).map_err(|err| {
            NetworkError::Backend {
                message: format!("failed to write profile {}: {err}", path.display()),
            }
        })?;
        if let Err(err) = self.reload_connections() {
            let _ = fs::remove_file(&path);
            let _ = fsync_directory(&self.profile_dir);
            return Err(err);
        }
        Ok(path)
    }

    /// Ensure the per-device rescue hotspot `MiroEgo-XXXXXXXX` exists while PRESERVING any
    /// existing PSK (and operator customizations) instead of overwriting them!
    pub fn ensure_rescue_hotspot(
        &self,
        device_id: &str,
        preserve_existing_psk: bool,
    ) -> NetworkResult<RescueHotspotInfo> {
        let (ssid, profile_name) = derive_rescue_identity(device_id)?;
        fs::create_dir_all(&self.profile_dir).map_err(|err| NetworkError::Backend {
            message: format!(
                "failed to create profile dir {}: {err}",
                self.profile_dir.display()
            ),
        })?;
        let path = self.profile_path(&profile_name);

        if path.exists() {
            let meta = fs::symlink_metadata(&path).map_err(|err| NetworkError::Backend {
                message: format!("cannot stat rescue profile {}: {err}", path.display()),
            })?;
            if !meta.file_type().is_file() {
                return Err(NetworkError::Invalid {
                    message: "rescue profile path must be a regular file".to_owned(),
                });
            }
            let previous = fs::read_to_string(&path).map_err(|err| NetworkError::Backend {
                message: format!("cannot read rescue profile {}: {err}", path.display()),
            })?;
            let inspected = inspect_existing_hotspot_profile(&previous, &ssid);
            let has_preserved_psk = preserve_existing_psk && inspected.existing_psk.is_some();

            if inspected.customized_outside_psk || has_preserved_psk {
                // Preserve existing PSK and custom fields; only ensure autoconnect=false.
                if !inspected.autoconnect_false {
                    let updated = rewrite_autoconnect_false(&previous);
                    if updated != previous {
                        self.atomic_update_with_rollback(&path, &previous, &updated)?;
                    }
                }
                // Ensure strict 0600 permissions.
                let mode = meta.permissions().mode() & 0o777;
                if mode != 0o600 {
                    let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o600));
                }
                return Ok(RescueHotspotInfo {
                    profile_name,
                    ssid: inspected.existing_ssid.unwrap_or(ssid),
                    interface: self.wifi_interface.clone(),
                    preserved_existing_psk: has_preserved_psk,
                });
            }

            let expected = render_nmconnection(
                &profile_name,
                &ProfileSpec::Hotspot {
                    ssid: ssid.clone(),
                    psk: DEFAULT_RESCUE_HOTSPOT_PSK.to_owned(),
                    ipv4_cidr: DEFAULT_HOTSPOT_IPV4_CIDR.to_owned(),
                },
                &self.wifi_interface,
                &self.ethernet_interface,
            )?;
            if previous != expected {
                self.atomic_update_with_rollback(&path, &previous, &expected)?;
            }
            return Ok(RescueHotspotInfo {
                profile_name,
                ssid,
                interface: self.wifi_interface.clone(),
                preserved_existing_psk: false,
            });
        }

        // First-time creation of rescue hotspot profile.
        self.write_profile(
            &profile_name,
            &ProfileSpec::Hotspot {
                ssid: ssid.clone(),
                psk: DEFAULT_RESCUE_HOTSPOT_PSK.to_owned(),
                ipv4_cidr: DEFAULT_HOTSPOT_IPV4_CIDR.to_owned(),
            },
        )?;
        Ok(RescueHotspotInfo {
            profile_name,
            ssid,
            interface: self.wifi_interface.clone(),
            preserved_existing_psk: false,
        })
    }

    fn atomic_update_with_rollback(
        &self,
        path: &Path,
        previous: &str,
        updated: &str,
    ) -> NetworkResult<()> {
        write_bytes_atomic(path, updated.as_bytes(), 0o600, true).map_err(|err| {
            NetworkError::Backend {
                message: format!("failed to update rescue profile: {err}"),
            }
        })?;
        if let Err(err) = self.reload_connections() {
            let _ = write_bytes_atomic(path, previous.as_bytes(), 0o600, true);
            let _ = self.reload_connections();
            return Err(err);
        }
        Ok(())
    }

    /// Activate an existing NetworkManager profile on `interface`.
    pub fn activate_connection(
        &self,
        profile_name: &str,
        interface: &str,
        wait_secs: u64,
        is_wifi_client: bool,
    ) -> NetworkResult<()> {
        let wait_arg = wait_secs.max(1).to_string();
        let timeout = Duration::from_secs(wait_secs.saturating_add(2).max(5));
        let out = self.runner.run(
            "nmcli",
            &[
                "--wait",
                &wait_arg,
                "connection",
                "up",
                "id",
                profile_name,
                "ifname",
                interface,
            ],
            timeout,
        )?;
        if out.exit_code != 0 {
            return Err(classify_activation_error(&out.stderr, is_wifi_client));
        }
        Ok(())
    }

    /// Delete a connection via `nmcli` and remove its keyfile if present.
    pub fn delete_connection(&self, profile_name: &str) -> NetworkResult<()> {
        let path = self.profile_path(profile_name);
        let removed_file = match fs::remove_file(&path) {
            Ok(()) => true,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
            Err(err) => {
                return Err(NetworkError::Backend {
                    message: format!("failed to delete profile file {}: {err}", path.display()),
                });
            }
        };
        if removed_file {
            let _ = fsync_directory(&self.profile_dir);
            return self.reload_connections();
        }
        let out = self.runner.run(
            "nmcli",
            &["connection", "delete", "id", profile_name],
            Duration::from_secs(10),
        )?;
        if out.exit_code != 0 {
            return Err(NetworkError::Backend {
                message: format!("connection_delete_failed: {}", out.stderr.trim()),
            });
        }
        Ok(())
    }

    /// Inspect link state, active connection, IPv4 addresses, routes, and (for Wi-Fi) SSID/signal.
    pub fn inspect_device(&self, interface: &str) -> NetworkResult<DeviceLinkSnapshot> {
        let out = self.runner.run(
            "nmcli",
            &[
                "--terse",
                "--escape",
                "no",
                "--fields",
                "GENERAL.STATE,GENERAL.CONNECTION,IP4.ADDRESS,IP4.ROUTE",
                "device",
                "show",
                interface,
            ],
            Duration::from_secs(10),
        )?;
        if out.exit_code != 0 {
            return Err(NetworkError::Backend {
                message: format!("interface_unavailable: {interface}: {}", out.stderr.trim()),
            });
        }

        let mut state_code = String::new();
        let mut connection: Option<String> = None;
        let mut ipv4_addresses = Vec::new();
        let mut routes = Vec::new();

        for line in out.stdout.lines() {
            let Some((raw_key, value)) = line.split_once(':') else {
                continue;
            };
            let base_key = raw_key.split('[').next().unwrap_or(raw_key);
            let trimmed = value.trim();
            match base_key {
                "GENERAL.STATE" => {
                    if let Some(code) = trimmed.split_whitespace().next() {
                        state_code = code.to_owned();
                    }
                }
                "GENERAL.CONNECTION" if !trimmed.is_empty() && trimmed != "--" => {
                    connection = Some(trimmed.to_owned());
                }
                "IP4.ADDRESS" if !trimmed.is_empty() => {
                    ipv4_addresses.push(trimmed.to_owned());
                }
                "IP4.ROUTE" if !trimmed.is_empty() => {
                    routes.push(trimmed.to_owned());
                }
                _ => {}
            }
        }

        let connected = state_code == "100";
        let (ssid, signal_dbm) = if connected && interface == self.wifi_interface {
            self.inspect_active_wifi_signal(interface)
        } else {
            (None, None)
        };

        Ok(DeviceLinkSnapshot {
            interface: interface.to_owned(),
            connected,
            connection,
            ipv4_addresses,
            routes,
            ssid,
            signal_dbm,
        })
    }

    fn inspect_active_wifi_signal(&self, interface: &str) -> (Option<String>, Option<i32>) {
        let Ok(out) = self.runner.run(
            "nmcli",
            &[
                "--terse",
                "--escape",
                "yes",
                "--fields",
                "ACTIVE,SSID,SIGNAL",
                "device",
                "wifi",
                "list",
                "ifname",
                interface,
                "--rescan",
                "no",
            ],
            Duration::from_secs(5),
        ) else {
            return (None, None);
        };
        if out.exit_code != 0 {
            return (None, None);
        }
        for line in out.stdout.lines() {
            let Some(fields) = split_nmcli_terse_escaped(line) else {
                continue;
            };
            if fields.len() == 3 && fields[0] == "yes" {
                let ssid = if fields[1].is_empty() {
                    None
                } else {
                    Some(fields[1].clone())
                };
                let signal_dbm = fields[2]
                    .trim()
                    .parse::<i32>()
                    .ok()
                    .filter(|&p| (0..=100).contains(&p))
                    .map(|p| ((p as f64) / 2.0 - 100.0).round() as i32);
                return (ssid, signal_dbm);
            }
        }
        (None, None)
    }

    /// Build a `WifiStatus` projection for `NetworkController::status()`.
    pub fn wifi_status(&self) -> NetworkResult<WifiStatus> {
        let snap = self.inspect_device(&self.wifi_interface)?;
        let ipv4 = snap
            .ipv4_addresses
            .first()
            .map(|cidr| cidr.split('/').next().unwrap_or(cidr).to_owned());

        let mode = if !snap.connected {
            NetworkMode::Offline
        } else if snap
            .connection
            .as_deref()
            .is_some_and(|c| c.contains("hotspot"))
            || snap
                .ipv4_addresses
                .iter()
                .any(|a| a == DEFAULT_HOTSPOT_IPV4_CIDR)
        {
            NetworkMode::Hotspot
        } else {
            NetworkMode::Station
        };

        let ssid = snap.ssid.or_else(|| {
            snap.connection
                .as_deref()
                .and_then(|profile| self.read_profile_ssid(profile))
        });

        Ok(WifiStatus {
            schema: NETWORK_STATUS_SCHEMA.to_owned(),
            mode,
            interface: self.wifi_interface.clone(),
            ssid,
            ipv4,
            signal_dbm: snap.signal_dbm,
        })
    }

    fn read_profile_ssid(&self, profile_name: &str) -> Option<String> {
        let text = fs::read_to_string(self.profile_path(profile_name)).ok()?;
        let mut in_wifi = false;
        for raw in text.lines() {
            let no_cr = raw.trim_end_matches('\r');
            let line = no_cr.trim();
            if line.starts_with('[') && line.ends_with(']') {
                in_wifi = line == "[wifi]";
                continue;
            }
            if in_wifi {
                if let Some((k, v)) = no_cr.split_once('=') {
                    if k.trim() == "ssid" {
                        if let Some(ssid) = parse_nm_ssid_value(v) {
                            return Some(ssid);
                        }
                    }
                }
            }
        }
        None
    }

    /// Build a live `ObservedNetworkState` snapshot matching OpenAPI `NetworkObservedState`.
    pub fn observed_network_state(
        &self,
        rescue_ssid: Option<&str>,
        mdns_hostname: &str,
        mdns_port: u16,
        mdns_scheme: crate::mdns::MdnsScheme,
    ) -> ObservedNetworkState {
        let wlan_snap = self.inspect_device(&self.wifi_interface).ok();
        let eth_snap = self.inspect_device(&self.ethernet_interface).ok();
        let devices = self.list_devices().unwrap_or_default();

        let (ap, wifi_client, wifi_default_metric) = match &wlan_snap {
            None => (
                ObservedInterfaceStatus {
                    state: "unavailable".to_owned(),
                    interface: None,
                    addresses: Vec::new(),
                    peer_or_ssid: rescue_ssid.map(str::to_owned),
                },
                ObservedInterfaceStatus {
                    state: "unavailable".to_owned(),
                    interface: None,
                    addresses: Vec::new(),
                    peer_or_ssid: None,
                },
                None,
            ),
            Some(snap) if snap.connected => {
                let is_ap = snap
                    .connection
                    .as_deref()
                    .is_some_and(|c| c.contains("hotspot"))
                    || snap
                        .ipv4_addresses
                        .iter()
                        .any(|a| a == DEFAULT_HOTSPOT_IPV4_CIDR);
                let ips = strip_cidr_prefixes(&usable_ipv4_cidrs(&snap.ipv4_addresses));
                let resolved_ssid = snap.ssid.clone().or_else(|| {
                    snap.connection
                        .as_deref()
                        .and_then(|profile| self.read_profile_ssid(profile))
                });
                if is_ap {
                    let ap_ssid = resolved_ssid.or_else(|| rescue_ssid.map(str::to_owned));
                    let ap_status = if ips.is_empty() {
                        ObservedInterfaceStatus {
                            state: "starting".to_owned(),
                            interface: Some(self.wifi_interface.clone()),
                            addresses: Vec::new(),
                            peer_or_ssid: ap_ssid,
                        }
                    } else {
                        ObservedInterfaceStatus {
                            state: "active".to_owned(),
                            interface: Some(self.wifi_interface.clone()),
                            addresses: ips,
                            peer_or_ssid: ap_ssid,
                        }
                    };
                    let sta_status = ObservedInterfaceStatus {
                        state: "disconnected".to_owned(),
                        interface: Some(self.wifi_interface.clone()),
                        addresses: Vec::new(),
                        peer_or_ssid: None,
                    };
                    (ap_status, sta_status, None)
                } else {
                    let ap_status = ObservedInterfaceStatus {
                        state: "disabled".to_owned(),
                        interface: Some(self.wifi_interface.clone()),
                        addresses: Vec::new(),
                        peer_or_ssid: rescue_ssid.map(str::to_owned),
                    };
                    let metric = has_default_route(&snap.routes)
                        .then(|| default_route_metric(&snap.routes).unwrap_or(600));
                    let sta_status = if ips.is_empty() {
                        ObservedInterfaceStatus {
                            state: "connecting".to_owned(),
                            interface: Some(self.wifi_interface.clone()),
                            addresses: Vec::new(),
                            peer_or_ssid: resolved_ssid,
                        }
                    } else {
                        ObservedInterfaceStatus {
                            state: "connected".to_owned(),
                            interface: Some(self.wifi_interface.clone()),
                            addresses: ips,
                            peer_or_ssid: resolved_ssid,
                        }
                    };
                    (ap_status, sta_status, metric)
                }
            }
            Some(_) => (
                ObservedInterfaceStatus {
                    state: "disabled".to_owned(),
                    interface: Some(self.wifi_interface.clone()),
                    addresses: Vec::new(),
                    peer_or_ssid: rescue_ssid.map(str::to_owned),
                },
                ObservedInterfaceStatus {
                    state: "disconnected".to_owned(),
                    interface: Some(self.wifi_interface.clone()),
                    addresses: Vec::new(),
                    peer_or_ssid: None,
                },
                None,
            ),
        };

        let (wired, eth_default_metric) = match &eth_snap {
            None => (
                ObservedInterfaceStatus {
                    state: "unavailable".to_owned(),
                    interface: None,
                    addresses: Vec::new(),
                    peer_or_ssid: None,
                },
                None,
            ),
            Some(snap) if snap.connected => {
                let ips = strip_cidr_prefixes(&usable_ipv4_cidrs(&snap.ipv4_addresses));
                let metric = has_default_route(&snap.routes)
                    .then(|| default_route_metric(&snap.routes).unwrap_or(100));
                let status = if ips.is_empty() {
                    ObservedInterfaceStatus {
                        state: "connecting".to_owned(),
                        interface: Some(self.ethernet_interface.clone()),
                        addresses: Vec::new(),
                        peer_or_ssid: None,
                    }
                } else {
                    ObservedInterfaceStatus {
                        state: "connected".to_owned(),
                        interface: Some(self.ethernet_interface.clone()),
                        addresses: ips,
                        peer_or_ssid: None,
                    }
                };
                (status, metric)
            }
            Some(_) => (
                ObservedInterfaceStatus {
                    state: "disconnected".to_owned(),
                    interface: Some(self.ethernet_interface.clone()),
                    addresses: Vec::new(),
                    peer_or_ssid: None,
                },
                None,
            ),
        };

        let default_route = match (wifi_default_metric, eth_default_metric) {
            (Some(wm), Some(em)) => {
                if wm < em {
                    ObservedDefaultRoute::WifiClient
                } else {
                    ObservedDefaultRoute::Wired
                }
            }
            (Some(_), None) => ObservedDefaultRoute::WifiClient,
            (None, Some(_)) => ObservedDefaultRoute::Wired,
            (None, None) => ObservedDefaultRoute::None,
        };

        let clean_host = sanitize_mdns_hostname(mdns_hostname);
        let mdns = crate::mdns::MdnsStatus::for_host(&clean_host, mdns_port, mdns_scheme);

        ObservedNetworkState {
            ap,
            wifi_client,
            wired,
            default_route,
            mdns,
            devices,
        }
    }

    /// Verify that `expected_profile` is active on the expected interface with valid IPv4 & routes.
    pub fn verify_activated_profile(
        &self,
        profile_name: &str,
        spec: &ProfileSpec,
    ) -> NetworkResult<DeviceLinkSnapshot> {
        let iface = spec.interface_name(&self.wifi_interface, &self.ethernet_interface);
        let snap = self.inspect_device(iface)?;
        if !snap.connected || snap.connection.as_deref() != Some(profile_name) {
            return Err(NetworkError::Backend {
                message: format!(
                    "activation_unverified: {iface} is not connected to {profile_name}"
                ),
            });
        }
        match spec {
            ProfileSpec::Hotspot { ipv4_cidr, .. } => {
                if !snap.ipv4_addresses.iter().any(|a| a == ipv4_cidr) {
                    return Err(NetworkError::Backend {
                        message: format!(
                            "hotspot_address_missing: expected {ipv4_cidr} on {iface}"
                        ),
                    });
                }
            }
            ProfileSpec::WifiClient { .. } | ProfileSpec::EthernetDhcp => {
                if usable_ipv4_cidrs(&snap.ipv4_addresses).is_empty() {
                    return Err(NetworkError::Backend {
                        message: format!("dhcp_timeout: no usable IPv4 on {iface}"),
                    });
                }
                if !has_default_route(&snap.routes) {
                    return Err(NetworkError::Backend {
                        message: format!("default_route_missing: no default route on {iface}"),
                    });
                }
            }
            ProfileSpec::EthernetStatic(cfg) => {
                let expected = cfg.cidr();
                if !snap.ipv4_addresses.iter().any(|a| a == &expected) {
                    return Err(NetworkError::Backend {
                        message: format!("static_address_mismatch: expected {expected} on {iface}"),
                    });
                }
                if cfg.gateway.is_some() && !has_default_route(&snap.routes) {
                    return Err(NetworkError::Backend {
                        message: format!("default_route_missing: gateway route missing on {iface}"),
                    });
                }
            }
        }
        Ok(snap)
    }

    /// Remove all managed `miro-ego-wifi-client-*.nmconnection` files and reload NM.
    pub fn remove_managed_wifi_client_profiles(&self) -> NetworkResult<Vec<String>> {
        let mut removed = Vec::new();
        if !self.profile_dir.is_dir() {
            return Ok(removed);
        }
        let entries = fs::read_dir(&self.profile_dir).map_err(|err| NetworkError::Backend {
            message: format!("failed to scan profile dir: {err}"),
        })?;
        for entry in entries {
            let entry = entry.map_err(|err| NetworkError::Backend {
                message: format!("failed to read profile dir entry: {err}"),
            })?;
            let file_name = entry.file_name().to_string_lossy().into_owned();
            if file_name.starts_with("miro-ego-wifi-client-")
                && file_name.ends_with(".nmconnection")
            {
                fs::remove_file(entry.path()).map_err(|err| NetworkError::Backend {
                    message: format!("failed to remove profile {file_name}: {err}"),
                })?;
                removed.push(file_name);
            }
        }
        if !removed.is_empty() {
            let _ = fsync_directory(&self.profile_dir);
            self.reload_connections()?;
        }
        removed.sort();
        Ok(removed)
    }

    /// Prune superseded managed `miro-ego-*.nmconnection` profiles while always preserving
    /// `miro-ego-hotspot-rescue-*` and any profile stem listed in `keep_profiles`.
    pub fn prune_managed_profiles(
        &self,
        keep_profiles: &HashSet<String>,
    ) -> NetworkResult<Vec<String>> {
        let mut removed = Vec::new();
        if !self.profile_dir.is_dir() {
            return Ok(removed);
        }
        let entries = fs::read_dir(&self.profile_dir).map_err(|err| NetworkError::Backend {
            message: format!("failed to scan profile dir: {err}"),
        })?;
        for entry in entries {
            let entry = entry.map_err(|err| NetworkError::Backend {
                message: format!("failed to read profile dir entry: {err}"),
            })?;
            let file_name = entry.file_name().to_string_lossy().into_owned();
            let Some(stem) = file_name.strip_suffix(".nmconnection") else {
                continue;
            };
            if !stem.starts_with("miro-ego-")
                || stem.starts_with("miro-ego-hotspot-rescue-")
                || keep_profiles.contains(stem)
            {
                continue;
            }
            fs::remove_file(entry.path()).map_err(|err| NetworkError::Backend {
                message: format!("failed to prune superseded profile {file_name}: {err}"),
            })?;
            removed.push(file_name);
        }
        if !removed.is_empty() {
            let _ = fsync_directory(&self.profile_dir);
            self.reload_connections()?;
        }
        removed.sort();
        Ok(removed)
    }
}

fn strip_cidr_prefixes(cidrs: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(cidrs.len());
    for cidr in cidrs {
        let ip = cidr.split('/').next().unwrap_or(cidr).to_owned();
        if !out.contains(&ip) {
            out.push(ip);
        }
    }
    out
}

fn default_route_metric(routes: &[String]) -> Option<u32> {
    let mut best: Option<u32> = None;
    for route in routes {
        if !has_default_route(std::slice::from_ref(route)) {
            continue;
        }
        for part in route.split(',') {
            let trimmed = part.trim();
            if let Some(rest) = trimmed.strip_prefix("mt") {
                if let Some(val) = rest.trim_start().strip_prefix('=') {
                    if let Ok(m) = val.trim().parse::<u32>() {
                        best = Some(best.map_or(m, |cur| cur.min(m)));
                    }
                }
            }
        }
    }
    best
}

fn sanitize_device_token(raw: &str) -> String {
    let mut out: String = raw
        .trim()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-') {
                c
            } else {
                '_'
            }
        })
        .take(64)
        .collect();
    if out.is_empty() {
        out.push_str("unknown");
    }
    out
}

fn sanitize_device_state_token(raw: &str) -> String {
    let head = raw.split_whitespace().next().unwrap_or("unknown");
    sanitize_device_token(head)
}

fn sanitize_mdns_hostname(raw: &str) -> String {
    let stem = raw
        .trim()
        .strip_suffix(".local")
        .unwrap_or_else(|| raw.trim());
    let cleaned: String = stem
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
        .take(120)
        .collect();
    if cleaned.is_empty() {
        "miro-ego".to_owned()
    } else {
        cleaned
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(tag: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "miro-ego-nmcli-{tag}-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    struct CountingScanRunner {
        calls: AtomicUsize,
        delay: Duration,
        stdout: String,
    }

    impl CommandRunner for CountingScanRunner {
        fn run(
            &self,
            _program: &str,
            args: &[&str],
            _timeout: Duration,
        ) -> NetworkResult<CommandOutput> {
            if args.contains(&"wifi") && args.contains(&"list") {
                self.calls.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(self.delay);
                return Ok(CommandOutput {
                    exit_code: 0,
                    stdout: self.stdout.clone(),
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
    fn coalesces_concurrent_wifi_scans_into_single_nmcli_invocation() {
        let dir = temp_dir("coalesce");
        let runner = Arc::new(CountingScanRunner {
            calls: AtomicUsize::new(0),
            delay: Duration::from_millis(80),
            stdout: "Office\\:5G:WPA2:82\nOffice\\:5G:WPA2:60\nGuest:--:50\nCorp:WPA2 802.1X:90\n:SAE:40\n"
                .to_owned(),
        });
        let backend = Arc::new(NmcliBackend::new(runner.clone(), &dir, "wlan0", "eth0"));

        let mut threads = Vec::new();
        for _ in 0..6 {
            let b = Arc::clone(&backend);
            threads.push(std::thread::spawn(move || b.scan_wifi().unwrap()));
        }
        for handle in threads {
            let entries = handle.join().unwrap();
            assert_eq!(entries.len(), 3);
            assert_eq!(entries[0].ssid.as_deref(), Some("Office:5G"));
            assert_eq!(entries[0].signal_dbm, -59);
            assert_eq!(entries[0].security, WifiSecurity::Wpa2Personal);
            assert!(entries[0].credential_required);
            assert_eq!(entries[1].ssid.as_deref(), Some("Guest"));
            assert_eq!(entries[1].security, WifiSecurity::Open);
            assert!(!entries[1].credential_required);
            assert!(entries[2].hidden);
            assert_eq!(entries[2].ssid, None);
            assert_eq!(entries[2].security, WifiSecurity::Wpa3Personal);
        }
        // All 6 concurrent callers coalesced onto a single nmcli call.
        assert_eq!(runner.calls.load(Ordering::SeqCst), 1);

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rescue_hotspot_preserves_existing_psk_instead_of_overwriting() {
        let dir = temp_dir("rescue-psk");
        let runner = Arc::new(CountingScanRunner {
            calls: AtomicUsize::new(0),
            delay: Duration::ZERO,
            stdout: String::new(),
        });
        let backend = NmcliBackend::new(runner, &dir, "wlan0", "eth0");

        let first = backend.ensure_rescue_hotspot("board-001", true).unwrap();
        assert!(first.ssid.starts_with(HOTSPOT_SSID_PREFIX));
        assert!(!first.preserved_existing_psk);

        // Simulate operator customizing PSK and setting autoconnect=true.
        let path = backend.profile_path(&first.profile_name);
        let original = fs::read_to_string(&path).unwrap();
        let customized = original
            .replace("psk=12345678", "psk=CustomOperatorSecret99!")
            .replace("autoconnect=false", "autoconnect=true");
        write_bytes_atomic(&path, customized.as_bytes(), 0o600, true).unwrap();

        let second = backend.ensure_rescue_hotspot("board-001", true).unwrap();
        assert!(second.preserved_existing_psk);
        let after = fs::read_to_string(&path).unwrap();
        assert!(after.contains("psk=CustomOperatorSecret99!"));
        assert!(after.contains("autoconnect=false"));
        assert!(!after.contains("psk=12345678"));

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn validates_static_ipv4_and_renders_ethernet_profiles() {
        let static_cfg = StaticIpv4Config {
            address: "192.168.10.25".to_owned(),
            prefix_length: 24,
            gateway: Some("192.168.10.1".to_owned()),
            dns: vec!["1.1.1.1".to_owned(), "8.8.8.8".to_owned()],
        };
        let rendered = render_nmconnection(
            "miro-ego-ethernet-static-0123456789ab",
            &ProfileSpec::EthernetStatic(static_cfg),
            "wlan0",
            "eth0",
        )
        .unwrap();
        assert!(rendered.contains("type=ethernet"));
        assert!(rendered.contains("method=manual"));
        assert!(rendered.contains("address1=192.168.10.25/24,192.168.10.1"));
        assert!(rendered.contains("dns=1.1.1.1;8.8.8.8;"));
        assert!(rendered.contains("never-default=false"));

        // Reject loopback and duplicate DNS
        let bad = StaticIpv4Config {
            address: "127.0.0.1".to_owned(),
            prefix_length: 8,
            gateway: None,
            dns: vec![],
        };
        assert!(bad.validate().is_err());

        assert!(has_default_route(&[
            "dst = 0.0.0.0/0, nh = 192.168.10.1, mt = 100".to_owned()
        ]));
        assert!(!has_default_route(&[
            "dst = 192.168.10.0/24, nh = 0.0.0.0, mt = 100".to_owned()
        ]));
    }

    #[test]
    fn unescapes_keyfile_ssids_preserves_trailing_spaces_and_encodes_integer_ssids() {
        let dir = temp_dir("keyfile-ssid");
        let runner = Arc::new(CountingScanRunner {
            calls: AtomicUsize::new(0),
            delay: Duration::ZERO,
            stdout: String::new(),
        });
        let backend = NmcliBackend::new(runner, &dir, "wlan0", "eth0");

        // Leading/trailing spaces + backslashes round-trip without stripping trailing space.
        backend
            .write_profile(
                "miro-ego-wifi-client-spaced",
                &ProfileSpec::WifiClient {
                    ssid: "  Lab\\Cafe  ".to_owned(),
                    security: WifiSecurity::Open,
                    psk: None,
                },
            )
            .unwrap();
        assert_eq!(
            backend.read_profile_ssid("miro-ego-wifi-client-spaced"),
            Some("  Lab\\Cafe  ".to_owned())
        );

        // Numeric / semicolon-shaped SSIDs ("101", "10;20") encode safely as NM uint8 lists
        // and round-trip losslessly.
        for numeric_ssid in ["101", "0", "255", "10;20"] {
            let prof = format!("miro-ego-wifi-client-{}", numeric_ssid.replace(';', "_"));
            backend
                .write_profile(
                    &prof,
                    &ProfileSpec::WifiClient {
                        ssid: numeric_ssid.to_owned(),
                        security: WifiSecurity::Open,
                        psk: None,
                    },
                )
                .unwrap();
            let raw = fs::read_to_string(backend.profile_path(&prof)).unwrap();
            assert!(!raw.contains(&format!("ssid={numeric_ssid}\n")));
            assert_eq!(
                backend.read_profile_ssid(&prof),
                Some(numeric_ssid.to_owned())
            );
        }

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn list_devices_uses_escaped_terse_parsing_for_colon_interfaces() {
        struct DeviceListRunner;
        impl CommandRunner for DeviceListRunner {
            fn run(
                &self,
                _program: &str,
                args: &[&str],
                _timeout: Duration,
            ) -> NetworkResult<CommandOutput> {
                assert!(args.windows(2).any(|w| w == ["--escape", "yes"]));
                Ok(CommandOutput {
                    exit_code: 0,
                    stdout: "wlan0:wifi:connected (externally)\neth0\\:0:ethernet:disconnected\n"
                        .to_owned(),
                    stderr: String::new(),
                })
            }
        }
        let dir = temp_dir("dev-list");
        let backend = NmcliBackend::new(Arc::new(DeviceListRunner), &dir, "wlan0", "eth0");
        let devs = backend.list_devices().unwrap();
        assert_eq!(devs.len(), 2);
        assert_eq!(devs[0].interface, "wlan0");
        assert_eq!(devs[0].state, "connected");
        assert_eq!(devs[1].interface, "eth0:0");
        assert_eq!(devs[1].device_type, "ethernet");
        assert_eq!(devs[1].state, "disconnected");
        fs::remove_dir_all(dir).unwrap();
    }
}
