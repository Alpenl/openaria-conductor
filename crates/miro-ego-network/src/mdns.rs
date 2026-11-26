//! Avahi mDNS service record builder and XML renderer for `_miro-ego._tcp`.

use crate::{MDNS_SERVICE_TYPE, NetworkError, NetworkResult};
use miro_ego_core::sys::fs::write_bytes_atomic;
use serde::{Deserialize, Serialize};
use std::path::Path;

pub const DEFAULT_MDNS_PORT: u16 = 8080;
pub const DEFAULT_MDNS_API_PATH: &str = "/api/v1/device";
pub const AVAHI_SERVICE_FILENAME: &str = "miro-ego.service";
pub const DEFAULT_AVAHI_SERVICES_DIR: &str = "/etc/avahi/services";

/// Scheme announced by the Miro-Ego Avahi service definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MdnsScheme {
    Http,
    Https,
}

impl MdnsScheme {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }

    pub fn alias_service_type(self) -> &'static str {
        match self {
            Self::Http => "_http._tcp",
            Self::Https => "_https._tcp",
        }
    }
}

/// Canonical mDNS service configuration for `_miro-ego._tcp`.
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

    /// Build a full Avahi service record from this config using the given scheme.
    pub fn to_avahi_record(&self, scheme: MdnsScheme) -> AvahiServiceRecord {
        AvahiServiceRecord {
            instance_name: self.instance_name.clone(),
            replace_wildcards: self.instance_name.contains("%h"),
            service_type: self.service_type.clone(),
            port: self.port,
            scheme,
            api_path: DEFAULT_MDNS_API_PATH.to_owned(),
            extra_txt_records: Vec::new(),
        }
    }
}

/// Complete Avahi XML service-group descriptor for `_miro-ego._tcp`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvahiServiceRecord {
    pub instance_name: String,
    pub replace_wildcards: bool,
    pub service_type: String,
    pub port: u16,
    pub scheme: MdnsScheme,
    pub api_path: String,
    pub extra_txt_records: Vec<(String, String)>,
}

impl AvahiServiceRecord {
    /// Standard wildcard service definition matching `deploy/miro-ego/miro-ego*.avahi`.
    pub fn standard(port: u16, scheme: MdnsScheme) -> Self {
        Self {
            instance_name: "Miro-Ego on %h".to_owned(),
            replace_wildcards: true,
            service_type: MDNS_SERVICE_TYPE.to_owned(),
            port,
            scheme,
            api_path: DEFAULT_MDNS_API_PATH.to_owned(),
            extra_txt_records: Vec::new(),
        }
    }

    /// Host-specific service definition (`Miro-Ego on <hostname>`).
    pub fn for_host(hostname: &str, port: u16, scheme: MdnsScheme) -> Self {
        Self {
            instance_name: format!("Miro-Ego on {hostname}"),
            replace_wildcards: false,
            service_type: MDNS_SERVICE_TYPE.to_owned(),
            port,
            scheme,
            api_path: DEFAULT_MDNS_API_PATH.to_owned(),
            extra_txt_records: Vec::new(),
        }
    }

    pub fn with_txt_record(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_txt_records.push((key.into(), value.into()));
        self
    }

    /// Validate fields before rendering XML.
    pub fn validate(&self) -> NetworkResult<()> {
        if self.port == 0 {
            return Err(NetworkError::Invalid {
                message: "mDNS port must be in 1..=65535".to_owned(),
            });
        }
        if self.instance_name.is_empty()
            || self.instance_name.len() > 128
            || self.instance_name.bytes().any(|b| b < 0x20)
        {
            return Err(NetworkError::Invalid {
                message: "mDNS instance_name is invalid".to_owned(),
            });
        }
        if !is_valid_service_type(&self.service_type) {
            return Err(NetworkError::Invalid {
                message: format!("invalid mDNS service_type: {}", self.service_type),
            });
        }
        if !self.api_path.starts_with('/') || self.api_path.bytes().any(|b| b < 0x20) {
            return Err(NetworkError::Invalid {
                message: "mDNS api_path must start with '/' and contain no control characters"
                    .to_owned(),
            });
        }
        for (k, v) in &self.extra_txt_records {
            if k.is_empty()
                || k.contains('=')
                || k.bytes().any(|b| b < 0x20)
                || v.bytes().any(|b| b < 0x20)
            {
                return Err(NetworkError::Invalid {
                    message: format!("invalid mDNS TXT record key/value: {k}"),
                });
            }
        }
        Ok(())
    }

    /// Render a valid Avahi `.service` XML document matching `deploy/miro-ego/miro-ego.avahi`.
    pub fn render_xml(&self) -> NetworkResult<String> {
        self.validate()?;
        let replace_attr = if self.replace_wildcards { "yes" } else { "no" };
        let mut xml = String::with_capacity(512);
        xml.push_str("<?xml version=\"1.0\" standalone='no'?>\n");
        xml.push_str("<!DOCTYPE service-group SYSTEM \"avahi-service.dtd\">\n");
        xml.push_str("<service-group>\n");
        xml.push_str(&format!(
            "  <name replace-wildcards=\"{replace_attr}\">{}</name>\n",
            xml_escape(&self.instance_name)
        ));
        xml.push_str("  <service>\n");
        xml.push_str(&format!(
            "    <type>{}</type>\n",
            xml_escape(&self.service_type)
        ));
        xml.push_str(&format!("    <port>{}</port>\n", self.port));
        xml.push_str(&format!(
            "    <txt-record>scheme={}</txt-record>\n",
            self.scheme.as_str()
        ));
        xml.push_str(&format!(
            "    <txt-record>api={}</txt-record>\n",
            xml_escape(&self.api_path)
        ));
        for (key, value) in &self.extra_txt_records {
            xml.push_str(&format!(
                "    <txt-record>{}={}</txt-record>\n",
                xml_escape(key),
                xml_escape(value)
            ));
        }
        xml.push_str("  </service>\n");
        xml.push_str("  <service>\n");
        xml.push_str(&format!(
            "    <type>{}</type>\n",
            self.scheme.alias_service_type()
        ));
        xml.push_str(&format!("    <port>{}</port>\n", self.port));
        xml.push_str("  </service>\n");
        xml.push_str("</service-group>\n");
        Ok(xml)
    }

    /// Atomically write the rendered Avahi XML definition with mode `0644`.
    pub fn write_atomic(&self, path: &Path) -> NetworkResult<()> {
        let xml = self.render_xml()?;
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|err| NetworkError::Backend {
                    message: format!(
                        "failed to create Avahi directory {}: {err}",
                        parent.display()
                    ),
                })?;
            }
        }
        write_bytes_atomic(path, xml.as_bytes(), 0o644, true).map_err(|err| NetworkError::Backend {
            message: format!("failed to write Avahi service {}: {err}", path.display()),
        })
    }
}

/// Public mDNS status projection matching OpenAPI `NetworkMdnsStatus`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MdnsStatus {
    pub hostname: String,
    pub service: String,
    pub aliases: Vec<String>,
    pub port: u16,
}

impl MdnsStatus {
    pub fn for_host(hostname: &str, port: u16, scheme: MdnsScheme) -> Self {
        let local_host = if hostname.ends_with(".local") {
            hostname.to_owned()
        } else {
            format!("{hostname}.local")
        };
        Self {
            hostname: local_host,
            service: MDNS_SERVICE_TYPE.to_owned(),
            aliases: vec![scheme.alias_service_type().to_owned()],
            port,
        }
    }
}

fn is_valid_service_type(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

fn xml_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            other => out.push(other),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn renders_http_and_https_avahi_xml_matching_deploy_assets() {
        let http_xml = AvahiServiceRecord::standard(8080, MdnsScheme::Http)
            .render_xml()
            .unwrap();
        assert!(http_xml.contains("<name replace-wildcards=\"yes\">Miro-Ego on %h</name>"));
        assert!(http_xml.contains("<type>_miro-ego._tcp</type>"));
        assert!(http_xml.contains("<txt-record>scheme=http</txt-record>"));
        assert!(http_xml.contains("<txt-record>api=/api/v1/device</txt-record>"));
        assert!(http_xml.contains("<type>_http._tcp</type>"));

        let https_xml = AvahiServiceRecord::standard(8443, MdnsScheme::Https)
            .render_xml()
            .unwrap();
        assert!(https_xml.contains("<port>8443</port>"));
        assert!(https_xml.contains("<txt-record>scheme=https</txt-record>"));
        assert!(https_xml.contains("<type>_https._tcp</type>"));
    }

    #[test]
    fn escapes_xml_special_characters_and_rejects_invalid_configs() {
        let record = AvahiServiceRecord::for_host("lab<&>\"'host", 8080, MdnsScheme::Http)
            .with_txt_record("device", "Miro-Ego<1>");
        let xml = record.render_xml().unwrap();
        assert!(xml.contains("Miro-Ego on lab&lt;&amp;&gt;&quot;&apos;host"));
        assert!(xml.contains("<txt-record>device=Miro-Ego&lt;1&gt;</txt-record>"));
        assert!(xml.contains("replace-wildcards=\"no\""));

        let bad_port = AvahiServiceRecord::for_host("rdk-x5", 0, MdnsScheme::Http);
        assert!(bad_port.render_xml().is_err());

        let bad_txt = AvahiServiceRecord::for_host("rdk-x5", 8080, MdnsScheme::Http)
            .with_txt_record("a=b", "val");
        assert!(bad_txt.render_xml().is_err());
    }

    #[test]
    fn writes_avahi_xml_atomically_and_builds_status() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "miro-ego-mdns-test-{}-{unique}",
            std::process::id()
        ));
        let target = dir.join(AVAHI_SERVICE_FILENAME);

        let cfg = MdnsServiceConfig::for_host("miro-x5", 8080);
        let record = cfg.to_avahi_record(MdnsScheme::Http);
        record.write_atomic(&target).unwrap();

        let written = std::fs::read_to_string(&target).unwrap();
        assert!(written.contains("Miro-Ego on miro-x5"));
        assert!(written.contains("api=/api/v1/device"));

        let status = MdnsStatus::for_host("miro-x5", 8080, MdnsScheme::Https);
        assert_eq!(status.hostname, "miro-x5.local");
        assert_eq!(status.service, MDNS_SERVICE_TYPE);
        assert_eq!(status.aliases, vec!["_https._tcp"]);

        std::fs::remove_dir_all(dir).unwrap();
    }
}
