//! Strongly-typed, strictly-bounded device configuration and dynamic settings.
//!
//! All configuration files stored on the 20 GiB system disk (`/etc/miro-ego/`) are
//! bounded to `MAX_CONFIG_FILE_BYTES` (64 KiB) to enforce zero unbounded disk growth.

use crate::error::{CoreError, CoreResult};
use crate::ids::{is_valid_sha256, is_valid_uuid_v4};
use crate::sys::fs::{read_bounded_json, write_bytes_atomic};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::path::{Path, PathBuf};

pub const DEVICE_CONFIG_SCHEMA: &str = "miro-ego.device-config.v1";
pub const SETTINGS_SCHEMA: &str = "miro-ego.settings.v1";
pub const RECORDING_ENCODING_SCHEMA: &str = "miro-ego.recording-encoding.v1";
pub const MAX_CONFIG_FILE_BYTES: usize = 64 * 1024;
pub const MAX_CONFIG_JSON_DEPTH: usize = 32;
pub const MAX_CAMERA_DIMENSION: u32 = 16_384;
pub const MAX_ENCODED_FPS: u32 = 60;
pub const MAX_SEGMENT_FRAMES: u64 = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListenConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ExposureSetting {
    Auto(String),
    Manual(u32),
}

impl Default for ExposureSetting {
    fn default() -> Self {
        Self::Auto("auto".to_owned())
    }
}

impl ExposureSetting {
    pub fn validate(&self) -> CoreResult<()> {
        match self {
            Self::Auto(mode) if mode == "auto" => Ok(()),
            Self::Manual(value) if (1..=100).contains(value) => Ok(()),
            _ => Err(CoreError::invalid_config(
                "invalid_config",
                "camera.exposure_time_absolute must be \"auto\" or an integer in 1..=100",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CameraConfig {
    pub device: PathBuf,
    pub width: u32,
    pub height: u32,
    pub fps: u32,
    #[serde(default = "default_frame_decimation")]
    pub frame_decimation: u32,
    #[serde(default)]
    pub exposure_time_absolute: Option<ExposureSetting>,
}

fn default_frame_decimation() -> u32 {
    2
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    pub mountpoint: PathBuf,
    #[serde(default = "default_min_available_bytes")]
    pub minimum_available_bytes: u64,
    #[serde(default = "default_min_available_inodes")]
    pub minimum_available_inodes: u64,
}

fn default_min_available_bytes() -> u64 {
    2 * 1024 * 1024 * 1024
}

fn default_min_available_inodes() -> u64 {
    1024
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceIdentity {
    pub device_id: String,
    pub device_label: String,
    pub hardware_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "profile", rename_all = "lowercase", deny_unknown_fields)]
pub enum SecurityConfig {
    Lab {
        isolated_network: bool,
    },
    Customer {
        isolated_network: bool,
        bearer_token_file: PathBuf,
        tls_certificate_file: PathBuf,
        tls_private_key_file: PathBuf,
        principal_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AudioConfig {
    pub enabled: bool,
    pub device: String,
    pub sample_rate_hz: u32,
    pub channels: u16,
    pub sample_format: String,
}

impl Default for AudioConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            device: "hw:CARD=D2UQ2,DEV=0".to_owned(),
            sample_rate_hz: 48_000,
            channels: 2,
            sample_format: "S16_LE".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VideoCodec {
    H264,
    Hevc,
}

impl VideoCodec {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::H264 => "h264",
            Self::Hevc => "hevc",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RateControlMode {
    Cbr,
    Vbr,
    Fixqp,
    Avbr,
}

impl RateControlMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cbr => "cbr",
            Self::Vbr => "vbr",
            Self::Fixqp => "fixqp",
            Self::Avbr => "avbr",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EncodingPreset {
    Standard,
    High,
    Efficient,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordingEncoding {
    #[serde(default = "default_codec")]
    pub codec: VideoCodec,
    #[serde(default = "default_rate_control")]
    pub rate_control: RateControlMode,
    #[serde(default = "default_bitrate_kbps")]
    pub bitrate_kbps: u32,
    #[serde(default = "default_min_qp")]
    pub min_qp: u8,
    #[serde(default = "default_max_qp")]
    pub max_qp: u8,
    #[serde(default = "default_intra_qp")]
    pub intra_qp: u8,
    #[serde(default = "default_initial_qp")]
    pub initial_qp: u8,
    #[serde(default = "default_gop_frames")]
    pub gop_frames: u32,
    #[serde(default = "default_vbv_ms")]
    pub vbv_ms: u32,
}

fn default_codec() -> VideoCodec {
    VideoCodec::Hevc
}
fn default_rate_control() -> RateControlMode {
    RateControlMode::Cbr
}
fn default_bitrate_kbps() -> u32 {
    12_288
}
fn default_min_qp() -> u8 {
    18
}
fn default_max_qp() -> u8 {
    32
}
fn default_intra_qp() -> u8 {
    20
}
fn default_initial_qp() -> u8 {
    22
}
fn default_gop_frames() -> u32 {
    30
}
fn default_vbv_ms() -> u32 {
    3000
}

impl Default for RecordingEncoding {
    fn default() -> Self {
        Self::from_preset(EncodingPreset::Efficient)
    }
}

impl RecordingEncoding {
    pub fn from_preset(preset: EncodingPreset) -> Self {
        match preset {
            EncodingPreset::Standard => Self {
                codec: VideoCodec::H264,
                rate_control: RateControlMode::Cbr,
                bitrate_kbps: 8192,
                min_qp: 20,
                max_qp: 51,
                intra_qp: 22,
                initial_qp: 24,
                gop_frames: 30,
                vbv_ms: 3000,
            },
            EncodingPreset::High => Self {
                codec: VideoCodec::H264,
                rate_control: RateControlMode::Cbr,
                bitrate_kbps: 16_384,
                min_qp: 18,
                max_qp: 32,
                intra_qp: 20,
                initial_qp: 22,
                gop_frames: 30,
                vbv_ms: 3000,
            },
            EncodingPreset::Efficient => Self {
                codec: VideoCodec::Hevc,
                rate_control: RateControlMode::Cbr,
                bitrate_kbps: 12_288,
                min_qp: 18,
                max_qp: 32,
                intra_qp: 20,
                initial_qp: 22,
                gop_frames: 30,
                vbv_ms: 3000,
            },
        }
    }

    pub fn validate(&self) -> CoreResult<()> {
        if !(1..=100_000).contains(&self.bitrate_kbps) {
            return Err(CoreError::invalid_config(
                "invalid_config",
                "recording.bitrate_kbps must be in 1..=100000",
            ));
        }
        if !(1..=300).contains(&self.gop_frames) {
            return Err(CoreError::invalid_config(
                "invalid_config",
                "recording.gop_frames must be in 1..=300",
            ));
        }
        if !(1..=3000).contains(&self.vbv_ms) {
            return Err(CoreError::invalid_config(
                "invalid_config",
                "recording.vbv_ms must be in 1..=3000",
            ));
        }
        for (name, val) in [
            ("min_qp", self.min_qp),
            ("max_qp", self.max_qp),
            ("intra_qp", self.intra_qp),
            ("initial_qp", self.initial_qp),
        ] {
            if val > 51 {
                return Err(CoreError::invalid_config(
                    "invalid_config",
                    format!("recording.{name} must be in 0..=51"),
                ));
            }
        }
        if self.min_qp > self.intra_qp || self.intra_qp > self.max_qp {
            return Err(CoreError::invalid_config(
                "invalid_config",
                "recording.intra_qp must lie within min_qp..=max_qp",
            ));
        }
        if self.min_qp > self.initial_qp || self.initial_qp > self.max_qp {
            return Err(CoreError::invalid_config(
                "invalid_config",
                "recording.initial_qp must lie within min_qp..=max_qp",
            ));
        }
        Ok(())
    }

    /// CLI arguments passed to `miro-ego-encoder`.
    pub fn encoder_arguments(&self) -> Vec<String> {
        vec![
            "--codec".to_owned(),
            self.codec.as_str().to_owned(),
            "--rc-mode".to_owned(),
            self.rate_control.as_str().to_owned(),
            "--bitrate-kbps".to_owned(),
            self.bitrate_kbps.to_string(),
            "--min-qp".to_owned(),
            self.min_qp.to_string(),
            "--max-qp".to_owned(),
            self.max_qp.to_string(),
            "--intra-qp".to_owned(),
            self.intra_qp.to_string(),
            "--initial-qp".to_owned(),
            self.initial_qp.to_string(),
            "--intra-period".to_owned(),
            self.gop_frames.to_string(),
            "--vbv-ms".to_owned(),
            self.vbv_ms.to_string(),
        ]
    }
}

/// Top-level bootstrap device configuration stored in `/etc/miro-ego/device.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceConfig {
    pub schema: String,
    pub listen: ListenConfig,
    pub camera: CameraConfig,
    pub storage: StorageConfig,
    pub state_root: PathBuf,
    pub device: DeviceIdentity,
    pub security: SecurityConfig,
    #[serde(default)]
    pub audio: AudioConfig,
    #[serde(default)]
    pub recording: RecordingEncoding,
    #[serde(default = "default_segment_seconds")]
    pub segment_seconds: f64,
}

fn default_segment_seconds() -> f64 {
    5.0
}

impl DeviceConfig {
    pub fn validate(&self) -> CoreResult<()> {
        if self.schema != DEVICE_CONFIG_SCHEMA {
            return Err(CoreError::invalid_config(
                "invalid_config",
                format!("device config schema must be {DEVICE_CONFIG_SCHEMA}"),
            ));
        }
        let ip: IpAddr = self.listen.host.parse().map_err(|_| {
            CoreError::invalid_config("invalid_config", "listen.host must be a valid IP address")
        })?;
        if ip.is_multicast() {
            return Err(CoreError::invalid_config(
                "invalid_config",
                "listen.host must not be multicast",
            ));
        }
        if self.listen.port == 0 {
            return Err(CoreError::invalid_config(
                "invalid_config",
                "listen.port must be in 1..=65535",
            ));
        }
        if !self.camera.device.is_absolute()
            || !self.storage.mountpoint.is_absolute()
            || !self.state_root.is_absolute()
        {
            return Err(CoreError::invalid_config(
                "invalid_config",
                "camera.device, storage.mountpoint, and state_root must be absolute paths",
            ));
        }
        if self.camera.width == 0
            || self.camera.width % 4 != 0
            || self.camera.width > MAX_CAMERA_DIMENSION
            || self.camera.height == 0
            || self.camera.height % 2 != 0
            || self.camera.height > MAX_CAMERA_DIMENSION
            || self.camera.fps == 0
            || self.camera.frame_decimation == 0
        {
            return Err(CoreError::invalid_config(
                "invalid_config",
                "camera width (multiple of 4, <= 16384), height (even, <= 16384), fps, and frame_decimation must be valid",
            ));
        }
        if self.camera.fps % self.camera.frame_decimation != 0 {
            return Err(CoreError::invalid_config(
                "invalid_config",
                "camera.fps must be an exact multiple of camera.frame_decimation",
            ));
        }
        let encoded_fps = self.camera.fps / self.camera.frame_decimation;
        if !(1..=MAX_ENCODED_FPS).contains(&encoded_fps) {
            return Err(CoreError::invalid_config(
                "invalid_config",
                "effective encoded fps (camera.fps / camera.frame_decimation) must be in 1..=60",
            ));
        }
        if let Some(exposure) = &self.camera.exposure_time_absolute {
            exposure.validate()?;
        }
        if !is_valid_uuid_v4(&self.device.device_id) {
            return Err(CoreError::invalid_config(
                "invalid_config",
                "device.device_id must be a canonical lowercase UUIDv4",
            ));
        }
        if !is_valid_device_label(&self.device.device_label) {
            return Err(CoreError::invalid_config(
                "invalid_config",
                "device.device_label must match Miro-Ego-XXXXXXXX (8 uppercase hex chars)",
            ));
        }
        let Some(fp_hex) = self.device.hardware_fingerprint.strip_prefix("sha256:") else {
            return Err(CoreError::invalid_config(
                "invalid_config",
                "device.hardware_fingerprint must start with sha256:",
            ));
        };
        if !is_valid_sha256(fp_hex) {
            return Err(CoreError::invalid_config(
                "invalid_config",
                "device.hardware_fingerprint must contain a 64-char lowercase sha256 digest",
            ));
        }
        match &self.security {
            SecurityConfig::Lab { isolated_network } => {
                if !isolated_network {
                    return Err(CoreError::invalid_config(
                        "invalid_config",
                        "security.isolated_network must be true",
                    ));
                }
            }
            SecurityConfig::Customer {
                isolated_network,
                bearer_token_file,
                tls_certificate_file,
                tls_private_key_file,
                principal_id,
            } => {
                if !isolated_network
                    || !bearer_token_file.is_absolute()
                    || !tls_certificate_file.is_absolute()
                    || !tls_private_key_file.is_absolute()
                    || !(1..=128).contains(&principal_id.len())
                    || !principal_id.bytes().all(|b| (0x21..=0x7E).contains(&b))
                {
                    return Err(CoreError::invalid_config(
                        "invalid_config",
                        "customer security profile requires absolute credential paths and valid principal_id",
                    ));
                }
            }
        }
        if self.audio.enabled && self.audio.device.is_empty() {
            return Err(CoreError::invalid_config(
                "invalid_config",
                "audio.device must not be empty when audio is enabled",
            ));
        }
        if self.audio.sample_rate_hz == 0
            || self.audio.channels == 0
            || self.audio.sample_format != "S16_LE"
        {
            return Err(CoreError::invalid_config(
                "invalid_config",
                "audio sample_rate_hz, channels, or sample_format is invalid",
            ));
        }
        if !self.segment_seconds.is_finite() || self.segment_seconds <= 0.0 {
            return Err(CoreError::invalid_config(
                "invalid_config",
                "segment_seconds must be finite and positive",
            ));
        }
        self.recording.validate()?;
        validate_segment_frame_bounds(
            encoded_fps,
            self.segment_seconds,
            self.recording.gop_frames,
            "invalid_config",
        )?;
        Ok(())
    }
}

fn validate_segment_frame_bounds(
    encoded_fps: u32,
    segment_seconds: f64,
    gop_frames: u32,
    code: &'static str,
) -> CoreResult<()> {
    let raw_frames = (f64::from(encoded_fps) * segment_seconds).round();
    if !raw_frames.is_finite() || raw_frames < 1.0 || raw_frames > (MAX_SEGMENT_FRAMES as f64) {
        return Err(CoreError::invalid_config(
            code,
            format!("segment frame count must be in 1..={MAX_SEGMENT_FRAMES}"),
        ));
    }
    let gop = u64::from(gop_frames.max(1));
    let base = (raw_frames as u64).max(gop);
    let aligned = base
        .div_ceil(gop)
        .checked_mul(gop)
        .ok_or_else(|| CoreError::invalid_config(code, "segment frame alignment overflow"))?;
    if aligned > MAX_SEGMENT_FRAMES {
        return Err(CoreError::invalid_config(
            code,
            format!("GOP-aligned segment frame count exceeds {MAX_SEGMENT_FRAMES}"),
        ));
    }
    Ok(())
}

/// Validated mutable device settings stored in `/etc/miro-ego/settings.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub schema: String,
    pub camera: CameraSettings,
    pub audio: AudioConfig,
    pub recording: RecordingEncoding,
    pub storage: StorageThresholds,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CameraSettings {
    pub width: u32,
    pub height: u32,
    pub fps: u32,
    #[serde(default)]
    pub exposure_time_absolute: Option<ExposureSetting>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageThresholds {
    pub minimum_available_bytes: u64,
    pub minimum_available_inodes: u64,
}

impl Settings {
    pub fn from_device_config(config: &DeviceConfig) -> Self {
        Self {
            schema: SETTINGS_SCHEMA.to_owned(),
            camera: CameraSettings {
                width: config.camera.width,
                height: config.camera.height,
                fps: config.camera.fps,
                exposure_time_absolute: config.camera.exposure_time_absolute.clone(),
            },
            audio: config.audio.clone(),
            recording: config.recording.clone(),
            storage: StorageThresholds {
                minimum_available_bytes: config.storage.minimum_available_bytes,
                minimum_available_inodes: config.storage.minimum_available_inodes,
            },
        }
    }

    pub fn validate(&self) -> CoreResult<()> {
        if self.schema != SETTINGS_SCHEMA {
            return Err(CoreError::invalid_config(
                "invalid_settings",
                format!("settings schema must be {SETTINGS_SCHEMA}"),
            ));
        }
        if self.camera.width == 0
            || self.camera.width % 4 != 0
            || self.camera.width > MAX_CAMERA_DIMENSION
            || self.camera.height == 0
            || self.camera.height % 2 != 0
            || self.camera.height > MAX_CAMERA_DIMENSION
            || self.camera.fps == 0
            || self.camera.fps > MAX_ENCODED_FPS
        {
            return Err(CoreError::invalid_config(
                "invalid_settings",
                "settings.camera width (multiple of 4, <= 16384), height (even, <= 16384), and fps (1..=60) must be valid",
            ));
        }
        if let Some(exposure) = &self.camera.exposure_time_absolute {
            exposure.validate()?;
        }
        if self.audio.enabled && self.audio.device.is_empty() {
            return Err(CoreError::invalid_config(
                "invalid_settings",
                "settings.audio.device must not be empty when enabled",
            ));
        }
        if self.audio.sample_rate_hz == 0
            || self.audio.channels == 0
            || self.audio.sample_format != "S16_LE"
        {
            return Err(CoreError::invalid_config(
                "invalid_settings",
                "settings.audio parameters are invalid",
            ));
        }
        self.recording.validate()?;
        Ok(())
    }

    /// Validate mutable `Settings` against the device's configured `frame_decimation`
    /// and `segment_seconds` so persisted settings remain compatible with `miro-ego-encoder`.
    pub fn validate_for_device(
        &self,
        frame_decimation: u32,
        segment_seconds: f64,
    ) -> CoreResult<()> {
        self.validate()?;
        if frame_decimation == 0 || self.camera.fps % frame_decimation != 0 {
            return Err(CoreError::invalid_config(
                "invalid_settings",
                "settings.camera.fps must be an exact multiple of frame_decimation",
            ));
        }
        let encoded_fps = self.camera.fps / frame_decimation;
        if !(1..=MAX_ENCODED_FPS).contains(&encoded_fps) {
            return Err(CoreError::invalid_config(
                "invalid_settings",
                "effective encoded fps must be in 1..=60",
            ));
        }
        validate_segment_frame_bounds(
            encoded_fps,
            segment_seconds,
            self.recording.gop_frames,
            "invalid_settings",
        )
    }
}

/// Enforce canonical device label format `^Miro-Ego-[0-9A-F]{8}$` matching OpenAPI v1 contracts.
pub fn is_valid_device_label(label: &str) -> bool {
    let Some(hex) = label.strip_prefix("Miro-Ego-") else {
        return false;
    };
    hex.len() == 8
        && hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'A'..=b'F').contains(&b))
}

/// Load and strictly validate `DeviceConfig` from a bounded JSON file (<= 64 KiB).
pub fn load_device_config(path: &Path) -> CoreResult<DeviceConfig> {
    let config: DeviceConfig =
        read_bounded_json(path, MAX_CONFIG_FILE_BYTES, MAX_CONFIG_JSON_DEPTH)
            .map_err(|err| CoreError::io("config_read_failed", "read device config", err))?;
    config.validate()?;
    Ok(config)
}

/// Load and strictly validate `Settings` from a bounded JSON file (<= 64 KiB).
pub fn load_settings(path: &Path) -> CoreResult<Settings> {
    let settings: Settings = read_bounded_json(path, MAX_CONFIG_FILE_BYTES, MAX_CONFIG_JSON_DEPTH)
        .map_err(|err| CoreError::io("settings_read_failed", "read device settings", err))?;
    settings.validate()?;
    Ok(settings)
}

/// Validate and atomically write `Settings` (strictly capped at `MAX_CONFIG_FILE_BYTES`).
pub fn save_settings_atomic(path: &Path, settings: &Settings) -> CoreResult<()> {
    settings.validate()?;
    let mut bytes = serde_json::to_vec_pretty(settings)
        .map_err(|err| CoreError::invalid_config("invalid_settings", err.to_string()))?;
    bytes.push(b'\n');
    if bytes.len() > MAX_CONFIG_FILE_BYTES {
        return Err(CoreError::invalid_config(
            "settings_too_large",
            "serialized settings exceed maximum config size",
        ));
    }
    write_bytes_atomic(path, &bytes, 0o640, true)
        .map_err(|err| CoreError::io("settings_write_failed", "write settings atomically", err))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::new_uuid_v4;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn sample_config() -> DeviceConfig {
        DeviceConfig {
            schema: DEVICE_CONFIG_SCHEMA.to_owned(),
            listen: ListenConfig {
                host: "0.0.0.0".to_owned(),
                port: 8080,
            },
            camera: CameraConfig {
                device: PathBuf::from("/dev/video0"),
                width: 3840,
                height: 1080,
                fps: 60,
                frame_decimation: 2,
                exposure_time_absolute: Some(ExposureSetting::default()),
            },
            storage: StorageConfig {
                mountpoint: PathBuf::from("/data"),
                minimum_available_bytes: 2 * 1024 * 1024 * 1024,
                minimum_available_inodes: 1024,
            },
            state_root: PathBuf::from("/var/lib/miro-ego"),
            device: DeviceIdentity {
                device_id: new_uuid_v4(),
                device_label: "Miro-Ego-1234ABCD".to_owned(),
                hardware_fingerprint: format!("sha256:{}", "a".repeat(64)),
            },
            security: SecurityConfig::Lab {
                isolated_network: true,
            },
            audio: AudioConfig::default(),
            recording: RecordingEncoding::from_preset(EncodingPreset::Efficient),
            segment_seconds: 5.0,
        }
    }

    #[test]
    fn round_trips_device_config_and_settings_with_strict_validation() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "miro-ego-config-test-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&dir).unwrap();

        let cfg = sample_config();
        let cfg_path = dir.join("device.json");
        let payload = serde_json::to_vec_pretty(&cfg).unwrap();
        write_bytes_atomic(&cfg_path, &payload, 0o640, true).unwrap();
        let loaded = load_device_config(&cfg_path).unwrap();
        assert_eq!(loaded, cfg);

        let settings = Settings::from_device_config(&loaded);
        let settings_path = dir.join("settings.json");
        save_settings_atomic(&settings_path, &settings).unwrap();
        let loaded_settings = load_settings(&settings_path).unwrap();
        assert_eq!(loaded_settings, settings);

        // Reject invalid labels (missing hyphen, wrong prefix, lowercase hex)
        for bad_label in ["BAD-1234", "MiroEgo-1234ABCD", "Miro-Ego-1234abcd"] {
            let mut bad = cfg.clone();
            bad.device.device_label = bad_label.to_owned();
            assert!(
                bad.validate().is_err(),
                "expected rejection for {bad_label}"
            );
        }

        // Reject oversized config (> 64 KiB)
        let huge_path = dir.join("huge.json");
        fs::write(&huge_path, vec![b' '; MAX_CONFIG_FILE_BYTES + 1]).unwrap();
        assert!(load_device_config(&huge_path).is_err());

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn enforces_c_encoder_dimension_fps_decimation_and_segment_invariants() {
        let cfg = sample_config();

        // Odd height rejected in DeviceConfig and Settings
        let mut odd_h = cfg.clone();
        odd_h.camera.height = 1081;
        assert!(odd_h.validate().is_err());
        let mut odd_settings = Settings::from_device_config(&cfg);
        odd_settings.camera.height = 1081;
        assert!(odd_settings.validate().is_err());

        // Dimensions > 16384 rejected
        let mut huge_w = cfg.clone();
        huge_w.camera.width = 16_388;
        assert!(huge_w.validate().is_err());
        let mut huge_h = cfg.clone();
        huge_h.camera.height = 16_386;
        assert!(huge_h.validate().is_err());

        // Non-divisible fps % frame_decimation rejected
        let mut bad_dec = cfg.clone();
        bad_dec.camera.fps = 60;
        bad_dec.camera.frame_decimation = 7;
        assert!(bad_dec.validate().is_err());
        let ok_settings = Settings::from_device_config(&cfg);
        assert!(ok_settings.validate_for_device(2, 5.0).is_ok());
        assert!(ok_settings.validate_for_device(7, 5.0).is_err());

        // Effective encoded fps > 60 or == 0 rejected
        let mut high_fps = cfg.clone();
        high_fps.camera.fps = 180;
        high_fps.camera.frame_decimation = 2;
        assert!(high_fps.validate().is_err());
        let mut zero_eff_fps = cfg.clone();
        zero_eff_fps.camera.fps = 30;
        zero_eff_fps.camera.frame_decimation = 60;
        assert!(zero_eff_fps.validate().is_err());
        let mut high_settings = Settings::from_device_config(&cfg);
        high_settings.camera.fps = 90;
        assert!(high_settings.validate().is_err());

        // segment_seconds exceeding 1,000,000 frames rejected
        let mut huge_seg = cfg.clone();
        huge_seg.segment_seconds = 40_000.0; // 30 fps * 40,000s = 1,200,000 > 1,000,000
        assert!(huge_seg.validate().is_err());
    }
}
