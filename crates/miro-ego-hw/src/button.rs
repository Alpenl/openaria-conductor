//! 40-pin GPIO recording button monitor, Linux `/dev/gpiochip*` character-device GPIO (`v2` +
//! `v1` + sysfs fallback), debounce/short/double/long press classifier, `/etc/miro-ego/button.json`
//! schema loader, and deadline-bounded HTTP/HTTPS local capture toggle client with pinned TLS
//! certificate verification.

use crate::{ButtonEvent, ButtonInput, HwError, HwResult};
use miro_ego_core::config::{ListenConfig, SecurityConfig, load_device_config};
use miro_ego_core::ids::new_uuid_v4;
use miro_ego_core::sys::fs::{read_bounded_bytes, read_bounded_json};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{WebPkiSupportedAlgorithms, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, ClientConnection, DigitallySignedStruct, SignatureScheme, StreamOwned};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs::{self, OpenOptions};
use std::io::{Cursor, ErrorKind, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use subtle::ConstantTimeEq;

pub const BUTTON_CONFIG_SCHEMA: &str = "miro-ego.button-config.v1";
pub const DEFAULT_BUTTON_CONFIG_PATH: &str = "/etc/miro-ego/button.json";
pub const DEFAULT_GPIO_SYSFS_ROOT: &str = "/sys/class/gpio";
pub const DEFAULT_DEV_ROOT: &str = "/dev";
pub const MAX_BUTTON_CONFIG_BYTES: usize = 8 * 1024;
pub const MAX_HTTP_RESPONSE_BYTES: usize = 1024 * 1024;
pub const MAX_REQUEST_TIMEOUT_MS: u64 = 30_000;

/// Valid 40-pin RDK X5 BOARD GPIO physical pins (excluding 3V3, 5V, and GND pins).
pub const RDK_X5_GPIO_PINS: &[u8] = &[
    3, 5, 7, 8, 10, 11, 12, 13, 15, 16, 18, 19, 21, 22, 23, 24, 26, 27, 28, 29, 31, 32, 33, 35, 36,
    37, 38, 40,
];

/// Translate a D-Robotics RDK X5 40-pin `BOARD` physical header pin (`1..=40`) to its
/// Sunrise X5 Linux sysfs SoC GPIO number (`lsio_gpio0` `@379..410`, `lsio_gpio1` `@347..363`,
/// `dsp_gpio0` `@411..433`, `aon_gpio0` `@498..505`).
///
/// In particular, the default recording button pin `37` (`LSIO_SPI2_MISO` / `LSIO_GPIO0_22`)
/// maps to SoC GPIO `401`.
pub fn rdk_x5_board_pin_to_soc_gpio(physical_pin: u8) -> HwResult<u32> {
    let soc_gpio = match physical_pin {
        3 => 390,  // LSIO_I2C5_SDA  (LSIO_GPIO0_11)
        5 => 389,  // LSIO_I2C5_SCL  (LSIO_GPIO0_10)
        7 => 498,  // AON_GPIO0_00
        8 => 384,  // LSIO_UART1_TX  (LSIO_GPIO0_05)
        10 => 383, // LSIO_UART1_RX  (LSIO_GPIO0_04)
        11 => 386, // LSIO_UART1_RTS (LSIO_GPIO0_07)
        12 => 421, // DSP_I2S1_SCLK  (DSP_GPIO0_10)
        13 => 500, // AON_GPIO0_02
        15 => 501, // AON_GPIO0_03
        16 => 402, // LSIO_SPI2_MOSI (LSIO_GPIO0_23)
        18 => 403, // LSIO_SPI3_SCLK (LSIO_GPIO0_24)
        19 => 398, // LSIO_SPI1_MOSI (LSIO_GPIO0_19)
        21 => 397, // LSIO_SPI1_MISO (LSIO_GPIO0_18)
        22 => 502, // AON_GPIO0_04
        23 => 395, // LSIO_SPI1_SCLK (LSIO_GPIO0_16)
        24 => 396, // LSIO_SPI1_SSN  (LSIO_GPIO0_17 / LPWM1_OUT3)
        26 => 394, // LSIO_SPI1_SSN1 (LSIO_GPIO0_15)
        27 => 355, // LSIO_I2C0_SDA  (LSIO_GPIO1_08 / LPWM2_OUT5)
        28 => 354, // LSIO_I2C0_SCL  (LSIO_GPIO1_07 / LPWM2_OUT4)
        29 => 399, // LSIO_SPI2_SCLK (LSIO_GPIO0_20 / LPWM0_OUT0)
        31 => 400, // LSIO_SPI2_SSN  (LSIO_GPIO0_21 / LPWM0_OUT1)
        32 => 347, // LSIO_SPI5_SCLK (LSIO_GPIO1_00 / LPWM3_OUT6)
        33 => 348, // LSIO_SPI5_SSN  (LSIO_GPIO1_01 / LPWM3_OUT7)
        35 => 422, // DSP_I2S1_WS    (DSP_GPIO0_11)
        36 => 385, // LSIO_UART1_CTS (LSIO_GPIO0_06)
        37 => 401, // LSIO_SPI2_MISO (LSIO_GPIO0_22 / LPWM1_OUT2)
        38 => 423, // DSP_I2S1_DI    (DSP_GPIO0_12)
        40 => 424, // DSP_I2S1_DO    (DSP_GPIO0_13)
        other => {
            return Err(HwError::Io {
                message: format!(
                    "physical_pin {other} is not a valid RDK X5 40-pin BOARD GPIO pin"
                ),
            });
        }
    };
    Ok(soc_gpio)
}

/// Map a Sunrise X5 SoC GPIO number (`347..=505`) to `(bank_keywords, line_offset)` for
/// `/dev/gpiochip*` character-device resolution.
pub fn rdk_x5_soc_gpio_to_bank_offset(soc_gpio: u32) -> Option<(&'static [&'static str], u32)> {
    match soc_gpio {
        347..=363 => Some((&["lsio_gpio1", "32150000"], soc_gpio - 347)),
        379..=410 => Some((&["lsio_gpio0", "32140000"], soc_gpio - 379)),
        411..=433 => Some((&["dsp_gpio0", "31040000"], soc_gpio - 411)),
        498..=505 => Some((&["aon_gpio0", "31020000"], soc_gpio - 498)),
        _ => None,
    }
}

fn default_schema() -> String {
    BUTTON_CONFIG_SCHEMA.to_owned()
}
fn default_enabled() -> bool {
    true
}
fn default_physical_pin() -> u8 {
    37
}
fn default_debounce_ms() -> u64 {
    80
}
fn default_cooldown_ms() -> u64 {
    1000
}
fn default_poll_ms() -> u64 {
    20
}
fn default_request_timeout_ms() -> u64 {
    5000
}
fn default_double_press_window_ms() -> u64 {
    400
}
fn default_long_press_ms() -> u64 {
    1500
}
fn default_status_led() -> Option<String> {
    Some("ACT".to_owned())
}

/// Configuration loaded from `/etc/miro-ego/button.json` (`deploy/miro-ego/miro-ego-button.json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ButtonConfig {
    #[serde(default = "default_schema")]
    pub schema: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "default_physical_pin")]
    pub physical_pin: u8,
    #[serde(default)]
    pub active_low: bool,
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
    #[serde(default = "default_cooldown_ms")]
    pub cooldown_ms: u64,
    #[serde(default = "default_poll_ms")]
    pub poll_ms: u64,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_double_press_window_ms")]
    pub double_press_window_ms: u64,
    #[serde(default = "default_long_press_ms")]
    pub long_press_ms: u64,
    #[serde(default = "default_status_led")]
    pub status_led: Option<String>,
    #[serde(default)]
    pub status_led_inverted: bool,
}

impl Default for ButtonConfig {
    fn default() -> Self {
        Self {
            schema: default_schema(),
            enabled: default_enabled(),
            physical_pin: default_physical_pin(),
            active_low: false,
            debounce_ms: default_debounce_ms(),
            cooldown_ms: default_cooldown_ms(),
            poll_ms: default_poll_ms(),
            request_timeout_ms: default_request_timeout_ms(),
            double_press_window_ms: default_double_press_window_ms(),
            long_press_ms: default_long_press_ms(),
            status_led: default_status_led(),
            status_led_inverted: false,
        }
    }
}

impl ButtonConfig {
    pub fn validate(&self) -> HwResult<()> {
        if self.schema != BUTTON_CONFIG_SCHEMA {
            return Err(HwError::Io {
                message: format!(
                    "button config schema must be {BUTTON_CONFIG_SCHEMA}, got {}",
                    self.schema
                ),
            });
        }
        if !RDK_X5_GPIO_PINS.contains(&self.physical_pin) {
            return Err(HwError::Io {
                message: format!(
                    "physical_pin {} is not a valid RDK X5 40-pin GPIO pin",
                    self.physical_pin
                ),
            });
        }
        if let Some(led) = &self.status_led {
            if !(1..=64).contains(&led.len())
                || !led
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b':' | b'-'))
            {
                return Err(HwError::Io {
                    message: format!("invalid status_led name: {led}"),
                });
            }
        }
        if !(20..=1000).contains(&self.debounce_ms) {
            return Err(HwError::Io {
                message: "debounce_ms must be in 20..=1000".to_owned(),
            });
        }
        if !(100..=10_000).contains(&self.cooldown_ms) {
            return Err(HwError::Io {
                message: "cooldown_ms must be in 100..=10000".to_owned(),
            });
        }
        if !(5..=100).contains(&self.poll_ms) {
            return Err(HwError::Io {
                message: "poll_ms must be in 5..=100".to_owned(),
            });
        }
        if !(100..=MAX_REQUEST_TIMEOUT_MS).contains(&self.request_timeout_ms) {
            return Err(HwError::Io {
                message: format!("request_timeout_ms must be in 100..={MAX_REQUEST_TIMEOUT_MS}"),
            });
        }
        if self.poll_ms > self.debounce_ms || self.cooldown_ms < self.debounce_ms {
            return Err(HwError::Io {
                message: "require poll_ms <= debounce_ms <= cooldown_ms".to_owned(),
            });
        }
        if !(100..=2000).contains(&self.double_press_window_ms) {
            return Err(HwError::Io {
                message: "double_press_window_ms must be in 100..=2000".to_owned(),
            });
        }
        if self.double_press_window_ms <= self.debounce_ms.saturating_mul(2) {
            return Err(HwError::Io {
                message: "double_press_window_ms must be > 2 * debounce_ms".to_owned(),
            });
        }
        if !(400..=10_000).contains(&self.long_press_ms) || self.long_press_ms <= self.debounce_ms {
            return Err(HwError::Io {
                message: "long_press_ms must be in 400..=10000 and > debounce_ms".to_owned(),
            });
        }
        Ok(())
    }

    /// Load and strictly validate `/etc/miro-ego/button.json` (bounded <= 8 KiB).
    pub fn load(path: &Path) -> HwResult<Self> {
        let cfg: Self =
            read_bounded_json(path, MAX_BUTTON_CONFIG_BYTES, 16).map_err(|err| HwError::Io {
                message: format!("failed to read button config {}: {err}", path.display()),
            })?;
        cfg.validate()?;
        Ok(cfg)
    }
}

/// Trait-abstracted raw digital GPIO input pin.
pub trait GpioPin: Send + Sync {
    /// Read current electrical logic level (`true` = high / `1`, `false` = low / `0`).
    fn read_level(&self) -> HwResult<bool>;
}

// ---------------------------------------------------------------------------
// Linux `/dev/gpiochip*` Character-Device GPIO (ABI v2 + v1 fallback)
// ---------------------------------------------------------------------------

const GPIO_MAX_NAME_SIZE: usize = 32;
const GPIO_V2_LINES_MAX: usize = 64;
const GPIO_V2_LINE_NUM_ATTRS_MAX: usize = 10;
const GPIOHANDLES_MAX: usize = 64;

const GPIO_V2_LINE_FLAG_INPUT: u64 = 1 << 2;
const GPIOHANDLE_REQUEST_INPUT: u32 = 1 << 0;

/// `_IOR(0xB4, 0x01, struct gpiochip_info)` (`sizeof == 68`).
const GPIO_GET_CHIPINFO_IOCTL: u32 = 0x8044_B401;
/// `_IOWR(0xB4, 0x07, struct gpio_v2_line_request)` (`sizeof == 592`).
const GPIO_V2_GET_LINE_IOCTL: u32 = 0xC250_B407;
/// `_IOWR(0xB4, 0x0E, struct gpio_v2_line_values)` (`sizeof == 16`).
const GPIO_V2_LINE_GET_VALUES_IOCTL: u32 = 0xC010_B40E;
/// `_IOWR(0xB4, 0x03, struct gpiohandle_request)` (`sizeof == 364`).
const GPIO_GET_LINEHANDLE_IOCTL: u32 = 0xC16C_B403;
/// `_IOWR(0xB4, 0x08, struct gpiohandle_data)` (`sizeof == 64`).
const GPIOHANDLE_GET_LINE_VALUES_IOCTL: u32 = 0xC040_B408;

#[repr(C)]
#[derive(Clone, Copy)]
struct GpioChipInfoRaw {
    name: [u8; GPIO_MAX_NAME_SIZE],
    label: [u8; GPIO_MAX_NAME_SIZE],
    lines: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct GpioV2LineAttributeRaw {
    id: u32,
    padding: u32,
    value: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct GpioV2LineConfigAttributeRaw {
    attr: GpioV2LineAttributeRaw,
    mask: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct GpioV2LineConfigRaw {
    flags: u64,
    num_attrs: u32,
    padding: [u32; 5],
    attrs: [GpioV2LineConfigAttributeRaw; GPIO_V2_LINE_NUM_ATTRS_MAX],
}

#[repr(C)]
#[derive(Clone, Copy)]
struct GpioV2LineRequestRaw {
    offsets: [u32; GPIO_V2_LINES_MAX],
    consumer: [u8; GPIO_MAX_NAME_SIZE],
    config: GpioV2LineConfigRaw,
    num_lines: u32,
    event_buffer_size: u32,
    padding: [u32; 5],
    fd: i32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct GpioV2LineValuesRaw {
    bits: u64,
    mask: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct GpioHandleRequestRaw {
    lineoffsets: [u32; GPIOHANDLES_MAX],
    flags: u32,
    default_values: [u8; GPIOHANDLES_MAX],
    consumer_label: [u8; GPIO_MAX_NAME_SIZE],
    lines: u32,
    fd: i32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct GpioHandleDataRaw {
    values: [u8; GPIOHANDLES_MAX],
}

const _: () = {
    assert!(std::mem::size_of::<GpioChipInfoRaw>() == 68);
    assert!(std::mem::size_of::<GpioV2LineAttributeRaw>() == 16);
    assert!(std::mem::size_of::<GpioV2LineConfigAttributeRaw>() == 24);
    assert!(std::mem::size_of::<GpioV2LineConfigRaw>() == 272);
    assert!(std::mem::size_of::<GpioV2LineRequestRaw>() == 592);
    assert!(std::mem::size_of::<GpioV2LineValuesRaw>() == 16);
    assert!(std::mem::size_of::<GpioHandleRequestRaw>() == 364);
    assert!(std::mem::size_of::<GpioHandleDataRaw>() == 64);
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GpioAbiVersion {
    V2,
    V1,
}

trait GpioIoctlOps: Send + Sync {
    fn query_chip_info(&self, chip_fd: RawFd) -> std::io::Result<(String, String, u32)>;
    fn request_line_v2(&self, chip_fd: RawFd, line_offset: u32) -> std::io::Result<OwnedFd>;
    fn request_line_v1(&self, chip_fd: RawFd, line_offset: u32) -> std::io::Result<OwnedFd>;
    fn read_value_v2(&self, line_fd: RawFd) -> std::io::Result<bool>;
    fn read_value_v1(&self, line_fd: RawFd) -> std::io::Result<bool>;
}

struct RealGpioIoctlOps;

fn c_bytes_to_string(raw: &[u8]) -> String {
    let end = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
    String::from_utf8_lossy(&raw[..end]).into_owned()
}

fn fill_consumer_label(dst: &mut [u8; GPIO_MAX_NAME_SIZE], label: &[u8]) {
    dst.fill(0);
    let max_copy = label.len().min(GPIO_MAX_NAME_SIZE - 1);
    dst[..max_copy].copy_from_slice(&label[..max_copy]);
}

impl GpioIoctlOps for RealGpioIoctlOps {
    fn query_chip_info(&self, chip_fd: RawFd) -> std::io::Result<(String, String, u32)> {
        // SAFETY: `GpioChipInfoRaw` is POD and zeroed before `GPIO_GET_CHIPINFO_IOCTL`.
        let mut info: GpioChipInfoRaw = unsafe { std::mem::zeroed() };
        #[allow(clippy::unnecessary_cast)]
        let req = GPIO_GET_CHIPINFO_IOCTL as libc::Ioctl;
        if unsafe { libc::ioctl(chip_fd, req, &mut info) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok((
            c_bytes_to_string(&info.name),
            c_bytes_to_string(&info.label),
            info.lines,
        ))
    }

    fn request_line_v2(&self, chip_fd: RawFd, line_offset: u32) -> std::io::Result<OwnedFd> {
        // SAFETY: `GpioV2LineRequestRaw` is POD and zero-initialized before setting request fields.
        let mut req_data: GpioV2LineRequestRaw = unsafe { std::mem::zeroed() };
        req_data.offsets[0] = line_offset;
        req_data.num_lines = 1;
        req_data.config.flags = GPIO_V2_LINE_FLAG_INPUT;
        fill_consumer_label(&mut req_data.consumer, b"miro-ego-button");

        #[allow(clippy::unnecessary_cast)]
        let ioctl_code = GPIO_V2_GET_LINE_IOCTL as libc::Ioctl;
        if unsafe { libc::ioctl(chip_fd, ioctl_code, &mut req_data) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        if req_data.fd < 0 {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        let _ = unsafe { libc::fcntl(req_data.fd, libc::F_SETFD, libc::FD_CLOEXEC) };
        // SAFETY: Kernel returned a fresh owned file descriptor in `req_data.fd`.
        Ok(unsafe { OwnedFd::from_raw_fd(req_data.fd) })
    }

    fn request_line_v1(&self, chip_fd: RawFd, line_offset: u32) -> std::io::Result<OwnedFd> {
        // SAFETY: `GpioHandleRequestRaw` is POD and zero-initialized before setting request fields.
        let mut req_data: GpioHandleRequestRaw = unsafe { std::mem::zeroed() };
        req_data.lineoffsets[0] = line_offset;
        req_data.lines = 1;
        req_data.flags = GPIOHANDLE_REQUEST_INPUT;
        fill_consumer_label(&mut req_data.consumer_label, b"miro-ego-button");

        #[allow(clippy::unnecessary_cast)]
        let ioctl_code = GPIO_GET_LINEHANDLE_IOCTL as libc::Ioctl;
        if unsafe { libc::ioctl(chip_fd, ioctl_code, &mut req_data) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        if req_data.fd < 0 {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        let _ = unsafe { libc::fcntl(req_data.fd, libc::F_SETFD, libc::FD_CLOEXEC) };
        // SAFETY: Kernel returned a fresh owned file descriptor in `req_data.fd`.
        Ok(unsafe { OwnedFd::from_raw_fd(req_data.fd) })
    }

    fn read_value_v2(&self, line_fd: RawFd) -> std::io::Result<bool> {
        let mut vals = GpioV2LineValuesRaw { bits: 0, mask: 1 };
        #[allow(clippy::unnecessary_cast)]
        let ioctl_code = GPIO_V2_LINE_GET_VALUES_IOCTL as libc::Ioctl;
        loop {
            if unsafe { libc::ioctl(line_fd, ioctl_code, &mut vals) } == 0 {
                return Ok((vals.bits & 1) != 0);
            }
            let err = std::io::Error::last_os_error();
            if err.kind() != ErrorKind::Interrupted {
                return Err(err);
            }
        }
    }

    fn read_value_v1(&self, line_fd: RawFd) -> std::io::Result<bool> {
        // SAFETY: `GpioHandleDataRaw` is POD `[u8; 64]`.
        let mut data: GpioHandleDataRaw = unsafe { std::mem::zeroed() };
        #[allow(clippy::unnecessary_cast)]
        let ioctl_code = GPIOHANDLE_GET_LINE_VALUES_IOCTL as libc::Ioctl;
        loop {
            if unsafe { libc::ioctl(line_fd, ioctl_code, &mut data) } == 0 {
                return Ok(data.values[0] != 0);
            }
            let err = std::io::Error::last_os_error();
            if err.kind() != ErrorKind::Interrupted {
                return Err(err);
            }
        }
    }
}

/// Linux `/dev/gpiochip*` character-device GPIO input pin holding a persistent line descriptor
/// (`GPIO_V2_GET_LINE_IOCTL` with automatic v1 `GPIO_GET_LINEHANDLE_IOCTL` fallback).
pub struct CharDevGpioPin {
    chip_path: PathBuf,
    line_offset: u32,
    abi: GpioAbiVersion,
    line_fd: OwnedFd,
    ops: Arc<dyn GpioIoctlOps>,
}

impl std::fmt::Debug for CharDevGpioPin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CharDevGpioPin")
            .field("chip_path", &self.chip_path)
            .field("line_offset", &self.line_offset)
            .field("abi", &self.abi)
            .finish()
    }
}

impl CharDevGpioPin {
    /// Open a specific `(chip_path, line_offset)` via Linux GPIO character-device ABI v2,
    /// falling back automatically to ABI v1 on older kernels.
    pub fn open_line(chip_path: &Path, line_offset: u32) -> HwResult<Self> {
        let chip_file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(chip_path)
            .map_err(|err| {
                if err.kind() == ErrorKind::NotFound {
                    HwError::Unavailable {
                        message: format!("gpiochip device missing: {}", chip_path.display()),
                    }
                } else {
                    HwError::Io {
                        message: format!("failed opening gpiochip {}: {err}", chip_path.display()),
                    }
                }
            })?;
        Self::open_line_with_ops(
            chip_path,
            chip_file.as_raw_fd(),
            line_offset,
            Arc::new(RealGpioIoctlOps),
        )
    }

    fn open_line_with_ops(
        chip_path: &Path,
        chip_fd: RawFd,
        line_offset: u32,
        ops: Arc<dyn GpioIoctlOps>,
    ) -> HwResult<Self> {
        match ops.request_line_v2(chip_fd, line_offset) {
            Ok(line_fd) => Ok(Self {
                chip_path: chip_path.to_path_buf(),
                line_offset,
                abi: GpioAbiVersion::V2,
                line_fd,
                ops,
            }),
            Err(v2_err) if is_v2_unsupported_errno(&v2_err) => {
                let line_fd = ops
                    .request_line_v1(chip_fd, line_offset)
                    .map_err(|v1_err| HwError::Io {
                        message: format!(
                            "failed requesting GPIO line {} on {} (v2: {v2_err}; v1: {v1_err})",
                            line_offset,
                            chip_path.display()
                        ),
                    })?;
                Ok(Self {
                    chip_path: chip_path.to_path_buf(),
                    line_offset,
                    abi: GpioAbiVersion::V1,
                    line_fd,
                    ops,
                })
            }
            Err(err) => Err(HwError::Io {
                message: format!(
                    "failed requesting GPIO v2 line {} on {}: {err}",
                    line_offset,
                    chip_path.display()
                ),
            }),
        }
    }

    /// Resolve a Sunrise X5 SoC GPIO number (`e.g. 401`) against `/dev/gpiochip*` (matching sysfs
    /// `gpiochip<base>/label` or Sunrise X5 controller bank labels) and open the line handle.
    pub fn open_soc_gpio_in(
        dev_root: &Path,
        sysfs_gpio_root: &Path,
        soc_gpio: u32,
    ) -> HwResult<Self> {
        let ops: Arc<dyn GpioIoctlOps> = Arc::new(RealGpioIoctlOps);
        let Some((chip_path, offset)) =
            discover_chardev_for_soc_gpio(dev_root, sysfs_gpio_root, soc_gpio, ops.as_ref())
        else {
            return Err(HwError::Unavailable {
                message: format!(
                    "no matching /dev/gpiochip* found for SoC GPIO {soc_gpio} under {}",
                    dev_root.display()
                ),
            });
        };
        Self::open_line(&chip_path, offset)
    }

    /// Open an RDK X5 40-pin BOARD physical header pin via `/dev/gpiochip*`.
    pub fn open_rdk_x5_board_pin(physical_pin: u8) -> HwResult<Self> {
        let soc_gpio = rdk_x5_board_pin_to_soc_gpio(physical_pin)?;
        Self::open_soc_gpio_in(
            Path::new(DEFAULT_DEV_ROOT),
            Path::new(DEFAULT_GPIO_SYSFS_ROOT),
            soc_gpio,
        )
    }

    pub fn chip_path(&self) -> &Path {
        &self.chip_path
    }

    pub fn line_offset(&self) -> u32 {
        self.line_offset
    }
}

impl GpioPin for CharDevGpioPin {
    fn read_level(&self) -> HwResult<bool> {
        let fd = self.line_fd.as_raw_fd();
        let res = match self.abi {
            GpioAbiVersion::V2 => self.ops.read_value_v2(fd),
            GpioAbiVersion::V1 => self.ops.read_value_v1(fd),
        };
        res.map_err(|err| HwError::Io {
            message: format!(
                "failed reading GPIO line {} on {}: {err}",
                self.line_offset,
                self.chip_path.display()
            ),
        })
    }
}

fn is_v2_unsupported_errno(err: &std::io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::ENOTTY | libc::EINVAL | libc::EOPNOTSUPP | libc::ENOSYS)
    )
}

fn discover_chardev_for_soc_gpio(
    dev_root: &Path,
    sysfs_gpio_root: &Path,
    soc_gpio: u32,
    ops: &dyn GpioIoctlOps,
) -> Option<(PathBuf, u32)> {
    // Optional sysfs `gpiochip<base>` metadata (`base`, `ngpio`, `label`) if readable.
    let mut sysfs_label_and_offset: Option<(String, u32)> = None;
    if let Ok(entries) = fs::read_dir(sysfs_gpio_root) {
        for entry in entries.filter_map(Result::ok) {
            let fname = entry.file_name().to_string_lossy().into_owned();
            if !fname.starts_with("gpiochip") {
                continue;
            }
            let p = entry.path();
            let Ok(base_txt) = fs::read_to_string(p.join("base")) else {
                continue;
            };
            let Ok(ngpio_txt) = fs::read_to_string(p.join("ngpio")) else {
                continue;
            };
            let (Ok(base), Ok(ngpio)) = (
                base_txt.trim().parse::<u32>(),
                ngpio_txt.trim().parse::<u32>(),
            ) else {
                continue;
            };
            if soc_gpio >= base && soc_gpio < base.saturating_add(ngpio) {
                if let Ok(lbl) = fs::read_to_string(p.join("label")) {
                    let trimmed = lbl.trim().to_owned();
                    if !trimmed.is_empty() {
                        sysfs_label_and_offset = Some((trimmed, soc_gpio - base));
                        break;
                    }
                }
            }
        }
    }

    let bank_fallback = rdk_x5_soc_gpio_to_bank_offset(soc_gpio);
    let entries = fs::read_dir(dev_root).ok()?;
    let mut dev_candidates: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.starts_with("gpiochip").then(|| e.path())
        })
        .collect();
    dev_candidates.sort();

    for chip_path in dev_candidates {
        let Ok(file) = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&chip_path)
        else {
            continue;
        };
        let Ok((name, label, lines)) = ops.query_chip_info(file.as_raw_fd()) else {
            continue;
        };
        if let Some((ref expected_label, offset)) = sysfs_label_and_offset {
            if label == *expected_label && offset < lines {
                return Some((chip_path, offset));
            }
        }
        if let Some((keywords, offset)) = bank_fallback {
            let lower_label = label.to_ascii_lowercase();
            let lower_name = name.to_ascii_lowercase();
            if offset < lines
                && keywords
                    .iter()
                    .any(|kw| lower_label.contains(kw) || lower_name.contains(kw))
            {
                return Some((chip_path, offset));
            }
        }
    }
    None
}

/// Unified platform GPIO input pin preferring `/dev/gpiochip*` (`GPIO_V2_GET_LINE_IOCTL` ->
/// `GPIO_GET_LINEHANDLE_IOCTL`) with automatic fallback to [`SysfsGpioPin`].
#[derive(Debug)]
pub enum PlatformGpioPin {
    CharDev(CharDevGpioPin),
    Sysfs(SysfsGpioPin),
}

impl PlatformGpioPin {
    /// Open an RDK X5 40-pin BOARD header pin via `/dev/gpiochip*`, falling back cleanly to
    /// `/sys/class/gpio` when character-device GPIO is unavailable.
    pub fn open_rdk_x5_board_pin(physical_pin: u8) -> HwResult<Self> {
        let soc_gpio = rdk_x5_board_pin_to_soc_gpio(physical_pin)?;
        match CharDevGpioPin::open_soc_gpio_in(
            Path::new(DEFAULT_DEV_ROOT),
            Path::new(DEFAULT_GPIO_SYSFS_ROOT),
            soc_gpio,
        ) {
            Ok(cdev) => Ok(Self::CharDev(cdev)),
            Err(_) => SysfsGpioPin::open_rdk_x5_board_pin(physical_pin).map(Self::Sysfs),
        }
    }

    /// Open a SoC GPIO number via `/dev/gpiochip*`, falling back to `/sys/class/gpio/gpio<N>/value`.
    pub fn from_gpio_number(soc_gpio: u32) -> Self {
        match CharDevGpioPin::open_soc_gpio_in(
            Path::new(DEFAULT_DEV_ROOT),
            Path::new(DEFAULT_GPIO_SYSFS_ROOT),
            soc_gpio,
        ) {
            Ok(cdev) => Self::CharDev(cdev),
            Err(_) => Self::Sysfs(SysfsGpioPin::from_gpio_number(soc_gpio)),
        }
    }

    pub fn from_value_path(path: impl Into<PathBuf>) -> Self {
        Self::Sysfs(SysfsGpioPin::from_value_path(path))
    }
}

impl GpioPin for PlatformGpioPin {
    fn read_level(&self) -> HwResult<bool> {
        match self {
            Self::CharDev(c) => c.read_level(),
            Self::Sysfs(s) => s.read_level(),
        }
    }
}

/// Linux sysfs GPIO input pin (`/sys/class/gpio/gpio<N>/value`).
#[derive(Debug, Clone)]
pub struct SysfsGpioPin {
    value_path: PathBuf,
}

impl SysfsGpioPin {
    pub fn from_gpio_number(soc_gpio: u32) -> Self {
        Self {
            value_path: PathBuf::from(format!("{DEFAULT_GPIO_SYSFS_ROOT}/gpio{soc_gpio}/value")),
        }
    }

    pub fn from_value_path(path: impl Into<PathBuf>) -> Self {
        Self {
            value_path: path.into(),
        }
    }

    /// Translate an RDK X5 40-pin `BOARD` physical header pin (`e.g. 37 -> SoC GPIO 401`),
    /// export `/sys/class/gpio/gpio<N>` if not yet exported, configure `direction = "in"`,
    /// and return a ready [`SysfsGpioPin`].
    pub fn open_rdk_x5_board_pin(physical_pin: u8) -> HwResult<Self> {
        Self::open_rdk_x5_board_pin_in(Path::new(DEFAULT_GPIO_SYSFS_ROOT), physical_pin)
    }

    /// Testable variant of [`SysfsGpioPin::open_rdk_x5_board_pin`] accepting a custom sysfs GPIO root.
    pub fn open_rdk_x5_board_pin_in(sysfs_gpio_root: &Path, physical_pin: u8) -> HwResult<Self> {
        let soc_gpio = rdk_x5_board_pin_to_soc_gpio(physical_pin)?;
        let pin_dir = sysfs_gpio_root.join(format!("gpio{soc_gpio}"));
        let value_path = pin_dir.join("value");

        if !value_path.exists() {
            let export_path = sysfs_gpio_root.join("export");
            if let Err(err) = fs::write(&export_path, format!("{soc_gpio}\n")) {
                // EBUSY (os error 16) occurs if another process exported the line concurrently.
                let is_ebusy =
                    err.raw_os_error() == Some(16) || err.kind() == ErrorKind::AlreadyExists;
                if err.kind() == ErrorKind::NotFound {
                    return Err(HwError::Unavailable {
                        message: format!(
                            "GPIO sysfs export interface unavailable: {}",
                            export_path.display()
                        ),
                    });
                }
                if !is_ebusy {
                    return Err(HwError::Io {
                        message: format!(
                            "failed exporting RDK X5 pin {physical_pin} (SoC GPIO {soc_gpio}) via {}: {err}",
                            export_path.display()
                        ),
                    });
                }
            }

            let wait_deadline = Instant::now() + Duration::from_millis(200);
            while !value_path.exists() && Instant::now() < wait_deadline {
                std::thread::sleep(Duration::from_millis(5));
            }
            if !value_path.exists() {
                return Err(HwError::Unavailable {
                    message: format!(
                        "GPIO sysfs value {} did not appear after exporting SoC GPIO {soc_gpio}",
                        value_path.display()
                    ),
                });
            }
        }

        let direction_path = pin_dir.join("direction");
        if direction_path.exists() {
            let needs_in = fs::read_to_string(&direction_path)
                .map(|cur| cur.trim() != "in")
                .unwrap_or(true);
            if needs_in {
                fs::write(&direction_path, "in\n").map_err(|err| HwError::Io {
                    message: format!(
                        "failed setting GPIO direction=in on {}: {err}",
                        direction_path.display()
                    ),
                })?;
            }
        }

        Ok(Self { value_path })
    }

    pub fn value_path(&self) -> &Path {
        &self.value_path
    }
}

impl GpioPin for SysfsGpioPin {
    fn read_level(&self) -> HwResult<bool> {
        let raw = fs::read_to_string(&self.value_path).map_err(|err| {
            if err.kind() == ErrorKind::NotFound {
                HwError::Unavailable {
                    message: format!(
                        "GPIO sysfs value unavailable: {}",
                        self.value_path.display()
                    ),
                }
            } else {
                HwError::Io {
                    message: format!("failed to read GPIO {}: {err}", self.value_path.display()),
                }
            }
        })?;
        match raw.trim() {
            "0" => Ok(false),
            "1" => Ok(true),
            other => Err(HwError::Io {
                message: format!("invalid GPIO level value: {other:?}"),
            }),
        }
    }
}

/// Deterministic debounce + short/double/long press classifier using monotonic milliseconds.
///
/// Requires a stable release after startup or [`ButtonClassifier::disarm`] before emitting
/// any press event, preventing stuck-button boot triggers or slow-request press queuing.
#[derive(Debug, Clone)]
pub struct ButtonClassifier {
    debounce_ms: u64,
    cooldown_ms: u64,
    double_press_window_ms: u64,
    long_press_ms: u64,
    single_press_only: bool,
    candidate_pressed: Option<bool>,
    candidate_since_ms: u64,
    stable_pressed: bool,
    armed: bool,
    press_started_ms: Option<u64>,
    long_press_fired: bool,
    pending_short_deadline_ms: Option<u64>,
    second_press_in_window: bool,
    last_event_ms: Option<u64>,
}

impl ButtonClassifier {
    pub fn from_config(config: &ButtonConfig) -> Self {
        Self::new(
            config.debounce_ms,
            config.cooldown_ms,
            config.double_press_window_ms,
            config.long_press_ms,
        )
    }

    pub fn new(
        debounce_ms: u64,
        cooldown_ms: u64,
        double_press_window_ms: u64,
        long_press_ms: u64,
    ) -> Self {
        Self {
            debounce_ms,
            cooldown_ms,
            double_press_window_ms,
            long_press_ms,
            single_press_only: false,
            candidate_pressed: None,
            candidate_since_ms: 0,
            stable_pressed: false,
            armed: false,
            press_started_ms: None,
            long_press_fired: false,
            pending_short_deadline_ms: None,
            second_press_in_window: false,
            last_event_ms: None,
        }
    }

    /// When enabled (used automatically by [`ButtonMonitor`]), emits [`ButtonEvent::ShortPress`]
    /// immediately on debounced release without 400ms double-press suppression, and never lets
    /// unhandled `DoublePress`/`LongPress` gestures swallow taps or stamp `cooldown_ms`.
    pub fn with_single_press_only(mut self, single_press_only: bool) -> Self {
        self.set_single_press_only(single_press_only);
        self
    }

    pub fn set_single_press_only(&mut self, single_press_only: bool) {
        self.single_press_only = single_press_only;
        if single_press_only {
            self.pending_short_deadline_ms = None;
            self.second_press_in_window = false;
        }
    }

    /// Disarm the button so input accumulated during a slow API command cannot fire another event.
    pub fn disarm(&mut self) {
        self.candidate_pressed = None;
        self.candidate_since_ms = 0;
        self.stable_pressed = false;
        self.armed = false;
        self.press_started_ms = None;
        self.long_press_fired = false;
        self.pending_short_deadline_ms = None;
        self.second_press_in_window = false;
    }

    /// Feed a sampled logical `pressed` state at monotonic timestamp `now_ms`.
    pub fn update(&mut self, pressed: bool, now_ms: u64) -> Option<ButtonEvent> {
        // Track raw debounce candidate first so a second press starting inside the window
        // does not get pre-empted mid-debounce by the pending short-press timer.
        if self.candidate_pressed != Some(pressed) {
            self.candidate_pressed = Some(pressed);
            self.candidate_since_ms = now_ms;
        }

        // Check if a pending single-click window has elapsed without a second press starting.
        if !self.single_press_only {
            if let Some(deadline) = self.pending_short_deadline_ms {
                let debouncing_second_press = pressed
                    && self.candidate_pressed == Some(true)
                    && self.candidate_since_ms <= deadline;
                if !self.stable_pressed
                    && !self.second_press_in_window
                    && !debouncing_second_press
                    && now_ms >= deadline
                {
                    self.pending_short_deadline_ms = None;
                    return self.emit_if_cooled_down(ButtonEvent::ShortPress, now_ms);
                }
            }
        }

        let debounced = now_ms.saturating_sub(self.candidate_since_ms) >= self.debounce_ms;
        if debounced {
            if !pressed {
                // Stable release.
                if !self.armed {
                    self.armed = true;
                    self.stable_pressed = false;
                    self.press_started_ms = None;
                    self.long_press_fired = false;
                    self.second_press_in_window = false;
                } else if self.stable_pressed {
                    self.stable_pressed = false;
                    let fired_long = self.long_press_fired;
                    let latched_double = self.second_press_in_window;
                    self.press_started_ms = None;
                    self.long_press_fired = false;
                    self.second_press_in_window = false;

                    if !fired_long {
                        if self.single_press_only {
                            self.pending_short_deadline_ms = None;
                            return self.emit_if_cooled_down(ButtonEvent::ShortPress, now_ms);
                        }
                        if latched_double
                            || self
                                .pending_short_deadline_ms
                                .is_some_and(|deadline| now_ms <= deadline)
                        {
                            self.pending_short_deadline_ms = None;
                            return self.emit_if_cooled_down(ButtonEvent::DoublePress, now_ms);
                        }
                        self.pending_short_deadline_ms =
                            Some(now_ms.saturating_add(self.double_press_window_ms));
                    }
                }
            } else if self.armed {
                // Stable press while armed.
                if !self.stable_pressed {
                    self.stable_pressed = true;
                    self.press_started_ms = Some(now_ms);
                    self.long_press_fired = false;
                    if !self.single_press_only {
                        if let Some(deadline) = self.pending_short_deadline_ms {
                            if self.candidate_since_ms <= deadline || now_ms <= deadline {
                                self.second_press_in_window = true;
                            }
                        }
                    }
                } else if !self.long_press_fired {
                    if let Some(started) = self.press_started_ms {
                        if now_ms.saturating_sub(started) >= self.long_press_ms {
                            self.long_press_fired = true;
                            self.pending_short_deadline_ms = None;
                            self.second_press_in_window = false;
                            if self.single_press_only {
                                // Do NOT stamp `last_event_ms` (`cooldown_ms`) for unhandled LongPress!
                                return Some(ButtonEvent::LongPress);
                            }
                            return self.emit_if_cooled_down(ButtonEvent::LongPress, now_ms);
                        }
                    }
                }
            }
        }

        None
    }

    fn emit_if_cooled_down(&mut self, event: ButtonEvent, now_ms: u64) -> Option<ButtonEvent> {
        if let Some(last) = self.last_event_ms {
            if now_ms.saturating_sub(last) < self.cooldown_ms {
                return None;
            }
        }
        self.last_event_ms = Some(now_ms);
        Some(event)
    }
}

/// Polling `ButtonInput` backed by any [`GpioPin`] and [`ButtonClassifier`].
pub struct GpioButtonInput<G: GpioPin> {
    pin: G,
    config: ButtonConfig,
    epoch: Instant,
    classifier: Mutex<ButtonClassifier>,
}

impl<G: GpioPin> GpioButtonInput<G> {
    pub fn new(pin: G, config: ButtonConfig) -> HwResult<Self> {
        config.validate()?;
        let classifier = ButtonClassifier::from_config(&config);
        Ok(Self {
            pin,
            config,
            epoch: Instant::now(),
            classifier: Mutex::new(classifier),
        })
    }

    pub fn with_single_press_only(self, single_press_only: bool) -> Self {
        self.set_single_press_only(single_press_only);
        self
    }

    pub fn set_single_press_only(&self, single_press_only: bool) {
        if let Ok(mut guard) = self.classifier.lock() {
            guard.set_single_press_only(single_press_only);
        }
    }

    pub fn disarm(&self) {
        if let Ok(mut guard) = self.classifier.lock() {
            guard.disarm();
        }
    }

    pub fn config(&self) -> &ButtonConfig {
        &self.config
    }
}

impl<G: GpioPin> ButtonInput for GpioButtonInput<G> {
    fn poll_event(&self, timeout_ms: u64) -> HwResult<Option<ButtonEvent>> {
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        let poll_interval = Duration::from_millis(self.config.poll_ms);

        loop {
            let level = self.pin.read_level()?;
            let pressed = level != self.config.active_low;
            let now_ms = self.epoch.elapsed().as_millis() as u64;

            let event = {
                let mut guard = self.classifier.lock().map_err(|_| HwError::Io {
                    message: "button classifier mutex poisoned".to_owned(),
                })?;
                guard.update(pressed, now_ms)
            };
            if event.is_some() {
                return Ok(event);
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            std::thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
        }
    }

    fn disarm(&self) {
        GpioButtonInput::disarm(self);
    }

    fn enable_single_press_mode(&self) {
        self.set_single_press_only(true);
    }
}

/// Action taken in response to a recording button press.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureToggleAction {
    Started,
    Stopped,
    Ignored { device_state: String },
}

/// Trait-abstracted capture toggle client invoked by the button monitor.
pub trait CaptureToggleClient: Send + Sync {
    fn toggle_capture(&self) -> HwResult<CaptureToggleAction>;
}

#[derive(Debug)]
struct PinnedCertVerifier {
    pinned_ders: Vec<CertificateDer<'static>>,
    supported: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for PinnedCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let matched = self.pinned_ders.iter().any(|pinned| {
            pinned.as_ref().len() == end_entity.as_ref().len()
                && bool::from(pinned.as_ref().ct_eq(end_entity.as_ref()))
        });
        if matched {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(
                "server certificate does not match pinned device certificate".to_owned(),
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.supported)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.supported)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported.supported_schemes()
    }
}

fn build_pinned_tls_config(cert_pem: &[u8]) -> HwResult<Arc<ClientConfig>> {
    let mut reader = Cursor::new(cert_pem);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| HwError::Io {
            message: format!("failed parsing pinned TLS certificate PEM: {err}"),
        })?;
    if certs.is_empty() {
        return Err(HwError::Io {
            message: "pinned TLS certificate PEM contains no CERTIFICATE blocks".to_owned(),
        });
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let supported = provider.signature_verification_algorithms;
    let verifier = Arc::new(PinnedCertVerifier {
        pinned_ders: certs,
        supported,
    });
    let cfg = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|err| HwError::Io {
            message: format!("failed configuring TLS protocol versions: {err}"),
        })?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    Ok(Arc::new(cfg))
}

fn loopback_addr_from_listen(listen: &ListenConfig) -> HwResult<SocketAddr> {
    let ip: IpAddr = listen.host.parse().map_err(|_| HwError::Io {
        message: format!("invalid listen.host IP address: {}", listen.host),
    })?;
    let target_ip = match ip {
        IpAddr::V4(v4) if v4.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(v6) if v6.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        concrete => concrete,
    };
    Ok(SocketAddr::new(target_ip, listen.port))
}

fn format_authority_host(addr: &SocketAddr) -> String {
    match addr {
        SocketAddr::V4(v4) => format!("{}:{}", v4.ip(), v4.port()),
        SocketAddr::V6(v6) => format!("[{}]:{}", v6.ip(), v6.port()),
    }
}

fn remaining_until_deadline(deadline: Instant) -> HwResult<Duration> {
    let now = Instant::now();
    match deadline.checked_duration_since(now) {
        Some(rem) if !rem.is_zero() => Ok(rem),
        _ => Err(HwError::Io {
            message: "capture API request exceeded total deadline".to_owned(),
        }),
    }
}

fn validate_http_header_token(token: &str, label: &str) -> HwResult<()> {
    if token.is_empty()
        || token
            .bytes()
            .any(|b| b == b'\r' || b == b'\n' || b < 0x20 || b == 0x7f)
    {
        return Err(HwError::Io {
            message: format!("{label} contains invalid control or newline characters"),
        });
    }
    Ok(())
}

/// Bounded HTTP/1.0 & HTTPS loopback client calling `GET /api/v1/capture/status` and
/// `POST /api/v1/capture/{start,stop}` on the local `miro-ego serve` daemon.
#[derive(Debug, Clone)]
pub struct LocalHttpCaptureClient {
    addr: SocketAddr,
    bearer_token: Option<String>,
    csrf_token: Option<String>,
    timeout: Duration,
    tls_config: Option<Arc<ClientConfig>>,
}

impl LocalHttpCaptureClient {
    pub fn new(addr: SocketAddr, bearer_token: Option<String>, timeout_ms: u64) -> Self {
        let capped = timeout_ms.clamp(100, MAX_REQUEST_TIMEOUT_MS);
        Self {
            addr,
            bearer_token,
            csrf_token: None,
            timeout: Duration::from_millis(capped),
            tls_config: None,
        }
    }

    /// Set a dedicated `X-CSRF-Token` header value distinct from the `Authorization: Bearer` token.
    pub fn with_csrf_token(mut self, csrf_token: impl Into<String>) -> Self {
        self.csrf_token = Some(csrf_token.into());
        self
    }

    /// Enable HTTPS loopback connections pinning the exact DER certificate(s) parsed from `cert_pem`
    /// while ignoring hostname mismatches on `127.0.0.1` / `[::1]`.
    pub fn with_pinned_tls_cert_pem(mut self, cert_pem: &[u8]) -> HwResult<Self> {
        self.tls_config = Some(build_pinned_tls_config(cert_pem)?);
        Ok(self)
    }

    /// Enable HTTPS loopback connections pinning the certificate(s) loaded from `cert_path`.
    pub fn with_pinned_tls_cert_file(self, cert_path: &Path) -> HwResult<Self> {
        let pem = read_bounded_bytes(cert_path, 64 * 1024).map_err(|err| HwError::Io {
            message: format!(
                "failed reading TLS certificate file {}: {err}",
                cert_path.display()
            ),
        })?;
        self.with_pinned_tls_cert_pem(&pem)
    }

    /// Construct a `LocalHttpCaptureClient` directly from `/etc/miro-ego/device.json`,
    /// automatically configuring `listen` port, `https` pinned certificate, and Customer
    /// `Authorization: Bearer` + `X-CSRF-Token` headers (surfacing DAC/read errors on credential files).
    pub fn from_device_config_path(config_path: &Path, timeout_ms: u64) -> HwResult<Self> {
        let cfg = load_device_config(config_path).map_err(|err| HwError::Io {
            message: format!(
                "failed loading device config {}: {err}",
                config_path.display()
            ),
        })?;
        let addr = loopback_addr_from_listen(&cfg.listen)?;
        match cfg.security {
            SecurityConfig::Lab { .. } => Ok(Self::new(addr, None, timeout_ms)),
            SecurityConfig::Customer {
                bearer_token_file,
                tls_certificate_file,
                ..
            } => {
                let raw_tok =
                    read_bounded_bytes(&bearer_token_file, 4096).map_err(|err| HwError::Io {
                        message: format!(
                            "failed reading bearer token {}: {err}",
                            bearer_token_file.display()
                        ),
                    })?;
                let token = String::from_utf8(raw_tok)
                    .map_err(|_| HwError::Io {
                        message: "bearer token file contains invalid UTF-8".to_owned(),
                    })?
                    .trim()
                    .to_owned();
                if token.is_empty() {
                    return Err(HwError::Io {
                        message: format!(
                            "bearer token file {} is empty",
                            bearer_token_file.display()
                        ),
                    });
                }
                validate_http_header_token(&token, "bearer_token")?;
                Self::new(addr, Some(token), timeout_ms)
                    .with_pinned_tls_cert_file(&tls_certificate_file)
            }
        }
    }

    fn http_json(&self, method: &str, path: &str, body: Option<&Value>) -> HwResult<(u16, Value)> {
        if let Some(tok) = &self.bearer_token {
            validate_http_header_token(tok, "bearer_token")?;
        }
        if let Some(csrf) = &self.csrf_token {
            validate_http_header_token(csrf, "csrf_token")?;
        }
        let deadline = Instant::now() + self.timeout;
        let connect_rem = remaining_until_deadline(deadline)?;
        let tcp = TcpStream::connect_timeout(&self.addr, connect_rem).map_err(|err| {
            HwError::Unavailable {
                message: format!("capture API {} unreachable: {err}", self.addr),
            }
        })?;
        let write_rem = remaining_until_deadline(deadline)?;
        tcp.set_read_timeout(Some(write_rem))
            .and_then(|()| tcp.set_write_timeout(Some(write_rem)))
            .map_err(|err| HwError::Io {
                message: format!("failed to configure capture API timeout: {err}"),
            })?;

        let scheme = if self.tls_config.is_some() {
            "https"
        } else {
            "http"
        };
        let authority = format_authority_host(&self.addr);
        let origin = format!("{scheme}://{authority}");
        let payload = match body {
            Some(v) => Some(serde_json::to_vec(v).map_err(|err| HwError::Io {
                message: format!("failed to serialize request body: {err}"),
            })?),
            None => None,
        };

        let mut req = format!(
            "{method} /api/v1{path} HTTP/1.0\r\nHost: {authority}\r\nAccept: application/json\r\nOrigin: {origin}\r\nConnection: close\r\n"
        );
        if let Some(tok) = &self.bearer_token {
            req.push_str(&format!("Authorization: Bearer {tok}\r\n"));
        }
        if let Some(bytes) = &payload {
            req.push_str("Content-Type: application/json\r\n");
            req.push_str(&format!("Idempotency-Key: {}\r\n", new_uuid_v4()));
            if let Some(csrf) = self.csrf_token.as_deref().or(self.bearer_token.as_deref()) {
                req.push_str(&format!("X-CSRF-Token: {csrf}\r\n"));
            }
            req.push_str(&format!("Content-Length: {}\r\n", bytes.len()));
        }
        req.push_str("\r\n");

        let raw = if let Some(tls_cfg) = &self.tls_config {
            let server_name = ServerName::try_from("localhost").map_err(|err| HwError::Io {
                message: format!("invalid TLS server name: {err}"),
            })?;
            let conn = ClientConnection::new(Arc::clone(tls_cfg), server_name).map_err(|err| {
                HwError::Io {
                    message: format!("TLS connection setup failed: {err}"),
                }
            })?;
            let mut tls_stream = StreamOwned::new(conn, tcp);
            tls_stream
                .write_all(req.as_bytes())
                .and_then(|()| {
                    if let Some(bytes) = &payload {
                        let rem =
                            remaining_until_deadline(deadline).map_err(std::io::Error::other)?;
                        let _ = tls_stream.get_ref().set_write_timeout(Some(rem));
                        tls_stream.write_all(bytes)?;
                    }
                    let rem = remaining_until_deadline(deadline).map_err(std::io::Error::other)?;
                    let _ = tls_stream.get_ref().set_write_timeout(Some(rem));
                    tls_stream.flush()
                })
                .map_err(|err| HwError::Io {
                    message: format!("capture HTTPS API write/handshake failed: {err}"),
                })?;
            read_http_response_bounded_tls(&mut tls_stream, deadline)?
        } else {
            let mut plain = tcp;
            plain
                .write_all(req.as_bytes())
                .and_then(|()| {
                    if let Some(bytes) = &payload {
                        let rem =
                            remaining_until_deadline(deadline).map_err(std::io::Error::other)?;
                        let _ = plain.set_write_timeout(Some(rem));
                        plain.write_all(bytes)?;
                    }
                    let rem = remaining_until_deadline(deadline).map_err(std::io::Error::other)?;
                    let _ = plain.set_write_timeout(Some(rem));
                    plain.flush()
                })
                .map_err(|err| HwError::Io {
                    message: format!("capture API write failed: {err}"),
                })?;
            read_http_response_bounded_plain(&mut plain, deadline)?
        };

        let header_end = raw
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .ok_or_else(|| HwError::Io {
                message: "malformed HTTP response from capture API".to_owned(),
            })?;
        let header_str = std::str::from_utf8(&raw[..header_end]).map_err(|_| HwError::Io {
            message: "non-UTF8 HTTP response headers".to_owned(),
        })?;
        let status_line = header_str.lines().next().ok_or_else(|| HwError::Io {
            message: "empty HTTP response status line".to_owned(),
        })?;
        let status_code: u16 = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| HwError::Io {
                message: format!("invalid HTTP status line: {status_line}"),
            })?;
        for line in header_str.lines().skip(1) {
            if let Some((k, v)) = line.split_once(':') {
                if k.trim().eq_ignore_ascii_case("transfer-encoding")
                    && v.to_ascii_lowercase().contains("chunked")
                {
                    return Err(HwError::Io {
                        message: "unsupported Transfer-Encoding: chunked from capture API"
                            .to_owned(),
                    });
                }
            }
        }

        let body_bytes = &raw[header_end + 4..];
        if status_code == 204 || body_bytes.is_empty() {
            return Ok((status_code, Value::Null));
        }
        let parsed: Value = serde_json::from_slice(body_bytes).map_err(|err| HwError::Io {
            message: format!("invalid JSON from capture API: {err}"),
        })?;
        Ok((status_code, parsed))
    }
}

fn read_http_response_bounded_plain(
    stream: &mut TcpStream,
    deadline: Instant,
) -> HwResult<Vec<u8>> {
    read_http_response_with_deadline(stream, deadline, false, |s, rem| {
        let _ = s.set_read_timeout(Some(rem));
    })
}

fn read_http_response_bounded_tls(
    stream: &mut StreamOwned<ClientConnection, TcpStream>,
    deadline: Instant,
) -> HwResult<Vec<u8>> {
    read_http_response_with_deadline(stream, deadline, true, |s, rem| {
        let _ = s.get_ref().set_read_timeout(Some(rem));
    })
}

fn read_http_response_with_deadline<R, F>(
    reader: &mut R,
    deadline: Instant,
    tolerate_tls_eof: bool,
    mut set_timeout: F,
) -> HwResult<Vec<u8>>
where
    R: Read,
    F: FnMut(&mut R, Duration),
{
    let mut raw = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let rem = remaining_until_deadline(deadline)?;
        set_timeout(reader, rem);
        match reader.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                raw.extend_from_slice(&chunk[..n]);
                if raw.len() > MAX_HTTP_RESPONSE_BYTES {
                    return Err(HwError::Io {
                        message: "capture API response exceeded 1 MiB limit".to_owned(),
                    });
                }
                if has_chunked_transfer_encoding(&raw) {
                    return Err(HwError::Io {
                        message: "unsupported Transfer-Encoding: chunked from capture API"
                            .to_owned(),
                    });
                }
                if let Some(expected_total) = complete_content_length_total(&raw) {
                    if raw.len() >= expected_total {
                        raw.truncate(expected_total);
                        break;
                    }
                }
            }
            Err(err) if err.kind() == ErrorKind::Interrupted => continue,
            Err(err)
                if tolerate_tls_eof
                    && err.kind() == ErrorKind::UnexpectedEof
                    && !raw.is_empty() =>
            {
                break;
            }
            Err(err) => {
                return Err(HwError::Io {
                    message: format!("capture API read failed: {err}"),
                });
            }
        }
    }
    Ok(raw)
}

fn has_chunked_transfer_encoding(raw: &[u8]) -> bool {
    let Some(header_end) = raw.windows(4).position(|w| w == b"\r\n\r\n") else {
        return false;
    };
    let Ok(header_str) = std::str::from_utf8(&raw[..header_end]) else {
        return false;
    };
    header_str.lines().skip(1).any(|line| {
        line.split_once(':').is_some_and(|(k, v)| {
            k.trim().eq_ignore_ascii_case("transfer-encoding")
                && v.to_ascii_lowercase().contains("chunked")
        })
    })
}

fn complete_content_length_total(raw: &[u8]) -> Option<usize> {
    let header_end = raw.windows(4).position(|w| w == b"\r\n\r\n")?;
    let header_str = std::str::from_utf8(&raw[..header_end]).ok()?;
    for line in header_str.lines().skip(1) {
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                let len: usize = v.trim().parse().ok()?;
                return header_end.checked_add(4)?.checked_add(len);
            }
        }
    }
    None
}

impl CaptureToggleClient for LocalHttpCaptureClient {
    fn toggle_capture(&self) -> HwResult<CaptureToggleAction> {
        let (code, status_doc) = self.http_json("GET", "/capture/status", None)?;
        if !(200..300).contains(&code) {
            return Err(HwError::Io {
                message: format!("/capture/status returned HTTP {code}"),
            });
        }
        let device_state = status_doc
            .get("snapshot")
            .and_then(|s| s.get("device_state"))
            .or_else(|| status_doc.get("device_state"))
            .and_then(Value::as_str)
            .ok_or_else(|| HwError::Io {
                message: "capture status missing device_state".to_owned(),
            })?;

        match device_state {
            "recording" => {
                let (stop_code, _) = self.http_json(
                    "POST",
                    "/capture/stop",
                    Some(&json!({
                        "schema": "miro-ego.capture-stop.v1",
                        "reason": "user"
                    })),
                )?;
                if !(200..300).contains(&stop_code) {
                    return Err(HwError::Io {
                        message: format!("/capture/stop returned HTTP {stop_code}"),
                    });
                }
                Ok(CaptureToggleAction::Stopped)
            }
            "idle" => {
                let (start_code, _) = self.http_json(
                    "POST",
                    "/capture/start",
                    Some(&json!({
                        "schema": "miro-ego.capture-start.v1",
                        "mode": "production",
                        "take": { "kind": "new" }
                    })),
                )?;
                if !(200..300).contains(&start_code) {
                    return Err(HwError::Io {
                        message: format!("/capture/start returned HTTP {start_code}"),
                    });
                }
                Ok(CaptureToggleAction::Started)
            }
            other => Ok(CaptureToggleAction::Ignored {
                device_state: other.to_owned(),
            }),
        }
    }
}

/// High-level monitor dispatching debounced button presses to a [`CaptureToggleClient`].
pub struct ButtonMonitor<B: ButtonInput, C: CaptureToggleClient> {
    input: B,
    client: C,
}

impl<B: ButtonInput, C: CaptureToggleClient> ButtonMonitor<B, C> {
    pub fn new(input: B, client: C) -> Self {
        input.enable_single_press_mode();
        Self { input, client }
    }

    /// Poll once for up to `timeout_ms` and dispatch any recognized event, always disarming
    /// the button input after a `ShortPress` toggle attempt.
    pub fn step(
        &self,
        timeout_ms: u64,
    ) -> HwResult<Option<(ButtonEvent, Option<CaptureToggleAction>)>> {
        let Some(event) = self.input.poll_event(timeout_ms)? else {
            return Ok(None);
        };
        let action = match event {
            ButtonEvent::ShortPress => {
                let toggle_res = self.client.toggle_capture();
                self.input.disarm();
                Some(toggle_res?)
            }
            ButtonEvent::DoublePress | ButtonEvent::LongPress => None,
        };
        Ok(Some((event, action)))
    }

    /// Run continuously until `stop` becomes `true`, recovering from transient GPIO `HwError::Io`
    /// errors with bounded backoff + disarming (exiting on persistent I/O failures) and logging
    /// transient capture API errors without terminating the button monitor loop.
    pub fn run_until(&self, stop: &AtomicBool, poll_slice_ms: u64) -> HwResult<()> {
        const MAX_CONSECUTIVE_POLL_IO_ERRORS: u32 = 5;
        let mut consecutive_io_errs: u32 = 0;
        while !stop.load(Ordering::Acquire) {
            let event = match self.input.poll_event(poll_slice_ms) {
                Ok(ev) => {
                    consecutive_io_errs = 0;
                    ev
                }
                Err(HwError::Io { message }) => {
                    consecutive_io_errs = consecutive_io_errs.saturating_add(1);
                    self.input.disarm();
                    eprintln!(
                        "miro-ego-button: transient GPIO poll error (attempt {consecutive_io_errs}/{MAX_CONSECUTIVE_POLL_IO_ERRORS}, continuing): {message}"
                    );
                    if consecutive_io_errs >= MAX_CONSECUTIVE_POLL_IO_ERRORS {
                        return Err(HwError::Io { message });
                    }
                    let backoff_ms = 25_u64
                        .saturating_mul(1_u64 << (consecutive_io_errs - 1).min(4))
                        .min(250);
                    let backoff_deadline = Instant::now() + Duration::from_millis(backoff_ms);
                    while !stop.load(Ordering::Acquire) && Instant::now() < backoff_deadline {
                        let rem = backoff_deadline.saturating_duration_since(Instant::now());
                        std::thread::sleep(rem.min(Duration::from_millis(20)));
                    }
                    continue;
                }
                Err(err) => return Err(err),
            };
            let Some(event) = event else {
                continue;
            };
            if event == ButtonEvent::ShortPress {
                let toggle_res = self.client.toggle_capture();
                self.input.disarm();
                if let Err(err) = toggle_res {
                    eprintln!("miro-ego-button: transient toggle error (continuing): {err}");
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::fs::File;
    use std::net::TcpListener;
    use std::sync::atomic::AtomicUsize;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(tag: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "miro-ego-btn-{tag}-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn loads_deploy_button_json_and_validates_pin_allowlist_and_30s_timeout() {
        let deploy_cfg = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/miro-ego/miro-ego-button.json");
        let cfg = ButtonConfig::load(&deploy_cfg).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.physical_pin, 37);
        assert_eq!(cfg.debounce_ms, 80);
        assert_eq!(cfg.cooldown_ms, 1000);
        assert_eq!(cfg.poll_ms, 20);
        assert_eq!(cfg.status_led.as_deref(), Some("ACT"));

        // 25s..30s request_timeout_ms (exceeding 20s ENCODER_FINISH_TIMEOUT) MUST be accepted!
        let mut long_timeout = cfg.clone();
        long_timeout.request_timeout_ms = 25_000;
        long_timeout.validate().unwrap();
        long_timeout.request_timeout_ms = 30_000;
        long_timeout.validate().unwrap();
        long_timeout.request_timeout_ms = 30_001;
        assert!(long_timeout.validate().is_err());

        // Pin 1 is 3.3V power — must be rejected!
        let mut bad = cfg.clone();
        bad.physical_pin = 1;
        assert!(bad.validate().is_err());

        // Pin 6 is GND — must be rejected!
        bad.physical_pin = 6;
        assert!(bad.validate().is_err());

        // double_press_window_ms <= 2 * debounce_ms must be rejected!
        let mut narrow = cfg.clone();
        narrow.debounce_ms = 100;
        narrow.double_press_window_ms = 200;
        assert!(narrow.validate().is_err());
    }

    #[test]
    fn maps_all_28_rdk_x5_board_pins_and_exports_sysfs_gpio_direction_in() {
        assert_eq!(rdk_x5_board_pin_to_soc_gpio(37).unwrap(), 401);
        assert_eq!(
            rdk_x5_soc_gpio_to_bank_offset(401),
            Some((&["lsio_gpio0", "32140000"][..], 22))
        );
        assert!(rdk_x5_board_pin_to_soc_gpio(1).is_err());
        assert!(rdk_x5_board_pin_to_soc_gpio(6).is_err());
        assert!(rdk_x5_board_pin_to_soc_gpio(39).is_err());

        let mut seen_soc = HashSet::new();
        for &board_pin in RDK_X5_GPIO_PINS {
            let soc = rdk_x5_board_pin_to_soc_gpio(board_pin).unwrap();
            assert!(
                rdk_x5_soc_gpio_to_bank_offset(soc).is_some(),
                "missing bank mapping for SoC GPIO {soc}"
            );
            assert!(
                seen_soc.insert(soc),
                "duplicate SoC GPIO {soc} for pin {board_pin}"
            );
        }
        assert_eq!(seen_soc.len(), 28);

        // Simulate unexported sysfs root where writing `401\n` to `export` materializes `gpio401/`.
        let root = temp_dir("sysfs-export");
        let export_file = root.join("export");
        fs::write(&export_file, "").unwrap();
        let root_bg = root.clone();
        let exporter = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < deadline {
                if let Ok(txt) = fs::read_to_string(root_bg.join("export")) {
                    if txt.trim() == "401" {
                        let gdir = root_bg.join("gpio401");
                        fs::create_dir_all(&gdir).unwrap();
                        fs::write(gdir.join("direction"), "out\n").unwrap();
                        fs::write(gdir.join("value"), "1\n").unwrap();
                        break;
                    }
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        });

        let pin = SysfsGpioPin::open_rdk_x5_board_pin_in(&root, 37).unwrap();
        exporter.join().unwrap();
        assert_eq!(
            fs::read_to_string(root.join("gpio401/direction"))
                .unwrap()
                .trim(),
            "in"
        );
        assert!(pin.read_level().unwrap());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn chardev_gpio_supports_v2_and_falls_back_to_v1_cleanly() {
        struct MockGpioIoctl {
            allow_v2: bool,
            level: AtomicBool,
            v2_requests: AtomicUsize,
            v1_requests: AtomicUsize,
        }
        impl GpioIoctlOps for MockGpioIoctl {
            fn query_chip_info(&self, _chip_fd: RawFd) -> std::io::Result<(String, String, u32)> {
                Ok(("gpiochip0".to_owned(), "32140000.gpio".to_owned(), 32))
            }
            fn request_line_v2(
                &self,
                _chip_fd: RawFd,
                line_offset: u32,
            ) -> std::io::Result<OwnedFd> {
                self.v2_requests.fetch_add(1, Ordering::SeqCst);
                assert_eq!(line_offset, 22);
                if !self.allow_v2 {
                    return Err(std::io::Error::from_raw_os_error(libc::ENOTTY));
                }
                let f = File::open("/dev/null")?;
                Ok(OwnedFd::from(f))
            }
            fn request_line_v1(
                &self,
                _chip_fd: RawFd,
                line_offset: u32,
            ) -> std::io::Result<OwnedFd> {
                self.v1_requests.fetch_add(1, Ordering::SeqCst);
                assert_eq!(line_offset, 22);
                let f = File::open("/dev/null")?;
                Ok(OwnedFd::from(f))
            }
            fn read_value_v2(&self, _line_fd: RawFd) -> std::io::Result<bool> {
                Ok(self.level.load(Ordering::SeqCst))
            }
            fn read_value_v1(&self, _line_fd: RawFd) -> std::io::Result<bool> {
                Ok(self.level.load(Ordering::SeqCst))
            }
        }

        let dir = temp_dir("chardev");
        let fake_chip = dir.join("gpiochip0");
        fs::write(&fake_chip, b"").unwrap();

        // 1. GPIO v2 character-device path
        let v2_ops = Arc::new(MockGpioIoctl {
            allow_v2: true,
            level: AtomicBool::new(true),
            v2_requests: AtomicUsize::new(0),
            v1_requests: AtomicUsize::new(0),
        });
        let (resolved_chip, offset) =
            discover_chardev_for_soc_gpio(&dir, &dir, 401, v2_ops.as_ref()).unwrap();
        assert_eq!(resolved_chip, fake_chip);
        assert_eq!(offset, 22);

        let pin_v2 =
            CharDevGpioPin::open_line_with_ops(&fake_chip, 0, offset, v2_ops.clone()).unwrap();
        assert_eq!(pin_v2.abi, GpioAbiVersion::V2);
        assert!(pin_v2.read_level().unwrap());
        v2_ops.level.store(false, Ordering::SeqCst);
        assert!(!pin_v2.read_level().unwrap());
        assert_eq!(v2_ops.v1_requests.load(Ordering::SeqCst), 0);

        // 2. GPIO v2 ENOTTY -> automatic fallback to GPIO v1 linehandle!
        let v1_ops = Arc::new(MockGpioIoctl {
            allow_v2: false,
            level: AtomicBool::new(true),
            v2_requests: AtomicUsize::new(0),
            v1_requests: AtomicUsize::new(0),
        });
        let pin_v1 =
            CharDevGpioPin::open_line_with_ops(&fake_chip, 0, offset, v1_ops.clone()).unwrap();
        assert_eq!(pin_v1.abi, GpioAbiVersion::V1);
        assert_eq!(v1_ops.v2_requests.load(Ordering::SeqCst), 1);
        assert_eq!(v1_ops.v1_requests.load(Ordering::SeqCst), 1);
        assert!(pin_v1.read_level().unwrap());

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn classifies_debounced_short_double_and_long_presses_accurately() {
        let mut clf = ButtonClassifier::new(80, 500, 350, 1200);

        // Startup while held: must NOT fire until stable release!
        assert_eq!(clf.update(true, 0), None);
        assert_eq!(clf.update(true, 100), None);
        assert_eq!(clf.update(true, 2000), None);

        // Stable release arms the button.
        assert_eq!(clf.update(false, 2100), None);
        assert_eq!(clf.update(false, 2200), None);

        // Glitch shorter than 80ms debounce is ignored.
        assert_eq!(clf.update(true, 2300), None);
        assert_eq!(clf.update(true, 2340), None);
        assert_eq!(clf.update(false, 2350), None);
        assert_eq!(clf.update(false, 2900), None);

        // Short press: press for 200ms, release, wait past 350ms double-press window.
        assert_eq!(clf.update(true, 3000), None);
        assert_eq!(clf.update(true, 3090), None);
        assert_eq!(clf.update(false, 3200), None);
        assert_eq!(clf.update(false, 3290), None); // stable release at 3290 -> deadline 3640
        assert_eq!(clf.update(false, 3500), None);
        assert_eq!(clf.update(false, 3650), Some(ButtonEvent::ShortPress));

        // Double press where 2nd press STARTS inside 350ms window (at 4750 <= 4790)
        // and RELEASES AFTER 4790 (at 4950) -> MUST still latch DoublePress!
        assert_eq!(clf.update(true, 4200), None);
        assert_eq!(clf.update(true, 4290), None);
        assert_eq!(clf.update(false, 4350), None);
        assert_eq!(clf.update(false, 4440), None); // 1st click pending until 4790
        assert_eq!(clf.update(true, 4750), None); // 2nd press starts inside window!
        assert_eq!(clf.update(true, 4800), None); // mid-debounce past 4790 must NOT fire ShortPress!
        assert_eq!(clf.update(true, 4840), None); // 2nd press debounced -> latched!
        assert_eq!(clf.update(false, 4950), None);
        assert_eq!(clf.update(false, 5040), Some(ButtonEvent::DoublePress));

        // Long press: held >= 1200ms after stable press.
        assert_eq!(clf.update(true, 6000), None);
        assert_eq!(clf.update(true, 6090), None);
        assert_eq!(clf.update(true, 7000), None);
        assert_eq!(clf.update(true, 7290), Some(ButtonEvent::LongPress));
        assert_eq!(clf.update(true, 8500), None);
        assert_eq!(clf.update(false, 8600), None);
        assert_eq!(clf.update(false, 8700), None);
        assert_eq!(clf.update(false, 9500), None);
    }

    #[test]
    fn single_press_only_mode_emits_immediately_and_long_press_never_stamps_cooldown() {
        let mut clf = ButtonClassifier::new(50, 1000, 400, 1000).with_single_press_only(true);

        // Arm via initial release.
        assert_eq!(clf.update(false, 0), None);
        assert_eq!(clf.update(false, 60), None);

        // Short tap emits ShortPress IMMEDIATELY on debounced release (0ms double-press wait!).
        assert_eq!(clf.update(true, 100), None);
        assert_eq!(clf.update(true, 160), None);
        assert_eq!(clf.update(false, 200), None);
        assert_eq!(clf.update(false, 260), Some(ButtonEvent::ShortPress));

        // Long press after cooldown emits LongPress WITHOUT stamping cooldown_ms, so an
        // immediate follow-up short tap at 2650ms (< 1000ms after LongPress) still fires!
        assert_eq!(clf.update(true, 1400), None);
        assert_eq!(clf.update(true, 1460), None);
        assert_eq!(clf.update(true, 2500), Some(ButtonEvent::LongPress));
        assert_eq!(clf.update(false, 2520), None);
        assert_eq!(clf.update(false, 2580), None);

        assert_eq!(clf.update(true, 2600), None);
        assert_eq!(clf.update(true, 2660), None);
        assert_eq!(clf.update(false, 2700), None);
        assert_eq!(clf.update(false, 2760), Some(ButtonEvent::ShortPress));
    }

    #[test]
    fn local_http_capture_client_toggles_idle_recording_and_enforces_total_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let step = Arc::new(AtomicUsize::new(0));
        let step_srv = Arc::clone(&step);

        let srv = std::thread::spawn(move || {
            for _ in 0..4 {
                let (mut sock, _) = listener.accept().unwrap();
                let mut buf = [0u8; 2048];
                let n = sock.read(&mut buf).unwrap();
                let req = String::from_utf8_lossy(&buf[..n]);
                let n_req = step_srv.fetch_add(1, Ordering::SeqCst);
                let resp = match n_req {
                    0 => {
                        assert!(req.starts_with("GET /api/v1/capture/status "));
                        let body = r#"{"schema":"miro-ego.capture-status.v1","snapshot":{"device_state":"idle"}}"#;
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        )
                    }
                    1 => {
                        assert!(req.starts_with("POST /api/v1/capture/start "));
                        assert!(req.contains("Idempotency-Key:"));
                        assert!(req.contains("Authorization: Bearer test-bearer-token\r\n"));
                        assert!(req.contains("X-CSRF-Token: test-csrf-token\r\n"));
                        assert!(!req.contains("X-CSRF-Token: test-bearer-token\r\n"));
                        "HTTP/1.1 202 Accepted\r\nContent-Length: 2\r\n\r\n{}".to_owned()
                    }
                    2 => {
                        let body = r#"{"schema":"miro-ego.capture-status.v1","snapshot":{"device_state":"recording"}}"#;
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        )
                    }
                    _ => {
                        assert!(req.starts_with("POST /api/v1/capture/stop "));
                        assert!(req.contains("Authorization: Bearer test-bearer-token\r\n"));
                        assert!(req.contains("X-CSRF-Token: test-csrf-token\r\n"));
                        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}".to_owned()
                    }
                };
                sock.write_all(resp.as_bytes()).unwrap();
            }
        });

        let client =
            LocalHttpCaptureClient::new(addr, Some("test-bearer-token".to_owned()), 25_000)
                .with_csrf_token("test-csrf-token");
        assert_eq!(client.timeout, Duration::from_millis(25_000));
        assert_eq!(
            client.toggle_capture().unwrap(),
            CaptureToggleAction::Started
        );
        assert_eq!(
            client.toggle_capture().unwrap(),
            CaptureToggleAction::Stopped
        );
        srv.join().unwrap();

        // Slowloris dribble server (1 byte every 40ms) MUST be cut off by total 150ms deadline!
        let slow_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let slow_addr = slow_listener.local_addr().unwrap();
        let slow_srv = std::thread::spawn(move || {
            if let Ok((mut sock, _)) = slow_listener.accept() {
                let mut buf = [0u8; 512];
                let _ = sock.read(&mut buf);
                for &b in b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n" {
                    if sock.write_all(&[b]).is_err() {
                        break;
                    }
                    let _ = sock.flush();
                    std::thread::sleep(Duration::from_millis(40));
                }
            }
        });
        let fast_deadline_client = LocalHttpCaptureClient::new(slow_addr, None, 150);
        let t0 = Instant::now();
        assert!(fast_deadline_client.toggle_capture().is_err());
        assert!(t0.elapsed() < Duration::from_secs(1));
        slow_srv.join().unwrap();
    }

    #[test]
    fn https_pinned_capture_client_and_resilient_button_monitor_survive_transient_500() {
        use rustls::pki_types::PrivateKeyDer;
        use rustls::{ServerConfig, ServerConnection};

        let certified = rcgen::generate_simple_self_signed(vec!["miro-ego.local".into()]).unwrap();
        let cert_pem = certified.cert.pem();
        let cert_der = certified.cert.der().clone();
        let key_der = PrivateKeyDer::try_from(certified.key_pair.serialize_der()).unwrap();

        let srv_cfg = Arc::new(
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(vec![cert_der], key_der)
                .unwrap(),
        );

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let srv_calls = Arc::new(AtomicUsize::new(0));
        let srv_calls_bg = Arc::clone(&srv_calls);

        let srv = std::thread::spawn(move || {
            // 1st call returns 503 Service Unavailable; 2nd + 3rd succeed (status + start).
            for _ in 0..3 {
                let (tcp, _) = listener.accept().unwrap();
                let conn = ServerConnection::new(Arc::clone(&srv_cfg)).unwrap();
                let mut tls = StreamOwned::new(conn, tcp);
                let mut buf = [0u8; 2048];
                let n = tls.read(&mut buf).unwrap();
                let req = String::from_utf8_lossy(&buf[..n]);
                assert!(req.contains(&format!("Origin: https://{addr}\r\n")));
                assert!(req.contains("Authorization: Bearer pinned-tok-1\r\n"));
                let idx = srv_calls_bg.fetch_add(1, Ordering::SeqCst);
                let resp = match idx {
                    0 => {
                        "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 2\r\n\r\n{}".to_owned()
                    }
                    1 => {
                        let body = r#"{"schema":"miro-ego.capture-status.v1","snapshot":{"device_state":"idle"}}"#;
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        )
                    }
                    _ => {
                        assert!(req.contains("X-CSRF-Token: pinned-tok-1\r\n"));
                        "HTTP/1.1 202 Accepted\r\nContent-Length: 2\r\n\r\n{}".to_owned()
                    }
                };
                tls.write_all(resp.as_bytes()).unwrap();
                let _ = tls.flush();
            }
        });

        let client = LocalHttpCaptureClient::new(addr, Some("pinned-tok-1".to_owned()), 8500)
            .with_pinned_tls_cert_pem(cert_pem.as_bytes())
            .unwrap();

        struct ScriptedButtonInput {
            polls: AtomicUsize,
            disarms: AtomicUsize,
            stop_flag: Arc<AtomicBool>,
        }
        impl ButtonInput for ScriptedButtonInput {
            fn poll_event(&self, _timeout_ms: u64) -> HwResult<Option<ButtonEvent>> {
                let idx = self.polls.fetch_add(1, Ordering::SeqCst);
                match idx {
                    0 | 1 => Ok(Some(ButtonEvent::ShortPress)),
                    _ => {
                        self.stop_flag.store(true, Ordering::Release);
                        Ok(None)
                    }
                }
            }
            fn disarm(&self) {
                self.disarms.fetch_add(1, Ordering::SeqCst);
            }
        }

        let stop = Arc::new(AtomicBool::new(false));
        let input = ScriptedButtonInput {
            polls: AtomicUsize::new(0),
            disarms: AtomicUsize::new(0),
            stop_flag: Arc::clone(&stop),
        };
        let monitor = ButtonMonitor::new(input, client);
        // First ShortPress hits HTTP 503 -> must NOT exit run_until, must disarm, and 2nd ShortPress succeeds!
        monitor.run_until(&stop, 10).unwrap();
        assert_eq!(monitor.input.disarms.load(Ordering::SeqCst), 2);
        assert_eq!(srv_calls.load(Ordering::SeqCst), 3);

        srv.join().unwrap();
    }

    #[test]
    fn button_monitor_recovers_from_transient_io_and_client_rejects_crlf_and_chunked() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // 1. Rejects CRLF / control chars in bearer and CSRF tokens before sending anything.
        let bad_bearer =
            LocalHttpCaptureClient::new(addr, Some("tok\r\nInjected: 1".to_owned()), 1000);
        assert!(bad_bearer.toggle_capture().is_err());

        let bad_csrf = LocalHttpCaptureClient::new(addr, Some("good-tok".to_owned()), 1000)
            .with_csrf_token("bad\ncSRF");
        assert!(bad_csrf.toggle_capture().is_err());

        // 2. Rejects Transfer-Encoding: chunked responses explicitly.
        let chunked_srv = std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().unwrap();
            let mut buf = [0u8; 1024];
            let _ = sock.read(&mut buf);
            let _ = sock.write_all(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\n\r\n",
            );
            let _ = sock.flush();
            let _ = sock.shutdown(std::net::Shutdown::Write);
            let _ = sock.read(&mut buf);
        });
        let chunked_client = LocalHttpCaptureClient::new(addr, None, 1000);
        let chunked_err = chunked_client.toggle_capture().unwrap_err();
        assert!(
            chunked_err.to_string().contains("chunked"),
            "unexpected chunked error: {chunked_err}"
        );
        chunked_srv.join().unwrap();

        // 3. ButtonMonitor::run_until recovers from transient HwError::Io in poll_event(), disarms,
        //    and dispatches subsequent ShortPress.
        struct TransientIoInput {
            polls: AtomicUsize,
            disarms: AtomicUsize,
            stop_flag: Arc<AtomicBool>,
        }
        impl ButtonInput for TransientIoInput {
            fn poll_event(&self, _timeout_ms: u64) -> HwResult<Option<ButtonEvent>> {
                match self.polls.fetch_add(1, Ordering::SeqCst) {
                    0 => Err(HwError::Io {
                        message: "transient sysfs read EIO".to_owned(),
                    }),
                    1 => Ok(Some(ButtonEvent::ShortPress)),
                    _ => {
                        self.stop_flag.store(true, Ordering::Release);
                        Ok(None)
                    }
                }
            }
            fn disarm(&self) {
                self.disarms.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct CountingToggleClient(AtomicUsize);
        impl CaptureToggleClient for CountingToggleClient {
            fn toggle_capture(&self) -> HwResult<CaptureToggleAction> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(CaptureToggleAction::Started)
            }
        }

        let stop = Arc::new(AtomicBool::new(false));
        let input = TransientIoInput {
            polls: AtomicUsize::new(0),
            disarms: AtomicUsize::new(0),
            stop_flag: Arc::clone(&stop),
        };
        let monitor = ButtonMonitor::new(input, CountingToggleClient(AtomicUsize::new(0)));
        monitor.run_until(&stop, 10).unwrap();
        assert_eq!(monitor.input.disarms.load(Ordering::SeqCst), 2);
        assert_eq!(monitor.client.0.load(Ordering::SeqCst), 1);
    }
}
