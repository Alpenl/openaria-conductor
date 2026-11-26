//! 40-pin GPIO recording button monitor, RDK X5 BOARD-to-SoC sysfs GPIO export,
//! debounce/short/double/long press classifier, `/etc/miro-ego/button.json` schema loader,
//! and bounded HTTP/HTTPS local capture toggle client with pinned TLS certificate verification.

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
use std::fs;
use std::io::{Cursor, ErrorKind, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use subtle::ConstantTimeEq;

pub const BUTTON_CONFIG_SCHEMA: &str = "miro-ego.button-config.v1";
pub const DEFAULT_BUTTON_CONFIG_PATH: &str = "/etc/miro-ego/button.json";
pub const DEFAULT_GPIO_SYSFS_ROOT: &str = "/sys/class/gpio";
pub const MAX_BUTTON_CONFIG_BYTES: usize = 8 * 1024;
pub const MAX_HTTP_RESPONSE_BYTES: usize = 1024 * 1024;

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
        if !(100..=10_000).contains(&self.request_timeout_ms) {
            return Err(HwError::Io {
                message: "request_timeout_ms must be in 100..=10000".to_owned(),
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
                    if let Some(deadline) = self.pending_short_deadline_ms {
                        if self.candidate_since_ms <= deadline || now_ms <= deadline {
                            self.second_press_in_window = true;
                        }
                    }
                } else if !self.long_press_fired {
                    if let Some(started) = self.press_started_ms {
                        if now_ms.saturating_sub(started) >= self.long_press_ms {
                            self.long_press_fired = true;
                            self.pending_short_deadline_ms = None;
                            self.second_press_in_window = false;
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

/// Bounded HTTP/1.1 & HTTPS loopback client calling `GET /api/v1/capture/status` and
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
        let capped = timeout_ms.clamp(100, 10_000);
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
                Self::new(addr, Some(token), timeout_ms)
                    .with_pinned_tls_cert_file(&tls_certificate_file)
            }
        }
    }

    fn http_json(&self, method: &str, path: &str, body: Option<&Value>) -> HwResult<(u16, Value)> {
        let tcp = TcpStream::connect_timeout(&self.addr, self.timeout).map_err(|err| {
            HwError::Unavailable {
                message: format!("capture API {} unreachable: {err}", self.addr),
            }
        })?;
        tcp.set_read_timeout(Some(self.timeout))
            .and_then(|()| tcp.set_write_timeout(Some(self.timeout)))
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
            "{method} /api/v1{path} HTTP/1.1\r\nHost: {authority}\r\nAccept: application/json\r\nOrigin: {origin}\r\nConnection: close\r\n"
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
                        tls_stream.write_all(bytes)?;
                    }
                    tls_stream.flush()
                })
                .map_err(|err| HwError::Io {
                    message: format!("capture HTTPS API write/handshake failed: {err}"),
                })?;
            read_http_response_bounded(&mut tls_stream, true)?
        } else {
            let mut plain = tcp;
            plain
                .write_all(req.as_bytes())
                .and_then(|()| {
                    if let Some(bytes) = &payload {
                        plain.write_all(bytes)?;
                    }
                    plain.flush()
                })
                .map_err(|err| HwError::Io {
                    message: format!("capture API write failed: {err}"),
                })?;
            read_http_response_bounded(&mut plain, false)?
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

fn read_http_response_bounded<R: Read>(
    reader: &mut R,
    tolerate_tls_eof: bool,
) -> HwResult<Vec<u8>> {
    let mut raw = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match reader.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                raw.extend_from_slice(&chunk[..n]);
                if raw.len() > MAX_HTTP_RESPONSE_BYTES {
                    return Err(HwError::Io {
                        message: "capture API response exceeded 1 MiB limit".to_owned(),
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

    /// Run continuously until `stop` becomes `true`, logging transient capture API errors
    /// without terminating the button monitor loop.
    pub fn run_until(&self, stop: &AtomicBool, poll_slice_ms: u64) -> HwResult<()> {
        while !stop.load(Ordering::Acquire) {
            let Some(event) = self.input.poll_event(poll_slice_ms)? else {
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
    fn loads_deploy_button_json_and_validates_pin_allowlist_and_window() {
        let deploy_cfg = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/miro-ego/miro-ego-button.json");
        let cfg = ButtonConfig::load(&deploy_cfg).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.physical_pin, 37);
        assert_eq!(cfg.debounce_ms, 80);
        assert_eq!(cfg.cooldown_ms, 1000);
        assert_eq!(cfg.poll_ms, 20);
        assert_eq!(cfg.status_led.as_deref(), Some("ACT"));

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
        assert!(rdk_x5_board_pin_to_soc_gpio(1).is_err());
        assert!(rdk_x5_board_pin_to_soc_gpio(6).is_err());
        assert!(rdk_x5_board_pin_to_soc_gpio(39).is_err());

        let mut seen_soc = HashSet::new();
        for &board_pin in RDK_X5_GPIO_PINS {
            let soc = rdk_x5_board_pin_to_soc_gpio(board_pin).unwrap();
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
    fn local_http_capture_client_toggles_idle_and_recording_cleanly() {
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

        let client = LocalHttpCaptureClient::new(addr, Some("test-bearer-token".to_owned()), 9000)
            .with_csrf_token("test-csrf-token");
        assert_eq!(client.timeout, Duration::from_millis(9000));
        assert_eq!(
            client.toggle_capture().unwrap(),
            CaptureToggleAction::Started
        );
        assert_eq!(
            client.toggle_capture().unwrap(),
            CaptureToggleAction::Stopped
        );
        srv.join().unwrap();
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
}
