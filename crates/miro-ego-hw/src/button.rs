//! 40-pin GPIO recording button monitor, debounce/short/double/long press classifier,
//! `/etc/miro-ego/button.json` schema loader, and bounded local capture toggle client.

use crate::{ButtonEvent, ButtonInput, HwError, HwResult};
use miro_ego_core::ids::new_uuid_v4;
use miro_ego_core::sys::fs::read_bounded_json;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub const BUTTON_CONFIG_SCHEMA: &str = "miro-ego.button-config.v1";
pub const DEFAULT_BUTTON_CONFIG_PATH: &str = "/etc/miro-ego/button.json";
pub const MAX_BUTTON_CONFIG_BYTES: usize = 8 * 1024;
pub const MAX_HTTP_RESPONSE_BYTES: usize = 1024 * 1024;

/// Valid 40-pin RDK X5 BOARD GPIO physical pins (excluding 3V3, 5V, and GND pins).
pub const RDK_X5_GPIO_PINS: &[u8] = &[
    3, 5, 7, 8, 10, 11, 12, 13, 15, 16, 18, 19, 21, 22, 23, 24, 26, 27, 28, 29, 31, 32, 33, 35, 36,
    37, 38, 40,
];

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
            value_path: PathBuf::from(format!("/sys/class/gpio/gpio{soc_gpio}/value")),
        }
    }

    pub fn from_value_path(path: impl Into<PathBuf>) -> Self {
        Self {
            value_path: path.into(),
        }
    }

    pub fn value_path(&self) -> &Path {
        &self.value_path
    }
}

impl GpioPin for SysfsGpioPin {
    fn read_level(&self) -> HwResult<bool> {
        let raw = fs::read_to_string(&self.value_path).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
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
    }

    /// Feed a sampled logical `pressed` state at monotonic timestamp `now_ms`.
    pub fn update(&mut self, pressed: bool, now_ms: u64) -> Option<ButtonEvent> {
        // Check if a pending single click window has elapsed without a second press.
        if let Some(deadline) = self.pending_short_deadline_ms {
            if !self.stable_pressed && now_ms >= deadline {
                self.pending_short_deadline_ms = None;
                return self.emit_if_cooled_down(ButtonEvent::ShortPress, now_ms);
            }
        }

        // Track raw debounce candidate.
        if self.candidate_pressed != Some(pressed) {
            self.candidate_pressed = Some(pressed);
            self.candidate_since_ms = now_ms;
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
                } else if self.stable_pressed {
                    self.stable_pressed = false;
                    let fired_long = self.long_press_fired;
                    self.press_started_ms = None;
                    self.long_press_fired = false;

                    if !fired_long {
                        if let Some(deadline) = self.pending_short_deadline_ms {
                            if now_ms <= deadline {
                                self.pending_short_deadline_ms = None;
                                return self.emit_if_cooled_down(ButtonEvent::DoublePress, now_ms);
                            }
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
                } else if !self.long_press_fired {
                    if let Some(started) = self.press_started_ms {
                        if now_ms.saturating_sub(started) >= self.long_press_ms {
                            self.long_press_fired = true;
                            self.pending_short_deadline_ms = None;
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

/// Bounded HTTP/1.1 loopback client calling `GET /api/v1/capture/status` and
/// `POST /api/v1/capture/{start,stop}` on the local `miro-ego serve` daemon.
#[derive(Debug, Clone)]
pub struct LocalHttpCaptureClient {
    addr: SocketAddr,
    bearer_token: Option<String>,
    timeout: Duration,
}

impl LocalHttpCaptureClient {
    pub fn new(addr: SocketAddr, bearer_token: Option<String>, timeout_ms: u64) -> Self {
        let capped = timeout_ms.clamp(100, 6000);
        Self {
            addr,
            bearer_token,
            timeout: Duration::from_millis(capped),
        }
    }

    fn http_json(&self, method: &str, path: &str, body: Option<&Value>) -> HwResult<(u16, Value)> {
        let mut stream = TcpStream::connect_timeout(&self.addr, self.timeout).map_err(|err| {
            HwError::Unavailable {
                message: format!("capture API {} unreachable: {err}", self.addr),
            }
        })?;
        stream
            .set_read_timeout(Some(self.timeout))
            .and_then(|()| stream.set_write_timeout(Some(self.timeout)))
            .map_err(|err| HwError::Io {
                message: format!("failed to configure capture API timeout: {err}"),
            })?;

        let origin = format!("http://{}", self.addr);
        let payload = match body {
            Some(v) => Some(serde_json::to_vec(v).map_err(|err| HwError::Io {
                message: format!("failed to serialize request body: {err}"),
            })?),
            None => None,
        };

        let mut req = format!(
            "{method} /api/v1{path} HTTP/1.1\r\nHost: {}\r\nAccept: application/json\r\nOrigin: {origin}\r\nConnection: close\r\n",
            self.addr
        );
        if let Some(tok) = &self.bearer_token {
            req.push_str(&format!("Authorization: Bearer {tok}\r\n"));
        }
        if let Some(bytes) = &payload {
            req.push_str("Content-Type: application/json\r\n");
            req.push_str(&format!("Idempotency-Key: {}\r\n", new_uuid_v4()));
            if let Some(tok) = &self.bearer_token {
                req.push_str(&format!("X-CSRF-Token: {tok}\r\n"));
            }
            req.push_str(&format!("Content-Length: {}\r\n", bytes.len()));
        }
        req.push_str("\r\n");

        stream
            .write_all(req.as_bytes())
            .and_then(|()| {
                if let Some(bytes) = &payload {
                    stream.write_all(bytes)?;
                }
                stream.flush()
            })
            .map_err(|err| HwError::Io {
                message: format!("capture API write failed: {err}"),
            })?;

        let mut raw = Vec::new();
        let mut limited = stream.take((MAX_HTTP_RESPONSE_BYTES + 1) as u64);
        limited.read_to_end(&mut raw).map_err(|err| HwError::Io {
            message: format!("capture API read failed: {err}"),
        })?;
        if raw.len() > MAX_HTTP_RESPONSE_BYTES {
            return Err(HwError::Io {
                message: "capture API response exceeded 1 MiB limit".to_owned(),
            });
        }

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

    /// Poll once for up to `timeout_ms` and dispatch any recognized event.
    pub fn step(
        &self,
        timeout_ms: u64,
    ) -> HwResult<Option<(ButtonEvent, Option<CaptureToggleAction>)>> {
        let Some(event) = self.input.poll_event(timeout_ms)? else {
            return Ok(None);
        };
        let action = match event {
            ButtonEvent::ShortPress => Some(self.client.toggle_capture()?),
            ButtonEvent::DoublePress | ButtonEvent::LongPress => None,
        };
        Ok(Some((event, action)))
    }

    /// Run continuously until `stop` becomes `true`.
    pub fn run_until(&self, stop: &AtomicBool, poll_slice_ms: u64) -> HwResult<()> {
        while !stop.load(Ordering::Acquire) {
            let _ = self.step(poll_slice_ms)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn loads_deploy_button_json_and_validates_pin_allowlist() {
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

        // Wait past 500ms cooldown (last_event = 3650 -> ready at 4150).
        // Double press: two short clicks within 350ms.
        assert_eq!(clf.update(true, 4200), None);
        assert_eq!(clf.update(true, 4290), None);
        assert_eq!(clf.update(false, 4350), None);
        assert_eq!(clf.update(false, 4440), None); // 1st click pending until 4790
        assert_eq!(clf.update(true, 4500), None);
        assert_eq!(clf.update(true, 4590), None);
        assert_eq!(clf.update(false, 4620), None);
        assert_eq!(clf.update(false, 4710), Some(ButtonEvent::DoublePress));

        // Long press: held >= 1200ms after stable press.
        assert_eq!(clf.update(true, 6000), None);
        assert_eq!(clf.update(true, 6090), None); // stable press starts at 6090
        assert_eq!(clf.update(true, 7000), None);
        assert_eq!(clf.update(true, 7290), Some(ButtonEvent::LongPress));
        // Still holding does not double-fire.
        assert_eq!(clf.update(true, 8500), None);
        // Releasing after long press does not emit a spurious short press.
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
                        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}".to_owned()
                    }
                };
                sock.write_all(resp.as_bytes()).unwrap();
            }
        });

        let client = LocalHttpCaptureClient::new(addr, Some("test-bearer-token".to_owned()), 2000);
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
}
