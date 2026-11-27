//! Linux sysfs LED `IndicatorController` (`/sys/class/leds/<led>/trigger` & `brightness`).
//!
//! Resets its cached indicator state BEFORE issuing any sysfs write so a partial write
//! error can never leave the LED wedged in a stale blinking state, and retries transient
//! `PermissionDenied` / `NotFound` errors on `delay_on` / `delay_off` while `udevd` applies
//! `0660` permissions after switching `trigger` to `"timer"`.

use crate::{HwError, HwResult, IndicatorController, IndicatorState};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const DEFAULT_LEDS_SYSFS_ROOT: &str = "/sys/class/leds";
pub const DEFAULT_UDEV_DELAY_RETRY_ATTEMPTS: u32 = 15;
pub const DEFAULT_UDEV_DELAY_RETRY_BACKOFF: Duration = Duration::from_millis(10);

/// Low-level LED attribute I/O abstraction (real sysfs default + deterministic fault injection).
pub trait LedIo: Send + Sync {
    fn read_attr(&self, attr: &str) -> HwResult<String>;
    fn write_attr(&self, attr: &str, value: &str) -> HwResult<()>;
}

/// Real Linux sysfs LED attribute reader/writer under `/sys/class/leds/<led>/`.
#[derive(Debug, Clone)]
pub struct SysfsLedIo {
    led_dir: PathBuf,
}

impl SysfsLedIo {
    pub fn new(led_dir: impl Into<PathBuf>) -> Self {
        Self {
            led_dir: led_dir.into(),
        }
    }

    pub fn for_led_name(led_name: &str) -> HwResult<Self> {
        if !(1..=64).contains(&led_name.len())
            || !led_name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b':' | b'-'))
        {
            return Err(HwError::Io {
                message: format!("invalid Linux LED name: {led_name}"),
            });
        }
        Ok(Self::new(Path::new(DEFAULT_LEDS_SYSFS_ROOT).join(led_name)))
    }

    pub fn led_dir(&self) -> &Path {
        &self.led_dir
    }
}

impl LedIo for SysfsLedIo {
    fn read_attr(&self, attr: &str) -> HwResult<String> {
        let path = self.led_dir.join(attr);
        fs::read_to_string(&path).map_err(|err| {
            if err.kind() == ErrorKind::NotFound {
                HwError::Unavailable {
                    message: format!("LED sysfs attribute missing: {}", path.display()),
                }
            } else {
                HwError::Io {
                    message: format!("failed to read LED attribute {}: {err}", path.display()),
                }
            }
        })
    }

    fn write_attr(&self, attr: &str, value: &str) -> HwResult<()> {
        let path = self.led_dir.join(attr);
        fs::write(&path, format!("{value}\n")).map_err(|err| match err.kind() {
            ErrorKind::NotFound => HwError::Unavailable {
                message: format!("LED sysfs attribute missing: {}", path.display()),
            },
            ErrorKind::PermissionDenied => HwError::Io {
                message: format!(
                    "permission denied writing LED attribute {}: {err}",
                    path.display()
                ),
            },
            _ => HwError::Io {
                message: format!("failed to write LED attribute {}: {err}", path.display()),
            },
        })
    }
}

/// Physical LED drive pattern mapped from [`IndicatorState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LedPattern {
    Steady { illuminated: bool },
    Blink { delay_on_ms: u32, delay_off_ms: u32 },
}

/// Map every [`IndicatorState`] variant to a distinct physical LED pattern compatible with
/// both binary (`max_brightness=1`) and PWM Linux LEDs.
pub fn pattern_for_state(state: IndicatorState) -> LedPattern {
    match state {
        IndicatorState::Idle => LedPattern::Steady { illuminated: false },
        IndicatorState::Ready => LedPattern::Blink {
            delay_on_ms: 150,
            delay_off_ms: 1850,
        },
        IndicatorState::Recording => LedPattern::Steady { illuminated: true },
        IndicatorState::Hotspot => LedPattern::Blink {
            delay_on_ms: 1000,
            delay_off_ms: 1000,
        },
        IndicatorState::Updating => LedPattern::Blink {
            delay_on_ms: 500,
            delay_off_ms: 500,
        },
        IndicatorState::Error => LedPattern::Blink {
            delay_on_ms: 125,
            delay_off_ms: 125,
        },
    }
}

struct LedInner {
    max_brightness: u32,
    previous_brightness: u32,
    previous_trigger: String,
    previous_timer_delays: Option<(String, String)>,
    inverted: bool,
    current_state: Option<IndicatorState>,
    owned: bool,
}

/// Linux sysfs status LED controller implementing [`IndicatorController`].
pub struct SysfsLedController {
    io: Arc<dyn LedIo>,
    inner: Mutex<LedInner>,
    udev_retry_attempts: u32,
    udev_retry_backoff: Duration,
}

impl SysfsLedController {
    /// Open an LED directory under `/sys/class/leds/<led>` (or a test temp directory).
    pub fn open(led_dir: impl Into<PathBuf>, inverted: bool) -> HwResult<Self> {
        Self::with_io(Arc::new(SysfsLedIo::new(led_dir)), inverted)
    }

    /// Construct with an explicit [`LedIo`] backend (useful for fault-injection tests).
    pub fn with_io(io: Arc<dyn LedIo>, inverted: bool) -> HwResult<Self> {
        let max_brightness: u32 = io
            .read_attr("max_brightness")?
            .trim()
            .parse()
            .map_err(|_| HwError::Io {
                message: "invalid LED max_brightness".to_owned(),
            })?;
        if max_brightness < 1 {
            return Err(HwError::Unavailable {
                message: "LED max_brightness must be >= 1".to_owned(),
            });
        }
        let previous_brightness: u32 =
            io.read_attr("brightness")?
                .trim()
                .parse()
                .map_err(|_| HwError::Io {
                    message: "invalid LED brightness".to_owned(),
                })?;

        let trigger_raw = io.read_attr("trigger")?;
        let mut active_triggers = Vec::new();
        let mut has_timer = false;
        for token in trigger_raw.split_whitespace() {
            if let Some(inner) = token.strip_prefix('[').and_then(|t| t.strip_suffix(']')) {
                if !inner.is_empty() {
                    active_triggers.push(inner.to_owned());
                }
                if inner == "timer" {
                    has_timer = true;
                }
            } else if token == "timer" {
                has_timer = true;
            }
        }
        if active_triggers.len() != 1 || !has_timer {
            return Err(HwError::Unavailable {
                message: "status LED requires an active trigger and kernel timer trigger support"
                    .to_owned(),
            });
        }
        let previous_trigger = active_triggers.remove(0);
        let previous_timer_delays = if previous_trigger == "timer" {
            let on = io.read_attr("delay_on")?.trim().to_owned();
            let off = io.read_attr("delay_off")?.trim().to_owned();
            Some((on, off))
        } else {
            None
        };

        Ok(Self {
            io,
            inner: Mutex::new(LedInner {
                max_brightness,
                previous_brightness,
                previous_trigger,
                previous_timer_delays,
                inverted,
                current_state: None,
                owned: false,
            }),
            udev_retry_attempts: DEFAULT_UDEV_DELAY_RETRY_ATTEMPTS,
            udev_retry_backoff: DEFAULT_UDEV_DELAY_RETRY_BACKOFF,
        })
    }

    /// Customize retry attempts and backoff when waiting for `udevd` to `chmod 0660` newly
    /// created `delay_on` / `delay_off` attributes after writing `"timer"` to `trigger`.
    pub fn with_udev_retry(mut self, attempts: u32, backoff: Duration) -> Self {
        self.udev_retry_attempts = attempts;
        self.udev_retry_backoff = backoff;
        self
    }

    fn write_timer_delay_with_retry(&self, attr: &str, value: &str) -> HwResult<()> {
        let mut attempt = 0u32;
        loop {
            match self.io.write_attr(attr, value) {
                Ok(()) => return Ok(()),
                Err(err)
                    if attempt < self.udev_retry_attempts && is_retryable_udev_delay_err(&err) =>
                {
                    attempt += 1;
                    std::thread::sleep(self.udev_retry_backoff);
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Return the currently committed cached state (`None` if unset or after a partial write error).
    pub fn cached_state(&self) -> HwResult<Option<IndicatorState>> {
        let guard = self.inner.lock().map_err(|_| HwError::Io {
            message: "LED controller mutex poisoned".to_owned(),
        })?;
        Ok(guard.current_state)
    }

    /// Restore the LED's original pre-ownership trigger, brightness, and timer delays.
    pub fn restore(&self) -> HwResult<()> {
        let mut guard = self.inner.lock().map_err(|_| HwError::Io {
            message: "LED controller mutex poisoned".to_owned(),
        })?;
        if !guard.owned {
            return Ok(());
        }
        guard.current_state = None;
        self.io.write_attr("trigger", "none")?;
        self.io
            .write_attr("brightness", &guard.previous_brightness.to_string())?;
        self.io.write_attr("trigger", &guard.previous_trigger)?;
        if let Some((on, off)) = &guard.previous_timer_delays {
            self.write_timer_delay_with_retry("delay_on", on)?;
            self.write_timer_delay_with_retry("delay_off", off)?;
        }
        guard.owned = false;
        Ok(())
    }
}

fn is_retryable_udev_delay_err(err: &HwError) -> bool {
    match err {
        HwError::Unavailable { .. } => true,
        HwError::Io { message } => {
            let lower = message.to_ascii_lowercase();
            lower.contains("permission denied") || lower.contains("eacces")
        }
        HwError::Core(_) => false,
    }
}

impl IndicatorController for SysfsLedController {
    fn set_state(&self, state: IndicatorState) -> HwResult<()> {
        let mut guard = self.inner.lock().map_err(|_| HwError::Io {
            message: "LED controller mutex poisoned".to_owned(),
        })?;
        if guard.current_state == Some(state) {
            return Ok(());
        }

        // CRITICAL: Clear cached state BEFORE any sysfs write so partial write errors
        // never leave the controller thinking the LED is already in a steady/blinking state.
        guard.current_state = None;

        self.io.write_attr("trigger", "none")?;
        guard.owned = true;

        match pattern_for_state(state) {
            LedPattern::Steady { illuminated } => {
                let brightness = if illuminated != guard.inverted {
                    guard.max_brightness
                } else {
                    0
                };
                self.io.write_attr("brightness", &brightness.to_string())?;
            }
            LedPattern::Blink {
                delay_on_ms,
                delay_off_ms,
            } => {
                // Kernel timer trigger drives `max_brightness` during `delay_on` and `0` during
                // `delay_off`. On active-low (`inverted`) LEDs, `max_brightness` is physically OFF
                // and `0` is physically ON, so swap on/off durations to preserve physical duty cycle.
                let (actual_on_ms, actual_off_ms) = if guard.inverted {
                    (delay_off_ms, delay_on_ms)
                } else {
                    (delay_on_ms, delay_off_ms)
                };
                self.io
                    .write_attr("brightness", &guard.max_brightness.to_string())?;
                self.io.write_attr("trigger", "timer")?;
                self.write_timer_delay_with_retry("delay_on", &actual_on_ms.to_string())?;
                self.write_timer_delay_with_retry("delay_off", &actual_off_ms.to_string())?;
            }
        }

        guard.current_state = Some(state);
        Ok(())
    }
}

impl Drop for SysfsLedController {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct FaultyLedIo {
        attrs: Mutex<HashMap<String, String>>,
        fail_attr: Mutex<Option<String>>,
        failed_once: AtomicBool,
        writes: Mutex<Vec<(String, String)>>,
    }

    impl FaultyLedIo {
        fn new(fail_on_attr: Option<&str>) -> Arc<Self> {
            let mut attrs = HashMap::new();
            attrs.insert("max_brightness".to_owned(), "255\n".to_owned());
            attrs.insert("brightness".to_owned(), "0\n".to_owned());
            attrs.insert("trigger".to_owned(), "[none] timer heartbeat\n".to_owned());
            attrs.insert("delay_on".to_owned(), "500\n".to_owned());
            attrs.insert("delay_off".to_owned(), "500\n".to_owned());
            Arc::new(Self {
                attrs: Mutex::new(attrs),
                fail_attr: Mutex::new(fail_on_attr.map(str::to_owned)),
                failed_once: AtomicBool::new(false),
                writes: Mutex::new(Vec::new()),
            })
        }
    }

    impl LedIo for FaultyLedIo {
        fn read_attr(&self, attr: &str) -> HwResult<String> {
            self.attrs
                .lock()
                .unwrap()
                .get(attr)
                .cloned()
                .ok_or_else(|| HwError::Unavailable {
                    message: format!("missing attr {attr}"),
                })
        }

        fn write_attr(&self, attr: &str, value: &str) -> HwResult<()> {
            if let Some(target) = self.fail_attr.lock().unwrap().as_deref() {
                if target == attr && !self.failed_once.swap(true, Ordering::SeqCst) {
                    return Err(HwError::Io {
                        message: format!("injected partial write failure on {attr}"),
                    });
                }
            }
            self.writes
                .lock()
                .unwrap()
                .push((attr.to_owned(), value.to_owned()));
            self.attrs
                .lock()
                .unwrap()
                .insert(attr.to_owned(), format!("{value}\n"));
            Ok(())
        }
    }

    #[test]
    fn partial_write_error_resets_cached_state_and_never_wedges_led() {
        let io = FaultyLedIo::new(Some("delay_on"));
        let led = SysfsLedController::with_io(io.clone(), false).unwrap();

        // First set Recording (steady ON) succeeds.
        led.set_state(IndicatorState::Recording).unwrap();
        assert_eq!(led.cached_state().unwrap(), Some(IndicatorState::Recording));

        // Transition to Error fails partway through on `delay_on` AFTER writing trigger=timer.
        assert!(led.set_state(IndicatorState::Error).is_err());
        // Cached state MUST be None so subsequent recovery call is NOT skipped!
        assert_eq!(led.cached_state().unwrap(), None);

        // Retrying Error now succeeds cleanly and sets full 125ms blink cadence.
        led.set_state(IndicatorState::Error).unwrap();
        assert_eq!(led.cached_state().unwrap(), Some(IndicatorState::Error));
        assert_eq!(io.read_attr("delay_on").unwrap().trim(), "125");
        assert_eq!(io.read_attr("delay_off").unwrap().trim(), "125");

        // Recovering back to Recording also works and turns trigger back to none.
        led.set_state(IndicatorState::Recording).unwrap();
        assert_eq!(io.read_attr("trigger").unwrap().trim(), "none");
        assert_eq!(io.read_attr("brightness").unwrap().trim(), "255");
    }

    #[test]
    fn retries_transient_udev_permission_denied_and_not_found_on_timer_delays() {
        struct UdevRaceLedIo {
            delay_on_attempts: AtomicUsize,
            delay_off_attempts: AtomicUsize,
            attrs: Mutex<HashMap<String, String>>,
        }
        impl LedIo for UdevRaceLedIo {
            fn read_attr(&self, attr: &str) -> HwResult<String> {
                self.attrs
                    .lock()
                    .unwrap()
                    .get(attr)
                    .cloned()
                    .ok_or_else(|| HwError::Unavailable {
                        message: format!("missing {attr}"),
                    })
            }
            fn write_attr(&self, attr: &str, value: &str) -> HwResult<()> {
                if attr == "delay_on" {
                    let n = self.delay_on_attempts.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        return Err(HwError::Unavailable {
                            message: "LED sysfs attribute missing: delay_on".to_owned(),
                        });
                    }
                    if n < 3 {
                        return Err(HwError::Io {
                            message: "permission denied writing LED attribute delay_on".to_owned(),
                        });
                    }
                }
                if attr == "delay_off" {
                    let n = self.delay_off_attempts.fetch_add(1, Ordering::SeqCst);
                    if n < 2 {
                        return Err(HwError::Io {
                            message: "permission denied writing LED attribute delay_off".to_owned(),
                        });
                    }
                }
                self.attrs
                    .lock()
                    .unwrap()
                    .insert(attr.to_owned(), format!("{value}\n"));
                Ok(())
            }
        }

        let mut init = HashMap::new();
        init.insert("max_brightness".to_owned(), "1\n".to_owned());
        init.insert("brightness".to_owned(), "0\n".to_owned());
        init.insert("trigger".to_owned(), "[none] timer\n".to_owned());
        let io = Arc::new(UdevRaceLedIo {
            delay_on_attempts: AtomicUsize::new(0),
            delay_off_attempts: AtomicUsize::new(0),
            attrs: Mutex::new(init),
        });
        let led = SysfsLedController::with_io(io.clone(), false)
            .unwrap()
            .with_udev_retry(10, Duration::from_millis(1));

        led.set_state(IndicatorState::Ready).unwrap();
        assert_eq!(led.cached_state().unwrap(), Some(IndicatorState::Ready));
        assert_eq!(io.delay_on_attempts.load(Ordering::SeqCst), 4);
        assert_eq!(io.delay_off_attempts.load(Ordering::SeqCst), 3);
        assert_eq!(io.read_attr("delay_on").unwrap().trim(), "150");
        assert_eq!(io.read_attr("delay_off").unwrap().trim(), "1850");
    }

    #[test]
    fn handles_all_six_indicator_states_and_restores_sysfs_files() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("miro-ego-led-test-{}-{unique}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("max_brightness"), "1\n").unwrap();
        fs::write(dir.join("brightness"), "1\n").unwrap();
        fs::write(dir.join("trigger"), "none [timer] heartbeat\n").unwrap();
        fs::write(dir.join("delay_on"), "300\n").unwrap();
        fs::write(dir.join("delay_off"), "700\n").unwrap();

        let led = SysfsLedController::open(&dir, true).unwrap();
        for state in [
            IndicatorState::Idle,
            IndicatorState::Ready,
            IndicatorState::Recording,
            IndicatorState::Hotspot,
            IndicatorState::Updating,
            IndicatorState::Error,
        ] {
            led.set_state(state).unwrap();
            assert_eq!(led.cached_state().unwrap(), Some(state));
        }

        // Inverted asymmetric blink (`Ready`: 150ms physical ON / 1850ms physical OFF) swaps
        // kernel timer delays (`delay_on=1850`, `delay_off=150`).
        led.set_state(IndicatorState::Ready).unwrap();
        assert_eq!(
            fs::read_to_string(dir.join("delay_on")).unwrap().trim(),
            "1850"
        );
        assert_eq!(
            fs::read_to_string(dir.join("delay_off")).unwrap().trim(),
            "150"
        );

        // Inverted steady Idle illuminates brightness=1; Recording sets brightness=0.
        led.set_state(IndicatorState::Idle).unwrap();
        assert_eq!(
            fs::read_to_string(dir.join("brightness")).unwrap().trim(),
            "1"
        );
        led.set_state(IndicatorState::Recording).unwrap();
        assert_eq!(
            fs::read_to_string(dir.join("brightness")).unwrap().trim(),
            "0"
        );

        led.restore().unwrap();
        assert_eq!(
            fs::read_to_string(dir.join("trigger")).unwrap().trim(),
            "timer"
        );
        assert_eq!(
            fs::read_to_string(dir.join("delay_on")).unwrap().trim(),
            "300"
        );
        assert_eq!(
            fs::read_to_string(dir.join("delay_off")).unwrap().trim(),
            "700"
        );

        // Dropping SysfsLedController after setting a state must automatically restore it.
        led.set_state(IndicatorState::Error).unwrap();
        assert_eq!(
            fs::read_to_string(dir.join("delay_on")).unwrap().trim(),
            "125"
        );
        drop(led);
        assert_eq!(
            fs::read_to_string(dir.join("trigger")).unwrap().trim(),
            "timer"
        );
        assert_eq!(
            fs::read_to_string(dir.join("delay_on")).unwrap().trim(),
            "300"
        );
        assert_eq!(
            fs::read_to_string(dir.join("delay_off")).unwrap().trim(),
            "700"
        );

        fs::remove_dir_all(dir).unwrap();
    }
}
