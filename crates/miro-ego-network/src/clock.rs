//! Privileged OS wall-clock inspection (`adjtimex`, `/proc/sys/kernel/random/boot_id`) and
//! boot-bound calibration (`clock_settime(CLOCK_REALTIME)`, `<run_dir>/clock-state.json`).

use crate::{NetworkError, NetworkResult};
use miro_ego_core::sys::fs::{read_bounded_json, write_bytes_atomic};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};

pub const CLOCK_STATE_SCHEMA: &str = "miro-ego.clock-state.v1";
pub const CLOCK_STATE_FILENAME: &str = "clock-state.json";
pub const DEFAULT_BOOT_ID_PATH: &str = "/proc/sys/kernel/random/boot_id";
pub const MAX_CLOCK_STATE_BYTES: usize = 8 * 1024;
/// Minimum accepted wall-clock timestamp (`2024-01-01T00:00:00Z` in ms).
pub const CLOCK_MIN_MS: u64 = 1_704_067_200_000;
/// Exclusive maximum accepted wall-clock timestamp (`2100-01-01T00:00:00Z` in ms).
pub const CLOCK_MAX_MS: u64 = 4_102_444_800_000;

/// Privileged clock status snapshot returned by `clock.status` and `clock.sync`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClockStatusSnapshot {
    pub source: String, // "ntp" | "client" | "unsynchronized"
    pub unix_time_ms: u64,
    pub applied: bool,
    pub boot_id: String,
    pub synchronized_at_ms: Option<u64>,
    pub age_seconds: Option<u64>,
}

/// Boot-bound client calibration state persisted under `<run_dir>/clock-state.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistedClockState {
    pub schema: String,
    pub boot_id: String,
    pub synchronized_at_ms: u64,
    pub synchronized_mono_ns: u64,
}

/// Trait abstracting OS kernel clock inspection and `clock_settime(CLOCK_REALTIME)` for
/// production Linux execution (`CAP_SYS_TIME`) and deterministic unit tests.
pub trait SystemClockBackend: Send + Sync {
    /// Read current OS boot identifier (trimmed `/proc/sys/kernel/random/boot_id` on Linux).
    fn boot_id(&self) -> NetworkResult<String>;

    /// Query kernel NTP discipline status (`adjtimex` `(tx.status & STA_UNSYNC) == 0`).
    fn is_ntp_synchronized(&self) -> NetworkResult<bool>;

    /// Read current wall-clock Unix epoch milliseconds (`CLOCK_REALTIME`).
    fn realtime_unix_ms(&self) -> NetworkResult<u64>;

    /// Read current monotonic clock nanoseconds (`CLOCK_MONOTONIC`).
    fn monotonic_ns(&self) -> NetworkResult<u64>;

    /// Set OS wall clock to `unix_time_ms` (`clock_settime(CLOCK_REALTIME, &timespec)`).
    fn set_realtime_unix_ms(&self, unix_time_ms: u64) -> NetworkResult<()>;
}

/// Real Linux `SystemClockBackend` backed by `/proc/sys/kernel/random/boot_id`, `libc::adjtimex`,
/// `libc::clock_gettime`, and `libc::clock_settime(CLOCK_REALTIME)`.
#[derive(Debug)]
pub struct LinuxSystemClockBackend {
    boot_id_path: PathBuf,
}

impl Default for LinuxSystemClockBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl LinuxSystemClockBackend {
    pub fn new() -> Self {
        Self {
            boot_id_path: PathBuf::from(DEFAULT_BOOT_ID_PATH),
        }
    }

    pub fn with_boot_id_path(path: impl Into<PathBuf>) -> Self {
        Self {
            boot_id_path: path.into(),
        }
    }
}

impl SystemClockBackend for LinuxSystemClockBackend {
    fn boot_id(&self) -> NetworkResult<String> {
        let raw = fs::read_to_string(&self.boot_id_path).map_err(|err| NetworkError::Backend {
            message: format!(
                "failed to read boot_id from {}: {err}",
                self.boot_id_path.display()
            ),
        })?;
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.len() > 64 {
            return Err(NetworkError::Backend {
                message: format!(
                    "invalid boot_id contents in {}",
                    self.boot_id_path.display()
                ),
            });
        }
        Ok(trimmed.to_ascii_lowercase())
    }

    fn is_ntp_synchronized(&self) -> NetworkResult<bool> {
        // SAFETY: `libc::timex` is POD and zero-initializing `modes = 0` performs a read-only
        // kernel NTP state query without modifying clock parameters.
        let mut tx: libc::timex = unsafe { std::mem::zeroed() };
        tx.modes = 0;
        let rc = unsafe { libc::adjtimex(&mut tx) };
        if rc < 0 {
            return Err(NetworkError::Backend {
                message: format!("adjtimex failed: {}", std::io::Error::last_os_error()),
            });
        }
        Ok(rc != libc::TIME_ERROR && (tx.status & libc::STA_UNSYNC) == 0)
    }

    fn realtime_unix_ms(&self) -> NetworkResult<u64> {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `ts` points to valid writable `timespec` storage.
        if unsafe { libc::clock_gettime(libc::CLOCK_REALTIME, &mut ts) } != 0 {
            return Err(NetworkError::Backend {
                message: format!(
                    "clock_gettime(CLOCK_REALTIME) failed: {}",
                    std::io::Error::last_os_error()
                ),
            });
        }
        if ts.tv_sec <= 0 {
            return Ok(0);
        }
        let secs_ms = (ts.tv_sec as u64).saturating_mul(1000);
        let sub_ms = (ts.tv_nsec.max(0) as u64) / 1_000_000;
        Ok(secs_ms.saturating_add(sub_ms))
    }

    fn monotonic_ns(&self) -> NetworkResult<u64> {
        miro_ego_core::sys::clock::monotonic_ns().map_err(|err| NetworkError::Backend {
            message: format!("clock_gettime(CLOCK_MONOTONIC) failed: {err}"),
        })
    }

    fn set_realtime_unix_ms(&self, unix_time_ms: u64) -> NetworkResult<()> {
        let secs_u64 = unix_time_ms / 1000;
        let rem_ms = unix_time_ms % 1000;
        let tv_sec = libc::time_t::try_from(secs_u64).map_err(|_| NetworkError::Invalid {
            message: format!("unix_time_ms out of range: {unix_time_ms}"),
        })?;
        let tv_nsec = (rem_ms * 1_000_000) as libc::c_long;
        let ts = libc::timespec { tv_sec, tv_nsec };
        // SAFETY: `ts` is a valid initialized `timespec` passed to `clock_settime`.
        if unsafe { libc::clock_settime(libc::CLOCK_REALTIME, &ts) } != 0 {
            return Err(NetworkError::Backend {
                message: format!(
                    "clock_settime(CLOCK_REALTIME) failed: {}",
                    std::io::Error::last_os_error()
                ),
            });
        }
        Ok(())
    }
}

/// Deterministic in-memory `SystemClockBackend` for unit and integration tests.
pub struct MockSystemClockBackend {
    boot_id: Mutex<String>,
    ntp_synced: AtomicBool,
    realtime_ms: AtomicU64,
    mono_ns: AtomicU64,
    settime_calls: AtomicU64,
    fail_settime: AtomicBool,
}

impl MockSystemClockBackend {
    pub fn new(boot_id: impl Into<String>, realtime_ms: u64, mono_ns: u64) -> Self {
        Self {
            boot_id: Mutex::new(boot_id.into()),
            ntp_synced: AtomicBool::new(false),
            realtime_ms: AtomicU64::new(realtime_ms),
            mono_ns: AtomicU64::new(mono_ns),
            settime_calls: AtomicU64::new(0),
            fail_settime: AtomicBool::new(false),
        }
    }

    pub fn set_ntp_synchronized(&self, synced: bool) {
        self.ntp_synced.store(synced, Ordering::SeqCst);
    }

    pub fn set_boot_id(&self, boot_id: impl Into<String>) {
        *self.boot_id.lock().unwrap_or_else(PoisonError::into_inner) = boot_id.into();
    }

    pub fn advance_seconds(&self, secs: u64) {
        self.realtime_ms
            .fetch_add(secs.saturating_mul(1000), Ordering::SeqCst);
        self.mono_ns
            .fetch_add(secs.saturating_mul(1_000_000_000), Ordering::SeqCst);
    }

    pub fn set_fail_settime(&self, fail: bool) {
        self.fail_settime.store(fail, Ordering::SeqCst);
    }

    pub fn settime_calls(&self) -> u64 {
        self.settime_calls.load(Ordering::SeqCst)
    }
}

impl SystemClockBackend for MockSystemClockBackend {
    fn boot_id(&self) -> NetworkResult<String> {
        Ok(self
            .boot_id
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone())
    }

    fn is_ntp_synchronized(&self) -> NetworkResult<bool> {
        Ok(self.ntp_synced.load(Ordering::SeqCst))
    }

    fn realtime_unix_ms(&self) -> NetworkResult<u64> {
        Ok(self.realtime_ms.load(Ordering::SeqCst))
    }

    fn monotonic_ns(&self) -> NetworkResult<u64> {
        Ok(self.mono_ns.load(Ordering::SeqCst))
    }

    fn set_realtime_unix_ms(&self, unix_time_ms: u64) -> NetworkResult<()> {
        if self.fail_settime.load(Ordering::SeqCst) {
            return Err(NetworkError::Backend {
                message: "simulated EPERM on clock_settime".to_owned(),
            });
        }
        self.settime_calls.fetch_add(1, Ordering::SeqCst);
        self.realtime_ms.store(unix_time_ms, Ordering::SeqCst);
        Ok(())
    }
}

/// Evaluate `clock_status` against `backend` and `<run_dir>/clock-state.json` without mutating
/// system time.
pub fn evaluate_clock_status(
    backend: &dyn SystemClockBackend,
    run_dir: &Path,
) -> NetworkResult<ClockStatusSnapshot> {
    let boot_id = backend.boot_id()?;
    let ntp_synced = backend.is_ntp_synchronized()?;
    let unix_time_ms = backend.realtime_unix_ms()?;
    let mono_now = backend.monotonic_ns()?;
    let persisted = load_boot_clock_state(run_dir, &boot_id);

    if ntp_synced {
        return Ok(ClockStatusSnapshot {
            source: "ntp".to_owned(),
            unix_time_ms,
            applied: false,
            boot_id,
            synchronized_at_ms: persisted.as_ref().map(|p| p.synchronized_at_ms),
            age_seconds: persisted
                .as_ref()
                .map(|p| mono_now.saturating_sub(p.synchronized_mono_ns) / 1_000_000_000),
        });
    }

    if let Some(st) = persisted {
        let age = mono_now.saturating_sub(st.synchronized_mono_ns) / 1_000_000_000;
        return Ok(ClockStatusSnapshot {
            source: "client".to_owned(),
            unix_time_ms,
            applied: true,
            boot_id,
            synchronized_at_ms: Some(st.synchronized_at_ms),
            age_seconds: Some(age),
        });
    }

    Ok(ClockStatusSnapshot {
        source: "unsynchronized".to_owned(),
        unix_time_ms,
        applied: false,
        boot_id,
        synchronized_at_ms: None,
        age_seconds: None,
    })
}

/// Execute `clock_sync(unix_time_ms, replace)` against `backend`, persisting per-boot calibration
/// state atomically to `<run_dir>/clock-state.json`.
pub fn execute_clock_sync(
    backend: &dyn SystemClockBackend,
    run_dir: &Path,
    unix_time_ms: u64,
    replace: bool,
) -> NetworkResult<ClockStatusSnapshot> {
    if !(CLOCK_MIN_MS..CLOCK_MAX_MS).contains(&unix_time_ms) {
        return Err(NetworkError::Invalid {
            message: format!(
                "unix_time_ms {unix_time_ms} is outside valid range [{CLOCK_MIN_MS}, {CLOCK_MAX_MS})"
            ),
        });
    }
    let boot_id = backend.boot_id()?;
    let ntp_synced = backend.is_ntp_synchronized()?;

    if ntp_synced {
        let now_ms = backend.realtime_unix_ms()?;
        let mono_now = backend.monotonic_ns()?;
        let persisted = load_boot_clock_state(run_dir, &boot_id);
        return Ok(ClockStatusSnapshot {
            source: "ntp".to_owned(),
            unix_time_ms: now_ms,
            applied: false,
            boot_id,
            synchronized_at_ms: persisted.as_ref().map(|p| p.synchronized_at_ms),
            age_seconds: persisted
                .as_ref()
                .map(|p| mono_now.saturating_sub(p.synchronized_mono_ns) / 1_000_000_000),
        });
    }

    let persisted = load_boot_clock_state(run_dir, &boot_id);
    if let Some(existing) = persisted {
        if !replace {
            let now_ms = backend.realtime_unix_ms()?;
            let mono_now = backend.monotonic_ns()?;
            let age = mono_now.saturating_sub(existing.synchronized_mono_ns) / 1_000_000_000;
            return Ok(ClockStatusSnapshot {
                source: "client".to_owned(),
                unix_time_ms: now_ms,
                applied: false,
                boot_id,
                synchronized_at_ms: Some(existing.synchronized_at_ms),
                age_seconds: Some(age),
            });
        }
    }

    backend.set_realtime_unix_ms(unix_time_ms)?;
    let synced_mono_ns = backend.monotonic_ns()?;
    let observed_unix_ms = backend.realtime_unix_ms()?;

    let record = PersistedClockState {
        schema: CLOCK_STATE_SCHEMA.to_owned(),
        boot_id: boot_id.clone(),
        synchronized_at_ms: unix_time_ms,
        synchronized_mono_ns: synced_mono_ns,
    };
    persist_boot_clock_state(run_dir, &record)?;

    Ok(ClockStatusSnapshot {
        source: "client".to_owned(),
        unix_time_ms: observed_unix_ms,
        applied: true,
        boot_id,
        synchronized_at_ms: Some(unix_time_ms),
        age_seconds: Some(0),
    })
}

fn load_boot_clock_state(run_dir: &Path, expected_boot_id: &str) -> Option<PersistedClockState> {
    let path = run_dir.join(CLOCK_STATE_FILENAME);
    let loaded: PersistedClockState = read_bounded_json(&path, MAX_CLOCK_STATE_BYTES, 8).ok()?;
    if loaded.schema != CLOCK_STATE_SCHEMA || loaded.boot_id != expected_boot_id {
        return None;
    }
    Some(loaded)
}

fn persist_boot_clock_state(run_dir: &Path, state: &PersistedClockState) -> NetworkResult<()> {
    fs::create_dir_all(run_dir).map_err(|err| NetworkError::Backend {
        message: format!(
            "failed to create clock state dir {}: {err}",
            run_dir.display()
        ),
    })?;
    let mut raw = serde_json::to_vec_pretty(state).map_err(|err| NetworkError::Backend {
        message: format!("failed to serialize clock-state.json: {err}"),
    })?;
    raw.push(b'\n');
    let path = run_dir.join(CLOCK_STATE_FILENAME);
    write_bytes_atomic(&path, &raw, 0o640, true).map_err(|err| NetworkError::Backend {
        message: format!("failed to persist {}: {err}", path.display()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use miro_ego_core::ids::new_uuid_v4;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_run_dir(tag: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "miro-ego-clock-{tag}-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn clock_sync_persists_per_boot_computes_real_age_and_respects_ntp_and_replace() {
        let run_dir = temp_run_dir("boot-sync");
        let boot_1 = new_uuid_v4();
        let boot_2 = new_uuid_v4();
        let mock = MockSystemClockBackend::new(&boot_1, 5_000, 10_000_000_000);

        // Initially unsynchronized.
        let init = evaluate_clock_status(&mock, &run_dir).unwrap();
        assert_eq!(init.source, "unsynchronized");
        assert!(!init.applied);
        assert_eq!(init.boot_id, boot_1);
        assert_eq!(init.synchronized_at_ms, None);
        assert_eq!(init.age_seconds, None);

        // When NTP is synchronized, even replace=true does NOT call clock_settime and returns source="ntp", applied=false.
        mock.set_ntp_synchronized(true);
        let ntp_skip = execute_clock_sync(&mock, &run_dir, 1_760_000_000_000, false).unwrap();
        assert_eq!(ntp_skip.source, "ntp");
        assert!(!ntp_skip.applied);
        let ntp_replace = execute_clock_sync(&mock, &run_dir, 1_760_000_000_000, true).unwrap();
        assert_eq!(ntp_replace.source, "ntp");
        assert!(!ntp_replace.applied);
        assert_eq!(mock.settime_calls(), 0);

        // Out-of-range timestamps must be rejected.
        assert!(matches!(
            execute_clock_sync(&mock, &run_dir, CLOCK_MIN_MS - 1, false),
            Err(NetworkError::Invalid { .. })
        ));
        assert!(matches!(
            execute_clock_sync(&mock, &run_dir, CLOCK_MAX_MS, false),
            Err(NetworkError::Invalid { .. })
        ));

        // Turn off NTP -> client sync applies and persists clock-state.json!
        mock.set_ntp_synchronized(false);
        let t1 = 1_760_000_123_456;
        let synced = execute_clock_sync(&mock, &run_dir, t1, false).unwrap();
        assert_eq!(synced.source, "client");
        assert!(synced.applied);
        assert_eq!(synced.unix_time_ms, t1);
        assert_eq!(synced.synchronized_at_ms, Some(t1));
        assert_eq!(synced.age_seconds, Some(0));
        assert_eq!(mock.settime_calls(), 1);
        assert!(run_dir.join(CLOCK_STATE_FILENAME).is_file());

        // Advance 42s monotonic: second sync without replace returns applied=false and age_seconds=42 (NOT 0!).
        mock.advance_seconds(42);
        let no_replace = execute_clock_sync(&mock, &run_dir, t1 + 999_000, false).unwrap();
        assert_eq!(no_replace.source, "client");
        assert!(!no_replace.applied);
        assert_eq!(no_replace.synchronized_at_ms, Some(t1));
        assert_eq!(no_replace.age_seconds, Some(42));
        assert_eq!(mock.settime_calls(), 1);

        // Explicit replace=true updates clock and resets age_seconds=0.
        let t2 = 1_760_000_999_000;
        let replaced = execute_clock_sync(&mock, &run_dir, t2, true).unwrap();
        assert!(replaced.applied);
        assert_eq!(replaced.synchronized_at_ms, Some(t2));
        assert_eq!(replaced.age_seconds, Some(0));
        assert_eq!(mock.settime_calls(), 2);

        // Simulated reboot (new boot_id) invalidates previous boot's clock-state.json!
        mock.set_boot_id(&boot_2);
        let after_reboot = evaluate_clock_status(&mock, &run_dir).unwrap();
        assert_eq!(after_reboot.source, "unsynchronized");
        assert!(!after_reboot.applied);
        assert_eq!(after_reboot.boot_id, boot_2);
        assert_eq!(after_reboot.synchronized_at_ms, None);
        assert_eq!(after_reboot.age_seconds, None);

        // Real Linux boot_id + adjtimex smoke check (read-only).
        let linux_backend = LinuxSystemClockBackend::new();
        assert!(!linux_backend.boot_id().unwrap().is_empty());
        let _ = linux_backend.is_ntp_synchronized().unwrap();
        assert!(linux_backend.monotonic_ns().unwrap() > 0);

        // Unreadable boot_id path returns NetworkError::Backend instead of fabricating a UUID.
        let bad_boot_backend =
            LinuxSystemClockBackend::with_boot_id_path(run_dir.join("missing-boot-id"));
        assert!(matches!(
            bad_boot_backend.boot_id(),
            Err(NetworkError::Backend { .. })
        ));

        fs::remove_dir_all(run_dir).unwrap();
    }
}
