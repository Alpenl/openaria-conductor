//! Boot-scoped clock synchronization state (`clock-state.json` under `<run_dir>`),
//! single-use principal-bound `/clock` & `/clock/check` challenges, kernel NTP status probing,
//! and [`ClockSyncProbe`] integration for `CaptureCoordinator`.

use super::error_map::map_clock_rpc_error;
use miro_ego_api::{ApiError, ApiResult};
use miro_ego_core::ids::new_uuid_v4;
use miro_ego_core::sys::clock::monotonic_ns;
use miro_ego_core::sys::fs::{read_bounded_json, write_bytes_atomic};
use miro_ego_network::NetworkRpcClient;
use miro_ego_storage::{ClockSyncProbe, ClockSyncSnapshot, ClockSyncSource};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAX_CLOCK_CHALLENGES: usize = 64;
const CLOCK_STATE_SCHEMA: &str = "miro-ego.clock-runtime-state.v1";
const CLOCK_STATE_MAX_BYTES: usize = 4096;

pub(super) fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub(super) fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn is_canonical_uuid(value: &str) -> bool {
    if value.len() != 36 {
        return false;
    }
    value.bytes().enumerate().all(|(idx, b)| match idx {
        8 | 13 | 18 | 23 => b == b'-',
        _ => b.is_ascii_hexdigit() && !b.is_ascii_uppercase(),
    })
}

pub(super) fn read_linux_boot_id() -> String {
    if let Ok(raw) = fs::read_to_string("/proc/sys/kernel/random/boot_id") {
        let candidate = raw.trim().to_ascii_lowercase();
        if is_canonical_uuid(&candidate) {
            return candidate;
        }
    }
    new_uuid_v4()
}

/// Inspect the Linux kernel discipline status via `adjtimex(2)` (`MOD_OFFSET == 0`, read-only)
/// to check whether `STA_UNSYNC` has been cleared by NTP (`systemd-timesyncd` / `chronyd`).
fn is_kernel_ntp_synchronized() -> bool {
    // SAFETY: `libc::timex` is a POD C struct; zero-initializing `modes = 0` performs a pure
    // read-only kernel clock status query without mutating kernel time state.
    let mut tx: libc::timex = unsafe { std::mem::zeroed() };
    tx.modes = 0;
    let rc = unsafe { libc::adjtimex(&mut tx) };
    rc >= 0 && rc != libc::TIME_ERROR && (tx.status & libc::STA_UNSYNC) == 0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedClockState {
    schema: String,
    boot_id: String,
    synced: bool,
    offset_ms: i64,
    synchronized_at_ms: Option<u64>,
    synchronized_mono_ns: Option<u64>,
}

struct ClockChallenge {
    principal_id: String,
    from_check: bool,
    expires_at: Instant,
}

struct ClockRuntimeInner {
    boot_id: String,
    synced: bool,
    offset_ms: i64,
    synchronized_at_ms: Option<u64>,
    synchronized_mono_ns: Option<u64>,
    challenges: HashMap<String, ClockChallenge>,
}

impl ClockRuntimeInner {
    fn load(boot_id: String, state_path: Option<&Path>) -> Self {
        if let Some(path) = state_path {
            if let Ok(persisted) =
                read_bounded_json::<PersistedClockState>(path, CLOCK_STATE_MAX_BYTES, 8)
            {
                if persisted.schema == CLOCK_STATE_SCHEMA && persisted.boot_id == boot_id {
                    return Self {
                        boot_id,
                        synced: persisted.synced,
                        offset_ms: persisted.offset_ms,
                        synchronized_at_ms: persisted.synchronized_at_ms,
                        synchronized_mono_ns: persisted.synchronized_mono_ns,
                        challenges: HashMap::new(),
                    };
                }
            }
        }
        Self {
            boot_id,
            synced: false,
            offset_ms: 0,
            synchronized_at_ms: None,
            synchronized_mono_ns: None,
            challenges: HashMap::new(),
        }
    }

    fn persist_best_effort(&self, state_path: Option<&Path>) {
        let Some(path) = state_path else {
            return;
        };
        let doc = PersistedClockState {
            schema: CLOCK_STATE_SCHEMA.to_owned(),
            boot_id: self.boot_id.clone(),
            synced: self.synced,
            offset_ms: self.offset_ms,
            synchronized_at_ms: self.synchronized_at_ms,
            synchronized_mono_ns: self.synchronized_mono_ns,
        };
        if let Ok(mut raw) = serde_json::to_vec_pretty(&doc) {
            raw.push(b'\n');
            let _ = write_bytes_atomic(path, &raw, 0o600, true);
        }
    }

    fn current_unix_ms(&self) -> u64 {
        let base = now_unix_ms() as i128;
        let adjusted = base + i128::from(self.offset_ms);
        u64::try_from(adjusted.max(0)).unwrap_or(u64::MAX)
    }

    fn age_seconds(&self) -> Option<u64> {
        let synced_ns = self.synchronized_mono_ns?;
        let now_ns = monotonic_ns().unwrap_or(synced_ns);
        Some(now_ns.saturating_sub(synced_ns) / 1_000_000_000)
    }

    fn issue_challenge(&mut self, principal_id: &str, from_check: bool) -> String {
        let now = Instant::now();
        self.challenges.retain(|_, v| v.expires_at > now);
        while self.challenges.len() >= MAX_CLOCK_CHALLENGES {
            let Some(earliest_key) = self
                .challenges
                .iter()
                .min_by_key(|(_, v)| v.expires_at)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            self.challenges.remove(&earliest_key);
        }
        let token = new_uuid_v4().replace('-', "");
        self.challenges.insert(
            token.clone(),
            ClockChallenge {
                principal_id: principal_id.to_owned(),
                from_check,
                expires_at: now + Duration::from_secs(5),
            },
        );
        token
    }

    fn take_valid_challenge(
        &mut self,
        challenge: &str,
        principal_id: &str,
        replace: bool,
    ) -> ApiResult<()> {
        let Some(record) = self.challenges.remove(challenge) else {
            return Err(ApiError::conflict(
                "clock_challenge_invalid",
                "clock challenge is expired, already used, or unknown",
            )
            .with_error_code_header());
        };
        if record.expires_at <= Instant::now() || record.principal_id != principal_id {
            return Err(ApiError::conflict(
                "clock_challenge_invalid",
                "clock challenge expired or bound to a different principal",
            )
            .with_error_code_header());
        }
        if replace && !record.from_check {
            return Err(ApiError::conflict(
                "clock_challenge_invalid",
                "replace=true requires a fresh /clock/check challenge",
            )
            .with_error_code_header());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct NetworkPersistedClockState {
    schema: String,
    boot_id: String,
    synchronized_at_ms: u64,
    synchronized_mono_ns: u64,
}

/// Shared boot-scoped clock service bridging `/api/v1/clock*`, `NetworkRpcClient`,
/// `<run_dir>/clock-state.json`, and `CaptureCoordinator`'s [`ClockSyncProbe`].
pub struct ClockStateService {
    state_path: Option<PathBuf>,
    network_state_fallback_path: Option<PathBuf>,
    network_probe_client: Option<NetworkRpcClient>,
    network_mutation_client: Option<NetworkRpcClient>,
    allow_kernel_ntp: bool,
    inner: Mutex<ClockRuntimeInner>,
}

impl ClockStateService {
    pub fn new(
        run_dir: Option<PathBuf>,
        network_probe_client: Option<NetworkRpcClient>,
        network_mutation_client: Option<NetworkRpcClient>,
        allow_kernel_ntp: bool,
    ) -> Self {
        let boot_id = read_linux_boot_id();
        let network_state_fallback_path = run_dir.as_ref().map(|dir| dir.join("clock-state.json"));
        // When `miro-ego-network` RPC is configured, `AuthoritativeNetworkController` owns
        // `<run_dir>/clock-state.json`; only use local file persistence in standalone mode.
        let state_path = if network_probe_client.is_none() && network_mutation_client.is_none() {
            network_state_fallback_path.clone()
        } else {
            None
        };
        let inner = ClockRuntimeInner::load(boot_id, state_path.as_deref());
        Self {
            state_path,
            network_state_fallback_path,
            network_probe_client,
            network_mutation_client,
            allow_kernel_ntp,
            inner: Mutex::new(inner),
        }
    }

    fn read_network_clock_state_fallback(
        &self,
        expected_boot_id: &str,
    ) -> Option<NetworkPersistedClockState> {
        let path = self.network_state_fallback_path.as_deref()?;
        let loaded: NetworkPersistedClockState = read_bounded_json(path, 8192, 8).ok()?;
        if loaded.schema != miro_ego_network::clock::CLOCK_STATE_SCHEMA
            || loaded.boot_id != expected_boot_id
        {
            return None;
        }
        Some(loaded)
    }

    fn local_source(&self, guard: &ClockRuntimeInner) -> ClockSyncSource {
        if guard.synced {
            ClockSyncSource::Client
        } else if self.allow_kernel_ntp && is_kernel_ntp_synchronized() {
            ClockSyncSource::Ntp
        } else {
            ClockSyncSource::Unsynchronized
        }
    }

    pub fn status_blocking(&self, principal_id: &str, inspect: bool) -> ApiResult<Value> {
        if let Some(ref rpc) = self.network_probe_client {
            match rpc.clock_status() {
                Ok(snap) => {
                    let mut guard = self.inner.lock().map_err(|_| {
                        ApiError::internal("clock_poisoned", "clock state mutex poisoned")
                    })?;
                    let (challenge, expires_in_ms) = if inspect {
                        (
                            Value::String(guard.issue_challenge(principal_id, true)),
                            5000_u64,
                        )
                    } else if snap.source == "unsynchronized" {
                        (
                            Value::String(guard.issue_challenge(principal_id, false)),
                            5000_u64,
                        )
                    } else {
                        (Value::Null, 0_u64)
                    };
                    return Ok(json!({
                        "schema": "miro-ego.clock-status.v1",
                        "source": snap.source,
                        "unix_time_ms": snap.unix_time_ms,
                        "challenge": challenge,
                        "expires_in_ms": expires_in_ms,
                        "applied": snap.applied,
                        "boot_id": snap.boot_id,
                        "synchronized_at_ms": snap.synchronized_at_ms,
                        "age_seconds": snap.age_seconds,
                    }));
                }
                Err(rpc_err) => {
                    let mut guard = self.inner.lock().map_err(|_| {
                        ApiError::internal("clock_poisoned", "clock state mutex poisoned")
                    })?;
                    if let Some(net_st) = self.read_network_clock_state_fallback(&guard.boot_id) {
                        let source = if self.allow_kernel_ntp && is_kernel_ntp_synchronized() {
                            ClockSyncSource::Ntp
                        } else {
                            ClockSyncSource::Client
                        };
                        let mono_now = monotonic_ns().unwrap_or(net_st.synchronized_mono_ns);
                        let age_seconds =
                            mono_now.saturating_sub(net_st.synchronized_mono_ns) / 1_000_000_000;
                        let (challenge, expires_in_ms) = if inspect {
                            (
                                Value::String(guard.issue_challenge(principal_id, true)),
                                5000_u64,
                            )
                        } else {
                            (Value::Null, 0_u64)
                        };
                        return Ok(json!({
                            "schema": "miro-ego.clock-status.v1",
                            "source": source.as_str(),
                            "unix_time_ms": now_unix_ms(),
                            "challenge": challenge,
                            "expires_in_ms": expires_in_ms,
                            "applied": source == ClockSyncSource::Client,
                            "boot_id": net_st.boot_id,
                            "synchronized_at_ms": net_st.synchronized_at_ms,
                            "age_seconds": age_seconds,
                        }));
                    }
                    return Err(map_clock_rpc_error(rpc_err));
                }
            }
        }

        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ApiError::internal("clock_poisoned", "clock state mutex poisoned"))?;
        let source = self.local_source(&guard);
        let (challenge, expires_in_ms) = if inspect {
            (
                Value::String(guard.issue_challenge(principal_id, true)),
                5000_u64,
            )
        } else if source == ClockSyncSource::Unsynchronized {
            (
                Value::String(guard.issue_challenge(principal_id, false)),
                5000_u64,
            )
        } else {
            (Value::Null, 0_u64)
        };
        Ok(json!({
            "schema": "miro-ego.clock-status.v1",
            "source": source.as_str(),
            "unix_time_ms": guard.current_unix_ms(),
            "challenge": challenge,
            "expires_in_ms": expires_in_ms,
            "applied": source.is_synchronized(),
            "boot_id": guard.boot_id,
            "synchronized_at_ms": guard.synchronized_at_ms,
            "age_seconds": guard.age_seconds(),
        }))
    }

    pub fn sync_blocking(
        &self,
        principal_id: &str,
        challenge: &str,
        unix_time_ms: u64,
        replace: bool,
        check_capture_busy: impl Fn() -> ApiResult<()>,
    ) -> ApiResult<Value> {
        if let Some(ref rpc) = self.network_mutation_client {
            {
                let mut guard = self.inner.lock().map_err(|_| {
                    ApiError::internal("clock_poisoned", "clock state mutex poisoned")
                })?;
                guard.take_valid_challenge(challenge, principal_id, replace)?;
            }
            check_capture_busy()?;
            let snap = rpc
                .clock_sync(unix_time_ms, replace)
                .map_err(map_clock_rpc_error)?;
            if let Ok(mut guard) = self.inner.lock() {
                if snap.source != "unsynchronized" {
                    let sys_now = now_unix_ms() as i128;
                    guard.offset_ms = (i128::from(snap.unix_time_ms) - sys_now)
                        .clamp(i128::from(i64::MIN), i128::from(i64::MAX))
                        as i64;
                    guard.synced = true;
                    guard.synchronized_at_ms = snap.synchronized_at_ms.or(Some(snap.unix_time_ms));
                    guard.synchronized_mono_ns = Some(monotonic_ns().unwrap_or(0));
                    guard.persist_best_effort(self.state_path.as_deref());
                }
            }
            return Ok(json!({
                "schema": "miro-ego.clock-status.v1",
                "source": snap.source,
                "unix_time_ms": snap.unix_time_ms,
                "challenge": Value::Null,
                "expires_in_ms": 0,
                "applied": snap.applied,
                "boot_id": snap.boot_id,
                "synchronized_at_ms": snap.synchronized_at_ms,
                "age_seconds": snap.age_seconds,
            }));
        }

        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ApiError::internal("clock_poisoned", "clock state mutex poisoned"))?;
        guard.take_valid_challenge(challenge, principal_id, replace)?;
        check_capture_busy()?;

        let current_source = self.local_source(&guard);
        let applied = if !current_source.is_synchronized() || replace {
            let sys_now = now_unix_ms() as i128;
            guard.offset_ms = (i128::from(unix_time_ms) - sys_now)
                .clamp(i128::from(i64::MIN), i128::from(i64::MAX))
                as i64;
            guard.synced = true;
            guard.synchronized_at_ms = Some(unix_time_ms);
            guard.synchronized_mono_ns = Some(monotonic_ns().unwrap_or(0));
            guard.persist_best_effort(self.state_path.as_deref());
            true
        } else {
            false
        };
        let final_source = self.local_source(&guard);
        let age_seconds = if applied {
            Some(0)
        } else {
            guard.age_seconds()
        };
        Ok(json!({
            "schema": "miro-ego.clock-status.v1",
            "source": final_source.as_str(),
            "unix_time_ms": guard.current_unix_ms(),
            "challenge": Value::Null,
            "expires_in_ms": 0,
            "applied": applied,
            "boot_id": guard.boot_id,
            "synchronized_at_ms": guard.synchronized_at_ms,
            "age_seconds": age_seconds,
        }))
    }
}

impl ClockSyncProbe for ClockStateService {
    fn snapshot(&self) -> ClockSyncSnapshot {
        let mono_ns = monotonic_ns().unwrap_or(0);
        if let Some(ref rpc) = self.network_probe_client {
            if let Ok(snap) = rpc.clock_status() {
                let source = match snap.source.as_str() {
                    "ntp" => ClockSyncSource::Ntp,
                    "client" => ClockSyncSource::Client,
                    _ => ClockSyncSource::Unsynchronized,
                };
                return ClockSyncSnapshot {
                    boot_id: snap.boot_id,
                    source,
                    unix_time_ms: snap.unix_time_ms,
                    monotonic_ns: mono_ns,
                    synchronized_at_ms: snap.synchronized_at_ms,
                };
            }
        }
        if let Ok(guard) = self.inner.lock() {
            if let Some(net_st) = self.read_network_clock_state_fallback(&guard.boot_id) {
                let source = if self.allow_kernel_ntp && is_kernel_ntp_synchronized() {
                    ClockSyncSource::Ntp
                } else {
                    ClockSyncSource::Client
                };
                return ClockSyncSnapshot {
                    boot_id: net_st.boot_id,
                    source,
                    unix_time_ms: now_unix_ms(),
                    monotonic_ns: mono_ns,
                    synchronized_at_ms: Some(net_st.synchronized_at_ms),
                };
            }
            let source = self.local_source(&guard);
            ClockSyncSnapshot {
                boot_id: guard.boot_id.clone(),
                source,
                unix_time_ms: guard.current_unix_ms(),
                monotonic_ns: mono_ns,
                synchronized_at_ms: guard.synchronized_at_ms,
            }
        } else {
            ClockSyncSnapshot {
                boot_id: read_linux_boot_id(),
                source: ClockSyncSource::Unsynchronized,
                unix_time_ms: now_unix_ms(),
                monotonic_ns: mono_ns,
                synchronized_at_ms: None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn clock_state_service_falls_back_to_run_dir_clock_state_json_on_rpc_failure() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let run_dir = std::env::temp_dir().join(format!(
            "miro-ego-cli-clock-fb-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&run_dir).unwrap();

        let boot_id = read_linux_boot_id();
        let doc = json!({
            "schema": miro_ego_network::clock::CLOCK_STATE_SCHEMA,
            "boot_id": boot_id,
            "synchronized_at_ms": 1_760_000_000_000_u64,
            "synchronized_mono_ns": 1_000_000_000_u64,
        });
        fs::write(
            run_dir.join("clock-state.json"),
            serde_json::to_vec(&doc).unwrap(),
        )
        .unwrap();

        let unreachable_sock = run_dir.join("missing-network.sock");
        let svc = ClockStateService::new(
            Some(run_dir.clone()),
            Some(NetworkRpcClient::new(&unreachable_sock).with_timeout(Duration::from_millis(50))),
            None,
            false,
        );

        let snap = svc.snapshot();
        assert_eq!(snap.source, ClockSyncSource::Client);
        assert_eq!(snap.boot_id, boot_id);
        assert_eq!(snap.synchronized_at_ms, Some(1_760_000_000_000));

        let status = svc.status_blocking("p1", false).unwrap();
        assert_eq!(status["source"], "client");
        assert_eq!(status["applied"], true);

        // Wrong boot_id must NOT be accepted!
        let stale_doc = json!({
            "schema": miro_ego_network::clock::CLOCK_STATE_SCHEMA,
            "boot_id": "00000000-0000-4000-8000-000000000000",
            "synchronized_at_ms": 1_760_000_000_000_u64,
            "synchronized_mono_ns": 1_000_000_000_u64,
        });
        fs::write(
            run_dir.join("clock-state.json"),
            serde_json::to_vec(&stale_doc).unwrap(),
        )
        .unwrap();
        let stale_snap = svc.snapshot();
        assert_eq!(stale_snap.source, ClockSyncSource::Unsynchronized);
        assert!(svc.status_blocking("p1", false).is_err());

        fs::remove_dir_all(run_dir).unwrap();
    }
}
