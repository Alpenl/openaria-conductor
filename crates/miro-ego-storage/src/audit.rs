//! Bounded rotating API authorization audit logger with zero-system-disk RAM fallback.
//!
//! Writes `miro-ego.api-audit-event.v1` NDJSON records to `/data/.miro-ego/audit/api-audit.ndjson`
//! with fixed-size rotation (default 16 MiB x 2). Whenever `/data` is unmounted, read-only,
//! or out of space, audit events fall back automatically to
//! [`miro_ego_core::telemetry::BoundedRingLog`] in RAM (`<= 4 MiB`), guaranteeing ZERO
//! writes to the 20 GiB ext4 system disk.

use crate::util::{now_rfc3339_utc, parse_rfc3339_utc_nanos};
use crate::{StorageError, StorageResult};
use miro_ego_core::paths::DataVolumePaths;
use miro_ego_core::sys::clock::monotonic_ns;
use miro_ego_core::sys::fs::{fstat, fsync_directory, open_directory_safe, pread_retry};
use miro_ego_core::telemetry::{
    BoundedRingLog, LogLevel, MAX_RING_BYTES, RingStats, TelemetryEntry,
};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const DEFAULT_ENOSPC_BACKOFF: Duration = Duration::from_millis(250);
const DEFAULT_IDLE_HANDLE_TIMEOUT: Duration = Duration::from_secs(5);

pub const API_AUDIT_EVENT_SCHEMA: &str = "miro-ego.api-audit-event.v1";
pub const AUDIT_LOG_FILENAME: &str = "api-audit.ndjson";
/// Default max bytes per active audit log generation (16 MiB).
pub const DEFAULT_AUDIT_MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;
/// Default number of retained generations (active + 1 rotated backup = 32 MiB max).
pub const DEFAULT_AUDIT_MAX_GENERATIONS: usize = 2;

/// Re-export alias for the bounded in-memory telemetry ring fallback.
pub type BoundedRingBuffer = BoundedRingLog;

/// Closed schema matching `contracts/schemas/miro-ego-api-audit-event-v1.schema.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiAuditEvent {
    pub schema: String,
    pub recorded_at: String,
    pub request_id: String,
    pub principal_id: Option<String>,
    pub operation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub poll_count: Option<u64>,
    pub resource_id: Option<String>,
    pub outcome: String,
}

impl ApiAuditEvent {
    pub fn new(
        request_id: impl Into<String>,
        principal_id: Option<String>,
        operation_id: impl Into<String>,
        resource_id: Option<String>,
        outcome: impl Into<String>,
    ) -> Self {
        Self {
            schema: API_AUDIT_EVENT_SCHEMA.to_owned(),
            recorded_at: now_rfc3339_utc(),
            request_id: request_id.into(),
            principal_id,
            operation_id: operation_id.into(),
            poll_count: None,
            resource_id,
            outcome: outcome.into(),
        }
    }

    pub fn validate(&self) -> StorageResult<()> {
        if self.schema != API_AUDIT_EVENT_SCHEMA {
            return Err(StorageError::invalid_input(
                "invalid_audit_event",
                format!("audit event schema must be {API_AUDIT_EVENT_SCHEMA}"),
            ));
        }
        if parse_rfc3339_utc_nanos(&self.recorded_at).is_none() {
            return Err(StorageError::invalid_input(
                "invalid_audit_event",
                "audit recorded_at must be a valid RFC 3339 date-time",
            ));
        }
        if !(1..=128).contains(&self.request_id.chars().count()) {
            return Err(StorageError::invalid_input(
                "invalid_audit_event",
                "audit request_id must be 1..=128 chars",
            ));
        }
        if !(1..=128).contains(&self.operation_id.chars().count()) {
            return Err(StorageError::invalid_input(
                "invalid_audit_event",
                "audit operation_id must be 1..=128 chars",
            ));
        }
        if !(1..=128).contains(&self.outcome.chars().count()) {
            return Err(StorageError::invalid_input(
                "invalid_audit_event",
                "audit outcome must be 1..=128 chars",
            ));
        }
        if let Some(principal) = &self.principal_id {
            if principal.chars().count() > 128 {
                return Err(StorageError::invalid_input(
                    "invalid_audit_event",
                    "audit principal_id exceeds 128 chars",
                ));
            }
        }
        if let Some(resource) = &self.resource_id {
            if resource.chars().count() > 512 {
                return Err(StorageError::invalid_input(
                    "invalid_audit_event",
                    "audit resource_id exceeds 512 chars",
                ));
            }
        }
        if let Some(count) = self.poll_count {
            if count == 0 {
                return Err(StorageError::invalid_input(
                    "invalid_audit_event",
                    "audit poll_count must be >= 1",
                ));
            }
        }
        Ok(())
    }
}

/// Configuration for [`AuditLogger`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditConfig {
    pub max_file_bytes: u64,
    pub max_generations: usize,
    pub fallback_ring_bytes: usize,
    pub idle_close_after: Duration,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            max_file_bytes: DEFAULT_AUDIT_MAX_FILE_BYTES,
            max_generations: DEFAULT_AUDIT_MAX_GENERATIONS,
            fallback_ring_bytes: MAX_RING_BYTES,
            idle_close_after: DEFAULT_IDLE_HANDLE_TIMEOUT,
        }
    }
}

/// Where an individual audit record was durably or transiently stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditWriteTarget {
    Disk,
    MemoryFallback,
    Unavailable,
}

impl AuditWriteTarget {
    /// Returns `Some("unavailable")` when the `Miro-Ego-Audit-Status` header should warn clients.
    pub fn header_status(self) -> Option<&'static str> {
        match self {
            Self::Disk => None,
            Self::MemoryFallback | Self::Unavailable => Some("unavailable"),
        }
    }
}

type VolumeReadyFn = Arc<dyn Fn() -> bool + Send + Sync>;

struct StoredFallbackEntry {
    entry: TelemetryEntry,
    approx_bytes: usize,
}

struct FallbackRingState {
    entries: VecDeque<StoredFallbackEntry>,
    used_bytes: usize,
    dropped_entries: u64,
    dropped_bytes: u64,
    next_seq: u64,
}

struct AuditFallbackRing {
    max_bytes: usize,
    state: Mutex<FallbackRingState>,
}

fn utf8_truncate_prefix(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_owned();
    }
    let mut boundary = max_bytes;
    while boundary > 0 && !text.is_char_boundary(boundary) {
        boundary -= 1;
    }
    text[..boundary].to_owned()
}

fn estimate_telemetry_entry_bytes(entry: &TelemetryEntry) -> usize {
    let serialized = serde_json::to_vec(entry).map(|v| v.len()).unwrap_or(128);
    serialized.saturating_add(64)
}

impl AuditFallbackRing {
    fn new(max_bytes: usize) -> Self {
        let clamped = max_bytes.clamp(1024, MAX_RING_BYTES);
        Self {
            max_bytes: clamped,
            state: Mutex::new(FallbackRingState {
                entries: VecDeque::new(),
                used_bytes: 0,
                dropped_entries: 0,
                dropped_bytes: 0,
                next_seq: 0,
            }),
        }
    }

    fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    fn push(
        &self,
        level: LogLevel,
        target: impl Into<String>,
        event: impl Into<String>,
        payload: serde_json::Value,
    ) -> Result<u64, miro_ego_core::CoreError> {
        let now_ns = monotonic_ns().unwrap_or(0);
        let raw_target = target.into();
        let raw_event = event.into();
        let short_target = utf8_truncate_prefix(&raw_target, 128);
        let short_event = utf8_truncate_prefix(&raw_event, 128);

        let mut guard = self
            .state
            .lock()
            .map_err(|_| miro_ego_core::CoreError::poisoned("audit_fallback_ring"))?;
        let seq = guard.next_seq;
        guard.next_seq = guard.next_seq.saturating_add(1);

        let mut entry = TelemetryEntry {
            seq,
            monotonic_ns: now_ns,
            level,
            target: raw_target,
            event: raw_event,
            payload,
        };
        let mut approx_bytes = estimate_telemetry_entry_bytes(&entry);
        if approx_bytes > self.max_bytes {
            entry.target = short_target;
            entry.event = short_event;
            entry.payload = serde_json::json!({
                "truncated": true,
                "original_bytes": approx_bytes,
            });
            approx_bytes = estimate_telemetry_entry_bytes(&entry);
            if approx_bytes > self.max_bytes {
                guard.dropped_entries = guard.dropped_entries.saturating_add(1);
                guard.dropped_bytes = guard.dropped_bytes.saturating_add(approx_bytes as u64);
                return Ok(seq);
            }
        }

        while guard.used_bytes + approx_bytes > self.max_bytes {
            let Some(evicted) = guard.entries.pop_front() else {
                break;
            };
            guard.used_bytes = guard.used_bytes.saturating_sub(evicted.approx_bytes);
            guard.dropped_entries = guard.dropped_entries.saturating_add(1);
            guard.dropped_bytes = guard
                .dropped_bytes
                .saturating_add(evicted.approx_bytes as u64);
        }

        guard.used_bytes += approx_bytes;
        guard.entries.push_back(StoredFallbackEntry {
            entry,
            approx_bytes,
        });
        Ok(seq)
    }

    fn peek_front(&self) -> Result<Option<TelemetryEntry>, miro_ego_core::CoreError> {
        let guard = self
            .state
            .lock()
            .map_err(|_| miro_ego_core::CoreError::poisoned("audit_fallback_ring"))?;
        Ok(guard.entries.front().map(|s| s.entry.clone()))
    }

    fn pop_front_if_seq(&self, seq: u64) -> Result<bool, miro_ego_core::CoreError> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| miro_ego_core::CoreError::poisoned("audit_fallback_ring"))?;
        if guard.entries.front().is_some_and(|s| s.entry.seq == seq) {
            if let Some(removed) = guard.entries.pop_front() {
                guard.used_bytes = guard.used_bytes.saturating_sub(removed.approx_bytes);
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn stats(&self) -> Result<RingStats, miro_ego_core::CoreError> {
        let guard = self
            .state
            .lock()
            .map_err(|_| miro_ego_core::CoreError::poisoned("audit_fallback_ring"))?;
        Ok(RingStats {
            entry_count: guard.entries.len(),
            used_bytes: guard.used_bytes,
            max_bytes: self.max_bytes,
            dropped_entries: guard.dropped_entries,
            dropped_bytes: guard.dropped_bytes,
            next_seq: guard.next_seq,
        })
    }

    fn snapshot(&self) -> Result<Vec<TelemetryEntry>, miro_ego_core::CoreError> {
        let guard = self
            .state
            .lock()
            .map_err(|_| miro_ego_core::CoreError::poisoned("audit_fallback_ring"))?;
        Ok(guard.entries.iter().map(|s| s.entry.clone()).collect())
    }
}

struct AuditDiskState {
    active_file: Option<io::BufWriter<File>>,
    active_bytes: u64,
    last_write_at: Option<Instant>,
    enospc_backoff_until: Option<Instant>,
}

/// Thread-safe rotating NDJSON audit logger with automatic RAM ring fallback.
pub struct AuditLogger {
    paths: DataVolumePaths,
    config: AuditConfig,
    enospc_backoff: Duration,
    idle_handle_timeout: Duration,
    volume_ready: VolumeReadyFn,
    disk: Mutex<AuditDiskState>,
    fallback: AuditFallbackRing,
}

impl AuditLogger {
    pub fn new(paths: DataVolumePaths, config: AuditConfig, volume_ready: VolumeReadyFn) -> Self {
        let clamped_gens = config.max_generations.max(1);
        let clamped_file_bytes = config.max_file_bytes.max(512);
        let fallback = AuditFallbackRing::new(config.fallback_ring_bytes);
        Self {
            paths,
            config: AuditConfig {
                max_file_bytes: clamped_file_bytes,
                max_generations: clamped_gens,
                fallback_ring_bytes: fallback.max_bytes(),
                idle_close_after: config.idle_close_after,
            },
            enospc_backoff: DEFAULT_ENOSPC_BACKOFF,
            idle_handle_timeout: config.idle_close_after,
            volume_ready,
            disk: Mutex::new(AuditDiskState {
                active_file: None,
                active_bytes: 0,
                last_write_at: None,
                enospc_backoff_until: None,
            }),
            fallback,
        }
    }

    /// Override the idle duration after which `evict_idle_handle` closes the active descriptor.
    pub fn with_idle_handle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_handle_timeout = timeout;
        self
    }

    /// Override the retry backoff duration after an `ENOSPC` / `EDQUOT` write failure.
    pub fn with_enospc_backoff(mut self, backoff: Duration) -> Self {
        self.enospc_backoff = backoff;
        self
    }

    pub fn active_log_path(&self) -> PathBuf {
        self.paths.audit_dir().join(AUDIT_LOG_FILENAME)
    }

    pub fn rotated_log_path(&self, generation: usize) -> PathBuf {
        self.paths
            .audit_dir()
            .join(format!("{AUDIT_LOG_FILENAME}.{generation}"))
    }

    /// Flush, fsync, and close any open active audit file handle (invoked on `SafeSwap` release).
    pub fn close_active_file(&self) {
        let mut guard = match self.disk.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(mut writer) = guard.active_file.take() {
            let _ = writer.flush();
            if let Ok(file) = writer.into_inner() {
                let _ = file.sync_all();
            }
        }
        guard.active_bytes = 0;
        guard.last_write_at = None;
    }

    /// Close the active `/data` audit file descriptor if it has been idle for `>= idle_handle_timeout`,
    /// preventing indefinite open descriptor retention across idle periods.
    pub fn evict_idle_handle(&self) -> bool {
        let mut guard = match self.disk.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let expired = guard
            .last_write_at
            .is_some_and(|last| last.elapsed() >= self.idle_handle_timeout);
        if guard.active_file.is_some() && (expired || !(self.volume_ready)()) {
            if let Some(mut writer) = guard.active_file.take() {
                let _ = writer.flush();
                if let Ok(file) = writer.into_inner() {
                    let _ = file.sync_all();
                }
            }
            guard.active_bytes = 0;
            guard.last_write_at = None;
            return true;
        }
        false
    }

    /// Return whether an active audit file descriptor is currently held open.
    pub fn has_open_file(&self) -> bool {
        match self.disk.lock() {
            Ok(g) => g.active_file.is_some(),
            Err(poisoned) => poisoned.into_inner().active_file.is_some(),
        }
    }

    /// Record one `miro-ego.api-audit-event.v1` event.
    ///
    /// Writes to `/data/.miro-ego/audit/api-audit.ndjson` (with fixed-size rotation) when
    /// the volume is mounted and writable; otherwise stores into the bounded RAM ring with
    /// ZERO system-disk writes.
    pub fn record(&self, event: &ApiAuditEvent) -> StorageResult<AuditWriteTarget> {
        event.validate()?;
        let mut line = serde_json::to_vec(event)
            .map_err(|err| StorageError::invalid_input("audit_encode_failed", err.to_string()))?;
        line.push(b'\n');

        if !(self.volume_ready)() {
            self.close_disk_handle(true);
            return Ok(self.push_to_fallback(event));
        }

        match self.write_to_disk_and_drain_fallback(&line) {
            Ok(()) => Ok(AuditWriteTarget::Disk),
            Err(_) => Ok(self.push_to_fallback(event)),
        }
    }

    /// Flush buffered RAM-fallback audit entries to `/data/.miro-ego/audit/api-audit.ndjson`
    /// once the recording volume is mounted and writable.
    pub fn flush_fallback_to_disk(&self) -> StorageResult<usize> {
        if !(self.volume_ready)() {
            self.close_disk_handle(true);
            return Ok(0);
        }
        let mut guard = self
            .disk
            .lock()
            .map_err(|_| StorageError::Core(miro_ego_core::CoreError::poisoned("audit_disk")))?;
        if guard
            .enospc_backoff_until
            .is_some_and(|until| Instant::now() < until)
        {
            return Ok(0);
        }
        match self.flush_fallback_locked(&mut guard) {
            Ok(count) => Ok(count),
            Err(err) => {
                Self::reset_disk_state_on_error(&mut guard, &err, self.enospc_backoff);
                Err(StorageError::io(
                    "audit_flush_failed",
                    "flush fallback audit log",
                    err,
                ))
            }
        }
    }

    pub fn fallback_stats(&self) -> StorageResult<RingStats> {
        self.fallback.stats().map_err(StorageError::from)
    }

    pub fn fallback_snapshot(&self) -> StorageResult<Vec<TelemetryEntry>> {
        self.fallback.snapshot().map_err(StorageError::from)
    }

    fn close_disk_handle(&self, clear_backoff: bool) {
        let mut guard = match self.disk.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(mut writer) = guard.active_file.take() {
            let _ = writer.flush();
        }
        guard.active_bytes = 0;
        guard.last_write_at = None;
        if clear_backoff {
            guard.enospc_backoff_until = None;
        }
    }

    fn reset_disk_state_on_error(guard: &mut AuditDiskState, err: &io::Error, backoff: Duration) {
        guard.active_file = None;
        guard.active_bytes = 0;
        guard.last_write_at = None;
        if matches!(err.raw_os_error(), Some(libc::ENOSPC | libc::EDQUOT)) && !backoff.is_zero() {
            guard.enospc_backoff_until = Some(Instant::now() + backoff);
        }
    }

    fn push_to_fallback(&self, event: &ApiAuditEvent) -> AuditWriteTarget {
        let payload = serde_json::to_value(event).unwrap_or(serde_json::Value::Null);
        match self
            .fallback
            .push(LogLevel::Info, "api-audit", &event.operation_id, payload)
        {
            Ok(_) => AuditWriteTarget::MemoryFallback,
            Err(_) => AuditWriteTarget::Unavailable,
        }
    }

    fn write_to_disk_and_drain_fallback(&self, line: &[u8]) -> io::Result<()> {
        let mut guard = self
            .disk
            .lock()
            .map_err(|_| io::Error::other("audit disk lock poisoned"))?;

        if guard
            .enospc_backoff_until
            .is_some_and(|until| Instant::now() < until)
        {
            return Err(io::Error::from_raw_os_error(libc::ENOSPC));
        }

        // Drain any previously buffered RAM events first to preserve chronological order.
        if let Err(err) = self.flush_fallback_locked(&mut guard) {
            Self::reset_disk_state_on_error(&mut guard, &err, self.enospc_backoff);
            return Err(err);
        }
        if let Err(err) = self.append_line_locked(&mut guard, line) {
            Self::reset_disk_state_on_error(&mut guard, &err, self.enospc_backoff);
            return Err(err);
        }
        guard.enospc_backoff_until = None;
        Ok(())
    }

    fn flush_fallback_locked(&self, guard: &mut AuditDiskState) -> io::Result<usize> {
        let mut flushed = 0_usize;
        loop {
            let front = self
                .fallback
                .peek_front()
                .map_err(|err| io::Error::other(err.to_string()))?;
            let Some(entry) = front else {
                break;
            };
            let seq = entry.seq;
            if entry.payload.get("truncated") == Some(&serde_json::Value::Bool(true)) {
                let _ = self.fallback.pop_front_if_seq(seq);
                continue;
            }
            let Ok(valid_event) = serde_json::from_value::<ApiAuditEvent>(entry.payload) else {
                let _ = self.fallback.pop_front_if_seq(seq);
                continue;
            };
            if valid_event.validate().is_err() {
                let _ = self.fallback.pop_front_if_seq(seq);
                continue;
            }
            let mut line = serde_json::to_vec(&valid_event)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
            line.push(b'\n');
            if let Err(err) = self.append_line_locked(guard, &line) {
                Self::reset_disk_state_on_error(guard, &err, self.enospc_backoff);
                return Err(err);
            }
            let _ = self
                .fallback
                .pop_front_if_seq(seq)
                .map_err(|err| io::Error::other(err.to_string()))?;
            flushed += 1;
        }
        if flushed > 0 {
            guard.enospc_backoff_until = None;
        }
        Ok(flushed)
    }

    fn append_line_locked(&self, guard: &mut AuditDiskState, line: &[u8]) -> io::Result<()> {
        let res = (|| -> io::Result<()> {
            let audit_dir = self.paths.audit_dir();
            // Verify audit directory exists and is not a symlink (never create `/data` on ext4!).
            let _dir_guard = open_directory_safe(&audit_dir)?;

            if guard.active_file.is_none() {
                let (file, size) = open_active_audit_file(&self.active_log_path())?;
                guard.active_file = Some(io::BufWriter::with_capacity(16 * 1024, file));
                guard.active_bytes = size;
            }

            let incoming = line.len() as u64;
            if guard.active_bytes > 0
                && guard.active_bytes.saturating_add(incoming) > self.config.max_file_bytes
            {
                self.rotate_locked(guard, &audit_dir)?;
            }

            let writer = match guard.active_file.as_mut() {
                Some(writer) => writer,
                None => {
                    let (opened, size) = open_active_audit_file(&self.active_log_path())?;
                    guard.active_bytes = size;
                    guard
                        .active_file
                        .insert(io::BufWriter::with_capacity(16 * 1024, opened))
                }
            };
            writer.write_all(line)?;
            writer.flush()?;
            guard.active_bytes = guard.active_bytes.saturating_add(incoming);
            guard.last_write_at = Some(Instant::now());
            Ok(())
        })();
        if let Err(ref err) = res {
            Self::reset_disk_state_on_error(guard, err, self.enospc_backoff);
        }
        res
    }

    fn rotate_locked(&self, guard: &mut AuditDiskState, audit_dir: &Path) -> io::Result<()> {
        if let Some(mut writer) = guard.active_file.take() {
            let _ = writer.flush();
            if let Ok(file) = writer.into_inner() {
                let _ = file.sync_all();
            }
        }
        guard.active_bytes = 0;

        let max_backups = self.config.max_generations.saturating_sub(1);
        if max_backups == 0 {
            let active = self.active_log_path();
            match fs::remove_file(&active) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
        } else {
            let oldest = self.rotated_log_path(max_backups);
            match fs::remove_file(&oldest) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
            for generation in (1..max_backups).rev() {
                let src = self.rotated_log_path(generation);
                let dst = self.rotated_log_path(generation + 1);
                if fs::symlink_metadata(&src).is_ok() {
                    fs::rename(&src, &dst)?;
                }
            }
            let active = self.active_log_path();
            if fs::symlink_metadata(&active).is_ok() {
                fs::rename(&active, self.rotated_log_path(1))?;
            }
        }
        let _ = fsync_directory(audit_dir);

        let (file, size) = open_active_audit_file(&self.active_log_path())?;
        guard.active_file = Some(io::BufWriter::with_capacity(16 * 1024, file));
        guard.active_bytes = size;
        Ok(())
    }
}

fn open_active_audit_file(path: &Path) -> io::Result<(File, u64)> {
    let file = OpenOptions::new()
        .read(true)
        .create(true)
        .append(true)
        .mode(0o640)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let st = fstat(file.as_raw_fd())?;
    if (st.st_mode & libc::S_IFMT) != libc::S_IFREG {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "audit log target is not a regular file",
        ));
    }
    #[allow(clippy::useless_conversion)]
    let nlink = u64::from(st.st_nlink);
    if nlink != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "audit log target must not be hardlinked",
        ));
    }
    let size = u64::try_from(st.st_size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative audit log size"))?;
    let repaired_size = repair_active_audit_tail(&file, size)?;
    Ok((file, repaired_size))
}

fn ftruncate_audit_fd(fd: libc::c_int, new_size: u64) -> io::Result<()> {
    let off = libc::off_t::try_from(new_size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "audit truncate size overflow"))?;
    loop {
        // SAFETY: `fd` is a valid open regular file descriptor owned by `File`.
        if unsafe { libc::ftruncate(fd, off) } == 0 {
            return Ok(());
        }
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

fn find_last_newline_before(file: &File, end_offset: u64) -> io::Result<Option<u64>> {
    const CHUNK: usize = 8192;
    let mut cursor = end_offset;
    let mut buf = [0_u8; CHUNK];
    while cursor > 0 {
        let read_len = usize::try_from(cursor.min(CHUNK as u64)).unwrap_or(CHUNK);
        let start = cursor - read_len as u64;
        let off = libc::off_t::try_from(start)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "audit offset overflow"))?;
        let n = pread_retry(file.as_raw_fd(), &mut buf[..read_len], off)?;
        if n == 0 {
            break;
        }
        if let Some(rel) = buf[..n].iter().rposition(|&b| b == b'\n') {
            return Ok(Some(start + rel as u64));
        }
        cursor = start;
    }
    Ok(None)
}

/// Inspect the tail of a reopened non-empty `api-audit.ndjson` file and truncate any
/// unterminated partial line (or corrupt trailing line from an interrupted write) back to
/// the last clean `\n` boundary before appending new records.
fn repair_active_audit_tail(file: &File, initial_size: u64) -> io::Result<u64> {
    if initial_size == 0 {
        return Ok(0);
    }

    let mut last_byte = [0_u8; 1];
    let last_off = libc::off_t::try_from(initial_size - 1)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "audit offset overflow"))?;
    let n = pread_retry(file.as_raw_fd(), &mut last_byte, last_off)?;
    let mut clean_size = initial_size;

    if n != 1 || last_byte[0] != b'\n' {
        clean_size = match find_last_newline_before(file, initial_size)? {
            Some(nl_pos) => nl_pos + 1,
            None => 0,
        };
        ftruncate_audit_fd(file.as_raw_fd(), clean_size)?;
        file.sync_all()?;
    }

    // Also verify the trailing newline-terminated record is schema-valid JSON so a prior
    // fused/corrupt line cannot remain at the tail.
    if clean_size > 0 {
        let prev_boundary = match find_last_newline_before(file, clean_size - 1)? {
            Some(nl_pos) => nl_pos + 1,
            None => 0,
        };
        let line_len = clean_size - prev_boundary;
        const MAX_AUDIT_LINE_BYTES: u64 = 64 * 1024;
        let line_valid = if line_len == 0 || line_len > MAX_AUDIT_LINE_BYTES {
            false
        } else {
            let len_usize = line_len as usize;
            let mut line_buf = vec![0_u8; len_usize];
            let off = libc::off_t::try_from(prev_boundary).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "audit offset overflow")
            })?;
            let read = pread_retry(file.as_raw_fd(), &mut line_buf, off)?;
            if read != len_usize {
                false
            } else {
                let trimmed = line_buf.strip_suffix(b"\n").unwrap_or(&line_buf);
                let trimmed = trimmed.strip_suffix(b"\r").unwrap_or(trimmed);
                serde_json::from_slice::<ApiAuditEvent>(trimmed)
                    .is_ok_and(|ev| ev.validate().is_ok())
            }
        };
        if !line_valid {
            clean_size = prev_boundary;
            ftruncate_audit_fd(file.as_raw_fd(), clean_size)?;
            file.sync_all()?;
        }
    }

    Ok(clean_size)
}
