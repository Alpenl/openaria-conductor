//! Bounded in-memory ring logger and audit fallback (`<= 4 MiB`).
//!
//! When `/data` is unmounted or read-only, operational logs and audit events stay in
//! this fixed-cap RAM ring buffer rather than writing unbounded files to the 20 GiB
//! ext4 system disk.

use crate::error::{CoreError, CoreResult};
use crate::sys::clock::monotonic_ns;
use crate::sys::fs::{
    fsync_directory, open_create_exclusive, renameat2_noreplace, sync_and_evict_page_cache,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::ffi::CString;
use std::io::{self, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

static TELEMETRY_FLUSH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Hard upper bound for in-memory telemetry fallback (4 MiB).
pub const MAX_RING_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TelemetryEntry {
    pub seq: u64,
    pub monotonic_ns: u64,
    pub level: LogLevel,
    pub target: String,
    pub event: String,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RingStats {
    pub entry_count: usize,
    pub used_bytes: usize,
    pub max_bytes: usize,
    pub dropped_entries: u64,
    pub dropped_bytes: u64,
    pub next_seq: u64,
}

struct StoredEntry {
    entry: TelemetryEntry,
    approx_bytes: usize,
}

struct RingState {
    entries: VecDeque<StoredEntry>,
    used_bytes: usize,
    dropped_entries: u64,
    dropped_bytes: u64,
    next_seq: u64,
}

fn floor_char_boundary_at(value: &str, max_bytes: usize) -> usize {
    if max_bytes >= value.len() {
        return value.len();
    }
    let mut index = max_bytes;
    while index > 0 && !value.is_char_boundary(index) {
        index -= 1;
    }
    index
}

/// Thread-safe FIFO ring buffer whose retained footprint never exceeds `max_bytes` (`<= 4 MiB`).
pub struct BoundedRingLog {
    max_bytes: usize,
    state: Mutex<RingState>,
}

impl BoundedRingLog {
    /// Create a ring buffer capped at `max_bytes.clamp(1024, MAX_RING_BYTES)`.
    pub fn new(max_bytes: usize) -> Self {
        let clamped = max_bytes.clamp(1024, MAX_RING_BYTES);
        Self {
            max_bytes: clamped,
            state: Mutex::new(RingState {
                entries: VecDeque::new(),
                used_bytes: 0,
                dropped_entries: 0,
                dropped_bytes: 0,
                next_seq: 0,
            }),
        }
    }

    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Push a structured event into the bounded ring buffer, evicting oldest entries as needed.
    ///
    /// If a single serialized entry exceeds `max_bytes`, its payload is replaced with a compact
    /// truncation marker so the ring invariant `used_bytes <= max_bytes` is never violated.
    pub fn push(
        &self,
        level: LogLevel,
        target: impl Into<String>,
        event: impl Into<String>,
        payload: Value,
    ) -> CoreResult<u64> {
        let now_ns = monotonic_ns().unwrap_or(0);
        let target = target.into();
        let event = event.into();
        let trunc_target_end = floor_char_boundary_at(&target, 128);
        let trunc_event_end = floor_char_boundary_at(&event, 128);

        let mut entry = TelemetryEntry {
            seq: 0,
            monotonic_ns: now_ns,
            level,
            target,
            event,
            payload,
        };
        let pre_approx = estimate_entry_bytes(&entry);
        let mut was_truncated = false;
        if pre_approx > self.max_bytes {
            entry.target.truncate(trunc_target_end);
            entry.event.truncate(trunc_event_end);
            entry.payload = serde_json::json!({
                "truncated": true,
                "original_bytes": pre_approx,
            });
            was_truncated = true;
        }

        let mut guard = self
            .state
            .lock()
            .map_err(|_| CoreError::poisoned("telemetry_ring"))?;
        let seq = guard.next_seq;
        guard.next_seq = guard.next_seq.saturating_add(1);
        entry.seq = seq;

        let mut approx_bytes = estimate_entry_bytes(&entry);
        if approx_bytes > self.max_bytes && !was_truncated {
            entry
                .target
                .truncate(trunc_target_end.min(entry.target.len()));
            entry.event.truncate(trunc_event_end.min(entry.event.len()));
            entry.payload = serde_json::json!({
                "truncated": true,
                "original_bytes": approx_bytes,
            });
            approx_bytes = estimate_entry_bytes(&entry);
        }
        if approx_bytes > self.max_bytes {
            guard.dropped_entries = guard.dropped_entries.saturating_add(1);
            guard.dropped_bytes = guard.dropped_bytes.saturating_add(approx_bytes as u64);
            return Ok(seq);
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
        guard.entries.push_back(StoredEntry {
            entry,
            approx_bytes,
        });
        Ok(seq)
    }

    pub fn stats(&self) -> CoreResult<RingStats> {
        let guard = self
            .state
            .lock()
            .map_err(|_| CoreError::poisoned("telemetry_ring"))?;
        Ok(RingStats {
            entry_count: guard.entries.len(),
            used_bytes: guard.used_bytes,
            max_bytes: self.max_bytes,
            dropped_entries: guard.dropped_entries,
            dropped_bytes: guard.dropped_bytes,
            next_seq: guard.next_seq,
        })
    }

    /// Snapshot all currently buffered entries in chronological order without draining.
    pub fn snapshot(&self) -> CoreResult<Vec<TelemetryEntry>> {
        let guard = self
            .state
            .lock()
            .map_err(|_| CoreError::poisoned("telemetry_ring"))?;
        Ok(guard.entries.iter().map(|s| s.entry.clone()).collect())
    }

    /// Drain all buffered entries in chronological order and reset `used_bytes` to 0.
    pub fn drain(&self) -> CoreResult<Vec<TelemetryEntry>> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| CoreError::poisoned("telemetry_ring"))?;
        guard.used_bytes = 0;
        Ok(guard.entries.drain(..).map(|s| s.entry).collect())
    }

    /// Flush buffered entries to a new NDJSON file (e.g., on `/data/.miro-ego/telemetry/`
    /// once the recording volume is mounted) and return the number of written entries.
    ///
    /// Snapshot-first and fail-safe: buffered entries remain in the ring if creating,
    /// writing, syncing, or atomically publishing `path` fails, and concurrent pushes
    /// arriving during disk I/O (`seq > last_seq`) are preserved.
    pub fn drain_to_ndjson_file(&self, path: &Path) -> CoreResult<usize> {
        let raw_parent = path.parent().ok_or_else(|| {
            CoreError::invalid_input(
                "telemetry_flush_failed",
                "telemetry path has no parent directory",
            )
        })?;
        let parent = if raw_parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            raw_parent
        };
        let file_name = path.file_name().ok_or_else(|| {
            CoreError::invalid_input("telemetry_flush_failed", "telemetry path has no file name")
        })?;
        let dst_c = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            CoreError::invalid_input("telemetry_flush_failed", "telemetry path contains NUL byte")
        })?;

        let entries = self.snapshot()?;
        let Some(last_seq) = entries.last().map(|entry| entry.seq) else {
            return Ok(0);
        };

        match std::fs::symlink_metadata(path) {
            Ok(_) => {
                return Err(CoreError::io(
                    "telemetry_flush_failed",
                    "telemetry dump already exists",
                    io::Error::from_raw_os_error(libc::EEXIST),
                ));
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(CoreError::io(
                    "telemetry_flush_failed",
                    "stat telemetry dump target",
                    err,
                ));
            }
        }

        let flush_seq = TELEMETRY_FLUSH_COUNTER.fetch_add(1, Ordering::Relaxed);
        let temp_path = parent.join(format!(
            ".{}.{}.{}.tmp",
            file_name.to_string_lossy(),
            std::process::id(),
            flush_seq
        ));
        let temp_c = CString::new(temp_path.as_os_str().as_bytes()).map_err(|_| {
            CoreError::invalid_input(
                "telemetry_flush_failed",
                "temporary telemetry path contains NUL byte",
            )
        })?;

        let write_result = (|| -> CoreResult<()> {
            let mut file = open_create_exclusive(&temp_path, 0o640).map_err(|err| {
                CoreError::io("telemetry_flush_failed", "create telemetry dump", err)
            })?;
            for entry in &entries {
                let line = serde_json::to_vec(entry)
                    .map_err(|err| CoreError::invalid_input("telemetry_encode", err.to_string()))?;
                file.write_all(&line)
                    .and_then(|()| file.write_all(b"\n"))
                    .map_err(|err| {
                        CoreError::io("telemetry_flush_failed", "write telemetry dump", err)
                    })?;
            }
            sync_and_evict_page_cache(&file).map_err(|err| {
                CoreError::io("telemetry_flush_failed", "sync telemetry dump", err)
            })?;
            drop(file);

            match renameat2_noreplace(&temp_c, &dst_c) {
                Ok(()) => {}
                Err(err) if matches!(err.raw_os_error(), Some(libc::ENOSYS | libc::EINVAL)) => {
                    match std::fs::symlink_metadata(path) {
                        Ok(_) => {
                            return Err(CoreError::io(
                                "telemetry_flush_failed",
                                "telemetry dump already exists",
                                io::Error::from_raw_os_error(libc::EEXIST),
                            ));
                        }
                        Err(stat_err) if stat_err.kind() == io::ErrorKind::NotFound => {}
                        Err(stat_err) => {
                            return Err(CoreError::io(
                                "telemetry_flush_failed",
                                "stat telemetry dump target",
                                stat_err,
                            ));
                        }
                    }
                    std::fs::rename(&temp_path, path).map_err(|rename_err| {
                        CoreError::io(
                            "telemetry_flush_failed",
                            "publish telemetry dump",
                            rename_err,
                        )
                    })?;
                }
                Err(err) => {
                    return Err(CoreError::io(
                        "telemetry_flush_failed",
                        "publish telemetry dump",
                        err,
                    ));
                }
            }

            fsync_directory(parent).map_err(|err| {
                CoreError::io("telemetry_flush_failed", "sync telemetry directory", err)
            })?;
            Ok(())
        })();

        if let Err(err) = write_result {
            let _ = std::fs::remove_file(&temp_path);
            return Err(err);
        }

        let mut guard = self
            .state
            .lock()
            .map_err(|_| CoreError::poisoned("telemetry_ring"))?;
        while let Some(front) = guard.entries.front() {
            if front.entry.seq <= last_seq {
                if let Some(removed) = guard.entries.pop_front() {
                    guard.used_bytes = guard.used_bytes.saturating_sub(removed.approx_bytes);
                }
            } else {
                break;
            }
        }
        Ok(entries.len())
    }
}

impl Default for BoundedRingLog {
    fn default() -> Self {
        Self::new(MAX_RING_BYTES)
    }
}

fn estimate_entry_bytes(entry: &TelemetryEntry) -> usize {
    let serialized = serde_json::to_vec(entry).map(|v| v.len()).unwrap_or(128);
    serialized.saturating_add(64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn enforces_hard_4mib_cap_and_evicts_oldest_entries_fifo() {
        let capped = BoundedRingLog::new(64 * 1024 * 1024);
        assert_eq!(capped.max_bytes(), MAX_RING_BYTES);

        let small = BoundedRingLog::new(2048);
        assert_eq!(small.max_bytes(), 2048);
        for idx in 0..50_u64 {
            small
                .push(
                    LogLevel::Info,
                    "storage",
                    "heartbeat",
                    serde_json::json!({ "idx": idx, "msg": "0123456789abcdef0123456789abcdef" }),
                )
                .unwrap();
        }
        let stats = small.stats().unwrap();
        assert!(stats.used_bytes <= small.max_bytes());
        assert!(stats.dropped_entries > 0);
        assert!(stats.entry_count > 0);

        let snap = small.snapshot().unwrap();
        assert_eq!(snap.last().unwrap().seq, 49);
        assert!(snap.first().unwrap().seq > 0);
    }

    #[test]
    fn truncates_oversized_single_entry_and_drains_to_ndjson() {
        let ring = BoundedRingLog::new(2048);
        let huge = "x".repeat(8192);
        ring.push(
            LogLevel::Warn,
            "audit",
            "oversized",
            serde_json::json!({ "blob": huge }),
        )
        .unwrap();
        let stats = ring.stats().unwrap();
        assert!(stats.used_bytes <= 2048);
        assert_eq!(stats.entry_count, 1);

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "miro-ego-telemetry-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&dir).unwrap();
        let out = dir.join("fallback.ndjson");
        let flushed = ring.drain_to_ndjson_file(&out).unwrap();
        assert_eq!(flushed, 1);
        assert_eq!(ring.stats().unwrap().used_bytes, 0);

        let text = fs::read_to_string(&out).unwrap();
        let parsed: TelemetryEntry = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(parsed.payload["truncated"], true);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn utf8_multibyte_truncation_never_panics_and_failed_drain_preserves_entries() {
        let ring = BoundedRingLog::new(1024);
        // Construct strings where byte 128 lands inside a 3-byte CJK character and 4-byte emoji.
        let multibyte_target = format!("{}{}", "a".repeat(127), "中文测试".repeat(20));
        let multibyte_event = format!("{}{}", "b".repeat(126), "🦀🔥🚀".repeat(20));
        assert!(!multibyte_target.is_char_boundary(128));
        assert!(!multibyte_event.is_char_boundary(128));

        let seq0 = ring
            .push(
                LogLevel::Error,
                multibyte_target,
                multibyte_event,
                serde_json::json!({ "blob": "z".repeat(4096) }),
            )
            .unwrap();
        assert_eq!(seq0, 0);
        let snap = ring.snapshot().unwrap();
        assert_eq!(snap.len(), 1);
        assert!(snap[0].target.len() <= 128);
        assert!(snap[0].event.len() <= 128);
        assert_eq!(snap[0].payload["truncated"], true);

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "miro-ego-telemetry-preserve-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&dir).unwrap();

        // Drain to missing directory fails without losing buffered entries.
        let missing = dir.join("missing-sub").join("dump.ndjson");
        assert!(ring.drain_to_ndjson_file(&missing).is_err());
        assert_eq!(ring.stats().unwrap().entry_count, 1);
        assert!(ring.stats().unwrap().used_bytes > 0);

        // Drain to pre-existing file fails without overwriting or losing entries.
        let existing = dir.join("existing.ndjson");
        fs::write(&existing, b"keep\n").unwrap();
        assert!(ring.drain_to_ndjson_file(&existing).is_err());
        assert_eq!(fs::read_to_string(&existing).unwrap(), "keep\n");
        assert_eq!(ring.stats().unwrap().entry_count, 1);

        // Subsequent drain to valid target succeeds and empties ring.
        let valid = dir.join("ok.ndjson");
        assert_eq!(ring.drain_to_ndjson_file(&valid).unwrap(), 1);
        assert_eq!(ring.stats().unwrap().entry_count, 0);
        assert_eq!(ring.stats().unwrap().used_bytes, 0);

        fs::remove_dir_all(dir).unwrap();
    }
}
