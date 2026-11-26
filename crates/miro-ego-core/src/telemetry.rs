//! Bounded in-memory ring logger and audit fallback (`<= 4 MiB`).
//!
//! When `/data` is unmounted or read-only, operational logs and audit events stay in
//! this fixed-cap RAM ring buffer rather than writing unbounded files to the 20 GiB
//! ext4 system disk.

use crate::error::{CoreError, CoreResult};
use crate::sys::clock::monotonic_ns;
use crate::sys::fs::{open_create_exclusive, sync_and_evict_page_cache};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

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
        let mut guard = self
            .state
            .lock()
            .map_err(|_| CoreError::poisoned("telemetry_ring"))?;
        let seq = guard.next_seq;
        guard.next_seq = guard.next_seq.saturating_add(1);

        let mut entry = TelemetryEntry {
            seq,
            monotonic_ns: now_ns,
            level,
            target: target.into(),
            event: event.into(),
            payload,
        };
        let mut approx_bytes = estimate_entry_bytes(&entry);
        if approx_bytes > self.max_bytes {
            entry.target.truncate(128);
            entry.event.truncate(128);
            entry.payload = serde_json::json!({
                "truncated": true,
                "original_bytes": approx_bytes,
            });
            approx_bytes = estimate_entry_bytes(&entry);
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
    pub fn drain_to_ndjson_file(&self, path: &Path) -> CoreResult<usize> {
        let entries = self.drain()?;
        if entries.is_empty() {
            return Ok(0);
        }
        let mut file = open_create_exclusive(path, 0o640)
            .map_err(|err| CoreError::io("telemetry_flush_failed", "create telemetry dump", err))?;
        for entry in &entries {
            let line = serde_json::to_vec(entry)
                .map_err(|err| CoreError::invalid_input("telemetry_encode", err.to_string()))?;
            file.write_all(&line)
                .and_then(|()| file.write_all(b"\n"))
                .map_err(|err| {
                    CoreError::io("telemetry_flush_failed", "write telemetry dump", err)
                })?;
        }
        sync_and_evict_page_cache(&file)
            .map_err(|err| CoreError::io("telemetry_flush_failed", "sync telemetry dump", err))?;
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
}
