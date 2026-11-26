use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsError {
    pub code: &'static str,
    pub message: String,
}

impl MetricsError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[derive(Clone)]
struct StageStats {
    histogram: [u64; 64],
    total_ns: u64,
    samples: u64,
}

impl StageStats {
    fn new() -> Self {
        Self {
            histogram: [0; 64],
            total_ns: 0,
            samples: 0,
        }
    }
}

#[derive(Clone, Copy)]
struct CopyStats {
    count: u64,
    bytes_total: u64,
}

#[derive(Clone, Copy)]
struct PayloadStats {
    live: i64,
    live_bytes: i64,
    acquired: u64,
    peak_live: u64,
    peak_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageSnapshot {
    pub name: String,
    pub samples: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub total_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopySnapshot {
    pub name: String,
    pub count: u64,
    pub bytes_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadSnapshot {
    pub name: String,
    pub acquired: u64,
    pub live: i64,
    pub live_bytes: i64,
    pub peak_live: u64,
    pub peak_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueSnapshot {
    pub capacity: u64,
    pub peak_depth: u64,
    pub rejected: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LossSnapshot {
    pub source_gap: u64,
    pub queue_rejected: u64,
    pub write_failure: u64,
    pub unknown_gap: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub stages: Vec<StageSnapshot>,
    pub copies: Vec<CopySnapshot>,
    pub payloads: Vec<PayloadSnapshot>,
    pub queue: QueueSnapshot,
    pub loss: LossSnapshot,
}

struct QueueStats {
    capacity: u64,
    peak_depth: u64,
    rejected: u64,
}

struct State {
    stages: BTreeMap<String, StageStats>,
    copies: BTreeMap<String, CopyStats>,
    payloads: BTreeMap<String, PayloadStats>,
    queue: QueueStats,
    source_gap: u64,
    queue_rejected: u64,
    write_failure: u64,
    unknown_gap: u64,
}

impl State {
    fn new() -> Self {
        Self {
            stages: BTreeMap::new(),
            copies: BTreeMap::new(),
            payloads: BTreeMap::new(),
            queue: QueueStats {
                capacity: 1,
                peak_depth: 0,
                rejected: 0,
            },
            source_gap: 0,
            queue_rejected: 0,
            write_failure: 0,
            unknown_gap: 0,
        }
    }
}

pub struct Metrics {
    state: Mutex<State>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::new()),
        }
    }

    pub fn record_stage(&self, name: &str, elapsed_ns: u64) -> Result<(), MetricsError> {
        let bucket = stage_bucket(elapsed_ns);
        let mut state = self.state.lock().map_err(|_| {
            MetricsError::new("metrics_poisoned", "performance metrics mutex is poisoned")
        })?;
        let stats = state
            .stages
            .entry(name.to_owned())
            .or_insert_with(StageStats::new);
        stats.histogram[bucket] = stats.histogram[bucket]
            .checked_add(1)
            .ok_or_else(|| MetricsError::new("metrics_overflow", "stage histogram overflowed"))?;
        stats.total_ns = stats
            .total_ns
            .checked_add(elapsed_ns)
            .ok_or_else(|| MetricsError::new("metrics_overflow", "stage total overflowed"))?;
        stats.samples = stats.samples.checked_add(1).ok_or_else(|| {
            MetricsError::new("metrics_overflow", "stage sample count overflowed")
        })?;
        Ok(())
    }

    pub fn record_copy(&self, name: &str, size: u64, count: u64) -> Result<(), MetricsError> {
        if size == 0 || count == 0 {
            return Ok(());
        }
        let mut state = self.state.lock().map_err(|_| {
            MetricsError::new("metrics_poisoned", "performance metrics mutex is poisoned")
        })?;
        let copied = state.copies.entry(name.to_owned()).or_insert(CopyStats {
            count: 0,
            bytes_total: 0,
        });
        copied.count = copied
            .count
            .checked_add(count)
            .ok_or_else(|| MetricsError::new("metrics_overflow", "copy count overflowed"))?;
        copied.bytes_total = copied
            .bytes_total
            .checked_add(size)
            .ok_or_else(|| MetricsError::new("metrics_overflow", "copy byte count overflowed"))?;
        Ok(())
    }

    pub fn change_payload(
        &self,
        name: &str,
        count_delta: i64,
        bytes_delta: i64,
    ) -> Result<(), MetricsError> {
        let mut state = self.state.lock().map_err(|_| {
            MetricsError::new("metrics_poisoned", "performance metrics mutex is poisoned")
        })?;
        let payload = state
            .payloads
            .entry(name.to_owned())
            .or_insert(PayloadStats {
                live: 0,
                live_bytes: 0,
                acquired: 0,
                peak_live: 0,
                peak_bytes: 0,
            });
        let live = payload.live.checked_add(count_delta).ok_or_else(|| {
            MetricsError::new("metrics_overflow", "payload live count overflowed")
        })?;
        let live_bytes = payload.live_bytes.checked_add(bytes_delta).ok_or_else(|| {
            MetricsError::new("metrics_overflow", "payload live byte count overflowed")
        })?;
        if live < 0 || live_bytes < 0 {
            return Err(MetricsError::new(
                "payload_underflow",
                format!("payload lifetime underflow: {name}"),
            ));
        }
        payload.live = live;
        payload.live_bytes = live_bytes;
        if count_delta > 0 {
            payload.acquired = payload
                .acquired
                .checked_add(count_delta as u64)
                .ok_or_else(|| {
                    MetricsError::new("metrics_overflow", "payload acquired count overflowed")
                })?;
        }
        payload.peak_live = payload.peak_live.max(live as u64);
        payload.peak_bytes = payload.peak_bytes.max(live_bytes as u64);
        Ok(())
    }

    pub fn observe_queue(
        &self,
        depth: u64,
        capacity: u64,
        rejected: u64,
        peak_depth: Option<u64>,
    ) -> Result<(), MetricsError> {
        let peak = peak_depth.unwrap_or(depth);
        if capacity == 0 || depth > capacity || peak < depth || peak > capacity {
            return Err(MetricsError::new(
                "invalid_argument",
                "invalid bounded queue observation",
            ));
        }
        let mut state = self.state.lock().map_err(|_| {
            MetricsError::new("metrics_poisoned", "performance metrics mutex is poisoned")
        })?;
        state.queue.capacity = state.queue.capacity.max(capacity);
        state.queue.peak_depth = state.queue.peak_depth.max(peak);
        state.queue.rejected =
            state.queue.rejected.checked_add(rejected).ok_or_else(|| {
                MetricsError::new("metrics_overflow", "queue rejection overflowed")
            })?;
        Ok(())
    }

    pub fn record_loss(&self, kind: &str, count: u64) -> Result<(), MetricsError> {
        let mut state = self.state.lock().map_err(|_| {
            MetricsError::new("metrics_poisoned", "performance metrics mutex is poisoned")
        })?;
        let target = match kind {
            "source_gap" => &mut state.source_gap,
            "queue_rejected" => &mut state.queue_rejected,
            "write_failure" => &mut state.write_failure,
            "unknown_gap" => &mut state.unknown_gap,
            _ => {
                return Err(MetricsError::new(
                    "invalid_argument",
                    "invalid loss observation",
                ));
            }
        };
        *target = target
            .checked_add(count)
            .ok_or_else(|| MetricsError::new("metrics_overflow", "loss counter overflowed"))?;
        Ok(())
    }

    pub fn snapshot(&self) -> Result<MetricsSnapshot, MetricsError> {
        let state = self.state.lock().map_err(|_| {
            MetricsError::new("metrics_poisoned", "performance metrics mutex is poisoned")
        })?;
        let stages = state
            .stages
            .iter()
            .map(|(name, stats)| StageSnapshot {
                name: name.clone(),
                samples: stats.samples,
                p50_ns: percentile(&stats.histogram, stats.samples, 0.50),
                p95_ns: percentile(&stats.histogram, stats.samples, 0.95),
                total_ns: stats.total_ns,
            })
            .collect();
        let copies = state
            .copies
            .iter()
            .map(|(name, stats)| CopySnapshot {
                name: name.clone(),
                count: stats.count,
                bytes_total: stats.bytes_total,
            })
            .collect();
        let payloads = state
            .payloads
            .iter()
            .map(|(name, stats)| PayloadSnapshot {
                name: name.clone(),
                acquired: stats.acquired,
                live: stats.live,
                live_bytes: stats.live_bytes,
                peak_live: stats.peak_live,
                peak_bytes: stats.peak_bytes,
            })
            .collect();
        Ok(MetricsSnapshot {
            stages,
            copies,
            payloads,
            queue: QueueSnapshot {
                capacity: state.queue.capacity,
                peak_depth: state.queue.peak_depth,
                rejected: state.queue.rejected,
            },
            loss: LossSnapshot {
                source_gap: state.source_gap,
                queue_rejected: state.queue_rejected,
                write_failure: state.write_failure,
                unknown_gap: state.unknown_gap,
            },
        })
    }
}

fn stage_bucket(elapsed_ns: u64) -> usize {
    if elapsed_ns == 0 {
        0
    } else {
        (u64::BITS - elapsed_ns.leading_zeros()).min(63) as usize
    }
}

fn percentile(histogram: &[u64; 64], samples: u64, percentile: f64) -> u64 {
    let threshold = ((samples as f64) * percentile + 0.999999).max(1.0) as u64;
    let mut seen = 0_u64;
    for (bucket, count) in histogram.iter().enumerate() {
        seen += count;
        if seen >= threshold {
            return if bucket == 0 { 0 } else { 1 << (bucket - 1) };
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::{Metrics, percentile, stage_bucket};

    #[test]
    fn stage_bucket_matches_python_bit_length_shape() {
        assert_eq!(stage_bucket(0), 0);
        assert_eq!(stage_bucket(1), 1);
        assert_eq!(stage_bucket(2), 2);
        assert_eq!(stage_bucket(3), 2);
        assert_eq!(stage_bucket(u64::MAX), 63);
    }

    #[test]
    fn percentile_returns_bucket_floor() {
        let mut histogram = [0_u64; 64];
        histogram[1] = 2;
        histogram[3] = 2;
        assert_eq!(percentile(&histogram, 4, 0.50), 1);
        assert_eq!(percentile(&histogram, 4, 0.95), 4);
    }

    #[test]
    fn zero_byte_copies_are_not_reported() {
        let metrics = Metrics::new();
        metrics.record_copy("empty_left", 0, 1).unwrap();
        metrics.record_copy("empty_right", 0, 3).unwrap();
        metrics.record_copy("no_count", 512, 0).unwrap();

        assert!(metrics.state.lock().unwrap().copies.is_empty());
    }
}
