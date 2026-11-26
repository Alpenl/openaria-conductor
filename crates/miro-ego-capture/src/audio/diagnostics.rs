use crate::audio::AudioProgress;
use miro_ego_core::sys::clock;

pub const IO_BUCKET_NS: [u64; 10] = [
    100_000,
    1_000_000,
    5_000_000,
    20_000_000,
    100_000_000,
    500_000_000,
    1_000_000_000,
    2_000_000_000,
    5_000_000_000,
    10_000_000_000,
];
pub const MAX_SLOW_IO_EVENTS: usize = 64;

#[derive(Default)]
pub struct IoTiming {
    pub count: u64,
    pub failures: u64,
    pub bytes: u64,
    pub total_ns: u64,
    pub max_ns: u64,
    pub histogram: [u64; 11],
}

impl IoTiming {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "count": self.count, "failures": self.failures, "bytes": self.bytes,
            "total_ns": self.total_ns, "max_ns": self.max_ns, "histogram": self.histogram,
        })
    }

    pub fn record(&mut self, elapsed_ns: u64, bytes: u64, failed: bool) {
        self.count += 1;
        self.failures += u64::from(failed);
        self.bytes += bytes;
        self.total_ns += elapsed_ns;
        self.max_ns = self.max_ns.max(elapsed_ns);
        let bucket = IO_BUCKET_NS.partition_point(|bound| elapsed_ns > *bound);
        self.histogram[bucket] += 1;
    }
}

#[derive(Clone, Copy)]
pub enum IoOperation {
    Open,
    Write,
    Sync,
    Seal,
    Checkpoint,
}

#[derive(Default)]
pub struct WriterDiagnostics {
    pub open: IoTiming,
    pub write: IoTiming,
    pub sync: IoTiming,
    pub seal: IoTiming,
    pub checkpoint: IoTiming,
    pub slow_events: Vec<serde_json::Value>,
    pub slow_events_omitted: u64,
}

impl WriterDiagnostics {
    pub fn record_slow_event(&mut self, event: serde_json::Value) {
        if self.slow_events.len() == MAX_SLOW_IO_EVENTS {
            // Keep startup context and the latest events preceding a fault.
            self.slow_events.remove(MAX_SLOW_IO_EVENTS / 2);
            self.slow_events_omitted += 1;
        }
        self.slow_events.push(event);
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "open": self.open.json(), "write": self.write.json(),
            "sync": self.sync.json(), "seal": self.seal.json(),
            "checkpoint": self.checkpoint.json(),
            "slow_events": self.slow_events, "slow_events_omitted": self.slow_events_omitted,
        })
    }
}

pub fn measure_io<T, E>(
    progress: &AudioProgress,
    operation: IoOperation,
    bytes: u64,
    action: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    let started = std::time::Instant::now();
    let result = action();
    let elapsed = started.elapsed().as_nanos() as u64;
    if let Ok(mut diagnostics) = progress.writer_diagnostics.lock() {
        let (name, timing) = match operation {
            IoOperation::Open => ("open", &mut diagnostics.open),
            IoOperation::Write => ("write", &mut diagnostics.write),
            IoOperation::Sync => ("sync", &mut diagnostics.sync),
            IoOperation::Seal => ("seal", &mut diagnostics.seal),
            IoOperation::Checkpoint => ("checkpoint", &mut diagnostics.checkpoint),
        };
        timing.record(elapsed, bytes, result.is_err());
        if elapsed >= 100_000_000 {
            diagnostics.record_slow_event(serde_json::json!({
                "operation": name, "elapsed_ns": elapsed,
                "ended_monotonic_ns": clock::monotonic_ns().unwrap_or(0),
                "bytes": bytes, "failed": result.is_err(),
            }));
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slow_io_samples_keep_startup_and_failure_tail_with_bounded_space() {
        let mut diagnostics = WriterDiagnostics::default();
        for index in 0..100 {
            diagnostics.record_slow_event(serde_json::json!({"ended_monotonic_ns": index}));
            assert!(diagnostics.slow_events.len() <= MAX_SLOW_IO_EVENTS);
        }
        let json = diagnostics.json();
        let retained: Vec<u64> = json["slow_events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event["ended_monotonic_ns"].as_u64().unwrap())
            .collect();
        assert_eq!(retained, (0..32).chain(68..100).collect::<Vec<_>>());
        assert_eq!(json["slow_events_omitted"], 36);
    }

    #[test]
    fn io_histogram_preserves_tail_and_failure_counts() {
        let mut timing = IoTiming::default();
        for (elapsed, failed) in [(100_000, false), (100_001, false), (16_000_000_000, true)] {
            timing.record(elapsed, 4096, failed);
        }
        assert_eq!(timing.count, 3);
        assert_eq!(timing.failures, 1);
        assert_eq!(timing.bytes, 12288);
        assert_eq!(timing.max_ns, 16_000_000_000);
        assert_eq!(timing.histogram[0], 1);
        assert_eq!(timing.histogram[1], 1);
        assert_eq!(timing.histogram[10], 1);
        assert_eq!(timing.histogram.iter().sum::<u64>(), 3);
    }

    #[test]
    fn io_measurement_does_not_swallow_disk_errors() {
        let progress = AudioProgress::default();
        let result: std::io::Result<()> = measure_io(&progress, IoOperation::Sync, 4096, || {
            Err(std::io::Error::from_raw_os_error(libc::ENOSPC))
        });
        assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::ENOSPC));
        let diagnostics = progress.writer_diagnostics.lock().unwrap();
        assert_eq!(diagnostics.sync.count, 1);
        assert_eq!(diagnostics.sync.failures, 1);
        assert_eq!(diagnostics.write.count, 0);
    }
}
