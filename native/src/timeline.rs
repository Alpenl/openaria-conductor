use std::io;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TimelineError {
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

impl TimelineError {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    fn io(code: &'static str, context: &str, error: io::Error) -> Self {
        Self::new(code, format!("{context}: {error}"))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Timeline {
    start_monotonic_ns: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AudioSync {
    pub(crate) session_start_monotonic_ns: u64,
    pub(crate) started_monotonic_ns: u64,
    pub(crate) stopped_monotonic_ns: u64,
    pub(crate) session_start_offset_ns: i128,
    pub(crate) session_stop_offset_ns: i128,
    pub(crate) session_start_offset_seconds: f64,
    pub(crate) session_stop_offset_seconds: f64,
    pub(crate) sample_duration_ns: u64,
}

impl Timeline {
    pub(crate) fn new(start_monotonic_ns: u64) -> Result<Self, TimelineError> {
        if start_monotonic_ns == 0 {
            return Err(TimelineError::new(
                "invalid_argument",
                "timeline start monotonic timestamp must be positive",
            ));
        }
        Ok(Self { start_monotonic_ns })
    }

    pub(crate) fn start_now() -> Result<Self, TimelineError> {
        Self::new(monotonic_ns()?)
    }

    pub(crate) fn start_monotonic_ns(&self) -> u64 {
        self.start_monotonic_ns
    }

    pub(crate) fn elapsed_ns(&self) -> Result<u64, TimelineError> {
        let now = monotonic_ns()?;
        now.checked_sub(self.start_monotonic_ns).ok_or_else(|| {
            TimelineError::new(
                "clock_regressed",
                "monotonic clock regressed before timeline start",
            )
        })
    }

    pub(crate) fn elapsed_seconds(&self) -> Result<f64, TimelineError> {
        Ok(ns_to_seconds_unsigned(self.elapsed_ns()?))
    }

    pub(crate) fn offset_ns(&self, monotonic_ns: u64) -> i128 {
        i128::from(monotonic_ns) - i128::from(self.start_monotonic_ns)
    }

    pub(crate) fn offset_seconds(&self, monotonic_ns: u64) -> f64 {
        ns_to_seconds_signed(self.offset_ns(monotonic_ns))
    }

    pub(crate) fn audio_sync(
        &self,
        started_monotonic_ns: u64,
        stopped_monotonic_ns: u64,
        sample_rate_hz: u32,
    ) -> Result<AudioSync, TimelineError> {
        if started_monotonic_ns == 0 || stopped_monotonic_ns == 0 {
            return Err(TimelineError::new(
                "invalid_argument",
                "audio monotonic timestamps must be positive",
            ));
        }
        if stopped_monotonic_ns < started_monotonic_ns {
            return Err(TimelineError::new(
                "clock_regressed",
                "audio monotonic clock regressed",
            ));
        }
        if sample_rate_hz == 0 {
            return Err(TimelineError::new(
                "invalid_argument",
                "audio sample rate must be positive",
            ));
        }
        let start_offset = self.offset_ns(started_monotonic_ns);
        let stop_offset = self.offset_ns(stopped_monotonic_ns);
        Ok(AudioSync {
            session_start_monotonic_ns: self.start_monotonic_ns,
            started_monotonic_ns,
            stopped_monotonic_ns,
            session_start_offset_ns: start_offset,
            session_stop_offset_ns: stop_offset,
            session_start_offset_seconds: ns_to_seconds_signed(start_offset),
            session_stop_offset_seconds: ns_to_seconds_signed(stop_offset),
            sample_duration_ns: 1_000_000_000_u64 / u64::from(sample_rate_hz),
        })
    }
}

pub(crate) fn monotonic_ns() -> Result<u64, TimelineError> {
    let mut timestamp = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: timestamp points to writable storage.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut timestamp) } != 0 {
        return Err(TimelineError::io(
            "clock_failed",
            "clock_gettime",
            io::Error::last_os_error(),
        ));
    }
    Ok(timestamp.tv_sec as u64 * 1_000_000_000 + timestamp.tv_nsec as u64)
}

fn ns_to_seconds_unsigned(ns: u64) -> f64 {
    ns as f64 / 1e9
}

fn ns_to_seconds_signed(ns: i128) -> f64 {
    ns as f64 / 1e9
}

#[cfg(test)]
mod tests {
    use super::Timeline;

    #[test]
    fn timeline_offsets_are_relative_to_take_start() {
        let timeline = Timeline::new(1_000_000_000).unwrap();
        assert_eq!(timeline.start_monotonic_ns(), 1_000_000_000);
        assert_eq!(timeline.offset_ns(1_250_000_000), 250_000_000);
        assert_eq!(timeline.offset_seconds(1_250_000_000), 0.25);
        assert_eq!(timeline.offset_ns(900_000_000), -100_000_000);
        assert_eq!(timeline.offset_seconds(900_000_000), -0.1);
    }

    #[test]
    fn audio_sync_uses_same_timeline_start() {
        let timeline = Timeline::new(10_000_000_000).unwrap();
        let sync = timeline
            .audio_sync(10_250_000_000, 12_500_000_000, 48_000)
            .unwrap();
        assert_eq!(sync.session_start_monotonic_ns, 10_000_000_000);
        assert_eq!(sync.session_start_offset_ns, 250_000_000);
        assert_eq!(sync.session_stop_offset_ns, 2_500_000_000);
        assert_eq!(sync.session_start_offset_seconds, 0.25);
        assert_eq!(sync.session_stop_offset_seconds, 2.5);
        assert_eq!(sync.sample_duration_ns, 20_833);
    }

    #[test]
    fn audio_sync_rejects_regression_and_bad_rate() {
        let timeline = Timeline::new(10).unwrap();
        assert!(timeline.audio_sync(20, 19, 48_000).is_err());
        assert!(timeline.audio_sync(20, 21, 0).is_err());
    }
}
