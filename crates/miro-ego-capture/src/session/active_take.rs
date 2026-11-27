#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveTakeError {
    pub code: &'static str,
    pub message: String,
}

impl ActiveTakeError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveSourceFrame {
    pub source_sequence: u64,
    pub host_monotonic_ns: u64,
    pub source_gap: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservedFrame {
    pub session_id: String,
    pub record_sequence: u64,
    pub source_sequence: u64,
    pub host_monotonic_ns: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ActiveDropEvent {
    pub start_frame: u64,
    pub end_frame: u64,
    pub at_time_seconds: f64,
    pub reason: &'static str,
    pub dropped: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ActiveTakeSnapshot {
    pub session_id: String,
    pub frame_domain: u64,
    pub frames_written: u64,
    pub bytes_written: u64,
    pub dropped_frames: u64,
    pub pending_frames: u64,
    pub drop_events: Vec<ActiveDropEvent>,
}

pub type ActiveTakeSummary = ActiveTakeSnapshot;

pub struct ActiveTakeWriter {
    session_id: String,
    frame_domain: u64,
    frames_written: u64,
    bytes_written: u64,
    dropped_frames: u64,
    corrupt_frames_dropped: u64,
    drop_events: Vec<ActiveDropEvent>,
    pending_frame: Option<u64>,
    closed: bool,
}

impl ActiveTakeWriter {
    pub fn new(session_id: &str) -> Result<Self, ActiveTakeError> {
        if session_id.is_empty() {
            return Err(ActiveTakeError::new(
                "invalid_argument",
                "session_id must not be empty",
            ));
        }
        Ok(Self {
            session_id: session_id.to_owned(),
            frame_domain: 0,
            frames_written: 0,
            bytes_written: 0,
            dropped_frames: 0,
            corrupt_frames_dropped: 0,
            drop_events: Vec::new(),
            pending_frame: None,
            closed: false,
        })
    }

    pub fn corrupt_frames_dropped(&self) -> u64 {
        self.corrupt_frames_dropped
    }

    pub fn record_corrupt_drops(
        &mut self,
        dropped: u64,
        at_time_seconds: f64,
    ) -> Result<(), ActiveTakeError> {
        self.ensure_open()?;
        if dropped == 0 {
            return Ok(());
        }
        let next_corrupt = self
            .corrupt_frames_dropped
            .checked_add(dropped)
            .ok_or_else(|| {
                ActiveTakeError::new(
                    "counter_overflow",
                    "active take corrupt frame drop count overflow",
                )
            })?;
        self.record_recovered_drops(dropped, at_time_seconds)?;
        self.corrupt_frames_dropped = next_corrupt;
        Ok(())
    }

    pub fn record_recovered_drops(
        &mut self,
        dropped: u64,
        at_time_seconds: f64,
    ) -> Result<(), ActiveTakeError> {
        self.ensure_open()?;
        if dropped == 0 {
            return Ok(());
        }
        let at_time_seconds = if at_time_seconds.is_finite() && at_time_seconds >= 0.0 {
            at_time_seconds
        } else {
            0.0
        };
        let start_frame = self.frames_written;
        let end_frame = start_frame.checked_add(dropped).ok_or_else(|| {
            ActiveTakeError::new("counter_overflow", "active take drop frame range overflow")
        })?;
        let next_dropped = self.dropped_frames.checked_add(dropped).ok_or_else(|| {
            ActiveTakeError::new(
                "counter_overflow",
                "active take dropped frame count overflow",
            )
        })?;
        self.dropped_frames = next_dropped;
        self.drop_events.push(ActiveDropEvent {
            start_frame,
            end_frame,
            at_time_seconds,
            reason: "write_backpressure",
            dropped,
        });
        Ok(())
    }

    pub fn reserve_frame(
        &mut self,
        source: ActiveSourceFrame,
    ) -> Result<ReservedFrame, ActiveTakeError> {
        self.ensure_open()?;
        if source.source_gap != 0 {
            return Err(ActiveTakeError::new(
                "source_sequence_gap",
                format!("source frame sequence has a gap of {}", source.source_gap),
            ));
        }
        // Capture owns one synchronous writer. A failed write remains pending
        // until teardown; it must not be overwritten by another reservation.
        if self.pending_frame.is_some() {
            return Err(ActiveTakeError::new(
                "invalid_state",
                "active take already has a pending frame",
            ));
        }
        let record_sequence = self.frame_domain;
        let next_frame_domain = self.frame_domain.checked_add(1).ok_or_else(|| {
            ActiveTakeError::new("counter_overflow", "active take frame domain overflow")
        })?;
        self.frame_domain = next_frame_domain;
        self.pending_frame = Some(record_sequence);
        Ok(ReservedFrame {
            session_id: self.session_id.clone(),
            record_sequence,
            source_sequence: source.source_sequence,
            host_monotonic_ns: source.host_monotonic_ns,
        })
    }

    pub fn finish_frame(
        &mut self,
        frame: ReservedFrame,
        bytes_written: u64,
    ) -> Result<(), ActiveTakeError> {
        self.ensure_pending(&frame)?;
        let next_frames_written = self.frames_written.checked_add(1).ok_or_else(|| {
            ActiveTakeError::new("counter_overflow", "active take frame count overflow")
        })?;
        let next_bytes_written =
            self.bytes_written
                .checked_add(bytes_written)
                .ok_or_else(|| {
                    ActiveTakeError::new("counter_overflow", "active take byte count overflow")
                })?;
        self.pending_frame = None;
        self.frames_written = next_frames_written;
        self.bytes_written = next_bytes_written;
        Ok(())
    }

    pub fn finish(&mut self) -> Result<ActiveTakeSummary, ActiveTakeError> {
        self.ensure_open()?;
        if self.pending_frame.is_some() {
            return Err(ActiveTakeError::new(
                "invalid_state",
                "active take has pending frames",
            ));
        }
        self.closed = true;
        Ok(self.snapshot())
    }

    pub fn snapshot(&self) -> ActiveTakeSnapshot {
        ActiveTakeSnapshot {
            session_id: self.session_id.clone(),
            frame_domain: self.frame_domain,
            frames_written: self.frames_written,
            bytes_written: self.bytes_written,
            // The encoded frame index is dense. Source gaps remain visible in
            // its original source sequences and timestamps, and in drop_events.
            dropped_frames: self.dropped_frames,
            pending_frames: u64::from(self.pending_frame.is_some()),
            drop_events: self.drop_events.clone(),
        }
    }

    fn ensure_open(&self) -> Result<(), ActiveTakeError> {
        if self.closed {
            Err(ActiveTakeError::new(
                "invalid_state",
                "active take is already finished",
            ))
        } else {
            Ok(())
        }
    }

    fn ensure_pending(&self, frame: &ReservedFrame) -> Result<(), ActiveTakeError> {
        self.ensure_open()?;
        if frame.session_id != self.session_id {
            return Err(ActiveTakeError::new(
                "invalid_session",
                "reserved frame belongs to another session",
            ));
        }
        if self.pending_frame != Some(frame.record_sequence) {
            return Err(ActiveTakeError::new(
                "invalid_state",
                "reserved frame is not pending",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{ActiveSourceFrame, ActiveTakeError, ActiveTakeSnapshot, ActiveTakeWriter};

    fn source_frame(source_sequence: u64) -> ActiveSourceFrame {
        ActiveSourceFrame {
            source_sequence,
            host_monotonic_ns: 1_000 + source_sequence,
            source_gap: 0,
        }
    }

    fn assert_error_code(error: ActiveTakeError, code: &str) {
        assert_eq!(error.code, code);
    }

    #[test]
    fn source_gap_is_rejected_without_consuming_frame_domain() {
        let mut writer = ActiveTakeWriter::new("session").unwrap();
        let error = writer
            .reserve_frame(ActiveSourceFrame {
                source_sequence: 7,
                host_monotonic_ns: 1_234,
                source_gap: 2,
            })
            .unwrap_err();
        assert_error_code(error, "source_sequence_gap");

        assert_eq!(
            writer.snapshot(),
            ActiveTakeSnapshot {
                session_id: "session".to_owned(),
                frame_domain: 0,
                frames_written: 0,
                bytes_written: 0,
                dropped_frames: 0,
                pending_frames: 0,
                drop_events: Vec::new(),
            }
        );
    }

    #[test]
    fn overlapping_reservation_preserves_the_pending_frame_and_sequence() {
        let mut writer = ActiveTakeWriter::new("session").unwrap();
        let first = writer.reserve_frame(source_frame(10)).unwrap();
        assert_eq!(first.record_sequence, 0);
        let before = writer.snapshot();
        assert_error_code(
            writer.reserve_frame(source_frame(11)).unwrap_err(),
            "invalid_state",
        );
        assert_eq!(writer.snapshot(), before);

        writer.finish_frame(first, 64).unwrap();
        let second = writer.reserve_frame(source_frame(11)).unwrap();
        assert_eq!(second.record_sequence, 1);
        writer.finish_frame(second, 32).unwrap();
        let summary = writer.finish().unwrap();
        assert_eq!(summary.frame_domain, 2);
        assert_eq!(summary.frames_written, 2);
        assert_eq!(summary.bytes_written, 96);
        assert_eq!(summary.pending_frames, 0);
    }

    #[test]
    fn frame_sequence_allocation_tracks_completed_frames() {
        let mut writer = ActiveTakeWriter::new("session").unwrap();

        let first = writer.reserve_frame(source_frame(20)).unwrap();
        assert_eq!(first.record_sequence, 0);
        writer.finish_frame(first, 100).unwrap();
        let snapshot = writer.snapshot();
        assert_eq!(snapshot.frames_written, 1);
        assert_eq!(snapshot.bytes_written, 100);

        let second = writer.reserve_frame(source_frame(21)).unwrap();
        assert_eq!(second.record_sequence, 1);
        writer.finish_frame(second, 24).unwrap();
        let snapshot = writer.snapshot();

        assert_eq!(snapshot.frame_domain, 2);
        assert_eq!(snapshot.frames_written, 2);
        assert_eq!(snapshot.bytes_written, 124);
        assert_eq!(snapshot.dropped_frames, 0);
    }

    #[test]
    fn finish_summary_requires_all_reserved_frames_to_be_settled() {
        let mut writer = ActiveTakeWriter::new("session").unwrap();
        let written = writer.reserve_frame(source_frame(30)).unwrap();
        writer.finish_frame(written, 64).unwrap();
        let pending = writer.reserve_frame(source_frame(31)).unwrap();

        let error = writer.finish().unwrap_err();
        assert_error_code(error, "invalid_state");

        writer.finish_frame(pending, 32).unwrap();
        let summary = writer.finish().unwrap();
        assert_eq!(summary.session_id, "session");
        assert_eq!(summary.frame_domain, 2);
        assert_eq!(summary.frames_written, 2);
        assert_eq!(summary.bytes_written, 96);
        assert_eq!(summary.dropped_frames, 0);
        assert_eq!(summary.pending_frames, 0);
        assert!(summary.drop_events.is_empty());

        let error = writer.reserve_frame(source_frame(32)).unwrap_err();
        assert_error_code(error, "invalid_state");
    }

    #[test]
    fn invalid_and_duplicate_completions_preserve_progress() {
        let mut writer = ActiveTakeWriter::new("session").unwrap();
        let reserved = writer.reserve_frame(source_frame(10)).unwrap();
        let before = writer.snapshot();
        let mut foreign = reserved.clone();
        foreign.session_id = "another-session".to_owned();
        assert_error_code(
            writer.finish_frame(foreign, 64).unwrap_err(),
            "invalid_session",
        );
        let mut unknown = reserved.clone();
        unknown.record_sequence += 1;
        assert_error_code(
            writer.finish_frame(unknown, 64).unwrap_err(),
            "invalid_state",
        );
        assert_eq!(writer.snapshot(), before);

        writer.finish_frame(reserved.clone(), 64).unwrap();
        let completed = writer.snapshot();
        assert_error_code(
            writer.finish_frame(reserved, 64).unwrap_err(),
            "invalid_state",
        );
        assert_eq!(writer.snapshot(), completed);
    }

    #[test]
    fn accounting_overflow_keeps_the_failed_frame_pending() {
        let mut writer = ActiveTakeWriter::new("session").unwrap();
        writer.bytes_written = u64::MAX;
        let pending = writer.reserve_frame(source_frame(10)).unwrap();
        let before = writer.snapshot();
        assert_error_code(
            writer.finish_frame(pending, 1).unwrap_err(),
            "counter_overflow",
        );
        assert_eq!(writer.snapshot(), before);
        assert_error_code(writer.finish().unwrap_err(), "invalid_state");
    }

    #[test]
    fn recovered_drops_record_dense_drop_events_and_total_count() {
        let mut writer = ActiveTakeWriter::new("session").unwrap();
        let f0 = writer.reserve_frame(source_frame(0)).unwrap();
        writer.finish_frame(f0, 50).unwrap();

        writer.record_recovered_drops(0, 0.05).unwrap();
        assert_eq!(writer.snapshot().dropped_frames, 0);
        assert!(writer.snapshot().drop_events.is_empty());

        writer.record_recovered_drops(2, 0.125).unwrap();
        let f1 = writer.reserve_frame(source_frame(3)).unwrap();
        writer.finish_frame(f1, 60).unwrap();

        let summary = writer.finish().unwrap();
        assert_eq!(summary.frames_written, 2);
        assert_eq!(summary.frame_domain, 2);
        assert_eq!(summary.dropped_frames, 2);
        assert_eq!(summary.drop_events.len(), 1);
        assert_eq!(summary.drop_events[0].start_frame, 1);
        assert_eq!(summary.drop_events[0].end_frame, 3);
        assert_eq!(summary.drop_events[0].dropped, 2);
        assert_eq!(summary.drop_events[0].reason, "write_backpressure");
        assert!((summary.drop_events[0].at_time_seconds - 0.125).abs() < 1e-9);
    }
}
