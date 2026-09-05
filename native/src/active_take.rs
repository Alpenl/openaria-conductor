use std::collections::BTreeSet;

const WRITE_BACKPRESSURE: &str = "write_backpressure";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActiveTakeError {
    pub(crate) code: &'static str,
    pub(crate) message: String,
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
pub(crate) struct ActiveSourceFrame {
    pub(crate) source_sequence: u64,
    pub(crate) host_monotonic_ns: u64,
    pub(crate) source_gap: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReservedFrame {
    pub(crate) session_id: String,
    pub(crate) record_sequence: u64,
    pub(crate) source_sequence: u64,
    pub(crate) host_monotonic_ns: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ActiveDropEvent {
    pub(crate) start_frame: u64,
    pub(crate) end_frame: u64,
    pub(crate) at_time_seconds: f64,
    pub(crate) reason: &'static str,
    pub(crate) dropped: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ActiveTakeSnapshot {
    pub(crate) session_id: String,
    pub(crate) frame_domain: u64,
    pub(crate) frames_written: u64,
    pub(crate) bytes_written: u64,
    pub(crate) dropped_frames: u64,
    pub(crate) pending_frames: u64,
    pub(crate) drop_events: Vec<ActiveDropEvent>,
}

pub(crate) type ActiveTakeSummary = ActiveTakeSnapshot;

pub(crate) struct ActiveTakeWriter {
    session_id: String,
    frame_domain: u64,
    frames_written: u64,
    bytes_written: u64,
    drop_events: Vec<ActiveDropEvent>,
    pending_frames: BTreeSet<u64>,
    closed: bool,
}

impl ActiveTakeWriter {
    pub(crate) fn new(session_id: &str) -> Result<Self, ActiveTakeError> {
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
            drop_events: Vec::new(),
            pending_frames: BTreeSet::new(),
            closed: false,
        })
    }

    pub(crate) fn reserve_frame(
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
        let record_sequence = self.frame_domain;
        let next_frame_domain = self.frame_domain.checked_add(1).ok_or_else(|| {
            ActiveTakeError::new("counter_overflow", "active take frame domain overflow")
        })?;
        self.frame_domain = next_frame_domain;
        self.pending_frames.insert(record_sequence);
        Ok(ReservedFrame {
            session_id: self.session_id.clone(),
            record_sequence,
            source_sequence: source.source_sequence,
            host_monotonic_ns: source.host_monotonic_ns,
        })
    }

    pub(crate) fn finish_frame(
        &mut self,
        frame: ReservedFrame,
        bytes_written: u64,
    ) -> Result<ActiveTakeSnapshot, ActiveTakeError> {
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
        self.pending_frames.remove(&frame.record_sequence);
        self.frames_written = next_frames_written;
        self.bytes_written = next_bytes_written;
        Ok(self.snapshot())
    }

    pub(crate) fn reject_frame(
        &mut self,
        frame: ReservedFrame,
        at_time_seconds: f64,
    ) -> Result<ActiveTakeSnapshot, ActiveTakeError> {
        self.ensure_pending(&frame)?;
        validate_elapsed(at_time_seconds, "at_time_seconds")?;
        let end_frame = frame.record_sequence.checked_add(1).ok_or_else(|| {
            ActiveTakeError::new("counter_overflow", "active take drop frame overflow")
        })?;
        self.pending_frames.remove(&frame.record_sequence);
        self.record_drop(frame.record_sequence, end_frame, at_time_seconds)?;
        Ok(self.snapshot())
    }

    pub(crate) fn finish(&mut self) -> Result<ActiveTakeSummary, ActiveTakeError> {
        self.ensure_open()?;
        if !self.pending_frames.is_empty() {
            return Err(ActiveTakeError::new(
                "invalid_state",
                "active take has pending frames",
            ));
        }
        self.closed = true;
        Ok(self.snapshot())
    }

    pub(crate) fn snapshot(&self) -> ActiveTakeSnapshot {
        ActiveTakeSnapshot {
            session_id: self.session_id.clone(),
            frame_domain: self.frame_domain,
            frames_written: self.frames_written,
            bytes_written: self.bytes_written,
            dropped_frames: self.dropped_frames(),
            pending_frames: u64::try_from(self.pending_frames.len()).unwrap_or(u64::MAX),
            drop_events: self.drop_events.clone(),
        }
    }

    fn record_drop(
        &mut self,
        start_frame: u64,
        end_frame: u64,
        at_time_seconds: f64,
    ) -> Result<(), ActiveTakeError> {
        if end_frame <= start_frame {
            return Ok(());
        }
        let dropped = end_frame
            .checked_sub(start_frame)
            .ok_or_else(|| ActiveTakeError::new("counter_overflow", "active take drop overflow"))?;
        if let Some(previous) = self.drop_events.last_mut() {
            if previous.reason == WRITE_BACKPRESSURE && previous.end_frame == start_frame {
                previous.end_frame = end_frame;
                previous.dropped = previous.dropped.checked_add(dropped).ok_or_else(|| {
                    ActiveTakeError::new("counter_overflow", "active take drop count overflow")
                })?;
                return Ok(());
            }
        }
        self.drop_events.push(ActiveDropEvent {
            start_frame,
            end_frame,
            at_time_seconds,
            reason: WRITE_BACKPRESSURE,
            dropped,
        });
        Ok(())
    }

    fn dropped_frames(&self) -> u64 {
        self.drop_events
            .iter()
            .fold(0, |total, event| total.saturating_add(event.dropped))
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
        if !self.pending_frames.contains(&frame.record_sequence) {
            return Err(ActiveTakeError::new(
                "invalid_state",
                "reserved frame is not pending",
            ));
        }
        Ok(())
    }
}

fn validate_elapsed(value: f64, name: &str) -> Result<(), ActiveTakeError> {
    if value.is_finite() && value >= 0.0 {
        Ok(())
    } else {
        Err(ActiveTakeError::new(
            "invalid_argument",
            format!("{name} must be finite and non-negative"),
        ))
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
    fn queue_rejections_consume_record_sequences_and_merge_drop_events() {
        let mut writer = ActiveTakeWriter::new("session").unwrap();
        let first = writer.reserve_frame(source_frame(10)).unwrap();
        assert_eq!(first.record_sequence, 0);
        let snapshot = writer.reject_frame(first, 0.25).unwrap();
        assert_eq!(snapshot.frame_domain, 1);
        assert_eq!(snapshot.dropped_frames, 1);

        let second = writer.reserve_frame(source_frame(11)).unwrap();
        assert_eq!(second.record_sequence, 1);
        let snapshot = writer.reject_frame(second, 0.50).unwrap();

        assert_eq!(snapshot.frame_domain, 2);
        assert_eq!(snapshot.frames_written, 0);
        assert_eq!(snapshot.pending_frames, 0);
        assert_eq!(snapshot.dropped_frames, 2);
        assert_eq!(snapshot.drop_events.len(), 1);
        let event = &snapshot.drop_events[0];
        assert_eq!(event.start_frame, 0);
        assert_eq!(event.end_frame, 2);
        assert_eq!(event.at_time_seconds, 0.25);
        assert_eq!(event.reason, "write_backpressure");
        assert_eq!(event.dropped, 2);
    }

    #[test]
    fn frame_sequence_allocation_tracks_completed_frames() {
        let mut writer = ActiveTakeWriter::new("session").unwrap();

        let first = writer.reserve_frame(source_frame(20)).unwrap();
        assert_eq!(first.record_sequence, 0);
        let snapshot = writer.finish_frame(first, 100).unwrap();
        assert_eq!(snapshot.frames_written, 1);
        assert_eq!(snapshot.bytes_written, 100);

        let second = writer.reserve_frame(source_frame(21)).unwrap();
        assert_eq!(second.record_sequence, 1);
        let snapshot = writer.finish_frame(second, 24).unwrap();

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
        let rejected = writer.reserve_frame(source_frame(31)).unwrap();

        let error = writer.finish().unwrap_err();
        assert_error_code(error, "invalid_state");

        writer.reject_frame(rejected, 1.0).unwrap();
        let summary = writer.finish().unwrap();
        assert_eq!(summary.session_id, "session");
        assert_eq!(summary.frame_domain, 2);
        assert_eq!(summary.frames_written, 1);
        assert_eq!(summary.bytes_written, 64);
        assert_eq!(summary.dropped_frames, 1);
        assert_eq!(summary.pending_frames, 0);
        assert_eq!(summary.drop_events.len(), 1);
        assert_eq!(summary.drop_events[0].start_frame, 1);
        assert_eq!(summary.drop_events[0].end_frame, 2);

        let error = writer.reserve_frame(source_frame(32)).unwrap_err();
        assert_error_code(error, "invalid_state");
    }
}
