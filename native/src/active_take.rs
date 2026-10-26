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
    pending_frame: Option<u64>,
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
            pending_frame: None,
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

    pub(crate) fn finish_frame(
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

    pub(crate) fn finish(&mut self) -> Result<ActiveTakeSummary, ActiveTakeError> {
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

    pub(crate) fn snapshot(&self) -> ActiveTakeSnapshot {
        ActiveTakeSnapshot {
            session_id: self.session_id.clone(),
            frame_domain: self.frame_domain,
            frames_written: self.frames_written,
            bytes_written: self.bytes_written,
            // Preserve the exported snapshot shape. Native recording fails on
            // source/queue gaps instead of accepting dropped record sequences;
            // FrameValidator, Metrics and the encoder retain the loss evidence.
            dropped_frames: 0,
            pending_frames: u64::from(self.pending_frame.is_some()),
            drop_events: Vec::new(),
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
}
