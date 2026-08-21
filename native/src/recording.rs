use crate::imu;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordingError {
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

impl RecordingError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArtifactIdentity {
    pub(crate) device: u64,
    pub(crate) inode: u64,
    pub(crate) size: u64,
    pub(crate) mtime_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArtifactSnapshot {
    pub(crate) role: &'static str,
    pub(crate) relative_path: &'static str,
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
    pub(crate) identity: ArtifactIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordingSinkSnapshot {
    pub(crate) artifacts: Vec<ArtifactSnapshot>,
    pub(crate) bytes_written: u64,
    pub(crate) frames_written: u64,
    pub(crate) imu_samples_written: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FrameGateDecision {
    pub(crate) record: bool,
    pub(crate) dropped_before: u64,
    pub(crate) observed_frames: u64,
    pub(crate) inflight_frames: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FrameGateSnapshot {
    pub(crate) frame_decimation: u64,
    pub(crate) first_frame: bool,
    pub(crate) observed_frames: u64,
    pub(crate) inflight_frames: u64,
    pub(crate) stopping: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordingTapSnapshot {
    pub(crate) frame_decimation: u64,
    pub(crate) first_frame: bool,
    pub(crate) observed_frames: u64,
    pub(crate) inflight_frames: u64,
    pub(crate) stopping: bool,
    pub(crate) failure_reported: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CaptureFanoutDecision {
    pub(crate) publish_preview: bool,
    pub(crate) record: bool,
    pub(crate) dropped_before: u64,
    pub(crate) observed_frames: u64,
    pub(crate) inflight_frames: u64,
    pub(crate) recording_active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CaptureFanoutSnapshot {
    pub(crate) frame_decimation: u64,
    pub(crate) recording_present: bool,
    pub(crate) recording_active: bool,
    pub(crate) first_frame: bool,
    pub(crate) observed_frames: u64,
    pub(crate) inflight_frames: u64,
    pub(crate) stopping: bool,
    pub(crate) failure_reported: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SegmentFramePlan {
    pub(crate) ordinal: u64,
    pub(crate) segment_index: u64,
    pub(crate) segment_frame: u64,
    pub(crate) frames_written: u64,
    pub(crate) boundary_recorded: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SegmentBoundary {
    pub(crate) frame: u64,
    pub(crate) time_seconds: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentPlannerSnapshot {
    pub(crate) segment_frames: u64,
    pub(crate) frames_written: u64,
    pub(crate) segment_count: u64,
    pub(crate) covered_frames: u64,
    pub(crate) boundary_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct DropEvent {
    pub(crate) at_time_seconds: f64,
    pub(crate) dropped: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct DropQualityPolicy {
    pub(crate) max_contiguous_dropped_frames: u64,
    pub(crate) max_total_dropped_frames: u64,
    pub(crate) max_drop_fraction: f64,
    pub(crate) window_seconds: f64,
    pub(crate) max_dropped_frames_per_window: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DropQualityEvaluation {
    pub(crate) accepted: bool,
    pub(crate) dropped: u64,
    pub(crate) total: u64,
    pub(crate) fraction: f64,
    pub(crate) contiguous: u64,
    pub(crate) window_drops: u64,
    pub(crate) violations: Vec<&'static str>,
}

pub(crate) struct RecordingFrameGate {
    frame_decimation: u64,
    first_frame: bool,
    observed_frames: u64,
    inflight_frames: u64,
    stopping: bool,
}

impl RecordingFrameGate {
    pub(crate) fn new(frame_decimation: u64) -> Result<Self, RecordingError> {
        if frame_decimation == 0 {
            return Err(RecordingError::new(
                "invalid_argument",
                "frame_decimation must be greater than zero",
            ));
        }
        Ok(Self {
            frame_decimation,
            first_frame: true,
            observed_frames: 0,
            inflight_frames: 0,
            stopping: false,
        })
    }

    pub(crate) fn begin_frame(
        &mut self,
        dropped_before: u64,
    ) -> Result<FrameGateDecision, RecordingError> {
        if self.stopping {
            return Ok(self.decision(false, dropped_before));
        }
        if self.first_frame {
            self.first_frame = false;
            self.observed_frames = 1;
            self.inflight_frames = self.inflight_frames.checked_add(1).ok_or_else(|| {
                RecordingError::new("counter_overflow", "recording frame gate inflight overflow")
            })?;
            // A source gap before the first recorded frame belongs to pre-recording
            // warmup/preview and must not poison the new take.
            return Ok(self.decision(true, 0));
        }

        let observed_index = self.observed_frames;
        self.observed_frames = self
            .observed_frames
            .checked_add(dropped_before)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| {
                RecordingError::new("counter_overflow", "recording frame gate observed overflow")
            })?;
        if dropped_before != 0 {
            self.inflight_frames = self.inflight_frames.checked_add(1).ok_or_else(|| {
                RecordingError::new("counter_overflow", "recording frame gate inflight overflow")
            })?;
            return Ok(self.decision(true, dropped_before));
        }
        if observed_index % self.frame_decimation != 0 {
            return Ok(self.decision(false, dropped_before));
        }
        self.inflight_frames = self.inflight_frames.checked_add(1).ok_or_else(|| {
            RecordingError::new("counter_overflow", "recording frame gate inflight overflow")
        })?;
        Ok(self.decision(true, dropped_before))
    }

    pub(crate) fn finish_frame(&mut self) -> Result<u64, RecordingError> {
        if self.inflight_frames == 0 {
            return Err(RecordingError::new(
                "invalid_state",
                "recording frame gate has no inflight frame to finish",
            ));
        }
        self.inflight_frames -= 1;
        Ok(self.inflight_frames)
    }

    pub(crate) fn start_stopping(&mut self) -> u64 {
        self.stopping = true;
        self.inflight_frames
    }

    pub(crate) fn snapshot(&self) -> FrameGateSnapshot {
        FrameGateSnapshot {
            frame_decimation: self.frame_decimation,
            first_frame: self.first_frame,
            observed_frames: self.observed_frames,
            inflight_frames: self.inflight_frames,
            stopping: self.stopping,
        }
    }

    fn decision(&self, record: bool, dropped_before: u64) -> FrameGateDecision {
        FrameGateDecision {
            record,
            dropped_before,
            observed_frames: self.observed_frames,
            inflight_frames: self.inflight_frames,
        }
    }
}

pub(crate) struct RecordingTapState {
    gate: RecordingFrameGate,
    failure_reported: bool,
}

impl RecordingTapState {
    pub(crate) fn new(frame_decimation: u64) -> Result<Self, RecordingError> {
        Ok(Self {
            gate: RecordingFrameGate::new(frame_decimation)?,
            failure_reported: false,
        })
    }

    pub(crate) fn begin_frame(
        &mut self,
        dropped_before: u64,
    ) -> Result<FrameGateDecision, RecordingError> {
        self.gate.begin_frame(dropped_before)
    }

    pub(crate) fn finish_frame(&mut self) -> Result<u64, RecordingError> {
        self.gate.finish_frame()
    }

    pub(crate) fn start_stopping(&mut self) -> u64 {
        self.gate.start_stopping()
    }

    pub(crate) fn mark_failure(&mut self) -> (bool, u64) {
        let first_report = !self.failure_reported;
        self.failure_reported = true;
        let inflight = self.gate.start_stopping();
        (first_report, inflight)
    }

    pub(crate) fn snapshot(&self) -> RecordingTapSnapshot {
        let gate = self.gate.snapshot();
        RecordingTapSnapshot {
            frame_decimation: gate.frame_decimation,
            first_frame: gate.first_frame,
            observed_frames: gate.observed_frames,
            inflight_frames: gate.inflight_frames,
            stopping: gate.stopping,
            failure_reported: self.failure_reported,
        }
    }
}

pub(crate) struct CaptureFanoutState {
    frame_decimation: u64,
    recording: Option<RecordingTapState>,
}

impl CaptureFanoutState {
    pub(crate) fn new(frame_decimation: u64) -> Result<Self, RecordingError> {
        if frame_decimation == 0 {
            return Err(RecordingError::new(
                "invalid_argument",
                "frame_decimation must be greater than zero",
            ));
        }
        Ok(Self {
            frame_decimation,
            recording: None,
        })
    }

    pub(crate) fn start_recording(&mut self) -> Result<CaptureFanoutSnapshot, RecordingError> {
        if let Some(existing) = &self.recording {
            let snapshot = existing.snapshot();
            if snapshot.inflight_frames != 0 || !snapshot.stopping {
                return Err(RecordingError::new(
                    "invalid_state",
                    "capture fanout is already recording",
                ));
            }
        }
        self.recording = Some(RecordingTapState::new(self.frame_decimation)?);
        Ok(self.snapshot())
    }

    pub(crate) fn begin_frame(
        &mut self,
        dropped_before: u64,
        has_preview: bool,
    ) -> Result<CaptureFanoutDecision, RecordingError> {
        let Some(recording) = self.recording.as_mut() else {
            return Ok(CaptureFanoutDecision {
                publish_preview: has_preview,
                record: false,
                dropped_before,
                observed_frames: 0,
                inflight_frames: 0,
                recording_active: false,
            });
        };
        let decision = recording.begin_frame(dropped_before)?;
        let snapshot = recording.snapshot();
        Ok(CaptureFanoutDecision {
            publish_preview: has_preview,
            record: decision.record,
            dropped_before: decision.dropped_before,
            observed_frames: decision.observed_frames,
            inflight_frames: decision.inflight_frames,
            recording_active: !snapshot.stopping,
        })
    }

    pub(crate) fn finish_frame(&mut self) -> Result<u64, RecordingError> {
        let Some(recording) = self.recording.as_mut() else {
            return Err(RecordingError::new(
                "invalid_state",
                "capture fanout has no recording frame to finish",
            ));
        };
        let inflight = recording.finish_frame()?;
        if inflight == 0 && recording.snapshot().stopping {
            self.recording = None;
        }
        Ok(inflight)
    }

    pub(crate) fn start_stopping(&mut self) -> u64 {
        let Some(recording) = self.recording.as_mut() else {
            return 0;
        };
        let inflight = recording.start_stopping();
        if inflight == 0 {
            self.recording = None;
        }
        inflight
    }

    pub(crate) fn mark_failure(&mut self) -> (bool, u64) {
        let Some(recording) = self.recording.as_mut() else {
            return (false, 0);
        };
        let result = recording.mark_failure();
        if result.1 == 0 {
            self.recording = None;
        }
        result
    }

    pub(crate) fn snapshot(&self) -> CaptureFanoutSnapshot {
        let Some(recording) = &self.recording else {
            return CaptureFanoutSnapshot {
                frame_decimation: self.frame_decimation,
                recording_present: false,
                recording_active: false,
                first_frame: true,
                observed_frames: 0,
                inflight_frames: 0,
                stopping: false,
                failure_reported: false,
            };
        };
        let tap = recording.snapshot();
        CaptureFanoutSnapshot {
            frame_decimation: self.frame_decimation,
            recording_present: true,
            recording_active: !tap.stopping,
            first_frame: tap.first_frame,
            observed_frames: tap.observed_frames,
            inflight_frames: tap.inflight_frames,
            stopping: tap.stopping,
            failure_reported: tap.failure_reported,
        }
    }
}

pub(crate) struct RecordingSegmentPlanner {
    segment_frames: u64,
    frames_written: u64,
    segment_count: u64,
    covered_frames: u64,
    boundary_record_sequence: BTreeMap<u64, u64>,
    boundary_elapsed: BTreeMap<u64, f64>,
}

impl RecordingSegmentPlanner {
    pub(crate) fn new(segment_frames: u64) -> Result<Self, RecordingError> {
        if segment_frames == 0 {
            return Err(RecordingError::new(
                "invalid_argument",
                "segment_frames must be greater than zero",
            ));
        }
        Ok(Self {
            segment_frames,
            frames_written: 0,
            segment_count: 0,
            covered_frames: 0,
            boundary_record_sequence: BTreeMap::new(),
            boundary_elapsed: BTreeMap::new(),
        })
    }

    pub(crate) fn next_frame(
        &mut self,
        record_sequence: u64,
        elapsed_seconds: f64,
    ) -> Result<SegmentFramePlan, RecordingError> {
        validate_non_negative_finite(elapsed_seconds, "elapsed_seconds")?;
        let ordinal = self.frames_written;
        let boundary_recorded = if ordinal % self.segment_frames == 0 {
            let already_present = self.boundary_record_sequence.contains_key(&ordinal);
            self.boundary_record_sequence
                .entry(ordinal)
                .or_insert(record_sequence);
            self.boundary_elapsed
                .entry(ordinal)
                .or_insert(elapsed_seconds);
            !already_present
        } else {
            false
        };
        let segment_index = ordinal / self.segment_frames;
        let segment_frame = ordinal % self.segment_frames;
        self.frames_written = self.frames_written.checked_add(1).ok_or_else(|| {
            RecordingError::new(
                "counter_overflow",
                "recording segment planner frame overflow",
            )
        })?;
        Ok(SegmentFramePlan {
            ordinal,
            segment_index,
            segment_frame,
            frames_written: self.frames_written,
            boundary_recorded,
        })
    }

    pub(crate) fn register_segment(
        &mut self,
        index: u64,
        start_ordinal: u64,
        end_ordinal: u64,
    ) -> Result<SegmentPlannerSnapshot, RecordingError> {
        if end_ordinal <= start_ordinal {
            return Err(RecordingError::new(
                "segment_invalid",
                "segment ordinal domain is empty or reversed",
            ));
        }
        if index != self.segment_count {
            return Err(RecordingError::new(
                "segment_invalid",
                format!(
                    "segment index {index} is not contiguous after {}",
                    self.segment_count
                ),
            ));
        }
        if start_ordinal != self.covered_frames {
            return Err(RecordingError::new(
                "segment_invalid",
                format!(
                    "segment starts at {start_ordinal}, expected {}",
                    self.covered_frames
                ),
            ));
        }
        self.covered_frames = end_ordinal;
        self.segment_count = self.segment_count.checked_add(1).ok_or_else(|| {
            RecordingError::new("counter_overflow", "recording segment count overflow")
        })?;
        Ok(self.snapshot())
    }

    pub(crate) fn finish(
        &mut self,
        submitted_frames: u64,
        frame_domain: u64,
        duration_seconds: f64,
    ) -> Result<SegmentPlannerSnapshot, RecordingError> {
        validate_non_negative_finite(duration_seconds, "duration_seconds")?;
        if submitted_frames != self.frames_written {
            return Err(RecordingError::new(
                "segment_invalid",
                format!(
                    "encoder received {submitted_frames} frames, planner wrote {}",
                    self.frames_written
                ),
            ));
        }
        self.boundary_record_sequence
            .entry(self.frames_written)
            .or_insert(frame_domain);
        self.boundary_elapsed
            .entry(self.frames_written)
            .or_insert(duration_seconds);
        if self.covered_frames != self.frames_written {
            return Err(RecordingError::new(
                "segment_invalid",
                format!(
                    "segments cover {} frames, planner wrote {} frames",
                    self.covered_frames, self.frames_written
                ),
            ));
        }
        Ok(self.snapshot())
    }

    pub(crate) fn boundary(
        &self,
        ordinal: u64,
        duration_seconds: f64,
    ) -> Result<SegmentBoundary, RecordingError> {
        validate_non_negative_finite(duration_seconds, "duration_seconds")?;
        let frame = *self
            .boundary_record_sequence
            .get(&ordinal)
            .ok_or_else(|| RecordingError::new("segment_invalid", "missing segment boundary"))?;
        let elapsed = *self
            .boundary_elapsed
            .get(&ordinal)
            .ok_or_else(|| RecordingError::new("segment_invalid", "missing segment boundary"))?;
        Ok(SegmentBoundary {
            frame,
            time_seconds: elapsed.min(duration_seconds),
        })
    }

    pub(crate) fn snapshot(&self) -> SegmentPlannerSnapshot {
        SegmentPlannerSnapshot {
            segment_frames: self.segment_frames,
            frames_written: self.frames_written,
            segment_count: self.segment_count,
            covered_frames: self.covered_frames,
            boundary_count: u64::try_from(self.boundary_record_sequence.len()).unwrap_or(u64::MAX),
        }
    }
}

fn validate_non_negative_finite(value: f64, name: &str) -> Result<(), RecordingError> {
    if value.is_finite() && value >= 0.0 {
        Ok(())
    } else {
        Err(RecordingError::new(
            "segment_invalid",
            format!("{name} must be finite and non-negative"),
        ))
    }
}

pub(crate) fn evaluate_drop_quality(
    frames_written: u64,
    events: &[DropEvent],
    policy: DropQualityPolicy,
) -> Result<DropQualityEvaluation, RecordingError> {
    if !policy.max_drop_fraction.is_finite()
        || policy.max_drop_fraction < 0.0
        || !policy.window_seconds.is_finite()
        || policy.window_seconds < 0.0
    {
        return Err(RecordingError::new(
            "invalid_argument",
            "drop quality policy limits are invalid",
        ));
    }
    let mut dropped = 0_u64;
    let mut contiguous = 0_u64;
    for event in events {
        if !event.at_time_seconds.is_finite() || event.at_time_seconds < 0.0 {
            return Err(RecordingError::new(
                "invalid_argument",
                "drop event time is invalid",
            ));
        }
        dropped = dropped
            .checked_add(event.dropped)
            .ok_or_else(|| RecordingError::new("counter_overflow", "drop count overflow"))?;
        contiguous = contiguous.max(event.dropped);
    }
    let total = frames_written
        .checked_add(dropped)
        .ok_or_else(|| RecordingError::new("counter_overflow", "frame count overflow"))?;
    let fraction = if total == 0 {
        0.0
    } else {
        dropped as f64 / total as f64
    };
    let mut window_drops = 0_u64;
    for start in events {
        let mut current = 0_u64;
        let window_end = start.at_time_seconds + policy.window_seconds;
        for event in events {
            if start.at_time_seconds <= event.at_time_seconds && event.at_time_seconds < window_end
            {
                current = current.checked_add(event.dropped).ok_or_else(|| {
                    RecordingError::new("counter_overflow", "window drop count overflow")
                })?;
            }
        }
        window_drops = window_drops.max(current);
    }
    let mut violations = Vec::new();
    if contiguous > policy.max_contiguous_dropped_frames {
        violations.push("contiguous");
    }
    if dropped > policy.max_total_dropped_frames {
        violations.push("total");
    }
    if fraction > policy.max_drop_fraction {
        violations.push("fraction");
    }
    if window_drops > policy.max_dropped_frames_per_window {
        violations.push("window");
    }
    Ok(DropQualityEvaluation {
        accepted: violations.is_empty(),
        dropped,
        total,
        fraction,
        contiguous,
        window_drops,
        violations,
    })
}

pub(crate) struct RecordingSink {
    session_id: String,
    split_eyes: bool,
    frames: ArtifactWriter,
    imu: ArtifactWriter,
    raw_video: Option<ArtifactWriter>,
    bytes_written: u64,
    frames_written: u64,
    imu_samples_written: u64,
    closed: bool,
}

impl RecordingSink {
    pub(crate) fn create(
        session_root: &Path,
        session_id: &str,
        split_eyes: bool,
    ) -> Result<Self, RecordingError> {
        if session_id.is_empty() {
            return Err(RecordingError::new(
                "invalid_argument",
                "session_id must not be empty",
            ));
        }
        let frames = ArtifactWriter::create(session_root, "frames.index", "frames.ndjson")?;
        let imu = ArtifactWriter::create(session_root, "imu.samples", "imu.ndjson")?;
        let raw_video = if split_eyes {
            None
        } else {
            Some(ArtifactWriter::create(
                session_root,
                "video.raw-side-by-side",
                "video/raw-sbs.mjpeg",
            )?)
        };
        Ok(Self {
            session_id: session_id.to_owned(),
            split_eyes,
            frames,
            imu,
            raw_video,
            bytes_written: 0,
            frames_written: 0,
            imu_samples_written: 0,
            closed: false,
        })
    }

    pub(crate) fn write_split_frame_index(
        &mut self,
        frame: u64,
        source_sequence: u64,
        host_monotonic_ns: u64,
        segment_index: u64,
        segment_frame: u64,
    ) -> Result<u64, RecordingError> {
        self.ensure_open()?;
        if !self.split_eyes {
            return Err(RecordingError::new(
                "invalid_state",
                "split frame index is only valid for split-eye recording",
            ));
        }
        let record = split_frame_index_record(
            &self.session_id,
            frame,
            source_sequence,
            host_monotonic_ns,
            segment_index,
            segment_frame,
        );
        let written = self.frames.write(&record)?;
        self.bytes_written = self
            .bytes_written
            .checked_add(written)
            .ok_or_else(|| RecordingError::new("write_failed", "recording byte count overflow"))?;
        self.frames_written = self
            .frames_written
            .checked_add(1)
            .ok_or_else(|| RecordingError::new("write_failed", "frame count overflow"))?;
        Ok(written)
    }

    pub(crate) fn write_raw_frame(
        &mut self,
        frame: u64,
        source_sequence: u64,
        host_monotonic_ns: u64,
        raw_side_by_side: &[u8],
    ) -> Result<RawFrameWrite, RecordingError> {
        self.ensure_open()?;
        if self.split_eyes {
            return Err(RecordingError::new(
                "invalid_state",
                "raw frame writes are only valid for raw-side-by-side recording",
            ));
        }
        let payload = jpeg_payload(raw_side_by_side)?;
        let video = self
            .raw_video
            .as_mut()
            .ok_or_else(|| RecordingError::new("invalid_state", "raw video writer is missing"))?;
        let video_offset = video.bytes;
        let video_bytes = u64::try_from(payload.len())
            .map_err(|_| RecordingError::new("write_failed", "frame too large"))?;
        let written_video = video.write(payload)?;
        let record = raw_frame_index_record(
            &self.session_id,
            frame,
            source_sequence,
            host_monotonic_ns,
            video_offset,
            video_bytes,
        );
        let written_index = self.frames.write(&record)?;
        let written = written_video
            .checked_add(written_index)
            .ok_or_else(|| RecordingError::new("write_failed", "recording byte count overflow"))?;
        self.bytes_written = self
            .bytes_written
            .checked_add(written)
            .ok_or_else(|| RecordingError::new("write_failed", "recording byte count overflow"))?;
        self.frames_written = self
            .frames_written
            .checked_add(1)
            .ok_or_else(|| RecordingError::new("write_failed", "frame count overflow"))?;
        Ok(RawFrameWrite {
            bytes_written: written,
            video_offset,
            video_bytes,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_imu_sample(
        &mut self,
        sequence: u64,
        packet_sequence: u64,
        sample_index: u8,
        device_timestamp_raw: u32,
        device_ticks: u64,
        host_read_start_ns: u64,
        host_read_end_ns: u64,
        host_monotonic_ns: u64,
        accelerometer: (i16, i16, i16),
        gyroscope: (i16, i16, i16),
        sync_offset_ns: Option<i64>,
        sync_residual_ns: Option<u64>,
        sync_quality: &str,
    ) -> Result<u64, RecordingError> {
        self.ensure_open()?;
        let record = imu_sample_record(
            &self.session_id,
            sequence,
            packet_sequence,
            sample_index,
            device_timestamp_raw,
            device_ticks,
            host_read_start_ns,
            host_read_end_ns,
            host_monotonic_ns,
            accelerometer,
            gyroscope,
            sync_offset_ns,
            sync_residual_ns,
            sync_quality,
        );
        let written = self.imu.write(&record)?;
        self.bytes_written = self
            .bytes_written
            .checked_add(written)
            .ok_or_else(|| RecordingError::new("write_failed", "recording byte count overflow"))?;
        self.imu_samples_written = self
            .imu_samples_written
            .checked_add(1)
            .ok_or_else(|| RecordingError::new("write_failed", "IMU sample count overflow"))?;
        Ok(written)
    }

    pub(crate) fn write_imu_observation(
        &mut self,
        observation: &imu::ImuObservation,
    ) -> Result<u64, RecordingError> {
        let mut bytes_written = 0_u64;
        for sample in &observation.samples {
            let written = self.write_imu_sample(
                sample.sequence,
                sample.packet_sequence,
                sample.sample_index,
                sample.device_timestamp_raw,
                sample.device_ticks,
                sample.host_read_start_ns,
                sample.host_read_end_ns,
                sample.host_monotonic_ns,
                (
                    sample.accelerometer.x,
                    sample.accelerometer.y,
                    sample.accelerometer.z,
                ),
                (sample.gyroscope.x, sample.gyroscope.y, sample.gyroscope.z),
                sample.sync_offset_ns,
                sample.sync_residual_ns,
                sample.sync_quality,
            )?;
            bytes_written = bytes_written.checked_add(written).ok_or_else(|| {
                RecordingError::new("write_failed", "IMU observation byte count overflow")
            })?;
        }
        Ok(bytes_written)
    }

    pub(crate) fn flush_and_close(&mut self) -> Result<RecordingSinkSnapshot, RecordingError> {
        self.ensure_open()?;
        let mut artifacts = Vec::with_capacity(if self.split_eyes { 2 } else { 3 });
        artifacts.push(self.frames.flush_and_close()?);
        artifacts.push(self.imu.flush_and_close()?);
        if let Some(raw_video) = self.raw_video.as_mut() {
            artifacts.push(raw_video.flush_and_close()?);
        }
        self.closed = true;
        Ok(RecordingSinkSnapshot {
            artifacts,
            bytes_written: self.bytes_written,
            frames_written: self.frames_written,
            imu_samples_written: self.imu_samples_written,
        })
    }

    pub(crate) fn snapshot(&self) -> RecordingSinkSnapshot {
        RecordingSinkSnapshot {
            artifacts: Vec::new(),
            bytes_written: self.bytes_written,
            frames_written: self.frames_written,
            imu_samples_written: self.imu_samples_written,
        }
    }

    pub(crate) fn close(&mut self) {
        self.frames.close();
        self.imu.close();
        if let Some(raw_video) = self.raw_video.as_mut() {
            raw_video.close();
        }
        self.closed = true;
    }

    fn ensure_open(&self) -> Result<(), RecordingError> {
        if self.closed {
            Err(RecordingError::new(
                "invalid_state",
                "recording sink is already closed",
            ))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RawFrameWrite {
    pub(crate) bytes_written: u64,
    pub(crate) video_offset: u64,
    pub(crate) video_bytes: u64,
}

struct ArtifactWriter {
    role: &'static str,
    relative_path: &'static str,
    file: Option<File>,
    hasher: Sha256,
    bytes: u64,
}

impl ArtifactWriter {
    fn create(
        session_root: &Path,
        role: &'static str,
        relative_path: &'static str,
    ) -> Result<Self, RecordingError> {
        validate_relative_path(relative_path)?;
        let path = session_root.join(relative_path);
        let parent = path
            .parent()
            .ok_or_else(|| RecordingError::new("invalid_argument", "artifact parent is missing"))?;
        if !parent.is_dir() {
            return Err(RecordingError::new(
                "storage_unavailable",
                format!("artifact parent is missing: {}", parent.display()),
            ));
        }
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o640)
            .open(&path)
            .map_err(|error| {
                RecordingError::new(
                    "write_failed",
                    format!("open {} failed: {}", relative_path, error),
                )
            })?;
        Ok(Self {
            role,
            relative_path,
            file: Some(file),
            hasher: Sha256::new(),
            bytes: 0,
        })
    }

    fn write(&mut self, payload: &[u8]) -> Result<u64, RecordingError> {
        let written = u64::try_from(payload.len())
            .map_err(|_| RecordingError::new("write_failed", "payload too large"))?;
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| RecordingError::new("invalid_state", "artifact writer is closed"))?;
        file.write_all(payload).map_err(|error| {
            RecordingError::new(
                "write_failed",
                format!("write {} failed: {}", self.relative_path, error),
            )
        })?;
        self.hasher.update(payload);
        self.bytes = self
            .bytes
            .checked_add(written)
            .ok_or_else(|| RecordingError::new("write_failed", "artifact byte count overflow"))?;
        Ok(written)
    }

    fn flush_and_close(&mut self) -> Result<ArtifactSnapshot, RecordingError> {
        let mut file = self
            .file
            .take()
            .ok_or_else(|| RecordingError::new("invalid_state", "artifact writer is closed"))?;
        file.flush().map_err(|error| {
            RecordingError::new(
                "write_failed",
                format!("flush {} failed: {}", self.relative_path, error),
            )
        })?;
        let offset = file.stream_position().map_err(|error| {
            RecordingError::new(
                "write_failed",
                format!("seek {} failed: {}", self.relative_path, error),
            )
        })?;
        if offset != self.bytes {
            return Err(RecordingError::new(
                "write_failed",
                format!("{} offset does not match bytes written", self.relative_path),
            ));
        }
        file.sync_all().map_err(|error| {
            RecordingError::new(
                "write_failed",
                format!("fsync {} failed: {}", self.relative_path, error),
            )
        })?;
        let metadata = file.metadata().map_err(|error| {
            RecordingError::new(
                "write_failed",
                format!("stat {} failed: {}", self.relative_path, error),
            )
        })?;
        if !metadata.is_file() || metadata.len() != self.bytes {
            return Err(RecordingError::new(
                "write_failed",
                format!("{} size does not match bytes written", self.relative_path),
            ));
        }
        Ok(ArtifactSnapshot {
            role: self.role,
            relative_path: self.relative_path,
            bytes: self.bytes,
            sha256: format!("{:x}", self.hasher.clone().finalize()),
            identity: ArtifactIdentity {
                device: metadata.dev(),
                inode: metadata.ino(),
                size: metadata.len(),
                mtime_ns: metadata
                    .mtime()
                    .saturating_mul(1_000_000_000)
                    .saturating_add(metadata.mtime_nsec()),
            },
        })
    }

    fn close(&mut self) {
        let _ = self.file.take();
    }
}

fn validate_relative_path(relative_path: &str) -> Result<(), RecordingError> {
    if relative_path.is_empty()
        || relative_path.starts_with('/')
        || relative_path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(RecordingError::new(
            "invalid_argument",
            format!("invalid artifact path: {relative_path}"),
        ));
    }
    Ok(())
}

pub(crate) fn jpeg_payload(payload: &[u8]) -> Result<&[u8], RecordingError> {
    let start = payload
        .windows(2)
        .position(|window| window == b"\xff\xd8")
        .ok_or_else(|| RecordingError::new("bad_frame", "原始 SBS 帧不是完整 JPEG"))?;
    let end = payload
        .windows(2)
        .rposition(|window| window == b"\xff\xd9")
        .filter(|end| *end >= start)
        .ok_or_else(|| RecordingError::new("bad_frame", "原始 SBS 帧不是完整 JPEG"))?;
    Ok(&payload[start..end + 2])
}

pub(crate) fn split_frame_index_record(
    session_id: &str,
    frame: u64,
    source_sequence: u64,
    host_monotonic_ns: u64,
    segment_index: u64,
    segment_frame: u64,
) -> Vec<u8> {
    let session_id = json_string(session_id);
    format!(
        "{{\"frame\":{frame},\"host_monotonic_ns\":{host_monotonic_ns},\
         \"schema\":\"ylx.frame-index.v1\",\"segment_frame\":{segment_frame},\
         \"segment_index\":{segment_index},\"session_id\":{session_id},\
         \"source_sequence\":{source_sequence}}}\n"
    )
    .into_bytes()
}

pub(crate) fn raw_frame_index_record(
    session_id: &str,
    frame: u64,
    source_sequence: u64,
    host_monotonic_ns: u64,
    video_offset: u64,
    video_bytes: u64,
) -> Vec<u8> {
    let session_id = json_string(session_id);
    format!(
        "{{\"frame\":{frame},\"host_monotonic_ns\":{host_monotonic_ns},\
         \"schema\":\"ylx.frame-index.v1\",\"session_id\":{session_id},\
         \"source_sequence\":{source_sequence},\"video_bytes\":{video_bytes},\
         \"video_offset\":{video_offset}}}\n"
    )
    .into_bytes()
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn imu_sample_record(
    session_id: &str,
    sequence: u64,
    packet_sequence: u64,
    sample_index: u8,
    device_timestamp_raw: u32,
    device_ticks: u64,
    host_read_start_ns: u64,
    host_read_end_ns: u64,
    host_monotonic_ns: u64,
    accelerometer: (i16, i16, i16),
    gyroscope: (i16, i16, i16),
    sync_offset_ns: Option<i64>,
    sync_residual_ns: Option<u64>,
    sync_quality: &str,
) -> Vec<u8> {
    let session_id = json_string(session_id);
    let sync_quality = json_string(sync_quality);
    let sync_offset = optional_i64(sync_offset_ns);
    let sync_residual = optional_u64(sync_residual_ns);
    format!(
        "{{\"device_ticks\":{device_ticks},\"device_timestamp_raw\":{device_timestamp_raw},\
         \"format\":\"ylx.imu.v0\",\"host_monotonic_ns\":{host_monotonic_ns},\
         \"host_read_end_ns\":{host_read_end_ns},\"host_read_start_ns\":{host_read_start_ns},\
         \"packet_sequence\":{packet_sequence},\"raw\":{{\"accelerometer\":[{},{},{}],\
         \"gyroscope\":[{},{},{}]}},\"sequence\":{sequence},\"session_id\":{session_id},\
         \"sample_index\":{sample_index},\"sync\":{{\"offset_ns\":{sync_offset},\
         \"quality\":{sync_quality},\"residual_ns\":{sync_residual}}}}}\n",
        accelerometer.0, accelerometer.1, accelerometer.2, gyroscope.0, gyroscope.1, gyroscope.2,
    )
    .into_bytes()
}

fn optional_i64(value: Option<i64>) -> String {
    value.map_or_else(|| "null".to_owned(), |number| number.to_string())
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "null".to_owned(), |number| number.to_string())
}

fn json_string(value: &str) -> String {
    let mut result = String::with_capacity(value.len() + 2);
    result.push('"');
    for character in value.chars() {
        match character {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\u{08}' => result.push_str("\\b"),
            '\u{0c}' => result.push_str("\\f"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            character if character < '\u{20}' => {
                result.push_str(&format!("\\u{:04x}", character as u32));
            }
            character => result.push(character),
        }
    }
    result.push('"');
    result
}

#[cfg(test)]
mod tests {
    use super::{
        CaptureFanoutState, DropEvent, DropQualityPolicy, RecordingFrameGate,
        RecordingSegmentPlanner, RecordingSink, RecordingTapState, evaluate_drop_quality,
        imu_sample_record, jpeg_payload, raw_frame_index_record, split_frame_index_record,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("rp-ylx-recording-{name}-{unique}"));
        fs::create_dir(&root).unwrap();
        root
    }

    #[test]
    fn extracts_enclosing_jpeg_payload() {
        assert_eq!(
            jpeg_payload(b"pad\xff\xd8one\xff\xd9tail").unwrap(),
            b"\xff\xd8one\xff\xd9"
        );
        assert!(jpeg_payload(b"missing").is_err());
    }

    #[test]
    fn encodes_frame_index_like_python_compact_sorted_json() {
        assert_eq!(
            split_frame_index_record("session", 7, 99, 1234, 2, 3),
            b"{\"frame\":7,\"host_monotonic_ns\":1234,\"schema\":\"ylx.frame-index.v1\",\
              \"segment_frame\":3,\"segment_index\":2,\"session_id\":\"session\",\
              \"source_sequence\":99}\n"
        );
        assert_eq!(
            raw_frame_index_record("session", 7, 99, 1234, 400, 50),
            b"{\"frame\":7,\"host_monotonic_ns\":1234,\"schema\":\"ylx.frame-index.v1\",\
              \"session_id\":\"session\",\"source_sequence\":99,\"video_bytes\":50,\
              \"video_offset\":400}\n"
        );
    }

    #[test]
    fn encodes_imu_sample_like_python_compact_sorted_json() {
        let record = imu_sample_record(
            "session",
            1,
            2,
            1,
            1000,
            2000,
            10,
            20,
            15,
            (1, -2, 3),
            (4, 5, -6),
            None,
            Some(100),
            "good",
        );
        assert_eq!(
            record,
            b"{\"device_ticks\":2000,\"device_timestamp_raw\":1000,\"format\":\"ylx.imu.v0\",\
              \"host_monotonic_ns\":15,\"host_read_end_ns\":20,\"host_read_start_ns\":10,\
              \"packet_sequence\":2,\"raw\":{\"accelerometer\":[1,-2,3],\
              \"gyroscope\":[4,5,-6]},\"sequence\":1,\"session_id\":\"session\",\
              \"sample_index\":1,\"sync\":{\"offset_ns\":null,\"quality\":\"good\",\
              \"residual_ns\":100}}\n"
        );
    }

    #[test]
    fn recording_frame_gate_matches_continuous_source_decimation() {
        let mut gate = RecordingFrameGate::new(3).unwrap();

        let first = gate.begin_frame(2).unwrap();
        assert!(first.record);
        assert_eq!(first.dropped_before, 0);
        assert_eq!(first.observed_frames, 1);
        assert_eq!(first.inflight_frames, 1);
        assert_eq!(gate.finish_frame().unwrap(), 0);

        let skipped = gate.begin_frame(0).unwrap();
        assert!(!skipped.record);
        assert_eq!(skipped.observed_frames, 2);
        assert_eq!(skipped.inflight_frames, 0);

        let skipped_again = gate.begin_frame(0).unwrap();
        assert!(!skipped_again.record);
        assert_eq!(skipped_again.observed_frames, 3);

        let recorded = gate.begin_frame(0).unwrap();
        assert!(recorded.record);
        assert_eq!(recorded.observed_frames, 4);
        assert_eq!(recorded.inflight_frames, 1);
        assert_eq!(gate.finish_frame().unwrap(), 0);

        let gap = gate.begin_frame(2).unwrap();
        assert!(gap.record);
        assert_eq!(gap.dropped_before, 2);
        assert_eq!(gap.observed_frames, 7);
        assert_eq!(gap.inflight_frames, 1);
        assert_eq!(gate.finish_frame().unwrap(), 0);
    }

    #[test]
    fn recording_frame_gate_stopping_rejects_new_frames_but_tracks_inflight() {
        let mut gate = RecordingFrameGate::new(1).unwrap();
        assert!(RecordingFrameGate::new(0).is_err());

        let decision = gate.begin_frame(0).unwrap();
        assert!(decision.record);
        assert_eq!(decision.inflight_frames, 1);
        assert_eq!(gate.start_stopping(), 1);

        let stopped = gate.begin_frame(0).unwrap();
        assert!(!stopped.record);
        assert_eq!(stopped.inflight_frames, 1);

        assert_eq!(gate.finish_frame().unwrap(), 0);
        assert!(gate.finish_frame().is_err());
        let snapshot = gate.snapshot();
        assert!(snapshot.stopping);
        assert_eq!(snapshot.inflight_frames, 0);
    }

    #[test]
    fn recording_tap_state_combines_gate_and_failure_latch() {
        let mut tap = RecordingTapState::new(2).unwrap();

        let first = tap.begin_frame(3).unwrap();
        assert!(first.record);
        assert_eq!(first.dropped_before, 0);
        assert_eq!(first.inflight_frames, 1);

        let (should_report, inflight) = tap.mark_failure();
        assert!(should_report);
        assert_eq!(inflight, 1);
        let (duplicate_report, duplicate_inflight) = tap.mark_failure();
        assert!(!duplicate_report);
        assert_eq!(duplicate_inflight, 1);

        let rejected = tap.begin_frame(0).unwrap();
        assert!(!rejected.record);
        assert_eq!(rejected.inflight_frames, 1);
        assert_eq!(tap.finish_frame().unwrap(), 0);

        let snapshot = tap.snapshot();
        assert_eq!(snapshot.frame_decimation, 2);
        assert!(snapshot.stopping);
        assert!(snapshot.failure_reported);
        assert_eq!(snapshot.inflight_frames, 0);
    }

    #[test]
    fn capture_fanout_publishes_preview_without_decimating_recording() {
        let mut fanout = CaptureFanoutState::new(2).unwrap();
        let idle = fanout.begin_frame(0, true).unwrap();
        assert!(idle.publish_preview);
        assert!(!idle.record);
        assert!(!idle.recording_active);
        assert_eq!(idle.inflight_frames, 0);

        let started = fanout.start_recording().unwrap();
        assert!(started.recording_present);
        assert!(started.recording_active);

        let first = fanout.begin_frame(3, true).unwrap();
        assert!(first.publish_preview);
        assert!(first.record);
        assert_eq!(first.dropped_before, 0);
        assert_eq!(first.inflight_frames, 1);
        assert_eq!(fanout.finish_frame().unwrap(), 0);

        let skipped = fanout.begin_frame(0, true).unwrap();
        assert!(skipped.publish_preview);
        assert!(!skipped.record);
        assert_eq!(skipped.inflight_frames, 0);

        let recorded = fanout.begin_frame(0, true).unwrap();
        assert!(recorded.publish_preview);
        assert!(recorded.record);
        assert_eq!(recorded.observed_frames, 3);
        assert_eq!(fanout.finish_frame().unwrap(), 0);
    }

    #[test]
    fn capture_fanout_stops_and_clears_after_inflight_finishes() {
        let mut fanout = CaptureFanoutState::new(1).unwrap();
        assert!(CaptureFanoutState::new(0).is_err());
        fanout.start_recording().unwrap();
        assert!(fanout.begin_frame(0, false).unwrap().record);
        assert!(fanout.start_recording().is_err());
        assert_eq!(fanout.start_stopping(), 1);
        let rejected = fanout.begin_frame(0, true).unwrap();
        assert!(rejected.publish_preview);
        assert!(!rejected.record);
        assert!(!rejected.recording_active);
        assert_eq!(fanout.finish_frame().unwrap(), 0);
        assert!(!fanout.snapshot().recording_present);
        fanout.start_recording().unwrap();
        assert!(fanout.snapshot().recording_active);
    }

    #[test]
    fn capture_fanout_reports_failure_once() {
        let mut fanout = CaptureFanoutState::new(1).unwrap();
        fanout.start_recording().unwrap();
        assert!(fanout.begin_frame(0, false).unwrap().record);
        assert_eq!(fanout.mark_failure(), (true, 1));
        assert_eq!(fanout.mark_failure(), (false, 1));
        assert_eq!(fanout.finish_frame().unwrap(), 0);
        assert!(!fanout.snapshot().recording_present);
        assert_eq!(fanout.mark_failure(), (false, 0));
    }

    #[test]
    fn recording_segment_planner_tracks_boundaries_and_coverage() {
        let mut planner = RecordingSegmentPlanner::new(3).unwrap();

        let first = planner.next_frame(10, 0.25).unwrap();
        assert_eq!(first.ordinal, 0);
        assert_eq!(first.segment_index, 0);
        assert_eq!(first.segment_frame, 0);
        assert_eq!(first.frames_written, 1);
        assert!(first.boundary_recorded);

        for expected_frame in 1..=2 {
            let plan = planner
                .next_frame(10 + expected_frame, 0.25 + expected_frame as f64)
                .unwrap();
            assert_eq!(plan.segment_index, 0);
            assert_eq!(plan.segment_frame, expected_frame);
            assert!(!plan.boundary_recorded);
        }
        let fourth = planner.next_frame(13, 3.5).unwrap();
        assert_eq!(fourth.ordinal, 3);
        assert_eq!(fourth.segment_index, 1);
        assert_eq!(fourth.segment_frame, 0);
        assert!(fourth.boundary_recorded);

        planner.register_segment(0, 0, 3).unwrap();
        planner.register_segment(1, 3, 4).unwrap();
        let snapshot = planner.finish(4, 14, 4.25).unwrap();
        assert_eq!(snapshot.segment_frames, 3);
        assert_eq!(snapshot.frames_written, 4);
        assert_eq!(snapshot.segment_count, 2);
        assert_eq!(snapshot.covered_frames, 4);
        assert_eq!(snapshot.boundary_count, 3);

        assert_eq!(
            planner.boundary(0, 10.0).unwrap(),
            super::SegmentBoundary {
                frame: 10,
                time_seconds: 0.25,
            }
        );
        assert_eq!(
            planner.boundary(4, 10.0).unwrap(),
            super::SegmentBoundary {
                frame: 14,
                time_seconds: 4.25,
            }
        );
    }

    #[test]
    fn recording_segment_planner_rejects_non_contiguous_segments() {
        let mut planner = RecordingSegmentPlanner::new(3).unwrap();
        planner.next_frame(0, 0.0).unwrap();
        planner.next_frame(1, 0.1).unwrap();
        planner.next_frame(2, 0.2).unwrap();

        let skipped = planner.register_segment(1, 0, 3).unwrap_err();
        assert_eq!(skipped.code, "segment_invalid");

        let mut planner = RecordingSegmentPlanner::new(3).unwrap();
        planner.next_frame(0, 0.0).unwrap();
        planner.next_frame(1, 0.1).unwrap();
        planner.next_frame(2, 0.2).unwrap();
        planner.register_segment(0, 0, 2).unwrap();
        let incomplete = planner.finish(3, 3, 0.3).unwrap_err();
        assert_eq!(incomplete.code, "segment_invalid");
    }

    #[test]
    fn drop_quality_policy_reports_same_violation_terms_as_python() {
        let events = [
            DropEvent {
                at_time_seconds: 0.5,
                dropped: 2,
            },
            DropEvent {
                at_time_seconds: 0.7,
                dropped: 1,
            },
            DropEvent {
                at_time_seconds: 2.0,
                dropped: 1,
            },
        ];
        let policy = DropQualityPolicy {
            max_contiguous_dropped_frames: 1,
            max_total_dropped_frames: 10,
            max_drop_fraction: 0.5,
            window_seconds: 1.0,
            max_dropped_frames_per_window: 2,
        };
        let result = evaluate_drop_quality(4, &events, policy).unwrap();
        assert!(!result.accepted);
        assert_eq!(result.dropped, 4);
        assert_eq!(result.total, 8);
        assert_eq!(result.contiguous, 2);
        assert_eq!(result.window_drops, 3);
        assert_eq!(result.violations, vec!["contiguous", "window"]);
    }

    #[test]
    fn recording_sink_writes_split_frame_and_imu_artifacts() {
        let root = temp_root("split");
        let mut sink = RecordingSink::create(&root, "session", true).unwrap();
        let frame_bytes = sink.write_split_frame_index(7, 99, 1234, 2, 3).unwrap();
        let imu_bytes = sink
            .write_imu_sample(
                1,
                2,
                1,
                1000,
                2000,
                10,
                20,
                15,
                (1, -2, 3),
                (4, 5, -6),
                None,
                Some(100),
                "good",
            )
            .unwrap();
        let snapshot = sink.flush_and_close().unwrap();
        assert_eq!(snapshot.frames_written, 1);
        assert_eq!(snapshot.imu_samples_written, 1);
        assert_eq!(snapshot.bytes_written, frame_bytes + imu_bytes);
        assert_eq!(
            fs::read(root.join("frames.ndjson")).unwrap(),
            split_frame_index_record("session", 7, 99, 1234, 2, 3)
        );
        assert_eq!(
            fs::read(root.join("imu.ndjson")).unwrap(),
            imu_sample_record(
                "session",
                1,
                2,
                1,
                1000,
                2000,
                10,
                20,
                15,
                (1, -2, 3),
                (4, 5, -6),
                None,
                Some(100),
                "good",
            )
        );
        let roles: Vec<_> = snapshot.artifacts.iter().map(|item| item.role).collect();
        assert_eq!(roles, vec!["frames.index", "imu.samples"]);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn recording_sink_writes_raw_video_and_index() {
        let root = temp_root("raw");
        fs::create_dir(root.join("video")).unwrap();
        let mut sink = RecordingSink::create(&root, "session", false).unwrap();
        let result = sink
            .write_raw_frame(5, 10, 123, b"prefix\xff\xd8payload\xff\xd9suffix")
            .unwrap();
        assert_eq!(result.video_offset, 0);
        assert_eq!(result.video_bytes, 11);
        assert_eq!(
            fs::read(root.join("video/raw-sbs.mjpeg")).unwrap(),
            b"\xff\xd8payload\xff\xd9"
        );
        assert_eq!(
            fs::read(root.join("frames.ndjson")).unwrap(),
            raw_frame_index_record("session", 5, 10, 123, 0, 11)
        );
        let snapshot = sink.flush_and_close().unwrap();
        assert_eq!(snapshot.frames_written, 1);
        let roles: Vec<_> = snapshot.artifacts.iter().map(|item| item.role).collect();
        assert_eq!(
            roles,
            vec!["frames.index", "imu.samples", "video.raw-side-by-side"]
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn recording_sink_snapshot_reports_counters_without_closing() {
        let root = temp_root("snapshot");
        let mut sink = RecordingSink::create(&root, "session", true).unwrap();
        let frame_bytes = sink.write_split_frame_index(7, 99, 1234, 2, 3).unwrap();

        let snapshot = sink.snapshot();
        assert_eq!(snapshot.frames_written, 1);
        assert_eq!(snapshot.imu_samples_written, 0);
        assert_eq!(snapshot.bytes_written, frame_bytes);
        assert!(snapshot.artifacts.is_empty());

        let imu_bytes = sink
            .write_imu_sample(
                1,
                2,
                1,
                1000,
                2000,
                10,
                20,
                15,
                (1, -2, 3),
                (4, 5, -6),
                None,
                Some(100),
                "good",
            )
            .unwrap();
        let final_snapshot = sink.flush_and_close().unwrap();
        assert_eq!(final_snapshot.frames_written, 1);
        assert_eq!(final_snapshot.imu_samples_written, 1);
        assert_eq!(final_snapshot.bytes_written, frame_bytes + imu_bytes);
        fs::remove_dir_all(root).unwrap();
    }
}
