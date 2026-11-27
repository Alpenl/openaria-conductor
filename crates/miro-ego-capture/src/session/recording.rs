use crate::hardware::imu;
use miro_ego_core::sys::fs::{AlignedBufWriter, sync_and_evict_page_cache};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordingError {
    pub code: &'static str,
    pub message: String,
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
pub struct ArtifactIdentity {
    pub device: u64,
    pub inode: u64,
    pub size: u64,
    pub mtime_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactSnapshot {
    pub role: &'static str,
    pub relative_path: &'static str,
    pub bytes: u64,
    pub sha256: String,
    pub identity: ArtifactIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordingSinkSnapshot {
    pub artifacts: Vec<ArtifactSnapshot>,
    pub bytes_written: u64,
    pub frames_written: u64,
    pub imu_samples_written: u64,
    // Accepted records are distinct from completed writes. Checkpoint publication
    // must gate on completed writes before syncing through a separate descriptor.
    pub frames_indexed: u64,
    pub imu_samples_indexed: u64,
    pub index_pending_bytes: u64,
    pub index_peak_bytes: u64,
    pub index_max_write_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FrameTimestampAudit {
    pub camera_counter_raw: Option<u32>,
    pub counter_bits: u8,
    pub host_dequeue_monotonic_ns: u64,
    pub schema: &'static str,
    pub timestamp_clock: &'static str,
    pub timestamp_source: &'static str,
    pub v4l2_buffer_flags: u32,
}

#[derive(Serialize)]
struct AuditedFrameIndexRecord<'a> {
    frame: u64,
    host_monotonic_ns: u64,
    schema: &'static str,
    segment_frame: u64,
    segment_index: u64,
    session_id: &'a str,
    source_sequence: u64,
    timestamp_audit: FrameTimestampAudit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureFanoutDecision {
    pub publish_preview: bool,
    pub record: bool,
    pub dropped_before: u64,
    pub recovered_source_gap: u64,
    pub recovered_recorded_drops: u64,
    pub observed_frames: u64,
    pub inflight_frames: u64,
    pub recording_active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureFanoutSnapshot {
    pub frame_decimation: u64,
    pub recording_present: bool,
    pub recording_active: bool,
    pub first_frame: bool,
    pub observed_frames: u64,
    pub inflight_frames: u64,
    pub stopping: bool,
    pub failure_reported: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentFramePlan {
    pub ordinal: u64,
    pub segment_index: u64,
    pub segment_frame: u64,
    pub frames_written: u64,
    pub boundary_recorded: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentBoundary {
    pub frame: u64,
    pub time_seconds: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentPlannerSnapshot {
    pub segment_frames: u64,
    pub frames_written: u64,
    pub segment_count: u64,
    pub covered_frames: u64,
    pub boundary_count: u64,
}

#[derive(Default)]
struct RecordingState {
    observed_frames: u64,
    inflight_frames: u64,
    stopping: bool,
    failure_reported: bool,
    recover_source_gaps: bool,
    last_recovered_gap_ns: Option<u64>,
}

pub struct CaptureFanoutState {
    frame_decimation: u64,
    recording: Option<RecordingState>,
}

impl CaptureFanoutState {
    pub fn new(frame_decimation: u64) -> Result<Self, RecordingError> {
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

    pub fn start_recording(
        &mut self,
        recover_source_gaps: bool,
    ) -> Result<CaptureFanoutSnapshot, RecordingError> {
        if let Some(existing) = &self.recording {
            if existing.inflight_frames != 0 || !existing.stopping {
                return Err(RecordingError::new(
                    "invalid_state",
                    "capture fanout is already recording",
                ));
            }
        }
        self.recording = Some(RecordingState {
            recover_source_gaps,
            ..RecordingState::default()
        });
        Ok(self.snapshot())
    }

    pub fn begin_frame(
        &mut self,
        mut dropped_before: u64,
        has_preview: bool,
        recovery_time_ns: Option<u64>,
    ) -> Result<CaptureFanoutDecision, RecordingError> {
        let Some(recording) = self.recording.as_mut() else {
            return Ok(CaptureFanoutDecision {
                publish_preview: has_preview,
                record: false,
                dropped_before,
                recovered_source_gap: 0,
                recovered_recorded_drops: 0,
                observed_frames: 0,
                inflight_frames: 0,
                recording_active: false,
            });
        };
        let mut record = false;
        let mut recovered_source_gap = 0;
        let mut recovered_recorded_drops = 0;
        if !recording.stopping {
            if recording.observed_frames == 0 {
                // Gaps before the first frame belong to preview, not this take.
                recording.observed_frames = 1;
                dropped_before = 0;
                record = true;
            } else {
                let gap_start = recording.observed_frames;
                let observed_index = gap_start.checked_add(dropped_before).ok_or_else(|| {
                    RecordingError::new(
                        "counter_overflow",
                        "recording frame gate observed overflow",
                    )
                })?;
                recording.observed_frames = observed_index.checked_add(1).ok_or_else(|| {
                    RecordingError::new(
                        "counter_overflow",
                        "recording frame gate observed overflow",
                    )
                })?;
                if recording.recover_source_gaps && (1..=4).contains(&dropped_before) {
                    if let Some(now) = recovery_time_ns.filter(|now| {
                        recording
                            .last_recovered_gap_ns
                            .is_none_or(|last| now.saturating_sub(last) >= 1_000_000_000)
                    }) {
                        recovered_source_gap = dropped_before;
                        recovered_recorded_drops = count_recorded_indices_in_range(
                            gap_start,
                            observed_index,
                            self.frame_decimation,
                        );
                        dropped_before = 0;
                        recording.last_recovered_gap_ns = Some(now);
                    }
                }
                record = dropped_before != 0 || observed_index % self.frame_decimation == 0;
            }
            if record {
                recording.inflight_frames =
                    recording.inflight_frames.checked_add(1).ok_or_else(|| {
                        RecordingError::new(
                            "counter_overflow",
                            "recording frame gate inflight overflow",
                        )
                    })?;
            }
        }
        Ok(CaptureFanoutDecision {
            publish_preview: has_preview,
            record,
            dropped_before,
            recovered_source_gap,
            recovered_recorded_drops,
            observed_frames: recording.observed_frames,
            inflight_frames: recording.inflight_frames,
            recording_active: !recording.stopping,
        })
    }

    pub fn finish_frame(&mut self) -> Result<u64, RecordingError> {
        let Some(recording) = self.recording.as_mut() else {
            return Err(RecordingError::new(
                "invalid_state",
                "capture fanout has no recording frame to finish",
            ));
        };
        let inflight = recording.inflight_frames.checked_sub(1).ok_or_else(|| {
            RecordingError::new(
                "invalid_state",
                "recording frame gate has no inflight frame to finish",
            )
        })?;
        recording.inflight_frames = inflight;
        if inflight == 0 && recording.stopping {
            self.recording = None;
        }
        Ok(inflight)
    }

    pub fn start_stopping(&mut self) -> u64 {
        let Some(recording) = self.recording.as_mut() else {
            return 0;
        };
        recording.stopping = true;
        let inflight = recording.inflight_frames;
        if inflight == 0 {
            self.recording = None;
        }
        inflight
    }

    pub fn mark_failure(&mut self) -> (bool, u64) {
        let Some(recording) = self.recording.as_mut() else {
            return (false, 0);
        };
        let should_report = !recording.failure_reported;
        recording.failure_reported = true;
        (should_report, self.start_stopping())
    }

    pub fn snapshot(&self) -> CaptureFanoutSnapshot {
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
        CaptureFanoutSnapshot {
            frame_decimation: self.frame_decimation,
            recording_present: true,
            recording_active: !recording.stopping,
            first_frame: recording.observed_frames == 0,
            observed_frames: recording.observed_frames,
            inflight_frames: recording.inflight_frames,
            stopping: recording.stopping,
            failure_reported: recording.failure_reported,
        }
    }
}

pub struct RecordingSegmentPlanner {
    segment_frames: u64,
    frames_written: u64,
    segment_count: u64,
    covered_frames: u64,
    boundary_record_sequence: BTreeMap<u64, u64>,
    boundary_elapsed: BTreeMap<u64, f64>,
}

impl RecordingSegmentPlanner {
    pub fn new(segment_frames: u64) -> Result<Self, RecordingError> {
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

    pub fn next_frame(
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

    pub fn register_segment(
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

    pub fn finish(
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

    pub fn boundary(
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

    pub fn snapshot(&self) -> SegmentPlannerSnapshot {
        SegmentPlannerSnapshot {
            segment_frames: self.segment_frames,
            frames_written: self.frames_written,
            segment_count: self.segment_count,
            covered_frames: self.covered_frames,
            boundary_count: u64::try_from(self.boundary_record_sequence.len()).unwrap_or(u64::MAX),
        }
    }
}

fn multiples_below(limit: u64, step: u64) -> u64 {
    if limit == 0 || step == 0 {
        0
    } else {
        (limit - 1) / step + 1
    }
}

fn count_recorded_indices_in_range(start: u64, end: u64, decimation: u64) -> u64 {
    if end <= start || decimation == 0 {
        0
    } else {
        multiples_below(end, decimation).saturating_sub(multiples_below(start, decimation))
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

pub struct RecordingSink {
    session_id: String,
    frames: ArtifactWriter,
    imu: ArtifactWriter,
    bytes_written: u64,
    frames_written: u64,
    imu_samples_written: u64,
    closed: bool,
}

impl RecordingSink {
    pub fn create(session_root: &Path, session_id: &str) -> Result<Self, RecordingError> {
        if session_id.is_empty() {
            return Err(RecordingError::new(
                "invalid_argument",
                "session_id must not be empty",
            ));
        }
        let frames = ArtifactWriter::create(session_root, "frames.index", "frames.ndjson")?;
        let imu = ArtifactWriter::create(session_root, "imu.samples", "imu.ndjson")?;
        Ok(Self {
            session_id: session_id.to_owned(),
            frames,
            imu,
            bytes_written: 0,
            frames_written: 0,
            imu_samples_written: 0,
            closed: false,
        })
    }

    #[cfg(test)]
    pub fn write_split_frame_index(
        &mut self,
        frame: u64,
        source_sequence: u64,
        host_monotonic_ns: u64,
        segment_index: u64,
        segment_frame: u64,
    ) -> Result<u64, RecordingError> {
        self.write_audited_frame_index(
            frame,
            source_sequence,
            host_monotonic_ns,
            segment_index,
            segment_frame,
            None,
        )
    }

    pub fn write_audited_frame_index(
        &mut self,
        frame: u64,
        source_sequence: u64,
        host_monotonic_ns: u64,
        segment_index: u64,
        segment_frame: u64,
        audit: Option<FrameTimestampAudit>,
    ) -> Result<u64, RecordingError> {
        self.ensure_open()?;
        let record = match audit {
            Some(timestamp_audit) => {
                let row = AuditedFrameIndexRecord {
                    frame,
                    host_monotonic_ns,
                    schema: "miro-ego.frame-index.v1",
                    segment_frame,
                    segment_index,
                    session_id: &self.session_id,
                    source_sequence,
                    timestamp_audit,
                };
                let mut encoded = serde_json::to_vec(&row)
                    .map_err(|error| RecordingError::new("write_failed", error.to_string()))?;
                encoded.push(b'\n');
                encoded
            }
            None => split_frame_index_record(
                &self.session_id,
                frame,
                source_sequence,
                host_monotonic_ns,
                segment_index,
                segment_frame,
            ),
        };
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

    pub fn write_imu_sample_record(
        &mut self,
        input: &ImuSampleRecord<'_>,
    ) -> Result<u64, RecordingError> {
        self.ensure_open()?;
        let record = imu_sample_record(&self.session_id, input);
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

    pub fn write_imu_observation(
        &mut self,
        observation: &imu::ImuObservation,
    ) -> Result<u64, RecordingError> {
        let mut bytes_written = 0_u64;
        for sample in &observation.samples {
            let written = self.write_imu_sample_record(&ImuSampleRecord {
                sequence: sample.sequence,
                packet_sequence: sample.packet_sequence,
                sample_index: sample.sample_index,
                device_timestamp_raw: sample.device_timestamp_raw,
                device_ticks: sample.device_ticks,
                host_read_start_ns: sample.host_read_start_ns,
                host_read_end_ns: sample.host_read_end_ns,
                host_monotonic_ns: sample.host_monotonic_ns,
                accelerometer: (
                    sample.accelerometer.x,
                    sample.accelerometer.y,
                    sample.accelerometer.z,
                ),
                gyroscope: (sample.gyroscope.x, sample.gyroscope.y, sample.gyroscope.z),
                sync_offset_ns: sample.sync_offset_ns,
                sync_residual_ns: sample.sync_residual_ns,
                sync_quality: sample.sync_quality,
            })?;
            bytes_written = bytes_written.checked_add(written).ok_or_else(|| {
                RecordingError::new("write_failed", "IMU observation byte count overflow")
            })?;
        }
        Ok(bytes_written)
    }

    pub fn flush_and_close(&mut self) -> Result<RecordingSinkSnapshot, RecordingError> {
        self.ensure_open()?;
        let frames_result = self.frames.flush_and_close();
        let imu_result = self.imu.flush_and_close();
        self.closed = true;
        let frames = frames_result?;
        let imu = imu_result?;
        let artifacts = vec![frames, imu];
        Ok(RecordingSinkSnapshot {
            artifacts,
            bytes_written: self.bytes_written,
            frames_written: self.frames_written,
            imu_samples_written: self.imu_samples_written,
            ..self.snapshot()
        })
    }

    pub fn snapshot(&self) -> RecordingSinkSnapshot {
        RecordingSinkSnapshot {
            artifacts: Vec::new(),
            bytes_written: self.bytes_written,
            frames_written: self.frames_written,
            imu_samples_written: self.imu_samples_written,
            frames_indexed: self.frames.progress.records.load(Ordering::Acquire),
            imu_samples_indexed: self.imu.progress.records.load(Ordering::Acquire),
            index_pending_bytes: (self.frames.progress.pending.load(Ordering::Relaxed)
                + self.imu.progress.pending.load(Ordering::Relaxed))
                as u64,
            index_peak_bytes: (self.frames.progress.peak.load(Ordering::Relaxed)
                + self.imu.progress.peak.load(Ordering::Relaxed))
                as u64,
            index_max_write_ns: self
                .frames
                .progress
                .max_write_ns
                .load(Ordering::Relaxed)
                .max(self.imu.progress.max_write_ns.load(Ordering::Relaxed)),
        }
    }

    pub fn close(&mut self) {
        self.frames.close();
        self.imu.close();
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

// Bounds include the in-flight write, so a stalled disk cannot grow memory
// without limit. Each artifact has its own worker: IMU IO cannot hold up frames.
const INDEX_PENDING_BYTES: usize = 8 * 1024 * 1024;
const INDEX_PENDING_RECORDS: usize = 16_384;

#[derive(Default)]
struct IndexProgress {
    records: AtomicU64,
    pending: AtomicUsize,
    peak: AtomicUsize,
    max_write_ns: AtomicU64,
    error: Mutex<Option<RecordingError>>,
}

struct ArtifactWriter {
    role: &'static str,
    relative_path: &'static str,
    sender: Option<mpsc::SyncSender<Vec<u8>>>,
    worker: Option<JoinHandle<Result<File, RecordingError>>>,
    progress: Arc<IndexProgress>,
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
        Self::with_buffered_writer(file, role, relative_path)
    }

    fn with_buffered_writer(
        file: File,
        role: &'static str,
        relative_path: &'static str,
    ) -> Result<Self, RecordingError> {
        let progress = Arc::new(IndexProgress::default());
        let state = Arc::clone(&progress);
        let (sender, receiver) = mpsc::sync_channel::<Vec<u8>>(INDEX_PENDING_RECORDS);
        let worker = thread::Builder::new()
            .name(format!("index-{role}"))
            .spawn(move || {
                let mut buffered = AlignedBufWriter::new(file);
                for payload in receiver {
                    let started = Instant::now();
                    let result = buffered.write_all(&payload);
                    state.max_write_ns.fetch_max(
                        started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
                        Ordering::Relaxed,
                    );
                    state.pending.fetch_sub(payload.len(), Ordering::AcqRel);
                    if let Err(error) = result {
                        let error = RecordingError::new(
                            "write_failed",
                            format!("write {relative_path} failed: {error}"),
                        );
                        *state
                            .error
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(error.clone());
                        return Err(error);
                    }
                    state.records.fetch_add(1, Ordering::Release);
                }
                buffered.into_inner().map_err(|error| {
                    let error = RecordingError::new(
                        "write_failed",
                        format!("flush {relative_path} failed: {error}"),
                    );
                    *state
                        .error
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(error.clone());
                    error
                })
            })
            .map_err(|error| {
                RecordingError::new("write_failed", format!("start index writer: {error}"))
            })?;
        Ok(Self {
            role,
            relative_path,
            sender: Some(sender),
            worker: Some(worker),
            progress,
            hasher: Sha256::new(),
            bytes: 0,
        })
    }

    #[cfg(test)]
    fn with_writer(
        mut file: File,
        role: &'static str,
        relative_path: &'static str,
        mut write: impl FnMut(&mut File, &[u8]) -> std::io::Result<()> + Send + 'static,
    ) -> Result<Self, RecordingError> {
        let progress = Arc::new(IndexProgress::default());
        let state = Arc::clone(&progress);
        let (sender, receiver) = mpsc::sync_channel::<Vec<u8>>(INDEX_PENDING_RECORDS);
        let worker = thread::Builder::new()
            .name(format!("index-{role}"))
            .spawn(move || {
                for payload in receiver {
                    let started = Instant::now();
                    let result = write(&mut file, &payload);
                    state.max_write_ns.fetch_max(
                        started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
                        Ordering::Relaxed,
                    );
                    state.pending.fetch_sub(payload.len(), Ordering::AcqRel);
                    if let Err(error) = result {
                        let error = RecordingError::new(
                            "write_failed",
                            format!("write {relative_path} failed: {error}"),
                        );
                        *state
                            .error
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(error.clone());
                        return Err(error);
                    }
                    state.records.fetch_add(1, Ordering::Release);
                }
                Ok(file)
            })
            .map_err(|error| {
                RecordingError::new("write_failed", format!("start index writer: {error}"))
            })?;
        Ok(Self {
            role,
            relative_path,
            sender: Some(sender),
            worker: Some(worker),
            progress,
            hasher: Sha256::new(),
            bytes: 0,
        })
    }

    fn check(&self) -> Result<(), RecordingError> {
        let guard = self.progress.error.lock().map_err(|_| {
            RecordingError::new(
                "native_recording_poisoned",
                "index writer error mutex is poisoned",
            )
        })?;
        match guard.as_ref() {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    fn write(&mut self, payload: &[u8]) -> Result<u64, RecordingError> {
        let written = u64::try_from(payload.len())
            .map_err(|_| RecordingError::new("write_failed", "payload too large"))?;
        self.check()?;
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| RecordingError::new("invalid_state", "artifact writer is closed"))?;
        let previous = miro_ego_core::sys::try_reserve_bounded(
            &self.progress.pending,
            payload.len(),
            INDEX_PENDING_BYTES,
        )
        .ok_or_else(|| {
            RecordingError::new("write_failed", "index writer byte backlog exhausted")
        })?;
        self.progress
            .peak
            .fetch_max(previous + payload.len(), Ordering::Relaxed);
        if sender.try_send(payload.to_vec()).is_err() {
            self.progress
                .pending
                .fetch_sub(payload.len(), Ordering::AcqRel);
            self.check()?;
            return Err(RecordingError::new(
                "write_failed",
                "index writer queue full or disconnected",
            ));
        }
        self.hasher.update(payload);
        self.bytes = self
            .bytes
            .checked_add(written)
            .ok_or_else(|| RecordingError::new("write_failed", "artifact byte count overflow"))?;
        Ok(written)
    }

    fn flush_and_close(&mut self) -> Result<ArtifactSnapshot, RecordingError> {
        self.sender.take();
        let worker = self
            .worker
            .take()
            .ok_or_else(|| RecordingError::new("invalid_state", "artifact writer is closed"))?;
        let mut file = worker
            .join()
            .map_err(|_| RecordingError::new("write_failed", "index writer panicked"))??;
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
        sync_and_evict_page_cache(&file).map_err(|error| {
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
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            // Drain before recovery can inspect/rename files. Never detach an
            // outstanding writer, even when another artifact already failed.
            if worker.join().is_err() {
                *self
                    .progress
                    .error
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    Some(RecordingError::new("write_failed", "index writer panicked"));
            }
        }
    }
}

impl Drop for ArtifactWriter {
    fn drop(&mut self) {
        self.close();
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

pub fn jpeg_payload(payload: &[u8]) -> Result<&[u8], RecordingError> {
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

/// Vendor AVI1 APP0 payload: signature, six vendor bytes, then BE24 frame counter.
/// Walk JPEG segments; never mistake entropy bytes or another camera's APP0 for it.
pub fn avi1_counter(jpeg: &[u8]) -> Option<u32> {
    if !jpeg.starts_with(b"\xff\xd8") {
        return None;
    }
    let mut offset = 2usize;
    while offset < jpeg.len() {
        if jpeg[offset] != 0xff {
            return None;
        }
        while offset < jpeg.len() && jpeg[offset] == 0xff {
            offset += 1;
        }
        if offset >= jpeg.len() {
            return None;
        }
        let marker = jpeg[offset];
        offset += 1;
        if marker == 0xda || marker == 0xd9 {
            return None;
        }
        if marker == 0x00 || marker == 0x01 || (0xd0..=0xd7).contains(&marker) {
            continue;
        }
        if offset + 2 > jpeg.len() {
            return None;
        }
        let length = u16::from_be_bytes([jpeg[offset], jpeg[offset + 1]]) as usize;
        if length < 2 || offset + length > jpeg.len() {
            return None;
        }
        let data = &jpeg[offset + 2..offset + length];
        if marker == 0xe0 && data.len() == 31 && data.starts_with(b"AVI1\0\x01\x01\x01") {
            return Some(((data[10] as u32) << 16) | ((data[11] as u32) << 8) | data[12] as u32);
        }
        offset += length;
    }
    None
}

pub struct ImuSampleRecord<'a> {
    pub sequence: u64,
    pub packet_sequence: u64,
    pub sample_index: u8,
    pub device_timestamp_raw: u32,
    pub device_ticks: u64,
    pub host_read_start_ns: u64,
    pub host_read_end_ns: u64,
    pub host_monotonic_ns: u64,
    pub accelerometer: (i16, i16, i16),
    pub gyroscope: (i16, i16, i16),
    pub sync_offset_ns: Option<i64>,
    pub sync_residual_ns: Option<u64>,
    pub sync_quality: &'a str,
}

pub fn split_frame_index_record(
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
         \"schema\":\"miro-ego.frame-index.v1\",\"segment_frame\":{segment_frame},\
         \"segment_index\":{segment_index},\"session_id\":{session_id},\
         \"source_sequence\":{source_sequence}}}\n"
    )
    .into_bytes()
}

pub fn imu_sample_record(session_id: &str, input: &ImuSampleRecord<'_>) -> Vec<u8> {
    let session_id = json_string(session_id);
    let sync_quality = json_string(input.sync_quality);
    let sync_offset = optional_i64(input.sync_offset_ns);
    let sync_residual = optional_u64(input.sync_residual_ns);
    let (ax, ay, az) = input.accelerometer;
    let (gx, gy, gz) = input.gyroscope;
    format!(
        "{{\"device_ticks\":{},\"device_timestamp_raw\":{},\"format\":\"miro-ego.imu.v1\",\
         \"host_monotonic_ns\":{},\"host_read_end_ns\":{},\"host_read_start_ns\":{},\
         \"packet_sequence\":{},\"raw\":{{\"accelerometer\":[{ax},{ay},{az}],\
         \"gyroscope\":[{gx},{gy},{gz}]}},\"sequence\":{},\"session_id\":{session_id},\
         \"sample_index\":{},\"sync\":{{\"offset_ns\":{sync_offset},\"quality\":{sync_quality},\
         \"residual_ns\":{sync_residual}}}}}\n",
        input.device_ticks,
        input.device_timestamp_raw,
        input.host_monotonic_ns,
        input.host_read_end_ns,
        input.host_read_start_ns,
        input.packet_sequence,
        input.sequence,
        input.sample_index,
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
        CaptureFanoutState, FrameTimestampAudit, ImuSampleRecord, RecordingSegmentPlanner,
        RecordingSink, avi1_counter, imu_sample_record, jpeg_payload, split_frame_index_record,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("miro-ego-recording-{name}-{unique}"));
        fs::create_dir(&root).unwrap();
        root
    }

    #[test]
    fn avi1_counter_requires_vendor_layout_and_valid_jpeg_segments() {
        let mut jpeg = vec![0xff, 0xd8, 0xff, 0xe0, 0x00, 0x21];
        let mut data = vec![0; 31];
        data[..8].copy_from_slice(b"AVI1\0\x01\x01\x01");
        data[10..13].copy_from_slice(&[0x12, 0x34, 0x56]);
        jpeg.extend_from_slice(&data);
        jpeg.extend_from_slice(&[0xff, 0xd9]);
        assert_eq!(super::avi1_counter(&jpeg), Some(0x123456));
        let mut shifted = vec![0xff, 0xd8, 0xff, 0xe1, 0, 4, 7, 9];
        shifted.extend_from_slice(&jpeg[2..]);
        assert_eq!(super::avi1_counter(&shifted), Some(0x123456));
        assert_eq!(super::avi1_counter(&jpeg[..18]), None);
        jpeg[6] = b'J';
        assert_eq!(super::avi1_counter(&jpeg), None);
        assert_eq!(super::avi1_counter(b"\xff\xd8\xff\xda\x00\x02AVI1"), None);
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
            b"{\"frame\":7,\"host_monotonic_ns\":1234,\"schema\":\"miro-ego.frame-index.v1\",\
              \"segment_frame\":3,\"segment_index\":2,\"session_id\":\"session\",\
              \"source_sequence\":99}\n"
        );
    }

    #[test]
    fn encodes_audited_frame_index_directly_as_compact_sorted_json() {
        let root = temp_root("audited-v2");
        let mut sink = RecordingSink::create(&root, "session").unwrap();
        sink.write_audited_frame_index(
            7,
            99,
            1234,
            2,
            3,
            Some(FrameTimestampAudit {
                camera_counter_raw: Some(42),
                counter_bits: 24,
                host_dequeue_monotonic_ns: 1200,
                schema: "miro-ego.frame-timestamp-audit.v1",
                timestamp_clock: "v4l2_monotonic",
                timestamp_source: "start_of_exposure",
                v4l2_buffer_flags: 0x12000,
            }),
        )
        .unwrap();
        sink.flush_and_close().unwrap();
        let written = fs::read_to_string(root.join("frames.ndjson")).unwrap();
        let expected = "{\"frame\":7,\"host_monotonic_ns\":1234,\"schema\":\"miro-ego.frame-index.v1\",\"segment_frame\":3,\"segment_index\":2,\"session_id\":\"session\",\"source_sequence\":99,\"timestamp_audit\":{\"camera_counter_raw\":42,\"counter_bits\":24,\"host_dequeue_monotonic_ns\":1200,\"schema\":\"miro-ego.frame-timestamp-audit.v1\",\"timestamp_clock\":\"v4l2_monotonic\",\"timestamp_source\":\"start_of_exposure\",\"v4l2_buffer_flags\":73728}}\n";
        assert_eq!(written, expected);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn encodes_imu_sample_like_python_compact_sorted_json() {
        let record = imu_sample_record(
            "session",
            &ImuSampleRecord {
                sequence: 1,
                packet_sequence: 2,
                sample_index: 1,
                device_timestamp_raw: 1000,
                device_ticks: 2000,
                host_read_start_ns: 10,
                host_read_end_ns: 20,
                host_monotonic_ns: 15,
                accelerometer: (1, -2, 3),
                gyroscope: (4, 5, -6),
                sync_offset_ns: None,
                sync_residual_ns: Some(100),
                sync_quality: "good",
            },
        );
        assert_eq!(
            record,
            b"{\"device_ticks\":2000,\"device_timestamp_raw\":1000,\"format\":\"miro-ego.imu.v1\",\
              \"host_monotonic_ns\":15,\"host_read_end_ns\":20,\"host_read_start_ns\":10,\
              \"packet_sequence\":2,\"raw\":{\"accelerometer\":[1,-2,3],\
              \"gyroscope\":[4,5,-6]},\"sequence\":1,\"session_id\":\"session\",\
              \"sample_index\":1,\"sync\":{\"offset_ns\":null,\"quality\":\"good\",\
              \"residual_ns\":100}}\n"
        );
    }

    #[test]
    fn isolated_source_gap_preserves_decimation_and_rejects_a_second_nearby_gap() {
        let mut gate = CaptureFanoutState::new(2).unwrap();
        gate.start_recording(true).unwrap();
        assert!(gate.begin_frame(0, true, None).unwrap().record);
        gate.finish_frame().unwrap();

        // The one absent source frame was already outside the recorded phase.
        let first_gap = gate.begin_frame(1, true, Some(10_000_000_000)).unwrap();
        assert!(first_gap.record);
        assert_eq!(first_gap.recovered_source_gap, 1);
        assert_eq!(first_gap.dropped_before, 0);
        assert_eq!(first_gap.observed_frames, 3);
        gate.finish_frame().unwrap();

        let frequent = gate.begin_frame(2, true, Some(10_500_000_000)).unwrap();
        assert!(frequent.record);
        assert_eq!(frequent.dropped_before, 2);
        assert_eq!(frequent.recovered_source_gap, 0);
    }

    #[test]
    fn source_gap_recovery_keeps_the_next_recorded_frame_on_its_original_phase() {
        let mut gate = CaptureFanoutState::new(2).unwrap();
        gate.start_recording(true).unwrap();
        assert!(gate.begin_frame(0, true, None).unwrap().record);
        gate.finish_frame().unwrap();

        let off_phase = gate.begin_frame(4, true, Some(10_000_000_000)).unwrap();
        assert!(!off_phase.record);
        assert_eq!(off_phase.recovered_source_gap, 4);
        assert!(gate.begin_frame(0, true, None).unwrap().record);
        gate.finish_frame().unwrap();

        let too_large = gate.begin_frame(5, true, Some(12_000_000_000)).unwrap();
        assert_eq!(too_large.dropped_before, 5);
        assert_eq!(too_large.recovered_source_gap, 0);
    }

    #[test]
    fn capture_fanout_matches_continuous_source_decimation() {
        let mut gate = CaptureFanoutState::new(3).unwrap();
        gate.start_recording(false).unwrap();

        let first = gate.begin_frame(2, true, None).unwrap();
        assert!(first.record);
        assert_eq!(first.dropped_before, 0);
        assert_eq!(first.observed_frames, 1);
        assert_eq!(first.inflight_frames, 1);
        assert_eq!(gate.finish_frame().unwrap(), 0);

        let skipped = gate.begin_frame(0, true, None).unwrap();
        assert!(!skipped.record);
        assert_eq!(skipped.observed_frames, 2);
        assert_eq!(skipped.inflight_frames, 0);

        let skipped_again = gate.begin_frame(0, true, None).unwrap();
        assert!(!skipped_again.record);
        assert_eq!(skipped_again.observed_frames, 3);

        let recorded = gate.begin_frame(0, true, None).unwrap();
        assert!(recorded.record);
        assert_eq!(recorded.observed_frames, 4);
        assert_eq!(recorded.inflight_frames, 1);
        assert_eq!(gate.finish_frame().unwrap(), 0);

        let gap = gate.begin_frame(2, true, None).unwrap();
        assert!(gap.record);
        assert_eq!(gap.dropped_before, 2);
        assert_eq!(gap.observed_frames, 7);
        assert_eq!(gap.inflight_frames, 1);
        assert_eq!(gate.finish_frame().unwrap(), 0);
    }

    #[test]
    fn capture_fanout_stopping_rejects_new_frames_but_tracks_inflight() {
        let mut gate = CaptureFanoutState::new(1).unwrap();
        gate.start_recording(false).unwrap();

        let decision = gate.begin_frame(0, true, None).unwrap();
        assert!(decision.record);
        assert_eq!(decision.inflight_frames, 1);
        assert_eq!(gate.start_stopping(), 1);

        let stopped = gate.begin_frame(0, true, None).unwrap();
        assert!(!stopped.record);
        assert_eq!(stopped.inflight_frames, 1);

        assert_eq!(gate.finish_frame().unwrap(), 0);
        assert!(gate.finish_frame().is_err());
        let snapshot = gate.snapshot();
        assert!(!snapshot.recording_present);
        assert_eq!(snapshot.inflight_frames, 0);
    }

    #[test]
    fn capture_fanout_preserves_failure_until_inflight_frames_drain() {
        let mut tap = CaptureFanoutState::new(2).unwrap();
        tap.start_recording(false).unwrap();

        let first = tap.begin_frame(3, true, None).unwrap();
        assert!(first.record);
        assert_eq!(first.dropped_before, 0);
        assert_eq!(first.inflight_frames, 1);

        let (should_report, inflight) = tap.mark_failure();
        assert!(should_report);
        assert_eq!(inflight, 1);
        let (duplicate_report, duplicate_inflight) = tap.mark_failure();
        assert!(!duplicate_report);
        assert_eq!(duplicate_inflight, 1);

        let rejected = tap.begin_frame(0, true, None).unwrap();
        assert!(!rejected.record);
        assert_eq!(rejected.inflight_frames, 1);
        let snapshot = tap.snapshot();
        assert_eq!(snapshot.frame_decimation, 2);
        assert!(snapshot.stopping);
        assert!(snapshot.failure_reported);
        assert_eq!(snapshot.inflight_frames, 1);
        assert_eq!(tap.finish_frame().unwrap(), 0);
        assert!(!tap.snapshot().recording_present);
    }

    #[test]
    fn capture_fanout_publishes_preview_without_decimating_recording() {
        let mut fanout = CaptureFanoutState::new(2).unwrap();
        let idle = fanout.begin_frame(0, true, None).unwrap();
        assert!(idle.publish_preview);
        assert!(!idle.record);
        assert!(!idle.recording_active);
        assert_eq!(idle.inflight_frames, 0);

        let started = fanout.start_recording(false).unwrap();
        assert!(started.recording_present);
        assert!(started.recording_active);

        let first = fanout.begin_frame(3, true, None).unwrap();
        assert!(first.publish_preview);
        assert!(first.record);
        assert_eq!(first.dropped_before, 0);
        assert_eq!(first.inflight_frames, 1);
        assert_eq!(fanout.finish_frame().unwrap(), 0);

        let skipped = fanout.begin_frame(0, true, None).unwrap();
        assert!(skipped.publish_preview);
        assert!(!skipped.record);
        assert_eq!(skipped.inflight_frames, 0);

        let recorded = fanout.begin_frame(0, true, None).unwrap();
        assert!(recorded.publish_preview);
        assert!(recorded.record);
        assert_eq!(recorded.observed_frames, 3);
        assert_eq!(fanout.finish_frame().unwrap(), 0);
    }

    #[test]
    fn capture_fanout_stops_and_clears_after_inflight_finishes() {
        let mut fanout = CaptureFanoutState::new(1).unwrap();
        assert!(CaptureFanoutState::new(0).is_err());
        fanout.start_recording(false).unwrap();
        assert!(fanout.begin_frame(0, false, None).unwrap().record);
        assert!(fanout.start_recording(false).is_err());
        assert_eq!(fanout.start_stopping(), 1);
        let rejected = fanout.begin_frame(0, true, None).unwrap();
        assert!(rejected.publish_preview);
        assert!(!rejected.record);
        assert!(!rejected.recording_active);
        assert_eq!(fanout.finish_frame().unwrap(), 0);
        assert!(!fanout.snapshot().recording_present);
        fanout.start_recording(false).unwrap();
        assert!(fanout.snapshot().recording_active);
    }

    #[test]
    fn capture_fanout_reports_failure_once() {
        let mut fanout = CaptureFanoutState::new(1).unwrap();
        fanout.start_recording(false).unwrap();
        assert!(fanout.begin_frame(0, false, None).unwrap().record);
        assert_eq!(fanout.mark_failure(), (true, 1));
        assert_eq!(fanout.mark_failure(), (false, 1));
        assert_eq!(fanout.finish_frame().unwrap(), 0);
        assert!(!fanout.snapshot().recording_present);
        assert_eq!(fanout.mark_failure(), (false, 0));
    }

    #[test]
    fn capture_fanout_counts_recovered_recorded_phase_drops_under_decimation() {
        let mut fanout = CaptureFanoutState::new(2).unwrap();
        fanout.start_recording(true).unwrap();

        // First frame -> index 0 (recorded)
        let f0 = fanout.begin_frame(0, true, None).unwrap();
        assert!(f0.record);
        assert_eq!(f0.recovered_recorded_drops, 0);
        fanout.finish_frame().unwrap();

        // Missing index 1 (preview-only) before index 2 (recorded): 0 recorded-phase drops
        let f2 = fanout.begin_frame(1, true, Some(1_000_000_000)).unwrap();
        assert!(f2.record);
        assert_eq!(f2.recovered_source_gap, 1);
        assert_eq!(f2.recovered_recorded_drops, 0);
        fanout.finish_frame().unwrap();

        // Missing indices [3, 4] before index 5 (preview-only): index 4 was recorded-phase -> 1 drop!
        let f5 = fanout.begin_frame(2, true, Some(2_500_000_000)).unwrap();
        assert!(!f5.record);
        assert_eq!(f5.recovered_source_gap, 2);
        assert_eq!(f5.recovered_recorded_drops, 1);
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
    fn recording_sink_writes_split_frame_and_imu_artifacts() {
        let root = temp_root("split");
        let mut sink = RecordingSink::create(&root, "session").unwrap();
        let frame_bytes = sink.write_split_frame_index(7, 99, 1234, 2, 3).unwrap();
        let sample = ImuSampleRecord {
            sequence: 1,
            packet_sequence: 2,
            sample_index: 1,
            device_timestamp_raw: 1000,
            device_ticks: 2000,
            host_read_start_ns: 10,
            host_read_end_ns: 20,
            host_monotonic_ns: 15,
            accelerometer: (1, -2, 3),
            gyroscope: (4, 5, -6),
            sync_offset_ns: None,
            sync_residual_ns: Some(100),
            sync_quality: "good",
        };
        let imu_bytes = sink.write_imu_sample_record(&sample).unwrap();
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
            imu_sample_record("session", &sample)
        );
        let roles: Vec<_> = snapshot.artifacts.iter().map(|item| item.role).collect();
        assert_eq!(roles, vec!["frames.index", "imu.samples"]);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn recording_sink_snapshot_reports_counters_without_closing() {
        let root = temp_root("snapshot");
        let mut sink = RecordingSink::create(&root, "session").unwrap();
        let frame_bytes = sink.write_split_frame_index(7, 99, 1234, 2, 3).unwrap();

        let snapshot = sink.snapshot();
        assert_eq!(snapshot.frames_written, 1);
        assert_eq!(snapshot.imu_samples_written, 0);
        assert_eq!(snapshot.bytes_written, frame_bytes);
        assert!(snapshot.artifacts.is_empty());

        let imu_bytes = sink
            .write_imu_sample_record(&ImuSampleRecord {
                sequence: 1,
                packet_sequence: 2,
                sample_index: 1,
                device_timestamp_raw: 1000,
                device_ticks: 2000,
                host_read_start_ns: 10,
                host_read_end_ns: 20,
                host_monotonic_ns: 15,
                accelerometer: (1, -2, 3),
                gyroscope: (4, 5, -6),
                sync_offset_ns: None,
                sync_residual_ns: Some(100),
                sync_quality: "good",
            })
            .unwrap();
        let final_snapshot = sink.flush_and_close().unwrap();
        assert_eq!(final_snapshot.frames_written, 1);
        assert_eq!(final_snapshot.imu_samples_written, 1);
        assert_eq!(final_snapshot.bytes_written, frame_bytes + imu_bytes);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn blocked_imu_writer_does_not_block_frame_acceptance_or_claim_completion() {
        use std::io::Write;
        use std::sync::mpsc;
        use std::time::Duration;
        let root = temp_root("async-isolation");
        let mut sink = RecordingSink::create(&root, "session").unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        sink.imu = super::ArtifactWriter::with_writer(
            fs::OpenOptions::new()
                .write(true)
                .open(root.join("imu.ndjson"))
                .unwrap(),
            "imu.samples",
            "imu.ndjson",
            move |file, payload| {
                entered_tx.send(()).unwrap();
                release_rx
                    .recv_timeout(Duration::from_secs(2))
                    .map_err(std::io::Error::other)?;
                file.write_all(payload)
            },
        )
        .unwrap();
        sink.imu.write(b"imu\n").unwrap();
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        sink.write_split_frame_index(0, 0, 123, 0, 0).unwrap();
        let pending = sink.snapshot();
        release_tx.send(()).unwrap();
        assert_eq!(pending.frames_written, 1);
        assert_eq!(pending.imu_samples_indexed, 0);
        assert!(pending.index_pending_bytes >= 4);
        let final_state = sink.flush_and_close().unwrap();
        assert_eq!(final_state.frames_indexed, 1);
        assert_eq!(final_state.imu_samples_indexed, 1);
        assert_eq!(final_state.index_pending_bytes, 0);
        assert_eq!(fs::read(root.join("imu.ndjson")).unwrap(), b"imu\n");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn asynchronous_index_write_failure_prevents_successful_seal() {
        let root = temp_root("async-error");
        let mut writer = super::ArtifactWriter::with_writer(
            fs::File::create(root.join("frames.ndjson")).unwrap(),
            "frames.index",
            "frames.ndjson",
            |_, _| Err(std::io::Error::other("injected disk error")),
        )
        .unwrap();
        writer.write(b"frame\n").unwrap();
        let error = writer.flush_and_close().unwrap_err();
        assert!(error.message.contains("injected disk error"));
        assert_eq!(
            writer
                .progress
                .records
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn index_pending_byte_limit_includes_inflight_write_and_drains_on_close() {
        use std::io::Write;
        use std::sync::mpsc;
        use std::time::Duration;
        let root = temp_root("async-bound");
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let mut writer = super::ArtifactWriter::with_writer(
            fs::File::create(root.join("frames.ndjson")).unwrap(),
            "frames.index",
            "frames.ndjson",
            move |file, payload| {
                entered_tx.send(()).unwrap();
                release_rx
                    .recv_timeout(Duration::from_secs(2))
                    .map_err(std::io::Error::other)?;
                file.write_all(payload)
            },
        )
        .unwrap();
        writer
            .write(&vec![b'x'; super::INDEX_PENDING_BYTES])
            .unwrap();
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let rejected = writer.write(b"extra");
        release_tx.send(()).unwrap();
        assert!(rejected.unwrap_err().message.contains("backlog exhausted"));
        writer.close();
        assert_eq!(
            fs::metadata(root.join("frames.ndjson")).unwrap().len(),
            super::INDEX_PENDING_BYTES as u64
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn flush_and_close_flushes_both_writers_even_on_first_error_and_marks_closed() {
        let root = temp_root("flush-both");
        let frames = super::ArtifactWriter::with_writer(
            fs::File::create(root.join("frames.ndjson")).unwrap(),
            "frames.index",
            "frames.ndjson",
            |_, _| Err(std::io::Error::other("frames failed")),
        )
        .unwrap();
        let imu = super::ArtifactWriter::create(&root, "imu.samples", "imu.ndjson").unwrap();
        let mut sink = RecordingSink {
            session_id: "session".to_owned(),
            frames,
            imu,
            bytes_written: 0,
            frames_written: 0,
            imu_samples_written: 0,
            closed: false,
        };
        sink.frames.write(b"frame\n").unwrap();
        sink.imu.write(b"imu\n").unwrap();
        let err = sink.flush_and_close().unwrap_err();
        assert!(err.message.contains("frames failed"));
        assert!(sink.closed);
        assert!(sink.imu.worker.is_none());
        assert_eq!(fs::read(root.join("imu.ndjson")).unwrap(), b"imu\n");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn avi1_counter_skips_repeated_ff_fill_bytes() {
        let mut jpeg = vec![0xff, 0xd8, 0xff, 0xff, 0xe0];
        jpeg.extend_from_slice(&33_u16.to_be_bytes());
        jpeg.extend_from_slice(b"AVI1\0\x01\x01\x01\x00\x00");
        jpeg.extend_from_slice(&[0x12, 0x34, 0x56]);
        jpeg.resize(jpeg.len() + 18, 0);
        jpeg.extend_from_slice(&[0xff, 0xd9]);
        assert_eq!(avi1_counter(&jpeg), Some(0x12_3456));
    }
}
