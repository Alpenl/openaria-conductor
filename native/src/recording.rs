use sha2::{Digest, Sha256};
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
        RecordingFrameGate, RecordingSink, imu_sample_record, jpeg_payload, raw_frame_index_record,
        split_frame_index_record,
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
}
