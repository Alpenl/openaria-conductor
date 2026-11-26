use crate::audio::checkpoint::{CheckpointJournal, commit_checkpoint};
use crate::audio::diagnostics::{IoOperation, measure_io};
use crate::audio::recorder::RecorderConfig;
use crate::audio::{
    AudioError, AudioProgress, AudioRecordingSnapshot, AudioSegment, BYTES_PER_SAMPLE,
    MAX_PENDING_COMMITS, WAV_HEADER_BYTES,
};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};

pub(crate) struct PendingSegment {
    pub(crate) file: File,
    pub(crate) record: AudioSegment,
}

pub(crate) struct CommitWorker {
    sender: Option<mpsc::SyncSender<PendingSegment>>,
    worker: Option<JoinHandle<Result<(), AudioError>>>,
    progress: Arc<AudioProgress>,
}

impl CommitWorker {
    pub(crate) fn spawn(
        progress: Arc<AudioProgress>,
        mut commit: impl FnMut(Vec<PendingSegment>) -> Result<(), AudioError> + Send + 'static,
    ) -> Result<Self, AudioError> {
        let (sender, receiver) = mpsc::sync_channel::<PendingSegment>(MAX_PENDING_COMMITS as usize);
        let state = Arc::clone(&progress);
        let worker = thread::Builder::new()
            .name("audio-commit".to_owned())
            .spawn(move || {
                for first in &receiver {
                    let mut batch = vec![first];
                    // Coalesce accumulated commits, but never wait for a new segment.
                    batch.extend(receiver.try_iter().take(MAX_PENDING_COMMITS as usize - 1));
                    let count = batch.len() as u64;
                    if let Err(error) = commit(batch) {
                        *state
                            .commit_error
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(error.clone());
                        state.fail(error.clone());
                        return Err(error);
                    }
                    state.pending_commits.fetch_sub(count, Ordering::AcqRel);
                }
                Ok(())
            })
            .map_err(|error| {
                AudioError::new(
                    "audio_failed",
                    format!("spawn audio commit worker: {error}"),
                )
            })?;
        Ok(Self {
            sender: Some(sender),
            worker: Some(worker),
            progress,
        })
    }

    pub(crate) fn submit(&self, segment: PendingSegment) -> Result<(), AudioError> {
        self.progress.check_commit()?;
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| AudioError::new("audio_failed", "audio commit worker is closed"))?;
        let previous = crate::sys::try_reserve_bounded_u64(
            &self.progress.pending_commits,
            1,
            MAX_PENDING_COMMITS,
        )
        .ok_or_else(|| AudioError::new("audio_failed", "audio durable commit backlog exhausted"))?;
        self.progress
            .peak_pending_commits
            .fetch_max(previous + 1, Ordering::Relaxed);
        if sender.try_send(segment).is_err() {
            self.progress.pending_commits.fetch_sub(1, Ordering::AcqRel);
            self.progress.check_commit()?;
            return Err(AudioError::new(
                "audio_failed",
                "audio commit worker disconnected",
            ));
        }
        Ok(())
    }

    pub(crate) fn finish(&mut self) -> Result<(), AudioError> {
        self.sender.take();
        match self.worker.take() {
            Some(worker) => worker
                .join()
                .map_err(|_| AudioError::new("audio_failed", "audio commit worker panicked"))?,
            None => self.progress.check_commit(),
        }
    }
}

impl Drop for CommitWorker {
    fn drop(&mut self) {
        // Also drain on an earlier writer error; never detach a file writer that
        // recovery or sealing could race. Explicit finish propagates its error.
        if let Err(error) = self.finish() {
            self.progress.fail(error);
        }
    }
}

pub(crate) struct SegmentWriter {
    root: PathBuf,
    sample_rate_hz: u32,
    channels: u16,
    bytes_per_frame: u64,
    segment_frames: u64,
    next_index: u64,
    active: Option<ActiveSegment>,
    records: Vec<AudioSegment>,
    // Accepted by the commit queue, not necessarily durable yet.
    completed_snapshot: AudioRecordingSnapshot,
    progress: Arc<AudioProgress>,
    committer: CommitWorker,
}

struct ActiveSegment {
    index: u64,
    relative_path: String,
    file: File,
    start_sample: u64,
    data_bytes: u64,
}

impl SegmentWriter {
    #[cfg(test)]
    pub(crate) fn new(config: &RecorderConfig) -> Result<Self, AudioError> {
        Self::with_progress(config, Arc::new(AudioProgress::default()))
    }

    pub(crate) fn with_progress(
        config: &RecorderConfig,
        progress: Arc<AudioProgress>,
    ) -> Result<Self, AudioError> {
        let segment_frames = (config.sample_rate_hz as f64 * config.segment_seconds).round();
        if !segment_frames.is_finite() || segment_frames < 1.0 || segment_frames > u64::MAX as f64 {
            return Err(AudioError::new(
                "invalid_argument",
                "audio segment duration is invalid",
            ));
        }
        let bytes_per_frame = u64::from(config.channels) * BYTES_PER_SAMPLE;
        let root = config.session_root.clone();
        let state = Arc::clone(&progress);
        let mut durable_records = Vec::new();
        let mut journal = CheckpointJournal::default();
        let committer = CommitWorker::spawn(Arc::clone(&progress), move |batch| {
            for pending in batch {
                measure_io(
                    &state,
                    IoOperation::Sync,
                    (pending.record.end_sample - pending.record.start_sample) * bytes_per_frame,
                    || pending.file.sync_all(),
                )
                .map_err(|error| AudioError::io("write_failed", "sync audio segment", error))?;
                durable_records.push(pending.record);
            }
            measure_io(&state, IoOperation::Checkpoint, 0, || {
                commit_checkpoint(&root, &state, &durable_records, &mut journal)
            })?;
            state.durable_samples.store(
                durable_records.last().map_or(0, |record| record.end_sample),
                Ordering::Release,
            );
            Ok(())
        })?;
        Ok(Self {
            root: config.session_root.clone(),
            sample_rate_hz: config.sample_rate_hz,
            channels: config.channels,
            bytes_per_frame,
            segment_frames: segment_frames as u64,
            next_index: 0,
            active: None,
            records: Vec::new(),
            completed_snapshot: AudioRecordingSnapshot {
                sample_count: 0,
                bytes_written: 0,
                segment_count: 0,
            },
            progress,
            committer,
        })
    }

    pub(crate) fn snapshot(&self) -> AudioRecordingSnapshot {
        let AudioRecordingSnapshot {
            mut sample_count,
            mut bytes_written,
            mut segment_count,
        } = self.completed_snapshot.clone();
        if let Some(active) = &self.active {
            if active.data_bytes > 0 {
                let active_samples = active.data_bytes / self.bytes_per_frame;
                sample_count = sample_count.max(active.start_sample + active_samples);
                bytes_written += WAV_HEADER_BYTES + active.data_bytes;
                segment_count += 1;
            }
        }
        AudioRecordingSnapshot {
            sample_count,
            bytes_written,
            segment_count,
        }
    }

    fn publish_snapshot(&self) {
        self.progress.publish(&self.snapshot());
    }

    pub(crate) fn write_frames(
        &mut self,
        data: &[u8],
        frames: u64,
        total_written_before: &mut u64,
    ) -> Result<(), AudioError> {
        // A capture failure must still drain buffers accepted before the fault.
        // Only a failed durable commit prevents further successful disk writes.
        self.progress.check_commit()?;
        let mut remaining = frames;
        let mut frame_offset = 0_u64;
        while remaining > 0 {
            if self.active.is_none() {
                self.open_segment(*total_written_before)?;
            }
            let segment_written = self
                .active
                .as_ref()
                .map(|segment| segment.data_bytes / self.bytes_per_frame)
                .unwrap_or(0);
            let capacity = self.segment_frames - segment_written;
            let selected = remaining.min(capacity);
            let byte_start = usize::try_from(frame_offset * self.bytes_per_frame)
                .map_err(|_| AudioError::new("audio_failed", "audio buffer offset overflow"))?;
            let byte_len = usize::try_from(selected * self.bytes_per_frame)
                .map_err(|_| AudioError::new("audio_failed", "audio buffer length overflow"))?;
            let byte_end = byte_start + byte_len;
            let active = self
                .active
                .as_mut()
                .ok_or_else(|| AudioError::new("invalid_state", "audio segment was not opened"))?;
            measure_io(&self.progress, IoOperation::Write, byte_len as u64, || {
                active.file.write_all(&data[byte_start..byte_end])
            })
            .map_err(|error| AudioError::io("write_failed", "write audio segment", error))?;
            active.data_bytes += selected * self.bytes_per_frame;
            *total_written_before += selected;
            remaining -= selected;
            frame_offset += selected;
            if active.data_bytes / self.bytes_per_frame == self.segment_frames {
                self.finish_active()?;
            }
        }
        self.publish_snapshot();
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<Vec<AudioSegment>, AudioError> {
        if self.active.is_some() {
            self.finish_active()?;
        }
        self.committer.finish()?;
        Ok(self.records)
    }

    fn open_segment(&mut self, start_sample: u64) -> Result<(), AudioError> {
        let relative_path = format!("audio/audio_{:05}.wav", self.next_index);
        let absolute = self.root.join(&relative_path);
        if let Some(parent) = absolute.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| AudioError::io("write_failed", "create audio directory", error))?;
        }
        let mut file = measure_io(&self.progress, IoOperation::Open, 0, || {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&absolute)
        })
        .map_err(|error| AudioError::io("write_failed", "open audio segment", error))?;
        write_wav_header(&mut file, self.sample_rate_hz, self.channels, 0)?;
        self.active = Some(ActiveSegment {
            index: self.next_index,
            relative_path,
            file,
            start_sample,
            data_bytes: 0,
        });
        self.next_index += 1;
        Ok(())
    }

    fn finish_active(&mut self) -> Result<(), AudioError> {
        let mut segment = self
            .active
            .take()
            .ok_or_else(|| AudioError::new("invalid_state", "audio segment is missing"))?;
        if segment.data_bytes == 0 {
            return Ok(());
        }
        measure_io(&self.progress, IoOperation::Seal, 0, || {
            segment
                .file
                .set_len(WAV_HEADER_BYTES + segment.data_bytes)
                .map_err(|error| {
                    AudioError::io("write_failed", "truncate incomplete audio write", error)
                })?;
            write_wav_header(
                &mut segment.file,
                self.sample_rate_hz,
                self.channels,
                segment.data_bytes,
            )
        })?;
        let end_sample = segment.start_sample + segment.data_bytes / self.bytes_per_frame;
        let record = AudioSegment {
            index: segment.index,
            relative_path: segment.relative_path,
            start_sample: segment.start_sample,
            end_sample,
            start_time_seconds: segment.start_sample as f64 / f64::from(self.sample_rate_hz),
            end_time_seconds: end_sample as f64 / f64::from(self.sample_rate_hz),
        };
        self.committer.submit(PendingSegment {
            file: segment.file,
            record: record.clone(),
        })?;
        // Update only after successful submission so rejected tails do not
        // inflate the completed inventory. Durable progress stays independent.
        self.completed_snapshot.sample_count =
            self.completed_snapshot.sample_count.max(record.end_sample);
        self.completed_snapshot.bytes_written += WAV_HEADER_BYTES + segment.data_bytes;
        self.completed_snapshot.segment_count += 1;
        self.records.push(record);
        self.publish_snapshot();
        Ok(())
    }
}

pub(crate) fn write_wav_header(
    file: &mut File,
    sample_rate_hz: u32,
    channels: u16,
    data_bytes: u64,
) -> Result<(), AudioError> {
    let data_size = u32::try_from(data_bytes)
        .map_err(|_| AudioError::new("audio_too_large", "WAV segment exceeds 4 GiB"))?;
    let riff_size = 36_u32
        .checked_add(data_size)
        .ok_or_else(|| AudioError::new("audio_too_large", "WAV RIFF size overflow"))?;
    let bits_per_sample = u16::try_from(BYTES_PER_SAMPLE * 8)
        .map_err(|_| AudioError::new("invalid_argument", "invalid sample width"))?;
    let bytes_per_sample = u16::try_from(BYTES_PER_SAMPLE)
        .map_err(|_| AudioError::new("invalid_argument", "invalid sample width"))?;
    let block_align = channels
        .checked_mul(bytes_per_sample)
        .ok_or_else(|| AudioError::new("invalid_argument", "WAV block align overflow"))?;
    let byte_rate = sample_rate_hz
        .checked_mul(u32::from(block_align))
        .ok_or_else(|| AudioError::new("invalid_argument", "WAV byte rate overflow"))?;
    let mut header = Vec::with_capacity(44);
    header.extend_from_slice(b"RIFF");
    header.extend_from_slice(&riff_size.to_le_bytes());
    header.extend_from_slice(b"WAVE");
    header.extend_from_slice(b"fmt ");
    header.extend_from_slice(&16_u32.to_le_bytes());
    header.extend_from_slice(&1_u16.to_le_bytes());
    header.extend_from_slice(&channels.to_le_bytes());
    header.extend_from_slice(&sample_rate_hz.to_le_bytes());
    header.extend_from_slice(&byte_rate.to_le_bytes());
    header.extend_from_slice(&block_align.to_le_bytes());
    header.extend_from_slice(&bits_per_sample.to_le_bytes());
    header.extend_from_slice(b"data");
    header.extend_from_slice(&data_size.to_le_bytes());
    file.seek(SeekFrom::Start(0))
        .map_err(|error| AudioError::io("write_failed", "seek WAV header", error))?;
    file.write_all(&header)
        .map_err(|error| AudioError::io("write_failed", "write WAV header", error))?;
    if data_bytes == 0 {
        file.seek(SeekFrom::Start(44))
            .map_err(|error| AudioError::io("write_failed", "seek WAV data", error))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audio::ClockCheckpoint;
    use std::io::Read;

    fn anchor(sample: u64, time: u64) -> crate::audio::clock::Anchor {
        [sample, time, time, time, time]
    }

    fn tempfile_dir() -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "rp-ylx-audio-wav-test-{}-{}",
            std::process::id(),
            unique
        ));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn stalled_commit_does_not_block_pcm_writes_and_backlog_is_bounded() {
        let root = tempfile_dir();
        let config = RecorderConfig {
            session_root: root.clone(),
            device: "hw:0,0".to_owned(),
            sample_rate_hz: 10,
            channels: 2,
            segment_seconds: 0.3,
        };
        let progress = Arc::new(AudioProgress::default());
        let mut writer = SegmentWriter::with_progress(&config, progress.clone()).unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let committed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = committed.clone();
        let mut first = true;
        writer.committer = CommitWorker::spawn(progress.clone(), move |batch| {
            if first {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                first = false;
            }
            for pending in batch {
                observed.lock().unwrap().push(pending.record.index);
            }
            Ok(())
        })
        .unwrap();
        let mut written = 0;
        writer.write_frames(&[7; 12], 3, &mut written).unwrap();
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        for _ in 1..30 {
            writer.write_frames(&[7; 12], 3, &mut written).unwrap();
        }
        assert_eq!(progress.pending_commits.load(Ordering::Acquire), 30);
        for _ in 30..MAX_PENDING_COMMITS {
            writer.write_frames(&[7; 12], 3, &mut written).unwrap();
        }
        assert_eq!(
            progress.pending_commits.load(Ordering::Acquire),
            MAX_PENDING_COMMITS
        );
        let error = writer.write_frames(&[7; 12], 3, &mut written).unwrap_err();
        assert!(error.message.contains("durable commit backlog exhausted"));
        release_tx.send(()).unwrap();
        drop(writer);
        assert_eq!(
            *committed.lock().unwrap(),
            (0..MAX_PENDING_COMMITS).collect::<Vec<_>>()
        );
        assert_eq!(progress.pending_commits.load(Ordering::Acquire), 0);
        assert_eq!(
            progress.peak_pending_commits.load(Ordering::Acquire),
            MAX_PENDING_COMMITS
        );
        assert!(!root.join("audio/checkpoint.json").exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn capture_failure_still_drains_previously_accepted_pcm() {
        let root = tempfile_dir();
        let config = RecorderConfig {
            session_root: root.clone(),
            device: "hw:0,0".to_owned(),
            sample_rate_hz: 10,
            channels: 2,
            segment_seconds: 0.3,
        };
        let progress = Arc::new(AudioProgress::default());
        let mut writer = SegmentWriter::with_progress(&config, progress.clone()).unwrap();
        let mut written = 0;
        writer.write_frames(&[7; 12], 3, &mut written).unwrap();
        progress.fail(AudioError::new("audio_failed", "capture disconnected"));
        writer.write_frames(&[9; 12], 3, &mut written).unwrap();
        let records = writer.finish().unwrap();
        assert_eq!(written, 6);
        assert_eq!(records.last().unwrap().end_sample, 6);
        assert_eq!(
            &std::fs::read(root.join("audio/audio_00001.wav")).unwrap()[44..],
            &[9; 12]
        );
        assert_eq!(
            progress.check().unwrap_err().message,
            "capture disconnected"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_async_checkpoint_is_reported_and_never_claims_durable_samples() {
        let root = tempfile_dir();
        std::fs::create_dir_all(root.join("audio/checkpoint.tmp")).unwrap();
        let config = RecorderConfig {
            session_root: root.clone(),
            device: "hw:0,0".to_owned(),
            sample_rate_hz: 10,
            channels: 2,
            segment_seconds: 0.3,
        };
        let progress = Arc::new(AudioProgress::default());
        *progress.clock_checkpoint.lock().unwrap() = Some(ClockCheckpoint {
            started: 1_000_000_000,
            period_frames: 1,
            buffer_frames: 8,
            base_index: 0,
            anchors: vec![anchor(1, 1_100_000_000)],
            last_anchor: Some(anchor(3, 1_300_000_000)),
        });
        let mut writer = SegmentWriter::with_progress(&config, progress.clone()).unwrap();
        writer.write_frames(&[7; 12], 3, &mut 0).unwrap();
        let error = writer.finish().unwrap_err();
        assert!(error.message.contains("create audio checkpoint"));
        assert!(progress.check().is_err());
        assert_eq!(progress.durable_samples.load(Ordering::Acquire), 0);
        assert_eq!(
            progress
                .writer_diagnostics
                .lock()
                .unwrap()
                .checkpoint
                .failures,
            1
        );
        assert!(!root.join("audio/checkpoint.json").exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn wav_header_records_pcm_shape() {
        let directory = tempfile_dir();
        let path = directory.join("audio.wav");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .read(true)
            .open(&path)
            .unwrap();
        write_wav_header(&mut file, 48_000, 2, 192_000).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut header = [0_u8; 44];
        file.read_exact(&mut header).unwrap();
        assert_eq!(&header[0..4], b"RIFF");
        assert_eq!(&header[8..12], b"WAVE");
        assert_eq!(u16::from_le_bytes(header[20..22].try_into().unwrap()), 1);
        assert_eq!(u16::from_le_bytes(header[22..24].try_into().unwrap()), 2);
        assert_eq!(
            u32::from_le_bytes(header[24..28].try_into().unwrap()),
            48_000
        );
        assert_eq!(u16::from_le_bytes(header[34..36].try_into().unwrap()), 16);
        assert_eq!(&header[36..40], b"data");
        assert_eq!(
            u32::from_le_bytes(header[40..44].try_into().unwrap()),
            192_000
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn committed_audio_checkpoint_survives_without_finishing_recorder() {
        let root = tempfile_dir();
        let config = RecorderConfig {
            session_root: root.clone(),
            device: "hw:0,0".to_owned(),
            sample_rate_hz: 10,
            channels: 2,
            segment_seconds: 0.3,
        };
        let progress = Arc::new(AudioProgress::default());
        *progress.clock_checkpoint.lock().unwrap() = Some(ClockCheckpoint {
            started: 1_000_000_000,
            period_frames: 1,
            buffer_frames: 8,
            base_index: 0,
            anchors: vec![anchor(1, 1_100_000_000)],
            last_anchor: Some(anchor(5, 1_500_000_000)),
        });
        let mut writer = SegmentWriter::with_progress(&config, progress.clone()).unwrap();
        let mut written = 0;
        writer.write_frames(&[7; 20], 5, &mut written).unwrap();
        drop(writer);
        let head: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join("audio/checkpoint.json")).unwrap())
                .unwrap();
        assert_eq!(head["sample_count"], 3);
        assert_eq!(head["segment_count"], 1);
        assert_eq!(head["last_anchor"][0], 5);
        let wav = std::fs::read(root.join("audio/audio_00000.wav")).unwrap();
        assert_eq!(u32::from_le_bytes(wav[40..44].try_into().unwrap()), 12);
        assert_eq!(&wav[44..], &[7; 12]);
        assert_eq!(progress.durable_samples.load(Ordering::Acquire), 3);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn segment_writer_splits_on_sample_boundaries() {
        let root = tempfile_dir();
        let config = RecorderConfig {
            session_root: root.clone(),
            device: "hw:0,0".to_owned(),
            sample_rate_hz: 10,
            channels: 2,
            segment_seconds: 0.3,
        };
        let mut writer = SegmentWriter::new(&config).unwrap();
        let mut total = 0_u64;
        let payload = vec![1_u8; 5 * 2 * 2];
        writer.write_frames(&payload, 5, &mut total).unwrap();
        let records = writer.finish().unwrap();
        assert_eq!(total, 5);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].start_sample, 0);
        assert_eq!(records[0].end_sample, 3);
        assert_eq!(records[1].start_sample, 3);
        assert_eq!(records[1].end_sample, 5);
        assert!(root.join("audio/audio_00000.wav").is_file());
        assert!(root.join("audio/audio_00001.wav").is_file());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn segment_writer_snapshot_counts_active_wav_bytes_without_closing() {
        let root = tempfile_dir();
        let config = RecorderConfig {
            session_root: root.clone(),
            device: "hw:0,0".to_owned(),
            sample_rate_hz: 10,
            channels: 2,
            segment_seconds: 1.0,
        };
        let mut writer = SegmentWriter::new(&config).unwrap();
        let mut total = 0_u64;
        let payload = vec![1_u8; 2 * 2 * 2];

        writer.write_frames(&payload, 2, &mut total).unwrap();
        let snapshot = writer.snapshot();

        assert_eq!(snapshot.sample_count, 2);
        assert_eq!(snapshot.bytes_written, 44 + 2 * 2 * 2);
        assert_eq!(snapshot.segment_count, 1);
        drop(writer);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn writer_progress_counts_boundaries_partial_tail_and_empty_tail() {
        let root = tempfile_dir();
        let config = RecorderConfig {
            session_root: root.clone(),
            device: "unused".to_owned(),
            sample_rate_hz: 10,
            channels: 2,
            segment_seconds: 0.3,
        };
        let mut writer = SegmentWriter::new(&config).unwrap();
        let mut total = 0;
        for (frames, samples, segments, bytes) in [
            (0, 0, 0, 0),
            (2, 2, 1, 52),
            (1, 3, 1, 56),
            (4, 7, 3, 160),
            (0, 7, 3, 160),
            (2, 9, 3, 168),
        ] {
            writer
                .write_frames(&vec![7; frames * 4], frames as u64, &mut total)
                .unwrap();
            let expected = AudioRecordingSnapshot {
                sample_count: samples,
                segment_count: segments,
                bytes_written: bytes,
            };
            assert_eq!(writer.snapshot(), expected);
            assert_eq!(writer.progress.snapshot(), expected);
        }
        writer.open_segment(total).unwrap();
        assert_eq!(writer.snapshot().segment_count, 3);
        writer.finish_active().unwrap();
        assert_eq!(writer.snapshot().bytes_written, 168);
        let records = writer.finish().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records.last().unwrap().end_sample, 9);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn writer_progress_does_not_count_tail_rejected_by_commit_worker() {
        let root = tempfile_dir();
        let config = RecorderConfig {
            session_root: root.clone(),
            device: "unused".to_owned(),
            sample_rate_hz: 10,
            channels: 2,
            segment_seconds: 0.3,
        };
        let mut writer = SegmentWriter::new(&config).unwrap();
        writer.write_frames(&[7; 20], 5, &mut 0).unwrap();
        assert_eq!(writer.snapshot().sample_count, 5);
        *writer.progress.commit_error.lock().unwrap() =
            Some(AudioError::new("write_failed", "injected rejected commit"));
        assert!(
            writer
                .finish_active()
                .unwrap_err()
                .message
                .contains("injected")
        );
        assert_eq!(
            writer.snapshot(),
            AudioRecordingSnapshot {
                sample_count: 3,
                segment_count: 1,
                bytes_written: 56,
            }
        );
        assert_eq!(writer.records.len(), 1);
        drop(writer);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn writer_progress_counts_writes_independently_of_sample_position() {
        let root = tempfile_dir();
        let config = RecorderConfig {
            session_root: root.clone(),
            device: "unused".to_owned(),
            sample_rate_hz: 10,
            channels: 1,
            segment_seconds: 0.2,
        };
        let mut writer = SegmentWriter::new(&config).unwrap();
        writer.write_frames(&[7; 4], 2, &mut 100).unwrap();
        writer.write_frames(&[7; 4], 2, &mut 0).unwrap();
        assert_eq!(
            writer.snapshot(),
            AudioRecordingSnapshot {
                sample_count: 102,
                segment_count: 2,
                bytes_written: 96,
            }
        );
        writer.finish().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn submit_on_closed_commit_worker_returns_error_instead_of_panicking() {
        let progress = Arc::new(AudioProgress::default());
        let mut worker = CommitWorker::spawn(progress, |_| Ok(())).unwrap();
        worker.finish().unwrap();
        let root = tempfile_dir();
        let file = File::create(root.join("dummy.wav")).unwrap();
        let err = worker
            .submit(PendingSegment {
                file,
                record: AudioSegment {
                    index: 0,
                    relative_path: "audio/audio_00000.wav".to_owned(),
                    start_sample: 0,
                    end_sample: 1,
                    start_time_seconds: 0.0,
                    end_time_seconds: 0.1,
                },
            })
            .unwrap_err();
        assert_eq!(err.code, "audio_failed");
        std::fs::remove_dir_all(root).unwrap();
    }
}
