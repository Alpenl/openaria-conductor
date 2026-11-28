pub mod alsa_ffi;
pub mod checkpoint;
pub mod clock;
pub mod diagnostics;
pub mod recorder;
pub mod wav_writer;

pub use recorder::{Recorder, SyncedAudioCapture, SyncedAudioSummary};

use diagnostics::WriterDiagnostics;
use std::io;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

pub const SAMPLE_FORMAT: &str = "S16_LE";
pub const SAMPLE_CODEC: &str = "pcm_s16le";
pub const BYTES_PER_SAMPLE: u64 = 2;
pub const WAV_HEADER_BYTES: u64 = 44;
pub const DEFAULT_PERIOD_FRAMES: u64 = 1024;
pub const MAX_CHANNELS: u16 = 8;
// About 32 seconds of PCM: bounded headroom over the observed 19-second write stall.
pub const QUEUE_BLOCKS: usize = 1536;
// Keep a finite backlog of closed WAV segments, including the in-flight batch.
// At five seconds per segment, 48 slots cover the observed 142-second
// checkpoint stall with finite headroom, without retaining extra PCM buffers.
// No segment is published as durable until the existing syncs have completed.
pub const MAX_PENDING_COMMITS: u64 = 48;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioError {
    pub code: &'static str,
    pub message: String,
}

impl AudioError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn io(code: &'static str, context: &str, error: io::Error) -> Self {
        Self::new(code, format!("{context}: {error}"))
    }
}

#[derive(Debug, Clone)]
pub struct AudioSegment {
    pub index: u64,
    pub relative_path: String,
    pub start_sample: u64,
    pub end_sample: u64,
    pub start_time_seconds: f64,
    pub end_time_seconds: f64,
    pub bytes: u64,
    pub sha256: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AudioRecordingResult {
    pub capture_clock: serde_json::Value,
    pub device: String,
    pub sample_rate_hz: u32,
    pub channels: u16,
    pub sample_format: &'static str,
    pub codec: &'static str,
    pub container: &'static str,
    pub sample_count: u64,
    pub started_monotonic_ns: u64,
    pub stopped_monotonic_ns: u64,
    pub segments: Vec<AudioSegment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioRecordingSnapshot {
    pub sample_count: u64,
    pub bytes_written: u64,
    pub segment_count: u64,
}

#[derive(Default)]
pub struct AudioProgress {
    pub error: Mutex<Option<AudioError>>,
    pub commit_error: Mutex<Option<AudioError>>,
    pub queued_frames: AtomicU64,
    pub queue_peak_frames: AtomicU64,
    pub max_write_ns: AtomicU64,
    pub pending_commits: AtomicU64,
    pub peak_pending_commits: AtomicU64,
    pub durable_samples: AtomicU64,
    pub sample_count: AtomicU64,
    pub bytes_written: AtomicU64,
    pub segment_count: AtomicU64,
    pub writer_diagnostics: Mutex<WriterDiagnostics>,
    pub clock_checkpoint: Mutex<Option<ClockCheckpoint>>,
}

#[derive(Clone)]
pub struct ClockCheckpoint {
    pub started: u64,
    pub period_frames: u64,
    pub buffer_frames: u64,
    pub base_index: usize,
    pub anchors: Vec<clock::Anchor>,
    pub last_anchor: Option<clock::Anchor>,
}

impl AudioProgress {
    pub fn check_commit(&self) -> Result<(), AudioError> {
        match &*self
            .commit_error
            .lock()
            .map_err(|_| AudioError::new("audio_failed", "audio commit error mutex poisoned"))?
        {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    pub fn fail(&self, error: AudioError) {
        if let Ok(mut first) = self.error.lock() {
            first.get_or_insert(error);
        }
    }

    pub fn check(&self) -> Result<(), AudioError> {
        let error = self
            .error
            .lock()
            .map_err(|_| AudioError::new("audio_failed", "audio error mutex poisoned"))?;
        match &*error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    pub fn publish(&self, snapshot: &AudioRecordingSnapshot) {
        self.sample_count
            .store(snapshot.sample_count, Ordering::Release);
        self.bytes_written
            .store(snapshot.bytes_written, Ordering::Release);
        self.segment_count
            .store(snapshot.segment_count, Ordering::Release);
    }

    pub fn snapshot(&self) -> AudioRecordingSnapshot {
        AudioRecordingSnapshot {
            sample_count: self.sample_count.load(Ordering::Acquire),
            bytes_written: self.bytes_written.load(Ordering::Acquire),
            segment_count: self.segment_count.load(Ordering::Acquire),
        }
    }
}

pub fn available() -> bool {
    alsa_ffi::available()
}
