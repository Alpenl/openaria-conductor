pub(crate) mod alsa_ffi;
pub(crate) mod checkpoint;
pub(crate) mod clock;
pub(crate) mod diagnostics;
pub(crate) mod recorder;
pub(crate) mod wav_writer;

pub(crate) use recorder::Recorder;

use diagnostics::WriterDiagnostics;
use std::io;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) const SAMPLE_FORMAT: &str = "S16_LE";
pub(crate) const SAMPLE_CODEC: &str = "pcm_s16le";
pub(crate) const BYTES_PER_SAMPLE: u64 = 2;
pub(crate) const WAV_HEADER_BYTES: u64 = 44;
pub(crate) const DEFAULT_PERIOD_FRAMES: u64 = 1024;
pub(crate) const MAX_CHANNELS: u16 = 8;
// About 32 seconds of PCM: bounded headroom over the observed 19-second write stall.
pub(crate) const QUEUE_BLOCKS: usize = 1536;
// Keep a finite backlog of closed WAV segments, including the in-flight batch.
// At five seconds per segment, 48 slots cover the observed 142-second
// checkpoint stall with finite headroom, without retaining extra PCM buffers.
// No segment is published as durable until the existing syncs have completed.
pub(crate) const MAX_PENDING_COMMITS: u64 = 48;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AudioError {
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

impl AudioError {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub(crate) fn io(code: &'static str, context: &str, error: io::Error) -> Self {
        Self::new(code, format!("{context}: {error}"))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AudioSegment {
    pub(crate) index: u64,
    pub(crate) relative_path: String,
    pub(crate) start_sample: u64,
    pub(crate) end_sample: u64,
    pub(crate) start_time_seconds: f64,
    pub(crate) end_time_seconds: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct AudioRecordingResult {
    pub(crate) capture_clock: serde_json::Value,
    pub(crate) device: String,
    pub(crate) sample_rate_hz: u32,
    pub(crate) channels: u16,
    pub(crate) sample_format: &'static str,
    pub(crate) codec: &'static str,
    pub(crate) container: &'static str,
    pub(crate) sample_count: u64,
    pub(crate) started_monotonic_ns: u64,
    pub(crate) stopped_monotonic_ns: u64,
    pub(crate) segments: Vec<AudioSegment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AudioRecordingSnapshot {
    pub(crate) sample_count: u64,
    pub(crate) bytes_written: u64,
    pub(crate) segment_count: u64,
}

#[derive(Default)]
pub(crate) struct AudioProgress {
    pub(crate) error: Mutex<Option<AudioError>>,
    pub(crate) commit_error: Mutex<Option<AudioError>>,
    pub(crate) queued_frames: AtomicU64,
    pub(crate) queue_peak_frames: AtomicU64,
    pub(crate) max_write_ns: AtomicU64,
    pub(crate) pending_commits: AtomicU64,
    pub(crate) peak_pending_commits: AtomicU64,
    pub(crate) durable_samples: AtomicU64,
    pub(crate) sample_count: AtomicU64,
    pub(crate) bytes_written: AtomicU64,
    pub(crate) segment_count: AtomicU64,
    pub(crate) writer_diagnostics: Mutex<WriterDiagnostics>,
    pub(crate) clock_checkpoint: Mutex<Option<ClockCheckpoint>>,
}

#[derive(Clone)]
pub(crate) struct ClockCheckpoint {
    pub(crate) started: u64,
    pub(crate) period_frames: u64,
    pub(crate) buffer_frames: u64,
    pub(crate) base_index: usize,
    pub(crate) anchors: Vec<clock::Anchor>,
    pub(crate) last_anchor: Option<clock::Anchor>,
}

impl AudioProgress {
    pub(crate) fn check_commit(&self) -> Result<(), AudioError> {
        match &*self
            .commit_error
            .lock()
            .map_err(|_| AudioError::new("audio_failed", "audio commit error mutex poisoned"))?
        {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    pub(crate) fn fail(&self, error: AudioError) {
        if let Ok(mut first) = self.error.lock() {
            first.get_or_insert(error);
        }
    }

    pub(crate) fn check(&self) -> Result<(), AudioError> {
        let error = self
            .error
            .lock()
            .map_err(|_| AudioError::new("audio_failed", "audio error mutex poisoned"))?;
        match &*error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    pub(crate) fn publish(&self, snapshot: &AudioRecordingSnapshot) {
        self.sample_count
            .store(snapshot.sample_count, Ordering::Release);
        self.bytes_written
            .store(snapshot.bytes_written, Ordering::Release);
        self.segment_count
            .store(snapshot.segment_count, Ordering::Release);
    }

    pub(crate) fn snapshot(&self) -> AudioRecordingSnapshot {
        AudioRecordingSnapshot {
            sample_count: self.sample_count.load(Ordering::Acquire),
            bytes_written: self.bytes_written.load(Ordering::Acquire),
            segment_count: self.segment_count.load(Ordering::Acquire),
        }
    }
}

pub(crate) fn available() -> bool {
    alsa_ffi::available()
}
