use crate::active_take::{self, ActiveTakeSnapshot, ActiveTakeWriter};
use crate::audio::{self, AudioRecordingResult, AudioRecordingSnapshot, Recorder};
use crate::recording::{
    self, RecordingSegmentPlanner, RecordingSink, RecordingSinkSnapshot, SegmentBoundary,
    SegmentPlannerSnapshot,
};
use crate::stereo_encoder::{self, EncoderProcess, SegmentEvent};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoreError {
    pub(crate) code: String,
    pub(crate) message: String,
}

impl StoreError {
    fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    fn poisoned(resource: &str) -> Self {
        Self::new(
            "session_transaction_poisoned",
            format!("session transaction {resource} mutex is poisoned"),
        )
    }
}

impl From<active_take::ActiveTakeError> for StoreError {
    fn from(error: active_take::ActiveTakeError) -> Self {
        Self::new(error.code, error.message)
    }
}

impl From<audio::AudioError> for StoreError {
    fn from(error: audio::AudioError) -> Self {
        Self::new(error.code, error.message)
    }
}

impl From<recording::RecordingError> for StoreError {
    fn from(error: recording::RecordingError) -> Self {
        Self::new(error.code, error.message)
    }
}

impl From<stereo_encoder::EncoderProcessError> for StoreError {
    fn from(error: stereo_encoder::EncoderProcessError) -> Self {
        Self::new(error.code, error.message)
    }
}

pub(crate) struct AudioPlan<'a> {
    pub(crate) device: &'a str,
    pub(crate) sample_rate_hz: u32,
    pub(crate) channels: u16,
    pub(crate) segment_seconds: f64,
}

pub(crate) struct RecordingPlan<'a> {
    pub(crate) session_root: &'a Path,
    pub(crate) session_id: &'a str,
    pub(crate) encoder_executable: &'a Path,
    pub(crate) width: u64,
    pub(crate) height: u64,
    pub(crate) fps: u64,
    pub(crate) bitrate_kbps: u64,
    pub(crate) segment_frames: u64,
    pub(crate) recording_start_monotonic_ns: u64,
    pub(crate) audio: Option<AudioPlan<'a>>,
}

#[derive(Debug, Clone)]
pub(crate) struct TransactionSnapshot {
    pub(crate) state: &'static str,
    pub(crate) active_take: ActiveTakeSnapshot,
    pub(crate) sink: RecordingSinkSnapshot,
    pub(crate) audio: Option<AudioRecordingSnapshot>,
    pub(crate) segments: Vec<SegmentEvent>,
    pub(crate) submitted_frames: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct RecordingOutcome {
    pub(crate) active_take: ActiveTakeSnapshot,
    pub(crate) sink: RecordingSinkSnapshot,
    pub(crate) audio: Option<AudioRecordingResult>,
    pub(crate) segments: Vec<SegmentEvent>,
    pub(crate) encoder_stats: BTreeMap<String, i64>,
    pub(crate) planner: SegmentPlannerSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    Recording,
    Finishing,
    Finished,
    Aborted,
}

struct State {
    lifecycle: Lifecycle,
    registered_segments: usize,
    outcome: Option<RecordingOutcome>,
}

pub(crate) struct SessionTransaction {
    pub(crate) active_take: Arc<Mutex<ActiveTakeWriter>>,
    pub(crate) sink: Arc<Mutex<RecordingSink>>,
    pub(crate) encoder: Arc<Mutex<EncoderProcess>>,
    pub(crate) segment_planner: Arc<Mutex<RecordingSegmentPlanner>>,
    pub(crate) recording_start_monotonic_ns: u64,
    audio: Option<Arc<Recorder>>,
    state: Mutex<State>,
}

impl SessionTransaction {
    pub(crate) fn begin(plan: RecordingPlan<'_>) -> Result<Self, StoreError> {
        if plan.recording_start_monotonic_ns == 0 {
            return Err(StoreError::new(
                "invalid_argument",
                "recording start monotonic timestamp must be positive",
            ));
        }
        let active_take = Arc::new(Mutex::new(ActiveTakeWriter::new(plan.session_id)?));
        let sink = Arc::new(Mutex::new(RecordingSink::create(
            plan.session_root,
            plan.session_id,
        )?));
        let segment_planner = Arc::new(Mutex::new(RecordingSegmentPlanner::new(
            plan.segment_frames,
        )?));
        let mut encoder = EncoderProcess::new(
            &plan.session_root.join("video"),
            plan.encoder_executable,
            plan.width,
            plan.height,
            plan.fps,
            plan.bitrate_kbps,
            plan.segment_frames,
            "video/",
        )?;
        if let Err(error) = encoder.start() {
            if let Ok(mut sink) = sink.lock() {
                sink.close();
            }
            return Err(error.into());
        }
        let encoder = Arc::new(Mutex::new(encoder));
        let audio = match plan.audio {
            Some(audio_plan) => {
                let recorder = Arc::new(Recorder::new(
                    plan.session_root.to_string_lossy().as_ref(),
                    audio_plan.device,
                    audio_plan.sample_rate_hz,
                    audio_plan.channels,
                    audio_plan.segment_seconds,
                )?);
                if let Err(error) = recorder.start() {
                    if let Ok(mut encoder) = encoder.lock() {
                        encoder.abort();
                    }
                    if let Ok(mut sink) = sink.lock() {
                        sink.close();
                    }
                    return Err(error.into());
                }
                Some(recorder)
            }
            None => None,
        };
        Ok(Self {
            active_take,
            sink,
            encoder,
            segment_planner,
            recording_start_monotonic_ns: plan.recording_start_monotonic_ns,
            audio,
            state: Mutex::new(State {
                lifecycle: Lifecycle::Recording,
                registered_segments: 0,
                outcome: None,
            }),
        })
    }

    pub(crate) fn ensure_recording(&self) -> Result<(), StoreError> {
        let state = self
            .state
            .lock()
            .map_err(|_| StoreError::poisoned("state"))?;
        if state.lifecycle != Lifecycle::Recording {
            return Err(StoreError::new(
                "invalid_state",
                "session transaction is not recording",
            ));
        }
        Ok(())
    }

    pub(crate) fn ensure_finished(&self) -> Result<(), StoreError> {
        let state = self
            .state
            .lock()
            .map_err(|_| StoreError::poisoned("state"))?;
        if state.lifecycle != Lifecycle::Finished || state.outcome.is_none() {
            return Err(StoreError::new(
                "invalid_state",
                "session transaction must finish before it can be sealed",
            ));
        }
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> Result<TransactionSnapshot, StoreError> {
        let lifecycle = self
            .state
            .lock()
            .map_err(|_| StoreError::poisoned("state"))?
            .lifecycle;
        let active_take = self
            .active_take
            .lock()
            .map_err(|_| StoreError::poisoned("active take"))?
            .snapshot();
        let sink = self
            .sink
            .lock()
            .map_err(|_| StoreError::poisoned("sink"))?
            .snapshot();
        let (segments, submitted_frames) = {
            let encoder = self
                .encoder
                .lock()
                .map_err(|_| StoreError::poisoned("encoder"))?;
            (encoder.segments(), encoder.submitted_frames())
        };
        Ok(TransactionSnapshot {
            state: match lifecycle {
                Lifecycle::Recording => "recording",
                Lifecycle::Finishing => "finishing",
                Lifecycle::Finished => "finished",
                Lifecycle::Aborted => "aborted",
            },
            active_take,
            sink,
            audio: self.audio.as_ref().map(|audio| audio.snapshot()),
            segments,
            submitted_frames,
        })
    }

    pub(crate) fn segments(&self) -> Result<Vec<SegmentEvent>, StoreError> {
        let segments = self
            .encoder
            .lock()
            .map_err(|_| StoreError::poisoned("encoder"))?
            .segments();
        self.register_segments(&segments)?;
        Ok(segments)
    }

    pub(crate) fn finish(
        &self,
        duration: Duration,
        timeout: Duration,
    ) -> Result<RecordingOutcome, StoreError> {
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| StoreError::poisoned("state"))?;
            match state.lifecycle {
                Lifecycle::Finished => {
                    return state.outcome.clone().ok_or_else(|| {
                        StoreError::new("invalid_state", "finished transaction has no outcome")
                    });
                }
                Lifecycle::Recording => state.lifecycle = Lifecycle::Finishing,
                Lifecycle::Finishing => {
                    return Err(StoreError::new(
                        "invalid_state",
                        "session transaction finish is already in progress",
                    ));
                }
                Lifecycle::Aborted => {
                    return Err(StoreError::new(
                        "invalid_state",
                        "session transaction is aborted",
                    ));
                }
            }
        }

        let result = self.finish_inner(duration, timeout);
        match result {
            Ok(outcome) => {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| StoreError::poisoned("state"))?;
                state.lifecycle = Lifecycle::Finished;
                state.outcome = Some(outcome.clone());
                Ok(outcome)
            }
            Err(error) => {
                self.abort_resources();
                if let Ok(mut state) = self.state.lock() {
                    state.lifecycle = Lifecycle::Aborted;
                }
                Err(error)
            }
        }
    }

    fn finish_inner(
        &self,
        duration: Duration,
        timeout: Duration,
    ) -> Result<RecordingOutcome, StoreError> {
        if timeout.is_zero() {
            return Err(StoreError::new(
                "invalid_argument",
                "transaction finish timeout must be positive",
            ));
        }
        let active_take = self
            .active_take
            .lock()
            .map_err(|_| StoreError::poisoned("active take"))?
            .finish()?;
        let audio = self
            .audio
            .as_ref()
            .map(|audio| audio.stop(timeout))
            .transpose()?;
        let sink = self
            .sink
            .lock()
            .map_err(|_| StoreError::poisoned("sink"))?
            .flush_and_close()?;
        let (segments, submitted_frames, encoder_stats) = {
            let mut encoder = self
                .encoder
                .lock()
                .map_err(|_| StoreError::poisoned("encoder"))?;
            let segments = encoder.finish(timeout)?;
            let submitted_frames = encoder.submitted_frames();
            let stats = encoder.stats();
            (segments, submitted_frames, stats)
        };
        self.register_segments(&segments)?;
        let planner = self
            .segment_planner
            .lock()
            .map_err(|_| StoreError::poisoned("segment planner"))?
            .finish(
                submitted_frames,
                active_take.frame_domain,
                duration.as_secs_f64(),
            )?;
        if active_take.frames_written != sink.frames_written
            || active_take.frames_written != submitted_frames
        {
            return Err(StoreError::new(
                "recording_count_mismatch",
                "active take, sink, and encoder frame counts differ",
            ));
        }
        Ok(RecordingOutcome {
            active_take,
            sink,
            audio,
            segments,
            encoder_stats,
            planner,
        })
    }

    pub(crate) fn boundary(
        &self,
        ordinal: u64,
        duration: Duration,
    ) -> Result<SegmentBoundary, StoreError> {
        self.segment_planner
            .lock()
            .map_err(|_| StoreError::poisoned("segment planner"))?
            .boundary(ordinal, duration.as_secs_f64())
            .map_err(Into::into)
    }

    pub(crate) fn abort(&self) {
        let should_abort = match self.state.lock() {
            Ok(mut state) => match state.lifecycle {
                Lifecycle::Finished | Lifecycle::Aborted => false,
                Lifecycle::Recording | Lifecycle::Finishing => {
                    state.lifecycle = Lifecycle::Aborted;
                    true
                }
            },
            Err(_) => true,
        };
        if should_abort {
            self.abort_resources();
        }
    }

    pub(crate) fn open_handle_count(&self) -> u64 {
        self.state
            .lock()
            .map(|state| {
                u64::from(matches!(
                    state.lifecycle,
                    Lifecycle::Recording | Lifecycle::Finishing
                ))
            })
            .unwrap_or(1)
    }

    fn register_segments(&self, segments: &[SegmentEvent]) -> Result<(), StoreError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| StoreError::poisoned("state"))?;
        if segments.len() < state.registered_segments {
            return Err(StoreError::new(
                "segment_invalid",
                "encoder segment list regressed",
            ));
        }
        let mut planner = self
            .segment_planner
            .lock()
            .map_err(|_| StoreError::poisoned("segment planner"))?;
        for segment in &segments[state.registered_segments..] {
            planner.register_segment(segment.index, segment.start_frame, segment.end_frame)?;
            state.registered_segments += 1;
        }
        Ok(())
    }

    fn abort_resources(&self) {
        if let Some(audio) = &self.audio {
            audio.abort();
            audio.close();
        }
        if let Ok(mut encoder) = self.encoder.lock() {
            encoder.abort();
        }
        if let Ok(mut sink) = self.sink.lock() {
            sink.close();
        }
    }
}

impl Drop for SessionTransaction {
    fn drop(&mut self) {
        self.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::{RecordingPlan, SessionTransaction};
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn temp_root(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "rp-ylx-session-store-{name}-{}-{unique}",
            std::process::id()
        ))
    }

    fn encoder_helper(root: &Path) -> PathBuf {
        let helper = root.join("encoder-helper.sh");
        fs::write(
            &helper,
            "#!/bin/sh\nprintf '{\"event\":\"ready\"}\\n'\ncat >/dev/null\nprintf '{\"event\":\"done\",\"frames\":0}\\n'\n",
        )
        .unwrap();
        let mut permissions = fs::metadata(&helper).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&helper, permissions).unwrap();
        helper
    }

    fn plan<'a>(
        root: &'a Path,
        helper: &'a Path,
        session_id: &'a str,
        recording_start_monotonic_ns: u64,
    ) -> RecordingPlan<'a> {
        RecordingPlan {
            session_root: root,
            session_id,
            encoder_executable: helper,
            width: 3840,
            height: 1080,
            fps: 30,
            bitrate_kbps: 8192,
            segment_frames: 3,
            recording_start_monotonic_ns,
            audio: None,
        }
    }

    #[test]
    fn rejects_invalid_start_before_acquiring_resources() {
        let root = temp_root("invalid-start");
        fs::create_dir_all(root.join("video")).unwrap();
        let error =
            match SessionTransaction::begin(plan(&root, Path::new("/bin/true"), "session", 0)) {
                Ok(_) => panic!("zero start timestamp unexpectedly acquired a transaction"),
                Err(error) => error,
            };
        assert_eq!(error.code, "invalid_argument");
        assert!(!root.join("frames.ndjson").exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn finish_is_idempotent_and_releases_the_transaction_handle() {
        let root = temp_root("finish");
        fs::create_dir_all(root.join("video")).unwrap();
        let helper = encoder_helper(&root);
        let transaction = SessionTransaction::begin(plan(&root, &helper, "session", 1)).unwrap();
        assert_eq!(transaction.open_handle_count(), 1);
        let first = transaction
            .finish(Duration::from_millis(1), Duration::from_secs(2))
            .unwrap();
        let second = transaction
            .finish(Duration::from_millis(1), Duration::from_secs(2))
            .unwrap();
        assert_eq!(first.active_take.frames_written, 0);
        assert_eq!(second.encoder_stats, first.encoder_stats);
        assert_eq!(transaction.open_handle_count(), 0);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn abort_is_idempotent_across_repeated_transactions() {
        let parent = temp_root("abort");
        fs::create_dir_all(&parent).unwrap();
        let helper = encoder_helper(&parent);
        for ordinal in 0..5 {
            let root = parent.join(format!("session-{ordinal}"));
            fs::create_dir_all(root.join("video")).unwrap();
            let session_id = format!("session-{ordinal}");
            let transaction =
                SessionTransaction::begin(plan(&root, &helper, &session_id, 1)).unwrap();
            transaction.abort();
            transaction.abort();
            assert_eq!(transaction.open_handle_count(), 0);
        }
        let _ = fs::remove_dir_all(parent);
    }
}
