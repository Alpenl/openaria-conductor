use crate::active_take::{self, ActiveTakeSnapshot, ActiveTakeWriter};
use crate::audio::{self, AudioRecordingResult, AudioRecordingSnapshot, Recorder};
use crate::recording::{
    self, RecordingSegmentPlanner, RecordingSink, RecordingSinkSnapshot, SegmentBoundary,
    SegmentPlannerSnapshot,
};
use crate::stereo_encoder::{self, EncoderProcess, SegmentEvent};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
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
    pub(crate) encoder_arguments: &'a [String],
    pub(crate) recording_start_monotonic_ns: u64,
    pub(crate) audio: Option<AudioPlan<'a>>,
}

#[derive(Debug, Clone)]
pub(crate) struct TransactionSnapshot {
    pub(crate) state: &'static str,
    pub(crate) active_take: ActiveTakeSnapshot,
    pub(crate) sink: RecordingSinkSnapshot,
    pub(crate) audio: Option<AudioRecordingSnapshot>,
    pub(crate) audio_failure: Option<audio::AudioError>,
    pub(crate) submitted_frames: u64,
    pub(crate) encoder_stats: BTreeMap<String, i64>,
    pub(crate) encoder_failure: Option<crate::stereo_encoder::EncoderProcessError>,
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
    pub(crate) capture_stopped_monotonic_ns: Arc<AtomicU64>,
    pub(crate) audio: Option<Arc<Recorder>>,
    state: Mutex<State>,
    segment_inventory: Mutex<()>,
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
        encoder.configure(plan.encoder_arguments)?;
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
            capture_stopped_monotonic_ns: Arc::new(AtomicU64::new(0)),
            audio,
            state: Mutex::new(State {
                lifecycle: Lifecycle::Recording,
                registered_segments: 0,
                outcome: None,
            }),
            segment_inventory: Mutex::new(()),
        })
    }

    pub(crate) fn ensure_recording(&self) -> Result<(), StoreError> {
        if self.capture_stopped_monotonic_ns.load(Ordering::Acquire) != 0 {
            return Err(StoreError::new(
                "invalid_state",
                "session transaction capture gate has already closed",
            ));
        }
        if let Some(audio) = &self.audio {
            audio.check_health()?;
        }
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
        if let Some(audio) = &self.audio {
            audio.check_health()?;
        }
        self.diagnostic_snapshot()
    }

    pub(crate) fn diagnostic_snapshot(&self) -> Result<TransactionSnapshot, StoreError> {
        // Health failures must stop capture, but must not hide other components'
        // progress and first errors when collecting a failure checkpoint.
        let audio_failure = self
            .audio
            .as_ref()
            .and_then(|audio| audio.check_health().err());
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
        let (submitted_frames, encoder_stats, encoder_failure) = {
            let encoder = self
                .encoder
                .lock()
                .map_err(|_| StoreError::poisoned("encoder"))?;
            (
                encoder.submitted_frames(),
                encoder.stats(),
                encoder.failure(),
            )
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
            audio_failure,
            submitted_frames,
            encoder_stats,
            encoder_failure,
        })
    }

    pub(crate) fn segments(&self) -> Result<Vec<SegmentEvent>, StoreError> {
        self.segments_since(0)
    }

    pub(crate) fn segments_since(&self, offset: usize) -> Result<Vec<SegmentEvent>, StoreError> {
        // Serialize the inventory snapshot with planner registration. Capturing
        // them separately lets a concurrent reader register a newer inventory
        // before an older snapshot, falsely reporting a regression.
        let _inventory = self
            .segment_inventory
            .lock()
            .map_err(|_| StoreError::poisoned("segment inventory"))?;
        let registered = self
            .state
            .lock()
            .map_err(|_| StoreError::poisoned("state"))?
            .registered_segments;
        let start = offset.min(registered);
        let mut segments = self
            .encoder
            .lock()
            .map_err(|_| StoreError::poisoned("encoder"))?
            .segments_since(start)?;
        // The encoder lock is released before the planner is acquired. Capture
        // submission also releases the planner before acquiring the encoder.
        // Do not hold the lifecycle state while waiting on encoder submission.
        let mut state = self
            .state
            .lock()
            .map_err(|_| StoreError::poisoned("state"))?;
        let total = start + segments.len();
        if total < state.registered_segments {
            return Err(StoreError::new(
                "segment_invalid",
                "encoder segment list regressed",
            ));
        }
        if offset > total {
            return Err(StoreError::new(
                "invalid_argument",
                "segment offset exceeds inventory",
            ));
        }
        let mut planner = self
            .segment_planner
            .lock()
            .map_err(|_| StoreError::poisoned("segment planner"))?;
        for segment in &segments[state.registered_segments - start..] {
            planner.register_segment(segment.index, segment.start_frame, segment.end_frame)?;
            state.registered_segments += 1;
        }
        if offset == start {
            Ok(segments)
        } else {
            // A first caller may ask beyond the registered prefix. Register
            // that prefix as well, but expose only the requested window.
            Ok(segments.split_off(offset - start))
        }
    }

    pub(crate) fn request_audio_stop(&self) {
        if let Some(audio) = &self.audio {
            audio.request_stop();
        }
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

        self.request_audio_stop();
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
        let _inventory = self
            .segment_inventory
            .lock()
            .map_err(|_| StoreError::poisoned("segment inventory"))?;
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
            encoder_arguments: &[],
            segment_frames: 3,
            recording_start_monotonic_ns,
            audio: None,
        }
    }

    fn segmented_helper(root: &Path, count: usize) -> PathBuf {
        let helper = encoder_helper(root);
        let mut events = String::new();
        for index in 0..count {
            let event = serde_json::json!({
                "event": "segment", "index": index,
                "start_frame": index * 3, "end_frame": (index + 1) * 3,
                "left": {"path": format!("video/left_{index:05}.mp4"), "bytes": 1},
                "right": {"path": format!("video/right_{index:05}.mp4"), "bytes": 1}
            });
            events.push_str(&event.to_string());
            events.push('\n');
        }
        fs::write(root.join("events.ndjson"), events).unwrap();
        fs::write(
            &helper,
            r#"#!/bin/sh
cat "$(dirname "$0")/events.ndjson"
printf '{"event":"ready"}\n'
cat >/dev/null
printf '{"event":"done","frames":0}\n'
"#,
        )
        .unwrap();
        helper
    }

    #[test]
    fn segment_window_registers_skipped_prefix_and_rejects_bad_offset_without_mutation() {
        let root = temp_root("segment-window");
        fs::create_dir_all(root.join("video")).unwrap();
        let helper = segmented_helper(&root, 3);
        let transaction = SessionTransaction::begin(plan(&root, &helper, "session", 1)).unwrap();
        assert_eq!(
            transaction.segments_since(4).unwrap_err().code,
            "invalid_argument"
        );
        assert_eq!(transaction.state.lock().unwrap().registered_segments, 0);
        assert_eq!(transaction.segments_since(2).unwrap()[0].index, 2);
        assert_eq!(transaction.state.lock().unwrap().registered_segments, 3);
        assert_eq!(
            transaction
                .segment_planner
                .lock()
                .unwrap()
                .snapshot()
                .segment_count,
            3
        );
        assert!(transaction.segments_since(3).unwrap().is_empty());
        assert_eq!(transaction.segments().unwrap().len(), 3);
        transaction.abort();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn closed_capture_gate_rejects_reattachment_but_retains_cleanup_ownership() {
        let root = temp_root("closed-capture-gate");
        fs::create_dir_all(root.join("video")).unwrap();
        let helper = segmented_helper(&root, 0);
        let transaction = SessionTransaction::begin(plan(&root, &helper, "session", 1)).unwrap();
        transaction.ensure_recording().unwrap();
        transaction
            .capture_stopped_monotonic_ns
            .store(10, std::sync::atomic::Ordering::Release);
        assert_eq!(transaction.ensure_recording().unwrap_err().code, "invalid_state");
        assert_eq!(transaction.snapshot().unwrap().state, "recording");
        transaction.abort();
        assert_eq!(transaction.open_handle_count(), 0);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn segment_window_still_rejects_inventory_regression() {
        let root = temp_root("segment-regression");
        fs::create_dir_all(root.join("video")).unwrap();
        let helper = segmented_helper(&root, 3);
        let transaction = SessionTransaction::begin(plan(&root, &helper, "session", 1)).unwrap();
        transaction.state.lock().unwrap().registered_segments = 4;
        for offset in [0, 2, 4] {
            assert_eq!(
                transaction.segments_since(offset).unwrap_err().code,
                "segment_invalid"
            );
        }
        transaction.abort();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn concurrent_segment_windows_register_each_segment_once() {
        let root = temp_root("segment-concurrency");
        fs::create_dir_all(root.join("video")).unwrap();
        let helper = segmented_helper(&root, 120);
        let transaction = std::sync::Arc::new(
            SessionTransaction::begin(plan(&root, &helper, "session", 1)).unwrap(),
        );
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let transaction = std::sync::Arc::clone(&transaction);
                scope.spawn(move || {
                    for index in 0..100 {
                        let offset = (index + worker) % 121;
                        let tail = transaction.segments_since(offset).unwrap();
                        assert_eq!(tail.len(), 120 - offset);
                        assert!(
                            tail.iter()
                                .enumerate()
                                .all(|(i, segment)| segment.index == (i + offset) as u64)
                        );
                    }
                });
            }
        });
        assert_eq!(transaction.state.lock().unwrap().registered_segments, 120);
        assert_eq!(
            transaction
                .segment_planner
                .lock()
                .unwrap()
                .snapshot()
                .segment_count,
            120
        );
        transaction.abort();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn blocked_segment_inventory_does_not_hold_lifecycle_state() {
        let root = temp_root("segment-state-lock");
        fs::create_dir_all(root.join("video")).unwrap();
        let helper = segmented_helper(&root, 3);
        let transaction = SessionTransaction::begin(plan(&root, &helper, "session", 1)).unwrap();
        let encoder = transaction.encoder.lock().unwrap();
        std::thread::scope(|scope| {
            let reader = scope.spawn(|| transaction.segments_since(0));
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            let entered = loop {
                if transaction.segment_inventory.try_lock().is_err() {
                    break true;
                }
                if std::time::Instant::now() >= deadline {
                    break false;
                }
                std::thread::yield_now();
            };
            // The inventory reader now waits for the encoder held above.
            std::thread::sleep(Duration::from_millis(10));
            let state_available = transaction.state.try_lock().is_ok();
            drop(encoder);
            assert_eq!(reader.join().unwrap().unwrap().len(), 3);
            assert!(entered && state_available);
        });
        transaction.abort();
        let _ = fs::remove_dir_all(root);
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
        transaction.request_audio_stop();
        transaction.request_audio_stop();
        transaction.ensure_recording().unwrap();
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
    fn audio_failure_remains_fatal_but_does_not_hide_diagnostics() {
        let root = temp_root("failed-audio-diagnostics");
        fs::create_dir_all(root.join("video")).unwrap();
        let helper = encoder_helper(&root);
        let mut transaction =
            SessionTransaction::begin(plan(&root, &helper, "session", 1)).unwrap();
        let audio =
            crate::audio::Recorder::new(root.to_str().unwrap(), "test", 48000, 2, 5.0).unwrap();
        audio.inject_failure(crate::audio::AudioError::new(
            "audio_failed",
            "audio durable commit backlog exhausted",
        ));
        transaction.audio = Some(std::sync::Arc::new(audio));
        let first = transaction.snapshot().unwrap_err();
        assert_eq!(first.code, "audio_failed");
        let diagnostics = transaction.diagnostic_snapshot().unwrap();
        assert_eq!(diagnostics.state, "recording");
        assert_eq!(diagnostics.sink.frames_indexed, 0);
        assert_eq!(diagnostics.submitted_frames, 0);
        assert!(diagnostics.audio.is_some());
        let failure = diagnostics.audio_failure.unwrap();
        assert_eq!(failure.code, first.code);
        assert_eq!(failure.message, first.message);
        // Diagnostic observation must neither clear the fault nor authorize writes.
        assert_eq!(transaction.ensure_recording().unwrap_err(), first);
        assert_eq!(transaction.snapshot().unwrap_err(), first);
        transaction.abort();
        assert!(
            transaction
                .diagnostic_snapshot()
                .unwrap()
                .audio_failure
                .is_some()
        );
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
