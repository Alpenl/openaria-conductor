use crate::audio::{Recorder, SyncedAudioCapture};
use crate::hardware::imu::{Collector, ImuError, ImuSample};
use crate::hardware::metrics::Metrics;
use crate::hardware::native_camera::{Frame, FrameValidator, Stream, StreamError};
use crate::hardware::preview::{LatestBuffer, PreviewError};
use crate::hardware::v4l2;
use crate::session::active_take::{self, ActiveTakeError, ActiveTakeWriter};
use crate::session::recording::{self, CaptureFanoutState, FrameTimestampAudit, RecordingError};
use crate::session::shm_ring::SyncSlotBundle;
use crate::session::stereo_encoder::{self, EncoderProcessError};
use miro_ego_core::sys::clock::monotonic_ns;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::io::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const RECORD_QUEUE_MAX_FRAMES: usize = 128;
const RECORD_QUEUE_MAX_BYTES: usize = 128 * 1024 * 1024;

pub trait FailureSink: Send + Sync {
    fn on_failure(&self, code: &str, message: &str);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeError {
    pub code: Cow<'static, str>,
    pub message: String,
}

impl RuntimeError {
    fn new(code: impl Into<Cow<'static, str>>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

impl From<StreamError> for RuntimeError {
    fn from(error: StreamError) -> Self {
        Self::new(error.code, error.message)
    }
}

impl From<PreviewError> for RuntimeError {
    fn from(error: PreviewError) -> Self {
        Self::new(error.code, error.message)
    }
}

impl From<ActiveTakeError> for RuntimeError {
    fn from(error: ActiveTakeError) -> Self {
        Self::new(error.code, error.message)
    }
}

impl From<RecordingError> for RuntimeError {
    fn from(error: RecordingError) -> Self {
        Self::new(error.code, error.message)
    }
}

impl From<ImuError> for RuntimeError {
    fn from(error: ImuError) -> Self {
        Self::new(error.code, error.message)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSnapshot {
    pub running: bool,
    pub recording_present: bool,
    pub recording_active: bool,
    pub inflight_frames: u64,
    pub observed_frames: u64,
    pub failure_reported: bool,
    pub terminal_error: Option<RuntimeError>,
    pub last_preview_error: Option<RuntimeError>,
}

const SYNCED_IMU_QUEUE_MAX_SAMPLES: usize = 8192;

#[derive(Default)]
pub struct SyncedImuQueue {
    inner: Mutex<VecDeque<ImuSample>>,
    dropped_overflow: AtomicU64,
    emitted_samples: AtomicU64,
}

impl SyncedImuQueue {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(1024)),
            dropped_overflow: AtomicU64::new(0),
            emitted_samples: AtomicU64::new(0),
        }
    }

    pub fn push_samples(&self, samples: &[ImuSample]) {
        if let Ok(mut guard) = self.inner.lock() {
            for sample in samples {
                if guard.len() >= SYNCED_IMU_QUEUE_MAX_SAMPLES {
                    let _ = guard.pop_front();
                    self.dropped_overflow.fetch_add(1, Ordering::Relaxed);
                }
                guard.push_back(sample.clone());
            }
        }
    }

    pub fn drain_up_to(&self, max_monotonic_ns: u64) -> Vec<ImuSample> {
        let mut out = Vec::new();
        if let Ok(mut guard) = self.inner.lock() {
            while let Some(front) = guard.front() {
                if front.host_monotonic_ns > max_monotonic_ns {
                    break;
                }
                if let Some(sample) = guard.pop_front() {
                    out.push(sample);
                }
            }
        }
        out
    }

    pub fn drain_all(&self) -> Vec<ImuSample> {
        if let Ok(mut guard) = self.inner.lock() {
            return guard.drain(..).collect();
        }
        Vec::new()
    }

    pub fn dropped_overflow(&self) -> u64 {
        self.dropped_overflow.load(Ordering::Relaxed)
    }

    pub fn record_emitted(&self, count: u64) {
        self.emitted_samples.fetch_add(count, Ordering::Relaxed);
    }

    pub fn emitted_samples(&self) -> u64 {
        self.emitted_samples.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct SyncedCaptureConfig {
    pub audio: Option<Arc<SyncedAudioCapture>>,
    pub imu_queue: Arc<SyncedImuQueue>,
    pub timescale: u32,
    pub fps: u32,
}

pub struct RecordingTarget {
    pub active_take: Arc<Mutex<ActiveTakeWriter>>,
    pub sink: Arc<Mutex<recording::RecordingSink>>,
    pub encoder: Arc<Mutex<stereo_encoder::EncoderProcess>>,
    pub segment_planner: Arc<Mutex<recording::RecordingSegmentPlanner>>,
    pub recording_start_monotonic_ns: u64,
    pub on_failure: Arc<dyn FailureSink>,
    pub imu: Option<Arc<Collector>>,
    pub imu_timeout: Duration,
    pub audio: Option<Arc<Recorder>>,
    pub capture_stopped_monotonic_ns: Arc<AtomicU64>,
    pub recover_source_gaps: bool,
    pub synced: Option<SyncedCaptureConfig>,
}

struct QueuedRecordFrame {
    frame: Frame,
    dropped_before: u64,
    recovered_recorded_drops: u64,
    corrupt_recorded_drops: u64,
    validation_source_gap: u64,
    validation_queue_rejected: u64,
    previous_source_frame: Option<SourceFrameEvidence>,
    current_source_frame: SourceFrameEvidence,
}

enum RecordWork {
    Frame(Box<QueuedRecordFrame>),
    RecoveredDrops {
        dropped: u64,
        host_monotonic_ns: u64,
        corrupt: bool,
    },
}

impl RecordWork {
    fn byte_weight(&self) -> usize {
        match self {
            Self::Frame(item) => item.frame.raw_side_by_side.len().max(1),
            Self::RecoveredDrops { .. } => 64,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum EnqueueOutcome {
    Enqueued,
    Closed,
    Full,
}

struct RecordQueueState {
    items: VecDeque<RecordWork>,
    queued_bytes: usize,
    closed: bool,
}

struct RecordWorkQueue {
    state: Mutex<RecordQueueState>,
    changed: Condvar,
}

impl RecordWorkQueue {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(RecordQueueState {
                items: VecDeque::new(),
                queued_bytes: 0,
                closed: false,
            }),
            changed: Condvar::new(),
        })
    }

    fn try_enqueue(&self, work: RecordWork) -> Result<EnqueueOutcome, RuntimeError> {
        let mut state = self.state.lock().map_err(|_| {
            RuntimeError::new(
                "capture_runtime_poisoned",
                "capture runtime record queue mutex is poisoned",
            )
        })?;
        if state.closed {
            return Ok(EnqueueOutcome::Closed);
        }
        let weight = work.byte_weight();
        if !state.items.is_empty()
            && (state.items.len() >= RECORD_QUEUE_MAX_FRAMES
                || state.queued_bytes.saturating_add(weight) > RECORD_QUEUE_MAX_BYTES)
        {
            return Ok(EnqueueOutcome::Full);
        }
        state.queued_bytes = state.queued_bytes.saturating_add(weight);
        state.items.push_back(work);
        self.changed.notify_one();
        Ok(EnqueueOutcome::Enqueued)
    }

    fn pop(&self) -> Option<RecordWork> {
        let mut state = self.state.lock().ok()?;
        loop {
            if let Some(work) = state.items.pop_front() {
                state.queued_bytes = state.queued_bytes.saturating_sub(work.byte_weight());
                return Some(work);
            }
            if state.closed {
                return None;
            }
            state = self.changed.wait(state).ok()?;
        }
    }

    fn close_and_drain_remaining_frames(&self) -> usize {
        let Ok(mut state) = self.state.lock() else {
            return 0;
        };
        state.closed = true;
        let mut dropped_inflight = 0_usize;
        while let Some(work) = state.items.pop_front() {
            if matches!(work, RecordWork::Frame(_)) {
                dropped_inflight += 1;
            }
        }
        state.queued_bytes = 0;
        self.changed.notify_all();
        dropped_inflight
    }

    fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
            self.changed.notify_all();
        }
    }
}

struct SplitSinkRecording {
    active_take: Arc<Mutex<ActiveTakeWriter>>,
    sink: Arc<Mutex<recording::RecordingSink>>,
    encoder: Arc<Mutex<stereo_encoder::EncoderProcess>>,
    encoder_abort: stereo_encoder::EncoderAbortHandle,
    segment_planner: Arc<Mutex<recording::RecordingSegmentPlanner>>,
    recording_start_monotonic_ns: u64,
    capture_stopped_monotonic_ns: Arc<AtomicU64>,
    on_failure: Arc<dyn FailureSink>,
    imu: Option<Arc<Collector>>,
    audio: Option<Arc<Recorder>>,
    synced: Option<SyncedCaptureConfig>,
    record_queue: Option<Arc<RecordWorkQueue>>,
}

impl SplitSinkRecording {
    fn request_capture_stop(&self) {
        if let Some(audio) = &self.audio {
            audio.request_stop();
        }
        // Preserve the first gate closure across repeated calls, without
        // waiting for encoder or sink locks held by an in-flight frame.
        // A clock failure marks the boundary invalid without blocking cleanup.
        let stopped = monotonic_ns()
            .ok()
            .filter(|value| *value > 0)
            .unwrap_or(u64::MAX);
        let _ = self.capture_stopped_monotonic_ns.compare_exchange(
            0,
            stopped,
            Ordering::Release,
            Ordering::Relaxed,
        );
        if let Some(queue) = &self.record_queue {
            queue.close();
        }
        self.encoder_abort.wake();
    }
}

// Keep scalar evidence only: no extra frame payload ownership or per-frame JSON.
#[derive(Debug, Clone, Copy, serde::Serialize)]
struct SourceFrameEvidence {
    source_sequence: u64,
    host_monotonic_ns: u64,
    dequeue_monotonic_ns: u64,
    buffer_flags: u32,
    payload_bytes: u64,
    camera_counter_raw: Option<u32>,
}

impl SourceFrameEvidence {
    fn extract_from_frame(frame: &Frame) -> (Self, Option<&[u8]>) {
        let raw = &frame.raw_side_by_side;
        let payload = if raw.is_empty() {
            None
        } else {
            recording::jpeg_payload(raw).ok()
        };
        let counter = payload.and_then(recording::avi1_counter);
        let evidence = Self {
            source_sequence: frame.source_sequence,
            host_monotonic_ns: frame.host_monotonic_ns,
            dequeue_monotonic_ns: frame.dequeue_monotonic_ns,
            buffer_flags: frame.buffer_flags,
            payload_bytes: usize_to_u64(raw.len()),
            camera_counter_raw: counter,
        };
        (evidence, payload)
    }

    #[cfg(test)]
    fn from_frame(frame: &Frame) -> Self {
        Self::extract_from_frame(frame).0
    }
}

struct State {
    fanout: CaptureFanoutState,
    validator: FrameValidator,
    previous_source_frame: Option<SourceFrameEvidence>,
    recording: Option<Arc<SplitSinkRecording>>,
    running: bool,
    terminal_error: Option<RuntimeError>,
    last_preview_error: Option<RuntimeError>,
    max_consumer_frame_ns: u64,
}

struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    imu_stop: Arc<AtomicBool>,
}

struct WorkerHandle {
    handle: JoinHandle<()>,
    done: Receiver<()>,
}

pub struct Runtime {
    stream: Arc<Stream>,
    preview: Arc<LatestBuffer>,
    shared: Arc<Shared>,
    stop: Arc<AtomicBool>,
    imu_stop: Arc<AtomicBool>,
    worker: Mutex<Option<JoinHandle<()>>>,
    imu_worker: Mutex<Option<WorkerHandle>>,
    record_worker: Mutex<Option<WorkerHandle>>,
    read_timeout: Duration,
    metrics: Option<Arc<Metrics>>,
}

impl Runtime {
    pub fn new(
        stream: Arc<Stream>,
        preview: Arc<LatestBuffer>,
        frame_decimation: u64,
        read_timeout: Duration,
        metrics: Option<Arc<Metrics>>,
    ) -> Result<Self, RuntimeError> {
        if read_timeout.is_zero() {
            return Err(RuntimeError::new(
                "invalid_argument",
                "read timeout must be positive",
            ));
        }
        let imu_stop = Arc::new(AtomicBool::new(false));
        Ok(Self {
            stream,
            preview,
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    fanout: CaptureFanoutState::new(frame_decimation)?,
                    validator: FrameValidator::new(),
                    previous_source_frame: None,
                    recording: None,
                    running: false,
                    terminal_error: None,
                    last_preview_error: None,
                    max_consumer_frame_ns: 0,
                }),
                changed: Condvar::new(),
                imu_stop: Arc::clone(&imu_stop),
            }),
            stop: Arc::new(AtomicBool::new(false)),
            imu_stop,
            worker: Mutex::new(None),
            imu_worker: Mutex::new(None),
            record_worker: Mutex::new(None),
            read_timeout,
            metrics,
        })
    }

    pub fn start_preview(&self) -> Result<(), RuntimeError> {
        self.reap_finished_aux_workers();
        let mut worker = self.worker.lock().map_err(|_| {
            RuntimeError::new(
                "capture_runtime_poisoned",
                "capture runtime worker mutex is poisoned",
            )
        })?;
        if let Some(existing) = worker.as_ref() {
            let state_running = self
                .shared
                .state
                .lock()
                .map_err(|_| {
                    RuntimeError::new(
                        "capture_runtime_poisoned",
                        "capture runtime state mutex is poisoned",
                    )
                })?
                .running;
            if state_running && !existing.is_finished() {
                return Ok(());
            }
        }
        if let Some(old_handle) = worker.take() {
            self.stop.store(true, Ordering::Release);
            let _ = self.stream.stop();
            if old_handle.thread().id() != thread::current().id() {
                let _ = old_handle.join();
            }
        }
        self.stream.start()?;
        self.stop.store(false, Ordering::Release);
        {
            let mut state = self.shared.state.lock().map_err(|_| {
                let _ = self.stream.stop();
                RuntimeError::new(
                    "capture_runtime_poisoned",
                    "capture runtime state mutex is poisoned",
                )
            })?;
            state.validator.reset();
            state.previous_source_frame = None;
            state.running = true;
            state.terminal_error = None;
            state.last_preview_error = None;
            if let Some(recording) = &state.recording {
                recording.request_capture_stop();
            }
            state.recording = None;
            state.fanout.start_stopping();
        }
        let stream = Arc::clone(&self.stream);
        let preview = Arc::clone(&self.preview);
        let shared = Arc::clone(&self.shared);
        let stop = Arc::clone(&self.stop);
        let read_timeout = self.read_timeout;
        let metrics = self.metrics.as_ref().map(Arc::clone);
        let handle = match thread::Builder::new()
            .name("capture-runtime".to_owned())
            .spawn(move || {
                run_loop(stream, preview, shared, stop, read_timeout, metrics);
            }) {
            Ok(handle) => handle,
            Err(error) => {
                if let Ok(mut state) = self.shared.state.lock() {
                    state.running = false;
                    self.shared.changed.notify_all();
                }
                let _ = self.stream.stop();
                return Err(RuntimeError::new(
                    "capture_runtime_worker_failed",
                    format!("failed to start capture runtime worker: {error}"),
                ));
            }
        };
        *worker = Some(handle);
        Ok(())
    }

    pub fn start_recording(
        &self,
        target: RecordingTarget,
    ) -> Result<RuntimeSnapshot, RuntimeError> {
        let RecordingTarget {
            active_take,
            sink,
            encoder,
            segment_planner,
            recording_start_monotonic_ns,
            on_failure,
            imu,
            imu_timeout,
            audio,
            capture_stopped_monotonic_ns,
            recover_source_gaps,
            synced,
        } = target;
        if recording_start_monotonic_ns == 0 {
            return Err(RuntimeError::new(
                "invalid_argument",
                "recording start monotonic timestamp must be positive",
            ));
        }
        if capture_stopped_monotonic_ns.load(Ordering::Acquire) != 0 {
            return Err(RuntimeError::new(
                "invalid_state",
                "session transaction capture gate has already closed",
            ));
        }
        if imu.is_some() && imu_timeout.is_zero() {
            return Err(RuntimeError::new(
                "invalid_argument",
                "capture runtime IMU timeout must be positive",
            ));
        }
        let encoder_abort = encoder
            .lock()
            .map_err(|_| RuntimeError::new("encoder_failed", "encoder process mutex is poisoned"))?
            .abort_handle();
        let mut record_worker = self.record_worker.lock().map_err(|_| {
            RuntimeError::new(
                "capture_runtime_poisoned",
                "capture runtime record worker mutex is poisoned",
            )
        })?;
        reap_if_finished(&mut record_worker);
        if record_worker.is_some() {
            return Err(RuntimeError::new(
                "invalid_state",
                "capture runtime record worker is already running",
            ));
        }
        let mut imu_worker = self.imu_worker.lock().map_err(|_| {
            RuntimeError::new(
                "capture_runtime_poisoned",
                "capture runtime IMU worker mutex is poisoned",
            )
        })?;
        reap_if_finished(&mut imu_worker);
        if imu_worker.is_some() {
            return Err(RuntimeError::new(
                "invalid_state",
                "capture runtime IMU worker is already running",
            ));
        }
        let imu_for_worker = imu.clone();
        let on_failure_for_imu = imu_for_worker.as_ref().map(|_| Arc::clone(&on_failure));
        let sink_for_worker = Arc::clone(&sink);
        let synced_audio_for_imu = synced.as_ref().and_then(|s| s.audio.clone());
        let synced_imu_for_imu = synced.as_ref().map(|s| Arc::clone(&s.imu_queue));
        let mut state = self.shared.state.lock().map_err(|_| {
            RuntimeError::new(
                "capture_runtime_poisoned",
                "capture runtime state mutex is poisoned",
            )
        })?;
        if !state.running {
            return Err(RuntimeError::new(
                "invalid_state",
                "capture runtime preview is not running",
            ));
        }
        if state.recording.is_some() {
            return Err(RuntimeError::new(
                "invalid_state",
                "capture runtime is already recording",
            ));
        }
        state.fanout.start_recording(recover_source_gaps)?;
        let previous_max_consumer_frame_ns = state.max_consumer_frame_ns;
        state.max_consumer_frame_ns = 0;
        let record_queue = RecordWorkQueue::new();
        let recording_arc = Arc::new(SplitSinkRecording {
            active_take,
            sink,
            encoder,
            encoder_abort,
            segment_planner,
            recording_start_monotonic_ns,
            capture_stopped_monotonic_ns,
            on_failure: Arc::clone(&on_failure),
            imu,
            audio: audio.clone(),
            synced,
            record_queue: Some(Arc::clone(&record_queue)),
        });

        let (rec_done_tx, rec_done_rx) = mpsc::channel();
        let rec_target = Arc::clone(&recording_arc);
        let rec_shared = Arc::clone(&self.shared);
        let rec_metrics = self.metrics.as_ref().map(Arc::clone);
        let rec_handle = match thread::Builder::new()
            .name("capture-record".to_owned())
            .spawn(move || {
                run_record_loop(rec_target, record_queue, rec_shared, rec_metrics);
                let _ = rec_done_tx.send(());
            }) {
            Ok(handle) => handle,
            Err(error) => {
                state.fanout.start_stopping();
                state.max_consumer_frame_ns = previous_max_consumer_frame_ns;
                self.shared.changed.notify_all();
                return Err(RuntimeError::new(
                    "capture_runtime_worker_failed",
                    format!("failed to start capture record worker: {error}"),
                ));
            }
        };

        state.recording = Some(recording_arc);
        if let (Some(collector), Some(on_failure)) = (imu_for_worker, on_failure_for_imu) {
            self.imu_stop.store(false, Ordering::Release);
            let shared = Arc::clone(&self.shared);
            let stop = Arc::clone(&self.imu_stop);
            let metrics = self.metrics.as_ref().map(Arc::clone);
            let (done_tx, done_rx) = mpsc::channel();
            let handle =
                match thread::Builder::new()
                    .name("capture-imu".to_owned())
                    .spawn(move || {
                        run_imu_loop(ImuLoopContext {
                            collector,
                            sink: sink_for_worker,
                            on_failure,
                            audio,
                            synced_audio: synced_audio_for_imu,
                            synced_imu: synced_imu_for_imu,
                            shared,
                            stop,
                            timeout: imu_timeout,
                            stall_budget: IMU_RETRYABLE_STALL_BUDGET,
                            metrics,
                        });
                        let _ = done_tx.send(());
                    }) {
                    Ok(handle) => handle,
                    Err(error) => {
                        if let Some(rec) = state.recording.take() {
                            rec.request_capture_stop();
                        }
                        state.fanout.start_stopping();
                        state.max_consumer_frame_ns = previous_max_consumer_frame_ns;
                        self.shared.changed.notify_all();
                        let _ = rec_handle.join();
                        return Err(RuntimeError::new(
                            "capture_runtime_imu_worker_failed",
                            format!("failed to start capture IMU worker: {error}"),
                        ));
                    }
                };
            *imu_worker = Some(WorkerHandle {
                handle,
                done: done_rx,
            });
        }
        *record_worker = Some(WorkerHandle {
            handle: rec_handle,
            done: rec_done_rx,
        });
        self.shared.changed.notify_all();
        Ok(snapshot_locked(&state))
    }

    pub fn prepare_recording(&self, timeout: Duration) -> Result<(), RuntimeError> {
        if self.snapshot()?.recording_active {
            return Err(RuntimeError::new(
                "invalid_state",
                "capture engine is already recording",
            ));
        }
        // Failure callbacks can clear recording before the IMU/record workers exit.
        // Always reap those workers before installing the next recording target.
        self.stop_recording(timeout)?;
        Ok(())
    }

    pub fn stop_recording(&self, timeout: Duration) -> Result<RuntimeSnapshot, RuntimeError> {
        if timeout.is_zero() {
            return Err(RuntimeError::new(
                "invalid_argument",
                "stop timeout must be positive",
            ));
        }
        let deadline = Instant::now() + timeout;
        let (inflight, imu) = self.begin_recording_stop()?;

        // Close the frame gate and fence IMU stop before collector shutdown can block.
        // Every exit path below MUST join and SeqCst-fence `capture-imu` so a resumed
        // `ImuPreview` worker can never contend on USB EP0 concurrently.
        self.imu_stop.store(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        if let Some(imu) = imu {
            imu.close();
        }

        let state = match self.shared.state.lock() {
            Ok(state) => state,
            Err(_) => {
                let imu_budget = deadline
                    .saturating_duration_since(Instant::now())
                    .max(Duration::from_millis(250));
                let _ = self.stop_imu_worker(imu_budget);
                fence(Ordering::SeqCst);
                let _ = self.stop_record_worker(deadline.saturating_duration_since(Instant::now()));
                return Err(RuntimeError::new(
                    "capture_runtime_poisoned",
                    "capture runtime state mutex is poisoned",
                ));
            }
        };
        let mut state = state;
        if inflight == 0 {
            state.recording = None;
            self.shared.changed.notify_all();
            let snapshot = snapshot_locked(&state);
            drop(state);
            let imu_res = self.stop_imu_worker(
                deadline
                    .saturating_duration_since(Instant::now())
                    .max(Duration::from_millis(250)),
            );
            fence(Ordering::SeqCst);
            let rec_res = self.stop_record_worker(
                deadline
                    .saturating_duration_since(Instant::now())
                    .max(Duration::from_millis(1)),
            );
            imu_res?;
            rec_res?;
            return Ok(snapshot);
        }
        let wait_budget = deadline
            .saturating_duration_since(Instant::now())
            .max(Duration::from_millis(1));
        let (state, wait_result) =
            match self
                .shared
                .changed
                .wait_timeout_while(state, wait_budget, |state| state.recording.is_some())
            {
                Ok(outcome) => outcome,
                Err(_) => {
                    let _ = self.stop_imu_worker(Duration::from_millis(250));
                    fence(Ordering::SeqCst);
                    let _ = self.stop_record_worker(Duration::from_millis(50));
                    return Err(RuntimeError::new(
                        "capture_runtime_poisoned",
                        "capture runtime state mutex is poisoned",
                    ));
                }
            };
        if wait_result.timed_out() && state.recording.is_some() {
            drop(state);
            let imu_res = self.stop_imu_worker(
                deadline
                    .saturating_duration_since(Instant::now())
                    .max(Duration::from_millis(250)),
            );
            fence(Ordering::SeqCst);
            let _ = self.stop_record_worker(Duration::from_millis(50));
            imu_res?;
            return Err(RuntimeError::new(
                "capture_runtime_stop_timeout",
                "capture runtime recording frames did not drain before timeout",
            ));
        }
        let snapshot = snapshot_locked(&state);
        drop(state);
        let imu_res = self.stop_imu_worker(
            deadline
                .saturating_duration_since(Instant::now())
                .max(Duration::from_millis(250)),
        );
        fence(Ordering::SeqCst);
        let rec_res = self.stop_record_worker(
            deadline
                .saturating_duration_since(Instant::now())
                .max(Duration::from_millis(1)),
        );
        imu_res?;
        rec_res?;
        Ok(snapshot)
    }

    fn begin_recording_stop(&self) -> Result<(u64, Option<Arc<Collector>>), RuntimeError> {
        let mut state = self.shared.state.lock().map_err(|_| {
            RuntimeError::new(
                "capture_runtime_poisoned",
                "capture runtime state mutex is poisoned",
            )
        })?;
        let imu = state
            .recording
            .as_ref()
            .and_then(|recording| recording.imu.clone());
        let inflight = state.fanout.start_stopping();
        self.imu_stop.store(true, Ordering::SeqCst);
        if let Some(recording) = &state.recording {
            recording.request_capture_stop();
        }
        self.shared.changed.notify_all();
        Ok((inflight, imu))
    }

    pub fn close(&self, timeout: Duration) -> Result<RuntimeSnapshot, RuntimeError> {
        self.stop.store(true, Ordering::Release);
        let mut first_error: Option<RuntimeError> = None;
        let _ = self.begin_recording_stop();
        capture_first_error(&mut first_error, self.request_imu_stop());
        capture_first_error(&mut first_error, self.stop_record_worker(timeout));
        capture_first_error(&mut first_error, self.stop_imu_worker(timeout));
        // Always stop the camera stream and join the capture worker even if IMU
        // cleanup timed out or failed, returning the first failure afterwards.
        let _ = self.stream.close();
        match self.worker.lock() {
            Ok(mut worker) => {
                if let Some(handle) = worker.take() {
                    if handle.thread().id() == thread::current().id() {
                        *worker = Some(handle);
                    } else if handle.join().is_err() {
                        capture_first_error(
                            &mut first_error,
                            Err(RuntimeError::new(
                                "capture_runtime_worker_failed",
                                "capture runtime worker panicked",
                            )),
                        );
                    }
                }
            }
            Err(_) => {
                capture_first_error(
                    &mut first_error,
                    Err(RuntimeError::new(
                        "capture_runtime_poisoned",
                        "capture runtime worker mutex is poisoned",
                    )),
                );
            }
        }
        let snapshot = match self.shared.state.lock() {
            Ok(mut state) => {
                state.running = false;
                if let Some(recording) = &state.recording {
                    recording.request_capture_stop();
                }
                state.recording = None;
                state.fanout.start_stopping();
                self.shared.changed.notify_all();
                Some(snapshot_locked(&state))
            }
            Err(_) => {
                capture_first_error(
                    &mut first_error,
                    Err(RuntimeError::new(
                        "capture_runtime_poisoned",
                        "capture runtime state mutex is poisoned",
                    )),
                );
                None
            }
        };
        if let Some(error) = first_error {
            return Err(error);
        }
        snapshot.ok_or_else(|| {
            RuntimeError::new(
                "capture_runtime_poisoned",
                "capture runtime state mutex is poisoned",
            )
        })
    }

    pub fn snapshot(&self) -> Result<RuntimeSnapshot, RuntimeError> {
        let state = self.shared.state.lock().map_err(|_| {
            RuntimeError::new(
                "capture_runtime_poisoned",
                "capture runtime state mutex is poisoned",
            )
        })?;
        Ok(snapshot_locked(&state))
    }

    fn reap_finished_aux_workers(&self) {
        if let Ok(mut record_worker) = self.record_worker.lock() {
            reap_if_finished(&mut record_worker);
        }
        if let Ok(mut imu_worker) = self.imu_worker.lock() {
            reap_if_finished(&mut imu_worker);
        }
    }

    fn request_imu_stop(&self) -> Result<(), RuntimeError> {
        self.imu_stop.store(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        let imu = {
            let state = self.shared.state.lock().map_err(|_| {
                RuntimeError::new(
                    "capture_runtime_poisoned",
                    "capture runtime state mutex is poisoned",
                )
            })?;
            state
                .recording
                .as_ref()
                .and_then(|recording| recording.imu.clone())
        };
        if let Some(imu) = imu {
            imu.close();
        }
        Ok(())
    }

    fn stop_record_worker(&self, timeout: Duration) -> Result<(), RuntimeError> {
        stop_aux_worker(
            &self.record_worker,
            timeout,
            "capture runtime record worker mutex is poisoned",
            "capture_runtime_worker_failed",
            "capture runtime record worker panicked",
            "capture_runtime_stop_timeout",
            "capture runtime record worker did not stop before timeout",
        )
    }

    fn stop_imu_worker(&self, timeout: Duration) -> Result<(), RuntimeError> {
        stop_aux_worker(
            &self.imu_worker,
            timeout,
            "capture runtime IMU worker mutex is poisoned",
            "capture_runtime_imu_worker_failed",
            "capture runtime IMU worker panicked",
            "capture_runtime_imu_stop_timeout",
            "capture runtime IMU worker did not stop before timeout",
        )
    }
}

fn reap_if_finished(slot: &mut Option<WorkerHandle>) {
    let should_reap = slot.as_ref().is_some_and(|w| {
        w.handle.is_finished() && w.handle.thread().id() != thread::current().id()
    });
    if should_reap {
        if let Some(done) = slot.take() {
            let _ = done.handle.join();
        }
    }
}

fn stop_aux_worker(
    slot: &Mutex<Option<WorkerHandle>>,
    timeout: Duration,
    poison_msg: &'static str,
    panic_code: &'static str,
    panic_msg: &'static str,
    timeout_code: &'static str,
    timeout_msg: &'static str,
) -> Result<(), RuntimeError> {
    let mut worker = slot
        .lock()
        .map_err(|_| RuntimeError::new("capture_runtime_poisoned", poison_msg))?;
    let Some(handle) = worker.take() else {
        return Ok(());
    };
    if handle.handle.thread().id() == thread::current().id() {
        // Never self-wait or self-join when invoked inside a `FailureSink` callback on
        // the worker's own thread; leave the handle in place so the next lifecycle call reaps it.
        *worker = Some(handle);
        return Ok(());
    }
    match handle.done.recv_timeout(timeout) {
        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {
            if handle.handle.join().is_err() {
                return Err(RuntimeError::new(panic_code, panic_msg));
            }
            Ok(())
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            *worker = Some(handle);
            Err(RuntimeError::new(timeout_code, timeout_msg))
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        let _ = self.close(Duration::from_secs(5));
    }
}

fn capture_first_error(first_error: &mut Option<RuntimeError>, result: Result<(), RuntimeError>) {
    if first_error.is_none() {
        if let Err(error) = result {
            *first_error = Some(error);
        }
    }
}

fn run_loop(
    stream: Arc<Stream>,
    preview: Arc<LatestBuffer>,
    shared: Arc<Shared>,
    stop: Arc<AtomicBool>,
    read_timeout: Duration,
    metrics: Option<Arc<Metrics>>,
) {
    while !stop.load(Ordering::Acquire) {
        let read_started = start_stage(metrics.as_ref());
        let read_result = stream.read(read_timeout);
        finish_stage(metrics.as_ref(), "native_capture_read", read_started);
        if let Some(metrics) = &metrics {
            let (depth, capacity, peak) = stream.queue_stats();
            let _ = metrics.observe_queue(depth as u64, capacity as u64, 0, Some(peak as u64));
        }
        match read_result {
            Ok(frame) => {
                if let Err(error) = process_frame(&preview, &shared, frame, metrics.as_ref()) {
                    set_terminal_error(&shared, error);
                    return;
                }
            }
            Err(error) => {
                if stop.load(Ordering::Acquire) {
                    break;
                }
                set_terminal_error(&shared, error.into());
                return;
            }
        }
    }
    if let Ok(mut state) = shared.state.lock() {
        state.running = false;
        if let Some(recording) = &state.recording {
            recording.request_capture_stop();
        }
        state.recording = None;
        state.fanout.start_stopping();
        shared.changed.notify_all();
    }
}

fn process_frame(
    preview: &LatestBuffer,
    shared: &Shared,
    frame: Frame,
    metrics: Option<&Arc<Metrics>>,
) -> Result<(), RuntimeError> {
    let frame_started = Instant::now();
    let result = (|| {
        let left_size = usize_to_u64(frame.left.len());
        let right_size = usize_to_u64(frame.right.len());
        let raw_size = usize_to_u64(frame.raw_side_by_side.len());
        let has_left = left_size > 0;
        let has_right = right_size > 0;
        let has_raw = raw_size > 0;
        record_copy(
            metrics,
            "native_capture_left_jpeg",
            left_size,
            u64::from(has_left),
        );
        record_copy(
            metrics,
            "native_capture_right_jpeg",
            right_size,
            u64::from(has_right),
        );
        record_copy(
            metrics,
            "native_capture_raw_sbs",
            raw_size,
            u64::from(has_raw),
        );
        let source_sequence = i64::try_from(frame.source_sequence).map_err(|_| {
            RuntimeError::new("bad_frame", "camera source sequence is out of range")
        })?;
        let host_monotonic_ns = i64::try_from(frame.host_monotonic_ns)
            .map_err(|_| RuntimeError::new("bad_frame", "camera timestamp is out of range"))?;
        let application_dropped_before =
            i64::try_from(frame.application_dropped_before).map_err(|_| {
                RuntimeError::new(
                    "invalid_drop_accounting",
                    "application dropped frame count is out of range",
                )
            })?;
        let corrupt_drops = frame.corrupt_dropped_before;
        let (current_source_frame, pre_extracted_jpeg) =
            SourceFrameEvidence::extract_from_frame(&frame);
        let (decision, validation, corrupt_recorded_drops, previous_source_frame, dispatch) = {
            let mut state = shared.state.lock().map_err(|_| {
                RuntimeError::new(
                    "capture_runtime_poisoned",
                    "capture runtime state mutex is poisoned",
                )
            })?;
            let validation = state
                .validator
                .validate(
                    source_sequence,
                    host_monotonic_ns,
                    true,
                    (has_left && has_right) || has_raw,
                    application_dropped_before,
                )
                .map_err(RuntimeError::from)?;
            let previous_source_frame = state.previous_source_frame.replace(current_source_frame);
            let explained_corrupt_gap = validation.source_gap.min(corrupt_drops);
            let unexplained_source_gap =
                validation.source_gap.saturating_sub(explained_corrupt_gap);
            let unexplained_dropped_before = validation
                .dropped_before
                .saturating_sub(explained_corrupt_gap);
            record_loss(metrics, "queue_rejected", validation.queue_rejected);
            record_loss(metrics, "corrupt_frame", corrupt_drops);
            record_loss(metrics, "source_gap", unexplained_source_gap);
            let recovery_time_ns = previous_source_frame
                .filter(|previous| {
                    unexplained_source_gap != 0
                        && validation.queue_rejected == 0
                        && frame.host_monotonic_ns - previous.host_monotonic_ns <= 150_000_000
                })
                .map(|_| frame.host_monotonic_ns);
            let decision = state
                .fanout
                .begin_frame_with_corrupt_drops(
                    unexplained_dropped_before,
                    explained_corrupt_gap,
                    has_left || has_raw,
                    recovery_time_ns,
                )
                .map_err(RuntimeError::from)?;
            let corrupt_recorded_drops = decision.corrupt_recorded_drops;
            let dispatch = if decision.record
                || decision.recovered_recorded_drops > 0
                || corrupt_recorded_drops > 0
            {
                state.recording.clone()
            } else {
                None
            };
            (
                decision,
                validation,
                corrupt_recorded_drops,
                previous_source_frame,
                dispatch,
            )
        };
        if decision.publish_preview {
            let publish_started = start_stage(metrics);
            let published = if has_left {
                preview.publish(&frame.left)
            } else {
                preview.publish_shared(Arc::clone(&frame.raw_side_by_side))
            };
            if let Err(error) = published {
                if let Ok(mut state) = shared.state.lock() {
                    state.last_preview_error = Some(RuntimeError::from(error));
                }
            }
            finish_stage(metrics, "native_preview_publish", publish_started);
        }
        if decision.recovered_source_gap > 0 {
            eprintln!(
                "recording continued after source gap: missing={}, next_source_sequence={}, host_monotonic_ns={}",
                decision.recovered_source_gap, frame.source_sequence, frame.host_monotonic_ns,
            );
        }
        if let Some(dispatch) = dispatch {
            if let Some(queue) = &dispatch.record_queue {
                if decision.record {
                    let work = RecordWork::Frame(Box::new(QueuedRecordFrame {
                        frame,
                        dropped_before: decision.dropped_before,
                        recovered_recorded_drops: decision.recovered_recorded_drops,
                        corrupt_recorded_drops,
                        validation_source_gap: validation.source_gap,
                        validation_queue_rejected: validation.queue_rejected,
                        previous_source_frame,
                        current_source_frame,
                    }));
                    match queue.try_enqueue(work)? {
                        EnqueueOutcome::Enqueued => {}
                        EnqueueOutcome::Closed => {
                            finish_recording_frame(shared)?;
                        }
                        EnqueueOutcome::Full => {
                            finish_recording_frame(shared)?;
                            report_recording_failure(
                                shared,
                                RuntimeError::new(
                                    "encoder_backpressure_exhausted",
                                    "capture recording frame queue exhausted",
                                ),
                                Some(Arc::clone(&dispatch.on_failure)),
                            );
                        }
                    }
                } else if enqueue_recovered_drops(
                    queue,
                    shared,
                    &dispatch,
                    corrupt_recorded_drops,
                    frame.host_monotonic_ns,
                    true,
                )? {
                    let _ = enqueue_recovered_drops(
                        queue,
                        shared,
                        &dispatch,
                        decision.recovered_recorded_drops,
                        frame.host_monotonic_ns,
                        false,
                    )?;
                }
            } else if decision.record {
                let write_started = start_stage(metrics);
                let write_result = write_split_sink_frame_with_drops(
                    &dispatch,
                    &frame,
                    decision.dropped_before,
                    decision.recovered_recorded_drops,
                    corrupt_recorded_drops,
                    pre_extracted_jpeg,
                    current_source_frame.camera_counter_raw,
                );
                finish_stage(metrics, "native_recording_split_sink", write_started);
                finish_recording_frame(shared)?;
                if let Err(mut error) = write_result {
                    decorate_source_gap_error(
                        &mut error,
                        &frame,
                        previous_source_frame,
                        current_source_frame,
                        validation.source_gap,
                        validation.queue_rejected,
                        shared,
                    );
                    report_recording_failure(shared, error, Some(Arc::clone(&dispatch.on_failure)));
                }
            } else {
                if corrupt_recorded_drops > 0 {
                    if let Err(error) = record_drops_on_take(
                        &dispatch,
                        corrupt_recorded_drops,
                        frame.host_monotonic_ns,
                        true,
                    ) {
                        report_recording_failure(
                            shared,
                            error,
                            Some(Arc::clone(&dispatch.on_failure)),
                        );
                    }
                }
                if decision.recovered_recorded_drops > 0 {
                    if let Err(error) = record_drops_on_take(
                        &dispatch,
                        decision.recovered_recorded_drops,
                        frame.host_monotonic_ns,
                        false,
                    ) {
                        report_recording_failure(
                            shared,
                            error,
                            Some(Arc::clone(&dispatch.on_failure)),
                        );
                    }
                }
            }
        }
        Ok(())
    })();
    if let Ok(mut state) = shared.state.lock() {
        state.max_consumer_frame_ns = state
            .max_consumer_frame_ns
            .max(frame_started.elapsed().as_nanos() as u64);
    }
    finish_stage(metrics, "native_capture_frame", Some(frame_started));
    result
}

fn enqueue_recovered_drops(
    queue: &RecordWorkQueue,
    shared: &Shared,
    dispatch: &SplitSinkRecording,
    dropped: u64,
    host_monotonic_ns: u64,
    corrupt: bool,
) -> Result<bool, RuntimeError> {
    if dropped == 0 {
        return Ok(true);
    }
    match queue.try_enqueue(RecordWork::RecoveredDrops {
        dropped,
        host_monotonic_ns,
        corrupt,
    })? {
        EnqueueOutcome::Enqueued => Ok(true),
        EnqueueOutcome::Closed => Ok(false),
        EnqueueOutcome::Full => {
            report_recording_failure(
                shared,
                RuntimeError::new(
                    "encoder_backpressure_exhausted",
                    "capture recording drop queue exhausted",
                ),
                Some(Arc::clone(&dispatch.on_failure)),
            );
            Ok(false)
        }
    }
}

fn recording_accepts_queued_work(shared: &Shared, recording: &SplitSinkRecording) -> bool {
    if recording.encoder_abort.is_aborted() {
        return false;
    }
    match shared.state.lock() {
        Ok(state) => {
            state.terminal_error.is_none()
                && !state.fanout.snapshot().failure_reported
                && state.recording.is_some()
        }
        Err(_) => false,
    }
}

struct PendingSyncedFrame {
    item: Box<QueuedRecordFrame>,
    hw_sample_start: u64,
}

#[allow(clippy::too_many_arguments)]
fn serialize_synced_frame_meta(
    seq: u64,
    vseq: u64,
    mono_ns: u64,
    raw_ns: u64,
    hw_s: u64,
    dur: u32,
    frame: &Frame,
    cam_ctr: Option<u32>,
    imu_samples: &[ImuSample],
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(192 + imu_samples.len() * 44);
    let _ = write!(
        buf,
        "{{\"seq\":{seq},\"vseq\":{vseq},\"mono_ns\":{mono_ns},\"raw_ns\":{raw_ns},\
         \"hw_s\":{hw_s},\"dur\":{dur},\"flags\":{},\"deq_ns\":{}",
        frame.buffer_flags, frame.dequeue_monotonic_ns
    );
    if let Some(ctr) = cam_ctr {
        let _ = write!(buf, ",\"cam_ctr\":{ctr}");
    }
    buf.extend_from_slice(b",\"imu\":[");
    for (idx, s) in imu_samples.iter().enumerate() {
        if idx > 0 {
            buf.push(b',');
        }
        let _ = write!(
            buf,
            "[{},{},{},{},{},{},{}]",
            s.host_monotonic_ns,
            s.accelerometer.x,
            s.accelerometer.y,
            s.accelerometer.z,
            s.gyroscope.x,
            s.gyroscope.y,
            s.gyroscope.z
        );
    }
    buf.extend_from_slice(b"]}");
    buf
}

#[allow(clippy::too_many_arguments)]
fn submit_synced_frame_pair(
    recording: &SplitSinkRecording,
    synced: &SyncedCaptureConfig,
    item: &QueuedRecordFrame,
    s_start: u64,
    dur: u32,
    pcm_timeout: Duration,
    imu_samples: &[ImuSample],
    allow_stop_abort: bool,
) -> Result<WriteFrameOutcome, RuntimeError> {
    let frame = &item.frame;
    if frame.raw_side_by_side.is_empty() {
        return Err(RuntimeError::new(
            "raw_frame_unavailable",
            "production split-eye recording is missing raw side-by-side MJPEG frame",
        ));
    }
    let payload = recording::jpeg_payload(&frame.raw_side_by_side)?;
    let elapsed_seconds = if frame.host_monotonic_ns >= recording.recording_start_monotonic_ns {
        (frame.host_monotonic_ns - recording.recording_start_monotonic_ns) as f64 / 1_000_000_000.0
    } else {
        0.0
    };

    let pcm_bytes = if let Some(audio) = &synced.audio {
        let s_end = s_start.saturating_add(u64::from(dur));
        let pcm = audio
            .wait_and_read_pcm_slice(s_start, s_end, pcm_timeout)
            .map_err(|e| RuntimeError::new(e.code, e.message))?;
        audio.drop_samples_before(s_end);
        pcm
    } else {
        Vec::new()
    };

    let reserved = {
        let mut writer = recording.active_take.lock().map_err(|_| {
            RuntimeError::new(
                "active_take_writer_poisoned",
                "active take writer mutex is poisoned",
            )
        })?;
        if item.corrupt_recorded_drops > 0 {
            writer.record_corrupt_drops(item.corrupt_recorded_drops, elapsed_seconds)?;
        }
        if item.recovered_recorded_drops > 0 {
            writer.record_recovered_drops(item.recovered_recorded_drops, elapsed_seconds)?;
        }
        writer.reserve_frame(active_take::ActiveSourceFrame {
            source_sequence: frame.source_sequence,
            host_monotonic_ns: frame.host_monotonic_ns,
            source_gap: item.dropped_before,
        })?
    };

    let cam_ctr = item
        .current_source_frame
        .camera_counter_raw
        .or_else(|| recording::avi1_counter(payload));
    let meta = serialize_synced_frame_meta(
        reserved.record_sequence,
        reserved.source_sequence,
        reserved.host_monotonic_ns,
        frame.host_monotonic_raw_ns,
        s_start,
        dur,
        frame,
        cam_ctr,
        imu_samples,
    );

    {
        let mut encoder = recording.encoder.lock().map_err(|_| {
            RuntimeError::new("encoder_failed", "encoder process mutex is poisoned")
        })?;
        let stopped_flag = &recording.capture_stopped_monotonic_ns;
        let bundle = SyncSlotBundle {
            frame_seq: reserved.record_sequence,
            monotonic_ns: reserved.host_monotonic_ns,
            sample_number: s_start,
            duration_ticks: dur,
            jpeg: payload,
            pcm: &pcm_bytes,
            meta: &meta,
        };
        if let Err(error) = encoder.submit_sync_bundle_with_stop_check(bundle, || {
            allow_stop_abort && stopped_flag.load(Ordering::Acquire) != 0
        }) {
            if error.code == "capture_stopped" {
                let mut writer = recording.active_take.lock().map_err(|_| {
                    RuntimeError::new(
                        "active_take_writer_poisoned",
                        "active take writer mutex is poisoned",
                    )
                })?;
                writer.cancel_pending_frame(reserved)?;
                return Ok(WriteFrameOutcome::Stopped);
            }
            return Err(encoder_runtime_error(error));
        }
    }
    synced
        .imu_queue
        .record_emitted(u64::try_from(imu_samples.len()).unwrap_or(0));
    let _plan = {
        let mut planner = recording.segment_planner.lock().map_err(|_| {
            RuntimeError::new(
                "native_recording_segment_planner_poisoned",
                "recording segment planner mutex is poisoned",
            )
        })?;
        planner.next_frame(reserved.record_sequence, elapsed_seconds)?
    };
    let total_bytes = u64::try_from(payload.len())
        .unwrap_or(u64::MAX)
        .saturating_add(u64::try_from(pcm_bytes.len()).unwrap_or(u64::MAX))
        .saturating_add(u64::try_from(meta.len()).unwrap_or(u64::MAX));
    let mut writer = recording.active_take.lock().map_err(|_| {
        RuntimeError::new(
            "active_take_writer_poisoned",
            "active take writer mutex is poisoned",
        )
    })?;
    writer.finish_frame(reserved, total_bytes)?;
    Ok(WriteFrameOutcome::Written)
}

fn compute_synced_interval(
    synced: &SyncedCaptureConfig,
    prev_raw_ns: u64,
    next_raw_ns: u64,
    s_k: u64,
) -> (u64, u32) {
    let default_dur = synced.timescale / synced.fps.max(1);
    if let Some(audio) = &synced.audio {
        let mapped = audio
            .map_raw_ns_to_sample(next_raw_ns)
            .unwrap_or_else(|| s_k.saturating_add(u64::from(default_dur)));
        let s_next = mapped.clamp(s_k.saturating_add(16), s_k.saturating_add(65_535));
        let dur = (s_next - s_k) as u32;
        (s_next, dur)
    } else {
        let dt_ns = next_raw_ns.saturating_sub(prev_raw_ns).max(1);
        let ticks = ((u128::from(dt_ns) * u128::from(synced.timescale)) / 1_000_000_000)
            .clamp(1, 65_535) as u32;
        (s_k.saturating_add(u64::from(ticks)), ticks)
    }
}

fn run_record_loop(
    recording: Arc<SplitSinkRecording>,
    queue: Arc<RecordWorkQueue>,
    shared: Arc<Shared>,
    metrics: Option<Arc<Metrics>>,
) {
    let mut synced_pending: Option<PendingSyncedFrame> = None;
    let mut last_synced_dur: Option<u32> = None;

    while let Some(work) = queue.pop() {
        match work {
            RecordWork::RecoveredDrops {
                dropped,
                host_monotonic_ns,
                corrupt,
            } => {
                if recording_accepts_queued_work(&shared, &recording) {
                    if let Err(error) =
                        record_drops_on_take(&recording, dropped, host_monotonic_ns, corrupt)
                    {
                        let remaining = queue.close_and_drain_remaining_frames();
                        for _ in 0..remaining {
                            let _ = finish_recording_frame(&shared);
                        }
                        report_recording_failure(
                            &shared,
                            error,
                            Some(Arc::clone(&recording.on_failure)),
                        );
                        return;
                    }
                }
            }
            RecordWork::Frame(item) => {
                if !recording_accepts_queued_work(&shared, &recording) {
                    let _ = finish_recording_frame(&shared);
                    continue;
                }
                let prev_sf = item.previous_source_frame;
                let curr_sf = item.current_source_frame;
                let v_gap = item.validation_source_gap;
                let v_q = item.validation_queue_rejected;
                let write_started = start_stage(metrics.as_ref());
                let mut err_frame: Option<Frame> = None;
                let write_result = if let Some(synced) = &recording.synced {
                    if let Some(prev) = synced_pending.take() {
                        let (s_next, dur) = compute_synced_interval(
                            synced,
                            prev.item.frame.host_monotonic_raw_ns,
                            item.frame.host_monotonic_raw_ns,
                            prev.hw_sample_start,
                        );
                        last_synced_dur = Some(dur);
                        let imu_samples =
                            synced.imu_queue.drain_up_to(item.frame.host_monotonic_ns);
                        let res = submit_synced_frame_pair(
                            &recording,
                            synced,
                            &prev.item,
                            prev.hw_sample_start,
                            dur,
                            Duration::from_millis(250),
                            &imu_samples,
                            true,
                        );
                        match res {
                            Ok(WriteFrameOutcome::Written) => {
                                synced_pending = Some(PendingSyncedFrame {
                                    item,
                                    hw_sample_start: s_next,
                                });
                                Ok(WriteFrameOutcome::Written)
                            }
                            other => {
                                err_frame = Some(item.frame);
                                other
                            }
                        }
                    } else {
                        let s0 = synced
                            .audio
                            .as_ref()
                            .and_then(|a| a.map_raw_ns_to_sample(item.frame.host_monotonic_raw_ns))
                            .unwrap_or(0);
                        let _ = synced.imu_queue.drain_up_to(item.frame.host_monotonic_ns);
                        synced_pending = Some(PendingSyncedFrame {
                            item,
                            hw_sample_start: s0,
                        });
                        Ok(WriteFrameOutcome::Written)
                    }
                } else {
                    let res = write_split_sink_frame_with_drops(
                        &recording,
                        &item.frame,
                        item.dropped_before,
                        item.recovered_recorded_drops,
                        item.corrupt_recorded_drops,
                        None,
                        curr_sf.camera_counter_raw,
                    );
                    if res.is_err() {
                        err_frame = Some(item.frame);
                    }
                    res
                };
                finish_stage(
                    metrics.as_ref(),
                    "native_recording_split_sink",
                    write_started,
                );
                let _ = finish_recording_frame(&shared);
                match write_result {
                    Ok(WriteFrameOutcome::Written) => {}
                    Ok(WriteFrameOutcome::Stopped) => {
                        let remaining = queue.close_and_drain_remaining_frames();
                        for _ in 0..remaining {
                            let _ = finish_recording_frame(&shared);
                        }
                        return;
                    }
                    Err(mut error) => {
                        if let Some(frame) = err_frame.as_ref() {
                            decorate_source_gap_error(
                                &mut error, frame, prev_sf, curr_sf, v_gap, v_q, &shared,
                            );
                        }
                        let remaining = queue.close_and_drain_remaining_frames();
                        for _ in 0..remaining {
                            let _ = finish_recording_frame(&shared);
                        }
                        report_recording_failure(
                            &shared,
                            error,
                            Some(Arc::clone(&recording.on_failure)),
                        );
                        return;
                    }
                }
            }
        }
    }

    // Flush the trailing 1-frame lookahead item on clean stop.
    if let (Some(synced), Some(prev)) = (&recording.synced, synced_pending.take()) {
        let clean = !recording.encoder_abort.is_aborted()
            && shared
                .state
                .lock()
                .is_ok_and(|s| s.terminal_error.is_none() && !s.fanout.snapshot().failure_reported);
        if clean {
            let default_dur = (synced.timescale / synced.fps.max(1)).clamp(16, 65_535);
            let dur = last_synced_dur.unwrap_or(default_dur).clamp(16, 65_535);
            let imu_samples = synced.imu_queue.drain_all();
            if let Err(error) = submit_synced_frame_pair(
                &recording,
                synced,
                &prev.item,
                prev.hw_sample_start,
                dur,
                Duration::from_millis(120),
                &imu_samples,
                false,
            ) {
                report_recording_failure(&shared, error, Some(Arc::clone(&recording.on_failure)));
            }
        }
    }
}

fn decorate_source_gap_error(
    error: &mut RuntimeError,
    frame: &Frame,
    previous_source_frame: Option<SourceFrameEvidence>,
    current_source_frame: SourceFrameEvidence,
    source_gap: u64,
    queue_rejected: u64,
    shared: &Shared,
) {
    if error.code == "source_sequence_gap" {
        error.message = format!(
            "{} (previous_sequence={}, current_sequence={}, source_gap={}, queue_rejected={}, producer_max_interval_ns={}, producer_max_control_ns={}, producer_max_read_ns={}, consumer_max_frame_ns={}); source_frame_evidence={}",
            error.message,
            previous_source_frame
                .map(|previous| previous.source_sequence)
                .unwrap_or(frame.source_sequence),
            frame.source_sequence,
            source_gap,
            queue_rejected,
            frame.producer_max_interval_ns,
            frame.producer_max_control_ns,
            frame.producer_max_read_ns,
            shared
                .state
                .lock()
                .map(|state| state.max_consumer_frame_ns)
                .unwrap_or(0),
            serde_json::json!({
                "schema": "miro-ego.source-frame-evidence.v1",
                "previous": previous_source_frame,
                "current": current_source_frame,
            }),
        );
    }
}

fn record_drops_on_take(
    recording: &SplitSinkRecording,
    dropped: u64,
    host_monotonic_ns: u64,
    corrupt: bool,
) -> Result<(), RuntimeError> {
    if dropped == 0 {
        return Ok(());
    }
    let elapsed_seconds = if host_monotonic_ns >= recording.recording_start_monotonic_ns {
        (host_monotonic_ns - recording.recording_start_monotonic_ns) as f64 / 1_000_000_000.0
    } else {
        0.0
    };
    let mut writer = recording.active_take.lock().map_err(|_| {
        RuntimeError::new(
            "active_take_writer_poisoned",
            "active take writer mutex is poisoned",
        )
    })?;
    if corrupt {
        writer.record_corrupt_drops(dropped, elapsed_seconds)?;
    } else {
        writer.record_recovered_drops(dropped, elapsed_seconds)?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteFrameOutcome {
    Written,
    Stopped,
}

#[cfg(test)]
fn write_split_sink_frame(
    recording: &SplitSinkRecording,
    frame: &Frame,
    dropped_before: u64,
    pre_extracted_jpeg: Option<&[u8]>,
    pre_extracted_counter: Option<u32>,
) -> Result<(), RuntimeError> {
    write_split_sink_frame_with_drops(
        recording,
        frame,
        dropped_before,
        0,
        0,
        pre_extracted_jpeg,
        pre_extracted_counter,
    )
    .map(|_| ())
}

fn write_split_sink_frame_with_drops(
    recording: &SplitSinkRecording,
    frame: &Frame,
    dropped_before: u64,
    recovered_recorded_drops: u64,
    corrupt_recorded_drops: u64,
    pre_extracted_jpeg: Option<&[u8]>,
    pre_extracted_counter: Option<u32>,
) -> Result<WriteFrameOutcome, RuntimeError> {
    if frame.raw_side_by_side.is_empty() {
        return Err(RuntimeError::new(
            "raw_frame_unavailable",
            "production split-eye recording is missing raw side-by-side MJPEG frame",
        ));
    }
    let payload = match pre_extracted_jpeg {
        Some(payload) => payload,
        None => recording::jpeg_payload(&frame.raw_side_by_side)?,
    };
    let elapsed_seconds = if frame.host_monotonic_ns >= recording.recording_start_monotonic_ns {
        (frame.host_monotonic_ns - recording.recording_start_monotonic_ns) as f64 / 1_000_000_000.0
    } else {
        0.0
    };
    let reserved = {
        let mut writer = recording.active_take.lock().map_err(|_| {
            RuntimeError::new(
                "active_take_writer_poisoned",
                "active take writer mutex is poisoned",
            )
        })?;
        if corrupt_recorded_drops > 0 {
            writer.record_corrupt_drops(corrupt_recorded_drops, elapsed_seconds)?;
        }
        if recovered_recorded_drops > 0 {
            writer.record_recovered_drops(recovered_recorded_drops, elapsed_seconds)?;
        }
        writer.reserve_frame(active_take::ActiveSourceFrame {
            source_sequence: frame.source_sequence,
            host_monotonic_ns: frame.host_monotonic_ns,
            source_gap: dropped_before,
        })?
    };
    {
        let mut encoder = recording.encoder.lock().map_err(|_| {
            RuntimeError::new("encoder_failed", "encoder process mutex is poisoned")
        })?;
        let stopped_flag = &recording.capture_stopped_monotonic_ns;
        if let Err(error) = encoder.submit_frame_with_stop_check(
            reserved.record_sequence,
            reserved.host_monotonic_ns,
            payload,
            || stopped_flag.load(Ordering::Acquire) != 0,
        ) {
            if error.code == "capture_stopped" {
                let mut writer = recording.active_take.lock().map_err(|_| {
                    RuntimeError::new(
                        "active_take_writer_poisoned",
                        "active take writer mutex is poisoned",
                    )
                })?;
                writer.cancel_pending_frame(reserved)?;
                return Ok(WriteFrameOutcome::Stopped);
            }
            return Err(encoder_runtime_error(error));
        }
    }
    let plan = {
        let mut planner = recording.segment_planner.lock().map_err(|_| {
            RuntimeError::new(
                "native_recording_segment_planner_poisoned",
                "recording segment planner mutex is poisoned",
            )
        })?;
        planner.next_frame(reserved.record_sequence, elapsed_seconds)?
    };
    let camera_counter_raw = pre_extracted_counter.or_else(|| recording::avi1_counter(payload));
    let written = {
        let mut sink = recording.sink.lock().map_err(|_| {
            RuntimeError::new(
                "native_recording_poisoned",
                "recording sink mutex is poisoned",
            )
        })?;
        sink.write_audited_frame_index(
            reserved.record_sequence,
            reserved.source_sequence,
            reserved.host_monotonic_ns,
            plan.segment_index,
            plan.segment_frame,
            Some(FrameTimestampAudit {
                camera_counter_raw,
                counter_bits: 24,
                host_dequeue_monotonic_ns: frame.dequeue_monotonic_ns,
                schema: "miro-ego.frame-timestamp-audit.v1",
                timestamp_clock: if v4l2::is_monotonic_timestamp(frame.buffer_flags) {
                    "v4l2_monotonic"
                } else {
                    "host_monotonic_fallback"
                },
                timestamp_source: if frame.buffer_flags & 0x70000 == 0x10000 {
                    "start_of_exposure"
                } else if frame.buffer_flags & 0x70000 == 0 {
                    "end_of_frame"
                } else {
                    "unknown"
                },
                v4l2_buffer_flags: frame.buffer_flags,
            }),
        )?
    };
    {
        let mut writer = recording.active_take.lock().map_err(|_| {
            RuntimeError::new(
                "active_take_writer_poisoned",
                "active take writer mutex is poisoned",
            )
        })?;
        writer.finish_frame(reserved, written)?;
    }
    Ok(WriteFrameOutcome::Written)
}

fn encoder_runtime_error(error: EncoderProcessError) -> RuntimeError {
    let code = if error.code.is_empty() {
        Cow::Borrowed("encoder_failed")
    } else {
        Cow::Owned(error.code)
    };
    RuntimeError::new(code, error.message)
}

fn finish_recording_frame(shared: &Shared) -> Result<(), RuntimeError> {
    let mut state = shared.state.lock().map_err(|_| {
        RuntimeError::new(
            "capture_runtime_poisoned",
            "capture runtime state mutex is poisoned",
        )
    })?;
    state.fanout.finish_frame()?;
    if !state.fanout.snapshot().recording_present {
        state.recording = None;
    }
    shared.changed.notify_all();
    Ok(())
}

const IMU_RETRYABLE_STALL_BUDGET: Duration = Duration::from_millis(750);
const IMU_RETRY_MIN_BACKOFF: Duration = Duration::from_millis(2);

struct ImuLoopContext {
    collector: Arc<Collector>,
    sink: Arc<Mutex<recording::RecordingSink>>,
    on_failure: Arc<dyn FailureSink>,
    audio: Option<Arc<Recorder>>,
    synced_audio: Option<Arc<SyncedAudioCapture>>,
    synced_imu: Option<Arc<SyncedImuQueue>>,
    shared: Arc<Shared>,
    stop: Arc<AtomicBool>,
    timeout: Duration,
    stall_budget: Duration,
    metrics: Option<Arc<Metrics>>,
}

fn run_imu_loop(ctx: ImuLoopContext) {
    let ImuLoopContext {
        collector,
        sink,
        on_failure,
        audio,
        synced_audio,
        synced_imu,
        shared,
        stop,
        timeout,
        stall_budget,
        metrics,
    } = ctx;
    let mut stall_deadline: Option<Instant> = None;
    loop {
        if stop.load(Ordering::Acquire) || !recording_present(&shared) {
            break;
        }
        if let Some(audio) = &audio {
            if let Err(error) = audio.check_health() {
                report_recording_failure(
                    &shared,
                    RuntimeError::new(error.code, error.message),
                    Some(Arc::clone(&on_failure)),
                );
                break;
            }
        }
        if let Some(audio) = &synced_audio {
            if let Err(error) = audio.check_health() {
                report_recording_failure(
                    &shared,
                    RuntimeError::new(error.code, error.message),
                    Some(Arc::clone(&on_failure)),
                );
                break;
            }
        }
        let call_started = Instant::now();
        let read_started = start_stage(metrics.as_ref());
        let read_result = collector.read(timeout);
        finish_stage(metrics.as_ref(), "native_imu_read", read_started);
        match read_result {
            Ok(observation) => {
                stall_deadline = None;
                if stop.load(Ordering::Acquire) || !recording_present(&shared) {
                    break;
                }
                if let Some(imu_queue) = &synced_imu {
                    imu_queue.push_samples(&observation.samples);
                    continue;
                }
                let write_started = start_stage(metrics.as_ref());
                let failure = sink
                    .lock()
                    .map_err(|_| {
                        RuntimeError::new(
                            "native_recording_poisoned",
                            "recording sink mutex is poisoned",
                        )
                    })
                    .and_then(|mut sink| {
                        sink.write_imu_observation(&observation)
                            .map(|_| ())
                            .map_err(RuntimeError::from)
                    })
                    .err();
                finish_stage(metrics.as_ref(), "native_imu_sink", write_started);
                if let Some(error) = failure {
                    if !stop.load(Ordering::Acquire) && recording_present(&shared) {
                        report_recording_failure(&shared, error, Some(Arc::clone(&on_failure)));
                    }
                    break;
                }
            }
            Err(error) => {
                if stop.load(Ordering::Acquire) || !recording_present(&shared) {
                    break;
                }
                if error.retryable && !stall_budget.is_zero() {
                    let now = Instant::now();
                    let deadline = *stall_deadline.get_or_insert_with(|| now + stall_budget);
                    if now < deadline {
                        let read_elapsed = call_started.elapsed();
                        if read_elapsed < IMU_RETRY_MIN_BACKOFF {
                            let sleep_for = (IMU_RETRY_MIN_BACKOFF - read_elapsed)
                                .min(deadline.saturating_duration_since(Instant::now()));
                            if !sleep_for.is_zero() {
                                thread::sleep(sleep_for);
                            }
                        }
                        continue;
                    }
                }
                report_recording_failure(&shared, error.into(), Some(Arc::clone(&on_failure)));
                break;
            }
        }
    }
    collector.close();
}

fn start_stage(metrics: Option<&Arc<Metrics>>) -> Option<Instant> {
    metrics.map(|_| Instant::now())
}

fn finish_stage(metrics: Option<&Arc<Metrics>>, name: &str, started: Option<Instant>) {
    if let (Some(metrics), Some(started)) = (metrics, started) {
        let _ = metrics.record_stage(name, elapsed_ns(started));
    }
}

fn record_copy(metrics: Option<&Arc<Metrics>>, name: &str, size: u64, count: u64) {
    if count == 0 {
        return;
    }
    if let Some(metrics) = metrics {
        let _ = metrics.record_copy(name, size, count);
    }
}

fn record_loss(metrics: Option<&Arc<Metrics>>, kind: &str, count: u64) {
    if count == 0 {
        return;
    }
    if let Some(metrics) = metrics {
        let _ = metrics.record_loss(kind, count);
    }
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn recording_present(shared: &Shared) -> bool {
    match shared.state.lock() {
        Ok(state) => state.recording.is_some() && state.fanout.snapshot().recording_present,
        Err(_) => false,
    }
}

fn report_recording_failure(
    shared: &Shared,
    error: RuntimeError,
    preferred_callback: Option<Arc<dyn FailureSink>>,
) {
    shared.imu_stop.store(true, Ordering::Release);
    let callback = {
        let mut state = match shared.state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        let (should_report, _inflight) = state.fanout.mark_failure();
        if let Some(recording) = &state.recording {
            recording.request_capture_stop();
        }
        let callback = if should_report {
            preferred_callback.or_else(|| {
                state
                    .recording
                    .as_ref()
                    .map(|target| Arc::clone(&target.on_failure))
            })
        } else {
            None
        };
        if !state.fanout.snapshot().recording_present {
            state.recording = None;
        }
        shared.changed.notify_all();
        callback
    };
    if let Some(callback) = callback {
        callback.on_failure(&error.code, &error.message);
    }
}

fn set_terminal_error(shared: &Shared, error: RuntimeError) {
    shared.imu_stop.store(true, Ordering::Release);
    let callback = {
        let mut state = match shared.state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        state.terminal_error = Some(error.clone());
        state.running = false;
        let (should_report, _inflight) = state.fanout.mark_failure();
        if let Some(recording) = &state.recording {
            recording.request_capture_stop();
        }
        let callback = if should_report {
            state
                .recording
                .as_ref()
                .map(|target| Arc::clone(&target.on_failure))
        } else {
            None
        };
        state.recording = None;
        shared.changed.notify_all();
        callback
    };
    if let Some(callback) = callback {
        callback.on_failure(&error.code, &error.message);
    }
}

fn snapshot_locked(state: &State) -> RuntimeSnapshot {
    let fanout = state.fanout.snapshot();
    RuntimeSnapshot {
        running: state.running,
        recording_present: state.recording.is_some() || fanout.recording_present,
        recording_active: fanout.recording_active,
        inflight_frames: fanout.inflight_frames,
        observed_frames: fanout.observed_frames,
        failure_reported: fanout.failure_reported,
        terminal_error: state.terminal_error.clone(),
        last_preview_error: state.last_preview_error.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hardware::native_camera::Stream;
    use crate::hardware::preview::LatestBuffer;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Default)]
    struct TestFailureSink {
        calls: Mutex<Vec<(String, String)>>,
    }

    impl FailureSink for TestFailureSink {
        fn on_failure(&self, code: &str, message: &str) {
            self.calls
                .lock()
                .unwrap()
                .push((code.to_owned(), message.to_owned()));
        }
    }

    fn temp_root(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("miro-ego-{name}-{}-{unique}", std::process::id()))
    }

    #[test]
    fn capture_preview_shares_the_owned_raw_frame_without_copying() {
        let runtime = Runtime::new(
            Arc::new(Stream::test_idle(1)),
            Arc::new(LatestBuffer::new(25).unwrap()),
            2,
            Duration::from_millis(10),
            None,
        )
        .unwrap();
        let raw: Arc<[u8]> = Arc::from(&b"\xff\xd8payload\xff\xd9"[..]);
        process_frame(
            &runtime.preview,
            &runtime.shared,
            Frame {
                buffer_flags: 0,
                dequeue_monotonic_ns: 0,
                source_sequence: 1,
                host_monotonic_ns: 1_000_000,
                host_monotonic_raw_ns: 1_000_000,
                application_dropped_before: 0,
                corrupt_dropped_before: 0,
                producer_max_interval_ns: 0,
                producer_max_control_ns: 0,
                producer_max_read_ns: 0,
                left: Vec::new(),
                right: Vec::new(),
                raw_side_by_side: Arc::clone(&raw),
            },
            None,
        )
        .unwrap();
        let snapshot = runtime.preview.snapshot().unwrap();
        assert!(Arc::ptr_eq(&raw, &snapshot.jpeg));
        assert_eq!(snapshot.sequence, 1);
        assert!(!runtime.snapshot().unwrap().recording_present);
    }

    fn test_recording(root: &std::path::Path, sink_cb: Arc<TestFailureSink>) -> SplitSinkRecording {
        fs::create_dir_all(root.join("video")).unwrap();
        let mut encoder = stereo_encoder::EncoderProcess::with_transport(
            stereo_encoder::EncoderConfig {
                out_dir: &root.join("video"),
                executable: std::path::Path::new("/bin/cat"),
                width: 3840,
                height: 1080,
                fps: 60,
                bitrate_kbps: 8192,
                segment_frames: 3,
                path_prefix: "video/",
            },
            stereo_encoder::EncoderTransport::StdinPipe,
        )
        .unwrap();
        encoder
            .configure(&["--intra-period".to_owned(), "3".to_owned()])
            .unwrap();
        let encoder_abort = encoder.abort_handle();
        SplitSinkRecording {
            active_take: Arc::new(Mutex::new(
                active_take::ActiveTakeWriter::new("session").unwrap(),
            )),
            sink: Arc::new(Mutex::new(
                recording::RecordingSink::create(root, "session").unwrap(),
            )),
            encoder: Arc::new(Mutex::new(encoder)),
            encoder_abort,
            segment_planner: Arc::new(Mutex::new(
                recording::RecordingSegmentPlanner::new(3).unwrap(),
            )),
            recording_start_monotonic_ns: 1,
            capture_stopped_monotonic_ns: Arc::new(AtomicU64::new(0)),
            on_failure: sink_cb,
            imu: None,
            audio: None,
            synced: None,
            record_queue: None,
        }
    }

    fn evidence_frame(sequence: u64, counter: Option<u32>) -> Frame {
        let mut raw = b"prefix\xff\xd8".to_vec();
        if let Some(counter) = counter {
            raw.extend_from_slice(b"\xff\xe0\x00\x21");
            let mut app0 = [0; 31];
            app0[..8].copy_from_slice(b"AVI1\0\x01\x01\x01");
            app0[10..13].copy_from_slice(&counter.to_be_bytes()[1..]);
            raw.extend_from_slice(&app0);
        }
        raw.extend_from_slice(b"\xff\xd9");
        Frame {
            buffer_flags: 0x2000,
            dequeue_monotonic_ns: sequence * 20_000_000 + 100_000,
            source_sequence: sequence,
            host_monotonic_ns: sequence * 20_000_000,
            host_monotonic_raw_ns: sequence * 20_000_000,
            application_dropped_before: 0,
            corrupt_dropped_before: 0,
            producer_max_interval_ns: 0,
            producer_max_control_ns: 0,
            producer_max_read_ns: 0,
            left: Vec::new(),
            right: Vec::new(),
            raw_side_by_side: raw.into(),
        }
    }

    #[test]
    fn source_gap_callback_keeps_real_adjacent_frames_and_rejects_gap_before_reservation() {
        // A gap on a normally decimated frame must still fail immediately.
        for decimation in [1, 2, 3] {
            let root = temp_root("source-gap-evidence");
            let callbacks = Arc::new(TestFailureSink::default());
            let mut target = test_recording(&root, Arc::clone(&callbacks));
            let helper = root.join("encoder-helper.sh");
            fs::write(
                &helper,
                "#!/bin/sh\nprintf '{\"event\":\"ready\"}\\n'\ncat >/dev/null\n",
            )
            .unwrap();
            fs::set_permissions(&helper, fs::Permissions::from_mode(0o755)).unwrap();
            let mut encoder = stereo_encoder::EncoderProcess::with_transport(
                stereo_encoder::EncoderConfig {
                    out_dir: &root.join("video"),
                    executable: &helper,
                    width: 3840,
                    height: 1080,
                    fps: 60,
                    bitrate_kbps: 8192,
                    segment_frames: 3,
                    path_prefix: "video/",
                },
                stereo_encoder::EncoderTransport::StdinPipe,
            )
            .unwrap();
            encoder
                .configure(&["--intra-period".to_owned(), "3".to_owned()])
                .unwrap();
            encoder.start().unwrap();
            target.encoder_abort = encoder.abort_handle();
            target.encoder = Arc::new(Mutex::new(encoder));
            let active_take = Arc::clone(&target.active_take);
            let encoder = Arc::clone(&target.encoder);
            let runtime = Runtime::new(
                Arc::new(Stream::test_idle(1)),
                Arc::new(LatestBuffer::new(25).unwrap()),
                decimation,
                Duration::from_millis(10),
                None,
            )
            .unwrap();
            // Preview-only gap at the recording boundary belongs to preview.
            process_frame(
                &runtime.preview,
                &runtime.shared,
                evidence_frame(1, Some(101)),
                None,
            )
            .unwrap();
            {
                let mut state = runtime.shared.state.lock().unwrap();
                state.running = true;
                state.fanout.start_recording(false).unwrap();
                state.recording = Some(Arc::new(target));
            }
            process_frame(
                &runtime.preview,
                &runtime.shared,
                evidence_frame(10, Some(110)),
                None,
            )
            .unwrap();
            assert!(!runtime.snapshot().unwrap().failure_reported);
            process_frame(
                &runtime.preview,
                &runtime.shared,
                evidence_frame(13, Some(113)),
                None,
            )
            .unwrap();
            assert!(!runtime.snapshot().unwrap().recording_present);
            assert_eq!(encoder.lock().unwrap().submitted_frames(), 1);
            let summary = active_take.lock().unwrap().finish().unwrap();
            assert_eq!(summary.frames_written, 1);
            assert_eq!(summary.frame_domain, 1);
            assert_eq!(summary.pending_frames, 0);
            let calls = callbacks.calls.lock().unwrap().clone();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].0, "source_sequence_gap");
            assert!(calls[0].1.contains(
                "previous_sequence=10, current_sequence=13, source_gap=2, queue_rejected=0"
            ));
            let (_, evidence) = calls[0].1.split_once("; source_frame_evidence=").unwrap();
            let evidence: serde_json::Value = serde_json::from_str(evidence).unwrap();
            assert_eq!(evidence["previous"]["source_sequence"], 10);
            assert_eq!(evidence["previous"]["camera_counter_raw"], 110);
            assert_eq!(evidence["current"]["camera_counter_raw"], 113);
            assert_eq!(evidence["previous"]["host_monotonic_ns"], 200_000_000);
            assert_eq!(evidence["current"]["host_monotonic_ns"], 260_000_000);
            assert_eq!(evidence["current"]["dequeue_monotonic_ns"], 260_100_000);
            assert_eq!(evidence["current"]["buffer_flags"], 0x2000);
            drop(runtime);
            drop(encoder);
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn source_frame_evidence_does_not_invent_a_missing_camera_counter() {
        let frame = evidence_frame(7, None);
        let evidence = SourceFrameEvidence::from_frame(&frame);
        assert_eq!(evidence.source_sequence, 7);
        assert_eq!(evidence.camera_counter_raw, None);
        assert_eq!(evidence.payload_bytes, frame.raw_side_by_side.len() as u64);
    }

    #[test]
    fn encoder_runtime_error_preserves_helper_error_code() {
        let err = super::encoder_runtime_error(EncoderProcessError {
            code: "vpu_hw_timeout".to_owned(),
            message: "timed out".to_owned(),
        });
        assert_eq!(err.code, "vpu_hw_timeout");
        assert_eq!(err.message, "timed out");
    }

    #[test]
    fn split_sink_frame_writes_index_and_finishes_active_take() {
        let root = temp_root("split-sink");
        fs::create_dir_all(root.join("video")).unwrap();
        let helper = root.join("encoder-helper.sh");
        fs::write(
            &helper,
            "#!/bin/sh\nprintf '{\"event\":\"ready\"}\\n'\ncat >/dev/null\n",
        )
        .unwrap();
        let mut permissions = fs::metadata(&helper).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&helper, permissions).unwrap();

        {
            let mut target = test_recording(&root, Arc::new(TestFailureSink::default()));
            let mut encoder = stereo_encoder::EncoderProcess::with_transport(
                stereo_encoder::EncoderConfig {
                    out_dir: &root.join("video"),
                    executable: &helper,
                    width: 3840,
                    height: 1080,
                    fps: 60,
                    bitrate_kbps: 8192,
                    segment_frames: 3,
                    path_prefix: "video/",
                },
                stereo_encoder::EncoderTransport::StdinPipe,
            )
            .unwrap();
            encoder
                .configure(&["--intra-period".to_owned(), "3".to_owned()])
                .unwrap();
            encoder.start().unwrap();
            target.encoder_abort = encoder.abort_handle();
            target.encoder = Arc::new(Mutex::new(encoder));
            target.recording_start_monotonic_ns = 1_000_000;

            let frame = Frame {
                buffer_flags: 0,
                dequeue_monotonic_ns: 0,
                source_sequence: 9,
                host_monotonic_ns: 34_000_000,
                host_monotonic_raw_ns: 34_000_000,
                application_dropped_before: 0,
                corrupt_dropped_before: 0,
                producer_max_interval_ns: 0,
                producer_max_control_ns: 0,
                producer_max_read_ns: 0,
                left: Vec::new(),
                right: Vec::new(),
                raw_side_by_side: Arc::from(&b"prefix\xff\xd8payload\xff\xd9suffix"[..]),
            };
            write_split_sink_frame(&target, &frame, 0, None, None).unwrap();

            let summary = target.active_take.lock().unwrap().finish().unwrap();
            assert_eq!(summary.frames_written, 1);
            assert_eq!(summary.frame_domain, 1);
            assert_eq!(summary.pending_frames, 0);
            let snapshot = target.sink.lock().unwrap().flush_and_close().unwrap();
            assert_eq!(snapshot.frames_written, 1);
            assert_eq!(snapshot.imu_samples_written, 0);
            assert!(snapshot.bytes_written > 0);
            assert!(
                snapshot
                    .artifacts
                    .iter()
                    .any(|artifact| artifact.relative_path == "frames.ndjson")
            );
            assert_eq!(target.encoder.lock().unwrap().submitted_frames(), 1);
            assert_eq!(
                target
                    .segment_planner
                    .lock()
                    .unwrap()
                    .snapshot()
                    .frames_written,
                1
            );
        }

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_recording_reaps_failed_imu_worker_and_allows_retry() {
        let root = temp_root("failed-recording-retry");
        let runtime = Runtime::new(
            Arc::new(Stream::test_idle(1)),
            Arc::new(LatestBuffer::new(30).unwrap()),
            1,
            Duration::from_millis(10),
            None,
        )
        .unwrap();
        {
            let mut state = runtime.shared.state.lock().unwrap();
            state.running = true;
            state.fanout.start_recording(false).unwrap();
            state.recording = Some(Arc::new(test_recording(
                &root,
                Arc::new(TestFailureSink::default()),
            )));
        }
        let stop = Arc::clone(&runtime.imu_stop);
        let (done_tx, done_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            // Exercise cleanup while the old monitoring thread still exists.
            while !stop.load(Ordering::Acquire) {
                thread::yield_now();
            }
            let _ = done_tx.send(());
        });
        *runtime.imu_worker.lock().unwrap() = Some(WorkerHandle {
            handle,
            done: done_rx,
        });
        report_recording_failure(
            &runtime.shared,
            RuntimeError::new(
                "source_sequence_gap",
                "source frame sequence has a gap of 8",
            ),
            None,
        );
        assert!(runtime.imu_stop.load(Ordering::Acquire));
        assert!(!runtime.snapshot().unwrap().recording_present);
        assert!(runtime.imu_worker.lock().unwrap().is_some());
        runtime.prepare_recording(Duration::from_secs(1)).unwrap();
        assert!(runtime.imu_worker.lock().unwrap().is_none());
        let next = test_recording(&root.join("next"), Arc::new(TestFailureSink::default()));
        let snapshot = runtime
            .start_recording(RecordingTarget {
                active_take: Arc::clone(&next.active_take),
                sink: Arc::clone(&next.sink),
                encoder: Arc::clone(&next.encoder),
                segment_planner: Arc::clone(&next.segment_planner),
                recording_start_monotonic_ns: 1,
                on_failure: Arc::clone(&next.on_failure),
                imu: None,
                imu_timeout: Duration::from_millis(10),
                audio: None,
                capture_stopped_monotonic_ns: Arc::clone(&next.capture_stopped_monotonic_ns),
                recover_source_gaps: false,
                synced: None,
            })
            .unwrap();
        assert!(snapshot.recording_active);
        let error = runtime
            .prepare_recording(Duration::from_secs(1))
            .unwrap_err();
        assert_eq!(error.code, "invalid_state");
        assert!(runtime.snapshot().unwrap().recording_active);
        runtime.stop_recording(Duration::from_secs(1)).unwrap();
        assert!(next.capture_stopped_monotonic_ns.load(Ordering::Acquire) > 0);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn close_still_joins_worker_and_closes_stream_when_imu_stop_times_out() {
        let stream = Arc::new(Stream::test_idle(1));
        let runtime = Runtime::new(
            Arc::clone(&stream),
            Arc::new(LatestBuffer::new(30).unwrap()),
            1,
            Duration::from_millis(10),
            None,
        )
        .unwrap();
        {
            let mut state = runtime.shared.state.lock().unwrap();
            state.running = true;
        }
        let stop = Arc::clone(&runtime.stop);
        let worker = thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                thread::yield_now();
            }
        });
        *runtime.worker.lock().unwrap() = Some(worker);
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let (_done_tx, done_rx) = mpsc::channel();
        let stalled_imu = thread::spawn(move || {
            let _ = release_rx.recv();
        });
        *runtime.imu_worker.lock().unwrap() = Some(WorkerHandle {
            handle: stalled_imu,
            done: done_rx,
        });
        let err = runtime.close(Duration::from_millis(20)).unwrap_err();
        assert_eq!(err.code, "capture_runtime_imu_stop_timeout");
        // Stream worker must still be joined and running cleared despite IMU timeout.
        assert!(runtime.worker.lock().unwrap().is_none());
        assert!(!runtime.snapshot().unwrap().running);
        let _ = release_tx.send(());
        if let Some(handle) = runtime.imu_worker.lock().unwrap().take() {
            let _ = handle.handle.join();
        }
    }

    #[test]
    fn stop_recording_clears_imu_worker_when_no_frames_are_inflight() {
        let root = temp_root("stop-recording");
        let runtime = Runtime::new(
            Arc::new(Stream::test_idle(1)),
            Arc::new(LatestBuffer::new(30).unwrap()),
            1,
            Duration::from_millis(10),
            None,
        )
        .unwrap();
        {
            let mut state = runtime.shared.state.lock().unwrap();
            state.running = true;
            state.fanout.start_recording(false).unwrap();
            state.recording = Some(Arc::new(test_recording(
                &root,
                Arc::new(TestFailureSink::default()),
            )));
        }
        let (done_tx, done_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let _ = done_tx.send(());
        });
        *runtime.imu_worker.lock().unwrap() = Some(WorkerHandle {
            handle,
            done: done_rx,
        });

        let snapshot = runtime.stop_recording(Duration::from_secs(1)).unwrap();

        assert!(!snapshot.recording_present);
        assert!(
            runtime.imu_worker.lock().unwrap().is_none(),
            "stop_recording must clear a completed IMU worker even when no frames are inflight",
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn begin_recording_stop_closes_frame_gate_before_resource_cleanup() {
        let root = temp_root("begin-stop");
        let runtime = Runtime::new(
            Arc::new(Stream::test_idle(1)),
            Arc::new(LatestBuffer::new(30).unwrap()),
            1,
            Duration::from_millis(10),
            None,
        )
        .unwrap();
        let audio = Arc::new(Recorder::new(root.to_str().unwrap(), "test", 48000, 2, 5.0).unwrap());
        {
            let mut state = runtime.shared.state.lock().unwrap();
            state.running = true;
            state.fanout.start_recording(false).unwrap();
            let mut target = test_recording(&root, Arc::new(TestFailureSink::default()));
            target.audio = Some(Arc::clone(&audio));
            state.recording = Some(Arc::new(target));
        }
        assert!(!audio.stop_requested());
        let (inflight, imu) = runtime.begin_recording_stop().unwrap();
        assert!(audio.stop_requested());
        assert_eq!(inflight, 0);
        assert!(imu.is_none());
        {
            let mut state = runtime.shared.state.lock().unwrap();
            assert!(state.recording.is_some());
            let fanout = state.fanout.snapshot();
            assert!(!fanout.recording_active);
            assert!(!fanout.recording_present);
            state.recording = None;
        }
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn recording_failure_terminal_error_and_close_signal_audio_stop() {
        for cause in ["recording", "terminal", "close"] {
            let root = temp_root("audio-stop-failure");
            let runtime = Runtime::new(
                Arc::new(Stream::test_idle(1)),
                Arc::new(LatestBuffer::new(30).unwrap()),
                1,
                Duration::from_millis(10),
                None,
            )
            .unwrap();
            let audio =
                Arc::new(Recorder::new(root.to_str().unwrap(), "test", 48000, 2, 5.0).unwrap());
            let mut target = test_recording(&root, Arc::new(TestFailureSink::default()));
            target.audio = Some(Arc::clone(&audio));
            {
                let mut state = runtime.shared.state.lock().unwrap();
                state.running = true;
                state.fanout.start_recording(false).unwrap();
                state.recording = Some(Arc::new(target));
            }
            assert!(!audio.stop_requested());
            let error = super::RuntimeError::new("audio_failed", "test failure");
            match cause {
                "recording" => super::report_recording_failure(&runtime.shared, error, None),
                "terminal" => super::set_terminal_error(&runtime.shared, error),
                "close" => {
                    runtime.close(Duration::from_secs(1)).unwrap();
                }
                _ => unreachable!(),
            }
            assert!(audio.stop_requested());
            assert!(runtime.imu_stop.load(Ordering::Acquire));
            assert!(!runtime.snapshot().unwrap().recording_present);
            drop(runtime);
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn stop_preserves_inflight_frame_until_encoder_and_index_finish() {
        let root = temp_root("inflight-stop");
        let mut target = test_recording(&root, Arc::new(TestFailureSink::default()));
        let helper = root.join("encoder-helper.sh");
        fs::write(
            &helper,
            "#!/bin/sh\nprintf '{\"event\":\"ready\"}\\n'\ncat >/dev/null\n",
        )
        .unwrap();
        fs::set_permissions(&helper, fs::Permissions::from_mode(0o755)).unwrap();
        let mut encoder = stereo_encoder::EncoderProcess::with_transport(
            stereo_encoder::EncoderConfig {
                out_dir: &root.join("video"),
                executable: &helper,
                width: 3840,
                height: 1080,
                fps: 60,
                bitrate_kbps: 8192,
                segment_frames: 3,
                path_prefix: "video/",
            },
            stereo_encoder::EncoderTransport::StdinPipe,
        )
        .unwrap();
        encoder
            .configure(&["--intra-period".to_owned(), "3".to_owned()])
            .unwrap();
        encoder.start().unwrap();
        target.encoder_abort = encoder.abort_handle();
        target.encoder = Arc::new(Mutex::new(encoder));
        let runtime = Runtime::new(
            Arc::new(Stream::test_idle(1)),
            Arc::new(LatestBuffer::new(30).unwrap()),
            1,
            Duration::from_millis(10),
            None,
        )
        .unwrap();
        let stopped = Arc::clone(&target.capture_stopped_monotonic_ns);
        let active_take = Arc::clone(&target.active_take);
        let sink = Arc::clone(&target.sink);
        let blocked_writer = active_take.lock().unwrap();
        {
            let mut state = runtime.shared.state.lock().unwrap();
            state.running = true;
            state.fanout.start_recording(false).unwrap();
            state.recording = Some(Arc::new(target));
        }
        let shared = Arc::clone(&runtime.shared);
        let preview = Arc::clone(&runtime.preview);
        let worker = thread::spawn(move || {
            process_frame(
                &preview,
                &shared,
                Frame {
                    buffer_flags: 0,
                    dequeue_monotonic_ns: 0,
                    source_sequence: 1,
                    host_monotonic_ns: 1_000_000,
                    host_monotonic_raw_ns: 1_000_000,
                    application_dropped_before: 0,
                    corrupt_dropped_before: 0,
                    producer_max_interval_ns: 0,
                    producer_max_control_ns: 0,
                    producer_max_read_ns: 0,
                    left: Vec::new(),
                    right: Vec::new(),
                    raw_side_by_side: Arc::from(&b"\xff\xd8payload\xff\xd9"[..]),
                },
                None,
            )
        });
        let started = Instant::now();
        while runtime.snapshot().unwrap().inflight_frames != 1 {
            assert!(started.elapsed() < Duration::from_secs(2));
            thread::yield_now();
        }
        let (inflight, _) = runtime.begin_recording_stop().unwrap();
        assert_eq!(inflight, 1);
        assert!(!runtime.snapshot().unwrap().recording_active);
        assert!(runtime.imu_stop.load(Ordering::Acquire));
        let gate_closed = stopped.load(Ordering::Acquire);
        assert!(gate_closed > 0 && gate_closed < u64::MAX);
        thread::sleep(Duration::from_millis(20));
        // Repeating a stop while a real frame still owns the writer must not
        // move the boundary to the later drain/retry time.
        runtime.begin_recording_stop().unwrap();
        assert_eq!(stopped.load(Ordering::Acquire), gate_closed);
        // Stopping closes admission while the blocked frame still owns its resources.
        drop(blocked_writer);
        worker.join().unwrap().unwrap();
        assert_eq!(stopped.load(Ordering::Acquire), gate_closed);
        let snapshot = runtime.snapshot().unwrap();
        assert_eq!(snapshot.inflight_frames, 0);
        assert!(!snapshot.recording_present);
        assert!(!snapshot.failure_reported);
        assert_eq!(
            active_take.lock().unwrap().finish().unwrap().frames_written,
            1
        );
        assert_eq!(
            sink.lock()
                .unwrap()
                .flush_and_close()
                .unwrap()
                .frames_written,
            1
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn start_preview_reaps_exited_worker_after_terminal_error_and_recovers() {
        let root = temp_root("preview-recovery");
        let stream = Arc::new(Stream::test_fake(false, 4));
        let runtime = Runtime::new(
            Arc::clone(&stream),
            Arc::new(LatestBuffer::new(30).unwrap()),
            1,
            Duration::from_millis(80),
            None,
        )
        .unwrap();

        runtime.start_preview().unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while runtime.snapshot().unwrap().running {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for fake stream read timeout to trigger terminal_error"
            );
            thread::sleep(Duration::from_millis(5));
        }
        let crashed_snap = runtime.snapshot().unwrap();
        assert!(!crashed_snap.running);
        assert_eq!(
            crashed_snap.terminal_error.as_ref().unwrap().code,
            "frame_timeout"
        );
        assert!(runtime.worker.lock().unwrap().is_some());

        // Prepare recording artifacts before restarting preview so process/disk setup never races the read timeout.
        let rec = test_recording(&root, Arc::new(TestFailureSink::default()));

        // Calling start_preview() must reap the dead worker, restart stream, and clear terminal_error.
        runtime.start_preview().unwrap();
        let recovered_snap = runtime.snapshot().unwrap();
        assert!(recovered_snap.running);
        assert!(recovered_snap.terminal_error.is_none());

        // Recording can now start immediately on the recovered runtime.
        let rec_snap = runtime
            .start_recording(RecordingTarget {
                active_take: Arc::clone(&rec.active_take),
                sink: Arc::clone(&rec.sink),
                encoder: Arc::clone(&rec.encoder),
                segment_planner: Arc::clone(&rec.segment_planner),
                recording_start_monotonic_ns: 1,
                on_failure: Arc::clone(&rec.on_failure),
                imu: None,
                imu_timeout: Duration::from_millis(10),
                audio: None,
                capture_stopped_monotonic_ns: Arc::clone(&rec.capture_stopped_monotonic_ns),
                recover_source_gaps: false,
                synced: None,
            })
            .unwrap();
        assert!(rec_snap.recording_active);
        let _ = runtime.stop_recording(Duration::from_secs(1));
        runtime.close(Duration::from_secs(1)).unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_imu_loop_tolerates_transient_retryable_errors_and_fails_on_fatal_or_exhausted_budget() {
        // 1. Transient retryable error (disconnected on non-UVC pipe fd) within stall budget
        //    must NOT immediately fail the recording while stop arrives shortly after.
        let root = temp_root("imu-retry-ok");
        let callbacks = Arc::new(TestFailureSink::default());
        let target = test_recording(&root, Arc::clone(&callbacks));
        let runtime = Runtime::new(
            Arc::new(Stream::test_idle(1)),
            Arc::new(LatestBuffer::new(30).unwrap()),
            1,
            Duration::from_millis(10),
            None,
        )
        .unwrap();
        {
            let mut state = runtime.shared.state.lock().unwrap();
            state.running = true;
            state.fanout.start_recording(false).unwrap();
            state.recording = Some(Arc::new(target));
        }

        let mut pipe_fds = [-1_i32; 2];
        assert_eq!(unsafe { libc::pipe(pipe_fds.as_mut_ptr()) }, 0);
        unsafe { libc::close(pipe_fds[1]) };
        let collector = Arc::new(Collector::test_with_fd(Some(pipe_fds[0])));
        let sink = Arc::clone(
            &runtime
                .shared
                .state
                .lock()
                .unwrap()
                .recording
                .as_ref()
                .unwrap()
                .sink,
        );
        let stop = Arc::new(AtomicBool::new(false));
        let stop_trigger = Arc::clone(&stop);
        let stopper = thread::spawn(move || {
            thread::sleep(Duration::from_millis(15));
            stop_trigger.store(true, Ordering::Release);
        });
        run_imu_loop(ImuLoopContext {
            collector,
            sink,
            on_failure: Arc::clone(&callbacks) as Arc<dyn FailureSink>,
            audio: None,
            synced_audio: None,
            synced_imu: None,
            shared: Arc::clone(&runtime.shared),
            stop,
            timeout: Duration::from_millis(5),
            stall_budget: Duration::from_millis(500),
            metrics: None,
        });
        stopper.join().unwrap();
        assert!(
            callbacks.calls.lock().unwrap().is_empty(),
            "transient retryable IMU errors within stall budget must not abort recording"
        );
        assert!(runtime.snapshot().unwrap().recording_present);
        let _ = runtime.stop_recording(Duration::from_secs(1));
        let _ = fs::remove_dir_all(root);

        // 2. Continuous retryable errors exceeding stall_budget DO fail the take.
        let root_exhaust = temp_root("imu-retry-exhaust");
        let exhaust_cb = Arc::new(TestFailureSink::default());
        let exhaust_target = test_recording(&root_exhaust, Arc::clone(&exhaust_cb));
        {
            let mut state = runtime.shared.state.lock().unwrap();
            state.running = true;
            state.fanout.start_recording(false).unwrap();
            state.recording = Some(Arc::new(exhaust_target));
        }
        let mut pipe_fds2 = [-1_i32; 2];
        assert_eq!(unsafe { libc::pipe(pipe_fds2.as_mut_ptr()) }, 0);
        unsafe { libc::close(pipe_fds2[1]) };
        let collector2 = Arc::new(Collector::test_with_fd(Some(pipe_fds2[0])));
        let sink2 = Arc::clone(
            &runtime
                .shared
                .state
                .lock()
                .unwrap()
                .recording
                .as_ref()
                .unwrap()
                .sink,
        );
        run_imu_loop(ImuLoopContext {
            collector: collector2,
            sink: sink2,
            on_failure: Arc::clone(&exhaust_cb) as Arc<dyn FailureSink>,
            audio: None,
            synced_audio: None,
            synced_imu: None,
            shared: Arc::clone(&runtime.shared),
            stop: Arc::new(AtomicBool::new(false)),
            timeout: Duration::from_millis(5),
            stall_budget: Duration::from_millis(25),
            metrics: None,
        });
        let exhaust_calls = exhaust_cb.calls.lock().unwrap().clone();
        assert_eq!(exhaust_calls.len(), 1);
        assert_eq!(exhaust_calls[0].0, "disconnected");
        let _ = fs::remove_dir_all(root_exhaust);

        // 3. Non-retryable error (fd == None -> invalid_state) fails immediately despite large budget.
        let root_fatal = temp_root("imu-fatal");
        let fatal_cb = Arc::new(TestFailureSink::default());
        let fatal_target = test_recording(&root_fatal, Arc::clone(&fatal_cb));
        {
            let mut state = runtime.shared.state.lock().unwrap();
            state.running = true;
            state.fanout.start_recording(false).unwrap();
            state.recording = Some(Arc::new(fatal_target));
        }
        let collector3 = Arc::new(Collector::test_with_fd(None));
        let sink3 = Arc::clone(
            &runtime
                .shared
                .state
                .lock()
                .unwrap()
                .recording
                .as_ref()
                .unwrap()
                .sink,
        );
        let started = Instant::now();
        run_imu_loop(ImuLoopContext {
            collector: collector3,
            sink: sink3,
            on_failure: Arc::clone(&fatal_cb) as Arc<dyn FailureSink>,
            audio: None,
            synced_audio: None,
            synced_imu: None,
            shared: Arc::clone(&runtime.shared),
            stop: Arc::new(AtomicBool::new(false)),
            timeout: Duration::from_millis(5),
            stall_budget: Duration::from_secs(5),
            metrics: None,
        });
        assert!(started.elapsed() < Duration::from_millis(500));
        let fatal_calls = fatal_cb.calls.lock().unwrap().clone();
        assert_eq!(fatal_calls.len(), 1);
        assert_eq!(fatal_calls[0].0, "invalid_state");
        let _ = fs::remove_dir_all(root_fatal);
    }

    #[test]
    fn transient_shm_backpressure_keeps_live_preview_updating_and_stop_unblocks_fast() {
        let root = temp_root("shm-decoupled-preview");
        fs::create_dir_all(root.join("video")).unwrap();
        let helper = root.join("stalled-shm.sh");
        fs::write(
            &helper,
            "#!/bin/sh\nprintf '{\"event\":\"ready\"}\\n'\nsleep 30\n",
        )
        .unwrap();
        fs::set_permissions(&helper, fs::Permissions::from_mode(0o755)).unwrap();

        let mut encoder = stereo_encoder::EncoderProcess::with_transport(
            stereo_encoder::EncoderConfig {
                out_dir: &root.join("video"),
                executable: &helper,
                width: 64,
                height: 32,
                fps: 30,
                bitrate_kbps: 8192,
                segment_frames: 3,
                path_prefix: "video/",
            },
            stereo_encoder::EncoderTransport::ShmRing(crate::session::shm_ring::ShmRingConfig {
                slot_count: 2,
                slot_capacity: 2 * 1024 * 1024,
            }),
        )
        .unwrap();
        encoder
            .configure(&["--intra-period".to_owned(), "3".to_owned()])
            .unwrap();
        encoder.start().unwrap();
        let encoder = Arc::new(Mutex::new(encoder));

        let runtime = Runtime::new(
            Arc::new(Stream::test_idle(4)),
            Arc::new(LatestBuffer::new(120).unwrap()),
            1,
            Duration::from_millis(10),
            None,
        )
        .unwrap();
        runtime.shared.state.lock().unwrap().running = true;

        let callbacks = Arc::new(TestFailureSink::default());
        let active_take = Arc::new(Mutex::new(
            active_take::ActiveTakeWriter::new("session").unwrap(),
        ));
        let sink = Arc::new(Mutex::new(
            recording::RecordingSink::create(&root, "session").unwrap(),
        ));
        let planner = Arc::new(Mutex::new(
            recording::RecordingSegmentPlanner::new(3).unwrap(),
        ));
        let stopped = Arc::new(AtomicU64::new(0));

        runtime
            .start_recording(RecordingTarget {
                active_take: Arc::clone(&active_take),
                sink: Arc::clone(&sink),
                encoder: Arc::clone(&encoder),
                segment_planner: Arc::clone(&planner),
                recording_start_monotonic_ns: 1_000_000,
                on_failure: Arc::clone(&callbacks) as Arc<dyn FailureSink>,
                imu: None,
                imu_timeout: Duration::from_millis(10),
                audio: None,
                capture_stopped_monotonic_ns: Arc::clone(&stopped),
                recover_source_gaps: false,
                synced: None,
            })
            .unwrap();

        // Feed 10 frames across >300ms while the 2-slot SHM ring is completely full!
        // `process_frame` must never stall waiting on `submit_frame`, and preview must keep advancing.
        let feed_started = Instant::now();
        for seq in 1..=10_u64 {
            let call_started = Instant::now();
            process_frame(
                &runtime.preview,
                &runtime.shared,
                evidence_frame(seq, Some(seq as u32)),
                None,
            )
            .unwrap();
            assert!(
                call_started.elapsed() < Duration::from_millis(100),
                "process_frame blocked on SHM backpressure at seq {seq}"
            );
            if seq == 3 {
                thread::sleep(Duration::from_millis(320));
            }
        }
        assert!(feed_started.elapsed() >= Duration::from_millis(300));
        assert_eq!(runtime.preview.snapshot().unwrap().sequence, 10);

        // `stop_recording` must wake the SHM-blocked record worker in well under 1s (never 10s!).
        let stop_started = Instant::now();
        let snap = runtime.stop_recording(Duration::from_secs(2)).unwrap();
        assert!(
            stop_started.elapsed() < Duration::from_millis(500),
            "stop_recording stalled for {:?}",
            stop_started.elapsed()
        );
        assert!(!snap.recording_present);
        encoder.lock().unwrap().abort();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn imu_failure_sink_calling_stop_recording_and_restart_never_self_deadlocks() {
        struct ReentrantFailureSink {
            runtime: Arc<Runtime>,
            stop_results: Mutex<Vec<Result<RuntimeSnapshot, RuntimeError>>>,
        }

        impl FailureSink for ReentrantFailureSink {
            fn on_failure(&self, _code: &str, _message: &str) {
                let res = self.runtime.stop_recording(Duration::from_millis(200));
                self.stop_results.lock().unwrap().push(res);
            }
        }

        let root = temp_root("imu-self-join");
        let runtime = Arc::new(
            Runtime::new(
                Arc::new(Stream::test_idle(1)),
                Arc::new(LatestBuffer::new(30).unwrap()),
                1,
                Duration::from_millis(10),
                None,
            )
            .unwrap(),
        );
        runtime.shared.state.lock().unwrap().running = true;

        let reentrant = Arc::new(ReentrantFailureSink {
            runtime: Arc::clone(&runtime),
            stop_results: Mutex::new(Vec::new()),
        });
        let rec = test_recording(&root, Arc::new(TestFailureSink::default()));
        let bad_imu = Arc::new(Collector::test_with_fd(None));
        runtime
            .start_recording(RecordingTarget {
                active_take: Arc::clone(&rec.active_take),
                sink: Arc::clone(&rec.sink),
                encoder: Arc::clone(&rec.encoder),
                segment_planner: Arc::clone(&rec.segment_planner),
                recording_start_monotonic_ns: 1,
                on_failure: Arc::clone(&reentrant) as Arc<dyn FailureSink>,
                imu: Some(bad_imu),
                imu_timeout: Duration::from_millis(10),
                audio: None,
                capture_stopped_monotonic_ns: Arc::clone(&rec.capture_stopped_monotonic_ns),
                recover_source_gaps: false,
                synced: None,
            })
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        while reentrant.stop_results.lock().unwrap().is_empty() {
            assert!(
                Instant::now() < deadline,
                "reentrant FailureSink::on_failure on capture-imu deadlocked"
            );
            thread::sleep(Duration::from_millis(5));
        }
        assert!(reentrant.stop_results.lock().unwrap()[0].is_ok());

        // Wait briefly for capture-imu thread to exit, then verify `start_recording` auto-reaps it!
        let finish_deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let finished = runtime
                .imu_worker
                .lock()
                .unwrap()
                .as_ref()
                .is_none_or(|w| w.handle.is_finished());
            if finished {
                break;
            }
            assert!(Instant::now() < finish_deadline);
            thread::sleep(Duration::from_millis(5));
        }

        let next = test_recording(&root.join("retry"), Arc::new(TestFailureSink::default()));
        let retry_snap = runtime
            .start_recording(RecordingTarget {
                active_take: Arc::clone(&next.active_take),
                sink: Arc::clone(&next.sink),
                encoder: Arc::clone(&next.encoder),
                segment_planner: Arc::clone(&next.segment_planner),
                recording_start_monotonic_ns: 2,
                on_failure: Arc::clone(&next.on_failure),
                imu: None,
                imu_timeout: Duration::from_millis(10),
                audio: None,
                capture_stopped_monotonic_ns: Arc::clone(&next.capture_stopped_monotonic_ns),
                recover_source_gaps: false,
                synced: None,
            })
            .unwrap();
        assert!(retry_snap.recording_active);
        runtime.stop_recording(Duration::from_secs(1)).unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn recovered_source_gaps_populate_active_take_drop_events() {
        let root = temp_root("recovered-drop-events");
        let callbacks = Arc::new(TestFailureSink::default());
        let mut target = test_recording(&root, Arc::clone(&callbacks));
        let helper = root.join("encoder-helper.sh");
        fs::write(
            &helper,
            "#!/bin/sh\nprintf '{\"event\":\"ready\"}\\n'\ncat >/dev/null\n",
        )
        .unwrap();
        fs::set_permissions(&helper, fs::Permissions::from_mode(0o755)).unwrap();
        let mut encoder = stereo_encoder::EncoderProcess::with_transport(
            stereo_encoder::EncoderConfig {
                out_dir: &root.join("video"),
                executable: &helper,
                width: 3840,
                height: 1080,
                fps: 60,
                bitrate_kbps: 8192,
                segment_frames: 3,
                path_prefix: "video/",
            },
            stereo_encoder::EncoderTransport::StdinPipe,
        )
        .unwrap();
        encoder
            .configure(&["--intra-period".to_owned(), "3".to_owned()])
            .unwrap();
        encoder.start().unwrap();
        target.encoder_abort = encoder.abort_handle();
        target.encoder = Arc::new(Mutex::new(encoder));
        target.recording_start_monotonic_ns = 20_000_000;
        let active_take = Arc::clone(&target.active_take);

        let runtime = Runtime::new(
            Arc::new(Stream::test_idle(1)),
            Arc::new(LatestBuffer::new(30).unwrap()),
            1,
            Duration::from_millis(10),
            None,
        )
        .unwrap();
        {
            let mut state = runtime.shared.state.lock().unwrap();
            state.running = true;
            state.fanout.start_recording(true).unwrap();
            state.recording = Some(Arc::new(target));
        }

        // Frame seq 1 (t=20ms) -> recorded ordinal 0
        process_frame(
            &runtime.preview,
            &runtime.shared,
            evidence_frame(1, Some(1)),
            None,
        )
        .unwrap();
        // Frame seq 4 (t=80ms, delta=60ms <= 150ms, missing seq 2 & 3 -> 2 dropped frames!)
        process_frame(
            &runtime.preview,
            &runtime.shared,
            evidence_frame(4, Some(4)),
            None,
        )
        .unwrap();

        let summary = active_take.lock().unwrap().finish().unwrap();
        assert_eq!(summary.frames_written, 2);
        assert_eq!(summary.dropped_frames, 2);
        assert_eq!(summary.drop_events.len(), 1);
        assert_eq!(summary.drop_events[0].start_frame, 1);
        assert_eq!(summary.drop_events[0].end_frame, 3);
        assert_eq!(summary.drop_events[0].dropped, 2);
        assert_eq!(summary.drop_events[0].reason, "write_backpressure");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn corrupt_frame_drops_continue_recording_and_stop_recording_fences_imu_before_record_error() {
        let root = temp_root("corrupt-drops-and-imu-fence");
        let callbacks = Arc::new(TestFailureSink::default());
        let mut target = test_recording(&root, Arc::clone(&callbacks));
        let helper = root.join("encoder-helper.sh");
        fs::write(
            &helper,
            "#!/bin/sh\nprintf '{\"event\":\"ready\"}\\n'\ncat >/dev/null\n",
        )
        .unwrap();
        fs::set_permissions(&helper, fs::Permissions::from_mode(0o755)).unwrap();
        let mut encoder = stereo_encoder::EncoderProcess::with_transport(
            stereo_encoder::EncoderConfig {
                out_dir: &root.join("video"),
                executable: &helper,
                width: 3840,
                height: 1080,
                fps: 60,
                bitrate_kbps: 8192,
                segment_frames: 3,
                path_prefix: "video/",
            },
            stereo_encoder::EncoderTransport::StdinPipe,
        )
        .unwrap();
        encoder
            .configure(&["--intra-period".to_owned(), "3".to_owned()])
            .unwrap();
        encoder.start().unwrap();
        target.encoder_abort = encoder.abort_handle();
        target.encoder = Arc::new(Mutex::new(encoder));
        target.recording_start_monotonic_ns = 20_000_000;
        let active_take = Arc::clone(&target.active_take);

        // Notice `recover_source_gaps = false`: transient corrupt frames MUST still be tolerated!
        let runtime = Runtime::new(
            Arc::new(Stream::test_idle(1)),
            Arc::new(LatestBuffer::new(30).unwrap()),
            1,
            Duration::from_millis(10),
            None,
        )
        .unwrap();
        {
            let mut state = runtime.shared.state.lock().unwrap();
            state.running = true;
            state.fanout.start_recording(false).unwrap();
            state.recording = Some(Arc::new(target));
        }

        process_frame(
            &runtime.preview,
            &runtime.shared,
            evidence_frame(1, Some(1)),
            None,
        )
        .unwrap();
        // Frame 2 was corrupt/requeued in native_camera, so frame 3 arrives with corrupt_dropped_before=1
        let mut frame3 = evidence_frame(3, Some(3));
        frame3.corrupt_dropped_before = 1;
        process_frame(&runtime.preview, &runtime.shared, frame3, None).unwrap();

        assert!(callbacks.calls.lock().unwrap().is_empty());
        assert_eq!(active_take.lock().unwrap().corrupt_frames_dropped(), 1);
        let summary = active_take.lock().unwrap().finish().unwrap();
        assert_eq!(summary.frames_written, 2);
        assert_eq!(summary.dropped_frames, 1);
        assert_eq!(summary.drop_events.len(), 1);
        assert_eq!(summary.drop_events[0].dropped, 1);

        // Even if `record_worker` panics, `stop_recording` must join `imu_worker` before returning!
        let imu_stop = Arc::clone(&runtime.imu_stop);
        let (imu_done_tx, imu_done_rx) = mpsc::channel();
        let imu_handle = thread::spawn(move || {
            while !imu_stop.load(Ordering::Acquire) {
                thread::yield_now();
            }
            thread::sleep(Duration::from_millis(15));
            let _ = imu_done_tx.send(());
        });
        *runtime.imu_worker.lock().unwrap() = Some(WorkerHandle {
            handle: imu_handle,
            done: imu_done_rx,
        });
        let (_rec_done_tx, rec_done_rx) = mpsc::channel();
        let panicked_rec = thread::spawn(|| panic!("simulated record worker panic"));
        let _ = panicked_rec.thread().id();
        while !panicked_rec.is_finished() {
            thread::yield_now();
        }
        drop(_rec_done_tx);
        *runtime.record_worker.lock().unwrap() = Some(WorkerHandle {
            handle: panicked_rec,
            done: rec_done_rx,
        });

        let err = runtime.stop_recording(Duration::from_secs(1)).unwrap_err();
        assert_eq!(err.code, "capture_runtime_worker_failed");
        assert!(
            runtime.imu_worker.lock().unwrap().is_none(),
            "stop_recording must join capture-imu before returning even when record_worker fails"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn decimated_corrupt_drops_keep_phase_and_recovered_drops_full_queue_reports_backpressure() {
        let root = temp_root("decimated-corrupt-and-drop-full");
        let callbacks = Arc::new(TestFailureSink::default());
        let mut target = test_recording(&root, Arc::clone(&callbacks));
        let helper = root.join("encoder-helper.sh");
        fs::write(
            &helper,
            "#!/bin/sh\nprintf '{\"event\":\"ready\"}\\n'\ncat >/dev/null\n",
        )
        .unwrap();
        fs::set_permissions(&helper, fs::Permissions::from_mode(0o755)).unwrap();
        let mut encoder = stereo_encoder::EncoderProcess::with_transport(
            stereo_encoder::EncoderConfig {
                out_dir: &root.join("video"),
                executable: &helper,
                width: 3840,
                height: 1080,
                fps: 60,
                bitrate_kbps: 8192,
                segment_frames: 3,
                path_prefix: "video/",
            },
            stereo_encoder::EncoderTransport::StdinPipe,
        )
        .unwrap();
        encoder
            .configure(&["--intra-period".to_owned(), "3".to_owned()])
            .unwrap();
        encoder.start().unwrap();
        target.encoder_abort = encoder.abort_handle();
        target.encoder = Arc::new(Mutex::new(encoder));
        target.recording_start_monotonic_ns = 20_000_000;
        let active_take = Arc::clone(&target.active_take);

        let runtime = Runtime::new(
            Arc::new(Stream::test_idle(1)),
            Arc::new(LatestBuffer::new(30).unwrap()),
            2,
            Duration::from_millis(10),
            None,
        )
        .unwrap();
        {
            let mut state = runtime.shared.state.lock().unwrap();
            state.running = true;
            state.fanout.start_recording(false).unwrap();
            state.recording = Some(Arc::new(target));
        }

        // Observed 0 -> recorded.
        process_frame(
            &runtime.preview,
            &runtime.shared,
            evidence_frame(1, Some(1)),
            None,
        )
        .unwrap();
        // Frame 2 (odd, not recorded) corrupt, frame 3 (even, recorded) corrupt -> frame 4 (odd):
        // gap [1, 3) contains recorded index 2 -> corrupt_recorded_drops = 1, observed_index = 3 (not recorded).
        let mut frame4 = evidence_frame(4, Some(4));
        frame4.corrupt_dropped_before = 2;
        process_frame(&runtime.preview, &runtime.shared, frame4, None).unwrap();
        // Frame 5 (even, index 4) must be in-phase and recorded!
        process_frame(
            &runtime.preview,
            &runtime.shared,
            evidence_frame(5, Some(5)),
            None,
        )
        .unwrap();

        let summary = active_take.lock().unwrap().finish().unwrap();
        assert_eq!(summary.frames_written, 2);
        assert_eq!(summary.dropped_frames, 1);

        // Also verify RecoveredDrops queue Full reports encoder_backpressure_exhausted.
        let full_root = temp_root("recovered-drops-queue-full");
        let full_cb = Arc::new(TestFailureSink::default());
        let full_target = test_recording(&full_root, Arc::clone(&full_cb));
        let queue = RecordWorkQueue::new();
        for i in 0..RECORD_QUEUE_MAX_FRAMES as u64 {
            assert!(matches!(
                queue
                    .try_enqueue(RecordWork::RecoveredDrops {
                        dropped: 1,
                        host_monotonic_ns: i,
                        corrupt: false,
                    })
                    .unwrap(),
                EnqueueOutcome::Enqueued
            ));
        }
        let shared = Arc::clone(&runtime.shared);
        let ok = enqueue_recovered_drops(&queue, &shared, &full_target, 1, 99, true).unwrap();
        assert!(!ok);
        assert_eq!(
            full_cb.calls.lock().unwrap()[0].0,
            "encoder_backpressure_exhausted"
        );
        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(full_root);
    }

    #[test]
    fn clean_stop_during_backpressure_capture_stopped_leaves_no_pending_frame() {
        let root = temp_root("capture-stopped-rollback");
        fs::create_dir_all(root.join("video")).unwrap();
        let helper = root.join("stalled-shm.sh");
        fs::write(
            &helper,
            "#!/bin/sh\nprintf '{\"event\":\"ready\"}\\n'\nsleep 30\n",
        )
        .unwrap();
        fs::set_permissions(&helper, fs::Permissions::from_mode(0o755)).unwrap();

        let mut encoder = stereo_encoder::EncoderProcess::with_transport(
            stereo_encoder::EncoderConfig {
                out_dir: &root.join("video"),
                executable: &helper,
                width: 64,
                height: 32,
                fps: 30,
                bitrate_kbps: 8192,
                segment_frames: 3,
                path_prefix: "video/",
            },
            stereo_encoder::EncoderTransport::ShmRing(crate::session::shm_ring::ShmRingConfig {
                slot_count: 1,
                slot_capacity: 2 * 1024 * 1024,
            }),
        )
        .unwrap();
        encoder
            .configure(&["--intra-period".to_owned(), "3".to_owned()])
            .unwrap();
        encoder.start().unwrap();
        let encoder = Arc::new(Mutex::new(encoder));

        let runtime = Runtime::new(
            Arc::new(Stream::test_idle(4)),
            Arc::new(LatestBuffer::new(120).unwrap()),
            1,
            Duration::from_millis(10),
            None,
        )
        .unwrap();
        runtime.shared.state.lock().unwrap().running = true;

        let callbacks = Arc::new(TestFailureSink::default());
        let active_take = Arc::new(Mutex::new(
            active_take::ActiveTakeWriter::new("session").unwrap(),
        ));
        let sink = Arc::new(Mutex::new(
            recording::RecordingSink::create(&root, "session").unwrap(),
        ));
        let planner = Arc::new(Mutex::new(
            recording::RecordingSegmentPlanner::new(3).unwrap(),
        ));
        let stopped = Arc::new(AtomicU64::new(0));

        runtime
            .start_recording(RecordingTarget {
                active_take: Arc::clone(&active_take),
                sink: Arc::clone(&sink),
                encoder: Arc::clone(&encoder),
                segment_planner: Arc::clone(&planner),
                recording_start_monotonic_ns: 1_000_000,
                on_failure: Arc::clone(&callbacks) as Arc<dyn FailureSink>,
                imu: None,
                imu_timeout: Duration::from_millis(10),
                audio: None,
                capture_stopped_monotonic_ns: Arc::clone(&stopped),
                recover_source_gaps: false,
                synced: None,
            })
            .unwrap();

        // Slot 0 fits; slot 1 blocks in submit_frame_with_stop_check until stop_recording().
        for seq in 1..=2_u64 {
            process_frame(
                &runtime.preview,
                &runtime.shared,
                evidence_frame(seq, Some(seq as u32)),
                None,
            )
            .unwrap();
        }
        thread::sleep(Duration::from_millis(50));

        let snap = runtime.stop_recording(Duration::from_secs(2)).unwrap();
        assert!(!snap.failure_reported);
        assert!(callbacks.calls.lock().unwrap().is_empty());

        let summary = active_take.lock().unwrap().finish().unwrap();
        assert_eq!(summary.pending_frames, 0);
        assert_eq!(summary.frames_written, 1);
        assert_eq!(summary.frame_domain, 1);
        let sink_snap = sink.lock().unwrap().flush_and_close().unwrap();
        assert_eq!(sink_snap.frames_written, 1);
        assert_eq!(planner.lock().unwrap().snapshot().frames_written, 1);

        encoder.lock().unwrap().abort();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn synced_lookahead_pairs_imu_and_flushes_tail_frame() {
        let imu_q = SyncedImuQueue::new();
        imu_q.push_samples(&[
            ImuSample {
                sequence: 1,
                packet_sequence: 1,
                sample_index: 0,
                device_timestamp_raw: 10,
                device_ticks: 10,
                host_read_start_ns: 100,
                host_read_end_ns: 200,
                host_monotonic_ns: 25_000_000,
                accelerometer: crate::hardware::imu::RawVector3 { x: 1, y: 2, z: 3 },
                gyroscope: crate::hardware::imu::RawVector3 {
                    x: -1,
                    y: -2,
                    z: -3,
                },
                sync_offset_ns: None,
                sync_residual_ns: None,
                sync_quality: "nominal",
            },
            ImuSample {
                sequence: 2,
                packet_sequence: 1,
                sample_index: 1,
                device_timestamp_raw: 20,
                device_ticks: 20,
                host_read_start_ns: 100,
                host_read_end_ns: 200,
                host_monotonic_ns: 55_000_000,
                accelerometer: crate::hardware::imu::RawVector3 { x: 4, y: 5, z: 6 },
                gyroscope: crate::hardware::imu::RawVector3 {
                    x: -4,
                    y: -5,
                    z: -6,
                },
                sync_offset_ns: None,
                sync_residual_ns: None,
                sync_quality: "nominal",
            },
        ]);
        let drained = imu_q.drain_up_to(35_000_000);
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].sequence, 1);

        let synced = SyncedCaptureConfig {
            audio: None,
            imu_queue: Arc::new(imu_q),
            timescale: 48_000,
            fps: 30,
        };
        let (s_next, dur) = compute_synced_interval(&synced, 20_000_000, 53_333_333, 1_000);
        assert_eq!(dur, 1_599);
        assert_eq!(s_next, 2_599);

        let frame = evidence_frame(1, Some(7));
        let meta = serialize_synced_frame_meta(
            0,
            1,
            20_000_000,
            20_000_000,
            1_000,
            1_599,
            &frame,
            Some(7),
            &drained,
        );
        let parsed: serde_json::Value = serde_json::from_slice(&meta).unwrap();
        assert_eq!(parsed["hw_s"], 1_000);
        assert_eq!(parsed["dur"], 1_599);
        assert_eq!(parsed["cam_ctr"], 7);
        assert_eq!(parsed["imu"].as_array().unwrap().len(), 1);
        assert_eq!(synced.imu_queue.drain_all().len(), 1);
    }
}
