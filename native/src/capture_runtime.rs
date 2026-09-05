use crate::active_take;
use crate::imu::{Collector, ImuError};
use crate::metrics::Metrics;
use crate::native_camera::{Frame, FrameValidator, Stream, StreamError};
use crate::preview::{LatestBuffer, PreviewError};
use crate::recording::{self, CaptureFanoutState, RecordingError};
use crate::stereo_encoder;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeError {
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

impl RuntimeError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
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

impl From<active_take::ActiveTakeError> for RuntimeError {
    fn from(error: active_take::ActiveTakeError) -> Self {
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
pub(crate) struct RuntimeSnapshot {
    pub(crate) running: bool,
    pub(crate) recording_present: bool,
    pub(crate) recording_active: bool,
    pub(crate) inflight_frames: u64,
    pub(crate) observed_frames: u64,
    pub(crate) failure_reported: bool,
    pub(crate) terminal_error: Option<RuntimeError>,
    pub(crate) last_preview_error: Option<RuntimeError>,
}

struct SplitSinkRecording {
    active_take: Arc<Mutex<active_take::ActiveTakeWriter>>,
    sink: Arc<Mutex<recording::RecordingSink>>,
    encoder: Arc<Mutex<stereo_encoder::EncoderProcess>>,
    segment_planner: Arc<Mutex<recording::RecordingSegmentPlanner>>,
    recording_start_monotonic_ns: u64,
    on_failure: Arc<Py<PyAny>>,
    imu: Option<Arc<Collector>>,
}

impl SplitSinkRecording {
    fn imu(&self) -> Option<Arc<Collector>> {
        self.imu.as_ref().map(Arc::clone)
    }
}

struct RecordingDispatch {
    active_take: Arc<Mutex<active_take::ActiveTakeWriter>>,
    sink: Arc<Mutex<recording::RecordingSink>>,
    encoder: Arc<Mutex<stereo_encoder::EncoderProcess>>,
    segment_planner: Arc<Mutex<recording::RecordingSegmentPlanner>>,
    recording_start_monotonic_ns: u64,
    on_failure: Arc<Py<PyAny>>,
}

struct State {
    fanout: CaptureFanoutState,
    validator: FrameValidator,
    recording: Option<SplitSinkRecording>,
    running: bool,
    terminal_error: Option<RuntimeError>,
    last_preview_error: Option<RuntimeError>,
}

struct Shared {
    state: Mutex<State>,
    changed: Condvar,
}

struct WorkerHandle {
    handle: JoinHandle<()>,
    done: Receiver<()>,
}

pub(crate) struct Runtime {
    stream: Arc<Stream>,
    preview: Arc<LatestBuffer>,
    shared: Arc<Shared>,
    stop: Arc<AtomicBool>,
    imu_stop: Arc<AtomicBool>,
    worker: Mutex<Option<JoinHandle<()>>>,
    imu_worker: Mutex<Option<WorkerHandle>>,
    read_timeout: Duration,
    metrics: Option<Arc<Metrics>>,
}

impl Runtime {
    pub(crate) fn new(
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
        Ok(Self {
            stream,
            preview,
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    fanout: CaptureFanoutState::new(frame_decimation)?,
                    validator: FrameValidator::new(),
                    recording: None,
                    running: false,
                    terminal_error: None,
                    last_preview_error: None,
                }),
                changed: Condvar::new(),
            }),
            stop: Arc::new(AtomicBool::new(false)),
            imu_stop: Arc::new(AtomicBool::new(false)),
            worker: Mutex::new(None),
            imu_worker: Mutex::new(None),
            read_timeout,
            metrics,
        })
    }

    pub(crate) fn start_preview(&self) -> Result<(), RuntimeError> {
        let mut worker = self.worker.lock().map_err(|_| {
            RuntimeError::new(
                "capture_runtime_poisoned",
                "capture runtime worker mutex is poisoned",
            )
        })?;
        if worker.is_some() {
            return Ok(());
        }
        self.stream.start()?;
        self.stop.store(false, Ordering::Release);
        {
            let mut state = self.shared.state.lock().map_err(|_| {
                RuntimeError::new(
                    "capture_runtime_poisoned",
                    "capture runtime state mutex is poisoned",
                )
            })?;
            state.validator.reset();
            state.running = true;
            state.terminal_error = None;
            state.last_preview_error = None;
        }
        let stream = Arc::clone(&self.stream);
        let preview = Arc::clone(&self.preview);
        let shared = Arc::clone(&self.shared);
        let stop = Arc::clone(&self.stop);
        let read_timeout = self.read_timeout;
        let metrics = self.metrics.as_ref().map(Arc::clone);
        *worker = Some(thread::spawn(move || {
            run_loop(stream, preview, shared, stop, read_timeout, metrics);
        }));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start_recording(
        &self,
        active_take: Arc<Mutex<active_take::ActiveTakeWriter>>,
        sink: Arc<Mutex<recording::RecordingSink>>,
        encoder: Arc<Mutex<stereo_encoder::EncoderProcess>>,
        segment_planner: Arc<Mutex<recording::RecordingSegmentPlanner>>,
        recording_start_monotonic_ns: u64,
        on_failure: Py<PyAny>,
        imu: Option<Arc<Collector>>,
        imu_timeout: Duration,
    ) -> Result<RuntimeSnapshot, RuntimeError> {
        if recording_start_monotonic_ns == 0 {
            return Err(RuntimeError::new(
                "invalid_argument",
                "recording start monotonic timestamp must be positive",
            ));
        }
        if imu.is_some() && imu_timeout.is_zero() {
            return Err(RuntimeError::new(
                "invalid_argument",
                "capture runtime IMU timeout must be positive",
            ));
        }
        let mut imu_worker = self.imu_worker.lock().map_err(|_| {
            RuntimeError::new(
                "capture_runtime_poisoned",
                "capture runtime IMU worker mutex is poisoned",
            )
        })?;
        if imu_worker.is_some() {
            return Err(RuntimeError::new(
                "invalid_state",
                "capture runtime IMU worker is already running",
            ));
        }
        let imu_for_worker = imu.clone();
        let on_failure = Arc::new(on_failure);
        let on_failure_for_imu = imu_for_worker.as_ref().map(|_| Arc::clone(&on_failure));
        let sink_for_worker = Arc::clone(&sink);
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
        state.fanout.start_recording()?;
        state.recording = Some(SplitSinkRecording {
            active_take,
            sink,
            encoder,
            segment_planner,
            recording_start_monotonic_ns,
            on_failure: Arc::clone(&on_failure),
            imu,
        });
        self.shared.changed.notify_all();
        let snapshot = snapshot_locked(&state);
        drop(state);
        if let (Some(collector), Some(on_failure)) = (imu_for_worker, on_failure_for_imu) {
            self.imu_stop.store(false, Ordering::Release);
            let shared = Arc::clone(&self.shared);
            let stop = Arc::clone(&self.imu_stop);
            let metrics = self.metrics.as_ref().map(Arc::clone);
            let (done_tx, done_rx) = mpsc::channel();
            *imu_worker = Some(WorkerHandle {
                handle: thread::spawn(move || {
                    run_imu_loop(
                        collector,
                        sink_for_worker,
                        on_failure,
                        shared,
                        stop,
                        imu_timeout,
                        metrics,
                    );
                    let _ = done_tx.send(());
                }),
                done: done_rx,
            });
        }
        Ok(snapshot)
    }

    pub(crate) fn stop_recording(
        &self,
        timeout: Duration,
    ) -> Result<RuntimeSnapshot, RuntimeError> {
        if timeout.is_zero() {
            return Err(RuntimeError::new(
                "invalid_argument",
                "stop timeout must be positive",
            ));
        }
        let (inflight, imu) = self.begin_recording_stop()?;

        // Close the frame gate before collector shutdown can block.  This
        // prevents frames arriving during IMU cleanup from entering a session
        // that is already stopping.
        self.imu_stop.store(true, Ordering::Release);
        if let Some(imu) = imu {
            imu.close();
        }

        let mut state = self.shared.state.lock().map_err(|_| {
            RuntimeError::new(
                "capture_runtime_poisoned",
                "capture runtime state mutex is poisoned",
            )
        })?;
        if inflight == 0 {
            state.recording = None;
            self.shared.changed.notify_all();
            let snapshot = snapshot_locked(&state);
            drop(state);
            self.stop_imu_worker(timeout)?;
            return Ok(snapshot);
        }
        let (state, wait_result) = self
            .shared
            .changed
            .wait_timeout_while(state, timeout, |state| state.recording.is_some())
            .map_err(|_| {
                RuntimeError::new(
                    "capture_runtime_poisoned",
                    "capture runtime state mutex is poisoned",
                )
            })?;
        if wait_result.timed_out() && state.recording.is_some() {
            return Err(RuntimeError::new(
                "capture_runtime_stop_timeout",
                "capture runtime recording frames did not drain before timeout",
            ));
        }
        let snapshot = snapshot_locked(&state);
        drop(state);
        self.stop_imu_worker(timeout)?;
        Ok(snapshot)
    }

    fn begin_recording_stop(&self) -> Result<(u64, Option<Arc<Collector>>), RuntimeError> {
        let mut state = self.shared.state.lock().map_err(|_| {
            RuntimeError::new(
                "capture_runtime_poisoned",
                "capture runtime state mutex is poisoned",
            )
        })?;
        let imu = state.recording.as_ref().and_then(SplitSinkRecording::imu);
        let inflight = state.fanout.start_stopping();
        self.shared.changed.notify_all();
        Ok((inflight, imu))
    }

    pub(crate) fn close(&self, timeout: Duration) -> Result<RuntimeSnapshot, RuntimeError> {
        self.stop.store(true, Ordering::Release);
        self.request_imu_stop()?;
        self.stop_imu_worker(timeout)?;
        let _ = self.stream.close();
        let worker = self
            .worker
            .lock()
            .map_err(|_| {
                RuntimeError::new(
                    "capture_runtime_poisoned",
                    "capture runtime worker mutex is poisoned",
                )
            })?
            .take();
        if let Some(worker) = worker {
            if worker.join().is_err() {
                return Err(RuntimeError::new(
                    "capture_runtime_worker_failed",
                    "capture runtime worker panicked",
                ));
            }
        }
        let mut state = self.shared.state.lock().map_err(|_| {
            RuntimeError::new(
                "capture_runtime_poisoned",
                "capture runtime state mutex is poisoned",
            )
        })?;
        state.running = false;
        state.recording = None;
        state.fanout.start_stopping();
        self.shared.changed.notify_all();
        if !timeout.is_zero() {
            let _ = timeout;
        }
        Ok(snapshot_locked(&state))
    }

    pub(crate) fn snapshot(&self) -> Result<RuntimeSnapshot, RuntimeError> {
        let state = self.shared.state.lock().map_err(|_| {
            RuntimeError::new(
                "capture_runtime_poisoned",
                "capture runtime state mutex is poisoned",
            )
        })?;
        Ok(snapshot_locked(&state))
    }

    fn request_imu_stop(&self) -> Result<(), RuntimeError> {
        self.imu_stop.store(true, Ordering::Release);
        let imu = {
            let state = self.shared.state.lock().map_err(|_| {
                RuntimeError::new(
                    "capture_runtime_poisoned",
                    "capture runtime state mutex is poisoned",
                )
            })?;
            state.recording.as_ref().and_then(SplitSinkRecording::imu)
        };
        if let Some(imu) = imu {
            imu.close();
        }
        Ok(())
    }

    fn stop_imu_worker(&self, timeout: Duration) -> Result<(), RuntimeError> {
        let mut worker = self.imu_worker.lock().map_err(|_| {
            RuntimeError::new(
                "capture_runtime_poisoned",
                "capture runtime IMU worker mutex is poisoned",
            )
        })?;
        let Some(handle) = worker.take() else {
            return Ok(());
        };
        match handle.done.recv_timeout(timeout) {
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                if handle.handle.join().is_err() {
                    return Err(RuntimeError::new(
                        "capture_runtime_imu_worker_failed",
                        "capture runtime IMU worker panicked",
                    ));
                }
                Ok(())
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                *worker = Some(handle);
                Err(RuntimeError::new(
                    "capture_runtime_imu_stop_timeout",
                    "capture runtime IMU worker did not stop before timeout",
                ))
            }
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        let _ = self.close(Duration::from_secs(5));
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
    let frame_started = start_stage(metrics);
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
        let preview_jpeg = if has_left {
            Some(frame.left.as_slice())
        } else if has_raw {
            Some(frame.raw_side_by_side.as_slice())
        } else {
            None
        };
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
        let (dropped_before, dispatch) = {
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
            record_loss(metrics, "queue_rejected", validation.queue_rejected);
            record_loss(metrics, "source_gap", validation.source_gap);
            let decision = state
                .fanout
                .begin_frame(validation.dropped_before, preview_jpeg.is_some())
                .map_err(RuntimeError::from)?;
            if decision.publish_preview {
                if let Some(jpeg) = preview_jpeg {
                    let publish_started = start_stage(metrics);
                    if let Err(error) = preview.publish(jpeg) {
                        state.last_preview_error = Some(RuntimeError::from(error));
                    }
                    finish_stage(metrics, "native_preview_publish", publish_started);
                }
            }
            let dispatch = if decision.record {
                state.recording.as_ref().map(|recording| RecordingDispatch {
                    active_take: Arc::clone(&recording.active_take),
                    sink: Arc::clone(&recording.sink),
                    encoder: Arc::clone(&recording.encoder),
                    segment_planner: Arc::clone(&recording.segment_planner),
                    recording_start_monotonic_ns: recording.recording_start_monotonic_ns,
                    on_failure: Arc::clone(&recording.on_failure),
                })
            } else {
                None
            };
            (decision.dropped_before, dispatch)
        };
        if let Some(dispatch) = dispatch {
            let write_started = start_stage(metrics);
            let write_result = write_split_sink_frame(
                dispatch.active_take,
                dispatch.sink,
                dispatch.encoder,
                dispatch.segment_planner,
                dispatch.recording_start_monotonic_ns,
                frame.source_sequence,
                frame.host_monotonic_ns,
                dropped_before,
                frame.raw_side_by_side,
            );
            finish_stage(metrics, "native_recording_split_sink", write_started);
            finish_recording_frame(shared)?;
            if let Err(error) = write_result {
                report_recording_failure(shared, error.clone(), Some(dispatch.on_failure));
            }
        }
        Ok(())
    })();
    finish_stage(metrics, "native_capture_frame", frame_started);
    result
}

#[allow(clippy::too_many_arguments)]
fn write_split_sink_frame(
    active_take: Arc<Mutex<active_take::ActiveTakeWriter>>,
    sink: Arc<Mutex<recording::RecordingSink>>,
    encoder: Arc<Mutex<stereo_encoder::EncoderProcess>>,
    segment_planner: Arc<Mutex<recording::RecordingSegmentPlanner>>,
    recording_start_monotonic_ns: u64,
    source_sequence: u64,
    host_monotonic_ns: u64,
    dropped_before: u64,
    raw_side_by_side: Vec<u8>,
) -> Result<(), RuntimeError> {
    if raw_side_by_side.is_empty() {
        return Err(RuntimeError::new(
            "raw_frame_unavailable",
            "production split-eye recording is missing raw side-by-side MJPEG frame",
        ));
    }
    let payload = recording::jpeg_payload(&raw_side_by_side)?;
    let reserved = {
        let mut writer = active_take.lock().map_err(|_| {
            RuntimeError::new(
                "active_take_writer_poisoned",
                "active take writer mutex is poisoned",
            )
        })?;
        writer.reserve_frame(active_take::ActiveSourceFrame {
            source_sequence,
            host_monotonic_ns,
            source_gap: dropped_before,
        })?
    };
    let elapsed_seconds = if host_monotonic_ns >= recording_start_monotonic_ns {
        (host_monotonic_ns - recording_start_monotonic_ns) as f64 / 1_000_000_000.0
    } else {
        0.0
    };
    let plan = {
        let mut planner = segment_planner.lock().map_err(|_| {
            RuntimeError::new(
                "native_recording_segment_planner_poisoned",
                "recording segment planner mutex is poisoned",
            )
        })?;
        planner.next_frame(reserved.record_sequence, elapsed_seconds)?
    };
    {
        let mut encoder = encoder.lock().map_err(|_| {
            RuntimeError::new("encoder_failed", "encoder process mutex is poisoned")
        })?;
        encoder.submit(payload).map_err(encoder_runtime_error)?;
    }
    let written = {
        let mut sink = sink.lock().map_err(|_| {
            RuntimeError::new(
                "native_recording_poisoned",
                "recording sink mutex is poisoned",
            )
        })?;
        sink.write_split_frame_index(
            reserved.record_sequence,
            reserved.source_sequence,
            reserved.host_monotonic_ns,
            plan.segment_index,
            plan.segment_frame,
        )?
    };
    {
        let mut writer = active_take.lock().map_err(|_| {
            RuntimeError::new(
                "active_take_writer_poisoned",
                "active take writer mutex is poisoned",
            )
        })?;
        writer.finish_frame(reserved, written)?;
    }
    Ok(())
}

fn encoder_runtime_error(error: stereo_encoder::EncoderProcessError) -> RuntimeError {
    let code = match error.code.as_str() {
        "invalid_argument" => "invalid_argument",
        "invalid_state" => "invalid_state",
        "encoder_unavailable" => "encoder_unavailable",
        "encoder_failed" => "encoder_failed",
        "counter_overflow" => "counter_overflow",
        "send_failed" => "send_failed",
        _ => "encoder_failed",
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

fn run_imu_loop(
    collector: Arc<Collector>,
    sink: Arc<Mutex<recording::RecordingSink>>,
    on_failure: Arc<Py<PyAny>>,
    shared: Arc<Shared>,
    stop: Arc<AtomicBool>,
    timeout: Duration,
    metrics: Option<Arc<Metrics>>,
) {
    loop {
        if stop.load(Ordering::Acquire) || !recording_present(&shared) {
            break;
        }
        let read_started = start_stage(metrics.as_ref());
        let read_result = collector.read(timeout);
        finish_stage(metrics.as_ref(), "native_imu_read", read_started);
        match read_result {
            Ok(observation) => {
                if stop.load(Ordering::Acquire) || !recording_present(&shared) {
                    break;
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
                if !stop.load(Ordering::Acquire) && recording_present(&shared) {
                    report_recording_failure(&shared, error.into(), Some(Arc::clone(&on_failure)));
                }
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
    preferred_callback: Option<Arc<Py<PyAny>>>,
) {
    let callback = {
        let mut state = match shared.state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        let (should_report, _inflight) = state.fanout.mark_failure();
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
        Python::with_gil(|py| {
            let _ = callback.call1(py, (error.code, error.message));
        });
    }
}

fn set_terminal_error(shared: &Shared, error: RuntimeError) {
    let callback = {
        let mut state = match shared.state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        state.terminal_error = Some(error.clone());
        state.running = false;
        let (should_report, _inflight) = state.fanout.mark_failure();
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
        Python::with_gil(|py| {
            let _ = callback.call1(py, (error.code, error.message));
        });
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
    use pyo3::types::PyBytes;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("rp-ylx-{name}-{}-{unique}", std::process::id()))
    }

    fn test_recording(py: Python<'_>, root: &std::path::Path) -> SplitSinkRecording {
        fs::create_dir_all(root.join("video")).unwrap();
        SplitSinkRecording {
            active_take: Arc::new(Mutex::new(
                active_take::ActiveTakeWriter::new("session").unwrap(),
            )),
            sink: Arc::new(Mutex::new(
                recording::RecordingSink::create(root, "session").unwrap(),
            )),
            encoder: Arc::new(Mutex::new(
                stereo_encoder::EncoderProcess::new(
                    &root.join("video"),
                    std::path::Path::new("/bin/cat"),
                    3840,
                    1080,
                    60,
                    8192,
                    3,
                    "video/",
                )
                .unwrap(),
            )),
            segment_planner: Arc::new(Mutex::new(
                recording::RecordingSegmentPlanner::new(3).unwrap(),
            )),
            recording_start_monotonic_ns: 1,
            on_failure: Arc::new(PyBytes::new(py, b"on-failure").into_any().unbind()),
            imu: None,
        }
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
            let active_take = Arc::new(Mutex::new(
                active_take::ActiveTakeWriter::new("session").unwrap(),
            ));
            let sink = Arc::new(Mutex::new(
                recording::RecordingSink::create(&root, "session").unwrap(),
            ));
            let mut encoder = stereo_encoder::EncoderProcess::new(
                &root.join("video"),
                &helper,
                3840,
                1080,
                60,
                8192,
                3,
                "video/",
            )
            .unwrap();
            encoder.start().unwrap();
            let encoder = Arc::new(Mutex::new(encoder));
            let segment_planner = Arc::new(Mutex::new(
                recording::RecordingSegmentPlanner::new(3).unwrap(),
            ));

            write_split_sink_frame(
                Arc::clone(&active_take),
                Arc::clone(&sink),
                Arc::clone(&encoder),
                Arc::clone(&segment_planner),
                1_000_000,
                9,
                34_000_000,
                0,
                b"prefix\xff\xd8payload\xff\xd9suffix".to_vec(),
            )
            .unwrap();

            let summary = active_take.lock().unwrap().finish().unwrap();
            assert_eq!(summary.frames_written, 1);
            assert_eq!(summary.frame_domain, 1);
            assert_eq!(summary.pending_frames, 0);
            let snapshot = sink.lock().unwrap().flush_and_close().unwrap();
            assert_eq!(snapshot.frames_written, 1);
            assert_eq!(snapshot.imu_samples_written, 0);
            assert!(snapshot.bytes_written > 0);
            assert!(
                snapshot
                    .artifacts
                    .iter()
                    .any(|artifact| artifact.relative_path == "frames.ndjson")
            );
            assert_eq!(encoder.lock().unwrap().submitted_frames(), 1);
            assert_eq!(segment_planner.lock().unwrap().snapshot().frames_written, 1);
        }

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn stop_recording_clears_imu_worker_when_no_frames_are_inflight() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
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
                state.fanout.start_recording().unwrap();
                state.recording = Some(test_recording(py, &root));
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
        });
    }

    #[test]
    fn begin_recording_stop_closes_frame_gate_before_resource_cleanup() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let root = temp_root("begin-stop");
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
                state.fanout.start_recording().unwrap();
                state.recording = Some(test_recording(py, &root));
            }
            let (inflight, imu) = runtime.begin_recording_stop().unwrap();
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
        });
    }
}
