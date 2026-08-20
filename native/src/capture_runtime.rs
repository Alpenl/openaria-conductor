use crate::imu::{self, Collector, ImuError};
use crate::metrics::Metrics;
use crate::native_camera::{Frame, FrameValidator, Stream, StreamError};
use crate::preview::{LatestBuffer, PreviewError};
use crate::recording::{CaptureFanoutState, RecordingError};
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

struct RecordingCallbacks {
    submit_frame: Py<PyAny>,
    on_failure: Py<PyAny>,
    imu: Option<Arc<Collector>>,
}

struct State {
    fanout: CaptureFanoutState,
    validator: FrameValidator,
    recording: Option<RecordingCallbacks>,
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

    pub(crate) fn start_recording(
        &self,
        submit_frame: Py<PyAny>,
        on_failure: Py<PyAny>,
        imu: Option<Arc<Collector>>,
        submit_imu: Option<Py<PyAny>>,
        imu_timeout: Duration,
    ) -> Result<RuntimeSnapshot, RuntimeError> {
        if imu.is_some() != submit_imu.is_some() {
            return Err(RuntimeError::new(
                "invalid_argument",
                "capture runtime IMU collector and callback must be provided together",
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
        let on_failure_for_imu = if imu_for_worker.is_some() {
            Some(Python::with_gil(|py| on_failure.clone_ref(py)))
        } else {
            None
        };
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
        state.recording = Some(RecordingCallbacks {
            submit_frame,
            on_failure,
            imu,
        });
        self.shared.changed.notify_all();
        let snapshot = snapshot_locked(&state);
        drop(state);
        if let (Some(collector), Some(callback), Some(on_failure)) =
            (imu_for_worker, submit_imu, on_failure_for_imu)
        {
            self.imu_stop.store(false, Ordering::Release);
            let shared = Arc::clone(&self.shared);
            let stop = Arc::clone(&self.imu_stop);
            let metrics = self.metrics.as_ref().map(Arc::clone);
            let (done_tx, done_rx) = mpsc::channel();
            *imu_worker = Some(WorkerHandle {
                handle: thread::spawn(move || {
                    run_imu_loop(
                        collector,
                        callback,
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
        self.request_imu_stop()?;
        let state = self.shared.state.lock().map_err(|_| {
            RuntimeError::new(
                "capture_runtime_poisoned",
                "capture runtime state mutex is poisoned",
            )
        })?;
        let mut state = state;
        let inflight = state.fanout.start_stopping();
        if inflight == 0 {
            state.recording = None;
            self.shared.changed.notify_all();
            return Ok(snapshot_locked(&state));
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
            state
                .recording
                .as_ref()
                .and_then(|callbacks| callbacks.imu.as_ref().map(Arc::clone))
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
    let mut submit_failure: Option<RuntimeError> = None;
    let result = Python::with_gil(|py| {
        let left_size = usize_to_u64(frame.left.as_bytes(py).len());
        let right_size = usize_to_u64(frame.right.as_bytes(py).len());
        let raw_size = usize_to_u64(frame.raw_side_by_side.as_bytes(py).len());
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
            Some(frame.left.clone_ref(py))
        } else if has_raw {
            Some(frame.raw_side_by_side.clone_ref(py))
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
        let (record, dropped_before, submit_frame, on_failure) = {
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
                if let Some(jpeg) = &preview_jpeg {
                    let publish_started = start_stage(metrics);
                    if let Err(error) = preview.publish(py, jpeg.clone_ref(py)) {
                        state.last_preview_error = Some(RuntimeError::from(error));
                    }
                    finish_stage(metrics, "native_preview_publish", publish_started);
                }
            }
            let callbacks = if decision.record {
                state.recording.as_ref().map(|callbacks| {
                    (
                        callbacks.submit_frame.clone_ref(py),
                        callbacks.on_failure.clone_ref(py),
                    )
                })
            } else {
                None
            };
            (
                decision.record && callbacks.is_some(),
                decision.dropped_before,
                callbacks
                    .as_ref()
                    .map(|callbacks| callbacks.0.clone_ref(py)),
                callbacks.map(|callbacks| callbacks.1),
            )
        };
        if record {
            if let Some(callback) = submit_frame {
                let callback_started = start_stage(metrics);
                let result = callback.call1(
                    py,
                    (
                        frame.source_sequence,
                        frame.host_monotonic_ns,
                        dropped_before,
                        frame.left.clone_ref(py),
                        frame.right.clone_ref(py),
                        frame.raw_side_by_side.clone_ref(py),
                    ),
                );
                finish_stage(metrics, "native_recording_callback", callback_started);
                if let Err(error) = result {
                    submit_failure = Some(RuntimeError::new(
                        "camera_failed",
                        format!("recording frame callback failed: {error}"),
                    ));
                }
            }
            finish_recording_frame(shared)?;
            if let (Some(error), Some(on_failure)) = (submit_failure.clone(), on_failure) {
                report_recording_failure(shared, py, error, Some(on_failure));
            }
        }
        Ok(())
    });
    finish_stage(metrics, "native_capture_frame", frame_started);
    result
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
    submit_imu: Py<PyAny>,
    on_failure: Py<PyAny>,
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
                let mut failure: Option<RuntimeError> = None;
                Python::with_gil(|py| match imu::observation_dict(py, &observation) {
                    Ok(payload) => {
                        let callback_started = start_stage(metrics.as_ref());
                        let result = submit_imu.call1(py, (payload,));
                        finish_stage(metrics.as_ref(), "native_imu_callback", callback_started);
                        if let Err(error) = result {
                            failure = Some(RuntimeError::new(
                                "imu_failed",
                                format!("recording IMU callback failed: {error}"),
                            ));
                        }
                    }
                    Err(error) => {
                        failure = Some(RuntimeError::new(
                            "imu_failed",
                            format!("recording IMU payload conversion failed: {error}"),
                        ));
                    }
                });
                if let Some(error) = failure {
                    if !stop.load(Ordering::Acquire) && recording_present(&shared) {
                        Python::with_gil(|py| {
                            report_recording_failure(
                                &shared,
                                py,
                                error,
                                Some(on_failure.clone_ref(py)),
                            );
                        });
                    }
                    break;
                }
            }
            Err(error) => {
                if !stop.load(Ordering::Acquire) && recording_present(&shared) {
                    Python::with_gil(|py| {
                        report_recording_failure(
                            &shared,
                            py,
                            error.into(),
                            Some(on_failure.clone_ref(py)),
                        );
                    });
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
    py: Python<'_>,
    error: RuntimeError,
    preferred_callback: Option<Py<PyAny>>,
) {
    let callback = {
        let mut state = match shared.state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        let (should_report, _inflight) = state.fanout.mark_failure();
        if !state.fanout.snapshot().recording_present {
            state.recording = None;
        }
        shared.changed.notify_all();
        if should_report {
            preferred_callback.or_else(|| {
                state
                    .recording
                    .as_ref()
                    .map(|callbacks| callbacks.on_failure.clone_ref(py))
            })
        } else {
            None
        }
    };
    if let Some(callback) = callback {
        let _ = callback.call1(py, (error.code, error.message));
    }
}

fn set_terminal_error(shared: &Shared, error: RuntimeError) {
    Python::with_gil(|py| {
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
                    .map(|callbacks| callbacks.on_failure.clone_ref(py))
            } else {
                None
            };
            state.recording = None;
            shared.changed.notify_all();
            callback
        };
        if let Some(callback) = callback {
            let _ = callback.call1(py, (error.code, error.message));
        }
    });
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
