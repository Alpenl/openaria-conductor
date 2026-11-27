use crate::hardware::bounded::{self, Consumer, Producer};
use crate::hardware::jpeg;
use crate::hardware::turbojpeg::{TransformHandle, TurboJpegError};
use crate::hardware::v4l2::{
    Capture, CaptureError, ExposureConfig, FocusStatus, MAX_CONSECUTIVE_BAD_FRAMES,
};
use miro_ego_core::config::ExposureSetting;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub type DropCallback = Arc<dyn Fn(u64, u64) + Send + Sync>;

const PRODUCER_POLL: Duration = Duration::from_millis(50);
const CONTROL_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamError {
    pub code: &'static str,
    pub message: String,
}

impl StreamError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl From<CaptureError> for StreamError {
    fn from(error: CaptureError) -> Self {
        Self::new(error.code, error.message)
    }
}

impl From<TurboJpegError> for StreamError {
    fn from(error: TurboJpegError) -> Self {
        Self::new(error.code, error.message)
    }
}

pub struct Frame {
    pub buffer_flags: u32,
    pub dequeue_monotonic_ns: u64,
    pub source_sequence: u64,
    pub host_monotonic_ns: u64,
    pub application_dropped_before: u64,
    pub corrupt_dropped_before: u64,
    pub producer_max_interval_ns: u64,
    pub producer_max_control_ns: u64,
    pub producer_max_read_ns: u64,
    pub left: Vec<u8>,
    pub right: Vec<u8>,
    pub raw_side_by_side: Arc<[u8]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameValidation {
    pub dropped_before: u64,
    pub queue_rejected: u64,
    pub source_gap: u64,
}

#[derive(Debug, Default)]
pub struct FrameValidator {
    last_source_sequence: Option<u64>,
    last_host_time: Option<u64>,
}

impl FrameValidator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        self.last_source_sequence = None;
        self.last_host_time = None;
    }

    pub fn validate(
        &mut self,
        source_sequence: i64,
        host_monotonic_ns: i64,
        valid: bool,
        has_payload: bool,
        application_dropped_before: i64,
    ) -> Result<FrameValidation, StreamError> {
        if !valid || source_sequence < 0 || host_monotonic_ns < 0 || !has_payload {
            return Err(StreamError::new(
                "bad_frame",
                "camera returned incomplete or invalid stereo frame",
            ));
        }
        if application_dropped_before < 0 {
            return Err(StreamError::new(
                "invalid_drop_accounting",
                "application dropped frame count must not be negative",
            ));
        }
        let source_sequence = u64::try_from(source_sequence)
            .map_err(|_| StreamError::new("bad_frame", "camera source sequence is out of range"))?;
        let host_monotonic_ns = u64::try_from(host_monotonic_ns)
            .map_err(|_| StreamError::new("bad_frame", "camera timestamp is out of range"))?;
        let application_dropped_before =
            u64::try_from(application_dropped_before).map_err(|_| {
                StreamError::new(
                    "invalid_drop_accounting",
                    "application dropped frame count is out of range",
                )
            })?;

        let mut dropped_before = 0;
        let mut queue_rejected = 0;
        let mut source_gap = 0;
        if let Some(last_source_sequence) = self.last_source_sequence {
            if source_sequence <= last_source_sequence {
                return Err(StreamError::new(
                    "sequence_regression",
                    "camera source sequence repeated or regressed",
                ));
            }
            dropped_before = source_sequence - last_source_sequence - 1;
            if application_dropped_before > dropped_before {
                return Err(StreamError::new(
                    "invalid_drop_accounting",
                    "application dropped frame count exceeds camera source sequence gap",
                ));
            }
            queue_rejected = application_dropped_before;
            source_gap = dropped_before - application_dropped_before;
        } else if application_dropped_before != 0 {
            dropped_before = application_dropped_before;
            queue_rejected = application_dropped_before;
        }

        if let Some(last_host_time) = self.last_host_time {
            if host_monotonic_ns <= last_host_time {
                return Err(StreamError::new(
                    "timestamp_regression",
                    "camera host timestamp repeated or regressed",
                ));
            }
        }
        self.last_source_sequence = Some(source_sequence);
        self.last_host_time = Some(host_monotonic_ns);
        Ok(FrameValidation {
            dropped_before,
            queue_rejected,
            source_gap,
        })
    }
}

struct Resources {
    capture: Capture,
    splitter: Option<TransformHandle>,
    width: i32,
    height: i32,
    controls: Receiver<ControlCommand>,
}

impl Resources {
    fn close(&mut self) {
        self.capture.close();
        if let Some(splitter) = self.splitter.as_mut() {
            splitter.close();
        }
    }
}

enum ControlCommand {
    FocusStatus(Sender<Result<Option<FocusStatus>, CaptureError>>),
    SetFocus {
        value: Option<i32>,
        auto_enabled: Option<bool>,
        response: Sender<Result<FocusStatus, CaptureError>>,
    },
    SetExposure {
        exposure: ExposureSetting,
        response: Sender<Result<ExposureConfig, CaptureError>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Open,
    Running,
    Stopping,
    Stopped,
    Closed,
}

struct Lifecycle {
    state: State,
    resources: Option<Resources>,
    worker: Option<JoinHandle<Option<Resources>>>,
}

pub struct StreamConfig<'a> {
    pub device: &'a str,
    pub width: u32,
    pub height: u32,
    pub fps: u32,
    pub encoding: &'a str,
    pub buffer_count: u32,
    pub queue_capacity: usize,
    pub split_eyes: bool,
}

pub struct Stream {
    lifecycle: Mutex<Lifecycle>,
    lifecycle_changed: Condvar,
    stop: Arc<AtomicBool>,
    producer: Producer<Frame>,
    consumer: Consumer<Frame>,
    controls: Sender<ControlCommand>,
    terminal_error: Arc<Mutex<Option<StreamError>>>,
    corrupt_frames_dropped: Arc<AtomicU64>,
    drop_callback: Arc<Mutex<Option<DropCallback>>>,
}

impl Stream {
    pub fn queue_stats(&self) -> (usize, usize, usize) {
        self.consumer.queue_stats()
    }

    pub fn corrupt_frames_dropped(&self) -> u64 {
        self.corrupt_frames_dropped.load(Ordering::Acquire)
    }

    pub fn set_drop_callback(&self, callback: Option<DropCallback>) {
        if let Ok(mut guard) = self.drop_callback.lock() {
            *guard = callback;
        }
    }

    pub fn open(config: StreamConfig<'_>) -> Result<Self, StreamError> {
        Self::open_with_exposure(config, None)
    }

    pub fn open_with_exposure(
        config: StreamConfig<'_>,
        exposure: Option<ExposureSetting>,
    ) -> Result<Self, StreamError> {
        if config.queue_capacity == 0 {
            return Err(StreamError::new(
                "invalid_argument",
                "native camera queue capacity must be positive",
            ));
        }
        let capture = Capture::open(
            config.device,
            config.width,
            config.height,
            config.fps,
            config.encoding,
            config.buffer_count,
        )?;
        if let Some(ref setting) = exposure {
            capture.set_exposure(setting)?;
        }
        let splitter = if config.split_eyes {
            Some(TransformHandle::open()?)
        } else {
            None
        };
        Self::from_parts(
            capture,
            splitter,
            config.width,
            config.height,
            config.queue_capacity,
        )
    }

    fn from_parts(
        capture: Capture,
        splitter: Option<TransformHandle>,
        width: u32,
        height: u32,
        queue_capacity: usize,
    ) -> Result<Self, StreamError> {
        // Absorb short codec/storage stalls without allowing large MJPEG frames
        // to turn a frame-count bound into unbounded memory pressure.
        let (producer, consumer) =
            bounded::weighted_channel(queue_capacity, 128 * 1024 * 1024, |frame: &Frame| {
                frame.raw_side_by_side.len() + frame.left.len() + frame.right.len()
            });
        let (control_sender, control_receiver) = mpsc::channel();
        Ok(Self {
            lifecycle: Mutex::new(Lifecycle {
                state: State::Open,
                resources: Some(Resources {
                    capture,
                    splitter,
                    width: i32::try_from(width).map_err(|_| {
                        StreamError::new("unsupported_mode", "capture width is too large")
                    })?,
                    height: i32::try_from(height).map_err(|_| {
                        StreamError::new("unsupported_mode", "capture height is too large")
                    })?,
                    controls: control_receiver,
                }),
                worker: None,
            }),
            lifecycle_changed: Condvar::new(),
            stop: Arc::new(AtomicBool::new(false)),
            producer,
            consumer,
            controls: control_sender,
            terminal_error: Arc::new(Mutex::new(None)),
            corrupt_frames_dropped: Arc::new(AtomicU64::new(0)),
            drop_callback: Arc::new(Mutex::new(None)),
        })
    }

    pub fn start(&self) -> Result<(), StreamError> {
        let mut lifecycle = self.lifecycle.lock().map_err(|_| {
            StreamError::new("native_camera_poisoned", "native camera mutex is poisoned")
        })?;
        if !matches!(lifecycle.state, State::Open | State::Stopped) {
            return Err(StreamError::new(
                "invalid_state",
                "native camera can only be started once",
            ));
        }
        let mut resources = lifecycle.resources.take().ok_or_else(|| {
            StreamError::new("invalid_state", "native camera resources are missing")
        })?;
        if let Err(error) = resources.capture.start() {
            resources.close();
            lifecycle.state = State::Closed;
            self.lifecycle_changed.notify_all();
            return Err(error.into());
        }
        self.stop.store(false, Ordering::Release);
        self.consumer.reopen();
        *self
            .terminal_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        let stop = Arc::clone(&self.stop);
        let producer = self.producer.clone();
        let terminal_error = Arc::clone(&self.terminal_error);
        let corrupt_counter = Arc::clone(&self.corrupt_frames_dropped);
        let drop_callback = Arc::clone(&self.drop_callback);
        let slot = Arc::new(Mutex::new(Some(resources)));
        let worker_slot = Arc::clone(&slot);
        let worker = thread::Builder::new()
            .name("native-camera".to_owned())
            .spawn(move || {
                let mut resources = match worker_slot
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .take()
                {
                    Some(resources) => resources,
                    None => {
                        producer.close();
                        return None;
                    }
                };
                run_producer(
                    &mut resources,
                    &producer,
                    &stop,
                    &terminal_error,
                    &corrupt_counter,
                    &drop_callback,
                );
                drain_pending_controls(&resources.controls);
                producer.close();
                Some(resources)
            })
            .map_err(|error| {
                if let Some(mut resources) = slot.lock().ok().and_then(|mut guard| guard.take()) {
                    resources.close();
                }
                lifecycle.state = State::Closed;
                self.lifecycle_changed.notify_all();
                StreamError::new(
                    "native_camera_start_failed",
                    format!("spawn camera worker: {error}"),
                )
            })?;
        lifecycle.worker = Some(worker);
        lifecycle.state = State::Running;
        self.lifecycle_changed.notify_all();
        Ok(())
    }

    pub fn read(&self, timeout: Duration) -> Result<Frame, StreamError> {
        let state = self
            .lifecycle
            .lock()
            .map_err(|_| {
                StreamError::new("native_camera_poisoned", "native camera mutex is poisoned")
            })?
            .state;
        if state != State::Running {
            return Err(StreamError::new(
                "invalid_state",
                "native camera is not running",
            ));
        }
        match self.consumer.receive(timeout) {
            Ok(frame) => Ok(frame),
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
                let terminal = self
                    .terminal_error
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                if let Some(error) = terminal {
                    Err(error)
                } else {
                    Err(StreamError::new(
                        "frame_timeout",
                        "native camera frame timed out",
                    ))
                }
            }
        }
    }

    pub fn focus_status(&self) -> Result<Option<FocusStatus>, StreamError> {
        let lifecycle = self.lifecycle.lock().map_err(|_| {
            StreamError::new("native_camera_poisoned", "native camera mutex is poisoned")
        })?;
        match lifecycle.state {
            State::Open | State::Stopped => lifecycle
                .resources
                .as_ref()
                .ok_or_else(|| {
                    StreamError::new("invalid_state", "native camera resources are missing")
                })?
                .capture
                .focus_status()
                .map_err(StreamError::from),
            State::Running => {
                let (response, result) = mpsc::channel();
                self.controls
                    .send(ControlCommand::FocusStatus(response))
                    .map_err(|_| {
                        StreamError::new(
                            "native_camera_control_failed",
                            "native camera control channel is closed",
                        )
                    })?;
                drop(lifecycle);
                result
                    .recv_timeout(CONTROL_TIMEOUT)
                    .map_err(|_| {
                        StreamError::new(
                            "native_camera_control_timeout",
                            "native camera focus query timed out",
                        )
                    })?
                    .map_err(StreamError::from)
            }
            State::Stopping | State::Closed => {
                Err(StreamError::new("invalid_state", "native camera is closed"))
            }
        }
    }

    pub fn set_focus(
        &self,
        value: Option<i32>,
        auto_enabled: Option<bool>,
    ) -> Result<FocusStatus, StreamError> {
        let lifecycle = self.lifecycle.lock().map_err(|_| {
            StreamError::new("native_camera_poisoned", "native camera mutex is poisoned")
        })?;
        match lifecycle.state {
            State::Open | State::Stopped => lifecycle
                .resources
                .as_ref()
                .ok_or_else(|| {
                    StreamError::new("invalid_state", "native camera resources are missing")
                })?
                .capture
                .set_focus(value, auto_enabled)
                .map_err(StreamError::from),
            State::Running => {
                let (response, result) = mpsc::channel();
                self.controls
                    .send(ControlCommand::SetFocus {
                        value,
                        auto_enabled,
                        response,
                    })
                    .map_err(|_| {
                        StreamError::new(
                            "native_camera_control_failed",
                            "native camera control channel is closed",
                        )
                    })?;
                drop(lifecycle);
                result
                    .recv_timeout(CONTROL_TIMEOUT)
                    .map_err(|_| {
                        StreamError::new(
                            "native_camera_control_timeout",
                            "native camera focus update timed out",
                        )
                    })?
                    .map_err(StreamError::from)
            }
            State::Stopping | State::Closed => {
                Err(StreamError::new("invalid_state", "native camera is closed"))
            }
        }
    }

    pub fn set_exposure(&self, exposure: &ExposureSetting) -> Result<ExposureConfig, StreamError> {
        let lifecycle = self.lifecycle.lock().map_err(|_| {
            StreamError::new("native_camera_poisoned", "native camera mutex is poisoned")
        })?;
        match lifecycle.state {
            State::Open | State::Stopped => lifecycle
                .resources
                .as_ref()
                .ok_or_else(|| {
                    StreamError::new("invalid_state", "native camera resources are missing")
                })?
                .capture
                .set_exposure(exposure)
                .map_err(StreamError::from),
            State::Running => {
                let (response, result) = mpsc::channel();
                self.controls
                    .send(ControlCommand::SetExposure {
                        exposure: exposure.clone(),
                        response,
                    })
                    .map_err(|_| {
                        StreamError::new(
                            "native_camera_control_failed",
                            "native camera control channel is closed",
                        )
                    })?;
                drop(lifecycle);
                result
                    .recv_timeout(CONTROL_TIMEOUT)
                    .map_err(|_| {
                        StreamError::new(
                            "native_camera_control_timeout",
                            "native camera exposure update timed out",
                        )
                    })?
                    .map_err(StreamError::from)
            }
            State::Stopping | State::Closed => {
                Err(StreamError::new("invalid_state", "native camera is closed"))
            }
        }
    }

    pub fn stop(&self) -> Result<(), StreamError> {
        let worker = {
            let mut lifecycle = self.lifecycle.lock().map_err(|_| {
                StreamError::new("native_camera_poisoned", "native camera mutex is poisoned")
            })?;
            while lifecycle.state == State::Stopping {
                lifecycle = self.lifecycle_changed.wait(lifecycle).map_err(|_| {
                    StreamError::new("native_camera_poisoned", "native camera mutex is poisoned")
                })?;
            }
            match lifecycle.state {
                State::Open => {
                    lifecycle.state = State::Stopped;
                    self.lifecycle_changed.notify_all();
                    return Ok(());
                }
                State::Stopped | State::Closed => return Ok(()),
                State::Stopping => unreachable!("waited until Stopping finished"),
                State::Running => {
                    self.stop.store(true, Ordering::Release);
                    lifecycle.state = State::Stopping;
                    lifecycle.worker.take()
                }
            }
        };
        let joined = worker
            .ok_or_else(|| StreamError::new("invalid_state", "native camera worker is missing"))
            .and_then(|handle| {
                handle.join().map_err(|_| {
                    StreamError::new(
                        "native_camera_worker_failed",
                        "native camera worker panicked",
                    )
                })
            })
            .and_then(|maybe_resources| {
                maybe_resources.ok_or_else(|| {
                    StreamError::new("invalid_state", "native camera resources are missing")
                })
            });
        match joined {
            Ok(mut resources) => {
                drain_pending_controls(&resources.controls);
                let stop_result = if resources.capture.is_closed() {
                    Ok(())
                } else {
                    resources.capture.stop().map_err(StreamError::from)
                };
                let must_close = stop_result.is_err() || resources.capture.is_closed();
                if must_close {
                    resources.close();
                }
                let mut lifecycle = self.lifecycle.lock().map_err(|_| {
                    resources.close();
                    StreamError::new("native_camera_poisoned", "native camera mutex is poisoned")
                })?;
                drain_pending_controls(&resources.controls);
                if must_close {
                    lifecycle.resources = None;
                    lifecycle.state = State::Closed;
                } else {
                    lifecycle.resources = Some(resources);
                    lifecycle.state = State::Stopped;
                }
                self.terminal_error
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .get_or_insert_with(|| {
                        StreamError::new("capture_stopped", "native camera capture stopped")
                    });
                self.consumer.close_and_clear();
                self.lifecycle_changed.notify_all();
                stop_result
            }
            Err(error) => {
                if let Ok(mut lifecycle) = self.lifecycle.lock() {
                    lifecycle.state = State::Closed;
                }
                self.consumer.close_and_clear();
                self.lifecycle_changed.notify_all();
                Err(error)
            }
        }
    }

    pub fn close(&self) -> Result<(), StreamError> {
        let stop_result = self.stop();
        let mut lifecycle = self.lifecycle.lock().map_err(|_| {
            StreamError::new("native_camera_poisoned", "native camera mutex is poisoned")
        })?;
        if lifecycle.state == State::Closed {
            return stop_result;
        }
        if let Some(resources) = lifecycle.resources.as_mut() {
            resources.close();
        }
        lifecycle.resources = None;
        lifecycle.state = State::Closed;
        self.lifecycle_changed.notify_all();
        stop_result
    }

    #[cfg(test)]
    pub fn test_idle(queue_capacity: usize) -> Self {
        let (producer, consumer) = bounded::channel(queue_capacity);
        let (control_sender, _control_receiver) = mpsc::channel();
        Self {
            lifecycle: Mutex::new(Lifecycle {
                state: State::Stopped,
                resources: None,
                worker: None,
            }),
            lifecycle_changed: Condvar::new(),
            stop: Arc::new(AtomicBool::new(false)),
            producer,
            consumer,
            controls: control_sender,
            terminal_error: Arc::new(Mutex::new(None)),
            corrupt_frames_dropped: Arc::new(AtomicU64::new(0)),
            drop_callback: Arc::new(Mutex::new(None)),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_fake(fail_start: bool, queue_capacity: usize) -> Self {
        let capture = Capture::test_fake(fail_start).unwrap();
        Self::from_parts(capture, None, 3840, 1080, queue_capacity).unwrap()
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn notify_corrupt_drop(
    corrupt_counter: &AtomicU64,
    drop_callback: &Mutex<Option<DropCallback>>,
    dropped: u64,
    host_monotonic_ns: u64,
) {
    corrupt_counter.fetch_add(dropped, Ordering::AcqRel);
    let cb = drop_callback.lock().ok().and_then(|guard| guard.clone());
    if let Some(cb) = cb {
        cb(dropped, host_monotonic_ns);
    }
}

fn run_producer(
    resources: &mut Resources,
    producer: &Producer<Frame>,
    stop: &AtomicBool,
    terminal_error: &Mutex<Option<StreamError>>,
    corrupt_counter: &AtomicU64,
    drop_callback: &Mutex<Option<DropCallback>>,
) {
    let mut pending_rejected = 0_u64;
    let mut pending_corrupt_drops = 0_u64;
    let mut consecutive_bad_frames = 0_u32;
    let mut previous_frame = Instant::now();
    let mut timings = [(0_u64, 0_u64, 0_u64); 64];
    let mut timing_index = 0;
    while !stop.load(Ordering::Acquire) {
        let control_started = Instant::now();
        process_control_commands(resources);
        let control_ns = control_started.elapsed().as_nanos() as u64;
        match resources.capture.wait(PRODUCER_POLL) {
            Ok(()) => {}
            Err(error) if error.code == "frame_timeout" => continue,
            Err(error) => {
                *terminal_error
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(error.into());
                return;
            }
        }
        let read_started = Instant::now();
        let read_res =
            resources
                .capture
                .read_ready_with(|source_sequence, host_monotonic_ns, payload| {
                    // Own the V4L2 bytes before requeueing its buffer. Recording
                    // and latest preview then share this immutable allocation.
                    Ok((
                        source_sequence,
                        host_monotonic_ns,
                        Arc::<[u8]>::from(payload),
                    ))
                });
        let (source_sequence, host_monotonic_ns, raw_side_by_side) = match read_res {
            Ok(tuple) => tuple,
            Err(error) if error.code == "frame_timeout" => continue,
            Err(error) if error.code == "bad_frame" => {
                let ts = resources.capture.last_dequeue_monotonic_ns;
                notify_corrupt_drop(corrupt_counter, drop_callback, 1, ts);
                pending_corrupt_drops = pending_corrupt_drops.saturating_add(1);
                consecutive_bad_frames = consecutive_bad_frames.saturating_add(1);
                if resources.capture.is_closed()
                    || consecutive_bad_frames >= MAX_CONSECUTIVE_BAD_FRAMES
                {
                    resources.capture.close();
                    *terminal_error
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(error.into());
                    return;
                }
                continue;
            }
            Err(error) => {
                resources.capture.close();
                *terminal_error
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(error.into());
                return;
            }
        };

        // Validate complete MJPEG SOI..EOI framing before splitting or forwarding downstream.
        if jpeg::ranges(&raw_side_by_side).is_empty() {
            notify_corrupt_drop(corrupt_counter, drop_callback, 1, host_monotonic_ns);
            pending_corrupt_drops = pending_corrupt_drops.saturating_add(1);
            consecutive_bad_frames = consecutive_bad_frames.saturating_add(1);
            if consecutive_bad_frames >= MAX_CONSECUTIVE_BAD_FRAMES {
                resources.capture.close();
                *terminal_error
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(StreamError::new(
                    "bad_frame",
                    format!(
                        "camera produced {consecutive_bad_frames} consecutive truncated/corrupt MJPEG frames"
                    ),
                ));
                return;
            }
            continue;
        }

        let split_res = if let Some(splitter) = resources.splitter.as_mut() {
            splitter
                .split_sbs(&raw_side_by_side, resources.width, resources.height)
                .map_err(|error| StreamError::new(error.code, error.message))
        } else {
            Ok((Vec::new(), Vec::new()))
        };
        let (left, right) = match split_res {
            Ok(pair) => pair,
            Err(error) => {
                notify_corrupt_drop(corrupt_counter, drop_callback, 1, host_monotonic_ns);
                pending_corrupt_drops = pending_corrupt_drops.saturating_add(1);
                consecutive_bad_frames = consecutive_bad_frames.saturating_add(1);
                if consecutive_bad_frames >= MAX_CONSECUTIVE_BAD_FRAMES {
                    resources.capture.close();
                    *terminal_error
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(error);
                    return;
                }
                continue;
            }
        };

        consecutive_bad_frames = 0;
        let now = Instant::now();
        timings[timing_index] = (
            now.duration_since(previous_frame).as_nanos() as u64,
            control_ns,
            read_started.elapsed().as_nanos() as u64,
        );
        timing_index = (timing_index + 1) % timings.len();
        previous_frame = now;
        let frame = Frame {
            buffer_flags: resources.capture.last_buffer_flags,
            dequeue_monotonic_ns: resources.capture.last_dequeue_monotonic_ns,
            source_sequence,
            host_monotonic_ns,
            application_dropped_before: pending_rejected,
            corrupt_dropped_before: pending_corrupt_drops,
            producer_max_interval_ns: timings.iter().map(|value| value.0).max().unwrap_or(0),
            producer_max_control_ns: timings.iter().map(|value| value.1).max().unwrap_or(0),
            producer_max_read_ns: timings.iter().map(|value| value.2).max().unwrap_or(0),
            left,
            right,
            raw_side_by_side,
        };
        match producer.try_push(frame) {
            Ok(()) => {
                pending_rejected = 0;
                pending_corrupt_drops = 0;
            }
            Err(frame) => {
                pending_rejected = frame.application_dropped_before + 1;
                pending_corrupt_drops = frame.corrupt_dropped_before;
            }
        }
    }
}

fn process_control_commands(resources: &Resources) {
    loop {
        match resources.controls.try_recv() {
            Ok(ControlCommand::FocusStatus(response)) => {
                let _ = response.send(resources.capture.focus_status());
            }
            Ok(ControlCommand::SetFocus {
                value,
                auto_enabled,
                response,
            }) => {
                let _ = response.send(resources.capture.set_focus(value, auto_enabled));
            }
            Ok(ControlCommand::SetExposure { exposure, response }) => {
                let _ = response.send(resources.capture.set_exposure(&exposure));
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => return,
        }
    }
}

fn drain_pending_controls(controls: &Receiver<ControlCommand>) {
    while let Ok(command) = controls.try_recv() {
        let error = CaptureError::new("invalid_state", "native camera is stopped");
        match command {
            ControlCommand::FocusStatus(response) => {
                let _ = response.send(Err(error));
            }
            ControlCommand::SetFocus { response, .. } => {
                let _ = response.send(Err(error));
            }
            ControlCommand::SetExposure { response, .. } => {
                let _ = response.send(Err(error));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FrameValidator;

    #[test]
    fn validator_separates_queue_rejection_from_source_gap() {
        let mut validator = FrameValidator::new();
        let first = validator.validate(10, 100, true, true, 0).unwrap();
        assert_eq!(first.dropped_before, 0);
        assert_eq!(first.queue_rejected, 0);
        assert_eq!(first.source_gap, 0);

        let second = validator.validate(14, 200, true, true, 2).unwrap();
        assert_eq!(second.dropped_before, 3);
        assert_eq!(second.queue_rejected, 2);
        assert_eq!(second.source_gap, 1);
    }

    #[test]
    fn validator_rejects_regressions_and_bad_drop_accounting() {
        let mut validator = FrameValidator::new();
        validator.validate(2, 100, true, true, 0).unwrap();
        let sequence = validator.validate(2, 200, true, true, 0).unwrap_err();
        assert_eq!(sequence.code, "sequence_regression");

        validator.reset();
        validator.validate(2, 100, true, true, 0).unwrap();
        let timestamp = validator.validate(3, 100, true, true, 0).unwrap_err();
        assert_eq!(timestamp.code, "timestamp_regression");

        validator.reset();
        validator.validate(2, 100, true, true, 0).unwrap();
        let drops = validator.validate(3, 200, true, true, 2).unwrap_err();
        assert_eq!(drops.code, "invalid_drop_accounting");
    }

    #[test]
    fn validator_reset_starts_a_new_stream() {
        let mut validator = FrameValidator::new();
        validator.validate(10, 100, true, true, 0).unwrap();
        validator.reset();
        let result = validator.validate(3, 50, true, true, 1).unwrap();
        assert_eq!(result.dropped_before, 1);
        assert_eq!(result.queue_rejected, 1);
        assert_eq!(result.source_gap, 0);
    }

    #[test]
    fn stream_start_failure_closes_and_rolls_back_state() {
        let capture = crate::hardware::v4l2::Capture::test_fake(true).unwrap();
        let stream = super::Stream::from_parts(capture, None, 3840, 1080, 4).unwrap();
        let err = stream.start().unwrap_err();
        assert_eq!(err.code, "v4l2_ioctl_failed");
        assert_eq!(stream.lifecycle.lock().unwrap().state, super::State::Closed);
        assert!(stream.lifecycle.lock().unwrap().resources.is_none());
    }

    #[test]
    fn stream_stop_drains_pending_controls_and_unblocks_callers_promptly() {
        let (drain_tx, drain_rx) = std::sync::mpsc::channel();
        let (status_tx, status_rx) = std::sync::mpsc::channel();
        drain_tx
            .send(super::ControlCommand::FocusStatus(status_tx))
            .unwrap();
        super::drain_pending_controls(&drain_rx);
        assert_eq!(
            status_rx.try_recv().unwrap().unwrap_err().code,
            "invalid_state"
        );

        let capture = crate::hardware::v4l2::Capture::test_fake(false).unwrap();
        let stream =
            std::sync::Arc::new(super::Stream::from_parts(capture, None, 3840, 1080, 4).unwrap());
        stream.start().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        stream
            .controls
            .send(super::ControlCommand::FocusStatus(tx))
            .unwrap();
        let started = std::time::Instant::now();
        stream.stop().unwrap();
        let result = rx
            .recv_timeout(std::time::Duration::from_millis(250))
            .unwrap();
        if let Err(err) = result {
            assert_eq!(err.code, "invalid_state");
        }
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        assert_eq!(
            stream.lifecycle.lock().unwrap().state,
            super::State::Stopped
        );
        // Subsequent focus_status on Stopped returns directly from capture.
        assert!(stream.focus_status().unwrap().is_some());
    }

    #[test]
    fn stop_preserves_causal_terminal_error_and_transitions_closed_capture_to_closed() {
        let mut capture = crate::hardware::v4l2::Capture::test_fake(false).unwrap();
        capture.close();
        assert!(capture.is_closed());
        let stream =
            std::sync::Arc::new(super::Stream::from_parts(capture, None, 3840, 1080, 4).unwrap());
        {
            let mut lifecycle = stream.lifecycle.lock().unwrap();
            let resources = lifecycle.resources.take().unwrap();
            lifecycle.state = super::State::Running;
            lifecycle.worker = Some(std::thread::spawn(move || Some(resources)));
        }
        *stream.terminal_error.lock().unwrap() = Some(super::StreamError::new(
            "device_disconnected",
            "usb camera unplugged",
        ));
        stream.stop().unwrap();
        let lifecycle = stream.lifecycle.lock().unwrap();
        assert_eq!(lifecycle.state, super::State::Closed);
        assert!(lifecycle.resources.is_none());
        let err_guard = stream.terminal_error.lock().unwrap();
        assert_eq!(err_guard.as_ref().unwrap().code, "device_disconnected");
        assert_eq!(err_guard.as_ref().unwrap().message, "usb camera unplugged");
    }

    #[test]
    fn producer_recovers_from_transient_corrupt_mjpeg_and_exposes_drop_callback() {
        use miro_ego_core::config::ExposureSetting;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::Duration;

        let capture = crate::hardware::v4l2::Capture::test_fake_with_frames(vec![
            (10, 1_000_000_000, b"\xff\xd8first\xff\xd9".to_vec(), 0x2000),
            // Truncated MJPEG missing EOI: must be dropped, requeued, and counted!
            (
                11,
                1_016_666_667,
                b"\xff\xd8truncated-no-eoi".to_vec(),
                0x2000,
            ),
            (12, 1_033_333_333, b"\xff\xd8third\xff\xd9".to_vec(), 0x2000),
        ])
        .unwrap();
        let stream = super::Stream::from_parts(capture, None, 3840, 1080, 4).unwrap();

        // Exposure control works cleanly through Stream::set_exposure before and during streaming.
        let exp = stream.set_exposure(&ExposureSetting::Manual(45)).unwrap();
        assert_eq!(exp.auto_exposure, 1);
        assert_eq!(exp.exposure_time_absolute, Some(45));

        let cb_drops = std::sync::Arc::new(AtomicU64::new(0));
        let cb_drops_clone = std::sync::Arc::clone(&cb_drops);
        stream.set_drop_callback(Some(std::sync::Arc::new(move |dropped, _ts| {
            cb_drops_clone.fetch_add(dropped, Ordering::AcqRel);
        })));

        stream.start().unwrap();
        let f1 = stream.read(Duration::from_secs(1)).unwrap();
        assert_eq!(f1.source_sequence, 10);
        assert_eq!(f1.corrupt_dropped_before, 0);

        let f2 = stream.read(Duration::from_secs(1)).unwrap();
        assert_eq!(f2.source_sequence, 12);
        assert_eq!(f2.corrupt_dropped_before, 1);
        assert_eq!(stream.corrupt_frames_dropped(), 1);
        assert_eq!(cb_drops.load(Ordering::Acquire), 1);

        stream.close().unwrap();
    }
}
