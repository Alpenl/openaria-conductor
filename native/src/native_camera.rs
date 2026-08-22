use crate::bounded::{self, Consumer, Producer, QueueStats};
use crate::turbojpeg::{TransformHandle, TurboJpegError};
use crate::v4l2::{Capture, CaptureError, FocusStatus};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const PRODUCER_POLL: Duration = Duration::from_millis(50);
const CONTROL_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamError {
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

impl StreamError {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
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

pub(crate) struct Frame {
    pub(crate) source_sequence: u64,
    pub(crate) host_monotonic_ns: u64,
    pub(crate) application_dropped_before: u64,
    pub(crate) left: Py<PyBytes>,
    pub(crate) right: Py<PyBytes>,
    pub(crate) raw_side_by_side: Py<PyBytes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FrameValidation {
    pub(crate) dropped_before: u64,
    pub(crate) queue_rejected: u64,
    pub(crate) source_gap: u64,
}

#[derive(Debug, Default)]
pub(crate) struct FrameValidator {
    last_source_sequence: Option<u64>,
    last_host_time: Option<u64>,
}

impl FrameValidator {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn reset(&mut self) {
        self.last_source_sequence = None;
        self.last_host_time = None;
    }

    pub(crate) fn validate(
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Open,
    Running,
    Stopped,
    Closed,
}

struct Lifecycle {
    state: State,
    resources: Option<Resources>,
    worker: Option<JoinHandle<Resources>>,
}

pub(crate) struct Stream {
    lifecycle: Mutex<Lifecycle>,
    stop: Arc<AtomicBool>,
    producer: Producer<Frame>,
    consumer: Consumer<Frame>,
    controls: Sender<ControlCommand>,
    terminal_error: Arc<Mutex<Option<StreamError>>>,
}

impl Stream {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open(
        device: &str,
        width: u32,
        height: u32,
        fps: u32,
        encoding: &str,
        buffer_count: u32,
        queue_capacity: usize,
        split_eyes: bool,
    ) -> Result<Self, StreamError> {
        if queue_capacity == 0 {
            return Err(StreamError::new(
                "invalid_argument",
                "native camera queue capacity must be positive",
            ));
        }
        let capture = Capture::open(device, width, height, fps, encoding, buffer_count)?;
        let splitter = if split_eyes {
            Some(TransformHandle::open()?)
        } else {
            None
        };
        let (producer, consumer) = bounded::channel(queue_capacity);
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
            stop: Arc::new(AtomicBool::new(false)),
            producer,
            consumer,
            controls: control_sender,
            terminal_error: Arc::new(Mutex::new(None)),
        })
    }

    pub(crate) fn start(&self) -> Result<(), StreamError> {
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
            return Err(error.into());
        }
        self.stop.store(false, Ordering::Release);
        self.consumer.reopen();
        *self.terminal_error.lock().unwrap() = None;
        let stop = Arc::clone(&self.stop);
        let producer = self.producer.clone();
        let terminal_error = Arc::clone(&self.terminal_error);
        lifecycle.worker = Some(thread::spawn(move || {
            run_producer(&mut resources, &producer, &stop, &terminal_error);
            resources
        }));
        lifecycle.state = State::Running;
        Ok(())
    }

    pub(crate) fn read(&self, timeout: Duration) -> Result<Frame, StreamError> {
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
                if let Some(error) = self.terminal_error.lock().unwrap().clone() {
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

    pub(crate) fn focus_status(&self) -> Result<Option<FocusStatus>, StreamError> {
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
                drop(lifecycle);
                let (response, result) = mpsc::channel();
                self.controls
                    .send(ControlCommand::FocusStatus(response))
                    .map_err(|_| {
                        StreamError::new(
                            "native_camera_control_failed",
                            "native camera control channel is closed",
                        )
                    })?;
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
            State::Closed => Err(StreamError::new("invalid_state", "native camera is closed")),
        }
    }

    pub(crate) fn set_focus(
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
                drop(lifecycle);
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
            State::Closed => Err(StreamError::new("invalid_state", "native camera is closed")),
        }
    }

    pub(crate) fn stop(&self) -> Result<(), StreamError> {
        let worker = {
            let mut lifecycle = self.lifecycle.lock().map_err(|_| {
                StreamError::new("native_camera_poisoned", "native camera mutex is poisoned")
            })?;
            match lifecycle.state {
                State::Open => {
                    lifecycle.state = State::Stopped;
                    return Ok(());
                }
                State::Stopped | State::Closed => return Ok(()),
                State::Running => {
                    self.stop.store(true, Ordering::Release);
                    lifecycle.worker.take()
                }
            }
        };
        let mut resources = worker
            .ok_or_else(|| StreamError::new("invalid_state", "native camera worker is missing"))?
            .join()
            .map_err(|_| {
                StreamError::new(
                    "native_camera_worker_failed",
                    "native camera worker panicked",
                )
            })?;
        let stop_result = resources.capture.stop().map_err(StreamError::from);
        let mut lifecycle = self.lifecycle.lock().map_err(|_| {
            resources.close();
            StreamError::new("native_camera_poisoned", "native camera mutex is poisoned")
        })?;
        lifecycle.resources = Some(resources);
        lifecycle.state = State::Stopped;
        *self.terminal_error.lock().unwrap() = Some(StreamError::new(
            "capture_stopped",
            "native camera capture stopped",
        ));
        self.consumer.close_and_clear();
        stop_result
    }

    pub(crate) fn close(&self) -> Result<(), StreamError> {
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
        stop_result
    }

    pub(crate) fn stats(&self) -> QueueStats {
        self.consumer.stats()
    }

    #[cfg(test)]
    pub(crate) fn test_idle(queue_capacity: usize) -> Self {
        let (producer, consumer) = bounded::channel(queue_capacity);
        let (control_sender, _control_receiver) = mpsc::channel();
        Self {
            lifecycle: Mutex::new(Lifecycle {
                state: State::Stopped,
                resources: None,
                worker: None,
            }),
            stop: Arc::new(AtomicBool::new(false)),
            producer,
            consumer,
            controls: control_sender,
            terminal_error: Arc::new(Mutex::new(None)),
        }
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn run_producer(
    resources: &mut Resources,
    producer: &Producer<Frame>,
    stop: &AtomicBool,
    terminal_error: &Mutex<Option<StreamError>>,
) {
    let mut pending_rejected = 0_u64;
    while !stop.load(Ordering::Acquire) {
        process_control_commands(resources);
        match resources.capture.wait(PRODUCER_POLL) {
            Ok(()) => {}
            Err(error) if error.code == "frame_timeout" => continue,
            Err(error) => {
                *terminal_error.lock().unwrap() = Some(error.into());
                return;
            }
        }
        let frame = Python::with_gil(|py| {
            let (source_sequence, host_monotonic_ns, raw_side_by_side) = resources
                .capture
                .read_ready_with(|source_sequence, host_monotonic_ns, payload| {
                    let raw = PyBytes::new_with(py, payload.len(), |target| {
                        target.copy_from_slice(payload);
                        Ok(())
                    })
                    .map(Bound::unbind)
                    .map_err(|error| {
                        CaptureError::new("native_allocation_failed", error.to_string())
                    })?;
                    Ok((source_sequence, host_monotonic_ns, raw))
                })?;
            let (left, right) = if let Some(splitter) = resources.splitter.as_mut() {
                let payload = raw_side_by_side.as_bytes(py);
                let (left, right) = py
                    .allow_threads(|| {
                        splitter.split_sbs(payload, resources.width, resources.height)
                    })
                    .map_err(|error| StreamError::new(error.code, error.message))?;
                (
                    PyBytes::new(py, &left).unbind(),
                    PyBytes::new(py, &right).unbind(),
                )
            } else {
                (
                    PyBytes::new(py, &[]).unbind(),
                    PyBytes::new(py, &[]).unbind(),
                )
            };
            Ok::<_, StreamError>(Frame {
                source_sequence,
                host_monotonic_ns,
                application_dropped_before: pending_rejected,
                left,
                right,
                raw_side_by_side,
            })
        });
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                resources.capture.close();
                *terminal_error.lock().unwrap() = Some(error);
                return;
            }
        };
        match producer.try_push(frame) {
            Ok(()) => pending_rejected = 0,
            Err(frame) => pending_rejected = frame.application_dropped_before + 1,
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
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => return,
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
}
