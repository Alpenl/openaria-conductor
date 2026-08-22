mod active_take;
mod audio;
mod bounded;
mod capture_runtime;
mod frame_stream;
mod imu;
mod jpeg;
mod metrics;
mod native_camera;
mod preview;
mod recording;
mod session_io;
mod stereo_encoder;
mod timeline;
mod turbojpeg;
mod v4l2;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyBytes, PyDict, PyList, PySequence, PySequenceMethods};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const NATIVE_ABI: u32 = 4;
const CAPABILITY_PROBE: &str = "capability_probe";
const JPEG_CONTRACT: &str = "jpeg_contract";
const FRAME_STREAM: &str = "frame_stream";
const TURBOJPEG_SPLIT: &str = "turbojpeg_split";
const V4L2_CAPTURE: &str = "v4l2_capture";
const V4L2_FOCUS_CONTROL: &str = "v4l2_focus_control";
const NATIVE_CAMERA: &str = "native_camera";
const CAMERA_FRAME_VALIDATOR: &str = "camera_frame_validator";
const NATIVE_AUDIO: &str = "native_audio";
const NATIVE_TIMELINE: &str = "native_timeline";
const ACTIVE_TAKE_WRITER: &str = "active_take_writer";
const NATIVE_IMU: &str = "native_imu";
const RECORDING_CODEC: &str = "recording_codec";
const RECORDING_SINK: &str = "recording_sink";
const RECORDING_IMU_BATCH: &str = "recording_imu_batch";
const RECORDING_FRAME_GATE: &str = "recording_frame_gate";
const RECORDING_TAP_STATE: &str = "recording_tap_state";
const CAPTURE_FANOUT: &str = "capture_fanout";
const CONTINUOUS_CAPTURE_RUNTIME: &str = "continuous_capture_runtime";
const CONTINUOUS_CAPTURE_RAW_SINK: &str = "continuous_capture_raw_sink";
const CONTINUOUS_CAPTURE_SPLIT_SINK: &str = "continuous_capture_split_sink";
const RECORDING_SEGMENT_PLANNER: &str = "recording_segment_planner";
const RECORDING_EVENT_QUEUE: &str = "recording_event_queue";
const ARTIFACT_FINALIZE: &str = "artifact_finalize";
const RANGE_PARSER: &str = "range_parser";
const STEREO_ENCODER_EVENTS: &str = "stereo_encoder_events";
const STEREO_ENCODER_PIPE: &str = "stereo_encoder_pipe";
const STEREO_ENCODER_PROCESS: &str = "stereo_encoder_process";
const SESSION_IO: &str = "session_io";
const DEVICE_SESSION_ARTIFACTS: &str = "device_session_artifacts";
const DEVICE_SESSION_FINALIZER: &str = "device_session_finalizer";
const DROP_QUALITY_POLICY: &str = "drop_quality_policy";
const PREVIEW_BUFFER: &str = "preview_buffer";
const PERFORMANCE_METRICS: &str = "performance_metrics";

fn native_error(error: turbojpeg::TurboJpegError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("{}: {}", error.code, error.message))
}

fn camera_error(error: native_camera::StreamError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("{}: {}", error.code, error.message))
}

fn v4l2_error(error: v4l2::CaptureError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("{}: {}", error.code, error.message))
}

fn audio_error(error: audio::AudioError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("{}: {}", error.code, error.message))
}

fn timeline_error(error: timeline::TimelineError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("{}: {}", error.code, error.message))
}

fn active_take_error(error: active_take::ActiveTakeError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("{}: {}", error.code, error.message))
}

fn imu_error(error: imu::ImuError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("{}: {}", error.code, error.message))
}

fn recording_error(error: recording::RecordingError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("{}: {}", error.code, error.message))
}

fn session_io_error(error: session_io::SessionIoError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("{}: {}", error.code, error.message))
}

fn stereo_encoder_event_error(error: stereo_encoder::EncoderEventError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("{}: {}", error.code, error.message))
}

fn stereo_encoder_process_error(error: stereo_encoder::EncoderProcessError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("{}: {}", error.code, error.message))
}

fn stereo_encoder_process_mutex_error() -> stereo_encoder::EncoderProcessError {
    stereo_encoder::EncoderProcessError {
        code: "encoder_failed".to_owned(),
        message: "encoder process mutex is poisoned".to_owned(),
    }
}

fn preview_error(error: preview::PreviewError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("{}: {}", error.code, error.message))
}

fn capture_runtime_error(error: capture_runtime::RuntimeError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("{}: {}", error.code, error.message))
}

fn metrics_error(error: metrics::MetricsError) -> PyErr {
    let message = format!("{}: {}", error.code, error.message);
    if error.code == "invalid_argument" {
        pyo3::exceptions::PyValueError::new_err(message)
    } else {
        pyo3::exceptions::PyRuntimeError::new_err(message)
    }
}

type PythonCameraFrame<'py> = (
    u64,
    u64,
    u64,
    Bound<'py, PyBytes>,
    Bound<'py, PyBytes>,
    Bound<'py, PyBytes>,
);

#[pyclass]
struct NativeCameraStream {
    stream: Arc<native_camera::Stream>,
    delivery: Arc<Mutex<()>>,
}

#[pymethods]
impl NativeCameraStream {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (device, width, height, fps, encoding="mjpg", buffer_count=4, queue_capacity=4, split_eyes=true))]
    fn new(
        device: &str,
        width: u32,
        height: u32,
        fps: u32,
        encoding: &str,
        buffer_count: u32,
        queue_capacity: usize,
        split_eyes: bool,
    ) -> PyResult<Self> {
        Ok(Self {
            stream: Arc::new(
                native_camera::Stream::open(
                    device,
                    width,
                    height,
                    fps,
                    encoding,
                    buffer_count,
                    queue_capacity,
                    split_eyes,
                )
                .map_err(camera_error)?,
            ),
            delivery: Arc::new(Mutex::new(())),
        })
    }

    fn start(&self) -> PyResult<()> {
        self.stream.start().map_err(camera_error)
    }

    fn read<'py>(&self, py: Python<'py>, timeout_seconds: f64) -> PyResult<PythonCameraFrame<'py>> {
        if !timeout_seconds.is_finite() || timeout_seconds <= 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: timeout must be finite and positive",
            ));
        }
        let timeout = Duration::from_secs_f64(timeout_seconds);
        let stream = Arc::clone(&self.stream);
        let delivery = Arc::clone(&self.delivery);
        let frame = py
            .allow_threads(move || {
                let _delivery = delivery.lock().map_err(|_| {
                    native_camera::StreamError::new(
                        "native_camera_poisoned",
                        "native camera delivery mutex is poisoned",
                    )
                })?;
                stream.read(timeout)
            })
            .map_err(camera_error)?;
        Ok((
            frame.source_sequence,
            frame.host_monotonic_ns,
            frame.application_dropped_before,
            frame.left.into_bound(py),
            frame.right.into_bound(py),
            frame.raw_side_by_side.into_bound(py),
        ))
    }

    fn stop(&self, py: Python<'_>) -> PyResult<()> {
        let stream = Arc::clone(&self.stream);
        py.allow_threads(move || stream.stop())
            .map_err(camera_error)
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let stream = Arc::clone(&self.stream);
        py.allow_threads(move || stream.close())
            .map_err(camera_error)
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let stats = self.stream.stats();
        let result = PyDict::new(py);
        result.set_item("capacity", stats.capacity)?;
        result.set_item("depth", stats.depth)?;
        result.set_item("peak_depth", stats.peak_depth)?;
        result.set_item("enqueued", stats.enqueued)?;
        result.set_item("delivered", stats.delivered)?;
        result.set_item("rejected", stats.rejected)?;
        Ok(result.unbind())
    }

    fn camera_focus_status(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let stream = Arc::clone(&self.stream);
        py.allow_threads(move || stream.focus_status())
            .map_err(camera_error)?
            .map(|status| focus_status_dict(py, status))
            .transpose()
    }

    #[pyo3(signature = (value=None, auto_enabled=None))]
    fn set_camera_focus(
        &self,
        py: Python<'_>,
        value: Option<i32>,
        auto_enabled: Option<bool>,
    ) -> PyResult<Py<PyDict>> {
        if value.is_none() && auto_enabled.is_none() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_camera_focus: value or auto_enabled is required",
            ));
        }
        let stream = Arc::clone(&self.stream);
        let status = py
            .allow_threads(move || stream.set_focus(value, auto_enabled))
            .map_err(camera_error)?;
        focus_status_dict(py, status)
    }
}

impl Drop for NativeCameraStream {
    fn drop(&mut self) {
        let stream = Arc::clone(&self.stream);
        Python::with_gil(|py| {
            let _ = py.allow_threads(move || stream.close());
        });
    }
}

#[pyclass]
struct NativeCameraFrameValidator {
    validator: Mutex<native_camera::FrameValidator>,
}

#[pymethods]
impl NativeCameraFrameValidator {
    #[new]
    fn new() -> Self {
        Self {
            validator: Mutex::new(native_camera::FrameValidator::new()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_frame(
        &self,
        py: Python<'_>,
        source_sequence: i64,
        host_monotonic_ns: i64,
        valid: bool,
        has_left: bool,
        has_right: bool,
        has_raw_side_by_side: bool,
        application_dropped_before: i64,
    ) -> PyResult<Py<PyDict>> {
        let has_payload = (has_left && has_right) || has_raw_side_by_side;
        let validation = {
            let mut validator = self.validator.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "native_camera_frame_validator_poisoned: camera frame validator mutex is poisoned",
                )
            })?;
            validator
                .validate(
                    source_sequence,
                    host_monotonic_ns,
                    valid,
                    has_payload,
                    application_dropped_before,
                )
                .map_err(camera_error)?
        };
        camera_frame_validation_dict(py, &validation)
    }

    fn reset(&self) -> PyResult<()> {
        let mut validator = self.validator.lock().map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "native_camera_frame_validator_poisoned: camera frame validator mutex is poisoned",
            )
        })?;
        validator.reset();
        Ok(())
    }
}

#[pyclass]
struct NativeAudioRecorder {
    recorder: Arc<audio::Recorder>,
}

#[pymethods]
impl NativeAudioRecorder {
    #[new]
    #[pyo3(signature = (session_root, device="hw:0,0", sample_rate_hz=48000, channels=2, segment_seconds=30.0))]
    fn new(
        session_root: &str,
        device: &str,
        sample_rate_hz: u32,
        channels: u16,
        segment_seconds: f64,
    ) -> PyResult<Self> {
        Ok(Self {
            recorder: Arc::new(
                audio::Recorder::new(
                    session_root,
                    device,
                    sample_rate_hz,
                    channels,
                    segment_seconds,
                )
                .map_err(audio_error)?,
            ),
        })
    }

    fn start(&self, py: Python<'_>) -> PyResult<()> {
        let recorder = Arc::clone(&self.recorder);
        py.allow_threads(move || recorder.start())
            .map_err(audio_error)
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let snapshot = self.recorder.snapshot();
        audio_snapshot_dict(py, &snapshot)
    }

    #[pyo3(signature = (timeout_seconds=5.0))]
    fn stop(&self, py: Python<'_>, timeout_seconds: f64) -> PyResult<Py<PyDict>> {
        if !timeout_seconds.is_finite() || timeout_seconds <= 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: timeout must be finite and positive",
            ));
        }
        let timeout = Duration::from_secs_f64(timeout_seconds);
        let recorder = Arc::clone(&self.recorder);
        let result = py
            .allow_threads(move || recorder.stop(timeout))
            .map_err(audio_error)?;
        audio_result_dict(py, &result)
    }

    fn abort(&self, py: Python<'_>) {
        let recorder = Arc::clone(&self.recorder);
        py.allow_threads(move || recorder.abort());
    }

    fn close(&self, py: Python<'_>) {
        let recorder = Arc::clone(&self.recorder);
        py.allow_threads(move || recorder.close());
    }
}

impl Drop for NativeAudioRecorder {
    fn drop(&mut self) {
        let recorder = Arc::clone(&self.recorder);
        Python::with_gil(|py| {
            py.allow_threads(move || recorder.close());
        });
    }
}

#[pyclass]
struct NativeTimeline {
    timeline: timeline::Timeline,
}

#[pymethods]
impl NativeTimeline {
    #[new]
    #[pyo3(signature = (start_monotonic_ns=None))]
    fn new(start_monotonic_ns: Option<u64>) -> PyResult<Self> {
        let timeline = match start_monotonic_ns {
            Some(value) => timeline::Timeline::new(value),
            None => timeline::Timeline::start_now(),
        }
        .map_err(timeline_error)?;
        Ok(Self { timeline })
    }

    #[staticmethod]
    fn now_monotonic_ns(py: Python<'_>) -> PyResult<u64> {
        py.allow_threads(timeline::monotonic_ns)
            .map_err(timeline_error)
    }

    fn start_monotonic_ns(&self) -> u64 {
        self.timeline.start_monotonic_ns()
    }

    fn elapsed_ns(&self, py: Python<'_>) -> PyResult<u64> {
        let timeline = self.timeline.clone();
        py.allow_threads(move || timeline.elapsed_ns())
            .map_err(timeline_error)
    }

    fn elapsed_seconds(&self, py: Python<'_>) -> PyResult<f64> {
        let timeline = self.timeline.clone();
        py.allow_threads(move || timeline.elapsed_seconds())
            .map_err(timeline_error)
    }

    fn offset_ns(&self, monotonic_ns: u64) -> i128 {
        self.timeline.offset_ns(monotonic_ns)
    }

    fn offset_seconds(&self, monotonic_ns: u64) -> f64 {
        self.timeline.offset_seconds(monotonic_ns)
    }

    fn audio_sync(
        &self,
        py: Python<'_>,
        started_monotonic_ns: u64,
        stopped_monotonic_ns: u64,
        sample_rate_hz: u32,
    ) -> PyResult<Py<PyDict>> {
        let timeline = self.timeline.clone();
        let sync = py
            .allow_threads(move || {
                timeline.audio_sync(started_monotonic_ns, stopped_monotonic_ns, sample_rate_hz)
            })
            .map_err(timeline_error)?;
        timeline_audio_sync_dict(py, &sync)
    }
}

#[pyclass]
struct NativeActiveTakeWriter {
    session_id: String,
    writer: Arc<Mutex<active_take::ActiveTakeWriter>>,
}

#[pymethods]
impl NativeActiveTakeWriter {
    #[new]
    fn new(session_id: &str) -> PyResult<Self> {
        Ok(Self {
            session_id: session_id.to_owned(),
            writer: Arc::new(Mutex::new(
                active_take::ActiveTakeWriter::new(session_id).map_err(active_take_error)?,
            )),
        })
    }

    fn reserve_frame(
        &self,
        py: Python<'_>,
        source_sequence: u64,
        host_monotonic_ns: u64,
        source_gap: u64,
    ) -> PyResult<Py<PyDict>> {
        let frame = {
            let mut writer = self.writer.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "active_take_writer_poisoned: active take writer mutex is poisoned",
                )
            })?;
            writer
                .reserve_frame(active_take::ActiveSourceFrame {
                    source_sequence,
                    host_monotonic_ns,
                    source_gap,
                })
                .map_err(active_take_error)?
        };
        active_take_reserved_frame_dict(py, &frame)
    }

    fn raw_write_decision(
        &self,
        py: Python<'_>,
        record_sequence: u64,
        source_sequence: u64,
        host_monotonic_ns: u64,
    ) -> PyResult<Py<PyDict>> {
        let frame = self.reserved_frame(record_sequence, source_sequence, host_monotonic_ns);
        let decision = {
            let writer = self.writer.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "active_take_writer_poisoned: active take writer mutex is poisoned",
                )
            })?;
            writer
                .raw_write_decision(&frame)
                .map_err(active_take_error)?
        };
        active_take_write_decision_dict(py, &decision)
    }

    fn split_write_decision(
        &self,
        py: Python<'_>,
        record_sequence: u64,
        source_sequence: u64,
        host_monotonic_ns: u64,
        segment_index: u64,
        segment_frame: u64,
    ) -> PyResult<Py<PyDict>> {
        let frame = self.reserved_frame(record_sequence, source_sequence, host_monotonic_ns);
        let decision = {
            let writer = self.writer.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "active_take_writer_poisoned: active take writer mutex is poisoned",
                )
            })?;
            writer
                .split_write_decision(
                    &frame,
                    active_take::SplitFrameLocation {
                        segment_index,
                        segment_frame,
                    },
                )
                .map_err(active_take_error)?
        };
        active_take_write_decision_dict(py, &decision)
    }

    fn finish_frame(
        &self,
        py: Python<'_>,
        record_sequence: u64,
        source_sequence: u64,
        host_monotonic_ns: u64,
        bytes_written: u64,
    ) -> PyResult<Py<PyDict>> {
        let frame = self.reserved_frame(record_sequence, source_sequence, host_monotonic_ns);
        let snapshot = {
            let mut writer = self.writer.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "active_take_writer_poisoned: active take writer mutex is poisoned",
                )
            })?;
            writer
                .finish_frame(frame, bytes_written)
                .map_err(active_take_error)?
        };
        active_take_snapshot_dict(py, &snapshot)
    }

    fn reject_frame(
        &self,
        py: Python<'_>,
        record_sequence: u64,
        source_sequence: u64,
        host_monotonic_ns: u64,
        at_time_seconds: f64,
    ) -> PyResult<Py<PyDict>> {
        let frame = self.reserved_frame(record_sequence, source_sequence, host_monotonic_ns);
        let snapshot = {
            let mut writer = self.writer.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "active_take_writer_poisoned: active take writer mutex is poisoned",
                )
            })?;
            writer
                .reject_frame(frame, at_time_seconds)
                .map_err(active_take_error)?
        };
        active_take_snapshot_dict(py, &snapshot)
    }

    fn finish(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let summary = {
            let mut writer = self.writer.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "active_take_writer_poisoned: active take writer mutex is poisoned",
                )
            })?;
            writer.finish().map_err(active_take_error)?
        };
        active_take_snapshot_dict(py, &summary)
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let snapshot = {
            let writer = self.writer.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "active_take_writer_poisoned: active take writer mutex is poisoned",
                )
            })?;
            writer.snapshot()
        };
        active_take_snapshot_dict(py, &snapshot)
    }
}

impl NativeActiveTakeWriter {
    fn reserved_frame(
        &self,
        record_sequence: u64,
        source_sequence: u64,
        host_monotonic_ns: u64,
    ) -> active_take::ReservedFrame {
        active_take::ReservedFrame {
            session_id: self.session_id.clone(),
            record_sequence,
            source_sequence,
            host_monotonic_ns,
        }
    }
}

#[pyclass]
struct NativeImuCollector {
    collector: Arc<imu::Collector>,
}

#[pymethods]
impl NativeImuCollector {
    #[new]
    #[pyo3(signature = (device, unit=None, selector=1, stale_poll_interval=0.001))]
    fn new(
        device: &str,
        unit: Option<u8>,
        selector: u8,
        stale_poll_interval: f64,
    ) -> PyResult<Self> {
        if !stale_poll_interval.is_finite() || stale_poll_interval < 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: stale_poll_interval must be finite and non-negative",
            ));
        }
        Ok(Self {
            collector: Arc::new(
                imu::Collector::open(
                    device,
                    unit,
                    selector,
                    Some(Duration::from_secs_f64(stale_poll_interval)),
                )
                .map_err(imu_error)?,
            ),
        })
    }

    #[pyo3(signature = (timeout_seconds=1.0))]
    fn read(&self, py: Python<'_>, timeout_seconds: f64) -> PyResult<Py<PyDict>> {
        if !timeout_seconds.is_finite() || timeout_seconds <= 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: timeout must be finite and positive",
            ));
        }
        let timeout = Duration::from_secs_f64(timeout_seconds);
        let collector = Arc::clone(&self.collector);
        let observation = py
            .allow_threads(move || collector.read(timeout))
            .map_err(imu_error)?;
        imu::observation_dict(py, &observation)
    }

    fn close(&self, py: Python<'_>) {
        let collector = Arc::clone(&self.collector);
        py.allow_threads(move || collector.close());
    }

    fn unit(&self) -> PyResult<u8> {
        self.collector.unit().map_err(imu_error)
    }

    fn latest_observation(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        match self.collector.latest_observation().map_err(imu_error)? {
            Some(observation) => imu::observation_dict(py, &observation).map(Some),
            None => Ok(None),
        }
    }
}

impl Drop for NativeImuCollector {
    fn drop(&mut self) {
        let collector = Arc::clone(&self.collector);
        Python::with_gil(|py| {
            py.allow_threads(move || collector.close());
        });
    }
}

#[pyclass]
struct NativeRecordingCodec;

#[pymethods]
impl NativeRecordingCodec {
    #[new]
    fn new() -> Self {
        Self
    }

    fn jpeg_payload<'py>(&self, py: Python<'py>, payload: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
        let payload = recording::jpeg_payload(payload).map_err(recording_error)?;
        Ok(PyBytes::new(py, payload))
    }

    #[allow(clippy::too_many_arguments)]
    fn encode_split_frame_index<'py>(
        &self,
        py: Python<'py>,
        session_id: &str,
        frame: u64,
        source_sequence: u64,
        host_monotonic_ns: u64,
        segment_index: u64,
        segment_frame: u64,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let payload = recording::split_frame_index_record(
            session_id,
            frame,
            source_sequence,
            host_monotonic_ns,
            segment_index,
            segment_frame,
        );
        Ok(PyBytes::new(py, &payload))
    }

    #[allow(clippy::too_many_arguments)]
    fn encode_raw_frame_index<'py>(
        &self,
        py: Python<'py>,
        session_id: &str,
        frame: u64,
        source_sequence: u64,
        host_monotonic_ns: u64,
        video_offset: u64,
        video_bytes: u64,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let payload = recording::raw_frame_index_record(
            session_id,
            frame,
            source_sequence,
            host_monotonic_ns,
            video_offset,
            video_bytes,
        );
        Ok(PyBytes::new(py, &payload))
    }

    #[allow(clippy::too_many_arguments)]
    fn encode_imu_sample<'py>(
        &self,
        py: Python<'py>,
        session_id: &str,
        sequence: u64,
        packet_sequence: u64,
        sample_index: u8,
        device_timestamp_raw: u32,
        device_ticks: u64,
        host_read_start_ns: u64,
        host_read_end_ns: u64,
        host_monotonic_ns: u64,
        accelerometer: (i16, i16, i16),
        gyroscope: (i16, i16, i16),
        sync_offset_ns: Option<i64>,
        sync_residual_ns: Option<u64>,
        sync_quality: &str,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let payload = recording::imu_sample_record(
            session_id,
            sequence,
            packet_sequence,
            sample_index,
            device_timestamp_raw,
            device_ticks,
            host_read_start_ns,
            host_read_end_ns,
            host_monotonic_ns,
            accelerometer,
            gyroscope,
            sync_offset_ns,
            sync_residual_ns,
            sync_quality,
        );
        Ok(PyBytes::new(py, &payload))
    }
}

#[derive(Debug)]
struct PyImuSampleRecord {
    sequence: u64,
    packet_sequence: u64,
    sample_index: u8,
    device_timestamp_raw: u32,
    device_ticks: u64,
    host_read_start_ns: u64,
    host_read_end_ns: u64,
    host_monotonic_ns: u64,
    accelerometer: (i16, i16, i16),
    gyroscope: (i16, i16, i16),
    sync_offset_ns: Option<i64>,
    sync_residual_ns: Option<u64>,
    sync_quality: String,
}

fn py_axis3(value: &Bound<'_, PyAny>, field: &str) -> PyResult<(i16, i16, i16)> {
    let vector = value.getattr(field)?;
    Ok((
        vector.getattr("x")?.extract::<i16>()?,
        vector.getattr("y")?.extract::<i16>()?,
        vector.getattr("z")?.extract::<i16>()?,
    ))
}

fn py_imu_sample(value: &Bound<'_, PyAny>) -> PyResult<PyImuSampleRecord> {
    Ok(PyImuSampleRecord {
        sequence: value.getattr("sequence")?.extract::<u64>()?,
        packet_sequence: value.getattr("packet_sequence")?.extract::<u64>()?,
        sample_index: value.getattr("sample_index")?.extract::<u8>()?,
        device_timestamp_raw: value.getattr("device_timestamp_raw")?.extract::<u32>()?,
        device_ticks: value.getattr("device_ticks")?.extract::<u64>()?,
        host_read_start_ns: value.getattr("host_read_start_ns")?.extract::<u64>()?,
        host_read_end_ns: value.getattr("host_read_end_ns")?.extract::<u64>()?,
        host_monotonic_ns: value.getattr("host_monotonic_ns")?.extract::<u64>()?,
        accelerometer: py_axis3(value, "accelerometer")?,
        gyroscope: py_axis3(value, "gyroscope")?,
        sync_offset_ns: value.getattr("sync_offset_ns")?.extract::<Option<i64>>()?,
        sync_residual_ns: value
            .getattr("sync_residual_ns")?
            .extract::<Option<u64>>()?,
        sync_quality: value.getattr("sync_quality")?.extract::<String>()?,
    })
}

#[pyclass]
struct NativeRecordingSink {
    sink: Arc<Mutex<recording::RecordingSink>>,
}

#[pymethods]
impl NativeRecordingSink {
    #[new]
    fn new(session_root: &str, session_id: &str, split_eyes: bool) -> PyResult<Self> {
        Ok(Self {
            sink: Arc::new(Mutex::new(
                recording::RecordingSink::create(
                    std::path::Path::new(session_root),
                    session_id,
                    split_eyes,
                )
                .map_err(recording_error)?,
            )),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn write_split_frame_index(
        &self,
        frame: u64,
        source_sequence: u64,
        host_monotonic_ns: u64,
        segment_index: u64,
        segment_frame: u64,
    ) -> PyResult<u64> {
        let mut sink = self.sink.lock().map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "native_recording_poisoned: recording sink mutex is poisoned",
            )
        })?;
        sink.write_split_frame_index(
            frame,
            source_sequence,
            host_monotonic_ns,
            segment_index,
            segment_frame,
        )
        .map_err(recording_error)
    }

    fn write_raw_frame(
        &self,
        py: Python<'_>,
        frame: u64,
        source_sequence: u64,
        host_monotonic_ns: u64,
        raw_side_by_side: &[u8],
    ) -> PyResult<Py<PyDict>> {
        let result = {
            let mut sink = self.sink.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "native_recording_poisoned: recording sink mutex is poisoned",
                )
            })?;
            sink.write_raw_frame(frame, source_sequence, host_monotonic_ns, raw_side_by_side)
                .map_err(recording_error)?
        };
        let value = PyDict::new(py);
        value.set_item("bytes_written", result.bytes_written)?;
        value.set_item("video_offset", result.video_offset)?;
        value.set_item("video_bytes", result.video_bytes)?;
        Ok(value.unbind())
    }

    #[allow(clippy::too_many_arguments)]
    fn write_imu_sample(
        &self,
        sequence: u64,
        packet_sequence: u64,
        sample_index: u8,
        device_timestamp_raw: u32,
        device_ticks: u64,
        host_read_start_ns: u64,
        host_read_end_ns: u64,
        host_monotonic_ns: u64,
        accelerometer: (i16, i16, i16),
        gyroscope: (i16, i16, i16),
        sync_offset_ns: Option<i64>,
        sync_residual_ns: Option<u64>,
        sync_quality: &str,
    ) -> PyResult<u64> {
        let mut sink = self.sink.lock().map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "native_recording_poisoned: recording sink mutex is poisoned",
            )
        })?;
        sink.write_imu_sample(
            sequence,
            packet_sequence,
            sample_index,
            device_timestamp_raw,
            device_ticks,
            host_read_start_ns,
            host_read_end_ns,
            host_monotonic_ns,
            accelerometer,
            gyroscope,
            sync_offset_ns,
            sync_residual_ns,
            sync_quality,
        )
        .map_err(recording_error)
    }

    fn write_imu_observation(
        &self,
        py: Python<'_>,
        observation: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyDict>> {
        let samples_object = observation.getattr("samples")?;
        let samples = samples_object.downcast::<PySequence>()?;
        let sample_count = samples.len()?;
        let mut records = Vec::with_capacity(sample_count);
        for index in 0..sample_count {
            let sample = samples.get_item(index)?;
            records.push(py_imu_sample(&sample)?);
        }

        let (bytes_written, samples_written) = {
            let mut sink = self.sink.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "native_recording_poisoned: recording sink mutex is poisoned",
                )
            })?;
            let mut bytes_written = 0_u64;
            let mut samples_written = 0_u64;
            for sample in records {
                let written = sink
                    .write_imu_sample(
                        sample.sequence,
                        sample.packet_sequence,
                        sample.sample_index,
                        sample.device_timestamp_raw,
                        sample.device_ticks,
                        sample.host_read_start_ns,
                        sample.host_read_end_ns,
                        sample.host_monotonic_ns,
                        sample.accelerometer,
                        sample.gyroscope,
                        sample.sync_offset_ns,
                        sample.sync_residual_ns,
                        &sample.sync_quality,
                    )
                    .map_err(recording_error)?;
                bytes_written = bytes_written.checked_add(written).ok_or_else(|| {
                    pyo3::exceptions::PyOverflowError::new_err(
                        "native_recording_overflow: IMU byte count overflowed",
                    )
                })?;
                samples_written = samples_written.checked_add(1).ok_or_else(|| {
                    pyo3::exceptions::PyOverflowError::new_err(
                        "native_recording_overflow: IMU sample count overflowed",
                    )
                })?;
            }
            (bytes_written, samples_written)
        };
        let value = PyDict::new(py);
        value.set_item("bytes_written", bytes_written)?;
        value.set_item("samples_written", samples_written)?;
        Ok(value.unbind())
    }

    fn flush_and_close(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let snapshot = {
            let mut sink = self.sink.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "native_recording_poisoned: recording sink mutex is poisoned",
                )
            })?;
            sink.flush_and_close().map_err(recording_error)?
        };
        recording_sink_snapshot_dict(py, &snapshot)
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let snapshot = {
            let sink = self.sink.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "native_recording_poisoned: recording sink mutex is poisoned",
                )
            })?;
            sink.snapshot()
        };
        recording_sink_snapshot_dict(py, &snapshot)
    }

    fn close(&self) {
        if let Ok(mut sink) = self.sink.lock() {
            sink.close();
        }
    }
}

impl Drop for NativeRecordingSink {
    fn drop(&mut self) {
        if let Ok(mut sink) = self.sink.lock() {
            sink.close();
        }
    }
}

#[pyclass]
struct NativeRecordingFrameGate {
    gate: Mutex<recording::RecordingFrameGate>,
}

#[pymethods]
impl NativeRecordingFrameGate {
    #[new]
    fn new(frame_decimation: u64) -> PyResult<Self> {
        Ok(Self {
            gate: Mutex::new(
                recording::RecordingFrameGate::new(frame_decimation).map_err(recording_error)?,
            ),
        })
    }

    fn begin_frame(&self, py: Python<'_>, dropped_before: u64) -> PyResult<Py<PyDict>> {
        let decision = {
            let mut gate = self.gate.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "native_recording_frame_gate_poisoned: frame gate mutex is poisoned",
                )
            })?;
            gate.begin_frame(dropped_before).map_err(recording_error)?
        };
        recording_frame_gate_decision_dict(py, &decision)
    }

    fn finish_frame(&self) -> PyResult<u64> {
        let mut gate = self.gate.lock().map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "native_recording_frame_gate_poisoned: frame gate mutex is poisoned",
            )
        })?;
        gate.finish_frame().map_err(recording_error)
    }

    fn start_stopping(&self) -> PyResult<u64> {
        let mut gate = self.gate.lock().map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "native_recording_frame_gate_poisoned: frame gate mutex is poisoned",
            )
        })?;
        Ok(gate.start_stopping())
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let snapshot = {
            let gate = self.gate.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "native_recording_frame_gate_poisoned: frame gate mutex is poisoned",
                )
            })?;
            gate.snapshot()
        };
        recording_frame_gate_snapshot_dict(py, &snapshot)
    }
}

#[pyclass]
struct NativeRecordingTapState {
    tap: Mutex<recording::RecordingTapState>,
}

#[pymethods]
impl NativeRecordingTapState {
    #[new]
    fn new(frame_decimation: u64) -> PyResult<Self> {
        Ok(Self {
            tap: Mutex::new(
                recording::RecordingTapState::new(frame_decimation).map_err(recording_error)?,
            ),
        })
    }

    fn begin_frame(&self, py: Python<'_>, dropped_before: u64) -> PyResult<Py<PyDict>> {
        let decision = {
            let mut tap = self.tap.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "native_recording_tap_state_poisoned: tap state mutex is poisoned",
                )
            })?;
            tap.begin_frame(dropped_before).map_err(recording_error)?
        };
        recording_frame_gate_decision_dict(py, &decision)
    }

    fn finish_frame(&self) -> PyResult<u64> {
        let mut tap = self.tap.lock().map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "native_recording_tap_state_poisoned: tap state mutex is poisoned",
            )
        })?;
        tap.finish_frame().map_err(recording_error)
    }

    fn start_stopping(&self) -> PyResult<u64> {
        let mut tap = self.tap.lock().map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "native_recording_tap_state_poisoned: tap state mutex is poisoned",
            )
        })?;
        Ok(tap.start_stopping())
    }

    fn mark_failure(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let (should_report, inflight_frames) = {
            let mut tap = self.tap.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "native_recording_tap_state_poisoned: tap state mutex is poisoned",
                )
            })?;
            tap.mark_failure()
        };
        let value = PyDict::new(py);
        value.set_item("should_report", should_report)?;
        value.set_item("inflight_frames", inflight_frames)?;
        Ok(value.unbind())
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let snapshot = {
            let tap = self.tap.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "native_recording_tap_state_poisoned: tap state mutex is poisoned",
                )
            })?;
            tap.snapshot()
        };
        recording_tap_snapshot_dict(py, &snapshot)
    }
}

#[pyclass]
struct NativeCaptureFanoutState {
    fanout: Mutex<recording::CaptureFanoutState>,
}

#[pymethods]
impl NativeCaptureFanoutState {
    #[new]
    fn new(frame_decimation: u64) -> PyResult<Self> {
        Ok(Self {
            fanout: Mutex::new(
                recording::CaptureFanoutState::new(frame_decimation).map_err(recording_error)?,
            ),
        })
    }

    fn start_recording(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let snapshot = {
            let mut fanout = self.fanout.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "native_capture_fanout_poisoned: capture fanout mutex is poisoned",
                )
            })?;
            fanout.start_recording().map_err(recording_error)?
        };
        capture_fanout_snapshot_dict(py, &snapshot)
    }

    fn begin_frame(
        &self,
        py: Python<'_>,
        dropped_before: u64,
        has_preview: bool,
    ) -> PyResult<Py<PyDict>> {
        let decision = {
            let mut fanout = self.fanout.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "native_capture_fanout_poisoned: capture fanout mutex is poisoned",
                )
            })?;
            fanout
                .begin_frame(dropped_before, has_preview)
                .map_err(recording_error)?
        };
        capture_fanout_decision_dict(py, &decision)
    }

    fn finish_frame(&self) -> PyResult<u64> {
        let mut fanout = self.fanout.lock().map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "native_capture_fanout_poisoned: capture fanout mutex is poisoned",
            )
        })?;
        fanout.finish_frame().map_err(recording_error)
    }

    fn start_stopping(&self) -> PyResult<u64> {
        let mut fanout = self.fanout.lock().map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "native_capture_fanout_poisoned: capture fanout mutex is poisoned",
            )
        })?;
        Ok(fanout.start_stopping())
    }

    fn mark_failure(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let (should_report, inflight_frames) = {
            let mut fanout = self.fanout.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "native_capture_fanout_poisoned: capture fanout mutex is poisoned",
                )
            })?;
            fanout.mark_failure()
        };
        let value = PyDict::new(py);
        value.set_item("should_report", should_report)?;
        value.set_item("inflight_frames", inflight_frames)?;
        Ok(value.unbind())
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let snapshot = {
            let fanout = self.fanout.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "native_capture_fanout_poisoned: capture fanout mutex is poisoned",
                )
            })?;
            fanout.snapshot()
        };
        capture_fanout_snapshot_dict(py, &snapshot)
    }
}

#[pyclass]
struct NativeContinuousCaptureRuntime {
    runtime: Arc<capture_runtime::Runtime>,
}

#[pymethods]
impl NativeContinuousCaptureRuntime {
    #[new]
    #[pyo3(signature = (camera, preview, frame_decimation, read_timeout_seconds=2.0, metrics=None))]
    fn new(
        camera: PyRef<'_, NativeCameraStream>,
        preview: PyRef<'_, NativePreviewBuffer>,
        frame_decimation: u64,
        read_timeout_seconds: f64,
        metrics: Option<PyRef<'_, NativePerformanceMetrics>>,
    ) -> PyResult<Self> {
        if !read_timeout_seconds.is_finite() || read_timeout_seconds <= 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: read_timeout_seconds must be finite and positive",
            ));
        }
        let metrics = metrics.as_ref().map(|metrics| Arc::clone(&metrics.metrics));
        Ok(Self {
            runtime: Arc::new(
                capture_runtime::Runtime::new(
                    Arc::clone(&camera.stream),
                    Arc::clone(&preview.buffer),
                    frame_decimation,
                    Duration::from_secs_f64(read_timeout_seconds),
                    metrics,
                )
                .map_err(capture_runtime_error)?,
            ),
        })
    }

    fn start_preview(&self, py: Python<'_>) -> PyResult<()> {
        let runtime = Arc::clone(&self.runtime);
        py.allow_threads(move || runtime.start_preview())
            .map_err(capture_runtime_error)
    }

    #[pyo3(signature = (submit_frame, on_failure, imu=None, submit_imu=None, imu_timeout_seconds=1.0))]
    fn start_recording(
        &self,
        py: Python<'_>,
        submit_frame: Py<PyAny>,
        on_failure: Py<PyAny>,
        imu: Option<PyRef<'_, NativeImuCollector>>,
        submit_imu: Option<Py<PyAny>>,
        imu_timeout_seconds: f64,
    ) -> PyResult<Py<PyDict>> {
        if imu.is_some() != submit_imu.is_some() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: imu and submit_imu must be provided together",
            ));
        }
        if !imu_timeout_seconds.is_finite() || imu_timeout_seconds <= 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: imu_timeout_seconds must be finite and positive",
            ));
        }
        let imu_collector = imu
            .as_ref()
            .map(|collector| Arc::clone(&collector.collector));
        let snapshot = self
            .runtime
            .start_recording(
                submit_frame,
                on_failure,
                imu_collector,
                submit_imu,
                Duration::from_secs_f64(imu_timeout_seconds),
            )
            .map_err(capture_runtime_error)?;
        capture_runtime_snapshot_dict(py, &snapshot)
    }

    #[pyo3(signature = (active_take, sink, on_failure, imu=None, imu_timeout_seconds=1.0))]
    fn start_recording_raw_sink(
        &self,
        py: Python<'_>,
        active_take: PyRef<'_, NativeActiveTakeWriter>,
        sink: PyRef<'_, NativeRecordingSink>,
        on_failure: Py<PyAny>,
        imu: Option<PyRef<'_, NativeImuCollector>>,
        imu_timeout_seconds: f64,
    ) -> PyResult<Py<PyDict>> {
        if !imu_timeout_seconds.is_finite() || imu_timeout_seconds <= 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: imu_timeout_seconds must be finite and positive",
            ));
        }
        let imu_collector = imu
            .as_ref()
            .map(|collector| Arc::clone(&collector.collector));
        let runtime = Arc::clone(&self.runtime);
        let active_take = Arc::clone(&active_take.writer);
        let sink = Arc::clone(&sink.sink);
        let timeout = Duration::from_secs_f64(imu_timeout_seconds);
        let snapshot = py
            .allow_threads(move || {
                runtime.start_recording_raw_sink(
                    active_take,
                    sink,
                    on_failure,
                    imu_collector,
                    timeout,
                )
            })
            .map_err(capture_runtime_error)?;
        capture_runtime_snapshot_dict(py, &snapshot)
    }

    #[pyo3(signature = (active_take, sink, encoder, segment_planner, recording_start_monotonic_ns, on_failure, imu=None, imu_timeout_seconds=1.0))]
    #[allow(clippy::too_many_arguments)]
    fn start_recording_split_sink(
        &self,
        py: Python<'_>,
        active_take: PyRef<'_, NativeActiveTakeWriter>,
        sink: PyRef<'_, NativeRecordingSink>,
        encoder: PyRef<'_, NativeStereoEncoderProcess>,
        segment_planner: PyRef<'_, NativeRecordingSegmentPlanner>,
        recording_start_monotonic_ns: u64,
        on_failure: Py<PyAny>,
        imu: Option<PyRef<'_, NativeImuCollector>>,
        imu_timeout_seconds: f64,
    ) -> PyResult<Py<PyDict>> {
        if !imu_timeout_seconds.is_finite() || imu_timeout_seconds <= 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: imu_timeout_seconds must be finite and positive",
            ));
        }
        let imu_collector = imu
            .as_ref()
            .map(|collector| Arc::clone(&collector.collector));
        let runtime = Arc::clone(&self.runtime);
        let active_take = Arc::clone(&active_take.writer);
        let sink = Arc::clone(&sink.sink);
        let encoder = Arc::clone(&encoder.process);
        let segment_planner = Arc::clone(&segment_planner.planner);
        let timeout = Duration::from_secs_f64(imu_timeout_seconds);
        let snapshot = py
            .allow_threads(move || {
                runtime.start_recording_split_sink(
                    active_take,
                    sink,
                    encoder,
                    segment_planner,
                    recording_start_monotonic_ns,
                    on_failure,
                    imu_collector,
                    timeout,
                )
            })
            .map_err(capture_runtime_error)?;
        capture_runtime_snapshot_dict(py, &snapshot)
    }

    #[pyo3(signature = (timeout_seconds=3.0))]
    fn stop_recording(&self, py: Python<'_>, timeout_seconds: f64) -> PyResult<Py<PyDict>> {
        if !timeout_seconds.is_finite() || timeout_seconds <= 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: timeout_seconds must be finite and positive",
            ));
        }
        let timeout = Duration::from_secs_f64(timeout_seconds);
        let runtime = Arc::clone(&self.runtime);
        let snapshot = py
            .allow_threads(move || runtime.stop_recording(timeout))
            .map_err(capture_runtime_error)?;
        capture_runtime_snapshot_dict(py, &snapshot)
    }

    #[pyo3(signature = (timeout_seconds=5.0))]
    fn close(&self, py: Python<'_>, timeout_seconds: f64) -> PyResult<Py<PyDict>> {
        if !timeout_seconds.is_finite() || timeout_seconds <= 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: timeout_seconds must be finite and positive",
            ));
        }
        let timeout = Duration::from_secs_f64(timeout_seconds);
        let runtime = Arc::clone(&self.runtime);
        let snapshot = py
            .allow_threads(move || runtime.close(timeout))
            .map_err(capture_runtime_error)?;
        capture_runtime_snapshot_dict(py, &snapshot)
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let snapshot = self.runtime.snapshot().map_err(capture_runtime_error)?;
        capture_runtime_snapshot_dict(py, &snapshot)
    }
}

impl Drop for NativeContinuousCaptureRuntime {
    fn drop(&mut self) {
        let runtime = Arc::clone(&self.runtime);
        Python::with_gil(|py| {
            let _ = py.allow_threads(move || runtime.close(Duration::from_secs(5)));
        });
    }
}

#[pyclass]
struct NativeRecordingSegmentPlanner {
    planner: Arc<Mutex<recording::RecordingSegmentPlanner>>,
}

#[pymethods]
impl NativeRecordingSegmentPlanner {
    #[new]
    fn new(segment_frames: u64) -> PyResult<Self> {
        Ok(Self {
            planner: Arc::new(Mutex::new(
                recording::RecordingSegmentPlanner::new(segment_frames).map_err(recording_error)?,
            )),
        })
    }

    fn next_frame(
        &self,
        py: Python<'_>,
        record_sequence: u64,
        elapsed_seconds: f64,
    ) -> PyResult<Py<PyDict>> {
        let plan = {
            let mut planner = self.planner.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "native_recording_segment_planner_poisoned: segment planner mutex is poisoned",
                )
            })?;
            planner
                .next_frame(record_sequence, elapsed_seconds)
                .map_err(recording_error)?
        };
        recording_segment_frame_plan_dict(py, &plan)
    }

    fn register_segment(
        &self,
        py: Python<'_>,
        index: u64,
        start_ordinal: u64,
        end_ordinal: u64,
    ) -> PyResult<Py<PyDict>> {
        let snapshot = {
            let mut planner = self.planner.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "native_recording_segment_planner_poisoned: segment planner mutex is poisoned",
                )
            })?;
            planner
                .register_segment(index, start_ordinal, end_ordinal)
                .map_err(recording_error)?
        };
        recording_segment_planner_snapshot_dict(py, &snapshot)
    }

    fn finish(
        &self,
        py: Python<'_>,
        submitted_frames: u64,
        frame_domain: u64,
        duration_seconds: f64,
    ) -> PyResult<Py<PyDict>> {
        let snapshot = {
            let mut planner = self.planner.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "native_recording_segment_planner_poisoned: segment planner mutex is poisoned",
                )
            })?;
            planner
                .finish(submitted_frames, frame_domain, duration_seconds)
                .map_err(recording_error)?
        };
        recording_segment_planner_snapshot_dict(py, &snapshot)
    }

    fn boundary(
        &self,
        py: Python<'_>,
        ordinal: u64,
        duration_seconds: f64,
    ) -> PyResult<Py<PyDict>> {
        let boundary = {
            let planner = self.planner.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "native_recording_segment_planner_poisoned: segment planner mutex is poisoned",
                )
            })?;
            planner
                .boundary(ordinal, duration_seconds)
                .map_err(recording_error)?
        };
        recording_segment_boundary_dict(py, &boundary)
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let snapshot = {
            let planner = self.planner.lock().map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "native_recording_segment_planner_poisoned: segment planner mutex is poisoned",
                )
            })?;
            planner.snapshot()
        };
        recording_segment_planner_snapshot_dict(py, &snapshot)
    }
}

#[pyclass]
struct NativeRecordingEventQueue {
    producer: bounded::Producer<Py<PyAny>>,
    consumer: bounded::Consumer<Py<PyAny>>,
}

#[pymethods]
impl NativeRecordingEventQueue {
    #[new]
    fn new(capacity: usize) -> PyResult<Self> {
        if capacity == 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: recording event queue capacity must be positive",
            ));
        }
        let (producer, consumer) = bounded::channel(capacity);
        Ok(Self { producer, consumer })
    }

    #[pyo3(signature = (item, timeout_seconds=0.0))]
    fn put(&self, py: Python<'_>, item: Py<PyAny>, timeout_seconds: f64) -> PyResult<bool> {
        if !timeout_seconds.is_finite() || timeout_seconds < 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: recording event queue timeout must be finite and non-negative",
            ));
        }
        let producer = self.producer.clone();
        let timeout = Duration::from_secs_f64(timeout_seconds);
        match py.allow_threads(move || producer.push_timeout(item, timeout)) {
            Ok(()) => Ok(true),
            Err(_item) => Ok(false),
        }
    }

    fn get(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let consumer = self.consumer.clone();
        match py.allow_threads(move || consumer.receive_blocking()) {
            Ok(item) => Ok(item),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                Err(pyo3::exceptions::PyRuntimeError::new_err(
                    "recording_event_queue_closed: recording event queue is closed",
                ))
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                Err(pyo3::exceptions::PyRuntimeError::new_err(
                    "recording_event_queue_timeout: blocking receive unexpectedly timed out",
                ))
            }
        }
    }

    fn qsize(&self) -> PyResult<usize> {
        Ok(self.consumer.stats().depth)
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        recording_event_queue_stats_dict(py, &self.consumer.stats())
    }

    fn close_and_clear(&self) {
        self.consumer.close_and_clear();
    }
}

#[pyclass]
struct NativeStereoEncoderEvents;

#[pymethods]
impl NativeStereoEncoderEvents {
    #[new]
    fn new() -> Self {
        Self
    }

    fn parse(&self, py: Python<'_>, line: &[u8]) -> PyResult<Option<Py<PyDict>>> {
        let event = stereo_encoder::parse_event(line).map_err(stereo_encoder_event_error)?;
        let Some(event) = event else {
            return Ok(None);
        };
        Ok(Some(stereo_encoder_event_dict(py, &event)?))
    }
}

#[pyclass]
struct NativeStereoEncoderPipe {
    descriptor: i32,
    submitted: AtomicU64,
}

#[pymethods]
impl NativeStereoEncoderPipe {
    #[new]
    fn new(descriptor: i32) -> PyResult<Self> {
        if descriptor < 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: encoder pipe descriptor must be non-negative",
            ));
        }
        Ok(Self {
            descriptor,
            submitted: AtomicU64::new(0),
        })
    }

    fn submit(&self, py: Python<'_>, jpeg: &[u8]) -> PyResult<u64> {
        let descriptor = self.descriptor;
        let written = py
            .allow_threads(move || session_io::write_encoder_frame(descriptor, jpeg))
            .map_err(session_io_error)?;
        self.submitted
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_add(1)
            })
            .map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "counter_overflow: encoder submitted frame count overflowed",
                )
            })?;
        Ok(written)
    }

    fn submitted_frames(&self) -> u64 {
        self.submitted.load(Ordering::SeqCst)
    }
}

#[pyclass]
struct NativeStereoEncoderProcess {
    process: Arc<Mutex<stereo_encoder::EncoderProcess>>,
}

#[pymethods]
impl NativeStereoEncoderProcess {
    #[new]
    #[pyo3(signature = (out_dir, executable, width, height, fps, bitrate_kbps=8192, segment_frames=900, path_prefix="video/"))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        out_dir: &str,
        executable: &str,
        width: u64,
        height: u64,
        fps: u64,
        bitrate_kbps: u64,
        segment_frames: u64,
        path_prefix: &str,
    ) -> PyResult<Self> {
        Ok(Self {
            process: Arc::new(Mutex::new(
                stereo_encoder::EncoderProcess::new(
                    std::path::Path::new(out_dir),
                    std::path::Path::new(executable),
                    width,
                    height,
                    fps,
                    bitrate_kbps,
                    segment_frames,
                    path_prefix,
                )
                .map_err(stereo_encoder_process_error)?,
            )),
        })
    }

    fn start(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| {
            let mut process = self
                .process
                .lock()
                .map_err(|_| stereo_encoder_process_mutex_error())?;
            process.start()
        })
        .map_err(stereo_encoder_process_error)
    }

    fn submit(&self, py: Python<'_>, jpeg: &[u8]) -> PyResult<u64> {
        py.allow_threads(|| {
            let mut process = self
                .process
                .lock()
                .map_err(|_| stereo_encoder_process_mutex_error())?;
            process.submit(jpeg)
        })
        .map_err(stereo_encoder_process_error)
    }

    #[pyo3(signature = (timeout_seconds=30.0))]
    fn finish(&self, py: Python<'_>, timeout_seconds: f64) -> PyResult<Py<PyList>> {
        if !timeout_seconds.is_finite() || timeout_seconds <= 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: timeout_seconds must be finite and positive",
            ));
        }
        let timeout = Duration::from_secs_f64(timeout_seconds);
        let result = py
            .allow_threads(|| {
                let mut process = self
                    .process
                    .lock()
                    .map_err(|_| stereo_encoder_process_mutex_error())?;
                process.finish(timeout)
            })
            .map_err(stereo_encoder_process_error);
        let segments = result?;
        stereo_encoder_segment_list(py, &segments)
    }

    fn abort(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| {
            let mut process = self
                .process
                .lock()
                .map_err(|_| stereo_encoder_process_mutex_error())?;
            process.abort();
            Ok::<(), stereo_encoder::EncoderProcessError>(())
        })
        .map_err(stereo_encoder_process_error)
    }

    fn segments(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let segments = py
            .allow_threads(|| {
                let process = self
                    .process
                    .lock()
                    .map_err(|_| stereo_encoder_process_mutex_error())?;
                Ok::<Vec<stereo_encoder::SegmentEvent>, stereo_encoder::EncoderProcessError>(
                    process.segments(),
                )
            })
            .map_err(stereo_encoder_process_error)?;
        stereo_encoder_segment_list(py, &segments)
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let stats = py
            .allow_threads(|| {
                let process = self
                    .process
                    .lock()
                    .map_err(|_| stereo_encoder_process_mutex_error())?;
                Ok::<BTreeMap<String, i64>, stereo_encoder::EncoderProcessError>(process.stats())
            })
            .map_err(stereo_encoder_process_error)?;
        let value = PyDict::new(py);
        for (key, number) in stats {
            value.set_item(key, number)?;
        }
        Ok(value.unbind())
    }

    fn submitted_frames(&self, py: Python<'_>) -> PyResult<u64> {
        py.allow_threads(|| {
            let process = self
                .process
                .lock()
                .map_err(|_| stereo_encoder_process_mutex_error())?;
            Ok::<u64, stereo_encoder::EncoderProcessError>(process.submitted_frames())
        })
        .map_err(stereo_encoder_process_error)
    }
}

impl Drop for NativeStereoEncoderProcess {
    fn drop(&mut self) {
        if let Ok(mut process) = self.process.lock() {
            process.abort();
        }
    }
}

#[pyclass]
struct NativeSessionIo;

#[pymethods]
impl NativeSessionIo {
    #[new]
    fn new() -> Self {
        Self
    }

    fn hash_file(&self, py: Python<'_>, path: &str) -> PyResult<Py<PyDict>> {
        let path = std::path::PathBuf::from(path);
        let digest = py
            .allow_threads(move || session_io::hash_file(&path))
            .map_err(session_io_error)?;
        file_digest_dict(py, &digest)
    }

    #[pyo3(signature = (path, expected_bytes=None))]
    fn finalize_artifact(
        &self,
        py: Python<'_>,
        path: &str,
        expected_bytes: Option<u64>,
    ) -> PyResult<Py<PyDict>> {
        let path = std::path::PathBuf::from(path);
        let digest = py
            .allow_threads(move || session_io::finalize_artifact(&path, expected_bytes))
            .map_err(session_io_error)?;
        file_digest_dict(py, &digest)
    }

    fn verify_fd(
        &self,
        py: Python<'_>,
        descriptor: i32,
        expected_bytes: u64,
        expected_sha256: &str,
    ) -> PyResult<Py<PyDict>> {
        let expected_sha256 = expected_sha256.to_owned();
        let identity = py
            .allow_threads(move || {
                session_io::verify_fd(descriptor, expected_bytes, &expected_sha256)
            })
            .map_err(session_io_error)?;
        file_identity_dict(py, &identity)
    }

    fn sendfile(
        &self,
        py: Python<'_>,
        output_descriptor: i32,
        input_descriptor: i32,
        offset: u64,
        length: u64,
    ) -> PyResult<u64> {
        py.allow_threads(move || {
            session_io::sendfile_all(output_descriptor, input_descriptor, offset, length)
        })
        .map_err(session_io_error)
    }

    fn write_encoder_frame(&self, py: Python<'_>, descriptor: i32, jpeg: &[u8]) -> PyResult<u64> {
        py.allow_threads(move || session_io::write_encoder_frame(descriptor, jpeg))
            .map_err(session_io_error)
    }

    fn open_relative_regular(
        &self,
        py: Python<'_>,
        root_descriptor: i32,
        relative_path: &str,
    ) -> PyResult<i32> {
        let relative_path = relative_path.to_owned();
        py.allow_threads(move || session_io::open_relative_regular(root_descriptor, &relative_path))
            .map_err(session_io_error)
    }

    fn read_bounded_fd<'py>(
        &self,
        py: Python<'py>,
        descriptor: i32,
        maximum_bytes: usize,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let payload = py
            .allow_threads(move || session_io::read_fd_bounded(descriptor, maximum_bytes))
            .map_err(session_io_error)?;
        Ok(PyBytes::new(py, &payload))
    }

    fn device_session_v1_artifacts(
        &self,
        py: Python<'_>,
        manifest: &[u8],
        session_id: &str,
    ) -> PyResult<Py<PyList>> {
        let manifest = manifest.to_vec();
        let session_id = session_id.to_owned();
        let artifacts = py
            .allow_threads(move || session_io::device_session_v1_artifacts(&manifest, &session_id))
            .map_err(session_io_error)?;
        artifact_descriptor_list(py, &artifacts)
    }

    fn device_session_v1_artifact(
        &self,
        py: Python<'_>,
        manifest: &[u8],
        session_id: &str,
        artifact_id: &str,
    ) -> PyResult<Option<Py<PyDict>>> {
        let manifest = manifest.to_vec();
        let session_id = session_id.to_owned();
        let artifact_id = artifact_id.to_owned();
        let artifact = py
            .allow_threads(move || {
                session_io::device_session_v1_artifact(&manifest, &session_id, &artifact_id)
            })
            .map_err(session_io_error)?;
        artifact
            .as_ref()
            .map(|descriptor| artifact_descriptor_dict(py, descriptor))
            .transpose()
    }

    fn device_session_v1_summary(
        &self,
        py: Python<'_>,
        manifest: &[u8],
        session_id: &str,
    ) -> PyResult<Py<PyDict>> {
        let manifest = manifest.to_vec();
        let session_id = session_id.to_owned();
        let summary = py
            .allow_threads(move || session_io::device_session_v1_summary(&manifest, &session_id))
            .map_err(session_io_error)?;
        device_session_v1_summary_dict(py, &summary)
    }

    #[pyo3(signature = (partial_path, final_path, session_id, manifest, expected_identities, control_names=None))]
    #[allow(clippy::too_many_arguments)]
    fn seal_device_session_v1(
        &self,
        py: Python<'_>,
        partial_path: &str,
        final_path: &str,
        session_id: &str,
        manifest: &[u8],
        expected_identities: &Bound<'_, PyDict>,
        control_names: Option<Vec<String>>,
    ) -> PyResult<Py<PyDict>> {
        let partial_path = std::path::PathBuf::from(partial_path);
        let final_path = std::path::PathBuf::from(final_path);
        let session_id = session_id.to_owned();
        let manifest = manifest.to_vec();
        let expected_identities = expected_artifact_identities(expected_identities)?;
        let control_names = control_names
            .unwrap_or_else(|| vec!["recording.json".to_owned(), "capture.json".to_owned()]);
        let result = py
            .allow_threads(move || {
                session_io::seal_device_session(
                    &partial_path,
                    &final_path,
                    &manifest,
                    &session_id,
                    &expected_identities,
                    &control_names,
                )
            })
            .map_err(session_io_error)?;
        device_session_seal_result_dict(py, &result)
    }
}

fn bounded_range_decimal(value: &str, upper_bound: u64) -> Option<u64> {
    if value.is_empty() || !value.is_ascii() || !value.bytes().all(|item| item.is_ascii_digit()) {
        return None;
    }
    let normalized = value.trim_start_matches('0');
    let normalized = if normalized.is_empty() {
        "0"
    } else {
        normalized
    };
    let bound = upper_bound.to_string();
    if normalized.len() > bound.len()
        || (normalized.len() == bound.len() && normalized > bound.as_str())
    {
        return Some(upper_bound.saturating_add(1));
    }
    normalized.parse::<u64>().ok()
}

fn parse_single_http_range(
    value: Option<&str>,
    complete_size: i64,
) -> Result<Option<(u64, u64)>, ()> {
    if complete_size < 0 {
        return Err(());
    }
    let Some(value) = value else {
        return Ok(None);
    };
    if !value.starts_with("bytes=") || value.contains(',') {
        return Err(());
    }
    let complete_size = complete_size as u64;
    let selected = &value["bytes=".len()..];
    if let Some(suffix) = selected.strip_prefix('-') {
        let Some(suffix_size) = bounded_range_decimal(suffix, complete_size) else {
            return Err(());
        };
        if suffix_size == 0 || complete_size == 0 {
            return Err(());
        }
        return Ok(Some((
            complete_size.saturating_sub(suffix_size),
            complete_size - 1,
        )));
    }
    let Some((first_text, last_text)) = selected.split_once('-') else {
        return Err(());
    };
    let Some(first) = bounded_range_decimal(first_text, complete_size) else {
        return Err(());
    };
    if first >= complete_size {
        return Err(());
    }
    if last_text.is_empty() {
        return Ok(Some((first, complete_size - 1)));
    }
    let Some(last) = bounded_range_decimal(last_text, complete_size) else {
        return Err(());
    };
    if last < first {
        return Err(());
    }
    Ok(Some((first, last.min(complete_size - 1))))
}

#[pyfunction]
fn parse_single_range(value: Option<&str>, complete_size: i64) -> PyResult<Option<(u64, u64)>> {
    if complete_size < 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "invalid_argument: complete_size must be non-negative",
        ));
    }
    parse_single_http_range(value, complete_size).map_err(|_| {
        pyo3::exceptions::PyValueError::new_err("range_not_satisfiable: range cannot be satisfied")
    })
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn evaluate_drop_quality_policy(
    py: Python<'_>,
    drop_events: &Bound<'_, PyAny>,
    frames_written: u64,
    max_contiguous_dropped_frames: u64,
    max_total_dropped_frames: u64,
    max_drop_fraction: f64,
    window_seconds: f64,
    max_dropped_frames_per_window: u64,
) -> PyResult<Py<PyDict>> {
    let events = drop_events.downcast::<PySequence>()?;
    let count = events.len()?;
    let mut parsed = Vec::with_capacity(count);
    for index in 0..count {
        let event = events.get_item(index)?;
        parsed.push(recording::DropEvent {
            at_time_seconds: event.get_item("at_time_seconds")?.extract()?,
            dropped: event.get_item("dropped")?.extract()?,
        });
    }
    let result = recording::evaluate_drop_quality(
        frames_written,
        &parsed,
        recording::DropQualityPolicy {
            max_contiguous_dropped_frames,
            max_total_dropped_frames,
            max_drop_fraction,
            window_seconds,
            max_dropped_frames_per_window,
        },
    )
    .map_err(recording_error)?;
    drop_quality_evaluation_dict(py, &result)
}

#[pyclass]
struct NativePreviewBuffer {
    buffer: Arc<preview::LatestBuffer>,
}

#[pymethods]
impl NativePreviewBuffer {
    #[new]
    fn new(stream_fps: u32) -> PyResult<Self> {
        Ok(Self {
            buffer: Arc::new(preview::LatestBuffer::new(stream_fps).map_err(preview_error)?),
        })
    }

    fn publish(&self, py: Python<'_>, jpeg: Py<PyBytes>) -> PyResult<u64> {
        self.buffer.publish(py, jpeg).map_err(preview_error)
    }

    fn clear(&self) -> PyResult<()> {
        self.buffer.clear().map_err(preview_error)
    }

    fn jpeg(&self, py: Python<'_>) -> PyResult<(u64, Py<PyBytes>)> {
        let snapshot = self.buffer.snapshot(py).map_err(preview_error)?;
        Ok((snapshot.sequence, snapshot.jpeg))
    }

    #[pyo3(signature = (fps=None))]
    fn multipart_stream(&self, fps: Option<u32>) -> PyResult<NativeMultipartPreview> {
        let requested_fps = fps.unwrap_or_else(|| self.buffer.stream_fps());
        if requested_fps < 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: fps must be at least 1",
            ));
        }
        let effective_fps = requested_fps.min(self.buffer.stream_fps());
        Ok(NativeMultipartPreview {
            buffer: Arc::clone(&self.buffer),
            stop: Arc::new(AtomicBool::new(false)),
            period: Duration::from_secs_f64(1.0 / f64::from(effective_fps)),
            state: Mutex::new(MultipartPreviewState {
                last_sequence: 0,
                next_delivery: Instant::now(),
            }),
        })
    }

    fn wake_streams(&self) {
        self.buffer.wake_streams();
    }
}

#[pyclass(frozen)]
struct NativeMultipartPreview {
    buffer: Arc<preview::LatestBuffer>,
    stop: Arc<AtomicBool>,
    period: Duration,
    state: Mutex<MultipartPreviewState>,
}

struct MultipartPreviewState {
    last_sequence: u64,
    next_delivery: Instant,
}

#[pymethods]
impl NativeMultipartPreview {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        while !self.stop.load(Ordering::Acquire) {
            let now = Instant::now();
            let (delay, last_sequence) = {
                let state = self.state.lock().map_err(|_| {
                    pyo3::exceptions::PyRuntimeError::new_err(
                        "preview_stream_poisoned: preview stream mutex is poisoned",
                    )
                })?;
                (
                    (now < state.next_delivery).then(|| state.next_delivery.duration_since(now)),
                    state.last_sequence,
                )
            };
            if let Some(delay) = delay {
                let buffer = Arc::clone(&self.buffer);
                let stop = Arc::clone(&self.stop);
                py.allow_threads(move || buffer.wait_until_stop(&stop, delay))
                    .map_err(preview_error)?;
                continue;
            }

            let buffer = Arc::clone(&self.buffer);
            let stop = Arc::clone(&self.stop);
            py.allow_threads(move || {
                buffer.wait_after(last_sequence, &stop, Duration::from_millis(250))
            })
            .map_err(preview_error)?;
            if self.stop.load(Ordering::Acquire) {
                break;
            }
            let snapshot = self.buffer.snapshot(py).map_err(preview_error)?;
            {
                let mut state = self.state.lock().map_err(|_| {
                    pyo3::exceptions::PyRuntimeError::new_err(
                        "preview_stream_poisoned: preview stream mutex is poisoned",
                    )
                })?;
                if snapshot.sequence == state.last_sequence {
                    continue;
                }
                state.last_sequence = snapshot.sequence;
                state.next_delivery = Instant::now() + self.period;
            }
            return Ok(Some(preview::multipart_part(py, &snapshot.jpeg)?));
        }
        Ok(None)
    }

    fn close(&self) {
        self.stop.store(true, Ordering::Release);
        self.buffer.wake_streams();
    }
}

impl Drop for NativeMultipartPreview {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.buffer.wake_streams();
    }
}

#[pyclass]
struct NativePerformanceMetrics {
    metrics: Arc<metrics::Metrics>,
}

#[pymethods]
impl NativePerformanceMetrics {
    #[new]
    fn new() -> Self {
        Self {
            metrics: Arc::new(metrics::Metrics::new()),
        }
    }

    fn record_stage(&self, name: &str, elapsed_ns: u64) -> PyResult<()> {
        self.metrics
            .record_stage(name, elapsed_ns)
            .map_err(metrics_error)
    }

    #[pyo3(signature = (name, size, count=1))]
    fn record_copy(&self, name: &str, size: u64, count: u64) -> PyResult<()> {
        self.metrics
            .record_copy(name, size, count)
            .map_err(metrics_error)
    }

    fn change_payload(&self, name: &str, count_delta: i64, bytes_delta: i64) -> PyResult<()> {
        self.metrics
            .change_payload(name, count_delta, bytes_delta)
            .map_err(metrics_error)
    }

    #[pyo3(signature = (depth, capacity, rejected=0, peak_depth=None))]
    fn observe_queue(
        &self,
        depth: u64,
        capacity: u64,
        rejected: u64,
        peak_depth: Option<u64>,
    ) -> PyResult<()> {
        self.metrics
            .observe_queue(depth, capacity, rejected, peak_depth)
            .map_err(metrics_error)
    }

    #[pyo3(signature = (kind, count=1))]
    fn record_loss(&self, kind: &str, count: u64) -> PyResult<()> {
        self.metrics.record_loss(kind, count).map_err(metrics_error)
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        self.metrics.snapshot(py).map_err(metrics_error)
    }
}

#[pyclass]
struct NativeSplitter {
    handle: Arc<Mutex<turbojpeg::TransformHandle>>,
}

#[pymethods]
impl NativeSplitter {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Self {
            handle: Arc::new(Mutex::new(
                turbojpeg::TransformHandle::open().map_err(native_error)?,
            )),
        })
    }

    fn split<'py>(
        &self,
        py: Python<'py>,
        payload: &[u8],
        width: i32,
        height: i32,
    ) -> PyResult<(Bound<'py, PyBytes>, Bound<'py, PyBytes>)> {
        let handle = Arc::clone(&self.handle);
        let (left, right) = py
            .allow_threads(move || {
                handle
                    .lock()
                    .map_err(|_| turbojpeg::TurboJpegError {
                        code: "native_splitter_poisoned",
                        message: "native splitter mutex is poisoned".to_owned(),
                    })?
                    .split_sbs(payload, width, height)
            })
            .map_err(native_error)?;
        Ok((PyBytes::new(py, &left), PyBytes::new(py, &right)))
    }

    fn close(&self) -> PyResult<()> {
        self.handle
            .lock()
            .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("native_splitter_poisoned"))?
            .close();
        Ok(())
    }
}

#[pyfunction]
fn capabilities(py: Python<'_>) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("module_version", env!("CARGO_PKG_VERSION"))?;
    result.set_item("abi", NATIVE_ABI)?;
    let mut features = vec![CAPABILITY_PROBE, JPEG_CONTRACT, FRAME_STREAM];
    features.push(RECORDING_CODEC);
    features.push(RECORDING_SINK);
    features.push(RECORDING_IMU_BATCH);
    features.push(RECORDING_FRAME_GATE);
    features.push(RECORDING_TAP_STATE);
    features.push(CAPTURE_FANOUT);
    features.push(RECORDING_SEGMENT_PLANNER);
    features.push(RECORDING_EVENT_QUEUE);
    features.push(ARTIFACT_FINALIZE);
    features.push(RANGE_PARSER);
    features.push(STEREO_ENCODER_EVENTS);
    features.push(STEREO_ENCODER_PIPE);
    features.push(STEREO_ENCODER_PROCESS);
    features.push(SESSION_IO);
    features.push(DEVICE_SESSION_ARTIFACTS);
    features.push(DEVICE_SESSION_FINALIZER);
    features.push(DROP_QUALITY_POLICY);
    features.push(CAMERA_FRAME_VALIDATOR);
    features.push(PREVIEW_BUFFER);
    features.push(PERFORMANCE_METRICS);
    features.push(NATIVE_TIMELINE);
    features.push(ACTIVE_TAKE_WRITER);
    features.push(CONTINUOUS_CAPTURE_RAW_SINK);
    features.push(CONTINUOUS_CAPTURE_SPLIT_SINK);
    features.push(V4L2_CAPTURE);
    features.push(V4L2_FOCUS_CONTROL);
    if turbojpeg::available() {
        features.push(TURBOJPEG_SPLIT);
        features.push(NATIVE_CAMERA);
        features.push(CONTINUOUS_CAPTURE_RUNTIME);
    }
    if audio::available() {
        features.push(NATIVE_AUDIO);
    }
    if imu::available() {
        features.push(NATIVE_IMU);
    }
    result.set_item("features", features)?;
    Ok(result.unbind())
}

#[pyfunction]
fn jpeg_metadata(py: Python<'_>, payload: &[u8]) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("ranges", jpeg::ranges(payload))?;
    result.set_item("dimensions", jpeg::dimensions(payload))?;
    Ok(result.unbind())
}

#[pyfunction]
fn encode_frame<'py>(py: Python<'py>, payload: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    let encoded = frame_stream::encode(payload)
        .map_err(|_| pyo3::exceptions::PyValueError::new_err("invalid_frame_length"))?;
    Ok(PyBytes::new(py, &encoded))
}

fn focus_status_dict(py: Python<'_>, status: v4l2::FocusStatus) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("schema", "ylx.camera-focus.v1")?;
    result.set_item("value", status.value)?;
    result.set_item("minimum", status.minimum)?;
    result.set_item("maximum", status.maximum)?;
    result.set_item("step", status.step)?;
    result.set_item("default", status.default_value)?;
    result.set_item("auto_supported", status.auto_supported)?;
    result.set_item("auto_enabled", status.auto_enabled)?;
    Ok(result.unbind())
}

#[pyfunction]
fn v4l2_focus_status(py: Python<'_>, device: &str) -> PyResult<Option<Py<PyDict>>> {
    py.allow_threads(|| v4l2::focus_status(device))
        .map_err(v4l2_error)?
        .map(|status| focus_status_dict(py, status))
        .transpose()
}

#[pyfunction]
#[pyo3(signature = (device, value=None, auto_enabled=None))]
fn v4l2_set_focus(
    py: Python<'_>,
    device: &str,
    value: Option<i32>,
    auto_enabled: Option<bool>,
) -> PyResult<Py<PyDict>> {
    let status = py
        .allow_threads(|| v4l2::set_focus(device, value, auto_enabled))
        .map_err(v4l2_error)?;
    focus_status_dict(py, status)
}

fn camera_frame_validation_dict(
    py: Python<'_>,
    validation: &native_camera::FrameValidation,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("dropped_before", validation.dropped_before)?;
    value.set_item("queue_rejected", validation.queue_rejected)?;
    value.set_item("source_gap", validation.source_gap)?;
    Ok(value.unbind())
}

fn audio_result_dict(py: Python<'_>, result: &audio::AudioRecordingResult) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("device", &result.device)?;
    value.set_item("codec", result.codec)?;
    value.set_item("container", result.container)?;
    value.set_item("sample_rate_hz", result.sample_rate_hz)?;
    value.set_item("channels", result.channels)?;
    value.set_item("sample_format", result.sample_format)?;
    value.set_item("sample_count", result.sample_count)?;
    value.set_item("started_monotonic_ns", result.started_monotonic_ns)?;
    value.set_item("stopped_monotonic_ns", result.stopped_monotonic_ns)?;
    let segments = PyList::empty(py);
    for segment in &result.segments {
        let item = PyDict::new(py);
        item.set_item("index", segment.index)?;
        item.set_item("path", &segment.relative_path)?;
        item.set_item("start_sample", segment.start_sample)?;
        item.set_item("end_sample", segment.end_sample)?;
        item.set_item("start_time_seconds", segment.start_time_seconds)?;
        item.set_item("end_time_seconds", segment.end_time_seconds)?;
        segments.append(item)?;
    }
    value.set_item("segments", segments)?;
    Ok(value.unbind())
}

fn audio_snapshot_dict(
    py: Python<'_>,
    snapshot: &audio::AudioRecordingSnapshot,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("sample_count", snapshot.sample_count)?;
    value.set_item("bytes_written", snapshot.bytes_written)?;
    value.set_item("segment_count", snapshot.segment_count)?;
    Ok(value.unbind())
}

fn timeline_audio_sync_dict(py: Python<'_>, sync: &timeline::AudioSync) -> PyResult<Py<PyDict>> {
    let session_start_offset_ns = i64::try_from(sync.session_start_offset_ns).map_err(|_| {
        pyo3::exceptions::PyOverflowError::new_err(
            "timeline_overflow: audio start offset does not fit in signed 64-bit nanoseconds",
        )
    })?;
    let session_stop_offset_ns = i64::try_from(sync.session_stop_offset_ns).map_err(|_| {
        pyo3::exceptions::PyOverflowError::new_err(
            "timeline_overflow: audio stop offset does not fit in signed 64-bit nanoseconds",
        )
    })?;
    let value = PyDict::new(py);
    value.set_item("clock", "host_monotonic")?;
    value.set_item("timebase", "monotonic_ns")?;
    value.set_item(
        "session_start_monotonic_ns",
        sync.session_start_monotonic_ns,
    )?;
    value.set_item("started_monotonic_ns", sync.started_monotonic_ns)?;
    value.set_item("stopped_monotonic_ns", sync.stopped_monotonic_ns)?;
    value.set_item("session_start_offset_ns", session_start_offset_ns)?;
    value.set_item("session_stop_offset_ns", session_stop_offset_ns)?;
    value.set_item(
        "session_start_offset_seconds",
        sync.session_start_offset_seconds,
    )?;
    value.set_item(
        "session_stop_offset_seconds",
        sync.session_stop_offset_seconds,
    )?;
    value.set_item("sample_duration_ns", sync.sample_duration_ns)?;
    Ok(value.unbind())
}

fn active_take_reserved_frame_dict(
    py: Python<'_>,
    frame: &active_take::ReservedFrame,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("session_id", &frame.session_id)?;
    value.set_item("record_sequence", frame.record_sequence)?;
    value.set_item("source_sequence", frame.source_sequence)?;
    value.set_item("host_monotonic_ns", frame.host_monotonic_ns)?;
    Ok(value.unbind())
}

fn active_take_write_decision_dict(
    py: Python<'_>,
    decision: &active_take::FrameWriteDecision,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    match decision {
        active_take::FrameWriteDecision::RawSideBySide(raw) => {
            value.set_item("layout", "raw-side-by-side")?;
            value.set_item("session_id", &raw.session_id)?;
            value.set_item("record_sequence", raw.record_sequence)?;
            value.set_item("source_sequence", raw.source_sequence)?;
            value.set_item("host_monotonic_ns", raw.host_monotonic_ns)?;
        }
        active_take::FrameWriteDecision::SplitEyes(split) => {
            value.set_item("layout", "split-eyes")?;
            value.set_item("session_id", &split.session_id)?;
            value.set_item("record_sequence", split.record_sequence)?;
            value.set_item("source_sequence", split.source_sequence)?;
            value.set_item("host_monotonic_ns", split.host_monotonic_ns)?;
            value.set_item("segment_index", split.segment_index)?;
            value.set_item("segment_frame", split.segment_frame)?;
        }
    }
    Ok(value.unbind())
}

fn active_take_snapshot_dict(
    py: Python<'_>,
    snapshot: &active_take::ActiveTakeSnapshot,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("session_id", &snapshot.session_id)?;
    value.set_item("frame_domain", snapshot.frame_domain)?;
    value.set_item("frames_written", snapshot.frames_written)?;
    value.set_item("bytes_written", snapshot.bytes_written)?;
    value.set_item("dropped_frames", snapshot.dropped_frames)?;
    value.set_item("pending_frames", snapshot.pending_frames)?;
    let drop_events = PyList::empty(py);
    for event in &snapshot.drop_events {
        let item = PyDict::new(py);
        item.set_item("start_frame", event.start_frame)?;
        item.set_item("end_frame", event.end_frame)?;
        item.set_item("at_time_seconds", event.at_time_seconds)?;
        item.set_item("reason", event.reason)?;
        item.set_item("dropped", event.dropped)?;
        drop_events.append(item)?;
    }
    value.set_item("drop_events", drop_events)?;
    Ok(value.unbind())
}

fn recording_sink_snapshot_dict(
    py: Python<'_>,
    snapshot: &recording::RecordingSinkSnapshot,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("bytes_written", snapshot.bytes_written)?;
    value.set_item("frames_written", snapshot.frames_written)?;
    value.set_item("imu_samples_written", snapshot.imu_samples_written)?;
    let artifacts = PyDict::new(py);
    for artifact in &snapshot.artifacts {
        let item = PyDict::new(py);
        item.set_item("role", artifact.role)?;
        item.set_item("path", artifact.relative_path)?;
        item.set_item("bytes", artifact.bytes)?;
        item.set_item("sha256", &artifact.sha256)?;
        let identity = PyDict::new(py);
        identity.set_item("device", artifact.identity.device)?;
        identity.set_item("inode", artifact.identity.inode)?;
        identity.set_item("size", artifact.identity.size)?;
        identity.set_item("mtime_ns", artifact.identity.mtime_ns)?;
        item.set_item("identity", identity)?;
        artifacts.set_item(artifact.role, item)?;
    }
    value.set_item("artifacts", artifacts)?;
    Ok(value.unbind())
}

fn recording_frame_gate_decision_dict(
    py: Python<'_>,
    decision: &recording::FrameGateDecision,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("record", decision.record)?;
    value.set_item("dropped_before", decision.dropped_before)?;
    value.set_item("observed_frames", decision.observed_frames)?;
    value.set_item("inflight_frames", decision.inflight_frames)?;
    Ok(value.unbind())
}

fn recording_frame_gate_snapshot_dict(
    py: Python<'_>,
    snapshot: &recording::FrameGateSnapshot,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("frame_decimation", snapshot.frame_decimation)?;
    value.set_item("first_frame", snapshot.first_frame)?;
    value.set_item("observed_frames", snapshot.observed_frames)?;
    value.set_item("inflight_frames", snapshot.inflight_frames)?;
    value.set_item("stopping", snapshot.stopping)?;
    Ok(value.unbind())
}

fn recording_tap_snapshot_dict(
    py: Python<'_>,
    snapshot: &recording::RecordingTapSnapshot,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("frame_decimation", snapshot.frame_decimation)?;
    value.set_item("first_frame", snapshot.first_frame)?;
    value.set_item("observed_frames", snapshot.observed_frames)?;
    value.set_item("inflight_frames", snapshot.inflight_frames)?;
    value.set_item("stopping", snapshot.stopping)?;
    value.set_item("failure_reported", snapshot.failure_reported)?;
    Ok(value.unbind())
}

fn capture_fanout_decision_dict(
    py: Python<'_>,
    decision: &recording::CaptureFanoutDecision,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("publish_preview", decision.publish_preview)?;
    value.set_item("record", decision.record)?;
    value.set_item("dropped_before", decision.dropped_before)?;
    value.set_item("observed_frames", decision.observed_frames)?;
    value.set_item("inflight_frames", decision.inflight_frames)?;
    value.set_item("recording_active", decision.recording_active)?;
    Ok(value.unbind())
}

fn capture_fanout_snapshot_dict(
    py: Python<'_>,
    snapshot: &recording::CaptureFanoutSnapshot,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("frame_decimation", snapshot.frame_decimation)?;
    value.set_item("recording_present", snapshot.recording_present)?;
    value.set_item("recording_active", snapshot.recording_active)?;
    value.set_item("first_frame", snapshot.first_frame)?;
    value.set_item("observed_frames", snapshot.observed_frames)?;
    value.set_item("inflight_frames", snapshot.inflight_frames)?;
    value.set_item("stopping", snapshot.stopping)?;
    value.set_item("failure_reported", snapshot.failure_reported)?;
    Ok(value.unbind())
}

fn capture_runtime_error_dict(
    py: Python<'_>,
    error: &capture_runtime::RuntimeError,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("code", error.code)?;
    value.set_item("message", &error.message)?;
    Ok(value.unbind())
}

fn capture_runtime_snapshot_dict(
    py: Python<'_>,
    snapshot: &capture_runtime::RuntimeSnapshot,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("running", snapshot.running)?;
    value.set_item("recording_present", snapshot.recording_present)?;
    value.set_item("recording_active", snapshot.recording_active)?;
    value.set_item("inflight_frames", snapshot.inflight_frames)?;
    value.set_item("observed_frames", snapshot.observed_frames)?;
    value.set_item("failure_reported", snapshot.failure_reported)?;
    match &snapshot.terminal_error {
        Some(error) => value.set_item("terminal_error", capture_runtime_error_dict(py, error)?)?,
        None => value.set_item("terminal_error", py.None())?,
    }
    match &snapshot.last_preview_error {
        Some(error) => {
            value.set_item("last_preview_error", capture_runtime_error_dict(py, error)?)?
        }
        None => value.set_item("last_preview_error", py.None())?,
    }
    Ok(value.unbind())
}

fn recording_segment_frame_plan_dict(
    py: Python<'_>,
    plan: &recording::SegmentFramePlan,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("ordinal", plan.ordinal)?;
    value.set_item("segment_index", plan.segment_index)?;
    value.set_item("segment_frame", plan.segment_frame)?;
    value.set_item("frames_written", plan.frames_written)?;
    value.set_item("boundary_recorded", plan.boundary_recorded)?;
    Ok(value.unbind())
}

fn recording_segment_boundary_dict(
    py: Python<'_>,
    boundary: &recording::SegmentBoundary,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("frame", boundary.frame)?;
    value.set_item("time_seconds", boundary.time_seconds)?;
    Ok(value.unbind())
}

fn recording_segment_planner_snapshot_dict(
    py: Python<'_>,
    snapshot: &recording::SegmentPlannerSnapshot,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("segment_frames", snapshot.segment_frames)?;
    value.set_item("frames_written", snapshot.frames_written)?;
    value.set_item("segment_count", snapshot.segment_count)?;
    value.set_item("covered_frames", snapshot.covered_frames)?;
    value.set_item("boundary_count", snapshot.boundary_count)?;
    Ok(value.unbind())
}

fn recording_event_queue_stats_dict(
    py: Python<'_>,
    stats: &bounded::QueueStats,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("capacity", stats.capacity)?;
    value.set_item("depth", stats.depth)?;
    value.set_item("peak_depth", stats.peak_depth)?;
    value.set_item("enqueued", stats.enqueued)?;
    value.set_item("delivered", stats.delivered)?;
    value.set_item("rejected", stats.rejected)?;
    Ok(value.unbind())
}

fn drop_quality_evaluation_dict(
    py: Python<'_>,
    result: &recording::DropQualityEvaluation,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("accepted", result.accepted)?;
    value.set_item("dropped", result.dropped)?;
    value.set_item("total", result.total)?;
    value.set_item("fraction", result.fraction)?;
    value.set_item("contiguous", result.contiguous)?;
    value.set_item("window_drops", result.window_drops)?;
    value.set_item("violations", result.violations.clone())?;
    Ok(value.unbind())
}

fn stereo_encoder_event_dict(
    py: Python<'_>,
    event: &stereo_encoder::EncoderEvent,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    match event {
        stereo_encoder::EncoderEvent::Ready => {
            value.set_item("event", "ready")?;
        }
        stereo_encoder::EncoderEvent::Segment(segment) => {
            value.set_item("event", "segment")?;
            value.set_item("index", segment.index)?;
            value.set_item("start_frame", segment.start_frame)?;
            value.set_item("end_frame", segment.end_frame)?;
            let left = PyDict::new(py);
            left.set_item("path", &segment.left_path)?;
            left.set_item("bytes", segment.left_bytes)?;
            value.set_item("left", left)?;
            let right = PyDict::new(py);
            right.set_item("path", &segment.right_path)?;
            right.set_item("bytes", segment.right_bytes)?;
            value.set_item("right", right)?;
        }
        stereo_encoder::EncoderEvent::Done(stats) => {
            value.set_item("event", "done")?;
            for (key, number) in stats {
                value.set_item(key, number)?;
            }
        }
        stereo_encoder::EncoderEvent::Error { code, message } => {
            value.set_item("event", "error")?;
            value.set_item("code", code)?;
            value.set_item("message", message)?;
        }
    }
    Ok(value.unbind())
}

fn stereo_encoder_segment_list(
    py: Python<'_>,
    segments: &[stereo_encoder::SegmentEvent],
) -> PyResult<Py<PyList>> {
    let value = PyList::empty(py);
    for segment in segments {
        let item = PyDict::new(py);
        item.set_item("index", segment.index)?;
        item.set_item("start_frame", segment.start_frame)?;
        item.set_item("end_frame", segment.end_frame)?;
        item.set_item("left_path", &segment.left_path)?;
        item.set_item("left_bytes", segment.left_bytes)?;
        item.set_item("right_path", &segment.right_path)?;
        item.set_item("right_bytes", segment.right_bytes)?;
        value.append(item)?;
    }
    Ok(value.unbind())
}

fn file_identity_dict(py: Python<'_>, identity: &session_io::FileIdentity) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("device", identity.device)?;
    value.set_item("inode", identity.inode)?;
    value.set_item("size", identity.size)?;
    value.set_item("modified_ns", identity.modified_ns)?;
    value.set_item("nlink", identity.nlink)?;
    Ok(value.unbind())
}

fn expected_artifact_identities(
    raw: &Bound<'_, PyDict>,
) -> PyResult<Vec<session_io::ExpectedArtifactIdentity>> {
    let mut identities = Vec::with_capacity(raw.len());
    for (path, identity) in raw.iter() {
        let path = path.extract::<String>()?;
        let identity = identity.downcast::<PySequence>()?;
        if identity.len()? != 4 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "artifact_invalid: artifact identity must be a 4-item tuple",
            ));
        }
        let device = strict_u64_identity_field(&identity.get_item(0)?, "device")?;
        let inode = strict_u64_identity_field(&identity.get_item(1)?, "inode")?;
        let size = strict_u64_identity_field(&identity.get_item(2)?, "size")?;
        let modified_ns = strict_i64_identity_field(&identity.get_item(3)?, "modified_ns")?;
        identities.push(session_io::ExpectedArtifactIdentity {
            path,
            identity: session_io::FileIdentity {
                device,
                inode,
                size,
                modified_ns,
                nlink: 1,
            },
        });
    }
    Ok(identities)
}

fn strict_u64_identity_field(value: &Bound<'_, PyAny>, field: &str) -> PyResult<u64> {
    if value.is_instance_of::<PyBool>() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "artifact_invalid: artifact identity {field} must be an integer"
        )));
    }
    value.extract::<u64>().map_err(|error| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "artifact_invalid: artifact identity {field} must be a non-negative integer: {error}"
        ))
    })
}

fn strict_i64_identity_field(value: &Bound<'_, PyAny>, field: &str) -> PyResult<i64> {
    if value.is_instance_of::<PyBool>() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "artifact_invalid: artifact identity {field} must be an integer"
        )));
    }
    value.extract::<i64>().map_err(|error| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "artifact_invalid: artifact identity {field} must be an integer: {error}"
        ))
    })
}

fn file_digest_dict(py: Python<'_>, digest: &session_io::FileDigest) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("sha256", &digest.sha256)?;
    value.set_item("identity", file_identity_dict(py, &digest.identity)?)?;
    Ok(value.unbind())
}

fn artifact_descriptor_list(
    py: Python<'_>,
    artifacts: &[session_io::ArtifactDescriptor],
) -> PyResult<Py<PyList>> {
    let value = PyList::empty(py);
    for artifact in artifacts {
        value.append(artifact_descriptor_dict(py, artifact)?)?;
    }
    Ok(value.unbind())
}

fn artifact_descriptor_dict(
    py: Python<'_>,
    artifact: &session_io::ArtifactDescriptor,
) -> PyResult<Py<PyDict>> {
    let item = PyDict::new(py);
    item.set_item("artifact_id", &artifact.artifact_id)?;
    item.set_item("role", &artifact.role)?;
    item.set_item("path", &artifact.path)?;
    item.set_item("media_type", &artifact.media_type)?;
    item.set_item("bytes", artifact.bytes)?;
    item.set_item("sha256", &artifact.sha256)?;
    Ok(item.unbind())
}

fn device_session_seal_result_dict(
    py: Python<'_>,
    result: &session_io::DeviceSessionSealResult,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("manifest_sha256", &result.manifest_sha256)?;
    value.set_item("artifact_count", result.artifact_count)?;
    value.set_item("manifest_bytes", result.manifest_bytes)?;
    Ok(value.unbind())
}

fn device_session_v1_summary_dict(
    py: Python<'_>,
    summary: &session_io::DeviceSessionV1Summary,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("session_id", &summary.session_id)?;
    value.set_item("display_name", &summary.display_name)?;
    value.set_item("started_at", &summary.started_at)?;
    value.set_item("ended_at", &summary.ended_at)?;
    value.set_item("duration_seconds", summary.duration_seconds)?;
    value.set_item("frames_count", summary.frames_count)?;
    value.set_item("imu_sample_count", summary.imu_sample_count)?;
    match summary.audio_sample_count {
        Some(count) => value.set_item("audio_sample_count", count)?,
        None => value.set_item("audio_sample_count", py.None())?,
    }
    value.set_item("total_bytes", summary.total_bytes)?;
    value.set_item(
        "artifacts",
        artifact_descriptor_list(py, &summary.artifacts)?,
    )?;
    Ok(value.unbind())
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("NATIVE_ABI", NATIVE_ABI)?;
    module.add("NATIVE_VERSION", env!("CARGO_PKG_VERSION"))?;
    module.add(
        "FRAME_STREAM_MAGIC",
        PyBytes::new(module.py(), frame_stream::MAGIC),
    )?;
    module.add_function(wrap_pyfunction!(capabilities, module)?)?;
    module.add_function(wrap_pyfunction!(parse_single_range, module)?)?;
    module.add_function(wrap_pyfunction!(evaluate_drop_quality_policy, module)?)?;
    module.add_function(wrap_pyfunction!(jpeg_metadata, module)?)?;
    module.add_function(wrap_pyfunction!(encode_frame, module)?)?;
    module.add_function(wrap_pyfunction!(v4l2_focus_status, module)?)?;
    module.add_function(wrap_pyfunction!(v4l2_set_focus, module)?)?;
    module.add_class::<NativeSplitter>()?;
    module.add_class::<NativeCameraStream>()?;
    module.add_class::<NativeCameraFrameValidator>()?;
    module.add_class::<NativeAudioRecorder>()?;
    module.add_class::<NativeTimeline>()?;
    module.add_class::<NativeActiveTakeWriter>()?;
    module.add_class::<NativeImuCollector>()?;
    module.add_class::<NativeRecordingCodec>()?;
    module.add_class::<NativeRecordingSink>()?;
    module.add_class::<NativeRecordingFrameGate>()?;
    module.add_class::<NativeRecordingTapState>()?;
    module.add_class::<NativeCaptureFanoutState>()?;
    module.add_class::<NativeContinuousCaptureRuntime>()?;
    module.add_class::<NativeRecordingSegmentPlanner>()?;
    module.add_class::<NativeRecordingEventQueue>()?;
    module.add_class::<NativeStereoEncoderEvents>()?;
    module.add_class::<NativeStereoEncoderPipe>()?;
    module.add_class::<NativeStereoEncoderProcess>()?;
    module.add_class::<NativeSessionIo>()?;
    module.add_class::<NativePreviewBuffer>()?;
    module.add_class::<NativeMultipartPreview>()?;
    module.add_class::<NativePerformanceMetrics>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ACTIVE_TAKE_WRITER, ARTIFACT_FINALIZE, CAMERA_FRAME_VALIDATOR, CAPABILITY_PROBE,
        CAPTURE_FANOUT, CONTINUOUS_CAPTURE_RAW_SINK, CONTINUOUS_CAPTURE_RUNTIME,
        CONTINUOUS_CAPTURE_SPLIT_SINK, DEVICE_SESSION_ARTIFACTS, DEVICE_SESSION_FINALIZER,
        DROP_QUALITY_POLICY, FRAME_STREAM, JPEG_CONTRACT, NATIVE_ABI, NATIVE_AUDIO, NATIVE_CAMERA,
        NATIVE_IMU, NATIVE_TIMELINE, PERFORMANCE_METRICS, PREVIEW_BUFFER, RANGE_PARSER,
        RECORDING_CODEC, RECORDING_EVENT_QUEUE, RECORDING_FRAME_GATE, RECORDING_IMU_BATCH,
        RECORDING_SEGMENT_PLANNER, RECORDING_SINK, RECORDING_TAP_STATE, SESSION_IO,
        STEREO_ENCODER_EVENTS, STEREO_ENCODER_PIPE, STEREO_ENCODER_PROCESS, TURBOJPEG_SPLIT,
        V4L2_CAPTURE, V4L2_FOCUS_CONTROL, parse_single_http_range,
    };

    #[test]
    fn abi_and_initial_capability_are_stable() {
        assert_eq!(NATIVE_ABI, 4);
        assert_eq!(CAPABILITY_PROBE, "capability_probe");
        assert_eq!(JPEG_CONTRACT, "jpeg_contract");
        assert_eq!(FRAME_STREAM, "frame_stream");
        assert_eq!(TURBOJPEG_SPLIT, "turbojpeg_split");
        assert_eq!(V4L2_CAPTURE, "v4l2_capture");
        assert_eq!(V4L2_FOCUS_CONTROL, "v4l2_focus_control");
        assert_eq!(NATIVE_CAMERA, "native_camera");
        assert_eq!(CAMERA_FRAME_VALIDATOR, "camera_frame_validator");
        assert_eq!(NATIVE_AUDIO, "native_audio");
        assert_eq!(NATIVE_TIMELINE, "native_timeline");
        assert_eq!(ACTIVE_TAKE_WRITER, "active_take_writer");
        assert_eq!(NATIVE_IMU, "native_imu");
        assert_eq!(RECORDING_CODEC, "recording_codec");
        assert_eq!(RECORDING_SINK, "recording_sink");
        assert_eq!(RECORDING_IMU_BATCH, "recording_imu_batch");
        assert_eq!(RECORDING_FRAME_GATE, "recording_frame_gate");
        assert_eq!(RECORDING_TAP_STATE, "recording_tap_state");
        assert_eq!(CAPTURE_FANOUT, "capture_fanout");
        assert_eq!(CONTINUOUS_CAPTURE_RUNTIME, "continuous_capture_runtime");
        assert_eq!(CONTINUOUS_CAPTURE_RAW_SINK, "continuous_capture_raw_sink");
        assert_eq!(
            CONTINUOUS_CAPTURE_SPLIT_SINK,
            "continuous_capture_split_sink"
        );
        assert_eq!(RECORDING_SEGMENT_PLANNER, "recording_segment_planner");
        assert_eq!(RECORDING_EVENT_QUEUE, "recording_event_queue");
        assert_eq!(ARTIFACT_FINALIZE, "artifact_finalize");
        assert_eq!(RANGE_PARSER, "range_parser");
        assert_eq!(STEREO_ENCODER_EVENTS, "stereo_encoder_events");
        assert_eq!(STEREO_ENCODER_PIPE, "stereo_encoder_pipe");
        assert_eq!(STEREO_ENCODER_PROCESS, "stereo_encoder_process");
        assert_eq!(SESSION_IO, "session_io");
        assert_eq!(DEVICE_SESSION_ARTIFACTS, "device_session_artifacts");
        assert_eq!(DEVICE_SESSION_FINALIZER, "device_session_finalizer");
        assert_eq!(DROP_QUALITY_POLICY, "drop_quality_policy");
        assert_eq!(PREVIEW_BUFFER, "preview_buffer");
        assert_eq!(PERFORMANCE_METRICS, "performance_metrics");
    }

    #[test]
    fn parses_single_http_ranges_like_gateway_contract() {
        assert_eq!(parse_single_http_range(None, 26), Ok(None));
        assert_eq!(
            parse_single_http_range(Some("bytes=10-"), 26),
            Ok(Some((10, 25)))
        );
        assert_eq!(
            parse_single_http_range(Some("bytes=-4"), 26),
            Ok(Some((22, 25)))
        );
        assert_eq!(
            parse_single_http_range(Some("bytes=0-999"), 26),
            Ok(Some((0, 25)))
        );
        assert_eq!(
            parse_single_http_range(Some("bytes=2-8"), 26),
            Ok(Some((2, 8)))
        );
        for value in [
            "bytes=0-1,3-4",
            "bytes=26-",
            "bytes=8-2",
            "bytes=-0",
            "items=0-1",
            "bytes=invalid",
            "bytes=999999999999999999999999999999999999",
        ] {
            assert!(parse_single_http_range(Some(value), 26).is_err(), "{value}");
        }
        assert!(parse_single_http_range(Some("bytes=0-"), -1).is_err());
    }
}
