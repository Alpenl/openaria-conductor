mod audio;
mod bounded;
mod frame_stream;
mod imu;
mod jpeg;
mod metrics;
mod native_camera;
mod preview;
mod recording;
mod session_io;
mod turbojpeg;
mod v4l2;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const NATIVE_ABI: u32 = 4;
const CAPABILITY_PROBE: &str = "capability_probe";
const JPEG_CONTRACT: &str = "jpeg_contract";
const FRAME_STREAM: &str = "frame_stream";
const TURBOJPEG_SPLIT: &str = "turbojpeg_split";
const V4L2_CAPTURE: &str = "v4l2_capture";
const NATIVE_CAMERA: &str = "native_camera";
const NATIVE_AUDIO: &str = "native_audio";
const NATIVE_IMU: &str = "native_imu";
const RECORDING_CODEC: &str = "recording_codec";
const SESSION_IO: &str = "session_io";
const PREVIEW_BUFFER: &str = "preview_buffer";
const PERFORMANCE_METRICS: &str = "performance_metrics";

fn native_error(error: turbojpeg::TurboJpegError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("{}: {}", error.code, error.message))
}

fn camera_error(error: native_camera::StreamError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("{}: {}", error.code, error.message))
}

fn audio_error(error: audio::AudioError) -> PyErr {
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

fn preview_error(error: preview::PreviewError) -> PyErr {
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
        imu_observation_dict(py, &observation)
    }

    fn close(&self, py: Python<'_>) {
        let collector = Arc::clone(&self.collector);
        py.allow_threads(move || collector.close());
    }

    fn unit(&self) -> PyResult<u8> {
        self.collector.unit().map_err(imu_error)
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
    features.push(SESSION_IO);
    features.push(PREVIEW_BUFFER);
    features.push(PERFORMANCE_METRICS);
    features.push(V4L2_CAPTURE);
    if turbojpeg::available() {
        features.push(TURBOJPEG_SPLIT);
        features.push(NATIVE_CAMERA);
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

fn imu_observation_dict(py: Python<'_>, result: &imu::ImuObservation) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("dropped_samples", result.dropped_samples)?;
    let samples = PyList::empty(py);
    for sample in &result.samples {
        let item = PyDict::new(py);
        item.set_item("sequence", sample.sequence)?;
        item.set_item("packet_sequence", sample.packet_sequence)?;
        item.set_item("sample_index", sample.sample_index)?;
        item.set_item("device_timestamp_raw", sample.device_timestamp_raw)?;
        item.set_item("device_ticks", sample.device_ticks)?;
        item.set_item("host_read_start_ns", sample.host_read_start_ns)?;
        item.set_item("host_read_end_ns", sample.host_read_end_ns)?;
        item.set_item("host_monotonic_ns", sample.host_monotonic_ns)?;

        let raw = PyDict::new(py);
        raw.set_item(
            "accelerometer",
            vec![
                sample.accelerometer.x,
                sample.accelerometer.y,
                sample.accelerometer.z,
            ],
        )?;
        raw.set_item(
            "gyroscope",
            vec![sample.gyroscope.x, sample.gyroscope.y, sample.gyroscope.z],
        )?;
        item.set_item("raw", raw)?;

        let sync = PyDict::new(py);
        sync.set_item("offset_ns", sample.sync_offset_ns)?;
        sync.set_item("residual_ns", sample.sync_residual_ns)?;
        sync.set_item("quality", sample.sync_quality)?;
        item.set_item("sync", sync)?;

        samples.append(item)?;
    }
    value.set_item("samples", samples)?;
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

fn file_digest_dict(py: Python<'_>, digest: &session_io::FileDigest) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("sha256", &digest.sha256)?;
    value.set_item("identity", file_identity_dict(py, &digest.identity)?)?;
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
    module.add_function(wrap_pyfunction!(jpeg_metadata, module)?)?;
    module.add_function(wrap_pyfunction!(encode_frame, module)?)?;
    module.add_class::<NativeSplitter>()?;
    module.add_class::<NativeCameraStream>()?;
    module.add_class::<NativeAudioRecorder>()?;
    module.add_class::<NativeImuCollector>()?;
    module.add_class::<NativeRecordingCodec>()?;
    module.add_class::<NativeSessionIo>()?;
    module.add_class::<NativePreviewBuffer>()?;
    module.add_class::<NativeMultipartPreview>()?;
    module.add_class::<NativePerformanceMetrics>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        CAPABILITY_PROBE, FRAME_STREAM, JPEG_CONTRACT, NATIVE_ABI, NATIVE_AUDIO, NATIVE_CAMERA,
        NATIVE_IMU, PERFORMANCE_METRICS, PREVIEW_BUFFER, RECORDING_CODEC, SESSION_IO,
        TURBOJPEG_SPLIT, V4L2_CAPTURE,
    };

    #[test]
    fn abi_and_initial_capability_are_stable() {
        assert_eq!(NATIVE_ABI, 4);
        assert_eq!(CAPABILITY_PROBE, "capability_probe");
        assert_eq!(JPEG_CONTRACT, "jpeg_contract");
        assert_eq!(FRAME_STREAM, "frame_stream");
        assert_eq!(TURBOJPEG_SPLIT, "turbojpeg_split");
        assert_eq!(V4L2_CAPTURE, "v4l2_capture");
        assert_eq!(NATIVE_CAMERA, "native_camera");
        assert_eq!(NATIVE_AUDIO, "native_audio");
        assert_eq!(NATIVE_IMU, "native_imu");
        assert_eq!(RECORDING_CODEC, "recording_codec");
        assert_eq!(SESSION_IO, "session_io");
        assert_eq!(PREVIEW_BUFFER, "preview_buffer");
        assert_eq!(PERFORMANCE_METRICS, "performance_metrics");
    }
}
