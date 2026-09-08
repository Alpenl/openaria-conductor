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
mod session_store;
mod stereo_encoder;
mod turbojpeg;
mod v4l2;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyBytes, PyDict, PyList, PySequence, PySequenceMethods};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const NATIVE_ABI: u32 = 5;
const CAPABILITY_PROBE: &str = "capability_probe";
const JPEG_CONTRACT: &str = "jpeg_contract";
const FRAME_STREAM: &str = "frame_stream";
const TURBOJPEG_SPLIT: &str = "turbojpeg_split";
const V4L2_FOCUS_CONTROL: &str = "v4l2_focus_control";
const CAPTURE_ENGINE: &str = "capture_engine";
const NATIVE_AUDIO: &str = "native_audio";
const SESSION_STORE: &str = "session_store";
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

fn imu_error(error: imu::ImuError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("{}: {}", error.code, error.message))
}

fn session_io_error(error: session_io::SessionIoError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("{}: {}", error.code, error.message))
}

fn session_store_error(error: session_store::StoreError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("{}: {}", error.code, error.message))
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

#[pyclass]
struct NativeSessionTransaction {
    transaction: Arc<session_store::SessionTransaction>,
}

#[pymethods]
impl NativeSessionTransaction {
    #[getter]
    fn recording_start_monotonic_ns(&self) -> u64 {
        self.transaction.recording_start_monotonic_ns
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let snapshot = self.transaction.snapshot().map_err(session_store_error)?;
        session_transaction_snapshot_dict(py, &snapshot)
    }

    fn segments(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let segments = self.transaction.segments().map_err(session_store_error)?;
        stereo_encoder_segment_list(py, &segments)
    }

    #[pyo3(signature = (duration_seconds, timeout_seconds=30.0))]
    fn finish(
        &self,
        py: Python<'_>,
        duration_seconds: f64,
        timeout_seconds: f64,
    ) -> PyResult<Py<PyDict>> {
        if !duration_seconds.is_finite() || duration_seconds < 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: duration_seconds must be finite and non-negative",
            ));
        }
        if !timeout_seconds.is_finite() || timeout_seconds <= 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: timeout_seconds must be finite and positive",
            ));
        }
        let transaction = Arc::clone(&self.transaction);
        let outcome = py
            .allow_threads(move || {
                transaction.finish(
                    Duration::from_secs_f64(duration_seconds),
                    Duration::from_secs_f64(timeout_seconds),
                )
            })
            .map_err(session_store_error)?;
        recording_outcome_dict(py, &outcome)
    }

    fn boundary(
        &self,
        py: Python<'_>,
        ordinal: u64,
        duration_seconds: f64,
    ) -> PyResult<Py<PyDict>> {
        if !duration_seconds.is_finite() || duration_seconds < 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: duration_seconds must be finite and non-negative",
            ));
        }
        let boundary = self
            .transaction
            .boundary(ordinal, Duration::from_secs_f64(duration_seconds))
            .map_err(session_store_error)?;
        recording_segment_boundary_dict(py, &boundary)
    }

    fn abort(&self, py: Python<'_>, _reason: &str) {
        let transaction = Arc::clone(&self.transaction);
        py.allow_threads(move || transaction.abort());
    }

    fn open_handle_count(&self) -> u64 {
        self.transaction.open_handle_count()
    }

    #[pyo3(signature = (partial_path, final_path, session_id, manifest, expected_identities, control_names=None))]
    #[allow(clippy::too_many_arguments)]
    fn seal(
        &self,
        py: Python<'_>,
        partial_path: &str,
        final_path: &str,
        session_id: &str,
        manifest: &[u8],
        expected_identities: &Bound<'_, PyDict>,
        control_names: Option<Vec<String>>,
    ) -> PyResult<Py<PyDict>> {
        self.transaction
            .ensure_finished()
            .map_err(session_store_error)?;
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

#[pyclass]
struct NativeSessionStore;

#[pymethods]
impl NativeSessionStore {
    #[new]
    fn new() -> Self {
        Self
    }

    fn begin_recording(
        &self,
        py: Python<'_>,
        plan: &Bound<'_, PyAny>,
    ) -> PyResult<NativeSessionTransaction> {
        let session_root =
            std::path::PathBuf::from(plan.getattr("session_root")?.extract::<String>()?);
        let session_id = plan.getattr("session_id")?.extract::<String>()?;
        let encoder_executable =
            std::path::PathBuf::from(plan.getattr("encoder_executable")?.extract::<String>()?);
        let width = plan.getattr("width")?.extract::<u64>()?;
        let height = plan.getattr("height")?.extract::<u64>()?;
        let fps = plan.getattr("fps")?.extract::<u64>()?;
        let bitrate_kbps = plan.getattr("bitrate_kbps")?.extract::<u64>()?;
        let segment_frames = plan.getattr("segment_frames")?.extract::<u64>()?;
        let recording_start_monotonic_ns = plan
            .getattr("recording_start_monotonic_ns")?
            .extract::<u64>()?;
        let audio_enabled = plan.getattr("audio_enabled")?.extract::<bool>()?;
        let audio_device = plan.getattr("audio_device")?.extract::<String>()?;
        let audio_sample_rate_hz = plan.getattr("audio_sample_rate_hz")?.extract::<u32>()?;
        let audio_channels = plan.getattr("audio_channels")?.extract::<u16>()?;
        let audio_segment_seconds = plan.getattr("audio_segment_seconds")?.extract::<f64>()?;
        let transaction = py
            .allow_threads(move || {
                let audio = audio_enabled.then_some(session_store::AudioPlan {
                    device: &audio_device,
                    sample_rate_hz: audio_sample_rate_hz,
                    channels: audio_channels,
                    segment_seconds: audio_segment_seconds,
                });
                session_store::SessionTransaction::begin(session_store::RecordingPlan {
                    session_root: &session_root,
                    session_id: &session_id,
                    encoder_executable: &encoder_executable,
                    width,
                    height,
                    fps,
                    bitrate_kbps,
                    segment_frames,
                    recording_start_monotonic_ns,
                    audio,
                })
            })
            .map_err(session_store_error)?;
        Ok(NativeSessionTransaction {
            transaction: Arc::new(transaction),
        })
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

    fn open_verified_artifact(
        &self,
        py: Python<'_>,
        root_descriptor: i32,
        relative_path: &str,
        expected_bytes: u64,
        expected_sha256: &str,
    ) -> PyResult<Py<PyDict>> {
        let relative_path = relative_path.to_owned();
        let expected_sha256 = expected_sha256.to_owned();
        let artifact = py
            .allow_threads(move || {
                session_io::open_verified_artifact(
                    root_descriptor,
                    &relative_path,
                    expected_bytes,
                    &expected_sha256,
                )
            })
            .map_err(session_io_error)?;
        let result = PyDict::new(py);
        result.set_item("descriptor", artifact.descriptor)?;
        result.set_item("identity", file_identity_dict(py, &artifact.identity)?)?;
        Ok(result.unbind())
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
}

struct CaptureImuConfig {
    device: String,
    unit: Option<u8>,
    selector: u8,
    stale_poll_interval: Duration,
    timeout: Duration,
}

#[pyclass]
struct NativeCaptureEngine {
    stream: Arc<native_camera::Stream>,
    runtime: Arc<capture_runtime::Runtime>,
    imu_config: CaptureImuConfig,
    imu: Mutex<Option<Arc<imu::Collector>>>,
    latest_imu: Mutex<Option<imu::ImuObservation>>,
    close_timeout: Duration,
}

#[pymethods]
impl NativeCaptureEngine {
    #[new]
    fn new(
        plan: &Bound<'_, PyAny>,
        preview: PyRef<'_, NativePreviewBuffer>,
        metrics: Option<PyRef<'_, NativePerformanceMetrics>>,
    ) -> PyResult<Self> {
        let device = plan.getattr("device")?.extract::<String>()?;
        let width = plan.getattr("width")?.extract::<u32>()?;
        let height = plan.getattr("height")?.extract::<u32>()?;
        let fps = plan.getattr("fps")?.extract::<u32>()?;
        let encoding = plan.getattr("encoding")?.extract::<String>()?;
        let buffer_count = plan.getattr("buffer_count")?.extract::<u32>()?;
        let queue_capacity = plan.getattr("queue_capacity")?.extract::<usize>()?;
        let frame_decimation = plan.getattr("frame_decimation")?.extract::<u64>()?;
        let read_timeout_seconds = plan.getattr("read_timeout_seconds")?.extract::<f64>()?;
        let imu_unit = plan.getattr("imu_unit")?.extract::<Option<u8>>()?;
        let imu_selector = plan.getattr("imu_selector")?.extract::<u8>()?;
        let imu_stale_poll_interval = plan.getattr("imu_stale_poll_interval")?.extract::<f64>()?;
        let imu_timeout_seconds = plan.getattr("imu_timeout_seconds")?.extract::<f64>()?;
        if !read_timeout_seconds.is_finite() || read_timeout_seconds <= 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: read_timeout_seconds must be finite and positive",
            ));
        }
        if !imu_timeout_seconds.is_finite() || imu_timeout_seconds <= 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: imu_timeout_seconds must be finite and positive",
            ));
        }
        if !imu_stale_poll_interval.is_finite() || imu_stale_poll_interval < 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: imu_stale_poll_interval must be finite and non-negative",
            ));
        }
        let stream = Arc::new(
            native_camera::Stream::open(
                &device,
                width,
                height,
                fps,
                &encoding,
                buffer_count,
                queue_capacity,
                false,
            )
            .map_err(camera_error)?,
        );
        let metrics = metrics.as_ref().map(|metrics| Arc::clone(&metrics.metrics));
        let runtime = Arc::new(
            capture_runtime::Runtime::new(
                Arc::clone(&stream),
                Arc::clone(&preview.buffer),
                frame_decimation,
                Duration::from_secs_f64(read_timeout_seconds),
                metrics,
            )
            .map_err(capture_runtime_error)?,
        );
        Ok(Self {
            stream,
            runtime,
            imu_config: CaptureImuConfig {
                device,
                unit: imu_unit,
                selector: imu_selector,
                stale_poll_interval: Duration::from_secs_f64(imu_stale_poll_interval),
                timeout: Duration::from_secs_f64(imu_timeout_seconds),
            },
            imu: Mutex::new(None),
            latest_imu: Mutex::new(None),
            close_timeout: Duration::from_secs_f64(read_timeout_seconds + 1.0),
        })
    }

    fn start_preview(&self, py: Python<'_>) -> PyResult<()> {
        let runtime = Arc::clone(&self.runtime);
        py.allow_threads(move || runtime.start_preview())
            .map_err(capture_runtime_error)
    }

    fn start_recording(
        &self,
        py: Python<'_>,
        transaction: PyRef<'_, NativeSessionTransaction>,
        on_failure: Py<PyAny>,
    ) -> PyResult<Py<PyDict>> {
        transaction
            .transaction
            .ensure_recording()
            .map_err(session_store_error)?;
        let runtime_snapshot = self.runtime.snapshot().map_err(capture_runtime_error)?;
        if runtime_snapshot.recording_active {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "invalid_state: capture engine is already recording",
            ));
        }
        if runtime_snapshot.recording_present {
            let runtime = Arc::clone(&self.runtime);
            let timeout = self.close_timeout;
            py.allow_threads(move || runtime.stop_recording(timeout))
                .map_err(capture_runtime_error)?;
        }
        if let Some(stale) = self
            .imu
            .lock()
            .map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "capture_engine_poisoned: IMU mutex is poisoned",
                )
            })?
            .take()
        {
            stale.close();
        }
        let collector = Arc::new(
            imu::Collector::open(
                &self.imu_config.device,
                self.imu_config.unit,
                self.imu_config.selector,
                Some(self.imu_config.stale_poll_interval),
            )
            .map_err(imu_error)?,
        );
        let runtime = Arc::clone(&self.runtime);
        let active_take = Arc::clone(&transaction.transaction.active_take);
        let sink = Arc::clone(&transaction.transaction.sink);
        let encoder = Arc::clone(&transaction.transaction.encoder);
        let segment_planner = Arc::clone(&transaction.transaction.segment_planner);
        let recording_start_monotonic_ns = transaction.transaction.recording_start_monotonic_ns;
        let worker_collector = Arc::clone(&collector);
        let imu_timeout = self.imu_config.timeout;
        let snapshot = py
            .allow_threads(move || {
                runtime.start_recording(
                    active_take,
                    sink,
                    encoder,
                    segment_planner,
                    recording_start_monotonic_ns,
                    on_failure,
                    Some(worker_collector),
                    imu_timeout,
                )
            })
            .map_err(capture_runtime_error);
        match snapshot {
            Ok(snapshot) => {
                *self.imu.lock().map_err(|_| {
                    pyo3::exceptions::PyRuntimeError::new_err(
                        "capture_engine_poisoned: IMU mutex is poisoned",
                    )
                })? = Some(collector);
                capture_runtime_snapshot_dict(py, &snapshot)
            }
            Err(error) => {
                collector.close();
                Err(error)
            }
        }
    }

    #[pyo3(signature = (timeout_seconds=3.0))]
    fn stop_recording(&self, py: Python<'_>, timeout_seconds: f64) -> PyResult<Py<PyDict>> {
        if !timeout_seconds.is_finite() || timeout_seconds <= 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: timeout_seconds must be finite and positive",
            ));
        }
        let runtime = Arc::clone(&self.runtime);
        let snapshot = py
            .allow_threads(move || runtime.stop_recording(Duration::from_secs_f64(timeout_seconds)))
            .map_err(capture_runtime_error)?;
        if let Some(collector) = self
            .imu
            .lock()
            .map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "capture_engine_poisoned: IMU mutex is poisoned",
                )
            })?
            .take()
        {
            if let Ok(latest) = collector.latest_observation() {
                *self.latest_imu.lock().map_err(|_| {
                    pyo3::exceptions::PyRuntimeError::new_err(
                        "capture_engine_poisoned: latest IMU mutex is poisoned",
                    )
                })? = latest;
            }
            collector.close();
        }
        capture_runtime_snapshot_dict(py, &snapshot)
    }

    fn latest_imu_observation(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let current = self
            .imu
            .lock()
            .map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "capture_engine_poisoned: IMU mutex is poisoned",
                )
            })?
            .as_ref()
            .map(Arc::clone);
        let observation = match current {
            Some(collector) => collector.latest_observation().map_err(imu_error)?,
            None => self
                .latest_imu
                .lock()
                .map_err(|_| {
                    pyo3::exceptions::PyRuntimeError::new_err(
                        "capture_engine_poisoned: latest IMU mutex is poisoned",
                    )
                })?
                .clone(),
        };
        observation
            .as_ref()
            .map(|value| imu::observation_dict(py, value))
            .transpose()
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

    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let snapshot = self.runtime.snapshot().map_err(capture_runtime_error)?;
        capture_runtime_snapshot_dict(py, &snapshot)
    }

    #[pyo3(signature = (timeout_seconds=5.0))]
    fn close(&self, py: Python<'_>, timeout_seconds: f64) -> PyResult<Py<PyDict>> {
        if !timeout_seconds.is_finite() || timeout_seconds <= 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid_argument: timeout_seconds must be finite and positive",
            ));
        }
        if let Some(collector) = self
            .imu
            .lock()
            .map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "capture_engine_poisoned: IMU mutex is poisoned",
                )
            })?
            .take()
        {
            collector.close();
        }
        let runtime = Arc::clone(&self.runtime);
        let snapshot = py
            .allow_threads(move || runtime.close(Duration::from_secs_f64(timeout_seconds)))
            .map_err(capture_runtime_error)?;
        capture_runtime_snapshot_dict(py, &snapshot)
    }
}

impl Drop for NativeCaptureEngine {
    fn drop(&mut self) {
        if let Ok(current) = self.imu.get_mut() {
            if let Some(collector) = current.take() {
                collector.close();
            }
        }
        let _ = self.runtime.close(self.close_timeout);
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

    fn publish(&self, jpeg: &[u8]) -> PyResult<u64> {
        self.buffer.publish(jpeg).map_err(preview_error)
    }

    fn clear(&self) -> PyResult<()> {
        self.buffer.clear().map_err(preview_error)
    }

    fn jpeg(&self, py: Python<'_>) -> PyResult<(u64, Py<PyBytes>)> {
        let snapshot = self.buffer.snapshot().map_err(preview_error)?;
        Ok((snapshot.sequence, PyBytes::new(py, &snapshot.jpeg).unbind()))
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
            let snapshot = self.buffer.snapshot().map_err(preview_error)?;
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
            return Ok(Some(PyBytes::new(
                py,
                &preview::multipart_part(&snapshot.jpeg),
            )));
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
    let mut features = vec![
        CAPABILITY_PROBE,
        JPEG_CONTRACT,
        FRAME_STREAM,
        SESSION_STORE,
        PREVIEW_BUFFER,
        PERFORMANCE_METRICS,
        V4L2_FOCUS_CONTROL,
    ];
    if turbojpeg::available() {
        features.push(TURBOJPEG_SPLIT);
        if imu::available() {
            features.push(CAPTURE_ENGINE);
        }
    }
    if audio::available() {
        features.push(NATIVE_AUDIO);
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

fn capture_runtime_error_dict(
    py: Python<'_>,
    error: &capture_runtime::RuntimeError,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("code", error.code)?;
    value.set_item("message", &error.message)?;
    Ok(value.unbind())
}

fn session_transaction_snapshot_dict(
    py: Python<'_>,
    snapshot: &session_store::TransactionSnapshot,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("state", snapshot.state)?;
    value.set_item(
        "active_take",
        active_take_snapshot_dict(py, &snapshot.active_take)?,
    )?;
    value.set_item("sink", recording_sink_snapshot_dict(py, &snapshot.sink)?)?;
    match &snapshot.audio {
        Some(audio) => value.set_item("audio", audio_snapshot_dict(py, audio)?)?,
        None => value.set_item("audio", py.None())?,
    }
    value.set_item(
        "segments",
        stereo_encoder_segment_list(py, &snapshot.segments)?,
    )?;
    value.set_item("submitted_frames", snapshot.submitted_frames)?;
    Ok(value.unbind())
}

fn recording_outcome_dict(
    py: Python<'_>,
    outcome: &session_store::RecordingOutcome,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item(
        "active_take",
        active_take_snapshot_dict(py, &outcome.active_take)?,
    )?;
    value.set_item("sink", recording_sink_snapshot_dict(py, &outcome.sink)?)?;
    match &outcome.audio {
        Some(audio) => value.set_item("audio", audio_result_dict(py, audio)?)?,
        None => value.set_item("audio", py.None())?,
    }
    value.set_item(
        "segments",
        stereo_encoder_segment_list(py, &outcome.segments)?,
    )?;
    let encoder_stats = PyDict::new(py);
    for (name, count) in &outcome.encoder_stats {
        encoder_stats.set_item(name, count)?;
    }
    value.set_item("encoder_stats", encoder_stats)?;
    value.set_item(
        "planner",
        recording_segment_planner_snapshot_dict(py, &outcome.planner)?,
    )?;
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
    module.add_function(wrap_pyfunction!(v4l2_focus_status, module)?)?;
    module.add_function(wrap_pyfunction!(v4l2_set_focus, module)?)?;
    module.add_class::<NativeSplitter>()?;
    module.add_class::<NativeCaptureEngine>()?;
    module.add_class::<NativeSessionStore>()?;
    module.add_class::<NativeSessionTransaction>()?;
    module.add_class::<NativePreviewBuffer>()?;
    module.add_class::<NativeMultipartPreview>()?;
    module.add_class::<NativePerformanceMetrics>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        CAPABILITY_PROBE, CAPTURE_ENGINE, FRAME_STREAM, JPEG_CONTRACT, NATIVE_ABI, NATIVE_AUDIO,
        PERFORMANCE_METRICS, PREVIEW_BUFFER, SESSION_STORE, TURBOJPEG_SPLIT, V4L2_FOCUS_CONTROL,
    };

    #[test]
    fn abi_and_initial_capability_are_stable() {
        assert_eq!(NATIVE_ABI, 5);
        assert_eq!(CAPABILITY_PROBE, "capability_probe");
        assert_eq!(JPEG_CONTRACT, "jpeg_contract");
        assert_eq!(FRAME_STREAM, "frame_stream");
        assert_eq!(TURBOJPEG_SPLIT, "turbojpeg_split");
        assert_eq!(V4L2_FOCUS_CONTROL, "v4l2_focus_control");
        assert_eq!(CAPTURE_ENGINE, "capture_engine");
        assert_eq!(NATIVE_AUDIO, "native_audio");
        assert_eq!(SESSION_STORE, "session_store");
        assert_eq!(PREVIEW_BUFFER, "preview_buffer");
        assert_eq!(PERFORMANCE_METRICS, "performance_metrics");
    }
}
