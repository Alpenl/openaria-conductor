use crate::hardware::{imu, imu_preview, native_camera};
use crate::py::convert::{capture_runtime_snapshot_dict, focus_status_dict, imu_observation_dict};
use crate::py::metrics::NativePerformanceMetrics;
use crate::py::preview::NativePreviewBuffer;
use crate::py::session::NativeSessionTransaction;
use crate::py::{
    allow_threads_if_gil_held, camera_error, capture_runtime_error, imu_error, session_store_error,
    try_duration_from_secs_f64,
};
use crate::session::capture_runtime::{self, FailureSink, RecordingTarget};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

type CaptureImuConfig = imu_preview::Config;

pub(crate) struct PyFailureSink {
    enabled: AtomicBool,
    callback: Mutex<Option<Py<PyAny>>>,
}

impl PyFailureSink {
    fn new(callback: Py<PyAny>) -> Self {
        Self {
            enabled: AtomicBool::new(true),
            callback: Mutex::new(Some(callback)),
        }
    }

    fn disable(&self) {
        self.enabled.store(false, Ordering::Release);
    }

    fn clear_with_gil(&self, _py: Python<'_>) {
        self.disable();
        let mut guard = self
            .callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _ = guard.take();
    }

    fn clear_on_drop(&self) {
        self.disable();
        let cb = self
            .callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let Some(cb) = cb else {
            return;
        };
        // SAFETY: Py_IsInitialized is a read-only C-API state query.
        if unsafe { pyo3::ffi::Py_IsInitialized() != 0 } {
            Python::with_gil(|_py| drop(cb));
        } else {
            std::mem::forget(cb);
        }
    }
}

impl Drop for PyFailureSink {
    fn drop(&mut self) {
        self.clear_on_drop();
    }
}

impl FailureSink for PyFailureSink {
    fn on_failure(&self, code: &str, message: &str) {
        if !self.enabled.load(Ordering::Acquire) {
            return;
        }
        // SAFETY: Py_IsInitialized is a read-only C-API state query.
        if unsafe { pyo3::ffi::Py_IsInitialized() == 0 } {
            return;
        }
        Python::with_gil(|py| {
            if !self.enabled.load(Ordering::Acquire) {
                return;
            }
            let callback = {
                let guard = match self.callback.lock() {
                    Ok(guard) => guard,
                    Err(_) => return,
                };
                guard.as_ref().map(|obj| obj.clone_ref(py))
            };
            if let Some(callback) = callback {
                if let Err(err) = callback.call1(py, (code, message)) {
                    err.write_unraisable(py, None);
                }
            }
        });
    }
}

#[pyclass]
pub(crate) struct NativeCaptureEngine {
    stream: Arc<native_camera::Stream>,
    runtime: Arc<capture_runtime::Runtime>,
    imu_config: CaptureImuConfig,
    imu: Mutex<Option<Arc<imu::Collector>>>,
    idle_imu: imu_preview::Preview,
    close_timeout: Duration,
    failure_sink: Mutex<Option<Arc<PyFailureSink>>>,
    lifecycle_lock: Mutex<()>,
}

impl NativeCaptureEngine {
    fn disable_active_sink(&self) -> Option<Arc<PyFailureSink>> {
        let guard = self
            .failure_sink
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(sink) = guard.as_ref() {
            sink.disable();
            return Some(Arc::clone(sink));
        }
        None
    }

    fn take_active_sink(&self) -> Option<Arc<PyFailureSink>> {
        self.failure_sink
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }

    fn replace_active_sink(&self, new_sink: Arc<PyFailureSink>) -> Option<Arc<PyFailureSink>> {
        self.failure_sink
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .replace(new_sink)
    }
}

#[pymethods]
impl NativeCaptureEngine {
    #[new]
    fn new(
        py: Python<'_>,
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
        let read_timeout =
            try_duration_from_secs_f64(read_timeout_seconds, "read_timeout_seconds", false)?;
        let imu_timeout =
            try_duration_from_secs_f64(imu_timeout_seconds, "imu_timeout_seconds", false)?;
        let imu_stale_poll_interval =
            try_duration_from_secs_f64(imu_stale_poll_interval, "imu_stale_poll_interval", true)?;
        let close_timeout = read_timeout
            .checked_add(Duration::from_secs(1))
            .ok_or_else(|| {
                PyValueError::new_err(
                    "invalid_argument: read_timeout_seconds is out of Duration range",
                )
            })?;
        let preview_buffer = Arc::clone(&preview.buffer);
        let metrics_arc = metrics.as_ref().map(|item| Arc::clone(&item.metrics));
        let device_for_open = device.clone();
        let encoding_for_open = encoding;
        let (stream, runtime) = py.allow_threads(move || -> PyResult<_> {
            let stream = Arc::new(
                native_camera::Stream::open(native_camera::StreamConfig {
                    device: &device_for_open,
                    width,
                    height,
                    fps,
                    encoding: &encoding_for_open,
                    buffer_count,
                    queue_capacity,
                    split_eyes: false,
                })
                .map_err(camera_error)?,
            );
            let runtime = Arc::new(
                capture_runtime::Runtime::new(
                    Arc::clone(&stream),
                    preview_buffer,
                    frame_decimation,
                    read_timeout,
                    metrics_arc,
                )
                .map_err(capture_runtime_error)?,
            );
            Ok((stream, runtime))
        })?;
        Ok(Self {
            stream,
            runtime,
            imu_config: CaptureImuConfig {
                device,
                unit: imu_unit,
                selector: imu_selector,
                stale_poll_interval: imu_stale_poll_interval,
                timeout: imu_timeout,
            },
            imu: Mutex::new(None),
            idle_imu: imu_preview::Preview::default(),
            close_timeout,
            failure_sink: Mutex::new(None),
            lifecycle_lock: Mutex::new(()),
        })
    }

    fn start_preview(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| {
            self.runtime
                .start_preview()
                .map_err(capture_runtime_error)?;
            let snapshot = self.runtime.snapshot().map_err(capture_runtime_error)?;
            if !snapshot.recording_present {
                self.idle_imu.start(self.imu_config.clone());
            }
            Ok(())
        })
    }

    #[pyo3(signature = (transaction, on_failure, *, recover_source_gaps=false))]
    fn start_recording(
        &self,
        py: Python<'_>,
        transaction: PyRef<'_, NativeSessionTransaction>,
        on_failure: Py<PyAny>,
        recover_source_gaps: bool,
    ) -> PyResult<Py<PyDict>> {
        let tx = transaction.inner()?;
        let new_sink = Arc::new(PyFailureSink::new(on_failure));
        let failure_sink: Arc<dyn FailureSink> = Arc::clone(&new_sink) as Arc<dyn FailureSink>;
        let sink_handle = Arc::clone(&new_sink);
        let imu_config = self.imu_config.clone();
        let close_timeout = self.close_timeout;
        let result = py.allow_threads(
            move || -> Result<(capture_runtime::RuntimeSnapshot, Option<Arc<PyFailureSink>>), PyErr> {
                let _lifecycle = self.lifecycle_lock.lock().map_err(|_| {
                    PyRuntimeError::new_err(
                        "capture_engine_poisoned: capture engine lifecycle mutex is poisoned",
                    )
                })?;
                tx.ensure_recording().map_err(session_store_error)?;
                self.runtime
                    .prepare_recording(close_timeout)
                    .map_err(capture_runtime_error)?;
                self.idle_imu.stop();
                if let Some(stale) = self
                    .imu
                    .lock()
                    .map_err(|_| {
                        self.idle_imu.start(imu_config.clone());
                        PyRuntimeError::new_err("capture_engine_poisoned: IMU mutex is poisoned")
                    })?
                    .take()
                {
                    stale.close();
                }
                let collector = match imu::Collector::open(
                    &imu_config.device,
                    imu_config.unit,
                    imu_config.selector,
                    Some(imu_config.stale_poll_interval),
                ) {
                    Ok(collector) => Arc::new(collector),
                    Err(error) => {
                        self.idle_imu.start(imu_config);
                        return Err(imu_error(error));
                    }
                };
                let snapshot = self
                    .runtime
                    .start_recording(RecordingTarget {
                        active_take: Arc::clone(&tx.active_take),
                        sink: Arc::clone(&tx.sink),
                        encoder: Arc::clone(&tx.encoder),
                        segment_planner: Arc::clone(&tx.segment_planner),
                        recording_start_monotonic_ns: tx.recording_start_monotonic_ns,
                        on_failure: failure_sink,
                        imu: Some(Arc::clone(&collector)),
                        imu_timeout: imu_config.timeout,
                        audio: tx.audio.clone(),
                        capture_stopped_monotonic_ns: Arc::clone(&tx.capture_stopped_monotonic_ns),
                        recover_source_gaps,
                    })
                    .map_err(capture_runtime_error);
                match snapshot {
                    Ok(snapshot) => {
                        match self.imu.lock() {
                            Ok(mut guard) => *guard = Some(collector),
                            Err(_) => {
                                sink_handle.disable();
                                let _ = self.runtime.stop_recording(close_timeout);
                                collector.close();
                                self.idle_imu.start(imu_config);
                                return Err(PyRuntimeError::new_err(
                                    "capture_engine_poisoned: IMU mutex is poisoned",
                                ));
                            }
                        }
                        let previous_sink = self.replace_active_sink(sink_handle);
                        Ok((snapshot, previous_sink))
                    }
                    Err(error) => {
                        collector.close();
                        self.idle_imu.start(imu_config);
                        Err(error)
                    }
                }
            },
        );
        match result {
            Ok((snapshot, previous_sink)) => {
                if let Some(prev) = previous_sink {
                    prev.clear_with_gil(py);
                }
                capture_runtime_snapshot_dict(py, &snapshot)
            }
            Err(error) => {
                new_sink.clear_with_gil(py);
                Err(error)
            }
        }
    }

    #[pyo3(signature = (timeout_seconds=3.0))]
    fn stop_recording(&self, py: Python<'_>, timeout_seconds: f64) -> PyResult<Py<PyDict>> {
        let timeout = try_duration_from_secs_f64(timeout_seconds, "timeout_seconds", false)?;
        let imu_config = self.imu_config.clone();
        let (result, sink) = py.allow_threads(
            move || -> (PyResult<capture_runtime::RuntimeSnapshot>, Option<Arc<PyFailureSink>>) {
                let Ok(_lifecycle) = self.lifecycle_lock.lock() else {
                    return (
                        Err(PyRuntimeError::new_err(
                            "capture_engine_poisoned: capture engine lifecycle mutex is poisoned",
                        )),
                        None,
                    );
                };
                let _ = self.disable_active_sink();
                let outcome = (|| -> PyResult<capture_runtime::RuntimeSnapshot> {
                    let snapshot = self
                        .runtime
                        .stop_recording(timeout)
                        .map_err(capture_runtime_error)?;
                    if let Some(collector) = self
                        .imu
                        .lock()
                        .map_err(|_| {
                            PyRuntimeError::new_err("capture_engine_poisoned: IMU mutex is poisoned")
                        })?
                        .take()
                    {
                        collector.close();
                    }
                    self.idle_imu.start(imu_config);
                    Ok(snapshot)
                })();
                let sink = self.take_active_sink();
                (outcome, sink)
            },
        );
        if let Some(sink) = sink {
            sink.clear_with_gil(py);
        }
        let snapshot = result?;
        capture_runtime_snapshot_dict(py, &snapshot)
    }

    fn latest_imu_observation(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let observation = py.allow_threads(|| -> PyResult<Option<imu::ImuObservation>> {
            let current = self
                .imu
                .lock()
                .map_err(|_| {
                    PyRuntimeError::new_err("capture_engine_poisoned: IMU mutex is poisoned")
                })?
                .as_ref()
                .map(Arc::clone);
            match current {
                Some(collector) => collector.latest_observation().map_err(imu_error),
                None => Ok(self.idle_imu.latest()),
            }
        })?;
        observation
            .as_ref()
            .map(|value| imu_observation_dict(py, value))
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
            return Err(PyValueError::new_err(
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
        let snapshot = py
            .allow_threads(|| self.runtime.snapshot())
            .map_err(capture_runtime_error)?;
        capture_runtime_snapshot_dict(py, &snapshot)
    }

    #[pyo3(signature = (timeout_seconds=5.0))]
    fn close(&self, py: Python<'_>, timeout_seconds: f64) -> PyResult<Py<PyDict>> {
        let timeout = try_duration_from_secs_f64(timeout_seconds, "timeout_seconds", false)?;
        let (result, sink) = py.allow_threads(
            move || -> (PyResult<capture_runtime::RuntimeSnapshot>, Option<Arc<PyFailureSink>>) {
                let Ok(_lifecycle) = self.lifecycle_lock.lock() else {
                    return (
                        Err(PyRuntimeError::new_err(
                            "capture_engine_poisoned: capture engine lifecycle mutex is poisoned",
                        )),
                        None,
                    );
                };
                let _ = self.disable_active_sink();
                self.idle_imu.stop();
                let mut imu_poisoned = false;
                let collector = match self.imu.lock() {
                    Ok(mut guard) => guard.take(),
                    Err(poisoned) => {
                        imu_poisoned = true;
                        poisoned.into_inner().take()
                    }
                };
                if let Some(collector) = collector {
                    collector.close();
                }
                let runtime_result = self.runtime.close(timeout).map_err(capture_runtime_error);
                let sink = self.take_active_sink();
                let outcome = match (runtime_result, imu_poisoned) {
                    (Err(err), _) => Err(err),
                    (Ok(_), true) => Err(PyRuntimeError::new_err(
                        "capture_engine_poisoned: IMU mutex is poisoned",
                    )),
                    (Ok(snapshot), false) => Ok(snapshot),
                };
                (outcome, sink)
            },
        );
        if let Some(sink) = sink {
            sink.clear_with_gil(py);
        }
        let snapshot = result?;
        capture_runtime_snapshot_dict(py, &snapshot)
    }
}

impl Drop for NativeCaptureEngine {
    fn drop(&mut self) {
        let _ = self.disable_active_sink();
        allow_threads_if_gil_held(|| {
            self.idle_imu.stop();
            if let Ok(current) = self.imu.get_mut() {
                if let Some(collector) = current.take() {
                    collector.close();
                }
            }
            let _ = self.runtime.close(self.close_timeout);
        });
        if let Some(sink) = self.take_active_sink() {
            sink.clear_on_drop();
        }
    }
}
