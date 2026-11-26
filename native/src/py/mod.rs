pub(crate) mod capture;
pub(crate) mod convert;
pub(crate) mod metrics;
pub(crate) mod preview;
pub(crate) mod session;

use crate::audio;
use crate::hardware::{
    imu, jpeg, metrics as hw_metrics, native_camera, preview as hw_preview, turbojpeg, v4l2,
};
use crate::py::convert::{exposure_config_dict, focus_status_dict};
use crate::session::{audio_checkpoint, capture_runtime, frame_stream, session_io, session_store};
use pyo3::exceptions::{PyBrokenPipeError, PyConnectionResetError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(crate) const NATIVE_ABI: u32 = 6;
pub(crate) const CAPABILITY_PROBE: &str = "capability_probe";
pub(crate) const JPEG_CONTRACT: &str = "jpeg_contract";
pub(crate) const FRAME_STREAM: &str = "frame_stream";
pub(crate) const TURBOJPEG_SPLIT: &str = "turbojpeg_split";
pub(crate) const V4L2_FOCUS_CONTROL: &str = "v4l2_focus_control";
pub(crate) const CAPTURE_ENGINE: &str = "capture_engine";
pub(crate) const NATIVE_AUDIO: &str = "native_audio";
pub(crate) const SESSION_STORE: &str = "session_store";
pub(crate) const SCHEMA_VALIDATION: &str = "schema_validation";
pub(crate) const PREVIEW_BUFFER: &str = "preview_buffer";
pub(crate) const PERFORMANCE_METRICS: &str = "performance_metrics";

unsafe extern "C" {
    // PyO3 omits PyGILState_Check under `abi3-py311`, though CPython exports it on 3.4+.
    fn PyGILState_Check() -> std::os::raw::c_int;
}

pub(crate) fn allow_threads_if_gil_held<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send,
    R: Send,
{
    // SAFETY: Py_IsInitialized and PyGILState_Check are read-only C-API queries;
    // PyGILState_Check is only valid while the interpreter is initialized.
    let gil_held = unsafe { pyo3::ffi::Py_IsInitialized() != 0 && PyGILState_Check() != 0 };
    if gil_held {
        Python::with_gil(|py| py.allow_threads(f))
    } else {
        f()
    }
}

pub(crate) fn try_duration_from_secs_f64(
    seconds: f64,
    label: &str,
    allow_zero: bool,
) -> PyResult<Duration> {
    if !seconds.is_finite() || (allow_zero && seconds < 0.0) || (!allow_zero && seconds <= 0.0) {
        let qualifier = if allow_zero {
            "non-negative"
        } else {
            "positive"
        };
        return Err(PyValueError::new_err(format!(
            "invalid_argument: {label} must be finite and {qualifier}"
        )));
    }
    Duration::try_from_secs_f64(seconds).map_err(|_| {
        PyValueError::new_err(format!(
            "invalid_argument: {label} is out of Duration range"
        ))
    })
}

fn coded_runtime_error(code: &str, message: &str) -> PyErr {
    PyRuntimeError::new_err(format!("{code}: {message}"))
}

pub(crate) fn native_error(error: turbojpeg::TurboJpegError) -> PyErr {
    coded_runtime_error(error.code, &error.message)
}

pub(crate) fn camera_error(error: native_camera::StreamError) -> PyErr {
    coded_runtime_error(error.code, &error.message)
}

pub(crate) fn v4l2_error(error: v4l2::CaptureError) -> PyErr {
    coded_runtime_error(error.code, &error.message)
}

pub(crate) fn imu_error(error: imu::ImuError) -> PyErr {
    coded_runtime_error(error.code, &error.message)
}

pub(crate) fn session_io_error(error: session_io::SessionIoError) -> PyErr {
    let message = format!("{}: {}", error.code, error.message);
    match error.code {
        "send_broken_pipe" => PyBrokenPipeError::new_err((libc::EPIPE, message)),
        "send_connection_reset" => PyConnectionResetError::new_err((libc::ECONNRESET, message)),
        _ => PyRuntimeError::new_err(message),
    }
}

pub(crate) fn session_store_error(error: session_store::StoreError) -> PyErr {
    coded_runtime_error(&error.code, &error.message)
}

pub(crate) fn preview_error(error: hw_preview::PreviewError) -> PyErr {
    coded_runtime_error(error.code, &error.message)
}

pub(crate) fn capture_runtime_error(error: capture_runtime::RuntimeError) -> PyErr {
    coded_runtime_error(error.code.as_ref(), &error.message)
}

pub(crate) fn metrics_error(error: hw_metrics::MetricsError) -> PyErr {
    if error.code == "invalid_argument" {
        PyValueError::new_err(format!("{}: {}", error.code, error.message))
    } else {
        coded_runtime_error(error.code, &error.message)
    }
}

#[pyclass]
pub(crate) struct NativeSplitter {
    handle: Arc<Mutex<turbojpeg::TransformHandle>>,
}

#[pymethods]
impl NativeSplitter {
    #[new]
    fn new(py: Python<'_>) -> PyResult<Self> {
        let handle = py
            .allow_threads(turbojpeg::TransformHandle::open)
            .map_err(native_error)?;
        Ok(Self {
            handle: Arc::new(Mutex::new(handle)),
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

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| {
            self.handle
                .lock()
                .map_err(|_| PyRuntimeError::new_err("native_splitter_poisoned"))?
                .close();
            Ok(())
        })
    }
}

#[pyfunction]
fn capabilities(py: Python<'_>) -> PyResult<Py<PyDict>> {
    let (has_turbojpeg, has_imu, has_audio) =
        py.allow_threads(|| (turbojpeg::available(), imu::available(), audio::available()));
    let result = PyDict::new(py);
    result.set_item("module_version", env!("CARGO_PKG_VERSION"))?;
    result.set_item("abi", NATIVE_ABI)?;
    let mut features = vec![
        CAPABILITY_PROBE,
        JPEG_CONTRACT,
        FRAME_STREAM,
        SESSION_STORE,
        SCHEMA_VALIDATION,
        PREVIEW_BUFFER,
        PERFORMANCE_METRICS,
        V4L2_FOCUS_CONTROL,
    ];
    if has_turbojpeg {
        features.push(TURBOJPEG_SPLIT);
    }
    if has_imu {
        features.push(CAPTURE_ENGINE);
    }
    if has_audio {
        features.push(NATIVE_AUDIO);
    }
    result.set_item("features", features)?;
    Ok(result.unbind())
}

#[pyfunction]
fn jpeg_metadata(py: Python<'_>, payload: &[u8]) -> PyResult<Py<PyDict>> {
    let (ranges, dimensions) =
        py.allow_threads(|| (jpeg::ranges(payload), jpeg::dimensions(payload)));
    let result = PyDict::new(py);
    result.set_item("ranges", ranges)?;
    result.set_item("dimensions", dimensions)?;
    Ok(result.unbind())
}

#[pyfunction]
fn encode_frame<'py>(py: Python<'py>, payload: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    let encoded = py
        .allow_threads(|| frame_stream::encode(payload))
        .map_err(|_| PyValueError::new_err("invalid_frame_length"))?;
    Ok(PyBytes::new(py, &encoded))
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

#[pyfunction]
#[pyo3(signature = (device, absolute=None, auto=false))]
fn v4l2_configure_exposure(
    py: Python<'_>,
    device: &str,
    absolute: Option<i32>,
    auto: bool,
) -> PyResult<Py<PyDict>> {
    let config = py
        .allow_threads(|| v4l2::configure_exposure(device, absolute, auto))
        .map_err(v4l2_error)?;
    exposure_config_dict(py, config)
}

#[pyfunction]
fn audio_checkpoint_segments(py: Python<'_>, payload: &[u8]) -> PyResult<Py<PyList>> {
    // bytes is immutable and borrowed for this call; parsing need not hold GIL.
    let segments = py
        .allow_threads(|| audio_checkpoint::segments(payload))
        .map_err(PyValueError::new_err)?;
    let result = PyList::empty(py);
    for segment in segments {
        let record = PyDict::new(py);
        record.set_item("index", segment.index)?;
        record.set_item("path", segment.path)?;
        result.append(record)?;
    }
    Ok(result.unbind())
}

pub(crate) fn register_module(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("NATIVE_ABI", NATIVE_ABI)?;
    module.add("NATIVE_VERSION", env!("CARGO_PKG_VERSION"))?;
    module.add(
        "FRAME_STREAM_MAGIC",
        PyBytes::new(module.py(), frame_stream::MAGIC),
    )?;
    module.add_function(wrap_pyfunction!(capabilities, module)?)?;
    module.add_function(wrap_pyfunction!(audio_checkpoint_segments, module)?)?;
    module.add_function(wrap_pyfunction!(jpeg_metadata, module)?)?;
    module.add_function(wrap_pyfunction!(encode_frame, module)?)?;
    module.add_function(wrap_pyfunction!(v4l2_focus_status, module)?)?;
    module.add_function(wrap_pyfunction!(v4l2_set_focus, module)?)?;
    module.add_function(wrap_pyfunction!(v4l2_configure_exposure, module)?)?;
    module.add_class::<NativeSplitter>()?;
    module.add_class::<capture::NativeCaptureEngine>()?;
    module.add_class::<session::NativeSessionStore>()?;
    module.add_class::<session::NativeSchemaValidator>()?;
    module.add_class::<session::NativeSessionTransaction>()?;
    module.add_class::<preview::NativePreviewBuffer>()?;
    module.add_class::<preview::NativeMultipartPreview>()?;
    module.add_class::<metrics::NativePerformanceMetrics>()?;
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
        assert_eq!(NATIVE_ABI, 6);
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
