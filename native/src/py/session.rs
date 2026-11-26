use crate::py::convert::{
    device_session_seal_result_dict, expected_artifact_identities, file_digest_dict,
    file_identity_dict, recording_outcome_dict, recording_segment_boundary_dict,
    session_transaction_snapshot_dict, stereo_encoder_segment_list,
};
use crate::py::{
    allow_threads_if_gil_held, session_io_error, session_store_error, try_duration_from_secs_f64,
};
use crate::session::{
    catalog_admission, catalog_signature, schema_validation, session_io, session_store,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::sync::Arc;
use std::sync::atomic::Ordering;

#[pyclass]
pub(crate) struct NativeSchemaValidator {
    validator: schema_validation::SchemaValidator,
}

#[pymethods]
impl NativeSchemaValidator {
    #[new]
    fn new(py: Python<'_>, schema_json: &str) -> PyResult<Self> {
        let validator = py
            .allow_threads(|| schema_validation::SchemaValidator::new(schema_json))
            .map_err(PyValueError::new_err)?;
        Ok(Self { validator })
    }

    fn is_valid(&self, py: Python<'_>, payload: &str) -> PyResult<bool> {
        py.allow_threads(|| self.validator.is_valid(payload))
            .map_err(PyValueError::new_err)
    }
}

#[pyclass]
pub(crate) struct NativeSessionTransaction {
    transaction: Option<Arc<session_store::SessionTransaction>>,
}

impl NativeSessionTransaction {
    pub(crate) fn inner(&self) -> PyResult<Arc<session_store::SessionTransaction>> {
        self.transaction.as_ref().map(Arc::clone).ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "invalid_state: session transaction has been dropped",
            )
        })
    }
}

impl Drop for NativeSessionTransaction {
    fn drop(&mut self) {
        if let Some(transaction) = self.transaction.take() {
            allow_threads_if_gil_held(move || drop(transaction));
        }
    }
}

#[pymethods]
impl NativeSessionTransaction {
    #[getter]
    fn recording_start_monotonic_ns(&self) -> PyResult<u64> {
        Ok(self.inner()?.recording_start_monotonic_ns)
    }

    #[getter]
    fn capture_stopped_monotonic_ns(&self) -> PyResult<Option<u64>> {
        let value = self
            .inner()?
            .capture_stopped_monotonic_ns
            .load(Ordering::Acquire);
        Ok((value != 0).then_some(value))
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let transaction = self.inner()?;
        let snapshot = py
            .allow_threads(move || transaction.snapshot())
            .map_err(session_store_error)?;
        session_transaction_snapshot_dict(py, &snapshot)
    }

    fn diagnostic_snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let transaction = self.inner()?;
        let snapshot = py
            .allow_threads(move || transaction.diagnostic_snapshot())
            .map_err(session_store_error)?;
        session_transaction_snapshot_dict(py, &snapshot)
    }

    fn segments(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let transaction = self.inner()?;
        let segments = py
            .allow_threads(move || transaction.segments())
            .map_err(session_store_error)?;
        stereo_encoder_segment_list(py, &segments)
    }

    fn segments_since(&self, py: Python<'_>, offset: usize) -> PyResult<Py<PyList>> {
        let transaction = self.inner()?;
        let segments = py
            .allow_threads(move || transaction.segments_since(offset))
            .map_err(session_store_error)?;
        stereo_encoder_segment_list(py, &segments)
    }

    fn request_audio_stop(&self, py: Python<'_>) -> PyResult<()> {
        let transaction = self.inner()?;
        py.allow_threads(move || transaction.request_audio_stop());
        Ok(())
    }

    #[pyo3(signature = (duration_seconds, timeout_seconds=30.0))]
    fn finish(
        &self,
        py: Python<'_>,
        duration_seconds: f64,
        timeout_seconds: f64,
    ) -> PyResult<Py<PyDict>> {
        let duration = try_duration_from_secs_f64(duration_seconds, "duration_seconds", true)?;
        let timeout = try_duration_from_secs_f64(timeout_seconds, "timeout_seconds", false)?;
        let transaction = self.inner()?;
        let outcome = py
            .allow_threads(move || transaction.finish(duration, timeout))
            .map_err(session_store_error)?;
        recording_outcome_dict(py, &outcome)
    }

    fn boundary(
        &self,
        py: Python<'_>,
        ordinal: u64,
        duration_seconds: f64,
    ) -> PyResult<Py<PyDict>> {
        let duration = try_duration_from_secs_f64(duration_seconds, "duration_seconds", true)?;
        let transaction = self.inner()?;
        let boundary = py
            .allow_threads(move || transaction.boundary(ordinal, duration))
            .map_err(session_store_error)?;
        recording_segment_boundary_dict(py, &boundary)
    }

    fn abort(&self, py: Python<'_>, _reason: &str) -> PyResult<()> {
        let transaction = self.inner()?;
        py.allow_threads(move || transaction.abort());
        Ok(())
    }

    fn open_handle_count(&self, py: Python<'_>) -> PyResult<u64> {
        let transaction = self.inner()?;
        Ok(py.allow_threads(move || transaction.open_handle_count()))
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
        let transaction = self.inner()?;
        let partial_path = std::path::PathBuf::from(partial_path);
        let final_path = std::path::PathBuf::from(final_path);
        let session_id = session_id.to_owned();
        let manifest = manifest.to_vec();
        let expected_identities = expected_artifact_identities(expected_identities)?;
        let control_names = control_names
            .unwrap_or_else(|| vec!["recording.json".to_owned(), "capture.json".to_owned()]);
        let result = py.allow_threads(move || {
            transaction.ensure_finished().map_err(session_store_error)?;
            session_io::seal_device_session(
                &partial_path,
                &final_path,
                &manifest,
                &session_id,
                &expected_identities,
                &control_names,
            )
            .map_err(session_io_error)
        })?;
        device_session_seal_result_dict(py, &result)
    }
}

#[pyclass]
pub(crate) struct NativeSessionStore;

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
        let encoder_arguments = plan
            .getattr("encoder_arguments")?
            .extract::<Vec<String>>()?;
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
                    encoder_arguments: &encoder_arguments,
                    recording_start_monotonic_ns,
                    audio,
                })
            })
            .map_err(session_store_error)?;
        Ok(NativeSessionTransaction {
            transaction: Some(Arc::new(transaction)),
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

    fn verify_fd_interruptible(
        &self,
        py: Python<'_>,
        descriptor: i32,
        expected_bytes: u64,
        expected_sha256: &str,
        interrupt_check: Py<PyAny>,
    ) -> PyResult<Py<PyDict>> {
        let mut callback_error = None;
        let result = py.allow_threads(|| {
            session_io::verify_fd_interruptible(descriptor, expected_bytes, expected_sha256, || {
                match Python::with_gil(|py| interrupt_check.call0(py)) {
                    Ok(_) => Ok(()),
                    Err(error) => {
                        callback_error = Some(error);
                        Err(session_io::SessionIoError {
                            code: "verification_changed",
                            message: "artifact verification interrupted".to_owned(),
                        })
                    }
                }
            })
        });
        if let Some(error) = callback_error {
            return Err(error);
        }
        file_identity_dict(py, &result.map_err(session_io_error)?)
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
        // SAFETY: open_verified_artifact returns a newly opened, verified file descriptor.
        let owned_fd = unsafe { OwnedFd::from_raw_fd(artifact.descriptor) };
        let result = PyDict::new(py);
        result.set_item("descriptor", owned_fd.as_raw_fd())?;
        result.set_item("identity", file_identity_dict(py, &artifact.identity)?)?;
        let _ = owned_fd.into_raw_fd();
        Ok(result.unbind())
    }

    fn catalog_manifest_signature(
        &self,
        py: Python<'_>,
        path: &[u8],
        maximum_bytes: usize,
    ) -> PyResult<String> {
        use std::os::unix::ffi::OsStrExt;
        let path = std::path::PathBuf::from(std::ffi::OsStr::from_bytes(path));
        py.allow_threads(move || catalog_signature::signature(&path, maximum_bytes))
            .map_err(|error| {
                pyo3::exceptions::PyOSError::new_err((
                    error.raw_os_error().unwrap_or(libc::EIO),
                    error.to_string(),
                ))
            })
    }

    fn catalog_artifacts_match(
        &self,
        py: Python<'_>,
        path: &[u8],
        artifacts: Vec<(Vec<u8>, i64)>,
    ) -> PyResult<bool> {
        use std::os::unix::ffi::OsStrExt;
        let path = std::path::PathBuf::from(std::ffi::OsStr::from_bytes(path));
        py.allow_threads(move || catalog_admission::matches(&path, &artifacts))
            .map_err(|error| {
                pyo3::exceptions::PyOSError::new_err((
                    error.raw_os_error().unwrap_or(libc::EIO),
                    error.to_string(),
                ))
            })
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
