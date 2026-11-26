use crate::hardware::preview;
use crate::py::{preview_error, try_duration_from_secs_f64};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[pyclass]
pub(crate) struct NativePreviewBuffer {
    pub(crate) buffer: Arc<preview::LatestBuffer>,
}

#[pymethods]
impl NativePreviewBuffer {
    #[new]
    fn new(stream_fps: u32) -> PyResult<Self> {
        Ok(Self {
            buffer: Arc::new(preview::LatestBuffer::new(stream_fps).map_err(preview_error)?),
        })
    }

    fn publish(&self, py: Python<'_>, jpeg: &[u8]) -> PyResult<u64> {
        py.allow_threads(|| self.buffer.publish(jpeg))
            .map_err(preview_error)
    }

    fn clear(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| self.buffer.clear())
            .map_err(preview_error)
    }

    fn jpeg(&self, py: Python<'_>) -> PyResult<(u64, Py<PyBytes>)> {
        let snapshot = py
            .allow_threads(|| self.buffer.snapshot())
            .map_err(preview_error)?;
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
        let period = try_duration_from_secs_f64(1.0 / f64::from(effective_fps), "fps", false)?;
        Ok(NativeMultipartPreview {
            buffer: Arc::clone(&self.buffer),
            stop: Arc::new(AtomicBool::new(false)),
            period,
            state: Mutex::new(MultipartPreviewState {
                last_sequence: 0,
                next_delivery: Instant::now(),
            }),
        })
    }

    fn wake_streams(&self, py: Python<'_>) {
        py.allow_threads(|| self.buffer.wake_streams());
    }
}

#[pyclass(frozen)]
pub(crate) struct NativeMultipartPreview {
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
            let (delay, last_sequence) = py.allow_threads(|| -> PyResult<_> {
                let state = self.state.lock().map_err(|_| {
                    pyo3::exceptions::PyRuntimeError::new_err(
                        "preview_stream_poisoned: preview stream mutex is poisoned",
                    )
                })?;
                Ok((
                    (now < state.next_delivery).then(|| state.next_delivery.duration_since(now)),
                    state.last_sequence,
                ))
            })?;
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
            let part = py.allow_threads(|| -> PyResult<Option<Vec<u8>>> {
                let snapshot = match self.buffer.snapshot() {
                    Ok(snapshot) => snapshot,
                    Err(error) if error.code == "preview_unavailable" => return Ok(None),
                    Err(error) => return Err(preview_error(error)),
                };
                {
                    let mut state = self.state.lock().map_err(|_| {
                        pyo3::exceptions::PyRuntimeError::new_err(
                            "preview_stream_poisoned: preview stream mutex is poisoned",
                        )
                    })?;
                    if snapshot.sequence == state.last_sequence {
                        return Ok(None);
                    }
                    state.last_sequence = snapshot.sequence;
                    state.next_delivery = Instant::now() + self.period;
                }
                Ok(Some(preview::multipart_part(&snapshot.jpeg)))
            })?;
            let Some(part) = part else {
                continue;
            };
            return Ok(Some(PyBytes::new(py, &part)));
        }
        Ok(None)
    }

    fn close(&self, py: Python<'_>) {
        self.stop.store(true, Ordering::Release);
        py.allow_threads(|| self.buffer.wake_streams());
    }
}

impl Drop for NativeMultipartPreview {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        crate::py::allow_threads_if_gil_held(|| self.buffer.wake_streams());
    }
}
