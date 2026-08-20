use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

pub(crate) const MULTIPART_BOUNDARY: &str = "ylx-preview";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreviewError {
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

impl PreviewError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

pub(crate) struct Snapshot {
    pub(crate) sequence: u64,
    pub(crate) jpeg: Py<PyBytes>,
}

struct Frame {
    sequence: u64,
    jpeg: Py<PyBytes>,
}

struct State {
    sequence: u64,
    latest: Option<Frame>,
}

pub(crate) struct LatestBuffer {
    stream_fps: u32,
    state: Mutex<State>,
    changed: Condvar,
}

impl LatestBuffer {
    pub(crate) fn new(stream_fps: u32) -> Result<Self, PreviewError> {
        if stream_fps < 1 {
            return Err(PreviewError::new(
                "invalid_argument",
                "stream_fps must be at least 1",
            ));
        }
        Ok(Self {
            stream_fps,
            state: Mutex::new(State {
                sequence: 0,
                latest: None,
            }),
            changed: Condvar::new(),
        })
    }

    pub(crate) fn stream_fps(&self) -> u32 {
        self.stream_fps
    }

    pub(crate) fn publish(&self, py: Python<'_>, jpeg: Py<PyBytes>) -> Result<u64, PreviewError> {
        if jpeg.as_bytes(py).is_empty() {
            return Err(PreviewError::new(
                "invalid_argument",
                "preview JPEG must be non-empty bytes",
            ));
        }
        let mut state = self.state.lock().map_err(|_| {
            PreviewError::new(
                "preview_buffer_poisoned",
                "preview buffer mutex is poisoned",
            )
        })?;
        let sequence = state.sequence.checked_add(1).ok_or_else(|| {
            PreviewError::new("sequence_overflow", "preview sequence number overflowed")
        })?;
        state.sequence = sequence;
        state.latest = Some(Frame { sequence, jpeg });
        self.changed.notify_all();
        Ok(sequence)
    }

    pub(crate) fn clear(&self) -> Result<(), PreviewError> {
        let mut state = self.state.lock().map_err(|_| {
            PreviewError::new(
                "preview_buffer_poisoned",
                "preview buffer mutex is poisoned",
            )
        })?;
        state.latest = None;
        self.changed.notify_all();
        Ok(())
    }

    pub(crate) fn snapshot(&self, py: Python<'_>) -> Result<Snapshot, PreviewError> {
        let state = self.state.lock().map_err(|_| {
            PreviewError::new(
                "preview_buffer_poisoned",
                "preview buffer mutex is poisoned",
            )
        })?;
        let frame = state.latest.as_ref().ok_or_else(|| {
            PreviewError::new(
                "preview_unavailable",
                "no preview frame is currently available",
            )
        })?;
        Ok(Snapshot {
            sequence: frame.sequence,
            jpeg: frame.jpeg.clone_ref(py),
        })
    }

    pub(crate) fn wait_after(
        &self,
        last_sequence: u64,
        stop: &AtomicBool,
        timeout: Duration,
    ) -> Result<(), PreviewError> {
        let state = self.state.lock().map_err(|_| {
            PreviewError::new(
                "preview_buffer_poisoned",
                "preview buffer mutex is poisoned",
            )
        })?;
        let _unused = self
            .changed
            .wait_timeout_while(state, timeout, |state| {
                !stop.load(Ordering::Acquire)
                    && state
                        .latest
                        .as_ref()
                        .is_none_or(|frame| frame.sequence == last_sequence)
            })
            .map_err(|_| {
                PreviewError::new(
                    "preview_buffer_poisoned",
                    "preview buffer mutex is poisoned",
                )
            })?;
        Ok(())
    }

    pub(crate) fn wait_until_stop(
        &self,
        stop: &AtomicBool,
        timeout: Duration,
    ) -> Result<(), PreviewError> {
        let state = self.state.lock().map_err(|_| {
            PreviewError::new(
                "preview_buffer_poisoned",
                "preview buffer mutex is poisoned",
            )
        })?;
        let _unused = self
            .changed
            .wait_timeout_while(state, timeout, |_| !stop.load(Ordering::Acquire))
            .map_err(|_| {
                PreviewError::new(
                    "preview_buffer_poisoned",
                    "preview buffer mutex is poisoned",
                )
            })?;
        Ok(())
    }

    pub(crate) fn wake_streams(&self) {
        self.changed.notify_all();
    }
}

pub(crate) fn multipart_part<'py>(
    py: Python<'py>,
    jpeg: &Py<PyBytes>,
) -> PyResult<Bound<'py, PyBytes>> {
    let payload = jpeg.as_bytes(py);
    let header = format!(
        "--{MULTIPART_BOUNDARY}\r\nContent-Type: image/jpeg\r\nContent-Length: {}\r\n\r\n",
        payload.len()
    );
    let trailer = b"\r\n";
    PyBytes::new_with(py, header.len() + payload.len() + trailer.len(), |target| {
        let mut cursor = 0;
        target[cursor..cursor + header.len()].copy_from_slice(header.as_bytes());
        cursor += header.len();
        target[cursor..cursor + payload.len()].copy_from_slice(payload);
        cursor += payload.len();
        target[cursor..cursor + trailer.len()].copy_from_slice(trailer);
        Ok(())
    })
}
