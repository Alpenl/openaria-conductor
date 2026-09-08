use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
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
    pub(crate) jpeg: Arc<[u8]>,
}

struct Frame {
    sequence: u64,
    jpeg: Arc<[u8]>,
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

    pub(crate) fn publish(&self, jpeg: &[u8]) -> Result<u64, PreviewError> {
        if jpeg.is_empty() {
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
        state.latest = Some(Frame {
            sequence,
            jpeg: Arc::from(jpeg),
        });
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

    pub(crate) fn snapshot(&self) -> Result<Snapshot, PreviewError> {
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
            jpeg: Arc::clone(&frame.jpeg),
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

pub(crate) fn multipart_part(jpeg: &[u8]) -> Vec<u8> {
    let header = format!(
        "--{MULTIPART_BOUNDARY}\r\nContent-Type: image/jpeg\r\nContent-Length: {}\r\n\r\n",
        jpeg.len()
    );
    let trailer = b"\r\n";
    let mut payload = Vec::with_capacity(header.len() + jpeg.len() + trailer.len());
    payload.extend_from_slice(header.as_bytes());
    payload.extend_from_slice(jpeg);
    payload.extend_from_slice(trailer);
    payload
}
