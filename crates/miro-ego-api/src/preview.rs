//! Zero-copy MJPEG (`GET /api/v1/preview`) and H.264 fMP4 / Annex-B (`GET /api/v1/preview/h264`)
//! preview fanout hubs using `tokio::sync::watch` and `tokio::sync::broadcast` of `Arc<Bytes>`.

use crate::error::{ApiError, ApiResult};
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, RwLock, broadcast, watch};

pub const MJPEG_BOUNDARY: &str = "miro-ego-preview";
pub const MJPEG_CONTENT_TYPE: &str = "multipart/x-mixed-replace; boundary=miro-ego-preview";

/// One immutable latest-only JPEG preview frame shared zero-copy across all connected viewers.
#[derive(Debug, Clone)]
pub struct PreviewFrame {
    pub sequence: u64,
    pub jpeg: Arc<Bytes>,
}

/// Latest-only MJPEG fanout hub backed by `tokio::sync::watch`.
///
/// Publishing never blocks or backpressures capture: slow viewers automatically skip
/// intermediate frames and always receive the newest available `Arc<Bytes>` frame.
pub struct MjpegPreviewHub {
    max_fps: u32,
    seq: AtomicU64,
    tx: watch::Sender<Option<PreviewFrame>>,
}

impl MjpegPreviewHub {
    pub fn new(max_fps: u32) -> Self {
        let (tx, _) = watch::channel(None);
        Self {
            max_fps: max_fps.max(1),
            seq: AtomicU64::new(0),
            tx,
        }
    }

    pub fn max_fps(&self) -> u32 {
        self.max_fps
    }

    /// Publish a new JPEG frame (shared across all subscribers via `Arc<Bytes>`).
    pub fn publish(&self, jpeg: Bytes) -> ApiResult<u64> {
        if jpeg.is_empty() {
            return Err(ApiError::bad_request("preview JPEG must be non-empty"));
        }
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let frame = PreviewFrame {
            sequence: seq,
            jpeg: Arc::new(jpeg),
        };
        self.tx.send_replace(Some(frame));
        Ok(seq)
    }

    pub fn clear(&self) {
        self.tx.send_replace(None);
    }

    pub fn latest(&self) -> Option<PreviewFrame> {
        self.tx.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<Option<PreviewFrame>> {
        self.tx.subscribe()
    }
}

/// Format one multipart MJPEG part with `miro-ego-preview` boundary.
pub fn encode_mjpeg_part(jpeg: &[u8]) -> Bytes {
    let header = format!(
        "--{MJPEG_BOUNDARY}\r\nContent-Type: image/jpeg\r\nContent-Length: {}\r\n\r\n",
        jpeg.len()
    );
    let mut buf = BytesMut::with_capacity(header.len() + jpeg.len() + 2);
    buf.put_slice(header.as_bytes());
    buf.put_slice(jpeg);
    buf.put_slice(b"\r\n");
    buf.freeze()
}

/// Build an async zero-copy MJPEG `multipart/x-mixed-replace` stream.
///
/// Holds the preview concurrency `permit` until the viewer disconnects or the stream ends.
pub fn mjpeg_stream_body(
    hub: Arc<MjpegPreviewHub>,
    requested_fps: Option<u32>,
    initial_frame: PreviewFrame,
    permit: OwnedSemaphorePermit,
) -> axum::body::Body {
    let effective_fps = requested_fps
        .map(|f| f.clamp(1, hub.max_fps()))
        .unwrap_or_else(|| hub.max_fps());
    let min_period = Duration::from_secs_f64(1.0 / f64::from(effective_fps));
    let mut rx = hub.subscribe();
    let (tx, out_rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(4);

    tokio::spawn(async move {
        let _permit = permit;
        let mut last_seq = initial_frame.sequence;
        if tx
            .send(Ok(encode_mjpeg_part(&initial_frame.jpeg)))
            .await
            .is_err()
        {
            return;
        }
        let mut next_allowed = tokio::time::Instant::now() + min_period;

        loop {
            tokio::select! {
                biased;
                _ = tx.closed() => return,
                changed = rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let now = tokio::time::Instant::now();
                    if now < next_allowed {
                        tokio::select! {
                            _ = tx.closed() => return,
                            _ = tokio::time::sleep_until(next_allowed) => {}
                        }
                    }
                    let maybe_frame = rx.borrow_and_update().clone();
                    let Some(frame) = maybe_frame else {
                        continue;
                    };
                    if frame.sequence == last_seq {
                        continue;
                    }
                    last_seq = frame.sequence;
                    next_allowed = tokio::time::Instant::now() + min_period;
                    if tx.send(Ok(encode_mjpeg_part(&frame.jpeg))).await.is_err() {
                        return;
                    }
                }
            }
        }
    });

    axum::body::Body::from_stream(PreviewByteStream(out_rx))
}

/// Low-bandwidth H.264 fragmented MP4 (`video/mp4`) or Annex-B (`video/h264`) preview hub.
pub struct H264PreviewHub {
    content_type: String,
    init_segment: RwLock<Option<Arc<Bytes>>>,
    last_hints: std::sync::Mutex<(Option<u32>, Option<u32>)>,
    tx: broadcast::Sender<Arc<Bytes>>,
}

impl H264PreviewHub {
    pub fn new_fmp4(capacity: usize) -> Self {
        Self::with_content_type("video/mp4", capacity)
    }

    pub fn new_annex_b(capacity: usize) -> Self {
        Self::with_content_type("video/h264", capacity)
    }

    pub fn with_content_type(content_type: impl Into<String>, capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity.max(16));
        Self {
            content_type: content_type.into(),
            init_segment: RwLock::new(None),
            last_hints: std::sync::Mutex::new((None, None)),
            tx,
        }
    }

    pub fn content_type(&self) -> &str {
        &self.content_type
    }

    /// Record optional caller-requested `(fps, bitrate_kbps)` upper-bound hints.
    pub fn record_stream_hints(&self, fps: Option<u32>, bitrate_kbps: Option<u32>) {
        if let Ok(mut guard) = self.last_hints.lock() {
            *guard = (fps, bitrate_kbps);
        }
    }

    /// Return the most recent caller-requested `(fps, bitrate_kbps)` stream hints.
    pub fn last_stream_hints(&self) -> (Option<u32>, Option<u32>) {
        self.last_hints.lock().map(|g| *g).unwrap_or((None, None))
    }

    /// Store or update the initialization segment (e.g. fMP4 `ftyp+moov` or SPS/PPS header).
    pub async fn set_init_segment(&self, segment: Bytes) {
        let mut guard = self.init_segment.write().await;
        *guard = if segment.is_empty() {
            None
        } else {
            Some(Arc::new(segment))
        };
    }

    /// Synchronously set the initialization segment (for setup/tests).
    pub fn set_init_segment_blocking(&self, segment: Bytes) {
        if let Ok(mut guard) = self.init_segment.try_write() {
            *guard = if segment.is_empty() {
                None
            } else {
                Some(Arc::new(segment))
            };
        }
    }

    pub async fn init_segment(&self) -> Option<Arc<Bytes>> {
        self.init_segment.read().await.clone()
    }

    /// Broadcast one H.264 media fragment (`moof+mdat` or Annex-B frame) zero-copy across viewers.
    pub fn publish_fragment(&self, fragment: Bytes) -> usize {
        if fragment.is_empty() {
            return 0;
        }
        self.tx.send(Arc::new(fragment)).unwrap_or(0)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Bytes>> {
        self.tx.subscribe()
    }
}

/// Build an async streaming `axum::body::Body` for `GET /api/v1/preview/h264`.
pub fn h264_stream_body(
    init_segment: Option<Arc<Bytes>>,
    mut rx: broadcast::Receiver<Arc<Bytes>>,
    permit: OwnedSemaphorePermit,
) -> axum::body::Body {
    let (tx, out_rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(16);
    tokio::spawn(async move {
        let _permit = permit;
        if let Some(init) = init_segment
            && tx.send(Ok((*init).clone())).await.is_err()
        {
            return;
        }
        loop {
            tokio::select! {
                biased;
                _ = tx.closed() => return,
                msg = rx.recv() => {
                    match msg {
                        Ok(fragment) => {
                            if tx.send(Ok((*fragment).clone())).await.is_err() {
                                return;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            // Slow viewer dropped intermediate fragments; never backpressure capture.
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                    }
                }
            }
        }
    });
    axum::body::Body::from_stream(PreviewByteStream(out_rx))
}

struct PreviewByteStream(tokio::sync::mpsc::Receiver<Result<Bytes, std::io::Error>>);

impl Stream for PreviewByteStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}
