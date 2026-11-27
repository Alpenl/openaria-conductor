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
use tokio::sync::{OwnedSemaphorePermit, broadcast, watch};

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
/// Holds the preview concurrency `permit` until the viewer disconnects, `shutdown_rx` triggers,
/// or the stream ends.
pub fn mjpeg_stream_body(
    hub: Arc<MjpegPreviewHub>,
    requested_fps: Option<u32>,
    initial_frame: PreviewFrame,
    permit: OwnedSemaphorePermit,
    mut shutdown_rx: watch::Receiver<bool>,
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
        let initial_part = encode_mjpeg_part(&initial_frame.jpeg);
        tokio::select! {
            biased;
            _ = crate::sse::wait_for_shutdown(&mut shutdown_rx) => return,
            res = tx.send(Ok(initial_part)) => {
                if res.is_err() {
                    return;
                }
            }
        }
        let mut next_allowed = tokio::time::Instant::now() + min_period;

        loop {
            tokio::select! {
                biased;
                _ = crate::sse::wait_for_shutdown(&mut shutdown_rx) => return,
                _ = tx.closed() => return,
                changed = rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let now = tokio::time::Instant::now();
                    if now < next_allowed {
                        tokio::select! {
                            biased;
                            _ = crate::sse::wait_for_shutdown(&mut shutdown_rx) => return,
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
                    let part = encode_mjpeg_part(&frame.jpeg);
                    tokio::select! {
                        biased;
                        _ = crate::sse::wait_for_shutdown(&mut shutdown_rx) => return,
                        res = tx.send(Ok(part)) => {
                            if res.is_err() {
                                return;
                            }
                        }
                    }
                }
            }
        }
    });

    axum::body::Body::from_stream(PreviewByteStream(out_rx))
}

const MAX_H264_GOP_FRAGMENTS: usize = 120;
const PREVIEW_SEND_STALL_TIMEOUT: Duration = Duration::from_secs(30);

/// One H.264 media fragment carried over the preview broadcast channel.
#[derive(Debug, Clone)]
pub struct H264Fragment {
    pub bytes: Arc<Bytes>,
    pub is_keyframe: bool,
}

#[derive(Default)]
struct H264HubState {
    init_segment: Option<Arc<Bytes>>,
    gop_fragments: Vec<Arc<Bytes>>,
}

/// Low-bandwidth H.264 fragmented MP4 (`video/mp4`) or Annex-B (`video/h264`) preview hub.
pub struct H264PreviewHub {
    content_type: String,
    state: std::sync::Mutex<H264HubState>,
    last_hints: std::sync::Mutex<(Option<u32>, Option<u32>)>,
    tx: broadcast::Sender<H264Fragment>,
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
            state: std::sync::Mutex::new(H264HubState::default()),
            last_hints: std::sync::Mutex::new((None, None)),
            tx,
        }
    }

    pub fn content_type(&self) -> &str {
        &self.content_type
    }

    /// Record optional caller-requested `(fps, bitrate_kbps)` upper-bound hints without
    /// clobbering active non-empty hints with `(None, None)`.
    pub fn record_stream_hints(&self, fps: Option<u32>, bitrate_kbps: Option<u32>) {
        if fps.is_none() && bitrate_kbps.is_none() {
            return;
        }
        if let Ok(mut guard) = self.last_hints.lock() {
            if let Some(f) = fps {
                guard.0 = Some(f);
            }
            if let Some(b) = bitrate_kbps {
                guard.1 = Some(b);
            }
        }
    }

    /// Return the most recent caller-requested `(fps, bitrate_kbps)` stream hints.
    pub fn last_stream_hints(&self) -> (Option<u32>, Option<u32>) {
        self.last_hints.lock().map(|g| *g).unwrap_or((None, None))
    }

    /// Store or update the initialization segment (e.g. fMP4 `ftyp+moov` or SPS/PPS header).
    pub async fn set_init_segment(&self, segment: Bytes) {
        self.set_init_segment_blocking(segment);
    }

    /// Synchronously set the initialization segment (for setup/tests), clearing cached GOP
    /// fragments whenever `init_segment` changes so viewers never mix mismatched SPS/PPS state.
    pub fn set_init_segment_blocking(&self, segment: Bytes) {
        if let Ok(mut state) = self.state.lock() {
            let next = if segment.is_empty() {
                None
            } else {
                Some(Arc::new(segment))
            };
            let changed = match (&state.init_segment, &next) {
                (None, None) => false,
                (Some(prev), Some(curr)) => prev.as_ref() != curr.as_ref(),
                _ => true,
            };
            if changed {
                state.gop_fragments.clear();
            }
            state.init_segment = next;
        }
    }

    pub async fn init_segment(&self) -> Option<Arc<Bytes>> {
        self.state.lock().ok().and_then(|g| g.init_segment.clone())
    }

    /// Broadcast an IDR-keyframe H.264 media fragment, resetting the cached current-GOP buffer
    /// so late-joining viewers immediately begin decoding from this keyframe.
    pub fn publish_keyframe_fragment(&self, fragment: Bytes) -> usize {
        self.publish_fragment_inner(fragment, true)
    }

    /// Broadcast one H.264 media fragment (`moof+mdat` or Annex-B frame) zero-copy across viewers.
    /// Automatically detects Annex-B IDR/SPS keyframes to reset the current-GOP cache.
    pub fn publish_fragment(&self, fragment: Bytes) -> usize {
        let is_idr = is_annex_b_idr_or_sps(&fragment);
        self.publish_fragment_inner(fragment, is_idr)
    }

    fn publish_fragment_inner(&self, fragment: Bytes, is_keyframe: bool) -> usize {
        if fragment.is_empty() {
            return 0;
        }
        let arc = Arc::new(fragment);
        let item = H264Fragment {
            bytes: Arc::clone(&arc),
            is_keyframe,
        };
        if let Ok(mut state) = self.state.lock() {
            if is_keyframe {
                state.gop_fragments.clear();
                state.gop_fragments.push(arc);
            } else if !state.gop_fragments.is_empty() {
                if state.gop_fragments.len() >= MAX_H264_GOP_FRAGMENTS {
                    // GOP exceeded cap; clear cached fragments and wait for the next IDR
                    // rather than serving a truncated prefix.
                    state.gop_fragments.clear();
                } else {
                    state.gop_fragments.push(arc);
                }
            }
            return self.tx.send(item).unwrap_or(0);
        }
        self.tx.send(item).unwrap_or(0)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<H264Fragment> {
        self.tx.subscribe()
    }

    /// Atomically snapshot `init_segment` and cached current-GOP fragments (starting from the
    /// latest IDR keyframe) and subscribe to subsequent live fragments without race windows.
    pub fn subscribe_with_gop(
        &self,
    ) -> (
        Option<Arc<Bytes>>,
        Vec<Arc<Bytes>>,
        broadcast::Receiver<H264Fragment>,
    ) {
        if let Ok(state) = self.state.lock() {
            let rx = self.tx.subscribe();
            return (state.init_segment.clone(), state.gop_fragments.clone(), rx);
        }
        (None, Vec::new(), self.tx.subscribe())
    }
}

fn is_annex_b_idr_or_sps(buf: &[u8]) -> bool {
    let mut i = 0;
    while i + 3 < buf.len() {
        if buf[i] == 0 && buf[i + 1] == 0 {
            if buf[i + 2] == 1 {
                let nal_type = buf[i + 3] & 0x1f;
                if matches!(nal_type, 5 | 7) {
                    return true;
                }
                i += 4;
                continue;
            }
            if i + 4 < buf.len() && buf[i + 2] == 0 && buf[i + 3] == 1 {
                let nal_type = buf[i + 4] & 0x1f;
                if matches!(nal_type, 5 | 7) {
                    return true;
                }
                i += 5;
                continue;
            }
        }
        i += 1;
    }
    false
}

/// Build an async streaming `axum::body::Body` for `GET /api/v1/preview/h264`.
pub fn h264_stream_body(
    init_segment: Option<Arc<Bytes>>,
    rx: broadcast::Receiver<H264Fragment>,
    permit: OwnedSemaphorePermit,
    shutdown_rx: watch::Receiver<bool>,
) -> axum::body::Body {
    h264_stream_body_with_gop(init_segment, Vec::new(), rx, permit, shutdown_rx)
}

/// Build an async streaming `axum::body::Body` for `GET /api/v1/preview/h264`, replaying
/// `gop_fragments` (from the latest IDR keyframe) immediately after `init_segment` before live frames.
pub fn h264_stream_body_with_gop(
    init_segment: Option<Arc<Bytes>>,
    gop_fragments: Vec<Arc<Bytes>>,
    mut rx: broadcast::Receiver<H264Fragment>,
    permit: OwnedSemaphorePermit,
    mut shutdown_rx: watch::Receiver<bool>,
) -> axum::body::Body {
    let (tx, out_rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(16);
    tokio::spawn(async move {
        let _permit = permit;
        if let Some(init) = init_segment {
            let chunk = (*init).clone();
            tokio::select! {
                biased;
                _ = crate::sse::wait_for_shutdown(&mut shutdown_rx) => return,
                res = tokio::time::timeout(PREVIEW_SEND_STALL_TIMEOUT, tx.send(Ok(chunk))) => {
                    if !matches!(res, Ok(Ok(()))) {
                        return;
                    }
                }
            }
        }
        for gop_frag in gop_fragments {
            let chunk = (*gop_frag).clone();
            tokio::select! {
                biased;
                _ = crate::sse::wait_for_shutdown(&mut shutdown_rx) => return,
                res = tokio::time::timeout(PREVIEW_SEND_STALL_TIMEOUT, tx.send(Ok(chunk))) => {
                    if !matches!(res, Ok(Ok(()))) {
                        return;
                    }
                }
            }
        }
        let mut awaiting_keyframe = false;
        loop {
            tokio::select! {
                biased;
                _ = crate::sse::wait_for_shutdown(&mut shutdown_rx) => return,
                _ = tx.closed() => return,
                msg = rx.recv() => {
                    match msg {
                        Ok(fragment) => {
                            if awaiting_keyframe {
                                if !fragment.is_keyframe {
                                    continue;
                                }
                                awaiting_keyframe = false;
                            }
                            let chunk = (*fragment.bytes).clone();
                            tokio::select! {
                                biased;
                                _ = crate::sse::wait_for_shutdown(&mut shutdown_rx) => return,
                                res = tokio::time::timeout(PREVIEW_SEND_STALL_TIMEOUT, tx.send(Ok(chunk))) => {
                                    if !matches!(res, Ok(Ok(()))) {
                                        return;
                                    }
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            // Slow viewer dropped intermediate fragments; skip non-keyframes until next IDR.
                            awaiting_keyframe = true;
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use tokio::sync::Semaphore;

    #[test]
    fn h264_hub_atomic_init_clears_gop_on_change_and_clears_oversized_gop() {
        let hub = H264PreviewHub::new_fmp4(16);
        hub.set_init_segment_blocking(Bytes::from_static(b"INIT-A"));
        hub.publish_keyframe_fragment(Bytes::from_static(b"IDR-1"));
        hub.publish_fragment(Bytes::from_static(b"P-1"));

        let (init1, gop1, _rx1) = hub.subscribe_with_gop();
        assert_eq!(
            init1.as_deref().map(Bytes::as_ref),
            Some(b"INIT-A".as_slice())
        );
        assert_eq!(gop1.len(), 2);

        // Changing init_segment atomically clears stale GOP fragments.
        hub.set_init_segment_blocking(Bytes::from_static(b"INIT-B"));
        let (init2, gop2, _rx2) = hub.subscribe_with_gop();
        assert_eq!(
            init2.as_deref().map(Bytes::as_ref),
            Some(b"INIT-B".as_slice())
        );
        assert!(gop2.is_empty(), "changing init_segment must clear GOP");

        // Exceeding MAX_H264_GOP_FRAGMENTS clears cached GOP instead of keeping truncated prefix.
        hub.publish_keyframe_fragment(Bytes::from_static(b"IDR-2"));
        for idx in 1..MAX_H264_GOP_FRAGMENTS {
            hub.publish_fragment(Bytes::from(format!("P-{idx}")));
        }
        assert_eq!(hub.subscribe_with_gop().1.len(), MAX_H264_GOP_FRAGMENTS);
        hub.publish_fragment(Bytes::from_static(b"P-OVERFLOW"));
        assert!(
            hub.subscribe_with_gop().1.is_empty(),
            "oversized GOP must be cleared until next IDR"
        );
    }

    #[tokio::test]
    async fn h264_stream_body_skips_inter_frames_after_lag_until_next_keyframe() {
        let hub = H264PreviewHub::new_fmp4(16);
        hub.set_init_segment_blocking(Bytes::from_static(b"INIT"));
        let (init, gop, rx) = hub.subscribe_with_gop();
        assert!(gop.is_empty());

        let sem = Arc::new(Semaphore::new(1));
        let permit = sem.acquire_owned().await.unwrap();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let body = h264_stream_body_with_gop(init, gop, rx, permit, shutdown_rx);
        let mut stream = body.into_data_stream();

        // First chunk must be INIT.
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            Bytes::from_static(b"INIT")
        );

        // Overflow the 16-slot broadcast channel so receiver lags, ending with P-frames then IDR-NEXT.
        hub.publish_keyframe_fragment(Bytes::from_static(b"IDR-DROPPED"));
        for idx in 0..20 {
            hub.publish_fragment(Bytes::from(format!("P-DROPPED-{idx}")));
        }
        hub.publish_fragment(Bytes::from_static(b"P-AFTER-LAG-BEFORE-IDR"));
        hub.publish_keyframe_fragment(Bytes::from_static(b"IDR-RESYNC"));
        hub.publish_fragment(Bytes::from_static(b"P-AFTER-RESYNC"));

        let next1 = stream.next().await.unwrap().unwrap();
        assert_eq!(next1, Bytes::from_static(b"IDR-RESYNC"));
        let next2 = stream.next().await.unwrap().unwrap();
        assert_eq!(next2, Bytes::from_static(b"P-AFTER-RESYNC"));
    }
}
