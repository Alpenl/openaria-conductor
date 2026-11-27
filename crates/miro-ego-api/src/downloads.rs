//! High-throughput RFC 9110 Range + `If-Range` async file streaming with
//! `posix_fadvise` sequential read-ahead and `POSIX_FADV_DONTNEED` page-cache eviction.

use crate::backend::CommandOutcome;
use crate::error::{ApiError, ApiResult};
use crate::range::{
    ByteRange, if_range_allows_partial, infer_log_media_type, is_valid_log_name, parse_single_range,
};
use bytes::Bytes;
use futures_util::Stream;
use miro_ego_core::ids::hex_encode;
use miro_ego_core::sys::fs::{
    DEFAULT_IO_BUFFER_BYTES, aligned_io_capacity, fadvise_dontneed, fadvise_sequential, fstat,
    fstatat_nofollow, open_directory_safe, openat_retry, pread_retry,
};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::CString;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedRwLockReadGuard, OwnedSemaphorePermit, watch};

pub const DEFAULT_LOG_ETAG_CACHE_CAPACITY: usize = 256;
const PAGE_BYTES: u64 = 4096;
const DELETE_REPLAY_CACHE_CAPACITY: usize = 512;
const DELETE_REPLAY_TTL: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LogEtagCacheKey {
    logs_root: PathBuf,
    relative_name: String,
    st_dev: u64,
    size: u64,
    mtime_sec: i64,
    mtime_nsec: i64,
}

struct LogEtagCacheInner {
    capacity: usize,
    entries: HashMap<LogEtagCacheKey, String>,
    lru_order: VecDeque<LogEtagCacheKey>,
}

/// Bounded LRU strong-ETag (`sha256`) cache keyed by `(logs_root, relative_name, st_dev, size, mtime_sec, mtime_nsec)`.
///
/// Validates `fstat` in O(1) on repeated `Range` downloads of unchanged `/api/v1/logs/{name}` files
/// without re-reading and re-hashing the entire file on every chunk.
pub struct LogEtagCache {
    inner: Mutex<LogEtagCacheInner>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl Default for LogEtagCache {
    fn default() -> Self {
        Self::new(DEFAULT_LOG_ETAG_CACHE_CAPACITY)
    }
}

impl LogEtagCache {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(1);
        Self {
            inner: Mutex::new(LogEtagCacheInner {
                capacity: cap,
                entries: HashMap::with_capacity(cap),
                lru_order: VecDeque::with_capacity(cap),
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    pub fn len(&self) -> usize {
        self.inner.lock().map(|g| g.entries.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn get(&self, key: &LogEtagCacheKey) -> Option<String> {
        let mut guard = self.inner.lock().ok()?;
        let digest = guard.entries.get(key)?.clone();
        if let Some(pos) = guard.lru_order.iter().position(|k| k == key) {
            if let Some(item) = guard.lru_order.remove(pos) {
                guard.lru_order.push_back(item);
            }
        }
        self.hits.fetch_add(1, Ordering::Relaxed);
        Some(digest)
    }

    fn insert(&self, key: LogEtagCacheKey, etag_hex: String) {
        let Ok(mut guard) = self.inner.lock() else {
            return;
        };
        if guard.entries.contains_key(&key) {
            guard.entries.insert(key.clone(), etag_hex);
            if let Some(pos) = guard.lru_order.iter().position(|k| k == &key)
                && let Some(item) = guard.lru_order.remove(pos)
            {
                guard.lru_order.push_back(item);
            }
            return;
        }
        while guard.entries.len() >= guard.capacity {
            if let Some(oldest) = guard.lru_order.pop_front() {
                guard.entries.remove(&oldest);
            } else {
                break;
            }
        }
        guard.lru_order.push_back(key.clone());
        guard.entries.insert(key, etag_hex);
    }
}

fn default_log_etag_cache() -> &'static LogEtagCache {
    static CACHE: OnceLock<LogEtagCache> = OnceLock::new();
    CACHE.get_or_init(LogEtagCache::default)
}

#[derive(Debug, Clone)]
struct CompletedDeleteReplay {
    canonical_body: Vec<u8>,
    outcome: CommandOutcome,
    expires_at: Instant,
}

struct InFlightDelete {
    canonical_body: Vec<u8>,
    rx: watch::Receiver<Option<ApiResult<CommandOutcome>>>,
}

struct ArtifactDeleteGateInner {
    active_downloads: HashMap<String, usize>,
    deleting_sessions: HashSet<String>,
    completed_replays: HashMap<(String, String), CompletedDeleteReplay>,
    completed_order: VecDeque<(String, String)>,
    in_flight_deletes: HashMap<(String, String), InFlightDelete>,
}

/// Per-session artifact download vs. `deleteSessions` coordination gate with
/// idempotency replay and in-flight duplicate retry coalescing.
pub struct ArtifactDeleteGate {
    inner: Mutex<ArtifactDeleteGateInner>,
}

impl Default for ArtifactDeleteGate {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII per-session download lease held while streaming a session artifact.
pub struct SessionDownloadLease {
    gate: Arc<ArtifactDeleteGate>,
    session_id: String,
}

impl Drop for SessionDownloadLease {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.gate.inner.lock()
            && let Some(count) = guard.active_downloads.get_mut(&self.session_id)
        {
            *count = count.saturating_sub(1);
            if *count == 0 {
                guard.active_downloads.remove(&self.session_id);
            }
        }
    }
}

pub(crate) enum DeleteReplayLookup {
    Replay(CommandOutcome),
    Coalesce(watch::Receiver<Option<ApiResult<CommandOutcome>>>),
    Miss,
}

pub(crate) enum DeleteBeginResult {
    Replay(CommandOutcome),
    Coalesce(watch::Receiver<Option<ApiResult<CommandOutcome>>>),
    Execute(DeleteExecutionGuard),
}

pub(crate) struct DeleteExecutionGuard {
    gate: Arc<ArtifactDeleteGate>,
    idem_key: (String, String),
    canonical_body: Vec<u8>,
    target_sids: Vec<String>,
    tx: Option<watch::Sender<Option<ApiResult<CommandOutcome>>>>,
}

impl DeleteExecutionGuard {
    pub(crate) fn finish(mut self, result: &ApiResult<CommandOutcome>) {
        if let Ok(mut guard) = self.gate.inner.lock() {
            for sid in &self.target_sids {
                guard.deleting_sessions.remove(sid);
            }
            guard.in_flight_deletes.remove(&self.idem_key);
            if let Ok(outcome) = result {
                while guard.completed_replays.len() >= DELETE_REPLAY_CACHE_CAPACITY {
                    if let Some(oldest) = guard.completed_order.pop_front() {
                        guard.completed_replays.remove(&oldest);
                    } else {
                        break;
                    }
                }
                guard.completed_order.push_back(self.idem_key.clone());
                guard.completed_replays.insert(
                    self.idem_key.clone(),
                    CompletedDeleteReplay {
                        canonical_body: self.canonical_body.clone(),
                        outcome: outcome.clone(),
                        expires_at: Instant::now() + DELETE_REPLAY_TTL,
                    },
                );
            }
        }
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(Some(result.clone()));
        }
    }
}

impl Drop for DeleteExecutionGuard {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            if let Ok(mut guard) = self.gate.inner.lock() {
                for sid in &self.target_sids {
                    guard.deleting_sessions.remove(sid);
                }
                guard.in_flight_deletes.remove(&self.idem_key);
            }
            let _ = tx.send(Some(Err(ApiError::service_unavailable(
                "delete_interrupted",
                "in-flight session deletion was interrupted; retry",
            ))));
        }
    }
}

impl ArtifactDeleteGate {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(ArtifactDeleteGateInner {
                active_downloads: HashMap::new(),
                deleting_sessions: HashSet::new(),
                completed_replays: HashMap::new(),
                completed_order: VecDeque::new(),
                in_flight_deletes: HashMap::new(),
            }),
        }
    }

    /// Acquire a per-session download lease for `session_id`, failing with `409 session_deleting`
    /// if `session_id` is currently being deleted (never returning a fake `423 capture_busy`).
    pub fn try_acquire_download(
        self: &Arc<Self>,
        session_id: &str,
    ) -> ApiResult<SessionDownloadLease> {
        let mut guard = self.inner.lock().map_err(|_| {
            ApiError::internal("state_poisoned", "artifact delete gate mutex poisoned")
        })?;
        if guard.deleting_sessions.contains(session_id) {
            return Err(ApiError::conflict(
                "session_deleting",
                "session is currently being deleted",
            )
            .with_retryable(false)
            .with_error_code_header());
        }
        *guard
            .active_downloads
            .entry(session_id.to_owned())
            .or_insert(0) += 1;
        Ok(SessionDownloadLease {
            gate: Arc::clone(self),
            session_id: session_id.to_owned(),
        })
    }

    pub fn active_download_count(&self, session_id: &str) -> usize {
        self.inner
            .lock()
            .ok()
            .and_then(|g| g.active_downloads.get(session_id).copied())
            .unwrap_or(0)
    }

    pub(crate) fn lookup_replay_or_inflight(
        &self,
        principal_id: &str,
        idempotency_key: &str,
        canonical_body: &[u8],
    ) -> ApiResult<DeleteReplayLookup> {
        let mut guard = self.inner.lock().map_err(|_| {
            ApiError::internal("state_poisoned", "artifact delete gate mutex poisoned")
        })?;
        prune_expired_replays(&mut guard);
        let key = (principal_id.to_owned(), idempotency_key.to_owned());
        if let Some(done) = guard.completed_replays.get(&key) {
            if done.canonical_body == canonical_body {
                let mut replayed = done.outcome.clone();
                replayed.replayed = true;
                return Ok(DeleteReplayLookup::Replay(replayed));
            }
            return Err(ApiError::conflict(
                "idempotency_conflict",
                "Idempotency-Key reused with different request payload",
            )
            .with_error_code_header());
        }
        if let Some(inflight) = guard.in_flight_deletes.get(&key) {
            if inflight.canonical_body == canonical_body {
                return Ok(DeleteReplayLookup::Coalesce(inflight.rx.clone()));
            }
            return Err(ApiError::conflict(
                "idempotency_conflict",
                "Idempotency-Key reused with different request payload",
            )
            .with_error_code_header());
        }
        Ok(DeleteReplayLookup::Miss)
    }

    pub(crate) fn try_begin_delete(
        self: &Arc<Self>,
        principal_id: &str,
        idempotency_key: &str,
        canonical_body: Vec<u8>,
        target_sids: &[String],
    ) -> ApiResult<DeleteBeginResult> {
        let mut guard = self.inner.lock().map_err(|_| {
            ApiError::internal("state_poisoned", "artifact delete gate mutex poisoned")
        })?;
        prune_expired_replays(&mut guard);
        let key = (principal_id.to_owned(), idempotency_key.to_owned());
        if let Some(done) = guard.completed_replays.get(&key) {
            if done.canonical_body == canonical_body {
                let mut replayed = done.outcome.clone();
                replayed.replayed = true;
                return Ok(DeleteBeginResult::Replay(replayed));
            }
            return Err(ApiError::conflict(
                "idempotency_conflict",
                "Idempotency-Key reused with different request payload",
            )
            .with_error_code_header());
        }
        if let Some(inflight) = guard.in_flight_deletes.get(&key) {
            if inflight.canonical_body == canonical_body {
                return Ok(DeleteBeginResult::Coalesce(inflight.rx.clone()));
            }
            return Err(ApiError::conflict(
                "idempotency_conflict",
                "Idempotency-Key reused with different request payload",
            )
            .with_error_code_header());
        }

        for sid in target_sids {
            if guard.active_downloads.get(sid).copied().unwrap_or(0) > 0 {
                return Err(ApiError::conflict(
                    "open_downloads",
                    "cannot delete sessions while artifact downloads are open",
                )
                .with_retryable(true)
                .with_error_code_header());
            }
        }
        for sid in target_sids {
            if guard.deleting_sessions.contains(sid) {
                return Err(ApiError::conflict(
                    "session_deleting",
                    "session is already being deleted by an active request",
                )
                .with_retryable(true)
                .with_error_code_header());
            }
        }

        for sid in target_sids {
            guard.deleting_sessions.insert(sid.clone());
        }
        let (tx, rx) = watch::channel(None);
        guard.in_flight_deletes.insert(
            key.clone(),
            InFlightDelete {
                canonical_body: canonical_body.clone(),
                rx,
            },
        );
        Ok(DeleteBeginResult::Execute(DeleteExecutionGuard {
            gate: Arc::clone(self),
            idem_key: key,
            canonical_body,
            target_sids: target_sids.to_vec(),
            tx: Some(tx),
        }))
    }
}

fn prune_expired_replays(guard: &mut ArtifactDeleteGateInner) {
    let now = Instant::now();
    while let Some(front) = guard.completed_order.front() {
        let expired = guard
            .completed_replays
            .get(front)
            .is_none_or(|entry| entry.expires_at <= now);
        if expired {
            if let Some(key) = guard.completed_order.pop_front() {
                guard.completed_replays.remove(&key);
            }
        } else {
            break;
        }
    }
}

/// Compute the next 4 KiB page-aligned `[adv_off, adv_off + adv_len)` range eligible for
/// `POSIX_FADV_DONTNEED` so pages straddling unaligned `Range` or partial `pread` chunk
/// boundaries are never skipped. When `reached_eof` is true, `adv_len == 0` evicts through EOF.
pub fn compute_page_aligned_dontneed(
    evicted_until: u64,
    new_cursor: u64,
    reached_eof: bool,
) -> Option<(i64, i64, u64)> {
    if reached_eof {
        if new_cursor > evicted_until
            && let Ok(adv_off) = i64::try_from(evicted_until)
        {
            return Some((adv_off, 0, new_cursor));
        }
        return None;
    }
    let aligned_end = new_cursor & !(PAGE_BYTES - 1);
    if aligned_end > evicted_until {
        let len = aligned_end - evicted_until;
        if let (Ok(adv_off), Ok(adv_len)) = (i64::try_from(evicted_until), i64::try_from(len)) {
            return Some((adv_off, adv_len, aligned_end));
        }
    }
    None
}

/// Backing byte source for an opened downloadable representation (artifact or log file).
pub enum DownloadSource {
    /// Regular file descriptor streamed asynchronously with `pread` + `posix_fadvise`.
    File(Arc<File>),
    /// Immutable in-memory byte slice (used by mock backends or small pre-verified payloads).
    Memory(Bytes),
}

/// Metadata and handle for an opened immutable artifact or diagnostic log file.
pub struct OpenRepresentation {
    pub content_type: String,
    pub etag: String,
    pub size: u64,
    pub content_disposition: Option<String>,
    pub source: DownloadSource,
}

impl OpenRepresentation {
    pub fn from_memory(
        content_type: impl Into<String>,
        etag: impl Into<String>,
        bytes: Bytes,
    ) -> Self {
        let size = bytes.len() as u64;
        Self {
            content_type: content_type.into(),
            etag: etag.into(),
            size,
            content_disposition: None,
            source: DownloadSource::Memory(bytes),
        }
    }

    pub fn from_file(
        content_type: impl Into<String>,
        etag: impl Into<String>,
        file: File,
        size: u64,
    ) -> Self {
        Self {
            content_type: content_type.into(),
            etag: etag.into(),
            size,
            content_disposition: None,
            source: DownloadSource::File(Arc::new(file)),
        }
    }

    pub fn with_content_disposition(mut self, value: impl Into<String>) -> Self {
        self.content_disposition = Some(value.into());
        self
    }
}

/// Resolved HTTP range selection ready to stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedRange {
    pub status: u16,
    pub offset: u64,
    pub length: u64,
    pub content_range: Option<ByteRange>,
}

/// Resolve `Range` and `If-Range` headers against `representation` according to RFC 9110.
pub fn resolve_range_selection(
    range_header: Option<&str>,
    range_is_single: bool,
    if_range_header: Option<&str>,
    if_range_is_single: bool,
    representation: &OpenRepresentation,
    head: bool,
) -> ApiResult<ResolvedRange> {
    if head {
        return Ok(ResolvedRange {
            status: 200,
            offset: 0,
            length: representation.size,
            content_range: None,
        });
    }
    if !range_is_single {
        return Err(ApiError::range_not_satisfiable(
            representation.size,
            "multiple Range headers are not supported",
        ));
    }
    let apply_range =
        if_range_allows_partial(if_range_header, if_range_is_single, &representation.etag);
    if !apply_range || range_header.is_none() {
        return Ok(ResolvedRange {
            status: 200,
            offset: 0,
            length: representation.size,
            content_range: None,
        });
    }
    let parsed = parse_single_range(range_header, representation.size).map_err(|err| {
        ApiError::range_not_satisfiable(err.complete_size, "requested byte range is unsatisfiable")
    })?;
    match parsed {
        None => Ok(ResolvedRange {
            status: 200,
            offset: 0,
            length: representation.size,
            content_range: None,
        }),
        Some(range) => Ok(ResolvedRange {
            status: 206,
            offset: range.first,
            length: range.length(),
            content_range: Some(range),
        }),
    }
}

/// Build an async streaming `axum::body::Body` over `[offset, offset + length)` using
/// 256 KiB–1 MiB page-aligned chunks and `posix_fadvise` sequential/dontneed hints.
///
/// The download semaphore `permit` is held until streaming finishes or the client disconnects.
pub fn stream_download_body(
    source: DownloadSource,
    offset: u64,
    length: u64,
    chunk_bytes: usize,
    permit: OwnedSemaphorePermit,
) -> axum::body::Body {
    stream_download_body_with_lease(source, offset, length, chunk_bytes, permit, None)
}

/// Build an async streaming `axum::body::Body` holding both `permit` and an optional
/// shared artifact-delete read lease until streaming finishes or the client disconnects.
pub fn stream_download_body_with_lease(
    source: DownloadSource,
    offset: u64,
    length: u64,
    chunk_bytes: usize,
    permit: OwnedSemaphorePermit,
    delete_lease: Option<OwnedRwLockReadGuard<()>>,
) -> axum::body::Body {
    stream_download_body_inner(
        source,
        offset,
        length,
        chunk_bytes,
        permit,
        delete_lease,
        None,
    )
}

/// Build an async streaming `axum::body::Body` holding `permit` and an optional
/// per-session [`SessionDownloadLease`] until streaming finishes or the client disconnects.
pub fn stream_download_body_with_session_lease(
    source: DownloadSource,
    offset: u64,
    length: u64,
    chunk_bytes: usize,
    permit: OwnedSemaphorePermit,
    session_lease: Option<SessionDownloadLease>,
) -> axum::body::Body {
    stream_download_body_inner(
        source,
        offset,
        length,
        chunk_bytes,
        permit,
        None,
        session_lease,
    )
}

fn stream_download_body_inner(
    source: DownloadSource,
    offset: u64,
    length: u64,
    chunk_bytes: usize,
    permit: OwnedSemaphorePermit,
    delete_lease: Option<OwnedRwLockReadGuard<()>>,
    session_lease: Option<SessionDownloadLease>,
) -> axum::body::Body {
    if length == 0 {
        drop(session_lease);
        drop(delete_lease);
        drop(permit);
        return axum::body::Body::empty();
    }
    let chunk_size = aligned_io_capacity(chunk_bytes.max(DEFAULT_IO_BUFFER_BYTES));
    match source {
        DownloadSource::Memory(bytes) => {
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, io::Error>>(4);
            tokio::spawn(async move {
                let _permit = permit;
                let _delete_lease = delete_lease;
                let _session_lease = session_lease;
                let mut pos = offset as usize;
                let end = (offset.saturating_add(length) as usize).min(bytes.len());
                while pos < end {
                    let next_end = (pos + chunk_size).min(end);
                    let slice = bytes.slice(pos..next_end);
                    pos = next_end;
                    if tx.send(Ok(slice)).await.is_err() {
                        return;
                    }
                }
            });
            axum::body::Body::from_stream(DownloadByteStream(rx))
        }
        DownloadSource::File(file) => {
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, io::Error>>(4);
            tokio::spawn(async move {
                let _permit = permit;
                let _delete_lease = delete_lease;
                let _session_lease = session_lease;
                if let (Ok(off_i64), Ok(len_i64)) = (i64::try_from(offset), i64::try_from(length)) {
                    let _ = fadvise_sequential(file.as_raw_fd(), off_i64, len_i64);
                }
                let mut cursor = offset;
                let mut remaining = length;
                let mut evicted_until = offset.div_ceil(PAGE_BYTES).saturating_mul(PAGE_BYTES);
                while remaining > 0 {
                    if tx.is_closed() {
                        return;
                    }
                    let want = (remaining.min(chunk_size as u64)) as usize;
                    let file_clone = Arc::clone(&file);
                    let read_offset = cursor;
                    let cur_evicted_until = evicted_until;
                    let read_result =
                        tokio::task::spawn_blocking(move || -> io::Result<(Bytes, u64)> {
                            let off_t = libc::off_t::try_from(read_offset).map_err(|_| {
                                io::Error::new(io::ErrorKind::InvalidInput, "file offset overflow")
                            })?;
                            let mut buf = vec![0_u8; want];
                            let mut filled = 0;
                            while filled < want {
                                let cur_off = off_t
                                    .checked_add(libc::off_t::try_from(filled).map_err(|_| {
                                        io::Error::new(
                                            io::ErrorKind::InvalidInput,
                                            "file offset overflow",
                                        )
                                    })?)
                                    .ok_or_else(|| {
                                        io::Error::new(
                                            io::ErrorKind::InvalidInput,
                                            "file offset overflow",
                                        )
                                    })?;
                                let n = pread_retry(
                                    file_clone.as_raw_fd(),
                                    &mut buf[filled..want],
                                    cur_off,
                                )?;
                                if n == 0 {
                                    break;
                                }
                                filled += n;
                            }
                            let new_cursor = read_offset.saturating_add(filled as u64);
                            let mut next_evicted = cur_evicted_until;
                            if filled > 0
                                && let Some((adv_off, adv_len, updated_evicted)) =
                                    compute_page_aligned_dontneed(
                                        cur_evicted_until,
                                        new_cursor,
                                        false,
                                    )
                            {
                                let _ = fadvise_dontneed(file_clone.as_raw_fd(), adv_off, adv_len);
                                next_evicted = updated_evicted;
                            }
                            buf.truncate(filled);
                            Ok((Bytes::from(buf), next_evicted))
                        })
                        .await;

                    match read_result {
                        Ok(Ok((chunk, next_evicted))) => {
                            if chunk.is_empty() {
                                let _ = tx
                                    .send(Err(io::Error::new(
                                        io::ErrorKind::UnexpectedEof,
                                        "file truncated during download stream",
                                    )))
                                    .await;
                                return;
                            }
                            evicted_until = next_evicted;
                            let n = chunk.len() as u64;
                            cursor = cursor.saturating_add(n);
                            remaining = remaining.saturating_sub(n);
                            if tx.send(Ok(chunk)).await.is_err() {
                                return;
                            }
                        }
                        Ok(Err(err)) => {
                            let _ = tx.send(Err(err)).await;
                            return;
                        }
                        Err(join_err) => {
                            let _ = tx
                                .send(Err(io::Error::other(format!(
                                    "blocking file reader failed: {join_err}"
                                ))))
                                .await;
                            return;
                        }
                    }
                }
            });
            axum::body::Body::from_stream(DownloadByteStream(rx))
        }
    }
}

struct DownloadByteStream(tokio::sync::mpsc::Receiver<Result<Bytes, io::Error>>);

impl Stream for DownloadByteStream {
    type Item = Result<Bytes, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

/// Safely open a regular diagnostic log file under `logs_root` using per-segment
/// `O_DIRECTORY | O_NOFOLLOW` traversal, rejecting symlinks, hidden dotfiles, hardlinks,
/// and path traversal. Uses the default bounded [`LogEtagCache`].
pub fn open_safe_log_file(logs_root: &Path, relative_name: &str) -> ApiResult<OpenRepresentation> {
    open_safe_log_file_with_cache(logs_root, relative_name, default_log_etag_cache())
}

/// Safely open a regular diagnostic log file under `logs_root`, validating `fstat` against
/// `etag_cache` in O(1) on unchanged files so `Range` requests do not re-hash the full file.
pub fn open_safe_log_file_with_cache(
    logs_root: &Path,
    relative_name: &str,
    etag_cache: &LogEtagCache,
) -> ApiResult<OpenRepresentation> {
    if !is_valid_log_name(relative_name) {
        return Err(ApiError::bad_request(
            "invalid diagnostic log path; traversal and hidden files are forbidden",
        ));
    }
    let mut current_dir = open_directory_safe(logs_root).map_err(|err| {
        if err.kind() == io::ErrorKind::NotFound {
            ApiError::storage_unavailable("diagnostic logs directory is unavailable")
        } else {
            ApiError::storage_unavailable(format!("failed to open logs directory: {err}"))
        }
    })?;

    let segments: Vec<&str> = relative_name.split('/').collect();
    let Some((leaf, parents)) = segments.split_last() else {
        return Err(ApiError::bad_request("log path cannot be empty"));
    };

    for dir_name in parents {
        let c_name = CString::new(*dir_name)
            .map_err(|_| ApiError::bad_request("log path contains NUL byte"))?;
        current_dir = openat_retry(
            current_dir.as_raw_fd(),
            &c_name,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
        .map_err(|err| map_log_open_error(err, relative_name))?;
    }

    let leaf_c =
        CString::new(*leaf).map_err(|_| ApiError::bad_request("log path contains NUL byte"))?;
    let expected = fstatat_nofollow(current_dir.as_raw_fd(), &leaf_c)
        .map_err(|err| map_log_open_error(err, relative_name))?;
    if (expected.st_mode & libc::S_IFMT) != libc::S_IFREG {
        return Err(ApiError::not_found("diagnostic log file not found"));
    }

    let file = openat_retry(
        current_dir.as_raw_fd(),
        &leaf_c,
        libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
    )
    .map_err(|err| map_log_open_error(err, relative_name))?;
    let stat = fstat(file.as_raw_fd()).map_err(|err| map_log_open_error(err, relative_name))?;
    if (stat.st_mode & libc::S_IFMT) != libc::S_IFREG {
        return Err(ApiError::not_found("diagnostic log file not found"));
    }
    #[allow(clippy::useless_conversion)]
    if u64::from(stat.st_nlink) != 1 {
        return Err(ApiError::not_found(
            "diagnostic log file must not have hardlinks",
        ));
    }
    let size = u64::try_from(stat.st_size)
        .map_err(|_| ApiError::internal("invalid_source_state", "negative log file size"))?;

    #[allow(clippy::useless_conversion)]
    let cache_key = LogEtagCacheKey {
        logs_root: logs_root.to_path_buf(),
        relative_name: relative_name.to_owned(),
        st_dev: u64::from(stat.st_dev),
        size,
        mtime_sec: i64::from(stat.st_mtime),
        mtime_nsec: i64::from(stat.st_mtime_nsec),
    };

    let etag_hex = if let Some(cached) = etag_cache.get(&cache_key) {
        cached
    } else {
        etag_cache.misses.fetch_add(1, Ordering::Relaxed);
        // Compute strong SHA-256 ETag matching OpenAPI `^"[0-9a-f]{64}"$` while evicting clean pages,
        // verifying post-read `fstat` so concurrently truncated/rotated logs never mismatch ETag vs size.
        let computed = sha256_fd(&file, size)?;
        let after =
            fstat(file.as_raw_fd()).map_err(|err| map_log_open_error(err, relative_name))?;
        let after_size = u64::try_from(after.st_size)
            .map_err(|_| ApiError::internal("invalid_source_state", "negative log file size"))?;
        if after.st_dev != stat.st_dev
            || after_size != size
            || after.st_mtime != stat.st_mtime
            || after.st_mtime_nsec != stat.st_mtime_nsec
        {
            return Err(ApiError::storage_unavailable(
                "diagnostic log file changed concurrently during read; retry",
            ));
        }
        etag_cache.insert(cache_key, computed.clone());
        computed
    };

    let etag = format!("\"{etag_hex}\"");
    let media_type = infer_log_media_type(relative_name);
    let safe_filename = leaf.replace('"', "");

    Ok(OpenRepresentation::from_file(media_type, etag, file, size)
        .with_content_disposition(format!("attachment; filename=\"{safe_filename}\"")))
}

fn sha256_fd(file: &File, size: u64) -> ApiResult<String> {
    let fd = file.as_raw_fd();
    if let Ok(len_i64) = i64::try_from(size) {
        let _ = fadvise_sequential(fd, 0, len_i64);
    }
    let mut hasher = Sha256::new();
    let mut offset: u64 = 0;
    let mut evicted_until: u64 = 0;
    let mut buf = vec![0_u8; DEFAULT_IO_BUFFER_BYTES];
    while offset < size {
        let want = (size - offset).min(buf.len() as u64) as usize;
        let off_t = libc::off_t::try_from(offset)
            .map_err(|_| ApiError::internal("invalid_source_state", "file offset overflow"))?;
        let n = pread_retry(fd, &mut buf[..want], off_t)
            .map_err(|err| ApiError::provider_failure(format!("read log file: {err}")))?;
        if n == 0 {
            return Err(ApiError::storage_unavailable(
                "diagnostic log file was truncated during read; retry",
            ));
        }
        hasher.update(&buf[..n]);
        offset = offset.saturating_add(n as u64);
        let reached_eof = offset >= size;
        if let Some((adv_off, adv_len, next_evicted)) =
            compute_page_aligned_dontneed(evicted_until, offset, reached_eof)
        {
            let _ = fadvise_dontneed(fd, adv_off, adv_len);
            evicted_until = next_evicted;
        }
    }
    if offset != size {
        return Err(ApiError::storage_unavailable(
            "diagnostic log file size changed during read; retry",
        ));
    }
    Ok(hex_encode(&hasher.finalize()))
}

fn map_log_open_error(err: io::Error, _name: &str) -> ApiError {
    match err.raw_os_error() {
        Some(libc::ENOENT | libc::ENOTDIR | libc::ELOOP) => {
            ApiError::not_found("diagnostic log file not found")
        }
        _ => ApiError::storage_unavailable(format!("failed to read diagnostic log: {err}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn sha256_fd_rejects_mid_read_file_truncation_with_503_storage_unavailable() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "miro-ego-log-trunc-{}-{unique}.log",
            std::process::id()
        ));
        std::fs::write(&path, b"short-payload").unwrap();
        let file = File::open(&path).unwrap();
        // Simulate pre-stat seeing 4096 bytes before concurrent O_TRUNC shrank the file to 13 bytes.
        let err = sha256_fd(&file, 4096).unwrap_err();
        let _ = std::fs::remove_file(&path);
        assert_eq!(err.status_code, 503);
        assert_eq!(err.code, "storage_unavailable");
        assert!(err.retryable);
    }
}
