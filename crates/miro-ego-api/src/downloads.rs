//! High-throughput RFC 9110 Range + `If-Range` async file streaming with
//! `posix_fadvise` sequential read-ahead and `POSIX_FADV_DONTNEED` page-cache eviction.

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
use std::ffi::CString;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{OwnedRwLockReadGuard, OwnedSemaphorePermit};

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
    if length == 0 {
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
                if let (Ok(off_i64), Ok(len_i64)) = (i64::try_from(offset), i64::try_from(length)) {
                    let _ = fadvise_sequential(file.as_raw_fd(), off_i64, len_i64);
                }
                let mut cursor = offset;
                let mut remaining = length;
                while remaining > 0 {
                    if tx.is_closed() {
                        return;
                    }
                    let want = (remaining.min(chunk_size as u64)) as usize;
                    let file_clone = Arc::clone(&file);
                    let read_offset = cursor;
                    let read_result = tokio::task::spawn_blocking(move || -> io::Result<Bytes> {
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
                        if let (Ok(adv_off), Ok(adv_len)) =
                            (i64::try_from(read_offset), i64::try_from(filled))
                            && adv_len > 0
                        {
                            let _ = fadvise_dontneed(file_clone.as_raw_fd(), adv_off, adv_len);
                        }
                        buf.truncate(filled);
                        Ok(Bytes::from(buf))
                    })
                    .await;

                    match read_result {
                        Ok(Ok(chunk)) => {
                            if chunk.is_empty() {
                                let _ = tx
                                    .send(Err(io::Error::new(
                                        io::ErrorKind::UnexpectedEof,
                                        "file truncated during download stream",
                                    )))
                                    .await;
                                return;
                            }
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
/// and path traversal.
pub fn open_safe_log_file(logs_root: &Path, relative_name: &str) -> ApiResult<OpenRepresentation> {
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

    // Compute strong SHA-256 ETag matching OpenAPI `^"[0-9a-f]{64}"$` while evicting clean pages,
    // verifying post-read `fstat` so concurrently truncated/rotated logs never mismatch ETag vs size.
    let etag_hex = sha256_fd(&file, size)?;
    let after = fstat(file.as_raw_fd()).map_err(|err| map_log_open_error(err, relative_name))?;
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
        if let (Ok(adv_off), Ok(adv_len)) = (i64::try_from(offset), i64::try_from(n)) {
            let _ = fadvise_dontneed(fd, adv_off, adv_len);
        }
        offset = offset.saturating_add(n as u64);
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
