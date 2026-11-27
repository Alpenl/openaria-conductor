//! Safe diagnostic log listing and bounded/range streaming under `/data/logs/`.
//!
//! Matches OpenAPI v1 `GET /api/v1/logs` (`miro-ego.log-list.v1`) and
//! `GET /api/v1/logs/{name}`, rejecting symlinks, hidden dot-prefixed entries,
//! cross-device mounts, and path traversal attempts.

use crate::artifact_io::{
    ArtifactFileIdentity, ByteRange, open_relative_regular_in_dir, parse_single_byte_range,
};
use crate::util::{VolumeHandleLease, now_rfc3339_utc, unix_secs_to_rfc3339_utc};
use crate::{StorageError, StorageResult};
use miro_ego_core::ids::hex_encode;
use miro_ego_core::paths::DataVolumePaths;
use miro_ego_core::sys::fs::{
    fadvise_dontneed, fadvise_sequential, fstat, fstatat_nofollow, is_exfat_or_msdos,
    open_directory_safe, openat_retry, pread_retry,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::ffi::CString;
use std::fs::{self, File};
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

pub const LOG_LIST_SCHEMA: &str = "miro-ego.log-list.v1";
pub const DEFAULT_LOG_LIST_LIMIT: usize = 200;
pub const MAX_LOG_LIST_LIMIT: usize = 500;
pub const MAX_LOG_NAME_LEN: usize = 255;
pub const MAX_LOG_PREFIX_LEN: usize = 128;
const MAX_SCAN_DEPTH: usize = 6;
const MAX_SCANNED_FILES: usize = 4096;
const STREAM_CHUNK_BYTES: usize = 256 * 1024;

/// Single diagnostic log file entry matching OpenAPI `LogFileEntry`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogFileEntry {
    pub name: String,
    pub bytes: u64,
    pub modified_at: String,
    pub media_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

/// Sorted diagnostic log list matching OpenAPI `LogList` (`miro-ego.log-list.v1`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogList {
    pub schema: String,
    pub volume_id: Option<String>,
    pub observed_at: String,
    pub items: Vec<LogFileEntry>,
    pub truncated: bool,
}

/// Safe open diagnostic log file handle supporting bounded reads and RFC 9110 single byte ranges.
pub struct OpenedLogFile {
    file: File,
    pub entry: LogFileEntry,
    pub identity: ArtifactFileIdentity,
    pub is_exfat: bool,
    lease: Option<VolumeHandleLease>,
}

impl OpenedLogFile {
    pub fn file(&self) -> &File {
        &self.file
    }

    pub fn as_raw_fd(&self) -> libc::c_int {
        self.file.as_raw_fd()
    }

    pub fn take_lease(&mut self) -> Option<VolumeHandleLease> {
        self.lease.take()
    }

    /// Compute exact SHA-256 hex digest and quoted ETag over the pinned log descriptor.
    pub fn compute_sha256_and_etag(&self) -> StorageResult<(String, String)> {
        let mut hasher = Sha256::new();
        let mut offset = 0_u64;
        let mut remaining = self.identity.size;
        let mut buf = vec![0_u8; STREAM_CHUNK_BYTES.min(remaining.max(1) as usize)];
        while remaining > 0 {
            let to_read = (remaining as usize).min(buf.len());
            let off = libc::off_t::try_from(offset).map_err(|_| {
                StorageError::invalid_input("offset_overflow", "log offset overflow")
            })?;
            let read = pread_retry(self.file.as_raw_fd(), &mut buf[..to_read], off)
                .map_err(|err| StorageError::io("log_read_failed", "hash diagnostic log", err))?;
            if read == 0 {
                return Err(StorageError::integrity(
                    "log_truncated",
                    "diagnostic log ended prematurely during digest calculation",
                ));
            }
            hasher.update(&buf[..read]);
            remaining -= read as u64;
            offset += read as u64;
        }
        let after = ArtifactFileIdentity::from_fd(self.file.as_raw_fd())?;
        if !after.matches(&self.identity, self.is_exfat) {
            return Err(StorageError::integrity(
                "log_changed",
                "diagnostic log changed during digest calculation",
            ));
        }
        self.evict_page_cache();
        let _ = fadvise_sequential(self.file.as_raw_fd(), 0, 0);
        let digest = hex_encode(hasher.finalize().as_slice());
        let etag = format!("\"{digest}\"");
        Ok((digest, etag))
    }

    /// Read at most `max_bytes` starting from offset 0 (fails if file exceeds `max_bytes`).
    pub fn read_bounded(&self, max_bytes: usize) -> StorageResult<Vec<u8>> {
        if self.identity.size > max_bytes as u64 {
            return Err(StorageError::invalid_input(
                "log_too_large",
                format!("log exceeds maximum bounded read size of {max_bytes} bytes"),
            ));
        }
        let len = self.identity.size as usize;
        let mut out = vec![0_u8; len];
        let mut filled = 0_usize;
        while filled < len {
            let end = (filled + STREAM_CHUNK_BYTES).min(len);
            let off = libc::off_t::try_from(filled).map_err(|_| {
                StorageError::invalid_input("offset_overflow", "log offset overflow")
            })?;
            let read = pread_retry(self.file.as_raw_fd(), &mut out[filled..end], off)
                .map_err(|err| StorageError::io("log_read_failed", "read diagnostic log", err))?;
            if read == 0 {
                return Err(StorageError::integrity(
                    "log_truncated",
                    "diagnostic log truncated during read",
                ));
            }
            filled += read;
        }
        let after = ArtifactFileIdentity::from_fd(self.file.as_raw_fd())?;
        if !after.matches(&self.identity, self.is_exfat) {
            return Err(StorageError::integrity(
                "log_changed",
                "diagnostic log changed during read",
            ));
        }
        self.evict_page_cache();
        Ok(out)
    }

    /// Read an optional single byte range (or complete file when `range_header` is `None`).
    pub fn read_with_range(
        &self,
        range_header: Option<&str>,
        if_range_header: Option<&str>,
        etag: &str,
    ) -> StorageResult<(Option<ByteRange>, Vec<u8>)> {
        let range =
            parse_single_byte_range(range_header, if_range_header, etag, self.identity.size)?;
        let (start, length) = match range {
            Some(r) => (r.start, r.len()),
            None => (0, self.identity.size),
        };
        let len_usize = usize::try_from(length).map_err(|_| {
            StorageError::invalid_input("range_too_large", "requested log range too large")
        })?;
        let mut out = vec![0_u8; len_usize];
        let mut filled = 0_usize;
        while filled < len_usize {
            let end = (filled + STREAM_CHUNK_BYTES).min(len_usize);
            let off = libc::off_t::try_from(start + filled as u64).map_err(|_| {
                StorageError::invalid_input("offset_overflow", "log range offset overflow")
            })?;
            let read =
                pread_retry(self.file.as_raw_fd(), &mut out[filled..end], off).map_err(|err| {
                    StorageError::io("log_read_failed", "read diagnostic log range", err)
                })?;
            if read == 0 {
                return Err(StorageError::integrity(
                    "log_truncated",
                    "diagnostic log truncated during range read",
                ));
            }
            filled += read;
        }
        let after = ArtifactFileIdentity::from_fd(self.file.as_raw_fd())?;
        if !after.matches(&self.identity, self.is_exfat) {
            return Err(StorageError::integrity(
                "log_changed",
                "diagnostic log changed during range read",
            ));
        }
        if let (Ok(off), Ok(l)) = (i64::try_from(start), i64::try_from(length)) {
            let _ = fadvise_dontneed(self.file.as_raw_fd(), off, l);
        }
        Ok((range, out))
    }

    pub fn evict_page_cache(&self) {
        let _ = fadvise_dontneed(self.file.as_raw_fd(), 0, 0);
    }
}

type VolumeReadyFn = Arc<dyn Fn() -> bool + Send + Sync>;

/// Safe `/data/logs/` diagnostic store.
#[derive(Clone)]
pub struct LogStore {
    paths: DataVolumePaths,
    volume_ready: VolumeReadyFn,
    handle_counter: Option<Arc<AtomicUsize>>,
}

impl LogStore {
    pub fn new(paths: DataVolumePaths, volume_ready: VolumeReadyFn) -> Self {
        Self {
            paths,
            volume_ready,
            handle_counter: None,
        }
    }

    pub fn with_handle_counter(mut self, counter: Arc<AtomicUsize>) -> Self {
        self.handle_counter = Some(counter);
        self
    }

    /// List visible regular diagnostic logs under `/data/logs/`, newest `modified_at` first.
    pub fn list_logs(
        &self,
        prefix: Option<&str>,
        limit: Option<usize>,
        volume_id: Option<String>,
    ) -> StorageResult<LogList> {
        let normalized_prefix = match prefix {
            Some(raw) => {
                validate_log_prefix(raw)?;
                Some(raw)
            }
            None => None,
        };
        let effective_limit = limit.unwrap_or(DEFAULT_LOG_LIST_LIMIT);
        if !(1..=MAX_LOG_LIST_LIMIT).contains(&effective_limit) {
            return Err(StorageError::invalid_input(
                "invalid_limit",
                format!("limit must be in 1..={MAX_LOG_LIST_LIMIT}"),
            ));
        }

        if !(self.volume_ready)() {
            return Err(StorageError::Unavailable {
                message: "recording volume is unavailable for diagnostic log listing".to_owned(),
            });
        }

        let mount_dir = open_directory_safe(self.paths.mountpoint()).map_err(|err| {
            StorageError::Unavailable {
                message: format!("recording volume mountpoint unavailable: {err}"),
            }
        })?;
        let mount_dev = fstat(mount_dir.as_raw_fd())
            .map_err(|err| StorageError::io("logs_stat_failed", "fstat volume mountpoint", err))?
            .st_dev;

        let logs_root = self.paths.logs_dir();
        let root_dir = match open_directory_safe(&logs_root) {
            Ok(dir) => dir,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Ok(LogList {
                    schema: LOG_LIST_SCHEMA.to_owned(),
                    volume_id,
                    observed_at: now_rfc3339_utc(),
                    items: Vec::new(),
                    truncated: false,
                });
            }
            Err(err) if err.raw_os_error() == Some(libc::ELOOP) => {
                return Err(StorageError::integrity(
                    "unsafe_path",
                    "diagnostic logs directory must not be a symlink",
                ));
            }
            Err(err) => {
                return Err(StorageError::Unavailable {
                    message: format!("diagnostic logs directory unavailable: {err}"),
                });
            }
        };
        let root_stat = fstat(root_dir.as_raw_fd()).map_err(|err| {
            StorageError::io("logs_stat_failed", "fstat diagnostic logs dir", err)
        })?;
        let root_dev = root_stat.st_dev;
        if root_dev != mount_dev {
            return Err(StorageError::integrity(
                "cross_device_boundary",
                "diagnostic logs directory crosses filesystem boundary",
            ));
        }

        let mut collected: Vec<(i128, LogFileEntry)> = Vec::new();
        let mut scanned = 0_usize;
        scan_logs_recursive(
            &logs_root,
            &root_dir,
            root_dev,
            "",
            0,
            normalized_prefix,
            &mut scanned,
            &mut collected,
        )?;

        // Sort newest modified_at first, deterministic ascending name tie-break.
        collected.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.name.cmp(&b.1.name)));

        let truncated = collected.len() > effective_limit || scanned >= MAX_SCANNED_FILES;
        let items: Vec<LogFileEntry> = collected
            .into_iter()
            .take(effective_limit)
            .map(|(_, entry)| entry)
            .collect();

        Ok(LogList {
            schema: LOG_LIST_SCHEMA.to_owned(),
            volume_id,
            observed_at: now_rfc3339_utc(),
            items,
            truncated,
        })
    }

    /// Open one safe regular diagnostic log file under `/data/logs/{name}`.
    pub fn open_log(&self, name: &str) -> StorageResult<OpenedLogFile> {
        validate_log_name(name)?;
        if !(self.volume_ready)() {
            return Err(StorageError::Unavailable {
                message: "recording volume is unavailable for diagnostic log download".to_owned(),
            });
        }

        let logs_root = self.paths.logs_dir();
        let mount_dir = open_directory_safe(self.paths.mountpoint()).map_err(|err| {
            StorageError::Unavailable {
                message: format!("recording volume mountpoint unavailable: {err}"),
            }
        })?;
        let mount_dev = fstat(mount_dir.as_raw_fd())
            .map_err(|err| StorageError::io("logs_stat_failed", "fstat volume mountpoint", err))?
            .st_dev;

        let root_dir = match open_directory_safe(&logs_root) {
            Ok(dir) => dir,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound {
                    session_id: name.to_owned(),
                });
            }
            Err(err) if err.raw_os_error() == Some(libc::ELOOP) => {
                return Err(StorageError::integrity(
                    "unsafe_path",
                    "diagnostic logs directory must not be a symlink",
                ));
            }
            Err(err) => {
                return Err(StorageError::Unavailable {
                    message: format!("diagnostic logs directory unavailable: {err}"),
                });
            }
        };
        let root_stat = fstat(root_dir.as_raw_fd())
            .map_err(|err| StorageError::io("logs_stat_failed", "fstat logs dir", err))?;
        let root_dev = root_stat.st_dev;
        if root_dev != mount_dev {
            return Err(StorageError::integrity(
                "cross_device_boundary",
                "diagnostic logs directory crosses filesystem boundary",
            ));
        }
        let is_exfat = is_exfat_or_msdos(root_dir.as_raw_fd()).unwrap_or(false);

        let lease = self
            .handle_counter
            .as_ref()
            .map(|c| VolumeHandleLease::acquire(Arc::clone(c)));

        let file = open_relative_regular_in_dir(&root_dir, root_dev, name)?;
        let st = fstat(file.as_raw_fd())
            .map_err(|err| StorageError::io("log_stat_failed", "fstat diagnostic log", err))?;
        if (st.st_mode & libc::S_IFMT) != libc::S_IFREG {
            return Err(StorageError::integrity(
                "invalid_log",
                "requested diagnostic log is not a regular file",
            ));
        }
        if st.st_dev != root_dev {
            return Err(StorageError::integrity(
                "cross_device_boundary",
                "diagnostic log crosses filesystem boundary",
            ));
        }
        let identity = ArtifactFileIdentity::from_stat(&st)?;
        if identity.nlink != 1 {
            return Err(StorageError::integrity(
                "invalid_log",
                "diagnostic log must not have hardlinks",
            ));
        }

        let _ = fadvise_sequential(file.as_raw_fd(), 0, 0);

        let entry = LogFileEntry {
            name: name.to_owned(),
            bytes: identity.size,
            modified_at: unix_secs_to_rfc3339_utc(st.st_mtime),
            media_type: media_type_for_log(name).to_owned(),
            sha256: None,
        };

        Ok(OpenedLogFile {
            file,
            entry,
            identity,
            is_exfat,
            lease,
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn scan_logs_recursive(
    dir_path: &Path,
    dir_file: &File,
    root_dev: u64,
    rel_prefix: &str,
    depth: usize,
    filter_prefix: Option<&str>,
    scanned: &mut usize,
    out: &mut Vec<(i128, LogFileEntry)>,
) -> StorageResult<()> {
    if depth > MAX_SCAN_DEPTH || *scanned >= MAX_SCANNED_FILES {
        return Ok(());
    }

    let entries = match fs::read_dir(dir_path) {
        Ok(it) => it,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(StorageError::io(
                "logs_readdir_failed",
                "read diagnostic logs directory",
                err,
            ));
        }
    };

    let mut names: Vec<String> = Vec::new();
    for entry in entries.flatten() {
        if let Ok(name) = entry.file_name().into_string() {
            if is_valid_log_segment(&name) {
                names.push(name);
            }
        }
    }
    names.sort_by(|a, b| match a.cmp(b) {
        Ordering::Equal => Ordering::Equal,
        other => other,
    });

    for name in names {
        if *scanned >= MAX_SCANNED_FILES {
            break;
        }
        let Ok(c_name) = CString::new(name.as_str()) else {
            continue;
        };
        let Ok(st) = fstatat_nofollow(dir_file.as_raw_fd(), &c_name) else {
            continue;
        };
        // Reject symlinks and cross-device mounts silently from visible listing.
        let file_type = st.st_mode & libc::S_IFMT;
        if file_type == libc::S_IFLNK || st.st_dev != root_dev {
            continue;
        }

        let rel_path = if rel_prefix.is_empty() {
            name.clone()
        } else {
            format!("{rel_prefix}/{name}")
        };

        if file_type == libc::S_IFDIR {
            if let Ok(child_dir) = openat_retry(
                dir_file.as_raw_fd(),
                &c_name,
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            ) {
                if let Ok(child_st) = fstat(child_dir.as_raw_fd()) {
                    if child_st.st_dev == root_dev {
                        scan_logs_recursive(
                            &dir_path.join(&name),
                            &child_dir,
                            root_dev,
                            &rel_path,
                            depth + 1,
                            filter_prefix,
                            scanned,
                            out,
                        )?;
                    }
                }
            }
            continue;
        }

        if file_type != libc::S_IFREG {
            continue;
        }
        #[allow(clippy::useless_conversion)]
        let nlink = u64::from(st.st_nlink);
        if nlink != 1 || rel_path.len() > MAX_LOG_NAME_LEN {
            continue;
        }
        let Ok(bytes) = u64::try_from(st.st_size) else {
            continue;
        };
        *scanned += 1;
        if let Some(prefix) = filter_prefix {
            if !rel_path.starts_with(prefix) {
                continue;
            }
        }
        let mtime_ns = i128::from(st.st_mtime) * 1_000_000_000 + i128::from(st.st_mtime_nsec);
        out.push((
            mtime_ns,
            LogFileEntry {
                media_type: media_type_for_log(&rel_path).to_owned(),
                name: rel_path,
                bytes,
                modified_at: unix_secs_to_rfc3339_utc(st.st_mtime),
                sha256: None,
            },
        ));
    }

    Ok(())
}

/// Strictly validate a diagnostic log file name matching OpenAPI `LogName`:
/// `^[A-Za-z0-9][A-Za-z0-9._-]*(?:/[A-Za-z0-9][A-Za-z0-9._-]*)*$`.
pub fn validate_log_name(name: &str) -> StorageResult<()> {
    if name.is_empty()
        || name.len() > MAX_LOG_NAME_LEN
        || name.starts_with('/')
        || name.ends_with('/')
    {
        return Err(StorageError::invalid_input(
            "invalid_log_name",
            "diagnostic log path is invalid",
        ));
    }
    for seg in name.split('/') {
        if !is_valid_log_segment(seg) {
            return Err(StorageError::invalid_input(
                "invalid_log_name",
                "diagnostic log path contains disallowed segment",
            ));
        }
    }
    Ok(())
}

/// Strictly validate an optional prefix filter for `GET /api/v1/logs?prefix=...`.
pub fn validate_log_prefix(prefix: &str) -> StorageResult<()> {
    if prefix.is_empty() || prefix.len() > MAX_LOG_PREFIX_LEN || prefix.starts_with('/') {
        return Err(StorageError::invalid_input(
            "invalid_log_prefix",
            "diagnostic log prefix is invalid",
        ));
    }
    let trimmed = prefix.strip_suffix('/').unwrap_or(prefix);
    if trimmed.is_empty() {
        return Err(StorageError::invalid_input(
            "invalid_log_prefix",
            "diagnostic log prefix must not be empty",
        ));
    }
    for seg in trimmed.split('/') {
        if !is_valid_log_segment(seg) {
            return Err(StorageError::invalid_input(
                "invalid_log_prefix",
                "diagnostic log prefix contains disallowed segment",
            ));
        }
    }
    Ok(())
}

fn is_valid_log_segment(seg: &str) -> bool {
    let bytes = seg.as_bytes();
    if bytes.is_empty() || seg == "." || seg == ".." {
        return false;
    }
    // Must start with ASCII alphanumeric (blocks dot-prefixed hidden entries like `.miro-ego`).
    if !bytes[0].is_ascii_alphanumeric() {
        return false;
    }
    bytes
        .iter()
        .all(|&b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// Select the OpenAPI v1 media type for a diagnostic log file name.
pub fn media_type_for_log(name: &str) -> &'static str {
    let lower = name.to_ascii_lowercase();
    if lower.ends_with(".ndjson") || lower.ends_with(".jsonl") {
        "application/x-ndjson"
    } else if lower.ends_with(".gz") {
        "application/gzip"
    } else if lower.ends_with(".log") || lower.ends_with(".txt") || lower.ends_with(".json") {
        "text/plain"
    } else {
        "application/octet-stream"
    }
}
