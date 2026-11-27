//! Pinned-descriptor manifest and artifact I/O with `st_dev` mount-boundary enforcement,
//! exFAT-safe file identity checks, RFC 9110 single byte-range parsing, and `posix_fadvise`
//! page-cache management.

use crate::util::{VolumeHandleLease, is_safe_path_segment, sha256_hex};
use crate::volume::ExfatVolumeManager;
use crate::{StorageError, StorageResult};
use miro_ego_core::ids::is_valid_sha256;
use miro_ego_core::sys::fs::{
    fadvise_dontneed, fadvise_sequential, fstat, is_exfat_or_msdos, open_directory_safe,
    openat_retry, parse_strict_json_value, pread_retry, read_retry,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

/// Maximum permitted manifest.json byte size (8 MiB).
pub const MAX_MANIFEST_BYTES: usize = 8 * 1024 * 1024;
/// Maximum JSON nesting depth permitted for manifests.
pub const MAX_MANIFEST_JSON_DEPTH: usize = 32;
/// Streaming read/verification chunk size (256 KiB).
pub const ARTIFACT_STREAM_CHUNK_BYTES: usize = 256 * 1024;

/// File identity tuple supporting exFAT-safe comparison (omitting volatile `st_ino` on exFAT).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactFileIdentity {
    pub device: u64,
    pub inode: u64,
    pub size: u64,
    pub modified_ns: i128,
    pub nlink: u64,
}

impl ArtifactFileIdentity {
    pub fn from_fd(fd: libc::c_int) -> StorageResult<Self> {
        let st = fstat(fd).map_err(|err| {
            StorageError::io("artifact_stat_failed", "fstat artifact descriptor", err)
        })?;
        Self::from_stat(&st)
    }

    pub fn from_stat(st: &libc::stat) -> StorageResult<Self> {
        let size = u64::try_from(st.st_size).map_err(|_| {
            StorageError::integrity("artifact_invalid", "artifact file has negative size")
        })?;
        let modified_ns = i128::from(st.st_mtime) * 1_000_000_000 + i128::from(st.st_mtime_nsec);
        #[allow(clippy::useless_conversion)]
        let nlink = u64::from(st.st_nlink);
        Ok(Self {
            device: st.st_dev,
            inode: st.st_ino,
            size,
            modified_ns,
            nlink,
        })
    }

    /// Compare two file identities; omits volatile kernel `st_ino` on exFAT / FAT mounts.
    pub fn matches(&self, other: &Self, is_exfat: bool) -> bool {
        if is_exfat {
            self.device == other.device
                && self.size == other.size
                && self.modified_ns == other.modified_ns
                && self.nlink == other.nlink
        } else {
            self == other
        }
    }
}

/// Artifact descriptor extracted from a sealed `miro-ego.device-session.v1` manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestArtifactDescriptor {
    pub artifact_id: String,
    pub role: String,
    pub path: String,
    pub media_type: String,
    pub bytes: u64,
    pub sha256: String,
}

impl ManifestArtifactDescriptor {
    pub fn etag(&self) -> String {
        format!("\"{}\"", self.sha256)
    }
}

/// Inclusive RFC 9110 byte range `[start, end]` within a representation of `total_bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
    pub total_bytes: u64,
}

impl ByteRange {
    pub fn len(self) -> u64 {
        self.end.saturating_sub(self.start).saturating_add(1)
    }

    pub fn is_empty(self) -> bool {
        self.total_bytes == 0 || self.start > self.end
    }

    pub fn content_range_header(self) -> String {
        format!("bytes {}-{}/{}", self.start, self.end, self.total_bytes)
    }
}

/// Parse an optional RFC 9110 single `Range` header (with optional `If-Range` ETag guard).
///
/// - Returns `Ok(None)` when no range is requested or `If-Range` does not match `etag`.
/// - Returns `Ok(Some(ByteRange))` for a valid single byte range (`206 Partial Content`).
/// - Returns `Err(StorageError::RangeNotSatisfiable { total_bytes })` (`416`) for
///   multipart ranges, invalid syntax, empty targets, or out-of-bounds intervals.
pub fn parse_single_byte_range(
    range_header: Option<&str>,
    if_range_header: Option<&str>,
    etag: &str,
    total_bytes: u64,
) -> StorageResult<Option<ByteRange>> {
    let Some(raw_range) = range_header else {
        return Ok(None);
    };
    if let Some(if_range) = if_range_header {
        if if_range.trim() != etag {
            return Ok(None);
        }
    }

    let trimmed = raw_range.trim();
    let Some(spec) = trimmed.strip_prefix("bytes=") else {
        return Err(StorageError::RangeNotSatisfiable { total_bytes });
    };
    // Multipart ranges are explicitly unsupported by Miro-Ego v1.
    if spec.is_empty() || spec.contains(',') {
        return Err(StorageError::RangeNotSatisfiable { total_bytes });
    }
    if total_bytes == 0 {
        return Err(StorageError::RangeNotSatisfiable { total_bytes });
    }

    let Some((start_str, end_str)) = spec.split_once('-') else {
        return Err(StorageError::RangeNotSatisfiable { total_bytes });
    };

    let (start, end) = if start_str.is_empty() {
        // Suffix range: bytes=-N
        if end_str.is_empty() || !end_str.bytes().all(|b| b.is_ascii_digit()) {
            return Err(StorageError::RangeNotSatisfiable { total_bytes });
        }
        let suffix: u64 = end_str
            .parse()
            .map_err(|_| StorageError::RangeNotSatisfiable { total_bytes })?;
        if suffix == 0 {
            return Err(StorageError::RangeNotSatisfiable { total_bytes });
        }
        let start = total_bytes.saturating_sub(suffix);
        (start, total_bytes - 1)
    } else {
        if !start_str.bytes().all(|b| b.is_ascii_digit()) {
            return Err(StorageError::RangeNotSatisfiable { total_bytes });
        }
        let start: u64 = start_str
            .parse()
            .map_err(|_| StorageError::RangeNotSatisfiable { total_bytes })?;
        if start >= total_bytes {
            return Err(StorageError::RangeNotSatisfiable { total_bytes });
        }
        let end = if end_str.is_empty() {
            total_bytes - 1
        } else {
            if !end_str.bytes().all(|b| b.is_ascii_digit()) {
                return Err(StorageError::RangeNotSatisfiable { total_bytes });
            }
            let parsed_end: u64 = end_str
                .parse()
                .map_err(|_| StorageError::RangeNotSatisfiable { total_bytes })?;
            if parsed_end < start {
                return Err(StorageError::RangeNotSatisfiable { total_bytes });
            }
            parsed_end.min(total_bytes - 1)
        };
        (start, end)
    };

    Ok(Some(ByteRange {
        start,
        end,
        total_bytes,
    }))
}

/// Exact stored bytes and parsed structure of a session's `manifest.json`.
#[derive(Debug, Clone)]
pub struct OpenedManifest {
    pub raw_bytes: Vec<u8>,
    pub sha256: String,
    pub etag: String,
    pub parsed: Value,
    pub is_exfat: bool,
}

/// Pinned, verified regular artifact file handle ready for range-bounded streaming.
pub struct VerifiedArtifactFile {
    file: File,
    pub descriptor: ManifestArtifactDescriptor,
    pub identity: ArtifactFileIdentity,
    pub is_exfat: bool,
    lease: Option<VolumeHandleLease>,
}

impl VerifiedArtifactFile {
    pub fn file(&self) -> &File {
        &self.file
    }

    pub fn as_raw_fd(&self) -> libc::c_int {
        self.file.as_raw_fd()
    }

    pub fn take_lease(&mut self) -> Option<VolumeHandleLease> {
        self.lease.take()
    }

    /// Re-verify that the open file descriptor still matches its pinned identity.
    pub fn verify_unchanged(&self) -> StorageResult<()> {
        let current = ArtifactFileIdentity::from_fd(self.file.as_raw_fd())?;
        if !current.matches(&self.identity, self.is_exfat) {
            return Err(StorageError::integrity(
                "artifact_changed",
                "artifact identity changed while open",
            ));
        }
        Ok(())
    }

    /// Read a chunk at `offset` via `pread` without mutating file cursor state.
    pub fn pread_chunk(&self, offset: u64, buf: &mut [u8]) -> StorageResult<usize> {
        let off = libc::off_t::try_from(offset)
            .map_err(|_| StorageError::invalid_input("range_overflow", "offset out of range"))?;
        pread_retry(self.file.as_raw_fd(), buf, off)
            .map_err(|err| StorageError::io("artifact_read_failed", "pread artifact chunk", err))
    }

    /// Read an exact [`ByteRange`] (or complete artifact when `range` is `None`) into memory,
    /// evicting read pages from page cache afterwards.
    pub fn read_range_bytes(&self, range: Option<ByteRange>) -> StorageResult<Vec<u8>> {
        let (start, to_read) = match range {
            Some(r) => {
                if r.total_bytes != self.descriptor.bytes || r.end >= self.descriptor.bytes {
                    return Err(StorageError::RangeNotSatisfiable {
                        total_bytes: self.descriptor.bytes,
                    });
                }
                (r.start, r.len())
            }
            None => (0, self.descriptor.bytes),
        };

        let len_usize = usize::try_from(to_read).map_err(|_| {
            StorageError::invalid_input("range_too_large", "requested range exceeds address space")
        })?;
        let mut out = vec![0_u8; len_usize];
        let mut filled = 0_usize;
        while filled < len_usize {
            let chunk_end = (filled + ARTIFACT_STREAM_CHUNK_BYTES).min(len_usize);
            let read = self.pread_chunk(start + filled as u64, &mut out[filled..chunk_end])?;
            if read == 0 {
                return Err(StorageError::integrity(
                    "artifact_truncated",
                    "artifact ended prematurely during range read",
                ));
            }
            filled += read;
        }
        self.verify_unchanged()?;
        self.evict_range_page_cache(start, to_read);
        Ok(out)
    }

    /// Advise the kernel to evict clean pages for `[offset, offset + len)` from page cache.
    pub fn evict_range_page_cache(&self, offset: u64, len: u64) {
        if let (Ok(off), Ok(l)) = (i64::try_from(offset), i64::try_from(len)) {
            let _ = fadvise_dontneed(self.file.as_raw_fd(), off, l);
        }
    }

    /// Advise the kernel to evict all clean pages for this artifact from page cache.
    pub fn evict_page_cache(&self) {
        let _ = fadvise_dontneed(self.file.as_raw_fd(), 0, 0);
    }
}

/// Pin a relative subdirectory under `root` using `O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC`,
/// enforcing the same `st_dev` mount boundary at every component.
///
/// Returns `(pinned_dir, root_device_id, is_exfat)`.
pub fn open_pinned_directory(root: &Path, relative_dir: &str) -> StorageResult<(File, u64, bool)> {
    let root_dir = open_directory_safe(root).map_err(|err| {
        if err.kind() == io::ErrorKind::NotFound {
            StorageError::Unavailable {
                message: format!("recordings root unavailable: {}", root.display()),
            }
        } else {
            StorageError::io("dir_open_failed", "open root directory", err)
        }
    })?;
    let root_stat = fstat(root_dir.as_raw_fd())
        .map_err(|err| StorageError::io("dir_stat_failed", "fstat root directory", err))?;
    let root_dev = root_stat.st_dev;
    let exfat = is_exfat_or_msdos(root_dir.as_raw_fd()).unwrap_or(false);

    let components = parse_relative_components(relative_dir)?;
    let mut current = root_dir;
    for component in components {
        let c_name = CString::new(component)
            .map_err(|_| StorageError::invalid_input("invalid_path", "path contains NUL byte"))?;
        let next = openat_retry(
            current.as_raw_fd(),
            &c_name,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
        .map_err(|err| map_open_err("session_dir_open_failed", err))?;
        let next_stat = fstat(next.as_raw_fd())
            .map_err(|err| StorageError::io("dir_stat_failed", "fstat child directory", err))?;
        if next_stat.st_dev != root_dev {
            return Err(StorageError::integrity(
                "cross_device_boundary",
                "session directory crosses filesystem boundary",
            ));
        }
        current = next;
    }

    Ok((current, root_dev, exfat))
}

/// Safely open and read `manifest.json` from `recordings_root/<relative_dir>` with pinned fds,
/// exact byte preservation, `st_dev` verification, and strict JSON validation.
pub fn open_manifest(
    recordings_root: &Path,
    relative_dir: &str,
    expected_session_id: Option<&str>,
) -> StorageResult<OpenedManifest> {
    let (session_dir, root_dev, exfat) = open_pinned_directory(recordings_root, relative_dir)?;
    open_manifest_in_dir(&session_dir, root_dev, exfat, expected_session_id)
}

pub(crate) fn open_manifest_in_dir(
    session_dir: &File,
    root_dev: u64,
    exfat: bool,
    expected_session_id: Option<&str>,
) -> StorageResult<OpenedManifest> {
    let manifest_c = CString::new("manifest.json")
        .map_err(|_| StorageError::invalid_input("invalid_path", "invalid manifest filename"))?;
    let mut file = openat_retry(
        session_dir.as_raw_fd(),
        &manifest_c,
        libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
    )
    .map_err(|err| map_open_err("manifest_open_failed", err))?;

    let before_raw = fstat(file.as_raw_fd())
        .map_err(|err| StorageError::io("manifest_stat_failed", "fstat manifest.json", err))?;
    if (before_raw.st_mode & libc::S_IFMT) != libc::S_IFREG {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "manifest.json is not a regular file",
        ));
    }
    if before_raw.st_dev != root_dev {
        return Err(StorageError::integrity(
            "cross_device_boundary",
            "manifest.json crosses filesystem boundary",
        ));
    }
    let before = ArtifactFileIdentity::from_stat(&before_raw)?;
    if before.nlink != 1 {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "manifest.json must not have hardlinks",
        ));
    }
    if before.size > MAX_MANIFEST_BYTES as u64 {
        return Err(StorageError::integrity(
            "manifest_too_large",
            "manifest.json exceeds maximum permitted size",
        ));
    }

    let limit = MAX_MANIFEST_BYTES.saturating_add(1);
    let mut raw_bytes = Vec::with_capacity((before.size as usize).min(64 * 1024));
    let mut chunk = [0_u8; 8192];
    while raw_bytes.len() < limit {
        let max_read = chunk.len().min(limit - raw_bytes.len());
        let read = read_retry(&mut file, &mut chunk[..max_read])
            .map_err(|err| StorageError::io("manifest_read_failed", "read manifest.json", err))?;
        if read == 0 {
            break;
        }
        raw_bytes.extend_from_slice(&chunk[..read]);
    }
    if raw_bytes.len() > MAX_MANIFEST_BYTES || raw_bytes.len() as u64 != before.size {
        return Err(StorageError::integrity(
            "manifest_changed",
            "manifest.json size changed during read",
        ));
    }
    let after = ArtifactFileIdentity::from_fd(file.as_raw_fd())?;
    if !after.matches(&before, exfat) {
        return Err(StorageError::integrity(
            "manifest_changed",
            "manifest.json identity changed during read",
        ));
    }

    let sha256 = sha256_hex(&raw_bytes);
    let etag = format!("\"{sha256}\"");
    let parsed = parse_strict_json_value(&raw_bytes, MAX_MANIFEST_JSON_DEPTH).map_err(|err| {
        StorageError::integrity("manifest_invalid", format!("manifest JSON invalid: {err}"))
    })?;

    if let Some(expected_sid) = expected_session_id {
        let actual_sid = parsed
            .as_object()
            .and_then(|o| o.get("session_id"))
            .and_then(Value::as_str);
        if actual_sid != Some(expected_sid) {
            return Err(StorageError::integrity(
                "manifest_invalid",
                "manifest session_id does not match expected session",
            ));
        }
    }

    Ok(OpenedManifest {
        raw_bytes,
        sha256,
        etag,
        parsed,
        is_exfat: exfat,
    })
}

/// Safely open a verified artifact inside `recordings_root/<relative_dir>` addressed by its
/// manifest `artifact_id` (64-char lowercase SHA-256).
///
/// Enforces:
/// - Pinned directory traversal with `O_NOFOLLOW | O_CLOEXEC` at every component
/// - Strict `st_dev` mount-boundary equality across root, directories, and target file
/// - Regular file (`S_ISREG`) with `st_nlink == 1` and exact byte size matching manifest
/// - `posix_fadvise(POSIX_FADV_SEQUENTIAL)` before streaming and `POSIX_FADV_DONTNEED` after hash verification
/// - Optional full SHA-256 verification with exFAT-safe post-read identity verification
pub fn open_verified_artifact(
    recordings_root: &Path,
    relative_dir: &str,
    artifact_id: &str,
    expected_session_id: Option<&str>,
    verify_digest: bool,
) -> StorageResult<VerifiedArtifactFile> {
    open_verified_artifact_with_lease(
        recordings_root,
        relative_dir,
        artifact_id,
        expected_session_id,
        verify_digest,
        None,
    )
}

/// Safely open a verified artifact while optionally acquiring an open-handle lease on `handle_counter`.
pub fn open_verified_artifact_with_lease(
    recordings_root: &Path,
    relative_dir: &str,
    artifact_id: &str,
    expected_session_id: Option<&str>,
    verify_digest: bool,
    handle_counter: Option<&Arc<AtomicUsize>>,
) -> StorageResult<VerifiedArtifactFile> {
    open_verified_artifact_checked_with_lease(
        recordings_root,
        relative_dir,
        artifact_id,
        expected_session_id,
        None,
        verify_digest,
        handle_counter,
    )
}

/// Safely open a verified artifact while optionally checking `expected_manifest_sha256`
/// against the single read of `manifest.json` and acquiring an open-handle lease on `handle_counter`.
pub fn open_verified_artifact_checked_with_lease(
    recordings_root: &Path,
    relative_dir: &str,
    artifact_id: &str,
    expected_session_id: Option<&str>,
    expected_manifest_sha256: Option<&str>,
    verify_digest: bool,
    handle_counter: Option<&Arc<AtomicUsize>>,
) -> StorageResult<VerifiedArtifactFile> {
    if !is_valid_sha256(artifact_id) {
        return Err(StorageError::invalid_input(
            "invalid_artifact_id",
            "artifact_id must be a 64-character lowercase SHA-256 digest",
        ));
    }

    let lease = handle_counter.map(|c| VolumeHandleLease::acquire(Arc::clone(c)));
    open_verified_artifact_inner(
        recordings_root,
        relative_dir,
        artifact_id,
        expected_session_id,
        expected_manifest_sha256,
        verify_digest,
        lease,
    )
}

/// Safely open a verified artifact gated atomically via [`ExfatVolumeManager::try_acquire_handle_lease`]
/// (`SeqCst` increment-then-check-`released` barrier) so zero descriptors open after `SafeSwap`.
pub fn open_verified_artifact_checked_with_volume(
    recordings_root: &Path,
    relative_dir: &str,
    artifact_id: &str,
    expected_session_id: Option<&str>,
    expected_manifest_sha256: Option<&str>,
    verify_digest: bool,
    volume: Option<&ExfatVolumeManager>,
) -> StorageResult<VerifiedArtifactFile> {
    if !is_valid_sha256(artifact_id) {
        return Err(StorageError::invalid_input(
            "invalid_artifact_id",
            "artifact_id must be a 64-character lowercase SHA-256 digest",
        ));
    }

    let lease = match volume {
        Some(vol) => {
            Some(
                vol.try_acquire_handle_lease()
                    .ok_or_else(|| StorageError::Unavailable {
                        message: "recording volume has been released for safe swap".to_owned(),
                    })?,
            )
        }
        None => None,
    };
    open_verified_artifact_inner(
        recordings_root,
        relative_dir,
        artifact_id,
        expected_session_id,
        expected_manifest_sha256,
        verify_digest,
        lease,
    )
}

fn open_verified_artifact_inner(
    recordings_root: &Path,
    relative_dir: &str,
    artifact_id: &str,
    expected_session_id: Option<&str>,
    expected_manifest_sha256: Option<&str>,
    verify_digest: bool,
    lease: Option<VolumeHandleLease>,
) -> StorageResult<VerifiedArtifactFile> {
    let (session_dir, root_dev, exfat) = open_pinned_directory(recordings_root, relative_dir)?;
    let manifest = open_manifest_in_dir(&session_dir, root_dev, exfat, expected_session_id)?;
    if let Some(expected_sha) = expected_manifest_sha256 {
        if manifest.sha256 != expected_sha {
            return Err(StorageError::integrity(
                "manifest_sha256_mismatch",
                "manifest.json sha256 does not match catalog record",
            ));
        }
    }
    let descriptors = extract_manifest_artifacts(&manifest.parsed)?;
    let descriptor = descriptors
        .into_iter()
        .find(|d| d.artifact_id == artifact_id)
        .ok_or_else(|| StorageError::NotFound {
            session_id: artifact_id.to_owned(),
        })?;

    let file =
        open_relative_regular_in_dir(&session_dir, root_dev, &descriptor.path).map_err(|err| {
            match err {
                StorageError::NotFound { .. } => StorageError::integrity(
                    "artifact_missing",
                    format!(
                        "declared manifest artifact '{}' is missing on disk",
                        descriptor.path
                    ),
                ),
                other => other,
            }
        })?;
    let raw_stat = fstat(file.as_raw_fd())
        .map_err(|err| StorageError::io("artifact_stat_failed", "fstat artifact", err))?;
    if (raw_stat.st_mode & libc::S_IFMT) != libc::S_IFREG {
        return Err(StorageError::integrity(
            "artifact_invalid",
            "artifact is not a regular file",
        ));
    }
    if raw_stat.st_dev != root_dev {
        return Err(StorageError::integrity(
            "cross_device_boundary",
            "artifact crosses filesystem boundary",
        ));
    }
    let identity = ArtifactFileIdentity::from_stat(&raw_stat)?;
    if identity.nlink != 1 {
        return Err(StorageError::integrity(
            "artifact_invalid",
            "artifact must not have hardlinks",
        ));
    }
    if identity.size != descriptor.bytes {
        return Err(StorageError::integrity(
            "artifact_size_mismatch",
            format!(
                "artifact byte size ({}) does not match manifest ({})",
                identity.size, descriptor.bytes
            ),
        ));
    }

    let _ = fadvise_sequential(file.as_raw_fd(), 0, 0);

    if verify_digest {
        let mut hasher = Sha256::new();
        let mut remaining = descriptor.bytes;
        let mut offset = 0_u64;
        let buf_len = usize::try_from(
            descriptor
                .bytes
                .clamp(1, ARTIFACT_STREAM_CHUNK_BYTES as u64),
        )
        .unwrap_or(ARTIFACT_STREAM_CHUNK_BYTES);
        let mut buf = vec![0_u8; buf_len];

        while remaining > 0 {
            let to_read = usize::try_from(remaining.min(buf.len() as u64)).unwrap_or(buf.len());
            let off = libc::off_t::try_from(offset).map_err(|_| {
                StorageError::integrity("artifact_invalid", "artifact offset overflow")
            })?;
            let read = pread_retry(file.as_raw_fd(), &mut buf[..to_read], off).map_err(|err| {
                StorageError::io(
                    "artifact_read_failed",
                    "pread artifact for verification",
                    err,
                )
            })?;
            if read == 0 {
                return Err(StorageError::integrity(
                    "artifact_truncated",
                    "artifact truncated during verification",
                ));
            }
            hasher.update(&buf[..read]);
            remaining -= read as u64;
            offset += read as u64;
        }

        let after = ArtifactFileIdentity::from_fd(file.as_raw_fd())?;
        if !after.matches(&identity, exfat) {
            return Err(StorageError::integrity(
                "artifact_changed",
                "artifact changed during verification",
            ));
        }

        let actual = miro_ego_core::ids::hex_encode(hasher.finalize().as_slice());
        if actual != descriptor.sha256 {
            return Err(StorageError::integrity(
                "artifact_digest_mismatch",
                "artifact SHA-256 does not match manifest",
            ));
        }

        // Evict verification pages so large session reads do not thrash page cache;
        // subsequent range streaming re-advises sequential read-ahead cleanly.
        let _ = fadvise_dontneed(file.as_raw_fd(), 0, 0);
        let _ = fadvise_sequential(file.as_raw_fd(), 0, 0);
    }

    Ok(VerifiedArtifactFile {
        file,
        descriptor,
        identity,
        is_exfat: exfat,
        lease,
    })
}

/// Extract unique downloadable artifact descriptors from a parsed `miro-ego.device-session.v1` manifest.
pub fn extract_manifest_artifacts(root: &Value) -> StorageResult<Vec<ManifestArtifactDescriptor>> {
    extract_manifest_artifacts_impl(root, true)
}

/// Extract every declared artifact path descriptor from a parsed `miro-ego.device-session.v1`
/// manifest without deduplicating shared SHA-256 digests, so integrity verification checks every
/// declared path on disk.
pub(crate) fn extract_all_manifest_artifacts(
    root: &Value,
) -> StorageResult<Vec<ManifestArtifactDescriptor>> {
    extract_manifest_artifacts_impl(root, false)
}

fn extract_manifest_artifacts_impl(
    root: &Value,
    dedup_by_id: bool,
) -> StorageResult<Vec<ManifestArtifactDescriptor>> {
    let obj = root.as_object().ok_or_else(|| {
        StorageError::integrity("manifest_invalid", "manifest root must be object")
    })?;

    let mut raw_descriptors: Vec<&Value> = Vec::new();

    let video = obj
        .get("video")
        .and_then(Value::as_object)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing video section"))?;
    if video.get("layout").and_then(Value::as_str) != Some("split-eyes") {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "video.layout must be 'split-eyes'",
        ));
    }
    let segments = video
        .get("segments")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "missing video.segments array")
        })?;
    if segments.is_empty() {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "video.segments must contain at least one segment",
        ));
    }
    for seg in segments {
        let seg_obj = seg.as_object().ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "video segment must be object")
        })?;
        let arts = seg_obj
            .get("artifacts")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                StorageError::integrity(
                    "manifest_invalid",
                    "video segment missing artifacts object",
                )
            })?;
        if let Some(left) = arts.get("left") {
            raw_descriptors.push(left);
        } else {
            return Err(StorageError::integrity(
                "manifest_invalid",
                "video segment missing left artifact",
            ));
        }
        if let Some(right) = arts.get("right") {
            raw_descriptors.push(right);
        } else {
            return Err(StorageError::integrity(
                "manifest_invalid",
                "video segment missing right artifact",
            ));
        }
    }

    for section in ["frames", "imu"] {
        let sec_obj = obj.get(section).and_then(Value::as_object).ok_or_else(|| {
            StorageError::integrity("manifest_invalid", format!("missing {section} section"))
        })?;
        let art = sec_obj.get("artifact").ok_or_else(|| {
            StorageError::integrity("manifest_invalid", format!("missing {section}.artifact"))
        })?;
        raw_descriptors.push(art);
    }

    let audio = obj
        .get("audio")
        .and_then(Value::as_object)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing audio section"))?;
    match audio.get("state").and_then(Value::as_str) {
        Some("recorded") => {
            let segments = audio
                .get("segments")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    StorageError::integrity(
                        "manifest_invalid",
                        "recorded audio missing segments array",
                    )
                })?;
            if segments.is_empty() {
                return Err(StorageError::integrity(
                    "manifest_invalid",
                    "recorded audio must contain at least one segment",
                ));
            }
            for seg in segments {
                let art = seg
                    .as_object()
                    .and_then(|s| s.get("artifact"))
                    .ok_or_else(|| {
                        StorageError::integrity(
                            "manifest_invalid",
                            "audio segment missing artifact",
                        )
                    })?;
                raw_descriptors.push(art);
            }
        }
        Some("not_recorded") => {}
        _ => {
            return Err(StorageError::integrity(
                "manifest_invalid",
                "audio.state must be 'recorded' or 'not_recorded'",
            ));
        }
    }

    let logs = obj
        .get("logs")
        .and_then(Value::as_array)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "missing logs array"))?;
    for log in logs {
        raw_descriptors.push(log);
    }

    let mut seen_paths = HashSet::new();
    let mut by_id: HashMap<String, ManifestArtifactDescriptor> = HashMap::new();
    let mut out = Vec::new();

    for raw in raw_descriptors {
        let desc = parse_artifact_descriptor(raw)?;
        if !seen_paths.insert(desc.path.clone()) {
            return Err(StorageError::integrity(
                "manifest_invalid",
                format!("duplicate manifest artifact path: {}", desc.path),
            ));
        }
        if let Some(existing) = by_id.get(&desc.artifact_id) {
            if existing.bytes != desc.bytes
                || existing.media_type != desc.media_type
                || existing.sha256 != desc.sha256
            {
                return Err(StorageError::integrity(
                    "manifest_invalid",
                    "conflicting representation metadata for duplicate artifact_id",
                ));
            }
            if !dedup_by_id {
                out.push(desc);
            }
        } else {
            by_id.insert(desc.artifact_id.clone(), desc.clone());
            out.push(desc);
        }
    }

    if out.is_empty() {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "manifest declares no artifacts",
        ));
    }

    Ok(out)
}

/// Compute total unique downloadable artifact bytes from a parsed manifest (matching OpenAPI `SessionSummary.total_bytes`).
pub fn unique_downloadable_artifact_bytes(root: &Value) -> StorageResult<u64> {
    let descriptors = extract_manifest_artifacts(root)?;
    let mut total = 0_u64;
    for desc in descriptors {
        total = total.checked_add(desc.bytes).ok_or_else(|| {
            StorageError::integrity("manifest_invalid", "total artifact bytes overflow")
        })?;
    }
    Ok(total)
}

fn parse_artifact_descriptor(raw: &Value) -> StorageResult<ManifestArtifactDescriptor> {
    let obj = raw
        .as_object()
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "artifact must be object"))?;

    let artifact_id = req_str(obj, "artifact_id")?;
    let role = req_str(obj, "role")?;
    let path = req_str(obj, "path")?;
    let media_type = req_str(obj, "media_type")?;
    let sha256 = req_str(obj, "sha256")?;
    let bytes = obj
        .get("bytes")
        .and_then(Value::as_u64)
        .ok_or_else(|| StorageError::integrity("manifest_invalid", "artifact bytes invalid"))?;

    if !is_valid_sha256(artifact_id) || !is_valid_sha256(sha256) || artifact_id != sha256 {
        return Err(StorageError::integrity(
            "manifest_invalid",
            "artifact_id and sha256 must be matching 64-char lowercase SHA-256 digests",
        ));
    }
    validate_artifact_relative_path(path)?;

    Ok(ManifestArtifactDescriptor {
        artifact_id: artifact_id.to_owned(),
        role: role.to_owned(),
        path: path.to_owned(),
        media_type: media_type.to_owned(),
        bytes,
        sha256: sha256.to_owned(),
    })
}

fn req_str<'a>(obj: &'a Map<String, Value>, key: &str) -> StorageResult<&'a str> {
    obj.get(key).and_then(Value::as_str).ok_or_else(|| {
        StorageError::integrity("manifest_invalid", format!("missing string field '{key}'"))
    })
}

/// Strictly validate a manifest artifact relative path.
pub fn validate_artifact_relative_path(path: &str) -> StorageResult<()> {
    let components = parse_relative_components(path)?;
    if let Some(&first) = components.first()
        && (first.eq_ignore_ascii_case("manifest.json")
            || first.eq_ignore_ascii_case("recording.json"))
    {
        return Err(StorageError::invalid_input(
            "invalid_path",
            "control files cannot be accessed as artifacts",
        ));
    }
    for comp in &components {
        if comp.to_ascii_lowercase().contains(".tmp") {
            return Err(StorageError::invalid_input(
                "invalid_path",
                "temporary files cannot be accessed as artifacts",
            ));
        }
    }
    Ok(())
}

pub(crate) fn parse_relative_components(relative: &str) -> StorageResult<Vec<&str>> {
    if relative.is_empty()
        || relative.len() > 1024
        || relative.starts_with('/')
        || relative.ends_with('/')
        || relative.contains('\\')
    {
        return Err(StorageError::invalid_input(
            "invalid_path",
            "path must be a non-empty normalized relative path",
        ));
    }
    let mut parts = Vec::new();
    for segment in relative.split('/') {
        if !is_safe_path_segment(segment) {
            return Err(StorageError::invalid_input(
                "invalid_path",
                "path contains illegal traversal or control segment",
            ));
        }
        parts.push(segment);
    }
    Ok(parts)
}

pub(crate) fn open_relative_regular_in_dir(
    root_dir: &File,
    root_dev: u64,
    relative_path: &str,
) -> StorageResult<File> {
    let components = parse_relative_components(relative_path)?;
    let (parents, file_name) = components.split_at(components.len() - 1);

    let mut owned_parent: Option<File> = None;
    for comp in parents {
        let dir_fd = owned_parent
            .as_ref()
            .map_or(root_dir.as_raw_fd(), AsRawFd::as_raw_fd);
        let c_comp = CString::new(*comp)
            .map_err(|_| StorageError::invalid_input("invalid_path", "path contains NUL byte"))?;
        let next = openat_retry(
            dir_fd,
            &c_comp,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
        .map_err(|err| map_open_err("artifact_dir_open_failed", err))?;
        let st = fstat(next.as_raw_fd())
            .map_err(|err| StorageError::io("artifact_stat_failed", "fstat parent dir", err))?;
        if st.st_dev != root_dev {
            return Err(StorageError::integrity(
                "cross_device_boundary",
                "artifact parent directory crosses filesystem boundary",
            ));
        }
        owned_parent = Some(next);
    }

    let parent_fd = owned_parent
        .as_ref()
        .map_or(root_dir.as_raw_fd(), AsRawFd::as_raw_fd);
    let c_file = CString::new(file_name[0])
        .map_err(|_| StorageError::invalid_input("invalid_path", "file name contains NUL byte"))?;
    openat_retry(
        parent_fd,
        &c_file,
        libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
    )
    .map_err(|err| map_open_err("artifact_open_failed", err))
}

fn map_open_err(code: &'static str, err: io::Error) -> StorageError {
    match err.raw_os_error() {
        Some(libc::ENOENT) => StorageError::NotFound {
            session_id: "resource".to_owned(),
        },
        Some(libc::ELOOP | libc::ENOTDIR) => StorageError::integrity(
            "unsafe_path",
            format!("rejected symlink or invalid path component: {err}"),
        ),
        _ => StorageError::io(code, "open resource", err),
    }
}
