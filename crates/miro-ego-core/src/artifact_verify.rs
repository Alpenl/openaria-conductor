//! Symlink-safe, exFAT-aware regular artifact hashing and pre-publish identity verification
//! shared by capture and storage layers.

use crate::ids::{hex_encode, is_valid_sha256};
use crate::media_types::SegmentEvent;
use crate::sys::fs::{
    dup_cloexec, fadvise_dontneed, fadvise_sequential, fstat, is_exfat_or_msdos, openat_retry,
    read_retry,
};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

const READ_CHUNK: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactDescriptor {
    pub artifact_id: String,
    pub role: String,
    pub path: String,
    pub media_type: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionIoError {
    pub code: &'static str,
    pub message: String,
}

impl SessionIoError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn os(code: &'static str, context: &str, error: std::io::Error) -> Self {
        Self::new(code, format!("{context}: {error}"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileIdentity {
    pub device: u64,
    pub inode: u64,
    pub size: u64,
    pub modified_ns: i64,
    pub nlink: u64,
}

impl FileIdentity {
    pub fn from_fd(fd: libc::c_int) -> Result<Self, SessionIoError> {
        let raw = fstat_raw(fd)?;
        identity_from_stat(&raw)
    }

    pub fn require_regular(&self, mode: libc::mode_t) -> Result<(), SessionIoError> {
        if (mode & libc::S_IFMT) != libc::S_IFREG {
            return Err(SessionIoError::new(
                "artifact_invalid",
                "artifact is not a regular file",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileDigest {
    pub identity: FileIdentity,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedArtifactIdentity {
    pub path: String,
    pub identity: FileIdentity,
}

pub fn hash_file(path: &Path) -> Result<FileDigest, SessionIoError> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|error| SessionIoError::os("artifact_invalid", "open artifact failed", error))?;
    hash_opened_regular_file(file)
}

fn hash_opened_regular_file(mut file: File) -> Result<FileDigest, SessionIoError> {
    let raw = fstat_raw(file.as_raw_fd())?;
    let before = identity_from_stat(&raw)?;
    before.require_regular(raw.st_mode)?;
    if before.nlink != 1 {
        return Err(SessionIoError::new(
            "artifact_invalid",
            "artifact must not have hardlinks",
        ));
    }
    let _ = fadvise_sequential(file.as_raw_fd(), 0, 0);
    let mut hasher = Sha256::new();
    let chunk_len = usize::try_from(before.size.clamp(1, READ_CHUNK as u64)).unwrap_or(READ_CHUNK);
    let mut buffer = vec![0_u8; chunk_len];
    loop {
        let read = read_retry(&mut file, &mut buffer).map_err(|error| {
            SessionIoError::os("artifact_invalid", "read artifact failed", error)
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let _ = fadvise_dontneed(file.as_raw_fd(), 0, 0);
    let after = FileIdentity::from_fd(file.as_raw_fd())?;
    if after != before {
        return Err(SessionIoError::new(
            "artifact_invalid",
            "artifact mutated while computing sha256",
        ));
    }
    Ok(FileDigest {
        identity: before,
        sha256: hex_encode(hasher.finalize().as_slice()),
    })
}

pub fn finalize_artifact(
    path: &Path,
    expected_bytes: Option<u64>,
) -> Result<FileDigest, SessionIoError> {
    finalize_artifact_with_digest(path, expected_bytes, None)
}

/// Finalize a regular artifact for session sealing.
///
/// When `precomputed_sha256` is supplied, verifies file identity and byte size with
/// zero disk reread; otherwise streams SHA-256 over the file.
pub fn finalize_artifact_with_digest(
    path: &Path,
    expected_bytes: Option<u64>,
    precomputed_sha256: Option<&str>,
) -> Result<FileDigest, SessionIoError> {
    if let Some(precomputed) = precomputed_sha256 {
        if !is_valid_sha256(precomputed) {
            return Err(SessionIoError::new(
                "artifact_invalid",
                "precomputed SHA-256 format is invalid",
            ));
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
            .map_err(|error| {
                SessionIoError::os("artifact_invalid", "open artifact failed", error)
            })?;
        return finalize_opened_file_with_precomputed_digest(file, expected_bytes, precomputed);
    }

    let digest = hash_file(path)?;
    if let Some(expected) = expected_bytes
        && digest.identity.size != expected
    {
        return Err(SessionIoError::new(
            "artifact_invalid",
            "artifact byte size differs from declared length",
        ));
    }
    Ok(digest)
}

fn finalize_opened_file_with_precomputed_digest(
    file: File,
    expected_bytes: Option<u64>,
    precomputed: &str,
) -> Result<FileDigest, SessionIoError> {
    let raw = fstat_raw(file.as_raw_fd())?;
    let identity = identity_from_stat(&raw)?;
    identity.require_regular(raw.st_mode)?;
    if identity.nlink != 1 {
        return Err(SessionIoError::new(
            "artifact_invalid",
            "artifact must not have hardlinks",
        ));
    }
    if let Some(expected) = expected_bytes
        && identity.size != expected
    {
        return Err(SessionIoError::new(
            "artifact_invalid",
            "artifact byte size differs from declared length",
        ));
    }
    Ok(FileDigest {
        identity,
        sha256: precomputed.to_owned(),
    })
}

fn finalize_relative_artifact_with_digest(
    root_fd: libc::c_int,
    relative_path: &str,
    expected_bytes: Option<u64>,
    precomputed_sha256: Option<&str>,
) -> Result<FileDigest, SessionIoError> {
    if let Some(precomputed) = precomputed_sha256
        && !is_valid_sha256(precomputed)
    {
        return Err(SessionIoError::new(
            "artifact_invalid",
            "precomputed SHA-256 format is invalid",
        ));
    }
    let descriptor = open_relative_regular(root_fd, relative_path)?;
    // SAFETY: `open_relative_regular` returns a freshly opened, owned file descriptor.
    let file = unsafe { File::from_raw_fd(descriptor) };
    if let Some(precomputed) = precomputed_sha256 {
        return finalize_opened_file_with_precomputed_digest(file, expected_bytes, precomputed);
    }
    let digest = hash_opened_regular_file(file)?;
    if let Some(expected) = expected_bytes
        && digest.identity.size != expected
    {
        return Err(SessionIoError::new(
            "artifact_invalid",
            "artifact byte size differs from declared length",
        ));
    }
    Ok(digest)
}

/// Finalize both left and right MP4 artifacts of an encoder [`SegmentEvent`], using
/// pre-supplied streaming digests with zero reread when available.
pub fn finalize_segment_artifacts(
    session_root: &Path,
    segment: &SegmentEvent,
) -> Result<(FileDigest, FileDigest), SessionIoError> {
    validate_relative_artifact_path(&segment.left_path)?;
    validate_relative_artifact_path(&segment.right_path)?;
    let root = open_directory(
        session_root,
        "artifact_invalid",
        "open session root directory failed",
    )?;
    let left = finalize_relative_artifact_with_digest(
        root.as_raw_fd(),
        &segment.left_path,
        Some(segment.left_bytes),
        segment.left_sha256.as_deref(),
    )?;
    let right = finalize_relative_artifact_with_digest(
        root.as_raw_fd(),
        &segment.right_path,
        Some(segment.right_bytes),
        segment.right_sha256.as_deref(),
    )?;
    Ok((left, right))
}

pub fn open_relative_regular(
    root_fd: libc::c_int,
    relative_path: &str,
) -> Result<libc::c_int, SessionIoError> {
    if root_fd < 0 {
        return Err(SessionIoError::new(
            "artifact_invalid",
            "invalid root directory descriptor",
        ));
    }
    validate_relative_regular_path(relative_path)?;
    let components = relative_path.split('/').collect::<Vec<_>>();
    // SAFETY: `dup_fd` returns a newly duplicated owned descriptor.
    let mut current = unsafe { OwnedFd::from_raw_fd(dup_fd(root_fd)?) };
    for component in &components[..components.len() - 1] {
        current = OwnedFd::from(openat_component(current.as_raw_fd(), component, true)?);
    }
    let file = openat_component(current.as_raw_fd(), components[components.len() - 1], false)?;
    let raw = fstat_raw(file.as_raw_fd())?;
    let identity = identity_from_stat(&raw)?;
    identity.require_regular(raw.st_mode)?;
    if identity.nlink != 1 {
        return Err(SessionIoError::new(
            "artifact_invalid",
            "artifact must not have hardlinks",
        ));
    }
    Ok(file.into_raw_fd())
}

pub fn verify_device_session_artifacts(
    session_root: &Path,
    manifest_payload: &[u8],
    expected_session_id: &str,
    expected_identities: &[ExpectedArtifactIdentity],
) -> Result<u64, SessionIoError> {
    let artifacts = device_session_path_artifacts(manifest_payload, expected_session_id)?;
    let expected = expected_identity_map(expected_identities)?;
    if expected.len() != artifacts.len() {
        return Err(SessionIoError::new(
            "artifact_invalid",
            "sealed artifact identities do not match manifest count",
        ));
    }
    let root = open_directory(
        session_root,
        "artifact_invalid",
        "open session root directory failed",
    )?;
    let omit_volatile_inode = is_exfat_or_msdos(root.as_raw_fd()).map_err(|error| {
        SessionIoError::os("artifact_invalid", "statfs session directory failed", error)
    })?;
    for artifact in &artifacts {
        let expected_identity = expected
            .get(artifact.path.as_str())
            .ok_or_else(|| SessionIoError::new("artifact_invalid", "missing sealed identity"))?;
        if expected_identity.size != artifact.bytes {
            return Err(SessionIoError::new(
                "artifact_invalid",
                "artifact byte size differs from manifest",
            ));
        }
        let descriptor = open_relative_regular(root.as_raw_fd(), &artifact.path)?;
        let actual = FileIdentity::from_fd(descriptor);
        close_fd(descriptor);
        let actual = actual?;
        if !identities_match(&actual, expected_identity, omit_volatile_inode) {
            return Err(SessionIoError::new(
                "digest_mismatch",
                "artifact changed before publication",
            ));
        }
    }
    u64::try_from(artifacts.len())
        .map_err(|_| SessionIoError::new("artifact_invalid", "artifact count overflow"))
}

pub fn identities_match(
    actual: &FileIdentity,
    expected: &FileIdentity,
    omit_volatile_inode: bool,
) -> bool {
    if omit_volatile_inode {
        actual.device == expected.device
            && actual.size == expected.size
            && actual.modified_ns == expected.modified_ns
            && actual.nlink == expected.nlink
    } else {
        actual == expected
    }
}

fn device_session_path_artifacts(
    payload: &[u8],
    expected_session_id: &str,
) -> Result<Vec<ArtifactDescriptor>, SessionIoError> {
    let root = parse_device_session_manifest(payload, expected_session_id)?;
    let mut collector = ArtifactCollector::default();
    collect_device_session_artifacts(manifest_object(&root)?, &mut collector)?;
    collector.finish()
}

fn parse_device_session_manifest(
    payload: &[u8],
    expected_session_id: &str,
) -> Result<Value, SessionIoError> {
    let root: Value = serde_json::from_slice(payload).map_err(|error| {
        SessionIoError::new(
            "manifest_invalid",
            format!("invalid manifest JSON: {error}"),
        )
    })?;
    let object = manifest_object(&root)?;
    let schema = string_field(object, "schema")?;
    if schema != "miro-ego.device-session.v1" {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "unsupported device-session manifest schema",
        ));
    }
    if bool_field(object, "sealed")? != Some(true) {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "manifest is not sealed",
        ));
    }
    if string_field(object, "session_id")? != expected_session_id {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "manifest session_id mismatch",
        ));
    }
    Ok(root)
}

fn manifest_object(root: &Value) -> Result<&Map<String, Value>, SessionIoError> {
    root.as_object()
        .ok_or_else(|| SessionIoError::new("manifest_invalid", "manifest must be a JSON object"))
}

#[derive(Default)]
struct ArtifactCollector {
    descriptors: Vec<ArtifactDescriptor>,
    paths: HashSet<String>,
}

impl ArtifactCollector {
    fn finish(self) -> Result<Vec<ArtifactDescriptor>, SessionIoError> {
        if self.descriptors.is_empty() {
            return Err(SessionIoError::new(
                "manifest_invalid",
                "manifest declares no artifacts",
            ));
        }
        Ok(self.descriptors)
    }

    fn collect(&mut self, raw: &Value) -> Result<(), SessionIoError> {
        let descriptor = parse_artifact_descriptor(raw)?;
        if !self.paths.insert(descriptor.path.clone()) {
            return Err(SessionIoError::new(
                "manifest_invalid",
                "duplicate manifest artifact path",
            ));
        }
        self.descriptors.push(descriptor);
        Ok(())
    }
}

fn collect_device_session_artifacts(
    object: &Map<String, Value>,
    collector: &mut ArtifactCollector,
) -> Result<(), SessionIoError> {
    let video = object_field(object, "video", "invalid manifest video section")?;
    match string_field(video, "layout")? {
        "raw-side-by-side" => {
            collector.collect(value_field(video, "artifact")?)?;
        }
        "split-eyes" => {
            let segments = array_field(video, "segments", "invalid manifest video segments")?;
            for segment in segments {
                let segment = segment.as_object().ok_or_else(|| {
                    SessionIoError::new("manifest_invalid", "invalid video segment entry")
                })?;
                let artifacts =
                    object_field(segment, "artifacts", "invalid video segment artifacts")?;
                collector.collect(value_field(artifacts, "left")?)?;
                collector.collect(value_field(artifacts, "right")?)?;
            }
        }
        _ => {
            return Err(SessionIoError::new(
                "manifest_invalid",
                "invalid manifest video layout",
            ));
        }
    }

    for section_name in ["frames", "imu"] {
        let section = object_field(object, section_name, "invalid manifest stream section")?;
        collector.collect(value_field(section, "artifact")?)?;
    }

    if let Some(audio) = object.get("audio") {
        let audio = audio
            .as_object()
            .ok_or_else(|| SessionIoError::new("manifest_invalid", "invalid audio section"))?;
        if !matches!(
            audio.get("state").and_then(Value::as_str),
            Some("not_recorded")
        ) {
            let segments = array_field(audio, "segments", "invalid audio segments")?;
            for segment in segments {
                let segment = segment.as_object().ok_or_else(|| {
                    SessionIoError::new("manifest_invalid", "invalid audio segment")
                })?;
                collector.collect(value_field(segment, "artifact")?)?;
            }
        }
    }

    if object.contains_key("logs") {
        for artifact in array_field(object, "logs", "invalid manifest logs array")? {
            collector.collect(artifact)?;
        }
    }

    Ok(())
}

fn parse_artifact_descriptor(raw: &Value) -> Result<ArtifactDescriptor, SessionIoError> {
    let object = raw
        .as_object()
        .ok_or_else(|| SessionIoError::new("manifest_invalid", "invalid artifact descriptor"))?;
    for key in [
        "artifact_id",
        "role",
        "path",
        "media_type",
        "bytes",
        "sha256",
    ] {
        if !object.contains_key(key) {
            return Err(SessionIoError::new(
                "manifest_invalid",
                "missing artifact descriptor field",
            ));
        }
    }
    if object.len() != 6 && !(object.len() == 7 && object.contains_key("storage_encoding")) {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "unexpected artifact descriptor fields",
        ));
    }
    let artifact_id = string_field(object, "artifact_id")?;
    let role = string_field(object, "role")?;
    let path = string_field(object, "path")?;
    let media_type = string_field(object, "media_type")?;
    let sha256 = string_field(object, "sha256")?;
    let bytes = object
        .get("bytes")
        .and_then(Value::as_u64)
        .ok_or_else(|| SessionIoError::new("manifest_invalid", "invalid artifact bytes"))?;
    if !is_valid_sha256(artifact_id) || !is_valid_sha256(sha256) || artifact_id != sha256 {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "invalid artifact SHA-256",
        ));
    }
    if !is_media_type(media_type) {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "invalid artifact media_type",
        ));
    }
    validate_relative_artifact_path(path)?;
    Ok(ArtifactDescriptor {
        artifact_id: artifact_id.to_owned(),
        role: role.to_owned(),
        path: path.to_owned(),
        media_type: media_type.to_owned(),
        bytes,
        sha256: sha256.to_owned(),
    })
}

fn value_field<'a>(
    object: &'a Map<String, Value>,
    name: &str,
) -> Result<&'a Value, SessionIoError> {
    object
        .get(name)
        .ok_or_else(|| SessionIoError::new("manifest_invalid", "missing manifest field"))
}

fn object_field<'a>(
    object: &'a Map<String, Value>,
    name: &str,
    message: &'static str,
) -> Result<&'a Map<String, Value>, SessionIoError> {
    object
        .get(name)
        .and_then(Value::as_object)
        .ok_or_else(|| SessionIoError::new("manifest_invalid", message))
}

fn array_field<'a>(
    object: &'a Map<String, Value>,
    name: &str,
    message: &'static str,
) -> Result<&'a Vec<Value>, SessionIoError> {
    object
        .get(name)
        .and_then(Value::as_array)
        .ok_or_else(|| SessionIoError::new("manifest_invalid", message))
}

fn string_field<'a>(object: &'a Map<String, Value>, name: &str) -> Result<&'a str, SessionIoError> {
    object
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| SessionIoError::new("manifest_invalid", "invalid string field"))
}

fn bool_field(object: &Map<String, Value>, name: &str) -> Result<Option<bool>, SessionIoError> {
    object
        .get(name)
        .map(|value| {
            value
                .as_bool()
                .ok_or_else(|| SessionIoError::new("manifest_invalid", "invalid bool field"))
        })
        .transpose()
}

fn is_media_type(value: &str) -> bool {
    let Some((left, right)) = value.split_once('/') else {
        return false;
    };
    !left.is_empty()
        && !right.is_empty()
        && left.bytes().all(is_media_token_byte)
        && right.bytes().all(is_media_token_byte)
}

fn is_media_token_byte(byte: u8) -> bool {
    byte.is_ascii_lowercase()
        || byte.is_ascii_digit()
        || matches!(
            byte,
            b'!' | b'#' | b'$' | b'&' | b'^' | b'_' | b'.' | b'+' | b'-'
        )
}

pub fn validate_relative_artifact_path(path: &str) -> Result<(), SessionIoError> {
    validate_relative_regular_path(path)?;
    let first = path
        .split('/')
        .next()
        .ok_or_else(|| SessionIoError::new("manifest_invalid", "invalid artifact path"))?;
    if matches!(
        first,
        "manifest.json" | "recording.json" | "capture.json" | "segments.json" | "checkpoint.json"
    ) {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "control file cannot be declared as an artifact",
        ));
    }
    Ok(())
}

pub fn validate_relative_regular_path(path: &str) -> Result<(), SessionIoError> {
    if path.is_empty() || path.len() > 1024 || path.starts_with('/') || path.contains('\\') {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "invalid relative artifact path",
        ));
    }
    if path
        .bytes()
        .any(|byte| byte < 32 || (127..=159).contains(&byte))
    {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "control characters in artifact path",
        ));
    }
    for component in path.split('/') {
        if invalid_path_component(component) {
            return Err(SessionIoError::new(
                "manifest_invalid",
                "invalid path component",
            ));
        }
    }
    Ok(())
}

fn dup_fd(fd: libc::c_int) -> Result<libc::c_int, SessionIoError> {
    dup_cloexec(fd)
        .map_err(|error| SessionIoError::os("artifact_invalid", "dup root fd failed", error))
}

fn openat_component(
    parent_fd: libc::c_int,
    component: &str,
    directory: bool,
) -> Result<File, SessionIoError> {
    let name = CString::new(component)
        .map_err(|_| SessionIoError::new("artifact_invalid", "NUL byte in path component"))?;
    let mut flags = libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW;
    if directory {
        flags |= libc::O_DIRECTORY;
    } else {
        flags |= libc::O_NONBLOCK;
    }
    openat_retry(parent_fd, &name, flags)
        .map_err(|error| SessionIoError::os("artifact_invalid", "openat artifact failed", error))
}

fn close_fd(fd: libc::c_int) {
    if fd >= 0 {
        // SAFETY: `fd` is owned and valid.
        let _ = unsafe { libc::close(fd) };
    }
}

fn invalid_path_component(component: &str) -> bool {
    component.is_empty() || component == "." || component == ".." || is_temp_component(component)
}

fn is_temp_component(component: &str) -> bool {
    let Some(index) = component.find(".tmp") else {
        return false;
    };
    let suffix = &component[index + 4..];
    suffix.is_empty()
        || (suffix.len() >= 2
            && matches!(suffix.as_bytes()[0], b'.' | b'_' | b'-')
            && !suffix[1..].is_empty())
}

fn fstat_raw(fd: libc::c_int) -> Result<libc::stat, SessionIoError> {
    fstat(fd).map_err(|error| SessionIoError::os("artifact_invalid", "fstat failed", error))
}

fn identity_from_stat(raw: &libc::stat) -> Result<FileIdentity, SessionIoError> {
    let size = u64::try_from(raw.st_size)
        .map_err(|_| SessionIoError::new("artifact_invalid", "negative file size"))?;
    let modified_ns = raw
        .st_mtime
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(raw.st_mtime_nsec))
        .ok_or_else(|| SessionIoError::new("artifact_invalid", "mtime overflow"))?;
    #[allow(clippy::useless_conversion)]
    let nlink = u64::from(raw.st_nlink);
    Ok(FileIdentity {
        device: raw.st_dev,
        inode: raw.st_ino,
        size,
        modified_ns,
        nlink,
    })
}

fn expected_identity_map(
    identities: &[ExpectedArtifactIdentity],
) -> Result<HashMap<&str, &FileIdentity>, SessionIoError> {
    let mut by_path = HashMap::with_capacity(identities.len());
    for item in identities {
        validate_relative_artifact_path(&item.path)?;
        if by_path.insert(item.path.as_str(), &item.identity).is_some() {
            return Err(SessionIoError::new(
                "artifact_invalid",
                "duplicate expected artifact identity path",
            ));
        }
    }
    Ok(by_path)
}

fn open_directory(path: &Path, code: &'static str, context: &str) -> Result<File, SessionIoError> {
    let target = if path.as_os_str().is_empty() {
        Path::new(".")
    } else {
        path
    };
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(target)
        .map_err(|error| SessionIoError::os(code, context, error))?;
    let metadata = file
        .metadata()
        .map_err(|error| SessionIoError::os(code, "stat directory failed", error))?;
    if !metadata.file_type().is_dir() {
        return Err(SessionIoError::new(code, "path is not a directory"));
    }
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "miro-ego-artifact-verify-{tag}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn hashes_regular_file_and_rejects_hardlinks() {
        let root = temp_dir("hash");
        let path = root.join("artifact.bin");
        fs::write(&path, b"abc").unwrap();
        let digest = hash_file(&path).unwrap();
        assert_eq!(
            digest.sha256,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(digest.identity.size, 3);

        let alias = root.join("alias.bin");
        fs::hard_link(&path, &alias).unwrap();
        assert_eq!(hash_file(&path).unwrap_err().code, "artifact_invalid");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn exfat_mode_omits_volatile_inode_while_posix_requires_exact_match() {
        let expected = FileIdentity {
            device: 8,
            inode: 1001,
            size: 4096,
            modified_ns: 1_700_000_000_000_000_000,
            nlink: 1,
        };
        let remounted_same_file = FileIdentity {
            inode: 2048,
            ..expected
        };
        assert!(!identities_match(&remounted_same_file, &expected, false));
        assert!(identities_match(&remounted_same_file, &expected, true));
    }
}
