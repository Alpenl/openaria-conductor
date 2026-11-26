use crate::session::stereo_encoder::SegmentEvent;
use miro_ego_core::sys::fs::{
    dup_cloexec, fadvise_dontneed, fadvise_sequential, fstat, is_exfat_or_msdos, openat_retry,
    pread_retry as sys_pread_retry, read_retry, renameat2_noreplace,
};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const READ_CHUNK: usize = 1024 * 1024;
const SENDFILE_CHUNK: u64 = 8 * 1024 * 1024;
const WRITEV_MAX: usize = 1024;
const ENCODER_FRAME_MAGIC: &[u8; 4] = b"MGOF";

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
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    fn os(code: &'static str, context: &str, error: std::io::Error) -> Self {
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
    fn from_fd(fd: libc::c_int) -> Result<Self, SessionIoError> {
        let raw = fstat_raw(fd)?;
        identity_from_stat(&raw)
    }

    fn require_regular(&self, mode: libc::mode_t) -> Result<(), SessionIoError> {
        if (mode & libc::S_IFMT) != libc::S_IFREG {
            return Err(SessionIoError::new(
                "artifact_invalid",
                "artifact 不是普通文件",
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenedVerifiedArtifact {
    pub descriptor: libc::c_int,
    pub identity: FileIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedArtifactIdentity {
    pub path: String,
    pub identity: FileIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceSessionSealResult {
    pub manifest_sha256: String,
    pub artifact_count: u64,
    pub manifest_bytes: u64,
}

pub fn hash_file(path: &Path) -> Result<FileDigest, SessionIoError> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|error| SessionIoError::os("artifact_invalid", "无法打开 artifact", error))?;
    let raw = fstat_raw(file.as_raw_fd())?;
    let before = identity_from_stat(&raw)?;
    before.require_regular(raw.st_mode)?;
    if before.nlink != 1 {
        return Err(SessionIoError::new(
            "artifact_invalid",
            "artifact 不是独占普通文件",
        ));
    }
    let _ = fadvise_sequential(file.as_raw_fd(), 0, 0);
    let mut hasher = Sha256::new();
    let chunk_len = usize::try_from(before.size.clamp(1, READ_CHUNK as u64)).unwrap_or(READ_CHUNK);
    let mut buffer = vec![0_u8; chunk_len];
    loop {
        let read = read_retry(&mut file, &mut buffer)
            .map_err(|error| SessionIoError::os("artifact_invalid", "读取 artifact 失败", error))?;
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
            "artifact 在计算摘要期间发生变化",
        ));
    }
    Ok(FileDigest {
        identity: before,
        sha256: hex_digest(hasher.finalize().as_slice()),
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
/// When `precomputed_sha256` is supplied (e.g. from streaming `Sha256Writer`, streaming
/// WAV writer, or `miro-ego-encoder` `segment` event), this verifies file identity and
/// byte size with **zero disk reread**. When `None`, it falls back to hashing the file.
pub fn finalize_artifact_with_digest(
    path: &Path,
    expected_bytes: Option<u64>,
    precomputed_sha256: Option<&str>,
) -> Result<FileDigest, SessionIoError> {
    if let Some(precomputed) = precomputed_sha256 {
        if !is_sha256(precomputed) {
            return Err(SessionIoError::new(
                "artifact_invalid",
                "precomputed SHA-256 format is invalid",
            ));
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
            .map_err(|error| SessionIoError::os("artifact_invalid", "无法打开 artifact", error))?;
        let raw = fstat_raw(file.as_raw_fd())?;
        let identity = identity_from_stat(&raw)?;
        identity.require_regular(raw.st_mode)?;
        if identity.nlink != 1 {
            return Err(SessionIoError::new(
                "artifact_invalid",
                "artifact 不是独占普通文件",
            ));
        }
        if let Some(expected) = expected_bytes {
            if identity.size != expected {
                return Err(SessionIoError::new(
                    "artifact_invalid",
                    "artifact 大小与声明不一致",
                ));
            }
        }
        return Ok(FileDigest {
            identity,
            sha256: precomputed.to_owned(),
        });
    }

    let digest = hash_file(path)?;
    if let Some(expected) = expected_bytes {
        if digest.identity.size != expected {
            return Err(SessionIoError::new(
                "artifact_invalid",
                "artifact 大小与声明不一致",
            ));
        }
    }
    Ok(digest)
}

/// Finalize both left and right MP4 artifacts of an encoder [`SegmentEvent`], using
/// pre-supplied streaming digests with zero reread when available and falling back to
/// hashing only if a digest was omitted.
pub fn finalize_segment_artifacts(
    session_root: &Path,
    segment: &SegmentEvent,
) -> Result<(FileDigest, FileDigest), SessionIoError> {
    validate_relative_artifact_path(&segment.left_path)?;
    validate_relative_artifact_path(&segment.right_path)?;
    let left = finalize_artifact_with_digest(
        &session_root.join(&segment.left_path),
        Some(segment.left_bytes),
        segment.left_sha256.as_deref(),
    )?;
    let right = finalize_artifact_with_digest(
        &session_root.join(&segment.right_path),
        Some(segment.right_bytes),
        segment.right_sha256.as_deref(),
    )?;
    Ok((left, right))
}

pub fn verify_fd(
    fd: libc::c_int,
    expected_bytes: u64,
    expected_sha256: &str,
) -> Result<FileIdentity, SessionIoError> {
    verify_fd_interruptible(fd, expected_bytes, expected_sha256, || Ok(()))
}

pub fn verify_fd_interruptible(
    fd: libc::c_int,
    expected_bytes: u64,
    expected_sha256: &str,
    mut interrupt_check: impl FnMut() -> Result<(), SessionIoError>,
) -> Result<FileIdentity, SessionIoError> {
    if !is_sha256(expected_sha256) {
        return Err(SessionIoError::new("artifact_invalid", "SHA-256 格式无效"));
    }
    let before_raw = fstat_raw(fd)?;
    let before = identity_from_stat(&before_raw)?;
    before.require_regular(before_raw.st_mode)?;
    if before.size != expected_bytes {
        return Err(SessionIoError::new(
            "artifact_invalid",
            "artifact 大小不匹配",
        ));
    }
    let mut hasher = Sha256::new();
    let mut remaining = expected_bytes;
    let mut offset: u64 = 0;
    let chunk_len =
        usize::try_from(expected_bytes.clamp(1, READ_CHUNK as u64)).unwrap_or(READ_CHUNK);
    let mut buffer = vec![0_u8; chunk_len];
    if remaining == 0 {
        interrupt_check()?;
    }
    while remaining > 0 {
        interrupt_check()?;
        let selected = usize::try_from(remaining.min(READ_CHUNK as u64))
            .map_err(|_| SessionIoError::new("artifact_invalid", "artifact 大小超出平台限制"))?;
        let read = pread_retry(fd, &mut buffer[..selected], offset)?;
        if read == 0 {
            return Err(SessionIoError::new("artifact_invalid", "artifact 发生短读"));
        }
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
        offset += read as u64;
    }
    let mut trailing = [0_u8; 1];
    if pread_retry(fd, &mut trailing, expected_bytes)? != 0 {
        return Err(SessionIoError::new(
            "artifact_invalid",
            "artifact 大小在校验期间变化",
        ));
    }
    let after_raw = fstat_raw(fd)?;
    let after = identity_from_stat(&after_raw)?;
    if after != before {
        return Err(SessionIoError::new(
            "artifact_invalid",
            "artifact 在校验期间变化",
        ));
    }
    let actual = hex_digest(hasher.finalize().as_slice());
    if actual != expected_sha256 {
        return Err(SessionIoError::new(
            "digest_mismatch",
            "artifact 摘要不匹配",
        ));
    }
    Ok(before)
}

pub fn sendfile_all(
    output_fd: libc::c_int,
    input_fd: libc::c_int,
    offset: u64,
    length: u64,
) -> Result<u64, SessionIoError> {
    if output_fd < 0 || input_fd < 0 {
        return Err(SessionIoError::new("send_failed", "文件描述符无效"));
    }
    let mut current = libc::off_t::try_from(offset)
        .map_err(|_| SessionIoError::new("send_failed", "发送偏移超出平台限制"))?;
    let mut remaining = length;
    while remaining > 0 {
        let selected = usize::try_from(remaining.min(SENDFILE_CHUNK))
            .map_err(|_| SessionIoError::new("send_failed", "发送长度超出平台限制"))?;
        let sent = unsafe { libc::sendfile(output_fd, input_fd, &mut current, selected) };
        if sent > 0 {
            remaining -= sent as u64;
            continue;
        }
        if sent == 0 {
            return Err(SessionIoError::new("send_failed", "sendfile 写入零字节"));
        }
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::Interrupted {
            continue;
        }
        return Err(sendfile_error(error));
    }
    Ok(length)
}

fn sendfile_error(error: std::io::Error) -> SessionIoError {
    // Preserve the OS category before formatting its platform-dependent text.
    // Only peer disconnects are expected connection errors at the Python boundary.
    let code = match error.raw_os_error() {
        Some(libc::EPIPE) => "send_broken_pipe",
        Some(libc::ECONNRESET) => "send_connection_reset",
        _ => "send_failed",
    };
    SessionIoError::os(code, "sendfile 失败", error)
}

pub fn write_encoder_frame(fd: libc::c_int, jpeg: &[u8]) -> Result<u64, SessionIoError> {
    let length = u32::try_from(jpeg.len())
        .map_err(|_| SessionIoError::new("write_failed", "encoder frame 超出长度限制"))?;
    let mut header = [0_u8; 8];
    header[..4].copy_from_slice(ENCODER_FRAME_MAGIC);
    header[4..].copy_from_slice(&length.to_le_bytes());
    writev_all(fd, &[&header, jpeg])
}

pub fn open_relative_regular(
    root_fd: libc::c_int,
    relative_path: &str,
) -> Result<libc::c_int, SessionIoError> {
    if root_fd < 0 {
        return Err(SessionIoError::new(
            "artifact_invalid",
            "根目录文件描述符无效",
        ));
    }
    validate_relative_regular_path(relative_path)?;
    let components = relative_path.split('/').collect::<Vec<_>>();
    // SAFETY: dup_fd returns a newly duplicated owned descriptor.
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
            "artifact 不是独占普通文件",
        ));
    }
    Ok(file.into_raw_fd())
}

pub fn open_verified_artifact(
    root_fd: libc::c_int,
    relative_path: &str,
    expected_bytes: u64,
    expected_sha256: &str,
) -> Result<OpenedVerifiedArtifact, SessionIoError> {
    let descriptor = open_relative_regular(root_fd, relative_path)?;
    match verify_fd(descriptor, expected_bytes, expected_sha256) {
        Ok(identity) => Ok(OpenedVerifiedArtifact {
            descriptor,
            identity,
        }),
        Err(error) => {
            close_fd(descriptor);
            Err(error)
        }
    }
}

pub fn read_fd_bounded(fd: libc::c_int, maximum_bytes: usize) -> Result<Vec<u8>, SessionIoError> {
    if fd < 0 {
        return Err(SessionIoError::new("artifact_invalid", "文件描述符无效"));
    }
    let limit = maximum_bytes
        .checked_add(1)
        .ok_or_else(|| SessionIoError::new("artifact_invalid", "文件大小上限超出可表示范围"))?;
    let mut payload = Vec::with_capacity(maximum_bytes.min(READ_CHUNK));
    let mut buffer = vec![0_u8; READ_CHUNK.min(limit.max(1))];
    while payload.len() < limit {
        let selected = READ_CHUNK.min(limit - payload.len());
        let read = pread_retry(fd, &mut buffer[..selected], payload.len() as u64)?;
        if read == 0 {
            break;
        }
        payload.extend_from_slice(&buffer[..read]);
    }
    if payload.len() > maximum_bytes {
        return Err(SessionIoError::new("artifact_invalid", "文件超过允许大小"));
    }
    Ok(payload)
}

#[cfg(test)]
pub fn device_session_v1_artifacts(
    payload: &[u8],
    expected_session_id: &str,
) -> Result<Vec<ArtifactDescriptor>, SessionIoError> {
    let root = parse_device_session_manifest(payload, expected_session_id)?;
    let mut collector = ContentAddressedArtifactCollector::default();
    collect_device_session_artifacts(manifest_object(&root)?, &mut collector)?;
    collector.finish()
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
        SessionIoError::new("manifest_invalid", format!("manifest JSON 无效: {error}"))
    })?;
    let object = manifest_object(&root)?;
    let schema = string_field(object, "schema")?;
    if schema != "miro-ego.device-session.v1" {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "manifest 不是支持的 device-session",
        ));
    }
    if bool_field(object, "sealed")? != Some(true) {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "manifest 尚未 sealed",
        ));
    }
    if string_field(object, "session_id")? != expected_session_id {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "manifest 会话身份不匹配",
        ));
    }
    Ok(root)
}

fn manifest_object(root: &Value) -> Result<&Map<String, Value>, SessionIoError> {
    root.as_object()
        .ok_or_else(|| SessionIoError::new("manifest_invalid", "manifest 必须是对象"))
}

trait ArtifactSink {
    fn collect(&mut self, raw: &Value) -> Result<(), SessionIoError>;
}

fn collect_device_session_artifacts(
    object: &Map<String, Value>,
    collector: &mut impl ArtifactSink,
) -> Result<(), SessionIoError> {
    let video = object_field(object, "video", "manifest video 结构无效")?;
    match string_field(video, "layout")? {
        "raw-side-by-side" => {
            collector.collect(value_field(video, "artifact")?)?;
        }
        "split-eyes" => {
            let segments = array_field(video, "segments", "manifest video segments 无效")?;
            for segment in segments {
                let segment = segment.as_object().ok_or_else(|| {
                    SessionIoError::new("manifest_invalid", "manifest video segment 无效")
                })?;
                let artifacts =
                    object_field(segment, "artifacts", "manifest video segment artifact 无效")?;
                collector.collect(value_field(artifacts, "left")?)?;
                collector.collect(value_field(artifacts, "right")?)?;
            }
        }
        _ => {
            return Err(SessionIoError::new(
                "manifest_invalid",
                "manifest video layout 无效",
            ));
        }
    }

    for section_name in ["frames", "imu"] {
        let section = object_field(object, section_name, "manifest artifact 清单无效")?;
        collector.collect(value_field(section, "artifact")?)?;
    }

    if let Some(audio) = object.get("audio") {
        let audio = audio
            .as_object()
            .ok_or_else(|| SessionIoError::new("manifest_invalid", "manifest audio 结构无效"))?;
        if !matches!(
            audio.get("state").and_then(Value::as_str),
            Some("not_recorded")
        ) {
            let segments = array_field(audio, "segments", "manifest audio segments 无效")?;
            for segment in segments {
                let segment = segment.as_object().ok_or_else(|| {
                    SessionIoError::new("manifest_invalid", "manifest audio segment 无效")
                })?;
                collector.collect(value_field(segment, "artifact")?)?;
            }
        }
    }

    if object.contains_key("logs") {
        for artifact in array_field(object, "logs", "manifest logs 无效")? {
            collector.collect(artifact)?;
        }
    }

    Ok(())
}

fn identities_match(
    actual: &FileIdentity,
    expected: &FileIdentity,
    omit_volatile_inode: bool,
) -> bool {
    if omit_volatile_inode {
        // exFAT / FAT Linux drivers synthesize ephemeral i_ino values when inodes leave
        // the dcache or across remounts; keep device/size/mtime/nlink checks intact.
        actual.device == expected.device
            && actual.size == expected.size
            && actual.modified_ns == expected.modified_ns
            && actual.nlink == expected.nlink
    } else {
        actual == expected
    }
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
            "artifact 封存身份与 manifest 不一致",
        ));
    }
    let root = open_directory(session_root, "artifact_invalid", "无法安全打开会话目录")?;
    let omit_volatile_inode = is_exfat_or_msdos(root.as_raw_fd())
        .map_err(|error| SessionIoError::os("artifact_invalid", "读取会话文件系统失败", error))?;
    for artifact in &artifacts {
        let expected_identity = expected
            .get(artifact.path.as_str())
            .ok_or_else(|| SessionIoError::new("artifact_invalid", "artifact 缺少封存身份"))?;
        if expected_identity.size != artifact.bytes {
            return Err(SessionIoError::new(
                "artifact_invalid",
                "artifact 大小与 manifest 不一致",
            ));
        }
        let descriptor = open_relative_regular(root.as_raw_fd(), &artifact.path)?;
        let actual = FileIdentity::from_fd(descriptor);
        close_fd(descriptor);
        let actual = actual?;
        if !identities_match(&actual, expected_identity, omit_volatile_inode) {
            return Err(SessionIoError::new(
                "digest_mismatch",
                "artifact 在封存前发生变化",
            ));
        }
    }
    u64::try_from(artifacts.len())
        .map_err(|_| SessionIoError::new("artifact_invalid", "artifact 数量超出可表示范围"))
}

pub fn seal_device_session(
    partial_root: &Path,
    final_root: &Path,
    manifest_payload: &[u8],
    expected_session_id: &str,
    expected_identities: &[ExpectedArtifactIdentity],
    control_names: &[String],
) -> Result<DeviceSessionSealResult, SessionIoError> {
    for control_name in control_names {
        validate_control_name(control_name)?;
    }
    if std::fs::symlink_metadata(final_root).is_ok() {
        return Err(SessionIoError::new(
            "session_exists",
            "最终会话目录已经存在",
        ));
    }
    // Verify artifacts before writing manifest or publishing so failed seals leave
    // partial_root intact with all control files untouched.
    let artifact_count = verify_device_session_artifacts(
        partial_root,
        manifest_payload,
        expected_session_id,
        expected_identities,
    )?;
    let manifest_path = partial_root.join("manifest.json");
    write_manifest_atomic(partial_root, &manifest_path, manifest_payload)?;
    if let Err(error) = fsync_directory(partial_root, "write_failed", "同步会话目录失败")
        .and_then(|()| publish_directory_noreplace(partial_root, final_root))
    {
        let _ = std::fs::remove_file(&manifest_path);
        let _ = fsync_directory(partial_root, "write_failed", "同步会话目录失败");
        return Err(error);
    }
    if let Some(parent) = final_root.parent() {
        fsync_directory(parent, "write_failed", "同步 generation 根目录失败")?;
    }
    // Unlink control markers only after final_root is published so a failed rename
    // never leaves partial_root without recording.json / capture.json.
    for control_name in control_names {
        match std::fs::remove_file(final_root.join(control_name)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(SessionIoError::os(
                    "write_failed",
                    "删除录制控制文件失败",
                    error,
                ));
            }
        }
    }
    fsync_directory(final_root, "write_failed", "同步会话目录失败")?;
    verify_device_session_artifacts(
        final_root,
        manifest_payload,
        expected_session_id,
        expected_identities,
    )?;
    Ok(DeviceSessionSealResult {
        manifest_sha256: hex_digest(Sha256::digest(manifest_payload).as_slice()),
        artifact_count,
        manifest_bytes: u64::try_from(manifest_payload.len())
            .map_err(|_| SessionIoError::new("write_failed", "manifest 大小超出可表示范围"))?,
    })
}

fn path_cstring(path: &Path, code: &'static str) -> Result<CString, SessionIoError> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| SessionIoError::new(code, "路径包含 NUL 字节"))
}

fn publish_directory_noreplace(
    partial_root: &Path,
    final_root: &Path,
) -> Result<(), SessionIoError> {
    let old_c = path_cstring(partial_root, "write_failed")?;
    let new_c = path_cstring(final_root, "write_failed")?;
    match renameat2_noreplace(&old_c, &new_c) {
        Ok(()) => Ok(()),
        Err(error) if matches!(error.raw_os_error(), Some(libc::EEXIST | libc::ENOTEMPTY)) => Err(
            SessionIoError::new("session_exists", "最终会话目录已经存在"),
        ),
        Err(error) if matches!(error.raw_os_error(), Some(libc::ENOSYS | libc::EINVAL)) => {
            // Filesystems or kernels without RENAME_NOREPLACE support fall back to an
            // explicit O_DIRECTORY/lstat pre-check plus standard rename.
            if open_directory(final_root, "session_exists", "检查最终会话目录失败").is_ok()
                || std::fs::symlink_metadata(final_root).is_ok()
            {
                return Err(SessionIoError::new(
                    "session_exists",
                    "最终会话目录已经存在",
                ));
            }
            std::fs::rename(partial_root, final_root).map_err(|rename_err| {
                SessionIoError::os("write_failed", "发布会话目录失败", rename_err)
            })
        }
        Err(error) => Err(SessionIoError::os(
            "write_failed",
            "发布会话目录失败",
            error,
        )),
    }
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
                "manifest 未声明 artifact",
            ));
        }
        Ok(self.descriptors)
    }
}

impl ArtifactSink for ArtifactCollector {
    fn collect(&mut self, raw: &Value) -> Result<(), SessionIoError> {
        let descriptor = parse_artifact_descriptor(raw)?;
        if !self.paths.insert(descriptor.path.clone()) {
            return Err(SessionIoError::new(
                "manifest_invalid",
                "manifest artifact 路径重复",
            ));
        }
        self.descriptors.push(descriptor);
        Ok(())
    }
}

#[cfg(test)]
#[derive(Default)]
struct ContentAddressedArtifactCollector {
    descriptors: Vec<ArtifactDescriptor>,
    by_id: HashMap<String, usize>,
    paths: HashSet<String>,
}

#[cfg(test)]
impl ContentAddressedArtifactCollector {
    fn finish(self) -> Result<Vec<ArtifactDescriptor>, SessionIoError> {
        if self.descriptors.is_empty() {
            return Err(SessionIoError::new(
                "manifest_invalid",
                "manifest 未声明 artifact",
            ));
        }
        Ok(self.descriptors)
    }
}

#[cfg(test)]
impl ArtifactSink for ContentAddressedArtifactCollector {
    fn collect(&mut self, raw: &Value) -> Result<(), SessionIoError> {
        let descriptor = parse_artifact_descriptor(raw)?;
        if !self.paths.insert(descriptor.path.clone()) {
            return Err(SessionIoError::new(
                "manifest_invalid",
                "manifest artifact 路径重复",
            ));
        }
        if let Some(index) = self.by_id.get(&descriptor.artifact_id).copied() {
            let existing = &self.descriptors[index];
            if descriptor.bytes != existing.bytes
                || descriptor.media_type != existing.media_type
                || descriptor.sha256 != existing.sha256
            {
                return Err(SessionIoError::new(
                    "manifest_invalid",
                    "同一 artifact 身份的表示元数据不一致",
                ));
            }
            return Ok(());
        }
        self.by_id
            .insert(descriptor.artifact_id.clone(), self.descriptors.len());
        self.descriptors.push(descriptor);
        Ok(())
    }
}

fn parse_artifact_descriptor(raw: &Value) -> Result<ArtifactDescriptor, SessionIoError> {
    let object = raw
        .as_object()
        .ok_or_else(|| SessionIoError::new("manifest_invalid", "manifest artifact 描述符无效"))?;
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
                "manifest artifact 描述符字段无效",
            ));
        }
    }
    if object.len() != 6 && !(object.len() == 7 && object.contains_key("storage_encoding")) {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "manifest artifact 描述符字段无效",
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
        .ok_or_else(|| SessionIoError::new("manifest_invalid", "manifest artifact bytes 无效"))?;
    if !is_sha256(artifact_id) || !is_sha256(sha256) || artifact_id != sha256 {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "manifest artifact SHA-256 无效",
        ));
    }
    if !is_media_type(media_type) {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "manifest artifact media_type 无效",
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
        .ok_or_else(|| SessionIoError::new("manifest_invalid", "manifest 字段缺失"))
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
        .ok_or_else(|| SessionIoError::new("manifest_invalid", "manifest 字符串字段无效"))
}

fn bool_field(object: &Map<String, Value>, name: &str) -> Result<Option<bool>, SessionIoError> {
    object
        .get(name)
        .map(|value| {
            value
                .as_bool()
                .ok_or_else(|| SessionIoError::new("manifest_invalid", "manifest bool 字段无效"))
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

fn validate_relative_artifact_path(path: &str) -> Result<(), SessionIoError> {
    validate_relative_regular_path(path)?;
    let first = path
        .split('/')
        .next()
        .ok_or_else(|| SessionIoError::new("manifest_invalid", "manifest artifact 路径无效"))?;
    if first == "manifest.json" || first == "recording.json" {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "manifest artifact 路径无效",
        ));
    }
    Ok(())
}

fn validate_relative_regular_path(path: &str) -> Result<(), SessionIoError> {
    if path.is_empty() || path.len() > 1024 || path.starts_with('/') || path.contains('\\') {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "manifest artifact 路径无效",
        ));
    }
    if path
        .bytes()
        .any(|byte| byte < 32 || (127..=159).contains(&byte))
    {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "manifest artifact 路径无效",
        ));
    }
    let mut components = path.split('/');
    let Some(first) = components.next() else {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "manifest artifact 路径无效",
        ));
    };
    if invalid_path_component(first) {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "manifest artifact 路径无效",
        ));
    }
    for component in components {
        if invalid_path_component(component) {
            return Err(SessionIoError::new(
                "manifest_invalid",
                "manifest artifact 路径无效",
            ));
        }
    }
    Ok(())
}

fn dup_fd(fd: libc::c_int) -> Result<libc::c_int, SessionIoError> {
    dup_cloexec(fd)
        .map_err(|error| SessionIoError::os("artifact_invalid", "无法复制根目录文件描述符", error))
}

fn openat_component(
    parent_fd: libc::c_int,
    component: &str,
    directory: bool,
) -> Result<File, SessionIoError> {
    let name = CString::new(component)
        .map_err(|_| SessionIoError::new("artifact_invalid", "路径组件包含 NUL"))?;
    let mut flags = libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW;
    if directory {
        flags |= libc::O_DIRECTORY;
    } else {
        flags |= libc::O_NONBLOCK;
    }
    openat_retry(parent_fd, &name, flags).map_err(|error| {
        SessionIoError::os("artifact_invalid", "无法安全打开 artifact 路径", error)
    })
}

fn close_fd(fd: libc::c_int) {
    if fd >= 0 {
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

pub fn writev_all(fd: libc::c_int, chunks: &[&[u8]]) -> Result<u64, SessionIoError> {
    if fd < 0 {
        return Err(SessionIoError::new("write_failed", "文件描述符无效"));
    }
    let mut index = 0;
    let mut offset = 0;
    let mut total = 0_u64;
    while index < chunks.len() {
        while index < chunks.len() && chunks[index].is_empty() {
            index += 1;
            offset = 0;
        }
        if index >= chunks.len() {
            break;
        }

        const STACK_IOVEC: usize = 8;
        let mut stack_vectors = [iovec(&[]); STACK_IOVEC];
        let mut heap_vectors: Vec<libc::iovec> = Vec::new();
        let mut stack_count = 0_usize;
        for (chunk_idx, chunk) in chunks[index..].iter().enumerate() {
            let slice = if chunk_idx == 0 {
                &chunk[offset..]
            } else {
                *chunk
            };
            if slice.is_empty() {
                continue;
            }
            let vec = iovec(slice);
            if heap_vectors.is_empty() && stack_count < STACK_IOVEC {
                stack_vectors[stack_count] = vec;
                stack_count += 1;
            } else {
                if heap_vectors.is_empty() {
                    heap_vectors.reserve((chunks.len() - index).min(WRITEV_MAX));
                    heap_vectors.extend_from_slice(&stack_vectors[..stack_count]);
                }
                if heap_vectors.len() == WRITEV_MAX {
                    break;
                }
                heap_vectors.push(vec);
            }
        }
        let vectors = if heap_vectors.is_empty() {
            &stack_vectors[..stack_count]
        } else {
            &heap_vectors[..]
        };
        let written = unsafe {
            libc::writev(
                fd,
                vectors.as_ptr(),
                libc::c_int::try_from(vectors.len())
                    .map_err(|_| SessionIoError::new("write_failed", "writev iovec 数量无效"))?,
            )
        };
        if written > 0 {
            let mut remaining = written as usize;
            total += written as u64;
            while index < chunks.len() {
                if chunks[index].is_empty() {
                    index += 1;
                    offset = 0;
                    continue;
                }
                let available = chunks[index].len() - offset;
                if remaining < available {
                    offset += remaining;
                    break;
                }
                remaining -= available;
                index += 1;
                offset = 0;
                if remaining == 0 {
                    break;
                }
            }
            continue;
        }
        if written == 0 {
            return Err(SessionIoError::new("write_failed", "writev 写入零字节"));
        }
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::Interrupted {
            continue;
        }
        return Err(SessionIoError::os("write_failed", "writev 失败", error));
    }
    Ok(total)
}

fn iovec(payload: &[u8]) -> libc::iovec {
    libc::iovec {
        iov_base: payload.as_ptr().cast::<libc::c_void>().cast_mut(),
        iov_len: payload.len(),
    }
}

fn fstat_raw(fd: libc::c_int) -> Result<libc::stat, SessionIoError> {
    fstat(fd).map_err(|error| SessionIoError::os("artifact_invalid", "无法读取文件身份", error))
}

fn identity_from_stat(raw: &libc::stat) -> Result<FileIdentity, SessionIoError> {
    let size = u64::try_from(raw.st_size)
        .map_err(|_| SessionIoError::new("artifact_invalid", "文件大小为负"))?;
    let modified_ns = raw
        .st_mtime
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(raw.st_mtime_nsec))
        .ok_or_else(|| SessionIoError::new("artifact_invalid", "文件修改时间超出可表示范围"))?;
    Ok(FileIdentity {
        device: raw.st_dev,
        inode: raw.st_ino,
        size,
        modified_ns,
        nlink: nlink_to_u64(raw.st_nlink),
    })
}

#[allow(clippy::useless_conversion)]
fn nlink_to_u64(nlink: libc::nlink_t) -> u64 {
    // libc::nlink_t is u64 on x86_64 Linux and u32 on aarch64 Linux.
    // Keep one normalized Python-facing type while preserving cross-builds.
    u64::from(nlink)
}

fn pread_retry(fd: libc::c_int, buffer: &mut [u8], offset: u64) -> Result<usize, SessionIoError> {
    let offset = libc::off_t::try_from(offset)
        .map_err(|_| SessionIoError::new("artifact_invalid", "artifact 偏移超出平台限制"))?;
    sys_pread_retry(fd, buffer, offset)
        .map_err(|error| SessionIoError::os("artifact_invalid", "读取 artifact 失败", error))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn hex_digest(payload: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(payload.len() * 2);
    for byte in payload {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
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
                "artifact 封存身份路径重复",
            ));
        }
    }
    Ok(by_path)
}

fn open_directory(path: &Path, code: &'static str, context: &str) -> Result<File, SessionIoError> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| SessionIoError::os(code, context, error))?;
    let metadata = file
        .metadata()
        .map_err(|error| SessionIoError::os(code, "读取目录元数据失败", error))?;
    if !metadata.file_type().is_dir() {
        return Err(SessionIoError::new(code, "路径不是目录"));
    }
    Ok(file)
}

static TEMP_MANIFEST_COUNTER: AtomicU64 = AtomicU64::new(0);

fn write_manifest_atomic(
    partial_root: &Path,
    manifest_path: &Path,
    payload: &[u8],
) -> Result<(), SessionIoError> {
    if std::fs::symlink_metadata(manifest_path).is_ok() {
        return Err(SessionIoError::new("write_failed", "manifest 已经存在"));
    }
    let seq = TEMP_MANIFEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let temp_path = partial_root.join(format!("manifest.json.tmp.{}.{}", std::process::id(), seq));
    let write_result = (|| -> Result<(), SessionIoError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o640)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&temp_path)
            .map_err(|error| {
                SessionIoError::os("write_failed", "创建 manifest 临时文件失败", error)
            })?;
        file.write_all(payload)
            .map_err(|error| SessionIoError::os("write_failed", "写入 manifest 失败", error))?;
        file.sync_all()
            .map_err(|error| SessionIoError::os("write_failed", "同步 manifest 失败", error))?;
        drop(file);
        let old_c = path_cstring(&temp_path, "write_failed")?;
        let new_c = path_cstring(manifest_path, "write_failed")?;
        match renameat2_noreplace(&old_c, &new_c) {
            Ok(()) => Ok(()),
            Err(error) if matches!(error.raw_os_error(), Some(libc::EEXIST)) => {
                Err(SessionIoError::new("write_failed", "manifest 已经存在"))
            }
            Err(error) if matches!(error.raw_os_error(), Some(libc::ENOSYS | libc::EINVAL)) => {
                if std::fs::symlink_metadata(manifest_path).is_ok() {
                    return Err(SessionIoError::new("write_failed", "manifest 已经存在"));
                }
                std::fs::rename(&temp_path, manifest_path)
                    .map_err(|err| SessionIoError::os("write_failed", "发布 manifest 失败", err))
            }
            Err(error) => Err(SessionIoError::os(
                "write_failed",
                "发布 manifest 失败",
                error,
            )),
        }
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&temp_path);
    }
    write_result
}

fn fsync_directory(path: &Path, code: &'static str, context: &str) -> Result<(), SessionIoError> {
    let directory = open_directory(path, code, context)?;
    directory
        .sync_all()
        .map_err(|error| SessionIoError::os(code, context, error))
}

fn validate_control_name(name: &str) -> Result<(), SessionIoError> {
    if name.is_empty()
        || name.contains('/')
        || name.contains('\\')
        || invalid_path_component(name)
        || name == "manifest.json"
    {
        return Err(SessionIoError::new("write_failed", "录制控制文件名无效"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ExpectedArtifactIdentity, FileIdentity, device_session_v1_artifacts, finalize_artifact,
        hash_file, identities_match, open_relative_regular, open_verified_artifact,
        read_fd_bounded, seal_device_session, sendfile_all, verify_device_session_artifacts,
        verify_fd, write_encoder_frame,
    };
    use std::fs::{self, File};
    use std::io::{Read, Seek, SeekFrom};
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::fs::symlink;
    use std::os::unix::net::UnixStream;

    #[test]
    fn hashes_regular_file_with_identity() {
        let root = tempfile_dir();
        let path = root.join("artifact.bin");
        fs::write(&path, b"abc").unwrap();
        let digest = hash_file(&path).unwrap();
        assert_eq!(
            digest.sha256,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(digest.identity.size, 3);
        assert!(digest.identity.inode > 0);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn hash_file_rejects_hard_linked_artifacts() {
        let root = tempfile_dir();
        let path = root.join("artifact.bin");
        let alias = root.join("alias.bin");
        fs::write(&path, b"abc").unwrap();
        fs::hard_link(&path, &alias).unwrap();
        let error = hash_file(&path).unwrap_err();
        assert_eq!(error.code, "artifact_invalid");
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

        let modified = FileIdentity {
            size: 4097,
            ..remounted_same_file
        };
        assert!(!identities_match(&modified, &expected, true));
    }

    #[test]
    fn finalizes_artifact_with_optional_size_check() {
        let root = tempfile_dir();
        let path = root.join("artifact.bin");
        fs::write(&path, b"abc").unwrap();
        let digest = finalize_artifact(&path, Some(3)).unwrap();
        assert_eq!(
            digest.sha256,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(digest.identity.size, 3);
        let error = finalize_artifact(&path, Some(4)).unwrap_err();
        assert_eq!(error.code, "artifact_invalid");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn verifies_fd_without_consuming_its_offset() {
        let root = tempfile_dir();
        let path = root.join("artifact.bin");
        fs::write(&path, b"abc").unwrap();
        let file = File::open(&path).unwrap();
        let identity = verify_fd(
            file.as_raw_fd(),
            3,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        )
        .unwrap();
        assert_eq!(identity.size, 3);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_digest_mismatch() {
        let root = tempfile_dir();
        let path = root.join("artifact.bin");
        fs::write(&path, b"abc").unwrap();
        let file = File::open(&path).unwrap();
        let error = verify_fd(
            file.as_raw_fd(),
            3,
            "0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap_err();
        assert_eq!(error.code, "digest_mismatch");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn interrupted_verification_stops_between_chunks_and_can_restart() {
        let root = tempfile_dir();
        let path = root.join("interruptible.bin");
        fs::write(&path, vec![7_u8; super::READ_CHUNK * 3]).unwrap();
        let digest = hash_file(&path).unwrap();
        let mut file = File::open(&path).unwrap();
        file.seek(SeekFrom::Start(17)).unwrap();
        let mut checks = 0;
        let error = super::verify_fd_interruptible(
            file.as_raw_fd(),
            digest.identity.size,
            &digest.sha256,
            || {
                checks += 1;
                if checks == 2 {
                    Err(super::SessionIoError::new(
                        "verification_changed",
                        "capture started",
                    ))
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
        assert_eq!(error.code, "verification_changed");
        assert_eq!(checks, 2);
        assert_eq!(file.stream_position().unwrap(), 17);
        super::verify_fd_interruptible(
            file.as_raw_fd(),
            digest.identity.size,
            &digest.sha256,
            || Ok(()),
        )
        .unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn sendfile_sends_selected_range_without_consuming_input_offset() {
        let root = tempfile_dir();
        let path = root.join("artifact.bin");
        fs::write(&path, b"abcdef").unwrap();
        let mut file = File::open(&path).unwrap();
        file.seek(SeekFrom::Start(5)).unwrap();
        let (left, mut right) = UnixStream::pair().unwrap();
        let sent = sendfile_all(left.as_raw_fd(), file.as_raw_fd(), 1, 3).unwrap();
        assert_eq!(sent, 3);
        drop(left);
        let mut received = Vec::new();
        right.read_to_end(&mut received).unwrap();
        assert_eq!(received, b"bcd");
        assert_eq!(file.stream_position().unwrap(), 5);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn sendfile_closed_peer_keeps_errno_category_and_owned_descriptors() {
        let root = tempfile_dir();
        let path = root.join("artifact.bin");
        fs::write(&path, b"abcdef").unwrap();
        let mut file = File::open(&path).unwrap();
        file.seek(SeekFrom::Start(5)).unwrap();
        let (left, right) = UnixStream::pair().unwrap();
        drop(right);
        let error = sendfile_all(left.as_raw_fd(), file.as_raw_fd(), 1, 3).unwrap_err();
        assert_eq!(error.code, "send_broken_pipe");
        assert_eq!(file.stream_position().unwrap(), 5);
        assert!(file.metadata().is_ok());
        assert!(left.local_addr().is_ok());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn sendfile_classifies_only_broken_pipe_and_connection_reset_as_disconnects() {
        for (number, expected) in [
            (libc::EPIPE, "send_broken_pipe"),
            (libc::ECONNRESET, "send_connection_reset"),
            (libc::EIO, "send_failed"),
            (libc::EBADF, "send_failed"),
            (libc::EINVAL, "send_failed"),
            (libc::ETIMEDOUT, "send_failed"),
        ] {
            let error = super::sendfile_error(std::io::Error::from_raw_os_error(number));
            assert_eq!(error.code, expected);
        }
        let error = super::sendfile_error(std::io::Error::other("unclassified failure"));
        assert_eq!(error.code, "send_failed");
    }

    #[test]
    fn write_encoder_frame_uses_wire_header_and_payload() {
        let (left, mut right) = UnixStream::pair().unwrap();
        let written = write_encoder_frame(left.as_raw_fd(), b"abc").unwrap();
        assert_eq!(written, 11);
        drop(left);
        let mut received = Vec::new();
        right.read_to_end(&mut received).unwrap();
        assert_eq!(received, b"MGOF\x03\0\0\0abc");
    }

    #[test]
    fn opens_relative_regular_artifact_without_following_links() {
        let root = tempfile_dir();
        fs::create_dir(root.join("video")).unwrap();
        fs::write(root.join("video/left.mp4"), b"left").unwrap();
        let root_file = File::open(&root).unwrap();
        let descriptor = open_relative_regular(root_file.as_raw_fd(), "video/left.mp4").unwrap();
        let mut file = unsafe { File::from_raw_fd(descriptor) };
        let mut payload = Vec::new();
        file.read_to_end(&mut payload).unwrap();
        assert_eq!(payload, b"left");

        fs::write(root.join("manifest.json"), b"{}").unwrap();
        let descriptor = open_relative_regular(root_file.as_raw_fd(), "manifest.json").unwrap();
        let mut file = unsafe { File::from_raw_fd(descriptor) };
        let mut payload = Vec::new();
        file.read_to_end(&mut payload).unwrap();
        assert_eq!(payload, b"{}");

        symlink("left.mp4", root.join("video/link.mp4")).unwrap();
        let error = open_relative_regular(root_file.as_raw_fd(), "video/link.mp4").unwrap_err();
        assert_eq!(error.code, "artifact_invalid");
        let error = open_relative_regular(root_file.as_raw_fd(), "../escape.mp4").unwrap_err();
        assert_eq!(error.code, "manifest_invalid");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn opens_and_verifies_artifact_as_one_owned_operation() {
        let root = tempfile_dir();
        fs::create_dir(root.join("video")).unwrap();
        let path = root.join("video/left.mp4");
        fs::write(&path, b"abc").unwrap();
        let root_file = File::open(&root).unwrap();
        let artifact = open_verified_artifact(
            root_file.as_raw_fd(),
            "video/left.mp4",
            3,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        )
        .unwrap();
        assert_eq!(artifact.identity.size, 3);
        let mut file = unsafe { File::from_raw_fd(artifact.descriptor) };
        let mut payload = Vec::new();
        file.read_to_end(&mut payload).unwrap();
        assert_eq!(payload, b"abc");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_open_and_verify_closes_the_artifact_descriptor() {
        let root = tempfile_dir();
        let path = root.join("artifact.bin");
        fs::write(&path, b"abc").unwrap();
        let root_file = File::open(&root).unwrap();
        let error = open_verified_artifact(
            root_file.as_raw_fd(),
            "artifact.bin",
            3,
            "0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap_err();
        assert_eq!(error.code, "digest_mismatch");
        let leaked = fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| fs::read_link(entry.path()).ok())
            .any(|target| target == path);
        assert!(!leaked, "failed verification leaked an artifact descriptor");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn artifact_paths_reject_session_reserved_filenames() {
        assert!(super::validate_relative_regular_path("manifest.json").is_ok());
        let error = super::validate_relative_artifact_path("manifest.json").unwrap_err();
        assert_eq!(error.code, "manifest_invalid");
        let error = super::validate_relative_artifact_path("recording.json").unwrap_err();
        assert_eq!(error.code, "manifest_invalid");
    }

    #[test]
    fn reads_fd_bounded_without_consuming_offset() {
        let root = tempfile_dir();
        let path = root.join("manifest.json");
        fs::write(&path, b"abcdef").unwrap();
        let mut file = File::open(&path).unwrap();
        file.seek(SeekFrom::Start(4)).unwrap();
        assert_eq!(read_fd_bounded(file.as_raw_fd(), 6).unwrap(), b"abcdef");
        assert_eq!(file.stream_position().unwrap(), 4);
        let error = read_fd_bounded(file.as_raw_fd(), 5).unwrap_err();
        assert_eq!(error.code, "artifact_invalid");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn extracts_device_session_v1_artifacts_in_manifest_order() {
        let payload = br#"{
          "schema":"miro-ego.device-session.v1",
          "sealed":true,
          "session_id":"01989f6a-2c00-7a1b-8c2d-3e4f50617283",
          "video":{"layout":"split-eyes","segments":[{"artifacts":{
            "left":{"artifact_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","role":"video.left","path":"video/left.mp4","media_type":"video/mp4","bytes":10,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
            "right":{"artifact_id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","role":"video.right","path":"video/right.mp4","media_type":"video/mp4","bytes":11,"sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
          }}]},
          "frames":{"artifact":{"artifact_id":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","role":"frames.index","path":"frames.ndjson","media_type":"application/x-ndjson","bytes":12,"sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}},
          "imu":{"artifact":{"artifact_id":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","role":"imu.samples","path":"imu.ndjson","media_type":"application/x-ndjson","bytes":13,"sha256":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}},
          "audio":{"segments":[{"artifact":{"artifact_id":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","role":"audio.wav","path":"audio/000000.wav","media_type":"audio/wav","bytes":14,"sha256":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}}]}
        }"#;
        let artifacts =
            device_session_v1_artifacts(payload, "01989f6a-2c00-7a1b-8c2d-3e4f50617283").unwrap();
        let paths: Vec<_> = artifacts.iter().map(|item| item.path.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                "video/left.mp4",
                "video/right.mp4",
                "frames.ndjson",
                "imu.ndjson",
                "audio/000000.wav"
            ]
        );
        assert_eq!(artifacts[0].media_type, "video/mp4");
        assert_eq!(artifacts[4].bytes, 14);
    }

    #[test]
    fn handles_device_session_v2_not_recorded_audio_without_audio_artifacts() {
        let payload = br#"{
          "schema":"miro-ego.device-session.v1",
          "sealed":true,
          "session_id":"01989f6a-2c00-7a1b-8c2d-3e4f50617283",
          "display_name":"test capture",
          "time":{"started_at":"2026-08-08T02:24:00Z","ended_at":"2026-08-08T02:24:01.250Z","duration_seconds":1.25},
          "video":{"layout":"raw-side-by-side","artifact":{"artifact_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","role":"video.raw-side-by-side","path":"video/raw.mjpeg","media_type":"video/x-motion-jpeg","bytes":10,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
          "frames":{"count":7,"artifact":{"artifact_id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","role":"frames.index","path":"frames.ndjson","media_type":"application/x-ndjson","bytes":12,"sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}},
          "imu":{"sample_count":42,"artifact":{"artifact_id":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","role":"imu.samples","path":"imu.ndjson","media_type":"application/x-ndjson","bytes":13,"sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}},
          "audio":{"state":"not_recorded","requested_mode":"disabled","resolved_mode":"disabled","reason":"user_disabled"}
        }"#;

        let artifacts =
            device_session_v1_artifacts(payload, "01989f6a-2c00-7a1b-8c2d-3e4f50617283").unwrap();
        let paths: Vec<_> = artifacts.iter().map(|item| item.path.as_str()).collect();
        assert_eq!(
            paths,
            vec!["video/raw.mjpeg", "frames.ndjson", "imu.ndjson"]
        );
        let mut with_log: serde_json::Value = serde_json::from_slice(payload).unwrap();
        with_log["logs"] = serde_json::json!([{
            "artifact_id": "d".repeat(64),
            "role": "log.recording-recovery",
            "path": "recovery/interruption.json",
            "media_type": "application/json",
            "bytes": 17,
            "sha256": "d".repeat(64)
        }]);
        let artifacts = device_session_v1_artifacts(
            &serde_json::to_vec(&with_log).unwrap(),
            "01989f6a-2c00-7a1b-8c2d-3e4f50617283",
        )
        .unwrap();
        assert_eq!(artifacts.len(), 4);
        assert_eq!(artifacts[3].path, "recovery/interruption.json");
    }

    #[test]
    fn native_finalizer_seals_manifest_and_verifies_artifact_identities() {
        let root = tempfile_dir();
        let session_id = "01989f6a-2c00-7a1b-8c2d-3e4f50617283";
        let partial = root.join(format!("{session_id}.partial"));
        let final_root = root.join(session_id);
        fs::create_dir(&partial).unwrap();
        fs::create_dir(partial.join("video")).unwrap();
        fs::write(partial.join("recording.json"), b"recording").unwrap();
        fs::write(partial.join("capture.json"), b"capture").unwrap();
        fs::write(partial.join("video/raw.mjpeg"), b"frame").unwrap();
        fs::write(partial.join("frames.ndjson"), b"frame-index\n").unwrap();
        fs::write(partial.join("imu.ndjson"), b"imu\n").unwrap();

        let video = finalize_artifact(&partial.join("video/raw.mjpeg"), Some(5)).unwrap();
        let frames = finalize_artifact(&partial.join("frames.ndjson"), Some(12)).unwrap();
        let imu = finalize_artifact(&partial.join("imu.ndjson"), Some(4)).unwrap();
        let manifest = format!(
            r#"{{
              "schema":"miro-ego.device-session.v1",
              "sealed":true,
              "session_id":"{session_id}",
              "video":{{"layout":"raw-side-by-side","artifact":{{"artifact_id":"{video_sha}","role":"video.raw-side-by-side","path":"video/raw.mjpeg","media_type":"video/x-motion-jpeg","bytes":5,"sha256":"{video_sha}"}}}},
              "frames":{{"artifact":{{"artifact_id":"{frames_sha}","role":"frames.index","path":"frames.ndjson","media_type":"application/x-ndjson","bytes":12,"sha256":"{frames_sha}"}}}},
              "imu":{{"artifact":{{"artifact_id":"{imu_sha}","role":"imu.samples","path":"imu.ndjson","media_type":"application/x-ndjson","bytes":4,"sha256":"{imu_sha}"}}}}
            }}"#,
            video_sha = video.sha256,
            frames_sha = frames.sha256,
            imu_sha = imu.sha256,
        );
        let manifest = manifest.into_bytes();
        let expected = vec![
            ExpectedArtifactIdentity {
                path: "video/raw.mjpeg".to_owned(),
                identity: video.identity,
            },
            ExpectedArtifactIdentity {
                path: "frames.ndjson".to_owned(),
                identity: frames.identity,
            },
            ExpectedArtifactIdentity {
                path: "imu.ndjson".to_owned(),
                identity: imu.identity,
            },
        ];

        assert_eq!(
            verify_device_session_artifacts(&partial, &manifest, session_id, &expected).unwrap(),
            3
        );
        let sealed = seal_device_session(
            &partial,
            &final_root,
            &manifest,
            session_id,
            &expected,
            &["recording.json".to_owned(), "capture.json".to_owned()],
        )
        .unwrap();

        assert_eq!(sealed.artifact_count, 3);
        assert_eq!(sealed.manifest_bytes, manifest.len() as u64);
        assert!(!partial.exists());
        assert!(final_root.join("manifest.json").is_file());
        assert!(!final_root.join("recording.json").exists());
        assert!(!final_root.join("capture.json").exists());
        assert_eq!(
            fs::read(final_root.join("manifest.json")).unwrap(),
            manifest
        );
        assert_eq!(
            verify_device_session_artifacts(&final_root, &manifest, session_id, &expected).unwrap(),
            3
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn seal_failure_preserves_control_files_in_partial_root() {
        let root = tempfile_dir();
        let session_id = "01989f6a-2c00-7a1b-8c2d-3e4f50617283";
        let partial = root.join(format!("{session_id}.partial"));
        let final_root = root.join(session_id);
        fs::create_dir(&partial).unwrap();
        fs::create_dir(partial.join("video")).unwrap();
        fs::write(partial.join("recording.json"), b"recording").unwrap();
        fs::write(partial.join("capture.json"), b"capture").unwrap();
        fs::write(partial.join("video/raw.mjpeg"), b"frame").unwrap();
        fs::write(partial.join("frames.ndjson"), b"frame-index\n").unwrap();
        fs::write(partial.join("imu.ndjson"), b"imu\n").unwrap();

        let video = finalize_artifact(&partial.join("video/raw.mjpeg"), Some(5)).unwrap();
        let frames = finalize_artifact(&partial.join("frames.ndjson"), Some(12)).unwrap();
        let imu = finalize_artifact(&partial.join("imu.ndjson"), Some(4)).unwrap();
        let manifest = format!(
            r#"{{
              "schema":"miro-ego.device-session.v1",
              "sealed":true,
              "session_id":"{session_id}",
              "video":{{"layout":"raw-side-by-side","artifact":{{"artifact_id":"{video_sha}","role":"video.raw-side-by-side","path":"video/raw.mjpeg","media_type":"video/x-motion-jpeg","bytes":5,"sha256":"{video_sha}"}}}},
              "frames":{{"artifact":{{"artifact_id":"{frames_sha}","role":"frames.index","path":"frames.ndjson","media_type":"application/x-ndjson","bytes":12,"sha256":"{frames_sha}"}}}},
              "imu":{{"artifact":{{"artifact_id":"{imu_sha}","role":"imu.samples","path":"imu.ndjson","media_type":"application/x-ndjson","bytes":4,"sha256":"{imu_sha}"}}}}
            }}"#,
            video_sha = video.sha256,
            frames_sha = frames.sha256,
            imu_sha = imu.sha256,
        );
        let expected = vec![
            ExpectedArtifactIdentity {
                path: "video/raw.mjpeg".to_owned(),
                identity: video.identity,
            },
            ExpectedArtifactIdentity {
                path: "frames.ndjson".to_owned(),
                identity: frames.identity,
            },
            ExpectedArtifactIdentity {
                path: "imu.ndjson".to_owned(),
                identity: imu.identity,
            },
        ];
        fs::create_dir(&final_root).unwrap();
        let error = seal_device_session(
            &partial,
            &final_root,
            manifest.as_bytes(),
            session_id,
            &expected,
            &["recording.json".to_owned(), "capture.json".to_owned()],
        )
        .unwrap_err();
        assert_eq!(error.code, "session_exists");
        assert!(partial.join("recording.json").exists());
        assert!(partial.join("capture.json").exists());
        assert!(!partial.join("manifest.json").exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn native_finalizer_rejects_artifact_identity_changes_before_publish() {
        let root = tempfile_dir();
        let session_id = "01989f6a-2c00-7a1b-8c2d-3e4f50617283";
        let partial = root.join(format!("{session_id}.partial"));
        let final_root = root.join(session_id);
        fs::create_dir(&partial).unwrap();
        fs::create_dir(partial.join("video")).unwrap();
        fs::write(partial.join("video/raw.mjpeg"), b"frame").unwrap();
        fs::write(partial.join("frames.ndjson"), b"frame-index\n").unwrap();
        fs::write(partial.join("imu.ndjson"), b"imu\n").unwrap();

        let video = finalize_artifact(&partial.join("video/raw.mjpeg"), Some(5)).unwrap();
        let frames = finalize_artifact(&partial.join("frames.ndjson"), Some(12)).unwrap();
        let imu = finalize_artifact(&partial.join("imu.ndjson"), Some(4)).unwrap();
        fs::write(partial.join("video/raw.mjpeg"), b"changed").unwrap();
        let manifest = format!(
            r#"{{
              "schema":"miro-ego.device-session.v1",
              "sealed":true,
              "session_id":"{session_id}",
              "video":{{"layout":"raw-side-by-side","artifact":{{"artifact_id":"{video_sha}","role":"video.raw-side-by-side","path":"video/raw.mjpeg","media_type":"video/x-motion-jpeg","bytes":5,"sha256":"{video_sha}"}}}},
              "frames":{{"artifact":{{"artifact_id":"{frames_sha}","role":"frames.index","path":"frames.ndjson","media_type":"application/x-ndjson","bytes":12,"sha256":"{frames_sha}"}}}},
              "imu":{{"artifact":{{"artifact_id":"{imu_sha}","role":"imu.samples","path":"imu.ndjson","media_type":"application/x-ndjson","bytes":4,"sha256":"{imu_sha}"}}}}
            }}"#,
            video_sha = video.sha256,
            frames_sha = frames.sha256,
            imu_sha = imu.sha256,
        );
        let expected = vec![
            ExpectedArtifactIdentity {
                path: "video/raw.mjpeg".to_owned(),
                identity: video.identity,
            },
            ExpectedArtifactIdentity {
                path: "frames.ndjson".to_owned(),
                identity: frames.identity,
            },
            ExpectedArtifactIdentity {
                path: "imu.ndjson".to_owned(),
                identity: imu.identity,
            },
        ];
        let error = seal_device_session(
            &partial,
            &final_root,
            manifest.as_bytes(),
            session_id,
            &expected,
            &[],
        )
        .unwrap_err();

        assert_eq!(error.code, "digest_mismatch");
        assert!(partial.exists());
        assert!(!partial.join("manifest.json").exists());
        assert!(!final_root.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_unsafe_device_session_artifact_paths() {
        let payload = br#"{
          "schema":"miro-ego.device-session.v1",
          "sealed":true,
          "session_id":"01989f6a-2c00-7a1b-8c2d-3e4f50617283",
          "video":{"layout":"raw-side-by-side","artifact":{"artifact_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","role":"video.raw","path":"../escape.mp4","media_type":"video/mp4","bytes":1,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
          "frames":{"artifact":{"artifact_id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","role":"frames.index","path":"frames.ndjson","media_type":"application/x-ndjson","bytes":1,"sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}},
          "imu":{"artifact":{"artifact_id":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","role":"imu.samples","path":"imu.ndjson","media_type":"application/x-ndjson","bytes":1,"sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}}
        }"#;
        let error = device_session_v1_artifacts(payload, "01989f6a-2c00-7a1b-8c2d-3e4f50617283")
            .unwrap_err();
        assert_eq!(error.code, "manifest_invalid");
    }

    #[test]
    fn seals_duplicate_content_artifacts_with_distinct_paths_and_media_types() {
        let root = tempfile_dir();
        let session_id = "01989f6a-2c00-7a1b-8c2d-3e4f50617283";
        let partial = root.join(format!("{session_id}.partial"));
        let final_root = root.join(session_id);
        fs::create_dir(&partial).unwrap();
        fs::create_dir(partial.join("video")).unwrap();
        fs::create_dir(partial.join("logs")).unwrap();
        fs::write(partial.join("video/raw.mjpeg"), b"same-bytes").unwrap();
        fs::write(partial.join("frames.ndjson"), b"same-bytes").unwrap();
        fs::write(partial.join("imu.ndjson"), b"").unwrap();
        fs::write(partial.join("logs/a.txt"), b"").unwrap();
        fs::write(partial.join("logs/b.json"), b"").unwrap();

        let video = finalize_artifact(&partial.join("video/raw.mjpeg"), Some(10)).unwrap();
        let frames = finalize_artifact(&partial.join("frames.ndjson"), Some(10)).unwrap();
        let imu = finalize_artifact(&partial.join("imu.ndjson"), Some(0)).unwrap();
        let log_a = finalize_artifact(&partial.join("logs/a.txt"), Some(0)).unwrap();
        let log_b = finalize_artifact(&partial.join("logs/b.json"), Some(0)).unwrap();
        assert_eq!(video.sha256, frames.sha256);
        assert_eq!(imu.sha256, log_a.sha256);
        assert_eq!(log_a.sha256, log_b.sha256);

        let manifest = format!(
            r#"{{
              "schema":"miro-ego.device-session.v1",
              "sealed":true,
              "session_id":"{session_id}",
              "video":{{"layout":"raw-side-by-side","artifact":{{"artifact_id":"{same}","role":"video.raw-side-by-side","path":"video/raw.mjpeg","media_type":"video/x-motion-jpeg","bytes":10,"sha256":"{same}"}}}},
              "frames":{{"artifact":{{"artifact_id":"{same}","role":"frames.index","path":"frames.ndjson","media_type":"application/x-ndjson","bytes":10,"sha256":"{same}"}}}},
              "imu":{{"artifact":{{"artifact_id":"{empty}","role":"imu.samples","path":"imu.ndjson","media_type":"application/x-ndjson","bytes":0,"sha256":"{empty}"}}}},
              "logs":[
                {{"artifact_id":"{empty}","role":"log.text","path":"logs/a.txt","media_type":"text/plain","bytes":0,"sha256":"{empty}"}},
                {{"artifact_id":"{empty}","role":"log.json","path":"logs/b.json","media_type":"application/json","bytes":0,"sha256":"{empty}"}}
              ]
            }}"#,
            same = video.sha256,
            empty = imu.sha256,
        );
        let expected = vec![
            ExpectedArtifactIdentity {
                path: "video/raw.mjpeg".to_owned(),
                identity: video.identity,
            },
            ExpectedArtifactIdentity {
                path: "frames.ndjson".to_owned(),
                identity: frames.identity,
            },
            ExpectedArtifactIdentity {
                path: "imu.ndjson".to_owned(),
                identity: imu.identity,
            },
            ExpectedArtifactIdentity {
                path: "logs/a.txt".to_owned(),
                identity: log_a.identity,
            },
            ExpectedArtifactIdentity {
                path: "logs/b.json".to_owned(),
                identity: log_b.identity,
            },
        ];

        let sealed = seal_device_session(
            &partial,
            &final_root,
            manifest.as_bytes(),
            session_id,
            &expected,
            &[],
        )
        .unwrap();
        assert_eq!(sealed.artifact_count, 5);
        assert!(final_root.join("logs/b.json").is_file());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn seal_publish_failure_removes_partial_manifest_and_preserves_control_files() {
        let root = tempfile_dir();
        let session_id = "01989f6a-2c00-7a1b-8c2d-3e4f50617283";
        let partial = root.join(format!("{session_id}.partial"));
        let final_root = root.join("missing-parent").join(session_id);
        fs::create_dir(&partial).unwrap();
        fs::create_dir(partial.join("video")).unwrap();
        fs::write(partial.join("recording.json"), b"recording").unwrap();
        fs::write(partial.join("capture.json"), b"capture").unwrap();
        fs::write(partial.join("video/raw.mjpeg"), b"frame").unwrap();
        fs::write(partial.join("frames.ndjson"), b"frame-index\n").unwrap();
        fs::write(partial.join("imu.ndjson"), b"imu\n").unwrap();

        let video = finalize_artifact(&partial.join("video/raw.mjpeg"), Some(5)).unwrap();
        let frames = finalize_artifact(&partial.join("frames.ndjson"), Some(12)).unwrap();
        let imu = finalize_artifact(&partial.join("imu.ndjson"), Some(4)).unwrap();
        let manifest = format!(
            r#"{{
              "schema":"miro-ego.device-session.v1",
              "sealed":true,
              "session_id":"{session_id}",
              "video":{{"layout":"raw-side-by-side","artifact":{{"artifact_id":"{video_sha}","role":"video.raw-side-by-side","path":"video/raw.mjpeg","media_type":"video/x-motion-jpeg","bytes":5,"sha256":"{video_sha}"}}}},
              "frames":{{"artifact":{{"artifact_id":"{frames_sha}","role":"frames.index","path":"frames.ndjson","media_type":"application/x-ndjson","bytes":12,"sha256":"{frames_sha}"}}}},
              "imu":{{"artifact":{{"artifact_id":"{imu_sha}","role":"imu.samples","path":"imu.ndjson","media_type":"application/x-ndjson","bytes":4,"sha256":"{imu_sha}"}}}}
            }}"#,
            video_sha = video.sha256,
            frames_sha = frames.sha256,
            imu_sha = imu.sha256,
        );
        let expected = vec![
            ExpectedArtifactIdentity {
                path: "video/raw.mjpeg".to_owned(),
                identity: video.identity,
            },
            ExpectedArtifactIdentity {
                path: "frames.ndjson".to_owned(),
                identity: frames.identity,
            },
            ExpectedArtifactIdentity {
                path: "imu.ndjson".to_owned(),
                identity: imu.identity,
            },
        ];

        let error = seal_device_session(
            &partial,
            &final_root,
            manifest.as_bytes(),
            session_id,
            &expected,
            &["recording.json".to_owned(), "capture.json".to_owned()],
        )
        .unwrap_err();
        assert_eq!(error.code, "write_failed");
        assert!(partial.join("recording.json").exists());
        assert!(partial.join("capture.json").exists());
        assert!(!partial.join("manifest.json").exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn verify_fd_interruptible_runs_check_on_empty_file_and_writev_handles_many_chunks() {
        let root = tempfile_dir();
        let empty_path = root.join("empty.bin");
        fs::write(&empty_path, b"").unwrap();
        let empty = File::open(&empty_path).unwrap();
        let mut checks = 0_usize;
        let error = super::verify_fd_interruptible(
            empty.as_raw_fd(),
            0,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            || {
                checks += 1;
                Err(super::SessionIoError::new(
                    "verification_changed",
                    "interrupted on empty file",
                ))
            },
        )
        .unwrap_err();
        assert_eq!(checks, 1);
        assert_eq!(error.code, "verification_changed");

        let (left, mut right) = UnixStream::pair().unwrap();
        let chunks: Vec<&[u8]> = (0..12).map(|_| b"ab".as_slice()).collect();
        assert_eq!(super::writev_all(left.as_raw_fd(), &chunks).unwrap(), 24);
        drop(left);
        let mut received = Vec::new();
        right.read_to_end(&mut received).unwrap();
        assert_eq!(received, b"ab".repeat(12));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn finalize_artifact_with_precomputed_digest_skips_reread_and_falls_back_when_absent() {
        let root = tempfile_dir();
        fs::create_dir(root.join("video")).unwrap();
        let left_path = root.join("video/left_00000.mp4");
        let right_path = root.join("video/right_00000.mp4");
        fs::write(&left_path, b"left-mp4-bytes").unwrap();
        fs::write(&right_path, b"right-mp4-bytes").unwrap();

        let precomputed_left = "1".repeat(64);
        let segment = super::SegmentEvent {
            index: 0,
            start_frame: 0,
            end_frame: 90,
            left_path: "video/left_00000.mp4".to_owned(),
            left_bytes: 14,
            left_sha256: Some(precomputed_left.clone()),
            right_path: "video/right_00000.mp4".to_owned(),
            right_bytes: 15,
            right_sha256: None,
        };
        let (left, right) = super::finalize_segment_artifacts(&root, &segment).unwrap();
        // Left used the precomputed streaming digest without rereading disk
        assert_eq!(left.sha256, precomputed_left);
        assert_eq!(left.identity.size, 14);
        // Right fell back to hashing from disk because right_sha256 was None
        assert_eq!(right.sha256, hash_file(&right_path).unwrap().sha256);
        assert_eq!(right.identity.size, 15);

        // Reject malformed precomputed digest and size mismatch
        assert_eq!(
            super::finalize_artifact_with_digest(&left_path, Some(14), Some("not-hex"))
                .unwrap_err()
                .code,
            "artifact_invalid"
        );
        assert_eq!(
            super::finalize_artifact_with_digest(&left_path, Some(999), Some(&precomputed_left))
                .unwrap_err()
                .code,
            "artifact_invalid"
        );
        fs::remove_dir_all(root).unwrap();
    }

    fn tempfile_dir() -> std::path::PathBuf {
        let mut root = std::env::temp_dir();
        let unique = format!(
            "miro-ego-session-io-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        root.push(unique);
        fs::create_dir(&root).unwrap();
        root
    }
}
