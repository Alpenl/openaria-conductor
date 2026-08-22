use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

const READ_CHUNK: usize = 1024 * 1024;
const SENDFILE_CHUNK: u64 = 8 * 1024 * 1024;
const WRITEV_MAX: usize = 1024;
const ENCODER_FRAME_MAGIC: &[u8; 4] = b"YLXF";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactDescriptor {
    pub artifact_id: String,
    pub role: String,
    pub path: String,
    pub media_type: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeviceSessionV1Summary {
    pub session_id: String,
    pub display_name: String,
    pub started_at: String,
    pub ended_at: String,
    pub duration_seconds: f64,
    pub frames_count: u64,
    pub imu_sample_count: u64,
    pub audio_sample_count: Option<u64>,
    pub total_bytes: u64,
    pub artifacts: Vec<ArtifactDescriptor>,
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
    fn from_metadata(metadata: &std::fs::Metadata) -> Result<Self, SessionIoError> {
        let modified_ns = metadata
            .mtime()
            .checked_mul(1_000_000_000)
            .and_then(|value| value.checked_add(metadata.mtime_nsec()))
            .ok_or_else(|| SessionIoError::new("artifact_invalid", "文件修改时间超出可表示范围"))?;
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            size: metadata.size(),
            modified_ns,
            nlink: metadata.nlink(),
        })
    }

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
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| SessionIoError::os("artifact_invalid", "无法打开 artifact", error))?;
    let metadata = file.metadata().map_err(|error| {
        SessionIoError::os("artifact_invalid", "无法读取 artifact 元数据", error)
    })?;
    if !metadata.file_type().is_file() {
        return Err(SessionIoError::new(
            "artifact_invalid",
            "artifact 不是普通文件",
        ));
    }
    let before = FileIdentity::from_metadata(&metadata)?;
    let mut reader = std::io::BufReader::with_capacity(READ_CHUNK, file);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; READ_CHUNK];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|error| SessionIoError::os("artifact_invalid", "读取 artifact 失败", error))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let after = FileIdentity::from_fd(reader.get_ref().as_raw_fd())?;
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

pub fn verify_fd(
    fd: libc::c_int,
    expected_bytes: u64,
    expected_sha256: &str,
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
    let mut buffer = vec![0_u8; READ_CHUNK];
    while remaining > 0 {
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
        return Err(SessionIoError::os("send_failed", "sendfile 失败", error));
    }
    Ok(length)
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
    let mut current = dup_fd(root_fd)?;
    let mut final_descriptor: Option<libc::c_int> = None;
    let result = (|| {
        for component in &components[..components.len() - 1] {
            let next = openat_component(current, component, true)?;
            close_fd(current);
            current = next;
        }
        let descriptor = openat_component(current, components[components.len() - 1], false)?;
        final_descriptor = Some(descriptor);
        let raw = fstat_raw(descriptor)?;
        let identity = identity_from_stat(&raw)?;
        identity.require_regular(raw.st_mode)?;
        if identity.nlink != 1 {
            return Err(SessionIoError::new(
                "artifact_invalid",
                "artifact 不是独占普通文件",
            ));
        }
        final_descriptor = None;
        Ok(descriptor)
    })();
    close_fd(current);
    if result.is_err() {
        if let Some(descriptor) = final_descriptor.take() {
            close_fd(descriptor);
        }
    }
    result
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
        let offset: libc::off_t = payload
            .len()
            .try_into()
            .map_err(|_| SessionIoError::new("artifact_invalid", "文件偏移超出可表示范围"))?;
        let read = loop {
            let read = unsafe {
                libc::pread(
                    fd,
                    buffer.as_mut_ptr().cast::<libc::c_void>(),
                    selected,
                    offset,
                )
            };
            if read >= 0 {
                break read as usize;
            }
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(SessionIoError::os(
                "artifact_invalid",
                "读取文件失败",
                error,
            ));
        };
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

pub fn device_session_v1_artifacts(
    payload: &[u8],
    expected_session_id: &str,
) -> Result<Vec<ArtifactDescriptor>, SessionIoError> {
    let root = parse_device_session_manifest(payload, expected_session_id)?;
    collect_device_session_v1_artifacts(manifest_object(&root)?)
}

pub fn device_session_v1_summary(
    payload: &[u8],
    expected_session_id: &str,
) -> Result<DeviceSessionV1Summary, SessionIoError> {
    let root = parse_device_session_manifest(payload, expected_session_id)?;
    let object = manifest_object(&root)?;
    let time = object_field(object, "time", "manifest time 结构无效")?;
    let frames = object_field(object, "frames", "manifest frames 结构无效")?;
    let imu = object_field(object, "imu", "manifest imu 结构无效")?;
    let audio_sample_count = object
        .get("audio")
        .map(|audio| {
            let audio = audio.as_object().ok_or_else(|| {
                SessionIoError::new("manifest_invalid", "manifest audio 结构无效")
            })?;
            if matches!(
                audio.get("state").and_then(Value::as_str),
                Some("not_recorded")
            ) {
                return Ok(None);
            }
            u64_field(audio, "sample_count", "manifest audio sample_count 无效").map(Some)
        })
        .transpose()?
        .flatten();

    let artifacts = collect_device_session_v1_artifacts(object)?;
    let total_bytes = total_artifact_bytes(&artifacts)?;
    Ok(DeviceSessionV1Summary {
        session_id: string_field(object, "session_id")?.to_owned(),
        display_name: string_field(object, "display_name")?.to_owned(),
        started_at: string_field(time, "started_at")?.to_owned(),
        ended_at: string_field(time, "ended_at")?.to_owned(),
        duration_seconds: non_negative_number_field(
            time,
            "duration_seconds",
            "manifest duration_seconds 无效",
        )?,
        frames_count: u64_field(frames, "count", "manifest frames count 无效")?,
        imu_sample_count: u64_field(imu, "sample_count", "manifest imu sample_count 无效")?,
        audio_sample_count,
        total_bytes,
        artifacts,
    })
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
    if !matches!(schema, "ylx.device-session.v1" | "ylx.device-session.v2") {
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

fn collect_device_session_v1_artifacts(
    object: &Map<String, Value>,
) -> Result<Vec<ArtifactDescriptor>, SessionIoError> {
    let mut collector = ArtifactCollector::default();
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
        if matches!(
            audio.get("state").and_then(Value::as_str),
            Some("not_recorded")
        ) {
            return Ok(collector.finish());
        }
        let segments = array_field(audio, "segments", "manifest audio segments 无效")?;
        for segment in segments {
            let segment = segment.as_object().ok_or_else(|| {
                SessionIoError::new("manifest_invalid", "manifest audio segment 无效")
            })?;
            collector.collect(value_field(segment, "artifact")?)?;
        }
    }

    let artifacts = collector.finish();
    if artifacts.is_empty() {
        return Err(SessionIoError::new(
            "manifest_invalid",
            "manifest 未声明 artifact",
        ));
    }
    Ok(artifacts)
}

fn total_artifact_bytes(artifacts: &[ArtifactDescriptor]) -> Result<u64, SessionIoError> {
    artifacts.iter().try_fold(0_u64, |total, artifact| {
        total.checked_add(artifact.bytes).ok_or_else(|| {
            SessionIoError::new("manifest_invalid", "manifest artifact bytes 总和溢出")
        })
    })
}

pub fn device_session_v1_artifact(
    payload: &[u8],
    expected_session_id: &str,
    artifact_id: &str,
) -> Result<Option<ArtifactDescriptor>, SessionIoError> {
    if !is_sha256(artifact_id) {
        return Ok(None);
    }
    Ok(device_session_v1_artifacts(payload, expected_session_id)?
        .into_iter()
        .find(|artifact| artifact.artifact_id == artifact_id))
}

pub fn verify_device_session_artifacts(
    session_root: &Path,
    manifest_payload: &[u8],
    expected_session_id: &str,
    expected_identities: &[ExpectedArtifactIdentity],
) -> Result<u64, SessionIoError> {
    let artifacts = device_session_v1_artifacts(manifest_payload, expected_session_id)?;
    let expected = expected_identity_map(expected_identities)?;
    if expected.len() != artifacts.len() {
        return Err(SessionIoError::new(
            "artifact_invalid",
            "artifact 封存身份与 manifest 不一致",
        ));
    }
    let root = open_directory(session_root, "artifact_invalid", "无法安全打开会话目录")?;
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
        if actual != **expected_identity {
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
    if final_root.exists() {
        return Err(SessionIoError::new(
            "session_exists",
            "最终会话目录已经存在",
        ));
    }
    let artifact_count = verify_device_session_artifacts(
        partial_root,
        manifest_payload,
        expected_session_id,
        expected_identities,
    )?;
    let manifest_path = partial_root.join("manifest.json");
    write_manifest(&manifest_path, manifest_payload)?;
    fsync_directory(partial_root, "write_failed", "同步会话目录失败")?;
    for control_name in control_names {
        validate_control_name(control_name)?;
        match std::fs::remove_file(partial_root.join(control_name)) {
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
    fsync_directory(partial_root, "write_failed", "同步会话目录失败")?;
    std::fs::rename(partial_root, final_root)
        .map_err(|error| SessionIoError::os("write_failed", "发布会话目录失败", error))?;
    if let Some(parent) = final_root.parent() {
        fsync_directory(parent, "write_failed", "同步 generation 根目录失败")?;
    }
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

#[derive(Default)]
struct ArtifactCollector {
    descriptors: Vec<ArtifactDescriptor>,
    by_id: HashMap<String, usize>,
    paths: HashSet<String>,
}

impl ArtifactCollector {
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

    fn finish(self) -> Vec<ArtifactDescriptor> {
        self.descriptors
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
    if object.len() != 6 {
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

fn u64_field(
    object: &Map<String, Value>,
    name: &str,
    message: &'static str,
) -> Result<u64, SessionIoError> {
    object
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| SessionIoError::new("manifest_invalid", message))
}

fn non_negative_number_field(
    object: &Map<String, Value>,
    name: &str,
    message: &'static str,
) -> Result<f64, SessionIoError> {
    let value = object
        .get(name)
        .and_then(Value::as_f64)
        .ok_or_else(|| SessionIoError::new("manifest_invalid", message))?;
    if value < 0.0 {
        return Err(SessionIoError::new("manifest_invalid", message));
    }
    Ok(value)
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
    loop {
        let result = unsafe { libc::dup(fd) };
        if result >= 0 {
            return Ok(result);
        }
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::Interrupted {
            continue;
        }
        return Err(SessionIoError::os(
            "artifact_invalid",
            "无法复制根目录文件描述符",
            error,
        ));
    }
}

fn openat_component(
    parent_fd: libc::c_int,
    component: &str,
    directory: bool,
) -> Result<libc::c_int, SessionIoError> {
    let name = CString::new(component)
        .map_err(|_| SessionIoError::new("artifact_invalid", "路径组件包含 NUL"))?;
    let mut flags = libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW;
    if directory {
        flags |= libc::O_DIRECTORY;
    } else {
        flags |= libc::O_NONBLOCK;
    }
    loop {
        let descriptor = unsafe { libc::openat(parent_fd, name.as_ptr(), flags) };
        if descriptor >= 0 {
            return Ok(descriptor);
        }
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::Interrupted {
            continue;
        }
        return Err(SessionIoError::os(
            "artifact_invalid",
            "无法安全打开 artifact 路径",
            error,
        ));
    }
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

        let mut vectors: Vec<libc::iovec> =
            Vec::with_capacity((chunks.len() - index).min(WRITEV_MAX));
        vectors.push(iovec(&chunks[index][offset..]));
        for chunk in chunks[index + 1..].iter().filter(|chunk| !chunk.is_empty()) {
            if vectors.len() == WRITEV_MAX {
                break;
            }
            vectors.push(iovec(chunk));
        }
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
    if fd < 0 {
        return Err(SessionIoError::new("artifact_invalid", "文件描述符无效"));
    }
    let mut raw = MaybeUninit::<libc::stat>::uninit();
    let result = unsafe { libc::fstat(fd, raw.as_mut_ptr()) };
    if result != 0 {
        return Err(SessionIoError::os(
            "artifact_invalid",
            "无法读取文件身份",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(unsafe { raw.assume_init() })
}

fn identity_from_stat(raw: &libc::stat) -> Result<FileIdentity, SessionIoError> {
    let modified_ns = raw
        .st_mtime
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(raw.st_mtime_nsec))
        .ok_or_else(|| SessionIoError::new("artifact_invalid", "文件修改时间超出可表示范围"))?;
    Ok(FileIdentity {
        device: raw.st_dev,
        inode: raw.st_ino,
        size: raw.st_size as u64,
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
    loop {
        let result = unsafe {
            libc::pread(
                fd,
                buffer.as_mut_ptr().cast::<libc::c_void>(),
                buffer.len(),
                offset,
            )
        };
        if result >= 0 {
            return Ok(result as usize);
        }
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::Interrupted {
            continue;
        }
        return Err(SessionIoError::os(
            "artifact_invalid",
            "读取 artifact 失败",
            error,
        ));
    }
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

fn write_manifest(path: &Path, payload: &[u8]) -> Result<(), SessionIoError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o640)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| SessionIoError::os("write_failed", "创建 manifest 失败", error))?;
    file.write_all(payload)
        .map_err(|error| SessionIoError::os("write_failed", "写入 manifest 失败", error))?;
    file.sync_all()
        .map_err(|error| SessionIoError::os("write_failed", "同步 manifest 失败", error))
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
        ExpectedArtifactIdentity, device_session_v1_artifact, device_session_v1_artifacts,
        device_session_v1_summary, finalize_artifact, hash_file, open_relative_regular,
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
    }

    #[test]
    fn write_encoder_frame_uses_wire_header_and_payload() {
        let (left, mut right) = UnixStream::pair().unwrap();
        let written = write_encoder_frame(left.as_raw_fd(), b"abc").unwrap();
        assert_eq!(written, 11);
        drop(left);
        let mut received = Vec::new();
        right.read_to_end(&mut received).unwrap();
        assert_eq!(received, b"YLXF\x03\0\0\0abc");
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
    }

    #[test]
    fn extracts_device_session_v1_artifacts_in_manifest_order() {
        let payload = br#"{
          "schema":"ylx.device-session.v1",
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
          "schema":"ylx.device-session.v2",
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

        let summary =
            device_session_v1_summary(payload, "01989f6a-2c00-7a1b-8c2d-3e4f50617283").unwrap();
        assert_eq!(summary.audio_sample_count, None);
        assert_eq!(summary.total_bytes, 35);
    }

    #[test]
    fn selects_one_device_session_v1_artifact_by_identity() {
        let payload = br#"{
          "schema":"ylx.device-session.v1",
          "sealed":true,
          "session_id":"01989f6a-2c00-7a1b-8c2d-3e4f50617283",
          "video":{"layout":"split-eyes","segments":[{"artifacts":{
            "left":{"artifact_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","role":"video.left","path":"video/left.mp4","media_type":"video/mp4","bytes":10,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
            "right":{"artifact_id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","role":"video.right","path":"video/right.mp4","media_type":"video/mp4","bytes":11,"sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
          }}]},
          "frames":{"artifact":{"artifact_id":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","role":"frames.index","path":"frames.ndjson","media_type":"application/x-ndjson","bytes":12,"sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}},
          "imu":{"artifact":{"artifact_id":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","role":"imu.samples","path":"imu.ndjson","media_type":"application/x-ndjson","bytes":13,"sha256":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}}
        }"#;
        let selected = device_session_v1_artifact(
            payload,
            "01989f6a-2c00-7a1b-8c2d-3e4f50617283",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        )
        .unwrap()
        .unwrap();
        assert_eq!(selected.path, "video/right.mp4");
        assert_eq!(selected.bytes, 11);
        assert!(
            device_session_v1_artifact(
                payload,
                "01989f6a-2c00-7a1b-8c2d-3e4f50617283",
                "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            )
            .unwrap()
            .is_none()
        );
        assert!(
            device_session_v1_artifact(
                payload,
                "01989f6a-2c00-7a1b-8c2d-3e4f50617283",
                "not-a-sha",
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn summarizes_device_session_v1_with_total_bytes_and_artifacts() {
        let payload = br#"{
          "schema":"ylx.device-session.v1",
          "sealed":true,
          "session_id":"01989f6a-2c00-7a1b-8c2d-3e4f50617283",
          "display_name":"test capture",
          "time":{"started_at":"2026-08-08T02:24:00Z","ended_at":"2026-08-08T02:24:01.250Z","duration_seconds":1.25},
          "video":{"layout":"raw-side-by-side","artifact":{"artifact_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","role":"video.raw-side-by-side","path":"video/raw.mjpeg","media_type":"video/x-motion-jpeg","bytes":10,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
          "frames":{"count":7,"artifact":{"artifact_id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","role":"frames.index","path":"frames.ndjson","media_type":"application/x-ndjson","bytes":12,"sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}},
          "imu":{"sample_count":42,"artifact":{"artifact_id":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","role":"imu.samples","path":"imu.ndjson","media_type":"application/x-ndjson","bytes":13,"sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}},
          "audio":{"sample_count":48000,"segments":[{"artifact":{"artifact_id":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","role":"audio.wav","path":"audio/000000.wav","media_type":"audio/wav","bytes":14,"sha256":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}}]}
        }"#;
        let summary =
            device_session_v1_summary(payload, "01989f6a-2c00-7a1b-8c2d-3e4f50617283").unwrap();

        assert_eq!(summary.session_id, "01989f6a-2c00-7a1b-8c2d-3e4f50617283");
        assert_eq!(summary.display_name, "test capture");
        assert_eq!(summary.started_at, "2026-08-08T02:24:00Z");
        assert_eq!(summary.ended_at, "2026-08-08T02:24:01.250Z");
        assert_eq!(summary.duration_seconds, 1.25);
        assert_eq!(summary.frames_count, 7);
        assert_eq!(summary.imu_sample_count, 42);
        assert_eq!(summary.audio_sample_count, Some(48000));
        assert_eq!(summary.total_bytes, 49);
        let paths: Vec<_> = summary
            .artifacts
            .iter()
            .map(|item| item.path.as_str())
            .collect();
        assert_eq!(
            paths,
            vec![
                "video/raw.mjpeg",
                "frames.ndjson",
                "imu.ndjson",
                "audio/000000.wav"
            ]
        );
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
              "schema":"ylx.device-session.v1",
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
              "schema":"ylx.device-session.v1",
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
    }

    #[test]
    fn rejects_unsafe_device_session_artifact_paths() {
        let payload = br#"{
          "schema":"ylx.device-session.v1",
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

    fn tempfile_dir() -> std::path::PathBuf {
        let mut root = std::env::temp_dir();
        let unique = format!(
            "rp-ylx-session-io-{}-{}",
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
