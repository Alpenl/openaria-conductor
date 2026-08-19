use sha2::{Digest, Sha256};
use std::fs::OpenOptions;
use std::io::Read;
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

#[cfg(test)]
mod tests {
    use super::{hash_file, sendfile_all, verify_fd, write_encoder_frame};
    use std::fs::{self, File};
    use std::io::{Read, Seek, SeekFrom};
    use std::os::fd::AsRawFd;
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
