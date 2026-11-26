use crate::ids::hex_encode;
use serde::de::{DeserializeOwned, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use sha2::{Digest, Sha256};
use std::ffi::{CStr, CString};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

pub const EXFAT_SUPER_MAGIC: i64 = 0x2011_bab0;
pub const MSDOS_SUPER_MAGIC: i64 = 0x4d44;
pub const FUSE_SUPER_MAGIC: i64 = 0x6573_5546;

/// Linux `<linux/msdos_fs.h>` `FAT_IOCTL_GET_ATTRIBUTES = _IOR('r', 0x10, __u32)`.
pub const FAT_IOCTL_GET_ATTRIBUTES: u32 = 0x8004_7210;
/// Linux `<linux/msdos_fs.h>` `FAT_IOCTL_SET_ATTRIBUTES = _IOW('r', 0x11, __u32)`.
pub const FAT_IOCTL_SET_ATTRIBUTES: u32 = 0x4004_7211;
/// FAT / exFAT directory entry hidden attribute (`ATTR_HIDDEN`).
pub const FAT_ATTR_HIDDEN: u32 = 0x02;

/// Minimum aligned streaming I/O buffer size (256 KiB).
pub const MIN_IO_BUFFER_BYTES: usize = 256 * 1024;
/// Default aligned streaming I/O buffer size (256 KiB).
pub const DEFAULT_IO_BUFFER_BYTES: usize = 256 * 1024;
/// Maximum aligned streaming I/O buffer size (1 MiB).
pub const MAX_IO_BUFFER_BYTES: usize = 1024 * 1024;
const PAGE_ALIGN: usize = 4096;

static ATOMIC_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn fstat(fd: libc::c_int) -> io::Result<libc::stat> {
    if fd < 0 {
        return Err(io::Error::from_raw_os_error(libc::EBADF));
    }
    loop {
        let mut value = MaybeUninit::<libc::stat>::uninit();
        // SAFETY: `value` points to writable `libc::stat` storage.
        if unsafe { libc::fstat(fd, value.as_mut_ptr()) } == 0 {
            // SAFETY: kernel initialized `value` on 0 return.
            return Ok(unsafe { value.assume_init() });
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

pub fn fstatat_nofollow(dir_fd: libc::c_int, name: &CStr) -> io::Result<libc::stat> {
    loop {
        let mut value = MaybeUninit::<libc::stat>::uninit();
        // SAFETY: `name` is NUL-terminated and `value` is writable.
        if unsafe {
            libc::fstatat(
                dir_fd,
                name.as_ptr(),
                value.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } == 0
        {
            // SAFETY: kernel initialized `value` on 0 return.
            return Ok(unsafe { value.assume_init() });
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

pub fn openat_retry(dir_fd: libc::c_int, name: &CStr, flags: libc::c_int) -> io::Result<File> {
    if dir_fd < 0 && dir_fd != libc::AT_FDCWD {
        return Err(io::Error::from_raw_os_error(libc::EBADF));
    }
    if (flags & libc::O_CREAT) != 0 || (flags & libc::O_TMPFILE) == libc::O_TMPFILE {
        return Err(io::Error::from_raw_os_error(libc::EINVAL));
    }
    loop {
        // SAFETY: `name` is NUL-terminated and `flags` does not require `mode`.
        let fd = unsafe { libc::openat(dir_fd, name.as_ptr(), flags) };
        if fd >= 0 {
            // SAFETY: `fd` is a newly opened owned descriptor.
            return Ok(unsafe { File::from_raw_fd(fd) });
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

pub fn dup_cloexec(fd: libc::c_int) -> io::Result<libc::c_int> {
    if fd < 0 {
        return Err(io::Error::from_raw_os_error(libc::EBADF));
    }
    loop {
        // SAFETY: `F_DUPFD_CLOEXEC` takes a single `c_int` minimum fd argument.
        let duplicated = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
        if duplicated >= 0 {
            return Ok(duplicated);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

pub fn pread_retry(fd: libc::c_int, buffer: &mut [u8], offset: libc::off_t) -> io::Result<usize> {
    loop {
        // SAFETY: `buffer` is valid writable memory for `buffer.len()` bytes.
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
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

pub fn read_retry(file: &mut File, buffer: &mut [u8]) -> io::Result<usize> {
    loop {
        match file.read(buffer) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

pub fn is_exfat_or_msdos(fd: libc::c_int) -> io::Result<bool> {
    if fd < 0 {
        return Err(io::Error::from_raw_os_error(libc::EBADF));
    }
    loop {
        let mut buf = MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: `buf` points to writable `libc::statfs` storage.
        if unsafe { libc::fstatfs(fd, buf.as_mut_ptr()) } == 0 {
            // SAFETY: kernel initialized `buf` on 0 return.
            let buf = unsafe { buf.assume_init() };
            #[allow(clippy::unnecessary_cast)]
            let f_type = buf.f_type as i64;
            #[allow(clippy::unnecessary_cast)]
            let f_files = buf.f_files as u64;
            return Ok(f_type == EXFAT_SUPER_MAGIC
                || f_type == MSDOS_SUPER_MAGIC
                || (f_type == FUSE_SUPER_MAGIC && f_files == 0));
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

pub fn renameat2_noreplace(old_path: &CStr, new_path: &CStr) -> io::Result<()> {
    loop {
        // SAFETY: `old_path` and `new_path` are NUL-terminated C strings.
        let rc = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                old_path.as_ptr(),
                libc::AT_FDCWD,
                new_path.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if rc == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

/// Safely open a regular file read-only with `O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK`,
/// verifying `S_ISREG` and `st_nlink == 1` (no symlinks, FIFOs, devices, or hardlinks).
pub fn open_readonly_safe(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)?;
    let stat = fstat(file.as_raw_fd())?;
    if (stat.st_mode & libc::S_IFMT) != libc::S_IFREG {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path is not a regular file",
        ));
    }
    #[allow(clippy::useless_conversion)]
    let nlink = u64::from(stat.st_nlink);
    if nlink != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "regular file must not have hardlinks",
        ));
    }
    Ok(file)
}

/// Safely open a directory read-only with `O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC`.
pub fn open_directory_safe(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
}

/// Safely create a new regular file exclusively with `O_NOFOLLOW | O_CLOEXEC`.
pub fn open_create_exclusive(path: &Path, mode: u32) -> io::Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
}

/// Flush directory metadata to disk (`O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC`).
pub fn fsync_directory(path: &Path) -> io::Result<()> {
    let dir = open_directory_safe(path)?;
    dir.sync_all()
}

/// Read FAT / exFAT file attributes via `FAT_IOCTL_GET_ATTRIBUTES`.
pub fn get_fat_attributes(fd: RawFd) -> io::Result<u32> {
    if fd < 0 {
        return Err(io::Error::from_raw_os_error(libc::EBADF));
    }
    let mut attrs: u32 = 0;
    #[allow(clippy::unnecessary_cast)]
    let request = FAT_IOCTL_GET_ATTRIBUTES as libc::Ioctl;
    // SAFETY: `attrs` is a valid mutable `u32` expected by `FAT_IOCTL_GET_ATTRIBUTES`.
    if unsafe { libc::ioctl(fd, request, &mut attrs) } == 0 {
        Ok(attrs)
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Write FAT / exFAT file attributes via `FAT_IOCTL_SET_ATTRIBUTES`.
pub fn set_fat_attributes(fd: RawFd, attrs: u32) -> io::Result<()> {
    if fd < 0 {
        return Err(io::Error::from_raw_os_error(libc::EBADF));
    }
    let mut value = attrs;
    #[allow(clippy::unnecessary_cast)]
    let request = FAT_IOCTL_SET_ATTRIBUTES as libc::Ioctl;
    // SAFETY: `value` points to a readable `u32` expected by `FAT_IOCTL_SET_ATTRIBUTES`.
    if unsafe { libc::ioctl(fd, request, &mut value) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Toggle exFAT/FAT native `ATTR_HIDDEN` on `path` (file or directory).
///
/// Returns `Ok(true)` if the ioctl succeeded, or `Ok(false)` when `path` is on a
/// filesystem that does not implement `FAT_IOCTL_*_ATTRIBUTES` (`ENOTTY`,
/// `EOPNOTSUPP`, `ENOTSUP`, `EINVAL`, `ENOSYS`). Other I/O errors are returned as `Err`.
pub fn set_exfat_hidden(path: &Path, hidden: bool) -> io::Result<bool> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)?;
    match set_exfat_hidden_fd(file.as_raw_fd(), hidden) {
        Ok(()) => Ok(true),
        Err(error) if is_fat_ioctl_unsupported(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Toggle exFAT/FAT native `ATTR_HIDDEN` on an open descriptor without suppressing
/// unsupported-filesystem errors.
pub fn set_exfat_hidden_fd(fd: RawFd, hidden: bool) -> io::Result<()> {
    let current = get_fat_attributes(fd)?;
    let updated = if hidden {
        current | FAT_ATTR_HIDDEN
    } else {
        current & !FAT_ATTR_HIDDEN
    };
    if updated == current {
        return Ok(());
    }
    set_fat_attributes(fd, updated)
}

fn is_fat_ioctl_unsupported(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::ENOTTY | libc::EOPNOTSUPP | libc::EINVAL | libc::ENOSYS)
    )
}

/// Call `posix_fadvise(fd, offset, len, advice)` and convert its direct errno return.
pub fn fadvise(fd: RawFd, offset: i64, len: i64, advice: libc::c_int) -> io::Result<()> {
    if fd < 0 {
        return Err(io::Error::from_raw_os_error(libc::EBADF));
    }
    let offset =
        libc::off_t::try_from(offset).map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
    let len = libc::off_t::try_from(len).map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
    // SAFETY: `posix_fadvise` has no pointer arguments and returns 0 or a positive errno.
    let rc = unsafe { libc::posix_fadvise(fd, offset, len, advice) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(rc))
    }
}

/// Hint sequential read-ahead for a file descriptor.
pub fn fadvise_sequential(fd: RawFd, offset: i64, len: i64) -> io::Result<()> {
    fadvise(fd, offset, len, libc::POSIX_FADV_SEQUENTIAL)
}

/// Hint that clean pages for `[offset, offset + len)` (or entire file when `len == 0`)
/// can be evicted from page cache.
pub fn fadvise_dontneed(fd: RawFd, offset: i64, len: i64) -> io::Result<()> {
    fadvise(fd, offset, len, libc::POSIX_FADV_DONTNEED)
}

/// Sync a regular file to disk and best-effort evict its clean pages from page cache
/// to avoid exFAT dirty-page writeback stalls.
pub fn sync_and_evict_page_cache(file: &File) -> io::Result<()> {
    file.sync_all()?;
    if let Err(error) = fadvise_dontneed(file.as_raw_fd(), 0, 0) {
        if !matches!(
            error.raw_os_error(),
            Some(libc::EINVAL | libc::ESPIPE | libc::ENOSYS | libc::EOPNOTSUPP)
        ) {
            return Err(error);
        }
    }
    Ok(())
}

/// Clamp and page-align an I/O buffer capacity into `[256 KiB, 1 MiB]`.
pub fn aligned_io_capacity(requested: usize) -> usize {
    let clamped = requested.clamp(MIN_IO_BUFFER_BYTES, MAX_IO_BUFFER_BYTES);
    let rounded = (clamped + (PAGE_ALIGN - 1)) & !(PAGE_ALIGN - 1);
    rounded.min(MAX_IO_BUFFER_BYTES)
}

/// Page-aligned 256 KiB–1 MiB buffered writer tailored for exFAT sequential throughput.
pub struct AlignedBufWriter<W: Write> {
    inner: BufWriter<W>,
    capacity: usize,
}

impl<W: Write> AlignedBufWriter<W> {
    pub fn new(inner: W) -> Self {
        Self::with_capacity(DEFAULT_IO_BUFFER_BYTES, inner)
    }

    pub fn with_capacity(requested_bytes: usize, inner: W) -> Self {
        let capacity = aligned_io_capacity(requested_bytes);
        Self {
            inner: BufWriter::with_capacity(capacity, inner),
            capacity,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn buffered(&self) -> usize {
        self.inner.buffer().len()
    }

    pub fn buffered_slice(&self) -> &[u8] {
        self.inner.buffer()
    }

    pub fn get_ref(&self) -> &W {
        self.inner.get_ref()
    }

    pub fn get_mut(&mut self) -> &mut W {
        self.inner.get_mut()
    }

    pub fn into_inner(self) -> io::Result<W> {
        self.inner.into_inner().map_err(|error| error.into_error())
    }
}

impl<W: Write> Write for AlignedBufWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Completed digest and exact byte length produced by [`Sha256Writer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingDigest {
    pub bytes: u64,
    pub sha256: String,
}

/// Streaming `Write` wrapper that computes SHA-256 on the fly with zero file reread.
pub struct Sha256Writer<W: Write> {
    inner: W,
    hasher: Sha256,
    bytes: u64,
}

impl<W: Write> Sha256Writer<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            bytes: 0,
        }
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes
    }

    pub fn peek_sha256(&self) -> String {
        hex_encode(self.hasher.clone().finalize().as_slice())
    }

    pub fn get_ref(&self) -> &W {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut W {
        &mut self.inner
    }

    /// Flush the underlying writer and return `(inner, StreamingDigest)`.
    pub fn finish(mut self) -> io::Result<(W, StreamingDigest)> {
        self.inner.flush()?;
        let digest = StreamingDigest {
            bytes: self.bytes,
            sha256: hex_encode(self.hasher.finalize().as_slice()),
        };
        Ok((self.inner, digest))
    }
}

impl<W: Write> Write for Sha256Writer<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.hasher.update(&buf[..written]);
        self.bytes = self
            .bytes
            .checked_add(written as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::FileTooLarge, "byte count overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Atomically write `payload` to `path` via an exclusive temp file + `fchmod` + `fsync` + `rename`
/// (+ parent directory `fsync` when `sync_dir` is true). Tolerates exFAT `fchmod` rejection.
pub fn write_bytes_atomic(
    path: &Path,
    payload: &[u8],
    mode: u32,
    sync_dir: bool,
) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "atomic write path has no parent directory",
        )
    })?;
    let name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "atomic write path has no file name",
        )
    })?;
    let seq = ATOMIC_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let temp_name = format!(
        ".{}.{}.{}.tmp",
        name.to_string_lossy(),
        std::process::id(),
        seq
    );
    let temp_path = parent.join(temp_name);

    let result = (|| -> io::Result<()> {
        let mut file = open_create_exclusive(&temp_path, mode)?;
        let fd = file.as_raw_fd();
        // SAFETY: `fchmod` operates on an open owned file descriptor.
        if unsafe { libc::fchmod(fd, mode as libc::mode_t) } != 0 {
            let error = io::Error::last_os_error();
            let exfat_unsupported = matches!(
                error.raw_os_error(),
                Some(libc::EPERM | libc::EINVAL | libc::EOPNOTSUPP | libc::EXDEV)
            );
            if !(exfat_unsupported && is_exfat_or_msdos(fd).unwrap_or(false)) {
                return Err(error);
            }
        }
        file.write_all(payload)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temp_path, path)?;
        if sync_dir {
            fsync_directory(parent)?;
        }
        Ok(())
    })();

    if result.is_err() {
        let _ = std::fs::remove_file(&temp_path);
    }
    result
}

/// TOCTOU-safe bounded regular-file reader rejecting symlinks, hardlinks, and oversized files.
pub fn read_bounded_bytes(path: &Path, max_bytes: usize) -> io::Result<Vec<u8>> {
    if max_bytes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "max_bytes must be positive",
        ));
    }
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "path has no parent directory")
    })?;
    let file_name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no file name"))?;
    let name_c = CString::new(file_name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file name contains NUL byte"))?;

    let dir = open_directory_safe(parent)?;
    let expected = fstatat_nofollow(dir.as_raw_fd(), &name_c)?;
    validate_bounded_regular_stat(&expected, max_bytes)?;

    let mut file = openat_retry(
        dir.as_raw_fd(),
        &name_c,
        libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
    )?;
    let opened = fstat(file.as_raw_fd())?;
    validate_bounded_regular_stat(&opened, max_bytes)?;
    if opened.st_dev != expected.st_dev || opened.st_ino != expected.st_ino {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "file changed before read",
        ));
    }

    let limit = max_bytes
        .checked_add(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "max_bytes overflow"))?;
    let mut payload = Vec::with_capacity(max_bytes.min(64 * 1024));
    let mut buf = [0_u8; 8192];
    while payload.len() < limit {
        let to_read = buf.len().min(limit - payload.len());
        let read = read_retry(&mut file, &mut buf[..to_read])?;
        if read == 0 {
            break;
        }
        payload.extend_from_slice(&buf[..read]);
    }
    if payload.len() > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("file exceeds maximum size of {max_bytes} bytes"),
        ));
    }
    let after = fstat(file.as_raw_fd())?;
    if after.st_dev != opened.st_dev
        || after.st_ino != opened.st_ino
        || after.st_size != opened.st_size
        || after.st_mtime != opened.st_mtime
        || after.st_mtime_nsec != opened.st_mtime_nsec
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "file changed during read",
        ));
    }
    Ok(payload)
}

fn validate_bounded_regular_stat(stat: &libc::stat, max_bytes: usize) -> io::Result<()> {
    if (stat.st_mode & libc::S_IFMT) != libc::S_IFREG {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path is not a regular file",
        ));
    }
    #[allow(clippy::useless_conversion)]
    let nlink = u64::from(stat.st_nlink);
    if nlink != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "regular file must not have hardlinks",
        ));
    }
    let size = u64::try_from(stat.st_size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative file size"))?;
    if size > max_bytes as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("file exceeds maximum size of {max_bytes} bytes"),
        ));
    }
    Ok(())
}

/// Strictly parse bounded UTF-8 JSON rejecting duplicate keys and excessive nesting depth.
pub fn parse_strict_json_value(raw: &[u8], max_depth: usize) -> io::Result<serde_json::Value> {
    let text = std::str::from_utf8(raw)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let strict: StrictJsonValue = serde_json::from_str(text)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    validate_json_depth(&strict.0, max_depth)?;
    Ok(strict.0)
}

/// TOCTOU-safe bounded strict JSON file reader returning a `serde_json::Value`.
pub fn read_bounded_json_value(
    path: &Path,
    max_bytes: usize,
    max_depth: usize,
) -> io::Result<serde_json::Value> {
    let raw = read_bounded_bytes(path, max_bytes)?;
    parse_strict_json_value(&raw, max_depth)
}

/// TOCTOU-safe bounded strict JSON reader deserializing into `T`.
pub fn read_bounded_json<T: DeserializeOwned>(
    path: &Path,
    max_bytes: usize,
    max_depth: usize,
) -> io::Result<T> {
    let value = read_bounded_json_value(path, max_bytes, max_depth)?;
    serde_json::from_value(value).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn validate_json_depth(root: &serde_json::Value, max_depth: usize) -> io::Result<()> {
    let mut stack = vec![(root, 0_usize)];
    while let Some((node, depth)) = stack.pop() {
        if depth > max_depth {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("JSON nesting depth exceeds {max_depth}"),
            ));
        }
        match node {
            serde_json::Value::Object(map) => {
                for child in map.values() {
                    stack.push((child, depth + 1));
                }
            }
            serde_json::Value::Array(items) => {
                for child in items {
                    stack.push((child, depth + 1));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

struct StrictJsonValue(serde_json::Value);

impl<'de> Deserialize<'de> for StrictJsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StrictVisitor;

        impl<'de> Visitor<'de> for StrictVisitor {
            type Value = StrictJsonValue;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("any valid JSON value without duplicate object keys")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(serde_json::Value::Bool(v)))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(serde_json::Value::Number(v.into())))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(serde_json::Value::Number(v.into())))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let number = serde_json::Number::from_f64(v)
                    .ok_or_else(|| E::custom("non-finite JSON number"))?;
                Ok(StrictJsonValue(serde_json::Value::Number(number)))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(serde_json::Value::String(v.to_owned())))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(serde_json::Value::String(v)))
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(serde_json::Value::Null))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(serde_json::Value::Null))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element::<StrictJsonValue>()? {
                    items.push(item.0);
                }
                Ok(StrictJsonValue(serde_json::Value::Array(items)))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut out = serde_json::Map::new();
                while let Some((key, value)) = map.next_entry::<String, StrictJsonValue>()? {
                    if out.contains_key(&key) {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate JSON key: {key}"
                        )));
                    }
                    out.insert(key, value.0);
                }
                Ok(StrictJsonValue(serde_json::Value::Object(out)))
            }
        }

        deserializer.deserialize_any(StrictVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::symlink;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "miro-ego-core-fs-{name}-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        path
    }

    #[test]
    fn rejects_invalid_fds_and_create_or_tmpfile_flags() {
        assert_eq!(fstat(-1).unwrap_err().raw_os_error(), Some(libc::EBADF));
        assert_eq!(
            dup_cloexec(-1).unwrap_err().raw_os_error(),
            Some(libc::EBADF)
        );
        assert_eq!(
            is_exfat_or_msdos(-1).unwrap_err().raw_os_error(),
            Some(libc::EBADF)
        );
        assert_eq!(
            get_fat_attributes(-1).unwrap_err().raw_os_error(),
            Some(libc::EBADF)
        );
        assert_eq!(
            set_fat_attributes(-1, FAT_ATTR_HIDDEN)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EBADF)
        );
        assert_eq!(
            fadvise_dontneed(-1, 0, 0).unwrap_err().raw_os_error(),
            Some(libc::EBADF)
        );

        let root = temp_dir("flags");
        let dir = File::open(&root).unwrap();
        let name = CString::new("item.txt").unwrap();
        assert_eq!(
            openat_retry(-2, &name, libc::O_RDONLY | libc::O_CLOEXEC)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EBADF)
        );
        for flags in [
            libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC,
            libc::O_RDWR | libc::O_TMPFILE | libc::O_CLOEXEC,
            libc::O_RDWR | libc::O_CREAT | libc::O_TMPFILE | libc::O_CLOEXEC,
        ] {
            assert_eq!(
                openat_retry(dir.as_raw_fd(), &name, flags)
                    .unwrap_err()
                    .raw_os_error(),
                Some(libc::EINVAL)
            );
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn openat_dup_pread_read_fstatat_and_renameat2_noreplace_work() {
        let root = temp_dir("ops");
        fs::create_dir(root.join("sub")).unwrap();
        fs::write(root.join("sub/data.bin"), b"hello world").unwrap();

        let dir = File::open(&root).unwrap();
        let sub_name = CString::new("sub").unwrap();
        let sub_dir = openat_retry(
            dir.as_raw_fd(),
            &sub_name,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
        .unwrap();
        let data_name = CString::new("data.bin").unwrap();
        let stat = fstatat_nofollow(sub_dir.as_raw_fd(), &data_name).unwrap();
        assert_eq!(stat.st_size, 11);

        let mut file = openat_retry(
            sub_dir.as_raw_fd(),
            &data_name,
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
        .unwrap();
        let mut pread_buf = [0_u8; 5];
        assert_eq!(pread_retry(file.as_raw_fd(), &mut pread_buf, 6).unwrap(), 5);
        assert_eq!(&pread_buf, b"world");

        let mut read_buf = [0_u8; 16];
        let n = read_retry(&mut file, &mut read_buf).unwrap();
        assert_eq!(&read_buf[..n], b"hello world");

        let dup_fd = dup_cloexec(file.as_raw_fd()).unwrap();
        let fd_flags = unsafe { libc::fcntl(dup_fd, libc::F_GETFD) };
        assert_ne!(fd_flags & libc::FD_CLOEXEC, 0);
        unsafe { libc::close(dup_fd) };

        let src = root.join("src.txt");
        let dst = root.join("dst.txt");
        let existing = root.join("existing.txt");
        File::create(&src).unwrap().write_all(b"x").unwrap();
        File::create(&existing).unwrap().write_all(b"y").unwrap();

        let src_c = CString::new(src.as_os_str().as_bytes()).unwrap();
        let dst_c = CString::new(dst.as_os_str().as_bytes()).unwrap();
        let existing_c = CString::new(existing.as_os_str().as_bytes()).unwrap();
        assert_eq!(
            renameat2_noreplace(&src_c, &existing_c)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EEXIST)
        );
        renameat2_noreplace(&src_c, &dst_c).unwrap();
        assert!(!src.exists());
        assert_eq!(fs::read(&dst).unwrap(), b"x");

        assert!(!is_exfat_or_msdos(sub_dir.as_raw_fd()).unwrap());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn fadvise_and_exfat_hidden_behave_safely_on_regular_filesystems() {
        let root = temp_dir("fadvise-fat");
        let file_path = root.join("payload.bin");
        fs::write(&file_path, vec![0x5a_u8; 64 * 1024]).unwrap();

        let file = open_readonly_safe(&file_path).unwrap();
        fadvise_sequential(file.as_raw_fd(), 0, 0).unwrap();
        fadvise_dontneed(file.as_raw_fd(), 0, 0).unwrap();
        sync_and_evict_page_cache(&file).unwrap();

        // On Linux tmpfs/ext4, FAT_IOCTL_GET_ATTRIBUTES returns ENOTTY/EINVAL,
        // so set_exfat_hidden cleanly reports Ok(false) instead of failing.
        let hidden_dir = root.join(".miro-ego");
        fs::create_dir(&hidden_dir).unwrap();
        assert!(!set_exfat_hidden(&hidden_dir, true).unwrap());
        assert!(set_exfat_hidden_fd(file.as_raw_fd(), true).is_err());

        // Verify exact Linux ioctl numbers match <linux/msdos_fs.h>
        assert_eq!(FAT_IOCTL_GET_ATTRIBUTES, 0x8004_7210);
        assert_eq!(FAT_IOCTL_SET_ATTRIBUTES, 0x4004_7211);
        assert_eq!(FAT_ATTR_HIDDEN, 0x02);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn aligned_buf_and_streaming_sha256_writer_compute_exact_digest_without_reread() {
        assert_eq!(aligned_io_capacity(1024), MIN_IO_BUFFER_BYTES);
        assert_eq!(aligned_io_capacity(300_000), 303_104);
        assert_eq!(aligned_io_capacity(4 * 1024 * 1024), MAX_IO_BUFFER_BYTES);

        let root = temp_dir("sha256-writer");
        let path = root.join("streamed.bin");
        let file = open_create_exclusive(&path, 0o640).unwrap();
        let buffered = AlignedBufWriter::new(file);
        assert_eq!(buffered.capacity(), DEFAULT_IO_BUFFER_BYTES);
        let mut writer = Sha256Writer::new(buffered);

        let chunk_a = b"miro-ego streaming chunk a ";
        let chunk_b = vec![0x42_u8; 300_000];
        writer.write_all(chunk_a).unwrap();
        writer.write_all(&chunk_b).unwrap();
        let (buffered_file, digest) = writer.finish().unwrap();
        let file = buffered_file.into_inner().unwrap();
        sync_and_evict_page_cache(&file).unwrap();

        let disk_bytes = fs::read(&path).unwrap();
        let expected_sha = hex_encode(Sha256::digest(&disk_bytes).as_slice());
        assert_eq!(digest.bytes, disk_bytes.len() as u64);
        assert_eq!(digest.sha256, expected_sha);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn atomic_write_and_strict_bounded_json_reject_links_duplicates_and_overflow() {
        let root = temp_dir("atomic-json");
        let target = root.join("config.json");
        write_bytes_atomic(&target, br#"{"a":1,"b":{"c":2}}"#, 0o640, true).unwrap();

        let parsed = read_bounded_json_value(&target, 1024, 8).unwrap();
        assert_eq!(parsed["b"]["c"], 2);

        // Reject oversized file
        assert!(read_bounded_bytes(&target, 4).is_err());

        // Reject duplicate keys
        let dup_path = root.join("dup.json");
        write_bytes_atomic(&dup_path, br#"{"a":1,"a":2}"#, 0o640, true).unwrap();
        assert!(read_bounded_json_value(&dup_path, 1024, 8).is_err());

        // Reject excessive depth
        assert!(read_bounded_json_value(&target, 1024, 1).is_err());

        // Reject symlink and hardlink
        let sym_path = root.join("sym.json");
        symlink(&target, &sym_path).unwrap();
        assert!(read_bounded_bytes(&sym_path, 1024).is_err());

        let hard_path = root.join("hard.json");
        fs::hard_link(&target, &hard_path).unwrap();
        assert!(read_bounded_bytes(&target, 1024).is_err());

        fs::remove_dir_all(root).unwrap();
    }
}
