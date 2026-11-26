use std::ffi::CStr;
use std::fs::File;
use std::io::{self, Read};
use std::mem::MaybeUninit;
use std::os::fd::FromRawFd;

pub(crate) const EXFAT_SUPER_MAGIC: i64 = 0x2011_bab0;
pub(crate) const MSDOS_SUPER_MAGIC: i64 = 0x4d44;
pub(crate) const FUSE_SUPER_MAGIC: i64 = 0x6573_5546;

pub(crate) fn fstat(fd: libc::c_int) -> io::Result<libc::stat> {
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

pub(crate) fn fstatat_nofollow(dir_fd: libc::c_int, name: &CStr) -> io::Result<libc::stat> {
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

pub(crate) fn openat_retry(
    dir_fd: libc::c_int,
    name: &CStr,
    flags: libc::c_int,
) -> io::Result<File> {
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

pub(crate) fn dup_cloexec(fd: libc::c_int) -> io::Result<libc::c_int> {
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

pub(crate) fn pread_retry(
    fd: libc::c_int,
    buffer: &mut [u8],
    offset: libc::off_t,
) -> io::Result<usize> {
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

pub(crate) fn read_retry(file: &mut File, buffer: &mut [u8]) -> io::Result<usize> {
    loop {
        match file.read(buffer) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

pub(crate) fn is_exfat_or_msdos(fd: libc::c_int) -> io::Result<bool> {
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

pub(crate) fn renameat2_noreplace(old_path: &CStr, new_path: &CStr) -> io::Result<()> {
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

#[cfg(test)]
mod tests {
    use super::{
        dup_cloexec, fstat, fstatat_nofollow, is_exfat_or_msdos, openat_retry, pread_retry,
        read_retry, renameat2_noreplace,
    };
    use std::ffi::CString;
    use std::fs::{self, File};
    use std::io::Write;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "rp-ylx-sys-fs-{name}-{}-{unique}",
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
}
