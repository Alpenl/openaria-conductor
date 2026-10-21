//! Metadata-only catalog admission. Payload hashes remain download-time checks.
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

type Identity = (u64, u64, u32, i64, i64, i64, i64);
fn identity(m: &fs::Metadata) -> Identity {
    (
        m.dev(),
        m.ino(),
        m.mode(),
        m.mtime(),
        m.mtime_nsec(),
        m.ctime(),
        m.ctime_nsec(),
    )
}
fn stat_identity(m: &libc::stat) -> Identity {
    (
        m.st_dev,
        m.st_ino,
        m.st_mode,
        m.st_mtime,
        m.st_mtime_nsec,
        m.st_ctime,
        m.st_ctime_nsec,
    )
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid indexed artifact path")
}
fn stat_at(fd: i32, name: &CString) -> io::Result<libc::stat> {
    loop {
        let mut result = MaybeUninit::<libc::stat>::uninit();
        if unsafe {
            libc::fstatat(
                fd,
                name.as_ptr(),
                result.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } == 0
        {
            return Ok(unsafe { result.assume_init() });
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}
fn open_at(fd: i32, name: &CString) -> io::Result<File> {
    loop {
        let child = unsafe {
            libc::openat(
                fd,
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            )
        };
        if child >= 0 {
            return Ok(unsafe { File::from_raw_fd(child) });
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}
fn parts(path: &[u8]) -> io::Result<Vec<&[u8]>> {
    if path.starts_with(b"/") || path.contains(&0) {
        return Err(invalid());
    }
    let parts: Vec<_> = path
        .split(|&b| b == b'/')
        .filter(|p| !p.is_empty() && *p != b".")
        .collect();
    if parts.is_empty() || parts.contains(&&b".."[..]) {
        return Err(invalid());
    }
    Ok(parts)
}
fn check_parents(root: &File, opened: &[(File, CString, Identity)]) -> io::Result<bool> {
    let mut parent = root.as_raw_fd();
    for (file, name, expected) in opened {
        if stat_identity(&stat_at(parent, name)?) != *expected {
            return Ok(false);
        }
        parent = file.as_raw_fd();
    }
    Ok(true)
}
pub fn matches(root: &Path, artifacts: &[(Vec<u8>, i64)]) -> io::Result<bool> {
    matches_inner(root, artifacts, |_| {})
}
fn matches_inner(
    root: &Path,
    artifacts: &[(Vec<u8>, i64)],
    mut after_file: impl FnMut(usize),
) -> io::Result<bool> {
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(root)?;
    let root_identity = identity(&directory.metadata()?);
    let mut previous: Option<Vec<&[u8]>> = None;
    let mut opened: Vec<(File, CString, Identity)> = Vec::new();
    for (index, (path, expected_bytes)) in artifacts.iter().enumerate() {
        let parts = parts(path)?;
        let parent = &parts[..parts.len() - 1];
        if previous.as_deref() != Some(parent) {
            if !check_parents(&directory, &opened)? {
                return Ok(false);
            }
            opened.clear();
            let mut fd = directory.as_raw_fd();
            for part in parent {
                let name = CString::new(*part).map_err(|_| invalid())?;
                let child = open_at(fd, &name)?;
                let id = identity(&child.metadata()?);
                fd = child.as_raw_fd();
                opened.push((child, name, id));
            }
            previous = Some(parent.to_vec());
        }
        let fd = opened
            .last()
            .map_or(directory.as_raw_fd(), |(f, _, _)| f.as_raw_fd());
        let name = CString::new(parts[parts.len() - 1]).map_err(|_| invalid())?;
        let meta = stat_at(fd, &name)?;
        after_file(index);
        if meta.st_mode & libc::S_IFMT != libc::S_IFREG
            || meta.st_size != *expected_bytes
            || meta.st_nlink != 1
        {
            return Ok(false);
        }
    }
    Ok(check_parents(&directory, &opened)?
        && identity(&fs::symlink_metadata(root)?) == root_identity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    struct Fixture(std::path::PathBuf);
    impl Fixture {
        fn new() -> Self {
            let p = std::env::temp_dir().join(format!(
                "oa-admit-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            fs::create_dir_all(p.join("video/left")).unwrap();
            fs::write(p.join("video/left/file"), b"x").unwrap();
            Self(p)
        }
        fn artifacts(&self) -> Vec<(Vec<u8>, i64)> {
            vec![(b"video/left/file".to_vec(), 1)]
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    #[test]
    fn normalizes_posix_paths_and_checks_exact_size() {
        let f = Fixture::new();
        for p in ["video/left/file", "./video//left/./file/"] {
            assert!(matches(&f.0, &[(p.as_bytes().to_vec(), 1)]).unwrap());
            assert!(!matches(&f.0, &[(p.as_bytes().to_vec(), 2)]).unwrap());
        }
        assert!(matches(&f.0, &[]).unwrap());
    }
    #[test]
    fn rejects_escape_nul_empty_paths_and_links() {
        let f = Fixture::new();
        for p in [
            "",
            ".",
            "/video/left/file",
            "../file",
            "video/../file",
            "video/left/\0",
        ] {
            assert!(matches(&f.0, &[(p.as_bytes().to_vec(), 1)]).is_err());
        }
        let p = f.0.join("video/left/file");
        fs::rename(&p, f.0.join("old")).unwrap();
        symlink(f.0.join("old"), &p).unwrap();
        assert!(!matches(&f.0, &f.artifacts()).unwrap());
        fs::remove_file(&p).unwrap();
        fs::hard_link(f.0.join("old"), &p).unwrap();
        assert!(!matches(&f.0, &f.artifacts()).unwrap());
    }
    #[test]
    fn rejects_parent_replacement_after_file_check() {
        let f = Fixture::new();
        let result = matches_inner(&f.0, &f.artifacts(), |_| {
            fs::rename(f.0.join("video/left"), f.0.join("retired")).unwrap();
            symlink(f.0.join("retired"), f.0.join("video/left")).unwrap();
        });
        assert!(!result.unwrap());
    }
    #[test]
    fn rejects_root_replacement_after_file_check() {
        let f = Fixture::new();
        let old = f.0.with_extension("retired");
        let result = matches_inner(&f.0, &f.artifacts(), |_| {
            fs::rename(&f.0, &old).unwrap();
            symlink(&old, &f.0).unwrap();
        });
        fs::remove_file(&f.0).unwrap();
        fs::rename(&old, &f.0).unwrap();
        assert!(!result.unwrap());
    }
    #[test]
    fn checks_previous_group_before_opening_next() {
        let f = Fixture::new();
        fs::write(f.0.join("root"), b"x").unwrap();
        let mut a = f.artifacts();
        a.push((b"root".to_vec(), 1));
        let result = matches_inner(&f.0, &a, |index| {
            if index == 0 {
                fs::rename(f.0.join("video/left"), f.0.join("retired")).unwrap();
                fs::create_dir(f.0.join("video/left")).unwrap();
            }
        });
        assert!(!result.unwrap());
    }
}
