//! Fresh, bounded manifest signatures without per-file Python I/O bookkeeping.
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

const NAME: &[u8] = b"manifest.json\0";
type Identity = (
    libc::dev_t,
    libc::ino_t,
    libc::mode_t,
    libc::nlink_t,
    libc::off_t,
    i128,
    i128,
);

fn identity(value: &libc::stat) -> Identity {
    (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_size,
        i128::from(value.st_mtime) * 1_000_000_000 + i128::from(value.st_mtime_nsec),
        i128::from(value.st_ctime) * 1_000_000_000 + i128::from(value.st_ctime_nsec),
    )
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn fstat(fd: libc::c_int) -> io::Result<libc::stat> {
    let mut value = MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstat(fd, value.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { value.assume_init() })
}

fn read_retry(file: &mut File, buffer: &mut [u8]) -> io::Result<usize> {
    loop {
        match file.read(buffer) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

pub fn signature(path: &Path, maximum_bytes: usize) -> io::Result<String> {
    signature_inner(path, maximum_bytes, || {})
}

fn signature_inner(
    path: &Path,
    maximum_bytes: usize,
    after_read: impl FnOnce(),
) -> io::Result<String> {
    let parent = fs::symlink_metadata(path)?;
    if !parent.is_dir() {
        return Err(invalid("catalog directory must not be a symbolic link"));
    }
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)?;
    let opened = fstat(directory.as_raw_fd())?;
    if parent.dev() != opened.st_dev || parent.ino() != opened.st_ino {
        return Err(invalid("catalog directory changed during open"));
    }
    let mut file = loop {
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                NAME.as_ptr().cast(),
                libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            )
        };
        if fd >= 0 {
            // File owns the descriptor before any fallible allocation/read.
            break unsafe { File::from_raw_fd(fd) };
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    };
    let before = fstat(file.as_raw_fd())?;
    if before.st_mode & libc::S_IFMT != libc::S_IFREG
        || before.st_nlink != 1
        || before.st_size < 0
        || before.st_size as u64 > maximum_bytes as u64
    {
        return Err(invalid(
            "catalog manifest must be a bounded exclusively linked file",
        ));
    }
    let expected = identity(&before);
    let mut remaining = before.st_size as u64;
    let mut buffer = vec![0_u8; remaining.clamp(1, 1024 * 1024) as usize];
    let mut digest = Sha256::new();
    while remaining > 0 {
        let count = remaining.min(buffer.len() as u64) as usize;
        let read = read_retry(&mut file, &mut buffer[..count])?;
        if read == 0 {
            return Err(invalid("catalog manifest truncated during read"));
        }
        digest.update(&buffer[..read]);
        remaining -= read as u64;
    }
    after_read();
    if read_retry(&mut file, &mut [0_u8; 1])? != 0
        || identity(&fstat(file.as_raw_fd())?) != expected
    {
        return Err(invalid("catalog manifest changed during read"));
    }
    let mut current = MaybeUninit::<libc::stat>::uninit();
    if unsafe {
        libc::fstatat(
            directory.as_raw_fd(),
            NAME.as_ptr().cast(),
            current.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    if identity(&unsafe { current.assume_init() }) != expected {
        return Err(invalid("catalog manifest replaced during read"));
    }
    // Preserve the existing json.dumps signature byte-for-byte, including spaces.
    Ok(format!(
        "[{}, {}, {}, {}, {}, {}, {}, {}, {}, \"{:x}\"]",
        parent.dev(),
        parent.ino(),
        expected.0,
        expected.1,
        expected.2,
        expected.3,
        expected.4,
        expected.5,
        expected.6,
        digest.finalize()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};

    struct Fixture(std::path::PathBuf);
    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "openaria-catalog-signature-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            fs::create_dir(&root).unwrap();
            fs::write(root.join("manifest.json"), b"original").unwrap();
            Self(root)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn signature_hashes_exact_content_and_all_identity_fields() {
        let f = Fixture::new();
        let value: serde_json::Value = serde_json::from_str(&signature(&f.0, 8).unwrap()).unwrap();
        assert_eq!(value.as_array().unwrap().len(), 10);
        assert_eq!(value[6], 8);
        assert_eq!(value[9], format!("{:x}", Sha256::digest(b"original")));
        assert!(signature(&f.0, 7).is_err());
        fs::write(f.0.join("manifest.json"), b"").unwrap();
        assert!(signature(&f.0, 0).is_ok());
    }

    #[test]
    fn signature_rejects_file_and_directory_symlinks_and_hardlinks() {
        let f = Fixture::new();
        let link = f.0.join("linked-directory");
        symlink(&f.0, &link).unwrap();
        assert!(signature(&link, 100).is_err());
        let file = f.0.join("manifest.json");
        let original = f.0.join("original");
        fs::rename(&file, &original).unwrap();
        symlink(&original, &file).unwrap();
        assert!(signature(&f.0, 100).is_err());
        fs::remove_file(&file).unwrap();
        fs::hard_link(&original, &file).unwrap();
        assert!(signature(&f.0, 100).is_err());
    }

    #[test]
    fn signature_rejects_replacement_before_final_identity_check() {
        let f = Fixture::new();
        let result = signature_inner(&f.0, 100, || {
            fs::rename(f.0.join("manifest.json"), f.0.join("old")).unwrap();
            fs::write(f.0.join("manifest.json"), b"original").unwrap();
        });
        assert!(result.is_err());
    }

    #[test]
    fn signature_rejects_content_change_after_read() {
        let f = Fixture::new();
        // Two immediate writes can share a filesystem timestamp tick. Seed an
        // old mtime so this test deterministically exercises identity change;
        // fresh content hashing between calls is covered separately.
        File::open(f.0.join("manifest.json"))
            .unwrap()
            .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1))
            .unwrap();
        assert!(
            signature_inner(&f.0, 100, || {
                fs::write(f.0.join("manifest.json"), b"modified").unwrap();
            })
            .is_err()
        );
    }

    #[test]
    fn signature_rejects_growth_after_read() {
        let f = Fixture::new();
        assert!(
            signature_inner(&f.0, 100, || {
                fs::write(f.0.join("manifest.json"), b"original plus tail").unwrap();
            })
            .is_err()
        );
    }

    #[test]
    fn signature_rejects_permission_change_after_read() {
        let f = Fixture::new();
        assert!(
            signature_inner(&f.0, 100, || {
                fs::set_permissions(f.0.join("manifest.json"), fs::Permissions::from_mode(0o400))
                    .unwrap();
            })
            .is_err()
        );
    }
}
