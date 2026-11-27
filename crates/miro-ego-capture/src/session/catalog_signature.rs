//! Fresh, bounded manifest signatures without per-file Python I/O bookkeeping.
use miro_ego_core::sys::fs as sys_fs;
use sha2::{Digest, Sha256};
use std::ffi::CStr;
use std::fs::OpenOptions;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

const NAME: &CStr = c"manifest.json";
type Identity = (
    libc::dev_t,
    libc::ino_t,
    libc::mode_t,
    libc::nlink_t,
    libc::off_t,
    i128,
    i128,
);

fn identity(value: &libc::stat, ignore_ino: bool) -> Identity {
    (
        value.st_dev,
        if ignore_ino { 0 } else { value.st_ino },
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

pub fn signature(path: &Path, maximum_bytes: usize) -> io::Result<String> {
    signature_inner(path, maximum_bytes, None, || {})
}

fn signature_inner(
    path: &Path,
    maximum_bytes: usize,
    force_exfat: Option<bool>,
    after_read: impl FnOnce(),
) -> io::Result<String> {
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| {
            if matches!(error.raw_os_error(), Some(libc::ELOOP | libc::ENOTDIR)) {
                invalid("catalog directory must not be a symbolic link")
            } else {
                error
            }
        })?;
    let opened = sys_fs::fstat(directory.as_raw_fd())?;
    if (opened.st_mode & libc::S_IFMT) != libc::S_IFDIR {
        return Err(invalid("catalog directory must not be a symbolic link"));
    }
    let exfat = match force_exfat {
        Some(value) => value,
        None => sys_fs::is_exfat_or_msdos(directory.as_raw_fd())?,
    };
    let mut file = sys_fs::openat_retry(
        directory.as_raw_fd(),
        NAME,
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
    )?;
    let before = sys_fs::fstat(file.as_raw_fd())?;
    if before.st_mode & libc::S_IFMT != libc::S_IFREG
        || before.st_nlink != 1
        || before.st_size < 0
        || before.st_size as u64 > maximum_bytes as u64
    {
        return Err(invalid(
            "catalog manifest must be a bounded exclusively linked file",
        ));
    }
    let expected = identity(&before, exfat);
    let mut remaining = before.st_size as u64;
    let mut buffer = vec![0_u8; remaining.clamp(1, 1024 * 1024) as usize];
    let mut digest = Sha256::new();
    while remaining > 0 {
        let count = remaining.min(buffer.len() as u64) as usize;
        let read = sys_fs::read_retry(&mut file, &mut buffer[..count])?;
        if read == 0 {
            return Err(invalid("catalog manifest truncated during read"));
        }
        digest.update(&buffer[..read]);
        remaining -= read as u64;
    }
    after_read();
    if sys_fs::read_retry(&mut file, &mut [0_u8; 1])? != 0
        || identity(&sys_fs::fstat(file.as_raw_fd())?, exfat) != expected
    {
        return Err(invalid("catalog manifest changed during read"));
    }
    let current = sys_fs::fstatat_nofollow(directory.as_raw_fd(), NAME)?;
    if identity(&current, exfat) != expected {
        return Err(invalid("catalog manifest replaced during read"));
    }
    // On exFAT/FAT, kernel st_ino is an ephemeral dcache counter that changes across
    // cache evictions and reboots, so omit volatile inodes from persisted signatures.
    let (reported_parent_ino, reported_file_ino) = if exfat {
        (0, 0)
    } else {
        (opened.st_ino, expected.1)
    };
    // Preserve the existing json.dumps signature byte-for-byte, including spaces.
    Ok(format!(
        "[{}, {}, {}, {}, {}, {}, {}, {}, {}, \"{:x}\"]",
        opened.st_dev,
        reported_parent_ino,
        expected.0,
        reported_file_ino,
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
    use std::fs::{self, File};
    use std::os::unix::fs::{PermissionsExt, symlink};

    struct Fixture(std::path::PathBuf);
    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "miro-ego-catalog-signature-{}-{}",
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
    fn exfat_mode_omits_volatile_parent_and_file_inodes() {
        let f = Fixture::new();
        let exfat_sig = signature_inner(&f.0, 100, Some(true), || {}).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&exfat_sig).unwrap();
        assert_eq!(parsed[1], 0);
        assert_eq!(parsed[3], 0);
        assert_eq!(parsed[6], 8);
        assert_eq!(parsed[9], format!("{:x}", Sha256::digest(b"original")));

        // Replacing the file with identical content/timestamps on exFAT yields the same signature.
        let mtime = fs::metadata(f.0.join("manifest.json"))
            .unwrap()
            .modified()
            .unwrap();
        fs::remove_file(f.0.join("manifest.json")).unwrap();
        fs::write(f.0.join("manifest.json"), b"original").unwrap();
        File::open(f.0.join("manifest.json"))
            .unwrap()
            .set_modified(mtime)
            .unwrap();
        let exfat_sig_recreated = signature_inner(&f.0, 100, Some(true), || {}).unwrap();
        let parsed_recreated: serde_json::Value =
            serde_json::from_str(&exfat_sig_recreated).unwrap();
        assert_eq!(parsed_recreated[1], 0);
        assert_eq!(parsed_recreated[3], 0);
        assert_eq!(parsed_recreated[9], parsed[9]);
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
        let result = signature_inner(&f.0, 100, None, || {
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
            signature_inner(&f.0, 100, None, || {
                fs::write(f.0.join("manifest.json"), b"modified").unwrap();
            })
            .is_err()
        );
    }

    #[test]
    fn signature_rejects_growth_after_read() {
        let f = Fixture::new();
        assert!(
            signature_inner(&f.0, 100, None, || {
                fs::write(f.0.join("manifest.json"), b"original plus tail").unwrap();
            })
            .is_err()
        );
    }

    #[test]
    fn signature_rejects_permission_change_after_read() {
        let f = Fixture::new();
        assert!(
            signature_inner(&f.0, 100, None, || {
                fs::set_permissions(f.0.join("manifest.json"), fs::Permissions::from_mode(0o400))
                    .unwrap();
            })
            .is_err()
        );
    }
}
