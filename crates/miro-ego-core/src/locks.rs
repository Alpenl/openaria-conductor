//! Safe POSIX advisory `flock` guards for Miro-Ego maintenance and network locks.

use crate::error::{CoreError, CoreResult};
use crate::paths::{MAINTENANCE_LOCK_NAME, NETWORK_OPERATION_LOCK_NAME};
use crate::sys::fs::fstat;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

pub const DEFAULT_LOCK_MODE: u32 = 0o660;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Exclusive,
    Shared,
}

/// RAII POSIX `flock` guard. Unlocks and closes the file descriptor on drop.
#[derive(Debug)]
pub struct FlockGuard {
    file: File,
    path: PathBuf,
    mode: LockMode,
}

impl FlockGuard {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn mode(&self) -> LockMode {
        self.mode
    }
}

impl Drop for FlockGuard {
    fn drop(&mut self) {
        // SAFETY: `self.file` holds an open valid file descriptor until after drop.
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

/// Acquire a safe POSIX `flock` at `path` using `O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC`.
///
/// Rejects symlinks and non-regular files. When `nonblocking` is `true` and another
/// holder conflicts, returns `CoreError::LockBusy`.
pub fn acquire_flock(
    path: impl AsRef<Path>,
    mode: LockMode,
    nonblocking: bool,
) -> CoreResult<FlockGuard> {
    let target = path.as_ref();
    if let Some(parent) = target.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|err| CoreError::io("lock_dir_failed", "create lock directory", err))?;
        }
    }

    let common_flags = libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC;
    let file = match mode {
        LockMode::Shared => OpenOptions::new()
            .read(true)
            .custom_flags(common_flags)
            .open(target)
            .or_else(|err| {
                if err.kind() == io::ErrorKind::NotFound {
                    OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .mode(DEFAULT_LOCK_MODE)
                        .custom_flags(common_flags)
                        .open(target)
                } else {
                    Err(err)
                }
            }),
        LockMode::Exclusive => OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(DEFAULT_LOCK_MODE)
            .custom_flags(common_flags)
            .open(target),
    }
    .map_err(|err| CoreError::io("lock_open_failed", "open lock file", err))?;

    let stat = fstat(file.as_raw_fd())
        .map_err(|err| CoreError::io("lock_stat_failed", "fstat lock file", err))?;
    if (stat.st_mode & libc::S_IFMT) != libc::S_IFREG {
        return Err(CoreError::invalid_input(
            "lock_invalid",
            "lock path must be a regular file",
        ));
    }

    let mut operation = match mode {
        LockMode::Exclusive => libc::LOCK_EX,
        LockMode::Shared => libc::LOCK_SH,
    };
    if nonblocking {
        operation |= libc::LOCK_NB;
    }

    loop {
        // SAFETY: `file.as_raw_fd()` is a valid open regular file descriptor.
        if unsafe { libc::flock(file.as_raw_fd(), operation) } == 0 {
            return Ok(FlockGuard {
                file,
                path: target.to_path_buf(),
                mode,
            });
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        if error.kind() == io::ErrorKind::WouldBlock
            || error.raw_os_error() == Some(libc::EWOULDBLOCK)
        {
            return Err(CoreError::lock_busy(format!(
                "lock already held: {}",
                target.display()
            )));
        }
        return Err(CoreError::io("lock_failed", "acquire flock", error));
    }
}

/// Acquire `/run/miro-ego/maintenance.lock` (or under a custom `run_dir` for tests).
pub fn acquire_maintenance_lock(
    run_dir: impl AsRef<Path>,
    mode: LockMode,
    nonblocking: bool,
) -> CoreResult<FlockGuard> {
    acquire_flock(
        run_dir.as_ref().join(MAINTENANCE_LOCK_NAME),
        mode,
        nonblocking,
    )
}

/// Acquire `/run/miro-ego/network-operation.lock` (or under a custom `run_dir` for tests).
pub fn acquire_network_operation_lock(
    run_dir: impl AsRef<Path>,
    mode: LockMode,
    nonblocking: bool,
) -> CoreResult<FlockGuard> {
    acquire_flock(
        run_dir.as_ref().join(NETWORK_OPERATION_LOCK_NAME),
        mode,
        nonblocking,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_run_dir(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "miro-ego-lock-{name}-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        path
    }

    #[test]
    fn exclusive_and_shared_flocks_coordinate_and_release_on_drop() {
        let dir = temp_run_dir("coord");
        let guard = acquire_maintenance_lock(&dir, LockMode::Exclusive, true).unwrap();
        assert_eq!(guard.mode(), LockMode::Exclusive);

        let contended = acquire_maintenance_lock(&dir, LockMode::Exclusive, true).unwrap_err();
        assert_eq!(contended.code(), "lock_busy");

        let shared_contended = acquire_maintenance_lock(&dir, LockMode::Shared, true).unwrap_err();
        assert_eq!(shared_contended.code(), "lock_busy");

        drop(guard);

        let s1 = acquire_maintenance_lock(&dir, LockMode::Shared, true).unwrap();
        let s2 = acquire_maintenance_lock(&dir, LockMode::Shared, true).unwrap();
        let ex_contended = acquire_maintenance_lock(&dir, LockMode::Exclusive, true).unwrap_err();
        assert_eq!(ex_contended.code(), "lock_busy");
        drop(s1);
        drop(s2);

        let net = acquire_network_operation_lock(&dir, LockMode::Exclusive, true).unwrap();
        assert!(net.path().ends_with(NETWORK_OPERATION_LOCK_NAME));
        drop(net);

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rejects_symlink_lock_targets() {
        let dir = temp_run_dir("symlink");
        let real = dir.join("real.txt");
        let link = dir.join(MAINTENANCE_LOCK_NAME);
        fs::write(&real, b"").unwrap();
        symlink(&real, &link).unwrap();

        let err = acquire_maintenance_lock(&dir, LockMode::Exclusive, true).unwrap_err();
        assert_eq!(err.code(), "lock_open_failed");
        fs::remove_dir_all(dir).unwrap();
    }
}
