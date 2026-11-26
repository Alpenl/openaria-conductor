//! ExFAT-only `MIRO-EGO` recording volume manager and storage reserve monitor.
//!
//! Strictly enforces that recordings, logs, and internal metadata live on the mounted
//! exFAT recording volume (`/data`) and **never** fall back to the 20 GiB ext4 system disk.

use crate::util::now_rfc3339_utc;
use crate::{StorageError, StorageResult, VOLUME_METADATA_SCHEMA, VolumeInfo, VolumeManager};
use miro_ego_core::ids::{is_valid_uuid_v4, new_uuid_v4};
use miro_ego_core::paths::{DataVolumePaths, RECORDING_VOLUME_LABEL};
use miro_ego_core::sys::fs::{
    fstat, fsync_directory, is_exfat_or_msdos, open_directory_safe, read_bounded_json,
    set_exfat_hidden, write_bytes_atomic,
};
use serde::{Deserialize, Serialize};
use std::ffi::CString;
use std::fs;
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Default minimum free space reserved on `/data` before refusing new captures (2 GiB).
pub const DEFAULT_MIN_AVAILABLE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Default minimum available allocation units / inodes on `/data`.
pub const DEFAULT_MIN_AVAILABLE_INODES: u64 = 1024;
/// Minimum free space required on the 20 GiB system disk (`/var/lib/miro-ego`) (256 MiB).
pub const SYSTEM_RESERVE_BYTES: u64 = 256 * 1024 * 1024;
/// Minimum free inodes required on the system disk.
pub const SYSTEM_RESERVE_INODES: u64 = 1024;
/// Maximum allowed size of `/data/.miro-ego/volume.json`.
pub const MAX_VOLUME_METADATA_BYTES: usize = 8 * 1024;

/// Persistent volume identity metadata written to `/data/.miro-ego/volume.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeMetadata {
    pub schema: String,
    pub volume_id: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initialized_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partuuid: Option<String>,
}

/// Storage budget entry matching OpenAPI `CaptureStorageBudget`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureStorageBudget {
    pub available_bytes: u64,
    pub required_bytes: u64,
}

/// Result of inspecting a mounted filesystem's capacity and writable flags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsCapacity {
    pub total_bytes: u64,
    pub available_bytes: u64,
    pub available_inodes: u64,
    pub read_only: bool,
    pub writable: bool,
    pub is_exfat: bool,
    pub device_id: u64,
    pub mount_identity: String,
    pub mounted: bool,
}

/// Admitted recording volume state for a single mount generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeAdmission {
    pub mountpoint: PathBuf,
    pub volume_id: String,
    pub generation_id: String,
    pub label: String,
    pub uuid: String,
    pub partuuid: String,
    pub device_id: u64,
    pub mount_identity: String,
    pub is_exfat: bool,
    pub total_bytes: u64,
    pub available_bytes: u64,
    pub available_inodes: u64,
    pub writable: bool,
}

/// Outcome of a storage reserve check on `/data` (and optional system disk check).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReserveStatus {
    pub ok: bool,
    pub code: String,
    pub message: String,
    pub recording: Option<CaptureStorageBudget>,
    pub system: Option<CaptureStorageBudget>,
    pub available_bytes: u64,
    pub required_bytes: u64,
    pub available_inodes: u64,
    pub required_inodes: u64,
    pub writable: bool,
}

/// Policy knobs for [`ExfatVolumeManager`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumePolicy {
    pub minimum_available_bytes: u64,
    pub minimum_available_inodes: u64,
    pub system_state_root: Option<PathBuf>,
    pub require_exfat: bool,
    pub require_mountpoint: bool,
    pub expected_label: String,
    pub expected_uuid: Option<String>,
    pub expected_partuuid: Option<String>,
}

impl VolumePolicy {
    /// Strict production policy for `/data` (`MIRO-EGO` exFAT mountpoint).
    pub fn production(minimum_available_bytes: u64, minimum_available_inodes: u64) -> Self {
        Self {
            minimum_available_bytes,
            minimum_available_inodes,
            system_state_root: Some(PathBuf::from(miro_ego_core::paths::SYSTEM_STATE_DIR)),
            require_exfat: true,
            require_mountpoint: true,
            expected_label: RECORDING_VOLUME_LABEL.to_owned(),
            expected_uuid: None,
            expected_partuuid: None,
        }
    }

    /// Hermetic policy for local directory testing (still enforces writability and reserves).
    pub fn relaxed_for_tests(minimum_available_bytes: u64) -> Self {
        Self {
            minimum_available_bytes,
            minimum_available_inodes: 1,
            system_state_root: None,
            require_exfat: false,
            require_mountpoint: false,
            expected_label: RECORDING_VOLUME_LABEL.to_owned(),
            expected_uuid: None,
            expected_partuuid: None,
        }
    }
}

impl Default for VolumePolicy {
    fn default() -> Self {
        Self::production(DEFAULT_MIN_AVAILABLE_BYTES, DEFAULT_MIN_AVAILABLE_INODES)
    }
}

/// Platform filesystem probe abstracted so unit tests can simulate exFAT, unmounted,
/// read-only (`errors=remount-ro`), or low-space conditions deterministically.
pub trait VolumeProbe: Send + Sync {
    fn probe_volume(&self, mountpoint: &Path) -> io::Result<FsCapacity>;
    fn probe_system(&self, state_root: &Path) -> io::Result<(u64, u64)>;
}

/// Real POSIX/Linux filesystem probe using `statvfs`, `fstatfs`, `/proc/self/mountinfo`,
/// and `access(W_OK)`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PosixVolumeProbe;

impl VolumeProbe for PosixVolumeProbe {
    fn probe_volume(&self, mountpoint: &Path) -> io::Result<FsCapacity> {
        let dir = open_directory_safe(mountpoint)?;
        let st = fstat(dir.as_raw_fd())?;
        let is_exfat = is_exfat_or_msdos(dir.as_raw_fd())?;
        let vfs = fstatvfs_fd(dir.as_raw_fd())?;
        #[allow(clippy::useless_conversion)]
        let frsize = u64::from(vfs.f_frsize);
        #[allow(clippy::useless_conversion)]
        let blocks = u64::from(vfs.f_blocks);
        #[allow(clippy::useless_conversion)]
        let bavail = u64::from(vfs.f_bavail);
        #[allow(clippy::useless_conversion)]
        let files = u64::from(vfs.f_files);
        #[allow(clippy::useless_conversion)]
        let favail = u64::from(vfs.f_favail);

        let total_bytes = frsize.saturating_mul(blocks);
        let available_bytes = frsize.saturating_mul(bavail);
        // exFAT has no fixed inode table and reports f_files == f_favail == 0;
        // use free allocation units as a conservative creation budget.
        let available_inodes = if files == 0 { bavail } else { favail };

        let read_only = (vfs.f_flag & libc::ST_RDONLY) != 0;
        let writable = !read_only && check_write_access(mountpoint);
        let (mounted, mount_identity) = inspect_mountinfo(mountpoint, st.st_dev, st.st_ino);

        Ok(FsCapacity {
            total_bytes,
            available_bytes,
            available_inodes,
            read_only,
            writable,
            is_exfat,
            device_id: st.st_dev,
            mount_identity,
            mounted,
        })
    }

    fn probe_system(&self, state_root: &Path) -> io::Result<(u64, u64)> {
        let dir = open_directory_safe(state_root)?;
        let vfs = fstatvfs_fd(dir.as_raw_fd())?;
        #[allow(clippy::useless_conversion)]
        let frsize = u64::from(vfs.f_frsize);
        #[allow(clippy::useless_conversion)]
        let bavail = u64::from(vfs.f_bavail);
        #[allow(clippy::useless_conversion)]
        let files = u64::from(vfs.f_files);
        #[allow(clippy::useless_conversion)]
        let favail = u64::from(vfs.f_favail);
        let available_inodes = if files == 0 { bavail } else { favail };
        Ok((frsize.saturating_mul(bavail), available_inodes))
    }
}

fn fstatvfs_fd(fd: libc::c_int) -> io::Result<libc::statvfs> {
    if fd < 0 {
        return Err(io::Error::from_raw_os_error(libc::EBADF));
    }
    loop {
        let mut buf = MaybeUninit::<libc::statvfs>::uninit();
        // SAFETY: `buf` points to writable `libc::statvfs` memory.
        if unsafe { libc::fstatvfs(fd, buf.as_mut_ptr()) } == 0 {
            // SAFETY: kernel initialized `buf` on 0 return.
            return Ok(unsafe { buf.assume_init() });
        }
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

fn check_write_access(path: &Path) -> bool {
    let Ok(c_path) = CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: `c_path` is a valid NUL-terminated path string.
    unsafe { libc::access(c_path.as_ptr(), libc::W_OK) == 0 }
}

fn inspect_mountinfo(mountpoint: &Path, st_dev: u64, st_ino: u64) -> (bool, String) {
    let canonical = fs::canonicalize(mountpoint).unwrap_or_else(|_| mountpoint.to_path_buf());
    let target = canonical
        .to_string_lossy()
        .replace(' ', "\\040")
        .replace('\t', "\\011");
    if let Ok(content) = fs::read_to_string("/proc/self/mountinfo") {
        let mut matched: Option<String> = None;
        for line in content.lines() {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() >= 6 && fields[4] == target {
                matched = Some(format!("mount:{}:{}", fields[0], fields[2]));
            }
        }
        if let Some(id) = matched {
            return (true, id);
        }
    }
    // Check parent st_dev difference as secondary mountpoint signal.
    let mounted = canonical
        .parent()
        .and_then(|p| fs::metadata(p).ok())
        .is_some_and(|parent_meta| parent_meta.dev() != st_dev);
    (mounted, format!("path:{st_dev}:{st_ino}"))
}

/// Concrete exFAT-only `MIRO-EGO` volume manager and reserve monitor.
#[derive(Clone)]
pub struct ExfatVolumeManager {
    paths: DataVolumePaths,
    policy: VolumePolicy,
    probe: Arc<dyn VolumeProbe>,
}

impl std::fmt::Debug for ExfatVolumeManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExfatVolumeManager")
            .field("mountpoint", &self.paths.mountpoint())
            .field("policy", &self.policy)
            .finish()
    }
}

impl ExfatVolumeManager {
    pub fn new(paths: DataVolumePaths, policy: VolumePolicy) -> Self {
        Self {
            paths,
            policy,
            probe: Arc::new(PosixVolumeProbe),
        }
    }

    pub fn with_probe(
        paths: DataVolumePaths,
        policy: VolumePolicy,
        probe: Arc<dyn VolumeProbe>,
    ) -> Self {
        Self {
            paths,
            policy,
            probe,
        }
    }

    pub fn policy(&self) -> &VolumePolicy {
        &self.policy
    }

    /// Fast mount & write-availability check used by audit/receipt fallback paths so
    /// unmounted `/data` NEVER falls back to writing on the root ext4 filesystem.
    pub fn is_mounted_and_writable(&self) -> bool {
        self.inspect_capacity_checked().is_ok()
    }

    /// Read and validate `/data/.miro-ego/volume.json`.
    pub fn read_metadata(&self) -> StorageResult<VolumeMetadata> {
        let meta_path = self.paths.volume_metadata_file();
        let meta: VolumeMetadata = read_bounded_json(&meta_path, MAX_VOLUME_METADATA_BYTES, 8)
            .map_err(|err| StorageError::VolumeInvalid {
                message: format!("volume metadata unreadable or invalid: {err}"),
            })?;
        validate_volume_metadata(&meta, &self.policy.expected_label)?;
        Ok(meta)
    }

    /// Ensure `/data/.miro-ego/volume.json` exists with a valid UUIDv4 `volume_id`,
    /// returning `(metadata, created_new)`.
    pub fn ensure_metadata(&self) -> StorageResult<(VolumeMetadata, bool)> {
        let meta_path = self.paths.volume_metadata_file();
        match fs::symlink_metadata(&meta_path) {
            Ok(meta) => {
                if !meta.file_type().is_file() {
                    return Err(StorageError::VolumeInvalid {
                        message: "volume metadata path must be a regular file".to_owned(),
                    });
                }
                let existing = self.read_metadata()?;
                Ok((existing, false))
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                let created = VolumeMetadata {
                    schema: VOLUME_METADATA_SCHEMA.to_owned(),
                    volume_id: new_uuid_v4(),
                    label: self.policy.expected_label.clone(),
                    initialized_at: Some(now_rfc3339_utc()),
                    uuid: self.policy.expected_uuid.clone(),
                    partuuid: self.policy.expected_partuuid.clone(),
                };
                let mut bytes = serde_json::to_vec_pretty(&created).map_err(|e| {
                    StorageError::VolumeInvalid {
                        message: format!("serialize volume metadata failed: {e}"),
                    }
                })?;
                bytes.push(b'\n');
                write_bytes_atomic(&meta_path, &bytes, 0o640, true).map_err(|e| {
                    StorageError::io("volume_metadata_write_failed", "write volume.json", e)
                })?;
                Ok((created, true))
            }
            Err(err) => Err(StorageError::io(
                "volume_metadata_stat_failed",
                "stat volume.json",
                err,
            )),
        }
    }

    /// Inspect raw capacity with fail-closed mount, exFAT, and read-only enforcement.
    pub fn inspect_capacity_checked(&self) -> StorageResult<FsCapacity> {
        let mountpoint = self.paths.mountpoint();
        let lstat = fs::symlink_metadata(mountpoint).map_err(|err| StorageError::Unavailable {
            message: format!("recording volume mountpoint unavailable: {err}"),
        })?;
        if lstat.file_type().is_symlink() || !lstat.is_dir() {
            return Err(StorageError::VolumeInvalid {
                message: "recording volume mountpoint must be a non-symlink directory".to_owned(),
            });
        }

        let cap = self
            .probe
            .probe_volume(mountpoint)
            .map_err(|err| StorageError::Unavailable {
                message: format!("failed to probe recording volume: {err}"),
            })?;

        if self.policy.require_mountpoint && !cap.mounted {
            return Err(StorageError::Unavailable {
                message: "recording volume is not mounted".to_owned(),
            });
        }
        if self.policy.require_exfat && !cap.is_exfat {
            return Err(StorageError::VolumeInvalid {
                message: "recording volume must be formatted as exFAT".to_owned(),
            });
        }
        if cap.read_only || !cap.writable {
            return Err(StorageError::Unavailable {
                message: "recording volume is read-only or not writable".to_owned(),
            });
        }
        Ok(cap)
    }

    /// Admit the recording volume, verifying mountpoint, exFAT policy, writability,
    /// `volume.json` identity, and same-device `recordings/` boundary.
    pub fn admit(
        &self,
        previous_binding: Option<(&str, u64, &str, &str)>,
    ) -> StorageResult<VolumeAdmission> {
        let cap = self.inspect_capacity_checked()?;
        let meta = self.read_metadata()?;

        // Verify `recordings/` does not escape the volume via symlink or cross-device mount.
        let recordings_dir = self.paths.recordings_dir();
        if let Ok(rec_meta) = fs::symlink_metadata(&recordings_dir) {
            if rec_meta.file_type().is_symlink() || !rec_meta.is_dir() {
                return Err(StorageError::VolumeInvalid {
                    message: "recordings directory must not be a symlink or file".to_owned(),
                });
            }
            if rec_meta.dev() != cap.device_id {
                return Err(StorageError::VolumeInvalid {
                    message: "recordings directory crosses filesystem boundary".to_owned(),
                });
            }
        }

        let generation_id = match previous_binding {
            Some((prev_vol_id, prev_dev, prev_mount_id, prev_gen))
                if prev_vol_id == meta.volume_id
                    && prev_dev == cap.device_id
                    && prev_mount_id == cap.mount_identity
                    && is_valid_uuid_v4(prev_gen) =>
            {
                prev_gen.to_owned()
            }
            _ => new_uuid_v4(),
        };

        Ok(VolumeAdmission {
            mountpoint: self.paths.mountpoint().to_path_buf(),
            volume_id: meta.volume_id,
            generation_id,
            label: meta.label,
            uuid: meta
                .uuid
                .or_else(|| self.policy.expected_uuid.clone())
                .unwrap_or_default(),
            partuuid: meta
                .partuuid
                .or_else(|| self.policy.expected_partuuid.clone())
                .unwrap_or_default(),
            device_id: cap.device_id,
            mount_identity: cap.mount_identity,
            is_exfat: cap.is_exfat,
            total_bytes: cap.total_bytes,
            available_bytes: cap.available_bytes,
            available_inodes: cap.available_inodes,
            writable: cap.writable,
        })
    }

    /// Check storage reserve thresholds on `/data` (and optional system disk).
    ///
    /// Never falls back to system disk when `/data` is unmounted, read-only, or full.
    pub fn check_reserve(&self) -> StorageResult<ReserveStatus> {
        let system_budget = self.probe_system_budget();

        let cap = match self.inspect_capacity_checked() {
            Ok(cap) => cap,
            Err(err) => {
                return Ok(ReserveStatus {
                    ok: false,
                    code: err.code().to_owned(),
                    message: err.to_string(),
                    recording: None,
                    system: system_budget.as_ref().ok().copied().flatten(),
                    available_bytes: 0,
                    required_bytes: self.policy.minimum_available_bytes,
                    available_inodes: 0,
                    required_inodes: self.policy.minimum_available_inodes,
                    writable: false,
                });
            }
        };

        let recording_budget = Some(CaptureStorageBudget {
            available_bytes: cap.available_bytes,
            required_bytes: self.policy.minimum_available_bytes,
        });

        let system = match system_budget {
            Ok(budget) => {
                if let Some((sys_avail, sys_inodes)) = self.probe_system_raw()? {
                    if sys_avail < SYSTEM_RESERVE_BYTES {
                        return Ok(ReserveStatus {
                            ok: false,
                            code: "insufficient_space".to_owned(),
                            message: "system state disk available space is below 256 MiB reserve"
                                .to_owned(),
                            recording: recording_budget,
                            system: budget,
                            available_bytes: cap.available_bytes,
                            required_bytes: self.policy.minimum_available_bytes,
                            available_inodes: cap.available_inodes,
                            required_inodes: self.policy.minimum_available_inodes,
                            writable: cap.writable,
                        });
                    }
                    if sys_inodes < SYSTEM_RESERVE_INODES {
                        return Ok(ReserveStatus {
                            ok: false,
                            code: "insufficient_inodes".to_owned(),
                            message: "system state disk available inodes are below reserve"
                                .to_owned(),
                            recording: recording_budget,
                            system: budget,
                            available_bytes: cap.available_bytes,
                            required_bytes: self.policy.minimum_available_bytes,
                            available_inodes: cap.available_inodes,
                            required_inodes: self.policy.minimum_available_inodes,
                            writable: cap.writable,
                        });
                    }
                }
                budget
            }
            Err(err) => {
                return Ok(ReserveStatus {
                    ok: false,
                    code: "storage_unavailable".to_owned(),
                    message: format!("unable to inspect system state disk: {err}"),
                    recording: recording_budget,
                    system: None,
                    available_bytes: cap.available_bytes,
                    required_bytes: self.policy.minimum_available_bytes,
                    available_inodes: cap.available_inodes,
                    required_inodes: self.policy.minimum_available_inodes,
                    writable: cap.writable,
                });
            }
        };

        if cap.available_bytes < self.policy.minimum_available_bytes {
            return Ok(ReserveStatus {
                ok: false,
                code: "insufficient_space".to_owned(),
                message: format!(
                    "recording volume available space ({} B) is below minimum reserve ({} B)",
                    cap.available_bytes, self.policy.minimum_available_bytes
                ),
                recording: recording_budget,
                system,
                available_bytes: cap.available_bytes,
                required_bytes: self.policy.minimum_available_bytes,
                available_inodes: cap.available_inodes,
                required_inodes: self.policy.minimum_available_inodes,
                writable: cap.writable,
            });
        }

        if cap.available_inodes < self.policy.minimum_available_inodes {
            return Ok(ReserveStatus {
                ok: false,
                code: "insufficient_inodes".to_owned(),
                message: format!(
                    "recording volume available allocation units ({}) below minimum reserve ({})",
                    cap.available_inodes, self.policy.minimum_available_inodes
                ),
                recording: recording_budget,
                system,
                available_bytes: cap.available_bytes,
                required_bytes: self.policy.minimum_available_bytes,
                available_inodes: cap.available_inodes,
                required_inodes: self.policy.minimum_available_inodes,
                writable: cap.writable,
            });
        }

        Ok(ReserveStatus {
            ok: true,
            code: "ready".to_owned(),
            message: "recording volume is ready".to_owned(),
            recording: recording_budget,
            system,
            available_bytes: cap.available_bytes,
            required_bytes: self.policy.minimum_available_bytes,
            available_inodes: cap.available_inodes,
            required_inodes: self.policy.minimum_available_inodes,
            writable: cap.writable,
        })
    }

    fn probe_system_raw(&self) -> StorageResult<Option<(u64, u64)>> {
        let Some(state_root) = &self.policy.system_state_root else {
            return Ok(None);
        };
        let stats =
            self.probe
                .probe_system(state_root)
                .map_err(|err| StorageError::Unavailable {
                    message: format!("failed to probe system state disk: {err}"),
                })?;
        Ok(Some(stats))
    }

    fn probe_system_budget(&self) -> Result<Option<CaptureStorageBudget>, io::Error> {
        let Some(state_root) = &self.policy.system_state_root else {
            return Ok(None);
        };
        let (avail, _) = self.probe.probe_system(state_root)?;
        Ok(Some(CaptureStorageBudget {
            available_bytes: avail,
            required_bytes: SYSTEM_RESERVE_BYTES,
        }))
    }
}

impl VolumeManager for ExfatVolumeManager {
    fn paths(&self) -> &DataVolumePaths {
        &self.paths
    }

    fn inspect_volume(&self) -> StorageResult<VolumeInfo> {
        let cap = self.inspect_capacity_checked()?;
        let meta = self.read_metadata().ok();
        let label = meta
            .as_ref()
            .map(|m| m.label.clone())
            .unwrap_or_else(|| self.policy.expected_label.clone());
        let uuid = meta
            .as_ref()
            .and_then(|m| m.uuid.clone())
            .or_else(|| self.policy.expected_uuid.clone())
            .or_else(|| meta.as_ref().map(|m| m.volume_id.clone()))
            .unwrap_or_default();
        let partuuid = meta
            .as_ref()
            .and_then(|m| m.partuuid.clone())
            .or_else(|| self.policy.expected_partuuid.clone())
            .unwrap_or_default();

        Ok(VolumeInfo {
            label,
            mountpoint: self.paths.mountpoint().to_path_buf(),
            uuid,
            partuuid,
            is_exfat: cap.is_exfat,
            total_bytes: cap.total_bytes,
            available_bytes: cap.available_bytes,
        })
    }

    fn ensure_layout(&self) -> StorageResult<bool> {
        let cap = self.inspect_capacity_checked()?;
        let dirs = [
            self.paths.recordings_dir(),
            self.paths.logs_dir(),
            self.paths.updates_dir(),
            self.paths.hidden_meta_dir(),
            self.paths.index_dir(),
            self.paths.staging_dir(),
            self.paths.quarantine_dir(),
            self.paths.audit_dir(),
            self.paths.receipts_dir(),
            self.paths.telemetry_dir(),
        ];

        for dir in &dirs {
            if let Ok(meta) = fs::symlink_metadata(dir) {
                if meta.file_type().is_symlink() || !meta.is_dir() {
                    return Err(StorageError::VolumeInvalid {
                        message: format!(
                            "canonical volume path is a symlink or non-directory: {}",
                            dir.display()
                        ),
                    });
                }
                if meta.dev() != cap.device_id {
                    return Err(StorageError::VolumeInvalid {
                        message: format!(
                            "canonical volume path crosses device boundary: {}",
                            dir.display()
                        ),
                    });
                }
            } else {
                fs::create_dir_all(dir).map_err(|err| {
                    StorageError::io(
                        "volume_layout_create_failed",
                        format!("create directory {}", dir.display()),
                        err,
                    )
                })?;
            }
        }

        let _ = self.ensure_metadata()?;
        let hidden_applied =
            set_exfat_hidden(&self.paths.hidden_meta_dir(), true).map_err(|e| {
                StorageError::io("exfat_hidden_failed", "apply ATTR_HIDDEN to .miro-ego", e)
            })?;
        let _ = fsync_directory(self.paths.mountpoint());
        Ok(hidden_applied)
    }
}

fn validate_volume_metadata(meta: &VolumeMetadata, expected_label: &str) -> StorageResult<()> {
    if meta.schema != VOLUME_METADATA_SCHEMA {
        return Err(StorageError::VolumeInvalid {
            message: format!("volume metadata schema must be {VOLUME_METADATA_SCHEMA}"),
        });
    }
    if !is_valid_uuid_v4(&meta.volume_id) {
        return Err(StorageError::VolumeInvalid {
            message: "volume metadata volume_id must be a canonical lowercase UUIDv4".to_owned(),
        });
    }
    if meta.label != expected_label {
        return Err(StorageError::VolumeInvalid {
            message: format!(
                "volume metadata label '{}' does not match expected '{expected_label}'",
                meta.label
            ),
        });
    }
    Ok(())
}
