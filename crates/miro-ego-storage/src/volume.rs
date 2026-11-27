//! ExFAT-only `MIRO-EGO` recording volume manager and storage reserve monitor.
//!
//! Strictly enforces that recordings, logs, and internal metadata live on the mounted
//! exFAT recording volume (`/data`) and **never** fall back to the 20 GiB ext4 system disk.

use crate::util::{VolumeHandleLease, now_rfc3339_utc, sweep_stale_atomic_temps};
use crate::{StorageError, StorageResult, VOLUME_METADATA_SCHEMA, VolumeInfo, VolumeManager};
use miro_ego_core::ids::{is_valid_uuid_v4, new_uuid_v4};
use miro_ego_core::paths::{DataVolumePaths, RECORDING_VOLUME_LABEL};
use miro_ego_core::sys::fs::{
    EXFAT_SUPER_MAGIC, fstat, fsync_directory, open_directory_safe, read_bounded_json,
    set_exfat_hidden, write_bytes_atomic,
};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Linux `<linux/fs.h>` `FS_IOC_GETFSLABEL = _IOR(0x94, 49, char[256])`.
const FS_IOC_GETFSLABEL: libc::Ioctl = 0x8100_9431_u32 as libc::Ioctl;
const MOUNTINFO_CACHE_TTL: Duration = Duration::from_millis(500);

/// Kernel / block-layer physical filesystem identity observed independently of `volume.json`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PhysicalVolumeIdentity {
    pub label: Option<String>,
    pub uuid: Option<String>,
    pub partuuid: Option<String>,
}

pub const DATA_VOLUME_UUID_ENV: &str = "MIRO_EGO_DATA_VOLUME_UUID";
pub const DATA_VOLUME_PARTUUID_ENV: &str = "MIRO_EGO_DATA_VOLUME_PARTUUID";

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

    /// Production policy populated with `MIRO_EGO_DATA_VOLUME_UUID` and
    /// `MIRO_EGO_DATA_VOLUME_PARTUUID` environment overrides when set.
    pub fn from_env_production() -> Self {
        Self::production(DEFAULT_MIN_AVAILABLE_BYTES, DEFAULT_MIN_AVAILABLE_INODES)
            .with_env_volume_identity()
    }

    /// Populate `expected_uuid` / `expected_partuuid` from `MIRO_EGO_DATA_VOLUME_{UUID,PARTUUID}`.
    pub fn with_env_volume_identity(mut self) -> Self {
        let env_uuid = std::env::var(DATA_VOLUME_UUID_ENV).ok();
        let env_partuuid = std::env::var(DATA_VOLUME_PARTUUID_ENV).ok();
        self.apply_volume_identity_overrides(env_uuid.as_deref(), env_partuuid.as_deref());
        self
    }

    /// Apply optional trimmed `uuid` and `partuuid` identity expectations.
    pub fn apply_volume_identity_overrides(&mut self, uuid: Option<&str>, partuuid: Option<&str>) {
        if let Some(u) = uuid.map(str::trim).filter(|s| !s.is_empty()) {
            self.expected_uuid = Some(u.to_owned());
        }
        if let Some(p) = partuuid.map(str::trim).filter(|s| !s.is_empty()) {
            self.expected_partuuid = Some(p.to_owned());
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
    fn probe_physical_identity(
        &self,
        _mountpoint: &Path,
        _device_id: u64,
    ) -> PhysicalVolumeIdentity {
        PhysicalVolumeIdentity::default()
    }
}

/// Real POSIX/Linux filesystem probe using `statvfs`, `fstatfs`, `/proc/self/mountinfo`,
/// and `access(W_OK)`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PosixVolumeProbe;

impl VolumeProbe for PosixVolumeProbe {
    fn probe_volume(&self, mountpoint: &Path) -> io::Result<FsCapacity> {
        let dir = open_directory_safe(mountpoint)?;
        let st = fstat(dir.as_raw_fd())?;
        let is_exfat = is_exfat_strict(dir.as_raw_fd())?;
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
        let writable = !read_only && check_dir_fd_writable(dir.as_raw_fd());
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

    fn probe_physical_identity(&self, mountpoint: &Path, device_id: u64) -> PhysicalVolumeIdentity {
        probe_linux_physical_identity(mountpoint, device_id)
    }
}

fn probe_fs_label_ioctl(mountpoint: &Path) -> Option<String> {
    let dir = open_directory_safe(mountpoint).ok()?;
    let mut raw = [0_u8; 256];
    // SAFETY: `raw` is a writable 256-byte stack buffer matching `FS_IOC_GETFSLABEL`.
    let rc = unsafe { libc::ioctl(dir.as_raw_fd(), FS_IOC_GETFSLABEL, raw.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let nul_pos = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
    let text = std::str::from_utf8(&raw[..nul_pos]).ok()?.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_owned())
    }
}

fn decode_udev_hex_escapes(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if i + 3 < bytes.len()
            && bytes[i] == b'\\'
            && bytes[i + 1] == b'x'
            && bytes[i + 2].is_ascii_hexdigit()
            && bytes[i + 3].is_ascii_hexdigit()
            && let Ok(hex_str) = std::str::from_utf8(&bytes[i + 2..i + 4])
            && let Ok(val) = u8::from_str_radix(hex_str, 16)
        {
            out.push(val);
            i += 4;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn scan_dev_disk_by_dir(by_dir: &Path, device_id: u64) -> Option<String> {
    let entries = fs::read_dir(by_dir).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        // Follow symlink to block node and compare `st_rdev` with the mounted filesystem's `st_dev`.
        if let Ok(meta) = fs::metadata(&path)
            && meta.rdev() == device_id
            && let Some(name) = entry.file_name().to_str()
        {
            let decoded = decode_udev_hex_escapes(name);
            if !decoded.trim().is_empty() {
                return Some(decoded);
            }
        }
    }
    None
}

fn probe_linux_physical_identity(mountpoint: &Path, device_id: u64) -> PhysicalVolumeIdentity {
    let mut out = PhysicalVolumeIdentity {
        label: probe_fs_label_ioctl(mountpoint),
        uuid: None,
        partuuid: None,
    };
    let maj = linux_dev_major(device_id);
    let min = linux_dev_minor(device_id);
    if maj == 0 && min == 0 {
        return out;
    }

    let udev_path = PathBuf::from(format!("/run/udev/data/b{maj}:{min}"));
    if let Ok(text) = fs::read_to_string(udev_path) {
        for line in text.lines() {
            if let Some(val) = line.strip_prefix("E:ID_FS_LABEL=") {
                if out.label.is_none() && !val.trim().is_empty() {
                    out.label = Some(val.trim().to_owned());
                }
            } else if let Some(val) = line.strip_prefix("E:ID_FS_UUID=") {
                if !val.trim().is_empty() {
                    out.uuid = Some(val.trim().to_owned());
                }
            } else if let Some(val) = line.strip_prefix("E:ID_PART_ENTRY_UUID=")
                && !val.trim().is_empty()
            {
                out.partuuid = Some(val.trim().to_owned());
            }
        }
    }

    if out.label.is_none() {
        out.label = scan_dev_disk_by_dir(Path::new("/dev/disk/by-label"), device_id);
    }
    if out.uuid.is_none() {
        out.uuid = scan_dev_disk_by_dir(Path::new("/dev/disk/by-uuid"), device_id);
    }
    if out.partuuid.is_none() {
        out.partuuid = scan_dev_disk_by_dir(Path::new("/dev/disk/by-partuuid"), device_id);
    }
    out
}

fn is_exfat_strict(fd: libc::c_int) -> io::Result<bool> {
    if fd < 0 {
        return Err(io::Error::from_raw_os_error(libc::EBADF));
    }
    loop {
        let mut buf = MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: `buf` points to writable `libc::statfs` memory.
        if unsafe { libc::fstatfs(fd, buf.as_mut_ptr()) } == 0 {
            // SAFETY: kernel initialized `buf` on 0 return.
            let st = unsafe { buf.assume_init() };
            #[allow(clippy::useless_conversion)]
            let fs_type = i64::from(st.f_type);
            return Ok(fs_type == EXFAT_SUPER_MAGIC);
        }
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
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

fn check_dir_fd_writable(dir_fd: libc::c_int) -> bool {
    if dir_fd < 0 {
        return false;
    }
    let dot = c".";
    loop {
        // SAFETY: `dir_fd` is a valid open directory fd and `dot` is NUL-terminated.
        let rc = unsafe {
            libc::faccessat(
                dir_fd,
                dot.as_ptr(),
                libc::W_OK,
                libc::AT_EACCESS | libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if rc == 0 {
            return true;
        }
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        if err.raw_os_error() == Some(libc::EINVAL) {
            // Fallback for kernels/libcs rejecting AT_EACCESS | AT_SYMLINK_NOFOLLOW on `.`.
            loop {
                // SAFETY: `dir_fd` is a valid open directory fd and `dot` is NUL-terminated.
                let rc2 = unsafe { libc::faccessat(dir_fd, dot.as_ptr(), libc::W_OK, 0) };
                if rc2 == 0 {
                    return true;
                }
                let err2 = io::Error::last_os_error();
                if err2.kind() != io::ErrorKind::Interrupted {
                    return false;
                }
            }
        }
        return false;
    }
}

pub(crate) fn linux_dev_major(dev: u64) -> u32 {
    (((dev >> 8) & 0xfff) as u32) | (((dev >> 32) as u32) & !0xfff)
}

pub(crate) fn linux_dev_minor(dev: u64) -> u32 {
    ((dev & 0xff) as u32) | (((dev >> 12) as u32) & !0xff)
}

pub(crate) fn escape_mountinfo_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for ch in path.chars() {
        match ch {
            '\\' => out.push_str("\\134"),
            ' ' => out.push_str("\\040"),
            '\t' => out.push_str("\\011"),
            '\n' => out.push_str("\\012"),
            other => out.push(other),
        }
    }
    out
}

pub(crate) fn parse_mountinfo_for_target(
    mountinfo_text: &str,
    mountpoint_escaped: &str,
    st_dev: u64,
) -> Option<(bool, String)> {
    let expected_maj_min = format!("{}:{}", linux_dev_major(st_dev), linux_dev_minor(st_dev));
    let mut last_for_target: Option<(&str, &str)> = None;
    for line in mountinfo_text.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() >= 6 && fields[4] == mountpoint_escaped {
            last_for_target = Some((fields[0], fields[2]));
        }
    }
    if let Some((mount_id, maj_min)) = last_for_target {
        if maj_min == expected_maj_min {
            return Some((true, format!("mount:{mount_id}:{maj_min}")));
        }
        return Some((
            false,
            format!("mismatch:{mount_id}:{maj_min}:{expected_maj_min}"),
        ));
    }
    None
}

type MountinfoCacheKey = (PathBuf, u64, u64);
type MountinfoCacheEntry = (MountinfoCacheKey, Instant, (bool, String));

fn mountinfo_cache() -> &'static Mutex<Option<MountinfoCacheEntry>> {
    static CACHE: OnceLock<Mutex<Option<MountinfoCacheEntry>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}

fn inspect_mountinfo(mountpoint: &Path, st_dev: u64, st_ino: u64) -> (bool, String) {
    let key = (mountpoint.to_path_buf(), st_dev, st_ino);
    if let Ok(guard) = mountinfo_cache().lock()
        && let Some((ref cached_key, cached_at, ref cached_val)) = *guard
        && cached_key == &key
        && cached_at.elapsed() < MOUNTINFO_CACHE_TTL
    {
        return cached_val.clone();
    }

    let canonical = fs::canonicalize(mountpoint).unwrap_or_else(|_| mountpoint.to_path_buf());
    let target = escape_mountinfo_path(&canonical.to_string_lossy());
    let computed = if let Ok(content) = fs::read_to_string("/proc/self/mountinfo")
        && let Some(found) = parse_mountinfo_for_target(&content, &target, st_dev)
    {
        found
    } else {
        // Check parent st_dev difference as secondary mountpoint signal when not listed as mount target.
        let mounted = canonical
            .parent()
            .and_then(|p| fs::symlink_metadata(p).ok())
            .is_some_and(|parent_meta| parent_meta.dev() != st_dev);
        (mounted, format!("path:{st_dev}:{st_ino}"))
    };

    if let Ok(mut guard) = mountinfo_cache().lock() {
        *guard = Some((key, Instant::now(), computed.clone()));
    }
    computed
}

/// Concrete exFAT-only `MIRO-EGO` volume manager and reserve monitor.
#[derive(Clone)]
pub struct ExfatVolumeManager {
    paths: DataVolumePaths,
    policy: VolumePolicy,
    probe: Arc<dyn VolumeProbe>,
    released: Arc<AtomicBool>,
    open_handles: Arc<AtomicUsize>,
}

impl std::fmt::Debug for ExfatVolumeManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExfatVolumeManager")
            .field("mountpoint", &self.paths.mountpoint())
            .field("policy", &self.policy)
            .field("released", &self.is_volume_released())
            .field("open_handles", &self.open_handle_count())
            .finish()
    }
}

impl ExfatVolumeManager {
    pub fn new(paths: DataVolumePaths, policy: VolumePolicy) -> Self {
        Self::with_probe(paths, policy, Arc::new(PosixVolumeProbe))
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
            released: Arc::new(AtomicBool::new(false)),
            open_handles: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn policy(&self) -> &VolumePolicy {
        &self.policy
    }

    /// Whether the volume has been logically released for `SafeSwap` handoff.
    pub fn is_volume_released(&self) -> bool {
        self.released.load(Ordering::SeqCst)
    }

    /// Mark or clear the `SafeSwap` volume-released state across all handles to this manager.
    pub fn set_volume_released(&self, released: bool) {
        self.released.store(released, Ordering::SeqCst);
    }

    /// Shared atomic flag tracking whether the volume is logically released for `SafeSwap`.
    pub fn released_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.released)
    }

    /// Shared atomic counter tracking open artifact/log/audit handles on `/data`.
    pub fn handle_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.open_handles)
    }

    /// Number of currently tracked open handles on `/data`.
    pub fn open_handle_count(&self) -> usize {
        self.open_handles.load(Ordering::SeqCst)
    }

    /// Acquire an RAII lease incrementing [`Self::open_handle_count`] until dropped.
    pub fn acquire_handle_lease(&self) -> VolumeHandleLease {
        VolumeHandleLease::acquire(Arc::clone(&self.open_handles))
    }

    /// Atomically acquire a [`VolumeHandleLease`] only if the volume is not released for `SafeSwap`,
    /// using a `SeqCst` increment-then-check barrier paired with [`Self::set_volume_released`] +
    /// [`Self::open_handle_count`].
    pub fn try_acquire_handle_lease(&self) -> Option<VolumeHandleLease> {
        VolumeHandleLease::try_acquire(Arc::clone(&self.open_handles), &self.released)
    }

    /// Fast mount & write-availability check used by audit/receipt/log paths so:
    /// 1) unmounted `/data` NEVER falls back to writing on the root ext4 filesystem, and
    /// 2) a `SafeSwap`-released volume (`is_volume_released() == true`) reports `false`
    ///    so zero new handles or writes touch `/data` after release.
    pub fn is_mounted_and_writable(&self) -> bool {
        !self.is_volume_released() && self.inspect_capacity_checked().is_ok()
    }

    /// Raw physical mount & writability check ignoring [`Self::is_volume_released`],
    /// used by supervisor polling to detect card swap or remount after `SafeSwap`.
    pub fn is_raw_mounted_and_writable(&self) -> bool {
        self.inspect_capacity_checked().is_ok()
    }

    /// Probe raw volume capacity/mount identity (ignoring `released` and policy writability errors)
    /// so supervisor loops can observe unmount -> remount transitions.
    pub fn probe_raw_volume(&self) -> Option<FsCapacity> {
        let mountpoint = self.paths.mountpoint();
        let lstat = fs::symlink_metadata(mountpoint).ok()?;
        if lstat.file_type().is_symlink() || !lstat.is_dir() {
            return None;
        }
        self.probe.probe_volume(mountpoint).ok()
    }

    /// Read and validate `/data/.miro-ego/volume.json`.
    pub fn read_metadata(&self) -> StorageResult<VolumeMetadata> {
        let meta_path = self.paths.volume_metadata_file();
        let meta: VolumeMetadata = read_bounded_json(&meta_path, MAX_VOLUME_METADATA_BYTES, 8)
            .map_err(|err| StorageError::VolumeInvalid {
                message: format!("volume metadata unreadable or invalid: {err}"),
            })?;
        validate_volume_metadata(&meta, &self.policy)?;
        Ok(meta)
    }

    fn verify_physical_identity(&self, device_id: u64) -> StorageResult<PhysicalVolumeIdentity> {
        let phys = self
            .probe
            .probe_physical_identity(self.paths.mountpoint(), device_id);
        if let Some(ref obs_label) = phys.label
            && obs_label != &self.policy.expected_label
        {
            return Err(StorageError::VolumeInvalid {
                message: format!(
                    "physical filesystem label '{obs_label}' does not match expected '{}'",
                    self.policy.expected_label
                ),
            });
        }
        if let (Some(exp_uuid), Some(obs_uuid)) = (&self.policy.expected_uuid, &phys.uuid)
            && !obs_uuid.eq_ignore_ascii_case(exp_uuid)
        {
            return Err(StorageError::VolumeInvalid {
                message: format!(
                    "physical filesystem UUID '{obs_uuid}' does not match expected '{exp_uuid}'"
                ),
            });
        }
        if let (Some(exp_partuuid), Some(obs_partuuid)) =
            (&self.policy.expected_partuuid, &phys.partuuid)
            && !obs_partuuid.eq_ignore_ascii_case(exp_partuuid)
        {
            return Err(StorageError::VolumeInvalid {
                message: format!(
                    "physical partition PARTUUID '{obs_partuuid}' does not match expected '{exp_partuuid}'"
                ),
            });
        }
        Ok(phys)
    }

    /// Ensure `/data/.miro-ego/volume.json` exists with a valid UUIDv4 `volume_id`,
    /// returning `(metadata, created_new)`.
    pub fn ensure_metadata(&self) -> StorageResult<(VolumeMetadata, bool)> {
        let cap = self.inspect_capacity_checked()?;
        let phys = self.verify_physical_identity(cap.device_id)?;
        let meta_path = self.paths.volume_metadata_file();
        match fs::symlink_metadata(&meta_path) {
            Ok(meta) => {
                if !meta.file_type().is_file() {
                    return Err(StorageError::VolumeInvalid {
                        message: "volume metadata path must be a regular file".to_owned(),
                    });
                }
                let mut existing: VolumeMetadata =
                    read_bounded_json(&meta_path, MAX_VOLUME_METADATA_BYTES, 8).map_err(|err| {
                        StorageError::VolumeInvalid {
                            message: format!("volume metadata unreadable or invalid: {err}"),
                        }
                    })?;
                validate_volume_metadata_base(&existing, &self.policy.expected_label)?;
                let mut backfilled = false;
                let fill_uuid = phys.uuid.or_else(|| self.policy.expected_uuid.clone());
                if existing.uuid.is_none() && fill_uuid.is_some() {
                    existing.uuid = fill_uuid;
                    backfilled = true;
                }
                let fill_partuuid = phys
                    .partuuid
                    .or_else(|| self.policy.expected_partuuid.clone());
                if existing.partuuid.is_none() && fill_partuuid.is_some() {
                    existing.partuuid = fill_partuuid;
                    backfilled = true;
                }
                validate_volume_metadata(&existing, &self.policy)?;
                if backfilled {
                    let mut bytes = serde_json::to_vec_pretty(&existing).map_err(|e| {
                        StorageError::VolumeInvalid {
                            message: format!("serialize volume metadata failed: {e}"),
                        }
                    })?;
                    bytes.push(b'\n');
                    write_bytes_atomic(&meta_path, &bytes, 0o640, true).map_err(|e| {
                        StorageError::io("volume_metadata_write_failed", "write volume.json", e)
                    })?;
                }
                Ok((existing, false))
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                let created = VolumeMetadata {
                    schema: VOLUME_METADATA_SCHEMA.to_owned(),
                    volume_id: new_uuid_v4(),
                    label: self.policy.expected_label.clone(),
                    initialized_at: Some(now_rfc3339_utc()),
                    uuid: phys.uuid.or_else(|| self.policy.expected_uuid.clone()),
                    partuuid: phys
                        .partuuid
                        .or_else(|| self.policy.expected_partuuid.clone()),
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
    /// `volume.json` identity, and same-device non-symlink canonical subdirectories.
    pub fn admit(
        &self,
        previous_binding: Option<(&str, u64, &str, &str)>,
    ) -> StorageResult<VolumeAdmission> {
        let cap = self.inspect_capacity_checked()?;
        let phys = self.verify_physical_identity(cap.device_id)?;

        let meta_path = self.paths.volume_metadata_file();
        let meta_stat =
            fs::symlink_metadata(&meta_path).map_err(|err| StorageError::VolumeInvalid {
                message: format!("volume metadata unreadable or missing: {err}"),
            })?;
        if meta_stat.file_type().is_symlink() || !meta_stat.is_file() {
            return Err(StorageError::VolumeInvalid {
                message: "volume metadata must be a non-symlink regular file".to_owned(),
            });
        }
        if meta_stat.dev() != cap.device_id {
            return Err(StorageError::VolumeInvalid {
                message: "volume metadata crosses filesystem boundary".to_owned(),
            });
        }
        let meta = self.read_metadata()?;

        // Verify all canonical subdirectories (`recordings/`, `logs/`, `updates/`, `.miro-ego/*`)
        // do not escape the recording volume via symlinks or cross-device mounts.
        let canonical_dirs = [
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
        for dir in &canonical_dirs {
            match fs::symlink_metadata(dir) {
                Ok(dir_meta) => {
                    if dir_meta.file_type().is_symlink() || !dir_meta.is_dir() {
                        return Err(StorageError::VolumeInvalid {
                            message: format!(
                                "canonical volume directory must not be a symlink or file: {}",
                                dir.display()
                            ),
                        });
                    }
                    if dir_meta.dev() != cap.device_id {
                        return Err(StorageError::VolumeInvalid {
                            message: format!(
                                "canonical volume directory crosses filesystem boundary: {}",
                                dir.display()
                            ),
                        });
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(StorageError::io(
                        "volume_dir_stat_failed",
                        format!("stat canonical volume directory {}", dir.display()),
                        err,
                    ));
                }
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
                .or(phys.uuid)
                .or_else(|| self.policy.expected_uuid.clone())
                .unwrap_or_default(),
            partuuid: meta
                .partuuid
                .or(phys.partuuid)
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

    fn is_volume_released(&self) -> bool {
        ExfatVolumeManager::is_volume_released(self)
    }

    fn set_volume_released(&self, released: bool) {
        ExfatVolumeManager::set_volume_released(self, released);
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
        let _ = self.verify_physical_identity(cap.device_id)?;
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

        for sweep_dir in [
            self.paths.hidden_meta_dir(),
            self.paths.index_dir(),
            self.paths.receipts_dir(),
        ] {
            let _ = sweep_stale_atomic_temps(&sweep_dir);
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

fn validate_volume_metadata_base(meta: &VolumeMetadata, expected_label: &str) -> StorageResult<()> {
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

fn validate_volume_metadata(meta: &VolumeMetadata, policy: &VolumePolicy) -> StorageResult<()> {
    validate_volume_metadata_base(meta, &policy.expected_label)?;
    if let Some(expected_uuid) = &policy.expected_uuid
        && !meta
            .uuid
            .as_deref()
            .is_some_and(|u| u.eq_ignore_ascii_case(expected_uuid))
    {
        return Err(StorageError::VolumeInvalid {
            message: format!(
                "volume metadata uuid {:?} does not match expected '{expected_uuid}'",
                meta.uuid
            ),
        });
    }
    if let Some(expected_partuuid) = &policy.expected_partuuid
        && !meta
            .partuuid
            .as_deref()
            .is_some_and(|p| p.eq_ignore_ascii_case(expected_partuuid))
    {
        return Err(StorageError::VolumeInvalid {
            message: format!(
                "volume metadata partuuid {:?} does not match expected '{expected_partuuid}'",
                meta.partuuid
            ),
        });
    }
    Ok(())
}
