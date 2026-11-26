//! Canonical Miro-Ego system disk and exFAT recording disk path layout.
//!
//! System disk (20 GiB ext4) holds only fixed-size binaries and small configuration
//! files under `/opt/miro-ego`, `/etc/miro-ego`, `/var/lib/miro-ego`, and `/run/miro-ego`.
//!
//! Recording disk (exFAT p3, label `MIRO-EGO`, mounted at `/data`) holds:
//! - User-visible directories: `recordings/`, `logs/`, `updates/`
//! - Native-hidden system directory (`ATTR_HIDDEN`): `.miro-ego/` containing
//!   `volume.json`, `index/catalog.bin`, `staging/`, `quarantine/`, `audit/`,
//!   `receipts/`, and `telemetry/`.

use crate::sys::fs::set_exfat_hidden;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub const SYSTEM_INSTALL_ROOT: &str = "/opt/miro-ego";
pub const SYSTEM_CURRENT_BIN: &str = "/opt/miro-ego/current/bin/miro-ego";
pub const SYSTEM_ENCODER_BIN: &str = "/opt/miro-ego/current/bin/miro-ego-encoder";
pub const SYSTEM_CONFIG_DIR: &str = "/etc/miro-ego";
pub const SYSTEM_STATE_DIR: &str = "/var/lib/miro-ego";
pub const SYSTEM_RUN_DIR: &str = "/run/miro-ego";

pub const RECORDING_VOLUME_LABEL: &str = "MIRO-EGO";
pub const RECORDING_MOUNTPOINT: &str = "/data";
pub const HIDDEN_META_DIR_NAME: &str = ".miro-ego";

pub const MAINTENANCE_LOCK_NAME: &str = "maintenance.lock";
pub const NETWORK_OPERATION_LOCK_NAME: &str = "network-operation.lock";
pub const MAINTENANCE_LOCK_ENV: &str = "MIRO_EGO_MAINTENANCE_LOCK";
pub const NETWORK_OPERATION_LOCK_ENV: &str = "MIRO_EGO_NETWORK_OPERATION_LOCK_PATH";

pub(crate) fn resolve_lock_path_with_env(
    run_dir: &Path,
    default_name: &str,
    env_override: Option<&OsStr>,
) -> PathBuf {
    if run_dir == Path::new(SYSTEM_RUN_DIR) {
        if let Some(override_path) = env_override.filter(|value| !value.is_empty()) {
            return PathBuf::from(override_path);
        }
    }
    run_dir.join(default_name)
}

/// Resolve the maintenance lock file path for `run_dir`, honoring
/// `MIRO_EGO_MAINTENANCE_LOCK` when `run_dir` is the canonical `/run/miro-ego`.
pub fn resolve_maintenance_lock_path(run_dir: impl AsRef<Path>) -> PathBuf {
    let env_val = std::env::var_os(MAINTENANCE_LOCK_ENV);
    resolve_lock_path_with_env(
        run_dir.as_ref(),
        MAINTENANCE_LOCK_NAME,
        env_val.as_deref(),
    )
}

/// Resolve the network-operation lock file path for `run_dir`, honoring
/// `MIRO_EGO_NETWORK_OPERATION_LOCK_PATH` when `run_dir` is the canonical `/run/miro-ego`.
pub fn resolve_network_operation_lock_path(run_dir: impl AsRef<Path>) -> PathBuf {
    let env_val = std::env::var_os(NETWORK_OPERATION_LOCK_ENV);
    resolve_lock_path_with_env(
        run_dir.as_ref(),
        NETWORK_OPERATION_LOCK_NAME,
        env_val.as_deref(),
    )
}

/// Canonical system-disk paths (parameterizable for hermetic testing).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemPaths {
    pub install_root: PathBuf,
    pub config_dir: PathBuf,
    pub state_dir: PathBuf,
    pub run_dir: PathBuf,
}

impl SystemPaths {
    pub fn canonical() -> Self {
        Self {
            install_root: PathBuf::from(SYSTEM_INSTALL_ROOT),
            config_dir: PathBuf::from(SYSTEM_CONFIG_DIR),
            state_dir: PathBuf::from(SYSTEM_STATE_DIR),
            run_dir: PathBuf::from(SYSTEM_RUN_DIR),
        }
    }

    pub fn miro_ego_bin(&self) -> PathBuf {
        self.install_root.join("current/bin/miro-ego")
    }

    pub fn encoder_bin(&self) -> PathBuf {
        self.install_root.join("current/bin/miro-ego-encoder")
    }

    pub fn device_config_file(&self) -> PathBuf {
        self.config_dir.join("device.json")
    }

    pub fn settings_file(&self) -> PathBuf {
        self.config_dir.join("settings.json")
    }

    pub fn network_config_file(&self) -> PathBuf {
        self.config_dir.join("network.json")
    }

    pub fn maintenance_lock_file(&self) -> PathBuf {
        resolve_maintenance_lock_path(&self.run_dir)
    }

    pub fn network_operation_lock_file(&self) -> PathBuf {
        resolve_network_operation_lock_path(&self.run_dir)
    }
}

impl Default for SystemPaths {
    fn default() -> Self {
        Self::canonical()
    }
}

/// Canonical exFAT recording-disk layout under `/data` (or a test mountpoint).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataVolumePaths {
    mountpoint: PathBuf,
}

impl DataVolumePaths {
    pub fn new(mountpoint: impl Into<PathBuf>) -> Self {
        Self {
            mountpoint: mountpoint.into(),
        }
    }

    pub fn canonical() -> Self {
        Self::new(RECORDING_MOUNTPOINT)
    }

    pub fn mountpoint(&self) -> &Path {
        &self.mountpoint
    }

    // --- User-visible directories ---

    pub fn recordings_dir(&self) -> PathBuf {
        self.mountpoint.join("recordings")
    }

    pub fn logs_dir(&self) -> PathBuf {
        self.mountpoint.join("logs")
    }

    pub fn updates_dir(&self) -> PathBuf {
        self.mountpoint.join("updates")
    }

    // --- Hidden system metadata directory (.miro-ego) ---

    pub fn hidden_meta_dir(&self) -> PathBuf {
        self.mountpoint.join(HIDDEN_META_DIR_NAME)
    }

    pub fn volume_metadata_file(&self) -> PathBuf {
        self.hidden_meta_dir().join("volume.json")
    }

    pub fn index_dir(&self) -> PathBuf {
        self.hidden_meta_dir().join("index")
    }

    pub fn catalog_file(&self) -> PathBuf {
        self.index_dir().join("catalog.bin")
    }

    pub fn staging_dir(&self) -> PathBuf {
        self.hidden_meta_dir().join("staging")
    }

    pub fn quarantine_dir(&self) -> PathBuf {
        self.hidden_meta_dir().join("quarantine")
    }

    pub fn audit_dir(&self) -> PathBuf {
        self.hidden_meta_dir().join("audit")
    }

    pub fn receipts_dir(&self) -> PathBuf {
        self.hidden_meta_dir().join("receipts")
    }

    pub fn telemetry_dir(&self) -> PathBuf {
        self.hidden_meta_dir().join("telemetry")
    }

    /// Ensure every canonical visible and hidden directory exists and apply exFAT
    /// `ATTR_HIDDEN` to `.miro-ego/`. Returns `Ok(true)` if the FAT hidden ioctl was
    /// applied, or `Ok(false)` on non-FAT filesystems.
    pub fn ensure_layout(&self) -> io::Result<bool> {
        for dir in [
            self.recordings_dir(),
            self.logs_dir(),
            self.updates_dir(),
            self.hidden_meta_dir(),
            self.index_dir(),
            self.staging_dir(),
            self.quarantine_dir(),
            self.audit_dir(),
            self.receipts_dir(),
            self.telemetry_dir(),
        ] {
            fs::create_dir_all(&dir)?;
        }
        set_exfat_hidden(&self.hidden_meta_dir(), true)
    }
}

impl Default for DataVolumePaths {
    fn default() -> Self {
        Self::canonical()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn canonical_system_and_data_paths_match_miro_ego_spec() {
        let sys = SystemPaths::canonical();
        assert_eq!(sys.miro_ego_bin(), PathBuf::from(SYSTEM_CURRENT_BIN));
        assert_eq!(sys.encoder_bin(), PathBuf::from(SYSTEM_ENCODER_BIN));
        assert_eq!(
            sys.device_config_file(),
            PathBuf::from("/etc/miro-ego/device.json")
        );
        assert_eq!(
            sys.settings_file(),
            PathBuf::from("/etc/miro-ego/settings.json")
        );
        assert_eq!(
            sys.network_config_file(),
            PathBuf::from("/etc/miro-ego/network.json")
        );
        assert_eq!(
            sys.maintenance_lock_file(),
            PathBuf::from("/run/miro-ego/maintenance.lock")
        );
        assert_eq!(
            sys.network_operation_lock_file(),
            PathBuf::from("/run/miro-ego/network-operation.lock")
        );

        let data = DataVolumePaths::canonical();
        assert_eq!(data.recordings_dir(), PathBuf::from("/data/recordings"));
        assert_eq!(data.logs_dir(), PathBuf::from("/data/logs"));
        assert_eq!(data.updates_dir(), PathBuf::from("/data/updates"));
        assert_eq!(data.hidden_meta_dir(), PathBuf::from("/data/.miro-ego"));
        assert_eq!(
            data.volume_metadata_file(),
            PathBuf::from("/data/.miro-ego/volume.json")
        );
        assert_eq!(
            data.catalog_file(),
            PathBuf::from("/data/.miro-ego/index/catalog.bin")
        );
        assert_eq!(data.staging_dir(), PathBuf::from("/data/.miro-ego/staging"));
        assert_eq!(
            data.quarantine_dir(),
            PathBuf::from("/data/.miro-ego/quarantine")
        );
        assert_eq!(data.audit_dir(), PathBuf::from("/data/.miro-ego/audit"));
        assert_eq!(
            data.receipts_dir(),
            PathBuf::from("/data/.miro-ego/receipts")
        );
        assert_eq!(
            data.telemetry_dir(),
            PathBuf::from("/data/.miro-ego/telemetry")
        );
    }

    #[test]
    fn ensure_layout_creates_visible_and_hidden_trees() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "miro-ego-paths-test-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&root).unwrap();

        let volume = DataVolumePaths::new(&root);
        let _ = volume.ensure_layout().unwrap();
        assert!(volume.recordings_dir().is_dir());
        assert!(volume.logs_dir().is_dir());
        assert!(volume.updates_dir().is_dir());
        assert!(volume.hidden_meta_dir().is_dir());
        assert!(volume.index_dir().is_dir());
        assert!(volume.staging_dir().is_dir());
        assert!(volume.quarantine_dir().is_dir());
        assert!(volume.audit_dir().is_dir());
        assert!(volume.receipts_dir().is_dir());
        assert!(volume.telemetry_dir().is_dir());

        fs::remove_dir_all(root).unwrap();
    }
}
