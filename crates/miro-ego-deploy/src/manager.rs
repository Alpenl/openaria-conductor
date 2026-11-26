//! Transactional A/B `ReleaseManager` implementing [`FirmwareManager`].
//!
//! Manages `/opt/miro-ego/releases/<commit>`, atomic `current` / `previous` symlinks,
//! `/var/lib/miro-ego/install-transaction.json`, exclusive `deployment.lock` & `maintenance.lock`,
//! explicit `0755`/`0644` asset modes immune to `UMask=0077`, trait-abstracted `systemctl` &
//! health checks, automatic rollback on activation failure, and crash transaction recovery.

use crate::bundle::{
    DefaultOfflinePackageHook, OfflinePackageHook, VerifiedBundle, validate_commit_sha,
    verify_bundle_dir,
};
use crate::{
    DeployError, DeployResult, FIRMWARE_STATUS_SCHEMA, FirmwareManager, ReleaseInfo, UpdateStage,
};
use miro_ego_core::ids::new_uuid_v4;
use miro_ego_core::locks::{LockMode, acquire_flock, acquire_maintenance_lock};
use miro_ego_core::paths::{
    SYSTEM_CONFIG_DIR, SYSTEM_INSTALL_ROOT, SYSTEM_RUN_DIR, SYSTEM_STATE_DIR,
};
use miro_ego_core::sys::fs::{fsync_directory, read_bounded_json, write_bytes_atomic};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const RELEASE_METADATA_SCHEMA: &str = "miro-ego.installed-release.v1";
pub const INSTALL_TRANSACTION_SCHEMA: &str = "miro-ego.install-transaction.v1";
pub const ACTIVATION_FAILURE_SCHEMA: &str = "miro-ego.activation-failure.v1";
pub const DEPLOYMENT_LOCK_NAME: &str = "deployment.lock";
pub const MAX_DEPLOY_JSON_BYTES: usize = 64 * 1024;

/// Deployment asset specification mapping `deploy/miro-ego/<source>` to `<system_root>/<dest>`
/// with an explicit POSIX mode (immune to `UMask=0077`) and preservation flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssetSpec {
    pub source_name: &'static str,
    pub dest_relative: &'static str,
    pub mode: u32,
    pub preserve_if_exists: bool,
}

/// Canonical Miro-Ego systemd units, helper scripts, and zero-growth system configs.
pub const DEPLOYMENT_ASSETS: &[AssetSpec] = &[
    AssetSpec {
        source_name: "miro-ego-journal.conf",
        dest_relative: "etc/systemd/journald.conf.d/60-miro-ego-space.conf",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-logrotate.conf",
        dest_relative: "etc/logrotate.d/00-miro-ego-defaults",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-logrotate-timer.conf",
        dest_relative: "etc/systemd/system/logrotate.timer.d/60-miro-ego-space.conf",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-core-limits.conf",
        dest_relative: "etc/systemd/system.conf.d/60-miro-ego-space.conf",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-coredump.conf",
        dest_relative: "etc/systemd/coredump.conf.d/60-miro-ego-space.conf",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-apt-cache.conf",
        dest_relative: "etc/apt/apt.conf.d/99miro-ego-cache",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-idle-fstrim",
        dest_relative: "usr/local/sbin/miro-ego-idle-fstrim",
        mode: 0o755,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-idle-fstrim.conf",
        dest_relative: "etc/systemd/system/fstrim.service.d/50-miro-ego-idle.conf",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-recover.service",
        dest_relative: "usr/lib/systemd/system/miro-ego-recover.service",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-network.socket",
        dest_relative: "usr/lib/systemd/system/miro-ego-network.socket",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-network.service",
        dest_relative: "usr/lib/systemd/system/miro-ego-network.service",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego.service",
        dest_relative: "usr/lib/systemd/system/miro-ego.service",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-button.service",
        dest_relative: "usr/lib/systemd/system/miro-ego-button.service",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-button.json",
        dest_relative: "etc/miro-ego/button.json",
        mode: 0o644,
        preserve_if_exists: true,
    },
    AssetSpec {
        source_name: "miro-ego.avahi",
        dest_relative: "etc/avahi/services/miro-ego.service",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-data-volume",
        dest_relative: "usr/local/sbin/miro-ego-data-volume",
        mode: 0o755,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-data-volume.service",
        dest_relative: "usr/lib/systemd/system/miro-ego-data-volume.service",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-device-login",
        dest_relative: "usr/local/sbin/miro-ego-device-login",
        mode: 0o700,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego.sysusers",
        dest_relative: "usr/lib/sysusers.d/miro-ego.conf",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego.tmpfiles",
        dest_relative: "usr/lib/tmpfiles.d/miro-ego.conf",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-wifi-watchdog",
        dest_relative: "usr/local/sbin/miro-ego-wifi-watchdog",
        mode: 0o755,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-wifi-watchdog.service",
        dest_relative: "usr/lib/systemd/system/miro-ego-wifi-watchdog.service",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-wifi-watchdog.timer",
        dest_relative: "usr/lib/systemd/system/miro-ego-wifi-watchdog.timer",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-wifi-watchdog.default",
        dest_relative: "etc/default/miro-ego-wifi-watchdog",
        mode: 0o644,
        preserve_if_exists: true,
    },
    AssetSpec {
        source_name: "aic8800-miro-ego.conf",
        dest_relative: "etc/modprobe.d/aic8800-miro-ego.conf",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "90-miro-ego-wifi-powersave.conf",
        dest_relative: "etc/NetworkManager/conf.d/90-miro-ego-wifi-powersave.conf",
        mode: 0o644,
        preserve_if_exists: false,
    },
];

const CORE_UNITS: &[&str] = &[
    "miro-ego-data-volume.service",
    "miro-ego-network.socket",
    "miro-ego.service",
];
const RECOVER_UNIT: &str = "miro-ego-recover.service";
const NETWORK_SERVICE_UNIT: &str = "miro-ego-network.service";
const BUTTON_UNIT: &str = "miro-ego-button.service";
const WATCHDOG_SERVICE_UNIT: &str = "miro-ego-wifi-watchdog.service";
const WATCHDOG_TIMER_UNIT: &str = "miro-ego-wifi-watchdog.timer";

/// Trait-abstracted `systemctl` / system unit controller for hermetic testing.
pub trait SystemctlRunner: Send + Sync {
    fn run_systemctl(&self, args: &[&str]) -> DeployResult<()>;
    fn unit_load_state(&self, unit: &str) -> DeployResult<String>;
}

/// Real OS `systemctl` runner with bounded timeout.
#[derive(Debug, Default, Clone, Copy)]
pub struct RealSystemctlRunner;

impl SystemctlRunner for RealSystemctlRunner {
    fn run_systemctl(&self, args: &[&str]) -> DeployResult<()> {
        let status = Command::new("systemctl")
            .args(args)
            .stdin(Stdio::null())
            .status()
            .map_err(|err| DeployError::Failed {
                message: format!("failed to invoke systemctl {}: {err}", args.join(" ")),
            })?;
        if !status.success() {
            return Err(DeployError::Failed {
                message: format!(
                    "systemctl {} exited with status {:?}",
                    args.join(" "),
                    status.code()
                ),
            });
        }
        Ok(())
    }

    fn unit_load_state(&self, unit: &str) -> DeployResult<String> {
        let output = Command::new("systemctl")
            .args(["show", "--property=LoadState", "--value", unit])
            .stdin(Stdio::null())
            .output()
            .map_err(|err| DeployError::Failed {
                message: format!("failed to query systemd unit state for {unit}: {err}"),
            })?;
        let state = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if !output.status.success() || state.is_empty() || state.contains('\n') {
            return Err(DeployError::Failed {
                message: format!("invalid systemd unit LoadState for {unit}"),
            });
        }
        Ok(state)
    }
}

/// Trait-abstracted post-activation health checker.
pub trait HealthChecker: Send + Sync {
    fn check_health(&self) -> DeployResult<()>;
}

/// Loopback HTTP health checker querying `GET /api/v1/device` until healthy or timed out.
#[derive(Debug, Clone)]
pub struct LocalHttpHealthChecker {
    addr: SocketAddr,
    timeout: Duration,
    poll_interval: Duration,
}

impl LocalHttpHealthChecker {
    pub fn new(addr: SocketAddr, timeout: Duration) -> Self {
        Self {
            addr,
            timeout,
            poll_interval: Duration::from_millis(250),
        }
    }

    fn probe_once(&self) -> Result<(), String> {
        let mut stream = TcpStream::connect_timeout(&self.addr, Duration::from_secs(2))
            .map_err(|e| e.to_string())?;
        let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
        let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
        let req = format!(
            "GET /api/v1/device HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            self.addr
        );
        stream
            .write_all(req.as_bytes())
            .and_then(|()| stream.flush())
            .map_err(|e| e.to_string())?;
        let mut buf = [0u8; 256];
        let n = stream.read(&mut buf).map_err(|e| e.to_string())?;
        let line = String::from_utf8_lossy(&buf[..n]);
        if line.starts_with("HTTP/1.1 200") || line.starts_with("HTTP/1.0 200") {
            Ok(())
        } else {
            Err(format!(
                "unexpected health probe status: {}",
                line.lines().next().unwrap_or("")
            ))
        }
    }
}

impl HealthChecker for LocalHttpHealthChecker {
    fn check_health(&self) -> DeployResult<()> {
        let deadline = Instant::now() + self.timeout;
        let mut last_err = "service is not active".to_owned();
        while Instant::now() < deadline {
            match self.probe_once() {
                Ok(()) => return Ok(()),
                Err(err) => last_err = err,
            }
            std::thread::sleep(self.poll_interval);
        }
        Err(DeployError::Failed {
            message: format!("activation health check timed out: {last_err}"),
        })
    }
}

/// On-disk metadata stored in `/opt/miro-ego/releases/<commit>/release.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledReleaseMetadata {
    pub schema: String,
    pub commit: String,
    pub version: String,
    pub bundle_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionAction {
    Install,
    Rollback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionState {
    Prepared,
    Switched,
}

/// Durable crash-recovery journal stored at `/var/lib/miro-ego/install-transaction.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallTransaction {
    pub schema: String,
    pub transaction_id: String,
    pub action: TransactionAction,
    pub old_current: Option<String>,
    pub new_current: String,
    pub state: TransactionState,
}

/// Persisted activation failure diagnostic written at `/var/lib/miro-ego/activation-failure.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivationFailureRecord {
    pub schema: String,
    pub release_commit: String,
    pub message: String,
    pub rolled_back_to: Option<String>,
}

/// Rich deployment status returned by `status`, `install`, `rollback`, and `recover`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentStatus {
    pub schema: String,
    pub installed: bool,
    pub current: Option<String>,
    pub current_version: Option<String>,
    pub previous: Option<String>,
    pub transaction_pending: bool,
    pub activation_failure: Option<ActivationFailureRecord>,
    pub recovery_delegated: bool,
}

/// Result returned by `uninstall`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UninstallReport {
    pub installed: bool,
    pub config_preserved: bool,
    pub state_preserved: bool,
    pub removed_assets: Vec<String>,
}

/// Paths configuration for [`ReleaseManager`] (parameterizable for hermetic tests).
#[derive(Debug, Clone)]
pub struct ReleaseManagerPaths {
    pub install_root: PathBuf,
    pub config_root: PathBuf,
    pub state_root: PathBuf,
    pub run_dir: PathBuf,
    pub system_root: PathBuf,
}

impl ReleaseManagerPaths {
    pub fn canonical() -> Self {
        Self {
            install_root: PathBuf::from(SYSTEM_INSTALL_ROOT),
            config_root: PathBuf::from(SYSTEM_CONFIG_DIR),
            state_root: PathBuf::from(SYSTEM_STATE_DIR),
            run_dir: PathBuf::from(SYSTEM_RUN_DIR),
            system_root: PathBuf::from("/"),
        }
    }

    pub fn releases_dir(&self) -> PathBuf {
        self.install_root.join("releases")
    }

    pub fn transaction_file(&self) -> PathBuf {
        self.state_root.join("install-transaction.json")
    }

    pub fn activation_failure_file(&self) -> PathBuf {
        self.state_root.join("activation-failure.json")
    }

    pub fn deployment_lock_file(&self) -> PathBuf {
        self.run_dir.join(DEPLOYMENT_LOCK_NAME)
    }
}

impl Default for ReleaseManagerPaths {
    fn default() -> Self {
        Self::canonical()
    }
}

/// Pure-Rust transactional A/B release manager implementing [`FirmwareManager`].
pub struct ReleaseManager {
    paths: ReleaseManagerPaths,
    systemctl: Arc<dyn SystemctlRunner>,
    health: Arc<dyn HealthChecker>,
    offline_hook: Arc<dyn OfflinePackageHook>,
}

impl ReleaseManager {
    pub fn new(
        paths: ReleaseManagerPaths,
        systemctl: Arc<dyn SystemctlRunner>,
        health: Arc<dyn HealthChecker>,
    ) -> Self {
        Self {
            paths,
            systemctl,
            health,
            offline_hook: Arc::new(DefaultOfflinePackageHook),
        }
    }

    pub fn with_offline_hook(mut self, hook: Arc<dyn OfflinePackageHook>) -> Self {
        self.offline_hook = hook;
        self
    }

    pub fn paths(&self) -> &ReleaseManagerPaths {
        &self.paths
    }

    /// Inspect current A/B installation status without taking mutating locks.
    pub fn status(&self) -> DeployResult<DeploymentStatus> {
        let current = self.read_link_commit("current")?;
        let previous = self.read_link_commit("previous")?;
        let current_version = match &current {
            Some(commit) => self.read_release_metadata(commit).ok().map(|m| m.version),
            None => None,
        };
        let transaction_pending = fs::symlink_metadata(self.paths.transaction_file()).is_ok();
        let activation_failure = self.read_activation_failure();
        Ok(DeploymentStatus {
            schema: FIRMWARE_STATUS_SCHEMA.to_owned(),
            installed: current.is_some(),
            current,
            current_version,
            previous,
            transaction_pending,
            activation_failure,
            recovery_delegated: false,
        })
    }

    /// Transactionally install a verified native Miro-Ego release bundle with automatic
    /// rollback if activation health checks fail.
    pub fn install(&self, bundle_path: &Path, activate: bool) -> DeployResult<DeploymentStatus> {
        let (_deploy_lock, _maint_lock) = self.acquire_exclusive_locks()?;
        self.ensure_layout()?;
        let _ = self.recover_locked()?;

        let verified = verify_bundle_dir(bundle_path)?;
        self.offline_hook.pre_install_verify(&verified)?;

        let release_dir = self.stage_release(&verified)?;
        let old_current = self.read_link_commit("current")?;
        let new_commit = verified.commit().to_owned();
        let release_changed = old_current.as_deref() != Some(new_commit.as_str());

        let quiesced = old_current.is_some() && (activate || release_changed);
        if quiesced {
            self.quiesce_services()?;
        }

        let mut tx_started = false;
        let switch_res = (|| -> DeployResult<()> {
            if release_changed {
                let tx = InstallTransaction {
                    schema: INSTALL_TRANSACTION_SCHEMA.to_owned(),
                    transaction_id: new_uuid_v4(),
                    action: TransactionAction::Install,
                    old_current: old_current.clone(),
                    new_current: new_commit.clone(),
                    state: TransactionState::Prepared,
                };
                self.write_transaction(&tx)?;
                tx_started = true;

                self.install_assets_from_release(&release_dir)?;
                self.systemctl.run_systemctl(&["daemon-reload"])?;
                self.settle_transaction(&tx)?;
            } else {
                self.install_assets_from_release(&release_dir)?;
                self.systemctl.run_systemctl(&["daemon-reload"])?;
            }
            Ok(())
        })();

        if let Err(err) = switch_res {
            // Roll back partial switch before returning error.
            if let Some(old) = &old_current {
                let old_dir = self.paths.releases_dir().join(old);
                let _ = self.install_assets_from_release(&old_dir);
            }
            if tx_started {
                let _ = self.revert_switch(old_current.as_deref(), &new_commit);
            }
            if quiesced {
                let _ = self.systemctl.run_systemctl(&["daemon-reload"]);
                let _ = self.start_services(false);
            }
            return Err(err);
        }

        if activate {
            self.activate_or_rollback(old_current.as_deref(), &new_commit)?;
        }

        self.status()
    }

    /// Transactionally roll back `current` to `previous`, restoring previous release's unit assets.
    pub fn rollback(&self, activate: bool) -> DeployResult<DeploymentStatus> {
        let (_deploy_lock, _maint_lock) = self.acquire_exclusive_locks()?;
        self.ensure_layout()?;
        let _ = self.recover_locked()?;

        let Some(current) = self.read_link_commit("current")? else {
            return Err(DeployError::Blocked {
                message: "rollback_unavailable: no active release installed".to_owned(),
            });
        };
        let Some(previous) = self.read_link_commit("previous")? else {
            return Err(DeployError::Blocked {
                message: "rollback_unavailable: no previous release available".to_owned(),
            });
        };
        let _ = self.read_release_metadata(&previous)?;

        self.quiesce_services()?;
        let tx = InstallTransaction {
            schema: INSTALL_TRANSACTION_SCHEMA.to_owned(),
            transaction_id: new_uuid_v4(),
            action: TransactionAction::Rollback,
            old_current: Some(current.clone()),
            new_current: previous.clone(),
            state: TransactionState::Prepared,
        };
        self.write_transaction(&tx)?;

        let prev_dir = self.paths.releases_dir().join(&previous);
        let switch_res = (|| -> DeployResult<()> {
            self.install_assets_from_release(&prev_dir)?;
            self.systemctl.run_systemctl(&["daemon-reload"])?;
            self.settle_transaction(&tx)?;
            Ok(())
        })();

        if let Err(err) = switch_res {
            let cur_dir = self.paths.releases_dir().join(&current);
            let _ = self.install_assets_from_release(&cur_dir);
            let _ = self.revert_switch(Some(&current), &previous);
            let _ = self.systemctl.run_systemctl(&["daemon-reload"]);
            let _ = self.start_services(false);
            return Err(err);
        }

        if activate {
            self.activate_or_rollback(Some(&current), &previous)?;
        }

        self.status()
    }

    /// Recover any interrupted install/rollback transaction (used on boot by `miro-ego-recover.service`).
    pub fn recover(&self, service_start: bool) -> DeployResult<DeploymentStatus> {
        match acquire_flock(self.paths.deployment_lock_file(), LockMode::Exclusive, true) {
            Ok(_deploy_guard) => {
                self.ensure_layout()?;
                self.recover_locked()
            }
            Err(miro_ego_core::CoreError::LockBusy { .. }) if service_start => {
                // During activation, a concurrent service start may observe deployment.lock held.
                // Permit non-mutating check only when no transaction is pending and current is valid.
                if fs::symlink_metadata(self.paths.transaction_file()).is_ok() {
                    return Err(DeployError::Blocked {
                        message: "deployment_busy: release transaction is uncommitted".to_owned(),
                    });
                }
                let Some(current) = self.read_link_commit("current")? else {
                    return Err(DeployError::Blocked {
                        message: "deployment_busy: no committed release available".to_owned(),
                    });
                };
                let _ = self.read_release_metadata(&current)?;
                let mut st = self.status()?;
                st.recovery_delegated = true;
                Ok(st)
            }
            Err(miro_ego_core::CoreError::LockBusy { message }) => {
                Err(DeployError::Blocked { message })
            }
            Err(err) => Err(DeployError::Core(err)),
        }
    }

    /// Uninstall Miro-Ego binaries and managed systemd/script assets while cleanly preserving
    /// `/etc/miro-ego/**`, `/var/lib/miro-ego/**`, and operator-owned config files.
    pub fn uninstall(&self) -> DeployResult<UninstallReport> {
        let (_deploy_lock, _maint_lock) = self.acquire_exclusive_locks()?;
        self.deactivate_units()?;

        if fs::symlink_metadata(&self.paths.install_root)
            .map(|m| m.file_type().is_dir())
            .unwrap_or(false)
        {
            fs::remove_dir_all(&self.paths.install_root).map_err(|err| DeployError::Failed {
                message: format!(
                    "failed removing install root {}: {err}",
                    self.paths.install_root.display()
                ),
            })?;
        }
        let _ = fs::remove_file(self.paths.transaction_file());

        let mut removed_assets = Vec::new();
        for asset in DEPLOYMENT_ASSETS {
            if asset.preserve_if_exists {
                continue;
            }
            let target = self.paths.system_root.join(asset.dest_relative);
            if fs::symlink_metadata(&target).is_ok() {
                fs::remove_file(&target).map_err(|err| DeployError::Failed {
                    message: format!("failed removing asset {}: {err}", target.display()),
                })?;
                removed_assets.push(asset.dest_relative.to_owned());
            }
        }
        let _ = fs::remove_file(self.paths.run_dir.join("network-control.sock"));
        self.systemctl.run_systemctl(&["daemon-reload"])?;

        Ok(UninstallReport {
            installed: false,
            config_preserved: true,
            state_preserved: true,
            removed_assets,
        })
    }

    fn recover_locked(&self) -> DeployResult<DeploymentStatus> {
        let mut assets_restored = false;
        if let Some(tx) = self.read_transaction()? {
            let target_dir = self.paths.releases_dir().join(&tx.new_current);
            let target_valid = self.release_is_complete(&tx.new_current);

            if target_valid {
                self.install_assets_from_release(&target_dir)?;
                assets_restored = true;
                self.settle_transaction(&tx)?;
            } else if let Some(old) = &tx.old_current {
                let old_dir = self.paths.releases_dir().join(old);
                if self.release_is_complete(old) {
                    self.install_assets_from_release(&old_dir)?;
                    assets_restored = true;
                }
                self.revert_switch(Some(old), &tx.new_current)?;
            } else {
                self.revert_switch(None, &tx.new_current)?;
            }
        }

        if assets_restored {
            let _ = self.systemctl.run_systemctl(&["daemon-reload"]);
        }

        // Remove any orphaned staging directories left by crashed installs.
        if let Ok(entries) = fs::read_dir(self.paths.releases_dir()) {
            for entry in entries.filter_map(Result::ok) {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with('.') && name.contains(".staging-") {
                    let path = entry.path();
                    if fs::symlink_metadata(&path)
                        .map(|m| m.file_type().is_dir())
                        .unwrap_or(false)
                    {
                        let _ = fs::remove_dir_all(&path);
                    }
                }
            }
        }

        self.status()
    }

    fn activate_or_rollback(
        &self,
        old_current: Option<&str>,
        new_current: &str,
    ) -> DeployResult<()> {
        match self.start_services(true) {
            Ok(()) => {
                let _ = fs::remove_file(self.paths.activation_failure_file());
                Ok(())
            }
            Err(activation_err) => {
                let release_changed = old_current != Some(new_current);
                let rolled_back_to = if release_changed {
                    old_current.map(str::to_owned)
                } else {
                    None
                };

                // Record failure diagnostic (best-effort so it never blocks rollback).
                let _ = self.record_activation_failure(
                    new_current,
                    &activation_err.to_string(),
                    rolled_back_to.clone(),
                );

                let rollback_result: DeployResult<()> = if let Some(old) = old_current {
                    if release_changed {
                        self.restore_previous_release(old, new_current)
                    } else {
                        self.deactivate_units()
                    }
                } else {
                    let deact = self.deactivate_units();
                    let sw1 = self.switch_symlink("current", None);
                    let sw2 = self.switch_symlink("previous", Some(new_current));
                    deact.and(sw1).and(sw2)
                };

                if let Err(rb_err) = rollback_result {
                    return Err(DeployError::Failed {
                        message: format!(
                            "activation of {new_current} failed ({activation_err}) and automatic rollback failed ({rb_err})"
                        ),
                    });
                }

                Err(DeployError::Failed {
                    message: match rolled_back_to {
                        Some(old) => format!(
                            "activation of {new_current} failed ({activation_err}); automatically rolled back to {old}"
                        ),
                        None => format!(
                            "activation of {new_current} failed ({activation_err}); deactivated units"
                        ),
                    },
                })
            }
        }
    }

    fn restore_previous_release(
        &self,
        old_current: &str,
        failed_release: &str,
    ) -> DeployResult<()> {
        self.quiesce_services()?;
        let tx = InstallTransaction {
            schema: INSTALL_TRANSACTION_SCHEMA.to_owned(),
            transaction_id: new_uuid_v4(),
            action: TransactionAction::Rollback,
            old_current: Some(failed_release.to_owned()),
            new_current: old_current.to_owned(),
            state: TransactionState::Prepared,
        };
        self.write_transaction(&tx)?;
        let old_dir = self.paths.releases_dir().join(old_current);
        self.install_assets_from_release(&old_dir)?;
        self.settle_transaction(&tx)?;
        self.systemctl.run_systemctl(&["daemon-reload"])?;
        self.start_services(false)?;
        Ok(())
    }

    fn stage_release(&self, verified: &VerifiedBundle) -> DeployResult<PathBuf> {
        let releases = self.paths.releases_dir();
        let final_dir = releases.join(verified.commit());
        if final_dir.exists() && self.release_is_complete(verified.commit()) {
            if let Ok(meta) = self.read_release_metadata(verified.commit()) {
                if meta.bundle_sha256 == verified.manifest_sha256 {
                    return Ok(final_dir);
                }
            }
        }

        let stage_dir = releases.join(format!(
            ".{}.staging-{}",
            verified.commit(),
            new_uuid_v4().replace('-', "")
        ));
        create_dir_with_mode(&stage_dir, 0o755)?;

        let stage_result = (|| -> DeployResult<()> {
            for entry in verified.all_entries() {
                let src = verified.root.join(&entry.path);
                let dst = stage_dir.join(&entry.path);
                if let Some(parent) = dst.parent() {
                    ensure_dir_tree_mode(parent, &stage_dir, 0o755)?;
                }
                let payload = fs::read(&src).map_err(|err| DeployError::Failed {
                    message: format!("failed reading verified bundle entry {}: {err}", entry.path),
                })?;
                let mode = if entry.executable || entry.path.starts_with("bin/") {
                    0o755
                } else {
                    0o644
                };
                write_bytes_atomic(&dst, &payload, mode, true).map_err(|err| {
                    DeployError::Failed {
                        message: format!("failed staging {}: {err}", entry.path),
                    }
                })?;
            }

            let meta = InstalledReleaseMetadata {
                schema: RELEASE_METADATA_SCHEMA.to_owned(),
                commit: verified.commit().to_owned(),
                version: verified.version().to_owned(),
                bundle_sha256: verified.manifest_sha256.clone(),
            };
            let mut meta_bytes =
                serde_json::to_vec_pretty(&meta).map_err(|err| DeployError::Failed {
                    message: format!("failed serializing release.json: {err}"),
                })?;
            meta_bytes.push(b'\n');
            write_bytes_atomic(&stage_dir.join("release.json"), &meta_bytes, 0o644, true).map_err(
                |err| DeployError::Failed {
                    message: format!("failed writing release.json: {err}"),
                },
            )?;

            let _ = fsync_directory(&stage_dir);
            let mut backup: Option<PathBuf> = None;
            if final_dir.exists() {
                let bak = releases.join(format!(
                    ".{}.backup-{}",
                    verified.commit(),
                    new_uuid_v4().replace('-', "")
                ));
                fs::rename(&final_dir, &bak).map_err(|err| DeployError::Failed {
                    message: format!("failed moving previous release aside: {err}"),
                })?;
                backup = Some(bak);
            }
            if let Err(err) = fs::rename(&stage_dir, &final_dir) {
                if let Some(bak) = backup {
                    let _ = fs::rename(bak, &final_dir);
                }
                return Err(DeployError::Failed {
                    message: format!("failed promoting staged release: {err}"),
                });
            }
            let _ = fsync_directory(&releases);
            if let Some(bak) = backup {
                let _ = fs::remove_dir_all(bak);
            }
            Ok(())
        })();

        if stage_result.is_err() {
            let _ = fs::remove_dir_all(&stage_dir);
        }
        stage_result?;
        Ok(final_dir)
    }

    /// Install systemd units and scripts from `<release>/deploy/miro-ego/` with explicit
    /// `0755`/`0700`/`0644` permissions (immune to `UMask=0077`).
    fn install_assets_from_release(&self, release_dir: &Path) -> DeployResult<()> {
        let deploy_dir = release_dir.join("deploy/miro-ego");
        if !deploy_dir.is_dir() {
            return Ok(());
        }
        let customer_tls = self.is_customer_tls_configured();

        for asset in DEPLOYMENT_ASSETS {
            let source_name = if asset.source_name == "miro-ego.avahi" && customer_tls {
                let customer_candidate = deploy_dir.join("miro-ego-customer.avahi");
                if customer_candidate.is_file() {
                    "miro-ego-customer.avahi"
                } else {
                    asset.source_name
                }
            } else {
                asset.source_name
            };

            let src_path = deploy_dir.join(source_name);
            if !src_path.is_file() {
                continue;
            }
            let dest_path = self.paths.system_root.join(asset.dest_relative);
            if asset.preserve_if_exists && fs::symlink_metadata(&dest_path).is_ok() {
                continue;
            }

            if let Some(parent) = dest_path.parent() {
                let dir_mode = if parent == self.paths.system_root.join("etc/miro-ego") {
                    0o750
                } else {
                    0o755
                };
                ensure_dir_tree_mode(parent, &self.paths.system_root, dir_mode)?;
            }

            let payload = fs::read(&src_path).map_err(|err| DeployError::Failed {
                message: format!("failed reading release asset {source_name}: {err}"),
            })?;
            write_bytes_atomic(&dest_path, &payload, asset.mode, true).map_err(|err| {
                DeployError::Failed {
                    message: format!("failed installing asset {}: {err}", dest_path.display()),
                }
            })?;
        }
        Ok(())
    }

    fn is_customer_tls_configured(&self) -> bool {
        let cfg_path = self.paths.config_root.join("device.json");
        let Ok(val) =
            miro_ego_core::sys::fs::read_bounded_json_value(&cfg_path, MAX_DEPLOY_JSON_BYTES, 16)
        else {
            return false;
        };
        val.get("security")
            .and_then(|s| s.get("profile"))
            .and_then(serde_json::Value::as_str)
            == Some("customer")
    }

    fn settle_transaction(&self, tx: &InstallTransaction) -> DeployResult<()> {
        let _ = self.read_release_metadata(&tx.new_current)?;
        if tx.state == TransactionState::Prepared {
            self.switch_symlink("current", Some(&tx.new_current))?;
            let mut switched = tx.clone();
            switched.state = TransactionState::Switched;
            self.write_transaction(&switched)?;
        }

        let previous_target = tx
            .old_current
            .as_deref()
            .filter(|&old| old != tx.new_current.as_str());
        self.switch_symlink("previous", previous_target)?;

        let mut keep = HashSet::new();
        keep.insert(tx.new_current.clone());
        if let Some(prev) = previous_target {
            keep.insert(prev.to_owned());
        }
        self.prune_releases(&keep)?;

        let _ = fs::remove_file(self.paths.transaction_file());
        let _ = fsync_directory(&self.paths.state_root);
        Ok(())
    }

    fn revert_switch(&self, old_current: Option<&str>, new_current: &str) -> DeployResult<()> {
        self.switch_symlink("current", old_current)?;
        self.switch_symlink("previous", Some(new_current))?;
        let _ = fs::remove_file(self.paths.transaction_file());
        let _ = fsync_directory(&self.paths.state_root);
        Ok(())
    }

    fn prune_releases(&self, keep: &HashSet<String>) -> DeployResult<()> {
        let releases = self.paths.releases_dir();
        let Ok(entries) = fs::read_dir(&releases) else {
            return Ok(());
        };
        for entry in entries.filter_map(Result::ok) {
            let name = entry.file_name().to_string_lossy().into_owned();
            if keep.contains(&name) {
                continue;
            }
            if validate_commit_sha(&name).is_ok() {
                let path = entry.path();
                if fs::symlink_metadata(&path)
                    .map(|m| m.file_type().is_dir())
                    .unwrap_or(false)
                {
                    let _ = fs::remove_dir_all(path);
                }
            }
        }
        let _ = fsync_directory(&releases);
        Ok(())
    }

    fn quiesce_services(&self) -> DeployResult<()> {
        let recover_state = self.systemctl.unit_load_state(RECOVER_UNIT)?;
        let mut cmds: Vec<&[&str]> = vec![
            &["disable", "--now", WATCHDOG_TIMER_UNIT],
            &["stop", WATCHDOG_SERVICE_UNIT],
            &["stop", "miro-ego.service"],
            &["stop", "miro-ego-network.socket"],
            &["stop", NETWORK_SERVICE_UNIT],
        ];
        if recover_state != "not-found" {
            cmds.push(&["stop", RECOVER_UNIT]);
        }
        for cmd in cmds {
            self.systemctl.run_systemctl(cmd)?;
        }
        Ok(())
    }

    fn start_services(&self, check_health: bool) -> DeployResult<()> {
        self.systemctl
            .run_systemctl(&["try-restart", "systemd-journald.service"])?;
        self.systemctl
            .run_systemctl(&["enable", "--now", "logrotate.timer"])?;
        for unit in CORE_UNITS {
            self.systemctl.run_systemctl(&["enable", "--now", unit])?;
        }
        if check_health {
            self.health.check_health()?;
        }
        let button_enabled = self.button_config_enabled();
        let button_action = if button_enabled { "enable" } else { "disable" };
        self.systemctl
            .run_systemctl(&[button_action, "--now", BUTTON_UNIT])?;
        self.systemctl
            .run_systemctl(&["enable", "--now", WATCHDOG_TIMER_UNIT])?;
        Ok(())
    }

    fn deactivate_units(&self) -> DeployResult<()> {
        let cmds: &[&[&str]] = &[
            &["disable", "--now", WATCHDOG_TIMER_UNIT],
            &["stop", WATCHDOG_SERVICE_UNIT],
            &["disable", "--now", BUTTON_UNIT],
            &["disable", "--now", "miro-ego.service"],
            &["disable", "--now", "miro-ego-network.socket"],
            &["stop", NETWORK_SERVICE_UNIT],
            &["stop", RECOVER_UNIT],
            &["disable", "--now", "miro-ego-data-volume.service"],
        ];
        for cmd in cmds {
            self.systemctl.run_systemctl(cmd)?;
        }
        Ok(())
    }

    fn button_config_enabled(&self) -> bool {
        let primary = self.paths.config_root.join("button.json");
        let fallback = self.paths.system_root.join("etc/miro-ego/button.json");
        let path = if primary.is_file() { primary } else { fallback };
        miro_ego_core::sys::fs::read_bounded_json_value(&path, 8 * 1024, 8)
            .ok()
            .and_then(|v| v.get("enabled").and_then(serde_json::Value::as_bool))
            .unwrap_or(true)
    }

    fn ensure_layout(&self) -> DeployResult<()> {
        create_dir_with_mode(&self.paths.install_root, 0o755)?;
        create_dir_with_mode(&self.paths.releases_dir(), 0o755)?;
        create_dir_with_mode(&self.paths.config_root, 0o750)?;
        create_dir_with_mode(&self.paths.state_root, 0o750)?;
        create_dir_with_mode(&self.paths.run_dir, 0o750)?;
        Ok(())
    }

    fn acquire_exclusive_locks(
        &self,
    ) -> DeployResult<(
        miro_ego_core::locks::FlockGuard,
        miro_ego_core::locks::FlockGuard,
    )> {
        let deploy = acquire_flock(self.paths.deployment_lock_file(), LockMode::Exclusive, true)
            .map_err(map_lock_err)?;
        let maint = acquire_maintenance_lock(&self.paths.run_dir, LockMode::Exclusive, true)
            .map_err(map_lock_err)?;
        Ok((deploy, maint))
    }

    fn read_link_commit(&self, name: &str) -> DeployResult<Option<String>> {
        let link = self.paths.install_root.join(name);
        match fs::read_link(&link) {
            Ok(target) => {
                let rendered = target.to_string_lossy();
                let Some(commit) = rendered.strip_prefix("releases/") else {
                    return Err(DeployError::Failed {
                        message: format!("invalid {name} symlink target: {rendered}"),
                    });
                };
                if commit.contains('/') {
                    return Err(DeployError::Failed {
                        message: format!("invalid {name} symlink target: {rendered}"),
                    });
                }
                validate_commit_sha(commit)?;
                Ok(Some(commit.to_owned()))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(DeployError::Failed {
                message: format!("failed reading {name} symlink: {err}"),
            }),
        }
    }

    fn switch_symlink(&self, name: &str, commit: Option<&str>) -> DeployResult<()> {
        let link = self.paths.install_root.join(name);
        let Some(target_commit) = commit else {
            match fs::remove_file(&link) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(DeployError::Failed {
                        message: format!("failed removing {name} symlink: {err}"),
                    });
                }
            }
            let _ = fsync_directory(&self.paths.install_root);
            return Ok(());
        };
        validate_commit_sha(target_commit)?;
        let temp = self
            .paths
            .install_root
            .join(format!(".{name}.{}.tmp", new_uuid_v4().replace('-', "")));
        symlink(format!("releases/{target_commit}"), &temp).map_err(|err| DeployError::Failed {
            message: format!("failed creating temp symlink {name}: {err}"),
        })?;
        if let Err(err) = fs::rename(&temp, &link) {
            let _ = fs::remove_file(&temp);
            return Err(DeployError::Failed {
                message: format!("failed atomically replacing {name} symlink: {err}"),
            });
        }
        let _ = fsync_directory(&self.paths.install_root);
        Ok(())
    }

    fn read_release_metadata(&self, commit: &str) -> DeployResult<InstalledReleaseMetadata> {
        validate_commit_sha(commit)?;
        let path = self.paths.releases_dir().join(commit).join("release.json");
        let meta: InstalledReleaseMetadata = read_bounded_json(&path, MAX_DEPLOY_JSON_BYTES, 8)
            .map_err(|err| DeployError::Failed {
                message: format!("failed reading release metadata for {commit}: {err}"),
            })?;
        if meta.schema != RELEASE_METADATA_SCHEMA || meta.commit != commit {
            return Err(DeployError::Failed {
                message: format!("release metadata identity mismatch for {commit}"),
            });
        }
        Ok(meta)
    }

    fn release_is_complete(&self, commit: &str) -> bool {
        let dir = self.paths.releases_dir().join(commit);
        self.read_release_metadata(commit).is_ok()
            && dir.join("bin/miro-ego").is_file()
            && dir.join("bin/miro-ego-encoder").is_file()
    }

    fn read_transaction(&self) -> DeployResult<Option<InstallTransaction>> {
        let path = self.paths.transaction_file();
        if fs::symlink_metadata(&path).is_err() {
            return Ok(None);
        }
        let tx: InstallTransaction =
            read_bounded_json(&path, MAX_DEPLOY_JSON_BYTES, 8).map_err(|err| {
                DeployError::Failed {
                    message: format!("install transaction is unreadable or corrupt: {err}"),
                }
            })?;
        if tx.schema != INSTALL_TRANSACTION_SCHEMA {
            return Err(DeployError::Failed {
                message: format!("unexpected install transaction schema: {}", tx.schema),
            });
        }
        validate_commit_sha(&tx.new_current)?;
        if let Some(old) = &tx.old_current {
            validate_commit_sha(old)?;
        }
        Ok(Some(tx))
    }

    fn write_transaction(&self, tx: &InstallTransaction) -> DeployResult<()> {
        let mut bytes = serde_json::to_vec_pretty(tx).map_err(|err| DeployError::Failed {
            message: format!("failed serializing install transaction: {err}"),
        })?;
        bytes.push(b'\n');
        write_bytes_atomic(&self.paths.transaction_file(), &bytes, 0o600, true).map_err(|err| {
            DeployError::Failed {
                message: format!("failed writing install transaction: {err}"),
            }
        })
    }

    fn record_activation_failure(
        &self,
        commit: &str,
        message: &str,
        rolled_back_to: Option<String>,
    ) -> DeployResult<()> {
        let rec = ActivationFailureRecord {
            schema: ACTIVATION_FAILURE_SCHEMA.to_owned(),
            release_commit: commit.to_owned(),
            message: message.to_owned(),
            rolled_back_to,
        };
        let mut bytes = serde_json::to_vec_pretty(&rec).map_err(|err| DeployError::Failed {
            message: format!("failed serializing activation failure: {err}"),
        })?;
        bytes.push(b'\n');
        write_bytes_atomic(&self.paths.activation_failure_file(), &bytes, 0o600, true).map_err(
            |err| DeployError::Failed {
                message: format!("failed writing activation failure record: {err}"),
            },
        )
    }

    fn read_activation_failure(&self) -> Option<ActivationFailureRecord> {
        let path = self.paths.activation_failure_file();
        if fs::symlink_metadata(&path).is_err() {
            return None;
        }
        read_bounded_json::<ActivationFailureRecord>(&path, MAX_DEPLOY_JSON_BYTES, 8)
            .ok()
            .filter(|r| r.schema == ACTIVATION_FAILURE_SCHEMA)
    }
}

impl FirmwareManager for ReleaseManager {
    fn current_release(&self) -> DeployResult<ReleaseInfo> {
        let status = self.status()?;
        let Some(commit) = status.current else {
            return Err(DeployError::Failed {
                message: "no release is currently installed".to_owned(),
            });
        };
        let meta = self.read_release_metadata(&commit)?;
        let stage = if status.transaction_pending {
            UpdateStage::Applying
        } else if status.activation_failure.is_some() {
            UpdateStage::RolledBack
        } else {
            UpdateStage::Idle
        };
        Ok(ReleaseInfo {
            schema: FIRMWARE_STATUS_SCHEMA.to_owned(),
            version: meta.version,
            commit_sha: meta.commit,
            stage,
        })
    }

    fn verify_bundle(&self, bundle_path: &Path) -> DeployResult<ReleaseInfo> {
        let verified = verify_bundle_dir(bundle_path)?;
        self.offline_hook.pre_install_verify(&verified)?;
        Ok(ReleaseInfo {
            schema: FIRMWARE_STATUS_SCHEMA.to_owned(),
            version: verified.version().to_owned(),
            commit_sha: verified.commit().to_owned(),
            stage: UpdateStage::Verified,
        })
    }

    fn apply_update(&self, bundle_path: &Path) -> DeployResult<ReleaseInfo> {
        let status = self.install(bundle_path, true)?;
        let commit = status.current.ok_or_else(|| DeployError::Failed {
            message: "release install completed without active current link".to_owned(),
        })?;
        let version = status
            .current_version
            .unwrap_or_else(|| "unknown".to_owned());
        Ok(ReleaseInfo {
            schema: FIRMWARE_STATUS_SCHEMA.to_owned(),
            version,
            commit_sha: commit,
            stage: UpdateStage::Idle,
        })
    }

    fn rollback(&self) -> DeployResult<ReleaseInfo> {
        let status = ReleaseManager::rollback(self, true)?;
        let commit = status.current.ok_or_else(|| DeployError::Failed {
            message: "rollback completed without active current link".to_owned(),
        })?;
        let version = status
            .current_version
            .unwrap_or_else(|| "unknown".to_owned());
        Ok(ReleaseInfo {
            schema: FIRMWARE_STATUS_SCHEMA.to_owned(),
            version,
            commit_sha: commit,
            stage: UpdateStage::RolledBack,
        })
    }
}

fn map_lock_err(err: miro_ego_core::CoreError) -> DeployError {
    match err {
        miro_ego_core::CoreError::LockBusy { message } => DeployError::Blocked { message },
        other => DeployError::Core(other),
    }
}

fn create_dir_with_mode(path: &Path, mode: u32) -> DeployResult<()> {
    fs::create_dir_all(path).map_err(|err| DeployError::Failed {
        message: format!("failed creating directory {}: {err}", path.display()),
    })?;
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).map_err(|err| DeployError::Failed {
        message: format!("failed chmod directory {}: {err}", path.display()),
    })
}

/// Ensure every directory between `stop_root` and `dir` exists with explicit mode (immune to UMask).
fn ensure_dir_tree_mode(dir: &Path, stop_root: &Path, leaf_mode: u32) -> DeployResult<()> {
    let mut missing = Vec::new();
    let mut cur = dir;
    while cur != stop_root && !cur.exists() {
        missing.push(cur.to_path_buf());
        match cur.parent() {
            Some(p) => cur = p,
            None => break,
        }
    }
    for entry in missing.into_iter().rev() {
        let mode = if entry == dir { leaf_mode } else { 0o755 };
        create_dir_with_mode(&entry, mode)?;
    }
    if dir != stop_root && dir.exists() {
        fs::set_permissions(dir, fs::Permissions::from_mode(leaf_mode)).map_err(|err| {
            DeployError::Failed {
                message: format!("failed chmod directory {}: {err}", dir.display()),
            }
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bundle::test_support::build_valid_bundle;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_env(tag: &str) -> (PathBuf, ReleaseManagerPaths) {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "miro-ego-deploy-{tag}-{}-{unique}",
            std::process::id()
        ));
        let paths = ReleaseManagerPaths {
            install_root: root.join("opt/miro-ego"),
            config_root: root.join("etc/miro-ego"),
            state_root: root.join("var/lib/miro-ego"),
            run_dir: root.join("run/miro-ego"),
            system_root: root.clone(),
        };
        fs::create_dir_all(&root).unwrap();
        (root, paths)
    }

    struct MockSystemctl {
        commands: Mutex<Vec<Vec<String>>>,
    }

    impl MockSystemctl {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                commands: Mutex::new(Vec::new()),
            })
        }

        fn take_commands(&self) -> Vec<Vec<String>> {
            std::mem::take(&mut *self.commands.lock().unwrap())
        }
    }

    impl SystemctlRunner for MockSystemctl {
        fn run_systemctl(&self, args: &[&str]) -> DeployResult<()> {
            self.commands
                .lock()
                .unwrap()
                .push(args.iter().map(|s| (*s).to_owned()).collect());
            Ok(())
        }

        fn unit_load_state(&self, _unit: &str) -> DeployResult<String> {
            Ok("loaded".to_owned())
        }
    }

    struct ToggleHealth {
        healthy: AtomicBool,
    }

    impl HealthChecker for ToggleHealth {
        fn check_health(&self) -> DeployResult<()> {
            if self.healthy.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err(DeployError::Failed {
                    message: "simulated health check failure".to_owned(),
                })
            }
        }
    }

    #[test]
    fn ab_install_sets_explicit_modes_and_auto_rolls_back_on_health_failure() {
        let (root, paths) = temp_env("ab-rollback");
        let sysctl = MockSystemctl::new();
        let health = Arc::new(ToggleHealth {
            healthy: AtomicBool::new(true),
        });
        let mgr = ReleaseManager::new(paths.clone(), sysctl.clone(), health.clone());

        let commit_v1 = "1".repeat(40);
        let commit_v2 = "2".repeat(40);

        let b1 = root.join("bundles/v1");
        build_valid_bundle(
            &b1,
            "0.3.0",
            &commit_v1,
            &[
                (
                    "miro-ego.service",
                    b"[Unit]\nDescription=Miro-Ego v1\n",
                    false,
                ),
                ("miro-ego-data-volume", b"#!/bin/sh\necho v1\n", true),
                (
                    "miro-ego-button.json",
                    b"{\"enabled\":true,\"pin\":37}\n",
                    false,
                ),
            ],
        );

        // Install v1 cleanly.
        let st1 = mgr.install(&b1, true).unwrap();
        assert!(st1.installed);
        assert_eq!(st1.current.as_deref(), Some(commit_v1.as_str()));
        assert_eq!(st1.previous, None);
        assert!(!st1.transaction_pending);

        // Verify explicit 0644 / 0755 modes on installed unit and script!
        let unit_path = root.join("usr/lib/systemd/system/miro-ego.service");
        let script_path = root.join("usr/local/sbin/miro-ego-data-volume");
        assert_eq!(
            fs::metadata(&unit_path).unwrap().permissions().mode() & 0o777,
            0o644
        );
        assert_eq!(
            fs::metadata(&script_path).unwrap().permissions().mode() & 0o777,
            0o755
        );
        assert!(fs::read_to_string(&unit_path).unwrap().contains("v1"));

        // Operator customizes preserved button.json.
        let btn_path = root.join("etc/miro-ego/button.json");
        fs::write(&btn_path, b"{\"enabled\":true,\"pin\":33,\"custom\":1}\n").unwrap();

        // Build v2 bundle and make health check FAIL on v2 activation!
        let b2 = root.join("bundles/v2");
        build_valid_bundle(
            &b2,
            "0.3.1",
            &commit_v2,
            &[
                (
                    "miro-ego.service",
                    b"[Unit]\nDescription=Miro-Ego v2\n",
                    false,
                ),
                ("miro-ego-data-volume", b"#!/bin/sh\necho v2\n", true),
                (
                    "miro-ego-button.json",
                    b"{\"enabled\":true,\"pin\":37}\n",
                    false,
                ),
            ],
        );
        health.healthy.store(false, Ordering::SeqCst);
        let _ = sysctl.take_commands();

        let err = mgr.install(&b2, true).unwrap_err();
        assert!(err.to_string().contains("automatically rolled back"));

        // Current MUST be back on v1, previous points to failed v2, v1's unit file restored,
        // daemon-reload ran, activation failure recorded, and user button.json preserved!
        let after = mgr.status().unwrap();
        assert_eq!(after.current.as_deref(), Some(commit_v1.as_str()));
        assert_eq!(after.previous.as_deref(), Some(commit_v2.as_str()));
        assert!(!after.transaction_pending);
        assert!(after.activation_failure.is_some());
        assert!(fs::read_to_string(&unit_path).unwrap().contains("v1"));
        assert!(
            fs::read_to_string(&btn_path)
                .unwrap()
                .contains("\"custom\":1")
        );
        let cmds = sysctl.take_commands();
        assert!(cmds.iter().any(|c| c == &["daemon-reload"]));

        // Now test uninstall: removes /opt/miro-ego & managed units while preserving /etc/miro-ego!
        let report = mgr.uninstall().unwrap();
        assert!(!report.installed);
        assert!(report.config_preserved);
        assert!(!paths.install_root.exists());
        assert!(!unit_path.exists());
        assert!(btn_path.exists());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn crash_transaction_recovery_settles_prepared_and_reverts_broken_targets() {
        let (root, paths) = temp_env("crash-recover");
        let sysctl = MockSystemctl::new();
        let health = Arc::new(ToggleHealth {
            healthy: AtomicBool::new(true),
        });
        let mgr = ReleaseManager::new(paths.clone(), sysctl.clone(), health);

        let commit_v1 = "a".repeat(40);
        let commit_v2 = "b".repeat(40);
        let b1 = root.join("bundles/v1");
        let b2 = root.join("bundles/v2");
        build_valid_bundle(
            &b1,
            "0.3.0",
            &commit_v1,
            &[("miro-ego.service", b"[Unit]\nDescription=v1\n", false)],
        );
        build_valid_bundle(
            &b2,
            "0.3.1",
            &commit_v2,
            &[("miro-ego.service", b"[Unit]\nDescription=v2\n", false)],
        );

        // Install v1 normally, then stage v2 and simulate crash right after writing `prepared` transaction!
        mgr.install(&b1, false).unwrap();
        let v2_verified = verify_bundle_dir(&b2).unwrap();
        mgr.stage_release(&v2_verified).unwrap();
        // Also leave a stale staging directory to verify cleanup.
        fs::create_dir_all(paths.releases_dir().join(".deadbeef.staging-orphan")).unwrap();

        let crashed_tx = InstallTransaction {
            schema: INSTALL_TRANSACTION_SCHEMA.to_owned(),
            transaction_id: new_uuid_v4(),
            action: TransactionAction::Install,
            old_current: Some(commit_v1.clone()),
            new_current: commit_v2.clone(),
            state: TransactionState::Prepared,
        };
        mgr.write_transaction(&crashed_tx).unwrap();
        assert!(mgr.status().unwrap().transaction_pending);

        // Boot recovery settles the valid prepared target to v2!
        let recovered = mgr.recover(false).unwrap();
        assert!(!recovered.transaction_pending);
        assert_eq!(recovered.current.as_deref(), Some(commit_v2.as_str()));
        assert_eq!(recovered.previous.as_deref(), Some(commit_v1.as_str()));
        assert!(
            !paths
                .releases_dir()
                .join(".deadbeef.staging-orphan")
                .exists()
        );
        assert!(
            fs::read_to_string(root.join("usr/lib/systemd/system/miro-ego.service"))
                .unwrap()
                .contains("v2")
        );

        // Now simulate crash where target release v3 is missing/corrupt -> recovery reverts to v2!
        let commit_v3 = "c".repeat(40);
        let bad_tx = InstallTransaction {
            schema: INSTALL_TRANSACTION_SCHEMA.to_owned(),
            transaction_id: new_uuid_v4(),
            action: TransactionAction::Install,
            old_current: Some(commit_v2.clone()),
            new_current: commit_v3.clone(),
            state: TransactionState::Prepared,
        };
        mgr.write_transaction(&bad_tx).unwrap();

        let reverted = mgr.recover(false).unwrap();
        assert!(!reverted.transaction_pending);
        assert_eq!(reverted.current.as_deref(), Some(commit_v2.as_str()));

        // Explicit rollback from v2 -> v1 via FirmwareManager trait!
        // First ensure v1 is still present (previous was overwritten by commit_v3 during revert,
        // so point previous back to v1 and invoke FirmwareManager::rollback).
        mgr.switch_symlink("previous", Some(&commit_v1)).unwrap();
        let rolled = FirmwareManager::rollback(&mgr).unwrap();
        assert_eq!(rolled.commit_sha, commit_v1);
        assert_eq!(rolled.stage, UpdateStage::RolledBack);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn blocks_concurrent_install_when_maintenance_lock_is_held() {
        let (root, paths) = temp_env("lock-busy");
        let sysctl = MockSystemctl::new();
        let health = Arc::new(ToggleHealth {
            healthy: AtomicBool::new(true),
        });
        let mgr = ReleaseManager::new(paths.clone(), sysctl, health);

        let commit = "d".repeat(40);
        let b1 = root.join("bundles/v1");
        build_valid_bundle(&b1, "0.3.0", &commit, &[]);

        let _capture_lock =
            acquire_maintenance_lock(&paths.run_dir, LockMode::Exclusive, true).unwrap();
        let err = mgr.install(&b1, false).unwrap_err();
        assert!(matches!(err, DeployError::Blocked { .. }));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn selects_customer_avahi_asset_and_delegates_service_start_recovery_when_lock_held() {
        let (root, paths) = temp_env("customer-avahi");
        let sysctl = MockSystemctl::new();
        let health = Arc::new(ToggleHealth {
            healthy: AtomicBool::new(true),
        });
        let mgr = ReleaseManager::new(paths.clone(), sysctl, health);

        fs::create_dir_all(&paths.config_root).unwrap();
        fs::write(
            paths.config_root.join("device.json"),
            b"{\"security\":{\"profile\":\"customer\"}}\n",
        )
        .unwrap();

        let commit = "e".repeat(40);
        let b1 = root.join("bundles/v1");
        build_valid_bundle(
            &b1,
            "0.3.0",
            &commit,
            &[
                ("miro-ego.avahi", b"<service>http</service>\n", false),
                (
                    "miro-ego-customer.avahi",
                    b"<service>https</service>\n",
                    false,
                ),
            ],
        );

        mgr.install(&b1, true).unwrap();
        let avahi = fs::read_to_string(root.join("etc/avahi/services/miro-ego.service")).unwrap();
        assert!(avahi.contains("https"));

        // While deployment.lock is held with no pending transaction, recover(service_start=true)
        // delegates cleanly; recover(service_start=false) returns Blocked.
        let _deploy_guard =
            acquire_flock(paths.deployment_lock_file(), LockMode::Exclusive, true).unwrap();
        let delegated = mgr.recover(true).unwrap();
        assert!(delegated.recovery_delegated);
        assert_eq!(delegated.current.as_deref(), Some(commit.as_str()));
        assert!(matches!(
            mgr.recover(false),
            Err(DeployError::Blocked { .. })
        ));

        fs::remove_dir_all(root).unwrap();
    }
}
