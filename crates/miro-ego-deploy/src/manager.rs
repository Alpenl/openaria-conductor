//! Transactional A/B `ReleaseManager` implementing [`FirmwareManager`].
//!
//! Manages `/opt/miro-ego/releases/<commit>`, atomic `current` / `previous` symlinks,
//! `/var/lib/miro-ego/install-transaction.json`, exclusive `deployment.lock` & `maintenance.lock`,
//! explicit `0755`/`0644` asset modes immune to `UMask=0077`, trait-abstracted `systemctl` &
//! health checks, automatic rollback on activation failure, and crash transaction recovery.

use crate::bundle::{
    DefaultOfflinePackageHook, OfflinePackageHook, VerifiedBundle, copy_and_verify_safe,
    validate_commit_sha, verify_bundle_dir, verify_bundle_manifest,
};
use crate::{
    DeployError, DeployResult, FIRMWARE_STATUS_SCHEMA, FirmwareManager, ReleaseInfo, UpdateStage,
};
use miro_ego_core::config::{ListenConfig, SecurityConfig, load_device_config};
use miro_ego_core::ids::new_uuid_v4;
use miro_ego_core::locks::{LockMode, acquire_flock, acquire_maintenance_lock};
use miro_ego_core::paths::{
    SYSTEM_CONFIG_DIR, SYSTEM_INSTALL_ROOT, SYSTEM_RUN_DIR, SYSTEM_STATE_DIR,
};
use miro_ego_core::sys::fs::{
    fsync_directory, open_directory_safe, openat_retry, read_bounded_bytes, read_bounded_json,
    write_bytes_atomic,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{WebPkiSupportedAlgorithms, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, ClientConnection, DigitallySignedStruct, SignatureScheme, StreamOwned};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::ffi::CString;
use std::fs::{self, File};
use std::io::{Cursor, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use subtle::ConstantTimeEq;

pub const RELEASE_METADATA_SCHEMA: &str = "miro-ego.installed-release.v1";
pub const INSTALLED_ASSETS_SCHEMA: &str = "miro-ego.installed-assets.v1";
pub const INSTALL_TRANSACTION_SCHEMA: &str = "miro-ego.install-transaction.v1";
pub const ACTIVATION_FAILURE_SCHEMA: &str = "miro-ego.activation-failure.v1";
pub const DEPLOYMENT_LOCK_NAME: &str = "deployment.lock";
pub const SYSTEM_DEPLOY_STATE_DIR: &str = "/var/lib/miro-ego-deploy";
pub const MAX_DEPLOY_JSON_BYTES: usize = 64 * 1024;
const INSTALLED_ASSETS_FILENAME: &str = "installed-assets.json";

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
    AssetSpec {
        source_name: "90-miro-ego-leds.rules",
        dest_relative: "etc/udev/rules.d/90-miro-ego-leds.rules",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-deploy.socket",
        dest_relative: "usr/lib/systemd/system/miro-ego-deploy.socket",
        mode: 0o644,
        preserve_if_exists: false,
    },
    AssetSpec {
        source_name: "miro-ego-deploy.service",
        dest_relative: "usr/lib/systemd/system/miro-ego-deploy.service",
        mode: 0o644,
        preserve_if_exists: false,
    },
];

const CORE_UNITS: &[&str] = &[
    "miro-ego-data-volume.service",
    "miro-ego-network.socket",
    "miro-ego.service",
];
const DEPLOY_SOCKET_UNIT: &str = "miro-ego-deploy.socket";
const RECOVER_UNIT: &str = "miro-ego-recover.service";
const NETWORK_SERVICE_UNIT: &str = "miro-ego-network.service";
const BUTTON_UNIT: &str = "miro-ego-button.service";
const WATCHDOG_SERVICE_UNIT: &str = "miro-ego-wifi-watchdog.service";
const WATCHDOG_TIMER_UNIT: &str = "miro-ego-wifi-watchdog.timer";
pub const DEFAULT_SYSTEM_COMMAND_TIMEOUT: Duration = Duration::from_secs(60);

/// Trait-abstracted `systemctl` / system bootstrap controller for hermetic testing.
pub trait SystemctlRunner: Send + Sync {
    fn run_systemctl(&self, args: &[&str]) -> DeployResult<()>;
    fn unit_load_state(&self, unit: &str) -> DeployResult<String>;

    /// Execute an auxiliary bootstrap utility (`systemd-sysusers`, `systemd-tmpfiles`,
    /// `udevadm`, `/usr/local/sbin/miro-ego-device-login`). Default is a no-op for mocks.
    fn run_bootstrap_command(&self, program: &str, args: &[&str]) -> DeployResult<()> {
        let _ = (program, args);
        Ok(())
    }
}

/// Real OS `systemctl` & bootstrap runner with bounded 60s execution timeout.
#[derive(Debug, Default, Clone, Copy)]
pub struct RealSystemctlRunner;

/// Configurable-timeout OS `systemctl` & bootstrap runner.
#[derive(Debug, Clone, Copy)]
pub struct BoundedSystemctlRunner {
    timeout: Duration,
}

impl RealSystemctlRunner {
    pub fn with_timeout(timeout: Duration) -> BoundedSystemctlRunner {
        BoundedSystemctlRunner { timeout }
    }
}

fn kill_bounded_process_group(child: &mut std::process::Child) {
    #[cfg(unix)]
    if let Ok(pid) = libc::pid_t::try_from(child.id()) {
        if pid > 0 {
            // SAFETY: `child` was placed in its own process group (`process_group(0)`),
            // so `-pid` targets only the spawned command and its descendants.
            let _ = unsafe { libc::kill(-pid, libc::SIGKILL) };
        }
    }
    let _ = child.kill();
    let _ = child.wait();
}

fn run_command_bounded(
    program: &str,
    args: &[&str],
    timeout: Duration,
) -> DeployResult<(i32, String, String)> {
    let mut cmd = Command::new(program);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    let mut child = cmd.spawn().map_err(|err| DeployError::Failed {
        message: format!("failed to invoke {program} {}: {err}", args.join(" ")),
    })?;

    let mut out_pipe = child.stdout.take();
    let mut err_pipe = child.stderr.take();
    let out_handle = std::thread::spawn(move || -> Vec<u8> {
        let mut buf = Vec::new();
        if let Some(ref mut p) = out_pipe {
            let _ = p.read_to_end(&mut buf);
        }
        buf
    });
    let err_handle = std::thread::spawn(move || -> Vec<u8> {
        let mut buf = Vec::new();
        if let Some(ref mut p) = err_pipe {
            let _ = p.read_to_end(&mut buf);
        }
        buf
    });

    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(st)) => break st,
            Ok(None) => {
                if Instant::now() >= deadline {
                    kill_bounded_process_group(&mut child);
                    let _ = out_handle.join();
                    let _ = err_handle.join();
                    return Err(DeployError::Failed {
                        message: format!(
                            "{program} {} timed out after {}s",
                            args.join(" "),
                            timeout.as_secs()
                        ),
                    });
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(err) => {
                kill_bounded_process_group(&mut child);
                let _ = out_handle.join();
                let _ = err_handle.join();
                return Err(DeployError::Failed {
                    message: format!("failed waiting on {program} {}: {err}", args.join(" ")),
                });
            }
        }
    };

    let stdout = String::from_utf8_lossy(&out_handle.join().unwrap_or_default()).into_owned();
    let stderr = String::from_utf8_lossy(&err_handle.join().unwrap_or_default()).into_owned();
    Ok((status.code().unwrap_or(-1), stdout, stderr))
}

impl SystemctlRunner for BoundedSystemctlRunner {
    fn run_systemctl(&self, args: &[&str]) -> DeployResult<()> {
        let (code, _stdout, stderr) = run_command_bounded("systemctl", args, self.timeout)?;
        if code != 0 {
            return Err(DeployError::Failed {
                message: format!(
                    "systemctl {} exited with status {code}: {}",
                    args.join(" "),
                    stderr.trim()
                ),
            });
        }
        Ok(())
    }

    fn unit_load_state(&self, unit: &str) -> DeployResult<String> {
        let (code, stdout, _stderr) = run_command_bounded(
            "systemctl",
            &["show", "--property=LoadState", "--value", unit],
            self.timeout,
        )?;
        let state = stdout.trim().to_owned();
        if code != 0 || state.is_empty() || state.contains('\n') {
            return Err(DeployError::Failed {
                message: format!("invalid systemd unit LoadState for {unit}"),
            });
        }
        Ok(state)
    }

    fn run_bootstrap_command(&self, program: &str, args: &[&str]) -> DeployResult<()> {
        let (code, _stdout, stderr) = run_command_bounded(program, args, self.timeout)?;
        if code != 0 {
            return Err(DeployError::Failed {
                message: format!(
                    "{program} {} exited with status {code}: {}",
                    args.join(" "),
                    stderr.trim()
                ),
            });
        }
        Ok(())
    }
}

impl SystemctlRunner for RealSystemctlRunner {
    fn run_systemctl(&self, args: &[&str]) -> DeployResult<()> {
        Self::with_timeout(DEFAULT_SYSTEM_COMMAND_TIMEOUT).run_systemctl(args)
    }

    fn unit_load_state(&self, unit: &str) -> DeployResult<String> {
        Self::with_timeout(DEFAULT_SYSTEM_COMMAND_TIMEOUT).unit_load_state(unit)
    }

    fn run_bootstrap_command(&self, program: &str, args: &[&str]) -> DeployResult<()> {
        Self::with_timeout(DEFAULT_SYSTEM_COMMAND_TIMEOUT).run_bootstrap_command(program, args)
    }
}

/// Trait-abstracted post-activation health checker.
pub trait HealthChecker: Send + Sync {
    fn check_health(&self) -> DeployResult<()>;
}

#[derive(Debug)]
struct PinnedCertVerifier {
    pinned_ders: Vec<CertificateDer<'static>>,
    supported: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for PinnedCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let matched = self.pinned_ders.iter().any(|pinned| {
            pinned.as_ref().len() == end_entity.as_ref().len()
                && bool::from(pinned.as_ref().ct_eq(end_entity.as_ref()))
        });
        if matched {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(
                "server certificate does not match pinned device certificate".to_owned(),
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.supported)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.supported)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported.supported_schemes()
    }
}

fn build_pinned_tls_client_config(cert_pem: &[u8]) -> DeployResult<Arc<ClientConfig>> {
    let mut reader = Cursor::new(cert_pem);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| DeployError::Failed {
            message: format!("failed parsing pinned TLS certificate PEM: {err}"),
        })?;
    if certs.is_empty() {
        return Err(DeployError::Failed {
            message: "pinned TLS certificate PEM contains no CERTIFICATE blocks".to_owned(),
        });
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let supported = provider.signature_verification_algorithms;
    let verifier = Arc::new(PinnedCertVerifier {
        pinned_ders: certs,
        supported,
    });
    let cfg = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|err| DeployError::Failed {
            message: format!("failed configuring TLS protocol versions: {err}"),
        })?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    Ok(Arc::new(cfg))
}

fn loopback_addr_from_listen(listen: &ListenConfig) -> Option<SocketAddr> {
    let ip: IpAddr = listen.host.parse().ok()?;
    let target_ip = match ip {
        IpAddr::V4(v4) if v4.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(v6) if v6.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        concrete => concrete,
    };
    Some(SocketAddr::new(target_ip, listen.port))
}

fn format_authority_host(addr: &SocketAddr) -> String {
    match addr {
        SocketAddr::V4(v4) => format!("{}:{}", v4.ip(), v4.port()),
        SocketAddr::V6(v6) => format!("[{}]:{}", v6.ip(), v6.port()),
    }
}

fn validate_http_header_token(token: &str, label: &str) -> DeployResult<()> {
    if token.is_empty()
        || token
            .bytes()
            .any(|b| b == b'\r' || b == b'\n' || b < 0x20 || b == 0x7f)
    {
        return Err(DeployError::Failed {
            message: format!("{label} contains invalid control or newline characters"),
        });
    }
    Ok(())
}

/// Loopback HTTP/HTTPS health checker querying `GET /api/v1/device` until healthy or timed out.
#[derive(Debug, Clone)]
pub struct LocalHttpHealthChecker {
    addr: SocketAddr,
    timeout: Duration,
    poll_interval: Duration,
    tls_config: Option<Arc<ClientConfig>>,
    bearer_token: Option<String>,
    config_path: Option<PathBuf>,
    fallback_addr: SocketAddr,
}

struct ResolvedProbeTarget {
    addr: SocketAddr,
    tls_config: Option<Arc<ClientConfig>>,
    bearer_token: Option<String>,
}

fn resolve_probe_target_from_path(
    config_path: &Path,
    fallback_addr: SocketAddr,
) -> DeployResult<ResolvedProbeTarget> {
    match fs::symlink_metadata(config_path) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ResolvedProbeTarget {
                addr: fallback_addr,
                tls_config: None,
                bearer_token: None,
            });
        }
        Err(err) => {
            return Err(DeployError::Failed {
                message: format!(
                    "cannot inspect device config {}: {err}",
                    config_path.display()
                ),
            });
        }
        Ok(_) => {}
    }

    let cfg = load_device_config(config_path).map_err(|err| DeployError::Failed {
        message: format!(
            "failed loading device config {}: {err}",
            config_path.display()
        ),
    })?;
    let addr = loopback_addr_from_listen(&cfg.listen).unwrap_or(fallback_addr);
    match cfg.security {
        SecurityConfig::Lab { .. } => Ok(ResolvedProbeTarget {
            addr,
            tls_config: None,
            bearer_token: None,
        }),
        SecurityConfig::Customer {
            bearer_token_file,
            tls_certificate_file,
            ..
        } => {
            let raw_tok = read_bounded_bytes(&bearer_token_file, 4096).map_err(|err| {
                DeployError::Failed {
                    message: format!(
                        "failed reading customer bearer token {}: {err}",
                        bearer_token_file.display()
                    ),
                }
            })?;
            let token = String::from_utf8(raw_tok)
                .map_err(|_| DeployError::Failed {
                    message: "customer bearer token is not valid UTF-8".to_owned(),
                })?
                .trim()
                .to_owned();
            if token.is_empty() {
                return Err(DeployError::Failed {
                    message: "customer bearer token file is empty".to_owned(),
                });
            }
            validate_http_header_token(&token, "customer bearer token")?;
            let cert_pem = read_bounded_bytes(&tls_certificate_file, 64 * 1024).map_err(|err| {
                DeployError::Failed {
                    message: format!(
                        "failed reading customer TLS certificate {}: {err}",
                        tls_certificate_file.display()
                    ),
                }
            })?;
            let tls_cfg = build_pinned_tls_client_config(&cert_pem)?;
            Ok(ResolvedProbeTarget {
                addr,
                tls_config: Some(tls_cfg),
                bearer_token: Some(token),
            })
        }
    }
}

impl LocalHttpHealthChecker {
    pub fn new(addr: SocketAddr, timeout: Duration) -> Self {
        Self {
            addr,
            timeout,
            poll_interval: Duration::from_millis(250),
            tls_config: None,
            bearer_token: None,
            config_path: None,
            fallback_addr: addr,
        }
    }

    /// Configure pinned-certificate TLS (`https`) and optional `Authorization: Bearer` token.
    pub fn with_tls_and_bearer(
        mut self,
        cert_pem: &[u8],
        bearer_token: Option<String>,
    ) -> DeployResult<Self> {
        if let Some(ref tok) = bearer_token {
            validate_http_header_token(tok, "bearer_token")?;
        }
        self.tls_config = Some(build_pinned_tls_client_config(cert_pem)?);
        self.bearer_token = bearer_token;
        self.config_path = None;
        Ok(self)
    }

    /// Construct a health checker from `/etc/miro-ego/device.json` (handling custom `listen.port`
    /// and `security.profile == "customer"` HTTPS + Bearer token), falling back to plain HTTP on
    /// `fallback_addr` when `config_path` does not exist yet, and dynamically reloading
    /// `device.json`, TLS certificate, and Bearer token on every `check_health()` call.
    pub fn from_device_config_path(
        config_path: &Path,
        fallback_addr: SocketAddr,
        timeout: Duration,
    ) -> DeployResult<Self> {
        let initial = resolve_probe_target_from_path(config_path, fallback_addr)?;
        Ok(Self {
            addr: initial.addr,
            timeout,
            poll_interval: Duration::from_millis(250),
            tls_config: initial.tls_config,
            bearer_token: initial.bearer_token,
            config_path: Some(config_path.to_path_buf()),
            fallback_addr,
        })
    }

    fn current_target(&self) -> Result<ResolvedProbeTarget, String> {
        if let Some(cfg_path) = &self.config_path {
            resolve_probe_target_from_path(cfg_path, self.fallback_addr).map_err(|e| e.to_string())
        } else {
            Ok(ResolvedProbeTarget {
                addr: self.addr,
                tls_config: self.tls_config.clone(),
                bearer_token: self.bearer_token.clone(),
            })
        }
    }

    fn probe_once(&self) -> Result<(), String> {
        let target = self.current_target()?;
        let tcp = TcpStream::connect_timeout(&target.addr, Duration::from_secs(2))
            .map_err(|e| e.to_string())?;
        let _ = tcp.set_read_timeout(Some(Duration::from_secs(2)));
        let _ = tcp.set_write_timeout(Some(Duration::from_secs(2)));

        let authority = format_authority_host(&target.addr);
        let mut req = format!(
            "GET /api/v1/device HTTP/1.1\r\nHost: {authority}\r\nAccept: application/json\r\nConnection: close\r\n"
        );
        if let Some(tok) = &target.bearer_token {
            req.push_str(&format!("Authorization: Bearer {tok}\r\n"));
        }
        req.push_str("\r\n");

        let mut buf = [0u8; 256];
        let n = if let Some(tls_cfg) = &target.tls_config {
            let server_name = ServerName::try_from("localhost").map_err(|e| e.to_string())?;
            let conn = ClientConnection::new(Arc::clone(tls_cfg), server_name)
                .map_err(|e| e.to_string())?;
            let mut tls_stream = StreamOwned::new(conn, tcp);
            tls_stream
                .write_all(req.as_bytes())
                .and_then(|()| tls_stream.flush())
                .map_err(|e| e.to_string())?;
            tls_stream.read(&mut buf).map_err(|e| e.to_string())?
        } else {
            let mut plain = tcp;
            plain
                .write_all(req.as_bytes())
                .and_then(|()| plain.flush())
                .map_err(|e| e.to_string())?;
            plain.read(&mut buf).map_err(|e| e.to_string())?
        };

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

/// Per-release ledger of non-preserved system/config relative paths installed into `system_root`
/// (`/opt/miro-ego/releases/<commit>/installed-assets.json`) so rollback removes release-only
/// unit drop-ins and configs cleanly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledAssetsManifest {
    pub schema: String,
    pub installed_dest_paths: Vec<String>,
}

pub(crate) struct HeldDeployLocks {
    _deploy: miro_ego_core::locks::FlockGuard,
    maintenance: Option<miro_ego_core::locks::FlockGuard>,
}

impl HeldDeployLocks {
    pub(crate) fn new(
        deploy: miro_ego_core::locks::FlockGuard,
        maintenance: miro_ego_core::locks::FlockGuard,
    ) -> Self {
        Self {
            _deploy: deploy,
            maintenance: Some(maintenance),
        }
    }

    pub(crate) fn drop_maintenance(&mut self) {
        self.maintenance = None;
    }
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
    #[serde(default)]
    pub old_previous: Option<String>,
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
    #[serde(default)]
    pub previous_version: Option<String>,
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
    pub deploy_state_root: PathBuf,
    pub run_dir: PathBuf,
    pub system_root: PathBuf,
}

impl ReleaseManagerPaths {
    pub fn canonical() -> Self {
        Self {
            install_root: PathBuf::from(SYSTEM_INSTALL_ROOT),
            config_root: PathBuf::from(SYSTEM_CONFIG_DIR),
            state_root: PathBuf::from(SYSTEM_STATE_DIR),
            deploy_state_root: PathBuf::from(SYSTEM_DEPLOY_STATE_DIR),
            run_dir: PathBuf::from(SYSTEM_RUN_DIR),
            system_root: PathBuf::from("/"),
        }
    }

    pub fn for_system_root(system_root: impl Into<PathBuf>) -> Self {
        let system_root = system_root.into();
        Self {
            install_root: system_root.join("opt/miro-ego"),
            config_root: system_root.join("etc/miro-ego"),
            state_root: system_root.join("var/lib/miro-ego"),
            deploy_state_root: system_root.join("var/lib/miro-ego-deploy"),
            run_dir: system_root.join("run/miro-ego"),
            system_root,
        }
    }

    pub fn releases_dir(&self) -> PathBuf {
        self.install_root.join("releases")
    }

    pub fn transaction_file(&self) -> PathBuf {
        self.deploy_state_root.join("install-transaction.json")
    }

    pub fn activation_failure_file(&self) -> PathBuf {
        self.deploy_state_root.join("activation-failure.json")
    }

    pub fn firmware_task_file(&self) -> PathBuf {
        self.deploy_state_root.join("firmware-task.json")
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

    pub fn offline_hook(&self) -> &dyn OfflinePackageHook {
        self.offline_hook.as_ref()
    }

    /// Inspect current A/B installation status without taking mutating locks.
    pub fn status(&self) -> DeployResult<DeploymentStatus> {
        let current = self.read_link_commit("current")?;
        let previous = self.read_link_commit("previous")?;
        let current_version = match &current {
            Some(commit) => self.read_release_metadata(commit).ok().map(|m| m.version),
            None => None,
        };
        let previous_version = match &previous {
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
            previous_version,
            transaction_pending,
            activation_failure,
            recovery_delegated: false,
        })
    }

    /// Transactionally install a verified native Miro-Ego release bundle with automatic
    /// rollback if activation health checks fail.
    pub fn install(&self, bundle_path: &Path, activate: bool) -> DeployResult<DeploymentStatus> {
        let mut locks = self.acquire_exclusive_locks()?;
        let preflight = verify_bundle_manifest(bundle_path)?;
        self.install_verified_with_locks(&preflight, activate, &mut locks)
    }

    /// Transactionally install an already-verified [`VerifiedBundle`] without re-hashing the
    /// bundle files on disk.
    pub fn install_verified(
        &self,
        verified: &VerifiedBundle,
        activate: bool,
    ) -> DeployResult<DeploymentStatus> {
        let mut locks = self.acquire_exclusive_locks()?;
        self.install_verified_with_locks(verified, activate, &mut locks)
    }

    pub(crate) fn install_verified_with_locks(
        &self,
        verified: &VerifiedBundle,
        activate: bool,
        locks: &mut HeldDeployLocks,
    ) -> DeployResult<DeploymentStatus> {
        self.ensure_layout()?;
        let _ = self.recover_locked()?;

        self.offline_hook.pre_install_verify(verified)?;

        let old_current = self.read_link_commit("current")?;
        let old_previous = self.read_link_commit("previous")?;
        let new_commit = verified.commit().to_owned();
        let release_changed = old_current.as_deref() != Some(new_commit.as_str());
        let replacing_live_commit = old_current.as_deref() == Some(new_commit.as_str())
            && self.paths.releases_dir().join(&new_commit).exists();

        // Single-pass staging: when staging a distinct commit, copy & verify SHA-256 into
        // `releases/<new_commit>` BEFORE quiescing live services so a corrupt payload never
        // interrupts running services. When replacing the live commit in-place, verify payload
        // digests before quiescing.
        let staged_before_quiesce = if !replacing_live_commit {
            Some(self.stage_release(verified)?)
        } else {
            if !verified.payloads_verified() {
                let _ = verify_bundle_dir(&verified.root)?;
            }
            None
        };

        // Quiesce running services BEFORE mutating/replacing any live release directory or unit!
        let quiesced =
            old_current.is_some() && (activate || release_changed || replacing_live_commit);
        if quiesced {
            self.quiesce_services()?;
        }

        let (release_dir, backup_dir) = match staged_before_quiesce {
            Some(pair) => pair,
            None => match self.stage_release(verified) {
                Ok(pair) => pair,
                Err(err) => {
                    if quiesced {
                        locks.drop_maintenance();
                        let _ = self.start_services(false);
                    }
                    return Err(err);
                }
            },
        };
        let tx = InstallTransaction {
            schema: INSTALL_TRANSACTION_SCHEMA.to_owned(),
            transaction_id: new_uuid_v4(),
            action: TransactionAction::Install,
            old_current: old_current.clone(),
            old_previous: old_previous.clone(),
            new_current: new_commit.clone(),
            state: TransactionState::Prepared,
        };
        self.write_transaction(&tx)?;

        let old_release_dir = old_current
            .as_ref()
            .filter(|&old| old != &new_commit)
            .map(|old| self.paths.releases_dir().join(old));

        let switch_res = (|| -> DeployResult<()> {
            self.sync_assets_between_releases(old_release_dir.as_deref(), &release_dir)?;
            self.systemctl.run_systemctl(&["daemon-reload"])?;
            self.bootstrap_system()?;
            if release_changed {
                self.switch_current_for_transaction(&tx)?;
            } else {
                let _ = self.read_release_metadata(&tx.new_current)?;
                let mut switched = tx.clone();
                switched.state = TransactionState::Switched;
                self.write_transaction(&switched)?;
            }
            Ok(())
        })();

        if let Err(err) = switch_res {
            if let Some(bak) = backup_dir {
                let _ = fs::remove_dir_all(&release_dir);
                let _ = fs::rename(&bak, &release_dir);
                let _ = fsync_directory(&self.paths.releases_dir());
            }
            if let Some(old) = &old_current {
                let old_dir = self.paths.releases_dir().join(old);
                let _ = self.sync_assets_between_releases(Some(&release_dir), &old_dir);
            }
            let _ = self.revert_to_previous_links(old_current.as_deref(), old_previous.as_deref());
            if quiesced {
                locks.drop_maintenance();
                let _ = self.systemctl.run_systemctl(&["daemon-reload"]);
                let _ = self.bootstrap_system();
                let _ = self.start_services(false);
            }
            return Err(err);
        }

        let committed_previous = if release_changed {
            old_current.as_deref()
        } else {
            old_previous.as_deref()
        };

        // Drop `maintenance.lock` BEFORE starting services while STILL holding `deployment.lock`,
        // so the newly started `miro-ego.service` daemon's `recover_on_startup()` never collides
        // with `503 maintenance_busy`!
        locks.drop_maintenance();

        if activate {
            self.activate_or_rollback(
                old_current.as_deref(),
                old_previous.as_deref(),
                committed_previous,
                &new_commit,
                backup_dir.as_deref(),
            )?;
        } else {
            self.finalize_verified_switch(&new_commit, committed_previous)?;
            if let Some(bak) = backup_dir {
                let _ = fs::remove_dir_all(bak);
                let _ = fsync_directory(&self.paths.releases_dir());
            }
        }

        self.status()
    }

    /// Transactionally roll back `current` to `previous`, restoring previous release's unit assets.
    pub fn rollback(&self, activate: bool) -> DeployResult<DeploymentStatus> {
        let mut locks = self.acquire_exclusive_locks()?;
        self.rollback_with_locks(activate, &mut locks)
    }

    pub(crate) fn rollback_with_locks(
        &self,
        activate: bool,
        locks: &mut HeldDeployLocks,
    ) -> DeployResult<DeploymentStatus> {
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
            old_previous: Some(previous.clone()),
            new_current: previous.clone(),
            state: TransactionState::Prepared,
        };
        self.write_transaction(&tx)?;

        let cur_dir = self.paths.releases_dir().join(&current);
        let prev_dir = self.paths.releases_dir().join(&previous);
        let switch_res = (|| -> DeployResult<()> {
            self.sync_assets_between_releases(Some(&cur_dir), &prev_dir)?;
            self.systemctl.run_systemctl(&["daemon-reload"])?;
            self.bootstrap_system()?;
            self.switch_current_for_transaction(&tx)?;
            Ok(())
        })();

        if let Err(err) = switch_res {
            let _ = self.sync_assets_between_releases(Some(&prev_dir), &cur_dir);
            let _ = self.revert_to_previous_links(Some(&current), Some(&previous));
            locks.drop_maintenance();
            let _ = self.systemctl.run_systemctl(&["daemon-reload"]);
            let _ = self.bootstrap_system();
            let _ = self.start_services(false);
            return Err(err);
        }

        // Drop `maintenance.lock` BEFORE starting services while STILL holding `deployment.lock`.
        locks.drop_maintenance();

        let committed_previous = (current != previous).then_some(current.as_str());
        if activate {
            self.activate_or_rollback(
                Some(&current),
                Some(&previous),
                committed_previous,
                &previous,
                None,
            )?;
        } else {
            self.finalize_verified_switch(&previous, committed_previous)?;
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
                // During activation (`start_services(true)`), `miro-ego-deploy` holds
                // `deployment.lock` exclusively while `install-transaction.json` is in `Switched`
                // state awaiting post-activation health verification. Delegate cleanly instead
                // of fighting the active upgrade or prematurely rolling back!
                if let Some(tx) = self.read_transaction()? {
                    if tx.state != TransactionState::Switched {
                        return Err(DeployError::Blocked {
                            message: "deployment_busy: release transaction is uncommitted"
                                .to_owned(),
                        });
                    }
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
        let _locks = self.acquire_exclusive_locks()?;
        self.deactivate_units(true)?;

        let mut extra_tracked_assets = HashSet::new();
        if let Ok(entries) = fs::read_dir(self.paths.releases_dir()) {
            for entry in entries.filter_map(Result::ok) {
                let rel_dir = entry.path();
                for dest in self.collect_release_owned_assets(&rel_dir) {
                    extra_tracked_assets.insert(dest);
                }
            }
        }

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
        let mut removed_set = HashSet::new();
        for asset in DEPLOYMENT_ASSETS {
            if asset.preserve_if_exists {
                continue;
            }
            let target = self.paths.system_root.join(asset.dest_relative);
            if fs::symlink_metadata(&target).is_ok() {
                fs::remove_file(&target).map_err(|err| DeployError::Failed {
                    message: format!("failed removing asset {}: {err}", target.display()),
                })?;
                self.cleanup_empty_dropin_parent(&target);
                removed_set.insert(asset.dest_relative.to_owned());
                removed_assets.push(asset.dest_relative.to_owned());
            }
        }
        let mut sorted_extra: Vec<String> = extra_tracked_assets.into_iter().collect();
        sorted_extra.sort();
        for dest_rel in sorted_extra {
            if removed_set.contains(&dest_rel) || is_preserved_dest_relative(&dest_rel) {
                continue;
            }
            let target = self.paths.system_root.join(&dest_rel);
            if fs::symlink_metadata(&target).is_ok() {
                fs::remove_file(&target).map_err(|err| DeployError::Failed {
                    message: format!("failed removing asset {}: {err}", target.display()),
                })?;
                self.cleanup_empty_dropin_parent(&target);
                removed_assets.push(dest_rel);
            }
        }
        let _ = fs::remove_file(self.paths.run_dir.join("network-control.sock"));
        let _ = fs::remove_file(self.paths.run_dir.join("deploy-control.sock"));
        self.systemctl.run_systemctl(&["daemon-reload"])?;

        Ok(UninstallReport {
            installed: false,
            config_preserved: true,
            state_preserved: true,
            removed_assets,
        })
    }

    fn recover_locked(&self) -> DeployResult<DeploymentStatus> {
        self.ensure_service_ownership_if_root()?;
        let mut assets_restored = false;
        if let Some(tx) = self.read_transaction()? {
            let new_dir = self.paths.releases_dir().join(&tx.new_current);
            match tx.action {
                TransactionAction::Install => {
                    // An uncommitted `Install` transaction on disk (`Prepared` or `Switched`)
                    // never completed post-activation health verification: always revert safely
                    // back to `old_current` + `old_previous`!
                    if let Some(old) = &tx.old_current {
                        let old_dir = self.paths.releases_dir().join(old);
                        if self.release_is_complete(old) {
                            self.sync_assets_between_releases(Some(&new_dir), &old_dir)?;
                            assets_restored = true;
                            self.revert_to_previous_links(
                                Some(old.as_str()),
                                tx.old_previous.as_deref(),
                            )?;
                        } else {
                            self.revert_to_previous_links(None, tx.old_previous.as_deref())?;
                        }
                    } else {
                        self.revert_to_previous_links(None, tx.old_previous.as_deref())?;
                    }
                }
                TransactionAction::Rollback => {
                    let old_dir = tx
                        .old_current
                        .as_ref()
                        .map(|old| self.paths.releases_dir().join(old));
                    if self.release_is_complete(&tx.new_current) {
                        self.sync_assets_between_releases(old_dir.as_deref(), &new_dir)?;
                        assets_restored = true;
                        let next_prev = tx
                            .old_current
                            .as_deref()
                            .filter(|&o| o != tx.new_current.as_str());
                        self.switch_current_for_transaction(&tx)?;
                        self.finalize_verified_switch(&tx.new_current, next_prev)?;
                    } else if let Some(old) = &tx.old_current {
                        let fallback_old_dir = self.paths.releases_dir().join(old);
                        if self.release_is_complete(old) {
                            self.sync_assets_between_releases(Some(&new_dir), &fallback_old_dir)?;
                            assets_restored = true;
                        }
                        self.revert_to_previous_links(
                            Some(old.as_str()),
                            tx.old_previous.as_deref(),
                        )?;
                    } else {
                        self.revert_to_previous_links(None, tx.old_previous.as_deref())?;
                    }
                }
            }
        }

        if assets_restored {
            let _ = self.systemctl.run_systemctl(&["daemon-reload"]);
            let _ = self.bootstrap_system();
        }

        // Remove any orphaned staging or backup directories left by crashed installs.
        if let Ok(entries) = fs::read_dir(self.paths.releases_dir()) {
            for entry in entries.filter_map(Result::ok) {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with('.')
                    && (name.contains(".staging-") || name.contains(".backup-"))
                {
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
        old_previous: Option<&str>,
        committed_previous: Option<&str>,
        new_current: &str,
        backup_dir: Option<&Path>,
    ) -> DeployResult<()> {
        match self.start_services(true) {
            Ok(()) => {
                self.finalize_verified_switch(new_current, committed_previous)?;
                if let Some(bak) = backup_dir {
                    let _ = fs::remove_dir_all(bak);
                    let _ = fsync_directory(&self.paths.releases_dir());
                }
                let _ = fs::remove_file(self.paths.activation_failure_file());
                Ok(())
            }
            Err(activation_err) => {
                let release_changed = old_current != Some(new_current);
                let rolled_back_to = if release_changed || backup_dir.is_some() {
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
                        self.restore_previous_release(Some(new_current), old, old_previous)
                    } else if let Some(bak) = backup_dir {
                        let target_dir = self.paths.releases_dir().join(old);
                        let _ = fs::remove_dir_all(&target_dir);
                        match fs::rename(bak, &target_dir) {
                            Ok(()) => {
                                let _ = fsync_directory(&self.paths.releases_dir());
                                self.restore_previous_release(None, old, old_previous)
                            }
                            Err(err) => Err(DeployError::Failed {
                                message: format!(
                                    "failed restoring backup release {}: {err}",
                                    target_dir.display()
                                ),
                            }),
                        }
                    } else {
                        let deact = self.deactivate_units(false);
                        let rev = self.revert_to_previous_links(Some(old), old_previous);
                        deact.and(rev)
                    }
                } else {
                    let deact = self.deactivate_units(false);
                    let failed_dir = self.paths.releases_dir().join(new_current);
                    for rel_dest in self.collect_release_owned_assets(&failed_dir) {
                        if !is_preserved_dest_relative(&rel_dest) {
                            let target = self.paths.system_root.join(&rel_dest);
                            let _ = fs::remove_file(&target);
                            self.cleanup_empty_dropin_parent(&target);
                        }
                    }
                    let rev = self.revert_to_previous_links(None, old_previous);
                    deact.and(rev)
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
        failed_current: Option<&str>,
        old_current: &str,
        old_previous: Option<&str>,
    ) -> DeployResult<()> {
        self.quiesce_services()?;
        let failed_dir = failed_current.map(|c| self.paths.releases_dir().join(c));
        let old_dir = self.paths.releases_dir().join(old_current);
        self.sync_assets_between_releases(failed_dir.as_deref(), &old_dir)?;
        self.revert_to_previous_links(Some(old_current), old_previous)?;
        self.systemctl.run_systemctl(&["daemon-reload"])?;
        self.bootstrap_system()?;
        self.start_services(false)?;
        Ok(())
    }

    fn stage_release(&self, verified: &VerifiedBundle) -> DeployResult<(PathBuf, Option<PathBuf>)> {
        let releases = self.paths.releases_dir();
        let final_dir = releases.join(verified.commit());
        if final_dir.exists() && self.release_is_complete(verified.commit()) {
            if let Ok(meta) = self.read_release_metadata(verified.commit()) {
                if meta.bundle_sha256 == verified.manifest_sha256 {
                    if !verified.payloads_verified() {
                        let _ = verify_bundle_dir(&verified.root)?;
                    }
                    return Ok((final_dir, None));
                }
            }
        }

        let stage_dir = releases.join(format!(
            ".{}.staging-{}",
            verified.commit(),
            new_uuid_v4().replace('-', "")
        ));
        create_dir_with_mode(&stage_dir, 0o755)?;

        let stage_result = (|| -> DeployResult<Option<PathBuf>> {
            for entry in verified.all_entries() {
                let rel = Path::new(&entry.path);
                let dst = stage_dir.join(&entry.path);
                if let Some(parent) = dst.parent() {
                    ensure_dir_tree_mode(parent, &stage_dir, 0o755)?;
                }
                let is_binary = entry.path.starts_with("bin/");
                let mode = if entry.executable || is_binary {
                    0o755
                } else {
                    0o644
                };
                let _ = copy_and_verify_safe(
                    Some(&verified.root),
                    rel,
                    &dst,
                    mode,
                    Some(entry.bytes),
                    &entry.sha256,
                    is_binary,
                )?;
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
            Ok(backup)
        })();

        match stage_result {
            Ok(backup) => Ok((final_dir, backup)),
            Err(err) => {
                let _ = fs::remove_dir_all(&stage_dir);
                Err(err)
            }
        }
    }

    /// Synchronize system/config assets when switching from `from_release_dir` to `to_release_dir`,
    /// installing `to_release_dir` assets (including dynamic Avahi rendering when `device.json`
    /// exists) and removing any non-preserved unit drop-ins or configs introduced only by
    /// `from_release_dir`.
    fn sync_assets_between_releases(
        &self,
        from_release_dir: Option<&Path>,
        to_release_dir: &Path,
    ) -> DeployResult<()> {
        let from_assets = match from_release_dir {
            Some(dir) => self.collect_release_owned_assets(dir),
            None => HashSet::new(),
        };
        let to_installed = self.install_assets_from_release(to_release_dir)?;
        let to_set: HashSet<String> = to_installed.into_iter().collect();

        for stale_rel in from_assets {
            if to_set.contains(&stale_rel) || is_preserved_dest_relative(&stale_rel) {
                continue;
            }
            let target = self.paths.system_root.join(&stale_rel);
            if fs::symlink_metadata(&target).is_ok() {
                fs::remove_file(&target).map_err(|err| DeployError::Failed {
                    message: format!(
                        "failed removing rolled-back release asset {}: {err}",
                        target.display()
                    ),
                })?;
                self.cleanup_empty_dropin_parent(&target);
            }
        }
        Ok(())
    }

    fn cleanup_empty_dropin_parent(&self, removed_file: &Path) {
        let Some(parent) = removed_file.parent() else {
            return;
        };
        if parent == self.paths.system_root {
            return;
        }
        let is_dropin_dir = parent
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|name| name.ends_with(".d"));
        if is_dropin_dir {
            let _ = fs::remove_dir(parent);
        }
        if let Some(grand) = parent.parent() {
            let _ = fsync_directory(grand);
        }
    }

    fn collect_release_owned_assets(&self, release_dir: &Path) -> HashSet<String> {
        let mut owned = HashSet::new();
        let ledger_path = release_dir.join(INSTALLED_ASSETS_FILENAME);
        if let Ok(manifest) =
            read_bounded_json::<InstalledAssetsManifest>(&ledger_path, MAX_DEPLOY_JSON_BYTES, 8)
        {
            if manifest.schema == INSTALLED_ASSETS_SCHEMA {
                for rel in manifest.installed_dest_paths {
                    if !is_preserved_dest_relative(&rel) {
                        owned.insert(rel);
                    }
                }
            }
        }
        for (_, dest_rel, preserve) in discover_release_asset_mappings(release_dir) {
            if !preserve {
                owned.insert(dest_rel);
            }
        }
        owned
    }

    fn write_installed_assets_ledger(
        &self,
        release_dir: &Path,
        installed_dest_paths: &[String],
    ) -> DeployResult<()> {
        if !release_dir.is_dir() {
            return Ok(());
        }
        let ledger = InstalledAssetsManifest {
            schema: INSTALLED_ASSETS_SCHEMA.to_owned(),
            installed_dest_paths: installed_dest_paths.to_vec(),
        };
        let mut raw = serde_json::to_vec_pretty(&ledger).map_err(|err| DeployError::Failed {
            message: format!("failed serializing {INSTALLED_ASSETS_FILENAME}: {err}"),
        })?;
        raw.push(b'\n');
        write_bytes_atomic(
            &release_dir.join(INSTALLED_ASSETS_FILENAME),
            &raw,
            0o644,
            true,
        )
        .map_err(|err| DeployError::Failed {
            message: format!("failed writing {INSTALLED_ASSETS_FILENAME}: {err}"),
        })
    }

    /// Install systemd units, drop-ins, and scripts from `<release>/deploy/miro-ego/` with explicit
    /// `0755`/`0700`/`0644` permissions (immune to `UMask=0077`), dynamically rendering
    /// `/etc/avahi/services/miro-ego.service` when `device.json` exists in `config_root`.
    fn install_assets_from_release(&self, release_dir: &Path) -> DeployResult<Vec<String>> {
        let deploy_dir = release_dir.join("deploy/miro-ego");
        let dynamic_avahi = self.render_dynamic_avahi_from_device_config();
        let mut installed_dest_paths: Vec<String> = Vec::new();
        let mut seen_dest: HashSet<String> = HashSet::new();

        if deploy_dir.is_dir() {
            let customer_tls = self.is_customer_tls_configured();

            for asset in DEPLOYMENT_ASSETS {
                if asset.source_name == "miro-ego.avahi" && dynamic_avahi.is_some() {
                    continue;
                }
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

                self.write_system_asset(
                    &dest_path,
                    &fs::read(&src_path).map_err(|err| DeployError::Failed {
                        message: format!("failed reading release asset {source_name}: {err}"),
                    })?,
                    asset.mode,
                )?;

                if !asset.preserve_if_exists && seen_dest.insert(asset.dest_relative.to_owned()) {
                    installed_dest_paths.push(asset.dest_relative.to_owned());
                }
            }

            // Also install any additional systemd drop-ins (`<unit>.d/<name>.conf`) or extra
            // `miro-ego-*.{service,socket,timer,path,target}` units shipped in this release.
            for (src_path, dest_rel, preserve) in discover_extra_release_assets(&deploy_dir) {
                if seen_dest.contains(&dest_rel) {
                    continue;
                }
                let dest_path = self.paths.system_root.join(&dest_rel);
                if preserve && fs::symlink_metadata(&dest_path).is_ok() {
                    continue;
                }
                let payload = fs::read(&src_path).map_err(|err| DeployError::Failed {
                    message: format!(
                        "failed reading extra release asset {}: {err}",
                        src_path.display()
                    ),
                })?;
                self.write_system_asset(&dest_path, &payload, 0o644)?;
                if !preserve && seen_dest.insert(dest_rel.clone()) {
                    installed_dest_paths.push(dest_rel);
                }
            }
        }

        if let Some(avahi_xml) = dynamic_avahi {
            let avahi_rel = "etc/avahi/services/miro-ego.service";
            let avahi_dest = self.paths.system_root.join(avahi_rel);
            self.write_system_asset(&avahi_dest, &avahi_xml, 0o644)?;
            if seen_dest.insert(avahi_rel.to_owned()) {
                installed_dest_paths.push(avahi_rel.to_owned());
            }
        }

        installed_dest_paths.sort();
        let _ = self.write_installed_assets_ledger(release_dir, &installed_dest_paths);
        Ok(installed_dest_paths)
    }

    fn write_system_asset(&self, dest_path: &Path, payload: &[u8], mode: u32) -> DeployResult<()> {
        if let Some(parent) = dest_path.parent() {
            let dir_mode = if parent == self.paths.system_root.join("etc/miro-ego") {
                0o750
            } else {
                0o755
            };
            ensure_dir_tree_mode(parent, &self.paths.system_root, dir_mode)?;
        }
        write_bytes_atomic(dest_path, payload, mode, true).map_err(|err| DeployError::Failed {
            message: format!("failed installing asset {}: {err}", dest_path.display()),
        })
    }

    /// Dynamically render `/etc/avahi/services/miro-ego.service` when `device.json` exists in
    /// `config_root`, using `device.device_label`, `listen.port`, and `_miro-ego._tcp` +
    /// `_http._tcp` (or `_https._tcp` for customer TLS).
    fn render_dynamic_avahi_from_device_config(&self) -> Option<Vec<u8>> {
        let cfg_path = self.paths.config_root.join("device.json");
        let val =
            miro_ego_core::sys::fs::read_bounded_json_value(&cfg_path, MAX_DEPLOY_JSON_BYTES, 16)
                .ok()?;
        let obj = val.as_object()?;

        let is_customer = obj
            .get("security")
            .and_then(|s| s.get("profile"))
            .and_then(serde_json::Value::as_str)
            == Some("customer");
        let port = obj
            .get("listen")
            .and_then(|l| l.get("port"))
            .and_then(serde_json::Value::as_u64)
            .and_then(|p| u16::try_from(p).ok())
            .filter(|&p| p > 0)
            .unwrap_or(8080);
        let raw_label = obj
            .get("device")
            .and_then(|d| d.get("device_label"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| {
                !s.is_empty() && s.len() <= 63 && !s.bytes().any(|b| b < 0x20 || b == 0x7f)
            });

        let (replace_wildcards, instance_name) = match raw_label {
            Some(label) => ("no", avahi_xml_escape(label)),
            None => ("yes", "Miro-Ego on %h".to_owned()),
        };
        let (scheme, alias_type) = if is_customer {
            ("https", "_https._tcp")
        } else {
            ("http", "_http._tcp")
        };

        let xml = format!(
            "<?xml version=\"1.0\" standalone='no'?>\n\
             <!DOCTYPE service-group SYSTEM \"avahi-service.dtd\">\n\
             <service-group>\n\
             \x20 <name replace-wildcards=\"{replace_wildcards}\">{instance_name}</name>\n\
             \x20 <service>\n\
             \x20   <type>_miro-ego._tcp</type>\n\
             \x20   <port>{port}</port>\n\
             \x20   <txt-record>scheme={scheme}</txt-record>\n\
             \x20   <txt-record>api=/api/v1/device</txt-record>\n\
             \x20 </service>\n\
             \x20 <service>\n\
             \x20   <type>{alias_type}</type>\n\
             \x20   <port>{port}</port>\n\
             \x20 </service>\n\
             </service-group>\n"
        );
        Some(xml.into_bytes())
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

    fn switch_current_for_transaction(&self, tx: &InstallTransaction) -> DeployResult<()> {
        let _ = self.read_release_metadata(&tx.new_current)?;
        if tx.state == TransactionState::Prepared {
            self.switch_symlink("current", Some(&tx.new_current))?;
            let mut switched = tx.clone();
            switched.state = TransactionState::Switched;
            self.write_transaction(&switched)?;
        } else {
            self.switch_symlink("current", Some(&tx.new_current))?;
        }
        Ok(())
    }

    fn finalize_verified_switch(
        &self,
        new_current: &str,
        previous_target: Option<&str>,
    ) -> DeployResult<()> {
        let normalized_prev = previous_target.filter(|&prev| prev != new_current);
        self.switch_symlink("previous", normalized_prev)?;

        let mut keep = HashSet::new();
        keep.insert(new_current.to_owned());
        if let Some(prev) = normalized_prev {
            keep.insert(prev.to_owned());
        }
        self.prune_releases(&keep)?;

        let _ = fs::remove_file(self.paths.transaction_file());
        let _ = fsync_directory(&self.paths.deploy_state_root);
        Ok(())
    }

    fn revert_to_previous_links(
        &self,
        old_current: Option<&str>,
        old_previous: Option<&str>,
    ) -> DeployResult<()> {
        self.switch_symlink("current", old_current)?;
        let normalized_prev = match (old_current, old_previous) {
            (Some(cur), Some(prev)) if cur == prev => None,
            (_, prev) => prev,
        };
        self.switch_symlink("previous", normalized_prev)?;

        let mut keep = HashSet::new();
        if let Some(cur) = old_current {
            keep.insert(cur.to_owned());
        }
        if let Some(prev) = normalized_prev {
            keep.insert(prev.to_owned());
        }
        self.prune_releases(&keep)?;

        let _ = fs::remove_file(self.paths.transaction_file());
        let _ = fsync_directory(&self.paths.deploy_state_root);
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

    fn bootstrap_system(&self) -> DeployResult<()> {
        let sysusers_conf = self
            .paths
            .system_root
            .join("usr/lib/sysusers.d/miro-ego.conf");
        if sysusers_conf.is_file() {
            let arg = sysusers_conf.to_string_lossy().into_owned();
            self.systemctl
                .run_bootstrap_command("systemd-sysusers", &[&arg])?;
        }

        let tmpfiles_conf = self
            .paths
            .system_root
            .join("usr/lib/tmpfiles.d/miro-ego.conf");
        if tmpfiles_conf.is_file() {
            let arg = tmpfiles_conf.to_string_lossy().into_owned();
            self.systemctl
                .run_bootstrap_command("systemd-tmpfiles", &["--create", &arg])?;
        }

        self.ensure_service_ownership_if_root()?;

        let udev_rules = self
            .paths
            .system_root
            .join("etc/udev/rules.d/90-miro-ego-leds.rules");
        if udev_rules.is_file() {
            self.systemctl
                .run_bootstrap_command("udevadm", &["control", "--reload-rules"])?;
            let _ = self
                .systemctl
                .run_bootstrap_command("udevadm", &["trigger", "--subsystem-match=leds"]);
        }

        let device_login = self
            .paths
            .system_root
            .join("usr/local/sbin/miro-ego-device-login");
        if device_login.is_file() {
            let prog = device_login.to_string_lossy().into_owned();
            self.systemctl.run_bootstrap_command(&prog, &[])?;
        }
        Ok(())
    }

    fn resolve_system_path(&self, raw: &Path) -> PathBuf {
        if self.paths.system_root != Path::new("/") {
            if let Ok(rel) = raw.strip_prefix("/") {
                return self.paths.system_root.join(rel);
            }
        }
        raw.to_path_buf()
    }

    fn ensure_service_ownership_if_root(&self) -> DeployResult<()> {
        create_dir_with_mode(&self.paths.config_root, 0o750)?;
        create_dir_with_mode(&self.paths.state_root, 0o750)?;
        create_dir_with_mode(&self.paths.deploy_state_root, 0o750)?;
        create_dir_with_mode(&self.paths.run_dir, 0o750)?;

        if unsafe { libc::geteuid() } != 0 {
            return Ok(());
        }
        chown_path(&self.paths.deploy_state_root, 0, 0);
        let (Some(uid), Some(gid)) = lookup_miro_ego_uid_gid() else {
            return Ok(());
        };
        chown_path(&self.paths.config_root, 0, gid);
        for name in [
            "device.json",
            "settings.json",
            "button.json",
            "network.json",
            "bearer.token",
            "calibration.json",
        ] {
            let p = self.paths.config_root.join(name);
            if fs::symlink_metadata(&p).is_ok_and(|m| !m.file_type().is_symlink()) {
                chown_path(&p, 0, gid);
            }
        }
        chown_subdirs_nofollow(&self.paths.config_root, 0, gid);

        let device_cfg_path = self.paths.config_root.join("device.json");
        for referenced in collect_device_config_referenced_paths(&device_cfg_path) {
            let resolved = self.resolve_system_path(&referenced);
            if fs::symlink_metadata(&resolved)
                .is_ok_and(|m| !m.file_type().is_symlink() && m.file_type().is_file())
            {
                chown_path(&resolved, 0, gid);
            }
        }

        chown_path(&self.paths.state_root, uid, gid);
        chown_regular_tree_nofollow(&self.paths.state_root, uid, gid);
        chown_path(&self.paths.run_dir, 0, gid);
        Ok(())
    }

    fn unit_exists(&self, unit: &str) -> DeployResult<bool> {
        Ok(self.systemctl.unit_load_state(unit)? != "not-found")
    }

    fn quiesce_services(&self) -> DeployResult<()> {
        let mut disabled_timer = false;
        let mut stopped_units: Vec<&str> = Vec::new();

        let res = (|| -> DeployResult<()> {
            if self.unit_exists(WATCHDOG_TIMER_UNIT)? {
                self.systemctl
                    .run_systemctl(&["disable", "--now", WATCHDOG_TIMER_UNIT])?;
                disabled_timer = true;
            }
            for unit in [
                WATCHDOG_SERVICE_UNIT,
                "miro-ego.service",
                "miro-ego-network.socket",
                NETWORK_SERVICE_UNIT,
            ] {
                if self.unit_exists(unit)? {
                    self.systemctl.run_systemctl(&["stop", unit])?;
                    stopped_units.push(unit);
                }
            }
            Ok(())
        })();

        if let Err(err) = res {
            for unit in stopped_units.into_iter().rev() {
                if unit == WATCHDOG_SERVICE_UNIT {
                    continue;
                }
                let _ = self.systemctl.run_systemctl(&["start", unit]);
            }
            if disabled_timer {
                let _ = self
                    .systemctl
                    .run_systemctl(&["enable", "--now", WATCHDOG_TIMER_UNIT]);
            }
            return Err(err);
        }
        Ok(())
    }

    fn start_services(&self, check_health: bool) -> DeployResult<()> {
        self.systemctl
            .run_systemctl(&["try-restart", "systemd-journald.service"])?;
        if self.unit_exists("logrotate.timer")? {
            self.systemctl
                .run_systemctl(&["enable", "--now", "logrotate.timer"])?;
        }
        if self.unit_exists(DEPLOY_SOCKET_UNIT)? {
            self.systemctl
                .run_systemctl(&["enable", "--now", DEPLOY_SOCKET_UNIT])?;
        }
        for unit in CORE_UNITS {
            self.systemctl.run_systemctl(&["enable", "--now", unit])?;
        }
        if check_health {
            self.health.check_health()?;
        }
        if self.unit_exists(BUTTON_UNIT)? {
            let button_enabled = self.button_config_enabled();
            let button_action = if button_enabled { "enable" } else { "disable" };
            self.systemctl
                .run_systemctl(&[button_action, "--now", BUTTON_UNIT])?;
        }
        if self.unit_exists(WATCHDOG_TIMER_UNIT)? {
            self.systemctl
                .run_systemctl(&["enable", "--now", WATCHDOG_TIMER_UNIT])?;
        }
        Ok(())
    }

    fn deactivate_units(&self, include_deploy_socket: bool) -> DeployResult<()> {
        for unit in [
            WATCHDOG_TIMER_UNIT,
            BUTTON_UNIT,
            "miro-ego.service",
            "miro-ego-network.socket",
        ] {
            if self.unit_exists(unit)? {
                self.systemctl.run_systemctl(&["disable", "--now", unit])?;
            }
        }
        if include_deploy_socket && self.unit_exists(DEPLOY_SOCKET_UNIT)? {
            self.systemctl
                .run_systemctl(&["disable", "--now", DEPLOY_SOCKET_UNIT])?;
        }
        for unit in [WATCHDOG_SERVICE_UNIT, NETWORK_SERVICE_UNIT, RECOVER_UNIT] {
            if self.unit_exists(unit)? {
                self.systemctl.run_systemctl(&["stop", unit])?;
            }
        }
        if self.unit_exists("miro-ego-data-volume.service")? {
            self.systemctl
                .run_systemctl(&["disable", "--now", "miro-ego-data-volume.service"])?;
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
        create_dir_with_mode(&self.paths.deploy_state_root, 0o750)?;
        create_dir_with_mode(&self.paths.run_dir, 0o750)?;
        Ok(())
    }

    fn acquire_exclusive_locks(&self) -> DeployResult<HeldDeployLocks> {
        let deploy = acquire_flock(self.paths.deployment_lock_file(), LockMode::Exclusive, true)
            .map_err(map_lock_err)?;
        let maint = acquire_maintenance_lock(&self.paths.run_dir, LockMode::Exclusive, true)
            .map_err(map_lock_err)?;
        Ok(HeldDeployLocks::new(deploy, maint))
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
        if let Some(old_prev) = &tx.old_previous {
            validate_commit_sha(old_prev)?;
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

fn is_preserved_dest_relative(dest_rel: &str) -> bool {
    DEPLOYMENT_ASSETS
        .iter()
        .any(|a| a.dest_relative == dest_rel && a.preserve_if_exists)
}

fn discover_release_asset_mappings(release_dir: &Path) -> Vec<(PathBuf, String, bool)> {
    let deploy_dir = release_dir.join("deploy/miro-ego");
    if !deploy_dir.is_dir() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for asset in DEPLOYMENT_ASSETS {
        let src = deploy_dir.join(asset.source_name);
        let alt_avahi = asset.source_name == "miro-ego.avahi"
            && deploy_dir.join("miro-ego-customer.avahi").is_file();
        if src.is_file() || alt_avahi {
            out.push((
                src,
                asset.dest_relative.to_owned(),
                asset.preserve_if_exists,
            ));
        }
    }
    out.extend(discover_extra_release_assets(&deploy_dir));
    out
}

fn discover_extra_release_assets(deploy_dir: &Path) -> Vec<(PathBuf, String, bool)> {
    let known_sources: HashSet<&str> = DEPLOYMENT_ASSETS
        .iter()
        .map(|a| a.source_name)
        .chain(std::iter::once("miro-ego-customer.avahi"))
        .collect();
    let mut extras = Vec::new();
    let Ok(entries) = fs::read_dir(deploy_dir) else {
        return extras;
    };
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        let Ok(meta) = fs::symlink_metadata(&path) else {
            continue;
        };
        if meta.file_type().is_symlink() {
            continue;
        }
        if meta.file_type().is_file() {
            if known_sources.contains(name.as_str()) {
                continue;
            }
            if name.starts_with("miro-ego-")
                && [".service", ".socket", ".timer", ".path", ".target"]
                    .iter()
                    .any(|ext| name.ends_with(ext))
            {
                extras.push((path, format!("usr/lib/systemd/system/{name}"), false));
            }
        } else if meta.file_type().is_dir() && crate::bundle::is_allowed_systemd_dropin_dir(&name) {
            if let Ok(sub_entries) = fs::read_dir(&path) {
                for sub in sub_entries.filter_map(Result::ok) {
                    let sub_path = sub.path();
                    let sub_name = sub.file_name().to_string_lossy().into_owned();
                    let Ok(sub_meta) = fs::symlink_metadata(&sub_path) else {
                        continue;
                    };
                    if !sub_meta.file_type().is_symlink()
                        && sub_meta.file_type().is_file()
                        && sub_name.ends_with(".conf")
                        && !sub_name.contains('/')
                    {
                        extras.push((
                            sub_path,
                            format!("etc/systemd/system/{name}/{sub_name}"),
                            false,
                        ));
                    }
                }
            }
        }
    }
    extras
}

fn avahi_xml_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            other => out.push(other),
        }
    }
    out
}

fn map_lock_err(err: miro_ego_core::CoreError) -> DeployError {
    match err {
        miro_ego_core::CoreError::LockBusy { message } => DeployError::Blocked { message },
        other => DeployError::Core(other),
    }
}

fn collect_device_config_referenced_paths(device_cfg_path: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(val) =
        miro_ego_core::sys::fs::read_bounded_json_value(device_cfg_path, MAX_DEPLOY_JSON_BYTES, 16)
    else {
        return out;
    };
    if let Some(cal) = val
        .get("camera")
        .and_then(|c| c.get("calibration_file"))
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
    {
        out.push(PathBuf::from(cal));
    }
    if let Some(sec) = val.get("security").and_then(serde_json::Value::as_object) {
        for key in [
            "bearer_token_file",
            "tls_certificate_file",
            "tls_private_key_file",
            "tls_ca_file",
            "ca_certificate_file",
        ] {
            if let Some(p) = sec
                .get(key)
                .and_then(serde_json::Value::as_str)
                .filter(|s| !s.is_empty())
            {
                out.push(PathBuf::from(p));
            }
        }
    }
    out
}

fn chown_subdirs_nofollow(root: &Path, uid: u32, gid: u32) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.filter_map(Result::ok) {
        let p = entry.path();
        if let Ok(meta) = fs::symlink_metadata(&p) {
            if !meta.file_type().is_symlink() && meta.file_type().is_dir() {
                chown_path(&p, uid, gid);
                chown_subdirs_nofollow(&p, uid, gid);
            }
        }
    }
}

fn chown_regular_tree_nofollow(root: &Path, uid: u32, gid: u32) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.filter_map(Result::ok) {
        let p = entry.path();
        if let Ok(meta) = fs::symlink_metadata(&p) {
            if meta.file_type().is_symlink() {
                continue;
            }
            if meta.file_type().is_file() {
                chown_path(&p, uid, gid);
            } else if meta.file_type().is_dir() {
                chown_path(&p, uid, gid);
                chown_regular_tree_nofollow(&p, uid, gid);
            }
        }
    }
}

fn lookup_uid_r(name_c: &CString) -> Option<u32> {
    let init_len = match unsafe { libc::sysconf(libc::_SC_GETPW_R_SIZE_MAX) } {
        n if n > 0 => (n as usize).clamp(1024, 65_536),
        _ => 4096,
    };
    let mut buf = vec![0u8; init_len];
    loop {
        let mut pwd = std::mem::MaybeUninit::<libc::passwd>::uninit();
        let mut result: *mut libc::passwd = std::ptr::null_mut();
        let rc = unsafe {
            libc::getpwnam_r(
                name_c.as_ptr(),
                pwd.as_mut_ptr(),
                buf.as_mut_ptr().cast::<libc::c_char>(),
                buf.len(),
                &raw mut result,
            )
        };
        if rc == libc::ERANGE && buf.len() < 1024 * 1024 {
            buf.resize(buf.len() * 2, 0);
            continue;
        }
        if rc == 0 && !result.is_null() {
            let pwd = unsafe { pwd.assume_init() };
            return Some(pwd.pw_uid);
        }
        return None;
    }
}

fn lookup_gid_r(name_c: &CString) -> Option<u32> {
    let init_len = match unsafe { libc::sysconf(libc::_SC_GETGR_R_SIZE_MAX) } {
        n if n > 0 => (n as usize).clamp(1024, 65_536),
        _ => 4096,
    };
    let mut buf = vec![0u8; init_len];
    loop {
        let mut grp = std::mem::MaybeUninit::<libc::group>::uninit();
        let mut result: *mut libc::group = std::ptr::null_mut();
        let rc = unsafe {
            libc::getgrnam_r(
                name_c.as_ptr(),
                grp.as_mut_ptr(),
                buf.as_mut_ptr().cast::<libc::c_char>(),
                buf.len(),
                &raw mut result,
            )
        };
        if rc == libc::ERANGE && buf.len() < 1024 * 1024 {
            buf.resize(buf.len() * 2, 0);
            continue;
        }
        if rc == 0 && !result.is_null() {
            let grp = unsafe { grp.assume_init() };
            return Some(grp.gr_gid);
        }
        return None;
    }
}

pub(crate) fn lookup_miro_ego_uid_gid() -> (Option<u32>, Option<u32>) {
    let Ok(name_c) = CString::new("miro-ego") else {
        return (None, None);
    };
    (lookup_uid_r(&name_c), lookup_gid_r(&name_c))
}

fn chown_path(path: &Path, uid: u32, gid: u32) {
    if let Ok(path_c) = CString::new(path.as_os_str().as_bytes()) {
        unsafe {
            let _ = libc::fchownat(
                libc::AT_FDCWD,
                path_c.as_ptr(),
                uid,
                gid,
                libc::AT_SYMLINK_NOFOLLOW,
            );
        }
    }
}

fn create_dir_with_mode(path: &Path, mode: u32) -> DeployResult<()> {
    let existed = fs::symlink_metadata(path).is_ok();
    fs::create_dir_all(path).map_err(|err| DeployError::Failed {
        message: format!("failed creating directory {}: {err}", path.display()),
    })?;
    let dir_file = open_directory_safe(path).map_err(|err| DeployError::Failed {
        message: format!("directory {} is unsafe or a symlink: {err}", path.display()),
    })?;
    if !existed || mode == 0o750 {
        dir_file
            .set_permissions(fs::Permissions::from_mode(mode))
            .map_err(|err| DeployError::Failed {
                message: format!("failed chmod directory {}: {err}", path.display()),
            })?;
    }
    Ok(())
}

/// Ensure every directory between `stop_root` and `dir` exists using `open_directory_safe` /
/// `openat(O_DIRECTORY | O_NOFOLLOW)`, applying explicit mode ONLY to newly created directories.
fn ensure_dir_tree_mode(dir: &Path, stop_root: &Path, leaf_mode: u32) -> DeployResult<()> {
    let rel = dir
        .strip_prefix(stop_root)
        .map_err(|_| DeployError::Failed {
            message: format!(
                "directory {} is not under root {}",
                dir.display(),
                stop_root.display()
            ),
        })?;
    let mut cur_dir: File = open_directory_safe(stop_root).map_err(|err| DeployError::Failed {
        message: format!(
            "root directory {} is unsafe or a symlink: {err}",
            stop_root.display()
        ),
    })?;

    let comps: Vec<_> = rel
        .components()
        .filter(|c| !matches!(c, Component::CurDir))
        .collect();
    for (idx, comp) in comps.iter().enumerate() {
        let Component::Normal(seg) = comp else {
            return Err(DeployError::Failed {
                message: format!("invalid directory component in {}", dir.display()),
            });
        };
        let seg_c = CString::new(seg.as_bytes()).map_err(|_| DeployError::Failed {
            message: format!("directory name contains NUL byte: {}", dir.display()),
        })?;
        let is_leaf = idx + 1 == comps.len();
        let mode = if is_leaf { leaf_mode } else { 0o755 };
        let dir_flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
        cur_dir = match openat_retry(cur_dir.as_raw_fd(), &seg_c, dir_flags) {
            Ok(existing) => existing,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let mut created = false;
                // SAFETY: `cur_dir` is an open directory fd and `seg_c` is NUL-terminated.
                if unsafe { libc::mkdirat(cur_dir.as_raw_fd(), seg_c.as_ptr(), mode) } == 0 {
                    created = true;
                } else {
                    let mk_err = std::io::Error::last_os_error();
                    if mk_err.kind() != std::io::ErrorKind::AlreadyExists {
                        return Err(DeployError::Failed {
                            message: format!(
                                "failed creating directory {}: {mk_err}",
                                dir.display()
                            ),
                        });
                    }
                }
                let next_dir =
                    openat_retry(cur_dir.as_raw_fd(), &seg_c, dir_flags).map_err(|open_err| {
                        DeployError::Failed {
                            message: format!(
                                "directory {} is unsafe or a symlink: {open_err}",
                                dir.display()
                            ),
                        }
                    })?;
                if created {
                    next_dir
                        .set_permissions(fs::Permissions::from_mode(mode))
                        .map_err(|chmod_err| DeployError::Failed {
                            message: format!(
                                "failed chmod newly created directory {}: {chmod_err}",
                                dir.display()
                            ),
                        })?;
                }
                next_dir
            }
            Err(err) => {
                return Err(DeployError::Failed {
                    message: format!("directory {} is unsafe or a symlink: {err}", dir.display()),
                });
            }
        };
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
        let paths = ReleaseManagerPaths::for_system_root(&root);
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

        // Build v2 bundle (including a v2-only drop-in!) and make health check FAIL on v2 activation!
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
                (
                    "miro-ego-idle-fstrim.conf",
                    b"[Service]\nExecStart=\n",
                    false,
                ),
                (
                    "deploy/miro-ego/miro-ego.service.d/10-v2-only.conf",
                    b"[Service]\nEnvironment=V2_ONLY=1\n",
                    false,
                ),
            ],
        );
        health.healthy.store(false, Ordering::SeqCst);
        let _ = sysctl.take_commands();

        let err = mgr.install(&b2, true).unwrap_err();
        assert!(err.to_string().contains("automatically rolled back"));

        // Current MUST be back on v1, previous MUST remain None (never point to failed v2!),
        // v1's unit file restored, v2-only drop-ins removed, daemon-reload ran, activation failure
        // recorded, and user button.json preserved!
        let after = mgr.status().unwrap();
        assert_eq!(after.current.as_deref(), Some(commit_v1.as_str()));
        assert_eq!(after.previous, None);
        assert_eq!(after.previous_version, None);
        assert!(!after.transaction_pending);
        assert!(after.activation_failure.is_some());
        assert!(fs::read_to_string(&unit_path).unwrap().contains("v1"));
        assert!(
            !root
                .join("etc/systemd/system/fstrim.service.d/50-miro-ego-idle.conf")
                .exists(),
            "v2-only canonical drop-in must be removed on rollback to v1"
        );
        assert!(
            !root
                .join("etc/systemd/system/miro-ego.service.d/10-v2-only.conf")
                .exists(),
            "v2-only extra unit drop-in must be removed on rollback to v1"
        );
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
    fn crash_transaction_recovery_reverts_unverified_installs_and_settles_rollbacks() {
        let (root, paths) = temp_env("crash-recover");
        let sysctl = MockSystemctl::new();
        let health = Arc::new(ToggleHealth {
            healthy: AtomicBool::new(true),
        });
        let mgr = ReleaseManager::new(paths.clone(), sysctl.clone(), health);

        let commit_v0 = "0".repeat(40);
        let commit_v1 = "a".repeat(40);
        let commit_v2 = "b".repeat(40);
        let b0 = root.join("bundles/v0");
        let b1 = root.join("bundles/v1");
        let b2 = root.join("bundles/v2");
        build_valid_bundle(
            &b0,
            "0.2.9",
            &commit_v0,
            &[("miro-ego.service", b"[Unit]\nDescription=v0\n", false)],
        );
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

        // Install v0 then v1 normally so current=v1, previous=v0, then stage v2 and simulate crash
        // during an unverified `Install` (`Prepared` or `Switched` before health check passed)!
        mgr.install(&b0, false).unwrap();
        mgr.install(&b1, false).unwrap();
        assert_eq!(
            mgr.status().unwrap().previous_version.as_deref(),
            Some("0.2.9")
        );

        let v2_verified = verify_bundle_dir(&b2).unwrap();
        mgr.stage_release(&v2_verified).unwrap();
        fs::create_dir_all(paths.releases_dir().join(".deadbeef.staging-orphan")).unwrap();

        let crashed_tx = InstallTransaction {
            schema: INSTALL_TRANSACTION_SCHEMA.to_owned(),
            transaction_id: new_uuid_v4(),
            action: TransactionAction::Install,
            old_current: Some(commit_v1.clone()),
            old_previous: Some(commit_v0.clone()),
            new_current: commit_v2.clone(),
            state: TransactionState::Switched,
        };
        mgr.switch_symlink("current", Some(&commit_v2)).unwrap();
        mgr.write_transaction(&crashed_tx).unwrap();
        assert!(mgr.status().unwrap().transaction_pending);

        // Boot recovery MUST revert unverified Install back to old_current (v1) + old_previous (v0)!
        let recovered = mgr.recover(false).unwrap();
        assert!(!recovered.transaction_pending);
        assert_eq!(recovered.current.as_deref(), Some(commit_v1.as_str()));
        assert_eq!(recovered.previous.as_deref(), Some(commit_v0.as_str()));
        assert_eq!(recovered.previous_version.as_deref(), Some("0.2.9"));
        assert!(
            !paths
                .releases_dir()
                .join(".deadbeef.staging-orphan")
                .exists()
        );
        assert!(
            fs::read_to_string(root.join("usr/lib/systemd/system/miro-ego.service"))
                .unwrap()
                .contains("v1")
        );

        // Interrupted Rollback (v1 -> v0) settles cleanly to known-good v0!
        let rollback_tx = InstallTransaction {
            schema: INSTALL_TRANSACTION_SCHEMA.to_owned(),
            transaction_id: new_uuid_v4(),
            action: TransactionAction::Rollback,
            old_current: Some(commit_v1.clone()),
            old_previous: Some(commit_v0.clone()),
            new_current: commit_v0.clone(),
            state: TransactionState::Prepared,
        };
        mgr.write_transaction(&rollback_tx).unwrap();
        let settled_rb = mgr.recover(false).unwrap();
        assert!(!settled_rb.transaction_pending);
        assert_eq!(settled_rb.current.as_deref(), Some(commit_v0.as_str()));
        assert_eq!(settled_rb.previous.as_deref(), Some(commit_v1.as_str()));

        // Explicit rollback from v0 -> v1 via FirmwareManager trait!
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

    #[test]
    fn stages_voice_assets_runs_bootstrap_and_probes_customer_tls_health() {
        use miro_ego_core::config::{
            AudioConfig, CameraConfig, DEVICE_CONFIG_SCHEMA, DeviceConfig, DeviceIdentity,
            EncodingPreset, ListenConfig, RecordingEncoding, SecurityConfig, StorageConfig,
        };
        use rustls::pki_types::PrivateKeyDer;
        use rustls::{ServerConfig, ServerConnection};
        use std::net::TcpListener;

        let (root, paths) = temp_env("tls-bootstrap");
        fs::create_dir_all(&paths.config_root).unwrap();

        // Generate self-signed cert via rcgen without 127.0.0.1 SAN (proving pinned DER cert
        // verification succeeds despite hostname mismatch, while wrong cert is rejected!).
        let certified = rcgen::generate_simple_self_signed(vec!["miro-ego-device.local".into()])
            .expect("rcgen self-signed");
        let cert_pem = certified.cert.pem();
        let key_pem = certified.key_pair.serialize_pem();

        let other_cert = rcgen::generate_simple_self_signed(vec!["attacker.local".into()]).unwrap();
        let wrong_cert_pem = other_cert.cert.pem();

        let cert_file = paths.config_root.join("tls-cert.pem");
        let key_file = paths.config_root.join("tls-key.pem");
        let tok_file = paths.config_root.join("bearer.token");
        fs::write(&cert_file, cert_pem.as_bytes()).unwrap();
        fs::write(&key_file, key_pem.as_bytes()).unwrap();
        fs::write(&tok_file, b"secret-customer-token-99\n").unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let bound_port = listener.local_addr().unwrap().port();

        let dev_cfg = DeviceConfig {
            schema: DEVICE_CONFIG_SCHEMA.to_owned(),
            listen: ListenConfig {
                host: "0.0.0.0".to_owned(),
                port: bound_port,
            },
            camera: CameraConfig {
                device: PathBuf::from("/dev/video0"),
                width: 3840,
                height: 1080,
                fps: 60,
                frame_decimation: 2,
                exposure_time_absolute: None,
                calibration_file: None,
            },
            storage: StorageConfig {
                mountpoint: PathBuf::from("/data"),
                minimum_available_bytes: 1024,
                minimum_available_inodes: 64,
            },
            state_root: paths.state_root.clone(),
            device: DeviceIdentity {
                device_id: new_uuid_v4(),
                device_label: "Miro-Ego-ABCD1234".to_owned(),
                hardware_fingerprint: format!("sha256:{}", "f".repeat(64)),
            },
            security: SecurityConfig::Customer {
                isolated_network: true,
                bearer_token_file: tok_file,
                tls_certificate_file: cert_file,
                tls_private_key_file: key_file,
                principal_id: "customer-admin".to_owned(),
            },
            audio: AudioConfig::default(),
            recording: RecordingEncoding::from_preset(EncodingPreset::Efficient),
            segment_seconds: 5.0,
        };
        let cfg_path = paths.config_root.join("device.json");
        fs::write(&cfg_path, serde_json::to_vec_pretty(&dev_cfg).unwrap()).unwrap();

        // Spawn a minimal rustls HTTPS server requiring `Authorization: Bearer secret-customer-token-99`.
        let cert_der = certified.cert.der().clone();
        let key_der = PrivateKeyDer::try_from(certified.key_pair.serialize_der()).unwrap();
        let srv_cfg = Arc::new(
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(vec![cert_der], key_der)
                .unwrap(),
        );
        listener.set_nonblocking(true).unwrap();
        let stop_srv = Arc::new(AtomicBool::new(false));
        let stop_srv_loop = Arc::clone(&stop_srv);
        let srv_handle = std::thread::spawn(move || {
            while !stop_srv_loop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((tcp, _)) => {
                        let _ = tcp.set_nonblocking(false);
                        let _ = tcp.set_read_timeout(Some(Duration::from_millis(500)));
                        let _ = tcp.set_write_timeout(Some(Duration::from_millis(500)));
                        let conn = ServerConnection::new(Arc::clone(&srv_cfg)).unwrap();
                        let mut tls = StreamOwned::new(conn, tcp);
                        let mut buf = [0u8; 2048];
                        if let Ok(n) = tls.read(&mut buf) {
                            let req = String::from_utf8_lossy(&buf[..n]);
                            if req.contains("Authorization: Bearer secret-customer-token-99\r\n") {
                                let _ = tls.write_all(
                                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                                );
                            } else {
                                let _ = tls.write_all(
                                    b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                                );
                            }
                            let _ = tls.flush();
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });

        // Wrong pinned cert MUST fail verification!
        let bad_checker = LocalHttpHealthChecker::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), bound_port),
            Duration::from_millis(300),
        )
        .with_tls_and_bearer(
            wrong_cert_pem.as_bytes(),
            Some("secret-customer-token-99".to_owned()),
        )
        .unwrap();
        assert!(bad_checker.check_health().is_err());

        // `from_device_config_path` loads Customer cert + Bearer token + custom port and succeeds!
        let good_checker = Arc::new(
            LocalHttpHealthChecker::from_device_config_path(
                &cfg_path,
                "127.0.0.1:8080".parse().unwrap(),
                Duration::from_secs(3),
            )
            .unwrap(),
        );

        struct RecordingBootstrapSystemctl {
            bootstrap_calls: Mutex<Vec<(String, Vec<String>)>>,
        }
        impl SystemctlRunner for RecordingBootstrapSystemctl {
            fn run_systemctl(&self, _args: &[&str]) -> DeployResult<()> {
                Ok(())
            }
            fn unit_load_state(&self, unit: &str) -> DeployResult<String> {
                if unit == RECOVER_UNIT {
                    Ok("not-found".to_owned())
                } else {
                    Ok("loaded".to_owned())
                }
            }
            fn run_bootstrap_command(&self, program: &str, args: &[&str]) -> DeployResult<()> {
                self.bootstrap_calls.lock().unwrap().push((
                    program.to_owned(),
                    args.iter().map(|s| (*s).to_owned()).collect(),
                ));
                Ok(())
            }
        }

        let sysctl = Arc::new(RecordingBootstrapSystemctl {
            bootstrap_calls: Mutex::new(Vec::new()),
        });
        let mgr = ReleaseManager::new(paths.clone(), sysctl.clone(), good_checker);
        let commit = "9".repeat(40);
        let bundle_dir = root.join("bundles/v-voice");
        build_valid_bundle(
            &bundle_dir,
            "0.3.2",
            &commit,
            &[
                ("miro-ego.sysusers", b"u miro-ego - \"Miro-Ego\"\n", false),
                (
                    "miro-ego.tmpfiles",
                    b"d /run/miro-ego 0750 root miro-ego -\n",
                    false,
                ),
                ("90-miro-ego-leds.rules", b"SUBSYSTEM==\"leds\"\n", false),
                ("miro-ego-device-login", b"#!/bin/sh\nexit 0\n", true),
                (
                    "miro-ego-deploy.socket",
                    b"[Socket]\nListenStream=/run/miro-ego/deploy-control.sock\n",
                    false,
                ),
                (
                    "miro-ego-deploy.service",
                    b"[Service]\nExecStart=/opt/miro-ego/current/bin/miro-ego\n",
                    false,
                ),
                (
                    "share/voice/recording_started.wav",
                    b"RIFF$\x00\x00\x00WAVE",
                    false,
                ),
            ],
        );

        let st = mgr.install(&bundle_dir, true).unwrap();
        assert_eq!(st.current.as_deref(), Some(commit.as_str()));
        assert!(
            paths
                .releases_dir()
                .join(&commit)
                .join("share/voice/recording_started.wav")
                .is_file()
        );
        assert!(
            root.join("etc/udev/rules.d/90-miro-ego-leds.rules")
                .is_file()
        );
        assert!(
            root.join("usr/lib/systemd/system/miro-ego-deploy.socket")
                .is_file()
        );
        assert!(
            root.join("usr/lib/systemd/system/miro-ego-deploy.service")
                .is_file()
        );

        let calls = sysctl.bootstrap_calls.lock().unwrap();
        assert!(calls.iter().any(|(p, _)| p == "systemd-sysusers"));
        assert!(calls.iter().any(|(p, _)| p == "systemd-tmpfiles"));
        assert!(calls.iter().any(|(p, _)| p == "udevadm"));
        assert!(
            calls
                .iter()
                .any(|(p, _)| p.ends_with("miro-ego-device-login"))
        );

        stop_srv.store(true, Ordering::Release);
        srv_handle.join().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn quiesce_omits_recover_unit_restarts_on_partial_failure_prunes_failed_release_and_keeps_deploy_socket()
     {
        let (root, paths) = temp_env("p0-01-p1-22");
        let fail_network_stop = Arc::new(AtomicBool::new(false));
        let fail_flag = Arc::clone(&fail_network_stop);

        struct SelectiveFailSystemctl {
            commands: Mutex<Vec<Vec<String>>>,
            fail_network_stop: Arc<AtomicBool>,
        }
        impl SystemctlRunner for SelectiveFailSystemctl {
            fn run_systemctl(&self, args: &[&str]) -> DeployResult<()> {
                self.commands
                    .lock()
                    .unwrap()
                    .push(args.iter().map(|s| (*s).to_owned()).collect());
                if self.fail_network_stop.load(Ordering::SeqCst)
                    && args == ["stop", NETWORK_SERVICE_UNIT]
                {
                    return Err(DeployError::Failed {
                        message: "simulated systemctl stop failure".to_owned(),
                    });
                }
                Ok(())
            }
            fn unit_load_state(&self, _unit: &str) -> DeployResult<String> {
                Ok("loaded".to_owned())
            }
        }

        let sysctl = Arc::new(SelectiveFailSystemctl {
            commands: Mutex::new(Vec::new()),
            fail_network_stop: fail_flag,
        });
        let health = Arc::new(ToggleHealth {
            healthy: AtomicBool::new(false),
        });
        let mgr = ReleaseManager::new(paths.clone(), sysctl.clone(), health.clone());

        let commit_v1 = "1".repeat(40);
        let commit_v2 = "2".repeat(40);
        let b1 = root.join("bundles/v1");
        let b2 = root.join("bundles/v2");
        build_valid_bundle(&b1, "0.3.0", &commit_v1, &[]);
        build_valid_bundle(&b2, "0.3.1", &commit_v2, &[]);

        // First-install activation failure:
        // 1) MUST prune failed candidate `releases/<commit_v1>` (no 512 MiB leak!)
        // 2) MUST NOT disable `miro-ego-deploy.socket`!
        assert!(mgr.install(&b1, true).is_err());
        assert!(!paths.releases_dir().join(&commit_v1).exists());
        {
            let cmds = std::mem::take(&mut *sysctl.commands.lock().unwrap());
            assert!(
                !cmds
                    .iter()
                    .any(|c| c == &["disable", "--now", DEPLOY_SOCKET_UNIT]),
                "activate_or_rollback must never disable miro-ego-deploy.socket"
            );
        }

        // Now succeed installing v1, verify run_dir mode is 0o750!
        health.healthy.store(true, Ordering::SeqCst);
        mgr.install(&b1, true).unwrap();
        assert_eq!(
            fs::symlink_metadata(&paths.run_dir)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o750
        );

        // Quiesce failure partway (after stopping miro-ego.service & network.socket) MUST:
        // - never invoke `stop miro-ego-recover.service`
        // - best-effort restart already-stopped units (`miro-ego-network.socket`, `miro-ego.service`)!
        sysctl.commands.lock().unwrap().clear();
        fail_network_stop.store(true, Ordering::SeqCst);
        assert!(mgr.install(&b2, true).is_err());
        {
            let cmds = std::mem::take(&mut *sysctl.commands.lock().unwrap());
            assert!(
                !cmds.iter().any(|c| c == &["stop", RECOVER_UNIT]),
                "quiesce_services must never stop miro-ego-recover.service"
            );
            assert!(
                cmds.iter()
                    .any(|c| c == &["start", "miro-ego-network.socket"])
            );
            assert!(cmds.iter().any(|c| c == &["start", "miro-ego.service"]));
            assert!(
                cmds.iter()
                    .any(|c| c == &["enable", "--now", WATCHDOG_TIMER_UNIT])
            );
        }

        // Upgrade v1 -> v2 failing health check MUST prune failed `releases/<commit_v2>`!
        fail_network_stop.store(false, Ordering::SeqCst);
        health.healthy.store(false, Ordering::SeqCst);
        assert!(mgr.install(&b2, true).is_err());
        assert!(paths.releases_dir().join(&commit_v1).exists());
        assert!(
            !paths.releases_dir().join(&commit_v2).exists(),
            "failed candidate v2 release directory must be pruned on rollback"
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn upgrade_drops_maintenance_lock_before_activation_and_recover_delegates_switched_tx() {
        let (root, paths) = temp_env("no-self-conflict");
        let paths_for_probe = paths.clone();

        // Simulate real `miro-ego.service` startup inside `HealthChecker::check_health()`
        // (`start_services(true)`):
        // 1) `miro-ego-recover.service` runs `mgr_probe.recover(true)` while `deployment.lock` is
        //    held AND `install-transaction.json` is in `TransactionState::Switched`!
        // 2) `miro-ego.service` runs `recover_on_startup()` which acquires `maintenance.lock`
        //    exclusively non-blocking (`acquire_maintenance_lock`)!
        // 3) Concurrent deploy attempts MUST still be blocked by `deployment.lock`!
        struct DaemonStartupSimHealth {
            paths: ReleaseManagerPaths,
        }
        impl HealthChecker for DaemonStartupSimHealth {
            fn check_health(&self) -> DeployResult<()> {
                assert!(
                    acquire_flock(self.paths.deployment_lock_file(), LockMode::Exclusive, true)
                        .is_err(),
                    "deployment.lock MUST remain held exclusively during start_services(true)"
                );
                let _maint =
                    acquire_maintenance_lock(&self.paths.run_dir, LockMode::Exclusive, true)
                        .expect("maintenance.lock MUST be dropped before start_services(true)");

                let probe_mgr = ReleaseManager::new(
                    self.paths.clone(),
                    MockSystemctl::new(),
                    Arc::new(ToggleHealth {
                        healthy: AtomicBool::new(true),
                    }),
                );
                let tx_on_disk = probe_mgr
                    .read_transaction()
                    .unwrap()
                    .expect("Switched transaction should still be on disk during health check");
                assert_eq!(tx_on_disk.state, TransactionState::Switched);

                let rec = probe_mgr
                    .recover(true)
                    .expect("recover(service_start=true) must delegate cleanly on Switched + held deployment.lock");
                assert!(rec.recovery_delegated);
                Ok(())
            }
        }

        fs::create_dir_all(&paths.config_root).unwrap();
        fs::write(
            paths.config_root.join("device.json"),
            br#"{
                "device": { "device_label": "Miro-Ego-Lab42" },
                "listen": { "host": "0.0.0.0", "port": 9443 },
                "security": { "profile": "customer" }
            }"#,
        )
        .unwrap();

        let mgr = ReleaseManager::new(
            paths.clone(),
            MockSystemctl::new(),
            Arc::new(DaemonStartupSimHealth {
                paths: paths_for_probe,
            }),
        );

        let c1 = "1".repeat(40);
        let c2 = "2".repeat(40);
        let b1 = root.join("bundles/v1");
        let b2 = root.join("bundles/v2");
        build_valid_bundle(&b1, "0.3.0", &c1, &[]);
        build_valid_bundle(
            &b2,
            "0.3.1",
            &c2,
            &[(
                "deploy/miro-ego/miro-ego.service.d/20-v2.conf",
                b"[Service]\nEnvironment=FOO=bar\n",
                false,
            )],
        );

        let st1 = mgr.install(&b1, true).unwrap();
        assert_eq!(st1.current.as_deref(), Some(c1.as_str()));

        // Verify dynamic Avahi rendering from device.json (`device_label`, `port`, `_https._tcp`).
        let avahi_xml =
            fs::read_to_string(root.join("etc/avahi/services/miro-ego.service")).unwrap();
        assert!(
            avahi_xml.contains("<name replace-wildcards=\"no\">Miro-Ego-Lab42</name>"),
            "unexpected avahi XML: {avahi_xml}"
        );
        assert!(avahi_xml.contains("<port>9443</port>"));
        assert!(avahi_xml.contains("<type>_miro-ego._tcp</type>"));
        assert!(avahi_xml.contains("<type>_https._tcp</type>"));

        // Live upgrade v1 -> v2 succeeds without maintenance.lock or recover(true) self-conflict!
        let st2 = mgr.install(&b2, true).unwrap();
        assert_eq!(st2.current.as_deref(), Some(c2.as_str()));
        assert!(
            root.join("etc/systemd/system/miro-ego.service.d/20-v2.conf")
                .is_file()
        );

        // Live rollback v2 -> v1 also drops maintenance.lock before activation AND removes v2 drop-in!
        let st_rb = mgr.rollback(true).unwrap();
        assert_eq!(st_rb.current.as_deref(), Some(c1.as_str()));
        assert!(
            !root
                .join("etc/systemd/system/miro-ego.service.d/20-v2.conf")
                .exists(),
            "explicit rollback must remove v2-only drop-in"
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn same_commit_reinstall_persists_switched_tx_and_restores_backup_on_activation_failure() {
        let (root, paths) = temp_env("same-commit-backup");
        let c1 = "1".repeat(40);
        let b1 = root.join("bundles/v1");
        let b1_reinstall = root.join("bundles/v1-reinstall");
        build_valid_bundle(
            &b1,
            "0.3.0",
            &c1,
            &[("miro-ego.service", b"[Unit]\nDescription=orig\n", false)],
        );
        build_valid_bundle(
            &b1_reinstall,
            "0.3.0-patch1",
            &c1,
            &[("miro-ego.service", b"[Unit]\nDescription=broken\n", false)],
        );

        let health = Arc::new(ToggleHealth {
            healthy: AtomicBool::new(true),
        });
        let mgr = ReleaseManager::new(paths.clone(), MockSystemctl::new(), health.clone());
        mgr.install(&b1, true).unwrap();
        assert!(paths.deploy_state_root.is_dir());
        assert!(!paths.state_root.join("install-transaction.json").exists());

        // Check Switched transaction state during activation AND backup preservation on failure!
        struct VerifySwitchedThenFail {
            paths: ReleaseManagerPaths,
            saw_switched: AtomicBool,
        }
        impl HealthChecker for VerifySwitchedThenFail {
            fn check_health(&self) -> DeployResult<()> {
                let tx = ReleaseManager::new(
                    self.paths.clone(),
                    MockSystemctl::new(),
                    Arc::new(ToggleHealth {
                        healthy: AtomicBool::new(true),
                    }),
                )
                .read_transaction()
                .unwrap()
                .expect("Switched tx must exist during same-commit reinstall activation");
                assert_eq!(tx.state, TransactionState::Switched);
                self.saw_switched.store(true, Ordering::SeqCst);
                Err(DeployError::Failed {
                    message: "simulated same-commit activation failure".to_owned(),
                })
            }
        }
        let fail_health = Arc::new(VerifySwitchedThenFail {
            paths: paths.clone(),
            saw_switched: AtomicBool::new(false),
        });
        let mgr_fail =
            ReleaseManager::new(paths.clone(), MockSystemctl::new(), fail_health.clone());
        assert!(mgr_fail.install(&b1_reinstall, true).is_err());
        assert!(fail_health.saw_switched.load(Ordering::SeqCst));
        assert!(paths.activation_failure_file().is_file());

        // Backup of original 0.3.0 release MUST have been restored!
        let restored_meta = mgr.read_release_metadata(&c1).unwrap();
        assert_eq!(restored_meta.version, "0.3.0");
        let unit_txt =
            fs::read_to_string(root.join("usr/lib/systemd/system/miro-ego.service")).unwrap();
        assert!(unit_txt.contains("Description=orig"));

        // Ensure drop-in allowlist & bundle path check reject arbitrary drop-in dirs and CRLF tokens.
        assert!(!crate::bundle::is_allowed_bundle_asset_path(
            "deploy/miro-ego/sshd.service.d/evil.conf"
        ));
        assert!(crate::bundle::is_allowed_bundle_asset_path(
            "deploy/miro-ego/fstrim.service.d/50-miro-ego-idle.conf"
        ));
        assert!(
            LocalHttpHealthChecker::new("127.0.0.1:8080".parse().unwrap(), Duration::from_secs(1))
                .with_tls_and_bearer(
                    rcgen::generate_simple_self_signed(vec!["localhost".into()])
                        .unwrap()
                        .cert
                        .pem()
                        .as_bytes(),
                    Some("bad\r\ntoken".to_owned()),
                )
                .is_err()
        );

        fs::remove_dir_all(root).unwrap();
    }
}
