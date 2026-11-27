//! Modular daemon runtime assembly, capture backend adapters, embedded Web Console serving,
//! boot-scoped clock synchronization, and graceful supervisor lifecycle for `miro-ego`.

pub mod clock;
pub mod error_map;
pub mod firmware;
pub mod gateway_backend;
pub mod mock_capture;
pub mod native_capture;
pub mod supervisor;
pub mod web_assets;

pub use error_map::{
    map_camera_focus_error, map_deploy_error, map_hw_error, map_network_error,
    map_network_mutation_error, map_storage_error,
};
pub use gateway_backend::{
    ProductionBackendConfig, ProductionGatewayBackend, degraded_network_status,
    degraded_network_status_with_scheme,
};
pub use mock_capture::{MockCaptureBackend, ShmEncoderCaptureBackend};
pub use native_capture::{NativeCaptureBackend, NetworkLockCaptureBackend};
pub use supervisor::{
    EventFeedbackTracker, run_bound_daemon_with_shutdown, wait_for_termination_signal,
};

use bytes::Bytes;
use clock::ClockStateService;
use miro_ego_api::{
    GatewayBackend, GatewayServer, GatewayServerConfig, H264PreviewHub, MjpegPreviewHub, Principal,
    SecurityPolicy, TlsConfig,
};
use miro_ego_core::config::{DeviceConfig, SecurityConfig};
use miro_ego_core::ids::hex_encode;
use miro_ego_core::paths::{DataVolumePaths, SYSTEM_ENCODER_BIN, SYSTEM_RUN_DIR};
use miro_ego_core::sys::fs::read_bounded_bytes;
use miro_ego_deploy::{
    DEFAULT_DEPLOY_SOCKET_PATH, DeployResult, HealthChecker, ReleaseManager, ReleaseManagerPaths,
    SystemctlRunner,
};
use miro_ego_hw::{SysfsLedController, VoiceController};
use miro_ego_network::NetworkRpcClient;
use miro_ego_storage::{
    AuditConfig, AuditLogger, BinarySessionCatalog, CaptureBackend, CaptureCoordinator,
    ClockSyncProbe, CoordinatorConfig, ExfatVolumeManager, LogStore, VolumeManager, VolumePolicy,
};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_RESERVE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_TIMER_POLL_INTERVAL: Duration = Duration::from_millis(250);
pub(crate) const ENCODER_FINISH_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_BEARER_TOKEN_FILE_BYTES: usize = 4096;

pub(crate) const SYNTHETIC_JPEG_FRAME: &[u8] = &[
    0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, b'J', b'F', b'I', b'F', 0x00, 0x01, 0x01, 0x00, 0x00, 0x01,
    0x00, 0x01, 0x00, 0x00, 0xFF, 0xDB, 0x00, 0x43, 0x00, 0x08, 0x06, 0x06, 0x07, 0x06, 0x05, 0x08,
    0x07, 0x07, 0x07, 0x09, 0x09, 0x08, 0x0A, 0x0C, 0x14, 0x0D, 0x0C, 0x0B, 0x0B, 0x0C, 0x19, 0x12,
    0x13, 0x0F, 0x14, 0x1D, 0x1A, 0x1F, 0x1E, 0x1D, 0x1A, 0x1C, 0x1C, 0x20, 0x24, 0x2E, 0x27, 0x20,
    0x22, 0x2C, 0x23, 0x1C, 0x1C, 0x28, 0x37, 0x29, 0x2C, 0x30, 0x31, 0x34, 0x34, 0x34, 0x1F, 0x27,
    0x39, 0x3D, 0x38, 0x32, 0x3C, 0x2E, 0x33, 0x34, 0x32, 0xFF, 0xC0, 0x00, 0x0B, 0x08, 0x00, 0x01,
    0x00, 0x01, 0x01, 0x01, 0x11, 0x00, 0xFF, 0xC4, 0x00, 0x14, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0xFF, 0xDA, 0x00, 0x08,
    0x01, 0x01, 0x00, 0x00, 0x3F, 0x00, 0x7F, 0xFF, 0xD9,
];
pub(crate) const SYNTHETIC_FMP4_INIT: &[u8] =
    b"\x00\x00\x00\x18ftypisom\x00\x00\x02\x00isomiso6\x00\x00\x00\x08moov";
pub(crate) const SYNTHETIC_FMP4_MOOF: &[u8] =
    b"\x00\x00\x00\x10moof\x00\x00\x00\x08mfhd\x00\x00\x00\x08mdat";

pub(crate) fn rfc3339_now_utc() -> String {
    miro_ego_api::security::rfc3339_now_utc()
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_encode(&hasher.finalize())
}

/// No-op `SystemctlRunner` used for read-only daemon firmware status inspection and hermetic CLI runs.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSystemctlRunner;

impl SystemctlRunner for NoopSystemctlRunner {
    fn run_systemctl(&self, _args: &[&str]) -> DeployResult<()> {
        Ok(())
    }

    fn unit_load_state(&self, _unit: &str) -> DeployResult<String> {
        Ok("loaded".to_owned())
    }
}

/// No-op `HealthChecker` used when daemon activation is disabled or redirected to a hermetic root.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopHealthChecker;

impl HealthChecker for NoopHealthChecker {
    fn check_health(&self) -> DeployResult<()> {
        Ok(())
    }
}

/// Build the `SecurityPolicy` and optional `TlsConfig` matching `DeviceConfig.security`.
pub fn build_security_and_tls(
    config: &DeviceConfig,
) -> Result<(Arc<SecurityPolicy>, Option<TlsConfig>), String> {
    match &config.security {
        SecurityConfig::Lab { .. } => Ok((Arc::new(SecurityPolicy::lab_default()), None)),
        SecurityConfig::Customer {
            bearer_token_file,
            tls_certificate_file,
            tls_private_key_file,
            principal_id,
            ..
        } => {
            let raw_bytes = read_bounded_bytes(bearer_token_file, MAX_BEARER_TOKEN_FILE_BYTES)
                .map_err(|err| {
                    format!(
                        "failed to read customer bearer token file {}: {err}",
                        bearer_token_file.display()
                    )
                })?;
            let raw_token = String::from_utf8(raw_bytes).map_err(|_| {
                format!(
                    "customer bearer token file {} is not valid UTF-8",
                    bearer_token_file.display()
                )
            })?;
            let bearer_token = raw_token.trim().to_owned();
            if bearer_token.is_empty() {
                return Err(format!(
                    "customer bearer token file {} is empty",
                    bearer_token_file.display()
                ));
            }
            let csrf_override = std::env::var("MIRO_EGO_CSRF_TOKEN")
                .ok()
                .map(|v| v.trim().to_owned())
                .filter(|v| !v.is_empty());
            let principal = Principal::all_v1_operations(principal_id.clone())
                .map_err(|err| format!("invalid customer principal_id: {err}"))?;
            let mut tokens = HashMap::new();
            tokens.insert(bearer_token, principal);
            let policy = SecurityPolicy::customer(tokens, HashSet::new(), csrf_override)
                .map_err(|err| format!("failed to construct customer security policy: {err}"))?;
            let tls = TlsConfig::from_pem_files(tls_certificate_file, tls_private_key_file)
                .map_err(|err| format!("failed to load customer TLS credentials: {err}"))?;
            Ok((Arc::new(policy), Some(tls)))
        }
    }
}

/// Capture backend selection mode for [`DaemonServeOptions`].
#[derive(Debug, Clone)]
pub enum CaptureModeKind {
    /// Real hardware V4L2 + `miro-ego-capture` + `MEGO_SHM` encoder.
    Native {
        encoder_bin: PathBuf,
        imu_device: Option<PathBuf>,
    },
    /// Real external C encoder (`miro-ego-encoder` / `miro-ego-encoder-mock`) driven over `MEGO_SHM`.
    ShmEncoder {
        encoder_bin: PathBuf,
        segment_frames: u64,
    },
    /// Pure-Rust synthetic capture backend (`--mock` without external encoder).
    Mock,
}

/// Parameters for assembling and serving the complete `miro-ego` daemon.
#[derive(Clone)]
pub struct DaemonServeOptions {
    pub device_config: DeviceConfig,
    pub capture_mode: CaptureModeKind,
    pub mock_mode: bool,
    pub mock_finish_delay: Duration,
    pub require_clock_sync: bool,
    pub listen_override: Option<SocketAddr>,
    pub network_socket: Option<PathBuf>,
    pub deploy_socket: Option<PathBuf>,
    pub release_paths: ReleaseManagerPaths,
    pub relaxed_volume: bool,
    pub enable_h264_preview: bool,
    pub run_dir: Option<PathBuf>,
    pub voice_controller: Option<Arc<VoiceController>>,
    pub indicator: Option<Arc<SysfsLedController>>,
    pub reserve_poll_interval: Duration,
    pub timer_poll_interval: Duration,
}

impl DaemonServeOptions {
    pub fn from_device_config(device_config: DeviceConfig, mock: bool) -> Self {
        let encoder_bin = std::env::var_os("MIRO_EGO_ENCODER_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(SYSTEM_ENCODER_BIN));
        let capture_mode = if mock {
            CaptureModeKind::Mock
        } else {
            CaptureModeKind::Native {
                encoder_bin,
                imu_device: Some(device_config.camera.device.clone()),
            }
        };
        let network_socket = std::env::var_os("MIRO_EGO_NETWORK_SOCKET")
            .map(PathBuf::from)
            .or_else(|| Some(PathBuf::from(miro_ego_network::DEFAULT_CONTROL_SOCKET_PATH)));
        let env_deploy_socket = std::env::var_os("MIRO_EGO_DEPLOY_SOCKET").map(PathBuf::from);
        let deploy_socket = if mock {
            env_deploy_socket
        } else {
            env_deploy_socket.or_else(|| Some(PathBuf::from(DEFAULT_DEPLOY_SOCKET_PATH)))
        };
        let run_dir = if mock {
            Some(device_config.state_root.join("run"))
        } else {
            Some(PathBuf::from(SYSTEM_RUN_DIR))
        };
        Self {
            device_config,
            capture_mode,
            mock_mode: mock,
            mock_finish_delay: Duration::ZERO,
            require_clock_sync: !mock,
            listen_override: None,
            network_socket,
            deploy_socket,
            release_paths: ReleaseManagerPaths::canonical(),
            relaxed_volume: mock,
            enable_h264_preview: mock,
            run_dir,
            voice_controller: None,
            indicator: None,
            reserve_poll_interval: DEFAULT_RESERVE_POLL_INTERVAL,
            timer_poll_interval: DEFAULT_TIMER_POLL_INTERVAL,
        }
    }
}

/// Assembled live daemon ready to bind and serve HTTP/HTTPS requests.
pub struct BoundDaemon {
    pub server: GatewayServer,
    pub backend: Arc<ProductionGatewayBackend>,
    pub reserve_poll_interval: Duration,
    pub timer_poll_interval: Duration,
}

/// Assemble all storage, capture, preview, security, and HTTP server subsystems, running
/// startup `StagingRecoveryManager::recover_staging()` BEFORE opening HTTP intake!
pub async fn bind_daemon(options: DaemonServeOptions) -> Result<BoundDaemon, String> {
    options
        .device_config
        .validate()
        .map_err(|err| format!("invalid device config: {err}"))?;

    let volume_paths = DataVolumePaths::new(options.device_config.storage.mountpoint.clone());
    let resolved_run_dir = match options.run_dir.clone() {
        Some(dir) => {
            if options.relaxed_volume && dir == Path::new(SYSTEM_RUN_DIR) {
                if fs::create_dir_all(&dir).is_ok()
                    && fs::metadata(&dir).is_ok_and(|m| !m.permissions().readonly())
                    && {
                        let probe = dir.join(format!(".miro-ego-probe-{}", std::process::id()));
                        match fs::OpenOptions::new()
                            .create(true)
                            .truncate(true)
                            .write(true)
                            .open(&probe)
                        {
                            Ok(_) => {
                                let _ = fs::remove_file(&probe);
                                true
                            }
                            Err(_) => false,
                        }
                    }
                {
                    Some(dir)
                } else {
                    let fallback = options.device_config.state_root.join("run");
                    let _ = fs::create_dir_all(&fallback);
                    Some(fallback)
                }
            } else {
                if options.relaxed_volume {
                    let _ = fs::create_dir_all(&dir);
                }
                Some(dir)
            }
        }
        None => None,
    };
    let policy = if options.relaxed_volume {
        VolumePolicy::relaxed_for_tests(options.device_config.storage.minimum_available_bytes)
    } else {
        let mut prod = VolumePolicy::from_env_production();
        prod.minimum_available_bytes = options.device_config.storage.minimum_available_bytes;
        prod.minimum_available_inodes = options.device_config.storage.minimum_available_inodes;
        prod.system_state_root = Some(options.device_config.state_root.clone());
        prod
    };
    let volume = Arc::new(ExfatVolumeManager::new(volume_paths.clone(), policy));
    let volume_mounted_writable = volume.is_mounted_and_writable();
    if volume_mounted_writable {
        let _ = volume.ensure_layout();
    }

    let catalog = Arc::new(
        if volume_mounted_writable {
            BinarySessionCatalog::open_fast_if_clean(volume_paths.clone()).map(|(cat, _)| cat)
        } else {
            BinarySessionCatalog::open(volume_paths.clone())
        }
        .map_err(|err| format!("failed to open binary session catalog: {err}"))?,
    );

    let vol_for_audit = Arc::clone(&volume);
    let audit = Arc::new(AuditLogger::new(
        volume_paths.clone(),
        AuditConfig::default(),
        Arc::new(move || vol_for_audit.is_mounted_and_writable()),
    ));

    let mjpeg_hub = Arc::new(MjpegPreviewHub::new(
        options.device_config.camera.fps.clamp(1, 30),
    ));
    let h264_hub = if options.enable_h264_preview {
        let hub = Arc::new(H264PreviewHub::new_fmp4(64));
        hub.set_init_segment_blocking(Bytes::from_static(SYNTHETIC_FMP4_INIT));
        Some(hub)
    } else {
        None
    };

    let effective_fps = (f64::from(options.device_config.camera.fps)
        / f64::from(options.device_config.camera.frame_decimation))
    .max(1.0);
    let default_segment_frames =
        ((effective_fps * options.device_config.segment_seconds).round() as u64).max(1);

    let obs_net_client = options
        .network_socket
        .as_ref()
        .map(|sock| NetworkRpcClient::new(sock).with_timeout(Duration::from_secs(2)));
    let mut_net_client = options
        .network_socket
        .as_ref()
        .map(|sock| NetworkRpcClient::new(sock).with_timeout(Duration::from_secs(20)));

    let mut native_capture_ref: Option<Arc<NativeCaptureBackend>> = None;
    let (raw_capture_backend, segment_frames): (Arc<dyn CaptureBackend>, u64) =
        match options.capture_mode {
            CaptureModeKind::Native {
                encoder_bin,
                imu_device,
            } => {
                let b = Arc::new(
                    NativeCaptureBackend::open_with_network(
                        options.device_config.clone(),
                        encoder_bin,
                        imu_device,
                        Arc::clone(&mjpeg_hub),
                        obs_net_client.clone(),
                    )
                    .map_err(|err| format!("failed to initialize native capture backend: {err}"))?,
                );
                native_capture_ref = Some(Arc::clone(&b));
                (b, default_segment_frames)
            }
            CaptureModeKind::ShmEncoder {
                encoder_bin,
                segment_frames,
            } => {
                let b = ShmEncoderCaptureBackend::new(
                    encoder_bin,
                    options.device_config.camera.width,
                    options.device_config.camera.height,
                    effective_fps.round() as u32,
                    segment_frames.max(1),
                    Arc::clone(&mjpeg_hub),
                    h264_hub.clone(),
                );
                (Arc::new(b), segment_frames.max(1))
            }
            CaptureModeKind::Mock => {
                let b = MockCaptureBackend::new(
                    effective_fps.round() as u32,
                    Arc::clone(&mjpeg_hub),
                    h264_hub.clone(),
                )
                .with_finish_delay(options.mock_finish_delay);
                (Arc::new(b), default_segment_frames)
            }
        };

    let capture_backend: Arc<dyn CaptureBackend> = match resolved_run_dir.clone() {
        Some(run_dir) => Arc::new(NetworkLockCaptureBackend::new(raw_capture_backend, run_dir)),
        None => raw_capture_backend,
    };

    let installed_release = ReleaseManager::new(
        options.release_paths.clone(),
        Arc::new(NoopSystemctlRunner),
        Arc::new(NoopHealthChecker),
    )
    .status()
    .ok();
    let compiled_commit = option_env!("MIRO_EGO_GIT_COMMIT")
        .map(str::to_owned)
        .filter(|s| (40..=64).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_hexdigit()))
        .unwrap_or_else(|| "0".repeat(40));
    let resolved_software_version = installed_release
        .as_ref()
        .and_then(|st| st.current_version.clone())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned());
    let resolved_commit = installed_release
        .as_ref()
        .and_then(|st| st.current.clone())
        .filter(|c| (40..=64).contains(&c.len()) && c.bytes().all(|b| b.is_ascii_hexdigit()))
        .unwrap_or(compiled_commit);

    let calibration_file = options
        .device_config
        .camera
        .calibration_file
        .clone()
        .or_else(|| {
            (!options.mock_mode)
                .then(|| PathBuf::from("/etc/miro-ego/calibration.json"))
                .filter(|p| p.is_file())
        });

    let clock_service = Arc::new(ClockStateService::new(
        resolved_run_dir.clone(),
        obs_net_client,
        mut_net_client,
        !options.mock_mode,
    ));

    let timer_settings_path = options.device_config.state_root.join("capture-timer.json");
    let coord_cfg = CoordinatorConfig {
        device_id: options.device_config.device.device_id.clone(),
        device_label: options.device_config.device.device_label.clone(),
        hardware_fingerprint: options.device_config.device.hardware_fingerprint.clone(),
        platform: "rdk-x5".to_owned(),
        software_version: resolved_software_version,
        commit: resolved_commit.clone(),
        timezone: "Etc/UTC".to_owned(),
        camera_width: options.device_config.camera.width,
        camera_height: options.device_config.camera.height,
        sensor_fps: f64::from(options.device_config.camera.fps),
        frame_decimation: options.device_config.camera.frame_decimation,
        segment_frames,
        encoding: options.device_config.recording.clone(),
        run_dir: resolved_run_dir,
        timer_settings_path: Some(timer_settings_path),
        require_clock_sync: options.require_clock_sync,
        calibration_file,
    };

    let audio_mode_str = if options.device_config.audio.enabled {
        "enabled"
    } else {
        "disabled"
    };
    let coordinator = Arc::new(
        CaptureCoordinator::new(
            coord_cfg,
            Arc::clone(&volume),
            Arc::clone(&catalog),
            Arc::clone(&capture_backend),
        )
        .with_audit_logger(Arc::clone(&audit))
        .with_clock_probe(Arc::clone(&clock_service) as Arc<dyn ClockSyncProbe>)
        .with_audio_requested_mode(audio_mode_str),
    );

    let vol_for_logs = Arc::clone(&volume);
    let logs = Arc::new(
        LogStore::new(
            volume_paths,
            Arc::new(move || vol_for_logs.is_mounted_and_writable()),
        )
        .with_volume(&volume),
    );

    // Step 1: Run crash staging recovery BEFORE opening HTTP intake.
    if volume.is_mounted_and_writable() {
        let coord_for_recovery = Arc::clone(&coordinator);
        let _ = tokio::task::spawn_blocking(move || coord_for_recovery.recover_on_startup()).await;
    }

    let mut backend_cfg =
        ProductionBackendConfig::from_device_config(options.device_config.clone());
    backend_cfg.software_commit = resolved_commit;
    backend_cfg.network_socket = options.network_socket;
    backend_cfg.deploy_socket = options.deploy_socket;
    backend_cfg.release_paths = options.release_paths;
    backend_cfg.enable_h264_preview = options.enable_h264_preview;
    backend_cfg.mock_mode = options.mock_mode;
    backend_cfg.voice_controller = options.voice_controller;
    backend_cfg.indicator = options.indicator;

    let backend = Arc::new(
        ProductionGatewayBackend::new_with_clock(
            backend_cfg,
            Arc::clone(&coordinator),
            Arc::clone(&volume),
            Arc::clone(&catalog),
            Arc::clone(&audit),
            Arc::clone(&logs),
            mjpeg_hub,
            h264_hub,
            clock_service,
        )
        .with_capture_backend(capture_backend)
        .with_native_capture(native_capture_ref),
    );
    let backend_for_swap = Arc::clone(&backend);
    coordinator.set_safe_swap_release_hook(Arc::new(move || {
        backend_for_swap.flush_coalesced_audit();
        backend_for_swap.audit().close_active_file();
    }));

    let (security, tls) = build_security_and_tls(&options.device_config)?;
    let bind_addr = match options.listen_override {
        Some(addr) => addr,
        None => {
            let ip: IpAddr = options
                .device_config
                .listen
                .host
                .parse()
                .map_err(|err| format!("invalid listen.host: {err}"))?;
            SocketAddr::new(ip, options.device_config.listen.port)
        }
    };

    let server_cfg = match tls {
        Some(tls_cfg) => GatewayServerConfig::https(bind_addr, tls_cfg),
        None => GatewayServerConfig::http(bind_addr),
    };
    let server = GatewayServer::bind(
        server_cfg,
        Arc::clone(&backend) as Arc<dyn GatewayBackend>,
        security,
    )
    .await
    .map_err(|err| format!("failed to bind gateway server on {bind_addr}: {err}"))?;

    Ok(BoundDaemon {
        server,
        backend,
        reserve_poll_interval: options.reserve_poll_interval,
        timer_poll_interval: options.timer_poll_interval,
    })
}
