//! Unified CLI argument parser and command executors for the `miro-ego` binary.
//!
//! Matches all `deploy/miro-ego/*.service` and helper script invocations:
//! - `miro-ego serve [--config <path>] [--mock] [--listen <host:port>] ...`
//! - `miro-ego network-control [serve ... | watchdog-mode ... | status ...]`
//! - `miro-ego button [--config <path>] [--device-config <path>] ...`
//! - `miro-ego deploy <status | install <bundle-dir> | rollback | recover | uninstall> ...`
//! - `miro-ego volume <init [mountpoint] | hide <path> | inspect [mountpoint]>`
//! - `miro-ego status [--config <path>] ...`
//! - `miro-ego probe`
//! - `miro-ego validate <session-dir>`
//! - `miro-ego version` / `--version` / `--help`

use crate::DEVICE_SESSION_V1_SCHEMA;
use crate::runtime::{
    CaptureModeKind, DaemonServeOptions, NoopHealthChecker, NoopSystemctlRunner, bind_daemon,
    run_bound_daemon_with_shutdown, wait_for_termination_signal,
};
use miro_ego_api::API_V1_PREFIX;
use miro_ego_capture::audio;
use miro_ego_capture::session::schema_validation::SchemaValidator;
use miro_ego_capture::session::shm_ring::SHM_VERSION;
use miro_ego_core::config::{
    AudioConfig, CameraConfig, DEVICE_CONFIG_SCHEMA, DeviceConfig, DeviceIdentity, ExposureSetting,
    ListenConfig, RecordingEncoding, SecurityConfig, StorageConfig, load_device_config,
};
use miro_ego_core::ids::new_uuid_v4;
use miro_ego_core::paths::{
    DataVolumePaths, RECORDING_MOUNTPOINT, RECORDING_VOLUME_LABEL, SYSTEM_CONFIG_DIR,
    SYSTEM_ENCODER_BIN, SYSTEM_INSTALL_ROOT, SYSTEM_RUN_DIR, SYSTEM_STATE_DIR,
};
use miro_ego_core::sys::fs::{set_exfat_hidden, write_bytes_atomic};
use miro_ego_deploy::{
    FIRMWARE_STATUS_SCHEMA, HealthChecker, LocalHttpHealthChecker, RealSystemctlRunner,
    ReleaseManager, ReleaseManagerPaths, SystemctlRunner,
};
use miro_ego_hw::thermal::{
    DEFAULT_THERMAL_ROOT, is_rdk_x5_model, read_board_model, read_soc_temperature_c,
    read_thermal_zones_in,
};
use miro_ego_hw::{
    AplayAudioPlayer, ButtonConfig, ButtonMonitor, DEFAULT_BUTTON_CONFIG_PATH,
    DEFAULT_LEDS_SYSFS_ROOT, DEFAULT_VOICE_DEVICE, GpioButtonInput, IndicatorState,
    LocalHttpCaptureClient, SysfsGpioPin, SysfsLedController, VoiceController,
};
use miro_ego_network::{
    AuthoritativeNetworkController, ControllerConfig, DEFAULT_CONTROL_SOCKET_PATH,
    DEFAULT_ETHERNET_INTERFACE, DEFAULT_NETWORK_STATE_FILE, DEFAULT_WIFI_INTERFACE,
    MDNS_SERVICE_TYPE, NetworkRpcClient, NetworkRpcServer, NetworkStateStore, NmcliBackend,
    RpcServerConfig, SystemCommandRunner,
};
use miro_ego_storage::artifact_io::extract_manifest_artifacts;
use miro_ego_storage::{
    BinarySessionCatalog, CATALOG_FORMAT_VERSION, ExfatVolumeManager, SessionCatalog,
    VOLUME_METADATA_SCHEMA, VolumeManager, VolumePolicy, open_manifest, open_verified_artifact,
};
use serde_json::{Value, json};
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::os::fd::FromRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::{UnixDatagram, UnixListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const DEFAULT_DEVICE_CONFIG_PATH: &str = "/etc/miro-ego/device.json";
const DEFAULT_NETWORK_PROFILES_DIR: &str = "/etc/NetworkManager/system-connections";
const DEFAULT_VOICE_ASSETS_DIR: &str = "/opt/miro-ego/current/share/voice";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServeArgs {
    pub config: Option<PathBuf>,
    pub mock: bool,
    pub listen: Option<SocketAddr>,
    pub network_socket: Option<PathBuf>,
    pub encoder_bin: Option<PathBuf>,
    pub data_dir: Option<PathBuf>,
    pub state_dir: Option<PathBuf>,
    pub relaxed_volume: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkControlSubcommand {
    Serve {
        socket: PathBuf,
        state: PathBuf,
        profiles_dir: PathBuf,
        run_dir: PathBuf,
        device_id: Option<String>,
        wifi_iface: String,
        eth_iface: String,
    },
    WatchdogMode {
        socket: PathBuf,
    },
    Status {
        socket: PathBuf,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ButtonArgs {
    pub config: PathBuf,
    pub device_config: Option<PathBuf>,
    pub gpio_value: Option<PathBuf>,
    pub gpio_num: Option<u32>,
    pub api_addr: Option<SocketAddr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeployAction {
    Status,
    Install { bundle_dir: PathBuf, activate: bool },
    Rollback { activate: bool },
    Recover { service_start: bool },
    Uninstall,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeployArgs {
    pub action: DeployAction,
    pub install_root: PathBuf,
    pub config_root: PathBuf,
    pub state_root: PathBuf,
    pub run_dir: PathBuf,
    pub system_root: PathBuf,
    pub no_systemctl: bool,
    pub health_addr: SocketAddr,
    pub health_timeout_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Version,
    Help,
    Serve(ServeArgs),
    NetworkControl(NetworkControlSubcommand),
    Button(ButtonArgs),
    Deploy(DeployArgs),
    VolumeInit {
        mountpoint: PathBuf,
    },
    VolumeHide {
        path: PathBuf,
    },
    VolumeInspect {
        mountpoint: PathBuf,
    },
    Status {
        config: PathBuf,
        network_socket: PathBuf,
        install_root: PathBuf,
        state_root: PathBuf,
    },
    Probe,
    Validate {
        session_dir: PathBuf,
    },
}

pub fn parse_args(args: &[String]) -> Result<Command, String> {
    let Some(first) = args.first().map(String::as_str) else {
        return Ok(Command::Help);
    };
    let rest = &args[1..];
    match first {
        "-V" | "--version" | "version" => Ok(Command::Version),
        "-h" | "--help" | "help" => Ok(Command::Help),
        "serve" => parse_serve_args(rest).map(Command::Serve),
        "network-control" => parse_network_control_args(rest).map(Command::NetworkControl),
        "button" => parse_button_args(rest).map(Command::Button),
        "deploy" => parse_deploy_args(rest).map(Command::Deploy),
        "volume" => parse_volume_args(rest),
        "status" => parse_status_args(rest),
        "probe" => {
            if !rest.is_empty() {
                return Err(format!(
                    "probe does not accept arguments: {}",
                    rest.join(" ")
                ));
            }
            Ok(Command::Probe)
        }
        "validate" => {
            let Some(dir) = rest.first() else {
                return Err("usage: miro-ego validate <session-dir>".to_owned());
            };
            if rest.len() > 1 {
                return Err("usage: miro-ego validate <session-dir>".to_owned());
            }
            Ok(Command::Validate {
                session_dir: PathBuf::from(dir),
            })
        }
        other => Err(format!("unknown subcommand: {other}")),
    }
}

fn next_flag_value<'a>(args: &'a [String], idx: &mut usize, flag: &str) -> Result<&'a str, String> {
    *idx += 1;
    args.get(*idx)
        .map(String::as_str)
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_serve_args(args: &[String]) -> Result<ServeArgs, String> {
    let mut parsed = ServeArgs {
        config: None,
        mock: false,
        listen: None,
        network_socket: None,
        encoder_bin: None,
        data_dir: None,
        state_dir: None,
        relaxed_volume: false,
    };
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                parsed.config = Some(PathBuf::from(next_flag_value(args, &mut i, "--config")?));
            }
            "--mock" => {
                parsed.mock = true;
            }
            "--listen" => {
                let raw = next_flag_value(args, &mut i, "--listen")?;
                let addr: SocketAddr = raw
                    .parse()
                    .map_err(|err| format!("invalid --listen address '{raw}': {err}"))?;
                parsed.listen = Some(addr);
            }
            "--network-socket" => {
                parsed.network_socket = Some(PathBuf::from(next_flag_value(
                    args,
                    &mut i,
                    "--network-socket",
                )?));
            }
            "--encoder-bin" => {
                parsed.encoder_bin = Some(PathBuf::from(next_flag_value(
                    args,
                    &mut i,
                    "--encoder-bin",
                )?));
            }
            "--data-dir" => {
                parsed.data_dir = Some(PathBuf::from(next_flag_value(args, &mut i, "--data-dir")?));
            }
            "--state-dir" => {
                parsed.state_dir =
                    Some(PathBuf::from(next_flag_value(args, &mut i, "--state-dir")?));
            }
            "--relaxed-volume" => {
                parsed.relaxed_volume = true;
            }
            other => return Err(format!("unknown serve option: {other}")),
        }
        i += 1;
    }
    Ok(parsed)
}

fn default_network_state_path() -> PathBuf {
    if let Some(dir) = env::var_os("MIRO_EGO_NETWORK_STATE_DIR") {
        return PathBuf::from(dir).join("state.json");
    }
    PathBuf::from(DEFAULT_NETWORK_STATE_FILE)
}

fn parse_network_control_args(args: &[String]) -> Result<NetworkControlSubcommand, String> {
    let (sub, rest) = match args.first().map(String::as_str) {
        None => ("serve", args),
        Some(s) if s.starts_with('-') => ("serve", args),
        Some(s) => (s, &args[1..]),
    };
    let default_sock = env::var_os("MIRO_EGO_NETWORK_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONTROL_SOCKET_PATH));

    match sub {
        "serve" => {
            let mut socket = default_sock;
            let mut state = default_network_state_path();
            let mut profiles_dir = PathBuf::from(DEFAULT_NETWORK_PROFILES_DIR);
            let mut run_dir = PathBuf::from(SYSTEM_RUN_DIR);
            let mut device_id = None;
            let mut wifi_iface = DEFAULT_WIFI_INTERFACE.to_owned();
            let mut eth_iface = DEFAULT_ETHERNET_INTERFACE.to_owned();

            let mut i = 0;
            while i < rest.len() {
                match rest[i].as_str() {
                    "--socket" => {
                        socket = PathBuf::from(next_flag_value(rest, &mut i, "--socket")?);
                    }
                    "--state" => {
                        state = PathBuf::from(next_flag_value(rest, &mut i, "--state")?);
                    }
                    "--profiles-dir" => {
                        profiles_dir =
                            PathBuf::from(next_flag_value(rest, &mut i, "--profiles-dir")?);
                    }
                    "--run-dir" => {
                        run_dir = PathBuf::from(next_flag_value(rest, &mut i, "--run-dir")?);
                    }
                    "--device-id" => {
                        device_id = Some(next_flag_value(rest, &mut i, "--device-id")?.to_owned());
                    }
                    "--wifi-iface" => {
                        wifi_iface = next_flag_value(rest, &mut i, "--wifi-iface")?.to_owned();
                    }
                    "--eth-iface" => {
                        eth_iface = next_flag_value(rest, &mut i, "--eth-iface")?.to_owned();
                    }
                    other => return Err(format!("unknown network-control serve option: {other}")),
                }
                i += 1;
            }
            Ok(NetworkControlSubcommand::Serve {
                socket,
                state,
                profiles_dir,
                run_dir,
                device_id,
                wifi_iface,
                eth_iface,
            })
        }
        "watchdog-mode" => {
            let socket = parse_single_socket_flag(rest, default_sock, "watchdog-mode")?;
            Ok(NetworkControlSubcommand::WatchdogMode { socket })
        }
        "status" => {
            let socket = parse_single_socket_flag(rest, default_sock, "status")?;
            Ok(NetworkControlSubcommand::Status { socket })
        }
        other => Err(format!(
            "unknown network-control subcommand '{other}'; expected serve, watchdog-mode, or status"
        )),
    }
}

fn parse_single_socket_flag(
    args: &[String],
    default_sock: PathBuf,
    sub_name: &str,
) -> Result<PathBuf, String> {
    let mut socket = default_sock;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--socket" => {
                socket = PathBuf::from(next_flag_value(args, &mut i, "--socket")?);
            }
            other => {
                return Err(format!(
                    "unknown network-control {sub_name} option: {other}"
                ));
            }
        }
        i += 1;
    }
    Ok(socket)
}

fn parse_button_args(args: &[String]) -> Result<ButtonArgs, String> {
    let mut parsed = ButtonArgs {
        config: PathBuf::from(DEFAULT_BUTTON_CONFIG_PATH),
        device_config: None,
        gpio_value: None,
        gpio_num: None,
        api_addr: None,
    };
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                parsed.config = PathBuf::from(next_flag_value(args, &mut i, "--config")?);
            }
            "--device-config" => {
                parsed.device_config = Some(PathBuf::from(next_flag_value(
                    args,
                    &mut i,
                    "--device-config",
                )?));
            }
            "--gpio-value" => {
                parsed.gpio_value = Some(PathBuf::from(next_flag_value(
                    args,
                    &mut i,
                    "--gpio-value",
                )?));
            }
            "--gpio-num" => {
                let raw = next_flag_value(args, &mut i, "--gpio-num")?;
                let num: u32 = raw
                    .parse()
                    .map_err(|err| format!("invalid --gpio-num '{raw}': {err}"))?;
                parsed.gpio_num = Some(num);
            }
            "--api-addr" => {
                let raw = next_flag_value(args, &mut i, "--api-addr")?;
                let addr: SocketAddr = raw
                    .parse()
                    .map_err(|err| format!("invalid --api-addr '{raw}': {err}"))?;
                parsed.api_addr = Some(addr);
            }
            other => return Err(format!("unknown button option: {other}")),
        }
        i += 1;
    }
    Ok(parsed)
}

fn parse_deploy_args(args: &[String]) -> Result<DeployArgs, String> {
    let Some(sub) = args.first().map(String::as_str) else {
        return Err(
            "usage: miro-ego deploy <status | install <bundle-dir> | rollback | recover | uninstall> [options]"
                .to_owned(),
        );
    };

    let mut install_root = PathBuf::from(SYSTEM_INSTALL_ROOT);
    let mut config_root = PathBuf::from(SYSTEM_CONFIG_DIR);
    let mut state_root = PathBuf::from(SYSTEM_STATE_DIR);
    let mut run_dir = PathBuf::from(SYSTEM_RUN_DIR);
    let mut system_root = PathBuf::from("/");
    let mut no_systemctl = false;
    let mut activate = true;
    let mut service_start = false;
    let mut health_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let mut health_timeout_secs = 20_u64;
    let mut positional: Vec<String> = Vec::new();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--install-root" => {
                install_root = PathBuf::from(next_flag_value(args, &mut i, "--install-root")?);
            }
            "--config-root" => {
                config_root = PathBuf::from(next_flag_value(args, &mut i, "--config-root")?);
            }
            "--state-root" => {
                state_root = PathBuf::from(next_flag_value(args, &mut i, "--state-root")?);
            }
            "--run-dir" => {
                run_dir = PathBuf::from(next_flag_value(args, &mut i, "--run-dir")?);
            }
            "--system-root" => {
                system_root = PathBuf::from(next_flag_value(args, &mut i, "--system-root")?);
            }
            "--no-systemctl" => {
                no_systemctl = true;
            }
            "--no-activate" => {
                activate = false;
            }
            "--service-start" => {
                service_start = true;
            }
            "--health-addr" => {
                let raw = next_flag_value(args, &mut i, "--health-addr")?;
                health_addr = raw
                    .parse()
                    .map_err(|err| format!("invalid --health-addr '{raw}': {err}"))?;
            }
            "--health-timeout" => {
                let raw = next_flag_value(args, &mut i, "--health-timeout")?;
                health_timeout_secs = raw
                    .parse()
                    .map_err(|err| format!("invalid --health-timeout '{raw}': {err}"))?;
            }
            other if other.starts_with('-') => {
                return Err(format!("unknown deploy option: {other}"));
            }
            other => positional.push(other.to_owned()),
        }
        i += 1;
    }

    let action = match sub {
        "status" => {
            if !positional.is_empty() {
                return Err("deploy status takes no positional arguments".to_owned());
            }
            DeployAction::Status
        }
        "install" => {
            let Some(bundle) = positional.first() else {
                return Err("deploy install requires <bundle-dir>".to_owned());
            };
            if positional.len() > 1 {
                return Err("deploy install accepts a single <bundle-dir>".to_owned());
            }
            DeployAction::Install {
                bundle_dir: PathBuf::from(bundle),
                activate,
            }
        }
        "rollback" => {
            if !positional.is_empty() {
                return Err("deploy rollback takes no positional arguments".to_owned());
            }
            DeployAction::Rollback { activate }
        }
        "recover" => {
            if !positional.is_empty() {
                return Err("deploy recover takes no positional arguments".to_owned());
            }
            DeployAction::Recover { service_start }
        }
        "uninstall" => {
            if !positional.is_empty() {
                return Err("deploy uninstall takes no positional arguments".to_owned());
            }
            DeployAction::Uninstall
        }
        other => {
            return Err(format!(
                "unknown deploy action '{other}'; expected status, install, rollback, recover, or uninstall"
            ));
        }
    };

    Ok(DeployArgs {
        action,
        install_root,
        config_root,
        state_root,
        run_dir,
        system_root,
        no_systemctl,
        health_addr,
        health_timeout_secs,
    })
}

fn parse_volume_args(args: &[String]) -> Result<Command, String> {
    let sub = args.first().map(String::as_str);
    match sub {
        Some("init") => {
            let mountpoint = args
                .get(1)
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(RECORDING_MOUNTPOINT));
            Ok(Command::VolumeInit { mountpoint })
        }
        Some("hide") => {
            let path = args
                .get(1)
                .map(PathBuf::from)
                .ok_or_else(|| "volume hide requires <path>".to_owned())?;
            Ok(Command::VolumeHide { path })
        }
        Some("inspect") => {
            let mountpoint = args
                .get(1)
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(RECORDING_MOUNTPOINT));
            Ok(Command::VolumeInspect { mountpoint })
        }
        _ => Err(
            "usage: miro-ego volume <init [mountpoint] | hide <path> | inspect [mountpoint]>"
                .to_owned(),
        ),
    }
}

fn parse_status_args(args: &[String]) -> Result<Command, String> {
    let mut config = PathBuf::from(DEFAULT_DEVICE_CONFIG_PATH);
    let mut network_socket = env::var_os("MIRO_EGO_NETWORK_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONTROL_SOCKET_PATH));
    let mut install_root = PathBuf::from(SYSTEM_INSTALL_ROOT);
    let mut state_root = PathBuf::from(SYSTEM_STATE_DIR);

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                config = PathBuf::from(next_flag_value(args, &mut i, "--config")?);
            }
            "--network-socket" => {
                network_socket = PathBuf::from(next_flag_value(args, &mut i, "--network-socket")?);
            }
            "--install-root" => {
                install_root = PathBuf::from(next_flag_value(args, &mut i, "--install-root")?);
            }
            "--state-root" => {
                state_root = PathBuf::from(next_flag_value(args, &mut i, "--state-root")?);
            }
            other => return Err(format!("unknown status option: {other}")),
        }
        i += 1;
    }
    Ok(Command::Status {
        config,
        network_socket,
        install_root,
        state_root,
    })
}

pub fn run(command: Command) -> Result<(), String> {
    match command {
        Command::Version => {
            println!(
                "miro-ego {} (api={} shm=v{} catalog=v{} mdns={} fw={} indicator={:?})",
                env!("CARGO_PKG_VERSION"),
                API_V1_PREFIX,
                SHM_VERSION,
                CATALOG_FORMAT_VERSION,
                MDNS_SERVICE_TYPE,
                FIRMWARE_STATUS_SCHEMA,
                IndicatorState::Ready,
            );
            Ok(())
        }
        Command::Help => {
            println!(
                "miro-ego {} — unified native Rust device runtime\n\
                 \n\
                 USAGE:\n\
                 \tmiro-ego <COMMAND> [OPTIONS]\n\
                 \n\
                 COMMANDS:\n\
                 \tserve [--config <path>] [--mock] [--listen <host:port>]\n\
                 \tnetwork-control [serve | watchdog-mode | status] [--socket <path>]\n\
                 \tbutton [--config <path>] [--device-config <path>]\n\
                 \tdeploy <status | install <bundle-dir> | rollback | recover | uninstall>\n\
                 \tvolume <init [mountpoint] | hide <path> | inspect [mountpoint]>\n\
                 \tstatus [--config <path>]\n\
                 \tprobe\n\
                 \tvalidate <session-dir>\n\
                 \tversion",
                env!("CARGO_PKG_VERSION")
            );
            Ok(())
        }
        Command::Serve(args) => run_serve(args),
        Command::NetworkControl(sub) => run_network_control(sub),
        Command::Button(args) => run_button(args),
        Command::Deploy(args) => run_deploy(args),
        Command::VolumeInit { mountpoint } => run_volume_init(mountpoint),
        Command::VolumeHide { path } => run_volume_hide(path),
        Command::VolumeInspect { mountpoint } => run_volume_inspect(mountpoint),
        Command::Status {
            config,
            network_socket,
            install_root,
            state_root,
        } => run_status(config, network_socket, install_root, state_root),
        Command::Probe => run_probe(),
        Command::Validate { session_dir } => run_validate(&session_dir),
    }
}

// ---------------------------------------------------------------------------
// Command Implementations
// ---------------------------------------------------------------------------

fn run_serve(args: ServeArgs) -> Result<(), String> {
    let device_config = load_or_build_serve_config(&args)?;

    let mut options = DaemonServeOptions::from_device_config(device_config.clone(), args.mock);
    if let Some(listen) = args.listen {
        options.listen_override = Some(listen);
    }
    if let Some(sock) = args.network_socket {
        options.network_socket = Some(sock);
    }
    if args.relaxed_volume {
        options.relaxed_volume = true;
    }
    if let Some(enc) = args.encoder_bin {
        options.capture_mode = if args.mock {
            CaptureModeKind::ShmEncoder {
                encoder_bin: enc,
                segment_frames: 3,
            }
        } else {
            CaptureModeKind::Native {
                encoder_bin: enc,
                imu_device: None,
            }
        };
    }

    // Best-effort hardware peripherals (LED & voice): silently degrade on dev hosts / WSL2.
    if !args.mock {
        let led_dir = Path::new(DEFAULT_LEDS_SYSFS_ROOT).join("ACT");
        if let Ok(led) = SysfsLedController::open(led_dir, false) {
            options.indicator = Some(Arc::new(led));
        }
        let voice_settings_path = device_config.state_root.join("voice.json");
        if let Ok(voice) = VoiceController::open(
            DEFAULT_VOICE_DEVICE,
            DEFAULT_VOICE_ASSETS_DIR,
            voice_settings_path,
            Arc::new(AplayAudioPlayer::default()),
        ) {
            options.voice_controller = Some(Arc::new(voice));
        }
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| format!("failed to build tokio runtime: {err}"))?;

    rt.block_on(async move {
        let daemon = bind_daemon(options).await?;
        let bound_addr = daemon.server.local_addr();
        let scheme = if daemon.server.is_tls() {
            "https"
        } else {
            "http"
        };
        eprintln!("miro-ego: listening on {scheme}://{bound_addr}{API_V1_PREFIX}");
        run_bound_daemon_with_shutdown(daemon, wait_for_termination_signal()).await
    })
}

fn load_or_build_serve_config(args: &ServeArgs) -> Result<DeviceConfig, String> {
    if let Some(ref cfg_path) = args.config {
        let mut cfg = load_device_config(cfg_path)
            .map_err(|err| format!("failed to load device config {}: {err}", cfg_path.display()))?;
        if let Some(ref data_dir) = args.data_dir {
            cfg.storage.mountpoint = data_dir.clone();
        }
        if let Some(ref state_dir) = args.state_dir {
            cfg.state_root = state_dir.clone();
        }
        return Ok(cfg);
    }

    let default_path = Path::new(DEFAULT_DEVICE_CONFIG_PATH);
    if !args.mock && default_path.is_file() {
        return load_device_config(default_path).map_err(|err| {
            format!(
                "failed to load default device config {}: {err}",
                default_path.display()
            )
        });
    }

    if !args.mock {
        return Err(format!(
            "device config {} not found; pass --config <path> or --mock",
            default_path.display()
        ));
    }

    let mountpoint = args.data_dir.clone().unwrap_or_else(|| {
        env::temp_dir().join(format!("miro-ego-mock-data-{}", std::process::id()))
    });
    let state_root = args.state_dir.clone().unwrap_or_else(|| {
        env::temp_dir().join(format!("miro-ego-mock-state-{}", std::process::id()))
    });
    fs::create_dir_all(&mountpoint).map_err(|err| {
        format!(
            "failed to create mock data directory {}: {err}",
            mountpoint.display()
        )
    })?;
    fs::create_dir_all(&state_root).map_err(|err| {
        format!(
            "failed to create mock state directory {}: {err}",
            state_root.display()
        )
    })?;
    let paths = DataVolumePaths::new(mountpoint.clone());
    let _ = paths.ensure_layout();
    let _ = ensure_volume_metadata(&paths);

    let listen = match args.listen {
        Some(addr) => ListenConfig {
            host: addr.ip().to_string(),
            port: addr.port(),
        },
        None => ListenConfig {
            host: "127.0.0.1".to_owned(),
            port: 8080,
        },
    };

    Ok(DeviceConfig {
        schema: DEVICE_CONFIG_SCHEMA.to_owned(),
        listen,
        camera: CameraConfig {
            device: PathBuf::from("/dev/video0"),
            width: 3840,
            height: 1080,
            fps: 60,
            frame_decimation: 2,
            exposure_time_absolute: Some(ExposureSetting::default()),
        },
        storage: StorageConfig {
            mountpoint,
            minimum_available_bytes: 16 * 1024 * 1024,
            minimum_available_inodes: 16,
        },
        state_root,
        device: DeviceIdentity {
            device_id: new_uuid_v4(),
            device_label: "Miro-Ego-0000MOCK".replace("MOCK", "1234"),
            hardware_fingerprint: format!("sha256:{}", "0".repeat(64)),
        },
        security: SecurityConfig::Lab {
            isolated_network: true,
        },
        audio: AudioConfig {
            enabled: false,
            ..AudioConfig::default()
        },
        recording: RecordingEncoding::default(),
        segment_seconds: 5.0,
    })
}

fn run_network_control(sub: NetworkControlSubcommand) -> Result<(), String> {
    match sub {
        NetworkControlSubcommand::WatchdogMode { socket } => {
            let client = NetworkRpcClient::new(socket);
            let mode = client
                .watchdog_mode()
                .map_err(|err| format!("failed to query network-control watchdog-mode: {err}"))?;
            println!("{mode}");
            Ok(())
        }
        NetworkControlSubcommand::Status { socket } => {
            let client = NetworkRpcClient::new(socket);
            let status = client
                .status()
                .map_err(|err| format!("failed to query network-control status: {err}"))?;
            print_json(&status)
        }
        NetworkControlSubcommand::Serve {
            socket,
            state,
            profiles_dir,
            run_dir,
            device_id,
            wifi_iface,
            eth_iface,
        } => {
            let resolved_device_id = device_id
                .or_else(|| {
                    load_device_config(Path::new(DEFAULT_DEVICE_CONFIG_PATH))
                        .ok()
                        .map(|c| c.device.device_id)
                })
                .unwrap_or_else(new_uuid_v4);

            let state_store = NetworkStateStore::open_or_init(&state).map_err(|err| {
                format!("failed to open network state {}: {err}", state.display())
            })?;
            let backend = NmcliBackend::new(
                Arc::new(SystemCommandRunner),
                profiles_dir,
                wifi_iface,
                eth_iface,
            );
            let controller = Arc::new(
                AuthoritativeNetworkController::new(
                    backend,
                    state_store,
                    ControllerConfig::new(resolved_device_id, run_dir),
                )
                .map_err(|err| format!("failed to initialize network controller: {err}"))?,
            );

            let server = if let Some(listener) = take_systemd_socket_fd() {
                NetworkRpcServer::from_listener(
                    listener,
                    None,
                    controller,
                    RpcServerConfig::default(),
                )
                .map_err(|err| format!("failed to attach systemd socket listener: {err}"))?
            } else {
                NetworkRpcServer::bind(&socket, controller, RpcServerConfig::default()).map_err(
                    |err| {
                        format!(
                            "failed to bind network-control socket {}: {err}",
                            socket.display()
                        )
                    },
                )?
            };

            notify_systemd("READY=1");

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|err| format!("failed to build signal runtime: {err}"))?;
            rt.block_on(wait_for_termination_signal());

            notify_systemd("STOPPING=1");
            server.shutdown();
            Ok(())
        }
    }
}

/// Check whether `fd` is an open, listening `AF_UNIX` `SOCK_STREAM` socket (`SO_ACCEPTCONN == 1`).
pub fn is_listening_unix_socket(fd: libc::c_int) -> bool {
    if fd < 0 {
        return false;
    }
    let mut accept_conn: libc::c_int = 0;
    let mut opt_len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `accept_conn` and `opt_len` are valid stack pointers matching `SO_ACCEPTCONN`.
    let rc_accept = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ACCEPTCONN,
            (&raw mut accept_conn).cast::<libc::c_void>(),
            &raw mut opt_len,
        )
    };
    if rc_accept != 0 || accept_conn != 1 {
        return false;
    }

    let mut domain: libc::c_int = 0;
    let mut domain_len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `domain` and `domain_len` are valid stack pointers matching `SO_DOMAIN`.
    let rc_domain = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_DOMAIN,
            (&raw mut domain).cast::<libc::c_void>(),
            &raw mut domain_len,
        )
    };
    if rc_domain != 0 || domain != libc::AF_UNIX {
        return false;
    }

    let mut sock_type: libc::c_int = 0;
    let mut type_len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `sock_type` and `type_len` are valid stack pointers matching `SO_TYPE`.
    let rc_type = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TYPE,
            (&raw mut sock_type).cast::<libc::c_void>(),
            &raw mut type_len,
        )
    };
    rc_type == 0 && sock_type == libc::SOCK_STREAM
}

/// Adopt a listening `AF_UNIX` socket descriptor as an owned [`UnixListener`].
///
/// When `dup_fd` is `true` (used for systemd `StandardInput=socket` on fd 0), duplicates the
/// descriptor atomically with `F_DUPFD_CLOEXEC` so the returned listener owns an independent fd.
pub fn adopt_listening_unix_socket_fd(fd: libc::c_int, dup_fd: bool) -> Option<UnixListener> {
    if !is_listening_unix_socket(fd) {
        return None;
    }
    if dup_fd {
        // SAFETY: `fd` was verified as a valid open listening Unix socket above.
        let duplicated = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
        if duplicated < 0 {
            return None;
        }
        // SAFETY: `duplicated` is a freshly duplicated owned fd referring to a listening Unix socket.
        return Some(unsafe { UnixListener::from_raw_fd(duplicated) });
    }
    // SAFETY: `fd` is owned exclusively by this process (SD_LISTEN_FDS_START) and verified above.
    let _ = unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
    // SAFETY: `fd` was verified as a listening AF_UNIX SOCK_STREAM socket passed exclusively to us.
    Some(unsafe { UnixListener::from_raw_fd(fd) })
}

/// Adopt a systemd socket-activated listening Unix socket.
///
/// Supports both standard `LISTEN_PID`/`LISTEN_FDS` activation (fd 3) and
/// `StandardInput=socket` (`Accept=no`) activation (fd 0, as configured by
/// `deploy/miro-ego/miro-ego-network.service`).
pub fn take_systemd_socket_fd() -> Option<UnixListener> {
    if let (Ok(pid_str), Ok(fds_str)) = (env::var("LISTEN_PID"), env::var("LISTEN_FDS")) {
        if pid_str.parse::<u32>().ok() == Some(std::process::id())
            && fds_str.parse::<u32>().ok().is_some_and(|n| n >= 1)
        {
            if let Some(listener) = adopt_listening_unix_socket_fd(3, false) {
                return Some(listener);
            }
        }
    }
    adopt_listening_unix_socket_fd(libc::STDIN_FILENO, true)
}

/// Send an `sd_notify`-compatible datagram (`READY=1`, `STOPPING=1`) to `notify_socket`,
/// supporting both filesystem paths and Linux abstract namespace (`@...`) sockets.
pub fn notify_systemd_to(notify_socket: &OsStr, state: &str) -> bool {
    let bytes = notify_socket.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    let Ok(sock) = UnixDatagram::unbound() else {
        return false;
    };
    if let Some(abstract_name) = bytes.strip_prefix(b"@") {
        if abstract_name.is_empty() {
            return false;
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::linux::net::SocketAddrExt;
            if let Ok(addr) = std::os::unix::net::SocketAddr::from_abstract_name(abstract_name) {
                return sock.send_to_addr(state.as_bytes(), &addr).is_ok();
            }
        }
        return false;
    }
    sock.send_to(state.as_bytes(), Path::new(notify_socket))
        .is_ok()
}

fn notify_systemd(state: &str) {
    let Some(notify_socket) = env::var_os("NOTIFY_SOCKET") else {
        return;
    };
    let _ = notify_systemd_to(&notify_socket, state);
}

fn run_button(args: ButtonArgs) -> Result<(), String> {
    let button_cfg = ButtonConfig::load(&args.config).map_err(|err| {
        format!(
            "failed to load button config {}: {err}",
            args.config.display()
        )
    })?;
    if !button_cfg.enabled {
        eprintln!("miro-ego button: disabled by configuration; exiting cleanly");
        return Ok(());
    }

    let device_cfg_path = args
        .device_config
        .unwrap_or_else(|| PathBuf::from(DEFAULT_DEVICE_CONFIG_PATH));
    let device_cfg = load_device_config(&device_cfg_path).ok();

    let api_addr = match args.api_addr {
        Some(addr) => addr,
        None => match &device_cfg {
            Some(cfg) => {
                let ip: IpAddr = cfg
                    .listen
                    .host
                    .parse()
                    .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
                let target_ip = if ip.is_unspecified() {
                    IpAddr::V4(Ipv4Addr::LOCALHOST)
                } else {
                    ip
                };
                SocketAddr::new(target_ip, cfg.listen.port)
            }
            None => SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        },
    };

    let bearer_token = device_cfg.as_ref().and_then(|cfg| match &cfg.security {
        SecurityConfig::Lab { .. } => None,
        SecurityConfig::Customer {
            bearer_token_file, ..
        } => fs::read_to_string(bearer_token_file)
            .ok()
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty()),
    });

    let pin = if let Some(val_path) = args.gpio_value {
        SysfsGpioPin::from_value_path(val_path)
    } else if let Some(soc_num) = args.gpio_num {
        SysfsGpioPin::from_gpio_number(soc_num)
    } else {
        SysfsGpioPin::from_gpio_number(u32::from(button_cfg.physical_pin))
    };

    let input = GpioButtonInput::new(pin, button_cfg.clone())
        .map_err(|err| format!("failed to initialize GPIO button input: {err}"))?;
    let mut client = LocalHttpCaptureClient::new(
        api_addr,
        bearer_token.clone(),
        button_cfg.request_timeout_ms,
    );
    if let Some(ref tok) = bearer_token {
        let mut hasher = sha2::Sha256::new();
        use sha2::Digest;
        hasher.update(b"miro-ego.csrf.v1:");
        hasher.update(tok.as_bytes());
        let derived_csrf = miro_ego_core::ids::hex_encode(&hasher.finalize());
        client = client.with_csrf_token(derived_csrf);
    }

    let monitor = ButtonMonitor::new(input, client);
    let stop = Arc::new(AtomicBool::new(false));
    let stop_sig = Arc::clone(&stop);

    let sig_thread = std::thread::Builder::new()
        .name("miro-ego-button-sig".to_owned())
        .spawn(move || {
            if let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                rt.block_on(wait_for_termination_signal());
                stop_sig.store(true, Ordering::Release);
            }
        })
        .ok();

    let result = monitor
        .run_until(&stop, button_cfg.poll_ms)
        .map_err(|err| format!("button monitor exited with error: {err}"));
    stop.store(true, Ordering::Release);
    if let Some(handle) = sig_thread {
        let _ = handle.join();
    }
    result
}

fn run_deploy(args: DeployArgs) -> Result<(), String> {
    let paths = ReleaseManagerPaths {
        install_root: args.install_root,
        config_root: args.config_root,
        state_root: args.state_root,
        run_dir: args.run_dir,
        system_root: args.system_root.clone(),
    };

    let hermetic = args.no_systemctl || args.system_root != Path::new("/");
    let systemctl: Arc<dyn SystemctlRunner> = if hermetic {
        Arc::new(NoopSystemctlRunner)
    } else {
        Arc::new(RealSystemctlRunner)
    };
    let health: Arc<dyn HealthChecker> = if hermetic {
        Arc::new(NoopHealthChecker)
    } else {
        Arc::new(LocalHttpHealthChecker::new(
            args.health_addr,
            Duration::from_secs(args.health_timeout_secs.max(1)),
        ))
    };

    let manager = ReleaseManager::new(paths, systemctl, health);
    match args.action {
        DeployAction::Status => {
            let st = manager
                .status()
                .map_err(|err| format!("deploy status failed: {err}"))?;
            print_json(&st)
        }
        DeployAction::Install {
            bundle_dir,
            activate,
        } => {
            let st = manager
                .install(&bundle_dir, activate)
                .map_err(|err| format!("deploy install failed: {err}"))?;
            print_json(&st)
        }
        DeployAction::Rollback { activate } => {
            let st = manager
                .rollback(activate)
                .map_err(|err| format!("deploy rollback failed: {err}"))?;
            print_json(&st)
        }
        DeployAction::Recover { service_start } => {
            let st = manager
                .recover(service_start)
                .map_err(|err| format!("deploy recover failed: {err}"))?;
            print_json(&st)
        }
        DeployAction::Uninstall => {
            let report = manager
                .uninstall()
                .map_err(|err| format!("deploy uninstall failed: {err}"))?;
            print_json(&report)
        }
    }
}

fn run_volume_init(mountpoint: PathBuf) -> Result<(), String> {
    let paths = DataVolumePaths::new(mountpoint);
    let hidden_applied = paths
        .ensure_layout()
        .map_err(|err| format!("failed to initialize MIRO-EGO volume layout: {err}"))?;
    let created_metadata = ensure_volume_metadata(&paths)?;
    println!(
        "initialized {} (exfat_hidden_applied={hidden_applied}, volume_metadata_created={created_metadata})",
        paths.mountpoint().display()
    );
    Ok(())
}

fn run_volume_hide(path: PathBuf) -> Result<(), String> {
    let applied = set_exfat_hidden(&path, true)
        .map_err(|err| format!("failed to set ATTR_HIDDEN on {}: {err}", path.display()))?;
    println!("path={} exfat_hidden_applied={applied}", path.display());
    Ok(())
}

fn run_volume_inspect(mountpoint: PathBuf) -> Result<(), String> {
    let paths = DataVolumePaths::new(mountpoint);
    let manager = ExfatVolumeManager::new(paths, VolumePolicy::relaxed_for_tests(0));
    let info = manager
        .inspect_volume()
        .map_err(|err| format!("failed to inspect volume: {err}"))?;
    let metadata = manager.read_metadata().ok();
    let writable = manager.is_mounted_and_writable();
    let report = json!({
        "mountpoint": info.mountpoint,
        "label": info.label,
        "is_exfat": info.is_exfat,
        "writable": writable,
        "total_bytes": info.total_bytes,
        "available_bytes": info.available_bytes,
        "metadata": metadata,
    });
    print_json(&report)
}

fn run_status(
    config_path: PathBuf,
    network_socket: PathBuf,
    install_root: PathBuf,
    state_root: PathBuf,
) -> Result<(), String> {
    let cfg = load_device_config(&config_path).ok();
    let mountpoint = cfg
        .as_ref()
        .map(|c| c.storage.mountpoint.clone())
        .unwrap_or_else(|| PathBuf::from(RECORDING_MOUNTPOINT));
    let paths = DataVolumePaths::new(mountpoint);
    let vol_mgr = ExfatVolumeManager::new(paths.clone(), VolumePolicy::relaxed_for_tests(0));
    let volume_info = vol_mgr.inspect_volume().ok();
    let volume_writable = vol_mgr.is_mounted_and_writable();
    let session_count = BinarySessionCatalog::open(paths)
        .ok()
        .and_then(|c| c.list_sessions().ok())
        .map(|s| s.len());

    let network_status = NetworkRpcClient::new(&network_socket)
        .with_timeout(Duration::from_secs(2))
        .status()
        .ok();

    let mut release_paths = ReleaseManagerPaths::canonical();
    release_paths.install_root = install_root;
    release_paths.state_root = state_root;
    let deploy_status = ReleaseManager::new(
        release_paths,
        Arc::new(NoopSystemctlRunner),
        Arc::new(NoopHealthChecker),
    )
    .status()
    .ok();

    let board_model = read_board_model().ok();
    let soc_temp_c = read_soc_temperature_c();

    let report = json!({
        "version": env!("CARGO_PKG_VERSION"),
        "config_path": config_path,
        "config_loaded": cfg.is_some(),
        "device": cfg.as_ref().map(|c| &c.device),
        "board_model": board_model,
        "soc_temperature_celsius": soc_temp_c,
        "volume": {
            "writable": volume_writable,
            "info": volume_info,
            "indexed_sessions": session_count,
        },
        "network": network_status,
        "deploy": deploy_status,
    });
    print_json(&report)
}

fn run_probe() -> Result<(), String> {
    let board_model = read_board_model().ok();
    let rdk_x5 = board_model.as_deref().is_some_and(is_rdk_x5_model);
    let thermal_zones = read_thermal_zones_in(Path::new(DEFAULT_THERMAL_ROOT));
    let soc_temp_c = read_soc_temperature_c();

    let mut video_devices = Vec::new();
    if let Ok(entries) = fs::read_dir("/dev") {
        for entry in entries.flatten() {
            if let Ok(name) = entry.file_name().into_string() {
                if name.starts_with("video") {
                    video_devices.push(format!("/dev/{name}"));
                }
            }
        }
    }
    video_devices.sort();

    let encoder_path = env::var_os("MIRO_EGO_ENCODER_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(SYSTEM_ENCODER_BIN));
    let encoder_present = encoder_path.is_file();

    let net_sock = env::var_os("MIRO_EGO_NETWORK_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONTROL_SOCKET_PATH));
    let network_rpc_reachable = NetworkRpcClient::new(&net_sock)
        .with_timeout(Duration::from_millis(500))
        .health()
        .is_ok();

    let data_paths = DataVolumePaths::canonical();
    let data_volume = ExfatVolumeManager::new(data_paths, VolumePolicy::relaxed_for_tests(0))
        .inspect_volume()
        .ok();

    let report = json!({
        "version": env!("CARGO_PKG_VERSION"),
        "shm_version": SHM_VERSION,
        "catalog_version": CATALOG_FORMAT_VERSION,
        "board_model": board_model,
        "is_rdk_x5": rdk_x5,
        "soc_temperature_celsius": soc_temp_c,
        "thermal_zones": thermal_zones,
        "video_devices": video_devices,
        "alsa_available": audio::available(),
        "encoder_bin": {
            "path": encoder_path,
            "present": encoder_present,
        },
        "network_control_socket": {
            "path": net_sock,
            "reachable": network_rpc_reachable,
        },
        "data_volume": data_volume,
    });
    print_json(&report)
}

/// Validate a sealed `miro-ego.device-session.v1` session directory:
/// 1. Parses `manifest.json` using pinned-descriptor safe I/O.
/// 2. Validates `manifest.json` against the embedded JSON Schema Draft 2020-12 contract.
/// 3. Cryptographically verifies every declared artifact's exact byte size & SHA-256 digest.
pub fn validate_session_dir(session_dir: &Path) -> Result<Value, String> {
    let canonical_dir = fs::canonicalize(session_dir).map_err(|err| {
        format!(
            "failed to resolve session directory {}: {err}",
            session_dir.display()
        )
    })?;
    let parent = canonical_dir.parent().ok_or_else(|| {
        format!(
            "session directory has no parent directory: {}",
            canonical_dir.display()
        )
    })?;
    let leaf = canonical_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| {
            format!(
                "session directory name is not valid UTF-8: {}",
                canonical_dir.display()
            )
        })?;

    let opened = open_manifest(parent, leaf, None)
        .map_err(|err| format!("failed to open sealed manifest: {err}"))?;

    let raw_str = std::str::from_utf8(&opened.raw_bytes)
        .map_err(|err| format!("manifest.json is not valid UTF-8: {err}"))?;
    let validator = SchemaValidator::new(DEVICE_SESSION_V1_SCHEMA)
        .map_err(|err| format!("failed to compile device-session v1 schema: {err}"))?;
    let schema_valid = validator
        .is_valid(raw_str)
        .map_err(|err| format!("schema validation error: {err}"))?;
    if !schema_valid {
        return Err(
            "manifest.json failed miro-ego.device-session.v1 JSON Schema validation".to_owned(),
        );
    }

    let session_id = opened
        .parsed
        .get("session_id")
        .and_then(Value::as_str)
        .ok_or_else(|| "manifest.json missing session_id".to_owned())?
        .to_owned();

    let descriptors = extract_manifest_artifacts(&opened.parsed)
        .map_err(|err| format!("failed to extract manifest artifacts: {err}"))?;

    let mut verified_artifacts = Vec::with_capacity(descriptors.len());
    let mut total_bytes: u64 = 0;
    for desc in &descriptors {
        let verified =
            open_verified_artifact(parent, leaf, &desc.artifact_id, Some(&session_id), true)
                .map_err(|err| {
                    format!(
                        "artifact verification failed for {} ({}): {err}",
                        desc.path, desc.artifact_id
                    )
                })?;
        total_bytes = total_bytes.saturating_add(verified.descriptor.bytes);
        verified_artifacts.push(json!({
            "role": verified.descriptor.role,
            "path": verified.descriptor.path,
            "bytes": verified.descriptor.bytes,
            "sha256": verified.descriptor.sha256,
        }));
    }

    Ok(json!({
        "valid": true,
        "schema": "miro-ego.device-session.v1",
        "session_id": session_id,
        "manifest_sha256": opened.sha256,
        "artifact_count": verified_artifacts.len(),
        "total_bytes": total_bytes,
        "artifacts": verified_artifacts,
    }))
}

fn run_validate(session_dir: &Path) -> Result<(), String> {
    let report = validate_session_dir(session_dir)?;
    print_json(&report)
}

fn ensure_volume_metadata(paths: &DataVolumePaths) -> Result<bool, String> {
    let meta_path = paths.volume_metadata_file();
    match fs::symlink_metadata(&meta_path) {
        Ok(meta) => {
            if !meta.file_type().is_file() {
                return Err(format!(
                    "existing volume metadata path is not a regular file: {}",
                    meta_path.display()
                ));
            }
            Ok(false)
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {
            let payload = json!({
                "schema": VOLUME_METADATA_SCHEMA,
                "volume_id": new_uuid_v4(),
                "label": RECORDING_VOLUME_LABEL,
            });
            let mut bytes = serde_json::to_vec(&payload)
                .map_err(|err| format!("failed to serialize volume metadata: {err}"))?;
            bytes.push(b'\n');
            write_bytes_atomic(&meta_path, &bytes, 0o640, true).map_err(|err| {
                format!(
                    "failed to write volume metadata {}: {err}",
                    meta_path.display()
                )
            })?;
            Ok(true)
        }
        Err(err) => Err(format!(
            "failed to inspect volume metadata {}: {err}",
            meta_path.display()
        )),
    }
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<(), String> {
    let text = serde_json::to_string_pretty(value)
        .map_err(|err| format!("failed to format JSON output: {err}"))?;
    println!("{text}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn parses_all_canonical_subcommands_and_flags() {
        assert_eq!(parse_args(&args(&["--version"])).unwrap(), Command::Version);
        assert_eq!(parse_args(&args(&["--help"])).unwrap(), Command::Help);
        assert_eq!(parse_args(&args(&["probe"])).unwrap(), Command::Probe);

        let serve = parse_args(&args(&[
            "serve",
            "--config",
            "/etc/miro-ego/device.json",
            "--mock",
            "--listen",
            "127.0.0.1:9090",
        ]))
        .unwrap();
        assert_eq!(
            serve,
            Command::Serve(ServeArgs {
                config: Some(PathBuf::from("/etc/miro-ego/device.json")),
                mock: true,
                listen: Some("127.0.0.1:9090".parse().unwrap()),
                network_socket: None,
                encoder_bin: None,
                data_dir: None,
                state_dir: None,
                relaxed_volume: false,
            })
        );

        assert_eq!(
            parse_args(&args(&[
                "network-control",
                "watchdog-mode",
                "--socket",
                "/tmp/net.sock",
            ]))
            .unwrap(),
            Command::NetworkControl(NetworkControlSubcommand::WatchdogMode {
                socket: PathBuf::from("/tmp/net.sock"),
            })
        );

        assert_eq!(
            parse_args(&args(&[
                "button",
                "--config",
                "/etc/miro-ego/button.json",
                "--device-config",
                "/etc/miro-ego/device.json",
            ]))
            .unwrap(),
            Command::Button(ButtonArgs {
                config: PathBuf::from("/etc/miro-ego/button.json"),
                device_config: Some(PathBuf::from("/etc/miro-ego/device.json")),
                gpio_value: None,
                gpio_num: None,
                api_addr: None,
            })
        );

        let deploy = parse_args(&args(&[
            "deploy",
            "install",
            "/tmp/bundle",
            "--no-activate",
            "--no-systemctl",
            "--install-root",
            "/tmp/opt",
        ]))
        .unwrap();
        assert!(matches!(
            deploy,
            Command::Deploy(DeployArgs {
                action: DeployAction::Install {
                    bundle_dir,
                    activate: false,
                },
                no_systemctl: true,
                ..
            }) if bundle_dir == Path::new("/tmp/bundle")
        ));

        assert_eq!(
            parse_args(&args(&["volume", "init", "/mnt/test"])).unwrap(),
            Command::VolumeInit {
                mountpoint: PathBuf::from("/mnt/test"),
            }
        );
        assert_eq!(
            parse_args(&args(&["volume", "hide", "/data/.miro-ego"])).unwrap(),
            Command::VolumeHide {
                path: PathBuf::from("/data/.miro-ego"),
            }
        );
        assert_eq!(
            parse_args(&args(&["volume", "inspect", "/data"])).unwrap(),
            Command::VolumeInspect {
                mountpoint: PathBuf::from("/data"),
            }
        );
        assert_eq!(
            parse_args(&args(&["validate", "/data/recordings/2026-11-26/10-00-00"])).unwrap(),
            Command::Validate {
                session_dir: PathBuf::from("/data/recordings/2026-11-26/10-00-00"),
            }
        );
        assert!(parse_args(&args(&["volume", "hide"])).is_err());
        assert!(parse_args(&args(&["deploy"])).is_err());
        assert!(parse_args(&args(&["unknown"])).is_err());
    }

    #[test]
    fn volume_init_and_hide_commands_succeed_on_temp_dir() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("miro-ego-cli-vol-{}-{unique}", std::process::id()));
        fs::create_dir(&dir).unwrap();

        run(Command::VolumeInit {
            mountpoint: dir.clone(),
        })
        .unwrap();
        assert!(dir.join("recordings").is_dir());
        assert!(dir.join("logs").is_dir());
        assert!(dir.join("updates").is_dir());
        assert!(dir.join(".miro-ego/index").is_dir());

        let meta_path = dir.join(".miro-ego/volume.json");
        let first_raw = fs::read(&meta_path).unwrap();
        let meta: Value = serde_json::from_slice(&first_raw).unwrap();
        assert_eq!(meta["schema"], VOLUME_METADATA_SCHEMA);
        assert_eq!(meta["label"], RECORDING_VOLUME_LABEL);
        assert!(miro_ego_core::ids::is_valid_uuid_v4(
            meta["volume_id"].as_str().unwrap()
        ));

        run(Command::VolumeInit {
            mountpoint: dir.clone(),
        })
        .unwrap();
        assert_eq!(fs::read(&meta_path).unwrap(), first_raw);

        run(Command::VolumeHide {
            path: dir.join(".miro-ego"),
        })
        .unwrap();

        run(Command::VolumeInspect {
            mountpoint: dir.clone(),
        })
        .unwrap();

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn adopts_listening_unix_socket_and_rejects_non_listening_fds() {
        use std::os::fd::AsRawFd;
        use std::os::unix::net::UnixStream;

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "miro-ego-sock-adopt-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&dir).unwrap();

        let sock_path = dir.join("test.sock");
        let orig = UnixListener::bind(&sock_path).unwrap();
        assert!(is_listening_unix_socket(orig.as_raw_fd()));

        let adopted = adopt_listening_unix_socket_fd(orig.as_raw_fd(), true).unwrap();
        // Dropping the original fd must leave the duplicated listener fully functional.
        drop(orig);
        let client = UnixStream::connect(&sock_path).unwrap();
        let (_accepted, _) = adopted.accept().unwrap();
        drop(client);
        drop(adopted);

        // Non-listening datagram socket and regular file must be rejected.
        let dgram = UnixDatagram::unbound().unwrap();
        assert!(!is_listening_unix_socket(dgram.as_raw_fd()));
        assert!(adopt_listening_unix_socket_fd(dgram.as_raw_fd(), true).is_none());

        let file_path = dir.join("regular.txt");
        let reg = fs::File::create(&file_path).unwrap();
        assert!(!is_listening_unix_socket(reg.as_raw_fd()));
        assert!(adopt_listening_unix_socket_fd(reg.as_raw_fd(), true).is_none());

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn notify_systemd_supports_filesystem_and_abstract_sockets() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "miro-ego-notify-test-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&dir).unwrap();

        // 1. Filesystem path socket
        let path_sock = dir.join("notify.sock");
        let rx_path = UnixDatagram::bind(&path_sock).unwrap();
        rx_path
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        assert!(notify_systemd_to(path_sock.as_os_str(), "READY=1"));
        let mut buf = [0u8; 32];
        let n = rx_path.recv(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"READY=1");

        // 2. Linux abstract namespace socket (@...)
        #[cfg(target_os = "linux")]
        {
            use std::os::linux::net::SocketAddrExt;
            let abs_name = format!("miro-ego-notify-{}-{unique}", std::process::id());
            let abs_addr =
                std::os::unix::net::SocketAddr::from_abstract_name(abs_name.as_bytes()).unwrap();
            let rx_abs = UnixDatagram::bind_addr(&abs_addr).unwrap();
            rx_abs
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let env_val = format!("@{abs_name}");
            assert!(notify_systemd_to(OsStr::new(&env_val), "READY=1"));
            let n2 = rx_abs.recv(&mut buf).unwrap();
            assert_eq!(&buf[..n2], b"READY=1");
        }

        fs::remove_dir_all(dir).unwrap();
    }
}
