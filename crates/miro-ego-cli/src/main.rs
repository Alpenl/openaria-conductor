//! Single unified on-device binary entrypoint (`miro-ego`) for Miro-Ego 0.3.0.

use miro_ego_api::API_V1_PREFIX;
use miro_ego_capture::session::shm_ring::SHM_VERSION;
use miro_ego_core::paths::{DataVolumePaths, RECORDING_MOUNTPOINT};
use miro_ego_core::sys::fs::set_exfat_hidden;
use miro_ego_deploy::FIRMWARE_STATUS_SCHEMA;
use miro_ego_hw::IndicatorState;
use miro_ego_network::MDNS_SERVICE_TYPE;
use miro_ego_storage::CATALOG_FORMAT_VERSION;
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Version,
    Help,
    Serve,
    NetworkControl,
    Button,
    Deploy,
    VolumeInit { mountpoint: PathBuf },
    VolumeHide { path: PathBuf },
}

fn parse_args(args: &[String]) -> Result<Command, String> {
    let Some(first) = args.first().map(String::as_str) else {
        return Ok(Command::Help);
    };
    match first {
        "-V" | "--version" | "version" => Ok(Command::Version),
        "-h" | "--help" | "help" => Ok(Command::Help),
        "serve" => Ok(Command::Serve),
        "network-control" => Ok(Command::NetworkControl),
        "button" => Ok(Command::Button),
        "deploy" => Ok(Command::Deploy),
        "volume" => {
            let sub = args.get(1).map(String::as_str);
            match sub {
                Some("init") => {
                    let mountpoint = args
                        .get(2)
                        .map(PathBuf::from)
                        .unwrap_or_else(|| PathBuf::from(RECORDING_MOUNTPOINT));
                    Ok(Command::VolumeInit { mountpoint })
                }
                Some("hide") => {
                    let path = args
                        .get(2)
                        .map(PathBuf::from)
                        .ok_or_else(|| "volume hide requires <path>".to_owned())?;
                    Ok(Command::VolumeHide { path })
                }
                _ => Err("usage: miro-ego volume <init [mountpoint] | hide <path>>".to_owned()),
            }
        }
        other => Err(format!("unknown subcommand: {other}")),
    }
}

fn run(command: Command) -> Result<(), String> {
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
                "miro-ego {} — unified native device runtime\n\
                 \n\
                 USAGE:\n\
                 \tmiro-ego <COMMAND>\n\
                 \n\
                 COMMANDS:\n\
                 \tserve                      Run device capture & HTTP API daemon\n\
                 \tnetwork-control            Run privileged Wi-Fi/hotspot control socket\n\
                 \tbutton                     Run GPIO recording button daemon\n\
                 \tdeploy                     Verify or apply firmware releases\n\
                 \tvolume init [mountpoint]   Initialize MIRO-EGO exFAT layout\n\
                 \tvolume hide <path>         Set exFAT ATTR_HIDDEN on path\n\
                 \tversion                    Print version",
                env!("CARGO_PKG_VERSION")
            );
            Ok(())
        }
        Command::VolumeInit { mountpoint } => {
            let paths = DataVolumePaths::new(mountpoint);
            let hidden_applied = paths
                .ensure_layout()
                .map_err(|err| format!("failed to initialize MIRO-EGO volume layout: {err}"))?;
            println!(
                "initialized {} (exfat_hidden_applied={hidden_applied})",
                paths.mountpoint().display()
            );
            Ok(())
        }
        Command::VolumeHide { path } => {
            let applied = set_exfat_hidden(&path, true)
                .map_err(|err| format!("failed to set ATTR_HIDDEN on {}: {err}", path.display()))?;
            println!("path={} exfat_hidden_applied={applied}", path.display());
            Ok(())
        }
        Command::Serve | Command::NetworkControl | Command::Button | Command::Deploy => {
            Err("runtime subcommand wiring arrives in Wave 3/4".to_owned())
        }
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    match parse_args(&args).and_then(run) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("miro-ego: {message}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn parses_all_canonical_subcommands() {
        assert_eq!(parse_args(&args(&["--version"])).unwrap(), Command::Version);
        assert_eq!(parse_args(&args(&["serve"])).unwrap(), Command::Serve);
        assert_eq!(
            parse_args(&args(&["network-control"])).unwrap(),
            Command::NetworkControl
        );
        assert_eq!(parse_args(&args(&["button"])).unwrap(), Command::Button);
        assert_eq!(parse_args(&args(&["deploy"])).unwrap(), Command::Deploy);
        assert_eq!(
            parse_args(&args(&["volume", "init", "/mnt/test"])).unwrap(),
            Command::VolumeInit {
                mountpoint: PathBuf::from("/mnt/test")
            }
        );
        assert_eq!(
            parse_args(&args(&["volume", "hide", "/data/.miro-ego"])).unwrap(),
            Command::VolumeHide {
                path: PathBuf::from("/data/.miro-ego")
            }
        );
        assert!(parse_args(&args(&["volume", "hide"])).is_err());
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

        run(Command::VolumeHide {
            path: dir.join(".miro-ego"),
        })
        .unwrap();

        fs::remove_dir_all(dir).unwrap();
    }
}
