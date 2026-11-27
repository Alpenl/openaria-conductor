//! RDK X5 SoC thermal zone reader (`/sys/class/thermal/thermal_zone*/temp`) and
//! device-tree board model reader (`/sys/firmware/devicetree/base/model`).

use crate::{HwError, HwResult};
use miro_ego_core::sys::fs::read_bounded_bytes;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

pub const DEFAULT_THERMAL_ROOT: &str = "/sys/class/thermal";
pub const DEFAULT_BOARD_MODEL_PATH: &str = "/sys/firmware/devicetree/base/model";
pub const RDK_X5_BOARD_MODEL: &str = "D-Robotics RDK X5 V1.0";
pub const MIN_VALID_TEMP_C: f64 = -40.0;
pub const MAX_VALID_TEMP_C: f64 = 125.0;
const MAX_MODEL_BYTES: usize = 512;

/// Individual thermal zone reading in degrees Celsius.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThermalZoneReading {
    pub zone: String,
    pub temperature_celsius: f64,
}

fn thermal_zone_sort_key(name: &str) -> (u8, u64, &str) {
    if let Some(suffix) = name.strip_prefix("thermal_zone") {
        if !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) {
            if let Ok(num) = suffix.parse::<u64>() {
                return (0, num, name);
            }
        }
    }
    (1, u64::MAX, name)
}

/// Read all valid nonblocking thermal zones under `thermal_root` (`thermal_zone*/temp`).
///
/// Linux `thermal_zone*/temp` always reports millidegrees Celsius. Readings outside
/// `[-40.0, 125.0]` °C or unreadable zones are skipped cleanly.
pub fn read_thermal_zones_in(thermal_root: &Path) -> Vec<ThermalZoneReading> {
    let Ok(entries) = fs::read_dir(thermal_root) else {
        return Vec::new();
    };
    let mut zones: Vec<(String, std::path::PathBuf)> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("thermal_zone") {
                Some((name, entry.path().join("temp")))
            } else {
                None
            }
        })
        .collect();
    zones.sort_by(|a, b| thermal_zone_sort_key(&a.0).cmp(&thermal_zone_sort_key(&b.0)));

    let mut readings = Vec::new();
    for (zone, temp_path) in zones {
        let Ok(raw) = fs::read_to_string(&temp_path) else {
            continue;
        };
        let Ok(millideg) = raw.trim().parse::<f64>() else {
            continue;
        };
        if !millideg.is_finite() {
            continue;
        }
        let celsius = millideg / 1000.0;
        if (MIN_VALID_TEMP_C..=MAX_VALID_TEMP_C).contains(&celsius) {
            readings.push(ThermalZoneReading {
                zone,
                temperature_celsius: celsius,
            });
        }
    }
    readings
}

/// Return the hottest valid SoC thermal reading in degrees Celsius under `thermal_root`.
pub fn read_soc_temperature_c_in(thermal_root: &Path) -> Option<f64> {
    read_thermal_zones_in(thermal_root)
        .into_iter()
        .map(|r| r.temperature_celsius)
        .reduce(f64::max)
}

/// Return the hottest valid SoC thermal reading in degrees Celsius from `/sys/class/thermal`.
pub fn read_soc_temperature_c() -> Option<f64> {
    read_soc_temperature_c_in(Path::new(DEFAULT_THERMAL_ROOT))
}

/// Read and normalize the device-tree board model string from `path`.
pub fn read_board_model_from(path: &Path) -> HwResult<String> {
    let raw = read_bounded_bytes(path, MAX_MODEL_BYTES).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            HwError::Unavailable {
                message: format!("board model file not found: {}", path.display()),
            }
        } else {
            HwError::Io {
                message: format!("failed to read board model {}: {err}", path.display()),
            }
        }
    })?;
    let trimmed = String::from_utf8_lossy(&raw);
    let cleaned: Vec<&str> = trimmed
        .trim_matches(|c: char| c == '\0' || c.is_whitespace())
        .split_whitespace()
        .collect();
    if cleaned.is_empty() {
        return Err(HwError::Unavailable {
            message: "board model string is empty".to_owned(),
        });
    }
    Ok(cleaned.join(" "))
}

/// Read and normalize `/sys/firmware/devicetree/base/model`.
pub fn read_board_model() -> HwResult<String> {
    read_board_model_from(Path::new(DEFAULT_BOARD_MODEL_PATH))
}

/// Return `true` if `model` matches the supported D-Robotics RDK X5 V1.0 board.
pub fn is_rdk_x5_model(model: &str) -> bool {
    let normalized = model
        .trim_matches(|c: char| c == '\0' || c.is_whitespace())
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    normalized.eq_ignore_ascii_case(RDK_X5_BOARD_MODEL)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn reads_hottest_valid_thermal_zone_and_filters_invalid_values() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "miro-ego-thermal-test-{}-{unique}",
            std::process::id()
        ));
        for (zone, content) in [
            ("thermal_zone0", "43250\n"),
            ("thermal_zone1", "58500\n"),
            ("thermal_zone2", "250000\n"), // out of range (> 125C)
            ("thermal_zone3", "not-a-number\n"),
            ("thermal_zone4", "-500\n"),   // -0.5C valid!
            ("thermal_zone10", "49000\n"), // must sort numerically AFTER thermal_zone4!
        ] {
            let zdir = dir.join(zone);
            fs::create_dir_all(&zdir).unwrap();
            fs::write(zdir.join("temp"), content).unwrap();
        }

        let zones = read_thermal_zones_in(&dir);
        let zone_names: Vec<&str> = zones.iter().map(|z| z.zone.as_str()).collect();
        assert_eq!(
            zone_names,
            vec![
                "thermal_zone0",
                "thermal_zone1",
                "thermal_zone4",
                "thermal_zone10"
            ]
        );
        assert_eq!(read_soc_temperature_c_in(&dir), Some(58.5));

        let model_path = dir.join("model");
        fs::write(&model_path, b"D-Robotics  RDK X5 V1.0\0\n").unwrap();
        let model = read_board_model_from(&model_path).unwrap();
        assert_eq!(model, "D-Robotics RDK X5 V1.0");
        assert!(is_rdk_x5_model(&model));
        assert!(!is_rdk_x5_model("Raspberry Pi 5 Model B"));

        let oversized_path = dir.join("oversized-model");
        fs::write(&oversized_path, vec![b'A'; MAX_MODEL_BYTES + 1]).unwrap();
        assert!(matches!(
            read_board_model_from(&oversized_path),
            Err(HwError::Io { .. })
        ));

        fs::remove_dir_all(dir).unwrap();
    }
}
