//! Shared pure-Rust utilities for CRC32 checksums, RFC 3339 timestamps,
//! domain-separated SHA-256 digests, and base64url cursor encoding.

use miro_ego_core::ids::hex_encode;
use miro_ego_core::sys::fs::fsync_directory;
use sha2::{Digest, Sha256};
use std::fs;
use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const CRC32_TABLE: [u32; 256] = build_crc32_table();

const fn build_crc32_table() -> [u32; 256] {
    let mut table = [0_u32; 256];
    let mut idx = 0_usize;
    while idx < 256 {
        let mut crc = idx as u32;
        let mut bit = 0;
        while bit < 8 {
            if (crc & 1) != 0 {
                crc = (crc >> 1) ^ 0xedb8_8320;
            } else {
                crc >>= 1;
            }
            bit += 1;
        }
        table[idx] = crc;
        idx += 1;
    }
    table
}

/// Compute standard IEEE 802.3 CRC32 checksum.
pub fn crc32_ieee(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffff_u32;
    for &byte in data {
        let index = ((crc ^ u32::from(byte)) & 0xff) as usize;
        crc = (crc >> 8) ^ CRC32_TABLE[index];
    }
    !crc
}

/// Compute lowercase 64-char hex SHA-256 of `data`.
pub fn sha256_hex(data: &[u8]) -> String {
    hex_encode(Sha256::digest(data).as_slice())
}

/// Compute domain-separated SHA-256 formatted as `"sha256:<64-lower-hex>"`.
pub fn domain_sha256(domain: &str, data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain.as_bytes());
    hasher.update(b"\0");
    hasher.update(data);
    format!("sha256:{}", hex_encode(hasher.finalize().as_slice()))
}

/// Format current UTC wall-clock time as RFC 3339 (`YYYY-MM-DDTHH:MM:SSZ`).
pub fn now_rfc3339_utc() -> String {
    system_time_to_rfc3339_utc(SystemTime::now())
}

/// Format a `SystemTime` as UTC RFC 3339 (`YYYY-MM-DDTHH:MM:SSZ`).
pub fn system_time_to_rfc3339_utc(time: SystemTime) -> String {
    let secs = match time.duration_since(UNIX_EPOCH) {
        Ok(dur) => i64::try_from(dur.as_secs()).unwrap_or(i64::MAX),
        Err(err) => -i64::try_from(err.duration().as_secs()).unwrap_or(0),
    };
    unix_secs_to_rfc3339_utc(secs)
}

/// Format Unix epoch seconds as UTC RFC 3339 (`YYYY-MM-DDTHH:MM:SSZ`).
pub fn unix_secs_to_rfc3339_utc(unix_secs: i64) -> String {
    let days = unix_secs.div_euclid(86_400);
    let secs_of_day = unix_secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Howard Hinnant's civil-from-days algorithm (exact Gregorian calendar conversion).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + i64::from(m <= 2);
    (year, m as u32, d as u32)
}

/// Howard Hinnant's days-from-civil algorithm (days relative to 1970-01-01).
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = year - i64::from(month <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let m = u64::from(month);
    let d = u64::from(day);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + (doe as i64) - 719_468
}

const fn is_leap_year(year: u32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

const fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Strictly parse an RFC 3339 timestamp (`YYYY-MM-DDTHH:MM:SS[.frac](Z|±HH:MM)`) into
/// UTC Unix epoch nanoseconds.
pub fn parse_rfc3339_utc_nanos(timestamp: &str) -> Option<i128> {
    let bytes = timestamp.as_bytes();
    if bytes.len() < 20 {
        return None;
    }
    if bytes[4] != b'-'
        || bytes[7] != b'-'
        || !matches!(bytes[10], b'T' | b't')
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let year: u32 = timestamp.get(0..4)?.parse().ok()?;
    let month: u32 = timestamp.get(5..7)?.parse().ok()?;
    let day: u32 = timestamp.get(8..10)?.parse().ok()?;
    let hour: u32 = timestamp.get(11..13)?.parse().ok()?;
    let minute: u32 = timestamp.get(14..16)?.parse().ok()?;
    let second: u32 = timestamp.get(17..19)?.parse().ok()?;

    let max_day = days_in_month(year, month);
    if year < 1970
        || !(1..=12).contains(&month)
        || day == 0
        || day > max_day
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }

    let mut rest = timestamp.get(19..)?;
    let mut nanos: u32 = 0;
    if let Some(after_dot) = rest.strip_prefix('.') {
        let frac_len = after_dot.bytes().take_while(u8::is_ascii_digit).count();
        if frac_len == 0 {
            return None;
        }
        let frac_digits = &after_dot.as_bytes()[..frac_len.min(9)];
        let mut mul = 100_000_000_u32;
        for &b in frac_digits {
            nanos += u32::from(b - b'0') * mul;
            mul /= 10;
        }
        rest = after_dot.get(frac_len..)?;
    }

    let tz_offset_secs: i64 = if rest == "Z" || rest == "z" {
        0
    } else if rest.len() == 6
        && matches!(rest.as_bytes()[0], b'+' | b'-')
        && rest.as_bytes()[3] == b':'
    {
        let tz_h: u32 = rest.get(1..3)?.parse().ok()?;
        let tz_m: u32 = rest.get(4..6)?.parse().ok()?;
        if tz_h > 23 || tz_m > 59 {
            return None;
        }
        let abs = i64::from(tz_h) * 3600 + i64::from(tz_m) * 60;
        if rest.as_bytes()[0] == b'-' {
            -abs
        } else {
            abs
        }
    } else {
        return None;
    };

    let days = days_from_civil(i64::from(year), month, day);
    let local_secs =
        days * 86_400 + i64::from(hour) * 3600 + i64::from(minute) * 60 + i64::from(second);
    let utc_secs = local_secs - tz_offset_secs;
    Some(i128::from(utc_secs) * 1_000_000_000 + i128::from(nanos))
}

/// Extract `(YYYY-MM-DD, HH-MM-SS)` for readable session publication from an RFC 3339 timestamp.
///
/// Requires a timezone-aware timestamp (`Z` or `±HH:MM`) and year in `2020..2100`.
pub fn readable_date_time_from_rfc3339(timestamp: &str) -> Option<(String, String)> {
    let _ = parse_rfc3339_utc_nanos(timestamp)?;
    let year: u32 = timestamp.get(0..4)?.parse().ok()?;
    let month: u32 = timestamp.get(5..7)?.parse().ok()?;
    let day: u32 = timestamp.get(8..10)?.parse().ok()?;
    let hour: u32 = timestamp.get(11..13)?.parse().ok()?;
    let minute: u32 = timestamp.get(14..16)?.parse().ok()?;
    let second: u32 = timestamp.get(17..19)?.parse().ok()?;
    if !(2020..2100).contains(&year) {
        return None;
    }

    Some((
        format!("{year:04}-{month:02}-{day:02}"),
        format!("{hour:02}-{minute:02}-{second:02}"),
    ))
}

/// Remove orphaned atomic write temporary files (`.*.tmp`) inside `dir` left behind by crashes.
pub fn sweep_stale_atomic_temps(dir: &Path) -> io::Result<usize> {
    let meta = match fs::symlink_metadata(dir) {
        Ok(m) => m,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err),
    };
    if meta.file_type().is_symlink() || !meta.is_dir() {
        return Ok(0);
    }

    let mut removed = 0_usize;
    for entry in fs::read_dir(dir)? {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        if !name.starts_with('.') || !name.ends_with(".tmp") || name.len() <= 5 {
            continue;
        }
        let path = entry.path();
        if let Ok(st) = fs::symlink_metadata(&path) {
            if st.file_type().is_file() && fs::remove_file(&path).is_ok() {
                removed += 1;
            }
        }
    }
    if removed > 0 {
        let _ = fsync_directory(dir);
    }
    Ok(removed)
}

/// Encode bytes with RFC 4648 URL-safe Base64 without padding.
pub fn base64url_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = u32::from(chunk[0]);
        let b1 = if chunk.len() > 1 {
            u32::from(chunk[1])
        } else {
            0
        };
        let b2 = if chunk.len() > 2 {
            u32::from(chunk[2])
        } else {
            0
        };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((triple >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((triple >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((triple >> 6) & 0x3f) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(TABLE[(triple & 0x3f) as usize] as char);
        }
    }
    out
}

/// Decode RFC 4648 URL-safe Base64 without padding.
pub fn base64url_decode(input: &str) -> Result<Vec<u8>, &'static str> {
    if input.is_empty() || input.len() % 4 == 1 {
        return Err("invalid base64url length");
    }
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity((bytes.len() * 3) / 4);
    let mut idx = 0;
    while idx < bytes.len() {
        let rem = bytes.len() - idx;
        let v0 = decode_b64_char(bytes[idx])?;
        let v1 = decode_b64_char(bytes[idx + 1])?;
        out.push((v0 << 2) | (v1 >> 4));
        if rem >= 3 {
            let v2 = decode_b64_char(bytes[idx + 2])?;
            out.push((v1 << 4) | (v2 >> 2));
            if rem >= 4 {
                let v3 = decode_b64_char(bytes[idx + 3])?;
                out.push((v2 << 6) | v3);
                idx += 4;
            } else {
                if (v2 & 0x03) != 0 {
                    return Err("non-canonical trailing base64url bits");
                }
                idx += 3;
            }
        } else {
            if (v1 & 0x0f) != 0 {
                return Err("non-canonical trailing base64url bits");
            }
            idx += 2;
        }
    }
    Ok(out)
}

fn decode_b64_char(byte: u8) -> Result<u8, &'static str> {
    match byte {
        b'A'..=b'Z' => Ok(byte - b'A'),
        b'a'..=b'z' => Ok(byte - b'a' + 26),
        b'0'..=b'9' => Ok(byte - b'0' + 52),
        b'-' => Ok(62),
        b'_' => Ok(63),
        _ => Err("invalid base64url character"),
    }
}

/// Validate a safe single path segment (not empty, not `.` or `..`, no `/` or `\`, no control chars).
pub fn is_safe_path_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment != "."
        && segment != ".."
        && !segment.contains('/')
        && !segment.contains('\\')
        && !segment.bytes().any(|b| b < 0x20 || b == 0x7f)
}

/// Validate `YYYY-MM-DD` date directory name.
pub fn is_date_dir_name(name: &str) -> bool {
    let b = name.as_bytes();
    b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && b[0..4].iter().all(u8::is_ascii_digit)
        && b[5..7].iter().all(u8::is_ascii_digit)
        && b[8..10].iter().all(u8::is_ascii_digit)
}

/// Validate `HH-MM-SS` or `HH-MM-SS_NN` time directory name.
pub fn is_time_dir_name(name: &str) -> bool {
    let (base, suffix) = match name.split_once('_') {
        Some((b, s)) => (b, Some(s)),
        None => (name, None),
    };
    let b = base.as_bytes();
    let base_ok = b.len() == 8
        && b[2] == b'-'
        && b[5] == b'-'
        && b[0..2].iter().all(u8::is_ascii_digit)
        && b[3..5].iter().all(u8::is_ascii_digit)
        && b[6..8].iter().all(u8::is_ascii_digit);
    if !base_ok {
        return false;
    }
    match suffix {
        None => true,
        Some(s) => !s.is_empty() && s.len() <= 5 && s.bytes().all(|c| c.is_ascii_digit()),
    }
}

/// Validate `recording_NN` fallback directory name under `unknown-date/`.
pub fn is_unknown_time_dir_name(name: &str) -> bool {
    match name.strip_prefix("recording_") {
        Some(digits) => {
            !digits.is_empty() && digits.len() <= 5 && digits.bytes().all(|c| c.is_ascii_digit())
        }
        None => false,
    }
}

pub const UNKNOWN_DATE_DIR_NAME: &str = "unknown-date";
