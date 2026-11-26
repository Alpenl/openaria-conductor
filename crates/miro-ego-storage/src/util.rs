//! Shared pure-Rust utilities for CRC32 checksums, RFC 3339 timestamps,
//! domain-separated SHA-256 digests, and base64url cursor encoding.

use miro_ego_core::ids::hex_encode;
use sha2::{Digest, Sha256};
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

/// Extract `(YYYY-MM-DD, HH-MM-SS)` for readable session publication from an RFC 3339 timestamp.
///
/// Requires a timezone-aware timestamp (`Z` or `±HH:MM`) and year in `2020..2100`.
pub fn readable_date_time_from_rfc3339(timestamp: &str) -> Option<(String, String)> {
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

    if !(2020..2100).contains(&year)
        || !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }

    // Require an explicit timezone designator (`Z`/`z` or `±HH:MM`) after optional fractional secs.
    let mut rest = timestamp.get(19..)?;
    if let Some(after_dot) = rest.strip_prefix('.') {
        let frac_len = after_dot.bytes().take_while(u8::is_ascii_digit).count();
        if frac_len == 0 {
            return None;
        }
        rest = after_dot.get(frac_len..)?;
    }
    let tz_valid = rest == "Z"
        || rest == "z"
        || (rest.len() == 6
            && matches!(rest.as_bytes()[0], b'+' | b'-')
            && rest.as_bytes()[3] == b':'
            && rest
                .get(1..3)
                .and_then(|h| h.parse::<u32>().ok())
                .is_some_and(|h| h <= 23)
            && rest
                .get(4..6)
                .and_then(|m| m.parse::<u32>().ok())
                .is_some_and(|m| m <= 59));
    if !tz_valid {
        return None;
    }

    Some((
        format!("{year:04}-{month:02}-{day:02}"),
        format!("{hour:02}-{minute:02}-{second:02}"),
    ))
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
