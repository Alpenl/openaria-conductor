//! Strict RFC 9110 single byte-range parsing, `If-Range` matching, and log path validation.

use std::fmt;

/// Inclusive RFC 9110 byte range `[first, last]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub first: u64,
    pub last: u64,
}

impl ByteRange {
    pub fn length(self) -> u64 {
        self.last - self.first + 1
    }

    pub fn content_range_header(self, complete_size: u64) -> String {
        format!("bytes {}-{}/{complete_size}", self.first, self.last)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnsatisfiableRange {
    pub complete_size: u64,
}

impl fmt::Display for UnsatisfiableRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "requested byte range cannot be satisfied for {} byte representation",
            self.complete_size
        )
    }
}

impl std::error::Error for UnsatisfiableRange {}

/// Parse a decimal string bounded above by `upper_bound` without integer overflow on giant inputs.
fn bounded_range_decimal(
    value: &str,
    upper_bound: u64,
    complete_size: u64,
) -> Result<u64, UnsatisfiableRange> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(UnsatisfiableRange { complete_size });
    }
    let trimmed = value.trim_start_matches('0');
    if trimmed.is_empty() {
        return Ok(0);
    }
    match trimmed.parse::<u64>() {
        Ok(parsed) if parsed <= upper_bound => Ok(parsed),
        _ => Ok(upper_bound.saturating_add(1)),
    }
}

/// Parse one RFC 9110 `bytes=...` range header against `complete_size`.
///
/// Rejects multiple ranges (containing `,`), invalid units, negative/non-digit offsets,
/// empty suffix lengths, and out-of-bounds ranges with `Err(UnsatisfiableRange)`.
pub fn parse_single_range(
    value: Option<&str>,
    complete_size: u64,
) -> Result<Option<ByteRange>, UnsatisfiableRange> {
    let Some(raw) = value else {
        return Ok(None);
    };
    let Some(selected) = raw.strip_prefix("bytes=") else {
        return Err(UnsatisfiableRange { complete_size });
    };
    if selected.contains(',') {
        return Err(UnsatisfiableRange { complete_size });
    }
    if let Some(suffix) = selected.strip_prefix('-') {
        let suffix_len = bounded_range_decimal(suffix, complete_size, complete_size)?;
        if suffix_len == 0 || complete_size == 0 {
            return Err(UnsatisfiableRange { complete_size });
        }
        let first = complete_size.saturating_sub(suffix_len);
        return Ok(Some(ByteRange {
            first,
            last: complete_size - 1,
        }));
    }

    let Some((first_text, last_text)) = selected.split_once('-') else {
        return Err(UnsatisfiableRange { complete_size });
    };
    if first_text.is_empty()
        || !first_text.bytes().all(|b| b.is_ascii_digit())
        || (!last_text.is_empty() && !last_text.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err(UnsatisfiableRange { complete_size });
    }
    let first = bounded_range_decimal(first_text, complete_size, complete_size)?;
    if first >= complete_size {
        return Err(UnsatisfiableRange { complete_size });
    }
    if last_text.is_empty() {
        return Ok(Some(ByteRange {
            first,
            last: complete_size - 1,
        }));
    }
    let last = bounded_range_decimal(last_text, complete_size, complete_size)?;
    if last < first {
        return Err(UnsatisfiableRange { complete_size });
    }
    Ok(Some(ByteRange {
        first,
        last: last.min(complete_size - 1),
    }))
}

/// Evaluate RFC 9110 `If-Range` against a strong quoted ETag.
/// Returns `true` when `Range` should be applied (`If-Range` absent or exact strong match).
pub fn if_range_allows_partial(if_range: Option<&str>, is_single: bool, etag: &str) -> bool {
    if !is_single {
        return false;
    }
    match if_range {
        None => true,
        Some(candidate) => candidate == etag,
    }
}

/// Validate a safe relative diagnostic log name per OpenAPI `LogName`:
/// `^[A-Za-z0-9][A-Za-z0-9._-]*(?:/[A-Za-z0-9][A-Za-z0-9._-]*)*$` (1..=255 bytes).
/// Rejects leading/trailing slashes, empty segments, `.`, `..`, dot-prefixed entries,
/// backslashes, percent signs, control chars, and whitespace.
pub fn is_valid_log_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 255 {
        return false;
    }
    for segment in name.split('/') {
        if !is_valid_log_segment(segment) {
            return false;
        }
    }
    true
}

/// Validate a safe relative diagnostic log prefix filter per OpenAPI `/logs?prefix=...`:
/// `^[A-Za-z0-9][A-Za-z0-9._-]*(?:/[A-Za-z0-9][A-Za-z0-9._-]*)*(?:/)?$` (1..=128 bytes).
pub fn is_valid_log_prefix(prefix: &str) -> bool {
    if prefix.is_empty() || prefix.len() > 128 {
        return false;
    }
    let trimmed = prefix.strip_suffix('/').unwrap_or(prefix);
    if trimmed.is_empty() {
        return false;
    }
    for segment in trimmed.split('/') {
        if !is_valid_log_segment(segment) {
            return false;
        }
    }
    true
}

fn is_valid_log_segment(segment: &str) -> bool {
    let bytes = segment.as_bytes();
    let Some(&first) = bytes.first() else {
        return false;
    };
    if !first.is_ascii_alphanumeric() {
        return false;
    }
    bytes
        .iter()
        .all(|&b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// Infer OpenAPI-declared media type for a diagnostic log file name.
pub fn infer_log_media_type(name: &str) -> &'static str {
    let lower = name.to_ascii_lowercase();
    if lower.ends_with(".ndjson") || lower.ends_with(".jsonl") {
        "application/x-ndjson"
    } else if lower.ends_with(".gz") || lower.ends_with(".gzip") {
        "application/gzip"
    } else if lower.ends_with(".log") || lower.ends_with(".txt") || lower.ends_with(".json") {
        "text/plain"
    } else {
        "application/octet-stream"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rfc9110_single_ranges_strictly() {
        assert_eq!(parse_single_range(None, 100).unwrap(), None);
        assert_eq!(
            parse_single_range(Some("bytes=0-9"), 100).unwrap(),
            Some(ByteRange { first: 0, last: 9 })
        );
        assert_eq!(
            parse_single_range(Some("bytes=90-999"), 100).unwrap(),
            Some(ByteRange {
                first: 90,
                last: 99
            })
        );
        assert_eq!(
            parse_single_range(Some("bytes=95-"), 100).unwrap(),
            Some(ByteRange {
                first: 95,
                last: 99
            })
        );
        assert_eq!(
            parse_single_range(Some("bytes=-10"), 100).unwrap(),
            Some(ByteRange {
                first: 90,
                last: 99
            })
        );
        assert_eq!(
            parse_single_range(Some("bytes=-9999999999999999999999999999"), 100).unwrap(),
            Some(ByteRange { first: 0, last: 99 })
        );

        // Unsatisfiable / invalid / multi-range
        assert!(parse_single_range(Some("bytes=0-1, 2-3"), 100).is_err());
        assert!(parse_single_range(Some("bytes=100-105"), 100).is_err());
        assert!(parse_single_range(Some("bytes=20-10"), 100).is_err());
        assert!(parse_single_range(Some("bytes=-0"), 100).is_err());
        assert!(parse_single_range(Some("bytes=0-0"), 0).is_err());
        assert!(parse_single_range(Some("items=0-5"), 100).is_err());
    }

    #[test]
    fn validates_log_paths_against_traversal_and_hidden_entries() {
        assert!(is_valid_log_name("device.log"));
        assert!(is_valid_log_name("wifi-watchdog/2026-11-26.ndjson"));
        assert!(is_valid_log_prefix("wifi-watchdog/"));
        assert!(is_valid_log_prefix("device"));

        for bad in [
            "",
            ".hidden",
            ".miro-ego/volume.json",
            "../etc/passwd",
            "wifi/../secret",
            "/etc/passwd",
            "wifi//a.log",
            "wifi/",
            "a\\b.log",
            "a%2eb.log",
        ] {
            assert!(!is_valid_log_name(bad), "expected rejection for {bad}");
        }
    }
}
