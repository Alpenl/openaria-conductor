//! Select live compaction inputs without constructing historical clock arrays.
//!
//! Recovery owns the complete checkpoint. This reader consumes the whole JSON
//! document but only materializes the fields live compression actually uses.

use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
pub struct Segment {
    pub index: u64,
    pub path: String,
}

#[derive(Deserialize)]
struct Checkpoint {
    segments: Vec<Segment>,
}

pub fn segments(payload: &[u8]) -> Result<Vec<Segment>, String> {
    // Ignored JSON strings need no allocation, but must still be valid UTF-8.
    let text = std::str::from_utf8(payload).map_err(|error| error.to_string())?;
    serde_json::from_str::<Checkpoint>(text)
        .map(|checkpoint| checkpoint.segments)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_only_segment_fields_and_preserves_order() {
        let payload = br#"{"anchors":[[1,2],[3,4]],"segments":[{"index":9,"path":"audio/a.wav","start_sample":0},{"index":0,"path":"audio/b.wav"}]}"#;
        let parsed = segments(payload).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(
            parsed[0],
            Segment {
                index: 9,
                path: "audio/a.wav".into()
            }
        );
        assert_eq!(parsed[1].index, 0);
    }

    #[test]
    fn rejects_bad_ignored_json_and_trailing_content() {
        for payload in [
            &b"{\"segments\":[],\"anchors\":[[1,]]}"[..],
            &b"{\"segments\":[]}x"[..],
            &b"{\"segments\":[],\"unknown\":\"\xff\"}"[..],
            &b"{\"segments\":[],\"unknown\":\"\\q\"}"[..],
        ] {
            assert!(segments(payload).is_err());
        }
    }

    #[test]
    fn rejects_duplicate_and_invalid_selected_fields() {
        for payload in [
            "{}",
            r#"{"segments":null}"#,
            r#"{"segments":[],"segments":[]}"#,
            r#"{"segments":[{"index":true,"path":"x"}]}"#,
            r#"{"segments":[{"index":-1,"path":"x"}]}"#,
            r#"{"segments":[{"index":0,"index":1,"path":"x"}]}"#,
        ] {
            assert!(segments(payload.as_bytes()).is_err());
        }
    }
}
