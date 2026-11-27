//! Shared pure-value media and encoder DTOs used across capture and storage layers.

use serde::{Deserialize, Serialize};

/// Completed stereo MP4 segment reported by `miro-ego-encoder` on `stdout`.
///
/// Includes optional streaming `left_sha256` / `right_sha256` digests computed while
/// segment bytes are written so session sealing skips rereading the MP4 segments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentEvent {
    pub index: u64,
    pub start_frame: u64,
    pub end_frame: u64,
    pub left_path: String,
    pub left_bytes: u64,
    pub left_sha256: Option<String>,
    pub right_path: String,
    pub right_bytes: u64,
    pub right_sha256: Option<String>,
}
