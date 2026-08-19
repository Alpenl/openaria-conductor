#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordingError {
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

impl RecordingError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

pub(crate) fn jpeg_payload(payload: &[u8]) -> Result<&[u8], RecordingError> {
    let start = payload
        .windows(2)
        .position(|window| window == b"\xff\xd8")
        .ok_or_else(|| RecordingError::new("bad_frame", "原始 SBS 帧不是完整 JPEG"))?;
    let end = payload
        .windows(2)
        .rposition(|window| window == b"\xff\xd9")
        .filter(|end| *end >= start)
        .ok_or_else(|| RecordingError::new("bad_frame", "原始 SBS 帧不是完整 JPEG"))?;
    Ok(&payload[start..end + 2])
}

pub(crate) fn split_frame_index_record(
    session_id: &str,
    frame: u64,
    source_sequence: u64,
    host_monotonic_ns: u64,
    segment_index: u64,
    segment_frame: u64,
) -> Vec<u8> {
    let session_id = json_string(session_id);
    format!(
        "{{\"frame\":{frame},\"host_monotonic_ns\":{host_monotonic_ns},\
         \"schema\":\"ylx.frame-index.v1\",\"segment_frame\":{segment_frame},\
         \"segment_index\":{segment_index},\"session_id\":{session_id},\
         \"source_sequence\":{source_sequence}}}\n"
    )
    .into_bytes()
}

pub(crate) fn raw_frame_index_record(
    session_id: &str,
    frame: u64,
    source_sequence: u64,
    host_monotonic_ns: u64,
    video_offset: u64,
    video_bytes: u64,
) -> Vec<u8> {
    let session_id = json_string(session_id);
    format!(
        "{{\"frame\":{frame},\"host_monotonic_ns\":{host_monotonic_ns},\
         \"schema\":\"ylx.frame-index.v1\",\"session_id\":{session_id},\
         \"source_sequence\":{source_sequence},\"video_bytes\":{video_bytes},\
         \"video_offset\":{video_offset}}}\n"
    )
    .into_bytes()
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn imu_sample_record(
    session_id: &str,
    sequence: u64,
    packet_sequence: u64,
    sample_index: u8,
    device_timestamp_raw: u32,
    device_ticks: u64,
    host_read_start_ns: u64,
    host_read_end_ns: u64,
    host_monotonic_ns: u64,
    accelerometer: (i16, i16, i16),
    gyroscope: (i16, i16, i16),
    sync_offset_ns: Option<i64>,
    sync_residual_ns: Option<u64>,
    sync_quality: &str,
) -> Vec<u8> {
    let session_id = json_string(session_id);
    let sync_quality = json_string(sync_quality);
    let sync_offset = optional_i64(sync_offset_ns);
    let sync_residual = optional_u64(sync_residual_ns);
    format!(
        "{{\"device_ticks\":{device_ticks},\"device_timestamp_raw\":{device_timestamp_raw},\
         \"format\":\"ylx.imu.v0\",\"host_monotonic_ns\":{host_monotonic_ns},\
         \"host_read_end_ns\":{host_read_end_ns},\"host_read_start_ns\":{host_read_start_ns},\
         \"packet_sequence\":{packet_sequence},\"raw\":{{\"accelerometer\":[{},{},{}],\
         \"gyroscope\":[{},{},{}]}},\"sequence\":{sequence},\"session_id\":{session_id},\
         \"sample_index\":{sample_index},\"sync\":{{\"offset_ns\":{sync_offset},\
         \"quality\":{sync_quality},\"residual_ns\":{sync_residual}}}}}\n",
        accelerometer.0, accelerometer.1, accelerometer.2, gyroscope.0, gyroscope.1, gyroscope.2,
    )
    .into_bytes()
}

fn optional_i64(value: Option<i64>) -> String {
    value.map_or_else(|| "null".to_owned(), |number| number.to_string())
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "null".to_owned(), |number| number.to_string())
}

fn json_string(value: &str) -> String {
    let mut result = String::with_capacity(value.len() + 2);
    result.push('"');
    for character in value.chars() {
        match character {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\u{08}' => result.push_str("\\b"),
            '\u{0c}' => result.push_str("\\f"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            character if character < '\u{20}' => {
                result.push_str(&format!("\\u{:04x}", character as u32));
            }
            character => result.push(character),
        }
    }
    result.push('"');
    result
}

#[cfg(test)]
mod tests {
    use super::{
        imu_sample_record, jpeg_payload, raw_frame_index_record, split_frame_index_record,
    };

    #[test]
    fn extracts_enclosing_jpeg_payload() {
        assert_eq!(
            jpeg_payload(b"pad\xff\xd8one\xff\xd9tail").unwrap(),
            b"\xff\xd8one\xff\xd9"
        );
        assert!(jpeg_payload(b"missing").is_err());
    }

    #[test]
    fn encodes_frame_index_like_python_compact_sorted_json() {
        assert_eq!(
            split_frame_index_record("session", 7, 99, 1234, 2, 3),
            b"{\"frame\":7,\"host_monotonic_ns\":1234,\"schema\":\"ylx.frame-index.v1\",\
              \"segment_frame\":3,\"segment_index\":2,\"session_id\":\"session\",\
              \"source_sequence\":99}\n"
        );
        assert_eq!(
            raw_frame_index_record("session", 7, 99, 1234, 400, 50),
            b"{\"frame\":7,\"host_monotonic_ns\":1234,\"schema\":\"ylx.frame-index.v1\",\
              \"session_id\":\"session\",\"source_sequence\":99,\"video_bytes\":50,\
              \"video_offset\":400}\n"
        );
    }

    #[test]
    fn encodes_imu_sample_like_python_compact_sorted_json() {
        let record = imu_sample_record(
            "session",
            1,
            2,
            1,
            1000,
            2000,
            10,
            20,
            15,
            (1, -2, 3),
            (4, 5, -6),
            None,
            Some(100),
            "good",
        );
        assert_eq!(
            record,
            b"{\"device_ticks\":2000,\"device_timestamp_raw\":1000,\"format\":\"ylx.imu.v0\",\
              \"host_monotonic_ns\":15,\"host_read_end_ns\":20,\"host_read_start_ns\":10,\
              \"packet_sequence\":2,\"raw\":{\"accelerometer\":[1,-2,3],\
              \"gyroscope\":[4,5,-6]},\"sequence\":1,\"session_id\":\"session\",\
              \"sample_index\":1,\"sync\":{\"offset_ns\":null,\"quality\":\"good\",\
              \"residual_ns\":100}}\n"
        );
    }
}
