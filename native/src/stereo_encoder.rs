use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EncoderEventError {
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

impl EncoderEventError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentEvent {
    pub(crate) index: u64,
    pub(crate) start_frame: u64,
    pub(crate) end_frame: u64,
    pub(crate) left_path: String,
    pub(crate) left_bytes: u64,
    pub(crate) right_path: String,
    pub(crate) right_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EncoderEvent {
    Ready,
    Segment(SegmentEvent),
    Done(BTreeMap<String, i64>),
    Error { code: String, message: String },
}

pub(crate) fn parse_event(line: &[u8]) -> Result<Option<EncoderEvent>, EncoderEventError> {
    let text = std::str::from_utf8(line)
        .map_err(|_| EncoderEventError::new("encoder_failed", "助手输出不是 UTF-8"))?
        .trim();
    if text.is_empty() {
        return Ok(None);
    }
    let value: Value = serde_json::from_str(text)
        .map_err(|_| EncoderEventError::new("encoder_failed", "助手输出不是 JSON"))?;
    let object = value
        .as_object()
        .ok_or_else(|| EncoderEventError::new("encoder_failed", "助手输出 JSON 不是对象"))?;
    let Some(kind) = object.get("event").and_then(Value::as_str) else {
        return Ok(None);
    };
    match kind {
        "ready" => Ok(Some(EncoderEvent::Ready)),
        "segment" => Ok(Some(EncoderEvent::Segment(parse_segment(object)?))),
        "done" => {
            let stats = object
                .iter()
                .filter_map(|(key, value)| {
                    if key == "event" {
                        None
                    } else {
                        value.as_i64().map(|number| (key.clone(), number))
                    }
                })
                .collect();
            Ok(Some(EncoderEvent::Done(stats)))
        }
        "error" => Ok(Some(EncoderEvent::Error {
            code: object
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or("encoder_failed")
                .to_owned(),
            message: object
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("助手报告失败")
                .to_owned(),
        })),
        _ => Ok(None),
    }
}

fn parse_segment(
    object: &serde_json::Map<String, Value>,
) -> Result<SegmentEvent, EncoderEventError> {
    Ok(SegmentEvent {
        index: required_u64(object, "index")?,
        start_frame: required_u64(object, "start_frame")?,
        end_frame: required_u64(object, "end_frame")?,
        left_path: required_artifact_path(object, "left")?,
        left_bytes: required_artifact_bytes(object, "left")?,
        right_path: required_artifact_path(object, "right")?,
        right_bytes: required_artifact_bytes(object, "right")?,
    })
}

fn required_u64(
    object: &serde_json::Map<String, Value>,
    key: &'static str,
) -> Result<u64, EncoderEventError> {
    object
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| EncoderEventError::new("encoder_failed", format!("助手 segment 缺少 {key}")))
}

fn required_artifact_path(
    object: &serde_json::Map<String, Value>,
    key: &'static str,
) -> Result<String, EncoderEventError> {
    object
        .get(key)
        .and_then(Value::as_object)
        .and_then(|artifact| artifact.get("path"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            EncoderEventError::new("encoder_failed", format!("助手 segment 缺少 {key}.path"))
        })
}

fn required_artifact_bytes(
    object: &serde_json::Map<String, Value>,
    key: &'static str,
) -> Result<u64, EncoderEventError> {
    object
        .get(key)
        .and_then(Value::as_object)
        .and_then(|artifact| artifact.get("bytes"))
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            EncoderEventError::new("encoder_failed", format!("助手 segment 缺少 {key}.bytes"))
        })
}

#[cfg(test)]
mod tests {
    use super::{EncoderEvent, SegmentEvent, parse_event};
    use std::collections::BTreeMap;

    #[test]
    fn parses_ready_segment_done_and_error_events() {
        assert_eq!(
            parse_event(br#"{"event":"ready"}"#).unwrap(),
            Some(EncoderEvent::Ready)
        );
        assert_eq!(
            parse_event(
                br#"{"event":"segment","index":2,"start_frame":10,"end_frame":20,"left":{"path":"video/left_00002.mp4","bytes":123},"right":{"path":"video/right_00002.mp4","bytes":456}}"#,
            )
            .unwrap(),
            Some(EncoderEvent::Segment(SegmentEvent {
                index: 2,
                start_frame: 10,
                end_frame: 20,
                left_path: "video/left_00002.mp4".to_owned(),
                left_bytes: 123,
                right_path: "video/right_00002.mp4".to_owned(),
                right_bytes: 456,
            }))
        );
        assert_eq!(
            parse_event(br#"{"event":"done","frames":7,"ignored":"x"}"#).unwrap(),
            Some(EncoderEvent::Done(BTreeMap::from([(
                "frames".to_owned(),
                7
            )])))
        );
        assert_eq!(
            parse_event(br#"{"event":"error","code":"vpu_failed","message":"bad"}"#).unwrap(),
            Some(EncoderEvent::Error {
                code: "vpu_failed".to_owned(),
                message: "bad".to_owned(),
            })
        );
    }

    #[test]
    fn ignores_empty_and_unknown_events_but_rejects_malformed_json() {
        assert_eq!(parse_event(b"\n").unwrap(), None);
        assert_eq!(parse_event(br#"{"event":"progress"}"#).unwrap(), None);
        let error = parse_event(b"not-json").unwrap_err();
        assert_eq!(error.code, "encoder_failed");
        assert_eq!(error.message, "助手输出不是 JSON");
    }
}
