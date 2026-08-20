use crate::session_io;
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const READY_TIMEOUT: Duration = Duration::from_secs(15);

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EncoderProcessError {
    pub(crate) code: String,
    pub(crate) message: String,
}

impl EncoderProcessError {
    fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

impl From<EncoderEventError> for EncoderProcessError {
    fn from(error: EncoderEventError) -> Self {
        Self::new(error.code, error.message)
    }
}

impl From<session_io::SessionIoError> for EncoderProcessError {
    fn from(error: session_io::SessionIoError) -> Self {
        Self::new(error.code, error.message)
    }
}

struct ProcessState {
    ready: bool,
    closed: bool,
    failure: Option<EncoderProcessError>,
    segments: Vec<SegmentEvent>,
    stats: BTreeMap<String, i64>,
}

impl ProcessState {
    fn new() -> Self {
        Self {
            ready: false,
            closed: false,
            failure: None,
            segments: Vec::new(),
            stats: BTreeMap::new(),
        }
    }
}

struct SharedProcess {
    state: Mutex<ProcessState>,
    changed: Condvar,
}

pub(crate) struct EncoderProcess {
    executable: PathBuf,
    args: Vec<String>,
    shared: Arc<SharedProcess>,
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    reader: Option<JoinHandle<()>>,
    submitted: u64,
}

impl EncoderProcess {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        out_dir: &Path,
        executable: &Path,
        width: u64,
        height: u64,
        fps: u64,
        bitrate_kbps: u64,
        segment_frames: u64,
        path_prefix: &str,
    ) -> Result<Self, EncoderProcessError> {
        if width == 0
            || width % 4 != 0
            || height == 0
            || fps == 0
            || bitrate_kbps == 0
            || segment_frames == 0
        {
            return Err(EncoderProcessError::new(
                "invalid_argument",
                "边录边编码参数无效",
            ));
        }
        Ok(Self {
            executable: executable.to_path_buf(),
            args: vec![
                "--out-dir".to_owned(),
                out_dir.to_string_lossy().into_owned(),
                "--path-prefix".to_owned(),
                path_prefix.to_owned(),
                "--width".to_owned(),
                width.to_string(),
                "--height".to_owned(),
                height.to_string(),
                "--fps".to_owned(),
                fps.to_string(),
                "--bitrate-kbps".to_owned(),
                bitrate_kbps.to_string(),
                "--segment-frames".to_owned(),
                segment_frames.to_string(),
            ],
            shared: Arc::new(SharedProcess {
                state: Mutex::new(ProcessState::new()),
                changed: Condvar::new(),
            }),
            child: None,
            stdin: None,
            reader: None,
            submitted: 0,
        })
    }

    pub(crate) fn start(&mut self) -> Result<(), EncoderProcessError> {
        if self.child.is_some() {
            return Err(EncoderProcessError::new(
                "invalid_state",
                "助手进程只能启动一次",
            ));
        }
        {
            let mut state = self.shared.state.lock().map_err(|_| {
                EncoderProcessError::new("encoder_failed", "encoder process mutex is poisoned")
            })?;
            *state = ProcessState::new();
        }
        let mut child = Command::new(&self.executable)
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| {
                EncoderProcessError::new("encoder_unavailable", format!("无法启动助手：{error}"))
            })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| EncoderProcessError::new("encoder_failed", "无法打开助手事件输出"))?;
        self.stdin = child.stdin.take();
        let shared = Arc::clone(&self.shared);
        self.reader = Some(thread::spawn(move || {
            read_events(stdout, shared);
        }));
        self.child = Some(child);
        if self.stdin.is_none() {
            self.abort();
            return Err(EncoderProcessError::new(
                "encoder_failed",
                "无法打开助手输入",
            ));
        }
        self.wait_ready()
    }

    pub(crate) fn submit(&mut self, jpeg: &[u8]) -> Result<u64, EncoderProcessError> {
        self.raise_if_failed()?;
        let stdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| EncoderProcessError::new("invalid_state", "助手进程未启动"))?;
        let written = session_io::write_encoder_frame(stdin.as_raw_fd(), jpeg)?;
        self.submitted = self.submitted.checked_add(1).ok_or_else(|| {
            EncoderProcessError::new(
                "counter_overflow",
                "encoder submitted frame count overflowed",
            )
        })?;
        Ok(written)
    }

    pub(crate) fn finish(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<SegmentEvent>, EncoderProcessError> {
        if timeout.is_zero() {
            return Err(EncoderProcessError::new(
                "invalid_argument",
                "finish timeout must be positive",
            ));
        }
        if self.child.is_none() {
            return Err(EncoderProcessError::new("invalid_state", "助手进程未启动"));
        }
        if let Some(mut stdin) = self.stdin.take() {
            let _ = session_io::write_encoder_frame(stdin.as_raw_fd(), &[]);
            let _ = stdin.flush();
        }
        let status = {
            let child = self.child.as_mut().expect("child checked above");
            wait_child(child, timeout).map_err(|error| {
                EncoderProcessError::new("encoder_failed", format!("助手等待失败：{error}"))
            })?
        };
        let Some(status) = status else {
            self.abort();
            return Err(EncoderProcessError::new(
                "encoder_failed",
                "助手未在期限内退出",
            ));
        };
        self.join_reader()?;
        self.raise_if_failed()?;
        if !status.success() {
            return Err(EncoderProcessError::new(
                "encoder_failed",
                format!("助手退出码 {}", status.code().unwrap_or(-1)),
            ));
        }
        Ok(self.segments())
    }

    pub(crate) fn abort(&mut self) {
        let _ = self.stdin.take();
        if let Some(child) = self.child.as_mut() {
            if matches!(child.try_wait(), Ok(None)) {
                let _ = child.kill();
            }
            let _ = child.wait();
        }
        let _ = self.join_reader();
    }

    pub(crate) fn segments(&self) -> Vec<SegmentEvent> {
        match self.shared.state.lock() {
            Ok(state) => state.segments.clone(),
            Err(_) => Vec::new(),
        }
    }

    pub(crate) fn stats(&self) -> BTreeMap<String, i64> {
        match self.shared.state.lock() {
            Ok(state) => state.stats.clone(),
            Err(_) => BTreeMap::new(),
        }
    }

    pub(crate) fn submitted_frames(&self) -> u64 {
        self.submitted
    }

    fn wait_ready(&mut self) -> Result<(), EncoderProcessError> {
        let deadline = Instant::now() + READY_TIMEOUT;
        let mut state = self.shared.state.lock().map_err(|_| {
            EncoderProcessError::new("encoder_failed", "encoder process mutex is poisoned")
        })?;
        while !state.ready && !state.closed && state.failure.is_none() {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let timeout = deadline.saturating_duration_since(now);
            let (next_state, wait_result) = self
                .shared
                .changed
                .wait_timeout(state, timeout)
                .map_err(|_| {
                    EncoderProcessError::new("encoder_failed", "encoder process mutex is poisoned")
                })?;
            state = next_state;
            if wait_result.timed_out() {
                break;
            }
        }
        if let Some(error) = state.failure.clone() {
            drop(state);
            self.abort();
            return Err(error);
        }
        if !state.ready {
            let closed = state.closed;
            drop(state);
            self.abort();
            return Err(EncoderProcessError::new(
                "encoder_unavailable",
                if closed {
                    "助手未就绪就退出"
                } else {
                    "助手未在期限内就绪"
                },
            ));
        }
        Ok(())
    }

    fn raise_if_failed(&self) -> Result<(), EncoderProcessError> {
        let state = self.shared.state.lock().map_err(|_| {
            EncoderProcessError::new("encoder_failed", "encoder process mutex is poisoned")
        })?;
        if let Some(error) = state.failure.clone() {
            return Err(error);
        }
        Ok(())
    }

    fn join_reader(&mut self) -> Result<(), EncoderProcessError> {
        if let Some(reader) = self.reader.take() {
            if reader.join().is_err() {
                return Err(EncoderProcessError::new(
                    "encoder_failed",
                    "助手事件线程崩溃",
                ));
            }
        }
        Ok(())
    }
}

impl Drop for EncoderProcess {
    fn drop(&mut self) {
        self.abort();
    }
}

fn read_events(stdout: std::process::ChildStdout, shared: Arc<SharedProcess>) {
    let reader = BufReader::new(stdout);
    for line in reader.split(b'\n') {
        match line {
            Ok(line) => {
                if let Err(error) = handle_event(&shared, &line) {
                    set_failure(&shared, error);
                    break;
                }
            }
            Err(error) => {
                set_failure(
                    &shared,
                    EncoderProcessError::new("encoder_failed", format!("助手事件流中断：{error}")),
                );
                break;
            }
        }
    }
    if let Ok(mut state) = shared.state.lock() {
        state.closed = true;
        shared.changed.notify_all();
    }
}

fn handle_event(shared: &SharedProcess, line: &[u8]) -> Result<(), EncoderProcessError> {
    let Some(event) = parse_event(line)? else {
        return Ok(());
    };
    let mut state = shared.state.lock().map_err(|_| {
        EncoderProcessError::new("encoder_failed", "encoder process mutex is poisoned")
    })?;
    match event {
        EncoderEvent::Ready => {
            state.ready = true;
        }
        EncoderEvent::Segment(segment) => {
            state.segments.push(segment);
        }
        EncoderEvent::Done(stats) => {
            state.stats = stats;
        }
        EncoderEvent::Error { code, message } => {
            state.failure = Some(EncoderProcessError::new(code, message));
        }
    }
    shared.changed.notify_all();
    Ok(())
}

fn set_failure(shared: &SharedProcess, error: EncoderProcessError) {
    if let Ok(mut state) = shared.state.lock() {
        if state.failure.is_none() {
            state.failure = Some(error);
        }
        shared.changed.notify_all();
    }
}

fn wait_child(
    child: &mut Child,
    timeout: Duration,
) -> std::io::Result<Option<std::process::ExitStatus>> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(Some(status));
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        thread::sleep(Duration::from_millis(10));
    }
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
