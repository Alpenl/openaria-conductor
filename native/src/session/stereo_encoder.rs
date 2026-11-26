use crate::session::session_io;
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::io::{BufRead, BufReader, Read};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const READY_TIMEOUT: Duration = Duration::from_secs(15);
const ABORT_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_EVENT_LINE_BYTES: usize = 64 * 1024;
const MAX_STDERR_BYTES: usize = 8 * 1024;

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
    stderr_ring: VecDeque<u8>,
}

impl ProcessState {
    fn new() -> Self {
        Self {
            ready: false,
            closed: false,
            failure: None,
            segments: Vec::new(),
            stats: BTreeMap::new(),
            stderr_ring: VecDeque::new(),
        }
    }

    fn append_stderr(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        if bytes.len() >= MAX_STDERR_BYTES {
            self.stderr_ring.clear();
            self.stderr_ring
                .extend(bytes[bytes.len() - MAX_STDERR_BYTES..].iter().copied());
            return;
        }
        let excess = (self.stderr_ring.len() + bytes.len()).saturating_sub(MAX_STDERR_BYTES);
        if excess > 0 {
            self.stderr_ring.drain(..excess);
        }
        self.stderr_ring.extend(bytes.iter().copied());
    }

    fn stderr_snippet(&self) -> Option<String> {
        if self.stderr_ring.is_empty() {
            return None;
        }
        let bytes: Vec<u8> = self.stderr_ring.iter().copied().collect();
        let text = String::from_utf8_lossy(&bytes).trim().to_owned();
        if text.is_empty() { None } else { Some(text) }
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
    stderr_reader: Option<JoinHandle<()>>,
    submitted: u64,
}

pub(crate) struct EncoderConfig<'a> {
    pub(crate) out_dir: &'a Path,
    pub(crate) executable: &'a Path,
    pub(crate) width: u64,
    pub(crate) height: u64,
    pub(crate) fps: u64,
    pub(crate) bitrate_kbps: u64,
    pub(crate) segment_frames: u64,
    pub(crate) path_prefix: &'a str,
}

impl EncoderProcess {
    pub(crate) fn new(config: EncoderConfig<'_>) -> Result<Self, EncoderProcessError> {
        if config.width == 0
            || config.width % 4 != 0
            || config.height == 0
            || config.fps == 0
            || config.bitrate_kbps == 0
            || config.segment_frames == 0
        {
            return Err(EncoderProcessError::new(
                "invalid_argument",
                "边录边编码参数无效",
            ));
        }
        Ok(Self {
            executable: config.executable.to_path_buf(),
            args: vec![
                "--out-dir".to_owned(),
                config.out_dir.to_string_lossy().into_owned(),
                "--path-prefix".to_owned(),
                config.path_prefix.to_owned(),
                "--width".to_owned(),
                config.width.to_string(),
                "--height".to_owned(),
                config.height.to_string(),
                "--fps".to_owned(),
                config.fps.to_string(),
                "--bitrate-kbps".to_owned(),
                config.bitrate_kbps.to_string(),
                "--segment-frames".to_owned(),
                config.segment_frames.to_string(),
            ],
            shared: Arc::new(SharedProcess {
                state: Mutex::new(ProcessState::new()),
                changed: Condvar::new(),
            }),
            child: None,
            stdin: None,
            reader: None,
            stderr_reader: None,
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
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| {
                EncoderProcessError::new("encoder_unavailable", format!("无法启动助手：{error}"))
            })?;
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        self.stdin = child.stdin.take();
        self.child = Some(child);
        let Some(stdout) = stdout else {
            self.abort();
            return Err(EncoderProcessError::new(
                "encoder_failed",
                "无法打开助手事件输出",
            ));
        };
        let shared = Arc::clone(&self.shared);
        self.reader = Some(
            match thread::Builder::new()
                .name("encoder-events".to_owned())
                .spawn(move || {
                    read_events(stdout, shared);
                }) {
                Ok(handle) => handle,
                Err(error) => {
                    self.abort();
                    return Err(EncoderProcessError::new(
                        "encoder_failed",
                        format!("启动助手事件线程失败：{error}"),
                    ));
                }
            },
        );
        if let Some(stderr) = stderr {
            let shared_err = Arc::clone(&self.shared);
            self.stderr_reader = Some(
                match thread::Builder::new()
                    .name("encoder-stderr".to_owned())
                    .spawn(move || {
                        read_stderr(stderr, shared_err);
                    }) {
                    Ok(handle) => handle,
                    Err(error) => {
                        self.abort();
                        return Err(EncoderProcessError::new(
                            "encoder_failed",
                            format!("启动助手错误流线程失败：{error}"),
                        ));
                    }
                },
            );
        }
        if self.stdin.is_none() {
            self.abort();
            return Err(EncoderProcessError::new(
                "encoder_failed",
                "无法打开助手输入",
            ));
        }
        self.wait_ready()
    }

    pub(crate) fn configure(&mut self, args: &[String]) -> Result<(), EncoderProcessError> {
        if self.child.is_some() || args.len() % 2 != 0 {
            return Err(EncoderProcessError::new(
                "invalid_argument",
                "invalid encoder settings",
            ));
        }
        for pair in args.chunks_exact(2) {
            if !matches!(
                pair[0].as_str(),
                "--codec"
                    | "--rc-mode"
                    | "--bitrate-kbps"
                    | "--min-qp"
                    | "--max-qp"
                    | "--intra-qp"
                    | "--initial-qp"
                    | "--intra-period"
                    | "--vbv-ms"
            ) || pair[1].starts_with('-')
            {
                return Err(EncoderProcessError::new(
                    "invalid_argument",
                    "unsupported encoder setting",
                ));
            }
        }
        self.args.extend_from_slice(args);
        Ok(())
    }

    pub(crate) fn submit(&mut self, jpeg: &[u8]) -> Result<u64, EncoderProcessError> {
        self.raise_if_failed()?;
        let stdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| EncoderProcessError::new("invalid_state", "助手进程未启动"))?;
        let written = match session_io::write_encoder_frame(stdin.as_raw_fd(), jpeg) {
            Ok(written) => written,
            Err(error) => {
                // Prefer the helper's causal error over the subsequent EPIPE.
                self.raise_if_failed()?;
                return Err(error.into());
            }
        };
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
        // EOF is the helper's normal finish signal. Writing a terminator to a
        // full pipe could block forever before wait_child's timeout even began.
        drop(self.stdin.take());
        let status = {
            let child = self
                .child
                .as_mut()
                .ok_or_else(|| EncoderProcessError::new("invalid_state", "助手进程未启动"))?;
            wait_child(child, timeout).map_err(|error| {
                EncoderProcessError::new("encoder_failed", format!("助手等待失败：{error}"))
            })?
        };
        let Some(status) = status else {
            self.abort();
            return Err(EncoderProcessError::new(
                "encoder_failed",
                self.with_stderr("助手未在期限内退出"),
            ));
        };
        self.join_readers()?;
        self.raise_if_failed()?;
        if !status.success() {
            return Err(EncoderProcessError::new(
                "encoder_failed",
                self.with_stderr(format!("助手退出码 {}", status.code().unwrap_or(-1))),
            ));
        }
        Ok(self.segments())
    }

    pub(crate) fn abort(&mut self) {
        let _ = self.stdin.take();
        let mut child_reaped = true;
        if let Some(mut child) = self.child.take() {
            if matches!(child.try_wait(), Ok(None)) {
                let _ = child.kill();
            }
            match wait_child(&mut child, ABORT_WAIT_TIMEOUT) {
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => {
                    // Uninterruptible kernel D-state child cannot be waited forever on close.
                    child_reaped = false;
                }
            }
        }
        if child_reaped {
            let _ = self.join_readers();
        } else {
            // Child pipes may remain open until kernel unblocks; detach reader handles.
            self.reader.take();
            self.stderr_reader.take();
        }
    }

    pub(crate) fn segments(&self) -> Vec<SegmentEvent> {
        match self.shared.state.lock() {
            Ok(state) => state.segments.clone(),
            Err(_) => Vec::new(),
        }
    }

    pub(crate) fn segments_since(
        &self,
        offset: usize,
    ) -> Result<Vec<SegmentEvent>, EncoderProcessError> {
        let state = self.shared.state.lock().map_err(|_| {
            EncoderProcessError::new("encoder_failed", "encoder process mutex is poisoned")
        })?;
        let tail = state.segments.get(offset..).ok_or_else(|| {
            EncoderProcessError::new("segment_invalid", "encoder segment list regressed")
        })?;
        Ok(tail.to_vec())
    }

    pub(crate) fn stats(&self) -> BTreeMap<String, i64> {
        match self.shared.state.lock() {
            Ok(state) => state.stats.clone(),
            Err(_) => BTreeMap::new(),
        }
    }

    pub(crate) fn failure(&self) -> Option<EncoderProcessError> {
        self.shared.state.lock().ok()?.failure.clone()
    }

    pub(crate) fn submitted_frames(&self) -> u64 {
        self.submitted
    }

    fn with_stderr(&self, message: impl Into<String>) -> String {
        let base = message.into();
        let snippet = self
            .shared
            .state
            .lock()
            .ok()
            .and_then(|state| state.stderr_snippet());
        match snippet {
            Some(stderr) => format!("{base}; stderr={stderr}"),
            None => base,
        }
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
                self.with_stderr(if closed {
                    "助手未就绪就退出"
                } else {
                    "助手未在期限内就绪"
                }),
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

    fn join_readers(&mut self) -> Result<(), EncoderProcessError> {
        if let Some(stderr_reader) = self.stderr_reader.take() {
            let _ = stderr_reader.join();
        }
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

fn read_bounded_line<R: BufRead>(
    reader: &mut R,
    limit: usize,
) -> Result<Option<Vec<u8>>, EncoderProcessError> {
    let mut line = Vec::new();
    loop {
        let available = match reader.fill_buf() {
            Ok(buf) => buf,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => {
                return Err(EncoderProcessError::new(
                    "encoder_failed",
                    format!("助手事件流中断：{error}"),
                ));
            }
        };
        if available.is_empty() {
            if line.is_empty() {
                return Ok(None);
            }
            return Ok(Some(line));
        }
        if let Some(pos) = available.iter().position(|&b| b == b'\n') {
            if line.len() + pos > limit {
                return Err(EncoderProcessError::new(
                    "encoder_failed",
                    "助手输出行超出长度限制",
                ));
            }
            line.extend_from_slice(&available[..pos]);
            reader.consume(pos + 1);
            return Ok(Some(line));
        }
        let consumed = available.len();
        if line.len() + consumed > limit {
            return Err(EncoderProcessError::new(
                "encoder_failed",
                "助手输出行超出长度限制",
            ));
        }
        line.extend_from_slice(available);
        reader.consume(consumed);
    }
}

fn read_events(stdout: std::process::ChildStdout, shared: Arc<SharedProcess>) {
    let mut reader = BufReader::new(stdout);
    loop {
        match read_bounded_line(&mut reader, MAX_EVENT_LINE_BYTES) {
            Ok(Some(line)) => {
                if let Err(error) = handle_event(&shared, &line) {
                    set_failure(&shared, error);
                    break;
                }
            }
            Ok(None) => break,
            Err(error) => {
                set_failure(&shared, error);
                break;
            }
        }
    }
    if let Ok(mut state) = shared.state.lock() {
        state.closed = true;
        shared.changed.notify_all();
    }
}

fn read_stderr(mut stderr: std::process::ChildStderr, shared: Arc<SharedProcess>) {
    let mut buffer = [0_u8; 1024];
    loop {
        match stderr.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                if let Ok(mut state) = shared.state.lock() {
                    state.append_stderr(&buffer[..read]);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
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
            if state.failure.is_none() {
                state.failure = Some(EncoderProcessError::new(code, message));
            }
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
        "done" | "stats" => {
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
    use super::{
        EncoderEvent, MAX_EVENT_LINE_BYTES, MAX_STDERR_BYTES, ProcessState, SegmentEvent,
        parse_event, read_bounded_line,
    };
    use std::collections::BTreeMap;
    use std::io::Cursor;

    #[test]
    fn preserves_first_codec_failure_after_later_errors_and_stats() {
        let encoder = super::EncoderProcess::new(super::EncoderConfig {
            out_dir: std::path::Path::new("/unused"),
            executable: std::path::Path::new("/unused/helper"),
            width: 3840,
            height: 1080,
            fps: 30,
            bitrate_kbps: 12000,
            segment_frames: 150,
            path_prefix: "video/",
        })
        .unwrap();
        super::handle_event(&encoder.shared,
            br#"{"event":"error","code":"frame_rejected","message":"decoder_input_dequeue_failed: ret=-42"}"#).unwrap();
        super::handle_event(
            &encoder.shared,
            br#"{"event":"error","code":"pipeline_failed","message":"drain failed"}"#,
        )
        .unwrap();
        super::handle_event(
            &encoder.shared,
            br#"{"event":"done","submitted":8,"decoded":7}"#,
        )
        .unwrap();
        let failure = encoder.failure().unwrap();
        assert_eq!(failure.code, "frame_rejected");
        assert_eq!(failure.message, "decoder_input_dequeue_failed: ret=-42");
        assert_eq!(encoder.stats()["decoded"], 7);
        assert_eq!(encoder.raise_if_failed(), Err(failure));
    }

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

    #[test]
    fn caps_stdout_event_lines_at_64_kib() {
        let mut cursor = Cursor::new(vec![b'x'; MAX_EVENT_LINE_BYTES + 10]);
        let error = read_bounded_line(&mut cursor, MAX_EVENT_LINE_BYTES).unwrap_err();
        assert_eq!(error.code, "encoder_failed");
        assert!(error.message.contains("超出长度限制"));

        let mut ok_payload = vec![b'a'; MAX_EVENT_LINE_BYTES];
        ok_payload.push(b'\n');
        let mut ok_cursor = Cursor::new(ok_payload);
        let line = read_bounded_line(&mut ok_cursor, MAX_EVENT_LINE_BYTES)
            .unwrap()
            .unwrap();
        assert_eq!(line.len(), MAX_EVENT_LINE_BYTES);
    }

    #[test]
    fn stderr_ring_buffer_keeps_only_bounded_tail() {
        let mut state = ProcessState::new();
        let prefix = vec![b'A'; MAX_STDERR_BYTES];
        state.append_stderr(&prefix);
        state.append_stderr(b"tail-diagnostic");
        let snippet = state.stderr_snippet().unwrap();
        assert_eq!(snippet.len(), MAX_STDERR_BYTES);
        assert!(snippet.ends_with("tail-diagnostic"));
    }

    #[test]
    fn finish_timeout_escalates_to_sigkill_and_reaps_child() {
        use std::os::unix::fs::PermissionsExt;
        use std::time::{SystemTime, UNIX_EPOCH};

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "rp-ylx-encoder-kill-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let pid_file = root.join("child.pid");
        let helper = root.join("stall-helper.sh");
        std::fs::write(
            &helper,
            format!(
                "#!/bin/sh\ntrap '' TERM\necho $$ > \"{}\"\nprintf '{{\"event\":\"ready\"}}\\n'\nexec sleep 30\n",
                pid_file.display()
            ),
        )
        .unwrap();
        let mut perms = std::fs::metadata(&helper).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&helper, perms).unwrap();

        let mut encoder = super::EncoderProcess::new(super::EncoderConfig {
            out_dir: &root,
            executable: &helper,
            width: 3840,
            height: 1080,
            fps: 30,
            bitrate_kbps: 8192,
            segment_frames: 30,
            path_prefix: "video/",
        })
        .unwrap();
        encoder.start().unwrap();
        let pid: i32 = std::fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let started = std::time::Instant::now();
        let err = encoder
            .finish(std::time::Duration::from_millis(50))
            .unwrap_err();
        assert_eq!(err.code, "encoder_failed");
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
        assert!(encoder.child.is_none());
        // Child was SIGKILLed and reaped (kill(pid, 0) fails with ESRCH).
        let alive = unsafe { libc::kill(pid, 0) };
        assert_eq!(alive, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
