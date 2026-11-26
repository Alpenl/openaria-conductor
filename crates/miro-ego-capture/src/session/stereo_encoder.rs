use crate::session::session_io;
use crate::session::shm_ring::{ShmRingConfig, ShmRingError, ShmRingProducer};
use miro_ego_core::ids::is_valid_sha256;
use miro_ego_core::sys::clock::monotonic_ns;
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::io::{BufRead, BufReader, Read};
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const READY_TIMEOUT: Duration = Duration::from_secs(15);
const ABORT_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const SHM_SUBMIT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_EVENT_LINE_BYTES: usize = 64 * 1024;
const MAX_STDERR_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncoderEventError {
    pub code: &'static str,
    pub message: String,
}

impl EncoderEventError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Completed stereo MP4 segment reported by `miro-ego-encoder` on `stdout`.
///
/// Includes optional streaming `left_sha256` / `right_sha256` digests computed while
/// pages are hot in cache so session sealing skips rereading the MP4 segments.
#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncoderEvent {
    Ready,
    Segment(SegmentEvent),
    Done(BTreeMap<String, i64>),
    Error { code: String, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncoderProcessError {
    pub code: String,
    pub message: String,
}

impl EncoderProcessError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
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

impl From<ShmRingError> for EncoderProcessError {
    fn from(error: ShmRingError) -> Self {
        Self::new(error.code, error.message)
    }
}

/// Frame transport between Rust capture runtime and `miro-ego-encoder`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncoderTransport {
    /// Default production zero-copy `memfd` + `eventfd` SHM ring (`--shm-fd --kick-fd --done-fd`).
    ShmRing(ShmRingConfig),
    /// Debug / test fallback piping framed MJPEG records over stdin (`--stdin`).
    StdinPipe,
}

impl Default for EncoderTransport {
    fn default() -> Self {
        Self::ShmRing(ShmRingConfig::default())
    }
}

enum ActiveTransport {
    ShmRing(ShmRingProducer),
    StdinPipe(ChildStdin),
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

pub struct EncoderProcess {
    executable: PathBuf,
    args: Vec<String>,
    transport_mode: EncoderTransport,
    shared: Arc<SharedProcess>,
    child: Option<Child>,
    active_transport: Option<ActiveTransport>,
    reader: Option<JoinHandle<()>>,
    stderr_reader: Option<JoinHandle<()>>,
    submitted: u64,
}

pub struct EncoderConfig<'a> {
    pub out_dir: &'a Path,
    pub executable: &'a Path,
    pub width: u64,
    pub height: u64,
    pub fps: u64,
    pub bitrate_kbps: u64,
    pub segment_frames: u64,
    pub path_prefix: &'a str,
}

impl EncoderProcess {
    pub fn new(config: EncoderConfig<'_>) -> Result<Self, EncoderProcessError> {
        Self::with_transport(config, EncoderTransport::default())
    }

    pub fn with_transport(
        config: EncoderConfig<'_>,
        transport_mode: EncoderTransport,
    ) -> Result<Self, EncoderProcessError> {
        if config.width == 0
            || config.width % 4 != 0
            || config.height == 0
            || config.fps == 0
            || config.bitrate_kbps == 0
            || config.segment_frames == 0
        {
            return Err(EncoderProcessError::new(
                "invalid_argument",
                "invalid encoder parameters",
            ));
        }
        if let EncoderTransport::ShmRing(shm_cfg) = transport_mode {
            shm_cfg.validate()?;
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
            transport_mode,
            shared: Arc::new(SharedProcess {
                state: Mutex::new(ProcessState::new()),
                changed: Condvar::new(),
            }),
            child: None,
            active_transport: None,
            reader: None,
            stderr_reader: None,
            submitted: 0,
        })
    }

    pub fn transport_mode(&self) -> EncoderTransport {
        self.transport_mode
    }

    pub fn start(&mut self) -> Result<(), EncoderProcessError> {
        if self.child.is_some() {
            return Err(EncoderProcessError::new(
                "invalid_state",
                "encoder child can only be started once",
            ));
        }
        {
            let mut state = self.shared.state.lock().map_err(|_| {
                EncoderProcessError::new("encoder_failed", "encoder process mutex is poisoned")
            })?;
            *state = ProcessState::new();
        }

        let mut command = Command::new(&self.executable);
        command.args(&self.args);
        command.stdout(Stdio::piped()).stderr(Stdio::piped());

        let shm_producer = match self.transport_mode {
            EncoderTransport::ShmRing(shm_config) => {
                let producer = ShmRingProducer::create(shm_config)?;
                let shm_fd = producer.shm_raw_fd();
                let kick_fd = producer.kick_raw_fd();
                let done_fd = producer.done_raw_fd();
                command.args([
                    "--shm-fd".to_owned(),
                    shm_fd.to_string(),
                    "--kick-fd".to_owned(),
                    kick_fd.to_string(),
                    "--done-fd".to_owned(),
                    done_fd.to_string(),
                ]);
                command.stdin(Stdio::null());
                // SAFETY: `pre_exec` runs only async-signal-safe `libc::fcntl(fd, F_SETFD, 0)`
                // on the three descriptors explicitly passed to the child process so the parent
                // retains `FD_CLOEXEC` for every other fork/exec.
                unsafe {
                    command.pre_exec(move || {
                        for fd in [shm_fd, kick_fd, done_fd] {
                            if libc::fcntl(fd, libc::F_SETFD, 0) != 0 {
                                return Err(std::io::Error::last_os_error());
                            }
                        }
                        Ok(())
                    });
                }
                Some(producer)
            }
            EncoderTransport::StdinPipe => {
                command.stdin(Stdio::piped());
                None
            }
        };

        let mut child = command.spawn().map_err(|error| {
            EncoderProcessError::new(
                "encoder_unavailable",
                format!("failed to spawn encoder: {error}"),
            )
        })?;
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let stdin = child.stdin.take();
        self.child = Some(child);

        self.active_transport = match shm_producer {
            Some(producer) => Some(ActiveTransport::ShmRing(producer)),
            None => match stdin {
                Some(pipe) => Some(ActiveTransport::StdinPipe(pipe)),
                None => {
                    self.abort();
                    return Err(EncoderProcessError::new(
                        "encoder_failed",
                        "unable to open encoder stdin",
                    ));
                }
            },
        };

        let Some(stdout) = stdout else {
            self.abort();
            return Err(EncoderProcessError::new(
                "encoder_failed",
                "unable to open encoder stdout event stream",
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
                        format!("failed to start encoder event thread: {error}"),
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
                            format!("failed to start encoder stderr thread: {error}"),
                        ));
                    }
                },
            );
        }
        self.wait_ready()
    }

    pub fn configure(&mut self, args: &[String]) -> Result<(), EncoderProcessError> {
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

    pub fn submit(&mut self, jpeg: &[u8]) -> Result<u64, EncoderProcessError> {
        let now_ns = monotonic_ns().unwrap_or(0);
        self.submit_frame(self.submitted, now_ns, jpeg)
    }

    pub fn submit_frame(
        &mut self,
        frame_seq: u64,
        host_monotonic_ns: u64,
        jpeg: &[u8],
    ) -> Result<u64, EncoderProcessError> {
        self.raise_if_failed()?;
        let transport = self.active_transport.as_mut().ok_or_else(|| {
            EncoderProcessError::new("invalid_state", "encoder process not started")
        })?;
        let written = match transport {
            ActiveTransport::ShmRing(ring) => {
                match ring.push_wait(frame_seq, host_monotonic_ns, 0, jpeg, SHM_SUBMIT_TIMEOUT) {
                    Ok(_) => u64::try_from(jpeg.len()).unwrap_or(u64::MAX),
                    Err(error) => {
                        self.raise_if_failed()?;
                        return Err(error.into());
                    }
                }
            }
            ActiveTransport::StdinPipe(stdin) => {
                match session_io::write_encoder_frame(stdin.as_raw_fd(), jpeg) {
                    Ok(written) => written,
                    Err(error) => {
                        // Prefer the helper's causal error over the subsequent EPIPE.
                        self.raise_if_failed()?;
                        return Err(error.into());
                    }
                }
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

    pub fn finish(&mut self, timeout: Duration) -> Result<Vec<SegmentEvent>, EncoderProcessError> {
        if timeout.is_zero() {
            return Err(EncoderProcessError::new(
                "invalid_argument",
                "finish timeout must be positive",
            ));
        }
        if self.child.is_none() {
            return Err(EncoderProcessError::new(
                "invalid_state",
                "encoder process not started",
            ));
        }
        // Signal EOF to the encoder: SHM sets FLAG_EOF + kicks eventfd; stdin closes pipe.
        match self.active_transport.take() {
            Some(ActiveTransport::ShmRing(ring)) => {
                let _ = ring.signal_eof();
                // Keep `ring` alive until child exits so mmap/fds remain valid while draining,
                // then drop after wait_child completes.
                self.active_transport = Some(ActiveTransport::ShmRing(ring));
            }
            Some(ActiveTransport::StdinPipe(stdin)) => {
                drop(stdin);
            }
            None => {}
        }
        let status = {
            let child = self.child.as_mut().ok_or_else(|| {
                EncoderProcessError::new("invalid_state", "encoder process not started")
            })?;
            wait_child(child, timeout).map_err(|error| {
                EncoderProcessError::new("encoder_failed", format!("wait encoder failed: {error}"))
            })?
        };
        let Some(status) = status else {
            self.abort();
            return Err(EncoderProcessError::new(
                "encoder_failed",
                self.with_stderr("encoder did not exit before timeout"),
            ));
        };
        let _ = self.active_transport.take();
        self.join_readers()?;
        self.raise_if_failed()?;
        if !status.success() {
            return Err(EncoderProcessError::new(
                "encoder_failed",
                self.with_stderr(format!("encoder exit code {}", status.code().unwrap_or(-1))),
            ));
        }
        Ok(self.segments())
    }

    pub fn abort(&mut self) {
        if let Some(ActiveTransport::ShmRing(ring)) = self.active_transport.as_ref() {
            let _ = ring.signal_eof();
        }
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
        let _ = self.active_transport.take();
        if child_reaped {
            let _ = self.join_readers();
        } else {
            // Child pipes may remain open until kernel unblocks; detach reader handles.
            self.reader.take();
            self.stderr_reader.take();
        }
    }

    pub fn segments(&self) -> Vec<SegmentEvent> {
        match self.shared.state.lock() {
            Ok(state) => state.segments.clone(),
            Err(_) => Vec::new(),
        }
    }

    pub fn segments_since(&self, offset: usize) -> Result<Vec<SegmentEvent>, EncoderProcessError> {
        let state = self.shared.state.lock().map_err(|_| {
            EncoderProcessError::new("encoder_failed", "encoder process mutex is poisoned")
        })?;
        let tail = state.segments.get(offset..).ok_or_else(|| {
            EncoderProcessError::new("segment_invalid", "encoder segment list regressed")
        })?;
        Ok(tail.to_vec())
    }

    pub fn stats(&self) -> BTreeMap<String, i64> {
        match self.shared.state.lock() {
            Ok(state) => state.stats.clone(),
            Err(_) => BTreeMap::new(),
        }
    }

    pub fn failure(&self) -> Option<EncoderProcessError> {
        self.shared.state.lock().ok()?.failure.clone()
    }

    pub fn submitted_frames(&self) -> u64 {
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
                    "encoder exited before ready"
                } else {
                    "encoder timed out waiting for ready"
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
                    "encoder event thread panicked",
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
                    format!("encoder event stream error: {error}"),
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
                    "encoder stdout line exceeds maximum length",
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
                "encoder stdout line exceeds maximum length",
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

pub fn parse_event(line: &[u8]) -> Result<Option<EncoderEvent>, EncoderEventError> {
    let text = std::str::from_utf8(line)
        .map_err(|_| EncoderEventError::new("encoder_failed", "encoder output is not UTF-8"))?
        .trim();
    if text.is_empty() {
        return Ok(None);
    }
    let value: Value = serde_json::from_str(text)
        .map_err(|_| EncoderEventError::new("encoder_failed", "encoder output is not JSON"))?;
    let object = value.as_object().ok_or_else(|| {
        EncoderEventError::new("encoder_failed", "encoder output JSON is not an object")
    })?;
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
                .unwrap_or("encoder reported failure")
                .to_owned(),
        })),
        _ => Ok(None),
    }
}

fn parse_segment(
    object: &serde_json::Map<String, Value>,
) -> Result<SegmentEvent, EncoderEventError> {
    let index = required_u64(object, "index")?;
    let start_frame = required_u64(object, "start_frame")?;
    let end_frame = required_u64(object, "end_frame")?;

    // Primary Miro-Ego v1 flat format:
    // {"event":"segment","index":0,"start_frame":0,"end_frame":90,
    //  "left_path":"...","left_bytes":...,"left_sha256":"<64-hex>",
    //  "right_path":"...","right_bytes":...,"right_sha256":"<64-hex>"}
    if object.contains_key("left_path") || object.contains_key("right_path") {
        let left_path = required_str(object, "left_path")?;
        let left_bytes = required_u64(object, "left_bytes")?;
        let left_sha256 = optional_sha256(object, "left_sha256")?;
        let right_path = required_str(object, "right_path")?;
        let right_bytes = required_u64(object, "right_bytes")?;
        let right_sha256 = optional_sha256(object, "right_sha256")?;
        return Ok(SegmentEvent {
            index,
            start_frame,
            end_frame,
            left_path,
            left_bytes,
            left_sha256,
            right_path,
            right_bytes,
            right_sha256,
        });
    }

    // Fallback nested artifact format:
    // {"left":{"path":"...","bytes":...,"sha256":"..."},"right":{...}}
    Ok(SegmentEvent {
        index,
        start_frame,
        end_frame,
        left_path: required_artifact_path(object, "left")?,
        left_bytes: required_artifact_bytes(object, "left")?,
        left_sha256: optional_artifact_sha256(object, "left")?,
        right_path: required_artifact_path(object, "right")?,
        right_bytes: required_artifact_bytes(object, "right")?,
        right_sha256: optional_artifact_sha256(object, "right")?,
    })
}

fn required_u64(
    object: &serde_json::Map<String, Value>,
    key: &'static str,
) -> Result<u64, EncoderEventError> {
    object
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| EncoderEventError::new("encoder_failed", format!("segment missing {key}")))
}

fn required_str(
    object: &serde_json::Map<String, Value>,
    key: &'static str,
) -> Result<String, EncoderEventError> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| EncoderEventError::new("encoder_failed", format!("segment missing {key}")))
}

fn optional_sha256(
    object: &serde_json::Map<String, Value>,
    key: &'static str,
) -> Result<Option<String>, EncoderEventError> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(hex)) if is_valid_sha256(hex) => Ok(Some(hex.clone())),
        Some(_) => Err(EncoderEventError::new(
            "encoder_failed",
            format!("segment {key} must be 64 lowercase hex characters"),
        )),
    }
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
            EncoderEventError::new("encoder_failed", format!("segment missing {key}.path"))
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
            EncoderEventError::new("encoder_failed", format!("segment missing {key}.bytes"))
        })
}

fn optional_artifact_sha256(
    object: &serde_json::Map<String, Value>,
    key: &'static str,
) -> Result<Option<String>, EncoderEventError> {
    let Some(artifact) = object.get(key).and_then(Value::as_object) else {
        return Ok(None);
    };
    optional_sha256(artifact, "sha256")
}

#[cfg(test)]
mod tests {
    use super::{
        EncoderConfig, EncoderEvent, EncoderProcess, EncoderTransport, MAX_EVENT_LINE_BYTES,
        MAX_STDERR_BYTES, ProcessState, SegmentEvent, parse_event, read_bounded_line,
    };
    use crate::session::shm_ring::ShmRingConfig;
    use std::collections::BTreeMap;
    use std::io::Cursor;

    #[test]
    fn preserves_first_codec_failure_after_later_errors_and_stats() {
        let encoder = EncoderProcess::new(EncoderConfig {
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
        super::handle_event(
            &encoder.shared,
            br#"{"event":"error","code":"frame_rejected","message":"decoder_input_dequeue_failed: ret=-42"}"#,
        )
        .unwrap();
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
    fn parses_flat_miro_ego_segment_events_with_streaming_sha256() {
        let left_sha = "a".repeat(64);
        let right_sha = "b".repeat(64);
        let line = format!(
            r#"{{"event":"segment","index":0,"start_frame":0,"end_frame":90,"left_path":"video/left_00000.mp4","left_bytes":1024,"left_sha256":"{left_sha}","right_path":"video/right_00000.mp4","right_bytes":2048,"right_sha256":"{right_sha}"}}"#
        );
        assert_eq!(
            parse_event(line.as_bytes()).unwrap(),
            Some(EncoderEvent::Segment(SegmentEvent {
                index: 0,
                start_frame: 0,
                end_frame: 90,
                left_path: "video/left_00000.mp4".to_owned(),
                left_bytes: 1024,
                left_sha256: Some(left_sha),
                right_path: "video/right_00000.mp4".to_owned(),
                right_bytes: 2048,
                right_sha256: Some(right_sha),
            }))
        );

        let bad_sha = br#"{"event":"segment","index":0,"start_frame":0,"end_frame":90,"left_path":"video/l.mp4","left_bytes":1,"left_sha256":"INVALID","right_path":"video/r.mp4","right_bytes":1}"#;
        assert_eq!(parse_event(bad_sha).unwrap_err().code, "encoder_failed");
    }

    #[test]
    fn parses_ready_nested_segment_done_and_error_events() {
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
                left_sha256: None,
                right_path: "video/right_00002.mp4".to_owned(),
                right_bytes: 456,
                right_sha256: None,
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
        assert!(error.message.contains("JSON"));
    }

    #[test]
    fn caps_stdout_event_lines_at_64_kib() {
        let mut cursor = Cursor::new(vec![b'x'; MAX_EVENT_LINE_BYTES + 10]);
        let error = read_bounded_line(&mut cursor, MAX_EVENT_LINE_BYTES).unwrap_err();
        assert_eq!(error.code, "encoder_failed");
        assert!(error.message.contains("maximum length"));

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
    fn shm_transport_inherits_memfd_and_eventfds_across_exec() {
        use std::os::unix::fs::PermissionsExt;
        use std::time::{SystemTime, UNIX_EPOCH};

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "miro-ego-encoder-shm-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let helper = root.join("shm-check-helper.sh");
        // Inspect the inherited --shm-fd, --kick-fd, --done-fd arguments and verify all three
        // descriptors exist in /proc/self/fd/ and that shm_fd starts with MEGO_SHM magic.
        std::fs::write(
            &helper,
            "#!/bin/sh\nset -e\nSHM=\"\"; KICK=\"\"; DONE=\"\"\nwhile [ \"$#\" -gt 0 ]; do\n  case \"$1\" in\n    --shm-fd) SHM=\"$2\"; shift 2 ;;\n    --kick-fd) KICK=\"$2\"; shift 2 ;;\n    --done-fd) DONE=\"$2\"; shift 2 ;;\n    *) shift ;;\n  esac\ndone\ntest -e \"/proc/self/fd/$SHM\"\ntest -e \"/proc/self/fd/$KICK\"\ntest -e \"/proc/self/fd/$DONE\"\nMAGIC=$(dd if=\"/proc/self/fd/$SHM\" bs=8 count=1 2>/dev/null)\ntest \"$MAGIC\" = \"MEGO_SHM\"\nprintf '{\"event\":\"ready\"}\\n'\nprintf '{\"event\":\"done\",\"frames\":0}\\n'\n",
        )
        .unwrap();
        let mut perms = std::fs::metadata(&helper).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&helper, perms).unwrap();

        let mut encoder = EncoderProcess::with_transport(
            EncoderConfig {
                out_dir: &root,
                executable: &helper,
                width: 3840,
                height: 1080,
                fps: 30,
                bitrate_kbps: 8192,
                segment_frames: 30,
                path_prefix: "video/",
            },
            EncoderTransport::ShmRing(ShmRingConfig {
                slot_count: 4,
                slot_capacity: 4096,
            }),
        )
        .unwrap();
        encoder.start().unwrap();
        let segments = encoder.finish(std::time::Duration::from_secs(2)).unwrap();
        assert!(segments.is_empty());
        assert_eq!(encoder.stats().get("frames"), Some(&0));
        std::fs::remove_dir_all(root).unwrap();
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
            "miro-ego-encoder-kill-{}-{unique}",
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

        let mut encoder = EncoderProcess::with_transport(
            EncoderConfig {
                out_dir: &root,
                executable: &helper,
                width: 3840,
                height: 1080,
                fps: 30,
                bitrate_kbps: 8192,
                segment_frames: 30,
                path_prefix: "video/",
            },
            EncoderTransport::StdinPipe,
        )
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
