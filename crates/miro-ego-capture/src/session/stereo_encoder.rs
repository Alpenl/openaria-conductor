use crate::session::session_io;
use crate::session::shm_ring::{
    ShmRingConfig, ShmRingError, ShmRingProducer, create_eventfd, min_slot_capacity_for_dimensions,
    wake_eventfd_best_effort,
};
use miro_ego_core::ids::is_valid_sha256;
use miro_ego_core::sys::clock::monotonic_ns;
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::io::{BufRead, BufReader, Read};
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const READY_TIMEOUT: Duration = Duration::from_secs(15);
const ABORT_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const SHM_SUBMIT_TIMEOUT: Duration = Duration::from_secs(10);
const STDERR_DRAIN_GRACE: Duration = Duration::from_millis(50);
const SPAWN_ETXTBSY_MAX_ATTEMPTS: usize = 8;
const SPAWN_ETXTBSY_BACKOFF: Duration = Duration::from_millis(10);
const MAX_EVENT_LINE_BYTES: usize = 64 * 1024;
const MAX_STDERR_BYTES: usize = 8 * 1024;
const MAX_ENCODER_DIMENSION: u64 = 16_384;
const MAX_ENCODER_FPS: u64 = 60;
const MAX_ENCODER_BITRATE_KBPS: u64 = 100_000;
const MAX_ENCODER_SEGMENT_FRAMES: u64 = 1_000_000;
const MAX_ENCODER_INTRA_PERIOD: u64 = 300;
const MAX_ENCODER_VBV_MS: u64 = 3_000;
const DEFAULT_MIN_QP: u8 = 20;
const DEFAULT_MAX_QP: u8 = 51;
const DEFAULT_INTRA_QP: u8 = 22;
const DEFAULT_INITIAL_QP: u8 = 24;

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

pub use miro_ego_core::media_types::SegmentEvent;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncoderEvent {
    Ready,
    Segment(SegmentEvent),
    Stats(BTreeMap<String, i64>),
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
    saw_done: bool,
    stderr_closed: bool,
    failure: Option<EncoderProcessError>,
    segments: Vec<SegmentEvent>,
    stats: BTreeMap<String, i64>,
    stderr_ring: VecDeque<u8>,
    wake_done_fd: Option<OwnedFd>,
}

impl ProcessState {
    fn new() -> Self {
        Self {
            ready: false,
            closed: false,
            saw_done: false,
            stderr_closed: false,
            failure: None,
            segments: Vec::new(),
            stats: BTreeMap::new(),
            stderr_ring: VecDeque::new(),
            wake_done_fd: None,
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

    fn wake_producer(&self) {
        if let Some(fd) = self.wake_done_fd.as_ref() {
            wake_eventfd_best_effort(fd.as_raw_fd());
        }
    }

    fn unexpected_eof_error(&self) -> EncoderProcessError {
        let base = if self.ready {
            "encoder stdout closed before done event"
        } else {
            "encoder exited before ready"
        };
        let message = match self.stderr_snippet() {
            Some(stderr) => format!("{base}; stderr={stderr}"),
            None => base.to_owned(),
        };
        EncoderProcessError::new("encoder_unexpected_eof", message)
    }

    fn effective_failure(&self) -> Option<EncoderProcessError> {
        if let Some(failure) = &self.failure {
            return Some(failure.clone());
        }
        if self.closed && !self.saw_done {
            return Some(self.unexpected_eof_error());
        }
        None
    }
}

struct SharedProcess {
    state: Mutex<ProcessState>,
    changed: Condvar,
    aborted: AtomicBool,
    live_pgid: Mutex<Option<i32>>,
}

impl SharedProcess {
    fn new() -> Self {
        Self {
            state: Mutex::new(ProcessState::new()),
            changed: Condvar::new(),
            aborted: AtomicBool::new(false),
            live_pgid: Mutex::new(None),
        }
    }

    fn raise_if_failed(&self) -> Result<(), EncoderProcessError> {
        let state = self.state.lock().map_err(|_| {
            EncoderProcessError::new("encoder_failed", "encoder process mutex is poisoned")
        })?;
        if let Some(error) = state.effective_failure() {
            return Err(error);
        }
        if self.aborted.load(Ordering::Acquire) {
            return Err(EncoderProcessError::new(
                "encoder_aborted",
                "encoder process was aborted",
            ));
        }
        Ok(())
    }

    fn wake_producer(&self) {
        if let Ok(state) = self.state.lock() {
            state.wake_producer();
        }
        self.changed.notify_all();
    }

    fn wake_raw_fd(&self) -> Option<libc::c_int> {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.wake_done_fd.as_ref().map(AsRawFd::as_raw_fd))
    }

    fn signal_abort(&self) {
        self.aborted.store(true, Ordering::Release);
        if let Ok(mut state) = self.state.lock() {
            if state.failure.is_none() {
                state.failure = Some(EncoderProcessError::new(
                    "encoder_aborted",
                    "encoder process was aborted",
                ));
            }
            state.wake_producer();
            self.changed.notify_all();
        }
        if let Ok(guard) = self.live_pgid.lock() {
            if let Some(pgid) = *guard {
                if pgid > 0 {
                    // SAFETY: `live_pgid` is cleared atomically under `live_pgid` lock when
                    // `try_wait` reaps the child, so `pgid` cannot refer to a recycled PID.
                    // The child is spawned with `process_group(0)` so `-pgid` kills the group.
                    unsafe {
                        let _ = libc::kill(-pgid, libc::SIGKILL);
                        let _ = libc::kill(pgid, libc::SIGKILL);
                    }
                }
            }
        }
    }
}

/// Lock-free handle capable of aborting or waking a blocked [`EncoderProcess`] without
/// waiting on an external `Mutex<EncoderProcess>` held by an in-flight `submit_frame`.
#[derive(Clone)]
pub struct EncoderAbortHandle {
    shared: Arc<SharedProcess>,
}

impl EncoderAbortHandle {
    pub fn abort(&self) {
        self.shared.signal_abort();
    }

    pub fn wake(&self) {
        self.shared.wake_producer();
    }

    pub fn is_aborted(&self) -> bool {
        self.shared.aborted.load(Ordering::Acquire)
    }
}

/// Minimum SHM slot capacity (bytes) sized for compressed stereo MJPEG payloads:
/// page-aligned `(width * height)` clamped to `2 MiB ..= 8 MiB`.
pub fn min_shm_slot_capacity(width: u64, height: u64) -> Result<usize, EncoderProcessError> {
    if width == 0 || height == 0 {
        return Err(EncoderProcessError::new(
            "invalid_argument",
            "encoder frame dimensions must be positive",
        ));
    }
    let sized = min_slot_capacity_for_dimensions(width, height).ok_or_else(|| {
        EncoderProcessError::new("invalid_argument", "encoder frame dimensions overflow")
    })?;
    usize::try_from(sized).map_err(|_| {
        EncoderProcessError::new(
            "invalid_argument",
            "encoder frame buffer size exceeds usize",
        )
    })
}

pub struct EncoderProcess {
    executable: PathBuf,
    base_args: Vec<String>,
    args: Vec<String>,
    fps: u64,
    segment_frames: u64,
    base_intra_period: Option<u64>,
    intra_period: Option<u64>,
    min_qp: Option<u8>,
    max_qp: Option<u8>,
    intra_qp: Option<u8>,
    initial_qp: Option<u8>,
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
        let width = u32::try_from(config.width).unwrap_or(u32::MAX);
        let height = u32::try_from(config.height).unwrap_or(u32::MAX);
        Self::with_transport(
            config,
            EncoderTransport::ShmRing(ShmRingConfig::for_dimensions(width, height)),
        )
    }

    pub fn with_transport(
        config: EncoderConfig<'_>,
        transport_mode: EncoderTransport,
    ) -> Result<Self, EncoderProcessError> {
        if config.width == 0
            || config.width % 4 != 0
            || config.width > MAX_ENCODER_DIMENSION
            || config.height == 0
            || config.height % 2 != 0
            || config.height > MAX_ENCODER_DIMENSION
            || config.fps == 0
            || config.fps > MAX_ENCODER_FPS
            || config.bitrate_kbps == 0
            || config.bitrate_kbps > MAX_ENCODER_BITRATE_KBPS
            || config.segment_frames == 0
            || config.segment_frames > MAX_ENCODER_SEGMENT_FRAMES
        {
            return Err(EncoderProcessError::new(
                "invalid_argument",
                "invalid encoder parameters",
            ));
        }
        let transport_mode = if transport_mode == EncoderTransport::default() {
            let width = u32::try_from(config.width).unwrap_or(u32::MAX);
            let height = u32::try_from(config.height).unwrap_or(u32::MAX);
            EncoderTransport::ShmRing(ShmRingConfig::for_dimensions(width, height))
        } else {
            transport_mode
        };
        if let EncoderTransport::ShmRing(shm_cfg) = transport_mode {
            shm_cfg.validate()?;
            let min_capacity = min_shm_slot_capacity(config.width, config.height)?;
            if (shm_cfg.slot_capacity as usize) < min_capacity {
                return Err(EncoderProcessError::new(
                    "invalid_shm_config",
                    format!(
                        "SHM ring slot_capacity ({}) is smaller than minimum bitstream buffer ({min_capacity}) for {}x{}",
                        shm_cfg.slot_capacity, config.width, config.height
                    ),
                ));
            }
        }
        let base_args = vec![
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
        ];
        Ok(Self {
            executable: config.executable.to_path_buf(),
            args: base_args.clone(),
            base_args,
            fps: config.fps,
            segment_frames: config.segment_frames,
            base_intra_period: None,
            intra_period: None,
            min_qp: None,
            max_qp: None,
            intra_qp: None,
            initial_qp: None,
            transport_mode,
            shared: Arc::new(SharedProcess::new()),
            child: None,
            active_transport: None,
            reader: None,
            stderr_reader: None,
            submitted: 0,
        })
    }

    pub fn with_transport_and_intra_period(
        config: EncoderConfig<'_>,
        intra_period: u64,
        transport_mode: EncoderTransport,
    ) -> Result<Self, EncoderProcessError> {
        if intra_period > MAX_ENCODER_INTRA_PERIOD {
            return Err(EncoderProcessError::new(
                "invalid_argument",
                "intra_period must be in 0..=300",
            ));
        }
        let mut proc = Self::with_transport(config, transport_mode)?;
        proc.base_intra_period = Some(intra_period);
        proc.intra_period = Some(intra_period);
        proc.validate_segment_gop()?;
        proc.base_args.push("--intra-period".to_owned());
        proc.base_args.push(intra_period.to_string());
        proc.args = proc.base_args.clone();
        Ok(proc)
    }

    fn validate_qp_bounds(&self) -> Result<(), EncoderProcessError> {
        validate_effective_qp_bounds(self.min_qp, self.max_qp, self.intra_qp, self.initial_qp)
    }

    fn validate_segment_gop(&self) -> Result<(), EncoderProcessError> {
        let effective_gop = self.intra_period.filter(|&p| p > 0).unwrap_or(self.fps);
        if self.segment_frames > 0
            && (effective_gop == 0 || self.segment_frames % effective_gop != 0)
        {
            return Err(EncoderProcessError::new(
                "invalid_argument",
                format!(
                    "segment_frames ({}) must be a multiple of effective intra_period ({effective_gop})",
                    self.segment_frames
                ),
            ));
        }
        Ok(())
    }

    pub fn transport_mode(&self) -> EncoderTransport {
        self.transport_mode
    }

    pub fn abort_handle(&self) -> EncoderAbortHandle {
        EncoderAbortHandle {
            shared: Arc::clone(&self.shared),
        }
    }

    pub fn start(&mut self) -> Result<(), EncoderProcessError> {
        if self.child.is_some() {
            return Err(EncoderProcessError::new(
                "invalid_state",
                "encoder child can only be started once",
            ));
        }
        self.validate_segment_gop()?;
        self.validate_qp_bounds()?;
        self.submitted = 0;
        self.shared.aborted.store(false, Ordering::Release);
        if let Ok(mut pgid) = self.shared.live_pgid.lock() {
            *pgid = None;
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
        command.process_group(0);

        let (shm_producer, wake_done_fd) = match self.transport_mode {
            EncoderTransport::ShmRing(shm_config) => {
                let producer = ShmRingProducer::create(shm_config)?;
                let wake_fd = producer.clone_done_wakeup_fd()?;
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
                (Some(producer), Some(wake_fd))
            }
            EncoderTransport::StdinPipe => {
                let wake_fd = create_eventfd()?;
                command.stdin(Stdio::piped());
                (None, Some(wake_fd))
            }
        };

        let mut child = spawn_with_etxtbsy_retry(&mut command).map_err(|error| {
            EncoderProcessError::new(
                "encoder_unavailable",
                format!("failed to spawn encoder: {error}"),
            )
        })?;
        if let Ok(pid) = i32::try_from(child.id()) {
            if pid > 0 {
                if let Ok(mut guard) = self.shared.live_pgid.lock() {
                    *guard = Some(pid);
                }
            }
        }
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let stdin = child.stdin.take();
        self.child = Some(child);

        {
            let mut state = self.shared.state.lock().map_err(|_| {
                EncoderProcessError::new("encoder_failed", "encoder process mutex is poisoned")
            })?;
            state.wake_done_fd = wake_done_fd;
            if stderr.is_none() {
                state.stderr_closed = true;
            }
        }

        self.active_transport = match shm_producer {
            Some(producer) => Some(ActiveTransport::ShmRing(producer)),
            None => match stdin {
                Some(pipe) => {
                    if let Err(error) = set_fd_nonblocking(pipe.as_raw_fd()) {
                        self.abort();
                        return Err(EncoderProcessError::new(
                            "encoder_failed",
                            format!("failed to configure non-blocking encoder stdin: {error}"),
                        ));
                    }
                    Some(ActiveTransport::StdinPipe(pipe))
                }
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
        // Start from constructor defaults so reconfiguring never retains stale overrides or
        // duplicate CLI flags from an earlier `configure()` call.
        let mut next_intra_period = self.base_intra_period;
        let mut saw_intra_period = false;
        let mut next_min_qp: Option<u8> = None;
        let mut next_max_qp: Option<u8> = None;
        let mut next_intra_qp: Option<u8> = None;
        let mut next_initial_qp: Option<u8> = None;
        let mut overrides: Vec<(String, String)> = Vec::with_capacity(args.len() / 2);

        for pair in args.chunks_exact(2) {
            let flag = pair[0].as_str();
            let value = pair[1].as_str();
            if value.starts_with('-') {
                return Err(EncoderProcessError::new(
                    "invalid_argument",
                    "unsupported encoder setting",
                ));
            }
            match flag {
                "--codec" => {
                    if !matches!(value, "h264" | "hevc") {
                        return Err(EncoderProcessError::new(
                            "invalid_argument",
                            "unsupported codec value",
                        ));
                    }
                }
                "--rc-mode" => {
                    if !matches!(value, "cbr" | "vbr" | "fixqp" | "avbr") {
                        return Err(EncoderProcessError::new(
                            "invalid_argument",
                            "unsupported rc-mode value",
                        ));
                    }
                }
                "--bitrate-kbps" => {
                    let kbps = value.parse::<u64>().map_err(|_| {
                        EncoderProcessError::new("invalid_argument", "invalid bitrate-kbps")
                    })?;
                    if !(1..=MAX_ENCODER_BITRATE_KBPS).contains(&kbps) {
                        return Err(EncoderProcessError::new(
                            "invalid_argument",
                            "bitrate-kbps out of range",
                        ));
                    }
                }
                "--min-qp" | "--max-qp" | "--intra-qp" | "--initial-qp" => {
                    let qp = value.parse::<u8>().map_err(|_| {
                        EncoderProcessError::new("invalid_argument", "invalid qp setting")
                    })?;
                    if qp > 51 {
                        return Err(EncoderProcessError::new(
                            "invalid_argument",
                            "qp setting must be in 0..=51",
                        ));
                    }
                    match flag {
                        "--min-qp" => next_min_qp = Some(qp),
                        "--max-qp" => next_max_qp = Some(qp),
                        "--intra-qp" => next_intra_qp = Some(qp),
                        "--initial-qp" => next_initial_qp = Some(qp),
                        _ => unreachable!(),
                    }
                }
                "--intra-period" => {
                    let period = value.parse::<u64>().map_err(|_| {
                        EncoderProcessError::new("invalid_argument", "invalid intra-period")
                    })?;
                    if period > MAX_ENCODER_INTRA_PERIOD {
                        return Err(EncoderProcessError::new(
                            "invalid_argument",
                            "intra-period must be in 0..=300",
                        ));
                    }
                    next_intra_period = Some(period);
                    saw_intra_period = true;
                }
                "--vbv-ms" => {
                    let vbv = value.parse::<u64>().map_err(|_| {
                        EncoderProcessError::new("invalid_argument", "invalid vbv-ms")
                    })?;
                    if !(1..=MAX_ENCODER_VBV_MS).contains(&vbv) {
                        return Err(EncoderProcessError::new(
                            "invalid_argument",
                            "vbv-ms must be in 1..=3000",
                        ));
                    }
                }
                _ => {
                    return Err(EncoderProcessError::new(
                        "invalid_argument",
                        "unsupported encoder setting",
                    ));
                }
            }
            if let Some(existing) = overrides.iter_mut().find(|(k, _)| k == flag) {
                existing.1 = value.to_owned();
            } else {
                overrides.push((flag.to_owned(), value.to_owned()));
            }
        }

        validate_effective_qp_bounds(next_min_qp, next_max_qp, next_intra_qp, next_initial_qp)?;

        if saw_intra_period || next_intra_period.is_some() {
            let effective_gop = next_intra_period.filter(|&p| p > 0).unwrap_or(self.fps);
            if self.segment_frames > 0
                && (effective_gop == 0 || self.segment_frames % effective_gop != 0)
            {
                return Err(EncoderProcessError::new(
                    "invalid_argument",
                    format!(
                        "segment_frames ({}) must be a multiple of effective intra_period ({effective_gop})",
                        self.segment_frames
                    ),
                ));
            }
        }

        let mut merged_args = Vec::with_capacity(self.base_args.len() + overrides.len() * 2);
        for base_pair in self.base_args.chunks_exact(2) {
            if !overrides.iter().any(|(flag, _)| flag == &base_pair[0]) {
                merged_args.push(base_pair[0].clone());
                merged_args.push(base_pair[1].clone());
            }
        }
        for (flag, value) in overrides {
            merged_args.push(flag);
            merged_args.push(value);
        }

        self.intra_period = next_intra_period;
        self.min_qp = next_min_qp;
        self.max_qp = next_max_qp;
        self.intra_qp = next_intra_qp;
        self.initial_qp = next_initial_qp;
        self.args = merged_args;
        self.submitted = 0;
        self.shared.aborted.store(false, Ordering::Release);
        if let Ok(mut pgid) = self.shared.live_pgid.lock() {
            *pgid = None;
        }
        if let Ok(mut state) = self.shared.state.lock() {
            *state = ProcessState::new();
        }
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
        self.submit_frame_with_stop_check(frame_seq, host_monotonic_ns, jpeg, || false)
    }

    pub fn submit_frame_with_stop_check<F>(
        &mut self,
        frame_seq: u64,
        host_monotonic_ns: u64,
        jpeg: &[u8],
        mut stop_check: F,
    ) -> Result<u64, EncoderProcessError>
    where
        F: FnMut() -> bool,
    {
        if jpeg.is_empty() {
            return Err(EncoderProcessError::new(
                "invalid_argument",
                "encoder frame payload must not be empty",
            ));
        }
        self.raise_if_failed()?;
        let shared = &self.shared;
        let wake_fd = shared.wake_raw_fd();
        let transport = self.active_transport.as_mut().ok_or_else(|| {
            EncoderProcessError::new("invalid_state", "encoder process not started")
        })?;
        let written = match transport {
            ActiveTransport::ShmRing(ring) => {
                match ring.push_wait_with_backpressure_check(
                    frame_seq,
                    host_monotonic_ns,
                    0,
                    jpeg,
                    SHM_SUBMIT_TIMEOUT,
                    |is_full| {
                        shared.raise_if_failed()?;
                        if is_full && stop_check() {
                            return Err(EncoderProcessError::new(
                                "capture_stopped",
                                "capture stopped while encoder input was backpressured",
                            ));
                        }
                        Ok(())
                    },
                ) {
                    Ok(_) => u64::try_from(jpeg.len()).unwrap_or(u64::MAX),
                    Err(error) => {
                        shared.raise_if_failed()?;
                        return Err(error);
                    }
                }
            }
            ActiveTransport::StdinPipe(stdin) => {
                let mut check_error: Option<EncoderProcessError> = None;
                match session_io::write_encoder_frame_nonblocking(
                    stdin.as_raw_fd(),
                    wake_fd,
                    jpeg,
                    SHM_SUBMIT_TIMEOUT,
                    |mid_frame, would_block| {
                        if let Err(causal) = shared.raise_if_failed() {
                            check_error = Some(causal);
                            return Err(session_io::SessionIoError {
                                code: "write_failed",
                                message: "encoder process failed".to_owned(),
                            });
                        }
                        if would_block && !mid_frame && stop_check() {
                            check_error = Some(EncoderProcessError::new(
                                "capture_stopped",
                                "capture stopped while encoder stdin pipe was backpressured",
                            ));
                            return Err(session_io::SessionIoError {
                                code: "write_failed",
                                message: "capture stopped".to_owned(),
                            });
                        }
                        Ok(())
                    },
                ) {
                    Ok(written) => written,
                    Err(error) => {
                        if let Some(captured) = check_error {
                            return Err(captured);
                        }
                        // Prefer the helper's causal error over the subsequent EPIPE.
                        shared.raise_if_failed()?;
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
            wait_child_and_clear_pgid(child, &self.shared.live_pgid, timeout).map_err(|error| {
                EncoderProcessError::new("encoder_failed", format!("wait encoder failed: {error}"))
            })?
        };
        let Some(status) = status else {
            let pre_abort_failure = self.failure();
            self.abort();
            if let Some(failure) = pre_abort_failure {
                return Err(failure);
            }
            if let Some(failure) = self.failure().filter(|err| err.code != "encoder_aborted") {
                return Err(failure);
            }
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
        self.abort_with_timeout(ABORT_WAIT_TIMEOUT);
    }

    fn abort_with_timeout(&mut self, wait_timeout: Duration) {
        if let Some(ActiveTransport::ShmRing(ring)) = self.active_transport.as_ref() {
            let _ = ring.signal_eof();
        }
        self.shared.signal_abort();
        let mut child_reaped = true;
        if let Some(mut child) = self.child.take() {
            match wait_child_and_clear_pgid(&mut child, &self.shared.live_pgid, wait_timeout) {
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => {
                    // Uninterruptible kernel D-state child cannot be waited forever on close;
                    // hand `child` to a detached background reaper so zombie PIDs never leak.
                    child_reaped = false;
                    spawn_detached_child_reaper(child, Arc::clone(&self.shared));
                }
            }
        }
        let _ = self.active_transport.take();
        if let Ok(mut state) = self.shared.state.lock() {
            state.wake_done_fd = None;
        }
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
        self.shared.state.lock().ok()?.effective_failure()
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
        self.shared.raise_if_failed()
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
        if let Ok(mut state) = self.shared.state.lock() {
            state.wake_done_fd = None;
        }
        Ok(())
    }
}

impl Drop for EncoderProcess {
    fn drop(&mut self) {
        self.abort();
    }
}

fn validate_effective_qp_bounds(
    min_qp: Option<u8>,
    max_qp: Option<u8>,
    intra_qp: Option<u8>,
    initial_qp: Option<u8>,
) -> Result<(), EncoderProcessError> {
    let eff_min = min_qp.unwrap_or(DEFAULT_MIN_QP);
    let eff_max = max_qp.unwrap_or(DEFAULT_MAX_QP);
    let eff_intra = intra_qp.unwrap_or(DEFAULT_INTRA_QP);
    let eff_initial = initial_qp.unwrap_or(DEFAULT_INITIAL_QP);
    if eff_min > eff_max {
        return Err(EncoderProcessError::new(
            "invalid_argument",
            "min-qp must not exceed max-qp",
        ));
    }
    if !(eff_min..=eff_max).contains(&eff_intra) {
        return Err(EncoderProcessError::new(
            "invalid_argument",
            format!(
                "intra-qp ({eff_intra}) must be within min-qp..=max-qp ({eff_min}..={eff_max})"
            ),
        ));
    }
    if !(eff_min..=eff_max).contains(&eff_initial) {
        return Err(EncoderProcessError::new(
            "invalid_argument",
            format!(
                "initial-qp ({eff_initial}) must be within min-qp..=max-qp ({eff_min}..={eff_max})"
            ),
        ));
    }
    Ok(())
}

fn spawn_detached_child_reaper(mut child: Child, shared: Arc<SharedProcess>) {
    let _ = thread::Builder::new()
        .name("encoder-reaper".to_owned())
        .spawn(move || {
            let _ = child.wait();
            if let Ok(mut pgid_guard) = shared.live_pgid.lock() {
                *pgid_guard = None;
            }
        });
}

fn read_bounded_line<R: BufRead>(
    reader: &mut R,
    limit: usize,
) -> Result<Option<Vec<u8>>, EncoderProcessError> {
    let max_read = (limit as u64).saturating_add(1);
    let mut line = Vec::new();
    loop {
        let remaining = max_read.saturating_sub(line.len() as u64);
        match reader.by_ref().take(remaining).read_until(b'\n', &mut line) {
            Ok(0) => {
                if line.is_empty() {
                    return Ok(None);
                }
                if line.len() > limit {
                    return Err(EncoderProcessError::new(
                        "encoder_failed",
                        "encoder stdout line exceeds maximum length",
                    ));
                }
                return Ok(Some(line));
            }
            Ok(_) => {
                if line.last() == Some(&b'\n') {
                    line.pop();
                    if line.last() == Some(&b'\r') {
                        line.pop();
                    }
                    if line.len() > limit {
                        return Err(EncoderProcessError::new(
                            "encoder_failed",
                            "encoder stdout line exceeds maximum length",
                        ));
                    }
                    return Ok(Some(line));
                }
                if line.len() > limit {
                    return Err(EncoderProcessError::new(
                        "encoder_failed",
                        "encoder stdout line exceeds maximum length",
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => {
                return Err(EncoderProcessError::new(
                    "encoder_failed",
                    format!("encoder event stream error: {error}"),
                ));
            }
        }
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
        if state.ready && !state.saw_done && state.failure.is_none() {
            let deadline = Instant::now() + STDERR_DRAIN_GRACE;
            while !state.stderr_closed {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let Ok((next_state, wait_res)) = shared.changed.wait_timeout(state, remaining)
                else {
                    return;
                };
                state = next_state;
                if wait_res.timed_out() {
                    break;
                }
            }
            if state.failure.is_none() {
                state.failure = Some(state.unexpected_eof_error());
            }
        }
        state.closed = true;
        state.wake_producer();
        shared.changed.notify_all();
    }
}

fn read_stderr(stderr: std::process::ChildStderr, shared: Arc<SharedProcess>) {
    let mut reader = BufReader::with_capacity(MAX_STDERR_BYTES, stderr);
    let mut chunk = Vec::with_capacity(1024);
    loop {
        chunk.clear();
        match reader
            .by_ref()
            .take(MAX_EVENT_LINE_BYTES as u64)
            .read_until(b'\n', &mut chunk)
        {
            Ok(0) => break,
            Ok(_) => {
                if let Ok(mut state) = shared.state.lock() {
                    state.append_stderr(&chunk);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }
    if let Ok(mut state) = shared.state.lock() {
        state.stderr_closed = true;
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
        EncoderEvent::Stats(stats) => {
            state.stats = stats;
        }
        EncoderEvent::Done(stats) => {
            state.stats = stats;
            state.saw_done = true;
        }
        EncoderEvent::Error { code, message } => {
            if state.failure.is_none() {
                state.failure = Some(EncoderProcessError::new(code, message));
            }
            state.wake_producer();
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
        state.wake_producer();
        shared.changed.notify_all();
    }
}

fn spawn_with_etxtbsy_retry(command: &mut Command) -> std::io::Result<Child> {
    retry_on_etxtbsy(SPAWN_ETXTBSY_MAX_ATTEMPTS, SPAWN_ETXTBSY_BACKOFF, || {
        command.spawn()
    })
}

fn retry_on_etxtbsy<T, F>(max_attempts: usize, backoff: Duration, mut op: F) -> std::io::Result<T>
where
    F: FnMut() -> std::io::Result<T>,
{
    let max_retries = max_attempts.saturating_sub(1);
    let mut retries = 0;
    loop {
        match op() {
            Ok(value) => return Ok(value),
            Err(error) if error.raw_os_error() == Some(libc::ETXTBSY) && retries < max_retries => {
                retries += 1;
                thread::sleep(backoff);
            }
            Err(error) => return Err(error),
        }
    }
}

fn set_fd_nonblocking(fd: libc::c_int) -> std::io::Result<()> {
    // SAFETY: `fd` is an open owned pipe write descriptor.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let rc = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
    if rc < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn wait_child_and_clear_pgid(
    child: &mut Child,
    live_pgid: &Mutex<Option<i32>>,
    timeout: Duration,
) -> std::io::Result<Option<std::process::ExitStatus>> {
    let deadline = Instant::now() + timeout;
    loop {
        {
            let mut pgid_guard = live_pgid.lock().ok();
            if let Some(status) = child.try_wait()? {
                if let Some(guard) = pgid_guard.as_mut() {
                    **guard = None;
                }
                return Ok(Some(status));
            }
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
        "stats" => {
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
            Ok(Some(EncoderEvent::Stats(stats)))
        }
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
            parse_event(br#"{"event":"stats","frames":3,"ignored":"x"}"#).unwrap(),
            Some(EncoderEvent::Stats(BTreeMap::from([(
                "frames".to_owned(),
                3
            )])))
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
                width: 64,
                height: 32,
                fps: 30,
                bitrate_kbps: 8192,
                segment_frames: 30,
                path_prefix: "video/",
            },
            EncoderTransport::ShmRing(ShmRingConfig {
                slot_count: 4,
                slot_capacity: 2 * 1024 * 1024,
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

    #[test]
    fn retry_on_etxtbsy_handles_transient_permanent_and_other_errors() {
        let mut attempts = 0;
        let value = super::retry_on_etxtbsy(4, std::time::Duration::ZERO, || {
            attempts += 1;
            if attempts < 3 {
                Err(std::io::Error::from_raw_os_error(libc::ETXTBSY))
            } else {
                Ok("spawned")
            }
        })
        .unwrap();
        assert_eq!(value, "spawned");
        assert_eq!(attempts, 3);

        let mut enoent_attempts = 0;
        let err =
            super::retry_on_etxtbsy(4, std::time::Duration::ZERO, || -> std::io::Result<()> {
                enoent_attempts += 1;
                Err(std::io::Error::from_raw_os_error(libc::ENOENT))
            })
            .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
        assert_eq!(enoent_attempts, 1);

        let mut exhausted_attempts = 0;
        let err =
            super::retry_on_etxtbsy(3, std::time::Duration::ZERO, || -> std::io::Result<()> {
                exhausted_attempts += 1;
                Err(std::io::Error::from_raw_os_error(libc::ETXTBSY))
            })
            .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ETXTBSY));
        assert_eq!(exhausted_attempts, 3);
    }

    #[test]
    fn validates_dimensions_fps_segment_bounds_gop_alignment_and_empty_frames() {
        let valid = || EncoderConfig {
            out_dir: std::path::Path::new("/unused"),
            executable: std::path::Path::new("/unused/helper"),
            width: 3840,
            height: 1080,
            fps: 30,
            bitrate_kbps: 12_000,
            segment_frames: 150,
            path_prefix: "video/",
        };

        // Odd height rejected
        let mut odd_h = valid();
        odd_h.height = 1081;
        assert!(EncoderProcess::new(odd_h).is_err());

        // Dimensions > 16384 rejected
        let mut big_w = valid();
        big_w.width = 16_388;
        assert!(EncoderProcess::new(big_w).is_err());

        // FPS > 60 rejected
        let mut big_fps = valid();
        big_fps.fps = 90;
        assert!(EncoderProcess::new(big_fps).is_err());

        // segment_frames > 1_000_000 rejected
        let mut big_seg = valid();
        big_seg.segment_frames = 1_000_001;
        assert!(EncoderProcess::new(big_seg).is_err());

        // segment_frames % intra_period != 0 rejected on configure() and start()
        let mut enc = EncoderProcess::new(valid()).unwrap();
        assert_eq!(
            enc.configure(&["--intra-period".to_owned(), "60".to_owned()])
                .unwrap_err()
                .code,
            "invalid_argument"
        );
        let mut misaligned_default = valid();
        misaligned_default.segment_frames = 100; // 100 % 30 != 0
        let mut misaligned_enc = EncoderProcess::new(misaligned_default).unwrap();
        assert_eq!(misaligned_enc.start().unwrap_err().code, "invalid_argument");

        // 0-byte frame rejected up front
        let mut zero_check = EncoderProcess::new(valid()).unwrap();
        assert_eq!(
            zero_check.submit_frame(0, 100, b"").unwrap_err().code,
            "invalid_argument"
        );
    }

    #[test]
    fn shm_encoder_crash_after_ready_fails_fast_with_stderr_and_never_stalls_10s() {
        use std::os::unix::fs::PermissionsExt;
        use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "miro-ego-encoder-crash-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let gate = root.join("crash.gate");
        let helper = root.join("crash-helper.sh");
        // Emit ready + stats (not done!), wait until test fills the 4 SHM slots, write stderr,
        // and exit non-zero without emitting error or done.
        std::fs::write(
            &helper,
            format!(
                "#!/bin/sh\nprintf '{{\"event\":\"ready\"}}\\n'\nprintf '{{\"event\":\"stats\",\"frames\":0}}\\n'\nwhile [ ! -e \"{}\" ]; do sleep 0.01; done\nprintf 'fatal simulated VPU SIGSEGV\\n' >&2\nexit 139\n",
                gate.display()
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
                width: 64,
                height: 32,
                fps: 30,
                bitrate_kbps: 8192,
                segment_frames: 30,
                path_prefix: "video/",
            },
            EncoderTransport::ShmRing(ShmRingConfig {
                slot_count: 4,
                slot_capacity: 2 * 1024 * 1024,
            }),
        )
        .unwrap();
        encoder.start().unwrap();

        // Fill all 4 SHM slots while helper is alive.
        for seq in 0..4_u64 {
            encoder
                .submit_frame(seq, (seq + 1) * 1_000_000, &[0xAB; 64])
                .unwrap();
        }
        assert_eq!(encoder.submitted_frames(), 4);

        // Trigger helper crash after a short delay while submit_frame(4) is blocked on full ring!
        let gate_clone = gate.clone();
        let trigger = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            std::fs::write(gate_clone, b"crash").unwrap();
        });

        let started = Instant::now();
        let err = encoder.submit_frame(4, 5_000_000, &[0xCD; 64]).unwrap_err();
        let elapsed = started.elapsed();
        trigger.join().unwrap();

        assert_eq!(err.code, "encoder_unexpected_eof");
        assert!(
            err.message.contains("fatal simulated VPU SIGSEGV"),
            "expected stderr tail in error message, got: {}",
            err.message
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "submit_frame stalled for {elapsed:?} instead of waking immediately on crash"
        );
        assert_eq!(encoder.submitted_frames(), 4);
        assert_eq!(encoder.failure().unwrap().code, "encoder_unexpected_eof");

        encoder.abort();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn shm_slot_capacity_validates_mjpeg_sizing_and_accepts_4mib_default() {
        let cfg = || EncoderConfig {
            out_dir: std::path::Path::new("/unused"),
            executable: std::path::Path::new("/unused/helper"),
            width: 3840,
            height: 1080,
            fps: 30,
            bitrate_kbps: 12_000,
            segment_frames: 150,
            path_prefix: "video/",
        };
        let min_sbs = super::min_shm_slot_capacity(3840, 1080).unwrap();
        assert_eq!(min_sbs, 4_149_248);
        assert_eq!(
            super::min_shm_slot_capacity(64, 32).unwrap(),
            2 * 1024 * 1024
        );
        assert_eq!(
            super::min_shm_slot_capacity(7680, 2160).unwrap(),
            8 * 1024 * 1024
        );
        let err = match EncoderProcess::with_transport(
            cfg(),
            EncoderTransport::ShmRing(ShmRingConfig {
                slot_count: 8,
                slot_capacity: 2 * 1024 * 1024,
            }),
        ) {
            Ok(_) => panic!("expected 2 MiB slot_capacity to be rejected for 3840x1080"),
            Err(err) => err,
        };
        assert_eq!(err.code, "invalid_shm_config");
        assert!(EncoderProcess::new(cfg()).is_ok());

        let uhd_cfg = || EncoderConfig {
            width: 3840,
            height: 2160,
            ..cfg()
        };
        let uhd_new = EncoderProcess::new(uhd_cfg()).unwrap();
        assert_eq!(
            uhd_new.transport_mode(),
            EncoderTransport::ShmRing(ShmRingConfig::for_dimensions(3840, 2160))
        );
        let uhd_default =
            EncoderProcess::with_transport(uhd_cfg(), EncoderTransport::default()).unwrap();
        assert_eq!(
            uhd_default.transport_mode(),
            EncoderTransport::ShmRing(ShmRingConfig::for_dimensions(3840, 2160))
        );
    }

    #[test]
    fn configure_validates_all_qp_bounds_clears_stale_state_and_reaper_prevents_zombies() {
        use std::os::unix::fs::PermissionsExt;
        use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

        let mut enc = EncoderProcess::new(EncoderConfig {
            out_dir: std::path::Path::new("/unused"),
            executable: std::path::Path::new("/unused/helper"),
            width: 3840,
            height: 1080,
            fps: 30,
            bitrate_kbps: 12_000,
            segment_frames: 150,
            path_prefix: "video/",
        })
        .unwrap();
        // Inverted min > max rejected
        assert_eq!(
            enc.configure(&[
                "--min-qp".to_owned(),
                "42".to_owned(),
                "--max-qp".to_owned(),
                "30".to_owned(),
            ])
            .unwrap_err()
            .code,
            "invalid_argument"
        );
        // Raising min-qp above default intra_qp (22) / initial_qp (24) without updating them is rejected
        assert_eq!(
            enc.configure(&["--min-qp".to_owned(), "35".to_owned()])
                .unwrap_err()
                .code,
            "invalid_argument"
        );
        // Explicit intra-qp outside min..=max is rejected
        assert_eq!(
            enc.configure(&[
                "--min-qp".to_owned(),
                "20".to_owned(),
                "--max-qp".to_owned(),
                "35".to_owned(),
                "--intra-qp".to_owned(),
                "18".to_owned(),
                "--initial-qp".to_owned(),
                "24".to_owned(),
            ])
            .unwrap_err()
            .code,
            "invalid_argument"
        );
        // Explicit initial-qp outside min..=max is rejected
        assert_eq!(
            enc.configure(&[
                "--min-qp".to_owned(),
                "20".to_owned(),
                "--max-qp".to_owned(),
                "35".to_owned(),
                "--intra-qp".to_owned(),
                "22".to_owned(),
                "--initial-qp".to_owned(),
                "40".to_owned(),
            ])
            .unwrap_err()
            .code,
            "invalid_argument"
        );

        // Valid high-QP set succeeds, and subsequent reconfigure clears stale QP/args/failure state!
        enc.configure(&[
            "--min-qp".to_owned(),
            "35".to_owned(),
            "--max-qp".to_owned(),
            "45".to_owned(),
            "--intra-qp".to_owned(),
            "36".to_owned(),
            "--initial-qp".to_owned(),
            "38".to_owned(),
            "--bitrate-kbps".to_owned(),
            "16000".to_owned(),
        ])
        .unwrap();
        super::handle_event(
            &enc.shared,
            br#"{"event":"error","code":"stale_err","message":"stale"}"#,
        )
        .unwrap();
        assert!(enc.failure().is_some());
        enc.submitted = 12;

        // Reconfigure back to codec=hevc: clears stale min_qp=35, deduplicates --bitrate-kbps, and resets state
        enc.configure(&["--codec".to_owned(), "hevc".to_owned()])
            .unwrap();
        assert!(enc.failure().is_none());
        assert_eq!(enc.submitted_frames(), 0);
        assert_eq!(enc.min_qp, None);
        assert_eq!(
            enc.args
                .iter()
                .filter(|a| a.as_str() == "--bitrate-kbps")
                .count(),
            1
        );

        // Verify detached background reaper reaps child if wait times out during abort
        let mut child = std::process::Command::new("/bin/sleep")
            .arg("0.15")
            .spawn()
            .unwrap();
        let child_pid = child.id() as i32;
        enc.shared.live_pgid.lock().unwrap().replace(child_pid);
        assert!(
            super::wait_child_and_clear_pgid(&mut child, &enc.shared.live_pgid, Duration::ZERO)
                .unwrap()
                .is_none()
        );
        super::spawn_detached_child_reaper(child, std::sync::Arc::clone(&enc.shared));
        let reap_deadline = Instant::now() + Duration::from_secs(2);
        while enc.shared.live_pgid.lock().unwrap().is_some() {
            assert!(
                Instant::now() < reap_deadline,
                "detached encoder reaper failed to reap child and clear live_pgid"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(unsafe { libc::kill(child_pid, 0) }, -1);

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "miro-ego-encoder-causal-timeout-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let helper = root.join("causal-timeout.sh");
        std::fs::write(
            &helper,
            "#!/bin/sh\nprintf '{\"event\":\"ready\"}\\n'\ncat >/dev/null\nprintf '{\"event\":\"error\",\"code\":\"vpu_hw_timeout\",\"message\":\"hardware hung\"}\\n'\nexec sleep 30\n",
        )
        .unwrap();
        std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mut encoder = EncoderProcess::with_transport(
            EncoderConfig {
                out_dir: &root,
                executable: &helper,
                width: 64,
                height: 32,
                fps: 30,
                bitrate_kbps: 8192,
                segment_frames: 30,
                path_prefix: "video/",
            },
            EncoderTransport::StdinPipe,
        )
        .unwrap();
        encoder.start().unwrap();
        let err = encoder.finish(Duration::from_millis(80)).unwrap_err();
        assert_eq!(err.code, "vpu_hw_timeout");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn abort_handle_unblocks_full_stdin_submission_and_kills_process_group() {
        use std::os::unix::fs::PermissionsExt;
        use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "miro-ego-encoder-pgid-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let sub_pid_file = root.join("grandchild.pid");
        let helper = root.join("pgid-helper.sh");
        // Spawn a grandchild sleep in the same process group and never read stdin so pipe fills!
        std::fs::write(
            &helper,
            format!(
                "#!/bin/sh\nsleep 30 &\necho $! > \"{}\"\nprintf '{{\"event\":\"ready\"}}\\n'\nwait\n",
                sub_pid_file.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mut encoder = EncoderProcess::with_transport(
            EncoderConfig {
                out_dir: &root,
                executable: &helper,
                width: 64,
                height: 32,
                fps: 30,
                bitrate_kbps: 8192,
                segment_frames: 30,
                path_prefix: "video/",
            },
            EncoderTransport::StdinPipe,
        )
        .unwrap();
        encoder.start().unwrap();
        let abort_handle = encoder.abort_handle();
        let encoder = std::sync::Arc::new(std::sync::Mutex::new(encoder));

        let grandchild_pid: i32 = std::fs::read_to_string(&sub_pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();

        let enc_writer = std::sync::Arc::clone(&encoder);
        let writer = std::thread::spawn(move || {
            let mut guard = enc_writer.lock().unwrap();
            let big = vec![0x55_u8; 256 * 1024];
            for seq in 0..64_u64 {
                guard.submit_frame(seq, (seq + 1) * 1000, &big)?;
            }
            Ok::<(), super::EncoderProcessError>(())
        });

        std::thread::sleep(Duration::from_millis(40));
        let started = Instant::now();
        abort_handle.abort();
        let err = writer.join().unwrap().unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(matches!(
            err.code.as_str(),
            "encoder_aborted" | "encoder_unexpected_eof" | "write_failed"
        ));
        encoder.lock().unwrap().abort();

        // Wait briefly for grandchild in process group to die (or become a zombie when PID 1 in a
        // minimal PID namespace does not immediately reap reparented children).
        let reap_deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let alive = unsafe { libc::kill(grandchild_pid, 0) };
            let is_zombie = alive == 0
                && std::fs::read_to_string(format!("/proc/{grandchild_pid}/stat"))
                    .ok()
                    .and_then(|stat| {
                        stat.rsplit_once(") ")
                            .and_then(|(_, rest)| rest.chars().next())
                    })
                    .is_some_and(|state| matches!(state, 'Z' | 'X'));
            if alive == -1 || is_zombie {
                break;
            }
            assert!(
                Instant::now() < reap_deadline,
                "grandchild in encoder process group survived abort()"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
