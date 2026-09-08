use libloading::Library;
use std::ffi::{CStr, CString};
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::os::raw::{c_char, c_int, c_uint, c_void};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const SND_PCM_STREAM_CAPTURE: c_int = 1;
const SND_PCM_NONBLOCK: c_int = 0x0001;
const SND_PCM_ACCESS_RW_INTERLEAVED: c_int = 3;
const SND_PCM_FORMAT_S16_LE: c_int = 2;
const SAMPLE_FORMAT: &str = "S16_LE";
const SAMPLE_CODEC: &str = "pcm_s16le";
const BYTES_PER_SAMPLE: u64 = 2;
const WAV_HEADER_BYTES: u64 = 44;
const DEFAULT_PERIOD_FRAMES: u64 = 1024;
const START_TIMEOUT: Duration = Duration::from_secs(2);
const READ_IDLE_SLEEP: Duration = Duration::from_millis(2);
const QUEUE_BLOCKS: usize = 256;

#[repr(C)]
struct SndPcm {
    _private: [u8; 0],
}

#[repr(C)]
struct SndPcmHwParams {
    _private: [u8; 0],
}

type SndPcmSframes = libc::c_long;
type SndPcmUframes = libc::c_ulong;

type SndPcmOpen = unsafe extern "C" fn(*mut *mut SndPcm, *const c_char, c_int, c_int) -> c_int;
type SndPcmClose = unsafe extern "C" fn(*mut SndPcm) -> c_int;
type SndPcmNonblock = unsafe extern "C" fn(*mut SndPcm, c_int) -> c_int;
type SndPcmHwParamsMalloc = unsafe extern "C" fn(*mut *mut SndPcmHwParams) -> c_int;
type SndPcmHwParamsAny = unsafe extern "C" fn(*mut SndPcm, *mut SndPcmHwParams) -> c_int;
type SndPcmHwParamsSetAccess =
    unsafe extern "C" fn(*mut SndPcm, *mut SndPcmHwParams, c_int) -> c_int;
type SndPcmHwParamsSetFormat =
    unsafe extern "C" fn(*mut SndPcm, *mut SndPcmHwParams, c_int) -> c_int;
type SndPcmHwParamsSetChannels =
    unsafe extern "C" fn(*mut SndPcm, *mut SndPcmHwParams, c_uint) -> c_int;
type SndPcmHwParamsSetRateNear =
    unsafe extern "C" fn(*mut SndPcm, *mut SndPcmHwParams, *mut c_uint, *mut c_int) -> c_int;
type SndPcmHwParamsSetPeriodSizeNear =
    unsafe extern "C" fn(*mut SndPcm, *mut SndPcmHwParams, *mut SndPcmUframes, *mut c_int) -> c_int;
type SndPcmHwParamsSetBufferSizeNear =
    unsafe extern "C" fn(*mut SndPcm, *mut SndPcmHwParams, *mut SndPcmUframes) -> c_int;
type SndPcmHwParamsApply = unsafe extern "C" fn(*mut SndPcm, *mut SndPcmHwParams) -> c_int;
type SndPcmHwParamsFree = unsafe extern "C" fn(*mut SndPcmHwParams);
type SndPcmPrepare = unsafe extern "C" fn(*mut SndPcm) -> c_int;
type SndPcmReadi = unsafe extern "C" fn(*mut SndPcm, *mut c_void, SndPcmUframes) -> SndPcmSframes;
type SndPcmDrop = unsafe extern "C" fn(*mut SndPcm) -> c_int;
type SndStrError = unsafe extern "C" fn(c_int) -> *const c_char;
type SwMalloc = unsafe extern "C" fn(*mut *mut c_void) -> c_int;
type SwApply = unsafe extern "C" fn(*mut SndPcm, *mut c_void) -> c_int;
type SwSet = unsafe extern "C" fn(*mut SndPcm, *mut c_void, c_int) -> c_int;
type SwFree = unsafe extern "C" fn(*mut c_void);
type Htimestamp =
    unsafe extern "C" fn(*mut SndPcm, *mut SndPcmUframes, *mut libc::timespec) -> c_int;
type GetParams = unsafe extern "C" fn(*mut SndPcm, *mut SndPcmUframes, *mut SndPcmUframes) -> c_int;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AudioError {
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

impl AudioError {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    fn io(code: &'static str, context: &str, error: io::Error) -> Self {
        Self::new(code, format!("{context}: {error}"))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AudioSegment {
    pub(crate) index: u64,
    pub(crate) relative_path: String,
    pub(crate) start_sample: u64,
    pub(crate) end_sample: u64,
    pub(crate) start_time_seconds: f64,
    pub(crate) end_time_seconds: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct AudioRecordingResult {
    pub(crate) capture_clock: serde_json::Value,
    pub(crate) device: String,
    pub(crate) sample_rate_hz: u32,
    pub(crate) channels: u16,
    pub(crate) sample_format: &'static str,
    pub(crate) codec: &'static str,
    pub(crate) container: &'static str,
    pub(crate) sample_count: u64,
    pub(crate) started_monotonic_ns: u64,
    pub(crate) stopped_monotonic_ns: u64,
    pub(crate) segments: Vec<AudioSegment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AudioRecordingSnapshot {
    pub(crate) sample_count: u64,
    pub(crate) bytes_written: u64,
    pub(crate) segment_count: u64,
}

#[derive(Default)]
struct AudioProgress {
    error: Mutex<Option<AudioError>>,
    queued_frames: AtomicU64,
    queue_peak_frames: AtomicU64,
    max_write_ns: AtomicU64,
    sample_count: AtomicU64,
    bytes_written: AtomicU64,
    segment_count: AtomicU64,
}

impl AudioProgress {
    fn fail(&self, error: AudioError) {
        if let Ok(mut first) = self.error.lock() {
            first.get_or_insert(error);
        }
    }

    fn check(&self) -> Result<(), AudioError> {
        let error = self
            .error
            .lock()
            .map_err(|_| AudioError::new("audio_failed", "audio error mutex poisoned"))?;
        match &*error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    fn publish(&self, snapshot: &AudioRecordingSnapshot) {
        self.sample_count
            .store(snapshot.sample_count, Ordering::Release);
        self.bytes_written
            .store(snapshot.bytes_written, Ordering::Release);
        self.segment_count
            .store(snapshot.segment_count, Ordering::Release);
    }

    fn snapshot(&self) -> AudioRecordingSnapshot {
        AudioRecordingSnapshot {
            sample_count: self.sample_count.load(Ordering::Acquire),
            bytes_written: self.bytes_written.load(Ordering::Acquire),
            segment_count: self.segment_count.load(Ordering::Acquire),
        }
    }
}

#[derive(Clone)]
struct RecorderConfig {
    session_root: PathBuf,
    device: String,
    sample_rate_hz: u32,
    channels: u16,
    segment_seconds: f64,
}

pub(crate) struct Recorder {
    config: RecorderConfig,
    lifecycle: Mutex<Lifecycle>,
    stop: Arc<AtomicBool>,
    progress: Arc<AudioProgress>,
}

struct Lifecycle {
    state: State,
    done: Option<Receiver<Result<AudioRecordingResult, AudioError>>>,
    worker: Option<JoinHandle<()>>,
    result: Option<AudioRecordingResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Open,
    Running,
    Stopped,
    Aborted,
    Closed,
}

impl Recorder {
    pub(crate) fn new(
        session_root: &str,
        device: &str,
        sample_rate_hz: u32,
        channels: u16,
        segment_seconds: f64,
    ) -> Result<Self, AudioError> {
        if device.is_empty()
            || sample_rate_hz == 0
            || channels == 0
            || !segment_seconds.is_finite()
            || segment_seconds <= 0.0
        {
            return Err(AudioError::new(
                "invalid_argument",
                "native audio recorder parameters are invalid",
            ));
        }
        Ok(Self {
            config: RecorderConfig {
                session_root: PathBuf::from(session_root),
                device: device.to_owned(),
                sample_rate_hz,
                channels,
                segment_seconds,
            },
            lifecycle: Mutex::new(Lifecycle {
                state: State::Open,
                done: None,
                worker: None,
                result: None,
            }),
            stop: Arc::new(AtomicBool::new(false)),
            progress: Arc::new(AudioProgress::default()),
        })
    }

    pub(crate) fn start(&self) -> Result<(), AudioError> {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| AudioError::new("native_audio_poisoned", "audio mutex is poisoned"))?;
        if lifecycle.state != State::Open {
            return Err(AudioError::new(
                "invalid_state",
                "native audio recorder can only be started once",
            ));
        }
        self.stop.store(false, Ordering::Release);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (done_tx, done_rx) = mpsc::channel();
        let config = self.config.clone();
        let stop = Arc::clone(&self.stop);
        let progress = Arc::clone(&self.progress);
        let worker = thread::spawn(move || {
            let result = start_and_capture(config, stop, Arc::clone(&progress), ready_tx);
            if let Err(error) = &result {
                progress.fail(error.clone());
            }
            let _ = done_tx.send(result);
        });
        match ready_rx.recv_timeout(START_TIMEOUT) {
            Ok(Ok(())) => {
                lifecycle.done = Some(done_rx);
                lifecycle.worker = Some(worker);
                lifecycle.state = State::Running;
                Ok(())
            }
            Ok(Err(error)) => {
                let _ = worker.join();
                lifecycle.state = State::Closed;
                Err(error)
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.stop.store(true, Ordering::Release);
                let _ = worker.join();
                lifecycle.state = State::Closed;
                Err(AudioError::new(
                    "audio_start_timeout",
                    "native audio recorder did not initialize in time",
                ))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = worker.join();
                lifecycle.state = State::Closed;
                Err(AudioError::new(
                    "audio_start_failed",
                    "native audio recorder worker exited before ready",
                ))
            }
        }
    }

    pub(crate) fn stop(&self, timeout: Duration) -> Result<AudioRecordingResult, AudioError> {
        let (done, worker) = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .map_err(|_| AudioError::new("native_audio_poisoned", "audio mutex is poisoned"))?;
            match lifecycle.state {
                State::Stopped => {
                    return lifecycle.result.clone().ok_or_else(|| {
                        AudioError::new("invalid_state", "native audio result is missing")
                    });
                }
                State::Open => {
                    lifecycle.state = State::Stopped;
                    return Err(AudioError::new(
                        "invalid_state",
                        "native audio recorder was not started",
                    ));
                }
                State::Aborted | State::Closed => {
                    return Err(AudioError::new(
                        "invalid_state",
                        "native audio recorder is already closed",
                    ));
                }
                State::Running => {
                    self.stop.store(true, Ordering::Release);
                    (
                        lifecycle.done.take().ok_or_else(|| {
                            AudioError::new("invalid_state", "native audio done channel is missing")
                        })?,
                        lifecycle.worker.take().ok_or_else(|| {
                            AudioError::new("invalid_state", "native audio worker is missing")
                        })?,
                    )
                }
            }
        };

        match done.recv_timeout(timeout) {
            Ok(result) => {
                worker.join().map_err(|_| {
                    AudioError::new("native_audio_worker_failed", "audio worker panicked")
                })?;
                let mut lifecycle = self.lifecycle.lock().map_err(|_| {
                    AudioError::new("native_audio_poisoned", "audio mutex is poisoned")
                })?;
                lifecycle.state = State::Stopped;
                lifecycle.result = Some(result.clone()?);
                result
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let mut lifecycle = self.lifecycle.lock().map_err(|_| {
                    AudioError::new("native_audio_poisoned", "audio mutex is poisoned")
                })?;
                lifecycle.done = Some(done);
                lifecycle.worker = Some(worker);
                lifecycle.state = State::Running;
                Err(AudioError::new(
                    "audio_stop_timeout",
                    "native audio recorder did not stop in time",
                ))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                worker.join().map_err(|_| {
                    AudioError::new("native_audio_worker_failed", "audio worker panicked")
                })?;
                let mut lifecycle = self.lifecycle.lock().map_err(|_| {
                    AudioError::new("native_audio_poisoned", "audio mutex is poisoned")
                })?;
                lifecycle.state = State::Closed;
                Err(AudioError::new(
                    "audio_failed",
                    "native audio recorder exited without a result",
                ))
            }
        }
    }

    pub(crate) fn snapshot(&self) -> AudioRecordingSnapshot {
        self.progress.snapshot()
    }

    pub(crate) fn check_health(&self) -> Result<(), AudioError> {
        self.progress.check()
    }

    pub(crate) fn abort(&self) {
        self.stop.store(true, Ordering::Release);
        let worker = self
            .lifecycle
            .lock()
            .ok()
            .and_then(|mut lifecycle| lifecycle.worker.take());
        if let Some(worker) = worker {
            let _ = worker.join();
        }
        if let Ok(mut lifecycle) = self.lifecycle.lock() {
            lifecycle.done = None;
            if !matches!(lifecycle.state, State::Stopped | State::Closed) {
                lifecycle.state = State::Aborted;
            }
        }
    }

    pub(crate) fn close(&self) {
        self.abort();
        if let Ok(mut lifecycle) = self.lifecycle.lock() {
            lifecycle.state = State::Closed;
        }
    }
}

impl Drop for Recorder {
    fn drop(&mut self) {
        self.abort();
    }
}

struct Alsa {
    sw_malloc: SwMalloc,
    sw_current: SwApply,
    sw_apply: SwApply,
    sw_mode: SwSet,
    sw_type: SwSet,
    sw_free: SwFree,
    htimestamp: Htimestamp,
    get_params: GetParams,
    _library: Library,
    snd_pcm_open: SndPcmOpen,
    snd_pcm_close: SndPcmClose,
    snd_pcm_nonblock: SndPcmNonblock,
    snd_pcm_hw_params_malloc: SndPcmHwParamsMalloc,
    snd_pcm_hw_params_any: SndPcmHwParamsAny,
    snd_pcm_hw_params_set_access: SndPcmHwParamsSetAccess,
    snd_pcm_hw_params_set_format: SndPcmHwParamsSetFormat,
    snd_pcm_hw_params_set_channels: SndPcmHwParamsSetChannels,
    snd_pcm_hw_params_set_rate_near: SndPcmHwParamsSetRateNear,
    snd_pcm_hw_params_set_period_size_near: SndPcmHwParamsSetPeriodSizeNear,
    snd_pcm_hw_params_set_buffer_size_near: SndPcmHwParamsSetBufferSizeNear,
    snd_pcm_hw_params: SndPcmHwParamsApply,
    snd_pcm_hw_params_free: SndPcmHwParamsFree,
    snd_pcm_prepare: SndPcmPrepare,
    snd_pcm_readi: SndPcmReadi,
    snd_pcm_drop: SndPcmDrop,
    snd_strerror: SndStrError,
}

unsafe impl Send for Alsa {}
unsafe impl Sync for Alsa {}

impl Alsa {
    fn load() -> Result<Arc<Self>, AudioError> {
        // SAFETY: loaded function pointers are kept valid by storing Library in Alsa.
        unsafe {
            let library = Library::new("libasound.so.2")
                .map_err(|error| AudioError::new("audio_unavailable", error.to_string()))?;
            Ok(Arc::new(Self {
                sw_malloc: load_symbol(&library, b"snd_pcm_sw_params_malloc\0")?,
                sw_current: load_symbol(&library, b"snd_pcm_sw_params_current\0")?,
                sw_apply: load_symbol(&library, b"snd_pcm_sw_params\0")?,
                sw_mode: load_symbol(&library, b"snd_pcm_sw_params_set_tstamp_mode\0")?,
                sw_type: load_symbol(&library, b"snd_pcm_sw_params_set_tstamp_type\0")?,
                sw_free: load_symbol(&library, b"snd_pcm_sw_params_free\0")?,
                htimestamp: load_symbol(&library, b"snd_pcm_htimestamp\0")?,
                get_params: load_symbol(&library, b"snd_pcm_get_params\0")?,
                snd_pcm_open: load_symbol(&library, b"snd_pcm_open\0")?,
                snd_pcm_close: load_symbol(&library, b"snd_pcm_close\0")?,
                snd_pcm_nonblock: load_symbol(&library, b"snd_pcm_nonblock\0")?,
                snd_pcm_hw_params_malloc: load_symbol(&library, b"snd_pcm_hw_params_malloc\0")?,
                snd_pcm_hw_params_any: load_symbol(&library, b"snd_pcm_hw_params_any\0")?,
                snd_pcm_hw_params_set_access: load_symbol(
                    &library,
                    b"snd_pcm_hw_params_set_access\0",
                )?,
                snd_pcm_hw_params_set_format: load_symbol(
                    &library,
                    b"snd_pcm_hw_params_set_format\0",
                )?,
                snd_pcm_hw_params_set_channels: load_symbol(
                    &library,
                    b"snd_pcm_hw_params_set_channels\0",
                )?,
                snd_pcm_hw_params_set_rate_near: load_symbol(
                    &library,
                    b"snd_pcm_hw_params_set_rate_near\0",
                )?,
                snd_pcm_hw_params_set_period_size_near: load_symbol(
                    &library,
                    b"snd_pcm_hw_params_set_period_size_near\0",
                )?,
                snd_pcm_hw_params_set_buffer_size_near: load_symbol(
                    &library,
                    b"snd_pcm_hw_params_set_buffer_size_near\0",
                )?,
                snd_pcm_hw_params: load_symbol(&library, b"snd_pcm_hw_params\0")?,
                snd_pcm_hw_params_free: load_symbol(&library, b"snd_pcm_hw_params_free\0")?,
                snd_pcm_prepare: load_symbol(&library, b"snd_pcm_prepare\0")?,
                snd_pcm_readi: load_symbol(&library, b"snd_pcm_readi\0")?,
                snd_pcm_drop: load_symbol(&library, b"snd_pcm_drop\0")?,
                snd_strerror: load_symbol(&library, b"snd_strerror\0")?,
                _library: library,
            }))
        }
    }

    fn error(&self, code: &'static str, context: &str, result: c_int) -> AudioError {
        // SAFETY: snd_strerror returns a static NUL-terminated string for ALSA errors.
        let message = unsafe {
            let pointer = (self.snd_strerror)(result);
            if pointer.is_null() {
                format!("{context}: ALSA error {result}")
            } else {
                format!("{context}: {}", CStr::from_ptr(pointer).to_string_lossy())
            }
        };
        AudioError::new(code, message)
    }
}

unsafe fn load_symbol<T: Copy>(library: &Library, name: &[u8]) -> Result<T, AudioError> {
    // SAFETY: caller keeps the Library alive for at least as long as copied function pointers.
    let symbol = unsafe { library.get::<T>(name) }
        .map_err(|error| AudioError::new("audio_unavailable", error.to_string()))?;
    Ok(*symbol)
}

struct Pcm {
    period_frames: u64,
    buffer_frames: u64,
    handle: *mut SndPcm,
    alsa: Arc<Alsa>,
}

unsafe impl Send for Pcm {}

impl Pcm {
    fn open(config: &RecorderConfig) -> Result<Self, AudioError> {
        let alsa = Alsa::load()?;
        let device = CString::new(config.device.as_str())
            .map_err(|_| AudioError::new("invalid_argument", "audio device contains NUL"))?;
        let mut handle: *mut SndPcm = std::ptr::null_mut();
        // SAFETY: handle points to writable storage, device is NUL-terminated.
        let result = unsafe {
            (alsa.snd_pcm_open)(
                &mut handle,
                device.as_ptr(),
                SND_PCM_STREAM_CAPTURE,
                SND_PCM_NONBLOCK,
            )
        };
        if result < 0 {
            return Err(alsa.error("audio_unavailable", "snd_pcm_open", result));
        }
        let mut pcm = Self {
            handle,
            alsa,
            period_frames: 0,
            buffer_frames: 0,
        };
        if let Err(error) = pcm.configure(config) {
            let _ = pcm.close();
            return Err(error);
        }
        Ok(pcm)
    }

    fn configure(&mut self, config: &RecorderConfig) -> Result<(), AudioError> {
        let mut raw_params: *mut SndPcmHwParams = std::ptr::null_mut();
        // SAFETY: raw_params points to writable storage.
        self.check("audio_unavailable", "snd_pcm_hw_params_malloc", unsafe {
            (self.alsa.snd_pcm_hw_params_malloc)(&mut raw_params)
        })?;
        let params = HwParams {
            pointer: raw_params,
            alsa: Arc::clone(&self.alsa),
        };
        self.check("audio_unavailable", "snd_pcm_hw_params_any", unsafe {
            (self.alsa.snd_pcm_hw_params_any)(self.handle, params.pointer)
        })?;
        self.check(
            "unsupported_audio_mode",
            "snd_pcm_hw_params_set_access",
            unsafe {
                (self.alsa.snd_pcm_hw_params_set_access)(
                    self.handle,
                    params.pointer,
                    SND_PCM_ACCESS_RW_INTERLEAVED,
                )
            },
        )?;
        self.check(
            "unsupported_audio_mode",
            "snd_pcm_hw_params_set_format",
            unsafe {
                (self.alsa.snd_pcm_hw_params_set_format)(
                    self.handle,
                    params.pointer,
                    SND_PCM_FORMAT_S16_LE,
                )
            },
        )?;
        self.check(
            "unsupported_audio_mode",
            "snd_pcm_hw_params_set_channels",
            unsafe {
                (self.alsa.snd_pcm_hw_params_set_channels)(
                    self.handle,
                    params.pointer,
                    c_uint::from(config.channels),
                )
            },
        )?;
        let mut rate = c_uint::from(config.sample_rate_hz);
        let mut direction: c_int = 0;
        self.check(
            "unsupported_audio_mode",
            "snd_pcm_hw_params_set_rate_near",
            unsafe {
                (self.alsa.snd_pcm_hw_params_set_rate_near)(
                    self.handle,
                    params.pointer,
                    &mut rate,
                    &mut direction,
                )
            },
        )?;
        if rate != config.sample_rate_hz {
            return Err(AudioError::new(
                "unsupported_audio_mode",
                format!(
                    "requested {} Hz but ALSA selected {} Hz",
                    config.sample_rate_hz, rate
                ),
            ));
        }
        let mut period = SndPcmUframes::try_from(DEFAULT_PERIOD_FRAMES)
            .map_err(|_| AudioError::new("invalid_argument", "period is too large"))?;
        self.check(
            "unsupported_audio_mode",
            "snd_pcm_hw_params_set_period_size_near",
            unsafe {
                (self.alsa.snd_pcm_hw_params_set_period_size_near)(
                    self.handle,
                    params.pointer,
                    &mut period,
                    &mut direction,
                )
            },
        )?;
        let mut buffer = SndPcmUframes::try_from(DEFAULT_PERIOD_FRAMES * 8)
            .map_err(|_| AudioError::new("invalid_argument", "buffer is too large"))?;
        self.check(
            "unsupported_audio_mode",
            "snd_pcm_hw_params_set_buffer_size_near",
            unsafe {
                (self.alsa.snd_pcm_hw_params_set_buffer_size_near)(
                    self.handle,
                    params.pointer,
                    &mut buffer,
                )
            },
        )?;
        self.check("unsupported_audio_mode", "snd_pcm_hw_params", unsafe {
            (self.alsa.snd_pcm_hw_params)(self.handle, params.pointer)
        })?;
        self.check("unsupported_audio_mode", "snd_pcm_get_params", unsafe {
            (self.alsa.get_params)(self.handle, &mut buffer, &mut period)
        })?;
        self.period_frames = period as u64;
        self.buffer_frames = buffer as u64;
        let mut sw = std::ptr::null_mut();
        self.check("audio_failed", "sw_params_malloc", unsafe {
            (self.alsa.sw_malloc)(&mut sw)
        })?;
        let configured = (|| {
            self.check("audio_failed", "sw_params_current", unsafe {
                (self.alsa.sw_current)(self.handle, sw)
            })?;
            self.check("audio_failed", "timestamp_enable", unsafe {
                (self.alsa.sw_mode)(self.handle, sw, 1)
            })?;
            self.check("audio_failed", "timestamp_monotonic", unsafe {
                (self.alsa.sw_type)(self.handle, sw, 1)
            })?;
            self.check("audio_failed", "sw_params", unsafe {
                (self.alsa.sw_apply)(self.handle, sw)
            })
        })();
        unsafe { (self.alsa.sw_free)(sw) };
        configured?;
        self.check("audio_failed", "snd_pcm_prepare", unsafe {
            (self.alsa.snd_pcm_prepare)(self.handle)
        })?;
        self.check("audio_failed", "snd_pcm_nonblock", unsafe {
            (self.alsa.snd_pcm_nonblock)(self.handle, 1)
        })?;
        Ok(())
    }

    fn readi(&mut self, buffer: &mut [u8], frames: u64) -> Result<ReadOutcome, AudioError> {
        let frames = SndPcmUframes::try_from(frames)
            .map_err(|_| AudioError::new("invalid_argument", "audio read size is too large"))?;
        let result = unsafe {
            (self.alsa.snd_pcm_readi)(self.handle, buffer.as_mut_ptr().cast::<c_void>(), frames)
        };
        classify_read_result(result).map_err(|code| {
            self.alsa.error(
                "audio_failed",
                "snd_pcm_readi: capture continuity lost",
                code,
            )
        })
    }

    fn clock_anchor(&self, read_frames: u64) -> Result<(u64, u64), AudioError> {
        let mut available = 0;
        let mut timestamp = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        self.check("audio_failed", "snd_pcm_htimestamp", unsafe {
            (self.alsa.htimestamp)(self.handle, &mut available, &mut timestamp)
        })?;
        if timestamp.tv_sec < 0
            || !(0..1_000_000_000).contains(&timestamp.tv_nsec)
            || available as u64 > self.buffer_frames
        {
            return Err(AudioError::new(
                "audio_failed",
                "invalid ALSA clock or overrun",
            ));
        }
        Ok((
            read_frames + available as u64,
            timestamp.tv_sec as u64 * 1_000_000_000 + timestamp.tv_nsec as u64,
        ))
    }

    fn check(&self, code: &'static str, context: &str, result: c_int) -> Result<(), AudioError> {
        if result < 0 {
            Err(self.alsa.error(code, context, result))
        } else {
            Ok(())
        }
    }

    fn close(&mut self) -> Result<(), AudioError> {
        if self.handle.is_null() {
            return Ok(());
        }
        let handle = self.handle;
        self.handle = std::ptr::null_mut();
        let drop_result = unsafe { (self.alsa.snd_pcm_drop)(handle) };
        let close_result = unsafe { (self.alsa.snd_pcm_close)(handle) };
        if close_result < 0 {
            return Err(self
                .alsa
                .error("audio_failed", "snd_pcm_close", close_result));
        }
        if drop_result < 0 {
            return Err(self.alsa.error("audio_failed", "snd_pcm_drop", drop_result));
        }
        Ok(())
    }
}

impl Drop for Pcm {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

struct HwParams {
    pointer: *mut SndPcmHwParams,
    alsa: Arc<Alsa>,
}

impl Drop for HwParams {
    fn drop(&mut self) {
        if !self.pointer.is_null() {
            unsafe { (self.alsa.snd_pcm_hw_params_free)(self.pointer) };
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ReadOutcome {
    Frames(u64),
    Again,
}

fn classify_read_result(result: SndPcmSframes) -> Result<ReadOutcome, c_int> {
    if result > 0 {
        return Ok(ReadOutcome::Frames(result as u64));
    }
    let code = c_int::try_from(result).unwrap_or(c_int::MIN);
    if code == 0 || code == -libc::EAGAIN || code == -libc::EINTR {
        Ok(ReadOutcome::Again)
    } else {
        Err(code)
    }
}

struct SegmentWriter {
    root: PathBuf,
    sample_rate_hz: u32,
    channels: u16,
    bytes_per_frame: u64,
    segment_frames: u64,
    next_index: u64,
    active: Option<ActiveSegment>,
    records: Vec<AudioSegment>,
    progress: Arc<AudioProgress>,
}

struct ActiveSegment {
    index: u64,
    relative_path: String,
    file: File,
    start_sample: u64,
    data_bytes: u64,
}

impl SegmentWriter {
    #[cfg(test)]
    fn new(config: &RecorderConfig) -> Result<Self, AudioError> {
        Self::with_progress(config, Arc::new(AudioProgress::default()))
    }

    fn with_progress(
        config: &RecorderConfig,
        progress: Arc<AudioProgress>,
    ) -> Result<Self, AudioError> {
        let segment_frames = (config.sample_rate_hz as f64 * config.segment_seconds).round();
        if !segment_frames.is_finite() || segment_frames < 1.0 || segment_frames > u64::MAX as f64 {
            return Err(AudioError::new(
                "invalid_argument",
                "audio segment duration is invalid",
            ));
        }
        let bytes_per_frame = u64::from(config.channels) * BYTES_PER_SAMPLE;
        Ok(Self {
            root: config.session_root.clone(),
            sample_rate_hz: config.sample_rate_hz,
            channels: config.channels,
            bytes_per_frame,
            segment_frames: segment_frames as u64,
            next_index: 0,
            active: None,
            records: Vec::new(),
            progress,
        })
    }

    fn snapshot(&self) -> AudioRecordingSnapshot {
        let mut sample_count = 0_u64;
        let mut bytes_written = 0_u64;
        let mut segment_count = 0_u64;
        for record in &self.records {
            sample_count = sample_count.max(record.end_sample);
            bytes_written +=
                WAV_HEADER_BYTES + (record.end_sample - record.start_sample) * self.bytes_per_frame;
            segment_count += 1;
        }
        if let Some(active) = &self.active {
            if active.data_bytes > 0 {
                let active_samples = active.data_bytes / self.bytes_per_frame;
                sample_count = sample_count.max(active.start_sample + active_samples);
                bytes_written += WAV_HEADER_BYTES + active.data_bytes;
                segment_count += 1;
            }
        }
        AudioRecordingSnapshot {
            sample_count,
            bytes_written,
            segment_count,
        }
    }

    fn publish_snapshot(&self) {
        self.progress.publish(&self.snapshot());
    }

    fn write_frames(
        &mut self,
        data: &[u8],
        frames: u64,
        total_written_before: &mut u64,
    ) -> Result<(), AudioError> {
        let mut remaining = frames;
        let mut frame_offset = 0_u64;
        while remaining > 0 {
            if self.active.is_none() {
                self.open_segment(*total_written_before)?;
            }
            let segment_written = self
                .active
                .as_ref()
                .map(|segment| segment.data_bytes / self.bytes_per_frame)
                .unwrap_or(0);
            let capacity = self.segment_frames - segment_written;
            let selected = remaining.min(capacity);
            let byte_start = usize::try_from(frame_offset * self.bytes_per_frame)
                .map_err(|_| AudioError::new("audio_failed", "audio buffer offset overflow"))?;
            let byte_len = usize::try_from(selected * self.bytes_per_frame)
                .map_err(|_| AudioError::new("audio_failed", "audio buffer length overflow"))?;
            let byte_end = byte_start + byte_len;
            let active = self
                .active
                .as_mut()
                .ok_or_else(|| AudioError::new("invalid_state", "audio segment was not opened"))?;
            active
                .file
                .write_all(&data[byte_start..byte_end])
                .map_err(|error| AudioError::io("write_failed", "write audio segment", error))?;
            active.data_bytes += selected * self.bytes_per_frame;
            *total_written_before += selected;
            remaining -= selected;
            frame_offset += selected;
            if active.data_bytes / self.bytes_per_frame == self.segment_frames {
                self.finish_active()?;
            }
        }
        self.publish_snapshot();
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<AudioSegment>, AudioError> {
        if self.active.is_some() {
            self.finish_active()?;
        }
        Ok(self.records)
    }

    fn open_segment(&mut self, start_sample: u64) -> Result<(), AudioError> {
        let relative_path = format!("audio/audio_{:05}.wav", self.next_index);
        let absolute = self.root.join(&relative_path);
        if let Some(parent) = absolute.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| AudioError::io("write_failed", "create audio directory", error))?;
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&absolute)
            .map_err(|error| AudioError::io("write_failed", "open audio segment", error))?;
        write_wav_header(&mut file, self.sample_rate_hz, self.channels, 0)?;
        self.active = Some(ActiveSegment {
            index: self.next_index,
            relative_path,
            file,
            start_sample,
            data_bytes: 0,
        });
        self.next_index += 1;
        Ok(())
    }

    fn finish_active(&mut self) -> Result<(), AudioError> {
        let mut segment = self
            .active
            .take()
            .ok_or_else(|| AudioError::new("invalid_state", "audio segment is missing"))?;
        if segment.data_bytes == 0 {
            return Ok(());
        }
        segment
            .file
            .set_len(WAV_HEADER_BYTES + segment.data_bytes)
            .map_err(|error| {
                AudioError::io("write_failed", "truncate incomplete audio write", error)
            })?;
        write_wav_header(
            &mut segment.file,
            self.sample_rate_hz,
            self.channels,
            segment.data_bytes,
        )?;
        segment
            .file
            .sync_all()
            .map_err(|error| AudioError::io("write_failed", "sync audio segment", error))?;
        let end_sample = segment.start_sample + segment.data_bytes / self.bytes_per_frame;
        self.records.push(AudioSegment {
            index: segment.index,
            relative_path: segment.relative_path,
            start_sample: segment.start_sample,
            end_sample,
            start_time_seconds: segment.start_sample as f64 / f64::from(self.sample_rate_hz),
            end_time_seconds: end_sample as f64 / f64::from(self.sample_rate_hz),
        });
        self.publish_snapshot();
        Ok(())
    }
}

fn start_and_capture(
    config: RecorderConfig,
    stop: Arc<AtomicBool>,
    progress: Arc<AudioProgress>,
    ready: mpsc::SyncSender<Result<(), AudioError>>,
) -> Result<AudioRecordingResult, AudioError> {
    let mut pcm = match Pcm::open(&config) {
        Ok(pcm) => pcm,
        Err(error) => {
            let _ = ready.send(Err(error.clone()));
            return Err(error);
        }
    };
    let mut writer = match SegmentWriter::with_progress(&config, Arc::clone(&progress)) {
        Ok(writer) => writer,
        Err(error) => {
            let _ = ready.send(Err(error.clone()));
            return Err(error);
        }
    };
    let bytes_per_frame = u64::from(config.channels) * BYTES_PER_SAMPLE;
    let period_frames = DEFAULT_PERIOD_FRAMES;
    let block_bytes = usize::try_from(period_frames * bytes_per_frame)
        .map_err(|_| AudioError::new("invalid_argument", "audio period buffer is too large"))?;
    let (blocks_tx, blocks_rx) = mpsc::sync_channel::<(Vec<u8>, u64)>(QUEUE_BLOCKS);
    let (pool_tx, pool_rx) = mpsc::sync_channel(QUEUE_BLOCKS + 1);
    for _ in 0..=QUEUE_BLOCKS {
        pool_tx.send(vec![0_u8; block_bytes]).unwrap();
    }
    let writer_progress = Arc::clone(&progress);
    let disk = thread::spawn(move || {
        let mut written = 0;
        let mut failure = None;
        for (buffer, frames) in blocks_rx {
            writer_progress
                .queued_frames
                .fetch_sub(frames, Ordering::AcqRel);
            let started = std::time::Instant::now();
            if let Err(error) = writer.write_frames(
                &buffer[..(frames * bytes_per_frame) as usize],
                frames,
                &mut written,
            ) {
                writer_progress.fail(error.clone());
                failure = Some(error);
                break;
            }
            writer_progress
                .max_write_ns
                .fetch_max(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
            let _ = pool_tx.try_send(buffer);
        }
        // Even on capture or disk failure, seal the prefix whose writes completed.
        let finished = writer.finish();
        if let Some(error) = failure {
            return Err(error);
        }
        finished.map(|segments| (written, segments))
    });
    let started_monotonic_ns = monotonic_ns()?;
    let _ = ready.send(Ok(()));
    let mut sample_count = 0_u64;
    let mut anchors: Vec<[u64; 2]> = Vec::new();
    let mut last_anchor = None;
    let enqueue = |buffer, frames| {
        let queued = progress.queued_frames.fetch_add(frames, Ordering::AcqRel) + frames;
        progress
            .queue_peak_frames
            .fetch_max(queued, Ordering::Relaxed);
        blocks_tx.try_send((buffer, frames)).map_err(|_| {
            AudioError::new(
                "audio_failed",
                "audio writer queue exhausted or disconnected",
            )
        })
    };
    let captured = (|| {
        let mut buffer = pool_rx.try_recv().unwrap();
        let mut buffered_frames = 0;
        while !stop.load(Ordering::Acquire) {
            progress.check()?;
            let offset = (buffered_frames * bytes_per_frame) as usize;
            match pcm.readi(&mut buffer[offset..], period_frames - buffered_frames)? {
                ReadOutcome::Frames(frames) => {
                    sample_count += frames;
                    buffered_frames += frames;
                    let (position, timestamp) = pcm.clock_anchor(sample_count)?;
                    if timestamp == 0 {
                        return Err(AudioError::new("audio_failed", "missing ALSA timestamp"));
                    }
                    let anchor = [position, timestamp];
                    if anchors
                        .last()
                        .is_none_or(|last| position >= last[0] + u64::from(config.sample_rate_hz))
                    {
                        anchors.push(anchor);
                    }
                    last_anchor = Some(anchor);
                    // USB reads can contain only a few milliseconds. Fill a
                    // block so queue capacity measures PCM time, not USB polls.
                    if buffered_frames == period_frames {
                        enqueue(buffer, buffered_frames)?;
                        buffered_frames = 0;
                        buffer = pool_rx.try_recv().map_err(|_| {
                            AudioError::new("audio_failed", "audio writer buffer pool exhausted")
                        })?;
                    }
                }
                ReadOutcome::Again => thread::sleep(READ_IDLE_SLEEP),
            }
        }
        if buffered_frames > 0 {
            enqueue(buffer, buffered_frames)?;
        }
        Ok::<(), AudioError>(())
    })();
    if let Err(error) = &captured {
        progress.fail(error.clone());
    }
    let stopped_monotonic_ns = monotonic_ns()?;
    let closed = pcm.close();
    drop(blocks_tx);
    let disk_result = disk
        .join()
        .map_err(|_| AudioError::new("audio_failed", "audio writer panicked"))?;
    if let Some(anchor) = last_anchor {
        if anchors.last() != Some(&anchor) {
            anchors.push(anchor);
        }
    }
    let clock_result = capture_clock(
        &config,
        (pcm.period_frames, pcm.buffer_frames),
        sample_count,
        started_monotonic_ns,
        stopped_monotonic_ns,
        &anchors,
        &progress,
    );
    let failure = captured
        .as_ref()
        .err()
        .or(closed.as_ref().err())
        .or(disk_result.as_ref().err())
        .or(clock_result.as_ref().err());
    let diagnostic = serde_json::json!({
        "schema": "openaria.audio-capture-diagnostic.v1", "sample_count": sample_count,
        "anchors": anchors, "thread_started_monotonic_ns": started_monotonic_ns,
        "thread_stopped_monotonic_ns": stopped_monotonic_ns,
        "error": failure.map(|error| serde_json::json!({"code": error.code, "message": error.message})),
    });
    let diagnostic_path = config.session_root.join("audio/capture-clock.json");
    if let Ok(payload) = serde_json::to_vec(&diagnostic) {
        let _ = std::fs::write(diagnostic_path, payload);
    }
    captured?;
    closed?;
    let (written, segments) = disk_result?;
    if written != sample_count {
        return Err(AudioError::new(
            "audio_failed",
            "audio queue did not drain completely",
        ));
    }
    let capture_clock = clock_result?;
    if sample_count == 0 || segments.is_empty() {
        return Err(AudioError::new(
            "audio_empty",
            "native audio recorder did not capture samples",
        ));
    }
    Ok(AudioRecordingResult {
        capture_clock,
        device: config.device,
        sample_rate_hz: config.sample_rate_hz,
        channels: config.channels,
        sample_format: SAMPLE_FORMAT,
        codec: SAMPLE_CODEC,
        container: "wav",
        sample_count,
        started_monotonic_ns,
        stopped_monotonic_ns,
        segments,
    })
}

fn capture_clock(
    config: &RecorderConfig,
    hardware_frames: (u64, u64),
    samples: u64,
    started: u64,
    stopped: u64,
    anchors: &[[u64; 2]],
    progress: &AudioProgress,
) -> Result<serde_json::Value, AudioError> {
    if anchors.len() < 2
        || anchors
            .windows(2)
            .any(|pair| pair[1][0] <= pair[0][0] || pair[1][1] <= pair[0][1])
    {
        return Err(AudioError::new(
            "audio_failed",
            "audio clock anchors are missing or nonmonotonic",
        ));
    }
    let first = anchors[0];
    let last = anchors[anchors.len() - 1];
    let ns_per_sample = (last[1] - first[1]) as f64 / (last[0] - first[0]) as f64;
    let actual_rate = 1e9 / ns_per_sample;
    let residual = anchors
        .iter()
        .map(|a| ((a[1] - first[1]) as f64 - (a[0] - first[0]) as f64 * ns_per_sample).abs())
        .fold(0.0_f64, f64::max);
    if (actual_rate / f64::from(config.sample_rate_hz) - 1.0).abs() > 0.01
        || residual > 20_000_000.0
    {
        return Err(AudioError::new(
            "audio_failed",
            format!("audio clock discontinuity: rate={actual_rate}, residual_ns={residual}"),
        ));
    }
    let sample_start = first[1] as f64 - first[0] as f64 * ns_per_sample;
    let sample_end = sample_start + samples as f64 * ns_per_sample;
    if sample_start < started as f64 - 20_000_000.0 || sample_end > stopped as f64 + 20_000_000.0 {
        return Err(AudioError::new(
            "audio_failed",
            "sample clock outside capture lifetime",
        ));
    }
    Ok(serde_json::json!({
        "schema": "openaria.audio-clock.v1", "clock": "host_monotonic",
        "timestamp_source": "alsa_htimestamp_dma", "continuity": "verified",
        "device": config.device, "period_frames": hardware_frames.0, "buffer_frames": hardware_frames.1,
        "thread_started_monotonic_ns": started, "thread_stopped_monotonic_ns": stopped,
        "sample_start_monotonic_ns": sample_start.round() as u64,
        "sample_end_monotonic_ns": sample_end.round() as u64,
        "max_residual_ns": residual.ceil() as u64, "anchors": anchors,
        "queue_capacity_frames": QUEUE_BLOCKS as u64 * DEFAULT_PERIOD_FRAMES,
        "queue_peak_frames": progress.queue_peak_frames.load(Ordering::Relaxed),
        "max_write_ns": progress.max_write_ns.load(Ordering::Relaxed),
        "xrun_count": 0, "suspend_count": 0,
    }))
}

fn monotonic_ns() -> Result<u64, AudioError> {
    let mut timestamp = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: timestamp points to writable storage.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut timestamp) } != 0 {
        return Err(AudioError::io(
            "clock_failed",
            "clock_gettime",
            io::Error::last_os_error(),
        ));
    }
    Ok(timestamp.tv_sec as u64 * 1_000_000_000 + timestamp.tv_nsec as u64)
}

fn write_wav_header(
    file: &mut File,
    sample_rate_hz: u32,
    channels: u16,
    data_bytes: u64,
) -> Result<(), AudioError> {
    let data_size = u32::try_from(data_bytes)
        .map_err(|_| AudioError::new("audio_too_large", "WAV segment exceeds 4 GiB"))?;
    let riff_size = 36_u32
        .checked_add(data_size)
        .ok_or_else(|| AudioError::new("audio_too_large", "WAV RIFF size overflow"))?;
    let bits_per_sample = u16::try_from(BYTES_PER_SAMPLE * 8)
        .map_err(|_| AudioError::new("invalid_argument", "invalid sample width"))?;
    let block_align = channels
        .checked_mul(u16::try_from(BYTES_PER_SAMPLE).unwrap())
        .ok_or_else(|| AudioError::new("invalid_argument", "WAV block align overflow"))?;
    let byte_rate = sample_rate_hz
        .checked_mul(u32::from(block_align))
        .ok_or_else(|| AudioError::new("invalid_argument", "WAV byte rate overflow"))?;
    let mut header = Vec::with_capacity(44);
    header.extend_from_slice(b"RIFF");
    header.extend_from_slice(&riff_size.to_le_bytes());
    header.extend_from_slice(b"WAVE");
    header.extend_from_slice(b"fmt ");
    header.extend_from_slice(&16_u32.to_le_bytes());
    header.extend_from_slice(&1_u16.to_le_bytes());
    header.extend_from_slice(&channels.to_le_bytes());
    header.extend_from_slice(&sample_rate_hz.to_le_bytes());
    header.extend_from_slice(&byte_rate.to_le_bytes());
    header.extend_from_slice(&block_align.to_le_bytes());
    header.extend_from_slice(&bits_per_sample.to_le_bytes());
    header.extend_from_slice(b"data");
    header.extend_from_slice(&data_size.to_le_bytes());
    file.seek(SeekFrom::Start(0))
        .map_err(|error| AudioError::io("write_failed", "seek WAV header", error))?;
    file.write_all(&header)
        .map_err(|error| AudioError::io("write_failed", "write WAV header", error))?;
    if data_bytes == 0 {
        file.seek(SeekFrom::Start(44))
            .map_err(|error| AudioError::io("write_failed", "seek WAV data", error))?;
    }
    Ok(())
}

pub(crate) fn available() -> bool {
    Alsa::load().is_ok()
}

#[cfg(test)]
mod tests {
    use super::{SegmentWriter, write_wav_header};
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom};
    use std::path::PathBuf;

    #[test]
    fn xruns_and_suspend_never_resume_a_continuous_pcm_stream() {
        for code in [libc::EPIPE, libc::ESTRPIPE, libc::ENODEV, libc::EIO] {
            assert_eq!(super::classify_read_result((-code).into()), Err(-code));
        }
        for code in [0, -libc::EAGAIN, -libc::EINTR] {
            assert_eq!(
                super::classify_read_result(code.into()),
                Ok(super::ReadOutcome::Again)
            );
        }
        assert_eq!(
            super::classify_read_result(512),
            Ok(super::ReadOutcome::Frames(512))
        );
    }

    #[test]
    fn first_audio_failure_is_visible_before_stop_and_survives_cleanup_errors() {
        let progress = super::AudioProgress::default();
        assert!(progress.check().is_ok());
        progress.fail(super::AudioError::new("audio_failed", "xrun"));
        progress.fail(super::AudioError::new("write_failed", "cleanup"));
        assert_eq!(progress.check().unwrap_err().message, "xrun");
    }

    #[test]
    fn sample_clock_rejects_steps_without_rewriting_thread_times() {
        let config = super::RecorderConfig {
            session_root: PathBuf::new(),
            device: "hw:test".into(),
            sample_rate_hz: 10_000,
            channels: 2,
            segment_seconds: 30.0,
        };
        let progress = super::AudioProgress::default();
        let anchors = [
            [1024, 1_202_400_000],
            [11024, 2_202_400_000],
            [20000, 3_100_000_000],
        ];
        let clock = super::capture_clock(
            &config,
            (1024, 8192),
            20000,
            1_100_000_000,
            3_110_000_000,
            &anchors,
            &progress,
        )
        .unwrap();
        assert_eq!(clock["sample_start_monotonic_ns"], 1_100_000_000_u64);
        assert_eq!(clock["sample_end_monotonic_ns"], 3_100_000_000_u64);
        assert_eq!(clock["thread_stopped_monotonic_ns"], 3_110_000_000_u64);
        let mut broken = anchors;
        broken[1][1] += 50_000_000;
        assert!(
            super::capture_clock(
                &config,
                (1024, 8192),
                20000,
                1_100_000_000,
                3_110_000_000,
                &broken,
                &progress
            )
            .is_err()
        );
    }

    #[test]
    fn wav_header_records_pcm_shape() {
        let directory = tempfile_dir();
        let path = directory.join("audio.wav");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .read(true)
            .open(&path)
            .unwrap();
        write_wav_header(&mut file, 48_000, 2, 192_000).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut header = [0_u8; 44];
        file.read_exact(&mut header).unwrap();
        assert_eq!(&header[0..4], b"RIFF");
        assert_eq!(&header[8..12], b"WAVE");
        assert_eq!(u16::from_le_bytes(header[20..22].try_into().unwrap()), 1);
        assert_eq!(u16::from_le_bytes(header[22..24].try_into().unwrap()), 2);
        assert_eq!(
            u32::from_le_bytes(header[24..28].try_into().unwrap()),
            48_000
        );
        assert_eq!(u16::from_le_bytes(header[34..36].try_into().unwrap()), 16);
        assert_eq!(&header[36..40], b"data");
        assert_eq!(
            u32::from_le_bytes(header[40..44].try_into().unwrap()),
            192_000
        );
    }

    #[test]
    fn segment_writer_splits_on_sample_boundaries() {
        let root = tempfile_dir();
        let config = super::RecorderConfig {
            session_root: root.clone(),
            device: "hw:0,0".to_owned(),
            sample_rate_hz: 10,
            channels: 2,
            segment_seconds: 0.3,
        };
        let mut writer = SegmentWriter::new(&config).unwrap();
        let mut total = 0_u64;
        let payload = vec![1_u8; 5 * 2 * 2];
        writer.write_frames(&payload, 5, &mut total).unwrap();
        let records = writer.finish().unwrap();
        assert_eq!(total, 5);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].start_sample, 0);
        assert_eq!(records[0].end_sample, 3);
        assert_eq!(records[1].start_sample, 3);
        assert_eq!(records[1].end_sample, 5);
        assert!(root.join("audio/audio_00000.wav").is_file());
        assert!(root.join("audio/audio_00001.wav").is_file());
    }

    #[test]
    fn segment_writer_snapshot_counts_active_wav_bytes_without_closing() {
        let root = tempfile_dir();
        let config = super::RecorderConfig {
            session_root: root,
            device: "hw:0,0".to_owned(),
            sample_rate_hz: 10,
            channels: 2,
            segment_seconds: 1.0,
        };
        let mut writer = SegmentWriter::new(&config).unwrap();
        let mut total = 0_u64;
        let payload = vec![1_u8; 2 * 2 * 2];

        writer.write_frames(&payload, 2, &mut total).unwrap();
        let snapshot = writer.snapshot();

        assert_eq!(snapshot.sample_count, 2);
        assert_eq!(snapshot.bytes_written, 44 + 2 * 2 * 2);
        assert_eq!(snapshot.segment_count, 1);
    }

    fn tempfile_dir() -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!(
            "rp-ylx-audio-test-{}-{}",
            std::process::id(),
            monotonic_suffix()
        ));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    fn monotonic_suffix() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }
}
