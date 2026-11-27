use crate::audio::recorder::RecorderConfig;
use crate::audio::{AudioError, DEFAULT_PERIOD_FRAMES};
use libloading::Library;
use miro_ego_core::sys::dynlib;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uint, c_void};
use std::sync::{Arc, Mutex, OnceLock};

const SND_PCM_STREAM_CAPTURE: c_int = 1;
const SND_PCM_NONBLOCK: c_int = 0x0001;
const SND_PCM_ACCESS_RW_INTERLEAVED: c_int = 3;
const SND_PCM_FORMAT_S16_LE: c_int = 2;

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

// ALSA uses unsigned long, which is 32 or 64 bits depending on the target ABI.
#[allow(clippy::unnecessary_cast)]
fn pcm_frames_u64(frames: SndPcmUframes) -> u64 {
    frames as u64
}

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
type SndPcmWait = unsafe extern "C" fn(*mut SndPcm, c_int) -> c_int;
type SndPcmDrop = unsafe extern "C" fn(*mut SndPcm) -> c_int;
type SndStrError = unsafe extern "C" fn(c_int) -> *const c_char;
type SwMalloc = unsafe extern "C" fn(*mut *mut c_void) -> c_int;
type SwApply = unsafe extern "C" fn(*mut SndPcm, *mut c_void) -> c_int;
type SwSet = unsafe extern "C" fn(*mut SndPcm, *mut c_void, c_int) -> c_int;
type SwFree = unsafe extern "C" fn(*mut c_void);
type Htimestamp =
    unsafe extern "C" fn(*mut SndPcm, *mut SndPcmUframes, *mut libc::timespec) -> c_int;
type GetParams = unsafe extern "C" fn(*mut SndPcm, *mut SndPcmUframes, *mut SndPcmUframes) -> c_int;

struct Alsa {
    sw_malloc: SwMalloc,
    sw_current: SwApply,
    sw_apply: SwApply,
    sw_mode: SwSet,
    sw_type: SwSet,
    sw_get_type: unsafe extern "C" fn(*const c_void, *mut c_int) -> c_int,
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
    snd_pcm_wait: SndPcmWait,
    snd_pcm_drop: SndPcmDrop,
    snd_strerror: SndStrError,
}

unsafe impl Send for Alsa {}
unsafe impl Sync for Alsa {}

static ALSA_CACHE: OnceLock<Arc<Alsa>> = OnceLock::new();
/// Serialize `snd_pcm_open` / `snd_pcm_close` across threads because `alsa-lib`'s global
/// configuration tree (`snd_config`) is not thread-safe.
static ALSA_OPEN_CLOSE_LOCK: Mutex<()> = Mutex::new(());

impl Alsa {
    fn cached() -> Result<Arc<Self>, AudioError> {
        if let Some(alsa) = ALSA_CACHE.get() {
            return Ok(Arc::clone(alsa));
        }
        let loaded = Self::load()?;
        Ok(Arc::clone(ALSA_CACHE.get_or_init(|| loaded)))
    }

    fn load() -> Result<Arc<Self>, AudioError> {
        // SAFETY: loaded function pointers are kept valid by storing Library in Alsa.
        unsafe {
            let library = Library::new("libasound.so.2")
                .map_err(|error| AudioError::new("audio_unavailable", error.to_string()))?;
            Ok(Arc::new(Self {
                sw_malloc: load_alsa(&library, b"snd_pcm_sw_params_malloc\0")?,
                sw_current: load_alsa(&library, b"snd_pcm_sw_params_current\0")?,
                sw_apply: load_alsa(&library, b"snd_pcm_sw_params\0")?,
                sw_mode: load_alsa(&library, b"snd_pcm_sw_params_set_tstamp_mode\0")?,
                sw_type: load_alsa(&library, b"snd_pcm_sw_params_set_tstamp_type\0")?,
                sw_get_type: load_alsa(&library, b"snd_pcm_sw_params_get_tstamp_type\0")?,
                sw_free: load_alsa(&library, b"snd_pcm_sw_params_free\0")?,
                htimestamp: load_alsa(&library, b"snd_pcm_htimestamp\0")?,
                get_params: load_alsa(&library, b"snd_pcm_get_params\0")?,
                snd_pcm_open: load_alsa(&library, b"snd_pcm_open\0")?,
                snd_pcm_close: load_alsa(&library, b"snd_pcm_close\0")?,
                snd_pcm_nonblock: load_alsa(&library, b"snd_pcm_nonblock\0")?,
                snd_pcm_hw_params_malloc: load_alsa(&library, b"snd_pcm_hw_params_malloc\0")?,
                snd_pcm_hw_params_any: load_alsa(&library, b"snd_pcm_hw_params_any\0")?,
                snd_pcm_hw_params_set_access: load_alsa(
                    &library,
                    b"snd_pcm_hw_params_set_access\0",
                )?,
                snd_pcm_hw_params_set_format: load_alsa(
                    &library,
                    b"snd_pcm_hw_params_set_format\0",
                )?,
                snd_pcm_hw_params_set_channels: load_alsa(
                    &library,
                    b"snd_pcm_hw_params_set_channels\0",
                )?,
                snd_pcm_hw_params_set_rate_near: load_alsa(
                    &library,
                    b"snd_pcm_hw_params_set_rate_near\0",
                )?,
                snd_pcm_hw_params_set_period_size_near: load_alsa(
                    &library,
                    b"snd_pcm_hw_params_set_period_size_near\0",
                )?,
                snd_pcm_hw_params_set_buffer_size_near: load_alsa(
                    &library,
                    b"snd_pcm_hw_params_set_buffer_size_near\0",
                )?,
                snd_pcm_hw_params: load_alsa(&library, b"snd_pcm_hw_params\0")?,
                snd_pcm_hw_params_free: load_alsa(&library, b"snd_pcm_hw_params_free\0")?,
                snd_pcm_prepare: load_alsa(&library, b"snd_pcm_prepare\0")?,
                snd_pcm_readi: load_alsa(&library, b"snd_pcm_readi\0")?,
                snd_pcm_wait: load_alsa(&library, b"snd_pcm_wait\0")?,
                snd_pcm_drop: load_alsa(&library, b"snd_pcm_drop\0")?,
                snd_strerror: load_alsa(&library, b"snd_strerror\0")?,
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

unsafe fn load_alsa<T: Copy>(library: &Library, name: &[u8]) -> Result<T, AudioError> {
    // SAFETY: caller keeps the Library alive for at least as long as copied function pointers.
    unsafe { dynlib::load_symbol(library, name) }
        .map_err(|error| AudioError::new("audio_unavailable", error.to_string()))
}

pub struct Pcm {
    pub period_frames: u64,
    pub buffer_frames: u64,
    bytes_per_frame: u64,
    handle: *mut SndPcm,
    alsa: Arc<Alsa>,
}

unsafe impl Send for Pcm {}

impl Pcm {
    pub fn open(config: &RecorderConfig) -> Result<Self, AudioError> {
        let alsa = Alsa::cached()?;
        let device = CString::new(config.device.as_str())
            .map_err(|_| AudioError::new("invalid_argument", "audio device contains NUL"))?;
        let mut handle: *mut SndPcm = std::ptr::null_mut();
        let result = {
            let _open_guard = ALSA_OPEN_CLOSE_LOCK
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // SAFETY: handle points to writable storage, device is NUL-terminated, and
            // global alsa-lib config access is serialized by ALSA_OPEN_CLOSE_LOCK.
            unsafe {
                (alsa.snd_pcm_open)(
                    &mut handle,
                    device.as_ptr(),
                    SND_PCM_STREAM_CAPTURE,
                    SND_PCM_NONBLOCK,
                )
            }
        };
        if result < 0 {
            return Err(alsa.error("audio_unavailable", "snd_pcm_open", result));
        }
        let mut pcm = Self {
            handle,
            alsa,
            period_frames: 0,
            buffer_frames: 0,
            bytes_per_frame: 0,
        };
        if let Err(error) = pcm.configure(config) {
            let _ = pcm.close();
            return Err(error);
        }
        Ok(pcm)
    }

    fn configure(&mut self, config: &RecorderConfig) -> Result<(), AudioError> {
        let bytes_per_frame = u64::from(config.channels)
            .checked_mul(crate::audio::BYTES_PER_SAMPLE)
            .filter(|&n| n > 0)
            .ok_or_else(|| AudioError::new("invalid_argument", "audio frame size is invalid"))?;
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
        direction = 0;
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
        // Keep one second of hardware-side PCM available across scheduling and
        // storage stalls. The old eight-period ring held only 171 ms at 48 kHz;
        // the downstream writer queue cannot recover samples already lost here.
        let mut buffer = SndPcmUframes::try_from(
            u64::from(config.sample_rate_hz).max(DEFAULT_PERIOD_FRAMES * 8),
        )
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
        self.period_frames = pcm_frames_u64(period);
        self.buffer_frames = pcm_frames_u64(buffer);
        if self.period_frames == 0 || self.period_frames > self.buffer_frames {
            return Err(AudioError::new(
                "unsupported_audio_mode",
                format!(
                    "ALSA selected invalid geometry: period_frames={} buffer_frames={}",
                    self.period_frames, self.buffer_frames
                ),
            ));
        }
        self.bytes_per_frame = bytes_per_frame;
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
            self.check("audio_failed", "timestamp_monotonic_raw", unsafe {
                (self.alsa.sw_type)(self.handle, sw, 2)
            })?;
            self.check("audio_failed", "sw_params", unsafe {
                (self.alsa.sw_apply)(self.handle, sw)
            })?;
            self.check("audio_failed", "sw_params_readback", unsafe {
                (self.alsa.sw_current)(self.handle, sw)
            })?;
            let mut timestamp_type = -1;
            self.check("audio_failed", "timestamp_type_readback", unsafe {
                (self.alsa.sw_get_type)(sw, &mut timestamp_type)
            })?;
            if timestamp_type != 2 {
                return Err(AudioError::new(
                    "audio_failed",
                    "ALSA did not retain MONOTONIC_RAW timestamps",
                ));
            }
            Ok(())
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

    pub fn readi(&mut self, buffer: &mut [u8], frames: u64) -> Result<ReadOutcome, AudioError> {
        if self.handle.is_null() {
            return Err(AudioError::new(
                "invalid_state",
                "ALSA PCM handle is closed",
            ));
        }
        if self.bytes_per_frame == 0 {
            return Err(AudioError::new(
                "invalid_state",
                "ALSA PCM frame size is unconfigured",
            ));
        }
        if frames
            .checked_mul(self.bytes_per_frame)
            .and_then(|n| usize::try_from(n).ok())
            .is_none_or(|n| n > buffer.len())
        {
            return Err(AudioError::new(
                "invalid_argument",
                "audio read buffer is smaller than requested frames",
            ));
        }
        let requested = SndPcmUframes::try_from(frames)
            .map_err(|_| AudioError::new("invalid_argument", "audio read size is too large"))?;
        let result = unsafe {
            (self.alsa.snd_pcm_readi)(self.handle, buffer.as_mut_ptr().cast::<c_void>(), requested)
        };
        let outcome = classify_read_result(result).map_err(|code| {
            self.alsa.error(
                "audio_failed",
                "snd_pcm_readi: capture continuity lost",
                code,
            )
        })?;
        if let ReadOutcome::Frames(read) = outcome {
            if read > frames {
                return Err(AudioError::new(
                    "audio_failed",
                    format!("snd_pcm_readi returned {read} frames, exceeding {frames}"),
                ));
            }
        }
        Ok(outcome)
    }

    pub fn wait(&mut self, timeout_ms: u32) -> Result<bool, AudioError> {
        if self.handle.is_null() {
            return Err(AudioError::new(
                "invalid_state",
                "ALSA PCM handle is closed",
            ));
        }
        let ms = c_int::try_from(timeout_ms).unwrap_or(c_int::MAX).max(1);
        // SAFETY: `self.handle` is a valid open PCM handle owned by `self`.
        let result = unsafe { (self.alsa.snd_pcm_wait)(self.handle, ms) };
        if result > 0 {
            return Ok(true);
        }
        if result == 0 || result == -libc::EINTR || result == -libc::EAGAIN {
            return Ok(false);
        }
        Err(self.alsa.error(
            "audio_failed",
            "snd_pcm_wait: capture continuity lost",
            result,
        ))
    }

    pub fn clock_anchor(&self, read_frames: u64) -> Result<(u64, u64), AudioError> {
        if self.handle.is_null() {
            return Err(AudioError::new(
                "invalid_state",
                "ALSA PCM handle is closed",
            ));
        }
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
            || pcm_frames_u64(available) > self.buffer_frames
        {
            return Err(AudioError::new(
                "audio_failed",
                "invalid ALSA clock or overrun",
            ));
        }
        let position = read_frames
            .checked_add(pcm_frames_u64(available))
            .ok_or_else(|| AudioError::new("audio_failed", "ALSA sample position overflow"))?;
        let timestamp_ns = (timestamp.tv_sec as u64)
            .checked_mul(1_000_000_000)
            .and_then(|sec_ns| sec_ns.checked_add(timestamp.tv_nsec as u64))
            .ok_or_else(|| AudioError::new("audio_failed", "ALSA timestamp overflow"))?;
        Ok((position, timestamp_ns))
    }

    fn check(&self, code: &'static str, context: &str, result: c_int) -> Result<(), AudioError> {
        if result < 0 {
            Err(self.alsa.error(code, context, result))
        } else {
            Ok(())
        }
    }

    pub fn close(&mut self) -> Result<(), AudioError> {
        if self.handle.is_null() {
            return Ok(());
        }
        let handle = self.handle;
        self.handle = std::ptr::null_mut();
        let drop_result = unsafe { (self.alsa.snd_pcm_drop)(handle) };
        let close_result = {
            let _close_guard = ALSA_OPEN_CLOSE_LOCK
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            unsafe { (self.alsa.snd_pcm_close)(handle) }
        };
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
pub enum ReadOutcome {
    Frames(u64),
    Again,
}

pub fn classify_read_result(result: SndPcmSframes) -> Result<ReadOutcome, c_int> {
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

pub fn available() -> bool {
    Alsa::cached().is_ok()
}

#[cfg(test)]
mod tests {
    use super::{ReadOutcome, classify_read_result};

    #[test]
    fn xruns_and_suspend_never_resume_a_continuous_pcm_stream() {
        for code in [libc::EPIPE, libc::ESTRPIPE, libc::ENODEV, libc::EIO] {
            assert_eq!(classify_read_result((-code).into()), Err(-code));
        }
        for code in [0, -libc::EAGAIN, -libc::EINTR] {
            assert_eq!(classify_read_result(code.into()), Ok(ReadOutcome::Again));
        }
        assert_eq!(classify_read_result(512), Ok(ReadOutcome::Frames(512)));
    }
}
