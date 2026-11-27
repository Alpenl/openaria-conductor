#[cfg(not(target_pointer_width = "64"))]
compile_error!("V4L2 ioctl ABI layouts require 64-bit pointer width");

use miro_ego_core::config::ExposureSetting;
use miro_ego_core::sys::clock;
use std::ffi::{CString, c_ulong, c_void};
use std::io;
use std::os::fd::RawFd;
use std::ptr::NonNull;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const MAX_CONSECUTIVE_BAD_FRAMES: u32 = 30;

#[cfg(target_pointer_width = "64")]
mod abi {
    use std::ffi::c_ulong;

    pub(super) const BUF_TYPE_CAPTURE: u32 = 1;
    pub(super) const MEMORY_MMAP: u32 = 1;
    pub(super) const CAP_VIDEO_CAPTURE: u32 = 0x0000_0001;
    pub(super) const CAP_STREAMING: u32 = 0x0400_0000;
    pub(super) const CAP_DEVICE_CAPS: u32 = 0x8000_0000;
    pub(super) const BUF_FLAG_ERROR: u32 = 0x0000_0040;
    pub(super) const BUF_FLAG_TIMESTAMP_MONOTONIC: u32 = 0x0000_2000;
    pub(super) const BUF_FLAG_TIMESTAMP_MASK: u32 = 0x0000_e000;
    pub(super) const IOC_READ: u32 = 2;
    pub(super) const IOC_WRITE: u32 = 1;
    pub(super) const FOURCC_MJPG: u32 = u32::from_le_bytes(*b"MJPG");
    pub(super) const FOURCC_JPEG: u32 = u32::from_le_bytes(*b"JPEG");

    pub(super) const fn ioc(direction: u32, number: u32, size: u32) -> c_ulong {
        ((direction << 30) | (size << 16) | ((b'V' as u32) << 8) | number) as c_ulong
    }

    pub(super) const QUERYCAP: c_ulong = ioc(IOC_READ, 0, 104);
    pub(super) const SET_FORMAT: c_ulong = ioc(IOC_READ | IOC_WRITE, 5, 208);
    pub(super) const GET_PARM: c_ulong = ioc(IOC_READ | IOC_WRITE, 21, 204);
    pub(super) const SET_PARM: c_ulong = ioc(IOC_READ | IOC_WRITE, 22, 204);
    pub(super) const REQUEST_BUFFERS: c_ulong = ioc(IOC_READ | IOC_WRITE, 8, 20);
    pub(super) const QUERY_BUFFER: c_ulong = ioc(IOC_READ | IOC_WRITE, 9, 88);
    pub(super) const QUEUE_BUFFER: c_ulong = ioc(IOC_READ | IOC_WRITE, 15, 88);
    pub(super) const DEQUEUE_BUFFER: c_ulong = ioc(IOC_READ | IOC_WRITE, 17, 88);
    pub(super) const STREAM_ON: c_ulong = ioc(IOC_WRITE, 18, 4);
    pub(super) const STREAM_OFF: c_ulong = ioc(IOC_WRITE, 19, 4);
    pub(super) const GET_CONTROL: c_ulong = ioc(IOC_READ | IOC_WRITE, 27, 8);
    pub(super) const SET_CONTROL: c_ulong = ioc(IOC_READ | IOC_WRITE, 28, 8);
    pub(super) const QUERY_CONTROL: c_ulong = ioc(IOC_READ | IOC_WRITE, 36, 68);
    pub(super) const CTRL_TYPE_INTEGER: u32 = 1;
    pub(super) const CTRL_TYPE_BOOLEAN: u32 = 2;
    pub(super) const CTRL_TYPE_MENU: u32 = 3;
    pub(super) const CTRL_FLAG_DISABLED: u32 = 0x0000_0001;
    pub(super) const CTRL_CLASS_CAMERA: u32 = 0x009a_0000;
    pub(super) const CID_CAMERA_CLASS_BASE: u32 = CTRL_CLASS_CAMERA | 0x900;
    pub(super) const CID_EXPOSURE_AUTO: u32 = CID_CAMERA_CLASS_BASE + 1;
    pub(super) const CID_EXPOSURE_ABSOLUTE: u32 = CID_CAMERA_CLASS_BASE + 2;
    pub(super) const CID_FOCUS_ABSOLUTE: u32 = CID_CAMERA_CLASS_BASE + 10;
    pub(super) const CID_FOCUS_AUTO: u32 = CID_CAMERA_CLASS_BASE + 12;
}

use abi::*;

pub fn is_monotonic_timestamp(flags: u32) -> bool {
    (flags & BUF_FLAG_TIMESTAMP_MASK) == BUF_FLAG_TIMESTAMP_MONOTONIC
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureError {
    pub code: &'static str,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FocusStatus {
    pub value: i32,
    pub minimum: i32,
    pub maximum: i32,
    pub step: i32,
    pub default_value: i32,
    pub auto_supported: bool,
    pub auto_enabled: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExposureConfig {
    pub auto_exposure: i32,
    pub exposure_time_absolute: Option<i32>,
}

impl CaptureError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    fn io(code: &'static str, operation: &str, error: io::Error) -> Self {
        Self::new(code, format!("{operation}: {error}"))
    }
}

#[cfg(test)]
#[derive(Debug)]
pub struct RawFrame {
    pub source_sequence: u64,
    pub host_monotonic_ns: u64,
    pub payload: Vec<u8>,
}

trait CaptureBuffer: Send {
    fn len(&self) -> usize;
    fn bytes(&self, length: usize) -> Result<&[u8], CaptureError>;
}

trait V4l2Io: Send + Sync {
    fn open(&self, path: &CString) -> io::Result<RawFd>;
    fn close(&self, fd: RawFd) -> io::Result<()>;
    fn ioctl(&self, fd: RawFd, request: c_ulong, payload: &mut [u8]) -> io::Result<()>;
    fn mmap(&self, fd: RawFd, length: usize, offset: i64) -> io::Result<Box<dyn CaptureBuffer>>;
    fn wait_readable(&self, fd: RawFd, timeout: Duration) -> io::Result<bool>;
    fn monotonic_ns(&self) -> io::Result<u64>;
}

struct MmapBuffer {
    pointer: NonNull<u8>,
    length: usize,
}

// A mapping is only accessed while Capture owns it and serializes reads.
unsafe impl Send for MmapBuffer {}

impl CaptureBuffer for MmapBuffer {
    fn len(&self) -> usize {
        self.length
    }

    fn bytes(&self, length: usize) -> Result<&[u8], CaptureError> {
        if length > self.length {
            return Err(CaptureError::new(
                "bad_frame",
                "mmap payload exceeds buffer",
            ));
        }
        // SAFETY: The V4L2 mapping is readable for its configured length until Drop.
        Ok(unsafe { std::slice::from_raw_parts(self.pointer.as_ptr(), length) })
    }
}

impl Drop for MmapBuffer {
    fn drop(&mut self) {
        // SAFETY: pointer/length came from one successful mmap and are unmapped once.
        unsafe { libc::munmap(self.pointer.as_ptr().cast(), self.length) };
    }
}

struct SystemIo;

impl V4l2Io for SystemIo {
    fn open(&self, path: &CString) -> io::Result<RawFd> {
        loop {
            // SAFETY: CString guarantees a NUL-terminated path.
            let fd = unsafe {
                libc::open(
                    path.as_ptr(),
                    libc::O_RDWR | libc::O_NONBLOCK | libc::O_CLOEXEC,
                )
            };
            if fd >= 0 {
                return Ok(fd);
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }

    fn close(&self, fd: RawFd) -> io::Result<()> {
        // SAFETY: fd is owned by Capture and closed once.
        if unsafe { libc::close(fd) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    fn ioctl(&self, fd: RawFd, request: c_ulong, payload: &mut [u8]) -> io::Result<()> {
        loop {
            // SAFETY: request sizes match payload allocations and fd is live.
            if unsafe { libc::ioctl(fd, request, payload.as_mut_ptr().cast::<c_void>()) } == 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }

    fn mmap(&self, fd: RawFd, length: usize, offset: i64) -> io::Result<Box<dyn CaptureBuffer>> {
        // SAFETY: Arguments are supplied by VIDIOC_QUERYBUF for the live fd.
        let pointer = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                length,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                offset,
            )
        };
        if pointer == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let Some(pointer) = NonNull::new(pointer.cast()) else {
            return Err(io::Error::other("mmap returned null pointer"));
        };
        Ok(Box::new(MmapBuffer { pointer, length }))
    }

    fn wait_readable(&self, fd: RawFd, timeout: Duration) -> io::Result<bool> {
        let deadline = Instant::now() + timeout;
        let mut descriptor = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let timeout_ms = if remaining.is_zero() {
                0
            } else {
                i32::try_from(remaining.as_millis())
                    .unwrap_or(i32::MAX)
                    .max(1)
            };
            descriptor.revents = 0;
            // SAFETY: descriptor points to one writable pollfd.
            let result = unsafe { libc::poll(&mut descriptor, 1, timeout_ms) };
            if result < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    if Instant::now() >= deadline {
                        return Ok(false);
                    }
                    continue;
                }
                return Err(error);
            }
            if descriptor.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    format!("V4L2 poll failed with revents {:#x}", descriptor.revents),
                ));
            }
            return Ok(result > 0 && descriptor.revents & libc::POLLIN != 0);
        }
    }

    fn monotonic_ns(&self) -> io::Result<u64> {
        clock::monotonic_ns()
    }
}

pub struct ControlDevice {
    io: Arc<dyn V4l2Io>,
    fd: Option<RawFd>,
    close_on_drop: bool,
}

impl ControlDevice {
    pub fn open(device: &str, open_error_code: &'static str) -> Result<Self, CaptureError> {
        Self::open_with_io(Arc::new(SystemIo), device, open_error_code)
    }

    fn open_with_io(
        io: Arc<dyn V4l2Io>,
        device: &str,
        open_error_code: &'static str,
    ) -> Result<Self, CaptureError> {
        let path = CString::new(device).map_err(|_| {
            CaptureError::new("camera_focus_invalid_device", "device path contains NUL")
        })?;
        let fd = io
            .open(&path)
            .map_err(|error| CaptureError::io(open_error_code, "open", error))?;
        Ok(Self {
            io,
            fd: Some(fd),
            close_on_drop: true,
        })
    }

    fn borrowed(io: Arc<dyn V4l2Io>, fd: RawFd) -> Self {
        Self {
            io,
            fd: Some(fd),
            close_on_drop: false,
        }
    }

    fn fd(&self) -> Result<RawFd, CaptureError> {
        self.fd
            .ok_or_else(|| CaptureError::new("invalid_state", "V4L2 control device is closed"))
    }

    pub fn close(&mut self) {
        if let Some(fd) = self.fd.take() {
            if self.close_on_drop {
                let _ = self.io.close(fd);
            }
        }
    }

    fn query_control(
        &self,
        id: u32,
        expected_type: u32,
    ) -> Result<Option<QueryControl>, CaptureError> {
        self.query_control_types(id, &[expected_type], "camera_focus_query_failed")
    }

    fn query_control_types(
        &self,
        id: u32,
        allowed_types: &[u32],
        error_code: &'static str,
    ) -> Result<Option<QueryControl>, CaptureError> {
        let mut payload = [0; 68];
        write_u32(&mut payload, 0, id);
        match self.io.ioctl(self.fd()?, QUERY_CONTROL, &mut payload) {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.raw_os_error(),
                    Some(libc::EINVAL) | Some(libc::ENOTTY)
                ) =>
            {
                return Ok(None);
            }
            Err(error) => {
                return Err(CaptureError::io(error_code, "VIDIOC_QUERYCTRL", error));
            }
        }
        let control_type = read_u32(&payload, 4);
        let flags = read_u32(&payload, 56);
        if flags & CTRL_FLAG_DISABLED != 0 || !allowed_types.contains(&control_type) {
            return Ok(None);
        }
        Ok(Some(QueryControl {
            minimum: read_i32(&payload, 40),
            maximum: read_i32(&payload, 44),
            step: read_i32(&payload, 48).max(1),
            default_value: read_i32(&payload, 52),
        }))
    }

    fn get_control(&self, id: u32, error_code: &'static str) -> Result<i32, CaptureError> {
        let mut payload = [0; 8];
        write_u32(&mut payload, 0, id);
        self.io
            .ioctl(self.fd()?, GET_CONTROL, &mut payload)
            .map_err(|error| CaptureError::io(error_code, "VIDIOC_G_CTRL", error))?;
        Ok(read_i32(&payload, 4))
    }

    fn set_control(
        &self,
        id: u32,
        value: i32,
        error_code: &'static str,
    ) -> Result<(), CaptureError> {
        let mut payload = [0; 8];
        write_u32(&mut payload, 0, id);
        write_i32(&mut payload, 4, value);
        self.io
            .ioctl(self.fd()?, SET_CONTROL, &mut payload)
            .map_err(|error| CaptureError::io(error_code, "VIDIOC_S_CTRL", error))
    }

    pub fn focus_status(&self) -> Result<Option<FocusStatus>, CaptureError> {
        let Some(focus) = self.query_control(CID_FOCUS_ABSOLUTE, CTRL_TYPE_INTEGER)? else {
            return Ok(None);
        };
        let value = self.get_control(CID_FOCUS_ABSOLUTE, "camera_focus_get_failed")?;
        let auto = self.query_control(CID_FOCUS_AUTO, CTRL_TYPE_BOOLEAN)?;
        let auto_enabled = if auto.is_some() {
            Some(self.get_control(CID_FOCUS_AUTO, "camera_focus_get_failed")? != 0)
        } else {
            None
        };
        Ok(Some(FocusStatus {
            value,
            minimum: focus.minimum,
            maximum: focus.maximum,
            step: focus.step,
            default_value: focus.default_value,
            auto_supported: auto.is_some(),
            auto_enabled,
        }))
    }

    pub fn set_focus(
        &self,
        value: Option<i32>,
        auto_enabled: Option<bool>,
    ) -> Result<FocusStatus, CaptureError> {
        if value.is_none() && auto_enabled.is_none() {
            return Err(CaptureError::new(
                "invalid_camera_focus",
                "focus value or auto_enabled must be specified",
            ));
        }
        let Some(focus) = self.query_control(CID_FOCUS_ABSOLUTE, CTRL_TYPE_INTEGER)? else {
            return Err(CaptureError::new(
                "camera_focus_unsupported",
                "V4L2 device does not expose focus_absolute",
            ));
        };
        if let Some(next) = value {
            if next < focus.minimum
                || next > focus.maximum
                || (i64::from(next) - i64::from(focus.minimum)) % i64::from(focus.step) != 0
            {
                return Err(CaptureError::new(
                    "invalid_camera_focus",
                    format!(
                        "focus value must be between {} and {} with step {}",
                        focus.minimum, focus.maximum, focus.step
                    ),
                ));
            }
        }
        let auto = self.query_control(CID_FOCUS_AUTO, CTRL_TYPE_BOOLEAN)?;
        if auto_enabled.is_some() && auto.is_none() {
            return Err(CaptureError::new(
                "camera_focus_auto_unsupported",
                "V4L2 device does not expose focus_auto",
            ));
        }
        match auto_enabled {
            Some(true) => {
                // Enable autofocus without writing CID_FOCUS_ABSOLUTE or resetting CID_FOCUS_AUTO.
                self.set_control(CID_FOCUS_AUTO, 1, "camera_focus_set_failed")?;
            }
            Some(false) => {
                // Disable autofocus first, then apply manual focus value if provided.
                self.set_control(CID_FOCUS_AUTO, 0, "camera_focus_set_failed")?;
                if let Some(next) = value {
                    self.set_control(CID_FOCUS_ABSOLUTE, next, "camera_focus_set_failed")?;
                }
            }
            None => {
                if let Some(next) = value {
                    if auto.is_some() {
                        self.set_control(CID_FOCUS_AUTO, 0, "camera_focus_set_failed")?;
                    }
                    self.set_control(CID_FOCUS_ABSOLUTE, next, "camera_focus_set_failed")?;
                }
            }
        }
        self.focus_status()?.ok_or_else(|| {
            CaptureError::new("camera_focus_unsupported", "focus control disappeared")
        })
    }

    pub fn set_exposure(&self, exposure: &ExposureSetting) -> Result<ExposureConfig, CaptureError> {
        match exposure {
            ExposureSetting::Auto(mode) => {
                if mode != "auto" {
                    return Err(CaptureError::new(
                        "invalid_argument",
                        "auto exposure mode must be \"auto\"",
                    ));
                }
                let auto_ctrl = self.query_control_types(
                    CID_EXPOSURE_AUTO,
                    &[CTRL_TYPE_MENU, CTRL_TYPE_INTEGER],
                    "exposure_query_failed",
                )?;
                let mut candidates = Vec::with_capacity(4);
                if let Some(ctrl) = auto_ctrl {
                    if (ctrl.minimum..=ctrl.maximum).contains(&3) {
                        candidates.push(3);
                    }
                    if ctrl.default_value != 1
                        && (ctrl.minimum..=ctrl.maximum).contains(&ctrl.default_value)
                        && !candidates.contains(&ctrl.default_value)
                    {
                        candidates.push(ctrl.default_value);
                    }
                    for fallback in [0, 2] {
                        if (ctrl.minimum..=ctrl.maximum).contains(&fallback)
                            && !candidates.contains(&fallback)
                        {
                            candidates.push(fallback);
                        }
                    }
                    if candidates.is_empty() {
                        candidates.push(3);
                    }
                } else {
                    candidates.push(3);
                    candidates.push(0);
                }
                let mut last_error = None;
                for (idx, &candidate) in candidates.iter().enumerate() {
                    let mut payload = [0; 8];
                    write_u32(&mut payload, 0, CID_EXPOSURE_AUTO);
                    write_i32(&mut payload, 4, candidate);
                    match self.io.ioctl(self.fd()?, SET_CONTROL, &mut payload) {
                        Ok(()) => {
                            let actual =
                                self.get_control(CID_EXPOSURE_AUTO, "exposure_get_failed")?;
                            if actual != candidate {
                                return Err(CaptureError::new(
                                    "exposure_readback_failed",
                                    format!("auto_exposure requested={candidate}, actual={actual}"),
                                ));
                            }
                            return Ok(ExposureConfig {
                                auto_exposure: actual,
                                exposure_time_absolute: None,
                            });
                        }
                        Err(error)
                            if idx + 1 < candidates.len()
                                && matches!(
                                    error.raw_os_error(),
                                    Some(libc::EINVAL) | Some(libc::ERANGE)
                                ) =>
                        {
                            last_error = Some(CaptureError::io(
                                "exposure_set_failed",
                                "VIDIOC_S_CTRL",
                                error,
                            ));
                        }
                        Err(error) => {
                            return Err(CaptureError::io(
                                "exposure_set_failed",
                                "VIDIOC_S_CTRL",
                                error,
                            ));
                        }
                    }
                }
                Err(last_error.unwrap_or_else(|| {
                    CaptureError::new("exposure_set_failed", "no supported auto exposure mode")
                }))
            }
            ExposureSetting::Manual(raw_val) => {
                let requested = i32::try_from(*raw_val)
                    .ok()
                    .filter(|&v| v >= 1)
                    .ok_or_else(|| {
                        CaptureError::new(
                            "invalid_argument",
                            "exposure_time_absolute must be a positive integer",
                        )
                    })?;
                let abs_ctrl = self.query_control_types(
                    CID_EXPOSURE_ABSOLUTE,
                    &[CTRL_TYPE_INTEGER],
                    "exposure_query_failed",
                )?;
                let (target, tolerance) = if let Some(ctrl) = abs_ctrl
                    && ctrl.minimum <= ctrl.maximum
                {
                    let clamped = requested.clamp(ctrl.minimum, ctrl.maximum);
                    let step = i64::from(ctrl.step.max(1));
                    let aligned = i64::from(ctrl.minimum)
                        + ((i64::from(clamped) - i64::from(ctrl.minimum)) / step) * step;
                    let target_i32 = i32::try_from(aligned).unwrap_or(clamped);
                    let quant_tolerance = ctrl.step.saturating_mul(2).max(2);
                    (target_i32, quant_tolerance)
                } else {
                    (requested, 2)
                };
                self.set_and_verify_control("auto_exposure", CID_EXPOSURE_AUTO, 1)?;
                self.set_control(CID_EXPOSURE_ABSOLUTE, target, "exposure_set_failed")?;
                let actual = self.get_control(CID_EXPOSURE_ABSOLUTE, "exposure_get_failed")?;
                if (i64::from(actual) - i64::from(target)).abs() > i64::from(tolerance) {
                    return Err(CaptureError::new(
                        "exposure_readback_failed",
                        format!(
                            "exposure_time_absolute target={target} (requested={requested}), actual={actual}, tolerance={tolerance}"
                        ),
                    ));
                }
                Ok(ExposureConfig {
                    auto_exposure: 1,
                    exposure_time_absolute: Some(actual),
                })
            }
        }
    }

    fn configure_exposure(
        &self,
        absolute: Option<i32>,
        auto: bool,
    ) -> Result<ExposureConfig, CaptureError> {
        if auto {
            if absolute.is_some() {
                return Err(CaptureError::new(
                    "invalid_argument",
                    "auto exposure cannot be combined with exposure_time_absolute",
                ));
            }
            return self.set_exposure(&ExposureSetting::Auto("auto".to_owned()));
        }
        let Some(value) = absolute else {
            return Err(CaptureError::new(
                "invalid_argument",
                "manual exposure requires exposure_time_absolute in 1..=100",
            ));
        };
        if !(1..=100).contains(&value) {
            return Err(CaptureError::new(
                "invalid_argument",
                "exposure_time_absolute must be in 1..=100",
            ));
        }
        self.set_exposure(&ExposureSetting::Manual(value as u32))
    }

    fn set_and_verify_control(&self, name: &str, id: u32, value: i32) -> Result<(), CaptureError> {
        self.set_control(id, value, "exposure_set_failed")?;
        let actual = self.get_control(id, "exposure_get_failed")?;
        if actual != value {
            return Err(CaptureError::new(
                "exposure_readback_failed",
                format!("{name} requested={value}, actual={actual}"),
            ));
        }
        Ok(())
    }
}

impl Drop for ControlDevice {
    fn drop(&mut self) {
        self.close();
    }
}

#[derive(Debug, Clone, Copy)]
struct QueryControl {
    minimum: i32,
    maximum: i32,
    step: i32,
    default_value: i32,
}

fn write_u32(payload: &mut [u8], offset: usize, value: u32) {
    payload[offset..offset + 4].copy_from_slice(&value.to_ne_bytes());
}

fn write_i32(payload: &mut [u8], offset: usize, value: i32) {
    payload[offset..offset + 4].copy_from_slice(&value.to_ne_bytes());
}

fn read_u32(payload: &[u8], offset: usize) -> u32 {
    u32::from_ne_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ])
}

fn read_i32(payload: &[u8], offset: usize) -> i32 {
    i32::from_ne_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ])
}

fn read_i64(payload: &[u8], offset: usize) -> i64 {
    i64::from_ne_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
        payload[offset + 4],
        payload[offset + 5],
        payload[offset + 6],
        payload[offset + 7],
    ])
}

pub fn focus_status(device: &str) -> Result<Option<FocusStatus>, CaptureError> {
    let control = ControlDevice::open(device, "camera_focus_open_failed")?;
    control.focus_status()
}

pub fn set_focus(
    device: &str,
    value: Option<i32>,
    auto_enabled: Option<bool>,
) -> Result<FocusStatus, CaptureError> {
    let control = ControlDevice::open(device, "camera_focus_open_failed")?;
    control.set_focus(value, auto_enabled)
}

pub fn set_exposure(
    device: &str,
    exposure: &ExposureSetting,
) -> Result<ExposureConfig, CaptureError> {
    let control = ControlDevice::open(device, "open_failed")?;
    control.set_exposure(exposure)
}

pub fn configure_exposure(
    device: &str,
    absolute: Option<i32>,
    auto: bool,
) -> Result<ExposureConfig, CaptureError> {
    let control = ControlDevice::open(device, "open_failed")?;
    control.configure_exposure(absolute, auto)
}

fn queue_payload(index: u32) -> [u8; 88] {
    let mut payload = [0; 88];
    write_u32(&mut payload, 0, index);
    write_u32(&mut payload, 4, BUF_TYPE_CAPTURE);
    write_u32(&mut payload, 60, MEMORY_MMAP);
    payload
}

pub struct Capture {
    io: Arc<dyn V4l2Io>,
    fd: Option<RawFd>,
    buffers: Vec<Box<dyn CaptureBuffer>>,
    streaming: bool,
    last_raw_sequence: Option<u32>,
    sequence_epoch: u64,
    last_timestamp_ns: Option<u64>,
    consecutive_bad_frames: u32,
    corrupt_frames_dropped: u64,
    pub last_buffer_flags: u32,
    pub last_dequeue_monotonic_ns: u64,
}

impl Capture {
    pub fn open(
        device: &str,
        width: u32,
        height: u32,
        fps: u32,
        encoding: &str,
        buffer_count: u32,
    ) -> Result<Self, CaptureError> {
        Self::with_io(
            Arc::new(SystemIo),
            device,
            width,
            height,
            fps,
            encoding,
            buffer_count,
        )
    }

    fn with_io(
        io: Arc<dyn V4l2Io>,
        device: &str,
        width: u32,
        height: u32,
        fps: u32,
        encoding: &str,
        buffer_count: u32,
    ) -> Result<Self, CaptureError> {
        if width == 0 || height == 0 || fps == 0 || buffer_count < 2 {
            return Err(CaptureError::new(
                "unsupported_mode",
                "invalid capture configuration",
            ));
        }
        let format = match encoding.to_ascii_lowercase().as_str() {
            "mjpg" | "mjpeg" | "motionjpeg" => FOURCC_MJPG,
            "jpeg" => FOURCC_JPEG,
            _ => {
                return Err(CaptureError::new(
                    "unsupported_mode",
                    "unsupported V4L2 encoding",
                ));
            }
        };
        let path = CString::new(device)
            .map_err(|_| CaptureError::new("open_failed", "device path contains NUL"))?;
        let fd = io
            .open(&path)
            .map_err(|error| CaptureError::io("open_failed", "open", error))?;
        let mut capture = Self {
            io,
            fd: Some(fd),
            buffers: Vec::new(),
            streaming: false,
            last_raw_sequence: None,
            sequence_epoch: 0,
            last_timestamp_ns: None,
            consecutive_bad_frames: 0,
            corrupt_frames_dropped: 0,
            last_buffer_flags: 0,
            last_dequeue_monotonic_ns: 0,
        };
        if let Err(error) = capture.configure(width, height, fps, format, buffer_count) {
            capture.close();
            return Err(error);
        }
        Ok(capture)
    }

    pub fn is_closed(&self) -> bool {
        self.fd.is_none()
    }

    fn fd(&self) -> Result<RawFd, CaptureError> {
        self.fd
            .ok_or_else(|| CaptureError::new("invalid_state", "V4L2 capture is closed"))
    }

    pub fn focus_status(&self) -> Result<Option<FocusStatus>, CaptureError> {
        ControlDevice::borrowed(Arc::clone(&self.io), self.fd()?).focus_status()
    }

    pub fn set_focus(
        &self,
        value: Option<i32>,
        auto_enabled: Option<bool>,
    ) -> Result<FocusStatus, CaptureError> {
        ControlDevice::borrowed(Arc::clone(&self.io), self.fd()?).set_focus(value, auto_enabled)
    }

    pub fn set_exposure(&self, exposure: &ExposureSetting) -> Result<ExposureConfig, CaptureError> {
        ControlDevice::borrowed(Arc::clone(&self.io), self.fd()?).set_exposure(exposure)
    }

    pub fn corrupt_frames_dropped(&self) -> u64 {
        self.corrupt_frames_dropped
    }

    pub fn consecutive_bad_frames(&self) -> u32 {
        self.consecutive_bad_frames
    }

    fn call(
        &self,
        request: c_ulong,
        payload: &mut [u8],
        operation: &str,
    ) -> Result<(), CaptureError> {
        self.io
            .ioctl(self.fd()?, request, payload)
            .map_err(|error| CaptureError::io("v4l2_ioctl_failed", operation, error))
    }

    fn configure(
        &mut self,
        width: u32,
        height: u32,
        fps: u32,
        format: u32,
        buffer_count: u32,
    ) -> Result<(), CaptureError> {
        let mut capabilities = [0; 104];
        self.call(QUERYCAP, &mut capabilities, "VIDIOC_QUERYCAP")?;
        let capability = read_u32(&capabilities, 84);
        let device_caps = read_u32(&capabilities, 88);
        let effective = if capability & CAP_DEVICE_CAPS != 0 {
            device_caps
        } else {
            capability
        };
        if effective & CAP_VIDEO_CAPTURE == 0 || effective & CAP_STREAMING == 0 {
            return Err(CaptureError::new(
                "unsupported_device",
                "V4L2 node must support capture and streaming",
            ));
        }

        let mut selected_format = [0; 208];
        write_u32(&mut selected_format, 0, BUF_TYPE_CAPTURE);
        for (offset, value) in [(8, width), (12, height), (16, format), (20, 1)] {
            write_u32(&mut selected_format, offset, value);
        }
        self.call(SET_FORMAT, &mut selected_format, "VIDIOC_S_FMT")?;
        if (
            read_u32(&selected_format, 8),
            read_u32(&selected_format, 12),
            read_u32(&selected_format, 16),
        ) != (width, height, format)
        {
            return Err(CaptureError::new(
                "unsupported_mode",
                "driver did not accept the exact capture format",
            ));
        }

        let mut parameters = [0; 204];
        write_u32(&mut parameters, 0, BUF_TYPE_CAPTURE);
        write_u32(&mut parameters, 12, 1);
        write_u32(&mut parameters, 16, fps);
        match self.io.ioctl(self.fd()?, SET_PARM, &mut parameters) {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.raw_os_error(),
                    Some(libc::EINVAL) | Some(libc::ENOTTY)
                ) =>
            {
                // A fixed-rate driver may not implement S_PARM. Read its
                // actual interval instead of treating our request as fact.
                parameters.fill(0);
                write_u32(&mut parameters, 0, BUF_TYPE_CAPTURE);
                self.io
                    .ioctl(self.fd()?, GET_PARM, &mut parameters)
                    .map_err(|error| {
                        CaptureError::io(
                            "unsupported_mode",
                            "cannot verify frame rate with VIDIOC_G_PARM",
                            error,
                        )
                    })?;
            }
            Err(error) => {
                return Err(CaptureError::io(
                    "v4l2_ioctl_failed",
                    "VIDIOC_S_PARM",
                    error,
                ));
            }
        }

        let numerator = read_u32(&parameters, 12);
        let denominator = read_u32(&parameters, 16);
        // u32 cross-products fit u64; division would silently truncate rates
        // such as 121/2 to 60 and accept a different capture cadence.
        if numerator == 0
            || denominator == 0
            || u64::from(denominator) != u64::from(numerator) * u64::from(fps)
        {
            return Err(CaptureError::new(
                "unsupported_mode",
                format!(
                    "driver returned frame interval {numerator}/{denominator}s, expected 1/{fps}s"
                ),
            ));
        }

        let mut request = [0; 20];
        write_u32(&mut request, 0, buffer_count);
        write_u32(&mut request, 4, BUF_TYPE_CAPTURE);
        write_u32(&mut request, 8, MEMORY_MMAP);
        self.call(REQUEST_BUFFERS, &mut request, "VIDIOC_REQBUFS")?;
        let actual = read_u32(&request, 0);
        if actual < 2 {
            return Err(CaptureError::new(
                "buffer_setup_failed",
                "driver returned fewer than two mmap buffers",
            ));
        }
        for index in 0..actual {
            let mut query = queue_payload(index);
            self.call(QUERY_BUFFER, &mut query, "VIDIOC_QUERYBUF")?;
            let offset = i64::from(read_u32(&query, 64));
            let length = read_u32(&query, 72) as usize;
            if length == 0 {
                return Err(CaptureError::new(
                    "buffer_setup_failed",
                    "empty mmap buffer",
                ));
            }
            let buffer = self
                .io
                .mmap(self.fd()?, length, offset)
                .map_err(|error| CaptureError::io("buffer_setup_failed", "mmap", error))?;
            self.buffers.push(buffer);
        }
        Ok(())
    }

    pub fn start(&mut self) -> Result<(), CaptureError> {
        if self.streaming {
            return Err(CaptureError::new(
                "invalid_state",
                "V4L2 capture already started",
            ));
        }
        let result = self.start_inner();
        if result.is_err() {
            self.close();
        }
        result
    }

    fn start_inner(&mut self) -> Result<(), CaptureError> {
        if let Some(last) = self.last_raw_sequence.take() {
            self.sequence_epoch = self.sequence_epoch.saturating_add(u64::from(last) + 1);
        }
        self.consecutive_bad_frames = 0;
        for index in 0..self.buffers.len() {
            let mut payload = queue_payload(index as u32);
            self.call(QUEUE_BUFFER, &mut payload, "VIDIOC_QBUF")?;
        }
        let mut stream_type = BUF_TYPE_CAPTURE.to_ne_bytes();
        self.call(STREAM_ON, &mut stream_type, "VIDIOC_STREAMON")?;
        self.streaming = true;
        Ok(())
    }

    fn source_sequence(&mut self, raw: u32) -> Result<u64, CaptureError> {
        if let Some(previous) = self.last_raw_sequence {
            if raw == previous {
                return Err(CaptureError::new(
                    "sequence_regression",
                    "V4L2 source sequence repeated",
                ));
            }
            if raw < previous {
                if previous - raw > 0x8000_0000 {
                    self.sequence_epoch += 1_u64 << 32;
                } else {
                    return Err(CaptureError::new(
                        "sequence_regression",
                        "V4L2 source sequence regressed",
                    ));
                }
            }
        }
        self.last_raw_sequence = Some(raw);
        Ok(self.sequence_epoch + u64::from(raw))
    }

    fn timestamp(&mut self, buffer: &[u8], flags: u32) -> Result<u64, CaptureError> {
        let driver_monotonic = is_monotonic_timestamp(flags);
        let mut value = if driver_monotonic {
            let seconds = read_i64(buffer, 24);
            let micros = read_i64(buffer, 32);
            if seconds >= 0 && (0..1_000_000).contains(&micros) {
                (seconds as u64)
                    .checked_mul(1_000_000_000)
                    .and_then(|sec_ns| sec_ns.checked_add((micros as u64) * 1_000))
                    .unwrap_or(0)
            } else {
                0
            }
        } else {
            0
        };
        if value == 0 {
            value = self
                .io
                .monotonic_ns()
                .map_err(|error| CaptureError::io("clock_failed", "clock_gettime", error))?;
        }
        if let Some(previous) = self.last_timestamp_ns {
            if value <= previous {
                if driver_monotonic {
                    return Err(CaptureError::new(
                        "timestamp_regression",
                        "V4L2 monotonic timestamp repeated or regressed",
                    ));
                }
                value = value.max(previous.saturating_add(1));
            }
        }
        self.last_timestamp_ns = Some(value);
        Ok(value)
    }

    pub fn wait(&mut self, timeout: Duration) -> Result<(), CaptureError> {
        if !self.streaming {
            return Err(CaptureError::new(
                "invalid_state",
                "V4L2 capture is not started",
            ));
        }
        if timeout.is_zero() {
            return Err(CaptureError::new(
                "invalid_argument",
                "timeout must be positive",
            ));
        }
        let readable = match self.io.wait_readable(self.fd()?, timeout) {
            Ok(readable) => readable,
            Err(error) => {
                let error = CaptureError::io("poll_failed", "poll", error);
                self.close();
                return Err(error);
            }
        };
        if !readable {
            return Err(CaptureError::new("frame_timeout", "V4L2 frame timed out"));
        }
        Ok(())
    }

    pub fn read_ready_with<T>(
        &mut self,
        process: impl FnOnce(u64, u64, &[u8]) -> Result<T, CaptureError>,
    ) -> Result<T, CaptureError> {
        if !self.streaming {
            return Err(CaptureError::new(
                "invalid_state",
                "V4L2 capture is not started",
            ));
        }
        let mut dequeue = queue_payload(0);
        if let Err(error) = self.io.ioctl(self.fd()?, DEQUEUE_BUFFER, &mut dequeue) {
            if error.raw_os_error() == Some(libc::EAGAIN) {
                return Err(CaptureError::new(
                    "frame_timeout",
                    "V4L2 frame was no longer ready",
                ));
            }
            let error = CaptureError::io("v4l2_ioctl_failed", "VIDIOC_DQBUF", error);
            self.close();
            return Err(error);
        }
        let index = read_u32(&dequeue, 0) as usize;
        let bytes_used = read_u32(&dequeue, 8) as usize;
        let flags = read_u32(&dequeue, 12);
        let raw_sequence = read_u32(&dequeue, 56);
        if index >= self.buffers.len() {
            let error = CaptureError::new("bad_frame", "invalid V4L2 buffer index");
            self.close();
            return Err(error);
        }
        self.last_buffer_flags = flags;
        let dequeue_mono = match self.io.monotonic_ns() {
            Ok(ns) => ns,
            Err(error) => {
                let mut queue = queue_payload(index as u32);
                let _ = self.call(QUEUE_BUFFER, &mut queue, "VIDIOC_QBUF");
                self.close();
                return Err(CaptureError::io("clock_failed", "clock_gettime", error));
            }
        };
        self.last_dequeue_monotonic_ns = dequeue_mono;
        if flags & BUF_FLAG_ERROR != 0 || bytes_used == 0 || bytes_used > self.buffers[index].len()
        {
            let reason = if flags & BUF_FLAG_ERROR != 0 {
                "V4L2 marked the buffer damaged"
            } else {
                "invalid V4L2 payload length"
            };
            let mut queue = queue_payload(index as u32);
            if let Err(error) = self.call(QUEUE_BUFFER, &mut queue, "VIDIOC_QBUF") {
                self.close();
                return Err(error);
            }
            self.corrupt_frames_dropped = self.corrupt_frames_dropped.saturating_add(1);
            self.consecutive_bad_frames = self.consecutive_bad_frames.saturating_add(1);
            if self.consecutive_bad_frames >= MAX_CONSECUTIVE_BAD_FRAMES {
                self.close();
                return Err(CaptureError::new(
                    "bad_frame",
                    format!(
                        "{reason} ({} consecutive bad frames)",
                        self.consecutive_bad_frames
                    ),
                ));
            }
            return Err(CaptureError::new("bad_frame", reason));
        }

        let result = (|| {
            let source_sequence = self.source_sequence(raw_sequence)?;
            let host_monotonic_ns = self.timestamp(&dequeue, flags)?;
            let payload = self.buffers[index].bytes(bytes_used)?;
            process(source_sequence, host_monotonic_ns, payload)
        })();
        let mut queue = queue_payload(index as u32);
        if let Err(error) = self.call(QUEUE_BUFFER, &mut queue, "VIDIOC_QBUF") {
            self.close();
            return Err(error);
        }
        if result.is_err() {
            self.close();
        } else {
            self.consecutive_bad_frames = 0;
        }
        result
    }

    #[cfg(test)]
    pub fn read_with<T>(
        &mut self,
        timeout: Duration,
        process: impl FnOnce(u64, u64, &[u8]) -> Result<T, CaptureError>,
    ) -> Result<T, CaptureError> {
        self.wait(timeout)?;
        self.read_ready_with(process)
    }

    #[cfg(test)]
    pub fn read(&mut self, timeout: Duration) -> Result<RawFrame, CaptureError> {
        let result = self.read_with(timeout, |source_sequence, host_monotonic_ns, payload| {
            Ok(RawFrame {
                source_sequence,
                host_monotonic_ns,
                payload: payload.to_vec(),
            })
        });
        if matches!(&result, Err(error) if error.code == "frame_timeout") {
            self.close();
        }
        result
    }

    pub fn stop(&mut self) -> Result<(), CaptureError> {
        if !self.streaming {
            return Ok(());
        }
        let mut stream_type = BUF_TYPE_CAPTURE.to_ne_bytes();
        match self.call(STREAM_OFF, &mut stream_type, "VIDIOC_STREAMOFF") {
            Ok(()) => {
                self.streaming = false;
                Ok(())
            }
            Err(error) => {
                self.close();
                Err(error)
            }
        }
    }

    #[cfg(test)]
    pub fn test_fake(fail_stream_on: bool) -> Result<Self, CaptureError> {
        let io = Arc::new(tests::FakeIo::with_stream_on_error(
            fail_stream_on.then_some(libc::EIO),
        ));
        Self::with_io(io, "/dev/video0", 3840, 1080, 60, "mjpg", 2)
    }

    #[cfg(test)]
    pub(crate) fn test_fake_with_frames(
        frames: Vec<(u32, u64, Vec<u8>, u32)>,
    ) -> Result<Self, CaptureError> {
        let io = Arc::new(tests::FakeIo::new(frames));
        Self::with_io(io, "/dev/video0", 3840, 1080, 60, "mjpg", 2)
    }

    pub fn close(&mut self) {
        if self.streaming {
            let mut stream_type = BUF_TYPE_CAPTURE.to_ne_bytes();
            let _ = self.call(STREAM_OFF, &mut stream_type, "VIDIOC_STREAMOFF");
            self.streaming = false;
        }
        self.buffers.clear();
        if let Some(fd) = self.fd.take() {
            let _ = self.io.close(fd);
        }
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    struct FakeBuffer(Vec<u8>);

    impl CaptureBuffer for FakeBuffer {
        fn len(&self) -> usize {
            self.0.len()
        }

        fn bytes(&self, length: usize) -> Result<&[u8], CaptureError> {
            Ok(&self.0[..length])
        }
    }

    pub(super) struct FakeIo {
        calls: Mutex<Vec<c_ulong>>,
        opened: Mutex<usize>,
        closed: Mutex<usize>,
        frames: Mutex<VecDeque<FakeFrame>>,
        focus: Mutex<i32>,
        focus_min: i32,
        focus_max: i32,
        auto_focus: Mutex<i32>,
        auto_exposure: Mutex<i32>,
        exposure_absolute: Mutex<i32>,
        exposure_readback_override: Option<i32>,
        frame_interval: (u32, u32),
        set_parm_error: Option<i32>,
        get_parm_error: Option<i32>,
        pub(super) stream_on_error: Option<i32>,
        control_ioctl_error: Option<(c_ulong, i32)>,
        fail_monotonic: bool,
    }

    type FakeFrame = (u32, u64, Vec<u8>, u32);

    impl FakeIo {
        pub(super) fn with_stream_on_error(stream_on_error: Option<i32>) -> Self {
            Self {
                stream_on_error,
                ..Self::new(vec![])
            }
        }

        pub(super) fn new(frames: Vec<FakeFrame>) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                opened: Mutex::new(0),
                closed: Mutex::new(0),
                frames: Mutex::new(frames.into()),
                focus: Mutex::new(32),
                focus_min: 0,
                focus_max: 255,
                auto_focus: Mutex::new(1),
                auto_exposure: Mutex::new(3),
                exposure_absolute: Mutex::new(50),
                exposure_readback_override: None,
                frame_interval: (1, 60),
                set_parm_error: None,
                get_parm_error: None,
                stream_on_error: None,
                control_ioctl_error: None,
                fail_monotonic: false,
            }
        }
    }

    impl V4l2Io for FakeIo {
        fn open(&self, _path: &CString) -> io::Result<RawFd> {
            *self.opened.lock().unwrap() += 1;
            Ok(7)
        }

        fn close(&self, _fd: RawFd) -> io::Result<()> {
            *self.closed.lock().unwrap() += 1;
            Ok(())
        }

        fn ioctl(&self, _fd: RawFd, request: c_ulong, payload: &mut [u8]) -> io::Result<()> {
            self.calls.lock().unwrap().push(request);
            match request {
                QUERYCAP => write_u32(payload, 84, CAP_VIDEO_CAPTURE | CAP_STREAMING),
                SET_FORMAT => {}
                request if request == SET_PARM || request == ioc(IOC_READ | IOC_WRITE, 21, 204) => {
                    let error = if request == SET_PARM {
                        self.set_parm_error
                    } else {
                        self.get_parm_error
                    };
                    if let Some(errno) = error {
                        return Err(io::Error::from_raw_os_error(errno));
                    }
                    write_u32(payload, 12, self.frame_interval.0);
                    write_u32(payload, 16, self.frame_interval.1);
                }
                REQUEST_BUFFERS => write_u32(payload, 0, 2),
                QUERY_BUFFER => {
                    let index = read_u32(payload, 0);
                    write_u32(payload, 64, index * 4096);
                    write_u32(payload, 72, 4096);
                }
                DEQUEUE_BUFFER => {
                    let (sequence, timestamp, frame, flags) =
                        self.frames.lock().unwrap().pop_front().unwrap();
                    let index = sequence as usize % 2;
                    write_u32(payload, 0, index as u32);
                    write_u32(payload, 8, frame.len() as u32);
                    write_u32(payload, 12, flags);
                    write_u32(payload, 56, sequence);
                    payload[24..32]
                        .copy_from_slice(&(timestamp as i64 / 1_000_000_000).to_ne_bytes());
                    payload[32..40].copy_from_slice(
                        &((timestamp % 1_000_000_000) as i64 / 1_000).to_ne_bytes(),
                    );
                }
                QUEUE_BUFFER | STREAM_OFF => {}
                STREAM_ON => {
                    if let Some(errno) = self.stream_on_error {
                        return Err(io::Error::from_raw_os_error(errno));
                    }
                }
                QUERY_CONTROL => {
                    let id = read_u32(payload, 0);
                    match id {
                        CID_FOCUS_ABSOLUTE => {
                            write_u32(payload, 4, CTRL_TYPE_INTEGER);
                            write_i32(payload, 40, self.focus_min);
                            write_i32(payload, 44, self.focus_max);
                            write_i32(payload, 48, 1);
                            write_i32(payload, 52, 32);
                        }
                        CID_FOCUS_AUTO => {
                            write_u32(payload, 4, CTRL_TYPE_BOOLEAN);
                            write_i32(payload, 40, 0);
                            write_i32(payload, 44, 1);
                            write_i32(payload, 48, 1);
                            write_i32(payload, 52, 1);
                        }
                        _ => return Err(io::Error::from_raw_os_error(libc::EINVAL)),
                    }
                }
                GET_CONTROL => {
                    if let Some((GET_CONTROL, errno)) = self.control_ioctl_error {
                        return Err(io::Error::from_raw_os_error(errno));
                    }
                    let value = match read_u32(payload, 0) {
                        CID_FOCUS_ABSOLUTE => *self.focus.lock().unwrap(),
                        CID_FOCUS_AUTO => *self.auto_focus.lock().unwrap(),
                        CID_EXPOSURE_AUTO => *self.auto_exposure.lock().unwrap(),
                        CID_EXPOSURE_ABSOLUTE => self
                            .exposure_readback_override
                            .unwrap_or(*self.exposure_absolute.lock().unwrap()),
                        _ => return Err(io::Error::from_raw_os_error(libc::EINVAL)),
                    };
                    write_i32(payload, 4, value);
                }
                SET_CONTROL => {
                    if let Some((SET_CONTROL, errno)) = self.control_ioctl_error {
                        return Err(io::Error::from_raw_os_error(errno));
                    }
                    match read_u32(payload, 0) {
                        CID_FOCUS_ABSOLUTE => {
                            *self.focus.lock().unwrap() = read_i32(payload, 4);
                        }
                        CID_FOCUS_AUTO => {
                            *self.auto_focus.lock().unwrap() = read_i32(payload, 4);
                        }
                        CID_EXPOSURE_AUTO => {
                            *self.auto_exposure.lock().unwrap() = read_i32(payload, 4);
                        }
                        CID_EXPOSURE_ABSOLUTE => {
                            *self.exposure_absolute.lock().unwrap() = read_i32(payload, 4);
                        }
                        _ => return Err(io::Error::from_raw_os_error(libc::EINVAL)),
                    }
                }
                _ => panic!("unexpected ioctl {request:#x}"),
            }
            Ok(())
        }

        fn mmap(
            &self,
            _fd: RawFd,
            length: usize,
            offset: i64,
        ) -> io::Result<Box<dyn CaptureBuffer>> {
            let index = usize::try_from(offset / 4096).unwrap();
            let frames = self.frames.lock().unwrap();
            let frame = frames
                .get(index)
                .map(|item| item.2.as_slice())
                .unwrap_or_default();
            let mut data = vec![0; length];
            data[..frame.len()].copy_from_slice(frame);
            Ok(Box::new(FakeBuffer(data)))
        }

        fn wait_readable(&self, _fd: RawFd, _timeout: Duration) -> io::Result<bool> {
            Ok(!self.frames.lock().unwrap().is_empty())
        }

        fn monotonic_ns(&self) -> io::Result<u64> {
            if self.fail_monotonic {
                Err(io::Error::other("clock_gettime failed"))
            } else {
                Ok(99)
            }
        }
    }

    #[test]
    fn negotiated_rate_requires_exact_rational_equality() {
        for interval in [(2, 121), (1000, 60001), (0, 60), (1, 0), (u32::MAX, 60)] {
            let io = Arc::new(FakeIo {
                frame_interval: interval,
                ..FakeIo::new(vec![])
            });
            let result = Capture::with_io(io.clone(), "/dev/video0", 3840, 1080, 60, "mjpg", 2);
            assert!(
                matches!(result, Err(ref error) if error.code == "unsupported_mode"),
                "accepted {interval:?}"
            );
            assert_eq!(*io.closed.lock().unwrap(), 1);
            assert!(!io.calls.lock().unwrap().contains(&REQUEST_BUFFERS));
        }
        let io = Arc::new(FakeIo {
            frame_interval: (1001, 60060),
            ..FakeIo::new(vec![])
        });
        let mut capture =
            Capture::with_io(io.clone(), "/dev/video0", 3840, 1080, 60, "mjpg", 2).unwrap();
        capture.close();
        assert_eq!(*io.closed.lock().unwrap(), 1);
    }

    #[test]
    fn unsupported_rate_setter_requires_verified_current_rate() {
        for errno in [libc::EINVAL, libc::ENOTTY] {
            for (interval, get_error, accepted) in [
                ((1, 60), None, true),
                ((1, 30), None, false),
                ((1, 60), Some(libc::ENOTTY), false),
            ] {
                let io = Arc::new(FakeIo {
                    frame_interval: interval,
                    set_parm_error: Some(errno),
                    get_parm_error: get_error,
                    ..FakeIo::new(vec![])
                });
                let result = Capture::with_io(io.clone(), "/dev/video0", 3840, 1080, 60, "mjpg", 2);
                assert_eq!(
                    result.is_ok(),
                    accepted,
                    "interval={interval:?}, get_error={get_error:?}"
                );
                drop(result);
                assert_eq!(*io.closed.lock().unwrap(), 1);
                assert!(
                    io.calls
                        .lock()
                        .unwrap()
                        .contains(&ioc(IOC_READ | IOC_WRITE, 21, 204))
                );
            }
        }
    }

    #[test]
    fn lifecycle_reads_requeues_and_releases_once() {
        let io = Arc::new(FakeIo::new(vec![
            (
                10,
                1_000_000_000,
                b"jpeg-one".to_vec(),
                BUF_FLAG_TIMESTAMP_MONOTONIC,
            ),
            (
                11,
                2_000_000_000,
                b"jpeg-two".to_vec(),
                BUF_FLAG_TIMESTAMP_MONOTONIC,
            ),
        ]));
        let mut capture =
            Capture::with_io(io.clone(), "/dev/video0", 3840, 1080, 60, "mjpg", 2).unwrap();
        capture.start().unwrap();
        let first = capture.read(Duration::from_secs(1)).unwrap();
        let second = capture.read(Duration::from_secs(1)).unwrap();
        assert_eq!(
            (
                first.source_sequence,
                first.host_monotonic_ns,
                first.payload.as_slice()
            ),
            (10, 1_000_000_000, b"jpeg-one".as_slice())
        );
        assert_eq!(
            (
                second.source_sequence,
                second.host_monotonic_ns,
                second.payload.as_slice()
            ),
            (11, 2_000_000_000, b"jpeg-two".as_slice())
        );
        capture.stop().unwrap();
        capture.close();
        capture.close();
        assert_eq!(*io.closed.lock().unwrap(), 1);
        let calls = io.calls.lock().unwrap();
        assert!(calls.contains(&STREAM_ON));
        assert!(calls.contains(&DEQUEUE_BUFFER));
        assert!(calls.contains(&QUEUE_BUFFER));
        assert!(calls.contains(&STREAM_OFF));
    }

    #[test]
    fn restart_advances_sequence_epoch_and_clears_last_raw_sequence() {
        let io = Arc::new(FakeIo::new(vec![
            (
                5,
                1_000_000_000,
                b"first-run".to_vec(),
                BUF_FLAG_TIMESTAMP_MONOTONIC,
            ),
            (
                0,
                2_000_000_000,
                b"second-run".to_vec(),
                BUF_FLAG_TIMESTAMP_MONOTONIC,
            ),
        ]));
        let mut capture =
            Capture::with_io(io.clone(), "/dev/video0", 3840, 1080, 60, "mjpg", 2).unwrap();
        capture.start().unwrap();
        assert_eq!(
            capture
                .read(Duration::from_secs(1))
                .unwrap()
                .source_sequence,
            5
        );
        capture.stop().unwrap();

        capture.start().unwrap();
        assert_eq!(
            capture
                .read(Duration::from_secs(1))
                .unwrap()
                .source_sequence,
            6
        );
        capture.close();
    }

    #[test]
    fn monotonic_clock_failure_requeues_buffer_before_closing() {
        let io = Arc::new(FakeIo {
            fail_monotonic: true,
            ..FakeIo::new(vec![(
                9,
                1_000_000_000,
                b"frame".to_vec(),
                BUF_FLAG_TIMESTAMP_MONOTONIC,
            )])
        });
        let mut capture =
            Capture::with_io(io.clone(), "/dev/video0", 3840, 1080, 60, "mjpg", 2).unwrap();
        capture.start().unwrap();
        assert_eq!(
            capture.read(Duration::from_secs(1)).unwrap_err().code,
            "clock_failed"
        );
        assert_eq!(*io.closed.lock().unwrap(), 1);
        let calls = io.calls.lock().unwrap();
        let dequeue = calls
            .iter()
            .position(|request| *request == DEQUEUE_BUFFER)
            .unwrap();
        assert_eq!(calls[dequeue + 1], QUEUE_BUFFER);
        assert!(calls[dequeue + 2..].contains(&STREAM_OFF));
    }

    #[test]
    fn active_capture_focus_reuses_the_capture_file_descriptor() {
        let io = Arc::new(FakeIo::new(vec![]));
        let mut capture =
            Capture::with_io(io.clone(), "/dev/video0", 3840, 1080, 60, "mjpg", 2).unwrap();
        capture.start().unwrap();

        let initial = capture.focus_status().unwrap().unwrap();
        assert_eq!(initial.value, 32);
        assert!(initial.auto_enabled.unwrap());
        let updated = capture.set_focus(Some(64), Some(false)).unwrap();
        assert_eq!(updated.value, 64);
        assert_eq!(updated.auto_enabled, Some(false));
        assert_eq!(*io.opened.lock().unwrap(), 1);
        assert_eq!(*io.closed.lock().unwrap(), 0);

        capture.close();
        assert_eq!(*io.closed.lock().unwrap(), 1);
    }

    #[test]
    fn set_focus_avoids_i32_overflow_with_extreme_bounds() {
        let io = Arc::new(FakeIo {
            focus_min: i32::MIN,
            focus_max: i32::MAX,
            ..FakeIo::new(vec![])
        });
        let control =
            ControlDevice::open_with_io(io, "/dev/video0", "camera_focus_open_failed").unwrap();
        let status = control.set_focus(Some(10), Some(false)).unwrap();
        assert_eq!(status.value, 10);
    }

    #[test]
    fn configure_exposure_verifies_auto_manual_and_readback() {
        let io = Arc::new(FakeIo::new(vec![]));
        let control =
            ControlDevice::open_with_io(io.clone(), "/dev/video0", "open_failed").unwrap();
        let auto_cfg = control.configure_exposure(None, true).unwrap();
        assert_eq!(
            auto_cfg,
            ExposureConfig {
                auto_exposure: 3,
                exposure_time_absolute: None,
            }
        );
        let manual_cfg = control.configure_exposure(Some(42), false).unwrap();
        assert_eq!(
            manual_cfg,
            ExposureConfig {
                auto_exposure: 1,
                exposure_time_absolute: Some(42),
            }
        );
        assert_eq!(
            control.configure_exposure(Some(0), false).unwrap_err().code,
            "invalid_argument"
        );
        assert_eq!(
            control
                .configure_exposure(Some(101), false)
                .unwrap_err()
                .code,
            "invalid_argument"
        );

        let mismatched = Arc::new(FakeIo {
            exposure_readback_override: Some(99),
            ..FakeIo::new(vec![])
        });
        let bad_control =
            ControlDevice::open_with_io(mismatched, "/dev/video0", "open_failed").unwrap();
        assert_eq!(
            bad_control
                .configure_exposure(Some(50), false)
                .unwrap_err()
                .code,
            "exposure_readback_failed"
        );
    }

    #[test]
    fn timeout_wrap_regression_and_error_are_stable() {
        let io = Arc::new(FakeIo::new(vec![]));
        let mut capture =
            Capture::with_io(io.clone(), "/dev/video0", 3840, 1080, 60, "mjpg", 2).unwrap();
        capture.start().unwrap();
        assert_eq!(
            capture.read(Duration::from_millis(1)).unwrap_err().code,
            "frame_timeout"
        );
        assert_eq!(*io.closed.lock().unwrap(), 1);

        let mut sequence = Capture::with_io(
            Arc::new(FakeIo::new(vec![])),
            "/dev/video0",
            3840,
            1080,
            60,
            "mjpg",
            2,
        )
        .unwrap();
        sequence.last_raw_sequence = Some(u32::MAX);
        assert_eq!(sequence.source_sequence(0).unwrap(), 1_u64 << 32);
        sequence.last_raw_sequence = Some(10);
        assert_eq!(
            sequence.source_sequence(9).unwrap_err().code,
            "sequence_regression"
        );
    }

    #[test]
    fn frame_error_requeues_and_recovers_until_hard_fault_threshold() {
        // 1. Single transient BUF_FLAG_ERROR requeues buffer, increments drop counter,
        //    keeps capture open, and recovers cleanly on the next valid frame!
        let io = Arc::new(FakeIo::new(vec![
            (
                9,
                1_000_000_000,
                b"damaged".to_vec(),
                BUF_FLAG_ERROR | BUF_FLAG_TIMESTAMP_MONOTONIC,
            ),
            (
                10,
                2_000_000_000,
                b"recovered".to_vec(),
                BUF_FLAG_TIMESTAMP_MONOTONIC,
            ),
        ]));
        let mut capture =
            Capture::with_io(io.clone(), "/dev/video0", 3840, 1080, 60, "mjpg", 2).unwrap();
        capture.start().unwrap();
        assert_eq!(
            capture.read(Duration::from_secs(1)).unwrap_err().code,
            "bad_frame"
        );
        assert!(!capture.is_closed());
        assert_eq!(capture.corrupt_frames_dropped(), 1);
        assert_eq!(capture.consecutive_bad_frames(), 1);
        assert_eq!(*io.closed.lock().unwrap(), 0);

        let good = capture.read(Duration::from_secs(1)).unwrap();
        assert_eq!(good.source_sequence, 10);
        assert_eq!(capture.corrupt_frames_dropped(), 1);
        assert_eq!(capture.consecutive_bad_frames(), 0);
        capture.close();
        assert_eq!(*io.closed.lock().unwrap(), 1);

        // 2. 30 consecutive BUF_FLAG_ERROR frames reach MAX_CONSECUTIVE_BAD_FRAMES and close stream.
        let hard_fault_frames = (0..MAX_CONSECUTIVE_BAD_FRAMES)
            .map(|seq| {
                (
                    seq,
                    (u64::from(seq) + 1) * 10_000_000,
                    b"damaged".to_vec(),
                    BUF_FLAG_ERROR | BUF_FLAG_TIMESTAMP_MONOTONIC,
                )
            })
            .collect::<Vec<_>>();
        let fault_io = Arc::new(FakeIo::new(hard_fault_frames));
        let mut fault_capture =
            Capture::with_io(fault_io.clone(), "/dev/video0", 3840, 1080, 60, "mjpg", 2).unwrap();
        fault_capture.start().unwrap();
        for i in 1..MAX_CONSECUTIVE_BAD_FRAMES {
            assert_eq!(
                fault_capture.read(Duration::from_secs(1)).unwrap_err().code,
                "bad_frame"
            );
            assert!(
                !fault_capture.is_closed(),
                "closed prematurely at frame {i}"
            );
        }
        let terminal = fault_capture.read(Duration::from_secs(1)).unwrap_err();
        assert_eq!(terminal.code, "bad_frame");
        assert!(terminal.message.contains("consecutive bad frames"));
        assert!(fault_capture.is_closed());
        assert_eq!(
            fault_capture.corrupt_frames_dropped(),
            u64::from(MAX_CONSECUTIVE_BAD_FRAMES)
        );
        assert_eq!(*fault_io.closed.lock().unwrap(), 1);
    }

    #[test]
    fn set_focus_preserves_autofocus_without_overwriting_focus_absolute_and_set_exposure_quantizes()
    {
        let io = Arc::new(FakeIo {
            focus_min: 10,
            focus_max: 500,
            ..FakeIo::new(vec![])
        });
        let control =
            ControlDevice::open_with_io(io.clone(), "/dev/video0", "camera_focus_open_failed")
                .unwrap();

        // Enabling autofocus with Some(value) must NOT overwrite CID_FOCUS_AUTO=0 or CID_FOCUS_ABSOLUTE!
        *io.auto_focus.lock().unwrap() = 0;
        *io.focus.lock().unwrap() = 32;
        let auto_status = control.set_focus(Some(200), Some(true)).unwrap();
        assert_eq!(auto_status.auto_enabled, Some(true));
        assert_eq!(auto_status.value, 32);
        assert_eq!(auto_status.minimum, 10);
        assert_eq!(auto_status.maximum, 500);

        // Disabling autofocus with Some(v) sets CID_FOCUS_AUTO=0 first, then CID_FOCUS_ABSOLUTE=v!
        io.calls.lock().unwrap().clear();
        let manual_status = control.set_focus(Some(250), Some(false)).unwrap();
        assert_eq!(manual_status.auto_enabled, Some(false));
        assert_eq!(manual_status.value, 250);

        // Sensor row-time quantization readback tolerance (target=50, actual=49 within tolerance=2)
        let quantized_io = Arc::new(FakeIo {
            exposure_readback_override: Some(49),
            ..FakeIo::new(vec![])
        });
        let quantized_ctl =
            ControlDevice::open_with_io(quantized_io, "/dev/video0", "open_failed").unwrap();
        let exp = quantized_ctl
            .set_exposure(&ExposureSetting::Manual(50))
            .unwrap();
        assert_eq!(
            exp,
            ExposureConfig {
                auto_exposure: 1,
                exposure_time_absolute: Some(49),
            }
        );
    }

    #[test]
    fn sequence_and_driver_timestamp_regressions_close_after_requeue() {
        let cases = [
            (
                vec![
                    (
                        10,
                        1_000_000_000,
                        b"first".to_vec(),
                        BUF_FLAG_TIMESTAMP_MONOTONIC,
                    ),
                    (
                        9,
                        2_000_000_000,
                        b"second".to_vec(),
                        BUF_FLAG_TIMESTAMP_MONOTONIC,
                    ),
                ],
                "sequence_regression",
            ),
            (
                vec![
                    (
                        10,
                        2_000_000_000,
                        b"first".to_vec(),
                        BUF_FLAG_TIMESTAMP_MONOTONIC,
                    ),
                    (
                        11,
                        1_000_000_000,
                        b"second".to_vec(),
                        BUF_FLAG_TIMESTAMP_MONOTONIC,
                    ),
                ],
                "timestamp_regression",
            ),
        ];
        for (frames, expected) in cases {
            let io = Arc::new(FakeIo::new(frames));
            let mut capture =
                Capture::with_io(io.clone(), "/dev/video0", 3840, 1080, 60, "mjpg", 2).unwrap();
            capture.start().unwrap();
            capture.read(Duration::from_secs(1)).unwrap();
            assert_eq!(
                capture.read(Duration::from_secs(1)).unwrap_err().code,
                expected
            );
            assert_eq!(*io.closed.lock().unwrap(), 1);
            let calls = io.calls.lock().unwrap();
            let dequeues = calls
                .iter()
                .enumerate()
                .filter(|(_, request)| **request == DEQUEUE_BUFFER)
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            assert_eq!(dequeues.len(), 2);
            assert_eq!(calls[dequeues[1] + 1], QUEUE_BUFFER);
            assert!(calls[dequeues[1] + 2..].contains(&STREAM_OFF));
        }
    }

    #[test]
    fn sequence_rejects_duplicate_and_timestamp_validates_mask_and_micros() {
        let mut capture = Capture::with_io(
            Arc::new(FakeIo::new(vec![])),
            "/dev/video0",
            3840,
            1080,
            60,
            "mjpg",
            2,
        )
        .unwrap();
        capture.last_raw_sequence = Some(10);
        assert_eq!(
            capture.source_sequence(10).unwrap_err().code,
            "sequence_regression"
        );

        assert!(is_monotonic_timestamp(0x2000));
        assert!(!is_monotonic_timestamp(0x4000));
        assert!(!is_monotonic_timestamp(0x6000));

        let mut buffer = [0_u8; 88];
        buffer[24..32].copy_from_slice(&5_i64.to_ne_bytes());
        buffer[32..40].copy_from_slice(&1_000_000_i64.to_ne_bytes());
        // Out-of-range micros falls back to monotonic_ns (99 in FakeIo).
        assert_eq!(
            capture
                .timestamp(&buffer, BUF_FLAG_TIMESTAMP_MONOTONIC)
                .unwrap(),
            99
        );
        // Non-monotonic timestamp flag (0x6000) falls back and enforces saturating_add(1).
        capture.last_timestamp_ns = Some(u64::MAX);
        assert_eq!(capture.timestamp(&buffer, 0x6000).unwrap(), u64::MAX);
    }

    #[test]
    fn focus_validates_before_mutating_auto_and_exposure_uses_exposure_error_codes() {
        let io = Arc::new(FakeIo::new(vec![]));
        let control =
            ControlDevice::open_with_io(io.clone(), "/dev/video0", "camera_focus_open_failed")
                .unwrap();
        assert_eq!(
            control.set_focus(Some(999), Some(false)).unwrap_err().code,
            "invalid_camera_focus"
        );
        assert_eq!(*io.auto_focus.lock().unwrap(), 1);

        let set_err_io = Arc::new(FakeIo {
            control_ioctl_error: Some((SET_CONTROL, libc::EIO)),
            ..FakeIo::new(vec![])
        });
        let set_err_ctl =
            ControlDevice::open_with_io(set_err_io, "/dev/video0", "open_failed").unwrap();
        assert_eq!(
            set_err_ctl.configure_exposure(None, true).unwrap_err().code,
            "exposure_set_failed"
        );

        let get_err_io = Arc::new(FakeIo {
            control_ioctl_error: Some((GET_CONTROL, libc::EIO)),
            ..FakeIo::new(vec![])
        });
        let get_err_ctl =
            ControlDevice::open_with_io(get_err_io, "/dev/video0", "open_failed").unwrap();
        assert_eq!(
            get_err_ctl.configure_exposure(None, true).unwrap_err().code,
            "exposure_get_failed"
        );
    }
}
