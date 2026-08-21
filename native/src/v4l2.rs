use std::ffi::{CString, c_ulong, c_void};
use std::io;
use std::os::fd::RawFd;
use std::ptr::NonNull;
use std::sync::Arc;
use std::time::Duration;

const BUF_TYPE_CAPTURE: u32 = 1;
const MEMORY_MMAP: u32 = 1;
const CAP_VIDEO_CAPTURE: u32 = 0x0000_0001;
const CAP_STREAMING: u32 = 0x0400_0000;
const CAP_DEVICE_CAPS: u32 = 0x8000_0000;
const BUF_FLAG_ERROR: u32 = 0x0000_0040;
const BUF_FLAG_TIMESTAMP_MONOTONIC: u32 = 0x0000_2000;
const IOC_READ: u32 = 2;
const IOC_WRITE: u32 = 1;
const FOURCC_MJPG: u32 = u32::from_le_bytes(*b"MJPG");
const FOURCC_JPEG: u32 = u32::from_le_bytes(*b"JPEG");

const fn ioc(direction: u32, number: u32, size: u32) -> c_ulong {
    ((direction << 30) | (size << 16) | ((b'V' as u32) << 8) | number) as c_ulong
}

const QUERYCAP: c_ulong = ioc(IOC_READ, 0, 104);
const SET_FORMAT: c_ulong = ioc(IOC_READ | IOC_WRITE, 5, 208);
const SET_PARM: c_ulong = ioc(IOC_READ | IOC_WRITE, 22, 204);
const REQUEST_BUFFERS: c_ulong = ioc(IOC_READ | IOC_WRITE, 8, 20);
const QUERY_BUFFER: c_ulong = ioc(IOC_READ | IOC_WRITE, 9, 88);
const QUEUE_BUFFER: c_ulong = ioc(IOC_READ | IOC_WRITE, 15, 88);
const DEQUEUE_BUFFER: c_ulong = ioc(IOC_READ | IOC_WRITE, 17, 88);
const STREAM_ON: c_ulong = ioc(IOC_WRITE, 18, 4);
const STREAM_OFF: c_ulong = ioc(IOC_WRITE, 19, 4);
const GET_CONTROL: c_ulong = ioc(IOC_READ | IOC_WRITE, 27, 8);
const SET_CONTROL: c_ulong = ioc(IOC_READ | IOC_WRITE, 28, 8);
const QUERY_CONTROL: c_ulong = ioc(IOC_READ | IOC_WRITE, 36, 68);
const CTRL_TYPE_INTEGER: u32 = 1;
const CTRL_TYPE_BOOLEAN: u32 = 2;
const CTRL_FLAG_DISABLED: u32 = 0x0000_0001;
const CTRL_CLASS_CAMERA: u32 = 0x009a_0000;
const CID_CAMERA_CLASS_BASE: u32 = CTRL_CLASS_CAMERA | 0x900;
const CID_FOCUS_ABSOLUTE: u32 = CID_CAMERA_CLASS_BASE + 10;
const CID_FOCUS_AUTO: u32 = CID_CAMERA_CLASS_BASE + 12;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CaptureError {
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FocusStatus {
    pub(crate) value: i32,
    pub(crate) minimum: i32,
    pub(crate) maximum: i32,
    pub(crate) step: i32,
    pub(crate) default_value: i32,
    pub(crate) auto_supported: bool,
    pub(crate) auto_enabled: Option<bool>,
}

impl CaptureError {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
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
pub(crate) struct RawFrame {
    pub(crate) source_sequence: u64,
    pub(crate) host_monotonic_ns: u64,
    pub(crate) payload: Vec<u8>,
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
        // SAFETY: CString guarantees a NUL-terminated path.
        let fd = unsafe {
            libc::open(
                path.as_ptr(),
                libc::O_RDWR | libc::O_NONBLOCK | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(fd)
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
        Ok(Box::new(MmapBuffer {
            pointer: NonNull::new(pointer.cast()).expect("mmap success cannot be null"),
            length,
        }))
    }

    fn wait_readable(&self, fd: RawFd, timeout: Duration) -> io::Result<bool> {
        let timeout_ms = i32::try_from(timeout.as_millis())
            .unwrap_or(i32::MAX)
            .max(1);
        let mut descriptor = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        loop {
            // SAFETY: descriptor points to one writable pollfd.
            let result = unsafe { libc::poll(&mut descriptor, 1, timeout_ms) };
            if result < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
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
        let mut timestamp = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: timestamp points to writable storage.
        if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut timestamp) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(timestamp.tv_sec as u64 * 1_000_000_000 + timestamp.tv_nsec as u64)
    }
}

struct ControlDevice {
    io: Arc<dyn V4l2Io>,
    fd: Option<RawFd>,
}

impl ControlDevice {
    fn open(device: &str) -> Result<Self, CaptureError> {
        let io = Arc::new(SystemIo);
        let path = CString::new(device).map_err(|_| {
            CaptureError::new("camera_focus_invalid_device", "device path contains NUL")
        })?;
        let fd = io
            .open(&path)
            .map_err(|error| CaptureError::io("camera_focus_open_failed", "open", error))?;
        Ok(Self { io, fd: Some(fd) })
    }

    fn fd(&self) -> Result<RawFd, CaptureError> {
        self.fd
            .ok_or_else(|| CaptureError::new("invalid_state", "V4L2 control device is closed"))
    }

    fn close(&mut self) {
        if let Some(fd) = self.fd.take() {
            let _ = self.io.close(fd);
        }
    }

    fn query_control(
        &self,
        id: u32,
        expected_type: u32,
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
                return Err(CaptureError::io(
                    "camera_focus_query_failed",
                    "VIDIOC_QUERYCTRL",
                    error,
                ));
            }
        }
        let control_type = read_u32(&payload, 4);
        let flags = read_u32(&payload, 56);
        if flags & CTRL_FLAG_DISABLED != 0 || control_type != expected_type {
            return Ok(None);
        }
        Ok(Some(QueryControl {
            minimum: read_i32(&payload, 40),
            maximum: read_i32(&payload, 44),
            step: read_i32(&payload, 48).max(1),
            default_value: read_i32(&payload, 52),
        }))
    }

    fn get_control(&self, id: u32) -> Result<i32, CaptureError> {
        let mut payload = [0; 8];
        write_u32(&mut payload, 0, id);
        self.io
            .ioctl(self.fd()?, GET_CONTROL, &mut payload)
            .map_err(|error| CaptureError::io("camera_focus_get_failed", "VIDIOC_G_CTRL", error))?;
        Ok(read_i32(&payload, 4))
    }

    fn set_control(&self, id: u32, value: i32) -> Result<(), CaptureError> {
        let mut payload = [0; 8];
        write_u32(&mut payload, 0, id);
        write_i32(&mut payload, 4, value);
        self.io
            .ioctl(self.fd()?, SET_CONTROL, &mut payload)
            .map_err(|error| CaptureError::io("camera_focus_set_failed", "VIDIOC_S_CTRL", error))
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
    u32::from_ne_bytes(payload[offset..offset + 4].try_into().unwrap())
}

fn read_i32(payload: &[u8], offset: usize) -> i32 {
    i32::from_ne_bytes(payload[offset..offset + 4].try_into().unwrap())
}

fn read_i64(payload: &[u8], offset: usize) -> i64 {
    i64::from_ne_bytes(payload[offset..offset + 8].try_into().unwrap())
}

pub(crate) fn focus_status(device: &str) -> Result<Option<FocusStatus>, CaptureError> {
    let control = ControlDevice::open(device)?;
    let Some(focus) = control.query_control(CID_FOCUS_ABSOLUTE, CTRL_TYPE_INTEGER)? else {
        return Ok(None);
    };
    let value = control.get_control(CID_FOCUS_ABSOLUTE)?;
    let auto = control.query_control(CID_FOCUS_AUTO, CTRL_TYPE_BOOLEAN)?;
    let auto_enabled = if auto.is_some() {
        Some(control.get_control(CID_FOCUS_AUTO)? != 0)
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

pub(crate) fn set_focus(
    device: &str,
    value: Option<i32>,
    auto_enabled: Option<bool>,
) -> Result<FocusStatus, CaptureError> {
    let control = ControlDevice::open(device)?;
    let Some(focus) = control.query_control(CID_FOCUS_ABSOLUTE, CTRL_TYPE_INTEGER)? else {
        return Err(CaptureError::new(
            "camera_focus_unsupported",
            "V4L2 device does not expose focus_absolute",
        ));
    };
    let auto = control.query_control(CID_FOCUS_AUTO, CTRL_TYPE_BOOLEAN)?;
    if let Some(enabled) = auto_enabled {
        if auto.is_none() {
            return Err(CaptureError::new(
                "camera_focus_auto_unsupported",
                "V4L2 device does not expose focus_auto",
            ));
        }
        control.set_control(CID_FOCUS_AUTO, i32::from(enabled))?;
    }
    if let Some(next) = value {
        if next < focus.minimum || next > focus.maximum || (next - focus.minimum) % focus.step != 0
        {
            return Err(CaptureError::new(
                "invalid_camera_focus",
                format!(
                    "focus value must be between {} and {} with step {}",
                    focus.minimum, focus.maximum, focus.step
                ),
            ));
        }
        if auto.is_some() && auto_enabled != Some(false) {
            control.set_control(CID_FOCUS_AUTO, 0)?;
        }
        control.set_control(CID_FOCUS_ABSOLUTE, next)?;
    }
    focus_status(device)?
        .ok_or_else(|| CaptureError::new("camera_focus_unsupported", "focus control disappeared"))
}

fn queue_payload(index: u32) -> [u8; 88] {
    let mut payload = [0; 88];
    write_u32(&mut payload, 0, index);
    write_u32(&mut payload, 4, BUF_TYPE_CAPTURE);
    write_u32(&mut payload, 60, MEMORY_MMAP);
    payload
}

pub(crate) struct Capture {
    io: Arc<dyn V4l2Io>,
    fd: Option<RawFd>,
    buffers: Vec<Box<dyn CaptureBuffer>>,
    streaming: bool,
    last_raw_sequence: Option<u32>,
    sequence_epoch: u64,
    last_timestamp_ns: Option<u64>,
}

impl Capture {
    pub(crate) fn open(
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
        };
        if let Err(error) = capture.configure(width, height, fps, format, buffer_count) {
            capture.close();
            return Err(error);
        }
        Ok(capture)
    }

    fn fd(&self) -> Result<RawFd, CaptureError> {
        self.fd
            .ok_or_else(|| CaptureError::new("invalid_state", "V4L2 capture is closed"))
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
            Ok(()) => {
                let numerator = read_u32(&parameters, 12);
                let denominator = read_u32(&parameters, 16);
                if numerator == 0 || denominator == 0 || denominator / numerator != fps {
                    return Err(CaptureError::new(
                        "unsupported_mode",
                        "driver did not accept the exact frame rate",
                    ));
                }
            }
            Err(error)
                if matches!(
                    error.raw_os_error(),
                    Some(libc::EINVAL) | Some(libc::ENOTTY)
                ) => {}
            Err(error) => {
                return Err(CaptureError::io(
                    "v4l2_ioctl_failed",
                    "VIDIOC_S_PARM",
                    error,
                ));
            }
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

    pub(crate) fn start(&mut self) -> Result<(), CaptureError> {
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
        let driver_monotonic = flags & BUF_FLAG_TIMESTAMP_MONOTONIC != 0;
        let mut value = if driver_monotonic {
            let seconds = read_i64(buffer, 24);
            let micros = read_i64(buffer, 32);
            if seconds >= 0 && micros >= 0 {
                seconds as u64 * 1_000_000_000 + micros as u64 * 1_000
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
                value = value.max(previous + 1);
            }
        }
        self.last_timestamp_ns = Some(value);
        Ok(value)
    }

    pub(crate) fn wait(&mut self, timeout: Duration) -> Result<(), CaptureError> {
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

    pub(crate) fn read_ready_with<T>(
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
        let result = (|| {
            if flags & BUF_FLAG_ERROR != 0 {
                return Err(CaptureError::new(
                    "bad_frame",
                    "V4L2 marked the buffer damaged",
                ));
            }
            if bytes_used == 0 || bytes_used > self.buffers[index].len() {
                return Err(CaptureError::new(
                    "bad_frame",
                    "invalid V4L2 payload length",
                ));
            }
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
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn read_with<T>(
        &mut self,
        timeout: Duration,
        process: impl FnOnce(u64, u64, &[u8]) -> Result<T, CaptureError>,
    ) -> Result<T, CaptureError> {
        self.wait(timeout)?;
        self.read_ready_with(process)
    }

    #[cfg(test)]
    pub(crate) fn read(&mut self, timeout: Duration) -> Result<RawFrame, CaptureError> {
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

    pub(crate) fn stop(&mut self) -> Result<(), CaptureError> {
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

    pub(crate) fn close(&mut self) {
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

    struct FakeIo {
        calls: Mutex<Vec<c_ulong>>,
        closed: Mutex<usize>,
        frames: Mutex<VecDeque<FakeFrame>>,
    }

    type FakeFrame = (u32, u64, Vec<u8>, u32);

    impl FakeIo {
        fn new(frames: Vec<FakeFrame>) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                closed: Mutex::new(0),
                frames: Mutex::new(frames.into()),
            }
        }
    }

    impl V4l2Io for FakeIo {
        fn open(&self, _path: &CString) -> io::Result<RawFd> {
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
                SET_PARM => {
                    write_u32(payload, 12, 1);
                    write_u32(payload, 16, 60);
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
                QUEUE_BUFFER | STREAM_ON | STREAM_OFF => {}
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
            Ok(99)
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
    fn frame_error_is_requeued_before_capture_closes() {
        let io = Arc::new(FakeIo::new(vec![(
            9,
            1_000_000_000,
            b"damaged".to_vec(),
            BUF_FLAG_ERROR | BUF_FLAG_TIMESTAMP_MONOTONIC,
        )]));
        let mut capture =
            Capture::with_io(io.clone(), "/dev/video0", 3840, 1080, 60, "mjpg", 2).unwrap();
        capture.start().unwrap();
        assert_eq!(
            capture.read(Duration::from_secs(1)).unwrap_err().code,
            "bad_frame"
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
}
