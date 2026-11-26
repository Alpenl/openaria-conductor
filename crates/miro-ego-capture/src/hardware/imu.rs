use miro_ego_core::sys::clock;
use std::collections::VecDeque;
use std::ffi::CString;
use std::fs;
use std::io;
use std::os::fd::RawFd;
use std::os::raw::c_ulong;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const UVC_GET_CUR: u8 = 0x81;
const UVC_GET_LEN: u8 = 0x85;
const UVC_CS_INTERFACE: u8 = 0x24;
const UVC_EXTENSION_UNIT: u8 = 0x06;
const PACKET_BYTES: usize = 27;
const GENERIC_XU_PAYLOAD_MAX: usize = 256;
const KNOWN_IMU_UNIT: u8 = 3;
const KNOWN_IMU_SELECTOR: u8 = 1;
const SAMPLES_PER_PACKET: usize = 2;
const DEVICE_TIMESTAMP_MODULUS: u64 = 1 << 24;
const DEFAULT_STALE_POLL: Duration = Duration::from_millis(1);
const XU_GUID_BYTES: [u8; 16] = [
    0x2c, 0xf4, 0xc2, 0xd5, 0x08, 0x18, 0x9f, 0x4d, 0xbe, 0x56, 0x75, 0x3e, 0x27, 0x1c, 0x92, 0x44,
];

#[repr(C)]
struct UvcXuControlQuery {
    unit: u8,
    selector: u8,
    query: u8,
    size: u16,
    data: *mut u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImuError {
    pub code: &'static str,
    pub message: String,
    pub retryable: bool,
}

impl ImuError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: false,
        }
    }

    fn retryable(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: true,
        }
    }

    fn io(code: &'static str, context: &str, error: io::Error) -> Self {
        Self::new(code, format!("{context}: {error}"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawVector3 {
    pub x: i16,
    pub y: i16,
    pub z: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImuSample {
    pub sequence: u64,
    pub packet_sequence: u64,
    pub sample_index: u8,
    pub device_timestamp_raw: u32,
    pub device_ticks: u64,
    pub host_read_start_ns: u64,
    pub host_read_end_ns: u64,
    pub host_monotonic_ns: u64,
    pub accelerometer: RawVector3,
    pub gyroscope: RawVector3,
    pub sync_offset_ns: Option<i64>,
    pub sync_residual_ns: Option<u64>,
    pub sync_quality: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImuObservation {
    pub samples: [ImuSample; SAMPLES_PER_PACKET],
    pub dropped_samples: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PacketRead {
    payload: [u8; PACKET_BYTES],
    host_read_start_ns: u64,
    host_read_end_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedRawSample {
    sample_index: u8,
    accelerometer: RawVector3,
    gyroscope: RawVector3,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedPacket {
    device_timestamp_raw: u32,
    samples: [DecodedRawSample; SAMPLES_PER_PACKET],
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct SyncEstimate {
    offset_ns: Option<i64>,
    residual_ns: Option<u64>,
    quality: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct SyncPoint {
    device_ticks: u64,
    host_ns: u64,
    uncertainty_ns: u64,
}

pub struct Collector {
    io: Mutex<IoState>,
    latest_observation: Mutex<Option<ImuObservation>>,
    stop: AtomicBool,
}

struct IoState {
    fd: Option<RawFd>,
    unit: u8,
    selector: u8,
    stale_poll_interval: Duration,
    last_payload: Option<[u8; PACKET_BYTES]>,
    unwrapper: TimestampUnwrapper,
    synchronizer: TimeSynchronizer,
    packet_sequence: u64,
    sample_sequence: u64,
}

impl Collector {
    pub fn open(
        device: &str,
        unit: Option<u8>,
        selector: u8,
        stale_poll_interval: Option<Duration>,
    ) -> Result<Self, ImuError> {
        if selector == 0 {
            return Err(ImuError::new(
                "invalid_argument",
                "UVC XU selector must be non-zero",
            ));
        }
        let unit = match unit {
            Some(0) => {
                return Err(ImuError::new(
                    "invalid_argument",
                    "UVC XU unit must be non-zero",
                ));
            }
            Some(value) => value,
            None => discover_uvc_xu_unit(device)?,
        };
        let fd = open_device(device)?;
        let mut io = IoState {
            fd: Some(fd),
            unit,
            selector,
            stale_poll_interval: stale_poll_interval.unwrap_or(DEFAULT_STALE_POLL),
            last_payload: None,
            unwrapper: TimestampUnwrapper::new(DEVICE_TIMESTAMP_MODULUS),
            synchronizer: TimeSynchronizer::default(),
            packet_sequence: 0,
            sample_sequence: 0,
        };
        if let Err(error) = io.verify_packet_length() {
            io.close();
            return Err(error);
        }
        Ok(Self {
            io: Mutex::new(io),
            latest_observation: Mutex::new(None),
            stop: AtomicBool::new(false),
        })
    }

    pub fn read(&self, timeout: Duration) -> Result<ImuObservation, ImuError> {
        if timeout.is_zero() {
            return Err(ImuError::new(
                "invalid_argument",
                "IMU timeout must be positive",
            ));
        }
        let result = {
            let mut io = self
                .io
                .lock()
                .map_err(|_| ImuError::new("native_imu_poisoned", "IMU mutex is poisoned"))?;
            if self.stop.load(Ordering::Acquire) {
                io.close();
                return Err(ImuError::new("invalid_state", "IMU source is closed"));
            }
            let result = io.read(timeout, &self.stop);
            match &result {
                Ok(observation) => {
                    if let Ok(mut latest) = self.latest_observation.lock() {
                        *latest = Some(observation.clone());
                    }
                    if self.stop.load(Ordering::Acquire) {
                        io.close();
                    }
                }
                Err(error) if error.retryable && !self.stop.load(Ordering::Acquire) => {}
                Err(_) => {
                    io.close();
                }
            }
            result
        };
        // Symmetric SeqCst check so a concurrent close() that missed try_lock()
        // while we held `io` never leaves the device descriptor open.
        if self.stop.load(Ordering::SeqCst) {
            if let Ok(mut io) = self.io.try_lock() {
                io.close();
            }
        }
        result
    }

    pub fn close(&self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Ok(mut io) = self.io.try_lock() {
            io.close();
        }
    }

    pub fn latest_observation(&self) -> Result<Option<ImuObservation>, ImuError> {
        self.latest_observation
            .lock()
            .map(|latest| latest.clone())
            .map_err(|_| ImuError::new("native_imu_poisoned", "IMU mutex is poisoned"))
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Ok(io) = self.io.get_mut() {
            io.close();
        }
    }
}

impl IoState {
    fn verify_packet_length(&mut self) -> Result<(), ImuError> {
        let payload = self.query(UVC_GET_LEN, 2, "xu_query_failed")?;
        if payload.len() != 2 {
            return Err(ImuError::new(
                "xu_query_failed",
                "UVC GET_LEN did not return two bytes",
            ));
        }
        let packet_bytes = u16::from_le_bytes([payload[0], payload[1]]);
        if usize::from(packet_bytes) != PACKET_BYTES {
            return Err(ImuError::new(
                "unsupported_packet_length",
                format!(
                    "Miro-Ego XU packet must be {PACKET_BYTES} bytes, device reports {packet_bytes}"
                ),
            ));
        }
        Ok(())
    }

    fn read(&mut self, timeout: Duration, stop: &AtomicBool) -> Result<ImuObservation, ImuError> {
        let packet_read = self.read_fresh_packet(timeout, stop)?;
        self.observe_packet(packet_read)
    }

    fn observe_packet(&mut self, packet_read: PacketRead) -> Result<ImuObservation, ImuError> {
        let packet = decode_packet(&packet_read.payload);
        let device_ticks = self
            .unwrapper
            .update(u64::from(packet.device_timestamp_raw))?;
        let estimate = self.synchronizer.add(
            device_ticks,
            packet_read.host_read_start_ns,
            packet_read.host_read_end_ns,
        )?;
        let host_ns = midpoint_ns(packet_read.host_read_start_ns, packet_read.host_read_end_ns);
        let packet_sequence = self.packet_sequence;
        let first_sequence = self.sample_sequence;
        let samples = packet.samples.map(|raw| {
            let sequence = first_sequence + u64::from(raw.sample_index);
            ImuSample {
                sequence,
                packet_sequence,
                sample_index: raw.sample_index,
                device_timestamp_raw: packet.device_timestamp_raw,
                device_ticks,
                host_read_start_ns: packet_read.host_read_start_ns,
                host_read_end_ns: packet_read.host_read_end_ns,
                host_monotonic_ns: host_ns,
                accelerometer: raw.accelerometer,
                gyroscope: raw.gyroscope,
                sync_offset_ns: estimate.offset_ns,
                sync_residual_ns: estimate.residual_ns,
                sync_quality: estimate.quality,
            }
        });
        self.packet_sequence += 1;
        self.sample_sequence += SAMPLES_PER_PACKET as u64;
        Ok(ImuObservation {
            samples,
            dropped_samples: 0,
        })
    }

    fn read_fresh_packet(
        &mut self,
        timeout: Duration,
        stop: &AtomicBool,
    ) -> Result<PacketRead, ImuError> {
        let deadline = monotonic_ns()?.saturating_add(duration_ns(timeout)?);
        loop {
            if stop.load(Ordering::Acquire) {
                return Err(ImuError::new("invalid_state", "IMU source is closed"));
            }
            let host_read_start_ns = monotonic_ns()?;
            let payload = self.query(UVC_GET_CUR, PACKET_BYTES, "disconnected")?;
            let host_read_end_ns = monotonic_ns()?;
            if payload.len() != PACKET_BYTES {
                return Err(ImuError::new(
                    "invalid_packet_length",
                    format!(
                        "UVC GET_CUR returned {} bytes instead of {PACKET_BYTES}",
                        payload.len()
                    ),
                ));
            }
            let mut packet = [0_u8; PACKET_BYTES];
            packet.copy_from_slice(&payload);
            if self.accept_payload(packet) {
                return Ok(PacketRead {
                    payload: packet,
                    host_read_start_ns,
                    host_read_end_ns,
                });
            }
            if stop.load(Ordering::Acquire) {
                return Err(ImuError::new("invalid_state", "IMU source is closed"));
            }
            if host_read_end_ns >= deadline {
                return Err(ImuError::retryable(
                    "sensor_stalled",
                    "IMU did not advance before timeout",
                ));
            }
            let remaining = Duration::from_nanos(deadline - host_read_end_ns);
            std::thread::sleep(self.stale_poll_interval.min(remaining));
        }
    }

    fn accept_payload(&mut self, payload: [u8; PACKET_BYTES]) -> bool {
        // Frame numbers can repeat while both IMU slots continue updating.
        if self.last_payload == Some(payload) {
            return false;
        }
        self.last_payload = Some(payload);
        true
    }

    fn query(
        &mut self,
        request: u8,
        size: usize,
        error_code: &'static str,
    ) -> Result<Vec<u8>, ImuError> {
        if size == 0 || size > u16::MAX as usize {
            return Err(ImuError::new(
                "invalid_argument",
                "UVC XU query size is out of range",
            ));
        }
        validate_xu_query(self.unit, self.selector, request, size)?;
        let fd = self
            .fd
            .ok_or_else(|| ImuError::new("invalid_state", "IMU source is closed"))?;
        let mut buffer = vec![0_u8; size];
        let mut query = UvcXuControlQuery {
            unit: self.unit,
            selector: self.selector,
            query: request,
            size: size as u16,
            data: buffer.as_mut_ptr(),
        };
        let status = loop {
            let result = unsafe { libc::ioctl(fd, uvc_ioctl_ctrl_query(), &mut query) };
            if result == 0 {
                break Ok(());
            }
            if result > 0 {
                break Err(Ok(result));
            }
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            break Err(Err(error));
        };
        match status {
            Ok(()) => Ok(buffer),
            Err(Ok(result)) => {
                let message = format!("UVC XU query returned non-zero status {result}");
                if error_code == "disconnected" {
                    return Err(ImuError::retryable(
                        "disconnected",
                        format!("IMU read failed: {message}"),
                    ));
                }
                Err(ImuError::new(error_code, message))
            }
            Err(Err(error)) => {
                if error_code == "disconnected" {
                    return Err(ImuError::retryable(
                        "disconnected",
                        format!("IMU read failed: {error}"),
                    ));
                }
                Err(ImuError::io(error_code, "UVC XU query failed", error))
            }
        }
    }

    fn close(&mut self) {
        if let Some(fd) = self.fd.take() {
            unsafe {
                libc::close(fd);
            }
        }
    }
}

fn is_get_request(request: u8) -> bool {
    request & 0x80 != 0
}

fn validate_xu_query(unit: u8, selector: u8, request: u8, size: usize) -> Result<(), ImuError> {
    if !is_get_request(request) {
        return Err(ImuError::new(
            "xu_query_denied",
            "UVC XU SET requests are disabled",
        ));
    }
    if request == UVC_GET_CUR && unit == 4 && matches!(selector, 10 | 15) {
        return Err(ImuError::new(
            "xu_query_denied",
            "UVC XU GET_CUR is denied for this unit/selector",
        ));
    }
    if request == UVC_GET_CUR && unit == KNOWN_IMU_UNIT && selector == KNOWN_IMU_SELECTOR {
        if size != PACKET_BYTES {
            return Err(ImuError::new(
                "xu_query_denied",
                format!("known IMU UVC XU GET_CUR must request exactly {PACKET_BYTES} bytes"),
            ));
        }
        return Ok(());
    }
    if size > GENERIC_XU_PAYLOAD_MAX {
        return Err(ImuError::new(
            "xu_query_denied",
            format!("unknown UVC XU payloads are limited to {GENERIC_XU_PAYLOAD_MAX} bytes"),
        ));
    }
    Ok(())
}

fn open_device(device: &str) -> Result<RawFd, ImuError> {
    let device = CString::new(device)
        .map_err(|_| ImuError::new("invalid_argument", "device contains NUL"))?;
    let fd = loop {
        let fd = unsafe { libc::open(device.as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
        if fd >= 0 {
            break fd;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(ImuError::io("disconnected", "open IMU video device", error));
        }
    };
    Ok(fd)
}

fn uvc_ioctl_ctrl_query() -> c_ulong {
    const IOC_NRBITS: u64 = 8;
    const IOC_TYPEBITS: u64 = 8;
    const IOC_SIZEBITS: u64 = 14;
    const IOC_NRSHIFT: u64 = 0;
    const IOC_TYPESHIFT: u64 = IOC_NRSHIFT + IOC_NRBITS;
    const IOC_SIZESHIFT: u64 = IOC_TYPESHIFT + IOC_TYPEBITS;
    const IOC_DIRSHIFT: u64 = IOC_SIZESHIFT + IOC_SIZEBITS;
    const IOC_READ_WRITE: u64 = 3;
    ((IOC_READ_WRITE << IOC_DIRSHIFT)
        | ((std::mem::size_of::<UvcXuControlQuery>() as u64) << IOC_SIZESHIFT)
        | ((b'u' as u64) << IOC_TYPESHIFT)
        | (0x21 << IOC_NRSHIFT)) as c_ulong
}

fn discover_uvc_xu_unit(device: &str) -> Result<u8, ImuError> {
    let name = Path::new(device)
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| ImuError::new("xu_discovery_unavailable", "invalid video device path"))?;
    let class_entry = Path::new("/sys")
        .join("class")
        .join("video4linux")
        .join(name)
        .join("device");
    let resolved = fs::canonicalize(&class_entry).map_err(|error| {
        ImuError::io(
            "xu_discovery_unavailable",
            "locate video device USB sysfs descriptor",
            error,
        )
    })?;
    for parent in resolved.ancestors() {
        let descriptor_path = parent.join("descriptors");
        let descriptors = match fs::read(&descriptor_path) {
            Ok(value) => value,
            Err(_) => continue,
        };
        match find_uvc_xu_unit(&descriptors) {
            Ok(unit) => return Ok(unit),
            Err(error) if error.code == "xu_not_found" => continue,
            Err(error) => return Err(error),
        }
    }
    Err(ImuError::new(
        "xu_not_found",
        format!("UVC XU GUID was not found for {device}"),
    ))
}

fn parse_uvc_extension_units(descriptors: &[u8]) -> Result<Vec<(u8, [u8; 16])>, ImuError> {
    let mut units = Vec::new();
    let mut offset = 0_usize;
    while offset < descriptors.len() {
        if descriptors.len() - offset < 2 {
            return Err(ImuError::new(
                "xu_descriptor_invalid",
                "USB descriptor has a truncated header",
            ));
        }
        let length = usize::from(descriptors[offset]);
        if length < 2 || offset + length > descriptors.len() {
            return Err(ImuError::new(
                "xu_descriptor_invalid",
                "USB descriptor has an invalid length",
            ));
        }
        if descriptors[offset + 1] == UVC_CS_INTERFACE
            && length >= 20
            && descriptors[offset + 2] == UVC_EXTENSION_UNIT
        {
            let mut guid = [0_u8; 16];
            guid.copy_from_slice(&descriptors[offset + 4..offset + 20]);
            units.push((descriptors[offset + 3], guid));
        }
        offset += length;
    }
    Ok(units)
}

fn find_uvc_xu_unit(descriptors: &[u8]) -> Result<u8, ImuError> {
    let mut selected = None;
    for (unit, guid) in parse_uvc_extension_units(descriptors)? {
        if guid == XU_GUID_BYTES {
            if selected.is_some() {
                return Err(ImuError::new(
                    "xu_ambiguous",
                    "UVC XU GUID matched multiple units",
                ));
            }
            selected = Some(unit);
        }
    }
    selected.ok_or_else(|| ImuError::new("xu_not_found", "UVC XU GUID was not found"))
}

fn decode_packet(payload: &[u8; PACKET_BYTES]) -> DecodedPacket {
    let device_timestamp_raw = device_timestamp(payload);
    let mut axes = [0_i16; 12];
    for (index, chunk) in payload[3..].chunks_exact(2).enumerate() {
        axes[index] = i16::from_be_bytes([chunk[0], chunk[1]]);
    }
    let sample = |index: usize| {
        let start = index * 6;
        DecodedRawSample {
            sample_index: index as u8,
            accelerometer: RawVector3 {
                x: axes[start],
                y: axes[start + 1],
                z: axes[start + 2],
            },
            gyroscope: RawVector3 {
                x: axes[start + 3],
                y: axes[start + 4],
                z: axes[start + 5],
            },
        }
    };
    DecodedPacket {
        device_timestamp_raw,
        samples: [sample(0), sample(1)],
    }
}

fn device_timestamp(payload: &[u8]) -> u32 {
    (u32::from(payload[0]) << 16) | (u32::from(payload[1]) << 8) | u32::from(payload[2])
}

struct TimestampUnwrapper {
    modulus: u64,
    raw: Option<u64>,
    unwrapped: Option<u64>,
}

impl TimestampUnwrapper {
    fn new(modulus: u64) -> Self {
        Self {
            modulus,
            raw: None,
            unwrapped: None,
        }
    }

    fn update(&mut self, raw: u64) -> Result<u64, ImuError> {
        if raw >= self.modulus {
            return Err(ImuError::new(
                "invalid_timestamp",
                "device time exceeds 24-bit range",
            ));
        }
        let Some(previous_raw) = self.raw else {
            self.raw = Some(raw);
            self.unwrapped = Some(raw);
            return Ok(raw);
        };
        let delta = (raw + self.modulus - previous_raw) % self.modulus;
        if delta == 0 {
            return Ok(self.unwrapped.unwrap_or(raw));
        }
        if delta > self.modulus / 2 {
            return Err(ImuError::retryable(
                "timestamp_regression",
                "device time regressed",
            ));
        }
        let current = self.unwrapped.unwrap_or(raw).saturating_add(delta);
        self.raw = Some(raw);
        self.unwrapped = Some(current);
        Ok(current)
    }
}

struct TimeSynchronizer {
    points: VecDeque<SyncPoint>,
    last_host_ns: Option<u64>,
    minimum_points: usize,
    window_points: usize,
    good_residual_ns: u64,
}

impl Default for TimeSynchronizer {
    fn default() -> Self {
        Self {
            points: VecDeque::with_capacity(64),
            last_host_ns: None,
            minimum_points: 3,
            window_points: 64,
            good_residual_ns: 1_000_000,
        }
    }
}

impl TimeSynchronizer {
    fn add(
        &mut self,
        device_ticks: u64,
        host_read_start_ns: u64,
        host_read_end_ns: u64,
    ) -> Result<SyncEstimate, ImuError> {
        if host_read_end_ns < host_read_start_ns {
            return Err(ImuError::new(
                "invalid_time_evidence",
                "device time or host read interval is invalid",
            ));
        }
        let host_ns = midpoint_ns(host_read_start_ns, host_read_end_ns);
        if let Some(previous) = self.points.back() {
            if device_ticks < previous.device_ticks
                || self.last_host_ns.is_some_and(|last| host_ns <= last)
            {
                return Err(ImuError::retryable(
                    "non_monotonic_time",
                    "time evidence must move strictly forward",
                ));
            }
            self.last_host_ns = Some(host_ns);
            if device_ticks == previous.device_ticks {
                // This response has no independent device-time anchor. Do not
                // reuse a frame fit as if it timed the new IMU samples.
                return Ok(SyncEstimate {
                    offset_ns: None,
                    residual_ns: None,
                    quality: "insufficient",
                });
            }
        }
        self.last_host_ns = Some(host_ns);
        if self.points.len() == self.window_points {
            self.points.pop_front();
        }
        self.points.push_back(SyncPoint {
            device_ticks,
            host_ns,
            uncertainty_ns: (host_read_end_ns - host_read_start_ns) / 2,
        });
        self.estimate()
    }

    fn estimate(&self) -> Result<SyncEstimate, ImuError> {
        let count = self.points.len();
        if count < self.minimum_points {
            return Ok(SyncEstimate {
                offset_ns: None,
                residual_ns: None,
                quality: "insufficient",
            });
        }
        let count_f64 = count as f64;
        let mean_x = self
            .points
            .iter()
            .map(|point| point.device_ticks as f64)
            .sum::<f64>()
            / count_f64;
        let mean_y = self
            .points
            .iter()
            .map(|point| point.host_ns as f64)
            .sum::<f64>()
            / count_f64;
        let denominator = self
            .points
            .iter()
            .map(|point| {
                let x = point.device_ticks as f64 - mean_x;
                x * x
            })
            .sum::<f64>();
        if denominator == 0.0 {
            return Ok(SyncEstimate {
                offset_ns: None,
                residual_ns: None,
                quality: "insufficient",
            });
        }
        let scale = self
            .points
            .iter()
            .map(|point| (point.device_ticks as f64 - mean_x) * (point.host_ns as f64 - mean_y))
            .sum::<f64>()
            / denominator;
        if !scale.is_finite() || scale <= 0.0 {
            return Err(ImuError::new(
                "invalid_clock_fit",
                "device clock fit slope is invalid",
            ));
        }
        let offset = mean_y - scale * mean_x;
        let fit_residual = self
            .points
            .iter()
            .map(|point| {
                ((point.host_ns as f64 - mean_y) - scale * (point.device_ticks as f64 - mean_x))
                    .abs()
            })
            .fold(0.0_f64, f64::max);
        let uncertainty = self
            .points
            .iter()
            .map(|point| point.uncertainty_ns)
            .max()
            .unwrap_or(0);
        let residual = (fit_residual.ceil() as u64).saturating_add(uncertainty);
        let quality = if residual > self.good_residual_ns {
            "degraded"
        } else {
            "good"
        };
        Ok(SyncEstimate {
            offset_ns: Some(offset.round() as i64),
            residual_ns: Some(residual),
            quality,
        })
    }
}

fn monotonic_ns() -> Result<u64, ImuError> {
    clock::monotonic_ns().map_err(|error| ImuError::io("clock_failed", "clock_gettime", error))
}

fn duration_ns(duration: Duration) -> Result<u64, ImuError> {
    u64::try_from(duration.as_nanos())
        .map_err(|_| ImuError::new("invalid_argument", "IMU timeout is too large"))
}

fn midpoint_ns(start_ns: u64, end_ns: u64) -> u64 {
    start_ns + (end_ns - start_ns) / 2
}

pub fn available() -> bool {
    cfg!(target_os = "linux")
}

#[cfg(test)]
mod tests {
    use super::{
        Collector, DEVICE_TIMESTAMP_MODULUS, IoState, PACKET_BYTES, PacketRead, TimeSynchronizer,
        TimestampUnwrapper, UVC_CS_INTERFACE, UVC_EXTENSION_UNIT, UVC_GET_CUR, UVC_GET_LEN,
        XU_GUID_BYTES, decode_packet, find_uvc_xu_unit, validate_xu_query,
    };
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    fn packet(timestamp: u32, seed: i16) -> [u8; PACKET_BYTES] {
        let mut payload = [0_u8; PACKET_BYTES];
        payload[0..3].copy_from_slice(&timestamp.to_be_bytes()[1..4]);
        for index in 0..12 {
            let value = seed + index as i16;
            payload[3 + index * 2..5 + index * 2].copy_from_slice(&value.to_be_bytes());
        }
        payload
    }

    #[test]
    fn decodes_fixed_packet() {
        let payload = packet(0x123456, -6);
        let decoded = decode_packet(&payload);
        assert_eq!(decoded.device_timestamp_raw, 0x123456);
        assert_eq!(decoded.samples[0].accelerometer.x, -6);
        assert_eq!(decoded.samples[0].gyroscope.z, -1);
        assert_eq!(decoded.samples[1].accelerometer.x, 0);
        assert_eq!(decoded.samples[1].gyroscope.z, 5);
    }

    #[test]
    fn same_frame_updates_survive_filter_clock_and_observation() {
        let mut state = IoState {
            fd: None,
            unit: 3,
            selector: 1,
            stale_poll_interval: Duration::from_millis(1),
            last_payload: None,
            unwrapper: TimestampUnwrapper::new(DEVICE_TIMESTAMP_MODULUS),
            synchronizer: TimeSynchronizer::default(),
            packet_sequence: 0,
            sample_sequence: 0,
        };
        let mut samples = Vec::new();
        let mut last_observation = None;
        for (index, (frame, seed)) in [(0xFF_FFFF, -6), (0xFF_FFFF, -5), (0, -4), (3, -3)]
            .into_iter()
            .enumerate()
        {
            let payload = packet(frame, seed);
            assert!(state.accept_payload(payload));
            assert!(!state.accept_payload(payload));
            let start = 1_000_000_000 + index as u64 * 5_000_000;
            let observation = state
                .observe_packet(PacketRead {
                    payload,
                    host_read_start_ns: start,
                    host_read_end_ns: start + 4_800_000,
                })
                .unwrap();
            assert_eq!(observation.dropped_samples, 0);
            assert_eq!(observation.samples[0].packet_sequence, index as u64);
            assert_eq!(observation.samples[0].accelerometer.x, seed);
            assert_eq!(observation.samples[1].gyroscope.z, seed + 11);
            if index == 1 {
                assert_eq!(observation.samples[0].sync_quality, "insufficient");
                assert_eq!(observation.samples[0].sync_offset_ns, None);
            }
            samples.extend(observation.samples.clone());
            last_observation = Some(observation);
        }
        assert_eq!(
            samples
                .iter()
                .map(|sample| sample.sequence)
                .collect::<Vec<_>>(),
            (0..8).collect::<Vec<_>>()
        );
        assert_eq!(samples[4].device_ticks, 0x1_000000);
        assert_eq!(samples[6].device_ticks, 0x1_000003);
        assert_eq!(last_observation.unwrap().samples[1].sequence, 7);
    }

    #[test]
    fn latest_observation_and_close_never_block_on_io_lock() {
        let mut io = IoState {
            fd: None,
            unit: 3,
            selector: 1,
            stale_poll_interval: Duration::from_millis(1),
            last_payload: None,
            unwrapper: TimestampUnwrapper::new(DEVICE_TIMESTAMP_MODULUS),
            synchronizer: TimeSynchronizer::default(),
            packet_sequence: 0,
            sample_sequence: 0,
        };
        let obs = io
            .observe_packet(PacketRead {
                payload: packet(10, 1),
                host_read_start_ns: 1_000,
                host_read_end_ns: 2_000,
            })
            .unwrap();
        let collector = Collector {
            io: Mutex::new(io),
            latest_observation: Mutex::new(Some(obs.clone())),
            stop: AtomicBool::new(false),
        };
        let _io_guard = collector.io.lock().unwrap();
        let started = Instant::now();
        assert_eq!(collector.latest_observation().unwrap(), Some(obs));
        collector.close();
        assert!(started.elapsed() < Duration::from_millis(50));
        assert!(collector.stop.load(std::sync::atomic::Ordering::Acquire));
    }

    #[test]
    fn finds_matching_uvc_extension_unit() {
        let mut extension = vec![24, UVC_CS_INTERFACE, UVC_EXTENSION_UNIT, 7];
        extension.extend_from_slice(&XU_GUID_BYTES);
        extension.extend_from_slice(&[0, 0, 0, 0]);
        let mut descriptors = vec![4, 4, 0, 0];
        descriptors.extend_from_slice(&extension);
        assert_eq!(find_uvc_xu_unit(&descriptors).unwrap(), 7);
    }

    #[test]
    fn validates_fail_closed_xu_query_shape() {
        assert!(validate_xu_query(3, 1, UVC_GET_CUR, PACKET_BYTES).is_ok());
        assert!(validate_xu_query(3, 1, UVC_GET_LEN, 2).is_ok());

        assert_eq!(
            validate_xu_query(4, 9, 0x01, 1).unwrap_err().code,
            "xu_query_denied"
        );
        assert_eq!(
            validate_xu_query(4, 10, UVC_GET_CUR, 16).unwrap_err().code,
            "xu_query_denied"
        );
        assert_eq!(
            validate_xu_query(4, 15, UVC_GET_CUR, 16).unwrap_err().code,
            "xu_query_denied"
        );
        assert_eq!(
            validate_xu_query(4, 9, UVC_GET_CUR, 257).unwrap_err().code,
            "xu_query_denied"
        );
        assert_eq!(
            validate_xu_query(3, 1, UVC_GET_CUR, PACKET_BYTES + 1)
                .unwrap_err()
                .code,
            "xu_query_denied"
        );
    }

    #[test]
    fn unwraps_rollover_and_rejects_regression() {
        let mut unwrapper = TimestampUnwrapper::new(256);
        assert_eq!(unwrapper.update(250).unwrap(), 250);
        assert_eq!(unwrapper.update(3).unwrap(), 259);
        let mut regressed = TimestampUnwrapper::new(256);
        regressed.update(10).unwrap();
        assert_eq!(
            regressed.update(9).unwrap_err().code,
            "timestamp_regression"
        );
        let mut full = TimestampUnwrapper::new(DEVICE_TIMESTAMP_MODULUS);
        assert_eq!(full.update(0xFF_FFFE).unwrap(), 0xFF_FFFE);
        assert_eq!(full.update(1).unwrap(), 0x1_000001);
    }

    #[test]
    fn synchronizer_reports_insufficient_then_good_and_degraded_residual() {
        let mut sync = TimeSynchronizer::default();
        assert_eq!(
            sync.add(1_000, 10_999_900, 11_000_100).unwrap().quality,
            "insufficient"
        );
        assert_eq!(
            sync.add(2_000, 11_999_900, 12_000_100).unwrap().quality,
            "insufficient"
        );
        let estimate = sync.add(3_000, 12_999_900, 13_000_100).unwrap();
        assert_eq!(estimate.quality, "good");
        assert_eq!(estimate.offset_ns, Some(10_000_000));
        assert_eq!(estimate.residual_ns, Some(100));

        let mut jittered = TimeSynchronizer::default();
        jittered.add(1_000, 10_999_900, 11_000_100).unwrap();
        jittered.add(2_000, 12_999_900, 13_000_100).unwrap();
        let degraded = jittered.add(3_000, 18_999_900, 19_000_100).unwrap();
        assert_eq!(degraded.quality, "degraded");
        assert!(degraded.residual_ns.unwrap() > 1_000_000);
    }

    #[test]
    fn collector_read_preserves_io_on_retryable_error_and_closes_on_fatal() {
        let collector = Collector {
            io: Mutex::new(IoState {
                fd: None,
                unit: 3,
                selector: 1,
                stale_poll_interval: Duration::from_millis(1),
                last_payload: None,
                unwrapper: TimestampUnwrapper::new(DEVICE_TIMESTAMP_MODULUS),
                synchronizer: TimeSynchronizer::default(),
                packet_sequence: 0,
                sample_sequence: 0,
            }),
            latest_observation: Mutex::new(None),
            stop: AtomicBool::new(false),
        };
        let mut pipe_fds = [-1_i32; 2];
        assert_eq!(unsafe { libc::pipe(pipe_fds.as_mut_ptr()) }, 0);
        let fd = pipe_fds[0];
        unsafe { libc::close(pipe_fds[1]) };
        collector.io.lock().unwrap().fd = Some(fd);
        let err = collector.read(Duration::from_millis(1)).unwrap_err();
        assert!(err.retryable);
        assert_eq!(err.code, "disconnected");
        assert_eq!(collector.io.lock().unwrap().fd, Some(fd));

        collector
            .stop
            .store(true, std::sync::atomic::Ordering::Release);
        let err = collector.read(Duration::from_millis(1)).unwrap_err();
        assert_eq!(err.code, "invalid_state");
        assert_eq!(collector.io.lock().unwrap().fd, None);
    }
}
