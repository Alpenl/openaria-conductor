use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::VecDeque;
use std::ffi::CString;
use std::fs;
use std::io;
use std::os::fd::RawFd;
use std::os::raw::c_ulong;
use std::path::Path;
use std::sync::Mutex;
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
pub(crate) struct ImuError {
    pub(crate) code: &'static str,
    pub(crate) message: String,
    pub(crate) retryable: bool,
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
pub(crate) struct RawVector3 {
    pub(crate) x: i16,
    pub(crate) y: i16,
    pub(crate) z: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImuSample {
    pub(crate) sequence: u64,
    pub(crate) packet_sequence: u64,
    pub(crate) sample_index: u8,
    pub(crate) device_timestamp_raw: u32,
    pub(crate) device_ticks: u64,
    pub(crate) host_read_start_ns: u64,
    pub(crate) host_read_end_ns: u64,
    pub(crate) host_monotonic_ns: u64,
    pub(crate) accelerometer: RawVector3,
    pub(crate) gyroscope: RawVector3,
    pub(crate) sync_offset_ns: Option<i64>,
    pub(crate) sync_residual_ns: Option<u64>,
    pub(crate) sync_quality: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImuObservation {
    pub(crate) samples: [ImuSample; SAMPLES_PER_PACKET],
    pub(crate) dropped_samples: u64,
}

pub(crate) fn observation_dict(py: Python<'_>, result: &ImuObservation) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("dropped_samples", result.dropped_samples)?;
    let samples = PyList::empty(py);
    for sample in &result.samples {
        let item = PyDict::new(py);
        item.set_item("sequence", sample.sequence)?;
        item.set_item("packet_sequence", sample.packet_sequence)?;
        item.set_item("sample_index", sample.sample_index)?;
        item.set_item("device_timestamp_raw", sample.device_timestamp_raw)?;
        item.set_item("device_ticks", sample.device_ticks)?;
        item.set_item("host_read_start_ns", sample.host_read_start_ns)?;
        item.set_item("host_read_end_ns", sample.host_read_end_ns)?;
        item.set_item("host_monotonic_ns", sample.host_monotonic_ns)?;

        let raw = PyDict::new(py);
        raw.set_item(
            "accelerometer",
            vec![
                sample.accelerometer.x,
                sample.accelerometer.y,
                sample.accelerometer.z,
            ],
        )?;
        raw.set_item(
            "gyroscope",
            vec![sample.gyroscope.x, sample.gyroscope.y, sample.gyroscope.z],
        )?;
        item.set_item("raw", raw)?;

        let sync = PyDict::new(py);
        sync.set_item("offset_ns", sample.sync_offset_ns)?;
        sync.set_item("residual_ns", sample.sync_residual_ns)?;
        sync.set_item("quality", sample.sync_quality)?;
        item.set_item("sync", sync)?;

        samples.append(item)?;
    }
    value.set_item("samples", samples)?;
    Ok(value.unbind())
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

#[derive(Debug, Clone, Copy)]
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

pub(crate) struct Collector {
    state: Mutex<State>,
}

struct State {
    fd: Option<RawFd>,
    unit: u8,
    selector: u8,
    stale_poll_interval: Duration,
    last_device_timestamp: Option<u32>,
    latest_observation: Option<ImuObservation>,
    unwrapper: TimestampUnwrapper,
    synchronizer: TimeSynchronizer,
    packet_sequence: u64,
    sample_sequence: u64,
}

impl Collector {
    pub(crate) fn open(
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
        let mut state = State {
            fd: Some(fd),
            unit,
            selector,
            stale_poll_interval: stale_poll_interval.unwrap_or(DEFAULT_STALE_POLL),
            last_device_timestamp: None,
            latest_observation: None,
            unwrapper: TimestampUnwrapper::new(DEVICE_TIMESTAMP_MODULUS),
            synchronizer: TimeSynchronizer::default(),
            packet_sequence: 0,
            sample_sequence: 0,
        };
        if let Err(error) = state.verify_packet_length() {
            state.close();
            return Err(error);
        }
        Ok(Self {
            state: Mutex::new(state),
        })
    }

    pub(crate) fn read(&self, timeout: Duration) -> Result<ImuObservation, ImuError> {
        if timeout.is_zero() {
            return Err(ImuError::new(
                "invalid_argument",
                "IMU timeout must be positive",
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ImuError::new("native_imu_poisoned", "IMU mutex is poisoned"))?;
        let result = state.read(timeout);
        if result.is_err() {
            state.close();
        }
        result
    }

    pub(crate) fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.close();
        }
    }

    pub(crate) fn unit(&self) -> Result<u8, ImuError> {
        self.state
            .lock()
            .map(|state| state.unit)
            .map_err(|_| ImuError::new("native_imu_poisoned", "IMU mutex is poisoned"))
    }

    pub(crate) fn latest_observation(&self) -> Result<Option<ImuObservation>, ImuError> {
        self.state
            .lock()
            .map(|state| state.latest_observation.clone())
            .map_err(|_| ImuError::new("native_imu_poisoned", "IMU mutex is poisoned"))
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        self.close();
    }
}

impl State {
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
                    "YLX XU packet must be {PACKET_BYTES} bytes, device reports {packet_bytes}"
                ),
            ));
        }
        Ok(())
    }

    fn read(&mut self, timeout: Duration) -> Result<ImuObservation, ImuError> {
        let packet_read = self.read_fresh_packet(timeout)?;
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
        let observation = ImuObservation {
            samples,
            dropped_samples: 0,
        };
        self.latest_observation = Some(observation.clone());
        Ok(observation)
    }

    fn read_fresh_packet(&mut self, timeout: Duration) -> Result<PacketRead, ImuError> {
        let deadline = monotonic_ns()?.saturating_add(duration_ns(timeout)?);
        loop {
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
            let device_timestamp = device_timestamp(&payload);
            if Some(device_timestamp) != self.last_device_timestamp {
                self.last_device_timestamp = Some(device_timestamp);
                let mut packet = [0_u8; PACKET_BYTES];
                packet.copy_from_slice(&payload);
                return Ok(PacketRead {
                    payload: packet,
                    host_read_start_ns,
                    host_read_end_ns,
                });
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
        let result = unsafe { libc::ioctl(fd, uvc_ioctl_ctrl_query(), &mut query) };
        if result != 0 {
            if result > 0 {
                let message = format!("UVC XU query returned non-zero status {result}");
                if error_code == "disconnected" {
                    return Err(ImuError::retryable(
                        "disconnected",
                        format!("IMU read failed: {message}"),
                    ));
                }
                return Err(ImuError::new(error_code, message));
            }
            let error = io::Error::last_os_error();
            if error_code == "disconnected" {
                return Err(ImuError::retryable(
                    "disconnected",
                    format!("IMU read failed: {error}"),
                ));
            }
            return Err(ImuError::io(error_code, "UVC XU query failed", error));
        }
        Ok(buffer)
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
    let fd = unsafe { libc::open(device.as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(ImuError::io(
            "disconnected",
            "open IMU video device",
            io::Error::last_os_error(),
        ));
    }
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
            return Err(ImuError::retryable(
                "timestamp_stalled",
                "device time did not advance",
            ));
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
    minimum_points: usize,
    window_points: usize,
    good_residual_ns: u64,
}

impl Default for TimeSynchronizer {
    fn default() -> Self {
        Self {
            points: VecDeque::with_capacity(64),
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
            if device_ticks <= previous.device_ticks || host_ns <= previous.host_ns {
                return Err(ImuError::retryable(
                    "non_monotonic_time",
                    "time evidence must move strictly forward",
                ));
            }
        }
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
                (point.host_ns as f64 - (offset + scale * point.device_ticks as f64)).abs()
            })
            .fold(0.0_f64, f64::max);
        let uncertainty = self
            .points
            .iter()
            .map(|point| point.uncertainty_ns)
            .max()
            .unwrap_or(0);
        let residual = fit_residual.ceil() as u64 + uncertainty;
        Ok(SyncEstimate {
            offset_ns: Some(offset.round() as i64),
            residual_ns: Some(residual),
            quality: if residual > self.good_residual_ns {
                "degraded"
            } else {
                "good"
            },
        })
    }
}

fn monotonic_ns() -> Result<u64, ImuError> {
    let mut timestamp = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut timestamp) } != 0 {
        return Err(ImuError::io(
            "clock_failed",
            "clock_gettime",
            io::Error::last_os_error(),
        ));
    }
    Ok(timestamp.tv_sec as u64 * 1_000_000_000 + timestamp.tv_nsec as u64)
}

fn duration_ns(duration: Duration) -> Result<u64, ImuError> {
    u64::try_from(duration.as_nanos())
        .map_err(|_| ImuError::new("invalid_argument", "IMU timeout is too large"))
}

fn midpoint_ns(start_ns: u64, end_ns: u64) -> u64 {
    start_ns + (end_ns - start_ns) / 2
}

pub(crate) fn available() -> bool {
    cfg!(target_os = "linux")
}

#[cfg(test)]
mod tests {
    use super::{
        DEVICE_TIMESTAMP_MODULUS, PACKET_BYTES, TimeSynchronizer, TimestampUnwrapper,
        UVC_CS_INTERFACE, UVC_EXTENSION_UNIT, UVC_GET_CUR, UVC_GET_LEN, XU_GUID_BYTES,
        decode_packet, find_uvc_xu_unit, validate_xu_query,
    };

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
    fn synchronizer_reports_insufficient_then_good() {
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
    }
}
