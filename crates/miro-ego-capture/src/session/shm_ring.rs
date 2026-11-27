//! Zero-copy `memfd_create` + `eventfd` shared-memory ring buffer IPC matching
//! `miro-ego-encoder` (`MEGO_SHM` v1) byte-for-byte.
//!
//! Binary layout (little-endian):
//! - Header (`4096` bytes at offset `0`):
//!   - `0..8`: `magic: [u8; 8] = *b"MEGO_SHM"`
//!   - `8..12`: `version: u32 = 1`
//!   - `12..16`: `slot_count: u32` (power of 2, `1..=256`, default `16`)
//!   - `16..20`: `slot_capacity: u32` (default `8 MiB`)
//!   - `20..24`: `flags: AtomicU32` (`bit 0` = EOF/shutdown)
//!   - `64..72`: `write_idx: AtomicU64` (producer monotonic)
//!   - `128..136`: `read_idx: AtomicU64` (consumer monotonic)
//!   - `192..200`: `dropped_frames: AtomicU64`
//! - Slot descriptor table at offset `4096`, `slot_count` entries of `64` bytes each (`#[repr(C)]`):
//!   - `0..8`: `frame_seq: u64`
//!   - `8..16`: `monotonic_ns: u64`
//!   - `16..20`: `payload_len: u32`
//!   - `20..24`: `flags: u32`
//!   - `24..32`: `data_offset: u64`
//!   - `32..64`: `reserved: [u8; 32]`
//! - Data region starts right after the slot table, 4096-byte aligned,
//!   occupying `slot_count * slot_capacity` bytes.

use miro_ego_core::sys::fs::{dup_cloexec, fstat};
use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub const SHM_MAGIC: [u8; 8] = *b"MEGO_SHM";
pub const SHM_VERSION: u32 = 1;
pub const SHM_HEADER_BYTES: usize = 4096;
pub const SHM_SLOT_DESC_BYTES: usize = 64;
pub const SHM_PAGE_ALIGN: usize = 4096;

pub const DEFAULT_SLOT_COUNT: u32 = 16;
pub const MAX_SLOT_COUNT: u32 = 256;
pub const DEFAULT_SLOT_CAPACITY: u32 = 8 * 1024 * 1024;
pub const MAX_SLOT_CAPACITY: u32 = 64 * 1024 * 1024;

/// Bit 0 of `ShmHeader::flags`: producer has reached EOF / shutdown.
pub const SHM_FLAG_EOF: u32 = 1 << 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShmRingConfig {
    pub slot_count: u32,
    pub slot_capacity: u32,
}

impl Default for ShmRingConfig {
    fn default() -> Self {
        Self {
            slot_count: DEFAULT_SLOT_COUNT,
            slot_capacity: DEFAULT_SLOT_CAPACITY,
        }
    }
}

impl ShmRingConfig {
    pub fn validate(self) -> Result<ShmLayout, ShmRingError> {
        ShmLayout::compute(self.slot_count, self.slot_capacity)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShmLayout {
    pub slot_count: u32,
    pub slot_capacity: u32,
    pub slot_table_offset: usize,
    pub data_region_offset: usize,
    pub total_bytes: usize,
}

impl ShmLayout {
    pub fn compute(slot_count: u32, slot_capacity: u32) -> Result<Self, ShmRingError> {
        if slot_count == 0 || slot_count > MAX_SLOT_COUNT || !slot_count.is_power_of_two() {
            return Err(ShmRingError::new(
                "invalid_shm_config",
                format!("slot_count must be a power of 2 in 1..={MAX_SLOT_COUNT}"),
            ));
        }
        if slot_capacity == 0 || slot_capacity > MAX_SLOT_CAPACITY {
            return Err(ShmRingError::new(
                "invalid_shm_config",
                format!("slot_capacity must be in 1..={MAX_SLOT_CAPACITY}"),
            ));
        }
        let table_bytes = (slot_count as usize)
            .checked_mul(SHM_SLOT_DESC_BYTES)
            .ok_or_else(|| ShmRingError::new("invalid_shm_config", "slot table size overflow"))?;
        let table_end = SHM_HEADER_BYTES
            .checked_add(table_bytes)
            .ok_or_else(|| ShmRingError::new("invalid_shm_config", "slot table offset overflow"))?;
        let data_region_offset = align_up(table_end, SHM_PAGE_ALIGN)
            .ok_or_else(|| ShmRingError::new("invalid_shm_config", "data alignment overflow"))?;
        let data_bytes = (slot_count as usize)
            .checked_mul(slot_capacity as usize)
            .ok_or_else(|| ShmRingError::new("invalid_shm_config", "data region size overflow"))?;
        let total_bytes = data_region_offset
            .checked_add(data_bytes)
            .ok_or_else(|| ShmRingError::new("invalid_shm_config", "total SHM size overflow"))?;
        Ok(Self {
            slot_count,
            slot_capacity,
            slot_table_offset: SHM_HEADER_BYTES,
            data_region_offset,
            total_bytes,
        })
    }

    pub fn slot_data_offset(&self, slot_index: u32) -> Option<usize> {
        if slot_index >= self.slot_count {
            return None;
        }
        let offset = (slot_index as usize).checked_mul(self.slot_capacity as usize)?;
        self.data_region_offset.checked_add(offset)
    }
}

fn align_up(value: usize, align: usize) -> Option<usize> {
    let rem = value % align;
    if rem == 0 {
        Some(value)
    } else {
        value.checked_add(align - rem)
    }
}

/// Shared-memory ring header (exactly 4096 bytes, 64-byte aligned atomics).
#[repr(C)]
pub struct ShmHeader {
    pub magic: [u8; 8],
    pub version: u32,
    pub slot_count: u32,
    pub slot_capacity: u32,
    pub flags: AtomicU32,
    _pad0: [u8; 40],
    pub write_idx: AtomicU64,
    _pad1: [u8; 56],
    pub read_idx: AtomicU64,
    _pad2: [u8; 56],
    pub dropped_frames: AtomicU64,
    _pad3: [u8; 3896],
}

/// Shared-memory slot descriptor (exactly 64 bytes).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShmSlotDescriptor {
    pub frame_seq: u64,
    pub monotonic_ns: u64,
    pub payload_len: u32,
    pub flags: u32,
    pub data_offset: u64,
    pub reserved: [u8; 32],
}

const _: () = {
    assert!(std::mem::size_of::<ShmHeader>() == SHM_HEADER_BYTES);
    assert!(std::mem::offset_of!(ShmHeader, magic) == 0);
    assert!(std::mem::offset_of!(ShmHeader, version) == 8);
    assert!(std::mem::offset_of!(ShmHeader, slot_count) == 12);
    assert!(std::mem::offset_of!(ShmHeader, slot_capacity) == 16);
    assert!(std::mem::offset_of!(ShmHeader, flags) == 20);
    assert!(std::mem::offset_of!(ShmHeader, write_idx) == 64);
    assert!(std::mem::offset_of!(ShmHeader, read_idx) == 128);
    assert!(std::mem::offset_of!(ShmHeader, dropped_frames) == 192);

    assert!(std::mem::size_of::<ShmSlotDescriptor>() == SHM_SLOT_DESC_BYTES);
    assert!(std::mem::offset_of!(ShmSlotDescriptor, frame_seq) == 0);
    assert!(std::mem::offset_of!(ShmSlotDescriptor, monotonic_ns) == 8);
    assert!(std::mem::offset_of!(ShmSlotDescriptor, payload_len) == 16);
    assert!(std::mem::offset_of!(ShmSlotDescriptor, flags) == 20);
    assert!(std::mem::offset_of!(ShmSlotDescriptor, data_offset) == 24);
    assert!(std::mem::offset_of!(ShmSlotDescriptor, reserved) == 32);
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShmRingError {
    pub code: &'static str,
    pub message: String,
}

impl ShmRingError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    fn io(code: &'static str, context: &str, error: io::Error) -> Self {
        Self::new(code, format!("{context}: {error}"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushStatus {
    Pushed { write_idx: u64, slot_index: u32 },
    Full,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumedFrame {
    pub frame_seq: u64,
    pub monotonic_ns: u64,
    pub flags: u32,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PopStatus {
    Frame(ConsumedFrame),
    Empty,
    Eof,
}

struct MmapRegion {
    ptr: NonNull<u8>,
    len: usize,
}

// SAFETY: Accesses to the shared-memory mapping are synchronized by header atomics
// (`write_idx`, `read_idx`, `flags`) with Acquire/Release ordering; slots are exclusively
// owned by producer or consumer at any instant.
unsafe impl Send for MmapRegion {}

impl MmapRegion {
    fn map(fd: RawFd, len: usize) -> Result<Self, ShmRingError> {
        if len == 0 {
            return Err(ShmRingError::new("invalid_shm_config", "zero mmap length"));
        }
        // SAFETY: `fd` is an open memfd sized to at least `len` bytes.
        let addr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if addr == libc::MAP_FAILED {
            return Err(ShmRingError::io(
                "shm_mmap_failed",
                "mmap shared ring",
                io::Error::last_os_error(),
            ));
        }
        let ptr = NonNull::new(addr.cast::<u8>())
            .ok_or_else(|| ShmRingError::new("shm_mmap_failed", "mmap returned null"))?;
        Ok(Self { ptr, len })
    }

    fn header(&self) -> &ShmHeader {
        // SAFETY: `self.len >= SHM_HEADER_BYTES` and mmap returns page-aligned memory.
        unsafe { &*self.ptr.as_ptr().cast::<ShmHeader>() }
    }

    fn slot_ptr(&self, slot_index: u32) -> *mut ShmSlotDescriptor {
        let byte_offset = SHM_HEADER_BYTES + (slot_index as usize) * SHM_SLOT_DESC_BYTES;
        // SAFETY: Caller validates `slot_index < slot_count` and mapping covers slot table.
        unsafe {
            self.ptr
                .as_ptr()
                .add(byte_offset)
                .cast::<ShmSlotDescriptor>()
        }
    }

    fn data_ptr(&self, byte_offset: usize) -> *mut u8 {
        // SAFETY: Caller validates `byte_offset + payload_len <= self.len`.
        unsafe { self.ptr.as_ptr().add(byte_offset) }
    }
}

impl Drop for MmapRegion {
    fn drop(&mut self) {
        // SAFETY: `ptr` and `len` came from a successful `libc::mmap`.
        let _ = unsafe { libc::munmap(self.ptr.as_ptr().cast::<libc::c_void>(), self.len) };
    }
}

/// Producer side of the `MEGO_SHM` v1 ring buffer (`memfd` + `eventfd`).
pub struct ShmRingProducer {
    layout: ShmLayout,
    shm_fd: OwnedFd,
    kick_fd: OwnedFd,
    done_fd: OwnedFd,
    region: MmapRegion,
}

impl ShmRingProducer {
    pub fn create(config: ShmRingConfig) -> Result<Self, ShmRingError> {
        let layout = config.validate()?;
        let shm_fd = create_memfd("miro-ego-shm")?;
        ftruncate_retry(shm_fd.as_raw_fd(), layout.total_bytes)?;
        let kick_fd = create_eventfd()?;
        let done_fd = create_eventfd()?;
        let region = MmapRegion::map(shm_fd.as_raw_fd(), layout.total_bytes)?;

        // Initialize header and pre-populate descriptor data_offsets before consumer attaches.
        // SAFETY: Exclusive owner during initialization before spawning/sharing fds.
        unsafe {
            let hdr_ptr = region.ptr.as_ptr().cast::<ShmHeader>();
            std::ptr::write_bytes(region.ptr.as_ptr(), 0, layout.data_region_offset);
            (*hdr_ptr).magic = SHM_MAGIC;
            (*hdr_ptr).version = SHM_VERSION;
            (*hdr_ptr).slot_count = layout.slot_count;
            (*hdr_ptr).slot_capacity = layout.slot_capacity;
            (*hdr_ptr).flags = AtomicU32::new(0);
            (*hdr_ptr).write_idx = AtomicU64::new(0);
            (*hdr_ptr).read_idx = AtomicU64::new(0);
            (*hdr_ptr).dropped_frames = AtomicU64::new(0);

            for slot in 0..layout.slot_count {
                let data_offset = layout.slot_data_offset(slot).ok_or_else(|| {
                    ShmRingError::new("invalid_shm_config", "slot data offset overflow")
                })?;
                std::ptr::write(
                    region.slot_ptr(slot),
                    ShmSlotDescriptor {
                        frame_seq: 0,
                        monotonic_ns: 0,
                        payload_len: 0,
                        flags: 0,
                        data_offset: data_offset as u64,
                        reserved: [0; 32],
                    },
                );
            }
        }

        Ok(Self {
            layout,
            shm_fd,
            kick_fd,
            done_fd,
            region,
        })
    }

    pub fn layout(&self) -> ShmLayout {
        self.layout
    }

    pub fn shm_raw_fd(&self) -> RawFd {
        self.shm_fd.as_raw_fd()
    }

    pub fn kick_raw_fd(&self) -> RawFd {
        self.kick_fd.as_raw_fd()
    }

    pub fn done_raw_fd(&self) -> RawFd {
        self.done_fd.as_raw_fd()
    }

    pub fn write_idx(&self) -> u64 {
        self.region.header().write_idx.load(Ordering::Acquire)
    }

    pub fn read_idx(&self) -> u64 {
        self.region.header().read_idx.load(Ordering::Acquire)
    }

    pub fn dropped_frames(&self) -> u64 {
        self.region.header().dropped_frames.load(Ordering::Acquire)
    }

    pub fn record_dropped_frame(&self) -> u64 {
        self.region
            .header()
            .dropped_frames
            .fetch_add(1, Ordering::AcqRel)
            + 1
    }

    /// Try to push one frame without blocking. Returns `PushStatus::Full` when all slots are in use.
    pub fn try_push(
        &mut self,
        frame_seq: u64,
        monotonic_ns: u64,
        flags: u32,
        payload: &[u8],
    ) -> Result<PushStatus, ShmRingError> {
        if payload.is_empty() {
            return Err(ShmRingError::new(
                "invalid_argument",
                "frame payload must not be empty",
            ));
        }
        let payload_len = u32::try_from(payload.len()).map_err(|_| {
            ShmRingError::new("frame_too_large", "frame payload exceeds u32 length")
        })?;
        if payload_len > self.layout.slot_capacity {
            return Err(ShmRingError::new(
                "frame_too_large",
                format!(
                    "frame payload ({payload_len} bytes) exceeds slot_capacity ({} bytes)",
                    self.layout.slot_capacity
                ),
            ));
        }

        let header = self.region.header();
        if (header.flags.load(Ordering::Acquire) & SHM_FLAG_EOF) != 0 {
            return Err(ShmRingError::new(
                "shm_eof",
                "cannot push frame after EOF has been signaled",
            ));
        }

        let write_idx = header.write_idx.load(Ordering::Relaxed);
        let read_idx = header.read_idx.load(Ordering::Acquire);
        if write_idx < read_idx || (write_idx - read_idx) > u64::from(self.layout.slot_count) {
            return Err(ShmRingError::new(
                "shm_corrupt",
                "SHM ring write_idx/read_idx out of bounds",
            ));
        }
        let in_flight = write_idx - read_idx;
        if in_flight == u64::from(self.layout.slot_count) {
            return Ok(PushStatus::Full);
        }

        let mask = u64::from(self.layout.slot_count - 1);
        let slot_index = (write_idx & mask) as u32;
        let data_offset = self
            .layout
            .slot_data_offset(slot_index)
            .ok_or_else(|| ShmRingError::new("shm_corrupt", "invalid slot data offset"))?;

        // SAFETY: `in_flight < slot_count` guarantees the consumer is not reading this slot,
        // and `payload_len <= slot_capacity` guarantees `data_offset + payload.len() <= total_bytes`.
        unsafe {
            if !payload.is_empty() {
                std::ptr::copy_nonoverlapping(
                    payload.as_ptr(),
                    self.region.data_ptr(data_offset),
                    payload.len(),
                );
            }
            std::ptr::write(
                self.region.slot_ptr(slot_index),
                ShmSlotDescriptor {
                    frame_seq,
                    monotonic_ns,
                    payload_len,
                    flags,
                    data_offset: data_offset as u64,
                    reserved: [0; 32],
                },
            );
        }

        let next_write_idx = write_idx.wrapping_add(1);
        header.write_idx.store(next_write_idx, Ordering::Release);
        signal_eventfd(self.kick_fd.as_raw_fd())?;
        Ok(PushStatus::Pushed {
            write_idx: next_write_idx,
            slot_index,
        })
    }

    /// Push a frame, waiting on `done_fd` up to `timeout` if the ring is full.
    pub fn push_wait(
        &mut self,
        frame_seq: u64,
        monotonic_ns: u64,
        flags: u32,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<u64, ShmRingError> {
        self.push_wait_with_check(frame_seq, monotonic_ns, flags, payload, timeout, || {
            Ok::<(), ShmRingError>(())
        })
    }

    /// Push a frame, polling `done_fd` in bounded slices while periodically running `check`
    /// so child encoder crashes / unexpected EOF abort immediately instead of stalling for `timeout`.
    pub fn push_wait_with_check<E, F>(
        &mut self,
        frame_seq: u64,
        monotonic_ns: u64,
        flags: u32,
        payload: &[u8],
        timeout: Duration,
        mut check: F,
    ) -> Result<u64, E>
    where
        E: From<ShmRingError>,
        F: FnMut() -> Result<(), E>,
    {
        self.push_wait_with_backpressure_check(
            frame_seq,
            monotonic_ns,
            flags,
            payload,
            timeout,
            |_| check(),
        )
    }

    /// Push a frame, running `check(false)` prior to each push attempt and `check(true)` when
    /// all SHM slots are full so normal in-flight drain succeeds while backpressured
    /// stops/aborts unblock immediately.
    pub(crate) fn push_wait_with_backpressure_check<E, F>(
        &mut self,
        frame_seq: u64,
        monotonic_ns: u64,
        flags: u32,
        payload: &[u8],
        timeout: Duration,
        mut check: F,
    ) -> Result<u64, E>
    where
        E: From<ShmRingError>,
        F: FnMut(bool) -> Result<(), E>,
    {
        const POLL_SLICE: Duration = Duration::from_millis(100);
        let deadline = Instant::now() + timeout;
        loop {
            check(false)?;
            match self.try_push(frame_seq, monotonic_ns, flags, payload)? {
                PushStatus::Pushed { write_idx, .. } => return Ok(write_idx),
                PushStatus::Full => {
                    check(true)?;
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(ShmRingError::new(
                            "shm_backpressure_timeout",
                            "timed out waiting for encoder to free a SHM slot",
                        )
                        .into());
                    }
                    let _ = wait_and_drain_eventfd(
                        self.done_fd.as_raw_fd(),
                        remaining.min(POLL_SLICE),
                    )?;
                }
            }
        }
    }

    pub(crate) fn clone_done_wakeup_fd(&self) -> Result<OwnedFd, ShmRingError> {
        dup_owned(self.done_fd.as_raw_fd())
    }

    /// Mark EOF in `header.flags` (`SHM_FLAG_EOF`) and wake the consumer via `kick_fd`.
    pub fn signal_eof(&self) -> Result<(), ShmRingError> {
        self.region
            .header()
            .flags
            .fetch_or(SHM_FLAG_EOF, Ordering::Release);
        signal_eventfd(self.kick_fd.as_raw_fd())
    }

    /// Duplicate `(shm_fd, kick_fd, done_fd)` with `FD_CLOEXEC` for an in-process consumer
    /// (primarily used in tests and pure-Rust loopback pipelines).
    pub fn clone_consumer_fds(&self) -> Result<(OwnedFd, OwnedFd, OwnedFd), ShmRingError> {
        let shm = dup_owned(self.shm_fd.as_raw_fd())?;
        let kick = dup_owned(self.kick_fd.as_raw_fd())?;
        let done = dup_owned(self.done_fd.as_raw_fd())?;
        Ok((shm, kick, done))
    }
}

/// Consumer side of the `MEGO_SHM` v1 ring buffer, validating header magic, version,
/// power-of-2 slot count, mapping bounds, and per-slot `data_offset + payload_len`.
pub struct ShmRingConsumer {
    layout: ShmLayout,
    _shm_fd: OwnedFd,
    kick_fd: OwnedFd,
    done_fd: OwnedFd,
    region: MmapRegion,
}

impl ShmRingConsumer {
    pub fn attach(
        shm_fd: OwnedFd,
        kick_fd: OwnedFd,
        done_fd: OwnedFd,
    ) -> Result<Self, ShmRingError> {
        let stat = fstat(shm_fd.as_raw_fd())
            .map_err(|err| ShmRingError::io("shm_stat_failed", "fstat shm_fd", err))?;
        let file_size = usize::try_from(stat.st_size)
            .map_err(|_| ShmRingError::new("shm_invalid", "shm_fd has negative size"))?;
        if file_size < SHM_HEADER_BYTES {
            return Err(ShmRingError::new(
                "shm_invalid",
                "shm_fd smaller than 4096-byte header",
            ));
        }

        // First map header to inspect and validate dimensions.
        let header_map = MmapRegion::map(shm_fd.as_raw_fd(), SHM_HEADER_BYTES)?;
        let header = header_map.header();
        if header.magic != SHM_MAGIC {
            return Err(ShmRingError::new("shm_invalid", "invalid MEGO_SHM magic"));
        }
        if header.version != SHM_VERSION {
            return Err(ShmRingError::new(
                "shm_invalid",
                format!("unsupported MEGO_SHM version {}", header.version),
            ));
        }
        let layout = ShmLayout::compute(header.slot_count, header.slot_capacity)?;
        drop(header_map);

        if file_size < layout.total_bytes {
            return Err(ShmRingError::new(
                "shm_invalid",
                format!(
                    "shm_fd length ({file_size}) smaller than required layout ({})",
                    layout.total_bytes
                ),
            ));
        }

        let region = MmapRegion::map(shm_fd.as_raw_fd(), layout.total_bytes)?;
        let hdr = region.header();
        let read_idx = hdr.read_idx.load(Ordering::Acquire);
        let write_idx = hdr.write_idx.load(Ordering::Acquire);
        if write_idx < read_idx || (write_idx - read_idx) > u64::from(layout.slot_count) {
            return Err(ShmRingError::new(
                "shm_invalid",
                "SHM ring initial write_idx/read_idx out of bounds",
            ));
        }
        Ok(Self {
            layout,
            _shm_fd: shm_fd,
            kick_fd,
            done_fd,
            region,
        })
    }

    pub fn layout(&self) -> ShmLayout {
        self.layout
    }

    /// Try to consume one frame without blocking.
    pub fn try_pop(&mut self) -> Result<PopStatus, ShmRingError> {
        let header = self.region.header();
        let read_idx = header.read_idx.load(Ordering::Relaxed);
        let mut write_idx = header.write_idx.load(Ordering::Acquire);
        if write_idx < read_idx || (write_idx - read_idx) > u64::from(self.layout.slot_count) {
            return Err(ShmRingError::new(
                "shm_corrupt",
                "SHM ring write_idx/read_idx out of bounds",
            ));
        }
        if read_idx == write_idx {
            if (header.flags.load(Ordering::Acquire) & SHM_FLAG_EOF) != 0 {
                // Re-check write_idx after observing EOF so a final frame pushed right before
                // signal_eof() is never lost.
                write_idx = header.write_idx.load(Ordering::Acquire);
                if write_idx < read_idx
                    || (write_idx - read_idx) > u64::from(self.layout.slot_count)
                {
                    return Err(ShmRingError::new(
                        "shm_corrupt",
                        "SHM ring write_idx/read_idx out of bounds",
                    ));
                }
                if write_idx == read_idx {
                    return Ok(PopStatus::Eof);
                }
            } else {
                return Ok(PopStatus::Empty);
            }
        }

        let mask = u64::from(self.layout.slot_count - 1);
        let slot_index = (read_idx & mask) as u32;
        // SAFETY: `read_idx < write_idx` with Acquire fence guarantees producer finished writing.
        let desc = unsafe { std::ptr::read(self.region.slot_ptr(slot_index)) };
        if desc.payload_len == 0 || desc.payload_len > self.layout.slot_capacity {
            return Err(ShmRingError::new(
                "shm_corrupt",
                "slot payload_len is zero or exceeds slot_capacity",
            ));
        }
        let expected_offset = self
            .layout
            .slot_data_offset(slot_index)
            .ok_or_else(|| ShmRingError::new("shm_corrupt", "invalid slot index"))?;
        let data_offset = usize::try_from(desc.data_offset)
            .map_err(|_| ShmRingError::new("shm_corrupt", "slot data_offset overflow"))?;
        let payload_len = desc.payload_len as usize;
        let end_offset = data_offset
            .checked_add(payload_len)
            .ok_or_else(|| ShmRingError::new("shm_corrupt", "slot payload end overflow"))?;
        if data_offset != expected_offset || end_offset > self.region.len {
            return Err(ShmRingError::new(
                "shm_corrupt",
                "slot data_offset does not match slot index or exceeds SHM data region",
            ));
        }

        let mut payload = vec![0_u8; payload_len];
        if payload_len > 0 {
            // SAFETY: Bounds validated against `self.region.len` and slot owned by consumer.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.region.data_ptr(data_offset),
                    payload.as_mut_ptr(),
                    payload_len,
                );
            }
        }

        header
            .read_idx
            .store(read_idx.wrapping_add(1), Ordering::Release);
        signal_eventfd(self.done_fd.as_raw_fd())?;

        Ok(PopStatus::Frame(ConsumedFrame {
            frame_seq: desc.frame_seq,
            monotonic_ns: desc.monotonic_ns,
            flags: desc.flags,
            payload,
        }))
    }

    /// Pop the next frame, waiting on `kick_fd` up to `timeout`.
    pub fn pop_wait(&mut self, timeout: Duration) -> Result<PopStatus, ShmRingError> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.try_pop()? {
                PopStatus::Empty => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Ok(PopStatus::Empty);
                    }
                    let _ = wait_and_drain_eventfd(self.kick_fd.as_raw_fd(), remaining)?;
                }
                status => return Ok(status),
            }
        }
    }
}

fn ensure_fd_ge_3(fd: OwnedFd) -> Result<OwnedFd, ShmRingError> {
    if fd.as_raw_fd() >= 3 {
        return Ok(fd);
    }
    loop {
        // SAFETY: `F_DUPFD_CLOEXEC` duplicates `fd` onto the lowest available descriptor >= 3.
        let duplicated = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
        if duplicated >= 0 {
            drop(fd);
            // SAFETY: `duplicated` is a newly created owned descriptor >= 3.
            return Ok(unsafe { OwnedFd::from_raw_fd(duplicated) });
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(ShmRingError::io(
                "shm_dup_failed",
                "fcntl(F_DUPFD_CLOEXEC, 3)",
                error,
            ));
        }
    }
}

fn create_memfd(name: &str) -> Result<OwnedFd, ShmRingError> {
    let c_name = CString::new(name)
        .map_err(|_| ShmRingError::new("invalid_shm_config", "memfd name contains NUL"))?;
    // SAFETY: `c_name` is a valid NUL-terminated string.
    let fd = unsafe { libc::memfd_create(c_name.as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(ShmRingError::io(
            "shm_create_failed",
            "memfd_create",
            io::Error::last_os_error(),
        ));
    }
    // SAFETY: `fd` is a newly created owned descriptor.
    ensure_fd_ge_3(unsafe { OwnedFd::from_raw_fd(fd) })
}

pub(crate) fn create_eventfd() -> Result<OwnedFd, ShmRingError> {
    // SAFETY: `eventfd` takes an initial value and flags.
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if fd < 0 {
        return Err(ShmRingError::io(
            "eventfd_create_failed",
            "eventfd",
            io::Error::last_os_error(),
        ));
    }
    // SAFETY: `fd` is a newly created owned descriptor.
    ensure_fd_ge_3(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn ftruncate_retry(fd: RawFd, len: usize) -> Result<(), ShmRingError> {
    let off = libc::off_t::try_from(len)
        .map_err(|_| ShmRingError::new("invalid_shm_config", "SHM size exceeds off_t"))?;
    loop {
        // SAFETY: `fd` is an owned memfd descriptor.
        if unsafe { libc::ftruncate(fd, off) } == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(ShmRingError::io("shm_resize_failed", "ftruncate", error));
        }
    }
}

fn dup_owned(fd: RawFd) -> Result<OwnedFd, ShmRingError> {
    let dup =
        dup_cloexec(fd).map_err(|err| ShmRingError::io("shm_dup_failed", "dup_cloexec", err))?;
    // SAFETY: `dup_cloexec` returned a newly owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(dup) })
}

fn signal_eventfd(fd: RawFd) -> Result<(), ShmRingError> {
    let value: u64 = 1;
    loop {
        // SAFETY: `value` points to 8 readable bytes expected by eventfd write.
        let written = unsafe {
            libc::write(
                fd,
                (&raw const value).cast::<libc::c_void>(),
                std::mem::size_of::<u64>(),
            )
        };
        if written == std::mem::size_of::<u64>() as isize {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        if error.kind() == io::ErrorKind::WouldBlock || error.raw_os_error() == Some(libc::EAGAIN) {
            // Counter already saturated; peer already has a pending wakeup.
            return Ok(());
        }
        return Err(ShmRingError::io(
            "eventfd_signal_failed",
            "write eventfd",
            error,
        ));
    }
}

pub(crate) fn wake_eventfd_best_effort(fd: RawFd) {
    let _ = signal_eventfd(fd);
}

fn wait_and_drain_eventfd(fd: RawFd, timeout: Duration) -> Result<bool, ShmRingError> {
    if fd < 0 {
        return Err(ShmRingError::io(
            "eventfd_poll_failed",
            "poll eventfd",
            io::Error::from_raw_os_error(libc::EBADF),
        ));
    }
    let deadline = Instant::now() + timeout;
    let nonblocking = timeout.is_zero();
    let mut pollfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    loop {
        let timeout_ms = if nonblocking {
            0
        } else {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(false);
            }
            remaining.as_millis().max(1).min(i32::MAX as u128) as i32
        };
        pollfd.revents = 0;
        // SAFETY: `pollfd` points to a valid `libc::pollfd` of length 1.
        let rc = unsafe { libc::poll(&mut pollfd, 1, timeout_ms) };
        if rc > 0 {
            if (pollfd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL)) != 0 {
                return Err(ShmRingError::new(
                    "eventfd_poll_failed",
                    format!("poll eventfd returned error revents={:#x}", pollfd.revents),
                ));
            }
            if (pollfd.revents & libc::POLLIN) != 0 {
                drain_eventfd(fd)?;
                return Ok(true);
            }
            return Ok(false);
        }
        if rc == 0 {
            return Ok(false);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(ShmRingError::io(
                "eventfd_poll_failed",
                "poll eventfd",
                error,
            ));
        }
    }
}

fn drain_eventfd(fd: RawFd) -> Result<(), ShmRingError> {
    let mut counter: u64 = 0;
    loop {
        // SAFETY: `counter` points to 8 writable bytes expected by eventfd read.
        let read = unsafe {
            libc::read(
                fd,
                (&raw mut counter).cast::<libc::c_void>(),
                std::mem::size_of::<u64>(),
            )
        };
        if read == std::mem::size_of::<u64>() as isize {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        if error.kind() == io::ErrorKind::WouldBlock || error.raw_os_error() == Some(libc::EAGAIN) {
            return Ok(());
        }
        return Err(ShmRingError::io(
            "eventfd_drain_failed",
            "read eventfd",
            error,
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn verifies_exact_mego_shm_v1_layout_and_alignment() {
        let layout = ShmLayout::compute(16, 4 * 1024 * 1024).unwrap();
        assert_eq!(layout.slot_table_offset, 4096);
        // 4096 + 16 * 64 = 5120 -> 4096-byte aligned = 8192
        assert_eq!(layout.data_region_offset, 8192);
        assert_eq!(layout.data_region_offset % 4096, 0);
        assert_eq!(layout.total_bytes, 8192 + 16 * 4 * 1024 * 1024);

        // 64 slots * 64 bytes = 4096 -> 4096 + 4096 = 8192 (exact alignment)
        let layout64 = ShmLayout::compute(64, 4096).unwrap();
        assert_eq!(layout64.data_region_offset, 8192);

        // Reject invalid slot counts (0, non-power-of-two, > 256) and 0 capacity
        assert!(ShmLayout::compute(0, 4096).is_err());
        assert!(ShmLayout::compute(3, 4096).is_err());
        assert!(ShmLayout::compute(512, 4096).is_err());
        assert!(ShmLayout::compute(16, 0).is_err());
    }

    #[test]
    fn single_thread_wrap_around_full_backpressure_and_eof_drain() {
        let mut producer = ShmRingProducer::create(ShmRingConfig {
            slot_count: 4,
            slot_capacity: 128,
        })
        .unwrap();
        let (shm, kick, done) = producer.clone_consumer_fds().unwrap();
        let mut consumer = ShmRingConsumer::attach(shm, kick, done).unwrap();

        // Fill all 4 slots
        for seq in 0..4_u64 {
            let payload = vec![seq as u8; 16 + seq as usize];
            assert!(matches!(
                producer.try_push(seq, 1000 + seq, 0, &payload).unwrap(),
                PushStatus::Pushed { .. }
            ));
        }
        // 5th push hits full-ring backpressure
        assert_eq!(
            producer.try_push(4, 1004, 0, b"blocked").unwrap(),
            PushStatus::Full
        );
        assert_eq!(
            producer
                .push_wait(4, 1004, 0, b"blocked", Duration::from_millis(20))
                .unwrap_err()
                .code,
            "shm_backpressure_timeout"
        );

        // Reject oversized payload
        assert_eq!(
            producer
                .try_push(99, 9999, 0, &[0_u8; 129])
                .unwrap_err()
                .code,
            "frame_too_large"
        );

        // Pop 2 frames and push 6 more across multiple wrap-arounds
        for expected in 0..2_u64 {
            let PopStatus::Frame(frame) = consumer.try_pop().unwrap() else {
                panic!("expected frame {expected}");
            };
            assert_eq!(frame.frame_seq, expected);
            assert_eq!(frame.monotonic_ns, 1000 + expected);
            assert_eq!(frame.payload, vec![expected as u8; 16 + expected as usize]);
        }

        for seq in 4..12_u64 {
            assert!(matches!(
                producer
                    .try_push(seq, 2000 + seq, 7, &[seq as u8; 32])
                    .unwrap(),
                PushStatus::Pushed { .. }
            ));
            let PopStatus::Frame(frame) = consumer.try_pop().unwrap() else {
                panic!("expected wrapped frame");
            };
            assert_eq!(frame.frame_seq, seq - 2);
        }

        // Signal EOF while 2 frames remain unread; consumer must drain them before Eof
        producer.signal_eof().unwrap();
        assert_eq!(
            producer.try_push(100, 0, 0, b"late").unwrap_err().code,
            "shm_eof"
        );

        let PopStatus::Frame(f10) = consumer.try_pop().unwrap() else {
            panic!("expected frame 10");
        };
        assert_eq!(f10.frame_seq, 10);
        let PopStatus::Frame(f11) = consumer.try_pop().unwrap() else {
            panic!("expected frame 11");
        };
        assert_eq!(f11.frame_seq, 11);
        assert_eq!(consumer.try_pop().unwrap(), PopStatus::Eof);
    }

    #[test]
    fn multithreaded_producer_consumer_streams_frames_under_backpressure() {
        let mut producer = ShmRingProducer::create(ShmRingConfig {
            slot_count: 8,
            slot_capacity: 4096,
        })
        .unwrap();
        let (shm, kick, done) = producer.clone_consumer_fds().unwrap();

        let consumer_thread = thread::spawn(move || {
            let mut consumer = ShmRingConsumer::attach(shm, kick, done).unwrap();
            let mut count = 0_u64;
            loop {
                match consumer.pop_wait(Duration::from_secs(2)).unwrap() {
                    PopStatus::Frame(frame) => {
                        assert_eq!(frame.frame_seq, count);
                        assert_eq!(frame.monotonic_ns, 10_000 + count);
                        assert_eq!(frame.payload.len(), (count as usize % 2048) + 1);
                        assert!(frame.payload.iter().all(|&b| b == (count & 0xff) as u8));
                        count += 1;
                    }
                    PopStatus::Eof => break count,
                    PopStatus::Empty => panic!("consumer timed out before EOF"),
                }
            }
        });

        for seq in 0..200_u64 {
            let payload = vec![(seq & 0xff) as u8; (seq as usize % 2048) + 1];
            producer
                .push_wait(seq, 10_000 + seq, 1, &payload, Duration::from_secs(2))
                .unwrap();
        }
        producer.signal_eof().unwrap();

        let received = consumer_thread.join().unwrap();
        assert_eq!(received, 200);
        assert_eq!(producer.write_idx(), 200);
        assert_eq!(producer.read_idx(), 200);
    }

    #[test]
    fn consumer_rejects_corrupted_magic_and_out_of_bounds_descriptors() {
        let producer = ShmRingProducer::create(ShmRingConfig {
            slot_count: 4,
            slot_capacity: 64,
        })
        .unwrap();
        let (shm, kick, done) = producer.clone_consumer_fds().unwrap();
        let mut consumer = ShmRingConsumer::attach(shm, kick, done).unwrap();

        // Corrupt descriptor data_offset past end of mapping
        unsafe {
            let slot = consumer.region.slot_ptr(0);
            (*slot).data_offset = (consumer.layout.total_bytes as u64) + 16;
            (*slot).payload_len = 8;
        }
        producer
            .region
            .header()
            .write_idx
            .store(1, Ordering::Release);
        assert_eq!(consumer.try_pop().unwrap_err().code, "shm_corrupt");

        // Corrupt descriptor data_offset to point to slot 1 instead of slot 0 (exact offset check)
        unsafe {
            let slot = consumer.region.slot_ptr(0);
            (*slot).data_offset = consumer.layout.slot_data_offset(1).unwrap() as u64;
            (*slot).payload_len = 8;
        }
        assert_eq!(consumer.try_pop().unwrap_err().code, "shm_corrupt");

        // Corrupt write_idx > read_idx + slot_count is rejected as shm_corrupt (not treated as Full)
        producer
            .region
            .header()
            .write_idx
            .store(99, Ordering::Release);
        assert_eq!(consumer.try_pop().unwrap_err().code, "shm_corrupt");
        assert_eq!(DEFAULT_SLOT_CAPACITY, 8 * 1024 * 1024);
        assert!(producer.shm_raw_fd() >= 3);
        assert!(producer.kick_raw_fd() >= 3);
        assert!(producer.done_raw_fd() >= 3);

        // Corrupt magic and verify new attach fails
        unsafe {
            let hdr = producer.region.ptr.as_ptr().cast::<ShmHeader>();
            (*hdr).magic = *b"BADMAGIC";
        }
        let (shm2, kick2, done2) = producer.clone_consumer_fds().unwrap();
        assert_eq!(
            ShmRingConsumer::attach(shm2, kick2, done2)
                .err()
                .unwrap()
                .code,
            "shm_invalid"
        );
    }

    #[test]
    fn rejects_empty_payloads_and_handles_submillisecond_and_invalid_eventfd_polls() {
        let mut producer = ShmRingProducer::create(ShmRingConfig {
            slot_count: 4,
            slot_capacity: 64,
        })
        .unwrap();
        let (shm, kick, done) = producer.clone_consumer_fds().unwrap();
        let mut consumer = ShmRingConsumer::attach(shm, kick, done).unwrap();

        assert_eq!(
            producer.try_push(0, 100, 0, b"").unwrap_err().code,
            "invalid_argument"
        );
        assert_eq!(
            producer
                .push_wait(0, 100, 0, b"", Duration::from_millis(5))
                .unwrap_err()
                .code,
            "invalid_argument"
        );

        // Positive sub-millisecond wait clamps to >= 1ms without busy-spinning and times out cleanly.
        assert!(
            !super::wait_and_drain_eventfd(
                producer.done_fd.as_raw_fd(),
                Duration::from_micros(250),
            )
            .unwrap()
        );
        // Negative fd is rejected immediately, and a closed non-negative fd triggers POLLNVAL.
        assert_eq!(
            super::wait_and_drain_eventfd(-1, Duration::from_millis(1))
                .unwrap_err()
                .code,
            "eventfd_poll_failed"
        );
        let closed_fd = dup_cloexec(producer.done_fd.as_raw_fd()).unwrap();
        unsafe {
            libc::close(closed_fd);
        }
        assert_eq!(
            super::wait_and_drain_eventfd(closed_fd, Duration::from_millis(1))
                .unwrap_err()
                .code,
            "eventfd_poll_failed"
        );

        // Zero-length forged slot descriptor is rejected as shm_corrupt.
        unsafe {
            let slot = consumer.region.slot_ptr(0);
            (*slot).data_offset = consumer.layout.data_region_offset as u64;
            (*slot).payload_len = 0;
        }
        producer
            .region
            .header()
            .write_idx
            .store(1, Ordering::Release);
        assert_eq!(consumer.try_pop().unwrap_err().code, "shm_corrupt");

        // Corrupt descriptor data_offset to point to slot 1 instead of slot 0 (exact offset check)
        unsafe {
            let slot = consumer.region.slot_ptr(0);
            (*slot).data_offset = consumer.layout.slot_data_offset(1).unwrap() as u64;
            (*slot).payload_len = 8;
        }
        assert_eq!(consumer.try_pop().unwrap_err().code, "shm_corrupt");

        // Corrupt write_idx > read_idx + slot_count is rejected as shm_corrupt (not treated as Full)
        producer
            .region
            .header()
            .write_idx
            .store(99, Ordering::Release);
        assert_eq!(consumer.try_pop().unwrap_err().code, "shm_corrupt");
        assert_eq!(DEFAULT_SLOT_CAPACITY, 8 * 1024 * 1024);
        assert!(producer.shm_raw_fd() >= 3);
        assert!(producer.kick_raw_fd() >= 3);
        assert!(producer.done_raw_fd() >= 3);
    }
}
