//! Synthetic SHM-encoder-backed (`ShmEncoderCaptureBackend`) and pure-Rust deterministic
//! (`MockCaptureBackend`) capture backends for hermetic E2E tests and `miro-ego serve --mock`.

use super::native_capture::CaptureFailureLog;
use super::{
    ENCODER_FINISH_TIMEOUT, SYNTHETIC_FMP4_INIT, SYNTHETIC_FMP4_MOOF, SYNTHETIC_JPEG_FRAME,
    sha256_hex,
};
use bytes::Bytes;
use miro_ego_api::{H264PreviewHub, MjpegPreviewHub};
use miro_ego_capture::session::capture_runtime::FailureSink;
use miro_ego_capture::session::shm_ring::ShmRingConfig;
use miro_ego_capture::session::stereo_encoder::{
    EncoderConfig, EncoderProcess, EncoderTransport, min_shm_slot_capacity,
};
use miro_ego_core::config::VideoCodec;
use miro_ego_core::media_types::{FragmentEvent, SealedMediaEvent};
use miro_ego_core::sys::clock::monotonic_ns;
use miro_ego_hw::thermal::read_soc_temperature_c;
use miro_ego_storage::coordinator::{CAMERA_FOCUS_SCHEMA, SyncedFmp4Artifacts};
use miro_ego_storage::{
    ActiveCaptureHandle, BackendStartRequest, CameraFocusSetRequest, CameraFocusStatus,
    CaptureBackend, CaptureProgressSnapshot, CapturedArtifact, CapturedAudioOutcome,
    CapturedSessionArtifacts, RuntimeObservation, StopReason, StorageError, StorageResult,
};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Real C-encoder-backed capture backend driving `miro-ego-encoder` / `miro-ego-encoder-mock`
/// over `MEGO_SHM` v1 (`memfd` + `eventfd`) with live synthetic stereo JPEG frame feeding.
pub struct ShmEncoderCaptureBackend {
    encoder_bin: PathBuf,
    width: u32,
    height: u32,
    fps: u32,
    segment_frames: u64,
    min_frames: u64,
    mjpeg_hub: Arc<MjpegPreviewHub>,
    h264_hub: Option<Arc<H264PreviewHub>>,
    focus: Mutex<CameraFocusStatus>,
}

impl ShmEncoderCaptureBackend {
    pub fn new(
        encoder_bin: PathBuf,
        width: u32,
        height: u32,
        fps: u32,
        segment_frames: u64,
        mjpeg_hub: Arc<MjpegPreviewHub>,
        h264_hub: Option<Arc<H264PreviewHub>>,
    ) -> Self {
        let _ = mjpeg_hub.publish(Bytes::from_static(SYNTHETIC_JPEG_FRAME));
        if let Some(hub) = &h264_hub {
            hub.set_init_segment_blocking(Bytes::from_static(SYNTHETIC_FMP4_INIT));
            let _ = hub.publish_fragment(Bytes::from_static(SYNTHETIC_FMP4_MOOF));
        }
        let seg_frames = segment_frames.max(1);
        Self {
            encoder_bin,
            width: width.max(8) & !3,
            height: height.max(8),
            fps: fps.max(1),
            segment_frames: seg_frames,
            min_frames: seg_frames,
            mjpeg_hub,
            h264_hub,
            focus: Mutex::new(CameraFocusStatus {
                schema: CAMERA_FOCUS_SCHEMA.to_owned(),
                value: 250,
                minimum: 0,
                maximum: 1023,
                step: 1,
                default: 250,
                auto_supported: true,
                auto_enabled: Some(false),
            }),
        }
    }
}

struct ShmActiveHandle {
    stop_flag: Arc<AtomicBool>,
    abort_flag: Arc<AtomicBool>,
    failure_log: Arc<CaptureFailureLog>,
    frames_count: Arc<AtomicU64>,
    bytes_written: Arc<AtomicU64>,
    started_monotonic_ns: u64,
    fps: u32,
    worker: Option<JoinHandle<StorageResult<CapturedSessionArtifacts>>>,
    completed: bool,
}

impl ShmActiveHandle {
    fn abort_worker(&mut self) {
        if self.completed {
            return;
        }
        self.completed = true;
        self.abort_flag.store(true, Ordering::Release);
        self.stop_flag.store(true, Ordering::Release);
        if let Some(handle) = self.worker.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ShmActiveHandle {
    fn drop(&mut self) {
        self.abort_worker();
    }
}

impl ActiveCaptureHandle for ShmActiveHandle {
    fn progress(&self) -> CaptureProgressSnapshot {
        let now_ns = monotonic_ns().unwrap_or(self.started_monotonic_ns);
        let elapsed_seconds =
            now_ns.saturating_sub(self.started_monotonic_ns) as f64 / 1_000_000_000.0;
        CaptureProgressSnapshot {
            elapsed_seconds,
            captured_frames: self.frames_count.load(Ordering::Acquire),
            bytes_written: self.bytes_written.load(Ordering::Acquire),
        }
    }

    fn take_failure(&self) -> Option<(String, String)> {
        self.failure_log.take()
    }

    fn finish(mut self: Box<Self>, _reason: StopReason) -> StorageResult<CapturedSessionArtifacts> {
        self.stop_flag.store(true, Ordering::Release);
        let Some(handle) = self.worker.take() else {
            return Err(StorageError::service_unavailable(
                "capture_worker_missing",
                "SHM capture worker handle already consumed",
            ));
        };
        self.completed = true;
        let mut artifacts = handle.join().map_err(|_| {
            StorageError::service_unavailable(
                "capture_worker_panicked",
                "SHM capture worker panicked",
            )
        })??;
        let _ = self.fps;
        if artifacts.duration_seconds <= 0.0 {
            artifacts.duration_seconds = 0.1;
        }
        Ok(artifacts)
    }

    fn abort(mut self: Box<Self>) {
        self.abort_worker();
    }
}

impl CaptureBackend for ShmEncoderCaptureBackend {
    fn is_camera_connected(&self) -> bool {
        true
    }

    fn camera_focus(&self) -> StorageResult<Option<CameraFocusStatus>> {
        let guard = self.focus.lock().map_err(|_| {
            StorageError::service_unavailable("focus_poisoned", "focus state mutex poisoned")
        })?;
        Ok(Some(guard.clone()))
    }

    fn set_camera_focus(&self, req: &CameraFocusSetRequest) -> StorageResult<CameraFocusStatus> {
        let mut guard = self.focus.lock().map_err(|_| {
            StorageError::service_unavailable("focus_poisoned", "focus state mutex poisoned")
        })?;
        if let Some(v) = req.value {
            guard.value = v;
        }
        if let Some(auto) = req.auto_enabled {
            guard.auto_enabled = Some(auto);
        }
        Ok(guard.clone())
    }

    fn begin_capture(
        &self,
        req: &BackendStartRequest,
    ) -> StorageResult<Box<dyn ActiveCaptureHandle>> {
        let min_slot_cap = min_shm_slot_capacity(u64::from(self.width), u64::from(self.height))
            .ok()
            .and_then(|cap| u32::try_from(cap).ok())
            .unwrap_or(2 * 1024 * 1024);
        let shm_cfg = ShmRingConfig {
            slot_count: 8,
            slot_capacity: (2 * 1024 * 1024_u32).max(min_slot_cap),
        };
        let mut encoder = EncoderProcess::with_transport(
            EncoderConfig {
                out_dir: &req.staging_dir,
                executable: &self.encoder_bin,
                width: u64::from(self.width),
                height: u64::from(self.height),
                fps: u64::from(self.fps),
                bitrate_kbps: u64::from(req.encoding.bitrate_kbps),
                segment_frames: self.segment_frames,
                path_prefix: "",
            },
            EncoderTransport::ShmRing(shm_cfg),
        )
        .map_err(|err| StorageError::service_unavailable("encoder_init_failed", err.message))?;

        encoder
            .configure(&req.encoding.encoder_arguments())
            .map_err(|err| StorageError::invalid_input("encoder_config_invalid", err.message))?;
        encoder
            .set_fmp4_options("session.mp4", self.segment_frames, 48_000, 0)
            .map_err(|err| StorageError::invalid_input("encoder_config_invalid", err.message))?;
        encoder.start().map_err(|err| {
            StorageError::service_unavailable("encoder_start_failed", err.message)
        })?;

        let stop_flag = Arc::new(AtomicBool::new(false));
        let abort_flag = Arc::new(AtomicBool::new(false));
        let failure_log = Arc::new(CaptureFailureLog::new());
        let frames_count = Arc::new(AtomicU64::new(0));
        let bytes_written = Arc::new(AtomicU64::new(0));

        let stop_worker = Arc::clone(&stop_flag);
        let abort_worker = Arc::clone(&abort_flag);
        let failure_worker = Arc::clone(&failure_log);
        let frames_worker = Arc::clone(&frames_count);
        let bytes_worker = Arc::clone(&bytes_written);
        let mjpeg_hub = Arc::clone(&self.mjpeg_hub);
        let h264_hub = self.h264_hub.clone();
        let fps = self.fps;
        let min_frames = self.min_frames;
        let started_ns = req.started_monotonic_ns;

        let worker = thread::Builder::new()
            .name("miro-ego-shm-feeder".to_owned())
            .spawn(move || -> StorageResult<CapturedSessionArtifacts> {
                let frame_interval =
                    Duration::from_millis((1000 / u64::from(fps.max(1))).clamp(5, 50));
                let mut seq: u64 = 0;

                while !abort_worker.load(Ordering::Acquire)
                    && (!stop_worker.load(Ordering::Acquire) || seq < min_frames)
                {
                    let frame_ns = monotonic_ns().unwrap_or(started_ns.saturating_add(seq + 1));
                    let written = match encoder.submit_frame(seq, frame_ns, SYNTHETIC_JPEG_FRAME) {
                        Ok(n) => n,
                        Err(err) => {
                            let msg = format!("{}: {}", err.code, err.message);
                            failure_worker.on_failure("encoder_submit_failed", &msg);
                            encoder.abort();
                            return Err(StorageError::service_unavailable(
                                "encoder_submit_failed",
                                msg,
                            ));
                        }
                    };

                    let _ = mjpeg_hub.publish(Bytes::from_static(SYNTHETIC_JPEG_FRAME));
                    if let Some(ref hub) = h264_hub {
                        let _ = hub.publish_fragment(Bytes::from_static(SYNTHETIC_FMP4_MOOF));
                    }

                    seq = seq.saturating_add(1);
                    frames_worker.store(seq, Ordering::Release);
                    bytes_worker.fetch_add(written, Ordering::AcqRel);

                    if abort_worker.load(Ordering::Acquire)
                        || (stop_worker.load(Ordering::Acquire) && seq >= min_frames)
                    {
                        break;
                    }
                    thread::sleep(frame_interval);
                }

                if abort_worker.load(Ordering::Acquire) {
                    encoder.abort();
                    return Err(StorageError::service_unavailable(
                        "capture_aborted",
                        "SHM capture worker aborted",
                    ));
                }

                let media_outcome = match encoder.finish_media(ENCODER_FINISH_TIMEOUT) {
                    Ok(ev) => ev,
                    Err(err) => {
                        let msg = format!("{}: {}", err.code, err.message);
                        failure_worker.on_failure("encoder_finish_failed", &msg);
                        return Err(StorageError::service_unavailable(
                            "encoder_finish_failed",
                            msg,
                        ));
                    }
                };
                let mut sealed = media_outcome.sealed.ok_or_else(|| {
                    StorageError::integrity(
                        "encoder_sealed_missing",
                        "SHM encoder completed without emitting sealed session.mp4 event",
                    )
                })?;
                if sealed.timescale == 0 {
                    sealed.timescale = 48_000;
                }

                let now_ns = monotonic_ns().unwrap_or(started_ns);
                let duration_seconds =
                    (now_ns.saturating_sub(started_ns) as f64 / 1_000_000_000.0).max(0.05);
                let fps_f = f64::from(fps.max(1));
                let placeholder_artifact = CapturedArtifact {
                    role: "media.session".to_owned(),
                    path: sealed.path.clone(),
                    media_type: "video/mp4".to_owned(),
                    bytes: sealed.bytes,
                    sha256: Some(sealed.sha256.clone()),
                };

                Ok(CapturedSessionArtifacts {
                    segments: Vec::new(),
                    frames_artifact: placeholder_artifact.clone(),
                    frames_count: sealed.frames.max(seq),
                    imu_artifact: placeholder_artifact,
                    imu_sample_count: sealed.imu_samples,
                    audio: CapturedAudioOutcome::NotRecorded {
                        requested_mode: "disabled".to_owned(),
                        reason: "device_default_disabled".to_owned(),
                    },
                    synced_fmp4: Some(SyncedFmp4Artifacts {
                        sealed,
                        fragments: media_outcome.fragments,
                        audio_sample_rate: None,
                        audio_channels: None,
                        audio_capture_clock: None,
                    }),
                    logs: Vec::new(),
                    dropped_frames: 0,
                    drop_events: Vec::new(),
                    duration_seconds,
                    effective_fps: fps_f,
                })
            })
            .map_err(|err| {
                StorageError::io("capture_worker_spawn", "spawn SHM feeder thread", err)
            })?;

        Ok(Box::new(ShmActiveHandle {
            stop_flag,
            abort_flag,
            failure_log,
            frames_count,
            bytes_written,
            started_monotonic_ns: req.started_monotonic_ns,
            fps: self.fps,
            worker: Some(worker),
            completed: false,
        }))
    }

    fn runtime_observation(&self) -> RuntimeObservation {
        let focus = self.focus.lock().ok().map(|g| g.clone());
        let mut obs = RuntimeObservation::default_with_camera(true, focus);
        if let Some(temp) = read_soc_temperature_c() {
            obs.temperature_celsius = temp;
        }
        obs
    }
}

/// Pure-Rust deterministic capture backend for `miro-ego serve --mock` when no external encoder
/// binary is supplied. Produces structurally valid ISO-BMFF MP4 payloads and streaming SHA-256 digests.
pub struct MockCaptureBackend {
    fps: u32,
    finish_delay: Duration,
    mjpeg_hub: Arc<MjpegPreviewHub>,
    h264_hub: Option<Arc<H264PreviewHub>>,
    focus: Mutex<CameraFocusStatus>,
}

impl MockCaptureBackend {
    pub fn new(
        fps: u32,
        mjpeg_hub: Arc<MjpegPreviewHub>,
        h264_hub: Option<Arc<H264PreviewHub>>,
    ) -> Self {
        let _ = mjpeg_hub.publish(Bytes::from_static(SYNTHETIC_JPEG_FRAME));
        if let Some(hub) = &h264_hub {
            hub.set_init_segment_blocking(Bytes::from_static(SYNTHETIC_FMP4_INIT));
            let _ = hub.publish_fragment(Bytes::from_static(SYNTHETIC_FMP4_MOOF));
        }
        Self {
            fps: fps.max(1),
            finish_delay: Duration::ZERO,
            mjpeg_hub,
            h264_hub,
            focus: Mutex::new(CameraFocusStatus {
                schema: CAMERA_FOCUS_SCHEMA.to_owned(),
                value: 250,
                minimum: 0,
                maximum: 1023,
                step: 1,
                default: 250,
                auto_supported: true,
                auto_enabled: Some(false),
            }),
        }
    }

    /// Configure an optional simulated encoder drain delay during `finish()` for testing
    /// non-blocking `202 Accepted` async `stopCapture` convergence.
    pub fn with_finish_delay(mut self, finish_delay: Duration) -> Self {
        self.finish_delay = finish_delay;
        self
    }
}

struct MockActiveHandle {
    staging_dir: PathBuf,
    session_id: String,
    started_monotonic_ns: u64,
    fps: u32,
    codec: VideoCodec,
    finish_delay: Duration,
    mjpeg_hub: Arc<MjpegPreviewHub>,
    h264_hub: Option<Arc<H264PreviewHub>>,
}

impl ActiveCaptureHandle for MockActiveHandle {
    fn progress(&self) -> CaptureProgressSnapshot {
        let now_ns = monotonic_ns().unwrap_or(self.started_monotonic_ns);
        let elapsed_seconds =
            now_ns.saturating_sub(self.started_monotonic_ns) as f64 / 1_000_000_000.0;
        let frames = ((elapsed_seconds * f64::from(self.fps)) as u64).max(1);
        CaptureProgressSnapshot {
            elapsed_seconds,
            captured_frames: frames,
            bytes_written: frames * 2048,
        }
    }

    fn finish(self: Box<Self>, _reason: StopReason) -> StorageResult<CapturedSessionArtifacts> {
        if !self.finish_delay.is_zero() {
            thread::sleep(self.finish_delay);
        }

        let frame_count: u64 = 5;
        let timescale: u32 = 48_000;
        let ticks_per_frame = timescale / self.fps.max(1);
        let total_ticks = frame_count * u64::from(ticks_per_frame);
        let fmp4_bytes = build_synthetic_synced_fmp4(
            &self.session_id,
            self.fps.max(1),
            self.codec,
            self.started_monotonic_ns,
            frame_count,
            timescale,
        );
        let media_sha = sha256_hex(&fmp4_bytes);
        write_file_synced(&self.staging_dir.join("session.mp4"), &fmp4_bytes)?;

        let _ = self
            .mjpeg_hub
            .publish(Bytes::from_static(SYNTHETIC_JPEG_FRAME));
        if let Some(hub) = &self.h264_hub {
            let _ = hub.publish_fragment(Bytes::from_static(SYNTHETIC_FMP4_MOOF));
        }

        let now_ns = monotonic_ns().unwrap_or(self.started_monotonic_ns);
        let duration_seconds =
            (now_ns.saturating_sub(self.started_monotonic_ns) as f64 / 1_000_000_000.0).max(0.1);
        let placeholder_artifact = CapturedArtifact {
            role: "media.session".to_owned(),
            path: "session.mp4".to_owned(),
            media_type: "video/mp4".to_owned(),
            bytes: fmp4_bytes.len() as u64,
            sha256: Some(media_sha.clone()),
        };

        Ok(CapturedSessionArtifacts {
            segments: Vec::new(),
            frames_artifact: placeholder_artifact.clone(),
            frames_count: frame_count,
            imu_artifact: placeholder_artifact,
            imu_sample_count: frame_count,
            audio: CapturedAudioOutcome::NotRecorded {
                requested_mode: "disabled".to_owned(),
                reason: "device_default_disabled".to_owned(),
            },
            synced_fmp4: Some(SyncedFmp4Artifacts {
                sealed: SealedMediaEvent {
                    path: "session.mp4".to_owned(),
                    bytes: fmp4_bytes.len() as u64,
                    sha256: media_sha,
                    frames: frame_count,
                    fragments: 1,
                    duration_ticks: total_ticks,
                    timescale,
                    audio_samples: 0,
                    imu_samples: frame_count,
                },
                fragments: vec![FragmentEvent {
                    index: 0,
                    start_frame: 0,
                    end_frame: frame_count,
                    duration_ticks: total_ticks,
                    bytes: fmp4_bytes.len() as u64,
                }],
                audio_sample_rate: None,
                audio_channels: None,
                audio_capture_clock: None,
            }),
            logs: Vec::new(),
            dropped_frames: 0,
            drop_events: Vec::new(),
            duration_seconds,
            effective_fps: f64::from(self.fps),
        })
    }

    fn abort(self: Box<Self>) {}
}

impl CaptureBackend for MockCaptureBackend {
    fn is_camera_connected(&self) -> bool {
        true
    }

    fn camera_focus(&self) -> StorageResult<Option<CameraFocusStatus>> {
        let guard = self.focus.lock().map_err(|_| {
            StorageError::service_unavailable("focus_poisoned", "focus state mutex poisoned")
        })?;
        Ok(Some(guard.clone()))
    }

    fn set_camera_focus(&self, req: &CameraFocusSetRequest) -> StorageResult<CameraFocusStatus> {
        let mut guard = self.focus.lock().map_err(|_| {
            StorageError::service_unavailable("focus_poisoned", "focus state mutex poisoned")
        })?;
        if let Some(v) = req.value {
            guard.value = v;
        }
        if let Some(auto) = req.auto_enabled {
            guard.auto_enabled = Some(auto);
        }
        Ok(guard.clone())
    }

    fn begin_capture(
        &self,
        req: &BackendStartRequest,
    ) -> StorageResult<Box<dyn ActiveCaptureHandle>> {
        Ok(Box::new(MockActiveHandle {
            staging_dir: req.staging_dir.clone(),
            session_id: req.session_id.clone(),
            started_monotonic_ns: req.started_monotonic_ns,
            fps: self.fps,
            codec: req.encoding.codec,
            finish_delay: self.finish_delay,
            mjpeg_hub: Arc::clone(&self.mjpeg_hub),
            h264_hub: self.h264_hub.clone(),
        }))
    }

    fn runtime_observation(&self) -> RuntimeObservation {
        let focus = self.focus.lock().ok().map(|g| g.clone());
        RuntimeObservation::default_with_camera(true, focus)
    }
}

fn push_box<F: FnOnce(&mut Vec<u8>)>(out: &mut Vec<u8>, tag: &[u8; 4], body: F) {
    let start = out.len();
    out.extend_from_slice(&0_u32.to_be_bytes());
    out.extend_from_slice(tag);
    body(out);
    let len = u32::try_from(out.len() - start).unwrap_or(u32::MAX);
    out[start..start + 4].copy_from_slice(&len.to_be_bytes());
}

fn build_synthetic_synced_fmp4(
    session_id: &str,
    fps: u32,
    codec: VideoCodec,
    started_monotonic_ns: u64,
    frame_count: u64,
    timescale: u32,
) -> Vec<u8> {
    let ticks_per_frame = timescale / fps.max(1);
    let total_ticks = u32::try_from(frame_count * u64::from(ticks_per_frame)).unwrap_or(u32::MAX);
    let mut out = Vec::with_capacity(4096);

    push_box(&mut out, b"ftyp", |b| {
        b.extend_from_slice(b"isom");
        b.extend_from_slice(&0x200_u32.to_be_bytes());
        b.extend_from_slice(b"isomiso6mp41");
    });

    push_box(&mut out, b"moov", |b| {
        push_box(b, b"mvhd", |m| {
            m.extend_from_slice(&[0_u8; 12]);
            m.extend_from_slice(&timescale.to_be_bytes());
            m.extend_from_slice(&total_ticks.to_be_bytes());
            m.extend_from_slice(&0x0001_0000_u32.to_be_bytes());
            m.extend_from_slice(&0x0100_u16.to_be_bytes());
            m.extend_from_slice(&[0_u8; 10]);
            for word in [0x0001_0000_u32, 0, 0, 0, 0x0001_0000, 0, 0, 0, 0x4000_0000] {
                m.extend_from_slice(&word.to_be_bytes());
            }
            m.extend_from_slice(&[0_u8; 24]);
            m.extend_from_slice(&4_u32.to_be_bytes());
        });
        push_box(b, b"mvex", |mv| {
            push_box(mv, b"mehd", |me| {
                me.extend_from_slice(&0_u32.to_be_bytes());
                me.extend_from_slice(&total_ticks.to_be_bytes());
            });
            for tid in 1_u32..=3 {
                push_box(mv, b"trex", |tx| {
                    tx.extend_from_slice(&0_u32.to_be_bytes());
                    tx.extend_from_slice(&tid.to_be_bytes());
                    tx.extend_from_slice(&1_u32.to_be_bytes());
                    tx.extend_from_slice(&0_u32.to_be_bytes());
                    tx.extend_from_slice(&0_u32.to_be_bytes());
                    tx.extend_from_slice(&0_u32.to_be_bytes());
                });
            }
        });
        for tid in 1_u32..=3 {
            let is_video = tid <= 2;
            push_box(b, b"trak", |tr| {
                push_box(tr, b"tkhd", |tk| {
                    tk.extend_from_slice(&0x0000_0003_u32.to_be_bytes());
                    tk.extend_from_slice(&[0_u8; 8]);
                    tk.extend_from_slice(&tid.to_be_bytes());
                    tk.extend_from_slice(&0_u32.to_be_bytes());
                    tk.extend_from_slice(&total_ticks.to_be_bytes());
                    tk.extend_from_slice(&[0_u8; 16]);
                    for word in [0x0001_0000_u32, 0, 0, 0, 0x0001_0000, 0, 0, 0, 0x4000_0000] {
                        tk.extend_from_slice(&word.to_be_bytes());
                    }
                    let (w_fp, h_fp) = if is_video {
                        (1920_u32 << 16, 1080_u32 << 16)
                    } else {
                        (0_u32, 0_u32)
                    };
                    tk.extend_from_slice(&w_fp.to_be_bytes());
                    tk.extend_from_slice(&h_fp.to_be_bytes());
                });
                push_box(tr, b"mdia", |md| {
                    push_box(md, b"mdhd", |mh| {
                        mh.extend_from_slice(&[0_u8; 12]);
                        mh.extend_from_slice(&timescale.to_be_bytes());
                        mh.extend_from_slice(&total_ticks.to_be_bytes());
                        mh.extend_from_slice(&0x55C4_0000_u32.to_be_bytes());
                    });
                    push_box(md, b"hdlr", |hd| {
                        hd.extend_from_slice(&0_u32.to_be_bytes());
                        hd.extend_from_slice(&0_u32.to_be_bytes());
                        hd.extend_from_slice(if is_video { b"vide" } else { b"meta" });
                        hd.extend_from_slice(&[0_u8; 12]);
                        hd.extend_from_slice(if is_video {
                            b"VideoHandler\0"
                        } else {
                            b"MetaHandler\0"
                        });
                    });
                    push_box(md, b"minf", |mi| {
                        if is_video {
                            push_box(mi, b"vmhd", |vm| {
                                vm.extend_from_slice(&1_u32.to_be_bytes());
                                vm.extend_from_slice(&[0_u8; 8]);
                            });
                        } else {
                            push_box(mi, b"nmhd", |nm| nm.extend_from_slice(&0_u32.to_be_bytes()));
                        }
                        push_box(mi, b"dinf", |di| {
                            push_box(di, b"dref", |dr| {
                                dr.extend_from_slice(&0_u32.to_be_bytes());
                                dr.extend_from_slice(&1_u32.to_be_bytes());
                                push_box(dr, b"url ", |u| {
                                    u.extend_from_slice(&1_u32.to_be_bytes())
                                });
                            });
                        });
                        push_box(mi, b"stbl", |sb| {
                            push_box(sb, b"stsd", |sd| {
                                sd.extend_from_slice(&0_u32.to_be_bytes());
                                sd.extend_from_slice(&1_u32.to_be_bytes());
                                if is_video {
                                    let entry_tag = match codec {
                                        VideoCodec::H264 => b"avc1",
                                        VideoCodec::Hevc => b"hvc1",
                                    };
                                    push_box(sb_dummy(sd), entry_tag, |ve| {
                                        ve.extend_from_slice(&[0_u8; 6]);
                                        ve.extend_from_slice(&1_u16.to_be_bytes());
                                        ve.extend_from_slice(&[0_u8; 16]);
                                        ve.extend_from_slice(&1920_u16.to_be_bytes());
                                        ve.extend_from_slice(&1080_u16.to_be_bytes());
                                        ve.extend_from_slice(&0x0048_0000_u32.to_be_bytes());
                                        ve.extend_from_slice(&0x0048_0000_u32.to_be_bytes());
                                        ve.extend_from_slice(&0_u32.to_be_bytes());
                                        ve.extend_from_slice(&1_u16.to_be_bytes());
                                        ve.extend_from_slice(&[0_u8; 32]);
                                        ve.extend_from_slice(&0x0018_u16.to_be_bytes());
                                        ve.extend_from_slice(&0xFFFF_u16.to_be_bytes());
                                    });
                                } else {
                                    push_box(sb_dummy(sd), b"mett", |mt| {
                                        mt.extend_from_slice(&[0_u8; 6]);
                                        mt.extend_from_slice(&1_u16.to_be_bytes());
                                        mt.push(0);
                                        mt.extend_from_slice(b"application/x-miro-ego-sync+json\0");
                                    });
                                }
                            });
                            for leaf in [b"stts", b"stsc", b"stco"] {
                                push_box(sb, leaf, |x| x.extend_from_slice(&[0_u8; 8]));
                            }
                            push_box(sb, b"stsz", |x| x.extend_from_slice(&[0_u8; 12]));
                        });
                    });
                });
            });
        }
    });

    let moof_off = out.len();
    let v_sample = format!("miro-ego:{session_id}:synthetic").into_bytes();
    let mut mett_samples = Vec::with_capacity(frame_count as usize);
    for idx in 0..frame_count {
        let mono = started_monotonic_ns.saturating_add(idx * 33_333_333);
        let json_line = format!(
            "{{\"seq\":{idx},\"vseq\":{vseq},\"mono_ns\":{mono},\"raw_ns\":{mono},\"hw_s\":0,\"dur\":{ticks_per_frame},\"flags\":16,\"deq_ns\":{mono},\"cam_ctr\":null,\"imu\":[[{mono},0,0,16384,0,0,0]]}}",
            vseq = idx + 1,
        );
        mett_samples.push(json_line.into_bytes());
    }

    // Reserve moof with deterministic placeholder data_offsets, then patch after mdat payload start.
    let mut track_payloads: [Vec<Vec<u8>>; 3] = [
        vec![v_sample.clone(); frame_count as usize],
        vec![v_sample; frame_count as usize],
        mett_samples,
    ];
    let mut trun_off_positions = [0_usize; 3];
    push_box(&mut out, b"moof", |mf| {
        push_box(mf, b"mfhd", |fh| {
            fh.extend_from_slice(&0_u32.to_be_bytes());
            fh.extend_from_slice(&1_u32.to_be_bytes());
        });
        for tidx in 0..3 {
            let tid = (tidx as u32) + 1;
            push_box(mf, b"traf", |tf| {
                push_box(tf, b"tfhd", |th| {
                    th.extend_from_slice(&0x0002_0000_u32.to_be_bytes());
                    th.extend_from_slice(&tid.to_be_bytes());
                });
                push_box(tf, b"tfdt", |td| {
                    td.extend_from_slice(&0x0100_0000_u32.to_be_bytes());
                    td.extend_from_slice(&0_u64.to_be_bytes());
                });
                push_box(tf, b"trun", |rn| {
                    rn.extend_from_slice(&0x0000_0701_u32.to_be_bytes());
                    rn.extend_from_slice(&(frame_count as u32).to_be_bytes());
                    trun_off_positions[tidx] = rn.len();
                    rn.extend_from_slice(&0_i32.to_be_bytes());
                    for sample in &track_payloads[tidx] {
                        rn.extend_from_slice(&ticks_per_frame.to_be_bytes());
                        rn.extend_from_slice(&(sample.len() as u32).to_be_bytes());
                        rn.extend_from_slice(&0_u32.to_be_bytes());
                    }
                });
            });
        }
    });

    let mdat_payload_off = out.len() + 8;
    let mut cur_rel_off = (mdat_payload_off - moof_off) as i32;
    for tidx in 0..3 {
        let pos = trun_off_positions[tidx];
        out[pos..pos + 4].copy_from_slice(&cur_rel_off.to_be_bytes());
        for s in &track_payloads[tidx] {
            cur_rel_off += s.len() as i32;
        }
    }
    push_box(&mut out, b"mdat", |md| {
        for samples in &mut track_payloads {
            for s in samples.drain(..) {
                md.extend_from_slice(&s);
            }
        }
    });
    push_box(&mut out, b"mfra", |ma| {
        for tid in 1_u32..=2 {
            push_box(ma, b"tfra", |ta| {
                ta.extend_from_slice(&0x0100_0000_u32.to_be_bytes());
                ta.extend_from_slice(&tid.to_be_bytes());
                ta.extend_from_slice(&0_u32.to_be_bytes());
                ta.extend_from_slice(&1_u32.to_be_bytes());
                ta.extend_from_slice(&0_u64.to_be_bytes());
                ta.extend_from_slice(&(moof_off as u64).to_be_bytes());
                ta.extend_from_slice(&[1, 1, 1]);
            });
        }
        push_box(ma, b"mfro", |ro| {
            ro.extend_from_slice(&0_u32.to_be_bytes());
            ro.extend_from_slice(&0_u32.to_be_bytes());
        });
    });
    // Patch mfro.size at last 4 bytes of mfra.
    if let Some(mfra_start) = out
        .windows(4)
        .rposition(|w| w == b"mfra")
        .and_then(|i| i.checked_sub(4))
    {
        let total = u32::try_from(out.len() - mfra_start).unwrap_or(0);
        let tail = out.len();
        out[tail - 4..tail].copy_from_slice(&total.to_be_bytes());
    }
    out
}

fn sb_dummy(v: &mut Vec<u8>) -> &mut Vec<u8> {
    v
}

fn write_file_synced(path: &Path, bytes: &[u8]) -> StorageResult<()> {
    let mut file = File::create(path)
        .map_err(|err| StorageError::io("artifact_write_failed", "create artifact file", err))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|err| StorageError::io("artifact_write_failed", "write artifact file", err))
}
