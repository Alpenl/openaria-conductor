//! Synthetic SHM-encoder-backed (`ShmEncoderCaptureBackend`) and pure-Rust deterministic
//! (`MockCaptureBackend`) capture backends for hermetic E2E tests and `miro-ego serve --mock`.

use super::native_capture::{CaptureFailureLog, extract_sink_artifacts};
use super::{
    ENCODER_FINISH_TIMEOUT, SYNTHETIC_FMP4_INIT, SYNTHETIC_FMP4_MOOF, SYNTHETIC_JPEG_FRAME,
    sha256_hex,
};
use bytes::Bytes;
use miro_ego_api::{H264PreviewHub, MjpegPreviewHub};
use miro_ego_capture::session::capture_runtime::FailureSink;
use miro_ego_capture::session::recording::{ImuSampleRecord, RecordingSink};
use miro_ego_capture::session::shm_ring::ShmRingConfig;
use miro_ego_capture::session::stereo_encoder::{
    EncoderConfig, EncoderProcess, EncoderTransport, min_shm_slot_capacity,
};
use miro_ego_core::sys::clock::monotonic_ns;
use miro_ego_hw::thermal::read_soc_temperature_c;
use miro_ego_storage::coordinator::CAMERA_FOCUS_SCHEMA;
use miro_ego_storage::{
    ActiveCaptureHandle, BackendStartRequest, CameraFocusSetRequest, CameraFocusStatus,
    CaptureBackend, CaptureProgressSnapshot, CapturedAudioOutcome, CapturedSessionArtifacts,
    CapturedVideoSegment, RuntimeObservation, StopReason, StorageError, StorageResult,
};
use std::fs::{self, File};
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
        let video_dir = req.staging_dir.join("video");
        fs::create_dir_all(&video_dir).map_err(|err| {
            StorageError::io("staging_create_failed", "create staging video dir", err)
        })?;

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
                out_dir: &video_dir,
                executable: &self.encoder_bin,
                width: u64::from(self.width),
                height: u64::from(self.height),
                fps: u64::from(self.fps),
                bitrate_kbps: u64::from(req.encoding.bitrate_kbps),
                segment_frames: self.segment_frames,
                path_prefix: "video/",
            },
            EncoderTransport::ShmRing(shm_cfg),
        )
        .map_err(|err| StorageError::service_unavailable("encoder_init_failed", err.message))?;

        encoder
            .configure(&req.encoding.encoder_arguments())
            .map_err(|err| StorageError::invalid_input("encoder_config_invalid", err.message))?;
        encoder.start().map_err(|err| {
            StorageError::service_unavailable("encoder_start_failed", err.message)
        })?;

        let mut sink = RecordingSink::create(&req.staging_dir, &req.session_id)
            .map_err(|err| StorageError::service_unavailable("sink_create_failed", err.message))?;

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
        let segment_frames = self.segment_frames;
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
                            sink.close();
                            return Err(StorageError::service_unavailable(
                                "encoder_submit_failed",
                                msg,
                            ));
                        }
                    };
                    let seg_index = seq / segment_frames;
                    let seg_frame = seq % segment_frames;
                    let idx_bytes = match sink.write_audited_frame_index(
                        seq,
                        seq + 1,
                        frame_ns,
                        seg_index,
                        seg_frame,
                        None,
                    ) {
                        Ok(n) => n,
                        Err(err) => {
                            failure_worker.on_failure("sink_write_failed", &err.message);
                            encoder.abort();
                            sink.close();
                            return Err(StorageError::service_unavailable(
                                "sink_write_failed",
                                err.message,
                            ));
                        }
                    };
                    let imu_bytes = match sink.write_imu_sample_record(&ImuSampleRecord {
                        sequence: seq,
                        packet_sequence: seq,
                        sample_index: 0,
                        device_timestamp_raw: u32::try_from(seq * 1000).unwrap_or(u32::MAX),
                        device_ticks: seq * 1000,
                        host_read_start_ns: frame_ns,
                        host_read_end_ns: frame_ns.saturating_add(100),
                        host_monotonic_ns: frame_ns,
                        accelerometer: (0, 0, 16384),
                        gyroscope: (0, 0, 0),
                        sync_offset_ns: Some(0),
                        sync_residual_ns: Some(0),
                        sync_quality: "nominal",
                    }) {
                        Ok(n) => n,
                        Err(err) => {
                            failure_worker.on_failure("sink_write_failed", &err.message);
                            encoder.abort();
                            sink.close();
                            return Err(StorageError::service_unavailable(
                                "sink_write_failed",
                                err.message,
                            ));
                        }
                    };

                    let _ = mjpeg_hub.publish(Bytes::from_static(SYNTHETIC_JPEG_FRAME));
                    if let Some(ref hub) = h264_hub {
                        let _ = hub.publish_fragment(Bytes::from_static(SYNTHETIC_FMP4_MOOF));
                    }

                    seq = seq.saturating_add(1);
                    frames_worker.store(seq, Ordering::Release);
                    bytes_worker.fetch_add(written + idx_bytes + imu_bytes, Ordering::AcqRel);

                    if abort_worker.load(Ordering::Acquire)
                        || (stop_worker.load(Ordering::Acquire) && seq >= min_frames)
                    {
                        break;
                    }
                    thread::sleep(frame_interval);
                }

                if abort_worker.load(Ordering::Acquire) {
                    encoder.abort();
                    sink.close();
                    return Err(StorageError::service_unavailable(
                        "capture_aborted",
                        "SHM capture worker aborted",
                    ));
                }

                let segment_events = match encoder.finish(ENCODER_FINISH_TIMEOUT) {
                    Ok(ev) => ev,
                    Err(err) => {
                        let msg = format!("{}: {}", err.code, err.message);
                        failure_worker.on_failure("encoder_finish_failed", &msg);
                        sink.close();
                        return Err(StorageError::service_unavailable(
                            "encoder_finish_failed",
                            msg,
                        ));
                    }
                };

                let sink_snap = sink.flush_and_close().map_err(|err| {
                    StorageError::service_unavailable("sink_flush_failed", err.message)
                })?;
                let (frames_artifact, imu_artifact) = extract_sink_artifacts(&sink_snap)?;

                let now_ns = monotonic_ns().unwrap_or(started_ns);
                let duration_seconds =
                    (now_ns.saturating_sub(started_ns) as f64 / 1_000_000_000.0).max(0.05);
                let fps_f = f64::from(fps.max(1));

                let segments = segment_events
                    .iter()
                    .map(|ev| {
                        CapturedVideoSegment::from_encoder_segment(
                            ev,
                            ev.start_frame as f64 / fps_f,
                            ev.end_frame as f64 / fps_f,
                        )
                    })
                    .collect();

                Ok(CapturedSessionArtifacts {
                    segments,
                    frames_artifact,
                    frames_count: sink_snap.frames_written,
                    imu_artifact,
                    imu_sample_count: sink_snap.imu_samples_written,
                    audio: CapturedAudioOutcome::NotRecorded {
                        requested_mode: "disabled".to_owned(),
                        reason: "device_default_disabled".to_owned(),
                    },
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
        let video_dir = self.staging_dir.join("video");
        fs::create_dir_all(&video_dir).map_err(|err| {
            StorageError::io("staging_create_failed", "create mock video dir", err)
        })?;

        let left_bytes = build_synthetic_mp4_payload(&self.session_id, "left");
        let right_bytes = build_synthetic_mp4_payload(&self.session_id, "right");
        let left_sha = sha256_hex(&left_bytes);
        let right_sha = sha256_hex(&right_bytes);

        write_file_synced(&video_dir.join("left_00000.mp4"), &left_bytes)?;
        write_file_synced(&video_dir.join("right_00000.mp4"), &right_bytes)?;

        let mut sink = RecordingSink::create(&self.staging_dir, &self.session_id)
            .map_err(|err| StorageError::service_unavailable("sink_create_failed", err.message))?;
        let frame_count: u64 = 5;
        for idx in 0..frame_count {
            let ts = self.started_monotonic_ns.saturating_add(idx * 33_333_333);
            sink.write_audited_frame_index(idx, idx + 1, ts, 0, idx, None)
                .map_err(|err| {
                    StorageError::service_unavailable("sink_write_failed", err.message)
                })?;
            sink.write_imu_sample_record(&ImuSampleRecord {
                sequence: idx,
                packet_sequence: idx,
                sample_index: 0,
                device_timestamp_raw: u32::try_from(idx * 1000).unwrap_or(u32::MAX),
                device_ticks: idx * 1000,
                host_read_start_ns: ts,
                host_read_end_ns: ts.saturating_add(100),
                host_monotonic_ns: ts,
                accelerometer: (0, 0, 16384),
                gyroscope: (0, 0, 0),
                sync_offset_ns: Some(0),
                sync_residual_ns: Some(0),
                sync_quality: "nominal",
            })
            .map_err(|err| StorageError::service_unavailable("sink_write_failed", err.message))?;
        }
        let sink_snap = sink
            .flush_and_close()
            .map_err(|err| StorageError::service_unavailable("sink_flush_failed", err.message))?;
        let (frames_artifact, imu_artifact) = extract_sink_artifacts(&sink_snap)?;

        let _ = self
            .mjpeg_hub
            .publish(Bytes::from_static(SYNTHETIC_JPEG_FRAME));
        if let Some(hub) = &self.h264_hub {
            let _ = hub.publish_fragment(Bytes::from_static(SYNTHETIC_FMP4_MOOF));
        }

        let now_ns = monotonic_ns().unwrap_or(self.started_monotonic_ns);
        let duration_seconds =
            (now_ns.saturating_sub(self.started_monotonic_ns) as f64 / 1_000_000_000.0).max(0.1);

        Ok(CapturedSessionArtifacts {
            segments: vec![CapturedVideoSegment {
                index: 0,
                start_frame: 0,
                end_frame: frame_count,
                start_time_seconds: 0.0,
                end_time_seconds: duration_seconds,
                left_path: "video/left_00000.mp4".to_owned(),
                left_bytes: left_bytes.len() as u64,
                left_sha256: Some(left_sha),
                right_path: "video/right_00000.mp4".to_owned(),
                right_bytes: right_bytes.len() as u64,
                right_sha256: Some(right_sha),
            }],
            frames_artifact,
            frames_count: sink_snap.frames_written,
            imu_artifact,
            imu_sample_count: sink_snap.imu_samples_written,
            audio: CapturedAudioOutcome::NotRecorded {
                requested_mode: "disabled".to_owned(),
                reason: "device_default_disabled".to_owned(),
            },
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

fn build_synthetic_mp4_payload(session_id: &str, eye: &str) -> Vec<u8> {
    let payload = format!("miro-ego:{session_id}:{eye}");
    let mdat_len = (8 + payload.len()) as u32;
    let mut out = Vec::with_capacity(64 + payload.len());
    // 24-byte `ftyp` box
    out.extend_from_slice(&24_u32.to_be_bytes());
    out.extend_from_slice(b"ftypisom\x00\x00\x02\x00isomiso6");
    // `mdat` box carrying non-empty payload
    out.extend_from_slice(&mdat_len.to_be_bytes());
    out.extend_from_slice(b"mdat");
    out.extend_from_slice(payload.as_bytes());
    // 16-byte `moov` box containing valid 8-byte `mvhd` child box
    out.extend_from_slice(&16_u32.to_be_bytes());
    out.extend_from_slice(b"moov");
    out.extend_from_slice(&8_u32.to_be_bytes());
    out.extend_from_slice(b"mvhd");
    out
}

fn write_file_synced(path: &Path, bytes: &[u8]) -> StorageResult<()> {
    let mut file = File::create(path)
        .map_err(|err| StorageError::io("artifact_write_failed", "create artifact file", err))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|err| StorageError::io("artifact_write_failed", "write artifact file", err))
}
