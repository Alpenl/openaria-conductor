//! Real on-device hardware capture backend (`NativeCaptureBackend`) and shared
//! `network-operation.lock` wrapper (`NetworkLockCaptureBackend`).

use super::error_map::map_camera_focus_error;
use super::{ENCODER_FINISH_TIMEOUT, rfc3339_now_utc};
use bytes::Bytes;
use miro_ego_api::MjpegPreviewHub;
use miro_ego_capture::audio::{self, Recorder as AudioRecorder};
use miro_ego_capture::hardware::imu::{Collector as ImuCollector, ImuObservation};
use miro_ego_capture::hardware::imu_preview::{self, Preview as ImuPreview};
use miro_ego_capture::hardware::native_camera::{Stream as NativeCameraStream, StreamConfig};
use miro_ego_capture::hardware::preview::LatestBuffer;
use miro_ego_capture::session::active_take::ActiveTakeWriter;
use miro_ego_capture::session::capture_runtime::{FailureSink, RecordingTarget, Runtime};
use miro_ego_capture::session::recording::{RecordingSegmentPlanner, RecordingSink};
use miro_ego_capture::session::shm_ring::ShmRingConfig;
use miro_ego_capture::session::stereo_encoder::{
    EncoderAbortHandle, EncoderConfig, EncoderProcess, EncoderTransport,
};
use miro_ego_core::config::DeviceConfig;
use miro_ego_core::locks::{FlockGuard, LockMode, acquire_network_operation_lock};
use miro_ego_core::sys::clock::monotonic_ns;
use miro_ego_hw::thermal::read_soc_temperature_c;
use miro_ego_network::{ControllerStatus, NetworkRpcClient, ObservedDefaultRoute};
use miro_ego_storage::coordinator::{CAMERA_FOCUS_SCHEMA, DropEventRecord};
use miro_ego_storage::{
    ActiveCaptureHandle, BackendStartRequest, CameraFocusSetRequest, CameraFocusStatus,
    CaptureBackend, CaptureProgressSnapshot, CapturedArtifact, CapturedAudioOutcome,
    CapturedAudioSegment, CapturedSessionArtifacts, CapturedVideoSegment, RuntimeObservation,
    StopReason, StorageError, StorageResult,
};
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub(super) struct CaptureFailureLog {
    last_error: Mutex<Option<(String, String)>>,
}

impl CaptureFailureLog {
    pub(super) fn new() -> Self {
        Self {
            last_error: Mutex::new(None),
        }
    }

    pub(super) fn take(&self) -> Option<(String, String)> {
        self.last_error.lock().ok().and_then(|mut g| g.take())
    }
}

impl FailureSink for CaptureFailureLog {
    fn on_failure(&self, code: &str, message: &str) {
        if let Ok(mut guard) = self.last_error.lock() {
            if guard.is_none() {
                *guard = Some((code.to_owned(), message.to_owned()));
            }
        }
    }
}

pub(super) fn extract_sink_artifacts(
    snap: &miro_ego_capture::session::recording::RecordingSinkSnapshot,
) -> StorageResult<(CapturedArtifact, CapturedArtifact)> {
    let mut frames: Option<CapturedArtifact> = None;
    let mut imu: Option<CapturedArtifact> = None;
    for art in &snap.artifacts {
        let captured = CapturedArtifact {
            role: art.role.to_owned(),
            path: art.relative_path.to_owned(),
            media_type: "application/x-ndjson".to_owned(),
            bytes: art.bytes,
            sha256: Some(art.sha256.clone()),
        };
        if art.role == "frames.index" {
            frames = Some(captured);
        } else if art.role == "imu.samples" {
            imu = Some(captured);
        }
    }
    let frames_art = frames.ok_or_else(|| {
        StorageError::integrity(
            "missing_frames_artifact",
            "recording sink omitted frames.ndjson",
        )
    })?;
    let imu_art = imu.ok_or_else(|| {
        StorageError::integrity("missing_imu_artifact", "recording sink omitted imu.ndjson")
    })?;
    Ok((frames_art, imu_art))
}

fn imu_observation_to_openapi(obs: &ImuObservation, session_id: Option<&str>) -> Value {
    let sample = &obs.samples[1];
    let quality = match sample.sync_quality {
        "good" => "good",
        "degraded" => "degraded",
        _ => "insufficient",
    };
    json!({
        "session_id": session_id,
        "clock": {
            "time_base": "host_monotonic",
            "timestamp_ns": sample.host_monotonic_ns,
        },
        "raw": {
            "units": "raw_int16",
            "accelerometer": {
                "x": sample.accelerometer.x,
                "y": sample.accelerometer.y,
                "z": sample.accelerometer.z,
            },
            "gyroscope": {
                "x": sample.gyroscope.x,
                "y": sample.gyroscope.y,
                "z": sample.gyroscope.z,
            },
        },
        "sync": {
            "quality": quality,
        },
    })
}

fn start_idle_imu_preview_worker(
    imu_preview: &ImuPreview,
    imu_device: Option<&Path>,
    imu_xu_unavailable: &AtomicBool,
) {
    if imu_xu_unavailable.load(Ordering::Acquire) {
        return;
    }
    let Some(dev_str) = imu_device.and_then(Path::to_str) else {
        return;
    };
    imu_preview.start(imu_preview::Config {
        device: dev_str.to_owned(),
        unit: None,
        selector: 1,
        stale_poll_interval: Duration::from_millis(500),
        timeout: Duration::from_millis(50),
    });
}

fn derive_connection_method_from_observed(st: &ControllerStatus) -> &'static str {
    let is_up = |state: &str| matches!(state, "connected" | "active" | "degraded");
    if matches!(st.observed.default_route, ObservedDefaultRoute::Wired) {
        "ethernet_lan"
    } else if is_up(&st.observed.wired.state) {
        "ethernet_direct"
    } else if is_up(&st.observed.wifi_client.state) {
        "wifi_client"
    } else if is_up(&st.observed.ap.state) {
        "wifi_ap"
    } else {
        "offline"
    }
}

/// Wrapper around any [`CaptureBackend`] that holds a shared `flock` (`LOCK_SH`) on
/// `<run_dir>/network-operation.lock` for the entire lifetime of an active capture handle,
/// preventing `AuthoritativeNetworkController` and `miro-ego-wifi-watchdog` (`LOCK_EX | LOCK_NB`)
/// from disrupting active Wi-Fi/Ethernet links mid-recording.
pub struct NetworkLockCaptureBackend {
    inner: Arc<dyn CaptureBackend>,
    run_dir: PathBuf,
}

impl NetworkLockCaptureBackend {
    pub fn new(inner: Arc<dyn CaptureBackend>, run_dir: PathBuf) -> Self {
        Self { inner, run_dir }
    }
}

struct NetworkLockedActiveHandle {
    inner: Option<Box<dyn ActiveCaptureHandle>>,
    _network_lock: Option<FlockGuard>,
}

impl ActiveCaptureHandle for NetworkLockedActiveHandle {
    fn progress(&self) -> CaptureProgressSnapshot {
        match &self.inner {
            Some(h) => h.progress(),
            None => CaptureProgressSnapshot {
                elapsed_seconds: 0.0,
                captured_frames: 0,
                bytes_written: 0,
            },
        }
    }

    fn take_failure(&self) -> Option<(String, String)> {
        self.inner.as_ref().and_then(|h| h.take_failure())
    }

    fn finish(mut self: Box<Self>, reason: StopReason) -> StorageResult<CapturedSessionArtifacts> {
        let res = match self.inner.take() {
            Some(h) => h.finish(reason),
            None => Err(StorageError::service_unavailable(
                "capture_handle_missing",
                "active capture handle already consumed",
            )),
        };
        let _ = self._network_lock.take();
        res
    }

    fn abort(mut self: Box<Self>) {
        if let Some(h) = self.inner.take() {
            h.abort();
        }
        let _ = self._network_lock.take();
    }
}

impl Drop for NetworkLockedActiveHandle {
    fn drop(&mut self) {
        if let Some(h) = self.inner.take() {
            h.abort();
        }
        let _ = self._network_lock.take();
    }
}

impl CaptureBackend for NetworkLockCaptureBackend {
    fn is_camera_connected(&self) -> bool {
        self.inner.is_camera_connected()
    }

    fn camera_focus(&self) -> StorageResult<Option<CameraFocusStatus>> {
        self.inner.camera_focus()
    }

    fn set_camera_focus(&self, req: &CameraFocusSetRequest) -> StorageResult<CameraFocusStatus> {
        self.inner.set_camera_focus(req)
    }

    fn begin_capture(
        &self,
        req: &BackendStartRequest,
    ) -> StorageResult<Box<dyn ActiveCaptureHandle>> {
        let lock_guard = acquire_network_operation_lock(&self.run_dir, LockMode::Shared, false)
            .map_err(|err| {
                StorageError::service_unavailable(
                    "network_operation_lock_failed",
                    format!("failed to acquire shared network-operation.lock: {err}"),
                )
            })?;
        let handle = self.inner.begin_capture(req)?;
        Ok(Box::new(NetworkLockedActiveHandle {
            inner: Some(handle),
            _network_lock: Some(lock_guard),
        }))
    }

    fn runtime_observation(&self) -> RuntimeObservation {
        self.inner.runtime_observation()
    }
}

type ActiveImuSlot = Arc<Mutex<Option<(String, Arc<ImuCollector>)>>>;

/// Real on-device hardware capture backend wiring `miro-ego-capture` (`NativeCameraStream`,
/// `Runtime`, `EncoderProcess` over `MEGO_SHM` v1, `RecordingSink`, optional IMU & ALSA audio)
/// to `miro-ego-storage::CaptureCoordinator`.
pub struct NativeCaptureBackend {
    config: DeviceConfig,
    encoder_bin: PathBuf,
    imu_device: Option<PathBuf>,
    imu_preview: Arc<ImuPreview>,
    imu_xu_unavailable: Arc<AtomicBool>,
    active_imu: ActiveImuSlot,
    network_client: Option<NetworkRpcClient>,
    stream: Mutex<Option<Arc<NativeCameraStream>>>,
    runtime: Mutex<Option<Arc<Runtime>>>,
    preview_buf: Arc<LatestBuffer>,
    preview_stop: Arc<AtomicBool>,
    preview_thread: Mutex<Option<JoinHandle<()>>>,
    mjpeg_hub: Arc<MjpegPreviewHub>,
}

impl NativeCaptureBackend {
    pub fn open(
        config: DeviceConfig,
        encoder_bin: PathBuf,
        imu_device: Option<PathBuf>,
        mjpeg_hub: Arc<MjpegPreviewHub>,
    ) -> StorageResult<Self> {
        Self::open_with_network(config, encoder_bin, imu_device, mjpeg_hub, None)
    }

    pub fn open_with_network(
        config: DeviceConfig,
        encoder_bin: PathBuf,
        imu_device: Option<PathBuf>,
        mjpeg_hub: Arc<MjpegPreviewHub>,
        network_client: Option<NetworkRpcClient>,
    ) -> StorageResult<Self> {
        let preview_fps = config.camera.fps.clamp(1, 30);
        let preview_buf = Arc::new(LatestBuffer::new(preview_fps).map_err(|err| {
            StorageError::service_unavailable("preview_init_failed", err.message)
        })?);
        let preview_stop = Arc::new(AtomicBool::new(false));
        let backend = Self {
            config,
            encoder_bin,
            imu_device,
            imu_preview: Arc::new(ImuPreview::default()),
            imu_xu_unavailable: Arc::new(AtomicBool::new(false)),
            active_imu: Arc::new(Mutex::new(None)),
            network_client,
            stream: Mutex::new(None),
            runtime: Mutex::new(None),
            preview_buf,
            preview_stop,
            preview_thread: Mutex::new(None),
            mjpeg_hub,
        };
        // Best-effort camera open at startup so daemon still boots even if camera is unplugged.
        let _ = backend.try_connect_camera();
        Ok(backend)
    }

    /// Attempt to open (or recover an exited preview worker on) the V4L2 camera stream.
    /// Invoked at daemon startup, from the 1 Hz background supervisor while `Idle`, and on `begin_capture`.
    pub fn try_connect_camera(&self) -> StorageResult<()> {
        let mut stream_guard = self.stream.lock().map_err(|_| {
            StorageError::service_unavailable("camera_poisoned", "camera mutex poisoned")
        })?;
        if stream_guard.is_some() {
            let existing_rt = self.runtime.lock().ok().and_then(|g| g.clone());
            if let Some(rt) = existing_rt {
                if rt.snapshot().is_ok_and(|snap| snap.running) {
                    return Ok(());
                }
                if rt.start_preview().is_ok() {
                    self.start_preview_forwarder();
                    if self.active_imu.lock().ok().is_some_and(|g| g.is_none()) {
                        start_idle_imu_preview_worker(
                            &self.imu_preview,
                            self.imu_device.as_deref(),
                            &self.imu_xu_unavailable,
                        );
                    }
                    return Ok(());
                }
                let _ = rt.close(Duration::from_millis(500));
            }
            if let Ok(mut rt_guard) = self.runtime.lock() {
                *rt_guard = None;
            }
            if let Some(old_stream) = stream_guard.take() {
                let _ = old_stream.close();
            }
            self.imu_preview.stop();
        }

        let device_str = self.config.camera.device.to_string_lossy().into_owned();
        let stream = NativeCameraStream::open_with_exposure(
            StreamConfig {
                device: &device_str,
                width: self.config.camera.width,
                height: self.config.camera.height,
                fps: self.config.camera.fps,
                encoding: "mjpeg",
                buffer_count: 6,
                queue_capacity: 16,
                split_eyes: false,
            },
            self.config.camera.exposure_time_absolute.clone(),
        )
        .map_err(|err| StorageError::camera_not_connected(err.message))?;
        let stream = Arc::new(stream);
        let runtime = Arc::new(
            Runtime::new(
                Arc::clone(&stream),
                Arc::clone(&self.preview_buf),
                u64::from(self.config.camera.frame_decimation),
                Duration::from_millis(500),
                None,
            )
            .map_err(|err| {
                StorageError::service_unavailable("capture_runtime_failed", err.message)
            })?,
        );
        runtime
            .start_preview()
            .map_err(|err| StorageError::camera_not_connected(err.message))?;

        self.start_preview_forwarder();
        *stream_guard = Some(stream);
        if let Ok(mut rt_guard) = self.runtime.lock() {
            *rt_guard = Some(runtime);
        }
        if self.active_imu.lock().ok().is_some_and(|g| g.is_none()) {
            start_idle_imu_preview_worker(
                &self.imu_preview,
                self.imu_device.as_deref(),
                &self.imu_xu_unavailable,
            );
        }
        Ok(())
    }

    pub(super) fn is_preview_worker_running(&self) -> bool {
        let has_stream = self.stream.lock().ok().is_some_and(|g| g.is_some());
        if !has_stream {
            return false;
        }
        self.runtime
            .lock()
            .ok()
            .and_then(|g| g.clone())
            .is_some_and(|rt| rt.snapshot().is_ok_and(|snap| snap.running))
    }

    fn start_preview_forwarder(&self) {
        let Ok(mut thread_guard) = self.preview_thread.lock() else {
            return;
        };
        if thread_guard.is_some() {
            return;
        }
        self.preview_stop.store(false, Ordering::Release);
        let mut subscriber = self.preview_buf.subscribe();
        let stop = Arc::clone(&self.preview_stop);
        let hub = Arc::clone(&self.mjpeg_hub);
        if let Ok(handle) = thread::Builder::new()
            .name("miro-ego-preview-fanout".to_owned())
            .spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    match subscriber.recv_latest(&stop, Duration::from_millis(200)) {
                        Ok(Some(snapshot)) => {
                            let _ = hub.publish(Bytes::copy_from_slice(&snapshot.jpeg));
                        }
                        Ok(None) => {}
                        Err(_) => thread::sleep(Duration::from_millis(50)),
                    }
                }
            })
        {
            *thread_guard = Some(handle);
        }
    }

    pub fn shutdown(&self) {
        self.imu_preview.stop();
        if let Ok(mut guard) = self.active_imu.lock() {
            *guard = None;
        }
        self.preview_stop.store(true, Ordering::Release);
        if let Ok(mut guard) = self.preview_thread.lock() {
            if let Some(handle) = guard.take() {
                let _ = handle.join();
            }
        }
        if let Ok(mut guard) = self.runtime.lock() {
            if let Some(rt) = guard.take() {
                let _ = rt.close(Duration::from_secs(3));
            }
        }
        if let Ok(mut guard) = self.stream.lock() {
            if let Some(stream) = guard.take() {
                let _ = stream.close();
            }
        }
    }
}

impl Drop for NativeCaptureBackend {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct NativeActiveHandle {
    runtime: Arc<Runtime>,
    active_take: Arc<Mutex<ActiveTakeWriter>>,
    sink: Arc<Mutex<RecordingSink>>,
    encoder: Arc<Mutex<EncoderProcess>>,
    encoder_abort: EncoderAbortHandle,
    segment_planner: Arc<Mutex<RecordingSegmentPlanner>>,
    failure_log: Arc<CaptureFailureLog>,
    audio: Option<Arc<AudioRecorder>>,
    audio_requested_mode: String,
    audio_not_recorded_reason: String,
    imu_preview: Arc<ImuPreview>,
    imu_device: Option<PathBuf>,
    imu_xu_unavailable: Arc<AtomicBool>,
    active_imu: ActiveImuSlot,
    started_monotonic_ns: u64,
    effective_fps: f64,
    completed: bool,
}

impl NativeActiveHandle {
    fn release_active_imu_and_resume_preview(&self) {
        if let Ok(mut guard) = self.active_imu.lock() {
            *guard = None;
        }
        start_idle_imu_preview_worker(
            &self.imu_preview,
            self.imu_device.as_deref(),
            &self.imu_xu_unavailable,
        );
    }

    fn abort_resources(&mut self) {
        if self.completed {
            return;
        }
        self.completed = true;
        self.encoder_abort.abort();
        let _ = self.runtime.stop_recording(Duration::from_secs(2));
        if let Some(ref audio) = self.audio {
            let _ = audio.stop(Duration::from_secs(2));
        }
        if let Ok(mut enc) = self.encoder.lock() {
            enc.abort();
        }
        if let Ok(mut sink) = self.sink.lock() {
            sink.close();
        }
        self.release_active_imu_and_resume_preview();
    }
}

impl Drop for NativeActiveHandle {
    fn drop(&mut self) {
        self.abort_resources();
    }
}

impl ActiveCaptureHandle for NativeActiveHandle {
    fn progress(&self) -> CaptureProgressSnapshot {
        let now_ns = monotonic_ns().unwrap_or(self.started_monotonic_ns);
        let elapsed_seconds =
            now_ns.saturating_sub(self.started_monotonic_ns) as f64 / 1_000_000_000.0;
        let (frames, bytes) = self
            .sink
            .lock()
            .map(|s| {
                let snap = s.snapshot();
                (snap.frames_written, snap.bytes_written)
            })
            .unwrap_or((0, 0));
        CaptureProgressSnapshot {
            elapsed_seconds,
            captured_frames: frames,
            bytes_written: bytes,
        }
    }

    fn take_failure(&self) -> Option<(String, String)> {
        self.failure_log.take()
    }

    fn finish(mut self: Box<Self>, _reason: StopReason) -> StorageResult<CapturedSessionArtifacts> {
        let _ = self.runtime.stop_recording(Duration::from_secs(5));

        if let Some((code, msg)) = self.failure_log.take() {
            self.encoder_abort.abort();
            if let Some(ref audio) = self.audio {
                let _ = audio.stop(Duration::from_secs(2));
            }
            if let Ok(mut enc) = self.encoder.lock() {
                enc.abort();
            }
            if let Ok(mut sink) = self.sink.lock() {
                sink.close();
            }
            self.completed = true;
            self.release_active_imu_and_resume_preview();
            return Err(StorageError::service_unavailable(
                "capture_failed",
                format!("{code}: {msg}"),
            ));
        }

        let segment_events = {
            let mut enc = self.encoder.lock().map_err(|_| {
                StorageError::service_unavailable("encoder_poisoned", "encoder mutex poisoned")
            })?;
            enc.finish(ENCODER_FINISH_TIMEOUT).map_err(|err| {
                StorageError::service_unavailable(
                    "encoder_finish_failed",
                    format!("{}: {}", err.code, err.message),
                )
            })?
        };

        let sink_snap = {
            let mut sink = self.sink.lock().map_err(|_| {
                StorageError::service_unavailable("sink_poisoned", "recording sink mutex poisoned")
            })?;
            sink.flush_and_close().map_err(|err| {
                StorageError::service_unavailable(
                    "sink_flush_failed",
                    format!("{}: {}", err.code, err.message),
                )
            })?
        };

        let now_ns = monotonic_ns().unwrap_or(self.started_monotonic_ns);
        let duration_seconds =
            (now_ns.saturating_sub(self.started_monotonic_ns) as f64 / 1_000_000_000.0).max(0.001);

        let take_summary = {
            let mut take = self.active_take.lock().map_err(|_| {
                StorageError::service_unavailable("take_poisoned", "active take mutex poisoned")
            })?;
            take.finish().map_err(|err| {
                StorageError::integrity(
                    "active_take_finish_failed",
                    format!("{}: {}", err.code, err.message),
                )
            })?
        };

        let segments: Vec<CapturedVideoSegment> = {
            let mut planner = self.segment_planner.lock().map_err(|_| {
                StorageError::service_unavailable(
                    "planner_poisoned",
                    "segment planner mutex poisoned",
                )
            })?;
            for ev in &segment_events {
                planner
                    .register_segment(ev.index, ev.start_frame, ev.end_frame)
                    .map_err(|err| {
                        StorageError::integrity(
                            "segment_plan_invalid",
                            format!("{}: {}", err.code, err.message),
                        )
                    })?;
            }
            planner
                .finish(
                    sink_snap.frames_written,
                    take_summary.frame_domain,
                    duration_seconds,
                )
                .map_err(|err| {
                    StorageError::integrity(
                        "segment_plan_incomplete",
                        format!("{}: {}", err.code, err.message),
                    )
                })?;
            let mut built = Vec::with_capacity(segment_events.len());
            for ev in &segment_events {
                let b0 = planner
                    .boundary(ev.start_frame, duration_seconds)
                    .map_err(|err| {
                        StorageError::integrity("segment_boundary_invalid", err.message)
                    })?;
                let b1 = planner
                    .boundary(ev.end_frame, duration_seconds)
                    .map_err(|err| {
                        StorageError::integrity("segment_boundary_invalid", err.message)
                    })?;
                built.push(CapturedVideoSegment::from_encoder_segment(
                    ev,
                    b0.time_seconds,
                    b1.time_seconds,
                ));
            }
            built
        };

        let (frames_artifact, imu_artifact) = extract_sink_artifacts(&sink_snap)?;

        let audio = match self.audio.take() {
            Some(recorder) => match recorder.stop(Duration::from_secs(5)) {
                Ok(res) => {
                    let clock_json = serde_json::to_value(&res.capture_clock).ok();
                    CapturedAudioOutcome::Recorded {
                        requested_mode: self.audio_requested_mode.clone(),
                        sample_rate: res.sample_rate_hz,
                        channels: res.channels,
                        sample_count: res.sample_count,
                        start_time_seconds: 0.0,
                        end_time_seconds: duration_seconds,
                        segments: res
                            .segments
                            .into_iter()
                            .map(|seg| CapturedAudioSegment {
                                index: seg.index,
                                start_sample: seg.start_sample,
                                end_sample: seg.end_sample,
                                start_time_seconds: seg.start_time_seconds,
                                end_time_seconds: seg.end_time_seconds,
                                pcm_payload_bytes: seg
                                    .bytes
                                    .saturating_sub(audio::WAV_HEADER_BYTES),
                                wav_header_bytes: audio::WAV_HEADER_BYTES,
                                artifact: CapturedArtifact {
                                    role: "audio.wav".to_owned(),
                                    path: seg.relative_path,
                                    media_type: "audio/wav".to_owned(),
                                    bytes: seg.bytes,
                                    sha256: seg.sha256,
                                },
                            })
                            .collect(),
                    }
                    .with_capture_clock(clock_json)
                }
                Err(_err) => CapturedAudioOutcome::NotRecorded {
                    requested_mode: self.audio_requested_mode.clone(),
                    reason: "recording_interrupted".to_owned(),
                },
            },
            None => CapturedAudioOutcome::NotRecorded {
                requested_mode: self.audio_requested_mode.clone(),
                reason: self.audio_not_recorded_reason.clone(),
            },
        };

        let dropped_frames = take_summary.dropped_frames;
        let drop_events = take_summary
            .drop_events
            .into_iter()
            .map(|ev| DropEventRecord {
                start_frame: ev.start_frame,
                end_frame: ev.end_frame,
                at_time_seconds: ev.at_time_seconds,
                reason: ev.reason.to_owned(),
                dropped: ev.dropped,
            })
            .collect();

        self.completed = true;
        self.release_active_imu_and_resume_preview();

        Ok(CapturedSessionArtifacts {
            segments,
            frames_artifact,
            frames_count: sink_snap.frames_written,
            imu_artifact,
            imu_sample_count: sink_snap.imu_samples_written,
            audio,
            logs: Vec::new(),
            dropped_frames,
            drop_events,
            duration_seconds,
            effective_fps: if duration_seconds > 0.0 {
                sink_snap.frames_written as f64 / duration_seconds
            } else {
                self.effective_fps
            },
        })
    }

    fn abort(mut self: Box<Self>) {
        self.abort_resources();
    }
}

impl CaptureBackend for NativeCaptureBackend {
    /// Non-mutating camera liveness check safe for read-only HTTP endpoints
    /// (`GET /device`, `GET /capture/status`, `GET /preview`, `GET /camera/focus`).
    fn is_camera_connected(&self) -> bool {
        self.is_preview_worker_running()
    }

    fn camera_focus(&self) -> StorageResult<Option<CameraFocusStatus>> {
        if !self.is_camera_connected() {
            return Err(StorageError::camera_not_connected(
                "camera is not connected",
            ));
        }
        let Some(stream) = self.stream.lock().ok().and_then(|g| g.clone()) else {
            return Err(StorageError::camera_not_connected(
                "camera is not connected",
            ));
        };
        let status = stream
            .focus_status()
            .map_err(|err| map_camera_focus_error(err.code, err.message))?;
        Ok(status.map(|f| CameraFocusStatus {
            schema: CAMERA_FOCUS_SCHEMA.to_owned(),
            value: u32::try_from(f.value).unwrap_or(0),
            minimum: u32::try_from(f.minimum).unwrap_or(0),
            maximum: u32::try_from(f.maximum).unwrap_or(0),
            step: u32::try_from(f.step).unwrap_or(1).max(1),
            default: u32::try_from(f.default_value).unwrap_or(0),
            auto_supported: f.auto_supported,
            auto_enabled: f.auto_enabled,
        }))
    }

    fn set_camera_focus(&self, req: &CameraFocusSetRequest) -> StorageResult<CameraFocusStatus> {
        if !self.is_camera_connected() {
            return Err(StorageError::camera_not_connected(
                "camera is not connected",
            ));
        }
        let Some(stream) = self.stream.lock().ok().and_then(|g| g.clone()) else {
            return Err(StorageError::camera_not_connected(
                "camera is not connected",
            ));
        };
        let val = req.value.map(i32::try_from).transpose().map_err(|_| {
            StorageError::unprocessable("invalid_camera_focus", "focus out of range")
        })?;
        let updated = stream
            .set_focus(val, req.auto_enabled)
            .map_err(|err| map_camera_focus_error(err.code, err.message))?;
        Ok(CameraFocusStatus {
            schema: CAMERA_FOCUS_SCHEMA.to_owned(),
            value: u32::try_from(updated.value).unwrap_or(0),
            minimum: u32::try_from(updated.minimum).unwrap_or(0),
            maximum: u32::try_from(updated.maximum).unwrap_or(0),
            step: u32::try_from(updated.step).unwrap_or(1).max(1),
            default: u32::try_from(updated.default_value).unwrap_or(0),
            auto_supported: updated.auto_supported,
            auto_enabled: updated.auto_enabled,
        })
    }

    fn begin_capture(
        &self,
        req: &BackendStartRequest,
    ) -> StorageResult<Box<dyn ActiveCaptureHandle>> {
        self.try_connect_camera()?;
        let runtime = self
            .runtime
            .lock()
            .ok()
            .and_then(|g| g.clone())
            .ok_or_else(|| StorageError::camera_not_connected("capture runtime unavailable"))?;

        // Stop idle IMU preview BEFORE opening the recording IMU collector so preview never
        // steals packets or contends on the UVC Extension Unit control channel.
        self.imu_preview.stop();

        let imu = match self.imu_device.as_deref() {
            Some(dev_path) => {
                let Some(dev_str) = dev_path.to_str() else {
                    return Err(StorageError::invalid_input(
                        "imu_device_invalid",
                        "IMU device path is not valid UTF-8",
                    ));
                };
                match ImuCollector::open(dev_str, None, 1, None) {
                    Ok(collector) => Some(Arc::new(collector)),
                    Err(err) if err.is_xu_not_found() || err.is_xu_unavailable() => {
                        self.imu_xu_unavailable.store(true, Ordering::Release);
                        eprintln!(
                            "miro-ego: IMU XU unavailable on {} ({}: {}); proceeding without IMU",
                            dev_path.display(),
                            err.code,
                            err.message
                        );
                        None
                    }
                    Err(err) => {
                        start_idle_imu_preview_worker(
                            &self.imu_preview,
                            self.imu_device.as_deref(),
                            &self.imu_xu_unavailable,
                        );
                        return Err(StorageError::service_unavailable(
                            "imu_open_failed",
                            format!("{}: {}", err.code, err.message),
                        ));
                    }
                }
            }
            None => None,
        };

        let effective_fps = (f64::from(self.config.camera.fps)
            / f64::from(self.config.camera.frame_decimation))
        .max(1.0);
        let gop = u64::from(req.encoding.gop_frames.max(1));
        let raw_seg_frames =
            ((effective_fps * self.config.segment_seconds).round() as u64).max(gop);
        let segment_frames = raw_seg_frames.div_ceil(gop) * gop;
        let video_dir = req.staging_dir.join("video");
        fs::create_dir_all(&video_dir).map_err(|err| {
            StorageError::io("staging_create_failed", "create staging video dir", err)
        })?;

        let mut encoder = EncoderProcess::with_transport(
            EncoderConfig {
                out_dir: &video_dir,
                executable: &self.encoder_bin,
                width: u64::from(self.config.camera.width),
                height: u64::from(self.config.camera.height),
                fps: effective_fps.round() as u64,
                bitrate_kbps: u64::from(req.encoding.bitrate_kbps),
                segment_frames,
                path_prefix: "video/",
            },
            EncoderTransport::ShmRing(ShmRingConfig::default()),
        )
        .map_err(|err| StorageError::service_unavailable("encoder_init_failed", err.message))?;

        encoder
            .configure(&req.encoding.encoder_arguments())
            .map_err(|err| StorageError::invalid_input("encoder_config_invalid", err.message))?;
        encoder.start().map_err(|err| {
            StorageError::service_unavailable("encoder_start_failed", err.message)
        })?;

        let mut sink = match RecordingSink::create(&req.staging_dir, &req.session_id) {
            Ok(s) => s,
            Err(err) => {
                encoder.abort();
                start_idle_imu_preview_worker(
                    &self.imu_preview,
                    self.imu_device.as_deref(),
                    &self.imu_xu_unavailable,
                );
                return Err(StorageError::service_unavailable(
                    "sink_create_failed",
                    err.message,
                ));
            }
        };
        let active_take = match ActiveTakeWriter::new(&req.session_id) {
            Ok(t) => t,
            Err(err) => {
                encoder.abort();
                sink.close();
                start_idle_imu_preview_worker(
                    &self.imu_preview,
                    self.imu_device.as_deref(),
                    &self.imu_xu_unavailable,
                );
                return Err(StorageError::service_unavailable(
                    "take_create_failed",
                    err.message,
                ));
            }
        };
        let segment_planner = match RecordingSegmentPlanner::new(segment_frames) {
            Ok(p) => p,
            Err(err) => {
                encoder.abort();
                sink.close();
                start_idle_imu_preview_worker(
                    &self.imu_preview,
                    self.imu_device.as_deref(),
                    &self.imu_xu_unavailable,
                );
                return Err(StorageError::invalid_input("segment_invalid", err.message));
            }
        };

        let audio_requested_mode = if self.config.audio.enabled {
            "enabled".to_owned()
        } else {
            "disabled".to_owned()
        };
        let (audio, audio_not_recorded_reason) = if self.config.audio.enabled {
            if !audio::available() {
                eprintln!(
                    "miro-ego: audio enabled in config ({}) but ALSA capture is unavailable; recording without audio",
                    self.config.audio.device
                );
                (None, "audio_open_failed".to_owned())
            } else {
                let root_str = req.staging_dir.to_string_lossy().into_owned();
                match AudioRecorder::new(
                    &root_str,
                    &self.config.audio.device,
                    self.config.audio.sample_rate_hz,
                    self.config.audio.channels,
                    self.config.segment_seconds,
                ) {
                    Ok(rec) => {
                        let rec = Arc::new(rec);
                        match rec.start() {
                            Ok(()) => (Some(rec), "device_default_disabled".to_owned()),
                            Err(err) => {
                                eprintln!(
                                    "miro-ego: failed starting ALSA audio capture on {}: {}; recording without audio",
                                    self.config.audio.device, err.message
                                );
                                (None, "audio_open_failed".to_owned())
                            }
                        }
                    }
                    Err(err) => {
                        eprintln!(
                            "miro-ego: failed opening ALSA audio capture on {}: {}; recording without audio",
                            self.config.audio.device, err.message
                        );
                        (None, "audio_open_failed".to_owned())
                    }
                }
            }
        } else {
            (None, "device_default_disabled".to_owned())
        };

        let encoder_abort = encoder.abort_handle();
        let active_take = Arc::new(Mutex::new(active_take));
        let sink = Arc::new(Mutex::new(sink));
        let encoder = Arc::new(Mutex::new(encoder));
        let segment_planner = Arc::new(Mutex::new(segment_planner));
        let failure_log = Arc::new(CaptureFailureLog::new());

        if let Some(ref collector) = imu {
            if let Ok(mut guard) = self.active_imu.lock() {
                *guard = Some((req.session_id.clone(), Arc::clone(collector)));
            }
        }

        let handle = Box::new(NativeActiveHandle {
            runtime: Arc::clone(&runtime),
            active_take: Arc::clone(&active_take),
            sink: Arc::clone(&sink),
            encoder: Arc::clone(&encoder),
            encoder_abort,
            segment_planner: Arc::clone(&segment_planner),
            failure_log: Arc::clone(&failure_log),
            audio: audio.clone(),
            audio_requested_mode,
            audio_not_recorded_reason,
            imu_preview: Arc::clone(&self.imu_preview),
            imu_device: self.imu_device.clone(),
            imu_xu_unavailable: Arc::clone(&self.imu_xu_unavailable),
            active_imu: Arc::clone(&self.active_imu),
            started_monotonic_ns: req.started_monotonic_ns,
            effective_fps,
            completed: false,
        });

        runtime
            .prepare_recording(Duration::from_secs(2))
            .map_err(|err| {
                StorageError::service_unavailable("capture_prepare_failed", err.message)
            })?;
        runtime
            .start_recording(RecordingTarget {
                active_take,
                sink,
                encoder,
                segment_planner,
                recording_start_monotonic_ns: req.started_monotonic_ns,
                on_failure: failure_log as Arc<dyn FailureSink>,
                imu,
                imu_timeout: Duration::from_millis(50),
                audio,
                capture_stopped_monotonic_ns: Arc::new(AtomicU64::new(0)),
                recover_source_gaps: true,
            })
            .map_err(|err| {
                StorageError::service_unavailable("capture_start_failed", err.message)
            })?;

        Ok(handle)
    }

    fn runtime_observation(&self) -> RuntimeObservation {
        let connected = self.is_preview_worker_running();
        let focus = if connected {
            self.camera_focus().ok().flatten()
        } else {
            None
        };
        let temperature_celsius = read_soc_temperature_c()
            .map(|t| t.clamp(-40.0, 125.0))
            .unwrap_or(0.0);

        let (connection_method, network) = match self
            .network_client
            .as_ref()
            .and_then(|client| client.status().ok())
        {
            Some(st) => {
                let method = derive_connection_method_from_observed(&st).to_owned();
                let mut ap_json = serde_json::to_value(&st.observed.ap).unwrap_or_else(|_| {
                    json!({
                        "state": "unavailable",
                        "interface": Value::Null,
                        "addresses": [],
                        "peer_or_ssid": Value::Null,
                    })
                });
                if ap_json.get("peer_or_ssid").is_some_and(Value::is_null) {
                    if let Some(ref ssid) = st.rescue_ssid {
                        if let Some(obj) = ap_json.as_object_mut() {
                            obj.insert("peer_or_ssid".to_owned(), Value::String(ssid.clone()));
                        }
                    }
                }
                let net = json!({
                    "ap": ap_json,
                    "wifi_client": st.observed.wifi_client,
                    "wired": st.observed.wired,
                    "default_route": st.observed.default_route,
                });
                (method, net)
            }
            None => (
                "offline".to_owned(),
                json!({
                    "ap": {
                        "state": "unavailable",
                        "interface": Value::Null,
                        "addresses": [],
                        "peer_or_ssid": Value::Null,
                    },
                    "wifi_client": {
                        "state": "unavailable",
                        "interface": Value::Null,
                        "addresses": [],
                        "peer_or_ssid": Value::Null,
                    },
                    "wired": {
                        "state": "unavailable",
                        "interface": Value::Null,
                        "addresses": [],
                        "peer_or_ssid": Value::Null,
                    },
                    "default_route": "none",
                }),
            ),
        };

        let active_imu_entry = self.active_imu.lock().ok().and_then(|g| g.clone());
        let live_imu = match active_imu_entry {
            Some((session_id, collector)) => collector
                .latest_observation()
                .ok()
                .flatten()
                .map(|obs| imu_observation_to_openapi(&obs, Some(&session_id))),
            None => self
                .imu_preview
                .latest()
                .map(|obs| imu_observation_to_openapi(&obs, None)),
        };

        RuntimeObservation {
            observed_at: rfc3339_now_utc(),
            connection_method,
            temperature_celsius,
            network,
            live_imu,
            camera_connected: connected,
            camera_focus: focus,
        }
    }
}
