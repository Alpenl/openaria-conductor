use crate::audio::alsa_ffi::{Pcm, ReadOutcome};
use crate::audio::clock;
use crate::audio::wav_writer::SegmentWriter;
use crate::audio::{
    AudioError, AudioProgress, AudioRecordingResult, AudioRecordingSnapshot, BYTES_PER_SAMPLE,
    ClockCheckpoint, DEFAULT_PERIOD_FRAMES, MAX_CHANNELS, MAX_PENDING_COMMITS, QUEUE_BLOCKS,
    SAMPLE_CODEC, SAMPLE_FORMAT,
};
use miro_ego_core::sys::clock as sys_clock;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const START_TIMEOUT: Duration = Duration::from_secs(2);
const START_TIMEOUT_JOIN_WAIT: Duration = Duration::from_millis(200);
const READ_IDLE_SLEEP: Duration = Duration::from_millis(2);
const MAX_CLOCK_OBSERVATIONS: usize = 65_536;

#[derive(Clone)]
pub struct RecorderConfig {
    pub session_root: PathBuf,
    pub device: String,
    pub sample_rate_hz: u32,
    pub channels: u16,
    pub segment_seconds: f64,
}

pub struct Recorder {
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
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
    Aborted,
    Closed,
}

impl Recorder {
    pub fn new(
        session_root: &str,
        device: &str,
        sample_rate_hz: u32,
        channels: u16,
        segment_seconds: f64,
    ) -> Result<Self, AudioError> {
        if device.is_empty()
            || sample_rate_hz == 0
            || channels == 0
            || channels > MAX_CHANNELS
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

    pub fn start(&self) -> Result<(), AudioError> {
        {
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
            lifecycle.state = State::Starting;
        }
        self.stop.store(false, Ordering::Release);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (done_tx, done_rx) = mpsc::channel();
        let config = self.config.clone();
        let stop = Arc::clone(&self.stop);
        let progress = Arc::clone(&self.progress);
        let worker = match thread::Builder::new()
            .name("audio-capture".to_owned())
            .spawn(move || {
                let result = start_and_capture(config, stop, Arc::clone(&progress), ready_tx);
                if let Err(error) = &result {
                    progress.fail(error.clone());
                }
                let _ = done_tx.send(result);
            }) {
            Ok(worker) => worker,
            Err(error) => {
                if let Ok(mut lifecycle) = self.lifecycle.lock() {
                    if lifecycle.state == State::Starting {
                        lifecycle.state = State::Closed;
                    }
                }
                return Err(AudioError::new(
                    "audio_start_failed",
                    format!("spawn audio capture worker: {error}"),
                ));
            }
        };

        match ready_rx.recv_timeout(START_TIMEOUT) {
            Ok(Ok(())) => {
                let mut lifecycle = self.lifecycle.lock().map_err(|_| {
                    AudioError::new("native_audio_poisoned", "audio mutex is poisoned")
                })?;
                if lifecycle.state != State::Starting {
                    self.stop.store(true, Ordering::Release);
                    drop(lifecycle);
                    let _ = worker.join();
                    return Err(AudioError::new(
                        "invalid_state",
                        "native audio recorder was stopped during start",
                    ));
                }
                lifecycle.done = Some(done_rx);
                lifecycle.worker = Some(worker);
                lifecycle.state = State::Running;
                Ok(())
            }
            Ok(Err(error)) => {
                let _ = worker.join();
                let mut lifecycle = self.lifecycle.lock().map_err(|_| {
                    AudioError::new("native_audio_poisoned", "audio mutex is poisoned")
                })?;
                if lifecycle.state == State::Starting {
                    lifecycle.state = State::Closed;
                }
                Err(error)
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.stop.store(true, Ordering::Release);
                // Bound waiting outside the lifecycle lock; detach if ALSA initialization is wedged.
                if done_rx.recv_timeout(START_TIMEOUT_JOIN_WAIT).is_ok() {
                    let _ = worker.join();
                }
                if let Ok(mut lifecycle) = self.lifecycle.lock() {
                    if lifecycle.state == State::Starting {
                        lifecycle.state = State::Closed;
                    }
                }
                Err(AudioError::new(
                    "audio_start_timeout",
                    "native audio recorder did not initialize in time",
                ))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = worker.join();
                if let Ok(mut lifecycle) = self.lifecycle.lock() {
                    if lifecycle.state == State::Starting {
                        lifecycle.state = State::Closed;
                    }
                }
                Err(AudioError::new(
                    "audio_start_failed",
                    "native audio recorder worker exited before ready",
                ))
            }
        }
    }

    /// Signal capture only. The worker still drains accepted PCM and commits it;
    /// stop() retains ownership of waiting, joining, and reporting its result.
    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub fn stop_requested(&self) -> bool {
        self.stop.load(Ordering::Acquire)
    }

    pub fn stop(&self, timeout: Duration) -> Result<AudioRecordingResult, AudioError> {
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
                State::Starting | State::Stopping => {
                    return Err(AudioError::new(
                        "invalid_state",
                        "native audio recorder transition is already in progress",
                    ));
                }
                State::Failed | State::Aborted | State::Closed => {
                    return Err(AudioError::new(
                        "invalid_state",
                        "native audio recorder is already closed",
                    ));
                }
                State::Running => {
                    self.stop.store(true, Ordering::Release);
                    lifecycle.state = State::Stopping;
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
                if lifecycle.state == State::Stopping {
                    match &result {
                        Ok(ok_result) => {
                            lifecycle.state = State::Stopped;
                            lifecycle.result = Some(ok_result.clone());
                        }
                        Err(_) => {
                            lifecycle.state = State::Failed;
                        }
                    }
                }
                result
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                {
                    let mut lifecycle = self.lifecycle.lock().map_err(|_| {
                        AudioError::new("native_audio_poisoned", "audio mutex is poisoned")
                    })?;
                    if lifecycle.state == State::Stopping {
                        lifecycle.done = Some(done);
                        lifecycle.worker = Some(worker);
                        lifecycle.state = State::Running;
                        return Err(AudioError::new(
                            "audio_stop_timeout",
                            "native audio recorder did not stop in time",
                        ));
                    }
                    if lifecycle.state == State::Aborted {
                        lifecycle.done = Some(done);
                        lifecycle.worker = Some(worker);
                        return Err(AudioError::new(
                            "audio_stop_timeout",
                            "native audio recorder did not stop in time",
                        ));
                    }
                }
                if done.try_recv().is_ok() {
                    let _ = worker.join();
                }
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
                if lifecycle.state == State::Stopping {
                    lifecycle.state = State::Closed;
                }
                Err(AudioError::new(
                    "audio_failed",
                    "native audio recorder exited without a result",
                ))
            }
        }
    }

    pub fn snapshot(&self) -> AudioRecordingSnapshot {
        self.progress.snapshot()
    }

    pub fn check_health(&self) -> Result<(), AudioError> {
        self.progress.check()
    }

    #[cfg(test)]
    pub fn inject_failure(&self, error: AudioError) {
        self.progress.fail(error);
    }

    #[cfg(test)]
    pub fn inject_completed_failure(&self, error: AudioError) {
        self.progress.fail(error.clone());
        let (sender, receiver) = mpsc::channel();
        if let Ok(worker) = thread::Builder::new()
            .name("audio-test-failure".to_owned())
            .spawn(move || {
                let _ = sender.send(Err(error));
            })
        {
            if let Ok(mut lifecycle) = self.lifecycle.lock() {
                lifecycle.state = State::Running;
                lifecycle.done = Some(receiver);
                lifecycle.worker = Some(worker);
            }
        }
    }

    pub fn abort(&self) {
        self.stop.store(true, Ordering::Release);
        let worker = self.lifecycle.lock().ok().and_then(|mut lifecycle| {
            lifecycle.done = None;
            if !matches!(lifecycle.state, State::Stopped | State::Closed) {
                lifecycle.state = State::Aborted;
            }
            lifecycle.worker.take()
        });
        if let Some(worker) = worker {
            let _ = worker.join();
        }
    }

    pub fn close(&self) {
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
    let block_bytes = match usize::try_from(period_frames * bytes_per_frame) {
        Ok(bytes) => bytes,
        Err(_) => {
            let error = AudioError::new("invalid_argument", "audio period buffer is too large");
            let _ = ready.send(Err(error.clone()));
            return Err(error);
        }
    };
    let started_monotonic_ns = match monotonic_ns() {
        Ok(ns) => ns,
        Err(error) => {
            let _ = ready.send(Err(error.clone()));
            return Err(error);
        }
    };
    let (blocks_tx, blocks_rx) = mpsc::sync_channel::<(Vec<u8>, u64)>(QUEUE_BLOCKS);
    let (pool_tx, pool_rx) = mpsc::sync_channel(QUEUE_BLOCKS + 1);
    for _ in 0..=QUEUE_BLOCKS {
        if pool_tx.send(vec![0_u8; block_bytes]).is_err() {
            let error = AudioError::new("audio_failed", "audio buffer pool initialization failed");
            let _ = ready.send(Err(error.clone()));
            return Err(error);
        }
    }
    let writer_progress = Arc::clone(&progress);
    let disk = match thread::Builder::new()
        .name("audio-writer".to_owned())
        .spawn(move || {
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
        }) {
        Ok(handle) => handle,
        Err(error) => {
            let error =
                AudioError::new("audio_failed", format!("spawn audio disk writer: {error}"));
            let _ = ready.send(Err(error.clone()));
            return Err(error);
        }
    };
    if let Ok(mut guard) = progress.clock_checkpoint.lock() {
        *guard = Some(ClockCheckpoint {
            started: started_monotonic_ns,
            period_frames: pcm.period_frames,
            buffer_frames: pcm.buffer_frames,
            base_index: 0,
            anchors: Vec::new(),
            last_anchor: None,
        });
    }
    let _ = ready.send(Ok(()));
    let mut sample_count = 0_u64;
    let mut anchors: Vec<clock::Anchor> = Vec::new();
    let mut last_anchor = None;
    // Diagnostic observations never replace or smooth the authoritative anchors.
    let mut clock_observations: Vec<[u64; 5]> = Vec::new();
    let mut observations_omitted = 0_u64;
    let mut bridge_rejections = 0_u64;
    let mut rejected_bridges: Vec<clock::Anchor> = Vec::new();
    let mut next_observation_ns = 0;
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
    let mut last_read_started = std::time::Instant::now();
    let mut max_read_gap_ns = 0_u64;
    let captured = (|| {
        let mut buffer = pool_rx
            .try_recv()
            .map_err(|_| AudioError::new("audio_failed", "audio writer buffer pool exhausted"))?;
        let mut buffered_frames = 0;
        while !stop.load(Ordering::Acquire) {
            progress.check()?;
            let offset = (buffered_frames * bytes_per_frame) as usize;
            let now = std::time::Instant::now();
            let read_gap_ns = now.duration_since(last_read_started).as_nanos() as u64;
            max_read_gap_ns = max_read_gap_ns.max(read_gap_ns);
            last_read_started = now;
            let read = pcm
                .readi(&mut buffer[offset..], period_frames - buffered_frames)
                .map_err(|error| {
                    AudioError::new(
                        error.code,
                        format!(
                            "{}; read_gap_ns={read_gap_ns}; buffer_frames={}",
                            error.message, pcm.buffer_frames
                        ),
                    )
                })?;
            match read {
                ReadOutcome::Frames(frames) => {
                    sample_count += frames;
                    buffered_frames += frames;
                    let (observed_anchor, _) = clock::observe_anchor(
                        || {
                            let (position, timestamp) = pcm.clock_anchor(sample_count)?;
                            let before_raw = clock_ns(libc::CLOCK_MONOTONIC_RAW)?;
                            let observed_mono = monotonic_ns()?;
                            let after_raw = clock_ns(libc::CLOCK_MONOTONIC_RAW)?;
                            Ok([position, timestamp, observed_mono, before_raw, after_raw])
                        },
                        |row| {
                            bridge_rejections += 1;
                            if rejected_bridges.len() < 64 {
                                rejected_bridges.push(row);
                            }
                        },
                    )?;
                    let anchor = clock::retain_same_point(last_anchor, observed_anchor);
                    let mapped_mono = clock::mapped_ns(&anchor)
                        .map_err(|message| AudioError::new("audio_failed", message))?;
                    let [position, timestamp, observed_mono, before_raw, after_raw] = anchor;
                    let timestamp_due = timestamp >= next_observation_ns;
                    if timestamp_due {
                        next_observation_ns = timestamp.saturating_add(100_000_000);
                    }
                    {
                        let mut guard = progress.clock_checkpoint.lock().map_err(|_| {
                            AudioError::new(
                                "native_audio_poisoned",
                                "audio clock checkpoint mutex is poisoned",
                            )
                        })?;
                        if let Some(clock) = guard.as_mut() {
                            clock.last_anchor = Some(anchor);
                            if timestamp_due {
                                clock.anchors.push(anchor);
                            }
                        }
                    }
                    if timestamp_due {
                        if clock_observations.len() < MAX_CLOCK_OBSERVATIONS {
                            clock_observations.push([
                                position,
                                mapped_mono,
                                observed_mono,
                                before_raw + (after_raw - before_raw) / 2,
                                sample_count,
                            ]);
                        } else {
                            observations_omitted += 1;
                        }
                    }
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
    let stopped_monotonic_ns = match monotonic_ns() {
        Ok(ns) => ns,
        Err(error) => {
            let _ = pcm.close();
            drop(blocks_tx);
            let _ = disk.join();
            return Err(error);
        }
    };
    if let Some(anchor) = last_anchor {
        let mut guard = progress
            .clock_checkpoint
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(clock) = guard.as_mut() {
            let last_global = clock.anchors.last().copied().or(clock.last_anchor);
            if last_global != Some(anchor) || clock.base_index == 0 && clock.anchors.is_empty() {
                clock.anchors.push(anchor);
            }
        }
    }
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
        "schema": "miro-ego.audio-capture-diagnostic.v1", "sample_count": sample_count,
        "anchors": anchors, "anchor_fields": ["sample_position", "alsa_raw_ns", "observed_monotonic_ns", "raw_before_ns", "raw_after_ns"], "thread_started_monotonic_ns": started_monotonic_ns,
        "thread_stopped_monotonic_ns": stopped_monotonic_ns,
        "error": failure.map(|error| serde_json::json!({"code": error.code, "message": error.message})),
        "clock_observation_fields": ["sample_position", "mapped_monotonic_ns", "observed_monotonic_ns", "observed_raw_ns", "read_samples"],
        "clock_observations": clock_observations,
        "clock_observations_omitted": observations_omitted,
        "clock_bridge_rejections": bridge_rejections,
        "rejected_clock_bridges": rejected_bridges,
        "rejected_clock_bridges_omitted": bridge_rejections.saturating_sub(rejected_bridges.len() as u64),
        "io_histogram_upper_bounds_ns": crate::audio::diagnostics::IO_BUCKET_NS,
        "writer_io": progress.writer_diagnostics.lock().ok().map(|value| value.json()),
        "queue_capacity_frames": QUEUE_BLOCKS as u64 * DEFAULT_PERIOD_FRAMES,
        "queue_peak_frames": progress.queue_peak_frames.load(Ordering::Relaxed),
        "durable_commit_capacity_segments": MAX_PENDING_COMMITS,
        "durable_commit_peak_segments": progress.peak_pending_commits.load(Ordering::Relaxed),
        "durable_sample_count": progress.durable_samples.load(Ordering::Acquire),
        "max_write_ns": progress.max_write_ns.load(Ordering::Relaxed),
        "max_read_gap_ns": max_read_gap_ns,
        "period_frames": pcm.period_frames,
        "buffer_frames": pcm.buffer_frames,
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
    anchors: &[clock::Anchor],
    progress: &AudioProgress,
) -> Result<serde_json::Value, AudioError> {
    let model = clock::evaluate(
        anchors,
        config.sample_rate_hz,
        samples,
        hardware_frames.0,
        hardware_frames.1,
    )
    .map_err(|message| AudioError::new("audio_failed", message))?;
    let sample_start = model.start_ns;
    let sample_end = model.end_ns;
    let residual = model.residual_ns;
    if sample_start < started as f64 - 20_000_000.0 || sample_end > stopped as f64 + 20_000_000.0 {
        return Err(AudioError::new(
            "audio_failed",
            "sample clock outside capture lifetime",
        ));
    }
    Ok(serde_json::json!({
        "schema": "miro-ego.audio-clock.v1", "clock": "host_monotonic",
        "timestamp_source": "alsa_htimestamp_dma_raw", "continuity": "verified",
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
    clock_ns(libc::CLOCK_MONOTONIC)
}

fn clock_ns(clock: libc::clockid_t) -> Result<u64, AudioError> {
    sys_clock::clock_ns(clock)
        .map_err(|error| AudioError::io("clock_failed", "clock_gettime", error))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn anchor(sample: u64, time: u64) -> clock::Anchor {
        [sample, time, time, time, time]
    }

    fn tempfile_dir() -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "miro-ego-audio-rec-test-{}-{}",
            std::process::id(),
            unique
        ));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn rejects_excessive_channel_count() {
        assert_eq!(
            Recorder::new("/tmp", "hw:0,0", 48000, MAX_CHANNELS + 1, 5.0)
                .err()
                .unwrap()
                .code,
            "invalid_argument"
        );
    }

    #[test]
    fn stop_signal_never_waits_for_lifecycle_lock_or_durable_commit() {
        let root = tempfile_dir();
        let recorder =
            Arc::new(Recorder::new(root.to_str().unwrap(), "test", 48000, 2, 5.0).unwrap());
        let flag = Arc::clone(&recorder.stop);
        let (observed_tx, observed_rx) = mpsc::channel();
        let (commit_tx, commit_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !flag.load(Ordering::Acquire) {
                assert!(std::time::Instant::now() < deadline);
                std::thread::yield_now();
            }
            observed_tx.send(()).unwrap();
            commit_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            done_tx
                .send(Err(AudioError::new("audio_failed", "commit failed")))
                .unwrap();
        });
        {
            let mut lifecycle = recorder.lifecycle.lock().unwrap();
            lifecycle.state = State::Running;
            lifecycle.done = Some(done_rx);
            lifecycle.worker = Some(worker);
            recorder.request_stop();
            recorder.request_stop();
            observed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            assert!(lifecycle.worker.is_some());
            assert_eq!(lifecycle.state, State::Running);
        }
        let pending = Arc::clone(&recorder);
        let (result_tx, result_rx) = mpsc::channel();
        let stopper = std::thread::spawn(move || {
            result_tx
                .send(pending.stop(Duration::from_secs(5)))
                .unwrap();
        });
        assert!(result_rx.recv_timeout(Duration::from_millis(50)).is_err());
        commit_tx.send(()).unwrap();
        let error = result_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, "audio_failed");
        assert_eq!(error.message, "commit failed");
        stopper.join().unwrap();
        assert_eq!(recorder.lifecycle.lock().unwrap().state, State::Failed);
        drop(recorder);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn concurrent_abort_during_stop_preserves_aborted_state() {
        let root = tempfile_dir();
        let recorder =
            Arc::new(Recorder::new(root.to_str().unwrap(), "test", 48000, 2, 5.0).unwrap());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            entered_tx.send(()).unwrap();
            let _ = release_rx.recv_timeout(Duration::from_secs(2));
            let _ = done_tx.send(Err(AudioError::new("audio_failed", "aborted")));
        });
        {
            let mut lifecycle = recorder.lifecycle.lock().unwrap();
            lifecycle.state = State::Running;
            lifecycle.done = Some(done_rx);
            lifecycle.worker = Some(worker);
        }
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let stopper_rec = Arc::clone(&recorder);
        let stopper = std::thread::spawn(move || stopper_rec.stop(Duration::from_millis(50)));
        std::thread::sleep(Duration::from_millis(10));
        recorder.abort();
        assert_eq!(recorder.lifecycle.lock().unwrap().state, State::Aborted);
        release_tx.send(()).unwrap();
        let _ = stopper.join().unwrap();
        assert_eq!(recorder.lifecycle.lock().unwrap().state, State::Aborted);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn first_audio_failure_is_visible_before_stop_and_survives_cleanup_errors() {
        let progress = AudioProgress::default();
        assert!(progress.check().is_ok());
        progress.fail(AudioError::new("audio_failed", "xrun"));
        progress.fail(AudioError::new("write_failed", "cleanup"));
        assert_eq!(progress.check().unwrap_err().message, "xrun");
    }

    #[test]
    fn native_clock_emits_verified_miro_ego_schema_and_bounds() {
        let config = RecorderConfig {
            session_root: PathBuf::new(),
            device: "hw:CARD=D2UQ2,DEV=0".into(),
            sample_rate_hz: 10_000,
            channels: 2,
            segment_seconds: 5.0,
        };
        let progress = AudioProgress::default();
        progress.queue_peak_frames.store(1024, Ordering::Relaxed);
        let clock = capture_clock(
            &config,
            (1024, 8192),
            20000,
            1_100_000_000,
            3_110_000_000,
            &[
                anchor(1024, 1_202_400_000),
                anchor(11024, 2_202_400_000),
                anchor(20000, 3_100_000_000),
            ],
            &progress,
        )
        .unwrap();
        assert_eq!(clock["schema"], "miro-ego.audio-clock.v1");
        assert_eq!(clock["clock"], "host_monotonic");
        assert_eq!(clock["continuity"], "verified");
        assert_eq!(clock["xrun_count"], 0);
        assert_eq!(clock["suspend_count"], 0);
        assert_eq!(clock["anchors"].as_array().unwrap().len(), 3);
        let start_ns = clock["sample_start_monotonic_ns"].as_u64().unwrap();
        let end_ns = clock["sample_end_monotonic_ns"].as_u64().unwrap();
        assert!(start_ns >= 1_080_000_000 && end_ns <= 3_130_000_000);
        assert!(end_ns > start_ns);
    }

    #[test]
    fn sample_clock_rejects_steps_without_rewriting_thread_times() {
        let config = RecorderConfig {
            session_root: PathBuf::new(),
            device: "hw:test".into(),
            sample_rate_hz: 10_000,
            channels: 2,
            segment_seconds: 30.0,
        };
        let progress = AudioProgress::default();
        let anchors = [
            anchor(1024, 1_202_400_000),
            anchor(11024, 2_202_400_000),
            anchor(20000, 3_100_000_000),
        ];
        let clock = capture_clock(
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
            capture_clock(
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
}
