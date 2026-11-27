//! Background supervisor tasks (1 Hz storage reserve & idle camera reconnect, 250ms active
//! failure & capture timer polling, network SSE status bridge, LED/voice cue tracker) and
//! graceful daemon shutdown with clean `mount.clean` catalog marker persistence.

use super::BoundDaemon;
use super::gateway_backend::{controller_status_to_openapi, degraded_network_status_with_scheme};
use miro_ego_api::{EventReplayBuffer, GatewayBackend};
use miro_ego_hw::{IndicatorController, IndicatorState, VoiceCue, VoiceFeedback};
use miro_ego_storage::{
    CaptureEvent, CaptureEventType, CapturePhase, CaptureStopRequest, ReserveEnforcementOutcome,
};
use serde_json::{Value, json};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_NETWORK_IDLE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_NETWORK_ACTIVE_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Serve a [`BoundDaemon`] together with its background supervisor tasks until `shutdown` fires,
/// then gracefully drain HTTP connections, seal any active capture, flush audit logs, mark the
/// session catalog clean (`mount.clean`), and restore LED/voice hardware.
pub async fn run_bound_daemon_with_shutdown<F>(
    daemon: BoundDaemon,
    shutdown: F,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send + 'static,
{
    let BoundDaemon {
        server,
        backend,
        reserve_poll_interval,
        timer_poll_interval,
    } = daemon;

    if let Some(led) = backend.indicator() {
        let _ = led.set_state(IndicatorState::Ready);
    }
    if let Some(voice) = backend.voice_controller() {
        let _ = voice.play_cue(VoiceCue::DeviceReady);
    }

    let bg_stop = Arc::new(AtomicBool::new(false));
    let (shutdown_tx, mut reserve_rx) = tokio::sync::watch::channel(false);
    let mut timer_rx = shutdown_tx.subscribe();
    let mut net_poll_rx = shutdown_tx.subscribe();

    // Task 1: Capture event bridge -> SSE replay buffer + LED + voice feedback.
    let event_sub = backend.coordinator().subscribe_events();
    let event_buf = backend.capture_event_buffer();
    let event_stop = Arc::clone(&bg_stop);
    let event_led = backend.indicator().cloned();
    let event_voice = backend.voice_controller().cloned();
    let event_thread = thread::Builder::new()
        .name("miro-ego-event-bridge".to_owned())
        .spawn(move || {
            let mut tracker = EventFeedbackTracker::default();
            while !event_stop.load(Ordering::Acquire) {
                match event_sub.recv_timeout(Duration::from_millis(200)) {
                    Ok(ev) => {
                        let led_ref = event_led.as_deref().map(|l| l as &dyn IndicatorController);
                        let voice_ref = event_voice.as_deref().map(|v| v as &dyn VoiceFeedback);
                        forward_capture_event(&mut tracker, &ev, &event_buf, led_ref, voice_ref);
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
        })
        .ok();

    // Task 2: 1 Hz storage reserve enforcement, audit RAM fallback flush, & idle camera reconnect.
    let reserve_coord = Arc::clone(backend.coordinator());
    let reserve_audit = Arc::clone(backend.audit());
    let reserve_backend = Arc::clone(&backend);
    let reserve_native_cam = backend.native_capture().cloned();
    let reserve_voice = backend.voice_controller().cloned();
    let reserve_task = tokio::spawn(async move {
        let mut ticker =
            tokio::time::interval(reserve_poll_interval.max(Duration::from_millis(50)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let c = Arc::clone(&reserve_coord);
                    let a = Arc::clone(&reserve_audit);
                    let b = Arc::clone(&reserve_backend);
                    let cam = reserve_native_cam.clone();
                    let outcome = tokio::task::spawn_blocking(move || {
                        b.flush_coalesced_audit();
                        let _ = a.flush_fallback_to_disk();
                        if let Some(ref native) = cam {
                            if matches!(c.phase(), Ok(CapturePhase::Idle))
                                && !native.is_preview_worker_running()
                            {
                                let _ = native.try_connect_camera();
                            }
                        }
                        c.enforce_storage_reserve()
                    })
                    .await;
                    if let Ok(Ok(ReserveEnforcementOutcome::StoppedForReserve { .. })) = outcome {
                        if let Some(ref voice) = reserve_voice {
                            let v = Arc::clone(voice);
                            let _ = tokio::task::spawn_blocking(move || {
                                let _ = v.play_cue(VoiceCue::StorageLow);
                            })
                            .await;
                        }
                    }
                }
                _ = reserve_rx.changed() => {
                    if *reserve_rx.borrow() {
                        break;
                    }
                }
            }
        }
    });

    // Task 3: Active failure supervisor & capture timer expiration monitor.
    let timer_coord = Arc::clone(backend.coordinator());
    let timer_task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(timer_poll_interval.max(Duration::from_millis(50)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let c = Arc::clone(&timer_coord);
                    let _ = tokio::task::spawn_blocking(move || {
                        let _ = c.poll_active_failure();
                        let _ = c.poll_capture_timer();
                    })
                    .await;
                }
                _ = timer_rx.changed() => {
                    if *timer_rx.borrow() {
                        break;
                    }
                }
            }
        }
    });

    // Task 4: Background network status poller feeding live `/api/v1/network/events` SSE subscribers.
    let net_poll_task = backend.network_probe_client().map(|probe_client| {
        let net_buf = backend.network_event_buffer();
        let dev_id = backend.config.device_config.device.device_id.clone();
        let listen_port = backend.config.device_config.listen.port;
        let mdns_scheme = backend.config.mdns_scheme();
        tokio::spawn(async move {
            let mut next_delay = DEFAULT_NETWORK_IDLE_POLL_INTERVAL;
            let mut had_live_status = false;
            loop {
                tokio::select! {
                    () = tokio::time::sleep(next_delay) => {
                        let c = probe_client.clone();
                        let did = dev_id.clone();
                        let polled = tokio::task::spawn_blocking(move || c.status()).await;
                        match polled {
                            Ok(Ok(st)) => {
                                had_live_status = true;
                                let tx_active = st.transaction.current.is_some();
                                let val = controller_status_to_openapi(
                                    &st,
                                    &did,
                                    listen_port,
                                    mdns_scheme,
                                );
                                let _ = net_buf.observe(val);
                                next_delay = if tx_active {
                                    DEFAULT_NETWORK_ACTIVE_POLL_INTERVAL
                                } else {
                                    DEFAULT_NETWORK_IDLE_POLL_INTERVAL
                                };
                            }
                            _ => {
                                if had_live_status {
                                    had_live_status = false;
                                    let val = degraded_network_status_with_scheme(
                                        &dev_id,
                                        listen_port,
                                        mdns_scheme,
                                        "controller_unavailable",
                                    );
                                    let _ = net_buf.observe(val);
                                }
                                next_delay = DEFAULT_NETWORK_IDLE_POLL_INTERVAL;
                            }
                        }
                    }
                    _ = net_poll_rx.changed() => {
                        if *net_poll_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        })
    });

    // Run HTTP/HTTPS gateway until shutdown signal resolves.
    let serve_result = server.serve_with_graceful_shutdown(shutdown).await;

    // Graceful shutdown sequence:
    // 1. Stop periodic polling tasks first so they do not race with final capture stop/seal.
    let _ = shutdown_tx.send(true);
    let _ = reserve_task.await;
    let _ = timer_task.await;
    if let Some(task) = net_poll_task {
        let _ = task.await;
    }

    // 2. Cleanly settle `Starting`/`Recording`/`Stopping`, seal any active capture, close coordinator,
    //    flush coalesced + RAM-fallback audit events, and write `mount.clean` if volume is writable.
    let shutdown_coord = Arc::clone(backend.coordinator());
    let shutdown_audit = Arc::clone(backend.audit());
    let shutdown_catalog = Arc::clone(backend.catalog());
    let shutdown_volume = Arc::clone(backend.volume());
    let shutdown_backend = Arc::clone(&backend);
    let _ = tokio::task::spawn_blocking(move || {
        let start_wait_deadline = Instant::now() + Duration::from_secs(5);
        while matches!(shutdown_coord.phase(), Ok(CapturePhase::Starting))
            && Instant::now() < start_wait_deadline
        {
            thread::sleep(Duration::from_millis(20));
        }
        let _ = shutdown_coord.poll_active_failure();
        if matches!(shutdown_coord.phase(), Ok(CapturePhase::Recording)) {
            let _ = shutdown_coord.stop_capture(CaptureStopRequest::user(), None);
        }
        let stop_wait_deadline = Instant::now() + Duration::from_secs(30);
        while matches!(shutdown_coord.phase(), Ok(CapturePhase::Stopping))
            && Instant::now() < stop_wait_deadline
        {
            thread::sleep(Duration::from_millis(20));
        }
        let _ = shutdown_coord.close();
        shutdown_backend.flush_coalesced_audit();
        let _ = shutdown_audit.flush_fallback_to_disk();
        shutdown_audit.close_active_file();
        if !shutdown_volume.is_volume_released() && shutdown_volume.is_mounted_and_writable() {
            shutdown_catalog.mark_mount_clean();
        }
    })
    .await;

    // 3. Stop event bridge after terminal coordinator state/safe-swap events have been drained.
    bg_stop.store(true, Ordering::Release);
    if let Some(handle) = event_thread {
        let _ = handle.join();
    }

    // 4. Restore LED state & close background voice queue on exit.
    if let Some(led) = backend.indicator() {
        let _ = led.restore();
    }
    if let Some(voice) = backend.voice_controller() {
        voice.close();
    }

    serve_result.map_err(|err| format!("gateway server exited with error: {err}"))
}

/// Stateful LED & voice cue transition tracker for `miro-ego-event-bridge`.
#[derive(Debug, Default)]
pub struct EventFeedbackTracker {
    capture_active: bool,
    blocked_latched: bool,
    error_latched_session: Option<String>,
}

impl EventFeedbackTracker {
    pub fn observe(
        &mut self,
        ev: &CaptureEvent,
        led: Option<&dyn IndicatorController>,
        voice: Option<&dyn VoiceFeedback>,
    ) {
        match ev.event_type {
            CaptureEventType::State => {
                let Some(state_str) = ev.data.get("state").and_then(Value::as_str) else {
                    return;
                };
                match state_str {
                    "recording" => {
                        self.blocked_latched = false;
                        self.error_latched_session = None;
                        if !self.capture_active {
                            self.capture_active = true;
                            if let Some(l) = led {
                                let _ = l.set_state(IndicatorState::Recording);
                            }
                            if let Some(v) = voice {
                                let _ = v.play_cue(VoiceCue::RecordingStarted);
                            }
                        }
                    }
                    "finalizing" | "encoding" | "verifying" => {
                        self.capture_active = true;
                    }
                    "failed" | "recoverable" | "abandoned" => {
                        self.capture_active = false;
                        let already_latched = ev.session_id.is_some()
                            && self.error_latched_session.as_deref() == ev.session_id.as_deref();
                        if !already_latched {
                            self.error_latched_session = ev.session_id.clone();
                            if let Some(l) = led {
                                let _ = l.set_state(IndicatorState::Error);
                            }
                            if let Some(v) = voice {
                                let _ = v.play_cue(VoiceCue::ErrorAlert);
                            }
                        }
                    }
                    "idle" => {
                        self.blocked_latched = false;
                        if self.capture_active {
                            self.capture_active = false;
                            if let Some(l) = led {
                                let _ = l.set_state(IndicatorState::Ready);
                            }
                            if let Some(v) = voice {
                                let _ = v.play_cue(VoiceCue::RecordingStopped);
                            }
                        }
                    }
                    _ => {}
                }
            }
            CaptureEventType::Diagnostic => {
                let is_error = ev
                    .data
                    .get("diagnostic")
                    .and_then(|d| d.get("severity"))
                    .and_then(Value::as_str)
                    == Some("error");
                if is_error && !self.capture_active {
                    let already_latched = ev.session_id.is_some()
                        && self.error_latched_session.as_deref() == ev.session_id.as_deref();
                    if !already_latched {
                        self.error_latched_session = ev.session_id.clone();
                        if let Some(l) = led {
                            let _ = l.set_state(IndicatorState::Error);
                        }
                        if let Some(v) = voice {
                            let _ = v.play_cue(VoiceCue::ErrorAlert);
                        }
                    }
                }
            }
            CaptureEventType::Snapshot => {
                let Some(device_state) = ev.data.get("device_state").and_then(Value::as_str) else {
                    return;
                };
                match device_state {
                    "recording" => {
                        self.blocked_latched = false;
                        self.error_latched_session = None;
                        if !self.capture_active {
                            self.capture_active = true;
                            if let Some(l) = led {
                                let _ = l.set_state(IndicatorState::Recording);
                            }
                            if let Some(v) = voice {
                                let _ = v.play_cue(VoiceCue::RecordingStarted);
                            }
                        }
                    }
                    "finalizing" | "encoding" | "verifying" => {
                        self.blocked_latched = false;
                        self.capture_active = true;
                    }
                    "idle" => {
                        self.blocked_latched = false;
                        if self.capture_active {
                            self.capture_active = false;
                            if let Some(l) = led {
                                let _ = l.set_state(IndicatorState::Ready);
                            }
                            if let Some(v) = voice {
                                let _ = v.play_cue(VoiceCue::RecordingStopped);
                            }
                        }
                    }
                    "blocked" => {
                        self.capture_active = false;
                        if let Some(l) = led {
                            let _ = l.set_state(IndicatorState::Error);
                        }
                        if !self.blocked_latched && self.error_latched_session.is_none() {
                            self.blocked_latched = true;
                            if let Some(v) = voice {
                                let _ = v.play_cue(VoiceCue::ErrorAlert);
                            }
                        } else {
                            self.blocked_latched = true;
                        }
                    }
                    _ => {}
                }
            }
            CaptureEventType::Progress | CaptureEventType::SafeSwap => {}
        }
    }
}

fn forward_capture_event(
    tracker: &mut EventFeedbackTracker,
    ev: &CaptureEvent,
    event_buf: &EventReplayBuffer,
    led: Option<&dyn IndicatorController>,
    voice: Option<&dyn VoiceFeedback>,
) {
    let ev_type = match ev.event_type {
        CaptureEventType::Snapshot => "snapshot",
        CaptureEventType::State => "state",
        CaptureEventType::Progress => "progress",
        CaptureEventType::Diagnostic => "diagnostic",
        CaptureEventType::SafeSwap => "safe_swap",
    };
    let payload = json!({
        "authority_epoch": ev.authority_epoch,
        "source_revision": ev.source_revision,
        "type": ev_type,
        "occurred_at": ev.occurred_at,
        "session_id": ev.session_id,
        "data": ev.data,
    });
    let _ = event_buf.publish(payload);
    tracker.observe(ev, led, voice);
}

/// Wait for `SIGINT` or `SIGTERM` on Unix systems.
pub async fn wait_for_termination_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    let sigterm = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            let _ = sig.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    tokio::select! {
        _ = ctrl_c => {}
        _ = sigterm => {}
    }
}
