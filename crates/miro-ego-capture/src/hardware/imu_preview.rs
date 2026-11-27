//! Idle IMU owner. Recording stops and joins it before opening its own collector,
//! so preview never consumes packets belonging to the recording session.
use crate::hardware::imu::{Collector, ImuObservation};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const ACTIVE_POLL_INTERVAL: Duration = Duration::from_millis(20);
const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(500);
const CONSUMER_ACTIVE_WINDOW: Duration = Duration::from_secs(3);

#[derive(Clone)]
pub struct Config {
    pub device: String,
    pub unit: Option<u8>,
    pub selector: u8,
    pub stale_poll_interval: Duration,
    pub timeout: Duration,
}

#[derive(Default)]
struct State {
    stop: bool,
    latest: Option<(Instant, ImuObservation)>,
    last_consumer_read: Option<Instant>,
}

fn poll_interval_for_state(state: &State, now: Instant) -> Duration {
    if state
        .last_consumer_read
        .is_some_and(|at| now.saturating_duration_since(at) <= CONSUMER_ACTIVE_WINDOW)
    {
        ACTIVE_POLL_INTERVAL
    } else {
        IDLE_POLL_INTERVAL
    }
}

#[derive(Default)]
pub struct Preview {
    state: Arc<(Mutex<State>, Condvar)>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl Preview {
    pub fn start(&self, config: Config) {
        let Ok(mut worker) = self.worker.lock() else {
            return;
        };
        if worker.is_some() {
            return;
        }
        if let Ok(mut state) = self.state.0.lock() {
            state.stop = false;
            state.latest = None;
            state.last_consumer_read = None;
        }
        let shared = Arc::clone(&self.state);
        if let Ok(handle) = thread::Builder::new()
            .name("imu-preview".to_owned())
            .spawn(move || {
                loop {
                    if stopped(&shared) {
                        break;
                    }
                    if let Ok(collector) = Collector::open(
                        &config.device,
                        config.unit,
                        config.selector,
                        Some(config.stale_poll_interval),
                    ) {
                        while !stopped(&shared) {
                            match collector.read(config.timeout.min(Duration::from_millis(200))) {
                                Ok(observation) => {
                                    if let Ok(mut state) = shared.0.lock() {
                                        let now = Instant::now();
                                        if !state.stop {
                                            state.latest = Some((now, observation));
                                        }
                                        // Poll at 50 Hz while a UI/API consumer is reading and back
                                        // off to 2 Hz when idle so USB EP0 control traffic stays minimal.
                                        let wait_for = poll_interval_for_state(&state, now);
                                        let _ = shared.1.wait_timeout_while(state, wait_for, |s| {
                                            !s.stop
                                                && !(wait_for == IDLE_POLL_INTERVAL
                                                    && poll_interval_for_state(s, Instant::now())
                                                        == ACTIVE_POLL_INTERVAL)
                                        });
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                        collector.close();
                    }
                    let Ok(mut state) = shared.0.lock() else {
                        break;
                    };
                    state.latest = None;
                    if state.stop {
                        break;
                    }
                    let _ = shared
                        .1
                        .wait_timeout_while(state, IDLE_POLL_INTERVAL, |s| !s.stop);
                }
            })
        {
            *worker = Some(handle);
        }
    }

    pub fn stop(&self) {
        // Serialize stop/start, and release the state mutex before joining.
        let Ok(mut worker) = self.worker.lock() else {
            return;
        };
        if let Ok(mut state) = self.state.0.lock() {
            state.stop = true;
            state.latest = None;
            state.last_consumer_read = None;
            self.state.1.notify_all();
        }
        if let Some(worker) = worker.take() {
            let _ = worker.join();
        }
    }

    pub fn latest(&self) -> Option<ImuObservation> {
        let mut state = self.state.0.lock().ok()?;
        if state.stop {
            return None;
        }
        let now = Instant::now();
        let was_idle = poll_interval_for_state(&state, now) == IDLE_POLL_INTERVAL;
        state.last_consumer_read = Some(now);
        if was_idle {
            self.state.1.notify_all();
        }
        let (at, observation) = state.latest.as_ref()?;
        if now.saturating_duration_since(*at) > Duration::from_secs(2) {
            None
        } else {
            Some(observation.clone())
        }
    }
}

fn stopped(shared: &(Mutex<State>, Condvar)) -> bool {
    shared.0.lock().map(|state| state.stop).unwrap_or(true)
}

impl Drop for Preview {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hardware::imu::{ImuSample, RawVector3};

    #[test]
    fn latest_is_bounded_by_freshness_and_cleared_on_stop() {
        let preview = Preview::default();
        let sample = ImuSample {
            sequence: 0,
            packet_sequence: 0,
            sample_index: 0,
            device_timestamp_raw: 1,
            device_ticks: 1,
            host_read_start_ns: 1,
            host_read_end_ns: 1,
            host_monotonic_ns: 1,
            accelerometer: RawVector3 { x: 1, y: 2, z: 3 },
            gyroscope: RawVector3 { x: 4, y: 5, z: 6 },
            sync_offset_ns: None,
            sync_residual_ns: None,
            sync_quality: "insufficient",
        };
        let observation = ImuObservation {
            samples: [sample.clone(), sample],
            dropped_samples: 0,
        };
        preview.state.0.lock().unwrap().latest = Some((Instant::now(), observation.clone()));
        assert_eq!(
            poll_interval_for_state(&preview.state.0.lock().unwrap(), Instant::now()),
            IDLE_POLL_INTERVAL
        );
        assert_eq!(preview.latest(), Some(observation.clone()));
        assert_eq!(
            poll_interval_for_state(&preview.state.0.lock().unwrap(), Instant::now()),
            ACTIVE_POLL_INTERVAL
        );
        assert_eq!(
            poll_interval_for_state(
                &preview.state.0.lock().unwrap(),
                Instant::now() + CONSUMER_ACTIVE_WINDOW + Duration::from_millis(10)
            ),
            IDLE_POLL_INTERVAL
        );
        preview.state.0.lock().unwrap().latest =
            Some((Instant::now() - Duration::from_secs(3), observation.clone()));
        assert!(preview.latest().is_none());
        preview.state.0.lock().unwrap().latest = Some((Instant::now(), observation));
        preview.stop();
        assert!(preview.latest().is_none());
    }

    #[test]
    fn absent_camera_preview_can_stop_and_restart_without_retained_data() {
        let preview = Preview::default();
        let config = Config {
            device: "/nonexistent/miro-ego-camera".into(),
            unit: Some(1),
            selector: 1,
            stale_poll_interval: Duration::from_millis(1),
            timeout: Duration::from_secs(2),
        };
        for _ in 0..3 {
            preview.start(config.clone());
            assert!(preview.latest().is_none());
            preview.stop();
            assert!(preview.worker.lock().unwrap().is_none());
            assert!(preview.latest().is_none());
        }
    }
}
