//! Idle IMU owner. Recording stops and joins it before opening its own collector,
//! so preview never consumes packets belonging to the recording session.
use crate::imu::{Collector, ImuObservation};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub(crate) struct Config {
    pub(crate) device: String,
    pub(crate) unit: Option<u8>,
    pub(crate) selector: u8,
    pub(crate) stale_poll_interval: Duration,
    pub(crate) timeout: Duration,
}

#[derive(Default)]
struct State {
    stop: bool,
    latest: Option<(Instant, ImuObservation)>,
}

#[derive(Default)]
pub(crate) struct Preview {
    state: Arc<(Mutex<State>, Condvar)>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl Preview {
    pub(crate) fn start(&self, config: Config) {
        let Ok(mut worker) = self.worker.lock() else {
            return;
        };
        if worker.is_some() {
            return;
        }
        if let Ok(mut state) = self.state.0.lock() {
            state.stop = false;
            state.latest = None;
        }
        let shared = Arc::clone(&self.state);
        *worker = Some(thread::spawn(move || {
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
                                    if !state.stop {
                                        state.latest = Some((Instant::now(), observation));
                                    }
                                    // UI preview needs at most 50 Hz. Release the USB
                                    // control channel between polls; stop remains interruptible.
                                    let _ = shared.1.wait_timeout_while(
                                        state,
                                        Duration::from_millis(20),
                                        |s| !s.stop,
                                    );
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
                    .wait_timeout_while(state, Duration::from_millis(500), |s| !s.stop);
            }
        }));
    }

    pub(crate) fn stop(&self) {
        // Serialize stop/start, and release the state mutex before joining.
        let Ok(mut worker) = self.worker.lock() else {
            return;
        };
        if let Ok(mut state) = self.state.0.lock() {
            state.stop = true;
            state.latest = None;
            self.state.1.notify_all();
        }
        if let Some(worker) = worker.take() {
            let _ = worker.join();
        }
    }

    pub(crate) fn latest(&self) -> Option<ImuObservation> {
        let state = self.state.0.lock().ok()?;
        let (at, observation) = state.latest.as_ref()?;
        if state.stop || at.elapsed() > Duration::from_secs(2) {
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
    use crate::imu::{ImuSample, RawVector3};

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
        assert_eq!(preview.latest(), Some(observation.clone()));
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
            device: "/nonexistent/openaria-camera".into(),
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
