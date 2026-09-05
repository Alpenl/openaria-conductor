use std::collections::VecDeque;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueueStats {
    pub(crate) capacity: usize,
    pub(crate) depth: usize,
    pub(crate) peak_depth: usize,
    pub(crate) enqueued: u64,
    pub(crate) delivered: u64,
    pub(crate) rejected: u64,
}

struct State<T> {
    queue: VecDeque<T>,
    closed: bool,
    peak_depth: usize,
    enqueued: u64,
    delivered: u64,
    rejected: u64,
}

struct Shared<T> {
    capacity: usize,
    state: Mutex<State<T>>,
    readable: Condvar,
    writable: Condvar,
}

impl<T> Shared<T> {
    fn stats(&self) -> QueueStats {
        let state = self.state.lock().unwrap();
        QueueStats {
            capacity: self.capacity,
            depth: state.queue.len(),
            peak_depth: state.peak_depth,
            enqueued: state.enqueued,
            delivered: state.delivered,
            rejected: state.rejected,
        }
    }
}

pub(crate) struct Producer<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Producer<T> {
    pub(crate) fn try_push(&self, value: T) -> Result<(), T> {
        self.push_timeout(value, Duration::ZERO)
    }

    pub(crate) fn push_timeout(&self, value: T, timeout: Duration) -> Result<(), T> {
        let mut state = self.shared.state.lock().unwrap();
        if timeout.is_zero() {
            if state.closed {
                return Err(value);
            }
            if state.queue.len() == self.shared.capacity {
                state.rejected += 1;
                return Err(value);
            }
            state.queue.push_back(value);
            state.peak_depth = state.peak_depth.max(state.queue.len());
            state.enqueued += 1;
            self.shared.readable.notify_one();
            return Ok(());
        }
        let started = Instant::now();
        while !state.closed && state.queue.len() == self.shared.capacity {
            let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                state.rejected += 1;
                return Err(value);
            };
            let (next, wait) = self.shared.writable.wait_timeout(state, remaining).unwrap();
            state = next;
            if wait.timed_out() && state.queue.len() == self.shared.capacity {
                state.rejected += 1;
                return Err(value);
            }
        }
        if state.closed {
            return Err(value);
        }
        state.queue.push_back(value);
        state.peak_depth = state.peak_depth.max(state.queue.len());
        state.enqueued += 1;
        self.shared.readable.notify_one();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn stats(&self) -> QueueStats {
        self.shared.stats()
    }
}

pub(crate) struct Consumer<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Consumer<T> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Consumer<T> {
    pub(crate) fn receive(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let state = self.shared.state.lock().unwrap();
        let (mut state, result) = self
            .shared
            .readable
            .wait_timeout_while(state, timeout, |state| {
                state.queue.is_empty() && !state.closed
            })
            .unwrap();
        if state.closed && state.queue.is_empty() {
            return Err(RecvTimeoutError::Disconnected);
        }
        if result.timed_out() && state.queue.is_empty() {
            return Err(RecvTimeoutError::Timeout);
        }
        let value = state
            .queue
            .pop_front()
            .expect("readable bounded queue must contain one value");
        state.delivered += 1;
        self.shared.writable.notify_one();
        Ok(value)
    }

    pub(crate) fn stats(&self) -> QueueStats {
        self.shared.stats()
    }

    pub(crate) fn close_and_clear(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.closed = true;
        state.queue.clear();
        self.shared.readable.notify_all();
        self.shared.writable.notify_all();
    }

    pub(crate) fn reopen(&self) {
        let mut state = self.shared.state.lock().unwrap();
        debug_assert!(state.queue.is_empty());
        state.closed = false;
    }
}

pub(crate) fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    assert!(capacity > 0, "bounded queue capacity must be positive");
    let shared = Arc::new(Shared {
        capacity,
        state: Mutex::new(State {
            queue: VecDeque::with_capacity(capacity),
            closed: false,
            peak_depth: 0,
            enqueued: 0,
            delivered: 0,
            rejected: 0,
        }),
        readable: Condvar::new(),
        writable: Condvar::new(),
    });
    (
        Producer {
            shared: Arc::clone(&shared),
        },
        Consumer { shared },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_rejection_and_delivery_are_accounted() {
        let (producer, consumer) = channel(2);
        assert_eq!(producer.try_push(10), Ok(()));
        assert_eq!(producer.try_push(11), Ok(()));
        assert_eq!(producer.try_push(12), Err(12));
        assert_eq!(
            producer.stats(),
            QueueStats {
                capacity: 2,
                depth: 2,
                peak_depth: 2,
                enqueued: 2,
                delivered: 0,
                rejected: 1,
            }
        );
        assert_eq!(consumer.receive(Duration::from_millis(1)), Ok(10));
        assert_eq!(consumer.receive(Duration::from_millis(1)), Ok(11));
        assert_eq!(
            consumer.receive(Duration::from_millis(1)),
            Err(RecvTimeoutError::Timeout)
        );
        assert_eq!(consumer.stats().depth, 0);
        assert_eq!(consumer.stats().delivered, 2);
    }

    #[test]
    fn timed_push_waits_for_consumer_capacity() {
        let (producer, consumer) = channel(1);
        assert_eq!(producer.try_push(10), Ok(()));
        let producer_thread = producer.clone();
        let handle =
            std::thread::spawn(move || producer_thread.push_timeout(11, Duration::from_secs(1)));
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(consumer.receive(Duration::from_secs(1)), Ok(10));
        assert_eq!(handle.join().unwrap(), Ok(()));
        assert_eq!(consumer.receive(Duration::from_secs(1)), Ok(11));
        let stats = consumer.stats();
        assert_eq!(stats.rejected, 0);
        assert_eq!(stats.enqueued, 2);
        assert_eq!(stats.delivered, 2);
    }

    #[test]
    fn timed_push_reports_rejection_after_timeout() {
        let (producer, _consumer) = channel(1);
        assert_eq!(producer.try_push(10), Ok(()));
        assert_eq!(producer.push_timeout(11, Duration::from_millis(1)), Err(11));
        assert_eq!(producer.stats().rejected, 1);
    }

    #[test]
    fn close_clears_values_and_wakes_receivers() {
        let (producer, consumer) = channel(2);
        producer.try_push(String::from("owned-frame")).unwrap();
        consumer.close_and_clear();
        assert_eq!(consumer.stats().depth, 0);
        assert_eq!(
            consumer.receive(Duration::from_secs(1)),
            Err(RecvTimeoutError::Disconnected)
        );
        assert_eq!(
            producer.try_push(String::from("late-frame")),
            Err(String::from("late-frame"))
        );
        consumer.reopen();
        producer.try_push(String::from("restarted-frame")).unwrap();
        assert_eq!(
            consumer.receive(Duration::from_millis(1)),
            Ok(String::from("restarted-frame"))
        );
    }
}
