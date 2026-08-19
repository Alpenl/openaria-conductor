use std::collections::VecDeque;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

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
        let mut state = self.shared.state.lock().unwrap();
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
