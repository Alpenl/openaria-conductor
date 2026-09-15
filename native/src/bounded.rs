use std::collections::VecDeque;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

struct State<T> {
    queue: VecDeque<T>,
    closed: bool,
    peak: usize,
}

struct Shared<T> {
    capacity: usize,
    state: Mutex<State<T>>,
    readable: Condvar,
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
        if state.closed || state.queue.len() == self.shared.capacity {
            return Err(value);
        }
        state.queue.push_back(value);
        state.peak = state.peak.max(state.queue.len());
        self.shared.readable.notify_one();
        Ok(())
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
    pub(crate) fn queue_stats(&self) -> (usize, usize, usize) {
        let state = self.shared.state.lock().unwrap();
        (state.queue.len(), self.shared.capacity, state.peak)
    }

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
        Ok(value)
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
        state.peak = 0;
    }
}

pub(crate) fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    assert!(capacity > 0, "bounded queue capacity must be positive");
    let shared = Arc::new(Shared {
        capacity,
        state: Mutex::new(State {
            queue: VecDeque::with_capacity(capacity),
            closed: false,
            peak: 0,
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
    fn rejects_full_queue_and_delivers_in_order() {
        let (producer, consumer) = channel(2);
        assert_eq!(producer.try_push(10), Ok(()));
        assert_eq!(producer.try_push(11), Ok(()));
        assert_eq!(producer.try_push(12), Err(12));
        assert_eq!(consumer.receive(Duration::from_millis(1)), Ok(10));
        assert_eq!(consumer.receive(Duration::from_millis(1)), Ok(11));
        assert_eq!(
            consumer.receive(Duration::from_millis(1)),
            Err(RecvTimeoutError::Timeout)
        );
    }

    #[test]
    fn push_wakes_waiting_receiver() {
        let (producer, consumer) = channel(1);
        let handle = std::thread::spawn(move || consumer.receive(Duration::from_secs(2)));
        assert_eq!(producer.try_push(10), Ok(()));
        assert_eq!(handle.join().unwrap(), Ok(10));
    }

    #[test]
    fn close_wakes_waiting_receivers() {
        let (_producer, consumer) = channel::<u8>(1);
        let receiver = consumer.clone();
        let handle = std::thread::spawn(move || receiver.receive(Duration::from_secs(2)));
        consumer.close_and_clear();
        assert_eq!(handle.join().unwrap(), Err(RecvTimeoutError::Disconnected));
    }

    #[test]
    fn close_releases_owned_values_and_allows_reopening() {
        let (producer, consumer) = channel(2);
        let frame = Arc::new(String::from("owned-frame"));
        producer.try_push(Arc::clone(&frame)).unwrap();
        assert_eq!(Arc::strong_count(&frame), 2);
        consumer.close_and_clear();
        assert_eq!(Arc::strong_count(&frame), 1);
        assert_eq!(
            consumer.receive(Duration::from_secs(1)),
            Err(RecvTimeoutError::Disconnected)
        );
        assert_eq!(
            producer.try_push(Arc::clone(&frame)),
            Err(Arc::clone(&frame))
        );
        consumer.reopen();
        producer.try_push(Arc::clone(&frame)).unwrap();
        assert_eq!(consumer.receive(Duration::from_millis(1)), Ok(frame));
    }
}
