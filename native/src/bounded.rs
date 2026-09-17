use std::collections::VecDeque;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

struct State<T> {
    queue: VecDeque<(T, usize)>,
    bytes: usize,
    closed: bool,
    peak: usize,
}

struct Shared<T> {
    capacity: usize,
    byte_capacity: usize,
    weight: fn(&T) -> usize,
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
        let bytes = (self.shared.weight)(&value);
        let mut state = self.shared.state.lock().unwrap();
        if state.closed
            || state.queue.len() == self.shared.capacity
            || bytes > self.shared.byte_capacity.saturating_sub(state.bytes)
        {
            return Err(value);
        }
        state.bytes += bytes;
        state.queue.push_back((value, bytes));
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
        let (value, bytes) = state
            .queue
            .pop_front()
            .expect("readable bounded queue must contain one value");
        state.bytes -= bytes;
        Ok(value)
    }

    pub(crate) fn close_and_clear(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.closed = true;
        state.queue.clear();
        state.bytes = 0;
        self.shared.readable.notify_all();
    }

    pub(crate) fn reopen(&self) {
        let mut state = self.shared.state.lock().unwrap();
        debug_assert!(state.queue.is_empty());
        state.closed = false;
        state.peak = 0;
    }
}

#[cfg(test)]
pub(crate) fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    weighted_channel(capacity, usize::MAX, |_| 0)
}

pub(crate) fn weighted_channel<T>(
    capacity: usize,
    byte_capacity: usize,
    weight: fn(&T) -> usize,
) -> (Producer<T>, Consumer<T>) {
    assert!(capacity > 0, "bounded queue capacity must be positive");
    let shared = Arc::new(Shared {
        capacity,
        byte_capacity,
        weight,
        state: Mutex::new(State {
            queue: VecDeque::with_capacity(capacity),
            bytes: 0,
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
    fn short_consumer_stall_retains_frames_with_both_memory_bounds() {
        // At 60 fps, 148 arrivals model a 2.47 s downstream pause. The old
        // 64-frame queue rejects exactly 84; the new default retains all.
        for (capacity, expected_rejected) in [(64, 84), (256, 0)] {
            let (producer, consumer) =
                weighted_channel(capacity, 128 * 1024 * 1024, |_: &usize| 256 * 1024);
            let rejected = (0..148)
                .filter(|frame| producer.try_push(*frame).is_err())
                .count();
            assert_eq!(rejected, expected_rejected);
            for expected in 0..(148 - expected_rejected) {
                assert_eq!(consumer.receive(Duration::from_millis(1)), Ok(expected));
            }
        }
    }

    #[test]
    fn byte_budget_rejects_before_frame_limit_and_releases_on_pop_and_close() {
        let (producer, consumer) = weighted_channel(256, 10, |value: &usize| *value);
        assert_eq!(producer.try_push(6), Ok(()));
        assert_eq!(producer.try_push(5), Err(5));
        assert_eq!(consumer.receive(Duration::from_millis(1)), Ok(6));
        assert_eq!(producer.try_push(10), Ok(()));
        consumer.close_and_clear();
        consumer.reopen();
        assert_eq!(producer.try_push(10), Ok(()));
        assert_eq!(producer.try_push(11), Err(11));
    }

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
