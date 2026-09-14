//! Compile the actual baseline/candidate queue module into the same experiment.
#![allow(dead_code)]

mod bounded {
    include!(concat!(env!("ABLATION_ROOT"), "/native/src/bounded.rs"));
}

use std::collections::VecDeque;
use std::hint::black_box;
use std::sync::mpsc::RecvTimeoutError;
use std::time::{Duration, Instant};

fn trace(capacity: usize) -> u64 {
    let (producer, consumer) = bounded::channel(capacity);
    let mut expected = VecDeque::new();
    let mut closed = false;
    let mut random = 20260912_u64;
    let mut digest = 0_u64;
    for value in 0..2000_u64 {
        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
        let event = match random >> 60 {
            0 => {
                consumer.close_and_clear();
                expected.clear();
                closed = true;
                1
            }
            1 if closed => {
                consumer.reopen();
                closed = false;
                2
            }
            2..=7 => {
                let result = consumer.receive(Duration::ZERO);
                let model = expected.pop_front().ok_or(if closed {
                    RecvTimeoutError::Disconnected
                } else {
                    RecvTimeoutError::Timeout
                });
                assert_eq!(result, model);
                match result {
                    Ok(value) => value + 3,
                    Err(RecvTimeoutError::Disconnected) => 4,
                    Err(RecvTimeoutError::Timeout) => 5,
                }
            }
            _ => {
                let accepted = !closed && expected.len() < capacity;
                assert_eq!(
                    producer.try_push(value),
                    if accepted { Ok(()) } else { Err(value) }
                );
                if accepted {
                    expected.push_back(value);
                }
                if accepted { 6 } else { 7 }
            }
        };
        digest = digest.wrapping_mul(31).wrapping_add(event);
    }
    digest
}

fn concurrent(capacity: usize) -> u64 {
    let (producer, consumer) = bounded::channel(capacity);
    let handles: Vec<_> = (0..2)
        .map(|source| {
            let producer = producer.clone();
            std::thread::spawn(move || {
                for sequence in 0..20_000_u64 {
                    let mut frame = (source, sequence);
                    while let Err(rejected) = producer.try_push(frame) {
                        frame = rejected;
                        std::thread::yield_now();
                    }
                }
            })
        })
        .collect();
    let mut expected = [0_u64; 2];
    for _ in 0..40_000 {
        let (source, sequence) = consumer.receive(Duration::from_secs(3)).unwrap();
        assert_eq!(sequence, expected[source]);
        expected[source] += 1;
    }
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(expected, [20_000, 20_000]);
    consumer.close_and_clear();
    assert_eq!(
        consumer.receive(Duration::ZERO),
        Err(RecvTimeoutError::Disconnected)
    );
    expected.iter().sum()
}

fn main() {
    let traces: Vec<_> = [1, 4, 32].into_iter().map(trace).collect();
    let concurrent_frames: u64 = [4, 4096].into_iter().map(concurrent).sum();
    let (producer, consumer) = bounded::channel(4);
    let mut samples = Vec::new();
    for _ in 0..7 {
        let started = Instant::now();
        for value in 0..200_000_u64 {
            producer.try_push(black_box(value)).unwrap();
            assert_eq!(consumer.receive(Duration::ZERO).unwrap(), value);
        }
        samples.push(started.elapsed().as_nanos());
    }
    println!(
        "{{\"trace_hashes\":{traces:?},\"concurrent_frames\":{concurrent_frames},\"round_trips_per_sample\":200000,\"samples_ns\":{samples:?}}}"
    );
}
