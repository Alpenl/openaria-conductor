#![allow(dead_code)]
// Actual source modules are reconstructed outside the checkout by run_native.py.
use std::hash::{Hash, Hasher};
use std::hint::black_box;
use std::time::Instant;
#[path = "active_take_a1.rs"]
mod a1;
#[path = "active_take_a2.rs"]
mod a2;
#[path = "active_take_a3.rs"]
mod a3;
#[path = "active_take_all.rs"]
mod all;
#[path = "active_take_baseline.rs"]
mod baseline;
#[path = "active_take_combined.rs"]
mod combined;
#[path = "active_take_negative.rs"]
mod negative;
#[cfg(track_alloc)]
mod alloc {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicU64, Ordering};
    pub static COUNT: AtomicU64 = AtomicU64::new(0);
    pub static BYTES: AtomicU64 = AtomicU64::new(0);
    struct Counter;
    unsafe impl GlobalAlloc for Counter {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            COUNT.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(l.size() as u64, Ordering::Relaxed);
            System.alloc(l)
        }
        unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
            System.dealloc(p, l)
        }
    }
    #[global_allocator]
    static COUNTER: Counter = Counter;
    pub fn reset() {
        COUNT.store(0, Ordering::Relaxed);
        BYTES.store(0, Ordering::Relaxed);
    }
    pub fn get() -> (u64, u64) {
        (COUNT.load(Ordering::Relaxed), BYTES.load(Ordering::Relaxed))
    }
}
macro_rules! variant {
    ($module:ident,$trace:ident,$bench:ident) => {
        fn $trace(seed: u64) -> Vec<String> {
            use $module::*;
            let mut out = Vec::new();
            let mut writer = ActiveTakeWriter::new("take-with-unicode-测试").unwrap();
            let mut rng = seed;
            macro_rules! record {
                ($op:expr) => {{
                    let result = $op.map(|_| ()).map_err(|e| e.code);
                    out.push(format!("{:?}|{:?}", result, writer.snapshot()));
                }};
            }
            for index in 0..16 {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                // Capture can fail its source-gap check before a frame is reserved.
                if rng % 4 == 0 {
                    record!(writer.reserve_frame(ActiveSourceFrame {
                        source_sequence: index * 2,
                        host_monotonic_ns: 100000 + index * 3333,
                        source_gap: rng % 8 + 1
                    }));
                }
                let source = ActiveSourceFrame {
                    source_sequence: index * 2,
                    host_monotonic_ns: 100000 + index * 3333,
                    source_gap: 0,
                };
                let reserved = writer.reserve_frame(source).unwrap();
                out.push(format!("{:?}|{:?}", reserved, writer.snapshot()));
                // Stop while encoder/index write is still in flight must reject completion.
                if rng % 3 == 0 {
                    record!(writer.finish());
                }
                if rng % 5 == 0 {
                    let mut foreign = reserved.clone();
                    foreign.session_id.push_str("-foreign");
                    record!(writer.finish_frame(foreign, 100));
                }
                if rng % 7 == 0 {
                    let mut unknown = reserved.clone();
                    unknown.record_sequence += 1;
                    record!(writer.finish_frame(unknown, 100));
                }
                if rng % 11 == 0 {
                    // A failed write remains pending until teardown; finish cannot seal it.
                    record!(writer.finish());
                    return out;
                }
                record!(writer.finish_frame(reserved.clone(), rng % 4096));
                if rng % 6 == 0 {
                    record!(writer.finish_frame(reserved, 100));
                }
            }
            record!(writer.finish());
            record!(writer.finish());
            record!(writer.reserve_frame(ActiveSourceFrame {
                source_sequence: 999,
                host_monotonic_ns: 999999,
                source_gap: 0
            }));
            out
        }
        fn $bench(n: u64) -> u64 {
            use $module::*;
            let mut writer = ActiveTakeWriter::new("recording-session-12345678-abcd-ef01").unwrap();
            for index in 0..n {
                let source = black_box(ActiveSourceFrame {
                    source_sequence: index * 2,
                    host_monotonic_ns: 100000 + index * 33333333,
                    source_gap: 0,
                });
                let frame = writer.reserve_frame(source).unwrap();
                writer.finish_frame(frame, black_box(512)).unwrap();
            }
            let result = writer.finish().unwrap();
            black_box(result.frames_written + result.bytes_written + result.frame_domain)
        }
    };
}
variant!(baseline, trace_base, bench_base);
variant!(a1, trace_a1, bench_a1);
variant!(a2, trace_a2, bench_a2);
variant!(combined, trace_combined, bench_combined);
variant!(a3, trace_a3, bench_a3);
variant!(all, trace_all, bench_all);
fn main() {
    let funcs: [(&str, fn(u64) -> u64); 6] = [
        ("baseline", bench_base),
        ("a1", bench_a1),
        ("a2", bench_a2),
        ("combined", bench_combined),
        ("a3", bench_a3),
        ("all", bench_all),
    ];
    #[cfg(track_alloc)]
    {
        for (name, bench) in funcs {
            alloc::reset();
            let result = bench(100000);
            let (count, bytes) = alloc::get();
            println!("{{\"variant\":\"{name}\",\"frames\":100000,\"allocation_count\":{count},\"allocation_bytes\":{bytes},\"result\":{result}}}");
        }
        return;
    }
    #[cfg(not(track_alloc))]
    {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        let mut operations = 0;
        for seed in 0..20000 {
            let reference = trace_base(seed);
            operations += reference.len();
            reference.hash(&mut hasher);
            assert_eq!(reference, trace_a1(seed), "a1 seed {seed}");
            assert_eq!(reference, trace_a2(seed), "a2 seed {seed}");
            assert_eq!(reference, trace_combined(seed), "combined seed {seed}");
            assert_eq!(reference, trace_a3(seed), "a3 seed {seed}");
            assert_eq!(reference, trace_all(seed), "all seed {seed}");
        }
        println!("{{\"equivalence_traces\":20000,\"observed_operations\":{operations},\"trace_digest\":\"{:016x}\"}}",hasher.finish());
        // Removing multi-pending support is intentional, and is not claimed to be
        // equivalent for formerly possible internal sequences outside production.
        let mut base = baseline::ActiveTakeWriter::new("s").unwrap();
        for n in 0..2 {
            base.reserve_frame(baseline::ActiveSourceFrame {
                source_sequence: n,
                host_monotonic_ns: n,
                source_gap: 0,
            })
            .unwrap();
        }
        let mut slim = a2::ActiveTakeWriter::new("s").unwrap();
        slim.reserve_frame(a2::ActiveSourceFrame {
            source_sequence: 0,
            host_monotonic_ns: 0,
            source_gap: 0,
        })
        .unwrap();
        assert_eq!(
            slim.reserve_frame(a2::ActiveSourceFrame {
                source_sequence: 1,
                host_monotonic_ns: 1,
                source_gap: 0
            })
            .unwrap_err()
            .code,
            "invalid_state"
        );
        println!("{{\"overlapping_reservations\":\"baseline accepts two; A2 intentionally rejects second with invalid_state\"}}");
        let mut unguarded = negative::ActiveTakeWriter::new("s").unwrap();
        unguarded
            .reserve_frame(negative::ActiveSourceFrame {
                source_sequence: 0,
                host_monotonic_ns: 0,
                source_gap: 0,
            })
            .unwrap();
        let invalid = unguarded.finish().unwrap();
        assert_eq!(invalid.pending_frames, 1);
        println!("{{\"negative_control\":\"removing pending-finish guard incorrectly seals with one unfinished frame\"}}");
        println!("{{\"writer_size_bytes\":{{\"baseline\":{},\"a1\":{},\"a2\":{},\"a3\":{},\"all\":{}}}}}",std::mem::size_of::<baseline::ActiveTakeWriter>(),std::mem::size_of::<a1::ActiveTakeWriter>(),std::mem::size_of::<a2::ActiveTakeWriter>(),std::mem::size_of::<a3::ActiveTakeWriter>(),std::mem::size_of::<all::ActiveTakeWriter>());
        for (_, bench) in funcs {
            black_box(bench(10000));
        }
        for round in 0..9 {
            for offset in 0..6 {
                let (name, bench) = funcs[(round + offset) % 6];
                let start = Instant::now();
                let result = bench(1000000);
                let ns = start.elapsed().as_nanos();
                println!("{{\"variant\":\"{name}\",\"round\":{round},\"frames\":1000000,\"elapsed_ns\":{ns},\"result\":{result}}}");
            }
        }
    }
}
