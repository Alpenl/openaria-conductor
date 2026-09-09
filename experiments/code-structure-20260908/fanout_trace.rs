#![allow(dead_code)]

#[path = "../imu.rs"]
mod imu;
#[path = "../recording.rs"]
mod recording;

use recording::CaptureFanoutState;
use sha2::{Digest, Sha256};

fn main() {
    const ACTIONS: u64 = 10;
    const DEPTH: u32 = 5;
    let mut digest = Sha256::new();
    let mut transitions = 0_u64;
    let mut traces = 0_u64;
    for decimation in [1, 2, 3, 60] {
        for sequence in 0..ACTIONS.pow(DEPTH) {
            let mut fanout = CaptureFanoutState::new(decimation).unwrap();
            let mut remaining = sequence;
            digest.update(format!("{decimation}:{sequence}\n"));
            for _ in 0..DEPTH {
                let action = remaining % ACTIONS;
                remaining /= ACTIONS;
                let result = match action {
                    0 => format!("{:?}", fanout.start_recording()),
                    1 => format!("{:?}", fanout.finish_frame()),
                    2 => format!("{:?}", fanout.start_stopping()),
                    3 => format!("{:?}", fanout.mark_failure()),
                    4 => format!("{:?}", fanout.begin_frame(0, true)),
                    5 => format!("{:?}", fanout.begin_frame(0, false)),
                    6 => format!("{:?}", fanout.begin_frame(1, true)),
                    7 => format!("{:?}", fanout.begin_frame(3, false)),
                    8 => format!("{:?}", fanout.begin_frame(u64::MAX - 2, true)),
                    9 => format!("{:?}", fanout.begin_frame(u64::MAX, true)),
                    _ => unreachable!(),
                };
                digest.update(format!("{action}:{result}:{:?}\n", fanout.snapshot()));
                transitions += 1;
            }
            traces += 1;
        }
    }
    println!(
        "{{\"traces\":{traces},\"transitions\":{transitions},\"sha256\":\"{:x}\"}}",
        digest.finalize(),
    );
}
