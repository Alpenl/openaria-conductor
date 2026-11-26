//! Single unified on-device binary entrypoint (`miro-ego`) for Miro-Ego 0.3.0.

use miro_ego_cli::commands::{parse_args, run};
use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    match parse_args(&args).and_then(run) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("miro-ego: {message}");
            ExitCode::FAILURE
        }
    }
}
