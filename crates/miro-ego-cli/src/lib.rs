//! Unified Miro-Ego 0.3.0 native Rust device runtime and CLI library.
//!
//! Wires together all seven workspace crates (`miro-ego-core`, `miro-ego-capture`,
//! `miro-ego-storage`, `miro-ego-api`, `miro-ego-network`, `miro-ego-hw`, and
//! `miro-ego-deploy`) into the single `miro-ego` binary and exposes the production
//! daemon runtime for end-to-end integration tests.

pub mod commands;
pub mod runtime;

/// Embedded `miro-ego.device-session.v1` JSON Schema used by `miro-ego validate`.
pub const DEVICE_SESSION_V1_SCHEMA: &str =
    include_str!("../../../contracts/schemas/miro-ego-device-session-v1.schema.json");
