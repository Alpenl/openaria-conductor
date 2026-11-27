//! Core system, filesystem, locking, path, configuration, identifier, and telemetry primitives
//! for the Miro-Ego native Rust runtime.

pub mod artifact_verify;
pub mod config;
pub mod error;
pub mod ids;
pub mod locks;
pub mod media_types;
pub mod paths;
pub mod schema_validation;
pub mod sys;
pub mod telemetry;

pub use error::{CoreError, CoreResult};
