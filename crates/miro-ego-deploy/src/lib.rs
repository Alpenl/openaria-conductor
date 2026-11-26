//! Firmware release metadata, staged OTA update verification, and atomic deployment
//! traits for Miro-Ego (staging bundles on `/data/updates/` to protect the 20 GiB system disk).

use miro_ego_core::CoreError;
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;

pub const FIRMWARE_STATUS_SCHEMA: &str = "miro-ego.firmware-status.v1";

#[derive(Debug, Error)]
pub enum DeployError {
    #[error("release bundle invalid: {message}")]
    InvalidBundle { message: String },

    #[error("deployment blocked: {message}")]
    Blocked { message: String },

    #[error("deployment failed: {message}")]
    Failed { message: String },

    #[error(transparent)]
    Core(#[from] CoreError),
}

pub type DeployResult<T> = Result<T, DeployError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateStage {
    Idle,
    Staged,
    Verified,
    Applying,
    RebootPending,
    RolledBack,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseInfo {
    pub schema: String,
    pub version: String,
    pub commit_sha: String,
    pub stage: UpdateStage,
}

pub trait FirmwareManager: Send + Sync {
    fn current_release(&self) -> DeployResult<ReleaseInfo>;
    fn verify_bundle(&self, bundle_path: &Path) -> DeployResult<ReleaseInfo>;
    fn apply_update(&self, bundle_path: &Path) -> DeployResult<ReleaseInfo>;
    fn rollback(&self) -> DeployResult<ReleaseInfo>;
}
