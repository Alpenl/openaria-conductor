//! Native firmware release bundle verification, Backlog-ready CAS/offline hooks, and
//! transactional A/B deployment manager for Miro-Ego (0 Python runtime, staging bundles on
//! `/data/updates/` to protect the 20 GiB system disk).

pub mod bundle;
pub mod manager;
pub mod rpc;

use miro_ego_core::CoreError;
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;

pub use bundle::{
    BUNDLE_SCHEMA, BUNDLE_SIGNATURE_DOMAIN, BUNDLE_SIGNATURE_FILENAME, BundleFileEntry,
    BundleManifest, DEFAULT_RELEASE_SIGNING_SEED_LABEL, DefaultOfflinePackageHook, LocalCasStore,
    MAX_BINARY_BYTES, MAX_BUNDLE_FILES, MAX_MANIFEST_BYTES, MAX_SIGNATURE_FILE_BYTES,
    MAX_TOTAL_BUNDLE_BYTES, OfflinePackageDescriptor, OfflinePackageHook, REQUIRED_BINARIES,
    StoreLayer, TARGET_PLATFORM, VerifiedBundle, copy_and_verify_safe, default_release_public_key,
    default_release_signing_seed, hash_file_safe, is_allowed_bundle_asset_path,
    sign_bundle_manifest_with_seed, validate_aarch64_elf64_header, validate_bundle_relative_path,
    validate_commit_sha, verify_bundle_dir, verify_bundle_dir_with_key,
};
pub use manager::{
    ACTIVATION_FAILURE_SCHEMA, ActivationFailureRecord, AssetSpec, BoundedSystemctlRunner,
    DEPLOYMENT_ASSETS, DEPLOYMENT_LOCK_NAME, DeploymentStatus, HealthChecker,
    INSTALL_TRANSACTION_SCHEMA, InstallTransaction, InstalledReleaseMetadata,
    LocalHttpHealthChecker, RELEASE_METADATA_SCHEMA, RealSystemctlRunner, ReleaseManager,
    ReleaseManagerPaths, SystemctlRunner, TransactionAction, TransactionState, UninstallReport,
};
pub use rpc::{
    DEFAULT_ALLOWED_UPDATES_ROOT, DEFAULT_DEPLOY_CLIENT_TIMEOUT, DEFAULT_DEPLOY_IDLE_TIMEOUT,
    DEFAULT_DEPLOY_SOCKET_PATH, DEPLOY_REQUEST_SCHEMA, DEPLOY_RESPONSE_SCHEMA, DeployControlStatus,
    DeployMutationOutcome, DeployRpcClient, DeployRpcConfig, DeployRpcErrorBody, DeployRpcRequest,
    DeployRpcResponse, DeployRpcServer, DeployRpcServerConfig, FIRMWARE_TASK_STATE_SCHEMA,
    FirmwareTaskAction, FirmwareTaskSnapshot, FirmwareTaskStatus, MAX_DEPLOY_IDEMPOTENCY_ENTRIES,
    MAX_DEPLOY_RPC_LINE_BYTES,
};

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
