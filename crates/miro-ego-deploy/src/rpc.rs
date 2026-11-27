//! Framed NDJSON Unix-domain-socket RPC server & client (`/run/miro-ego/deploy-control.sock`).
//!
//! Enables the unprivileged `miro-ego.service` API daemon (`User=miro-ego`, `NoNewPrivileges=yes`)
//! to trigger real privileged root `ReleaseManager` A/B updates and rollbacks while tracking
//! `FirmwareTaskSnapshot` progress (`accepted` -> `running` -> `succeeded` / `failed`), persisting
//! bounded idempotency & task state to `<state_root>/firmware-task.json`, enforcing
//! `allowed_updates_root` (`/data/updates`), and supporting clean idle auto-exit.

use crate::bundle::{validate_commit_sha, verify_bundle_manifest};
use crate::manager::{DeploymentStatus, HeldDeployLocks, ReleaseManager};
use crate::{DeployError, DeployResult};
use miro_ego_core::ids::{hex_encode, new_uuid_v4};
use miro_ego_core::locks::{LockMode, acquire_flock, acquire_maintenance_lock};
use miro_ego_core::sys::fs::{read_bounded_json, write_bytes_atomic};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::ffi::{CString, OsStr};
use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::{UnixDatagram, UnixListener, UnixStream};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub const DEFAULT_DEPLOY_SOCKET_PATH: &str = "/run/miro-ego/deploy-control.sock";
pub const DEFAULT_ALLOWED_UPDATES_ROOT: &str = "/data/updates";
pub const DEPLOY_REQUEST_SCHEMA: &str = "miro-ego.deploy-control-request.v1";
pub const DEPLOY_RESPONSE_SCHEMA: &str = "miro-ego.deploy-control-response.v1";
pub const FIRMWARE_TASK_STATE_SCHEMA: &str = "miro-ego.firmware-task-state.v1";
pub const MAX_DEPLOY_RPC_LINE_BYTES: usize = 64 * 1024;
pub const MAX_DEPLOY_TASK_STATE_BYTES: usize = 256 * 1024;
pub const MAX_DEPLOY_IDEMPOTENCY_ENTRIES: usize = 256;
pub const DEFAULT_DEPLOY_SERVER_READ_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_DEPLOY_SERVER_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_DEPLOY_CLIENT_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_DEPLOY_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_DEPLOY_MAX_CONNS: usize = 8;

static DEPLOY_UMASK_LOCK: Mutex<()> = Mutex::new(());

/// Firmware task action matching OpenAPI `FirmwareTask.action`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FirmwareTaskAction {
    Update,
    Rollback,
}

/// Firmware task lifecycle state matching OpenAPI `FirmwareTask.status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FirmwareTaskStatus {
    Accepted,
    Running,
    Succeeded,
    Failed,
}

impl FirmwareTaskStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }
}

/// Authoritative privileged task snapshot matching OpenAPI `FirmwareTask`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FirmwareTaskSnapshot {
    pub id: String,
    pub action: FirmwareTaskAction,
    pub target_commit: String,
    pub status: FirmwareTaskStatus,
    pub message: String,
    pub started_at: u64,
    pub finished_at: Option<u64>,
}

/// Outcome of an idempotent `install` or `rollback` RPC request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeployMutationOutcome {
    pub task: FirmwareTaskSnapshot,
    pub replayed: bool,
}

/// Combined deployment + task status returned by `DeployRpcClient::status()`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeployControlStatus {
    pub deployment: DeploymentStatus,
    pub task: Option<FirmwareTaskSnapshot>,
}

/// Wire request envelope sent over `/run/miro-ego/deploy-control.sock`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeployRpcRequest {
    pub schema: String,
    pub operation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<Value>,
}

/// Wire error body returned inside failed deploy RPC responses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeployRpcErrorBody {
    pub code: String,
    pub message: String,
}

/// Wire response envelope returned by the privileged deploy control server.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeployRpcResponse {
    pub schema: String,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replayed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<DeployRpcErrorBody>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retryable: Option<bool>,
}

impl DeployRpcResponse {
    pub fn ok(operation: &str, status: u16, body: Value) -> Self {
        Self {
            schema: DEPLOY_RESPONSE_SCHEMA.to_owned(),
            ok: true,
            operation: Some(operation.to_owned()),
            status: Some(status),
            body: Some(body),
            replayed: None,
            error: None,
            retryable: None,
        }
    }

    pub fn error(
        code: impl Into<String>,
        message: impl Into<String>,
        operation: Option<&str>,
        retryable: bool,
    ) -> Self {
        Self {
            schema: DEPLOY_RESPONSE_SCHEMA.to_owned(),
            ok: false,
            operation: operation.map(str::to_owned),
            status: None,
            body: None,
            replayed: None,
            error: Some(DeployRpcErrorBody {
                code: code.into(),
                message: message.into(),
            }),
            retryable: Some(retryable),
        }
    }
}

/// Configuration for [`DeployRpcServer`].
#[derive(Debug, Clone)]
pub struct DeployRpcServerConfig {
    pub max_line_bytes: usize,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub max_concurrent_connections: usize,
    pub allowed_updates_root: PathBuf,
    pub idle_timeout: Option<Duration>,
}

/// Alias matching [`DeployRpcServerConfig`].
pub type DeployRpcConfig = DeployRpcServerConfig;

impl Default for DeployRpcServerConfig {
    fn default() -> Self {
        Self {
            max_line_bytes: MAX_DEPLOY_RPC_LINE_BYTES,
            read_timeout: DEFAULT_DEPLOY_SERVER_READ_TIMEOUT,
            write_timeout: DEFAULT_DEPLOY_SERVER_WRITE_TIMEOUT,
            max_concurrent_connections: DEFAULT_DEPLOY_MAX_CONNS,
            allowed_updates_root: PathBuf::from(DEFAULT_ALLOWED_UPDATES_ROOT),
            idle_timeout: None,
        }
    }
}

impl DeployRpcServerConfig {
    pub fn with_allowed_updates_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.allowed_updates_root = root.into();
        self
    }

    pub fn with_idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.idle_timeout = timeout;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedDeployIdempotencyEntry {
    scope: String,
    request_hash: String,
    recorded_at: u64,
    task: FirmwareTaskSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedFirmwareTaskState {
    schema: String,
    latest_task: Option<FirmwareTaskSnapshot>,
    #[serde(default)]
    idempotency: Vec<PersistedDeployIdempotencyEntry>,
}

impl Default for PersistedFirmwareTaskState {
    fn default() -> Self {
        Self {
            schema: FIRMWARE_TASK_STATE_SCHEMA.to_owned(),
            latest_task: None,
            idempotency: Vec::new(),
        }
    }
}

struct TaskState {
    active: bool,
    response_flushed: bool,
    latest_task: Option<FirmwareTaskSnapshot>,
    idempotency: Vec<PersistedDeployIdempotencyEntry>,
    worker_handle: Option<JoinHandle<()>>,
}

struct ActiveTaskDropGuard {
    coord: Arc<DeployTaskCoordinator>,
    task_id: String,
    completed_cleanly: bool,
}

impl Drop for ActiveTaskDropGuard {
    fn drop(&mut self) {
        if self.completed_cleanly {
            return;
        }
        let finished_at = unix_now_secs();
        let mut guard = self
            .coord
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(cur) = guard.latest_task.as_mut() {
            if cur.id == self.task_id && !cur.status.is_terminal() {
                cur.status = FirmwareTaskStatus::Failed;
                cur.message = "deploy worker terminated unexpectedly".to_owned();
                cur.finished_at = Some(finished_at);
            }
        }
        if let Some(snapshot) = guard.latest_task.clone() {
            for entry in &mut guard.idempotency {
                if entry.task.id == snapshot.id {
                    entry.task = snapshot.clone();
                }
            }
        }
        guard.active = false;
        let persisted = PersistedFirmwareTaskState {
            schema: FIRMWARE_TASK_STATE_SCHEMA.to_owned(),
            latest_task: guard.latest_task.clone(),
            idempotency: guard.idempotency.clone(),
        };
        drop(guard);
        let _ = self.coord.persist_task_state(&persisted);
        self.coord.bump_activity();
        self.coord.idle_cv.notify_all();
    }
}

struct ActiveConnDropGuard {
    counter: Arc<AtomicUsize>,
    coord: Arc<DeployTaskCoordinator>,
}

impl Drop for ActiveConnDropGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
        self.coord.idle_cv.notify_all();
    }
}

struct DeployTaskCoordinator {
    manager: Arc<ReleaseManager>,
    allowed_updates_root: PathBuf,
    state: Mutex<TaskState>,
    idle_cv: Condvar,
    flushed_cv: Condvar,
    last_activity_mono_ns: AtomicU64,
}

impl DeployTaskCoordinator {
    fn new(manager: Arc<ReleaseManager>, allowed_updates_root: PathBuf) -> Arc<Self> {
        let initial = load_initial_task_state(&manager);
        let now_ns = monotonic_now_ns();
        Arc::new(Self {
            manager,
            allowed_updates_root,
            state: Mutex::new(TaskState {
                active: false,
                response_flushed: false,
                latest_task: initial.latest_task,
                idempotency: initial.idempotency,
                worker_handle: None,
            }),
            idle_cv: Condvar::new(),
            flushed_cv: Condvar::new(),
            last_activity_mono_ns: AtomicU64::new(now_ns),
        })
    }

    fn task_state_path(&self) -> PathBuf {
        self.manager.paths().firmware_task_file()
    }

    fn persist_task_state(&self, state: &PersistedFirmwareTaskState) -> DeployResult<()> {
        let path = self.task_state_path();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|err| DeployError::Failed {
                    message: format!("failed creating state dir {}: {err}", parent.display()),
                })?;
            }
        }
        let mut raw = serde_json::to_vec_pretty(state).map_err(|err| DeployError::Failed {
            message: format!("failed serializing firmware-task.json: {err}"),
        })?;
        raw.push(b'\n');
        write_bytes_atomic(&path, &raw, 0o600, true).map_err(|err| DeployError::Failed {
            message: format!("failed writing {}: {err}", path.display()),
        })
    }

    fn bump_activity(&self) {
        self.last_activity_mono_ns
            .store(monotonic_now_ns(), Ordering::Release);
    }

    fn is_active(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .active
    }

    fn status(&self) -> DeployResult<DeployControlStatus> {
        let deployment = self.manager.status()?;
        let task = self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .latest_task
            .clone();
        Ok(DeployControlStatus { deployment, task })
    }

    fn notify_response_flushed(&self) {
        let mut guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        guard.response_flushed = true;
        self.flushed_cv.notify_all();
    }

    fn wait_for_idle(&self, timeout: Duration) -> DeployResult<Option<FirmwareTaskSnapshot>> {
        self.notify_response_flushed();
        let deadline = Instant::now() + timeout;
        let mut join_target = None;
        let snapshot = {
            let mut guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            while guard.active {
                let now = Instant::now();
                if now >= deadline {
                    return Err(DeployError::Blocked {
                        message: "deployment_busy: timed out waiting for firmware task".to_owned(),
                    });
                }
                let wait_dur = deadline.saturating_duration_since(now);
                let (next, _) = self
                    .idle_cv
                    .wait_timeout(guard, wait_dur)
                    .unwrap_or_else(PoisonError::into_inner);
                guard = next;
            }
            if guard
                .worker_handle
                .as_ref()
                .is_some_and(std::thread::JoinHandle::is_finished)
            {
                join_target = guard.worker_handle.take();
            }
            guard.latest_task.clone()
        };
        if let Some(h) = join_target {
            let _ = h.join();
        }
        Ok(snapshot)
    }

    fn acquire_task_locks(&self) -> DeployResult<HeldDeployLocks> {
        fs::create_dir_all(&self.manager.paths().run_dir).map_err(|err| DeployError::Failed {
            message: format!(
                "failed creating run dir {}: {err}",
                self.manager.paths().run_dir.display()
            ),
        })?;
        let deploy = acquire_flock(
            self.manager.paths().deployment_lock_file(),
            LockMode::Exclusive,
            true,
        )
        .map_err(|err| match err {
            miro_ego_core::CoreError::LockBusy { message } => DeployError::Blocked {
                message: format!("deployment_busy: {message}"),
            },
            other => DeployError::Core(other),
        })?;
        let maint =
            acquire_maintenance_lock(&self.manager.paths().run_dir, LockMode::Exclusive, true)
                .map_err(|err| match err {
                    miro_ego_core::CoreError::LockBusy { message } => DeployError::Blocked {
                        message: format!("maintenance_busy: {message}"),
                    },
                    other => DeployError::Core(other),
                })?;
        Ok(HeldDeployLocks::new(deploy, maint))
    }

    fn check_idempotency_replay(
        &self,
        scope: &str,
        request_hash: &str,
    ) -> DeployResult<Option<FirmwareTaskSnapshot>> {
        let guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(entry) = guard.idempotency.iter().find(|e| e.scope == scope) {
            if entry.request_hash != request_hash {
                return Err(DeployError::Blocked {
                    message:
                        "idempotency_conflict: idempotency_key reused with different firmware payload"
                            .to_owned(),
                });
            }
            let current = guard
                .latest_task
                .as_ref()
                .filter(|t| t.id == entry.task.id)
                .cloned()
                .unwrap_or_else(|| entry.task.clone());
            return Ok(Some(current));
        }
        Ok(None)
    }

    fn start_install(
        self: &Arc<Self>,
        bundle_dir: PathBuf,
        expected_commit: String,
        activate: bool,
        idem_meta: Option<(String, String)>,
    ) -> DeployResult<DeployMutationOutcome> {
        self.bump_activity();
        validate_commit_sha(&expected_commit)?;

        if let Some((ref scope, ref req_hash)) = idem_meta {
            if let Some(replayed_task) = self.check_idempotency_replay(scope, req_hash)? {
                return Ok(DeployMutationOutcome {
                    task: replayed_task,
                    replayed: true,
                });
            }
        }

        validate_bundle_under_updates_root(&self.allowed_updates_root, &bundle_dir)?;

        {
            let guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if guard.active {
                return Err(DeployError::Blocked {
                    message: "deployment_busy: another firmware task is already active".to_owned(),
                });
            }
        }
        let locks = self.acquire_task_locks()?;

        // Fast synchronous preflight (`bundle.json` + Ed25519 `bundle.json.sig` + file sizes +
        // 64B ELF headers); heavy payload copy & SHA-256 verification execute once in background
        // `stage_release`!
        let verified = verify_bundle_manifest(&bundle_dir)?;
        self.manager.offline_hook().pre_install_verify(&verified)?;
        if verified.commit() != expected_commit {
            return Err(DeployError::InvalidBundle {
                message: format!(
                    "commit_mismatch: bundle commit {} does not match expected {expected_commit}",
                    verified.commit()
                ),
            });
        }

        let task = FirmwareTaskSnapshot {
            id: new_uuid_v4(),
            action: FirmwareTaskAction::Update,
            target_commit: expected_commit,
            status: FirmwareTaskStatus::Accepted,
            message: format!("accepted firmware update {}", verified.version()),
            started_at: unix_now_secs(),
            finished_at: None,
        };

        let started = self.spawn_worker(task, idem_meta, locks, move |mgr, held_locks| {
            mgr.install_verified_with_locks(&verified, activate, held_locks)
        })?;
        Ok(DeployMutationOutcome {
            task: started,
            replayed: false,
        })
    }

    fn start_rollback(
        self: &Arc<Self>,
        expected_previous: String,
        activate: bool,
        idem_meta: Option<(String, String)>,
    ) -> DeployResult<DeployMutationOutcome> {
        self.bump_activity();
        validate_commit_sha(&expected_previous)?;

        if let Some((ref scope, ref req_hash)) = idem_meta {
            if let Some(replayed_task) = self.check_idempotency_replay(scope, req_hash)? {
                return Ok(DeployMutationOutcome {
                    task: replayed_task,
                    replayed: true,
                });
            }
        }

        {
            let guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if guard.active {
                return Err(DeployError::Blocked {
                    message: "deployment_busy: another firmware task is already active".to_owned(),
                });
            }
        }
        let locks = self.acquire_task_locks()?;

        let current_status = self.manager.status()?;
        if !current_status.installed {
            return Err(DeployError::Blocked {
                message: "rollback_unavailable: no active release installed".to_owned(),
            });
        }
        if current_status.previous.as_deref() != Some(expected_previous.as_str()) {
            return Err(DeployError::Blocked {
                message: format!(
                    "rollback_unavailable: expected previous {expected_previous}, actual {:?}",
                    current_status.previous
                ),
            });
        }

        let task = FirmwareTaskSnapshot {
            id: new_uuid_v4(),
            action: FirmwareTaskAction::Rollback,
            target_commit: expected_previous,
            status: FirmwareTaskStatus::Accepted,
            message: "accepted firmware rollback".to_owned(),
            started_at: unix_now_secs(),
            finished_at: None,
        };

        let started = self.spawn_worker(task, idem_meta, locks, move |mgr, held_locks| {
            mgr.rollback_with_locks(activate, held_locks)
        })?;
        Ok(DeployMutationOutcome {
            task: started,
            replayed: false,
        })
    }

    fn spawn_worker<F>(
        self: &Arc<Self>,
        initial_task: FirmwareTaskSnapshot,
        idem_meta: Option<(String, String)>,
        locks: HeldDeployLocks,
        op: F,
    ) -> DeployResult<FirmwareTaskSnapshot>
    where
        F: FnOnce(&ReleaseManager, &mut HeldDeployLocks) -> DeployResult<DeploymentStatus>
            + Send
            + 'static,
    {
        let (prev_handle, to_persist) = {
            let mut guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some((ref scope, ref req_hash)) = idem_meta {
                if let Some(existing) = guard.idempotency.iter().find(|e| &e.scope == scope) {
                    if &existing.request_hash != req_hash {
                        return Err(DeployError::Blocked {
                            message:
                                "idempotency_conflict: idempotency_key reused with different firmware payload"
                                    .to_owned(),
                        });
                    }
                    return Ok(existing.task.clone());
                }
            }
            if guard.active {
                return Err(DeployError::Blocked {
                    message: "deployment_busy: another firmware task is already active".to_owned(),
                });
            }
            guard.active = true;
            guard.response_flushed = false;
            guard.latest_task = Some(initial_task.clone());
            if let Some((scope, request_hash)) = idem_meta {
                while guard.idempotency.len() >= MAX_DEPLOY_IDEMPOTENCY_ENTRIES {
                    guard.idempotency.remove(0);
                }
                guard.idempotency.push(PersistedDeployIdempotencyEntry {
                    scope,
                    request_hash,
                    recorded_at: initial_task.started_at,
                    task: initial_task.clone(),
                });
            }
            let persisted = PersistedFirmwareTaskState {
                schema: FIRMWARE_TASK_STATE_SCHEMA.to_owned(),
                latest_task: guard.latest_task.clone(),
                idempotency: guard.idempotency.clone(),
            };
            (guard.worker_handle.take(), persisted)
        };
        if let Some(h) = prev_handle {
            let _ = h.join();
        }
        let _ = self.persist_task_state(&to_persist);

        let coord = Arc::clone(self);
        let task_id = initial_task.id.clone();
        let spawn_res = thread::Builder::new()
            .name("miro-ego-deploy-tx".to_owned())
            .spawn(move || {
                let mut held_locks = locks;
                let mut active_guard = ActiveTaskDropGuard {
                    coord: Arc::clone(&coord),
                    task_id: task_id.clone(),
                    completed_cleanly: false,
                };

                {
                    let mut guard = coord.state.lock().unwrap_or_else(PoisonError::into_inner);
                    while !guard.response_flushed {
                        let (next, wait_res) = coord
                            .flushed_cv
                            .wait_timeout(guard, Duration::from_secs(2))
                            .unwrap_or_else(PoisonError::into_inner);
                        guard = next;
                        if wait_res.timed_out() {
                            break;
                        }
                    }
                }

                coord.bump_activity();
                let running_persisted = {
                    let mut guard = coord.state.lock().unwrap_or_else(PoisonError::into_inner);
                    if let Some(cur) = guard.latest_task.as_mut() {
                        if cur.id == task_id {
                            cur.status = FirmwareTaskStatus::Running;
                            cur.message = match cur.action {
                                FirmwareTaskAction::Update => {
                                    "installing and activating firmware release".to_owned()
                                }
                                FirmwareTaskAction::Rollback => {
                                    "rolling back to previous firmware release".to_owned()
                                }
                            };
                        }
                    }
                    if let Some(snap) = guard.latest_task.clone() {
                        for entry in &mut guard.idempotency {
                            if entry.task.id == snap.id {
                                entry.task = snap.clone();
                            }
                        }
                    }
                    PersistedFirmwareTaskState {
                        schema: FIRMWARE_TASK_STATE_SCHEMA.to_owned(),
                        latest_task: guard.latest_task.clone(),
                        idempotency: guard.idempotency.clone(),
                    }
                };
                let _ = coord.persist_task_state(&running_persisted);

                let outcome = op(&coord.manager, &mut held_locks);
                drop(held_locks);
                let finished_at = unix_now_secs();

                let final_persisted = {
                    let mut guard = coord.state.lock().unwrap_or_else(PoisonError::into_inner);
                    if let Some(cur) = guard.latest_task.as_mut() {
                        if cur.id == task_id {
                            match outcome {
                                Ok(st) => {
                                    cur.status = FirmwareTaskStatus::Succeeded;
                                    cur.message = format!(
                                        "active commit {}",
                                        st.current.unwrap_or_else(|| "none".to_owned())
                                    );
                                    cur.finished_at = Some(finished_at);
                                }
                                Err(err) => {
                                    cur.status = FirmwareTaskStatus::Failed;
                                    cur.message = truncate_msg(&err.to_string(), 1024);
                                    cur.finished_at = Some(finished_at);
                                }
                            }
                        }
                    }
                    if let Some(snap) = guard.latest_task.clone() {
                        for entry in &mut guard.idempotency {
                            if entry.task.id == snap.id {
                                entry.task = snap.clone();
                            }
                        }
                    }
                    guard.active = false;
                    PersistedFirmwareTaskState {
                        schema: FIRMWARE_TASK_STATE_SCHEMA.to_owned(),
                        latest_task: guard.latest_task.clone(),
                        idempotency: guard.idempotency.clone(),
                    }
                };
                let _ = coord.persist_task_state(&final_persisted);
                active_guard.completed_cleanly = true;
                coord.bump_activity();
                coord.idle_cv.notify_all();
            });

        match spawn_res {
            Ok(handle) => {
                let mut guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                guard.worker_handle = Some(handle);
                Ok(initial_task)
            }
            Err(err) => {
                let persisted = {
                    let mut guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                    guard.active = false;
                    if let Some(cur) = guard.latest_task.as_mut() {
                        cur.status = FirmwareTaskStatus::Failed;
                        cur.message = format!("failed spawning deploy worker: {err}");
                        cur.finished_at = Some(unix_now_secs());
                    }
                    if let Some(snap) = guard.latest_task.clone() {
                        for entry in &mut guard.idempotency {
                            if entry.task.id == snap.id {
                                entry.task = snap.clone();
                            }
                        }
                    }
                    self.idle_cv.notify_all();
                    PersistedFirmwareTaskState {
                        schema: FIRMWARE_TASK_STATE_SCHEMA.to_owned(),
                        latest_task: guard.latest_task.clone(),
                        idempotency: guard.idempotency.clone(),
                    }
                };
                let _ = self.persist_task_state(&persisted);
                Err(DeployError::Failed {
                    message: format!("failed to spawn deploy worker thread: {err}"),
                })
            }
        }
    }
}

fn load_initial_task_state(manager: &ReleaseManager) -> PersistedFirmwareTaskState {
    let path = manager.paths().firmware_task_file();
    let Ok(mut loaded) =
        read_bounded_json::<PersistedFirmwareTaskState>(&path, MAX_DEPLOY_TASK_STATE_BYTES, 16)
    else {
        return PersistedFirmwareTaskState::default();
    };
    if loaded.schema != FIRMWARE_TASK_STATE_SCHEMA {
        return PersistedFirmwareTaskState::default();
    }
    let mut dirty = false;
    if loaded.idempotency.len() > MAX_DEPLOY_IDEMPOTENCY_ENTRIES {
        let excess = loaded.idempotency.len() - MAX_DEPLOY_IDEMPOTENCY_ENTRIES;
        loaded.idempotency.drain(0..excess);
        dirty = true;
    }
    if let Some(cur) = loaded.latest_task.as_mut() {
        if !cur.status.is_terminal() {
            cur.status = FirmwareTaskStatus::Failed;
            cur.message = "firmware task interrupted by deploy controller restart".to_owned();
            cur.finished_at = Some(unix_now_secs());
            for entry in &mut loaded.idempotency {
                if entry.task.id == cur.id {
                    entry.task = cur.clone();
                }
            }
            dirty = true;
        }
    }
    if dirty {
        if let Ok(mut raw) = serde_json::to_vec_pretty(&loaded) {
            raw.push(b'\n');
            let _ = write_bytes_atomic(&path, &raw, 0o600, true);
        }
    }
    loaded
}

/// Verify that `allowed_updates_root` and `bundle_dir` are normalized absolute paths with zero
/// symlinks along any path component, and that `bundle_dir` resides directly inside
/// `allowed_updates_root`.
pub fn validate_bundle_under_updates_root(
    allowed_updates_root: &Path,
    bundle_dir: &Path,
) -> DeployResult<()> {
    let root_components = collect_strict_absolute_components(allowed_updates_root, "updates_root")?;
    let bundle_components = collect_strict_absolute_components(bundle_dir, "bundle_dir")?;

    if bundle_components.len() != root_components.len() + 1
        || bundle_components[..root_components.len()] != root_components[..]
    {
        return Err(DeployError::InvalidBundle {
            message: format!(
                "bundle_dir {} must reside directly inside allowed updates root {}",
                bundle_dir.display(),
                allowed_updates_root.display()
            ),
        });
    }

    let mut cur = PathBuf::from("/");
    let root_dir_meta = fs::symlink_metadata(&cur).map_err(|err| DeployError::InvalidBundle {
        message: format!("cannot stat /: {err}"),
    })?;
    if root_dir_meta.file_type().is_symlink() || !root_dir_meta.file_type().is_dir() {
        return Err(DeployError::InvalidBundle {
            message: "root path / is not a directory".to_owned(),
        });
    }

    for seg in &bundle_components {
        cur.push(seg);
        let meta = fs::symlink_metadata(&cur).map_err(|err| DeployError::InvalidBundle {
            message: format!("bundle path component unavailable {}: {err}", cur.display()),
        })?;
        if meta.file_type().is_symlink() {
            return Err(DeployError::InvalidBundle {
                message: format!("symlink rejected along bundle path: {}", cur.display()),
            });
        }
        if !meta.file_type().is_dir() {
            return Err(DeployError::InvalidBundle {
                message: format!(
                    "bundle path component is not a directory: {}",
                    cur.display()
                ),
            });
        }
    }
    Ok(())
}

fn collect_strict_absolute_components<'a>(
    path: &'a Path,
    label: &str,
) -> DeployResult<Vec<&'a std::ffi::OsStr>> {
    if !path.is_absolute() {
        return Err(DeployError::InvalidBundle {
            message: format!("{label} must be an absolute path: {}", path.display()),
        });
    }
    let mut normals = Vec::new();
    for comp in path.components() {
        match comp {
            Component::RootDir => {}
            Component::Normal(seg) if !seg.is_empty() => normals.push(seg),
            _ => {
                return Err(DeployError::InvalidBundle {
                    message: format!(
                        "{label} contains forbidden traversal or non-normal component: {}",
                        path.display()
                    ),
                });
            }
        }
    }
    Ok(normals)
}

/// Bounded framed-JSON Unix domain socket server exposing privileged [`ReleaseManager`] operations.
pub struct DeployRpcServer {
    socket_path: Option<PathBuf>,
    stop: Arc<AtomicBool>,
    idle_exited: Arc<AtomicBool>,
    accept_thread: Option<JoinHandle<()>>,
    active_workers: Arc<AtomicUsize>,
    coordinator: Arc<DeployTaskCoordinator>,
}

impl DeployRpcServer {
    /// Bind a new Unix domain socket at `socket_path` (mode `0660`, group `miro-ego` when root)
    /// under a restrictive `umask` (`0o077`) and start serving privileged deployment requests.
    pub fn bind(
        socket_path: impl Into<PathBuf>,
        manager: Arc<ReleaseManager>,
        config: DeployRpcServerConfig,
    ) -> DeployResult<Self> {
        let path = socket_path.into();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|err| DeployError::Failed {
                    message: format!("failed to create socket dir {}: {err}", parent.display()),
                })?;
                if let Ok(pmeta) = fs::symlink_metadata(parent) {
                    if pmeta.file_type().is_symlink() || !pmeta.file_type().is_dir() {
                        return Err(DeployError::Failed {
                            message: format!(
                                "socket parent {} must be a real directory, not a symlink",
                                parent.display()
                            ),
                        });
                    }
                }
            }
        }
        if let Ok(existing) = fs::symlink_metadata(&path) {
            if existing.file_type().is_dir() {
                return Err(DeployError::Failed {
                    message: format!(
                        "refusing to unlink directory at socket path {}",
                        path.display()
                    ),
                });
            }
            let _ = fs::remove_file(&path);
        }

        let listener = {
            let _umask_lock = DEPLOY_UMASK_LOCK
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let prev_mask = unsafe { libc::umask(0o077) };
            let bind_res = UnixListener::bind(&path);
            unsafe {
                libc::umask(prev_mask);
            }
            bind_res.map_err(|err| DeployError::Failed {
                message: format!(
                    "failed to bind deploy Unix socket {}: {err}",
                    path.display()
                ),
            })?
        };

        let setup_res = (|| -> DeployResult<()> {
            let meta = fs::symlink_metadata(&path).map_err(|err| DeployError::Failed {
                message: format!(
                    "failed to stat bound deploy socket {}: {err}",
                    path.display()
                ),
            })?;
            if meta.file_type().is_symlink() || !meta.file_type().is_socket() {
                return Err(DeployError::Failed {
                    message: format!("bound deploy path {} is not a Unix socket", path.display()),
                });
            }
            best_effort_chgrp_miro_ego(&path);
            chmod_socket_nofollow(&path, 0o660)?;
            Ok(())
        })();

        if let Err(err) = setup_res {
            drop(listener);
            let _ = fs::remove_file(&path);
            return Err(err);
        }

        match Self::from_listener(listener, Some(path.clone()), manager, config) {
            Ok(srv) => {
                let _ = send_systemd_notify("READY=1\n");
                Ok(srv)
            }
            Err(err) => {
                let _ = fs::remove_file(&path);
                Err(err)
            }
        }
    }

    /// Start serving on an already-bound (or systemd socket-activated) `UnixListener`.
    pub fn from_listener(
        listener: UnixListener,
        socket_path: Option<PathBuf>,
        manager: Arc<ReleaseManager>,
        config: DeployRpcServerConfig,
    ) -> DeployResult<Self> {
        listener
            .set_nonblocking(true)
            .map_err(|err| DeployError::Failed {
                message: format!("failed to configure nonblocking deploy listener: {err}"),
            })?;

        let stop = Arc::new(AtomicBool::new(false));
        let idle_exited = Arc::new(AtomicBool::new(false));
        let active_workers = Arc::new(AtomicUsize::new(0));
        let coordinator = DeployTaskCoordinator::new(manager, config.allowed_updates_root.clone());
        let mutation_lock = Arc::new(Mutex::new(()));

        let stop_loop = Arc::clone(&stop);
        let idle_exited_loop = Arc::clone(&idle_exited);
        let workers_loop = Arc::clone(&active_workers);
        let coord_loop = Arc::clone(&coordinator);
        let accept_thread = thread::Builder::new()
            .name("miro-ego-deploy-rpc".to_owned())
            .spawn(move || {
                let mut handles: Vec<JoinHandle<()>> = Vec::new();
                let spawn_accepted =
                    |stream: UnixStream,
                     handles: &mut Vec<JoinHandle<()>>,
                     workers_loop: &Arc<AtomicUsize>,
                     coord_loop: &Arc<DeployTaskCoordinator>,
                     mutation_lock: &Arc<Mutex<()>>,
                     config: &DeployRpcServerConfig| {
                        let current = workers_loop.load(Ordering::Acquire);
                        if current >= config.max_concurrent_connections {
                            let busy = DeployRpcResponse::error(
                                "deployment_busy",
                                "deploy-control server is busy",
                                None,
                                true,
                            );
                            let _ = write_framed_response(
                                stream,
                                &busy,
                                config.max_line_bytes,
                                Duration::from_millis(200),
                            );
                            return;
                        }
                        workers_loop.fetch_add(1, Ordering::AcqRel);
                        let guard = ActiveConnDropGuard {
                            counter: Arc::clone(workers_loop),
                            coord: Arc::clone(coord_loop),
                        };
                        let coord = Arc::clone(coord_loop);
                        let mlock = Arc::clone(mutation_lock);
                        let cfg = config.clone();
                        if let Ok(handle) = thread::Builder::new()
                            .name("miro-ego-deploy-conn".to_owned())
                            .spawn(move || {
                                let _conn_guard = guard;
                                handle_connection(stream, &coord, &mlock, &cfg);
                            })
                        {
                            handles.push(handle);
                        }
                    };

                while !stop_loop.load(Ordering::Acquire) {
                    handles.retain(|h| !h.is_finished());
                    match listener.accept() {
                        Ok((stream, _)) => {
                            spawn_accepted(
                                stream,
                                &mut handles,
                                &workers_loop,
                                &coord_loop,
                                &mutation_lock,
                                &config,
                            );
                        }
                        Err(err) if err.kind() == ErrorKind::WouldBlock => {
                            let mut poll_wait = Duration::from_millis(20);
                            if let Some(idle_limit) = config.idle_timeout {
                                if coord_loop.is_active() {
                                    coord_loop.bump_activity();
                                } else if workers_loop.load(Ordering::Acquire) == 0 {
                                    let last_ns =
                                        coord_loop.last_activity_mono_ns.load(Ordering::Acquire);
                                    let elapsed = Duration::from_nanos(
                                        monotonic_now_ns().saturating_sub(last_ns),
                                    );
                                    if elapsed >= idle_limit {
                                        // Close TOCTOU window: re-attempt `accept()` immediately
                                        // before committing idle exit so a client that queued a
                                        // connection right as the deadline elapsed is served!
                                        match listener.accept() {
                                            Ok((stream, _)) => {
                                                spawn_accepted(
                                                    stream,
                                                    &mut handles,
                                                    &workers_loop,
                                                    &coord_loop,
                                                    &mutation_lock,
                                                    &config,
                                                );
                                                continue;
                                            }
                                            Err(recheck)
                                                if recheck.kind() == ErrorKind::WouldBlock =>
                                            {
                                                if workers_loop.load(Ordering::Acquire) == 0
                                                    && !coord_loop.is_active()
                                                {
                                                    idle_exited_loop.store(true, Ordering::Release);
                                                    stop_loop.store(true, Ordering::Release);
                                                    coord_loop.idle_cv.notify_all();
                                                    break;
                                                }
                                            }
                                            Err(_) => {}
                                        }
                                    } else {
                                        poll_wait = poll_wait.min(
                                            idle_limit
                                                .saturating_sub(elapsed)
                                                .max(Duration::from_millis(1)),
                                        );
                                    }
                                }
                            }
                            poll_listener_readable(listener.as_raw_fd(), poll_wait);
                        }
                        Err(_) => {
                            if stop_loop.load(Ordering::Acquire) {
                                break;
                            }
                            poll_listener_readable(listener.as_raw_fd(), Duration::from_millis(20));
                        }
                    }
                }
                // Drain any connections already queued in the kernel accept backlog before exit.
                while let Ok((stream, _)) = listener.accept() {
                    spawn_accepted(
                        stream,
                        &mut handles,
                        &workers_loop,
                        &coord_loop,
                        &mutation_lock,
                        &config,
                    );
                }
                for handle in handles {
                    let _ = handle.join();
                }
            })
            .map_err(|err| DeployError::Failed {
                message: format!("failed to spawn deploy RPC accept thread: {err}"),
            })?;

        Ok(Self {
            socket_path,
            stop,
            idle_exited,
            accept_thread: Some(accept_thread),
            active_workers,
            coordinator,
        })
    }

    pub fn active_workers(&self) -> usize {
        self.active_workers.load(Ordering::Acquire)
    }

    /// Return `true` when zero RPC connections and zero firmware update/rollback tasks are active.
    pub fn is_idle(&self) -> bool {
        self.active_workers() == 0 && !self.coordinator.is_active()
    }

    /// Return `true` if the server auto-stopped after reaching its configured `idle_timeout`.
    pub fn has_exited_idle(&self) -> bool {
        self.idle_exited.load(Ordering::Acquire)
    }

    /// Return monotonic duration elapsed since the last mutating RPC or active firmware task.
    pub fn idle_elapsed(&self) -> Duration {
        if !self.is_idle() {
            return Duration::ZERO;
        }
        let last_ns = self
            .coordinator
            .last_activity_mono_ns
            .load(Ordering::Acquire);
        Duration::from_nanos(monotonic_now_ns().saturating_sub(last_ns))
    }

    /// Block until `stop` becomes `true` OR the server has been continuously idle (`active_workers == 0`
    /// and no running firmware task) for at least `idle_timeout`.
    pub fn wait_for_idle_or_exit(
        &self,
        idle_timeout: Duration,
        stop: &AtomicBool,
    ) -> DeployResult<()> {
        while !stop.load(Ordering::Acquire) && !self.stop.load(Ordering::Acquire) {
            if self.coordinator.is_active() {
                self.coordinator.bump_activity();
            } else if self.is_idle() && self.idle_elapsed() >= idle_timeout {
                self.idle_exited.store(true, Ordering::Release);
                self.stop.store(true, Ordering::Release);
                self.coordinator.idle_cv.notify_all();
                break;
            }
            let wait_for = if self.is_idle() {
                idle_timeout
                    .saturating_sub(self.idle_elapsed())
                    .min(Duration::from_millis(25))
                    .max(Duration::from_millis(1))
            } else {
                Duration::from_millis(25)
            };
            let guard = self
                .coordinator
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let _ = self.coordinator.idle_cv.wait_timeout(guard, wait_for);
        }
        Ok(())
    }

    /// Wait until any in-flight asynchronous update/rollback worker completes.
    pub fn wait_for_idle(&self, timeout: Duration) -> DeployResult<Option<FirmwareTaskSnapshot>> {
        self.coordinator.wait_for_idle(timeout)
    }

    /// Gracefully stop accepting connections and drain any active task worker.
    pub fn shutdown(mut self) {
        self.shutdown_inner();
    }

    fn shutdown_inner(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.coordinator.notify_response_flushed();
        if let Some(handle) = self.accept_thread.take() {
            let _ = handle.join();
        }
        let task_handle = self
            .coordinator
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .worker_handle
            .take();
        if let Some(th) = task_handle {
            let _ = th.join();
        }
        if let Some(path) = self.socket_path.take() {
            let _ = fs::remove_file(path);
        }
    }
}

impl Drop for DeployRpcServer {
    fn drop(&mut self) {
        self.shutdown_inner();
    }
}

fn handle_connection(
    mut stream: UnixStream,
    coordinator: &Arc<DeployTaskCoordinator>,
    mutation_lock: &Mutex<()>,
    config: &DeployRpcServerConfig,
) {
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(config.read_timeout));
    let _ = stream.set_write_timeout(Some(config.write_timeout));

    let response = match read_ndjson_line(&mut stream, config.max_line_bytes, config.read_timeout) {
        Ok(None) => return,
        Ok(Some(line)) => dispatch_raw_payload(&line, coordinator, mutation_lock),
        Err(FrameReadError::TooLarge) => DeployRpcResponse::error(
            "request_too_large",
            "deploy-control request exceeded max line bytes",
            None,
            false,
        ),
        Err(FrameReadError::Timeout) => DeployRpcResponse::error(
            "request_timeout",
            "deploy-control request timed out",
            None,
            true,
        ),
        Err(FrameReadError::Invalid(msg)) => {
            DeployRpcResponse::error("request_invalid", msg, None, false)
        }
    };

    let _ = write_framed_response(
        stream,
        &response,
        config.max_line_bytes,
        config.write_timeout,
    );
    if matches!(
        response.operation.as_deref(),
        Some("install" | "update" | "rollback")
    ) {
        coordinator.notify_response_flushed();
    }
}

fn extract_idempotency_meta(
    op: &str,
    request: &DeployRpcRequest,
    fallback_payload: &Value,
) -> DeployResult<Option<(String, String)>> {
    let body = request.body.as_ref();
    let idem_key = request.idempotency_key.as_deref().or_else(|| {
        body.and_then(|b| b.get("idempotency_key"))
            .and_then(Value::as_str)
    });
    let Some(key) = idem_key else {
        return Ok(None);
    };
    if key.is_empty() || key.len() > 128 || key.bytes().any(|b| !(0x21..=0x7E).contains(&b)) {
        return Err(DeployError::InvalidBundle {
            message: "request_invalid: idempotency_key must be 1..=128 visible ASCII chars"
                .to_owned(),
        });
    }
    let principal = request
        .principal_id
        .as_deref()
        .or_else(|| {
            body.and_then(|b| b.get("principal_id"))
                .and_then(Value::as_str)
        })
        .unwrap_or("default");
    let explicit_hash = request.request_hash.as_deref().or_else(|| {
        body.and_then(|b| b.get("request_hash"))
            .and_then(Value::as_str)
    });
    let req_hash = match explicit_hash {
        Some(h) if !h.is_empty() && h.len() <= 128 => h.to_owned(),
        Some(_) => {
            return Err(DeployError::InvalidBundle {
                message: "request_invalid: invalid request_hash".to_owned(),
            });
        }
        None => {
            let bytes = serde_json::to_vec(fallback_payload).unwrap_or_default();
            hex_encode(Sha256::digest(&bytes).as_slice())
        }
    };
    let mut scope_hasher = Sha256::new();
    scope_hasher.update(op.as_bytes());
    scope_hasher.update([0u8]);
    scope_hasher.update(principal.as_bytes());
    scope_hasher.update([0u8]);
    scope_hasher.update(key.as_bytes());
    let scope = hex_encode(scope_hasher.finalize().as_slice());
    Ok(Some((scope, req_hash)))
}

fn dispatch_raw_payload(
    raw_line: &[u8],
    coordinator: &Arc<DeployTaskCoordinator>,
    mutation_lock: &Mutex<()>,
) -> DeployRpcResponse {
    let request: DeployRpcRequest = match serde_json::from_slice(raw_line) {
        Ok(req) => req,
        Err(_) => {
            return DeployRpcResponse::error(
                "request_invalid",
                "request must be valid JSON",
                None,
                false,
            );
        }
    };
    if request.schema != DEPLOY_REQUEST_SCHEMA {
        return DeployRpcResponse::error(
            "request_invalid",
            "unsupported deploy-control request schema",
            None,
            false,
        );
    }

    let op = request.operation.as_str();
    match op {
        "health" => DeployRpcResponse::ok(
            "health",
            200,
            json!({
                "ok": true,
                "operations": ["status", "install", "update", "rollback"],
            }),
        ),
        "status" => match coordinator.status() {
            Ok(st) => serialize_ok("status", 200, &st, None),
            Err(err) => error_from_deploy_error(op, &err),
        },
        "install" | "update" => {
            let _guard = mutation_lock.lock().unwrap_or_else(PoisonError::into_inner);
            let Some(body) = &request.body else {
                return DeployRpcResponse::error(
                    "request_invalid",
                    "missing body",
                    Some(op),
                    false,
                );
            };
            let Some(bundle_dir_str) = body.get("bundle_dir").and_then(Value::as_str) else {
                return DeployRpcResponse::error(
                    "request_invalid",
                    "install requires bundle_dir",
                    Some(op),
                    false,
                );
            };
            let Some(expected_commit) = body.get("expected_commit").and_then(Value::as_str) else {
                return DeployRpcResponse::error(
                    "request_invalid",
                    "install requires expected_commit",
                    Some(op),
                    false,
                );
            };
            let activate = body
                .get("activate")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            let bundle_path = PathBuf::from(bundle_dir_str);
            if !bundle_path.is_absolute() {
                return DeployRpcResponse::error(
                    "request_invalid",
                    "bundle_dir must be an absolute path",
                    Some(op),
                    false,
                );
            }
            let idem_meta = match extract_idempotency_meta(
                "install",
                &request,
                &json!({
                    "bundle_dir": bundle_dir_str,
                    "expected_commit": expected_commit,
                    "activate": activate,
                }),
            ) {
                Ok(m) => m,
                Err(err) => return error_from_deploy_error(op, &err),
            };
            match coordinator.start_install(
                bundle_path,
                expected_commit.to_owned(),
                activate,
                idem_meta,
            ) {
                Ok(out) => serialize_ok(op, 202, &out.task, Some(out.replayed)),
                Err(err) => error_from_deploy_error(op, &err),
            }
        }
        "rollback" => {
            let _guard = mutation_lock.lock().unwrap_or_else(PoisonError::into_inner);
            let Some(body) = &request.body else {
                return DeployRpcResponse::error(
                    "request_invalid",
                    "missing body",
                    Some(op),
                    false,
                );
            };
            let Some(expected_previous) = body.get("expected_previous").and_then(Value::as_str)
            else {
                return DeployRpcResponse::error(
                    "request_invalid",
                    "rollback requires expected_previous",
                    Some(op),
                    false,
                );
            };
            let activate = body
                .get("activate")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            let idem_meta = match extract_idempotency_meta(
                "rollback",
                &request,
                &json!({
                    "expected_previous": expected_previous,
                    "activate": activate,
                }),
            ) {
                Ok(m) => m,
                Err(err) => return error_from_deploy_error(op, &err),
            };
            match coordinator.start_rollback(expected_previous.to_owned(), activate, idem_meta) {
                Ok(out) => serialize_ok("rollback", 202, &out.task, Some(out.replayed)),
                Err(err) => error_from_deploy_error(op, &err),
            }
        }
        _ => DeployRpcResponse::error(
            "operation_invalid",
            format!("unsupported operation: {op}"),
            Some(op),
            false,
        ),
    }
}

fn serialize_ok<T: Serialize>(
    op: &str,
    status: u16,
    value: &T,
    replayed: Option<bool>,
) -> DeployRpcResponse {
    match serde_json::to_value(value) {
        Ok(body) => {
            let mut resp = DeployRpcResponse::ok(op, status, body);
            resp.replayed = replayed;
            resp
        }
        Err(err) => DeployRpcResponse::error(
            "deploy_failure",
            format!("failed to serialize response: {err}"),
            Some(op),
            true,
        ),
    }
}

fn error_from_deploy_error(op: &str, err: &DeployError) -> DeployRpcResponse {
    match err {
        DeployError::InvalidBundle { message } => {
            let code = if message.contains("commit_mismatch") {
                "commit_mismatch"
            } else if message.contains("request_invalid") {
                "request_invalid"
            } else {
                "invalid_bundle"
            };
            DeployRpcResponse::error(code, message.clone(), Some(op), false)
        }
        DeployError::Blocked { message } => {
            let code = if message.contains("idempotency_conflict") {
                "idempotency_conflict"
            } else if message.contains("rollback_unavailable") {
                "rollback_unavailable"
            } else if message.contains("maintenance_busy") {
                "maintenance_busy"
            } else {
                "deployment_busy"
            };
            let retryable = !matches!(code, "rollback_unavailable" | "idempotency_conflict");
            DeployRpcResponse::error(code, message.clone(), Some(op), retryable)
        }
        DeployError::Failed { message } => {
            DeployRpcResponse::error("deploy_failure", message.clone(), Some(op), true)
        }
        DeployError::Core(core_err) => {
            DeployRpcResponse::error(core_err.code(), core_err.to_string(), Some(op), true)
        }
    }
}

/// Client for communicating with the privileged `/run/miro-ego/deploy-control.sock` Unix socket.
#[derive(Debug, Clone)]
pub struct DeployRpcClient {
    socket_path: PathBuf,
    timeout: Duration,
    max_line_bytes: usize,
}

impl DeployRpcClient {
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
            timeout: DEFAULT_DEPLOY_CLIENT_TIMEOUT,
            max_line_bytes: MAX_DEPLOY_RPC_LINE_BYTES,
        }
    }

    pub fn canonical() -> Self {
        Self::new(DEFAULT_DEPLOY_SOCKET_PATH)
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn send(&self, request: &DeployRpcRequest) -> DeployResult<DeployRpcResponse> {
        let mut encoded =
            serde_json::to_vec(request).map_err(|err| DeployError::InvalidBundle {
                message: format!("failed to encode deploy RPC request: {err}"),
            })?;
        encoded.push(b'\n');
        if encoded.len() > self.max_line_bytes {
            return Err(DeployError::InvalidBundle {
                message: format!(
                    "request_too_large: {} > {} bytes",
                    encoded.len(),
                    self.max_line_bytes
                ),
            });
        }

        let mut stream =
            UnixStream::connect(&self.socket_path).map_err(|err| DeployError::Failed {
                message: format!(
                    "deploy_controller_unavailable: {}: {err}",
                    self.socket_path.display()
                ),
            })?;
        stream
            .set_write_timeout(Some(self.timeout))
            .and_then(|()| stream.set_read_timeout(Some(self.timeout)))
            .map_err(|err| DeployError::Failed {
                message: format!("failed configuring deploy RPC socket timeout: {err}"),
            })?;

        stream
            .write_all(&encoded)
            .and_then(|()| stream.flush())
            .map_err(|err| DeployError::Failed {
                message: format!("failed writing deploy RPC request: {err}"),
            })?;
        let _ = stream.shutdown(std::net::Shutdown::Write);

        let raw_line = match read_ndjson_line(&mut stream, self.max_line_bytes, self.timeout) {
            Ok(Some(line)) => line,
            Ok(None) => {
                return Err(DeployError::Failed {
                    message: "response_invalid: empty EOF from deploy-control".to_owned(),
                });
            }
            Err(FrameReadError::TooLarge) => {
                return Err(DeployError::Failed {
                    message: "response_too_large: response exceeded max line bytes".to_owned(),
                });
            }
            Err(FrameReadError::Timeout) => {
                return Err(DeployError::Failed {
                    message: "response_timeout: timed out waiting for deploy-control".to_owned(),
                });
            }
            Err(FrameReadError::Invalid(msg)) => {
                return Err(DeployError::Failed {
                    message: format!("response_invalid: {msg}"),
                });
            }
        };

        let resp: DeployRpcResponse =
            serde_json::from_slice(&raw_line).map_err(|err| DeployError::Failed {
                message: format!("response_invalid: malformed JSON: {err}"),
            })?;
        if resp.schema != DEPLOY_RESPONSE_SCHEMA {
            return Err(DeployError::Failed {
                message: format!("response_invalid: unexpected schema {}", resp.schema),
            });
        }
        Ok(resp)
    }

    pub fn health(&self) -> DeployResult<Value> {
        let resp = self.send(&DeployRpcRequest {
            schema: DEPLOY_REQUEST_SCHEMA.to_owned(),
            operation: "health".to_owned(),
            principal_id: None,
            idempotency_key: None,
            request_hash: None,
            body: None,
        })?;
        decode_body(resp)
    }

    pub fn status(&self) -> DeployResult<DeployControlStatus> {
        let resp = self.send(&DeployRpcRequest {
            schema: DEPLOY_REQUEST_SCHEMA.to_owned(),
            operation: "status".to_owned(),
            principal_id: None,
            idempotency_key: None,
            request_hash: None,
            body: None,
        })?;
        decode_body(resp)
    }

    pub fn install(
        &self,
        bundle_dir: &Path,
        expected_commit: &str,
        activate: bool,
    ) -> DeployResult<FirmwareTaskSnapshot> {
        self.install_idempotent(bundle_dir, expected_commit, activate, None, None, None)
            .map(|out| out.task)
    }

    pub fn install_idempotent(
        &self,
        bundle_dir: &Path,
        expected_commit: &str,
        activate: bool,
        principal_id: Option<&str>,
        idempotency_key: Option<&str>,
        request_hash: Option<&str>,
    ) -> DeployResult<DeployMutationOutcome> {
        let resp = self.send(&DeployRpcRequest {
            schema: DEPLOY_REQUEST_SCHEMA.to_owned(),
            operation: "install".to_owned(),
            principal_id: principal_id.map(str::to_owned),
            idempotency_key: idempotency_key.map(str::to_owned),
            request_hash: request_hash.map(str::to_owned),
            body: Some(json!({
                "bundle_dir": bundle_dir.to_string_lossy(),
                "expected_commit": expected_commit,
                "activate": activate,
            })),
        })?;
        let replayed = resp.replayed.unwrap_or(false);
        let task = decode_body(resp)?;
        Ok(DeployMutationOutcome { task, replayed })
    }

    pub fn rollback(
        &self,
        expected_previous: &str,
        activate: bool,
    ) -> DeployResult<FirmwareTaskSnapshot> {
        self.rollback_idempotent(expected_previous, activate, None, None, None)
            .map(|out| out.task)
    }

    pub fn rollback_idempotent(
        &self,
        expected_previous: &str,
        activate: bool,
        principal_id: Option<&str>,
        idempotency_key: Option<&str>,
        request_hash: Option<&str>,
    ) -> DeployResult<DeployMutationOutcome> {
        let resp = self.send(&DeployRpcRequest {
            schema: DEPLOY_REQUEST_SCHEMA.to_owned(),
            operation: "rollback".to_owned(),
            principal_id: principal_id.map(str::to_owned),
            idempotency_key: idempotency_key.map(str::to_owned),
            request_hash: request_hash.map(str::to_owned),
            body: Some(json!({
                "expected_previous": expected_previous,
                "activate": activate,
            })),
        })?;
        let replayed = resp.replayed.unwrap_or(false);
        let task = decode_body(resp)?;
        Ok(DeployMutationOutcome { task, replayed })
    }
}

fn extract_body(resp: DeployRpcResponse) -> DeployResult<Value> {
    if !resp.ok {
        let err = resp.error.unwrap_or(DeployRpcErrorBody {
            code: "deploy_failure".to_owned(),
            message: "unknown deploy RPC error".to_owned(),
        });
        let prefix = format!("{}:", err.code);
        let formatted = if err.message.starts_with(&prefix) {
            err.message
        } else {
            format!("{}: {}", err.code, err.message)
        };
        return match err.code.as_str() {
            "invalid_bundle" | "commit_mismatch" | "request_invalid" | "operation_invalid" => {
                Err(DeployError::InvalidBundle { message: formatted })
            }
            "deployment_busy"
            | "maintenance_busy"
            | "rollback_unavailable"
            | "idempotency_conflict" => Err(DeployError::Blocked { message: formatted }),
            _ => Err(DeployError::Failed { message: formatted }),
        };
    }
    resp.body.ok_or_else(|| DeployError::Failed {
        message: "response_invalid: missing body".to_owned(),
    })
}

fn decode_body<T: serde::de::DeserializeOwned>(resp: DeployRpcResponse) -> DeployResult<T> {
    let body = extract_body(resp)?;
    serde_json::from_value(body).map_err(|err| DeployError::Failed {
        message: format!("response_invalid: failed decoding body: {err}"),
    })
}

enum FrameReadError {
    TooLarge,
    Timeout,
    Invalid(String),
}

fn read_ndjson_line(
    stream: &mut UnixStream,
    max_bytes: usize,
    timeout: Duration,
) -> Result<Option<Vec<u8>>, FrameReadError> {
    let deadline = Instant::now() + timeout;
    let mut buf = Vec::with_capacity(max_bytes.min(4096));
    let mut chunk = [0u8; 4096];

    loop {
        let now = Instant::now();
        let Some(remaining) = deadline.checked_duration_since(now) else {
            return Err(FrameReadError::Timeout);
        };
        if remaining.is_zero() {
            return Err(FrameReadError::Timeout);
        }
        let _ = stream.set_read_timeout(Some(remaining));

        match stream.read(&mut chunk) {
            Ok(0) => {
                if buf.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(buf));
            }
            Ok(n) => {
                let slice = &chunk[..n];
                if let Some(nl_pos) = slice.iter().position(|&b| b == b'\n') {
                    if buf.len().saturating_add(nl_pos) > max_bytes {
                        return Err(FrameReadError::TooLarge);
                    }
                    buf.extend_from_slice(&slice[..nl_pos]);
                    return Ok(Some(buf));
                }
                if buf.len().saturating_add(n) > max_bytes {
                    return Err(FrameReadError::TooLarge);
                }
                buf.extend_from_slice(slice);
            }
            Err(err) if err.kind() == ErrorKind::Interrupted => continue,
            Err(err) if matches!(err.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock) => {
                return Err(FrameReadError::Timeout);
            }
            Err(err) => {
                return Err(FrameReadError::Invalid(format!("socket read error: {err}")));
            }
        }
    }
}

fn write_framed_response(
    mut stream: UnixStream,
    response: &DeployRpcResponse,
    max_bytes: usize,
    timeout: Duration,
) -> DeployResult<()> {
    let _ = stream.set_write_timeout(Some(timeout));
    let mut payload = serde_json::to_vec(response).map_err(|err| DeployError::Failed {
        message: format!("failed serializing deploy RPC response: {err}"),
    })?;
    payload.push(b'\n');
    if payload.len() > max_bytes {
        let fallback = DeployRpcResponse::error(
            "response_too_large",
            "deploy-control response exceeded max line bytes",
            response.operation.as_deref(),
            false,
        );
        payload = serde_json::to_vec(&fallback).map_err(|err| DeployError::Failed {
            message: format!("failed serializing fallback deploy RPC error: {err}"),
        })?;
        payload.push(b'\n');
    }
    stream
        .write_all(&payload)
        .and_then(|()| stream.flush())
        .map_err(|err| DeployError::Failed {
            message: format!("failed writing deploy RPC response: {err}"),
        })?;
    let _ = stream.shutdown(std::net::Shutdown::Write);
    // Drain any unread client bytes in nonblocking mode after half-closing the write side so
    // closing the AF_UNIX socket never triggers ECONNRESET while keeping RPC latency sub-ms.
    if stream.set_nonblocking(true).is_ok() {
        let mut discard = [0u8; 4096];
        let mut drained = 0usize;
        while drained < MAX_DEPLOY_RPC_LINE_BYTES {
            match stream.read(&mut discard) {
                Ok(0) | Err(_) => break,
                Ok(n) => drained = drained.saturating_add(n),
            }
        }
    }
    Ok(())
}

/// Send an `sd_notify`-compatible datagram (e.g. `"READY=1\n"`) to `notify_socket`, supporting
/// both filesystem paths and Linux abstract namespace (`@...`) sockets with zero external crates.
pub fn send_systemd_notify_to(notify_socket: &OsStr, state: &str) -> bool {
    let bytes = notify_socket.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    let Ok(sock) = UnixDatagram::unbound() else {
        return false;
    };
    if let Some(abstract_name) = bytes.strip_prefix(b"@") {
        if abstract_name.is_empty() {
            return false;
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::linux::net::SocketAddrExt;
            if let Ok(addr) = std::os::unix::net::SocketAddr::from_abstract_name(abstract_name) {
                return sock.send_to_addr(state.as_bytes(), &addr).is_ok();
            }
        }
        return false;
    }
    sock.send_to(state.as_bytes(), Path::new(notify_socket))
        .is_ok()
}

/// Send an `sd_notify` datagram over `NOTIFY_SOCKET` when set in the process environment.
pub fn send_systemd_notify(state: &str) -> bool {
    let Some(notify_socket) = std::env::var_os("NOTIFY_SOCKET") else {
        return false;
    };
    send_systemd_notify_to(&notify_socket, state)
}

fn poll_listener_readable(fd: libc::c_int, timeout: Duration) {
    let timeout_ms = i32::try_from(timeout.as_millis().min(1000)).unwrap_or(20);
    let mut pfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: `pfd` is a valid stack `pollfd` referencing an open `UnixListener` fd.
    let _ = unsafe { libc::poll(&raw mut pfd, 1, timeout_ms) };
}

fn unix_now_secs() -> u64 {
    miro_ego_core::sys::clock::realtime_ns()
        .map(|ns| ns / 1_000_000_000)
        .unwrap_or(0)
}

fn monotonic_now_ns() -> u64 {
    miro_ego_core::sys::clock::monotonic_ns().unwrap_or(0)
}

fn truncate_msg(s: &str, max_chars: usize) -> String {
    s.chars().take(max_chars).collect()
}

fn chmod_socket_nofollow(path: &Path, mode: u32) -> DeployResult<()> {
    let meta = fs::symlink_metadata(path).map_err(|err| DeployError::Failed {
        message: format!(
            "failed to stat deploy Unix socket {}: {err}",
            path.display()
        ),
    })?;
    if meta.file_type().is_symlink() {
        return Err(DeployError::Failed {
            message: format!("symlink rejected at deploy Unix socket {}", path.display()),
        });
    }
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).map_err(|err| DeployError::Failed {
        message: format!(
            "failed to chmod deploy Unix socket {}: {err}",
            path.display()
        ),
    })
}

fn best_effort_chgrp_miro_ego(path: &Path) {
    if unsafe { libc::geteuid() } != 0 {
        return;
    }
    let Ok(group_c) = CString::new("miro-ego") else {
        return;
    };
    let Ok(path_c) = CString::new(path.as_os_str().as_bytes()) else {
        return;
    };
    unsafe {
        let grp = libc::getgrnam(group_c.as_ptr());
        if !grp.is_null() {
            let gid = (*grp).gr_gid;
            let _ = libc::fchownat(
                libc::AT_FDCWD,
                path_c.as_ptr(),
                u32::MAX,
                gid,
                libc::AT_SYMLINK_NOFOLLOW,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bundle::test_support::build_valid_bundle;
    use crate::manager::{HealthChecker, ReleaseManagerPaths, SystemctlRunner};
    use std::os::unix::fs::symlink;
    use std::sync::atomic::AtomicBool;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct NoopSystemctl;
    impl SystemctlRunner for NoopSystemctl {
        fn run_systemctl(&self, _args: &[&str]) -> DeployResult<()> {
            Ok(())
        }
        fn unit_load_state(&self, _unit: &str) -> DeployResult<String> {
            Ok("loaded".to_owned())
        }
    }

    struct OkHealth;
    impl HealthChecker for OkHealth {
        fn check_health(&self) -> DeployResult<()> {
            Ok(())
        }
    }

    fn temp_env(tag: &str) -> (PathBuf, ReleaseManagerPaths) {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "miro-ego-deploy-rpc-{tag}-{}-{unique}",
            std::process::id()
        ));
        let paths = ReleaseManagerPaths {
            install_root: root.join("opt/miro-ego"),
            config_root: root.join("etc/miro-ego"),
            state_root: root.join("var/lib/miro-ego"),
            run_dir: root.join("run/miro-ego"),
            system_root: root.clone(),
        };
        fs::create_dir_all(&paths.run_dir).unwrap();
        (root, paths)
    }

    #[test]
    fn live_deploy_rpc_installs_v1_v2_and_rolls_back_asynchronously() {
        let (root, paths) = temp_env("roundtrip");
        let updates_dir = root.join("updates");
        fs::create_dir_all(&updates_dir).unwrap();
        let sock = paths.run_dir.join("deploy-control.sock");
        let mgr = Arc::new(ReleaseManager::new(
            paths.clone(),
            Arc::new(NoopSystemctl),
            Arc::new(OkHealth),
        ));
        let cfg = DeployRpcServerConfig::default().with_allowed_updates_root(&updates_dir);
        let server = DeployRpcServer::bind(&sock, Arc::clone(&mgr), cfg).unwrap();
        assert_eq!(
            fs::symlink_metadata(&sock).unwrap().permissions().mode() & 0o777,
            0o660
        );
        let client = DeployRpcClient::new(&sock);

        let health = client.health().unwrap();
        assert_eq!(health["ok"], true);

        let commit_v1 = "1".repeat(40);
        let commit_v2 = "2".repeat(40);
        let b1 = updates_dir.join("v1");
        let b2 = updates_dir.join("v2");
        build_valid_bundle(
            &b1,
            "0.3.0",
            &commit_v1,
            &[("miro-ego.service", b"[Unit]\nDescription=v1\n", false)],
        );
        build_valid_bundle(
            &b2,
            "0.3.1",
            &commit_v2,
            &[("miro-ego.service", b"[Unit]\nDescription=v2\n", false)],
        );

        // Bundle outside allowed_updates_root OR via symlink MUST be rejected!
        let outside = root.join("outside-v1");
        build_valid_bundle(&outside, "0.3.0", &commit_v1, &[]);
        assert!(matches!(
            client.install(&outside, &commit_v1, true),
            Err(DeployError::InvalidBundle { .. })
        ));
        let sym_inside = updates_dir.join("sym-v1");
        symlink(&outside, &sym_inside).unwrap();
        assert!(matches!(
            client.install(&sym_inside, &commit_v1, true),
            Err(DeployError::InvalidBundle { .. })
        ));

        // Commit mismatch fails upfront with InvalidBundle!
        let bad = client.install(&b1, &commit_v2, true).unwrap_err();
        assert!(matches!(bad, DeployError::InvalidBundle { .. }));

        // Install v1 with idempotency key + replay verification
        let out1 = client
            .install_idempotent(
                &b1,
                &commit_v1,
                true,
                Some("admin"),
                Some("idem-v1"),
                Some("hash-v1"),
            )
            .unwrap();
        assert!(!out1.replayed);
        assert_eq!(out1.task.action, FirmwareTaskAction::Update);
        assert_eq!(out1.task.status, FirmwareTaskStatus::Accepted);
        let s1 = server
            .wait_for_idle(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(s1.status, FirmwareTaskStatus::Succeeded);

        // Replaying same idempotency key + hash returns replayed=true & latest task state;
        // reusing key with different hash fails with idempotency_conflict!
        let replay1 = client
            .install_idempotent(
                &b1,
                &commit_v1,
                true,
                Some("admin"),
                Some("idem-v1"),
                Some("hash-v1"),
            )
            .unwrap();
        assert!(replay1.replayed);
        assert_eq!(replay1.task.id, out1.task.id);
        assert_eq!(replay1.task.status, FirmwareTaskStatus::Succeeded);

        let conflict = client
            .install_idempotent(
                &b2,
                &commit_v2,
                true,
                Some("admin"),
                Some("idem-v1"),
                Some("hash-different"),
            )
            .unwrap_err();
        assert!(conflict.to_string().contains("idempotency_conflict"));

        // Verify firmware-task.json persisted atomically under state_root!
        assert!(paths.firmware_task_file().is_file());

        // Install v2
        let _ = client.install(&b2, &commit_v2, true).unwrap();
        let s2 = server
            .wait_for_idle(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(s2.status, FirmwareTaskStatus::Succeeded);

        let st = client.status().unwrap();
        assert_eq!(st.deployment.current.as_deref(), Some(commit_v2.as_str()));
        assert_eq!(st.deployment.current_version.as_deref(), Some("0.3.1"));
        assert_eq!(st.deployment.previous.as_deref(), Some(commit_v1.as_str()));
        assert_eq!(st.deployment.previous_version.as_deref(), Some("0.3.0"));

        // Rollback v2 -> v1
        let rb = client.rollback(&commit_v1, true).unwrap();
        assert_eq!(rb.action, FirmwareTaskAction::Rollback);
        assert_eq!(rb.status, FirmwareTaskStatus::Accepted);
        let s_rb = server
            .wait_for_idle(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(s_rb.status, FirmwareTaskStatus::Succeeded);

        let after_rb = client.status().unwrap();
        assert_eq!(
            after_rb.deployment.current.as_deref(),
            Some(commit_v1.as_str())
        );
        assert_eq!(
            after_rb.deployment.current_version.as_deref(),
            Some("0.3.0")
        );
        assert_eq!(
            after_rb.deployment.previous.as_deref(),
            Some(commit_v2.as_str())
        );
        assert_eq!(
            after_rb.deployment.previous_version.as_deref(),
            Some("0.3.1")
        );

        server.shutdown();

        // Restart server on same state_root: persisted task & idempotency survive restart!
        let server2 = DeployRpcServer::bind(
            &sock,
            Arc::clone(&mgr),
            DeployRpcServerConfig::default()
                .with_allowed_updates_root(&updates_dir)
                .with_idle_timeout(Some(Duration::from_millis(80))),
        )
        .unwrap();
        let st_reloaded = client.status().unwrap();
        assert_eq!(
            st_reloaded.task.as_ref().map(|t| t.status),
            Some(FirmwareTaskStatus::Succeeded)
        );
        let replay_after_restart = client
            .install_idempotent(
                &b1,
                &commit_v1,
                true,
                Some("admin"),
                Some("idem-v1"),
                Some("hash-v1"),
            )
            .unwrap();
        assert!(replay_after_restart.replayed);

        // Verify wait_for_idle_or_exit exits cleanly after idle_timeout EVEN WHILE status/health
        // RPCs are continuously polled!
        let server3_sock = paths.run_dir.join("idle-poll.sock");
        let server3 = DeployRpcServer::bind(
            &server3_sock,
            Arc::clone(&mgr),
            DeployRpcServerConfig::default()
                .with_allowed_updates_root(&updates_dir)
                .with_idle_timeout(Some(Duration::from_millis(120))),
        )
        .unwrap();
        let poll_client = DeployRpcClient::new(&server3_sock);
        let start_poll = Instant::now();
        while !server3.has_exited_idle() && start_poll.elapsed() < Duration::from_secs(2) {
            let _ = poll_client.status();
            let _ = poll_client.health();
            thread::sleep(Duration::from_millis(20));
        }
        assert!(
            server3.has_exited_idle(),
            "read-only status/health polling must NOT prevent idle auto-exit"
        );
        server3.shutdown();

        // Verify sd_notify READY=1\n over UnixDatagram + single-pass background SHA-256 rejection:
        let notify_sock_path = root.join("notify.sock");
        let notify_rx = UnixDatagram::bind(&notify_sock_path).unwrap();
        notify_rx
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        assert!(send_systemd_notify_to(
            notify_sock_path.as_os_str(),
            "READY=1\n"
        ));
        let mut nbuf = [0u8; 32];
        let n_ready = notify_rx.recv(&mut nbuf).unwrap();
        assert_eq!(&nbuf[..n_ready], b"READY=1\n");

        // Corrupt a single payload byte inside an otherwise size-valid, ELF-valid signed bundle:
        // fast preflight returns 202 Accepted immediately, and background single-pass staging
        // catches the SHA-256 mismatch and marks the task Failed without touching current!
        let commit_bad_sha = "3".repeat(40);
        let b_bad = updates_dir.join("v3-bad-sha");
        build_valid_bundle(&b_bad, "0.3.2", &commit_bad_sha, &[]);
        let mut bin_bytes = fs::read(b_bad.join("bin/miro-ego-encoder")).unwrap();
        bin_bytes[90] ^= 0x55;
        fs::write(b_bad.join("bin/miro-ego-encoder"), &bin_bytes).unwrap();

        let server4_sock = paths.run_dir.join("single-pass.sock");
        let server4 = DeployRpcServer::bind(
            &server4_sock,
            Arc::clone(&mgr),
            DeployRpcServerConfig::default().with_allowed_updates_root(&updates_dir),
        )
        .unwrap();
        let client4 = DeployRpcClient::new(&server4_sock);
        let accepted_bad = client4.install(&b_bad, &commit_bad_sha, true).unwrap();
        assert_eq!(accepted_bad.status, FirmwareTaskStatus::Accepted);
        let settled_bad = server4
            .wait_for_idle(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(settled_bad.status, FirmwareTaskStatus::Failed);
        assert!(settled_bad.message.contains("SHA-256 mismatch"));
        assert_eq!(
            client4.status().unwrap().deployment.current.as_deref(),
            Some(commit_v1.as_str())
        );
        server4.shutdown();

        // Verify wait_for_idle_or_exit on server2:
        let stop_flag = AtomicBool::new(false);
        server2
            .wait_for_idle_or_exit(Duration::from_millis(80), &stop_flag)
            .unwrap();
        assert!(server2.has_exited_idle());
        server2.shutdown();

        fs::remove_dir_all(root).unwrap();
    }
}
