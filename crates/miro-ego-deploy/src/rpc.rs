//! Framed NDJSON Unix-domain-socket RPC server & client (`/run/miro-ego/deploy-control.sock`).
//!
//! Enables the unprivileged `miro-ego.service` API daemon (`User=miro-ego`, `NoNewPrivileges=yes`)
//! to trigger real privileged root `ReleaseManager` A/B updates and rollbacks while tracking
//! `FirmwareTaskSnapshot` progress (`accepted` -> `running` -> `succeeded` / `failed`).

use crate::bundle::{validate_commit_sha, verify_bundle_dir};
use crate::manager::{DeploymentStatus, ReleaseManager};
use crate::{DeployError, DeployResult};
use miro_ego_core::ids::new_uuid_v4;
use miro_ego_core::locks::{LockMode, acquire_flock, acquire_maintenance_lock};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::ffi::CString;
use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub const DEFAULT_DEPLOY_SOCKET_PATH: &str = "/run/miro-ego/deploy-control.sock";
pub const DEPLOY_REQUEST_SCHEMA: &str = "miro-ego.deploy-control-request.v1";
pub const DEPLOY_RESPONSE_SCHEMA: &str = "miro-ego.deploy-control-response.v1";
pub const MAX_DEPLOY_RPC_LINE_BYTES: usize = 64 * 1024;
pub const DEFAULT_DEPLOY_SERVER_READ_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_DEPLOY_SERVER_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_DEPLOY_CLIENT_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_DEPLOY_MAX_CONNS: usize = 8;

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
}

impl Default for DeployRpcServerConfig {
    fn default() -> Self {
        Self {
            max_line_bytes: MAX_DEPLOY_RPC_LINE_BYTES,
            read_timeout: DEFAULT_DEPLOY_SERVER_READ_TIMEOUT,
            write_timeout: DEFAULT_DEPLOY_SERVER_WRITE_TIMEOUT,
            max_concurrent_connections: DEFAULT_DEPLOY_MAX_CONNS,
        }
    }
}

struct TaskState {
    active: bool,
    response_flushed: bool,
    latest_task: Option<FirmwareTaskSnapshot>,
    worker_handle: Option<JoinHandle<()>>,
}

struct DeployTaskCoordinator {
    manager: Arc<ReleaseManager>,
    state: Mutex<TaskState>,
    idle_cv: Condvar,
    flushed_cv: Condvar,
}

impl DeployTaskCoordinator {
    fn new(manager: Arc<ReleaseManager>) -> Arc<Self> {
        Arc::new(Self {
            manager,
            state: Mutex::new(TaskState {
                active: false,
                response_flushed: false,
                latest_task: None,
                worker_handle: None,
            }),
            idle_cv: Condvar::new(),
            flushed_cv: Condvar::new(),
        })
    }

    fn status(&self) -> DeployResult<DeployControlStatus> {
        let deployment = self.manager.status()?;
        let task = self
            .state
            .lock()
            .map_err(|_| DeployError::Failed {
                message: "deploy task mutex poisoned".to_owned(),
            })?
            .latest_task
            .clone();
        Ok(DeployControlStatus { deployment, task })
    }

    fn notify_response_flushed(&self) {
        if let Ok(mut guard) = self.state.lock() {
            guard.response_flushed = true;
            self.flushed_cv.notify_all();
        }
    }

    fn wait_for_idle(&self, timeout: Duration) -> DeployResult<Option<FirmwareTaskSnapshot>> {
        self.notify_response_flushed();
        let deadline = Instant::now() + timeout;
        let mut join_target = None;
        let snapshot = {
            let mut guard = self.state.lock().map_err(|_| DeployError::Failed {
                message: "deploy task mutex poisoned".to_owned(),
            })?;
            while guard.active {
                let now = Instant::now();
                if now >= deadline {
                    return Err(DeployError::Blocked {
                        message: "deployment_busy: timed out waiting for firmware task".to_owned(),
                    });
                }
                let wait_dur = deadline.saturating_duration_since(now);
                let (next, _) = self.idle_cv.wait_timeout(guard, wait_dur).map_err(|_| {
                    DeployError::Failed {
                        message: "deploy task condvar poisoned".to_owned(),
                    }
                })?;
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

    fn probe_locks_available(&self) -> DeployResult<()> {
        fs::create_dir_all(&self.manager.paths().run_dir).map_err(|err| DeployError::Failed {
            message: format!(
                "failed creating run dir {}: {err}",
                self.manager.paths().run_dir.display()
            ),
        })?;
        let _deploy = acquire_flock(
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
        let _maint =
            acquire_maintenance_lock(&self.manager.paths().run_dir, LockMode::Exclusive, true)
                .map_err(|err| match err {
                    miro_ego_core::CoreError::LockBusy { message } => DeployError::Blocked {
                        message: format!("maintenance_busy: {message}"),
                    },
                    other => DeployError::Core(other),
                })?;
        Ok(())
    }

    fn start_install(
        self: &Arc<Self>,
        bundle_dir: PathBuf,
        expected_commit: String,
        activate: bool,
    ) -> DeployResult<FirmwareTaskSnapshot> {
        validate_commit_sha(&expected_commit)?;
        {
            let guard = self.state.lock().map_err(|_| DeployError::Failed {
                message: "deploy task mutex poisoned".to_owned(),
            })?;
            if guard.active {
                return Err(DeployError::Blocked {
                    message: "deployment_busy: another firmware task is already active".to_owned(),
                });
            }
        }
        self.probe_locks_available()?;

        let verified = verify_bundle_dir(&bundle_dir)?;
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

        self.spawn_worker(task, move |mgr| mgr.install(&bundle_dir, activate))
    }

    fn start_rollback(
        self: &Arc<Self>,
        expected_previous: String,
        activate: bool,
    ) -> DeployResult<FirmwareTaskSnapshot> {
        validate_commit_sha(&expected_previous)?;
        {
            let guard = self.state.lock().map_err(|_| DeployError::Failed {
                message: "deploy task mutex poisoned".to_owned(),
            })?;
            if guard.active {
                return Err(DeployError::Blocked {
                    message: "deployment_busy: another firmware task is already active".to_owned(),
                });
            }
        }
        self.probe_locks_available()?;

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

        self.spawn_worker(task, move |mgr| mgr.rollback(activate))
    }

    fn spawn_worker<F>(
        self: &Arc<Self>,
        initial_task: FirmwareTaskSnapshot,
        op: F,
    ) -> DeployResult<FirmwareTaskSnapshot>
    where
        F: FnOnce(&ReleaseManager) -> DeployResult<DeploymentStatus> + Send + 'static,
    {
        let prev_handle = {
            let mut guard = self.state.lock().map_err(|_| DeployError::Failed {
                message: "deploy task mutex poisoned".to_owned(),
            })?;
            if guard.active {
                return Err(DeployError::Blocked {
                    message: "deployment_busy: another firmware task is already active".to_owned(),
                });
            }
            guard.active = true;
            guard.response_flushed = false;
            guard.latest_task = Some(initial_task.clone());
            guard.worker_handle.take()
        };
        if let Some(h) = prev_handle {
            let _ = h.join();
        }

        let coord = Arc::clone(self);
        let task_id = initial_task.id.clone();
        let spawn_res = thread::Builder::new()
            .name("miro-ego-deploy-tx".to_owned())
            .spawn(move || {
                if let Ok(guard) = coord.state.lock() {
                    if !guard.response_flushed {
                        let _ = coord
                            .flushed_cv
                            .wait_timeout(guard, Duration::from_millis(30));
                    }
                }
                if let Ok(mut guard) = coord.state.lock() {
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
                }

                let outcome = op(&coord.manager);
                let finished_at = unix_now_secs();

                if let Ok(mut guard) = coord.state.lock() {
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
                    guard.active = false;
                    coord.idle_cv.notify_all();
                }
            });

        match spawn_res {
            Ok(handle) => {
                if let Ok(mut guard) = self.state.lock() {
                    guard.worker_handle = Some(handle);
                }
                Ok(initial_task)
            }
            Err(err) => {
                if let Ok(mut guard) = self.state.lock() {
                    guard.active = false;
                    if let Some(cur) = guard.latest_task.as_mut() {
                        cur.status = FirmwareTaskStatus::Failed;
                        cur.message = format!("failed spawning deploy worker: {err}");
                        cur.finished_at = Some(unix_now_secs());
                    }
                    self.idle_cv.notify_all();
                }
                Err(DeployError::Failed {
                    message: format!("failed to spawn deploy worker thread: {err}"),
                })
            }
        }
    }
}

/// Bounded framed-JSON Unix domain socket server exposing privileged [`ReleaseManager`] operations.
pub struct DeployRpcServer {
    socket_path: Option<PathBuf>,
    stop: Arc<AtomicBool>,
    accept_thread: Option<JoinHandle<()>>,
    active_workers: Arc<AtomicUsize>,
    coordinator: Arc<DeployTaskCoordinator>,
}

impl DeployRpcServer {
    /// Bind a new Unix domain socket at `socket_path` (mode `0660`, group `miro-ego` when root)
    /// and start serving privileged deployment requests.
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
            }
        }
        if fs::symlink_metadata(&path).is_ok() {
            let _ = fs::remove_file(&path);
        }
        let listener = UnixListener::bind(&path).map_err(|err| DeployError::Failed {
            message: format!(
                "failed to bind deploy Unix socket {}: {err}",
                path.display()
            ),
        })?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o660)).map_err(|err| {
            DeployError::Failed {
                message: format!(
                    "failed to chmod deploy Unix socket {}: {err}",
                    path.display()
                ),
            }
        })?;
        best_effort_chgrp_miro_ego(&path);
        Self::from_listener(listener, Some(path), manager, config)
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
        let active_workers = Arc::new(AtomicUsize::new(0));
        let coordinator = DeployTaskCoordinator::new(manager);
        let mutation_lock = Arc::new(Mutex::new(()));

        let stop_loop = Arc::clone(&stop);
        let workers_loop = Arc::clone(&active_workers);
        let coord_loop = Arc::clone(&coordinator);
        let accept_thread = thread::Builder::new()
            .name("miro-ego-deploy-rpc".to_owned())
            .spawn(move || {
                let mut handles: Vec<JoinHandle<()>> = Vec::new();
                while !stop_loop.load(Ordering::Acquire) {
                    handles.retain(|h| !h.is_finished());
                    match listener.accept() {
                        Ok((stream, _)) => {
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
                                continue;
                            }
                            workers_loop.fetch_add(1, Ordering::AcqRel);
                            let coord = Arc::clone(&coord_loop);
                            let mlock = Arc::clone(&mutation_lock);
                            let cfg = config.clone();
                            let counter = Arc::clone(&workers_loop);
                            if let Ok(handle) = thread::Builder::new()
                                .name("miro-ego-deploy-conn".to_owned())
                                .spawn(move || {
                                    handle_connection(stream, &coord, &mlock, &cfg);
                                    counter.fetch_sub(1, Ordering::AcqRel);
                                })
                            {
                                handles.push(handle);
                            } else {
                                workers_loop.fetch_sub(1, Ordering::AcqRel);
                            }
                        }
                        Err(err) if err.kind() == ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(_) => {
                            if stop_loop.load(Ordering::Acquire) {
                                break;
                            }
                            thread::sleep(Duration::from_millis(20));
                        }
                    }
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
            accept_thread: Some(accept_thread),
            active_workers,
            coordinator,
        })
    }

    pub fn active_workers(&self) -> usize {
        self.active_workers.load(Ordering::Acquire)
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
            .ok()
            .and_then(|mut g| g.worker_handle.take());
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
    if response.ok && matches!(response.operation.as_deref(), Some("install" | "rollback")) {
        coordinator.notify_response_flushed();
    }
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
                "operations": ["status", "install", "rollback"],
            }),
        ),
        "status" => match coordinator.status() {
            Ok(st) => serialize_ok("status", 200, &st),
            Err(err) => error_from_deploy_error(op, &err),
        },
        "install" => {
            let Ok(_guard) = mutation_lock.lock() else {
                return DeployRpcResponse::error(
                    "deploy_failure",
                    "mutation lock poisoned",
                    Some(op),
                    true,
                );
            };
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
            match coordinator.start_install(bundle_path, expected_commit.to_owned(), activate) {
                Ok(task) => serialize_ok("install", 202, &task),
                Err(err) => error_from_deploy_error(op, &err),
            }
        }
        "rollback" => {
            let Ok(_guard) = mutation_lock.lock() else {
                return DeployRpcResponse::error(
                    "deploy_failure",
                    "mutation lock poisoned",
                    Some(op),
                    true,
                );
            };
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
            match coordinator.start_rollback(expected_previous.to_owned(), activate) {
                Ok(task) => serialize_ok("rollback", 202, &task),
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

fn serialize_ok<T: Serialize>(op: &str, status: u16, value: &T) -> DeployRpcResponse {
    match serde_json::to_value(value) {
        Ok(body) => DeployRpcResponse::ok(op, status, body),
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
            } else {
                "invalid_bundle"
            };
            DeployRpcResponse::error(code, message.clone(), Some(op), false)
        }
        DeployError::Blocked { message } => {
            let code = if message.contains("rollback_unavailable") {
                "rollback_unavailable"
            } else if message.contains("maintenance_busy") {
                "maintenance_busy"
            } else {
                "deployment_busy"
            };
            let retryable = code != "rollback_unavailable";
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
            body: None,
        })?;
        decode_body(resp)
    }

    pub fn status(&self) -> DeployResult<DeployControlStatus> {
        let resp = self.send(&DeployRpcRequest {
            schema: DEPLOY_REQUEST_SCHEMA.to_owned(),
            operation: "status".to_owned(),
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
        let resp = self.send(&DeployRpcRequest {
            schema: DEPLOY_REQUEST_SCHEMA.to_owned(),
            operation: "install".to_owned(),
            body: Some(json!({
                "bundle_dir": bundle_dir.to_string_lossy(),
                "expected_commit": expected_commit,
                "activate": activate,
            })),
        })?;
        decode_body(resp)
    }

    pub fn rollback(
        &self,
        expected_previous: &str,
        activate: bool,
    ) -> DeployResult<FirmwareTaskSnapshot> {
        let resp = self.send(&DeployRpcRequest {
            schema: DEPLOY_REQUEST_SCHEMA.to_owned(),
            operation: "rollback".to_owned(),
            body: Some(json!({
                "expected_previous": expected_previous,
                "activate": activate,
            })),
        })?;
        decode_body(resp)
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
            "deployment_busy" | "maintenance_busy" | "rollback_unavailable" => {
                Err(DeployError::Blocked { message: formatted })
            }
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

fn read_ndjson_line<R: Read>(
    reader: &mut R,
    max_bytes: usize,
    timeout: Duration,
) -> Result<Option<Vec<u8>>, FrameReadError> {
    let deadline = Instant::now() + timeout;
    let mut buf = Vec::with_capacity(max_bytes.min(4096));
    let mut byte = [0u8; 1];

    loop {
        if Instant::now() > deadline {
            return Err(FrameReadError::Timeout);
        }
        match reader.read(&mut byte) {
            Ok(0) => {
                if buf.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(buf));
            }
            Ok(_) => {
                if byte[0] == b'\n' {
                    return Ok(Some(buf));
                }
                if buf.len() >= max_bytes {
                    return Err(FrameReadError::TooLarge);
                }
                buf.push(byte[0]);
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

fn unix_now_secs() -> u64 {
    miro_ego_core::sys::clock::realtime_ns()
        .map(|ns| ns / 1_000_000_000)
        .unwrap_or(0)
}

fn truncate_msg(s: &str, max_chars: usize) -> String {
    s.chars().take(max_chars).collect()
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
            let _ = libc::chown(path_c.as_ptr(), u32::MAX, gid);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bundle::test_support::build_valid_bundle;
    use crate::manager::{HealthChecker, ReleaseManagerPaths, SystemctlRunner};
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
        let sock = paths.run_dir.join("deploy-control.sock");
        let mgr = Arc::new(ReleaseManager::new(
            paths.clone(),
            Arc::new(NoopSystemctl),
            Arc::new(OkHealth),
        ));
        let server =
            DeployRpcServer::bind(&sock, Arc::clone(&mgr), DeployRpcServerConfig::default())
                .unwrap();
        let client = DeployRpcClient::new(&sock);

        let health = client.health().unwrap();
        assert_eq!(health["ok"], true);

        let commit_v1 = "1".repeat(40);
        let commit_v2 = "2".repeat(40);
        let b1 = root.join("updates/v1");
        let b2 = root.join("updates/v2");
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

        // Commit mismatch fails upfront with InvalidBundle!
        let bad = client.install(&b1, &commit_v2, true).unwrap_err();
        assert!(matches!(bad, DeployError::InvalidBundle { .. }));

        // Install v1
        let t1 = client.install(&b1, &commit_v1, true).unwrap();
        assert_eq!(t1.action, FirmwareTaskAction::Update);
        assert_eq!(t1.status, FirmwareTaskStatus::Accepted);
        let s1 = server
            .wait_for_idle(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(s1.status, FirmwareTaskStatus::Succeeded);

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

        let _ = AtomicBool::new(false);
        server.shutdown();
        fs::remove_dir_all(root).unwrap();
    }
}
