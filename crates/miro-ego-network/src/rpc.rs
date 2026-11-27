//! Framed NDJSON Unix-domain-socket RPC server & client (`/run/miro-ego/network-control.sock`).
//!
//! Enforces strict bounded line sizes (`MAX_RPC_LINE_BYTES`), deadline-aware buffered read/write
//! timeouts, concurrent read / serialized mutation dispatch, privileged `clock.status` /
//! `clock.sync` & `notify_response_flushed` methods, secret-field redaction, and graceful shutdown.

use crate::clock::ClockStatusSnapshot;
use crate::controller::{
    ApplyDesiredState, AuthoritativeNetworkController, ControllerStatus, CredentialReceipt,
    MutationOutcome, ScanResponse, WatchdogMode,
};
use crate::{NetworkError, NetworkResult};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::ffi::CString;
use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub const DEFAULT_CONTROL_SOCKET_PATH: &str = "/run/miro-ego/network-control.sock";
pub const CONTROL_REQUEST_SCHEMA: &str = "miro-ego.network-control-request.v1";
pub const CONTROL_RESPONSE_SCHEMA: &str = "miro-ego.network-control-response.v1";
pub const MAX_RPC_LINE_BYTES: usize = 64 * 1024;
pub const DEFAULT_SERVER_READ_TIMEOUT: Duration = Duration::from_secs(2);
pub const DEFAULT_SERVER_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
pub const DEFAULT_CLIENT_TIMEOUT: Duration = Duration::from_secs(45);
pub const DEFAULT_MAX_CONCURRENT_CONNS: usize = 8;

static NET_UMASK_LOCK: Mutex<()> = Mutex::new(());

const MUTATION_INLINE_SECRET_KEYS: &[&str] = &["password", "passphrase", "psk", "secret", "token"];
const RESPONSE_FORBIDDEN_SECRET_KEYS: &[&str] = &[
    "password",
    "passphrase",
    "psk",
    "secret",
    "token",
    "credential",
];

/// Wire request envelope sent over `/run/miro-ego/network-control.sock`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcRequest {
    pub schema: String,
    pub operation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<Value>,
}

/// Wire error body returned inside failed RPC responses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcErrorBody {
    pub code: String,
    pub message: String,
}

/// Wire response envelope returned by `miro-ego network-control`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcResponse {
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
    pub error: Option<RpcErrorBody>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retryable: Option<bool>,
}

impl RpcResponse {
    pub fn ok(operation: &str, status: u16, body: Value) -> Self {
        Self {
            schema: CONTROL_RESPONSE_SCHEMA.to_owned(),
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
            schema: CONTROL_RESPONSE_SCHEMA.to_owned(),
            ok: false,
            operation: operation.map(str::to_owned),
            status: None,
            body: None,
            replayed: None,
            error: Some(RpcErrorBody {
                code: code.into(),
                message: message.into(),
            }),
            retryable: Some(retryable),
        }
    }
}

/// Configuration for [`NetworkRpcServer`].
#[derive(Debug, Clone)]
pub struct RpcServerConfig {
    pub max_line_bytes: usize,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub max_concurrent_connections: usize,
}

impl Default for RpcServerConfig {
    fn default() -> Self {
        Self {
            max_line_bytes: MAX_RPC_LINE_BYTES,
            read_timeout: DEFAULT_SERVER_READ_TIMEOUT,
            write_timeout: DEFAULT_SERVER_WRITE_TIMEOUT,
            max_concurrent_connections: DEFAULT_MAX_CONCURRENT_CONNS,
        }
    }
}

struct ActiveRpcConnDropGuard {
    counter: Arc<AtomicUsize>,
}

impl Drop for ActiveRpcConnDropGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Dedicated scan worker isolation gate ensuring slow `nmcli device wifi list --rescan auto`
/// scans cannot starve fast-path `status`, `health`, `watchdog_mode`, or `clock` RPCs.
struct ScanRpcGate {
    active_scans: AtomicUsize,
    max_scan_slots: usize,
}

impl ScanRpcGate {
    fn new(max_concurrent_connections: usize) -> Arc<Self> {
        // Reserve at least half of connection worker slots for fast-path RPCs whenever possible.
        let max_scan_slots = if max_concurrent_connections <= 1 {
            1
        } else {
            (max_concurrent_connections / 2).max(1)
        };
        Arc::new(Self {
            active_scans: AtomicUsize::new(0),
            max_scan_slots,
        })
    }

    fn execute_scan(
        &self,
        controller: &AuthoritativeNetworkController,
    ) -> NetworkResult<ScanResponse> {
        let prev = self.active_scans.fetch_add(1, Ordering::AcqRel);
        if prev >= self.max_scan_slots {
            self.active_scans.fetch_sub(1, Ordering::AcqRel);
            return Err(NetworkError::Busy {
                message: "controller_busy: Wi-Fi scan worker slots are saturated".to_owned(),
            });
        }
        struct ScanSlotGuard<'a>(&'a AtomicUsize);
        impl Drop for ScanSlotGuard<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::AcqRel);
            }
        }
        let _active_guard = ScanSlotGuard(&self.active_scans);
        // `NmcliBackend::scan_wifi` coalesces concurrent scan callers via `ScanFlight`.
        controller.scan()
    }
}

/// Bounded framed-JSON Unix domain socket server exposing [`AuthoritativeNetworkController`].
pub struct NetworkRpcServer {
    socket_path: Option<PathBuf>,
    stop: Arc<AtomicBool>,
    accept_thread: Option<JoinHandle<()>>,
    active_workers: Arc<AtomicUsize>,
}

impl NetworkRpcServer {
    /// Bind a new Unix domain socket at `socket_path` (mode `0660`, group `miro-ego` when root)
    /// under a restrictive `umask` (`0o077`) with `AT_SYMLINK_NOFOLLOW` protection and start serving requests.
    pub fn bind(
        socket_path: impl Into<PathBuf>,
        controller: Arc<AuthoritativeNetworkController>,
        config: RpcServerConfig,
    ) -> NetworkResult<Self> {
        let path = socket_path.into();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|err| NetworkError::Backend {
                    message: format!("failed to create socket dir {}: {err}", parent.display()),
                })?;
                if let Ok(pmeta) = fs::symlink_metadata(parent) {
                    if pmeta.file_type().is_symlink() || !pmeta.file_type().is_dir() {
                        return Err(NetworkError::Backend {
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
                return Err(NetworkError::Backend {
                    message: format!(
                        "refusing to unlink directory at socket path {}",
                        path.display()
                    ),
                });
            }
            let _ = fs::remove_file(&path);
        }

        let listener = {
            let _umask_guard = NET_UMASK_LOCK
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let prev_mask = unsafe { libc::umask(0o077) };
            let bind_res = UnixListener::bind(&path);
            unsafe {
                libc::umask(prev_mask);
            }
            bind_res.map_err(|err| NetworkError::Backend {
                message: format!("failed to bind Unix socket {}: {err}", path.display()),
            })?
        };

        let setup_res = (|| -> NetworkResult<()> {
            let meta = fs::symlink_metadata(&path).map_err(|err| NetworkError::Backend {
                message: format!("failed to stat Unix socket {}: {err}", path.display()),
            })?;
            if meta.file_type().is_symlink() || !meta.file_type().is_socket() {
                return Err(NetworkError::Backend {
                    message: format!("bound path {} is not a Unix socket", path.display()),
                });
            }
            best_effort_chgrp_miro_ego_nofollow(&path);
            fs::set_permissions(&path, fs::Permissions::from_mode(0o660)).map_err(|err| {
                NetworkError::Backend {
                    message: format!("failed to chmod Unix socket {}: {err}", path.display()),
                }
            })?;
            Ok(())
        })();

        if let Err(err) = setup_res {
            drop(listener);
            let _ = fs::remove_file(&path);
            return Err(err);
        }

        match Self::from_listener(listener, Some(path.clone()), controller, config) {
            Ok(srv) => Ok(srv),
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
        controller: Arc<AuthoritativeNetworkController>,
        config: RpcServerConfig,
    ) -> NetworkResult<Self> {
        listener
            .set_nonblocking(true)
            .map_err(|err| NetworkError::Backend {
                message: format!("failed to configure nonblocking listener: {err}"),
            })?;

        let stop = Arc::new(AtomicBool::new(false));
        let active_workers = Arc::new(AtomicUsize::new(0));
        let mutation_lock = Arc::new(Mutex::new(()));
        let scan_gate = ScanRpcGate::new(config.max_concurrent_connections);

        let stop_loop = Arc::clone(&stop);
        let workers_loop = Arc::clone(&active_workers);
        let accept_thread = thread::Builder::new()
            .name("miro-ego-net-rpc".to_owned())
            .spawn(move || {
                let mut handles: Vec<JoinHandle<()>> = Vec::new();
                while !stop_loop.load(Ordering::Acquire) {
                    handles.retain(|h| !h.is_finished());
                    match listener.accept() {
                        Ok((stream, _addr)) => {
                            let current = workers_loop.load(Ordering::Acquire);
                            if current >= config.max_concurrent_connections {
                                let busy = RpcResponse::error(
                                    "controller_busy",
                                    "network-control server is busy",
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
                            let conn_guard = ActiveRpcConnDropGuard {
                                counter: Arc::clone(&workers_loop),
                            };
                            let ctrl = Arc::clone(&controller);
                            let mlock = Arc::clone(&mutation_lock);
                            let sgate = Arc::clone(&scan_gate);
                            let cfg = config.clone();
                            if let Ok(handle) = thread::Builder::new()
                                .name("miro-ego-net-conn".to_owned())
                                .spawn(move || {
                                    let _guard = conn_guard;
                                    handle_connection(stream, &ctrl, &mlock, Some(&sgate), &cfg);
                                })
                            {
                                handles.push(handle);
                            }
                        }
                        Err(err) if err.kind() == ErrorKind::WouldBlock => {
                            poll_listener_readable(listener.as_raw_fd(), Duration::from_millis(20));
                        }
                        Err(_) => {
                            if stop_loop.load(Ordering::Acquire) {
                                break;
                            }
                            poll_listener_readable(listener.as_raw_fd(), Duration::from_millis(20));
                        }
                    }
                }
                for handle in handles {
                    let _ = handle.join();
                }
            })
            .map_err(|err| NetworkError::Backend {
                message: format!("failed to spawn RPC accept thread: {err}"),
            })?;

        Ok(Self {
            socket_path,
            stop,
            accept_thread: Some(accept_thread),
            active_workers,
        })
    }

    pub fn active_workers(&self) -> usize {
        self.active_workers.load(Ordering::Acquire)
    }

    /// Gracefully stop accepting new connections and drain in-flight requests.
    pub fn shutdown(mut self) {
        self.shutdown_inner();
    }

    fn shutdown_inner(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.accept_thread.take() {
            let _ = handle.join();
        }
        if let Some(path) = self.socket_path.take() {
            let _ = fs::remove_file(path);
        }
    }
}

impl Drop for NetworkRpcServer {
    fn drop(&mut self) {
        self.shutdown_inner();
    }
}

fn lookup_miro_ego_uid_r() -> Option<u32> {
    let name_c = CString::new("miro-ego").ok()?;
    let init_len = match unsafe { libc::sysconf(libc::_SC_GETPW_R_SIZE_MAX) } {
        n if n > 0 => (n as usize).clamp(1024, 65_536),
        _ => 4096,
    };
    let mut buf = vec![0u8; init_len];
    loop {
        let mut pwd = std::mem::MaybeUninit::<libc::passwd>::uninit();
        let mut result: *mut libc::passwd = std::ptr::null_mut();
        let rc = unsafe {
            libc::getpwnam_r(
                name_c.as_ptr(),
                pwd.as_mut_ptr(),
                buf.as_mut_ptr().cast::<libc::c_char>(),
                buf.len(),
                &raw mut result,
            )
        };
        if rc == libc::ERANGE && buf.len() < 1024 * 1024 {
            buf.resize(buf.len() * 2, 0);
            continue;
        }
        if rc == 0 && !result.is_null() {
            let pwd = unsafe { pwd.assume_init() };
            return Some(pwd.pw_uid);
        }
        return None;
    }
}

fn lookup_miro_ego_gid_r() -> Option<u32> {
    let name_c = CString::new("miro-ego").ok()?;
    let init_len = match unsafe { libc::sysconf(libc::_SC_GETGR_R_SIZE_MAX) } {
        n if n > 0 => (n as usize).clamp(1024, 65_536),
        _ => 4096,
    };
    let mut buf = vec![0u8; init_len];
    loop {
        let mut grp = std::mem::MaybeUninit::<libc::group>::uninit();
        let mut result: *mut libc::group = std::ptr::null_mut();
        let rc = unsafe {
            libc::getgrnam_r(
                name_c.as_ptr(),
                grp.as_mut_ptr(),
                buf.as_mut_ptr().cast::<libc::c_char>(),
                buf.len(),
                &raw mut result,
            )
        };
        if rc == libc::ERANGE && buf.len() < 1024 * 1024 {
            buf.resize(buf.len() * 2, 0);
            continue;
        }
        if rc == 0 && !result.is_null() {
            let grp = unsafe { grp.assume_init() };
            return Some(grp.gr_gid);
        }
        return None;
    }
}

fn best_effort_chgrp_miro_ego_nofollow(path: &Path) {
    if unsafe { libc::geteuid() } != 0 {
        return;
    }
    let Some(gid) = lookup_miro_ego_gid_r() else {
        return;
    };
    let Ok(path_c) = CString::new(path.as_os_str().as_bytes()) else {
        return;
    };
    unsafe {
        let _ = libc::fchownat(
            libc::AT_FDCWD,
            path_c.as_ptr(),
            u32::MAX,
            gid,
            libc::AT_SYMLINK_NOFOLLOW,
        );
    }
}

fn peer_uid_authorized(stream: &UnixStream) -> Result<(), String> {
    let mut cred = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&raw mut cred).cast::<libc::c_void>(),
            &raw mut len,
        )
    };
    if rc != 0 || (len as usize) < std::mem::size_of::<libc::ucred>() {
        return Err(format!(
            "failed to inspect peer credentials: {}",
            std::io::Error::last_os_error()
        ));
    }
    let euid = unsafe { libc::geteuid() };
    if euid != 0 {
        if cred.uid == euid {
            return Ok(());
        }
        return Err(format!(
            "peer uid {} unauthorized (expected {euid})",
            cred.uid
        ));
    }
    if cred.uid == 0 || lookup_miro_ego_uid_r().is_some_and(|uid| cred.uid == uid) {
        return Ok(());
    }
    Err(format!("peer uid {} is not authorized", cred.uid))
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

fn handle_connection(
    mut stream: UnixStream,
    controller: &AuthoritativeNetworkController,
    mutation_lock: &Mutex<()>,
    scan_gate: Option<&ScanRpcGate>,
    config: &RpcServerConfig,
) {
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(config.read_timeout));
    let _ = stream.set_write_timeout(Some(config.write_timeout));

    if let Err(msg) = peer_uid_authorized(&stream) {
        let denied = seal_rpc_response(RpcResponse::error("permission_denied", msg, None, false));
        let _ = write_framed_response(stream, &denied, config.max_line_bytes, config.write_timeout);
        return;
    }

    let response = match read_ndjson_line(&mut stream, config.max_line_bytes, config.read_timeout) {
        Ok(None) => return, // clean empty EOF
        Ok(Some(line)) => {
            dispatch_raw_payload_with_scan_gate(&line, controller, mutation_lock, scan_gate)
        }
        Err(FrameReadError::TooLarge) => RpcResponse::error(
            "request_too_large",
            "network-control request exceeded max line bytes",
            None,
            false,
        ),
        Err(FrameReadError::Timeout) => RpcResponse::error(
            "request_timeout",
            "network-control request timed out",
            None,
            true,
        ),
        Err(FrameReadError::Invalid(msg)) => {
            RpcResponse::error("request_invalid", msg, None, false)
        }
    };

    let sealed = seal_rpc_response(response);
    let _ = write_framed_response(stream, &sealed, config.max_line_bytes, config.write_timeout);
}

/// Dispatch one parsed JSON line against the controller.
pub fn dispatch_raw_payload(
    raw_line: &[u8],
    controller: &AuthoritativeNetworkController,
    mutation_lock: &Mutex<()>,
) -> RpcResponse {
    dispatch_raw_payload_with_scan_gate(raw_line, controller, mutation_lock, None)
}

fn dispatch_raw_payload_with_scan_gate(
    raw_line: &[u8],
    controller: &AuthoritativeNetworkController,
    mutation_lock: &Mutex<()>,
    scan_gate: Option<&ScanRpcGate>,
) -> RpcResponse {
    let value: Value = match serde_json::from_slice(raw_line) {
        Ok(v) => v,
        Err(_) => {
            return RpcResponse::error(
                "request_invalid",
                "request must be valid JSON",
                None,
                false,
            );
        }
    };
    let Some(obj) = value.as_object() else {
        return RpcResponse::error(
            "request_invalid",
            "request must be a JSON object",
            None,
            false,
        );
    };
    if obj.get("schema").and_then(Value::as_str) != Some(CONTROL_REQUEST_SCHEMA) {
        return RpcResponse::error(
            "request_invalid",
            "unsupported network-control request schema",
            None,
            false,
        );
    };
    let Some(operation) = obj
        .get("operation")
        .and_then(Value::as_str)
        .map(str::to_owned)
    else {
        return RpcResponse::error("operation_invalid", "missing operation", None, false);
    };

    let is_mutation = matches!(operation.as_str(), "apply" | "retry" | "forget");
    if is_mutation && json_contains_keys(&value, MUTATION_INLINE_SECRET_KEYS) {
        return RpcResponse::error(
            "inline_secret_rejected",
            "mutation requests must use credential_ref, not inline secret fields",
            Some(operation.as_str()),
            false,
        );
    }

    let request: RpcRequest = match serde_json::from_value(value) {
        Ok(r) => r,
        Err(_) => {
            return RpcResponse::error(
                "request_invalid",
                "invalid network-control request shape",
                Some(operation.as_str()),
                false,
            );
        }
    };

    match operation.as_str() {
        "scan" => {
            let scan_res = match scan_gate {
                Some(gate) => gate.execute_scan(controller),
                None => controller.scan(),
            };
            match scan_res {
                Ok(scan) => serialize_ok("scan", 200, &scan, None),
                Err(err) => error_from_network_error("scan", &err),
            }
        }
        "health"
        | "status"
        | "watchdog_mode"
        | "clock.status"
        | "clock.sync"
        | "create_credential"
        | "notify_response_flushed" => execute_operation(&request, controller),
        _ => {
            let _guard = mutation_lock.lock().unwrap_or_else(PoisonError::into_inner);
            execute_operation(&request, controller)
        }
    }
}

fn execute_operation(
    request: &RpcRequest,
    controller: &AuthoritativeNetworkController,
) -> RpcResponse {
    let op = request.operation.as_str();
    match op {
        "health" => RpcResponse::ok(
            "health",
            200,
            json!({
                "mutation_enabled": true,
                "operations": ["apply", "retry", "forget"],
                "secret_handling": "opaque_credential_reference_only",
            }),
        ),
        "status" => match controller.controller_status() {
            Ok(status) => serialize_ok("status", 200, &status, None),
            Err(err) => error_from_network_error(op, &err),
        },
        "watchdog_mode" => match controller.watchdog_mode() {
            Ok(mode) => RpcResponse::ok("watchdog_mode", 200, json!({ "mode": mode.as_str() })),
            Err(err) => error_from_network_error(op, &err),
        },
        "scan" => match controller.scan() {
            Ok(scan) => serialize_ok("scan", 200, &scan, None),
            Err(err) => error_from_network_error(op, &err),
        },
        "notify_response_flushed" => {
            controller.notify_rpc_response_flushed();
            RpcResponse::ok("notify_response_flushed", 200, json!({ "ok": true }))
        }
        "clock.status" => match controller.clock_status() {
            Ok(snap) => serialize_ok("clock.status", 200, &snap, None),
            Err(err) => error_from_network_error(op, &err),
        },
        "clock.sync" => {
            let Some(body) = &request.body else {
                return RpcResponse::error("request_invalid", "missing body", Some(op), false);
            };
            let Some(unix_time_ms) = body.get("unix_time_ms").and_then(Value::as_u64) else {
                return RpcResponse::error(
                    "request_invalid",
                    "clock.sync requires unix_time_ms",
                    Some(op),
                    false,
                );
            };
            let replace = body
                .get("replace")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            match controller.clock_sync(unix_time_ms, replace) {
                Ok(snap) => serialize_ok("clock.sync", 200, &snap, None),
                Err(err) => error_from_network_error(op, &err),
            }
        }
        "create_credential" => {
            let Some(principal) = request.principal_id.as_deref().filter(|s| !s.is_empty()) else {
                return RpcResponse::error(
                    "request_invalid",
                    "create_credential requires principal_id",
                    Some(op),
                    false,
                );
            };
            let Some(body) = &request.body else {
                return RpcResponse::error("request_invalid", "missing body", Some(op), false);
            };
            let Some(passphrase) = body.get("passphrase").and_then(Value::as_str) else {
                return RpcResponse::error(
                    "request_invalid",
                    "missing passphrase field",
                    Some(op),
                    false,
                );
            };
            match controller.create_credential_for_principal(principal, passphrase) {
                Ok(receipt) => serialize_ok("create_credential", 201, &receipt, None),
                Err(err) => error_from_network_error(op, &err),
            }
        }
        "apply" => {
            let (Some(principal), Some(idem), Some(body)) = (
                request.principal_id.as_deref(),
                request.idempotency_key.as_deref(),
                request.body.as_ref(),
            ) else {
                return RpcResponse::error(
                    "request_invalid",
                    "apply requires principal_id, idempotency_key, and body",
                    Some(op),
                    false,
                );
            };
            let desired_val = body.get("desired").unwrap_or(body);
            if let Some(mode_str) = desired_val.get("mode").and_then(Value::as_str) {
                if !matches!(
                    mode_str,
                    "hotspot" | "wifi-client" | "ethernet-dhcp" | "ethernet-static"
                ) {
                    return RpcResponse::error(
                        "unsupported_mode",
                        format!("unsupported_mode: unsupported network mode '{mode_str}'"),
                        Some(op),
                        false,
                    );
                }
            }
            if let Some(sec_str) = desired_val
                .get("wifi_client")
                .and_then(|w| w.get("security"))
                .and_then(Value::as_str)
            {
                if !matches!(
                    sec_str,
                    "open" | "wpa2-personal" | "wpa3-personal" | "wpa2-wpa3-personal"
                ) {
                    return RpcResponse::error(
                        "security_mismatch",
                        format!("security_mismatch: unsupported Wi-Fi security '{sec_str}'"),
                        Some(op),
                        false,
                    );
                }
            }
            let desired: ApplyDesiredState = match serde_json::from_value(desired_val.clone()) {
                Ok(d) => d,
                Err(err) => {
                    return RpcResponse::error(
                        "invalid_network_desired_state",
                        format!("invalid_network_desired_state: invalid desired state: {err}"),
                        Some(op),
                        false,
                    );
                }
            };
            match controller.apply(principal, idem, &desired) {
                Ok(outcome) => serialize_ok("apply", 202, &outcome, Some(outcome.replayed)),
                Err(err) => error_from_network_error(op, &err),
            }
        }
        "retry" => {
            let (Some(principal), Some(idem), Some(body)) = (
                request.principal_id.as_deref(),
                request.idempotency_key.as_deref(),
                request.body.as_ref(),
            ) else {
                return RpcResponse::error(
                    "request_invalid",
                    "retry requires principal_id, idempotency_key, and body",
                    Some(op),
                    false,
                );
            };
            let Some(tx_id) = body.get("transaction_id").and_then(Value::as_str) else {
                return RpcResponse::error(
                    "request_invalid",
                    "retry body requires transaction_id",
                    Some(op),
                    false,
                );
            };
            match controller.retry(principal, idem, tx_id) {
                Ok(outcome) => serialize_ok("retry", 202, &outcome, Some(outcome.replayed)),
                Err(err) => error_from_network_error(op, &err),
            }
        }
        "forget" => {
            let (Some(principal), Some(idem)) = (
                request.principal_id.as_deref(),
                request.idempotency_key.as_deref(),
            ) else {
                return RpcResponse::error(
                    "request_invalid",
                    "forget requires principal_id and idempotency_key",
                    Some(op),
                    false,
                );
            };
            match controller.forget(principal, idem) {
                Ok(outcome) => serialize_ok("forget", 202, &outcome, Some(outcome.replayed)),
                Err(err) => error_from_network_error(op, &err),
            }
        }
        _ => RpcResponse::error(
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
) -> RpcResponse {
    match serde_json::to_value(value) {
        Ok(body) => {
            let mut resp = RpcResponse::ok(op, status, body);
            resp.replayed = replayed;
            resp
        }
        Err(err) => RpcResponse::error(
            "controller_failure",
            format!("failed to serialize response: {err}"),
            Some(op),
            true,
        ),
    }
}

fn error_from_network_error(op: &str, error: &NetworkError) -> RpcResponse {
    match error {
        NetworkError::Busy { message } => {
            let code = error.error_code().unwrap_or("controller_busy");
            RpcResponse::error(code, message.clone(), Some(op), true)
        }
        NetworkError::Invalid { message } => {
            let code = error.error_code().unwrap_or("request_invalid");
            RpcResponse::error(code, message.clone(), Some(op), false)
        }
        NetworkError::Backend { message } => {
            let code = error.error_code().unwrap_or("network_manager_unavailable");
            RpcResponse::error(code, message.clone(), Some(op), true)
        }
        NetworkError::Core(core_err) => {
            RpcResponse::error(core_err.code(), core_err.to_string(), Some(op), true)
        }
    }
}

fn json_contains_keys(value: &Value, forbidden: &[&str]) -> bool {
    match value {
        Value::Object(map) => map.iter().any(|(k, v)| {
            let lower = k.to_ascii_lowercase();
            forbidden.iter().any(|&f| f == lower) || json_contains_keys(v, forbidden)
        }),
        Value::Array(items) => items.iter().any(|v| json_contains_keys(v, forbidden)),
        _ => false,
    }
}

fn seal_rpc_response(response: RpcResponse) -> RpcResponse {
    if let Ok(val) = serde_json::to_value(&response) {
        if json_contains_keys(&val, RESPONSE_FORBIDDEN_SECRET_KEYS) {
            return RpcResponse::error(
                "response_invalid",
                "network-control response redacted for safety",
                response.operation.as_deref(),
                false,
            );
        }
    }
    response
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
    response: &RpcResponse,
    max_bytes: usize,
    timeout: Duration,
) -> NetworkResult<()> {
    let _ = stream.set_write_timeout(Some(timeout));
    let mut payload = serde_json::to_vec(response).map_err(|err| NetworkError::Backend {
        message: format!("failed to serialize RPC response: {err}"),
    })?;
    payload.push(b'\n');
    if payload.len() > max_bytes {
        let fallback = RpcResponse::error(
            "response_too_large",
            "network-control response exceeded max line bytes",
            response.operation.as_deref(),
            false,
        );
        payload = serde_json::to_vec(&fallback).map_err(|err| NetworkError::Backend {
            message: format!("failed to serialize fallback error: {err}"),
        })?;
        payload.push(b'\n');
    }
    stream
        .write_all(&payload)
        .and_then(|()| stream.flush())
        .map_err(|err| NetworkError::Backend {
            message: format!("failed to write RPC response: {err}"),
        })?;
    let _ = stream.shutdown(std::net::Shutdown::Write);
    // Drain any buffered unread client bytes (e.g. after TooLarge) in nonblocking mode so
    // closing the AF_UNIX socket does not trigger ECONNRESET while the peer reads our reply.
    if stream.set_nonblocking(true).is_ok() {
        let mut discard = [0u8; 4096];
        let mut drained = 0usize;
        while drained < MAX_RPC_LINE_BYTES {
            match stream.read(&mut discard) {
                Ok(0) | Err(_) => break,
                Ok(n) => drained = drained.saturating_add(n),
            }
        }
    }
    Ok(())
}

/// Client for communicating with the privileged `miro-ego network-control` Unix socket.
#[derive(Debug, Clone)]
pub struct NetworkRpcClient {
    socket_path: PathBuf,
    timeout: Duration,
    max_line_bytes: usize,
}

impl NetworkRpcClient {
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
            timeout: DEFAULT_CLIENT_TIMEOUT,
            max_line_bytes: MAX_RPC_LINE_BYTES,
        }
    }

    pub fn canonical() -> Self {
        Self::new(DEFAULT_CONTROL_SOCKET_PATH)
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_max_line_bytes(mut self, max_line_bytes: usize) -> Self {
        self.max_line_bytes = max_line_bytes;
        self
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Send one framed JSON request and decode the bounded response.
    pub fn send(&self, request: &RpcRequest) -> NetworkResult<RpcResponse> {
        let mut encoded = serde_json::to_vec(request).map_err(|err| NetworkError::Invalid {
            message: format!("failed to encode RPC request: {err}"),
        })?;
        encoded.push(b'\n');
        if encoded.len() > self.max_line_bytes {
            return Err(NetworkError::Invalid {
                message: format!(
                    "request_too_large: {} > {} bytes",
                    encoded.len(),
                    self.max_line_bytes
                ),
            });
        }

        let mut stream =
            UnixStream::connect(&self.socket_path).map_err(|err| NetworkError::Backend {
                message: format!(
                    "network_controller_unavailable: {}: {err}",
                    self.socket_path.display()
                ),
            })?;
        stream
            .set_write_timeout(Some(self.timeout))
            .map_err(|err| NetworkError::Backend {
                message: format!("failed to set write timeout: {err}"),
            })?;
        stream
            .set_read_timeout(Some(self.timeout))
            .map_err(|err| NetworkError::Backend {
                message: format!("failed to set read timeout: {err}"),
            })?;

        stream
            .write_all(&encoded)
            .and_then(|()| stream.flush())
            .map_err(|err| NetworkError::Backend {
                message: format!("failed to write RPC request: {err}"),
            })?;
        let _ = stream.shutdown(std::net::Shutdown::Write);

        let raw_line = match read_ndjson_line(&mut stream, self.max_line_bytes, self.timeout) {
            Ok(Some(line)) => line,
            Ok(None) => {
                return Err(NetworkError::Backend {
                    message: "response_invalid: empty EOF from network-control".to_owned(),
                });
            }
            Err(FrameReadError::TooLarge) => {
                return Err(NetworkError::Backend {
                    message: "response_too_large: response exceeded max line bytes".to_owned(),
                });
            }
            Err(FrameReadError::Timeout) => {
                return Err(NetworkError::Backend {
                    message: "response_timeout: timed out waiting for network-control".to_owned(),
                });
            }
            Err(FrameReadError::Invalid(msg)) => {
                return Err(NetworkError::Backend {
                    message: format!("response_invalid: {msg}"),
                });
            }
        };

        let response: RpcResponse =
            serde_json::from_slice(&raw_line).map_err(|err| NetworkError::Backend {
                message: format!("response_invalid: malformed JSON response: {err}"),
            })?;
        if response.schema != CONTROL_RESPONSE_SCHEMA {
            return Err(NetworkError::Backend {
                message: format!("response_invalid: unexpected schema {}", response.schema),
            });
        }
        Ok(response)
    }

    pub fn health(&self) -> NetworkResult<Value> {
        let resp = self.send(&RpcRequest {
            schema: CONTROL_REQUEST_SCHEMA.to_owned(),
            operation: "health".to_owned(),
            principal_id: None,
            idempotency_key: None,
            body: None,
        })?;
        extract_body(resp)
    }

    pub fn status(&self) -> NetworkResult<ControllerStatus> {
        let resp = self.send(&RpcRequest {
            schema: CONTROL_REQUEST_SCHEMA.to_owned(),
            operation: "status".to_owned(),
            principal_id: None,
            idempotency_key: None,
            body: None,
        })?;
        decode_body(resp)
    }

    /// Notify the privileged network controller that the HTTP `202 Accepted` response has been
    /// flushed to the client so background interface transitions can proceed immediately.
    pub fn notify_response_flushed(&self) -> NetworkResult<()> {
        let resp = self.send(&RpcRequest {
            schema: CONTROL_REQUEST_SCHEMA.to_owned(),
            operation: "notify_response_flushed".to_owned(),
            principal_id: None,
            idempotency_key: None,
            body: None,
        })?;
        let _ = extract_body(resp)?;
        Ok(())
    }

    /// Query privileged OS clock status (`adjtimex`, `boot_id`, `<run_dir>/clock-state.json`).
    pub fn clock_status(&self) -> NetworkResult<ClockStatusSnapshot> {
        let resp = self.send(&RpcRequest {
            schema: CONTROL_REQUEST_SCHEMA.to_owned(),
            operation: "clock.status".to_owned(),
            principal_id: None,
            idempotency_key: None,
            body: None,
        })?;
        decode_body(resp)
    }

    /// Synchronize OS wall clock via privileged `clock_settime(CLOCK_REALTIME)`.
    pub fn clock_sync(
        &self,
        unix_time_ms: u64,
        replace: bool,
    ) -> NetworkResult<ClockStatusSnapshot> {
        let resp = self.send(&RpcRequest {
            schema: CONTROL_REQUEST_SCHEMA.to_owned(),
            operation: "clock.sync".to_owned(),
            principal_id: None,
            idempotency_key: None,
            body: Some(json!({
                "unix_time_ms": unix_time_ms,
                "replace": replace,
            })),
        })?;
        decode_body(resp)
    }

    /// Query `watchdog-mode` for `miro-ego-wifi-watchdog`.
    pub fn watchdog_mode(&self) -> NetworkResult<WatchdogMode> {
        let resp = self.send(&RpcRequest {
            schema: CONTROL_REQUEST_SCHEMA.to_owned(),
            operation: "watchdog_mode".to_owned(),
            principal_id: None,
            idempotency_key: None,
            body: None,
        })?;
        let body = extract_body(resp)?;
        let mode_val = body
            .get("mode")
            .cloned()
            .ok_or_else(|| NetworkError::Backend {
                message: "response_invalid: missing mode field".to_owned(),
            })?;
        serde_json::from_value(mode_val).map_err(|err| NetworkError::Backend {
            message: format!("response_invalid: invalid watchdog mode: {err}"),
        })
    }

    pub fn scan(&self) -> NetworkResult<ScanResponse> {
        let resp = self.send(&RpcRequest {
            schema: CONTROL_REQUEST_SCHEMA.to_owned(),
            operation: "scan".to_owned(),
            principal_id: None,
            idempotency_key: None,
            body: None,
        })?;
        decode_body(resp)
    }

    pub fn create_credential(
        &self,
        principal_id: &str,
        passphrase: &str,
    ) -> NetworkResult<CredentialReceipt> {
        let resp = self.send(&RpcRequest {
            schema: CONTROL_REQUEST_SCHEMA.to_owned(),
            operation: "create_credential".to_owned(),
            principal_id: Some(principal_id.to_owned()),
            idempotency_key: None,
            body: Some(json!({ "passphrase": passphrase })),
        })?;
        decode_body(resp)
    }

    pub fn apply(
        &self,
        principal_id: &str,
        idempotency_key: &str,
        desired: &ApplyDesiredState,
    ) -> NetworkResult<MutationOutcome> {
        let body = serde_json::to_value(json!({ "desired": desired })).map_err(|err| {
            NetworkError::Invalid {
                message: format!("failed to encode apply body: {err}"),
            }
        })?;
        let resp = self.send(&RpcRequest {
            schema: CONTROL_REQUEST_SCHEMA.to_owned(),
            operation: "apply".to_owned(),
            principal_id: Some(principal_id.to_owned()),
            idempotency_key: Some(idempotency_key.to_owned()),
            body: Some(body),
        })?;
        decode_body(resp)
    }

    pub fn retry(
        &self,
        principal_id: &str,
        idempotency_key: &str,
        transaction_id: &str,
    ) -> NetworkResult<MutationOutcome> {
        let resp = self.send(&RpcRequest {
            schema: CONTROL_REQUEST_SCHEMA.to_owned(),
            operation: "retry".to_owned(),
            principal_id: Some(principal_id.to_owned()),
            idempotency_key: Some(idempotency_key.to_owned()),
            body: Some(json!({ "transaction_id": transaction_id })),
        })?;
        decode_body(resp)
    }

    pub fn forget(
        &self,
        principal_id: &str,
        idempotency_key: &str,
    ) -> NetworkResult<MutationOutcome> {
        let resp = self.send(&RpcRequest {
            schema: CONTROL_REQUEST_SCHEMA.to_owned(),
            operation: "forget".to_owned(),
            principal_id: Some(principal_id.to_owned()),
            idempotency_key: Some(idempotency_key.to_owned()),
            body: Some(json!({})),
        })?;
        decode_body(resp)
    }
}

fn extract_body(resp: RpcResponse) -> NetworkResult<Value> {
    if !resp.ok {
        let err = resp.error.unwrap_or(RpcErrorBody {
            code: "controller_failure".to_owned(),
            message: "unknown RPC error".to_owned(),
        });
        let prefix = format!("{}:", err.code);
        let formatted = if err.message.starts_with(&prefix) {
            err.message
        } else {
            format!("{}: {}", err.code, err.message)
        };
        return match err.code.as_str() {
            "capture_active" | "controller_busy" | "network_busy" | "credential_store_full" => {
                Err(NetworkError::Busy { message: formatted })
            }
            "request_invalid"
            | "operation_invalid"
            | "inline_secret_rejected"
            | "idempotency_conflict"
            | "transaction_not_found"
            | "network_transaction_not_found"
            | "transaction_not_retryable"
            | "invalid_network_desired_state"
            | "credential_ref_expired"
            | "credential_ref_not_allowed"
            | "credential_ref_already_used"
            | "credential_ref_invalid"
            | "missing_credential_ref"
            | "security_mismatch"
            | "ssid_too_long"
            | "invalid_static_ipv4"
            | "unsupported_mode" => Err(NetworkError::Invalid { message: formatted }),
            _ => Err(NetworkError::Backend { message: formatted }),
        };
    }
    resp.body.ok_or_else(|| NetworkError::Backend {
        message: "response_invalid: missing body".to_owned(),
    })
}

fn decode_body<T: serde::de::DeserializeOwned>(resp: RpcResponse) -> NetworkResult<T> {
    let body = extract_body(resp)?;
    serde_json::from_value(body).map_err(|err| NetworkError::Backend {
        message: format!("response_invalid: failed to decode response body: {err}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockSystemClockBackend;
    use crate::controller::{ApplyWifiClient, ControllerConfig};
    use crate::nmcli::{
        CommandOutput, CommandRunner, DEFAULT_HOTSPOT_IPV4_CIDR, NmcliBackend, WifiSecurity,
    };
    use crate::state::{DesiredMode, NetworkStateStore, TransactionStatus};
    use miro_ego_core::ids::new_uuid_v4;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(tag: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "miro-ego-rpc-{tag}-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    struct StaticMockRunner;

    impl CommandRunner for StaticMockRunner {
        fn run(
            &self,
            _program: &str,
            args: &[&str],
            _timeout: Duration,
        ) -> NetworkResult<CommandOutput> {
            if args.contains(&"wifi") && args.contains(&"list") {
                return Ok(CommandOutput {
                    exit_code: 0,
                    stdout: "StudioNet:WPA2:80\n".to_owned(),
                    stderr: String::new(),
                });
            }
            if args.contains(&"device") && args.contains(&"show") {
                let iface = *args.last().unwrap();
                let stdout = if iface == "eth0" {
                    "GENERAL.STATE:100 (connected)\nGENERAL.CONNECTION:miro-ego-ethernet-dhcp-placeholder\nIP4.ADDRESS[1]:192.168.1.99/24\nIP4.ROUTE[1]:dst = 0.0.0.0/0, nh = 192.168.1.1, mt = 100\n"
                } else {
                    "GENERAL.STATE:100 (connected)\nGENERAL.CONNECTION:miro-ego-hotspot-rescue-000000000000\nIP4.ADDRESS[1]:10.42.0.1/24\n"
                };
                return Ok(CommandOutput {
                    exit_code: 0,
                    stdout: stdout.to_owned(),
                    stderr: String::new(),
                });
            }
            Ok(CommandOutput {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
            })
        }
    }

    struct DynamicMockRunner {
        active: Mutex<Option<(String, String)>>,
    }

    impl CommandRunner for DynamicMockRunner {
        fn run(
            &self,
            _program: &str,
            args: &[&str],
            _timeout: Duration,
        ) -> NetworkResult<CommandOutput> {
            if args.contains(&"connection") && args.contains(&"up") {
                let id_idx = args.iter().position(|a| *a == "id").unwrap();
                let if_idx = args.iter().position(|a| *a == "ifname").unwrap();
                let profile = args[id_idx + 1].to_owned();
                let iface = args[if_idx + 1].to_owned();
                *self.active.lock().unwrap() = Some((iface, profile));
                return Ok(CommandOutput {
                    exit_code: 0,
                    stdout: String::new(),
                    stderr: String::new(),
                });
            }
            if args.contains(&"device") && args.contains(&"show") {
                let iface = *args.last().unwrap();
                let active = self.active.lock().unwrap().clone();
                if let Some((act_if, prof)) = active {
                    if act_if == iface {
                        let (ip, route) = if prof.contains("hotspot") {
                            (DEFAULT_HOTSPOT_IPV4_CIDR, "")
                        } else {
                            (
                                "192.168.50.10/24",
                                "IP4.ROUTE[1]:dst = 0.0.0.0/0, nh = 192.168.50.1, mt = 100\n",
                            )
                        };
                        return Ok(CommandOutput {
                            exit_code: 0,
                            stdout: format!(
                                "GENERAL.STATE:100 (connected)\nGENERAL.CONNECTION:{prof}\nIP4.ADDRESS[1]:{ip}\n{route}"
                            ),
                            stderr: String::new(),
                        });
                    }
                }
                return Ok(CommandOutput {
                    exit_code: 0,
                    stdout: "GENERAL.STATE:30 (disconnected)\nGENERAL.CONNECTION:--\n".to_owned(),
                    stderr: String::new(),
                });
            }
            if args.contains(&"wifi") && args.contains(&"list") {
                return Ok(CommandOutput {
                    exit_code: 0,
                    stdout: "StudioNet:WPA2:84\n".to_owned(),
                    stderr: String::new(),
                });
            }
            Ok(CommandOutput {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
            })
        }
    }

    #[test]
    fn live_unix_socket_rpc_roundtrip_scan_apply_clock_flush_watchdog_and_shutdown() {
        let dir = temp_dir("live");
        let sock = dir.join("network-control.sock");
        let runner = Arc::new(DynamicMockRunner {
            active: Mutex::new(None),
        });
        let mock_clock = Arc::new(MockSystemClockBackend::new(
            new_uuid_v4(),
            10_000,
            5_000_000_000,
        ));
        let backend = NmcliBackend::new(runner, dir.join("profiles"), "wlan0", "eth0");
        let state = NetworkStateStore::open_or_init(dir.join("network-state.json")).unwrap();
        let ctrl = Arc::new(
            AuthoritativeNetworkController::new(
                backend,
                state,
                ControllerConfig::new("rdk-x5-rpc-1", dir.join("run"))
                    .with_clock_backend(mock_clock.clone()),
            )
            .unwrap(),
        );

        let server =
            NetworkRpcServer::bind(&sock, Arc::clone(&ctrl), RpcServerConfig::default()).unwrap();
        assert_eq!(
            fs::symlink_metadata(&sock).unwrap().permissions().mode() & 0o777,
            0o660
        );
        let client = NetworkRpcClient::new(&sock);
        assert_eq!(DEFAULT_CLIENT_TIMEOUT, Duration::from_secs(45));

        let health = client.health().unwrap();
        assert_eq!(health["mutation_enabled"], true);
        assert_eq!(health["operations"], json!(["apply", "retry", "forget"]));
        assert_eq!(client.watchdog_mode().unwrap(), WatchdogMode::Hotspot);

        // Clock status & sync RPC roundtrip!
        let c0 = client.clock_status().unwrap();
        assert_eq!(c0.source, "unsynchronized");
        assert!(!c0.applied);
        assert_eq!(c0.age_seconds, None);

        let c_sync = client.clock_sync(1_760_111_222_333, false).unwrap();
        assert_eq!(c_sync.source, "client");
        assert!(c_sync.applied);
        assert_eq!(c_sync.synchronized_at_ms, Some(1_760_111_222_333));
        assert_eq!(c_sync.age_seconds, Some(0));

        mock_clock.advance_seconds(19);
        let c_repeat = client.clock_sync(1_760_999_000_000, false).unwrap();
        assert!(!c_repeat.applied);
        assert_eq!(c_repeat.age_seconds, Some(19));

        let scan = client.scan().unwrap();
        assert_eq!(scan.networks.len(), 1);
        assert_eq!(scan.networks[0].ssid.as_deref(), Some("StudioNet"));

        let cred = client
            .create_credential("api-gw", "super-secret-psk")
            .unwrap();
        assert!(cred.credential_ref.starts_with("cred-"));

        // Invalid credential_ref reconstructs NetworkError::Invalid (not Backend)!
        let invalid_cred_err = client
            .apply(
                "api-gw",
                "idem-rpc-bad-cred",
                &ApplyDesiredState {
                    mode: DesiredMode::WifiClient,
                    wifi_client: Some(ApplyWifiClient {
                        ssid: "StudioNet".to_owned(),
                        security: WifiSecurity::Wpa2Personal,
                        credential_ref: Some("cred-nonexistent".to_owned()),
                    }),
                    ethernet: None,
                },
            )
            .unwrap_err();
        assert!(matches!(invalid_cred_err, NetworkError::Invalid { .. }));

        let applied = client
            .apply(
                "api-gw",
                "idem-rpc-1",
                &ApplyDesiredState {
                    mode: DesiredMode::WifiClient,
                    wifi_client: Some(ApplyWifiClient {
                        ssid: "StudioNet".to_owned(),
                        security: WifiSecurity::Wpa2Personal,
                        credential_ref: Some(cred.credential_ref),
                    }),
                    ethernet: None,
                },
            )
            .unwrap();
        // Async RPC returns 202 Accepted WITHOUT prematurely firing notify_response_flushed
        // before the HTTP gateway flushes its response; explicit notify_response_flushed wakes it!
        assert_eq!(applied.final_status, TransactionStatus::Accepted);
        client.notify_response_flushed().unwrap();
        let settled = ctrl.wait_for_idle(Duration::from_secs(5)).unwrap().unwrap();
        assert_eq!(settled.status, TransactionStatus::Committed);
        assert_eq!(client.watchdog_mode().unwrap(), WatchdogMode::WifiClient);
        let st = client.status().unwrap();
        assert_eq!(st.observed.wifi_client.state, "connected");
        assert_eq!(st.observed.wifi_client.addresses, vec!["192.168.50.10"]);

        // Inline secret in apply request MUST be rejected!
        let bad_resp = client
            .send(&RpcRequest {
                schema: CONTROL_REQUEST_SCHEMA.to_owned(),
                operation: "apply".to_owned(),
                principal_id: Some("api-gw".to_owned()),
                idempotency_key: Some("idem-inline-secret".to_owned()),
                body: Some(json!({
                    "desired": {
                        "mode": "wifi-client",
                        "wifi_client": { "ssid": "X", "security": "wpa2-personal", "psk": "leaked1234" }
                    }
                })),
            })
            .unwrap();
        assert!(!bad_resp.ok);
        assert_eq!(bad_resp.error.unwrap().code, "inline_secret_rejected");

        server.shutdown();
        assert!(!sock.exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rpc_server_rejects_oversized_lines_and_handles_read_timeout() {
        let dir = temp_dir("limits");
        let sock = dir.join("network-control.sock");
        let backend = NmcliBackend::new(
            Arc::new(StaticMockRunner),
            dir.join("profiles"),
            "wlan0",
            "eth0",
        );
        let state = NetworkStateStore::open_or_init(dir.join("network-state.json")).unwrap();
        let ctrl = Arc::new(
            AuthoritativeNetworkController::new(
                backend,
                state,
                ControllerConfig::new("rdk-x5-rpc-2", dir.join("run")),
            )
            .unwrap(),
        );
        let cfg = RpcServerConfig {
            max_line_bytes: 256,
            read_timeout: Duration::from_millis(150),
            write_timeout: Duration::from_millis(500),
            max_concurrent_connections: 4,
        };
        let server = NetworkRpcServer::bind(&sock, ctrl, cfg).unwrap();

        // 1. Oversized line > 256 bytes
        {
            let mut raw_stream = UnixStream::connect(&sock).unwrap();
            raw_stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let huge = vec![b'a'; 512];
            let _ = raw_stream.write_all(&huge);
            let _ = raw_stream.write_all(b"\n");
            let mut reply = String::new();
            raw_stream.read_to_string(&mut reply).unwrap();
            let parsed: RpcResponse = serde_json::from_str(reply.trim()).unwrap();
            assert!(!parsed.ok);
            assert_eq!(parsed.error.unwrap().code, "request_too_large");
        }

        // 2. Slowloris client that sends partial bytes without newline -> read timeout
        {
            let mut raw_stream = UnixStream::connect(&sock).unwrap();
            raw_stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            raw_stream.write_all(b"{\"schema\":").unwrap();
            raw_stream.flush().unwrap();
            let mut reply = String::new();
            raw_stream.read_to_string(&mut reply).unwrap();
            let parsed: RpcResponse = serde_json::from_str(reply.trim()).unwrap();
            assert!(!parsed.ok);
            assert_eq!(parsed.error.unwrap().code, "request_timeout");
        }

        server.shutdown();
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn fast_path_status_health_and_clock_respond_under_5ms_during_slow_wifi_scan() {
        struct SlowScanRunner {
            scan_started: Arc<AtomicBool>,
        }
        impl CommandRunner for SlowScanRunner {
            fn run(
                &self,
                _program: &str,
                args: &[&str],
                _timeout: Duration,
            ) -> NetworkResult<CommandOutput> {
                if args.contains(&"wifi")
                    && args.contains(&"list")
                    && args.windows(2).any(|w| w == ["--rescan", "auto"])
                {
                    self.scan_started.store(true, Ordering::Release);
                    thread::sleep(Duration::from_millis(350));
                    return Ok(CommandOutput {
                        exit_code: 0,
                        stdout: "SlowNet:WPA2:80\n".to_owned(),
                        stderr: String::new(),
                    });
                }
                if args.contains(&"device") && args.contains(&"show") {
                    return Ok(CommandOutput {
                        exit_code: 0,
                        stdout: "GENERAL.STATE:100 (connected)\nGENERAL.CONNECTION:miro-ego-hotspot-rescue-000000000000\nIP4.ADDRESS[1]:10.42.0.1/24\n"
                            .to_owned(),
                        stderr: String::new(),
                    });
                }
                Ok(CommandOutput {
                    exit_code: 0,
                    stdout: String::new(),
                    stderr: String::new(),
                })
            }
        }

        let dir = temp_dir("scan-isolation");
        let sock = dir.join("network-control.sock");
        let scan_started = Arc::new(AtomicBool::new(false));
        let runner = Arc::new(SlowScanRunner {
            scan_started: Arc::clone(&scan_started),
        });
        let mock_clock = Arc::new(MockSystemClockBackend::new(
            new_uuid_v4(),
            10_000,
            5_000_000_000,
        ));
        let backend = NmcliBackend::new(runner, dir.join("profiles"), "wlan0", "eth0");
        let state = NetworkStateStore::open_or_init(dir.join("state.json")).unwrap();
        let ctrl = Arc::new(
            AuthoritativeNetworkController::new(
                backend,
                state,
                ControllerConfig::new("rdk-x5-scan-iso", dir.join("run"))
                    .with_clock_backend(mock_clock),
            )
            .unwrap(),
        );
        let server = NetworkRpcServer::bind(&sock, ctrl, RpcServerConfig::default()).unwrap();
        let client = NetworkRpcClient::new(&sock);

        // Warm up socket path once before measuring latency under contention.
        let _ = client.health().unwrap();

        let scan_client = client.clone();
        let scan_handle = thread::spawn(move || scan_client.scan().unwrap());
        while !scan_started.load(Ordering::Acquire) {
            thread::yield_now();
        }

        let t_health = Instant::now();
        let _ = client.health().unwrap();
        let health_elapsed = t_health.elapsed();

        let t_status = Instant::now();
        let _ = client.status().unwrap();
        let status_elapsed = t_status.elapsed();

        let t_clock = Instant::now();
        let _ = client.clock_status().unwrap();
        let clock_elapsed = t_clock.elapsed();

        assert!(
            health_elapsed < Duration::from_millis(5),
            "health RPC took {health_elapsed:?} during active Wi-Fi scan"
        );
        assert!(
            status_elapsed < Duration::from_millis(5),
            "status RPC took {status_elapsed:?} during active Wi-Fi scan"
        );
        assert!(
            clock_elapsed < Duration::from_millis(5),
            "clock.status RPC took {clock_elapsed:?} during active Wi-Fi scan"
        );

        let scan_res = scan_handle.join().unwrap();
        assert_eq!(scan_res.networks.len(), 1);

        // Also verify principal-bound credential & canonical error codes over RPC:
        let cred = client
            .create_credential("principal-a", "valid-psk-9999")
            .unwrap();
        let wrong_principal_err = client
            .apply(
                "principal-b",
                "idem-wrong-p",
                &ApplyDesiredState {
                    mode: DesiredMode::WifiClient,
                    wifi_client: Some(ApplyWifiClient {
                        ssid: "SlowNet".to_owned(),
                        security: WifiSecurity::Wpa2Personal,
                        credential_ref: Some(cred.credential_ref),
                    }),
                    ethernet: None,
                },
            )
            .unwrap_err();
        assert_eq!(
            wrong_principal_err.error_code(),
            Some("credential_ref_not_allowed")
        );

        let unsupported_resp = client
            .send(&RpcRequest {
                schema: CONTROL_REQUEST_SCHEMA.to_owned(),
                operation: "apply".to_owned(),
                principal_id: Some("principal-a".to_owned()),
                idempotency_key: Some("idem-unsup".to_owned()),
                body: Some(json!({ "desired": { "mode": "ad-hoc-mesh" } })),
            })
            .unwrap();
        assert_eq!(
            unsupported_resp.error.as_ref().map(|e| e.code.as_str()),
            Some("unsupported_mode")
        );

        server.shutdown();
        fs::remove_dir_all(dir).unwrap();
    }
}
