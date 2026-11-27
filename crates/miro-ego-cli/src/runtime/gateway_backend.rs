//! Production implementation of [`GatewayBackend`] bridging all 7 workspace crates
//! (`miro-ego-core`, `miro-ego-storage`, `miro-ego-capture`, `miro-ego-api`, `miro-ego-network`,
//! `miro-ego-deploy`, and `miro-ego-hw`).

use super::clock::{ClockStateService, now_unix_secs};
use super::error_map::{
    map_deploy_error, map_hw_error, map_network_error, map_network_mutation_error,
    map_storage_error, network_mutation_unavailable,
};
use super::firmware::{
    FIRMWARE_CHECK_CACHE_TTL, FirmwareRuntimeState, build_firmware_status_payload,
    format_firmware_status_from_parts,
};
use super::native_capture::NativeCaptureBackend;
use super::web_assets::load_embedded_web_assets;
use super::{NoopHealthChecker, NoopSystemctlRunner, rfc3339_now_utc};
use async_trait::async_trait;
use bytes::Bytes;
use miro_ego_api::{
    ApiError, ApiResult, AuditEvent, CommandOutcome, EventReplayBuffer, GatewayBackend,
    H264PreviewHub, IdempotentCommand, ListLogsQuery, ListSessionsQuery, LogEtagCache,
    MjpegPreviewHub, NetworkEventReplayBuffer, OpenRepresentation, RawManifest, SecurityProfile,
    StaticAsset, open_safe_log_file_with_cache,
};
use miro_ego_core::config::{DeviceConfig, SecurityConfig};
use miro_ego_core::ids::hex_encode;
use miro_ego_deploy::{
    DefaultOfflinePackageHook, DeployError, DeployRpcClient, OfflinePackageHook, ReleaseManager,
    ReleaseManagerPaths, validate_commit_sha,
};
use miro_ego_hw::{SysfsLedController, VoiceController};
use miro_ego_network::{
    ApplyDesiredState, ControllerStatus, MdnsScheme, NetworkError, NetworkRpcClient,
    derive_rescue_identity,
};
use miro_ego_storage::coordinator::CAPTURE_TIMER_SET_SCHEMA;
use miro_ego_storage::{
    ApiAuditEvent, AuditLogger, AuditWriteTarget, BinarySessionCatalog, CameraFocusSetRequest,
    CaptureBackend, CaptureCoordinator, CapturePhase, CaptureStartRequest, CaptureStopRequest,
    CaptureTimerSetRequest, DeleteSessionsRequest, ExfatVolumeManager, LogStore, StorageError,
    VolumeManager, open_manifest, open_verified_artifact_checked_with_volume,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const DEFAULT_NETWORK_MUTATION_TIMEOUT: Duration = Duration::from_secs(20);
const DEFAULT_NETWORK_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_NETWORK_HEALTH_TIMEOUT: Duration = Duration::from_millis(800);
const DEFAULT_DEPLOY_STATUS_TIMEOUT: Duration = Duration::from_secs(5);
const AUDIT_POLL_COALESCE_WINDOW: Duration = Duration::from_secs(30);
const AUDIT_POLL_COALESCE_MAX_COUNT: u64 = 20;

fn is_coalesceable_poll_operation(operation_id: &str) -> bool {
    matches!(
        operation_id,
        "getCaptureStatus" | "getDevice" | "getNetworkStatus"
    )
}

struct PendingAuditPoll {
    event: ApiAuditEvent,
    count: u64,
    started_at: Instant,
}

#[derive(Default)]
struct AuditPollCoalescer {
    seen_initial: HashMap<(Option<String>, String), Instant>,
    pending: Option<PendingAuditPoll>,
}

/// Configuration knobs for constructing a [`ProductionGatewayBackend`].
#[derive(Clone)]
pub struct ProductionBackendConfig {
    pub device_config: DeviceConfig,
    pub software_commit: String,
    pub network_socket: Option<PathBuf>,
    pub deploy_socket: Option<PathBuf>,
    pub release_paths: ReleaseManagerPaths,
    pub enable_h264_preview: bool,
    pub mock_mode: bool,
    pub voice_controller: Option<Arc<VoiceController>>,
    pub indicator: Option<Arc<SysfsLedController>>,
}

impl ProductionBackendConfig {
    pub fn from_device_config(device_config: DeviceConfig) -> Self {
        Self {
            device_config,
            software_commit: option_env!("MIRO_EGO_GIT_COMMIT")
                .map(str::to_owned)
                .filter(|s| {
                    (40..=64).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_hexdigit())
                })
                .unwrap_or_else(|| "0".repeat(40)),
            network_socket: Some(PathBuf::from(miro_ego_network::DEFAULT_CONTROL_SOCKET_PATH)),
            deploy_socket: std::env::var_os("MIRO_EGO_DEPLOY_SOCKET").map(PathBuf::from),
            release_paths: ReleaseManagerPaths::canonical(),
            enable_h264_preview: false,
            mock_mode: false,
            voice_controller: None,
            indicator: None,
        }
    }

    pub(super) fn mdns_scheme(&self) -> MdnsScheme {
        match self.device_config.security {
            SecurityConfig::Customer { .. } => MdnsScheme::Https,
            SecurityConfig::Lab { .. } => MdnsScheme::Http,
        }
    }
}

/// Complete production implementation of [`GatewayBackend`] bridging all 7 workspace crates.
pub struct ProductionGatewayBackend {
    pub(super) config: ProductionBackendConfig,
    coordinator: Arc<CaptureCoordinator>,
    capture_backend: Option<Arc<dyn CaptureBackend>>,
    native_capture: Option<Arc<NativeCaptureBackend>>,
    volume: Arc<ExfatVolumeManager>,
    catalog: Arc<BinarySessionCatalog>,
    audit: Arc<AuditLogger>,
    audit_coalescer: Mutex<AuditPollCoalescer>,
    logs: Arc<LogStore>,
    network_client: Option<NetworkRpcClient>,
    network_probe_client: Option<NetworkRpcClient>,
    network_health_client: Option<NetworkRpcClient>,
    deploy_client: Option<DeployRpcClient>,
    deploy_status_client: Option<DeployRpcClient>,
    release_manager: Arc<ReleaseManager>,
    mjpeg_hub: Arc<MjpegPreviewHub>,
    h264_hub: Option<Arc<H264PreviewHub>>,
    capture_events: Arc<EventReplayBuffer>,
    network_events: Arc<NetworkEventReplayBuffer>,
    log_etag_cache: Arc<LogEtagCache>,
    clock: Arc<ClockStateService>,
    voice_fallback_volume: Mutex<u8>,
    firmware: Mutex<FirmwareRuntimeState>,
    web_assets: HashMap<String, StaticAsset>,
}

impl ProductionGatewayBackend {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: ProductionBackendConfig,
        coordinator: Arc<CaptureCoordinator>,
        volume: Arc<ExfatVolumeManager>,
        catalog: Arc<BinarySessionCatalog>,
        audit: Arc<AuditLogger>,
        logs: Arc<LogStore>,
        mjpeg_hub: Arc<MjpegPreviewHub>,
        h264_hub: Option<Arc<H264PreviewHub>>,
    ) -> Self {
        let network_client = config
            .network_socket
            .as_ref()
            .map(|sock| NetworkRpcClient::new(sock).with_timeout(DEFAULT_NETWORK_MUTATION_TIMEOUT));
        let network_probe_client = config
            .network_socket
            .as_ref()
            .map(|sock| NetworkRpcClient::new(sock).with_timeout(DEFAULT_NETWORK_PROBE_TIMEOUT));

        let clock = Arc::new(ClockStateService::new(
            None,
            network_probe_client,
            network_client,
            !config.mock_mode,
        ));
        Self::new_with_clock(
            config,
            coordinator,
            volume,
            catalog,
            audit,
            logs,
            mjpeg_hub,
            h264_hub,
            clock,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_with_clock(
        config: ProductionBackendConfig,
        coordinator: Arc<CaptureCoordinator>,
        volume: Arc<ExfatVolumeManager>,
        catalog: Arc<BinarySessionCatalog>,
        audit: Arc<AuditLogger>,
        logs: Arc<LogStore>,
        mjpeg_hub: Arc<MjpegPreviewHub>,
        h264_hub: Option<Arc<H264PreviewHub>>,
        clock: Arc<ClockStateService>,
    ) -> Self {
        let network_client = config
            .network_socket
            .as_ref()
            .map(|sock| NetworkRpcClient::new(sock).with_timeout(DEFAULT_NETWORK_MUTATION_TIMEOUT));
        let network_probe_client = config
            .network_socket
            .as_ref()
            .map(|sock| NetworkRpcClient::new(sock).with_timeout(DEFAULT_NETWORK_PROBE_TIMEOUT));
        let network_health_client = config
            .network_socket
            .as_ref()
            .map(|sock| NetworkRpcClient::new(sock).with_timeout(DEFAULT_NETWORK_HEALTH_TIMEOUT));

        let deploy_client = config
            .deploy_socket
            .as_ref()
            .map(|sock| DeployRpcClient::new(sock).with_timeout(Duration::from_secs(30)));
        let deploy_status_client = config
            .deploy_socket
            .as_ref()
            .map(|sock| DeployRpcClient::new(sock).with_timeout(DEFAULT_DEPLOY_STATUS_TIMEOUT));

        let release_manager = Arc::new(ReleaseManager::new(
            config.release_paths.clone(),
            Arc::new(NoopSystemctlRunner),
            Arc::new(NoopHealthChecker),
        ));

        Self {
            config,
            coordinator,
            capture_backend: None,
            native_capture: None,
            volume,
            catalog,
            audit,
            audit_coalescer: Mutex::new(AuditPollCoalescer::default()),
            logs,
            network_client,
            network_probe_client,
            network_health_client,
            deploy_client,
            deploy_status_client,
            release_manager,
            mjpeg_hub,
            h264_hub,
            capture_events: Arc::new(EventReplayBuffer::default()),
            network_events: Arc::new(NetworkEventReplayBuffer::default()),
            log_etag_cache: Arc::new(LogEtagCache::default()),
            clock,
            voice_fallback_volume: Mutex::new(80),
            firmware: Mutex::new(FirmwareRuntimeState::default()),
            web_assets: load_embedded_web_assets(),
        }
    }

    pub fn with_capture_backend(mut self, capture_backend: Arc<dyn CaptureBackend>) -> Self {
        self.capture_backend = Some(capture_backend);
        self
    }

    pub(super) fn with_native_capture(mut self, native: Option<Arc<NativeCaptureBackend>>) -> Self {
        self.native_capture = native;
        self
    }

    pub(super) fn native_capture(&self) -> Option<&Arc<NativeCaptureBackend>> {
        self.native_capture.as_ref()
    }

    pub fn coordinator(&self) -> &Arc<CaptureCoordinator> {
        &self.coordinator
    }

    pub fn volume(&self) -> &Arc<ExfatVolumeManager> {
        &self.volume
    }

    pub fn catalog(&self) -> &Arc<BinarySessionCatalog> {
        &self.catalog
    }

    pub fn audit(&self) -> &Arc<AuditLogger> {
        &self.audit
    }

    pub fn indicator(&self) -> Option<&Arc<SysfsLedController>> {
        self.config.indicator.as_ref()
    }

    pub fn voice_controller(&self) -> Option<&Arc<VoiceController>> {
        self.config.voice_controller.as_ref()
    }

    pub(super) fn network_probe_client(&self) -> Option<NetworkRpcClient> {
        self.network_probe_client.clone()
    }

    fn require_network_scan_client(&self) -> ApiResult<NetworkRpcClient> {
        self.network_client.clone().ok_or_else(|| {
            ApiError::service_unavailable(
                "network_controller_unavailable",
                "network control socket is not configured",
            )
            .with_header("Retry-After", "1")
            .with_error_code_header()
        })
    }

    fn require_network_mutation_client(&self) -> ApiResult<NetworkRpcClient> {
        self.network_client.clone().ok_or_else(|| {
            network_mutation_unavailable("not_enabled", "network control socket is not configured")
        })
    }

    fn write_storage_audit_event(&self, storage_event: &ApiAuditEvent) -> ApiResult<bool> {
        let target = match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| self.audit.record(storage_event))
            }
            _ => self.audit.record(storage_event),
        }
        .map_err(map_storage_error)?;
        match target {
            AuditWriteTarget::Disk => Ok(true),
            AuditWriteTarget::MemoryFallback => Ok(false),
            AuditWriteTarget::Unavailable => Err(ApiError::audit_unavailable()),
        }
    }

    /// Flush any pending coalesced read-only status poll audit event to `AuditLogger`.
    pub fn flush_coalesced_audit(&self) {
        let flushed = self
            .audit_coalescer
            .lock()
            .ok()
            .and_then(|mut guard| guard.pending.take())
            .map(|mut p| {
                p.event.poll_count = Some(p.count);
                p.event
            });
        if let Some(ev) = flushed {
            let _ = self.audit.record(&ev);
        }
    }
}

pub(super) fn controller_status_to_openapi(
    status: &ControllerStatus,
    device_id: &str,
    listen_port: u16,
    mdns_scheme: MdnsScheme,
) -> Value {
    let mut observed = serde_json::to_value(&status.observed).unwrap_or_else(|_| {
        json!({
            "ap": {
                "state": "unavailable",
                "interface": Value::Null,
                "addresses": [],
                "peer_or_ssid": Value::Null,
            },
            "wifi_client": {
                "state": "unavailable",
                "interface": Value::Null,
                "addresses": [],
                "peer_or_ssid": Value::Null,
            },
            "wired": {
                "state": "unavailable",
                "interface": Value::Null,
                "addresses": [],
                "peer_or_ssid": Value::Null,
            },
            "default_route": "none",
            "mdns": {
                "hostname": "miro-ego.local",
                "service": miro_ego_network::MDNS_SERVICE_TYPE,
                "aliases": [mdns_scheme.alias_service_type()],
                "port": listen_port.max(1),
            },
            "devices": [],
        })
    });

    if let Some(obs_obj) = observed.as_object_mut() {
        let rescue_ssid = status
            .rescue_ssid
            .clone()
            .or_else(|| derive_rescue_identity(device_id).ok().map(|(ssid, _)| ssid));
        if let Some(ap_obj) = obs_obj.get_mut("ap").and_then(Value::as_object_mut) {
            let needs_ssid = ap_obj.get("peer_or_ssid").is_none_or(Value::is_null);
            if needs_ssid {
                if let Some(ssid) = rescue_ssid {
                    ap_obj.insert("peer_or_ssid".to_owned(), Value::String(ssid));
                }
            }
        }
        if let Some(mdns_obj) = obs_obj.get_mut("mdns").and_then(Value::as_object_mut) {
            if mdns_obj.get("port").and_then(Value::as_u64).unwrap_or(0) == 0 {
                mdns_obj.insert("port".to_owned(), json!(listen_port.max(1)));
            }
        }
    }

    json!({
        "schema": miro_ego_network::NETWORK_STATUS_SCHEMA,
        "authority_epoch": status.authority_epoch,
        "source_revision": status.source_revision,
        "observed_at": rfc3339_now_utc(),
        "saved": status.saved,
        "verified": status.verified,
        "desired": status.desired,
        "observed": observed,
        "transaction": status.transaction,
        "mutation_capability": status.capability,
        "concurrency_capability": {
            "rescue_ap_required": true,
            "same_phy_ap_sta": "supported",
            "exclusive_client_failure_timeout_seconds": 10,
            "max_managed_interfaces": 1,
            "max_ap_interfaces": 1,
        }
    })
}

pub fn degraded_network_status(device_id: &str, listen_port: u16, disabled_reason: &str) -> Value {
    degraded_network_status_with_scheme(device_id, listen_port, MdnsScheme::Http, disabled_reason)
}

pub fn degraded_network_status_with_scheme(
    device_id: &str,
    listen_port: u16,
    mdns_scheme: MdnsScheme,
    disabled_reason: &str,
) -> Value {
    let rescue_ssid = derive_rescue_identity(device_id)
        .ok()
        .map(|(ssid, _)| Value::String(ssid))
        .unwrap_or(Value::Null);
    let normalized_reason = match disabled_reason {
        "not_enabled"
        | "auth_profile_unavailable"
        | "controller_unavailable"
        | "network_manager_unavailable"
        | "rescue_ap_not_validated"
        | "capture_active"
        | "recovery_required"
        | "maintenance_window_closed"
        | "unsupported_concurrency" => disabled_reason,
        "network_controller_not_configured" => "not_enabled",
        _ => "controller_unavailable",
    };
    json!({
        "schema": miro_ego_network::NETWORK_STATUS_SCHEMA,
        "authority_epoch": "00000000-0000-4000-8000-000000000000",
        "source_revision": 0,
        "observed_at": rfc3339_now_utc(),
        "saved": false,
        "verified": false,
        "desired": {
            "mode": "hotspot",
            "wifi_client": Value::Null,
            "ethernet": Value::Null,
        },
        "observed": {
            "ap": {
                "state": "unavailable",
                "interface": Value::Null,
                "addresses": [],
                "peer_or_ssid": rescue_ssid,
            },
            "wifi_client": {
                "state": "unavailable",
                "interface": Value::Null,
                "addresses": [],
                "peer_or_ssid": Value::Null,
            },
            "wired": {
                "state": "unavailable",
                "interface": Value::Null,
                "addresses": [],
                "peer_or_ssid": Value::Null,
            },
            "default_route": "none",
            "mdns": {
                "hostname": "miro-ego.local",
                "service": miro_ego_network::MDNS_SERVICE_TYPE,
                "aliases": [mdns_scheme.alias_service_type()],
                "port": listen_port.max(1),
            },
            "devices": [],
        },
        "transaction": {
            "current": Value::Null,
            "latest": Value::Null,
        },
        "mutation_capability": {
            "enabled": false,
            "disabled_reason": normalized_reason,
            "operations": ["apply", "retry", "forget"],
            "idempotency_key_required": true,
            "secret_handling": "opaque_credential_reference_only",
            "active_state_policy": "idle_only",
        },
        "concurrency_capability": {
            "rescue_ap_required": true,
            "same_phy_ap_sta": "unverified",
            "exclusive_client_failure_timeout_seconds": 10,
            "max_managed_interfaces": 1,
            "max_ap_interfaces": 1,
        }
    })
}

async fn run_blocking<F, T>(f: F) -> ApiResult<T>
where
    F: FnOnce() -> ApiResult<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.map_err(|err| {
        ApiError::internal("task_join_failed", format!("blocking task failed: {err}"))
    })?
}

fn spawn_deferred_network_response_flush(client: NetworkRpcClient) {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(async move {
            tokio::time::sleep(Duration::from_millis(15)).await;
            let _ = tokio::task::spawn_blocking(move || {
                let _ = client.notify_response_flushed();
            })
            .await;
        });
    }
}

#[async_trait]
impl GatewayBackend for ProductionGatewayBackend {
    async fn device_info(&self, security_profile: SecurityProfile) -> ApiResult<Value> {
        let cfg = self.config.device_config.clone();
        let volume = Arc::clone(&self.volume);
        let coordinator = Arc::clone(&self.coordinator);
        let release_manager = Arc::clone(&self.release_manager);
        let fallback_commit = self.config.software_commit.clone();
        let preview_h264 = self.h264_hub.is_some();
        let net_health_client = self.network_health_client.clone();

        run_blocking(move || {
            let released = volume.is_volume_released();
            let vol_info = if released {
                None
            } else {
                volume.inspect_volume().ok()
            };
            let vol_meta = if released {
                None
            } else {
                volume.read_metadata().ok()
            };
            let writable = !released && volume.is_mounted_and_writable();
            let volume_id = vol_meta
                .map(|m| m.volume_id)
                .unwrap_or_else(|| "00000000-0000-4000-8000-000000000000".to_owned());
            let status = coordinator.capture_status().map_err(map_storage_error)?;
            let phase_busy = coordinator.phase().map(|p| p.is_busy()).unwrap_or(false);
            let storage_ready = writable
                && coordinator
                    .capture_readiness()
                    .map(|r| r.allowed)
                    .unwrap_or(false);

            let deploy = release_manager.status().ok();
            let commit = deploy
                .as_ref()
                .and_then(|d| d.current.clone())
                .unwrap_or(fallback_commit);
            let pkg_version = deploy
                .as_ref()
                .and_then(|d| d.current_version.clone())
                .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned());
            let network_mutation = net_health_client
                .as_ref()
                .is_some_and(|client| client.health().is_ok());

            let camera_connected = status
                .snapshot
                .runtime
                .get("camera")
                .and_then(|c| c.get("state"))
                .and_then(Value::as_str)
                != Some("disconnected");
            let active_state = matches!(
                status.snapshot.device_state.as_str(),
                "recording" | "finalizing" | "encoding" | "verifying"
            );
            let (cal_enabled, cal_disabled_reason) = if !camera_connected {
                (false, Value::String("hardware_unavailable".to_owned()))
            } else if phase_busy || active_state {
                (
                    false,
                    Value::String("maintenance_or_capture_busy".to_owned()),
                )
            } else if !storage_ready {
                (false, Value::String("storage_unavailable".to_owned()))
            } else {
                (true, Value::Null)
            };

            Ok(json!({
                "schema": "miro-ego.device.v1",
                "device": {
                    "device_id": cfg.device.device_id,
                    "device_label": cfg.device.device_label,
                },
                "hardware_fingerprint": cfg.device.hardware_fingerprint,
                "api_version": "1.0",
                "build": {
                    "package_version": pkg_version,
                    "commit": commit,
                    "build_id": format!("miro-ego-{pkg_version}-rust"),
                },
                "security_profile": security_profile.as_str(),
                "capabilities": {
                    "capture": true,
                    "preview": true,
                    "preview_h264": preview_h264,
                    "diagnostic_logs": true,
                    "range_download": true,
                    "network_mutation": network_mutation,
                    "session_list": true,
                    "session_detail": true,
                    "artifact_download": true,
                    "capture_status": true,
                    "session_deletion": true,
                    "calibration_capture": {
                        "supported": true,
                        "enabled": cal_enabled,
                        "disabled_reason": cal_disabled_reason,
                        "required_video_layout": "split-eyes",
                    },
                },
                "storage": {
                    "volume_id": volume_id,
                    "total_bytes": vol_info.as_ref().map(|v| v.total_bytes).unwrap_or(0),
                    "available_bytes": vol_info.as_ref().map(|v| v.available_bytes).unwrap_or(0),
                    "writable": writable,
                },
                "runtime": status.snapshot.runtime,
            }))
        })
        .await
    }

    async fn capture_status(&self) -> ApiResult<Value> {
        let coordinator = Arc::clone(&self.coordinator);
        run_blocking(move || {
            let status = coordinator.capture_status().map_err(map_storage_error)?;
            serde_json::to_value(status).map_err(|err| {
                ApiError::internal(
                    "serialize_failed",
                    format!("serialize capture status: {err}"),
                )
            })
        })
        .await
    }

    async fn capture_snapshot_event(&self) -> ApiResult<Value> {
        let coordinator = Arc::clone(&self.coordinator);
        run_blocking(move || {
            let status = coordinator.capture_status().map_err(map_storage_error)?;
            let session_id = status
                .snapshot
                .active_recording
                .as_ref()
                .map(|r| r.recording_state.session_id.clone());
            Ok(json!({
                "authority_epoch": status.authority_epoch,
                "source_revision": status.source_revision,
                "type": "snapshot",
                "occurred_at": rfc3339_now_utc(),
                "session_id": session_id,
                "data": status.snapshot,
            }))
        })
        .await
    }

    async fn capture_readiness(&self) -> ApiResult<Value> {
        let coordinator = Arc::clone(&self.coordinator);
        run_blocking(move || {
            let readiness = coordinator.capture_readiness().map_err(map_storage_error)?;
            serde_json::to_value(readiness).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize readiness: {err}"))
            })
        })
        .await
    }

    async fn capture_safe_swap(&self) -> ApiResult<Option<Value>> {
        let coordinator = Arc::clone(&self.coordinator);
        run_blocking(move || {
            let receipt = coordinator.safe_swap_receipt().map_err(map_storage_error)?;
            match receipt {
                Some(r) => serde_json::to_value(r).map(Some).map_err(|err| {
                    ApiError::internal("serialize_failed", format!("serialize safe-swap: {err}"))
                }),
                None => Ok(None),
            }
        })
        .await
    }

    async fn start_capture(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        let req: CaptureStartRequest = serde_json::from_value(cmd.body).map_err(|err| {
            ApiError::bad_request(format!("invalid capture start request: {err}"))
        })?;
        let coordinator = Arc::clone(&self.coordinator);
        let key = cmd.idempotency_key;
        let principal_id = cmd.principal_id;
        run_blocking(move || {
            let outcome = coordinator
                .start_capture_for_principal(req, Some(&key), &principal_id)
                .map_err(map_storage_error)?;
            let body = serde_json::to_value(&outcome.value).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize start status: {err}"))
            })?;
            Ok(CommandOutcome::accepted(body, outcome.replayed))
        })
        .await
    }

    async fn stop_capture(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        let req: CaptureStopRequest = serde_json::from_value(cmd.body)
            .map_err(|err| ApiError::bad_request(format!("invalid capture stop request: {err}")))?;
        let coordinator = Arc::clone(&self.coordinator);
        let key = cmd.idempotency_key;
        let principal_id = cmd.principal_id;
        run_blocking(move || {
            let outcome = coordinator
                .begin_stop_capture_for_principal(req, Some(&key), &principal_id)
                .map_err(map_storage_error)?;
            match outcome.value {
                Some(snapshot) => {
                    let body = serde_json::to_value(&snapshot).map_err(|err| {
                        ApiError::internal(
                            "serialize_failed",
                            format!("serialize stop status: {err}"),
                        )
                    })?;
                    Ok(CommandOutcome::accepted(body, outcome.replayed))
                }
                None => Ok(CommandOutcome::no_content(outcome.replayed)),
            }
        })
        .await
    }

    async fn capture_timer(&self) -> ApiResult<Value> {
        let coordinator = Arc::clone(&self.coordinator);
        run_blocking(move || {
            let status = coordinator.get_capture_timer().map_err(map_storage_error)?;
            serde_json::to_value(status).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize timer: {err}"))
            })
        })
        .await
    }

    async fn set_capture_timer(&self, enabled: bool, duration_seconds: u64) -> ApiResult<Value> {
        let dur_u32 = u32::try_from(duration_seconds)
            .map_err(|_| ApiError::bad_request("duration_seconds exceeds supported range"))?;
        let req = CaptureTimerSetRequest {
            schema: CAPTURE_TIMER_SET_SCHEMA.to_owned(),
            enabled,
            duration_seconds: dur_u32,
        };
        let coordinator = Arc::clone(&self.coordinator);
        run_blocking(move || {
            let status = coordinator
                .set_capture_timer(req)
                .map_err(map_storage_error)?;
            serde_json::to_value(status).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize timer: {err}"))
            })
        })
        .await
    }

    async fn camera_focus(&self) -> ApiResult<Option<Value>> {
        let coordinator = Arc::clone(&self.coordinator);
        run_blocking(move || match coordinator.get_camera_focus() {
            Ok(focus) => serde_json::to_value(focus).map(Some).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize focus: {err}"))
            }),
            Err(StorageError::Unsupported { .. }) => Ok(None),
            Err(err) => Err(map_storage_error(err)),
        })
        .await
    }

    async fn set_camera_focus(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        let req: CameraFocusSetRequest = serde_json::from_value(cmd.body)
            .map_err(|err| ApiError::bad_request(format!("invalid camera focus request: {err}")))?;
        let coordinator = Arc::clone(&self.coordinator);
        let key = cmd.idempotency_key;
        let principal_id = cmd.principal_id;
        run_blocking(move || {
            let outcome = coordinator
                .set_camera_focus_for_principal(req, Some(&key), &principal_id)
                .map_err(map_storage_error)?;
            let body = serde_json::to_value(&outcome.value).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize focus: {err}"))
            })?;
            Ok(CommandOutcome::ok(body, outcome.replayed))
        })
        .await
    }

    async fn list_sessions(&self, query: ListSessionsQuery) -> ApiResult<Value> {
        let catalog = Arc::clone(&self.catalog);
        run_blocking(move || {
            let page = catalog
                .list_sessions_page(
                    Some(query.limit),
                    query.cursor.as_deref(),
                    query.include_recovery,
                    query.take_id.as_deref(),
                )
                .map_err(map_storage_error)?;
            serde_json::to_value(page).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize session list: {err}"))
            })
        })
        .await
    }

    async fn open_session_manifest(&self, session_id: &str) -> ApiResult<RawManifest> {
        let catalog = Arc::clone(&self.catalog);
        let volume = Arc::clone(&self.volume);
        let recordings_root = self.volume.paths().recordings_dir();
        let sid = session_id.to_owned();
        run_blocking(move || {
            if volume.is_volume_released() {
                return Err(ApiError::storage_unavailable(
                    "recording volume has been released for safe swap",
                ));
            }
            let indexed = catalog
                .get_indexed_session(&sid)
                .map_err(map_storage_error)?
                .ok_or_else(|| ApiError::not_found("session not found or not sealed"))?;
            let opened = open_manifest(&recordings_root, &indexed.relative_dir, Some(&sid))
                .map_err(map_storage_error)?;
            Ok(RawManifest {
                bytes: Bytes::from(opened.raw_bytes),
                sha256: opened.sha256,
            })
        })
        .await
    }

    async fn retained_unsuccessful_outcome(&self, session_id: &str) -> ApiResult<Option<Value>> {
        let coordinator = Arc::clone(&self.coordinator);
        let sid = session_id.to_owned();
        run_blocking(move || {
            let out = coordinator
                .retained_unsuccessful_outcome(&sid)
                .map_err(map_storage_error)?;
            match out {
                Some(res) => serde_json::to_value(res).map(Some).map_err(|err| {
                    ApiError::internal(
                        "serialize_failed",
                        format!("serialize unsuccessful outcome: {err}"),
                    )
                }),
                None => Ok(None),
            }
        })
        .await
    }

    async fn artifact_io_busy_state(&self) -> ApiResult<Option<String>> {
        let phase = self.coordinator.phase().map_err(map_storage_error)?;
        let busy = match phase {
            CapturePhase::Starting | CapturePhase::Recording => Some("recording".to_owned()),
            CapturePhase::Stopping | CapturePhase::Recovering => Some("finalizing".to_owned()),
            CapturePhase::Idle
            | CapturePhase::Deleting
            | CapturePhase::Failed
            | CapturePhase::Closing => None,
        };
        Ok(busy)
    }

    async fn open_session_artifact(
        &self,
        session_id: &str,
        artifact_id: &str,
    ) -> ApiResult<OpenRepresentation> {
        let catalog = Arc::clone(&self.catalog);
        let volume = Arc::clone(&self.volume);
        let recordings_root = self.volume.paths().recordings_dir();
        let sid = session_id.to_owned();
        let aid = artifact_id.to_owned();
        run_blocking(move || {
            if volume.is_volume_released() {
                return Err(ApiError::storage_unavailable(
                    "recording volume has been released for safe swap",
                ));
            }
            let indexed = catalog
                .get_indexed_session(&sid)
                .map_err(map_storage_error)?
                .ok_or_else(|| ApiError::not_found("session not found"))?;
            let Some(ref verification) = indexed.verification else {
                return Err(ApiError::session_not_verified("absent"));
            };
            if verification.manifest_sha256 != indexed.manifest_sha256 {
                return Err(ApiError::session_not_verified("stale"));
            }
            if verification.verdict != "usable" {
                let reason = match verification.verdict.as_str() {
                    "absent" => "absent",
                    "stale" => "stale",
                    _ => "unusable",
                };
                return Err(ApiError::session_not_verified(reason));
            }
            let mut verified = open_verified_artifact_checked_with_volume(
                &recordings_root,
                &indexed.relative_dir,
                &aid,
                Some(&sid),
                Some(&verification.manifest_sha256),
                false,
                Some(&volume),
            )
            .map_err(|err| match err {
                StorageError::Integrity {
                    code: "manifest_sha256_mismatch",
                    ..
                } => ApiError::session_not_verified("stale"),
                other => map_storage_error(other),
            })?;
            let lease = verified.take_lease();
            let cloned_file = verified.file().try_clone().map_err(|err| {
                ApiError::provider_failure(format!("clone verified artifact descriptor: {err}"))
            })?;
            let mut repr = OpenRepresentation::from_file(
                verified.descriptor.media_type.clone(),
                verified.descriptor.etag(),
                cloned_file,
                verified.descriptor.bytes,
            );
            if let Some(l) = lease {
                repr = repr.with_lease_guard(l);
            }
            Ok(repr)
        })
        .await
    }

    async fn check_delete_sessions_idempotency(
        &self,
        cmd: &IdempotentCommand,
    ) -> ApiResult<Option<CommandOutcome>> {
        let req: DeleteSessionsRequest = serde_json::from_value(cmd.body.clone())
            .map_err(|err| ApiError::bad_request(format!("invalid delete request: {err}")))?;
        let coordinator = Arc::clone(&self.coordinator);
        let key = cmd.idempotency_key.clone();
        let principal_id = cmd.principal_id.clone();
        run_blocking(move || {
            let Some(replayed) = coordinator
                .lookup_delete_sessions_idempotency(&req, &key, &principal_id)
                .map_err(map_storage_error)?
            else {
                return Ok(None);
            };
            let body = serde_json::to_value(&replayed).map_err(|err| {
                ApiError::internal(
                    "serialize_failed",
                    format!("serialize delete result: {err}"),
                )
            })?;
            Ok(Some(CommandOutcome::ok(body, true)))
        })
        .await
    }

    async fn delete_sessions(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        let req: DeleteSessionsRequest = serde_json::from_value(cmd.body)
            .map_err(|err| ApiError::bad_request(format!("invalid delete request: {err}")))?;
        let coordinator = Arc::clone(&self.coordinator);
        let key = cmd.idempotency_key;
        let principal_id = cmd.principal_id;
        run_blocking(move || {
            let outcome = coordinator
                .delete_sessions_for_principal(req, &key, &principal_id)
                .map_err(map_storage_error)?;
            let body = serde_json::to_value(&outcome.value).map_err(|err| {
                ApiError::internal(
                    "serialize_failed",
                    format!("serialize delete result: {err}"),
                )
            })?;
            Ok(CommandOutcome::ok(body, outcome.replayed))
        })
        .await
    }

    async fn list_logs(&self, query: ListLogsQuery) -> ApiResult<Value> {
        let logs = Arc::clone(&self.logs);
        let volume = Arc::clone(&self.volume);
        run_blocking(move || {
            let volume_id = if volume.is_volume_released() {
                None
            } else {
                volume.read_metadata().ok().map(|m| m.volume_id)
            };
            let list = logs
                .list_logs(query.prefix.as_deref(), Some(query.limit), volume_id)
                .map_err(map_storage_error)?;
            serde_json::to_value(list).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize log list: {err}"))
            })
        })
        .await
    }

    async fn open_log_file(&self, name: &str) -> ApiResult<OpenRepresentation> {
        let volume = Arc::clone(&self.volume);
        let coordinator = Arc::clone(&self.coordinator);
        let logs_dir = self.volume.paths().logs_dir();
        let etag_cache = Arc::clone(&self.log_etag_cache);
        let log_name = name.to_owned();
        run_blocking(move || {
            let Some(lease) = coordinator.try_acquire_open_handle_lease() else {
                return Err(ApiError::storage_unavailable(
                    "diagnostic logs are unavailable while storage volume is unmounted or released",
                ));
            };
            if !volume.is_mounted_and_writable() {
                return Err(ApiError::storage_unavailable(
                    "diagnostic logs are unavailable while storage volume is unmounted or released",
                ));
            }
            let repr = open_safe_log_file_with_cache(&logs_dir, &log_name, &etag_cache)?;
            Ok(repr.with_lease_guard(lease))
        })
        .await
    }

    fn preview_mjpeg_hub(&self) -> Option<Arc<MjpegPreviewHub>> {
        Some(Arc::clone(&self.mjpeg_hub))
    }

    fn preview_h264_hub(&self) -> Option<Arc<H264PreviewHub>> {
        self.h264_hub.as_ref().map(Arc::clone)
    }

    async fn open_preview_mjpeg(&self, _fps: Option<u32>) -> ApiResult<Arc<MjpegPreviewHub>> {
        let hub = self
            .preview_mjpeg_hub()
            .ok_or_else(|| ApiError::preview_unavailable("preview stream is unavailable"))?;
        let coordinator = Arc::clone(&self.coordinator);
        let capture_backend = self.capture_backend.clone();
        run_blocking(move || {
            let phase = coordinator.phase().map_err(map_storage_error)?;
            if matches!(
                phase,
                CapturePhase::Stopping
                    | CapturePhase::Recovering
                    | CapturePhase::Closing
                    | CapturePhase::Failed
            ) {
                return Err(ApiError::conflict(
                    "preview_state_conflict",
                    format!(
                        "preview stream is unavailable while device is in {} state",
                        phase.as_str()
                    ),
                )
                .with_error_code_header());
            }
            if let Some(ref backend) = capture_backend {
                if !backend.is_camera_connected() {
                    return Err(ApiError::camera_not_connected("camera is not connected"));
                }
            }
            Ok(hub)
        })
        .await
    }

    async fn open_preview_h264(
        &self,
        _fps: Option<u32>,
        _bitrate_kbps: Option<u32>,
    ) -> ApiResult<Arc<H264PreviewHub>> {
        let hub = self
            .preview_h264_hub()
            .ok_or_else(ApiError::preview_h264_unsupported)?;
        let coordinator = Arc::clone(&self.coordinator);
        let capture_backend = self.capture_backend.clone();
        run_blocking(move || {
            let phase = coordinator.phase().map_err(map_storage_error)?;
            if matches!(
                phase,
                CapturePhase::Stopping
                    | CapturePhase::Recovering
                    | CapturePhase::Closing
                    | CapturePhase::Failed
            ) {
                return Err(ApiError::conflict(
                    "preview_state_conflict",
                    format!(
                        "preview stream is unavailable while device is in {} state",
                        phase.as_str()
                    ),
                )
                .with_error_code_header());
            }
            if let Some(ref backend) = capture_backend {
                if !backend.is_camera_connected() {
                    return Err(ApiError::camera_not_connected("camera is not connected"));
                }
            }
            Ok(hub)
        })
        .await
    }

    fn capture_event_buffer(&self) -> Arc<EventReplayBuffer> {
        Arc::clone(&self.capture_events)
    }

    fn network_event_buffer(&self) -> Arc<NetworkEventReplayBuffer> {
        Arc::clone(&self.network_events)
    }

    async fn clock_status(&self, principal_id: &str) -> ApiResult<Value> {
        let clock = Arc::clone(&self.clock);
        let principal = principal_id.to_owned();
        run_blocking(move || clock.status_blocking(&principal, false)).await
    }

    async fn clock_inspect(&self, principal_id: &str) -> ApiResult<Value> {
        let clock = Arc::clone(&self.clock);
        let principal = principal_id.to_owned();
        run_blocking(move || clock.status_blocking(&principal, true)).await
    }

    async fn clock_sync(&self, principal_id: &str, body: Value) -> ApiResult<Value> {
        if self
            .coordinator
            .phase()
            .map_err(map_storage_error)?
            .is_busy()
        {
            return Err(ApiError::conflict(
                "capture_active",
                "clock sync is prohibited while capture is active",
            )
            .with_error_code_header());
        }
        let challenge = body
            .get("challenge")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::bad_request("missing clock challenge"))?
            .to_owned();
        let unix_time_ms = body
            .get("unix_time_ms")
            .and_then(Value::as_u64)
            .ok_or_else(|| ApiError::bad_request("missing unix_time_ms"))?;
        let replace = body
            .get("replace")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let clock = Arc::clone(&self.clock);
        let coordinator = Arc::clone(&self.coordinator);
        let principal = principal_id.to_owned();
        run_blocking(move || {
            clock.sync_blocking(&principal, &challenge, unix_time_ms, replace, move || {
                if coordinator.phase().map_err(map_storage_error)?.is_busy() {
                    Err(ApiError::conflict(
                        "capture_active",
                        "clock sync is prohibited while capture is active",
                    )
                    .with_error_code_header())
                } else {
                    Ok(())
                }
            })
        })
        .await
    }

    async fn voice_status(&self) -> ApiResult<Value> {
        if let Some(voice) = self.config.voice_controller.clone() {
            return run_blocking(move || {
                let status = voice.status().map_err(map_hw_error)?;
                serde_json::to_value(status).map_err(|err| {
                    ApiError::internal("serialize_failed", format!("serialize voice status: {err}"))
                })
            })
            .await;
        }
        if self.config.mock_mode {
            let volume = self.voice_fallback_volume.lock().map(|g| *g).unwrap_or(80);
            return Ok(json!({
                "schema": miro_ego_hw::VOICE_STATUS_SCHEMA,
                "enabled": volume > 0,
                "volume": volume,
            }));
        }
        Ok(json!({
            "schema": miro_ego_hw::VOICE_STATUS_SCHEMA,
            "enabled": false,
            "volume": 0,
        }))
    }

    async fn set_voice_volume(&self, volume: u8) -> ApiResult<Value> {
        if self
            .coordinator
            .phase()
            .map_err(map_storage_error)?
            .is_busy()
        {
            return Err(ApiError::conflict(
                "capture_active",
                "voice volume cannot be modified while capture is active",
            )
            .with_error_code_header());
        }
        if let Some(voice) = self.config.voice_controller.clone() {
            return run_blocking(move || {
                let status = voice.set_volume(volume).map_err(map_hw_error)?;
                serde_json::to_value(status).map_err(|err| {
                    ApiError::internal("serialize_failed", format!("serialize voice status: {err}"))
                })
            })
            .await;
        }
        if !self.config.mock_mode {
            return Err(ApiError::service_unavailable(
                "voice_unavailable",
                "voice prompt controller is unavailable; volume cannot be persisted",
            )
            .with_header("Retry-After", "1")
            .with_error_code_header());
        }
        if let Ok(mut guard) = self.voice_fallback_volume.lock() {
            *guard = volume;
        }
        Ok(json!({
            "schema": miro_ego_hw::VOICE_STATUS_SCHEMA,
            "enabled": volume > 0,
            "volume": volume,
        }))
    }

    async fn network_status(&self) -> ApiResult<Value> {
        let client = self.network_probe_client.clone();
        let device_id = self.config.device_config.device.device_id.clone();
        let port = self.config.device_config.listen.port;
        let scheme = self.config.mdns_scheme();
        let status_json = run_blocking(move || {
            let val = match client {
                Some(ref c) => match c.status() {
                    Ok(st) => controller_status_to_openapi(&st, &device_id, port, scheme),
                    Err(_) => degraded_network_status_with_scheme(
                        &device_id,
                        port,
                        scheme,
                        "controller_unavailable",
                    ),
                },
                None => {
                    degraded_network_status_with_scheme(&device_id, port, scheme, "not_enabled")
                }
            };
            Ok(val)
        })
        .await?;
        let _ = self.network_events.observe(status_json.clone());
        Ok(status_json)
    }

    async fn network_scan(&self) -> ApiResult<Value> {
        if self
            .coordinator
            .phase()
            .map_err(map_storage_error)?
            .is_busy()
        {
            return Err(ApiError::service_unavailable(
                "capture_active",
                "Wi-Fi scan is temporarily unavailable while capture is active",
            )
            .with_header("Retry-After", "1")
            .with_error_code_header());
        }
        let client = self.require_network_scan_client()?;
        run_blocking(move || {
            let scan = client.scan().map_err(|err| match err {
                NetworkError::Busy { ref message } | NetworkError::Backend { ref message }
                    if err.error_code() == Some("capture_active")
                        || message.contains("capture_active") =>
                {
                    ApiError::service_unavailable("capture_active", message.clone())
                        .with_header("Retry-After", "1")
                        .with_error_code_header()
                }
                other => map_network_error(other),
            })?;
            serde_json::to_value(scan).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize network scan: {err}"))
            })
        })
        .await
    }

    async fn create_network_credential(
        &self,
        principal_id: &str,
        passphrase: &str,
    ) -> ApiResult<Value> {
        if self
            .coordinator
            .phase()
            .map_err(map_storage_error)?
            .is_busy()
        {
            return Err(network_mutation_unavailable(
                "capture_active",
                "network credential creation is unavailable while capture is active",
            ));
        }
        let client = self.require_network_mutation_client()?;
        let principal = principal_id.to_owned();
        let pass = passphrase.to_owned();
        run_blocking(move || {
            let receipt = client
                .create_credential(&principal, &pass)
                .map_err(map_network_mutation_error)?;
            serde_json::to_value(receipt).map_err(|err| {
                ApiError::internal(
                    "serialize_failed",
                    format!("serialize network credential receipt: {err}"),
                )
            })
        })
        .await
    }

    async fn apply_network_desired_state(
        &self,
        cmd: IdempotentCommand,
    ) -> ApiResult<CommandOutcome> {
        if self
            .coordinator
            .phase()
            .map_err(map_storage_error)?
            .is_busy()
        {
            return Err(network_mutation_unavailable(
                "capture_active",
                "network changes are unavailable while capture is active",
            ));
        }
        let desired_val = cmd
            .body
            .get("desired")
            .cloned()
            .ok_or_else(|| ApiError::bad_request("missing desired network state"))?;
        let desired: ApplyDesiredState = serde_json::from_value(desired_val).map_err(|err| {
            ApiError::bad_request(format!("invalid desired network state: {err}"))
        })?;
        let client = self.require_network_mutation_client()?;
        let flush_client = self
            .network_probe_client
            .clone()
            .unwrap_or_else(|| client.clone());
        let principal = cmd.principal_id;
        let key = cmd.idempotency_key;

        let outcome = run_blocking(move || {
            let res = client
                .apply(&principal, &key, &desired)
                .map_err(map_network_mutation_error)?;
            let body = serde_json::to_value(&res.receipt).map_err(|err| {
                ApiError::internal(
                    "serialize_failed",
                    format!("serialize network receipt: {err}"),
                )
            })?;
            Ok(CommandOutcome::accepted(body, res.replayed))
        })
        .await?;
        if !outcome.replayed {
            spawn_deferred_network_response_flush(flush_client);
        }
        Ok(outcome)
    }

    async fn retry_network_transaction(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        if self
            .coordinator
            .phase()
            .map_err(map_storage_error)?
            .is_busy()
        {
            return Err(network_mutation_unavailable(
                "capture_active",
                "network retry is unavailable while capture is active",
            ));
        }
        let tx_id = cmd
            .body
            .get("transaction_id")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::bad_request("missing transaction_id"))?
            .to_owned();
        let client = self.require_network_mutation_client()?;
        let flush_client = self
            .network_probe_client
            .clone()
            .unwrap_or_else(|| client.clone());
        let principal = cmd.principal_id;
        let key = cmd.idempotency_key;

        let outcome = run_blocking(move || {
            let res = client.retry(&principal, &key, &tx_id).map_err(|err| {
                if err.error_code() == Some("transaction_not_found")
                    || err.to_string().contains("transaction_not_found")
                {
                    ApiError::network_transaction_not_found(&tx_id)
                } else {
                    map_network_mutation_error(err)
                }
            })?;
            let body = serde_json::to_value(&res.receipt).map_err(|err| {
                ApiError::internal(
                    "serialize_failed",
                    format!("serialize retry receipt: {err}"),
                )
            })?;
            Ok(CommandOutcome::accepted(body, res.replayed))
        })
        .await?;
        if !outcome.replayed {
            spawn_deferred_network_response_flush(flush_client);
        }
        Ok(outcome)
    }

    async fn forget_network_client_profile(
        &self,
        cmd: IdempotentCommand,
    ) -> ApiResult<CommandOutcome> {
        if self
            .coordinator
            .phase()
            .map_err(map_storage_error)?
            .is_busy()
        {
            return Err(network_mutation_unavailable(
                "capture_active",
                "forgetting Wi-Fi profile is unavailable while capture is active",
            ));
        }
        let client = self.require_network_mutation_client()?;
        let flush_client = self
            .network_probe_client
            .clone()
            .unwrap_or_else(|| client.clone());
        let principal = cmd.principal_id;
        let key = cmd.idempotency_key;

        let outcome = run_blocking(move || {
            let res = client
                .forget(&principal, &key)
                .map_err(map_network_mutation_error)?;
            let body = serde_json::to_value(&res.receipt).map_err(|err| {
                ApiError::internal(
                    "serialize_failed",
                    format!("serialize forget receipt: {err}"),
                )
            })?;
            Ok(CommandOutcome::accepted(body, res.replayed))
        })
        .await?;
        if !outcome.replayed {
            spawn_deferred_network_response_flush(flush_client);
        }
        Ok(outcome)
    }

    async fn firmware_status(&self) -> ApiResult<Value> {
        let (checked_at, available, task) = self
            .firmware
            .lock()
            .map(|g| (g.checked_at, g.available.clone(), g.task.clone()))
            .map_err(|_| ApiError::internal("firmware_poisoned", "firmware state poisoned"))?;
        let deploy_client = self.deploy_status_client.clone();
        let release_manager = Arc::clone(&self.release_manager);
        let fallback_commit = self.config.software_commit.clone();
        run_blocking(move || {
            Ok(build_firmware_status_payload(
                deploy_client.as_ref(),
                &release_manager,
                &fallback_commit,
                checked_at,
                available,
                task,
            ))
        })
        .await
    }

    async fn firmware_check(&self, force: bool) -> ApiResult<Value> {
        if !force {
            let cached = self
                .firmware
                .lock()
                .map_err(|_| ApiError::internal("firmware_poisoned", "firmware state poisoned"))
                .map(|g| {
                    if g.checked_at.is_some()
                        && g.checked_instant
                            .is_some_and(|ts| ts.elapsed() < FIRMWARE_CHECK_CACHE_TTL)
                    {
                        Some((g.checked_at, g.available.clone(), g.task.clone()))
                    } else {
                        None
                    }
                })?;
            if let Some((checked_at, available, task)) = cached {
                let deploy_client = self.deploy_status_client.clone();
                let release_manager = Arc::clone(&self.release_manager);
                let fallback_commit = self.config.software_commit.clone();
                return run_blocking(move || {
                    Ok(build_firmware_status_payload(
                        deploy_client.as_ref(),
                        &release_manager,
                        &fallback_commit,
                        checked_at,
                        available,
                        task,
                    ))
                })
                .await;
            }
        }

        let now = now_unix_secs();
        let volume = Arc::clone(&self.volume);
        let updates_dir = self.volume.paths().updates_dir();
        let deploy_client = self.deploy_status_client.clone();
        let release_manager = Arc::clone(&self.release_manager);
        let fallback_commit = self.config.software_commit.clone();

        let (discovered_available, deploy, rpc_task) = run_blocking(move || {
            let (deploy, rpc_task) = match deploy_client {
                Some(ref rpc) => match rpc.status() {
                    Ok(ctrl) => (
                        Some(ctrl.deployment),
                        ctrl.task.and_then(|t| serde_json::to_value(t).ok()),
                    ),
                    Err(_) => (release_manager.status().ok(), None),
                },
                None => (release_manager.status().ok(), None),
            };
            let current_commit = deploy
                .as_ref()
                .and_then(|d| d.current.clone())
                .unwrap_or_else(|| fallback_commit.clone());
            let packages = if !volume.is_volume_released() && volume.is_mounted_and_writable() {
                DefaultOfflinePackageHook
                    .discover_packages(&updates_dir)
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            let next_pkg = packages
                .into_iter()
                .find(|pkg| pkg.commit != current_commit)
                .map(|pkg| {
                    json!({
                        "version": pkg.version,
                        "commit": pkg.commit,
                    })
                });
            Ok((next_pkg, deploy, rpc_task))
        })
        .await?;

        let fallback_task = {
            let mut guard = self
                .firmware
                .lock()
                .map_err(|_| ApiError::internal("firmware_poisoned", "firmware state poisoned"))?;
            guard.checked_at = Some(now);
            guard.checked_instant = Some(Instant::now());
            guard.available = discovered_available.clone();
            if let Some(ref t) = rpc_task {
                guard.task = Some(t.clone());
            }
            guard.task.clone()
        };

        Ok(format_firmware_status_from_parts(
            deploy,
            &self.config.software_commit,
            Some(now),
            discovered_available,
            rpc_task.or(fallback_task),
        ))
    }

    async fn firmware_update(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        if self
            .coordinator
            .phase()
            .map_err(map_storage_error)?
            .is_busy()
        {
            return Err(ApiError::conflict(
                "capture_active",
                "firmware update is prohibited while capture is active",
            )
            .with_error_code_header());
        }
        let commit = cmd
            .body
            .get("commit")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::bad_request("missing commit"))?
            .to_owned();
        validate_commit_sha(&commit).map_err(map_deploy_error)?;

        let Some(rpc) = self.deploy_client.clone() else {
            return Err(ApiError::service_unavailable(
                "firmware_unavailable",
                "privileged firmware deploy controller is unavailable",
            )
            .with_header("Retry-After", "1")
            .with_error_code_header());
        };

        let volume = Arc::clone(&self.volume);
        let staged_bundle = self.volume.paths().updates_dir().join(&commit);
        let commit_for_task = commit.clone();
        let principal_for_rpc = cmd.principal_id.clone();
        let idem_for_rpc = cmd.idempotency_key.clone();
        let req_hash = hex_encode(Sha256::digest(&cmd.canonical_body).as_slice());
        let (task, rpc_replayed) = run_blocking(move || {
            let bundle_meta = fs::symlink_metadata(&staged_bundle);
            if let Ok(ref meta) = bundle_meta {
                if meta.file_type().is_symlink() || !meta.is_dir() {
                    return Err(ApiError::unprocessable(
                        "invalid_firmware_bundle",
                        "staged firmware bundle path must be a non-symlink directory",
                    )
                    .with_error_code_header());
                }
            } else if volume.is_volume_released() || !volume.is_mounted_and_writable() {
                return Err(ApiError::storage_unavailable(
                    "recording volume is unavailable or released for safe swap",
                ));
            }
            let out = match rpc.install_idempotent(
                &staged_bundle,
                &commit_for_task,
                true,
                Some(&principal_for_rpc),
                Some(&idem_for_rpc),
                Some(&req_hash),
            ) {
                Ok(out) => out,
                Err(DeployError::InvalidBundle { .. }) if bundle_meta.is_err() => {
                    return Err(ApiError::conflict(
                        "update_not_staged",
                        format!(
                            "offline firmware bundle for commit {commit_for_task} is not staged under /data/updates/"
                        ),
                    )
                    .with_error_code_header());
                }
                Err(err) => return Err(map_deploy_error(err)),
            };
            if !out.replayed && (volume.is_volume_released() || !volume.is_mounted_and_writable()) {
                return Err(ApiError::storage_unavailable(
                    "recording volume is unavailable or released for safe swap",
                ));
            }
            let val = serde_json::to_value(out.task).map_err(|err| {
                ApiError::internal("serialize_failed", format!("serialize firmware task: {err}"))
            })?;
            Ok((val, out.replayed))
        })
        .await?;

        let mut guard = self
            .firmware
            .lock()
            .map_err(|_| ApiError::internal("firmware_poisoned", "firmware state poisoned"))?;
        guard.checked_instant = None;
        guard.task = Some(task.clone());
        Ok(CommandOutcome::accepted(task, rpc_replayed))
    }

    async fn firmware_rollback(&self, cmd: IdempotentCommand) -> ApiResult<CommandOutcome> {
        if self
            .coordinator
            .phase()
            .map_err(map_storage_error)?
            .is_busy()
        {
            return Err(ApiError::conflict(
                "capture_active",
                "firmware rollback is prohibited while capture is active",
            )
            .with_error_code_header());
        }
        let commit = cmd
            .body
            .get("commit")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::bad_request("missing commit"))?
            .to_owned();
        validate_commit_sha(&commit).map_err(map_deploy_error)?;

        let Some(rpc) = self.deploy_client.clone() else {
            return Err(ApiError::service_unavailable(
                "firmware_unavailable",
                "privileged firmware deploy controller is unavailable",
            )
            .with_header("Retry-After", "1")
            .with_error_code_header());
        };

        let commit_for_task = commit.clone();
        let principal_for_rpc = cmd.principal_id.clone();
        let idem_for_rpc = cmd.idempotency_key.clone();
        let req_hash = hex_encode(Sha256::digest(&cmd.canonical_body).as_slice());
        let (task, rpc_replayed) = run_blocking(move || {
            let out = rpc
                .rollback_idempotent(
                    &commit_for_task,
                    true,
                    Some(&principal_for_rpc),
                    Some(&idem_for_rpc),
                    Some(&req_hash),
                )
                .map_err(map_deploy_error)?;
            let val = serde_json::to_value(out.task).map_err(|err| {
                ApiError::internal(
                    "serialize_failed",
                    format!("serialize firmware task: {err}"),
                )
            })?;
            Ok((val, out.replayed))
        })
        .await?;

        let mut guard = self
            .firmware
            .lock()
            .map_err(|_| ApiError::internal("firmware_poisoned", "firmware state poisoned"))?;
        guard.checked_instant = None;
        guard.task = Some(task.clone());
        Ok(CommandOutcome::accepted(task, rpc_replayed))
    }

    fn record_audit(&self, event: AuditEvent) -> ApiResult<()> {
        let _ = self.record_audit_durable(event)?;
        Ok(())
    }

    fn record_audit_durable(&self, event: AuditEvent) -> ApiResult<bool> {
        let storage_event = ApiAuditEvent {
            schema: event.schema,
            recorded_at: event.recorded_at,
            request_id: event.request_id,
            principal_id: event.principal_id,
            operation_id: event.operation_id,
            poll_count: None,
            resource_id: event.resource_id,
            outcome: event.outcome,
        };

        let volume_durable =
            !self.volume.is_volume_released() && self.volume.is_mounted_and_writable();
        let is_poll = volume_durable
            && is_coalesceable_poll_operation(&storage_event.operation_id)
            && storage_event.outcome == "allowed"
            && storage_event.resource_id.is_none();

        let mut to_write: Vec<ApiAuditEvent> = Vec::with_capacity(2);

        if let Ok(mut guard) = self.audit_coalescer.lock() {
            let now = Instant::now();
            guard
                .seen_initial
                .retain(|_, ts| now.saturating_duration_since(*ts) < AUDIT_POLL_COALESCE_WINDOW);

            if is_poll {
                let key = (
                    storage_event.principal_id.clone(),
                    storage_event.operation_id.clone(),
                );
                let had_initial = guard.seen_initial.contains_key(&key);
                if !had_initial {
                    if let Some(mut prev) = guard.pending.take() {
                        prev.event.poll_count = Some(prev.count);
                        to_write.push(prev.event);
                    }
                    guard.seen_initial.insert(key, now);
                    to_write.push(storage_event);
                } else {
                    match guard.pending.as_mut() {
                        Some(pending)
                            if pending.event.principal_id == storage_event.principal_id
                                && pending.event.operation_id == storage_event.operation_id
                                && now.saturating_duration_since(pending.started_at)
                                    < AUDIT_POLL_COALESCE_WINDOW
                                && pending.count < AUDIT_POLL_COALESCE_MAX_COUNT =>
                        {
                            pending.count += 1;
                            pending.event.recorded_at = storage_event.recorded_at;
                            pending.event.request_id = storage_event.request_id;
                        }
                        _ => {
                            if let Some(mut prev) = guard.pending.take() {
                                prev.event.poll_count = Some(prev.count);
                                to_write.push(prev.event);
                            }
                            guard.pending = Some(PendingAuditPoll {
                                event: storage_event,
                                count: 1,
                                started_at: now,
                            });
                        }
                    }
                }
            } else {
                if let Some(mut prev) = guard.pending.take() {
                    prev.event.poll_count = Some(prev.count);
                    to_write.push(prev.event);
                }
                to_write.push(storage_event);
            }
        } else {
            to_write.push(storage_event);
        }

        let mut durable = volume_durable;
        for ev in &to_write {
            durable = self.write_storage_audit_event(ev)?;
        }
        Ok(durable)
    }

    fn get_web_asset(&self, name: &str) -> Option<StaticAsset> {
        self.web_assets.get(name).cloned()
    }
}
