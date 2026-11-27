//! Firmware status formatting and check caching for `/api/v1/firmware*`.

use miro_ego_deploy::{DeployRpcClient, ReleaseManager};
use serde_json::{Value, json};
use std::time::{Duration, Instant};

pub(super) const FIRMWARE_CHECK_CACHE_TTL: Duration = Duration::from_secs(30);

#[derive(Default)]
pub(super) struct FirmwareRuntimeState {
    pub checked_at: Option<u64>,
    pub checked_instant: Option<Instant>,
    pub available: Option<Value>,
    pub task: Option<Value>,
}

pub(super) fn build_firmware_status_payload(
    deploy_client: Option<&DeployRpcClient>,
    release_manager: &ReleaseManager,
    fallback_commit: &str,
    checked_at: Option<u64>,
    available: Option<Value>,
    fallback_task: Option<Value>,
) -> Value {
    let (deploy, task) = match deploy_client {
        Some(rpc) => match rpc.status() {
            Ok(ctrl) => (
                Some(ctrl.deployment),
                ctrl.task
                    .and_then(|t| serde_json::to_value(t).ok())
                    .or(fallback_task),
            ),
            Err(_) => (release_manager.status().ok(), fallback_task),
        },
        None => (release_manager.status().ok(), fallback_task),
    };
    format_firmware_status_from_parts(deploy, fallback_commit, checked_at, available, task)
}

pub(super) fn format_firmware_status_from_parts(
    deploy: Option<miro_ego_deploy::DeploymentStatus>,
    fallback_commit: &str,
    checked_at: Option<u64>,
    available: Option<Value>,
    task: Option<Value>,
) -> Value {
    let current_version = deploy
        .as_ref()
        .and_then(|d| d.current_version.clone())
        .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned());
    let current_commit = deploy
        .as_ref()
        .and_then(|d| d.current.clone())
        .unwrap_or_else(|| fallback_commit.to_owned());
    let previous = deploy.as_ref().and_then(|d| {
        let commit = d.previous.clone()?;
        let version = d
            .previous_version
            .clone()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned());
        Some(json!({
            "version": version,
            "commit": commit,
        }))
    });
    let warning = deploy
        .as_ref()
        .and_then(|d| d.activation_failure.as_ref().map(|f| f.message.clone()));

    let filtered_available = available.filter(|av| {
        av.get("commit")
            .and_then(Value::as_str)
            .is_some_and(|c| c != current_commit)
    });
    let has_update = filtered_available.is_some();

    json!({
        "schema": miro_ego_deploy::FIRMWARE_STATUS_SCHEMA,
        "supported": true,
        "current": {
            "version": current_version,
            "commit": current_commit,
        },
        "previous": previous,
        "available": filtered_available.unwrap_or(Value::Null),
        "has_update": has_update,
        "checked_at": checked_at,
        "warning": warning,
        "task": task,
    })
}
