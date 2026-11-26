"""Closed source-event, status, descriptor, session-list, and outcome validators."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Collection, Mapping
from copy import deepcopy
from datetime import datetime
from importlib.resources import files

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

from rp_ylx.api.compat import facade_attr
from rp_ylx.api.validators.network import (
    INTERFACE_NAME as INTERFACE_NAME,
)
from rp_ylx.api.validators.network import (
    valid_datetime,
    valid_network_interface,
)
from rp_ylx.camera_focus import CAMERA_FOCUS_KEYS, valid_camera_focus_status
from rp_ylx.core.ids import (
    SHA256_HEX_RE,
    UUID_V4_RE,
    UUID_V7_RE,
    parse_rfc3339,
    valid_sha256,
    valid_uuid4,
    valid_uuid7,
)


class InvalidSourceEvent(ValueError):
    """daemon source event 不是冻结契约定义的闭合事件。"""


SOURCE_EVENT_KEYS = frozenset(
    {"authority_epoch", "source_revision", "type", "occurred_at", "session_id", "data"}
)
SOURCE_EVENT_TYPES = frozenset({"snapshot", "state", "progress", "diagnostic", "safe_swap"})
SNAPSHOT_KEYS = frozenset(
    {"schema", "device_state", "active_recording", "retained_unsuccessful", "runtime"}
)
STATE_KEYS = frozenset({"schema", "state", "volume_id", "generation_id"})
PROGRESS_KEYS = frozenset(
    {"schema", "phase", "elapsed_seconds", "completed_units", "total_units", "unit"}
)
DIAGNOSTIC_EVENT_KEYS = frozenset({"schema", "diagnostic"})
RUNTIME_BASE_KEYS = frozenset(
    {
        "observed_at",
        "connection_method",
        "temperature_celsius",
        "network",
        "live_imu",
    }
)
RUNTIME_SOURCE_KEYS = frozenset(
    {
        "observed_at",
        "connection_method",
        "temperature_celsius",
        "network",
        "live_imu",
        "camera",
        "camera_focus",
    }
)
SOURCE_API_VERSION = "source"
CAPTURE_STATUS_SCHEMA_V2 = "ylx.capture-status.v2"
CAPTURE_STATUS_SCHEMA_V4 = "ylx.capture-status.v4"
CAPTURE_SNAPSHOT_SCHEMA_V2 = "ylx.capture-snapshot-event.v2"
CAPTURE_SNAPSHOT_SCHEMA_V4 = "ylx.capture-snapshot-event.v4"
CAPTURE_ACTIVE_STATES = frozenset({"recording", "finalizing", "encoding", "verifying"})
CALIBRATION_CAPTURE_DISABLED_REASONS = frozenset(
    {
        "capture_source_unsupported",
        "storage_unavailable",
        "hardware_unavailable",
        "maintenance_or_capture_busy",
    }
)
CAPTURE_DEVICE_STATES = frozenset({"idle", *CAPTURE_ACTIVE_STATES, "blocked"})
CAPTURE_TERMINAL_STATES = frozenset({"recoverable", "failed", "abandoned"})
CAPTURE_RECORDING_STATES = frozenset({*CAPTURE_ACTIVE_STATES, *CAPTURE_TERMINAL_STATES})
NETWORK_KEYS = frozenset({"ap", "wifi_client", "wired", "default_route"})
NETWORK_INTERFACE_KEYS = frozenset({"state", "interface", "addresses", "peer_or_ssid"})
LIVE_IMU_KEYS = frozenset({"session_id", "clock", "raw", "sync"})
LIVE_IMU_RAW_KEYS = frozenset({"units", "accelerometer", "gyroscope"})
LIVE_IMU_SYNC_KEYS = frozenset({"quality"})
SUPPORTED_DEVICE_API_VERSIONS = frozenset({"v2", "v3", "v4"})
LEGACY_DEVICE_API_VERSIONS = frozenset({"v2", "v3"})
SAFE_SWAP_V3_KEYS = frozenset(
    {
        "schema",
        "session_id",
        "volume_id",
        "generation_id",
        "manifest_id",
        "manifest_sha256",
        "sealed_at",
        "released_at",
        "release_state",
        "open_handle_count",
    }
)
UUID_V4 = UUID_V4_RE
UUID_V7 = UUID_V7_RE
SHA256 = SHA256_HEX_RE
DIAGNOSTIC_CODE = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")
GATEWAY_VERIFICATION_DIAGNOSTIC_CODES = frozenset(
    {
        "artifact_digest_mismatch",
        "artifact_invalid",
        "manifest_invalid",
        "verification_failed",
        "recording_interrupted",
    }
)
_FORMAT_CHECKER = FormatChecker()
_FORMAT_CHECKER.checks("date-time", ValueError)(
    lambda value: not isinstance(value, str) or bool(parse_rfc3339(value))
)


_RECORDING_STATE_SCHEMA = json.loads(
    files("rp_ylx").joinpath("schemas", "ylx-recording-state-v1.schema.json").read_bytes()
)
_RECORDING_STATE_VALIDATOR = Draft202012Validator(
    _RECORDING_STATE_SCHEMA,
    format_checker=_FORMAT_CHECKER,
)
_DIAGNOSTIC_VALIDATOR = Draft202012Validator(
    _RECORDING_STATE_SCHEMA["$defs"]["diagnostic"],
    format_checker=_FORMAT_CHECKER,
)


def _event_schema_version(api_version: str) -> str:
    if api_version not in SUPPORTED_DEVICE_API_VERSIONS:
        raise ValueError("api_version 必须是 v2、v3 或 v4")
    return api_version


def project_capture_status(status: object, *, api_version: str) -> object:
    """Project the canonical daemon capture status into the requested Device API major."""
    _event_schema_version(api_version)
    projected = deepcopy(status)
    if isinstance(projected, dict):
        if projected.get("schema") == CAPTURE_STATUS_SCHEMA_V2:
            projected["schema"] = _capture_status_schema_for_api(api_version)
        snapshot = projected.get("snapshot")
        if isinstance(snapshot, dict):
            _project_snapshot_data(snapshot, api_version=api_version)
    return projected


def project_device_descriptor(descriptor: object, *, api_version: str) -> object:
    """Project the canonical provider device descriptor into the requested Device API major."""
    _event_schema_version(api_version)
    projected = deepcopy(descriptor)
    if isinstance(projected, dict):
        runtime = projected.get("runtime")
        if isinstance(runtime, dict):
            _project_runtime(runtime, api_version=api_version)
    return projected


def project_source_event(
    source_event: Mapping[str, object],
    *,
    api_version: str,
) -> Mapping[str, object]:
    """Project a canonical source event into the requested SSE CaptureEvent major."""
    _event_schema_version(api_version)
    projected = deepcopy(dict(source_event))
    if projected.get("type") == "snapshot":
        data = projected.get("data")
        if isinstance(data, dict):
            _project_snapshot_data(data, api_version=api_version)
    return projected


def validate_capture_status(status: object, *, api_version: str = SOURCE_API_VERSION) -> None:
    """验证 HTTP 与命令响应共用的权威 capture status。"""
    _validate_source_or_api_version(api_version)
    _validate_finite_json(status)
    if not isinstance(status, Mapping) or set(status) != {
        "schema",
        "authority_epoch",
        "source_revision",
        "snapshot",
    }:
        raise InvalidSourceEvent("capture status 必须是闭合对象")
    authority_epoch = status["authority_epoch"]
    source_revision = status["source_revision"]
    snapshot = status["snapshot"]
    if (
        status["schema"] != _capture_status_schema_for_api(api_version)
        or not valid_uuid4(authority_epoch)
        or isinstance(source_revision, bool)
        or not isinstance(source_revision, int)
        or source_revision < 0
        or not isinstance(snapshot, Mapping)
    ):
        raise InvalidSourceEvent("capture status 基础字段无效")

    source_session_id: object = None
    active = snapshot.get("active_recording")
    if isinstance(active, Mapping):
        recording_state = active.get("recording_state")
        if isinstance(recording_state, Mapping):
            source_session_id = recording_state.get("session_id")
    _validate_snapshot_data(
        snapshot,
        authority_epoch,
        source_revision,
        source_session_id,
        api_version=api_version,
    )


def validate_device_descriptor(
    descriptor: object,
    *,
    api_version: str,
    security_profile: str,
) -> None:
    """验证 provider 返回的闭合设备描述。"""
    _validate_finite_json(descriptor)
    if not isinstance(descriptor, Mapping) or set(descriptor) != {
        "schema",
        "device",
        "hardware_fingerprint",
        "api_version",
        "build",
        "security_profile",
        "capabilities",
        "storage",
        "runtime",
    }:
        raise InvalidSourceEvent("device descriptor 必须是闭合对象")
    expected_version = api_version.removeprefix("v")
    fingerprint = descriptor["hardware_fingerprint"]
    if (
        api_version not in SUPPORTED_DEVICE_API_VERSIONS
        or descriptor["schema"] != f"ylx.device.{api_version}"
        or descriptor["api_version"] != f"{expected_version}.0"
        or descriptor["security_profile"] != security_profile
        or security_profile not in {"customer", "lab"}
        or not isinstance(fingerprint, str)
        or not fingerprint.startswith("sha256:")
        or not valid_sha256(fingerprint.removeprefix("sha256:"))
    ):
        raise InvalidSourceEvent("device descriptor 身份或版本无效")
    _validate_device_identity(descriptor["device"])
    _validate_build(descriptor["build"])
    _validate_capabilities(descriptor["capabilities"], api_version=api_version)
    _validate_device_storage(descriptor["storage"])
    _validate_runtime(descriptor["runtime"], api_version=api_version)


def validate_session_summary(value: object) -> None:
    """Validate one canonical v4 summary before admitting a persisted cache row."""
    _validate_finite_json(value)
    _validate_session_summary(value, take_id=None, api_version="v4")


def validate_session_list(
    resource: object,
    *,
    limit: int,
    take_id: str | None,
    api_version: str = "v3",
) -> None:
    """验证 provider 返回的闭合会话页。"""
    _validate_finite_json(resource)
    expected = {
        "schema",
        "items",
        "diagnostics",
        "next_cursor",
    }
    if api_version == "v4":
        expected.add("catalog_revision")
    if (
        api_version not in SUPPORTED_DEVICE_API_VERSIONS
        or not isinstance(resource, Mapping)
        or set(resource) != expected
    ):
        raise InvalidSourceEvent("session list 必须是闭合对象")
    items = resource["items"]
    diagnostics = resource["diagnostics"]
    next_cursor = resource["next_cursor"]
    expected_schema = "ylx.session-list.v3" if api_version == "v4" else "ylx.session-list.v2"
    catalog_revision = resource.get("catalog_revision")
    if (
        resource["schema"] != expected_schema
        or (
            api_version == "v4"
            and (
                not isinstance(catalog_revision, str)
                or not catalog_revision.startswith("sha256:")
                or not valid_sha256(catalog_revision.removeprefix("sha256:"))
            )
        )
        or not isinstance(items, list)
        or not isinstance(diagnostics, list)
        or len(items) + len(diagnostics) > limit
        or (next_cursor is not None and (not isinstance(next_cursor, str) or not next_cursor))
    ):
        raise InvalidSourceEvent("session list 基础字段无效")
    for item in items:
        _validate_session_summary(item, take_id=take_id, api_version=api_version)
    if len({item["session_id"] for item in items}) != len(items):
        raise InvalidSourceEvent("session list 不得重复会话")
    order = [(_date_time_value(item["started_at"]), item["session_id"]) for item in items]
    if order != sorted(order, reverse=True):
        raise InvalidSourceEvent("session list 排序无效")
    for diagnostic in diagnostics:
        _validate_session_discovery_diagnostic(diagnostic)


def validate_retained_unsuccessful_outcome(resource: object, *, session_id: str) -> None:
    """验证按会话读取的未成功终态资源。"""
    _validate_finite_json(resource)
    if not isinstance(resource, Mapping) or set(resource) != {
        "schema",
        "authority_epoch",
        "source_revision",
        "outcome",
    }:
        raise InvalidSourceEvent("retained outcome 必须是闭合对象")
    authority_epoch = resource["authority_epoch"]
    source_revision = resource["source_revision"]
    if (
        resource["schema"] != "ylx.retained-unsuccessful-session-resource.v2"
        or not valid_uuid4(authority_epoch)
        or type(source_revision) is not int
        or source_revision < 0
    ):
        raise InvalidSourceEvent("retained outcome 基础字段无效")
    _validate_snapshot_recording(
        resource["outcome"],
        expected_states={"recoverable", "failed", "abandoned"},
        source_authority_epoch=authority_epoch,
        source_revision=source_revision,
        source_session_id=session_id,
    )


def _validate_source_event(source_event: Mapping[str, object]) -> None:
    _validate_finite_json(source_event)
    if set(source_event) != SOURCE_EVENT_KEYS:
        raise InvalidSourceEvent("source event 字段必须与冻结契约精确一致")
    authority_epoch = source_event["authority_epoch"]
    source_revision = source_event["source_revision"]
    event_type = source_event["type"]
    occurred_at = source_event["occurred_at"]
    session_id = source_event["session_id"]
    data = source_event["data"]
    if not valid_uuid4(authority_epoch):
        raise InvalidSourceEvent("authority_epoch 必须是小写 UUIDv4")
    if (
        isinstance(source_revision, bool)
        or not isinstance(source_revision, int)
        or source_revision < 0
    ):
        raise InvalidSourceEvent("source_revision 必须是非负整数")
    if not isinstance(event_type, str) or event_type not in SOURCE_EVENT_TYPES:
        raise InvalidSourceEvent("type 不是已知 capture event 类型")
    if not _date_time(occurred_at):
        raise InvalidSourceEvent("occurred_at 必须是 date-time 字符串")
    if session_id is not None and not valid_uuid7(session_id):
        raise InvalidSourceEvent("session_id 必须是 UUIDv7 或 null")
    if not isinstance(data, Mapping):
        raise InvalidSourceEvent("data 必须是对象")
    if event_type == "snapshot":
        _validate_snapshot_data(
            data,
            authority_epoch,
            source_revision,
            session_id,
            api_version=SOURCE_API_VERSION,
        )
    elif event_type == "state":
        _require_session_id(session_id, event_type)
        _validate_state_data(data)
    elif event_type == "progress":
        _require_session_id(session_id, event_type)
        _validate_progress_data(data)
    elif event_type == "diagnostic":
        _validate_diagnostic_event_data(data)
    else:
        _require_session_id(session_id, event_type)
        validate_safe_swap_v3_receipt(data)
        if data["session_id"] != session_id:
            raise InvalidSourceEvent("safe_swap session_id 必须与 source event 一致")


def _require_session_id(session_id: object, event_type: object) -> None:
    if not valid_uuid7(session_id):
        raise InvalidSourceEvent(f"{event_type} source event 必须携带 UUIDv7 session_id")


def _validate_snapshot_data(
    data: Mapping[str, object],
    source_authority_epoch: object,
    source_revision: object,
    source_session_id: object,
    *,
    api_version: str = SOURCE_API_VERSION,
) -> None:
    _validate_source_or_api_version(api_version)
    if set(data) != SNAPSHOT_KEYS or data["schema"] != _capture_snapshot_schema_for_api(
        api_version
    ):
        raise InvalidSourceEvent("snapshot data 字段或 schema 无效")
    device_state = data["device_state"]
    if not _is_enum(device_state, CAPTURE_DEVICE_STATES):
        raise InvalidSourceEvent("snapshot device_state 无效")

    active = data["active_recording"]
    retained = data["retained_unsuccessful"]
    active_session_id: str | None = None
    if device_state in CAPTURE_ACTIVE_STATES:
        _require_session_id(source_session_id, "active snapshot")
        _validate_snapshot_recording(
            active,
            expected_states={device_state},
            source_authority_epoch=source_authority_epoch,
            source_revision=source_revision,
            source_session_id=source_session_id,
        )
        if isinstance(active, Mapping) and isinstance(active.get("recording_state"), Mapping):
            session_id = active["recording_state"].get("session_id")
            if isinstance(session_id, str):
                active_session_id = session_id
        if retained is not None:
            raise InvalidSourceEvent("活动 snapshot 不得携带 retained_unsuccessful")
    elif active is not None:
        raise InvalidSourceEvent("非活动 snapshot 不得携带 active_recording")

    if retained is not None:
        if device_state != "idle":
            raise InvalidSourceEvent("retained_unsuccessful 只允许出现在 idle snapshot")
        _validate_snapshot_recording(
            retained,
            expected_states=set(CAPTURE_TERMINAL_STATES),
            source_authority_epoch=source_authority_epoch,
            source_revision=source_revision,
            source_session_id=None,
        )
    _validate_runtime(data["runtime"], api_version=api_version)
    _validate_runtime_live_imu_session_relation(
        data["runtime"],
        active_session_id=active_session_id,
        api_version=api_version,
    )


def _validate_snapshot_recording(
    value: object,
    *,
    expected_states: set[str],
    source_authority_epoch: object,
    source_revision: object,
    source_session_id: object,
) -> None:
    if not isinstance(value, Mapping) or set(value) != {"generation_id", "recording_state"}:
        raise InvalidSourceEvent("snapshot recording 必须是闭合对象")
    generation_id = value["generation_id"]
    recording_state = value["recording_state"]
    if not valid_uuid4(generation_id):
        raise InvalidSourceEvent("snapshot generation_id 必须是 UUIDv4")
    if not isinstance(recording_state, Mapping) or not _is_enum(
        recording_state.get("state"), expected_states
    ):
        raise InvalidSourceEvent("snapshot recording_state 与 device_state 不一致")
    validate_recording_state(recording_state)
    if (
        recording_state["authority_epoch"] != source_authority_epoch
        or recording_state["state_revision"] != source_revision
    ):
        raise InvalidSourceEvent("snapshot recording_state 权威修订与 source event 不一致")
    if source_session_id is not None and recording_state.get("session_id") != source_session_id:
        raise InvalidSourceEvent("snapshot recording_state session_id 与 source event 不一致")


def validate_recording_state(state: object) -> None:
    """Validate one complete recording checkpoint before using its fields."""
    _validate_finite_json(state)
    try:
        _RECORDING_STATE_VALIDATOR.validate(state)
    except ValidationError as error:
        raise InvalidSourceEvent("recording_state 不符合冻结 schema") from error


def _validate_recording_device(value: object) -> None:
    if not isinstance(value, Mapping) or set(value) != {"device_id", "device_label"}:
        raise InvalidSourceEvent("recording_state device 必须是闭合对象")
    if not valid_uuid4(value["device_id"]) or not isinstance(value["device_label"], str):
        raise InvalidSourceEvent("recording_state device 无效")
    if re.fullmatch(r"YLX-[0-9A-F]{8}", value["device_label"]) is None:
        raise InvalidSourceEvent("recording_state device_label 无效")


def _validate_device_identity(value: object) -> None:
    _validate_recording_device(value)


def _validate_build(value: object) -> None:
    if not isinstance(value, Mapping) or set(value) != {
        "package_version",
        "commit",
        "build_id",
    }:
        raise InvalidSourceEvent("device build 必须是闭合对象")
    package_version = value["package_version"]
    commit = value["commit"]
    build_id = value["build_id"]
    if (
        not isinstance(package_version, str)
        or not 1 <= len(package_version) <= 64
        or not isinstance(commit, str)
        or re.fullmatch(r"[0-9a-f]{40,64}", commit) is None
        or not isinstance(build_id, str)
        or not 1 <= len(build_id) <= 128
    ):
        raise InvalidSourceEvent("device build 无效")


def _validate_capabilities(value: object, *, api_version: str) -> None:
    expected = {
        "capture",
        "preview",
        "range_download",
        "network_mutation",
    }
    if api_version == "v4":
        expected.update(
            {
                "calibration_capture",
                "session_list",
                "session_detail",
                "artifact_download",
                "capture_status",
                "session_deletion",
            }
        )
    if not isinstance(value, Mapping) or set(value) != expected:
        raise InvalidSourceEvent("device capabilities 必须是闭合对象")
    if (
        type(value["capture"]) is not bool
        or type(value["preview"]) is not bool
        or value["range_download"] is not True
        or type(value["network_mutation"]) is not bool
        or (api_version != "v4" and value["network_mutation"] is not False)
        or (
            api_version == "v4"
            and (
                value["session_list"] is not True
                or value["session_detail"] is not True
                or value["artifact_download"] is not True
                or value["capture_status"] is not True
                or type(value["session_deletion"]) is not bool
            )
        )
    ):
        raise InvalidSourceEvent("device capabilities 无效")
    if api_version != "v4":
        return
    calibration = value["calibration_capture"]
    if not isinstance(calibration, Mapping) or set(calibration) != {
        "supported",
        "enabled",
        "disabled_reason",
        "required_video_layout",
    }:
        raise InvalidSourceEvent("calibration_capture capability 必须是闭合对象")
    supported = calibration["supported"]
    enabled = calibration["enabled"]
    reason = calibration["disabled_reason"]
    if (
        type(supported) is not bool
        or type(enabled) is not bool
        or calibration["required_video_layout"] != "split-eyes"
        or (enabled and (supported is not True or reason is not None))
        or (
            not enabled
            and (not isinstance(reason, str) or reason not in CALIBRATION_CAPTURE_DISABLED_REASONS)
        )
    ):
        raise InvalidSourceEvent("calibration_capture capability 无效")


def _validate_device_storage(value: object) -> None:
    if not isinstance(value, Mapping) or set(value) != {
        "volume_id",
        "total_bytes",
        "available_bytes",
        "writable",
    }:
        raise InvalidSourceEvent("device storage 必须是闭合对象")
    total_bytes = value["total_bytes"]
    available_bytes = value["available_bytes"]
    volume_id = value["volume_id"]
    if (
        (volume_id is not None and not valid_uuid4(volume_id))
        or type(total_bytes) is not int
        or total_bytes < 0
        or type(available_bytes) is not int
        or available_bytes < 0
        or type(value["writable"]) is not bool
        or (volume_id is None and (total_bytes != 0 or available_bytes != 0 or value["writable"]))
    ):
        raise InvalidSourceEvent("device storage 无效")


def _validate_session_summary(
    value: object,
    *,
    take_id: str | None,
    api_version: str,
) -> None:
    if not isinstance(value, Mapping) or set(value) != {
        "session_id",
        "producer_outcome",
        "take_id",
        "take_sequence",
        "continuation_of",
        "display_name",
        "device",
        "started_at",
        "ended_at",
        "duration_seconds",
        "total_bytes",
        "verification",
    }:
        raise InvalidSourceEvent("session summary 必须是闭合对象")
    continuation = value["continuation_of"]
    display_name = value["display_name"]
    if (
        not valid_uuid7(value["session_id"])
        or value["producer_outcome"] != "sealed"
        or not valid_uuid7(value["take_id"])
        or (take_id is not None and value["take_id"] != take_id)
        or type(value["take_sequence"]) is not int
        or value["take_sequence"] < 1
        or (continuation is not None and not valid_uuid7(continuation))
        or not isinstance(display_name, str)
        or not 1 <= len(display_name) <= 160
        or not _date_time(value["started_at"])
        or not _date_time(value["ended_at"])
        or not _number(value["duration_seconds"])
        or value["duration_seconds"] < 0
        or type(value["total_bytes"]) is not int
        or value["total_bytes"] < 0
    ):
        raise InvalidSourceEvent("session summary 无效")
    _validate_device_identity(value["device"])
    verification = value["verification"]
    if verification is not None:
        _validate_gateway_verification(verification, api_version=api_version)


def _validate_gateway_verification(value: object, *, api_version: str) -> None:
    if not isinstance(value, Mapping) or set(value) != {
        "actor",
        "validator",
        "manifest_sha256",
        "verified_at",
        "verdict",
        "diagnostics",
    }:
        raise InvalidSourceEvent("gateway verification 必须是闭合对象")
    validator = value["validator"]
    diagnostics = value["diagnostics"]
    if (
        not isinstance(validator, Mapping)
        or set(validator) != {"name", "version", "build_sha256"}
        or value["actor"] != "gateway"
        or not _bounded_string(validator["name"], 128)
        or not _bounded_string(validator["version"], 64)
        or not valid_sha256(validator["build_sha256"])
        or not valid_sha256(value["manifest_sha256"])
        or not _date_time(value["verified_at"])
        or not _is_enum(value["verdict"], {"usable", "unusable"})
        or not isinstance(diagnostics, list)
        or (value["verdict"] == "unusable" and not diagnostics)
    ):
        raise InvalidSourceEvent("gateway verification 无效")
    if api_version == "v4":
        if any(
            not isinstance(item, Mapping)
            or set(item) != {"code", "summary"}
            or not _is_enum(item.get("code"), GATEWAY_VERIFICATION_DIAGNOSTIC_CODES)
            or not _bounded_string(item.get("summary"), 512)
            for item in diagnostics
        ):
            raise InvalidSourceEvent("gateway verification diagnostics 无效")
    elif any(not _bounded_string(item, 512) for item in diagnostics):
        raise InvalidSourceEvent("gateway verification diagnostics 无效")


def _validate_session_discovery_diagnostic(value: object) -> None:
    if not isinstance(value, Mapping) or set(value) != {
        "quarantine_id",
        "code",
        "observed_at",
        "message",
    }:
        raise InvalidSourceEvent("session discovery diagnostic 必须是闭合对象")
    if (
        not valid_uuid4(value["quarantine_id"])
        or not _is_enum(
            value["code"],
            {
                "manifest_unreadable",
                "unsupported_schema",
                "manifest_invalid",
                "manifest_not_sealed",
            },
        )
        or not _date_time(value["observed_at"])
        or not _bounded_string(value["message"], 512)
    ):
        raise InvalidSourceEvent("session discovery diagnostic 无效")


def _project_snapshot_data(data: dict[str, object], *, api_version: str) -> None:
    if data.get("schema") == CAPTURE_SNAPSHOT_SCHEMA_V2:
        data["schema"] = _capture_snapshot_schema_for_api(api_version)
    runtime = data.get("runtime")
    if isinstance(runtime, dict):
        _project_runtime(runtime, api_version=api_version)


def _project_runtime(runtime: dict[str, object], *, api_version: str) -> None:
    if api_version in LEGACY_DEVICE_API_VERSIONS:
        runtime["live_imu"] = None
        runtime.pop("camera", None)
        runtime.pop("camera_focus", None)


def _capture_status_schema_for_api(api_version: str) -> str:
    _validate_source_or_api_version(api_version)
    return CAPTURE_STATUS_SCHEMA_V4 if api_version == "v4" else CAPTURE_STATUS_SCHEMA_V2


def _capture_snapshot_schema_for_api(api_version: str) -> str:
    _validate_source_or_api_version(api_version)
    return CAPTURE_SNAPSHOT_SCHEMA_V4 if api_version == "v4" else CAPTURE_SNAPSHOT_SCHEMA_V2


def _validate_source_or_api_version(api_version: str) -> None:
    if api_version != SOURCE_API_VERSION:
        _event_schema_version(api_version)


def _runtime_keys_for_api(api_version: str) -> frozenset[str]:
    _validate_source_or_api_version(api_version)
    return RUNTIME_BASE_KEYS if api_version in LEGACY_DEVICE_API_VERSIONS else RUNTIME_SOURCE_KEYS


def _validate_runtime(value: object, *, api_version: str = SOURCE_API_VERSION) -> None:
    if not isinstance(value, Mapping) or set(value) != _runtime_keys_for_api(api_version):
        raise InvalidSourceEvent("runtime 必须是闭合对象")
    if not _date_time(value["observed_at"]):
        raise InvalidSourceEvent("runtime observed_at 无效")
    if not _is_enum(
        value["connection_method"],
        {"wifi_ap", "wifi_client", "ethernet_direct", "ethernet_lan", "offline"},
    ):
        raise InvalidSourceEvent("runtime connection_method 无效")
    temperature = value["temperature_celsius"]
    if not _number(temperature) or not -40 <= temperature <= 125:
        raise InvalidSourceEvent("runtime temperature_celsius 无效")
    net_validator = facade_attr("rp_ylx.api.events", "_validate_network", _validate_network)
    net_validator(value["network"])
    if api_version in LEGACY_DEVICE_API_VERSIONS and value["live_imu"] is not None:
        raise InvalidSourceEvent("v2/v3 runtime live_imu 必须是 null")
    if api_version not in LEGACY_DEVICE_API_VERSIONS and value["live_imu"] is not None:
        _validate_live_imu(value["live_imu"])
    if api_version not in LEGACY_DEVICE_API_VERSIONS:
        _validate_camera_connection(value["camera"])
    if "camera_focus" in value and value["camera_focus"] is not None:
        _validate_camera_focus(value["camera_focus"])


def _validate_runtime_live_imu_session_relation(
    runtime: object,
    *,
    active_session_id: str | None,
    api_version: str,
) -> None:
    if api_version in LEGACY_DEVICE_API_VERSIONS:
        return
    if not isinstance(runtime, Mapping):
        raise InvalidSourceEvent("runtime 必须是闭合对象")
    live_imu = runtime["live_imu"]
    if live_imu is None:
        return
    if not isinstance(live_imu, Mapping) or live_imu.get("session_id") != active_session_id:
        raise InvalidSourceEvent("v4/source runtime live_imu session_id 必须与活动录制一致")


def _validate_camera_focus(value: object) -> None:
    if not isinstance(value, Mapping) or set(value) != CAMERA_FOCUS_KEYS:
        raise InvalidSourceEvent("camera focus 必须是闭合对象")
    if not valid_camera_focus_status(value):
        raise InvalidSourceEvent("camera focus 无效")


def _validate_camera_connection(value: object) -> None:
    if (
        not isinstance(value, Mapping)
        or set(value) != {"schema", "state"}
        or value.get("schema") != "ylx.camera-connection.v1"
        or value.get("state") not in {"connected", "disconnected"}
    ):
        raise InvalidSourceEvent("camera connection status 无效")


def _validate_network(value: object) -> None:
    if not isinstance(value, Mapping) or set(value) != NETWORK_KEYS:
        raise InvalidSourceEvent("runtime network 必须是闭合对象")
    for name in ("ap", "wifi_client", "wired"):
        _validate_network_interface(value[name])
    if not _is_enum(value["default_route"], {"wifi_client", "wired", "none"}):
        raise InvalidSourceEvent("runtime default_route 无效")


def _validate_network_interface(value: object) -> None:
    if not valid_network_interface(value):
        raise InvalidSourceEvent("network interface 无效")


def _validate_live_imu(value: object) -> None:
    if not isinstance(value, Mapping) or set(value) != LIVE_IMU_KEYS:
        raise InvalidSourceEvent("live_imu 必须是闭合对象")
    session_id = value["session_id"]
    if session_id is not None and not valid_uuid7(session_id):
        raise InvalidSourceEvent("live_imu session_id 无效")
    clock = value["clock"]
    if (
        not isinstance(clock, Mapping)
        or set(clock) != {"time_base", "timestamp_ns"}
        or clock["time_base"] != "host_monotonic"
        or type(clock["timestamp_ns"]) is not int
        or clock["timestamp_ns"] < 0
    ):
        raise InvalidSourceEvent("live_imu clock 无效")
    raw = value["raw"]
    if not isinstance(raw, Mapping) or set(raw) != LIVE_IMU_RAW_KEYS or raw["units"] != "raw_int16":
        raise InvalidSourceEvent("live_imu raw 无效")
    _validate_raw_vector(raw["accelerometer"])
    _validate_raw_vector(raw["gyroscope"])
    sync = value["sync"]
    if (
        not isinstance(sync, Mapping)
        or set(sync) != LIVE_IMU_SYNC_KEYS
        or not _is_enum(sync["quality"], {"insufficient", "good", "degraded"})
    ):
        raise InvalidSourceEvent("live_imu sync 无效")


def _validate_raw_vector(value: object) -> None:
    if (
        not isinstance(value, Mapping)
        or set(value) != {"x", "y", "z"}
        or any(type(value[key]) is not int or not -32768 <= value[key] <= 32767 for key in value)
    ):
        raise InvalidSourceEvent("live_imu raw vector 无效")


def _validate_state_data(data: Mapping[str, object]) -> None:
    if set(data) != STATE_KEYS or data["schema"] != "ylx.capture-state-event.v2":
        raise InvalidSourceEvent("state data 字段或 schema 无效")
    if not _is_enum(
        data["state"],
        CAPTURE_RECORDING_STATES,
    ):
        raise InvalidSourceEvent("state 值无效")
    for field in ("volume_id", "generation_id"):
        if not valid_uuid4(data[field]):
            raise InvalidSourceEvent(f"state {field} 必须是 UUIDv4")


def _validate_progress_data(data: Mapping[str, object]) -> None:
    if set(data) != PROGRESS_KEYS or data["schema"] != "ylx.capture-progress-event.v2":
        raise InvalidSourceEvent("progress data 字段或 schema 无效")
    if not _is_enum(data["phase"], CAPTURE_ACTIVE_STATES):
        raise InvalidSourceEvent("progress phase 无效")
    if not _is_enum(data["unit"], {"frames", "bytes", "artifacts", "checks"}):
        raise InvalidSourceEvent("progress unit 无效")
    if not _number(data["elapsed_seconds"]) or data["elapsed_seconds"] < 0:
        raise InvalidSourceEvent("progress elapsed_seconds 无效")
    if type(data["completed_units"]) is not int or data["completed_units"] < 0:
        raise InvalidSourceEvent("progress completed_units 无效")
    total = data["total_units"]
    if total is not None and (type(total) is not int or total < 0):
        raise InvalidSourceEvent("progress total_units 无效")


def _validate_diagnostic_event_data(data: Mapping[str, object]) -> None:
    if set(data) != DIAGNOSTIC_EVENT_KEYS or data["schema"] != "ylx.capture-diagnostic-event.v2":
        raise InvalidSourceEvent("diagnostic data 字段或 schema 无效")
    _validate_diagnostic(data["diagnostic"])


def _validate_diagnostic(diagnostic: object) -> None:
    _validate_finite_json(diagnostic)
    try:
        _DIAGNOSTIC_VALIDATOR.validate(diagnostic)
    except ValidationError as error:
        raise InvalidSourceEvent("diagnostic 不符合冻结 schema") from error


def _number(value: object) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and (isinstance(value, int) or math.isfinite(value))
    )


def _validate_finite_json(value: object) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise InvalidSourceEvent("JSON 数值必须有限")
    if isinstance(value, Mapping):
        for item in value.values():
            _validate_finite_json(item)
    elif isinstance(value, list):
        for item in value:
            _validate_finite_json(item)


def _uuid(value: object, pattern: re.Pattern[str]) -> bool:
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _sha256(value: object) -> bool:
    return valid_sha256(value)


def _bounded_string(value: object, max_length: int) -> bool:
    return isinstance(value, str) and 1 <= len(value) <= max_length


def _is_enum(value: object, allowed: Collection[str]) -> bool:
    return isinstance(value, str) and value in allowed


_date_time = valid_datetime


def _date_time_value(value: object) -> datetime:
    try:
        return parse_rfc3339(value)
    except ValueError as error:
        raise InvalidSourceEvent("date-time 字符串无效") from error


def validate_safe_swap_v3_receipt(receipt: Mapping[str, object]) -> None:
    """验证新生产的最小 v3 safe-swap receipt。"""
    _validate_finite_json(receipt)
    if set(receipt) != SAFE_SWAP_V3_KEYS:
        raise InvalidSourceEvent("safe_swap 必须是最小十字段 v3 回执")
    string_fields = {
        "session_id",
        "volume_id",
        "generation_id",
        "manifest_id",
        "manifest_sha256",
        "sealed_at",
        "released_at",
        "release_state",
    }
    if receipt["schema"] != "ylx.safe-swap-receipt.v3" or any(
        not isinstance(receipt[field], str) for field in string_fields
    ):
        raise InvalidSourceEvent("safe_swap v3 字段类型或 schema 无效")
    if (
        not valid_uuid7(receipt["session_id"])
        or not valid_uuid4(receipt["volume_id"])
        or not valid_uuid4(receipt["generation_id"])
        or not valid_uuid7(receipt["manifest_id"])
        or not valid_sha256(receipt["manifest_sha256"])
        or not _is_enum(receipt["release_state"], {"unmounted", "device-released"})
        or not _date_time(receipt["sealed_at"])
        or not _date_time(receipt["released_at"])
        or type(receipt["open_handle_count"]) is not int
        or receipt["open_handle_count"] != 0
    ):
        raise InvalidSourceEvent("safe_swap v3 回执值无效")
