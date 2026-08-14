"""Capture daemon 源事件的有界 SSE 重放。"""

from __future__ import annotations

import json
import math
import re
import threading
import time
from collections import deque
from collections.abc import Callable, Collection, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from importlib.resources import files

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError


class InvalidEventCursor(ValueError):
    """Last-Event-ID 不是契约要求的十进制 delivery ID。"""


class InvalidSourceEvent(ValueError):
    """daemon source event 不是冻结契约定义的闭合事件。"""


class UnsupportedEventVersion(ValueError):
    """事件不能无损投影到请求的 API 版本。"""


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
RUNTIME_KEYS = frozenset(
    {"observed_at", "connection_method", "temperature_celsius", "network", "live_imu"}
)
NETWORK_KEYS = frozenset({"ap", "wifi_client", "wired", "default_route"})
NETWORK_INTERFACE_KEYS = frozenset({"state", "interface", "addresses", "peer_or_ssid"})
LIVE_IMU_KEYS = frozenset(
    {"session_id", "clock", "acceleration_m_s2", "angular_velocity_rad_s", "orientation_quaternion"}
)
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
UUID_V4 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
UUID_V7 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
DIAGNOSTIC_CODE = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")
INTERFACE_NAME = re.compile(r"^[A-Za-z0-9_.:-]+$")
RFC3339_DATE_TIME = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]+)?(?:[Zz]|[+-][0-9]{2}:[0-9]{2})$"
)
_FORMAT_CHECKER = FormatChecker()
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


@dataclass(frozen=True, slots=True)
class SseEvent:
    delivery_id: int
    api_version: str
    source_event: Mapping[str, object]

    def encode(self) -> bytes:
        delivery_id = str(self.delivery_id)
        payload = {
            "schema": f"ylx.capture-event.{self.api_version}",
            "sse_delivery_id": delivery_id,
            **self.source_event,
        }
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        return (f"id: {delivery_id}\nevent: {self.source_event['type']}\ndata: {data}\n\n").encode()


@dataclass(frozen=True, slots=True)
class _BufferedEvent:
    delivery_id: int
    source_event: Mapping[str, object]
    resync: bool = False


class EventReplayBuffer:
    """为 daemon 源事件分配独立 delivery ID，并保留有限重放窗口。"""

    def __init__(self, capacity: int = 128) -> None:
        if capacity < 1:
            raise ValueError("capacity 必须大于零")
        self._events: deque[_BufferedEvent] = deque(maxlen=capacity)
        self._next_delivery_id = 1
        self._lock = threading.RLock()
        self._changed = threading.Condition(self._lock)

    def publish(self, source_event: Mapping[str, object]) -> str:
        with self._changed:
            event = self._publish_locked(source_event)
            self._changed.notify_all()
            return str(event.delivery_id)

    def replay(
        self,
        last_event_id: str | None,
        *,
        api_version: str,
        snapshot: Callable[[], Mapping[str, object]],
    ) -> tuple[SseEvent, ...]:
        version = _event_schema_version(api_version)
        cursor = _parse_cursor(last_event_id)
        with self._changed:
            if cursor is None:
                current = self._publish_locked(snapshot(), resync=True)
                self._changed.notify_all()
                return (_for_version(current, version),)

            replayed = self._events_after_locked(cursor, version, snapshot)
            return () if replayed is None else replayed

    def wait_after(
        self,
        delivery_id: int,
        timeout: float,
        *,
        api_version: str,
        snapshot: Callable[[], Mapping[str, object]],
    ) -> tuple[SseEvent, ...]:
        """等待 cursor 后的新事件；超时返回空元组供 gateway 发 heartbeat。"""

        if isinstance(delivery_id, bool) or not isinstance(delivery_id, int) or delivery_id < 0:
            raise InvalidEventCursor("delivery_id 必须是非负十进制整数")
        if timeout < 0:
            raise ValueError("timeout 不能小于零")
        version = _event_schema_version(api_version)
        deadline = time.monotonic() + timeout
        with self._changed:
            while True:
                replayed = self._events_after_locked(delivery_id, version, snapshot)
                if replayed is not None:
                    return replayed
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return ()
                self._changed.wait(remaining)

    def _events_after_locked(
        self,
        cursor: int,
        api_version: str,
        snapshot: Callable[[], Mapping[str, object]],
    ) -> tuple[SseEvent, ...] | None:
        buffered = tuple(self._events)
        if not buffered or cursor < buffered[0].delivery_id or cursor > buffered[-1].delivery_id:
            current = self._publish_locked(snapshot(), resync=True)
            self._changed.notify_all()
            return (_for_version(current, api_version),)

        cursor_index = cursor - buffered[0].delivery_id
        if cursor_index >= len(buffered) or buffered[cursor_index].delivery_id != cursor:
            current = self._publish_locked(snapshot(), resync=True)
            self._changed.notify_all()
            return (_for_version(current, api_version),)

        replayed = buffered[cursor_index + 1 :]
        if not replayed:
            return None
        if not _source_is_contiguous(buffered[cursor_index:]):
            current = self._publish_locked(snapshot(), resync=True)
            self._changed.notify_all()
            return (_for_version(current, api_version),)
        return tuple(_for_version(event, api_version) for event in replayed)

    def _publish_locked(
        self, source_event: Mapping[str, object], *, resync: bool = False
    ) -> _BufferedEvent:
        copied_event = deepcopy(dict(source_event))
        _validate_source_event(copied_event)
        if resync and copied_event["type"] != "snapshot":
            raise InvalidSourceEvent("重同步 callback 必须返回 snapshot source event")
        event = _BufferedEvent(self._next_delivery_id, copied_event, resync)
        self._next_delivery_id += 1
        self._events.append(event)
        return event


def heartbeat_comment() -> bytes:
    """返回不携带 id、也不占用 delivery sequence 的 SSE heartbeat。"""

    return b": heartbeat\n\n"


def validate_capture_status(status: object) -> None:
    """验证 HTTP 与命令响应共用的权威 capture status。"""

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
        status["schema"] != "ylx.capture-status.v2"
        or not _uuid(authority_epoch, UUID_V4)
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
        api_version not in {"v2", "v3"}
        or descriptor["schema"] != f"ylx.device.{api_version}"
        or descriptor["api_version"] != f"{expected_version}.0"
        or descriptor["security_profile"] != security_profile
        or security_profile not in {"customer", "lab"}
        or not isinstance(fingerprint, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", fingerprint) is None
    ):
        raise InvalidSourceEvent("device descriptor 身份或版本无效")
    _validate_device_identity(descriptor["device"])
    _validate_build(descriptor["build"])
    _validate_capabilities(descriptor["capabilities"])
    _validate_device_storage(descriptor["storage"])
    _validate_runtime(descriptor["runtime"])


def validate_session_list(
    resource: object,
    *,
    limit: int,
    take_id: str | None,
) -> None:
    """验证 provider 返回的闭合会话页。"""

    _validate_finite_json(resource)
    if not isinstance(resource, Mapping) or set(resource) != {
        "schema",
        "items",
        "diagnostics",
        "next_cursor",
    }:
        raise InvalidSourceEvent("session list 必须是闭合对象")
    items = resource["items"]
    diagnostics = resource["diagnostics"]
    next_cursor = resource["next_cursor"]
    if (
        resource["schema"] != "ylx.session-list.v2"
        or not isinstance(items, list)
        or not isinstance(diagnostics, list)
        or len(items) + len(diagnostics) > limit
        or (next_cursor is not None and (not isinstance(next_cursor, str) or not next_cursor))
    ):
        raise InvalidSourceEvent("session list 基础字段无效")
    for item in items:
        _validate_session_summary(item, take_id=take_id)
    if len({item["session_id"] for item in items}) != len(items):
        raise InvalidSourceEvent("session list 不得重复会话")
    order = [(_date_time_value(item["started_at"]), item["session_id"]) for item in items]
    if order != sorted(order):
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
        or not _uuid(authority_epoch, UUID_V4)
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


def _parse_cursor(last_event_id: str | None) -> int | None:
    if last_event_id is None:
        return None
    if not last_event_id or not last_event_id.isascii() or not last_event_id.isdecimal():
        raise InvalidEventCursor("Last-Event-ID 必须是十进制 delivery ID")
    try:
        return int(last_event_id)
    except ValueError as error:
        raise InvalidEventCursor("Last-Event-ID 超出十进制 delivery ID 范围") from error


def _event_schema_version(api_version: str) -> str:
    if api_version not in {"v2", "v3"}:
        raise ValueError("api_version 必须是 v2 或 v3")
    return api_version


def _for_version(event: _BufferedEvent, api_version: str) -> SseEvent:
    if api_version == "v2" and event.source_event["type"] == "safe_swap":
        raise UnsupportedEventVersion("v3 safe-swap event 不能投影为 v2")
    return SseEvent(event.delivery_id, api_version, event.source_event)


def _source_is_contiguous(events: tuple[_BufferedEvent, ...]) -> bool:
    for previous, current in zip(events, events[1:], strict=False):
        previous_source = previous.source_event
        current_source = current.source_event
        if current.resync:
            continue
        if current_source.get("authority_epoch") != previous_source.get(
            "authority_epoch"
        ) or current_source.get("source_revision") != _next_revision(
            previous_source.get("source_revision")
        ):
            return False
    return True


def _next_revision(revision: object) -> int | None:
    if isinstance(revision, int) and not isinstance(revision, bool):
        return revision + 1
    return None


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
    if not isinstance(authority_epoch, str) or UUID_V4.fullmatch(authority_epoch) is None:
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
    if session_id is not None and (
        not isinstance(session_id, str) or UUID_V7.fullmatch(session_id) is None
    ):
        raise InvalidSourceEvent("session_id 必须是 UUIDv7 或 null")
    if not isinstance(data, Mapping):
        raise InvalidSourceEvent("data 必须是对象")
    if event_type == "snapshot":
        _validate_snapshot_data(data, authority_epoch, source_revision, session_id)
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
    if not isinstance(session_id, str) or UUID_V7.fullmatch(session_id) is None:
        raise InvalidSourceEvent(f"{event_type} source event 必须携带 UUIDv7 session_id")


def _validate_snapshot_data(
    data: Mapping[str, object],
    source_authority_epoch: object,
    source_revision: object,
    source_session_id: object,
) -> None:
    if set(data) != SNAPSHOT_KEYS or data["schema"] != "ylx.capture-snapshot-event.v2":
        raise InvalidSourceEvent("snapshot data 字段或 schema 无效")
    device_state = data["device_state"]
    if not _is_enum(
        device_state,
        {"idle", "recording", "finalizing", "encoding", "verifying", "blocked"},
    ):
        raise InvalidSourceEvent("snapshot device_state 无效")

    active = data["active_recording"]
    retained = data["retained_unsuccessful"]
    if device_state in {"recording", "finalizing", "encoding", "verifying"}:
        _require_session_id(source_session_id, "active snapshot")
        _validate_snapshot_recording(
            active,
            expected_states={device_state},
            source_authority_epoch=source_authority_epoch,
            source_revision=source_revision,
            source_session_id=source_session_id,
        )
        if retained is not None:
            raise InvalidSourceEvent("活动 snapshot 不得携带 retained_unsuccessful")
    elif active is not None:
        raise InvalidSourceEvent("非活动 snapshot 不得携带 active_recording")

    if retained is not None:
        if device_state != "idle":
            raise InvalidSourceEvent("retained_unsuccessful 只允许出现在 idle snapshot")
        _validate_snapshot_recording(
            retained,
            expected_states={"recoverable", "failed", "abandoned"},
            source_authority_epoch=source_authority_epoch,
            source_revision=source_revision,
            source_session_id=None,
        )
    _validate_runtime(data["runtime"])


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
    if not isinstance(generation_id, str) or UUID_V4.fullmatch(generation_id) is None:
        raise InvalidSourceEvent("snapshot generation_id 必须是 UUIDv4")
    if not isinstance(recording_state, Mapping) or not _is_enum(
        recording_state.get("state"), expected_states
    ):
        raise InvalidSourceEvent("snapshot recording_state 与 device_state 不一致")
    _validate_recording_state(recording_state)
    if (
        recording_state["authority_epoch"] != source_authority_epoch
        or recording_state["state_revision"] != source_revision
    ):
        raise InvalidSourceEvent("snapshot recording_state 权威修订与 source event 不一致")
    if source_session_id is not None and recording_state.get("session_id") != source_session_id:
        raise InvalidSourceEvent("snapshot recording_state session_id 与 source event 不一致")


def _validate_recording_state(state: Mapping[str, object]) -> None:
    _validate_finite_json(state)
    try:
        _RECORDING_STATE_VALIDATOR.validate(state)
    except ValidationError as error:
        raise InvalidSourceEvent("recording_state 不符合冻结 schema") from error


def _validate_recording_device(value: object) -> None:
    if not isinstance(value, Mapping) or set(value) != {"device_id", "device_label"}:
        raise InvalidSourceEvent("recording_state device 必须是闭合对象")
    if not _uuid(value["device_id"], UUID_V4) or not isinstance(value["device_label"], str):
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


def _validate_capabilities(value: object) -> None:
    if not isinstance(value, Mapping) or set(value) != {
        "capture",
        "preview",
        "range_download",
        "network_mutation",
    }:
        raise InvalidSourceEvent("device capabilities 必须是闭合对象")
    if (
        type(value["capture"]) is not bool
        or type(value["preview"]) is not bool
        or value["range_download"] is not True
        or value["network_mutation"] is not False
    ):
        raise InvalidSourceEvent("device capabilities 无效")


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
    if (
        not _uuid(value["volume_id"], UUID_V4)
        or type(total_bytes) is not int
        or total_bytes < 0
        or type(available_bytes) is not int
        or available_bytes < 0
        or type(value["writable"]) is not bool
    ):
        raise InvalidSourceEvent("device storage 无效")


def _validate_session_summary(value: object, *, take_id: str | None) -> None:
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
        not _uuid(value["session_id"], UUID_V7)
        or value["producer_outcome"] != "sealed"
        or not _uuid(value["take_id"], UUID_V7)
        or (take_id is not None and value["take_id"] != take_id)
        or type(value["take_sequence"]) is not int
        or value["take_sequence"] < 1
        or (continuation is not None and not _uuid(continuation, UUID_V7))
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
        _validate_gateway_verification(verification)


def _validate_gateway_verification(value: object) -> None:
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
        or not _sha256(validator["build_sha256"])
        or not _sha256(value["manifest_sha256"])
        or not _date_time(value["verified_at"])
        or not _is_enum(value["verdict"], {"usable", "unusable"})
        or not isinstance(diagnostics, list)
        or any(not _bounded_string(item, 512) for item in diagnostics)
        or (value["verdict"] == "unusable" and not diagnostics)
    ):
        raise InvalidSourceEvent("gateway verification 无效")


def _validate_session_discovery_diagnostic(value: object) -> None:
    if not isinstance(value, Mapping) or set(value) != {
        "quarantine_id",
        "code",
        "observed_at",
        "message",
    }:
        raise InvalidSourceEvent("session discovery diagnostic 必须是闭合对象")
    if (
        not _uuid(value["quarantine_id"], UUID_V4)
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


def _validate_runtime(value: object) -> None:
    if not isinstance(value, Mapping) or set(value) != RUNTIME_KEYS:
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
    _validate_network(value["network"])
    if value["live_imu"] is not None:
        _validate_live_imu(value["live_imu"])


def _validate_network(value: object) -> None:
    if not isinstance(value, Mapping) or set(value) != NETWORK_KEYS:
        raise InvalidSourceEvent("runtime network 必须是闭合对象")
    for name in ("ap", "wifi_client", "wired"):
        _validate_network_interface(value[name])
    if not _is_enum(value["default_route"], {"wifi_client", "wired", "none"}):
        raise InvalidSourceEvent("runtime default_route 无效")


def _validate_network_interface(value: object) -> None:
    if not isinstance(value, Mapping) or set(value) != NETWORK_INTERFACE_KEYS:
        raise InvalidSourceEvent("network interface 必须是闭合对象")
    state = value["state"]
    if not _is_enum(
        state,
        {
            "disabled",
            "disconnected",
            "starting",
            "connecting",
            "connected",
            "active",
            "degraded",
            "failed",
            "unavailable",
        },
    ):
        raise InvalidSourceEvent("network interface state 无效")
    interface = value["interface"]
    if interface is not None and (
        not isinstance(interface, str)
        or not 1 <= len(interface) <= 64
        or INTERFACE_NAME.fullmatch(interface) is None
    ):
        raise InvalidSourceEvent("network interface 名称无效")
    addresses = value["addresses"]
    if (
        not isinstance(addresses, list)
        or any(not isinstance(address, str) or not 1 <= len(address) <= 64 for address in addresses)
        or len(addresses) != len(set(addresses))
    ):
        raise InvalidSourceEvent("network interface addresses 无效")
    peer = value["peer_or_ssid"]
    if peer is not None and (not isinstance(peer, str) or not 1 <= len(peer) <= 128):
        raise InvalidSourceEvent("network interface peer_or_ssid 无效")
    if _is_enum(state, {"connected", "active", "degraded"}) and (
        not isinstance(interface, str) or not addresses
    ):
        raise InvalidSourceEvent("可用 network interface 必须携带接口名和地址")


def _validate_live_imu(value: object) -> None:
    if not isinstance(value, Mapping) or set(value) != LIVE_IMU_KEYS:
        raise InvalidSourceEvent("live_imu 必须是闭合对象")
    session_id = value["session_id"]
    if not isinstance(session_id, str) or UUID_V7.fullmatch(session_id) is None:
        raise InvalidSourceEvent("live_imu session_id 无效")
    clock = value["clock"]
    if (
        not isinstance(clock, Mapping)
        or set(clock) != {"time_base", "epoch_id", "timestamp_ns"}
        or clock["time_base"] != "session_monotonic"
        or clock["epoch_id"] != session_id
        or type(clock["timestamp_ns"]) is not int
        or clock["timestamp_ns"] < 0
    ):
        raise InvalidSourceEvent("live_imu clock 无效")
    _validate_vector(value["acceleration_m_s2"], {"x", "y", "z"})
    _validate_vector(value["angular_velocity_rad_s"], {"x", "y", "z"})
    _validate_vector(value["orientation_quaternion"], {"w", "x", "y", "z"})


def _validate_vector(value: object, keys: set[str]) -> None:
    if (
        not isinstance(value, Mapping)
        or set(value) != keys
        or any(not _number(value[key]) for key in keys)
    ):
        raise InvalidSourceEvent("live_imu vector 无效")


def _validate_state_data(data: Mapping[str, object]) -> None:
    if set(data) != STATE_KEYS or data["schema"] != "ylx.capture-state-event.v2":
        raise InvalidSourceEvent("state data 字段或 schema 无效")
    if not _is_enum(
        data["state"],
        {
            "recording",
            "finalizing",
            "encoding",
            "verifying",
            "recoverable",
            "failed",
            "abandoned",
        },
    ):
        raise InvalidSourceEvent("state 值无效")
    for field in ("volume_id", "generation_id"):
        value = data[field]
        if not isinstance(value, str) or UUID_V4.fullmatch(value) is None:
            raise InvalidSourceEvent(f"state {field} 必须是 UUIDv4")


def _validate_progress_data(data: Mapping[str, object]) -> None:
    if set(data) != PROGRESS_KEYS or data["schema"] != "ylx.capture-progress-event.v2":
        raise InvalidSourceEvent("progress data 字段或 schema 无效")
    if not _is_enum(data["phase"], {"recording", "finalizing", "encoding", "verifying"}):
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
    return isinstance(value, str) and SHA256.fullmatch(value) is not None


def _bounded_string(value: object, max_length: int) -> bool:
    return isinstance(value, str) and 1 <= len(value) <= max_length


def _is_enum(value: object, allowed: Collection[str]) -> bool:
    return isinstance(value, str) and value in allowed


def _date_time(value: object) -> bool:
    return (
        isinstance(value, str)
        and RFC3339_DATE_TIME.fullmatch(value) is not None
        and _FORMAT_CHECKER.conforms(value, "date-time")
    )


def _date_time_value(value: object) -> datetime:
    if not _date_time(value):
        raise InvalidSourceEvent("date-time 字符串无效")
    assert isinstance(value, str)
    return datetime.fromisoformat(value.replace("z", "+00:00").replace("Z", "+00:00"))


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
        UUID_V7.fullmatch(receipt["session_id"]) is None
        or UUID_V4.fullmatch(receipt["volume_id"]) is None
        or UUID_V4.fullmatch(receipt["generation_id"]) is None
        or UUID_V7.fullmatch(receipt["manifest_id"]) is None
        or SHA256.fullmatch(receipt["manifest_sha256"]) is None
        or not _is_enum(receipt["release_state"], {"unmounted", "device-released"})
        or not _date_time(receipt["sealed_at"])
        or not _date_time(receipt["released_at"])
        or type(receipt["open_handle_count"]) is not int
        or receipt["open_handle_count"] != 0
    ):
        raise InvalidSourceEvent("safe_swap v3 回执值无效")
