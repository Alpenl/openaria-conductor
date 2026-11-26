"""Device info, firmware control, voice, clock, and embedded web asset controllers."""

from __future__ import annotations

import re
from collections.abc import Mapping
from contextlib import suppress
from http import HTTPStatus
from typing import TYPE_CHECKING

from rp_ylx.api.compat import facade_attr
from rp_ylx.api.controllers.types import ProviderError
from rp_ylx.api.validators.events import (
    InvalidSourceEvent,
    project_device_descriptor,
    validate_device_descriptor,
)
from rp_ylx.clock_sync import valid_clock_request, valid_clock_response
from rp_ylx.firmware_control import FirmwareError, request_firmware
from rp_ylx.voice_feedback import valid_volume
from rp_ylx.web import EchoWebArtifactError, asset_content_type, read_asset

if TYPE_CHECKING:
    from rp_ylx.api.http_server import GatewayHandler

WEB_CONTENT_SECURITY_POLICY = (
    "default-src 'self'; base-uri 'none'; connect-src 'self'; form-action 'self'; "
    "frame-ancestors 'none'; img-src 'self' blob: data:; object-src 'none'; "
    "script-src 'self'; worker-src 'self' blob:; style-src 'self'"
)
_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")


def _valid_firmware_release(value: object) -> bool:
    return (
        isinstance(value, Mapping)
        and set(value) == {"version", "commit"}
        and isinstance(value.get("version"), str)
        and 1 <= len(value["version"]) <= 64
        and isinstance(value.get("commit"), str)
        and _COMMIT_RE.fullmatch(value["commit"]) is not None
    )


def _valid_firmware_available_release(value: object) -> bool:
    if not isinstance(value, Mapping):
        return False
    keys = set(value)
    if not {"version", "commit"}.issubset(keys) or keys - {
        "version",
        "commit",
        "release_notes",
        "published_at",
    }:
        return False
    version = value.get("version")
    commit = value.get("commit")
    if (
        not isinstance(version, str)
        or not 1 <= len(version) <= 64
        or not isinstance(commit, str)
        or _COMMIT_RE.fullmatch(commit) is None
    ):
        return False
    if "release_notes" in value:
        notes = value["release_notes"]
        if not isinstance(notes, str) or len(notes) > 16000:
            return False
    if "published_at" in value and value["published_at"] is not None:
        published = value["published_at"]
        if not isinstance(published, str) or not published or len(published.encode("utf-8")) > 128:
            return False
    return True


def _valid_firmware_task(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "id",
        "action",
        "target_commit",
        "status",
        "message",
        "started_at",
        "finished_at",
    }:
        return False
    task_id = value.get("id")
    action = value.get("action")
    target_commit = value.get("target_commit")
    status = value.get("status")
    message = value.get("message")
    started_at = value.get("started_at")
    finished_at = value.get("finished_at")
    if (
        not isinstance(task_id, str)
        or not 1 <= len(task_id) <= 128
        or action not in {"update", "rollback"}
        or not isinstance(target_commit, str)
        or _COMMIT_RE.fullmatch(target_commit) is None
        or status not in {"accepted", "running", "succeeded", "failed"}
        or not isinstance(message, str)
        or len(message) > 4096
        or type(started_at) is not int
        or not 0 <= started_at <= 2**63 - 1
    ):
        return False
    if status in {"accepted", "running"}:
        return finished_at is None
    return type(finished_at) is int and 0 <= finished_at <= 2**63 - 1


def _valid_firmware_status(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "schema",
        "supported",
        "current",
        "previous",
        "available",
        "has_update",
        "checked_at",
        "warning",
        "task",
    }:
        return False
    current = value.get("current")
    previous = value.get("previous")
    available = value.get("available")
    checked_at = value.get("checked_at")
    warning = value.get("warning")
    task = value.get("task")
    return (
        value.get("schema") == "openaria.firmware-status.v1"
        and type(value.get("supported")) is bool
        and (current is None or _valid_firmware_release(current))
        and (previous is None or _valid_firmware_release(previous))
        and (available is None or _valid_firmware_available_release(available))
        and type(value.get("has_update")) is bool
        and (checked_at is None or (type(checked_at) is int and 0 <= checked_at <= 2**63 - 1))
        and (warning is None or (isinstance(warning, str) and len(warning) <= 1024))
        and (task is None or _valid_firmware_task(task))
    )


def _provider_implements(provider: object, method_name: str) -> object | None:
    """Return a concrete provider method, ignoring dynamic Mock/SimpleNamespace fallbacks."""
    if any(cls.__module__ == "unittest.mock" for cls in type(provider).__mro__):
        return None
    if method_name in getattr(provider, "__dict__", {}):
        candidate = provider.__dict__[method_name]
        return candidate if callable(candidate) else None
    cls_attr = getattr(type(provider), method_name, None)
    return getattr(provider, method_name) if callable(cls_attr) else None


def _dispatch_firmware(
    handler: GatewayHandler, operation: str, fields: dict[str, object]
) -> object:
    method_name = f"firmware_{operation}"
    provider_method = _provider_implements(handler.server.provider, method_name)
    if provider_method is not None:
        return provider_method(**fields)
    caller = facade_attr("rp_ylx.api.gateway", "request_firmware", request_firmware)
    return caller(operation, **fields)


def send_web_asset(handler: GatewayHandler, name: str, *, head: bool = False) -> None:
    host_ok, _ = handler._check_gateway_host("webAsset")
    if not host_ok:
        return
    try:
        payload = read_asset(name)
        content_type = asset_content_type(name)
    except (EchoWebArtifactError, OSError, ValueError):
        handler._problem(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            "embedded_web_unavailable",
            "内嵌设备工作台资源不可用",
        )
        return
    handler.send_response(HTTPStatus.OK)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(payload)))
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Security-Policy", WEB_CONTENT_SECURITY_POLICY)
    handler.send_header("Cross-Origin-Resource-Policy", "same-origin")
    handler.send_header("Referrer-Policy", "no-referrer")
    handler.send_header("X-Content-Type-Options", "nosniff")
    handler.send_header("X-Frame-Options", "DENY")
    handler._send_common_tail_headers()
    handler.end_headers()
    if not head:
        handler.wfile.write(payload)


def get_device(handler: GatewayHandler, api_version: str) -> None:
    if handler._principal("getDevice") is None:
        return
    try:
        source_descriptor = handler.server.provider.device_descriptor(
            api_version, handler.server.security.profile
        )
        descriptor = project_device_descriptor(source_descriptor, api_version=api_version)
        validate_device_descriptor(
            descriptor,
            api_version=api_version,
            security_profile=handler.server.security.profile,
        )
    except InvalidSourceEvent:
        handler._invalid_source_state("daemon device descriptor 无效")
        return
    except ProviderError as error:
        handler._problem(
            error.status,
            error.code,
            error.message,
            retryable=error.retryable,
            details=error.details,
        )
        return
    except Exception:
        handler._provider_failure()
        return
    handler._send_json(HTTPStatus.OK, descriptor)


def firmware_request(handler: GatewayHandler, operation: str) -> None:
    fields: dict[str, object] = {}
    if operation in {"update", "rollback"}:
        identity = handler._command_principal("firmwareUpdate")
        if identity is None:
            return
        principal, key = identity
        body = handler._read_json()
        if body is None:
            return
        if (
            not isinstance(body, dict)
            or set(body) != {"commit"}
            or not isinstance(body.get("commit"), str)
            or _COMMIT_RE.fullmatch(body["commit"]) is None
        ):
            handler._problem(
                HTTPStatus.BAD_REQUEST, "invalid_request", "更新请求必须指定完整 commit"
            )
            return
        fields = {"key": key, "commit": body["commit"]}
    else:
        principal = handler._principal("firmwareStatus")
        if principal is None:
            return
        if operation == "check":
            query = handler.query_params()
            if set(query) - {"force"} or query.get("force", ["false"]) not in (
                ["true"],
                ["false"],
            ):
                handler._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "force 参数无效")
                return
            fields = {"force": query.get("force") == ["true"]}

    slots = handler.server.firmware_slots
    if not slots.acquire(timeout=0.1):
        handler._problem(
            HTTPStatus.SERVICE_UNAVAILABLE,
            "firmware_busy",
            "固件控制通道繁忙，请稍后重试",
            retryable=True,
            headers={"Retry-After": "1"},
        )
        return
    try:
        try:
            result = _dispatch_firmware(handler, operation, fields)
        except FirmwareError as error:
            handler._problem(
                error.status,
                error.code,
                error.message,
                retryable=error.status >= HTTPStatus.INTERNAL_SERVER_ERROR,
            )
            return
        except ProviderError as error:
            handler._problem(
                error.status,
                error.code,
                error.message,
                retryable=error.retryable,
                details=error.details,
            )
            return
        except Exception:
            handler._provider_failure()
            return
    finally:
        with suppress(ValueError):
            slots.release()

    is_mutation = operation in {"update", "rollback"}
    valid_result = _valid_firmware_task(result) if is_mutation else _valid_firmware_status(result)
    if not valid_result:
        handler._invalid_source_state("固件控制响应无效")
        return
    handler._audit("firmware" + operation.title(), None, "completed", principal)
    handler._send_json(
        HTTPStatus.ACCEPTED if is_mutation else HTTPStatus.OK,
        result,
    )


def voice_request(handler: GatewayHandler, *, write: bool) -> None:
    principal = (
        handler._write_principal("setVoiceVolume") if write else handler._principal("getVoice")
    )
    if principal is None:
        return
    body = handler._read_json() if write else {}
    if body is None:
        return
    if write and (
        set(body) != {"schema", "volume"}
        or body.get("schema") != "ylx.voice-set.v1"
        or not valid_volume(body.get("volume"))
    ):
        handler._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "提示音量须为0至100的整数")
        return
    method = getattr(handler.server.provider, "set_voice_volume" if write else "voice_status", None)
    if not callable(method):
        handler._problem(HTTPStatus.NOT_FOUND, "not_found", "设备不支持提示音量设置")
        return
    try:
        result = method(body["volume"]) if write else method()
    except ProviderError as error:
        handler._problem(
            error.status,
            error.code,
            error.message,
            retryable=error.retryable,
            details=error.details,
        )
        return
    except Exception:
        handler._provider_failure()
        return
    if (
        not isinstance(result, dict)
        or result.get("schema") != "ylx.voice.v1"
        or type(result.get("enabled")) is not bool
        or not valid_volume(result.get("volume"))
    ):
        handler._invalid_source_state("设备提示音状态无效")
        return
    handler._send_json(HTTPStatus.OK, result)


def clock_request(handler: GatewayHandler, *, sync: bool, extended: bool = False) -> None:
    if sync:
        principal = handler._write_principal("syncDeviceClock")
    elif extended:
        principal = handler._principal("inspectDeviceClock")
    else:
        principal = handler._principal("getDeviceClock")
    if principal is None:
        return
    body = handler._read_json() if sync else {}
    if body is None:
        return
    if sync and not valid_clock_request(body):
        handler._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "客户端日期或校时请求无效")
        return
    try:
        result = (
            handler.server.provider.sync_clock(principal.principal_id, body)
            if sync
            else handler.server.provider.clock_inspect(principal.principal_id)
            if extended
            else handler.server.provider.clock_status(principal.principal_id)
        )
    except ProviderError as error:
        handler._problem(
            error.status,
            error.code,
            error.message,
            retryable=error.retryable,
            details=error.details,
        )
        return
    except Exception:
        handler._provider_failure()
        return
    if not valid_clock_response(result):
        handler._invalid_source_state("设备校时响应无效")
        return
    handler._send_json(HTTPStatus.OK, result)
