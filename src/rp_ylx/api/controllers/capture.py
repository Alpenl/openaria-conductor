"""Capture status, capture commands, SSE events, safe-swap, timer, focus, readiness, preview."""

from __future__ import annotations

import json
from collections.abc import Mapping
from contextlib import suppress
from http import HTTPStatus
from typing import TYPE_CHECKING

from rp_ylx.api.controllers.types import CaptureCommand, ProviderError
from rp_ylx.api.preview import PreviewFrameUnavailable
from rp_ylx.api.streams import InvalidEventCursor, UnsupportedEventVersion, heartbeat_comment
from rp_ylx.api.validators.commands import valid_camera_focus_body, valid_capture_body
from rp_ylx.api.validators.events import (
    CALIBRATION_CAPTURE_DISABLED_REASONS,
    InvalidSourceEvent,
    project_capture_status,
    validate_capture_status,
    validate_safe_swap_v3_receipt,
)
from rp_ylx.camera_focus import valid_camera_focus_status
from rp_ylx.capture_readiness import valid_readiness
from rp_ylx.capture_timer import valid_timer_settings, valid_timer_status

if TYPE_CHECKING:
    from rp_ylx.api.http_server import GatewayHandler


def _send_camera_provider_error(handler: GatewayHandler, error: ProviderError) -> None:
    if error.code == "calibration_unavailable":
        details = error.details
        if (
            error.status != HTTPStatus.SERVICE_UNAVAILABLE
            or not isinstance(details, Mapping)
            or set(details) != {"reason"}
            or details.get("reason") not in CALIBRATION_CAPTURE_DISABLED_REASONS
        ):
            handler._invalid_source_state("daemon calibration_unavailable 响应无效")
            return
    headers = (
        {"YLX-Error-Code": error.code}
        if error.code in {"camera_not_connected", "calibration_unavailable"}
        else None
    )
    handler._problem(
        error.status,
        error.code,
        error.message,
        retryable=error.retryable,
        headers=headers,
        details=error.details,
    )


def _send_capture_status(
    handler: GatewayHandler,
    status: int,
    body: object,
    *,
    api_version: str,
    headers: Mapping[str, str] | None = None,
) -> None:
    try:
        validate_capture_status(body, api_version=api_version)
    except InvalidSourceEvent:
        handler._invalid_source_state("daemon capture status 无效")
        return
    handler._send_json(status, body, headers=headers)


def _send_camera_focus_status(
    handler: GatewayHandler,
    status: int,
    body: object,
    *,
    headers: Mapping[str, str] | None = None,
) -> None:
    if not valid_camera_focus_status(body):
        handler._invalid_source_state("daemon camera focus 状态无效")
        return
    handler._send_json(status, body, headers=headers)


def get_capture_status(handler: GatewayHandler, api_version: str) -> None:
    if handler._principal("getCaptureStatus") is None:
        return
    try:
        source_body = handler.server.provider.capture_status()
        body = project_capture_status(source_body, api_version=api_version)
    except ProviderError as error:
        _send_camera_provider_error(handler, error)
        return
    except Exception:
        handler._provider_failure()
        return
    _send_capture_status(handler, HTTPStatus.OK, body, api_version=api_version)


def get_safe_swap(handler: GatewayHandler, api_version: str) -> None:
    if handler._principal("getCurrentSafeSwapReceipt") is None:
        return
    try:
        resource = handler.server.provider.current_safe_swap_receipt()
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
    if not isinstance(resource, Mapping) or set(resource) != {"schema", "receipt"}:
        handler._problem(HTTPStatus.NOT_FOUND, "not_found", "当前没有可读取的安全换盘回执")
        return
    schema = resource.get("schema")
    receipt = resource.get("receipt")
    expected_schema_version = "v2" if api_version == "v2" else "v3"
    expected_schema = f"ylx.safe-swap-receipt-resource.{expected_schema_version}"
    if schema != expected_schema or not isinstance(receipt, Mapping):
        handler._problem(HTTPStatus.NOT_FOUND, "not_found", "当前没有可读取的安全换盘回执")
        return
    if schema == "ylx.safe-swap-receipt-resource.v3":
        try:
            validate_safe_swap_v3_receipt(receipt)
        except InvalidSourceEvent:
            handler._problem(HTTPStatus.NOT_FOUND, "not_found", "当前没有可读取的安全换盘回执")
            return
    elif receipt.get("schema") != "ylx.safe-swap-receipt.v2":
        handler._problem(HTTPStatus.NOT_FOUND, "not_found", "当前没有可读取的安全换盘回执")
        return
    handler._send_json(HTTPStatus.OK, resource)


def get_camera_focus(handler: GatewayHandler) -> None:
    if handler._principal("getCameraFocus") is None:
        return
    try:
        status = handler.server.provider.camera_focus_status()
    except ProviderError as error:
        _send_camera_provider_error(handler, error)
        return
    except Exception:
        handler._provider_failure()
        return
    if status is None:
        handler._problem(
            HTTPStatus.NOT_FOUND,
            "camera_focus_unsupported",
            "当前相机没有可读取的焦距控制",
        )
        return
    _send_camera_focus_status(handler, HTTPStatus.OK, status)


def capture_readiness_request(handler: GatewayHandler) -> None:
    if handler._principal("getCaptureReadiness") is None:
        return
    method = getattr(handler.server.provider, "capture_readiness", None)
    if not callable(method):
        handler._problem(HTTPStatus.NOT_FOUND, "not_found", "设备不支持录制准入详情")
        return
    try:
        result = method()
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
    if not valid_readiness(result):
        handler._invalid_source_state("录制准入详情无效")
        return
    handler._send_json(HTTPStatus.OK, result)


def capture_timer_request(handler: GatewayHandler, *, write: bool) -> None:
    principal = (
        handler._write_principal("setCaptureTimer")
        if write
        else handler._principal("getCaptureTimer")
    )
    if principal is None:
        return
    body = handler._read_json() if write else {}
    if body is None:
        return
    if write and (
        set(body) != {"schema", "enabled", "duration_seconds"}
        or body.get("schema") != "ylx.capture-timer-set.v1"
        or not valid_timer_settings({k: v for k, v in body.items() if k != "schema"})
    ):
        handler._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "定时设置无效")
        return
    method = getattr(
        handler.server.provider, "set_capture_timer" if write else "capture_timer_status", None
    )
    if not callable(method):
        handler._problem(HTTPStatus.NOT_FOUND, "not_found", "设备不支持定时录制")
        return
    try:
        result = method({k: v for k, v in body.items() if k != "schema"}) if write else method()
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
    if not valid_timer_status(result):
        handler._invalid_source_state("设备定时状态无效")
        return
    handler._send_json(HTTPStatus.OK, result)


def capture_command(handler: GatewayHandler, api_version: str, operation: str) -> None:
    operation_id = "startCapture" if operation == "start" else "stopCapture"
    command_identity = handler._command_principal(operation_id)
    if command_identity is None:
        return
    principal, key = command_identity
    body = handler._read_json()
    if body is None:
        return
    if not valid_capture_body(operation, body):
        handler._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "请求体不符合捕获命令契约")
        return
    canonical = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    command = CaptureCommand(principal.principal_id, key, body, canonical)
    try:
        if operation == "start":
            result = handler.server.provider.start_capture(command)
        else:
            request_stop = getattr(handler.server.provider, "request_stop_capture", None)
            result = (
                request_stop(command)
                if callable(request_stop)
                else handler.server.provider.stop_capture(command)
            )
    except ProviderError as error:
        _send_camera_provider_error(handler, error)
        return
    except Exception:
        handler._provider_failure()
        return
    headers = {"Idempotency-Replayed": "true"} if result.replayed else None
    if operation == "stop" and result.status == HTTPStatus.NO_CONTENT and result.body is None:
        handler._send_empty(result.status, headers=headers)
    elif result.status == HTTPStatus.ACCEPTED:
        projected = project_capture_status(result.body, api_version=api_version)
        _send_capture_status(
            handler,
            result.status,
            projected,
            api_version=api_version,
            headers=headers,
        )
    else:
        handler._problem(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            "invalid_source_state",
            "daemon capture command 响应无效",
        )


def camera_focus_command(handler: GatewayHandler) -> None:
    command_identity = handler._command_principal("setCameraFocus")
    if command_identity is None:
        return
    principal, key = command_identity
    body = handler._read_json()
    if body is None:
        return
    if not valid_camera_focus_body(body):
        handler._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "请求体不符合相机焦距契约")
        return
    canonical = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    command = CaptureCommand(principal.principal_id, key, body, canonical)
    try:
        result = handler.server.provider.set_camera_focus(command)
    except ProviderError as error:
        _send_camera_provider_error(handler, error)
        return
    except Exception:
        handler._provider_failure()
        return
    headers = {"Idempotency-Replayed": "true"} if result.replayed else None
    if result.status == HTTPStatus.OK:
        _send_camera_focus_status(handler, result.status, result.body, headers=headers)
        return
    handler._problem(
        HTTPStatus.INTERNAL_SERVER_ERROR,
        "invalid_source_state",
        "daemon camera focus command 响应无效",
    )


def get_preview(handler: GatewayHandler, query: Mapping[str, list[str]]) -> None:
    if handler._principal("getPreview") is None:
        return
    if set(query) - {"fps"} or any(len(values) != 1 for values in query.values()):
        handler._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "预览查询参数无效")
        return
    fps_value = query.get("fps", [None])[0]
    fps = None
    if fps_value is not None:
        try:
            fps = int(fps_value) if fps_value.isascii() and fps_value.isdecimal() else 0
        except ValueError:
            fps = 0
        if fps < 1:
            handler._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "fps 必须是正整数")
            return
    requested = ",".join(handler.headers.get_all("Accept", failobj=[])).lower()
    accept = (
        "multipart/x-mixed-replace" if "multipart/x-mixed-replace" in requested else "image/jpeg"
    )
    streaming = accept == "multipart/x-mixed-replace"
    session = (
        handler.server.begin_stream("preview", handler.connection, lambda: None)
        if streaming
        else None
    )
    if streaming and session is None:
        handler._problem(
            HTTPStatus.SERVICE_UNAVAILABLE,
            "preview_capacity_exhausted",
            "预览流连接数已达设备上限",
            retryable=True,
            headers={"Retry-After": "1"},
        )
        return
    close_reason = "response_complete"
    try:
        try:
            response = handler.server.provider.latest_preview(fps=fps, accept=accept)
        except PreviewFrameUnavailable:
            handler._problem(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "preview_unavailable",
                "当前没有可用的预览帧",
                retryable=True,
            )
            return
        except ProviderError as error:
            _send_camera_provider_error(handler, error)
            return
        except Exception:
            handler._provider_failure()
            return
        if session is not None and not session.attach_body(response.body):
            return
        handler.send_response(HTTPStatus.OK)
        handler.send_header("Content-Type", response.content_type)
        if response.content_length is not None:
            handler.send_header("Content-Length", str(response.content_length))
        else:
            handler.close_connection = True
        handler.send_header("Cache-Control", "no-store")
        handler._send_common_tail_headers()
        handler.end_headers()
        try:
            if isinstance(response.body, bytes):
                handler.wfile.write(response.body)
            else:
                for chunk in response.body:
                    if session is not None and session.cancelled.is_set():
                        return
                    handler.wfile.write(chunk)
                    handler.wfile.flush()
                close_reason = "iterator_eof"
        except OSError:
            handler.close_connection = True
            close_reason = "write_error"
            return
        except Exception as error:
            close_reason = "stream_error"
            handler._log_midstream_abort(error)
            return
        finally:
            if session is None:
                close = getattr(response.body, "close", None)
                if close is not None:
                    with suppress(Exception):
                        close()
    finally:
        if session is not None:
            session.finish(close_reason)


def capture_events(
    handler: GatewayHandler, api_version: str, query: Mapping[str, list[str]]
) -> None:
    if handler._principal("streamCaptureEvents") is None:
        return
    if query:
        handler._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "SSE 不接受查询参数")
        return
    last_event_id, last_event_id_is_single = handler._single_header("Last-Event-ID")
    if not last_event_id_is_single:
        handler._problem(
            HTTPStatus.BAD_REQUEST,
            "invalid_request",
            "Last-Event-ID 只能出现一次",
        )
        return
    session = handler.server.begin_stream(
        "sse",
        handler.connection,
        handler.server.event_buffer.wake_waiters,
    )
    if session is None:
        handler._problem(
            HTTPStatus.SERVICE_UNAVAILABLE,
            "stream_capacity_exhausted",
            "SSE 连接数已达设备上限",
            retryable=True,
            headers={"Retry-After": "1"},
        )
        return
    close_reason = "response_complete"
    try:
        try:
            events = handler.server.event_buffer.replay(
                last_event_id,
                api_version=api_version,
                snapshot=handler.server.provider.capture_snapshot_event,
            )
        except InvalidEventCursor:
            handler._problem(
                HTTPStatus.BAD_REQUEST,
                "invalid_request",
                "Last-Event-ID 必须是十进制 delivery ID",
            )
            return
        except InvalidSourceEvent:
            handler._problem(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "invalid_source_event",
                "daemon source event 无效",
            )
            return
        except UnsupportedEventVersion:
            handler._problem(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "event_version_unsupported",
                "事件不能投影到请求的 API 版本",
            )
            return
        except ProviderError as error:
            _send_camera_provider_error(handler, error)
            return
        except Exception:
            handler._provider_failure()
            return
        if session.cancelled.is_set():
            return
        handler.close_connection = True
        handler.send_response(HTTPStatus.OK)
        handler.send_header("Content-Type", "text/event-stream")
        handler.send_header("Cache-Control", "no-cache")
        handler.send_header("X-Accel-Buffering", "no")
        handler._send_common_tail_headers()
        handler.end_headers()
        try:
            for event in events:
                handler.wfile.write(event.encode())
            handler.wfile.flush()
            cursor = events[-1].delivery_id if events else int(last_event_id)
            while not session.cancelled.is_set():
                delivered = handler.server.event_buffer.wait_after(
                    cursor,
                    handler.server.sse_heartbeat_seconds,
                    api_version=api_version,
                    snapshot=handler.server.provider.capture_snapshot_event,
                    cancel_event=session.cancelled,
                )
                if session.cancelled.is_set():
                    return
                if delivered:
                    for event in delivered:
                        handler.wfile.write(event.encode())
                    cursor = delivered[-1].delivery_id
                else:
                    handler.wfile.write(heartbeat_comment())
                handler.wfile.flush()
        except UnsupportedEventVersion:
            close_reason = "event_version_unsupported"
            return
        except (BrokenPipeError, ConnectionResetError, InvalidSourceEvent, OSError):
            close_reason = "write_error"
            return
        except Exception as error:
            close_reason = "stream_error"
            handler._log_midstream_abort(error)
            return
    finally:
        session.finish(close_reason)
