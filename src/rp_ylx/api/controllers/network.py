"""Network status, scan, SSE event stream (shared poller), credentials, and mutations."""

from __future__ import annotations

import json
import re
import time
from collections.abc import Mapping
from http import HTTPStatus
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from rp_ylx.api.controllers.types import NetworkCommand, NetworkCredentialCommand, ProviderError
from rp_ylx.api.streams import heartbeat_comment
from rp_ylx.api.validators.network import (
    valid_network_credential_receipt,
    valid_network_provider_error,
    valid_network_receipt,
    valid_network_scan,
    valid_network_status,
)
from rp_ylx.network_validation import valid_network_request

if TYPE_CHECKING:
    from rp_ylx.api.http_server import GatewayHandler


def _send_network_provider_error(
    handler: GatewayHandler, error: ProviderError, *, operation: str
) -> None:
    if not valid_network_provider_error(error, operation=operation):
        handler._invalid_source_state("daemon network error 响应无效")
        return
    handler._problem(
        error.status,
        error.code,
        error.message,
        retryable=error.retryable,
        headers={"YLX-Error-Code": error.code},
        details=error.details,
    )


def get_network_status(handler: GatewayHandler) -> None:
    if handler._principal("getNetworkStatus") is None:
        return
    try:
        status = handler.server.provider.network_status()
    except ProviderError as error:
        _send_network_provider_error(handler, error, operation="status")
        return
    except Exception:
        handler._provider_failure()
        return
    if not valid_network_status(status):
        handler._invalid_source_state("daemon network 状态无效")
        return
    handler._send_json(HTTPStatus.OK, status)


def get_network_scan(handler: GatewayHandler) -> None:
    if handler._principal("scanNetworks") is None:
        return
    try:
        body = handler.server.provider.scan_networks()
    except ProviderError as error:
        _send_network_provider_error(handler, error, operation="scan")
        return
    except Exception:
        handler._provider_failure()
        return
    if not valid_network_scan(body):
        handler._invalid_source_state("daemon network scan 无效")
        return
    handler._send_json(HTTPStatus.OK, body)


def network_events(handler: GatewayHandler) -> None:
    if handler._principal("streamNetworkEvents") is None:
        return
    parsed = urlsplit(handler.path)
    if parsed.query:
        handler._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "SSE 不接受查询参数")
        return
    last_event_id, last_event_id_is_single = handler._single_header("Last-Event-ID")
    if not last_event_id_is_single or (
        last_event_id is not None
        and (re.fullmatch(r"[0-9]+", last_event_id) is None or len(last_event_id) > 128)
    ):
        handler._problem(
            HTTPStatus.BAD_REQUEST,
            "invalid_request",
            "Last-Event-ID 必须是十进制 delivery ID",
        )
        return
    session = handler.server.begin_stream(
        "sse",
        handler.connection,
        handler.server.network_event_buffer.wake_waiters,
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
            status = handler.server.provider.network_status()
        except ProviderError as error:
            _send_network_provider_error(handler, error, operation="status")
            return
        except Exception:
            handler._provider_failure()
            return
        if not valid_network_status(status):
            handler._invalid_source_state("daemon network 状态无效")
            return
        if session.cancelled.is_set():
            return
        cursor = None if last_event_id is None else int(last_event_id)
        events = handler.server.network_event_buffer.replay(cursor, status)
        handler.close_connection = True
        handler.send_response(HTTPStatus.OK)
        handler.send_header("Content-Type", "text/event-stream")
        handler.send_header("Cache-Control", "no-cache")
        handler.send_header("X-Accel-Buffering", "no")
        handler._send_common_tail_headers()
        handler.end_headers()
        poller = handler.server.network_poller
        poller.acquire()
        try:
            for event in events:
                handler.wfile.write(event.encode())
            handler.wfile.flush()
            if events:
                cursor = events[-1].delivery_id
            if cursor is None:
                return
            heartbeat_at = time.monotonic() + handler.server.sse_heartbeat_seconds
            while not session.cancelled.is_set():
                poller.check_error()
                remaining = max(0.0, heartbeat_at - time.monotonic())
                delivered = handler.server.network_event_buffer.wait_after(
                    cursor,
                    remaining,
                    status_provider=handler.server.provider.network_status,
                    cancel_event=session.cancelled,
                )
                poller.check_error()
                if session.cancelled.is_set():
                    return
                if delivered:
                    for event in delivered:
                        handler.wfile.write(event.encode())
                    cursor = delivered[-1].delivery_id
                    heartbeat_at = time.monotonic() + handler.server.sse_heartbeat_seconds
                elif time.monotonic() >= heartbeat_at:
                    handler.wfile.write(heartbeat_comment())
                    heartbeat_at = time.monotonic() + handler.server.sse_heartbeat_seconds
                else:
                    continue
                handler.wfile.flush()
        except OSError:
            handler.close_connection = True
            close_reason = "write_error"
            return
        except Exception as error:
            close_reason = "stream_error"
            handler._log_midstream_abort(error)
            return
        finally:
            poller.release()
    finally:
        session.finish(close_reason)


def create_network_credential(handler: GatewayHandler) -> None:
    principal = handler._write_principal("createNetworkCredentialReference")
    if principal is None:
        return
    body = handler._read_json()
    if body is None:
        return
    passphrase = body.get("passphrase") if isinstance(body, Mapping) else None
    if not (
        isinstance(body, Mapping)
        and set(body) == {"schema", "passphrase"}
        and body.get("schema") == "ylx.network-credential-request.v1"
        and isinstance(passphrase, str)
        and 8 <= len(passphrase.encode("utf-8")) <= 63
        and not any(ord(character) < 32 for character in passphrase)
    ):
        handler._problem(
            HTTPStatus.BAD_REQUEST,
            "invalid_request",
            "请求体不符合网络凭据契约",
        )
        return
    try:
        receipt = handler.server.provider.create_network_credential(
            NetworkCredentialCommand(principal.principal_id, passphrase)
        )
    except ProviderError as error:
        _send_network_provider_error(handler, error, operation="credentials")
        return
    except Exception:
        handler._provider_failure()
        return
    if not valid_network_credential_receipt(receipt):
        handler._invalid_source_state("daemon network credential 响应无效")
        return
    handler._send_json(HTTPStatus.CREATED, receipt)


def network_command(handler: GatewayHandler, operation: str) -> None:
    operation_id = {
        "apply": "applyNetworkDesiredState",
        "retry": "retryNetworkTransaction",
        "forget": "forgetNetworkClientProfile",
    }[operation]
    command_identity = handler._command_principal(operation_id)
    if command_identity is None:
        return
    principal, key = command_identity
    body = handler._read_json()
    if body is None:
        return
    if not valid_network_request(operation, body):
        handler._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "请求体不符合网络命令契约")
        return
    canonical = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    command = NetworkCommand(principal.principal_id, key, body, canonical)
    try:
        if operation == "apply":
            result = handler.server.provider.apply_network_desired_state(command)
        elif operation == "retry":
            result = handler.server.provider.retry_network_transaction(command)
        else:
            result = handler.server.provider.forget_network_client_profile(command)
    except ProviderError as error:
        _send_network_provider_error(handler, error, operation=operation)
        return
    except Exception:
        handler._provider_failure()
        return
    headers = {"Idempotency-Replayed": "true"} if result.replayed else None
    if result.status == HTTPStatus.ACCEPTED and valid_network_receipt(result.body):
        handler._send_json(result.status, result.body, headers=headers)
        return
    handler._problem(
        HTTPStatus.INTERNAL_SERVER_ERROR,
        "invalid_source_state",
        "daemon network command 响应无效",
    )
