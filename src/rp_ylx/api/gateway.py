"""冻结 Device API v2 与当前 v3 的共享 HTTP gateway。"""

from __future__ import annotations

import hmac
import json
import re
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import BoundedSemaphore
from typing import Protocol
from urllib.parse import parse_qs, urlsplit

from rp_ylx.api.downloads import (
    ArtifactAccessError,
    UnsatisfiableRange,
    parse_single_range,
)
from rp_ylx.api.events import (
    EventReplayBuffer,
    InvalidEventCursor,
    InvalidSourceEvent,
    UnsupportedEventVersion,
    heartbeat_comment,
    validate_capture_status,
    validate_device_descriptor,
    validate_retained_unsuccessful_outcome,
    validate_safe_swap_v3_receipt,
    validate_session_list,
)
from rp_ylx.api.preview import PreviewFrameUnavailable
from rp_ylx.api.security import AuditEvent, Principal, SecurityPolicy
from rp_ylx.web import WEB_ASSETS, read_asset

MAX_BODY_BYTES = 64 * 1024
UUID_V4 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
UUID_V7 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
WEB_PATHS = {"/": "index.html", **{f"/{name}": name for name in WEB_ASSETS if name != "index.html"}}
WEB_CONTENT_TYPES = {
    "index.html": "text/html; charset=utf-8",
    "styles.css": "text/css; charset=utf-8",
    "app.js": "text/javascript; charset=utf-8",
    "api-client.js": "text/javascript; charset=utf-8",
    "state.js": "text/javascript; charset=utf-8",
    "event-stream.js": "text/javascript; charset=utf-8",
    "preview.js": "text/javascript; charset=utf-8",
}
WEB_CONTENT_SECURITY_POLICY = (
    "default-src 'self'; base-uri 'none'; connect-src 'self'; form-action 'self'; "
    "frame-ancestors 'none'; img-src 'self' blob: data:; object-src 'none'; "
    "script-src 'self'; style-src 'self'"
)


def _valid_session_id(api_version: str, session_id: str) -> bool:
    return UUID_V7.fullmatch(session_id) is not None or (
        api_version == "v2" and UUID_V4.fullmatch(session_id) is not None
    )


@dataclass(frozen=True, slots=True)
class CaptureCommand:
    principal_id: str
    idempotency_key: str
    body: Mapping[str, object]
    canonical_body: bytes


@dataclass(frozen=True, slots=True)
class CaptureCommandResult:
    status: int
    body: object | None
    replayed: bool = False


class ProviderError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        status: int,
        retryable: bool = False,
        details: Mapping[str, object] | None = None,
    ) -> None:
        self.code = code
        self.message = message
        self.status = status
        self.retryable = retryable
        self.details = details
        super().__init__(message)


class DeviceProvider(Protocol):
    """DX-02 后续接入的窄设备守护进程边界。"""

    def device_descriptor(
        self, api_version: str, security_profile: str
    ) -> Mapping[str, object]: ...

    def capture_status(self) -> Mapping[str, object]: ...

    def start_capture(self, command: CaptureCommand) -> CaptureCommandResult: ...

    def stop_capture(self, command: CaptureCommand) -> CaptureCommandResult: ...

    def camera_focus_status(self) -> Mapping[str, object] | None: ...

    def set_camera_focus(self, command: CaptureCommand) -> CaptureCommandResult: ...

    def list_sessions(
        self, *, cursor: str | None, limit: int, take_id: str | None
    ) -> Mapping[str, object]: ...

    def open_manifest(self, session_id: str, api_version: str) -> LockedRepresentation: ...

    def retained_unsuccessful_outcome(self, session_id: str) -> object | None: ...

    def current_safe_swap_receipt(self) -> object | None: ...

    def capture_snapshot_event(self) -> Mapping[str, object]: ...

    def latest_preview(self, *, fps: int | None, accept: str) -> PreviewResponse: ...

    def artifact_io_state(self) -> str | None: ...

    def open_verified_artifact(
        self, session_id: str, artifact_id: str, api_version: str
    ) -> LockedRepresentation: ...


class LockedRepresentation(Protocol):
    etag: str
    size: int
    content_type: str

    def __enter__(self) -> LockedRepresentation: ...

    def __exit__(self, *args: object) -> None: ...

    def iter_chunks(
        self,
        offset: int = 0,
        length: int | None = None,
        *,
        chunk_size: int = 1024 * 1024,
    ) -> Iterator[bytes]: ...

    def send_to(
        self, output_descriptor: int, offset: int = 0, length: int | None = None
    ) -> int: ...


class PreviewResponse(Protocol):
    content_type: str
    body: bytes | Iterator[bytes]
    content_length: int | None


class GatewayServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        provider: DeviceProvider,
        security: SecurityPolicy,
        audit_sink: Callable[[AuditEvent], None] | None,
        event_buffer: EventReplayBuffer,
        sse_heartbeat_seconds: float = 15.0,
        max_sse_connections: int = 4,
        max_preview_streams: int = 2,
    ) -> None:
        if sse_heartbeat_seconds <= 0:
            raise ValueError("sse_heartbeat_seconds 必须大于零")
        if max_sse_connections < 1 or max_preview_streams < 1:
            raise ValueError("长连接并发上限必须大于零")
        self.provider = provider
        self.security = security
        self.audit_sink = audit_sink or (lambda event: None)
        self.event_buffer = event_buffer
        self.sse_heartbeat_seconds = sse_heartbeat_seconds
        self.sse_slots = BoundedSemaphore(max_sse_connections)
        self.preview_stream_slots = BoundedSemaphore(max_preview_streams)
        super().__init__(address, GatewayHandler)


class GatewayHandler(BaseHTTPRequestHandler):
    server: GatewayServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:
        return

    @property
    def request_id(self) -> str:
        request_id = getattr(self, "_request_id", None)
        if request_id is None:
            request_id = str(uuid.uuid4())
            self._request_id = request_id
        return request_id

    def _single_header(self, name: str) -> tuple[str | None, bool]:
        values = self.headers.get_all(name, failobj=[])
        if len(values) > 1:
            return None, False
        return (values[0] if values else None), True

    def _is_same_gateway_origin(self, origin: str | None) -> bool:
        if origin is None:
            return False
        host, host_is_single = self._single_header("Host")
        if not host_is_single or host is None:
            return False
        parsed = urlsplit(origin)
        return (
            parsed.scheme == "http"
            and parsed.netloc.casefold() == host.casefold()
            and not parsed.path
            and not parsed.query
            and not parsed.fragment
        )

    def _cors_headers(self) -> None:
        origin, unambiguous = self._single_header("Origin")
        if unambiguous and origin and origin in self.server.security.allowed_origins:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header(
                "Access-Control-Expose-Headers",
                (
                    "Accept-Ranges, Content-Range, ETag, Idempotency-Replayed, Retry-After, "
                    "YLX-Error-Code, YLX-Manifest-SHA256, YLX-Wait-State"
                ),
            )
            self.send_header("Vary", "Origin")

    def _send_json(
        self,
        status: int,
        body: object,
        *,
        headers: Mapping[str, str] | None = None,
        content_type: str = "application/json; charset=utf-8",
    ) -> None:
        payload = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        if headers:
            for name, value in headers.items():
                self.send_header(name, value)
        self._cors_headers()
        self.end_headers()
        self.wfile.write(payload)

    def _send_empty(
        self,
        status: int,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.send_header("Cache-Control", "no-store")
        if headers:
            for name, value in headers.items():
                self.send_header(name, value)
        self._cors_headers()
        self.end_headers()

    def _send_web_asset(self, name: str) -> None:
        try:
            payload = read_asset(name)
        except OSError:
            self._problem(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "embedded_web_unavailable",
                "内嵌设备工作台资源不可用",
            )
            return
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", WEB_CONTENT_TYPES[name])
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Security-Policy", WEB_CONTENT_SECURITY_POLICY)
        self.send_header("Cross-Origin-Resource-Policy", "same-origin")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.end_headers()
        self.wfile.write(payload)

    def _send_representation(
        self,
        representation: LockedRepresentation,
        *,
        status: int = HTTPStatus.OK,
        offset: int = 0,
        length: int | None = None,
        content_range: str | None = None,
        head: bool = False,
    ) -> None:
        selected_length = representation.size if length is None else length
        self.send_response(status)
        self.send_header("Content-Type", representation.content_type)
        self.send_header("Content-Length", str(selected_length))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("ETag", representation.etag)
        self.send_header("Cache-Control", "no-store")
        if content_range is not None:
            self.send_header("Content-Range", content_range)
        self._cors_headers()
        self.end_headers()
        if not head:
            send_to = getattr(representation, "send_to", None)
            if callable(send_to):
                self.wfile.flush()
                send_to(self.connection.fileno(), offset, selected_length)
            else:
                for chunk in representation.iter_chunks(offset, selected_length):
                    self.wfile.write(chunk)

    def _problem(
        self,
        status: int,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        headers: Mapping[str, str] | None = None,
        details: Mapping[str, object] | None = None,
    ) -> None:
        error: dict[str, object] = {
            "code": code,
            "message": message,
            "request_id": self.request_id,
            "retryable": retryable,
        }
        if details is not None:
            error["details"] = dict(details)
        self._send_json(
            status,
            {
                "schema": "ylx.api-error.v2",
                "error": error,
            },
            headers=headers,
            content_type="application/problem+json",
        )

    def _audit(
        self,
        operation_id: str,
        resource_id: str | None,
        outcome: str,
        principal: Principal | None = None,
    ) -> None:
        self.server.audit_sink(
            AuditEvent(
                request_id=self.request_id,
                principal_id=None if principal is None else principal.principal_id,
                operation_id=operation_id,
                resource_id=resource_id,
                outcome=outcome,
            )
        )

    def _principal(self, operation_id: str, resource_id: str | None = None) -> Principal | None:
        origin, origin_is_single = self._single_header("Origin")
        if not origin_is_single or (
            origin
            and not self._is_same_gateway_origin(origin)
            and origin not in self.server.security.allowed_origins
        ):
            self._audit(operation_id, resource_id, "origin_forbidden")
            if self.command == "HEAD":
                self._send_empty(HTTPStatus.FORBIDDEN)
            else:
                self._problem(HTTPStatus.FORBIDDEN, "origin_forbidden", "请求来源未获允许")
            return None
        authorization, authorization_is_single = self._single_header("Authorization")
        principal = (
            self.server.security.authenticate(authorization) if authorization_is_single else None
        )
        if principal is None:
            self._audit(operation_id, resource_id, "unauthorized")
            headers = {"WWW-Authenticate": "Bearer"}
            if self.command == "HEAD":
                self._send_empty(HTTPStatus.UNAUTHORIZED, headers=headers)
            else:
                self._problem(
                    HTTPStatus.UNAUTHORIZED,
                    "unauthorized",
                    "Bearer 凭据缺失或无效",
                    headers=headers,
                )
            return None
        if not principal.permits(operation_id, resource_id):
            self._audit(operation_id, resource_id, "forbidden", principal)
            if self.command == "HEAD":
                self._send_empty(HTTPStatus.FORBIDDEN)
            else:
                self._problem(HTTPStatus.FORBIDDEN, "forbidden", "调用方无权访问此资源")
            return None
        self._audit(operation_id, resource_id, "allowed", principal)
        return principal

    def _begin_request(self) -> None:
        self._request_id = str(uuid.uuid4())

    def _route_methods(self, path: str) -> tuple[str, ...] | None:
        parts = path.split("/")
        if len(parts) < 4 or parts[1] != "api" or parts[2] not in {"v2", "v3"}:
            return None
        if len(parts) == 4 and parts[3] in {"device", "preview", "sessions"}:
            return ("GET", "OPTIONS")
        if len(parts) == 5 and parts[3] == "capture":
            if parts[4] in {"start", "stop"}:
                return ("POST", "OPTIONS")
            if parts[4] in {"status", "events", "safe-swap"}:
                return ("GET", "OPTIONS")
        if len(parts) == 5 and parts[2] == "v3" and parts[3:] == ["camera", "focus"]:
            return ("GET", "POST", "OPTIONS")
        if len(parts) == 5 and parts[3] == "sessions" and _valid_session_id(parts[2], parts[4]):
            return ("GET", "OPTIONS")
        if (
            len(parts) == 6
            and parts[3] == "sessions"
            and _valid_session_id(parts[2], parts[4])
            and parts[5] == "unsuccessful-outcome"
        ):
            return ("GET", "OPTIONS")
        if self._artifact_route(parts) is not None:
            return ("GET", "HEAD", "OPTIONS")
        return None

    def _route_request_headers(self, path: str, method: str) -> tuple[str, ...]:
        headers = ["Authorization"]
        parts = path.split("/")
        if method == "POST":
            headers.extend(("Content-Type", "Idempotency-Key", "X-CSRF-Token"))
        elif len(parts) == 5 and parts[3:] == ["capture", "events"]:
            headers.append("Last-Event-ID")
        elif self._artifact_route(parts) is not None:
            headers.extend(("If-Range", "Range"))
        return tuple(headers)

    def do_OPTIONS(self) -> None:
        self._begin_request()
        methods = self._route_methods(urlsplit(self.path).path)
        if methods is None:
            self._problem(HTTPStatus.NOT_FOUND, "not_found", "接口不存在")
            return

        origin, origin_is_single = self._single_header("Origin")
        if not origin_is_single or origin not in self.server.security.allowed_origins:
            self._problem(HTTPStatus.FORBIDDEN, "forbidden", "请求来源不在允许列表")
            return

        requested_method = self.headers.get("Access-Control-Request-Method")
        if requested_method is None:
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "缺少预检请求方法")
            return
        requested_method = requested_method.upper()
        if requested_method not in methods:
            self._problem(
                HTTPStatus.METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "接口不允许该请求方法",
                headers={"Allow": ", ".join(methods)},
            )
            return

        path = urlsplit(self.path).path
        allowed_headers = self._route_request_headers(path, requested_method)
        requested_headers = {
            value.strip().lower()
            for value in self.headers.get("Access-Control-Request-Headers", "").split(",")
            if value.strip()
        }
        if requested_headers - {value.lower() for value in allowed_headers}:
            self._problem(HTTPStatus.FORBIDDEN, "forbidden", "预检请求头不在允许列表")
            return

        self._send_empty(
            HTTPStatus.NO_CONTENT,
            headers={
                "Access-Control-Allow-Methods": ", ".join(methods),
                "Access-Control-Allow-Headers": ", ".join(allowed_headers),
                "Access-Control-Max-Age": "600",
            },
        )

    def do_GET(self) -> None:
        self._begin_request()
        parsed = urlsplit(self.path)
        path = parsed.path
        web_asset = WEB_PATHS.get(path)
        if web_asset is not None:
            self._send_web_asset(web_asset)
            return
        parts = path.split("/")
        if (
            len(parts) == 4
            and parts[1] == "api"
            and parts[2] in {"v2", "v3"}
            and parts[3] == "device"
        ):
            self._get_device(parts[2])
            return
        if (
            len(parts) == 5
            and parts[1] == "api"
            and parts[2] in {"v2", "v3"}
            and parts[3:] == ["capture", "status"]
        ):
            self._get_capture_status()
            return
        if (
            len(parts) == 5
            and parts[1] == "api"
            and parts[2] in {"v2", "v3"}
            and parts[3:] == ["capture", "events"]
        ):
            self._capture_events(parts[2], parse_qs(parsed.query, keep_blank_values=True))
            return
        if (
            len(parts) == 5
            and parts[1] == "api"
            and parts[2] in {"v2", "v3"}
            and parts[3:] == ["capture", "safe-swap"]
        ):
            self._get_safe_swap(parts[2])
            return
        if (
            len(parts) == 5
            and parts[1] == "api"
            and parts[2] == "v3"
            and parts[3:] == ["camera", "focus"]
        ):
            self._get_camera_focus()
            return
        if (
            len(parts) == 4
            and parts[1] == "api"
            and parts[2] in {"v2", "v3"}
            and parts[3] == "preview"
        ):
            self._get_preview(parse_qs(parsed.query, keep_blank_values=True))
            return
        if (
            len(parts) == 4
            and parts[1] == "api"
            and parts[2] in {"v2", "v3"}
            and parts[3] == "sessions"
        ):
            self._list_sessions(parse_qs(parsed.query, keep_blank_values=True))
            return
        if (
            len(parts) == 5
            and parts[1] == "api"
            and parts[2] in {"v2", "v3"}
            and parts[3] == "sessions"
            and _valid_session_id(parts[2], parts[4])
        ):
            self._get_manifest(parts[2], parts[4])
            return
        if (
            len(parts) == 6
            and parts[1] == "api"
            and parts[2] in {"v2", "v3"}
            and parts[3] == "sessions"
            and _valid_session_id(parts[2], parts[4])
            and parts[5] == "unsuccessful-outcome"
        ):
            self._get_retained_outcome(parts[4])
            return
        artifact = self._artifact_route(parts)
        if artifact is not None:
            api_version, session_id, artifact_id = artifact
            self._session_artifact(api_version, session_id, artifact_id, head=False)
            return
        self._problem(HTTPStatus.NOT_FOUND, "not_found", "接口不存在")

    def do_HEAD(self) -> None:
        self._begin_request()
        path = urlsplit(self.path).path
        artifact = self._artifact_route(path.split("/"))
        if artifact is not None:
            api_version, session_id, artifact_id = artifact
            self._session_artifact(api_version, session_id, artifact_id, head=True)
            return
        self._problem(HTTPStatus.NOT_FOUND, "not_found", "接口不存在")

    def do_POST(self) -> None:
        self._begin_request()
        path = urlsplit(self.path).path
        parts = path.split("/")
        if (
            len(parts) == 5
            and parts[1] == "api"
            and parts[2] in {"v2", "v3"}
            and parts[3] == "capture"
            and parts[4] in {"start", "stop"}
        ):
            self._capture_command(parts[4])
            return
        if (
            len(parts) == 5
            and parts[1] == "api"
            and parts[2] == "v3"
            and parts[3:] == ["camera", "focus"]
        ):
            self._camera_focus_command()
            return
        self._problem(HTTPStatus.NOT_FOUND, "not_found", "接口不存在")

    def _get_device(self, api_version: str) -> None:
        if self._principal("getDevice") is None:
            return
        try:
            descriptor = self.server.provider.device_descriptor(
                api_version, self.server.security.profile
            )
            validate_device_descriptor(
                descriptor,
                api_version=api_version,
                security_profile=self.server.security.profile,
            )
        except InvalidSourceEvent:
            self._invalid_source_state("daemon device descriptor 无效")
            return
        except Exception:
            self._provider_failure()
            return
        self._send_json(HTTPStatus.OK, descriptor)

    def _get_capture_status(self) -> None:
        if self._principal("getCaptureStatus") is None:
            return
        try:
            body = self.server.provider.capture_status()
        except Exception:
            self._provider_failure()
            return
        self._send_capture_status(HTTPStatus.OK, body)

    def _invalid_source_state(self, message: str) -> None:
        self._problem(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            "invalid_source_state",
            message,
        )

    def _provider_failure(self) -> None:
        self._problem(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            "provider_failure",
            "设备 provider 未能完成请求",
            retryable=True,
        )

    def _send_capture_status(
        self,
        status: int,
        body: object,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        try:
            validate_capture_status(body)
        except InvalidSourceEvent:
            self._invalid_source_state("daemon capture status 无效")
            return
        self._send_json(status, body, headers=headers)

    @staticmethod
    def _valid_camera_focus_status(value: object) -> bool:
        if not isinstance(value, Mapping) or set(value) != {
            "schema",
            "value",
            "minimum",
            "maximum",
            "step",
            "default",
            "auto_supported",
            "auto_enabled",
        }:
            return False
        return not (
            value["schema"] != "ylx.camera-focus.v1"
            or any(
                type(value[key]) is not int
                for key in ("value", "minimum", "maximum", "step", "default")
            )
            or value["minimum"] > value["maximum"]
            or value["step"] <= 0
            or not value["minimum"] <= value["value"] <= value["maximum"]
            or (value["value"] - value["minimum"]) % value["step"] != 0
            or not value["minimum"] <= value["default"] <= value["maximum"]
            or type(value["auto_supported"]) is not bool
            or (value["auto_enabled"] is not None and type(value["auto_enabled"]) is not bool)
            or (not value["auto_supported"] and value["auto_enabled"] is not None)
        )

    def _send_camera_focus_status(
        self,
        status: int,
        body: object,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        if not self._valid_camera_focus_status(body):
            self._invalid_source_state("daemon camera focus 状态无效")
            return
        self._send_json(status, body, headers=headers)

    def _capture_events(self, api_version: str, query: Mapping[str, list[str]]) -> None:
        if self._principal("streamCaptureEvents") is None:
            return
        if query:
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "SSE 不接受查询参数")
            return
        last_event_id, last_event_id_is_single = self._single_header("Last-Event-ID")
        if not last_event_id_is_single:
            self._problem(
                HTTPStatus.BAD_REQUEST,
                "invalid_request",
                "Last-Event-ID 只能出现一次",
            )
            return
        if not self.server.sse_slots.acquire(blocking=False):
            self._problem(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "stream_capacity_exhausted",
                "SSE 连接数已达设备上限",
                retryable=True,
            )
            return
        try:
            try:
                events = self.server.event_buffer.replay(
                    last_event_id,
                    api_version=api_version,
                    snapshot=self.server.provider.capture_snapshot_event,
                )
            except InvalidEventCursor:
                self._problem(
                    HTTPStatus.BAD_REQUEST,
                    "invalid_request",
                    "Last-Event-ID 必须是十进制 delivery ID",
                )
                return
            except InvalidSourceEvent:
                self._problem(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    "invalid_source_event",
                    "daemon source event 无效",
                )
                return
            except UnsupportedEventVersion:
                self._problem(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    "event_version_unsupported",
                    "事件不能投影到请求的 API 版本",
                )
                return
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("X-Accel-Buffering", "no")
            self.send_header("Connection", "keep-alive")
            self._cors_headers()
            self.end_headers()
            try:
                for event in events:
                    self.wfile.write(event.encode())
                self.wfile.flush()
                cursor = events[-1].delivery_id if events else int(last_event_id)
                while True:
                    delivered = self.server.event_buffer.wait_after(
                        cursor,
                        self.server.sse_heartbeat_seconds,
                        api_version=api_version,
                        snapshot=self.server.provider.capture_snapshot_event,
                    )
                    if delivered:
                        for event in delivered:
                            self.wfile.write(event.encode())
                        cursor = delivered[-1].delivery_id
                    else:
                        self.wfile.write(heartbeat_comment())
                    self.wfile.flush()
            except (
                BrokenPipeError,
                ConnectionResetError,
                InvalidSourceEvent,
                UnsupportedEventVersion,
            ):
                return
        finally:
            self.server.sse_slots.release()

    def _list_sessions(self, query: Mapping[str, list[str]]) -> None:
        if self._principal("listSessions") is None:
            return
        if set(query) - {"cursor", "limit", "take_id"} or any(
            len(values) != 1 for values in query.values()
        ):
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "会话查询参数无效")
            return
        cursor = query.get("cursor", [None])[0]
        if cursor == "":
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "cursor 不能为空")
            return
        limit_value = query.get("limit", ["50"])[0]
        try:
            limit = int(limit_value)
        except (TypeError, ValueError):
            limit = 0
        if not 1 <= limit <= 200:
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "limit 必须介于 1 和 200")
            return
        take_id = query.get("take_id", [None])[0]
        if take_id is not None and UUID_V7.fullmatch(take_id) is None:
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "take_id 必须是 UUIDv7")
            return
        try:
            result = self.server.provider.list_sessions(cursor=cursor, limit=limit, take_id=take_id)
            validate_session_list(result, limit=limit, take_id=take_id)
        except InvalidSourceEvent:
            self._invalid_source_state("daemon session list 无效")
            return
        except ProviderError as error:
            self._problem(
                error.status,
                error.code,
                error.message,
                retryable=error.retryable,
                details=error.details,
            )
            return
        except Exception:
            self._provider_failure()
            return
        self._send_json(HTTPStatus.OK, result)

    def _get_manifest(self, api_version: str, session_id: str) -> None:
        if self._principal("getSession", session_id) is None:
            return
        try:
            locked = self.server.provider.open_manifest(session_id, api_version)
            with locked as manifest:
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", manifest.content_type)
                self.send_header("Content-Length", str(manifest.size))
                self.send_header("ETag", manifest.etag)
                self.send_header("YLX-Manifest-SHA256", manifest.etag.strip('"'))
                self.send_header("Cache-Control", "no-store")
                self._cors_headers()
                self.end_headers()
                for chunk in manifest.iter_chunks(0, manifest.size):
                    self.wfile.write(chunk)
        except ArtifactAccessError:
            self._problem(HTTPStatus.NOT_FOUND, "not_found", "会话不存在或尚未封存")
        except ProviderError as error:
            self._problem(error.status, error.code, error.message, retryable=error.retryable)

    def _get_retained_outcome(self, session_id: str) -> None:
        if self._principal("getRetainedUnsuccessfulSessionOutcome", session_id) is None:
            return
        outcome = self.server.provider.retained_unsuccessful_outcome(session_id)
        if outcome is None:
            self._problem(HTTPStatus.NOT_FOUND, "not_found", "没有保留的失败终态")
            return
        try:
            validate_retained_unsuccessful_outcome(outcome, session_id=session_id)
        except InvalidSourceEvent:
            self._invalid_source_state("daemon retained outcome 无效")
            return
        self._send_json(HTTPStatus.OK, outcome)

    def _get_safe_swap(self, api_version: str) -> None:
        if self._principal("getCurrentSafeSwapReceipt") is None:
            return
        resource = self.server.provider.current_safe_swap_receipt()
        if not isinstance(resource, Mapping) or set(resource) != {"schema", "receipt"}:
            self._problem(HTTPStatus.NOT_FOUND, "not_found", "当前没有可读取的安全换盘回执")
            return
        schema = resource.get("schema")
        receipt = resource.get("receipt")
        expected_schema = f"ylx.safe-swap-receipt-resource.{api_version}"
        if schema != expected_schema or not isinstance(receipt, Mapping):
            self._problem(HTTPStatus.NOT_FOUND, "not_found", "当前没有可读取的安全换盘回执")
            return
        if schema == "ylx.safe-swap-receipt-resource.v3":
            try:
                validate_safe_swap_v3_receipt(receipt)
            except InvalidSourceEvent:
                self._problem(
                    HTTPStatus.NOT_FOUND,
                    "not_found",
                    "当前没有可读取的安全换盘回执",
                )
                return
        elif receipt.get("schema") != "ylx.safe-swap-receipt.v2":
            self._problem(HTTPStatus.NOT_FOUND, "not_found", "当前没有可读取的安全换盘回执")
            return
        self._send_json(HTTPStatus.OK, resource)

    def _get_camera_focus(self) -> None:
        if self._principal("getCameraFocus") is None:
            return
        try:
            status = self.server.provider.camera_focus_status()
        except ProviderError as error:
            self._problem(
                error.status,
                error.code,
                error.message,
                retryable=error.retryable,
                details=error.details,
            )
            return
        except Exception:
            self._provider_failure()
            return
        if status is None:
            self._problem(
                HTTPStatus.NOT_FOUND,
                "camera_focus_unsupported",
                "当前相机没有可读取的焦距控制",
            )
            return
        self._send_camera_focus_status(HTTPStatus.OK, status)

    def _get_preview(self, query: Mapping[str, list[str]]) -> None:
        if self._principal("getPreview") is None:
            return
        if set(query) - {"fps"} or any(len(values) != 1 for values in query.values()):
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "预览查询参数无效")
            return
        fps_value = query.get("fps", [None])[0]
        fps = None
        if fps_value is not None:
            try:
                fps = int(fps_value) if fps_value.isascii() and fps_value.isdecimal() else 0
            except ValueError:
                fps = 0
            if fps < 1:
                self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "fps 必须是正整数")
                return
        requested = self.headers.get("Accept", "image/jpeg").lower()
        accept = (
            "multipart/x-mixed-replace"
            if "multipart/x-mixed-replace" in requested
            else "image/jpeg"
        )
        streaming = accept == "multipart/x-mixed-replace"
        if streaming and not self.server.preview_stream_slots.acquire(blocking=False):
            self._problem(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "preview_capacity_exhausted",
                "预览流连接数已达设备上限",
                retryable=True,
            )
            return
        try:
            try:
                response = self.server.provider.latest_preview(fps=fps, accept=accept)
            except PreviewFrameUnavailable:
                self._problem(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    "preview_unavailable",
                    "当前没有可用的预览帧",
                    retryable=True,
                )
                return
            except ProviderError as error:
                self._problem(error.status, error.code, error.message, retryable=error.retryable)
                return
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", response.content_type)
            if response.content_length is not None:
                self.send_header("Content-Length", str(response.content_length))
            else:
                self.send_header("Connection", "close")
            self.send_header("Cache-Control", "no-store")
            self._cors_headers()
            self.end_headers()
            close = getattr(response.body, "close", None)
            try:
                if isinstance(response.body, bytes):
                    self.wfile.write(response.body)
                else:
                    for chunk in response.body:
                        self.wfile.write(chunk)
                        self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                return
            finally:
                if close is not None:
                    close()
        finally:
            if streaming:
                self.server.preview_stream_slots.release()

    @staticmethod
    def _artifact_route(parts: list[str]) -> tuple[str, str, str] | None:
        if (
            len(parts) == 7
            and parts[1] == "api"
            and parts[2] in {"v2", "v3"}
            and parts[3] == "sessions"
            and parts[5] == "artifacts"
            and _valid_session_id(parts[2], parts[4])
            and re.fullmatch(r"[0-9a-f]{64}", parts[6]) is not None
        ):
            return parts[2], parts[4], parts[6]
        return None

    def _session_artifact(
        self, api_version: str, session_id: str, artifact_id: str, *, head: bool
    ) -> None:
        operation_id = "headSessionArtifact" if head else "getSessionArtifact"
        if self._principal(operation_id, session_id) is None:
            return
        state = self.server.provider.artifact_io_state()
        if state is not None:
            headers = {
                "YLX-Error-Code": "capture_busy",
                "YLX-Wait-State": "idle",
                "Retry-After": "1",
            }
            if head:
                self._send_empty(HTTPStatus.LOCKED, headers=headers)
            else:
                self._problem(
                    HTTPStatus.LOCKED,
                    "capture_busy",
                    "采集资源繁忙，请等待 idle",
                    retryable=True,
                    headers=headers,
                    details={
                        "wait_for": "idle",
                        "retry_after_seconds": 1,
                        "current_state": state,
                    },
                )
            return
        try:
            locked = self.server.provider.open_verified_artifact(
                session_id, artifact_id, api_version
            )
            with locked as representation:
                selected_range = None
                if not head:
                    range_value, range_is_single = self._single_header("Range")
                    if_range, if_range_is_single = self._single_header("If-Range")
                    if not range_is_single:
                        self._problem(
                            HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE,
                            "range_not_satisfiable",
                            "Range 只能出现一次",
                            headers={"Content-Range": f"bytes */{representation.size}"},
                        )
                        return
                    if range_value is not None and (
                        if_range_is_single and (if_range is None or if_range == representation.etag)
                    ):
                        try:
                            selected_range = parse_single_range(range_value, representation.size)
                        except UnsatisfiableRange:
                            self._problem(
                                HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE,
                                "range_not_satisfiable",
                                "请求的字节范围无法满足",
                                headers={"Content-Range": f"bytes */{representation.size}"},
                            )
                            return
                if selected_range is None:
                    self._send_representation(representation, head=head)
                else:
                    first, last = selected_range
                    self._send_representation(
                        representation,
                        status=HTTPStatus.PARTIAL_CONTENT,
                        offset=first,
                        length=last - first + 1,
                        content_range=f"bytes {first}-{last}/{representation.size}",
                    )
        except ArtifactAccessError as error:
            if error.code == "not_verified":
                if head:
                    self._send_empty(
                        HTTPStatus.CONFLICT,
                        headers={"YLX-Error-Code": "session_not_verified"},
                    )
                else:
                    self._problem(
                        HTTPStatus.CONFLICT,
                        "session_not_verified",
                        "会话没有当前可用的验证结果",
                        headers={"YLX-Error-Code": "session_not_verified"},
                        details={"reason": error.reason},
                    )
            elif head:
                self._send_empty(HTTPStatus.NOT_FOUND)
            else:
                self._problem(HTTPStatus.NOT_FOUND, "not_found", "artifact 不存在")
        except ProviderError as error:
            headers = (
                {"YLX-Error-Code": error.code} if error.code == "session_not_verified" else None
            )
            if head:
                self._send_empty(error.status, headers=headers)
            else:
                self._problem(
                    error.status,
                    error.code,
                    error.message,
                    retryable=error.retryable,
                    headers=headers,
                    details=error.details,
                )

    def _read_json(self) -> Mapping[str, object] | None:
        content_type_value, content_type_is_single = self._single_header("Content-Type")
        content_length_value, content_length_is_single = self._single_header("Content-Length")
        if not content_type_is_single or not content_length_is_single:
            self._problem(
                HTTPStatus.BAD_REQUEST,
                "invalid_request",
                "Content-Type 和 Content-Length 只能分别出现一次",
            )
            return None
        content_type = (content_type_value or "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            self._problem(
                HTTPStatus.BAD_REQUEST, "invalid_request", "Content-Type 必须是 application/json"
            )
            return None
        try:
            length = int(content_length_value or "0")
        except ValueError:
            length = -1
        if length <= 0 or length > MAX_BODY_BYTES:
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "请求体大小无效")
            return None
        try:
            body = json.loads(self.rfile.read(length))
        except (UnicodeError, json.JSONDecodeError):
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "请求体不是有效 JSON")
            return None
        if not isinstance(body, dict):
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "请求体必须是 JSON 对象")
            return None
        return body

    def _validate_capture_body(self, operation: str, body: Mapping[str, object]) -> bool:
        if operation == "start":
            allowed = {"schema", "mode", "volume_id", "display_name", "take"}
            if set(body) - allowed or body.get("schema") != "ylx.capture-start.v2":
                return False
            if body.get("mode") not in {"production", "calibration"}:
                return False
            volume_id = body.get("volume_id")
            if volume_id is not None and (
                not isinstance(volume_id, str) or UUID_V4.fullmatch(volume_id) is None
            ):
                return False
            display_name = body.get("display_name")
            if display_name is not None and (
                not isinstance(display_name, str) or not 1 <= len(display_name) <= 160
            ):
                return False
            take = body.get("take")
            if not isinstance(take, dict):
                return False
            if take.get("kind") == "new":
                return set(take) == {"kind"}
            continuation = take.get("continuation_of")
            return (
                take.get("kind") == "continue"
                and set(take) == {"kind", "continuation_of"}
                and isinstance(continuation, str)
                and UUID_V7.fullmatch(continuation) is not None
            )
        return (
            set(body) == {"schema", "reason"}
            and body.get("schema") == "ylx.capture-stop.v2"
            and body.get("reason") in {"user", "safe_swap"}
        )

    @staticmethod
    def _validate_camera_focus_body(body: Mapping[str, object]) -> bool:
        if set(body) - {"schema", "value", "auto_enabled"}:
            return False
        if body.get("schema") != "ylx.camera-focus-set.v1":
            return False
        has_value = "value" in body
        has_auto = "auto_enabled" in body
        if not has_value and not has_auto:
            return False
        if has_value and type(body["value"]) is not int:
            return False
        return not (has_auto and type(body["auto_enabled"]) is not bool)

    def _command_principal(
        self,
        operation_id: str,
    ) -> tuple[Principal, str] | None:
        principal = self._principal(operation_id)
        if principal is None:
            return None
        origin, _ = self._single_header("Origin")
        csrf_token = self.server.security.csrf_token
        csrf_value, csrf_is_single = self._single_header("X-CSRF-Token")
        if not csrf_is_single or (
            origin
            and not self._is_same_gateway_origin(origin)
            and (csrf_token is None or not hmac.compare_digest(csrf_value or "", csrf_token))
        ):
            self._audit(operation_id, None, "csrf_forbidden", principal)
            self._problem(HTTPStatus.FORBIDDEN, "csrf_forbidden", "CSRF token 缺失或无效")
            return None
        key, key_is_single = self._single_header("Idempotency-Key")
        if (
            not key_is_single
            or key is None
            or not 1 <= len(key) <= 128
            or any(character == " " or not 0x21 <= ord(character) <= 0x7E for character in key)
        ):
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "Idempotency-Key 无效")
            return None
        return principal, key

    def _capture_command(self, operation: str) -> None:
        operation_id = "startCapture" if operation == "start" else "stopCapture"
        command_identity = self._command_principal(operation_id)
        if command_identity is None:
            return
        principal, key = command_identity
        body = self._read_json()
        if body is None:
            return
        if not self._validate_capture_body(operation, body):
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "请求体不符合捕获命令契约")
            return
        canonical = json.dumps(
            body, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
        command = CaptureCommand(principal.principal_id, key, body, canonical)
        try:
            result = (
                self.server.provider.start_capture(command)
                if operation == "start"
                else self.server.provider.stop_capture(command)
            )
        except ProviderError as error:
            self._problem(
                error.status,
                error.code,
                error.message,
                retryable=error.retryable,
                details=error.details,
            )
            return
        headers = {"Idempotency-Replayed": "true"} if result.replayed else None
        if operation == "stop" and result.status == HTTPStatus.NO_CONTENT and result.body is None:
            self._send_empty(result.status, headers=headers)
        elif result.status == HTTPStatus.ACCEPTED:
            self._send_capture_status(result.status, result.body, headers=headers)
        else:
            self._problem(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "invalid_source_state",
                "daemon capture command 响应无效",
            )

    def _camera_focus_command(self) -> None:
        command_identity = self._command_principal("setCameraFocus")
        if command_identity is None:
            return
        principal, key = command_identity
        body = self._read_json()
        if body is None:
            return
        if not self._validate_camera_focus_body(body):
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "请求体不符合相机焦距契约")
            return
        canonical = json.dumps(
            body, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
        command = CaptureCommand(principal.principal_id, key, body, canonical)
        try:
            result = self.server.provider.set_camera_focus(command)
        except ProviderError as error:
            self._problem(
                error.status,
                error.code,
                error.message,
                retryable=error.retryable,
                details=error.details,
            )
            return
        headers = {"Idempotency-Replayed": "true"} if result.replayed else None
        if result.status == HTTPStatus.OK:
            self._send_camera_focus_status(result.status, result.body, headers=headers)
            return
        self._problem(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            "invalid_source_state",
            "daemon camera focus command 响应无效",
        )


def create_gateway_server(
    host: str,
    port: int,
    provider: DeviceProvider,
    *,
    security: SecurityPolicy,
    audit_sink: Callable[[AuditEvent], None] | None = None,
    event_buffer: EventReplayBuffer | None = None,
    sse_heartbeat_seconds: float = 15.0,
    max_sse_connections: int = 4,
    max_preview_streams: int = 2,
) -> GatewayServer:
    return GatewayServer(
        (host, port),
        provider,
        security,
        audit_sink,
        event_buffer or EventReplayBuffer(),
        sse_heartbeat_seconds,
        max_sse_connections,
        max_preview_streams,
    )
