"""Device API v2/v3/v4 的共享 HTTP gateway。"""

from __future__ import annotations

import hmac
import ipaddress
import json
import re
import time
import uuid
from collections import deque
from collections.abc import Callable, Iterator, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import BoundedSemaphore, RLock
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
    project_capture_status,
    project_device_descriptor,
    validate_capture_status,
    validate_device_descriptor,
    validate_retained_unsuccessful_outcome,
    validate_safe_swap_v3_receipt,
    validate_session_list,
)
from rp_ylx.api.preview import PreviewFrameUnavailable
from rp_ylx.api.security import AuditEvent, Principal, SecurityPolicy
from rp_ylx.web import WEB_ASSETS, EchoWebArtifactError, asset_content_type, read_asset

MAX_BODY_BYTES = 64 * 1024
UUID_V4 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
UUID_V7 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
WEB_PATHS = {"/": "index.html", **{f"/{name}": name for name in WEB_ASSETS if name != "index.html"}}
WEB_CONTENT_SECURITY_POLICY = (
    "default-src 'self'; base-uri 'none'; connect-src 'self'; form-action 'self'; "
    "frame-ancestors 'none'; img-src 'self' blob: data:; object-src 'none'; "
    "script-src 'self'; style-src 'self'"
)
SUPPORTED_API_VERSIONS = frozenset({"v2", "v3", "v4"})
CAMERA_FOCUS_API_VERSIONS = frozenset({"v4"})
NETWORK_API_VERSIONS = frozenset({"v4"})
NETWORK_MODES = ["hotspot", "wifi-client", "ethernet-dhcp", "ethernet-static"]
NETWORK_MUTATION_OPERATIONS = ["apply", "retry", "forget"]
NETWORK_WIFI_SECURITY = frozenset({"open", "wpa2-personal", "wpa3-personal", "wpa2-wpa3-personal"})
NETWORK_TOKEN = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
MDNS_TOKEN = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
NETWORK_CREDENTIAL_REF = re.compile(r"^cred-[A-Za-z0-9_.:-]+$")
HTTP_CONTENT_LENGTH = re.compile(r"^(?:0|[1-9][0-9]*)$")
NETWORK_INTERFACE_STATES = frozenset(
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
    }
)
NETWORK_TRANSACTION_STATUSES = frozenset({"accepted", "running", "committed", "rescued", "failed"})
NETWORK_TRANSACTION_STAGES = frozenset(
    {
        "accepted",
        "prepared",
        "ap_ready",
        "activating",
        "verifying",
        "committed",
        "falling_back",
        "rescued",
        "failed",
        "forgetting",
        "forgotten",
    }
)
NETWORK_RECOVERY_ACTIONS = frozenset(
    {
        "await_device",
        "reconnect_target_lan",
        "reconnect_rescue_ap",
        "retry",
        "service_required",
        "none",
    }
)
NETWORK_MUTATION_DISABLED_REASONS = frozenset(
    {
        "not_enabled",
        "auth_profile_unavailable",
        "controller_unavailable",
        "network_manager_unavailable",
        "rescue_ap_not_validated",
        "capture_active",
        "recovery_required",
        "maintenance_window_closed",
        "unsupported_concurrency",
    }
)
INVALID_NETWORK_DESIRED_STATE_REASONS = frozenset(
    {
        "missing_credential_ref",
        "credential_ref_expired",
        "credential_ref_already_used",
        "credential_ref_not_allowed",
        "security_mismatch",
        "ssid_too_long",
        "unsupported_mode",
        "invalid_static_ipv4",
        "unsafe_secret_field",
        "active_state",
    }
)
NETWORK_STAGE_STATUSES = {
    "accepted": "accepted",
    "prepared": "running",
    "ap_ready": "running",
    "activating": "running",
    "verifying": "running",
    "committed": "committed",
    "falling_back": "running",
    "rescued": "rescued",
    "failed": "failed",
    "forgetting": "running",
    "forgotten": "committed",
}
NETWORK_TRANSACTION_ERROR_CODES = frozenset(
    {
        "rescue_ap_unavailable",
        "credential_rejected",
        "dhcp_timeout",
        "route_lost",
        "network_manager_unavailable",
        "concurrency_unsupported",
    }
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


@dataclass(frozen=True, slots=True)
class NetworkCommand:
    principal_id: str
    idempotency_key: str
    body: Mapping[str, object]
    canonical_body: bytes


@dataclass(frozen=True, slots=True)
class NetworkCommandResult:
    status: int
    body: object | None
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class NetworkCredentialCommand:
    principal_id: str
    passphrase: str


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

    def network_status(self) -> Mapping[str, object]: ...

    def scan_networks(self) -> Mapping[str, object]: ...

    def create_network_credential(
        self, command: NetworkCredentialCommand
    ) -> Mapping[str, object]: ...

    def apply_network_desired_state(self, command: NetworkCommand) -> NetworkCommandResult: ...

    def retry_network_transaction(self, command: NetworkCommand) -> NetworkCommandResult: ...

    def forget_network_client_profile(self, command: NetworkCommand) -> NetworkCommandResult: ...

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


@dataclass(frozen=True, slots=True)
class _NetworkSseEvent:
    delivery_id: int
    source_event: Mapping[str, object]

    def encode(self) -> bytes:
        delivery_id = str(self.delivery_id)
        payload = {
            "schema": "ylx.network-event.v1",
            "sse_delivery_id": delivery_id,
            **self.source_event,
        }
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        return (f"id: {delivery_id}\nevent: {self.source_event['type']}\ndata: {data}\n\n").encode()


class _NetworkEventReplayBuffer:
    """Assign server-wide delivery IDs and retain bounded network SSE replay."""

    def __init__(self, capacity: int = 128, *, initial_delivery_id: int | None = None) -> None:
        if capacity < 1:
            raise ValueError("capacity 必须大于零")
        if initial_delivery_id is not None and (
            type(initial_delivery_id) is not int or initial_delivery_id < 1
        ):
            raise ValueError("initial_delivery_id 必须是正整数")
        self._events: deque[_NetworkSseEvent] = deque(maxlen=capacity)
        self._next_delivery_id = initial_delivery_id or uuid.uuid4().int
        self._last_status: dict[str, object] | None = None
        self._retired_authority_epochs: set[str] = set()
        self._lock = RLock()

    def replay(
        self, cursor: int | None, status: Mapping[str, object]
    ) -> tuple[_NetworkSseEvent, ...]:
        with self._lock:
            if cursor is None:
                return (self._publish_snapshot_locked(status),)
            buffered = tuple(self._events)
            if not self._contains_cursor(buffered, cursor):
                return (self._publish_snapshot_locked(status, after_cursor=cursor),)
            self._observe_locked(status)
            return self._events_after_locked(cursor)

    def observe(self, status: Mapping[str, object]) -> None:
        with self._lock:
            self._observe_locked(status)

    def events_after(
        self, cursor: int, status: Mapping[str, object]
    ) -> tuple[_NetworkSseEvent, ...]:
        with self._lock:
            buffered = tuple(self._events)
            if not self._contains_cursor(buffered, cursor):
                return (self._publish_snapshot_locked(status, after_cursor=cursor),)
            self._observe_locked(status)
            return self._events_after_locked(cursor)

    @staticmethod
    def _contains_cursor(events: tuple[_NetworkSseEvent, ...], cursor: int) -> bool:
        return any(event.delivery_id == cursor for event in events)

    def _events_after_locked(self, cursor: int) -> tuple[_NetworkSseEvent, ...]:
        return tuple(event for event in self._events if event.delivery_id > cursor)

    def _observe_locked(self, status: Mapping[str, object]) -> None:
        relation = self._status_relation_locked(status)
        if relation in {"same", "stale"}:
            return
        self._publish_locked(
            status,
            force_snapshot=relation in {"new_authority", "projection"},
        )

    def _status_relation_locked(self, status: Mapping[str, object]) -> str:
        if self._last_status is None:
            return "new"
        authority_epoch = str(status["authority_epoch"])
        source_revision = int(status["source_revision"])
        last_authority_epoch = str(self._last_status["authority_epoch"])
        last_source_revision = int(self._last_status["source_revision"])
        if authority_epoch == last_authority_epoch:
            if source_revision < last_source_revision:
                return "stale"
            if source_revision > last_source_revision:
                return "new"
            projection_fields = (
                "saved",
                "verified",
                "desired",
                "observed",
                "transaction",
                "mutation_capability",
                "concurrency_capability",
            )
            if any(
                status.get(field) != self._last_status.get(field) for field in projection_fields
            ):
                return "projection"
            return "same"
        if authority_epoch in self._retired_authority_epochs:
            return "stale"
        return "new_authority"

    def _publish_snapshot_locked(
        self,
        status: Mapping[str, object],
        *,
        after_cursor: int | None = None,
    ) -> _NetworkSseEvent:
        relation = self._status_relation_locked(status)
        selected_status: Mapping[str, object]
        if relation in {"same", "stale"} and self._last_status is not None:
            selected_status = self._last_status
        else:
            selected_status = status
        if after_cursor is not None and self._next_delivery_id <= after_cursor:
            self._next_delivery_id = after_cursor + 1
        return self._publish_locked(selected_status, force_snapshot=True)

    def _publish_locked(
        self, status: Mapping[str, object], *, force_snapshot: bool
    ) -> _NetworkSseEvent:
        copied_status = deepcopy(dict(status))
        transaction: object = None
        if not force_snapshot:
            transactions = copied_status.get("transaction")
            if isinstance(transactions, Mapping):
                transaction = transactions.get("current") or transactions.get("latest")
            if not (
                isinstance(transaction, Mapping)
                and transaction.get("authority_epoch") == copied_status["authority_epoch"]
                and transaction.get("source_revision") == copied_status["source_revision"]
            ):
                transaction = None
        if isinstance(transaction, Mapping):
            source_event = {
                "authority_epoch": copied_status["authority_epoch"],
                "source_revision": copied_status["source_revision"],
                "occurred_at": transaction["updated_at"],
                "type": "transaction",
                "transaction_id": transaction["transaction_id"],
                "data": deepcopy(dict(transaction)),
            }
        else:
            source_event = {
                "authority_epoch": copied_status["authority_epoch"],
                "source_revision": copied_status["source_revision"],
                "occurred_at": copied_status["observed_at"],
                "type": "snapshot",
                "transaction_id": None,
                "data": copied_status,
            }
        event = _NetworkSseEvent(self._next_delivery_id, source_event)
        self._next_delivery_id += 1
        self._events.append(event)
        if self._last_status is not None:
            previous_authority = str(self._last_status["authority_epoch"])
            current_authority = str(copied_status["authority_epoch"])
            if previous_authority != current_authority:
                self._retired_authority_epochs.add(previous_authority)
        self._last_status = copied_status
        return event


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
        network_sse_poll_seconds: float = 0.25,
        max_sse_connections: int = 4,
        max_preview_streams: int = 2,
        external_scheme: str = "http",
    ) -> None:
        if sse_heartbeat_seconds <= 0 or network_sse_poll_seconds <= 0:
            raise ValueError("SSE 心跳和轮询间隔必须大于零")
        if max_sse_connections < 1 or max_preview_streams < 1:
            raise ValueError("长连接并发上限必须大于零")
        if external_scheme not in {"http", "https"}:
            raise ValueError("external_scheme 必须是 http 或 https")
        self.provider = provider
        self.security = security
        self.audit_sink = audit_sink or (lambda event: None)
        self.event_buffer = event_buffer
        self.network_event_buffer = _NetworkEventReplayBuffer()
        self.sse_heartbeat_seconds = sse_heartbeat_seconds
        self.network_sse_poll_seconds = network_sse_poll_seconds
        self.sse_slots = BoundedSemaphore(max_sse_connections)
        self.preview_stream_slots = BoundedSemaphore(max_preview_streams)
        self.external_scheme = external_scheme
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
            parsed.scheme == self.server.external_scheme
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
        if self.close_connection:
            self.send_header("Connection", "close")
        self._cors_headers()
        self.end_headers()
        if self.command != "HEAD":
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
        if self.close_connection:
            self.send_header("Connection", "close")
        self._cors_headers()
        self.end_headers()

    def _send_web_asset(self, name: str, *, head: bool = False) -> None:
        try:
            payload = read_asset(name)
            content_type = asset_content_type(name)
        except (EchoWebArtifactError, OSError, ValueError):
            self._problem(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "embedded_web_unavailable",
                "内嵌设备工作台资源不可用",
            )
            return
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Security-Policy", WEB_CONTENT_SECURITY_POLICY)
        self.send_header("Cross-Origin-Resource-Policy", "same-origin")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.end_headers()
        if not head:
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

    def _close_if_request_body_is_unread(self) -> None:
        content_length = getattr(self, "_request_content_length", None)
        if self.command in {"POST", "PUT", "PATCH"} or content_length not in {None, 0}:
            self.close_connection = True

    def _principal(self, operation_id: str, resource_id: str | None = None) -> Principal | None:
        origin, origin_is_single = self._single_header("Origin")
        if not origin_is_single or (
            origin
            and not self._is_same_gateway_origin(origin)
            and origin not in self.server.security.allowed_origins
        ):
            self._audit(operation_id, resource_id, "origin_forbidden")
            self._close_if_request_body_is_unread()
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
            self._close_if_request_body_is_unread()
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
            self._close_if_request_body_is_unread()
            if self.command == "HEAD":
                self._send_empty(HTTPStatus.FORBIDDEN)
            else:
                self._problem(HTTPStatus.FORBIDDEN, "forbidden", "调用方无权访问此资源")
            return None
        self._audit(operation_id, resource_id, "allowed", principal)
        return principal

    def _begin_request(self, *, body_allowed: bool) -> bool:
        self._request_id = str(uuid.uuid4())
        self._request_content_length: int | None = None
        transfer_encodings = self.headers.get_all("Transfer-Encoding", failobj=[])
        content_lengths = self.headers.get_all("Content-Length", failobj=[])
        if transfer_encodings:
            self.close_connection = True
            self._problem(
                HTTPStatus.BAD_REQUEST,
                "invalid_request",
                "Transfer-Encoding 请求正文不受支持",
            )
            return False
        if len(content_lengths) > 1:
            self.close_connection = True
            self._problem(
                HTTPStatus.BAD_REQUEST,
                "invalid_request",
                "Content-Length 只能出现一次",
            )
            return False
        if content_lengths:
            content_length_value = content_lengths[0].strip()
            if HTTP_CONTENT_LENGTH.fullmatch(content_length_value) is None or len(
                content_length_value
            ) > len(str(MAX_BODY_BYTES)):
                self.close_connection = True
                self._problem(
                    HTTPStatus.BAD_REQUEST,
                    "invalid_request",
                    "Content-Length 格式无效",
                )
                return False
            content_length = int(content_length_value)
            if content_length > MAX_BODY_BYTES:
                self.close_connection = True
                self._problem(
                    HTTPStatus.BAD_REQUEST,
                    "invalid_request",
                    "请求体大小无效",
                )
                return False
            self._request_content_length = content_length
            if not body_allowed and content_length != 0:
                self.close_connection = True
                self._problem(
                    HTTPStatus.BAD_REQUEST,
                    "invalid_request",
                    "该请求方法不接受请求体",
                )
                return False
        return True

    def _route_methods(self, path: str) -> tuple[str, ...] | None:
        parts = path.split("/")
        if len(parts) < 4 or parts[1] != "api" or parts[2] not in SUPPORTED_API_VERSIONS:
            return None
        if len(parts) == 4 and parts[3] in {"device", "preview", "sessions"}:
            return ("GET", "OPTIONS")
        if len(parts) == 4 and parts[2] in NETWORK_API_VERSIONS and parts[3] == "network":
            return ("GET", "OPTIONS")
        if (
            len(parts) == 5
            and parts[2] in NETWORK_API_VERSIONS
            and parts[3:] == ["network", "scan"]
        ):
            return ("GET", "OPTIONS")
        if (
            len(parts) == 5
            and parts[2] in NETWORK_API_VERSIONS
            and parts[3:] == ["network", "credentials"]
        ):
            return ("POST", "OPTIONS")
        if (
            len(parts) == 5
            and parts[2] in NETWORK_API_VERSIONS
            and parts[3:] == ["network", "events"]
        ):
            return ("GET", "OPTIONS")
        if (
            len(parts) == 5
            and parts[2] in NETWORK_API_VERSIONS
            and parts[3] == "network"
            and parts[4] in NETWORK_MUTATION_OPERATIONS
        ):
            return ("POST", "OPTIONS")
        if len(parts) == 5 and parts[3] == "capture":
            if parts[4] in {"start", "stop"}:
                return ("POST", "OPTIONS")
            if parts[4] in {"status", "events", "safe-swap"}:
                return ("GET", "OPTIONS")
        if (
            len(parts) == 5
            and parts[2] in CAMERA_FOCUS_API_VERSIONS
            and parts[3:] == ["camera", "focus"]
        ):
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
            headers.extend(("Content-Type", "X-CSRF-Token"))
            if parts[3:] != ["network", "credentials"]:
                headers.append("Idempotency-Key")
        elif len(parts) == 5 and parts[3:] in (["capture", "events"], ["network", "events"]):
            headers.append("Last-Event-ID")
        elif self._artifact_route(parts) is not None:
            headers.extend(("If-Range", "Range"))
        return tuple(headers)

    def do_OPTIONS(self) -> None:
        if not self._begin_request(body_allowed=False):
            return
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
        if not self._begin_request(body_allowed=False):
            return
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
            and parts[2] in SUPPORTED_API_VERSIONS
            and parts[3] == "device"
        ):
            self._get_device(parts[2])
            return
        if (
            len(parts) == 5
            and parts[1] == "api"
            and parts[2] in SUPPORTED_API_VERSIONS
            and parts[3:] == ["capture", "status"]
        ):
            self._get_capture_status(parts[2])
            return
        if (
            len(parts) == 5
            and parts[1] == "api"
            and parts[2] in SUPPORTED_API_VERSIONS
            and parts[3:] == ["capture", "events"]
        ):
            self._capture_events(parts[2], parse_qs(parsed.query, keep_blank_values=True))
            return
        if (
            len(parts) == 5
            and parts[1] == "api"
            and parts[2] in SUPPORTED_API_VERSIONS
            and parts[3:] == ["capture", "safe-swap"]
        ):
            self._get_safe_swap(parts[2])
            return
        if (
            len(parts) == 5
            and parts[1] == "api"
            and parts[2] in CAMERA_FOCUS_API_VERSIONS
            and parts[3:] == ["camera", "focus"]
        ):
            self._get_camera_focus()
            return
        if (
            len(parts) == 4
            and parts[1] == "api"
            and parts[2] in NETWORK_API_VERSIONS
            and parts[3] == "network"
        ):
            self._get_network_status()
            return
        if (
            len(parts) == 5
            and parts[1] == "api"
            and parts[2] in NETWORK_API_VERSIONS
            and parts[3:] == ["network", "scan"]
        ):
            self._get_network_scan()
            return
        if (
            len(parts) == 5
            and parts[1] == "api"
            and parts[2] in NETWORK_API_VERSIONS
            and parts[3:] == ["network", "events"]
        ):
            self._network_events()
            return
        if (
            len(parts) == 4
            and parts[1] == "api"
            and parts[2] in SUPPORTED_API_VERSIONS
            and parts[3] == "preview"
        ):
            self._get_preview(parse_qs(parsed.query, keep_blank_values=True))
            return
        if (
            len(parts) == 4
            and parts[1] == "api"
            and parts[2] in SUPPORTED_API_VERSIONS
            and parts[3] == "sessions"
        ):
            self._list_sessions(parse_qs(parsed.query, keep_blank_values=True))
            return
        if (
            len(parts) == 5
            and parts[1] == "api"
            and parts[2] in SUPPORTED_API_VERSIONS
            and parts[3] == "sessions"
            and _valid_session_id(parts[2], parts[4])
        ):
            self._get_manifest(parts[2], parts[4])
            return
        if (
            len(parts) == 6
            and parts[1] == "api"
            and parts[2] in SUPPORTED_API_VERSIONS
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
        if not self._begin_request(body_allowed=False):
            return
        path = urlsplit(self.path).path
        web_asset = WEB_PATHS.get(path)
        if web_asset is not None:
            self._send_web_asset(web_asset, head=True)
            return
        artifact = self._artifact_route(path.split("/"))
        if artifact is not None:
            api_version, session_id, artifact_id = artifact
            self._session_artifact(api_version, session_id, artifact_id, head=True)
            return
        self._problem(HTTPStatus.NOT_FOUND, "not_found", "接口不存在")

    def do_POST(self) -> None:
        if not self._begin_request(body_allowed=True):
            return
        path = urlsplit(self.path).path
        parts = path.split("/")
        if (
            len(parts) == 5
            and parts[1] == "api"
            and parts[2] in SUPPORTED_API_VERSIONS
            and parts[3] == "capture"
            and parts[4] in {"start", "stop"}
        ):
            self._capture_command(parts[2], parts[4])
            return
        if (
            len(parts) == 5
            and parts[1] == "api"
            and parts[2] in CAMERA_FOCUS_API_VERSIONS
            and parts[3:] == ["camera", "focus"]
        ):
            self._camera_focus_command()
            return
        if (
            len(parts) == 5
            and parts[1] == "api"
            and parts[2] in NETWORK_API_VERSIONS
            and parts[3:] == ["network", "credentials"]
        ):
            self._create_network_credential()
            return
        if (
            len(parts) == 5
            and parts[1] == "api"
            and parts[2] in NETWORK_API_VERSIONS
            and parts[3] == "network"
            and parts[4] in NETWORK_MUTATION_OPERATIONS
        ):
            self._network_command(parts[4])
            return
        self._close_if_request_body_is_unread()
        self._problem(HTTPStatus.NOT_FOUND, "not_found", "接口不存在")

    def _unsupported_body_method(self) -> None:
        if not self._begin_request(body_allowed=True):
            return
        self._close_if_request_body_is_unread()
        self._problem(
            HTTPStatus.METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "接口不允许该请求方法",
        )

    def do_PUT(self) -> None:
        self._unsupported_body_method()

    def do_PATCH(self) -> None:
        self._unsupported_body_method()

    def do_DELETE(self) -> None:
        if not self._begin_request(body_allowed=False):
            return
        self._problem(
            HTTPStatus.METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "接口不允许该请求方法",
        )

    def _get_device(self, api_version: str) -> None:
        if self._principal("getDevice") is None:
            return
        try:
            source_descriptor = self.server.provider.device_descriptor(
                api_version, self.server.security.profile
            )
            descriptor = project_device_descriptor(source_descriptor, api_version=api_version)
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

    def _get_capture_status(self, api_version: str) -> None:
        if self._principal("getCaptureStatus") is None:
            return
        try:
            source_body = self.server.provider.capture_status()
            body = project_capture_status(source_body, api_version=api_version)
        except Exception:
            self._provider_failure()
            return
        self._send_capture_status(HTTPStatus.OK, body, api_version=api_version)

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
        api_version: str,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        try:
            validate_capture_status(body, api_version=api_version)
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

    @staticmethod
    def _valid_network_status(value: object) -> bool:
        if not isinstance(value, Mapping) or set(value) != {
            "schema",
            "authority_epoch",
            "source_revision",
            "observed_at",
            "saved",
            "verified",
            "desired",
            "observed",
            "transaction",
            "mutation_capability",
            "concurrency_capability",
        }:
            return False
        transaction = value.get("transaction")
        transactions = (
            []
            if not isinstance(transaction, Mapping)
            else [transaction.get("current"), transaction.get("latest")]
        )
        return (
            value.get("schema") == "ylx.network-status.v1"
            and GatewayHandler._valid_uuid4(value.get("authority_epoch"))
            and type(value.get("source_revision")) is int
            and value["source_revision"] >= 0
            and GatewayHandler._valid_datetime(value.get("observed_at"))
            and type(value.get("saved")) is bool
            and type(value.get("verified")) is bool
            and (not value["verified"] or value["saved"])
            and GatewayHandler._valid_network_desired_state(value.get("desired"))
            and GatewayHandler._valid_network_observed_state(value.get("observed"))
            and GatewayHandler._valid_network_transaction_window(transaction)
            and all(
                item is None
                or item["authority_epoch"] == value["authority_epoch"]
                and item["source_revision"] <= value["source_revision"]
                for item in transactions
            )
            and (
                transaction.get("current") is None
                or transaction["current"]["status"] in {"accepted", "running"}
            )
            and GatewayHandler._valid_network_mutation_capability(value.get("mutation_capability"))
            and GatewayHandler._valid_network_concurrency_capability(
                value.get("concurrency_capability")
            )
        )

    @staticmethod
    def _valid_uuid4(value: object) -> bool:
        if not isinstance(value, str):
            return False
        try:
            parsed = uuid.UUID(value)
        except ValueError:
            return False
        return parsed.version == 4 and str(parsed) == value.lower()

    @staticmethod
    def _valid_datetime(value: object) -> bool:
        if not isinstance(value, str) or not value:
            return False
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return False
        return parsed.tzinfo is not None

    @staticmethod
    def _valid_ipv4(value: object) -> bool:
        if not isinstance(value, str):
            return False
        try:
            ipaddress.IPv4Address(value)
        except ValueError:
            return False
        return True

    @staticmethod
    def _valid_network_static_ipv4(value: object) -> bool:
        if not isinstance(value, Mapping) or set(value) != {
            "address",
            "prefix_length",
            "gateway",
            "dns",
        }:
            return False
        dns = value.get("dns")
        return (
            GatewayHandler._valid_ipv4(value.get("address"))
            and type(value.get("prefix_length")) is int
            and 1 <= value["prefix_length"] <= 32
            and (value.get("gateway") is None or GatewayHandler._valid_ipv4(value.get("gateway")))
            and isinstance(dns, list)
            and len(dns) <= 3
            and all(GatewayHandler._valid_ipv4(item) for item in dns)
            and len(set(dns)) == len(dns)
        )

    @staticmethod
    def _valid_network_ethernet(value: object) -> bool:
        if not isinstance(value, Mapping) or set(value) != {"addressing", "static_ipv4"}:
            return False
        addressing = value.get("addressing")
        static = value.get("static_ipv4")
        return (
            addressing == "dhcp"
            and static is None
            or addressing == "static"
            and GatewayHandler._valid_network_static_ipv4(static)
        )

    @staticmethod
    def _valid_network_desired_wifi_client(value: object) -> bool:
        if not isinstance(value, Mapping) or set(value) != {
            "ssid",
            "security",
            "credential_state",
        }:
            return False
        ssid = value.get("ssid")
        security = value.get("security")
        credential_state = value.get("credential_state")
        return (
            isinstance(ssid, str)
            and 1 <= len(ssid.encode("utf-8")) <= 32
            and security in NETWORK_WIFI_SECURITY
            and credential_state in {"absent", "pending_input", "stored"}
            and (security == "open") == (credential_state == "absent")
        )

    @staticmethod
    def _valid_network_desired_state(value: object) -> bool:
        if not isinstance(value, Mapping) or set(value) != {"mode", "wifi_client", "ethernet"}:
            return False
        mode = value.get("mode")
        wifi = value.get("wifi_client")
        ethernet = value.get("ethernet")
        if mode not in NETWORK_MODES:
            return False
        if wifi is not None and not GatewayHandler._valid_network_desired_wifi_client(wifi):
            return False
        if ethernet is not None and not GatewayHandler._valid_network_ethernet(ethernet):
            return False
        return not (
            mode == "wifi-client" and wifi is None or mode == "ethernet-static" and ethernet is None
        )

    @staticmethod
    def _valid_network_apply_wifi_client(value: object) -> bool:
        if not isinstance(value, Mapping) or not {"ssid", "security"}.issubset(value):
            return False
        security = value.get("security")
        expected_keys = (
            {"ssid", "security"} if security == "open" else {"ssid", "security", "credential_ref"}
        )
        if set(value) != expected_keys or security not in NETWORK_WIFI_SECURITY:
            return False
        ssid = value.get("ssid")
        credential_ref = value.get("credential_ref")
        return (
            isinstance(ssid, str)
            and 1 <= len(ssid.encode("utf-8")) <= 32
            and (
                security == "open"
                or isinstance(credential_ref, str)
                and 1 <= len(credential_ref) <= 128
                and NETWORK_CREDENTIAL_REF.fullmatch(credential_ref) is not None
            )
        )

    @staticmethod
    def _valid_network_apply_desired_state(value: object) -> bool:
        if not isinstance(value, Mapping) or set(value) != {"mode", "wifi_client", "ethernet"}:
            return False
        mode = value.get("mode")
        wifi = value.get("wifi_client")
        ethernet = value.get("ethernet")
        if mode not in NETWORK_MODES:
            return False
        if mode == "wifi-client":
            if not GatewayHandler._valid_network_apply_wifi_client(wifi):
                return False
        elif wifi is not None:
            return False
        if ethernet is not None and not GatewayHandler._valid_network_ethernet(ethernet):
            return False
        return not (mode == "ethernet-static" and ethernet is None)

    @staticmethod
    def _valid_network_interface(value: object) -> bool:
        if not isinstance(value, Mapping) or set(value) != {
            "state",
            "interface",
            "addresses",
            "peer_or_ssid",
        }:
            return False
        state = value.get("state")
        interface = value.get("interface")
        addresses = value.get("addresses")
        peer_or_ssid = value.get("peer_or_ssid")
        if (
            state not in NETWORK_INTERFACE_STATES
            or (interface is not None and not isinstance(interface, str))
            or (isinstance(interface, str) and NETWORK_TOKEN.fullmatch(interface) is None)
            or not isinstance(addresses, list)
            or any(not isinstance(address, str) or not address for address in addresses)
            or len(set(addresses)) != len(addresses)
            or (peer_or_ssid is not None and not isinstance(peer_or_ssid, str))
            or (isinstance(peer_or_ssid, str) and not 1 <= len(peer_or_ssid) <= 128)
        ):
            return False
        return not (
            state in {"connected", "active", "degraded"} and (interface is None or not addresses)
        )

    @staticmethod
    def _valid_network_mdns(value: object) -> bool:
        if not isinstance(value, Mapping) or set(value) != {
            "hostname",
            "service",
            "aliases",
            "port",
        }:
            return False
        aliases = value.get("aliases")
        return (
            isinstance(value.get("hostname"), str)
            and MDNS_TOKEN.fullmatch(str(value["hostname"])) is not None
            and str(value["hostname"]).endswith(".local")
            and isinstance(value.get("service"), str)
            and MDNS_TOKEN.fullmatch(str(value["service"])) is not None
            and isinstance(aliases, list)
            and len(aliases) <= 16
            and all(
                isinstance(alias, str) and MDNS_TOKEN.fullmatch(alias) is not None
                for alias in aliases
            )
            and len(set(aliases)) == len(aliases)
            and type(value.get("port")) is int
            and 1 <= value["port"] <= 65535
        )

    @staticmethod
    def _valid_network_devices(value: object) -> bool:
        if not isinstance(value, list) or len(value) > 64:
            return False
        interfaces: set[str] = set()
        for device in value:
            if (
                not isinstance(device, Mapping)
                or set(device) != {"interface", "type", "state"}
                or not all(
                    isinstance(device.get(key), str) for key in ("interface", "type", "state")
                )
                or any(
                    NETWORK_TOKEN.fullmatch(str(device[key])) is None
                    for key in ("interface", "type", "state")
                )
                or device["interface"] in interfaces
            ):
                return False
            interfaces.add(str(device["interface"]))
        return True

    @staticmethod
    def _valid_network_observed_state(value: object) -> bool:
        if not isinstance(value, Mapping) or set(value) != {
            "ap",
            "wifi_client",
            "wired",
            "default_route",
            "mdns",
            "devices",
        }:
            return False
        return (
            GatewayHandler._valid_network_interface(value.get("ap"))
            and GatewayHandler._valid_network_interface(value.get("wifi_client"))
            and GatewayHandler._valid_network_interface(value.get("wired"))
            and value.get("default_route") in {"wifi_client", "wired", "none"}
            and GatewayHandler._valid_network_mdns(value.get("mdns"))
            and GatewayHandler._valid_network_devices(value.get("devices"))
        )

    @staticmethod
    def _valid_network_mutation_capability(value: object) -> bool:
        if not isinstance(value, Mapping) or set(value) != {
            "enabled",
            "disabled_reason",
            "operations",
            "idempotency_key_required",
            "secret_handling",
            "active_state_policy",
        }:
            return False
        enabled = value.get("enabled")
        disabled_reason = value.get("disabled_reason")
        return (
            type(enabled) is bool
            and (
                disabled_reason is None
                if enabled
                else disabled_reason in NETWORK_MUTATION_DISABLED_REASONS
            )
            and value.get("operations") == NETWORK_MUTATION_OPERATIONS
            and value.get("idempotency_key_required") is True
            and value.get("secret_handling") == "opaque_credential_reference_only"
            and value.get("active_state_policy") == "idle_only"
        )

    @staticmethod
    def _valid_network_concurrency_capability(value: object) -> bool:
        return (
            isinstance(value, Mapping)
            and set(value)
            == {
                "rescue_ap_required",
                "same_phy_ap_sta",
                "exclusive_client_failure_timeout_seconds",
                "max_managed_interfaces",
                "max_ap_interfaces",
            }
            and value.get("rescue_ap_required") is True
            and value.get("same_phy_ap_sta") in {"supported", "unsupported", "unverified"}
            and value.get("exclusive_client_failure_timeout_seconds") == 10
            and type(value.get("max_managed_interfaces")) is int
            and 0 <= value["max_managed_interfaces"] <= 8
            and type(value.get("max_ap_interfaces")) is int
            and 0 <= value["max_ap_interfaces"] <= 8
        )

    @staticmethod
    def _valid_network_transaction_error(value: object) -> bool:
        return (
            isinstance(value, Mapping)
            and set(value) == {"code", "message", "retryable"}
            and value.get("code") in NETWORK_TRANSACTION_ERROR_CODES
            and isinstance(value.get("message"), str)
            and 1 <= len(value["message"]) <= 512
            and type(value.get("retryable")) is bool
        )

    @staticmethod
    def _valid_network_transaction(value: object) -> bool:
        if not isinstance(value, Mapping) or set(value) != {
            "schema",
            "authority_epoch",
            "source_revision",
            "transaction_id",
            "operation",
            "status",
            "stage",
            "desired",
            "accepted_at",
            "updated_at",
            "deadline",
            "recovery_action",
            "rescue",
            "error",
        }:
            return False
        rescue = value.get("rescue")
        error = value.get("error")
        deadline = value.get("deadline")
        status = value.get("status")
        stage = value.get("stage")
        valid_deadline = deadline is None or (
            isinstance(deadline, Mapping)
            and set(deadline) == {"time_base", "deadline_ns", "remaining_seconds"}
            and deadline.get("time_base") == "device_monotonic"
            and type(deadline.get("deadline_ns")) is int
            and deadline["deadline_ns"] >= 0
            and isinstance(deadline.get("remaining_seconds"), (int, float))
            and not isinstance(deadline.get("remaining_seconds"), bool)
            and 0 <= deadline["remaining_seconds"] <= 10
        )
        valid = (
            value.get("schema") == "ylx.network-transaction.v1"
            and GatewayHandler._valid_uuid4(value.get("authority_epoch"))
            and type(value.get("source_revision")) is int
            and value["source_revision"] >= 0
            and isinstance(value.get("transaction_id"), str)
            and UUID_V7.fullmatch(str(value["transaction_id"])) is not None
            and value.get("operation") in NETWORK_MUTATION_OPERATIONS
            and status in NETWORK_TRANSACTION_STATUSES
            and stage in NETWORK_TRANSACTION_STAGES
            and NETWORK_STAGE_STATUSES.get(str(stage)) == status
            and GatewayHandler._valid_network_desired_state(value.get("desired"))
            and GatewayHandler._valid_datetime(value.get("accepted_at"))
            and GatewayHandler._valid_datetime(value.get("updated_at"))
            and valid_deadline
            and value.get("recovery_action") in NETWORK_RECOVERY_ACTIONS
            and isinstance(rescue, Mapping)
            and set(rescue) == {"ap_validated", "fallback_mode", "failure_trigger_seconds"}
            and type(rescue.get("ap_validated")) is bool
            and rescue.get("fallback_mode") == "hotspot"
            and rescue.get("failure_trigger_seconds") == 10
            and (error is None or GatewayHandler._valid_network_transaction_error(error))
        )
        if not valid:
            return False
        terminal = status in {"committed", "rescued", "failed"}
        return not (
            (status in {"accepted", "running", "committed"} and error is not None)
            or (status in {"rescued", "failed"} and error is None)
            or (terminal and deadline is not None)
            or (stage in {"activating", "verifying"} and deadline is None)
            or (status == "rescued" and value.get("recovery_action") != "reconnect_rescue_ap")
        )

    @staticmethod
    def _valid_network_transaction_window(value: object) -> bool:
        return (
            isinstance(value, Mapping)
            and set(value) == {"current", "latest"}
            and (
                value.get("current") is None
                or GatewayHandler._valid_network_transaction(value.get("current"))
            )
            and (
                value.get("latest") is None
                or GatewayHandler._valid_network_transaction(value.get("latest"))
            )
        )

    @staticmethod
    def _valid_network_receipt(value: object) -> bool:
        return (
            isinstance(value, Mapping)
            and set(value) == {"schema", "accepted_at", "transaction"}
            and value.get("schema") == "ylx.network-transaction-receipt.v1"
            and GatewayHandler._valid_datetime(value.get("accepted_at"))
            and GatewayHandler._valid_network_transaction(value.get("transaction"))
            and value["transaction"]["status"] == "accepted"
            and value.get("accepted_at") == value["transaction"]["accepted_at"]
        )

    @staticmethod
    def _valid_network_scan(value: object) -> bool:
        if not isinstance(value, Mapping) or set(value) != {
            "schema",
            "authority_epoch",
            "source_revision",
            "scanned_at",
            "networks",
        }:
            return False
        networks = value.get("networks")
        if not isinstance(networks, list) or len(networks) > 256:
            return False
        for entry in networks:
            if not isinstance(entry, Mapping) or set(entry) != {
                "ssid",
                "hidden",
                "security",
                "signal_dbm",
                "credential_required",
            }:
                return False
            ssid = entry.get("ssid")
            hidden = entry.get("hidden")
            security = entry.get("security")
            signal = entry.get("signal_dbm")
            if (
                type(hidden) is not bool
                or security not in NETWORK_WIFI_SECURITY
                or type(signal) is not int
                or not -127 <= signal <= 0
                or type(entry.get("credential_required")) is not bool
                or entry["credential_required"] != (security != "open")
                or hidden
                and ssid is not None
                or not hidden
                and (not isinstance(ssid, str) or not 1 <= len(ssid.encode("utf-8")) <= 32)
            ):
                return False
        return (
            value.get("schema") == "ylx.network-scan.v1"
            and GatewayHandler._valid_uuid4(value.get("authority_epoch"))
            and type(value.get("source_revision")) is int
            and value["source_revision"] >= 0
            and GatewayHandler._valid_datetime(value.get("scanned_at"))
        )

    @staticmethod
    def _valid_network_credential_receipt(value: object) -> bool:
        if not isinstance(value, Mapping) or set(value) != {
            "schema",
            "credential_ref",
            "issued_at",
            "expires_at",
            "ttl_seconds",
            "single_use",
        }:
            return False
        issued_at = value.get("issued_at")
        expires_at = value.get("expires_at")
        if not (
            value.get("schema") == "ylx.network-credential-receipt.v1"
            and isinstance(value.get("credential_ref"), str)
            and 1 <= len(value["credential_ref"]) <= 128
            and NETWORK_CREDENTIAL_REF.fullmatch(value["credential_ref"]) is not None
            and GatewayHandler._valid_datetime(issued_at)
            and GatewayHandler._valid_datetime(expires_at)
            and type(value.get("ttl_seconds")) is int
            and 1 <= value["ttl_seconds"] <= 120
            and value.get("single_use") is True
        ):
            return False
        issued = datetime.fromisoformat(str(issued_at).replace("Z", "+00:00"))
        expires = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
        return (expires - issued).total_seconds() == value["ttl_seconds"]

    @staticmethod
    def _valid_network_provider_error(error: ProviderError, *, operation: str) -> bool:
        if not isinstance(error.message, str) or not 1 <= len(error.message) <= 1024:
            return False
        details = error.details
        read_errors = {
            "status": ("network_status_unavailable", "网络状态暂时不可用"),
            "scan": ("network_scan_unavailable", "无线网络扫描暂时不可用"),
        }
        expected_read_error = read_errors.get(operation)
        if expected_read_error is not None:
            return (
                (error.code, error.message) == expected_read_error
                and error.status == HTTPStatus.SERVICE_UNAVAILABLE
                and error.retryable is True
                and details is None
            )
        if error.code == "network_mutation_unavailable":
            return (
                error.status == HTTPStatus.SERVICE_UNAVAILABLE
                and error.retryable is True
                and isinstance(details, Mapping)
                and set(details) == {"reason"}
                and details.get("reason") in NETWORK_MUTATION_DISABLED_REASONS
            )
        if error.code == "idempotency_conflict":
            original_transaction_id = (
                details.get("original_transaction_id") if isinstance(details, Mapping) else None
            )
            return (
                operation in NETWORK_MUTATION_OPERATIONS
                and error.status == HTTPStatus.CONFLICT
                and error.retryable is False
                and isinstance(details, Mapping)
                and set(details) == {"idempotency_scope", "original_transaction_id"}
                and details.get("idempotency_scope") == "network-mutation"
                and (
                    original_transaction_id is None
                    or isinstance(original_transaction_id, str)
                    and UUID_V7.fullmatch(original_transaction_id) is not None
                )
            )
        if error.code == "invalid_network_desired_state":
            return (
                operation in {"apply", "retry"}
                and error.status == HTTPStatus.UNPROCESSABLE_ENTITY
                and error.retryable is False
                and isinstance(details, Mapping)
                and set(details) == {"field", "reason"}
                and isinstance(details.get("field"), str)
                and 1 <= len(details["field"]) <= 128
                and details.get("reason") in INVALID_NETWORK_DESIRED_STATE_REASONS
            )
        if error.code == "network_transaction_not_found":
            transaction_id = details.get("transaction_id") if isinstance(details, Mapping) else None
            return (
                operation == "retry"
                and error.status == HTTPStatus.NOT_FOUND
                and error.retryable is False
                and isinstance(details, Mapping)
                and set(details) == {"transaction_id"}
                and isinstance(transaction_id, str)
                and UUID_V7.fullmatch(transaction_id) is not None
            )
        return (
            operation == "credentials"
            and error.code == "invalid_request"
            and error.status == HTTPStatus.BAD_REQUEST
            and error.retryable is False
        )

    def _send_network_provider_error(self, error: ProviderError, *, operation: str) -> None:
        if not self._valid_network_provider_error(error, operation=operation):
            self._invalid_source_state("daemon network error 响应无效")
            return
        self._problem(
            error.status,
            error.code,
            error.message,
            retryable=error.retryable,
            headers={"YLX-Error-Code": error.code},
            details=error.details,
        )

    def _send_camera_provider_error(self, error: ProviderError) -> None:
        headers = {"YLX-Error-Code": error.code} if error.code == "camera_not_connected" else None
        self._problem(
            error.status,
            error.code,
            error.message,
            retryable=error.retryable,
            headers=headers,
            details=error.details,
        )

    def _get_network_status(self) -> None:
        if self._principal("getNetworkStatus") is None:
            return
        try:
            status = self.server.provider.network_status()
        except ProviderError as error:
            self._send_network_provider_error(error, operation="status")
            return
        except Exception:
            self._provider_failure()
            return
        if not self._valid_network_status(status):
            self._invalid_source_state("daemon network 状态无效")
            return
        self._send_json(HTTPStatus.OK, status)

    def _get_network_scan(self) -> None:
        if self._principal("scanNetworks") is None:
            return
        try:
            body = self.server.provider.scan_networks()
        except ProviderError as error:
            self._send_network_provider_error(error, operation="scan")
            return
        except Exception:
            self._provider_failure()
            return
        if not self._valid_network_scan(body):
            self._invalid_source_state("daemon network scan 无效")
            return
        self._send_json(HTTPStatus.OK, body)

    def _network_events(self) -> None:
        if self._principal("streamNetworkEvents") is None:
            return
        parsed = urlsplit(self.path)
        if parsed.query:
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "SSE 不接受查询参数")
            return
        last_event_id, last_event_id_is_single = self._single_header("Last-Event-ID")
        if not last_event_id_is_single or (
            last_event_id is not None
            and (re.fullmatch(r"[0-9]+", last_event_id) is None or len(last_event_id) > 128)
        ):
            self._problem(
                HTTPStatus.BAD_REQUEST,
                "invalid_request",
                "Last-Event-ID 必须是十进制 delivery ID",
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
                status = self.server.provider.network_status()
            except ProviderError as error:
                self._send_network_provider_error(error, operation="status")
                return
            except Exception:
                self._provider_failure()
                return
            if not self._valid_network_status(status):
                self._invalid_source_state("daemon network 状态无效")
                return
            cursor = None if last_event_id is None else int(last_event_id)
            events = self.server.network_event_buffer.replay(cursor, status)
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
                if events:
                    cursor = events[-1].delivery_id
                if cursor is None:
                    return
                heartbeat_at = time.monotonic() + self.server.sse_heartbeat_seconds
                while True:
                    now = time.monotonic()
                    time.sleep(
                        min(
                            self.server.network_sse_poll_seconds,
                            max(0.0, heartbeat_at - now),
                        )
                    )
                    try:
                        status = self.server.provider.network_status()
                    except Exception:
                        return
                    if not self._valid_network_status(status):
                        return
                    delivered = self.server.network_event_buffer.events_after(cursor, status)
                    if delivered:
                        for event in delivered:
                            self.wfile.write(event.encode())
                        cursor = delivered[-1].delivery_id
                        heartbeat_at = time.monotonic() + self.server.sse_heartbeat_seconds
                    elif time.monotonic() >= heartbeat_at:
                        self.wfile.write(heartbeat_comment())
                        heartbeat_at = time.monotonic() + self.server.sse_heartbeat_seconds
                    else:
                        continue
                    self.wfile.flush()
            except OSError:
                return
        finally:
            self.server.sse_slots.release()

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
        expected_schema_version = "v2" if api_version == "v2" else "v3"
        expected_schema = f"ylx.safe-swap-receipt-resource.{expected_schema_version}"
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
            self._send_camera_provider_error(error)
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
                self._send_camera_provider_error(error)
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
            and parts[2] in SUPPORTED_API_VERSIONS
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
        if not content_type_is_single:
            self._close_if_request_body_is_unread()
            self._problem(
                HTTPStatus.BAD_REQUEST,
                "invalid_request",
                "Content-Type 只能出现一次",
            )
            return None
        content_type = (content_type_value or "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            self._close_if_request_body_is_unread()
            self._problem(
                HTTPStatus.BAD_REQUEST, "invalid_request", "Content-Type 必须是 application/json"
            )
            return None
        length = getattr(self, "_request_content_length", None)
        if length is None or length <= 0:
            self._close_if_request_body_is_unread()
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "请求体大小无效")
            return None
        try:
            payload = self.rfile.read(length)
        except OSError:
            self.close_connection = True
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "请求体读取失败")
            return None
        if len(payload) != length:
            self.close_connection = True
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "请求体长度不完整")
            return None
        try:
            body = json.loads(payload)
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

    @staticmethod
    def _validate_network_body(operation: str, body: Mapping[str, object]) -> bool:
        if operation == "apply":
            return (
                set(body) == {"schema", "desired"}
                and body.get("schema") == "ylx.network-apply-request.v1"
                and GatewayHandler._valid_network_apply_desired_state(body.get("desired"))
            )
        if operation == "retry":
            return (
                set(body) == {"schema", "transaction_id"}
                and body.get("schema") == "ylx.network-retry-request.v1"
                and isinstance(body.get("transaction_id"), str)
                and UUID_V7.fullmatch(str(body["transaction_id"])) is not None
            )
        return set(body) == {"schema"} and body.get("schema") == "ylx.network-forget-request.v1"

    def _write_principal(self, operation_id: str) -> Principal | None:
        principal = self._principal(operation_id)
        if principal is None:
            return None
        origin, origin_is_single = self._single_header("Origin")
        if self.server.security.profile == "customer" and (not origin_is_single or origin is None):
            self._audit(operation_id, None, "origin_forbidden", principal)
            self._close_if_request_body_is_unread()
            self._problem(HTTPStatus.FORBIDDEN, "origin_forbidden", "客户写请求必须提供单一 Origin")
            return None
        csrf_token = self.server.security.csrf_token
        csrf_value, csrf_is_single = self._single_header("X-CSRF-Token")
        csrf_required = self.server.security.profile == "customer" or (
            origin is not None and not self._is_same_gateway_origin(origin)
        )
        if not csrf_is_single or (
            csrf_required
            and (csrf_token is None or not hmac.compare_digest(csrf_value or "", csrf_token))
        ):
            self._audit(operation_id, None, "csrf_forbidden", principal)
            self._close_if_request_body_is_unread()
            self._problem(HTTPStatus.FORBIDDEN, "csrf_forbidden", "CSRF token 缺失或无效")
            return None
        return principal

    def _command_principal(
        self,
        operation_id: str,
    ) -> tuple[Principal, str] | None:
        principal = self._write_principal(operation_id)
        if principal is None:
            return None
        key, key_is_single = self._single_header("Idempotency-Key")
        if (
            not key_is_single
            or key is None
            or not 1 <= len(key) <= 128
            or any(character == " " or not 0x21 <= ord(character) <= 0x7E for character in key)
        ):
            self._close_if_request_body_is_unread()
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "Idempotency-Key 无效")
            return None
        return principal, key

    def _create_network_credential(self) -> None:
        principal = self._write_principal("createNetworkCredentialReference")
        if principal is None:
            return
        body = self._read_json()
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
            self._problem(
                HTTPStatus.BAD_REQUEST,
                "invalid_request",
                "请求体不符合网络凭据契约",
            )
            return
        try:
            receipt = self.server.provider.create_network_credential(
                NetworkCredentialCommand(principal.principal_id, passphrase)
            )
        except ProviderError as error:
            self._send_network_provider_error(error, operation="credentials")
            return
        except Exception:
            self._provider_failure()
            return
        if not self._valid_network_credential_receipt(receipt):
            self._invalid_source_state("daemon network credential 响应无效")
            return
        self._send_json(HTTPStatus.CREATED, receipt)

    def _capture_command(self, api_version: str, operation: str) -> None:
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
            self._send_camera_provider_error(error)
            return
        headers = {"Idempotency-Replayed": "true"} if result.replayed else None
        if operation == "stop" and result.status == HTTPStatus.NO_CONTENT and result.body is None:
            self._send_empty(result.status, headers=headers)
        elif result.status == HTTPStatus.ACCEPTED:
            body = project_capture_status(result.body, api_version=api_version)
            self._send_capture_status(
                result.status,
                body,
                api_version=api_version,
                headers=headers,
            )
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
            self._send_camera_provider_error(error)
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

    def _network_command(self, operation: str) -> None:
        operation_id = {
            "apply": "applyNetworkDesiredState",
            "retry": "retryNetworkTransaction",
            "forget": "forgetNetworkClientProfile",
        }[operation]
        command_identity = self._command_principal(operation_id)
        if command_identity is None:
            return
        principal, key = command_identity
        body = self._read_json()
        if body is None:
            return
        if not self._validate_network_body(operation, body):
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "请求体不符合网络命令契约")
            return
        canonical = json.dumps(
            body, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
        command = NetworkCommand(principal.principal_id, key, body, canonical)
        try:
            if operation == "apply":
                result = self.server.provider.apply_network_desired_state(command)
            elif operation == "retry":
                result = self.server.provider.retry_network_transaction(command)
            else:
                result = self.server.provider.forget_network_client_profile(command)
        except ProviderError as error:
            self._send_network_provider_error(error, operation=operation)
            return
        headers = {"Idempotency-Replayed": "true"} if result.replayed else None
        if result.status == HTTPStatus.ACCEPTED and self._valid_network_receipt(result.body):
            self._send_json(result.status, result.body, headers=headers)
            return
        self._problem(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            "invalid_source_state",
            "daemon network command 响应无效",
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
    network_sse_poll_seconds: float = 0.25,
    max_sse_connections: int = 4,
    max_preview_streams: int = 2,
    external_scheme: str = "http",
) -> GatewayServer:
    return GatewayServer(
        (host, port),
        provider,
        security,
        audit_sink,
        event_buffer or EventReplayBuffer(),
        sse_heartbeat_seconds,
        network_sse_poll_seconds,
        max_sse_connections,
        max_preview_streams,
        external_scheme,
    )
