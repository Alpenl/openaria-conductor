"""Gateway security, Host/Origin validation, CSRF, audit, JSON parsing, and response helpers."""

from __future__ import annotations

import hmac
import ipaddress
import json
import re
import ssl
import sys
import time
import uuid
from collections.abc import Mapping
from contextlib import suppress
from http import HTTPStatus
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from rp_ylx.api.controllers.types import LockedRepresentation
from rp_ylx.api.security import AuditEvent, Principal
from rp_ylx.api.validators.commands import valid_idempotency_key
from rp_ylx.api.validators.network import MDNS_TOKEN
from rp_ylx.operational_logging import operational_logger

if TYPE_CHECKING:
    from rp_ylx.api.router import RouteMatch

MAX_BODY_BYTES = 64 * 1024
HTTP_CONTENT_LENGTH = re.compile(r"^(?:0|[1-9][0-9]*)$")


def _valid_port(port: str) -> bool:
    return bool(port) and port.isascii() and port.isdigit() and 1 <= int(port) <= 65535


def extract_host_hostname(host_header: str) -> str | None:
    """Extract normalized lowercase hostname from a single HTTP Host header."""
    raw = host_header.strip()
    if not raw or any(ch.isspace() or ord(ch) < 32 or ch in "/@\\#" for ch in raw):
        return None
    if raw.startswith("["):
        end = raw.find("]")
        if end < 0:
            return None
        addr = raw[1:end]
        rest = raw[end + 1 :]
        if rest and (not rest.startswith(":") or not _valid_port(rest[1:])):
            return None
        try:
            ip = ipaddress.IPv6Address(addr)
        except ValueError:
            return None
        return str(ip).casefold()
    if ":" in raw:
        host_part, _, port_part = raw.partition(":")
        if not _valid_port(port_part):
            return None
        hostname = host_part
    else:
        hostname = raw
    hostname = hostname.rstrip(".").casefold()
    if not hostname:
        return None
    return hostname


class GatewayMiddlewareMixin:
    """Security, framing, audit, and response-writing primitives on GatewayHandler."""

    def end_headers(self) -> None:
        self._headers_sent = True
        super().end_headers()

    def _single_header(self, name: str) -> tuple[str | None, bool]:
        headers = getattr(self, "headers", None)
        if headers is None:
            return None, True
        values = headers.get_all(name, failobj=[])
        if len(values) > 1:
            return None, False
        return (values[0] if values else None), True

    def _is_allowed_gateway_host(self, host_header: str | None) -> bool:
        if host_header is None:
            return False
        hostname = extract_host_hostname(host_header)
        if hostname is None:
            return False
        if hostname in self.server.allowed_hosts:
            return True
        if (
            hostname.endswith(".local")
            and len(hostname) > len(".local")
            and MDNS_TOKEN.fullmatch(hostname) is not None
        ):
            return True
        try:
            ip = ipaddress.ip_address(hostname)
        except ValueError:
            return False
        return not ip.is_unspecified and (ip.is_loopback or ip.is_private or ip.is_link_local)

    def _check_gateway_host(
        self, operation_id: str, resource_id: str | None = None
    ) -> tuple[bool, bool]:
        """Validate Host header when present; return (allowed, host_is_single)."""
        host, host_is_single = self._single_header("Host")
        if not host_is_single or (host is not None and not self._is_allowed_gateway_host(host)):
            self._audit(operation_id, resource_id, "origin_forbidden")
            self._close_if_request_body_is_unread()
            if self.command == "HEAD":
                self._send_empty(HTTPStatus.FORBIDDEN)
            else:
                self._problem(HTTPStatus.FORBIDDEN, "origin_forbidden", "请求 Host 未获允许")
            return False, host_is_single
        return True, host_is_single

    def _is_same_gateway_origin(self, origin: str | None) -> bool:
        if origin is None:
            return False
        host, host_is_single = self._single_header("Host")
        if not host_is_single or host is None or not self._is_allowed_gateway_host(host):
            return False
        try:
            parsed = urlsplit(origin)
        except ValueError:
            return False
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
                    "YLX-Audit-Status, YLX-Error-Code, YLX-Manifest-SHA256, YLX-Wait-State"
                ),
            )
            self.send_header("Vary", "Origin")

    def _send_common_tail_headers(self, *, headers: Mapping[str, str] | None = None) -> None:
        if getattr(self, "_audit_failed", False):
            self.send_header("YLX-Audit-Status", "unavailable")
        if headers:
            for name, value in headers.items():
                self.send_header(name, value)
        if self.close_connection:
            self.send_header("Connection", "close")
        self._cors_headers()

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
        self._send_common_tail_headers(headers=headers)
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
        self._send_common_tail_headers(headers=headers)
        self.end_headers()

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
        self._send_common_tail_headers()
        self.end_headers()
        if head or selected_length == 0:
            return

        write_chunk = self.server.write_chunk_size
        is_tls = isinstance(self.connection, ssl.SSLSocket)
        send_to = getattr(representation, "send_to", None)
        if not is_tls and callable(send_to):
            self.wfile.flush()
            for position in range(0, selected_length, write_chunk):
                count = min(write_chunk, selected_length - position)
                with self.server.response_write(self.connection):
                    sent = send_to(self.connection.fileno(), offset + position, count)
                if sent != count:
                    raise OSError("representation sender returned a short write")
            return

        # Over TLS (ssl.SSLSocket) or when send_to is unavailable, never write to raw fd.
        written = 0
        for chunk in representation.iter_chunks(offset, selected_length):
            view = memoryview(chunk).cast("B")
            if written + len(view) > selected_length:
                raise OSError("representation sender exceeded expected length")
            count = self.wfile.write(chunk)
            if isinstance(count, int) and count != len(view):
                raise OSError("representation sender returned a short write")
            written += len(view)
        if written != selected_length:
            raise OSError("representation sender returned a short write")

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
        if getattr(self, "_headers_sent", False):
            # Never emit a second HTTP status/JSON body into an already-started 200 stream.
            self.close_connection = True
            return
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

    def _invalid_source_state(self, message: str) -> None:
        self._problem(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            "invalid_source_state",
            message,
        )

    def _provider_failure(self, exc: BaseException | None = None) -> None:
        err = exc if exc is not None else sys.exc_info()[1]
        if err is not None:
            with suppress(Exception):
                operational_logger("gateway").event(
                    "gateway_provider_failure",
                    level="error",
                    exception_type=type(err).__name__,
                )
        self._problem(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            "provider_failure",
            "设备 provider 未能完成请求",
            retryable=True,
        )

    def _audit(
        self,
        operation_id: str,
        resource_id: str | None,
        outcome: str,
        principal: Principal | None = None,
    ) -> bool:
        try:
            self.server.audit_sink(
                AuditEvent(
                    request_id=self.request_id,
                    principal_id=None if principal is None else principal.principal_id,
                    operation_id=operation_id,
                    resource_id=resource_id,
                    outcome=outcome,
                )
            )
        except Exception as error:
            self._audit_failed = True
            self.server.report_audit_failure(operation_id, outcome, error)
            return False
        return True

    def _close_if_request_body_is_unread(self) -> None:
        content_length = getattr(self, "_request_content_length", None)
        if self.command in {"POST", "PUT", "PATCH"} or content_length not in {None, 0}:
            self.close_connection = True

    def _authorize_principal(
        self,
        operation_id: str,
        resource_id: str | None = None,
        *,
        audit_allowed: bool = True,
    ) -> Principal | None:
        host_ok, _ = self._check_gateway_host(operation_id, resource_id)
        if not host_ok:
            return None
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
        if audit_allowed and not self._emit_allowed_audit(operation_id, resource_id, principal):
            return None
        return principal

    def _emit_allowed_audit(
        self, operation_id: str, resource_id: str | None, principal: Principal
    ) -> bool:
        if not self._audit(operation_id, resource_id, "allowed", principal):
            self._close_if_request_body_is_unread()
            headers = {"Retry-After": "1", "YLX-Error-Code": "audit_unavailable"}
            if self.command == "HEAD":
                self._send_empty(HTTPStatus.SERVICE_UNAVAILABLE, headers=headers)
            else:
                self._problem(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    "audit_unavailable",
                    "审计记录暂不可用，请稍后重试",
                    retryable=True,
                    headers=headers,
                )
            return False
        return True

    def _principal(self, operation_id: str, resource_id: str | None = None) -> Principal | None:
        return self._authorize_principal(operation_id, resource_id, audit_allowed=True)

    def _write_principal(self, operation_id: str) -> Principal | None:
        # Defer emitting "allowed" until Origin, Auth, Permission, and CSRF all pass.
        principal = self._authorize_principal(operation_id, audit_allowed=False)
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
            and (csrf_token is None or not _constant_time_equals(csrf_value or "", csrf_token))
        ):
            self._audit(operation_id, None, "csrf_forbidden", principal)
            self._close_if_request_body_is_unread()
            self._problem(HTTPStatus.FORBIDDEN, "csrf_forbidden", "CSRF token 缺失或无效")
            return None
        if not self._emit_allowed_audit(operation_id, None, principal):
            return None
        return principal

    def _command_principal(self, operation_id: str) -> tuple[Principal, str] | None:
        principal = self._write_principal(operation_id)
        if principal is None:
            return None
        key, key_is_single = self._single_header("Idempotency-Key")
        if not valid_idempotency_key(key, is_single=key_is_single):
            self._close_if_request_body_is_unread()
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "Idempotency-Key 无效")
            return None
        assert key is not None
        return principal, key

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
            with self.server.request_read(self.connection, self._request_read_expires_at):
                remaining = max(0.001, self._request_read_expires_at - time.monotonic())
                self.connection.settimeout(remaining)
                try:
                    payload = self.rfile.read(length)
                finally:
                    self.connection.settimeout(None)
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
            # JSON escapes can represent lone surrogates that UTF-8 storage and
            # command canonicalization cannot encode. Reject before dispatch.
            pending = [body]
            while pending:
                value = pending.pop()
                if isinstance(value, str):
                    value.encode("utf-8")
                elif isinstance(value, dict):
                    pending.extend(value)
                    pending.extend(value.values())
                elif isinstance(value, list):
                    pending.extend(value)
        except (ValueError, RecursionError):
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "请求体不是有效 JSON")
            return None
        if not isinstance(body, dict):
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "请求体必须是 JSON 对象")
            return None
        return body

    def _begin_request(self, *, body_allowed: bool) -> bool:
        self._request_id = str(uuid.uuid4())
        self._audit_failed = False
        self._headers_sent = False
        self._request_content_length: int | None = None
        try:
            urlsplit(self.path)
        except ValueError:
            self.close_connection = True
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "请求目标 URL 无效")
            return False
        if self.request_version == "HTTP/1.1" and not self.headers.get_all("Host", failobj=[]):
            self.close_connection = True
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "HTTP/1.1 请求必须包含 Host")
            return False
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

    def _handle_options(self, matched: RouteMatch) -> None:
        host_ok, _ = self._check_gateway_host("options")
        if not host_ok:
            return
        methods = matched.spec.methods
        allow_header = ", ".join(methods)
        origin, origin_is_single = self._single_header("Origin")
        if not origin_is_single:
            self._problem(HTTPStatus.FORBIDDEN, "forbidden", "请求来源不在允许列表")
            return
        if origin is None:
            self._send_empty(HTTPStatus.NO_CONTENT, headers={"Allow": allow_header})
            return
        if origin not in self.server.security.allowed_origins:
            self._problem(HTTPStatus.FORBIDDEN, "forbidden", "请求来源不在允许列表")
            return

        requested_method, method_is_single = self._single_header("Access-Control-Request-Method")
        if not method_is_single:
            self._problem(
                HTTPStatus.BAD_REQUEST,
                "invalid_request",
                "Access-Control-Request-Method 只能出现一次",
            )
            return
        if requested_method is None:
            self._problem(HTTPStatus.BAD_REQUEST, "invalid_request", "缺少预检请求方法")
            return
        requested_method = requested_method.upper()
        if requested_method not in methods:
            self._problem(
                HTTPStatus.METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "接口不允许该请求方法",
                headers={"Allow": allow_header},
            )
            return

        allowed_headers = matched.spec.allowed_headers(requested_method)
        raw_requested_headers = ",".join(
            self.headers.get_all("Access-Control-Request-Headers", failobj=[])
        )
        requested_headers = {
            value.strip().lower() for value in raw_requested_headers.split(",") if value.strip()
        }
        if requested_headers - {value.lower() for value in allowed_headers}:
            self._problem(HTTPStatus.FORBIDDEN, "forbidden", "预检请求头不在允许列表")
            return

        self._send_empty(
            HTTPStatus.NO_CONTENT,
            headers={
                "Allow": allow_header,
                "Access-Control-Allow-Methods": allow_header,
                "Access-Control-Allow-Headers": ", ".join(allowed_headers),
                "Access-Control-Max-Age": "600",
            },
        )

    def _log_midstream_abort(self, exc: Exception) -> None:
        self.close_connection = True
        with suppress(Exception):
            operational_logger("gateway").event(
                "gateway_midstream_abort",
                level="error",
                request_id=self.request_id,
                exception_type=type(exc).__name__,
            )


def _constant_time_equals(a: str, b: str) -> bool:
    try:
        return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))
    except UnicodeEncodeError:
        return False
