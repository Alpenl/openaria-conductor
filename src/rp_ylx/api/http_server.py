"""Threaded HTTP gateway server, connection deadlines, and declarative request handler."""

from __future__ import annotations

import ipaddress
import math
import os
import select
import selectors
import socket
import ssl
import sys
import threading
import time
import uuid
from collections.abc import Callable, Collection, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager, suppress
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Literal
from urllib.parse import parse_qs, urlsplit

from rp_ylx.api.compat import facade_attr
from rp_ylx.api.controllers.types import DeviceProvider
from rp_ylx.api.middleware import GatewayMiddlewareMixin, extract_host_hostname
from rp_ylx.api.router import match_route
from rp_ylx.api.security import AuditEvent, SecurityPolicy
from rp_ylx.api.streams import (
    EventReplayBuffer,
    _NetworkEventReplayBuffer,
    _SharedNetworkPoller,
    _StreamRegistry,
    _StreamSession,
)
from rp_ylx.operational_logging import operational_logger

_RESPONSE_WRITE_CHUNK = 2 * 1024 * 1024
_DEFAULT_ALLOWED_HOSTS = frozenset({"127.0.0.1", "localhost", "::1", "local"})
_WILDCARD_LISTEN_HOSTS = frozenset({"", "0.0.0.0", "::", "[::]"})


@dataclass
class _IODeadline:
    expires_at: float
    expired: bool = False


class _BoundedResponseWriter:
    def __init__(self, writer: object, connection: socket.socket, server: GatewayServer) -> None:
        self._writer = writer
        self._connection = connection
        self._server = server

    def __getattr__(self, name: str) -> object:
        return getattr(self._writer, name)

    def write(self, payload: bytes) -> int:
        chunk_size = self._server.write_chunk_size
        view = memoryview(payload).cast("B")
        for offset in range(0, len(view), chunk_size):
            with self._server.response_write(self._connection):
                self._writer.write(view[offset : offset + chunk_size])
        return len(view)


def _compute_write_chunk_size(write_timeout_seconds: float) -> int:
    if write_timeout_seconds >= 5.0:
        return _RESPONSE_WRITE_CHUNK
    return 16 * 1024


def _normalize_allowed_hosts(
    listen_host: str, configured: Collection[str] | None
) -> frozenset[str]:
    hosts = set(_DEFAULT_ALLOWED_HOSTS)
    listen_norm = extract_host_hostname(listen_host)
    if listen_norm and listen_host.strip() not in _WILDCARD_LISTEN_HOSTS:
        hosts.add(listen_norm)
    if configured:
        for entry in configured:
            extracted = extract_host_hostname(entry)
            if extracted:
                hosts.add(extracted)
            else:
                with suppress(ValueError):
                    hosts.add(str(ipaddress.IPv6Address(entry.strip())).casefold())
    return frozenset(hosts)


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
        request_timeout_seconds: float = 30.0,
        max_http_connections: int = 32,
        write_timeout_seconds: float = 30.0,
        max_session_downloads: int = 8,
        allowed_hosts: Collection[str] | None = None,
    ) -> None:
        if sse_heartbeat_seconds <= 0 or network_sse_poll_seconds <= 0:
            raise ValueError("SSE 心跳和轮询间隔必须大于零")
        if max_sse_connections < 1 or max_preview_streams < 1:
            raise ValueError("长连接并发上限必须大于零")
        if external_scheme not in {"http", "https"}:
            raise ValueError("external_scheme 必须是 http 或 https")
        if not math.isfinite(request_timeout_seconds) or request_timeout_seconds <= 0:
            raise ValueError("HTTP 连接超时必须是有限正数")
        if (
            not isinstance(max_http_connections, int)
            or isinstance(max_http_connections, bool)
            or max_http_connections < 1
        ):
            raise ValueError("HTTP 连接总上限必须是正整数")
        if not math.isfinite(write_timeout_seconds) or not 0 < write_timeout_seconds <= 3600:
            raise ValueError("HTTP 发送等待上限必须在 (0, 3600] 秒内")
        if (
            not isinstance(max_session_downloads, int)
            or isinstance(max_session_downloads, bool)
            or max_session_downloads < 1
        ):
            raise ValueError("HTTP 录制下载并发上限必须是正整数")
        self.max_session_downloads = max_session_downloads
        self._session_downloads: set[socket.socket] = set()
        self.write_timeout_seconds = write_timeout_seconds
        self.max_http_connections = max_http_connections
        self.allowed_hosts = _normalize_allowed_hosts(address[0], allowed_hosts)
        self.firmware_slots = threading.BoundedSemaphore(4)
        self._http_condition = threading.Condition()
        self._audit_next_report_at: float | None = None
        self._audit_suppressed = 0
        self._http_connections: set[socket.socket] = set()
        self._http_reclaimable: dict[socket.socket, float] = {}
        self._http_cancelled: set[socket.socket] = set()
        self._http_closing = False
        self._io_deadlines: dict[tuple[socket.socket, str], _IODeadline] = {}
        self._deadline_watcher: threading.Thread | None = None
        self._deadline_next_check: float | None = None
        self.request_timeout_seconds = request_timeout_seconds
        self.provider = provider
        self.security = security
        self.audit_sink = audit_sink or (lambda event: None)
        self.event_buffer = event_buffer
        self.network_event_buffer = _NetworkEventReplayBuffer()
        self.sse_heartbeat_seconds = sse_heartbeat_seconds
        self.network_sse_poll_seconds = network_sse_poll_seconds
        self.network_poller = _SharedNetworkPoller(
            lambda: self.provider.network_status(),
            self.network_event_buffer,
            network_sse_poll_seconds,
        )
        self._streams = _StreamRegistry(
            max_sse=max_sse_connections,
            max_preview=max_preview_streams,
        )
        self.external_scheme = external_scheme
        self._use_posix_wakeup = os.name == "posix"
        self._serve_is_shut_down = threading.Event()
        self._serve_shutdown_request = False
        self._serve_started = False
        self._server_closed = False
        self._wakeup_lock = threading.Lock()
        self._wakeup_read_fd: int | None = None
        self._wakeup_write_fd: int | None = None
        handler_cls = facade_attr("rp_ylx.api.gateway", "GatewayHandler", GatewayHandler)
        super().__init__(address, handler_cls)
        self._deadline_watcher = threading.Thread(
            target=self._watch_io_deadlines, name="gateway-io-deadlines", daemon=True
        )
        try:
            if self._use_posix_wakeup:
                self._create_wakeup_pipe()
            self._deadline_watcher.start()
        except BaseException:
            self.server_close()
            raise

    @property
    def write_chunk_size(self) -> int:
        return _compute_write_chunk_size(self.write_timeout_seconds)

    def report_audit_failure(self, operation: str, outcome: str, error: Exception) -> None:
        now_fn = facade_attr("rp_ylx.api.gateway", "time", time).monotonic
        now = now_fn()
        with self._http_condition:
            if self._audit_next_report_at is not None and now < self._audit_next_report_at:
                self._audit_suppressed += 1
                return
            suppressed = self._audit_suppressed
            self._audit_suppressed = 0
            self._audit_next_report_at = now + 30.0
        # A broken diagnostic destination must not replace the operation result.
        # Advance the deadline before logging to bound repeated logger failures too.
        logger_fn = facade_attr("rp_ylx.api.gateway", "operational_logger", operational_logger)
        with suppress(Exception):
            logger_fn("gateway").event(
                "api_audit_failed",
                level="error",
                operation=operation,
                outcome=outcome,
                exception_type=type(error).__name__,
                suppressed_count=suppressed,
            )

    def response_write(self, connection: socket.socket) -> AbstractContextManager[None]:
        return self._io_deadline(connection, "write", time.monotonic() + self.write_timeout_seconds)

    def request_read(
        self, connection: socket.socket, expires_at: float
    ) -> AbstractContextManager[None]:
        return self._io_deadline(connection, "read", expires_at)

    @contextmanager
    def _io_deadline(
        self, connection: socket.socket, kind: Literal["read", "write"], expires_at: float
    ) -> Iterator[None]:
        deadline = _IODeadline(expires_at)
        key = (connection, kind)
        with self._http_condition:
            if self._http_closing or connection in self._http_cancelled:
                raise ConnectionResetError("gateway connection is closing")
            if expires_at <= time.monotonic():
                raise TimeoutError(f"HTTP {kind} exceeded its deadline")
            self._io_deadlines[key] = deadline
            if kind == "read":
                self._http_reclaimable[connection] = expires_at - self.request_timeout_seconds
            if self._deadline_next_check is None or expires_at < self._deadline_next_check:
                self._http_condition.notify_all()
        try:
            yield
        finally:
            with self._http_condition:
                if not deadline.expired and time.monotonic() >= expires_at:
                    deadline.expired = True
                    with suppress(OSError):
                        connection.shutdown(socket.SHUT_RDWR)
                if self._io_deadlines.get(key) is deadline:
                    self._io_deadlines.pop(key)
                if kind == "read":
                    self._http_reclaimable.pop(connection, None)
                cancelled = connection in self._http_cancelled
            if deadline.expired:
                raise TimeoutError(f"HTTP {kind} exceeded its deadline")
            if cancelled:
                raise ConnectionResetError("incomplete request yielded connection capacity")

    def _watch_io_deadlines(self) -> None:
        with self._http_condition:
            while not self._http_closing:
                if not self._io_deadlines:
                    self._deadline_next_check = None
                    self._http_condition.wait()
                    continue
                now = time.monotonic()
                for key, deadline in tuple(self._io_deadlines.items()):
                    connection, _kind = key
                    if deadline.expires_at <= now:
                        deadline.expired = True
                        self._io_deadlines.pop(key)
                        with suppress(OSError):
                            connection.shutdown(socket.SHUT_RDWR)
                if self._io_deadlines:
                    self._deadline_next_check = min(
                        d.expires_at for d in self._io_deadlines.values()
                    )
                    self._http_condition.wait(max(0, self._deadline_next_check - time.monotonic()))

    def process_request(self, request: socket.socket, client_address: tuple[str, int]) -> None:
        with self._http_condition:
            if not self._http_closing and len(self._http_connections) >= self.max_http_connections:
                candidates = {
                    connection: started
                    for connection, started in self._http_reclaimable.items()
                    if connection not in self._http_cancelled
                }
                if candidates:
                    oldest = min(candidates, key=candidates.__getitem__)
                    self._http_cancelled.add(oldest)
                    with suppress(OSError):
                        oldest.shutdown(socket.SHUT_RDWR)
                    self._http_condition.wait_for(
                        lambda: (
                            self._http_closing
                            or len(self._http_connections) < self.max_http_connections
                        ),
                        timeout=0.1,
                    )
            admitted = (
                not self._http_closing and len(self._http_connections) < self.max_http_connections
            )
            if admitted:
                self._http_connections.add(request)
                self._http_reclaimable[request] = time.monotonic()
        if not admitted:
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._http_finished(request)
            raise

    def process_request_thread(
        self, request: socket.socket, client_address: tuple[str, int]
    ) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._http_finished(request)

    def _http_finished(self, request: socket.socket) -> None:
        with self._http_condition:
            self._http_connections.discard(request)
            self._http_reclaimable.pop(request, None)
            self._http_cancelled.discard(request)
            self._session_downloads.discard(request)
            self._http_condition.notify_all()

    def begin_session_download(self, connection: socket.socket) -> bool:
        with self._http_condition:
            limit = min(self.max_session_downloads, max(1, self.max_http_connections - 1))
            if (
                self._http_closing
                or connection in self._http_cancelled
                or len(self._session_downloads) >= limit
            ):
                return False
            self._session_downloads.add(connection)
            return True

    def end_session_download(self, connection: socket.socket) -> None:
        with self._http_condition:
            self._session_downloads.discard(connection)

    def session_download_cancelled(self, connection: socket.socket) -> bool:
        with self._http_condition:
            if self._http_closing or connection in self._http_cancelled:
                return True
        mask = select.POLLERR | select.POLLHUP | select.POLLNVAL
        mask |= getattr(select, "POLLRDHUP", 0)
        try:
            poller = select.poll()
            poller.register(connection.fileno(), mask)
            return any(events & mask for _, events in poller.poll(0))
        except (OSError, ValueError):
            return True

    def begin_request_input(self, connection: socket.socket) -> None:
        with self._http_condition:
            if self._http_closing or connection in self._http_cancelled:
                raise ConnectionResetError("gateway connection is closing")
            self._http_reclaimable[connection] = time.monotonic()

    def _cancel_http_connections(self) -> None:
        with self._http_condition:
            self._http_closing = True
            self._http_condition.notify_all()
            connections = tuple(self._http_connections)
        for connection in connections:
            with suppress(OSError):
                connection.shutdown(socket.SHUT_RDWR)

    def _wait_http_connections(self) -> None:
        with self._http_condition:
            self._http_condition.wait_for(lambda: not self._http_connections, timeout=2.0)

    def handle_error(self, request: object, client_address: object) -> None:
        if isinstance(sys.exception(), (BrokenPipeError, ConnectionResetError, ssl.SSLError)):
            return
        super().handle_error(request, client_address)

    def begin_stream(
        self,
        kind: str,
        connection: socket.socket,
        wake_waiters: Callable[[], None],
    ) -> _StreamSession | None:
        session_cls = facade_attr("rp_ylx.api.gateway", "_StreamSession", _StreamSession)
        return self._streams.begin(kind, connection, wake_waiters, session_cls=session_cls)

    def stream_lifecycle_snapshot(self) -> Mapping[str, object]:
        return self._streams.snapshot()

    def _create_wakeup_pipe(self) -> None:
        if hasattr(os, "pipe2"):
            flags = getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_CLOEXEC", 0)
            read_fd, write_fd = os.pipe2(flags)
        else:
            read_fd, write_fd = os.pipe()
            for fd in (read_fd, write_fd):
                os.set_blocking(fd, False)
        with self._wakeup_lock:
            self._wakeup_read_fd = read_fd
            self._wakeup_write_fd = write_fd

    def _wake_serve_loop(self) -> None:
        with self._wakeup_lock:
            fd = self._wakeup_write_fd
            if fd is None:
                return
            with suppress(BlockingIOError, OSError):
                os.write(fd, b"\x01")

    def _drain_wakeup_pipe(self) -> None:
        with self._wakeup_lock:
            fd = self._wakeup_read_fd
            if fd is None:
                return
            while True:
                try:
                    if not os.read(fd, 256):
                        break
                except (BlockingIOError, OSError):
                    break

    def _close_wakeup_pipe(self) -> None:
        with self._wakeup_lock:
            for attr in ("_wakeup_read_fd", "_wakeup_write_fd"):
                fd = getattr(self, attr)
                if fd is not None:
                    setattr(self, attr, None)
                    with suppress(OSError):
                        os.close(fd)

    def serve_forever(self, poll_interval: float = 0.5) -> None:
        if self._server_closed:
            self._serve_is_shut_down.set()
            return
        self._serve_started = True
        if not self._use_posix_wakeup or self._wakeup_read_fd is None:
            super().serve_forever(poll_interval)
            return
        self._serve_is_shut_down.clear()
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(self, selectors.EVENT_READ)
                with self._wakeup_lock:
                    wakeup_fd = self._wakeup_read_fd
                if wakeup_fd is not None:
                    selector.register(wakeup_fd, selectors.EVENT_READ)
                while not self._serve_shutdown_request:
                    ready = selector.select(poll_interval)
                    if self._serve_shutdown_request:
                        break
                    for key, _ in ready:
                        if key.fileobj == wakeup_fd:
                            self._drain_wakeup_pipe()
                        else:
                            self._handle_request_noblock()
                    self.service_actions()
        finally:
            self._serve_shutdown_request = False
            self._serve_is_shut_down.set()

    def shutdown(self) -> None:
        self._streams.cancel_all("server_shutdown")
        self.network_poller.stop()
        self._cancel_http_connections()
        if self._use_posix_wakeup:
            self._serve_shutdown_request = True
            self._wake_serve_loop()
            if self._serve_started or not self._server_closed:
                self._serve_is_shut_down.wait()
        elif self._serve_started or not self._server_closed:
            super().shutdown()
        self._wait_http_connections()
        self._streams.wait_empty(timeout=2.0)

    def server_close(self) -> None:
        self._server_closed = True
        self._streams.cancel_all("server_shutdown")
        self.network_poller.stop()
        self._cancel_http_connections()
        if self._use_posix_wakeup:
            self._serve_shutdown_request = True
            self._wake_serve_loop()
            if self._serve_started:
                self._serve_is_shut_down.wait(timeout=2.0)
            self._close_wakeup_pipe()
        super().server_close()
        self._wait_http_connections()
        self._streams.wait_empty(timeout=2.0)
        if self._deadline_watcher is not None and self._deadline_watcher.is_alive():
            self._deadline_watcher.join(timeout=2.0)


class GatewayHandler(GatewayMiddlewareMixin, BaseHTTPRequestHandler):
    server: GatewayServer
    protocol_version = "HTTP/1.1"

    def setup(self) -> None:
        super().setup()
        self._headers_sent = False
        self._tls_handshaked = False
        self.wfile = _BoundedResponseWriter(self.wfile, self.connection, self.server)

    def handle_one_request(self) -> None:
        self._header_read_context: AbstractContextManager[None] | None = None
        self.connection.settimeout(self.server.request_timeout_seconds)
        try:
            try:
                self.server.begin_request_input(self.connection)
                if isinstance(self.connection, ssl.SSLSocket) and not getattr(
                    self, "_tls_handshaked", False
                ):
                    if self.connection.cipher() is None:
                        handshake_expires_at = (
                            time.monotonic() + self.server.request_timeout_seconds
                        )
                        with self.server.request_read(self.connection, handshake_expires_at):
                            remaining = max(0.001, handshake_expires_at - time.monotonic())
                            self.connection.settimeout(remaining)
                            self.connection.do_handshake()
                    self._tls_handshaked = True
                self.connection.settimeout(self.server.request_timeout_seconds)
                if not self.rfile.peek(1):
                    self.close_connection = True
                    return
                self._request_read_expires_at = (
                    time.monotonic() + self.server.request_timeout_seconds
                )
                context = self.server.request_read(self.connection, self._request_read_expires_at)
                context.__enter__()
                self._header_read_context = context
                self.connection.settimeout(
                    max(0.001, self._request_read_expires_at - time.monotonic())
                )
                super().handle_one_request()
            finally:
                self._finish_header_read()
        except (TimeoutError, ssl.SSLError, ConnectionResetError, BrokenPipeError):
            self.close_connection = True
        finally:
            self.connection.settimeout(None)

    def send_error(self, code: int, message: str | None = None, explain: str | None = None) -> None:
        del explain
        self.close_connection = True
        if getattr(self, "_headers_sent", False):
            return
        if getattr(self, "request_version", "HTTP/0.9") == "HTTP/0.9":
            self.request_version = self.protocol_version
        if getattr(self, "command", None) == "HEAD" or code in {204, 304} or 100 <= code < 200:
            self._send_empty(code)
            return
        error_code = (
            "not_implemented"
            if code == HTTPStatus.NOT_IMPLEMENTED
            else "method_not_allowed"
            if code == HTTPStatus.METHOD_NOT_ALLOWED
            else "invalid_request"
        )
        try:
            default_message = HTTPStatus(code).phrase
        except ValueError:
            default_message = "Bad request"
        self._problem(code, error_code, message or default_message)

    def _finish_header_read(self) -> None:
        context = self._header_read_context
        self._header_read_context = None
        if context is not None:
            context.__exit__(None, None, None)

    def parse_request(self) -> bool:
        try:
            return super().parse_request()
        finally:
            self.connection.settimeout(None)
            self._finish_header_read()

    def log_message(self, format: str, *args: object) -> None:
        return

    @property
    def request_id(self) -> str:
        request_id = getattr(self, "_request_id", None)
        if request_id is None:
            request_id = str(uuid.uuid4())
            self._request_id = request_id
        return request_id

    def query_params(self) -> dict[str, list[str]]:
        return parse_qs(urlsplit(self.path).query, keep_blank_values=True)

    def _dispatch(self, method: str, *, body_allowed: bool) -> None:
        if not self._begin_request(body_allowed=body_allowed):
            return
        path = urlsplit(self.path).path
        matched = match_route(path)
        if matched is None:
            self._close_if_request_body_is_unread()
            self._problem(HTTPStatus.NOT_FOUND, "not_found", "接口不存在")
            return
        if method in matched.spec.not_found_methods:
            self._close_if_request_body_is_unread()
            self._problem(HTTPStatus.NOT_FOUND, "not_found", "接口不存在")
            return
        if method == "OPTIONS":
            self._handle_options(matched)
            return
        if method not in matched.spec.methods:
            self._close_if_request_body_is_unread()
            self._problem(
                HTTPStatus.METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "接口不允许该请求方法",
                headers={"Allow": ", ".join(matched.spec.methods)},
            )
            return
        try:
            matched.handler(self, method)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        except Exception as error:
            if getattr(self, "_headers_sent", False):
                self._log_midstream_abort(error)
            else:
                self._close_if_request_body_is_unread()
                self._provider_failure(error)

    def do_OPTIONS(self) -> None:
        self._dispatch("OPTIONS", body_allowed=False)

    def do_GET(self) -> None:
        self._dispatch("GET", body_allowed=False)

    def do_HEAD(self) -> None:
        self._dispatch("HEAD", body_allowed=False)

    def do_POST(self) -> None:
        self._dispatch("POST", body_allowed=True)

    def do_PUT(self) -> None:
        self._dispatch("PUT", body_allowed=True)

    def do_PATCH(self) -> None:
        self._dispatch("PATCH", body_allowed=True)

    def do_DELETE(self) -> None:
        self._dispatch("DELETE", body_allowed=False)


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
    request_timeout_seconds: float = 30.0,
    max_http_connections: int = 32,
    write_timeout_seconds: float = 30.0,
    max_session_downloads: int = 8,
    allowed_hosts: Collection[str] | None = None,
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
        request_timeout_seconds,
        max_http_connections,
        write_timeout_seconds,
        max_session_downloads,
        allowed_hosts,
    )
