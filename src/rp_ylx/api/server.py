"""基于 Python 标准库的设备 API v0 模拟服务。"""

from __future__ import annotations

import json
import threading
from collections.abc import Iterable
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from rp_ylx.api.mock_device import ApiError, MockDevice

MAX_BODY_BYTES = 64 * 1024


class DeviceApiServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        device: MockDevice,
        *,
        auto_transition: bool = True,
        allowed_origins: Iterable[str] = (),
    ) -> None:
        self.device = device
        self.auto_transition = auto_transition
        origins = frozenset(allowed_origins)
        if "*" in origins:
            raise ValueError("设备 API 禁止通配符 CORS origin")
        self.allowed_origins = origins
        super().__init__(address, DeviceApiHandler)


class DeviceApiHandler(BaseHTTPRequestHandler):
    server: DeviceApiServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:
        return

    def _json(self, status: int, body: object) -> None:
        payload = json.dumps(body, ensure_ascii=False, sort_keys=True).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self._cors_headers()
        self.end_headers()
        self.wfile.write(payload)

    def _cors_headers(self) -> None:
        origin = self.headers.get("Origin")
        if origin and origin in self.server.allowed_origins:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
            self.send_header(
                "Access-Control-Expose-Headers",
                "X-YLX-Sequence, X-YLX-Monotonic-Ns",
            )

    def _error(self, error: ApiError) -> None:
        self._json(error.status, error.as_body())

    def _read_json(self) -> object:
        content_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            raise ApiError(
                "invalid_request",
                "Content-Type 必须是 application/json",
                retryable=False,
                revision=self.server.device.revision,
            )
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise ApiError(
                "invalid_request",
                "Content-Length 无效",
                retryable=False,
                revision=self.server.device.revision,
            ) from exc
        if length <= 0 or length > MAX_BODY_BYTES:
            raise ApiError(
                "invalid_request",
                "请求体大小无效",
                retryable=False,
                revision=self.server.device.revision,
            )
        try:
            return json.loads(self.rfile.read(length))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ApiError(
                "invalid_request",
                "请求体不是有效 JSON",
                retryable=False,
                revision=self.server.device.revision,
            ) from exc

    def do_GET(self) -> None:
        parsed = urlsplit(self.path)
        try:
            if parsed.path == "/api/v0/device":
                self._json(HTTPStatus.OK, self.server.device.device())
            elif parsed.path == "/api/v0/status":
                self._json(HTTPStatus.OK, self.server.device.status())
            elif parsed.path == "/api/v0/sessions":
                self._json(HTTPStatus.OK, self.server.device.sessions())
            elif parsed.path == "/api/v0/preview/frame":
                self._preview(parse_qs(parsed.query, keep_blank_values=True))
            elif parsed.path == "/api/v0/events":
                self._events(parse_qs(parsed.query))
            else:
                raise ApiError(
                    "not_found", "接口不存在", retryable=False, revision=self.server.device.revision
                )
        except ApiError as exc:
            self._error(exc)

    def do_POST(self) -> None:
        parsed = urlsplit(self.path)
        try:
            payload = self._read_json()
            if parsed.path == "/api/v0/recordings/start":
                result = self.server.device.start(payload)
                transition = self.server.device.complete_start
            elif parsed.path == "/api/v0/recordings/stop":
                result = self.server.device.stop(payload)
                transition = self.server.device.complete_stop
            else:
                raise ApiError(
                    "not_found", "接口不存在", retryable=False, revision=self.server.device.revision
                )
            self._json(result.status, result.body)
            if self.server.auto_transition:
                timer = threading.Timer(0.05, transition)
                timer.daemon = True
                timer.start()
        except ApiError as exc:
            self._error(exc)

    def do_OPTIONS(self) -> None:
        parsed = urlsplit(self.path)
        if parsed.path not in {
            "/api/v0/recordings/start",
            "/api/v0/recordings/stop",
        }:
            self._error(
                ApiError(
                    "not_found", "接口不存在", retryable=False, revision=self.server.device.revision
                )
            )
            return
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Content-Length", "0")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Max-Age", "600")
        self._cors_headers()
        self.end_headers()

    def _preview(self, query: dict[str, list[str]]) -> None:
        eyes = query.get("eye", [])
        sequence_values = query.get("sequence", [])
        if len(eyes) != 1 or len(sequence_values) > 1:
            raise ApiError(
                "invalid_request",
                "eye 必须出现一次，sequence 最多出现一次",
                retryable=False,
                revision=self.server.device.revision,
            )
        sequence = None
        if sequence_values:
            value = sequence_values[0]
            if not value.isascii() or not value.isdigit():
                raise ApiError(
                    "invalid_request",
                    "sequence 必须是非负十进制整数",
                    retryable=False,
                    revision=self.server.device.revision,
                )
            sequence = int(value)
        payload, sequence, monotonic_ns = self.server.device.preview(eyes[0], sequence=sequence)
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "image/jpeg")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-YLX-Sequence", str(sequence))
        self.send_header("X-YLX-Monotonic-Ns", str(monotonic_ns))
        self._cors_headers()
        self.end_headers()
        self.wfile.write(payload)

    def _write_event(self, event_id: int, event: str, data: dict[str, Any]) -> None:
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        self.wfile.write(f"id: {event_id}\nevent: {event}\ndata: {payload}\n\n".encode())
        self.wfile.flush()

    def _events(self, query: dict[str, list[str]]) -> None:
        header = self.headers.get("Last-Event-ID")
        try:
            last_revision = -1 if header is None else int(header)
        except ValueError as exc:
            raise ApiError(
                "invalid_request",
                "Last-Event-ID 必须是整数 revision",
                retryable=False,
                revision=self.server.device.revision,
            ) from exc
        once = query.get("once", ["false"])[0].lower() == "true"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close" if once else "keep-alive")
        self._cors_headers()
        self.end_headers()
        snapshot = self.server.device.status()
        current = snapshot["revision"]
        try:
            self._write_event(current, "snapshot", snapshot)
            last_revision = current
            if once:
                return
            while True:
                events = self.server.device.wait_events(last_revision, 15)
                if not events:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    continue
                for revision, event, data in events:
                    self._write_event(revision, event, data)
                    last_revision = revision
        except (BrokenPipeError, ConnectionResetError):
            return


def create_server(
    host: str,
    port: int,
    device: MockDevice | None = None,
    *,
    auto_transition: bool = True,
    allowed_origins: Iterable[str] = (),
) -> DeviceApiServer:
    return DeviceApiServer(
        (host, port),
        device or MockDevice(),
        auto_transition=auto_transition,
        allowed_origins=allowed_origins,
    )
