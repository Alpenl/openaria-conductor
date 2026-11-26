"""Bounded NDJSON-over-Unix-socket RPC server and client with cumulative deadlines."""

from __future__ import annotations

import errno
import json
import math
import os
import selectors
import socket
import threading
import time
from collections.abc import Callable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import suppress
from pathlib import Path
from typing import Any, BinaryIO

from rp_ylx.core.fs import reject_nonfinite_constant, unique_object_pairs, validate_json_tree

__all__ = [
    "ControlRpcClient",
    "ControlRpcServer",
    "RpcError",
    "RpcFramingError",
    "RpcTimeoutError",
    "decode_ndjson_object",
    "encode_ndjson_object",
    "read_bounded_ndjson_stream",
    "read_ndjson_line",
    "write_all_with_deadline",
]


class RpcError(RuntimeError):
    """Base error for control-plane NDJSON RPC operations."""


class RpcFramingError(RpcError):
    """Raised when an NDJSON RPC frame violates size, UTF-8, newline, or JSON limits."""


class RpcTimeoutError(RpcError, TimeoutError):
    """Raised when a cumulative RPC read/write/connect deadline expires."""


def _validate_positive_finite(value: float, name: str) -> float:
    if type(value) not in {int, float} or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a finite positive number")
    return float(value)


def _validate_positive_int(value: int, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _read_ndjson_line(
    sock: socket.socket,
    *,
    max_bytes: int,
    deadline: float,
    allow_empty_eof: bool = False,
) -> bytes:
    raw = bytearray()
    while len(raw) <= max_bytes:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RpcTimeoutError("RPC read deadline expired")
        sock.settimeout(remaining)
        try:
            chunk = sock.recv(min(4096, max_bytes + 1 - len(raw)))
        except TimeoutError as error:
            raise RpcTimeoutError("RPC read timed out") from error
        if not chunk:
            if not raw and allow_empty_eof:
                return b""
            raise RpcFramingError("RPC stream closed before newline")
        newline = chunk.find(b"\n")
        if newline >= 0:
            raw.extend(chunk[: newline + 1])
            if len(raw) > max_bytes:
                raise RpcFramingError("NDJSON frame exceeded size limit")
            return bytes(raw)
        raw.extend(chunk)
        if len(raw) > max_bytes:
            raise RpcFramingError("NDJSON frame exceeded size limit")
    raise RpcFramingError("NDJSON frame exceeded size limit")


def _write_all_with_deadline(sock: socket.socket, data: bytes, *, deadline: float) -> None:
    view = memoryview(data)
    offset = 0
    while offset < len(view):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RpcTimeoutError("RPC write deadline expired")
        sock.settimeout(remaining)
        try:
            sent = sock.send(view[offset:])
        except TimeoutError as error:
            raise RpcTimeoutError("RPC write timed out") from error
        if sent <= 0:
            raise OSError("short write on RPC socket")
        offset += sent


def _decode_ndjson_object(
    raw: bytes,
    *,
    max_bytes: int,
    max_depth: int = 64,
) -> dict[str, Any]:
    if len(raw) > max_bytes or not raw.endswith(b"\n"):
        raise RpcFramingError("NDJSON frame must be newline-terminated and within size limit")
    if len(raw) <= 1:
        raise RpcFramingError("NDJSON frame cannot be empty")
    try:
        text = raw[:-1].decode("utf-8")
        value = json.loads(
            text,
            object_pairs_hook=unique_object_pairs,
            parse_constant=reject_nonfinite_constant,
        )
        validate_json_tree(value, max_depth=max_depth)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as error:
        raise RpcFramingError(f"invalid NDJSON payload: {error}") from error
    if not isinstance(value, dict):
        raise RpcFramingError("NDJSON frame must decode to a JSON object")
    return value


def _encode_ndjson_object(payload: Mapping[str, Any], *, max_bytes: int) -> bytes:
    if not isinstance(payload, Mapping):
        raise RpcFramingError("RPC payload must be a mapping")
    try:
        rendered = json.dumps(
            dict(payload),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise RpcFramingError(f"RPC payload is not serializable JSON: {error}") from error
    encoded = rendered.encode("utf-8") + b"\n"
    if len(encoded) > max_bytes:
        raise RpcFramingError("NDJSON frame exceeded size limit")
    return encoded


def read_bounded_ndjson_stream(
    stream: BinaryIO,
    *,
    max_bytes: int,
    max_depth: int = 64,
) -> dict[str, Any] | None:
    """Read one bounded NDJSON line from a binary stream without leaving trailing bytes."""
    _validate_positive_int(max_bytes, "max_bytes")
    line = stream.readline(max_bytes + 1)
    if line == b"":
        return None
    if not line.endswith(b"\n"):
        if len(line) > max_bytes:
            while True:
                discard = stream.readline(max_bytes + 1)
                if not discard or discard.endswith(b"\n"):
                    break
            raise RpcFramingError("NDJSON frame exceeded size limit")
        raise RpcFramingError("NDJSON frame missing trailing newline")
    return _decode_ndjson_object(line, max_bytes=max_bytes, max_depth=max_depth)


class ControlRpcServer:
    """Bounded concurrent NDJSON-over-Unix-socket server."""

    def __init__(
        self,
        socket_path: Path | str | None,
        handler: Callable[[dict[str, Any]], Mapping[str, Any]],
        *,
        max_workers: int = 8,
        max_pending: int | None = None,
        max_request_bytes: int = 64 * 1024,
        max_response_bytes: int = 64 * 1024,
        max_depth: int = 64,
        request_timeout_seconds: float = 10.0,
        read_timeout_seconds: float | None = None,
        max_connections: int | None = None,
        concurrent_predicate: Callable[[dict[str, Any]], bool] | None = None,
        socket_mode: int = 0o660,
        listener: socket.socket | None = None,
        error_response_factory: Callable[[Exception], Mapping[str, Any]] | None = None,
        post_write_hook: Callable[[Mapping[str, Any], bool], None] | None = None,
    ) -> None:
        self.max_workers = _validate_positive_int(max_workers, "max_workers")
        self.max_pending = (
            self.max_workers * 4
            if max_pending is None
            else _validate_positive_int(max_pending, "max_pending")
        )
        self.max_request_bytes = _validate_positive_int(max_request_bytes, "max_request_bytes")
        self.max_response_bytes = _validate_positive_int(max_response_bytes, "max_response_bytes")
        self.max_depth = _validate_positive_int(max_depth, "max_depth")
        self.request_timeout_seconds = _validate_positive_finite(
            request_timeout_seconds, "request_timeout_seconds"
        )
        self.read_timeout_seconds = (
            None
            if read_timeout_seconds is None
            else _validate_positive_finite(read_timeout_seconds, "read_timeout_seconds")
        )
        self.max_connections = (
            None
            if max_connections is None
            else _validate_positive_int(max_connections, "max_connections")
        )
        self.handler = handler
        self.concurrent_predicate = concurrent_predicate
        self.error_response_factory = error_response_factory
        self.post_write_hook = post_write_hook
        self.socket_path = Path(socket_path) if socket_path is not None else None
        self._owns_listener = listener is None
        self._owns_socket_path = listener is None and self.socket_path is not None

        if listener is not None:
            self._listener = listener
        else:
            if self.socket_path is None:
                raise ValueError("socket_path is required when listener is None")
            self.socket_path.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
            self.socket_path.unlink(missing_ok=True)
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.bind(str(self.socket_path))
                os.chmod(self.socket_path, socket_mode)
                sock.listen(min(128, max(16, self.max_pending)))
            except BaseException:
                sock.close()
                with suppress(OSError):
                    self.socket_path.unlink(missing_ok=True)
                raise
            self._listener = sock

        wakeup_r: int | None = None
        wakeup_w: int | None = None
        executor: ThreadPoolExecutor | None = None
        try:
            self._listener.setblocking(False)
            wakeup_r, wakeup_w = os.pipe()
            os.set_blocking(wakeup_r, False)
            os.set_blocking(wakeup_w, False)
            executor = ThreadPoolExecutor(
                max_workers=self.max_workers,
                thread_name_prefix="control-rpc",
            )
        except BaseException:
            if executor is not None:
                executor.shutdown(wait=False, cancel_futures=True)
            for fd in (wakeup_r, wakeup_w):
                if fd is not None:
                    with suppress(OSError):
                        os.close(fd)
            if self._owns_listener:
                with suppress(OSError):
                    self._listener.close()
            if self._owns_socket_path and self.socket_path is not None:
                with suppress(OSError):
                    self.socket_path.unlink(missing_ok=True)
            raise

        self._wakeup_r = wakeup_r
        self._wakeup_w = wakeup_w
        self._executor = executor
        self._mutation_lock = threading.Lock()
        self._state_lock = threading.Condition()
        self._in_flight: set[Future[None]] = set()
        self._closing = False
        self._closed = False
        self._resources_closed = False
        self._serve_active = False
        self._serve_done = threading.Event()
        self._serve_done.set()
        self._serve_thread: threading.Thread | None = None
        self._connections_accepted = 0

    def _close_resources_locked(self) -> None:
        if self._resources_closed:
            return
        self._resources_closed = True
        for fd in (self._wakeup_r, self._wakeup_w):
            with suppress(OSError):
                os.close(fd)
        if self._owns_listener:
            with suppress(OSError):
                self._listener.close()
        if self._owns_socket_path and self.socket_path is not None:
            with suppress(OSError):
                self.socket_path.unlink(missing_ok=True)

    def _default_error_response(self, error: Exception) -> Mapping[str, Any]:
        if self.error_response_factory is not None:
            return self.error_response_factory(error)
        code = "invalid_request" if isinstance(error, RpcFramingError) else "internal_error"
        return {"ok": False, "error": {"code": code, "message": str(error) or code}}

    def _reject_busy(self, conn: socket.socket) -> None:
        try:
            payload = _encode_ndjson_object(
                {
                    "ok": False,
                    "error": {"code": "service_busy", "message": "RPC server is busy"},
                },
                max_bytes=self.max_response_bytes,
            )
            _write_all_with_deadline(conn, payload, deadline=time.monotonic() + 0.1)
        except (OSError, RpcFramingError, RpcTimeoutError):
            pass
        finally:
            with suppress(OSError):
                conn.close()

    def _handle_connection(self, conn: socket.socket, deadline: float) -> None:
        read_deadline = (
            deadline
            if self.read_timeout_seconds is None
            else min(deadline, time.monotonic() + self.read_timeout_seconds)
        )
        try:
            with conn:
                response: Mapping[str, Any]
                try:
                    raw = _read_ndjson_line(
                        conn,
                        max_bytes=self.max_request_bytes,
                        deadline=read_deadline,
                        allow_empty_eof=True,
                    )
                except RpcFramingError as error:
                    response = self._default_error_response(error)
                else:
                    if not raw:
                        return
                    try:
                        request = _decode_ndjson_object(
                            raw,
                            max_bytes=self.max_request_bytes,
                            max_depth=self.max_depth,
                        )
                        is_concurrent = self.concurrent_predicate is not None and bool(
                            self.concurrent_predicate(request)
                        )
                        if is_concurrent:
                            response = self.handler(request)
                        else:
                            with self._mutation_lock:
                                response = self.handler(request)
                    except Exception as error:
                        response = self._default_error_response(error)

                try:
                    encoded = _encode_ndjson_object(response, max_bytes=self.max_response_bytes)
                except RpcFramingError as error:
                    try:
                        response = self._default_error_response(error)
                        encoded = _encode_ndjson_object(
                            response,
                            max_bytes=self.max_response_bytes,
                        )
                    except RpcFramingError:
                        return

                wrote = False
                try:
                    _write_all_with_deadline(conn, encoded, deadline=deadline)
                    wrote = True
                finally:
                    if self.post_write_hook is not None:
                        self.post_write_hook(response, wrote)
        except (RpcTimeoutError, OSError):
            return

    def _submit_connection(self, conn: socket.socket) -> None:
        deadline = time.monotonic() + self.request_timeout_seconds
        with self._state_lock:
            if self._closing or len(self._in_flight) >= self.max_pending:
                reject = True
            else:
                reject = False
                future = self._executor.submit(self._handle_connection, conn, deadline)
                self._in_flight.add(future)
                future.add_done_callback(self._on_future_done)
        if reject:
            self._reject_busy(conn)

    def _on_future_done(self, future: Future[None]) -> None:
        with self._state_lock:
            self._in_flight.discard(future)
            self._state_lock.notify_all()

    def serve_forever(self) -> None:
        """Accept Unix socket connections until shutdown() is called."""
        with self._state_lock:
            if self._closing or self._closed or self._resources_closed:
                return
            self._serve_active = True
            self._serve_done.clear()

        selector = selectors.DefaultSelector()
        selector.register(self._listener, selectors.EVENT_READ, data="accept")
        selector.register(self._wakeup_r, selectors.EVENT_READ, data="wakeup")
        try:
            while self.max_connections is None or self._connections_accepted < self.max_connections:
                with self._state_lock:
                    if self._closing:
                        break
                events = selector.select()
                for key, _ in events:
                    if key.data == "wakeup":
                        with suppress(OSError):
                            os.read(self._wakeup_r, 1024)
                        return
                    while (
                        self.max_connections is None
                        or self._connections_accepted < self.max_connections
                    ):
                        with self._state_lock:
                            if self._closing:
                                return
                        try:
                            conn, _ = self._listener.accept()
                        except BlockingIOError:
                            break
                        except (ConnectionAbortedError, InterruptedError):
                            continue
                        except OSError as error:
                            if error.errno in {errno.EMFILE, errno.ENFILE}:
                                time.sleep(0.01)
                                break
                            with self._state_lock:
                                if self._closing:
                                    return
                            return
                        self._connections_accepted += 1
                        self._submit_connection(conn)
        finally:
            selector.close()
            with self._state_lock:
                self._serve_active = False
                self._serve_done.set()
                if self._closed:
                    self._close_resources_locked()

    def start(self) -> ControlRpcServer:
        """Start serving in a background daemon thread."""
        with self._state_lock:
            if self._closing or self._closed:
                raise RuntimeError("cannot start a closed ControlRpcServer")
            if self._serve_thread is not None:
                raise RuntimeError("ControlRpcServer already started")
            thread = threading.Thread(
                target=self.serve_forever,
                name="control-rpc-accept",
                daemon=True,
            )
            self._serve_thread = thread
            self._serve_active = True
            self._serve_done.clear()
        try:
            thread.start()
        except BaseException:
            with self._state_lock:
                self._serve_thread = None
                self._serve_active = False
                self._serve_done.set()
            raise
        return self

    def shutdown(self, timeout: float = 5.0) -> None:
        """Stop accepting new connections and drain active workers up to timeout seconds."""
        timeout = max(0.0, float(timeout))
        deadline = time.monotonic() + timeout
        with self._state_lock:
            if self._closed:
                return
            self._closing = True
            serve_thread = self._serve_thread
            serve_active = self._serve_active
            wakeup_w = None if self._resources_closed else self._wakeup_w
        if wakeup_w is not None:
            with suppress(OSError):
                os.write(wakeup_w, b"x")
        if serve_thread is not None and serve_thread is not threading.current_thread():
            serve_thread.join(timeout=max(0.0, deadline - time.monotonic()))
        elif serve_thread is None and serve_active:
            self._serve_done.wait(timeout=max(0.0, deadline - time.monotonic()))
        with self._state_lock:
            while self._in_flight:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._state_lock.wait(timeout=remaining)
            self._closed = True
            if not self._serve_active:
                self._close_resources_locked()
        self._executor.shutdown(wait=False, cancel_futures=True)

    def __enter__(self) -> ControlRpcServer:
        return self

    def __exit__(self, *_args: object) -> None:
        self.shutdown()


class ControlRpcClient:
    """Bounded NDJSON-over-Unix-socket client enforcing a cumulative request deadline."""

    def __init__(
        self,
        *,
        max_request_bytes: int = 64 * 1024,
        max_response_bytes: int = 64 * 1024,
        max_depth: int = 64,
        default_timeout_seconds: float = 10.0,
    ) -> None:
        self.max_request_bytes = _validate_positive_int(max_request_bytes, "max_request_bytes")
        self.max_response_bytes = _validate_positive_int(max_response_bytes, "max_response_bytes")
        self.max_depth = _validate_positive_int(max_depth, "max_depth")
        self.default_timeout_seconds = _validate_positive_finite(
            default_timeout_seconds, "default_timeout_seconds"
        )

    def request(
        self,
        socket_path: Path | str,
        payload: Mapping[str, Any],
        *,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Send one JSON request line to socket_path and decode one JSON response line."""
        effective_timeout = (
            self.default_timeout_seconds
            if timeout_seconds is None
            else _validate_positive_finite(timeout_seconds, "timeout_seconds")
        )
        encoded = _encode_ndjson_object(payload, max_bytes=self.max_request_bytes)
        deadline = time.monotonic() + effective_timeout

        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RpcTimeoutError("RPC connect deadline expired")
            client.settimeout(remaining)
            try:
                client.connect(str(socket_path))
            except TimeoutError as error:
                raise RpcTimeoutError("RPC connect timed out") from error

            write_error: OSError | None = None
            try:
                _write_all_with_deadline(client, encoded, deadline=deadline)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as error:
                # Preserve a refusal frame queued by a busy server before closing.
                write_error = error
            else:
                with suppress(OSError):
                    client.shutdown(socket.SHUT_WR)

            try:
                raw = _read_ndjson_line(
                    client,
                    max_bytes=self.max_response_bytes,
                    deadline=deadline,
                )
            except RpcFramingError:
                if write_error is not None:
                    raise write_error from None
                raise

        return decode_ndjson_object(
            raw,
            max_bytes=self.max_response_bytes,
            max_depth=self.max_depth,
        )


read_ndjson_line = _read_ndjson_line
write_all_with_deadline = _write_all_with_deadline
decode_ndjson_object = _decode_ndjson_object
encode_ndjson_object = _encode_ndjson_object
