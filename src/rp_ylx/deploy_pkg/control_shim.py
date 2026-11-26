"""Fail-closed network-control socket compatibility launcher."""

from __future__ import annotations

import json
import os
import socket
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

from .schema import (
    NETWORK_CONTROL_MAX_REQUEST_BYTES,
    NETWORK_CONTROL_PROTOCOL,
    DeploymentError,
    _current_release_metadata,
)
from .systemd import _notify_systemd_ready


def _control_compatibility_error(payload: bytes) -> bytes:
    operation: str | None = None
    if len(payload) <= NETWORK_CONTROL_MAX_REQUEST_BYTES:
        try:
            request = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            request = None
        if isinstance(request, Mapping) and isinstance(request.get("operation"), str):
            operation = str(request["operation"])
    response: dict[str, object] = {
        "schema": "ylx.network-control-response.v1",
        "ok": False,
        "error": {
            "code": "network_controller_release_incompatible",
            "message": "the active rollback release does not support network mutation",
        },
        "retryable": False,
    }
    if operation is not None:
        response["operation"] = operation
    return (
        json.dumps(response, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _read_control_compatibility_request(connection: socket.socket) -> bytes:
    payload = bytearray()
    deadline = time.monotonic() + 2.0
    while len(payload) <= NETWORK_CONTROL_MAX_REQUEST_BYTES:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("network-control compatibility request timed out")
        connection.settimeout(remaining)
        block = connection.recv(4096)
        if not block:
            break
        payload.extend(block)
        newline = payload.find(b"\n")
        if newline >= 0:
            return bytes(payload[:newline])
    return bytes(payload)


def serve_network_control_launcher(
    install_root: Path = Path("/opt/rp-ylx"),
    *,
    listener: socket.socket | None = None,
    executor: Callable[[str, Sequence[str]], object] = os.execv,
    notifier: Callable[[str], None] = _notify_systemd_ready,
    max_connections: int | None = None,
) -> int:
    """Run the current controller protocol or a persistent fail-closed rollback shim."""
    release, metadata = _current_release_metadata(install_root)
    executable = release / "bin/rp-ylx"
    if metadata.get("network_control_protocol") == NETWORK_CONTROL_PROTOCOL:
        if not executable.is_file() or not os.access(executable, os.X_OK):
            raise DeploymentError("release_incomplete", "network-control executable 不可执行")
        executor(
            str(executable),
            [str(executable), "network-control", "serve", "--stdio"],
        )
        return 0

    owns_listener = listener is None
    if listener is None:
        try:
            listener = socket.fromfd(sys.stdin.fileno(), socket.AF_UNIX, socket.SOCK_STREAM)
        except (AttributeError, OSError, ValueError) as error:
            raise DeploymentError(
                "socket_activation_invalid",
                "network-control launcher 没有收到监听 socket",
            ) from error
    try:
        if listener.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN) != 1:
            raise DeploymentError(
                "socket_activation_invalid",
                "network-control launcher 收到的不是监听 socket",
            )
        notifier("rollback release active; network mutation is fail-closed")
        served = 0
        while max_connections is None or served < max_connections:
            connection, _ = listener.accept()
            with connection:
                try:
                    payload = _read_control_compatibility_request(connection)
                    connection.sendall(_control_compatibility_error(payload))
                except (OSError, TimeoutError):
                    pass
            served += 1
        return 0
    finally:
        if owns_listener:
            listener.close()
