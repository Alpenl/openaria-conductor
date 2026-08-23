"""Privileged network-control request boundary.

This module is the socket-activated root-side boundary for future NetworkManager
mutation. It deliberately does not call the existing network writer yet; until
credential materialization and AP-first rollback are complete, newline-delimited
mutation requests fail closed after shape and secret-safety checks.
"""

from __future__ import annotations

import ipaddress
import json
import re
import socket
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any, TextIO

CONTROL_REQUEST_SCHEMA = "ylx.network-control-request.v1"
CONTROL_RESPONSE_SCHEMA = "ylx.network-control-response.v1"
MAX_CONTROL_REQUEST_BYTES = 64 * 1024
MAX_CONTROL_RESPONSE_BYTES = 64 * 1024
CONTROL_SOCKET_PATH = Path("/run/rp-ylx/network-control.sock")
CONTROL_SOCKET_TIMEOUT_SECONDS = 2.0
NETWORK_MODES = frozenset({"hotspot", "wifi-client", "ethernet-dhcp", "ethernet-static"})
MUTATION_OPERATIONS = frozenset({"apply", "retry", "forget"})
SUPPORTED_OPERATIONS = MUTATION_OPERATIONS | {"health"}
SECRET_FIELD_NAMES = frozenset({"password", "psk", "secret", "token"})
NETWORK_CREDENTIAL_REF = re.compile(r"^cred-[A-Za-z0-9_.:-]+$")
UUID_V7 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


class NetworkControlClientError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


def _error_response(
    code: str,
    message: str,
    *,
    operation: str | None = None,
    retryable: bool = False,
) -> dict[str, Any]:
    response: dict[str, Any] = {
        "schema": CONTROL_RESPONSE_SCHEMA,
        "ok": False,
        "error": {
            "code": code,
            "message": message,
        },
        "retryable": retryable,
    }
    if operation is not None:
        response["operation"] = operation
    return response


def _contains_inline_secret(value: object) -> bool:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if isinstance(key, str) and key.casefold() in SECRET_FIELD_NAMES:
                return True
            if _contains_inline_secret(item):
                return True
    elif isinstance(value, list):
        return any(_contains_inline_secret(item) for item in value)
    return False


def _valid_idempotency_key(value: object) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value) <= 128
        and all(character != " " and 0x21 <= ord(character) <= 0x7E for character in value)
    )


def _valid_principal_id(value: object) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value) <= 128
        and all(0x21 <= ord(character) <= 0x7E for character in value)
    )


def _valid_ipv4(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        ipaddress.IPv4Address(value)
    except ValueError:
        return False
    return True


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
        _valid_ipv4(value.get("address"))
        and type(value.get("prefix_length")) is int
        and 1 <= value["prefix_length"] <= 32
        and (value.get("gateway") is None or _valid_ipv4(value.get("gateway")))
        and isinstance(dns, list)
        and len(dns) <= 3
        and all(_valid_ipv4(item) for item in dns)
        and len(set(dns)) == len(dns)
    )


def _valid_network_ethernet(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {"addressing", "static_ipv4"}:
        return False
    addressing = value.get("addressing")
    static = value.get("static_ipv4")
    return (addressing == "dhcp" and static is None) or (
        addressing == "static" and _valid_network_static_ipv4(static)
    )


def _valid_network_apply_wifi_client(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {"ssid", "credential_ref"}:
        return False
    ssid = value.get("ssid")
    credential_ref = value.get("credential_ref")
    return (
        isinstance(ssid, str)
        and 1 <= len(ssid.encode("utf-8")) <= 32
        and isinstance(credential_ref, str)
        and 1 <= len(credential_ref) <= 128
        and NETWORK_CREDENTIAL_REF.fullmatch(credential_ref) is not None
    )


def _valid_network_apply_desired_state(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {"mode", "wifi_client", "ethernet"}:
        return False
    mode = value.get("mode")
    wifi = value.get("wifi_client")
    ethernet = value.get("ethernet")
    if mode not in NETWORK_MODES:
        return False
    if mode == "wifi-client":
        if not _valid_network_apply_wifi_client(wifi):
            return False
    elif wifi is not None:
        return False
    if ethernet is not None and not _valid_network_ethernet(ethernet):
        return False
    return not (mode == "ethernet-static" and ethernet is None)


def _valid_network_body(operation: str, body: object) -> bool:
    if not isinstance(body, Mapping):
        return False
    if operation == "apply":
        return (
            set(body) == {"schema", "desired"}
            and body.get("schema") == "ylx.network-apply-request.v1"
            and _valid_network_apply_desired_state(body.get("desired"))
        )
    if operation == "retry":
        return (
            set(body) == {"schema", "transaction_id"}
            and body.get("schema") == "ylx.network-retry-request.v1"
            and isinstance(body.get("transaction_id"), str)
            and UUID_V7.fullmatch(str(body["transaction_id"])) is not None
        )
    return set(body) == {"schema"} and body.get("schema") == "ylx.network-forget-request.v1"


def _valid_control_request(operation: str, request: Mapping[str, object]) -> bool:
    if operation == "health":
        return set(request) == {"schema", "operation"}
    return (
        set(request) == {"schema", "operation", "principal_id", "idempotency_key", "body"}
        and _valid_principal_id(request.get("principal_id"))
        and _valid_idempotency_key(request.get("idempotency_key"))
        and _valid_network_body(operation, request.get("body"))
    )


def handle_control_request(request: object) -> dict[str, Any]:
    if not isinstance(request, Mapping):
        return _error_response("request_invalid", "request must be a JSON object")
    if request.get("schema") != CONTROL_REQUEST_SCHEMA:
        return _error_response("request_invalid", "unsupported network-control request schema")
    operation = request.get("operation")
    if operation not in SUPPORTED_OPERATIONS:
        return _error_response("operation_invalid", "unsupported network-control operation")
    if operation in MUTATION_OPERATIONS and _contains_inline_secret(request):
        return _error_response(
            "inline_secret_rejected",
            "network-control requests must use credential_ref, not inline secret material",
            operation=str(operation),
        )
    if not _valid_control_request(str(operation), request):
        return _error_response("request_invalid", "network-control request shape is invalid")
    if operation == "health":
        return {
            "schema": CONTROL_RESPONSE_SCHEMA,
            "ok": True,
            "operation": "health",
            "capabilities": {
                "mutation_enabled": False,
                "operations": sorted(MUTATION_OPERATIONS),
                "secret_handling": "opaque_credential_reference_only",
            },
        }
    return _error_response(
        "network_controller_not_enabled",
        "network mutation controller is staged but not enabled",
        operation=str(operation),
        retryable=True,
    )


def handle_control_payload(payload: str) -> dict[str, Any]:
    try:
        size = len(payload.encode("utf-8"))
    except UnicodeError:
        return _error_response("request_invalid", "request must be valid UTF-8 JSON")
    if size > MAX_CONTROL_REQUEST_BYTES:
        return _error_response("request_too_large", "network-control request is too large")
    try:
        request = json.loads(payload)
    except json.JSONDecodeError:
        return _error_response("request_invalid", "request must be valid JSON")
    return handle_control_request(request)


def serve_stdio(*, stdin: TextIO | None = None, stdout: TextIO | None = None) -> int:
    if stdin is None:
        stdin = sys.stdin
    if stdout is None:
        stdout = sys.stdout
    payload = stdin.readline(MAX_CONTROL_REQUEST_BYTES + 1)
    response = handle_control_payload(payload)
    stdout.write(json.dumps(response, ensure_ascii=False, sort_keys=True) + "\n")
    stdout.flush()
    return 0


def _read_socket_response(client: socket.socket) -> bytes:
    chunks: list[bytes] = []
    size = 0
    while True:
        chunk = client.recv(4096)
        if not chunk:
            break
        chunks.append(chunk)
        size += len(chunk)
        if size > MAX_CONTROL_RESPONSE_BYTES:
            raise NetworkControlClientError(
                "response_too_large",
                "network-control response exceeded the size limit",
            )
        if b"\n" in chunk:
            break
    line, _, _ = b"".join(chunks).partition(b"\n")
    return line


def request_control(
    operation: str,
    *,
    principal_id: str | None = None,
    idempotency_key: str | None = None,
    body: Mapping[str, object] | None = None,
    socket_path: Path = CONTROL_SOCKET_PATH,
    timeout_seconds: float = CONTROL_SOCKET_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    request: dict[str, object] = {
        "schema": CONTROL_REQUEST_SCHEMA,
        "operation": operation,
    }
    if operation in MUTATION_OPERATIONS:
        if principal_id is None or idempotency_key is None or body is None:
            raise NetworkControlClientError(
                "request_invalid",
                "network-control mutation request is incomplete",
            )
        request.update(
            {
                "principal_id": principal_id,
                "idempotency_key": idempotency_key,
                "body": dict(body),
            }
        )
    payload = json.dumps(request, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(timeout_seconds)
            client.connect(str(socket_path))
            client.sendall(payload.encode("utf-8") + b"\n")
            client.shutdown(socket.SHUT_WR)
            rendered = _read_socket_response(client)
    except OSError as exc:
        raise NetworkControlClientError(
            "network_controller_unavailable",
            "network-control socket is unavailable",
        ) from exc
    try:
        response = json.loads(rendered.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NetworkControlClientError(
            "response_invalid",
            "network-control response is not valid JSON",
        ) from exc
    if not isinstance(response, dict):
        raise NetworkControlClientError(
            "response_invalid",
            "network-control response must be a JSON object",
        )
    if response.get("schema") != CONTROL_RESPONSE_SCHEMA:
        raise NetworkControlClientError(
            "response_invalid",
            "network-control response schema is invalid",
        )
    return response
