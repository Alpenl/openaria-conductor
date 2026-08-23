from __future__ import annotations

import io
import json
import socket
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from http import HTTPStatus
from pathlib import Path
from unittest.mock import patch

from rp_ylx.api.gateway import NetworkCommand, ProviderError
from rp_ylx.cli import main
from rp_ylx.network_control import (
    CONTROL_RESPONSE_SCHEMA,
    NetworkControlClientError,
    handle_control_payload,
    request_control,
    serve_stdio,
)
from rp_ylx.recording.coordinator import CaptureCoordinator


class NetworkControlTest(unittest.TestCase):
    def _serve_once(self, socket_path: Path, response: bytes | None = None) -> threading.Thread:
        ready = threading.Event()

        def run() -> None:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
                server.bind(str(socket_path))
                server.listen(1)
                ready.set()
                connection, _ = server.accept()
                with connection:
                    payload = b""
                    while not payload.endswith(b"\n"):
                        chunk = connection.recv(4096)
                        if not chunk:
                            break
                        payload += chunk
                    rendered = (
                        json.dumps(handle_control_payload(payload.decode("utf-8"))).encode() + b"\n"
                        if response is None
                        else response
                    )
                    connection.sendall(rendered)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        self.assertTrue(ready.wait(timeout=2))
        return thread

    def test_health_reports_mutation_disabled(self) -> None:
        response = handle_control_payload(
            json.dumps(
                {
                    "schema": "ylx.network-control-request.v1",
                    "operation": "health",
                }
            )
        )

        self.assertEqual(response["schema"], CONTROL_RESPONSE_SCHEMA)
        self.assertIs(response["ok"], True)
        self.assertEqual(response["operation"], "health")
        capabilities = response["capabilities"]
        self.assertEqual(capabilities["operations"], ["apply", "forget", "retry"])
        self.assertIs(capabilities["mutation_enabled"], False)
        self.assertEqual(
            capabilities["secret_handling"],
            "opaque_credential_reference_only",
        )

    def test_mutation_request_fails_closed_without_echoing_body(self) -> None:
        response = handle_control_payload(
            json.dumps(
                {
                    "schema": "ylx.network-control-request.v1",
                    "operation": "apply",
                    "principal_id": "customer",
                    "idempotency_key": "idem-1",
                    "body": {
                        "schema": "ylx.network-apply-request.v1",
                        "desired": {
                            "mode": "wifi-client",
                            "wifi_client": {
                                "ssid": "Field LAN",
                                "credential_ref": "cred-network-field-lan",
                            },
                            "ethernet": None,
                        },
                    },
                }
            )
        )

        self.assertIs(response["ok"], False)
        self.assertEqual(response["operation"], "apply")
        self.assertEqual(response["error"]["code"], "network_controller_not_enabled")
        self.assertIs(response["retryable"], True)
        self.assertNotIn("Field LAN", json.dumps(response))
        self.assertNotIn("credential_ref", json.dumps(response))

    def test_inline_secret_is_rejected_before_not_enabled_response(self) -> None:
        response = handle_control_payload(
            json.dumps(
                {
                    "schema": "ylx.network-control-request.v1",
                    "operation": "apply",
                    "principal_id": "customer",
                    "idempotency_key": "idem-1",
                    "body": {
                        "schema": "ylx.network-apply-request.v1",
                        "desired": {
                            "mode": "wifi-client",
                            "wifi_client": {"ssid": "Field LAN", "psk": "candidate-secret"},
                            "ethernet": None,
                        },
                    },
                }
            )
        )

        self.assertIs(response["ok"], False)
        self.assertEqual(response["error"]["code"], "inline_secret_rejected")
        self.assertNotIn("candidate-secret", json.dumps(response))

    def test_invalid_payloads_return_structured_error(self) -> None:
        response = handle_control_payload("{not json")

        self.assertEqual(response["schema"], CONTROL_RESPONSE_SCHEMA)
        self.assertIs(response["ok"], False)
        self.assertEqual(response["error"]["code"], "request_invalid")

    def test_health_rejects_extra_fields(self) -> None:
        response = handle_control_payload(
            json.dumps(
                {
                    "schema": "ylx.network-control-request.v1",
                    "operation": "health",
                    "body": {},
                }
            )
        )

        self.assertIs(response["ok"], False)
        self.assertEqual(response["error"]["code"], "request_invalid")

    def test_invalid_mutation_shape_fails_before_not_enabled(self) -> None:
        response = handle_control_payload(
            json.dumps(
                {
                    "schema": "ylx.network-control-request.v1",
                    "operation": "apply",
                    "principal_id": "customer",
                    "idempotency_key": "idem-1",
                    "body": {
                        "schema": "ylx.network-apply-request.v1",
                        "desired": {
                            "mode": "wifi-client",
                            "wifi_client": {
                                "ssid": "Field LAN",
                                "credential_ref": "cred/network/field-lan",
                            },
                            "ethernet": None,
                        },
                    },
                }
            )
        )

        self.assertIs(response["ok"], False)
        self.assertEqual(response["error"]["code"], "request_invalid")

    def test_stdio_server_handles_one_request(self) -> None:
        stdout = io.StringIO()
        code = serve_stdio(
            stdin=io.StringIO(
                json.dumps(
                    {
                        "schema": "ylx.network-control-request.v1",
                        "operation": "health",
                    }
                )
            ),
            stdout=stdout,
        )

        self.assertEqual(code, 0)
        response = json.loads(stdout.getvalue())
        self.assertIs(response["ok"], True)

    def test_cli_network_control_serve_stdio(self) -> None:
        output = io.StringIO()
        with (
            patch(
                "sys.stdin",
                io.StringIO(
                    json.dumps(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "health",
                        }
                    )
                ),
            ),
            redirect_stdout(output),
        ):
            code = main(["network-control", "serve", "--stdio"])

        self.assertEqual(code, 0)
        response = json.loads(output.getvalue())
        self.assertIs(response["ok"], True)

    def test_client_round_trips_one_socket_request(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            socket_path = Path(directory) / "network-control.sock"
            thread = self._serve_once(socket_path)
            response = request_control(
                "apply",
                principal_id="customer",
                idempotency_key="idem-1",
                body={
                    "schema": "ylx.network-apply-request.v1",
                    "desired": {
                        "mode": "wifi-client",
                        "wifi_client": {
                            "ssid": "Field LAN",
                            "credential_ref": "cred-network-field-lan",
                        },
                        "ethernet": None,
                    },
                },
                socket_path=socket_path,
            )
            thread.join(timeout=2)

        self.assertFalse(response["ok"])
        self.assertEqual(response["operation"], "apply")
        self.assertEqual(response["error"]["code"], "network_controller_not_enabled")

    def test_client_reports_unavailable_socket(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            socket_path = Path(directory) / "missing.sock"
            with self.assertRaises(NetworkControlClientError) as raised:
                request_control(
                    "health",
                    socket_path=socket_path,
                    timeout_seconds=0.1,
                )

        self.assertEqual(raised.exception.code, "network_controller_unavailable")

    def test_client_rejects_invalid_socket_response(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            socket_path = Path(directory) / "network-control.sock"
            thread = self._serve_once(socket_path, response=b"not-json\n")
            with self.assertRaises(NetworkControlClientError) as raised:
                request_control(
                    "health",
                    socket_path=socket_path,
                    timeout_seconds=0.5,
                )
            thread.join(timeout=2)

        self.assertEqual(raised.exception.code, "response_invalid")

    def test_coordinator_forwards_mutation_to_root_controller_and_fails_closed(self) -> None:
        command = NetworkCommand(
            "customer",
            "idem-1",
            {
                "schema": "ylx.network-forget-request.v1",
            },
            b'{"schema":"ylx.network-forget-request.v1"}',
        )
        provider = object.__new__(CaptureCoordinator)

        with (
            patch(
                "rp_ylx.recording.coordinator.request_network_control",
                return_value={
                    "schema": CONTROL_RESPONSE_SCHEMA,
                    "ok": False,
                    "operation": "forget",
                    "error": {
                        "code": "network_controller_not_enabled",
                        "message": "staged but disabled",
                    },
                    "retryable": True,
                },
            ) as request,
            self.assertRaises(ProviderError) as raised,
        ):
            provider.forget_network_client_profile(command)

        self.assertEqual(raised.exception.code, "network_mutation_unavailable")
        self.assertEqual(raised.exception.status, HTTPStatus.SERVICE_UNAVAILABLE)
        self.assertTrue(raised.exception.retryable)
        request.assert_called_once_with(
            "forget",
            principal_id="customer",
            idempotency_key="idem-1",
            body=command.body,
        )

    def test_coordinator_maps_root_controller_transport_failure_to_fail_closed(self) -> None:
        command = NetworkCommand(
            "customer",
            "idem-1",
            {
                "schema": "ylx.network-retry-request.v1",
                "transaction_id": "0198d2a0-41a0-7b7a-a751-0e86a39d4db1",
            },
            b"{}",
        )
        provider = object.__new__(CaptureCoordinator)

        with (
            patch(
                "rp_ylx.recording.coordinator.request_network_control",
                side_effect=NetworkControlClientError(
                    "network_controller_unavailable",
                    "network-control socket is unavailable",
                ),
            ),
            self.assertRaises(ProviderError) as raised,
        ):
            provider.retry_network_transaction(command)

        self.assertEqual(raised.exception.code, "network_mutation_unavailable")
        self.assertEqual(raised.exception.status, HTTPStatus.SERVICE_UNAVAILABLE)
        self.assertTrue(raised.exception.retryable)

    def test_coordinator_maps_enabled_root_controller_receipt(self) -> None:
        receipt = {
            "schema": "ylx.network-command-receipt.v1",
            "transaction_id": "0198d2a0-41a0-7b7a-a751-0e86a39d4db1",
            "operation": "apply",
            "accepted_at": "2026-08-23T15:00:00+08:00",
            "status": "accepted",
        }
        command = NetworkCommand(
            "customer",
            "idem-1",
            {
                "schema": "ylx.network-apply-request.v1",
                "desired": {
                    "mode": "hotspot",
                    "wifi_client": None,
                    "ethernet": None,
                },
            },
            b"{}",
        )
        provider = object.__new__(CaptureCoordinator)

        with patch(
            "rp_ylx.recording.coordinator.request_network_control",
            return_value={
                "schema": CONTROL_RESPONSE_SCHEMA,
                "ok": True,
                "operation": "apply",
                "status": 202,
                "body": receipt,
                "replayed": True,
            },
        ):
            result = provider.apply_network_desired_state(command)

        self.assertEqual(result.status, HTTPStatus.ACCEPTED)
        self.assertEqual(result.body, receipt)
        self.assertTrue(result.replayed)


if __name__ == "__main__":
    unittest.main()
