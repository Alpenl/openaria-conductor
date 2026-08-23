from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from rp_ylx.cli import main
from rp_ylx.network_control import CONTROL_RESPONSE_SCHEMA, handle_control_payload, serve_stdio


class NetworkControlTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
