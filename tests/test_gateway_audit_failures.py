"""Audit I/O failures must not make completed mutations ambiguous."""

import http.client
import json
import socket
import sys
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from rp_ylx.api import CaptureCommandResult
from rp_ylx.api.gateway import GatewayHandler, create_gateway_server
from rp_ylx.api.security import Principal, SecurityPolicy

SID = "01989f69-0000-7000-8000-000000000001"
DELETE = {
    "schema": "ylx.session-delete-request.v1",
    "sessions": [{"session_id": SID, "manifest_sha256": "a" * 64}],
}


class GatewayAuditFailuresTest(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.fail_outcomes = {"completed"}
        self.result = {"deleted_session_ids": [SID], "failed": []}
        self.delete = Mock(return_value=CaptureCommandResult(200, self.result, replayed=True))
        self.server = create_gateway_server(
            "127.0.0.1",
            0,
            SimpleNamespace(delete_sessions=self.delete),
            security=SecurityPolicy.lab(
                allowed_operations={"deleteSessions", "firmwareStatus", "firmwareUpdate"},
                allowed_origins={"http://console.test"},
                csrf_token="audit-csrf",
            ),
            audit_sink=self.sink,
        )
        self.errors = []
        # Keep the production filter for ordinary disconnects. Record only
        # errors that it delegates to the default server error handler.
        error_reporter = patch(
            "socketserver.BaseServer.handle_error",
            side_effect=lambda *_: self.errors.append(type(sys.exception()).__name__),
        )
        error_reporter.start()
        self.addCleanup(error_reporter.stop)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.client = http.client.HTTPConnection(*self.server.server_address, timeout=2)

    def tearDown(self):
        self.client.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)
        self.assertFalse(self.thread.is_alive())
        self.assertEqual(self.errors, [])

    def sink(self, event):
        self.events.append(event)
        if event.outcome in self.fail_outcomes:
            raise OSError("private failure detail must not leak")

    def request(self, method="POST", path="/api/v4/sessions/delete", body=DELETE, headers=None):
        merged = {"Content-Type": "application/json", "Idempotency-Key": "audit-test"}
        merged.update(headers or {})
        self.client.request(
            method, path, body=None if body is None else json.dumps(body), headers=merged
        )
        response = self.client.getresponse()
        payload = response.read()
        return response, payload

    def test_completed_delete_preserves_result_and_replay_header(self):
        response, payload = self.request()
        self.assertEqual(response.status, 200)
        self.assertEqual(json.loads(payload), self.result)
        self.assertEqual(response.getheader("Idempotency-Replayed"), "true")
        self.assertEqual(response.getheader("YLX-Audit-Status"), "unavailable")
        self.delete.assert_called_once()
        self.assertEqual([e.outcome for e in self.events], ["allowed", "completed"])

    def test_preoperation_failure_rejects_without_mutation_and_closes_unread_body(self):
        self.fail_outcomes = {"allowed"}
        response, payload = self.request()
        self.assertEqual(response.status, 503)
        self.assertEqual(response.getheader("Connection"), "close")
        self.assertEqual(response.getheader("Retry-After"), "1")
        self.assertEqual(json.loads(payload)["error"]["code"], "audit_unavailable")
        self.assertTrue(json.loads(payload)["error"]["retryable"])
        self.assertNotIn(b"private failure", payload)
        self.delete.assert_not_called()

    def test_keepalive_next_request_clears_warning_and_uses_fresh_request_id(self):
        first, _ = self.request()
        self.assertEqual(first.status, 200)
        connection = self.client.sock
        self.fail_outcomes.clear()
        second, _ = self.request()
        self.assertEqual(second.status, 200)
        self.assertIs(self.client.sock, connection)
        self.assertIsNone(second.getheader("YLX-Audit-Status"))
        self.assertNotEqual(self.events[0].request_id, self.events[2].request_id)
        self.assertEqual(self.delete.call_count, 2)

    def test_cors_exposes_audit_warning(self):
        response, _ = self.request(
            headers={"Origin": "http://console.test", "X-CSRF-Token": "audit-csrf"}
        )
        self.assertEqual(response.status, 200)
        self.assertIn("YLX-Audit-Status", response.getheader("Access-Control-Expose-Headers"))

    def test_unauthorized_keeps_auth_challenge_when_audit_fails(self):
        self.server.security = SecurityPolicy.customer(
            tokens={"test-token": Principal("reader", permissions={})}
        )
        self.fail_outcomes = {"unauthorized"}
        response, _ = self.request()
        self.assertEqual(response.status, 401)
        self.assertEqual(response.getheader("WWW-Authenticate"), "Bearer")
        self.assertEqual(response.getheader("YLX-Audit-Status"), "unavailable")
        self.delete.assert_not_called()

    def test_origin_and_permission_denials_stay_forbidden(self):
        for outcome, headers in [
            ("origin_forbidden", {"Origin": "http://evil.test"}),
            ("forbidden", {}),
        ]:
            with self.subTest(outcome=outcome):
                self.fail_outcomes = {outcome}
                if outcome == "forbidden":
                    self.server.security = SecurityPolicy.lab(allowed_operations=set())
                response, _ = self.request(headers=headers)
                self.assertEqual(response.status, 403)
                self.assertEqual(response.getheader("YLX-Audit-Status"), "unavailable")
        self.delete.assert_not_called()

    def test_head_audit_failure_has_empty_body_and_error_header(self):
        self.server.security = SecurityPolicy.lab(allowed_operations={"headSessionArtifact"})
        self.fail_outcomes = {"allowed"}
        response, payload = self.request(
            "HEAD", f"/api/v4/sessions/{SID}/artifacts/" + "a" * 64, None
        )
        self.assertEqual(response.status, 503)
        self.assertEqual(payload, b"")
        self.assertEqual(response.getheader("YLX-Error-Code"), "audit_unavailable")

    def test_firmware_completed_results_survive_audit_failure(self):
        for operation in ["status", "check", "update", "rollback"]:
            with (
                self.subTest(operation=operation),
                patch(
                    "rp_ylx.api.gateway.request_firmware", return_value={"task": "accepted"}
                ) as firmware,
            ):
                mutation = operation in {"update", "rollback"}
                response, payload = self.request(
                    "POST" if mutation else "GET",
                    "/api/v4/firmware" + ("" if operation == "status" else "/" + operation),
                    {"commit": "a" * 40} if mutation else None,
                )
                self.assertEqual(response.status, 202 if mutation else 200)
                self.assertEqual(json.loads(payload), {"task": "accepted"})
                self.assertEqual(response.getheader("YLX-Audit-Status"), "unavailable")
                firmware.assert_called_once()

    def test_firmware_preoperation_audit_failure_never_invokes_updater(self):
        self.fail_outcomes = {"allowed"}
        with patch("rp_ylx.api.gateway.request_firmware") as firmware:
            response, _ = self.request("POST", "/api/v4/firmware/update", {"commit": "a" * 40})
            self.assertEqual(response.status, 503)
            firmware.assert_not_called()

    def test_secondary_logger_failure_does_not_change_result(self):
        with patch("rp_ylx.api.gateway.operational_logger", side_effect=OSError("log full")):
            response, _ = self.request()
            self.assertEqual(response.status, 200)
            self.fail_outcomes = {"allowed"}
            response, _ = self.request()
            self.assertEqual(response.status, 503)

    def test_diagnostic_rate_limit_counts_suppressed_failures_without_secrets(self):
        with (
            patch("rp_ylx.api.gateway.time.monotonic", return_value=100) as now,
            patch("rp_ylx.api.gateway.operational_logger") as logger,
        ):
            for _ in range(100):
                self.server.report_audit_failure("deleteSessions", "completed", OSError("secret"))
            logger.return_value.event.assert_called_once()
            now.return_value = 130
            self.server.report_audit_failure("deleteSessions", "allowed", ValueError("secret"))
            self.assertEqual(logger.return_value.event.call_count, 2)
            fields = logger.return_value.event.call_args.kwargs
            self.assertEqual(fields["suppressed_count"], 99)
            self.assertEqual(fields["exception_type"], "ValueError")
            self.assertNotIn("secret", str(logger.mock_calls))

    def test_process_control_exceptions_propagate(self):
        handler = GatewayHandler.__new__(GatewayHandler)
        handler._request_id = "unit-only"
        handler.server = self.server
        for error in [KeyboardInterrupt(), SystemExit()]:
            with self.subTest(error=type(error).__name__):
                self.server.audit_sink = Mock(side_effect=error)
                with self.assertRaises(type(error)):
                    handler._audit("deleteSessions", None, "allowed")

    def test_read_failure_is_typed_and_does_not_invoke_provider(self):
        self.server.security = SecurityPolicy.lab(allowed_operations={"getDevice"})
        self.server.provider.device_descriptor = Mock()
        self.fail_outcomes = {"allowed"}
        response, payload = self.request("GET", "/api/v4/device", None)
        self.assertEqual(response.status, 503)
        self.assertEqual(json.loads(payload)["error"]["code"], "audit_unavailable")
        self.server.provider.device_descriptor.assert_not_called()

    def test_csrf_denial_survives_audit_failure(self):
        self.fail_outcomes = {"csrf_forbidden"}
        response, payload = self.request(headers={"Origin": "http://console.test"})
        self.assertEqual(response.status, 403)
        self.assertEqual(json.loads(payload)["error"]["code"], "csrf_forbidden")
        self.assertEqual(response.getheader("YLX-Audit-Status"), "unavailable")
        self.delete.assert_not_called()

    def test_client_disconnect_after_operation_releases_capacity_without_reexecution(self):
        entered = threading.Event()
        release = threading.Event()
        completed = threading.Event()
        errors = []
        self.server.handle_error = lambda *_: errors.append(type(sys.exception()))

        def operation(command):
            entered.set()
            if not release.wait(2):
                raise RuntimeError("test release was not signalled")
            completed.set()
            return CaptureCommandResult(200, self.result)

        self.delete.side_effect = operation
        try:
            self.client.request(
                "POST",
                "/api/v4/sessions/delete",
                json.dumps(DELETE),
                {"Content-Type": "application/json", "Idempotency-Key": "disconnect"},
            )
            self.assertTrue(entered.wait(2))
            self.client.sock.shutdown(socket.SHUT_RDWR)
            self.client.close()
        finally:
            release.set()
        self.assertTrue(completed.wait(2))
        with self.server._http_condition:
            self.assertTrue(
                self.server._http_condition.wait_for(
                    lambda: not self.server._http_connections, timeout=2
                )
            )
        self.delete.assert_called_once()
        self.assertEqual([e.outcome for e in self.events], ["allowed", "completed"])
        self.assertTrue(
            all(issubclass(error, (BrokenPipeError, ConnectionResetError)) for error in errors)
        )

    def test_error_observer_preserves_disconnect_filter_and_catches_unknown_errors(self):
        for error in [ConnectionResetError(), BrokenPipeError()]:
            try:
                raise error
            except OSError:
                self.server.handle_error(None, None)
        self.assertEqual(self.errors, [])
        try:
            raise ValueError("unexpected handler failure")
        except ValueError:
            self.server.handle_error(None, None)
        self.assertEqual(self.errors, ["ValueError"])
        self.errors.clear()
