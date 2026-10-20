from __future__ import annotations

import http.client
import json
import socket
import socketserver
import tempfile
import threading
import unittest
from contextlib import contextmanager, suppress
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from rp_ylx import firmware_control as control
from rp_ylx.api.gateway import create_gateway_server
from rp_ylx.api.security import SecurityPolicy


class ReplyHandler(socketserver.StreamRequestHandler):
    def handle(self):
        self.connection.settimeout(2)
        self.rfile.readline(control.LIMIT + 1)
        with suppress(BrokenPipeError, ConnectionResetError):
            self.wfile.write(self.server.reply)


@contextmanager
def running_server(server):
    errors = []
    server.handle_error = lambda *_: errors.append("handler error")
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
    thread.start()
    try:
        yield errors
    finally:
        server.shutdown()
        server.server_close()
        thread.join(3)
        if thread.is_alive() or errors:
            raise AssertionError((thread.is_alive(), errors))


class FirmwareControlResponseTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = str(Path(directory.name) / "control.sock")
        self.server = control.ControlServer(self.path, ReplyHandler)
        self.context = running_server(self.server)
        self.context.__enter__()
        self.addCleanup(self.context.__exit__, None, None, None)
        p = patch.object(control, "SOCKET", self.path)
        p.start()
        self.addCleanup(p.stop)

    def reply(self, value):
        self.server.reply = json.dumps(value).encode() + b"\n"

    def reject_raw(self, raw):
        self.server.reply = raw
        with self.assertRaises(control.FirmwareError) as raised:
            control.request_firmware("status")
        self.assertEqual(raised.exception.code, "update_service_unavailable")
        self.assertEqual(raised.exception.status, 503)
        self.assertNotIn("private-marker", raised.exception.message)

    def reject(self, value):
        self.reject_raw(json.dumps(value).encode() + b"\n")

    def test_valid_success_preserves_unicode_and_nested_business_values(self):
        body = {"message": "更新完成😀", "task": None, "values": [1, True, 1.5, {"x": "y"}]}
        self.reply({"ok": True, "body": body})
        self.assertEqual(control.request_firmware("status"), body)

    def test_valid_error_preserves_code_message_and_status(self):
        self.reply(
            {"ok": False, "error": {"code": "update_busy", "message": "请稍后重试", "status": 409}}
        )
        with self.assertRaises(control.FirmwareError) as raised:
            control.request_firmware("status")
        self.assertEqual(
            (raised.exception.code, raised.exception.message, raised.exception.status),
            ("update_busy", "请稍后重试", 409),
        )

    def test_non_object_envelopes_are_typed_failures(self):
        for value in [None, False, True, 0, 1, "private-marker", []]:
            with self.subTest(value=value):
                self.reject(value)

    def test_ok_requires_boolean_and_closed_envelope(self):
        for value in [
            {},
            {"ok": 1, "body": {}},
            {"ok": "true", "body": {}},
            {"ok": None, "body": {}},
            {"ok": True},
            {"ok": True, "body": {}, "error": {}},
            {"ok": False, "body": {}},
            {"ok": False, "error": {}, "extra": 1},
        ]:
            with self.subTest(value=value):
                self.reject(value)

    def test_success_body_requires_object(self):
        for body in [None, [], "private-marker", True, 1]:
            with self.subTest(body=body):
                self.reject({"ok": True, "body": body})

    def test_error_structure_requires_complete_object(self):
        for error in [
            None,
            [],
            "private-marker",
            {},
            {"code": "busy"},
            {"code": "busy", "message": "x", "status": 409, "extra": 1},
        ]:
            with self.subTest(error=error):
                self.reject({"ok": False, "error": error})

    def test_error_status_requires_integer_http_error_range(self):
        for status in [None, True, False, 409.0, "409", -1, 200, 399, 600]:
            with self.subTest(status=status):
                self.reject(
                    {"ok": False, "error": {"code": "busy", "message": "x", "status": status}}
                )

    def test_error_code_and_message_types_are_checked(self):
        for code in [None, 7, "", "UPPER", "with-dash", "x\r\n", "x" * 129]:
            with self.subTest(code=code):
                self.reject({"ok": False, "error": {"code": code, "message": "x", "status": 503}})
        for message in [None, True, 7, [], ""]:
            with self.subTest(message=message):
                self.reject(
                    {"ok": False, "error": {"code": "busy", "message": message, "status": 503}}
                )

    def test_duplicate_fields_are_rejected_at_every_level(self):
        for raw in [
            b'{"ok":false,"ok":true,"body":{}}\n',
            b'{"ok":true,"body":{"x":1,"x":2}}\n',
            b'{"ok":false,"error":{"code":"busy","message":"x","status":409,"status":503}}\n',
        ]:
            with self.subTest(raw=raw):
                self.reject_raw(raw)

    def test_invalid_utf8_and_unpaired_surrogates_are_rejected(self):
        for raw in [
            b"\xff\n",
            b'{"ok":true,"body":{"x":"\\ud800"}}\n',
            b'{"ok":true,"body":{"\\udfff":0}}\n',
            '{"ok":true,"body":{}}\n'.encode("utf-16"),
        ]:
            with self.subTest(raw=raw):
                self.reject_raw(raw)

    def test_non_finite_numbers_are_rejected_including_overflow(self):
        for number in [b"NaN", b"Infinity", b"-Infinity", b"1e999"]:
            with self.subTest(number=number):
                self.reject_raw(b'{"ok":true,"body":{"x":' + number + b"}}\n")

    def test_truncated_or_missing_frame_terminator_is_rejected(self):
        for raw in [b"", b"{", b'{"ok":true,"body":{}}', b"private-marker\n"]:
            with self.subTest(raw=raw):
                self.reject_raw(raw)

    def test_size_limit_accepts_exact_boundary_and_rejects_one_byte_over(self):
        prefix, suffix = b'{"ok":true,"body":{"x":"', b'"}}\n'
        raw = prefix + b"x" * (control.LIMIT - len(prefix) - len(suffix)) + suffix
        self.assertEqual(len(raw), control.LIMIT)
        self.server.reply = raw
        self.assertEqual(
            len(control.request_firmware("status")["x"]), control.LIMIT - len(prefix) - len(suffix)
        )
        self.reject_raw(prefix + b"x" * (control.LIMIT + 1 - len(prefix) - len(suffix)) + suffix)

    def test_deep_json_is_typed_failure_instead_of_recursion_error(self):
        for depth in [65, 1500]:
            with self.subTest(depth=depth):
                self.reject_raw(
                    b'{"ok":true,"body":{"x":' + b"[" * depth + b"0" + b"]" * depth + b"}}\n"
                )
        self.server.reply = b'{"ok":true,"body":{"x":' + b"[" * 20 + b"0" + b"]" * 20 + b"}}\n"
        self.assertIn("x", control.request_firmware("status"))

    def test_protocol_failure_does_not_poison_next_connection(self):
        self.reject([])
        self.reply({"ok": True, "body": {"recovered": True}})
        self.assertEqual(control.request_firmware("status"), {"recovered": True})

    def test_gateway_returns_problem_for_bad_control_response_then_recovers(self):
        server = create_gateway_server(
            "127.0.0.1",
            0,
            SimpleNamespace(),
            security=SecurityPolicy.lab(allowed_operations={"firmwareStatus"}),
        )
        with running_server(server):
            client = http.client.HTTPConnection(*server.server_address, timeout=2)
            try:
                self.reply([])
                client.request("GET", "/api/v4/firmware")
                response = client.getresponse()
                self.assertEqual(response.status, 503)
                self.assertEqual(
                    json.loads(response.read())["error"]["code"], "update_service_unavailable"
                )
                self.reply({"ok": True, "body": {"recovered": True}})
                client.request("GET", "/api/v4/firmware")
                response = client.getresponse()
                self.assertEqual(response.status, 200)
                self.assertEqual(json.loads(response.read()), {"recovered": True})
            finally:
                client.close()

    def test_invalid_or_oversized_outgoing_payload_does_not_connect(self):
        with patch.object(control.socket, "socket") as connect:
            for fields in [
                {"value": object()},
                {"value": float("nan")},
                {"value": "x" * control.LIMIT},
            ]:
                with (
                    self.subTest(fields_type=type(fields["value"])),
                    self.assertRaises(control.FirmwareError),
                ):
                    control.request_firmware("check", **fields)
            connect.assert_not_called()

    def test_process_control_exception_is_not_converted_to_service_error(self):
        self.reply({"ok": True, "body": {}})
        for error in (KeyboardInterrupt(), SystemExit(7)):
            with (
                self.subTest(error=type(error).__name__),
                patch.object(control.json, "loads", side_effect=error),
                self.assertRaises(type(error)) as raised,
            ):
                control.request_firmware("status")
            self.assertIs(raised.exception, error)


class FirmwareControlHandlerProtocolTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = str(Path(directory.name) / "control.sock")
        self.server = control.ControlServer(self.path, control.ControlHandler)
        self.provider = Mock()
        self.provider.status.return_value = {"healthy": True}
        self.server.supervisor = self.provider
        self.context = running_server(self.server)
        self.context.__enter__()
        self.addCleanup(self.context.__exit__, None, None, None)

    def request(self, raw):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(2)
            client.connect(self.path)
            client.sendall(raw)
            client.shutdown(socket.SHUT_WR)
            with client.makefile("rb") as stream:
                reply = stream.readline(control.LIMIT + 1)
        self.assertLessEqual(len(reply), control.LIMIT)
        self.assertTrue(reply.endswith(b"\n"))
        return json.loads(reply)

    def test_invalid_request_framing_and_json_never_dispatch(self):
        for raw in [
            b'{"operation":"status"}',
            b'{"operation":"status","operation":"status"}\n',
            b'{"operation":"check","force":NaN}\n',
            b'{"operation":"\\ud800"}\n',
            b"[" * 1500 + b"0" + b"]" * 1500 + b"\n",
        ]:
            with self.subTest(raw_prefix=raw[:40]):
                self.assertFalse(self.request(raw)["ok"])
        self.assertEqual(self.provider.mock_calls, [])
        self.assertEqual(
            self.request(b'{"operation":"status"}\n'), {"ok": True, "body": {"healthy": True}}
        )

    def test_invalid_provider_results_return_bounded_error_and_recover(self):
        cyclic = {}
        cyclic["self"] = cyclic
        for body in [
            [],
            None,
            {"x": "\ud800"},
            {"x": float("inf")},
            {"x": "x" * control.LIMIT},
            cyclic,
        ]:
            with self.subTest(body_type=type(body)):
                self.provider.status.return_value = body
                reply = self.request(b'{"operation":"status"}\n')
                self.assertEqual(reply["error"]["status"], 503)
                self.assertEqual(reply["error"]["code"], "update_request_failed")
        self.provider.status.return_value = {"healthy": True}
        self.assertTrue(self.request(b'{"operation":"status"}\n')["ok"])

    def test_invalid_firmware_error_is_not_emitted_as_invalid_wire_response(self):
        self.provider.status.side_effect = control.FirmwareError("bad-code", "x", "200")
        reply = self.request(b'{"operation":"status"}\n')
        self.assertEqual(reply["error"]["code"], "update_request_failed")
        self.assertEqual(reply["error"]["status"], 503)
