from __future__ import annotations

import json
import socket
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from rp_ylx import firmware_control as control


class FirmwareControlCapacityTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.clients = []
        self.servers = []
        self.releases = []

    def tearDown(self):
        for event in self.releases:
            event.set()
        for client in self.clients:
            client.close()
        for server, acceptor, errors in reversed(self.servers):
            if acceptor:
                server.shutdown()
            server.server_close()
            if acceptor:
                acceptor.join(3)
                self.assertFalse(acceptor.is_alive())
            self.assertEqual(server._control_connections, {})
            self.assertEqual(server._control_workers, {})
            self.assertEqual(errors, [])

    def server(self, *, serve=True, handler=control.ControlHandler, **kwargs):
        path = self.root / (str(len(self.servers)) + ".sock")
        server = control.ControlServer(str(path), handler, **kwargs)
        server.supervisor = Mock()
        server.supervisor.status.return_value = {"healthy": True}
        errors = []
        server.handle_error = lambda *_: errors.append(type(sys.exception()).__name__)
        acceptor = None
        if serve:
            acceptor = threading.Thread(
                target=lambda: server.serve_forever(poll_interval=0.01), daemon=True
            )
            acceptor.start()
        self.servers.append((server, acceptor, errors))
        return server

    def connect(self, server):
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.settimeout(2)
        self.clients.append(client)
        deadline = time.monotonic() + 2
        while True:
            try:
                client.connect(server.server_address)
                return client
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.005)

    def pair(self):
        request, client = socket.socketpair()
        request.settimeout(2)
        client.settimeout(2)
        self.clients.extend([request, client])
        return request, client

    def wait_count(self, server, count):
        with server._control_condition:
            self.assertTrue(
                server._control_condition.wait_for(
                    lambda: len(server._control_connections) == count, timeout=2
                )
            )

    def response(self, client):
        with client.makefile("rb") as stream:
            raw = stream.readline(control.LIMIT + 1)
        return json.loads(raw)

    def assert_busy(self, client):
        reply = self.response(client)
        self.assertFalse(reply["ok"])
        self.assertEqual(reply["error"]["code"], "update_service_busy")
        self.assertEqual(reply["error"]["status"], 503)

    def assert_healthy_direct(self, server):
        request, client = self.pair()
        client.sendall(b'{"operation":"status"}\n')
        server.process_request(request, "test")
        self.assertEqual(self.response(client), {"ok": True, "body": {"healthy": True}})
        self.wait_count(server, 0)

    def test_capacity_rejection_does_not_allocate_threads_or_dispatch(self):
        server = self.server(max_connections=2)
        self.connect(server)
        self.connect(server)
        self.wait_count(server, 2)
        workers = set(server._control_workers.values())
        for _ in range(20):
            self.assert_busy(self.connect(server))
        self.assertEqual(set(server._control_workers.values()), workers)
        self.assertEqual(server.supervisor.mock_calls, [])
        self.assertEqual(len(workers), 2)

    def test_client_helper_receives_typed_busy_without_executing_operation(self):
        server = self.server(max_connections=1)
        self.connect(server)
        self.wait_count(server, 1)
        with (
            patch.object(control, "SOCKET", server.server_address),
            self.assertRaises(control.FirmwareError) as raised,
        ):
            control.request_firmware("status")
        self.assertEqual(
            (raised.exception.code, raised.exception.status), ("update_service_busy", 503)
        )
        server.supervisor.status.assert_not_called()

    def test_clean_eof_releases_capacity_without_error_logging(self):
        server = self.server(max_connections=1)
        client = self.connect(server)
        self.wait_count(server, 1)
        client.close()
        self.wait_count(server, 0)
        client = self.connect(server)
        client.sendall(b'{"operation":"status"}\n')
        self.assertTrue(self.response(client)["ok"])
        self.wait_count(server, 0)
        server.supervisor.status.assert_called_once()

    def test_idle_and_partial_requests_expire_and_restore_capacity(self):
        server = self.server(max_connections=1, request_timeout_seconds=0.15)
        for payload in [b"", b'{"operation":']:
            with self.subTest(payload=payload):
                client = self.connect(server)
                if payload:
                    client.sendall(payload)
                started = time.monotonic()
                self.assertEqual(client.recv(1), b"")
                self.assertLess(time.monotonic() - started, 1.5)
                self.wait_count(server, 0)
        self.assertEqual(server.supervisor.mock_calls, [])

    def test_trickled_bytes_do_not_reset_total_read_deadline(self):
        server = self.server(max_connections=1, request_timeout_seconds=0.2)
        client = self.connect(server)
        stop = threading.Event()
        self.releases.append(stop)
        sent = []

        def trickle():
            while not stop.wait(0.025):
                try:
                    client.sendall(b" ")
                except OSError:
                    return
                sent.append(1)

        writer = threading.Thread(target=trickle, daemon=True)
        started = time.monotonic()
        writer.start()
        try:
            # The peer may still be writing when the deadline closes its
            # socket; the kernel can report reset instead of an orderly EOF.
            try:
                closed = client.recv(1)
            except ConnectionResetError:
                closed = b""
            self.assertEqual(closed, b"")
            self.assertLess(time.monotonic() - started, 1.5)
            self.assertGreaterEqual(len(sent), 2)
        finally:
            stop.set()
            writer.join(2)
        self.assertFalse(writer.is_alive())
        self.wait_count(server, 0)
        self.assertEqual(server.supervisor.mock_calls, [])

    def test_fragmented_valid_frame_completes_before_deadline(self):
        server = self.server(request_timeout_seconds=1)
        client = self.connect(server)
        for chunk in [b'{"operation":', b'"status"}', b"\n"]:
            client.sendall(chunk)
            time.sleep(0.01)
        self.assertEqual(self.response(client), {"ok": True, "body": {"healthy": True}})

    def test_close_wakes_idle_handlers_and_joins_owned_threads(self):
        server = self.server(max_connections=3)
        for _ in range(3):
            self.connect(server)
        self.wait_count(server, 3)
        workers = list(server._control_workers.values())
        server.server_close()
        self.assertEqual(server._control_connections, {})
        self.assertTrue(all(not worker.is_alive() for worker in workers))
        self.assertEqual(server.supervisor.mock_calls, [])

    def test_close_is_bounded_but_retains_active_application_ownership(self):
        server = self.server(close_timeout_seconds=0.05)
        entered, release = threading.Event(), threading.Event()
        self.releases.append(release)

        def status():
            entered.set()
            release.wait(3)
            return {"finished": True}

        server.supervisor.status.side_effect = status
        client = self.connect(server)
        client.sendall(b'{"operation":"status"}\n')
        self.assertTrue(entered.wait(2))
        started = time.monotonic()
        server.server_close()
        self.assertLess(time.monotonic() - started, 0.5)
        self.assertEqual(len(server._control_connections), 1)
        with self.assertRaises(control.FirmwareError):
            server.dispatch_control({"operation": "status"})
        release.set()
        self.wait_count(server, 0)
        server.server_close()
        server.supervisor.status.assert_called_once()

    def test_close_from_handler_does_not_join_itself(self):
        server = self.server()

        def status():
            server.server_close()
            return {"closed": True}

        server.supervisor.status.side_effect = status
        client = self.connect(server)
        client.sendall(b'{"operation":"status"}\n')
        self.assertEqual(client.recv(1), b"")
        self.wait_count(server, 0)
        server.supervisor.status.assert_called_once()

    def test_closed_server_rejects_new_direct_admission(self):
        server = self.server(serve=False)
        server.server_close()
        request, client = self.pair()
        with patch.object(control.threading, "Thread") as worker:
            server.process_request(request, "test")
            self.assert_busy(client)
            worker.assert_not_called()
        self.assertEqual(request.fileno(), -1)
        self.assertEqual(server.supervisor.mock_calls, [])

    def test_thread_constructor_failure_releases_slot_and_recovers(self):
        server = self.server(serve=False, max_connections=1)
        request, client = self.pair()
        with patch.object(control.threading, "Thread", side_effect=RuntimeError("cannot create")):
            server.process_request(request, "test")
        self.assert_busy(client)
        self.assertEqual(request.fileno(), -1)
        self.assertEqual(server._control_connections, {})
        self.assert_healthy_direct(server)

    def test_thread_start_failure_releases_slot_and_recovers(self):
        server = self.server(serve=False, max_connections=1)
        request, client = self.pair()
        with patch.object(threading.Thread, "start", side_effect=RuntimeError("cannot start")):
            server.process_request(request, "test")
        self.assert_busy(client)
        self.assertEqual(server._control_connections, {})
        self.assert_healthy_direct(server)

    def test_failure_or_cancellation_after_real_start_never_dispatches(self):
        server = self.server(serve=False, max_connections=1)
        real_start = threading.Thread.start
        for error in [RuntimeError("after start"), KeyboardInterrupt(), SystemExit(7)]:
            with self.subTest(error=type(error).__name__):
                request, client = self.pair()
                workers = []
                client.sendall(b'{"operation":"status"}\n')

                def start_then_raise(worker, error=error, workers=workers):
                    real_start(worker)
                    workers.append(worker)
                    raise error

                with patch.object(threading.Thread, "start", start_then_raise):
                    if isinstance(error, Exception):
                        server.process_request(request, "test")
                        self.assert_busy(client)
                    else:
                        with self.assertRaises(type(error)) as raised:
                            server.process_request(request, "test")
                        self.assertIs(raised.exception, error)
                for worker in workers:
                    worker.join(2)
                    self.assertFalse(worker.is_alive())
                self.assertEqual(request.fileno(), -1)
                self.assertEqual(server._control_connections, {})
                self.assertEqual(server.supervisor.mock_calls, [])
        self.assert_healthy_direct(server)

    def test_gate_creation_failure_does_not_leak_reserved_capacity(self):
        server = self.server(serve=False, max_connections=1)
        request, client = self.pair()
        with patch.object(threading, "Event", side_effect=RuntimeError("cannot create gate")):
            server.process_request(request, "test")
        self.assert_busy(client)
        self.assertEqual(server._control_connections, {})
        self.assert_healthy_direct(server)

    def test_failed_cleanup_join_still_closes_rejected_connection(self):
        server = self.server(serve=False)
        request, client = self.pair()
        workers = []
        real_start, real_join = threading.Thread.start, threading.Thread.join

        def start_then_raise(worker):
            real_start(worker)
            workers.append(worker)
            raise RuntimeError("after start")

        with (
            patch.object(threading.Thread, "start", start_then_raise),
            patch.object(threading.Thread, "join", side_effect=RuntimeError("join failed")),
        ):
            server.process_request(request, "test")
        self.assert_busy(client)
        for worker in workers:
            real_join(worker, 2)
        self.assertEqual(request.fileno(), -1)
        self.assertEqual(server._control_connections, {})
        self.assertEqual(server.supervisor.mock_calls, [])

    def test_rejected_peer_disconnect_does_not_raise_or_allocate_worker(self):
        server = self.server(max_connections=1)
        self.connect(server)
        self.wait_count(server, 1)
        request, peer = self.pair()
        peer.close()
        with patch.object(control.threading, "Thread") as worker:
            server.process_request(request, "test")
            worker.assert_not_called()
        self.assertEqual(request.fileno(), -1)
        self.assertEqual(len(server._control_connections), 1)

    def test_handler_setup_error_releases_capacity_and_server_recovers(self):
        class BadSetup(control.ControlHandler):
            def setup(self):
                raise RuntimeError("setup failure")

        server = self.server(handler=BadSetup, max_connections=1)
        client = self.connect(server)
        self.assertEqual(client.recv(1), b"")
        self.wait_count(server, 0)
        errors = self.servers[-1][2]
        self.assertEqual(errors, ["RuntimeError"])
        errors.clear()
        server.RequestHandlerClass = control.ControlHandler
        client = self.connect(server)
        client.sendall(b'{"operation":"status"}\n')
        self.assertTrue(self.response(client)["ok"])

    def test_invalid_capacity_and_timeouts_fail_before_binding(self):
        cases = [{"max_connections": v} for v in [0, -1, True, 1.5, "2"]]
        cases += [
            {name: v}
            for name in ["request_timeout_seconds", "close_timeout_seconds"]
            for v in [0, -1, True, float("nan"), float("inf"), "1"]
        ]
        path = self.root / "invalid.sock"
        for kwargs in cases:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                control.ControlServer(str(path), control.ControlHandler, **kwargs)
            self.assertFalse(path.exists())
