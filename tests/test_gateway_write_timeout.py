from __future__ import annotations

import http.client
import os
import socket
import tempfile
import threading
import time
import unittest

from rp_ylx.api.gateway import create_gateway_server
from rp_ylx.api.security import SecurityPolicy
from rp_ylx.native import native_session_store_or_none
from tests.test_gateway import SESSION_ID, DeviceProvider, MemoryRepresentation


class GatewayWriteTimeoutTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = create_gateway_server(
            "127.0.0.1",
            0,
            DeviceProvider(),
            security=SecurityPolicy.lab(allowed_operations={"getDevice", "getSessionArtifact"}),
        )
        self.server.write_timeout_seconds = 0.5
        original = self.server.get_request

        def small_send_buffer():
            connection, address = original()
            connection.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16384)
            return connection, address

        self.server.get_request = small_send_buffer
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)
        self.assertFalse(self.thread.is_alive())

    def check_download(self, mode: str, *, keep_reading: bool = False) -> None:
        native = native_session_store_or_none() if mode == "native" else None
        if mode == "native" and native is None:
            self.skipTest("native session store unavailable")
        entered = threading.Event()
        closed = threading.Event()
        sent_all = threading.Event()
        payload = b"bounded-write" * (65536 if keep_reading else 400000)
        with tempfile.TemporaryFile() as source:
            source.write(payload)
            source.flush()

            class Representation(MemoryRepresentation):
                def __exit__(self, *args):
                    closed.set()

                def iter_chunks(self, offset=0, length=None):
                    entered.set()
                    yield from super().iter_chunks(offset, length)
                    sent_all.set()

            if mode in {"native", "raw"}:

                def send_to(self, descriptor, offset, length):
                    entered.set()
                    if native is not None:
                        sent = native.sendfile(descriptor, source.fileno(), offset, length)
                    else:
                        sent = 0
                        while sent < length:
                            sent += os.write(descriptor, payload[offset + sent : offset + length])
                    if offset + length == len(payload):
                        sent_all.set()
                    return sent

                Representation.send_to = send_to

            self.server.provider.artifact_io_state = lambda: None
            self.server.provider.open_verified_artifact = lambda *args: Representation(payload)
            client = http.client.HTTPConnection(*self.server.server_address, timeout=5)
            try:
                client.connect()
                client.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16384)
                client.request(
                    "GET",
                    f"/api/v3/sessions/{SESSION_ID}/artifacts/{'a' * 64}",
                    headers={"Connection": "close"},
                )
                response = client.getresponse()
                self.assertEqual(response.status, 200)
                self.assertTrue(entered.wait(1))
                begin = time.monotonic()
                if keep_reading:
                    received = bytearray()
                    while chunk := response.read(8192):
                        received.extend(chunk)
                        time.sleep(0.005)
                    self.assertEqual(received, payload)
                    self.assertTrue(sent_all.wait(1))
                    self.assertGreater(time.monotonic() - begin, self.server.write_timeout_seconds)
                else:
                    self.assertTrue(closed.wait(2), "stalled client retained the download")
                    self.assertFalse(sent_all.is_set(), "test did not create receiver backpressure")
                    self.assertTrue(self.thread.is_alive(), "server stopped to recover download")
                self.assertTrue(closed.wait(1))
            finally:
                client.close()

    def test_stalled_native_download_releases_representation(self) -> None:
        self.check_download("native")

    def test_stalled_raw_descriptor_download_releases_representation(self) -> None:
        self.check_download("raw")

    def test_stalled_chunked_writer_releases_representation(self) -> None:
        self.check_download("chunks")

    def test_slow_native_download_making_progress_completes(self) -> None:
        self.check_download("native", keep_reading=True)

    def test_slow_raw_descriptor_download_making_progress_completes(self) -> None:
        self.check_download("raw", keep_reading=True)

    def test_write_timeout_requires_finite_bounded_positive_value(self) -> None:
        for value in (0, -1, float("nan"), float("inf"), 3601):
            with self.subTest(timeout=value), self.assertRaises(ValueError):
                create_gateway_server(
                    "127.0.0.1",
                    0,
                    DeviceProvider(),
                    security=SecurityPolicy.lab(allowed_operations={"getDevice"}),
                    write_timeout_seconds=value,
                )

    def test_write_monitor_stops_with_server(self) -> None:
        watcher = self.server._deadline_watcher
        self.assertTrue(watcher.is_alive())
        self.server.shutdown()
        self.server.server_close()
        watcher.join(1)
        self.assertFalse(watcher.is_alive())

    def test_completed_write_deadline_does_not_close_next_keepalive_request(self) -> None:
        client = http.client.HTTPConnection(*self.server.server_address, timeout=2)
        try:
            client.request("GET", "/api/v4/device")
            response = client.getresponse()
            self.assertEqual(response.status, 200)
            response.read()
            connection = client.sock
            time.sleep(self.server.write_timeout_seconds + 0.1)
            client.request("GET", "/api/v4/device")
            response = client.getresponse()
            self.assertEqual(response.status, 200)
            response.read()
            self.assertIs(client.sock, connection)
        finally:
            client.close()
