from __future__ import annotations

import http.client
import socket
import threading
import time
import unittest

import test_session_download as fixtures

from rp_ylx.api import ProviderError, SecurityPolicy, create_gateway_server


class _CancellableProvider(fixtures._MemoryProvider):
    def __init__(self):
        super().__init__()
        self.entered = threading.Event()
        self.exited = threading.Event()
        self.block = True
        self.error = None

    def open_cancellable_artifact(self, session, artifact, version, *, cancelled):
        self.entered.set()
        try:
            if self.error is not None:
                raise self.error
            deadline = time.monotonic() + 3
            while self.block and time.monotonic() < deadline:
                if cancelled():
                    raise ProviderError(
                        "verification_cancelled", "已取消", status=503, retryable=True
                    )
                time.sleep(0.01)
            if self.block:
                raise AssertionError("gateway failed to propagate cancellation")
            return super().open_verified_artifact(session, artifact, version)
        finally:
            self.exited.set()


class DownloadVerificationCancelTest(unittest.TestCase):
    def setUp(self):
        self.provider = _CancellableProvider()
        self.server = create_gateway_server(
            "127.0.0.1",
            0,
            self.provider,
            security=SecurityPolicy.lab(
                allowed_operations={"getSessionArtifact", "headSessionArtifact"}
            ),
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.path = f"/api/v4/sessions/{fixtures.SESSION_ID}/artifacts/{fixtures.ARTIFACT_ID}"

    def tearDown(self):
        self.provider.block = False
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def test_get_and_head_disconnect_interrupt_verification(self):
        for method in ("GET", "HEAD"):
            with self.subTest(method=method):
                self.provider.entered.clear()
                self.provider.exited.clear()
                with socket.create_connection(("127.0.0.1", self.server.server_port)) as sock:
                    sock.sendall(
                        f"{method} {self.path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode()
                    )
                    self.assertTrue(self.provider.entered.wait(1))
                    sock.shutdown(socket.SHUT_RDWR)
                self.assertTrue(self.provider.exited.wait(1))
        self.assertEqual(self.provider.open_count, 0)

    def test_shutdown_interrupts_verification(self):
        with socket.create_connection(("127.0.0.1", self.server.server_port)) as sock:
            sock.sendall(f"GET {self.path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
            self.assertTrue(self.provider.entered.wait(1))
            self.server.shutdown()
            self.assertTrue(self.provider.exited.wait(1))

    def test_busy_response_preserves_retry_headers_for_get_and_head(self):
        self.provider.error = ProviderError(
            "verification_busy", "校验繁忙", status=503, retryable=True
        )
        for method in ("GET", "HEAD"):
            with self.subTest(method=method):
                connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port)
                try:
                    connection.request(method, self.path)
                    response = connection.getresponse()
                    self.assertEqual(response.status, 503)
                    self.assertEqual(response.getheader("Retry-After"), "1")
                    self.assertEqual(response.getheader("YLX-Error-Code"), "verification_busy")
                    response.read()
                finally:
                    connection.close()


if __name__ == "__main__":
    unittest.main()
