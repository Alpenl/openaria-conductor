from __future__ import annotations

import http.client
import json
import socket
import sys
import threading
import time
import unittest
from unittest.mock import patch

from rp_ylx.api.gateway import _StreamRegistry, create_gateway_server
from rp_ylx.api.security import SecurityPolicy
from tests.test_gateway_stream_lifecycle import _LifecycleProvider


class StreamAdmissionRecoveryTest(unittest.TestCase):
    def test_failed_watcher_start_restores_each_stream_kind_and_retains_limit(self):
        for kind in ("preview", "sse"):
            with self.subTest(kind=kind):
                registry = _StreamRegistry(max_sse=1, max_preview=1)
                left, right = socket.socketpair()
                self.addCleanup(left.close)
                self.addCleanup(right.close)
                error = None
                with patch.object(
                    threading.Thread, "start", side_effect=RuntimeError("no threads")
                ):
                    try:
                        self.assertIsNone(registry.begin(kind, left, lambda: None))
                    except RuntimeError as caught:
                        error = caught
                recovered = registry.begin(kind, left, lambda: None)
                self.assertIsNotNone(recovered, "failed startup leaked the only stream slot")
                try:
                    self.assertIsNone(
                        error, "resource shortage escaped instead of rejecting admission"
                    )
                    self.assertIsNone(registry.begin(kind, left, lambda: None))
                    state = registry.snapshot()[kind]
                    self.assertEqual(state["active"], 1)
                    self.assertEqual(state["rejected"], 2)
                finally:
                    recovered.finish("test_complete")
                again = registry.begin(kind, left, lambda: None)
                self.assertIsNotNone(again)
                again.finish("test_complete")
                self.assertTrue(registry.wait_empty(1))

    def test_thread_construction_failure_also_refunds_slot(self):
        registry = _StreamRegistry(max_sse=1, max_preview=1)
        left, right = socket.socketpair()
        try:
            with patch.object(threading, "Thread", side_effect=RuntimeError("construction failed")):
                self.assertIsNone(registry.begin("preview", left, lambda: None))
            recovered = registry.begin("preview", left, lambda: None)
            self.assertIsNotNone(recovered)
            recovered.finish("test_complete")
        finally:
            left.close()
            right.close()

    def test_unexpected_constructor_failure_propagates_after_refunding_slot(self):
        registry = _StreamRegistry(max_sse=1, max_preview=1)
        left, right = socket.socketpair()
        try:
            with (
                patch("rp_ylx.api.gateway._StreamSession", side_effect=ValueError("unexpected")),
                self.assertRaisesRegex(ValueError, "unexpected"),
            ):
                registry.begin("sse", left, lambda: None)
            recovered = registry.begin("sse", left, lambda: None)
            self.assertIsNotNone(recovered)
            recovered.finish("test_complete")
        finally:
            left.close()
            right.close()


class StreamStartHttpTest(unittest.TestCase):
    def setUp(self):
        self.provider = _LifecycleProvider()
        self.server = create_gateway_server(
            "127.0.0.1",
            0,
            self.provider,
            security=SecurityPolicy.lab(
                allowed_operations={
                    "getPreview",
                    "streamCaptureEvents",
                    "streamNetworkEvents",
                }
            ),
            max_preview_streams=1,
            max_sse_connections=1,
        )
        self.errors = []
        self.server.handle_error = lambda *_: self.errors.append(type(sys.exc_info()[1]).__name__)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)
        self.assertFalse(self.thread.is_alive())

    def exercise(self, path, kind, expected_error):
        original = threading.Thread.start

        def fail_watcher(thread):
            if thread.name.startswith("gateway-" + kind + "-peer-"):
                raise RuntimeError("no thread resources")
            return original(thread)

        client = http.client.HTTPConnection(*self.server.server_address, timeout=2)
        try:
            with patch.object(threading.Thread, "start", fail_watcher):
                client.request("GET", path, headers={"Accept": "multipart/x-mixed-replace"})
                response = client.getresponse()
                self.assertEqual(response.status, 503)
                body = json.loads(response.read())
                self.assertEqual(body["error"]["code"], expected_error)
                self.assertEqual(response.getheader("Retry-After"), "1")
            client.request("GET", path, headers={"Accept": "multipart/x-mixed-replace"})
            response = client.getresponse()
            self.assertEqual(response.status, 200)
            self.assertTrue(response.read(1))
            response.close()
        finally:
            client.close()
        deadline = time.monotonic() + 2
        while (
            self.server.stream_lifecycle_snapshot()[kind]["active"] and time.monotonic() < deadline
        ):
            time.sleep(0.01)
        self.assertEqual(self.server.stream_lifecycle_snapshot()[kind]["active"], 0)
        self.assertEqual(self.errors, [])

    def test_preview_recovers_without_server_restart(self):
        self.exercise("/api/v4/preview", "preview", "preview_capacity_exhausted")

    def test_capture_events_recover_without_server_restart(self):
        self.exercise("/api/v4/capture/events", "sse", "stream_capacity_exhausted")

    def test_network_events_recover_without_server_restart(self):
        self.exercise("/api/v4/network/events", "sse", "stream_capacity_exhausted")
