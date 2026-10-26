from __future__ import annotations

import errno
import http.client
import json
import os
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from rp_ylx.api import AuditEvent
from rp_ylx.api.gateway import create_gateway_server
from rp_ylx.api.security import SecurityPolicy
from rp_ylx.daemon import _audit_sink
from tests.test_gateway import DeviceProvider


class AuditFileAdmissionTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.path = self.root / "audit.ndjson"
        self.sink = _audit_sink(self.path)
        self.event = AuditEvent("request", None, "getDevice", None, "allowed")

    def test_normal_creation_append_and_permission_repair(self):
        self.sink(self.event)
        self.path.chmod(0o644)
        self.sink(self.event)
        rows = [json.loads(line) for line in self.path.read_text().splitlines()]
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(row["request_id"] == "request" for row in rows))
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)

    def test_fifo_without_reader_does_not_block_open(self):
        os.mkfifo(self.path)
        self.check_fifo_child(race=False)

    def test_fifo_replacing_regular_file_between_inspection_and_open_does_not_block(self):
        self.path.write_text("")
        self.check_fifo_child(race=True)

    def check_fifo_child(self, *, race):
        code = """import os,sys
from pathlib import Path
from unittest.mock import patch
from rp_ylx.daemon import _audit_sink
from rp_ylx.api import AuditEvent
p=Path(sys.argv[1]);original=os.open
def replace(path,*args,**kwargs):
    if Path(path)==p:
        p.unlink();os.mkfifo(p)
    return original(path,*args,**kwargs)
try:
    if sys.argv[2]=='race':
        with patch('os.open',replace):
            _audit_sink(p)(AuditEvent('request',None,'getDevice',None,'allowed'))
    else:_audit_sink(p)(AuditEvent('request',None,'getDevice',None,'allowed'))
except OSError:sys.exit(0)
sys.exit(1)
"""
        child = subprocess.run(
            [sys.executable, "-c", code, str(self.path), "race" if race else "static"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        self.assertEqual(child.returncode, 0, child.stderr)

    def test_fifo_with_reader_is_rejected_before_any_bytes_are_written(self):
        os.mkfifo(self.path)
        descriptor = os.open(self.path, os.O_RDONLY | os.O_NONBLOCK)
        try:
            with self.assertRaises(OSError):
                self.sink(self.event)
            self.assertEqual(os.read(descriptor, 8192), b"")
        finally:
            os.close(descriptor)

    def test_hardlink_target_content_and_permissions_are_unchanged(self):
        target = self.root / "target"
        target.write_bytes(b"preserved\n")
        target.chmod(0o644)
        os.link(target, self.path)
        with self.assertRaises(OSError):
            self.sink(self.event)
        self.assertEqual(target.read_bytes(), b"preserved\n")
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o644)

    def test_hardlink_replacement_after_inspection_is_rejected_by_descriptor(self):
        target = self.root / "target"
        target.write_bytes(b"preserved\n")
        target.chmod(0o644)
        self.path.touch()
        original = os.open

        def replace(path, *args, **kwargs):
            if Path(path) == self.path:
                self.path.unlink()
                os.link(target, self.path)
            return original(path, *args, **kwargs)

        before = len(os.listdir("/proc/self/fd"))
        with patch("os.open", replace), self.assertRaises(OSError):
            self.sink(self.event)
        self.assertEqual(target.read_bytes(), b"preserved\n")
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o644)
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)

    def test_directory_symlink_and_socket_are_rejected_without_open(self):
        target = self.root / "target"
        target.write_bytes(b"preserved")
        for kind in ("directory", "symlink", "socket"):
            with self.subTest(kind=kind):
                listener = None
                if kind == "directory":
                    self.path.mkdir()
                elif kind == "symlink":
                    self.path.symlink_to(target)
                else:
                    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    listener.bind(str(self.path))
                try:
                    with (
                        patch("os.open", side_effect=AssertionError("must reject before open")),
                        self.assertRaises(OSError),
                    ):
                        self.sink(self.event)
                finally:
                    if listener is not None:
                        listener.close()
                    if kind == "directory":
                        self.path.rmdir()
                    else:
                        self.path.unlink()
        self.assertEqual(target.read_bytes(), b"preserved")

    def test_descriptor_is_closed_when_inspection_permission_or_write_fails(self):
        before = len(os.listdir("/proc/self/fd"))
        for operation in ("fstat", "fchmod", "write"):
            self.path.touch()
            self.path.chmod(0o644)
            error = OSError(errno.ENOSPC, "injected failure")
            with self.subTest(operation=operation):
                with (
                    patch("os." + operation, side_effect=error),
                    self.assertRaises(OSError) as caught,
                ):
                    self.sink(self.event)
                self.assertIs(caught.exception, error)
                self.assertEqual(len(os.listdir("/proc/self/fd")), before)

    def test_short_writes_and_concurrent_appends_keep_complete_records(self):
        original = os.write

        def short(descriptor, payload):
            return original(descriptor, payload[:7])

        with patch("os.write", short), ThreadPoolExecutor(max_workers=8) as pool:
            list(
                pool.map(
                    lambda i: self.sink(AuditEvent(str(i), None, "getDevice", None, "allowed")),
                    range(64),
                )
            )
        rows = [json.loads(line) for line in self.path.read_text().splitlines()]
        self.assertEqual({row["request_id"] for row in rows}, {str(i) for i in range(64)})
        self.assertEqual(len(rows), 64)

    def test_repaired_target_recovers_using_the_same_sink(self):
        os.mkfifo(self.path)
        with self.assertRaises(OSError):
            self.sink(self.event)
        self.path.unlink()
        self.sink(self.event)
        self.assertEqual(json.loads(self.path.read_text())["request_id"], "request")

    def test_http_requests_release_capacity_and_recover_after_target_repair(self):
        os.mkfifo(self.path)
        provider = DeviceProvider()
        server = create_gateway_server(
            "127.0.0.1",
            0,
            provider,
            security=SecurityPolicy.lab(allowed_operations={"getDevice"}),
            audit_sink=self.sink,
            max_http_connections=2,
        )
        errors = []
        server.handle_error = lambda *_: errors.append(type(sys.exception()).__name__)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with patch.object(
                provider, "device_descriptor", wraps=provider.device_descriptor
            ) as get:
                for _ in range(4):
                    client = http.client.HTTPConnection(*server.server_address, timeout=2)
                    try:
                        client.request("GET", "/api/v4/device", headers={"Connection": "close"})
                        response = client.getresponse()
                        self.assertEqual(response.status, 503)
                        self.assertEqual(
                            json.loads(response.read())["error"]["code"], "audit_unavailable"
                        )
                    finally:
                        client.close()
                    with server._http_condition:
                        self.assertTrue(
                            server._http_condition.wait_for(
                                lambda: not server._http_connections,
                                timeout=1,
                            )
                        )
                get.assert_not_called()
                self.path.unlink()
                client = http.client.HTTPConnection(*server.server_address, timeout=2)
                try:
                    client.request("GET", "/api/v4/device", headers={"Connection": "close"})
                    response = client.getresponse()
                    self.assertEqual(response.status, 200)
                    self.assertTrue(response.read())
                finally:
                    client.close()
                self.assertEqual(get.call_count, 1)
                self.assertEqual(errors, [])
        finally:
            # Also let an old blocking implementation exit if this is used as
            # a negative test; never strand its FIFO writer in the test process.
            reader = None
            if self.path.exists() and stat.S_ISFIFO(self.path.lstat().st_mode):
                reader = os.open(self.path, os.O_RDONLY | os.O_NONBLOCK)
            server.shutdown()
            server.server_close()
            thread.join(2)
            if reader is not None:
                os.close(reader)
            self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main()
