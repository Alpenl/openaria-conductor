"""Unit tests for Wave 1 stdlib-only core primitives, config, and errors."""

from __future__ import annotations

import errno
import hashlib
import http.server
import json
import os
import socket
import ssl
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
import uuid
from datetime import UTC
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from rp_ylx import daemon
from rp_ylx.api import gateway as api_gateway
from rp_ylx.config import (
    PRODUCTION_CONFIG_SCHEMA,
    ProductionConfig,
    ProductionConfigError,
    RecordingEncoding,
    load_bootstrap_config,
    production_config_from_value,
    read_bearer_token,
)
from rp_ylx.core.board import RDK_X5_MODEL, is_rdk_x5_model, normalize_board_model
from rp_ylx.core.device_client import BootstrapEndpoint, load_bootstrap_endpoint
from rp_ylx.core.fs import (
    chown_to_parent,
    export_atomic_no_overwrite,
    fsync_directory,
    read_bounded_json,
    require_regular_file,
    sha256_file,
    write_bytes_atomic,
    write_json_atomic,
)
from rp_ylx.core.ids import (
    SHA256_HEX_RE,
    UUID_V4_RE,
    UUID_V7_RE,
    parse_rfc3339,
    valid_customer_bearer_token,
    valid_sha256,
    valid_uuid4,
    valid_uuid7,
)
from rp_ylx.core.locks import acquire_flock
from rp_ylx.core.rpc import (
    ControlRpcClient,
    ControlRpcServer,
    RpcFramingError,
    RpcTimeoutError,
    read_bounded_ndjson_stream,
)
from rp_ylx.core.time import utc_now_iso
from rp_ylx.errors import ProviderError
from tests.testkit.tls import write_self_signed_tls


def _sample_bootstrap_dict(root: Path, *, profile: str = "lab") -> dict[str, object]:
    payload: dict[str, object] = {
        "schema": PRODUCTION_CONFIG_SCHEMA,
        "listen": {"host": "127.0.0.1", "port": 8080},
        "camera": {
            "device": "/dev/video0",
            "width": 3840,
            "height": 1080,
            "fps": 60,
            "data_plane": "rust",
        },
        "storage": {
            "mountpoint": str(root / "volume"),
            "minimum_available_bytes": 0,
            "minimum_available_inodes": 0,
        },
        "state_root": str(root / "state"),
        "device": {
            "device_id": str(uuid.uuid4()),
            "device_label": "YLX-12AB34CD",
            "hardware_fingerprint": "sha256:" + "a" * 64,
        },
        "security": {"profile": "lab", "isolated_network": True},
    }
    if profile == "customer":
        token_path = root / "customer.token"
        cert_path = root / "device.crt"
        key_path = root / "device.key"
        payload["security"] = {
            "profile": "customer",
            "isolated_network": True,
            "bearer_token_file": str(token_path),
            "tls_certificate_file": str(cert_path),
            "tls_private_key_file": str(key_path),
            "principal_id": "device-owner",
        }
    return payload


class ErrorsAndTimeTest(unittest.TestCase):
    def test_provider_error_attributes_and_parity(self) -> None:
        err = ProviderError(
            "camera_busy",
            "camera is busy",
            status=409,
            retryable=True,
            headers={"Retry-After": "2"},
            details={"slot": 1},
        )
        self.assertIsInstance(err, RuntimeError)
        self.assertEqual(err.code, "camera_busy")
        self.assertEqual(err.message, "camera is busy")
        self.assertEqual(str(err), "camera is busy")
        self.assertEqual(err.status, 409)
        self.assertTrue(err.retryable)
        self.assertEqual(err.headers, {"Retry-After": "2"})
        self.assertEqual(err.details, {"slot": 1})

        gateway_err = api_gateway.ProviderError("camera_busy", "camera is busy", status=409)
        self.assertEqual(
            (err.code, err.message, err.status),
            (gateway_err.code, gateway_err.message, gateway_err.status),
        )

    def test_utc_now_iso_is_valid_z_timestamp(self) -> None:
        stamp = utc_now_iso()
        self.assertTrue(stamp.endswith("Z"))
        parsed = parse_rfc3339(stamp)
        self.assertIs(parsed.tzinfo, UTC)


class IdsAndBoardTest(unittest.TestCase):
    def test_uuid_and_sha256_validators(self) -> None:
        u4 = str(uuid.uuid4())
        u7 = "01936b8c-1234-7abc-8def-0123456789ab"
        digest = "a" * 64
        self.assertTrue(UUID_V4_RE.fullmatch(u4))
        self.assertTrue(UUID_V7_RE.fullmatch(u7))
        self.assertTrue(SHA256_HEX_RE.fullmatch(digest))
        self.assertTrue(valid_uuid4(u4))
        self.assertFalse(valid_uuid4(u4.upper()))
        self.assertFalse(valid_uuid4(u4 + "\n"))
        self.assertFalse(valid_uuid4(u7))
        self.assertFalse(valid_uuid4(None))
        self.assertTrue(valid_uuid7(u7))
        self.assertFalse(valid_uuid7(u7.upper()))
        self.assertFalse(valid_uuid7(u4))
        self.assertTrue(valid_sha256(digest))
        self.assertFalse(valid_sha256(digest.upper()))
        self.assertFalse(valid_sha256("g" * 64))

    def test_parse_rfc3339_strict(self) -> None:
        dt_z = parse_rfc3339("2026-05-01T12:30:45.123456789Z")
        self.assertEqual((dt_z.year, dt_z.month, dt_z.day, dt_z.microsecond), (2026, 5, 1, 123456))
        self.assertIs(dt_z.tzinfo, UTC)

        dt_offset = parse_rfc3339("2026-05-01t10:00:00+08:00")
        self.assertEqual(dt_offset.hour, 2)
        self.assertIs(dt_offset.tzinfo, UTC)

        invalid_inputs: list[object] = [
            None,
            12345,
            "",
            " 2026-05-01T12:00:00Z",
            "2026-05-01T12:00:00Z ",
            "2026-05-01T12:00:00Z\n",
            "2026-05-01T12:00:00",
            "2026-05-01 12:00:00Z",
            "9999-99-99T99:99:99Z",
            "2026-02-30T12:00:00Z",
            "2026-05-01T24:00:00Z",
            "2026-05-01T12:00:00+24:00",
            "2026-05-01T12:00:00+00:60",
        ]
        for bad in invalid_inputs:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                parse_rfc3339(bad)

    def test_valid_customer_bearer_token(self) -> None:
        self.assertTrue(valid_customer_bearer_token("12345678"))
        self.assertTrue(valid_customer_bearer_token("!" * 512))
        self.assertFalse(valid_customer_bearer_token("1234567"))
        self.assertFalse(valid_customer_bearer_token("a" * 513))
        self.assertFalse(valid_customer_bearer_token("has space"))
        self.assertFalse(valid_customer_bearer_token("has\nnewline"))
        self.assertFalse(valid_customer_bearer_token(b"12345678"))

    def test_board_model_normalization_with_nul_bytes(self) -> None:
        self.assertEqual(RDK_X5_MODEL, "D-Robotics RDK X5 V1.0")
        self.assertEqual(
            normalize_board_model(b"  D-Robotics\tRDK  X5 V1.0\x00\x00\n"),
            RDK_X5_MODEL,
        )
        self.assertTrue(is_rdk_x5_model("  d-robotics rdk x5 v1.0\x00 "))
        self.assertTrue(is_rdk_x5_model(b"D-Robotics RDK X5 V1.0\x00"))
        self.assertFalse(is_rdk_x5_model("Raspberry Pi 5\x00"))
        self.assertFalse(is_rdk_x5_model(None))


class FilesystemPrimitivesTest(unittest.TestCase):
    def test_atomic_writes_preserve_mode_under_restrictive_umask(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "config.json"
            old_umask = os.umask(0o077)
            try:
                write_json_atomic(target, {"b": 2, "a": 1}, mode=0o644, sort_keys=True)
            finally:
                os.umask(old_umask)
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o644)
            self.assertEqual(target.read_text(encoding="utf-8"), '{"a":1,"b":2}\n')

            with self.assertRaises(ValueError):
                write_json_atomic(target, {"bad": float("nan")})
            self.assertEqual(list(root.glob("*.tmp")), [])

    def test_write_bytes_atomic_cleans_up_temp_on_write_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "data.bin"
            with (
                patch("rp_ylx.core.fs.os.write", side_effect=OSError("disk full")),
                self.assertRaises(OSError),
            ):
                write_bytes_atomic(target, b"hello", mode=0o640)
            self.assertFalse(target.exists())
            self.assertEqual(list(root.glob("*.tmp")), [])

    def test_fsync_directory_and_chown_to_parent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fsync_directory(root)
            file_path = root / "file.txt"
            file_path.write_bytes(b"abc")
            with self.assertRaises(OSError):
                fsync_directory(file_path)

            with (
                patch("rp_ylx.core.fs.os.geteuid", return_value=0, create=True),
                patch("rp_ylx.core.fs.os.chown") as mock_chown,
            ):
                chown_to_parent(file_path)
            st = root.stat()
            mock_chown.assert_called_once_with(
                file_path, st.st_uid, st.st_gid, follow_symlinks=False
            )

    def test_export_atomic_no_overwrite_posix_and_exfat_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dest = root / "export.json"
            export_atomic_no_overwrite(dest, b'{"ok":true}\n', mode=0o600)
            self.assertEqual(dest.read_bytes(), b'{"ok":true}\n')
            self.assertEqual(stat.S_IMODE(dest.stat().st_mode), 0o600)
            self.assertEqual(list(root.glob("*.tmp")), [])

            with self.assertRaises(FileExistsError):
                export_atomic_no_overwrite(dest, b"overwrite", mode=0o600)
            self.assertEqual(dest.read_bytes(), b'{"ok":true}\n')

            # Simulate exFAT mount where fchmod raises EOPNOTSUPP and os.link raises EPERM.
            exfat_dest = root / "exfat-export.json"
            with (
                patch(
                    "rp_ylx.core.fs.os.fchmod",
                    side_effect=OSError(errno.EOPNOTSUPP, "not supported"),
                ),
                patch(
                    "rp_ylx.core.fs.os.link",
                    side_effect=OSError(errno.EPERM, "hardlinks unsupported on exfat"),
                ),
            ):
                export_atomic_no_overwrite(exfat_dest, b'{"exfat":1}\n', mode=0o600)
            self.assertEqual(exfat_dest.read_bytes(), b'{"exfat":1}\n')
            self.assertEqual(list(root.glob("*.tmp")), [])

            with (
                patch(
                    "rp_ylx.core.fs.os.fchmod",
                    side_effect=OSError(errno.EPERM, "not supported"),
                ),
                patch(
                    "rp_ylx.core.fs.os.link",
                    side_effect=OSError(errno.EXDEV, "cross-device"),
                ),
                self.assertRaises(FileExistsError),
            ):
                export_atomic_no_overwrite(exfat_dest, b"nope", mode=0o600)
            self.assertEqual(exfat_dest.read_bytes(), b'{"exfat":1}\n')

            # On POSIX, fchmod PermissionError without EPERM/EOPNOTSUPP errno still fails.
            posix_fail = root / "posix-fail.json"
            with (
                patch("rp_ylx.core.fs.os.fchmod", side_effect=PermissionError("mode denied")),
                self.assertRaises(PermissionError),
            ):
                export_atomic_no_overwrite(posix_fail, b"{}", mode=0o600)
            self.assertFalse(posix_fail.exists())
            self.assertEqual(list(root.glob("*.tmp")), [])

    def test_sha256_file_require_regular_file_and_read_bounded_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            regular = root / "sample.json"
            payload = b'{"key":"value"}\n'
            regular.write_bytes(payload)
            self.assertEqual(sha256_file(regular), hashlib.sha256(payload).hexdigest())
            self.assertEqual(read_bounded_json(regular), {"key": "value"})
            self.assertTrue(stat.S_ISREG(require_regular_file(regular).st_mode))

            link = root / "link.json"
            link.symlink_to(regular)
            with self.assertRaises(OSError):
                sha256_file(link)
            with self.assertRaises(ValueError):
                require_regular_file(link)
            with self.assertRaises(ValueError):
                read_bounded_json(link)

            hardlink = root / "hardlink.json"
            os.link(regular, hardlink)
            with self.assertRaises(ValueError):
                sha256_file(regular)
            with self.assertRaises(ValueError):
                require_regular_file(regular)
            hardlink_meta = require_regular_file(regular, allow_hardlinks=True)
            self.assertTrue(stat.S_ISREG(hardlink_meta.st_mode))
            with self.assertRaises(ValueError):
                read_bounded_json(regular)
            hardlink.unlink()

            fifo = root / "pipe"
            os.mkfifo(fifo)
            with self.assertRaises(ValueError):
                require_regular_file(fifo)

            dup = root / "dup.json"
            dup.write_bytes(b'{"a":1,"a":2}')
            with self.assertRaises(ValueError):
                read_bounded_json(dup)

            nan_file = root / "nan.json"
            nan_file.write_bytes(b'{"x":NaN}')
            with self.assertRaises(ValueError):
                read_bounded_json(nan_file)

            deep_file = root / "deep.json"
            deep_file.write_bytes(b"[" * 10 + b"1" + b"]" * 10)
            with self.assertRaises(ValueError):
                read_bounded_json(deep_file, max_depth=5)

            big_file = root / "big.json"
            big_file.write_bytes(b'{"a":"' + b"x" * 200 + b'"}')
            with self.assertRaises(ValueError):
                read_bounded_json(big_file, max_bytes=100)


class LocksTest(unittest.TestCase):
    def test_acquire_flock_forces_mode_under_umask_and_enforces_exclusivity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / "sub" / "operation.lock"
            old_umask = os.umask(0o077)
            try:
                with acquire_flock(lock_path, mode=0o660, group_name=None):
                    self.assertEqual(stat.S_IMODE(lock_path.stat().st_mode), 0o660)
                    with (
                        self.assertRaises(BlockingIOError),
                        acquire_flock(lock_path, nonblocking=True, group_name=None),
                    ):
                        pass
            finally:
                os.umask(old_umask)
            # Lock file inode persists after release and can be re-acquired.
            self.assertTrue(lock_path.exists())
            with acquire_flock(lock_path, nonblocking=True, group_name=None):
                pass

    def test_acquire_flock_shared_and_root_group_chown(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / "shared.lock"
            with acquire_flock(lock_path, shared=True, nonblocking=True, group_name=None):
                with acquire_flock(lock_path, shared=True, nonblocking=True, group_name=None):
                    pass
                with (
                    self.assertRaises(BlockingIOError),
                    acquire_flock(lock_path, shared=False, nonblocking=True, group_name=None),
                ):
                    pass

            with (
                patch("rp_ylx.core.locks.os.geteuid", return_value=0, create=True),
                patch("rp_ylx.core.locks.grp.getgrnam", return_value=SimpleNamespace(gr_gid=4242)),
                patch("rp_ylx.core.locks.os.fchown") as mock_fchown,
                acquire_flock(lock_path, mode=0o660, group_name="rp-ylx") as fd,
            ):
                mock_fchown.assert_called_once_with(fd, -1, 4242)


class RpcTest(unittest.TestCase):
    def test_roundtrip_concurrent_reads_and_serialized_mutations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            sock_path = Path(directory) / "rpc.sock"
            active_reads = 0
            max_reads = 0
            active_writes = 0
            max_writes = 0
            read_barrier = threading.Barrier(2, timeout=3)
            counter_lock = threading.Lock()

            def handle(req: dict[str, object]) -> dict[str, object]:
                nonlocal active_reads, max_reads, active_writes, max_writes
                op = req.get("op")
                if op == "read":
                    with counter_lock:
                        active_reads += 1
                        max_reads = max(max_reads, active_reads)
                    try:
                        read_barrier.wait()
                        return {"ok": True, "op": "read"}
                    finally:
                        with counter_lock:
                            active_reads -= 1
                with counter_lock:
                    active_writes += 1
                    max_writes = max(max_writes, active_writes)
                try:
                    time.sleep(0.05)
                    return {"ok": True, "op": op}
                finally:
                    with counter_lock:
                        active_writes -= 1

            server = ControlRpcServer(
                sock_path,
                handle,
                max_workers=4,
                concurrent_predicate=lambda req: req.get("op") == "read",
            ).start()
            try:
                client = ControlRpcClient()
                read_results: list[dict[str, object]] = []
                write_results: list[dict[str, object]] = []

                def do_read() -> None:
                    read_results.append(
                        client.request(sock_path, {"op": "read"}, timeout_seconds=3.0)
                    )

                def do_write(idx: int) -> None:
                    write_results.append(
                        client.request(sock_path, {"op": "write", "i": idx}, timeout_seconds=3.0)
                    )

                threads = [
                    threading.Thread(target=do_read),
                    threading.Thread(target=do_read),
                    threading.Thread(target=do_write, args=(1,)),
                    threading.Thread(target=do_write, args=(2,)),
                ]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=5)
                self.assertEqual(len(read_results), 2)
                self.assertEqual(len(write_results), 2)
                self.assertEqual(max_reads, 2)
                self.assertEqual(max_writes, 1)
            finally:
                server.shutdown(timeout=2.0)

    def test_slowloris_and_framing_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            sock_path = Path(directory) / "rpc.sock"
            server = ControlRpcServer(
                sock_path,
                lambda req: {"ok": True, "echo": req},
                max_request_bytes=128,
                request_timeout_seconds=0.15,
            ).start()
            try:
                # 1-byte trickle slowloris should hit cumulative server deadline.
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as raw_sock:
                    raw_sock.settimeout(1.0)
                    raw_sock.connect(str(sock_path))
                    closed = False
                    for byte in b'{"op":"ping"' * 10:
                        try:
                            raw_sock.sendall(bytes([byte]))
                            time.sleep(0.03)
                        except OSError:
                            closed = True
                            break
                    if not closed:
                        tail = raw_sock.recv(1024)
                        self.assertEqual(tail, b"")

                # EOF without newline returns error response from server.
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as no_nl:
                    no_nl.settimeout(1.0)
                    no_nl.connect(str(sock_path))
                    no_nl.sendall(b'{"op":"ping"}')
                    no_nl.shutdown(socket.SHUT_WR)
                    payload = b""
                    while not payload.endswith(b"\n"):
                        chunk = no_nl.recv(1024)
                        if not chunk:
                            break
                        payload += chunk
                    self.assertTrue(payload.endswith(b"\n"))
                    response = json.loads(payload)
                    self.assertFalse(response["ok"])
                    self.assertEqual(response["error"]["code"], "invalid_request")

                # Client cumulative timeout against slow server.
                slow_sock = Path(directory) / "slow.sock"
                slow_server = ControlRpcServer(
                    slow_sock,
                    lambda req: (time.sleep(0.3), {"ok": True})[1],
                    request_timeout_seconds=2.0,
                ).start()
                try:
                    client = ControlRpcClient()
                    with self.assertRaises(RpcTimeoutError):
                        client.request(slow_sock, {"op": "slow"}, timeout_seconds=0.1)
                finally:
                    slow_server.shutdown(timeout=1.0)

                # Client rejects oversized request.
                with self.assertRaises(RpcFramingError):
                    ControlRpcClient(max_request_bytes=32).request(
                        sock_path, {"op": "x" * 64}, timeout_seconds=1.0
                    )
            finally:
                server.shutdown(timeout=2.0)

    def test_stdio_stream_drains_oversized_line_cleanly(self) -> None:
        stream = BytesIO(b'{"big":"' + b"a" * 200 + b'"}\n{"ok":1}\n')
        with self.assertRaises(RpcFramingError):
            read_bounded_ndjson_stream(stream, max_bytes=64)
        self.assertEqual(read_bounded_ndjson_stream(stream, max_bytes=64), {"ok": 1})
        self.assertIsNone(read_bounded_ndjson_stream(stream, max_bytes=64))

        no_nl_stream = BytesIO(b'{"ok":1}')
        with self.assertRaises(RpcFramingError):
            read_bounded_ndjson_stream(no_nl_stream, max_bytes=64)

    def test_shutdown_drains_active_worker(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            sock_path = Path(directory) / "drain.sock"
            entered = threading.Event()
            release = threading.Event()
            finished = threading.Event()

            def handle(_req: dict[str, object]) -> dict[str, object]:
                entered.set()
                release.wait(2.0)
                finished.set()
                return {"ok": True}

            server = ControlRpcServer(sock_path, handle, max_workers=2).start()
            client_result: list[dict[str, object]] = []

            def run_client() -> None:
                client_result.append(
                    ControlRpcClient().request(sock_path, {"op": "work"}, timeout_seconds=3.0)
                )

            worker = threading.Thread(target=run_client)
            worker.start()
            self.assertTrue(entered.wait(2.0))
            release_thread = threading.Thread(target=lambda: (time.sleep(0.05), release.set()))
            release_thread.start()
            server.shutdown(timeout=2.0)
            worker.join(timeout=2.0)
            release_thread.join(timeout=2.0)
            self.assertTrue(finished.is_set())
            self.assertEqual(client_result, [{"ok": True}])


class ConfigAndDeviceClientTest(unittest.TestCase):
    def test_production_config_parity_with_daemon(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = _sample_bootstrap_dict(root)
            cfg_path = root / "device.json"
            cfg_path.write_text(json.dumps(payload), encoding="utf-8")

            core_cfg = load_bootstrap_config(cfg_path)
            daemon_cfg = daemon.production_config_from_value(payload)
            self.assertIsInstance(core_cfg, ProductionConfig)
            self.assertIsInstance(core_cfg.recording_encoding, RecordingEncoding)
            self.assertEqual(core_cfg.host, daemon_cfg.host)
            self.assertEqual(core_cfg.port, daemon_cfg.port)
            self.assertEqual(core_cfg.device_id, daemon_cfg.device_id)
            self.assertEqual(core_cfg.video_bitrate_kbps, daemon_cfg.video_bitrate_kbps)
            self.assertEqual(
                production_config_from_value(payload).recording_encoding.manifest(),
                daemon_cfg.recording_encoding.manifest(),
            )

            # Corrupt settings.sqlite3 is completely ignored by load_bootstrap_config.
            state_root = root / "state"
            state_root.mkdir()
            (state_root / "settings.sqlite3").write_bytes(b"corrupt sqlite bytes")
            self.assertEqual(load_bootstrap_config(cfg_path).port, 8080)

    def test_read_bearer_token_validation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            token_path = Path(directory) / "customer.token"
            token_path.write_text("valid-token-1234\n", encoding="ascii")
            token_path.chmod(0o640)
            self.assertEqual(read_bearer_token(token_path), "valid-token-1234")

            token_path.chmod(0o644)
            with self.assertRaises(ProductionConfigError):
                read_bearer_token(token_path)

    def test_load_bootstrap_endpoint_host_mapping_and_no_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state_root = root / "state"
            state_root.mkdir()
            (state_root / "settings.sqlite3").write_bytes(b"not-a-sqlite-db")

            for raw_host, expected_host, expected_url in (
                ("0.0.0.0", "127.0.0.1", "http://127.0.0.1:8080"),
                ("::", "::1", "http://[::1]:8080"),
                ("10.42.0.1", "10.42.0.1", "http://10.42.0.1:8080"),
                ("::1", "::1", "http://[::1]:8080"),
            ):
                payload = _sample_bootstrap_dict(root)
                payload["listen"] = {"host": raw_host, "port": 8080}
                cfg = root / "device.json"
                cfg.write_text(json.dumps(payload), encoding="utf-8")
                endpoint = load_bootstrap_endpoint(cfg)
                self.assertIsInstance(endpoint, BootstrapEndpoint)
                self.assertEqual(endpoint.scheme, "http")
                self.assertEqual(endpoint.host, expected_host)
                self.assertEqual(endpoint.port, 8080)
                self.assertEqual(endpoint.url, expected_url)
                self.assertIsNone(endpoint.bearer_token)
                self.assertIsNone(endpoint.ssl_context)

    def test_load_bootstrap_endpoint_tls_partial_chain_with_self_signed_ca_false(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cert_path = root / "device.crt"
            key_path = root / "device.key"
            write_self_signed_tls(
                cert_path,
                key_path,
                common_name="127.0.0.1",
                ip_addresses=("127.0.0.1",),
                cert_mode=0o644,
                key_mode=0o640,
            )

            token_path = root / "customer.token"
            token_path.write_text("customer-secret-token\n", encoding="ascii")
            token_path.chmod(0o640)

            class _Handler(http.server.BaseHTTPRequestHandler):
                def do_GET(self) -> None:
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b'{"ok":true}')

                def log_message(self, format: str, *args: object) -> None:
                    return

            server = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
            server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server_ctx.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
            server.socket = server_ctx.wrap_socket(server.socket, server_side=True)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                port = server.server_address[1]
                payload = _sample_bootstrap_dict(root, profile="customer")
                payload["listen"] = {"host": "0.0.0.0", "port": port}
                cfg = root / "device.json"
                cfg.write_text(json.dumps(payload), encoding="utf-8")

                endpoint = load_bootstrap_endpoint(cfg)
                self.assertEqual(endpoint.scheme, "https")
                self.assertEqual(endpoint.url, f"https://127.0.0.1:{port}")
                self.assertEqual(endpoint.bearer_token, "customer-secret-token")
                self.assertIsNotNone(endpoint.ssl_context)
                assert endpoint.ssl_context is not None
                partial_chain = getattr(ssl, "VERIFY_X509_PARTIAL_CHAIN", 0)
                if partial_chain:
                    self.assertTrue(endpoint.ssl_context.verify_flags & partial_chain)

                req = urllib.request.Request(
                    f"{endpoint.url}/health",
                    headers={"Authorization": f"Bearer {endpoint.bearer_token}"},
                )
                with urllib.request.urlopen(req, context=endpoint.ssl_context, timeout=5) as resp:
                    self.assertEqual(resp.status, 200)
                    self.assertEqual(json.loads(resp.read()), {"ok": True})
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

    def test_core_errors_config_use_stdlib_only(self) -> None:
        code = """
import sys
for mod in ("jsonschema", "PIL", "zeroconf", "zstandard", "rp_ylx._native"):
    sys.modules[mod] = None
import rp_ylx.errors
import rp_ylx.config
import rp_ylx.core
import rp_ylx.core.board
import rp_ylx.core.device_client
import rp_ylx.core.fs
import rp_ylx.core.ids
import rp_ylx.core.locks
import rp_ylx.core.rpc
import rp_ylx.core.time
print("stdlib-only-ok")
"""
        env = dict(os.environ)
        env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            env=env,
            check=True,
            timeout=15,
        )
        self.assertEqual(result.stdout.strip(), "stdlib-only-ok")


if __name__ == "__main__":
    unittest.main()
