"""Wave 6 regression tests covering API/TLS/SSE, deployment/scripts, and Hobot C guards."""

from __future__ import annotations

import fcntl
import hashlib
import http.client
import json
import os
import re
import shutil
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from copy import deepcopy
from pathlib import Path
from urllib.request import Request, urlopen

from rp_ylx.api import (
    Principal,
    SecurityPolicy,
    create_gateway_server,
)
from rp_ylx.api.locked_io import LockedBytes, _FileIdentity
from rp_ylx.api.streams import EventReplayBuffer
from rp_ylx.deploy_pkg.storage import require_state_compatible_release
from rp_ylx.deployment import DeploymentError
from rp_ylx.recording.session_catalog import _TrackedRepresentation
from tests.testkit import (
    CAPTURE_STATUS,
    SESSION_ID,
    DeploymentHarness,
    DeviceProvider,
    GatewayHarness,
    write_self_signed_tls,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _make_self_signed_tls(directory: Path) -> tuple[Path, Path]:
    return write_self_signed_tls(
        directory / "device.crt",
        directory / "device.key",
        common_name="localhost",
        dns_names=("localhost",),
        ip_addresses=("127.0.0.1",),
        key_mode=0o600,
    )


def _client_ssl_context(cert: Path) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(cafile=str(cert))
    return ctx


def _start_tls_gateway(
    cert: Path,
    key: Path,
    provider: object,
    policy: SecurityPolicy,
    *,
    do_handshake_on_connect: bool = False,
    request_timeout_seconds: float = 5.0,
) -> tuple[object, threading.Thread]:
    server = create_gateway_server(
        "127.0.0.1",
        0,
        provider,  # type: ignore[arg-type]
        security=policy,
        external_scheme="https",
        request_timeout_seconds=request_timeout_seconds,
    )
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(certfile=str(cert), keyfile=str(key))
    server.socket = ctx.wrap_socket(
        server.socket,
        server_side=True,
        do_handshake_on_connect=do_handshake_on_connect,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


class ApiAndStreamingRegressionTests(unittest.TestCase):
    def test_a01_https_artifact_download_decrypts_without_sendfile_on_sslsocket(self) -> None:
        payload = (b"encrypted-over-tls-" * 2048)[:32768]
        artifact_id = hashlib.sha256(payload).hexdigest()
        send_to_calls: list[tuple[int, int, int | None]] = []

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cert, key = _make_self_signed_tls(root)
            artifact_path = root / "artifact.bin"
            artifact_path.write_bytes(payload)

            class TlsArtifactProvider(DeviceProvider):
                def artifact_io_state(self) -> str | None:
                    return None

                def open_verified_artifact(
                    self, session_id: str, req_artifact_id: str, api_version: str
                ) -> LockedBytes:
                    assert session_id == SESSION_ID
                    assert req_artifact_id == artifact_id
                    assert api_version == "v4"
                    fd = os.open(artifact_path, os.O_RDONLY | os.O_CLOEXEC)
                    rep = LockedBytes(
                        fd,
                        [fd],
                        identity=_FileIdentity.read(fd),
                        etag=f'"{artifact_id}"',
                        content_type="application/octet-stream",
                    )

                    def forbid_send_to(
                        out_fd: int, offset: int = 0, length: int | None = None
                    ) -> int:
                        send_to_calls.append((out_fd, offset, length))
                        raise AssertionError("raw-fd send_to/sendfile must not run on SSLSocket")

                    rep.send_to = forbid_send_to  # type: ignore[method-assign]
                    return rep

            policy = SecurityPolicy.customer(
                tokens={
                    "reader-token": Principal(
                        "reader",
                        permissions={
                            "getDevice": None,
                            "getSessionArtifact": {SESSION_ID},
                        },
                    )
                },
                allowed_origins={"https://127.0.0.1"},
                csrf_token="csrf",
            )
            server, thread = _start_tls_gateway(cert, key, TlsArtifactProvider(), policy)
            try:
                client_ctx = _client_ssl_context(cert)
                req = Request(
                    f"https://127.0.0.1:{server.server_port}/api/v4/sessions/{SESSION_ID}/artifacts/{artifact_id}",
                    headers={"Authorization": "Bearer reader-token"},
                )
                with urlopen(req, context=client_ctx, timeout=5) as resp:
                    self.assertEqual(resp.status, 200)
                    body = resp.read()
                self.assertEqual(body, payload)
                self.assertEqual(send_to_calls, [])
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

    def test_a02_stalled_tls_client_does_not_block_other_https_requests(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cert, key = _make_self_signed_tls(root)
            policy = SecurityPolicy.customer(
                tokens={"reader-token": Principal("reader", permissions={"getDevice": None})},
                allowed_origins={"https://127.0.0.1"},
                csrf_token="csrf",
            )
            server, thread = _start_tls_gateway(
                cert,
                key,
                DeviceProvider(),
                policy,
                do_handshake_on_connect=False,
                request_timeout_seconds=0.35,
            )
            try:
                client_ctx = _client_ssl_context(cert)
                with socket.create_connection(
                    ("127.0.0.1", server.server_port), timeout=2
                ) as stalled:
                    t0 = time.monotonic()
                    req = Request(
                        f"https://127.0.0.1:{server.server_port}/api/v4/device",
                        headers={"Authorization": "Bearer reader-token"},
                    )
                    with urlopen(req, context=client_ctx, timeout=2) as resp:
                        self.assertEqual(resp.status, 200)
                        data = json.loads(resp.read())
                    elapsed = time.monotonic() - t0
                    self.assertEqual(data["schema"], "ylx.device.v4")
                    self.assertLess(elapsed, 0.3)

                    stalled.settimeout(1.5)
                    try:
                        tail = stalled.recv(16)
                        self.assertEqual(tail, b"")
                    except ConnectionResetError:
                        pass
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

    def test_a03_lab_profile_rejects_dns_rebinding_host_and_origin_allows_local(self) -> None:
        def raw_http(
            port: int, method: str, path: str, headers: dict[str, str], body: bytes = b""
        ) -> tuple[int, bytes]:
            with socket.create_connection(("127.0.0.1", port), timeout=2) as sock:
                lines = [f"{method} {path} HTTP/1.1"]
                for k, v in headers.items():
                    lines.append(f"{k}: {v}")
                lines.append("Connection: close")
                req_bytes = ("\r\n".join(lines) + "\r\n\r\n").encode("ascii") + body
                sock.sendall(req_bytes)
                resp = http.client.HTTPResponse(sock, method=method)
                resp.begin()
                return resp.status, resp.read()

        policy = SecurityPolicy.lab(allowed_operations={"getDevice", "startCapture"})
        with GatewayHarness(security=policy).setUp() as harness:
            port = harness.server.server_port
            body = json.dumps(
                {
                    "schema": "ylx.capture-start.v2",
                    "mode": "production",
                    "display_name": "rebinding",
                    "take": {"kind": "new"},
                }
            ).encode()

            status, raw = raw_http(
                port,
                "POST",
                "/api/v4/capture/start",
                {
                    "Host": f"evil.tld:{port}",
                    "Content-Type": "application/json",
                    "Content-Length": str(len(body)),
                    "Idempotency-Key": "rebind-host",
                },
                body,
            )
            self.assertEqual(status, 403)
            self.assertEqual(json.loads(raw)["error"]["code"], "origin_forbidden")

            status, raw = raw_http(
                port,
                "POST",
                "/api/v4/capture/start",
                {
                    "Host": f"127.0.0.1:{port}",
                    "Origin": "http://evil.tld",
                    "Content-Type": "application/json",
                    "Content-Length": str(len(body)),
                    "Idempotency-Key": "rebind-origin",
                },
                body,
            )
            self.assertEqual(status, 403)
            self.assertEqual(json.loads(raw)["error"]["code"], "origin_forbidden")

            for allowed_host in (
                f"127.0.0.1:{port}",
                f"rp-ylx.local:{port}",
                f"10.42.0.1:{port}",
            ):
                with self.subTest(host=allowed_host):
                    status, raw = raw_http(port, "GET", "/api/v4/device", {"Host": allowed_host})
                    self.assertEqual(status, 200, raw)

    def test_a04_huge_last_event_id_does_not_inflate_network_delivery_counter(self) -> None:
        policy = SecurityPolicy.lab(allowed_operations={"streamNetworkEvents"})
        with GatewayHarness(security=policy).setUp() as harness:
            port = harness.server.server_port
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
            try:
                conn.request("GET", "/api/v4/network/events", headers={"Last-Event-ID": "9" * 128})
                resp = conn.getresponse()
                self.assertEqual(resp.status, 200)
                first_event = harness.read_sse_event(resp)
                delivery_id, _ = harness.decode_sse_event(first_event)
            finally:
                conn.close()

            self.assertLess(len(str(delivery_id)), 128)
            self.assertLess(harness.server.network_event_buffer._next_delivery_id, 10**128)

            conn2 = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
            try:
                conn2.request(
                    "GET", "/api/v4/network/events", headers={"Last-Event-ID": str(delivery_id)}
                )
                resp2 = conn2.getresponse()
                self.assertEqual(resp2.status, 200)
            finally:
                conn2.close()

    def test_a05_options_sessions_delete_and_clock_check_permission(self) -> None:
        without_inspect = Principal(
            "clock-basic",
            permissions={"getDeviceClock": None, "syncDeviceClock": None},
        )
        with_inspect = Principal(
            "clock-full",
            permissions={
                "getDeviceClock": None,
                "inspectDeviceClock": None,
                "syncDeviceClock": None,
            },
        )
        policy = SecurityPolicy.customer(
            tokens={"basic": without_inspect, "full": with_inspect},
            allowed_origins={"http://127.0.0.1:4173"},
            csrf_token="csrf",
        )
        provider = DeviceProvider()
        provider.clock_inspect = lambda _principal_id: {
            "schema": "ylx.clock-status.v2",
            "source": "unsynchronized",
            "unix_time_ms": 946684800000,
            "challenge": "a" * 32,
            "expires_in_ms": 5000,
            "applied": False,
            "boot_id": "a4578322-241e-4b9c-bb54-a860d5a2aa37",
            "synchronized_at_ms": None,
            "age_seconds": None,
        }
        with GatewayHarness(provider=provider, security=policy).setUp() as harness:
            status, _, headers = harness.request("/api/v4/sessions/delete", method="OPTIONS")
            self.assertEqual(status, 204)
            self.assertEqual(headers["Allow"], "POST, OPTIONS")

            status, _, _ = harness.request("/api/v4/clock/check", token="basic")
            self.assertEqual(status, 403)

            status, body, _ = harness.request("/api/v4/clock/check", token="full")
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(body)["schema"], "ylx.clock-status.v2")

    def test_a07_sse_resync_does_not_broadcast_duplicate_snapshot_to_connected_clients(
        self,
    ) -> None:
        buf = EventReplayBuffer(capacity=16)

        def snapshot_event(revision: int = 1) -> dict[str, object]:
            snap = deepcopy(CAPTURE_STATUS["snapshot"])
            assert isinstance(snap, dict)
            return {
                "authority_epoch": "4fa85f64-5717-4562-b3fc-2c963f66afa6",
                "source_revision": revision,
                "occurred_at": "2026-09-05T00:00:00Z",
                "type": "snapshot",
                "session_id": None,
                "data": snap,
            }

        client1_init = buf.replay(None, api_version="v4", snapshot=lambda: snapshot_event(1))
        self.assertEqual(len(client1_init), 1)
        cursor1 = client1_init[0].delivery_id

        received: list[tuple[int, str]] = []
        cancel = threading.Event()
        started = threading.Event()

        def client1_waiter() -> None:
            started.set()
            events = buf.wait_after(
                cursor1,
                2.0,
                api_version="v4",
                snapshot=lambda: snapshot_event(99),
                cancel_event=cancel,
            )
            received.extend((ev.delivery_id, str(ev.source_event["type"])) for ev in events)

        waiter = threading.Thread(target=client1_waiter, daemon=True)
        waiter.start()
        self.assertTrue(started.wait(1))
        time.sleep(0.05)

        client2_resync = buf.replay("999999", api_version="v4", snapshot=lambda: snapshot_event(1))
        self.assertEqual(len(client2_resync), 1)
        self.assertEqual(client2_resync[0].source_event["type"], "snapshot")
        time.sleep(0.05)
        self.assertEqual(received, [])

        published = snapshot_event(2)
        buf.publish(published)
        waiter.join(timeout=2)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0][1], "snapshot")

    def test_a09_chunked_send_to_over_16kib_marks_full_read_and_served(self) -> None:
        payload = b"X" * 65536
        digest = hashlib.sha256(payload).hexdigest()
        outcomes: list[tuple[str, int]] = []
        released = False

        def release() -> None:
            nonlocal released
            released = True

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "chunked.bin"
            path.write_bytes(payload)
            fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
            rep = LockedBytes(
                fd,
                [fd],
                identity=_FileIdentity.read(fd),
                etag=f'"{digest}"',
                content_type="application/octet-stream",
            )
            tracked = _TrackedRepresentation(
                rep, release, lambda status, count: outcomes.append((status, count))
            )
            pipe_r, pipe_w = os.pipe()
            try:
                transferred = bytearray()

                def drain_pipe() -> None:
                    while len(transferred) < len(payload):
                        chunk = os.read(pipe_r, 8192)
                        if not chunk:
                            break
                        transferred.extend(chunk)

                reader = threading.Thread(target=drain_pipe, daemon=True)
                reader.start()
                with tracked:
                    for offset in range(0, len(payload), 16384):
                        sent = tracked.send_to(pipe_w, offset, 16384)
                        self.assertEqual(sent, 16384)
                reader.join(timeout=2)
                self.assertEqual(bytes(transferred), payload)
            finally:
                os.close(pipe_r)
                os.close(pipe_w)

        self.assertTrue(tracked._full_read)
        self.assertTrue(released)
        self.assertEqual(outcomes, [("served", 65536)])


class DeploymentAndScriptRegressionTests(unittest.TestCase):
    @staticmethod
    def _write_stub_bin(directory: Path, name: str, body: str) -> None:
        path = directory / name
        path.write_text("#!/bin/sh\n" + body, encoding="utf-8")
        path.chmod(0o755)

    def test_d01_device_login_preserves_password_hash_when_user_exists(self) -> None:
        script = REPO_ROOT / "src/rp_ylx/deploy/rp-ylx-device-login"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            log = root / "calls.log"
            for cmd in ("useradd", "usermod", "chage"):
                self._write_stub_bin(bin_dir, cmd, f'printf "{cmd}: %s\\n" "$*" >> "{log}"\n')

            env = {**os.environ, "PATH": f"{bin_dir}:/usr/bin:/bin"}

            self._write_stub_bin(bin_dir, "getent", "exit 0\n")
            subprocess.run(["sh", str(script)], env=env, check=True)
            existing_calls = log.read_text(encoding="utf-8").splitlines()
            self.assertEqual(
                existing_calls,
                ["usermod: --shell /bin/bash OpenAria"],
            )

            log.unlink()
            self._write_stub_bin(bin_dir, "getent", "exit 2\n")
            subprocess.run(["sh", str(script)], env=env, check=True)
            created_calls = log.read_text(encoding="utf-8").splitlines()
            self.assertTrue(any(line.startswith("useradd:") for line in created_calls))
            self.assertTrue(any("--password" in line for line in created_calls))
            self.assertTrue(any(line.startswith("chage:") for line in created_calls))

    def test_d02_wifi_watchdog_checks_maintenance_lock_and_rotates_diagnostics(self) -> None:
        script = REPO_ROOT / "src/rp_ylx/deploy/rp-ylx-wifi-watchdog"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            rp_ylx_called = root / "rp_ylx_called"
            self._write_stub_bin(bin_dir, "logger", "exit 0\n")
            self._write_stub_bin(
                bin_dir, "rp-ylx-stub", f'touch "{rp_ylx_called}"\nprintf "hotspot\\n"\n'
            )

            state_dir = root / "state"
            run_dir = root / "run"
            net_lock = root / "network-operation.lock"
            net_lock.touch()

            base_env = {
                **os.environ,
                "PATH": f"{bin_dir}:/usr/bin:/bin",
                "STATE_DIRECTORY": str(state_dir),
                "RUNTIME_DIRECTORY": str(run_dir),
                "RP_YLX_NETWORK_OPERATION_LOCK_PATH": str(net_lock),
                "RP_YLX_COMMAND": str(bin_dir / "rp-ylx-stub"),
            }

            symlink_lock = root / "symlink.lock"
            symlink_lock.symlink_to(root / "target.lock")
            res_sym = subprocess.run(
                ["sh", str(script)],
                env={**base_env, "OPENARIA_MAINTENANCE_LOCK": str(symlink_lock)},
                capture_output=True,
                text=True,
                check=True,
            )
            self.assertIn("unsafe symlink", res_sym.stdout)
            self.assertFalse(rp_ylx_called.exists())

            real_lock = root / "maintenance.lock"
            real_lock.touch()
            fd = os.open(real_lock, os.O_RDWR)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                res_held = subprocess.run(
                    ["sh", str(script)],
                    env={**base_env, "OPENARIA_MAINTENANCE_LOCK": str(real_lock)},
                    capture_output=True,
                    text=True,
                    check=True,
                )
            finally:
                os.close(fd)
            self.assertIn("maintenance operation is active", res_held.stdout)
            self.assertFalse(rp_ylx_called.exists())

            diag_dir = root / "diag"
            diag_dir.mkdir()
            for idx in range(15):
                p = diag_dir / f"diagnostic-20260101T0000{idx:02d}Z.log"
                p.write_text(f"log-{idx}\n", encoding="utf-8")
                os.utime(p, (1_700_000_000 + idx, 1_700_000_000 + idx))

            rotate_match = re.search(
                r"^rotate_diagnostics\(\)\n\{.*?\n\}",
                script.read_text(encoding="utf-8"),
                flags=re.MULTILINE | re.DOTALL,
            )
            self.assertIsNotNone(rotate_match)
            assert rotate_match is not None
            rotate_fn = rotate_match.group(0)
            subprocess.run(
                [
                    "sh",
                    "-eu",
                    "-c",
                    f'STATE_DIRECTORY="{diag_dir}"\nDIAGNOSTIC_LOG_LIMIT=10\n{rotate_fn}\nrotate_diagnostics',
                ],
                check=True,
            )
            kept = sorted(p.name for p in diag_dir.glob("diagnostic-*.log"))
            self.assertEqual(len(kept), 10)
            self.assertEqual(kept[0], "diagnostic-20260101T000005Z.log")
            self.assertEqual(kept[-1], "diagnostic-20260101T000014Z.log")

    def test_d03_deployment_runs_standalone_without_rp_ylx_persistence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            isolated = Path(directory) / "standalone"
            isolated.mkdir()
            shutil.copy2(REPO_ROOT / "src/rp_ylx/deployment.py", isolated / "deployment.py")
            shutil.copytree(REPO_ROOT / "src/rp_ylx/deploy_pkg", isolated / "deploy_pkg")

            roots = Path(directory) / "roots"
            code = (
                f"import pathlib, sys; sys.path.insert(0, {str(isolated)!r}); "
                "import deployment; "
                "assert 'rp_ylx.persistence' not in sys.modules; "
                f"r = pathlib.Path({str(roots)!r}); "
                "m = deployment.ReleaseManager("
                "install_root=r / 'opt/rp-ylx', "
                "config_root=r / 'etc/rp-ylx', "
                "state_root=r / 'var/lib/rp-ylx', "
                "system_root=r"
                "); "
                "assert m.status()['installed'] is False; "
                "print('ok')"
            )
            res = subprocess.run(
                [sys.executable, "-I", "-c", code],
                cwd=str(isolated),
                capture_output=True,
                text=True,
                check=True,
                timeout=15,
            )
            self.assertEqual(res.stdout.strip(), "ok")

    def test_d04_release_manager_rejects_exfat_downgrade(self) -> None:
        with DeploymentHarness().setUp() as harness:
            manager = harness.manager()
            manager.install(harness.bundle("a"))
            data_vol_cfg = harness.root / "etc/default/rp-ylx-data-volume"
            data_vol_cfg.parent.mkdir(parents=True, exist_ok=True)
            data_vol_cfg.write_text("RP_YLX_DATA_VOLUME_MODE=exfat\n", encoding="utf-8")

            old_commit = harness.commit("z")
            pkg_root = manager.install_root / "releases" / old_commit / "site-packages/rp_ylx"
            (pkg_root / "recording").mkdir(parents=True)
            (pkg_root / "persistence.py").write_text("# sqlite\n", encoding="utf-8")
            (pkg_root / "recording/volume.py").write_text("# legacy statvfs\n", encoding="utf-8")
            (pkg_root / "deployment.py").write_text("# legacy loop only\n", encoding="utf-8")

            with self.assertRaises(DeploymentError) as raised:
                require_state_compatible_release(
                    manager.install_root,
                    manager.state_root,
                    harness.root,
                    old_commit,
                )
            self.assertEqual(raised.exception.code, "storage_schema_incompatible")

    def test_d05_d06_data_volume_syntax_and_loop_atomic_temp_and_mount_options(self) -> None:
        script = REPO_ROOT / "src/rp_ylx/deploy/rp-ylx-data-volume"
        subprocess.run(["bash", "-n", str(script)], check=True)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            calls = root / "calls.log"
            mounted_flag = root / "mounted"

            self._write_stub_bin(bin_dir, "getent", "exit 0\n")
            self._write_stub_bin(
                bin_dir,
                "install",
                'while [ "$#" -gt 0 ]; do\n'
                '  case "$1" in -d|-o|-g|-m) shift ;; *) mkdir -p "$1" ;; esac\n'
                "  shift\n"
                "done\n",
            )
            self._write_stub_bin(
                bin_dir,
                "fallocate",
                'printf "fallocate:%s\\n" "$*" >> "' + str(calls) + '"\n: > "$3"\n',
            )
            self._write_stub_bin(
                bin_dir,
                "mkfs.ext4",
                'printf "mkfs.ext4:%s\\n" "$*" >> "' + str(calls) + '"\n',
            )
            self._write_stub_bin(
                bin_dir, "sync", 'printf "sync:%s\\n" "$*" >> "' + str(calls) + '"\n'
            )
            self._write_stub_bin(
                bin_dir,
                "mountpoint",
                f'if [ -f "{mounted_flag}" ]; then exit 0; else exit 1; fi\n',
            )
            self._write_stub_bin(bin_dir, "e2fsck", "exit 0\n")
            self._write_stub_bin(
                bin_dir,
                "mount",
                f'printf "mount:%s\\n" "$*" >> "{calls}"\ntouch "{mounted_flag}"\n',
            )
            self._write_stub_bin(
                bin_dir,
                "findmnt",
                'case "$*" in *FSTYPE*) echo ext4 ;; *SOURCE*) echo /dev/loop9 ;; '
                "*OPTIONS*) echo rw,nosuid,nodev,noexec,noatime ;; *) echo ok ;; esac\n",
            )
            image_path = root / "vol/data-volume.img"
            mount_path = root / "data"
            mount_path.mkdir()
            (mount_path / ".ylx-volume.json").write_text("{}", encoding="utf-8")
            self._write_stub_bin(bin_dir, "losetup", f'echo "{image_path}"\n')
            self._write_stub_bin(bin_dir, "chown", "exit 0\n")
            self._write_stub_bin(bin_dir, "chmod", "exit 0\n")
            self._write_stub_bin(bin_dir, "find", "exit 0\n")
            self._write_stub_bin(bin_dir, "df", "exit 0\n")
            self._write_stub_bin(bin_dir, "runuser", "exit 0\n")

            env = {
                **os.environ,
                "PATH": f"{bin_dir}:/usr/bin:/bin",
                "RP_YLX_DATA_VOLUME_MODE": "loop",
                "RP_YLX_DATA_VOLUME_SIZE": "64M",
                "RP_YLX_DATA_VOLUME_IMAGE": str(image_path),
                "RP_YLX_DATA_VOLUME_MOUNT": str(mount_path),
                "RP_YLX_DATA_VOLUME_OWNER": f"{os.getuid()}:{os.getgid()}",
            }
            subprocess.run(["bash", str(script), "ensure"], env=env, check=True)
            log_text = calls.read_text(encoding="utf-8")
            self.assertIn(f"{image_path}.tmp", log_text)
            self.assertTrue(image_path.is_file())
            self.assertFalse(Path(f"{image_path}.tmp").exists())
            self.assertIn("mount:-o loop,rw,nosuid,nodev,noexec,noatime", log_text)


class HobotNativeRegressionTests(unittest.TestCase):
    @staticmethod
    def _compiler() -> str:
        compiler = shutil.which("gcc") or shutil.which("cc")
        if compiler is None:
            raise unittest.SkipTest("C compiler unavailable")
        return compiler

    def test_n06_ylx_stereo_pipeline_guards(self) -> None:
        compiler = self._compiler()
        source = (REPO_ROOT / "src/rp_ylx/hobot/ylx_stereo_pipeline.c").read_text(encoding="utf-8")
        helpers = source[
            source.index("static uint32_t read_u32(") : source.index(
                "/* --------------------------------------------------------------- muxer"
            )
        ]
        segment_open_fn = source[
            source.index("static int segment_open(") : source.index("static void ledger_record(")
        ]
        split_eye_fn = source[
            source.index("static void split_eye(") : source.index("static void *split_thread_main(")
        ]
        submit_fn = source[
            source.index("int ylx_pipeline_submit(") : source.index("int ylx_pipeline_finish(")
        ]

        c_code = (
            r"""
#define _GNU_SOURCE
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define YLX_EYES 2
#define ERROR_LEN 256
#define PATH_LEN 512
#define MAX_PENDING_SEGMENTS 32u
#define DECODER_INPUT_TIMEOUT_MS 200
#define MC_VIDEO_STREAM_BUFFER 2
#define MC_PIXEL_FORMAT_NV12 3
#define MC_PIXEL_FORMAT_YUV420P 4
#define MEDIA_MUXER_OUTPUT_FORMAT_MP4 1
#define MEDIA_CODEC_ID_H264 2
#define MEDIA_CODEC_ID_H265 1

typedef struct { int unused; } media_codec_context_t;
typedef struct { char *output_file_name; int output_format; } media_muxer_context_t;
typedef struct {
    int codec_id, numerator, denominator;
    struct { int width, height; uint32_t frame_rate, bit_rate; int pix_fmt; } video_params;
} mx_stream_params_t;
typedef struct {
    int type;
    struct { uint32_t size; int stream_end; unsigned char *vir_ptr; } vstream_buf;
    struct { int width, height, pix_fmt, stride; uint32_t size; uint8_t *vir_ptr[2]; } vframe_buf;
} media_codec_buffer_t;

typedef struct segment {
    struct segment *next;
    media_muxer_context_t muxer;
    int eye, index;
    char absolute[PATH_LEN], relative[PATH_LEN];
    unsigned long long start_frame, end_frame, finalize_ns;
} segment_t;

typedef struct ylx_pipeline ylx_pipeline_t;
typedef struct { ylx_pipeline_t *pipeline; int eye; segment_t *current; } eye_t;
struct ylx_pipeline {
    struct { int fps, height, bitrate_kbps, hevc; const char *out_dir, *path_prefix; } config;
    int eye_width;
    media_codec_context_t decoder;
    eye_t eyes[2];
    atomic_uint pending_segments;
    atomic_int failed;
    atomic_ullong offered, submitted, drop_decoder_input;
    char error[ERROR_LEN];
};
static const char *const EYE_NAMES[2] = {"left", "right"};
static void fail(ylx_pipeline_t *p, const char *fmt, ...) {
    if (!p->error[0]) {
        va_list a; va_start(a, fmt);
        vsnprintf(p->error, sizeof(p->error), fmt, a);
        va_end(a);
    }
    atomic_store(&p->failed, 1);
}

static unsigned char g_in_buf[16];
static uint32_t g_last_queued_size = 9999u;
static int g_last_queued_stream_end = 9999;
static int g_mx_stop_calls = 0;

static int hb_mm_mc_dequeue_input_buffer(
    media_codec_context_t *c, media_codec_buffer_t *b, int t) {
    (void)c; (void)t;
    b->vstream_buf.size = sizeof(g_in_buf);
    b->vstream_buf.vir_ptr = g_in_buf;
    return 0;
}
static int hb_mm_mc_queue_input_buffer(
    media_codec_context_t *c, media_codec_buffer_t *b, int t) {
    (void)c; (void)t;
    g_last_queued_size = b->vstream_buf.size;
    g_last_queued_stream_end = b->vstream_buf.stream_end;
    return 0;
}
static int hb_mm_mx_get_default_context(media_muxer_context_t *m) {
    memset(m, 0, sizeof(*m)); return 0;
}
static int hb_mm_mx_initialize(media_muxer_context_t *m) { (void)m; return 0; }
static int hb_mm_mx_add_stream(media_muxer_context_t *m, mx_stream_params_t *p) {
    (void)m; (void)p; return 0;
}
static int hb_mm_mx_start(media_muxer_context_t *m) { (void)m; return -1; }
static int hb_mm_mx_stop(media_muxer_context_t *m) { (void)m; g_mx_stop_calls++; return 0; }
"""
            + helpers
            + segment_open_fn
            + split_eye_fn
            + submit_fn
            + r"""
int main(int argc, char **argv) {
    assert(argc == 2);
    ylx_pipeline_t p;
    memset(&p, 0, sizeof(p));
    p.eye_width = 8;
    p.config.fps = 30;
    p.config.height = 4;
    p.config.bitrate_kbps = 1000;
    p.config.out_dir = "/tmp";
    p.config.path_prefix = "";
    p.eyes[0].pipeline = &p;
    p.eyes[0].eye = 0;

    unsigned char big_jpeg[32];
    memset(big_jpeg, 0xaa, sizeof(big_jpeg));
    assert(ylx_pipeline_submit(&p, big_jpeg, sizeof(big_jpeg), 100) == -1);
    assert(g_last_queued_size == 0);
    assert(g_last_queued_stream_end == 0);

    atomic_store(&p.failed, 0);
    p.error[0] = '\0';
    assert(segment_open(&p, &p.eyes[0], 0, 0) == -1);
    assert(g_mx_stop_calls == 1);
    assert(atomic_load(&p.pending_segments) == 0);

    uint8_t src_y[128], src_uv[64], dst_y[64], dst_uv[32];
    media_codec_buffer_t src, dst;
    memset(&src, 0, sizeof(src));
    memset(&dst, 0, sizeof(dst));
    src.vframe_buf.pix_fmt = MC_PIXEL_FORMAT_YUV420P;
    src.vframe_buf.width = 16; src.vframe_buf.stride = 16;
    src.vframe_buf.vir_ptr[0] = src_y; src.vframe_buf.vir_ptr[1] = src_uv;
    dst.vframe_buf.stride = 8; dst.vframe_buf.size = 99;
    dst.vframe_buf.vir_ptr[0] = dst_y; dst.vframe_buf.vir_ptr[1] = dst_uv;
    split_eye(&src, &dst, 8, 4, 0);
    assert(dst.vframe_buf.size == 0);

    src.vframe_buf.pix_fmt = MC_PIXEL_FORMAT_NV12;
    dst.vframe_buf.size = 99;
    split_eye(&src, &dst, 8, 4, 12); /* x_offset > src_stride - eye_width */
    assert(dst.vframe_buf.size == 0);

    dst.vframe_buf.stride = 4; /* dst_stride < eye_width */
    dst.vframe_buf.size = 99;
    split_eye(&src, &dst, 8, 4, 0);
    assert(dst.vframe_buf.size == 0);

    char reason[128] = {0};
    assert(mp4_extend_durations(argv[1], 30, reason, sizeof(reason)) == 0);
    return 0;
}
"""
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            mp4 = root / "test.mp4"
            decoy = b"colr" + b"nclx" + b"\x00\x00\x00\x00\x00\x00\x00" + b"mvhd" + bytes(28)

            def box(kind: bytes, data: bytes) -> bytes:
                return struct.pack(">I", len(data) + 8) + kind + data

            mdat = box(b"mdat", decoy * 4)
            colr = box(b"colr", b"nclx" + struct.pack(">HHHB", 0, 0, 0, 0))
            stsd = box(b"stsd", bytes(4) + struct.pack(">I", 1) + box(b"avc1", bytes(78) + colr))
            mvhd = bytearray(28)
            struct.pack_into(">II", mvhd, 12, 1000, 1000)
            tkhd = bytearray(32)
            struct.pack_into(">I", tkhd, 20, 1000)
            elst = box(b"elst", bytes(4) + struct.pack(">III", 1, 1000, 0) + bytes(4))
            moov = box(
                b"moov",
                box(b"mvhd", bytes(mvhd))
                + box(
                    b"trak",
                    box(b"tkhd", bytes(tkhd))
                    + box(b"edts", elst)
                    + box(b"mdia", box(b"mdhd", bytes(mvhd)) + box(b"minf", box(b"stbl", stsd))),
                ),
            )
            mp4.write_bytes(mdat + moov)

            src_c = root / "n06.c"
            src_c.write_text(c_code, encoding="utf-8")
            binary = root / "n06"
            subprocess.run(
                [compiler, "-Wall", "-Wextra", "-Werror", str(src_c), "-o", str(binary)],
                check=True,
                capture_output=True,
                text=True,
            )
            subprocess.run([str(binary), str(mp4)], check=True)
            patched = mp4.read_bytes()
            self.assertEqual(patched[: len(mdat)], mdat)
            pos = patched.index(b"nclx", len(mdat)) + 4
            self.assertEqual(struct.unpack(">HHHB", patched[pos : pos + 7]), (2, 2, 2, 128))

    def test_n07_sbs_live_encode_bounds_json_and_cleanup(self) -> None:
        compiler = self._compiler()
        sbs_src = REPO_ROOT / "native/hobot/sbs_live_encode.c"
        hobot_inc = REPO_ROOT / "src/rp_ylx/hobot"

        c_code = (
            r"""
#define _GNU_SOURCE
#include <assert.h>
#include <errno.h>
#include <linux/videodev2.h>
#include <poll.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>
#include "ylx_stereo_pipeline.h"

static int g_open_calls = 0;
static int g_close_calls = 0;
static int g_munmap_calls = 0;
static int g_submit_calls = 0;
static int g_pipeline_close_calls = 0;
static int g_dqbuf_mode = 0;
static unsigned char g_fake_buf[64];

static int fake_open(const char *path, int flags, ...) {
    (void)path; (void)flags;
    g_open_calls++;
    return 42;
}
static int fake_close(int fd) {
    (void)fd;
    g_close_calls++;
    return 0;
}
static void *fake_mmap(void *addr, size_t len, int prot, int flags, int fd, off_t off) {
    (void)addr; (void)len; (void)prot; (void)flags; (void)fd; (void)off;
    return g_fake_buf;
}
static int fake_munmap(void *addr, size_t len) {
    (void)addr; (void)len;
    g_munmap_calls++;
    return 0;
}
static int fake_poll(struct pollfd *fds, nfds_t nfds, int timeout) {
    (void)fds; (void)nfds; (void)timeout;
    return 1;
}
static int fake_ioctl(int fd, unsigned long req, ...) {
    (void)fd;
    va_list args;
    va_start(args, req);
    void *arg = va_arg(args, void *);
    va_end(args);
    if (req == VIDIOC_S_FMT || req == VIDIOC_S_PARM || req == VIDIOC_STREAMON ||
        req == VIDIOC_STREAMOFF || req == VIDIOC_QBUF) {
        return 0;
    }
    if (req == VIDIOC_REQBUFS) {
        struct v4l2_requestbuffers *r = (struct v4l2_requestbuffers *)arg;
        r->count = 2;
        return 0;
    }
    if (req == VIDIOC_QUERYBUF) {
        struct v4l2_buffer *b = (struct v4l2_buffer *)arg;
        b->length = sizeof(g_fake_buf);
        b->m.offset = 0;
        return 0;
    }
    if (req == VIDIOC_DQBUF) {
        struct v4l2_buffer *b = (struct v4l2_buffer *)arg;
        if (g_dqbuf_mode == 0) {
            b->index = 99; /* out of bounds */
            b->bytesused = 16;
        } else {
            b->index = 0;
            b->bytesused = sizeof(g_fake_buf) + 1; /* exceeds buffer length */
        }
        b->flags = 0;
        return 0;
    }
    return -1;
}

struct ylx_pipeline { int dummy; };
static struct ylx_pipeline g_pipeline;
int ylx_pipeline_open(const ylx_pipeline_config_t *c, ylx_segment_closed_fn cb,
                      void *u, ylx_pipeline_t **out, char *err, size_t n) {
    (void)c; (void)cb; (void)u; (void)err; (void)n;
    *out = &g_pipeline;
    return 0;
}
int ylx_pipeline_submit(ylx_pipeline_t *p, const unsigned char *d, size_t n, int t) {
    (void)p; (void)d; (void)n; (void)t;
    g_submit_calls++;
    return 0;
}
int ylx_pipeline_finish(ylx_pipeline_t *p) { (void)p; return 0; }
void ylx_pipeline_close(ylx_pipeline_t *p) { (void)p; g_pipeline_close_calls++; }
void ylx_pipeline_stats(const ylx_pipeline_t *p, ylx_pipeline_stats_t *s) {
    (void)p; memset(s, 0, sizeof(*s));
}
const char *ylx_pipeline_error(const ylx_pipeline_t *p) {
    (void)p; return "err\"quote\\\n\x01";
}

#define open fake_open
#define close fake_close
#define ioctl fake_ioctl
#define mmap fake_mmap
#define munmap fake_munmap
#define poll fake_poll
#define main sbs_live_encode_main
"""
            + f'#include "{sbs_src}"\n'
            + r"""
#undef main
#undef open
#undef close
#undef ioctl
#undef mmap
#undef munmap
#undef poll

int main(int argc, char **argv) {
    assert(argc == 2);
    char *args[] = {
        "sbs_live_encode",
        "--out-dir", argv[1],
        "--duration", "1",
        NULL
    };
    g_dqbuf_mode = 0;
    assert(sbs_live_encode_main(5, args) == 0);
    assert(g_submit_calls == 0);
    assert(g_munmap_calls == 2);
    assert(g_close_calls == 1);
    assert(g_pipeline_close_calls == 1);

    g_munmap_calls = 0;
    g_close_calls = 0;
    g_pipeline_close_calls = 0;
    g_dqbuf_mode = 1;
    assert(sbs_live_encode_main(5, args) == 0);
    assert(g_submit_calls == 0);
    assert(g_munmap_calls == 2);
    assert(g_close_calls == 1);
    assert(g_pipeline_close_calls == 1);
    return 0;
}
"""
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            out_dir = root / "out"
            src_c = root / "n07.c"
            src_c.write_text(c_code, encoding="utf-8")
            binary = root / "n07"
            subprocess.run(
                [
                    compiler,
                    "-std=gnu11",
                    "-Wall",
                    "-Wextra",
                    "-Werror",
                    "-I",
                    str(hobot_inc),
                    str(src_c),
                    "-o",
                    str(binary),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            subprocess.run([str(binary), str(out_dir)], check=True)
            summary = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["pipeline_error"], 'err"quote\\\n\x01')
            self.assertEqual(summary["captured_frames"], 0)
            self.assertEqual(summary["submitted_frames"], 0)


if __name__ == "__main__":
    unittest.main()
