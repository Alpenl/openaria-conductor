"""Round 2 unit and regression tests for core primitives, config, indicator, API, and adapter."""

from __future__ import annotations

import copy
import errno
import http.client
import json
import math
import os
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import rp_ylx.api.locked_io as locked_io
import rp_ylx.api.manifest_validator as manifest_validator
import rp_ylx.api.router as router
import rp_ylx.api.security as security_mod
import rp_ylx.core.device_client as device_client
import rp_ylx.core.fs as core_fs
import rp_ylx.recording_indicator as recording_indicator
import rp_ylx.spectacular.adapter as spectacular_adapter
from rp_ylx.api import Principal, SecurityPolicy, create_gateway_server
from rp_ylx.api.downloads import ArtifactAccessError
from rp_ylx.api.streams import (
    _NEED_RESYNC,
    EventReplayBuffer,
    _NetworkEventReplayBuffer,
    _SharedNetworkPoller,
)
from rp_ylx.camera import FrameObservation, StereoFrame
from rp_ylx.config import (
    ProductionConfig,
    ProductionConfigError,
    RecordingEncoding,
    production_config_from_value,
)
from rp_ylx.core.rpc import ControlRpcClient, ControlRpcServer, RpcFramingError
from rp_ylx.imu import ImuObservation, ImuSample, RawVector3
from rp_ylx.recording import (
    DeviceSessionConfig,
    DeviceSessionRecorder,
    SessionPlan,
    StorageStatus,
    uuid7,
)
from rp_ylx.recording.stereo_encoder import ClosedSegment, StereoEncoderError
from tests.testkit.gateway import DEVICE_V3, NETWORK_STATUS, DeviceProvider

_JPEG = b"\xff\xd8round2-sbs\xff\xd9"


def _make_self_signed_cert(root: Path) -> tuple[Path, Path]:
    openssl = shutil.which("openssl")
    if openssl is None:
        raise unittest.SkipTest("openssl is required for TLS tests")
    cert = root / "device.crt"
    key = root / "device.key"
    subprocess.run(
        [
            openssl,
            "req",
            "-x509",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-keyout",
            str(key),
            "-out",
            str(cert),
        ],
        check=True,
        capture_output=True,
        timeout=10,
    )
    key.chmod(0o600)
    return cert, key


def _valid_device_json_dict(root: Path, *, profile: str = "lab") -> dict[str, object]:
    security: dict[str, object] = {"profile": profile, "isolated_network": True}
    if profile == "customer":
        token = root / "customer.token"
        token.write_text("valid-customer-token-1234\n", encoding="ascii")
        token.chmod(0o600)
        cert, key = _make_self_signed_cert(root)
        security.update(
            {
                "bearer_token_file": str(token),
                "tls_certificate_file": str(cert),
                "tls_private_key_file": str(key),
                "principal_id": "owner",
            }
        )
    return {
        "schema": "ylx.production-config.v1",
        "listen": {"host": "127.0.0.1", "port": 8443},
        "camera": {"device": "/dev/video0", "width": 3840, "height": 1080, "fps": 60},
        "storage": {
            "mountpoint": str(root / "data"),
            "minimum_available_bytes": 1024,
            "minimum_available_inodes": 16,
        },
        "state_root": str(root / "state"),
        "device": {
            "device_id": "550e8400-e29b-41d4-a716-446655440000",
            "device_label": "YLX-1234ABCD",
            "hardware_fingerprint": "sha256:" + "a" * 64,
        },
        "security": security,
    }


def _valid_production_config_kwargs(root: Path) -> dict[str, object]:
    return {
        "host": "127.0.0.1",
        "port": 8443,
        "camera_device": Path("/dev/video0"),
        "mountpoint": root / "data",
        "state_root": root / "state",
        "device_id": "550e8400-e29b-41d4-a716-446655440000",
        "device_label": "YLX-1234ABCD",
        "hardware_fingerprint": "sha256:" + "a" * 64,
        "isolated_network": True,
    }


class _FakeSplitEyeEncoder:
    def __init__(self, out_dir: Path, *, segment_frames: int, **unused: object) -> None:
        del unused
        self._out_dir = out_dir
        self._segment_frames = segment_frames
        self._segments: list[ClosedSegment] = []
        self._submitted = 0
        self._started = False

    @property
    def segments(self) -> tuple[ClosedSegment, ...]:
        return tuple(self._segments)

    @property
    def submitted_frames(self) -> int:
        return self._submitted

    def start(self) -> None:
        self._out_dir.mkdir(parents=True, exist_ok=True)
        self._started = True

    def submit(self, jpeg: bytes) -> None:
        del jpeg
        if not self._started:
            raise StereoEncoderError("invalid_state", "fake encoder not started")
        self._submitted += 1
        if self._submitted % self._segment_frames == 0:
            self._close(self._submitted - self._segment_frames, self._submitted)

    def finish(self, *, timeout: float = 30.0) -> tuple[ClosedSegment, ...]:
        del timeout
        closed = len(self._segments) * self._segment_frames
        if closed < self._submitted:
            self._close(closed, self._submitted)
        return self.segments

    def abort(self) -> None:
        return

    def _close(self, start_frame: int, end_frame: int) -> None:
        index = len(self._segments)
        artifacts: dict[str, tuple[str, int]] = {}
        for eye in ("left", "right"):
            name = f"{eye}_{index:05d}.mp4"
            payload = f"{eye}-{index}-{start_frame}-{end_frame}".encode() * 8
            (self._out_dir / name).write_bytes(payload)
            artifacts[eye] = (f"video/{name}", len(payload))
        self._segments.append(
            ClosedSegment(
                index=index,
                start_frame=start_frame,
                end_frame=end_frame,
                left_path=artifacts["left"][0],
                left_bytes=artifacts["left"][1],
                right_path=artifacts["right"][0],
                right_bytes=artifacts["right"][1],
            )
        )


def _create_calibration_session(
    root: Path,
    *,
    frame_count: int = 4,
    segment_frames: int = 3,
    lossless_storage: bool = False,
) -> tuple[Path, dict[str, object]]:
    revision = 0

    def next_rev() -> int:
        nonlocal revision
        revision += 1
        return revision

    session_id = uuid7()
    recorder = DeviceSessionRecorder(
        root,
        DeviceSessionConfig(
            device_id=str(uuid.uuid4()),
            device_label="YLX-12AB34CD",
            hardware_fingerprint="sha256:" + "a" * 64,
            platform="D-Robotics RDK X5 V1.0 + YLX 2UQ2",
            software_version="0.5.0",
            commit="b" * 40,
            width=3840,
            height=1080,
            sensor_fps=30.0,
            frame_decimation=1,
            video_layout="split-eyes",
            recording_encoding=RecordingEncoding(gop_frames=3) if lossless_storage else None,
            lossless_storage=lossless_storage,
            segment_seconds=0.1,
            audio_enabled=False,
        ),
        SessionPlan(
            session_id=session_id,
            volume_id=str(uuid.uuid4()),
            generation_id=str(uuid.uuid4()),
            capture_mode="calibration",
            display_name="Round 2 manifest fixture",
            take_id=uuid7(),
            take_sequence=1,
            continuation_of=None,
        ),
        authority_epoch=str(uuid.uuid4()),
        allocate_revision=next_rev,
        storage_status=lambda: StorageStatus(1024 * 1024 * 1024, True),
        checkpoint_interval=0.0,
        before_write=lambda _role, _payload: None,
        encoder_factory=lambda partial: _FakeSplitEyeEncoder(
            partial / "video",
            segment_frames=segment_frames,
        ),
    )
    recorder.start()
    for seq in range(frame_count):
        assert recorder.submit_frame(
            FrameObservation(
                StereoFrame(
                    source_sequence=seq,
                    host_monotonic_ns=1_000_000_000 + seq * 33_333_333,
                    left=b"",
                    right=b"",
                    raw_side_by_side=_JPEG,
                ),
                dropped_before=0,
            )
        )
    sample_seq = 0
    for packet in range(2):
        host = 1_000_000_000 + packet * 16_666_667
        ticks = 1_000 + packet * 1_000
        samples = [
            ImuSample(
                sequence=sample_seq + i,
                packet_sequence=packet,
                sample_index=i,
                device_timestamp_raw=ticks,
                device_ticks=ticks,
                host_read_start_ns=host - 500,
                host_read_end_ns=host + 500,
                host_monotonic_ns=host,
                accelerometer=RawVector3(1 + i, 2, 3),
                gyroscope=RawVector3(4, 5, 6 + i),
                sync_offset_ns=None,
                sync_residual_ns=None,
                sync_quality="insufficient",
            )
            for i in range(2)
        ]
        sample_seq += 2
        assert recorder.submit_imu(ImuObservation(tuple(samples), dropped_samples=0))
    sealed = recorder.stop()
    return sealed.path, dict(sealed.manifest)


def _capture_snapshot_event(revision: int = 1) -> dict[str, object]:
    return {
        "authority_epoch": "4fa85f64-5717-4562-b3fc-2c963f66afa6",
        "source_revision": revision,
        "type": "snapshot",
        "occurred_at": "2026-08-08T10:24:15+08:00",
        "session_id": None,
        "data": {
            "schema": "ylx.capture-snapshot-event.v2",
            "device_state": "idle",
            "active_recording": None,
            "retained_unsuccessful": None,
            "runtime": {
                "observed_at": "2026-08-08T10:24:15+08:00",
                "connection_method": "ethernet_lan",
                "temperature_celsius": 50.0,
                "network": DEVICE_V3["runtime"]["network"],
                "live_imu": None,
                "camera": {"schema": "ylx.camera-connection.v1", "state": "connected"},
                "camera_focus": None,
            },
        },
    }


def _network_status_with(
    revision: int, *, epoch: str = "4fa85f64-5717-4562-b3fc-2c963f66afa6"
) -> dict[str, object]:
    status = copy.deepcopy(NETWORK_STATUS)
    status["authority_epoch"] = epoch
    status["source_revision"] = revision
    return status


class CoreFsTests(unittest.TestCase):
    def test_write_atomic_exfat_eperm_and_posix_eperm_and_mode_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "data.json"

            with (
                patch.object(
                    core_fs.os, "fchmod", side_effect=PermissionError(errno.EPERM, "eperm")
                ),
                patch.object(core_fs, "_fstatfs_type", return_value=None),
                patch.object(core_fs.os, "fstatvfs", return_value=SimpleNamespace(f_files=0)),
            ):
                core_fs.write_json_atomic(target, {"ok": True}, mode=0o600)
            self.assertEqual(json.loads(target.read_text(encoding="utf-8")), {"ok": True})

            with (
                patch.object(
                    core_fs.os, "fchmod", side_effect=PermissionError(errno.EPERM, "eperm")
                ),
                patch.object(core_fs, "_fstatfs_type", return_value=None),
                patch.object(core_fs.os, "fstatvfs", return_value=SimpleNamespace(f_files=100)),
                self.assertRaises(PermissionError),
            ):
                core_fs.write_bytes_atomic(target, b"fail", mode=0o600)

            with patch.object(
                core_fs.os,
                "fchmod",
                side_effect=AssertionError("fchmod must be skipped when mode=None"),
            ):
                core_fs.write_bytes_atomic(target, b"no-chmod", mode=None)
            self.assertEqual(target.read_bytes(), b"no-chmod")

    def test_read_bounded_json_deep_nesting_raises_value_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "deep.json"
            path.write_text("[" * 2000 + "]" * 2000, encoding="utf-8")
            with self.assertRaises(ValueError):
                core_fs.read_bounded_json(path, max_bytes=64 * 1024, max_depth=64)

    def test_read_bearer_token_file_security_guards(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            real = root / "real.token"
            real.write_bytes(b"valid-token-value\n")
            real.chmod(0o600)

            symlink = root / "sym.token"
            symlink.symlink_to(real)
            with self.assertRaises(ValueError):
                core_fs.read_bearer_token_file(symlink)

            hardlink = root / "hard.token"
            os.link(real, hardlink)
            with self.assertRaises(ValueError):
                core_fs.read_bearer_token_file(hardlink)

            fifo = root / "fifo.token"
            os.mkfifo(fifo, 0o600)
            with self.assertRaises(ValueError):
                core_fs.read_bearer_token_file(fifo)

            oversized = root / "big.token"
            oversized.write_bytes(b"a" * 64)
            oversized.chmod(0o600)
            with self.assertRaises(ValueError):
                core_fs.read_bearer_token_file(oversized, max_bytes=32)

            double_nl = root / "double_nl.token"
            double_nl.write_bytes(b"valid-token-value\n\n")
            double_nl.chmod(0o600)
            with self.assertRaises(ValueError):
                core_fs.read_bearer_token_file(double_nl)

    def test_export_atomic_no_overwrite_handles_enotsup_on_fchmod(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "exported.bin"
            enotsup = getattr(errno, "ENOTSUP", errno.EOPNOTSUPP)
            with patch.object(core_fs.os, "fchmod", side_effect=OSError(enotsup, "not supported")):
                core_fs.export_atomic_no_overwrite(target, b"payload-bytes", mode=0o600)
            self.assertEqual(target.read_bytes(), b"payload-bytes")


class CoreRpcTests(unittest.TestCase):
    def test_max_pending_busy_and_framing_error_and_caller_owned_listener(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sock_path = Path(tmp) / "rpc.sock"
            entered = threading.Event()
            release = threading.Event()

            def handler(request: dict[str, object]) -> dict[str, object]:
                if request.get("op") == "block":
                    entered.set()
                    release.wait(timeout=2.0)
                return {"ok": True, "echo": request.get("op")}

            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            listener.bind(str(sock_path))
            listener.listen(16)
            server = ControlRpcServer(
                None,
                handler,
                listener=listener,
                max_workers=1,
                max_pending=1,
                request_timeout_seconds=2.0,
            ).start()
            client = ControlRpcClient(default_timeout_seconds=2.0)
            try:
                # 1. Saturate max_pending=1 and confirm second connection gets service_busy.
                results: list[dict[str, object]] = []
                worker_thread = threading.Thread(
                    target=lambda: results.append(client.request(sock_path, {"op": "block"})),
                    daemon=True,
                )
                worker_thread.start()
                self.assertTrue(entered.wait(timeout=1.5))

                busy_resp = client.request(sock_path, {"op": "ping"})
                self.assertFalse(busy_resp["ok"])
                self.assertEqual(busy_resp["error"]["code"], "service_busy")

                release.set()
                worker_thread.join(timeout=1.5)
                self.assertEqual(results, [{"ok": True, "echo": "block"}])

                # 2. Malformed NDJSON frame triggers structured invalid_request response.
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as raw:
                    raw.settimeout(2.0)
                    raw.connect(str(sock_path))
                    raw.sendall(b"{not-json\n")
                    raw.shutdown(socket.SHUT_WR)
                    reply = json.loads(raw.recv(4096).decode("utf-8"))
                self.assertFalse(reply["ok"])
                self.assertEqual(reply["error"]["code"], "invalid_request")

                # 3. Direct _reject_busy helper sends service_busy cleanly.
                left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
                with right:
                    server._reject_busy(left)
                    frame = json.loads(right.recv(4096).decode("utf-8"))
                self.assertEqual(frame["error"]["code"], "service_busy")

                # 4. Concurrent shutdown is idempotent and does NOT close caller-owned listener.
                threads = [threading.Thread(target=server.shutdown) for _ in range(4)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=2.0)
                self.assertGreaterEqual(listener.fileno(), 0)
            finally:
                release.set()
                server.shutdown()
                listener.close()

    def test_accept_connection_aborted_does_not_kill_server(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sock_path = Path(tmp) / "abort.sock"
            real_listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            real_listener.bind(str(sock_path))
            real_listener.listen(8)
            aborted_once = False

            class _FlakyListener:
                def __init__(self, wrapped: socket.socket) -> None:
                    self._wrapped = wrapped

                def fileno(self) -> int:
                    return self._wrapped.fileno()

                def setblocking(self, flag: bool) -> None:
                    self._wrapped.setblocking(flag)

                def accept(self):
                    nonlocal aborted_once
                    if not aborted_once:
                        aborted_once = True
                        raise ConnectionAbortedError("transient ECONNABORTED")
                    return self._wrapped.accept()

                def close(self) -> None:
                    self._wrapped.close()

            server = ControlRpcServer(
                None,
                lambda req: {"ok": True, "op": req.get("op")},
                listener=_FlakyListener(real_listener),  # type: ignore[arg-type]
                max_workers=1,
            ).start()
            try:
                client = ControlRpcClient(default_timeout_seconds=2.0)
                response = client.request(sock_path, {"op": "health"})
                self.assertEqual(response, {"ok": True, "op": "health"})
                self.assertTrue(aborted_once)
            finally:
                server.shutdown()
                real_listener.close()

    def test_rpc_framing_error_type(self) -> None:
        with self.assertRaises(RpcFramingError):
            ControlRpcClient().request("/nonexistent", {"bad": float("nan")})


class DeviceClientAndConfigTests(unittest.TestCase):
    def test_bootstrap_endpoint_ipv6_unspecified_and_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _valid_device_json_dict(root, profile="customer")
            cfg["listen"] = {"host": "::", "port": 8443}
            cfg_path = root / "device.json"
            cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

            endpoint = device_client.load_bootstrap_endpoint(cfg_path, check_hostname=False)
            self.assertEqual(endpoint.host, "::1")
            self.assertEqual(endpoint.url, "https://[::1]:8443")
            self.assertIsNotNone(endpoint.ssl_context)

            bad_cases = [
                ("multicast", lambda d: d["listen"].update({"host": "224.0.0.1"})),
                ("empty_host", lambda d: d["listen"].update({"host": ""})),
                ("scope_id", lambda d: d["listen"].update({"host": "fe80::1%eth0"})),
                ("bool_port", lambda d: d["listen"].update({"port": True})),
                ("invalid_profile", lambda d: d["security"].update({"profile": "custom"})),
                (
                    "missing_cert",
                    lambda d: d["security"].update(
                        {"tls_certificate_file": str(root / "missing.crt")}
                    ),
                ),
                (
                    "missing_token",
                    lambda d: d["security"].update(
                        {"bearer_token_file": str(root / "missing.token")}
                    ),
                ),
            ]
            for label, mutator in bad_cases:
                with self.subTest(case=label):
                    bad = copy.deepcopy(cfg)
                    mutator(bad)
                    bad_path = root / f"{label}.json"
                    bad_path.write_text(json.dumps(bad), encoding="utf-8")
                    with self.assertRaises(ValueError):
                        device_client.load_bootstrap_endpoint(bad_path)

    def test_production_config_rejects_nan_bool_dims_and_none_strings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base_kwargs = _valid_production_config_kwargs(root)

            with self.assertRaises(ProductionConfigError):
                ProductionConfig(**{**base_kwargs, "segment_seconds": float("nan")})
            for dim in ("width", "height", "fps"):
                with self.subTest(dim=dim), self.assertRaises(ProductionConfigError):
                    ProductionConfig(**{**base_kwargs, dim: True})

            valid_raw = _valid_device_json_dict(root, profile="lab")
            for mutator in (
                lambda d: d.update({"state_root": None}),
                lambda d: d["listen"].update({"host": None}),
                lambda d: d["camera"].update({"device": None}),
            ):
                bad = copy.deepcopy(valid_raw)
                mutator(bad)
                with self.assertRaises(ProductionConfigError):
                    production_config_from_value(bad)
            self.assertTrue(math.isfinite(ProductionConfig(**base_kwargs).segment_seconds))


class RecordingIndicatorTests(unittest.TestCase):
    def _make_led_dir(self, root: Path) -> Path:
        led = root / "led"
        led.mkdir()
        (led / "max_brightness").write_text("255\n")
        (led / "brightness").write_text("0\n")
        (led / "trigger").write_text("[none] timer heartbeat\n")
        (led / "delay_on").write_text("0\n")
        (led / "delay_off").write_text("0\n")
        return led

    def test_partial_write_failure_resets_pattern_and_http_exception_survives(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            led_dir = self._make_led_dir(Path(tmp))
            led = recording_indicator.StatusLed(led_dir)
            led.show("saving")
            self.assertEqual(led.pattern, "saving")

            # Fail mid-transition on delay_on when switching to "error"
            original_write = led._write

            def flaky_write(name: str, value: str | int) -> None:
                if name == "delay_on":
                    raise OSError(errno.EIO, "transient sysfs write error")
                original_write(name, value)

            with (
                patch.object(led, "_write", side_effect=flaky_write),
                self.assertRaises(OSError),
            ):
                led.show("error")
            self.assertIsNone(led.pattern)

            # Next poll to "saving" or "idle" must NOT short-circuit and must recover LED.
            led.show("idle")
            self.assertEqual(led.pattern, "idle")
            self.assertEqual((led_dir / "trigger").read_text().strip(), "none")

        with tempfile.TemporaryDirectory() as tmp:
            led_dir = self._make_led_dir(Path(tmp))
            stopped = threading.Event()
            snapshots: list[object] = [
                http.client.RemoteDisconnected("peer closed"),
                {"device_state": "idle", "retained_unsuccessful": None},
            ]

            class _Client:
                def capture_snapshot(self) -> dict[str, object]:
                    item = (
                        snapshots.pop(0)
                        if snapshots
                        else {
                            "device_state": "idle",
                            "retained_unsuccessful": None,
                        }
                    )
                    if isinstance(item, Exception):
                        raise item
                    stopped.set()
                    return item

            with patch.object(stopped, "wait", side_effect=lambda _t: False):
                recording_indicator.run_indicator(led_dir, _Client(), stopped)
            self.assertTrue(stopped.is_set())


class ApiAndAdapterTests(unittest.TestCase):
    def test_non_ascii_auth_and_csrf_close_unread_post_cleanly(self) -> None:
        policy = SecurityPolicy.customer(
            tokens={
                "good-token-123456": Principal(
                    "owner",
                    permissions={"getDevice": None, "startCapture": None},
                )
            },
            allowed_origins={"https://localhost"},
            csrf_token="good-csrf-123456",
        )
        server = create_gateway_server(
            "127.0.0.1",
            0,
            DeviceProvider(),
            security=policy,
            event_buffer=EventReplayBuffer(),
            external_scheme="https",
            request_timeout_seconds=2.0,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            host = f"127.0.0.1:{server.server_port}"
            # 1. Non-ASCII Authorization header -> 401 + Connection: close
            raw_request = (
                b"POST /api/v4/capture/start HTTP/1.1\r\n"
                + f"Host: {host}\r\n".encode("ascii")
                + b"Origin: https://localhost\r\n"
                + "Authorization: Bearer \u00e9\r\n".encode("utf-8")
                + b"Content-Type: application/json\r\n"
                + b"Content-Length: 2\r\n\r\n{}"
            )
            with socket.create_connection(("127.0.0.1", server.server_port), timeout=2.0) as sock:
                sock.sendall(raw_request)
                chunks = []
                while True:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    chunks.append(chunk)
                resp = b"".join(chunks)
                self.assertIn(b"401 Unauthorized", resp)
                self.assertIn(b"Connection: close", resp)

            # 2. Non-ASCII X-CSRF-Token header -> 403 + Connection: close
            csrf_request = (
                b"POST /api/v4/capture/start HTTP/1.1\r\n"
                + f"Host: {host}\r\n".encode("ascii")
                + b"Origin: https://localhost\r\n"
                + b"Authorization: Bearer good-token-123456\r\n"
                + "X-CSRF-Token: \u00e9\r\n".encode("utf-8")
                + b"Content-Type: application/json\r\n"
                + b"Content-Length: 2\r\n\r\n{}"
            )
            with socket.create_connection(("127.0.0.1", server.server_port), timeout=2.0) as sock:
                sock.sendall(csrf_request)
                chunks = []
                while True:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    chunks.append(chunk)
                resp = b"".join(chunks)
                self.assertIn(b"403 Forbidden", resp)
                self.assertIn(b"Connection: close", resp)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2.0)

    def test_customer_tls_slowloris_and_malformed_record_close_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cert, key = _make_self_signed_cert(Path(tmp))
            policy = SecurityPolicy.customer(
                tokens={"token-12345678": Principal("owner", permissions={"getDevice": None})}
            )
            server = create_gateway_server(
                "127.0.0.1",
                0,
                DeviceProvider(),
                security=policy,
                event_buffer=EventReplayBuffer(),
                external_scheme="https",
                request_timeout_seconds=0.15,
            )
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ctx.minimum_version = ssl.TLSVersion.TLSv1_2
            ctx.load_cert_chain(cert, key)
            server.socket = ctx.wrap_socket(
                server.socket, server_side=True, do_handshake_on_connect=False
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                # Partial TLS ClientHello stalls and hits cumulative handshake deadline.
                with socket.create_connection(
                    ("127.0.0.1", server.server_port), timeout=2.0
                ) as slow:
                    slow.sendall(b"\x16\x03\x01\x00\x40\x01")
                    self.assertEqual(slow.recv(1024), b"")

                # Malformed plaintext HTTP probe on TLS port closes cleanly.
                with socket.create_connection(
                    ("127.0.0.1", server.server_port), timeout=2.0
                ) as bad:
                    bad.sendall(b"GET / HTTP/1.1\r\n\r\n")
                    _ = bad.recv(1024)

                # Subsequent valid TLS request still succeeds.
                client_ctx = ssl.create_default_context(cafile=str(cert))
                client_ctx.check_hostname = False
                with (
                    socket.create_connection(("127.0.0.1", server.server_port), timeout=2.0) as tcp,
                    client_ctx.wrap_socket(tcp, server_hostname="localhost") as tls_sock,
                ):
                    tls_sock.sendall(
                        b"GET /api/v4/device HTTP/1.1\r\n"
                        b"Host: localhost\r\n"
                        b"Authorization: Bearer token-12345678\r\n"
                        b"Connection: close\r\n\r\n"
                    )
                    reply = tls_sock.recv(4096)
                    self.assertIn(b"200 OK", reply)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2.0)

    def test_manifest_validator_omitted_nominal_fps_still_checks_video_audio_and_fast_drops(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, base_manifest = _create_calibration_session(
                Path(tmp), frame_count=4, segment_frames=3
            )

            # Build a legacy v1 manifest omitting BOTH camera.nominal_fps
            # and integrity.quality_policy.
            legacy = copy.deepcopy(base_manifest)
            legacy["schema"] = "ylx.device-session.v1"
            legacy["camera"].pop("nominal_fps", None)
            legacy["integrity"].pop("quality_policy", None)
            legacy["imu"]["coordinate_frame"] = "opencv_optical"
            legacy["camera"]["effective_fps"] = (
                legacy["camera"]["sensor_fps"] / legacy["camera"]["frame_decimation"]
            )
            legacy["time"].pop("duration_clock", None)
            ended = manifest_validator.parse_rfc3339(legacy["time"]["ended_at"])
            started = manifest_validator.parse_rfc3339(legacy["time"]["started_at"])
            legacy["time"]["duration_seconds"] = (ended - started).total_seconds()
            legacy["audio"] = {
                "device": "hw:CARD=D2UQ2,DEV=0",
                "codec": "pcm_s16le",
                "container": "wav",
                "sample_rate_hz": 48000,
                "channels": 2,
                "sample_format": "S16_LE",
                "sample_count": 4800,
                "sync": {
                    "clock": "host_monotonic",
                    "started_monotonic_ns": 1000,
                    "stopped_monotonic_ns": 2000,
                    "session_start_offset_seconds": 0.0,
                    "session_stop_offset_seconds": 0.1,
                },
                "segments": [
                    {
                        "index": 0,
                        "start_sample": 0,
                        "end_sample": 4800,
                        "start_time_seconds": 0.0,
                        "end_time_seconds": 0.1,
                        "artifact": {
                            "artifact_id": "c" * 64,
                            "sha256": "c" * 64,
                            "role": "audio.wav",
                            "path": "audio/seg_00000.wav",
                            "media_type": "audio/wav",
                            "bytes": 19244,
                        },
                    }
                ],
            }
            manifest_validator.validate_device_session_manifest(legacy)

            bad_video = copy.deepcopy(legacy)
            bad_video["video"]["segments"][0]["end_frame"] += 1
            with self.assertRaises(ArtifactAccessError):
                manifest_validator.validate_device_session_manifest(bad_video)

            bad_audio = copy.deepcopy(legacy)
            bad_audio["audio"]["sample_count"] = 9999
            with self.assertRaises(ArtifactAccessError):
                manifest_validator.validate_device_session_manifest(bad_audio)

            # Large drop_events list validates rapidly in O(N log N).
            many = copy.deepcopy(base_manifest)
            drop_count = 10_000
            drops = [
                {
                    "start_frame": i * 2,
                    "end_frame": i * 2 + 1,
                    "reason": "write_backpressure",
                    "dropped": 1,
                    "at_time_seconds": i * 0.001,
                }
                for i in range(drop_count)
            ]
            many["integrity"]["drop_events"] = drops
            many["integrity"]["dropped_frames"] = drop_count
            many["integrity"]["quality_policy"] = {
                "policy_id": "rdk-x5-lossless-v1",
                "window_seconds": 1.0,
                "max_contiguous_dropped_frames": 10,
                "max_total_dropped_frames": drop_count + 10,
                "max_drop_fraction": 1.0,
                "max_dropped_frames_per_window": drop_count + 10,
            }
            many["video"]["segments"][-1]["end_frame"] = drop_count * 2 + 4
            many["frames"]["count"] = (
                many["video"]["segments"][-1]["end_frame"]
                - many["video"]["segments"][0]["start_frame"]
                - drop_count
            )
            many["camera"]["effective_fps"] = (
                many["frames"]["count"] / many["time"]["duration_seconds"]
            )
            started_at = time.monotonic()
            manifest_validator._validate_device_session_semantics(many, schema_version="v2")
            self.assertLess(time.monotonic() - started_at, 2.0)

    def test_locked_io_exclusive_regular_identity_zeroes_inode_on_exfat(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "item.bin"
            path.write_bytes(b"hello")
            fd = os.open(path, os.O_RDONLY)
            try:
                with patch.object(locked_io, "is_exfat_or_msdos_fd", return_value=True):
                    ident = locked_io._exclusive_regular_identity(fd)
                self.assertEqual(ident[1], 0)
            finally:
                os.close(fd)

    def test_security_policy_operations_match_router_and_rejects_bad_csrf(self) -> None:
        router_write_ops = {
            op_id
            for _version, _path, method, op_id in router.ALL_DECLARED_OPENAPI_ROUTES
            if method == "post"
        }
        self.assertEqual(security_mod._CUSTOMER_MUTATION_OPERATIONS, router_write_ops)
        with self.assertRaises(ValueError):
            SecurityPolicy.lab(allowed_operations={"startCapture"}, csrf_token="\n")

    def test_midstream_headers_sent_guard_closes_without_json_500_body(self) -> None:
        class _MidstreamFailProvider(DeviceProvider):
            def artifact_io_state(self):
                return None

            def open_verified_artifact(self, session_id: str, artifact_id: str, api_version: str):
                class _BrokenArtifact:
                    etag = '"' + ("a" * 64) + '"'
                    content_type = "application/octet-stream"
                    size = 32

                    def __enter__(self):
                        return self

                    def __exit__(self, *_args):
                        return None

                    def iter_chunks(self, offset: int = 0, length: int | None = None):
                        del offset, length
                        yield b"first-chunk-"
                        raise RuntimeError("boom after headers")

                del session_id, artifact_id, api_version
                return _BrokenArtifact()

        server = create_gateway_server(
            "127.0.0.1",
            0,
            _MidstreamFailProvider(),
            security=SecurityPolicy.lab(allowed_operations={"getSessionArtifact"}),
            event_buffer=EventReplayBuffer(),
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            sid = "01993e29-74e8-7000-8000-000000000001"
            aid = "a" * 64
            with socket.create_connection(("127.0.0.1", server.server_port), timeout=2.0) as sock:
                sock.sendall(
                    (
                        f"GET /api/v4/sessions/{sid}/artifacts/{aid} HTTP/1.1\r\n"
                        "Host: localhost\r\n\r\n"
                    ).encode()
                )
                chunks = []
                while True:
                    data = sock.recv(4096)
                    if not data:
                        break
                    chunks.append(data)
            raw = b"".join(chunks)
            self.assertIn(b"200 OK", raw)
            self.assertIn(b"first-chunk-", raw)
            self.assertNotIn(b"ylx.api-error.v2", raw)
            self.assertNotIn(b"provider_failure", raw)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2.0)

    def test_streams_resync_isolation_poller_race_and_capacity_boundary(self) -> None:
        # 1. _NetworkEventReplayBuffer keeps per-client resync out of shared buffer.
        net_buf = _NetworkEventReplayBuffer(capacity=4, initial_delivery_id=10)
        s1 = _network_status_with(1)
        resync_events = net_buf.replay(None, s1)
        self.assertEqual(len(resync_events), 1)
        self.assertEqual(len(net_buf._events), 0)
        cursor_from_resync = resync_events[0].delivery_id
        self.assertEqual(net_buf.replay(cursor_from_resync, s1), ())
        net_buf.observe(_network_status_with(2))
        replayed_after = net_buf.replay(cursor_from_resync, _network_status_with(2))
        self.assertEqual(len(replayed_after), 1)
        self.assertEqual(replayed_after[0].source_event["source_revision"], 2)

        # 2. _SharedNetworkPoller acquire/release race stays active when client count > 0.
        poll_count = 0

        def status_provider():
            nonlocal poll_count
            poll_count += 1
            return _network_status_with(poll_count)

        poller = _SharedNetworkPoller(status_provider, _NetworkEventReplayBuffer(), 0.02)
        try:
            poller.acquire()
            poller.acquire()
            poller.release()
            self.assertEqual(poller._active_clients, 1)
            self.assertFalse(poller._stop_event.is_set())
            self.assertIsNotNone(poller._thread)
            self.assertTrue(poller._thread.is_alive())
        finally:
            poller.release()
            poller.stop()

        # 3. EventReplayBuffer capacity N boundary does not spuriously resync.
        cap_buf = EventReplayBuffer(capacity=3)
        ids = [int(cap_buf.publish(_capture_snapshot_event(i))) for i in range(1, 4)]
        with cap_buf._changed:
            replayed = cap_buf._replayed_after_locked(ids[0], "v4")
        self.assertIsNot(replayed, _NEED_RESYNC)
        self.assertEqual([ev.delivery_id for ev in replayed], ids[1:])

    def test_spectacular_adapter_checks_zstd_media_type_and_closes_video_fds_eagerly(self) -> None:
        import zstandard

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            # 1. _read_jsonl_fd inspects compressed=True (from media_type == "application/zstd")
            # rather than requiring a .zst file suffix.
            raw_jsonl = b'{"a":1}\n'
            zstd_payload = zstandard.ZstdCompressor(write_checksum=True).compress(raw_jsonl)
            non_zst_path = root / "frames.bin"
            non_zst_path.write_bytes(zstd_payload)
            fd = os.open(non_zst_path, os.O_RDONLY)
            try:
                records = spectacular_adapter._read_jsonl_fd(
                    fd,
                    non_zst_path,
                    "frames",
                    expected_bytes=len(zstd_payload),
                    expected_sha256=spectacular_adapter.hashlib.sha256(zstd_payload).hexdigest(),
                    compressed=True,
                )
            finally:
                os.close(fd)
            self.assertEqual(records, [{"a": 1}])

            # 2. load_capture with lossless_storage (v4 zstd artifacts) + multiple video segments
            # closes video FDs eagerly without accumulating 2N open FDs.
            v4_root = root / "v4cap"
            v4_root.mkdir()
            session_dir, manifest = _create_calibration_session(
                v4_root, frame_count=12, segment_frames=3, lossless_storage=True
            )
            self.assertEqual(manifest["schema"], "ylx.device-session.v4")
            self.assertEqual(manifest["frames"]["artifact"]["media_type"], "application/zstd")

            opened_fds: list[int] = []
            closed_fds: list[int] = []
            real_open = spectacular_adapter.os.open
            real_close = spectacular_adapter.os.close

            def track_open(*args, **kwargs):
                fd = real_open(*args, **kwargs)
                opened_fds.append(fd)
                return fd

            def track_close(fd: int) -> None:
                closed_fds.append(fd)
                real_close(fd)

            with (
                patch.object(spectacular_adapter.os, "open", side_effect=track_open),
                patch.object(spectacular_adapter.os, "close", side_effect=track_close),
            ):
                loaded = spectacular_adapter.load_capture(session_dir)

            self.assertEqual(len(loaded.video.segments), 4)
            self.assertEqual(len(loaded.frames), 12)
            # Every opened fd during load_capture must be closed before return.
            self.assertEqual(set(opened_fds) - set(closed_fds), set())


if __name__ == "__main__":
    unittest.main()
