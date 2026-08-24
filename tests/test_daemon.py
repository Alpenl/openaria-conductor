from __future__ import annotations

import http.client
import json
import socket
import tempfile
import threading
import time
import unittest
import uuid
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

from rp_ylx.daemon import (
    CUSTOMER_OPERATIONS,
    LAB_OPERATIONS,
    CaptureEventPump,
    ProductionConfig,
    ProductionConfigError,
    _enable_customer_tls,
    _security_policy,
    build_production_service,
    load_production_config,
)
from rp_ylx.deployment import ReleaseManager
from rp_ylx.native import NativeCapabilities

PRODUCTION_NATIVE_FEATURES = (
    "capability_probe",
    "native_camera",
    "camera_frame_validator",
    "native_audio",
    "native_timeline",
    "native_imu",
    "recording_codec",
    "recording_sink",
    "recording_imu_batch",
    "active_take_writer",
    "recording_frame_gate",
    "capture_fanout",
    "continuous_capture_runtime",
    "continuous_capture_raw_sink",
    "continuous_capture_split_sink",
    "recording_event_queue",
    "artifact_finalize",
    "stereo_encoder_events",
    "stereo_encoder_pipe",
    "session_io",
    "device_session_artifacts",
    "device_session_finalizer",
    "preview_buffer",
    "performance_metrics",
)


class ProductionDaemonTest(unittest.TestCase):
    def test_lab_profile_allows_trusted_internal_network_control(self) -> None:
        self.assertIn("getNetworkStatus", LAB_OPERATIONS)
        self.assertIn("scanNetworks", LAB_OPERATIONS)
        self.assertIn("streamNetworkEvents", LAB_OPERATIONS)
        self.assertIn("createNetworkCredentialReference", LAB_OPERATIONS)
        self.assertIn("applyNetworkDesiredState", LAB_OPERATIONS)
        self.assertIn("retryNetworkTransaction", LAB_OPERATIONS)
        self.assertIn("forgetNetworkClientProfile", LAB_OPERATIONS)
        self.assertEqual(CUSTOMER_OPERATIONS, LAB_OPERATIONS)

    def config(self, root: Path) -> ProductionConfig:
        return ProductionConfig(
            host="127.0.0.1",
            port=8080,
            camera_device=Path("/dev/video0"),
            mountpoint=root / "volume",
            state_root=root / "state",
            device_id=str(uuid.uuid4()),
            device_label="YLX-12AB34CD",
            hardware_fingerprint="sha256:" + "a" * 64,
            isolated_network=True,
            minimum_available_bytes=0,
            minimum_available_inodes=0,
        )

    def test_load_config_is_strict_and_requires_explicit_isolated_lab(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = self.config(root)
            path = root / "device.json"
            value = {
                "schema": "ylx.production-config.v1",
                "listen": {"host": config.host, "port": config.port},
                "camera": {
                    "device": str(config.camera_device),
                    "width": config.width,
                    "height": config.height,
                    "fps": config.fps,
                    "data_plane": "rust",
                },
                "storage": {
                    "mountpoint": str(config.mountpoint),
                    "minimum_available_bytes": 0,
                    "minimum_available_inodes": 0,
                },
                "state_root": str(config.state_root),
                "device": {
                    "device_id": config.device_id,
                    "device_label": config.device_label,
                    "hardware_fingerprint": config.hardware_fingerprint,
                },
                "security": {"profile": "lab", "isolated_network": True},
            }
            path.write_text(json.dumps(value), encoding="utf-8")
            self.assertEqual(load_production_config(path), config)
            value["security"]["isolated_network"] = False
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaises(ProductionConfigError):
                load_production_config(path)

    def test_customer_profile_reads_a_locked_token_file_and_grants_network_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = self.config(root)
            token = "0123456789abcdef0123456789abcdef0123456789abcdef"
            token_path = root / "customer.token"
            token_path.write_text(token + "\n", encoding="ascii")
            token_path.chmod(0o640)
            certificate_path = root / "device.crt"
            certificate_path.write_text("certificate", encoding="ascii")
            certificate_path.chmod(0o644)
            private_key_path = root / "device.key"
            private_key_path.write_text("private-key", encoding="ascii")
            private_key_path.chmod(0o640)
            path = root / "device.json"
            value = {
                "schema": "ylx.production-config.v1",
                "listen": {"host": base.host, "port": base.port},
                "camera": {
                    "device": str(base.camera_device),
                    "width": base.width,
                    "height": base.height,
                    "fps": base.fps,
                    "data_plane": "rust",
                },
                "storage": {
                    "mountpoint": str(base.mountpoint),
                    "minimum_available_bytes": 0,
                    "minimum_available_inodes": 0,
                },
                "state_root": str(base.state_root),
                "device": {
                    "device_id": base.device_id,
                    "device_label": base.device_label,
                    "hardware_fingerprint": base.hardware_fingerprint,
                },
                "security": {
                    "profile": "customer",
                    "isolated_network": True,
                    "bearer_token_file": str(token_path),
                    "tls_certificate_file": str(certificate_path),
                    "tls_private_key_file": str(private_key_path),
                    "principal_id": "device-owner",
                },
            }
            path.write_text(json.dumps(value), encoding="utf-8")

            config = load_production_config(path)
            self.assertEqual(config.security_profile, "customer")
            self.assertEqual(config.bearer_token_file, token_path)
            self.assertEqual(config.tls_certificate_file, certificate_path)
            self.assertEqual(config.tls_private_key_file, private_key_path)
            policy = _security_policy(config)
            principal = policy.authenticate(f"Bearer {token}")
            self.assertIsNotNone(principal)
            self.assertEqual(policy.csrf_token, token)
            assert principal is not None
            self.assertTrue(principal.permits("createNetworkCredentialReference"))
            self.assertTrue(principal.permits("applyNetworkDesiredState"))

            value["security"]["unexpected"] = True
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaises(ProductionConfigError):
                load_production_config(path)
            del value["security"]["unexpected"]
            path.write_text(json.dumps(value), encoding="utf-8")

            token_path.chmod(0o644)
            with self.assertRaisesRegex(ProductionConfigError, "权限"):
                _security_policy(config)

            private_key_path.chmod(0o644)
            with self.assertRaisesRegex(ProductionConfigError, "TLS 私钥权限"):
                _enable_customer_tls(Mock(socket=Mock()), config)

    def test_customer_profile_wraps_gateway_socket_with_tls(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            certificate_path = root / "device.crt"
            certificate_path.write_text("certificate", encoding="ascii")
            certificate_path.chmod(0o644)
            private_key_path = root / "device.key"
            private_key_path.write_text("private-key", encoding="ascii")
            private_key_path.chmod(0o640)
            config = replace(
                self.config(root),
                security_profile="customer",
                bearer_token_file=root / "customer.token",
                tls_certificate_file=certificate_path,
                tls_private_key_file=private_key_path,
            )
            original_socket = Mock()
            server = Mock(socket=original_socket)
            wrapped = Mock()
            context = Mock()
            context.wrap_socket.return_value = wrapped

            with patch("rp_ylx.daemon.ssl.SSLContext", return_value=context):
                _enable_customer_tls(server, config)

            context.load_cert_chain.assert_called_once_with(
                certfile=str(certificate_path),
                keyfile=str(private_key_path),
            )
            context.wrap_socket.assert_called_once_with(
                original_socket,
                server_side=True,
            )
            self.assertIs(server.socket, wrapped)

    def test_default_installed_lab_profile_is_reachable_from_device_network(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manager = ReleaseManager(
                install_root=root / "opt/rp-ylx",
                config_root=root / "etc/rp-ylx",
                state_root=root / "var/lib/rp-ylx",
                system_root=root,
                target_machine="aarch64",
                target_model="D-Robotics RDK X5 V1.0",
                library_finder=lambda name: f"lib{name}.so",
                executable_finder=lambda name: f"/usr/bin/{name}",
                runner=lambda command: None,
                health_checker=lambda: None,
                group_resolver=lambda name: root.stat().st_gid,
            )
            manager._ensure_layout()
            manager._install_assets()
            manager._ensure_device_config()
            config = load_production_config(root / "etc/rp-ylx/device.json")
            self.assertEqual(config.host, "0.0.0.0")
            self.assertTrue(config.isolated_network)

    def test_load_config_accepts_legacy_v1_as_rust_but_rejects_python_data_plane(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = self.config(root)
            path = root / "device.json"
            value = {
                "schema": "ylx.production-config.v1",
                "listen": {"host": config.host, "port": config.port},
                "camera": {
                    "device": str(config.camera_device),
                    "width": config.width,
                    "height": config.height,
                    "fps": config.fps,
                },
                "storage": {
                    "mountpoint": str(config.mountpoint),
                    "minimum_available_bytes": 0,
                    "minimum_available_inodes": 0,
                },
                "state_root": str(config.state_root),
                "device": {
                    "device_id": config.device_id,
                    "device_label": config.device_label,
                    "hardware_fingerprint": config.hardware_fingerprint,
                },
                "security": {"profile": "lab", "isolated_network": True},
            }
            path.write_text(json.dumps(value), encoding="utf-8")
            self.assertEqual(load_production_config(path).data_plane, "rust")
            value["camera"]["data_plane"] = "python"
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaises(ProductionConfigError):
                load_production_config(path)

    def test_build_service_connects_real_source_boundaries_and_gateway(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = self.config(root)
            config.mountpoint.mkdir()
            selector_backend = Mock()
            source_backend = Mock()
            backend_factory = Mock(side_effect=[selector_backend, source_backend])
            coordinator = Mock()
            server = Mock()
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.stable_id_for_device", return_value="camera-stable"),
                patch("rp_ylx.daemon.ContinuousCaptureSources") as sources,
                patch("rp_ylx.daemon.CaptureCoordinator", return_value=coordinator) as capture,
                patch("rp_ylx.daemon.create_gateway_server", return_value=server) as gateway,
                patch("rp_ylx.daemon.CaptureEventPump") as event_pump,
                patch("rp_ylx.daemon.MdnsPublisher", autospec=True) as mdns_publisher,
            ):
                service = build_production_service(
                    config,
                    camera_backend_factory=backend_factory,
                    imu_source_factory=Mock(),
                    mount_checker=lambda path: True,
                )
            self.assertIs(service.coordinator, coordinator)
            self.assertIs(service.server, server)
            self.assertEqual(sources.call_args.kwargs["stable_id"], "camera-stable")
            self.assertEqual(sources.call_args.kwargs["warmup_frames"], config.fps)
            self.assertEqual(sources.call_args.kwargs["frame_decimation"], config.frame_decimation)
            sources.return_value.start_preview.assert_called_once_with()
            self.assertIs(
                capture.call_args.kwargs["preview"],
                sources.call_args.kwargs["publish_preview"].__self__,
            )
            self.assertEqual(capture.call_args.kwargs["sources"], sources.return_value)
            coordinator_config = capture.call_args.args[0]
            self.assertEqual(coordinator_config.queue_capacity, 1024)
            security = gateway.call_args.kwargs["security"]
            self.assertEqual(security.profile, "lab")
            self.assertEqual(set(security.lab_principal.permissions), set(LAB_OPERATIONS))
            self.assertEqual(gateway.call_args.kwargs["external_scheme"], "http")
            event_buffer = gateway.call_args.kwargs["event_buffer"]
            event_pump.assert_called_once_with(coordinator, event_buffer)
            self.assertIs(service.event_pump, event_pump.return_value)
            mdns_publisher.assert_called_once_with(config.port, scheme="http")
            mdns_publisher.return_value.start.assert_called_once_with()
            self.assertIs(service.mdns_publisher, mdns_publisher.return_value)

    def test_mdns_start_failure_releases_every_constructed_service(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            coordinator = Mock()
            server = Mock()
            event_pump = Mock()
            mdns_publisher = Mock()
            mdns_publisher.start.side_effect = RuntimeError("multicast unavailable")
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.stable_id_for_device", return_value="camera-stable"),
                patch("rp_ylx.daemon.ContinuousCaptureSources"),
                patch("rp_ylx.daemon.CaptureCoordinator", return_value=coordinator),
                patch("rp_ylx.daemon.create_gateway_server", return_value=server),
                patch("rp_ylx.daemon.CaptureEventPump", return_value=event_pump),
                self.assertRaisesRegex(RuntimeError, "multicast unavailable"),
            ):
                build_production_service(
                    config,
                    camera_backend_factory=Mock(return_value=Mock()),
                    imu_source_factory=Mock(),
                    mount_checker=lambda path: False,
                    mdns_publisher_factory=Mock(return_value=mdns_publisher),
                )
            mdns_publisher.close.assert_called_once_with()
            event_pump.close.assert_called_once_with()
            server.server_close.assert_called_once_with()
            coordinator.close.assert_called_once_with()

    def test_unmounted_volume_keeps_production_http_control_plane_available(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            config = replace(self.config(root), port=port)
            source = Mock(open_handle_count=0)
            source.camera_connection_status.return_value = {
                "schema": "ylx.camera-connection.v1",
                "state": "connected",
            }
            source.camera_focus_status.return_value = None
            mdns_publisher = Mock()
            lock_path_patch = patch(
                "rp_ylx.recording.coordinator.network_operation_lock_path",
                return_value=root / "network-operation.lock",
            )
            controller_patch = patch(
                "rp_ylx.recording.coordinator.request_network_control",
                return_value={
                    "ok": True,
                    "operation": "status",
                    "status": 200,
                    "body": {"transaction": {"current": None}},
                },
            )
            lock_path_patch.start()
            controller_patch.start()
            self.addCleanup(lock_path_patch.stop)
            self.addCleanup(controller_patch.stop)
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.stable_id_for_device", return_value="camera-stable"),
                patch("rp_ylx.daemon.ContinuousCaptureSources", return_value=source),
            ):
                service = build_production_service(
                    config,
                    camera_backend_factory=Mock(return_value=Mock()),
                    imu_source_factory=Mock(),
                    mount_checker=lambda path: False,
                    mdns_publisher_factory=Mock(return_value=mdns_publisher),
                )
            mdns_publisher.start.assert_called_once_with()
            thread = threading.Thread(target=service.server.serve_forever, daemon=True)
            thread.start()
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
            try:
                connection.request("GET", "/")
                response = connection.getresponse()
                self.assertEqual(response.status, 200)
                response.read()

                connection.request("GET", "/api/v3/device")
                response = connection.getresponse()
                self.assertEqual(response.status, 200)
                device = json.loads(response.read())
                self.assertFalse(device["capabilities"]["capture"])
                self.assertEqual(device["storage"]["volume_id"], None)

                connection.request("GET", "/api/v3/capture/status")
                response = connection.getresponse()
                self.assertEqual(response.status, 200)
                self.assertEqual(json.loads(response.read())["snapshot"]["device_state"], "idle")

                connection.request("GET", "/api/v3/sessions")
                response = connection.getresponse()
                self.assertEqual(response.status, 409)
                self.assertEqual(json.loads(response.read())["error"]["code"], "volume_not_mounted")

                body = json.dumps(
                    {
                        "schema": "ylx.capture-start.v2",
                        "mode": "production",
                        "take": {"kind": "new"},
                    }
                )
                connection.request(
                    "POST",
                    "/api/v3/capture/start",
                    body=body,
                    headers={
                        "Content-Type": "application/json",
                        "Idempotency-Key": "missing-volume-start",
                    },
                )
                response = connection.getresponse()
                problem = json.loads(response.read())
                self.assertEqual(response.status, 409, problem)
                self.assertEqual(problem["error"]["code"], "volume_not_mounted")
                self.assertTrue(problem["error"]["retryable"])

                stop_body = json.dumps({"schema": "ylx.capture-stop.v2", "reason": "user"})
                connection.request(
                    "POST",
                    "/api/v3/capture/stop",
                    body=stop_body,
                    headers={
                        "Content-Type": "application/json",
                        "Idempotency-Key": "missing-volume-stop",
                    },
                )
                response = connection.getresponse()
                self.assertEqual(response.status, 409)
                self.assertEqual(json.loads(response.read())["error"]["code"], "volume_not_mounted")

                session_id = "01989f6a-2c00-7a1b-8c2d-3e4f50617283"
                connection.request("GET", f"/api/v3/sessions/{session_id}")
                response = connection.getresponse()
                self.assertEqual(response.status, 409)
                self.assertEqual(json.loads(response.read())["error"]["code"], "volume_not_mounted")
            finally:
                connection.close()
                service.server.shutdown()
                thread.join(timeout=2)
                service.close()
            mdns_publisher.close.assert_called_once_with()

    def test_event_pump_publishes_each_observed_revision_once_and_closes(self) -> None:
        source = {"authority_epoch": str(uuid.uuid4()), "source_revision": 1}
        coordinator = Mock(capture_snapshot_event=lambda: dict(source))
        event_buffer = Mock()
        pump = CaptureEventPump(coordinator, event_buffer, interval=0.01)
        try:
            source["source_revision"] = 2
            deadline = time.monotonic() + 1.0
            while event_buffer.publish.call_count == 0 and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(event_buffer.publish.call_count, 1)
            event = event_buffer.publish.call_args.args[0]
            self.assertEqual(event["source_revision"], 2)
            time.sleep(0.03)
            self.assertEqual(event_buffer.publish.call_count, 1)
        finally:
            pump.close()

    def test_event_pump_defaults_keep_idle_light_and_active_progress_live(self) -> None:
        source = {"authority_epoch": str(uuid.uuid4()), "source_revision": 1}
        coordinator = Mock(capture_snapshot_event=lambda: dict(source))
        event_buffer = Mock()
        pump = CaptureEventPump(coordinator, event_buffer)
        try:
            self.assertEqual(pump._interval, 1.0)
            self.assertEqual(pump._progress_interval, 0.2)
        finally:
            pump.close()

    def test_event_pump_publishes_same_revision_recording_progress(self) -> None:
        source = {
            "authority_epoch": str(uuid.uuid4()),
            "source_revision": 7,
            "type": "snapshot",
            "occurred_at": "2026-08-21T02:25:01Z",
            "session_id": "01989f6a-2c00-7a1b-8c2d-3e4f50617283",
            "data": {
                "schema": "ylx.capture-snapshot-event.v2",
                "device_state": "recording",
                "active_recording": {
                    "generation_id": str(uuid.uuid4()),
                    "recording_state": {
                        "schema": "ylx.recording-state.v1",
                        "state": "recording",
                        "authority_epoch": None,
                        "state_revision": 7,
                        "updated_at": "2026-08-21T02:25:01Z",
                        "session_id": "01989f6a-2c00-7a1b-8c2d-3e4f50617283",
                        "take_id": "01989f6a-2c00-7a1b-8c2d-3e4f50617284",
                        "display_name": "live projection",
                        "device": {
                            "device_id": str(uuid.uuid4()),
                            "device_label": "YLX-12AB34CD",
                        },
                        "storage": {
                            "volume_id": str(uuid.uuid4()),
                            "status": "mounted",
                            "writable": True,
                            "remaining_bytes": 1024,
                        },
                        "progress": {
                            "elapsed_seconds": 0.0,
                            "captured_frames": 0,
                            "bytes_written": 0,
                        },
                        "diagnostics": [],
                    },
                },
                "retained_unsuccessful": None,
                "runtime": {},
            },
        }
        source["data"]["active_recording"]["recording_state"]["authority_epoch"] = source[
            "authority_epoch"
        ]

        def snapshot_event() -> dict[str, object]:
            return dict(source)

        coordinator = Mock(capture_snapshot_event=snapshot_event)
        event_buffer = Mock()
        pump = CaptureEventPump(
            coordinator,
            event_buffer,
            interval=0.01,
            progress_interval=0.02,
        )
        try:
            progress = source["data"]["active_recording"]["recording_state"]["progress"]
            progress["elapsed_seconds"] = 1.0
            progress["captured_frames"] = 60
            progress["bytes_written"] = 4096
            deadline = time.monotonic() + 1.0
            while event_buffer.publish.call_count == 0 and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(event_buffer.publish.call_count, 1)
            event = event_buffer.publish.call_args.args[0]
            self.assertEqual(event["type"], "progress")
            self.assertEqual(event["source_revision"], 7)
            self.assertEqual(event["session_id"], source["session_id"])
            self.assertEqual(event["data"]["phase"], "recording")
            self.assertEqual(event["data"]["completed_units"], 60)
        finally:
            pump.close()

    def test_source_checkout_cannot_start_production_service(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            with (
                patch("rp_ylx.daemon.__commit__", "unknown"),
                self.assertRaises(ProductionConfigError),
            ):
                build_production_service(config)

    def test_missing_camera_does_not_block_native_control_plane_startup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = replace(self.config(root), camera_device=root / "missing-video0")
            sources = Mock()
            sources.start_preview.side_effect = RuntimeError(
                "open_failed: No such file or directory"
            )
            coordinator = Mock()
            server = Mock()
            event_pump = Mock()
            mdns_publisher = Mock()
            operational_log = Mock()
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon._OPERATIONAL_LOG", operational_log),
                patch(
                    "rp_ylx.daemon.native_capabilities",
                    return_value=NativeCapabilities(
                        True,
                        "0.1.0",
                        4,
                        PRODUCTION_NATIVE_FEATURES,
                    ),
                ),
                patch("rp_ylx.daemon.stable_id_for_device") as stable_id,
                patch(
                    "rp_ylx.daemon.NativeContinuousCaptureSources",
                    return_value=sources,
                ),
                patch("rp_ylx.daemon.CaptureCoordinator", return_value=coordinator),
                patch("rp_ylx.daemon.create_gateway_server", return_value=server),
                patch("rp_ylx.daemon.CaptureEventPump", return_value=event_pump),
                patch("rp_ylx.daemon.MdnsPublisher", return_value=mdns_publisher),
            ):
                service = build_production_service(config)
            try:
                stable_id.assert_not_called()
                sources.start_preview.assert_called_once_with()
                mdns_publisher.start.assert_called_once_with()
                self.assertIs(service.server, server)
                operational_log.event.assert_called_once_with(
                    "camera_preview_degraded",
                    level="warning",
                    error_code="camera_not_connected",
                    exception_type="RuntimeError",
                    retryable=True,
                )
            finally:
                service.close()

    def test_missing_native_camera_fails_before_production_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(True, "0.1.0", 4, ("capability_probe",))
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(raised.exception.code, "native_camera_unavailable")
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_camera_frame_validator_fails_before_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                ("capability_probe", "native_camera"),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(
                raised.exception.code,
                "native_camera_frame_validator_unavailable",
            )
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_audio_fails_before_production_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_imu",
                    "recording_codec",
                    "session_io",
                    "device_session_artifacts",
                    "device_session_finalizer",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(raised.exception.code, "native_audio_unavailable")
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_timeline_fails_before_production_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_imu",
                    "recording_codec",
                    "session_io",
                    "device_session_artifacts",
                    "device_session_finalizer",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(raised.exception.code, "native_timeline_unavailable")
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_imu_fails_before_production_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_timeline",
                    "recording_codec",
                    "session_io",
                    "device_session_artifacts",
                    "device_session_finalizer",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(raised.exception.code, "native_imu_unavailable")
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_recording_codec_fails_before_production_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_timeline",
                    "native_imu",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(raised.exception.code, "native_recording_unavailable")
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_session_io_fails_before_production_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_timeline",
                    "native_imu",
                    "recording_codec",
                    "recording_sink",
                    "recording_imu_batch",
                    "active_take_writer",
                    "recording_frame_gate",
                    "capture_fanout",
                    "continuous_capture_runtime",
                    "continuous_capture_raw_sink",
                    "continuous_capture_split_sink",
                    "recording_event_queue",
                    "artifact_finalize",
                    "stereo_encoder_events",
                    "stereo_encoder_pipe",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(raised.exception.code, "native_session_io_unavailable")
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_device_session_artifacts_fails_before_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_timeline",
                    "native_imu",
                    "recording_codec",
                    "recording_sink",
                    "recording_imu_batch",
                    "active_take_writer",
                    "recording_frame_gate",
                    "capture_fanout",
                    "continuous_capture_runtime",
                    "continuous_capture_raw_sink",
                    "continuous_capture_split_sink",
                    "recording_event_queue",
                    "artifact_finalize",
                    "stereo_encoder_events",
                    "stereo_encoder_pipe",
                    "session_io",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(raised.exception.code, "native_device_session_artifacts_unavailable")
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_device_session_finalizer_fails_before_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_timeline",
                    "native_imu",
                    "recording_codec",
                    "recording_sink",
                    "recording_imu_batch",
                    "active_take_writer",
                    "recording_frame_gate",
                    "capture_fanout",
                    "continuous_capture_runtime",
                    "continuous_capture_raw_sink",
                    "continuous_capture_split_sink",
                    "recording_event_queue",
                    "artifact_finalize",
                    "stereo_encoder_events",
                    "stereo_encoder_pipe",
                    "session_io",
                    "device_session_artifacts",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(raised.exception.code, "native_device_session_finalizer_unavailable")
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_recording_sink_fails_before_production_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_timeline",
                    "native_imu",
                    "recording_codec",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(raised.exception.code, "native_recording_sink_unavailable")
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_recording_imu_batch_fails_before_production_side_effects(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_timeline",
                    "native_imu",
                    "recording_codec",
                    "recording_sink",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(raised.exception.code, "native_recording_imu_batch_unavailable")
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_active_take_writer_fails_before_production_side_effects(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_timeline",
                    "native_imu",
                    "recording_codec",
                    "recording_sink",
                    "recording_imu_batch",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(raised.exception.code, "native_active_take_writer_unavailable")
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_recording_frame_gate_fails_before_production_side_effects(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_timeline",
                    "native_imu",
                    "recording_codec",
                    "recording_sink",
                    "recording_imu_batch",
                    "active_take_writer",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(raised.exception.code, "native_recording_frame_gate_unavailable")
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_capture_fanout_fails_before_production_side_effects(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_timeline",
                    "native_imu",
                    "recording_codec",
                    "recording_sink",
                    "recording_imu_batch",
                    "active_take_writer",
                    "recording_frame_gate",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(raised.exception.code, "native_capture_fanout_unavailable")
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_continuous_capture_runtime_fails_before_side_effects(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_timeline",
                    "native_imu",
                    "recording_codec",
                    "recording_sink",
                    "recording_imu_batch",
                    "active_take_writer",
                    "recording_frame_gate",
                    "capture_fanout",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(
                raised.exception.code,
                "native_continuous_capture_runtime_unavailable",
            )
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_continuous_capture_raw_sink_fails_before_side_effects(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_timeline",
                    "native_imu",
                    "recording_codec",
                    "recording_sink",
                    "recording_imu_batch",
                    "active_take_writer",
                    "recording_frame_gate",
                    "capture_fanout",
                    "continuous_capture_runtime",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(
                raised.exception.code,
                "native_continuous_capture_raw_sink_unavailable",
            )
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_continuous_capture_split_sink_fails_before_side_effects(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_timeline",
                    "native_imu",
                    "recording_codec",
                    "recording_sink",
                    "recording_imu_batch",
                    "active_take_writer",
                    "recording_frame_gate",
                    "capture_fanout",
                    "continuous_capture_runtime",
                    "continuous_capture_raw_sink",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(
                raised.exception.code,
                "native_continuous_capture_split_sink_unavailable",
            )
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_recording_event_queue_fails_before_production_side_effects(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_timeline",
                    "native_imu",
                    "recording_codec",
                    "recording_sink",
                    "recording_imu_batch",
                    "active_take_writer",
                    "recording_frame_gate",
                    "capture_fanout",
                    "continuous_capture_runtime",
                    "continuous_capture_raw_sink",
                    "continuous_capture_split_sink",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(raised.exception.code, "native_recording_event_queue_unavailable")
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_artifact_finalize_fails_before_production_side_effects(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_timeline",
                    "native_imu",
                    "recording_codec",
                    "recording_sink",
                    "recording_imu_batch",
                    "active_take_writer",
                    "recording_frame_gate",
                    "capture_fanout",
                    "continuous_capture_runtime",
                    "continuous_capture_raw_sink",
                    "continuous_capture_split_sink",
                    "recording_event_queue",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(raised.exception.code, "native_artifact_finalize_unavailable")
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_stereo_encoder_events_fails_before_production_side_effects(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_timeline",
                    "native_imu",
                    "recording_codec",
                    "recording_sink",
                    "recording_imu_batch",
                    "active_take_writer",
                    "recording_frame_gate",
                    "capture_fanout",
                    "continuous_capture_runtime",
                    "continuous_capture_raw_sink",
                    "continuous_capture_split_sink",
                    "recording_event_queue",
                    "artifact_finalize",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(raised.exception.code, "native_stereo_encoder_events_unavailable")
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_stereo_encoder_pipe_fails_before_production_side_effects(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_timeline",
                    "native_imu",
                    "recording_codec",
                    "recording_sink",
                    "recording_imu_batch",
                    "active_take_writer",
                    "recording_frame_gate",
                    "capture_fanout",
                    "continuous_capture_runtime",
                    "continuous_capture_raw_sink",
                    "continuous_capture_split_sink",
                    "recording_event_queue",
                    "artifact_finalize",
                    "stereo_encoder_events",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(raised.exception.code, "native_stereo_encoder_pipe_unavailable")
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_preview_buffer_fails_before_production_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_timeline",
                    "native_imu",
                    "recording_codec",
                    "recording_sink",
                    "recording_imu_batch",
                    "active_take_writer",
                    "recording_frame_gate",
                    "capture_fanout",
                    "continuous_capture_runtime",
                    "continuous_capture_raw_sink",
                    "continuous_capture_split_sink",
                    "recording_event_queue",
                    "artifact_finalize",
                    "stereo_encoder_events",
                    "stereo_encoder_pipe",
                    "session_io",
                    "device_session_artifacts",
                    "device_session_finalizer",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(raised.exception.code, "native_preview_buffer_unavailable")
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())

    def test_missing_native_metrics_fails_before_production_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            capabilities = NativeCapabilities(
                True,
                "0.1.0",
                4,
                (
                    "capability_probe",
                    "native_camera",
                    "camera_frame_validator",
                    "native_audio",
                    "native_timeline",
                    "native_imu",
                    "recording_codec",
                    "recording_sink",
                    "recording_imu_batch",
                    "active_take_writer",
                    "recording_frame_gate",
                    "capture_fanout",
                    "continuous_capture_runtime",
                    "continuous_capture_raw_sink",
                    "continuous_capture_split_sink",
                    "recording_event_queue",
                    "artifact_finalize",
                    "stereo_encoder_events",
                    "stereo_encoder_pipe",
                    "session_io",
                    "device_session_artifacts",
                    "device_session_finalizer",
                    "preview_buffer",
                ),
            )
            with (
                patch("rp_ylx.daemon.__commit__", "a" * 40),
                patch("rp_ylx.daemon.native_capabilities", return_value=capabilities),
                patch("rp_ylx.daemon.V4L2DiscoveryBackend") as backend,
                patch("rp_ylx.daemon.CaptureCoordinator") as coordinator,
                patch("rp_ylx.daemon.create_gateway_server") as gateway,
                self.assertRaises(ProductionConfigError) as raised,
            ):
                build_production_service(config)
            self.assertEqual(raised.exception.code, "native_metrics_unavailable")
            backend.assert_not_called()
            coordinator.assert_not_called()
            gateway.assert_not_called()
            self.assertFalse(config.state_root.exists())


if __name__ == "__main__":
    unittest.main()
