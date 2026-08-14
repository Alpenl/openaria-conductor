from __future__ import annotations

import json
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

from rp_ylx.daemon import (
    LAB_OPERATIONS,
    CaptureEventPump,
    ProductionConfig,
    ProductionConfigError,
    build_production_service,
    load_production_config,
)
from rp_ylx.deployment import ReleaseManager


class ProductionDaemonTest(unittest.TestCase):
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
            )
            manager._ensure_layout()
            manager._install_assets()
            config = load_production_config(root / "etc/rp-ylx/device.json")
            self.assertEqual(config.host, "0.0.0.0")
            self.assertTrue(config.isolated_network)

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
                patch("rp_ylx.daemon.ThreadedCaptureSources") as sources,
                patch("rp_ylx.daemon.CaptureCoordinator", return_value=coordinator) as capture,
                patch("rp_ylx.daemon.create_gateway_server", return_value=server) as gateway,
                patch("rp_ylx.daemon.CaptureEventPump") as event_pump,
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
            self.assertEqual(capture.call_args.kwargs["sources"], sources.return_value)
            coordinator_config = capture.call_args.args[0]
            self.assertEqual(coordinator_config.queue_capacity, 1024)
            security = gateway.call_args.kwargs["security"]
            self.assertEqual(security.profile, "lab")
            self.assertEqual(set(security.lab_principal.permissions), set(LAB_OPERATIONS))
            event_buffer = gateway.call_args.kwargs["event_buffer"]
            event_pump.assert_called_once_with(coordinator, event_buffer)
            self.assertIs(service.event_pump, event_pump.return_value)

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

    def test_source_checkout_cannot_start_production_service(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(Path(directory))
            with (
                patch("rp_ylx.daemon.__commit__", "unknown"),
                self.assertRaises(ProductionConfigError),
            ):
                build_production_service(config)


if __name__ == "__main__":
    unittest.main()
