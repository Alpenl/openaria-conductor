from __future__ import annotations

import io
import json
import os
import shutil
import socket
import ssl
import subprocess
import sys
import tarfile
import tempfile
import threading
import unittest
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from rp_ylx.deployment import (
    BUNDLE_SCHEMA,
    CUSTOMER_TLS_CERTIFICATE_RELATIVE,
    CUSTOMER_TLS_PRIVATE_KEY_RELATIVE,
    DEPLOYMENT_ASSETS,
    NETWORK_CONTROL_PROTOCOL,
    NETWORK_STATE_MIGRATION_MARKER,
    SUPPORTING_DEPLOYMENT_ASSETS,
    TARGET_PLATFORM,
    DeploymentError,
    ReleaseManager,
    _asset_bytes,
    _default_tls_material_generator,
    _extract_runtime,
    _extract_wheel,
    _launcher,
    _sha256,
    _system_launcher,
    _systemd_unit_load_state,
    _write_atomic,
    load_bundle,
    normalize_runtime_archive,
    serve_network_control_launcher,
    write_bundle_manifest,
)
from rp_ylx.network import MDNS_ASSET_NAME, MDNS_PORT, MDNS_SERVICE, _avahi_service
from scripts import rdk_x5_install
from scripts.rdk_x5_install import _prepare_runtime


class ReleaseManagerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.install_root = self.root / "opt/rp-ylx"
        self.config_root = self.root / "etc/rp-ylx"
        self.state_root = self.root / "var/lib/rp-ylx"
        self.commands: list[tuple[str, ...]] = []

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def commit(character: str) -> str:
        return character * 40

    @staticmethod
    def runtime_descriptor(root: Path) -> dict[str, object]:
        runtime = root / "cpython-3.11.13-aarch64.tar.gz"
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "runtime/bin/python3"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"managed python runtime\n")
            with tarfile.open(runtime, "w:gz") as archive:
                archive.add(source.parents[1], arcname="runtime")
        return {
            "file": runtime.name,
            "bytes": runtime.stat().st_size,
            "sha256": _sha256(runtime),
            "implementation": "cpython",
            "python_version": "3.11.13",
            "platform": "linux_aarch64",
            "executable": "runtime/bin/python3",
        }

    def bundle(
        self,
        character: str,
        *,
        platform_tag: str = "manylinux_2_28_aarch64",
        manifest_commit: str | None = None,
    ) -> Path:
        commit = self.commit(character)
        root = self.root / f"bundle-{character}-{platform_tag}"
        root.mkdir()
        name = f"rp_ylx-0.1.0-cp311-abi3-{platform_tag}.whl"
        wheel = root / name
        with zipfile.ZipFile(wheel, "w") as archive:
            archive.writestr(
                "rp_ylx/_build_info.py",
                f'__commit__ = "{commit}"\n',
            )
            archive.writestr(
                "rp_ylx-0.1.0.dist-info/METADATA",
                "Metadata-Version: 2.1\nName: rp-ylx\nVersion: 0.1.0\n",
            )
            elf = bytearray(64)
            elf[:6] = b"\x7fELF\x02\x01"
            elf[18:20] = (183).to_bytes(2, "little")
            archive.writestr("rp_ylx/_native.abi3.so", elf)
        installer = root / "rdk_x5_install.py"
        installer.write_text("# bootstrap\n", encoding="utf-8")
        manifest = {
            "schema": BUNDLE_SCHEMA,
            "platform": TARGET_PLATFORM,
            "commit": manifest_commit or commit,
            "version": "0.1.0",
            "application_wheel": name,
            "installer": {
                "file": installer.name,
                "bytes": installer.stat().st_size,
                "sha256": _sha256(installer),
            },
            "runtime": self.runtime_descriptor(root),
            "wheels": [{"file": name, "bytes": wheel.stat().st_size, "sha256": _sha256(wheel)}],
        }
        _write_atomic(root / "bundle.json", manifest)
        return root

    @staticmethod
    def stage_installer(bundle: object, stage: Path) -> None:
        (stage / "bin").mkdir()
        (stage / "bin/rp-ylx").write_text("installed", encoding="utf-8")
        _extract_runtime(bundle.root / str(bundle.runtime["file"]), stage)

    @staticmethod
    def tls_material_generator(
        certificate: Path,
        private_key: Path,
        common_name: str,
    ) -> None:
        certificate.write_bytes(f"certificate:{common_name}".encode())
        private_key.write_bytes(f"private-key:{common_name}".encode())

    def manager(self) -> ReleaseManager:
        return ReleaseManager(
            install_root=self.install_root,
            config_root=self.config_root,
            state_root=self.state_root,
            system_root=self.root,
            target_machine="aarch64",
            target_model="D-Robotics RDK X5 V1.0",
            library_finder=lambda name: f"lib{name}.so",
            executable_finder=lambda name: f"/usr/bin/{name}",
            runner=lambda command: self.commands.append(tuple(command)),
            stage_installer=self.stage_installer,
            encoder_builder=lambda stage: None,
            health_checker=lambda: self.commands.append(("health-check",)),
            group_resolver=lambda name: os.getgid(),
            tls_material_generator=self.tls_material_generator,
            unit_load_state_reader=lambda unit: "loaded",
        )

    def seed_lab_config(self, manager: ReleaseManager) -> None:
        manager._ensure_layout()
        config = manager._default_device_config()
        config["security"] = {"profile": "lab", "isolated_network": True}
        _write_atomic(self.config_root / "device.json", config)

    def test_systemd_unit_load_state_returns_closed_systemd_value(self) -> None:
        completed = subprocess.CompletedProcess(
            ["systemctl"],
            0,
            stdout="not-found\n",
            stderr="",
        )
        with patch("rp_ylx.deployment.subprocess.run", return_value=completed) as run:
            self.assertEqual(_systemd_unit_load_state("rp-ylx-recover.service"), "not-found")

        run.assert_called_once_with(
            [
                "systemctl",
                "show",
                "--property=LoadState",
                "--value",
                "rp-ylx-recover.service",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def test_systemd_unit_load_state_rejects_failed_or_ambiguous_query(self) -> None:
        for completed in (
            subprocess.CompletedProcess(["systemctl"], 1, stdout="", stderr="failed"),
            subprocess.CompletedProcess(
                ["systemctl"],
                0,
                stdout="loaded\nnot-found\n",
                stderr="",
            ),
        ):
            with (
                self.subTest(returncode=completed.returncode, stdout=completed.stdout),
                patch("rp_ylx.deployment.subprocess.run", return_value=completed),
                self.assertRaises(DeploymentError) as rejected,
            ):
                _systemd_unit_load_state("rp-ylx-recover.service")
            self.assertEqual(rejected.exception.code, "unit_state_query_failed")

    def test_clean_and_repeated_install_preserve_identity_and_enable_boot(self) -> None:
        manager = self.manager()
        bundle = self.bundle("a")
        first = manager.install(bundle)
        self.assertEqual(first["current"], self.commit("a"))
        self.assertIsNone(first["previous"])
        config_path = self.config_root / "device.json"
        installed_config = json.loads(config_path.read_bytes())
        identity = installed_config["device"]
        self.assertEqual(installed_config["security"]["profile"], "customer")
        self.assertTrue(Path(installed_config["security"]["bearer_token_file"]).is_file())
        self.assertTrue(Path(installed_config["security"]["tls_certificate_file"]).is_file())
        self.assertTrue(Path(installed_config["security"]["tls_private_key_file"]).is_file())
        self.assertEqual(installed_config["camera"]["data_plane"], "rust")
        self.assertEqual(
            installed_config["audio"],
            {
                "enabled": True,
                "device": "hw:0,0",
                "sample_rate_hz": 48000,
                "channels": 2,
                "sample_format": "S16_LE",
            },
        )
        self.assertEqual(installed_config["storage"]["mountpoint"], "/data")
        (self.state_root / "business-state.json").write_text("keep", encoding="utf-8")

        repeated = manager.install(bundle)
        self.assertEqual(repeated, first)
        self.assertEqual(json.loads(config_path.read_bytes())["device"], identity)
        self.assertEqual((self.state_root / "business-state.json").read_text(), "keep")
        self.assertIn(("systemctl", "enable", "--now", "rp-ylx-data-volume.service"), self.commands)
        self.assertIn(("systemctl", "enable", "--now", "rp-ylx.service"), self.commands)
        self.assertIn(("systemctl", "enable", "--now", "rp-ylx-wifi-watchdog.timer"), self.commands)
        self.assertIn(
            ("systemctl", "enable", "--now", "rp-ylx-network-control.socket"),
            self.commands,
        )
        self.assertLess(
            self.commands.index(("systemctl", "enable", "--now", "rp-ylx-network-control.socket")),
            self.commands.index(("systemctl", "enable", "--now", "rp-ylx.service")),
        )
        self.assertNotIn(
            ("systemctl", "try-restart", "rp-ylx-network-control.service"),
            self.commands,
        )
        self.assertIn(("health-check",), self.commands)
        self.assertLess(
            self.commands.index(("health-check",)),
            self.commands.index(("systemctl", "enable", "--now", "rp-ylx-wifi-watchdog.timer")),
        )
        self.assertTrue((self.root / "usr/lib/systemd/system/rp-ylx.service").is_file())
        self.assertTrue((self.root / "usr/lib/systemd/system/rp-ylx-data-volume.service").is_file())
        self.assertTrue(
            (self.root / "usr/lib/systemd/system/rp-ylx-network-control.service").is_file()
        )
        self.assertTrue(
            (self.root / "usr/lib/systemd/system/rp-ylx-network-control.socket").is_file()
        )
        data_volume = self.root / "usr/local/sbin/rp-ylx-data-volume"
        self.assertTrue(data_volume.stat().st_mode & 0o100)
        watchdog = self.root / "usr/local/sbin/rp-ylx-wifi-watchdog"
        self.assertTrue(watchdog.stat().st_mode & 0o100)
        self.assertTrue((self.root / "usr/lib/systemd/system/rp-ylx-wifi-watchdog.timer").is_file())
        self.assertTrue((self.root / "etc/modprobe.d/aic8800-rp-ylx.conf").is_file())
        self.assertTrue(
            (self.root / "etc/NetworkManager/conf.d/90-rp-ylx-wifi-powersave.conf").is_file()
        )
        bootstrap = self.root / "usr/local/lib/rp-ylx/deployment.py"
        self.assertIn("import argparse", bootstrap.read_text(encoding="utf-8"))

    def test_repeated_install_preserves_wifi_watchdog_device_configuration(self) -> None:
        manager = self.manager()
        bundle = self.bundle("a")
        manager.install(bundle, activate=False)
        configuration = self.root / "etc/default/rp-ylx-wifi-watchdog"
        configuration.write_text("WIFI_GATEWAY=192.0.2.1\n", encoding="utf-8")

        manager.install(bundle, activate=False)

        self.assertEqual(configuration.read_text(encoding="utf-8"), "WIFI_GATEWAY=192.0.2.1\n")

    def test_clean_install_advertises_mdns_without_a_network_apply(self) -> None:
        # #118：默认安装必须在受支持设备网络上可被按名发现。此前 avahi service 文件只在
        # network apply 事务中写入，因此从未改过网络模式的设备完全不广播。
        manager = self.manager()
        manager.install(self.bundle("a"))

        advertised = self.root / "etc/avahi/services/rp-ylx.service"
        self.assertTrue(advertised.is_file(), "干净安装后 avahi service 文件必须存在")
        self.assertEqual(advertised.stat().st_mode & 0o777, 0o644)

        document = advertised.read_text(encoding="utf-8")
        self.assertIn(f"<type>{MDNS_SERVICE}</type>", document)
        self.assertIn(f"<port>{MDNS_PORT}</port>", document)
        # %h 通配符让 avahi 自行解析主机与地址，因此设备换地址后无需重写该文件。
        self.assertIn('replace-wildcards="yes"', document)

    def test_installed_mdns_service_matches_the_network_apply_payload(self) -> None:
        # 安装器与 network apply 必须写同一份字节，否则端口或服务类型会出现两份真相。
        manager = self.manager()
        self.seed_lab_config(manager)
        manager.install(self.bundle("a"))

        advertised = self.root / "etc/avahi/services/rp-ylx.service"
        self.assertEqual(advertised.read_bytes(), _avahi_service())
        self.assertEqual(advertised.read_bytes(), _asset_bytes(MDNS_ASSET_NAME))

    def test_bootstrap_root_carries_the_mdns_asset_for_standalone_deploys(self) -> None:
        # deployment.py 会被单独复制到 /usr/local/lib/rp-ylx 运行，其资产也必须一并落地。
        manager = self.manager()
        manager.install(self.bundle("a"))

        standalone = self.root / "usr/local/lib/rp-ylx" / MDNS_ASSET_NAME
        self.assertTrue(standalone.is_file())
        self.assertEqual(standalone.read_bytes(), _avahi_service())

    def test_legacy_network_state_is_migrated_once_without_changing_identity(self) -> None:
        legacy = self.root / "var/lib/rp-ylx/network"
        requests = legacy / "requests"
        requests.mkdir(parents=True, mode=0o700)
        legacy.chmod(0o700)
        requests.chmod(0o700)
        receipt_name = f"{'1' * 64}.json"
        payloads = {
            "rescue.json": b'{"config":{"mode":"hotspot"},"marker":"preserve-me"}\n',
            "lkg-wlan0.json": b'{"config":{"mode":"wifi-client","ssid":"known"}}\n',
            "controller-state.json": b'{"schema":"ylx.network-controller-state.v1"}\n',
            f"requests/{receipt_name}": b'{"outcome":"committed"}\n',
            ".idempotency-key": bytes(range(32)),
        }
        for relative, payload in payloads.items():
            path = legacy / relative
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            path.write_bytes(payload)
            path.chmod(0o600)

        manager = self.manager()
        bundle = self.bundle("a")
        manager.install(bundle, activate=False)

        target = self.root / "var/lib/rp-ylx-network"
        for relative, payload in payloads.items():
            with self.subTest(relative=relative):
                self.assertEqual((target / relative).read_bytes(), payload)
                self.assertEqual((target / relative).stat().st_mode & 0o777, 0o600)
                self.assertEqual((legacy / relative).read_bytes(), payload)
        marker = target / NETWORK_STATE_MIGRATION_MARKER
        self.assertEqual(marker.stat().st_mode & 0o777, 0o600)
        marker_value = json.loads(marker.read_bytes())
        self.assertEqual(marker_value["source"], "/var/lib/rp-ylx/network")
        self.assertEqual(set(marker_value["files"]), set(payloads))

        changed = b'{"config":{"mode":"wifi-client","ssid":"new-state"}}\n'
        (target / "rescue.json").write_bytes(changed)
        (target / "rescue.json").chmod(0o600)
        manager.install(bundle, activate=False)
        self.assertEqual((target / "rescue.json").read_bytes(), changed)
        self.assertEqual((legacy / "rescue.json").read_bytes(), payloads["rescue.json"])

    def test_legacy_network_state_with_secret_fields_is_never_migrated(self) -> None:
        legacy = self.root / "var/lib/rp-ylx/network"
        legacy.mkdir(parents=True, mode=0o700)
        legacy.chmod(0o700)
        secret_state = legacy / "rescue.json"
        secret_state.write_text(
            json.dumps(
                {
                    "config": {"mode": "hotspot"},
                    "legacy": {"credential_ref": "must-not-migrate"},
                }
            ),
            encoding="utf-8",
        )
        secret_state.chmod(0o600)

        with self.assertRaises(DeploymentError) as rejected:
            self.manager().install(self.bundle("a"), activate=False)

        target = self.root / "var/lib/rp-ylx-network/rescue.json"
        self.assertEqual(rejected.exception.code, "network_state_migration_unsafe")
        self.assertNotIn("must-not-migrate", str(rejected.exception))
        self.assertFalse(target.exists())
        self.assertFalse((self.install_root / "current").exists())

    def test_completed_legacy_migration_is_rescanned_for_historical_secret_leaks(self) -> None:
        legacy = self.root / "var/lib/rp-ylx/network"
        legacy.mkdir(parents=True, mode=0o700)
        legacy.chmod(0o700)
        rescue = legacy / "rescue.json"
        rescue.write_text('{"config":{"mode":"hotspot"}}\n', encoding="utf-8")
        rescue.chmod(0o600)
        manager = self.manager()
        bundle = self.bundle("a")
        manager.install(bundle, activate=False)

        migrated = self.root / "var/lib/rp-ylx-network/rescue.json"
        migrated.write_text(
            '{"config":{"mode":"hotspot"},"legacy":{"token":"must-not-survive"}}\n',
            encoding="utf-8",
        )
        migrated.chmod(0o600)

        with self.assertRaises(DeploymentError) as rejected:
            manager.install(bundle, activate=False)

        self.assertEqual(rejected.exception.code, "network_state_migration_unsafe")
        self.assertNotIn("must-not-survive", str(rejected.exception))

    def test_network_state_migration_rejects_conflicts_and_symlinks(self) -> None:
        legacy = self.root / "var/lib/rp-ylx/network"
        target = self.root / "var/lib/rp-ylx-network"
        legacy.mkdir(parents=True, mode=0o700)
        target.mkdir(parents=True, mode=0o700)
        legacy.chmod(0o700)
        target.chmod(0o700)
        (legacy / "rescue.json").write_text('{"side":"legacy"}\n', encoding="utf-8")
        (target / "rescue.json").write_text('{"side":"target"}\n', encoding="utf-8")
        (legacy / "rescue.json").chmod(0o600)
        (target / "rescue.json").chmod(0o600)

        with self.assertRaises(DeploymentError) as conflict:
            self.manager().install(self.bundle("a"), activate=False)
        self.assertEqual(conflict.exception.code, "network_state_migration_conflict")
        self.assertFalse((self.install_root / "current").exists())

        (target / "rescue.json").unlink()
        (legacy / "rescue.json").unlink()
        outside = self.root / "outside-state.json"
        outside.write_text("{}\n", encoding="utf-8")
        (legacy / "rescue.json").symlink_to(outside)
        with self.assertRaises(DeploymentError) as unsafe:
            self.manager().install(self.bundle("b"), activate=False)
        self.assertEqual(unsafe.exception.code, "network_state_migration_unsafe")

        (legacy / "rescue.json").unlink()
        (legacy / "rescue.json").write_text('{"side":"legacy"}\n', encoding="utf-8")
        (legacy / "rescue.json").chmod(0o600)
        shutil.rmtree(target)
        outside_directory = self.root / "outside-network-state"
        outside_directory.mkdir(mode=0o755)
        target.symlink_to(outside_directory, target_is_directory=True)
        with self.assertRaises(DeploymentError) as target_unsafe:
            self.manager().install(self.bundle("c"), activate=False)
        self.assertEqual(target_unsafe.exception.code, "network_state_migration_unsafe")
        self.assertEqual(outside_directory.stat().st_mode & 0o777, 0o755)

    def test_install_disables_managed_wifi_autoconnect_without_rewriting_secret(self) -> None:
        connections = self.root / "etc/NetworkManager/system-connections"
        connections.mkdir(parents=True)
        connections.chmod(0o700)
        managed = connections / "rp-ylx-wifi-client-33fb909c78fc.nmconnection"
        original = (
            b"[connection]\r\n"
            b"id=rp-ylx-wifi-client-33fb909c78fc\r\n"
            b"autoconnect=true\r\n"
            b"\r\n"
            b"[wifi-security]\r\n"
            b"key-mgmt=wpa-psk\r\n"
            b"psk=#keep=this\\value\r\n"
        )
        managed.write_bytes(original)
        managed.chmod(0o600)
        unmanaged = connections / "HKU-CGVU.nmconnection"
        unmanaged.write_bytes(original)

        manager = self.manager()
        bundle = self.bundle("a")
        manager.install(bundle, activate=False)

        expected = original.replace(b"autoconnect=true", b"autoconnect=false", 1)
        self.assertEqual(managed.read_bytes(), expected)
        self.assertEqual(unmanaged.read_bytes(), original)
        self.assertEqual(managed.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.commands.count(("nmcli", "connection", "reload")), 1)

        manager.install(bundle, activate=False)
        self.assertEqual(managed.read_bytes(), expected)
        self.assertEqual(self.commands.count(("nmcli", "connection", "reload")), 1)

    def test_install_rejects_unsafe_managed_wifi_profile(self) -> None:
        connections = self.root / "etc/NetworkManager/system-connections"
        connections.mkdir(parents=True)
        connections.chmod(0o700)
        outside = self.root / "outside.nmconnection"
        outside.write_text("[connection]\nautoconnect=true\n", encoding="utf-8")
        managed = connections / "rp-ylx-wifi-client-33fb909c78fc.nmconnection"
        managed.symlink_to(outside)

        with self.assertRaises(DeploymentError) as unsafe:
            self.manager().install(self.bundle("a"), activate=False)
        self.assertEqual(unsafe.exception.code, "network_profile_migration_unsafe")
        self.assertFalse((self.install_root / "current").exists())

    def test_customer_install_generates_and_preserves_tls_token_and_https_mdns(self) -> None:
        manager = self.manager()

        def generate(certificate: Path, private_key: Path, common_name: str) -> None:
            certificate.write_bytes(f"certificate:{common_name}".encode())
            private_key.write_bytes(f"private-key:{common_name}".encode())

        generator = MagicMock(side_effect=generate)
        manager.tls_material_generator = generator
        manager.install(self.bundle("a"), activate=False)

        installed = json.loads((self.config_root / "device.json").read_bytes())
        security = installed["security"]
        token = Path(security["bearer_token_file"])
        certificate = Path(security["tls_certificate_file"])
        private_key = Path(security["tls_private_key_file"])
        self.assertEqual(certificate, self.config_root / CUSTOMER_TLS_CERTIFICATE_RELATIVE)
        self.assertEqual(private_key, self.config_root / CUSTOMER_TLS_PRIVATE_KEY_RELATIVE)
        self.assertGreaterEqual(len(token.read_text(encoding="ascii").strip()), 32)
        self.assertEqual(token.stat().st_mode & 0o777, 0o640)
        self.assertEqual(certificate.stat().st_mode & 0o777, 0o644)
        self.assertEqual(private_key.stat().st_mode & 0o777, 0o640)
        self.assertEqual((self.config_root / "device.json").stat().st_mode & 0o777, 0o640)
        identity_before = {
            "token": token.read_bytes(),
            "certificate": certificate.read_bytes(),
            "private_key": private_key.read_bytes(),
            "device": installed["device"],
        }
        advertised = (self.root / "etc/avahi/services/rp-ylx.service").read_text(encoding="utf-8")
        self.assertIn("<type>_https._tcp</type>", advertised)
        self.assertIn("<txt-record>scheme=https</txt-record>", advertised)
        self.assertIn("<txt-record>api=/api/v4/device</txt-record>", advertised)

        manager.install(self.bundle("b"), activate=False)
        upgraded = json.loads((self.config_root / "device.json").read_bytes())
        self.assertEqual(token.read_bytes(), identity_before["token"])
        self.assertEqual(certificate.read_bytes(), identity_before["certificate"])
        self.assertEqual(private_key.read_bytes(), identity_before["private_key"])
        self.assertEqual(upgraded["device"], identity_before["device"])
        generator.assert_called_once()

    def test_customer_install_rejects_existing_identity_outside_config_root(self) -> None:
        manager = self.manager()
        manager._ensure_layout()
        outside = self.root / "outside-customer.token"
        payload = b"x" * 48 + b"\n"
        outside.write_bytes(payload)
        outside.chmod(0o644)
        config = manager._default_device_config()
        config["security"]["bearer_token_file"] = str(outside)
        _write_atomic(self.config_root / "device.json", config)

        with self.assertRaises(DeploymentError) as rejected:
            manager.install(self.bundle("a"), activate=False)

        self.assertEqual(rejected.exception.code, "customer_identity_path_unmanaged")
        self.assertEqual(outside.read_bytes(), payload)
        self.assertEqual(outside.stat().st_mode & 0o777, 0o644)
        self.assertFalse((self.install_root / "current").exists())

    def test_customer_install_rejects_symlinked_identity_parent(self) -> None:
        manager = self.manager()
        manager._ensure_layout()
        outside = self.root / "outside-tls"
        outside.mkdir()
        (self.config_root / "tls").symlink_to(outside, target_is_directory=True)

        with self.assertRaises(DeploymentError) as rejected:
            manager.install(self.bundle("a"), activate=False)

        self.assertEqual(rejected.exception.code, "customer_identity_path_unsafe")
        self.assertEqual(list(outside.iterdir()), [])
        self.assertFalse((self.install_root / "current").exists())

    def test_customer_tls_upgrade_failure_and_rollback_restore_release_config(self) -> None:
        manager = self.manager()
        self.seed_lab_config(manager)
        manager.install(self.bundle("a"), activate=False)
        config_path = self.config_root / "device.json"
        legacy_config = json.loads(config_path.read_bytes())
        token = self.config_root / "legacy-customer.token"
        token.write_text("legacy-token-" + "x" * 40 + "\n", encoding="ascii")
        token.chmod(0o640)
        legacy_config["security"] = {
            "profile": "customer",
            "isolated_network": True,
            "bearer_token_file": str(token),
            "principal_id": "legacy-owner",
        }
        _write_atomic(config_path, legacy_config)
        config_path.chmod(0o640)

        def generate(certificate: Path, private_key: Path, common_name: str) -> None:
            certificate.write_bytes(f"certificate:{common_name}".encode())
            private_key.write_bytes(f"private-key:{common_name}".encode())

        generator = MagicMock(side_effect=generate)
        manager.tls_material_generator = generator

        def fail_health() -> None:
            raise DeploymentError("service_unhealthy", "TLS release did not become ready")

        manager.health_checker = fail_health
        bundle_b = self.bundle("b")
        with self.assertRaises(DeploymentError):
            manager.install(bundle_b)
        self.assertEqual(manager.status()["current"], self.commit("a"))
        self.assertEqual(json.loads(config_path.read_bytes()), legacy_config)
        restored_mdns = (self.root / "etc/avahi/services/rp-ylx.service").read_text(
            encoding="utf-8"
        )
        self.assertIn("<txt-record>scheme=http</txt-record>", restored_mdns)

        manager.health_checker = lambda: self.commands.append(("health-check",))
        manager.install(bundle_b)
        tls_config = json.loads(config_path.read_bytes())
        self.assertIn("tls_certificate_file", tls_config["security"])
        self.assertIn("tls_private_key_file", tls_config["security"])
        generator.assert_called_once()

        manager.rollback()
        self.assertEqual(manager.status()["current"], self.commit("a"))
        self.assertEqual(json.loads(config_path.read_bytes()), legacy_config)
        self.assertEqual(token.read_text(encoding="ascii"), "legacy-token-" + "x" * 40 + "\n")

    def test_lab_install_stays_http_and_does_not_create_customer_identity(self) -> None:
        manager = self.manager()
        self.seed_lab_config(manager)
        manager.install(self.bundle("a"), activate=False)
        self.assertFalse((self.config_root / "customer.token").exists())
        self.assertFalse((self.config_root / CUSTOMER_TLS_CERTIFICATE_RELATIVE).exists())
        self.assertFalse((self.config_root / CUSTOMER_TLS_PRIVATE_KEY_RELATIVE).exists())
        advertised = (self.root / "etc/avahi/services/rp-ylx.service").read_text(encoding="utf-8")
        self.assertIn("<type>_http._tcp</type>", advertised)
        self.assertIn("<txt-record>scheme=http</txt-record>", advertised)
        self.assertNotIn("_https._tcp", advertised)

    def test_generated_customer_certificate_contains_fixed_local_sans(self) -> None:
        certificate = self.root / "tls/device.crt"
        private_key = self.root / "tls/device.key"
        openssl_config: list[str] = []
        calls: list[tuple[list[str], dict[str, object]]] = []

        def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
            calls.append((command, kwargs))
            output = Path(command[command.index("-out") + 1])
            if command[1] == "genpkey":
                output.write_bytes(b"generated-private-key")
            else:
                config_path = Path(command[command.index("-config") + 1])
                openssl_config.append(config_path.read_text(encoding="ascii"))
                output.write_bytes(b"generated-certificate")
            return subprocess.CompletedProcess(command, 0, b"", b"")

        with patch("rp_ylx.deployment.subprocess.run", side_effect=fake_run):
            _default_tls_material_generator(certificate, private_key, "YLX-0123ABCD")

        self.assertEqual(len(calls), 2)
        self.assertTrue(all(kwargs["capture_output"] is True for _, kwargs in calls))
        self.assertIn("DNS.1 = rp-ylx.local", openssl_config[0])
        self.assertIn("DNS.2 = localhost", openssl_config[0])
        self.assertIn("IP.1 = 127.0.0.1", openssl_config[0])
        self.assertIn("IP.2 = 10.42.0.1", openssl_config[0])
        self.assertNotIn("192.168.", openssl_config[0])
        self.assertEqual(private_key.stat().st_mode & 0o777, 0o640)
        self.assertEqual(certificate.stat().st_mode & 0o777, 0o644)

    @unittest.skipUnless(shutil.which("openssl"), "openssl is required for deployment TLS")
    def test_generated_certificate_verifies_for_loopback_ip_with_itself_as_cafile(self) -> None:
        certificate = self.root / "real-tls/device.crt"
        private_key = self.root / "real-tls/device.key"
        _default_tls_material_generator(certificate, private_key, "YLX-89ABCDEF")
        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(certificate, private_key)
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        server_errors: list[BaseException] = []

        def serve() -> None:
            try:
                connection, _ = listener.accept()
                with (
                    connection,
                    server_context.wrap_socket(
                        connection,
                        server_side=True,
                    ) as secured,
                ):
                    secured.recv(1)
                    secured.sendall(b"y")
            except BaseException as error:
                server_errors.append(error)

        worker = threading.Thread(target=serve)
        worker.start()
        client_context = ssl.create_default_context(cafile=str(certificate))
        partial_chain = getattr(ssl, "VERIFY_X509_PARTIAL_CHAIN", 0)
        if partial_chain:
            client_context.verify_flags |= partial_chain
        with (
            socket.create_connection(listener.getsockname(), timeout=5) as connection,
            client_context.wrap_socket(
                connection,
                server_hostname="127.0.0.1",
            ) as secured,
        ):
            secured.sendall(b"x")
            self.assertEqual(secured.recv(1), b"y")
            sans = secured.getpeercert()["subjectAltName"]
        worker.join(timeout=5)
        listener.close()

        self.assertFalse(worker.is_alive())
        self.assertEqual(server_errors, [])
        self.assertIn(("IP Address", "127.0.0.1"), sans)

    def test_every_system_asset_is_declared_for_packaging(self) -> None:
        # 新增资产若没同时进打包声明，wheel 里就没有它，安装到真机才会暴露。
        repository = Path(__file__).resolve().parents[1]
        pyproject = (repository / "pyproject.toml").read_text(encoding="utf-8")
        manifest = (repository / "MANIFEST.in").read_text(encoding="utf-8")
        assets = set(DEPLOYMENT_ASSETS) | set(SUPPORTING_DEPLOYMENT_ASSETS)
        for name in assets:
            with self.subTest(asset=name):
                self.assertTrue((repository / "src/rp_ylx/deploy" / name).is_file())
                self.assertTrue(_asset_bytes(name))
                suffix = Path(name).suffix
                if suffix:
                    glob = f"*{suffix}"
                    self.assertIn(glob, pyproject, f"pyproject.toml 未声明 {glob}")
                    self.assertIn(glob, manifest, f"MANIFEST.in 未声明 {glob}")
                else:
                    self.assertIn(f'"{name}"', pyproject, f"pyproject.toml 未声明 {name}")
                    self.assertIn(name, manifest, f"MANIFEST.in 未声明 {name}")

    def test_launchers_use_only_release_managed_runtime(self) -> None:
        application = _launcher("rp_ylx").decode()
        deployment = _system_launcher().decode()
        self.assertIn("$RELEASE_ROOT/runtime/bin/python3", application)
        self.assertIn("/opt/rp-ylx/current/runtime/bin/python3", deployment)
        self.assertIn("/opt/rp-ylx/previous/runtime/bin/python3", deployment)
        self.assertNotIn("/usr/bin/python3", application)
        self.assertNotIn("/usr/bin/python3", deployment)

    def test_network_control_launcher_execs_current_protocol_release(self) -> None:
        commit = self.commit("a")
        release = self.install_root / "releases" / commit
        executable = release / "bin/rp-ylx"
        executable.parent.mkdir(parents=True)
        executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        executable.chmod(0o755)
        _write_atomic(
            release / "release.json",
            {
                "schema": "ylx.installed-release.v1",
                "commit": commit,
                "network_control_protocol": NETWORK_CONTROL_PROTOCOL,
            },
        )
        self.install_root.mkdir(exist_ok=True)
        (self.install_root / "current").symlink_to(f"releases/{commit}")
        executor = MagicMock()
        notifier = MagicMock()

        result = serve_network_control_launcher(
            self.install_root,
            executor=executor,
            notifier=notifier,
        )

        self.assertEqual(result, 0)
        executor.assert_called_once_with(
            str(executable),
            [str(executable), "network-control", "serve", "--stdio"],
        )
        notifier.assert_not_called()

    def test_network_control_launcher_keeps_old_release_socket_ready_fail_closed(self) -> None:
        commit = self.commit("a")
        release = self.install_root / "releases" / commit
        release.mkdir(parents=True)
        _write_atomic(
            release / "release.json",
            {"schema": "ylx.installed-release.v1", "commit": commit},
        )
        self.install_root.mkdir(exist_ok=True)
        (self.install_root / "current").symlink_to(f"releases/{commit}")
        socket_path = self.root / "controller.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(socket_path))
        listener.listen()
        notifications: list[str] = []
        failures: list[BaseException] = []

        def serve() -> None:
            try:
                serve_network_control_launcher(
                    self.install_root,
                    listener=listener,
                    notifier=notifications.append,
                    max_connections=1,
                )
            except BaseException as error:
                failures.append(error)

        worker = threading.Thread(target=serve)
        worker.start()
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.connect(str(socket_path))
            client.sendall(b'{"operation":"apply"}\n')
            response = json.loads(client.recv(4096))
        worker.join(timeout=5)
        listener.close()

        self.assertFalse(worker.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(
            notifications,
            ["rollback release active; network mutation is fail-closed"],
        )
        self.assertFalse(response["ok"])
        self.assertEqual(response["operation"], "apply")
        self.assertEqual(
            response["error"]["code"],
            "network_controller_release_incompatible",
        )

    def test_clean_install_on_system_python_310_uses_packaged_runtime(self) -> None:
        manager = self.manager()
        manager.python_version = (3, 10)
        manager.install(self.bundle("a"), activate=False)
        runtime = manager.releases / self.commit("a") / "runtime/bin/python3"
        self.assertEqual(runtime.read_bytes(), b"managed python runtime\n")
        self.assertTrue(runtime.stat().st_mode & 0o100)

    def test_system_python_bootstrap_safely_extracts_packaged_runtime(self) -> None:
        bundle = self.bundle("a")
        original_extractall = tarfile.TarFile.extractall

        def python_310_extractall(
            archive: tarfile.TarFile,
            path: str | Path = ".",
            members: object = None,
        ) -> None:
            original_extractall(archive, path=path, members=members)

        with patch.object(tarfile.TarFile, "extractall", python_310_extractall):
            runtime = _prepare_runtime(bundle)
        self.assertEqual(runtime, bundle / "runtime/bin/python3")
        self.assertEqual(runtime.read_bytes(), b"managed python runtime\n")
        self.assertTrue(runtime.stat().st_mode & 0o100)

    def test_bootstrap_execs_managed_runtime_even_from_system_python_311(self) -> None:
        bundle = self.bundle("a")
        installer = bundle / "rdk_x5_install.py"
        runtime = bundle / "runtime/bin/python3"
        with (
            patch.object(rdk_x5_install, "__file__", str(installer)),
            patch.object(rdk_x5_install.sys, "executable", "/usr/bin/python3.11"),
            patch.object(rdk_x5_install.sys, "version_info", (3, 11)),
            patch.object(rdk_x5_install.sys, "argv", [str(installer), "status"]),
            patch.object(rdk_x5_install, "_prepare_runtime", return_value=runtime) as prepare,
            patch.object(rdk_x5_install.os, "execv", side_effect=SystemExit) as execute,
            self.assertRaises(SystemExit),
        ):
            rdk_x5_install.main()
        prepare.assert_called_once_with(bundle)
        execute.assert_called_once_with(
            str(runtime), [str(runtime), str(installer.resolve()), "status"]
        )

    def test_system_python_bootstrap_rejects_tampered_and_unsafe_runtime(self) -> None:
        tampered = self.bundle("a")
        archive_path = tampered / "cpython-3.11.13-aarch64.tar.gz"
        archive_path.write_bytes(archive_path.read_bytes() + b"tamper")
        with self.assertRaisesRegex(RuntimeError, "digest check failed"):
            _prepare_runtime(tampered)

        for character, member_type, linkname in (
            ("b", tarfile.REGTYPE, ""),
            ("c", tarfile.SYMTYPE, "../../outside"),
            ("d", tarfile.LNKTYPE, "runtime/bin/python3"),
        ):
            with self.subTest(member_type=member_type):
                bundle = self.bundle(character)
                archive_path = bundle / "cpython-3.11.13-aarch64.tar.gz"
                member_name = (
                    "runtime/../escape" if member_type == tarfile.REGTYPE else "runtime/bin/python3"
                )
                with tarfile.open(archive_path, "w:gz") as archive:
                    member = tarfile.TarInfo(member_name)
                    member.type = member_type
                    member.linkname = linkname
                    if member_type == tarfile.REGTYPE:
                        member.size = len(b"escape")
                        archive.addfile(member, io.BytesIO(b"escape"))
                    else:
                        archive.addfile(member)
                manifest = json.loads((bundle / "bundle.json").read_bytes())
                manifest["runtime"]["bytes"] = archive_path.stat().st_size
                manifest["runtime"]["sha256"] = _sha256(archive_path)
                _write_atomic(bundle / "bundle.json", manifest)
                with self.assertRaisesRegex(RuntimeError, "unsafe package-owned runtime member"):
                    _prepare_runtime(bundle)

    def test_bundle_build_rejects_unsafe_or_incomplete_runtime_archive(self) -> None:
        bundle = self.bundle("a")
        runtime = bundle / "cpython-3.11.13-aarch64.tar.gz"
        (bundle / "bundle.json").unlink()

        with tarfile.open(runtime, "w:gz") as archive:
            member = tarfile.TarInfo("runtime/../escape")
            member.size = len(b"escape")
            archive.addfile(member, io.BytesIO(b"escape"))
        with self.assertRaises(DeploymentError) as unsafe:
            write_bundle_manifest(
                bundle,
                application_wheel=next(bundle.glob("*.whl")).name,
                version="0.1.0",
            )
        self.assertEqual(unsafe.exception.code, "runtime_unsafe")
        self.assertFalse((bundle / "bundle.json").exists())

        with tarfile.open(runtime, "w:gz") as archive:
            member = tarfile.TarInfo("runtime/lib/python3.11/os.py")
            member.size = len(b"pass\n")
            archive.addfile(member, io.BytesIO(b"pass\n"))
        with self.assertRaises(DeploymentError) as incomplete:
            write_bundle_manifest(
                bundle,
                application_wheel=next(bundle.glob("*.whl")).name,
                version="0.1.0",
            )
        self.assertEqual(incomplete.exception.code, "runtime_dependency_missing")
        self.assertFalse((bundle / "bundle.json").exists())

    def test_upstream_runtime_is_normalized_and_internal_symlinks_are_materialized(self) -> None:
        source = self.root / (
            "cpython-3.11.15+20260807-aarch64-unknown-linux-gnu-install_only_stripped.tar.gz"
        )
        output = self.root / "cpython-3.11.15-aarch64.tar.gz"
        with tarfile.open(source, "w:gz") as archive:
            executable = tarfile.TarInfo("python/bin/python3.11")
            executable.mode = 0o755
            executable.size = len(b"managed python runtime\n")
            archive.addfile(executable, io.BytesIO(b"managed python runtime\n"))
            link = tarfile.TarInfo("python/bin/python3")
            link.type = tarfile.SYMTYPE
            link.linkname = "python3.11"
            archive.addfile(link)

        normalize_runtime_archive(source, output)

        with tarfile.open(output, "r:gz") as archive:
            members = {member.name: member for member in archive.getmembers()}
            self.assertEqual(set(members), {"runtime/bin/python3.11", "runtime/bin/python3"})
            self.assertTrue(all(member.isfile() for member in members.values()))
            self.assertEqual(
                archive.extractfile("runtime/bin/python3").read(),
                b"managed python runtime\n",
            )

    def test_runtime_normalization_rejects_links_outside_the_archive_root(self) -> None:
        source = self.root / "unsafe-upstream.tar.gz"
        output = self.root / "normalized.tar.gz"
        with tarfile.open(source, "w:gz") as archive:
            link = tarfile.TarInfo("python/bin/python3")
            link.type = tarfile.SYMTYPE
            link.linkname = "../../../outside"
            archive.addfile(link)
        with self.assertRaises(DeploymentError) as rejected:
            normalize_runtime_archive(source, output)
        self.assertEqual(rejected.exception.code, "runtime_unsafe")
        self.assertFalse(output.exists())

    def test_failed_first_activation_is_not_reported_as_installed(self) -> None:
        manager = self.manager()
        runtime_socket = self.root / "run/rp-ylx/network-control.sock"

        def fail_health() -> None:
            runtime_socket.parent.mkdir(parents=True)
            runtime_socket.write_text("stale", encoding="utf-8")
            raise DeploymentError("service_unhealthy", "gateway health probe failed")

        manager.health_checker = fail_health
        with self.assertRaises(DeploymentError) as rejected:
            manager.install(self.bundle("a"))
        self.assertEqual(rejected.exception.code, "service_unhealthy")
        self.assertIsNone(manager.status()["current"])
        self.assertEqual(manager.status()["previous"], self.commit("a"))
        diagnostic = json.loads((self.state_root / "activation-failure.json").read_bytes())
        self.assertEqual(diagnostic["code"], "service_unhealthy")
        self.assertEqual(manager.status()["activation_failure"], diagnostic)
        self.assertIn(("systemctl", "disable", "--now", "rp-ylx.service"), self.commands)
        self.assertIn(
            ("systemctl", "disable", "--now", "rp-ylx-network-control.socket"),
            self.commands,
        )
        self.assertIn(
            ("systemctl", "stop", "rp-ylx-network-control.service"),
            self.commands,
        )
        self.assertIn(
            ("systemctl", "disable", "--now", "rp-ylx-wifi-watchdog.timer"),
            self.commands,
        )
        self.assertIn(
            ("systemctl", "disable", "--now", "rp-ylx-data-volume.service"),
            self.commands,
        )
        self.assertFalse(runtime_socket.exists())

    def test_first_activation_stop_failure_keeps_release_for_fail_closed_recovery(self) -> None:
        manager = self.manager()

        def run(command: object) -> None:
            normalized = tuple(command)
            self.commands.append(normalized)
            if normalized == (
                "systemctl",
                "disable",
                "--now",
                "rp-ylx-network-control.socket",
            ):
                raise DeploymentError("command_failed", "socket did not stop")

        manager.runner = run

        def fail_health() -> None:
            raise DeploymentError("service_unhealthy", "new release failed health check")

        manager.health_checker = fail_health
        with self.assertRaises(DeploymentError) as rejected:
            manager.install(self.bundle("a"))

        self.assertEqual(rejected.exception.code, "activation_cleanup_failed")
        self.assertEqual(manager.status()["current"], self.commit("a"))
        self.assertIsNone(manager.status()["previous"])
        self.assertTrue((manager.releases / self.commit("a") / "release.json").is_file())
        self.assertEqual(
            json.loads((self.state_root / "activation-failure.json").read_bytes())["code"],
            "service_unhealthy",
        )

    def test_upgrade_stop_failure_aborts_before_assets_or_release_switch(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        service_asset = self.root / "usr/lib/systemd/system/rp-ylx.service"
        asset_before = service_asset.read_bytes()
        self.commands.clear()

        def run(command: object) -> None:
            normalized = tuple(command)
            self.commands.append(normalized)
            if normalized == ("systemctl", "stop", "rp-ylx-network-control.service"):
                raise DeploymentError("command_failed", "controller did not stop")

        manager.runner = run
        with self.assertRaises(DeploymentError) as rejected:
            manager.install(self.bundle("b"))

        self.assertEqual(rejected.exception.code, "unit_quiesce_failed")
        self.assertEqual(manager.status()["current"], self.commit("a"))
        self.assertIsNone(manager.status()["previous"])
        self.assertEqual(service_asset.read_bytes(), asset_before)
        self.assertNotIn(("systemd-sysusers",), self.commands)

    def test_upgrade_skips_only_recover_stop_when_old_release_does_not_have_unit(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        recover_asset = self.root / DEPLOYMENT_ASSETS["rp-ylx-recover.service"][0]
        recover_asset.unlink()
        queried: list[str] = []
        manager.unit_load_state_reader = lambda unit: queried.append(unit) or "not-found"
        self.commands.clear()

        manager.install(self.bundle("b"))

        self.assertEqual(queried, ["rp-ylx-recover.service"])
        self.assertEqual(
            self.commands[:5],
            [
                ("systemctl", "disable", "--now", "rp-ylx-wifi-watchdog.timer"),
                ("systemctl", "stop", "rp-ylx-wifi-watchdog.service"),
                ("systemctl", "stop", "rp-ylx.service"),
                ("systemctl", "stop", "rp-ylx-network-control.socket"),
                ("systemctl", "stop", "rp-ylx-network-control.service"),
            ],
        )
        self.assertNotIn(("systemctl", "stop", "rp-ylx-recover.service"), self.commands)
        self.assertTrue(recover_asset.is_file())
        self.assertEqual(manager.status()["current"], self.commit("b"))
        self.assertEqual(manager.status()["previous"], self.commit("a"))

    def test_upgrade_rejects_invalid_optional_unit_load_state_before_stopping_services(
        self,
    ) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        manager.unit_load_state_reader = lambda unit: ""
        self.commands.clear()

        with self.assertRaises(DeploymentError) as rejected:
            manager.install(self.bundle("b"))

        self.assertEqual(rejected.exception.code, "unit_quiesce_failed")
        self.assertEqual(self.commands, [])
        self.assertEqual(manager.status()["current"], self.commit("a"))

    def test_upgrade_quiesces_first_and_health_failure_restores_old_release(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        self.commands.clear()
        manager.install(self.bundle("b"))
        expected_quiesce = [
            ("systemctl", "disable", "--now", "rp-ylx-wifi-watchdog.timer"),
            ("systemctl", "stop", "rp-ylx-wifi-watchdog.service"),
            ("systemctl", "stop", "rp-ylx.service"),
            ("systemctl", "stop", "rp-ylx-network-control.socket"),
            ("systemctl", "stop", "rp-ylx-network-control.service"),
            ("systemctl", "stop", "rp-ylx-recover.service"),
        ]
        self.assertEqual(self.commands[:6], expected_quiesce)
        self.assertLess(
            self.commands.index(("systemctl", "stop", "rp-ylx-network-control.service")),
            self.commands.index(("systemd-sysusers",)),
        )
        self.assertLess(
            self.commands.index(("health-check",)),
            self.commands.index(("systemctl", "enable", "--now", "rp-ylx-wifi-watchdog.timer")),
        )

        def fail_health() -> None:
            raise DeploymentError("service_unhealthy", "new release failed health check")

        manager.health_checker = fail_health
        self.commands.clear()
        with self.assertRaises(DeploymentError):
            manager.install(self.bundle("c"))
        self.assertEqual(manager.status()["current"], self.commit("b"))
        self.assertEqual(manager.status()["previous"], self.commit("c"))
        self.assertEqual(
            self.commands.count(("systemctl", "stop", "rp-ylx-network-control.service")),
            2,
        )
        self.assertEqual(
            self.commands.count(("systemctl", "enable", "--now", "rp-ylx.service")),
            2,
        )
        self.assertEqual(
            self.commands.count(("systemctl", "enable", "--now", "rp-ylx-wifi-watchdog.timer")),
            1,
        )

    def test_health_wait_accepts_large_session_catalog_after_30_seconds(self) -> None:
        manager = self.manager()
        self.seed_lab_config(manager)
        response = MagicMock()
        response.__enter__.return_value.status = 200
        with (
            patch("rp_ylx.deployment.time.monotonic", side_effect=[0.0, 31.0]),
            patch(
                "rp_ylx.deployment.subprocess.run",
                return_value=subprocess.CompletedProcess([], 0),
            ),
            patch(
                "rp_ylx.deployment.urllib.request.urlopen",
                return_value=response,
            ) as open_url,
        ):
            manager._wait_for_health()
        request = open_url.call_args.args[0]
        self.assertEqual(request.full_url, "http://127.0.0.1:8080/api/v4/device")
        self.assertIsNone(open_url.call_args.kwargs["context"])

    def test_customer_health_uses_verified_https_device_api_and_bearer_token(self) -> None:
        manager = self.manager()
        manager._ensure_layout()
        certificate = self.config_root / CUSTOMER_TLS_CERTIFICATE_RELATIVE
        private_key = self.config_root / CUSTOMER_TLS_PRIVATE_KEY_RELATIVE
        token = self.config_root / "customer.token"
        certificate.parent.mkdir(parents=True)
        certificate.write_bytes(b"trusted-device-certificate")
        private_key.write_bytes(b"private-key")
        token.write_text("a" * 48 + "\n", encoding="ascii")
        config = manager._default_device_config()
        config["security"] = {
            "profile": "customer",
            "isolated_network": True,
            "bearer_token_file": str(token),
            "tls_certificate_file": str(certificate),
            "tls_private_key_file": str(private_key),
            "principal_id": "device-owner",
        }
        _write_atomic(self.config_root / "device.json", config)
        response = MagicMock()
        response.__enter__.return_value.status = 200
        context = MagicMock()
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED
        context.verify_flags = 0
        with (
            patch("rp_ylx.deployment.time.monotonic", side_effect=[0.0, 1.0]),
            patch(
                "rp_ylx.deployment.subprocess.run",
                return_value=subprocess.CompletedProcess([], 0),
            ),
            patch(
                "rp_ylx.deployment.ssl.create_default_context",
                return_value=context,
            ) as create_context,
            patch(
                "rp_ylx.deployment.urllib.request.urlopen",
                return_value=response,
            ) as open_url,
        ):
            manager._wait_for_health()

        create_context.assert_called_once_with(cafile=str(certificate))
        request = open_url.call_args.args[0]
        self.assertEqual(request.full_url, "https://127.0.0.1:8080/api/v4/device")
        self.assertEqual(request.get_header("Authorization"), f"Bearer {'a' * 48}")
        self.assertIs(open_url.call_args.kwargs["context"], context)
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertEqual(context.minimum_version, ssl.TLSVersion.TLSv1_2)

    def test_repeated_install_health_failure_preserves_previous_release(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        bundle = self.bundle("b")
        manager.install(bundle)

        def fail_health() -> None:
            raise DeploymentError("service_unhealthy", "repeat failed health check")

        manager.health_checker = fail_health
        with self.assertRaises(DeploymentError):
            manager.install(bundle)
        self.assertEqual(manager.status()["current"], self.commit("b"))
        self.assertEqual(manager.status()["previous"], self.commit("a"))

    def test_rollback_health_failure_restores_the_release_it_replaced(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        manager.install(self.bundle("b"))

        def fail_health() -> None:
            raise DeploymentError("service_unhealthy", "rollback target failed health check")

        manager.health_checker = fail_health
        self.commands.clear()
        with self.assertRaises(DeploymentError):
            manager.rollback()
        self.assertEqual(manager.status()["current"], self.commit("b"))
        self.assertEqual(manager.status()["previous"], self.commit("a"))
        self.assertEqual(
            self.commands.count(("systemctl", "stop", "rp-ylx-network-control.service")),
            2,
        )
        self.assertEqual(
            self.commands.count(("systemctl", "enable", "--now", "rp-ylx.service")),
            2,
        )

    def test_upgrade_keeps_only_current_and_previous_then_rolls_back(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        manager.install(self.bundle("b"))
        upgraded = manager.install(self.bundle("c"))
        self.assertEqual(upgraded["current"], self.commit("c"))
        self.assertEqual(upgraded["previous"], self.commit("b"))
        self.assertFalse((manager.releases / self.commit("a")).exists())
        self.assertEqual(
            sorted(path.name for path in manager.releases.iterdir()),
            [self.commit("b"), self.commit("c")],
        )
        rolled_back = manager.rollback()
        self.assertEqual(rolled_back["current"], self.commit("b"))
        self.assertEqual(rolled_back["previous"], self.commit("c"))
        self.assertIn(
            ("systemctl", "stop", "rp-ylx-network-control.service"),
            self.commands,
        )

    def test_prepared_install_recovers_after_interruption(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        bundle = load_bundle(self.bundle("b"))
        manager._prepare_release(bundle)
        manager._snapshot_device_config(self.commit("b"))
        transaction = manager._transaction_document(
            "install", self.commit("a"), self.commit("b"), "prepared"
        )
        _write_atomic(manager.transaction, transaction)

        recovered = self.manager().recover()
        self.assertEqual(recovered["current"], self.commit("b"))
        self.assertEqual(recovered["previous"], self.commit("a"))
        self.assertFalse(recovered["transaction_pending"])

    def test_recovery_keeps_legacy_transaction_compatibility_without_snapshots(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"), activate=False)
        bundle = load_bundle(self.bundle("b"))
        manager._prepare_release(bundle)
        transaction = dict(
            manager._transaction_document(
                "install",
                self.commit("a"),
                self.commit("b"),
                "prepared",
            )
        )
        transaction.pop("config_snapshot_required")
        _write_atomic(manager.transaction, transaction)

        recovered = self.manager().recover()

        self.assertEqual(recovered["current"], self.commit("b"))
        self.assertEqual(recovered["previous"], self.commit("a"))
        self.assertFalse(recovered["transaction_pending"])

    def test_switched_install_recovers_before_previous_update(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        bundle = load_bundle(self.bundle("b"))
        manager._prepare_release(bundle)
        manager._snapshot_device_config(self.commit("b"))
        manager._switch("current", self.commit("b"))
        transaction = manager._transaction_document(
            "install", self.commit("a"), self.commit("b"), "switched"
        )
        _write_atomic(manager.transaction, transaction)

        recovered = self.manager().recover()
        self.assertEqual(recovered["current"], self.commit("b"))
        self.assertEqual(recovered["previous"], self.commit("a"))
        self.assertFalse(recovered["transaction_pending"])

    def test_recovery_cancels_new_transaction_without_fsynced_config_snapshot(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"), activate=False)
        original_config = (self.config_root / "device.json").read_bytes()
        bundle = load_bundle(self.bundle("b"))
        manager._prepare_release(bundle)
        transaction = manager._transaction_document(
            "install",
            self.commit("a"),
            self.commit("b"),
            "prepared",
        )
        _write_atomic(manager.transaction, transaction)
        manager._switch("current", self.commit("b"))
        changed = json.loads(original_config)
        changed["listen"]["port"] = 9443
        _write_atomic(self.config_root / "device.json", changed)

        recovered = self.manager().recover()

        self.assertEqual(recovered["current"], self.commit("a"))
        self.assertEqual(recovered["previous"], self.commit("b"))
        self.assertFalse(recovered["transaction_pending"])
        self.assertEqual((self.config_root / "device.json").read_bytes(), original_config)

    def test_incomplete_release_never_becomes_current(self) -> None:
        manager = self.manager()
        manager._ensure_layout()
        transaction = manager._transaction_document("install", None, self.commit("a"), "prepared")
        _write_atomic(manager.transaction, transaction)
        with self.assertRaises(DeploymentError) as rejected:
            manager.recover()
        self.assertEqual(rejected.exception.code, "release_incomplete")
        self.assertIsNone(manager.status()["current"])

    def test_bundle_rejects_digest_commit_and_non_aarch64_wheels(self) -> None:
        digest = self.bundle("a")
        wheel = next(digest.glob("*.whl"))
        wheel.write_bytes(wheel.read_bytes() + b"tamper")
        with self.assertRaises(DeploymentError) as mismatch:
            load_bundle(digest)
        self.assertEqual(mismatch.exception.code, "bundle_digest_mismatch")

        with self.assertRaises(DeploymentError) as commit:
            load_bundle(self.bundle("b", manifest_commit=self.commit("c")))
        self.assertEqual(commit.exception.code, "bundle_invalid")

        with self.assertRaises(DeploymentError) as platform_error:
            load_bundle(self.bundle("d", platform_tag="manylinux_2_17_x86_64"))
        self.assertEqual(platform_error.exception.code, "bundle_platform_mismatch")

        with self.assertRaises(DeploymentError) as musl_error:
            load_bundle(self.bundle("e", platform_tag="musllinux_1_2_aarch64"))
        self.assertEqual(musl_error.exception.code, "bundle_platform_mismatch")

    def test_uninstall_preserves_config_state_and_recording_mount(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        config = self.config_root / "device.json"
        business = self.state_root / "business-state.json"
        business.write_text("keep", encoding="utf-8")
        recording = self.root / "mnt/ylx-recording/session.bin"
        recording.parent.mkdir(parents=True)
        recording.write_bytes(b"keep")

        advertised = self.root / "etc/avahi/services/rp-ylx.service"
        self.assertTrue(advertised.is_file())
        runtime_socket = self.root / "run/rp-ylx/network-control.sock"
        runtime_socket.parent.mkdir(parents=True)
        runtime_socket.write_text("stale", encoding="utf-8")

        result = manager.uninstall()
        self.assertFalse(result["installed"])
        self.assertFalse(self.install_root.exists())
        self.assertTrue(config.is_file())
        self.assertEqual(business.read_text(), "keep")
        self.assertEqual(recording.read_bytes(), b"keep")
        self.assertIn(
            ("systemctl", "disable", "--now", "rp-ylx-wifi-watchdog.timer"),
            self.commands,
        )
        self.assertIn(
            ("systemctl", "disable", "--now", "rp-ylx-network-control.socket"),
            self.commands,
        )
        self.assertIn(
            ("systemctl", "stop", "rp-ylx-network-control.service"),
            self.commands,
        )
        self.assertIn(
            ("systemctl", "disable", "--now", "rp-ylx-data-volume.service"),
            self.commands,
        )
        self.assertFalse(runtime_socket.exists())
        for relative, _ in DEPLOYMENT_ASSETS.values():
            self.assertFalse((self.root / relative).exists())
        # 卸载后设备不得继续广播一个已不存在的采集服务。
        self.assertFalse(advertised.exists())

    def test_uninstall_stop_failure_preserves_all_installation_files(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        service_asset = self.root / "usr/lib/systemd/system/rp-ylx.service"
        bootstrap = self.root / "usr/local/sbin/rp-ylx-deploy"

        def fail_controller_stop(command: object) -> None:
            normalized = tuple(command)
            self.commands.append(normalized)
            if normalized == ("systemctl", "stop", "rp-ylx-network-control.service"):
                raise DeploymentError("command_failed", "controller still active")

        manager.runner = fail_controller_stop
        with self.assertRaises(DeploymentError) as rejected:
            manager.uninstall()

        self.assertEqual(rejected.exception.code, "unit_deactivation_failed")
        self.assertTrue(self.install_root.exists())
        self.assertTrue(service_asset.is_file())
        self.assertTrue(bootstrap.is_file())
        self.assertEqual(manager.status()["current"], self.commit("a"))

    def test_x86_install_fails_before_writing_layout(self) -> None:
        manager = ReleaseManager(
            install_root=self.install_root,
            config_root=self.config_root,
            state_root=self.state_root,
            system_root=self.root,
            target_machine="x86_64",
            runner=lambda command: None,
            stage_installer=self.stage_installer,
            encoder_builder=lambda stage: None,
        )
        with self.assertRaises(DeploymentError) as rejected:
            manager.install(self.bundle("a"))
        self.assertEqual(rejected.exception.code, "unsupported_platform")
        self.assertFalse(self.install_root.exists())

    def test_non_x5_aarch64_install_fails_before_writing_layout(self) -> None:
        manager = ReleaseManager(
            install_root=self.install_root,
            config_root=self.config_root,
            state_root=self.state_root,
            system_root=self.root,
            target_machine="aarch64",
            target_model="Raspberry Pi 5 Model B Rev 1.0",
            runner=lambda command: None,
            stage_installer=self.stage_installer,
            encoder_builder=lambda stage: None,
        )
        with self.assertRaises(DeploymentError) as rejected:
            manager.install(self.bundle("a"))
        self.assertEqual(rejected.exception.code, "unsupported_platform")
        self.assertFalse(self.install_root.exists())

    def test_missing_native_runtime_fails_before_writing_layout(self) -> None:
        manager = ReleaseManager(
            install_root=self.install_root,
            config_root=self.config_root,
            state_root=self.state_root,
            system_root=self.root,
            target_machine="aarch64",
            target_model="D-Robotics RDK X5 V1.0",
            library_finder=lambda name: None,
            executable_finder=lambda name: f"/usr/bin/{name}",
            runner=lambda command: None,
            stage_installer=self.stage_installer,
            encoder_builder=lambda stage: None,
        )
        with self.assertRaises(DeploymentError) as rejected:
            manager.install(self.bundle("a"))
        self.assertEqual(rejected.exception.code, "runtime_dependency_missing")
        self.assertFalse(self.install_root.exists())
        self.assertFalse(self.config_root.exists())
        self.assertFalse(self.state_root.exists())

    def test_wheel_data_directory_is_rejected_before_extraction(self) -> None:
        wheel = self.root / "unsafe-py3-none-any.whl"
        with zipfile.ZipFile(wheel, "w") as archive:
            archive.writestr("unsafe-1.0.data/scripts/command", "payload")
        target = self.root / "site-packages"
        target.mkdir()
        with self.assertRaises(DeploymentError) as rejected:
            _extract_wheel(wheel, target, max_bytes=1024)
        self.assertEqual(rejected.exception.code, "wheel_unsafe")
        self.assertEqual(list(target.iterdir()), [])

    def test_uninstall_remains_available_when_native_runtime_is_missing(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        manager.library_finder = lambda name: None
        result = manager.uninstall()
        self.assertFalse(result["installed"])
        self.assertFalse(self.install_root.exists())
        self.assertTrue(self.config_root.exists())
        self.assertTrue(self.state_root.exists())

    def test_recover_remains_available_when_native_runtime_is_missing(self) -> None:
        manager = self.manager()
        manager.install(self.bundle("a"))
        bundle = load_bundle(self.bundle("b"))
        manager._prepare_release(bundle)
        manager._snapshot_device_config(self.commit("b"))
        transaction = manager._transaction_document(
            "install", self.commit("a"), self.commit("b"), "prepared"
        )
        _write_atomic(manager.transaction, transaction)
        manager.library_finder = lambda name: None
        recovered = manager.recover()
        self.assertEqual(recovered["current"], self.commit("b"))
        self.assertEqual(recovered["previous"], self.commit("a"))

    def test_zip_wheel_bootstrap_can_install_without_source_tree(self) -> None:
        commit = self.commit("a")
        bundle = self.root / "zip-bootstrap-bundle"
        bundle.mkdir()
        wheel = bundle / "rp_ylx-0.1.0-cp311-abi3-manylinux_2_28_aarch64.whl"
        repository = Path(__file__).resolve().parents[1]
        with zipfile.ZipFile(wheel, "w") as archive:
            archive.writestr("rp_ylx/__init__.py", "from ._build_info import __commit__\n")
            archive.writestr("rp_ylx/deploy/__init__.py", "")
            archive.write(repository / "src/rp_ylx/deployment.py", "rp_ylx/deployment.py")
            archive.writestr("rp_ylx/_build_info.py", f'__commit__ = "{commit}"\n')
            for name in DEPLOYMENT_ASSETS:
                archive.write(repository / "src/rp_ylx/deploy" / name, f"rp_ylx/deploy/{name}")
            for name in SUPPORTING_DEPLOYMENT_ASSETS:
                archive.write(repository / "src/rp_ylx/deploy" / name, f"rp_ylx/deploy/{name}")
            archive.writestr(
                "rp_ylx-0.1.0.dist-info/METADATA",
                "Metadata-Version: 2.1\nName: rp-ylx\nVersion: 0.1.0\n",
            )
            elf = bytearray(64)
            elf[:6] = b"\x7fELF\x02\x01"
            elf[18:20] = (183).to_bytes(2, "little")
            archive.writestr("rp_ylx/_native.abi3.so", elf)
        installer = bundle / "rdk_x5_install.py"
        installer.write_text("# bootstrap\n", encoding="utf-8")
        manifest = {
            "schema": BUNDLE_SCHEMA,
            "platform": TARGET_PLATFORM,
            "commit": commit,
            "version": "0.1.0",
            "application_wheel": wheel.name,
            "installer": {
                "file": installer.name,
                "bytes": installer.stat().st_size,
                "sha256": _sha256(installer),
            },
            "runtime": self.runtime_descriptor(bundle),
            "wheels": [
                {"file": wheel.name, "bytes": wheel.stat().st_size, "sha256": _sha256(wheel)}
            ],
        }
        _write_atomic(bundle / "bundle.json", manifest)
        isolated_root = self.root / "zip-installed-root"
        code = "\n".join(
            (
                "import json, os, sys",
                "from pathlib import Path",
                f"sys.path.insert(0, {str(wheel)!r})",
                "from rp_ylx.deployment import ReleaseManager",
                f"root = Path({str(isolated_root)!r})",
                "manager = ReleaseManager(",
                "    install_root=root / 'opt/rp-ylx',",
                "    config_root=root / 'etc/rp-ylx',",
                "    state_root=root / 'var/lib/rp-ylx',",
                "    system_root=root,",
                "    target_machine='aarch64',",
                "    target_model='D-Robotics RDK X5 V1.0',",
                "    library_finder=lambda name: f'lib{name}.so',",
                "    executable_finder=lambda name: f'/usr/bin/{name}',",
                "    encoder_builder=lambda stage: None,",
                "    runner=lambda command: None,",
                "    health_checker=lambda: None,",
                "    group_resolver=lambda name: os.getgid(),",
                ")",
                f"print(json.dumps(manager.install(Path({str(bundle)!r}), activate=False)))",
            )
        )
        result = subprocess.run(
            [sys.executable, "-I", "-c", code],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["current"], commit)
        bootstrap = isolated_root / "usr/local/lib/rp-ylx/deployment.py"
        source_module = repository / "src/rp_ylx/deployment.py"
        self.assertEqual(bootstrap.read_bytes(), source_module.read_bytes())
        self.assertTrue((isolated_root / "usr/lib/systemd/system/rp-ylx.service").is_file())
        self.assertTrue(
            (isolated_root / "usr/lib/systemd/system/rp-ylx-data-volume.service").is_file()
        )
        self.assertTrue(
            (isolated_root / "usr/lib/systemd/system/rp-ylx-network-control.socket").is_file()
        )
        self.assertTrue((isolated_root / "usr/local/sbin/rp-ylx-data-volume").is_file())
        self.assertTrue((isolated_root / "usr/local/sbin/rp-ylx-wifi-watchdog").is_file())

    def test_service_declares_boot_restart_and_privileged_recovery(self) -> None:
        root = Path(__file__).resolve().parents[1] / "src/rp_ylx/deploy"
        unit = (root / "rp-ylx.service").read_text(encoding="utf-8")
        recover = (root / "rp-ylx-recover.service").read_text(encoding="utf-8")
        self.assertIn("WantedBy=multi-user.target", unit)
        self.assertIn("Restart=on-failure", unit)
        self.assertIn("Wants=rp-ylx-data-volume.service network-online.target", unit)
        self.assertIn("After=rp-ylx-data-volume.service", unit)
        self.assertIn(
            "Requires=rp-ylx-recover.service rp-ylx-network-control.socket "
            "rp-ylx-network-control.service",
            unit,
        )
        self.assertIn(
            "After=rp-ylx-data-volume.service rp-ylx-recover.service "
            "rp-ylx-network-control.socket rp-ylx-network-control.service",
            unit,
        )
        self.assertNotIn("ExecStartPre", unit)
        self.assertIn("ExecStart=/opt/rp-ylx/current/bin/rp-ylx serve", unit)
        self.assertIn("SupplementaryGroups=video vpu jpu vps misc", unit)
        self.assertIn("ReadWritePaths=/run/rp-ylx/network-operation.lock", unit)
        self.assertIn("ExecStart=/usr/local/sbin/rp-ylx-deploy recover", recover)
        self.assertIn(
            "Before=rp-ylx-data-volume.service rp-ylx-network-control.service rp-ylx.service",
            recover,
        )
        self.assertIn("TimeoutStartSec=120s", recover)
        self.assertIn("ProtectSystem=strict", recover)
        self.assertIn("ReadWritePaths=/etc/rp-ylx", recover)
        self.assertIn("ReadWritePaths=/etc/avahi/services", recover)
        self.assertNotIn("raspberry", unit.casefold())

    def test_data_volume_asset_uses_data_mount_and_reserved_backing_file(self) -> None:
        root = Path(__file__).resolve().parents[1] / "src/rp_ylx/deploy"
        script = root / "rp-ylx-data-volume"
        syntax = subprocess.run(
            ["bash", "-n", str(script)], check=False, capture_output=True, text=True
        )
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        content = script.read_text(encoding="utf-8")
        self.assertIn("/var/lib/rp-ylx-data-volume/data-volume.img", content)
        self.assertIn("/data", content)
        self.assertIn("mkfs.ext4 -F -E nodiscard", content)
        self.assertIn("mountpoint -q", content)
        service = (root / "rp-ylx-data-volume.service").read_text(encoding="utf-8")
        self.assertIn("ExecStart=/usr/local/sbin/rp-ylx-data-volume ensure", service)
        self.assertIn("Requires=rp-ylx-recover.service", service)
        self.assertIn("After=local-fs.target rp-ylx-recover.service", service)
        sysusers = (root / "rp-ylx.sysusers").read_text(encoding="utf-8")
        self.assertIn("m rp-ylx misc", sysusers)
        tmpfiles = (root / "rp-ylx.tmpfiles").read_text(encoding="utf-8")
        self.assertIn("d /var/lib/rp-ylx-data-volume 0755 root root -", tmpfiles)
        self.assertIn("d /var/lib/rp-ylx-network 0700 root root -", tmpfiles)
        self.assertIn("d /run/rp-ylx 0750 root rp-ylx -", tmpfiles)
        self.assertIn(
            "f /run/rp-ylx/network-operation.lock 0660 root rp-ylx -",
            tmpfiles,
        )

    def test_wifi_watchdog_assets_are_syntactically_valid_and_safely_wired(self) -> None:
        root = Path(__file__).resolve().parents[1] / "src/rp_ylx/deploy"
        script = root / "rp-ylx-wifi-watchdog"
        syntax = subprocess.run(
            ["sh", "-n", str(script)], check=False, capture_output=True, text=True
        )
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        service = (root / "rp-ylx-wifi-watchdog.service").read_text(encoding="utf-8")
        timer = (root / "rp-ylx-wifi-watchdog.timer").read_text(encoding="utf-8")
        watchdog = script.read_text(encoding="utf-8")
        self.assertIn("ExecStart=/usr/local/sbin/rp-ylx-wifi-watchdog check", service)
        self.assertIn("OnUnitActiveSec=1min", timer)
        self.assertIn("TimeoutStartSec=5min", service)
        self.assertIn("RuntimeDirectoryPreserve=yes", service)
        self.assertIn('modprobe -r "$DRIVER_MODULE"', watchdog)
        self.assertNotIn("nmcli connection up", watchdog)
        self.assertIn("network-control watchdog-mode", watchdog)
        self.assertIn("systemctl restart rp-ylx-network-control.service", watchdog)
        self.assertIn(
            "RP_YLX_NETWORK_OPERATION_LOCK_PATH=/run/rp-ylx/network-operation.lock",
            service,
        )
        self.assertIn("ReadWritePaths=/run/rp-ylx/network-operation.lock", service)
        self.assertIn("NoNewPrivileges=yes", service)
        self.assertIn("CapabilityBoundingSet=CAP_DAC_READ_SEARCH", service)
        self.assertIn('exec 8<>"$NETWORK_OPERATION_LOCK"', watchdog)
        self.assertLess(
            watchdog.index('exec 8<>"$NETWORK_OPERATION_LOCK"'),
            watchdog.index("network-control watchdog-mode"),
        )
        self.assertIn("desired hotspot mode does not require an upstream default route", watchdog)
        self.assertIn("desired ethernet mode does not permit Wi-Fi recovery", watchdog)
        self.assertIn("desired network mode is unavailable; deferring Wi-Fi recovery", watchdog)
        self.assertIn("network transaction or rescue outcome requires watchdog to defer", watchdog)
        self.assertIn("automatic reboot is in cooldown", watchdog)
        self.assertIn("systemctl reboot", watchdog)

    def test_network_control_socket_is_staged_as_root_only_fail_closed_boundary(self) -> None:
        root = Path(__file__).resolve().parents[1] / "src/rp_ylx/deploy"
        socket = (root / "rp-ylx-network-control.socket").read_text(encoding="utf-8")
        service = (root / "rp-ylx-network-control.service").read_text(encoding="utf-8")

        self.assertIn("ListenStream=/run/rp-ylx/network-control.sock", socket)
        self.assertIn("SocketUser=rp-ylx", socket)
        self.assertIn("SocketGroup=rp-ylx", socket)
        self.assertIn("SocketMode=0600", socket)
        self.assertIn("DirectoryMode=0750", socket)
        self.assertIn("User=root", service)
        self.assertIn("StandardInput=socket", service)
        self.assertIn("StandardOutput=socket", service)
        self.assertIn(
            "ExecStart=/usr/local/sbin/rp-ylx-deploy network-control-launch",
            service,
        )
        self.assertIn("TimeoutStartSec=120s", service)
        self.assertIn("ProtectSystem=strict", service)
        self.assertIn("NoNewPrivileges=yes", service)
        self.assertIn("PrivateDevices=yes", service)
        self.assertIn("ProtectKernelTunables=yes", service)
        self.assertIn("ProtectKernelModules=yes", service)
        self.assertIn("ProtectControlGroups=yes", service)
        self.assertIn("CapabilityBoundingSet=\n", service)
        self.assertIn(
            "Environment=RP_YLX_NETWORK_STATE_DIR=/var/lib/rp-ylx-network",
            service,
        )
        self.assertIn("ReadWritePaths=/var/lib/rp-ylx-network", service)
        self.assertIn("ReadWritePaths=/etc/NetworkManager/system-connections", service)
        self.assertIn("ReadWritePaths=/etc/avahi/services", service)
        self.assertIn("ReadWritePaths=/run/rp-ylx/network-operation.lock", service)
        self.assertIn("Before=rp-ylx.service", service)
        self.assertIn("Type=notify", service)
        self.assertIn("NotifyAccess=main", service)
        self.assertIn(
            "Requires=NetworkManager.service rp-ylx-recover.service rp-ylx-network-control.socket",
            service,
        )
        self.assertNotIn("/var/lib/rp-ylx/network", service)
        self.assertNotIn("rp-ylx serve", service)

    def test_wifi_watchdog_reboots_once_when_driver_reload_does_not_restore_gateway(self) -> None:
        root = Path(__file__).resolve().parents[1] / "src/rp_ylx/deploy"
        script = root / "rp-ylx-wifi-watchdog"
        fake_bin = self.root / "fake-bin"
        fake_bin.mkdir()
        calls = self.root / "systemctl-calls"
        for command in ("ip", "iw", "journalctl", "logger", "nmcli", "sleep"):
            fake = fake_bin / command
            fake.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            fake.chmod(0o755)
        ping = fake_bin / "ping"
        ping.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
        ping.chmod(0o755)
        modprobe = fake_bin / "modprobe"
        modprobe.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        modprobe.chmod(0o755)
        systemctl = fake_bin / "systemctl"
        systemctl.write_text(
            f"#!/bin/sh\nprintf '%s\\n' \"$*\" >> {calls}\n",
            encoding="utf-8",
        )
        systemctl.chmod(0o755)
        rp_ylx = fake_bin / "rp-ylx"
        rp_ylx.write_text("#!/bin/sh\nprintf 'wifi-client\\n'\n", encoding="utf-8")
        rp_ylx.chmod(0o755)
        sys_class_net = self.root / "sys/class/net"
        (sys_class_net / "wlan0").mkdir(parents=True)
        operation_lock = self.root / "run/rp-ylx/network-operation.lock"
        operation_lock.parent.mkdir(parents=True)
        operation_lock.touch()
        environment = os.environ | {
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "STATE_DIRECTORY": str(self.root / "state"),
            "RUNTIME_DIRECTORY": str(self.root / "run"),
            "WIFI_SYS_CLASS_NET": str(sys_class_net),
            "WIFI_CONNECTION_UUID": "00000000-0000-0000-0000-000000000001",
            "WIFI_GATEWAY": "192.0.2.1",
            "WIFI_RELOAD_TIMEOUT_SECONDS": "2",
            "WIFI_REBOOT_ON_RECOVERY_FAILURE": "1",
            "WIFI_REBOOT_COOLDOWN_SECONDS": "21600",
            "RP_YLX_COMMAND": str(rp_ylx),
            "RP_YLX_NETWORK_OPERATION_LOCK_PATH": str(operation_lock),
        }

        first = subprocess.run(
            [str(script), "recover"],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        second = subprocess.run(
            [str(script), "recover"],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        last_reboot = self.root / "state/last-automatic-reboot-epoch"
        last_reboot.write_text("4102444800\n", encoding="utf-8")
        third = subprocess.run(
            [str(script), "recover"],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )

        self.assertEqual(first.returncode, 1, first.stderr)
        self.assertEqual(second.returncode, 1, second.stderr)
        self.assertEqual(third.returncode, 1, third.stderr)
        self.assertEqual(
            calls.read_text(encoding="utf-8").splitlines(),
            [
                "restart rp-ylx-network-control.service",
                "reboot",
                "restart rp-ylx-network-control.service",
                "restart rp-ylx-network-control.service",
            ],
        )
        self.assertIn("automatic reboot is in cooldown", second.stdout)
        self.assertIn("automatic reboot is in cooldown", third.stdout)

    def test_wifi_watchdog_only_recovers_explicit_wifi_client_mode(self) -> None:
        root = Path(__file__).resolve().parents[1] / "src/rp_ylx/deploy"
        script = root / "rp-ylx-wifi-watchdog"
        fake_bin = self.root / "watchdog-mode-bin"
        fake_bin.mkdir()
        dangerous_calls = self.root / "dangerous-watchdog-calls"
        logger = fake_bin / "logger"
        logger.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        logger.chmod(0o755)
        for name in ("ip", "iw", "ping", "nmcli", "modprobe", "systemctl"):
            command = fake_bin / name
            command.write_text(
                f"#!/bin/sh\nprintf '%s\\n' '{name}' >> {dangerous_calls}\nexit 1\n",
                encoding="utf-8",
            )
            command.chmod(0o755)

        expectations = {
            "hotspot": "desired hotspot mode does not require",
            "ethernet-dhcp": "desired ethernet mode does not permit Wi-Fi recovery",
            "ethernet-static": "desired ethernet mode does not permit Wi-Fi recovery",
            "unknown": "desired network mode is unavailable; deferring Wi-Fi recovery",
            "defer": "network transaction or rescue outcome requires watchdog to defer",
        }
        for mode, message in expectations.items():
            with self.subTest(mode=mode):
                desired_mode = self.root / f"rp-ylx-{mode}"
                desired_mode.write_text(
                    f"#!/bin/sh\nprintf '%s\\n' '{mode}'\n",
                    encoding="utf-8",
                )
                desired_mode.chmod(0o755)
                operation_lock = self.root / f"lock-{mode}"
                operation_lock.touch()
                result = subprocess.run(
                    [str(script), "recover"],
                    check=False,
                    capture_output=True,
                    text=True,
                    env=os.environ
                    | {
                        "PATH": f"{fake_bin}:{os.environ['PATH']}",
                        "STATE_DIRECTORY": str(self.root / f"watchdog-state-{mode}"),
                        "RUNTIME_DIRECTORY": str(self.root / f"watchdog-run-{mode}"),
                        "RP_YLX_COMMAND": str(desired_mode),
                        "RP_YLX_NETWORK_OPERATION_LOCK_PATH": str(operation_lock),
                    },
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(message, result.stdout)
        self.assertFalse(dangerous_calls.exists())


if __name__ == "__main__":
    unittest.main()
