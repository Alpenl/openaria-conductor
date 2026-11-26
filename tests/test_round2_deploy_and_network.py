"""Round 2 unit and regression tests for deploy_pkg, systemd assets, network, update, and export."""

from __future__ import annotations

import errno
import importlib
import io
import json
import os
import socket
import stat
import struct
import subprocess
import sys
import tempfile
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext, redirect_stderr
from importlib.resources import files
from pathlib import Path
from unittest.mock import patch

import rp_ylx.deploy_pkg.cli as deploy_cli
import rp_ylx.deploy_pkg.control_shim as control_shim
import rp_ylx.deploy_pkg.network_migration as network_migration
import rp_ylx.deploy_pkg.storage as deploy_storage
import rp_ylx.deploy_pkg.systemd as deploy_systemd
import rp_ylx.network_control as network_control
import rp_ylx.network_paths as network_paths
import rp_ylx.persistence as persistence
import rp_ylx.update as update_mod
from rp_ylx.deploy_pkg.schema import (
    DEPLOYMENT_ASSETS,
    PRESERVED_DEPLOYMENT_ASSETS,
    TRANSACTION_SCHEMA,
    DeploymentError,
    _write_atomic,
)
from tests.testkit.deployment import DeploymentHarness


def _build_synthetic_exfat_image(*, file_data: bytes, valid_data_length: int) -> io.BytesIO:
    """Construct a minimal single-cluster-per-file exFAT filesystem image in memory."""
    sector_size = 512
    sectors_per_cluster = 1
    fat_offset = 1
    fat_length = 1
    heap_offset = 2
    cluster_count = 16
    root_cluster = 2
    total_sectors = heap_offset + cluster_count * sectors_per_cluster
    buf = bytearray(total_sectors * sector_size)

    # VBR
    buf[3:11] = b"EXFAT   "
    struct.pack_into("<I", buf, 80, fat_offset)
    struct.pack_into("<I", buf, 84, fat_length)
    struct.pack_into("<I", buf, 88, heap_offset)
    struct.pack_into("<I", buf, 92, cluster_count)
    struct.pack_into("<I", buf, 96, root_cluster)
    struct.pack_into("<H", buf, 106, 0)
    buf[108] = 9  # 512 bytes/sector
    buf[109] = 0  # 1 sector/cluster
    buf[110] = 1  # number of FATs
    buf[510:512] = b"\x55\xaa"

    # FAT entries: cluster 2 (root) EOF, cluster 3 (file) EOF
    fat_base = fat_offset * sector_size
    struct.pack_into("<I", buf, fat_base + 2 * 4, 0xFFFFFFFF)
    struct.pack_into("<I", buf, fat_base + 3 * 4, 0xFFFFFFFF)

    # Root directory at cluster 2:
    # 0: 0x85 file entry (secondary_count = 3: 0xC0 + 0xC1 + benign 0xE5 metadata entry)
    root_off = heap_offset * sector_size
    file_size = len(file_data) + 128  # Declared DataLength extends beyond ValidDataLength
    buf[root_off] = 0x85
    buf[root_off + 1] = 3
    struct.pack_into("<H", buf, root_off + 4, 0x20)  # regular file

    # 1: 0xC0 stream extension
    stream_off = root_off + 32
    buf[stream_off] = 0xC0
    buf[stream_off + 1] = 0x01  # AllocationPossible
    buf[stream_off + 3] = 4  # NameLength = 4 ("a.tx")
    struct.pack_into("<Q", buf, stream_off + 8, valid_data_length)
    struct.pack_into("<I", buf, stream_off + 20, 3)  # FirstCluster = 3
    struct.pack_into("<Q", buf, stream_off + 24, file_size)

    # 2: 0xC1 file name
    name_off = root_off + 64
    buf[name_off] = 0xC1
    encoded_name = "a.tx".encode("utf-16-le")
    buf[name_off + 2 : name_off + 2 + len(encoded_name)] = encoded_name

    # 3: 0xE5 benign secondary metadata entry (0xE0..0xFF must be skipped, not rejected)
    meta_off = root_off + 96
    buf[meta_off] = 0xE5

    # Cluster 3 payload (non-zero garbage after valid_data_length to prove zero-fill!)
    cluster3_off = (heap_offset + (3 - 2)) * sector_size
    buf[cluster3_off : cluster3_off + len(file_data)] = file_data
    buf[cluster3_off + valid_data_length : cluster3_off + file_size] = b"\xff" * (
        file_size - valid_data_length
    )
    return io.BytesIO(buf)


class DeployPkgTests(unittest.TestCase):
    def test_schema_parity_with_persistence(self) -> None:
        self.assertEqual(network_migration._STATE_TABLES, persistence._STATE_TABLES)
        self.assertEqual(network_migration._SQLITE_SCHEMA_VERSION, persistence.SCHEMA_VERSION)
        self.assertEqual(network_migration._SQLITE_APPLICATION_ID, persistence.APPLICATION_ID)

    def test_cli_unexpected_exception_outputs_json_unexpected_error_exit_2(self) -> None:
        class _BoomManager:
            def __init__(self, **_kw: object) -> None:
                pass

            def status(self) -> dict[str, object]:
                raise RuntimeError("boom!")

        stderr = io.StringIO()
        with (
            patch.object(deploy_cli, "facade_attr", return_value=_BoomManager),
            redirect_stderr(stderr),
        ):
            code = deploy_cli.main(["status"])
        self.assertEqual(code, 2)
        payload = json.loads(stderr.getvalue())
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "unexpected_error")
        self.assertIn("RuntimeError: boom!", payload["error"]["message"])

    def test_systemd_start_release_services_rejects_corrupt_or_nondict_recording_button_json(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_root = root / "etc/rp-ylx"
            cfg_root.mkdir(parents=True)
            button = cfg_root / "recording-button.json"

            for raw in (b"{not-json", b"[1, 2, 3]"):
                button.write_bytes(raw)
                with self.assertRaises(DeploymentError) as err:
                    deploy_systemd.start_release_services(
                        lambda _cmd: None,
                        cfg_root,
                        root,
                        check_health=False,
                        health_checker=lambda: None,
                    )
                self.assertEqual(err.exception.code, "production_config_invalid")

    def test_storage_fail_closed_and_release_supports_exfat_checks_data_volume_script(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = root / deploy_storage.DATA_VOLUME_CONFIG_RELATIVE
            cfg.parent.mkdir(parents=True)
            cfg.write_text("INVALID LINE WITHOUT EQUALS\n", encoding="utf-8")
            with (
                patch.dict(os.environ, {}, clear=False),
                self.assertRaises(DeploymentError) as err,
            ):
                os.environ.pop("RP_YLX_DATA_VOLUME_MODE", None)
                deploy_storage.exfat_storage_required(root)
            self.assertEqual(err.exception.code, "storage_schema_incompatible")

            pkg = root / "pkg"
            (pkg / "recording").mkdir(parents=True)
            (pkg / "deploy").mkdir(parents=True)
            (pkg / "recording/volume.py").write_text("f_files == 0\n", encoding="utf-8")
            # Missing rp-ylx-data-volume script -> False
            self.assertFalse(deploy_storage.release_supports_exfat(pkg))
            # Script missing PARTUUID -> False
            dv = pkg / "deploy/rp-ylx-data-volume"
            dv.write_text("#!/bin/sh\necho exfat\n", encoding="utf-8")
            self.assertFalse(deploy_storage.release_supports_exfat(pkg))
            # Script containing both exfat and PARTUUID -> True
            dv.write_text("#!/bin/sh\necho exfat PARTUUID\n", encoding="utf-8")
            self.assertTrue(deploy_storage.release_supports_exfat(pkg))

    def test_transaction_umask_permissions_recover_assets_before_unlink_and_uninstall_preserves(
        self,
    ) -> None:
        with DeploymentHarness() as harness:
            bundle = harness.bundle("a")
            manager = harness.manager()

            # 1. Under restrictive umask(0o077), install still creates 0755/0644 layout.
            old_umask = os.umask(0o077)
            try:
                status = manager.install(bundle, activate=False)
            finally:
                os.umask(old_umask)
            self.assertTrue(status["installed"])
            release_dir = harness.install_root / "releases" / harness.commit("a")
            self.assertEqual(stat.S_IMODE(harness.install_root.stat().st_mode), 0o755)
            self.assertEqual(stat.S_IMODE(release_dir.stat().st_mode), 0o755)
            self.assertEqual(stat.S_IMODE((release_dir / "release.json").stat().st_mode), 0o644)

            # 2. _recover_locked restores assets BEFORE deleting self.transaction.
            commit_a = harness.commit("a")
            (release_dir / "site-packages/rp_ylx/deploy").mkdir(parents=True)
            tx = {
                "schema": TRANSACTION_SCHEMA,
                "transaction_id": str(uuid.uuid4()),
                "action": "install",
                "old_current": None,
                "new_current": commit_a,
                "state": "prepared",
                "config_snapshot_required": True,
            }
            _write_atomic(manager.transaction, tx, mode=0o600)
            events: list[str] = []
            real_install_assets = manager._install_assets
            real_settle = manager._settle

            def tracked_install_assets(*, release=None):
                events.append(f"install_assets:{manager.transaction.exists()}")
                return real_install_assets(release=release)

            def tracked_settle(txn):
                events.append(f"settle:{manager.transaction.exists()}")
                return real_settle(txn)

            with (
                patch.object(manager, "_install_assets", side_effect=tracked_install_assets),
                patch.object(manager, "_settle", side_effect=tracked_settle),
            ):
                manager._recover_locked()
            self.assertEqual(events, ["install_assets:True", "settle:True"])
            self.assertFalse(manager.transaction.exists())

            # 3. uninstall preserves PRESERVED_DEPLOYMENT_ASSETS.
            for preserved_name in PRESERVED_DEPLOYMENT_ASSETS:
                rel, _ = DEPLOYMENT_ASSETS[preserved_name]
                target = harness.root / rel
                self.assertTrue(target.exists(), f"missing preserved asset {preserved_name}")
            manager.uninstall()
            for preserved_name in PRESERVED_DEPLOYMENT_ASSETS:
                rel, _ = DEPLOYMENT_ASSETS[preserved_name]
                self.assertTrue((harness.root / rel).exists())

    def test_control_shim_cumulative_deadline_times_out_trickled_reads(self) -> None:
        left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        now = 0.0

        def fake_monotonic() -> float:
            nonlocal now
            now += 0.75
            return now

        with (
            left,
            right,
            patch.object(control_shim.time, "monotonic", side_effect=fake_monotonic),
        ):
            right.sendall(b'{"operation":')
            with self.assertRaises(TimeoutError):
                control_shim._read_control_compatibility_request(left)


class SystemdUnitAssetTests(unittest.TestCase):
    def _asset_text(self, name: str) -> str:
        return files("rp_ylx.deploy").joinpath(name).read_text(encoding="utf-8")

    def test_systemd_units_and_wifi_watchdog_security_invariants(self) -> None:
        recover = self._asset_text("rp-ylx-recover.service")
        self.assertIn("ReadWritePaths=/usr/local/lib/rp-ylx", recover)
        self.assertIn("ReadWritePaths=/usr/local/lib/openaria", recover)
        self.assertIn("ReadWritePaths=-/etc/avahi/services", recover)

        fstrim = self._asset_text("openaria-idle-fstrim.conf")
        self.assertIn("Type=exec", fstrim)

        net_ctl = self._asset_text("rp-ylx-network-control.service")
        self.assertIn("SupplementaryGroups=rp-ylx", net_ctl)
        self.assertIn("ReadWritePaths=-/etc/avahi/services", net_ctl)

        watchdog_text = self._asset_text("rp-ylx-wifi-watchdog")
        self.assertIn("flock -x -n 7", watchdog_text)
        self.assertLess(watchdog_text.index("flock -x -n 7"), watchdog_text.index("modprobe"))

        # Execute rp-ylx-wifi-watchdog with a missing maintenance lock -> exits 0, fails closed!
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            script = root / "watchdog.sh"
            script.write_text(watchdog_text, encoding="utf-8")
            script.chmod(0o755)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            (bin_dir / "logger").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            (bin_dir / "logger").chmod(0o755)
            marker = root / "rp-ylx-invoked"
            fake_cli = root / "rp-ylx"
            fake_cli.write_text(
                f"#!/bin/sh\ntouch {marker}\necho wifi-client\n",
                encoding="utf-8",
            )
            fake_cli.chmod(0o755)
            env = {
                "PATH": f"{bin_dir}:/usr/bin:/bin",
                "STATE_DIRECTORY": str(root / "state"),
                "RUNTIME_DIRECTORY": str(root / "run"),
                "RP_YLX_NETWORK_OPERATION_LOCK_PATH": str(root / "net.lock"),
                "OPENARIA_MAINTENANCE_LOCK": str(root / "missing-maint.lock"),
                "RP_YLX_COMMAND": str(fake_cli),
            }
            proc = subprocess.run(
                [str(script)],
                env=env,
                capture_output=True,
                text=True,
                timeout=5,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertIn("maintenance lock is missing", proc.stdout)
            self.assertFalse(marker.exists())


class NetworkAndUpdateTests(unittest.TestCase):
    def _make_controller(self, root: Path, **kwargs: object) -> network_control.NetworkController:
        boot_file = root / "boot_id"
        machine_file = root / "machine_id"
        boot_file.write_text(str(uuid.uuid4()) + "\n", encoding="utf-8")
        machine_file.write_text("test-machine-id\n", encoding="utf-8")
        env = {
            "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
            "RP_YLX_NM_PROFILE_DIR": str(root / "profiles"),
            "RP_YLX_AVAHI_SERVICE_DIR": str(root / "avahi"),
            "RP_YLX_NETWORK_OPERATION_LOCK_PATH": str(root / "net.lock"),
            "RP_YLX_BOOT_ID_PATH": str(boot_file),
            "RP_YLX_MACHINE_ID_PATH": str(machine_file),
        }
        (root / "profiles").mkdir(exist_ok=True)
        (root / "avahi").mkdir(exist_ok=True)
        with (
            patch.dict(os.environ, env, clear=False),
            patch("rp_ylx.network.subprocess.run", side_effect=lambda *_a, **_kw: None),
            patch.object(network_control, "prepare_first_install_network"),
            patch.object(network_control, "ensure_rescue_ap", return_value="rescue"),
            patch.object(network_control, "rescue_network"),
            patch.object(network_control, "cleanup_orphan_network_candidates"),
        ):
            return network_control.NetworkController(
                device_id="dev-1", start_worker=False, require_root=False, **kwargs
            )

    def test_scan_concurrent_waiters_per_generation_and_deferred_ttl_abort(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            clock_ns = 1_000_000_000
            controller = self._make_controller(
                root,
                defer_execution_until_response=True,
                deferred_release_timeout_seconds=5.0,
                monotonic_ns=lambda: clock_ns,
            )
            env = {
                "RP_YLX_NETWORK_STATE_DIR": str(root / "state"),
                "RP_YLX_NETWORK_OPERATION_LOCK_PATH": str(root / "net.lock"),
            }
            entered = threading.Event()
            release_scan = threading.Event()

            def slow_scan():
                entered.set()
                release_scan.wait(timeout=2.0)
                return [{"ssid": "Studio", "signal": 80, "security": "wpa2-personal"}]

            try:
                with (
                    patch.dict(os.environ, env, clear=False),
                    patch.object(
                        network_control,
                        "network_operation_lock",
                        side_effect=lambda **_kw: nullcontext(),
                    ),
                    patch.object(network_control, "scan_wifi_networks", side_effect=slow_scan),
                    ThreadPoolExecutor(max_workers=4) as pool,
                ):
                    f1 = pool.submit(controller._scan)
                    self.assertTrue(entered.wait(timeout=1.5))
                    f2 = pool.submit(controller._scan)
                    f3 = pool.submit(controller._scan)
                    release_scan.set()
                    results = [
                        f1.result(timeout=2.0),
                        f2.result(timeout=2.0),
                        f3.result(timeout=2.0),
                    ]
                self.assertEqual(len(results), 3)
                for res in results:
                    self.assertEqual(res["networks"][0]["ssid"], "Studio")
                self.assertIsNone(controller._scan_job)

                # Deferred unreleased transaction auto-aborts after TTL on same boot.
                tx_id = "0198d2a0-41a0-7b7a-a751-0e86a39d4db1"
                controller._deferred.add(tx_id)
                aborted: list[tuple[str, str]] = []
                with (
                    patch.object(controller._state, "execution_released", return_value=False),
                    patch.object(
                        controller,
                        "_abort_unreleased_transaction",
                        side_effect=lambda tid, reason: aborted.append((tid, reason)),
                    ),
                ):
                    controller._expire_stale_deferred(now_ns=1_000_000_000)
                    self.assertEqual(aborted, [])
                    controller._expire_stale_deferred(now_ns=7_000_000_000)
                    self.assertEqual(aborted, [(tx_id, "deferred_release_timeout")])
            finally:
                controller.close()

    def test_network_paths_open_eacces_not_mapped_to_capture_active(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lock_path = Path(tmp) / "net.lock"
            with (
                patch.object(
                    network_paths.os,
                    "open",
                    side_effect=PermissionError(errno.EACCES, "permission denied"),
                ),
                self.assertRaises(network_paths.NetworkError) as err,
                network_paths._file_operation_lock(lock_path, blocking=False),
            ):
                pass
            self.assertEqual(err.exception.code, "operation_lock_failed")
            self.assertNotEqual(err.exception.code, "capture_active")

    def test_update_broken_signing_key_symlink_and_exfat_storage_required_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            broken = root / "release-signing.pub"
            broken.symlink_to(root / "nonexistent.pub")
            with (
                patch.dict(os.environ, {}, clear=False),
                patch.object(update_mod, "RELEASE_SIGNING_PUBLIC_KEY", broken),
                self.assertRaises(ValueError),
            ):
                os.environ.pop(update_mod.OPENARIA_UPDATE_PUBLIC_KEY_ENV, None)
                update_mod._configured_public_key_pem()

            bad_dv = root / "rp-ylx-data-volume"
            bad_dv.write_text("CORRUPT ENTRY\n", encoding="utf-8")
            with (
                patch.dict(os.environ, {}, clear=False),
                patch.object(update_mod, "DATA_VOLUME_CONFIG", bad_dv),
                patch.object(update_mod, "FACTORY_STORAGE_STATE", root / "missing.json"),
                self.assertRaises(ValueError),
            ):
                os.environ.pop("RP_YLX_DATA_VOLUME_MODE", None)
                update_mod.exfat_storage_required()


class CardExportAndDisksTests(unittest.TestCase):
    def test_exfat_fs_skips_0xe0_to_0xff_and_zero_fills_beyond_valid_data_length(self) -> None:
        scripts_dir = Path(__file__).resolve().parents[1] / "scripts"
        with patch.object(sys, "path", [str(scripts_dir), *sys.path]):
            _host_disks = importlib.import_module("_host_disks")
            export_mod = importlib.import_module("card_export.export")

        img = _build_synthetic_exfat_image(file_data=b"HELLO!", valid_data_length=6)
        fs = export_mod._ExfatFS(img)
        node = fs.get("/a.tx")
        with node.open() as stream:
            content = stream.read()
        self.assertEqual(len(content), 6 + 128)
        self.assertEqual(content[:6], b"HELLO!")
        self.assertEqual(content[6:], b"\x00" * 128)

        # _host_disks.windows_disks rejects Size=0 even when filter_attributes=True
        disks = _host_disks.windows_disks(
            [
                {
                    "Number": 1,
                    "Size": 0,
                    "BusType": "USB",
                    "IsBoot": False,
                    "IsSystem": False,
                    "IsReadOnly": False,
                }
            ],
            filter_attributes=True,
        )
        self.assertEqual(disks, [])


if __name__ == "__main__":
    unittest.main()
