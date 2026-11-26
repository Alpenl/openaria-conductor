"""Unit tests for first-boot exFAT MIRO-EGO setup, bundle install, and p1 CONFIG cleanup."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from miro_ego_host import disks
from miro_ego_host.factory_recovery import (
    device,
    firstboot,
    partition_layout,
    provision,
    storage_firstboot,
)
from miro_ego_host.tests.fixtures import factory_profile, make_native_bundle, vendor_mbr


class FirstbootAndCleanupTests(unittest.TestCase):
    def test_storage_firstboot_formats_miro_ego_once_and_writes_data_volume_env(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            _, layout = partition_layout.plan(vendor_mbr(), 64 * 1024**3)
            package = root / partition_layout.EXFATPROGS_PACKAGE
            package.write_bytes(b"exfatprogs-fixture")
            pkg_sha = hashlib.sha256(package.read_bytes()).hexdigest()
            layout["exfatprogs_sha256"] = pkg_sha

            fail_format = [True]
            calls: list[tuple[str, ...]] = []

            def fake_run(*args: str, **_kwargs: object) -> str:
                calls.append(args)
                if args[0] == "mkfs.exfat" and fail_format[0]:
                    raise subprocess.CalledProcessError(1, args)
                return "ABCD-1234" if args[0] == "blkid" else ""

            config_path = root / "etc/default/miro-ego-data-volume"
            with (
                patch.object(partition_layout, "EXFATPROGS_SHA256", pkg_sha),
                patch.object(storage_firstboot, "CONFIG", config_path),
                patch.object(
                    storage_firstboot,
                    "target",
                    return_value=(Path("/dev/mmcblk0p2"), Path("/dev/mmcblk0p3")),
                ),
                patch.object(storage_firstboot, "run", side_effect=fake_run),
            ):
                # Interrupted format persists formatting journal without writing config.
                with self.assertRaises(subprocess.CalledProcessError):
                    storage_firstboot.prepare(layout, root, root, firstboot.atomic_json)
                self.assertEqual(
                    json.loads((root / "storage.json").read_text())["phase"], "formatting"
                )
                self.assertFalse(config_path.exists())

                # Retry succeeds and formats with label MIRO-EGO.
                fail_format[0] = False
                data_dev, uuid_str = storage_firstboot.prepare(
                    layout, root, root, firstboot.atomic_json
                )
                self.assertEqual(data_dev, Path("/dev/mmcblk0p3"))
                self.assertEqual(uuid_str, "ABCD-1234")
                self.assertIn(("mkfs.exfat", "-L", "MIRO-EGO", "/dev/mmcblk0p3"), calls)
                self.assertEqual(json.loads((root / "storage.json").read_text())["phase"], "ready")
                cfg_text = config_path.read_text(encoding="utf-8")
                self.assertIn("MIRO_EGO_DATA_VOLUME_MODE=exfat\n", cfg_text)
                self.assertIn("MIRO_EGO_DATA_VOLUME_PARTUUID=db2e7205-03\n", cfg_text)
                self.assertIn("MIRO_EGO_DATA_VOLUME_UUID=ABCD-1234\n", cfg_text)

                # Subsequent retry NEVER reformats ready volume.
                calls.clear()
                config_path.unlink()
                storage_firstboot.prepare(layout, root, root, firstboot.atomic_json)
                self.assertFalse(any(c[0] in ("mkfs.exfat", "resize2fs") for c in calls))
                self.assertTrue(config_path.exists())

                # Changed UUID on already-ready card fails closed.
                with (
                    patch.object(storage_firstboot, "run", return_value="BAD0-BAD0"),
                    self.assertRaises(RuntimeError),
                ):
                    storage_firstboot.prepare(layout, root, root, firstboot.atomic_json)

    def test_ensure_data_layout_creates_canonical_tree_and_invokes_attr_hidden(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            mount = Path(folder) / "data"
            mount.mkdir()
            hidden_calls: list[Path] = []

            def fake_hide(path: Path, *, miro_ego_bin: Path | None = None) -> bool:
                hidden_calls.append(path)
                return True

            with patch.object(storage_firstboot, "set_exfat_hidden", side_effect=fake_hide):
                applied = storage_firstboot.ensure_data_layout(
                    mount, miro_ego_bin=Path(folder) / "nonexistent-miro-ego"
                )
            self.assertTrue(applied)
            for subdir in (
                "recordings",
                "logs",
                "logs/wifi-watchdog",
                "updates",
                ".miro-ego/index",
                ".miro-ego/staging",
                ".miro-ego/quarantine",
                ".miro-ego/audit",
                ".miro-ego/receipts",
                ".miro-ego/telemetry",
            ):
                self.assertTrue((mount / subdir).is_dir(), f"missing {subdir}")
            self.assertEqual(hidden_calls, [mount / ".miro-ego"])

    @unittest.skipUnless(os.name == "posix", "POSIX umask & chmod behavior")
    def test_install_bundle_invokes_native_miro_ego_deploy_install_with_022_umask(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            bundle = Path(folder) / "bundle"
            (bundle / "bin").mkdir(parents=True)
            fake_bin = bundle / "bin" / "miro-ego"
            fake_bin.write_text(
                "#!/bin/sh\n"
                "set -eu\n"
                'printf "%s\\n" "$@" > "$3/invoked-args.txt"\n'
                'mkdir "$3/installed-dir"\n'
                'printf "ok" > "$3/installed-dir/asset.txt"\n',
                encoding="utf-8",
            )
            fake_bin.chmod(0o600)  # install_bundle must chmod +x before exec

            prev_umask = os.umask(0o077)
            try:
                firstboot.install_bundle(bundle)
                (bundle / "private-secret").write_text("secret", encoding="utf-8")
            finally:
                os.umask(prev_umask)

            self.assertEqual(
                (bundle / "invoked-args.txt").read_text(encoding="utf-8").splitlines(),
                ["deploy", "install", str(bundle)],
            )
            self.assertEqual(stat.S_IMODE((bundle / "installed-dir").stat().st_mode), 0o755)
            self.assertEqual(
                stat.S_IMODE((bundle / "installed-dir/asset.txt").stat().st_mode), 0o644
            )
            self.assertEqual(stat.S_IMODE((bundle / "private-secret").stat().st_mode), 0o600)

    def test_firstboot_end_to_end_resumes_and_cleans_up_p1_config_payload(self) -> None:
        """Verify p1 CONFIG deletes transient payloads leaving status.json + DO_NOT_DELETE.txt."""
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            state_root = base / "state"
            config_p1 = base / "boot_config"
            boot = config_p1 / "miro-ego"
            hook_path = config_p1 / "hobot_config.sh"
            unit_path = base / "miro-ego-firstboot.service"
            voice_path = base / "voice.json"
            state_root.mkdir()
            boot.mkdir(parents=True)

            # Seed full p1 CONFIG payload just like provision.compose() writes it.
            hook_path.write_text(provision.HOOK, encoding="utf-8")
            unit_path.write_text(firstboot.UNIT, encoding="utf-8")
            for script in (
                "disks.py",
                "firstboot.py",
                "device.py",
                "provision.py",
                "partition_layout.py",
                "storage_firstboot.py",
            ):
                (boot / script).write_text("# provisioned script\n", encoding="utf-8")
            (boot / "__pycache__").mkdir()
            (boot / "__pycache__" / "firstboot.cpython-312.pyc").write_bytes(b"pyc")

            exfat_deb = boot / partition_layout.EXFATPROGS_PACKAGE
            exfat_deb.write_bytes(b"exfatprogs-deb")
            logrotate_deb = boot / partition_layout.LOGROTATE_PACKAGE
            logrotate_deb.write_bytes(b"logrotate-deb")
            exfat_sha = hashlib.sha256(exfat_deb.read_bytes()).hexdigest()
            logrotate_sha = hashlib.sha256(logrotate_deb.read_bytes()).hexdigest()

            _, layout = partition_layout.plan(vendor_mbr(), 64 * 1024**3)
            layout["exfatprogs_sha256"] = exfat_sha
            layout["logrotate_sha256"] = logrotate_sha

            profile = factory_profile(board="a" * 64, voice_volume=63, voice_enabled=True)
            sealed = provision.seal_profile(profile)
            (boot / "profile.bin").write_bytes(sealed)

            archive = make_native_bundle(boot / "bundle.tar")
            manifest = {
                "schema": provision.AUTOINSTALL_SCHEMA,
                "profile_sha256": hashlib.sha256(sealed).hexdigest(),
                "bundle_sha256": disks.sha256_file(archive),
                "storage": layout,
            }
            (boot / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            calls: list[tuple[str, ...]] = []
            chowned_voice: list[tuple[int, int]] = []
            fail_install_once = [True]

            def fake_run(*args: str, **_kwargs: object) -> None:
                calls.append(args)
                if (
                    len(args) >= 3
                    and args[1] == "deploy"
                    and args[2] == "install"
                    and fail_install_once[0]
                ):
                    raise RuntimeError("simulated deploy install failure")

            def fake_fchown(_fd: int, uid: int, gid: int) -> None:
                chowned_voice.append((uid, gid))

            with (
                patch.object(firstboot, "ROOT", state_root),
                patch.object(firstboot, "BOOT", boot),
                patch.object(firstboot, "HOBOT_HOOK", hook_path),
                patch.object(firstboot, "FIRSTBOOT_UNIT", unit_path),
                patch.object(firstboot, "VOICE_STATE", voice_path),
                patch.object(firstboot, "_resolve_service_owner", return_value=(997, 997)),
                patch.object(os, "fchown", side_effect=fake_fchown),
                patch.object(partition_layout, "EXFATPROGS_SHA256", exfat_sha),
                patch.object(partition_layout, "LOGROTATE_SHA256", logrotate_sha),
                patch.object(device, "hardware", return_value="a" * 64),
                patch.object(device, "require_fresh") as fresh_mock,
                patch.object(device, "write_files") as write_mock,
                patch.object(
                    storage_firstboot,
                    "prepare",
                    return_value=(Path("/dev/mmcblk0p3"), "ABCD-1234"),
                ) as prep_mock,
                patch.object(
                    storage_firstboot, "mount_and_initialize", return_value=True
                ) as mount_mock,
                patch.object(firstboot.shutil, "which", return_value=None),
                patch.object(firstboot, "run", side_effect=fake_run),
                patch.object(os, "sync") as sync_mock,
            ):
                # First run fails during deploy install; payload must remain intact for retry.
                with self.assertRaisesRegex(RuntimeError, "simulated deploy"):
                    firstboot.execute()
                self.assertEqual(
                    json.loads((state_root / "state.json").read_bytes())["phase"], "configured"
                )
                self.assertTrue((boot / "bundle.tar").exists())
                self.assertTrue((boot / "profile.bin").exists())
                self.assertTrue(exfat_deb.exists())

                # Retry completes installation and cleans up p1 CONFIG!
                fail_install_once[0] = False
                firstboot.execute()

                self.assertEqual(
                    json.loads((state_root / "state.json").read_bytes())["phase"], "complete"
                )
                self.assertEqual(
                    json.loads(voice_path.read_bytes()),
                    {
                        "schema": "miro-ego.voice-settings.v1",
                        "enabled": True,
                        "volume": 63,
                    },
                )
                self.assertEqual(chowned_voice, [(997, 997)])

                # Verify ONLY status.json and DO_NOT_DELETE.txt remain on p1 CONFIG/miro-ego/.
                remaining = {p.name for p in boot.iterdir()}
                self.assertEqual(remaining, {"status.json", "DO_NOT_DELETE.txt"})
                status = json.loads((boot / "status.json").read_bytes())
                self.assertEqual(status["phase"], "complete")
                self.assertEqual(status["schema"], "miro-ego.autoinstall-status.v1")
                self.assertIn("MIRO-EGO", (boot / "DO_NOT_DELETE.txt").read_text(encoding="utf-8"))

                # Verify transient payload files were genuinely removed.
                self.assertFalse((boot / "bundle.tar").exists())
                self.assertFalse((boot / "profile.bin").exists())
                self.assertFalse((boot / "manifest.json").exists())
                self.assertFalse(exfat_deb.exists())
                self.assertFalse(logrotate_deb.exists())
                self.assertFalse((boot / "firstboot.py").exists())
                self.assertFalse((boot / "__pycache__").exists())
                self.assertFalse((state_root / "bundle").exists())

                # Verify hobot_config.sh reset & transient systemd unit removed.
                self.assertEqual(hook_path.read_text(encoding="utf-8"), provision.CLEAN_HOOK)
                self.assertFalse(unit_path.exists())

                # Verify key calls occurred properly.
                fresh_mock.assert_called_once()
                write_mock.assert_called_once()
                self.assertEqual(prep_mock.call_count, 2)
                mount_mock.assert_called_once_with(Path("/dev/mmcblk0p3"))
                self.assertTrue(
                    any(len(c) >= 3 and c[1] == "deploy" and c[2] == "install" for c in calls)
                )
                self.assertIn(("dpkg", "-i", str(logrotate_deb)), calls)
                self.assertEqual(calls[-1], ("systemctl", "reboot", "--no-block"))
                sync_mock.assert_called_once()

                # Re-running after complete is an idempotent no-op.
                call_count = len(calls)
                firstboot.execute()
                self.assertEqual(len(calls), call_count)

    def test_wrong_board_aborts_before_touching_destination(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            _, layout = partition_layout.plan(vendor_mbr(), 64 * 1024**3)
            sealed = provision.seal_profile(factory_profile(board="a" * 64))
            (root / "profile.bin").write_bytes(sealed)
            (root / "manifest.json").write_text(
                json.dumps(
                    {
                        "schema": provision.AUTOINSTALL_SCHEMA,
                        "profile_sha256": hashlib.sha256(sealed).hexdigest(),
                        "bundle_sha256": "0" * 64,
                        "storage": layout,
                    }
                ),
                encoding="utf-8",
            )
            with (
                patch.object(firstboot, "ROOT", root / "state"),
                patch.object(firstboot, "BOOT", root),
                patch.object(device, "hardware", return_value="b" * 64),
                patch.object(device, "require_fresh") as fresh_mock,
                patch.object(device, "write_files") as write_mock,
            ):
                from cryptography.exceptions import InvalidTag

                with self.assertRaises(InvalidTag):
                    firstboot.execute()
                fresh_mock.assert_not_called()
                write_mock.assert_not_called()

    def test_extract_bundle_rejects_traversal_and_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for name, kind in (("../escape", tarfile.REGTYPE), ("link", tarfile.SYMTYPE)):
                archive = root / "bad.tar"
                with tarfile.open(archive, "w") as tar:
                    member = tarfile.TarInfo(name)
                    member.type = kind
                    member.linkname = "/etc/shadow" if kind == tarfile.SYMTYPE else ""
                    tar.addfile(member)
                with self.assertRaises(RuntimeError):
                    firstboot.extract_bundle(archive, root / "out")
                self.assertFalse((root / "out").exists())

    def test_flat_boot_partition_scripts_import_standalone(self) -> None:
        """Simulate executing flat scripts copied onto FAT32 /boot/config/miro-ego/."""
        import shutil
        import sys

        pkg_root = Path(__file__).resolve().parents[1]
        recovery_dir = pkg_root / "factory_recovery"
        with tempfile.TemporaryDirectory() as folder:
            flat = Path(folder)
            shutil.copy2(pkg_root / "disks.py", flat / "disks.py")
            for name in (
                "device.py",
                "partition_layout.py",
                "provision.py",
                "storage_firstboot.py",
                "firstboot.py",
            ):
                shutil.copy2(recovery_dir / name, flat / name)
            proc = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-c",
                    (
                        "import firstboot, provision, storage_firstboot, partition_layout, "
                        "device, disks; "
                        "assert partition_layout.DATA_VOLUME_LABEL == 'MIRO-EGO'; "
                        "assert provision.MAGIC == b'MEB1'; "
                        "assert firstboot.KEEP_BOOT_FILES == frozenset("
                        "{'status.json', 'DO_NOT_DELETE.txt'})"
                    ),
                ],
                cwd=flat,
                env={**os.environ, "PYTHONPATH": str(flat)},
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc.returncode, 0)
            self.assertFalse((flat / "__pycache__").exists())


if __name__ == "__main__":
    unittest.main()
