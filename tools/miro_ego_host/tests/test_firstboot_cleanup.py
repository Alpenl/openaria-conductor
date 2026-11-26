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
                patch.object(os, "fchown", side_effect=fake_fchown, create=True),
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
                patch.object(os, "sync", create=True) as sync_mock,
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
                self.assertIn(
                    ("dpkg", "-i", "--force-confdef", "--force-confold", str(logrotate_deb)),
                    calls,
                )
                self.assertEqual(calls[-1], ("systemctl", "reboot", "--no-block"))
                sync_mock.assert_called_once()

                # Re-running after complete is an idempotent no-op.
                call_count = len(calls)
                firstboot.execute()
                self.assertEqual(len(calls), call_count)

    def test_firstboot_cleanup_failure_does_not_mark_complete_and_resumes_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            state_root = base / "state"
            config_p1 = base / "boot_config"
            boot = config_p1 / "miro-ego"
            hook_path = config_p1 / "hobot_config.sh"
            unit_path = base / "miro-ego-firstboot.service"
            voice_path = base / "voice.json"
            login_helper = base / "miro-ego-device-login"
            state_root.mkdir()
            boot.mkdir(parents=True)

            hook_path.write_text(provision.HOOK, encoding="utf-8")
            unit_path.write_text(firstboot.UNIT, encoding="utf-8")
            login_helper.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            (boot / "firstboot.py").write_text("# script\n", encoding="utf-8")

            exfat_deb = boot / partition_layout.EXFATPROGS_PACKAGE
            exfat_deb.write_bytes(b"exfatprogs-deb")
            logrotate_deb = boot / partition_layout.LOGROTATE_PACKAGE
            logrotate_deb.write_bytes(b"logrotate-deb")
            exfat_sha = hashlib.sha256(exfat_deb.read_bytes()).hexdigest()
            logrotate_sha = hashlib.sha256(logrotate_deb.read_bytes()).hexdigest()

            _, layout = partition_layout.plan(vendor_mbr(), 64 * 1024**3)
            layout["exfatprogs_sha256"] = exfat_sha
            layout["logrotate_sha256"] = logrotate_sha
            profile = factory_profile(board="a" * 64, voice_volume=50, voice_enabled=True)
            sealed = provision.seal_profile(profile)
            (boot / "profile.bin").write_bytes(sealed)
            archive = make_native_bundle(boot / "bundle.tar")
            (boot / "manifest.json").write_text(
                json.dumps(
                    {
                        "schema": provision.AUTOINSTALL_SCHEMA,
                        "profile_sha256": hashlib.sha256(sealed).hexdigest(),
                        "bundle_sha256": disks.sha256_file(archive),
                        "storage": layout,
                    }
                ),
                encoding="utf-8",
            )

            real_cleanup = firstboot.cleanup_boot_partition
            fail_cleanup = [True]
            calls: list[tuple[str, ...]] = []

            def flaky_cleanup(boot_dir: Path, **kw: object) -> list[str]:
                if fail_cleanup[0]:
                    # Simulate partial unlink (manifest.json + bundle.tar gone) before I/O failure.
                    (boot_dir / "manifest.json").unlink(missing_ok=True)
                    (boot_dir / "bundle.tar").unlink(missing_ok=True)
                    raise OSError("simulated FAT32 cleanup interruption")
                return real_cleanup(boot_dir, **kw)  # type: ignore[arg-type]

            with (
                patch.object(firstboot, "ROOT", state_root),
                patch.object(firstboot, "BOOT", boot),
                patch.object(firstboot, "HOBOT_HOOK", hook_path),
                patch.object(firstboot, "FIRSTBOOT_UNIT", unit_path),
                patch.object(firstboot, "VOICE_STATE", voice_path),
                patch.object(firstboot, "DEVICE_LOGIN_HELPER", login_helper),
                patch.object(firstboot, "_resolve_service_owner", return_value=None),
                patch.object(partition_layout, "EXFATPROGS_SHA256", exfat_sha),
                patch.object(partition_layout, "LOGROTATE_SHA256", logrotate_sha),
                patch.object(device, "hardware", return_value="a" * 64),
                patch.object(device, "require_fresh"),
                patch.object(device, "write_files"),
                patch.object(device, "apply_service_permissions") as perms_mock,
                patch.object(
                    storage_firstboot,
                    "prepare",
                    return_value=(Path("/dev/mmcblk0p3"), "ABCD-1234"),
                ),
                patch.object(storage_firstboot, "mount_and_initialize", return_value=True),
                patch.object(firstboot.shutil, "which", return_value="/usr/sbin/logrotate"),
                patch.object(firstboot, "cleanup_boot_partition", side_effect=flaky_cleanup),
                patch.object(firstboot, "run", side_effect=lambda *a, **_k: calls.append(a)),
                patch.object(os, "sync", create=True),
            ):
                with self.assertRaisesRegex(OSError, "simulated FAT32 cleanup"):
                    firstboot.execute()

                # Must NOT be marked complete yet so next boot retries cleanup!
                self.assertFalse(firstboot.complete())
                self.assertEqual(
                    json.loads((state_root / "state.json").read_bytes())["phase"], "cleaning"
                )
                self.assertIn((str(login_helper),), calls)
                perms_mock.assert_called_once()

                # Retry succeeds even though manifest.json and bundle.tar were already unlinked!
                fail_cleanup[0] = False
                firstboot.execute()
                self.assertTrue(firstboot.complete())
                self.assertEqual(
                    {p.name for p in boot.iterdir()},
                    {"status.json", "DO_NOT_DELETE.txt"},
                )
                self.assertEqual(hook_path.read_text(encoding="utf-8"), provision.CLEAN_HOOK)

    def test_ensure_ssh_data_volume_preserves_exfat_and_provisions_missing_p3(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            cfg_path = root / "etc/default/miro-ego-data-volume"
            disk_img = root / "mmcblk0"
            disk_img.write_bytes(vendor_mbr())

            # Case 1: Existing p3 with exFAT is preserved without mkfs.exfat.
            calls: list[tuple[str, ...]] = []

            def fake_run_existing(*args: str, **_kwargs: object) -> str:
                calls.append(args)
                if args[:2] == ("findmnt", "-n"):
                    return "/dev/mmcblk0p2"
                if args[0] == "blkid" and "PARTUUID" in args:
                    return "db2e7205-03"
                if args[0] == "blkid" and "TYPE" in args:
                    return "exfat"
                if args[0] == "blkid" and "UUID" in args:
                    return "CAFE-BEEF"
                return ""

            blk_stat = os.stat_result((stat.S_IFBLK | 0o660, 0, 0, 1, 0, 0, 0, 0, 0, 0))
            with (
                patch.object(storage_firstboot, "run", side_effect=fake_run_existing),
                patch.object(Path, "resolve", lambda self: self),
                patch.object(Path, "exists", lambda self: True),
                patch.object(Path, "stat", lambda self: blk_stat),
            ):
                info = storage_firstboot.ensure_ssh_data_volume(config_path=cfg_path)
            self.assertEqual(info["uuid"], "CAFE-BEEF")
            self.assertFalse(any(c[0] == "mkfs.exfat" for c in calls))
            self.assertIn("MIRO_EGO_DATA_VOLUME_UUID=CAFE-BEEF\n", cfg_path.read_text("utf-8"))

            # Case 2: Missing p3 on a 64 GiB card creates MBR slot 3, formats exFAT, writes config.
            calls.clear()
            p3_created = [False]

            def fake_run_create(*args: str, **_kwargs: object) -> str:
                calls.append(args)
                if args[:2] == ("findmnt", "-n"):
                    return str(disk_img) + "p2"
                if args[:2] == ("blockdev", "--getsize64"):
                    return str(64 * 1024**3)
                if args[0] in ("partx", "partprobe"):
                    p3_created[0] = True
                    return ""
                if args[0] == "blkid" and "PARTUUID" in args:
                    return "db2e7205-03"
                if args[0] == "blkid" and "TYPE" in args:
                    return ""
                if args[0] == "blkid" and "-p" in args:
                    return "1234-5678"
                return ""

            orig_exists = Path.exists
            orig_stat = Path.stat

            def fake_exists(self: Path) -> bool:
                if str(self) == str(disk_img) + "p3":
                    return p3_created[0]
                return orig_exists(self)

            def fake_stat(self: Path, *a: object, **kw: object):
                if str(self) in (str(disk_img), str(disk_img) + "p2", str(disk_img) + "p3"):
                    return blk_stat
                return orig_stat(self, *a, **kw)  # type: ignore[arg-type]

            with (
                patch.object(storage_firstboot, "run", side_effect=fake_run_create),
                patch.object(storage_firstboot.shutil, "which", return_value="/usr/bin/partx"),
                patch.object(
                    storage_firstboot.re,
                    "fullmatch",
                    side_effect=lambda pat, val: (
                        True
                        if pat == r"/dev/mmcblk[0-9]+p2" and val == disk_img.as_posix() + "p2"
                        else __import__("re").compile(pat).fullmatch(val)
                    ),
                ),
                patch.object(Path, "resolve", lambda self: self),
                patch.object(Path, "exists", fake_exists),
                patch.object(Path, "stat", fake_stat),
            ):
                info2 = storage_firstboot.ensure_ssh_data_volume(config_path=cfg_path)
            self.assertEqual(info2["uuid"], "1234-5678")
            self.assertTrue(any(c[:3] == ("mkfs.exfat", "-L", "MIRO-EGO") for c in calls))
            raw_mbr = disk_img.read_bytes()[:512]
            entries = partition_layout.partitions(raw_mbr)
            self.assertEqual(entries[2][0], 0x07)
            self.assertEqual(raw_mbr[479:482], b"\xfe\xff\xff")
            self.assertEqual(raw_mbr[483:486], b"\xfe\xff\xff")

            # Case 3: ensure_offline_packages installs missing exfatprogs & logrotate.
            tools_dir = root / "offline_debs"
            tools_dir.mkdir()
            exfat_pkg = tools_dir / partition_layout.EXFATPROGS_PACKAGE
            log_pkg = tools_dir / partition_layout.LOGROTATE_PACKAGE
            exfat_pkg.write_bytes(b"exfat-deb")
            log_pkg.write_bytes(b"logrotate-deb")
            exfat_sha = hashlib.sha256(exfat_pkg.read_bytes()).hexdigest()
            log_sha = hashlib.sha256(log_pkg.read_bytes()).hexdigest()
            dpkg_calls: list[tuple[tuple[str, ...], dict[str, str] | None]] = []

            def record_dpkg(*args: str, env: dict[str, str] | None = None, **_kw: object) -> str:
                dpkg_calls.append((args, env))
                return ""

            with (
                patch.object(partition_layout, "EXFATPROGS_SHA256", exfat_sha),
                patch.object(partition_layout, "LOGROTATE_SHA256", log_sha),
                patch.object(storage_firstboot.shutil, "which", return_value=None),
                patch.object(storage_firstboot, "run", side_effect=record_dpkg),
            ):
                storage_firstboot.ensure_offline_packages(tools_dir)
            self.assertEqual(len(dpkg_calls), 2)
            for cmd_args, cmd_env in dpkg_calls:
                self.assertEqual(cmd_args[:4], ("dpkg", "-i", "--force-confdef", "--force-confold"))
                self.assertEqual(cmd_env, {"DEBIAN_FRONTEND": "noninteractive"})

            # Case 4: fallback mount_and_initialize passes uid/gid/fmask/dmask options.
            mount_calls: list[tuple[str, ...]] = []
            mount_dir = root / "fallback_data"
            with (
                patch.object(storage_firstboot, "_is_mounted", return_value=False),
                patch.object(
                    storage_firstboot,
                    "run",
                    side_effect=lambda *a, **_k: mount_calls.append(a) or "",
                ),
                patch.object(storage_firstboot, "ensure_data_layout", return_value=True),
            ):
                storage_firstboot.mount_and_initialize(
                    Path("/dev/mmcblk0p3"),
                    mountpoint=mount_dir,
                    helper=root / "missing-helper",
                    service_ids=(997, 998),
                )
            self.assertEqual(len(mount_calls), 1)
            self.assertIn("uid=997,gid=998,fmask=0137,dmask=0027", mount_calls[0][4])

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
