from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import struct
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.factory_recovery import device, disks, firstboot, partition_layout, provision, recover
from tests.test_factory_recovery import profile


def bound_profile():
    value = profile()
    value["board"] = "a" * 64
    value["voice"] = {"volume": 37}
    return value


def make_bundle(path):
    with tarfile.open(path, "w") as tar:
        for name, raw in {"rdk_x5_install.py": b"installer", "SHA256SUMS": b"test"}.items():
            member = tarfile.TarInfo(name)
            member.size = len(raw)
            tar.addfile(member, io.BytesIO(raw))


class AutoinstallTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "factory first boot runs on Linux")
    def test_installer_permissions_do_not_inherit_private_recovery_umask(self):
        import stat

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "rdk_x5_install.py").write_text(
                "from pathlib import Path; import sys\n"
                "root=Path(sys.argv[-1]); (root/'application').mkdir(); "
                "(root/'application/module.py').write_text('application')\n"
            )
            previous = os.umask(0o077)
            try:
                firstboot.install_bundle(root)
                (root / "private-profile").write_text("secret fixture")
            finally:
                os.umask(previous)
            self.assertEqual(stat.S_IMODE((root / "application").stat().st_mode), 0o755)
            self.assertEqual(stat.S_IMODE((root / "application/module.py").stat().st_mode), 0o644)
            self.assertEqual(stat.S_IMODE((root / "private-profile").stat().st_mode), 0o600)

    def test_sparse_copy_preserves_leading_and_trailing_zeros_and_exact_size(self):
        with tempfile.TemporaryDirectory() as folder:
            base, output = Path(folder) / "base.img", Path(folder) / "output.img"
            raw = bytes(4 * 1024**2) + b"payload" + bytes(8 * 1024**2)
            base.write_bytes(raw)
            with base.open("rb") as src, output.open("wb") as dst:
                provision.copy_image(src, dst)
            self.assertEqual(output.read_bytes(), raw)
            self.assertEqual(output.stat().st_size, len(raw))
            if os.name == "posix":
                self.assertLess(provision.copy_budget(base), len(raw))

    def test_configuration_backup_allows_failed_history_but_not_active_capture(self):
        historical = {"old-session": {"recording_state": {"state": "failed"}}}
        device.require_idle(
            {"active_session": None, "pending_safe_swap": None, "retained": historical}
        )
        for value in (
            {"active_session": {"session_id": "active"}},
            {"pending_safe_swap": {"state": "pending"}},
            None,
        ):
            with self.assertRaises(RuntimeError):
                device.require_idle(value)

    def test_board_encryption_and_tamper(self):
        from cryptography.exceptions import InvalidTag

        value = bound_profile()
        sealed = provision.seal_profile(value)
        self.assertNotIn(value["board"].encode(), sealed)
        self.assertNotIn(b"machine-id", sealed)
        self.assertEqual(provision.open_profile(sealed, value["board"]), value)
        for raw, board in ((sealed, "b" * 64), (sealed[:-1] + b"!", value["board"])):
            with self.assertRaises(InvalidTag):
                provision.open_profile(raw, board)

    def test_voice_profile_validation_and_legacy_default(self):
        device.validate(profile())
        for volume in (-1, 101, True, "37"):
            value = bound_profile()
            value["voice"]["volume"] = volume
            with self.assertRaises(RuntimeError):
                device.validate(value)

    def test_cli_fast_default_and_full_erase_opt_in(self):
        args = [
            "auto-flash",
            "--image",
            "base.img",
            "--sha256",
            "a",
            "--bundle",
            "b",
            "--bundle-sha256",
            "b",
            "--profile",
            "p",
            "--output",
            "out.img",
            "--disk",
            "/dev/sdb",
        ]
        self.assertFalse(recover.parser().parse_args(args).full_erase)
        self.assertTrue(recover.parser().parse_args(args + ["--full-erase"]).full_erase)

    def test_bundle_traversal_and_symlink_rejected_before_extract(self):
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
                    firstboot.extract_bundle(archive, root / "output")
                self.assertFalse((root / "output").exists())

    def test_install_failure_resumes_without_rewriting_configuration(self):
        with tempfile.TemporaryDirectory() as folder:
            root, boot = Path(folder) / "state", Path(folder) / "boot"
            root.mkdir()
            boot.mkdir()
            sealed = provision.seal_profile(bound_profile())
            (boot / "profile.bin").write_bytes(sealed)
            make_bundle(boot / "bundle.tar")
            manifest = {
                "schema": "openaria.autoinstall.v1",
                "profile_sha256": hashlib.sha256(sealed).hexdigest(),
                "bundle_sha256": disks.digest(boot / "bundle.tar"),
            }
            (boot / "manifest.json").write_text(json.dumps(manifest))
            calls = []
            failing = [True]

            def runner(*args, **kwargs):
                calls.append(args)
                if args[0] == "/usr/bin/python3" and failing[0]:
                    raise RuntimeError("simulated installer failure")

            with (
                patch.object(firstboot, "ROOT", root),
                patch.object(firstboot, "BOOT", boot),
                patch.object(device, "hardware", return_value="a" * 64),
                patch.object(device, "require_fresh") as fresh,
                patch.object(device, "write_files") as write,
                patch.object(firstboot, "restore_volume") as volume,
                patch.object(firstboot, "run", side_effect=runner),
                patch.object(os, "sync"),
            ):
                with self.assertRaises(RuntimeError):
                    firstboot.execute()
                self.assertEqual(
                    json.loads((root / "state.json").read_bytes())["phase"], "configured"
                )
                self.assertTrue((boot / "profile.bin").exists())
                failing[0] = False
                firstboot.execute()
                self.assertEqual(
                    json.loads((root / "state.json").read_bytes())["phase"], "complete"
                )
                self.assertFalse((boot / "profile.bin").exists())
                count = len(calls)
                firstboot.execute()
                self.assertEqual(len(calls), count)
                fresh.assert_called_once()
                write.assert_called_once()
                volume.assert_called_once_with(bound_profile())
                self.assertEqual(calls[-1], ("systemctl", "reboot", "--no-block"))

    def test_wrong_board_never_checks_or_writes_destination(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            sealed = provision.seal_profile(bound_profile())
            (root / "profile.bin").write_bytes(sealed)
            (root / "manifest.json").write_text(
                json.dumps(
                    {
                        "schema": "openaria.autoinstall.v1",
                        "profile_sha256": hashlib.sha256(sealed).hexdigest(),
                    }
                )
            )
            with (
                patch.object(firstboot, "ROOT", root),
                patch.object(firstboot, "BOOT", root),
                patch.object(device, "hardware", return_value="b" * 64),
                patch.object(device, "require_fresh") as fresh,
                patch.object(device, "write_files") as write,
            ):
                from cryptography.exceptions import InvalidTag

                with self.assertRaises(InvalidTag):
                    firstboot.execute()
                fresh.assert_not_called()
                write.assert_not_called()

    def test_real_fat_composition_preserves_source_and_embeds_offline_payload(self):
        from pyfatfs.PyFat import PyFat
        from pyfatfs.PyFatFS import PyFatFS

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            partition, base = root / "fat", root / "base.img"
            partition.touch()
            fat = PyFat()
            fat.mkfs(str(partition), PyFat.FAT_TYPE_FAT32, size=128 * 1024**2, label="CONFIG")
            fat.close()
            with PyFatFS(str(partition)) as fs:
                fs.writetext("/hobot_config.sh", "#!/bin/bash -e\n# vendor\nexit 0\n")
            mbr = bytearray(512)
            mbr[510:] = b"\x55\xaa"
            mbr[450] = 0xC
            struct.pack_into("<II", mbr, 454, 2048, 262144)
            with base.open("wb") as dst, partition.open("rb") as src:
                dst.write(mbr)
                dst.seek(1024**2)
                shutil.copyfileobj(src, dst)
            bundle, output = root / "bundle.tar", root / "out.img"
            make_bundle(bundle)
            original = disks.digest(base)
            receipt = provision.compose(
                base, original, bundle, disks.digest(bundle), bound_profile(), output
            )
            self.assertEqual(disks.digest(base), original)
            self.assertEqual(disks.digest(output), receipt["sha256"])
            with PyFatFS(str(output), offset=1024**2, read_only=True) as fs:
                sealed = fs.readbytes("/openaria/profile.bin")
                self.assertEqual(provision.open_profile(sealed, "a" * 64), bound_profile())
                self.assertEqual(fs.readbytes("/openaria/bundle.tar"), bundle.read_bytes())
                self.assertIn("--register", fs.readtext("/hobot_config.sh"))
            with self.assertRaises(FileExistsError):
                provision.compose(
                    base, original, bundle, disks.digest(bundle), bound_profile(), output
                )
            self.assertEqual(disks.digest(output), receipt["sha256"])

    def test_exfat_composition_embeds_layout_tools_and_refuses_old_firmware(self):
        import zipfile

        from pyfatfs.PyFat import PyFat
        from pyfatfs.PyFatFS import PyFatFS

        from tests.test_exfat_storage import vendor_mbr

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            base, partition = root / "vendor.img", root / "fat"
            partition.touch()
            fat = PyFat()
            fat.mkfs(str(partition), PyFat.FAT_TYPE_FAT32, size=256 * 1024**2, label="CONFIG")
            fat.close()
            with PyFatFS(str(partition)) as fs:
                fs.writetext("/hobot_config.sh", "#!/bin/bash -e\nexit 0\n")
            with base.open("wb") as dst, partition.open("rb") as src:
                dst.write(vendor_mbr())
                dst.seek(8192 * 512)
                shutil.copyfileobj(src, dst)
            original = disks.digest(base)
            bundle = root / "bundle.tar"
            make_bundle(bundle)
            output = root / "new.img"
            with self.assertRaises(RuntimeError):
                provision.compose(
                    base,
                    original,
                    bundle,
                    disks.digest(bundle),
                    bound_profile(),
                    output,
                    card_bytes=127999672320,
                )
            self.assertFalse(output.exists())
            wheel_bytes = io.BytesIO()
            with zipfile.ZipFile(wheel_bytes, "w") as wheel:
                for name in ("rp-ylx-data-volume", "rp-ylx-data-volume.service"):
                    source = Path(__file__).parents[1] / "src/rp_ylx/deploy" / name
                    wheel.writestr("rp_ylx/deploy/" + name, source.read_bytes())
            with tarfile.open(bundle, "w") as tar:
                member = tarfile.TarInfo("rp_ylx-test.whl")
                member.size = len(wheel_bytes.getvalue())
                tar.addfile(member, io.BytesIO(wheel_bytes.getvalue()))
            package = root / partition_layout.PACKAGE
            package.write_bytes(b"verified-package-fixture")
            with patch.object(partition_layout, "prepare_tools", return_value=package):
                receipt = provision.compose(
                    base,
                    original,
                    bundle,
                    disks.digest(bundle),
                    bound_profile(),
                    output,
                    card_bytes=127999672320,
                )
            self.assertEqual(original, disks.digest(base))
            self.assertEqual(receipt["sha256"], disks.digest(output))
            partition_layout.check_fit(output, 127999672320)
            with PyFatFS(str(output), offset=8192 * 512, read_only=True) as fs:
                manifest = json.loads(fs.readbytes("/openaria/manifest.json"))
                self.assertEqual(manifest["storage"], receipt["storage"])
                self.assertEqual(fs.readbytes("/openaria/" + package.name), package.read_bytes())
                self.assertTrue(fs.exists("/openaria/storage_firstboot.py"))
            with output.open("rb") as src:
                self.assertEqual(partition_layout.partitions(src.read(512))[2][0], 7)


if __name__ == "__main__":
    unittest.main()
