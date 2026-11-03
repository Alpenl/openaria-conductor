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

from scripts.factory_recovery import device, disks, firstboot, provision, recover
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


if __name__ == "__main__":
    unittest.main()
