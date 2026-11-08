"""Partition boundaries, interrupted factory initialization, and admission safety."""

import json
import os
import struct
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from rp_ylx.recording.coordinator import _stat_capacity
from scripts.factory_recovery import firstboot, partition_layout, storage_firstboot


def vendor_mbr():
    raw = bytearray(512)
    raw[440:444] = bytes.fromhex("05722edb")
    raw[450], raw[466] = 0xC, 0x83
    struct.pack_into("<II", raw, 454, 8192, 524288)
    struct.pack_into("<II", raw, 470, 532480, 19767296)
    raw[510:] = b"\x55\xaa"
    return bytes(raw)


class ExfatStorageTests(unittest.TestCase):
    def test_layout_preserves_boot_and_root_start_and_uses_remaining_capacity(self):
        original = vendor_mbr()
        for size in (32 * 1024**3, 127999672320, 256 * 1024**3):
            mbr, layout = partition_layout.plan(original, size)
            entries = partition_layout.partitions(mbr)
            self.assertEqual(mbr[:470], original[:470])
            self.assertEqual(entries[1], (0x83, 532480, 16 * 1024**3 // 512))
            self.assertEqual(entries[2][1], entries[1][1] + entries[1][2])
            self.assertEqual(entries[2][0], 7)
            self.assertLess(size - sum(entries[2][1:]) * 512, 1024**2)
            self.assertEqual(layout["partuuid"], "db2e7205-03")
            with tempfile.TemporaryDirectory() as folder:
                image = Path(folder) / "test.img"
                image.write_bytes(mbr)
                partition_layout.check_fit(image, size)
                with self.assertRaises(RuntimeError):
                    partition_layout.check_fit(image, size - 1024**2)

    def test_rejects_nonvendor_layout_and_insufficient_capacity(self):
        for size, system in ((16 * 1024**3, 16), (32 * 1024**3, 32), (64 * 1024**3, 8)):
            with self.assertRaises(RuntimeError):
                partition_layout.plan(vendor_mbr(), size, system)
        changed = bytearray(vendor_mbr())
        changed[478] = 1
        # Even a non-empty unused partition entry must not be overwritten.
        changed[482] = 0x83
        with self.assertRaises(RuntimeError):
            partition_layout.plan(changed, 64 * 1024**3)

    def test_zero_inode_accounting_is_not_exhausted_ext4(self):
        values = dict(f_frsize=32768, f_blocks=5000, f_bavail=4000, f_files=0, f_favail=0)
        with patch("os.statvfs", return_value=SimpleNamespace(**values)):
            self.assertEqual(_stat_capacity(Path("/data"))[2], 4000)
        values["f_files"] = 5000
        with patch("os.statvfs", return_value=SimpleNamespace(**values)):
            self.assertEqual(_stat_capacity(Path("/data"))[2], 0)

    def test_interrupted_format_retries_but_ready_volume_never_formats_again(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            _, layout = partition_layout.plan(vendor_mbr(), 64 * 1024**3)
            package = root / partition_layout.PACKAGE
            package.write_bytes(b"fixture")
            import hashlib

            sha = hashlib.sha256(package.read_bytes()).hexdigest()
            layout["package_sha256"] = sha
            fail = [True]
            calls = []

            def run(*args, **kwargs):
                calls.append(args)
                if args[0] == "mkfs.exfat" and fail[0]:
                    raise subprocess.CalledProcessError(1, args)
                return "ABCD-1234" if args[0] == "blkid" else ""

            config = root / "etc/default/rp-ylx-data-volume"
            with (
                patch.object(partition_layout, "PACKAGE_SHA256", sha),
                patch.object(storage_firstboot, "CONFIG", config),
                patch.object(
                    storage_firstboot,
                    "target",
                    return_value=(Path("/dev/test2"), Path("/dev/test3")),
                ),
                patch.object(storage_firstboot, "run", side_effect=run),
            ):
                with self.assertRaises(subprocess.CalledProcessError):
                    storage_firstboot.prepare(layout, root, root, firstboot.atomic_json)
                self.assertEqual(
                    json.loads((root / "storage.json").read_text())["phase"], "formatting"
                )
                self.assertFalse(config.exists())
                fail[0] = False
                storage_firstboot.prepare(layout, root, root, firstboot.atomic_json)
                self.assertEqual(json.loads((root / "storage.json").read_text())["phase"], "ready")
                calls.clear()
                config.unlink()
                storage_firstboot.prepare(layout, root, root, firstboot.atomic_json)
                self.assertFalse(any(c[0] in ("mkfs.exfat", "resize2fs") for c in calls))
                self.assertIn("RP_YLX_DATA_VOLUME_UUID=ABCD-1234", config.read_text())
                with (
                    patch.object(storage_firstboot, "run", return_value="BAD0-BAD0"),
                    self.assertRaises(RuntimeError),
                ):
                    storage_firstboot.prepare(layout, root, root, firstboot.atomic_json)

    def test_absent_partition_never_falls_back_to_creating_loop_image(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            binary = root / "bin"
            binary.mkdir()
            getent = binary / "getent"
            getent.write_text("#!/bin/sh\nexit 0\n")
            getent.chmod(0o755)
            image = root / "must-not-exist.img"
            script = Path(__file__).parents[1] / "src/rp_ylx/deploy/rp-ylx-data-volume"
            result = subprocess.run(
                ["bash", str(script), "ensure"],
                capture_output=True,
                env={
                    **os.environ,
                    "PATH": str(binary) + os.pathsep + os.environ["PATH"],
                    "RP_YLX_DATA_VOLUME_MODE": "exfat",
                    "RP_YLX_DATA_VOLUME_PARTUUID": "deadbeef-03",
                    "RP_YLX_DATA_VOLUME_UUID": "DEAD-BEEF",
                    "RP_YLX_DATA_VOLUME_IMAGE": str(image),
                },
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(image.exists())


if __name__ == "__main__":
    unittest.main()
