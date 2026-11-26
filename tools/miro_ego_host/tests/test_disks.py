"""Unit tests for tools/miro_ego_host/disks.py host disk safety & streaming SHA-256."""

from __future__ import annotations

import copy
import hashlib
import io
import os
import plistlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from miro_ego_host import disks


class HostDisksTests(unittest.TestCase):
    def test_linux_source_on_disk_strict_prefix_and_partition_matching(self) -> None:
        # Valid whole-disk & partition matches
        for source, disk in (
            ("/dev/sda", "/dev/sda"),
            ("/dev/sda1", "/dev/sda"),
            ("/dev/sda15", "/dev/sda"),
            ("/dev/sda2[/subvol]", "/dev/sda"),
            ("/dev/mmcblk0", "/dev/mmcblk0"),
            ("/dev/mmcblk0p1", "/dev/mmcblk0"),
            ("/dev/mmcblk0p3[/data]", "/dev/mmcblk0"),
            ("/dev/nvme0n1p2", "/dev/nvme0n1"),
        ):
            self.assertTrue(
                disks.linux_source_on_disk(source, disk),
                f"expected {source} to match {disk}",
            )

        # Prefix-collision & unrelated-disk rejections
        for source, disk in (
            ("/dev/sdab1", "/dev/sda"),
            ("/dev/sdap1", "/dev/sda"),
            ("/dev/sdb1", "/dev/sda"),
            ("/dev/sda_backup", "/dev/sda"),
            ("/dev/mmcblk01", "/dev/mmcblk0"),
            ("/dev/mmcblk0p", "/dev/mmcblk0"),
            ("/dev/mmcblk1p3", "/dev/mmcblk0"),
            ("", "/dev/sda"),
            ("/dev/sda1", ""),
        ):
            self.assertFalse(
                disks.linux_source_on_disk(source, disk),
                f"expected {source} NOT to match {disk}",
            )

    def test_linux_disks_skips_zero_byte_readers_and_blocks_system_disks(self) -> None:
        good = {
            "path": "/dev/sdb",
            "type": "disk",
            "rm": True,
            "ro": False,
            "size": 64 * 1024**3,
            "tran": "usb",
            "serial": "SER-123",
            "model": "USB Card Reader ",
            "mountpoints": [None],
        }
        found = disks.linux_disks({"blockdevices": [good]})
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].path, "/dev/sdb")
        self.assertEqual(found[0].model, "USB Card Reader")

        for mutation in (
            {"size": 0},
            {"size": None},
            {"children": [{"type": "part", "mountpoints": ["/"]}]},
            {"children": [{"type": "part", "mountpoints": ["/boot/efi"]}]},
            {"children": [{"type": "part", "mountpoints": ["[SWAP]"]}]},
            {"children": [{"type": "crypt", "mountpoints": [None]}]},
            {"children": [{"type": "lvm", "mountpoints": [None]}]},
            {"ro": True},
            {"rm": False, "tran": "sata"},
            {"type": "part"},
            {"path": "/dev/nvme0n1"},
        ):
            bad = copy.deepcopy(good)
            bad.update(mutation)
            self.assertEqual(
                disks.linux_disks({"blockdevices": [bad]}),
                [],
                f"expected rejection for {mutation}",
            )

    def test_windows_disks_skips_zero_byte_and_blocks_boot_system_disks(self) -> None:
        good = {
            "Number": 3,
            "Size": 64 * 1024**3,
            "BusType": "USB",
            "IsBoot": False,
            "IsSystem": False,
            "IsReadOnly": False,
            "FriendlyName": "SD Reader",
        }
        self.assertEqual(disks.windows_disks(good)[0].path, r"\\.\PhysicalDrive3")

        for key, value in (
            ("Size", 0),
            ("Size", None),
            ("IsBoot", True),
            ("IsSystem", True),
            ("BusType", "SATA"),
            ("BusType", "NVMe"),
            ("IsReadOnly", True),
        ):
            bad = dict(good)
            bad[key] = value
            self.assertEqual(disks.windows_disks(bad), [], f"expected rejection for {key}={value}")

        # Even with filter_attributes=False, boot/system/0-byte disks remain blocked.
        for key, value in (("Size", 0), ("IsBoot", True), ("IsSystem", True)):
            bad = dict(good)
            bad[key] = value
            self.assertEqual(disks.windows_disks(bad, filter_attributes=False), [])

    def test_macos_disks_skips_zero_byte_and_blocks_apfs_boot_stores(self) -> None:
        def fake_run(*args: str) -> bytes:
            if args[1] == "list":
                value: object = {"WholeDisks": ["disk2", "disk4", "disk5"]}
            elif args[1] == "apfs":
                value = {"Containers": [{"PhysicalStores": [{"DeviceIdentifier": "disk2s2"}]}]}
            elif args[-1] == "/":
                value = {"ParentWholeDisk": "disk3"}
            elif args[-1] == "disk2s2":
                value = {"ParentWholeDisk": "disk2"}
            elif args[-1] == "/dev/disk5":
                value = {
                    "Whole": True,
                    "Internal": False,
                    "Writable": True,
                    "TotalSize": 0,
                    "DeviceTreePath": "empty-reader",
                }
            else:
                value = {
                    "Whole": True,
                    "Internal": False,
                    "Writable": True,
                    "TotalSize": 64 * 1024**3,
                    "DeviceTreePath": "usb-card",
                    "MediaName": "SD Card",
                }
            return plistlib.dumps(value)

        with (
            patch.object(disks.platform, "system", return_value="Darwin"),
            patch.object(disks, "run", side_effect=fake_run),
        ):
            self.assertEqual([d.path for d in disks.discover()], ["/dev/disk4"])

    def test_streaming_sha256_file_and_stream(self) -> None:
        payload = bytes(range(256)) * 513
        expected = hashlib.sha256(payload).hexdigest()
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "sample.bin"
            target.write_bytes(payload)
            self.assertEqual(disks.sha256_file(target, chunk_size=1024), expected)
            self.assertEqual(disks.digest(target), expected)
        stream_hex, stream_bytes = disks.sha256_stream(io.BytesIO(payload), chunk_size=1024)
        self.assertEqual((stream_hex, stream_bytes), (expected, len(payload)))
        with self.assertRaises(RuntimeError):
            disks.sha256_stream(io.BytesIO(b"short"), size=100)

    def test_changed_disk_identity_refuses_unmount(self) -> None:
        d = disks.Disk("/dev/sdb", 64 * 1024**3, "serial-a", "card")
        with (
            patch.object(
                disks, "select", return_value=disks.Disk(d.path, d.size, "serial-b", "card")
            ),
            patch.object(disks, "run") as run_mock,
        ):
            with self.assertRaises(RuntimeError):
                disks.unmount(d)
            run_mock.assert_not_called()

    def test_operate_bad_digest_or_cancel_never_unmounts(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            image = Path(folder) / "base.img"
            image.write_bytes(bytes(1024**2))
            d = disks.Disk("/dev/sdb", 64 * 1024**3, "a", "card")
            with (
                patch.object(disks, "check_source"),
                patch.object(disks, "unmount") as unmount_mock,
            ):
                with self.assertRaises(RuntimeError):
                    disks.operate(d, image, "0" * 64)
                with patch("builtins.input", return_value="no"), self.assertRaises(RuntimeError):
                    disks.operate(d, image, disks.sha256_file(image))
                unmount_mock.assert_not_called()

    def test_write_card_full_erase_and_quick_mode(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            card = Path(folder) / "card"
            image = Path(folder) / "system.img"
            original = b"old-data" * 1024
            card.write_bytes(original)
            image.write_bytes(b"new-img!" * 100)
            d = disks.Disk(str(card), len(original), "test", "file")

            disks.write_card(str(card), os.O_RDWR, d, image, disks.sha256_file(image))
            self.assertEqual(card.read_bytes()[: image.stat().st_size], image.read_bytes())
            self.assertEqual(
                card.read_bytes()[image.stat().st_size :], bytes(d.size - image.stat().st_size)
            )

            card.write_bytes(original)
            disks.write_card(str(card), os.O_RDWR, d, image, disks.sha256_file(image), quick=True)
            self.assertEqual(
                card.read_bytes(), image.read_bytes() + original[image.stat().st_size :]
            )
            with self.assertRaises(RuntimeError):
                disks.write_card(str(card), os.O_RDWR, d, image, "0" * 64, quick=True)
            with self.assertRaises(RuntimeError):
                disks.operate(d, quick=True)

    def test_verify_rejects_partial_reads_and_corruption(self) -> None:
        for data, size, expected in ((b"", 1, None), (b"x", 1, None), (b"x", 1, "0" * 64)):
            with self.assertRaises(RuntimeError):
                disks.verify(io.BytesIO(data), size, expected, lambda *_: None)

    def test_windows_volume_locks_tolerates_raw_unpartitioned_cards_and_null_paths(self) -> None:
        from types import SimpleNamespace

        d = disks.Disk(r"\\.\PhysicalDrive3", 64 * 1024**3, "SER", "SD Reader")
        seen_cmds: list[str] = []

        class FakeWinDLL:
            def __init__(self, *_a: object, **_kw: object) -> None:
                self.CreateFileW = SimpleNamespace(argtypes=None, restype=None)
                self.CloseHandle = SimpleNamespace(argtypes=None)
                self.DeviceIoControl = SimpleNamespace(argtypes=None)

        for ps_output in (b"[]\r\n", b'[null, ""]\r\n', b"\r\n"):

            def fake_ps(cmd: str, out: bytes = ps_output) -> bytes:
                seen_cmds.append(cmd)
                return out

            with (
                patch.object(disks, "powershell", side_effect=fake_ps),
                patch("ctypes.WinDLL", FakeWinDLL, create=True),
                disks.windows_volume_locks(d),
            ):
                pass

        self.assertEqual(len(seen_cmds), 3)
        for cmd in seen_cmds:
            self.assertIn("Get-Partition -DiskNumber 3 -ErrorAction SilentlyContinue", cmd)
            self.assertIn("Get-Volume -ErrorAction SilentlyContinue", cmd)


if __name__ == "__main__":
    unittest.main()
