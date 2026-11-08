from __future__ import annotations

import base64
import copy
import hashlib
import io
import json
import lzma
import os
import plistlib
import shlex
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts.factory_recovery import device, disks, prepare_image, recover


def profile():
    files = {}
    config = {"state_root": "/var/lib/rp-ylx", "security": {"profile": "lab"}}
    for name, raw in {
        "etc/rp-ylx/device.json": json.dumps(config).encode(),
        "etc/machine-id": b"a" * 32,
    }.items():
        files[name] = {
            "data": base64.b64encode(raw).decode(),
            "sha256": hashlib.sha256(raw).hexdigest(),
        }
    return {"schema": device.SCHEMA, "board": "test-board", "files": files}


class RecoveryTests(unittest.TestCase):
    def test_profile_excludes_recordings_databases_and_traversal(self):
        for name in (
            "data/recordings/a",
            "var/lib/rp-ylx/state.sqlite3",
            "etc/../etc/shadow",
            "/etc/netplan/a",
            "etc/netplan/a/b",
            "etc/ssh/ssh_host_rsa_key",
        ):
            self.assertFalse(device.allowed(name))
            value = profile()
            value["files"][name] = next(iter(value["files"].values()))
            with self.assertRaises(RuntimeError):
                device.validate(value)

    def test_profile_checks_hash_and_required_identity(self):
        value = profile()
        self.assertEqual(len(device.validate(value)), 2)
        value["files"]["etc/machine-id"]["sha256"] = "bad"
        with self.assertRaises(RuntimeError):
            device.validate(value)
        del value["files"]["etc/machine-id"]
        with self.assertRaises(RuntimeError):
            device.validate(value)

    def test_restore_refuses_other_board_before_writes(self):
        with (
            patch.object(device, "hardware", return_value="other"),
            patch.object(Path, "mkdir") as m,
        ):
            with self.assertRaises(RuntimeError):
                device.restore(profile())
            m.assert_not_called()

    def test_restore_refuses_initialized_destination(self):
        with (
            patch.object(device, "hardware", return_value="test-board"),
            patch.object(Path, "exists", return_value=True),
            patch.object(Path, "mkdir") as m,
        ):
            with self.assertRaises(RuntimeError):
                device.restore(profile())
            m.assert_not_called()

    def test_encryption_authenticates_password_and_content(self):
        try:
            from cryptography.exceptions import InvalidTag
        except ImportError:
            self.skipTest("Install factory_recovery/requirements.txt for encryption tests")
        raw = json.dumps(profile()).encode()
        sealed = recover.encrypt(raw, "test-password-long")
        self.assertNotIn(b"machine-id", sealed)
        self.assertEqual(recover.decrypt(sealed, "test-password-long"), raw)
        for payload, password in (
            (sealed, "wrong"),
            (sealed[:-1] + bytes([sealed[-1] ^ 1]), "test-password-long"),
        ):
            with self.assertRaises(InvalidTag):
                recover.decrypt(payload, password)

    def test_private_output_no_overwrite(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "profile.oar"
            recover.save_private(path, b"encrypted")
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                recover.save_private(path, b"replacement")
            self.assertEqual(path.read_bytes(), b"encrypted")

    def test_linux_rejects_system_readonly_internal_and_mapped(self):
        good = {
            "path": "/dev/sdb",
            "type": "disk",
            "rm": True,
            "ro": False,
            "size": 16 * 1024**3,
            "tran": "usb",
            "serial": "123",
            "mountpoints": [None],
        }
        self.assertEqual(len(disks.linux_disks({"blockdevices": [good]})), 1)
        for change in (
            {"children": [{"type": "part", "mountpoints": ["/"]}]},
            {"children": [{"type": "crypt", "mountpoints": [None]}]},
            {"ro": True},
            {"rm": False, "tran": "sata"},
            {"type": "part"},
        ):
            bad = copy.deepcopy(good)
            bad.update(change)
            self.assertEqual(disks.linux_disks({"blockdevices": [bad]}), [])

    def test_windows_rejects_system_and_internal(self):
        d = {
            "Number": 3,
            "Size": 16 * 1024**3,
            "BusType": "USB",
            "IsBoot": False,
            "IsSystem": False,
            "IsReadOnly": False,
        }
        self.assertEqual(disks.windows_disks(d)[0].path, r"\\.\PhysicalDrive3")
        for key, value in (
            ("IsBoot", True),
            ("IsSystem", True),
            ("BusType", "SATA"),
            ("IsReadOnly", True),
        ):
            bad = dict(d)
            bad[key] = value
            self.assertEqual(disks.windows_disks(bad), [])

    def test_macos_rejects_external_apfs_boot_store(self):
        def run(*args):
            if args[1] == "list":
                value = {"WholeDisks": ["disk2", "disk4"]}
            elif args[1] == "apfs":
                value = {"Containers": [{"PhysicalStores": [{"DeviceIdentifier": "disk2s2"}]}]}
            elif args[-1] == "/":
                value = {"ParentWholeDisk": "disk3"}
            elif args[-1] == "disk2s2":
                value = {"ParentWholeDisk": "disk2"}
            else:
                value = {
                    "Whole": True,
                    "Internal": False,
                    "Writable": True,
                    "TotalSize": 16 * 1024**3,
                    "DeviceTreePath": "usb-card",
                }
            return plistlib.dumps(value)

        with (
            patch.object(disks.platform, "system", return_value="Darwin"),
            patch.object(disks, "run", side_effect=run),
        ):
            self.assertEqual([d.path for d in disks.discover()], ["/dev/disk4"])

    def test_changed_disk_identity_refuses_unmount(self):
        d = disks.Disk("/dev/sdb", 16 * 1024**3, "a", "card")
        with (
            patch.object(disks, "select", return_value=disks.Disk(d.path, d.size, "b", "card")),
            patch.object(disks, "run") as run,
        ):
            with self.assertRaises(RuntimeError):
                disks.unmount(d)
            run.assert_not_called()

    def test_flash_bad_digest_and_cancel_do_not_unmount(self):
        with tempfile.TemporaryDirectory() as folder:
            image = Path(folder) / "base.img"
            image.write_bytes(bytes(1024**2))
            d = disks.Disk("/dev/sdb", 16 * 1024**3, "a", "card")
            with patch.object(disks, "check_source"), patch.object(disks, "unmount") as unmount:
                with self.assertRaises(RuntimeError):
                    disks.operate(d, image, "0" * 64)
                with patch("builtins.input", return_value="no"), self.assertRaises(RuntimeError):
                    disks.operate(d, image, disks.digest(image))
                unmount.assert_not_called()

    def test_actual_write_and_verify_use_only_temporary_regular_file(self):
        with tempfile.TemporaryDirectory() as folder:
            card = Path(folder) / "test-card"
            image = Path(folder) / "base.img"
            card.write_bytes(b"private" * 4096)
            image.write_bytes(b"system" * 512)
            d = disks.Disk(str(card), card.stat().st_size, "test", "file")
            disks.write_card(str(card), os.O_RDWR, d, image, disks.digest(image))
            value = card.read_bytes()
            self.assertEqual(value[: image.stat().st_size], image.read_bytes())
            self.assertEqual(value[image.stat().st_size :], bytes(d.size - image.stat().st_size))

    def test_quick_flash_preserves_tail_and_checks_image(self):
        with tempfile.TemporaryDirectory() as folder:
            card, image = Path(folder) / "card", Path(folder) / "system.img"
            original = b"old-data" * 1024
            card.write_bytes(original)
            image.write_bytes(b"new-system" * 100)
            disk = disks.Disk(str(card), len(original), "test", "file")
            disks.write_card(str(card), os.O_RDWR, disk, image, disks.digest(image), quick=True)
            self.assertEqual(
                card.read_bytes(), image.read_bytes() + original[image.stat().st_size :]
            )
            with self.assertRaises(RuntimeError):
                disks.write_card(str(card), os.O_RDWR, disk, image, "0" * 64, quick=True)
            with self.assertRaises(RuntimeError):
                disks.operate(disk, quick=True)

    def test_verification_rejects_partial_reads_and_corruption(self):
        for data, size, digest in ((b"", 1, None), (b"x", 1, None), (b"x", 1, "0" * 64)):
            with self.assertRaises(RuntimeError):
                disks.verify(io.BytesIO(data), size, digest, lambda *_: None)

    def test_restore_writes_profile_to_fresh_root_without_databases(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)

            def rooted(*parts):
                return root / Path(*parts).relative_to("/")

            with (
                patch.object(device, "hardware", return_value="test-board"),
                patch.object(device, "Path", side_effect=rooted),
            ):
                result = device.restore(profile())
            self.assertTrue(result["restored"])
            self.assertEqual((root / "etc/machine-id").read_bytes(), b"a" * 32)
            self.assertFalse((root / "var/lib/rp-ylx").exists())
            self.assertEqual((root / "etc/rp-ylx/device.json").stat().st_mode & 0o777, 0o600)

    def test_restore_refuses_symlink_before_writing_identity(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "etc/rp-ylx").mkdir(parents=True)
            (root / "etc/machine-id").symlink_to(root / "secret")

            def rooted(*parts):
                return root / Path(*parts).relative_to("/")

            with (
                patch.object(device, "hardware", return_value="test-board"),
                patch.object(device, "Path", side_effect=rooted),
                self.assertRaises(RuntimeError),
            ):
                device.restore(profile())
            self.assertFalse((root / "secret").exists())

    def test_image_preparation_checks_vendor_digest_before_extracting(self):
        raw = b"test-system" * 1024
        compressed = lzma.compress(raw)
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / prepare_image.NAME).write_bytes(compressed)
            with self.assertRaises(RuntimeError):
                prepare_image.prepare(root)
            self.assertFalse((root / "image.json").exists())
            with patch.object(prepare_image, "MD5", hashlib.md5(compressed).hexdigest()):
                prepare_image.prepare(root)
            manifest = json.loads((root / "image.json").read_bytes())
            self.assertEqual(manifest["sha256"], hashlib.sha256(raw).hexdigest())
            self.assertEqual(Path(manifest["image"]).read_bytes(), raw)

    def test_ssh_framing_with_root_password_and_passwordless_sudo(self):
        for user, consume_password in (("root", False), ("sunrise", True), ("sunrise", False)):
            pending = io.BytesIO()
            pending.channel = SimpleNamespace(shutdown_write=lambda: None)
            result = []

            def exec_command(
                command, timeout, pending=pending, result=result, consume_password=consume_password
            ):
                self.assertNotIn("fake-login-password", command)

                def execute():
                    if not result:
                        data = pending.getvalue()
                        if consume_password:
                            data = data.split(b"\n", 1)[1]
                        args = shlex.split(command)
                        args = args[args.index("python3") :]
                        result.append(subprocess.run(args, input=data, capture_output=True))
                    return result[0]

                channel = SimpleNamespace(recv_exit_status=lambda: execute().returncode)
                return (
                    pending,
                    SimpleNamespace(read=lambda: execute().stdout, channel=channel),
                    SimpleNamespace(read=lambda: execute().stderr),
                )

            client = SimpleNamespace(exec_command=exec_command)
            raw = recover.remote(
                client,
                "fake-login-password",
                user,
                "import sys,json; print(json.dumps(json.load(sys.stdin)))",
                payload=b'{"config":"test"}',
            )
            self.assertEqual(json.loads(raw), {"config": "test"})

    def test_mirror_range_response_is_checked_before_image_publication(self):
        payload = lzma.compress(b"clean-system" * 1024)
        for valid in (True, False):
            response = io.BytesIO(payload)
            response.status = 206 if valid else 200
            response.headers = {"Content-Range": f"bytes 0-{len(payload) - 1}/{len(payload)}"}
            with (
                tempfile.TemporaryDirectory() as folder,
                patch.object(prepare_image, "ARCHIVE_BYTES", len(payload)),
                patch.object(prepare_image, "MD5", hashlib.md5(payload).hexdigest()),
                patch.object(prepare_image.urllib.request, "urlopen", return_value=response),
            ):
                root = Path(folder)
                if valid:
                    prepare_image.prepare(root)
                    self.assertTrue((root / "image.json").exists())
                else:
                    with self.assertRaises(RuntimeError):
                        prepare_image.prepare(root)
                    self.assertFalse((root / "image.json").exists())
                    self.assertFalse((root / prepare_image.NAME).exists())

    def test_embedded_remote_install_scripts_compile(self):
        compile(recover.INSTALL, "prepare", "exec")
        compile(recover.ACTIVATE, "activate", "exec")


if __name__ == "__main__":
    unittest.main()
