from __future__ import annotations

import os
import stat
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from rp_ylx.recording.backing_storage import backing_capacity, require_backing_reserve
from rp_ylx.recording.device_session import DeviceRecordingError


class BackingCapacityTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.mount = self.root / "data"
        self.mount.mkdir()
        self.image = self.root / "image with spaces.img"
        self.image.touch()
        self.sys = self.root / "sys"
        self.devices = {self.mount: (7, 0), self.image: (8, 1)}
        self.modes = {}
        self.capacities = {self.image: (1000, 500)}
        real_stat = Path.stat

        def path_stat(path, *args, **kwargs):
            actual = real_stat(path, *args, **kwargs)
            if path in self.devices:
                return SimpleNamespace(
                    st_dev=os.makedev(*self.devices[path]),
                    st_mode=self.modes.get(path, actual.st_mode),
                )
            return actual

        def statvfs(path):
            free, inodes = self.capacities[path]
            return SimpleNamespace(f_bavail=free, f_frsize=4096, f_favail=inodes)

        for context in [
            patch("rp_ylx.recording.backing_storage.SYS_DEV_BLOCK", self.sys),
            patch.object(Path, "stat", path_stat),
            patch("rp_ylx.recording.backing_storage.os.statvfs", statvfs),
        ]:
            context.start()
            self.addCleanup(context.stop)

    def attach(self, device, backing):
        p = self.sys / f"{device[0]}:{device[1]}" / "loop/backing_file"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(str(backing) + "\n")

    def test_physical_volume_has_no_file_backing_budget(self):
        self.devices[self.mount] = (179, 2)
        self.assertIsNone(backing_capacity(self.mount))

    def test_loop_uses_host_available_not_reserved_or_logical_bytes(self):
        self.attach((7, 0), self.image)
        self.assertEqual(backing_capacity(self.mount), (4096000, 500))
        require_backing_reserve(self.mount, 4096000, 500)

    def test_nested_loops_use_tightest_budget_without_adding_space(self):
        self.devices[self.image] = (7, 1)
        outer = self.root / "outer.img"
        outer.touch()
        self.devices[outer] = (8, 1)
        self.capacities[outer] = (50, 900)
        self.attach((7, 0), self.image)
        self.attach((7, 1), outer)
        self.assertEqual(backing_capacity(self.mount), (50 * 4096, 500))

    def test_host_reserve_reports_the_correct_resource(self):
        self.attach((7, 0), self.image)
        for byte_limit, inode_limit, code in [
            (4096001, 0, "insufficient_space"),
            (0, 501, "insufficient_inodes"),
        ]:
            with self.subTest(code=code), self.assertRaises(DeviceRecordingError) as error:
                require_backing_reserve(self.mount, byte_limit, inode_limit)
            self.assertEqual(error.exception.code, code)

    def test_missing_loop_identity_is_not_treated_as_physical_storage(self):
        with self.assertRaises(DeviceRecordingError) as error:
            backing_capacity(self.mount)
        self.assertEqual(error.exception.code, "storage_unavailable")

    def test_missing_backing_file_or_malformed_path_fails_closed(self):
        for target in [self.root / "missing.img", "relative.img", ""]:
            with self.subTest(target=target):
                self.attach((7, 0), target)
                with self.assertRaises(DeviceRecordingError):
                    backing_capacity(self.mount)

    def test_permission_error_does_not_disable_guard(self):
        self.attach((7, 0), self.image)
        with (
            patch.object(Path, "read_text", side_effect=PermissionError),
            self.assertRaises(DeviceRecordingError),
        ):
            backing_capacity(self.mount)

    def test_cycle_is_rejected(self):
        self.devices[self.image] = (7, 0)
        self.attach((7, 0), self.image)
        with self.assertRaises(DeviceRecordingError):
            backing_capacity(self.mount)

    def test_block_backed_loop_does_not_charge_device_node_filesystem(self):
        self.attach((7, 0), self.image)
        self.modes[self.image] = stat.S_IFBLK | 0o600
        self.assertIsNone(backing_capacity(self.mount))
