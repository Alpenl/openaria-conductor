"""Run the boot script against a mounted-volume fixture without mounting a disk."""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class DataVolumeOwnershipTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.mount = self.root / "data"
        self.mount.mkdir()
        self.image = self.root / "image"
        self.image.touch()
        (self.mount / ".ylx-volume.json").write_text("{}")
        for name in ("recordings", "sessions"):
            directory = self.mount / name / "session"
            directory.mkdir(parents=True)
            (directory / "manifest.json").write_text("{}")
            (directory / "video.mp4").write_bytes(b"fixture")
        self.bin = self.root / "bin"
        self.bin.mkdir()
        # OS/mount setup is mocked; ownership traversal uses real find/chown.
        for name in ("getent", "install", "mountpoint", "findmnt", "df"):
            self.executable(name, "#!/bin/sh\nexit 0\n")
        self.executable(
            "id",
            f'#!/bin/sh\ncase "$1" in -u) echo {os.getuid()};; -g) echo {os.getgid()};; esac\n',
        )
        chown = shutil.which("chown")
        self.assertIsNotNone(chown)
        self.executable(
            "chown",
            f"#!{sys.executable}\nimport os,sys\n"
            f"owner='{os.getuid()}:{os.getgid()}'\n"
            "args=[a.replace('rp-ylx:rp-ylx', owner) for a in sys.argv[1:]]\n"
            f"os.execv({chown!r}, [{chown!r}, *args])\n",
        )
        self.environment = {
            **os.environ,
            "PATH": str(self.bin) + os.pathsep + os.environ["PATH"],
            "RP_YLX_DATA_VOLUME_IMAGE": str(self.image),
            "RP_YLX_DATA_VOLUME_MOUNT": str(self.mount),
            "RP_YLX_DATA_VOLUME_OWNER": f"{os.getuid()}:{os.getgid()}",
        }
        self.script = Path(__file__).parents[1] / "src/rp_ylx/deploy/rp-ylx-data-volume"

    def executable(self, name, body):
        path = self.bin / name
        path.write_text(body)
        path.chmod(0o755)

    def ensure(self):
        subprocess.run(
            ["bash", str(self.script), "ensure"],
            env=self.environment,
            check=True,
            capture_output=True,
            text=True,
        )

    def identities(self):
        return {
            str(p.relative_to(self.mount)): (
                p.lstat().st_ino,
                p.lstat().st_ctime_ns,
                p.lstat().st_mtime_ns,
                p.lstat().st_uid,
                p.lstat().st_gid,
            )
            for p in self.mount.rglob("*")
        }

    def test_repeated_boot_preserves_existing_capture_file_identities(self):
        before = self.identities()
        self.ensure()
        self.ensure()
        self.assertEqual(self.identities(), before)

    @unittest.skipUnless(os.geteuid() == 0, "ownership repair requires root")
    def test_repairs_only_mismatched_owners_without_following_links(self):
        correct = self.mount / "sessions/session/manifest.json"
        correct_identity = correct.stat().st_ctime_ns
        wrong = self.mount / "recordings/session/video.mp4"
        os.chown(wrong, 65534, 65534)
        group_only = self.mount / "recordings/session/manifest.json"
        os.chown(group_only, 0, 65534)
        outside = self.root / "outside"
        outside.mkdir()
        payload = outside / "untouched"
        payload.write_bytes(b"outside volume")
        os.chown(payload, 65534, 65534)
        identity = payload.stat().st_ctime_ns
        link = self.mount / "recordings/external"
        link.symlink_to(outside, target_is_directory=True)
        os.chown(link, 65534, 65534, follow_symlinks=False)
        self.ensure()
        for p in (wrong, group_only, link):
            self.assertEqual((p.lstat().st_uid, p.lstat().st_gid), (os.getuid(), os.getgid()))
        self.assertEqual(correct.stat().st_ctime_ns, correct_identity)
        self.assertEqual((payload.stat().st_uid, payload.stat().st_gid), (65534, 65534))
        self.assertEqual(payload.stat().st_ctime_ns, identity)
        after = self.identities()
        self.ensure()
        self.assertEqual(self.identities(), after)


if __name__ == "__main__":
    unittest.main()
