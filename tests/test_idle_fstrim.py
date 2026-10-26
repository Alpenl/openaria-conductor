from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx import update
from rp_ylx.deployment import DEPLOYMENT_ASSETS


class IdleTrimTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.lock = self.root / "maintenance.lock"
        self.lock.touch(mode=0o660)
        self.log = self.root / "calls"
        self.ready = self.root / "ready"
        self.release = self.root / "release"
        self.assets = Path(__file__).resolve().parents[1] / "src/rp_ylx/deploy"
        self.script = self.assets / "rp-ylx-idle-fstrim"
        self.env = dict(
            os.environ,
            OPENARIA_MAINTENANCE_LOCK=str(self.lock),
            OPENARIA_DATA_MOUNT="/test-recordings",
            PATH=f"{self.root}:/usr/bin:/bin",
            TRIM_TEST_ROOT=str(self.root),
        )
        mountpoint = self.root / "mountpoint"
        mountpoint.write_text("#!/bin/sh\nexit 0\n")
        mountpoint.chmod(0o755)
        # Only fstrim is replaced; the real shell/flock and inherited descriptor
        # enforce exclusion across processes. No test discards actual storage.
        trim = self.root / "fstrim"
        trim.write_text(
            f"#!{sys.executable}\n"
            "import fcntl,json,os,pathlib,sys,time\n"
            "r=pathlib.Path(os.environ['TRIM_TEST_ROOT'])\n"
            "os.fstat(8)\n"
            "with (r/'calls').open('a') as f: f.write(json.dumps(sys.argv[1:])+'\\n')\n"
            "(r/'ready').write_text(str(os.getpid()))\n"
            "deadline=time.monotonic()+10\n"
            "while os.environ.get('TRIM_TEST_WAIT') and not (r/'release').exists():\n"
            " if time.monotonic()>deadline: sys.exit(1)\n"
            " time.sleep(.01)\n"
            "sys.exit(int(os.environ.get('TRIM_TEST_EXIT','0')))\n"
        )
        trim.chmod(0o755)

    def run_trim(self):
        return subprocess.run(
            ["sh", str(self.script)], env=self.env, capture_output=True, text=True, timeout=15
        )

    def test_busy_capture_or_update_defers_without_starting_discard(self):
        for shared in (True, False):
            with (
                self.subTest(shared=shared),
                patch.dict(os.environ, self.env),
                update.maintenance_lock(shared=shared),
            ):
                result = self.run_trim()
                self.assertEqual(result.returncode, 75, result.stderr)
                self.assertFalse(self.log.exists())

    def test_idle_trims_data_before_system_mounts_without_replacing_lock(self):
        inode = self.lock.stat().st_ino
        result = self.run_trim()
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()]
        self.assertEqual(calls[0], ["--verbose", "--quiet-unsupported", "/test-recordings"])
        self.assertEqual(
            calls[1],
            ["--listed-in", "/etc/fstab:/proc/self/mountinfo", "--verbose", "--quiet-unsupported"],
        )
        self.assertEqual(self.lock.stat().st_ino, inode)

    def test_child_keeps_capture_excluded_even_when_wrapper_is_killed(self):
        self.env["TRIM_TEST_WAIT"] = "1"
        process = subprocess.Popen(
            ["sh", str(self.script)],
            env=self.env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            deadline = time.monotonic() + 5
            while not self.ready.exists():
                self.assertIsNone(process.poll())
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.01)
            with patch.dict(os.environ, self.env):
                with self.assertRaises(ValueError), update.maintenance_lock(shared=True):
                    pass
                process.terminate()
                process.wait(timeout=5)
                with self.assertRaises(ValueError), update.maintenance_lock(shared=True):
                    pass
            self.release.touch()
            deadline = time.monotonic() + 5
            with self.lock.open() as fd:
                while True:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        self.assertLess(time.monotonic(), deadline)
                        time.sleep(0.01)
        finally:
            self.release.touch()
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=5)

    def test_discard_failure_is_not_reported_as_busy_and_releases_lease(self):
        self.env["TRIM_TEST_EXIT"] = "75"
        result = self.run_trim()
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(self.log.read_text().splitlines()), 1)
        with patch.dict(os.environ, self.env), update.maintenance_lock(shared=True):
            pass

    def test_missing_or_symlink_lock_fails_closed(self):
        self.lock.unlink()
        self.assertEqual(self.run_trim().returncode, 1)
        self.assertFalse(self.lock.exists())
        other = self.root / "other"
        other.touch()
        self.lock.symlink_to(other)
        self.assertEqual(self.run_trim().returncode, 1)
        self.assertFalse(self.log.exists())

    def test_deployment_overrides_existing_timer_target_and_retries_only_busy(self):
        self.assertEqual(
            DEPLOYMENT_ASSETS["openaria-idle-fstrim.conf"],
            ("etc/systemd/system/fstrim.service.d/50-openaria-idle.conf", 0o644),
        )
        self.assertEqual(DEPLOYMENT_ASSETS["rp-ylx-idle-fstrim"][1], 0o755)
        unit = (self.assets / "openaria-idle-fstrim.conf").read_text()
        self.assertIn("ExecStart=\nExecStart=/usr/local/sbin/rp-ylx-idle-fstrim", unit)
        self.assertIn("RestartPreventExitStatus=1", unit)
        self.assertIn("RestartSec=15min", unit)
