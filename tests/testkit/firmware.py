"""Firmware supervisor test harness and release link helpers."""

from __future__ import annotations

import json
import tempfile
import time
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

from rp_ylx import update
from rp_ylx.firmware_control import FirmwareSupervisor

from . import ManagedHarness

FIRMWARE_STATUS: dict[str, object] = {
    "schema": "openaria.firmware-status.v1",
    "supported": True,
    "current": {"version": "0.2.1", "commit": "a" * 40},
    "previous": None,
    "available": None,
    "has_update": False,
    "checked_at": None,
    "warning": None,
    "task": None,
}


def firmware_task(
    *,
    task_id: str = "01989f69-0000-4000-8000-000000000001",
    action: str = "update",
    target_commit: str = "a" * 40,
    status: str = "accepted",
    message: str = "更新任务已受理",
    started_at: int = 1_794_144_394,
    finished_at: int | None = None,
) -> dict[str, object]:
    """Build a valid openaria firmware task payload matching _valid_firmware_task."""
    return {
        "id": task_id,
        "action": action,
        "target_commit": target_commit,
        "status": status,
        "message": message,
        "started_at": started_at,
        "finished_at": finished_at,
    }


class FirmwareHarness(ManagedHarness):
    """Temporary install/state harness with patched update.CONFIG, fetch_manifest, require_idle."""

    def _configure(self, stack: ExitStack) -> None:
        self.temporary = stack.enter_context(tempfile.TemporaryDirectory())
        self.root = Path(self.temporary)
        self.install = self.root / "install"
        self.install.mkdir()
        self.state = self.root / "state"
        self.config = self.root / "device.json"
        self.config.write_text(json.dumps({"state_root": str(self.state)}))
        stack.enter_context(patch.object(update, "CONFIG", self.config))
        self.make_release("a", "0.2.1", "current")
        self.make_release("b", "0.1.0", "previous")
        self.supervisor = FirmwareSupervisor(self.state, self.install)
        self.manifest: dict[str, object] = {
            "version": "0.2.2",
            "commit": "c" * 40,
            "release_notes": "修复录制问题",
            "published_at": "2026-09-17T00:00:00Z",
        }
        self.fetch = stack.enter_context(
            patch("rp_ylx.update.fetch_manifest", return_value=self.manifest)
        )
        self.idle = stack.enter_context(patch("rp_ylx.update.require_idle"))

    def make_release(self, char: str, version: str, link: str) -> Path:
        """Create a release directory and atomically point `link` to it."""
        target = self.install / "releases" / (char * 40)
        target.mkdir(parents=True, exist_ok=True)
        (target / "release.json").write_text(json.dumps({"version": version, "commit": char * 40}))
        path = self.install / link
        temporary = path.with_name(path.name + ".next")
        temporary.symlink_to(target)
        temporary.replace(path)
        return target

    def wait_task(self, timeout: float = 3.0) -> dict[str, object]:
        """Poll `supervisor.status()` until the background task reaches succeeded or failed."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self.supervisor.status()
            task = status.get("task")
            if isinstance(task, dict) and task.get("status") in {"succeeded", "failed"}:
                return status
            time.sleep(0.01)
        raise AssertionError("firmware worker did not complete")


class FirmwareMixin(unittest.TestCase):
    """unittest.TestCase mixin provisioning a FirmwareHarness as self.harness."""

    harness: FirmwareHarness

    def setUp(self) -> None:
        super().setUp()
        self.harness = FirmwareHarness().setUp()
        self.addCleanup(self.harness.tearDown)


__all__ = [
    "FIRMWARE_STATUS",
    "FirmwareHarness",
    "FirmwareMixin",
    "firmware_task",
]
