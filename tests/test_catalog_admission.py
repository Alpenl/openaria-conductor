from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.recording.catalog import SessionCatalog


class CatalogAdmissionTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.directory = self.root / "session"
        self.parent = self.directory / "video" / "left"
        self.parent.mkdir(parents=True)
        self.artifacts = [
            {"path": f"video/left/{i:04}.fixture", "bytes": 1, "sha256": "0" * 64}
            for i in range(100)
        ]
        for artifact in self.artifacts:
            (self.directory / artifact["path"]).write_bytes(b"x")
        self.manifest = {"artifacts": self.artifacts}
        (self.directory / "manifest.json").write_text(json.dumps(self.manifest))
        self.summary = {
            "session_id": "session",
            "started_at": "2026",
            "take_id": "fixture",
            "verification": None,
        }
        self.store = SessionCatalog(self.root / "catalog.sqlite3")
        self.store.remember("volume", self.directory, "commit", self.summary, self.manifest)
        self.signature = self.store.signature(self.directory)

    def cached(self):
        return self.store.cached(
            "volume", "session", self.signature, "commit", directory=self.directory
        )

    def assert_rejected(self):
        try:
            result = self.cached()
        except OSError:
            result = None
        self.assertIsNone(result)
        self.assertEqual(self.store._admitted, set())

    def test_admission_checks_all_files_without_repeating_parent_stats(self):
        original = os.stat
        calls = []

        def observe(path, *args, **kwargs):
            calls.append(os.fspath(path))
            return original(path, *args, **kwargs)

        with patch("os.stat", observe):
            self.assertEqual(self.cached(), self.summary)
        self.assertEqual(sum(str(p).endswith(".fixture") for p in calls), 100)
        self.assertLess(len(calls), 115)

    def test_artifact_invalidations_and_failure_release_descriptors(self):
        path = self.directory / self.artifacts[0]["path"]
        baseline = len(os.listdir("/proc/self/fd"))
        for change in ("missing", "size", "link", "symlink", "fifo", "directory"):
            with self.subTest(change=change):
                path.unlink()
                if change == "size":
                    path.write_bytes(b"xx")
                elif change == "link":
                    os.link(self.directory / self.artifacts[1]["path"], path)
                elif change == "symlink":
                    path.symlink_to("0001.fixture")
                elif change == "fifo":
                    os.mkfifo(path)
                elif change == "directory":
                    path.mkdir()
                self.assert_rejected()
                self.assertEqual(len(os.listdir("/proc/self/fd")), baseline)
                if change == "directory":
                    path.rmdir()
                else:
                    path.unlink(missing_ok=True)
                path.write_bytes(b"x")

    def test_static_parent_link_and_escape_are_rejected(self):
        retired = self.root / "retired"
        self.parent.rename(retired)
        self.parent.symlink_to(retired, target_is_directory=True)
        self.assert_rejected()
        self.parent.unlink()
        retired.rename(self.parent)
        for relative in ("../outside", "/outside", "video/../outside", "."):
            with self.subTest(relative=relative):
                with self.store.connection() as db, db:
                    db.execute(
                        "UPDATE artifacts SET path=? WHERE path=?",
                        (relative, self.artifacts[0]["path"]),
                    )
                self.assert_rejected()
                with self.store.connection() as db, db:
                    db.execute(
                        "UPDATE artifacts SET path=? WHERE path=?",
                        (self.artifacts[0]["path"], relative),
                    )

    def test_parent_replaced_by_link_during_file_check_is_rejected(self):
        original = os.stat
        replaced = False

        def race(path, *args, **kwargs):
            nonlocal replaced
            if str(path).endswith("0099.fixture") and not replaced:
                replaced = True
                retired = self.root / "retired"
                self.parent.rename(retired)
                self.parent.symlink_to(retired, target_is_directory=True)
            return original(path, *args, **kwargs)

        with patch("os.stat", race):
            self.assert_rejected()
        self.assertTrue(replaced)

    def test_root_replaced_during_file_check_is_rejected(self):
        original = os.stat
        replaced = False

        def race(path, *args, **kwargs):
            nonlocal replaced
            if str(path).endswith(".fixture") and not replaced:
                replaced = True
                retired = self.root / "retired"
                self.directory.rename(retired)
                self.directory.symlink_to(retired, target_is_directory=True)
            return original(path, *args, **kwargs)

        with patch("os.stat", race):
            self.assert_rejected()
        self.assertTrue(replaced)

    def test_manifest_changed_after_signature_does_not_admit_old_summary(self):
        (self.directory / "manifest.json").write_text("{}")
        self.assert_rejected()

    def test_nested_open_failure_closes_already_opened_directories(self):
        original = os.open
        baseline = len(os.listdir("/proc/self/fd"))

        def fail(path, *args, **kwargs):
            if path == "left":
                raise OSError("injected open failure")
            return original(path, *args, **kwargs)

        with patch("os.open", fail):
            self.assert_rejected()
        self.assertEqual(len(os.listdir("/proc/self/fd")), baseline)

    def test_root_artifacts_and_many_parents_use_bounded_descriptors(self):
        artifacts = [{"path": "root.fixture", "bytes": 1, "sha256": "0" * 64}]
        for index in range(30):
            parent = self.directory / "groups" / str(index)
            parent.mkdir(parents=True)
            artifacts.append(
                {"path": f"groups/{index}/data.fixture", "bytes": 1, "sha256": "0" * 64}
            )
        for artifact in artifacts:
            (self.directory / artifact["path"]).write_bytes(b"x")
        self.manifest["artifacts"].extend(artifacts)
        (self.directory / "manifest.json").write_text(json.dumps(self.manifest))
        self.store.remember("volume", self.directory, "commit", self.summary, self.manifest)
        self.signature = self.store.signature(self.directory)
        baseline = len(os.listdir("/proc/self/fd"))
        high_water = baseline
        original = os.open

        def observe(*args, **kwargs):
            nonlocal high_water
            descriptor = original(*args, **kwargs)
            high_water = max(high_water, len(os.listdir("/proc/self/fd")))
            return descriptor

        with patch("os.open", observe):
            self.assertEqual(self.cached(), self.summary)
        self.assertLessEqual(high_water, baseline + 3)
        self.assertEqual(len(os.listdir("/proc/self/fd")), baseline)


if __name__ == "__main__":
    unittest.main()
