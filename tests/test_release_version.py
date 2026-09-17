from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.publish_rdk_x5_oss import check_channel_promotion
from scripts.release_version import project_version, release_tag


class ReleaseVersionTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "native").mkdir()
        (self.root / "src/rp_ylx/web").mkdir(parents=True)
        self.set_version("0.2.1")
        self.git("init", "-q")
        self.git("config", "user.email", "test@example.invalid")
        self.git("config", "user.name", "Release Test")
        self.git("add", ".")
        self.git("commit", "-qm", "test release")

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root, text=True).strip()

    def set_version(self, value):
        (self.root / "pyproject.toml").write_text(f'[project]\nversion = "{value}"\n')
        (self.root / "native/Cargo.toml").write_text(f'[package]\nversion = "{value}"\n')
        (self.root / "src/rp_ylx/web/assets.json").write_text(json.dumps({"version": value}))

    def test_annotated_tag_matches_version_commit_and_notes(self):
        self.git("tag", "-a", "v0.2.1", "-m", "新增网页更新\n\n修复安装")
        self.assertEqual(release_tag("v0.2.1", self.root), ("0.2.1", "新增网页更新\n\n修复安装"))
        with self.assertRaises(ValueError):
            release_tag("v0.2.2", self.root)
        (self.root / "other").touch()
        self.git("add", ".")
        self.git("commit", "-qm", "new commit")
        with self.assertRaises(ValueError):
            release_tag("v0.2.1", self.root)

    def test_lightweight_tag_and_version_drift_are_rejected(self):
        self.git("tag", "v0.2.1")
        with self.assertRaises(ValueError):
            release_tag("v0.2.1", self.root)
        (self.root / "src/rp_ylx/web/assets.json").write_text('{"version":"0.1.0"}')
        with self.assertRaises(ValueError):
            project_version(self.root)

    def test_other_series_zero_patch_and_noncanonical_versions_are_rejected(self):
        for value in ("0.3.0", "1.0.0", "0.1.1", "0.2.0", "0.2.01", "0.2.2-rc.1"):
            self.set_version(value)
            with self.assertRaises(ValueError):
                project_version(self.root)
        self.set_version("0.2.123")
        self.assertEqual(project_version(self.root), "0.2.123")

    def test_old_or_conflicting_release_cannot_replace_public_channel(self):
        current = {"version": "0.2.2", "commit": "b" * 40, "bundle": {"sha256": "b" * 64}}
        config = {"public_base_url": "https://example.invalid/rdk-x5"}
        with patch("scripts.publish_rdk_x5_oss.fetch_manifest", return_value=current):
            for candidate in (
                {**current, "version": "0.2.1"},
                {**current, "commit": "c" * 40},
                {**current, "bundle": {"sha256": "c" * 64}},
            ):
                with self.assertRaises(ValueError):
                    check_channel_promotion({"manifest": candidate}, config)
            check_channel_promotion({"manifest": current}, config)
            check_channel_promotion({"manifest": {**current, "version": "0.2.3"}}, config)
