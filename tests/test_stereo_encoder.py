from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.recording.stereo_encoder import _writev_all, resolve_executable


class StereoEncoderResolutionTest(unittest.TestCase):
    def test_installed_release_helper_is_found_from_managed_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            release = Path(directory) / "release"
            runtime_bin = release / "runtime/bin"
            release_bin = release / "bin"
            runtime_bin.mkdir(parents=True)
            release_bin.mkdir()
            helper = release_bin / "ylx-stereo-encoder"
            helper.write_text("#!/bin/sh\n", encoding="utf-8")
            executable = runtime_bin / "python3"
            executable.write_text("", encoding="utf-8")

            with (
                patch("rp_ylx.recording.stereo_encoder.sys.executable", str(executable)),
                patch.dict(os.environ, {"PYTHONPATH": ""}, clear=False),
            ):
                self.assertEqual(resolve_executable(), helper.resolve())

    def test_installed_release_helper_is_found_from_pythonpath(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            release = Path(directory) / "release"
            site_packages = release / "site-packages"
            release_bin = release / "bin"
            site_packages.mkdir(parents=True)
            release_bin.mkdir()
            helper = release_bin / "ylx-stereo-encoder"
            helper.write_text("#!/bin/sh\n", encoding="utf-8")

            with (
                patch("rp_ylx.recording.stereo_encoder.sys.executable", "/usr/bin/python3"),
                patch.dict(os.environ, {"PYTHONPATH": str(site_packages)}, clear=False),
            ):
                self.assertEqual(resolve_executable(), helper.resolve())

    def test_writev_all_handles_partial_pipe_writes(self) -> None:
        writes: list[bytes] = []

        def fake_writev(descriptor: int, chunks: object) -> int:
            self.assertEqual(descriptor, 7)
            remaining = b"".join(bytes(chunk) for chunk in chunks)  # type: ignore[union-attr]
            selected = remaining[:3]
            writes.append(selected)
            return len(selected)

        with patch("rp_ylx.recording.stereo_encoder.os.writev", side_effect=fake_writev):
            _writev_all(7, (b"abcd", b"efgh"))
        self.assertEqual(b"".join(writes), b"abcdefgh")


if __name__ == "__main__":
    unittest.main()
