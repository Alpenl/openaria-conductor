from __future__ import annotations

import os
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from rp_ylx.recording.stereo_encoder import StereoEncoderProcess, _writev_all, resolve_executable


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

    def test_submit_uses_native_encoder_write_when_available(self) -> None:
        encoder = object.__new__(StereoEncoderProcess)
        encoder._process = SimpleNamespace(stdin=SimpleNamespace(fileno=lambda: 7))
        encoder._lock = threading.Lock()
        encoder._failure = None
        encoder._submitted = 0
        native = SimpleNamespace(write_encoder_frame=Mock(return_value=11))

        with (
            patch("rp_ylx.recording.stereo_encoder._session_io_or_none", return_value=native),
            patch("rp_ylx.recording.stereo_encoder._writev_all") as fallback,
        ):
            encoder.submit(b"abc")

        native.write_encoder_frame.assert_called_once_with(7, b"abc")
        fallback.assert_not_called()
        self.assertEqual(encoder.submitted_frames, 1)


if __name__ == "__main__":
    unittest.main()
