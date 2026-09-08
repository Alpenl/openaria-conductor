from __future__ import annotations

import os
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from rp_ylx.recording.stereo_encoder import (
    StereoEncoderError,
    StereoEncoderProcess,
    _parse_event,
    _writev_all,
    resolve_executable,
)


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

    def test_submit_uses_one_python_writev_in_explicit_test_adapter(self) -> None:
        encoder = object.__new__(StereoEncoderProcess)
        encoder._process = SimpleNamespace(stdin=SimpleNamespace(fileno=lambda: 7))
        encoder._lock = threading.Lock()
        encoder._failure = None
        encoder._submitted = 0

        with patch("rp_ylx.recording.stereo_encoder._writev_all") as writev:
            encoder.submit(b"abc")

        header, payload = writev.call_args.args[1]
        self.assertEqual(writev.call_args.args[0], 7)
        self.assertEqual(payload, b"abc")
        self.assertEqual(header[:4], b"YLXF")
        self.assertEqual(encoder.submitted_frames, 1)

    def test_parse_event_uses_json(self) -> None:
        self.assertEqual(
            _parse_event(b'{"event":"done","frames":3}'),
            {
                "event": "done",
                "frames": 3,
            },
        )

    def test_handle_event_records_segment_and_failure(self) -> None:
        encoder = object.__new__(StereoEncoderProcess)
        encoder._lock = threading.Lock()
        encoder._segments = []
        encoder._stats = {}
        encoder._failure = None
        encoder._handle_event(
            b'{"event":"segment","index":1,"start_frame":2,"end_frame":3,'
            b'"left":{"path":"video/left_00001.mp4","bytes":10},'
            b'"right":{"path":"video/right_00001.mp4","bytes":11}}'
        )

        self.assertEqual(len(encoder.segments), 1)
        self.assertEqual(encoder.segments[0].left_path, "video/left_00001.mp4")

        encoder._handle_event(b"bad")

        self.assertIsInstance(encoder._failure, StereoEncoderError)
        self.assertEqual(encoder._failure.code, "encoder_failed")
        self.assertEqual(encoder._failure.message, "助手输出不是 JSON")


if __name__ == "__main__":
    unittest.main()
