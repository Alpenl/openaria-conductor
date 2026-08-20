from __future__ import annotations

import os
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from rp_ylx.native import NativeModuleError
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

    def test_submit_uses_native_encoder_write_when_available(self) -> None:
        encoder = object.__new__(StereoEncoderProcess)
        encoder._process = SimpleNamespace(stdin=SimpleNamespace(fileno=lambda: 7))
        encoder._lock = threading.Lock()
        encoder._failure = None
        encoder._submitted = 0
        encoder._native_pipe = None
        native = SimpleNamespace(write_encoder_frame=Mock(return_value=11))

        with (
            patch("rp_ylx.recording.stereo_encoder._session_io_or_none", return_value=native),
            patch("rp_ylx.recording.stereo_encoder._writev_all") as fallback,
        ):
            encoder.submit(b"abc")

        native.write_encoder_frame.assert_called_once_with(7, b"abc")
        fallback.assert_not_called()
        self.assertEqual(encoder.submitted_frames, 1)

    def test_submit_prefers_native_encoder_pipe_when_available(self) -> None:
        encoder = object.__new__(StereoEncoderProcess)
        encoder._process = SimpleNamespace(stdin=SimpleNamespace(fileno=lambda: 7))
        encoder._lock = threading.Lock()
        encoder._failure = None
        encoder._submitted = 0
        encoder._native_pipe = SimpleNamespace(
            submit=Mock(return_value=11),
            submitted_frames=Mock(return_value=1),
        )

        with (
            patch("rp_ylx.recording.stereo_encoder._session_io_or_none") as session_io,
            patch("rp_ylx.recording.stereo_encoder._writev_all") as fallback,
        ):
            encoder.submit(b"abc")

        encoder._native_pipe.submit.assert_called_once_with(b"abc")
        session_io.assert_not_called()
        fallback.assert_not_called()
        self.assertEqual(encoder.submitted_frames, 1)

    def test_start_prefers_native_encoder_process_when_available(self) -> None:
        native = SimpleNamespace(
            start=Mock(),
            submit=Mock(return_value=11),
            finish=Mock(
                return_value=[
                    {
                        "index": 1,
                        "start_frame": 2,
                        "end_frame": 3,
                        "left_path": "video/left_00001.mp4",
                        "left_bytes": 10,
                        "right_path": "video/right_00001.mp4",
                        "right_bytes": 11,
                    }
                ]
            ),
            abort=Mock(),
            segments=Mock(
                return_value=[
                    {
                        "index": 1,
                        "start_frame": 2,
                        "end_frame": 3,
                        "left_path": "video/left_00001.mp4",
                        "left_bytes": 10,
                        "right_path": "video/right_00001.mp4",
                        "right_bytes": 11,
                    }
                ]
            ),
            stats=Mock(return_value={"frames": 3}),
            submitted_frames=Mock(return_value=1),
        )
        with tempfile.TemporaryDirectory() as directory:
            helper = Path(directory) / "ylx-stereo-encoder"
            helper.write_text("#!/bin/sh\n", encoding="utf-8")
            encoder = StereoEncoderProcess(
                directory,
                executable=helper,
                width=3840,
                height=1080,
                fps=60,
            )
            with (
                patch(
                    "rp_ylx.recording.stereo_encoder._encoder_process_or_none",
                    return_value=native,
                ) as create_native,
                patch("rp_ylx.recording.stereo_encoder.subprocess.Popen") as popen,
            ):
                encoder.start()
                encoder.submit(b"abc")
                segments = encoder.finish(timeout=1.5)

        create_native.assert_called_once()
        native.start.assert_called_once_with()
        native.submit.assert_called_once_with(b"abc")
        native.finish.assert_called_once_with(1.5)
        popen.assert_not_called()
        self.assertEqual(segments[0].left_path, "video/left_00001.mp4")
        self.assertEqual(encoder.segments[0].right_bytes, 11)
        self.assertEqual(encoder.stats, {"frames": 3})
        self.assertEqual(encoder.submitted_frames, 1)

    def test_native_encoder_process_init_failure_does_not_fallback_to_python_process(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            helper = Path(directory) / "ylx-stereo-encoder"
            helper.write_text("#!/bin/sh\n", encoding="utf-8")
            encoder = StereoEncoderProcess(
                directory,
                executable=helper,
                width=3840,
                height=1080,
                fps=60,
            )
            with (
                patch("rp_ylx.recording.stereo_encoder._ENCODER_PROCESS_UNAVAILABLE", False),
                patch(
                    "rp_ylx.recording.stereo_encoder.create_native_stereo_encoder_process",
                    side_effect=NativeModuleError(
                        "native_stereo_encoder_process_init_failed",
                        "bad native process",
                    ),
                ),
                patch("rp_ylx.recording.stereo_encoder.subprocess.Popen") as popen,
                self.assertRaises(StereoEncoderError) as raised,
            ):
                encoder.start()

        self.assertEqual(raised.exception.code, "native_stereo_encoder_process_init_failed")
        popen.assert_not_called()

    def test_parse_event_uses_native_parser_when_available(self) -> None:
        parsed = {"event": "done", "frames": 3}
        native = SimpleNamespace(parse=Mock(return_value=parsed))

        with patch("rp_ylx.recording.stereo_encoder._encoder_events_or_none", return_value=native):
            self.assertEqual(_parse_event(b"ignored by fake native"), parsed)

        native.parse.assert_called_once_with(b"ignored by fake native")

    def test_handle_event_records_native_segment_and_failure(self) -> None:
        encoder = object.__new__(StereoEncoderProcess)
        encoder._lock = threading.Lock()
        encoder._segments = []
        encoder._stats = {}
        encoder._failure = None
        native = SimpleNamespace(
            parse=Mock(
                return_value={
                    "event": "segment",
                    "index": 1,
                    "start_frame": 2,
                    "end_frame": 3,
                    "left": {"path": "video/left_00001.mp4", "bytes": 10},
                    "right": {"path": "video/right_00001.mp4", "bytes": 11},
                }
            )
        )

        with patch("rp_ylx.recording.stereo_encoder._encoder_events_or_none", return_value=native):
            encoder._handle_event(b"segment")

        self.assertEqual(len(encoder.segments), 1)
        self.assertEqual(encoder.segments[0].left_path, "video/left_00001.mp4")

        native.parse.side_effect = RuntimeError("encoder_failed: malformed event")
        with patch("rp_ylx.recording.stereo_encoder._encoder_events_or_none", return_value=native):
            encoder._handle_event(b"bad")

        self.assertIsInstance(encoder._failure, StereoEncoderError)
        self.assertEqual(encoder._failure.code, "encoder_failed")
        self.assertEqual(encoder._failure.message, "malformed event")


if __name__ == "__main__":
    unittest.main()
