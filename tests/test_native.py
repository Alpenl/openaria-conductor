from __future__ import annotations

import importlib
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import rp_ylx.native as native_module
from rp_ylx.native import (
    NativeModuleError,
    create_native_active_take_writer,
    create_native_audio_recorder,
    create_native_camera,
    create_native_continuous_capture_runtime,
    create_native_imu_collector,
    create_native_performance_metrics,
    create_native_preview_buffer,
    create_native_recording_segment_planner,
    create_native_recording_sink,
    create_native_session_io,
    create_native_splitter,
    create_native_stereo_encoder_process,
    native_camera_focus_status,
    native_capabilities,
    native_session_io_or_none,
    set_native_camera_focus,
)


class NativeCapabilitiesTest(unittest.TestCase):
    def test_explicit_camera_requires_complete_native_capability(self) -> None:
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "v4l2_capture"],
            }
        )
        with (
            patch("rp_ylx.native.importlib.import_module", return_value=module),
            self.assertRaises(NativeModuleError) as raised,
        ):
            create_native_camera("/dev/video0", 3840, 1080, 60, "mjpg")
        self.assertEqual(raised.exception.code, "native_camera_unavailable")

    def test_explicit_camera_constructs_native_owner_with_bounded_buffers(self) -> None:
        owner = object()
        constructor = unittest.mock.Mock(return_value=owner)
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "v4l2_capture", "native_camera"],
            },
            NativeCameraStream=constructor,
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertIs(
                create_native_camera("/dev/video0", 3840, 1080, 60, "mjpg", buffer_count=6),
                owner,
            )
        constructor.assert_called_once_with("/dev/video0", 3840, 1080, 60, "mjpg", 6, 4, True)

    def test_explicit_camera_focus_requires_native_capability(self) -> None:
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "v4l2_capture"],
            }
        )
        with (
            patch("rp_ylx.native.importlib.import_module", return_value=module),
            self.assertRaises(NativeModuleError) as raised,
        ):
            native_camera_focus_status("/dev/video0")
        self.assertEqual(raised.exception.code, "native_focus_unavailable")

    def test_explicit_camera_focus_reads_sets_and_validates_inputs(self) -> None:
        status = {
            "schema": "ylx.camera-focus.v1",
            "value": 42,
            "minimum": 0,
            "maximum": 255,
            "step": 1,
            "default": 32,
            "auto_supported": True,
            "auto_enabled": False,
        }
        updated = {**status, "value": 77}
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "v4l2_focus_control"],
            },
            v4l2_focus_status=unittest.mock.Mock(side_effect=[status, updated]),
            v4l2_set_focus=unittest.mock.Mock(return_value=updated),
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertEqual(native_camera_focus_status("/dev/video0"), status)
            self.assertEqual(
                set_native_camera_focus("/dev/video0", value=77, auto_enabled=False),
                updated,
            )
            with self.assertRaises(NativeModuleError) as raised:
                set_native_camera_focus("/dev/video0", value=True)
        module.v4l2_focus_status.assert_has_calls(
            [unittest.mock.call("/dev/video0"), unittest.mock.call("/dev/video0")]
        )
        module.v4l2_set_focus.assert_called_once_with("/dev/video0", 77, False)
        self.assertEqual(raised.exception.code, "invalid_camera_focus")

    def test_explicit_camera_can_disable_eye_splitting(self) -> None:
        owner = object()
        constructor = unittest.mock.Mock(return_value=owner)
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "v4l2_capture", "native_camera"],
            },
            NativeCameraStream=constructor,
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertIs(
                create_native_camera(
                    "/dev/video0",
                    3840,
                    1080,
                    60,
                    "mjpg",
                    queue_capacity=64,
                    split_eyes=False,
                ),
                owner,
            )
        constructor.assert_called_once_with("/dev/video0", 3840, 1080, 60, "mjpg", 4, 64, False)

    def test_explicit_splitter_never_silently_falls_back(self) -> None:
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "jpeg_contract", "frame_stream"],
            }
        )
        with (
            patch("rp_ylx.native.importlib.import_module", return_value=module),
            self.assertRaises(NativeModuleError) as raised,
        ):
            create_native_splitter()
        self.assertEqual(raised.exception.code, "native_splitter_unavailable")

    def test_explicit_splitter_returns_native_owner(self) -> None:
        splitter = object()
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": [
                    "capability_probe",
                    "jpeg_contract",
                    "frame_stream",
                    "turbojpeg_split",
                ],
            },
            NativeSplitter=lambda: splitter,
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertIs(create_native_splitter(), splitter)

    def test_source_checkout_explicitly_uses_python_adapter(self) -> None:
        missing = ModuleNotFoundError(name="rp_ylx._native")
        with patch("rp_ylx.native.importlib.import_module", side_effect=missing):
            capabilities = native_capabilities()
        self.assertEqual(
            capabilities.as_dict(),
            {
                "adapter": "python",
                "module_available": False,
                "module_version": None,
                "abi": None,
                "features": [],
            },
        )

    def test_accepts_exact_rust_capability_interface(self) -> None:
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "jpeg_contract", "frame_stream"],
            }
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            capabilities = native_capabilities()
        self.assertEqual(capabilities.adapter, "rust")
        self.assertEqual(capabilities.as_report_identity()["abi"], 4)

    def test_rejects_dependency_failure_unknown_fields_and_wrong_abi(self) -> None:
        cases = [
            (
                ModuleNotFoundError(name="native_dependency"),
                "native_dependency_missing",
            ),
            (
                SimpleNamespace(
                    capabilities=lambda: {
                        "module_version": "0.1.0",
                        "abi": 4,
                        "features": ["capability_probe", "jpeg_contract", "frame_stream"],
                        "claim": "fast",
                    }
                ),
                "invalid_native_capabilities",
            ),
            (
                SimpleNamespace(
                    capabilities=lambda: {
                        "module_version": "0.1.0",
                        "abi": 1,
                        "features": ["capability_probe", "jpeg_contract", "frame_stream"],
                    }
                ),
                "unsupported_native_abi",
            ),
        ]
        for module_or_error, code in cases:
            with (
                self.subTest(code=code),
                patch("rp_ylx.native.importlib.import_module", side_effect=module_or_error)
                if isinstance(module_or_error, BaseException)
                else patch("rp_ylx.native.importlib.import_module", return_value=module_or_error),
                self.assertRaises(NativeModuleError) as raised,
            ):
                native_capabilities()
            self.assertEqual(raised.exception.code, code)

    def test_explicit_audio_requires_native_audio_capability(self) -> None:
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe"],
            }
        )
        with (
            patch("rp_ylx.native.importlib.import_module", return_value=module),
            self.assertRaises(NativeModuleError) as raised,
        ):
            create_native_audio_recorder("/tmp/session")
        self.assertEqual(raised.exception.code, "native_audio_unavailable")

    def test_explicit_audio_returns_native_owner(self) -> None:
        owner = object()
        constructor = unittest.mock.Mock(return_value=owner)
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "native_audio"],
            },
            NativeAudioRecorder=constructor,
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertIs(
                create_native_audio_recorder(
                    "/tmp/session",
                    device="hw:0,0",
                    sample_rate_hz=48_000,
                    channels=2,
                    segment_seconds=30.0,
                ),
                owner,
            )
        constructor.assert_called_once_with("/tmp/session", "hw:0,0", 48_000, 2, 30.0)

    def test_explicit_active_take_writer_requires_native_capability(self) -> None:
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe"],
            }
        )
        with (
            patch("rp_ylx.native.importlib.import_module", return_value=module),
            self.assertRaises(NativeModuleError) as raised,
        ):
            create_native_active_take_writer("session")
        self.assertEqual(raised.exception.code, "active_take_writer_unavailable")

    def test_explicit_active_take_writer_returns_native_owner(self) -> None:
        owner = object()
        constructor = unittest.mock.Mock(return_value=owner)
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "active_take_writer"],
            },
            NativeActiveTakeWriter=constructor,
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertIs(create_native_active_take_writer("session"), owner)
        constructor.assert_called_once_with("session")

    def test_explicit_imu_requires_native_imu_capability(self) -> None:
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe"],
            }
        )
        with (
            patch("rp_ylx.native.importlib.import_module", return_value=module),
            self.assertRaises(NativeModuleError) as raised,
        ):
            create_native_imu_collector("/dev/video0")
        self.assertEqual(raised.exception.code, "native_imu_unavailable")

    def test_explicit_imu_returns_native_owner(self) -> None:
        owner = object()
        constructor = unittest.mock.Mock(return_value=owner)
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "native_imu"],
            },
            NativeImuCollector=constructor,
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertIs(
                create_native_imu_collector(
                    "/dev/video0",
                    unit=None,
                    selector=1,
                    stale_poll_interval=0.001,
                ),
                owner,
            )
        constructor.assert_called_once_with("/dev/video0", None, 1, 0.001)

    def test_explicit_recording_sink_requires_native_capability(self) -> None:
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe"],
            }
        )
        with (
            patch("rp_ylx.native.importlib.import_module", return_value=module),
            self.assertRaises(NativeModuleError) as raised,
        ):
            create_native_recording_sink("/tmp/session", "session")
        self.assertEqual(raised.exception.code, "native_recording_sink_unavailable")

    def test_explicit_recording_sink_returns_native_owner(self) -> None:
        owner = object()
        constructor = unittest.mock.Mock(return_value=owner)
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "recording_sink"],
            },
            NativeRecordingSink=constructor,
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertIs(
                create_native_recording_sink("/tmp/session", "session"),
                owner,
            )
        constructor.assert_called_once_with("/tmp/session", "session", True)

    def test_explicit_continuous_capture_runtime_requires_native_capability(self) -> None:
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe"],
            }
        )
        with (
            patch("rp_ylx.native.importlib.import_module", return_value=module),
            self.assertRaises(NativeModuleError) as raised,
        ):
            create_native_continuous_capture_runtime(object(), object(), 2)
        self.assertEqual(
            raised.exception.code,
            "native_continuous_capture_runtime_unavailable",
        )

    def test_explicit_continuous_capture_runtime_returns_native_owner(self) -> None:
        owner = object()
        constructor = unittest.mock.Mock(return_value=owner)
        camera = object()
        preview = object()
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "continuous_capture_runtime"],
            },
            NativeContinuousCaptureRuntime=constructor,
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertIs(
                create_native_continuous_capture_runtime(
                    camera,
                    preview,
                    3,
                    read_timeout_seconds=1.5,
                ),
                owner,
            )
        constructor.assert_called_once_with(camera, preview, 3, 1.5)

    def test_explicit_continuous_capture_runtime_passes_native_metrics(self) -> None:
        owner = object()
        constructor = unittest.mock.Mock(return_value=owner)
        camera = object()
        preview = object()
        metrics = object()
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "continuous_capture_runtime"],
            },
            NativeContinuousCaptureRuntime=constructor,
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertIs(
                create_native_continuous_capture_runtime(
                    camera,
                    preview,
                    3,
                    read_timeout_seconds=1.5,
                    metrics=metrics,
                ),
                owner,
            )
        constructor.assert_called_once_with(camera, preview, 3, 1.5, metrics)

    def test_explicit_recording_segment_planner_requires_native_capability(self) -> None:
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe"],
            }
        )
        with (
            patch("rp_ylx.native.importlib.import_module", return_value=module),
            self.assertRaises(NativeModuleError) as raised,
        ):
            create_native_recording_segment_planner(3)
        self.assertEqual(raised.exception.code, "native_recording_segment_planner_unavailable")

    def test_explicit_recording_segment_planner_returns_native_owner(self) -> None:
        owner = object()
        constructor = unittest.mock.Mock(return_value=owner)
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "recording_segment_planner"],
            },
            NativeRecordingSegmentPlanner=constructor,
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertIs(create_native_recording_segment_planner(3), owner)
        constructor.assert_called_once_with(3)

    def test_explicit_stereo_encoder_process_requires_native_capability(self) -> None:
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe"],
            }
        )
        with (
            patch("rp_ylx.native.importlib.import_module", return_value=module),
            self.assertRaises(NativeModuleError) as raised,
        ):
            create_native_stereo_encoder_process(
                "/tmp/out",
                "/tmp/encoder",
                width=3840,
                height=1080,
                fps=60,
            )
        self.assertEqual(raised.exception.code, "native_stereo_encoder_process_unavailable")

    def test_explicit_stereo_encoder_process_returns_native_owner(self) -> None:
        owner = object()
        constructor = unittest.mock.Mock(return_value=owner)
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "stereo_encoder_process"],
            },
            NativeStereoEncoderProcess=constructor,
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertIs(
                create_native_stereo_encoder_process(
                    "/tmp/out",
                    "/tmp/encoder",
                    width=3840,
                    height=1080,
                    fps=60,
                    bitrate_kbps=4096,
                    segment_frames=120,
                    path_prefix="v/",
                ),
                owner,
            )
        constructor.assert_called_once_with(
            "/tmp/out",
            "/tmp/encoder",
            3840,
            1080,
            60,
            4096,
            120,
            "v/",
        )

    def test_native_encoder_process_read_methods_wait_during_submit(self) -> None:
        try:
            native = importlib.import_module("rp_ylx._native")
        except ModuleNotFoundError as error:
            raise unittest.SkipTest("native wheel is not installed") from error

        with tempfile.TemporaryDirectory() as directory:
            helper = Path(directory) / "blocking-encoder.py"
            helper.write_text(
                "\n".join(
                    [
                        f"#!{sys.executable}",
                        "import struct",
                        "import sys",
                        "import time",
                        "header = struct.Struct('<4sI')",
                        'sys.stdout.write(\'{"event":"ready"}\\n\')',
                        "sys.stdout.flush()",
                        "time.sleep(0.3)",
                        "while True:",
                        "    data = sys.stdin.buffer.read(header.size)",
                        "    if len(data) != header.size:",
                        "        break",
                        "    magic, size = header.unpack(data)",
                        "    if size == 0:",
                        "        break",
                        "    remaining = size",
                        "    while remaining:",
                        "        chunk = sys.stdin.buffer.read(min(65536, remaining))",
                        "        if not chunk:",
                        "            break",
                        "        remaining -= len(chunk)",
                        'sys.stdout.write(\'{"event":"done","frames":1}\\n\')',
                        "sys.stdout.flush()",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            helper.chmod(0o755)
            process = native.NativeStereoEncoderProcess(
                directory,
                str(helper),
                3840,
                1080,
                60,
                8192,
                900,
                "video/",
            )
            process.start()
            errors: list[BaseException] = []

            def submit_frame() -> None:
                try:
                    process.submit(b"x" * (8 * 1024 * 1024))
                except BaseException as error:  # pragma: no cover - reported below
                    errors.append(error)

            thread = threading.Thread(target=submit_frame)
            thread.start()
            time.sleep(0.05)

            try:
                self.assertEqual(process.submitted_frames(), 1)
            finally:
                process.abort()
                thread.join(timeout=5)

            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])

    def test_explicit_session_io_requires_native_capability(self) -> None:
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe"],
            }
        )
        with (
            patch("rp_ylx.native.importlib.import_module", return_value=module),
            self.assertRaises(NativeModuleError) as raised,
        ):
            create_native_session_io()
        self.assertEqual(raised.exception.code, "native_session_io_unavailable")

    def test_explicit_session_io_returns_native_owner(self) -> None:
        owner = object()
        constructor = unittest.mock.Mock(return_value=owner)
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "session_io"],
            },
            NativeSessionIo=constructor,
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertIs(create_native_session_io(), owner)
        constructor.assert_called_once_with()

    def test_session_io_helper_caches_one_process_wide_owner(self) -> None:
        owner = object()
        with (
            patch.object(native_module, "_SESSION_IO", None),
            patch.object(native_module, "_SESSION_IO_UNAVAILABLE", False),
            patch("rp_ylx.native.create_native_session_io", return_value=owner) as create,
        ):
            self.assertIs(native_session_io_or_none(), owner)
            self.assertIs(native_session_io_or_none(), owner)
        create.assert_called_once_with()

    def test_session_io_helper_caches_unavailable_result(self) -> None:
        with (
            patch.object(native_module, "_SESSION_IO", None),
            patch.object(native_module, "_SESSION_IO_UNAVAILABLE", False),
            patch(
                "rp_ylx.native.create_native_session_io",
                side_effect=NativeModuleError("native_session_io_unavailable", "missing"),
            ) as create,
        ):
            self.assertIsNone(native_session_io_or_none())
            self.assertIsNone(native_session_io_or_none())
        create.assert_called_once_with()

    def test_explicit_preview_buffer_requires_native_capability(self) -> None:
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe"],
            }
        )
        with (
            patch("rp_ylx.native.importlib.import_module", return_value=module),
            self.assertRaises(NativeModuleError) as raised,
        ):
            create_native_preview_buffer(15)
        self.assertEqual(raised.exception.code, "native_preview_buffer_unavailable")

    def test_explicit_preview_buffer_returns_native_owner(self) -> None:
        owner = object()
        constructor = unittest.mock.Mock(return_value=owner)
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "preview_buffer"],
            },
            NativePreviewBuffer=constructor,
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertIs(create_native_preview_buffer(15), owner)
        constructor.assert_called_once_with(15)

    def test_explicit_performance_metrics_requires_native_capability(self) -> None:
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe"],
            }
        )
        with (
            patch("rp_ylx.native.importlib.import_module", return_value=module),
            self.assertRaises(NativeModuleError) as raised,
        ):
            create_native_performance_metrics()
        self.assertEqual(raised.exception.code, "native_metrics_unavailable")

    def test_explicit_performance_metrics_returns_native_owner(self) -> None:
        owner = object()
        constructor = unittest.mock.Mock(return_value=owner)
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "performance_metrics"],
            },
            NativePerformanceMetrics=constructor,
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertIs(create_native_performance_metrics(), owner)
        constructor.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
