from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from rp_ylx.native import (
    NativeModuleError,
    create_native_audio_recorder,
    create_native_camera,
    create_native_imu_collector,
    create_native_performance_metrics,
    create_native_preview_buffer,
    create_native_recording_codec,
    create_native_recording_event_queue,
    create_native_recording_frame_gate,
    create_native_recording_sink,
    create_native_session_io,
    create_native_splitter,
    native_capabilities,
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

    def test_explicit_recording_codec_requires_native_capability(self) -> None:
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
            create_native_recording_codec()
        self.assertEqual(raised.exception.code, "native_recording_unavailable")

    def test_explicit_recording_codec_returns_native_owner(self) -> None:
        owner = object()
        constructor = unittest.mock.Mock(return_value=owner)
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "recording_codec"],
            },
            NativeRecordingCodec=constructor,
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertIs(create_native_recording_codec(), owner)
        constructor.assert_called_once_with()

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
            create_native_recording_sink("/tmp/session", "session", split_eyes=True)
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
                create_native_recording_sink("/tmp/session", "session", split_eyes=True),
                owner,
            )
        constructor.assert_called_once_with("/tmp/session", "session", True)

    def test_explicit_recording_frame_gate_requires_native_capability(self) -> None:
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
            create_native_recording_frame_gate(2)
        self.assertEqual(raised.exception.code, "native_recording_frame_gate_unavailable")

    def test_explicit_recording_frame_gate_returns_native_owner(self) -> None:
        owner = object()
        constructor = unittest.mock.Mock(return_value=owner)
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "recording_frame_gate"],
            },
            NativeRecordingFrameGate=constructor,
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertIs(create_native_recording_frame_gate(3), owner)
        constructor.assert_called_once_with(3)

    def test_explicit_recording_event_queue_requires_native_capability(self) -> None:
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
            create_native_recording_event_queue(128)
        self.assertEqual(raised.exception.code, "native_recording_event_queue_unavailable")

    def test_explicit_recording_event_queue_returns_native_owner(self) -> None:
        owner = object()
        constructor = unittest.mock.Mock(return_value=owner)
        module = SimpleNamespace(
            capabilities=lambda: {
                "module_version": "0.1.0",
                "abi": 4,
                "features": ["capability_probe", "recording_event_queue"],
            },
            NativeRecordingEventQueue=constructor,
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertIs(create_native_recording_event_queue(256), owner)
        constructor.assert_called_once_with(256)

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
