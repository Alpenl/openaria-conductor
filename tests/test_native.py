from __future__ import annotations

import importlib
import unittest
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from unittest.mock import Mock, patch

import rp_ylx.native as native_module
from rp_ylx.native import (
    NativeCapturePlan,
    NativeModuleError,
    NativeRecordingPlan,
    create_native_capture_engine,
    create_native_performance_metrics,
    create_native_preview_buffer,
    create_native_session_store,
    create_native_splitter,
    native_camera_focus_status,
    native_capabilities,
    native_session_store,
    native_session_store_or_none,
    set_native_camera_focus,
)


def _module(*features: str, **members: object) -> SimpleNamespace:
    return SimpleNamespace(
        capabilities=lambda: {
            "module_version": "0.1.0",
            "abi": 5,
            "features": ["capability_probe", *features],
        },
        **members,
    )


def _capture_plan() -> NativeCapturePlan:
    return NativeCapturePlan(
        device="/dev/video0",
        width=3840,
        height=1080,
        fps=60,
        encoding="mjpg",
        buffer_count=16,
        queue_capacity=64,
        frame_decimation=2,
        read_timeout_seconds=2.0,
    )


def _recording_plan() -> NativeRecordingPlan:
    return NativeRecordingPlan(
        session_root="/data/session.partial",
        session_id="01993e29-74e8-7000-8000-000000000001",
        encoder_executable="/opt/rp-ylx/bin/rp-ylx-stereo-encoder",
        width=3840,
        height=1080,
        fps=30,
        bitrate_kbps=8192,
        segment_frames=900,
        recording_start_monotonic_ns=1,
        audio_enabled=False,
        audio_device="default",
        audio_sample_rate_hz=48_000,
        audio_channels=2,
        audio_segment_seconds=30.0,
    )


class NativeCapabilitiesTest(unittest.TestCase):
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

    def test_accepts_exact_abi_five_capability_interface(self) -> None:
        with patch(
            "rp_ylx.native.importlib.import_module",
            return_value=_module("capture_engine", "session_store"),
        ):
            capabilities = native_capabilities()
        self.assertEqual(capabilities.adapter, "rust")
        self.assertEqual(capabilities.as_report_identity()["abi"], 5)

    def test_rejects_dependency_failure_unknown_fields_and_wrong_abi(self) -> None:
        cases = [
            (ModuleNotFoundError(name="native_dependency"), "native_dependency_missing"),
            (
                SimpleNamespace(
                    capabilities=lambda: {
                        "module_version": "0.1.0",
                        "abi": 5,
                        "features": ["capability_probe"],
                        "claim": "fast",
                    }
                ),
                "invalid_native_capabilities",
            ),
            (
                SimpleNamespace(
                    capabilities=lambda: {
                        "module_version": "0.1.0",
                        "abi": 4,
                        "features": ["capability_probe"],
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

    def test_capture_and_recording_plans_are_immutable(self) -> None:
        capture = _capture_plan()
        recording = _recording_plan()
        with self.assertRaises(FrozenInstanceError):
            capture.fps = 30  # type: ignore[misc]
        with self.assertRaises(FrozenInstanceError):
            recording.segment_frames = 1  # type: ignore[misc]

    def test_capture_engine_is_one_recording_level_constructor(self) -> None:
        owner = object()
        constructor = Mock(return_value=owner)
        plan = _capture_plan()
        preview = object()
        metrics = object()
        with patch(
            "rp_ylx.native.importlib.import_module",
            return_value=_module("capture_engine", NativeCaptureEngine=constructor),
        ):
            self.assertIs(
                create_native_capture_engine(plan, preview, metrics=metrics),
                owner,
            )
        constructor.assert_called_once_with(plan, preview, metrics)

    def test_capture_engine_requires_deep_capability_and_preserves_error_code(self) -> None:
        with (
            patch("rp_ylx.native.importlib.import_module", return_value=_module()),
            self.assertRaises(NativeModuleError) as unavailable,
        ):
            create_native_capture_engine(_capture_plan(), object())
        self.assertEqual(unavailable.exception.code, "native_capture_engine_unavailable")

        constructor = Mock(side_effect=RuntimeError("camera_busy: already streaming"))
        with (
            patch(
                "rp_ylx.native.importlib.import_module",
                return_value=_module("capture_engine", NativeCaptureEngine=constructor),
            ),
            self.assertRaises(NativeModuleError) as failed,
        ):
            create_native_capture_engine(_capture_plan(), object())
        self.assertEqual(failed.exception.code, "camera_busy")

    def test_session_store_is_one_process_wide_owner(self) -> None:
        owner = object()
        constructor = Mock(return_value=owner)
        with (
            patch.object(native_module, "_SESSION_STORE", None),
            patch.object(native_module, "_SESSION_STORE_UNAVAILABLE", False),
            patch(
                "rp_ylx.native.importlib.import_module",
                return_value=_module("session_store", NativeSessionStore=constructor),
            ),
        ):
            self.assertIs(native_session_store(), owner)
            self.assertIs(native_session_store_or_none(), owner)
        constructor.assert_called_once_with()

    def test_session_store_optional_lookup_caches_unavailable_result(self) -> None:
        with (
            patch.object(native_module, "_SESSION_STORE", None),
            patch.object(native_module, "_SESSION_STORE_UNAVAILABLE", False),
            patch(
                "rp_ylx.native.create_native_session_store",
                side_effect=NativeModuleError("native_session_store_unavailable", "missing"),
            ) as create,
        ):
            self.assertIsNone(native_session_store_or_none())
            self.assertIsNone(native_session_store_or_none())
        create.assert_called_once_with()

    def test_explicit_session_store_requires_capability(self) -> None:
        with (
            patch("rp_ylx.native.importlib.import_module", return_value=_module()),
            self.assertRaises(NativeModuleError) as raised,
        ):
            create_native_session_store()
        self.assertEqual(raised.exception.code, "native_session_store_unavailable")

    def test_focus_reads_sets_and_validates_inputs(self) -> None:
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
        module = _module(
            "v4l2_focus_control",
            v4l2_focus_status=Mock(side_effect=[status, updated]),
            v4l2_set_focus=Mock(),
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertEqual(native_camera_focus_status("/dev/video0"), status)
            self.assertEqual(
                set_native_camera_focus("/dev/video0", value=77, auto_enabled=False),
                updated,
            )
            with self.assertRaises(NativeModuleError) as raised:
                set_native_camera_focus("/dev/video0", value=True)
        module.v4l2_set_focus.assert_called_once_with("/dev/video0", 77, False)
        self.assertEqual(raised.exception.code, "invalid_camera_focus")

    def test_focus_requires_native_capability(self) -> None:
        with (
            patch("rp_ylx.native.importlib.import_module", return_value=_module()),
            self.assertRaises(NativeModuleError) as raised,
        ):
            native_camera_focus_status("/dev/video0")
        self.assertEqual(raised.exception.code, "native_focus_unavailable")

    def test_splitter_never_silently_falls_back(self) -> None:
        with (
            patch("rp_ylx.native.importlib.import_module", return_value=_module()),
            self.assertRaises(NativeModuleError) as raised,
        ):
            create_native_splitter()
        self.assertEqual(raised.exception.code, "native_splitter_unavailable")

    def test_splitter_preview_and_metrics_return_native_support_owners(self) -> None:
        splitter = object()
        preview = object()
        metrics = object()
        module = _module(
            "turbojpeg_split",
            "preview_buffer",
            "performance_metrics",
            NativeSplitter=Mock(return_value=splitter),
            NativePreviewBuffer=Mock(return_value=preview),
            NativePerformanceMetrics=Mock(return_value=metrics),
        )
        with patch("rp_ylx.native.importlib.import_module", return_value=module):
            self.assertIs(create_native_splitter(), splitter)
            self.assertIs(create_native_preview_buffer(15), preview)
            self.assertIs(create_native_performance_metrics(), metrics)
        module.NativePreviewBuffer.assert_called_once_with(15)

    def test_support_owners_require_their_capabilities(self) -> None:
        for factory, code in (
            (lambda: create_native_preview_buffer(15), "native_preview_buffer_unavailable"),
            (create_native_performance_metrics, "native_metrics_unavailable"),
        ):
            with (
                self.subTest(code=code),
                patch("rp_ylx.native.importlib.import_module", return_value=_module()),
                self.assertRaises(NativeModuleError) as raised,
            ):
                factory()
            self.assertEqual(raised.exception.code, code)

    def test_python_surface_does_not_restore_shallow_resource_owners(self) -> None:
        removed = {
            "create_native_camera",
            "create_native_audio_recorder",
            "create_native_active_take_writer",
            "create_native_imu_collector",
            "create_native_recording_sink",
            "create_native_continuous_capture_runtime",
            "create_native_recording_segment_planner",
            "create_native_stereo_encoder_process",
            "create_native_session_io",
        }
        self.assertEqual({name for name in removed if hasattr(native_module, name)}, set())

    def test_abi_five_extension_exports_only_deep_and_support_classes(self) -> None:
        try:
            extension = importlib.import_module("rp_ylx._native")
        except ModuleNotFoundError as error:
            raise unittest.SkipTest("native wheel is not installed") from error
        if extension.NATIVE_ABI != 5:
            raise unittest.SkipTest("installed native module predates ABI 5")
        exported = {
            name
            for name in dir(extension)
            if name.startswith("Native") and isinstance(getattr(extension, name), type)
        }
        self.assertEqual(
            exported,
            {
                "NativeCaptureEngine",
                "NativeMultipartPreview",
                "NativePerformanceMetrics",
                "NativePreviewBuffer",
                "NativeSessionStore",
                "NativeSessionTransaction",
                "NativeSplitter",
            },
        )
        session_store = extension.NativeSessionStore()
        removed_methods = {
            "device_session_v1_artifact",
            "device_session_v1_artifacts",
            "device_session_v1_summary",
            "hash_file",
            "seal_device_session_v1",
            "write_encoder_frame",
        }
        self.assertEqual(
            {name for name in removed_methods if hasattr(session_store, name)},
            set(),
        )


if __name__ == "__main__":
    unittest.main()
