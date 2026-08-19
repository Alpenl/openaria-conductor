from __future__ import annotations

import base64
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.camera import CameraMode
from rp_ylx.camera.models import StereoFrame
from rp_ylx.native import NativeCapabilities
from rp_ylx.performance import BenchmarkConfig, BenchmarkError, run_benchmark
from rp_ylx.performance.benchmark import _ExactCameraBackend, _preview_pair_for_frame
from rp_ylx.performance.metrics import PerformanceMetrics
from rp_ylx.performance.report import validate_performance_report

COMMIT = "a" * 40
WHEEL_SHA256 = "b" * 64
LEFT = base64.b64decode(
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDABALDA4MChAODQ4SERATGCgaGBYWGDEjJR0oOjM9"
    "PDkzODdASFxOQERXRTc4UG1RV19iZ2hnPk1xeXBkeFxlZ2P/wAALCAACAAIBAREA/8QAFAAB"
    "AAAAAAAAAAAAAAAAAAAABv/EABQQAQAAAAAAAAAAAAAAAAAAAAD/2gAIAQEAAD8APv/Z"
)
RIGHT = base64.b64decode(
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDABALDA4MChAODQ4SERATGCgaGBYWGDEjJR0oOjM9"
    "PDkzODdASFxOQERXRTc4UG1RV19iZ2hnPk1xeXBkeFxlZ2P/wAALCAACAAIBAREA/8QAFAAB"
    "AAAAAAAAAAAAAAAAAAAABv/EABQQAQAAAAAAAAAAAAAAAAAAAAD/2gAIAQEAAD8AQP/Z"
)


class PerformanceBenchmarkTest(unittest.TestCase):
    def test_fixed_trace_is_strict_fixture_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trace = Path(directory) / "trace.mjpeg"
            trace.write_bytes(LEFT + RIGHT)
            with patch("rp_ylx.performance.benchmark.__commit__", COMMIT):
                report = run_benchmark(
                    BenchmarkConfig("fixed_trace", 0.001, 2, WHEEL_SHA256, trace=trace)
                )

        self.assertIs(validate_performance_report(report), report)
        self.assertEqual(report["environment"]["evidence_kind"], "fixture")
        self.assertFalse(report["environment"]["target"]["supported"])
        self.assertEqual(report["identity"]["wheel_sha256"], WHEEL_SHA256)
        self.assertEqual(report["workload"]["round"], 2)
        self.assertGreater(report["workload"]["frames_output"], 0)
        self.assertEqual(report["loss"], {"source_gap": 0, "application_drop": 0, "unknown_gap": 0})
        self.assertEqual(
            report["native"],
            {
                "adapter": "python",
                "module_available": False,
                "module_version": None,
                "abi": None,
            },
        )

    def test_fixed_trace_rust_adapter_uses_native_splitter_and_reports_identity(self) -> None:
        class Splitter:
            closed = False

            def split(self, payload: bytes, width: int, height: int) -> tuple[bytes, bytes]:
                self.input = (payload, width, height)
                return LEFT, RIGHT

            def close(self) -> None:
                self.closed = True

        splitter = Splitter()
        capabilities = NativeCapabilities(
            True,
            "0.1.0",
            4,
            ("capability_probe", "turbojpeg_split"),
        )
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("rp_ylx.performance.benchmark.__commit__", COMMIT),
            patch("rp_ylx.performance.benchmark.native_capabilities", return_value=capabilities),
            patch("rp_ylx.performance.benchmark.create_native_splitter", return_value=splitter),
        ):
            trace = Path(directory) / "trace.mjpeg"
            trace.write_bytes(LEFT + RIGHT)
            report = run_benchmark(
                BenchmarkConfig(
                    "fixed_trace",
                    0.001,
                    1,
                    WHEEL_SHA256,
                    trace=trace,
                    adapter="rust",
                )
            )

        self.assertEqual(
            report["native"],
            {
                "adapter": "rust",
                "module_available": True,
                "module_version": "0.1.0",
                "abi": 4,
            },
        )
        self.assertEqual(splitter.input[1:], (3840, 1080))
        self.assertTrue(splitter.closed)

    def test_rust_adapter_requires_the_workload_capability(self) -> None:
        capabilities = NativeCapabilities(True, "0.1.0", 4, ("capability_probe",))
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("rp_ylx.performance.benchmark.__commit__", COMMIT),
            patch("rp_ylx.performance.benchmark.native_capabilities", return_value=capabilities),
            self.assertRaises(BenchmarkError) as raised,
        ):
            trace = Path(directory) / "trace.mjpeg"
            trace.write_bytes(LEFT + RIGHT)
            run_benchmark(
                BenchmarkConfig(
                    "fixed_trace",
                    0.001,
                    1,
                    WHEEL_SHA256,
                    trace=trace,
                    adapter="rust",
                )
            )
        self.assertEqual(raised.exception.code, "native_adapter_unavailable")

    def test_hardware_adapter_selects_python_or_rust_with_the_same_metrics(self) -> None:
        metrics = PerformanceMetrics()
        mode = CameraMode(3840, 1080, 60.0, "mjpg")
        for adapter, factory_name in (
            ("python", "V4L2CameraStream"),
            ("rust", "v4l2_production_stream_factory"),
        ):
            with self.subTest(adapter=adapter):
                backend = _ExactCameraBackend(Path("/dev/video0"), metrics, adapter)
                descriptor = backend.discover()[0]
                with patch(f"rp_ylx.performance.benchmark.{factory_name}") as factory:
                    self.assertIs(backend.open(descriptor, mode), factory.return_value)
                self.assertIs(factory.call_args.kwargs["metrics"], metrics)

    def test_concurrent_preview_uses_raw_sbs_when_native_stream_does_not_split_eyes(self) -> None:
        frame = StereoFrame(7, 11, b"", b"", raw_side_by_side=LEFT + RIGHT)

        self.assertEqual(_preview_pair_for_frame(frame), (LEFT + RIGHT, LEFT + RIGHT))

    def test_concurrent_preview_rejects_missing_payloads(self) -> None:
        with self.assertRaises(BenchmarkError) as raised:
            _preview_pair_for_frame(StereoFrame(7, 11, b"", b"", raw_side_by_side=None))

        self.assertEqual(raised.exception.code, "preview_payload_unavailable")

    def test_source_checkout_cannot_emit_candidate_report(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trace = Path(directory) / "trace.mjpeg"
            trace.write_bytes(LEFT + RIGHT)
            with (
                patch("rp_ylx.performance.benchmark.__commit__", "unknown"),
                self.assertRaises(BenchmarkError) as raised,
            ):
                run_benchmark(BenchmarkConfig("fixed_trace", 0.001, 1, WHEEL_SHA256, trace=trace))
        self.assertEqual(raised.exception.code, "unbound_distribution")

    def test_non_target_host_cannot_emit_hardware_evidence(self) -> None:
        facts = {"target": {"board": "unsupported", "camera": "not_found", "supported": False}}
        with (
            patch("rp_ylx.performance.benchmark.collect_hardware_facts", return_value=facts),
            self.assertRaises(BenchmarkError) as raised,
        ):
            run_benchmark(BenchmarkConfig("preview", 0.001, 1, WHEEL_SHA256))
        self.assertEqual(raised.exception.code, "unsupported_target")

    def test_golden_corpus_has_exact_content_digests(self) -> None:
        corpus = json.loads(
            (Path(__file__).parents[1] / "contracts/golden/data-plane-v0.json").read_text()
        )
        left = base64.b64decode(corpus["jpeg"]["left_base64"])
        right = base64.b64decode(corpus["jpeg"]["right_base64"])
        self.assertEqual(hashlib.sha256(left).hexdigest(), corpus["jpeg"]["left_sha256"])
        self.assertEqual(hashlib.sha256(right).hexdigest(), corpus["jpeg"]["right_sha256"])
        self.assertEqual(hashlib.sha256(left + right).hexdigest(), corpus["jpeg"]["sbs_sha256"])


if __name__ == "__main__":
    unittest.main()
