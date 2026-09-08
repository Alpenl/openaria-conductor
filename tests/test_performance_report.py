from __future__ import annotations

import copy
import unittest

from rp_ylx.performance import PerformanceReportError, validate_performance_report


def report() -> dict[str, object]:
    return {
        "format": "ylx.performance-report.v0",
        "observed_at": "2026-08-13T08:00:00Z",
        "identity": {
            "commit": "a" * 40,
            "wheel_sha256": "b" * 64,
            "software_version": "0.1.0",
        },
        "environment": {
            "evidence_kind": "hardware",
            "machine": "aarch64",
            "kernel": "6.1.83",
            "python": "3.11.9",
            "target": {"board": "rdk_x5_v1.0", "camera": "ylx_2uq2", "supported": True},
        },
        "workload": {
            "kind": "recording",
            "round": 1,
            "duration_ns": 2_000_000_000,
            "frames_input": 40,
            "frames_output": 40,
            "mode": {"width": 3840, "height": 1080, "fps": 60, "encoding": "mjpg"},
        },
        "native": {
            "adapter": "python",
            "module_available": False,
            "module_version": None,
            "abi": None,
        },
        "stages": [
            {"name": "jpeg_split", "samples": 40, "p50_ns": 40, "p95_ns": 60, "total_ns": 1900}
        ],
        "copies": [{"name": "sbs_input", "count": 40, "bytes_total": 4_000_000}],
        "queue": {"capacity": 128, "peak_depth": 4, "rejected": 0},
        "loss": {"source_gap": 0, "application_drop": 0, "unknown_gap": 0},
        "resources": {
            "cpu_time_ns": 1_000_000_000,
            "rss_peak_bytes": 50_000_000,
            "bytes_written": 8_000_000,
        },
        "result": {"effective_fps": 20.0},
    }


class PerformanceReportTest(unittest.TestCase):
    def test_accepts_exact_hardware_report(self) -> None:
        value = report()
        self.assertIs(validate_performance_report(value), value)

    def test_rejects_unknown_field_and_boolean_counter(self) -> None:
        cases = []
        unknown = report()
        unknown["claim"] = "fast"
        cases.append(unknown)
        boolean = report()
        boolean["queue"]["rejected"] = False  # type: ignore[index]
        cases.append(boolean)
        for value in cases:
            with self.subTest(value=value):
                with self.assertRaises(PerformanceReportError) as raised:
                    validate_performance_report(value)
                self.assertEqual(raised.exception.code, "schema_violation")

    def test_fixture_cannot_claim_target_hardware(self) -> None:
        value = report()
        value["environment"]["evidence_kind"] = "fixture"  # type: ignore[index]
        with self.assertRaises(PerformanceReportError) as raised:
            validate_performance_report(value)
        self.assertEqual(raised.exception.code, "false_hardware_claim")

    def test_hardware_requires_exact_target(self) -> None:
        value = report()
        value["environment"]["target"]["board"] = "generic-aarch64"  # type: ignore[index]
        with self.assertRaises(PerformanceReportError) as raised:
            validate_performance_report(value)
        self.assertEqual(raised.exception.code, "target_mismatch")

    def test_percentiles_names_queue_and_rate_are_cross_checked(self) -> None:
        cases: list[tuple[dict[str, object], str]] = []

        percentiles = report()
        percentiles["stages"][0]["p95_ns"] = 39  # type: ignore[index]
        cases.append((percentiles, "invalid_percentile"))

        duplicate = report()
        duplicate["copies"].append(copy.deepcopy(duplicate["copies"][0]))  # type: ignore[union-attr,index]
        cases.append((duplicate, "duplicate_name"))

        queue = report()
        queue["queue"]["peak_depth"] = 129  # type: ignore[index]
        cases.append((queue, "queue_overflow"))

        rate = report()
        rate["result"]["effective_fps"] = 60.0  # type: ignore[index]
        cases.append((rate, "inconsistent_rate"))

        for value, code in cases:
            with self.subTest(code=code):
                with self.assertRaises(PerformanceReportError) as raised:
                    validate_performance_report(value)
                self.assertEqual(raised.exception.code, code)

    def test_queue_rejection_must_be_accounted_as_application_drop(self) -> None:
        value = report()
        value["queue"]["rejected"] = 2  # type: ignore[index]
        with self.assertRaises(PerformanceReportError) as raised:
            validate_performance_report(value)
        self.assertEqual(raised.exception.code, "unaccounted_rejection")

    def test_native_adapter_and_identity_must_agree(self) -> None:
        cases: list[tuple[dict[str, object], str]] = []

        missing = report()
        missing["native"]["adapter"] = "rust"  # type: ignore[index]
        cases.append((missing, "native_unavailable"))

        false_python = report()
        false_python["native"].update(  # type: ignore[union-attr]
            {"module_available": True, "module_version": "0.1.0", "abi": 5}
        )
        cases.append((false_python, "adapter_mismatch"))

        for value, code in cases:
            with self.subTest(code=code):
                with self.assertRaises(PerformanceReportError) as raised:
                    validate_performance_report(value)
                self.assertEqual(raised.exception.code, code)

    def test_observed_at_requires_real_rfc3339_timestamp(self) -> None:
        value = report()
        value["observed_at"] = "not-a-timestamp"
        with self.assertRaises(PerformanceReportError) as raised:
            validate_performance_report(value)
        self.assertEqual(raised.exception.code, "invalid_timestamp")


if __name__ == "__main__":
    unittest.main()
