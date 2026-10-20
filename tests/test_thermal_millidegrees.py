from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from rp_ylx.api.events import (
    project_capture_status,
    validate_capture_status,
    validate_device_descriptor,
)
from rp_ylx.runtime import _temperature, collect_linux_runtime
from tests import test_capture_coordinator as fixture


class ThermalMillidegreesTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.temp = self.root / "thermal_zone0" / "temp"
        self.temp.parent.mkdir()

    def reading(self, raw: str) -> float:
        self.temp.write_text(raw, encoding="ascii")
        return _temperature((self.temp,))

    def test_signed_subdegree_readings_are_never_guessed_as_celsius(self) -> None:
        for raw, expected in (
            ("1", 0.001),
            ("52", 0.052),
            ("125", 0.125),
            ("126", 0.126),
            ("500", 0.5),
            ("999", 0.999),
            ("-1", -0.001),
            ("-40", -0.04),
            ("-41", -0.041),
            ("-500", -0.5),
            ("-999", -0.999),
            ("0", 0.0),
        ):
            with self.subTest(raw=raw):
                self.assertEqual(self.reading(raw), expected)

    def test_readings_are_continuous_across_one_degree(self) -> None:
        for raw, expected in (
            ("999", 0.999),
            ("1000", 1.0),
            ("1001", 1.001),
            ("-999", -0.999),
            ("-1000", -1.0),
            ("-1001", -1.001),
        ):
            with self.subTest(raw=raw):
                self.assertEqual(self.reading(raw), expected)

    def test_hottest_zone_is_selected_after_unit_conversion(self) -> None:
        other = self.root / "other_temp"
        other.write_text("25000\n", encoding="ascii")
        self.temp.write_text("125\n", encoding="ascii")
        self.assertEqual(_temperature((self.temp, other)), 25.0)
        self.temp.write_text("-500\n", encoding="ascii")
        other.write_text("-1000\n", encoding="ascii")
        self.assertEqual(_temperature((self.temp, other)), -0.5)

    def test_api_range_and_typical_soc_temperatures_remain_valid(self) -> None:
        for raw, expected in (
            ("-40000", -40.0),
            ("125000", 125.0),
            ("40359\n", 40.359),
            ("52000", 52.0),
            ("-40001", 0.0),
            ("125001", 0.0),
        ):
            with self.subTest(raw=raw):
                self.assertEqual(self.reading(raw), expected)

    def test_bad_or_missing_sensor_does_not_hide_healthy_reading(self) -> None:
        other = self.root / "healthy"
        other.write_text("40359\n", encoding="ascii")
        for raw in (b"NaN", b"inf", b"-inf", b"broken", b"\xff", b"", b"999999999"):
            with self.subTest(raw=raw):
                self.temp.write_bytes(raw)
                self.assertEqual(_temperature((self.temp, other, self.root / "missing")), 40.359)
        self.assertEqual(_temperature(()), 0.0)
        self.assertEqual(_temperature((self.root / "missing",)), 0.0)

    def test_status_and_descriptor_preserve_subdegree_readings(self) -> None:
        self.temp.write_text("125\n", encoding="ascii")
        case = fixture.CaptureCoordinatorTest()
        case.setUp()
        self.addCleanup(case.tearDown)
        coordinator = case.coordinator(sources=fixture.FakeSources())
        self.addCleanup(coordinator.close)
        coordinator._runtime = lambda: collect_linux_runtime(
            net_root=self.root / "missing_net",
            route_path=self.root / "missing_route",
            thermal_paths=(self.temp,),
            ipv4_lookup=lambda name: [],
        )
        for version in ("v2", "v3", "v4"):
            with self.subTest(version=version):
                status = project_capture_status(coordinator.capture_status(), api_version=version)
                validate_capture_status(status, api_version=version)
                self.assertEqual(status["snapshot"]["runtime"]["temperature_celsius"], 0.125)
                descriptor = coordinator.device_descriptor(version, "lab")
                validate_device_descriptor(descriptor, api_version=version, security_profile="lab")
                self.assertEqual(descriptor["runtime"]["temperature_celsius"], 0.125)


if __name__ == "__main__":
    unittest.main()
