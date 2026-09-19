import math
import unittest

from rp_ylx.recording.cadence import FrameCadence


class FrameCadenceTests(unittest.TestCase):
    def measure(self, intervals):
        cadence = FrameCadence(30)
        timestamp = 8_000_000_000_000_000
        cadence.observe(timestamp)
        for interval in intervals:
            timestamp += interval
            cadence.observe(timestamp)
        return cadence.summary()

    def test_nanosecond_quantized_30fps_at_large_clock_origin(self):
        result = self.measure([33_333_333, 33_333_333, 33_333_334] * 110)
        self.assertEqual(result["status"], "within_tolerance")
        self.assertAlmostEqual(result["rate_error_ppm"], 0)
        self.assertLess(result["max_absolute_interval_error_ns"], 1)
        self.assertFalse(result["physical_exposure_timing_verified"])

    def test_perfect_average_with_alternating_intervals_is_not_stable(self):
        result = self.measure([30_000_000, 36_666_667] * 180)
        self.assertLess(abs(result["rate_error_ppm"]), 1)
        self.assertEqual(result["status"], "outside_tolerance")
        self.assertNotIn("average_rate", result["failed_checks"])
        self.assertIn("interval_error_over_1ms", result["failed_checks"])

    def test_uniform_but_slow_sensor_fails_average_and_rolling_rate(self):
        result = self.measure([33_626_000] * 330)
        self.assertEqual(result["interval_stddev_ns"], 0)
        self.assertEqual(result["intervals_over_500us_error"], 0)
        self.assertIn("average_rate", result["failed_checks"])
        self.assertIn("rolling_rate", result["failed_checks"])

    def test_one_outlier_is_not_hidden_by_percentile(self):
        result = self.measure([33_333_333] * 329 + [35_000_000])
        self.assertEqual(result["intervals_over_1ms_error"], 1)
        self.assertIn("interval_error_over_1ms", result["failed_checks"])
        self.assertNotIn("interval_error_over_500us", result["failed_checks"])

    def test_slow_then_fast_windows_cannot_cancel_in_average(self):
        result = self.measure([33_633_333] * 180 + [33_033_334] * 180)
        self.assertLess(abs(result["rate_error_ppm"]), 1)
        self.assertEqual(result["failed_checks"], ["rolling_rate"])

    def test_no_interval_and_short_duration_do_not_pass(self):
        for intervals in ([], [33_333_333] * 10):
            result = self.measure(intervals)
            self.assertEqual(result["status"], "insufficient_duration")
            self.assertEqual(result["failed_checks"], [])
        result = FrameCadence(30).summary()
        self.assertIsNone(result["rate_error_ppm"])
        self.assertIsNone(result["interval_stddev_ns"])
        self.assertEqual(result["interval_count"], 0)

    def test_invalid_clock_is_rejected_without_mutating_accumulator(self):
        cadence = FrameCadence(30)
        cadence.observe(100)
        for timestamp in (100, 99, -1, True, 101.5):
            with self.assertRaises(ValueError):
                cadence.observe(timestamp)
        self.assertEqual(cadence.count, 1)
        self.assertEqual(cadence.last, 100)
        for fps in (0, -30, math.nan, math.inf, 1001):
            with self.assertRaises(ValueError):
                FrameCadence(fps)
