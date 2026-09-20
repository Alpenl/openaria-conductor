"""Bounded, observational frame cadence statistics; never synthesize timestamps."""

from __future__ import annotations

import math
from collections import deque


class FrameCadence:
    """Measure source timestamp spacing independently of nominal container FPS.

    The limits are an explicit engineering acceptance profile, not a claim about
    exposure timing: V4L2 monotonic timestamps may still reflect USB arrival.
    """

    def __init__(self, fps: float):
        if not math.isfinite(fps) or not 0 < fps <= 1000:
            raise ValueError("cadence FPS must be finite and in (0, 1000]")
        self.fps = fps
        self.period = 1e9 / fps
        self.count = 0
        self.first = self.last = None
        self.minimum = self.maximum = None
        self.mean = self.m2 = 0.0
        self.max_error = 0.0
        self.over_500us = self.over_1ms = 0
        self.window_intervals = math.ceil(fps)
        self.window: deque[int] = deque(maxlen=self.window_intervals + 1)
        self.window_min = self.window_max = None

    def observe(self, timestamp_ns: int) -> None:
        if type(timestamp_ns) is not int or timestamp_ns < 0:
            raise ValueError("cadence timestamp must be a nonnegative integer")
        if self.last is not None:
            interval = timestamp_ns - self.last
            if interval <= 0:
                raise ValueError("cadence timestamps must strictly increase")
            self.minimum = interval if self.minimum is None else min(self.minimum, interval)
            self.maximum = interval if self.maximum is None else max(self.maximum, interval)
            delta = interval - self.mean
            self.mean += delta / self.count
            self.m2 += delta * (interval - self.mean)
            error = abs(interval - self.period)
            self.max_error = max(self.max_error, error)
            self.over_500us += error > 500_000
            self.over_1ms += error > 1_000_000
        else:
            self.first = timestamp_ns
        self.last = timestamp_ns
        self.count += 1
        self.window.append(timestamp_ns)
        if len(self.window) == self.window.maxlen:
            rate = self.window_intervals * 1e9 / (self.window[-1] - self.window[0])
            self.window_min = rate if self.window_min is None else min(self.window_min, rate)
            self.window_max = rate if self.window_max is None else max(self.window_max, rate)

    def summary(self) -> dict:
        intervals = self.count - 1
        span = (self.last - self.first) if self.count else 0
        actual = intervals * 1e9 / span if span else None
        error_ppm = (actual / self.fps - 1) * 1e6 if actual is not None else None
        failures = []
        if error_ppm is not None and abs(error_ppm) > 1000:
            failures.append("average_rate")
        if intervals > 0 and self.over_500us / intervals > 0.01:
            failures.append("interval_error_over_500us")
        if self.over_1ms:
            failures.append("interval_error_over_1ms")
        if self.window_min is not None and (
            self.window_min < self.fps * 0.995 or self.window_max > self.fps * 1.005
        ):
            failures.append("rolling_rate")
        sufficient = span >= 10_000_000_000
        return {
            "schema": "openaria.frame-cadence.v1",
            "basis": "recorded_host_monotonic_timestamps",
            "physical_exposure_timing_verified": False,
            "target_fps": self.fps,
            "target_interval_ns": self.period,
            "interval_count": max(0, intervals),
            "min_interval_ns": self.minimum,
            "max_interval_ns": self.maximum,
            "mean_interval_ns": self.mean if intervals > 0 else None,
            "interval_stddev_ns": math.sqrt(max(0.0, self.m2 / intervals))
            if intervals > 0
            else None,
            "rate_error_ppm": error_ppm,
            "max_absolute_interval_error_ns": self.max_error if intervals > 0 else None,
            "intervals_over_500us_error": self.over_500us,
            "intervals_over_1ms_error": self.over_1ms,
            "rolling_window_intervals": self.window_intervals,
            "rolling_fps_min": self.window_min,
            "rolling_fps_max": self.window_max,
            "acceptance_limits": {
                "min_duration_seconds": 10,
                "max_average_rate_error_ppm": 1000,
                "max_fraction_intervals_over_500us_error": 0.01,
                "max_absolute_interval_error_ns": 1_000_000,
                "max_rolling_rate_error_ppm": 5000,
            },
            "status": "outside_tolerance"
            if failures
            else "within_tolerance"
            if sufficient
            else "insufficient_duration",
            "failed_checks": failures,
        }
