"""24-bit 时间展开和设备时钟到主机时钟的证据拟合。"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass

from rp_ylx.imu.models import ImuError
from rp_ylx.imu.protocol import DEVICE_TIMESTAMP_MODULUS


class TimestampUnwrapper:
    def __init__(self, *, modulus: int = DEVICE_TIMESTAMP_MODULUS) -> None:
        if modulus <= 2:
            raise ValueError("时间戳模数必须大于 2")
        self._modulus = modulus
        self._raw: int | None = None
        self._unwrapped: int | None = None

    def update(self, raw: int) -> int:
        if raw < 0 or raw >= self._modulus:
            raise ImuError("invalid_timestamp", "设备时间超出 24-bit 范围")
        if self._raw is None:
            self._raw = raw
            self._unwrapped = raw
            return raw
        delta = (raw - self._raw) % self._modulus
        if delta == 0:
            raise ImuError("timestamp_stalled", "设备时间没有更新", retryable=True)
        if delta > self._modulus // 2:
            raise ImuError("timestamp_regression", "设备时间发生回退", retryable=True)
        assert self._unwrapped is not None
        self._unwrapped += delta
        self._raw = raw
        return self._unwrapped


@dataclass(frozen=True, slots=True)
class SyncEstimate:
    offset_ns: int | None
    residual_ns: int | None
    quality: str
    scale_ns_per_tick: float | None
    estimated_tick_hz: float | None
    drift_ppm: float | None
    points: int


@dataclass(frozen=True, slots=True)
class _Point:
    device_ticks: int
    host_ns: int
    uncertainty_ns: int


class TimeSynchronizer:
    def __init__(
        self,
        *,
        expected_tick_hz: float | None = None,
        minimum_points: int = 3,
        window_points: int = 64,
        good_residual_ns: int = 1_000_000,
        good_drift_ppm: float = 2_000,
    ) -> None:
        if expected_tick_hz is not None and expected_tick_hz <= 0:
            raise ValueError("expected_tick_hz 必须大于零或为 None")
        if minimum_points < 2 or window_points < minimum_points:
            raise ValueError("同步窗口大小无效")
        self._expected_tick_hz = expected_tick_hz
        self._minimum_points = minimum_points
        self._good_residual_ns = good_residual_ns
        self._good_drift_ppm = good_drift_ppm
        self._points: deque[_Point] = deque(maxlen=window_points)

    def add(
        self, device_ticks: int, host_read_start_ns: int, host_read_end_ns: int
    ) -> SyncEstimate:
        if device_ticks < 0 or host_read_start_ns < 0 or host_read_end_ns < host_read_start_ns:
            raise ImuError("invalid_time_evidence", "设备时间或主机读取区间无效")
        host_ns = (host_read_start_ns + host_read_end_ns) // 2
        if self._points and (
            device_ticks <= self._points[-1].device_ticks or host_ns <= self._points[-1].host_ns
        ):
            raise ImuError("non_monotonic_time", "同步证据必须严格前进", retryable=True)
        self._points.append(
            _Point(device_ticks, host_ns, (host_read_end_ns - host_read_start_ns) // 2)
        )
        return self.estimate()

    def estimate(self) -> SyncEstimate:
        count = len(self._points)
        if count < self._minimum_points:
            return SyncEstimate(None, None, "insufficient", None, None, None, count)
        mean_x = sum(point.device_ticks for point in self._points) / count
        mean_y = sum(point.host_ns for point in self._points) / count
        denominator = sum((point.device_ticks - mean_x) ** 2 for point in self._points)
        if denominator == 0:
            return SyncEstimate(None, None, "insufficient", None, None, None, count)
        scale = (
            sum((point.device_ticks - mean_x) * (point.host_ns - mean_y) for point in self._points)
            / denominator
        )
        if not math.isfinite(scale) or scale <= 0:
            raise ImuError("invalid_clock_fit", "设备时钟拟合斜率无效")
        offset = mean_y - scale * mean_x
        fit_residual = max(
            abs(point.host_ns - (offset + scale * point.device_ticks)) for point in self._points
        )
        uncertainty = max(point.uncertainty_ns for point in self._points)
        residual = math.ceil(fit_residual + uncertainty)
        tick_hz = 1_000_000_000 / scale
        drift_ppm = None
        if self._expected_tick_hz is not None:
            drift_ppm = (tick_hz / self._expected_tick_hz - 1) * 1_000_000
        quality = "good"
        if residual > self._good_residual_ns or (
            drift_ppm is not None and abs(drift_ppm) > self._good_drift_ppm
        ):
            quality = "degraded"
        return SyncEstimate(
            offset_ns=round(offset),
            residual_ns=residual,
            quality=quality,
            scale_ns_per_tick=scale,
            estimated_tick_hz=tick_hz,
            drift_ppm=drift_ppm,
            points=count,
        )
