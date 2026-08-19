"""Bounded, opt-in measurements for the camera data plane."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Literal

from rp_ylx.native import (
    NativeModuleError,
    NativePerformanceMetrics,
    create_native_performance_metrics,
)

LossKind = Literal["source_gap", "queue_rejected", "write_failure", "unknown_gap"]
_LOSS_KINDS = {"source_gap", "queue_rejected", "write_failure", "unknown_gap"}


@dataclass(frozen=True, slots=True)
class MetricsSnapshot:
    stages: tuple[dict[str, int | str], ...]
    copies: tuple[dict[str, int | str], ...]
    payloads: tuple[dict[str, int | str], ...]
    queue: dict[str, int]
    loss: dict[str, int]


class PayloadLease:
    """Account for one payload owner without exposing that owner to callers."""

    __slots__ = ("_bytes", "_metrics", "_name", "_released")

    def __init__(self, metrics: PerformanceMetrics, name: str, size: int) -> None:
        self._metrics = metrics
        self._name = name
        self._bytes = size
        self._released = False
        metrics._change_payload(name, 1, size)

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self._metrics._change_payload(self._name, -1, -self._bytes)

    def __enter__(self) -> PayloadLease:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.release()

    def __del__(self) -> None:
        self.release()


class PerformanceMetrics:
    """Thread-safe metrics backed by fixed-size logarithmic histograms."""

    def __init__(self) -> None:
        try:
            self._native: NativePerformanceMetrics | None = create_native_performance_metrics()
        except NativeModuleError:
            self._native = None
        self._lock = threading.Lock()
        self._stages: dict[str, list[int]] = {}
        self._copies: dict[str, list[int]] = {}
        self._payloads: dict[str, list[int]] = {}
        self._queue_capacity = 1
        self._queue_peak = 0
        self._queue_rejected = 0
        self._loss = {
            "source_gap": 0,
            "queue_rejected": 0,
            "write_failure": 0,
            "unknown_gap": 0,
        }

    @staticmethod
    def start() -> int:
        return time.perf_counter_ns()

    def finish(self, name: str, started_ns: int) -> None:
        self.record_stage(name, max(0, time.perf_counter_ns() - started_ns))

    def record_stage(self, name: str, elapsed_ns: int) -> None:
        if elapsed_ns < 0:
            raise ValueError("stage elapsed_ns cannot be negative")
        if self._native is not None:
            self._native.record_stage(name, elapsed_ns)
            return
        bucket = min(63, elapsed_ns.bit_length())
        with self._lock:
            value = self._stages.setdefault(name, [0] * 66)
            value[bucket] += 1
            value[64] += elapsed_ns
            value[65] += 1

    def record_copy(self, name: str, size: int, *, count: int = 1) -> None:
        if size < 0 or count < 0:
            raise ValueError("copy size and count cannot be negative")
        if count == 0:
            return
        if self._native is not None:
            self._native.record_copy(name, size, count)
            return
        with self._lock:
            copied = self._copies.setdefault(name, [0, 0])
            copied[0] += count
            copied[1] += size

    def retain_payload(self, name: str, size: int) -> PayloadLease:
        if size < 0:
            raise ValueError("payload size cannot be negative")
        return PayloadLease(self, name, size)

    def _change_payload(self, name: str, count_delta: int, bytes_delta: int) -> None:
        if self._native is not None:
            self._native.change_payload(name, count_delta, bytes_delta)
            return
        with self._lock:
            payload = self._payloads.setdefault(name, [0, 0, 0, 0, 0])
            payload[0] += count_delta
            payload[1] += bytes_delta
            if count_delta > 0:
                payload[2] += count_delta
            payload[3] = max(payload[3], payload[0])
            payload[4] = max(payload[4], payload[1])
            if payload[0] < 0 or payload[1] < 0:
                raise RuntimeError(f"payload lifetime underflow: {name}")

    def observe_queue(
        self,
        *,
        depth: int,
        capacity: int,
        rejected: int = 0,
        peak_depth: int | None = None,
    ) -> None:
        peak = depth if peak_depth is None else peak_depth
        if (
            capacity <= 0
            or depth < 0
            or depth > capacity
            or peak < depth
            or peak > capacity
            or rejected < 0
        ):
            raise ValueError("invalid bounded queue observation")
        if self._native is not None:
            self._native.observe_queue(depth, capacity, rejected, peak)
            return
        with self._lock:
            self._queue_capacity = max(self._queue_capacity, capacity)
            self._queue_peak = max(self._queue_peak, peak)
            self._queue_rejected += rejected

    def record_loss(self, kind: LossKind, count: int = 1) -> None:
        if kind not in _LOSS_KINDS or count < 0:
            raise ValueError("invalid loss observation")
        if self._native is not None:
            self._native.record_loss(kind, count)
            return
        with self._lock:
            self._loss[kind] += count

    @staticmethod
    def _percentile(histogram: list[int], samples: int, percentile: float) -> int:
        threshold = max(1, int(samples * percentile + 0.999999))
        seen = 0
        for bucket, count in enumerate(histogram[:64]):
            seen += count
            if seen >= threshold:
                return 0 if bucket == 0 else 1 << (bucket - 1)
        return 0

    def snapshot(self) -> MetricsSnapshot:
        if self._native is not None:
            raw = self._native.snapshot()
            return MetricsSnapshot(
                tuple(raw["stages"]),  # type: ignore[arg-type]
                tuple(raw["copies"]),  # type: ignore[arg-type]
                tuple(raw["payloads"]),  # type: ignore[arg-type]
                dict(raw["queue"]),  # type: ignore[arg-type]
                dict(raw["loss"]),  # type: ignore[arg-type]
            )
        with self._lock:
            stages = []
            for name, raw in sorted(self._stages.items()):
                samples = raw[65]
                stages.append(
                    {
                        "name": name,
                        "samples": samples,
                        "p50_ns": self._percentile(raw, samples, 0.50),
                        "p95_ns": self._percentile(raw, samples, 0.95),
                        "total_ns": raw[64],
                    }
                )
            copies = tuple(
                {"name": name, "count": values[0], "bytes_total": values[1]}
                for name, values in sorted(self._copies.items())
            )
            payloads = tuple(
                {
                    "name": name,
                    "acquired": values[2],
                    "live": values[0],
                    "live_bytes": values[1],
                    "peak_live": values[3],
                    "peak_bytes": values[4],
                }
                for name, values in sorted(self._payloads.items())
            )
            return MetricsSnapshot(
                tuple(stages),
                copies,
                payloads,
                {
                    "capacity": self._queue_capacity,
                    "peak_depth": self._queue_peak,
                    "rejected": self._queue_rejected,
                },
                dict(self._loss),
            )
