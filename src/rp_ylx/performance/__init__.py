"""RDK X5 performance measurement interfaces."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rp_ylx.performance.metrics import MetricsSnapshot, PayloadLease, PerformanceMetrics

if TYPE_CHECKING:
    from rp_ylx.performance.report import PerformanceReportError

__all__ = [
    "PERFORMANCE_REPORT_FORMAT",
    "MetricsSnapshot",
    "PayloadLease",
    "PerformanceMetrics",
    "PerformanceReportError",
    "validate_performance_report",
    "BenchmarkConfig",
    "BenchmarkError",
    "run_benchmark",
]


def __getattr__(name: str) -> Any:
    if name in {"BenchmarkConfig", "BenchmarkError", "run_benchmark"}:
        from rp_ylx.performance import benchmark

        return getattr(benchmark, name)
    if name in {
        "PERFORMANCE_REPORT_FORMAT",
        "PerformanceReportError",
        "validate_performance_report",
    }:
        from rp_ylx.performance import report

        return getattr(report, name)
    raise AttributeError(name)
