"""严格验证可比较的 RDK X5 性能报告。"""

from __future__ import annotations

import json
import math
from datetime import datetime
from importlib.resources import files
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from rp_ylx.hardware.target import RDK_X5_BOARD_ID, YLX_2UQ2_CAMERA_ID

PERFORMANCE_REPORT_FORMAT = "ylx.performance-report.v0"


class PerformanceReportError(ValueError):
    """携带稳定错误码和位置的性能报告验证失败。"""

    def __init__(self, code: str, location: str, message: str) -> None:
        self.code = code
        self.location = location
        self.message = message
        super().__init__(f"{code} {location}: {message}")


def _schema() -> dict[str, Any]:
    resource = files("rp_ylx.schemas").joinpath("ylx-performance-report-v0.schema.json")
    return json.loads(resource.read_text(encoding="utf-8"))


_VALIDATOR = Draft202012Validator(_schema(), format_checker=FormatChecker())


def _location(parts: object) -> str:
    path = list(parts)  # type: ignore[arg-type]
    if not path:
        return "$"
    return "$" + "".join(f"[{part}]" if isinstance(part, int) else f".{part}" for part in path)


def _fail(code: str, location: str, message: str) -> None:
    raise PerformanceReportError(code, location, message)


def _unique_names(records: list[dict[str, Any]], location: str) -> None:
    names: set[str] = set()
    for index, record in enumerate(records):
        name = record["name"]
        if name in names:
            _fail("duplicate_name", f"{location}[{index}].name", f"重复名称：{name}")
        names.add(name)


def validate_performance_report(value: object) -> dict[str, Any]:
    """返回经过严格结构和跨字段语义验证的性能报告。"""

    errors = sorted(_VALIDATOR.iter_errors(value), key=lambda error: list(error.absolute_path))
    if errors:
        error = errors[0]
        _fail("schema_violation", _location(error.absolute_path), error.message)
    if not isinstance(value, dict):  # 已由 schema 保证，保留类型收窄。
        _fail("schema_violation", "$", "必须是对象")

    report = value
    observed_at = report["observed_at"]
    if not observed_at.endswith(("Z", "+00:00")):
        _fail("invalid_timestamp", "$.observed_at", "必须是带 UTC 时区的 RFC 3339 时间")
    try:
        datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    except ValueError:
        _fail("invalid_timestamp", "$.observed_at", "不是有效 RFC 3339 时间")

    evidence_kind = report["environment"]["evidence_kind"]
    target = report["environment"]["target"]
    exact_target = (
        target["board"] == RDK_X5_BOARD_ID
        and target["camera"] == YLX_2UQ2_CAMERA_ID
        and target["supported"] is True
    )
    if evidence_kind == "hardware" and not exact_target:
        _fail(
            "target_mismatch",
            "$.environment.target",
            "hardware 证据必须来自唯一支持的 RDK X5 + YLX 2UQ2",
        )
    if evidence_kind != "hardware" and target["supported"] is True:
        _fail(
            "false_hardware_claim",
            "$.environment.target.supported",
            "非 hardware 证据不得声明目标硬件通过",
        )

    native = report["native"]
    native_identity = native["module_version"] is not None and native["abi"] is not None
    if native["adapter"] == "rust" and not (native["module_available"] and native_identity):
        _fail(
            "native_unavailable",
            "$.native",
            "Rust adapter 必须绑定可用的原生模块、版本和 ABI",
        )
    if native["adapter"] == "python" and native["module_available"]:
        _fail(
            "adapter_mismatch",
            "$.native.adapter",
            "Python adapter 报告不能声明正在使用原生模块",
        )
    if not native["module_available"] and native_identity:
        _fail(
            "native_identity_mismatch",
            "$.native",
            "不可用的原生模块不能携带版本和 ABI",
        )

    _unique_names(report["stages"], "$.stages")
    for index, stage in enumerate(report["stages"]):
        if stage["p50_ns"] > stage["p95_ns"]:
            _fail(
                "invalid_percentile",
                f"$.stages[{index}].p95_ns",
                "p95 不能小于 p50",
            )
        if stage["p95_ns"] > stage["total_ns"]:
            _fail(
                "invalid_total",
                f"$.stages[{index}].total_ns",
                "总耗时不能小于 p95",
            )

    _unique_names(report["copies"], "$.copies")
    for index, copied in enumerate(report["copies"]):
        if (copied["count"] == 0) != (copied["bytes_total"] == 0):
            _fail(
                "invalid_copy_total",
                f"$.copies[{index}]",
                "复制次数和复制字节必须同时为零或同时为正",
            )
    queue = report["queue"]
    if queue["peak_depth"] > queue["capacity"]:
        _fail("queue_overflow", "$.queue.peak_depth", "峰值深度不能超过固定容量")

    workload = report["workload"]
    if workload["frames_output"] > workload["frames_input"]:
        _fail("invalid_frame_count", "$.workload.frames_output", "输出帧数不能超过输入帧数")
    expected_fps = workload["frames_output"] * 1_000_000_000 / workload["duration_ns"]
    actual_fps = report["result"]["effective_fps"]
    if not math.isclose(actual_fps, expected_fps, rel_tol=1e-6, abs_tol=1e-6):
        _fail(
            "inconsistent_rate",
            "$.result.effective_fps",
            "有效帧率必须由输出帧数和持续时间计算",
        )

    loss = report["loss"]
    if loss["application_drop"] < queue["rejected"]:
        _fail(
            "unaccounted_rejection",
            "$.loss.application_drop",
            "应用丢帧不能少于队列拒绝数",
        )
    return report
