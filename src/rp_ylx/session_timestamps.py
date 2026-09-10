"""Per-frame timestamp alignment for sealed Device Session recordings."""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path, PurePosixPath
from statistics import fmean, median, pstdev
from typing import Any

from rp_ylx.validation import PublicValidationError, validate_public_session

MAX_NDJSON_LINE_BYTES = 1024 * 1024

TIMESTAMP_CSV_FIELDS = (
    "frame_index",
    "source_sequence",
    "left_eye_host_monotonic_ns",
    "right_eye_host_monotonic_ns",
    "nearest_imu_sample_number",
    "nearest_imu_host_monotonic_ns",
    "nearest_imu_estimated_monotonic_ns",
    "nearest_imu_delta_ms",
    "imu_device_timestamp_raw",
    "imu_device_ticks",
    "imu_packet_sequence",
    "imu_sample_index",
    "imu_sync_quality",
)


class SessionTimestampError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SessionTimestampError("session_timestamps_invalid", f"{label} 不是对象")
    return value


def _integer(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise SessionTimestampError("session_timestamps_invalid", f"{label} 不是非负整数")
    return value


def _number(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SessionTimestampError("session_timestamps_invalid", f"{label} 不是数字")
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise SessionTimestampError("session_timestamps_invalid", f"{label} 必须为正数")
    return result


def _safe_relative(value: object, label: str) -> PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise SessionTimestampError("session_timestamps_invalid", f"{label} 路径无效")
    relative = PurePosixPath(value)
    if (
        relative.is_absolute()
        or relative.as_posix() != value
        or any(part in {"", ".", ".."} for part in relative.parts)
    ):
        raise SessionTimestampError("session_timestamps_invalid", f"{label} 路径不安全")
    return relative


def _artifact_path(root: Path, descriptor: object, *, role: str, label: str) -> Path:
    artifact = _mapping(descriptor, label)
    if artifact.get("role") != role or artifact.get("media_type") != "application/x-ndjson":
        raise SessionTimestampError("session_timestamps_invalid", f"{label} artifact 角色无效")
    relative = _safe_relative(artifact.get("path"), label)
    path = root.joinpath(*relative.parts)
    if not path.is_file() or path.is_symlink():
        raise SessionTimestampError("session_timestamps_invalid", f"{label} artifact 不可安全读取")
    return path


def _read_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("rb") as stream:
        line_number = 0
        while True:
            line = stream.readline(MAX_NDJSON_LINE_BYTES + 1)
            if not line:
                break
            line_number += 1
            if len(line) > MAX_NDJSON_LINE_BYTES or not line.endswith(b"\n"):
                raise SessionTimestampError(
                    "session_timestamps_invalid",
                    f"{label}:{line_number} 行过长或缺少换行",
                )
            try:
                value = json.loads(line)
            except (UnicodeError, json.JSONDecodeError) as error:
                raise SessionTimestampError(
                    "session_timestamps_invalid",
                    f"{label}:{line_number} JSON 无效：{error}",
                ) from error
            rows.append(_mapping(value, f"{label}:{line_number}"))
    if not rows:
        raise SessionTimestampError("session_timestamps_empty", f"{label} 为空")
    return rows


def _load_frames(root: Path, manifest: dict[str, Any]) -> list[dict[str, int]]:
    frames = _mapping(manifest.get("frames"), "frames")
    path = _artifact_path(root, frames.get("artifact"), role="frames.index", label="frames")
    records = _read_jsonl(path, "frames")
    expected_count = _integer(frames.get("count"), "frames.count")
    if len(records) != expected_count:
        raise SessionTimestampError("session_timestamps_invalid", "frames.count 与文件行数不一致")
    normalized: list[dict[str, int]] = []
    previous_host = -1
    for index, record in enumerate(records):
        expected_keys = {
            "schema",
            "session_id",
            "frame",
            "source_sequence",
            "host_monotonic_ns",
            "segment_index",
            "segment_frame",
        }
        if set(record) != expected_keys or record.get("schema") != "ylx.frame-index.v1":
            raise SessionTimestampError(
                "session_timestamps_invalid",
                f"frames:{index + 1} 不是闭合帧索引记录",
            )
        frame = _integer(record["frame"], f"frames[{index}].frame")
        source_sequence = _integer(record["source_sequence"], f"frames[{index}].source_sequence")
        host = _integer(record["host_monotonic_ns"], f"frames[{index}].host_monotonic_ns")
        if frame != index or host <= previous_host:
            raise SessionTimestampError(
                "session_timestamps_invalid",
                f"frames:{index + 1} 帧号或主机时间不递增",
            )
        normalized.append(
            {
                "frame_index": frame,
                "source_sequence": source_sequence,
                "host_monotonic_ns": host,
            }
        )
        previous_host = host
    return normalized


def _load_imu(root: Path, manifest: dict[str, Any]) -> list[dict[str, Any]]:
    imu = _mapping(manifest.get("imu"), "imu")
    path = _artifact_path(root, imu.get("artifact"), role="imu.samples", label="imu")
    records = _read_jsonl(path, "imu")
    expected_count = _integer(imu.get("sample_count"), "imu.sample_count")
    if len(records) != expected_count:
        raise SessionTimestampError(
            "session_timestamps_invalid",
            "imu.sample_count 与文件行数不一致",
        )
    normalized: list[dict[str, Any]] = []
    previous_sequence = -1
    for index, record in enumerate(records):
        expected_keys = {
            "format",
            "session_id",
            "sequence",
            "packet_sequence",
            "sample_index",
            "device_timestamp_raw",
            "device_ticks",
            "host_read_start_ns",
            "host_read_end_ns",
            "host_monotonic_ns",
            "raw",
            "sync",
        }
        if set(record) != expected_keys or record.get("format") != "ylx.imu.v0":
            raise SessionTimestampError(
                "session_timestamps_invalid",
                f"imu:{index + 1} 不是闭合 IMU 记录",
            )
        sequence = _integer(record["sequence"], f"imu[{index}].sequence")
        packet_sequence = _integer(record["packet_sequence"], f"imu[{index}].packet_sequence")
        sample_index = _integer(record["sample_index"], f"imu[{index}].sample_index")
        device_timestamp_raw = _integer(
            record["device_timestamp_raw"], f"imu[{index}].device_timestamp_raw"
        )
        device_ticks = _integer(record["device_ticks"], f"imu[{index}].device_ticks")
        host_start = _integer(record["host_read_start_ns"], f"imu[{index}].host_read_start_ns")
        host_end = _integer(record["host_read_end_ns"], f"imu[{index}].host_read_end_ns")
        host = _integer(record["host_monotonic_ns"], f"imu[{index}].host_monotonic_ns")
        if sequence != previous_sequence + 1 or host_start > host or host > host_end:
            raise SessionTimestampError(
                "session_timestamps_invalid",
                f"imu:{index + 1} 序号或主机读取区间无效",
            )
        sync = _mapping(record["sync"], f"imu[{index}].sync")
        quality = sync.get("quality")
        if quality not in {"insufficient", "degraded", "good"}:
            raise SessionTimestampError(
                "session_timestamps_invalid",
                f"imu:{index + 1} sync quality 无效",
            )
        normalized.append(
            {
                "sample_number": index,
                "sequence": sequence,
                "packet_sequence": packet_sequence,
                "sample_index": sample_index,
                "device_timestamp_raw": device_timestamp_raw,
                "device_ticks": device_ticks,
                "host_monotonic_ns": host,
                "sync_quality": quality,
            }
        )
        previous_sequence = sequence
    return normalized


def _packets(samples: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    packets: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_packet: int | None = None
    for sample in samples:
        packet = int(sample["packet_sequence"])
        if current_packet is None or packet != current_packet:
            if current:
                packets.append(current)
            current = []
            current_packet = packet
        current.append(sample)
    if current:
        packets.append(current)
    previous_packet = -1
    for packet in packets:
        packet_sequence = int(packet[0]["packet_sequence"])
        if packet_sequence != previous_packet + 1:
            raise SessionTimestampError("session_timestamps_invalid", "IMU packet_sequence 不连续")
        indices = [int(sample["sample_index"]) for sample in packet]
        if indices != list(range(len(packet))):
            raise SessionTimestampError(
                "session_timestamps_invalid",
                "IMU packet 内 sample_index 不连续",
            )
        host = int(packet[0]["host_monotonic_ns"])
        if any(int(sample["host_monotonic_ns"]) != host for sample in packet):
            raise SessionTimestampError("session_timestamps_invalid", "IMU packet 内主机时间不一致")
        previous_packet = packet_sequence
    return packets


def _rate_from_times(times_ns: list[int], events_per_tick: float = 1.0) -> float | None:
    if len(times_ns) < 2:
        return None
    span = times_ns[-1] - times_ns[0]
    if span <= 0:
        return None
    return (len(times_ns) - 1) * events_per_tick * 1_000_000_000.0 / span


def _estimated_imu_times(
    packets: list[list[dict[str, Any]]],
    record_rate_hz: float | None,
) -> list[float]:
    if record_rate_hz is None:
        return [float(sample["host_monotonic_ns"]) for packet in packets for sample in packet]
    global_period_ns = 1_000_000_000.0 / record_rate_hz
    estimated: list[float] = []
    previous_packet_host: int | None = None
    for packet in packets:
        host = int(packet[0]["host_monotonic_ns"])
        period_ns = global_period_ns
        if previous_packet_host is not None:
            period_ns = min(period_ns, (host - previous_packet_host) / len(packet))
        for sample in packet:
            offset = len(packet) - 1 - int(sample["sample_index"])
            estimated.append(host - offset * period_ns)
        previous_packet_host = host
    if any(later <= earlier for earlier, later in zip(estimated, estimated[1:], strict=False)):
        return [float(sample["host_monotonic_ns"]) for packet in packets for sample in packet]
    return estimated


def _nearest_index(values: list[float], target: int) -> int:
    lo = 0
    hi = len(values)
    while lo < hi:
        mid = (lo + hi) // 2
        if values[mid] < target:
            lo = mid + 1
        else:
            hi = mid
    if lo <= 0:
        return 0
    if lo >= len(values):
        return len(values) - 1
    before = lo - 1
    after = lo
    return before if abs(values[before] - target) <= abs(values[after] - target) else after


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, math.ceil(fraction * len(ordered)) - 1)
    return ordered[max(index, 0)]


def _frame_interval_summary(
    frames: list[dict[str, int]],
    manifest: dict[str, Any],
) -> dict[str, float | int | None]:
    camera = _mapping(manifest.get("camera"), "camera")
    sensor_fps = _number(camera.get("sensor_fps"), "camera.sensor_fps")
    frame_decimation = _integer(camera.get("frame_decimation"), "camera.frame_decimation")
    nominal_fps = _number(
        camera.get("nominal_fps", sensor_fps / frame_decimation),
        "camera.nominal_fps",
    )
    intervals = [
        later["host_monotonic_ns"] - earlier["host_monotonic_ns"]
        for earlier, later in zip(frames, frames[1:], strict=False)
    ]
    observed_span_ns = (
        frames[-1]["host_monotonic_ns"] - frames[0]["host_monotonic_ns"] if len(frames) >= 2 else 0
    )
    measured_fps = _rate_from_times([frame["host_monotonic_ns"] for frame in frames])
    expected_interval_ns = 1_000_000_000.0 / nominal_fps
    jitter_ms = [abs(interval - expected_interval_ns) / 1_000_000.0 for interval in intervals]
    interval_ms = [interval / 1_000_000.0 for interval in intervals]
    expected_span_ns = (len(frames) - 1) * expected_interval_ns if len(frames) >= 2 else None
    drift_ms = (
        None if expected_span_ns is None else (observed_span_ns - expected_span_ns) / 1_000_000.0
    )
    return {
        "nominal_fps": nominal_fps,
        "measured_fps": measured_fps,
        "interval_count": len(intervals),
        "mean_interval_ms": fmean(interval_ms) if interval_ms else None,
        "interval_stddev_ms": pstdev(interval_ms) if len(interval_ms) >= 2 else 0.0,
        "p95_interval_error_ms": _percentile(jitter_ms, 0.95),
        "cumulative_drift_ms": drift_ms,
    }


def _alignment_rows(
    frames: list[dict[str, int]],
    samples: list[dict[str, Any]],
    imu_times: list[float],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for frame in frames:
        frame_time = frame["host_monotonic_ns"]
        imu_index = _nearest_index(imu_times, frame_time)
        imu = samples[imu_index]
        imu_time = imu_times[imu_index]
        rows.append(
            {
                "frame_index": frame["frame_index"],
                "source_sequence": frame["source_sequence"],
                "left_eye_host_monotonic_ns": frame_time,
                "right_eye_host_monotonic_ns": frame_time,
                "nearest_imu_sample_number": imu["sample_number"],
                "nearest_imu_host_monotonic_ns": imu["host_monotonic_ns"],
                "nearest_imu_estimated_monotonic_ns": int(round(imu_time)),
                "nearest_imu_delta_ms": (imu_time - frame_time) / 1_000_000.0,
                "imu_device_timestamp_raw": imu["device_timestamp_raw"],
                "imu_device_ticks": imu["device_ticks"],
                "imu_packet_sequence": imu["packet_sequence"],
                "imu_sample_index": imu["sample_index"],
                "imu_sync_quality": imu["sync_quality"],
            }
        )
    return rows


def _alignment_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    deltas = [abs(float(row["nearest_imu_delta_ms"])) for row in rows]
    return {
        "camera_timestamp_source": "shared_sbs_frame",
        "camera_time_base": "host_monotonic",
        "left_right_delta_ms": 0.0,
        "imu_matching": "nearest_estimated_sample",
        "rows": len(rows),
        "nearest_imu_delta_p50_ms": _percentile(deltas, 0.50),
        "nearest_imu_delta_p95_ms": _percentile(deltas, 0.95),
        "nearest_imu_delta_max_ms": max(deltas, default=None),
    }


def _imu_summary(
    samples: list[dict[str, Any]],
    packets: list[list[dict[str, Any]]],
) -> dict[str, float | int | None]:
    packet_hosts = [int(packet[0]["host_monotonic_ns"]) for packet in packets]
    samples_per_packet = median([len(packet) for packet in packets]) if packets else 0
    packet_rate_hz = _rate_from_times(packet_hosts)
    record_rate_hz = None if packet_rate_hz is None else packet_rate_hz * float(samples_per_packet)
    return {
        "samples": len(samples),
        "packets": len(packets),
        "median_samples_per_packet": samples_per_packet,
        "packet_rate_hz": packet_rate_hz,
        "record_rate_hz": record_rate_hz,
    }


def build_session_timestamp_report(
    directory: str | Path,
    *,
    output: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(directory)
    try:
        manifest = validate_public_session(root)
    except PublicValidationError as error:
        raise SessionTimestampError(error.code, error.message) from error
    if manifest.get("schema") not in {
        "ylx.device-session.v1",
        "ylx.device-session.v2",
        "ylx.device-session.v3",
    }:
        raise SessionTimestampError("unsupported_session", "只支持 Device Session v1/v2/v3")
    frames = _load_frames(root, manifest)
    samples = _load_imu(root, manifest)
    packets = _packets(samples)
    if not samples or not packets:
        raise SessionTimestampError("session_timestamps_empty", "会话没有可对齐的 IMU 样本")
    imu = _imu_summary(samples, packets)
    imu_times = _estimated_imu_times(packets, imu["record_rate_hz"])
    rows = _alignment_rows(frames, samples, imu_times)
    report: dict[str, Any] = {
        "ok": True,
        "schema": "openaria.session-timestamps.v1",
        "source_schema": manifest["schema"],
        "session_id": manifest["session_id"],
        "capture_mode": manifest.get("capture_mode"),
        "frames": len(frames),
        "imu_samples": len(samples),
        "frame_interval": _frame_interval_summary(frames, manifest),
        "imu": imu,
        "timestamp_alignment": _alignment_summary(rows),
    }
    if output is not None:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=TIMESTAMP_CSV_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        report["csv"] = {
            "path": str(path),
            "rows": len(rows),
            "columns": list(TIMESTAMP_CSV_FIELDS),
        }
    return report
