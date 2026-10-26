"""Per-frame timestamp alignment for sealed Device Session recordings."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import tempfile
from collections import Counter
from collections.abc import Iterator
from contextlib import ExitStack, closing
from pathlib import Path, PurePosixPath
from statistics import fmean, pstdev
from typing import Any, BinaryIO, TextIO

from zstandard import ZstdError

from rp_ylx.api.downloads import iter_device_session_v1_artifacts
from rp_ylx.frame_index import valid_frame_record
from rp_ylx.recording.storage import open_metadata
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
    if artifact.get("role") != role or artifact.get("media_type") not in {
        "application/x-ndjson",
        "application/zstd",
    }:
        raise SessionTimestampError("session_timestamps_invalid", f"{label} artifact 角色无效")
    relative = _safe_relative(artifact.get("path"), label)
    path = root.joinpath(*relative.parts)
    if not path.is_file() or path.is_symlink():
        raise SessionTimestampError("session_timestamps_invalid", f"{label} artifact 不可安全读取")
    return path


def _iter_jsonl(
    path: Path,
    label: str,
    descriptor: dict[str, Any] | None = None,
) -> Iterator[dict[str, Any]]:
    digest = hashlib.sha256()
    size = 0
    line_number = 0
    try:
        with open_metadata(path) as stream:
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
                digest.update(line)
                size += len(line)
                yield _mapping(value, f"{label}:{line_number}")
    except (OSError, ZstdError) as error:
        raise SessionTimestampError(
            "session_timestamps_invalid", f"{label} 无法完整读取：{error}"
        ) from error
    if not line_number:
        raise SessionTimestampError("session_timestamps_empty", f"{label} 为空")
    if descriptor is not None:
        encoding = descriptor.get("storage_encoding")
        expected = (
            (encoding["uncompressed_bytes"], encoding["uncompressed_sha256"])
            if encoding
            else (descriptor["bytes"], descriptor["sha256"])
        )
        if (size, digest.hexdigest()) != expected:
            raise SessionTimestampError("session_timestamps_invalid", f"{label} 读取校验不一致")


def _read_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    # Compatibility helper; export itself never materializes the IMU journal.
    return list(_iter_jsonl(path, label))


def _load_frames(root: Path, manifest: dict[str, Any]) -> list[dict[str, int]]:
    frames = _mapping(manifest.get("frames"), "frames")
    path = _artifact_path(root, frames.get("artifact"), role="frames.index", label="frames")
    records = _iter_jsonl(path, "frames", frames["artifact"])
    expected_count = _integer(frames.get("count"), "frames.count")
    normalized: list[dict[str, int]] = []
    previous_host = -1
    with closing(records):
        for index, record in enumerate(records):
            if not valid_frame_record(record) or record["session_id"] != manifest["session_id"]:
                raise SessionTimestampError(
                    "session_timestamps_invalid",
                    f"frames:{index + 1} 不是闭合帧索引记录",
                )
            frame = _integer(record["frame"], f"frames[{index}].frame")
            source_sequence = _integer(
                record["source_sequence"], f"frames[{index}].source_sequence"
            )
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
    if len(normalized) != expected_count:
        raise SessionTimestampError("session_timestamps_invalid", "frames.count 与文件行数不一致")
    return normalized


def _iter_imu(root: Path, manifest: dict[str, Any]) -> Iterator[dict[str, Any]]:
    imu = _mapping(manifest.get("imu"), "imu")
    path = _artifact_path(root, imu.get("artifact"), role="imu.samples", label="imu")
    records = _iter_jsonl(path, "imu", imu["artifact"])
    expected_count = _integer(imu.get("sample_count"), "imu.sample_count")
    count = 0
    previous_sequence = -1
    with closing(records):
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
            if (
                set(record) != expected_keys
                or record.get("format") != "ylx.imu.v0"
                or record["session_id"] != manifest["session_id"]
            ):
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
            yield {
                "sample_number": index,
                "sequence": sequence,
                "packet_sequence": packet_sequence,
                "sample_index": sample_index,
                "device_timestamp_raw": device_timestamp_raw,
                "device_ticks": device_ticks,
                "host_monotonic_ns": host,
                "sync_quality": quality,
            }
            previous_sequence = sequence
            count += 1
    if count != expected_count:
        raise SessionTimestampError(
            "session_timestamps_invalid", "imu.sample_count 与文件行数不一致"
        )


def _packet_summary(root: Path, manifest: dict[str, Any], spool: BinaryIO) -> dict[str, Any]:
    """Store only [host, count] per packet on disk, never whole sample dictionaries."""
    sizes: Counter[int] = Counter()
    packet = -1
    host = first_host = count = total = 0

    def finish_packet() -> None:
        data = (json.dumps([host, count]) + "\n").encode("ascii")
        if spool.write(data) != len(data):
            raise OSError("short timestamp packet spool write")
        sizes[count] += 1

    with closing(_iter_imu(root, manifest)) as samples:
        for sample in samples:
            sequence = sample["packet_sequence"]
            if sequence != packet:
                if sequence != packet + 1:
                    raise SessionTimestampError(
                        "session_timestamps_invalid", "IMU packet_sequence 不连续"
                    )
                if packet >= 0:
                    finish_packet()
                    if sample["host_monotonic_ns"] < host:
                        raise SessionTimestampError(
                            "session_timestamps_invalid", "IMU packet 主机时间倒退"
                        )
                else:
                    first_host = sample["host_monotonic_ns"]
                host, count, packet = sample["host_monotonic_ns"], 0, sequence
            if sample["sample_index"] != count or sample["host_monotonic_ns"] != host:
                raise SessionTimestampError(
                    "session_timestamps_invalid", "IMU packet 内索引或主机时间无效"
                )
            count += 1
            total += 1
    finish_packet()
    packets = packet + 1
    # The counter has at most O(sqrt(samples)) distinct positive packet sizes.
    low, high = (packets - 1) // 2, packets // 2
    cumulative = 0
    middle = []
    for size, frequency in sorted(sizes.items()):
        for position in (low, high):
            if cumulative <= position < cumulative + frequency:
                middle.append(size)
        cumulative += frequency
    per_packet = middle[0] if packets % 2 else sum(middle) / 2
    rate = None if packets < 2 or host <= first_host else (packets - 1) * 1e9 / (host - first_host)
    return {
        "samples": total,
        "packets": packets,
        "median_samples_per_packet": per_packet,
        "packet_rate_hz": rate,
        "record_rate_hz": None if rate is None else rate * float(per_packet),
    }


class _UseHostTimes(Exception):
    """A late non-increasing estimate requires restarting the entire alignment."""


def _timed_samples(
    root: Path,
    manifest: dict[str, Any],
    spool: BinaryIO,
    rate: float | None,
    *,
    raw: bool = False,
) -> Iterator[tuple[dict[str, Any], float]]:
    spool.seek(0)
    packet = -1
    host = count = seen = 0
    previous_host = None
    previous_time = None
    period = 0.0
    with closing(_iter_imu(root, manifest)) as samples:
        for sample in samples:
            if sample["packet_sequence"] != packet:
                if seen != count or sample["packet_sequence"] != packet + 1:
                    raise SessionTimestampError(
                        "session_timestamps_invalid", "IMU packet 在读取间改变"
                    )
                descriptor = spool.readline()
                if not descriptor:
                    raise SessionTimestampError(
                        "session_timestamps_invalid", "IMU packet 在读取间增加"
                    )
                host, count = json.loads(descriptor)
                packet += 1
                seen = 0
                if rate is not None and not raw:
                    period = 1e9 / rate
                    if previous_host is not None:
                        period = min(period, (host - previous_host) / count)
                previous_host = host
            if (
                sample["sample_index"] != seen
                or sample["host_monotonic_ns"] != host
                or seen >= count
            ):
                raise SessionTimestampError("session_timestamps_invalid", "IMU packet 在读取间改变")
            when = float(host) if raw or rate is None else host - (count - 1 - seen) * period
            if not raw and rate is not None and previous_time is not None and when <= previous_time:
                raise _UseHostTimes
            yield sample, when
            seen += 1
            previous_time = when
    if seen != count or spool.read(1):
        raise SessionTimestampError("session_timestamps_invalid", "IMU packet 在读取间减少")


def _rate_from_times(times_ns: list[int], events_per_tick: float = 1.0) -> float | None:
    if len(times_ns) < 2:
        return None
    span = times_ns[-1] - times_ns[0]
    if span <= 0:
        return None
    return (len(times_ns) - 1) * events_per_tick * 1_000_000_000.0 / span


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
    samples: Iterator[tuple[dict[str, Any], float]],
) -> Iterator[dict[str, Any]]:
    before = None
    after = next(samples, None)
    if after is None:
        raise SessionTimestampError("session_timestamps_empty", "会话没有可对齐的 IMU 样本")
    for frame in frames:
        frame_time = frame["host_monotonic_ns"]
        while after is not None and after[1] < frame_time:
            before, after = after, next(samples, None)
        if before is None:
            chosen = after
        elif after is None or abs(before[1] - frame_time) <= abs(after[1] - frame_time):
            chosen = before
        else:
            chosen = after
        assert chosen is not None
        imu, imu_time = chosen
        yield {
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
    # EOF checks and late fallback apply even beyond the final camera frame.
    for _ in samples:
        pass


def _alignment_summary(deltas: list[float]) -> dict[str, Any]:
    return {
        "camera_timestamp_source": "shared_sbs_frame",
        "camera_time_base": "host_monotonic",
        "left_right_delta_ms": 0.0,
        "imu_matching": "nearest_estimated_sample",
        "rows": len(deltas),
        "nearest_imu_delta_p50_ms": _percentile(deltas, 0.50),
        "nearest_imu_delta_p95_ms": _percentile(deltas, 0.95),
        "nearest_imu_delta_max_ms": max(deltas, default=None),
    }


class _CheckedCsvOutput:
    def __init__(self, stream: TextIO) -> None:
        self.stream = stream

    def write(self, text: str) -> int:
        written = self.stream.write(text)
        if written != len(text):
            raise OSError("short timestamp CSV write")
        return written


def _check_output(path: Path, root: Path, manifest: dict[str, Any]) -> None:
    protected = [root / "manifest.json"]
    protected.extend(root / item["path"] for item in iter_device_session_v1_artifacts(manifest))
    resolved = path.resolve()
    if any(resolved == item.resolve() for item in protected):
        raise SessionTimestampError("session_timestamps_invalid", "CSV 输出不能覆盖会话原始文件")


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
        "ylx.device-session.v4",
    }:
        raise SessionTimestampError("unsupported_session", "只支持 Device Session v1/v2/v3/v4")
    path = Path(output) if output is not None else None
    if path is not None:
        _check_output(path, root, manifest)
    frames = _load_frames(root, manifest)
    frame_interval = _frame_interval_summary(frames, manifest)
    with ExitStack() as stack:
        spool = stack.enter_context(tempfile.TemporaryFile(mode="w+b"))
        imu = _packet_summary(root, manifest, spool)
        stream = None
        temporary = None
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            stream = tempfile.NamedTemporaryFile(  # noqa: SIM115 - owned by ExitStack below
                mode="w",
                encoding="utf-8",
                newline="",
                dir=path.parent,
                prefix=f".{path.name}.",
                suffix=".tmp",
                delete=False,
            )
            temporary = Path(stream.name)
            stack.callback(temporary.unlink, missing_ok=True)
            stack.enter_context(stream)
        for raw in (False, True):
            deltas: list[float] = []
            writer = None
            if stream is not None:
                stream.seek(0)
                stream.truncate()
                writer = csv.DictWriter(_CheckedCsvOutput(stream), fieldnames=TIMESTAMP_CSV_FIELDS)
                writer.writeheader()
            try:
                with closing(
                    _timed_samples(root, manifest, spool, imu["record_rate_hz"], raw=raw)
                ) as samples:
                    for row in _alignment_rows(frames, samples):
                        deltas.append(abs(row["nearest_imu_delta_ms"]))
                        if writer is not None:
                            writer.writerow(row)
            except _UseHostTimes:
                continue
            break
        report: dict[str, Any] = {
            "ok": True,
            "schema": "openaria.session-timestamps.v1",
            "source_schema": manifest["schema"],
            "session_id": manifest["session_id"],
            "capture_mode": manifest.get("capture_mode"),
            "frames": len(frames),
            "imu_samples": imu["samples"],
            "frame_interval": frame_interval,
            "imu": imu,
            "timestamp_alignment": _alignment_summary(deltas),
        }
        if stream is not None:
            stream.flush()
            os.fsync(stream.fileno())
            stream.close()
            # Recheck aliases after the potentially long input passes.
            assert path is not None and temporary is not None
            _check_output(path, root, manifest)
            os.replace(temporary, path)
            report["csv"] = {
                "path": str(path),
                "rows": len(frames),
                "columns": list(TIMESTAMP_CSV_FIELDS),
            }
    return report
