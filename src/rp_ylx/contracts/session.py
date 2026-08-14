"""录制会话格式 v0 的完整目录验证器。"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any

from rp_ylx.contracts.frame_stream import FrameStreamError, iter_frames

SESSION_FORMAT = "ylx.recording-session.v0"
STATE_FORMAT = "ylx.recording-session-state.v0"
FRAME_FORMAT = "ylx.frame.v0"
IMU_FORMAT = "ylx.imu.v0"
DIAGNOSTIC_FORMAT = "ylx.diagnostic.v0"

ROLE_PATHS = {
    "session.metadata": "session.json",
    "video.left": "video/left.bin",
    "video.right": "video/right.bin",
    "frames.timeline": "frames.ndjson",
    "imu.samples": "imu.ndjson",
    "diagnostics.events": "diagnostics.ndjson",
}

ROLE_MEDIA_TYPES = {
    "session.metadata": "application/json",
    "video.left": "application/vnd.ylx.frame-stream",
    "video.right": "application/vnd.ylx.frame-stream",
    "frames.timeline": "application/x-ndjson",
    "imu.samples": "application/x-ndjson",
    "diagnostics.events": "application/x-ndjson",
}

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class SessionValidationError(ValueError):
    """携带稳定错误码的会话验证失败。"""

    def __init__(self, code: str, location: str, message: str) -> None:
        self.code = code
        self.location = location
        self.message = message
        super().__init__(f"{code} {location}: {message}")


def _fail(code: str, location: str, message: str) -> None:
    raise SessionValidationError(code, location, message)


def _read_json(path: Path, location: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        _fail("missing_file", location, "文件不存在")
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        _fail("invalid_json", location, f"无法读取 JSON：{exc}")
    if not isinstance(value, dict):
        _fail("invalid_type", location, "必须是 JSON 对象")
    return value


def _strict_object(
    value: object,
    location: str,
    *,
    required: set[str],
    optional: set[str] | None = None,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail("invalid_type", location, "必须是对象")
    optional = optional or set()
    missing = required - value.keys()
    if missing:
        _fail("missing_field", location, f"缺少字段：{', '.join(sorted(missing))}")
    extra = value.keys() - required - optional
    if extra:
        _fail("unknown_field", location, f"未知字段：{', '.join(sorted(extra))}")
    return value


def _non_negative_int(value: object, location: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        _fail("invalid_value", location, "必须是非负整数")
    return value


def _positive_number(value: object, location: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        _fail("invalid_value", location, "必须是正数")
    return float(value)


def _uuid(value: object, location: str) -> str:
    if not isinstance(value, str):
        _fail("invalid_value", location, "必须是 UUID 字符串")
    try:
        parsed = uuid.UUID(value)
    except ValueError:
        _fail("invalid_value", location, "不是有效 UUID")
    if str(parsed) != value:
        _fail("invalid_value", location, "UUID 必须使用小写规范形式")
    return value


def _timestamp(value: object, location: str) -> datetime:
    if not isinstance(value, str) or not value.endswith(("Z", "+00:00")):
        _fail("invalid_value", location, "必须是带 UTC 时区的 RFC 3339 时间")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        _fail("invalid_value", location, "不是有效 RFC 3339 时间")


def _safe_path(value: object, location: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        _fail("unsafe_path", location, "必须是非空 POSIX 相对路径")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        _fail("unsafe_path", location, "不允许绝对路径、点路径或路径穿越")
    if path.as_posix() != value:
        _fail("unsafe_path", location, "路径必须使用规范 POSIX 形式")
    return value


def _digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def _ndjson(path: Path, expected_format: str, session_id: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        _fail("invalid_ndjson", path.name, f"无法读取 NDJSON：{exc}")
    for number, line in enumerate(lines, 1):
        if not line.strip():
            _fail("invalid_ndjson", f"{path.name}:{number}", "不允许空行")
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            _fail("invalid_ndjson", f"{path.name}:{number}", str(exc))
        if not isinstance(record, dict):
            _fail("invalid_ndjson", f"{path.name}:{number}", "每行必须是对象")
        if record.get("format") != expected_format:
            _fail("unsupported_version", f"{path.name}:{number}.format", "记录格式不受支持")
        if record.get("session_id") != session_id:
            _fail("identity_mismatch", f"{path.name}:{number}.session_id", "会话身份不一致")
        records.append(record)
    return records


def _validate_frames(records: list[dict[str, Any]]) -> None:
    required = {"format", "session_id", "sequence", "source_sequence", "host_monotonic_ns"}
    previous_source = -1
    previous_time = -1
    for index, record in enumerate(records):
        item = _strict_object(record, f"frames.ndjson:{index + 1}", required=required)
        if _non_negative_int(item["sequence"], f"frames[{index}].sequence") != index:
            _fail("count_mismatch", f"frames[{index}].sequence", "持久帧序号必须从零连续递增")
        source = _non_negative_int(item["source_sequence"], f"frames[{index}].source_sequence")
        host_time = _non_negative_int(
            item["host_monotonic_ns"], f"frames[{index}].host_monotonic_ns"
        )
        if source <= previous_source:
            _fail("non_monotonic", f"frames[{index}].source_sequence", "源帧序号必须严格递增")
        if host_time <= previous_time:
            _fail("non_monotonic", f"frames[{index}].host_monotonic_ns", "主机时间必须严格递增")
        previous_source, previous_time = source, host_time


def _raw_vector(value: object, location: str) -> None:
    if not isinstance(value, list) or len(value) != 3:
        _fail("invalid_value", location, "必须是三个 raw int16")
    if any(
        isinstance(axis, bool) or not isinstance(axis, int) or not -32768 <= axis <= 32767
        for axis in value
    ):
        _fail("invalid_value", location, "每个轴必须是 raw int16")


def _validate_imu(records: list[dict[str, Any]]) -> None:
    required = {
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
    previous_sequence = -1
    previous_packet = -1
    previous_sample_index = -1
    previous_raw = -1
    previous_ticks = -1
    previous_time = -1
    previous_interval = (-1, -1)
    for index, record in enumerate(records):
        item = _strict_object(record, f"imu.ndjson:{index + 1}", required=required)
        sequence = _non_negative_int(item["sequence"], f"imu[{index}].sequence")
        packet = _non_negative_int(item["packet_sequence"], f"imu[{index}].packet_sequence")
        sample_index = _non_negative_int(item["sample_index"], f"imu[{index}].sample_index")
        if sample_index not in {0, 1}:
            _fail("invalid_value", f"imu[{index}].sample_index", "v0 包内样本序号必须是 0 或 1")
        raw_timestamp = _non_negative_int(
            item["device_timestamp_raw"], f"imu[{index}].device_timestamp_raw"
        )
        if raw_timestamp >= 1 << 24:
            _fail("invalid_value", f"imu[{index}].device_timestamp_raw", "必须是 24-bit 原始值")
        ticks = _non_negative_int(item["device_ticks"], f"imu[{index}].device_ticks")
        if ticks % (1 << 24) != raw_timestamp:
            _fail("invalid_value", f"imu[{index}].device_ticks", "展开 tick 与 24-bit 原始值不一致")
        read_start = _non_negative_int(
            item["host_read_start_ns"], f"imu[{index}].host_read_start_ns"
        )
        read_end = _non_negative_int(item["host_read_end_ns"], f"imu[{index}].host_read_end_ns")
        host_time = _non_negative_int(item["host_monotonic_ns"], f"imu[{index}].host_monotonic_ns")
        if read_end < read_start or not read_start <= host_time <= read_end:
            _fail("invalid_value", f"imu[{index}]", "主机代表时间必须位于读取区间内")
        if sequence <= previous_sequence:
            _fail("non_monotonic", f"imu[{index}].sequence", "IMU 序号必须严格递增")
        if packet < previous_packet:
            _fail("non_monotonic", f"imu[{index}].packet_sequence", "包序号不能回退")
        if packet == previous_packet:
            if (
                sample_index <= previous_sample_index
                or raw_timestamp != previous_raw
                or ticks != previous_ticks
                or host_time != previous_time
                or (read_start, read_end) != previous_interval
            ):
                _fail("packet_mismatch", f"imu[{index}]", "同包样本必须共享设备时间和主机读取证据")
        elif previous_packet >= 0 and (ticks <= previous_ticks or host_time <= previous_time):
            _fail("non_monotonic", f"imu[{index}]", "新包的展开设备时间和主机时间必须前进")
        raw = _strict_object(
            item["raw"], f"imu[{index}].raw", required={"accelerometer", "gyroscope"}
        )
        _raw_vector(raw["accelerometer"], f"imu[{index}].raw.accelerometer")
        _raw_vector(raw["gyroscope"], f"imu[{index}].raw.gyroscope")
        sync = _strict_object(
            item["sync"],
            f"imu[{index}].sync",
            required={"offset_ns", "residual_ns", "quality"},
        )
        if sync["quality"] not in {"good", "degraded", "insufficient"}:
            _fail("invalid_value", f"imu[{index}].sync", "同步质量或残差无效")
        if sync["quality"] == "insufficient":
            if sync["offset_ns"] is not None or sync["residual_ns"] is not None:
                _fail("invalid_value", f"imu[{index}].sync", "证据不足时不能伪造偏移或残差")
        elif (
            isinstance(sync["offset_ns"], bool)
            or not isinstance(sync["offset_ns"], int)
            or isinstance(sync["residual_ns"], bool)
            or not isinstance(sync["residual_ns"], int)
            or sync["residual_ns"] < 0
        ):
            _fail("invalid_value", f"imu[{index}].sync", "有效同步必须给出整数纳秒偏移和非负残差")
        previous_sequence = sequence
        previous_packet = packet
        previous_sample_index = sample_index
        previous_raw = raw_timestamp
        previous_ticks = ticks
        previous_time = host_time
        previous_interval = (read_start, read_end)


def _validate_diagnostics(records: list[dict[str, Any]]) -> set[str]:
    required = {"format", "session_id", "monotonic_ns", "severity", "code", "message", "count"}
    codes: set[str] = set()
    previous_time = -1
    for index, record in enumerate(records):
        item = _strict_object(record, f"diagnostics.ndjson:{index + 1}", required=required)
        monotonic = _non_negative_int(item["monotonic_ns"], f"diagnostics[{index}].monotonic_ns")
        if monotonic < previous_time:
            _fail("non_monotonic", f"diagnostics[{index}].monotonic_ns", "诊断时间不能回退")
        if item["severity"] not in {"info", "warning", "error"}:
            _fail("invalid_value", f"diagnostics[{index}].severity", "诊断级别无效")
        if not isinstance(item["code"], str) or not item["code"]:
            _fail("invalid_value", f"diagnostics[{index}].code", "诊断码不能为空")
        if not isinstance(item["message"], str) or not item["message"]:
            _fail("invalid_value", f"diagnostics[{index}].message", "诊断信息不能为空")
        _non_negative_int(item["count"], f"diagnostics[{index}].count")
        codes.add(item["code"])
        previous_time = monotonic
    return codes


def validate_session(directory: str | Path, *, allow_partial: bool = False) -> dict[str, Any]:
    """验证会话目录；消费者必须保留默认值以拒绝临时目录。"""

    root = Path(directory)
    is_partial = root.name.endswith(".partial")
    if is_partial and not allow_partial:
        _fail("not_sealed", str(root), "临时会话不是完整会话")
    manifest = _read_json(root / "manifest.json", "manifest.json")
    manifest = _strict_object(
        manifest,
        "manifest",
        required={
            "format",
            "state",
            "session_id",
            "time",
            "device",
            "capture",
            "counts",
            "artifacts",
        },
    )
    if manifest["format"] != SESSION_FORMAT:
        _fail("unsupported_version", "manifest.format", "仅支持 ylx.recording-session.v0")
    if manifest["state"] != "sealed":
        _fail("not_sealed", "manifest.state", "完整会话状态必须为 sealed")
    session_id = _uuid(manifest["session_id"], "manifest.session_id")
    expected_directory_name = f"{session_id}.partial" if is_partial else session_id
    if root.name != expected_directory_name:
        _fail("identity_mismatch", str(root), "目录名必须等于 session_id")

    time = _strict_object(manifest["time"], "manifest.time", required={"started_at", "ended_at"})
    if _timestamp(time["ended_at"], "manifest.time.ended_at") < _timestamp(
        time["started_at"], "manifest.time.started_at"
    ):
        _fail("invalid_value", "manifest.time", "结束时间不能早于开始时间")
    device = _strict_object(
        manifest["device"], "manifest.device", required={"id", "software_version"}
    )
    if not all(isinstance(device[key], str) and device[key] for key in device):
        _fail("invalid_value", "manifest.device", "设备字段必须是非空字符串")
    capture = _strict_object(
        manifest["capture"], "manifest.capture", required={"video", "imu", "clock"}
    )
    video = _strict_object(
        capture["video"],
        "manifest.capture.video",
        required={"width", "height", "fps", "encoding", "coordinate_frame"},
    )
    _non_negative_int(video["width"], "manifest.capture.video.width")
    _non_negative_int(video["height"], "manifest.capture.video.height")
    if video["width"] == 0 or video["height"] == 0:
        _fail("invalid_value", "manifest.capture", "图像尺寸必须大于零")
    _positive_number(video["fps"], "manifest.capture.video.fps")
    if not isinstance(video["encoding"], str) or not video["encoding"]:
        _fail("invalid_value", "manifest.capture.video.encoding", "帧编码不能为空")
    if video["coordinate_frame"] != "opencv_optical":
        _fail("invalid_value", "manifest.capture.video.coordinate_frame", "视频坐标系不受支持")
    imu_config = _strict_object(
        capture["imu"],
        "manifest.capture.imu",
        required={"coordinate_frame", "device_tick_hz", "ranges"},
    )
    if imu_config["coordinate_frame"] != "opencv_optical":
        _fail("invalid_value", "manifest.capture.imu.coordinate_frame", "IMU 坐标系不受支持")
    if imu_config["device_tick_hz"] is not None:
        _positive_number(imu_config["device_tick_hz"], "manifest.capture.imu.device_tick_hz")
    if imu_config["ranges"] is not None:
        ranges = _strict_object(
            imu_config["ranges"],
            "manifest.capture.imu.ranges",
            required={"accelerometer", "gyroscope"},
        )
        for name, expected_unit in (("accelerometer", "g"), ("gyroscope", "degree_per_second")):
            configured_range = _strict_object(
                ranges[name],
                f"manifest.capture.imu.ranges.{name}",
                required={"full_scale", "unit"},
            )
            _positive_number(
                configured_range["full_scale"],
                f"manifest.capture.imu.ranges.{name}.full_scale",
            )
            if configured_range["unit"] != expected_unit:
                _fail("invalid_value", f"manifest.capture.imu.ranges.{name}.unit", "量程单位无效")
    clock = _strict_object(capture["clock"], "manifest.capture.clock", required={"domain", "unit"})
    if clock != {"domain": "host_monotonic", "unit": "nanosecond"}:
        _fail("invalid_value", "manifest.capture.clock", "v0 仅支持主机单调纳秒时钟")

    counts = _strict_object(
        manifest["counts"],
        "manifest.counts",
        required={"frames", "imu_samples", "diagnostics", "dropped_frames", "dropped_imu_samples"},
    )
    parsed_counts = {
        key: _non_negative_int(value, f"manifest.counts.{key}") for key, value in counts.items()
    }

    artifacts = manifest["artifacts"]
    if not isinstance(artifacts, list):
        _fail("invalid_type", "manifest.artifacts", "必须是数组")
    by_role: dict[str, dict[str, Any]] = {}
    seen_paths: set[str] = set()
    root_resolved = root.resolve()
    for index, raw_artifact in enumerate(artifacts):
        artifact = _strict_object(
            raw_artifact,
            f"manifest.artifacts[{index}]",
            required={"role", "path", "media_type", "bytes", "sha256", "records"},
        )
        role = artifact["role"]
        if role not in ROLE_PATHS:
            _fail("invalid_role", f"manifest.artifacts[{index}].role", "文件角色无效")
        if role in by_role:
            _fail("duplicate_role", f"manifest.artifacts[{index}].role", "文件角色重复")
        relative = _safe_path(artifact["path"], f"manifest.artifacts[{index}].path")
        if relative in seen_paths:
            _fail("duplicate_path", f"manifest.artifacts[{index}].path", "文件路径重复")
        if relative != ROLE_PATHS[role] or artifact["media_type"] != ROLE_MEDIA_TYPES[role]:
            _fail("role_mismatch", f"manifest.artifacts[{index}]", "角色、路径或媒体类型不匹配")
        candidate = root / relative
        try:
            resolved = candidate.resolve(strict=True)
        except FileNotFoundError:
            _fail("missing_file", relative, "清单声明的文件不存在")
        if (
            not resolved.is_relative_to(root_resolved)
            or candidate.is_symlink()
            or not resolved.is_file()
        ):
            _fail("unsafe_path", relative, "文件必须是会话目录内的普通文件且不能是符号链接")
        expected_bytes = _non_negative_int(artifact["bytes"], f"manifest.artifacts[{index}].bytes")
        expected_records = _non_negative_int(
            artifact["records"], f"manifest.artifacts[{index}].records"
        )
        if resolved.stat().st_size != expected_bytes:
            _fail("size_mismatch", relative, "文件大小与清单不一致")
        if not isinstance(artifact["sha256"], str) or not _SHA256.fullmatch(artifact["sha256"]):
            _fail("invalid_value", f"manifest.artifacts[{index}].sha256", "SHA-256 格式无效")
        if _digest(resolved) != artifact["sha256"]:
            _fail("digest_mismatch", relative, "文件摘要与清单不一致")
        by_role[role] = {**artifact, "resolved": resolved, "records": expected_records}
        seen_paths.add(relative)
    if set(by_role) != set(ROLE_PATHS):
        _fail("missing_role", "manifest.artifacts", "缺少必需文件角色")

    state = _read_json(by_role["session.metadata"]["resolved"], "session.json")
    state = _strict_object(
        state,
        "session.json",
        required={"format", "session_id", "state", "started_at", "updated_at", "failure"},
    )
    if state["format"] != STATE_FORMAT:
        _fail("unsupported_version", "session.json.format", "会话状态格式不受支持")
    if state["session_id"] != session_id:
        _fail("identity_mismatch", "session.json.session_id", "会话身份不一致")
    if state["state"] != "sealed" or state["failure"] is not None:
        _fail("not_sealed", "session.json.state", "封存状态必须是无失败原因的 sealed")
    _timestamp(state["started_at"], "session.json.started_at")
    _timestamp(state["updated_at"], "session.json.updated_at")
    if by_role["session.metadata"]["records"] != 1:
        _fail("count_mismatch", "session.metadata.records", "session.json 的记录数必须为 1")

    frames = _ndjson(by_role["frames.timeline"]["resolved"], FRAME_FORMAT, session_id)
    imu = _ndjson(by_role["imu.samples"]["resolved"], IMU_FORMAT, session_id)
    diagnostics = _ndjson(by_role["diagnostics.events"]["resolved"], DIAGNOSTIC_FORMAT, session_id)
    _validate_frames(frames)
    _validate_imu(imu)
    diagnostic_codes = _validate_diagnostics(diagnostics)
    expected_by_role = {
        "video.left": parsed_counts["frames"],
        "video.right": parsed_counts["frames"],
        "frames.timeline": parsed_counts["frames"],
        "imu.samples": parsed_counts["imu_samples"],
        "diagnostics.events": parsed_counts["diagnostics"],
    }
    for role, expected in expected_by_role.items():
        if by_role[role]["records"] != expected:
            _fail("count_mismatch", f"manifest.artifacts[{role}].records", "记录数与 counts 不一致")
    if len(frames) != parsed_counts["frames"] or len(imu) != parsed_counts["imu_samples"]:
        _fail("count_mismatch", "manifest.counts", "NDJSON 实际记录数与 counts 不一致")
    if len(diagnostics) != parsed_counts["diagnostics"]:
        _fail("count_mismatch", "manifest.counts.diagnostics", "诊断实际记录数不一致")
    for role in ("video.left", "video.right"):
        try:
            with by_role[role]["resolved"].open("rb") as stream:
                actual_frames = sum(1 for _ in iter_frames(stream))
        except (OSError, FrameStreamError) as exc:
            _fail("invalid_frame_stream", by_role[role]["path"], str(exc))
        if actual_frames != parsed_counts["frames"]:
            _fail("count_mismatch", by_role[role]["path"], "视频帧数与 counts.frames 不一致")
    if parsed_counts["dropped_frames"] and "frame_dropped" not in diagnostic_codes:
        _fail("unreported_loss", "manifest.counts.dropped_frames", "丢帧必须有 frame_dropped 诊断")
    if parsed_counts["dropped_imu_samples"] and "imu_dropped" not in diagnostic_codes:
        _fail(
            "unreported_loss",
            "manifest.counts.dropped_imu_samples",
            "IMU 丢样必须有 imu_dropped 诊断",
        )
    return manifest
