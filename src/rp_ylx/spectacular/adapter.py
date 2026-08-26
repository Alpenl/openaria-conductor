"""Strict legacy and Device Session inputs for the Spectacular calibration toolchain."""

from __future__ import annotations

import json
import math
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from rp_ylx.recording.device_session import (
    DeviceRecordingError,
    validate_device_session_directory,
)

MAX_MANIFEST_BYTES = 8 * 1024 * 1024
MAX_NDJSON_LINE_BYTES = 1024 * 1024


class CaptureValidationError(ValueError):
    """The selected capture cannot be consumed without guessing or trusting unsafe data."""


VideoAuthority = Literal["legacy_raw", "device_session"]


@dataclass(frozen=True, slots=True)
class CaptureVideoSegment:
    index: int
    start_frame: int
    end_frame: int
    left_path: Path
    right_path: Path


@dataclass(frozen=True, slots=True)
class CaptureVideo:
    authority: VideoAuthority
    path: Path | None = None
    segments: tuple[CaptureVideoSegment, ...] = ()


@dataclass(frozen=True, slots=True)
class LoadedCapture:
    root: Path
    source_schema: str
    session_id: str | None
    manifest: dict[str, Any]
    frames: tuple[dict[str, Any], ...]
    imu_samples: tuple[dict[str, Any], ...]
    video: CaptureVideo
    fps: float
    width: int
    eye_width: int
    height: int
    imu_samples_per_packet: int
    imu_timestamp_bits: int | None


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CaptureValidationError(f"{label} must be an object")
    return value


def _integer(
    value: object,
    label: str,
    *,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    if type(value) is not int or value < minimum or (maximum is not None and value > maximum):
        raise CaptureValidationError(f"{label} is not a bounded integer")
    return value


def _number(value: object, label: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CaptureValidationError(f"{label} is not numeric")
    result = float(value)
    if not math.isfinite(result) or (positive and result <= 0):
        raise CaptureValidationError(f"{label} must be positive")
    return result


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number: {value}")


def _loads_json(payload: str | bytes, label: str) -> object:
    try:
        return json.loads(
            payload,
            object_pairs_hook=_strict_object,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeError, json.JSONDecodeError, ValueError) as error:
        raise CaptureValidationError(f"invalid JSON in {label}: {error}") from error


def _require_directory(path: Path, label: str) -> None:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise CaptureValidationError(f"missing {label}: {path}") from error
    if not stat.S_ISDIR(metadata.st_mode):
        raise CaptureValidationError(f"{label} is not a real directory: {path}")


def _require_regular(path: Path, label: str, *, allow_empty: bool = False) -> None:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise CaptureValidationError(f"missing {label}: {path}") from error
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or (not allow_empty and metadata.st_size <= 0)
    ):
        raise CaptureValidationError(f"{label} is not an exclusive regular file: {path}")


def _safe_relative(value: object, label: str) -> PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise CaptureValidationError(f"invalid {label} path")
    relative = PurePosixPath(value)
    if (
        relative.is_absolute()
        or relative.as_posix() != value
        or any(part in {"", ".", ".."} for part in relative.parts)
    ):
        raise CaptureValidationError(f"unsafe {label} path: {value!r}")
    return relative


def _resolve_file(
    root: Path,
    value: object,
    label: str,
    *,
    allow_empty: bool = False,
) -> Path:
    relative = _safe_relative(value, label)
    current = root
    for component in relative.parts[:-1]:
        current /= component
        _require_directory(current, f"{label} parent")
    path = root.joinpath(*relative.parts)
    _require_regular(path, label, allow_empty=allow_empty)
    return path


def _read_json_object(path: Path, label: str) -> dict[str, Any]:
    _require_regular(path, label)
    if path.stat().st_size > MAX_MANIFEST_BYTES:
        raise CaptureValidationError(f"{label} exceeds the bounded size")
    try:
        value = _loads_json(path.read_text(encoding="utf-8"), label)
    except OSError as error:
        raise CaptureValidationError(f"cannot read {label}: {error}") from error
    return _mapping(value, label)


def _read_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    try:
        with path.open("rb") as stream:
            line_number = 0
            while True:
                line = stream.readline(MAX_NDJSON_LINE_BYTES + 1)
                if not line:
                    break
                line_number += 1
                if len(line) > MAX_NDJSON_LINE_BYTES or not line.endswith(b"\n"):
                    raise CaptureValidationError(
                        f"{label}:{line_number} is overlong or lacks a newline"
                    )
                value = _loads_json(line, f"{label}:{line_number}")
                records.append(_mapping(value, f"{label}:{line_number}"))
    except OSError as error:
        raise CaptureValidationError(f"cannot read {label}: {error}") from error
    if not records:
        raise CaptureValidationError(f"{label} is empty")
    return records


def _raw_vector(value: object, label: str) -> list[int]:
    if not isinstance(value, list) or len(value) != 3:
        raise CaptureValidationError(f"{label} must contain exactly three axes")
    return [
        _integer(axis, f"{label}[{index}]", minimum=-32768, maximum=32767)
        for index, axis in enumerate(value)
    ]


def _normalize_device_frames(
    records: list[dict[str, Any]],
    *,
    session_id: str,
    frame_decimation: int,
    segments: tuple[CaptureVideoSegment, ...],
) -> tuple[dict[str, Any], ...]:
    expected_keys = {
        "schema",
        "session_id",
        "frame",
        "source_sequence",
        "host_monotonic_ns",
        "segment_index",
        "segment_frame",
    }
    normalized: list[dict[str, Any]] = []
    previous_host = -1
    previous_source: int | None = None
    for index, record in enumerate(records):
        if set(record) != expected_keys:
            raise CaptureValidationError(f"Device Session frame {index} is not a closed record")
        if record["schema"] != "ylx.frame-index.v1" or record["session_id"] != session_id:
            raise CaptureValidationError(f"Device Session frame {index} identity is invalid")
        frame = _integer(record["frame"], f"frame[{index}].frame")
        source_sequence = _integer(
            record["source_sequence"],
            f"frame[{index}].source_sequence",
        )
        host = _integer(record["host_monotonic_ns"], f"frame[{index}].host_monotonic_ns")
        segment_index = _integer(record["segment_index"], f"frame[{index}].segment_index")
        segment_frame = _integer(record["segment_frame"], f"frame[{index}].segment_frame")
        if frame != index or host <= previous_host:
            raise CaptureValidationError(
                f"Device Session frame {index} order or host timestamp is invalid"
            )
        if segment_index >= len(segments):
            raise CaptureValidationError(f"Device Session frame {index} segment is out of range")
        segment = segments[segment_index]
        if not segment.start_frame <= frame < segment.end_frame:
            raise CaptureValidationError(
                f"Device Session frame {index} is outside its declared segment"
            )
        if segment_frame != frame - segment.start_frame:
            raise CaptureValidationError(
                f"Device Session frame {index} segment-local index is invalid"
            )
        if previous_source is not None and source_sequence != previous_source + frame_decimation:
            raise CaptureValidationError(
                f"Device Session source sequence gap or regression at record {index}: "
                f"{previous_source} -> {source_sequence}; expected declared "
                f"frame_decimation {frame_decimation}"
            )
        normalized.append(
            {
                "frame_index": frame,
                "uvc_sequence": source_sequence,
                "callback_monotonic_ns": host,
                "segment_index": segment_index,
                "segment_frame": segment_frame,
                "source": dict(record),
            }
        )
        previous_host = host
        previous_source = source_sequence
    if segments[0].start_frame != 0 or segments[-1].end_frame != len(records):
        raise CaptureValidationError(
            "Device Session frame index does not cover the declared video segments"
        )
    return tuple(normalized)


def _normalize_device_imu(
    records: list[dict[str, Any]], *, session_id: str
) -> tuple[dict[str, Any], ...]:
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
    normalized: list[dict[str, Any]] = []
    previous_sequence: int | None = None
    for index, record in enumerate(records):
        if set(record) != expected_keys:
            raise CaptureValidationError(f"Device Session IMU {index} is not a closed record")
        if record["format"] != "ylx.imu.v0" or record["session_id"] != session_id:
            raise CaptureValidationError(f"Device Session IMU {index} identity is invalid")
        sequence = _integer(record["sequence"], f"imu[{index}].sequence")
        if previous_sequence is not None and sequence != previous_sequence + 1:
            raise CaptureValidationError(f"Device Session IMU sequence gap at record {index}")
        packet_sequence = _integer(
            record["packet_sequence"],
            f"imu[{index}].packet_sequence",
            maximum=(1 << 32) - 1,
        )
        sample_index = _integer(record["sample_index"], f"imu[{index}].sample_index")
        device_timestamp_raw = _integer(
            record["device_timestamp_raw"], f"imu[{index}].device_timestamp_raw"
        )
        device_ticks = _integer(record["device_ticks"], f"imu[{index}].device_ticks")
        read_start = _integer(record["host_read_start_ns"], f"imu[{index}].host_read_start_ns")
        read_end = _integer(record["host_read_end_ns"], f"imu[{index}].host_read_end_ns")
        host = _integer(record["host_monotonic_ns"], f"imu[{index}].host_monotonic_ns")
        if read_end < read_start or not read_start <= host <= read_end:
            raise CaptureValidationError(f"Device Session IMU {index} host interval is invalid")
        raw = _mapping(record["raw"], f"imu[{index}].raw")
        if set(raw) != {"accelerometer", "gyroscope"}:
            raise CaptureValidationError(f"Device Session IMU {index} raw axes are not closed")
        sync = _mapping(record["sync"], f"imu[{index}].sync")
        if set(sync) != {"offset_ns", "residual_ns", "quality"}:
            raise CaptureValidationError(f"Device Session IMU {index} sync is not closed")
        for key in ("offset_ns", "residual_ns"):
            if sync[key] is not None and type(sync[key]) is not int:
                raise CaptureValidationError(f"Device Session IMU {index} sync.{key} is invalid")
        if sync["quality"] not in {"insufficient", "degraded", "good"}:
            raise CaptureValidationError(f"Device Session IMU {index} sync quality is invalid")
        normalized.append(
            {
                "sample_number": index,
                "sequence": sequence,
                "packet_sequence": packet_sequence,
                "sample_index": sample_index,
                "samples_in_packet": 2,
                "device_timestamp_raw": device_timestamp_raw,
                "device_ticks": device_ticks,
                "host_read_start_ns": read_start,
                "host_read_end_ns": read_end,
                "host_monotonic_ns": host,
                "accel_raw": _raw_vector(raw["accelerometer"], f"imu[{index}].accelerometer"),
                "gyro_raw": _raw_vector(raw["gyroscope"], f"imu[{index}].gyroscope"),
                "sync": dict(sync),
                "source": {
                    "format": record["format"],
                    "session_id": record["session_id"],
                    "sequence": sequence,
                    "packet_sequence": packet_sequence,
                    "sample_index": sample_index,
                    "device_timestamp_raw": device_timestamp_raw,
                    "device_ticks": device_ticks,
                    "host_read_start_ns": read_start,
                    "host_read_end_ns": read_end,
                    "host_monotonic_ns": host,
                    "raw": {
                        "accelerometer": list(raw["accelerometer"]),
                        "gyroscope": list(raw["gyroscope"]),
                    },
                    "sync": dict(sync),
                },
            }
        )
        previous_sequence = sequence
    return tuple(normalized)


def _artifact(
    root: Path,
    value: object,
    *,
    role: str,
    media_type: str,
    label: str,
    allow_empty: bool = False,
) -> tuple[dict[str, Any], Path]:
    descriptor = _mapping(value, label)
    if set(descriptor) != {"artifact_id", "role", "path", "media_type", "bytes", "sha256"}:
        raise CaptureValidationError(f"{label} descriptor is not closed")
    if (
        descriptor["role"] != role
        or descriptor["media_type"] != media_type
        or descriptor["artifact_id"] != descriptor["sha256"]
    ):
        raise CaptureValidationError(f"{label} role or content identity is invalid")
    expected_bytes = _integer(descriptor["bytes"], f"{label}.bytes")
    path = _resolve_file(root, descriptor["path"], label, allow_empty=allow_empty)
    if path.stat().st_size != expected_bytes:
        raise CaptureValidationError(f"{label} size differs from its descriptor")
    return descriptor, path


def _load_device_session(root: Path, manifest: dict[str, Any]) -> LoadedCapture:
    schema = manifest.get("schema")
    if schema != "ylx.device-session.v2":
        raise CaptureValidationError(f"unsupported Device Session schema: {schema!r}")
    if manifest.get("capture_mode") != "calibration":
        raise CaptureValidationError("Spectacular only accepts calibration Device Sessions")
    video = _mapping(manifest.get("video"), "Device Session video")
    if video.get("layout") != "split-eyes":
        raise CaptureValidationError("calibration Device Session must use split-eyes video")
    if video.get("codec") != "h264" or video.get("container") != "mp4":
        raise CaptureValidationError("calibration Device Session segmented video is invalid")
    frames_block = _mapping(manifest.get("frames"), "Device Session frames")
    imu_block = _mapping(manifest.get("imu"), "Device Session IMU")
    camera = _mapping(manifest.get("camera"), "Device Session camera")
    frame_decimation = _integer(
        camera.get("frame_decimation"), "camera.frame_decimation", minimum=1
    )
    audio = _mapping(manifest.get("audio"), "Device Session audio")
    if audio.get("state") != "not_recorded":
        raise CaptureValidationError(
            "calibration Device Session must not depend on an audio artifact"
        )
    session_id = manifest.get("session_id")
    if not isinstance(session_id, str):
        raise CaptureValidationError("Device Session has no session_id")
    try:
        validated = validate_device_session_directory(root, expected_session_id=session_id)
    except DeviceRecordingError as error:
        raise CaptureValidationError(
            f"Device Session integrity validation failed: {error}"
        ) from error
    if dict(validated) != manifest:
        raise CaptureValidationError("Device Session manifest changed during validation")
    raw_segments = video.get("segments")
    if not isinstance(raw_segments, list) or not raw_segments:
        raise CaptureValidationError("Device Session has no split-eye video segments")
    segments: list[CaptureVideoSegment] = []
    expected_start = 0
    for expected_index, raw_segment in enumerate(raw_segments):
        segment = _mapping(raw_segment, f"video segment {expected_index}")
        index = _integer(segment.get("index"), f"video segment {expected_index}.index")
        start_frame = _integer(
            segment.get("start_frame"), f"video segment {expected_index}.start_frame"
        )
        end_frame = _integer(
            segment.get("end_frame"), f"video segment {expected_index}.end_frame", minimum=1
        )
        if index != expected_index or start_frame != expected_start or end_frame <= start_frame:
            raise CaptureValidationError(
                "Device Session video segment frame domain is not contiguous"
            )
        artifacts = _mapping(segment.get("artifacts"), f"video segment {expected_index}.artifacts")
        _, left_path = _artifact(
            root,
            artifacts.get("left"),
            role="video.left",
            media_type="video/mp4",
            label=f"video segment {expected_index} left",
        )
        _, right_path = _artifact(
            root,
            artifacts.get("right"),
            role="video.right",
            media_type="video/mp4",
            label=f"video segment {expected_index} right",
        )
        segments.append(CaptureVideoSegment(index, start_frame, end_frame, left_path, right_path))
        expected_start = end_frame
    _, frames_path = _artifact(
        root,
        frames_block["artifact"],
        role="frames.index",
        media_type="application/x-ndjson",
        label="frame index",
    )
    _, imu_path = _artifact(
        root,
        imu_block["artifact"],
        role="imu.samples",
        media_type="application/x-ndjson",
        label="IMU samples",
    )
    frame_records = _read_jsonl(frames_path, "frame index")
    imu_records = _read_jsonl(imu_path, "IMU samples")
    frames = _normalize_device_frames(
        frame_records,
        session_id=session_id,
        frame_decimation=frame_decimation,
        segments=tuple(segments),
    )
    imu_samples = _normalize_device_imu(imu_records, session_id=session_id)
    if len(frames) != _integer(frames_block.get("count"), "frames.count", minimum=1):
        raise CaptureValidationError("Device Session frame count differs from the manifest")
    if len(imu_samples) != _integer(imu_block.get("sample_count"), "imu.sample_count", minimum=1):
        raise CaptureValidationError("Device Session IMU count differs from the manifest")
    width = _integer(camera.get("width"), "camera.width", minimum=2)
    eye_width = _integer(camera.get("eye_width"), "camera.eye_width", minimum=1)
    if width != eye_width * 2:
        raise CaptureValidationError("stereo camera width must equal two eye widths")
    return LoadedCapture(
        root=root,
        source_schema=schema,
        session_id=session_id,
        manifest=manifest,
        frames=frames,
        imu_samples=imu_samples,
        video=CaptureVideo("device_session", segments=tuple(segments)),
        fps=_number(camera.get("nominal_fps"), "camera.nominal_fps", positive=True),
        width=width,
        eye_width=eye_width,
        height=_integer(camera.get("height"), "camera.height", minimum=1),
        imu_samples_per_packet=2,
        imu_timestamp_bits=None,
    )


def _normalize_legacy_frames(
    records: list[dict[str, Any]], *, video_bytes: int
) -> tuple[dict[str, Any], ...]:
    keys = {
        "frame_index",
        "uvc_sequence",
        "callback_monotonic_ns",
        "libuvc_capture_time_ns",
        "jpeg_offset",
        "jpeg_bytes",
    }
    expected_offset = 0
    previous_host = -1
    for index, record in enumerate(records):
        if set(record) != keys or record.get("frame_index") != index:
            raise CaptureValidationError(f"legacy frame {index} is not a closed contiguous record")
        _integer(
            record["uvc_sequence"],
            f"legacy frame[{index}].uvc_sequence",
            maximum=(1 << 32) - 1,
        )
        host = _integer(record["callback_monotonic_ns"], f"legacy frame[{index}].host")
        offset = _integer(record["jpeg_offset"], f"legacy frame[{index}].offset")
        size = _integer(record["jpeg_bytes"], f"legacy frame[{index}].bytes", minimum=1)
        _integer(record["libuvc_capture_time_ns"], f"legacy frame[{index}].capture_time")
        if host <= previous_host or offset != expected_offset:
            raise CaptureValidationError(f"legacy frame {index} timing or offset is invalid")
        expected_offset += size
        previous_host = host
    if expected_offset != video_bytes:
        raise CaptureValidationError("legacy frame index does not cover the video bytes")
    return tuple(records)


def _normalize_legacy_imu(records: list[dict[str, Any]]) -> tuple[dict[str, Any], ...]:
    keys = {
        "sample_number",
        "device_timestamp_raw",
        "sample_index",
        "samples_in_packet",
        "host_read_start_ns",
        "host_read_end_ns",
        "host_monotonic_ns",
        "accel_raw",
        "gyro_raw",
    }
    for index, record in enumerate(records):
        if set(record) != keys or record.get("sample_number") != index:
            raise CaptureValidationError(f"legacy IMU {index} is not a closed contiguous record")
        _integer(record["device_timestamp_raw"], f"legacy imu[{index}].timestamp")
        _integer(record["sample_index"], f"legacy imu[{index}].sample_index")
        _integer(record["samples_in_packet"], f"legacy imu[{index}].samples", minimum=1)
        start = _integer(record["host_read_start_ns"], f"legacy imu[{index}].read_start")
        end = _integer(record["host_read_end_ns"], f"legacy imu[{index}].read_end")
        host = _integer(record["host_monotonic_ns"], f"legacy imu[{index}].host")
        if end < start or not start <= host <= end:
            raise CaptureValidationError(f"legacy IMU {index} host interval is invalid")
        _raw_vector(record["accel_raw"], f"legacy imu[{index}].accel")
        _raw_vector(record["gyro_raw"], f"legacy imu[{index}].gyro")
    return tuple(records)


def _load_legacy(root: Path, manifest: dict[str, Any]) -> LoadedCapture:
    if manifest.get("schema") != "ylx.stereo_imu.raw.v2":
        raise CaptureValidationError("unsupported or missing legacy raw capture schema")
    summary = _mapping(manifest.get("summary"), "legacy summary")
    if summary.get("result") != 0:
        raise CaptureValidationError("legacy capture did not complete successfully")
    files = _mapping(manifest.get("files"), "legacy files")
    video_path = _resolve_file(root, files.get("video"), "legacy video")
    frames_path = _resolve_file(root, files.get("frames"), "legacy frames")
    imu_path = _resolve_file(root, files.get("imu"), "legacy IMU")
    frames = _normalize_legacy_frames(
        _read_jsonl(frames_path, "legacy frames"), video_bytes=video_path.stat().st_size
    )
    imu_samples = _normalize_legacy_imu(_read_jsonl(imu_path, "legacy IMU"))
    video = _mapping(manifest.get("video"), "legacy video config")
    imu = _mapping(manifest.get("imu"), "legacy IMU config")
    if len(frames) != _integer(video.get("requested_frames"), "video.requested_frames", minimum=1):
        raise CaptureValidationError("legacy frame count differs from the manifest")
    samples_per_packet = _integer(
        imu.get("samples_per_packet"), "imu.samples_per_packet", minimum=1
    )
    return LoadedCapture(
        root=root,
        source_schema="ylx.stereo_imu.raw.v2",
        session_id=None,
        manifest=manifest,
        frames=frames,
        imu_samples=imu_samples,
        video=CaptureVideo("legacy_raw", video_path),
        fps=_number(video.get("fps"), "video.fps", positive=True),
        width=_integer(video.get("eye_width"), "video.eye_width", minimum=1) * 2,
        eye_width=_integer(video.get("eye_width"), "video.eye_width", minimum=1),
        height=_integer(video.get("height"), "video.height", minimum=1),
        imu_samples_per_packet=samples_per_packet,
        imu_timestamp_bits=_integer(
            imu.get("device_timestamp_bits"), "imu.device_timestamp_bits", minimum=1, maximum=63
        ),
    )


def load_capture(path: str | Path) -> LoadedCapture:
    """Load one explicit legacy or Device Session layout and reject ambiguous inputs."""

    root = Path(path)
    _require_directory(root, "capture root")
    legacy_path = root / "capture.json"
    session_path = root / "manifest.json"
    legacy_exists = legacy_path.exists() or legacy_path.is_symlink()
    session_exists = session_path.exists() or session_path.is_symlink()
    if legacy_exists == session_exists:
        raise CaptureValidationError(
            "capture root must contain exactly one of capture.json or manifest.json"
        )
    if legacy_exists:
        return _load_legacy(root, _read_json_object(legacy_path, "legacy capture manifest"))
    return _load_device_session(root, _read_json_object(session_path, "Device Session manifest"))


def artifact_roles(capture: LoadedCapture) -> tuple[str, ...]:
    """Return the authoritative required roles without deriving them from filenames."""

    if capture.source_schema == "ylx.stereo_imu.raw.v2":
        return ("legacy.video", "legacy.frames", "legacy.imu")
    return ("video.left", "video.right", "frames.index", "imu.samples")
