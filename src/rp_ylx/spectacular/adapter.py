"""Strict legacy and Device Session inputs for the Spectacular calibration toolchain."""

from __future__ import annotations

import hashlib
import json
import math
import os
import stat
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Literal

from zstandard import ZstdError

from rp_ylx.frame_index import valid_frame_record
from rp_ylx.manifest_limits import MAX_MANIFEST_BYTES
from rp_ylx.recording.device_session import (
    DeviceRecordingError,
    validate_device_session_directory,
)
from rp_ylx.recording.fs_safety import file_identity
from rp_ylx.recording.storage import decoded_metadata
from rp_ylx.recording.verified import open_regular

MAX_NDJSON_LINE_BYTES = 1024 * 1024
MAX_NDJSON_RECORDS = 2_000_000
DEVICE_SESSION_SCHEMAS = frozenset(
    {"ylx.device-session.v2", "ylx.device-session.v3", "ylx.device-session.v4"}
)


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


class _VerifiedFdStream:
    """Wrap an already-open binary stream to bound size and verify SHA-256 on read."""

    def __init__(self, stream: BinaryIO, expected_bytes: int | None) -> None:
        self._stream = stream
        self._expected_bytes = expected_bytes
        self._digest = hashlib.sha256()
        self.bytes_read = 0

    def read(self, size: int = -1) -> bytes:
        block = self._stream.read(size)
        if block:
            self.bytes_read += len(block)
            if self._expected_bytes is not None and self.bytes_read > self._expected_bytes:
                raise CaptureValidationError("artifact grew beyond its declared byte size")
            self._digest.update(block)
        return block

    def readline(self, size: int = -1) -> bytes:
        line = self._stream.readline(size)
        if line:
            self.bytes_read += len(line)
            if self._expected_bytes is not None and self.bytes_read > self._expected_bytes:
                raise CaptureValidationError("artifact grew beyond its declared byte size")
            self._digest.update(line)
        return line

    def hexdigest(self) -> str:
        return self._digest.hexdigest()


def _open_pinned_root(root: Path) -> int:
    flags = (
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        fd = os.open(root, flags)
    except OSError as error:
        raise CaptureValidationError(f"missing capture root: {root}") from error
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISDIR(metadata.st_mode):
            raise CaptureValidationError(f"capture root is not a real directory: {root}")
        return fd
    except BaseException:
        os.close(fd)
        raise


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


def _open_pinned_relative(
    root_fd: int,
    root: Path,
    value: object,
    label: str,
    *,
    allow_empty: bool = False,
) -> tuple[Path, int, os.stat_result]:
    relative = _safe_relative(value, label)
    dir_flags = (
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    )
    file_flags = (
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    )
    root_dev = os.fstat(root_fd).st_dev
    current = os.dup(root_fd)
    try:
        for component in relative.parts[:-1]:
            try:
                nxt = os.open(component, dir_flags, dir_fd=current)
            except OSError as error:
                raise CaptureValidationError(f"missing {label} parent: {component}") from error
            os.close(current)
            current = nxt
            meta = os.fstat(current)
            if not stat.S_ISDIR(meta.st_mode) or meta.st_dev != root_dev:
                raise CaptureValidationError(f"{label} parent is not a safe directory")
        try:
            fd = os.open(relative.parts[-1], file_flags, dir_fd=current)
        except OSError as error:
            target = root.joinpath(*relative.parts)
            raise CaptureValidationError(f"missing {label}: {target}") from error
    finally:
        os.close(current)
    try:
        metadata = os.fstat(fd)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_dev != root_dev
            or (not allow_empty and metadata.st_size <= 0)
        ):
            raise CaptureValidationError(
                f"{label} is not an exclusive regular file: {root.joinpath(*relative.parts)}"
            )
        return root.joinpath(*relative.parts), fd, metadata
    except BaseException:
        os.close(fd)
        raise


def _read_json_from_fd(fd: int, label: str) -> tuple[dict[str, Any], bytes]:
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size <= 0:
            raise CaptureValidationError(f"{label} is not an exclusive regular file")
        if before.st_size > MAX_MANIFEST_BYTES:
            raise CaptureValidationError(f"{label} exceeds the bounded size")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            payload = stream.read(MAX_MANIFEST_BYTES + 1)
        after = os.fstat(fd)
        if len(payload) > MAX_MANIFEST_BYTES or file_identity(after) != file_identity(before):
            raise CaptureValidationError(f"{label} changed or grew during read")
        value = _loads_json(payload.decode("utf-8"), label)
    except CaptureValidationError:
        raise
    except (OSError, ValueError) as error:
        raise CaptureValidationError(f"cannot read {label}: {error}") from error
    return _mapping(value, label), payload


def _read_jsonl_fd(
    fd: int,
    path: Path,
    label: str,
    *,
    max_records: int = MAX_NDJSON_RECORDS,
    expected_bytes: int | None = None,
    expected_sha256: str | None = None,
    compressed: bool | None = None,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    try:
        before = os.fstat(fd)
        expected_id = file_identity(before)
        if expected_bytes is not None and before.st_size != expected_bytes:
            raise CaptureValidationError(f"{label} size differs from its descriptor")
        is_zstd = (path.suffix == ".zst") if compressed is None else compressed
        with os.fdopen(fd, "rb", closefd=False) as raw:
            verified = _VerifiedFdStream(raw, expected_bytes)
            if is_zstd:
                with decoded_metadata(verified) as stream:
                    records = _scan_jsonl_stream(stream, label, max_records=max_records)
                # Ensure any trailing compressed/EOF bytes are consumed before checking digest.
                while verified.read(65536):
                    pass
            else:
                records = _scan_jsonl_stream(verified, label, max_records=max_records)
        after = os.fstat(fd)
        if file_identity(after) != expected_id:
            raise CaptureValidationError(f"{label} changed during read")
        if expected_bytes is not None and verified.bytes_read != expected_bytes:
            raise CaptureValidationError(f"{label} size differs from its descriptor")
        if expected_sha256 is not None and verified.hexdigest() != expected_sha256:
            raise CaptureValidationError(f"{label} digest differs from its descriptor")
    except CaptureValidationError:
        raise
    except (OSError, ZstdError) as error:
        raise CaptureValidationError(f"cannot read {label}: {error}") from error
    if not records:
        raise CaptureValidationError(f"{label} is empty")
    return records


def _scan_jsonl_stream(
    stream: Any,
    label: str,
    *,
    max_records: int,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    line_number = 0
    while True:
        line = stream.readline(MAX_NDJSON_LINE_BYTES + 1)
        if not line:
            break
        line_number += 1
        if line_number > max_records:
            raise CaptureValidationError(f"{label} exceeds maximum record count ({max_records})")
        if len(line) > MAX_NDJSON_LINE_BYTES or not line.endswith(b"\n"):
            raise CaptureValidationError(f"{label}:{line_number} is overlong or lacks a newline")
        value = _loads_json(line, f"{label}:{line_number}")
        records.append(_mapping(value, f"{label}:{line_number}"))
    return records


def _read_jsonl(
    path: Path, label: str, *, max_records: int = MAX_NDJSON_RECORDS
) -> list[dict[str, Any]]:
    try:
        with open_regular(path) as stream:
            return _read_jsonl_fd(stream.fileno(), path, label, max_records=max_records)
    except (OSError, ZstdError) as error:
        raise CaptureValidationError(f"cannot read {label}: {error}") from error


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
    normalized: list[dict[str, Any]] = []
    previous_host = -1
    previous_source: int | None = None
    for index, record in enumerate(records):
        if not valid_frame_record(record):
            raise CaptureValidationError(f"Device Session frame {index} is not a closed record")
        if (
            record["schema"] not in {"ylx.frame-index.v1", "ylx.frame-index.v2"}
            or record["session_id"] != session_id
        ):
            raise CaptureValidationError(f"Device Session frame {index} identity is invalid")
        frame = _integer(record["frame"], f"frame[{index}].frame")
        source_sequence = _integer(
            record["source_sequence"],
            f"frame[{index}].source_sequence",
        )
        host = _integer(record["host_monotonic_ns"], f"frame[{index}].host_monotonic_ns")
        segment_index = _integer(record["segment_index"], f"frame[{index}].segment_index")
        segment_frame = _integer(record["segment_frame"], f"frame[{index}].segment_frame")
        if frame != segments[0].start_frame + index or host <= previous_host:
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
    if segments[-1].end_frame - segments[0].start_frame != len(records):
        raise CaptureValidationError(
            "Device Session frame index does not cover the declared video segments"
        )
    return tuple(normalized)


def _infer_samples_per_packet(
    records: list[dict[str, Any]], *, declared_samples_per_packet: int | None
) -> int:
    if declared_samples_per_packet is not None:
        return declared_samples_per_packet
    if not records:
        return 2
    first_packet = records[0].get("packet_sequence")
    count = 0
    for record in records:
        if record.get("packet_sequence") != first_packet:
            break
        count += 1
    return count if count >= 1 else 2


def _normalize_device_imu(
    records: list[dict[str, Any]],
    *,
    session_id: str,
    samples_per_packet: int | None = None,
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
    packet_size = _infer_samples_per_packet(records, declared_samples_per_packet=samples_per_packet)
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
        sample_index = _integer(
            record["sample_index"], f"imu[{index}].sample_index", maximum=packet_size - 1
        )
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
        quality = sync["quality"]
        if quality not in {"insufficient", "degraded", "good"}:
            raise CaptureValidationError(f"Device Session IMU {index} sync quality is invalid")
        if quality == "insufficient":
            if sync["offset_ns"] is not None or sync["residual_ns"] is not None:
                raise CaptureValidationError(
                    f"Device Session IMU {index} sync fields must be null for insufficient quality"
                )
        else:
            if type(sync["offset_ns"]) is not int or type(sync["residual_ns"]) is not int:
                raise CaptureValidationError(
                    f"Device Session IMU {index} sync offset_ns and residual_ns must be integers"
                )
            if sync["residual_ns"] < 0:
                raise CaptureValidationError(
                    f"Device Session IMU {index} sync.residual_ns must be non-negative"
                )
        normalized.append(
            {
                "sample_number": index,
                "sequence": sequence,
                "packet_sequence": packet_sequence,
                "sample_index": sample_index,
                "samples_in_packet": packet_size,
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


def _artifact_fd(
    root_fd: int,
    root: Path,
    value: object,
    *,
    role: str,
    media_type: str,
    label: str,
    allow_empty: bool = False,
) -> tuple[dict[str, Any], Path, int]:
    descriptor = _mapping(value, label)
    if set(descriptor) - {"storage_encoding"} != {
        "artifact_id",
        "role",
        "path",
        "media_type",
        "bytes",
        "sha256",
    }:
        raise CaptureValidationError(f"{label} descriptor is not closed")
    if (
        descriptor["role"] != role
        or (
            descriptor["media_type"] != media_type
            and not (
                media_type == "application/x-ndjson"
                and descriptor["media_type"] == "application/zstd"
            )
        )
        or descriptor["artifact_id"] != descriptor["sha256"]
    ):
        raise CaptureValidationError(f"{label} role or content identity is invalid")
    expected_bytes = _integer(descriptor["bytes"], f"{label}.bytes")
    path, fd, metadata = _open_pinned_relative(
        root_fd, root, descriptor["path"], label, allow_empty=allow_empty
    )
    if metadata.st_size != expected_bytes:
        os.close(fd)
        raise CaptureValidationError(f"{label} size differs from its descriptor")
    return descriptor, path, fd


def _load_device_session(root_fd: int, root: Path, manifest: dict[str, Any]) -> LoadedCapture:
    schema = manifest.get("schema")
    if schema not in DEVICE_SESSION_SCHEMAS:
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
    try:
        root_stat = os.fstat(root_fd)
        path_stat = root.lstat()
        if (root_stat.st_dev, root_stat.st_ino) != (path_stat.st_dev, path_stat.st_ino):
            raise CaptureValidationError("Device Session root changed during validation")
    except OSError as error:
        raise CaptureValidationError("Device Session root is inaccessible") from error

    raw_segments = video.get("segments")
    if not isinstance(raw_segments, list) or not raw_segments:
        raise CaptureValidationError("Device Session has no split-eye video segments")

    with ExitStack() as cleanup:
        segments: list[CaptureVideoSegment] = []
        expected_start: int | None = None
        for expected_index, raw_segment in enumerate(raw_segments):
            segment = _mapping(raw_segment, f"video segment {expected_index}")
            index = _integer(segment.get("index"), f"video segment {expected_index}.index")
            start_frame = _integer(
                segment.get("start_frame"), f"video segment {expected_index}.start_frame"
            )
            end_frame = _integer(
                segment.get("end_frame"), f"video segment {expected_index}.end_frame", minimum=1
            )
            if expected_start is None:
                expected_start = start_frame
            if index != expected_index or start_frame != expected_start or end_frame <= start_frame:
                raise CaptureValidationError(
                    "Device Session video segment frame domain is not contiguous"
                )
            artifacts = _mapping(
                segment.get("artifacts"), f"video segment {expected_index}.artifacts"
            )
            _, left_path, left_fd = _artifact_fd(
                root_fd,
                root,
                artifacts.get("left"),
                role="video.left",
                media_type="video/mp4",
                label=f"video segment {expected_index} left",
            )
            os.close(left_fd)
            _, right_path, right_fd = _artifact_fd(
                root_fd,
                root,
                artifacts.get("right"),
                role="video.right",
                media_type="video/mp4",
                label=f"video segment {expected_index} right",
            )
            os.close(right_fd)
            segments.append(
                CaptureVideoSegment(index, start_frame, end_frame, left_path, right_path)
            )
            expected_start = end_frame

        frames_descriptor, frames_path, frames_fd = _artifact_fd(
            root_fd,
            root,
            frames_block["artifact"],
            role="frames.index",
            media_type="application/x-ndjson",
            label="frame index",
        )
        cleanup.callback(os.close, frames_fd)
        imu_descriptor, imu_path, imu_fd = _artifact_fd(
            root_fd,
            root,
            imu_block["artifact"],
            role="imu.samples",
            media_type="application/x-ndjson",
            label="IMU samples",
        )
        cleanup.callback(os.close, imu_fd)

        expected_frame_count = _integer(frames_block.get("count"), "frames.count", minimum=1)
        expected_imu_count = _integer(imu_block.get("sample_count"), "imu.sample_count", minimum=1)
        frame_records = _read_jsonl_fd(
            frames_fd,
            frames_path,
            "frame index",
            max_records=min(MAX_NDJSON_RECORDS, expected_frame_count),
            expected_bytes=int(frames_descriptor["bytes"]),
            expected_sha256=str(frames_descriptor["sha256"]),
            compressed=frames_descriptor["media_type"] == "application/zstd",
        )
        imu_records = _read_jsonl_fd(
            imu_fd,
            imu_path,
            "IMU samples",
            max_records=min(MAX_NDJSON_RECORDS, expected_imu_count),
            expected_bytes=int(imu_descriptor["bytes"]),
            expected_sha256=str(imu_descriptor["sha256"]),
            compressed=imu_descriptor["media_type"] == "application/zstd",
        )

    frames = _normalize_device_frames(
        frame_records,
        session_id=session_id,
        frame_decimation=frame_decimation,
        segments=tuple(segments),
    )
    declared_spp_raw = imu_block.get("samples_per_packet")
    declared_spp = (
        _integer(declared_spp_raw, "imu.samples_per_packet", minimum=1)
        if declared_spp_raw is not None
        else None
    )
    imu_samples = _normalize_device_imu(
        imu_records,
        session_id=session_id,
        samples_per_packet=declared_spp,
    )
    if len(frames) != expected_frame_count:
        raise CaptureValidationError("Device Session frame count differs from the manifest")
    if len(imu_samples) != expected_imu_count:
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
        imu_samples_per_packet=int(imu_samples[0]["samples_in_packet"]),
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


def _load_legacy(root_fd: int, root: Path, manifest: dict[str, Any]) -> LoadedCapture:
    if manifest.get("schema") != "ylx.stereo_imu.raw.v2":
        raise CaptureValidationError("unsupported or missing legacy raw capture schema")
    summary = _mapping(manifest.get("summary"), "legacy summary")
    if summary.get("result") != 0:
        raise CaptureValidationError("legacy capture did not complete successfully")
    files = _mapping(manifest.get("files"), "legacy files")
    with ExitStack() as cleanup:
        video_path, video_fd, video_stat = _open_pinned_relative(
            root_fd, root, files.get("video"), "legacy video"
        )
        cleanup.callback(os.close, video_fd)
        frames_path, frames_fd, _ = _open_pinned_relative(
            root_fd, root, files.get("frames"), "legacy frames"
        )
        cleanup.callback(os.close, frames_fd)
        imu_path, imu_fd, _ = _open_pinned_relative(root_fd, root, files.get("imu"), "legacy IMU")
        cleanup.callback(os.close, imu_fd)
        frames = _normalize_legacy_frames(
            _read_jsonl_fd(frames_fd, frames_path, "legacy frames"),
            video_bytes=video_stat.st_size,
        )
        imu_samples = _normalize_legacy_imu(_read_jsonl_fd(imu_fd, imu_path, "legacy IMU"))
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


def _pinned_entry_exists(root_fd: int, name: str) -> bool:
    try:
        os.stat(name, dir_fd=root_fd, follow_symlinks=False)
        return True
    except FileNotFoundError:
        return False
    except OSError as error:
        raise CaptureValidationError(f"cannot inspect {name}: {error}") from error


def load_capture(path: str | Path) -> LoadedCapture:
    """Load one explicit legacy or Device Session layout and reject ambiguous inputs."""

    root = Path(path)
    root_fd = _open_pinned_root(root)
    try:
        legacy_exists = _pinned_entry_exists(root_fd, "capture.json")
        session_exists = _pinned_entry_exists(root_fd, "manifest.json")
        if legacy_exists == session_exists:
            raise CaptureValidationError(
                "capture root must contain exactly one of capture.json or manifest.json"
            )
        file_flags = (
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        )
        name = "capture.json" if legacy_exists else "manifest.json"
        label = "legacy capture manifest" if legacy_exists else "Device Session manifest"
        try:
            manifest_fd = os.open(name, file_flags, dir_fd=root_fd)
        except OSError as error:
            raise CaptureValidationError(f"cannot open {label}: {error}") from error
        try:
            manifest, _ = _read_json_from_fd(manifest_fd, label)
        finally:
            os.close(manifest_fd)
        if legacy_exists:
            return _load_legacy(root_fd, root, manifest)
        return _load_device_session(root_fd, root, manifest)
    finally:
        os.close(root_fd)


def artifact_roles(capture: LoadedCapture) -> tuple[str, ...]:
    """Return the authoritative required roles without deriving them from filenames."""

    if capture.source_schema == "ylx.stereo_imu.raw.v2":
        return ("legacy.video", "legacy.frames", "legacy.imu")
    return ("video.left", "video.right", "frames.index", "imu.samples")
