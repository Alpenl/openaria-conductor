"""Stateless Device Session manifest construction, artifact finalization, and shared models."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import stat
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from rp_ylx.api.downloads import ArtifactAccessError, validate_device_session_manifest
from rp_ylx.manifest_limits import MAX_MANIFEST_BYTES
from rp_ylx.native import native_session_store_or_none
from rp_ylx.recording.encoding import RecordingEncoding
from rp_ylx.recording.fs_safety import open_regular_at
from rp_ylx.recording.verified import VerifiedArtifact
from rp_ylx.recording_layout import manifest_identity

_UUID7_LOCK = threading.Lock()
_UUID7_MILLISECOND = 0
_UUID7_COUNTER = 0
_UNSET = object()


class DeviceRecordingError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        self.code = code
        self.message = message
        self.retryable = retryable
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True, slots=True)
class FinalizedArtifact:
    sha256: str
    bytes: int
    identity: tuple[int, int, int, int]


@dataclass(frozen=True, slots=True)
class DeviceSessionConfig:
    device_id: str
    device_label: str
    hardware_fingerprint: str
    platform: str
    software_version: str
    commit: str
    width: int
    height: int
    sensor_fps: float
    frame_decimation: int = 1
    timezone: str = "Asia/Shanghai"
    quality_policy_id: str = "rdk-x5-lossless-v1"
    max_contiguous_dropped_frames: int = 0
    max_total_dropped_frames: int = 0
    max_drop_fraction: float = 0.0
    drop_window_seconds: float = 1.0
    max_dropped_frames_per_window: int = 0
    video_layout: str = "split-eyes"
    video_bitrate_kbps: int = 8192
    recording_encoding: RecordingEncoding | None = None
    lossless_storage: bool = False
    # Bound stop-time hashing/compaction to a short tail. Closed segments are
    # independently verified during capture; GOP alignment is checked below.
    segment_seconds: float = 5.0
    audio_enabled: bool = False
    audio_device: str = "hw:CARD=D2UQ2,DEV=0"
    audio_sample_rate_hz: int = 48_000
    audio_channels: int = 2
    audio_sample_format: str = "S16_LE"

    def __post_init__(self) -> None:
        try:
            device_id = uuid.UUID(self.device_id)
            ZoneInfo(self.timezone)
        except (ValueError, KeyError) as error:
            raise ValueError("设备身份或时区无效") from error
        if (
            device_id.version != 4
            or str(device_id) != self.device_id
            or len(self.device_label) != 12
            or not self.device_label.startswith("YLX-")
            or any(character not in "0123456789ABCDEF" for character in self.device_label[4:])
            or len(self.hardware_fingerprint) != 71
            or not self.hardware_fingerprint.startswith("sha256:")
            or any(
                character not in "0123456789abcdef" for character in self.hardware_fingerprint[7:]
            )
            or not self.platform
            or not self.software_version
            or len(self.software_version) > 64
            or len(self.commit) not in {40, 64}
            or any(character not in "0123456789abcdef" for character in self.commit)
            or self.width <= 0
            or self.width % 2
            or self.height <= 0
            or self.sensor_fps <= 0
            or self.frame_decimation <= 0
            or self.quality_policy_id != "rdk-x5-lossless-v1"
            or self.max_contiguous_dropped_frames != 0
            or self.max_total_dropped_frames != 0
            or self.max_drop_fraction != 0
            or self.drop_window_seconds != 1.0
            or self.max_dropped_frames_per_window != 0
            or self.video_layout != "split-eyes"
            or self.video_bitrate_kbps <= 0
            or self.segment_seconds <= 0
            or type(self.audio_enabled) is not bool
            or (self.audio_enabled and not self.audio_device)
            or self.audio_sample_rate_hz <= 0
            or self.audio_channels <= 0
            or self.audio_sample_format != "S16_LE"
        ):
            raise ValueError("Device Session 配置无效")
        if self.recording_encoding is not None:
            if not isinstance(self.recording_encoding, RecordingEncoding):
                raise ValueError("recording_encoding must be RecordingEncoding")
            segment_frames = round(self.segment_seconds * self.sensor_fps / self.frame_decimation)
            if segment_frames % self.recording_encoding.gop_frames:
                raise ValueError("segment frames must be a multiple of recording GOP")


@dataclass(frozen=True, slots=True)
class SessionPlan:
    session_id: str
    volume_id: str
    generation_id: str
    capture_mode: str
    display_name: str
    take_id: str
    take_sequence: int
    continuation_of: str | None
    clock_provenance: dict[str, object] | None = None

    def __post_init__(self) -> None:
        if self.capture_mode not in {"production", "calibration"}:
            raise ValueError("capture_mode 无效")
        if not 1 <= len(self.display_name) <= 160 or self.take_sequence < 1:
            raise ValueError("会话显示名称或 take 序号无效")
        identities = {
            "session_id": (self.session_id, 7),
            "volume_id": (self.volume_id, 4),
            "generation_id": (self.generation_id, 4),
            "take_id": (self.take_id, 7),
        }
        if self.continuation_of is not None:
            identities["continuation_of"] = (self.continuation_of, 7)
        for name, (value, version) in identities.items():
            try:
                parsed = uuid.UUID(value)
            except ValueError as error:
                raise ValueError(f"{name} 不是规范 UUID") from error
            if parsed.version != version or str(parsed) != value:
                raise ValueError(f"{name} 不是规范 UUIDv{version}")
        if (self.take_sequence == 1) != (self.continuation_of is None):
            raise ValueError("take sequence 与 continuation_of 不一致")


@dataclass(frozen=True, slots=True)
class StorageStatus:
    remaining_bytes: int | None
    writable: bool
    status: str = "mounted"


@dataclass(frozen=True, slots=True)
class SealedDeviceSession:
    path: Path
    manifest: Mapping[str, object]
    manifest_bytes: bytes
    manifest_sha256: str
    verified_artifacts: Mapping[str, VerifiedArtifact] = field(default_factory=dict)


def uuid7() -> str:
    """生成进程内单调的规范 UUIDv7，兼容 Python 3.11。"""
    global _UUID7_COUNTER, _UUID7_MILLISECOND
    with _UUID7_LOCK:
        current = time.time_ns() // 1_000_000
        if current > _UUID7_MILLISECOND:
            _UUID7_MILLISECOND = current
            _UUID7_COUNTER = secrets.randbits(12)
        else:
            _UUID7_COUNTER = (_UUID7_COUNTER + 1) & 0xFFF
            if _UUID7_COUNTER == 0:
                _UUID7_MILLISECOND += 1
        random_tail = secrets.randbits(62)
        value = (
            ((_UUID7_MILLISECOND & ((1 << 48) - 1)) << 80)
            | (0x7 << 76)
            | (_UUID7_COUNTER << 64)
            | (0b10 << 62)
            | random_tail
        )
    return str(uuid.UUID(int=value))


def json_bytes(value: object, *, pretty: bool = False) -> bytes:
    """Encode JSON object to UTF-8 bytes with trailing newline."""
    rendered = (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
        if pretty
        else json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    return (rendered + "\n").encode()


def format_session_timestamp(value: datetime) -> str:
    """Format session datetime using microsecond ISO-8601 precision."""
    return value.isoformat(timespec="microseconds")


def recording_error(error: BaseException, fallback_code: str) -> DeviceRecordingError:
    """Convert an arbitrary exception into a coded DeviceRecordingError."""
    if isinstance(error, DeviceRecordingError):
        return error
    raw = str(error)
    code, separator, message = raw.partition(": ")
    if not separator or not code.replace("_", "").isalnum():
        code, message = fallback_code, raw
    return DeviceRecordingError(code, message)


def _strict_native_result_int(value: object, field: str, *, code: str) -> int:
    if type(value) is not int or value < 0:
        raise DeviceRecordingError(code, f"{field} 必须是非负整数")
    return value


def finalize_artifact(
    path: Path,
    expected_bytes: int | None,
    *,
    code: str,
    session_store: object = _UNSET,
) -> FinalizedArtifact:
    """Read, hash, and capture the sealed identity of an exclusive regular artifact."""
    if expected_bytes is not None and expected_bytes < 0:
        raise DeviceRecordingError(code, "artifact 声明大小无效")
    native = native_session_store_or_none() if session_store is _UNSET else session_store
    if native is not None and hasattr(native, "finalize_artifact"):
        try:
            result = native.finalize_artifact(os.fspath(path), expected_bytes)
        except BaseException as error:
            converted = recording_error(error, "native_session_io_failed")
            raise DeviceRecordingError(code, converted.message) from error
        if not isinstance(result, Mapping):
            raise DeviceRecordingError(code, "原生 artifact 封存结果无效")
        digest = result.get("sha256")
        identity = result.get("identity")
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
            or not isinstance(identity, Mapping)
        ):
            raise DeviceRecordingError(code, "原生 artifact 封存结果字段无效")
        size = _strict_native_result_int(identity.get("size"), "artifact.identity.size", code=code)
        if expected_bytes is not None and size != expected_bytes:
            raise DeviceRecordingError(code, "artifact 大小与声明不一致")
        return FinalizedArtifact(
            sha256=digest,
            bytes=size,
            identity=(
                _strict_native_result_int(
                    identity.get("device"), "artifact.identity.device", code=code
                ),
                _strict_native_result_int(
                    identity.get("inode"), "artifact.identity.inode", code=code
                ),
                size,
                _strict_native_result_int(
                    identity.get("modified_ns"), "artifact.identity.modified_ns", code=code
                ),
            ),
        )

    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise DeviceRecordingError(code, f"无法打开 artifact：{error}") from error
    try:
        stream = os.fdopen(descriptor, "rb")
    except BaseException:
        os.close(descriptor)
        raise
    with stream:
        before_stat = os.fstat(stream.fileno())
        if not stat.S_ISREG(before_stat.st_mode) or (
            os.name == "posix" and before_stat.st_nlink != 1
        ):
            raise DeviceRecordingError(code, "artifact 不是普通文件")
        if expected_bytes is not None and before_stat.st_size != expected_bytes:
            raise DeviceRecordingError(code, "artifact 大小与声明不一致")
        before = (
            before_stat.st_dev,
            before_stat.st_ino,
            before_stat.st_size,
            before_stat.st_mtime_ns,
        )
        digest = hashlib.sha256()
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
        after_stat = os.fstat(stream.fileno())
        after = (
            after_stat.st_dev,
            after_stat.st_ino,
            after_stat.st_size,
            after_stat.st_mtime_ns,
        )
        if not stat.S_ISREG(after_stat.st_mode) or after != before:
            raise DeviceRecordingError(code, "artifact 在封存校验期间发生变化")
        return FinalizedArtifact(
            sha256=digest.hexdigest(), bytes=after_stat.st_size, identity=after
        )


def manifest_artifacts(manifest: Mapping[str, object]) -> list[Mapping[str, object]]:
    """Collect all artifact descriptor mappings in a manifest."""
    descriptors: list[Mapping[str, object]] = []

    def collect(value: object) -> None:
        if isinstance(value, Mapping):
            if "artifact_id" in value:
                descriptors.append(value)
            else:
                for child in value.values():
                    collect(child)
        elif isinstance(value, list):
            for child in value:
                collect(child)

    collect(manifest)
    return descriptors


def artifact_path_and_bytes(
    descriptor: Mapping[str, object],
    *,
    code: str,
) -> tuple[str, int]:
    """Extract and validate (path, bytes) from an artifact descriptor."""
    path = descriptor.get("path")
    bytes_value = descriptor.get("bytes")
    if (
        not isinstance(path, str)
        or isinstance(bytes_value, bool)
        or not isinstance(bytes_value, int)
        or bytes_value < 0
    ):
        raise DeviceRecordingError(code, "manifest artifact 描述符无效")
    return path, bytes_value


def manifest_artifact_bytes_total(
    manifest: Mapping[str, object],
    *,
    code: str,
) -> int:
    """Return sum of declared byte counts across all artifacts in manifest."""
    total = 0
    for descriptor in manifest_artifacts(manifest):
        _, artifact_bytes = artifact_path_and_bytes(descriptor, code=code)
        total += artifact_bytes
    return total


def read_bounded_fd(
    descriptor: int,
    maximum_bytes: int,
    *,
    code: str,
    session_store: object = _UNSET,
) -> bytes:
    """Read up to maximum_bytes from an already-open descriptor."""
    native = native_session_store_or_none() if session_store is _UNSET else session_store
    if native is not None and hasattr(native, "read_bounded_fd"):
        try:
            payload = native.read_bounded_fd(descriptor, maximum_bytes)
        except AttributeError:
            payload = None
        except BaseException as error:
            converted = recording_error(error, "native_session_io_failed")
            raise DeviceRecordingError(code, converted.message) from error
        if payload is not None:
            if not isinstance(payload, bytes):
                raise DeviceRecordingError(code, "原生 bounded read 返回无效 payload")
            if len(payload) > maximum_bytes:
                raise DeviceRecordingError(code, "文件超过允许大小")
            return payload

    payload = bytearray()
    while len(payload) <= maximum_bytes:
        block = os.read(descriptor, min(1024 * 1024, maximum_bytes + 1 - len(payload)))
        if not block:
            break
        payload.extend(block)
    if len(payload) > maximum_bytes:
        raise DeviceRecordingError(code, "文件超过允许大小")
    return bytes(payload)


def inspect_device_session_directory(
    path: str | Path,
    *,
    expected_session_id: str | None = None,
    open_regular_fn: Callable[..., int] | None = None,
    read_bounded_fn: Callable[..., bytes] | None = None,
) -> tuple[Mapping[str, object], bytes]:
    """校验 sealed manifest 和 artifact 身份/大小，不读取 artifact 内容。"""
    root = Path(path)
    root_flags = (
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        root_fd = os.open(root, root_flags)
    except OSError as error:
        raise DeviceRecordingError("manifest_invalid", f"会话目录不可安全打开：{error}") from error

    def open_at(relative: str, *, code: str) -> int:
        if open_regular_fn is not None:
            return open_regular_fn(root_fd, relative, code=code)
        native = native_session_store_or_none()
        if native is not None and hasattr(native, "open_relative_regular"):
            try:
                descriptor = native.open_relative_regular(root_fd, relative)
            except BaseException as error:
                converted = recording_error(error, "native_session_io_failed")
                raise DeviceRecordingError(code, converted.message) from error
            if type(descriptor) is not int or descriptor < 0:
                raise DeviceRecordingError(code, "原生 artifact 打开返回无效 fd")
            return descriptor
        return open_regular_at(
            root_fd,
            relative,
            error_factory=lambda msg: DeviceRecordingError(code, msg),
        )

    read_fn = read_bounded_fn or read_bounded_fd
    try:
        manifest_fd = open_at("manifest.json", code="manifest_invalid")
        try:
            metadata = os.fstat(manifest_fd)
            if metadata.st_size > MAX_MANIFEST_BYTES:
                raise DeviceRecordingError("manifest_invalid", "manifest 超过允许大小")
            payload = read_fn(manifest_fd, MAX_MANIFEST_BYTES, code="manifest_invalid")
        finally:
            os.close(manifest_fd)
        manifest = json.loads(payload)
        selected_session_id = (
            manifest_identity(root, manifest)
            if expected_session_id is None
            else expected_session_id
        )
        if not isinstance(manifest, dict) or manifest.get("session_id") != selected_session_id:
            raise DeviceRecordingError("manifest_invalid", "manifest 会话身份无效")
        validate_device_session_manifest(manifest)

        for artifact in manifest_artifacts(manifest):
            relative, expected_bytes = artifact_path_and_bytes(artifact, code="artifact_invalid")
            artifact_fd = open_at(relative, code="artifact_invalid")
            try:
                if os.fstat(artifact_fd).st_size != expected_bytes:
                    raise DeviceRecordingError("artifact_invalid", "artifact 大小不匹配")
            finally:
                os.close(artifact_fd)
        return manifest, payload
    except ArtifactAccessError as error:
        raise DeviceRecordingError("manifest_invalid", str(error)) from error
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise DeviceRecordingError("manifest_invalid", str(error)) from error
    finally:
        os.close(root_fd)


def build_device_session_manifest(
    *,
    config: DeviceSessionConfig,
    plan: SessionPlan,
    started_at: datetime,
    ended_at: datetime,
    verified_at: datetime,
    sealed_at: datetime,
    duration: float,
    frames_written: int,
    imu_written: int,
    video_segments: Sequence[Mapping[str, object]],
    frames_artifact: Mapping[str, object],
    imu_artifact: Mapping[str, object],
    audio: Mapping[str, object] | None,
    logs: Sequence[Mapping[str, object]],
    drop_events: Sequence[Mapping[str, object]],
    throughput_bytes: int = 0,
    schema: str | None = None,
    manifest_id: str | None = None,
) -> dict[str, object]:
    """Build a Device Session manifest from finalized video/index/audio/log descriptors."""
    if not video_segments:
        raise DeviceRecordingError("no_frames", "没有可封存的成片分段")
    display_name = plan.display_name
    original_started_at = started_at
    # NTP may correct the RTC during capture. Keep wall-clock metadata
    # consistent with the monotonic duration, using the calibrated end.
    if plan.clock_provenance is not None:
        ended_at = started_at + timedelta(seconds=duration)
    elif ended_at.year >= 2020 and abs((ended_at - started_at).total_seconds() - duration) > 5:
        started_at = ended_at - timedelta(seconds=duration)
        if display_name == original_started_at.strftime("录制 %Y-%m-%d %H:%M:%S"):
            display_name = started_at.strftime("录制 %Y-%m-%d %H:%M:%S")
    video: dict[str, object] = {
        "layout": "split-eyes",
        "codec": "h264" if config.recording_encoding is None else config.recording_encoding.codec,
        "container": "mp4",
        "segments": [dict(segment) for segment in video_segments],
    }
    if config.recording_encoding is not None:
        video["encoding"] = config.recording_encoding.manifest()
    dropped = sum(int(event["dropped"]) for event in drop_events)
    nominal_fps = config.sensor_fps / config.frame_decimation
    effective_fps = 0.0 if duration == 0 else frames_written / duration
    resolved_schema = schema or (
        "ylx.device-session.v2" if config.recording_encoding is None else "ylx.device-session.v3"
    )
    if resolved_schema == "ylx.device-session.v4" and config.recording_encoding is None:
        raise DeviceRecordingError(
            "manifest_invalid", "ylx.device-session.v4 必须提供 recording_encoding"
        )
    if config.audio_enabled:
        if audio is None:
            raise DeviceRecordingError("audio_invalid", "音频结果缺失")
        audio_block: dict[str, object] = dict(audio)
    else:
        audio_block = {
            "state": "not_recorded",
            "requested_mode": "disabled",
            "resolved_mode": "disabled",
            "reason": "user_disabled",
        }
    return {
        "schema": resolved_schema,
        "manifest_id": manifest_id or uuid7(),
        "sealed": True,
        "sealed_at": format_session_timestamp(sealed_at),
        "session_id": plan.session_id,
        "volume_id": plan.volume_id,
        "capture_mode": plan.capture_mode,
        "display_name": display_name,
        "device": {
            "device_id": config.device_id,
            "device_label": config.device_label,
            "hardware_fingerprint": config.hardware_fingerprint,
            "platform": config.platform,
            "software_version": config.software_version,
            "commit": config.commit,
        },
        "time": {
            "started_at": format_session_timestamp(started_at),
            "ended_at": format_session_timestamp(ended_at),
            "timezone": config.timezone,
            "duration_seconds": duration,
            "duration_clock": "host_monotonic",
        },
        "take": {
            "take_id": plan.take_id,
            "sequence": plan.take_sequence,
            "continuation_of": plan.continuation_of,
        },
        "camera": {
            "width": config.width,
            "height": config.height,
            "eye_width": config.width // 2,
            "sensor_fps": config.sensor_fps,
            "frame_decimation": config.frame_decimation,
            "nominal_fps": nominal_fps,
            "effective_fps": effective_fps,
            "coordinate_frame": "opencv_optical",
        },
        "video": video,
        "imu": {
            "artifact": dict(imu_artifact),
            "sample_count": imu_written,
            "units": "raw_int16",
            "coordinate_frame": "raw_device_axes",
        },
        "frames": {"artifact": dict(frames_artifact), "count": frames_written},
        "logs": [dict(entry) for entry in logs],
        "integrity": {
            "verified_at": format_session_timestamp(verified_at),
            "dropped_frames": dropped,
            "drop_events": [dict(event) for event in drop_events],
            "quality_policy": {
                "policy_id": config.quality_policy_id,
                "max_contiguous_dropped_frames": config.max_contiguous_dropped_frames,
                "max_total_dropped_frames": config.max_total_dropped_frames,
                "max_drop_fraction": config.max_drop_fraction,
                "window_seconds": config.drop_window_seconds,
                "max_dropped_frames_per_window": config.max_dropped_frames_per_window,
            },
            "media_write_throughput_bytes_per_second": (
                0 if duration == 0 else int(throughput_bytes / duration)
            ),
            "fatal_errors": [],
        },
        "audio": audio_block,
    }
