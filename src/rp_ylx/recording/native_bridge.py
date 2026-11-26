"""Validation and sealing helpers for the Rust SessionTransaction data plane."""

from __future__ import annotations

import hashlib
import math
import os
from collections.abc import Collection, Mapping
from pathlib import Path, PurePosixPath

from rp_ylx.native import NativeRecordingPlan, NativeSessionTransaction
from rp_ylx.recording.manifest_builder import (
    DeviceRecordingError,
    DeviceSessionConfig,
    SessionPlan,
    recording_error,
)


def native_uint(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DeviceRecordingError("native_recording_sink_failed", f"{field} 必须是非负整数")
    return value


def native_segment_uint(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DeviceRecordingError(
            "native_recording_segment_planner_failed", f"{field} 必须是非负整数"
        )
    return value


def native_segment_float(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise DeviceRecordingError(
            "native_recording_segment_planner_failed", f"{field} 必须是有限数值"
        )
    if value < 0:
        raise DeviceRecordingError(
            "native_recording_segment_planner_failed", f"{field} 必须是非负数值"
        )
    return float(value)


def native_segment_path(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 1024 or "\\" in value:
        raise DeviceRecordingError("native_session_store_failed", f"{field} 路径无效")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise DeviceRecordingError("native_session_store_failed", f"{field} 路径无效")
    return value


def active_take_uint(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DeviceRecordingError("active_take_writer_failed", f"{field} 必须是非负整数")
    return value


def active_take_float(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise DeviceRecordingError("active_take_writer_failed", f"{field} 必须是有限数值")
    if value < 0:
        raise DeviceRecordingError("active_take_writer_failed", f"{field} 必须是非负数值")
    return float(value)


def active_take_drop_events(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise DeviceRecordingError(
            "active_take_writer_failed", "active take drop_events 必须是列表"
        )
    events: list[dict[str, object]] = []
    previous_end = 0
    for raw in value:
        if not isinstance(raw, Mapping):
            raise DeviceRecordingError(
                "active_take_writer_failed", "active take drop event 必须是对象"
            )
        start = active_take_uint(raw.get("start_frame"), "active_take.drop.start_frame")
        end = active_take_uint(raw.get("end_frame"), "active_take.drop.end_frame")
        dropped = active_take_uint(raw.get("dropped"), "active_take.drop.dropped")
        at_time = active_take_float(raw.get("at_time_seconds"), "active_take.drop.at_time_seconds")
        reason = raw.get("reason")
        if (
            not isinstance(reason, str)
            or reason != "write_backpressure"
            or end <= start
            or dropped != end - start
            or start < previous_end
        ):
            raise DeviceRecordingError(
                "active_take_writer_failed", "active take drop event 字段不一致"
            )
        events.append(
            {
                "start_frame": start,
                "end_frame": end,
                "at_time_seconds": at_time,
                "reason": reason,
                "dropped": dropped,
            }
        )
        previous_end = end
    return events


def seal_native_transaction(
    transaction: NativeSessionTransaction,
    partial: Path,
    final: Path,
    session_id: str,
    manifest_bytes: bytes,
    artifact_identities: Mapping[str, tuple[int, int, int, int]],
) -> str:
    """Seal a native SessionTransaction and verify the returned manifest digest."""
    try:
        result = transaction.seal(
            os.fspath(partial),
            os.fspath(final),
            session_id,
            manifest_bytes,
            dict(artifact_identities),
        )
    except BaseException as error:
        raise recording_error(error, "native_session_store_failed") from error
    if not isinstance(result, Mapping):
        raise DeviceRecordingError("native_session_store_failed", "原生会话封存结果无效")
    manifest_sha256 = result.get("manifest_sha256")
    artifact_count = result.get("artifact_count")
    manifest_size = result.get("manifest_bytes")
    if (
        not isinstance(manifest_sha256, str)
        or len(manifest_sha256) != 64
        or any(character not in "0123456789abcdef" for character in manifest_sha256)
        or type(artifact_count) is not int
        or artifact_count < 0
        or type(manifest_size) is not int
        or manifest_size != len(manifest_bytes)
    ):
        raise DeviceRecordingError("native_session_store_failed", "原生会话封存结果字段无效")
    expected_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    if manifest_sha256 != expected_sha256:
        raise DeviceRecordingError("native_session_store_failed", "原生 manifest 摘要不一致")
    return manifest_sha256


def build_native_recording_plan(
    *,
    partial: Path,
    config: DeviceSessionConfig,
    plan: SessionPlan,
    encoder_executable: Path,
    segment_frames: int,
    started_monotonic_ns: int,
) -> NativeRecordingPlan:
    """Construct the NativeRecordingPlan passed to `native_session_store().begin_recording()`."""
    return NativeRecordingPlan(
        session_root=str(partial),
        session_id=plan.session_id,
        encoder_executable=str(encoder_executable),
        width=config.width,
        height=config.height,
        fps=round(config.sensor_fps / config.frame_decimation),
        bitrate_kbps=config.video_bitrate_kbps,
        segment_frames=segment_frames,
        recording_start_monotonic_ns=started_monotonic_ns,
        audio_enabled=config.audio_enabled,
        audio_device=config.audio_device,
        audio_sample_rate_hz=config.audio_sample_rate_hz,
        audio_channels=config.audio_channels,
        audio_segment_seconds=config.segment_seconds,
        encoder_arguments=()
        if config.recording_encoding is None
        else config.recording_encoding.arguments(),
    )


def parse_active_take_snapshot(
    snapshot: Mapping[str, object],
    *,
    expected_session_id: str,
    expected_frames_written: int | None,
    queue_capacity: int,
) -> tuple[int, list[dict[str, object]]]:
    """Validate and extract `(frame_domain, drop_events)` from a native active_take snapshot."""
    session_id = snapshot.get("session_id")
    if session_id != expected_session_id:
        raise DeviceRecordingError("active_take_writer_failed", "active take 会话身份不一致")
    frame_domain = active_take_uint(snapshot.get("frame_domain"), "active_take.frame_domain")
    frames_written = active_take_uint(snapshot.get("frames_written"), "active_take.frames_written")
    pending_frames = active_take_uint(snapshot.get("pending_frames"), "active_take.pending_frames")
    drop_events = active_take_drop_events(snapshot.get("drop_events"))
    if expected_frames_written is not None and frames_written != expected_frames_written:
        raise DeviceRecordingError(
            "active_take_writer_failed",
            "active take 写入帧数与控制面不一致",
        )
    max_pending_frames = queue_capacity + 1
    if expected_frames_written is not None and pending_frames > max_pending_frames:
        raise DeviceRecordingError(
            "active_take_writer_failed",
            "active take pending 帧数超过录制队列容量",
        )
    return frame_domain, drop_events


def parse_session_transaction_sink(
    result: Mapping[str, object],
    *,
    expected_frames_written: int,
    valid_roles: Collection[str],
) -> tuple[
    int,
    int,
    dict[str, int],
    dict[str, dict[str, object]],
    dict[str, tuple[int, int, int, int]],
]:
    """Validate and extract counts, artifacts, and sealed identities from native sink output."""
    if not isinstance(result, Mapping):
        raise DeviceRecordingError("native_recording_sink_failed", "原生录制写入结果无效")
    artifacts = result.get("artifacts")
    if not isinstance(artifacts, Mapping):
        raise DeviceRecordingError("native_recording_sink_failed", "原生录制 artifact 结果无效")
    frames_written = native_uint(result.get("frames_written"), "recording_sink.frames_written")
    imu_written = native_uint(
        result.get("imu_samples_written"), "recording_sink.imu_samples_written"
    )
    bytes_written = native_uint(result.get("bytes_written"), "recording_sink.bytes_written")
    if frames_written != expected_frames_written:
        raise DeviceRecordingError(
            "native_session_store_failed", "SessionStore sink 与 active take 帧数不一致"
        )
    artifact_bytes: dict[str, int] = {}
    native_artifacts: dict[str, dict[str, object]] = {}
    artifact_identities: dict[str, tuple[int, int, int, int]] = {}
    for raw_role, raw_artifact in artifacts.items():
        role = str(raw_role)
        if not isinstance(raw_artifact, Mapping):
            raise DeviceRecordingError("native_recording_sink_failed", "原生 artifact 字段无效")
        relative = str(raw_artifact.get("path"))
        digest = str(raw_artifact.get("sha256"))
        size = native_uint(raw_artifact.get("bytes"), f"{role}.bytes")
        identity = raw_artifact.get("identity")
        if (
            role != raw_artifact.get("role")
            or role not in valid_roles
            or not relative
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
            or not isinstance(identity, Mapping)
        ):
            raise DeviceRecordingError("native_recording_sink_failed", "原生 artifact 身份无效")
        device = native_uint(identity.get("device"), f"{role}.identity.device")
        inode = native_uint(identity.get("inode"), f"{role}.identity.inode")
        identity_size = native_uint(identity.get("size"), f"{role}.identity.size")
        mtime_ns = native_uint(identity.get("mtime_ns"), f"{role}.identity.mtime_ns")
        if identity_size != size:
            raise DeviceRecordingError("native_recording_sink_failed", "原生 artifact 大小不一致")
        artifact_bytes[role] = size
        native_artifacts[role] = {
            "artifact_id": digest,
            "role": role,
            "path": relative,
            "bytes": size,
            "sha256": digest,
        }
        artifact_identities[relative] = (device, inode, identity_size, mtime_ns)
    return imu_written, bytes_written, artifact_bytes, native_artifacts, artifact_identities
