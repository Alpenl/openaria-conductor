"""Publish the verified, independently committed prefix of an interrupted take.

Original media and indexes remain in place, including an unusable trailing
segment. Only derived indexes and the recovery receipt are written. Publication
uses the normal manifest and atomic directory rename, so readers need no special
download or export path. A retry may safely repeat every pre-publication step.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import stat
import uuid
import wave
from collections.abc import Iterable
from contextlib import contextmanager, suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from rp_ylx.api.events import validate_recording_state
from rp_ylx.audio_clock import (
    audio_clock_report,
    audio_sample_time_offset_ns,
    raw_audio_clock_model,
)
from rp_ylx.operational_logging import operational_logger
from rp_ylx.recording.audio_journal import SCHEMA as AUDIO_JOURNAL_SCHEMA
from rp_ylx.recording.audio_journal import AudioJournalReader
from rp_ylx.recording.encoding import RecordingEncoding
from rp_ylx.recording.fs_safety import (
    copy_regular_atomic,
    file_identity,
    fsync_directory,
    is_exfat_or_msdos_path,
    write_json_atomic,
)
from rp_ylx.recording.manifest_builder import (
    DeviceRecordingError,
    DeviceSessionConfig,
    SealedDeviceSession,
    SessionPlan,
    build_device_session_manifest,
    finalize_artifact,
    inspect_device_session_directory,
    json_bytes,
)
from rp_ylx.recording.recovery_index import RecoveryIndex
from rp_ylx.recording.verified import VerifiedArtifact
from rp_ylx.recording.verified import identity as verified_identity

_finalize_artifact = finalize_artifact
_copy_regular_atomic = copy_regular_atomic


RECOVERY_ROLE = "log.recording-recovery"
MAX_RECOVERY_METADATA_BYTES = 8 * 1024 * 1024
# Match the live capture audit limit, including the terminating newline.
_MAX_JOURNAL_LINE_BYTES = 1024 * 1024


def _journal_lines(stream):
    """Read a stopped writer's complete prefix without allocating a torn tail.

    Never skip a bad row and resume later: only a contiguous prefix can be
    recovered. The source journal is retained unchanged for diagnosis.
    """
    while True:
        line = stream.readline(_MAX_JOURNAL_LINE_BYTES + 1)
        if len(line) > _MAX_JOURNAL_LINE_BYTES or not line.endswith(b"\n"):
            return
        yield line


def _regular(root: Path, relative: str) -> Path:
    path = Path(relative)
    if path.is_absolute() or not path.parts or any(p in {".", ".."} for p in path.parts):
        raise ValueError("unsafe recovery artifact path")
    current = root
    for part in path.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError("symlink in recovery artifact path")
    if not stat.S_ISREG(current.stat(follow_symlinks=False).st_mode):
        raise ValueError("recovery artifact is not a regular file")
    return current


def _read_json(root: Path, relative: str) -> dict:
    path = _regular(root, relative)
    if path.stat().st_size > MAX_RECOVERY_METADATA_BYTES:
        raise ValueError("recovery metadata is too large")
    try:
        value = json.loads(path.read_bytes())
    except RecursionError as error:
        raise ValueError("recovery metadata nesting exceeds parser limit") from error
    if not isinstance(value, dict):
        raise ValueError("invalid recovery metadata")
    return value


def _artifact(
    root: Path,
    relative: str,
    role: str,
    media_type: str,
    *,
    verified: dict[str, VerifiedArtifact] | None = None,
) -> dict:
    path = _regular(root, relative)
    exfat = is_exfat_or_msdos_path(path)
    before = None
    if verified is not None:
        before = (
            file_identity(path.stat(follow_symlinks=False), include_inode=False)
            if exfat
            else verified_identity(path.stat(follow_symlinks=False))
        )
    finalized = _finalize_artifact(path, None, code="recovery_invalid")
    if verified is not None:
        assert before is not None
        after = (
            file_identity(path.stat(follow_symlinks=False), include_inode=False)
            if exfat
            else verified_identity(path.stat(follow_symlinks=False))
        )
        if after != before:
            raise ValueError("recovery artifact changed during independent readback")
        verified[relative] = VerifiedArtifact(before, finalized.sha256, include_inode=not exfat)
    return {
        "artifact_id": finalized.sha256,
        "sha256": finalized.sha256,
        "bytes": finalized.bytes,
        "path": relative,
        "role": role,
        "media_type": media_type,
    }


@contextmanager
def _temporary_output(path: Path):
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
        0o640,
    )
    try:
        stream = os.fdopen(descriptor, "wb")
    except BaseException:
        os.close(descriptor)
        raise
    with stream:
        yield stream


def _write_records(
    path: Path, records: Iterable[dict], *, storage_encoding: dict | None = None
) -> int:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    writer = RecoveryIndex(storage_encoding is not None)
    try:
        with _temporary_output(temporary) as stream:
            count = 0
            for record in records:
                try:
                    payload = json_bytes(record)
                except UnicodeEncodeError:
                    # Escaped lone surrogates parse as JSON but cannot be
                    # written to our UTF-8 index. End the recoverable IMU
                    # prefix here; never skip corruption or lose valid video.
                    break
                writer.write(stream, payload)
                count += 1
            writer.finish(stream)
            stream.flush()
            os.fsync(stream.fileno())
        encoding = writer.verify(temporary)
        os.replace(temporary, path)
        fsync_directory(path.parent)
        if storage_encoding is not None:
            storage_encoding.update(encoding)
        return count
    finally:
        # Long IMU indexes can be gigabytes. Failed retries must not accumulate
        # private copies, and only a complete durable index replaces the prior one.
        with suppress(OSError):
            temporary.unlink(missing_ok=True)


@dataclass(frozen=True)
class _RecoveredFrames:
    count: int
    first_monotonic_ns: int
    last_monotonic_ns: int
    boundary_monotonic_ns: dict[int, int]
    path: str = "recovery/frames.ndjson"
    storage_encoding: dict | None = None


def _frames(
    root: Path,
    plan: SessionPlan,
    segments: list[dict],
    decimation: int,
    origin: int,
    *,
    compressed: bool = False,
) -> _RecoveredFrames | None:
    """Stream a canonical index, rolling back any incomplete final segment.

    Keep a byte checkpoint only after an entire independently committed pair
    has its index. Original journals survive failures and only complete prefixes
    replace the derived index. Memory grows with segment boundaries, not frames.
    """
    segment_index = 0
    count = committed_count = 0
    checkpoint = None
    first = last_committed = segment_first = None
    previous = None
    serialization_error = None
    boundaries: dict[int, int] = {}
    temporary = root / f".recovery-frames.{uuid.uuid4().hex}.tmp"
    writer = RecoveryIndex(compressed)
    try:
        with _temporary_output(temporary) as output:
            with _regular(root, "frames.ndjson").open("rb") as stream:
                for line in _journal_lines(stream):
                    if segment_index >= len(segments):
                        break
                    try:
                        row = json.loads(line)
                        segment = segments[segment_index]
                        if (
                            not line.endswith(b"\n")
                            or row["session_id"] != plan.session_id
                            or row["schema"] not in {"ylx.frame-index.v1", "ylx.frame-index.v2"}
                            or row["frame"] != count
                            or row["segment_index"] != segment_index
                            or row["segment_frame"] != count - segment["start_ordinal"]
                            or type(row["host_monotonic_ns"]) is not int
                            or row["host_monotonic_ns"] <= 0
                            or (
                                previous is not None
                                and (
                                    row["source_sequence"] - previous["source_sequence"]
                                    != decimation
                                    or row["host_monotonic_ns"] <= previous["host_monotonic_ns"]
                                )
                            )
                        ):
                            break
                    except (ValueError, KeyError, TypeError, RecursionError):
                        break
                    if first is None:
                        first = row["host_monotonic_ns"]
                    if count == segment["start_ordinal"]:
                        segment_first = row["host_monotonic_ns"]
                    # JSON can parse escaped lone surrogates that UTF-8 cannot
                    # encode. Incomplete segments were previously discarded
                    # before serialization, so defer this failure until the
                    # segment is known to be retained.
                    if serialization_error is None:
                        try:
                            payload = json_bytes(row)
                        except UnicodeEncodeError as error:
                            serialization_error = error
                        else:
                            writer.write(output, payload)
                    count += 1
                    previous = row
                    if count == segment["end_ordinal"]:
                        if serialization_error is not None:
                            raise serialization_error
                        committed_count = count
                        checkpoint = writer.checkpoint(output)
                        last_committed = row["host_monotonic_ns"]
                        boundaries[segment["start_ordinal"]] = segment_first
                        segment_index += 1
                        if segment_index == len(segments):
                            break
            del segments[segment_index:]
            if not committed_count:
                return None
            if type(origin) is not int or not 0 < origin <= first:
                raise ValueError("invalid capture clock origin")
            writer.rollback(output, checkpoint)
            writer.finish(output)
            output.flush()
            os.fsync(output.fileno())
        encoding = writer.verify(temporary)
        recovery = root / "recovery"
        if recovery.is_symlink():
            raise ValueError("unsafe recovery directory")
        recovery.mkdir(exist_ok=True, mode=0o750)
        relative = "recovery/frames.ndjson" + (".zst" if compressed else "")
        os.replace(temporary, root / relative)
        fsync_directory(recovery)
        return _RecoveredFrames(
            committed_count, first, last_committed, boundaries, relative, encoding
        )
    finally:
        with suppress(OSError):
            temporary.unlink(missing_ok=True)


def _audio(
    root: Path,
    config: DeviceSessionConfig,
    origin: int,
    video_end: float,
    *,
    verified: dict[str, VerifiedArtifact] | None = None,
) -> dict | None:
    """Use sealed PCM and hardware clock observations; never invent silence."""
    if not config.audio_enabled:
        return None
    try:
        clock = _read_json(root, "audio/checkpoint.json")
    except (OSError, ValueError):
        return None
    if clock.get("schema") == AUDIO_JOURNAL_SCHEMA:
        clock = AudioJournalReader(root).read(clock)
    rate, channels = config.audio_sample_rate_hz, config.audio_channels
    anchors = clock.get("anchors", [])
    raw_clock = clock.get("schema") == "openaria.audio-checkpoint.v2"
    if clock.get("schema") not in (
        None,
        "openaria.audio-checkpoint.v1",
        "openaria.audio-checkpoint.v2",
    ):
        raise ValueError("unsupported audio checkpoint")
    if len(anchors) < 2 or any(
        b[0] <= a[0] or b[1] <= a[1] for a, b in zip(anchors, anchors[1:], strict=False)
    ):
        return None
    if raw_clock:
        # The capture thread may be ahead of the last durable PCM segment.
        # Validate its observation range, then limit recovery to durable samples
        # and the observed video boundary using the piecewise host timeline.
        model = raw_audio_clock_model(
            anchors, rate, anchors[-1][0], clock["period_frames"], clock["buffer_frames"]
        )
        low, high = 0, min(clock["sample_count"], anchors[-1][0])
        boundary_offset_ns = video_end * 1e9
        while low < high:
            middle = (low + high + 1) // 2
            if audio_sample_time_offset_ns(model["points"], middle, origin) <= boundary_offset_ns:
                low = middle
            else:
                high = middle - 1
        target = low
    else:
        first, last = anchors[0], anchors[-1]
        slope = (last[1] - first[1]) / (last[0] - first[0])
        start_offset = (first[1] - origin) - first[0] * slope
        target = min(clock["sample_count"], math.floor((video_end * 1e9 - start_offset) / slope))
        if abs(1e9 / slope / rate - 1) > 0.01:
            return None
    if target <= 0:
        return None
    records = []
    samples = 0
    for item in clock["segments"]:
        if samples >= target:
            break
        relative = item["path"]
        path = _regular(root, relative)
        with wave.open(str(path), "rb") as stream:
            count = stream.getnframes()
            if (stream.getnchannels(), stream.getsampwidth(), stream.getframerate()) != (
                channels,
                2,
                rate,
            ):
                break
            if item["start_sample"] != samples or item["end_sample"] != samples + count:
                break
            if path.stat().st_size != 44 + count * channels * 2:
                break
            selected = min(count, target - samples)
            if selected != count:
                recovery_dir = root / "recovery"
                if recovery_dir.is_symlink():
                    raise ValueError("unsafe recovery directory")
                recovery_dir.mkdir(exist_ok=True, mode=0o750)
                relative = "recovery/audio-tail.wav"
                if (root / relative).is_symlink():
                    raise ValueError("unsafe recovery audio path")
                temporary = recovery_dir / f".audio-tail.{uuid.uuid4().hex}.tmp"
                replaced = False
                try:
                    with _temporary_output(temporary) as out_stream:
                        with wave.open(out_stream, "wb") as output:
                            output.setparams(stream.getparams())
                            remaining = selected
                            while remaining:
                                take = min(remaining, rate)
                                payload = stream.readframes(take)
                                if len(payload) != take * channels * 2:
                                    raise ValueError("truncated recovered audio")
                                output.writeframesraw(payload)
                                remaining -= take
                        out_stream.flush()
                        os.fsync(out_stream.fileno())
                    os.replace(temporary, root / relative)
                    replaced = True
                    fsync_directory(recovery_dir)
                finally:
                    if not replaced:
                        with suppress(OSError):
                            temporary.unlink(missing_ok=True)
            records.append(
                {
                    "index": len(records),
                    "start_sample": samples,
                    "end_sample": samples + selected,
                    "start_time_seconds": samples / rate,
                    "end_time_seconds": (samples + selected) / rate,
                    "pcm_payload_bytes": selected * channels * 2,
                    "wav_header_bytes": 44,
                    "artifact": _artifact(
                        root, relative, "audio.wav", "audio/wav", verified=verified
                    ),
                }
            )
            samples += selected
    if not records:
        return None
    # Keep actual ALSA observations through the first one covering the prefix.
    selected_anchors = []
    for anchor in anchors:
        selected_anchors.append(anchor)
        if anchor[0] >= samples:
            break
    if len(selected_anchors) < 2 or selected_anchors[-1][0] < samples:
        return None
    if raw_clock:
        model = raw_audio_clock_model(
            selected_anchors, rate, samples, clock["period_frames"], clock["buffer_frames"]
        )
        start_offset_ns = audio_sample_time_offset_ns(model["points"], 0, origin)
        end_offset_ns = audio_sample_time_offset_ns(model["points"], samples, origin)
        observed_stop = model["points"][-1][1]
        residual = model["max_residual_ns"]
    else:
        first, last = selected_anchors[0], selected_anchors[-1]
        slope = (last[1] - first[1]) / (last[0] - first[0])
        start_offset_ns = (first[1] - origin) - first[0] * slope
        end_offset_ns = start_offset_ns + samples * slope
        observed_stop = last[1]
        residual = max(abs((t - first[1]) - (n - first[0]) * slope) for n, t in selected_anchors)
    sample_start_ns = origin + round(start_offset_ns)
    sample_end_ns = origin + round(end_offset_ns)
    proof = {
        "schema": "openaria.audio-clock.v2" if raw_clock else "openaria.audio-clock.v1",
        "clock": "host_monotonic",
        "timestamp_source": "alsa_htimestamp_dma_raw" if raw_clock else "alsa_htimestamp_dma",
        "continuity": "verified",
        "device": config.audio_device,
        "period_frames": clock["period_frames"],
        "buffer_frames": clock["buffer_frames"],
        "anchors": selected_anchors,
        "thread_started_monotonic_ns": clock["thread_started_monotonic_ns"],
        # This is the observed end of the recovered capture window. The full
        # checkpoint keeps the original, longer capture window for diagnosis.
        "thread_stopped_monotonic_ns": max(observed_stop, origin + math.ceil(end_offset_ns)),
        "sample_start_monotonic_ns": sample_start_ns,
        "sample_end_monotonic_ns": sample_end_ns,
        "session_start_monotonic_ns": origin,
        "max_residual_ns": math.ceil(residual),
        "queue_capacity_frames": clock["queue_capacity_frames"],
        "queue_peak_frames": clock["queue_peak_frames"],
        "max_write_ns": clock["max_write_ns"],
        "xrun_count": 0,
        "suspend_count": 0,
    }
    return {
        "state": "recorded",
        "requested_mode": "enabled",
        "resolved_mode": "enabled",
        "codec": "pcm_s16le",
        "container": "wav",
        "sample_format": "S16_LE",
        "sample_rate": rate,
        "channels": channels,
        "sample_count": samples,
        "sync": {
            "time_base": "host_monotonic",
            "video_time_reference": "session_time_seconds",
            "start_time_seconds": (sample_start_ns - origin) / 1e9,
            "end_time_seconds": (sample_end_ns - origin) / 1e9,
        },
        "segments": records,
        "capture_clock": proof,
    }


def recover_device_session(partial: Path) -> SealedDeviceSession | None:
    """Recover a stopped writer; callers must first release all media handles."""
    if partial.is_symlink() or not partial.is_dir():
        return None
    capture = _read_json(partial, "capture.json")
    checkpoint = _read_json(partial, "segments.json")
    config_values = dict(capture["config"])
    if config_values.get("recording_encoding") is not None:
        config_values["recording_encoding"] = RecordingEncoding(
            **config_values["recording_encoding"]
        )
    config = DeviceSessionConfig(**config_values)
    plan = SessionPlan(**capture["plan"])
    clock = None
    if (partial / "clock-calibration.json").exists():
        clock = _read_json(partial, "clock-calibration.json")
        if (
            clock.get("schema") != "openaria.recording-clock.v1"
            or clock.get("session_id") != plan.session_id
            or clock.get("started_at") != capture["started_at"]
            or not isinstance(clock.get("calibration"), dict)
        ):
            raise ValueError("invalid recording clock metadata")
        plan = replace(plan, clock_provenance=clock["calibration"])
    if (
        partial.name != f"{plan.session_id}.partial"
        or capture["session_id"] != plan.session_id
        or checkpoint.get("schema") != "openaria.segment-checkpoint.v1"
        or checkpoint.get("session_id") != plan.session_id
    ):
        raise ValueError("recovery session identity mismatch")
    final = partial.parent / plan.session_id
    if final.exists():
        raise ValueError("recovery target already exists")
    # Every proof starts with an independent full read in this invocation. Never
    # trust a checkpoint's digest or a prior attempt as proof of current bytes.
    verified: dict[str, VerifiedArtifact] = {}
    segments = []
    end = 0
    for record in checkpoint["segments"]:
        try:
            if (
                record["index"] != len(segments)
                or record["start_ordinal"] != end
                or type(record["end_ordinal"]) is not int
                or record["end_ordinal"] <= end
            ):
                break
            for eye in ("left", "right"):
                descriptor = record["artifacts"][eye]
                actual = _artifact(
                    partial, descriptor["path"], f"video.{eye}", "video/mp4", verified=verified
                )
                if actual != descriptor:
                    raise ValueError("committed segment digest mismatch")
            segments.append(record)
            end = record["end_ordinal"]
        except (OSError, ValueError, KeyError, TypeError, DeviceRecordingError):
            break
    # v4 requires explicit encoder settings. Legacy captures without them keep
    # their original manifest contract; never invent settings to enable compression.
    compress_indexes = config.lossless_storage and config.recording_encoding is not None
    origin = capture["started_monotonic_ns"]
    frames = _frames(
        partial, plan, segments, config.frame_decimation, origin, compressed=compress_indexes
    )
    if frames is None:
        return None
    video_end = (
        frames.last_monotonic_ns - origin
    ) / 1e9 + config.frame_decimation / config.sensor_fps
    recovery = partial / "recovery"

    def imu_prefix():
        # IMU data can be much larger than the frame index on long takes.
        # Stream it through recovery so memory does not grow with sample count.
        with _regular(partial, "imu.ndjson").open("rb") as stream:
            for line in _journal_lines(stream):
                try:
                    sample = json.loads(line)
                    timestamp = sample["host_monotonic_ns"]
                    if (
                        not line.endswith(b"\n")
                        or sample["session_id"] != plan.session_id
                        or type(timestamp) is not int
                        or timestamp <= 0
                    ):
                        break
                    if origin <= timestamp <= origin + video_end * 1e9:
                        yield sample
                except (ValueError, KeyError, TypeError, RecursionError):
                    break

    imu_relative = "recovery/imu.ndjson" + (".zst" if compress_indexes else "")
    imu_encoding = {} if compress_indexes else None
    imu_count = _write_records(partial / imu_relative, imu_prefix(), storage_encoding=imu_encoding)
    audio_error = None
    try:
        audio = _audio(partial, config, origin, video_end, verified=verified)
        if audio is not None:
            audio_clock_report(
                audio,
                max(
                    video_end,
                    (audio["capture_clock"]["thread_stopped_monotonic_ns"] - origin) / 1e9,
                ),
            )
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        RecursionError,
        wave.Error,
        DeviceRecordingError,
    ) as error:
        # Bad/missing audio never invalidates already committed video pairs.
        audio = None
        audio_error = str(error)
    duration = max(
        video_end,
        0
        if audio is None
        else (audio["capture_clock"]["thread_stopped_monotonic_ns"] - origin) / 1e9,
    )
    started_at = datetime.fromisoformat(capture["started_at"].replace("Z", "+00:00"))
    now = datetime.now(UTC)
    media_type = "application/zstd" if compress_indexes else "application/x-ndjson"
    frames_artifact = _artifact(partial, frames.path, "frames.index", media_type, verified=verified)
    imu_artifact = _artifact(partial, imu_relative, "imu.samples", media_type, verified=verified)
    if compress_indexes:
        frames_artifact["storage_encoding"] = frames.storage_encoding
        imu_artifact["storage_encoding"] = imu_encoding
    video_segments = [
        {
            "index": int(record["index"]),
            "start_frame": int(record["start_ordinal"]),
            "end_frame": int(record["end_ordinal"]),
            "start_time_seconds": (
                frames.boundary_monotonic_ns[int(record["start_ordinal"])] - origin
            )
            / 1e9,
            "end_time_seconds": (
                video_end
                if int(record["end_ordinal"]) == frames.count
                else (frames.boundary_monotonic_ns[int(record["end_ordinal"])] - origin) / 1e9
            ),
            "artifacts": record["artifacts"],
        }
        for record in segments
    ]
    resolved_audio = audio
    if resolved_audio is None and config.audio_enabled:
        resolved_audio = {
            "state": "not_recorded",
            "requested_mode": "enabled",
            "resolved_mode": "disabled",
            "reason": "recording_interrupted",
        }
    try:
        state = _read_json(partial, "recording.json")
        validate_recording_state(state)
        if (
            state["session_id"] != plan.session_id
            or state["storage"]["volume_id"] != plan.volume_id
            or state["device"]["device_id"] != config.device_id
        ):
            raise ValueError("recovery state identity mismatch")
    except (OSError, ValueError, KeyError, TypeError, RecursionError):
        state = {}
    diagnostics = state.get("diagnostics") or [
        {"code": "process_interrupted", "message": "录制进程在正常封存前退出"}
    ]
    receipt = {
        "schema": "openaria.recording-recovery.v1",
        "session_id": plan.session_id,
        "outcome": "interrupted",
        "recovered_at": now.isoformat(),
        "saved_segments": len(segments),
        "saved_frames": frames.count,
        "saved_video_seconds": video_end,
        "diagnostics": diagnostics,
        "audio_recovered": audio is not None,
        "audio_recovery_error": audio_error,
        "original_artifacts_preserved": True,
    }
    write_json_atomic(recovery / "interruption.json", receipt)
    logs = [
        _artifact(
            partial,
            "recovery/interruption.json",
            RECOVERY_ROLE,
            "application/json",
            verified=verified,
        )
    ]
    if clock is not None:
        logs.append(
            _artifact(
                partial,
                "clock-calibration.json",
                "log.clock-calibration",
                "application/json",
                verified=verified,
            )
        )
    manifest = build_device_session_manifest(
        config=config,
        plan=plan,
        started_at=started_at,
        ended_at=started_at + timedelta(seconds=duration),
        verified_at=now,
        sealed_at=now,
        duration=duration,
        frames_written=frames.count,
        imu_written=imu_count,
        video_segments=video_segments,
        frames_artifact=frames_artifact,
        imu_artifact=imu_artifact,
        audio=resolved_audio,
        logs=logs,
        drop_events=(),
        schema="ylx.device-session.v4" if compress_indexes else None,
    )
    write_json_atomic(partial / "manifest.json", manifest, pretty=False)
    _, payload = inspect_device_session_directory(partial, expected_session_id=plan.session_id)
    # Recovery owns these files until publication. Detect concurrent replacement
    # or writes from metadata without rereading the recovered media.
    for relative, artifact in verified.items():
        current_stat = (partial / relative).stat(follow_symlinks=False)
        current_id = (
            verified_identity(current_stat)
            if artifact.include_inode
            else file_identity(current_stat, include_inode=False)
        )
        if current_id != artifact.identity:
            raise DeviceRecordingError("artifact_invalid", "恢复文件在保存前发生变化")
    # Stage control archives in recovery/ before rename (so crash-before-rename
    # already has recovery/recording.json), but keep the top-level controls in
    # partial until os.rename(partial, final) succeeds so failed renames stay
    # recoverable.
    archived_controls: list[str] = []
    for name in ("capture.json", "recording.json"):
        if copy_regular_atomic(partial / name, recovery / name):
            archived_controls.append(name)
    for subdir_name in ("video", "audio"):
        subdir = partial / subdir_name
        if subdir.is_dir():
            fsync_directory(subdir)
    fsync_directory(recovery)
    fsync_directory(partial)
    os.rename(partial, final)
    fsync_directory(final.parent)
    unlinked = False
    for name in archived_controls:
        try:
            (final / name).unlink(missing_ok=True)
            unlinked = True
        except OSError as error:
            operational_logger("recording").event(
                "recovery_control_unlink_failed",
                level="warning",
                transaction_id=plan.session_id,
                file=name,
                error=str(error),
            )
    if unlinked:
        with suppress(OSError):
            fsync_directory(final)
    return SealedDeviceSession(
        final,
        manifest,
        payload,
        hashlib.sha256(payload).hexdigest(),
        verified_artifacts=verified,
    )
