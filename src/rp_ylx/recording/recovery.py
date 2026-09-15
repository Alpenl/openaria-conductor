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
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from rp_ylx.audio_clock import audio_clock_report
from rp_ylx.recording.device_session import (
    DeviceRecordingError,
    DeviceSessionConfig,
    DeviceSessionRecorder,
    SealedDeviceSession,
    SessionPlan,
    StorageStatus,
    _finalize_artifact,
    fsync_directory,
    json_bytes,
    validate_device_session_directory,
    write_json_atomic,
)
from rp_ylx.recording.encoding import RecordingEncoding

RECOVERY_ROLE = "log.recording-recovery"


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
    if path.stat().st_size > 8 * 1024 * 1024:
        raise ValueError("recovery metadata is too large")
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise ValueError("invalid recovery metadata")
    return value


def _artifact(root: Path, relative: str, role: str, media_type: str) -> dict:
    finalized = _finalize_artifact(_regular(root, relative), None, code="recovery_invalid")
    return {
        "artifact_id": finalized.sha256,
        "sha256": finalized.sha256,
        "bytes": finalized.bytes,
        "path": relative,
        "role": role,
        "media_type": media_type,
    }


def _write_records(path: Path, records: Iterable[dict]) -> int:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o640)
    with os.fdopen(descriptor, "wb") as stream:
        count = 0
        for record in records:
            stream.write(json_bytes(record))
            count += 1
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    fsync_directory(path.parent)
    return count


def _frames(root: Path, plan: SessionPlan, segments: list[dict], decimation: int) -> list[dict]:
    """Reject an unindexed pair as a unit; a torn last line is not a frame."""
    rows: list[dict] = []
    segment_index = 0
    with _regular(root, "frames.ndjson").open("rb") as stream:
        for line in stream:
            if segment_index >= len(segments):
                break
            try:
                row = json.loads(line)
                segment = segments[segment_index]
                if (
                    not line.endswith(b"\n")
                    or row["session_id"] != plan.session_id
                    or row["schema"] != "ylx.frame-index.v1"
                    or row["frame"] != len(rows)
                    or row["segment_index"] != segment_index
                    or row["segment_frame"] != len(rows) - segment["start_ordinal"]
                    or type(row["host_monotonic_ns"]) is not int
                    or row["host_monotonic_ns"] <= 0
                    or (
                        rows
                        and (
                            row["source_sequence"] - rows[-1]["source_sequence"] != decimation
                            or row["host_monotonic_ns"] <= rows[-1]["host_monotonic_ns"]
                        )
                    )
                ):
                    break
                rows.append(row)
                if len(rows) == segment["end_ordinal"]:
                    segment_index += 1
            except (ValueError, KeyError, TypeError):
                break
    del segments[segment_index:]
    return rows[: segments[-1]["end_ordinal"]] if segments else []


def _audio(root: Path, config: DeviceSessionConfig, origin: int, video_end: float) -> dict | None:
    """Use sealed PCM and hardware clock observations; never invent silence."""
    if not config.audio_enabled:
        return None
    try:
        clock = _read_json(root, "audio/checkpoint.json")
    except (OSError, ValueError):
        return None
    rate, channels = config.audio_sample_rate_hz, config.audio_channels
    anchors = clock.get("anchors", [])
    if len(anchors) < 2 or any(
        b[0] <= a[0] or b[1] <= a[1] for a, b in zip(anchors, anchors[1:], strict=False)
    ):
        return None
    first, last = anchors[0], anchors[-1]
    slope = (last[1] - first[1]) / (last[0] - first[0])
    start = first[1] - first[0] * slope
    target = min(clock["sample_count"], math.floor((origin + video_end * 1e9 - start) / slope))
    if target <= 0 or abs(1e9 / slope / rate - 1) > 0.01:
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
                relative = "recovery/audio-tail.wav"
                temporary = root / "recovery/audio-tail.tmp"
                if temporary.is_symlink() or (root / relative).is_symlink():
                    raise ValueError("unsafe recovery audio path")
                with wave.open(str(temporary), "wb") as output:
                    output.setparams(stream.getparams())
                    remaining = selected
                    while remaining:
                        take = min(remaining, rate)
                        payload = stream.readframes(take)
                        if len(payload) != take * channels * 2:
                            raise ValueError("truncated recovered audio")
                        output.writeframesraw(payload)
                        remaining -= take
                with temporary.open("rb") as output:
                    os.fsync(output.fileno())
                os.replace(temporary, root / relative)
            records.append(
                {
                    "index": len(records),
                    "start_sample": samples,
                    "end_sample": samples + selected,
                    "start_time_seconds": samples / rate,
                    "end_time_seconds": (samples + selected) / rate,
                    "pcm_payload_bytes": selected * channels * 2,
                    "wav_header_bytes": 44,
                    "artifact": _artifact(root, relative, "audio.wav", "audio/wav"),
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
    first, last = selected_anchors[0], selected_anchors[-1]
    slope = (last[1] - first[1]) / (last[0] - first[0])
    start = first[1] - first[0] * slope
    end = start + samples * slope
    proof = {
        "schema": "openaria.audio-clock.v1",
        "clock": "host_monotonic",
        "timestamp_source": "alsa_htimestamp_dma",
        "continuity": "verified",
        "device": config.audio_device,
        "period_frames": clock["period_frames"],
        "buffer_frames": clock["buffer_frames"],
        "anchors": selected_anchors,
        "thread_started_monotonic_ns": clock["thread_started_monotonic_ns"],
        # This is the observed end of the recovered capture window. The full
        # checkpoint keeps the original, longer capture window for diagnosis.
        "thread_stopped_monotonic_ns": max(last[1], math.ceil(end)),
        "sample_start_monotonic_ns": round(start),
        "sample_end_monotonic_ns": round(end),
        "session_start_monotonic_ns": origin,
        "max_residual_ns": math.ceil(
            max(abs((t - first[1]) - (n - first[0]) * slope) for n, t in selected_anchors)
        ),
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
            "start_time_seconds": (round(start) - origin) / 1e9,
            "end_time_seconds": (round(end) - origin) / 1e9,
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
                actual = _artifact(partial, descriptor["path"], f"video.{eye}", "video/mp4")
                if actual != descriptor:
                    raise ValueError("committed segment digest mismatch")
            segments.append(record)
            end = record["end_ordinal"]
        except (OSError, ValueError, KeyError, TypeError, DeviceRecordingError):
            break
    rows = _frames(partial, plan, segments, config.frame_decimation)
    if not rows:
        return None
    origin = capture["started_monotonic_ns"]
    if type(origin) is not int or not 0 < origin <= rows[0]["host_monotonic_ns"]:
        raise ValueError("invalid capture clock origin")
    video_end = (
        rows[-1]["host_monotonic_ns"] - origin
    ) / 1e9 + config.frame_decimation / config.sensor_fps
    recovery = partial / "recovery"
    if recovery.is_symlink():
        raise ValueError("unsafe recovery directory")
    recovery.mkdir(exist_ok=True, mode=0o750)
    _write_records(recovery / "frames.ndjson", rows)

    def imu_prefix():
        # IMU data can be much larger than the frame index on long takes.
        # Stream it through recovery so memory does not grow with sample count.
        with _regular(partial, "imu.ndjson").open("rb") as stream:
            for line in stream:
                try:
                    sample = json.loads(line)
                    timestamp = sample["host_monotonic_ns"]
                    if not line.endswith(b"\n") or sample["session_id"] != plan.session_id:
                        break
                    if origin <= timestamp <= origin + video_end * 1e9:
                        yield sample
                except (ValueError, KeyError, TypeError):
                    break

    imu_count = _write_records(recovery / "imu.ndjson", imu_prefix())
    audio_error = None
    try:
        audio = _audio(partial, config, origin, video_end)
        if audio is not None:
            audio_clock_report(
                audio,
                max(
                    video_end,
                    (audio["capture_clock"]["thread_stopped_monotonic_ns"] - origin) / 1e9,
                ),
            )
    except (OSError, ValueError, KeyError, TypeError, wave.Error, DeviceRecordingError) as error:
        # Bad/missing audio never invalidates already committed video pairs.
        audio = None
        audio_error = str(error)
    duration = max(
        video_end,
        0
        if audio is None
        else (audio["capture_clock"]["thread_stopped_monotonic_ns"] - origin) / 1e9,
    )
    # Restore the producer's metadata and reuse the normal manifest builder.
    recorder = DeviceSessionRecorder(
        partial.parent,
        replace(config, audio_enabled=False),
        plan,
        authority_epoch=str(uuid.uuid4()),
        allocate_revision=lambda: 1,
        storage_status=lambda: StorageStatus(None, True),
    )
    recorder._started_at = datetime.fromisoformat(capture["started_at"].replace("Z", "+00:00"))
    recorder._started_monotonic_ns = origin
    recorder._frames_written = len(rows)
    recorder._imu_written = imu_count
    recorder._segment_records = segments
    for record in segments:
        for ordinal in (record["start_ordinal"], record["end_ordinal"]):
            recorder._boundary_record_sequence[ordinal] = ordinal
            recorder._boundary_elapsed[ordinal] = (
                video_end
                if ordinal == len(rows)
                else (rows[ordinal]["host_monotonic_ns"] - origin) / 1e9
            )
    now = datetime.now(UTC)
    manifest = recorder._manifest(
        recorder._started_at + timedelta(seconds=duration), now, now, duration
    )
    manifest["frames"]["artifact"] = _artifact(
        partial, "recovery/frames.ndjson", "frames.index", "application/x-ndjson"
    )
    manifest["imu"]["artifact"] = _artifact(
        partial, "recovery/imu.ndjson", "imu.samples", "application/x-ndjson"
    )
    if audio is not None:
        manifest["audio"] = audio
    elif config.audio_enabled:
        manifest["audio"] = {
            "state": "not_recorded",
            "requested_mode": "enabled",
            "resolved_mode": "disabled",
            "reason": "recording_interrupted",
        }
    try:
        state = _read_json(partial, "recording.json")
    except (OSError, ValueError):
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
        "saved_frames": len(rows),
        "saved_video_seconds": video_end,
        "diagnostics": diagnostics,
        "audio_recovered": audio is not None,
        "audio_recovery_error": audio_error,
        "original_artifacts_preserved": True,
    }
    write_json_atomic(recovery / "interruption.json", receipt)
    manifest["logs"] = [
        _artifact(partial, "recovery/interruption.json", RECOVERY_ROLE, "application/json")
    ]
    write_json_atomic(partial / "manifest.json", manifest)
    validate_device_session_directory(partial, expected_session_id=plan.session_id)
    # Archive controls only after the independent validator accepts every byte.
    # If power fails here, startup can finish the ordinary publish transaction.
    for name in ("capture.json", "recording.json"):
        path = partial / name
        if path.exists():
            os.replace(path, recovery / name)
    fsync_directory(recovery)
    fsync_directory(partial)
    os.rename(partial, final)
    fsync_directory(final.parent)
    payload = (final / "manifest.json").read_bytes()
    return SealedDeviceSession(final, manifest, payload, hashlib.sha256(payload).hexdigest())
