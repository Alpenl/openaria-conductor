"""Explicit mapping from validated Miro-Ego calibration captures into Spectacular model input."""

from __future__ import annotations

import csv
import hashlib
import json
import math
from bisect import bisect_left
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from .adapter import LoadedCapture, artifact_roles
from .timebase import CaptureTiming, analyze_capture

MODEL_INPUT_SCHEMA = "miro-ego.spectacular.model-input.v1"
FRAME_TIMESTAMP_ALIGNMENT_SCHEMA = "miro-ego.spectacular.frame-timestamp-alignment.v1"
CHECK_SCHEMA = "miro-ego.spectacular.check.v1"

TIMESTAMP_CSV_FIELDS = (
    "frame_index",
    "source_sequence",
    "left_eye_host_monotonic_ns",
    "right_eye_host_monotonic_ns",
    "left_eye_time_seconds",
    "right_eye_time_seconds",
    "nearest_imu_sample_number",
    "nearest_imu_host_monotonic_ns",
    "nearest_imu_estimated_monotonic_ns",
    "nearest_imu_time_seconds",
    "nearest_imu_delta_ms",
    "imu_device_timestamp_raw",
    "imu_device_ticks",
    "imu_packet_sequence",
    "imu_sample_index",
    "imu_sync_quality",
)


def build_model_input(timing: CaptureTiming) -> dict[str, Any]:
    """Build the calibration-facing payload without filename or layout inference."""
    capture = timing.capture
    origin_ns = min(timing.frame_times_ns[0], timing.imu_times_ns[0])

    frames: list[dict[str, Any]] = []
    for record, time_ns in zip(capture.frames, timing.frame_times_ns, strict=True):
        frames.append(
            {
                "frame_index": record["frame_index"],
                "source_sequence": record["uvc_sequence"],
                "time_seconds": (time_ns - origin_ns) / 1e9,
                "segment": {
                    "index": record["segment_index"],
                    "frame": record["segment_frame"],
                },
                "source": dict(record.get("source", {})),
            }
        )

    imu_samples: list[dict[str, Any]] = []
    for record, time_ns in zip(capture.imu_samples, timing.imu_times_ns, strict=True):
        imu_samples.append(
            {
                "sample_number": record["sample_number"],
                "sample_index": record["sample_index"],
                "packet_sequence": record["packet_sequence"],
                "time_seconds": (time_ns - origin_ns) / 1e9,
                "accelerometer_raw": list(record["accel_raw"]),
                "gyroscope_raw": list(record["gyro_raw"]),
                "source": dict(record.get("source", {})),
            }
        )

    video: dict[str, Any] = {
        "authority": capture.video.authority,
        "layout": capture.video.layout,
        "codec": capture.video.codec,
        "container": capture.video.container,
        "segments": [
            {
                "index": segment.index,
                "start_frame": segment.start_frame,
                "end_frame": segment.end_frame,
                "left_path": segment.left_path.relative_to(capture.root).as_posix(),
                "right_path": segment.right_path.relative_to(capture.root).as_posix(),
            }
            for segment in capture.video.segments
        ],
        "width": capture.width,
        "eye_width": capture.eye_width,
        "height": capture.height,
        "fps": capture.fps,
    }
    if capture.video.media_path is not None:
        video["media_path"] = capture.video.media_path.relative_to(capture.root).as_posix()
        video["tracks"] = {"left_track_id": 1, "right_track_id": 2}

    source_block: dict[str, Any] = {
        "schema": capture.source_schema,
        "session_id": capture.session_id,
        "capture_mode": capture.manifest["capture_mode"],
        "artifact_roles": list(artifact_roles(capture)),
    }
    if capture.logs:
        source_block["logs"] = [
            {
                "role": log.role,
                "path": log.path.relative_to(capture.root).as_posix(),
                "media_type": log.media_type,
                "bytes": log.bytes,
                "sha256": log.sha256,
            }
            for log in capture.logs
        ]

    return {
        "schema": MODEL_INPUT_SCHEMA,
        "source": source_block,
        "video": video,
        "time_origin_monotonic_ns": origin_ns,
        "imu_timing": {
            "basis": timing.imu_time_basis,
            "sample_times_estimated": True,
            "missed_packets_estimate_available": False,
        },
        "frames": frames,
        "imu_samples": imu_samples,
    }


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, math.ceil(fraction * len(ordered)) - 1)
    return ordered[max(index, 0)]


def _nearest_index(values: tuple[float, ...], target: float) -> int:
    lo = bisect_left(values, target)
    if lo <= 0:
        return 0
    if lo >= len(values):
        return len(values) - 1
    before = lo - 1
    after = lo
    return before if abs(values[before] - target) <= abs(values[after] - target) else after


def build_frame_timestamp_rows(timing: CaptureTiming) -> tuple[dict[str, Any], ...]:
    """Pair every video frame with the nearest reconstructed IMU sample timestamp."""
    return tuple(_iter_frame_timestamp_rows(timing))


def _iter_frame_timestamp_rows(timing: CaptureTiming) -> Iterator[dict[str, Any]]:
    origin_ns = min(timing.frame_times_ns[0], timing.imu_times_ns[0])
    for frame, frame_time_ns in zip(timing.capture.frames, timing.frame_times_ns, strict=True):
        imu_index = _nearest_index(timing.imu_times_ns, frame_time_ns)
        imu_time_ns = timing.imu_times_ns[imu_index]
        imu = timing.capture.imu_samples[imu_index]
        imu_sync = imu.get("sync", {})
        frame_source_ns = int(frame["callback_monotonic_ns"])
        rounded_imu_ns = int(round(imu_time_ns))
        frame_seconds = (frame_time_ns - origin_ns) / 1e9
        imu_seconds = (imu_time_ns - origin_ns) / 1e9
        yield {
            "frame_index": frame["frame_index"],
            "source_sequence": frame["uvc_sequence"],
            "left_eye": {
                "time_base": "host_monotonic",
                "timestamp_ns": frame_source_ns,
                "time_seconds": frame_seconds,
                "timestamp_source": "shared_sbs_frame",
            },
            "right_eye": {
                "time_base": "host_monotonic",
                "timestamp_ns": frame_source_ns,
                "time_seconds": frame_seconds,
                "timestamp_source": "shared_sbs_frame",
            },
            "nearest_imu": {
                "time_base": "host_monotonic",
                "estimated_timestamp_ns": rounded_imu_ns,
                "source_host_monotonic_ns": int(imu["host_monotonic_ns"]),
                "time_seconds": imu_seconds,
                "sample_number": imu["sample_number"],
                "sample_index": imu["sample_index"],
                "packet_sequence": imu.get("packet_sequence"),
                "device_timestamp_raw": imu.get("device_timestamp_raw"),
                "device_ticks": imu.get("device_ticks"),
                "sync_quality": imu_sync.get("quality") if isinstance(imu_sync, dict) else None,
            },
            "nearest_imu_delta_ms": (imu_time_ns - frame_time_ns) / 1e6,
        }


def frame_timestamp_alignment_summary(timing: CaptureTiming) -> dict[str, Any]:
    """Return compact timing alignment facts for the CLI check output."""
    deltas = [
        abs(
            (timing.imu_times_ns[_nearest_index(timing.imu_times_ns, frame_time)] - frame_time)
            / 1e6
        )
        for frame_time in timing.frame_times_ns
    ]
    return {
        "schema": FRAME_TIMESTAMP_ALIGNMENT_SCHEMA,
        "camera_timestamp_source": "shared_sbs_frame",
        "camera_time_base": "host_monotonic",
        "left_right_delta_ms": 0.0,
        "imu_matching": "nearest_reconstructed_sample",
        "imu_time_basis": timing.imu_time_basis,
        "rows": len(timing.frame_times_ns),
        "nearest_imu_delta_p50_ms": _percentile(deltas, 0.50),
        "nearest_imu_delta_p95_ms": _percentile(deltas, 0.95),
        "nearest_imu_delta_max_ms": max(deltas, default=0.0),
    }


def _csv_value(value: object) -> object:
    if isinstance(value, float):
        return f"{value:.9f}"
    return "" if value is None else value


def write_frame_timestamp_csv(timing: CaptureTiming, output_path: str | Path) -> int:
    """Write one row per video frame with left/right camera and nearest IMU timestamps."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=TIMESTAMP_CSV_FIELDS)
        writer.writeheader()
        for row in _iter_frame_timestamp_rows(timing):
            left = row["left_eye"]
            right = row["right_eye"]
            imu = row["nearest_imu"]
            writer.writerow(
                {
                    "frame_index": row["frame_index"],
                    "source_sequence": row["source_sequence"],
                    "left_eye_host_monotonic_ns": left["timestamp_ns"],
                    "right_eye_host_monotonic_ns": right["timestamp_ns"],
                    "left_eye_time_seconds": _csv_value(left["time_seconds"]),
                    "right_eye_time_seconds": _csv_value(right["time_seconds"]),
                    "nearest_imu_sample_number": imu["sample_number"],
                    "nearest_imu_host_monotonic_ns": imu["source_host_monotonic_ns"],
                    "nearest_imu_estimated_monotonic_ns": imu["estimated_timestamp_ns"],
                    "nearest_imu_time_seconds": _csv_value(imu["time_seconds"]),
                    "nearest_imu_delta_ms": _csv_value(row["nearest_imu_delta_ms"]),
                    "imu_device_timestamp_raw": imu["device_timestamp_raw"],
                    "imu_device_ticks": imu["device_ticks"],
                    "imu_packet_sequence": imu["packet_sequence"],
                    "imu_sample_index": imu["sample_index"],
                    "imu_sync_quality": imu["sync_quality"],
                }
            )
    return len(timing.frame_times_ns)


def check_capture(
    capture: CaptureTiming | LoadedCapture | str | Path,
    *,
    imu_rate_hz: float | None = None,
) -> dict[str, Any]:
    """Return bounded diagnostics and a deterministic SHA-256 identity for the mapped input."""
    timing = (
        capture
        if isinstance(capture, CaptureTiming)
        else analyze_capture(capture, imu_rate_hz=imu_rate_hz)
    )
    model_input = build_model_input(timing)
    encoded = json.dumps(
        model_input, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return {
        "schema": CHECK_SCHEMA,
        "source_schema": timing.capture.source_schema,
        "session_id": timing.capture.session_id,
        "video": model_input["video"],
        "diagnostics": timing.diagnostics(),
        "timestamp_alignment": frame_timestamp_alignment_summary(timing),
        "model_input": {
            "schema": model_input["schema"],
            "sha256": hashlib.sha256(encoded).hexdigest(),
            "frames": len(model_input["frames"]),
            "imu_samples": len(model_input["imu_samples"]),
        },
    }
