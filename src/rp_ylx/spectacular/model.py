"""Explicit mapping from validated captures into Spectacular model input."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .adapter import artifact_roles
from .timebase import CaptureTiming, analyze_capture


def build_model_input(timing: CaptureTiming) -> dict[str, Any]:
    """Build the calibration-facing shape without filename or layout inference."""

    capture = timing.capture
    origin_ns = min(timing.frame_times_ns[0], timing.imu_times_ns[0])
    frames = []
    for record, time_ns in zip(capture.frames, timing.frame_times_ns, strict=True):
        frames.append(
            {
                "frame_index": record["frame_index"],
                "source_sequence": record["uvc_sequence"],
                "time_seconds": (time_ns - origin_ns) / 1e9,
                "jpeg": {
                    "offset": record["jpeg_offset"],
                    "bytes": record["jpeg_bytes"],
                },
                "source": dict(record.get("source", {})),
            }
        )

    imu_samples = []
    for record, time_ns in zip(capture.imu_samples, timing.imu_times_ns, strict=True):
        mapped = {
            "sample_number": record["sample_number"],
            "sample_index": record["sample_index"],
            "time_seconds": (time_ns - origin_ns) / 1e9,
            "accelerometer_raw": list(record["accel_raw"]),
            "gyroscope_raw": list(record["gyro_raw"]),
            "source": dict(record.get("source", {})),
        }
        if "packet_sequence" in record:
            mapped["packet_sequence"] = record["packet_sequence"]
        imu_samples.append(mapped)

    return {
        "schema": "rp-ylx.spectacular.model-input.v1",
        "source": {
            "schema": capture.source_schema,
            "session_id": capture.session_id,
            "capture_mode": capture.manifest.get("capture_mode", "legacy-calibration"),
            "artifact_roles": list(artifact_roles(capture)),
        },
        "video": {
            "authority": capture.video.authority,
            "path": capture.video.path.relative_to(capture.root).as_posix(),
            "layout": "raw-side-by-side",
            "width": capture.width,
            "eye_width": capture.eye_width,
            "height": capture.height,
            "fps": capture.fps,
        },
        "time_origin_monotonic_ns": origin_ns,
        "frames": frames,
        "imu_samples": imu_samples,
    }


def check_capture(
    capture: CaptureTiming | str | Path,
    *,
    imu_rate_hz: float = 120.0,
) -> dict[str, Any]:
    """Return bounded diagnostics and a stable identity for the mapped input."""

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
        "schema": "rp-ylx.spectacular.check.v1",
        "source_schema": timing.capture.source_schema,
        "session_id": timing.capture.session_id,
        "video": model_input["video"],
        "diagnostics": timing.diagnostics(),
        "model_input": {
            "schema": model_input["schema"],
            "sha256": hashlib.sha256(encoded).hexdigest(),
            "frames": len(model_input["frames"]),
            "imu_samples": len(model_input["imu_samples"]),
        },
    }
