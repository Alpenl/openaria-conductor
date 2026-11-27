"""Strict capture ingestion boundary for the Miro-Ego Spectacular calibration toolchain."""

from __future__ import annotations

from .adapter import (
    CaptureLogArtifact,
    CaptureValidationError,
    CaptureVideo,
    CaptureVideoSegment,
    LoadedCapture,
    artifact_roles,
    load_capture,
    open_metadata,
    open_regular,
)
from .model import (
    CHECK_SCHEMA,
    FRAME_TIMESTAMP_ALIGNMENT_SCHEMA,
    MODEL_INPUT_SCHEMA,
    TIMESTAMP_CSV_FIELDS,
    build_frame_timestamp_rows,
    build_model_input,
    check_capture,
    frame_timestamp_alignment_summary,
    write_frame_timestamp_csv,
)
from .timebase import CaptureTiming, ClockDiagnostics, analyze_capture

__all__ = [
    "CHECK_SCHEMA",
    "FRAME_TIMESTAMP_ALIGNMENT_SCHEMA",
    "MODEL_INPUT_SCHEMA",
    "TIMESTAMP_CSV_FIELDS",
    "CaptureLogArtifact",
    "CaptureTiming",
    "CaptureValidationError",
    "CaptureVideo",
    "CaptureVideoSegment",
    "ClockDiagnostics",
    "LoadedCapture",
    "analyze_capture",
    "artifact_roles",
    "build_frame_timestamp_rows",
    "build_model_input",
    "check_capture",
    "frame_timestamp_alignment_summary",
    "load_capture",
    "open_metadata",
    "open_regular",
    "write_frame_timestamp_csv",
]
