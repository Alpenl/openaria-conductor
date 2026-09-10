"""Strict capture ingestion boundary for the Spectacular calibration toolchain."""

from .adapter import CaptureValidationError, LoadedCapture, load_capture
from .model import (
    build_frame_timestamp_rows,
    build_model_input,
    check_capture,
    frame_timestamp_alignment_summary,
    write_frame_timestamp_csv,
)
from .timebase import CaptureTiming, ClockDiagnostics, analyze_capture

__all__ = [
    "CaptureTiming",
    "CaptureValidationError",
    "ClockDiagnostics",
    "LoadedCapture",
    "analyze_capture",
    "build_frame_timestamp_rows",
    "build_model_input",
    "check_capture",
    "frame_timestamp_alignment_summary",
    "load_capture",
    "write_frame_timestamp_csv",
]
