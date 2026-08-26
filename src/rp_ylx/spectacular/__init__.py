"""Strict capture ingestion boundary for the Spectacular calibration toolchain."""

from .adapter import CaptureValidationError, LoadedCapture, load_capture
from .model import build_model_input, check_capture
from .timebase import CaptureTiming, ClockDiagnostics, analyze_capture

__all__ = [
    "CaptureTiming",
    "CaptureValidationError",
    "ClockDiagnostics",
    "LoadedCapture",
    "analyze_capture",
    "build_model_input",
    "check_capture",
    "load_capture",
]
