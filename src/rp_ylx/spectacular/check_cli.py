"""Command-line acceptance check for Spectacular calibration inputs."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from .adapter import CaptureValidationError
from .model import check_capture, write_frame_timestamp_csv
from .timebase import analyze_capture


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate and map a legacy raw or Device Session calibration capture."
    )
    parser.add_argument("capture_dir", type=Path)
    parser.add_argument(
        "--imu-rate",
        type=float,
        help="Expected received records/s (optional for Device Session; legacy default: 120). "
        "This validates throughput and does not configure hardware ODR.",
    )
    parser.add_argument(
        "--timestamp-csv",
        type=Path,
        help=(
            "Write one row per video frame with left-eye, right-eye, and nearest IMU "
            "timestamps. YLX split-eye video uses one shared SBS frame timestamp for both eyes."
        ),
    )
    args = parser.parse_args(argv)
    try:
        timing = analyze_capture(args.capture_dir, imu_rate_hz=args.imu_rate)
        result = check_capture(timing)
        if args.timestamp_csv is not None:
            rows = write_frame_timestamp_csv(timing, args.timestamp_csv)
            result["timestamp_csv"] = {
                "path": str(args.timestamp_csv),
                "rows": rows,
            }
    except (CaptureValidationError, OSError, ValueError) as error:
        print(f"spectacular validation failed: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
