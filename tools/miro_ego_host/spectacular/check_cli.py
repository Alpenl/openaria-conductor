"""Command-line acceptance check for Miro-Ego Spectacular calibration sessions."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

if __package__:
    from .adapter import CaptureValidationError
    from .model import check_capture, write_frame_timestamp_csv
    from .timebase import analyze_capture
else:
    _tools_root = str(Path(__file__).resolve().parents[2])
    if _tools_root not in sys.path:
        sys.path.insert(0, _tools_root)
    from miro_ego_host.spectacular.adapter import CaptureValidationError
    from miro_ego_host.spectacular.model import check_capture, write_frame_timestamp_csv
    from miro_ego_host.spectacular.timebase import analyze_capture


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="miro-ego-spectacular-check",
        description="Validate and map a sealed Miro-Ego calibration Device Session.",
    )
    parser.add_argument("capture_dir", type=Path)
    parser.add_argument(
        "--imu-rate",
        type=float,
        help=(
            "Expected received IMU samples/s (optional; validates throughput and does not "
            "configure hardware ODR)."
        ),
    )
    parser.add_argument(
        "--timestamp-csv",
        type=Path,
        help=(
            "Write one row per video frame with left-eye, right-eye, and nearest IMU "
            "timestamps. Miro-Ego split-eye video uses one shared side-by-side frame "
            "timestamp for both eyes."
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
