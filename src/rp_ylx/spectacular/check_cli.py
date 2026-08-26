"""Command-line acceptance check for Spectacular calibration inputs."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from .adapter import CaptureValidationError
from .model import check_capture


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate and map a legacy raw or Device Session calibration capture."
    )
    parser.add_argument("capture_dir", type=Path)
    parser.add_argument("--imu-rate", type=float, default=120.0)
    args = parser.parse_args(argv)
    try:
        result = check_capture(args.capture_dir, imu_rate_hz=args.imu_rate)
    except (CaptureValidationError, OSError, ValueError) as error:
        print(f"spectacular validation failed: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
