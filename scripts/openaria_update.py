#!/usr/bin/env python3
"""Source checkout entry point; OSS serves the standalone rp_ylx/update.py."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from rp_ylx.update import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
