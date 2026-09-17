"""Check existing raw or Zstandard frame journals without modifying a recording."""

from __future__ import annotations

import argparse
import io
import json
from contextlib import ExitStack
from pathlib import Path

import zstandard

from rp_ylx.recording.cadence import FrameCadence


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("frames", type=Path, help="frames.ndjson or frames.ndjson.zst")
    parser.add_argument("--fps", type=float, default=30)
    args = parser.parse_args()
    cadence = FrameCadence(args.fps)
    with ExitStack() as stack:
        stream = stack.enter_context(args.frames.open("rb"))
        if args.frames.suffix == ".zst":
            stream = stack.enter_context(zstandard.ZstdDecompressor().stream_reader(stream))
        stream = stack.enter_context(io.BufferedReader(stream))
        while line := stream.readline(1_048_577):
            if len(line) > 1_048_576 or not line.endswith(b"\n"):
                raise ValueError("frame journal has oversized or incomplete record")
            cadence.observe(json.loads(line)["host_monotonic_ns"])
    result = cadence.summary()
    print(json.dumps(result, indent=2, allow_nan=False))
    return {"within_tolerance": 0, "outside_tolerance": 2, "insufficient_duration": 3}[
        result["status"]
    ]


if __name__ == "__main__":
    raise SystemExit(main())
