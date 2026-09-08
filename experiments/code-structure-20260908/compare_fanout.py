"""Compare the actual baseline and candidate fanout implementations."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def run_variant(source: Path, destination: Path, target: Path) -> dict[str, object]:
    destination.mkdir()
    for name in ("Cargo.toml", "Cargo.lock"):
        shutil.copy2(source / name, destination / name)
    shutil.copytree(source / "native/src", destination / "native/src")
    shutil.copy2(source / "native/Cargo.toml", destination / "native/Cargo.toml")
    binary = destination / "native/src/bin/fanout_trace.rs"
    binary.parent.mkdir()
    shutil.copy2(Path(__file__).with_name("fanout_trace.rs"), binary)
    process = subprocess.run(
        ["cargo", "run", "--locked", "--release", "--bin", "fanout_trace"],
        cwd=destination,
        env={**os.environ, "CARGO_TARGET_DIR": str(target), "PYO3_PYTHON": sys.executable},
        check=True,
        capture_output=True,
        text=True,
    )
    return {
        "recording_rs_sha256": hashlib.sha256(
            (destination / "native/src/recording.rs").read_bytes()
        ).hexdigest(),
        "trace": json.loads(process.stdout),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="openaria-fanout-") as directory:
        root = Path(directory)
        results = {
            name: run_variant(source.resolve(), root / name, root / "target")
            for name, source in (("baseline", args.baseline), ("candidate", args.candidate))
        }
    results["equal"] = results["baseline"]["trace"] == results["candidate"]["trace"]
    rendered = json.dumps(results, indent=2, sort_keys=True) + "\n"
    args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if results["equal"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
