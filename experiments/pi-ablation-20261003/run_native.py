#!/usr/bin/env python3
"""Reconstruct native active-take ablations and run them outside the repository."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import statistics
import subprocess
from pathlib import Path

BASELINE = "e7da4babf1d1f48ebc262b5714497cfabac98801"
SOURCE = "native/src/active_take.rs"


def without_snapshot(source: str) -> str:
    start = source.index("    pub(crate) fn finish_frame(")
    end = source.index("    #[cfg(test)]", start)
    method = source[start:end].replace(
        "Result<ActiveTakeSnapshot, ActiveTakeError>", "Result<(), ActiveTakeError>"
    )
    method = method.replace("Ok(self.snapshot())", "Ok(())")
    return source[:start] + method + source[end:]


def without_set(source: str) -> str:
    source = source.replace("use std::collections::BTreeSet;\n\n", "")
    source = source.replace("pending_frames: BTreeSet<u64>", "pending_frames: Option<u64>")
    source = source.replace("pending_frames: BTreeSet::new()", "pending_frames: None")
    source = source.replace(
        "        let record_sequence = self.frame_domain;",
        """        if self.pending_frames.is_some() {
            return Err(ActiveTakeError::new(
                "invalid_state", "active take already has a pending frame",
            ));
        }
        let record_sequence = self.frame_domain;""",
    )
    source = source.replace(
        "self.pending_frames.insert(record_sequence);",
        "self.pending_frames = Some(record_sequence);",
    )
    source = source.replace(
        "self.pending_frames.remove(&frame.record_sequence);", "self.pending_frames = None;"
    )
    source = source.replace("!self.pending_frames.is_empty()", "self.pending_frames.is_some()")
    source = source.replace(
        "u64::try_from(self.pending_frames.len()).unwrap_or(u64::MAX)",
        "u64::from(self.pending_frames.is_some())",
    )
    return source.replace(
        "!self.pending_frames.contains(&frame.record_sequence)",
        "self.pending_frames != Some(frame.record_sequence)",
    )


def without_test_drop_model(source: str) -> str:
    source = source.replace(
        '#[cfg(test)]\nconst WRITE_BACKPRESSURE: &str = "write_backpressure";\n\n', ""
    )
    source = source.replace("    drop_events: Vec<ActiveDropEvent>,\n", "")
    source = source.replace("            drop_events: Vec::new(),\n", "", 1)
    for beginning, following in (
        ("    #[cfg(test)]\n    pub(crate) fn reject_frame(", "    pub(crate) fn finish("),
        ("    #[cfg(test)]\n    fn record_drop(", "    fn ensure_open("),
        ("#[cfg(test)]\nfn validate_elapsed(", "#[cfg(test)]\nmod tests"),
    ):
        start = source.index(beginning)
        end = source.index(following, start)
        source = source[:start] + source[end:]
    return source.replace("dropped_frames: self.dropped_frames(),", "dropped_frames: 0,").replace(
        "drop_events: self.drop_events.clone(),", "drop_events: Vec::new(),"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--output", type=Path, required=True, help="new external output directory")
    args = parser.parse_args()
    repo, output = args.repo.resolve(), args.output.resolve()
    if output.is_relative_to(repo):
        parser.error("build and measurement output must be outside the repository")
    output.mkdir(parents=True, exist_ok=False)
    baseline = subprocess.check_output(["git", "show", f"{BASELINE}:{SOURCE}"], cwd=repo, text=True)
    candidate = (repo / SOURCE).read_text()
    variants = {
        "baseline": baseline,
        "a1": without_snapshot(baseline),
        "a2": without_set(baseline),
        "combined": without_set(without_snapshot(baseline)),
        "a3": without_test_drop_model(baseline),
        "all": candidate,
    }
    # Negative control: incomplete writes must remain a barrier to finalization.
    start = candidate.index("    pub(crate) fn finish(")
    guard = candidate.index("        if self.pending_frame.is_some() {", start)
    end = candidate.index("        self.closed = true;", guard)
    variants["negative"] = candidate[:guard] + candidate[end:]
    for name, source in variants.items():
        (output / f"active_take_{name}.rs").write_text(source)
    probe = Path(__file__).with_name("native_active_take_probe.rs").read_text()
    (output / "probe.rs").write_text(probe)
    for label, extra in (
        ("timing", []),
        ("allocations", ["--cfg", "track_alloc", "-A", "unused_imports"]),
    ):
        binary = output / label
        subprocess.run(
            ["rustc", "-O", "--edition=2021", *extra, str(output / "probe.rs"), "-o", str(binary)],
            check=True,
        )
        with (output / f"{label}.jsonl").open("w") as stream:
            subprocess.run([str(binary)], stdout=stream, check=True)
    timing = [json.loads(line) for line in (output / "timing.jsonl").read_text().splitlines()]
    allocations = [
        json.loads(line) for line in (output / "allocations.jsonl").read_text().splitlines()
    ]
    summary = {
        name: statistics.median(
            row["elapsed_ns"] / row["frames"] for row in timing if row.get("variant") == name
        )
        for name in variants
        if name != "negative"
    }
    result = {
        "baseline_commit": BASELINE,
        "compiler": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "platform": platform.platform(),
        "source_sha256": {
            name: hashlib.sha256(source.encode()).hexdigest() for name, source in variants.items()
        },
        "probe_sha256": hashlib.sha256(probe.encode()).hexdigest(),
        "median_ns_per_frame": summary,
        "raw_timing": timing,
        "allocations": allocations,
        "limits": [
            "Host microbenchmark only; all variants exclude camera, codec, disk, and Python work.",
            "At 30 fps these CPU savings are negligible; no device FPS improvement is claimed.",
            "A2 intentionally rejects overlapping reservations; "
            "only single-writer production traces are equivalent.",
            "The exported engine API does not globally prevent sharing a transaction "
            "between engines; overlapping writes from that unsupported setup now fail.",
            "A3 removes a test-only loss model; "
            "exported loss fields and real telemetry remain intact.",
        ],
    }
    (output / "native-results.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
