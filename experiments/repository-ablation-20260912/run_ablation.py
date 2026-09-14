"""Reproduce independent queue, network, and manifest ablations without hardware."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import statistics
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
SOURCE_FILES = (
    "native/src/bounded.rs",
    "native/src/capture_runtime.rs",
    "src/rp_ylx/network_validation.py",
    "src/rp_ylx/network_control.py",
    "src/rp_ylx/network_state.py",
    "src/rp_ylx/network.py",
    "src/rp_ylx/api/gateway.py",
    "src/rp_ylx/api/downloads.py",
    "src/rp_ylx/recording/device_session.py",
    "src/rp_ylx/recording/coordinator.py",
    "src/rp_ylx/daemon.py",
    "src/rp_ylx/imu/native.py",
)


def sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def run(command: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(command, capture_output=True, text=True, check=True, timeout=90, **kwargs)


def source_identity(root: Path) -> dict:
    return {
        "head": run(["git", "-C", str(root), "rev-parse", "HEAD"]).stdout.strip(),
        "tracked_patch_sha256": sha256(
            run(["git", "-C", str(root), "diff", "--binary", "HEAD"]).stdout.encode()
        ),
        "sources": {
            name: sha256((root / name).read_bytes()) if (root / name).exists() else None
            for name in SOURCE_FILES
        },
    }


def python_probe(root: Path, work: Path, name: str, negative: str | None = None) -> dict:
    output = work / f"{name}-probe.json"
    command = [sys.executable, str(HERE / "probe.py"), str(root), str(output)]
    if negative:
        command.extend(["--negative", negative])
    run(command)
    return json.loads(output.read_text())


def queue_probe(root: Path, work: Path, name: str, negative: bool = False) -> dict:
    binary = work / f"{name}-queue"
    run(
        ["rustc", "--edition=2024", "-O", str(HERE / "queue_probe.rs"), "-o", str(binary)],
        env={**os.environ, "ABLATION_ROOT": str(root)},
    )
    result = subprocess.run([str(binary)], capture_output=True, text=True, timeout=60)
    if negative:
        assert result.returncode != 0, "Removing the capacity check must break the model trace"
        return {"returncode": result.returncode, "stderr": result.stderr.strip()}
    result.check_returncode()
    value = json.loads(result.stdout)
    (work / f"{name}-queue.json").write_text(result.stdout)
    value["median_ns"] = statistics.median(value["samples_ns"])
    return value


def counts(records: list[dict], boundary: str) -> dict:
    return dict(Counter(json.dumps(record[boundary], sort_keys=True) for record in records))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, default=HERE.parents[1])
    parser.add_argument("--output", type=Path, default=HERE / "results.json")
    args = parser.parse_args()
    baseline, candidate = args.baseline.resolve(), args.candidate.resolve()
    with tempfile.TemporaryDirectory(prefix="openaria-repository-ablation-") as temporary:
        work = Path(temporary)
        before = python_probe(baseline, work, "baseline")
        after = python_probe(candidate, work, "candidate")
        assert before["network"]["corpus_sha256"] == after["network"]["corpus_sha256"]
        assert before["manifest"] == after["manifest"]
        control_changes = 0
        for old, new in zip(before["network"]["records"], after["network"]["records"], strict=True):
            assert old["http"] == new["http"]
            if old["control"] != new["control"]:
                assert old["unhashable_mode"]
                assert old["control"] == {"error": "TypeError", "code": None}
                assert new["control"] == {"value": False}
                control_changes += 1

        queue_before = queue_probe(baseline, work, "baseline")
        queue_after = queue_probe(candidate, work, "candidate")
        for field in ("trace_hashes", "concurrent_frames", "round_trips_per_sample"):
            assert queue_before[field] == queue_after[field]

        negative_network = python_probe(candidate, work, "no-network-validation", "network")
        accepted_invalid = sum(
            good["http"] == {"value": False} and bad["http"] == {"value": True}
            for good, bad in zip(
                after["network"]["records"], negative_network["network"]["records"], strict=True
            )
        )
        assert accepted_invalid > 0
        negative_paths = python_probe(candidate, work, "no-path-validation", "paths")
        changed_paths = sum(
            a != b
            for a, b in zip(
                after["manifest"]["records"], negative_paths["manifest"]["records"], strict=True
            )
        )
        assert changed_paths > 0
        unsafe_root = work / "no-queue-capacity"
        unsafe_queue = unsafe_root / "native/src/bounded.rs"
        unsafe_queue.parent.mkdir(parents=True)
        queue_source = (candidate / "native/src/bounded.rs").read_text()
        check = "state.closed || state.queue.len() == self.shared.capacity"
        assert queue_source.count(check) == 1
        unsafe_queue.write_text(queue_source.replace(check, "state.closed"))
        negative_queue = queue_probe(unsafe_root, work, "no-queue-capacity", negative=True)

        manifest = {key: value for key, value in after["manifest"].items() if key != "records"}
        manifest["identical"] = True
        manifest["successful_summary_cases"] = sum(
            isinstance(record, list) and "value" in record[0]
            for record in after["manifest"]["records"]
        )
        assert manifest["successful_summary_cases"] > 0
        result = {
            "schema": "openaria.repository-ablation.v1",
            "environment": {
                "platform": platform.platform(),
                "python": platform.python_version(),
                "rustc": run(["rustc", "--version"]).stdout.strip(),
            },
            "baseline": source_identity(baseline),
            "candidate": source_identity(candidate),
            "network": {
                "cases": after["network"]["cases"],
                "corpus_sha256": after["network"]["corpus_sha256"],
                "http_identical": True,
                "control_typeerror_to_rejection": control_changes,
                "baseline_http_outcomes": counts(before["network"]["records"], "http"),
                "candidate_control_outcomes": counts(after["network"]["records"], "control"),
            },
            "manifest": manifest,
            "queue": {
                "baseline": queue_before,
                "candidate": queue_after,
                "median_reduction_percent": 100
                * (1 - queue_after["median_ns"] / queue_before["median_ns"]),
            },
            "negative_controls": {
                "network_validation_removed_accepts_invalid": accepted_invalid,
                "path_validation_removed_changes_cases": changed_paths,
                "queue_capacity_removed": negative_queue,
            },
            "limits": [
                "Queue timing is a host microbenchmark, not camera throughput.",
                "Manifest probes exercise decoded projection and descriptors; "
                "full validation is covered by the existing suites.",
                "Existing TypeError/UnicodeEncodeError outcomes outside mode membership "
                "remain unchanged.",
                "No RDK X5, camera, IMU, ALSA, NetworkManager, or board codec acceptance was run.",
            ],
        }
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
        print(
            json.dumps(
                {
                    "network_cases": result["network"]["cases"],
                    "manifest": manifest,
                    "queue_reduction_percent": result["queue"]["median_reduction_percent"],
                    "negative_controls": result["negative_controls"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
