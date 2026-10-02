"""Compare actual catalog discovery methods against a Git baseline.

Run from the Conductor repository, using its development Python environment.
Temporary recordings use --temp-root (the OS temp directory by default). The
catalog fixture is synthetic: one real sealed session supplies already-checked
summary/snapshot shapes, then cache entries and manifest identities are seeded.
This measures warm listing, not cold validation or real Pi hardware throughput.
"""

from __future__ import annotations

import argparse
import ast
import copy
import hashlib
import inspect
import json
import platform
import statistics
import subprocess
import sys
import tempfile
import textwrap
import time
import tracemalloc
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--baseline", default="e7da4babf1d1f48ebc262b5714497cfabac98801")
    parser.add_argument("--counts", type=int, nargs="+", default=[100, 1000, 10000])
    parser.add_argument("--repeats", type=int, default=9)
    parser.add_argument("--temp-root", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(args.counts) < 1 or args.repeats < 1:
        parser.error("counts and repeats must be positive")
    repo = args.repo.resolve()
    if args.temp_root is not None:
        args.temp_root.mkdir(parents=True, exist_ok=True)
        tempfile.tempdir = str(args.temp_root.resolve())
    sys.path[:0] = [str(repo / "src"), str(repo / "tests")]
    from test_capture_coordinator import CaptureCoordinatorTest

    import rp_ylx.recording.coordinator as module

    baseline_text = subprocess.check_output(
        ["git", "show", f"{args.baseline}:src/rp_ylx/recording/coordinator.py"],
        cwd=repo,
        text=True,
    )
    cls = next(
        node
        for node in ast.parse(baseline_text).body
        if isinstance(node, ast.ClassDef) and node.name == "CaptureCoordinator"
    )
    method = next(
        node
        for node in cls.body
        if isinstance(node, ast.FunctionDef) and node.name == "_catalog_sessions"
    )
    baseline_source = textwrap.dedent(
        "\n".join(baseline_text.splitlines()[method.lineno - 1 : method.end_lineno])
    )
    namespace = dict(vars(module))
    exec(baseline_source, namespace)
    baseline = namespace["_catalog_sessions"]
    candidate = module.CaptureCoordinator._catalog_sessions
    results = []
    for count in args.counts:
        fixture = CaptureCoordinatorTest()
        fixture.setUp()
        coordinator = fixture.coordinator()
        try:
            original = fixture.seal_one(coordinator, prefix="catalog-ablation")
            root = fixture.mountpoint / "recordings"
            summary = copy.deepcopy(coordinator._session_summaries[original])
            snapshot = coordinator._session_snapshots[original]
            payload = (root / original / "manifest.json").read_bytes()
            for number in range(1, count):
                session_id = f"00000000-0000-7000-8000-{number:012x}"
                directory = root / session_id
                directory.mkdir()
                manifest_path = directory / "manifest.json"
                manifest_path.write_bytes(payload)
                metadata = manifest_path.stat()
                identity = (
                    metadata.st_dev,
                    metadata.st_ino,
                    metadata.st_mode,
                    metadata.st_nlink,
                    metadata.st_size,
                    metadata.st_mtime_ns,
                    metadata.st_ctime_ns,
                )
                coordinator._session_snapshots[session_id] = replace(
                    snapshot, manifest_identity=identity
                )
                coordinator._session_summaries[session_id] = dict(summary, session_id=session_id)
                coordinator._verified[session_id] = snapshot.manifest_sha256
            kwargs = dict(cursor=None, limit=50, take_id=None, api_version="v4")
            for mode in ("verified", "metadata_only"):
                if mode == "metadata_only":
                    coordinator._session_snapshots.clear()
                    coordinator._verified.clear()
                    for item in coordinator._session_summaries.values():
                        item["verification"] = None
                entry = dict(sessions=count, mode=mode)
                expected = coordinator.list_sessions(**kwargs)
                for label, implementation in (("baseline", baseline), ("candidate", candidate)):
                    with patch.object(
                        module.CaptureCoordinator, "_catalog_sessions", implementation
                    ):
                        durations = []
                        for _ in range(args.repeats):
                            started = time.perf_counter()
                            actual = coordinator.list_sessions(**kwargs)
                            durations.append(time.perf_counter() - started)
                            assert actual == expected
                        tracemalloc.start()
                        coordinator.list_sessions(**kwargs)
                        _, peak = tracemalloc.get_traced_memory()
                        tracemalloc.stop()
                        entry[label] = dict(
                            median_seconds=statistics.median(durations),
                            seconds=durations,
                            peak_python_bytes=peak,
                        )
                results.append(entry)
        finally:
            coordinator.close()
            fixture.tearDown()
    report = dict(
        baseline=args.baseline,
        python=sys.version,
        platform=platform.platform(),
        candidate_method_sha256=hashlib.sha256(inspect.getsource(candidate).encode()).hexdigest(),
        note=(
            "Actual list_sessions; baseline _catalog_sessions compiled from Git and candidate "
            "loaded from working tree. Synthetic warm catalog fixture. Fresh manifest identity "
            "checks and exact response equality retained. Not a cold-validation or Pi benchmark."
        ),
        results=results,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
