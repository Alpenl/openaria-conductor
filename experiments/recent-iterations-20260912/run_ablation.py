"""Compare a baseline checkout with the candidate; no hardware or network writes."""

from __future__ import annotations

import argparse
import gc
import hashlib
import inspect
import io
import json
import platform
import random
import statistics
import subprocess
import sys
import tempfile
import textwrap
import time
import tracemalloc
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def measure(operation, fingerprint, rounds):
    expected = fingerprint(operation())
    samples = []
    for _ in range(rounds):
        gc.collect()
        started = time.perf_counter()
        value = operation()
        samples.append((time.perf_counter() - started) * 1000)
        assert fingerprint(value) == expected
    gc.collect()
    tracemalloc.start()
    value = operation()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert fingerprint(value) == expected
    return {
        "median_ms": statistics.median(samples),
        "samples_ms": samples,
        "peak_python_bytes": peak,
        "output_sha256": expected,
    }


def catalog_probe(rounds, siblings, scan):
    from test_capture_coordinator import CaptureCoordinatorTest

    import rp_ylx.recording.coordinator as module

    if scan:
        # Isolate state simplification from direct lookup without shipping a switch.
        source = textwrap.dedent(inspect.getsource(module.CaptureCoordinator._catalog_sessions))
        start = source.index("\n            candidates = (") + 1
        end = source.index("\n                if (", start) + 1
        source = (
            source[:start]
            + "            for candidate in sessions_root.iterdir():\n"
            + source[end:]
        ).replace(
            "                discovered.add(candidate.name)",
            "                if verify_session is not None and candidate.name != verify_session:\n"
            "                    continue\n"
            "                discovered.add(candidate.name)",
        )
        namespace = dict(vars(module))
        exec(compile(source, "<catalog-state-only>", "exec"), namespace)
        module.CaptureCoordinator._catalog_sessions = namespace["_catalog_sessions"]

    fixture = CaptureCoordinatorTest()
    fixture.setUp()
    coordinator = fixture.coordinator()
    try:
        session_id = fixture.seal_one(coordinator, prefix="ablation")
        root = fixture.mountpoint / "recordings"
        manifest = json.loads((root / session_id / "manifest.json").read_bytes())
        artifact = manifest["video"]["segments"][0]["artifacts"]["left"]
        expected = (root / session_id / artifact["path"]).read_bytes()
        for index in range(siblings):
            (root / f"01900000-0000-7000-8000-{index:012x}").mkdir()
        roots = set(coordinator._require_admission().catalog_roots)
        original_iterdir, original_is_dir = Path.iterdir, Path.is_dir
        counts = {"catalog_entries": 0, "candidate_is_dir": 0, "full_verifications": 0}
        verify = module.validate_device_session_directory

        def counted_iterdir(path):
            for entry in original_iterdir(path):
                if path in roots:
                    counts["catalog_entries"] += 1
                yield entry

        def counted_is_dir(path):
            if path.parent in roots:
                counts["candidate_is_dir"] += 1
            return original_is_dir(path)

        def counted_verify(*args, **kwargs):
            counts["full_verifications"] += 1
            return verify(*args, **kwargs)

        def read():
            with coordinator.open_verified_artifact(
                session_id, artifact["artifact_id"], "v4"
            ) as opened:
                result = opened.read()
            assert result == expected
            return result.hex()

        # Timings exclude instrumentation; work counts come from a separate request.
        result = measure(read, digest, rounds)
        with (
            patch.object(Path, "iterdir", counted_iterdir),
            patch.object(Path, "is_dir", counted_is_dir),
            patch.object(module, "validate_device_session_directory", counted_verify),
        ):
            read()
        return {**result, "siblings": siblings, "work_per_request": counts}
    finally:
        coordinator.close()
        fixture.tearDown()


def timestamp_probe(rounds, frame_count):
    import rp_ylx.session_timestamps as production
    import rp_ylx.spectacular.model as calibration

    frames = [
        {
            "frame_index": i,
            "source_sequence": i,
            "uvc_sequence": i,
            "host_monotonic_ns": 1_000_000_000 + i * 33_333_333,
            "callback_monotonic_ns": 1_000_000_000 + i * 33_333_333,
        }
        for i in range(frame_count)
    ]
    sample_count = (frame_count * 400 // 30) // 2 * 2
    samples = [
        {
            "sample_number": i,
            "sequence": i,
            "packet_sequence": i // 2,
            "sample_index": i % 2,
            "device_timestamp_raw": i // 14,
            "device_ticks": i // 14,
            "host_monotonic_ns": 1_000_000_000 + (i // 2) * 5_000_000,
            "sync_quality": "insufficient",
            "sync": {"quality": "insufficient"},
        }
        for i in range(sample_count)
    ]
    timing = SimpleNamespace(
        frame_times_ns=tuple(float(f["host_monotonic_ns"]) for f in frames),
        imu_times_ns=tuple(997_500_000.0 + i * 2_500_000 for i in range(sample_count)),
        capture=SimpleNamespace(frames=tuple(frames), imu_samples=tuple(samples)),
        imu_time_basis="host_receive_interpolation",
    )
    manifest = {
        "schema": "ylx.device-session.v3",
        "session_id": "01900000-0000-7000-8000-000000000001",
        "capture_mode": "production",
        "camera": {"sensor_fps": 30.0, "frame_decimation": 1},
    }
    with tempfile.TemporaryDirectory() as temporary:
        output = Path(temporary) / "timestamps.csv"

        def production_csv():
            result = production.build_session_timestamp_report(temporary, output=output)
            result["csv"]["path"] = "timestamps.csv"
            return result

        def csv_fingerprint(value):
            return digest(
                {"result": value, "csv_sha256": hashlib.sha256(output.read_bytes()).hexdigest()}
            )

        # Ingestion/validation is unchanged: isolate the new reporting/export layer.
        with (
            patch.object(production, "validate_public_session", return_value=manifest),
            patch.object(production, "_load_frames", return_value=frames),
            patch.object(production, "_load_imu", return_value=samples),
        ):
            results = {
                "production_summary": measure(
                    lambda: production.build_session_timestamp_report(temporary), digest, rounds
                ),
                "production_csv": measure(production_csv, csv_fingerprint, rounds),
                "calibration_summary": measure(
                    lambda: calibration.frame_timestamp_alignment_summary(timing), digest, rounds
                ),
                "calibration_csv": measure(
                    lambda: calibration.write_frame_timestamp_csv(timing, output),
                    csv_fingerprint,
                    rounds,
                ),
            }
        return {"frames": frame_count, "imu_samples": sample_count, "operations": results}


def negative_probe():
    from test_capture_coordinator import CaptureCoordinatorTest

    import rp_ylx.recording.coordinator as module

    name = "test_artifact_access_rejects_corruption_in_another_artifact"

    def run():
        log = io.StringIO()
        result = unittest.TextTestRunner(stream=log).run(CaptureCoordinatorTest(name))
        return {
            "passed": result.wasSuccessful(),
            "failures": len(result.failures),
            "errors": len(result.errors),
            "log": log.getvalue(),
        }

    intact = run()
    with patch.object(
        module,
        "validate_device_session_directory",
        side_effect=lambda path, **_: module.inspect_device_session_directory(path)[0],
    ):
        ablated = run()
    assert intact["passed"] and ablated["failures"] == 1 and ablated["errors"] == 0
    return {"test": name, "intact": intact, "without_session_payload_validation": ablated}


def nearest_probe():
    import rp_ylx.session_timestamps as production
    import rp_ylx.spectacular.model as calibration

    rng = random.Random(20260912)
    results = []
    for _ in range(10000):
        # Duplicates, exact hits, ties, outside endpoints, and large monotonic clocks.
        origin = rng.choice((0, 10**9, 10**16))
        values = sorted(float(origin + rng.randrange(1000)) for _ in range(rng.randrange(1, 65)))
        target = rng.choice(
            (
                values[0] - 1,
                values[-1] + 1,
                rng.choice(values),
                (values[0] + values[-1]) / 2,
                float(origin + rng.randrange(1000)),
            )
        )
        for nearest, sequence in (
            (production._nearest_index, values),
            (calibration._nearest_index, tuple(values)),
        ):
            index = nearest(sequence, target)
            assert abs(values[index] - target) == min(abs(value - target) for value in values)
            results.append(index)
    assert production._nearest_index([0.0, 10.0], 5) == 0
    assert calibration._nearest_index((0.0, 10.0), 5.0) == 0
    return {"cases": len(results), "output_sha256": digest(results)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--candidate", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--output", type=Path)
    parser.add_argument("--rounds", type=int, default=7)
    parser.add_argument("--siblings", type=int, default=2000)
    parser.add_argument("--frames", type=int, default=18000)
    parser.add_argument(
        "--probe", choices=("catalog", "catalog-scan", "timestamps", "negative", "nearest")
    )
    parser.add_argument("--source", type=Path)
    args = parser.parse_args()
    if args.probe:
        sys.path[:0] = [str(args.source / "src"), str(args.candidate / "tests")]
        if args.probe.startswith("catalog"):
            result = catalog_probe(args.rounds, args.siblings, args.probe == "catalog-scan")
        elif args.probe == "timestamps":
            result = timestamp_probe(args.rounds, args.frames)
        elif args.probe == "nearest":
            result = nearest_probe()
        else:
            result = negative_probe()
        print(json.dumps(result))
        return
    if args.baseline is None or args.output is None:
        parser.error("--baseline and --output are required")
    result = {
        "python": sys.version,
        "platform": platform.platform(),
        "baseline_commit": subprocess.check_output(
            ["git", "-C", str(args.baseline), "rev-parse", "HEAD"], text=True
        ).strip(),
        "probes": {},
    }
    for label, root, probe in (
        ("baseline_catalog", args.baseline, "catalog"),
        ("state_only_catalog", args.candidate, "catalog-scan"),
        ("candidate_catalog", args.candidate, "catalog"),
        ("baseline_timestamps", args.baseline, "timestamps"),
        ("candidate_timestamps", args.candidate, "timestamps"),
        ("baseline_nearest", args.baseline, "nearest"),
        ("candidate_nearest", args.candidate, "nearest"),
        ("negative_control", args.candidate, "negative"),
    ):
        print(f"Running {label}", file=sys.stderr, flush=True)
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--probe",
            probe,
            "--source",
            str(root.resolve()),
            "--candidate",
            str(args.candidate.resolve()),
            "--rounds",
            str(args.rounds),
            "--siblings",
            str(args.siblings),
            "--frames",
            str(args.frames),
        ]
        completed = subprocess.run(command, capture_output=True, text=True)
        if completed.returncode:
            raise RuntimeError(f"{label}: {completed.stderr}")
        result["probes"][label] = json.loads(completed.stdout)
    probes = result["probes"]
    assert probes["baseline_nearest"] == probes["candidate_nearest"]
    for name in ("state_only_catalog", "candidate_catalog"):
        assert probes[name]["output_sha256"] == probes["baseline_catalog"]["output_sha256"]
    for name, before in probes["baseline_timestamps"]["operations"].items():
        assert (
            before["output_sha256"]
            == probes["candidate_timestamps"]["operations"][name]["output_sha256"]
        )
    result["source_sha256"] = {
        label: {
            str(path): hashlib.sha256((root / path).read_bytes()).hexdigest()
            for path in (
                Path("src/rp_ylx/recording/coordinator.py"),
                Path("src/rp_ylx/session_timestamps.py"),
                Path("src/rp_ylx/spectacular/model.py"),
                Path("src/rp_ylx/_native.abi3.so"),
            )
        }
        for label, root in (("baseline", args.baseline), ("candidate", args.candidate))
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(args.output)


if __name__ == "__main__":
    main()
