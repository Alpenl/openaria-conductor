import argparse
import gc
import importlib.util
import json
import statistics
import sys
import tempfile
import time
import tracemalloc
from pathlib import Path
from types import SimpleNamespace

parser = argparse.ArgumentParser(
    description="Measure actual native transaction snapshots using synthetic encoder events."
)
parser.add_argument(
    "--extension", type=Path, required=True, help="Release-built lib_native.so or _native.abi3.so"
)
parser.add_argument("--variant", choices=("baseline", "candidate"), required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--reference", type=Path, help="Baseline JSON to compare observable behavior")
parser.add_argument("--work-dir", type=Path, help="Optional external temporary-data directory")
parser.add_argument("--calls", type=int, default=1000)
parser.add_argument("--repeats", type=int, default=7)
args = parser.parse_args()
if args.calls <= 0 or args.repeats <= 0:
    parser.error("--calls and --repeats must be positive")
if args.work_dir is not None:
    args.work_dir.mkdir(parents=True, exist_ok=True)
variant = args.variant
module_path = args.extension.resolve()
spec = importlib.util.spec_from_file_location("_native", module_path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def transaction(root, count=0, malformed=False):
    (root / "video").mkdir()
    events = []
    for index in range(count):
        events.append(
            {
                "event": "segment",
                "index": index + int(malformed),
                "start_frame": index * 150,
                "end_frame": (index + 1) * 150,
                "left": {"path": f"video/left_{index:05d}.mp4", "bytes": 1024},
                "right": {"path": f"video/right_{index:05d}.mp4", "bytes": 1024},
            }
        )
    (root / "events.jsonl").write_text("".join(json.dumps(row) + "\n" for row in events))
    helper = root / "encoder-helper.py"
    helper.write_text(
        f"#!{sys.executable}\n"
        "import pathlib, sys\n"
        'sys.stdout.write(pathlib.Path(__file__).with_name("events.jsonl").read_text())\n'
        'print(\'{"event":"ready"}\', flush=True)\n'
        "sys.stdin.buffer.read()\n"
        'print(\'{"event":"done","frames":0}\', flush=True)\n'
    )
    helper.chmod(0o755)
    plan = SimpleNamespace(
        session_root=str(root),
        session_id="test-session",
        encoder_executable=str(helper),
        width=3840,
        height=1080,
        fps=30,
        bitrate_kbps=8192,
        segment_frames=150,
        encoder_arguments=[],
        recording_start_monotonic_ns=1,
        audio_enabled=False,
        audio_device="unused",
        audio_sample_rate_hz=48000,
        audio_channels=2,
        audio_segment_seconds=5.0,
    )
    return module.NativeSessionStore().begin_recording(plan), events


results = {
    "variant": variant,
    "calls_per_repeat": args.calls,
    "repeats": args.repeats,
    "scope": (
        "actual NativeSessionTransaction.snapshot PyO3 call; "
        "synthetic encoder events, no camera/media processing"
    ),
    "cases": [],
}
for count in (0, 120, 720, 2880):
    with tempfile.TemporaryDirectory(dir=args.work_dir) as temporary:
        tx, events = transaction(Path(temporary), count)
        try:
            snapshot = tx.snapshot()
            if variant == "baseline":
                assert len(snapshot.pop("segments")) == count
            else:
                assert "segments" not in snapshot
            explicit = tx.segments()
            assert len(explicit) == count
            for segment, event in zip(explicit, events, strict=True):
                assert segment == {
                    "index": event["index"],
                    "start_frame": event["start_frame"],
                    "end_frame": event["end_frame"],
                    "left_path": event["left"]["path"],
                    "left_bytes": 1024,
                    "right_path": event["right"]["path"],
                    "right_bytes": 1024,
                }
            for _ in range(100):
                tx.snapshot()
            timings = []
            for _ in range(args.repeats):
                start = time.perf_counter_ns()
                for _ in range(args.calls):
                    tx.snapshot()
                timings.append((time.perf_counter_ns() - start) / args.calls)
            gc.collect()
            tracemalloc.start()
            for _ in range(100):
                tx.snapshot()
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            results["cases"].append(
                {
                    "segments": count,
                    "equivalent_recording_seconds": count * 5,
                    "median_snapshot_ns": statistics.median(timings),
                    "repeat_snapshot_ns": timings,
                    "peak_python_bytes": peak,
                    "snapshot_without_removed_field": snapshot,
                }
            )
        finally:
            tx.abort("experiment finished")
            tx.abort("repeat abort")
            assert tx.open_handle_count() == 0

behaviors = {}
with tempfile.TemporaryDirectory(dir=args.work_dir) as temporary:
    tx, _ = transaction(Path(temporary))
    before = tx.snapshot()
    first = tx.finish(0.001, 2.0)
    second = tx.finish(0.001, 2.0)
    assert first == second
    assert tx.segments() == []
    after = tx.snapshot()
    before.pop("segments", None)
    after.pop("segments", None)
    behaviors["empty_finish"] = {
        "before": before,
        "after": after,
        "outcome": first,
        "open_handles": tx.open_handle_count(),
    }
with tempfile.TemporaryDirectory(dir=args.work_dir) as temporary:
    tx, _ = transaction(Path(temporary), 1, malformed=True)
    try:
        try:
            tx.segments()
            raise AssertionError("malformed segment registration accepted")
        except RuntimeError as error:
            behaviors["malformed_segments"] = str(error)
        try:
            tx.finish(0.001, 2.0)
            raise AssertionError("malformed segment finish accepted")
        except RuntimeError as error:
            behaviors["malformed_finish"] = str(error)
    finally:
        tx.abort("malformed experiment")
        behaviors["malformed_open_handles_after_abort"] = tx.open_handle_count()
results["behaviors"] = behaviors
if args.reference is not None:
    reference = json.loads(args.reference.read_text())

    def normalized(value):
        # Separate temporary roots necessarily have different inode/timestamps.
        # Preserve their types, and compare sizes, digests and all other fields.
        if isinstance(value, dict):
            return {
                key: type(item).__name__
                if key in {"device", "inode", "mtime_ns"}
                else normalized(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [normalized(item) for item in value]
        return value

    assert normalized(reference["behaviors"]) == normalized(behaviors)
    for before, after in zip(reference["cases"], results["cases"], strict=True):
        assert before["segments"] == after["segments"]
        assert before["snapshot_without_removed_field"] == after["snapshot_without_removed_field"]
    results["behavior_parity"] = True
out = args.output
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(results, indent=2) + "\n")
print(json.dumps({key: value for key, value in results.items() if key != "behaviors"}, indent=2))
