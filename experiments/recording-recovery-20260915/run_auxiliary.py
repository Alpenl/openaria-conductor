"""Short ablations for retained progress and finishing a full encoder pipe."""

# Local source imports must follow the checkout path setup below.
# ruff: noqa: E402

import inspect
import json
import selectors
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[2]
OUT = Path(__file__).parent
sys.path[:0] = [str(REPO), str(REPO / "src")]
from rp_ylx.recording import device_session
from tests.test_split_eye_recording import FakeSessionStore, SplitEyeRecordingTest

source = textwrap.dedent(inspect.getsource(device_session.DeviceSessionRecorder._abandon_encoder))
start = source.index("        # Preserve native progress")
end = source.index(
    "        with suppress(BaseException):\n            self._native_transaction.abort", start
)
ablated_source = source[:start] + source[end:]
scope = dict(vars(device_session))
exec(compile(ablated_source, "<ablation-progress-copy-removed>", "exec"), scope)
ablated = scope["_abandon_encoder"]
rows = []
for repeat in range(5):
    for remove_progress in [bool(repeat % 2), not bool(repeat % 2)]:
        with tempfile.TemporaryDirectory(prefix="openaria-progress-") as temporary:
            store = FakeSessionStore()
            selected = (
                ablated
                if remove_progress
                else device_session.DeviceSessionRecorder._abandon_encoder
            )
            with (
                patch("rp_ylx.recording.device_session.native_session_store", return_value=store),
                patch(
                    "rp_ylx.recording.device_session.resolve_executable",
                    return_value=Path("/bin/true"),
                ),
                patch.object(device_session.DeviceSessionRecorder, "_abandon_encoder", selected),
            ):
                recorder, _, _ = SplitEyeRecordingTest().build(
                    Path(temporary), native_data_plane=True
                )
                recorder.start()
                store.transaction.advance(frames=3, imu_samples=2)
                path = recorder.fail("source_sequence_gap", "source frame sequence has a gap of 8")
                state = json.loads((path / "recording.json").read_bytes())
                row = {
                    "experiment": "failure_progress",
                    "repeat": repeat,
                    "variant": "no_progress_snapshot" if remove_progress else "full",
                    "actual_frames": 3,
                    "reported_frames": state["progress"]["captured_frames"],
                    "reported_bytes": state["progress"]["bytes_written"],
                    "raw_index_bytes": (path / "frames.ndjson").stat().st_size,
                    "error_code": state["diagnostics"][0]["code"],
                }
                rows.append(row)

# Actual OS pipe operations corresponding to the old terminator write and new EOF.
# The reader intentionally stays open and does not consume data.
pipe_code = r"""
import fcntl,json,os,sys,time
r,w=os.pipe()
os.set_blocking(w,False)
filled=0
try:
 while True:filled+=os.write(w,b'x'*4096)
except BlockingIOError:pass
os.set_blocking(w,True)
print(json.dumps({'ready':True,'filled_bytes':filled}),flush=True)
start=time.perf_counter_ns()
if sys.argv[1]=='old_terminator':os.write(w,b'\0'*4)
else:os.close(w)
print(json.dumps({'operation_ms':(time.perf_counter_ns()-start)/1e6}),flush=True)
os.close(r)
"""
for repeat in range(3):
    for variant in ["full_eof", "old_terminator"]:
        child = subprocess.Popen(
            [sys.executable, "-c", pipe_code, variant],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=3), "pipe adapter failed to initialize"
        ready = json.loads(child.stdout.readline())
        row = {
            "experiment": "finish_pipe",
            "variant": variant,
            "repeat": repeat,
            "pipe_bytes": ready["filled_bytes"],
            "watchdog_ms": 300,
        }
        try:
            child.wait(timeout=0.3)
            out, error = child.stdout.read(), child.stderr.read()
            assert child.returncode == 0, error
            row.update(json.loads(out))
            row["blocked_at_watchdog"] = False
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()
            row["blocked_at_watchdog"] = True
        rows.append(row)

assert all(
    r["reported_frames"] == (0 if r["variant"] == "no_progress_snapshot" else 3)
    for r in rows
    if r["experiment"] == "failure_progress"
)
assert all(
    r["blocked_at_watchdog"] == (r["variant"] == "old_terminator")
    for r in rows
    if r["experiment"] == "finish_pipe"
)
(OUT / "auxiliary-results.json").write_text(json.dumps(rows, indent=2) + "\n")
for row in rows:
    print(json.dumps(row), flush=True)
