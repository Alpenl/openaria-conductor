# Open Aria structural ablation: recording ownership

Date: 2026-09-08 (Asia/Shanghai)

Integration note (2026-09-09): the measurements below describe the original ABI 4
experiment. The ABI 5 integration retains its newer CaptureEngine/SessionStore
interfaces and ports the flattened fanout state and direct download forwarding.
Removed ABI 4 Python/PyO3 interfaces are not reintroduced. See the integration
report for current tests; the historical measurements are not measurements of this merge.

## Result

Starting from `6ce782d74d7653368b31d20c50c76776b1d577d4`, this follow-up audit
removed unused native recording modes, flattened nested frame state, and
simplified the download adapter. The implementation and test patch spans seven
files, with 403 insertions and 994 deletions: a net reduction of 591 lines,
excluding this experiment directory and its README link.

All 711 Python tests still pass. Rust coverage increased from 69 to 70 tests.
The baseline and candidate produce identical results for 400,000 deterministic
traces containing 2,000,000 state transitions. The optimized wheel is 10,902 bytes
smaller (0.962%); its native extension is 21,384 bytes smaller (1.164%).

Measurements were taken from working-tree changes on top of the baseline commit.
Both measured wheel build identities therefore contain the parent commit. The source patch
digest and wheel digests in [results.json](results.json) distinguish the
candidate from the baseline; these wheels are local experiment artifacts.

## Ablation Decisions

| Step | Removed | Evidence and resulting behavior |
| --- | --- | --- |
| A1 | Native frame/IMU callbacks, `RecordingTarget`, `RecordingDispatch`, `ImuSubmitTarget`, and their PyO3 entry point | Successful production and benchmark setup, in `daemon.py` and `performance/benchmark.py`, supplies `DeviceSessionRecorder` and uses its native split sink. Only tests deliberately selected callback mode. Native capture now rejects a missing sink in both production and calibration modes. One shared `SplitSinkRecording` holds the resources through each frame write. |
| A2 | `require_native_imu=False`, the additional Python IMU worker, and native use of the general Python recording tap | The opt-out was selected only by test doubles. Native capture always uses the Rust IMU owner and reads its latest observation directly for telemetry. Its Python state now contains only the IMU handle and failure callback. The separate `ContinuousCaptureSources` Python implementation remains available. |
| A3 | `RecordingFrameGate`, `RecordingTapState`, and three intermediate decision/snapshot types | Only `CaptureFanoutState` consumed these layers after the previous interface cleanup. It now owns decimation, first-frame handling, inflight counts, stopping, and failure suppression directly. A plain optional recording state holds the per-take counters. |
| A4 | Coordinator-local `_Representation` protocol and the wrapper's second socket write loop | Both directory-store results inherit `LockedBytes`, which already owns verified file transmission and the Python fallback. The coordinator uses those concrete types and forwards `send_to` directly. `_TrackedRepresentation` still releases the coordinator's handle reservation exactly once. |

A1 and A2 each passed the 19 capture-source tests before the next step. A3
passed the full Rust suite. A4 passed all 102 coordinator/download tests.
All steps were then validated together with freshly built native code.

The callback-conversion tests were rewritten around the supported direct-sink
path. They continue to check camera configuration, raw SBS input, native IMU
ownership, current IMU telemetry, failure cleanup, and retry after a failed
worker. Missing split sinks are now tested for both recording modes.

Rust stop/drain tests now construct actual split-sink resources. The frame-write
test exercises `process_frame`, including its GIL release and shared resource
lifetime. A new test verifies that a missing raw frame reports one failure,
drains the inflight frame, stops recording, and leaves preview running.

## Deterministic Comparison

[compare_fanout.py](compare_fanout.py) builds the actual baseline and candidate
Rust modules in temporary directories using the repository's locked Cargo
dependencies. [fanout_trace.rs](fanout_trace.rs) runs every length-five sequence
from ten operations at frame-decimation values 1, 2, 3, and 60:

- Start recording, finish a frame, request stop, and report failure.
- Receive a frame with or without preview availability.
- Receive source gaps of 1, 3, `u64::MAX - 2`, and `u64::MAX`.

Each operation's complete debug result, including error code/message, and the
complete fanout snapshot enter the same ordered SHA-256 stream. This covers
invalid ordering, duplicate starts/failures, first-frame gap suppression,
decimation, inflight draining, preview independence, and counter overflow.

Both versions produced:

```text
400,000 traces
2,000,000 transitions
SHA-256: 08c0e50020435a344d0a9c24cf064fc69472aea4913d6caedc81f9edce05c13a
```

The module digests and comparison result are in
[fanout-comparison.json](fanout-comparison.json). This is a bounded behavior
comparison, not a proof over all sequence lengths or thread schedules.

To reproduce, check out the baseline commit in a separate directory, then run
from the candidate checkout with Python and Rust available:

```bash
shnote --what "Compare fanout implementations" --why "Check structural ablation behavior" run \
  uv run --frozen python experiments/code-structure-20260908/compare_fanout.py \
  /path/to/baseline . --output /tmp/fanout-comparison.json
```

A Python distribution with a nonstandard shared-library location may require
its library directory in `LD_LIBRARY_PATH`. This host used
`/home/alpen/miniconda3/lib` for Rust tests and the comparison driver.

## Repository Audit

The audit examined Python and Rust type definitions, production constructors,
dynamic dispatch sites, native exports, CLI/benchmark entry points, packaging,
and the complete hardware-free test suite. The deletion decisions above are
based on reachable product paths, not type names or line counts alone.

| Area | Retained responsibility |
| --- | --- |
| Camera, IMU, JPEG, audio, encoder | Resource ownership, real hardware operations, fault injection, and process isolation for the hardware encoder. Camera/native protocols remain useful testing boundaries. |
| Recording and storage | Active-take accounting, bounded queues, segment planning, durable sealing, admission checks, and legacy session readers protect different invariants. |
| API, SSE, schemas | Versioned client contracts, validation, replay order, authentication, and download integrity remain intact. Capture and network event streams have different revision and transaction rules. |
| Network and deployment | Privileged network operations, credential reservations, durable state, and atomic release installation are distinct failure boundaries. |
| Spectacular adapter | Session loading, timestamp reconstruction, and compatibility readers perform real format conversions. |
| Performance and CLI | Both explicit benchmark adapters and the opt-in metrics path remain usable. This experiment does not change the measured JPEG splitter. |
| Echo / Web | Pinned upstream build artifacts and their resource/hash contract remain intact. |
| Build and scripts | The PEP 517 commit binding and offline bundle tooling remain the existing build owners. No dependency or CI changes were needed. |

Vulture at 80% confidence reported 39 unused parameter names in context-manager,
framework, and Protocol signatures, and no unused classes or functions.

## Package Measurements

Both variants were built with `uv build --wheel` from the same locked
dependencies, using separate Cargo target directories. The baseline came from
the exact committed tree; the candidate came from the working tree.

| Artifact | Baseline | Candidate | Delta |
| --- | ---: | ---: | ---: |
| Optimized wheel | 1,133,658 B | 1,122,756 B | -10,902 B |
| Native extension, compressed | 717,652 B | 707,379 B | -10,273 B |
| Native extension, uncompressed | 1,836,440 B | 1,815,056 B | -21,384 B |

This host was x86_64 Linux with Python 3.13.13 and Rust 1.93.1. The wheel tag
was `cp311-abi3-linux_x86_64`. Package size is not a throughput, CPU, or resident
memory measurement.

## Verification

| Check | Baseline | Candidate |
| --- | --- | --- |
| `python scripts/check.py` | 711 tests, 175.038 s; Ruff passed | 711 tests, 168.172 s; Ruff passed |
| `cargo test --workspace --all-targets` | 69 tests | 70 tests |
| Fanout trace comparison | 2,000,000 transitions | Identical ordered digest |
| `cargo clippy --workspace --all-targets -- -D warnings` | Not rerun | Passed |
| `git diff --check` | Clean starting tree | Passed |

The Python suite includes independent wheel and sdist-to-wheel installation,
exact commit identity, CLI aliases, and all packaged resource hashes. A loaded
candidate-extension probe also confirmed that `start_recording_split_sink`
exists and the removed callback `start_recording` method does not.

The initial in-place Rust baseline reused a stale build containing an extra
audio test absent from the committed source. That result was discarded. Both
baseline suites and the baseline wheel were rebuilt in an isolated checkout
with a separate Cargo target directory; the corrected baseline is used above.

No RDK X5 recording or hardware throughput experiment was run. Before shipping,
verify preview, production/calibration capture, audio-enabled recording,
stop/retry, and session download on the target using one matching Python/Rust
bundle. The removed native callback method and mixed-IMU option were internal
interfaces; external Device API versions and session formats are unchanged.
