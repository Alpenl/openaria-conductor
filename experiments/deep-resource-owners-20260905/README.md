# Python/Rust deep resource-owner ablation

Date: 2026-09-05 (Asia/Shanghai)

Issue: `mirrorbloom/RP-YLX#74`

## Decision

Keep Python and Rust. Do not add Go.

The production recording data plane now has two constructible native owners:

- `CaptureEngine` owns the camera stream, V4L2 mappings, continuous capture
  runtime, IMU collector, recording gate, and frame fanout.
- `SessionStore` creates `SessionTransaction`, which owns the active take,
  recording sink, segment planner, encoder helper process, optional ALSA audio
  recorder, artifact identity, durable publication, and verified reads.

Python still owns admission policy, idempotency, recovery, manifest product
semantics, API projection, deployment, NetworkManager, CLI, and process
assembly. Captured frames, audio chunks, IMU samples, encoder events, and file
writes do not call Python. The exact current construction graph is available in
[owner-graph.html](owner-graph.html).

The timeboxed `GatewayRuntime` experiment is a **NO-GO**. Its isolated Rust
harness proves that immediate peer cancellation, RAII permits, TLS, replay,
slow-client isolation, shutdown drain, and a bounded Python control queue are
feasible. It does not satisfy the same-stage deletion gate for the complete
Python HTTP/TLS/auth/CORS/body-limit/SSE/error-contract owner. Shipping it
would therefore add a second gateway rather than remove one. The fixed Python
gateway from Issue #73 remains the sole production listener.

## Scope and baseline

The requested comparison baseline is product commit
`5d4c74bd6bab9760f7fbf63337d1b89f1bdf73d5`. At that commit, Python declared
22 native Protocols and Rust exported the corresponding 22 PyO3 classes.
Python constructed and connected most of those Rust internals individually.

The candidate declares seven Protocols and exports seven classes. Only
`NativeCaptureEngine` and `NativeSessionStore` are independently constructed
recording owners. `NativeSessionTransaction` and `NativeMultipartPreview` are
returned views, `NativePreviewBuffer` and `NativePerformanceMetrics` are
shared support surfaces needed by the retained Python gateway and reporting,
and `NativeSplitter` is restricted to the explicit fixed-trace benchmark.

The baseline did not contain dead Protocol declarations: each one had a source
caller. This work is consolidation of reachable production interfaces, not a
claim that reachable code was dead. Earlier repository-wide ablation removed
the genuinely unreachable branches first.

## Baseline owner inventory

Every Protocol requested by Stage A is classified below. `production` means it
was reachable from the packaged daemon or a packaged operational command;
`production/benchmark` is reachable only through the explicit performance
command. There were no `legacy-read`, `test-only`, or `dead` entries among the
22 Protocols at the issue baseline.

| Baseline Protocol | Classification | Candidate disposition |
| --- | --- | --- |
| `NativeSplitter` | production/benchmark | Retained only for fixed-trace comparison; not constructed by the daemon. |
| `NativeCamera` | production | Removed; internal to `CaptureEngine`. |
| `NativeCameraFrameValidator` | production | Removed; stream validation is internal to camera/capture. |
| `NativeAudioRecorder` | production | Removed; optional recorder is owned by `SessionTransaction`. |
| `NativeTimeline` | production | Removed; monotonic timing lives with its Rust consumer. |
| `NativeActiveTakeWriter` | production | Removed; owned by `SessionTransaction`. |
| `NativeImuCollector` | production | Removed; opened and closed by `CaptureEngine`. |
| `NativeRecordingCodec` | production | Removed; encoding helpers are internal to the sink. |
| `NativeRecordingSink` | production | Removed; owned by `SessionTransaction`. |
| `NativeRecordingFrameGate` | production | Removed; internal to `CaptureEngine`. |
| `NativeRecordingTapState` | production | Removed; internal to `CaptureEngine`. |
| `NativeCaptureFanoutState` | production | Removed; internal to `CaptureEngine`. |
| `NativeContinuousCaptureRuntime` | production | Removed; internal to `CaptureEngine`. |
| `NativeRecordingSegmentPlanner` | production | Removed; owned by `SessionTransaction`. |
| `NativeRecordingEventQueue` | production | Removed; Rust owners communicate internally. |
| `NativeStereoEncoderEvents` | production | Removed; parsed inside `SessionTransaction`. |
| `NativeStereoEncoderPipe` | production | Removed; frame transport stays inside Rust. |
| `NativeStereoEncoderProcess` | production | Removed; helper-process isolation is retained inside `SessionTransaction`. |
| `NativeSessionIo` | production | Removed; operations moved behind `SessionStore`/`SessionTransaction`. |
| `NativeMultipartPreview` | production | Retained as a returned view while Python remains the HTTP stream owner. |
| `NativePreviewBuffer` | production | Retained as the latest-only boundary shared by capture and Python gateway. |
| `NativePerformanceMetrics` | production | Retained as a process-wide observation sink; not a recording assembler. |

## Current production call graph

The graph was traced from `build_production_service`, through
`NativeContinuousCaptureSources`, `CaptureCoordinator`, and
`DeviceSessionRecorder`, to every native factory and method call.

| Python owner | Native boundary | Frequency and ownership consequence |
| --- | --- | --- |
| daemon | `native_capabilities` | One startup probe; production fails before side effects if deep capabilities are absent. |
| daemon/preview | `NativePreviewBuffer()` | One process-level shared latest-only view. |
| daemon/metrics | `NativePerformanceMetrics()` | One process-level metrics sink. |
| capture sources | `NativeCaptureEngine(plan, preview, metrics)` | One engine per camera open/reopen, followed by one `start_preview`. |
| recorder | `SessionStore.begin_recording(plan)` | One transaction per recording. |
| capture sources | `engine.start_recording(transaction, failure_callback)` | One handoff per recording; only terminal failure invokes Python asynchronously. |
| capture runtime | Rust-internal transaction resources | Zero PyO3 calls per captured video frame, audio chunk, IMU sample, or encoder event. |
| recorder status | `transaction.snapshot()` | At most one call per user-visible progress snapshot; independent of payload count. |
| recorder segment harvest | `transaction.segments()` | One call per 200 ms harvest tick; newly closed artifacts are finalized through `SessionStore`. |
| capture status | `engine.latest_imu_observation()` | Bounded by control-plane/status polling, not IMU sample rate. |
| stop | `engine.stop_recording()` + `transaction.finish()` | One call to each; both are idempotent/bounded owners. |
| manifest projection | `transaction.boundary()` | Two low-frequency calls per encoded segment; no call per frame. |
| durable publication | `transaction.seal(...)` | One call; verifies identities, writes/fsyncs manifest, removes control files, renames, and fsyncs parent. |
| artifact HTTP | `SessionStore.open_verified_artifact(...)` | One combined no-follow open, identity check, size check, and SHA-256 verification per uncached representation. |
| preview JPEG | `preview.jpeg()` | One call per still-image request. |
| multipart preview | `preview.multipart_stream()` / iterator | One open, one PyO3 iterator step per delivered preview frame, one idempotent close. This remains because GatewayRuntime is NO-GO. |

The remaining per-segment calls are deliberately low-frequency integrity and
manifest boundaries. Moving manifest policy into Rust merely to remove them
would duplicate the schema owner and violate the issue's language split.

## Minimal interfaces and error models

### CaptureEngine

Input is one immutable `NativeCapturePlan`. Recording begins with exactly one
`SessionTransaction`. The engine exposes preview start, recording start/stop,
bounded snapshots, focus control, latest IMU observation, and close.

Construction and runtime errors retain stable code/message pairs. Invalid
plans use `invalid_argument`; camera/V4L2 and IMU codes are preserved; repeated
stop/close is safe; a recording terminal failure is latched once and delivered
through the one failure callback. Python never receives a frame callback in
the native path.

### SessionStore and SessionTransaction

Input is one immutable `NativeRecordingPlan`. `begin_recording` acquires the
active-take, sink, encoder, planner, and optional audio resources as one
transaction; partial acquisition unwinds before returning an error. The
transaction owns snapshot, finish, abort, segment projection, handle count,
and final seal. Store-level methods cover verified reads and the limited
artifact operations also needed by legacy readers and HTTP transport.

Errors are fail-closed code/message pairs. Important classes include
`invalid_argument`, `invalid_state`, `session_transaction_poisoned`,
`storage_unavailable`, `write_failed`, `digest_mismatch`, `artifact_invalid`,
`segment_invalid`, and encoder/audio-specific failures. `finish`, `abort`, and
resource close paths are idempotent. A failed finish aborts every subordinate
resource and can never publish a success manifest.

### GatewayRuntime experiment

The experiment interface accepts TCP or TLS streams, owns one scoped permit
per preview/SSE session, and records one close reason. Peer EOF/RST, handler
error, and server shutdown all drop the same RAII session. Control calls use a
capacity-one queue to a single Python fake dispatcher; queue saturation is an
explicit retryable 503 and cannot block accept/health handling.

This interface is intentionally confined to `experiments/`; no feature flag,
PyO3 class, listener, runtime dependency, or production fallback was added.

## Production fallback reachability

| Surface | Source/test behavior | Packaged production behavior |
| --- | --- | --- |
| camera + IMU data plane | Python adapters require both factories to be explicitly injected. | Daemon rejects missing `capture_engine`; no Python fallback is selectable. |
| recorder/writer/encoder/audio | Test hooks can exercise Python fixtures. | `DeviceSessionRecorder` requires a native transaction and rejects production hooks. |
| SessionStore reads | Optional probe supports source checkout and isolated tests. | Daemon gates `session_store`; the process-wide owner is mandatory. |
| preview buffer and metrics | Python implementations keep source-checkout contract tests usable. | Daemon gates both native capabilities before construction. |
| JPEG splitter | Explicit Python adapter exists for comparison tests. | Hardware/native benchmark fails closed when its requested native capability is absent. |
| gateway | Python is the selected production implementation. | This is not a fallback: the Rust experiment is rejected and never packaged. |

## Deletion ledger

The implementation removes 18 old PyO3 owner classes and introduces three
transaction/deep-owner classes, a net reduction of 15 classes. It removes the
matching Protocols, factories, Python wrappers, duplicate V4L2/IMU production
adapters, encoder-pipe assembly, and tests that reached through those internal
interfaces. Behavioral tests now target `CaptureEngine`, `SessionStore`, or an
external API/session contract.

Implementation commit `d75ffa7f74f8bad453b1015aac06af2af9b5307d`
contains 2,953 insertions and 6,057 deletions across 34 product/test files, a
net reduction of 3,104 lines. Documentation and the isolated experiment are
not included in that product delta.

Stage expectations and results:

| Stage | Expected old interface deletion | Result |
| --- | --- | --- |
| A | None; facts only. | Owner inventory, call-frequency table, error models, graph, and JSON baseline added. |
| B | Camera, validator, runtime, gate/tap/fanout, IMU and recording internals. | Removed from Python/PyO3 construction; collapsed behind `CaptureEngine` plus one transaction handoff. |
| C | Duplicate Python V4L2/IMU/writer/encoder production paths. | Removed or limited to explicitly injected tests; no production feature flag restores them. |
| D | Only proceed if the complete Python gateway can be deleted in the same stage. | NO-GO; experiment remains isolated and all production code is unchanged. |
| E | Python stream loops/listener, but only after Stage D GO. | Not applicable because Stage D is NO-GO. |
| F | `NativeSessionIo` and independent recording resource factories. | Removed; consolidated into `SessionStore`/`SessionTransaction`. |

## GatewayRuntime experiment

The isolated crate is under `../gateway-runtime-20260905/harness`. It uses
`rustls`, standard-library sockets/threads, scoped permits, and no application
route code. The driver runs the same preview/capture-SSE/network-SSE abort
cycle against the fixed Python gateway and the Rust harness, in plain HTTP and
customer-equivalent TLS. It also checks:

- frozen preview disconnect and reacquisition within 250 ms;
- Last-Event-ID replay without replaying the cursor event;
- a non-reading client does not delay a health request;
- four simultaneous 400 ms Python fake calls produce both 200 and bounded 503
  responses while health remains responsive;
- shutdown cancels frozen preview/SSE and drains all permits within two
  seconds;
- process CPU, RSS, thread count, file-descriptor count, latency, rejection
  count, and acquired/released permit counts.

Reproduce after building the isolated binary:

```bash
cargo test --manifest-path experiments/gateway-runtime-20260905/harness/Cargo.toml
cargo clippy --manifest-path experiments/gateway-runtime-20260905/harness/Cargo.toml \
  --all-targets -- -D warnings
cargo build --release \
  --manifest-path experiments/gateway-runtime-20260905/harness/Cargo.toml
uv run --frozen python experiments/gateway-runtime-20260905/run_experiment.py
```

The committed formal result is `../gateway-runtime-20260905/result.json`.
Absolute RSS is informative but not a product forecast: the harness implements
only experimental routes, while Python loads the complete product gateway.
The go/no-go decision rests on deletion and compatibility, not on this
intentionally unequal binary-size comparison.

## Verification and limitations

Host verification covers all Python tests, all Rust tests, strict Ruff,
strict Clippy, wheel/sdist installation, exact public contracts, the isolated
harness, and the protocol/resource experiment. Machine-readable structural and
hardware fields live in [baseline.json](baseline.json).

No RDK X5 CPU/RSS/thread/fd/queue/drop/stop-latency claim is inferred from the
x86_64 host. Those fields remain explicitly unavailable until the exact public
artifact is installed on a reachable RDK X5. Issue #74 must not close before
that target evidence exists, and Issue #73 must close first.
