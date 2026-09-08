# Follow-up: frame copies and status polling

Date: 2026-09-08 (Asia/Shanghai)
Baseline: `b9a644840ad65df563f32364be5751167a15d065`.

## Changes

| Area | Implementation | Preserved behavior |
| --- | --- | --- |
| Native recording | Borrow the immutable `PyBytes` payload as `&[u8]` through synchronous encoder submission instead of calling `to_vec()`. | The frame owns the payload until submission returns; the GIL remains released during writing, with the same failure accounting and stop/drain behavior. |
| Camera focus validation | Share one ordinary function between the native, SSE, and HTTP boundaries. | Native accepts `dict`/`None` and integer subclasses excluding booleans; API paths accept `Mapping` and require exact integers. Existing shape/value error messages and default-value rules remain intact. |
| Production startup | Replace 16 repeated capability branches with ordered data and a short loop. | Error codes, messages, first-error priority, optional audio, and failure before resource creation are unchanged. |
| Status polling | Sample runtime, camera, and IMU outside the coordinator lock; assemble recording identity and fresh IMU under the lock. Supply a lightweight recording-progress read to the event pump. | Idle polling still retries camera recovery; storage admission still retries. Changed source revisions publish full snapshots and use the revision actually returned. |

The native change removes one allocation and one complete JPEG copy per recorded
frame. That is a structural result; native CPU use, throughput, and target-device
frame rate were not measured.

## Host Experiment

[status_probe.py](status_probe.py) runs the actual coordinator and event pump
against the existing test suite's fake sources and encoder. It compares the
baseline Python source with the candidate using the same installed, ABI-compatible
native extension. It does not contact a camera or RDK device.

For each of five trials, a status thread pauses for 300 ms in its focus read.
After that read starts, another thread stops a recording containing one frame.
The measured stop duration includes session sealing and verification. Separately,
the actual event-pump loop is driven through 25 ticks with an unchanged recording
revision; loop waits are mocked so this measures call counts, not a real-time
five-second recording.

| Measurement | Baseline | Candidate |
| --- | ---: | ---: |
| Stop duration, median of five trials | 383.75 ms | 60.19 ms |
| Full runtime reads in 25 active progress ticks | 25 | 0 |
| Focus reads in 25 active progress ticks | 25 | 0 |

The individual samples and coordinator source hashes are in
[follow-up-results.json](follow-up-results.json). These are synthetic contention
measurements on the development host, not RDK latency guarantees or encoder
performance results.

To reproduce, make the baseline checkout and candidate checkout importable with
their native extensions and dependencies available, then run from the candidate:

```bash
shnote --what "Measure baseline polling" --why "Compare coordinator contention" run \
  uv run --frozen --extra dev python experiments/code-structure-20260908/status_probe.py /path/to/baseline
shnote --what "Measure candidate polling" --why "Compare coordinator contention" run \
  uv run --frozen --extra dev python experiments/code-structure-20260908/status_probe.py .
```

## Concurrency Checks

Before the coordinator change, blocking runtime, camera connection, focus, or IMU
sampling prevented stop/restart from completing within the test's one-second
deadline. All four cases now complete while the sample is still blocked and
return the current session, storage generation, revision, and IMU afterward.

Additional deterministic regressions cover:

- Old-session observations cannot enter a new session's cache.
- A slower IMU call cannot overwrite a later sample; repeated samples do not
  extend freshness, and stale samples are omitted.
- A concurrent focus write triggers resampling, including partial hardware writes
  followed by an error. Failed writes keep their existing public revision/error
  semantics. A local focus counter avoids retrying hardware reads for every
  ordinary recording checkpoint.
- A sealed recording awaiting payload verification has no active recording in
  its capture snapshot and therefore exposes no live IMU there.
- If a full event snapshot advances past the revision observed by a lightweight
  read, the event pump follows the returned revision without duplicate publication.

The command transaction locks and their idempotent response construction remain
in place. This change removes polling-induced contention; it does not make every
control operation independent of hardware latency.

## Verification

- Fresh native extension built with `uv sync --frozen --extra dev --reinstall-package rp-ylx`.
- `python scripts/check.py`: 723 tests passed in 145.671 s, plus Ruff lint/format.
  This includes independent wheel and sdist installation, build identity,
  CLI aliases, and packaged resource verification.
- `cargo test --workspace --all-targets`: 70 tests passed.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `cargo fmt --all -- --check` and `git diff --check`: passed.

RDK preview, recording, audio synchronization, and physical camera recovery
remain target-device acceptance checks. No device was deployed or modified.
