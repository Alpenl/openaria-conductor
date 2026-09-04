# Open Aria fixed-trace data-plane ablation

Date: 2026-09-05 (Asia/Shanghai)

## Result

On a real 3840x1080 YLX side-by-side JPEG, moving the JPEG boundary,
dimension, and adapter path from Python to Rust increased fixed-trace
throughput from **66.578 +/- 0.131 FPS** to **72.154 +/- 0.146 FPS**. The
paired mean improvement was **8.376%** (95% Student-t CI: 8.161% to 8.591%;
paired bootstrap CI: 8.200% to 8.554%). CPU time per frame fell from
14.965 ms to 13.858 ms, a paired reduction of **7.392%**.

Both variants exceeded the nominal 60 FPS target on this host, and all
measured runs reported zero application drops and zero unknown gaps. Python
and Rust produced byte-identical left/right JPEG outputs.

This is valid fixed-trace evidence for the x86_64 host only. It is not RDK X5
hardware, V4L2 capture, recording, or concurrent-preview evidence.

## Question and variants

The experiment asks whether the Rust splitter is useful when the expensive
lossless crop is held constant.

- `python`: `split_sbs_mjpeg`, with JPEG marker/dimension handling in Python
  and the lossless crop through the TurboJPEG C API via `ctypes`.
- `rust`: `NativeSplitter.split`, with marker/dimension handling in Rust and
  the lossless crop through the same TurboJPEG C API via `libloading`.

The comparison therefore measures the adapter, validation, marker parsing,
and buffer handling around the common C transform. It does not compare two
different JPEG codecs.

## Inputs

The primary trace is a historical YLX Device API preview captured during
hardware acceptance:

- format: one baseline 8-bit, three-component JPEG;
- dimensions: 3840x1080;
- bytes: 395,403;
- SHA-256: `757fc8e484d31fb008485da5168150807eb51088b0abfc9572bd030f272482b8`;
- local source: `/data2/openaria-score-46/f05-20260830-continue-staged-assets/rdk-true-device/f05-20260830-rdk-071531Z/acceptance/preview-during-first-catalog.jpg`.

The image is intentionally not copied into this public repository because it
is real camera imagery. Reproduction requires a file with the exact digest
above, or a separately disclosed equivalent trace.

The mechanism control concatenates the two verified lossless crop outputs.
It takes the documented two-JPEG pass-through branch and removes the crop
transform from the measured path:

- bytes: 395,438;
- SHA-256: `554e87167e4a8036b932ef1641974dc1ebf9f99e10209d7358048083526173ee`.

For both inputs, the Python and Rust outputs were byte-identical:

| Eye | Bytes | SHA-256 |
| --- | ---: | --- |
| Left | 202,812 | `72ca2ceae7a444637e28d019abbfdd553c15a9e57acc9d5b53ebd0b0e826b938` |
| Right | 192,626 | `baeec212a52c424c9e670aa62618f1ca2524657d4d1e55cac5e742f2713f0075` |

## Protocol

- Source identity: clean detached worktree at
  `132783d2885c7468d42186c6131cbe53f9198bb7`.
- Wheel SHA-256:
  `0b57872d2cf8fd69e8bbece33367e85094aa14219c3012dedd58bb478f2b7ffe`.
- Wheel tag: `cp311-abi3-linux_x86_64`.
- Python: 3.11.14.
- TurboJPEG: conda-forge `libjpeg-turbo 3.1.4.1`.
- Host: AMD Ryzen 9 7900X, Linux 6.8.0-94-generic, x86_64.
- CPU affinity: logical CPU 2 for every measured process.
- Scaling: `amd-pstate-epp`, `powersave`; frequency was not locked.
- Isolation: every adapter/round ran in a fresh process.
- Order: odd rounds Python then Rust; even rounds Rust then Python.
- Warm-up: 2 seconds per primary variant and 1 second per control variant.
- Measurement: 10 paired rounds; 3 seconds per primary run and 1.5 seconds
  per control run.
- Statistics: arithmetic mean, sample standard deviation, round-paired effect,
  Student-t 95% CI, and deterministic 100,000-draw paired bootstrap CI.

Each process emitted a strict `ylx.performance-report.v0` report, so malformed
identity, environment, loss accounting, queue accounting, and effective-rate
relationships would have failed the run instead of entering the aggregate.

## Measurements

| Path | Python FPS | Rust FPS | Paired FPS uplift | Python CPU ms/frame | Rust CPU ms/frame | Paired CPU reduction |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Single-JPEG lossless crop | 66.578 +/- 0.131 | 72.154 +/- 0.146 | 8.376% | 14.965 | 13.858 | 7.392% |
| Dual-JPEG pass-through control | 3696.678 +/- 68.722 | 6114.078 +/- 88.996 | 65.449% | 0.271 | 0.164 | 39.527% |

Primary-path paired 95% Student-t confidence intervals:

| Metric | Mean | 95% CI |
| --- | ---: | ---: |
| Rust minus Python throughput | 5.576 FPS | 5.438 to 5.714 FPS |
| Rust throughput uplift | 8.376% | 8.161% to 8.591% |
| CPU time per frame reduction | 7.392% | 7.208% to 7.575% |
| Rust minus Python peak RSS | -0.317 MiB | -0.434 to -0.200 MiB |

Control-path paired 95% Student-t confidence intervals:

| Metric | Mean | 95% CI |
| --- | ---: | ---: |
| Rust minus Python throughput | 2417.400 FPS | 2335.347 to 2499.454 FPS |
| Rust throughput uplift | 65.449% | 62.528% to 68.370% |
| CPU time per frame reduction | 39.527% | 38.483% to 40.570% |
| Rust minus Python peak RSS | 1.050 MiB | 0.919 to 1.182 MiB |

The primary-path uplift was positive in every round, ranging from 7.964% to
8.887%. Mean uplift was 8.466% when Python ran first and 8.286% when Rust ran
first, so the balanced-order difference was small relative to the measured
effect.

## Interpretation

The Rust adapter provides a small, stable improvement on the production-shaped
single-JPEG splitter workload. The 5.576 FPS mean gain moves this host from
about 6.58 FPS to 12.15 FPS of headroom over a 60 FPS input rate.

The much larger pass-through-control result shows that Rust is substantially
better at marker scanning and buffer handling. On the normal single-JPEG path,
however, both variants spend most of their time in the same TurboJPEG lossless
crop. That common C operation limits the end-to-end benefit to about 8.4%.

The peak-RSS differences are below 1.1 MiB and include interpreter/import
baseline effects, so they should not drive an implementation decision.

## Limitations

- This host is x86_64, not D-Robotics RDK X5 aarch64. SIMD behavior, memory
  bandwidth, CPU scaling, and native library builds differ on the target.
- The trace is a Device API preview with the correct production dimensions,
  not a raw `/dev/video0` buffer captured by this exact wheel.
- Repeating one frame is deliberately deterministic and cache-friendly; it
  does not cover changing JPEG sizes, entropy, corruption, or capture jitter.
- Fixture loops have no live source, bounded capture queue, encoder, filesystem,
  IMU, audio, or preview client, so zero drops here are not an end-to-end claim.
- CPU frequency was not locked. CPU pinning, fresh processes, low host load,
  paired rounds, and balanced ordering reduce but do not eliminate host noise.
- Known RDK X5 devices `192.168.110.238` and `192.168.110.36` were unreachable
  during this run, so hardware `preview`, `recording`, and `concurrent` variants
  were not attempted or simulated.

## Decision

The fixed-trace evidence supports retaining the Rust splitter: it preserves
the exact JPEG outputs and reduces CPU cost on the tested production-shaped
input. It does not by itself establish an RDK X5 product-level speedup.

Before using performance as a release claim, run a balanced, process-isolated
matrix on the target device for `preview`, `recording`, and `concurrent`, using
the exact deployed wheel and at least five 30-second rounds per adapter. Report
source gaps separately from application drops, and compare effective FPS,
CPU/frame, peak RSS, queue rejection, and recording bytes.

## Evidence

- [Machine-readable aggregate](summary.json)
- [SHA-256 manifest](SHA256SUMS)
- [Primary raw reports](raw/single-jpeg/)
- [Mechanism-control raw reports](raw/dual-jpeg-control/)
- [Warm-up reports](warmup/)
