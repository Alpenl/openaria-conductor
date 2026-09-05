# Open Aria whole-repository structural ablation

Date: 2026-09-05 (Asia/Shanghai)

## Result

Starting from `4d8c68094cba1d30c7bb1408af35a44c844eb329`, this ablation removed
unreachable implementations, duplicate bridges, duplicated build metadata,
and tests that asserted deleted implementation details. Before this report was
added, the implementation patch contained 564 insertions and 5,895 deletions
across 28 files, for a net reduction of 5,331 lines.

The release wheel became 62,206 bytes smaller (5.202%). The uncompressed native
extension became 207,560 bytes smaller (10.155%). All 711 remaining Python
tests and all 69 remaining Rust tests pass. No hardware claims are made.

## Method

This was a repository-wide reachability and responsibility audit, not a blind
line-count exercise. Each candidate was checked against production call sites,
tests, packaging behavior, compatibility contracts, and the existing
fixed-trace performance experiment. A candidate was removed only when its
responsibility was already owned elsewhere, its configured state was
unreachable, or it duplicated build/test machinery without adding an
independent failure boundary.

The audit covered:

- Python production modules and tests;
- the Rust crate and every PyO3 export;
- package/build metadata and CI;
- session schemas, legacy readers, deployment assets, and embedded web assets;
- native capability gates and source-checkout fallbacks.

## Removed

| Area | Removed design | Evidence and replacement |
| --- | --- | --- |
| Capture sources | `ThreadedCaptureSources` and its public re-export | It had no production constructor or caller. `ContinuousCaptureSources` owns the supported Python fallback; the native runtime owns production capture. |
| Native factories | Repeated import, capability, and exception adapters | One `_require_native_feature` and one `_parse_native_error` now enforce the same boundary for all factories. |
| Session I/O | Three independent lazy caches | One process-wide `native_session_io_or_none()` owns probing and caching for downloads, device-session scanning, and encoder artifact finalization. |
| Context cleanup | Private suppress-only context managers | `contextlib.suppress` expresses the same behavior directly. |
| PyO3 surface | Standalone frame validator, timeline, recording codec/event queue, stereo event parser/pipe, HTTP range parser, and drop-quality evaluator wrappers | These were thin alternate entry points with no production consumer. Integrated runtime/session owners or small Python standard-library operations already perform the work. |
| Rust modules | `timeline.rs`, unused bounded blocking receive, and wrapper-only DTOs/tests | Timeline clock sampling now lives with audio, its only consumer. Other code disappeared with its only wrapper. |
| Raw recording writer | Python fallback file writer, Rust sink mode, capture-runtime dispatch, PyO3 methods, capability flag, and raw/split decision DTOs | `DeviceSessionConfig` rejects every layout except `split-eyes`; production configuration does the same. The sink now has one valid shape. Raw SBS remains the camera input to the split encoder. |
| Recording errors | Repeated native-to-domain exception conversion | One `_recording_error` function owns code/message normalization. |
| Daemon tests | Repeated one-test-per-native-feature setup | One table-driven test exercises every required production feature through the same observable failure boundary. |
| Packaging | `setup.py`, setuptools package-data configuration, and `MANIFEST.in` | Maturin is the sole build owner. The distribution test builds a wheel directly and a wheel through an sdist, installs both in clean environments, and hashes every tracked non-Python package resource. |
| CI | Stale expected product name | The assertion now matches the shipped `Open Aria` identity. |

## Retained

| Area | Decision | Reason |
| --- | --- | --- |
| Rust JPEG splitter | Retain | The existing fixed-trace experiment measured 72.154 FPS versus 66.578 FPS for Python, an 8.376% paired uplift, with byte-identical eye JPEGs. |
| Integrated Rust owners | Retain | Camera, audio, IMU, continuous runtime, split sink, encoder process, segment planner, finalizer, preview buffer, metrics, and session I/O own real resources or non-trivial invariants. |
| Python protocols/facades | Retain | They isolate hardware and native objects at testable boundaries and support the explicitly tested source-checkout fallback. |
| Legacy raw readers | Retain | Existing `raw-side-by-side` manifests remain downloadable and convertible. Only creation of new raw sessions was unreachable. |
| `video_layout` validation | Retain | Explicit rejection of obsolete raw-writer configuration is safer than silently changing caller intent. |
| Native ABI 4 constructor arity | Retain | The Python factory always supplies `split_eyes=True`, while PyO3 rejects `false`. This preserves active old/new adapter combinations without restoring raw writing. |
| Schemas and frozen contracts | Retain | They are external compatibility and fail-closed validation boundaries, not implementation duplication. |
| Deployment and security checks | Retain | Apparent repetition protects distinct filesystem, identity, authentication, and durable-publication boundaries. |
| Embedded web source/artifacts | Retain unchanged | This audit did not regenerate upstream web assets; their pinned integrity contract remains intact. |

## Package-size ablation

Both variants were built as optimized `cp311-abi3` wheels on the same x86_64
host with `uvx maturin build --release`. The baseline came from a temporary
`git archive HEAD`; the candidate came from the working tree. Build products
were kept outside the repository.

| Artifact | Before | After | Delta | Reduction |
| --- | ---: | ---: | ---: | ---: |
| Wheel | 1,195,842 B | 1,133,636 B | -62,206 B | 5.202% |
| Native extension, compressed in wheel | 773,633 B | 717,652 B | -55,981 B | 7.236% |
| Native extension, uncompressed | 2,044,000 B | 1,836,440 B | -207,560 B | 10.155% |

Wheel totals can vary by a few bytes with ZIP metadata. The native extension
measurements provide the stronger indication that removed Rust/PyO3 code is
absent from the release artifact.

## Verification

- `python scripts/check.py`: 711 tests passed in 135.900 seconds; Ruff check
  and format check passed. This includes the external wheel/sdist installation
  and resource-integrity test.
- Focused Python suite for native factories, capture sources, split recording,
  and daemon gates: 97 tests passed.
- `cargo test --workspace --all-targets`: 69 tests passed.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `git diff --check`: passed.
- Removed-symbol scan: no production or test references remained.
- ABI probe: the ABI remains 4, the supported constructor arity is preserved,
  and attempting to select raw recording fails explicitly.
- Vulture at 80% confidence found no unused definitions. Its 38 reports were
  required protocol/framework parameter names (`__exit__`, logging,
  zeroconf-compatible arguments, and Protocol signatures).

The pre-ablation suite had 762 Python and 75 Rust tests. The removed tests
covered deleted wrappers, unreachable raw writing, or repeated feature-gate
fixtures; behavioral and external-contract tests remain.

## Limitations

- No RDK X5 device was available for camera, IMU, audio, encoder, or concurrent
  recording acceptance. The result proves host-side behavior and packaging,
  not target throughput or device stability.
- The package-size result does not imply a proportional runtime memory or CPU
  improvement.
- Legacy raw reading is covered by tests, but this change intentionally removes
  the ability to create new raw-side-by-side sessions.

## Decision

Keep this structural ablation. It removes alternate implementations and
configuration branches that the product cannot select, while preserving the
resource-owning native modules, compatibility readers, external schemas, and
security boundaries that make the system reliable.

Before release, repeat the existing hardware acceptance matrix on an RDK X5
using the exact candidate wheel. Preview, production recording, calibration,
audio-enabled recording, session download, and service restart should all be
exercised without changing the now-single `split-eyes` writer path.
