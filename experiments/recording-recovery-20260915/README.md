# Recording recovery ablation evidence

See [the experiment report](../../docs/reports/2026-09-15-recording-ablation.md)
for methods, results, interpretation, and limits. These are short local controlled
experiments, not device reliability or physical power loss tests.

The JSON results and CSV summaries are the original 2026-09-15 outputs.
`provenance.json` records the original source versions and result hashes. Its
`scripts` hashes refer to the original workstation scripts; `archived_scripts`
records the versions committed here. The archived scripts use a checkout-relative
source path, follow repository formatting, and explicitly bind the per-trial
volume in the mount-check lambda. No experiment result was regenerated during
publication.

Use the report's commands from the repository root with its development
environment installed. GCC and Rust are also needed for the C and Rust probes.
The C baseline is extracted from Git commit `f89a3b7`; retain that history.
The Rust probe creates an isolated source snapshot from the current Git HEAD
and reuses the repository's target cache. It does not edit production sources.

Scripts write beside themselves and overwrite matching result files. Preserve
these original results before rerunning. Generated binaries, extracted C sources,
and the Rust snapshot are not included in this small publication archive; the
original full workstation evidence remains at
`/data2/openaria-recording-ablation-20260915/`.
