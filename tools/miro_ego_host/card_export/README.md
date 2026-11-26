# Miro-Ego SD-Card Exporter

Pure-Python, read-only exporter for Miro-Ego `MIRO-EGO` exFAT recording partitions (p3).

## Capabilities

- **Strictly read-only**: opens raw cards or images in `rb` mode without mounting, repairing, or modifying a single byte on the SD card.
- **Canonical layout only**: reads `recordings/YYYY-MM-DD/HH-MM-SS/` (including `_02..` same-second suffixes) and unsynced-clock `recordings/unknown-date/recording_NN/` sessions with `miro-ego.device-session.v1` manifests.
- **Full SHA-256 verification**: verifies every copied file against both source-to-destination readback SHA-256 and the exact `bytes` + `sha256` recorded for every artifact (`video.left`, `video.right`, `imu.samples`, `frames.index`, `audio.wav`, `logs`) in `manifest.json`.
- **Diagnostic log export**: supports exporting `/logs/` (including `logs/wifi-watchdog/`) alongside recordings (`--include-logs`) or standalone (`--logs-only`).
- **`sudo` ownership restoration**: restores exported directory and file ownership to `SUDO_UID:SUDO_GID` on Linux/macOS when invoked under `sudo`.
- **Flexible inputs**: reads whole physical disks, raw MBR `.img` files, standalone exFAT partition images, or already-mounted `MIRO-EGO` directories using Python's standard library alone.

## Usage

```bash
# List recordings and diagnostics on an SD card or disk image
python -m miro_ego_host.card_export.export --source /dev/sdb --list

# Export and verify all sealed recordings
python -m miro_ego_host.card_export.export --source /dev/sdb --destination ~/Exports

# Export recordings plus /logs diagnostics
python -m miro_ego_host.card_export.export --source /dev/sdb --destination ~/Exports --include-logs

# Export only /logs diagnostics
python -m miro_ego_host.card_export.export --source /dev/sdb --destination ~/Exports --logs-only
```

Each export writes a timestamped `Miro-Ego-export-*` directory containing an `export-report.json` (`miro-ego.card-export-report.v1`) with per-file byte counts, SHA-256 digests, and manifest verification status.
