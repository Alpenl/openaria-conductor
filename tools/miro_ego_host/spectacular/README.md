# Miro-Ego Spectacular Capture Boundary

This package owns host-PC capture ingestion and integrity validation for Spectacular stereo-IMU calibration on **Miro-Ego 0.3.0**.

## Accepted Input Contract

It accepts **only** a sealed `miro-ego.device-session.v1` session directory meeting all of the following preconditions:

- `capture_mode == "calibration"` and `sealed == true` (with zero dropped frames and no fatal errors).
- `video.layout == "split-eyes"`, `video.container == "mp4"`, and `video.codec` in `{"h264", "hevc"}` with contiguous left/right MP4 segment pairs (`video.left`, `video.right`).
- `camera.width == 2 * camera.eye_width` and `camera.effective_fps` consistent with `camera.sensor_fps / camera.frame_decimation`.
- `audio.state == "not_recorded"` (`resolved_mode == "disabled"`).
- Closed `miro-ego.frame-index.v1` frame metadata (`frames.index`) and `miro-ego.imu.v1` IMU metadata (`imu.samples`) stored as plain NDJSON (`application/x-ndjson`) or Zstandard-compressed NDJSON (`application/zstd`, `.ndjson.zst`), plus optional verified `log.*` artifacts (such as `log.clock-calibration` and `log.camera-calibration`).

Every declared artifact is opened with `O_NOFOLLOW` and `st_nlink == 1` regular-file checks (`open_regular`), and its exact byte length and SHA-256 digest (plus `storage_encoding` uncompressed length and SHA-256 for `.ndjson.zst` streams) are verified before mapping.

## Output Schemas

- Model input: `miro-ego.spectacular.model-input.v1`
- Frame/IMU timestamp alignment summary: `miro-ego.spectacular.frame-timestamp-alignment.v1`
- CLI acceptance check report: `miro-ego.spectacular.check.v1`

## Timing Bounds & CSV Export

Timing reconstruction remains fail-closed:

- Video frame clock fit: `<= 20,000 ppm` rate error against `camera.effective_fps` and `<= 5.0 ms` p95 host residual.
- USB control-read IMU packet clock fit: contiguous packet sequences, non-regressing `device_ticks`, `<= 10.0 ms` p95 host residual, and `<= 10.0 ms` p95 control-read duration.

For calibration review or vendor comparison, `--timestamp-csv` emits one row per frame pairing the shared side-by-side left/right camera host-monotonic timestamp with the nearest reconstructed IMU sample timestamp.

## CLI Usage

```console
PYTHONPATH=tools uv run python -m miro_ego_host.spectacular.check_cli /path/to/session
PYTHONPATH=tools uv run python -m miro_ego_host.spectacular.check_cli /path/to/session --timestamp-csv frame-timestamps.csv
```
