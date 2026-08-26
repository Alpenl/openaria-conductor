# Spectacular capture boundary

This package is the active owner of capture ingestion for Spectacular calibration.
It accepts exactly two inputs:

- the historical `ylx.stereo_imu.raw.v2` calibration directory; or
- a sealed `ylx.device-session.v2` with `capture_mode=calibration`, continuous
  `raw-side-by-side` MJPEG, frame metadata, IMU metadata, and no audio artifact.

The adapter validates the Device Session schema, artifact roles, safe relative paths,
regular-file identity, byte counts, and SHA-256 values before reading metadata. It does
not infer layouts from filenames and does not fall back to production split-eye media,
single-file MP4, embedded audio, or separately recorded audio.

The Device Session mapping is explicit: frame `frame`, `source_sequence`, and
`host_monotonic_ns` become the model frame index, source sequence, and fitted frame
time. The source sequence must advance by the declared `frame_decimation`; the fitted
frame clock uses the contiguous `frame` recording domain. IMU `sequence`,
`packet_sequence`, `sample_index`, `device_timestamp_raw`,
`device_ticks`, all three host timestamps, `raw`, and `sync` are retained under each
sample's `source`; raw axes and the reconstructed monotonic sample time are also mapped
to the model-facing fields. No mapping is inferred from a coincidentally similar name.

Run the same acceptance boundary used by downstream calibration code with:

```console
rp-ylx-spectacular-check /path/to/session
```
