"""Unit and CLI tests for the Miro-Ego Spectacular calibration ingestion boundary."""

from __future__ import annotations

import contextlib
import csv
import ctypes
import hashlib
import io
import json
import os
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any

from miro_ego_host.spectacular import (
    CHECK_SCHEMA,
    FRAME_TIMESTAMP_ALIGNMENT_SCHEMA,
    MODEL_INPUT_SCHEMA,
    CaptureValidationError,
    analyze_capture,
    build_frame_timestamp_rows,
    build_model_input,
    check_capture,
    frame_timestamp_alignment_summary,
    load_capture,
    open_metadata,
    open_regular,
    write_frame_timestamp_csv,
)
from miro_ego_host.spectacular.adapter import ZSTD_BLOCK_BYTES, _load_libzstd
from miro_ego_host.spectacular.check_cli import main as check_main


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")


def _compress_zstd_or_none(payload: bytes) -> bytes | None:
    try:
        from compression import zstd as stdlib_zstd  # type: ignore[import-not-found]

        return stdlib_zstd.compress(payload, level=1)
    except ImportError:
        pass

    try:
        import zstandard  # type: ignore[import-not-found]

        return zstandard.ZstdCompressor(level=1, write_checksum=True).compress(payload)
    except ImportError:
        pass

    lib = _load_libzstd()
    if lib is None:
        return None
    lib.ZSTD_compressBound.argtypes = [ctypes.c_size_t]
    lib.ZSTD_compressBound.restype = ctypes.c_size_t
    lib.ZSTD_compress.argtypes = [
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_int,
    ]
    lib.ZSTD_compress.restype = ctypes.c_size_t

    bound = lib.ZSTD_compressBound(len(payload))
    dst = (ctypes.c_ubyte * bound)()
    src = (ctypes.c_ubyte * len(payload)).from_buffer_copy(payload)
    written = lib.ZSTD_compress(
        ctypes.addressof(dst),
        bound,
        ctypes.addressof(src),
        len(payload),
        1,
    )
    if lib.ZSTD_isError(written):
        raise RuntimeError("ZSTD_compress failed in test fixture")
    return bytes(memoryview(dst)[:written])


def _artifact_descriptor(
    rel: str,
    role: str,
    media_type: str,
    raw: bytes,
    *,
    storage_encoding: dict[str, Any] | None = None,
) -> dict[str, Any]:
    digest = hashlib.sha256(raw).hexdigest()
    desc: dict[str, Any] = {
        "artifact_id": digest,
        "role": role,
        "path": rel,
        "media_type": media_type,
        "bytes": len(raw),
        "sha256": digest,
    }
    if storage_encoding is not None:
        desc["storage_encoding"] = storage_encoding
    return desc


def _make_calibration_session(
    root: Path,
    *,
    name: str = "session-cal",
    session_id: str = "0198c9a8-7a3c-7000-8000-000000000011",
    frame_decimation: int = 1,
    codec: str = "h264",
    audited_frames: bool = False,
    zstd_metadata: bool = False,
    include_calibration_logs: bool = False,
) -> Path:
    session = root / name
    (session / "video").mkdir(parents=True)
    (session / "frames").mkdir(parents=True)
    (session / "imu").mkdir(parents=True)

    segments_meta: list[dict[str, Any]] = []
    segment_ranges = ((0, 3, 0.0, 0.1), (3, 4, 0.1, 0.133333333))
    for seg_idx, (start_frame, end_frame, start_sec, end_sec) in enumerate(segment_ranges):
        left_rel = f"video/left_{seg_idx:05d}.mp4"
        right_rel = f"video/right_{seg_idx:05d}.mp4"
        left_bytes = f"mp4-left-{seg_idx}-{start_frame}-{end_frame}".encode("ascii") * 8
        right_bytes = f"mp4-right-{seg_idx}-{start_frame}-{end_frame}".encode("ascii") * 8
        (session / left_rel).write_bytes(left_bytes)
        (session / right_rel).write_bytes(right_bytes)
        segments_meta.append(
            {
                "index": seg_idx,
                "start_frame": start_frame,
                "end_frame": end_frame,
                "start_time_seconds": start_sec,
                "end_time_seconds": end_sec,
                "artifacts": {
                    "left": _artifact_descriptor(left_rel, "video.left", "video/mp4", left_bytes),
                    "right": _artifact_descriptor(
                        right_rel, "video.right", "video/mp4", right_bytes
                    ),
                },
            }
        )

    frame_rows: list[dict[str, Any]] = []
    for frame_idx in range(4):
        seg_idx = 0 if frame_idx < 3 else 1
        seg_frame = frame_idx if seg_idx == 0 else frame_idx - 3
        host_ns = 1_000_000_000 + frame_idx * 33_333_333
        row: dict[str, Any] = {
            "schema": "miro-ego.frame-index.v1",
            "session_id": session_id,
            "frame": frame_idx,
            "source_sequence": frame_idx * frame_decimation,
            "host_monotonic_ns": host_ns,
            "segment_index": seg_idx,
            "segment_frame": seg_frame,
        }
        if audited_frames:
            row["timestamp_audit"] = {
                "schema": "miro-ego.frame-timestamp-audit.v1",
                "v4l2_buffer_flags": 0x12000,
                "host_dequeue_monotonic_ns": host_ns + 250_000,
                "timestamp_clock": "v4l2_monotonic",
                "timestamp_source": "start_of_exposure",
                "camera_counter_raw": 100 + frame_idx * frame_decimation,
                "counter_bits": 24,
            }
        frame_rows.append(row)

    imu_rows: list[dict[str, Any]] = []
    sample_seq = 0
    for packet_seq in range(3):
        host_ns = 1_000_000_000 + packet_seq * 16_666_667
        ticks = 1_000 + packet_seq * 1_000
        for sample_index in range(2):
            imu_rows.append(
                {
                    "format": "miro-ego.imu.v1",
                    "session_id": session_id,
                    "sequence": sample_seq,
                    "packet_sequence": packet_seq,
                    "sample_index": sample_index,
                    "device_timestamp_raw": ticks & ((1 << 24) - 1),
                    "device_ticks": ticks,
                    "host_read_start_ns": host_ns - 500,
                    "host_read_end_ns": host_ns + 500,
                    "host_monotonic_ns": host_ns,
                    "raw": {
                        "accelerometer": [1 + sample_index, -2, 3],
                        "gyroscope": [4, 5, -6 - sample_index],
                    },
                    "sync": {
                        "offset_ns": None,
                        "residual_ns": None,
                        "quality": "insufficient",
                    },
                }
            )
            sample_seq += 1

    frames_raw = b"".join(map(_json_bytes, frame_rows))
    imu_raw = b"".join(map(_json_bytes, imu_rows))

    if zstd_metadata:
        frames_zst = _compress_zstd_or_none(frames_raw)
        imu_zst = _compress_zstd_or_none(imu_raw)
        assert frames_zst is not None and imu_zst is not None
        frames_rel = "frames/frames.ndjson.zst"
        imu_rel = "imu/imu.ndjson.zst"
        (session / frames_rel).write_bytes(frames_zst)
        (session / imu_rel).write_bytes(imu_zst)
        frames_art = _artifact_descriptor(
            frames_rel,
            "frames.index",
            "application/zstd",
            frames_zst,
            storage_encoding={
                "codec": "zstd",
                "level": 1,
                "block_bytes": ZSTD_BLOCK_BYTES,
                "uncompressed_bytes": len(frames_raw),
                "uncompressed_sha256": hashlib.sha256(frames_raw).hexdigest(),
            },
        )
        imu_art = _artifact_descriptor(
            imu_rel,
            "imu.samples",
            "application/zstd",
            imu_zst,
            storage_encoding={
                "codec": "zstd",
                "level": 1,
                "block_bytes": ZSTD_BLOCK_BYTES,
                "uncompressed_bytes": len(imu_raw),
                "uncompressed_sha256": hashlib.sha256(imu_raw).hexdigest(),
            },
        )
    else:
        frames_rel = "frames/frames.ndjson"
        imu_rel = "imu/imu.ndjson"
        (session / frames_rel).write_bytes(frames_raw)
        (session / imu_rel).write_bytes(imu_raw)
        frames_art = _artifact_descriptor(
            frames_rel, "frames.index", "application/x-ndjson", frames_raw
        )
        imu_art = _artifact_descriptor(imu_rel, "imu.samples", "application/x-ndjson", imu_raw)

    logs_list: list[dict[str, Any]] = []
    if include_calibration_logs:
        (session / "logs").mkdir(parents=True)
        clock_log = _json_bytes({"event": "clock_fit_ok", "drift_ppm": 12.5})
        cam_log = _json_bytes({"event": "board_detected", "corners": 88})
        (session / "logs/clock.ndjson").write_bytes(clock_log)
        (session / "logs/camera.ndjson").write_bytes(cam_log)
        logs_list.extend(
            (
                _artifact_descriptor(
                    "logs/clock.ndjson",
                    "log.clock-calibration",
                    "application/x-ndjson",
                    clock_log,
                ),
                _artifact_descriptor(
                    "logs/camera.ndjson",
                    "log.camera-calibration",
                    "application/x-ndjson",
                    cam_log,
                ),
            )
        )

    profile = "high" if codec == "h264" else "main"
    manifest: dict[str, Any] = {
        "schema": "miro-ego.device-session.v1",
        "manifest_id": "0198c9a8-7a3c-7000-8000-000000000012",
        "sealed": True,
        "sealed_at": "2026-11-27T03:00:00Z",
        "session_id": session_id,
        "volume_id": "12345678-1234-4234-8234-123456789abc",
        "capture_mode": "calibration",
        "display_name": "Spectacular calibration take",
        "device": {
            "device_id": "12345678-1234-4234-8234-123456789abd",
            "device_label": "Miro-Ego-1234ABCD",
            "hardware_fingerprint": "sha256:" + ("a" * 64),
            "platform": "D-Robotics RDK X5 V1.0",
            "software_version": "0.3.0",
            "commit": "b" * 40,
        },
        "time": {
            "started_at": "2026-11-27T02:59:59Z",
            "ended_at": "2026-11-27T03:00:00Z",
            "timezone": "Asia/Shanghai",
            "duration_seconds": 0.133333333,
            "duration_clock": "host_monotonic",
        },
        "take": {
            "take_id": "0198c9a8-7a3c-7000-8000-000000000013",
            "sequence": 1,
            "continuation_of": None,
        },
        "camera": {
            "width": 3840,
            "height": 1080,
            "eye_width": 1920,
            "sensor_fps": 30.0 * frame_decimation,
            "frame_decimation": frame_decimation,
            "effective_fps": 30.0,
            "coordinate_frame": "opencv_optical",
        },
        "video": {
            "layout": "split-eyes",
            "codec": codec,
            "container": "mp4",
            "segments": segments_meta,
            "encoding": {
                "schema": "miro-ego.recording-encoding.v1",
                "codec": codec,
                "rate_control": "cbr",
                "profile": profile,
                "pixel_format": "yuv420p",
                "bit_depth": 8,
                "b_frames": 0,
                "bitrate_kbps": 12288,
                "min_qp": 18,
                "max_qp": 32,
                "intra_qp": 20,
                "initial_qp": 22,
                "gop_frames": 30,
                "vbv_ms": 3000,
            },
        },
        "imu": {
            "artifact": imu_art,
            "sample_count": len(imu_rows),
            "units": "raw_int16",
            "coordinate_frame": "raw_device_axes",
        },
        "frames": {
            "artifact": frames_art,
            "count": len(frame_rows),
        },
        "audio": {
            "state": "not_recorded",
            "requested_mode": "disabled",
            "resolved_mode": "disabled",
            "reason": "user_disabled",
        },
        "logs": logs_list,
        "integrity": {
            "verified_at": "2026-11-27T03:00:00Z",
            "dropped_frames": 0,
            "drop_events": [],
            "fatal_errors": [],
        },
    }
    (session / "manifest.json").write_bytes(_json_bytes(manifest))
    return session


def _read_manifest(session: Path) -> dict[str, Any]:
    return json.loads((session / "manifest.json").read_text(encoding="utf-8"))


def _write_manifest(session: Path, manifest: dict[str, Any]) -> None:
    (session / "manifest.json").write_bytes(_json_bytes(manifest))


def _rewrite_ndjson_artifact(
    session: Path,
    section: str,
    transform: Callable[[list[dict[str, Any]]], None],
) -> None:
    manifest = _read_manifest(session)
    descriptor = manifest[section]["artifact"]
    target = session / descriptor["path"]
    records = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines() if line]
    transform(records)
    payload = b"".join(map(_json_bytes, records))
    target.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    descriptor["bytes"] = len(payload)
    descriptor["sha256"] = digest
    descriptor["artifact_id"] = digest
    _write_manifest(session, manifest)


class SpectacularTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.session = _make_calibration_session(self.root)

    def test_valid_calibration_session_maps_exact_fields_and_schemas(self) -> None:
        timing = analyze_capture(self.session)
        model = build_model_input(timing)
        result = check_capture(timing)

        self.assertEqual(timing.capture.source_schema, "miro-ego.device-session.v1")
        self.assertEqual(model["schema"], MODEL_INPUT_SCHEMA)
        self.assertEqual(model["schema"], "miro-ego.spectacular.model-input.v1")
        self.assertEqual(model["source"]["capture_mode"], "calibration")
        self.assertEqual(
            model["source"]["artifact_roles"],
            ["video.left", "video.right", "frames.index", "imu.samples"],
        )
        self.assertEqual(model["video"]["authority"], "device_session")
        self.assertEqual(model["video"]["layout"], "split-eyes")
        self.assertEqual(model["video"]["codec"], "h264")
        self.assertEqual(model["video"]["container"], "mp4")
        self.assertEqual(model["video"]["width"], 3840)
        self.assertEqual(model["video"]["eye_width"], 1920)
        self.assertEqual(model["video"]["height"], 1080)
        self.assertEqual(model["video"]["fps"], 30.0)
        self.assertEqual(
            model["video"]["segments"],
            [
                {
                    "index": 0,
                    "start_frame": 0,
                    "end_frame": 3,
                    "left_path": "video/left_00000.mp4",
                    "right_path": "video/right_00000.mp4",
                },
                {
                    "index": 1,
                    "start_frame": 3,
                    "end_frame": 4,
                    "left_path": "video/left_00001.mp4",
                    "right_path": "video/right_00001.mp4",
                },
            ],
        )
        self.assertEqual(model["frames"][2]["source_sequence"], 2)
        self.assertEqual(model["frames"][2]["segment"], {"index": 0, "frame": 2})
        self.assertEqual(model["imu_samples"][3]["packet_sequence"], 1)
        self.assertEqual(model["imu_samples"][3]["accelerometer_raw"], [2, -2, 3])
        self.assertEqual(model["imu_samples"][3]["gyroscope_raw"], [4, 5, -7])
        self.assertEqual(model["imu_samples"][3]["source"]["device_ticks"], 2_000)

        rows = build_frame_timestamp_rows(timing)
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0]["left_eye"]["timestamp_ns"], 1_000_000_000)
        self.assertEqual(rows[0]["right_eye"]["timestamp_ns"], 1_000_000_000)
        self.assertEqual(rows[0]["nearest_imu"]["sample_number"], 1)

        alignment = frame_timestamp_alignment_summary(timing)
        self.assertEqual(alignment["schema"], FRAME_TIMESTAMP_ALIGNMENT_SCHEMA)
        self.assertEqual(alignment["schema"], "miro-ego.spectacular.frame-timestamp-alignment.v1")
        self.assertEqual(alignment["rows"], 4)

        self.assertEqual(result["schema"], CHECK_SCHEMA)
        self.assertEqual(result["schema"], "miro-ego.spectacular.check.v1")
        self.assertEqual(result["model_input"]["frames"], 4)
        self.assertEqual(result["model_input"]["imu_samples"], 6)
        self.assertEqual(len(result["model_input"]["sha256"]), 64)
        self.assertEqual(check_capture(self.session)["model_input"], result["model_input"])

    def test_accepts_hevc_decimation_audited_frames_and_optional_calibration_logs(self) -> None:
        hevc_session = _make_calibration_session(
            self.root,
            name="session-hevc",
            frame_decimation=2,
            codec="hevc",
            audited_frames=True,
            include_calibration_logs=True,
        )
        timing = analyze_capture(hevc_session)
        model = build_model_input(timing)

        self.assertEqual(model["video"]["codec"], "hevc")
        self.assertEqual(timing.frame_clock.expected_rate_hz, 30.0)
        self.assertEqual(model["frames"][2]["frame_index"], 2)
        self.assertEqual(model["frames"][2]["source_sequence"], 4)
        self.assertEqual(
            model["frames"][0]["source"]["timestamp_audit"]["schema"],
            "miro-ego.frame-timestamp-audit.v1",
        )
        self.assertEqual(
            [entry["role"] for entry in model["source"]["logs"]],
            ["log.clock-calibration", "log.camera-calibration"],
        )

    def test_zstd_compressed_ndjson_streams_and_verifies_uncompressed_digest(self) -> None:
        if _compress_zstd_or_none(b"probe\n") is None:
            self.skipTest("No Zstandard compressor available in current Python runtime")

        zstd_session = _make_calibration_session(
            self.root,
            name="session-zstd",
            zstd_metadata=True,
        )
        plain_model = build_model_input(analyze_capture(self.session))
        zstd_model = build_model_input(analyze_capture(zstd_session))
        self.assertEqual(zstd_model["frames"], plain_model["frames"])
        self.assertEqual(zstd_model["imu_samples"], plain_model["imu_samples"])

        # Tampering uncompressed_sha256 in storage_encoding must fail closed even when outer
        # compressed file sha256 matches!
        manifest = _read_manifest(zstd_session)
        manifest["frames"]["artifact"]["storage_encoding"]["uncompressed_sha256"] = "0" * 64
        _write_manifest(zstd_session, manifest)
        with self.assertRaisesRegex(CaptureValidationError, "storage_encoding"):
            load_capture(zstd_session)

        # File with .zst suffix lacking Zstandard magic must be rejected by open_metadata.
        fake_zst = self.root / "fake.ndjson.zst"
        fake_zst.write_bytes(b'{"not":"zstd"}\n')
        with (
            self.assertRaisesRegex(CaptureValidationError, "Zstandard magic"),
            open_metadata(fake_zst) as stream,
        ):
            stream.read()

    def test_timestamp_csv_export_and_cli_acceptance_boundary(self) -> None:
        direct_csv = self.root / "direct-timestamps.csv"
        self.assertEqual(write_frame_timestamp_csv(analyze_capture(self.session), direct_csv), 4)

        csv_path = self.root / "cli-timestamps.csv"
        stdout_buf = io.StringIO()
        with contextlib.redirect_stdout(stdout_buf):
            rc = check_main([str(self.session), "--timestamp-csv", str(csv_path)])
        self.assertEqual(rc, 0)
        parsed = json.loads(stdout_buf.getvalue())
        self.assertEqual(parsed["schema"], "miro-ego.spectacular.check.v1")
        self.assertEqual(parsed["timestamp_csv"], {"path": str(csv_path), "rows": 4})

        with csv_path.open(encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0]["frame_index"], "0")
        self.assertEqual(rows[0]["left_eye_host_monotonic_ns"], "1000000000")
        self.assertEqual(rows[0]["right_eye_host_monotonic_ns"], "1000000000")
        self.assertEqual(rows[0]["nearest_imu_sample_number"], "1")
        self.assertEqual(rows[0]["imu_sync_quality"], "insufficient")

        # CLI returns exit code 2 and stderr message on invalid capture directory.
        stderr_buf = io.StringIO()
        with contextlib.redirect_stderr(stderr_buf):
            bad_rc = check_main([str(self.root / "does-not-exist")])
        self.assertEqual(bad_rc, 2)
        self.assertIn("spectacular validation failed:", stderr_buf.getvalue())

    def test_rejects_non_calibration_audio_inconsistent_fps_and_unsealed_states(self) -> None:
        manifest = _read_manifest(self.session)
        manifest["capture_mode"] = "production"
        _write_manifest(self.session, manifest)
        with self.assertRaisesRegex(CaptureValidationError, "only accepts calibration"):
            load_capture(self.session)

        manifest = _read_manifest(self.session)
        manifest["capture_mode"] = "calibration"
        manifest["sealed"] = False
        _write_manifest(self.session, manifest)
        with self.assertRaisesRegex(CaptureValidationError, "not sealed"):
            load_capture(self.session)

        manifest["sealed"] = True
        manifest["schema"] = "miro-ego.device-session.v99"
        _write_manifest(self.session, manifest)
        with self.assertRaisesRegex(CaptureValidationError, "unsupported Device Session"):
            load_capture(self.session)

        manifest["schema"] = "miro-ego.device-session.v1"
        manifest["video"]["layout"] = "raw-side-by-side"
        _write_manifest(self.session, manifest)
        with self.assertRaisesRegex(CaptureValidationError, "split-eyes"):
            load_capture(self.session)

        manifest["video"]["layout"] = "split-eyes"
        manifest["audio"] = {"state": "recorded"}
        _write_manifest(self.session, manifest)
        with self.assertRaisesRegex(CaptureValidationError, "must not depend on an audio"):
            load_capture(self.session)

        manifest["audio"] = {
            "state": "not_recorded",
            "requested_mode": "disabled",
            "resolved_mode": "disabled",
            "reason": "user_disabled",
        }
        manifest["camera"]["effective_fps"] = 15.0
        _write_manifest(self.session, manifest)
        with self.assertRaisesRegex(CaptureValidationError, "effective_fps"):
            load_capture(self.session)

        # Forbidden control file in session root is rejected before reading manifest.
        fresh = _make_calibration_session(self.root, name="with-control")
        (fresh / "recording.json").write_text('{"state":"recording"}\n', encoding="utf-8")
        with self.assertRaisesRegex(CaptureValidationError, "forbidden"):
            load_capture(fresh)

    def test_rejects_digest_size_mismatch_duplicate_json_keys_and_path_traversal(self) -> None:
        # Tamper byte without changing length -> SHA-256 mismatch.
        left_mp4 = self.session / "video/left_00000.mp4"
        original = left_mp4.read_bytes()
        left_mp4.write_bytes(bytes([original[0] ^ 0xFF]) + original[1:])
        with self.assertRaisesRegex(CaptureValidationError, "SHA-256 mismatch"):
            load_capture(self.session)

        # Tamper length -> size mismatch.
        left_mp4.write_bytes(original + b"extra")
        with self.assertRaisesRegex(CaptureValidationError, "size mismatch"):
            load_capture(self.session)
        left_mp4.write_bytes(original)

        # Path traversal in manifest artifact descriptor.
        manifest = _read_manifest(self.session)
        manifest["frames"]["artifact"]["path"] = "../escape.ndjson"
        _write_manifest(self.session, manifest)
        with self.assertRaisesRegex(CaptureValidationError, "unsafe"):
            load_capture(self.session)

        # Duplicate JSON key & non-finite JSON constant rejection.
        dup_session = _make_calibration_session(self.root, name="dup-key")
        raw_text = (dup_session / "manifest.json").read_text(encoding="utf-8").strip()
        (dup_session / "manifest.json").write_text(
            raw_text[:-1] + ',"sealed":true}\n', encoding="utf-8"
        )
        with self.assertRaisesRegex(CaptureValidationError, "duplicate JSON key"):
            load_capture(dup_session)

        nan_session = _make_calibration_session(self.root, name="nan-json")
        nan_text = (nan_session / "manifest.json").read_text(encoding="utf-8")
        (nan_session / "manifest.json").write_text(
            nan_text.replace('"effective_fps":30.0', '"effective_fps":NaN'),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(CaptureValidationError, "non-finite JSON number"):
            load_capture(nan_session)

    def test_rejects_sequence_gaps_ticks_regression_and_unclosed_records(self) -> None:
        # Unknown extra field in frame record rejected even with recomputed SHA-256.
        unclosed = _make_calibration_session(self.root, name="unclosed-frame")
        _rewrite_ndjson_artifact(
            unclosed,
            "frames",
            lambda rows: rows[0].__setitem__("extra_field", 123),
        )
        with self.assertRaisesRegex(CaptureValidationError, "not a closed record"):
            load_capture(unclosed)

        # Frame source_sequence gap rejected even with recomputed SHA-256.
        frame_gap = _make_calibration_session(self.root, name="frame-gap")
        _rewrite_ndjson_artifact(
            frame_gap,
            "frames",
            lambda rows: rows[2].__setitem__("source_sequence", 9),
        )
        with self.assertRaisesRegex(CaptureValidationError, "source sequence gap"):
            analyze_capture(frame_gap)

        # IMU packet_sequence gap rejected.
        pkt_gap = _make_calibration_session(self.root, name="pkt-gap")

        def skip_pkt(rows: list[dict[str, Any]]) -> None:
            for r in rows:
                if r["packet_sequence"] == 2:
                    r["packet_sequence"] = 3

        _rewrite_ndjson_artifact(pkt_gap, "imu", skip_pkt)
        with self.assertRaisesRegex(CaptureValidationError, "IMU packet sequence gap"):
            analyze_capture(pkt_gap)

        # IMU device_ticks regression rejected.
        ticks_reg = _make_calibration_session(self.root, name="ticks-reg")

        def regress_ticks(rows: list[dict[str, Any]]) -> None:
            for r in rows:
                if r["packet_sequence"] == 1:
                    r["device_ticks"] = 999
                    r["device_timestamp_raw"] = 999

        _rewrite_ndjson_artifact(ticks_reg, "imu", regress_ticks)
        with self.assertRaisesRegex(CaptureValidationError, "device_ticks regressed"):
            analyze_capture(ticks_reg)

    def test_allows_bounded_imu_jitter_and_400hz_packet_interpolation(self) -> None:
        jitter_session = _make_calibration_session(self.root, name="imu-jitter")

        def delay_packet(rows: list[dict[str, Any]]) -> None:
            for r in rows:
                if r["packet_sequence"] == 1:
                    for key in ("host_read_start_ns", "host_read_end_ns", "host_monotonic_ns"):
                        r[key] = int(r[key]) + 7_000_000

        _rewrite_ndjson_artifact(jitter_session, "imu", delay_packet)
        timing = analyze_capture(jitter_session)
        self.assertGreater(timing.imu_clock.residual_p95_ms, 5.0)
        self.assertLess(timing.imu_clock.residual_p95_ms, 10.0)
        with self.assertRaisesRegex(CaptureValidationError, "IMU packet host timing p95"):
            analyze_capture(jitter_session, max_imu_residual_p95_ms=5.0)

        fast_imu = _make_calibration_session(self.root, name="imu-400hz")

        def set_400hz(rows: list[dict[str, Any]]) -> None:
            for r in rows:
                r["device_ticks"] = 1000
                r["device_timestamp_raw"] = 1000
                host = 1_000_000_000 + int(r["packet_sequence"]) * 5_000_000
                r["host_monotonic_ns"] = host
                r["host_read_start_ns"] = host - 2_000_000
                r["host_read_end_ns"] = host + 2_000_000

        _rewrite_ndjson_artifact(fast_imu, "imu", set_400hz)
        fast_timing = analyze_capture(fast_imu, imu_rate_hz=400.0)
        self.assertEqual(fast_timing.imu_clock.measured_rate_hz, 200.0)
        for prev, curr in zip(fast_timing.imu_times_ns, fast_timing.imu_times_ns[1:], strict=False):
            self.assertEqual(curr - prev, 2_500_000)

    def test_rejects_symlinks_and_hardlinks(self) -> None:
        # Hardlink rejection (st_nlink > 1) on regular artifact.
        hl_session = _make_calibration_session(self.root, name="hardlink-session")
        frames_file = hl_session / "frames/frames.ndjson"
        alias_file = self.root / "frames-hardlink.ndjson"
        os.link(frames_file, alias_file)
        with self.assertRaisesRegex(CaptureValidationError, "exclusive regular file"):
            load_capture(hl_session)

        # Symlink rejection on artifact file and session root directory.
        sym_session = _make_calibration_session(self.root, name="symlink-session")
        outside_target = self.root / "outside.mp4"
        outside_target.write_bytes((sym_session / "video/left_00000.mp4").read_bytes())
        (sym_session / "video/left_00000.mp4").unlink()
        try:
            (sym_session / "video/left_00000.mp4").symlink_to(outside_target)
        except OSError as exc:
            if getattr(exc, "winerror", None) == 1314:
                self.skipTest("Windows privilege required to create filesystem symlinks")
            raise
        with self.assertRaises(CaptureValidationError):
            load_capture(sym_session)
        with self.assertRaises(CaptureValidationError):
            open_regular(sym_session / "video/left_00000.mp4")

        sym_root = self.root / "symlink-root"
        sym_root.symlink_to(self.session, target_is_directory=True)
        with self.assertRaisesRegex(CaptureValidationError, "not a real directory"):
            load_capture(sym_root)

    def test_effective_fps_within_rate_tolerance_passes_and_fits_against_expected_fps(
        self,
    ) -> None:
        manifest = _read_manifest(self.session)
        # 29.5 fps is ~16,667 ppm away from nominal 30.0 (within 20,000 ppm).
        manifest["camera"]["effective_fps"] = 29.5
        _write_manifest(self.session, manifest)
        timing = analyze_capture(self.session)
        self.assertEqual(timing.capture.fps, 30.0)
        self.assertEqual(timing.frame_clock.expected_rate_hz, 30.0)

        # 29.0 fps is ~33,333 ppm away from nominal 30.0 (exceeds 20,000 ppm).
        manifest["camera"]["effective_fps"] = 29.0
        _write_manifest(self.session, manifest)
        with self.assertRaisesRegex(CaptureValidationError, "effective_fps"):
            load_capture(self.session)

    def test_zstd_stream_preserves_inner_capture_validation_error(self) -> None:
        if _compress_zstd_or_none(b"probe\n") is None:
            self.skipTest("No Zstandard compressor available in current Python runtime")

        zstd_session = _make_calibration_session(
            self.root,
            name="session-zstd-inner-err",
            zstd_metadata=True,
        )
        manifest = _read_manifest(zstd_session)
        bad_payload = b'{"broken":NaN}\n'
        bad_zst = _compress_zstd_or_none(bad_payload)
        assert bad_zst is not None
        frames_path = zstd_session / "frames/frames.ndjson.zst"
        frames_path.write_bytes(bad_zst)
        digest = hashlib.sha256(bad_zst).hexdigest()
        manifest["frames"]["artifact"]["bytes"] = len(bad_zst)
        manifest["frames"]["artifact"]["sha256"] = digest
        manifest["frames"]["artifact"]["artifact_id"] = digest
        manifest["frames"]["artifact"]["storage_encoding"]["uncompressed_bytes"] = len(bad_payload)
        manifest["frames"]["artifact"]["storage_encoding"]["uncompressed_sha256"] = hashlib.sha256(
            bad_payload
        ).hexdigest()
        _write_manifest(zstd_session, manifest)

        with self.assertRaisesRegex(CaptureValidationError, "non-finite JSON number"):
            load_capture(zstd_session)

    def test_per_sample_imu_timestamps_with_2_5ms_sample0_offset_validate_and_interpolate(
        self,
    ) -> None:
        session = _make_calibration_session(self.root, name="imu-per-sample-offsets")

        def set_per_sample(rows: list[dict[str, Any]]) -> None:
            for r in rows:
                r["device_ticks"] = 1000
                r["device_timestamp_raw"] = 1000
                mid = 1_000_000_000 + int(r["packet_sequence"]) * 5_000_000
                r["host_read_start_ns"] = mid - 2_000_000
                r["host_read_end_ns"] = mid + 2_000_000
                r["host_monotonic_ns"] = mid - 2_500_000 if int(r["sample_index"]) == 0 else mid

        _rewrite_ndjson_artifact(session, "imu", set_per_sample)
        timing = analyze_capture(session, imu_rate_hz=400.0)
        for prev, curr in zip(timing.imu_times_ns, timing.imu_times_ns[1:], strict=False):
            self.assertEqual(curr - prev, 2_500_000)

        out_of_order = _make_calibration_session(self.root, name="imu-intra-packet-ooo")

        def set_ooo(rows: list[dict[str, Any]]) -> None:
            set_per_sample(rows)
            rows[0]["host_monotonic_ns"] = int(rows[1]["host_monotonic_ns"]) + 100

        _rewrite_ndjson_artifact(out_of_order, "imu", set_ooo)
        with self.assertRaisesRegex(CaptureValidationError, "disagree"):
            analyze_capture(out_of_order, imu_rate_hz=400.0)

        bad_latest = _make_calibration_session(self.root, name="imu-latest-outside-interval")

        def set_bad_latest(rows: list[dict[str, Any]]) -> None:
            set_per_sample(rows)
            rows[1]["host_monotonic_ns"] = int(rows[1]["host_read_start_ns"]) - 1

        _rewrite_ndjson_artifact(bad_latest, "imu", set_bad_latest)
        with self.assertRaisesRegex(CaptureValidationError, "host interval is invalid"):
            analyze_capture(bad_latest, imu_rate_hz=400.0)


if __name__ == "__main__":
    unittest.main()
