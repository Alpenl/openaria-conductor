"""Spectacular accepts only explicit, integrity-checked calibration captures."""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import tempfile
import unittest
import uuid
from pathlib import Path

from rp_ylx.camera import FrameObservation, StereoFrame
from rp_ylx.imu import ImuObservation, ImuSample, RawVector3
from rp_ylx.recording import (
    DeviceSessionConfig,
    DeviceSessionRecorder,
    SessionPlan,
    StorageStatus,
    uuid7,
)
from rp_ylx.spectacular import (
    CaptureValidationError,
    analyze_capture,
    build_model_input,
    check_capture,
    load_capture,
)
from rp_ylx.spectacular.check_cli import main as check_main

JPEG = b"\xff\xd8spectacular-raw-sbs\xff\xd9"


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n").encode()


def _make_device_session(root: Path, *, frame_decimation: int = 1) -> Path:
    revision = 0

    def allocate_revision() -> int:
        nonlocal revision
        revision += 1
        return revision

    session_id = uuid7()
    recorder = DeviceSessionRecorder(
        root,
        DeviceSessionConfig(
            device_id=str(uuid.uuid4()),
            device_label="YLX-12AB34CD",
            hardware_fingerprint="sha256:" + "a" * 64,
            platform="D-Robotics RDK X5 V1.0 + YLX 2UQ2",
            software_version="0.5.0",
            commit="b" * 40,
            width=3840,
            height=1080,
            sensor_fps=30.0 * frame_decimation,
            frame_decimation=frame_decimation,
            video_layout="raw-side-by-side",
            audio_enabled=False,
        ),
        SessionPlan(
            session_id=session_id,
            volume_id=str(uuid.uuid4()),
            generation_id=str(uuid.uuid4()),
            capture_mode="calibration",
            display_name="Spectacular adapter fixture",
            take_id=uuid7(),
            take_sequence=1,
            continuation_of=None,
        ),
        authority_epoch=str(uuid.uuid4()),
        allocate_revision=allocate_revision,
        storage_status=lambda: StorageStatus(1024 * 1024 * 1024, True),
        checkpoint_interval=0.0,
        before_write=lambda _role, _payload: None,
    )
    recorder.start()
    for sequence in range(4):
        assert recorder.submit_frame(
            FrameObservation(
                StereoFrame(
                    source_sequence=sequence * frame_decimation,
                    host_monotonic_ns=1_000_000_000 + sequence * 33_333_333,
                    left=b"",
                    right=b"",
                    raw_side_by_side=JPEG,
                ),
                dropped_before=0,
            )
        )
    sample_sequence = 0
    for packet_sequence in range(3):
        host = 1_000_000_000 + packet_sequence * 16_666_667
        device_ticks = 1_000 + packet_sequence * 1_000
        samples = []
        for sample_index in range(2):
            samples.append(
                ImuSample(
                    sequence=sample_sequence,
                    packet_sequence=packet_sequence,
                    sample_index=sample_index,
                    device_timestamp_raw=device_ticks,
                    device_ticks=device_ticks,
                    host_read_start_ns=host - 500,
                    host_read_end_ns=host + 500,
                    host_monotonic_ns=host,
                    accelerometer=RawVector3(1 + sample_index, 2, 3),
                    gyroscope=RawVector3(4, 5, 6 + sample_index),
                    sync_offset_ns=None,
                    sync_residual_ns=None,
                    sync_quality="insufficient",
                )
            )
            sample_sequence += 1
        assert recorder.submit_imu(ImuObservation((samples[0], samples[1]), dropped_samples=0))
    return recorder.stop().path


def _make_legacy_capture(root: Path) -> Path:
    capture = root / "legacy"
    capture.mkdir()
    video = JPEG * 3
    (capture / "video.mjpeg").write_bytes(video)
    offset = 0
    frame_records = []
    for index in range(3):
        frame_records.append(
            {
                "frame_index": index,
                "uvc_sequence": index,
                "callback_monotonic_ns": 1_000_000_000 + index * 33_333_333,
                "libuvc_capture_time_ns": 1_000_000_000 + index * 33_333_333,
                "jpeg_offset": offset,
                "jpeg_bytes": len(JPEG),
            }
        )
        offset += len(JPEG)
    (capture / "frames.ndjson").write_bytes(b"".join(map(_json_bytes, frame_records)))
    imu_records = []
    sample_number = 0
    for packet in range(3):
        host = 1_000_000_000 + packet * 16_666_667
        for sample_index in range(2):
            imu_records.append(
                {
                    "sample_number": sample_number,
                    "device_timestamp_raw": 1_000 + packet * 1_000,
                    "sample_index": sample_index,
                    "samples_in_packet": 2,
                    "host_read_start_ns": host - 500,
                    "host_read_end_ns": host + 500,
                    "host_monotonic_ns": host,
                    "accel_raw": [1, 2, 3],
                    "gyro_raw": [4, 5, 6],
                }
            )
            sample_number += 1
    (capture / "imu.ndjson").write_bytes(b"".join(map(_json_bytes, imu_records)))
    manifest = {
        "schema": "ylx.stereo_imu.raw.v2",
        "summary": {"result": 0},
        "files": {
            "video": "video.mjpeg",
            "frames": "frames.ndjson",
            "imu": "imu.ndjson",
        },
        "video": {"requested_frames": 3, "fps": 30.0, "eye_width": 1920, "height": 1080},
        "imu": {"samples_per_packet": 2, "device_timestamp_bits": 24},
    }
    (capture / "capture.json").write_bytes(_json_bytes(manifest))
    return capture


def _manifest(path: Path) -> dict[str, object]:
    return json.loads((path / "manifest.json").read_text())


def _write_manifest(path: Path, manifest: dict[str, object]) -> None:
    (path / "manifest.json").write_bytes(_json_bytes(manifest))


def _rewrite_artifact(
    session: Path,
    block: str,
    transform: object,
) -> None:
    manifest = _manifest(session)
    descriptor = manifest[block]["artifact"]  # type: ignore[index]
    artifact = session / descriptor["path"]  # type: ignore[index]
    records = [json.loads(line) for line in artifact.read_text().splitlines()]
    transform(records)  # type: ignore[operator]
    payload = b"".join(map(_json_bytes, records))
    artifact.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    descriptor["bytes"] = len(payload)  # type: ignore[index]
    descriptor["sha256"] = digest  # type: ignore[index]
    descriptor["artifact_id"] = digest  # type: ignore[index]
    _write_manifest(session, manifest)


class SpectacularAdapterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.session = _make_device_session(self.root)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_device_session_maps_exact_fields_and_checks_timing(self) -> None:
        timing = analyze_capture(self.session)
        model = build_model_input(timing)
        result = check_capture(timing)

        self.assertEqual(timing.capture.source_schema, "ylx.device-session.v2")
        self.assertEqual(model["source"]["capture_mode"], "calibration")
        self.assertEqual(
            model["source"]["artifact_roles"],
            ["video.raw-side-by-side", "frames.index", "imu.samples"],
        )
        self.assertEqual(model["video"]["path"], "video/raw-sbs.mjpeg")
        self.assertEqual(model["video"]["width"], 3840)
        self.assertEqual(model["frames"][2]["source_sequence"], 2)
        self.assertEqual(model["frames"][2]["jpeg"], {"offset": len(JPEG) * 2, "bytes": len(JPEG)})
        self.assertEqual(model["imu_samples"][3]["packet_sequence"], 1)
        self.assertEqual(model["imu_samples"][3]["accelerometer_raw"], [2, 2, 3])
        self.assertEqual(model["imu_samples"][3]["source"]["device_ticks"], 2_000)
        self.assertEqual(
            model["imu_samples"][3]["source"]["sync"],
            {"offset_ns": None, "residual_ns": None, "quality": "insufficient"},
        )
        self.assertEqual(result["model_input"]["frames"], 4)
        self.assertEqual(len(result["model_input"]["sha256"]), 64)
        self.assertEqual(check_capture(self.session)["model_input"], result["model_input"])

    def test_device_session_accepts_declared_frame_decimation(self) -> None:
        session = _make_device_session(self.root, frame_decimation=2)

        timing = analyze_capture(session)
        model = build_model_input(timing)

        self.assertEqual(model["frames"][2]["frame_index"], 2)
        self.assertEqual(model["frames"][2]["source_sequence"], 4)
        self.assertEqual(timing.frame_clock.expected_rate_hz, 30.0)

    def test_device_session_accepts_unwrapped_source_sequence(self) -> None:
        def move_past_u32(records: list[dict[str, object]]) -> None:
            for record in records:
                record["source_sequence"] = int(record["source_sequence"]) + (1 << 32)

        _rewrite_artifact(self.session, "frames", move_past_u32)

        timing = analyze_capture(self.session)

        self.assertEqual(timing.frames[0]["uvc_sequence"], 1 << 32)

    def test_legacy_raw_capture_routes_through_compatibility_adapter(self) -> None:
        legacy = _make_legacy_capture(self.root)
        timing = analyze_capture(legacy)
        model = build_model_input(timing)

        self.assertEqual(timing.capture.source_schema, "ylx.stereo_imu.raw.v2")
        self.assertEqual(model["video"]["authority"], "legacy_raw")
        self.assertEqual(model["source"]["capture_mode"], "legacy-calibration")
        self.assertEqual(len(model["frames"]), 3)
        self.assertEqual(len(model["imu_samples"]), 6)

    def test_cli_uses_the_same_acceptance_boundary(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            status = check_main([str(self.session)])
        self.assertEqual(status, 0)
        self.assertEqual(json.loads(output.getvalue())["schema"], "rp-ylx.spectacular.check.v1")

    def test_rejects_production_mode_before_reading_artifacts(self) -> None:
        manifest = _manifest(self.session)
        manifest["capture_mode"] = "production"
        _write_manifest(self.session, manifest)
        with self.assertRaisesRegex(CaptureValidationError, "only accepts calibration"):
            load_capture(self.session)

    def test_rejects_non_raw_layout_and_audio_artifact(self) -> None:
        manifest = _manifest(self.session)
        manifest["video"]["layout"] = "split-eyes"  # type: ignore[index]
        _write_manifest(self.session, manifest)
        with self.assertRaisesRegex(CaptureValidationError, "raw-side-by-side"):
            load_capture(self.session)

        manifest["video"]["layout"] = "raw-side-by-side"  # type: ignore[index]
        manifest["audio"]["state"] = "recorded"  # type: ignore[index]
        _write_manifest(self.session, manifest)
        with self.assertRaisesRegex(CaptureValidationError, "must not depend on an audio"):
            load_capture(self.session)

    def test_rejects_unknown_schema_and_wrong_artifact_role(self) -> None:
        manifest = _manifest(self.session)
        manifest["schema"] = "ylx.device-session.v3"
        _write_manifest(self.session, manifest)
        with self.assertRaisesRegex(CaptureValidationError, "unsupported Device Session"):
            load_capture(self.session)

        manifest["schema"] = "ylx.device-session.v2"
        manifest["video"]["artifact"]["role"] = "video.left"  # type: ignore[index]
        _write_manifest(self.session, manifest)
        with self.assertRaisesRegex(CaptureValidationError, "artifact roles"):
            load_capture(self.session)

    def test_rejects_duplicate_keys_and_non_finite_json_numbers(self) -> None:
        manifest_path = self.session / "manifest.json"
        payload = manifest_path.read_text().strip()
        manifest_path.write_text(payload[:-1] + ',"schema":"ylx.device-session.v2"}\n')
        with self.assertRaisesRegex(CaptureValidationError, "duplicate JSON key"):
            load_capture(self.session)

        legacy = _make_legacy_capture(self.root)
        legacy_manifest = legacy / "capture.json"
        legacy_manifest.write_text(legacy_manifest.read_text().replace('"fps":30.0', '"fps":NaN'))
        with self.assertRaisesRegex(CaptureValidationError, "non-finite JSON number"):
            load_capture(legacy)

    def test_rejects_unsafe_path_and_ambiguous_route(self) -> None:
        manifest = _manifest(self.session)
        manifest["frames"]["artifact"]["path"] = "../frames.ndjson"  # type: ignore[index]
        _write_manifest(self.session, manifest)
        with self.assertRaises(CaptureValidationError):
            load_capture(self.session)

        (self.session / "capture.json").write_bytes(_json_bytes({"schema": "other"}))
        with self.assertRaisesRegex(CaptureValidationError, "exactly one"):
            load_capture(self.session)

    def test_rejects_missing_or_digest_mismatched_artifact(self) -> None:
        video = self.session / "video/raw-sbs.mjpeg"
        video.write_bytes(video.read_bytes() + b"tamper")
        with self.assertRaisesRegex(CaptureValidationError, "integrity validation failed"):
            load_capture(self.session)

    def test_rejects_unknown_frame_field_even_with_updated_hash(self) -> None:
        def add_unknown(records: list[dict[str, object]]) -> None:
            records[0]["guessed_eye"] = "left"

        _rewrite_artifact(self.session, "frames", add_unknown)
        with self.assertRaisesRegex(CaptureValidationError, "not a closed record"):
            load_capture(self.session)

    def test_rejects_frame_sequence_gap_even_with_updated_hash(self) -> None:
        def create_gap(records: list[dict[str, object]]) -> None:
            records[2]["source_sequence"] = 7

        _rewrite_artifact(self.session, "frames", create_gap)
        with self.assertRaisesRegex(CaptureValidationError, "sequence gap"):
            analyze_capture(self.session)


if __name__ == "__main__":
    unittest.main()
