import hashlib
import json
import os
import tempfile
import unittest
from contextlib import suppress
from pathlib import Path
from unittest.mock import patch

from rp_ylx.api import Principal, SecurityPolicy, downloads
from rp_ylx.api.downloads import DirectorySessionStore
from tests.testkit import SESSION_ID, Cancelled, DirectoryArtifactProvider, GatewayHarness


class ManifestInputTests(unittest.TestCase):
    def setUp(self) -> None:
        downloads._clear_validated_manifest_cache_for_tests()
        self.addCleanup(downloads._clear_validated_manifest_cache_for_tests)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.generation_root = Path(self.temporary.name) / "generation"
        self.session_root = self.generation_root / SESSION_ID
        self.session_root.mkdir(parents=True)

        def write_artifact(
            role: str, relative_path: str, media_type: str, payload: bytes
        ) -> dict[str, object]:
            artifact_path = self.session_root / relative_path
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path.write_bytes(payload)
            digest = hashlib.sha256(payload).hexdigest()
            return {
                "artifact_id": digest,
                "role": role,
                "path": relative_path,
                "media_type": media_type,
                "bytes": len(payload),
                "sha256": digest,
            }

        left_artifact = write_artifact("video.left", "video/left.mp4", "video/mp4", b"left")
        right_artifact = write_artifact("video.right", "video/right.mp4", "video/mp4", b"right")
        imu_artifact = write_artifact("imu.samples", "imu/imu.ndjson", "application/x-ndjson", b"")
        frames_artifact = write_artifact(
            "frames.index", "frames/frames.ndjson", "application/x-ndjson", b'{"frame":0}\n'
        )
        self.manifest = {
            "schema": "ylx.device-session.v1",
            "manifest_id": "01989f6a-2c01-7b2c-9d3e-4f5061728394",
            "sealed": True,
            "sealed_at": "2026-08-08T02:24:02Z",
            "session_id": SESSION_ID,
            "volume_id": "6ba7b810-9dad-41d1-80b4-00c04fd430c8",
            "capture_mode": "production",
            "display_name": "2026-08-08_10-24-00_YLX-30D5872D",
            "device": {
                "device_id": "550e8400-e29b-41d4-a716-446655440000",
                "device_label": "YLX-30D5872D",
                "hardware_fingerprint": f"sha256:{'a' * 64}",
                "platform": "D-Robotics RDK X5 V1.0",
                "software_version": "0.5.0-dev",
                "commit": "2db57ae68e04197397b8ac84f4d71548aa2fcb36",
            },
            "time": {
                "started_at": "2026-08-08T02:24:00Z",
                "ended_at": "2026-08-08T02:24:01Z",
                "timezone": "Asia/Shanghai",
                "duration_seconds": 1,
            },
            "take": {
                "take_id": "01989f69-f000-7c3d-ae4f-5061728394a5",
                "sequence": 1,
                "continuation_of": None,
            },
            "camera": {
                "width": 2,
                "height": 1,
                "eye_width": 1,
                "sensor_fps": 30,
                "frame_decimation": 1,
                "effective_fps": 30,
                "coordinate_frame": "opencv_optical",
            },
            "video": {
                "layout": "split-eyes",
                "codec": "h264",
                "container": "mp4",
                "segments": [
                    {
                        "index": 0,
                        "start_frame": 0,
                        "end_frame": 1,
                        "start_time_seconds": 0,
                        "end_time_seconds": 1,
                        "artifacts": {"left": left_artifact, "right": right_artifact},
                    }
                ],
            },
            "imu": {
                "artifact": imu_artifact,
                "sample_count": 0,
                "units": "raw_int16",
                "coordinate_frame": "opencv_optical",
            },
            "frames": {"artifact": frames_artifact, "count": 1},
            "logs": [],
            "integrity": {
                "verified_at": "2026-08-08T02:24:01Z",
                "dropped_frames": 0,
                "drop_events": [],
                "fatal_errors": [],
            },
        }
        self.manifest_bytes = json.dumps(
            self.manifest, ensure_ascii=False, separators=(",", ":")
        ).encode()
        self.path = self.session_root / "manifest.json"
        self.path.write_bytes(self.manifest_bytes)
        self.verdicts = {SESSION_ID: hashlib.sha256(self.manifest_bytes).hexdigest()}
        self.store = DirectorySessionStore(self.generation_root, verified_manifests=self.verdicts)
        self.addCleanup(self.store.close)
        reader = Principal("reader", permissions={"getSession": None})
        self.gateway = GatewayHarness(
            provider=DirectoryArtifactProvider(self.store),
            security=SecurityPolicy.customer(tokens={"reader-token": reader}),
        ).setUp()
        self.addCleanup(self.gateway.tearDown)
        self.errors: list[str] = []
        self.gateway.server.handle_error = lambda *args: self.errors.append("unhandled")

    def write_manifest(self, manifest: dict[str, object]) -> None:
        payload = json.dumps(manifest, ensure_ascii=False, separators=(",", ":")).encode()
        self.write_payload(payload)

    def descriptors(self):
        result = set()
        for value in os.listdir("/proc/self/fd"):
            with suppress(OSError):
                os.fstat(int(value))
                result.add(value)
        return result

    def close_leaked(self, before):
        for value in self.descriptors() - before:
            with suppress(OSError):
                os.close(int(value))

    def write_payload(self, payload):
        self.path.write_bytes(payload)
        self.verdicts[SESSION_ID] = hashlib.sha256(payload).hexdigest()

    def test_invalid_json_depth_is_rejected_without_descriptor_leak(self):
        self.write_payload(b'{"x":' + b"[" * 10000 + b"0" + b"]" * 10000 + b"}")
        before = self.descriptors()
        try:
            for version in ("v2", "v3", "v4"):
                with self.subTest(version=version):
                    with self.assertRaises(downloads.ArtifactAccessError) as error:
                        self.store.open_manifest(SESSION_ID, version)
                    self.assertEqual(error.exception.code, "not_found")
                    self.assertEqual(self.descriptors(), before)
        finally:
            self.close_leaked(before)

    def test_invalid_schema_types_return_structured_http_error(self):
        before = self.descriptors()
        try:
            for schema in ([], {}, ["ylx.device-session.v2"]):
                manifest = {**self.manifest, "schema": schema}
                self.write_manifest(manifest)
                for version in ("v3", "v4"):
                    with self.subTest(schema=schema, version=version):
                        status, body, _ = self.gateway.request(
                            f"/api/{version}/sessions/{SESSION_ID}",
                            token="reader-token",
                        )
                        self.assertEqual(status, 404)
                        self.assertEqual(json.loads(body)["error"]["code"], "not_found")
            self.assertEqual(self.errors, [])
        finally:
            self.close_leaked(before)
        self.write_payload(self.manifest_bytes)
        status, body, _ = self.gateway.request(
            f"/api/v4/sessions/{SESSION_ID}", token="reader-token"
        )
        self.assertEqual((status, body), (200, self.manifest_bytes))

    def test_producer_validator_rejects_schema_types(self):
        for schema in ([], {}, ["ylx.device-session.v2"]):
            with self.subTest(schema=schema):
                with self.assertRaises(downloads.ArtifactAccessError) as error:
                    downloads.validate_device_session_manifest({**self.manifest, "schema": schema})
                self.assertEqual(error.exception.code, "not_verified")

    def test_validation_recursion_is_not_an_unhandled_exception(self):
        with patch.object(
            downloads,
            "_validate_device_session_manifest",
            side_effect=RecursionError("nested metadata"),
        ):
            with self.assertRaises(downloads.ArtifactAccessError) as error:
                self.store.open_manifest(SESSION_ID, "v4")
            self.assertEqual(error.exception.code, "not_verified")

    def test_unexpected_failure_or_cancellation_closes_opened_manifest_descriptors(self):
        for stage in ("_FileIdentity.read", "_read_exact_file", "_validated_manifest"):
            target, attribute = (
                (downloads._FileIdentity, "read")
                if stage == "_FileIdentity.read"
                else (downloads, stage)
            )
            for failure in (RuntimeError("unexpected"), Cancelled()):
                with self.subTest(stage=stage, error=type(failure).__name__):
                    before = self.descriptors()
                    try:
                        with (
                            patch.object(target, attribute, side_effect=failure),
                            self.assertRaises(type(failure)),
                        ):
                            self.store.open_manifest(SESSION_ID, "v4")
                        self.assertEqual(self.descriptors(), before)
                    finally:
                        self.close_leaked(before)

    def test_cancelled_path_walks_close_intermediate_descriptors(self):
        before = self.descriptors()
        try:
            with (
                patch.object(downloads, "_open_directory_component", side_effect=Cancelled()),
                self.assertRaises(Cancelled),
            ):
                downloads._open_absolute_directory(self.session_root)
            self.assertEqual(self.descriptors(), before)
            (self.session_root / "cancel.bin").write_bytes(b"test")
            parent = os.open(self.session_root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                held = self.descriptors()
                with (
                    patch.object(downloads, "_native_open_relative_regular", return_value=None),
                    patch.object(downloads._FileIdentity, "read", side_effect=Cancelled()),
                    self.assertRaises(Cancelled),
                ):
                    downloads._open_relative_regular("cancel.bin", parent)
                self.assertEqual(self.descriptors(), held)
            finally:
                os.close(parent)
        finally:
            self.close_leaked(before)


if __name__ == "__main__":
    unittest.main()
