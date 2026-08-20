from __future__ import annotations

import hashlib
import http.client
import json
import os
import tempfile
import threading
import unittest
import uuid
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import rp_ylx.api.downloads as downloads
from rp_ylx.api import Principal, SecurityPolicy, create_gateway_server
from rp_ylx.api.downloads import (
    ArtifactAccessError,
    DirectorySessionStore,
    LockedBytes,
    UnsatisfiableRange,
    parse_single_range,
)
from rp_ylx.native import NativeModuleError
from rp_ylx.recording import RecordingConfig, SessionRecorder

SESSION_ID = "01989f6a-2c00-7a1b-8c2d-3e4f50617283"
ARTIFACT_BYTES = b"immutable-session-artifact"
ARTIFACT_ID = hashlib.sha256(ARTIFACT_BYTES).hexdigest()


class RangeParserTest(unittest.TestCase):
    def setUp(self) -> None:
        downloads._RANGE_PARSER_UNAVAILABLE = False

    def tearDown(self) -> None:
        downloads._RANGE_PARSER_UNAVAILABLE = False

    def test_native_range_parser_is_preferred(self) -> None:
        with patch("rp_ylx.api.downloads.parse_native_single_range", return_value=(2, 8)) as native:
            self.assertEqual(parse_single_range("bytes=2-8", 26), (2, 8))
        native.assert_called_once_with("bytes=2-8", 26)

    def test_missing_native_range_parser_falls_back_to_python(self) -> None:
        with patch(
            "rp_ylx.api.downloads.parse_native_single_range",
            side_effect=NativeModuleError("native_range_parser_unavailable", "missing"),
        ):
            self.assertEqual(parse_single_range("bytes=-4", 26), (22, 25))
        self.assertTrue(downloads._RANGE_PARSER_UNAVAILABLE)

    def test_native_unsatisfiable_range_maps_to_contract_error(self) -> None:
        with (
            patch(
                "rp_ylx.api.downloads.parse_native_single_range",
                side_effect=ValueError("range_not_satisfiable"),
            ),
            self.assertRaises(UnsatisfiableRange) as raised,
        ):
            parse_single_range("bytes=26-", 26)
        self.assertEqual(raised.exception.complete_size, 26)


@dataclass(frozen=True, slots=True)
class _Descriptor:
    artifact_id: str
    path: str
    media_type: str
    bytes: int
    sha256: str


class _MemoryArtifact:
    def __init__(self, payload: bytes) -> None:
        digest = hashlib.sha256(payload).hexdigest()
        self.descriptor = _Descriptor(
            artifact_id=digest,
            path="video/left.mp4",
            media_type="video/mp4",
            bytes=len(payload),
            sha256=digest,
        )
        self.etag = f'"{digest}"'
        self.size = len(payload)
        self.content_type = "video/mp4"
        self._payload = payload

    def __enter__(self) -> _MemoryArtifact:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self, offset: int = 0, length: int | None = None) -> bytes:
        end = None if length is None else offset + length
        return self._payload[offset:end]

    def iter_chunks(
        self,
        offset: int = 0,
        length: int | None = None,
        *,
        chunk_size: int = 1024 * 1024,
    ) -> object:
        del chunk_size
        yield self.read(offset, length)


class _MemoryProvider:
    def __init__(
        self,
        *,
        io_state: str | None = None,
        access_error: ArtifactAccessError | None = None,
    ) -> None:
        self.io_state = io_state
        self.access_error = access_error
        self.open_count = 0

    def artifact_io_state(self) -> str | None:
        return self.io_state

    def open_verified_artifact(
        self, session_id: str, artifact_id: str, api_version: str
    ) -> _MemoryArtifact:
        self.open_count += 1
        if api_version not in {"v2", "v3"}:
            raise AssertionError("gateway 传递了未知 API 版本")
        if self.access_error is not None:
            raise self.access_error
        if session_id != SESSION_ID or artifact_id != ARTIFACT_ID:
            raise AssertionError("gateway changed the authorized artifact identity")
        return _MemoryArtifact(ARTIFACT_BYTES)


class _DirectoryProvider:
    def __init__(self, store: DirectorySessionStore) -> None:
        self.store = store
        self.io_state: str | None = None
        self.after_open: object | None = None
        self.after_manifest_open: object | None = None

    def artifact_io_state(self) -> str | None:
        return self.io_state

    def open_manifest(self, session_id: str, api_version: str) -> object:
        locked = self.store.open_manifest(session_id, api_version)
        if callable(self.after_manifest_open):
            self.after_manifest_open()
        return locked

    def open_verified_artifact(self, session_id: str, artifact_id: str, api_version: str) -> object:
        locked = self.store.open_verified_artifact(session_id, artifact_id, api_version)
        if callable(self.after_open):
            self.after_open()
        return locked


class _FakeNativeSessionIo:
    def sendfile(
        self,
        output_descriptor: int,
        input_descriptor: int,
        offset: int,
        length: int,
    ) -> int:
        sent = 0
        cursor = offset
        remaining = length
        while remaining:
            block = os.pread(input_descriptor, min(1024 * 1024, remaining), cursor)
            if not block:
                raise RuntimeError("short test sendfile")
            view = memoryview(block)
            while view:
                written = os.write(output_descriptor, view)
                if written <= 0:
                    raise RuntimeError("test sendfile wrote zero bytes")
                sent += written
                view = view[written:]
            cursor += len(block)
            remaining -= len(block)
        return sent


class _NativeArtifacts(_FakeNativeSessionIo):
    def __init__(self, descriptors: list[dict[str, object]] | None = None) -> None:
        self.calls: list[tuple[bytes, str]] = []
        self.descriptors = [] if descriptors is None else descriptors

    def device_session_v1_artifacts(
        self,
        manifest: bytes,
        session_id: str,
    ) -> list[dict[str, object]]:
        self.calls.append((manifest, session_id))
        return self.descriptors


class _NativeSummary(_FakeNativeSessionIo):
    def __init__(self, summary: dict[str, object]) -> None:
        self.summary = summary
        self.summary_calls: list[tuple[bytes, str]] = []

    def device_session_v1_summary(
        self,
        manifest: bytes,
        session_id: str,
    ) -> dict[str, object]:
        self.summary_calls.append((manifest, session_id))
        return self.summary


class _NativeSingleArtifact(_NativeArtifacts):
    def __init__(self, descriptor: dict[str, object] | None) -> None:
        super().__init__([])
        self.single_calls: list[tuple[bytes, str, str]] = []
        self.descriptor = descriptor

    def device_session_v1_artifact(
        self,
        manifest: bytes,
        session_id: str,
        artifact_id: str,
    ) -> dict[str, object] | None:
        self.single_calls.append((manifest, session_id, artifact_id))
        if self.descriptor is not None and self.descriptor["artifact_id"] == artifact_id:
            return self.descriptor
        return None


class _NativeOpenRelativeRegular(_NativeSingleArtifact):
    def __init__(self, descriptor: dict[str, object]) -> None:
        super().__init__(descriptor)
        self.open_calls: list[tuple[int, str]] = []
        self.read_calls: list[tuple[int, int]] = []

    def open_relative_regular(self, root_descriptor: int, relative_path: str) -> int:
        self.open_calls.append((root_descriptor, relative_path))
        return os.open(
            relative_path,
            downloads._required_open_flags(),
            dir_fd=root_descriptor,
        )

    def read_bounded_fd(self, descriptor: int, maximum_bytes: int) -> bytes:
        self.read_calls.append((descriptor, maximum_bytes))
        return os.pread(descriptor, maximum_bytes + 1, 0)


class _MissingNativeArtifacts(_FakeNativeSessionIo):
    pass


class _FailingNativeArtifacts(_FakeNativeSessionIo):
    def device_session_v1_artifacts(
        self,
        manifest: bytes,
        session_id: str,
    ) -> list[dict[str, object]]:
        del manifest, session_id
        raise RuntimeError("manifest_invalid: bad artifact list")


class SessionDownloadHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.provider = _MemoryProvider()
        reader = Principal(
            "reader",
            permissions={
                "getSessionArtifact": {SESSION_ID},
                "headSessionArtifact": {SESSION_ID},
            },
        )
        denied = Principal("denied", permissions={})
        self.server = create_gateway_server(
            "127.0.0.1",
            0,
            self.provider,
            security=SecurityPolicy.customer(
                tokens={"reader-token": reader, "denied-token": denied}
            ),
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(
        self,
        path: str,
        *,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        token: str | None = "reader-token",
    ) -> tuple[int, bytes, object]:
        request_headers = {} if token is None else {"Authorization": f"Bearer {token}"}
        request_headers.update(headers or {})
        request = Request(
            self.base + path,
            headers=request_headers,
            method=method,
        )
        try:
            with urlopen(request, timeout=2) as response:
                return response.status, response.read(), response.headers
        except HTTPError as error:
            return error.code, error.read(), error.headers

    def test_authorized_get_returns_complete_immutable_artifact(self) -> None:
        for api_version in ("v2", "v3"):
            with self.subTest(api_version=api_version):
                status, payload, headers = self.request(
                    f"/api/{api_version}/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"
                )

                self.assertEqual(status, 200)
                self.assertEqual(payload, ARTIFACT_BYTES)
                self.assertEqual(headers["Accept-Ranges"], "bytes")
                self.assertEqual(headers["Content-Type"], "video/mp4")
                self.assertEqual(headers["Content-Length"], str(len(ARTIFACT_BYTES)))
                self.assertEqual(headers["ETag"], f'"{ARTIFACT_ID}"')

    def test_get_honors_one_inclusive_byte_range(self) -> None:
        status, payload, headers = self.request(
            f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}",
            headers={"Range": "bytes=2-8"},
        )

        self.assertEqual(status, 206)
        self.assertEqual(payload, ARTIFACT_BYTES[2:9])
        self.assertEqual(headers["Content-Range"], f"bytes 2-8/{len(ARTIFACT_BYTES)}")
        self.assertEqual(headers["Content-Length"], "7")
        self.assertEqual(headers["ETag"], f'"{ARTIFACT_ID}"')

    def test_busy_gate_precedes_range_parsing_and_artifact_lookup(self) -> None:
        self.provider.io_state = "recording"

        status, payload, headers = self.request(
            f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}",
            headers={"Range": "bytes=invalid,multiple"},
        )

        self.assertEqual(status, 423)
        self.assertEqual(headers["YLX-Error-Code"], "capture_busy")
        self.assertEqual(headers["YLX-Wait-State"], "idle")
        self.assertEqual(headers["Retry-After"], "1")
        self.assertEqual(json.loads(payload)["error"]["code"], "capture_busy")
        self.assertEqual(self.provider.open_count, 0)

    def test_verification_gate_precedes_range_and_representation_lookup(self) -> None:
        self.provider.access_error = ArtifactAccessError(
            "not_verified", "missing exact-manifest verdict"
        )

        status, payload, headers = self.request(
            f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}",
            headers={"Range": "bytes=invalid,multiple"},
        )

        self.assertEqual(status, 409)
        self.assertEqual(headers["YLX-Error-Code"], "session_not_verified")
        self.assertIsNone(headers["Content-Range"])
        self.assertEqual(json.loads(payload)["error"]["code"], "session_not_verified")
        self.assertEqual(self.provider.open_count, 1)

    def test_range_forms_if_range_and_unsatisfied_response(self) -> None:
        path = f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"
        cases = (
            ("bytes=10-", None, 206, ARTIFACT_BYTES[10:], "bytes 10-25/26"),
            ("bytes=-4", None, 206, ARTIFACT_BYTES[-4:], "bytes 22-25/26"),
            ("bytes=0-999", None, 206, ARTIFACT_BYTES, "bytes 0-25/26"),
            (
                "bytes=2-8",
                f'"{ARTIFACT_ID}"',
                206,
                ARTIFACT_BYTES[2:9],
                "bytes 2-8/26",
            ),
            ("bytes=2-8", f'"{"0" * 64}"', 200, ARTIFACT_BYTES, None),
        )
        for range_value, if_range, expected_status, expected_body, content_range in cases:
            with self.subTest(range_value=range_value, if_range=if_range):
                request_headers = {"Range": range_value}
                if if_range is not None:
                    request_headers["If-Range"] = if_range
                status, payload, headers = self.request(path, headers=request_headers)
                self.assertEqual(status, expected_status)
                self.assertEqual(payload, expected_body)
                self.assertEqual(headers["Content-Range"], content_range)

        invalid_ranges = (
            "bytes=0-1,3-4",
            "bytes=26-",
            "bytes=8-2",
            "bytes=-0",
            "items=0-1",
            "bytes=invalid",
            "bytes=" + "9" * 100,
        )
        for range_value in invalid_ranges:
            with self.subTest(range_value=range_value):
                status, payload, headers = self.request(path, headers={"Range": range_value})
                self.assertEqual(status, 416)
                self.assertEqual(headers["Content-Range"], "bytes */26")
                self.assertEqual(json.loads(payload)["error"]["code"], "range_not_satisfiable")

    def test_repeated_range_control_headers_are_rejected(self) -> None:
        path = f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"
        cases = (
            (("Range", "bytes=0-1"), ("Range", "bytes=3-4")),
            (("Range", "bytes=0-1"), ("Range", "bytes=0-1")),
        )
        for headers in cases:
            with self.subTest(headers=headers):
                connection = http.client.HTTPConnection(
                    "127.0.0.1", self.server.server_port, timeout=2
                )
                try:
                    connection.putrequest("GET", path)
                    connection.putheader("Authorization", "Bearer reader-token")
                    for name, value in headers:
                        connection.putheader(name, value)
                    connection.endheaders()
                    response = connection.getresponse()
                    payload = response.read()
                finally:
                    connection.close()

                self.assertEqual(response.status, 416)
                self.assertEqual(response.headers["Content-Range"], "bytes */26")
                self.assertEqual(json.loads(payload)["error"]["code"], "range_not_satisfiable")

        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=2)
        try:
            connection.putrequest("GET", path)
            connection.putheader("Authorization", "Bearer reader-token")
            connection.putheader("Range", "bytes=0-1")
            connection.putheader("If-Range", f'"{ARTIFACT_ID}"')
            connection.putheader("If-Range", f'"{ARTIFACT_ID}"')
            connection.endheaders()
            response = connection.getresponse()
            payload = response.read()
        finally:
            connection.close()

        self.assertEqual(response.status, 200)
        self.assertEqual(payload, ARTIFACT_BYTES)
        self.assertIsNone(response.headers["Content-Range"])

    def test_head_never_sends_a_body_for_success_or_error(self) -> None:
        path = f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"

        status, payload, headers = self.request(
            path,
            method="HEAD",
            headers={"Range": "bytes=invalid,multiple"},
        )
        self.assertEqual((status, payload), (200, b""))
        self.assertEqual(headers["Content-Length"], str(len(ARTIFACT_BYTES)))

        self.provider.io_state = "recording"
        status, payload, headers = self.request(path, method="HEAD")
        self.assertEqual((status, payload, headers["Content-Length"]), (423, b"", "0"))

        self.provider.io_state = None
        self.provider.access_error = ArtifactAccessError("not_verified", "stale")
        status, payload, headers = self.request(path, method="HEAD")
        self.assertEqual((status, payload, headers["Content-Length"]), (409, b"", "0"))

        self.provider.access_error = ArtifactAccessError("not_found", "missing")
        status, payload, headers = self.request(path, method="HEAD")
        self.assertEqual((status, payload, headers["Content-Length"]), (404, b"", "0"))

        for token, expected_status in ((None, 401), ("denied-token", 403)):
            with self.subTest(token=token):
                status, payload, headers = self.request(path, method="HEAD", token=token)
                self.assertEqual((status, payload), (expected_status, b""))
                self.assertEqual(headers["Content-Length"], "0")


class DirectorySessionDownloadHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.generation_root = Path(self.temporary.name) / "generation"
        self.session_root = self.generation_root / SESSION_ID
        self.session_root.mkdir(parents=True)

        def write_artifact(
            role: str,
            relative_path: str,
            media_type: str,
            payload: bytes,
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

        left_artifact = write_artifact(
            "video.left",
            "video/left.mp4",
            "video/mp4",
            ARTIFACT_BYTES,
        )
        right_artifact = write_artifact(
            "video.right",
            "video/right.mp4",
            "video/mp4",
            b"right-eye-artifact",
        )
        imu_artifact = write_artifact(
            "imu.samples",
            "imu/imu.ndjson",
            "application/x-ndjson",
            b"",
        )
        frames_artifact = write_artifact(
            "frames.index",
            "frames/frames.ndjson",
            "application/x-ndjson",
            b'{"frame":0}\n',
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
                        "artifacts": {
                            "left": left_artifact,
                            "right": right_artifact,
                        },
                    }
                ],
            },
            "imu": {
                "artifact": imu_artifact,
                "sample_count": 0,
                "units": "raw_int16",
                "coordinate_frame": "opencv_optical",
            },
            "frames": {
                "artifact": frames_artifact,
                "count": 1,
            },
            "logs": [],
            "integrity": {
                "verified_at": "2026-08-08T02:24:01Z",
                "dropped_frames": 0,
                "drop_events": [],
                "fatal_errors": [],
            },
        }
        self.manifest_bytes = json.dumps(
            self.manifest,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
        (self.session_root / "manifest.json").write_bytes(self.manifest_bytes)
        self.manifest_sha256 = hashlib.sha256(self.manifest_bytes).hexdigest()
        self.verdicts = {SESSION_ID: self.manifest_sha256}
        self.store = DirectorySessionStore(
            self.generation_root,
            verified_manifests=self.verdicts,
        )
        self.provider = _DirectoryProvider(self.store)
        reader = Principal(
            "reader",
            permissions={
                "getSession": None,
                "getSessionArtifact": None,
                "headSessionArtifact": None,
            },
        )
        self.server = create_gateway_server(
            "127.0.0.1",
            0,
            self.provider,
            security=SecurityPolicy.customer(tokens={"reader-token": reader}),
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"
        downloads._DEVICE_SESSION_SUMMARY_UNAVAILABLE = False
        downloads._DEVICE_SESSION_ARTIFACTS_UNAVAILABLE = False

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.store.close()
        self.temporary.cleanup()
        downloads._DEVICE_SESSION_SUMMARY_UNAVAILABLE = False
        downloads._DEVICE_SESSION_ARTIFACTS_UNAVAILABLE = False

    def request(
        self,
        path: str,
        *,
        method: str = "GET",
        headers: dict[str, str] | None = None,
    ) -> tuple[int, bytes, object]:
        request_headers = {"Authorization": "Bearer reader-token"}
        request_headers.update(headers or {})
        request = Request(
            self.base + path,
            method=method,
            headers=request_headers,
        )
        try:
            with urlopen(request, timeout=2) as response:
                return response.status, response.read(), response.headers
        except HTTPError as error:
            return error.code, error.read(), error.headers

    def write_manifest(self, manifest: object) -> bytes:
        payload = json.dumps(
            manifest,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
        (self.session_root / "manifest.json").write_bytes(payload)
        self.verdicts[SESSION_ID] = hashlib.sha256(payload).hexdigest()
        return payload

    def test_directory_store_serves_exact_manifest_and_verified_artifact(self) -> None:
        status, payload, headers = self.request(f"/api/v3/sessions/{SESSION_ID}")

        self.assertEqual(status, 200)
        self.assertEqual(payload, self.manifest_bytes)
        self.assertEqual(headers["ETag"], f'"{self.manifest_sha256}"')
        self.assertEqual(headers["YLX-Manifest-SHA256"], self.manifest_sha256)

        status, payload, headers = self.request(
            f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload, ARTIFACT_BYTES)
        self.assertEqual(headers["Content-Type"], "video/mp4")
        self.assertEqual(headers["ETag"], f'"{ARTIFACT_ID}"')

    def test_directory_store_prefers_native_v1_artifact_descriptors(self) -> None:
        native = _NativeArtifacts(
            [
                {
                    "artifact_id": ARTIFACT_ID,
                    "role": "video.left",
                    "path": "video/left.mp4",
                    "media_type": "video/mp4",
                    "bytes": len(ARTIFACT_BYTES),
                    "sha256": ARTIFACT_ID,
                }
            ]
        )

        with patch("rp_ylx.api.downloads._session_io_or_none", return_value=native):
            status, payload, headers = self.request(
                f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"
            )

        self.assertEqual(status, 200)
        self.assertEqual(payload, ARTIFACT_BYTES)
        self.assertEqual(headers["ETag"], f'"{ARTIFACT_ID}"')
        self.assertEqual(native.calls, [(self.manifest_bytes, SESSION_ID)])

    def test_directory_store_prefers_native_v1_summary_for_artifact_descriptors(self) -> None:
        python_summary = downloads._device_session_v1_summary_python(
            self.manifest,
            manifest_bytes=self.manifest_bytes,
            session_id=SESSION_ID,
        )
        native = _NativeSummary(python_summary)

        with (
            patch("rp_ylx.api.downloads._session_io_or_none", return_value=native),
            patch(
                "rp_ylx.api.downloads._artifact_descriptors",
                side_effect=AssertionError("Python artifact descriptors were used"),
            ),
        ):
            status, payload, headers = self.request(
                f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"
            )

        self.assertEqual(status, 200)
        self.assertEqual(payload, ARTIFACT_BYTES)
        self.assertEqual(headers["ETag"], f'"{ARTIFACT_ID}"')
        self.assertEqual(native.summary_calls, [(self.manifest_bytes, SESSION_ID)])

    def test_device_session_v1_summary_prefers_native_and_matches_python_fallback(self) -> None:
        python_summary = downloads._device_session_v1_summary_python(
            self.manifest,
            manifest_bytes=self.manifest_bytes,
            session_id=SESSION_ID,
        )
        native = _NativeSummary(dict(python_summary))

        with patch("rp_ylx.api.downloads._session_io_or_none", return_value=native):
            summary = downloads.device_session_v1_summary(self.manifest_bytes, SESSION_ID)

        self.assertEqual(native.summary_calls, [(self.manifest_bytes, SESSION_ID)])
        self.assertEqual(summary["total_bytes"], python_summary["total_bytes"])
        self.assertEqual(summary["artifacts"], python_summary["artifacts"])
        self.assertEqual(summary["frames_count"], 1)
        self.assertEqual(summary["imu_sample_count"], 0)
        self.assertIsNone(summary["audio_sample_count"])

    def test_device_session_v1_summary_falls_back_when_native_missing(self) -> None:
        with (
            patch(
                "rp_ylx.api.downloads._session_io_or_none",
                return_value=_MissingNativeArtifacts(),
            ),
            patch(
                "rp_ylx.api.downloads._artifact_descriptors",
                wraps=downloads._artifact_descriptors,
            ) as python_path,
        ):
            summary = downloads.device_session_v1_summary(self.manifest_bytes, SESSION_ID)

        self.assertTrue(downloads._DEVICE_SESSION_SUMMARY_UNAVAILABLE)
        python_path.assert_called_once()
        self.assertEqual(
            summary["total_bytes"],
            sum(item["bytes"] for item in summary["artifacts"]),
        )

    def test_directory_store_prefers_native_v1_single_artifact_descriptor(self) -> None:
        descriptor = {
            "artifact_id": ARTIFACT_ID,
            "role": "video.left",
            "path": "video/left.mp4",
            "media_type": "video/mp4",
            "bytes": len(ARTIFACT_BYTES),
            "sha256": ARTIFACT_ID,
        }
        native = _NativeSingleArtifact(descriptor)

        with patch("rp_ylx.api.downloads._session_io_or_none", return_value=native):
            status, payload, headers = self.request(
                f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"
            )

        self.assertEqual(status, 200)
        self.assertEqual(payload, ARTIFACT_BYTES)
        self.assertEqual(headers["ETag"], f'"{ARTIFACT_ID}"')
        self.assertEqual(native.single_calls, [(self.manifest_bytes, SESSION_ID, ARTIFACT_ID)])
        self.assertEqual(native.calls, [])

    def test_directory_store_prefers_native_relative_artifact_open(self) -> None:
        descriptor = {
            "artifact_id": ARTIFACT_ID,
            "role": "video.left",
            "path": "video/left.mp4",
            "media_type": "video/mp4",
            "bytes": len(ARTIFACT_BYTES),
            "sha256": ARTIFACT_ID,
        }
        native = _NativeOpenRelativeRegular(descriptor)

        with (
            patch("rp_ylx.api.downloads._session_io_or_none", return_value=native),
            patch(
                "rp_ylx.api.downloads._safe_relative_components",
                side_effect=AssertionError("Python relative artifact path opened"),
            ),
        ):
            status, payload, headers = self.request(
                f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"
            )

        self.assertEqual(status, 200)
        self.assertEqual(payload, ARTIFACT_BYTES)
        self.assertEqual(headers["ETag"], f'"{ARTIFACT_ID}"')
        self.assertEqual(native.open_calls[0][1], "video/left.mp4")
        self.assertEqual(len(native.read_calls), 1)

    def test_directory_store_falls_back_when_native_v1_artifact_descriptors_are_missing(
        self,
    ) -> None:
        with (
            patch(
                "rp_ylx.api.downloads._session_io_or_none",
                return_value=_MissingNativeArtifacts(),
            ),
            patch(
                "rp_ylx.api.downloads._artifact_descriptors",
                wraps=downloads._artifact_descriptors,
            ) as python_path,
        ):
            status, payload, _ = self.request(
                f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"
            )

        self.assertEqual(status, 200)
        self.assertEqual(payload, ARTIFACT_BYTES)
        self.assertTrue(downloads._DEVICE_SESSION_ARTIFACTS_UNAVAILABLE)
        python_path.assert_called_once()

    def test_directory_store_native_v1_artifact_descriptor_error_fails_closed(self) -> None:
        with patch(
            "rp_ylx.api.downloads._session_io_or_none",
            return_value=_FailingNativeArtifacts(),
        ):
            status, payload, headers = self.request(
                f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"
            )

        self.assertEqual(status, 409)
        self.assertIsNone(headers["Content-Range"])
        self.assertEqual(json.loads(payload)["error"]["code"], "session_not_verified")

    def test_every_session_path_component_rejects_symbolic_links(self) -> None:
        artifact_url = f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"

        artifact_path = self.session_root / "video" / "left.mp4"
        artifact_real = artifact_path.with_name("left.real.mp4")
        artifact_path.rename(artifact_real)
        artifact_path.symlink_to(artifact_real.name)
        status, payload, _ = self.request(artifact_url)
        self.assertEqual(status, 409)
        self.assertEqual(json.loads(payload)["error"]["code"], "session_not_verified")

    def test_artifact_intermediate_directory_rejects_symbolic_link(self) -> None:
        video_path = self.session_root / "video"
        video_real = self.session_root / "video-real"
        video_path.rename(video_real)
        video_path.symlink_to(video_real.name, target_is_directory=True)

        status, payload, _ = self.request(f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}")

        self.assertEqual(status, 409)
        self.assertEqual(json.loads(payload)["error"]["code"], "session_not_verified")

    def test_manifest_and_session_directory_reject_symbolic_links(self) -> None:
        manifest_path = self.session_root / "manifest.json"
        manifest_real = self.session_root / "manifest.real.json"
        manifest_path.rename(manifest_real)
        manifest_path.symlink_to(manifest_real.name)

        status, payload, _ = self.request(f"/api/v3/sessions/{SESSION_ID}")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(payload)["error"]["code"], "not_found")

        manifest_path.unlink()
        manifest_real.rename(manifest_path)
        session_real = self.generation_root / "real-session"
        self.session_root.rename(session_real)
        self.session_root.symlink_to(session_real.name, target_is_directory=True)

        status, payload, _ = self.request(f"/api/v3/sessions/{SESSION_ID}")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(payload)["error"]["code"], "not_found")

    def test_store_remains_bound_to_open_generation_when_root_path_is_replaced(self) -> None:
        pinned_generation = self.generation_root.with_name("pinned-generation")
        self.generation_root.rename(pinned_generation)
        replacement_session = self.generation_root / SESSION_ID
        replacement_session.mkdir(parents=True)
        (replacement_session / "manifest.json").write_text("not the pinned generation")

        status, payload, _ = self.request(f"/api/v3/sessions/{SESSION_ID}")

        self.assertEqual(status, 200)
        self.assertEqual(payload, self.manifest_bytes)
        status, payload, _ = self.request(f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}")
        self.assertEqual(status, 200)
        self.assertEqual(payload, ARTIFACT_BYTES)

    def test_artifact_path_replacement_after_verification_sends_from_locked_fd(self) -> None:
        artifact_path = self.session_root / "video" / "left.mp4"
        pinned_path = artifact_path.with_name("left.pinned.mp4")

        def replace_path() -> None:
            artifact_path.rename(pinned_path)
            artifact_path.write_bytes(b"replacement-path-contents")

        self.provider.after_open = replace_path

        status, payload, headers = self.request(
            f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"
        )

        self.assertEqual(status, 200)
        self.assertEqual(payload, ARTIFACT_BYTES)
        self.assertEqual(headers["ETag"], f'"{ARTIFACT_ID}"')

    def test_artifact_in_place_mutation_after_open_is_rejected(self) -> None:
        artifact_path = self.session_root / "video" / "left.mp4"
        replacement = b"x" * len(ARTIFACT_BYTES)

        locked = self.store.open_verified_artifact(SESSION_ID, ARTIFACT_ID, "v3")
        artifact_path.write_bytes(replacement)
        with locked, self.assertRaises(ArtifactAccessError) as rejected:
            locked.read()
        self.assertEqual(rejected.exception.code, "not_verified")

    def test_head_and_single_byte_range_read_only_requested_artifact_bytes(self) -> None:
        artifact_url = f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"
        real_send_to = LockedBytes.send_to
        artifact_sends: list[int] = []

        def measured_send_to(
            locked: LockedBytes,
            output_descriptor: int,
            offset: int = 0,
            length: int | None = None,
        ) -> int:
            if locked.size == len(ARTIFACT_BYTES):
                artifact_sends.append(locked.size - offset if length is None else length)
            return real_send_to(locked, output_descriptor, offset, length)

        with patch("rp_ylx.api.downloads.LockedBytes.send_to", measured_send_to):
            status, payload, _ = self.request(artifact_url, method="HEAD")
        self.assertEqual((status, payload, artifact_sends), (200, b"", []))

        artifact_sends.clear()
        with patch("rp_ylx.api.downloads.LockedBytes.send_to", measured_send_to):
            status, payload, _ = self.request(
                artifact_url,
                headers={"Range": "bytes=0-0"},
            )
        self.assertEqual((status, payload, artifact_sends), (206, ARTIFACT_BYTES[:1], [1]))

    def test_manifest_path_replacement_after_open_sends_exact_locked_bytes(self) -> None:
        manifest_path = self.session_root / "manifest.json"
        pinned_path = self.session_root / "manifest.pinned.json"

        def replace_path() -> None:
            manifest_path.rename(pinned_path)
            manifest_path.write_text("replacement manifest path contents")

        self.provider.after_manifest_open = replace_path

        status, payload, headers = self.request(f"/api/v3/sessions/{SESSION_ID}")

        self.assertEqual(status, 200)
        self.assertEqual(payload, self.manifest_bytes)
        self.assertEqual(headers["ETag"], f'"{self.manifest_sha256}"')

    def test_manifest_in_place_mutation_after_open_cannot_change_exact_response(self) -> None:
        manifest_path = self.session_root / "manifest.json"

        def mutate_open_file() -> None:
            manifest_path.write_text("mutated after exact bytes were admitted")

        self.provider.after_manifest_open = mutate_open_file

        status, payload, headers = self.request(f"/api/v3/sessions/{SESSION_ID}")

        self.assertEqual(status, 200)
        self.assertEqual(payload, self.manifest_bytes)
        self.assertEqual(headers["Content-Length"], str(len(self.manifest_bytes)))
        self.assertEqual(headers["ETag"], f'"{self.manifest_sha256}"')

    def test_unsealed_manifest_is_not_a_downloadable_session(self) -> None:
        self.manifest["sealed"] = False
        self.write_manifest(self.manifest)

        status, payload, _ = self.request(f"/api/v3/sessions/{SESSION_ID}")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(payload)["error"]["code"], "not_found")

        status, payload, headers = self.request(
            f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"
        )
        self.assertEqual(status, 409)
        self.assertEqual(headers["YLX-Error-Code"], "session_not_verified")
        error = json.loads(payload)["error"]
        self.assertEqual(error["code"], "session_not_verified")
        self.assertEqual(error["details"]["reason"], "unusable")

    def test_v2_serves_real_default_uuid_v4_v0_session(self) -> None:
        recorder = SessionRecorder(
            self.generation_root,
            RecordingConfig(
                device_id="rdk-x5-test",
                software_version="0.5.0-dev",
                width=2,
                height=1,
                fps=30.0,
                encoding="test-bytes",
            ),
        )
        recorder.start()
        legacy_session_id = recorder.session_id
        self.assertIsNotNone(legacy_session_id)
        assert legacy_session_id is not None
        self.assertEqual(uuid.UUID(legacy_session_id).version, 4)
        legacy_root = recorder.stop()
        legacy_bytes = (legacy_root / "manifest.json").read_bytes()
        legacy_manifest = json.loads(legacy_bytes)
        legacy_artifact = next(
            artifact
            for artifact in legacy_manifest["artifacts"]
            if artifact["role"] == "video.left"
        )
        legacy_artifact_id = legacy_artifact["sha256"]
        legacy_artifact_bytes = (legacy_root / legacy_artifact["path"]).read_bytes()
        legacy_manifest_sha256 = hashlib.sha256(legacy_bytes).hexdigest()
        self.verdicts[legacy_session_id] = legacy_manifest_sha256

        status, payload, headers = self.request(f"/api/v2/sessions/{legacy_session_id}")
        self.assertEqual(status, 200)
        self.assertEqual(payload, legacy_bytes)
        self.assertEqual(headers["ETag"], f'"{legacy_manifest_sha256}"')

        artifact_url = f"/api/v2/sessions/{legacy_session_id}/artifacts/{legacy_artifact_id}"
        status, payload, headers = self.request(artifact_url)
        self.assertEqual(status, 200)
        self.assertEqual(payload, legacy_artifact_bytes)
        self.assertEqual(headers["ETag"], f'"{legacy_artifact_id}"')

        status, payload, headers = self.request(artifact_url, method="HEAD")
        self.assertEqual((status, payload), (200, b""))
        self.assertEqual(headers["Content-Length"], str(len(legacy_artifact_bytes)))

        status, payload, headers = self.request(
            artifact_url,
            headers={"Range": "bytes=1-3"},
        )
        self.assertEqual(status, 206)
        self.assertEqual(payload, legacy_artifact_bytes[1:4])
        self.assertEqual(
            headers["Content-Range"],
            f"bytes 1-3/{len(legacy_artifact_bytes)}",
        )

        status, payload, _ = self.request(f"/api/v3/sessions/{legacy_session_id}")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(payload)["error"]["code"], "not_found")

        status, payload, headers = self.request(
            f"/api/v3/sessions/{legacy_session_id}/artifacts/{legacy_artifact_id}"
        )
        self.assertEqual(status, 404)
        self.assertIsNone(headers["YLX-Error-Code"])
        self.assertEqual(json.loads(payload)["error"]["code"], "not_found")

    def test_incomplete_v0_and_v1_manifests_fail_closed(self) -> None:
        incomplete_manifests = (
            (
                "v1",
                "v3",
                {
                    "schema": "ylx.device-session.v1",
                    "sealed": True,
                    "session_id": SESSION_ID,
                    "video": {
                        "artifact": {
                            "artifact_id": ARTIFACT_ID,
                            "role": "video.left",
                            "path": "video/left.mp4",
                            "media_type": "video/mp4",
                            "bytes": len(ARTIFACT_BYTES),
                            "sha256": ARTIFACT_ID,
                        }
                    },
                },
            ),
            (
                "v0",
                "v2",
                {
                    "format": "ylx.recording-session.v0",
                    "state": "sealed",
                    "session_id": SESSION_ID,
                    "artifacts": [
                        {
                            "role": "video.left",
                            "path": "video/left.mp4",
                            "media_type": "video/mp4",
                            "bytes": len(ARTIFACT_BYTES),
                            "sha256": ARTIFACT_ID,
                            "records": 1,
                        }
                    ],
                },
            ),
        )
        for label, api_version, manifest in incomplete_manifests:
            with self.subTest(manifest=label):
                self.write_manifest(manifest)

                status, payload, _ = self.request(f"/api/{api_version}/sessions/{SESSION_ID}")
                self.assertEqual(status, 404)
                self.assertEqual(json.loads(payload)["error"]["code"], "not_found")

                status, payload, headers = self.request(
                    f"/api/{api_version}/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"
                )
                self.assertEqual(status, 409)
                self.assertEqual(headers["YLX-Error-Code"], "session_not_verified")
                error = json.loads(payload)["error"]
                self.assertEqual(error["code"], "session_not_verified")
                self.assertEqual(error["details"]["reason"], "unusable")

    def test_manifest_inspection_is_exact_but_artifact_requires_current_verdict(self) -> None:
        artifact_url = f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"
        self.verdicts.clear()

        status, payload, headers = self.request(f"/api/v3/sessions/{SESSION_ID}")
        self.assertEqual(status, 200)
        self.assertEqual(payload, self.manifest_bytes)
        self.assertEqual(headers["ETag"], f'"{self.manifest_sha256}"')
        status, payload, headers = self.request(artifact_url)
        self.assertEqual(status, 409)
        self.assertIsNone(headers["Content-Range"])
        error = json.loads(payload)["error"]
        self.assertEqual(error["code"], "session_not_verified")
        self.assertEqual(error["details"]["reason"], "absent")

        self.verdicts[SESSION_ID] = self.manifest_sha256
        changed_manifest = self.manifest_bytes + b"\n"
        (self.session_root / "manifest.json").write_bytes(changed_manifest)

        status, payload, headers = self.request(f"/api/v3/sessions/{SESSION_ID}")
        changed_sha256 = hashlib.sha256(changed_manifest).hexdigest()
        self.assertEqual(status, 200)
        self.assertEqual(payload, changed_manifest)
        self.assertEqual(headers["ETag"], f'"{changed_sha256}"')
        status, payload, headers = self.request(
            artifact_url,
            headers={"Range": "bytes=999-"},
        )
        self.assertEqual(status, 409)
        self.assertIsNone(headers["Content-Range"])
        error = json.loads(payload)["error"]
        self.assertEqual(error["code"], "session_not_verified")
        self.assertEqual(error["details"]["reason"], "stale")

    def test_legacy_v1_with_nonzero_drop_is_unusable_even_if_digest_was_cached(self) -> None:
        self.manifest["integrity"]["dropped_frames"] = 1
        self.manifest["integrity"]["drop_events"] = [
            {
                "start_frame": 1,
                "end_frame": 2,
                "at_time_seconds": 0.5,
                "reason": "write_backpressure",
                "dropped": 1,
            }
        ]
        self.manifest_bytes = json.dumps(
            self.manifest,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
        (self.session_root / "manifest.json").write_bytes(self.manifest_bytes)
        self.manifest_sha256 = hashlib.sha256(self.manifest_bytes).hexdigest()
        self.verdicts[SESSION_ID] = self.manifest_sha256

        artifact_url = f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"
        status, payload, _ = self.request(artifact_url)
        self.assertEqual(status, 409)
        error = json.loads(payload)["error"]
        self.assertEqual(error["code"], "session_not_verified")
        self.assertEqual(error["details"]["reason"], "unusable")

    def test_manifest_artifact_paths_fail_closed_before_filesystem_escape(self) -> None:
        artifact_url = f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"
        unsafe_paths = (
            "../outside.mp4",
            "/absolute/outside.mp4",
            "video/../outside.mp4",
            "video\\outside.mp4",
            "video//outside.mp4",
            "manifest.json",
            "video/capture.tmp",
        )
        for unsafe_path in unsafe_paths:
            with self.subTest(path=unsafe_path):
                self.manifest["video"]["segments"][0]["artifacts"]["left"]["path"] = unsafe_path
                self.write_manifest(self.manifest)

                status, payload, headers = self.request(artifact_url)

                self.assertEqual(status, 409)
                self.assertIsNone(headers["Content-Range"])
                self.assertEqual(json.loads(payload)["error"]["code"], "session_not_verified")

    def test_descriptor_size_mismatch_fails_closed(self) -> None:
        artifact_url = f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}"
        descriptor = self.manifest["video"]["segments"][0]["artifacts"]["left"]

        descriptor["bytes"] = len(ARTIFACT_BYTES) + 1
        self.write_manifest(self.manifest)
        status, payload, _ = self.request(artifact_url)
        self.assertEqual(status, 409)
        error = json.loads(payload)["error"]
        self.assertEqual(error["code"], "session_not_verified")
        self.assertEqual(error["details"]["reason"], "unusable")

    def test_non_regular_artifact_fails_closed_without_blocking(self) -> None:
        artifact_path = self.session_root / "video" / "left.mp4"
        artifact_path.unlink()
        os.mkfifo(artifact_path)

        status, payload, _ = self.request(f"/api/v3/sessions/{SESSION_ID}/artifacts/{ARTIFACT_ID}")

        self.assertEqual(status, 409)
        self.assertEqual(json.loads(payload)["error"]["code"], "session_not_verified")


if __name__ == "__main__":
    unittest.main()
