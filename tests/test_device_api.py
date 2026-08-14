from __future__ import annotations

import io
import json
import threading
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from PIL import Image

from rp_ylx.api import ApiError, MockDevice, create_server


class MockDeviceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.device = MockDevice(session_id_factory=lambda: "0198c9a8-7a3c-7000-8000-000000000002")

    def test_start_is_idempotent(self) -> None:
        command = {"request_id": "start-1", "expected_revision": 0}
        first = self.device.start(command)
        second = self.device.start(command)
        self.assertEqual(first, second)
        self.assertEqual(self.device.revision, 1)
        self.assertEqual(first.body["recording"]["state"], "starting")

    def test_second_start_is_busy_and_hardware_fault_marks_failure(self) -> None:
        self.device.start({"request_id": "start-1"})
        with self.assertRaises(ApiError) as busy:
            self.device.start({"request_id": "start-2"})
        self.assertEqual(busy.exception.code, "capture_busy")
        self.device.set_fault("hardware_unavailable", "相机断开")
        self.assertEqual(self.device.status()["recording"]["state"], "failed")

    def test_same_request_id_cannot_change_body(self) -> None:
        self.device.start({"request_id": "start-1"})
        with self.assertRaises(ApiError) as raised:
            self.device.start({"request_id": "start-1", "expected_revision": 1})
        self.assertEqual(raised.exception.code, "invalid_request")

    def test_stale_revision_and_fault_are_explicit(self) -> None:
        self.device.set_fault("hardware_unavailable", "相机未连接")
        with self.assertRaises(ApiError) as stale:
            self.device.start({"request_id": "start-1", "expected_revision": 0})
        self.assertEqual(stale.exception.code, "stale_revision")
        with self.assertRaises(ApiError) as unavailable:
            self.device.start({"request_id": "start-2", "expected_revision": 1})
        self.assertEqual(unavailable.exception.code, "hardware_unavailable")

    def test_reading_or_disconnect_does_not_change_recording(self) -> None:
        self.device.start({"request_id": "start-1"})
        self.device.complete_start()
        revision = self.device.revision
        self.device.status()
        self.device.device()
        self.device.sessions()
        self.device.events_after(-1)
        self.assertEqual(self.device.revision, revision)
        self.assertEqual(self.device.status()["recording"]["state"], "recording")

    def test_stop_produces_sealed_session_summary(self) -> None:
        self.device.start({"request_id": "start-1"})
        self.device.complete_start()
        result = self.device.stop({"request_id": "stop-1", "reason": "user"})
        self.assertEqual(result.body["recording"]["state"], "stopping")
        self.device.complete_stop()
        self.assertEqual(self.device.sessions()["sessions"][0]["state"], "sealed")

    def test_preview_pair_shares_source_sequence_and_time(self) -> None:
        left, left_sequence, left_time = self.device.preview("left")
        _, repeated_sequence, repeated_time = self.device.preview("left")
        right, right_sequence, right_time = self.device.preview("right", sequence=left_sequence)

        self.assertEqual((left_sequence, left_time), (repeated_sequence, repeated_time))
        self.assertEqual((left_sequence, left_time), (right_sequence, right_time))
        next_sequence = self.device.publish_preview_pair(left, right)
        _, next_left_sequence, next_left_time = self.device.preview("left")
        self.assertEqual(next_left_sequence, left_sequence + 1)
        self.assertEqual(next_left_sequence, next_sequence)
        self.assertGreater(next_left_time, left_time)

    def test_preview_pair_can_preserve_camera_source_sequence(self) -> None:
        device = MockDevice()
        sequence = device.publish_preview_pair(
            b"left-camera", b"right-camera", source_sequence=4, capture_monotonic_ns=10
        )
        self.assertEqual(sequence, 4)
        _, observed_sequence, observed_time = device.preview("left")
        self.assertEqual((observed_sequence, observed_time), (4, 10))
        with self.assertRaises(ApiError) as placeholder:
            device.preview("left", sequence=0)
        self.assertEqual(placeholder.exception.code, "preview_frame_expired")

        sequence = device.publish_preview_pair(
            b"left-next", b"right-next", source_sequence=8, capture_monotonic_ns=20
        )
        self.assertEqual(sequence, 8)
        with self.assertRaises(ValueError):
            device.publish_preview_pair(
                b"left-old", b"right-old", source_sequence=8, capture_monotonic_ns=30
            )

    def test_expired_preview_pair_has_stable_error(self) -> None:
        device = MockDevice(preview_cache_capacity=2)
        left, old_sequence, _ = device.preview("left")
        right, _, _ = device.preview("right", sequence=old_sequence)
        device.publish_preview_pair(left, right)
        device.publish_preview_pair(left, right)

        with self.assertRaises(ApiError) as expired:
            device.preview("right", sequence=old_sequence)
        self.assertEqual(expired.exception.code, "preview_frame_expired")
        self.assertEqual(expired.exception.status, 410)
        self.assertTrue(expired.exception.retryable)


class HttpApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.device = MockDevice(session_id_factory=lambda: "0198c9a8-7a3c-7000-8000-000000000003")
        cls.server = create_server(
            "127.0.0.1",
            0,
            cls.device,
            auto_transition=False,
            allowed_origins=["http://127.0.0.1:4173"],
        )
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}/api/v0"

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def request(self, path: str, body: object | None = None) -> tuple[int, bytes, object]:
        data = None if body is None else json.dumps(body).encode()
        request = Request(
            self.base + path,
            data=data,
            headers={} if data is None else {"Content-Type": "application/json"},
        )
        try:
            with urlopen(request, timeout=2) as response:
                payload = response.read()
                return response.status, payload, response.headers
        except HTTPError as error:
            return error.code, error.read(), error.headers

    def test_device_status_and_preview(self) -> None:
        status, payload, _ = self.request("/device")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload)["api_version"], "v0")
        status, left_payload, headers = self.request("/preview/frame?eye=left")
        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], "image/jpeg")
        self.assertIsNotNone(headers["X-YLX-Sequence"])
        status, _, repeated_headers = self.request("/preview/frame?eye=left")
        self.assertEqual(status, 200)
        self.assertEqual(headers["X-YLX-Sequence"], repeated_headers["X-YLX-Sequence"])
        sequence = headers["X-YLX-Sequence"]
        status, right_payload, right_headers = self.request(
            f"/preview/frame?eye=right&sequence={sequence}"
        )
        self.assertEqual(status, 200)
        self.assertEqual(headers["X-YLX-Sequence"], right_headers["X-YLX-Sequence"])
        self.assertEqual(headers["X-YLX-Monotonic-Ns"], right_headers["X-YLX-Monotonic-Ns"])

        with Image.open(io.BytesIO(left_payload)) as left_image:
            left_image.load()
            self.assertEqual(left_image.format, "JPEG")
            left_luma = left_image.convert("L").getpixel((0, 0))
        with Image.open(io.BytesIO(right_payload)) as right_image:
            right_image.load()
            self.assertEqual(right_image.format, "JPEG")
            right_luma = right_image.convert("L").getpixel((0, 0))
        self.assertNotEqual(left_luma, right_luma)

        for _ in range(4):
            self.device.publish_preview_pair(left_payload, right_payload)
        status, payload, _ = self.request(f"/preview/frame?eye=right&sequence={sequence}")
        self.assertEqual(status, 410)
        self.assertEqual(json.loads(payload)["error"]["code"], "preview_frame_expired")

    def test_command_and_sse_snapshot(self) -> None:
        current = self.device.revision
        status, payload, _ = self.request(
            "/recordings/start",
            {"request_id": f"http-start-{current}", "expected_revision": current},
        )
        self.assertEqual(status, 202)
        self.assertTrue(json.loads(payload)["accepted"])
        status, events, _ = self.request("/events?once=true")
        self.assertEqual(status, 200)
        self.assertIn(b"event: snapshot", events)
        self.assertIn(b"data: {", events)

    def test_invalid_preview_eye_has_stable_error(self) -> None:
        status, payload, _ = self.request("/preview/frame?eye=middle")
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_request")
        status, payload, _ = self.request("/preview/frame?eye=left&sequence=bad")
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_request")

    def test_openapi_declares_all_public_paths(self) -> None:
        path = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "rp_ylx"
            / "api"
            / "device-api-v0.openapi.json"
        )

        def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError(f"OpenAPI 存在重复键：{key}")
                result[key] = value
            return result

        specification = json.loads(
            path.read_text(encoding="utf-8"), object_pairs_hook=reject_duplicate_keys
        )
        self.assertEqual(specification["openapi"], "3.1.0")
        self.assertEqual(
            set(specification["paths"]),
            {
                "/device",
                "/status",
                "/sessions",
                "/recordings/start",
                "/recordings/stop",
                "/events",
                "/preview/frame",
            },
        )
        preview_operation = specification["paths"]["/preview/frame"]["get"]
        self.assertEqual(
            {parameter["name"] for parameter in preview_operation["parameters"]},
            {"eye", "sequence"},
        )
        self.assertIn("410", preview_operation["responses"])
        preview = preview_operation["responses"]["200"]
        self.assertEqual(set(preview["headers"]), {"X-YLX-Sequence", "X-YLX-Monotonic-Ns"})
        events = specification["paths"]["/events"]["get"]["responses"]["200"]
        self.assertEqual(set(events["x-sse-events"]), {"snapshot", "status", "diagnostic"})

    def test_cors_allows_only_configured_origin_and_preflight(self) -> None:
        allowed = Request(self.base + "/status", headers={"Origin": "http://127.0.0.1:4173"})
        with urlopen(allowed, timeout=2) as response:
            self.assertEqual(
                response.headers["Access-Control-Allow-Origin"], "http://127.0.0.1:4173"
            )

        denied = Request(self.base + "/status", headers={"Origin": "https://example.invalid"})
        with urlopen(denied, timeout=2) as response:
            self.assertIsNone(response.headers["Access-Control-Allow-Origin"])

        preflight = Request(
            self.base + "/recordings/start",
            method="OPTIONS",
            headers={
                "Origin": "http://127.0.0.1:4173",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "content-type",
            },
        )
        with urlopen(preflight, timeout=2) as response:
            self.assertEqual(response.status, 204)
            self.assertEqual(response.headers["Access-Control-Allow-Methods"], "POST, OPTIONS")

    def test_wildcard_cors_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            create_server("127.0.0.1", 0, allowed_origins=["*"])


if __name__ == "__main__":
    unittest.main()
