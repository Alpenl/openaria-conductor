from __future__ import annotations

import http.client
import json
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from rp_ylx.api.gateway import ProviderError, create_gateway_server
from rp_ylx.api.preview import LatestPreviewBuffer, PreviewResponse
from rp_ylx.api.security import Principal, SecurityPolicy

JPEG_FRAME = b"\xff\xd8latest-preview\xff\xd9"
FPS_FIVE_FRAME = b"\xff\xd8fps=5;accept=image/jpeg\xff\xd9"
STREAM_INITIAL = b"\xff\xd8stream-initial\xff\xd9"
STREAM_LATEST = b"\xff\xd8stream-latest\xff\xd9"
FIRST_PART = (
    b"--ylx-preview\r\nContent-Type: image/jpeg\r\nContent-Length: 9\r\n\r\n"
    b"\xff\xd8first\xff\xd9\r\n"
)
SECOND_PART = (
    b"--ylx-preview\r\nContent-Type: image/jpeg\r\nContent-Length: 10\r\n\r\n"
    b"\xff\xd8second\xff\xd9\r\n"
)
STREAM_INITIAL_PART = (
    b"--ylx-preview\r\nContent-Type: image/jpeg\r\nContent-Length: 18\r\n\r\n"
    b"\xff\xd8stream-initial\xff\xd9\r\n"
)
STREAM_LATEST_PART = (
    b"--ylx-preview\r\nContent-Type: image/jpeg\r\nContent-Length: 17\r\n\r\n"
    b"\xff\xd8stream-latest\xff\xd9\r\n"
)


class _PreviewProvider:
    def __init__(self) -> None:
        self.error: ProviderError | None = None
        self.buffer: LatestPreviewBuffer | None = None
        self.response: PreviewResponse | None = None
        self.last_response: PreviewResponse | None = None

    def latest_preview(self, *, fps: int | None, accept: str) -> PreviewResponse:
        if self.error is not None:
            raise self.error
        if self.response is not None:
            result = self.response
        elif self.buffer is not None:
            result = self.buffer.latest_preview(fps=fps, accept=accept)
        else:
            payload = FPS_FIVE_FRAME if fps == 5 and accept == "image/jpeg" else JPEG_FRAME
            result = PreviewResponse("image/jpeg", payload, len(payload))
        self.last_response = result
        return result


class GatewayPreviewHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.provider = _PreviewProvider()
        reader = Principal("reader", permissions={"getPreview": None})
        denied = Principal("denied", permissions={})
        self.server = create_gateway_server(
            "127.0.0.1",
            0,
            self.provider,
            security=SecurityPolicy.customer(
                tokens={"reader-token": reader, "denied-token": denied},
                allowed_origins={"http://127.0.0.1:4173"},
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
        headers: dict[str, str] | None = None,
        token: str | None = "reader-token",
    ) -> tuple[int, bytes, object]:
        request_headers = {} if token is None else {"Authorization": f"Bearer {token}"}
        request_headers.update(headers or {})
        request = Request(self.base + path, headers=request_headers)
        try:
            with urlopen(request, timeout=2) as response:
                return response.status, response.read(), response.headers
        except HTTPError as error:
            return error.code, error.read(), error.headers

    def open_stream(self, path: str) -> tuple[http.client.HTTPConnection, http.client.HTTPResponse]:
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=3)
        connection.request(
            "GET",
            path,
            headers={
                "Authorization": "Bearer reader-token",
                "Accept": "multipart/x-mixed-replace",
            },
        )
        return connection, connection.getresponse()

    def test_each_version_returns_the_exact_latest_jpeg_and_closes(self) -> None:
        for version in ("v2", "v3"):
            with self.subTest(version=version):
                status, payload, headers = self.request(
                    f"/api/{version}/preview", headers={"Accept": "image/jpeg"}
                )

                self.assertEqual(status, 200)
                self.assertEqual(payload, JPEG_FRAME)
                self.assertEqual(headers["Content-Type"], "image/jpeg")
                self.assertEqual(headers["Content-Length"], str(len(JPEG_FRAME)))
                self.assertEqual(headers["Cache-Control"], "no-store")

    def test_fps_and_accept_are_normalized_and_invalid_queries_are_rejected(self) -> None:
        status, payload, _ = self.request(
            "/api/v3/preview?fps=5", headers={"Accept": "application/json"}
        )
        self.assertEqual((status, payload), (200, FPS_FIVE_FRAME))

        for query in (
            "fps=0",
            "fps=-1",
            "fps=abc",
            "fps=",
            "fps=" + "9" * 5000,
            "fps=1&fps=2",
            "fps=%EF%BC%91",
            "eye=left",
        ):
            with self.subTest(query=query):
                status, payload, headers = self.request(f"/api/v3/preview?{query}")
                self.assertEqual(status, 400)
                self.assertEqual(headers["Content-Type"], "application/problem+json")
                self.assertEqual(json.loads(payload)["error"]["code"], "invalid_request")

    def test_auth_authorization_and_origin_are_enforced_before_preview(self) -> None:
        cases = (
            (None, {}, 401, "unauthorized"),
            ("denied-token", {}, 403, "forbidden"),
            (
                "reader-token",
                {"Origin": "https://example.invalid"},
                403,
                "origin_forbidden",
            ),
        )
        for token, headers, expected_status, expected_code in cases:
            with self.subTest(expected_code=expected_code):
                status, payload, _ = self.request("/api/v3/preview", token=token, headers=headers)
                self.assertEqual(status, expected_status)
                self.assertEqual(json.loads(payload)["error"]["code"], expected_code)

    def test_provider_state_conflict_and_empty_frame_keep_the_declared_errors(self) -> None:
        self.provider.error = ProviderError(
            "preview_state_conflict",
            "当前设备状态不允许预览",
            status=409,
            retryable=True,
        )
        status, payload, headers = self.request("/api/v3/preview")
        self.assertEqual(status, 409)
        self.assertEqual(headers["Content-Type"], "application/problem+json")
        self.assertEqual(
            json.loads(payload)["error"],
            {
                "code": "preview_state_conflict",
                "message": "当前设备状态不允许预览",
                "request_id": json.loads(payload)["error"]["request_id"],
                "retryable": True,
            },
        )

        self.provider.error = None
        self.provider.buffer = LatestPreviewBuffer(stream_fps=5)
        status, payload, headers = self.request("/api/v2/preview")
        self.assertEqual(status, 503)
        self.assertEqual(headers["Content-Type"], "application/problem+json")
        self.assertEqual(json.loads(payload)["error"]["code"], "preview_unavailable")
        self.assertTrue(json.loads(payload)["error"]["retryable"])

    def test_finite_multipart_stream_has_exact_framing_and_connection_delimiting(self) -> None:
        self.provider.response = PreviewResponse(
            "multipart/x-mixed-replace; boundary=ylx-preview",
            iter((FIRST_PART, SECOND_PART)),
            None,
        )

        status, payload, headers = self.request(
            "/api/v2/preview?fps=3",
            headers={"Accept": "multipart/x-mixed-replace"},
        )

        self.assertEqual((status, payload), (200, FIRST_PART + SECOND_PART))
        self.assertEqual(
            headers["Content-Type"],
            "multipart/x-mixed-replace; boundary=ylx-preview",
        )
        self.assertEqual(headers["Connection"], "close")
        self.assertIsNone(headers["Content-Length"])
        self.assertEqual(headers["Cache-Control"], "no-store")

    def test_slow_http_consumer_cannot_block_publish_and_observes_only_latest(self) -> None:
        buffer = LatestPreviewBuffer(stream_fps=1)
        buffer.publish(STREAM_INITIAL)
        self.provider.buffer = buffer
        connection, response = self.open_stream("/api/v3/preview?fps=30")
        try:
            self.assertEqual(response.status, 200)
            self.assertEqual(
                response.headers["Content-Type"],
                "multipart/x-mixed-replace; boundary=ylx-preview",
            )
            self.assertEqual(
                response.read(len(STREAM_INITIAL_PART)),
                STREAM_INITIAL_PART,
            )

            completed = threading.Event()
            elapsed: list[float] = []

            def publish_burst() -> None:
                started = time.monotonic()
                buffer.publish(b"\xff\xd8discard-one\xff\xd9")
                buffer.publish(b"\xff\xd8discard-two\xff\xd9")
                buffer.publish(STREAM_LATEST)
                elapsed.append(time.monotonic() - started)
                completed.set()

            publisher = threading.Thread(target=publish_burst)
            publisher.start()
            self.assertTrue(completed.wait(0.25), "slow HTTP consumer blocked preview publish")
            publisher.join(timeout=1)
            self.assertLess(elapsed[0], 0.25)

            self.assertEqual(
                response.read(len(STREAM_LATEST_PART)),
                STREAM_LATEST_PART,
            )
        finally:
            body = None if self.provider.last_response is None else self.provider.last_response.body
            close = getattr(body, "close", None)
            if close is not None:
                close()
            response.close()
            connection.close()

    def test_preview_retains_the_original_owned_bytes_without_copying(self) -> None:
        buffer = LatestPreviewBuffer(stream_fps=15)
        payload = bytes(bytearray(STREAM_INITIAL))
        buffer.publish(payload)
        response = buffer.jpeg_response()
        self.assertIs(response.body, payload)

    def test_latest_preview_buffer_uses_native_owner_when_available(self) -> None:
        class _NativePreview:
            def __init__(self) -> None:
                self.latest: tuple[int, bytes] | None = None
                self.stream_fps: int | None = None
                self.woken = False

            def publish(self, jpeg: bytes) -> int:
                self.latest = (7, jpeg)
                return 7

            def clear(self) -> None:
                self.latest = None

            def jpeg(self) -> tuple[int, bytes]:
                if self.latest is None:
                    raise RuntimeError("preview_unavailable: empty")
                return self.latest

            def multipart_stream(self, fps: int | None = None) -> object:
                self.stream_fps = fps
                return iter((b"native-stream",))

            def wake_streams(self) -> None:
                self.woken = True

        native = _NativePreview()
        with patch("rp_ylx.api.preview.create_native_preview_buffer", return_value=native):
            buffer = LatestPreviewBuffer(stream_fps=15)
        payload = bytes(bytearray(STREAM_LATEST))

        self.assertEqual(buffer.publish(payload), 7)
        jpeg = buffer.jpeg_response()
        stream = buffer.multipart_response(30)
        buffer._wake_streams()

        self.assertIs(jpeg.body, payload)
        self.assertEqual(list(stream.body), [b"native-stream"])
        self.assertEqual(native.stream_fps, 15)
        self.assertTrue(native.woken)


if __name__ == "__main__":
    unittest.main()
