import json
import threading
import unittest
from unittest.mock import Mock
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from rp_ylx.api import Principal, SecurityPolicy, create_gateway_server
from rp_ylx.daemon import LAB_OPERATIONS


class ReadinessGatewayTests(unittest.TestCase):
    def test_read_only_permission_validation_and_preflight(self):
        provider = Mock()
        value = {
            "schema": "ylx.capture-readiness.v1",
            "allowed": False,
            "code": "recovery_pending",
            "message": "正在收尾",
            "storage": {"recording": None, "system": None, "backing": None},
        }
        provider.capture_readiness.return_value = value
        policy = SecurityPolicy.customer(
            tokens={
                "viewer": Principal("viewer", permissions={"getCaptureReadiness": None}),
                "other": Principal("other", permissions={}),
            },
            allowed_origins={"http://device.test"},
            csrf_token="csrf",
        )
        server = create_gateway_server("127.0.0.1", 0, provider, security=policy)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        url = f"http://127.0.0.1:{server.server_address[1]}/api/v4/capture/readiness"

        def request(token="viewer"):
            return urlopen(Request(url, headers={"Authorization": "Bearer " + token}), timeout=3)

        with request() as response:
            self.assertEqual(json.load(response), value)
        for token in ("other", "missing"):
            with self.assertRaises(HTTPError) as caught:
                request(token)
            self.assertIn(caught.exception.code, (401, 403))
        provider.capture_readiness.return_value = {**value, "allowed": True}
        with self.assertRaises(HTTPError) as caught:
            request()
        self.assertEqual(caught.exception.code, 500)
        self.assertIn("getCaptureReadiness", LAB_OPERATIONS)
        with urlopen(
            Request(
                url,
                method="OPTIONS",
                headers={"Origin": "http://device.test", "Access-Control-Request-Method": "GET"},
            ),
            timeout=3,
        ) as response:
            self.assertNotIn("POST", response.headers["Access-Control-Allow-Methods"])
