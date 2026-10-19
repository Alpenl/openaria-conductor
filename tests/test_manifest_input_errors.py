import hashlib
import json
import os
import unittest
from contextlib import suppress
from unittest.mock import patch

from rp_ylx.api import downloads
from tests import test_session_download as fixtures


class Cancelled(BaseException):
    pass


class ManifestInputTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.DirectorySessionDownloadHttpTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.errors = []
        self.fixture.server.handle_error = lambda *args: self.errors.append("unhandled")
        self.path = self.fixture.session_root / "manifest.json"

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
        self.fixture.verdicts[fixtures.SESSION_ID] = hashlib.sha256(payload).hexdigest()

    def test_invalid_json_depth_is_rejected_without_descriptor_leak(self):
        self.write_payload(b'{"x":' + b"[" * 10000 + b"0" + b"]" * 10000 + b"}")
        before = self.descriptors()
        try:
            for version in ("v2", "v3", "v4"):
                with self.subTest(version=version):
                    with self.assertRaises(downloads.ArtifactAccessError) as error:
                        self.fixture.store.open_manifest(fixtures.SESSION_ID, version)
                    self.assertEqual(error.exception.code, "not_found")
                    self.assertEqual(self.descriptors(), before)
        finally:
            self.close_leaked(before)

    def test_invalid_schema_types_return_structured_http_error(self):
        before = self.descriptors()
        try:
            for schema in ([], {}, ["ylx.device-session.v2"]):
                manifest = {**self.fixture.manifest, "schema": schema}
                self.fixture.write_manifest(manifest)
                for version in ("v3", "v4"):
                    with self.subTest(schema=schema, version=version):
                        status, body, _ = self.fixture.request(
                            f"/api/{version}/sessions/{fixtures.SESSION_ID}"
                        )
                        self.assertEqual(status, 404)
                        self.assertEqual(json.loads(body)["error"]["code"], "not_found")
            self.assertEqual(self.errors, [])
        finally:
            self.close_leaked(before)
        self.write_payload(self.fixture.manifest_bytes)
        status, body, _ = self.fixture.request(f"/api/v4/sessions/{fixtures.SESSION_ID}")
        self.assertEqual((status, body), (200, self.fixture.manifest_bytes))

    def test_producer_validator_rejects_schema_types(self):
        for schema in ([], {}, ["ylx.device-session.v2"]):
            with self.subTest(schema=schema):
                with self.assertRaises(downloads.ArtifactAccessError) as error:
                    downloads.validate_device_session_manifest(
                        {**self.fixture.manifest, "schema": schema}
                    )
                self.assertEqual(error.exception.code, "not_verified")

    def test_validation_recursion_is_not_an_unhandled_exception(self):
        with patch.object(
            downloads,
            "_validate_device_session_manifest",
            side_effect=RecursionError("nested metadata"),
        ):
            with self.assertRaises(downloads.ArtifactAccessError) as error:
                self.fixture.store.open_manifest(fixtures.SESSION_ID, "v4")
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
                            self.fixture.store.open_manifest(fixtures.SESSION_ID, "v4")
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
                downloads._open_absolute_directory(self.fixture.session_root)
            self.assertEqual(self.descriptors(), before)
            (self.fixture.session_root / "cancel.bin").write_bytes(b"test")
            parent = os.open(self.fixture.session_root, os.O_RDONLY | os.O_DIRECTORY)
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
