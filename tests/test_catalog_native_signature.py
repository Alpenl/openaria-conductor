import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from rp_ylx.manifest_limits import MAX_MANIFEST_BYTES
from rp_ylx.native import create_native_session_store
from rp_ylx.recording.catalog import SessionCatalog


class CatalogNativeSignatureTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.manifest = self.root / "manifest.json"
        self.manifest.write_bytes(b"original")
        self.native = create_native_session_store()

    def signature(self, maximum=MAX_MANIFEST_BYTES):
        return self.native.catalog_manifest_signature(os.fsencode(self.root), maximum)

    def test_native_signature_matches_python_bytes_across_read_boundaries(self):
        for size in (0, 1, 4097, 1024**2 - 1, 1024**2, 1024**2 + 1):
            with self.subTest(size=size):
                self.manifest.write_bytes(b"z" * size)
                self.assertEqual(self.signature(), SessionCatalog._signature_python(self.root))
                self.assertEqual(
                    json.loads(self.signature())[-1], hashlib.sha256(b"z" * size).hexdigest()
                )

    def test_native_signature_handles_non_utf8_directory_names(self):
        directory = self.root / os.fsdecode(b"session-\xff")
        directory.mkdir()
        (directory / "manifest.json").write_bytes(b"payload")
        self.assertEqual(
            self.native.catalog_manifest_signature(os.fsencode(directory), 100),
            SessionCatalog._signature_python(directory),
        )

    def test_native_signature_rehashes_same_size_edit_with_restored_mtime(self):
        original = self.signature()
        metadata = self.manifest.stat()
        self.manifest.write_bytes(b"modified")
        os.utime(self.manifest, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
        current = self.signature()
        self.assertEqual(json.loads(original)[7], json.loads(current)[7])
        self.assertNotEqual(json.loads(original)[-1], json.loads(current)[-1])
        self.assertEqual(current, SessionCatalog._signature_python(self.root))

    def test_native_rejects_symbolic_and_hard_links_and_special_files(self):
        original = self.root / "original"
        self.manifest.rename(original)
        self.manifest.symlink_to(original)
        with self.assertRaises(OSError):
            self.signature()
        self.manifest.unlink()
        os.link(original, self.manifest)
        with self.assertRaises(OSError):
            self.signature()
        self.manifest.unlink()
        os.mkfifo(self.manifest)
        with self.assertRaises(OSError):
            self.signature()
        self.manifest.unlink()
        self.manifest.mkdir()
        with self.assertRaises(OSError):
            self.signature()
        linked = self.root / "linked-root"
        linked.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(OSError):
            self.native.catalog_manifest_signature(os.fsencode(linked), 100)

    def test_native_size_limit_is_checked_before_reading(self):
        with self.manifest.open("wb") as stream:
            stream.truncate(MAX_MANIFEST_BYTES + 1)
        with self.assertRaises(OSError):
            self.signature()

    def test_native_errors_and_success_do_not_retain_descriptors(self):
        before = len(os.listdir("/proc/self/fd"))
        errors = []
        for _ in range(100):
            self.signature()
            try:
                self.signature(maximum=1)
            except OSError as error:
                errors.append(error)
        self.assertEqual(len(errors), 100)
        # Keep exception tracebacks alive; cleanup must not depend on GC.
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)

    def test_public_signature_uses_native_path_without_python_stream_setup(self):
        expected = self.signature()
        with (
            patch(
                "rp_ylx.recording.catalog.native_session_store_or_none", return_value=self.native
            ),
            patch(
                "rp_ylx.recording.catalog.os.fdopen", side_effect=AssertionError("Python I/O used")
            ),
        ):
            self.assertEqual(SessionCatalog.signature(self.root), expected)


class CatalogSignatureDispatchTest(unittest.TestCase):
    def test_missing_optional_native_method_keeps_python_fallback(self):
        for backend in (None, SimpleNamespace()):
            with (
                self.subTest(backend=backend),
                patch(
                    "rp_ylx.recording.catalog.native_session_store_or_none", return_value=backend
                ),
                patch.object(
                    SessionCatalog, "_signature_python", return_value="fallback"
                ) as fallback,
            ):
                self.assertEqual(SessionCatalog.signature(Path("/synthetic")), "fallback")
                fallback.assert_called_once_with(Path("/synthetic"))

    def test_native_io_error_does_not_retry_via_python(self):
        failure = OSError("changed while reading")

        def signature(*unused):
            raise failure

        with (
            patch(
                "rp_ylx.recording.catalog.native_session_store_or_none",
                return_value=SimpleNamespace(catalog_manifest_signature=signature),
            ),
            patch.object(
                SessionCatalog, "_signature_python", side_effect=AssertionError("must not retry")
            ),
            self.assertRaises(OSError) as caught,
        ):
            SessionCatalog.signature(Path("/synthetic"))
        self.assertIs(caught.exception, failure)

    def test_native_cancellation_and_allocation_failure_propagate(self):
        for error in (KeyboardInterrupt(), MemoryError()):

            def signature(*unused, failure=error):
                raise failure

            with (
                self.subTest(error=type(error).__name__),
                patch(
                    "rp_ylx.recording.catalog.native_session_store_or_none",
                    return_value=SimpleNamespace(catalog_manifest_signature=signature),
                ),
                self.assertRaises(type(error)) as caught,
            ):
                SessionCatalog.signature(Path("/synthetic"))
            self.assertIs(caught.exception, error)

    def test_invalid_native_return_is_rejected(self):
        with (
            patch(
                "rp_ylx.recording.catalog.native_session_store_or_none",
                return_value=SimpleNamespace(catalog_manifest_signature=lambda *unused: None),
            ),
            self.assertRaises(OSError),
        ):
            SessionCatalog.signature(Path("/synthetic"))
