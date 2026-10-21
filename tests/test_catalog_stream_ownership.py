import errno
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.recording.catalog import SessionCatalog


class CatalogStreamOwnershipTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.raw = b'{"synthetic":true}'
        (self.root / "manifest.json").write_bytes(self.raw)

    def exercise(self, phase):
        failures = (OSError(errno.ENOMEM, "stream allocation"), MemoryError(), KeyboardInterrupt())
        for failure in failures:
            with self.subTest(phase=phase, failure=type(failure).__name__):
                self.exercise_failure(phase, failure)

    def exercise_failure(self, phase, failure):
        original = os.fdopen
        opened = []
        streams = []

        class Stream:
            def __init__(self, raw):
                self.raw = raw

            def __enter__(self):
                if phase == "enter":
                    raise failure
                return self

            def __exit__(self, *args):
                self.raw.close()
                if phase == "exit":
                    raise failure

            def fileno(self):
                return self.raw.fileno()

            def read(self, count):
                if phase == "read":
                    raise failure
                return self.raw.read(count)

        def fdopen(fd, *args, **kwargs):
            opened.append(fd)
            if phase == "allocate":
                raise failure
            stream = original(fd, *args, **kwargs)
            streams.append(stream)
            return Stream(stream)

        before = len(os.listdir("/proc/self/fd"))
        with (
            patch("rp_ylx.recording.catalog.os.fdopen", side_effect=fdopen),
            self.assertRaises(type(failure)) as caught,
        ):
            SessionCatalog.signature(self.root)
        self.assertIs(caught.exception, failure)
        # Retain both the exception traceback and stream wrappers while
        # checking ownership. Garbage collection must not be required.
        leaked = []
        for fd in opened:
            try:
                os.fstat(fd)
            except OSError as error:
                self.assertEqual(error.errno, errno.EBADF)
            else:
                leaked.append(fd)
        for stream in streams:
            stream.close()
        for fd in leaked:
            try:
                os.close(fd)
            except OSError as error:
                self.assertEqual(error.errno, errno.EBADF)
        self.assertEqual(leaked, [], f"unclosed manifest handles: {leaked}")
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)
        self.assertEqual((self.root / "manifest.json").read_bytes(), self.raw)

    def test_stream_allocation_failure_releases_both_descriptors(self):
        self.exercise("allocate")

    def test_stream_entry_failure_releases_both_descriptors(self):
        self.exercise("enter")

    def test_stream_read_failure_releases_both_descriptors(self):
        self.exercise("read")

    def test_stream_close_failure_releases_both_descriptors(self):
        self.exercise("exit")

    def test_successful_signatures_preserve_digest_and_release_descriptors(self):
        before = len(os.listdir("/proc/self/fd"))
        expected = hashlib.sha256(self.raw).hexdigest()
        for _ in range(50):
            self.assertEqual(json.loads(SessionCatalog.signature(self.root))[-1], expected)
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)
