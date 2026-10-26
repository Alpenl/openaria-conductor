from __future__ import annotations

import errno
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.recording.live_seal import LiveSealing, _Journal


class LiveCloseErrorsTest(unittest.TestCase):
    def root(self, directory):
        root = Path(directory)
        for name in ("frames.ndjson", "imu.ndjson"):
            (root / name).write_bytes(b"original recovery evidence\n")
        return root

    def cancelled(self, root):
        sealing = object.__new__(LiveSealing)
        sealing.root = root
        sealing.prepared = {}
        sealing.cancelled = threading.Event()
        sealing.cancelled.set()
        sealing.finished = threading.Event()
        return sealing

    def test_real_enospc_on_close_releases_both_journals(self):
        with tempfile.TemporaryDirectory() as directory, open("/dev/full", "wb") as full:
            root = self.root(directory)
            opened = []

            def journal(path):
                result = _Journal(path)
                opened.append(result)
                if path.name == "frames.ndjson":
                    result.output.close()
                    result.output = full
                    result.output.write(b"pending buffered bytes")
                return result

            try:
                with (
                    patch("rp_ylx.recording.live_seal._Journal", side_effect=journal),
                    self.assertRaises(OSError) as raised,
                ):
                    self.cancelled(root)._compact()
                self.assertEqual(raised.exception.errno, errno.ENOSPC)
                self.assertEqual(len(opened), 2)
                self.assertTrue(all(j.source.closed and j.output.closed for j in opened))
                for name in ("frames.ndjson", "imu.ndjson"):
                    self.assertEqual((root / name).read_bytes(), b"original recovery evidence\n")
            finally:
                for item in opened:
                    item.source.close()
                    item.output.close()

    def test_source_close_error_or_cancellation_still_closes_output(self):
        for error in (OSError("source close failed"), KeyboardInterrupt("cancel close")):
            with (
                self.subTest(error=type(error).__name__),
                tempfile.TemporaryDirectory() as directory,
            ):
                journal = _Journal(self.root(directory) / "frames.ndjson")
                original = journal.source.close

                def failing_close(original=original, error=error):
                    original()
                    raise error

                try:
                    with (
                        patch.object(journal.source, "close", side_effect=failing_close),
                        self.assertRaises(type(error)),
                    ):
                        journal.close()
                    self.assertTrue(journal.source.closed)
                    self.assertTrue(journal.output.closed)
                    journal.close()
                finally:
                    original()
                    journal.output.close()

    def test_second_journal_initialization_failure_closes_first(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.root(directory)
            collision = root / "imu.ndjson.zst.tmp"
            collision.write_bytes(b"previous failed compaction")
            opened = []

            def journal(path):
                result = _Journal(path)
                opened.append(result)
                return result

            with (
                patch("rp_ylx.recording.live_seal._Journal", side_effect=journal),
                self.assertRaises(FileExistsError),
            ):
                self.cancelled(root)._compact()
            self.assertEqual(len(opened), 1)
            self.assertTrue(opened[0].source.closed and opened[0].output.closed)
            self.assertEqual(collision.read_bytes(), b"previous failed compaction")
