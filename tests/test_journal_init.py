"""Allocation failures must not acquire journal files or leave a retry collision."""

import gc
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import rp_ylx.recording.live_seal as live


class JournalInitTests(unittest.TestCase):
    def check_allocation(self, kind, call=1, *, pair=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original = b"original recovery evidence\n"
            for name in ("frames.ndjson", "imu.ndjson"):
                (root / name).write_bytes(original)
            target = live.zstandard if kind == "compressor" else live.hashlib
            attribute = "ZstdCompressor" if kind == "compressor" else "sha256"
            factory = getattr(target, attribute)
            calls = 0
            held = None
            injected = MemoryError("injected allocation failure")
            baseline = len(os.listdir("/proc/self/fd"))

            def allocate(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == call:
                    raise injected
                return factory(*args, **kwargs)

            try:
                with patch.object(target, attribute, side_effect=allocate):
                    try:
                        if pair:
                            sealing = object.__new__(live.LiveSealing)
                            sealing.root = root
                            sealing.cancelled = threading.Event()
                            sealing.cancelled.set()
                            sealing._compact()
                        else:
                            live._Journal(root / "frames.ndjson")
                    except MemoryError as error:
                        held = error
                    else:
                        self.fail("allocation injection did not fire")
                # Keep the traceback alive, just as a Future retains its exception.
                self.assertIs(held, injected)
                self.assertIsNotNone(held.__traceback__)
                self.assertEqual(len(os.listdir("/proc/self/fd")), baseline)
                failed_name = "imu.ndjson" if pair else "frames.ndjson"
                failed_path = root / failed_name
                self.assertFalse(failed_path.with_suffix(".ndjson.zst.tmp").exists())
                for name in ("frames.ndjson", "imu.ndjson"):
                    self.assertEqual((root / name).read_bytes(), original)
                # Retry this journal immediately, without GC or deleting a stale temp.
                retry = live._Journal(failed_path)
                try:
                    artifact = retry.finish()
                    self.assertEqual(artifact.compression["uncompressed_bytes"], len(original))
                    self.assertEqual(failed_path.read_bytes(), original)
                finally:
                    retry.close()
                self.assertEqual(len(os.listdir("/proc/self/fd")), baseline)
            finally:
                # Release old-code reproduction's retained constructor frame.
                injected.__traceback__ = None
                if held is not None:
                    held.__traceback__ = None
                gc.collect()

    def test_compressor_allocation_failure_acquires_no_files(self):
        self.check_allocation("compressor")

    def test_digest_allocation_failure_acquires_no_files(self):
        for call in (1, 2):
            with self.subTest(call=call):
                self.check_allocation("digest", call)

    def test_second_journal_allocation_failure_closes_first_and_allows_retry(self):
        self.check_allocation("compressor", 2, pair=True)
