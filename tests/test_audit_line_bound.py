from __future__ import annotations

import io
import tempfile
import threading
import unittest
from pathlib import Path

from rp_ylx.recording.audit import _journal_lines

LIMIT = 1_048_576


class AuditLineBoundTest(unittest.TestCase):
    def test_oversize_line_is_rejected_before_consuming_large_file(self):
        for newline in (False, True):
            with self.subTest(newline=newline), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "frames.ndjson"
                with path.open("wb") as output:
                    output.truncate(32 * LIMIT)
                    if newline:
                        output.seek(0, 2)
                        output.write(b"\n")
                with path.open("rb") as source:
                    with self.assertRaisesRegex(ValueError, "record exceeds limit"):
                        next(_journal_lines(source, None))
                    self.assertLessEqual(source.tell(), LIMIT + 1)

    def test_exact_limit_and_following_record_are_preserved(self):
        record = b"x" * (LIMIT - 1) + b"\n"
        source = io.BytesIO(record + b"next\n")
        self.assertEqual(list(_journal_lines(source, None)), [record, b"next\n"])

    def test_live_fragmented_exact_limit_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "frames.ndjson"
            prefix = b"x" * (LIMIT // 2)
            suffix = b"y" * (LIMIT - len(prefix) - 1) + b"\n"
            path.write_bytes(prefix)
            finished = threading.Event()
            waiting = threading.Event()
            original_wait = finished.wait

            def wait(timeout):
                waiting.set()
                return original_wait(timeout)

            finished.wait = wait
            errors = []
            result = []

            def read():
                try:
                    with path.open("rb") as source:
                        result.extend(_journal_lines(source, finished))
                except BaseException as error:
                    errors.append(error)

            thread = threading.Thread(target=read)
            thread.start()
            try:
                self.assertTrue(waiting.wait(2))
                with path.open("ab") as output:
                    output.write(suffix + b"next\n")
                finished.set()
                thread.join(2)
                self.assertFalse(thread.is_alive())
                self.assertEqual(errors, [])
                self.assertEqual(result, [prefix + suffix, b"next\n"])
            finally:
                finished.set()
                thread.join(2)

    def test_exact_limit_without_newline_waits_then_rejects_next_byte(self):
        class AppendOnWait:
            def __init__(self, source):
                self.source = source
                self.waits = 0

            def is_set(self):
                return False

            def wait(self, timeout):
                self.waits += 1
                position = self.source.tell()
                self.source.seek(0, 2)
                self.source.write(b"\n")
                self.source.seek(position)

        source = io.BytesIO(b"x" * LIMIT)
        finished = AppendOnWait(source)
        with self.assertRaisesRegex(ValueError, "record exceeds limit"):
            next(_journal_lines(source, finished))
        self.assertEqual(finished.waits, 1)
        self.assertEqual(source.tell(), LIMIT + 1)

    def test_final_incomplete_record_is_not_silently_discarded(self):
        for payload in (b"x", b"x" * LIMIT):
            with (
                self.subTest(length=len(payload)),
                self.assertRaisesRegex(ValueError, "truncated record"),
            ):
                list(_journal_lines(io.BytesIO(payload), None))
