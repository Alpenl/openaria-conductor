import importlib
import json
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

from tests.test_native import _recording_plan


def transaction(root, count, append=0):
    extension = importlib.import_module("rp_ylx._native")
    (root / "video").mkdir()
    events = [
        {
            "event": "segment",
            "index": i,
            "start_frame": i * 150,
            "end_frame": (i + 1) * 150,
            "left": {"path": f"video/left_{i:05d}.mp4", "bytes": 1024},
            "right": {"path": f"video/right_{i:05d}.mp4", "bytes": 2048},
        }
        for i in range(count + append)
    ]
    (root / "events.ndjson").write_text("".join(json.dumps(item) + "\n" for item in events[:count]))
    if append:
        (root / "append.ndjson").write_text(
            "".join(json.dumps(item) + "\n" for item in events[count:])
        )
    helper = root / "encoder.py"
    helper.write_text(
        f"#!{sys.executable}\n"
        "import pathlib,sys,time\n"
        'sys.stdout.write(pathlib.Path(__file__).with_name("events.ndjson").read_text())\n'
        'print(\'{"event":"ready"}\', flush=True)\n'
        'pending = pathlib.Path(__file__).with_name("append.ndjson")\n'
        "if pending.exists():\n"
        '    while not pending.with_name("release").exists(): time.sleep(.001)\n'
        "    for line in pending.read_text().splitlines():\n"
        "        print(line, flush=True); time.sleep(.001)\n"
        "sys.stdin.buffer.read()\n"
        'print(\'{"event":"done","frames":0}\', flush=True)\n'
    )
    helper.chmod(0o755)
    return extension.NativeSessionStore().begin_recording(
        replace(_recording_plan(), session_root=str(root), encoder_executable=str(helper))
    )


class NativeSegmentTailTests(unittest.TestCase):
    def test_first_tail_request_registers_skipped_prefix_and_keeps_full_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            tx = transaction(Path(directory), 8)
            try:
                tail = tx.segments_since(6)
                full = tx.segments()
                self.assertEqual(tail, full[6:])
                self.assertEqual(tx.segments_since(0), full)
                self.assertEqual(tx.segments_since(8), [])
                self.assertEqual([item["index"] for item in full], list(range(8)))
            finally:
                tx.abort("test complete")

    def test_invalid_offset_is_rejected_and_does_not_hide_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            tx = transaction(Path(directory), 3)
            try:
                with self.assertRaisesRegex(RuntimeError, "invalid_argument: segment offset"):
                    tx.segments_since(4)
                with self.assertRaises((OverflowError, ValueError)):
                    tx.segments_since(-1)
                with self.assertRaises(TypeError):
                    tx.segments_since("1")
                self.assertEqual(len(tx.segments_since(0)), 3)
            finally:
                tx.abort("test complete")

    def test_concurrent_full_and_tail_queries_keep_contiguous_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            tx = transaction(Path(directory), 120)
            try:

                def read(index):
                    offset = index % 121
                    result = tx.segments() if offset == 0 else tx.segments_since(offset)
                    return offset, [item["index"] for item in result]

                with ThreadPoolExecutor(max_workers=8) as pool:
                    results = list(pool.map(read, range(240)))
                for offset, indices in results:
                    self.assertEqual(indices, list(range(offset, 120)))
                self.assertEqual(tx.snapshot()["state"], "recording")
            finally:
                tx.abort("test complete")

    def test_inventory_growth_during_concurrent_queries_preserves_new_tail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tx = transaction(root, 8, append=12)
            try:
                self.assertEqual(len(tx.segments()), 8)
                (root / "release").touch()

                def read(offset):
                    for _ in range(100):
                        tail = tx.segments_since(offset)
                        self.assertEqual(
                            [item["index"] for item in tail],
                            list(range(offset, offset + len(tail))),
                        )
                        self.assertLessEqual(offset + len(tail), 20)
                        time.sleep(0.001)

                with ThreadPoolExecutor(max_workers=4) as pool:
                    list(pool.map(read, (0, 3, 6, 8)))
                deadline = time.monotonic() + 5
                while len(tx.segments_since(8)) < 12 and time.monotonic() < deadline:
                    time.sleep(0.005)
                self.assertEqual(
                    [item["index"] for item in tx.segments_since(8)], list(range(8, 20))
                )
                self.assertEqual(tx.segments_since(20), [])
            finally:
                tx.abort("test complete")

    def test_empty_finished_transaction_supports_tail_and_full_reads(self):
        with tempfile.TemporaryDirectory() as directory:
            tx = transaction(Path(directory), 0)
            try:
                self.assertEqual(tx.segments_since(0), [])
                first = tx.finish(0.01, timeout_seconds=2)
                self.assertEqual(tx.finish(0.01, timeout_seconds=2), first)
                self.assertEqual(tx.segments_since(0), [])
                self.assertEqual(tx.segments(), [])
                self.assertEqual(tx.open_handle_count(), 0)
            finally:
                tx.abort("test complete")
