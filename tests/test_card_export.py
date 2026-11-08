from __future__ import annotations

import hashlib
import io
import json
import shutil
import struct
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.card_export import export as tool


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda: f.read(4 * 1024**2), b""):
            h.update(b)
    return h.hexdigest()


@unittest.skipUnless(shutil.which("mkfs.ext4"), "real ext4 fixture requires mkfs.ext4")
class RealCardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        seed = self.root / "seed"
        session = seed / "recordings" / "recording-01"
        (session / "video").mkdir(parents=True)
        (session / "audio").mkdir()
        (session / "manifest.json").write_text('{"test":true}')
        (session / "video" / "left_00000.mp4").write_bytes(bytes(range(256)) * 123)
        (session / "audio" / "audio_00000.wav").write_bytes(b"wav-test" * 117)
        outer_seed = self.root / "outer-seed"
        nested = outer_seed / "var/lib/rp-ylx-data-volume/data-volume.img"
        nested.parent.mkdir(parents=True)
        subprocess.run(
            ["mkfs.ext4", "-q", "-F", "-d", str(seed), str(nested), "16M"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        outer = self.root / "root.ext4"
        subprocess.run(
            ["mkfs.ext4", "-q", "-F", "-d", str(outer_seed), str(outer), "64M"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.card = self.root / "card.img"
        mbr = bytearray(512)
        mbr[510:] = b"\x55\xaa"
        mbr[450] = 0x83
        struct.pack_into("<II", mbr, 454, 2048, outer.stat().st_size // 512)
        with self.card.open("wb") as dst, outer.open("rb") as src:
            dst.write(mbr)
            dst.seek(1024**2)
            shutil.copyfileobj(src, dst)

    def test_nested_ext4_stream_export_and_source_unchanged(self):
        before = sha(self.card)
        with tool.volumes(str(self.card)) as found:
            records = tool.recordings(found)
            self.assertEqual(len(records), 1)
            self.assertFalse(records[0]["dirty"])
            output = tool.export_selected(records, self.root)
        report = json.loads((output / "export-report.json").read_bytes())
        self.assertEqual(report["status"], "complete")
        self.assertEqual(len(report["files"]), 3)
        for file in report["files"]:
            p = output / file["path"]
            self.assertEqual(sha(p), file["sha256"])
            self.assertEqual(p.stat().st_size, file["bytes"])
        copied = next(output.rglob("left_00000.mp4"))
        self.assertEqual(copied.read_bytes(), bytes(range(256)) * 123)
        self.assertEqual(sha(self.card), before)
        self.assertFalse(any(p.name == "data-volume.img" for p in output.rglob("*")))

    def test_dirty_outer_filesystem_refuses_export_by_default(self):
        with self.card.open("r+b") as f:
            f.seek(1024**2 + 1024 + 58)
            f.write(b"\0\0")
        with tool.volumes(str(self.card)) as found:
            records = tool.recordings(found)
            self.assertTrue(records[0]["dirty"])
            with self.assertRaisesRegex(RuntimeError, "cleanly"):
                tool.export_selected(records, self.root)
            result = tool.export_selected(records, self.root, allow_dirty=True)
            self.assertTrue(
                json.loads((result / "export-report.json").read_bytes())["source_dirty"]
            )

    def test_short_read_keeps_incomplete_receipt(self):
        with tool.volumes(str(self.card)) as found:
            records = tool.recordings(found)
            node = next(n for _, n in tool.walk(records[0]["node"]) if n.size > 100)
            with (
                patch.object(node, "open", return_value=io.BytesIO(b"")),
                self.assertRaisesRegex(RuntimeError, "Short source"),
            ):
                tool.export_selected(records, self.root)
        receipts = list(self.root.glob("OpenAria-export-*/export-report.json"))
        self.assertEqual(len(receipts), 1)
        self.assertEqual(json.loads(receipts[0].read_bytes())["status"], "incomplete")


class GuardTests(unittest.TestCase):
    def test_portable_name_validation(self):
        for name in ("..", "../escape", "a/b", "a\\b", "CON", "a:b", "x.", "NUL.mp4"):
            with self.assertRaises(RuntimeError):
                tool.safe_name(name)
        self.assertEqual(tool.safe_name("left_00000.mp4"), "left_00000.mp4")

    def test_linux_destination_cannot_be_card_partition(self):
        with (
            tempfile.TemporaryDirectory() as folder,
            patch.object(tool.platform, "system", return_value="Linux"),
            patch.object(tool, "command", return_value=b"/dev/sdb2\n"),
            self.assertRaisesRegex(RuntimeError, "source card"),
        ):
            tool.destination_check("/dev/sdb", folder)

    def test_windows_device_listing(self):
        with (
            patch.object(tool.platform, "system", return_value="Windows"),
            patch.object(
                tool,
                "ps",
                return_value=b'[{"Number":2,"Size":128000000000,"FriendlyName":"Reader"}]',
            ),
        ):
            self.assertEqual(tool.discover()[0]["path"], r"\\.\PhysicalDrive2")


if __name__ == "__main__":
    unittest.main()
