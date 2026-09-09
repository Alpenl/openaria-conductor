"""Compile the actual hardware-independent MP4 helpers on the host."""

from __future__ import annotations

import shutil
import struct
import subprocess
import tempfile
import unittest
from pathlib import Path


def box(kind: bytes, data: bytes) -> bytes:
    return struct.pack(">I", len(data) + 8) + kind + data


class HobotMp4MetadataTest(unittest.TestCase):
    def test_metadata_patch_preserves_media_and_indexes_actual_idr(self) -> None:
        compiler = shutil.which("cc")
        if compiler is None:
            self.skipTest("C compiler unavailable")
        source = (
            Path(__file__).resolve().parents[1] / "src/rp_ylx/hobot/ylx_stereo_pipeline.c"
        ).read_text()
        helpers = source[
            source.index("static uint32_t read_u32(") : source.index(
                "/* --------------------------------------------------------------- muxer"
            )
        ]
        idr = source[source.index("static int h264_has_idr(") : source.index("static void fail(")]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            harness = root / "metadata.c"
            harness.write_text(
                "#define _GNU_SOURCE\n#include <stdint.h>\n#include <stddef.h>\n"
                "#include <stdio.h>\n#include <stdlib.h>\n#include <string.h>\n"
                "#include <fcntl.h>\n#include <unistd.h>\n#include <errno.h>\n"
                + helpers
                + idr
                + "\nint main(int argc, char **argv) {\n"
                "unsigned char idr[] = {0,0,0,1,0x67,9,0,0,1,0x65,3};\n"
                "unsigned char nonidr[] = {0,0,1,0x41,5,0,0};\n"
                "if (!h264_has_idr(idr,sizeof(idr)) || "
                "h264_has_idr(nonidr,sizeof(nonidr))) return 9;\n"
                "unsigned char hevc[] = {0,0,0,1,0x26,1,3};\n"
                "unsigned char cra[] = {0,0,1,0x2a,1,3};\n"
                "if (!h265_has_idr(hevc,sizeof(hevc)) || h265_has_idr(cra,sizeof(cra)) || "
                "h265_has_idr(hevc,5)) return 10;\n"
                "char reason[128]; if (argc != 2) return 8;\n"
                "return mp4_extend_durations(argv[1],30,reason,sizeof(reason)) == 0 ? 0 : 1; }\n"
            )
            binary = root / "metadata"
            subprocess.run(
                [compiler, "-Wall", "-Wextra", "-Werror", str(harness), "-o", str(binary)],
                check=True,
            )
            for sample_entry, primaries, transfer, matrix in [
                (kind, *color)
                for kind in (b"avc1", b"hvc1", b"hev1")
                for color in [(0, 0, 0), (1, 1, 1)]
            ]:
                with self.subTest(sample_entry=sample_entry, color=(primaries, transfer, matrix)):
                    colr = box(
                        b"colr", b"nclx" + struct.pack(">HHHB", primaries, transfer, matrix, 0)
                    )
                    stsd = box(
                        b"stsd",
                        bytes(4) + struct.pack(">I", 1) + box(sample_entry, bytes(78) + colr),
                    )
                    mvhd = bytearray(28)
                    struct.pack_into(">II", mvhd, 12, 1000, 1000)
                    tkhd = bytearray(32)
                    struct.pack_into(">I", tkhd, 20, 1000)
                    elst = box(b"elst", bytes(4) + struct.pack(">III", 1, 1000, 0) + bytes(4))
                    mdia = box(
                        b"mdia", box(b"mdhd", bytes(mvhd)) + box(b"minf", box(b"stbl", stsd))
                    )
                    moov = box(
                        b"moov",
                        box(b"mvhd", bytes(mvhd))
                        + box(b"trak", box(b"tkhd", bytes(tkhd)) + box(b"edts", elst) + mdia),
                    )
                    media = box(b"mdat", b"unaltered frame payload colr nclx" * 20)
                    path = root / "clip.mp4"
                    path.write_bytes(media + moov)
                    subprocess.run([str(binary), str(path)], check=True)
                    actual = path.read_bytes()
                    self.assertEqual(actual[: len(media)], media)
                    pos = actual.index(b"nclx", len(media)) + 4
                    self.assertEqual(
                        struct.unpack(">HHHB", actual[pos : pos + 7]),
                        (primaries or 2, transfer or 2, matrix or 2, 128),
                    )
                    pos = actual.index(b"mvhd", len(media))
                    self.assertEqual(struct.unpack(">I", actual[pos + 20 : pos + 24])[0], 1034)
            path.write_bytes(box(b"moov", box(b"trak", bytes(4))))
            self.assertEqual(subprocess.run([str(binary), str(path)]).returncode, 1)


if __name__ == "__main__":
    unittest.main()
