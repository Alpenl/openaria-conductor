"""Detect incomplete metadata frames without unbounded decode or retry barriers."""

import hashlib
import io
import os
import random
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import zstandard

from rp_ylx import session_timestamps
from rp_ylx.recording import storage
from rp_ylx.recording.metadata_frames import MetadataFrames
from rp_ylx.spectacular import adapter


class MetadataFrameIntegrityTests(unittest.TestCase):
    def encoded(self, content, *, known=True, checksum=True):
        parameters = zstandard.ZstdCompressionParameters.from_level(
            1, window_log=20, write_content_size=known, write_checksum=checksum
        )
        return zstandard.ZstdCompressor(compression_params=parameters).compress(content)

    def read(self, encoded):
        with (
            io.BytesIO(encoded) as source,
            io.BufferedReader(MetadataFrames(source, storage.BLOCK_BYTES)) as stream,
        ):
            return stream.read()

    def test_every_incomplete_prefix_is_rejected_even_if_all_text_is_decodable(self):
        encoded = self.encoded(b'{"sample":123}\n')
        for cut in range(len(encoded)):
            with self.subTest(cut=cut), self.assertRaises(zstandard.ZstdError):
                self.read(encoded[:cut])
        self.assertEqual(self.read(encoded), b'{"sample":123}\n')

    def test_second_frame_cannot_disappear_as_a_partial_or_garbage_suffix(self):
        first, second = self.encoded(b"first\n"), self.encoded(b"second\n")
        self.assertEqual(self.read(first), b"first\n")
        for cut in range(1, len(second)):
            with self.subTest(cut=cut), self.assertRaises(zstandard.ZstdError):
                self.read(first + second[:cut])
        for suffix in (b"x", b"garbage", b"\x28\xb5\x2f\xfd", b"\x00" * 32):
            with self.subTest(suffix=suffix), self.assertRaises(zstandard.ZstdError):
                self.read(first + suffix)
        self.assertEqual(self.read(first + second), b"first\nsecond\n")

    def test_valid_empty_frames_and_known_or_unknown_sizes_remain_readable(self):
        for known in (True, False):
            for checksum in (True, False):
                with self.subTest(known=known, checksum=checksum):
                    encoded = self.encoded(b"", known=known, checksum=checksum)
                    self.assertEqual(self.read(encoded), b"")
                    encoded += self.encoded(b"data\n", known=known, checksum=checksum)
                    encoded += self.encoded(b"", known=known, checksum=checksum)
                    self.assertEqual(self.read(encoded), b"data\n")
        with self.assertRaises(zstandard.ZstdError):
            self.read(b"")

    def test_library_generated_block_boundaries_match_independent_decoder(self):
        randomizer = random.Random(20261021)
        for size in (1, 255, 256, 65535, 65536, 131071, 131072, storage.BLOCK_BYTES):
            for kind in ("random", "repeated"):
                for known in (False, True):
                    with self.subTest(size=size, kind=kind, known=known):
                        payload = randomizer.randbytes(size) if kind == "random" else b"a" * size
                        encoded = self.encoded(payload, known=known)
                        expected = zstandard.ZstdDecompressor().decompress(
                            encoded, max_output_size=storage.BLOCK_BYTES, allow_extra_data=False
                        )
                        self.assertEqual(self.read(encoded), expected)
                        self.assertEqual(expected, payload)

    def test_declared_oversized_frame_is_rejected_before_decoding(self):
        encoded = self.encoded(b"x" * (storage.BLOCK_BYTES + 1))
        with (
            patch("rp_ylx.recording.metadata_frames.zstandard.ZstdDecompressor") as decoder,
            self.assertRaisesRegex(zstandard.ZstdError, "block limit"),
        ):
            self.read(encoded)
        decoder.return_value.stream_reader.assert_not_called()

    def test_unknown_output_size_cannot_exceed_the_metadata_block_limit(self):
        encoded = self.encoded(b"x" * (2 * storage.BLOCK_BYTES), known=False)
        self.assertEqual(zstandard.get_frame_parameters(encoded).window_size, storage.BLOCK_BYTES)
        with self.assertRaises(zstandard.ZstdError):
            self.read(encoded)

    def test_large_compression_window_remains_rejected(self):
        parameters = zstandard.ZstdCompressionParameters.from_level(
            1, window_log=23, write_content_size=False
        )
        encoded = zstandard.ZstdCompressor(compression_params=parameters).compress(b"x" * (2**21))
        with self.assertRaisesRegex(zstandard.ZstdError, "block limit"):
            self.read(encoded)

    def test_reserved_or_oversized_blocks_are_rejected(self):
        encoded = self.encoded(b"data")
        header = encoded[: zstandard.frame_header_size(encoded)]
        for value in (7, (131073 << 3) | 1):
            with self.subTest(value=value), self.assertRaises(zstandard.ZstdError):
                self.read(header + value.to_bytes(3, "little"))

    def test_slow_short_source_reads_and_buffered_line_reads(self):
        content = b"one\n" + b"two" * 50000 + b"\nlast\n"
        encoded = self.encoded(content[:100000]) + self.encoded(content[100000:])

        class ShortSource(io.BytesIO):
            def read(self, size=-1):
                self.assert_size(size)
                return super().read(min(size, 7))

            @staticmethod
            def assert_size(size):
                if not 0 <= size <= 128 * 1024:
                    raise AssertionError("unbounded compressed source read")

        with ShortSource(encoded) as source:
            with io.BufferedReader(MetadataFrames(source, storage.BLOCK_BYTES)) as stream:
                self.assertEqual(stream.read(0), b"")
                self.assertEqual(stream.readline(), b"one\n")
                self.assertEqual(stream.readlines(), [b"two" * 50000 + b"\n", b"last\n"])
            self.assertFalse(source.closed)

    def test_checksum_damage_is_rejected_including_empty_frames(self):
        for content in (b"", b"sample\n"):
            with self.subTest(content=content):
                encoded = self.encoded(content)
                with self.assertRaises(zstandard.ZstdError):
                    self.read(encoded[:-1] + bytes([encoded[-1] ^ 1]))

    def test_false_empty_content_size_cannot_bypass_content_validation(self):
        encoded = bytearray(self.encoded(b"sample\n"))
        self.assertEqual(zstandard.frame_header_size(encoded), 6)
        encoded[5] = 0
        with self.assertRaises(zstandard.ZstdError):
            self.read(encoded)

    def test_missing_checksum_during_compaction_preserves_prior_and_allows_retry(self):
        for content, missing in ((b"{}\n", 1), (b"{}\n", 4), (b"", 13)):
            with self.subTest(content=content, missing=missing), tempfile.TemporaryDirectory() as d:
                path = Path(d) / "imu.ndjson"
                target = path.with_suffix(".ndjson.zst")
                path.write_bytes(content)
                target.write_bytes(b"prior verified result")
                fsync = os.fsync

                def truncate(fd, missing=missing, fsync=fsync):
                    os.ftruncate(fd, os.fstat(fd).st_size - missing)
                    fsync(fd)

                with (
                    patch.object(storage.os, "fsync", truncate),
                    self.assertRaises(zstandard.ZstdError),
                ):
                    storage.compact_metadata(path)
                self.assertEqual(path.read_bytes(), content)
                self.assertEqual(target.read_bytes(), b"prior verified result")
                self.assertEqual(list(Path(d).glob(".*.tmp")), [])
                result, encoding = storage.compact_metadata(path)
                with storage.open_metadata(result) as stream:
                    self.assertEqual(stream.read(), content)
                self.assertEqual(
                    encoding["uncompressed_sha256"], hashlib.sha256(content).hexdigest()
                )

    def test_failed_compaction_does_not_leave_retry_barriers_or_descriptors(self):
        for operation in ("fsync", "replace"):
            for fault in (OSError, KeyboardInterrupt, MemoryError):
                with (
                    self.subTest(operation=operation, fault=fault),
                    tempfile.TemporaryDirectory() as d,
                ):
                    path = Path(d) / "imu.ndjson"
                    path.write_bytes(b"{}\n")
                    target = path.with_suffix(".ndjson.zst")
                    target.write_bytes(b"prior")
                    before = len(os.listdir("/proc/self/fd"))
                    retained = []
                    with patch.object(storage.os, operation, side_effect=fault):
                        try:
                            storage.compact_metadata(path)
                        except BaseException as error:
                            retained.append(error)
                    self.assertEqual(len(retained), 1)
                    self.assertIsInstance(retained[0], fault)
                    self.assertEqual(len(os.listdir("/proc/self/fd")), before)
                    self.assertEqual(list(Path(d).glob(".*.tmp")), [])
                    self.assertEqual(target.read_bytes(), b"prior")
                    storage.compact_metadata(path)

    def test_abandoned_older_temporary_is_preserved_and_does_not_block_retry(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "imu.ndjson"
            path.write_bytes(b"{}\n")
            abandoned = path.with_suffix(".ndjson.zst.tmp")
            abandoned.write_bytes(b"older interrupted write")
            storage.compact_metadata(path)
            self.assertEqual(abandoned.read_bytes(), b"older interrupted write")

    def test_metadata_consumers_report_typed_errors_for_truncated_zstd(self):
        for reader, error_type in (
            (session_timestamps._read_jsonl, session_timestamps.SessionTimestampError),
            (adapter._read_jsonl, adapter.CaptureValidationError),
        ):
            with self.subTest(reader=reader), tempfile.TemporaryDirectory() as d:
                path = Path(d) / "frames.ndjson.zst"
                path.write_bytes(self.encoded(b'{"frame":0}\n')[:-1])
                with self.assertRaises(error_type) as raised:
                    reader(path, "frames")
                self.assertIsInstance(raised.exception.__cause__, zstandard.ZstdError)

    def test_encoded_empty_block_flood_has_a_bounded_read_limit(self):
        empty = self.encoded(b"", checksum=False)
        header = empty[: zstandard.frame_header_size(empty)]
        encoded = header + b"\x00\x00\x00" * 1000 + b"\x01\x00\x00"
        with io.BytesIO(encoded) as source:
            with (
                io.BufferedReader(MetadataFrames(source, 1024)) as stream,
                self.assertRaisesRegex(zstandard.ZstdError, "encoded metadata frame"),
            ):
                stream.read()
            self.assertLessEqual(source.tell(), 2050)

    def test_short_compaction_writes_preserve_prior_result_and_release_files(self):
        for content in (b"", b"{}\n"):
            with self.subTest(content=content), tempfile.TemporaryDirectory() as d:
                path = Path(d) / "imu.ndjson"
                path.write_bytes(content)
                target = path.with_suffix(".ndjson.zst")
                target.write_bytes(b"prior")
                opener = Path.open
                before = len(os.listdir("/proc/self/fd"))
                retained = []

                class ShortOutput:
                    def __init__(self, stream):
                        self.stream = stream

                    def __enter__(self):
                        return self

                    def __exit__(self, *args):
                        self.stream.close()

                    def __getattr__(self, name):
                        return getattr(self.stream, name)

                    def write(self, value):
                        return self.stream.write(value[:-1])

                def short(path, *args, opener=opener, wrapper=ShortOutput, **kwargs):
                    result = opener(path, *args, **kwargs)
                    return wrapper(result) if args == ("xb",) else result

                with patch.object(Path, "open", short):
                    try:
                        storage.compact_metadata(path)
                    except OSError as error:
                        retained.append(error)
                self.assertEqual(len(retained), 1)
                self.assertEqual(len(os.listdir("/proc/self/fd")), before)
                self.assertEqual(target.read_bytes(), b"prior")
                self.assertEqual(list(Path(d).glob(".*.tmp")), [])
                storage.compact_metadata(path)
