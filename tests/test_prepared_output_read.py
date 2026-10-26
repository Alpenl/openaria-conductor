"""Prepared output is read once at finalization, still against its original proof."""

import collections
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.recording import live_seal, storage, verified
from tests import test_prepared_output_admission as fixtures


class PreparedOutputReadTests(unittest.TestCase):
    def test_final_admission_reads_each_output_once_and_checks_source_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, prepared = fixtures.PreparedOutputAdmissionTests().fixture(root)
            counts = collections.Counter()
            real_open = verified.open_regular

            class Reader:
                def __init__(self, path):
                    self.path = path
                    self.stream = real_open(path)

                def read(self, size=-1):
                    payload = self.stream.read(size)
                    counts[self.path.name] += len(payload)
                    return payload

                def fileno(self):
                    return self.stream.fileno()

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    self.stream.close()

            def finalize(path):
                with Reader(path) as stream:
                    payload = stream.read()
                return {
                    "path": path.name,
                    "bytes": len(payload),
                    "sha256": hashlib.sha256(payload).hexdigest(),
                }

            with patch.object(verified, "open_regular", Reader):
                storage.compact_manifest(root, manifest, finalize, prepared=prepared)
            for relative, artifact in prepared.items():
                self.assertEqual(counts[relative], (root / relative).stat().st_size)
                self.assertEqual(counts[artifact.target.name], artifact.target.stat().st_size)
                self.assertEqual(
                    manifest[relative.split(".")[0]]["artifact"]["sha256"], artifact.output.sha256
                )

    def test_same_identity_output_corruption_before_preparation_is_rejected(self):
        self.reject_corruption(source=False)

    def test_same_identity_source_corruption_before_preparation_is_rejected(self):
        self.reject_corruption(source=True)

    def reject_corruption(self, *, source):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, prepared = fixtures.PreparedOutputAdmissionTests().fixture(root)
            artifact = prepared["frames.ndjson"]
            target = root / "frames.ndjson" if source else artifact.target
            proof = artifact.source if source else artifact.output
            data = bytearray(target.read_bytes())
            data[-1] ^= 1
            target.write_bytes(data)
            identity = verified.identity

            def collide(metadata):
                return (
                    proof.identity if metadata.st_ino == proof.identity[1] else identity(metadata)
                )

            def finalize(path):
                data = path.read_bytes()
                return {
                    "path": path.name,
                    "bytes": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                }

            with (
                patch.object(verified, "identity", side_effect=collide),
                patch.object(live_seal, "identity", side_effect=collide),
                self.assertRaisesRegex(ValueError, "source changed|.*output.*changed"),
            ):
                storage.compact_manifest(root, manifest, finalize, prepared=prepared)
            self.assertEqual(manifest["schema"], "old")
            self.assertEqual(manifest["frames"]["artifact"]["path"], "frames.ndjson")
