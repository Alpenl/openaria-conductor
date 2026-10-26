"""A fresh digest must still agree with the proven lossless output."""

import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import zstandard

from rp_ylx.recording import storage, verified
from rp_ylx.recording.live_seal import _Journal


class PreparedOutputAdmissionTests(unittest.TestCase):
    def fixture(self, root):
        manifest = {"schema": "old"}
        prepared = {}
        for kind in ("frames", "imu"):
            path = root / f"{kind}.ndjson"
            payload = b'{"value":1}\n'
            path.write_bytes(payload)
            journal = _Journal(path)
            artifact = journal.finish()
            prepared[path.name] = artifact
            manifest[kind] = {
                "artifact": {
                    "path": path.name,
                    "bytes": len(payload),
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "media_type": "application/x-ndjson",
                }
            }
        return manifest, prepared

    def exercise(self, *, corrupt, collide=False):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest, prepared = self.fixture(root)
            original = dict(manifest["frames"]["artifact"])
            target = prepared["frames.ndjson"].target
            proof = prepared["frames.ndjson"].output
            identity = verified.identity
            admitted = []

            def maybe_collide(metadata):
                if collide and metadata.st_ino == proof.identity[1]:
                    return proof.identity
                return identity(metadata)

            def finalize(path):
                # PreparedArtifact.use has returned. Change to another valid
                # compressed stream before the caller's fresh final read.
                if corrupt and path == target:
                    path.write_bytes(
                        zstandard.ZstdCompressor(level=1, write_checksum=True).compress(
                            b'{"value":2}\n'
                        )
                    )
                payload = path.read_bytes()
                admitted.append(path.name)
                return {
                    "path": path.name,
                    "bytes": len(payload),
                    "sha256": hashlib.sha256(payload).hexdigest(),
                }

            with patch.object(verified, "identity", side_effect=maybe_collide):
                if corrupt:
                    with self.assertRaisesRegex(ValueError, "prepared.*output"):
                        storage.compact_manifest(root, manifest, finalize, prepared=prepared)
                    self.assertEqual(manifest["schema"], "old")
                    self.assertEqual(manifest["frames"]["artifact"], original)
                else:
                    result, originals = storage.compact_manifest(
                        root, manifest, finalize, prepared=prepared
                    )
                    self.assertEqual(result["schema"], "ylx.device-session.v4")
                    self.assertEqual(originals, ["frames.ndjson", "imu.ndjson"])
                    self.assertEqual(admitted, ["frames.ndjson.zst", "imu.ndjson.zst"])
            self.assertEqual((root / "frames.ndjson").read_bytes(), b'{"value":1}\n')
            self.assertEqual((root / "imu.ndjson").read_bytes(), b'{"value":1}\n')

    def test_final_read_rejects_changed_prepared_output(self):
        self.exercise(corrupt=True)

    def test_final_read_rejects_corruption_even_when_identity_collides(self):
        self.exercise(corrupt=True, collide=True)

    def test_unchanged_prepared_output_is_admitted(self):
        self.exercise(corrupt=False)
