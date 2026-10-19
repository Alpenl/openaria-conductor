import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import rp_ylx.validation as validation


class ManifestBoundsTests(unittest.TestCase):
    def test_public_reader_rejects_growth_after_initial_stat(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "manifest.json"
            path.write_bytes(b"{}")
            original = path.stat()
            path.write_bytes(b"{}" + b" " * 128)
            fstat = os.fstat

            def initial_size(fd):
                current = fstat(fd)
                return original if current.st_ino == original.st_ino else current

            with (
                patch.object(validation, "MAX_MANIFEST_BYTES", 64),
                patch.object(os, "fstat", side_effect=initial_size),
                self.assertRaises(validation.PublicValidationError),
            ):
                validation._read_discriminator(root)

    def test_calibration_reader_rejects_growth_after_initial_stat(self):
        import rp_ylx.spectacular.adapter as adapter

        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "manifest.json"
            path.write_bytes(b"{}")
            original = path.stat()
            path.write_bytes(b"{}" + b" " * 128)
            stat_path = Path.stat

            def initial_size(value, *args, **kwargs):
                current = stat_path(value, *args, **kwargs)
                return original if current.st_ino == original.st_ino else current

            with (
                patch.object(adapter, "MAX_MANIFEST_BYTES", 64),
                patch.object(Path, "stat", initial_size),
                self.assertRaises(adapter.CaptureValidationError),
            ):
                adapter._read_json_object(path, "manifest")
