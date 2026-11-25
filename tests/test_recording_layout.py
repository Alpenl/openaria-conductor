from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from rp_ylx.recording_layout import iter_recordings, publish_readable, resolve_recording


class RecordingLayoutTests(unittest.TestCase):
    def test_same_second_keeps_both_recordings_and_old_layout(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identities = [f"01900000-0000-7000-8000-{i:012d}" for i in range(3)]
            paths = []
            for identity in identities:
                path = root / identity
                path.mkdir()
                manifest = {
                    "session_id": identity,
                    "time": {"started_at": "2026-11-25T14:32:08+08:00"},
                }
                (path / "manifest.json").write_text(json.dumps(manifest))
                (path / "video.mp4").write_bytes(identity.encode())
                paths.append(publish_readable(path, manifest) if len(paths) < 2 else path)
            self.assertEqual(paths[0].relative_to(root), Path("2026-11-25/14-32-08"))
            self.assertEqual(paths[1].relative_to(root), Path("2026-11-25/14-32-08_02"))
            self.assertEqual(
                dict((identity, path) for path, identity in iter_recordings(root)),
                dict(zip(identities, paths, strict=True)),
            )
            for identity, path in zip(identities, paths, strict=True):
                self.assertEqual(resolve_recording(root, identity), path)
                self.assertEqual((path / "video.mp4").read_bytes(), identity.encode())

    def test_unknown_time_and_duplicate_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity = "01900000-0000-7000-8000-000000000001"
            manifest = {"session_id": identity, "time": {"started_at": "2000-01-01T00:00:00Z"}}
            original = root / identity
            original.mkdir()
            (original / "manifest.json").write_text(json.dumps(manifest))
            published = publish_readable(original, manifest)
            self.assertEqual(published.relative_to(root), Path("录制时间未知/录制_01"))
            original.mkdir()
            (original / "manifest.json").write_text(json.dumps(manifest))
            with self.assertRaisesRegex(OSError, "ambiguous"):
                resolve_recording(root, identity)

    def test_date_symlink_cannot_redirect_publication(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as outside:
            root = Path(directory)
            identity = "01900000-0000-7000-8000-000000000001"
            original = root / identity
            original.mkdir()
            (root / "2026-11-25").symlink_to(outside, target_is_directory=True)
            with self.assertRaises(OSError):
                publish_readable(
                    original,
                    {"session_id": identity, "time": {"started_at": "2026-11-25T14:32:08+08:00"}},
                )
            self.assertTrue(original.is_dir())
            self.assertEqual(list(Path(outside).iterdir()), [])
