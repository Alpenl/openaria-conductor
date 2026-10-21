import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.native import create_native_session_store
from rp_ylx.recording.catalog import SessionCatalog


class NativeCatalogAdmissionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name) / "session"
        (self.root / "video" / "left").mkdir(parents=True)
        self.file = self.root / "video/left/file"
        self.file.write_bytes(b"x")
        self.native = create_native_session_store()
        self.artifacts = [("video/left/file", 1)]

    def native_check(self, root=None, artifacts=None):
        return self.native.catalog_artifacts_match(
            os.fsencode(self.root if root is None else root),
            [
                (os.fsencode(name), size)
                for name, size in (self.artifacts if artifacts is None else artifacts)
            ],
        )

    def outcome(self, function):
        try:
            return function()
        except OSError:
            return "error"

    def compare(self, artifacts=None, root=None):
        artifacts = self.artifacts if artifacts is None else artifacts
        root = self.root if root is None else root
        native = self.outcome(lambda: self.native_check(root, artifacts))
        python = self.outcome(lambda: SessionCatalog._artifacts_match_python(root, artifacts))
        self.assertEqual(native, python)
        return native

    def test_exact_posix_spellings_and_non_utf8_names_match_reference(self):
        for spelling in ("video/left/file", "./video//left/./file/", "video/left/file//"):
            self.assertTrue(self.compare([(spelling, 1)]))
            self.assertFalse(self.compare([(spelling, 2)]))
        for name in ("back\\slash", "汉字", os.fsdecode(b"nonutf8-\xff")):
            (self.root / name).write_bytes(b"x")
            self.assertTrue(self.compare([(name, 1)]))
        self.assertTrue(self.compare([]))

    def test_invalid_paths_are_rejected_like_reference(self):
        for name in ("", ".", "./", "/file", "//file", "../file", "video/../file", "a\0b"):
            with self.subTest(name=name):
                self.assertEqual(self.compare([(name, 1)]), "error")

    def test_file_types_missing_sizes_and_links_match_reference(self):
        for kind in ("missing", "size", "hardlink", "symlink", "fifo", "directory"):
            with self.subTest(kind=kind):
                self.file.unlink()
                if kind == "size":
                    self.file.write_bytes(b"xx")
                elif kind == "hardlink":
                    (self.root / "other").write_bytes(b"x")
                    os.link(self.root / "other", self.file)
                elif kind == "symlink":
                    self.file.symlink_to(self.root / "other")
                elif kind == "fifo":
                    os.mkfifo(self.file)
                elif kind == "directory":
                    self.file.mkdir()
                self.assertIn(self.compare(), (False, "error"))
                if self.file.is_dir():
                    self.file.rmdir()
                else:
                    self.file.unlink(missing_ok=True)
                (self.root / "other").unlink(missing_ok=True)
                self.file.write_bytes(b"x")

    def test_parent_and_root_symlinks_are_rejected(self):
        original = self.root / "video/left"
        retired = self.root / "retired"
        original.rename(retired)
        original.symlink_to(retired, target_is_directory=True)
        self.assertEqual(self.compare(), "error")
        original.unlink()
        retired.rename(original)
        link = self.root.parent / "linked"
        link.symlink_to(self.root, target_is_directory=True)
        self.assertEqual(self.compare(root=link), "error")

    def test_many_groups_and_repeated_failures_release_all_descriptors(self):
        artifacts = []
        for index in range(150):
            parent = self.root / "groups" / str(index)
            parent.mkdir(parents=True)
            (parent / "file").write_bytes(b"x")
            artifacts.append((f"groups/{index}/file", 1))
        baseline = len(os.listdir("/proc/self/fd"))
        for _ in range(50):
            self.assertTrue(self.compare(artifacts))
            self.assertEqual(self.compare(artifacts + [("missing/file", 1)]), "error")
            self.assertEqual(len(os.listdir("/proc/self/fd")), baseline)

    def test_public_path_uses_native_without_python_file_iteration(self):
        with (
            patch(
                "rp_ylx.recording.catalog.native_session_store_or_none", return_value=self.native
            ),
            patch.object(SessionCatalog, "_artifacts_match_python", side_effect=AssertionError),
        ):
            self.assertTrue(SessionCatalog._artifacts_match(self.root, self.artifacts))

    def test_missing_optional_method_uses_python_fallback(self):
        for backend in (None, object()):
            with patch(
                "rp_ylx.recording.catalog.native_session_store_or_none", return_value=backend
            ):
                self.assertTrue(SessionCatalog._artifacts_match(self.root, self.artifacts))

    def test_native_io_and_cancellation_never_fall_back(self):
        class Backend:
            def catalog_artifacts_match(self, *args):
                raise fault

        for fault in (OSError("io failed"), KeyboardInterrupt(), MemoryError()):
            with (
                patch(
                    "rp_ylx.recording.catalog.native_session_store_or_none", return_value=Backend()
                ),
                patch.object(SessionCatalog, "_artifacts_match_python", side_effect=AssertionError),
                self.assertRaises(type(fault)),
            ):
                SessionCatalog._artifacts_match(self.root, self.artifacts)

    def test_bad_native_return_and_bad_database_types_are_rejected(self):
        class Backend:
            def catalog_artifacts_match(self, *args):
                return "true"

        with (
            patch("rp_ylx.recording.catalog.native_session_store_or_none", return_value=Backend()),
            self.assertRaises(OSError),
        ):
            SessionCatalog._artifacts_match(self.root, self.artifacts)
        with patch(
            "rp_ylx.recording.catalog.native_session_store_or_none", return_value=self.native
        ):
            for name in (None, 12):
                with self.assertRaises(OSError):
                    SessionCatalog._artifacts_match(self.root, [(name, 1)])
            for size in (None, "1", -1, 2**64, True):
                self.assertFalse(
                    SessionCatalog._artifacts_match(self.root, [("video/left/file", size)])
                )

    def test_persisted_catalog_admission_uses_native_and_rejects_changed_payload_size(self):
        manifest = {"artifacts": [{"path": "video/left/file", "bytes": 1, "sha256": "0" * 64}]}
        (self.root / "manifest.json").write_text(json.dumps(manifest))
        summary = {"session_id": "session", "started_at": "2026", "take_id": "test"}
        store = SessionCatalog(self.root.parent / "catalog.sqlite3")
        store.remember("volume", self.root, "commit", summary, manifest)
        signature = store.signature(self.root)
        with patch.object(SessionCatalog, "_artifacts_match_python", side_effect=AssertionError):
            cached = store.cached("volume", "session", signature, "commit", directory=self.root)
        self.assertEqual(cached, dict(summary, verification=None))
        store = SessionCatalog(self.root.parent / "catalog.sqlite3")
        self.file.write_bytes(b"xx")
        self.assertIsNone(
            store.cached("volume", "session", signature, "commit", directory=self.root)
        )
        self.assertEqual(store._admitted, {})

    def test_native_walk_is_bounded_under_low_fd_limit(self):
        # Child-only limit, with 150 independent parent groups and depth two.
        import subprocess
        import sys

        for index in range(150):
            parent = self.root / "groups" / str(index)
            parent.mkdir(parents=True)
            (parent / "file").write_bytes(b"x")
        program = """import resource,sys,os
from rp_ylx.native import create_native_session_store
resource.setrlimit(resource.RLIMIT_NOFILE,(32,32))
n=create_native_session_store()
a=[(f'groups/{i}/file'.encode(),1) for i in range(150)]
assert n.catalog_artifacts_match(os.fsencode(sys.argv[1]),a)
"""
        subprocess.run([sys.executable, "-c", program, str(self.root)], check=True, timeout=10)
