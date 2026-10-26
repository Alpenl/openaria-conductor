"""Startup must never settle or publish a linked partial directory."""

import json
import unittest
from unittest.mock import patch

from rp_ylx.recording import coordinator as module
from tests import test_capture_coordinator as fixtures


class PartialRootBoundaryTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)

    def test_linked_partial_does_not_delete_or_rewrite_external_metadata(self):
        first = self.fixture.coordinator()
        try:
            first.start_capture(fixtures.start_command("partial-root-start"))
            sid = self.fixture.active_session_id(first)
            state = first.capture_status()["snapshot"]["active_recording"]["recording_state"]
        finally:
            first.close()
        partial = self.fixture.mountpoint / "recordings" / (sid + ".partial")
        external = self.fixture.root / "protected-directory"
        partial.rename(external)
        (external / "recording.json").write_text(json.dumps(state))
        (external / "manifest.json").write_text("{}")
        before = {path.name: path.read_bytes() for path in external.iterdir() if path.is_file()}
        partial.symlink_to(external, target_is_directory=True)
        restarted = self.fixture.coordinator()
        try:
            after = {path.name: path.read_bytes() for path in external.iterdir() if path.is_file()}
            self.assertEqual(set(before), set(after), "startup removed external metadata")
            self.assertEqual(before, after, "startup rewrote external metadata")
            self.assertTrue(partial.is_symlink())
            self.assertFalse((partial.parent / sid).exists())
        finally:
            restarted.close()

    def make_interrupted_partial(self):
        first = self.fixture.coordinator()
        try:
            first.start_capture(fixtures.start_command("partial-boundary-start"))
            sid = self.fixture.active_session_id(first)
            state = first.capture_status()["snapshot"]["active_recording"]["recording_state"]
        finally:
            first.close()
        partial = self.fixture.mountpoint / "recordings" / (sid + ".partial")
        (partial / "recording.json").write_text(json.dumps(state))
        external = self.fixture.root / "protected-directory"
        external.mkdir()
        (external / "recording.json").write_bytes(b"protected recording metadata")
        (external / "manifest.json").write_bytes(b"protected manifest")
        return sid, partial, external

    def replace_with_link(self, partial, external):
        partial.rename(self.fixture.root / "original-partial")
        partial.symlink_to(external, target_is_directory=True)

    def check_replacement_during_validation(self, *, rejected):
        sid, partial, external = self.make_interrupted_partial()
        (partial / "manifest.json").write_text("{}")
        before = {path.name: path.read_bytes() for path in external.iterdir()}
        inspect = module.inspect_device_session_directory

        def replace_directory(*args, **kwargs):
            if args[0] != partial:
                return inspect(*args, **kwargs)
            self.replace_with_link(partial, external)
            if rejected:
                raise module.DeviceRecordingError("manifest_invalid", "injected validation error")

        with patch.object(
            module, "inspect_device_session_directory", side_effect=replace_directory
        ) as validate:
            restarted = self.fixture.coordinator()
        try:
            self.assertEqual(sum(call.args[0] == partial for call in validate.call_args_list), 1)
            self.assertEqual(before, {path.name: path.read_bytes() for path in external.iterdir()})
            self.assertTrue(partial.is_symlink())
            self.assertFalse((partial.parent / sid).exists())
            self.assertNotIn(sid, restarted._verified)
        finally:
            restarted.close()

    def test_validation_failure_after_directory_replacement_preserves_external_files(self):
        self.check_replacement_during_validation(rejected=True)

    def test_validation_success_after_directory_replacement_does_not_publish_link(self):
        self.check_replacement_during_validation(rejected=False)

    def test_settlement_write_remains_on_opened_directory_after_replacement(self):
        sid, partial, external = self.make_interrupted_partial()
        before = {path.name: path.read_bytes() for path in external.iterdir()}
        write = module.write_json_atomic
        replaced = []

        def replace_before_write(path, value, **kwargs):
            if path.name == "recording.json":
                self.assertFalse(replaced)
                self.replace_with_link(partial, external)
                replaced.append(True)
            return write(path, value, **kwargs)

        with patch.object(module, "write_json_atomic", side_effect=replace_before_write):
            restarted = self.fixture.coordinator()
        try:
            self.assertEqual(replaced, [True])
            self.assertEqual(before, {path.name: path.read_bytes() for path in external.iterdir()})
            self.assertFalse((partial.parent / sid).exists())
        finally:
            restarted.close()

    def test_publication_directory_fsync_failure_keeps_published_manifest(self):
        first = self.fixture.coordinator()
        try:
            sid = self.fixture.seal_one(first, prefix="partial-sync")
        finally:
            first.close()
        final = self.fixture.mountpoint / "recordings" / sid
        manifest = (final / "manifest.json").read_bytes()
        partial = final.with_name(sid + ".partial")
        final.rename(partial)
        fsync_directory = module.fsync_directory
        failures = []

        def fail_after_rename(path):
            if path == final.parent and final.exists():
                failures.append(True)
                raise OSError("injected parent directory fsync failure")
            return fsync_directory(path)

        with patch.object(module, "fsync_directory", side_effect=fail_after_rename):
            restarted = self.fixture.coordinator()
        try:
            self.assertEqual(failures, [True])
            self.assertFalse(partial.exists())
            self.assertEqual((final / "manifest.json").read_bytes(), manifest)
            self.assertNotIn(sid, restarted._verified)
            fixtures.validate_device_session_directory(final)
        finally:
            restarted.close()
