import json
import unittest
from contextlib import ExitStack
from unittest.mock import patch

from rp_ylx.api.downloads import ArtifactAccessError
from tests import test_capture_coordinator as fixtures


class CameraWorkflowTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)

    def prohibit_media_verification(self):
        stack = ExitStack()
        self.addCleanup(stack.close)
        for name in (
            "rp_ylx.recording.coordinator.validate_device_session_directory",
            "rp_ylx.recording.device_session._verify_artifact_fd",
            "rp_ylx.api.downloads._verify_open_artifact_python",
            "rp_ylx.api.downloads._native_open_verified_artifact",
        ):
            stack.enter_context(patch(name, side_effect=AssertionError("media scan forbidden")))

    def save(self):
        coordinator = self.fixture.coordinator()
        try:
            session = self.fixture.seal_one(coordinator, prefix="camera-workflow")
            page = coordinator.list_sessions(cursor=None, limit=50, take_id=None)
            self.assertIsNone(page["items"][0]["verification"])
            self.assertEqual(coordinator.capture_status()["snapshot"]["device_state"], "idle")
        finally:
            coordinator.close()
        directory = self.fixture.mountpoint / "recordings" / session
        manifest = json.loads((directory / "manifest.json").read_bytes())
        artifact = manifest["video"]["segments"][0]["artifacts"]["left"]
        return session, directory, artifact

    def test_save_restart_list_and_range_read_never_scan_media(self):
        self.prohibit_media_verification()
        session, directory, artifact = self.save()
        expected = (directory / artifact["path"]).read_bytes()
        for _ in range(2):
            coordinator = self.fixture.coordinator()
            try:
                page = coordinator.list_sessions(cursor=None, limit=50, take_id=None)
                self.assertEqual(page["items"][0]["session_id"], session)
                self.assertIsNone(page["items"][0]["verification"])
                with coordinator.open_verified_artifact(
                    session, artifact["artifact_id"], "v4"
                ) as opened:
                    self.assertEqual(opened.read(0, 4), expected[:4])
                self.assertEqual(coordinator.open_handle_count, 0)
            finally:
                coordinator.close()

    def test_playback_does_not_read_unrelated_files_or_require_digests(self):
        self.prohibit_media_verification()
        session, directory, artifact = self.save()
        path = directory / artifact["path"]
        data = bytearray(path.read_bytes())
        data[-1] ^= 1
        path.write_bytes(data)
        manifest = json.loads((directory / "manifest.json").read_bytes())
        (directory / manifest["imu"]["artifact"]["path"]).unlink()
        coordinator = self.fixture.coordinator()
        try:
            with coordinator.open_verified_artifact(
                session, artifact["artifact_id"], "v4"
            ) as opened:
                self.assertEqual(opened.read(), data)
        finally:
            coordinator.close()

    def test_missing_truncated_and_symlink_files_still_fail_cleanly(self):
        self.prohibit_media_verification()
        session, directory, artifact = self.save()
        path = directory / artifact["path"]
        data = path.read_bytes()
        outside = self.fixture.mountpoint / "outside-video"
        outside.write_bytes(data)
        coordinator = self.fixture.coordinator()
        try:
            for mode in ("missing", "truncated", "symlink"):
                with self.subTest(mode=mode):
                    path.unlink(missing_ok=True)
                    if mode == "truncated":
                        path.write_bytes(data[:-1])
                    elif mode == "symlink":
                        path.symlink_to(outside)
                    with self.assertRaises(ArtifactAccessError):
                        coordinator.open_verified_artifact(session, artifact["artifact_id"], "v4")
                    self.assertEqual(coordinator.open_handle_count, 0)
        finally:
            coordinator.close()

    def test_safe_remove_finishes_without_media_verification(self):
        self.prohibit_media_verification()
        coordinator = self.fixture.coordinator()
        try:
            coordinator.start_capture(fixtures.start_command("safe-remove"))
            coordinator.submit_frame(fixtures.frame())
            result = coordinator.stop_capture(
                fixtures.stop_command("safe-stop", reason="safe_swap")
            )
            self.assertEqual(result.status, 204)
            self.assertEqual(
                coordinator.current_safe_swap_receipt()["receipt"]["release_state"],
                "device-released",
            )
        finally:
            coordinator.close()
