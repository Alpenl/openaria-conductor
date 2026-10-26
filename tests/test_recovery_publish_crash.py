"""Recovery publication crash boundaries with isolated fake media."""

import json
import multiprocessing
import os
import signal
import sqlite3
import time
import traceback
import unittest
import uuid
from dataclasses import replace
from unittest.mock import patch

from rp_ylx.recording import coordinator as module
from tests import test_capture_coordinator as fixtures


class RecoveryPublishCrashTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.fixture.session_config = replace(
            self.fixture.session_config,
            frame_decimation=2,
            video_layout="split-eyes",
            segment_seconds=0.1,
        )

    def kill_after_publication(
        self, *, database_failure=False, normal_stop=False, before_rename=False
    ):
        context = multiprocessing.get_context("fork")
        receive, send = context.Pipe(duplex=False)

        def child():
            try:
                coordinator = self.fixture.coordinator()
                coordinator.start_capture(fixtures.start_command("recovery-publish-crash"))
                sid = self.fixture.active_session_id(coordinator)
                recorder = coordinator._active
                origin = recorder._started_monotonic_ns
                for index in range(6):
                    observation = fixtures.frame(index * 2)
                    observation = replace(
                        observation,
                        frame=replace(
                            observation.frame,
                            host_monotonic_ns=origin + 10_000_000 + round(index * 1e9 / 30),
                        ),
                    )
                    self.assertTrue(coordinator.submit_frame(observation))
                recorder._stop_writer()
                self.assertEqual(recorder._frames_written, 6)
                self.assertIsNone(recorder._writer_error)
                time.sleep(0.25)

                failed_writes = []

                def boundary(active):
                    self.assertIs(active, recorder)
                    if not normal_stop:
                        self.assertIsNotNone(active.recovered_session)
                    self.assertTrue(
                        (self.fixture.mountpoint / "recordings" / sid / "manifest.json").is_file()
                    )
                    if database_failure:
                        self.assertTrue(failed_writes, "database injection was not exercised")
                        durable = coordinator._read_local_state()
                        self.assertEqual(durable["active_session"]["session_id"], sid)
                        self.assertNotIn(sid, durable["retained"])
                    send.send({"session_id": sid, "revision": coordinator._revision})
                    signal.pause()

                if normal_stop:
                    persist = coordinator._persist_local_state

                    def after_normal_publication():
                        if coordinator._active is None:
                            boundary(recorder)
                        return persist()

                    coordinator._persist_local_state = after_normal_publication
                    coordinator.stop_capture(fixtures.stop_command("normal-stop-crash"))
                    raise AssertionError("normal stop crash boundary returned")
                if database_failure:

                    def unavailable(*args, **kwargs):
                        failed_writes.append(True)
                        raise sqlite3.OperationalError("injected database unavailable")

                    coordinator._state_db.put_capture_state = unavailable
                if before_rename:
                    rename = os.rename

                    def before_publish(source, destination, *args, **kwargs):
                        expected = self.fixture.mountpoint / "recordings" / sid
                        if destination == expected:
                            self.assertTrue((source / "recovery" / "recording.json").is_file())
                            self.assertTrue((source / "manifest.json").is_file())
                            send.send({"session_id": sid, "revision": coordinator._revision})
                            signal.pause()
                        return rename(source, destination, *args, **kwargs)

                    os.rename = before_publish
                coordinator._commit_active_failure = boundary
                coordinator._source_failure(
                    coordinator.generation_id, "source_sequence_gap", "original capture fault"
                )
                raise AssertionError("crash boundary returned")
            except BaseException:
                send.send({"error": traceback.format_exc()})
                raise

        process = context.Process(target=child)
        process.start()
        send.close()
        try:
            self.assertTrue(receive.poll(15), "recovery did not reach publication boundary")
            message = receive.recv()
            self.assertNotIn("error", message, message)
            process.kill()
            process.join(5)
            self.assertEqual(process.exitcode, -signal.SIGKILL)
            return message
        finally:
            if process.is_alive():
                process.kill()
                process.join(5)
            receive.close()
            process.close()

    def test_published_prefix_fault_survives_crash_before_coordinator_commit(self):
        published = self.kill_after_publication()
        coordinator = self.fixture.coordinator()
        try:
            sid = published["session_id"]
            outcome = coordinator.retained_unsuccessful_outcome(sid)
            self.assertIsNotNone(outcome, "published recovery lost its failure outcome")
            state = outcome["outcome"]["recording_state"]
            self.assertEqual(state["diagnostics"][0]["code"], "source_sequence_gap")
            self.assertEqual(state["diagnostics"][0]["message"], "original capture fault")
            self.assertIn("recording_prefix_saved", [d["code"] for d in state["diagnostics"]])
            self.assertGreaterEqual(coordinator._revision, published["revision"])
            self.fixture.assert_network_operation_lock_available()
            listed = coordinator.list_sessions(cursor=None, limit=50, take_id=None)
            self.assertEqual([item["session_id"] for item in listed["items"]], [sid])
        finally:
            coordinator.close()

    def test_fault_survives_crash_with_all_failure_database_writes_unavailable(self):
        published = self.kill_after_publication(database_failure=True)
        coordinator = self.fixture.coordinator()
        try:
            outcome = coordinator.retained_unsuccessful_outcome(published["session_id"])
            self.assertIsNotNone(outcome)
            self.assertEqual(
                outcome["outcome"]["recording_state"]["diagnostics"][0]["message"],
                "original capture fault",
            )
        finally:
            coordinator.close()

    def test_normal_publication_is_not_reclassified_as_failed(self):
        published = self.kill_after_publication(normal_stop=True)
        coordinator = self.fixture.coordinator()
        try:
            sid = published["session_id"]
            self.assertIsNone(coordinator.retained_unsuccessful_outcome(sid))
            self.assertIsNone(coordinator._read_local_state()["active_session"])
            listed = coordinator.list_sessions(cursor=None, limit=50, take_id=None)
            self.assertEqual([item["session_id"] for item in listed["items"]], [sid])
        finally:
            coordinator.close()

    def test_restart_restores_only_metadata_and_does_not_grant_payload_verification(self):
        published = self.kill_after_publication()
        sid = published["session_id"]
        with (
            patch.object(
                module,
                "inspect_device_session_directory",
                side_effect=AssertionError("startup must not inspect payload artifacts"),
            ),
            patch.object(
                module,
                "validate_device_session_directory",
                side_effect=AssertionError("startup must not hash payload artifacts"),
            ),
            patch.object(
                module, "_read_recovery_metadata", wraps=module._read_recovery_metadata
            ) as read,
        ):
            coordinator = self.fixture.coordinator()
            try:
                first = coordinator.retained_unsuccessful_outcome(sid)
                self.assertIsNotNone(first)
                self.assertEqual(
                    [call.args[1] for call in read.call_args_list],
                    [
                        ("recovery", "recording.json"),
                        ("recovery", "interruption.json"),
                        ("manifest.json",),
                    ],
                )
                self.assertNotIn(sid, coordinator._verified)
                self.assertNotIn(sid, coordinator._session_snapshots)
                self.assertIsNone(coordinator._read_local_state()["active_session"])
            finally:
                coordinator.close()
        restarted = self.fixture.coordinator()
        try:
            self.assertEqual(restarted.retained_unsuccessful_outcome(sid), first)
            self.fixture.assert_network_operation_lock_available()
        finally:
            restarted.close()

    def test_corrupt_metadata_never_creates_outcome_or_blocks_startup(self):
        published = self.kill_after_publication()
        sid = published["session_id"]
        root = self.fixture.mountpoint / "recordings" / sid
        cases = [
            ("manifest.json", lambda x: x.update(session_id=str(uuid.uuid4()))),
            ("manifest.json", lambda x: x.update(volume_id=str(uuid.uuid4()))),
            ("manifest.json", lambda x: x.update(sealed=False)),
            ("manifest.json", lambda x: x["time"].update(duration_seconds=10**400)),
            ("manifest.json", lambda x: x["time"].update(duration_seconds=float("nan"))),
            ("manifest.json", lambda x: x.update(logs=[])),
            ("recovery/recording.json", lambda x: x.update(authority_epoch=str(uuid.uuid4()))),
            ("recovery/recording.json", lambda x: x["device"].update(device_id=str(uuid.uuid4()))),
            ("recovery/recording.json", lambda x: x["storage"].update(volume_id=str(uuid.uuid4()))),
            ("recovery/interruption.json", lambda x: x.update(saved_video_seconds=-1)),
            ("recovery/interruption.json", lambda x: x.update(saved_video_seconds=10**400)),
            ("recovery/interruption.json", lambda x: x.update(saved_frames=123456)),
        ]
        for name, mutate in cases:
            with self.subTest(name=name, mutate=mutate):
                path = root / name
                original = path.read_bytes()
                changed = json.loads(original)
                mutate(changed)
                path.write_text(json.dumps(changed))
                try:
                    coordinator = self.fixture.coordinator()
                    try:
                        self.assertIsNone(coordinator.retained_unsuccessful_outcome(sid))
                        self.assertNotIn(sid, coordinator._verified)
                    finally:
                        coordinator.close()
                finally:
                    path.write_bytes(original)
        coordinator = self.fixture.coordinator()
        try:
            self.assertIsNotNone(coordinator.retained_unsuccessful_outcome(sid))
        finally:
            coordinator.close()

    def test_unsafe_metadata_files_are_rejected_without_blocking(self):
        published = self.kill_after_publication()
        sid = published["session_id"]
        root = self.fixture.mountpoint / "recordings" / sid
        path = root / "recovery" / "recording.json"
        original = path.read_bytes()
        target = self.fixture.root / "external.json"
        target.write_bytes(original)
        for kind in ("symlink", "hardlink", "fifo", "oversize", "deep-json"):
            with self.subTest(kind=kind):
                path.unlink()
                if kind == "symlink":
                    path.symlink_to(target)
                elif kind == "hardlink":
                    os.link(target, path)
                elif kind == "fifo":
                    os.mkfifo(path)
                elif kind == "oversize":
                    path.write_bytes(b" " * (1024 * 1024 + 1))
                else:
                    path.write_text("[" * 2000 + "]" * 2000)
                try:
                    coordinator = self.fixture.coordinator()
                    try:
                        self.assertIsNone(coordinator.retained_unsuccessful_outcome(sid))
                    finally:
                        coordinator.close()
                finally:
                    path.unlink()
                    path.write_bytes(original)
        coordinator = self.fixture.coordinator()
        try:
            self.assertIsNotNone(coordinator.retained_unsuccessful_outcome(sid))
        finally:
            coordinator.close()

    def test_active_identity_is_durable_before_sources_start_and_clears_on_success(self):
        sources = fixtures.FakeSources()
        coordinator = self.fixture.coordinator(sources=sources)
        original_start = sources.start
        observed = []

        def start(*args, **kwargs):
            identity = coordinator._read_local_state()["active_session"]
            self.assertEqual(identity["session_id"], coordinator._active_plan.session_id)
            self.assertEqual(identity["volume_id"], coordinator.volume_id)
            self.assertEqual(identity["generation_id"], coordinator.generation_id)
            observed.append(identity)
            return original_start(*args, **kwargs)

        try:
            with patch.object(sources, "start", side_effect=start):
                coordinator.start_capture(fixtures.start_command("identity-start"))
            self.assertEqual(len(observed), 1)
            self.assertTrue(coordinator.submit_frame(fixtures.frame()))
            coordinator.stop_capture(fixtures.stop_command("identity-stop"))
            self.assertIsNone(coordinator._read_local_state()["active_session"])
        finally:
            coordinator.close()

    def test_archived_controls_survive_crash_before_publication_rename(self):
        published = self.kill_after_publication(before_rename=True)
        sid = published["session_id"]
        root = self.fixture.mountpoint / "recordings"
        self.assertTrue((root / (sid + ".partial") / "manifest.json").is_file())
        self.assertFalse((root / sid).exists())
        coordinator = self.fixture.coordinator()
        try:
            outcome = coordinator.retained_unsuccessful_outcome(sid)
            self.assertIsNotNone(outcome)
            self.assertEqual(
                outcome["outcome"]["recording_state"]["diagnostics"][0]["code"],
                "source_sequence_gap",
            )
            self.assertTrue((root / sid / "manifest.json").is_file())
            self.assertFalse((root / (sid + ".partial")).exists())
        finally:
            coordinator.close()

    def test_symlink_session_and_recovery_directories_are_not_followed(self):
        published = self.kill_after_publication()
        sid = published["session_id"]
        root = self.fixture.mountpoint / "recordings" / sid
        for path in (root, root / "recovery"):
            with self.subTest(path=path.name):
                moved = self.fixture.root / "moved-directory"
                path.rename(moved)
                path.symlink_to(moved, target_is_directory=True)
                try:
                    with patch.object(
                        module, "_read_recovery_metadata", wraps=module._read_recovery_metadata
                    ) as read:
                        coordinator = self.fixture.coordinator()
                        try:
                            self.assertIsNone(coordinator.retained_unsuccessful_outcome(sid))
                            if path == root:
                                read.assert_not_called()
                        finally:
                            coordinator.close()
                finally:
                    path.unlink()
                    moved.rename(path)
        coordinator = self.fixture.coordinator()
        try:
            self.assertIsNotNone(coordinator.retained_unsuccessful_outcome(sid))
        finally:
            coordinator.close()
