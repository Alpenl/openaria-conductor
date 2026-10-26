"""Invalid recovery controls must not poison persisted capture outcomes."""

import copy
import json
import os
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

from rp_ylx.api.events import validate_capture_status, validate_retained_unsuccessful_outcome
from rp_ylx.recording import coordinator as module
from tests import test_capture_coordinator as fixtures


class PartialStateAdmissionTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)

    def interrupted(self):
        first = self.fixture.coordinator()
        first.start_capture(fixtures.start_command("state-admission-start"))
        sid = self.fixture.active_session_id(first)
        active = copy.deepcopy(first._active.current_recording_state)
        first.close()
        durable = first._state_db.get("capture")
        trusted = copy.deepcopy(durable["retained"][sid])
        path = self.fixture.mountpoint / "recordings" / (sid + ".partial") / "recording.json"
        return sid, path, active, trusted, durable, first._state_db

    def test_invalid_partial_state_never_replaces_trusted_outcome(self):
        sid, path, active, trusted, durable, database = self.interrupted()
        cases = [
            ("state-list", lambda value: value.update(state=[])),
            ("missing-fields", lambda value: (value.clear(), value.update(state="failed"))),
            (
                "nonfinite-progress",
                lambda value: value["progress"].update(elapsed_seconds=float("nan")),
            ),
            ("bad-diagnostics", lambda value: value.update(diagnostics={"bad": True})),
            ("wrong-session", lambda value: value.update(session_id=module.uuid7())),
            ("wrong-volume", lambda value: value["storage"].update(volume_id=str(uuid.uuid4()))),
            ("negative-revision", lambda value: value.update(state_revision=-10)),
            ("unknown-field", lambda value: value.update(unrecognized="bad")),
        ]
        for name, change in cases:
            with self.subTest(case=name):
                database.put_capture_state(copy.deepcopy(durable), [], set())
                value = copy.deepcopy(active)
                change(value)
                payload = json.dumps(value).encode()
                path.write_bytes(payload)
                restarted = self.fixture.coordinator()
                try:
                    self.assertEqual(restarted._retained[sid], trusted)
                    validate_capture_status(restarted.capture_status())
                    validate_retained_unsuccessful_outcome(
                        restarted.retained_unsuccessful_outcome(sid), session_id=sid
                    )
                    self.assertEqual(path.read_bytes(), payload)
                    self.assertEqual(restarted._state_db.get("capture")["retained"][sid], trusted)
                finally:
                    restarted.close()

    def test_invalid_sqlite_retained_rows_are_removed_without_losing_good_history(self):
        sid, path, active, trusted, durable, database = self.interrupted()
        bad_sid = module.uuid7()
        bad_state = copy.deepcopy(trusted["recording_state"])
        bad_state["session_id"] = bad_sid
        cases = [
            {},
            {"generation_id": str(uuid.uuid4()), "recording_state": []},
            {"generation_id": "invalid", "recording_state": bad_state},
            {"generation_id": str(uuid.uuid4()), "recording_state": trusted["recording_state"]},
        ]
        for malformed in cases:
            with self.subTest(value=malformed):
                value = copy.deepcopy(durable)
                value["retained"][bad_sid] = malformed
                database.put_capture_state(value, [], set())
                restarted = self.fixture.coordinator()
                try:
                    self.assertIsNone(restarted.retained_unsuccessful_outcome(bad_sid))
                    self.assertEqual(restarted._retained[sid], trusted)
                    validate_capture_status(restarted.capture_status())
                    self.assertNotIn(bad_sid, restarted._state_db.get("capture")["retained"])
                finally:
                    restarted.close()

    def test_fifo_recording_control_cannot_block_service_startup(self):
        script = r"""
import os
from tests import test_capture_coordinator as fixtures
case = fixtures.CaptureCoordinatorTest()
case.setUp()
try:
    coordinator = case.coordinator()
    coordinator.start_capture(fixtures.start_command("fifo-state"))
    sid = case.active_session_id(coordinator)
    coordinator.close()
    path = case.mountpoint / "recordings" / (sid + ".partial") / "recording.json"
    path.unlink()
    os.mkfifo(path)
    print("ready", flush=True)
    restarted = case.coordinator()
    restarted.close()
finally:
    case.tearDown()
print("started")
"""
        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=5
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), ["ready", "started"])

    def test_linked_oversize_and_deep_controls_leave_trusted_history_unchanged(self):
        sid, path, active, trusted, durable, database = self.interrupted()
        original = json.dumps(active).encode()
        external = self.fixture.root / "external-state.json"
        external.write_bytes(original)
        for kind in ("symlink", "hardlink", "oversize", "deep-json"):
            with self.subTest(kind=kind):
                database.put_capture_state(copy.deepcopy(durable), [], set())
                path.unlink()
                if kind == "symlink":
                    path.symlink_to(external)
                elif kind == "hardlink":
                    os.link(external, path)
                elif kind == "oversize":
                    path.write_bytes(original + b" " * (8 * 1024 * 1024))
                else:
                    path.write_text("[" * 2000 + "]" * 2000)
                before = path.lstat()
                restarted = self.fixture.coordinator()
                try:
                    self.assertEqual(restarted._retained[sid], trusted)
                    self.assertEqual(
                        (path.lstat().st_ino, path.lstat().st_size), (before.st_ino, before.st_size)
                    )
                    self.assertEqual(external.read_bytes(), original)
                    validate_capture_status(restarted.capture_status())
                finally:
                    restarted.close()

    def test_valid_historical_authority_and_device_identity_are_preserved(self):
        sid, path, active, trusted, durable, database = self.interrupted()
        historical = copy.deepcopy(trusted)
        historical["recording_state"]["authority_epoch"] = str(uuid.uuid4())
        historical["recording_state"]["device"]["device_id"] = str(uuid.uuid4())
        capture_path = path.parent / "capture.json"
        capture = json.loads(capture_path.read_bytes())
        capture["config"]["device_id"] = historical["recording_state"]["device"]["device_id"]
        capture_path.write_text(json.dumps(capture))
        durable["retained"] = {}
        database.put_capture_state(durable, [], set())
        path.write_text(json.dumps(historical["recording_state"]))
        for _ in range(2):
            restarted = self.fixture.coordinator()
            try:
                self.assertEqual(restarted._retained[sid], historical)
                validate_retained_unsuccessful_outcome(
                    restarted.retained_unsuccessful_outcome(sid), session_id=sid
                )
                validate_capture_status(restarted.capture_status())
            finally:
                restarted.close()

    def test_bad_control_diagnostics_are_replaced_in_verified_prefix_receipt(self):
        from rp_ylx.recording.recovery import recover_device_session
        from tests import test_recording_recovery as recovery_fixtures

        for kind in ("nonfinite-diagnostic", "wrong-session", "wrong-volume", "missing-field"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temp:
                recorder = recovery_fixtures.RecordingRecoveryTests().recording(Path(temp))
                partial = recorder.partial_path
                index = (partial / "frames.ndjson").read_bytes()
                state = json.loads((partial / "recording.json").read_bytes())
                if kind == "nonfinite-diagnostic":
                    state["diagnostics"] = [{"message": float("nan")}]
                elif kind == "wrong-session":
                    state["session_id"] = module.uuid7()
                elif kind == "wrong-volume":
                    state["storage"]["volume_id"] = str(uuid.uuid4())
                else:
                    del state["schema"]
                raw = json.dumps(state).encode()
                (partial / "recording.json").write_bytes(raw)
                recovered = recover_device_session(partial)
                self.assertIsNotNone(recovered)
                verified = fixtures.validate_device_session_directory(recovered.path)
                self.assertEqual(verified["frames"]["count"], 30)
                self.assertEqual((recovered.path / "frames.ndjson").read_bytes(), index)
                self.assertEqual((recovered.path / "recovery/recording.json").read_bytes(), raw)
                receipt = json.loads((recovered.path / "recovery/interruption.json").read_bytes())
                self.assertEqual(receipt["diagnostics"][0]["code"], "process_interrupted")

    def test_valid_historical_volume_state_uses_its_capture_plan(self):
        sid, path, active, trusted, durable, database = self.interrupted()
        historical = copy.deepcopy(trusted)
        historical_volume = str(uuid.uuid4())
        historical["recording_state"]["storage"]["volume_id"] = historical_volume
        capture_path = path.parent / "capture.json"
        capture = json.loads(capture_path.read_bytes())
        capture["plan"]["volume_id"] = historical_volume
        capture_path.write_text(json.dumps(capture))
        path.write_text(json.dumps(historical["recording_state"]))
        durable["retained"] = {}
        database.put_capture_state(durable, [], set())
        restarted = self.fixture.coordinator()
        try:
            self.assertNotEqual(restarted.volume_id, historical_volume)
            self.assertEqual(restarted._retained[sid], historical)
            validate_retained_unsuccessful_outcome(
                restarted.retained_unsuccessful_outcome(sid), session_id=sid
            )
            validate_capture_status(restarted.capture_status())
        finally:
            restarted.close()
