import select
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock

from rp_ylx.network_state import NetworkStateStore
from rp_ylx.persistence import PersistenceError, StateDatabase
from tests import test_capture_coordinator as fixtures


class AuthorityRowsTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.store = StateDatabase(self.root / "state.sqlite3")
        self.legacy = self.root / "old.json"

    def dump(self, store=None):
        with (store or self.store).connection() as db:
            return tuple(db.iterdump())

    def delete(self, store, name):
        with store.connection() as db, db:
            db.execute("DELETE FROM documents WHERE name=?", (name,))

    def test_missing_imported_document_never_reimports_legacy(self):
        self.legacy.write_text('{"revision":42}')
        self.store.import_json("capture", self.legacy)
        self.delete(self.store, "capture")
        self.legacy.write_text('{"revision":0}')
        before = self.dump()
        with self.assertRaises(PersistenceError):
            self.store.import_json("capture", self.legacy)
        self.assertEqual(self.dump(), before)
        self.assertEqual(self.legacy.read_text(), '{"revision":0}')
        self.assertEqual(self.legacy.with_suffix(".json.pre-sqlite").read_text(), '{"revision":42}')

    def test_absent_legacy_commits_default_and_receipt_atomically(self):
        factory = Mock(return_value={"revision": 42})
        result = self.store.import_json("capture", self.legacy, default_factory=factory)
        self.assertEqual(result, {"revision": 42})
        self.assertEqual(self.store.get("capture"), result)
        with self.store.connection() as db:
            self.assertEqual(db.execute("SELECT name FROM imports").fetchall(), [("capture",)])
        self.legacy.write_text('{"revision":0}')
        self.assertEqual(self.store.import_json("capture", self.legacy), result)
        factory.assert_called_once_with()
        self.assertFalse(self.legacy.with_suffix(".json.pre-sqlite").exists())

    def test_default_failure_rolls_back_receipt_and_can_retry(self):
        before = self.dump()
        with self.assertRaisesRegex(RuntimeError, "interrupted default"):
            self.store.import_json(
                "capture",
                self.legacy,
                default_factory=Mock(side_effect=RuntimeError("interrupted default")),
            )
        self.assertEqual(self.dump(), before)
        self.assertEqual(self.store.import_json("capture", self.legacy), {})
        self.assertEqual(self.store.get("capture"), {})

    def test_process_death_between_state_and_receipt_or_commit_rolls_back_both(self):
        code = """
import sys, time
from contextlib import contextmanager
from pathlib import Path
from rp_ylx.persistence import StateDatabase
store = StateDatabase(Path(sys.argv[1]))
original = store.connection
def trace(sql):
    if sql.startswith(sys.argv[2]):
        print('transaction-open', flush=True)
        time.sleep(60)
@contextmanager
def connection():
    with original() as db:
        db.set_trace_callback(trace)
        yield db
store.connection = connection
store.import_json('capture', Path(sys.argv[1]).with_suffix('.json'))
"""
        for phase in ("INSERT INTO imports", "COMMIT"):
            with self.subTest(phase=phase):
                path = self.root / (phase.replace(" ", "-") + ".sqlite3")
                child = subprocess.Popen(
                    [sys.executable, "-c", code, str(path), phase],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                try:
                    ready, _, _ = select.select([child.stdout], [], [], 10)
                    self.assertTrue(ready, "child did not reach the uncommitted transaction")
                    self.assertEqual(child.stdout.readline(), b"transaction-open\n")
                    child.kill()
                    _, stderr = child.communicate(timeout=10)
                    self.assertLess(child.returncode, 0, stderr.decode())
                finally:
                    if child.poll() is None:
                        child.kill()
                    child.communicate(timeout=10)
                store = StateDatabase(path)
                with store.connection() as db:
                    self.assertEqual(db.execute("SELECT count(*) FROM documents").fetchone()[0], 0)
                    self.assertEqual(db.execute("SELECT count(*) FROM imports").fetchone()[0], 0)
                self.assertEqual(store.import_json("capture", path.with_suffix(".json")), {})
                self.assertEqual(store.get("capture"), {})

    def test_invalid_default_or_validation_rolls_back_all_rows(self):
        before = self.dump()
        with self.assertRaises(PersistenceError):
            self.store.import_json("capture", self.legacy, default_factory=lambda: [])
        self.assertEqual(self.dump(), before)
        with self.assertRaisesRegex(ValueError, "invalid generated state"):
            self.store.import_json(
                "capture",
                self.legacy,
                default_factory=lambda: {"revision": 0},
                validate=Mock(side_effect=ValueError("invalid generated state")),
            )
        self.assertEqual(self.dump(), before)

    def test_concurrent_first_imports_share_one_default(self):
        factory = Mock(return_value={"epoch": "same"})
        with ThreadPoolExecutor(max_workers=8) as pool:
            values = list(
                pool.map(
                    lambda _: self.store.import_json(
                        "capture", self.legacy, default_factory=factory
                    ),
                    range(8),
                )
            )
        self.assertEqual(values, [{"epoch": "same"}] * 8)
        factory.assert_called_once_with()

    def test_existing_document_does_not_evaluate_defaults(self):
        self.store.initialize_document("capture", {"revision": 42}, "test")
        before = self.dump()
        factory = Mock(side_effect=AssertionError("must not rebuild defaults"))
        self.assertEqual(
            self.store.import_json("capture", self.legacy, default_factory=factory),
            {"revision": 42},
        )
        factory.assert_not_called()
        self.assertEqual(self.dump(), before)

    def test_network_restart_refuses_lost_authority_and_old_receipt_only(self):
        for kind in ("previous_state", "old_receipt_only"):
            with self.subTest(kind=kind):
                root = self.root / kind
                if kind == "previous_state":
                    network = NetworkStateStore(root)
                    state = network._database
                    self.delete(state, "controller")
                else:
                    state = StateDatabase(root / "controller.sqlite3")
                    with state.connection() as db, db:
                        db.execute("INSERT INTO imports VALUES ('controller','old-missing.json')")
                before = self.dump(state)
                with self.assertRaises(PersistenceError):
                    NetworkStateStore(root)
                self.assertEqual(self.dump(state), before)

    def test_capture_restart_refuses_lost_authority(self):
        fixture = fixtures.CaptureCoordinatorTest()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        coordinator = fixture.coordinator()
        coordinator.close()
        state = StateDatabase(fixture.state_root / "state.sqlite3")
        self.delete(state, "capture")
        before = self.dump(state)
        reopened = None
        try:
            with self.assertRaises(PersistenceError):
                reopened = fixture.coordinator()
        finally:
            if reopened is not None:
                reopened.close()
        self.assertEqual(self.dump(state), before)

    def test_writes_cannot_recreate_a_missing_imported_row(self):
        for operation in ("put", "history", "capture"):
            with self.subTest(operation=operation):
                state = StateDatabase(self.root / (operation + ".sqlite3"))
                state.initialize_document("capture", {"revision": 42}, "bootstrap")
                self.delete(state, "capture")
                before = self.dump(state)
                with self.assertRaises(PersistenceError):
                    if operation == "capture":
                        state.put_capture_state({"revision": 0}, [], set())
                    else:
                        state.put("capture", {"revision": 0}, history=operation == "history")
                self.assertEqual(self.dump(state), before)

    def test_capture_read_does_not_treat_missing_authority_as_empty_state(self):
        self.store.initialize_document("capture", {}, "bootstrap")
        self.assertEqual(self.store.capture_state(), {"commands": []})
        self.delete(self.store, "capture")
        before = self.dump()
        with self.assertRaises(PersistenceError):
            self.store.capture_state()
        self.assertEqual(self.dump(), before)
