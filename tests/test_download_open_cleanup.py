import errno
import json
import sqlite3
import unittest
from unittest.mock import Mock, patch

from rp_ylx.recording import coordinator as module
from tests import test_capture_coordinator as fixtures


class Cancelled(BaseException):
    pass


class DownloadOpenCleanupTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CaptureCoordinatorTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.coordinator = self.fixture.coordinator()
        self.addCleanup(self.coordinator.close)
        self.sid = self.fixture.seal_one(self.coordinator)
        manifest = json.loads(
            (self.fixture.mountpoint / "recordings" / self.sid / "manifest.json").read_text()
        )
        self.artifact = manifest["video"]["segments"][0]["artifacts"]["left"]["artifact_id"]
        self.coordinator.list_sessions(cursor=None, limit=50, take_id=None)

    def open(self, kind):
        if kind == "manifest":
            return self.coordinator.open_manifest(self.sid, "v4")
        return self.coordinator.open_verified_artifact(self.sid, self.artifact, "v4")

    def test_store_creation_failure_returns_volume_reservation_and_allows_delete(self):
        for kind in ("manifest", "artifact"):
            for failure in (OSError(errno.EMFILE, "descriptor limit"), Cancelled()):
                with self.subTest(kind=kind, error=type(failure).__name__):
                    with (
                        patch.object(self.coordinator, "_open_store", side_effect=failure),
                        self.assertRaises(type(failure)),
                    ):
                        self.open(kind)
                    self.assertEqual(self.coordinator.open_handle_count, 0)
        result = self.coordinator.delete_sessions(
            self.fixture.deletion_command(self.coordinator, {self.sid})
        )
        self.assertEqual(result.body["deleted_session_ids"], [self.sid])

    def test_store_open_failure_or_cancellation_closes_store_and_releases(self):
        for kind in ("manifest", "artifact"):
            for failure in (OSError(errno.EIO, "read failed"), Cancelled()):
                with self.subTest(kind=kind, error=type(failure).__name__):
                    store = self.coordinator._open_store()
                    name = "open_manifest" if kind == "manifest" else "open_artifact"
                    with (
                        patch.object(self.coordinator, "_open_store", return_value=store),
                        patch.object(store, name, side_effect=failure),
                        patch.object(store, "close", wraps=store.close) as close,
                    ):
                        with self.assertRaises(type(failure)):
                            self.open(kind)
                        close.assert_called_once_with()
                    self.assertEqual(self.coordinator.open_handle_count, 0)

    def test_begin_receipt_failure_and_cancellation_close_file_and_release(self):
        for failure in (sqlite3.OperationalError("database is full"), Cancelled()):
            with self.subTest(error=type(failure).__name__):
                held = []
                original = module._MultiRootSessionStore.open_artifact

                def opened(store, *args, original=original, held=held):
                    representation = original(store, *args)
                    held.append(representation)
                    return representation

                with (
                    patch.object(module._MultiRootSessionStore, "open_artifact", opened),
                    patch.object(self.coordinator._state_db, "begin_transfer", side_effect=failure),
                    self.assertRaises(type(failure)),
                ):
                    self.open("artifact")
                self.assertEqual(len(held), 1)
                with self.assertRaises(RuntimeError):
                    held[0].read()
                self.assertEqual(self.coordinator.open_handle_count, 0)

    def test_finish_receipt_failure_still_releases_once(self):
        representation = self.open("artifact")
        self.assertEqual(self.coordinator.open_handle_count, 1)
        with (
            patch.object(
                self.coordinator._state_db,
                "finish_transfer",
                side_effect=sqlite3.OperationalError("database is full"),
            ) as finish,
            patch.object(module, "operational_logger") as log,
        ):
            representation.close()
            representation.close()
            finish.assert_called_once()
            log.return_value.event.assert_called_once()
        self.assertEqual(self.coordinator.open_handle_count, 0)
        with self.assertRaises(RuntimeError):
            representation.read()

    def test_store_close_failure_closes_unreturned_representation_and_receipt(self):
        for kind in ("manifest", "artifact"):
            with self.subTest(kind=kind):
                store = self.coordinator._open_store()
                original_close = store.close
                held = []
                name = "open_manifest" if kind == "manifest" else "open_artifact"
                original_open = getattr(store, name)

                def opened(*args, original_open=original_open, held=held):
                    representation = original_open(*args)
                    held.append(representation)
                    return representation

                def close(original_close=original_close):
                    original_close()
                    raise OSError(errno.EIO, "close failed")

                with (
                    patch.object(self.coordinator, "_open_store", return_value=store),
                    patch.object(store, name, opened),
                    patch.object(store, "close", close),
                    self.assertRaises(OSError),
                ):
                    self.open(kind)
                self.assertEqual(self.coordinator.open_handle_count, 0)
                self.assertTrue(held[0]._closed)
                self.assertEqual(held[0]._owned_descriptors, [])
                if kind == "artifact":
                    with self.assertRaises(RuntimeError):
                        held[0].read()
        with self.coordinator._state_db.connection() as db:
            self.assertEqual(db.execute("SELECT status FROM transfers").fetchall(), [("failed",)])

    def test_partial_multi_root_construction_closes_every_opened_root(self):
        first, second = Mock(), Mock()
        with (
            patch.object(
                module,
                "DirectorySessionStore",
                side_effect=[first, second, OSError(errno.EMFILE, "limit")],
            ),
            self.assertRaises(OSError),
        ):
            module._MultiRootSessionStore((self.fixture.mountpoint,) * 3, verified_manifests={})
        first.close.assert_called_once_with()
        second.close.assert_called_once_with()

    def test_multi_root_close_continues_after_error_and_is_idempotent(self):
        first, second = Mock(), Mock()
        first.close.side_effect = OSError(errno.EIO, "close failed")
        with patch.object(module, "DirectorySessionStore", side_effect=[first, second]):
            store = module._MultiRootSessionStore(
                (self.fixture.mountpoint,) * 2, verified_manifests={}
            )
        with self.assertRaises(OSError):
            store.close()
        first.close.assert_called_once_with()
        second.close.assert_called_once_with()
        store.close()
        first.close.assert_called_once_with()
        second.close.assert_called_once_with()
