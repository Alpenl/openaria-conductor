import json
import multiprocessing
import os
import stat
import threading
import traceback
import unittest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import patch

from rp_ylx import settings
from rp_ylx.persistence import PersistenceError
from tests import test_settings_presets as fixtures


class SettingsExportTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.SettingsPresetTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root = self.fixture.root
        self.destination = self.root / "export.json"
        self.before = self.fixture.snapshot()
        self.addCleanup(lambda: self.assertEqual(self.fixture.snapshot(), self.before))

    def export(self):
        return settings.manage(self.fixture.config, "export", destination=self.destination)

    def staging(self):
        return list(self.destination.parent.glob(".openaria-settings-*.tmp"))

    def assert_export(self):
        self.assertEqual(json.loads(self.destination.read_bytes()), self.fixture.initial)
        self.assertEqual(stat.S_IMODE(self.destination.stat().st_mode), 0o600)

    def fails_cleanly(self, injection, error):
        descriptors = len(os.listdir("/proc/self/fd"))
        with injection, self.assertRaises(error):
            self.export()
        self.assertFalse(os.path.lexists(self.destination))
        self.assertEqual(self.staging(), [])
        self.assertEqual(len(os.listdir("/proc/self/fd")), descriptors)
        self.assertEqual(self.export(), {"export": str(self.destination)})
        self.assert_export()

    def test_create_mode_zero_is_corrected_before_publication(self):
        original = os.open

        def restrictive(path, flags, *args, **kwargs):
            fd = original(path, flags, *args, **kwargs)
            if flags & os.O_CREAT and str(path).startswith(".openaria-settings-"):
                os.fchmod(fd, 0)
            return fd

        with patch.object(settings.os, "open", side_effect=restrictive):
            self.export()
        self.assert_export()

    def test_permission_setting_failure_cleans_staging_and_allows_retry(self):
        self.fails_cleanly(
            patch.object(settings.os, "fchmod", side_effect=PermissionError("mode denied")),
            PermissionError,
        )

    def test_ignored_permission_change_is_rejected_before_publication(self):
        original_open, original_chmod = os.open, os.fchmod

        def restrictive(path, flags, *args, **kwargs):
            fd = original_open(path, flags, *args, **kwargs)
            if flags & os.O_CREAT and str(path).startswith(".openaria-settings-"):
                original_chmod(fd, 0)
            return fd

        descriptors = len(os.listdir("/proc/self/fd"))
        with (
            patch.object(settings.os, "open", side_effect=restrictive),
            patch.object(settings.os, "fchmod", return_value=None),
            self.assertRaises(PersistenceError),
        ):
            self.export()
        self.assertFalse(self.destination.exists())
        self.assertEqual(self.staging(), [])
        self.assertEqual(len(os.listdir("/proc/self/fd")), descriptors)
        self.export()
        self.assert_export()

    def test_stream_allocation_failure_closes_fd_and_allows_retry(self):
        self.fails_cleanly(
            patch.object(settings.os, "fdopen", side_effect=MemoryError()), MemoryError,
        )

    def test_partial_json_write_failure_leaves_no_final_file(self):
        def partial(value, stream, **kwargs):
            stream.write('{"camera":')
            raise OSError("partial JSON write")
        self.fails_cleanly(patch.object(settings.json, "dump", side_effect=partial), OSError)

    def test_cancelled_json_write_cleans_staging_and_allows_retry(self):
        def cancelled(value, stream, **kwargs):
            stream.write('{"camera":')
            raise KeyboardInterrupt()
        self.fails_cleanly(
            patch.object(settings.json, "dump", side_effect=cancelled), KeyboardInterrupt,
        )

    def test_file_sync_failure_closes_fds_and_allows_retry(self):
        self.fails_cleanly(patch.object(settings.os, "fsync", side_effect=OSError("file sync")),
                           OSError)

    def test_stream_entry_failure_does_not_leak_fd(self):
        class FailedEntry:
            def __enter__(self):
                raise MemoryError("stream enter")

            def __exit__(self, *args):
                return False
        self.fails_cleanly(patch.object(settings.os, "fdopen", return_value=FailedEntry()),
                           MemoryError)

    def test_stream_close_failure_does_not_publish_or_leak_fd(self):
        original = os.fdopen

        class FailedClose:
            def __init__(self, *args, **kwargs):
                self.stream = original(*args, **kwargs)

            def __enter__(self):
                return self.stream

            def __exit__(self, *args):
                self.stream.close()
                raise OSError("stream close")
        self.fails_cleanly(patch.object(settings.os, "fdopen", side_effect=FailedClose), OSError)

    def test_link_failure_cleans_staging_and_allows_retry(self):
        self.fails_cleanly(patch.object(settings.os, "link", side_effect=OSError("link failed")),
                           OSError)

    def test_non_ascii_values_are_utf8_and_resolved(self):
        self.fixture.set_patch({"audio": {"device": "测试声卡"}})
        self.fixture.initial = settings.manage(self.fixture.config, "show")["settings"]
        self.before = self.fixture.snapshot()
        self.export()
        self.assertIn("测试声卡".encode(), self.destination.read_bytes())
        self.assert_export()
        self.assertEqual(self.staging(), [])

    def test_existing_destinations_are_never_overwritten(self):
        original = self.root / "original"
        original.write_bytes(b"protected")
        for kind in ("file", "symlink", "dangling", "fifo", "directory"):
            with self.subTest(kind=kind):
                if kind == "file":
                    self.destination.write_bytes(b"existing")
                elif kind in ("symlink", "dangling"):
                    self.destination.symlink_to(
                        original if kind == "symlink" else self.root/"absent",
                    )
                elif kind == "fifo":
                    os.mkfifo(self.destination)
                else:
                    self.destination.mkdir()
                before = self.destination.lstat()
                try:
                    with self.assertRaises(FileExistsError):
                        self.export()
                    after = self.destination.lstat()
                    self.assertEqual((before.st_ino, before.st_mode), (after.st_ino, after.st_mode))
                    self.assertEqual(original.read_bytes(), b"protected")
                    self.assertEqual(self.staging(), [])
                finally:
                    if kind == "directory":
                        self.destination.rmdir()
                    else:
                        self.destination.unlink()

    def test_competing_final_name_is_preserved(self):
        original = json.dump

        def competing(*args, **kwargs):
            original(*args, **kwargs)
            self.destination.write_bytes(b"other exporter")

        with (
            patch.object(settings.json, "dump", side_effect=competing),
            self.assertRaises(FileExistsError),
        ):
            self.export()
        self.assertEqual(self.destination.read_bytes(), b"other exporter")
        self.assertEqual(self.staging(), [])

    def test_parallel_exports_publish_exactly_one_complete_file(self):
        barrier = threading.Barrier(2)
        original = json.dump

        def gated(*args, **kwargs):
            original(*args, **kwargs)
            barrier.wait(5)

        def attempt():
            try:
                self.export()
                return "published"
            except FileExistsError:
                return "exists"

        with patch.object(settings.json, "dump", side_effect=gated), ThreadPoolExecutor(2) as pool:
            outcomes = list(pool.map(lambda _: attempt(), range(2)))
        self.assertEqual(sorted(outcomes), ["exists", "published"])
        self.assert_export()
        self.assertEqual(self.staging(), [])

    def test_partial_export_is_not_visible_at_final_name(self):
        entered, release = threading.Event(), threading.Event()
        original = json.dump
        errors = []

        def slow(value, stream, **kwargs):
            stream.write('{"camera":')
            stream.flush()
            entered.set()
            if not release.wait(5):
                raise TimeoutError("export test gate")
            stream.seek(0)
            stream.truncate()
            original(value, stream, **kwargs)

        def run():
            try:
                self.export()
            except BaseException as error:
                errors.append(error)

        with patch.object(settings.json, "dump", side_effect=slow):
            worker = threading.Thread(target=run)
            worker.start()
            try:
                self.assertTrue(entered.wait(3))
                self.assertFalse(os.path.lexists(self.destination))
                staging = self.staging()
                self.assertEqual(len(staging), 1)
                self.assertEqual(staging[0].read_bytes(), b'{"camera":')
            finally:
                release.set()
                worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertFalse(errors, errors)
        self.assert_export()

    def test_directory_sync_failure_retains_complete_published_file(self):
        original = os.fsync

        def sync(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                raise OSError("directory sync failed")
            return original(fd)

        with (
            patch.object(settings.os, "fsync", side_effect=sync),
            self.assertRaisesRegex(OSError, "directory sync"),
        ):
            self.export()
        self.assert_export()
        self.assertEqual(self.staging(), [])
        with self.assertRaises(FileExistsError):
            self.export()

    def test_replaced_staging_before_publication_is_preserved_and_rejected(self):
        original = json.dump

        def replace(*args, **kwargs):
            original(*args, **kwargs)
            staging = self.staging()[0]
            staging.unlink()
            staging.write_bytes(b"foreign staging")

        with (
            patch.object(settings.json, "dump", side_effect=replace),
            self.assertRaises(PersistenceError),
        ):
            self.export()
        self.assertFalse(self.destination.exists())
        self.assertEqual(self.staging()[0].read_bytes(), b"foreign staging")

    def test_fd_publication_cannot_publish_replacement_staging(self):
        original = os.link

        def replace_then_link(*args, **kwargs):
            staging = self.staging()[0]
            staging.rename(self.root/"moved-original")
            staging.write_bytes(b"foreign staging")
            return original(*args, **kwargs)

        with patch.object(settings.os, "link", side_effect=replace_then_link):
            self.export()
        self.assert_export()
        self.assertEqual(self.staging()[0].read_bytes(), b"foreign staging")

    def test_parent_replacement_before_publication_is_rejected(self):
        parent = self.root/"out"
        parent.mkdir()
        self.destination = parent/"export.json"
        original = json.dump

        def replace(*args, **kwargs):
            original(*args, **kwargs)
            parent.rename(self.root/"moved")
            parent.mkdir()

        with (
            patch.object(settings.json, "dump", side_effect=replace),
            self.assertRaises(PersistenceError),
        ):
            self.export()
        self.assertEqual(list(parent.iterdir()), [])
        self.assertEqual(list((self.root/"moved").iterdir()), [])

    def test_target_replacement_after_link_is_not_removed_or_reported_success(self):
        original = os.link

        def replaced(*args, **kwargs):
            original(*args, **kwargs)
            self.destination.unlink()
            self.destination.write_bytes(b"other writer")

        with (
            patch.object(settings.os, "link", side_effect=replaced),
            self.assertRaises(PersistenceError),
        ):
            self.export()
        self.assertEqual(self.destination.read_bytes(), b"other writer")
        self.assertEqual(self.staging(), [])

    def test_parent_replacement_after_publication_preserves_completed_copy(self):
        parent = self.root/"out"
        parent.mkdir()
        self.destination = parent/"export.json"
        original = os.link

        def replaced(*args, **kwargs):
            original(*args, **kwargs)
            parent.rename(self.root/"moved")
            parent.mkdir()

        with (
            patch.object(settings.os, "link", side_effect=replaced),
            self.assertRaises(PersistenceError),
        ):
            self.export()
        self.assertEqual(list(parent.iterdir()), [])
        self.assertEqual(json.loads((self.root/"moved/export.json").read_bytes()),
                         self.fixture.initial)
        self.assertEqual(list((self.root/"moved").glob(".openaria-settings-*.tmp")), [])

    def test_staging_name_collision_preserves_other_file(self):
        staging = self.root/".openaria-settings-collision.tmp"
        staging.write_bytes(b"not ours")
        with (
            patch.object(settings.uuid, "uuid4", return_value=SimpleNamespace(hex="collision")),
            self.assertRaises(FileExistsError),
        ):
            self.export()
        self.assertEqual(staging.read_bytes(), b"not ours")
        self.assertFalse(self.destination.exists())

    def test_cleanup_failure_preserves_primary_error_and_closes_fds(self):
        descriptors = len(os.listdir("/proc/self/fd"))
        with (
            patch.object(settings.os, "fdopen", side_effect=MemoryError("primary failure")),
            patch.object(settings.os, "unlink", side_effect=PermissionError("cleanup denied")),
            self.assertRaisesRegex(MemoryError, "primary failure"),
        ):
            self.export()
        self.assertFalse(self.destination.exists())
        self.assertEqual(len(os.listdir("/proc/self/fd")), descriptors)
        staging = self.staging()
        self.assertEqual(len(staging), 1)
        self.assertEqual(stat.S_IMODE(staging[0].stat().st_mode), 0o600)
        self.export()
        self.assert_export()


class SettingsExportCrashTest(unittest.TestCase):
    def crash(self, after):
        fixture = fixtures.SettingsPresetTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        destination = fixture.root/"export.json"
        context = multiprocessing.get_context("fork")
        receive, send = context.Pipe(duplex=False)
        self.addCleanup(receive.close)
        self.addCleanup(send.close)

        def child():
            try:
                original = os.link

                def link(*args, **kwargs):
                    if after:
                        original(*args, **kwargs)
                    send.send({"boundary": "publication"})
                    threading.Event().wait()

                with patch.object(settings.os, "link", side_effect=link):
                    settings.manage(fixture.config, "export", destination=destination)
            except BaseException:
                send.send({"error": traceback.format_exc()})
                raise

        worker = context.Process(target=child)
        worker.start()
        try:
            self.assertTrue(receive.poll(5))
            self.assertEqual(receive.recv(), {"boundary": "publication"})
            worker.kill()
            worker.join(5)
            self.assertEqual(worker.exitcode, -9)
        finally:
            if worker.is_alive():
                worker.kill()
                worker.join(5)
            worker.close()
        if after:
            self.assertEqual(json.loads(destination.read_bytes()), fixture.initial)
            with self.assertRaises(FileExistsError):
                settings.manage(fixture.config, "export", destination=destination)
        else:
            self.assertFalse(destination.exists())
            settings.manage(fixture.config, "export", destination=destination)
            self.assertEqual(json.loads(destination.read_bytes()), fixture.initial)
        self.assertEqual(settings.manage(fixture.config, "show")["settings"], fixture.initial)

    def test_kill_before_publication_has_no_final_and_retry_succeeds(self):
        self.crash(False)

    def test_kill_after_publication_leaves_only_complete_final_content(self):
        self.crash(True)
