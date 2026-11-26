"""Round 2 unit and regression tests for extracted recording modules and coordinator concurrency."""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import rp_ylx.recording.backing_storage as backing_storage
import rp_ylx.recording.deletion as deletion
import rp_ylx.recording.fs_safety as fs_safety
import rp_ylx.recording.recovery_orchestrator as recovery_orchestrator
import rp_ylx.recording.session_catalog as session_catalog
from rp_ylx.api.gateway import NetworkCommand, NetworkCredentialCommand, ProviderError
from rp_ylx.network_control import NetworkControlClientError
from rp_ylx.recording import CoordinatorConfig, DeviceRecordingError, DeviceSessionConfig
from rp_ylx.recording.device_session import SessionPlan, StorageStatus, uuid7
from rp_ylx.recording.hardware_proxies import CaptureLeaseManager, NetworkControllerProxy
from rp_ylx.recording.lifecycle import CaptureLifecycle, CapturePhase
from rp_ylx.recording.volume import VolumeAdmission, VolumeManager, default_mount_identity
from tests.testkit.coordinator import CoordinatorHarness, start_command, stop_command
from tests.testkit.media import FakeSources, frame, imu_observation


def _session_plan(session_id: str | None = None) -> SessionPlan:
    return SessionPlan(
        session_id=session_id or uuid7(),
        volume_id="550e8400-e29b-41d4-a716-446655440000",
        generation_id="550e8400-e29b-41d4-a716-446655440001",
        capture_mode="production",
        display_name="Round2 unit",
        take_id=uuid7(),
        take_sequence=1,
        continuation_of=None,
    )


def _coordinator_config(root: Path) -> CoordinatorConfig:
    return CoordinatorConfig(
        mountpoint=root / "vol",
        state_root=root / "state",
        session=DeviceSessionConfig(
            device_id="550e8400-e29b-41d4-a716-446655440000",
            device_label="YLX-1234ABCD",
            hardware_fingerprint="sha256:" + "a" * 64,
            platform="D-Robotics RDK X5 V1.0 + YLX 2UQ2",
            software_version="0.5.0",
            commit="a" * 40,
            width=3840,
            height=1080,
            sensor_fps=30.0,
            frame_decimation=1,
        ),
        minimum_available_bytes=1024,
        minimum_available_inodes=10,
    )


class LifecycleUnitTests(unittest.TestCase):
    def test_all_capture_phase_states_and_require_idle_for_settings(self) -> None:
        lifecycle = CaptureLifecycle()
        self.assertEqual(lifecycle.sync_phase(), CapturePhase.IDLE)
        lifecycle.require_idle_for_settings("ok")

        # RELEASED
        lifecycle._released = True
        self.assertEqual(lifecycle.sync_phase(), CapturePhase.RELEASED)
        with self.assertRaises(ProviderError):
            lifecycle.require_idle_for_settings("released")
        lifecycle._released = False

        # SAFE_SWAP_PENDING
        self.assertEqual(
            lifecycle.sync_phase(pending_safe_swap={"session_id": "s"}),
            CapturePhase.SAFE_SWAP_PENDING,
        )
        with self.assertRaises(ProviderError):
            lifecycle.require_idle_for_settings("swap", pending_safe_swap={"session_id": "s"})

        # RECORDING
        lifecycle._active = object()  # type: ignore[assignment]
        self.assertEqual(lifecycle.sync_phase(), CapturePhase.RECORDING)

        # COMMITTING_STATE
        lifecycle._pending_stop_state_commit = ("p", "stop", "k")
        self.assertEqual(lifecycle.sync_phase(), CapturePhase.COMMITTING_STATE)

        # STOPPING
        lifecycle._stop_inflight = ("p", "stop", "k2")
        self.assertEqual(lifecycle.sync_phase(), CapturePhase.STOPPING)

        # RECOVERING_FAILURE
        lifecycle._failure_inflight = object()  # type: ignore[assignment]
        self.assertEqual(lifecycle.sync_phase(), CapturePhase.RECOVERING_FAILURE)

        # DELETING
        lifecycle._deleting = True
        self.assertEqual(lifecycle.sync_phase(), CapturePhase.DELETING)

        # STARTING
        lifecycle._starting = True
        self.assertEqual(lifecycle.sync_phase(), CapturePhase.STARTING)

        # CLOSING
        lifecycle._closing = True
        self.assertEqual(lifecycle.sync_phase(), CapturePhase.CLOSING)

        # Settings update gate even when phase would otherwise be IDLE
        fresh = CaptureLifecycle()
        fresh._settings_updating = True
        self.assertFalse(fresh.is_idle())
        with self.assertRaises(ProviderError):
            fresh.require_idle_for_settings("settings busy")


class FsSafetyUnitTests(unittest.TestCase):
    def test_copy_regular_atomic_and_exfat_helpers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "src.txt"
            dst = root / "dst.txt"
            missing = root / "missing.txt"
            self.assertFalse(fs_safety.copy_regular_atomic(missing, dst))

            src.write_bytes(b"hello-world")
            self.assertTrue(fs_safety.copy_regular_atomic(src, dst, mode=0o640))
            self.assertEqual(dst.read_bytes(), b"hello-world")

            sym = root / "sym.txt"
            sym.symlink_to(src)
            with self.assertRaises(OSError):
                fs_safety.copy_regular_atomic(sym, root / "out.txt")

            fd = os.open(dst, os.O_RDONLY)
            try:
                with (
                    patch("rp_ylx.core.fs._fstatfs_type", return_value=None),
                    patch("rp_ylx.core.fs.os.fstatvfs", return_value=SimpleNamespace(f_files=0)),
                ):
                    self.assertTrue(fs_safety.is_exfat_or_msdos_fd(fd))
                    self.assertTrue(fs_safety.is_exfat_or_msdos_path(dst))
            finally:
                os.close(fd)

    def test_write_zstd_frame_checked_handles_short_pread(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "frame.zst"
            compressor = fs_safety.make_zstd_compressor()
            payload = b"incremental metadata line\n" * 16
            real_pread = os.pread

            def short_pread(fd: int, length: int, offset: int) -> bytes:
                return real_pread(fd, min(3, length), offset)

            with (
                path.open("w+b") as stream,
                patch.object(fs_safety.os, "pread", side_effect=short_pread),
            ):
                encoded = fs_safety.write_zstd_frame_checked(
                    stream, compressor, payload, verify_pread=True
                )
            self.assertGreater(len(encoded), 0)

            # Empty pread raises OSError
            with (
                path.open("w+b") as stream,
                patch.object(fs_safety.os, "pread", return_value=b""),
                self.assertRaises(OSError),
            ):
                fs_safety.write_zstd_frame_checked(
                    stream, fs_safety.make_zstd_compressor(), payload, verify_pread=True
                )


class SessionCatalogUnitTests(unittest.TestCase):
    def test_decode_session_cursor_rejects_oversize_tamper_bool_and_noncanonical(self) -> None:
        with self.assertRaises(ProviderError):
            session_catalog.decode_session_cursor("a" * 2049)

        valid = session_catalog.encode_session_cursor(
            catalog_revision="sha256:" + "a" * 64,
            take_id=None,
            offset=10,
        )
        decoded = session_catalog.decode_session_cursor(valid)
        self.assertEqual(decoded["offset"], 10)

        # Tampered last character
        tampered = valid[:-1] + ("A" if valid[-1] != "A" else "B")
        with self.assertRaises(ProviderError):
            session_catalog.decode_session_cursor(tampered)

        # Signed bool offset or non-canonical JSON payload
        import base64

        for bad_payload in (
            json.dumps(
                {
                    "schema": session_catalog.SESSION_CURSOR_SCHEMA,
                    "catalog_revision": "sha256:" + "a" * 64,
                    "take_id": None,
                    "offset": True,
                },
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8"),
            json.dumps(
                {
                    "schema": session_catalog.SESSION_CURSOR_SCHEMA,
                    "catalog_revision": "sha256:" + "a" * 64,
                    "take_id": None,
                    "offset": 10,
                },
                indent=2,
            ).encode("utf-8"),
        ):
            sig = hashlib.sha256(session_catalog.SESSION_CURSOR_DOMAIN + bad_payload).digest()
            token = base64.urlsafe_b64encode(bad_payload + sig).rstrip(b"=").decode("ascii")
            with self.assertRaises(ProviderError):
                session_catalog.decode_session_cursor(token)

    def test_tracked_representation_merges_out_of_order_ranges(self) -> None:
        class _MemRep:
            etag = '"abc"'
            size = 100
            content_type = "application/octet-stream"

            def close(self) -> None:
                return

            def read(self, offset: int = 0, length: int | None = None) -> bytes:
                del offset
                return b"x" * (0 if length is None else length)

            def iter_chunks(
                self, offset: int = 0, length: int | None = None, *, chunk_size: int = 1
            ):
                del offset, length, chunk_size
                return iter(())

            def send_to(self, output_descriptor: int, offset: int = 0, length: int | None = None):
                del output_descriptor, offset, length
                return 0

        outcomes: list[tuple[str, int]] = []
        tracked = session_catalog._TrackedRepresentation(
            _MemRep(),  # type: ignore[arg-type]
            lambda: None,
            on_close=lambda status, n: outcomes.append((status, n)),
        )
        tracked.read(60, 40)
        tracked.read(20, 45)
        tracked.read(0, 25)
        tracked.close()
        self.assertEqual(outcomes, [("served", 110)])

    def test_inplace_dir_replacement_never_corrupts_s1_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_root = root / "state"
            vol_root = root / "vol" / "recordings"
            state_root.mkdir()
            vol_root.mkdir(parents=True)
            sid = uuid7()
            sess_dir = vol_root / sid
            sess_dir.mkdir()
            (sess_dir / "manifest.json").write_text('{"session_id":"' + sid + '"}')

            service = session_catalog.CatalogService(
                state_root,
                commit="a" * 40,
                inspect_fn=lambda _p: (
                    {
                        "session_id": sid,
                        "display_name": "ok",
                        "sealed_at": "2026-08-08T10:00:00Z",
                        "take": {"take_id": uuid7(), "sequence": 1, "continuation_of": None},
                        "time": {
                            "started_at": "2026-08-08T09:59:00Z",
                            "ended_at": "2026-08-08T10:00:00Z",
                            "duration_seconds": 60.0,
                        },
                        "device": {"device_id": "d1", "device_label": "YLX-1234ABCD"},
                        "integrity": {},
                        "logs": [],
                        "video": {
                            "layout": "split-eyes",
                            "segments": [
                                {
                                    "artifacts": {
                                        "left": {
                                            "artifact_id": "a" * 64,
                                            "sha256": "a" * 64,
                                            "bytes": 10,
                                            "path": "video/left_00000.mp4",
                                            "role": "video.left",
                                            "media_type": "video/mp4",
                                        },
                                        "right": {
                                            "artifact_id": "b" * 64,
                                            "sha256": "b" * 64,
                                            "bytes": 10,
                                            "path": "video/right_00000.mp4",
                                            "role": "video.right",
                                            "media_type": "video/mp4",
                                        },
                                    }
                                }
                            ],
                        },
                        "frames": {
                            "artifact": {
                                "artifact_id": "c" * 64,
                                "sha256": "c" * 64,
                                "bytes": 10,
                                "path": "frames.ndjson",
                                "role": "frames.index",
                                "media_type": "application/x-ndjson",
                            }
                        },
                        "imu": {
                            "artifact": {
                                "artifact_id": "d" * 64,
                                "sha256": "d" * 64,
                                "bytes": 10,
                                "path": "imu.ndjson",
                                "role": "imu.samples",
                                "media_type": "application/x-ndjson",
                            }
                        },
                    },
                    b"{}",
                ),
                validate_summary_fn=lambda _s: None,
            )
            admission = VolumeAdmission(
                mountpoint=root / "vol",
                sessions_root=vol_root,
                catalog_roots=(vol_root,),
                volume_id="550e8400-e29b-41d4-a716-446655440000",
                generation_id="550e8400-e29b-41d4-a716-446655440001",
                device=vol_root.stat().st_dev,
                mount_identity="path:1:1",
                total_bytes=1000,
                available_bytes=1000,
                available_inodes=100,
            )

            # Simulate in-place directory replacement between inspect and DB remember.
            with patch.object(
                service._catalog_db,
                "remember",
                side_effect=DeviceRecordingError("verification_changed", "dir replaced"),
            ):
                service.catalog_sessions(
                    admission,
                    retained_ids=set(),
                    check_manifest_current=lambda _sid, _snap: False,
                    verify_session_fn=lambda *_a, **_kw: None,
                )

            self.assertNotIn(sid, service._session_summaries)
            self.assertIn(sid, service._session_diagnostics)


class VolumeAndProxiesUnitTests(unittest.TestCase):
    def test_default_mount_identity_last_match_and_monotonic_cache_and_reserve_priority(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mnt = root / "vol"
            mnt.mkdir()
            encoded = str(mnt).replace(" ", "\\040")
            mountinfo = (
                f"10 1 8:1 / {encoded} rw,relatime - ext4 /dev/sda1 rw\n"
                f"42 10 8:17 / {encoded} rw,relatime - exfat /dev/sdb1 rw\n"
            )
            real_read_text = Path.read_text

            def fake_read_text(path_obj: Path, *args, **kwargs):
                if path_obj == Path("/proc/self/mountinfo"):
                    return mountinfo
                return real_read_text(path_obj, *args, **kwargs)

            with patch.object(Path, "read_text", autospec=True, side_effect=fake_read_text):
                self.assertEqual(default_mount_identity(mnt), "mount:42:8:17")

            now = 100.0
            vm = VolumeManager(
                _coordinator_config(root),
                monotonic_fn=lambda: now,
                system_capacity_fn=lambda _p: (10**9, 10**6),
                backing_capacity_fn=lambda _p: (10**9, 10**6),
                stat_capacity_fn=lambda _p: (10**9, 10**9, 10**6),
            )
            vm.update_storage_cache(StorageStatus(5000, True), 20.0)
            vm.update_storage_cache(StorageStatus(1000, True), 10.0)  # stale timestamp refused
            self.assertEqual(vm.cached_storage(), StorageStatus(5000, True))

            admission = VolumeAdmission(
                mountpoint=mnt,
                sessions_root=mnt / "recordings",
                catalog_roots=(mnt / "recordings",),
                volume_id="550e8400-e29b-41d4-a716-446655440000",
                generation_id="550e8400-e29b-41d4-a716-446655440001",
                device=mnt.stat().st_dev,
                mount_identity="mount:42:8:17",
                total_bytes=10**9,
                available_bytes=10**9,
                available_inodes=10**6,
            )
            plan = _session_plan()
            # Reserve violation priority:
            # system_error > backing_error > volume_bytes > volume_inodes
            # > backing_bytes > backing_inodes
            with (
                patch.object(
                    vm, "probe_storage_capacity", return_value=(StorageStatus(50, True), 1)
                ),
                patch.object(
                    vm,
                    "require_system_reserve",
                    side_effect=DeviceRecordingError("insufficient_space", "sys"),
                ),
                patch.object(
                    vm,
                    "_backing_capacity",
                    side_effect=DeviceRecordingError("storage_unavailable", "backing"),
                ),
            ):
                _, _, v1 = vm.check_reserve_violation(admission, plan)
                self.assertEqual(v1, ("system_reserve_low", "insufficient_space"))

            with (
                patch.object(
                    vm, "probe_storage_capacity", return_value=(StorageStatus(50, True), 1)
                ),
                patch.object(vm, "require_system_reserve", return_value=None),
                patch.object(
                    vm,
                    "_backing_capacity",
                    side_effect=DeviceRecordingError("storage_unavailable", "backing"),
                ),
            ):
                _, _, v2 = vm.check_reserve_violation(admission, plan)
                self.assertEqual(v2, ("backing_capacity_unavailable", "storage_unavailable"))

            with (
                patch.object(
                    vm, "probe_storage_capacity", return_value=(StorageStatus(50, True), 1)
                ),
                patch.object(vm, "require_system_reserve", return_value=None),
                patch.object(vm, "_backing_capacity", return_value=(50, 1)),
            ):
                _, _, v3 = vm.check_reserve_violation(admission, plan)
                self.assertEqual(v3, ("volume_bytes_low", "insufficient_space"))

            with (
                patch.object(
                    vm, "probe_storage_capacity", return_value=(StorageStatus(5000, True), 1)
                ),
                patch.object(vm, "require_system_reserve", return_value=None),
                patch.object(vm, "_backing_capacity", return_value=(50, 1)),
            ):
                _, _, v4 = vm.check_reserve_violation(admission, plan)
                self.assertEqual(v4, ("volume_inodes_low", "insufficient_inodes"))

            with (
                patch.object(
                    vm, "probe_storage_capacity", return_value=(StorageStatus(5000, True), 100)
                ),
                patch.object(vm, "require_system_reserve", return_value=None),
                patch.object(vm, "_backing_capacity", return_value=(50, 1)),
            ):
                _, _, v5 = vm.check_reserve_violation(admission, plan)
                self.assertEqual(v5, ("backing_bytes_low", "insufficient_space"))

            with (
                patch.object(
                    vm, "probe_storage_capacity", return_value=(StorageStatus(5000, True), 100)
                ),
                patch.object(vm, "require_system_reserve", return_value=None),
                patch.object(vm, "_backing_capacity", return_value=(5000, 1)),
            ):
                _, _, v6 = vm.check_reserve_violation(admission, plan)
                self.assertEqual(v6, ("backing_inodes_low", "insufficient_inodes"))

    def test_capture_lease_manager_and_network_controller_proxy_error_mappings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            maint_lock = root / "maint.lock"
            net_lock = root / "net.lock"
            with patch.dict(os.environ, {"OPENARIA_MAINTENANCE_LOCK": str(maint_lock)}):
                leases = CaptureLeaseManager(lock_path_fn=lambda: net_lock)
                # Maintenance busy -> 409 maintenance_active (retryable=True)
                maint_fd = os.open(maint_lock, os.O_RDONLY | os.O_CREAT, 0o660)
                try:
                    fcntl.flock(maint_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    with self.assertRaises(ProviderError) as err_maint:
                        leases.acquire()
                    self.assertEqual(err_maint.exception.code, "maintenance_active")
                    self.assertTrue(err_maint.exception.retryable)
                finally:
                    fcntl.flock(maint_fd, fcntl.LOCK_UN)
                    os.close(maint_fd)

                # Network lock already held -> 409 network_mutation_active
                net_fd = os.open(net_lock, os.O_RDONLY | os.O_CREAT, 0o660)
                try:
                    fcntl.flock(net_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    with self.assertRaises(ProviderError) as err_net:
                        leases.acquire()
                    self.assertEqual(err_net.exception.code, "network_mutation_active")
                finally:
                    fcntl.flock(net_fd, fcntl.LOCK_UN)
                    os.close(net_fd)

                # Clean acquire + release
                leases.acquire()
                self.assertIsNotNone(leases.network_operation_lease)
                leases.release()
                self.assertIsNone(leases.network_operation_lease)

        # NetworkControllerProxy error mappings
        proxy = NetworkControllerProxy(
            request_control_fn=lambda *_a, **_kw: (_ for _ in ()).throw(
                NetworkControlClientError("down", "down")
            )
        )
        self.assertFalse(proxy.controller_available())
        with self.assertRaises(ProviderError) as scan_err:
            proxy.scan_networks()
        self.assertEqual(scan_err.exception.code, "network_scan_unavailable")

        with self.assertRaises(ProviderError) as cred_err:
            proxy.create_network_credential(NetworkCredentialCommand("owner", "passphrase123"))
        self.assertEqual(cred_err.exception.code, "network_mutation_unavailable")

        err_idem = NetworkControllerProxy.network_provider_error(
            {"error": {"code": "idempotency_conflict"}}, operation="apply"
        )
        self.assertEqual(err_idem.code, "idempotency_conflict")
        self.assertEqual(err_idem.status, 409)

        tx_id = uuid7()
        err_not_found = NetworkControllerProxy.network_provider_error(
            {"error": {"code": "transaction_not_found"}},
            operation="retry",
            body={"transaction_id": tx_id},
        )
        self.assertEqual(err_not_found.code, "network_transaction_not_found")
        self.assertEqual(err_not_found.status, 404)

        err_cand = NetworkControllerProxy.network_provider_error(
            {"error": {"code": "candidate_invalid"}},
            operation="apply",
            body={"desired": {"mode": "ethernet-static"}},
        )
        self.assertEqual(err_cand.code, "invalid_network_desired_state")
        self.assertEqual(err_cand.status, 422)

        with self.assertRaises(ProviderError) as cmd_err:
            proxy.network_command("apply", NetworkCommand("owner", "k1", {}, b"{}"))
        self.assertEqual(cmd_err.exception.code, "network_mutation_unavailable")


class CoordinatorAndStorageRegressionTests(unittest.TestCase):
    def test_concurrent_start_vs_status_and_admission_vs_stop_no_deadlock(self) -> None:
        with CoordinatorHarness() as harness:
            sources = FakeSources()
            coordinator = harness.coordinator(sources=sources)
            try:
                errors: list[BaseException] = []
                stop_polling = threading.Event()

                def poller() -> None:
                    try:
                        while not stop_polling.is_set():
                            coordinator.capture_status()
                    except BaseException as exc:
                        errors.append(exc)

                poll_thread = threading.Thread(target=poller, daemon=True)
                poll_thread.start()
                try:
                    for idx in range(5):
                        started = coordinator.start_capture(start_command(f"start-{idx}"))
                        self.assertEqual(started.status, 202)
                        assert sources.submit_frame is not None
                        assert sources.submit_imu is not None
                        sources.submit_frame(frame(idx * 2))
                        sources.submit_imu(imu_observation(idx * 2))
                        stopped = coordinator.stop_capture(stop_command(f"stop-{idx}"))
                        self.assertIn(stopped.status, (200, 202, 204))
                finally:
                    stop_polling.set()
                    poll_thread.join(timeout=3.0)
                self.assertEqual(errors, [])

                # Concurrent _require_admission vs stop_capture when admission resets
                coordinator._volume._admission = None
                admission_thread = threading.Thread(
                    target=lambda: coordinator._require_admission(), daemon=True
                )
                stop_thread = threading.Thread(
                    target=lambda: coordinator.stop_capture(stop_command("stop-concurrent")),
                    daemon=True,
                )
                admission_thread.start()
                stop_thread.start()
                admission_thread.join(timeout=3.0)
                stop_thread.join(timeout=3.0)
                self.assertFalse(admission_thread.is_alive())
                self.assertFalse(stop_thread.is_alive())
            finally:
                coordinator.close()

    def test_crash_recovery_publish_before_unlink_survives_rename_failure(self) -> None:
        with CoordinatorHarness() as harness:
            coordinator = harness.coordinator()
            try:
                admission = coordinator._require_admission()
                sid = uuid7()
                partial = admission.sessions_root / f"{sid}.partial"
                partial.mkdir()
                (partial / "manifest.json").write_text('{"session_id":"' + sid + '"}')
                (partial / "recording.json").write_text('{"state":"recording"}')
                (partial / "capture.json").write_text('{"session_id":"' + sid + '"}')

                fd = os.open(partial, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    with (
                        patch(
                            "rp_ylx.recording.coordinator.inspect_device_session_directory",
                            return_value=({}, b"{}"),
                        ),
                        patch.object(
                            recovery_orchestrator.os,
                            "rename",
                            side_effect=OSError(errno.EIO, "simulated rename failure"),
                        ),
                    ):
                        coordinator._recover_partial_directory(partial, sid, fd)
                finally:
                    os.close(fd)

                # Controls must remain intact in partial/ (and archived in recovery/) for retry!
                self.assertTrue((partial / "recording.json").is_file())
                self.assertTrue((partial / "capture.json").is_file())
                self.assertTrue((partial / "recovery/recording.json").is_file())
                self.assertTrue((partial / "recovery/capture.json").is_file())
            finally:
                coordinator.close()

    def test_sealed_session_sqlite_session_paths_root_equals_sessions_root(self) -> None:
        with CoordinatorHarness() as harness:
            sources = FakeSources()
            coordinator = harness.coordinator(sources=sources)
            try:
                admission = coordinator._require_admission()
                coordinator.start_capture(start_command("start-paths"))
                assert sources.submit_frame is not None
                assert sources.submit_imu is not None
                for seq in range(4):
                    sources.submit_frame(frame(seq))
                sources.submit_imu(imu_observation(0))
                coordinator.stop_capture(stop_command("stop-paths"))

                with closing(sqlite3.connect(harness.state_root / "catalog.sqlite3")) as db:
                    rows = db.execute(
                        "SELECT volume, session, root, relative_path FROM session_paths"
                    ).fetchall()
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0][0], admission.volume_id)
                self.assertEqual(Path(rows[0][2]), admission.sessions_root)
                self.assertNotEqual(Path(rows[0][2]), admission.mountpoint)
            finally:
                coordinator.close()

    def test_exfat_deletion_checks_mtime_and_sha256_and_backing_storage_supports_f_files_zero(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sessions_root = root / "recordings"
            sessions_root.mkdir()
            sid = uuid7()
            sess_dir = sessions_root / sid
            sess_dir.mkdir()
            payload = (
                b'{"session_id":"'
                + sid.encode()
                + b'","take":{"sequence":1,"continuation_of":null}}'
            )
            (sess_dir / "manifest.json").write_bytes(payload)
            digest = hashlib.sha256(payload).hexdigest()
            manifest = json.loads(payload)

            with (
                patch.object(
                    deletion,
                    "inspect_device_session_directory",
                    return_value=(manifest, payload),
                ),
                patch.object(deletion, "is_exfat_or_msdos_fd", return_value=True),
            ):
                # Corrupt manifest between initial scan and delete check -> fails cleanly
                real_verify = deletion._verify_directory_manifest_digest

                def tamper_before_verify(parent_fd, name, *, expected_stat, expected_sha256):
                    (sess_dir / "manifest.json").write_bytes(payload + b" ")
                    real_verify(
                        parent_fd,
                        name,
                        expected_stat=expected_stat,
                        expected_sha256=expected_sha256,
                    )

                with patch.object(
                    deletion, "_verify_directory_manifest_digest", side_effect=tamper_before_verify
                ):
                    result = deletion.delete_recordings((sessions_root,), {sid: digest})
                self.assertEqual(result["deleted_session_ids"], [])
                self.assertEqual(len(result["failed_sessions"]), 1)
                self.assertTrue(sess_dir.is_dir())

                # Restore exact bytes and mtime -> deletion succeeds
                (sess_dir / "manifest.json").write_bytes(payload)
                ok = deletion.delete_recordings((sessions_root,), {sid: digest})
                self.assertEqual(ok["deleted_session_ids"], [sid])
                self.assertFalse(sess_dir.exists())

        # backing_storage supports f_files == 0 (exFAT host filesystem)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mnt = root / "mnt"
            host_img = root / "backing.img"
            mnt.mkdir()
            host_img.write_bytes(b"img")
            sys_block = root / "sys_dev_block"
            mnt_dev = mnt.stat().st_dev
            loop_node = sys_block / f"{os.major(mnt_dev)}:{os.minor(mnt_dev)}" / "loop"
            loop_node.mkdir(parents=True)
            (loop_node / "backing_file").write_text(str(host_img) + "\n")

            fake_vfs = SimpleNamespace(f_bavail=500, f_frsize=4096, f_files=0, f_favail=0)
            real_stat = Path.stat

            def distinct_dev_stat(path_obj: Path, *args, **kwargs):
                st = real_stat(path_obj, *args, **kwargs)
                if path_obj == host_img:
                    return SimpleNamespace(st_dev=mnt_dev + 1, st_mode=st.st_mode)
                return st

            with (
                patch.object(backing_storage, "SYS_DEV_BLOCK", sys_block),
                patch.object(Path, "stat", autospec=True, side_effect=distinct_dev_stat),
                patch.object(backing_storage.os, "statvfs", return_value=fake_vfs),
            ):
                cap = backing_storage.backing_capacity(mnt)
                self.assertEqual(cap, (500 * 4096, 500))
                backing_storage.require_backing_reserve(mnt, 1000, 100)


if __name__ == "__main__":
    unittest.main()
