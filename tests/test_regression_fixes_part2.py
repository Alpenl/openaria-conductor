"""Regression tests for wave 3-5 recording, security, network, and script fixes (part 2)."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import socket
import stat
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import rp_ylx.network as network_module
import rp_ylx.recording.device_session as device_session_module
import rp_ylx.recording.recovery as recovery_module
import rp_ylx.update as update_module
from rp_ylx.api import ProviderError
from rp_ylx.camera import FrameObservation, StereoFrame
from rp_ylx.network_control import (
    NetworkController,
    _NetworkControlRpcServer,
)
from rp_ylx.recording import (
    DeviceRecordingError,
    uuid7,
    validate_device_session_directory,
)
from rp_ylx.recording.catalog import SessionCatalog
from rp_ylx.recording.deletion import delete_recordings
from rp_ylx.recording.live_seal import _read_back
from rp_ylx.recording.recovery import recover_device_session
from rp_ylx.recording_indicator import indicator_pattern, run_indicator
from rp_ylx.recording_layout import resolve_recording
from rp_ylx.spectacular.timebase import _fit_clock
from scripts import _host_disks, publish_rdk_x5_oss, rdk_ap_sta_preflight
from scripts.card_export import export as card_export
from tests.testkit import (
    FRAME,
    CoordinatorHarness,
    FakeSessionStore,
    FakeSources,
    NetworkHarness,
    RecoveryFixtureBuilder,
    SplitEyeHarness,
    start_command,
    write_ed25519_keypair,
)


@contextmanager
def _simulated_exfat_fstatvfs():
    real_fstatvfs = os.fstatvfs

    def exfat_fstatvfs(fd: int) -> object:
        result = real_fstatvfs(fd)
        return SimpleNamespace(
            f_bsize=result.f_bsize,
            f_frsize=result.f_frsize,
            f_blocks=result.f_blocks,
            f_bfree=result.f_bfree,
            f_bavail=result.f_bavail,
            f_files=0,
            f_ffree=0,
            f_favail=0,
            f_flag=result.f_flag,
            f_namemax=result.f_namemax,
        )

    with patch("os.fstatvfs", side_effect=exfat_fstatvfs):
        yield


class _StatProxy:
    """Stat wrapper overriding st_ino while preserving all nanosecond timestamps."""

    def __init__(self, inner: os.stat_result, *, st_ino: int) -> None:
        self._inner = inner
        self.st_ino = st_ino

    def __getattr__(self, name: str) -> object:
        return getattr(self._inner, name)


def _bump_inode_on_stat(
    st: os.stat_result,
    *,
    extra_ino: int = 10_000,
) -> _StatProxy:
    return _StatProxy(st, st_ino=st.st_ino + extra_ino)


class ExfatInodeRegressionTests(unittest.TestCase):
    """R-01: Volatile st_ino on exFAT (f_files=0) must not break closed-file verification."""

    def test_r01_device_session_recorder_survives_exfat_inode_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            recorder, _, _ = SplitEyeHarness().build(root)
            with _simulated_exfat_fstatvfs():
                recorder.start()
                SplitEyeHarness.feed(recorder, 3)
                deadline = time.monotonic() + 5.0
                while recorder._harvested_segments < 1 and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertGreaterEqual(recorder._harvested_segments, 1)

                target = recorder.partial_path / "video/left_00000.mp4"
                original_stat = target.stat(follow_symlinks=False)
                real_stat = os.stat
                real_fstat = os.fstat
                real_path_stat = Path.stat

                def bumped_stat(path, *args, **kwargs):
                    st = real_stat(path, *args, **kwargs)
                    if st.st_ino == original_stat.st_ino:
                        return _bump_inode_on_stat(st)
                    return st

                def bumped_fstat(fd: int) -> object:
                    st = real_fstat(fd)
                    if st.st_ino == original_stat.st_ino:
                        return _bump_inode_on_stat(st)
                    return st

                def bumped_path_stat(self_path: Path, *args, **kwargs):
                    st = real_path_stat(self_path, *args, **kwargs)
                    if st.st_ino == original_stat.st_ino:
                        return _bump_inode_on_stat(st)
                    return st

                with (
                    patch.object(device_session_module.os, "stat", side_effect=bumped_stat),
                    patch.object(device_session_module.os, "fstat", side_effect=bumped_fstat),
                    patch.object(Path, "stat", bumped_path_stat),
                ):
                    sealed = recorder.stop()
                self.assertTrue((sealed.path / "manifest.json").is_file())
                validate_device_session_directory(sealed.path)

    def test_r01_recover_device_session_survives_exfat_inode_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            recorder = RecoveryFixtureBuilder().recording(root, frames=6)
            target = recorder.partial_path / "video/left_00000.mp4"
            original_stat = target.stat(follow_symlinks=False)
            real_stat = os.stat
            real_fstat = os.fstat
            seen_target = 0

            def bumped_stat(path, *args, **kwargs):
                nonlocal seen_target
                st = real_stat(path, *args, **kwargs)
                if st.st_ino == original_stat.st_ino:
                    seen_target += 1
                    if seen_target > 1:
                        return _bump_inode_on_stat(st)
                return st

            def bumped_fstat(fd: int) -> os.stat_result:
                st = real_fstat(fd)
                if st.st_ino == original_stat.st_ino and seen_target > 1:
                    return _bump_inode_on_stat(st)
                return st

            with (
                _simulated_exfat_fstatvfs(),
                patch.object(recovery_module.os, "stat", side_effect=bumped_stat),
                patch.object(recovery_module.os, "fstat", side_effect=bumped_fstat),
                patch.object(Path, "stat", lambda p, *a, **kw: bumped_stat(p, *a, **kw)),
            ):
                recovered = recover_device_session(recorder.partial_path)
            self.assertIsNotNone(recovered)
            assert recovered is not None
            validate_device_session_directory(recovered.path)

    def test_r01_live_sealing_read_back_tolerates_exfat_inode_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "audio_00000.wav"
            payload = b"RIFF-exfat-test-payload"
            path.write_bytes(payload)
            with _simulated_exfat_fstatvfs():
                proof = _read_back(path)
                self.assertFalse(proof.include_inode)
                original_stat = path.stat(follow_symlinks=False)
                real_stat = os.stat
                real_fstat = os.fstat
                real_path_stat = Path.stat

                def bumped_stat(p, *args, **kwargs):
                    st = real_stat(p, *args, **kwargs)
                    return _bump_inode_on_stat(st) if st.st_ino == original_stat.st_ino else st

                def bumped_fstat(fd: int) -> object:
                    st = real_fstat(fd)
                    return _bump_inode_on_stat(st) if st.st_ino == original_stat.st_ino else st

                def bumped_path_stat(self_path: Path, *args, **kwargs):
                    st = real_path_stat(self_path, *args, **kwargs)
                    return _bump_inode_on_stat(st) if st.st_ino == original_stat.st_ino else st

                with (
                    patch("os.stat", side_effect=bumped_stat),
                    patch("os.fstat", side_effect=bumped_fstat),
                    patch.object(Path, "stat", bumped_path_stat),
                ):
                    self.assertTrue(proof.unchanged(path))

            # On POSIX (f_files > 0), the same inode swap must still be rejected.
            posix_proof = _read_back(path)
            self.assertTrue(posix_proof.include_inode)
            with (
                patch("os.stat", side_effect=bumped_stat),
                patch("os.fstat", side_effect=bumped_fstat),
                patch.object(Path, "stat", bumped_path_stat),
            ):
                self.assertFalse(posix_proof.unchanged(path))

    def test_r01_session_catalog_signature_ignores_exfat_inode_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            session_dir = root / uuid7()
            session_dir.mkdir()
            manifest = session_dir / "manifest.json"
            manifest.write_bytes(b'{"schema":"ylx.device-session.v1","sealed":true}\n')
            original_dir_stat = session_dir.lstat()
            original_file_stat = manifest.lstat()

            with (
                _simulated_exfat_fstatvfs(),
                patch("rp_ylx.recording.catalog.native_session_store_or_none", return_value=None),
            ):
                sig_before = SessionCatalog.signature(session_dir)
                parsed = json.loads(sig_before)
                self.assertEqual(parsed[1], 0)
                self.assertEqual(parsed[3], 0)

                real_lstat = Path.lstat
                real_stat = os.stat
                real_fstat = os.fstat

                def bumped_lstat(self_path: Path) -> os.stat_result:
                    st = real_lstat(self_path)
                    if st.st_ino == original_dir_stat.st_ino:
                        return _bump_inode_on_stat(st)
                    return st

                def bumped_stat(target, *args, **kwargs):
                    st = real_stat(target, *args, **kwargs)
                    if st.st_ino in {original_dir_stat.st_ino, original_file_stat.st_ino}:
                        return _bump_inode_on_stat(st)
                    return st

                def bumped_fstat(fd: int) -> os.stat_result:
                    st = real_fstat(fd)
                    if st.st_ino in {original_dir_stat.st_ino, original_file_stat.st_ino}:
                        return _bump_inode_on_stat(st)
                    return st

                with (
                    patch.object(Path, "lstat", bumped_lstat),
                    patch("os.stat", side_effect=bumped_stat),
                    patch("os.fstat", side_effect=bumped_fstat),
                ):
                    sig_after = SessionCatalog.signature(session_dir)
                self.assertEqual(sig_before, sig_after)


class RecordingRecoveryAndFailureTests(unittest.TestCase):
    """R-02, R-03, R-04, R-08 regression tests."""

    def test_r02_failed_rename_during_stop_preserves_partial_controls_for_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            recorder, _, _ = SplitEyeHarness().build(root)
            with patch.object(
                recorder, "_now", return_value=datetime.now(UTC) - timedelta(seconds=10)
            ):
                recorder.start()
            origin = recorder._started_monotonic_ns
            assert origin is not None
            for index in range(3):
                recorder.submit_frame(
                    FrameObservation(
                        StereoFrame(
                            source_sequence=index * 2,
                            host_monotonic_ns=origin + 10_000_000 + index * 33_000_000,
                            left=b"",
                            right=b"",
                            raw_side_by_side=FRAME,
                        ),
                        dropped_before=0,
                    )
                )

            real_rename = os.rename

            def fail_final_rename(src: Path | str, dst: Path | str, *args, **kwargs) -> None:
                if Path(src) == recorder.partial_path and Path(dst) == recorder._final:
                    raise OSError(errno.EXDEV, "injected rename failure")
                real_rename(src, dst, *args, **kwargs)

            with (
                patch.object(device_session_module.os, "rename", side_effect=fail_final_rename),
                self.assertRaises(DeviceRecordingError),
            ):
                recorder.stop()

            self.assertTrue((recorder.partial_path / "capture.json").is_file())
            self.assertTrue((recorder.partial_path / "recording.json").is_file())
            self.assertFalse(recorder._final.exists())

            recovered = recover_device_session(recorder.partial_path)
            self.assertIsNotNone(recovered)
            assert recovered is not None
            validate_device_session_directory(recovered.path)

    def test_r02_failed_rename_during_recovery_preserves_partial_controls(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            recorder = RecoveryFixtureBuilder().recording(root, frames=6)
            partial = recorder.partial_path
            real_rename = os.rename

            def fail_recovery_rename(src: Path | str, dst: Path | str, *args, **kwargs) -> None:
                if Path(src) == partial:
                    raise OSError(errno.EIO, "injected recovery rename failure")
                real_rename(src, dst, *args, **kwargs)

            with (
                patch.object(recovery_module.os, "rename", side_effect=fail_recovery_rename),
                self.assertRaises(OSError),
            ):
                recover_device_session(partial)

            self.assertTrue((partial / "capture.json").is_file())
            self.assertTrue((partial / "recording.json").is_file())

            recovered = recover_device_session(partial)
            self.assertIsNotNone(recovered)
            assert recovered is not None
            validate_device_session_directory(recovered.path)

    def test_r03_native_harvest_loop_exception_is_surfaced_by_stop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = FakeSessionStore()
            recorder, _, _ = SplitEyeHarness().build(root, native_data_plane=True)
            with (
                patch.object(device_session_module, "native_session_store", return_value=store),
                patch.object(
                    device_session_module, "resolve_executable", return_value=Path("/bin/true")
                ),
            ):
                recorder.start()
            assert store.transaction is not None
            store.transaction.advance(frames=3)

            def broken_segments() -> list[dict[str, object]]:
                raise RuntimeError("harvester segment boom")

            store.transaction.segments = broken_segments  # type: ignore[method-assign]
            deadline = time.monotonic() + 5.0
            while recorder._writer_error is None and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertIsNotNone(recorder._writer_error)

            with self.assertRaises(DeviceRecordingError):
                recorder.stop()
            recorder.abort()

    def test_r04_coordinator_stops_sources_before_recorder_fail(self) -> None:
        order: list[str] = []

        class OrderedSources(FakeSources):
            def stop(self) -> None:
                order.append("sources.stop")
                super().stop()

        sources = OrderedSources()
        with CoordinatorHarness() as harness, harness.coordinator(sources=sources) as coordinator:
            coordinator.start_capture(start_command("r04-order"))
            active = coordinator._active
            assert active is not None
            real_fail = active.fail

            def traced_fail(*args, **kwargs):
                order.append("recorder.fail")
                return real_fail(*args, **kwargs)

            with patch.object(active, "fail", side_effect=traced_fail):
                coordinator._source_failure(
                    coordinator.generation_id,
                    "camera_disconnected",
                    "camera unplugged",
                )
            self.assertGreaterEqual(len(order), 2)
            self.assertEqual(order[:2], ["sources.stop", "recorder.fail"])

    def test_r04_native_abort_exception_does_not_skip_close_files_or_mark_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = FakeSessionStore()
            recorder, _, _ = SplitEyeHarness().build(root, native_data_plane=True)
            with (
                patch.object(device_session_module, "native_session_store", return_value=store),
                patch.object(
                    device_session_module, "resolve_executable", return_value=Path("/bin/true")
                ),
            ):
                recorder.start()
            assert store.transaction is not None

            def raising_abort(reason: str) -> None:
                del reason
                raise RuntimeError("native abort failed")

            store.transaction.abort = raising_abort  # type: ignore[method-assign]
            recorder.fail("source_error", "upstream failure")
            self.assertEqual(recorder._files, {})
            self.assertEqual(recorder.state, "failed")
            rec_state = json.loads((recorder.partial_path / "recording.json").read_bytes())
            self.assertEqual(rec_state["state"], "failed")
            diag = json.loads((recorder.partial_path / "failure-diagnostics.json").read_bytes())
            self.assertIn("native abort failed", diag["native_abort_error"])

    def test_r08_deletion_ignores_stray_folders_and_isolates_corrupt_directories(self) -> None:
        with CoordinatorHarness() as harness:
            with harness.coordinator() as coordinator:
                sid = harness.seal_one(coordinator, prefix="r08")
                with coordinator.open_manifest(sid, "v4") as opened:
                    digest = hashlib.sha256(opened.read()).hexdigest()

            recordings_root = harness.mountpoint / "recordings"
            (recordings_root / "notes").mkdir()
            (recordings_root / "notes" / "readme.txt").write_text("stray")
            (recordings_root / "System Volume Information").mkdir()

            corrupt_sid = uuid7()
            corrupt_dir = recordings_root / corrupt_sid
            corrupt_dir.mkdir()
            (corrupt_dir / "manifest.json").write_text("{not-valid-json")

            result = delete_recordings((recordings_root,), {sid: digest})
            self.assertEqual(result["deleted_session_ids"], [sid])
            self.assertEqual(result["failed_sessions"], [])
            self.assertFalse(resolve_recording(recordings_root, sid).exists())
            self.assertEqual(list(recordings_root.rglob(f"{sid}*")), [])
            self.assertTrue((recordings_root / "notes").is_dir())

            with self.assertRaises(ProviderError) as raised:
                delete_recordings((recordings_root,), {corrupt_sid: "a" * 64})
            self.assertEqual(raised.exception.code, "catalog_unavailable")
            self.assertEqual(raised.exception.status, 409)


class NetworkAndUpdateSecurityTests(unittest.TestCase):
    """S-01, S-02, S-03, S-05 regression tests."""

    def test_s01_deferred_response_write_error_aborts_and_unblocks_subsequent_mutations(
        self,
    ) -> None:
        with NetworkHarness(patch_all_env=True, patch_nmcli=True) as harness:
            sock_path = harness.root / "net-ctrl.sock"
            controller = NetworkController(
                device_id="device-s01",
                require_root=False,
                defer_execution_until_response=True,
            )
            controller.start()
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            listener.bind(str(sock_path))
            listener.listen(8)
            server = _NetworkControlRpcServer(listener, controller)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                apply_payload = (
                    json.dumps(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "apply",
                            "principal_id": "customer",
                            "idempotency_key": "s01-fail-write",
                            "body": {
                                "schema": "ylx.network-apply-request.v1",
                                "desired": {
                                    "mode": "hotspot",
                                    "wifi_client": None,
                                    "ethernet": None,
                                },
                            },
                        }
                    ).encode()
                    + b"\n"
                )
                with (
                    patch(
                        "rp_ylx.network_control.write_all_with_deadline",
                        side_effect=OSError("broken pipe"),
                    ),
                    socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client,
                ):
                    client.connect(str(sock_path))
                    client.sendall(apply_payload)
                    self.assertEqual(client.recv(1024), b"")

                self.assertTrue(controller.wait_for_idle(timeout=3.0))
                status = controller.handle(
                    {"schema": "ylx.network-control-request.v1", "operation": "status"}
                )
                self.assertIsNone(status["body"]["transaction"]["current"])

                next_payload = (
                    json.dumps(
                        {
                            "schema": "ylx.network-control-request.v1",
                            "operation": "apply",
                            "principal_id": "customer",
                            "idempotency_key": "s01-recover",
                            "body": {
                                "schema": "ylx.network-apply-request.v1",
                                "desired": {
                                    "mode": "hotspot",
                                    "wifi_client": None,
                                    "ethernet": None,
                                },
                            },
                        }
                    ).encode()
                    + b"\n"
                )
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.connect(str(sock_path))
                    client.sendall(next_payload)
                    response_line = b""
                    while not response_line.endswith(b"\n"):
                        chunk = client.recv(4096)
                        if not chunk:
                            break
                        response_line += chunk
                parsed = json.loads(response_line)
                self.assertTrue(parsed["ok"])
                self.assertTrue(controller.wait_for_idle(timeout=5.0))
            finally:
                server.shutdown(timeout=2.0)
                thread.join(timeout=2.0)
                listener.close()
                controller.close()

    def test_s01_restart_aborts_unreleased_transaction_when_non_deferred(self) -> None:
        with NetworkHarness(patch_all_env=True, patch_nmcli=True):
            first = NetworkController(
                device_id="device-s01-restart",
                require_root=False,
                defer_execution_until_response=True,
            )
            first.start()
            try:
                accepted = first.handle(
                    {
                        "schema": "ylx.network-control-request.v1",
                        "operation": "apply",
                        "principal_id": "customer",
                        "idempotency_key": "s01-unreleased",
                        "body": {
                            "schema": "ylx.network-apply-request.v1",
                            "desired": {
                                "mode": "hotspot",
                                "wifi_client": None,
                                "ethernet": None,
                            },
                        },
                    }
                )
                self.assertTrue(accepted["ok"])
                tx_id = accepted["body"]["transaction"]["transaction_id"]
                self.assertFalse(first._state.execution_released(tx_id))
            finally:
                first.close()

            restarted = NetworkController(
                device_id="device-s01-restart",
                require_root=False,
                defer_execution_until_response=False,
            )
            restarted.start()
            try:
                status = restarted.handle(
                    {"schema": "ylx.network-control-request.v1", "operation": "status"}
                )
                self.assertIsNone(status["body"]["transaction"]["current"])
                latest = status["body"]["transaction"]["latest"]
                self.assertIsNotNone(latest)
                self.assertEqual(latest["transaction_id"], tx_id)
                self.assertIn(latest["status"], {"failed", "rescued"})
            finally:
                restarted.close()

    def test_s03_ensure_rescue_ap_preserves_operator_customized_psk(self) -> None:
        with NetworkHarness(patch_all_env=True, patch_nmcli=True):
            record = network_module.ensure_rescue_ap("device-s03")
            profile_path = network_module._profile_dir() / f"{record['profile']}.nmconnection"
            content = profile_path.read_text(encoding="utf-8")
            self.assertIn(f"psk={network_module.PUBLIC_RESCUE_AP_PSK}", content)

            custom_psk = "OperatorCustomPass99"
            updated = content.replace(
                f"psk={network_module.PUBLIC_RESCUE_AP_PSK}",
                f"psk={custom_psk}",
            )
            profile_path.write_text(updated, encoding="utf-8")

            network_module.ensure_rescue_ap("device-s03", preserve_existing_psk=True)
            self.assertIn(f"psk={custom_psk}", profile_path.read_text(encoding="utf-8"))

            network_module.ensure_rescue_ap("device-s03", preserve_existing_psk=False)
            self.assertIn(
                f"psk={network_module.PUBLIC_RESCUE_AP_PSK}",
                profile_path.read_text(encoding="utf-8"),
            )

    def test_s02_manifest_signature_roundtrip_and_tamper_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            priv = root / "ed25519.key"
            pub = root / "ed25519.pub"
            write_ed25519_keypair(priv, pub)
            manifest = {
                "schema": update_module.RELEASE_SCHEMA,
                "platform": update_module.PLATFORM,
                "version": "0.5.1",
                "commit": "a" * 40,
                "bundle": {
                    "url": "https://example.invalid/bundle.tar.gz",
                    "sha256": "b" * 64,
                    "bytes": 100,
                },
                "updater": {
                    "url": "https://example.invalid/update.py",
                    "sha256": "c" * 64,
                    "bytes": 50,
                },
            }
            manifest["signature"] = publish_rdk_x5_oss.sign_manifest(manifest, priv)

            with patch.dict(os.environ, {update_module.OPENARIA_UPDATE_PUBLIC_KEY_ENV: str(pub)}):
                update_module.verify_manifest_signature(manifest)

                tampered = dict(manifest, version="0.5.2")
                with self.assertRaises(ValueError):
                    update_module.verify_manifest_signature(tampered)

                unsigned = {k: v for k, v in manifest.items() if k != "signature"}
                with self.assertRaises(ValueError):
                    update_module.verify_manifest_signature(unsigned)

    def test_s05_maintenance_lock_creates_0660_under_restrictive_umask(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lock_path = Path(tmp) / "run" / "maintenance.lock"
            previous_umask = os.umask(0o077)
            try:
                with (
                    patch.dict(os.environ, {"OPENARIA_MAINTENANCE_LOCK": str(lock_path)}),
                    update_module.maintenance_lock(),
                ):
                    self.assertEqual(stat.S_IMODE(lock_path.stat().st_mode), 0o660)
            finally:
                os.umask(previous_umask)


class IndicatorAndToolingRegressionTests(unittest.TestCase):
    """S-15, D-09, and D-11 regression tests."""

    def test_s15_indicator_pattern_maps_verifying_and_encoding_to_saving(self) -> None:
        self.assertEqual(indicator_pattern({"device_state": "verifying"}), "saving")
        self.assertEqual(indicator_pattern({"device_state": "encoding"}), "saving")

    def test_s15_transient_sysfs_oserror_does_not_terminate_run_indicator(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            led = Path(tmp) / "ACT"
            led.mkdir()
            (led / "max_brightness").write_text("255\n")
            (led / "brightness").write_text("0\n")
            (led / "trigger").write_text("[none] timer\n")
            stopped = threading.Event()
            polls = 0
            show_calls = 0

            class FakeClient:
                def capture_snapshot(self) -> dict[str, object]:
                    nonlocal polls
                    polls += 1
                    if polls >= 5:
                        stopped.set()
                    return {"device_state": "recording" if polls >= 3 else "idle"}

            real_write_text = Path.write_text

            def flaky_write(self_path: Path, data: str, *args, **kwargs) -> int:
                nonlocal show_calls
                if self_path.parent == led and self_path.name == "brightness":
                    show_calls += 1
                    if show_calls <= 2:
                        raise OSError(errno.EIO, "transient sysfs write error")
                return real_write_text(self_path, data, *args, **kwargs)

            with (
                patch.object(Path, "write_text", flaky_write),
                patch.object(stopped, "wait", side_effect=lambda _: stopped.is_set()),
            ):
                run_indicator(led, FakeClient(), stopped)

            self.assertGreaterEqual(polls, 4)
            self.assertGreaterEqual(show_calls, 3)

    def test_d09_linux_source_on_disk_rejects_sibling_disk_prefixes(self) -> None:
        self.assertTrue(_host_disks.linux_source_on_disk("/dev/sda1", "/dev/sda"))
        self.assertTrue(_host_disks.linux_source_on_disk("/dev/mmcblk1p1", "/dev/mmcblk1"))
        self.assertFalse(_host_disks.linux_source_on_disk("/dev/sdab1", "/dev/sda"))
        self.assertFalse(_host_disks.linux_source_on_disk("/dev/mmcblk10p1", "/dev/mmcblk1"))

    def test_d11_card_export_discovers_date_time_nested_sessions(self) -> None:
        class FakeNode:
            def __init__(
                self, filename: str, filetype: int, children: dict[str, FakeNode] | None = None
            ):
                self.filename = filename
                self.filetype = filetype
                self._children = children or {}

            def iterdir(self):
                return list(self._children.values())

        class FakeFs:
            def __init__(self) -> None:
                self._nodes: dict[str, FakeNode] = {}
                sealed_session = FakeNode(
                    "14-30-00",
                    stat.S_IFDIR,
                    {"manifest.json": FakeNode("manifest.json", stat.S_IFREG)},
                )
                unsealed_session = FakeNode("15-00-00", stat.S_IFDIR, {})
                day_dir = FakeNode(
                    "2026-11-25",
                    stat.S_IFDIR,
                    {"14-30-00": sealed_session, "15-00-00": unsealed_session},
                )
                rec_root = FakeNode("recordings", stat.S_IFDIR, {"2026-11-25": day_dir})
                self._nodes = {
                    "/recordings": rec_root,
                    "/recordings/2026-11-25": day_dir,
                    "/recordings/2026-11-25/14-30-00": sealed_session,
                    "/recordings/2026-11-25/14-30-00/manifest.json": sealed_session._children[
                        "manifest.json"
                    ],
                    "/recordings/2026-11-25/15-00-00": unsealed_session,
                }

            def get(self, path: str) -> FakeNode:
                if path not in self._nodes:
                    raise FileNotFoundError(path)
                return self._nodes[path]

        discovered = card_export.recordings([("disk0", FakeFs(), False)])
        by_id = {item["id"]: item for item in discovered}
        self.assertEqual(
            set(by_id),
            {"disk0/recordings/2026-11-25/14-30-00", "disk0/recordings/2026-11-25/15-00-00"},
        )
        self.assertTrue(by_id["disk0/recordings/2026-11-25/14-30-00"]["sealed"])
        self.assertFalse(by_id["disk0/recordings/2026-11-25/15-00-00"]["sealed"])

    def test_d11_preflight_sanitize_argv_redacts_bearer_token(self) -> None:
        sanitized = rdk_ap_sta_preflight.sanitize_argv(
            [
                "preflight",
                "--host",
                "127.0.0.1",
                "--bearer-token",
                "super-secret-value",
                "--bearer-token=inline-secret",
                "--token",
                "another-secret",
            ]
        )
        self.assertEqual(
            sanitized,
            [
                "preflight",
                "--host",
                "127.0.0.1",
                "--bearer-token",
                "[REDACTED]",
                "--bearer-token=[REDACTED]",
                "--token",
                "[REDACTED]",
            ],
        )

    def test_d11_spectacular_centered_ols_precision_at_high_monotonic_ns(self) -> None:
        base_ns = 10_000_000_000_000_000  # 10^16 ns (~115 days uptime)
        expected_hz = 30.0
        period_ns = 1_000_000_000 / expected_hz
        counters = list(range(500, 700))
        host_ns = [base_ns + round((c - counters[0]) * period_ns) for c in counters]

        modeled, diag = _fit_clock(
            counters,
            host_ns,
            expected_rate_hz=expected_hz,
            label="frame",
            max_rate_error_ppm=50.0,
            max_residual_p95_ms=0.05,
        )
        self.assertEqual(len(modeled), len(counters))
        self.assertLess(abs(diag.rate_error_ppm or 0.0), 1.0)
        self.assertLess(diag.residual_max_ms, 0.001)


if __name__ == "__main__":
    unittest.main()
