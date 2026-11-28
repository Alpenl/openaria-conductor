"""Unit tests for the MIRO-EGO exFAT SD-card exporter (recordings, SHA-256, logs, sudo)."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from miro_ego_host import disks
from miro_ego_host.card_export import export as tool
from miro_ego_host.tests.fixtures import (
    build_exfat_image,
    build_session_manifest,
    wrap_mbr_disk,
)


def _sample_session_files(*, seed: int = 0) -> tuple[dict[str, bytes], bytes]:
    left = bytes((i + seed) % 256 for i in range(256)) * 41  # > 2 exFAT clusters (10496 B)
    right = bytes((255 - i + seed) % 256 for i in range(256)) * 37  # 9472 B
    wav = b"RIFF" + b"\x00" * 40 + (b"\x01\x02\x03\x04" * 2000)  # 8044 B
    frames = b'{"frame":0,"host_monotonic_ns":1000}\n{"frame":1,"host_monotonic_ns":34333}\n'
    imu = b'{"seq":0,"ax":1,"ay":2,"az":3}\n'
    session_log = b'{"event":"capture_started"}\n'
    files = {
        "video/left_00000.mp4": left,
        "video/right_00000.mp4": right,
        "audio/audio_00000.wav": wav,
        "frames.ndjson": frames,
        "imu.ndjson": imu,
        "logs/capture.ndjson": session_log,
    }
    manifest = build_session_manifest(
        files,
        session_id=f"0198c9a8-7a3c-7000-8000-{seed:012d}",
        extra_log_paths=("logs/capture.ndjson",),
    )
    manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")
    return files, manifest_bytes


def _session_tree(files: dict[str, bytes], manifest_bytes: bytes) -> dict[str, object]:
    return {
        "manifest.json": manifest_bytes,
        "frames.ndjson": files["frames.ndjson"],
        "imu.ndjson": files["imu.ndjson"],
        "video": {
            "left_00000.mp4": files["video/left_00000.mp4"],
            "right_00000.mp4": files["video/right_00000.mp4"],
        },
        "audio": {
            "audio_00000.wav": files["audio/audio_00000.wav"],
        },
        "logs": {
            "capture.ndjson": files["logs/capture.ndjson"],
        },
    }


class CardExportExfatTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

        self.files1, self.manifest1 = _sample_session_files(seed=1)
        self.files2, self.manifest2 = _sample_session_files(seed=2)
        self.watchdog_log = b"2026-11-26T14:00:00Z wlan0 recovered\n" * 40
        self.app_log = b"INFO miro-ego serve ready\n"

        tree = {
            "recordings": {
                "2026-11-26": {
                    "14-30-05": _session_tree(self.files1, self.manifest1),
                    "14-30-05_02": _session_tree(self.files2, self.manifest2),
                },
                # Legacy top-level UUID folder must be ignored!
                "0198c9a8-7a3c-7000-8000-000000000099": {
                    "manifest.json": b'{"legacy":true}',
                },
            },
            # Legacy top-level /sessions folder must be ignored!
            "sessions": {
                "2026-11-26": {
                    "10-00-00": {"manifest.json": b'{"legacy":true}'},
                },
            },
            "logs": {
                "miro-ego.log": self.app_log,
                "wifi-watchdog": {
                    "diagnostic-20261126T140000Z.log": self.watchdog_log,
                },
            },
            "updates": {},
            ".miro-ego": {
                "volume.json": b'{"schema":"miro-ego.volume.v1","label":"MIRO-EGO"}\n',
            },
        }
        exfat_bytes = build_exfat_image(
            tree,
            label="MIRO-EGO",
            dirty=False,
            no_fat_chain_paths={"recordings/2026-11-26/14-30-05/audio/audio_00000.wav"},
        )
        # Wrap inside a 3-partition MBR disk image (dummy p1/p2 + real exFAT p3).
        disk_bytes = wrap_mbr_disk(
            {
                1: (0x0C, 64, b"FAT32-DUMMY" + bytes(4096)),
                2: (0x83, 128, b"EXT4-DUMMY" + bytes(4096)),
                3: (0x07, 256, exfat_bytes),
            }
        )
        self.card = self.root / "miro-ego-card.img"
        self.card.write_bytes(disk_bytes)

    def test_mbr_exfat_exports_verified_recordings_and_logs_without_legacy_baggage(self) -> None:
        before_sha = disks.sha256_file(self.card)
        dest = self.root / "out"
        dest.mkdir()

        with tool.volumes(self.card) as found:
            self.assertEqual(len(found), 1)
            label, _fs, unclean, volume_label = found[0]
            self.assertEqual(label, "partition-3")
            self.assertFalse(unclean)
            self.assertEqual(volume_label, "MIRO-EGO")

            records = tool.recordings(found)
            self.assertEqual(
                [r["id"] for r in records],
                [
                    "partition-3/recordings/2026-11-26/14-30-05",
                    "partition-3/recordings/2026-11-26/14-30-05_02",
                ],
            )
            self.assertTrue(all(r["sealed"] for r in records))

            logs = tool.diagnostic_logs(found)
            self.assertEqual([item["id"] for item in logs], ["partition-3/logs"])

            output = tool.export_selected(records, dest, log_sources=logs)

        self.assertTrue(output.name.startswith("Miro-Ego-export-"))
        report = json.loads((output / "export-report.json").read_bytes())
        self.assertEqual(report["schema"], "miro-ego.card-export-report.v1")
        self.assertEqual(report["status"], "complete")
        self.assertFalse(report["source_dirty"])
        # 2 recordings * 6 artifacts each = 12 verified manifest artifacts.
        self.assertEqual(report["verified_artifacts"], 12)
        # 2 recordings * 7 files + 2 diagnostic log files = 16 exported files.
        self.assertEqual(len(report["files"]), 16)
        for entry in report["files"]:
            exported = output / entry["path"]
            self.assertEqual(disks.sha256_file(exported), entry["sha256"])
            self.assertEqual(exported.stat().st_size, entry["bytes"])

        # Verify exact bytes of multi-cluster FAT-chained MP4, NoFatChain WAV, and diagnostic logs.
        rec1 = output / "partition-3/recordings/2026-11-26/14-30-05"
        self.assertEqual(
            (rec1 / "video/left_00000.mp4").read_bytes(), self.files1["video/left_00000.mp4"]
        )
        self.assertEqual(
            (rec1 / "audio/audio_00000.wav").read_bytes(), self.files1["audio/audio_00000.wav"]
        )
        self.assertEqual(
            (
                output / "partition-3/logs/wifi-watchdog/diagnostic-20261126T140000Z.log"
            ).read_bytes(),
            self.watchdog_log,
        )
        self.assertEqual((output / "partition-3/logs/miro-ego.log").read_bytes(), self.app_log)

        # Source card image must remain bit-for-bit unmodified.
        self.assertEqual(disks.sha256_file(self.card), before_sha)

    def test_unknown_date_unsynced_clock_recordings_export_and_verify(self) -> None:
        files_u1, manifest_u1 = _sample_session_files(seed=21)
        exfat_img = self.root / "unknown-date.exfat"
        exfat_img.write_bytes(
            build_exfat_image(
                {
                    "recordings": {
                        "unknown-date": {
                            "recording_01": _session_tree(files_u1, manifest_u1),
                            # Non-canonical child under unknown-date must be ignored.
                            "not_a_recording": {"manifest.json": b"{}"},
                        },
                        # Legacy UUID folder still ignored.
                        "0198c9a8-7a3c-7000-8000-000000000088": {"manifest.json": b"{}"},
                    },
                    "sessions": {"unknown-date": {"recording_01": {"manifest.json": b"{}"}}},
                }
            )
        )
        dest = self.root / "unknown-out"
        dest.mkdir()
        with tool.volumes(exfat_img) as found:
            records = tool.recordings(found)
            self.assertEqual(
                [r["id"] for r in records], ["exfat/recordings/unknown-date/recording_01"]
            )
            self.assertTrue(records[0]["sealed"])
            output = tool.export_selected(records, dest)

        report = json.loads((output / "export-report.json").read_bytes())
        self.assertEqual(report["status"], "complete")
        self.assertEqual(report["verified_artifacts"], 6)
        exported_left = output / "exfat/recordings/unknown-date/recording_01/video/left_00000.mp4"
        self.assertEqual(exported_left.read_bytes(), files_u1["video/left_00000.mp4"])

    def test_logs_only_cli_export(self) -> None:
        dest = self.root / "logs-out"
        dest.mkdir()
        tool.main(["--source", str(self.card), "--destination", str(dest), "--logs-only"])
        export_dirs = list(dest.glob("Miro-Ego-export-*"))
        self.assertEqual(len(export_dirs), 1)
        report = json.loads((export_dirs[0] / "export-report.json").read_bytes())
        self.assertEqual(report["status"], "complete")
        self.assertEqual(report["recordings"], [])
        self.assertEqual(report["logs"], ["partition-3/logs"])
        self.assertEqual(len(report["files"]), 2)

    def test_tampered_artifact_fails_manifest_sha256_verification(self) -> None:
        files, manifest_bytes = _sample_session_files(seed=7)
        # Corrupt one byte in left video after manifest was computed.
        tampered = dict(files)
        tampered["video/left_00000.mp4"] = b"\xff" + files["video/left_00000.mp4"][1:]
        exfat_img = self.root / "tampered.exfat"
        exfat_img.write_bytes(
            build_exfat_image(
                {
                    "recordings": {
                        "2026-11-26": {"15-00-00": _session_tree(tampered, manifest_bytes)}
                    }
                }
            )
        )
        dest = self.root / "tampered-out"
        dest.mkdir()
        with tool.volumes(exfat_img) as found:
            records = tool.recordings(found)
            with self.assertRaisesRegex(RuntimeError, "Manifest SHA-256 mismatch"):
                tool.export_selected(records, dest)
        reports = list(dest.glob("Miro-Ego-export-*/export-report.json"))
        self.assertEqual(len(reports), 1)
        parsed = json.loads(reports[0].read_bytes())
        self.assertEqual(parsed["status"], "incomplete")
        self.assertIn("Manifest SHA-256 mismatch", parsed["error"])
        # Verify failed export unlinked .export-partial temporary files.
        self.assertEqual(list(dest.rglob("*.export-partial")), [])

    def test_undeclared_extra_file_rejected_in_sealed_session_and_report_oserror_suppressed(
        self,
    ) -> None:
        files, manifest_bytes = _sample_session_files(seed=17)
        tree = _session_tree(files, manifest_bytes)
        tree["rogue_extra.txt"] = b"undeclared"
        exfat_img = self.root / "extra-file.exfat"
        exfat_img.write_bytes(build_exfat_image({"recordings": {"2026-11-26": {"15-05-00": tree}}}))
        dest = self.root / "extra-out"
        dest.mkdir()
        with tool.volumes(exfat_img) as found:
            records = tool.recordings(found)
            with self.assertRaisesRegex(RuntimeError, "undeclared file"):
                tool.export_selected(records, dest)
            output = tool.export_selected(records, dest, allow_unsealed=True)
            self.assertTrue(
                (output / "exfat/recordings/2026-11-26/15-05-00/rogue_extra.txt").is_file()
            )

        # Verify OSError while writing export-report.json in finally never masks original error.
        tampered = dict(files)
        tampered["imu.ndjson"] = b"X" + files["imu.ndjson"][1:]
        fail_img = self.root / "report-oserror.exfat"
        fail_img.write_bytes(
            build_exfat_image(
                {
                    "recordings": {
                        "2026-11-26": {"15-06-00": _session_tree(tampered, manifest_bytes)}
                    }
                }
            )
        )
        orig_open = Path.open

        def fail_report_open(self_path: Path, *args: object, **kwargs: object):
            if self_path.name == "export-report.json":
                raise OSError("ENOSPC writing export-report.json")
            return orig_open(self_path, *args, **kwargs)  # type: ignore[arg-type]

        with (
            tool.volumes(fail_img) as found,
            patch.object(Path, "open", fail_report_open),
            self.assertRaisesRegex(RuntimeError, "Manifest SHA-256 mismatch"),
        ):
            tool.export_selected(tool.recordings(found), dest)

    def test_missing_manifest_artifact_and_bad_schema_rejected(self) -> None:
        files, manifest_bytes = _sample_session_files(seed=8)
        tree = _session_tree(files, manifest_bytes)
        del tree["imu.ndjson"]  # type: ignore[arg-type]
        exfat_img = self.root / "missing.exfat"
        exfat_img.write_bytes(build_exfat_image({"recordings": {"2026-11-26": {"15-10-00": tree}}}))
        dest = self.root / "missing-out"
        dest.mkdir()
        with tool.volumes(exfat_img) as found:
            records = tool.recordings(found)
            with self.assertRaisesRegex(RuntimeError, "missing manifest artifact"):
                tool.export_selected(records, dest)

        # Bad manifest schema rejected.
        bad_manifest = json.dumps(build_session_manifest(files, schema="legacy.session.v0")).encode(
            "utf-8"
        )
        bad_img = self.root / "bad-schema.exfat"
        bad_img.write_bytes(
            build_exfat_image(
                {"recordings": {"2026-11-26": {"15-20-00": _session_tree(files, bad_manifest)}}}
            )
        )
        with tool.volumes(bad_img) as found:
            records = tool.recordings(found)
            self.assertFalse(records[0]["sealed"])
            self.assertIsNotNone(records[0]["manifest_error"])
            with self.assertRaisesRegex(RuntimeError, "Invalid session manifest"):
                tool.export_selected(records, dest)

    def test_dirty_exfat_volume_refused_by_default_and_valid_data_length_zero_padded(self) -> None:
        files, _ = _sample_session_files(seed=9)
        padded_bytes = b"ABCD" + b"\x00" * 4092  # 4096 bytes, valid_length=4
        files["imu.ndjson"] = padded_bytes
        manifest_bytes = json.dumps(
            build_session_manifest(files, extra_log_paths=("logs/capture.ndjson",))
        ).encode("utf-8")
        dirty_img = self.root / "dirty.exfat"
        dirty_img.write_bytes(
            build_exfat_image(
                {"recordings": {"2026-11-26": {"16-00-00": _session_tree(files, manifest_bytes)}}},
                dirty=True,
                valid_length_overrides={"recordings/2026-11-26/16-00-00/imu.ndjson": 4},
            )
        )
        dest = self.root / "dirty-out"
        dest.mkdir()
        with tool.volumes(dirty_img) as found:
            records = tool.recordings(found)
            self.assertTrue(records[0]["dirty"])
            with self.assertRaisesRegex(RuntimeError, "cleanly"):
                tool.export_selected(records, dest)
            output = tool.export_selected(records, dest, allow_dirty=True)
        report = json.loads((output / "export-report.json").read_bytes())
        self.assertTrue(report["source_dirty"])
        self.assertEqual(
            (output / "exfat/recordings/2026-11-26/16-00-00/imu.ndjson").read_bytes(),
            padded_bytes,
        )

    def test_mounted_directory_export_and_destination_guard(self) -> None:
        mounted = self.root / "MIRO-EGO"
        files, manifest_bytes = _sample_session_files(seed=11)
        rec_dir = mounted / "recordings/2026-11-26/17-00-00"
        (rec_dir / "video").mkdir(parents=True)
        (rec_dir / "audio").mkdir(parents=True)
        (rec_dir / "logs").mkdir(parents=True)
        (rec_dir / "manifest.json").write_bytes(manifest_bytes)
        for rel, data in files.items():
            (rec_dir / rel).write_bytes(data)
        (mounted / "logs").mkdir(parents=True)
        (mounted / "logs/service.log").write_bytes(b"ok\n")

        # Exporting into the mounted source tree must be rejected.
        with self.assertRaisesRegex(RuntimeError, "source card"):
            tool.destination_check(mounted, mounted / "logs")

        dest = self.root / "mounted-out"
        dest.mkdir()
        with tool.volumes(mounted) as found:
            output = tool.export_selected(
                tool.recordings(found), dest, log_sources=tool.diagnostic_logs(found)
            )
        report = json.loads((output / "export-report.json").read_bytes())
        self.assertEqual(report["status"], "complete")
        self.assertEqual(report["verified_artifacts"], 6)

    @unittest.skipUnless(os.name == "posix", "sudo UID/GID ownership restoration is POSIX-only")
    def test_sudo_ownership_restoration_chowns_dirs_files_and_report(self) -> None:
        dest = self.root / "sudo-out"
        dest.mkdir()
        chowned: list[tuple[Path, int, int]] = []

        def fake_chown(path: Path, uid: int, gid: int) -> None:
            chowned.append((Path(path), uid, gid))

        with (
            patch.object(os, "geteuid", return_value=0),
            patch.dict(os.environ, {"SUDO_UID": "1000", "SUDO_GID": "1000"}),
            patch.object(os, "chown", side_effect=fake_chown),
            tool.volumes(self.card) as found,
        ):
            output = tool.export_selected(tool.recordings(found)[:1], dest)

        chowned_paths = {p for p, uid, gid in chowned if (uid, gid) == (1000, 1000)}
        self.assertIn(output, chowned_paths)
        self.assertIn(output / "export-report.json", chowned_paths)
        self.assertIn(
            output / "partition-3/recordings/2026-11-26/14-30-05/manifest.json", chowned_paths
        )

    def test_portable_names_and_linux_destination_check(self) -> None:
        for bad in ("..", "../escape", "a/b", "a\\b", "CON", "a:b", "x.", "NUL.mp4"):
            with self.assertRaises(RuntimeError):
                tool.safe_name(bad)
        for bad_rel in (
            "/abs.mp4",
            "../up.mp4",
            "manifest.json",
            "recording.json",
            "capture.json",
            "segments.json",
            "checkpoint.json",
            "audio/checkpoint.json",
            "video/a.tmp",
            "video\\a.mp4",
        ):
            with self.assertRaises(RuntimeError, msg=bad_rel):
                tool.safe_artifact_relpath(bad_rel)
        self.assertEqual(tool.safe_name("left_00000.mp4"), "left_00000.mp4")

        with (
            patch.object(disks.platform, "system", return_value="Linux"),
            patch.object(disks, "run", return_value=b"/dev/sdb3\n"),
            self.assertRaisesRegex(RuntimeError, "source card"),
        ):
            tool.destination_check("/dev/sdb", self.root)

        # Prefix-collision device /dev/sdba3 must NOT falsely block /dev/sdb!
        with (
            patch.object(disks.platform, "system", return_value="Linux"),
            patch.object(disks, "run", return_value=b"/dev/sdba3\n"),
        ):
            self.assertEqual(tool.destination_check("/dev/sdb", self.root), self.root.resolve())

    def test_case_insensitive_exfat_artifacts_verify_and_reject_folded_dups(self) -> None:
        files, manifest_bytes = _sample_session_files(seed=33)
        upper_tree = {
            "MANIFEST.JSON": manifest_bytes,
            "FRAMES.NDJSON": files["frames.ndjson"],
            "IMU.NDJSON": files["imu.ndjson"],
            "VIDEO": {
                "LEFT_00000.MP4": files["video/left_00000.mp4"],
                "RIGHT_00000.MP4": files["video/right_00000.mp4"],
            },
            "AUDIO": {
                "AUDIO_00000.WAV": files["audio/audio_00000.wav"],
            },
            "LOGS": {
                "CAPTURE.NDJSON": files["logs/capture.ndjson"],
            },
        }
        exfat_img = self.root / "uppercase.exfat"
        exfat_img.write_bytes(
            build_exfat_image({"recordings": {"2026-11-26": {"18-00-00": upper_tree}}})
        )
        dest = self.root / "uppercase-out"
        dest.mkdir()
        with tool.volumes(exfat_img) as found:
            records = tool.recordings(found)
            self.assertEqual(len(records), 1)
            self.assertTrue(records[0]["sealed"])
            output = tool.export_selected(records, dest)

        report = json.loads((output / "export-report.json").read_bytes())
        self.assertEqual(report["status"], "complete")
        self.assertEqual(report["verified_artifacts"], 6)
        exported_left = output / "exfat/recordings/2026-11-26/18-00-00/video/left_00000.mp4"
        self.assertEqual(exported_left.read_bytes(), files["video/left_00000.mp4"])

        # Case-folded duplicate paths inside manifest.json must be rejected.
        dup_manifest = build_session_manifest(files, extra_log_paths=("logs/capture.ndjson",))
        dup_manifest["logs"].append(
            {
                "artifact_id": "0" * 64,
                "role": "log.diagnostics",
                "path": "LOGS/CAPTURE.NDJSON",
                "media_type": "application/x-ndjson",
                "bytes": len(files["logs/capture.ndjson"]),
                "sha256": "0" * 64,
            }
        )
        with self.assertRaisesRegex(RuntimeError, "Duplicate artifact path"):
            tool.manifest_artifacts(dup_manifest)

    def test_exports_v2_synced_fmp4_session_with_sha256_verification(self) -> None:
        import hashlib

        session_mp4 = b"\x00\x00\x00\x18ftypisom\x00\x00\x02\x00isomiso6\x00\x00\x00\x08moov"
        digest = hashlib.sha256(session_mp4).hexdigest()
        v2_manifest = {
            "schema": "miro-ego.device-session.v2",
            "manifest_id": "0198c9a8-7a3c-7000-8000-000000000090",
            "sealed": True,
            "sealed_at": "2026-11-28T04:00:00Z",
            "session_id": "0198c9a8-7a3c-7000-8000-000000000091",
            "volume_id": "2c9d0e1f-4444-4000-8000-000000000001",
            "capture_mode": "production",
            "display_name": "V2 Take",
            "device": {
                "device_id": "2c9d0e1f-4444-4000-8000-000000000002",
                "device_label": "Miro-Ego-12345678",
                "hardware_fingerprint": f"sha256:{'a' * 64}",
                "platform": "rdk-x5",
                "software_version": "0.3.0",
                "commit": "a" * 40,
            },
            "time": {
                "started_at": "2026-11-28T03:59:55Z",
                "ended_at": "2026-11-28T04:00:00Z",
                "timezone": "UTC",
                "duration_seconds": 1.0,
                "duration_clock": "audio_hardware_samples",
            },
            "take": {
                "take_id": "0198c9a8-7a3c-7000-8000-000000000091",
                "sequence": 1,
                "continuation_of": None,
            },
            "camera": {
                "width": 3840,
                "height": 1080,
                "eye_width": 1920,
                "sensor_fps": 30.0,
                "frame_decimation": 1,
                "effective_fps": 30.0,
                "coordinate_frame": "opencv_optical",
            },
            "media": {
                "container": "fmp4",
                "layout": "synced-fmp4",
                "timescale": 48000,
                "duration_ticks": 48000,
                "fragment_count": 1,
                "artifact": {
                    "artifact_id": digest,
                    "role": "media.session",
                    "path": "session.mp4",
                    "media_type": "video/mp4",
                    "bytes": len(session_mp4),
                    "sha256": digest,
                },
            },
            "video": {
                "layout": "split-eyes",
                "codec": "hevc",
                "container": "fmp4",
                "frame_count": 30,
                "tracks": {"left_track_id": 1, "right_track_id": 2},
                "encoding": {"schema": "miro-ego.recording-encoding.v1"},
            },
            "imu": {
                "embedded_in": "media.session",
                "track_id": 4,
                "mime_type": "application/x-miro-ego-sync+json",
                "sample_count": 200,
                "units": "raw_int16",
                "coordinate_frame": "raw_device_axes",
            },
            "frames": {
                "embedded_in": "media.session",
                "track_id": 4,
                "mime_type": "application/x-miro-ego-sync+json",
                "count": 30,
            },
            "audio": {
                "state": "recorded",
                "requested_mode": "enabled",
                "resolved_mode": "enabled",
                "codec": "flac",
                "container": "fmp4",
                "track_id": 3,
                "sample_format": "S16_LE",
                "sample_rate": 48000,
                "channels": 2,
                "sample_count": 48000,
                "sync": {
                    "time_base": "audio_hardware_samples",
                    "timescale": 48000,
                    "boundary_gap_samples": 0,
                },
            },
            "logs": [],
            "integrity": {
                "verified_at": "2026-11-28T04:00:00Z",
                "dropped_frames": 0,
                "drop_events": [],
                "fatal_errors": [],
            },
        }
        v2_tree = {
            "manifest.json": json.dumps(v2_manifest).encode("utf-8"),
            "session.mp4": session_mp4,
        }
        exfat_img = self.root / "v2.exfat"
        exfat_img.write_bytes(
            build_exfat_image({"recordings": {"2026-11-28": {"12-00-00": v2_tree}}})
        )
        dest = self.root / "v2-out"
        dest.mkdir()
        with tool.volumes(exfat_img) as found:
            records = tool.recordings(found)
            self.assertEqual(len(records), 1)
            self.assertTrue(records[0]["sealed"])
            output = tool.export_selected(records, dest)
        report = json.loads((output / "export-report.json").read_bytes())
        self.assertEqual(report["status"], "complete")
        self.assertEqual(report["verified_artifacts"], 1)
        self.assertEqual(
            (output / "exfat/recordings/2026-11-28/12-00-00/session.mp4").read_bytes(),
            session_mp4,
        )


if __name__ == "__main__":
    unittest.main()
