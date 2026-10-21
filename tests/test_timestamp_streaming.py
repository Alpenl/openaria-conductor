from __future__ import annotations

import csv
import hashlib
import io
import json
import random
import tempfile
import unittest
from bisect import bisect_left
from pathlib import Path
from statistics import median
from unittest.mock import patch

from rp_ylx import session_timestamps as timestamps
from rp_ylx.api.downloads import iter_device_session_v1_artifacts
from rp_ylx.recording.storage import compact_metadata
from tests.test_spectacular_adapter import _json_bytes, _make_device_session


class TimestampStreamingTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = _make_device_session(Path(self.temporary.name))
        self.manifest_path = self.root / "manifest.json"
        self.manifest = json.loads(self.manifest_path.read_bytes())
        self.imu_path = self.root / self.manifest["imu"]["artifact"]["path"]
        self.template = json.loads(self.imu_path.read_bytes().splitlines()[0])
        self.output = self.root / "timestamps.csv"

    def save(self, rows, *, section="imu"):
        descriptor = self.manifest[section]["artifact"]
        data = b"".join(_json_bytes(row) for row in rows)
        (self.root / descriptor["path"]).write_bytes(data)
        digest = hashlib.sha256(data).hexdigest()
        descriptor.update(bytes=len(data), sha256=digest, artifact_id=digest)
        self.manifest[section]["sample_count" if section == "imu" else "count"] = len(rows)
        self.manifest_path.write_bytes(_json_bytes(self.manifest))

    def packets(self, hosts, sizes):
        rows = []
        for packet, (host, size) in enumerate(zip(hosts, sizes, strict=True)):
            for index in range(size):
                row = dict(self.template)
                row.update(
                    sequence=len(rows),
                    packet_sequence=packet,
                    sample_index=index,
                    host_monotonic_ns=host,
                    host_read_start_ns=host - 500,
                    host_read_end_ns=host + 500,
                    device_ticks=1000 + len(rows),
                    device_timestamp_raw=1000 + len(rows),
                )
                rows.append(row)
        self.save(rows)
        return rows

    def export(self):
        report = timestamps.build_session_timestamp_report(self.root, output=self.output)
        with self.output.open(newline="") as stream:
            return report, list(csv.DictReader(stream))

    def assert_no_temporary(self):
        self.assertEqual(list(self.root.glob(".timestamps.csv.*.tmp")), [])

    def test_variable_packets_and_nearest_matching_against_bisect(self):
        rng = random.Random(77104)
        for case in range(40):
            with self.subTest(case=case):
                sizes = [rng.randint(1, 7) for _ in range(rng.randint(1, 12))]
                host = 975_000_000
                hosts = []
                for _ in sizes:
                    host += rng.choice([0, 1_000_000, 20_000_000, 40_000_000])
                    hosts.append(host)
                samples = self.packets(hosts, sizes)
                rate = (
                    None
                    if len(hosts) < 2 or hosts[-1] == hosts[0]
                    else ((len(hosts) - 1) * 1e9 / (hosts[-1] - hosts[0]))
                )
                record_rate = None if rate is None else rate * float(median(sizes))
                estimated = []
                previous = None
                for host, size in zip(hosts, sizes, strict=True):
                    period = 0 if record_rate is None else 1e9 / record_rate
                    if previous is not None:
                        period = min(period, (host - previous) / size)
                    estimated.extend(host - (size - 1 - index) * period for index in range(size))
                    previous = host
                if record_rate is None or any(
                    b <= a for a, b in zip(estimated, estimated[1:], strict=False)
                ):
                    estimated = [float(sample["host_monotonic_ns"]) for sample in samples]
                report, rows = self.export()
                self.assertEqual(
                    report["imu"],
                    {
                        "samples": len(samples),
                        "packets": len(sizes),
                        "median_samples_per_packet": median(sizes),
                        "packet_rate_hz": rate,
                        "record_rate_hz": record_rate,
                    },
                )
                for row in rows:
                    target = int(row["left_eye_host_monotonic_ns"])
                    at = bisect_left(estimated, target)
                    if at == len(estimated) or (
                        at and abs(estimated[at - 1] - target) <= abs(estimated[at] - target)
                    ):
                        at -= 1
                    self.assertEqual(int(row["nearest_imu_sample_number"]), at)
                    self.assertEqual(
                        int(row["nearest_imu_estimated_monotonic_ns"]), round(estimated[at])
                    )
                    self.assertEqual(
                        float(row["nearest_imu_delta_ms"]), (estimated[at] - target) / 1e6
                    )
                self.assert_no_temporary()

    def test_equal_distance_chooses_earlier_and_duplicates_choose_bisect_left(self):
        samples = []
        for i, when in enumerate([90.0, 110.0, 110.0, 130.0]):
            sample = dict(
                sample_number=i,
                host_monotonic_ns=int(when),
                device_timestamp_raw=i,
                device_ticks=i,
                packet_sequence=i,
                sample_index=0,
                sync_quality="good",
            )
            samples.append((sample, when))
        frames = [
            dict(frame_index=i, source_sequence=i, host_monotonic_ns=t)
            for i, t in enumerate([80, 100, 110, 120, 140])
        ]
        rows = list(timestamps._alignment_rows(frames, iter(samples)))
        self.assertEqual([row["nearest_imu_sample_number"] for row in rows], [0, 0, 1, 2, 3])

    def test_late_fallback_rewrites_all_rows(self):
        self.packets([1_000_000_000, 1_200_000_000, 1_200_000_000], [2, 2, 2])
        _, rows = self.export()
        # Duplicate times occur after the final video frame, but still require
        # replacing the early estimated rows with raw-host matches.
        self.assertEqual([int(row["nearest_imu_sample_number"]) for row in rows], [0, 1, 1, 1])
        self.assertTrue(
            all(int(row["nearest_imu_estimated_monotonic_ns"]) == 1_000_000_000 for row in rows)
        )
        self.assert_no_temporary()

    def test_single_packet_uses_host_times(self):
        self.packets([1_000_000_000], [3])
        report, rows = self.export()
        self.assertIsNone(report["imu"]["record_rate_hz"])
        self.assertEqual([int(row["nearest_imu_sample_number"]) for row in rows], [0, 2, 2, 2])

    def test_invalid_last_packet_rejected(self):
        for kind in (
            "sequence",
            "packet_sequence",
            "sample_index",
            "host",
            "session_id",
            "quality",
        ):
            with self.subTest(kind=kind):
                rows = self.packets([1_000_000_000, 1_200_000_000], [2, 2])
                if kind == "host":
                    rows[-1]["host_monotonic_ns"] += 1
                elif kind == "session_id":
                    rows[-1][kind] = "01999999-9999-7999-8999-999999999999"
                elif kind == "quality":
                    rows[-1]["sync"] = {"quality": "unknown"}
                else:
                    rows[-1][kind] += 1
                self.save(rows)
                with self.assertRaises(timestamps.SessionTimestampError):
                    self.export()

    def test_packet_host_regression_rejected(self):
        self.packets([1_000_000_000, 999_000_000], [2, 2])
        with self.assertRaisesRegex(timestamps.SessionTimestampError, "倒退"):
            self.export()

    def test_frame_session_mismatch_rejected(self):
        descriptor = self.manifest["frames"]["artifact"]
        rows = [
            json.loads(line) for line in (self.root / descriptor["path"]).read_bytes().splitlines()
        ]
        rows[-1]["session_id"] = "01999999-9999-7999-8999-999999999999"
        self.save(rows, section="frames")
        with self.assertRaises(timestamps.SessionTimestampError):
            self.export()

    def test_count_mismatch_rejected(self):
        self.manifest["imu"]["sample_count"] += 1
        self.manifest_path.write_bytes(_json_bytes(self.manifest))
        with self.assertRaises(timestamps.SessionTimestampError):
            self.export()

    def test_changed_input_after_first_pass_fails_before_publication(self):
        rows = self.packets([1_000_000_000, 1_200_000_000], [2, 2])
        self.output.write_text("previous export")
        original = timestamps._packet_summary

        def changed(*args):
            result = original(*args)
            rows[-1]["device_ticks"] += 1
            self.imu_path.write_bytes(b"".join(_json_bytes(row) for row in rows))
            return result

        with (
            patch.object(timestamps, "_packet_summary", changed),
            self.assertRaisesRegex(timestamps.SessionTimestampError, "校验不一致"),
        ):
            self.export()
        self.assertEqual(self.output.read_text(), "previous export")
        self.assert_no_temporary()

    def test_csv_write_replace_sync_and_cancel_failures_preserve_prior_output(self):
        original = timestamps._CheckedCsvOutput.write
        for failure in ("write", "short_write", "replace", "fsync", "cancel"):
            with self.subTest(failure=failure):
                self.output.write_text("previous export")
                if failure == "replace":
                    target, effect = "rp_ylx.session_timestamps.os.replace", OSError("replace")
                elif failure == "fsync":
                    target, effect = "rp_ylx.session_timestamps.os.fsync", OSError("fsync")
                else:
                    target = "rp_ylx.session_timestamps._CheckedCsvOutput.write"
                    effect = KeyboardInterrupt() if failure == "cancel" else OSError("disk full")
                if failure == "short_write":

                    def short_write(instance, text):
                        with patch.object(instance.stream, "write", return_value=1):
                            return original(instance, text)

                    replacement = patch(target, short_write)
                else:
                    replacement = patch(target, side_effect=effect)
                with (
                    replacement,
                    self.assertRaises(KeyboardInterrupt if failure == "cancel" else OSError),
                ):
                    self.export()
                self.assertEqual(self.output.read_text(), "previous export")
                self.assert_no_temporary()
                self.export()  # Retry does not collide with an abandoned temporary.

    def test_packet_spool_short_write_rejected(self):
        class Short(io.BytesIO):
            def write(self, data):
                return super().write(data[:-1])

        with self.assertRaisesRegex(OSError, "short timestamp packet"):
            timestamps._packet_summary(self.root, self.manifest, Short())

    def test_output_cannot_replace_any_source_or_symlink_alias(self):
        paths = [self.manifest_path]
        paths.extend(
            self.root / artifact["path"]
            for artifact in iter_device_session_v1_artifacts(self.manifest)
        )
        for path in paths:
            with self.subTest(path=path.name):
                original = path.read_bytes()
                with self.assertRaises(timestamps.SessionTimestampError):
                    timestamps.build_session_timestamp_report(self.root, output=path)
                self.assertEqual(path.read_bytes(), original)
        self.output.symlink_to(self.imu_path)
        with self.assertRaises(timestamps.SessionTimestampError):
            self.export()
        self.assertTrue(self.output.is_symlink())

    def test_compressed_imu_matches_raw_and_rejects_missing_checksum(self):
        raw_report, raw_rows = self.export()
        target, encoding = compact_metadata(self.imu_path)
        descriptor = self.manifest["imu"]["artifact"]
        data = target.read_bytes()
        descriptor.update(
            path=target.relative_to(self.root).as_posix(),
            media_type="application/zstd",
            bytes=len(data),
            sha256=hashlib.sha256(data).hexdigest(),
            artifact_id=hashlib.sha256(data).hexdigest(),
            storage_encoding=encoding,
        )
        self.manifest["schema"] = "ylx.device-session.v4"
        self.manifest_path.write_bytes(_json_bytes(self.manifest))
        # The public v4 contract additionally requires session encoding parameters;
        # isolate the reader here; full v4 validation is covered by recording tests.
        with patch.object(timestamps, "validate_public_session", return_value=self.manifest):
            report, rows = self.export()
            report["source_schema"] = raw_report["source_schema"]
            self.assertEqual(report, raw_report)
            self.assertEqual(rows, raw_rows)
            target.write_bytes(data[:-1])
            previous = self.output.read_bytes()
            with self.assertRaises(timestamps.SessionTimestampError):
                self.export()
            self.assertEqual(self.output.read_bytes(), previous)
            self.assert_no_temporary()

    def test_report_without_csv_matches_export(self):
        report, _ = self.export()
        report.pop("csv")
        self.assertEqual(timestamps.build_session_timestamp_report(self.root), report)


if __name__ == "__main__":
    unittest.main()
