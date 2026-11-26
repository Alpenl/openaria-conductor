"""Unit tests for 3-partition MBR MIRO-EGO exFAT planning and offline .deb verification."""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from miro_ego_host.factory_recovery import partition_layout
from miro_ego_host.tests.fixtures import vendor_mbr


class PartitionLayoutTests(unittest.TestCase):
    def test_plan_creates_exact_three_partition_miro_ego_mbr_across_card_sizes(self) -> None:
        original = vendor_mbr()
        for size in (
            32_000_000_000,
            64_000_000_000,
            127_999_672_320,
            256 * 1024**3,
            512 * 1024**3,
        ):
            mbr, layout = partition_layout.plan(original, size)
            entries = partition_layout.partitions(mbr)

            # Boot code, 32-bit disk signature, p1 CONFIG, and p2 start preserved verbatim.
            self.assertEqual(mbr[:470], original[:470])
            self.assertEqual(entries[0], (0x0C, 8192, 524288))
            self.assertEqual(entries[1], (0x83, 532480, 20 * 1024**3 // 512))
            self.assertEqual(entries[2][0], 0x07)
            self.assertEqual(entries[2][1], entries[1][1] + entries[1][2])
            self.assertEqual(entries[2][1] % 2048, 0)
            self.assertEqual(entries[2][2] % 2048, 0)
            self.assertEqual(entries[3], (0, 0, 0))
            self.assertGreaterEqual(entries[2][2] * 512, 4 * 1024**3)
            self.assertLess(size - (entries[2][1] + entries[2][2]) * 512, 1024**2)

            self.assertEqual(layout["schema"], "miro-ego.storage.exfat.v1")
            self.assertEqual(layout["label"], "MIRO-EGO")
            self.assertEqual(layout["system_gib"], 20)
            self.assertEqual(layout["partuuid"], "db2e7205-03")
            self.assertEqual(layout["mbr_sha256"], hashlib.sha256(mbr).hexdigest())
            self.assertEqual(layout["exfatprogs_sha256"], partition_layout.EXFATPROGS_SHA256)
            self.assertEqual(layout["logrotate_sha256"], partition_layout.LOGROTATE_SHA256)

            with tempfile.TemporaryDirectory() as folder:
                image = Path(folder) / "planned.img"
                image.write_bytes(mbr)
                partition_layout.check_fit(image, size)
                with self.assertRaises(RuntimeError):
                    partition_layout.check_fit(image, size - 1024**2)

    def test_plan_rejects_invalid_bounds_and_nonvendor_mbr(self) -> None:
        for size, system in (
            (24 * 1024**3, 20),  # < 4 GiB remaining after p1+p2
            (20 * 1024**3 - 512, 20),  # below min card
            (512 * 1024**3 + 512, 20),  # above max card
            (64 * 1024**3 + 1, 20),  # non-sector multiple
            (64 * 1024**3, 16),  # non-20 GiB system
            (64 * 1024**3, 32),
        ):
            with self.assertRaises(RuntimeError, msg=f"size={size} system={system}"):
                partition_layout.plan(vendor_mbr(), size, system)

        # Reject non-empty p3/p4 slot bytes even if sector count is 0.
        tampered_slot = bytearray(vendor_mbr())
        tampered_slot[478] = 1
        with self.assertRaises(RuntimeError):
            partition_layout.plan(tampered_slot, 64 * 1024**3)

        # Reject missing disk signature.
        with self.assertRaises(RuntimeError):
            partition_layout.plan(vendor_mbr(disk_sig=b"\x00\x00\x00\x00"), 64 * 1024**3)

        # Reject invalid MBR signature.
        bad_sig = bytearray(vendor_mbr())
        bad_sig[510:512] = b"\x00\x00"
        with self.assertRaises(RuntimeError):
            partition_layout.plan(bad_sig, 64 * 1024**3)

    def test_offline_deb_sha256_verification_for_exfatprogs_and_logrotate(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            tools_dir = Path(folder)
            exfat_raw = b"exfatprogs-deb-fixture"
            exfat_sha = hashlib.sha256(exfat_raw).hexdigest()
            logrotate_raw = b"logrotate-deb-fixture"
            logrotate_sha = hashlib.sha256(logrotate_raw).hexdigest()

            exfat_path = tools_dir / partition_layout.EXFATPROGS_PACKAGE
            logrotate_path = tools_dir / partition_layout.LOGROTATE_PACKAGE
            exfat_path.write_bytes(exfat_raw)
            logrotate_path.write_bytes(logrotate_raw)

            self.assertEqual(
                partition_layout.prepare_tools(
                    tools_dir,
                    partition_layout.EXFATPROGS_PACKAGE,
                    partition_layout.EXFATPROGS_URL,
                    exfat_sha,
                ),
                exfat_path,
            )
            self.assertEqual(
                partition_layout.prepare_tools(
                    tools_dir,
                    partition_layout.LOGROTATE_PACKAGE,
                    partition_layout.LOGROTATE_URL,
                    logrotate_sha,
                ),
                logrotate_path,
            )

            # Tampered cached .deb must be rejected.
            with self.assertRaises(RuntimeError):
                partition_layout.verify_deb(exfat_path, "0" * 64)

            # Download path validates digest before replacing output.
            exfat_path.unlink()
            with (
                patch.object(partition_layout, "_download_package", return_value=b"corrupted"),
                self.assertRaises(RuntimeError),
            ):
                partition_layout.prepare_tools(
                    tools_dir,
                    partition_layout.EXFATPROGS_PACKAGE,
                    partition_layout.EXFATPROGS_URL,
                    exfat_sha,
                )
            self.assertFalse(exfat_path.exists())


if __name__ == "__main__":
    unittest.main()
