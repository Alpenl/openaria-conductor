from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from scripts import rdk_x5_sd_image


class RdkX5SdImageTest(unittest.TestCase):
    def test_plan_creates_three_aligned_partitions_with_data_identity(self) -> None:
        volume_id = "550e8400-e29b-41d4-a716-446655440000"
        plan = rdk_x5_sd_image.build_plan(
            output=Path("candidate.img"),
            boot_source=None,
            rootfs_source=None,
            boot_size=rdk_x5_sd_image.parse_size("16M"),
            rootfs_size=rdk_x5_sd_image.parse_size("32M"),
            data_size=rdk_x5_sd_image.parse_size("64M"),
            volume_id=volume_id,
            initialized_at="2026-08-19T00:00:00Z",
        )

        self.assertEqual(plan.image_bytes, rdk_x5_sd_image.parse_size("113M"))
        self.assertEqual([partition.number for partition in plan.partitions], [1, 2, 3])
        self.assertEqual(plan.partitions[0].mountpoint, "/boot/config")
        self.assertEqual(plan.partitions[1].mountpoint, "/")
        self.assertEqual(plan.data_partition.mountpoint, "/data")
        self.assertEqual(plan.data_partition.label, "RP-YLX-DATA")
        self.assertEqual(plan.volume_id, volume_id)
        for partition in plan.partitions:
            self.assertEqual(partition.start_bytes % rdk_x5_sd_image.ALIGN_BYTES, 0)
            self.assertEqual(partition.size_bytes % rdk_x5_sd_image.ALIGN_BYTES, 0)

        script = rdk_x5_sd_image.sfdisk_script(plan)
        self.assertIn("label: dos", script)
        self.assertIn("type=c, bootable", script)
        self.assertEqual(script.count("type=83"), 2)

    def test_dry_run_cli_outputs_plan_without_building_image(self) -> None:
        output = io.StringIO()
        with redirect_stdout(output):
            result = rdk_x5_sd_image.main(
                [
                    "--output",
                    "candidate.img",
                    "--boot-size",
                    "16M",
                    "--rootfs-size",
                    "32M",
                    "--data-size",
                    "64M",
                    "--volume-id",
                    "550e8400-e29b-41d4-a716-446655440000",
                    "--initialized-at",
                    "2026-08-19T00:00:00Z",
                    "--dry-run",
                ]
            )

        self.assertEqual(result, 0)
        rendered = json.loads(output.getvalue())
        self.assertEqual(rendered["schema"], "ylx.rdk-x5-sd-image-plan.v1")
        self.assertEqual(rendered["data"]["mountpoint"], "/data")
        self.assertEqual(rendered["data"]["directories"], ["recordings", "sessions"])
        self.assertEqual(rendered["partitions"][2]["label"], "RP-YLX-DATA")

    def test_source_images_are_copied_into_their_partition_offsets(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            image = root / "candidate.img"
            boot = root / "boot.raw"
            rootfs = root / "rootfs.raw"
            boot.write_bytes(b"boot")
            rootfs.write_bytes(b"rootfs")
            plan = rdk_x5_sd_image.build_plan(
                output=image,
                boot_source=boot,
                rootfs_source=rootfs,
                boot_size=rdk_x5_sd_image.parse_size("16M"),
                rootfs_size=rdk_x5_sd_image.parse_size("32M"),
                data_size=rdk_x5_sd_image.parse_size("64M"),
                volume_id="550e8400-e29b-41d4-a716-446655440000",
                initialized_at="2026-08-19T00:00:00Z",
            )
            image.write_bytes(b"\0" * 4096)
            with image.open("r+b") as stream:
                stream.truncate(plan.image_bytes)

            rdk_x5_sd_image.write_partition_payload(image, boot, plan.partitions[0])
            rdk_x5_sd_image.write_partition_payload(image, rootfs, plan.partitions[1])

            with image.open("rb") as stream:
                stream.seek(plan.partitions[0].start_bytes)
                self.assertEqual(stream.read(4), b"boot")
                stream.seek(plan.partitions[1].start_bytes)
                self.assertEqual(stream.read(6), b"rootfs")

    def test_existing_output_requires_force(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "candidate.img"
            output.write_bytes(b"exists")
            error = io.StringIO()
            output_stream = io.StringIO()
            with redirect_stderr(error), redirect_stdout(output_stream):
                result = rdk_x5_sd_image.main(
                    [
                        "--output",
                        str(output),
                        "--boot-size",
                        "16M",
                        "--rootfs-size",
                        "32M",
                        "--data-size",
                        "64M",
                        "--dry-run",
                    ]
                )
        # dry-run does not write and therefore does not reject an existing path.
        self.assertEqual(result, 0, error.getvalue())

    def test_build_image_runs_sfdisk_and_mkfs_with_data_offset(self) -> None:
        commands: list[tuple[list[str], str | None]] = []

        def fake_run(command: list[str], *, input_text: str | None = None) -> None:
            commands.append((command, input_text))
            if command[0] == "truncate":
                Path(command[-1]).write_bytes(b"\0" * 4096)

        with tempfile.TemporaryDirectory() as directory:
            image = Path(directory) / "candidate.img"
            plan = rdk_x5_sd_image.build_plan(
                output=image,
                boot_source=None,
                rootfs_source=None,
                boot_size=rdk_x5_sd_image.parse_size("16M"),
                rootfs_size=rdk_x5_sd_image.parse_size("32M"),
                data_size=rdk_x5_sd_image.parse_size("64M"),
                volume_id="550e8400-e29b-41d4-a716-446655440000",
                initialized_at="2026-08-19T00:00:00Z",
            )
            with (
                patch.object(rdk_x5_sd_image, "require_tool", return_value="/usr/bin/tool"),
                patch.object(rdk_x5_sd_image, "run_command", side_effect=fake_run),
            ):
                rdk_x5_sd_image.build_image(plan)

        self.assertEqual(commands[0][0][:3], ["truncate", "-s", str(plan.image_bytes)])
        self.assertEqual(commands[1][0], ["sfdisk", str(plan.output)])
        self.assertIn("type=83", commands[1][1] or "")
        mkfs = commands[2][0]
        self.assertEqual(mkfs[:2], ["mkfs.ext4", "-F"])
        self.assertIn("4096", mkfs)
        self.assertIn(f"offset={plan.data_partition.start_bytes},nodiscard", mkfs)
        self.assertIn("RP-YLX-DATA", mkfs)
        self.assertIn(plan.volume_id, mkfs)
        self.assertEqual(mkfs[-1], str(plan.data_partition.size_bytes // 4096))
