from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.hardware.probe import collect_hardware_facts


class HardwareProbeTest(unittest.TestCase):
    def _target_for_model(self, model: str) -> dict[str, object]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "proc/device-tree").mkdir(parents=True)
            (root / "proc/device-tree/model").write_text(model + "\x00")
            usb = root / "sys/bus/usb/devices/1-1"
            usb.mkdir(parents=True)
            (usb / "idVendor").write_text("1BCF\n")
            (usb / "idProduct").write_text("0B15\n")
            return collect_hardware_facts(
                sys_root=root / "sys",
                proc_root=root / "proc",
                etc_root=root / "etc",
                dev_root=root / "dev",
                storage_path=root,
                v4l2_ctl="",
            )["target"]

    def test_collects_fake_linux_hardware_without_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "proc/device-tree").mkdir(parents=True)
            (root / "proc/device-tree/model").write_text("Raspberry Pi 5 Model B\x00")
            (root / "proc/meminfo").write_text("MemTotal:       8192000 kB\n")
            (root / "etc").mkdir()
            (root / "etc/os-release").write_text('ID=debian\nVERSION_ID="12"\n')
            video = root / "sys/class/video4linux/video0"
            video.mkdir(parents=True)
            (video / "name").write_text("YLX Stereo Camera\n")
            (video / "index").write_text("0\n")
            usb = root / "sys/bus/usb/devices/1-1"
            usb.mkdir(parents=True)
            (usb / "idVendor").write_text("1BCF\n")
            (usb / "idProduct").write_text("0B15\n")
            (usb / "serial").write_text("secret-device-id\n")
            dev = root / "dev"
            dev.mkdir()
            (dev / "video0").touch()

            with (
                patch("platform.machine", return_value="aarch64"),
                patch("platform.release", return_value="test-kernel"),
            ):
                facts = collect_hardware_facts(
                    sys_root=root / "sys",
                    proc_root=root / "proc",
                    etc_root=root / "etc",
                    dev_root=dev,
                    storage_path=root,
                    v4l2_ctl="",
                )

            self.assertTrue(facts["platform"]["raspberry_pi"])
            self.assertEqual(facts["platform"]["memory_total_kib"], 8192000)
            self.assertEqual(facts["video_devices"][0]["name"], "YLX Stereo Camera")
            self.assertEqual(facts["usb_devices"][0]["vendor_id"], "1bcf")
            self.assertNotIn("secret-device-id", str(facts))

    def test_identifies_the_only_supported_rdk_x5_and_ylx_2uq2_pair(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "proc/device-tree").mkdir(parents=True)
            (root / "proc/device-tree/model").write_text("D-Robotics RDK X5 V1.0\x00")
            (root / "proc/meminfo").write_text("MemTotal:       8192000 kB\n")
            (root / "etc").mkdir()
            (root / "etc/os-release").write_text(
                'ID=ubuntu\nVERSION_ID="22.04"\nPRETTY_NAME="Ubuntu 22.04.5 LTS"\n'
            )
            video = root / "sys/class/video4linux/video0"
            video.mkdir(parents=True)
            (video / "name").write_text("YLX 2UQ2\n")
            (video / "index").write_text("0\n")
            usb = root / "sys/bus/usb/devices/1-1"
            usb.mkdir(parents=True)
            (usb / "idVendor").write_text("1BCF\n")
            (usb / "idProduct").write_text("0B15\n")
            (usb / "bcdDevice").write_text("0100\n")
            (usb / "product").write_text("YLX 2UQ2\n")
            dev = root / "dev"
            dev.mkdir()
            (dev / "video0").touch()

            with (
                patch("platform.machine", return_value="aarch64"),
                patch("platform.release", return_value="6.1.83"),
            ):
                facts = collect_hardware_facts(
                    sys_root=root / "sys",
                    proc_root=root / "proc",
                    etc_root=root / "etc",
                    dev_root=dev,
                    storage_path=root,
                    v4l2_ctl="",
                )

            self.assertEqual(
                facts["target"],
                {
                    "board": "rdk_x5_v1.0",
                    "camera": "ylx_2uq2",
                    "supported": True,
                    "reason": "matched",
                },
            )
            self.assertEqual(facts["usb_devices"][0]["device_release_bcd"], "0100")

    def test_board_model_match_is_normalized_but_exact(self) -> None:
        normalized = self._target_for_model("  d-robotics   rdk x5 v1.0  ")
        suffixed = self._target_for_model("D-Robotics RDK X5 V1.0 Plus")

        self.assertTrue(normalized["supported"])
        self.assertEqual(
            suffixed,
            {
                "board": "unsupported",
                "camera": "ylx_2uq2",
                "supported": False,
                "reason": "unsupported_board",
            },
        )


if __name__ == "__main__":
    unittest.main()
