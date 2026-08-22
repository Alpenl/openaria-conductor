from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from rp_ylx.runtime import collect_linux_runtime


class LinuxRuntimeTelemetryTest(unittest.TestCase):
    def test_reports_active_wifi_default_route_address_and_temperature(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            net_root = root / "sys/class/net"
            wlan = net_root / "wlan0"
            wlan.mkdir(parents=True)
            (wlan / "operstate").write_text("up\n", encoding="ascii")
            (wlan / "wireless").mkdir()
            eth = net_root / "eth0"
            eth.mkdir()
            (eth / "operstate").write_text("down\n", encoding="ascii")
            route = root / "proc/net/route"
            route.parent.mkdir(parents=True)
            route.write_text(
                "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\n"
                "wlan0\t00000000\t016EA8C0\t0003\t0\t0\t600\t00000000\n",
                encoding="ascii",
            )
            thermal = root / "sys/class/thermal/thermal_zone0/temp"
            thermal.parent.mkdir(parents=True)
            thermal.write_text("40359\n", encoding="ascii")

            runtime = collect_linux_runtime(
                net_root=net_root,
                route_path=route,
                thermal_paths=(thermal,),
                ipv4_lookup=lambda name: ["198.51.100.36/24"] if name == "wlan0" else [],
            )

        self.assertEqual(runtime["connection_method"], "wifi_client")
        self.assertEqual(runtime["temperature_celsius"], 40.359)
        self.assertEqual(runtime["network"]["default_route"], "wifi_client")
        self.assertEqual(runtime["network"]["wifi_client"]["state"], "connected")
        self.assertEqual(runtime["network"]["wifi_client"]["interface"], "wlan0")
        self.assertEqual(
            runtime["network"]["wifi_client"]["addresses"],
            ["198.51.100.36/24"],
        )
        self.assertEqual(runtime["network"]["wired"]["state"], "disconnected")

    def test_reports_wired_lan_when_ethernet_owns_default_route(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            net_root = root / "net"
            eth = net_root / "enp1s0"
            eth.mkdir(parents=True)
            (eth / "operstate").write_text("up\n", encoding="ascii")
            route = root / "route"
            route.write_text(
                "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\n"
                "enp1s0\t00000000\t0100000A\t0003\t0\t0\t100\t00000000\n",
                encoding="ascii",
            )
            thermal = root / "temp"
            thermal.write_text("52\n", encoding="ascii")

            runtime = collect_linux_runtime(
                net_root=net_root,
                route_path=route,
                thermal_paths=(thermal,),
                ipv4_lookup=lambda name: ["10.0.0.2/24"] if name == "enp1s0" else [],
            )

        self.assertEqual(runtime["connection_method"], "ethernet_lan")
        self.assertEqual(runtime["network"]["default_route"], "wired")
        self.assertEqual(runtime["network"]["wired"]["addresses"], ["10.0.0.2/24"])
        self.assertEqual(runtime["temperature_celsius"], 52.0)


if __name__ == "__main__":
    unittest.main()
