from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.api.events import _validate_network, validate_capture_status, validate_device_descriptor
from rp_ylx.mdns import runtime_ipv4_addresses
from rp_ylx.runtime import _default_interface, _ipv4_addresses, collect_linux_runtime
from tests import test_capture_coordinator as fixture


class RuntimeInputIsolationTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.net = self.root / "net"
        self.net.mkdir()
        self.route = self.root / "route"
        self.route.write_bytes(b"Iface Destination Gateway Flags RefCnt Use Metric Mask\n")
        self.addresses = {"wlan0": ["192.0.2.2/24"], "eth0": ["198.51.100.2/24"]}

    def interface(self, name: str, state: bytes = b"up\n") -> None:
        path = self.net / name
        path.mkdir()
        (path / "operstate").write_bytes(state)

    def routes(self, *rows: bytes) -> None:
        with self.route.open("ab") as output:
            for row in rows:
                output.write(row + b"\n")

    def collect(self):
        result = collect_linux_runtime(
            net_root=self.net,
            route_path=self.route,
            thermal_paths=(),
            ipv4_lookup=lambda name: self.addresses.get(name, []),
        )
        _validate_network(result["network"])
        return result

    def test_bad_route_row_does_not_erase_healthy_default(self) -> None:
        self.interface("wlan0")
        self.routes(
            b"wl\xff 00000000 00000000 0003 0 0 0 00000000",
            b"wlan0 00000000 00000000 0003 0 0 100 00000000",
        )
        self.assertEqual(self.collect()["network"]["default_route"], "wifi_client")

    def test_invalid_operstate_does_not_block_address_lookup(self) -> None:
        self.interface("wlan0", b"\xff")
        self.assertEqual(
            self.collect()["network"]["wifi_client"]["addresses"], self.addresses["wlan0"]
        )

    def test_unsupported_names_do_not_hide_healthy_interface(self) -> None:
        for name in ("wl+test", "wl中", "eth" + "x" * 20, "eth0"):
            self.interface(name)
        self.routes(
            b"wl+test 00000000 00000000 0003 0 0 0 00000000",
            b"eth0 00000000 00000000 0003 0 0 100 00000000",
        )
        with patch.dict(self.addresses, {"wl+test": ["192.0.2.3/24"]}):
            result = self.collect()
        self.assertEqual(result["connection_method"], "ethernet_lan")
        self.assertIsNone(result["network"]["wifi_client"]["interface"])

    def test_unsupported_ioctl_names_never_query_a_truncated_alias(self) -> None:
        for name in ("wl中", "wl\udcff", "eth0\0alias", "eth" + "x" * 20, "wl+test", ""):
            with self.subTest(name=name), patch("rp_ylx.runtime.socket.socket") as socket:
                self.assertEqual(_ipv4_addresses(name), [])
                socket.assert_not_called()

    def test_zero_destination_with_nonzero_mask_is_not_default(self) -> None:
        self.routes(
            b"eth0 00000000 00000000 0001 0 0 0 000000FF",
            b"wlan0 00000000 00000000 0003 0 0 100 00000000",
        )
        self.assertEqual(_default_interface(self.route), "wlan0")

    def test_rejected_down_or_invalid_metric_routes_are_not_default(self) -> None:
        for flags, metric in (
            (b"0201", b"0"),
            (b"0000", b"0"),
            (b"0003", b"-1"),
            (b"0003", b"bad"),
        ):
            with self.subTest(flags=flags, metric=metric):
                self.route.write_bytes(b"header\n")
                self.routes(
                    b"eth0 00000000 00000000 " + flags + b" 0 0 " + metric + b" 00000000",
                    b"wlan0 00000000 00000000 0003 0 0 100 00000000",
                )
                self.assertEqual(_default_interface(self.route), "wlan0")

    def test_lookup_oserror_leaves_other_interface_available(self) -> None:
        self.interface("wlan0")
        self.interface("eth0")

        def lookup(name):
            if name == "wlan0":
                raise OSError("interface disappeared during lookup")
            return self.addresses[name]

        result = collect_linux_runtime(
            net_root=self.net, route_path=self.route, thermal_paths=(), ipv4_lookup=lookup
        )
        _validate_network(result["network"])
        self.assertEqual(result["connection_method"], "ethernet_direct")
        self.assertEqual(result["network"]["wired"]["addresses"], self.addresses["eth0"])

    def test_client_without_default_route_is_not_a_hotspot(self) -> None:
        self.interface("wlan0")
        result = self.collect()
        self.assertEqual(result["connection_method"], "wifi_client")
        self.assertEqual(result["network"]["default_route"], "none")
        self.assertEqual(result["network"]["ap"]["state"], "disabled")
        self.assertEqual(result["network"]["wifi_client"]["addresses"], self.addresses["wlan0"])

    def test_status_descriptor_and_mdns_survive_bad_network_row(self) -> None:
        self.interface("eth0")
        self.routes(
            b"wl\xff 00000000 00000000 0003 0 0 0 00000000",
            b"eth0 00000000 00000000 0003 0 0 100 00000000",
        )
        case = fixture.CaptureCoordinatorTest()
        case.setUp()
        self.addCleanup(case.tearDown)
        coordinator = case.coordinator(sources=fixture.FakeSources())
        self.addCleanup(coordinator.close)
        coordinator._runtime = self.collect
        status = coordinator.capture_status()
        validate_capture_status(status)
        descriptor = coordinator.device_descriptor("v4", "lab")
        validate_device_descriptor(descriptor, api_version="v4", security_profile="lab")
        with patch("rp_ylx.mdns.collect_linux_runtime", side_effect=self.collect):
            self.assertEqual(runtime_ipv4_addresses(), ("198.51.100.2",))


if __name__ == "__main__":
    unittest.main()
