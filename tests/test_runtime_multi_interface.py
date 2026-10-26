from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.api.events import _validate_network, validate_capture_status, validate_device_descriptor
from rp_ylx.mdns import runtime_ipv4_addresses
from rp_ylx.runtime import collect_linux_runtime
from tests import test_capture_coordinator as fixture


class RuntimeMultiInterfaceTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.net = self.root / "net"
        self.net.mkdir()
        self.route = self.root / "route"
        self.route.write_text("Iface Destination Gateway Flags RefCnt Use Metric Mask\n")
        self.addresses = {}
        self.lookups = []

    def interface(self, name, state="up", addresses=(), wireless=False):
        path = self.net / name
        path.mkdir()
        (path / "operstate").write_text(state + "\n")
        if wireless:
            (path / "wireless").mkdir()
        self.addresses[name] = list(addresses)

    def default(self, name):
        with self.route.open("a") as output:
            output.write(f"{name} 00000000 00000000 0003 0 0 100 00000000\n")

    def collect(self):
        def lookup(name):
            self.lookups.append(name)
            value = self.addresses[name]
            if isinstance(value, OSError):
                raise value
            return value

        result = collect_linux_runtime(
            net_root=self.net, route_path=self.route, thermal_paths=(), ipv4_lookup=lookup
        )
        _validate_network(result["network"])
        return result

    def test_direct_wired_connection_is_not_hidden_by_first_down_port(self):
        self.interface("eth0", state="down")
        self.interface("eth1", addresses=["192.0.2.2/24"])
        result = self.collect()
        self.assertEqual(result["connection_method"], "ethernet_direct")
        self.assertEqual(result["network"]["wired"]["interface"], "eth1")
        self.assertEqual(result["network"]["default_route"], "none")
        with patch("rp_ylx.mdns.collect_linux_runtime", side_effect=self.collect):
            self.assertEqual(runtime_ipv4_addresses(), ("192.0.2.2",))

    def test_wifi_client_is_not_hidden_by_disconnected_or_connecting_adapter(self):
        for state in ("down", "up"):
            with self.subTest(first_state=state):
                if not (self.net / "wlan0").exists():
                    self.interface("wlan0")
                    self.interface("wlan1", addresses=["198.51.100.2/24"])
                (self.net / "wlan0" / "operstate").write_text(state)
                result = self.collect()
                self.assertEqual(result["connection_method"], "wifi_client")
                self.assertEqual(result["network"]["wifi_client"]["interface"], "wlan1")

    def test_rescue_hotspot_is_found_on_second_adapter(self):
        self.interface("wlan0", state="down")
        self.interface("wlan1", addresses=["10.42.0.1/24"])
        result = self.collect()
        self.assertEqual(result["connection_method"], "wifi_ap")
        self.assertEqual(result["network"]["ap"]["interface"], "wlan1")
        self.assertEqual(result["network"]["ap"]["state"], "active")

    def test_hotspot_and_default_wifi_client_are_reported_simultaneously(self):
        self.interface("wlan0", addresses=["10.42.0.1/24"])
        self.interface("wlan1", addresses=["198.51.100.2/24"])
        self.default("wlan1")
        result = self.collect()
        self.assertEqual(result["connection_method"], "wifi_client")
        self.assertEqual(result["network"]["default_route"], "wifi_client")
        self.assertEqual(result["network"]["ap"]["interface"], "wlan0")
        self.assertEqual(result["network"]["ap"]["state"], "active")
        self.assertEqual(result["network"]["wifi_client"]["interface"], "wlan1")
        with patch("rp_ylx.mdns.collect_linux_runtime", side_effect=self.collect):
            self.assertEqual(runtime_ipv4_addresses(), ("198.51.100.2",))

    def test_ethernet_default_preserves_both_wireless_roles(self):
        self.interface("wlan0", addresses=["10.42.0.1/24"])
        self.interface("wlan1", addresses=["198.51.100.2/24"])
        self.interface("eth0", addresses=["192.0.2.2/24"])
        self.default("eth0")
        result = self.collect()
        self.assertEqual(result["connection_method"], "ethernet_lan")
        self.assertEqual(result["network"]["ap"]["state"], "active")
        self.assertEqual(result["network"]["wifi_client"]["addresses"], ["198.51.100.2/24"])
        self.assertEqual(result["network"]["wired"]["addresses"], ["192.0.2.2/24"])

    def test_hotspot_and_client_without_gateway_keep_both_roles(self):
        self.interface("wlan0", addresses=["198.51.100.2/24"])
        self.interface("wlan1", addresses=["10.42.0.1/24"])
        result = self.collect()
        self.assertEqual(result["connection_method"], "wifi_ap")
        self.assertEqual(result["network"]["default_route"], "none")
        self.assertEqual(result["network"]["wifi_client"]["interface"], "wlan0")
        self.assertEqual(result["network"]["wifi_client"]["addresses"], ["198.51.100.2/24"])
        self.assertEqual(result["network"]["ap"]["interface"], "wlan1")
        with patch("rp_ylx.mdns.collect_linux_runtime", side_effect=self.collect):
            self.assertEqual(runtime_ipv4_addresses(), ("10.42.0.1",))

    def test_hotspot_route_is_not_projected_as_a_client_route(self):
        self.interface("wlan0", addresses=["10.42.0.1/24"])
        self.default("wlan0")
        result = self.collect()
        self.assertEqual(result["network"]["default_route"], "none")
        self.assertEqual(result["network"]["wifi_client"]["state"], "disconnected")
        self.assertEqual(result["network"]["wifi_client"]["addresses"], [])
        self.assertEqual(result["connection_method"], "wifi_ap")

    def test_disappearing_adapter_does_not_hide_other_adapter(self):
        self.interface("eth0")
        self.addresses["eth0"] = OSError("interface disappeared")
        self.interface("eth1", addresses=["192.0.2.2/24"])
        self.assertEqual(self.collect()["network"]["wired"]["interface"], "eth1")

    def test_real_default_route_owner_is_preserved_even_when_not_connected(self):
        self.interface("eth0", addresses=["192.0.2.2/24"])
        self.interface("eth1", state="down")
        self.default("eth1")
        result = self.collect()
        self.assertEqual(result["network"]["wired"]["interface"], "eth1")
        self.assertEqual(result["network"]["wired"]["state"], "disconnected")
        self.assertEqual(result["network"]["default_route"], "wired")
        self.assertEqual(result["connection_method"], "offline")

    def test_wireless_marker_prevents_duplicate_wired_role(self):
        self.interface("eth0", addresses=["198.51.100.2/24"], wireless=True)
        self.default("eth0")
        result = self.collect()
        self.assertEqual(result["network"]["wifi_client"]["interface"], "eth0")
        self.assertEqual(result["network"]["wired"]["state"], "unavailable")
        self.assertEqual(self.lookups, ["eth0"])

    def test_status_and_descriptor_preserve_available_secondary_interface(self):
        self.interface("eth0", state="down")
        self.interface("eth1", addresses=["192.0.2.2/24"])
        case = fixture.CaptureCoordinatorTest()
        case.setUp()
        self.addCleanup(case.tearDown)
        coordinator = case.coordinator(sources=fixture.FakeSources())
        self.addCleanup(coordinator.close)
        coordinator._runtime = self.collect
        status = coordinator.capture_status()
        validate_capture_status(status)
        self.assertEqual(status["snapshot"]["runtime"]["connection_method"], "ethernet_direct")
        descriptor = coordinator.device_descriptor("v4", "lab")
        validate_device_descriptor(descriptor, api_version="v4", security_profile="lab")
        self.assertEqual(descriptor["runtime"]["network"]["wired"]["interface"], "eth1")


if __name__ == "__main__":
    unittest.main()
