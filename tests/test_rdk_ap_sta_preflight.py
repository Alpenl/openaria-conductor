from __future__ import annotations

import unittest

from scripts import rdk_ap_sta_preflight as harness


class RdkApStaPreflightTest(unittest.TestCase):
    def test_parse_iw_list_detects_same_phy_ap_sta_advertisement(self) -> None:
        parsed = harness.parse_iw_list(
            """
Wiphy phy0
        Supported interface modes:
                 * managed
                 * AP
        valid interface combinations:
                 * #{ managed } <= 1, #{ AP } <= 1, total <= 2, #channels <= 1
"""
        )

        self.assertEqual(parsed["driver_advertises_same_phy_ap_sta"], "driver_advertised")
        self.assertEqual(parsed["max_managed_interfaces"], 1)
        self.assertEqual(parsed["max_ap_interfaces"], 1)
        self.assertEqual(parsed["max_total_interfaces"], 2)
        self.assertEqual(parsed["max_channels"], 1)

    def test_parse_iw_list_reports_not_advertised_when_combo_lacks_ap(self) -> None:
        parsed = harness.parse_iw_list(
            """
Wiphy phy0
        Supported interface modes:
                 * managed
                 * AP
        valid interface combinations:
                 * #{ managed } <= 1, total <= 1, #channels <= 1
"""
        )

        self.assertEqual(parsed["driver_advertises_same_phy_ap_sta"], "not_advertised")
        self.assertEqual(parsed["max_ap_interfaces"], 0)

    def test_parse_sse_data_extracts_json_data_lines(self) -> None:
        self.assertEqual(
            harness.parse_sse_data('id: 1\nevent: snapshot\ndata: {"schema":"x","ok":true}\n\n'),
            [{"schema": "x", "ok": True}],
        )

    def test_redaction_removes_secret_values_and_hashes_network_identifiers(self) -> None:
        redacted = harness.redact_value(
            {
                "psk": "plain-text-password",
                "peer_or_ssid": "PrivateLab",
                "authorization": "Bearer abc.def",
                "body": "bssid=00:11:22:33:44:55\nwifi.psk:super-secret",
            }
        )

        self.assertEqual(redacted["psk"], "[REDACTED]")
        self.assertNotEqual(redacted["peer_or_ssid"], "PrivateLab")
        self.assertTrue(redacted["peer_or_ssid"].startswith("sha256:"))
        self.assertEqual(redacted["authorization"], "Bearer [REDACTED]")
        self.assertNotIn("00:11:22:33:44:55", redacted["body"])
        self.assertNotIn("super-secret", redacted["body"])

    def test_command_allowlist_accepts_only_read_only_inventory_commands(self) -> None:
        self.assertTrue(harness.command_is_allowed(["nmcli", "--version"]))
        self.assertTrue(
            harness.command_is_allowed(
                ["iw", "dev", "wlan0", "get", "power_save"],
            )
        )
        self.assertTrue(
            harness.command_is_allowed(
                [
                    "systemctl",
                    "show",
                    "rp-ylx",
                    "--property=ActiveState",
                ]
            )
        )

        forbidden = (
            ["nmcli", "connection", "up", "rp-ylx-ap"],
            ["nmcli", "connection", "modify", "rp-ylx-ap", "wifi-sec.psk", "secret"],
            ["ip", "link", "set", "wlan0", "down"],
            ["iw", "dev", "wlan0", "interface", "add", "ap0", "type", "__ap"],
            ["systemctl", "restart", "rp-ylx"],
        )
        for args in forbidden:
            with self.subTest(args=args):
                self.assertFalse(harness.command_is_allowed(args))

    def test_summarize_readiness_keeps_preflight_non_closeable(self) -> None:
        summary = harness.summarize_readiness(
            expected_commit="abc",
            actual_commit="abc",
            http={"/api/v4/network": {"status": 200}},
            network_payload={"capabilities": {"same_phy_ap_sta": "unverified"}},
            iw_summary={"driver_advertises_same_phy_ap_sta": "driver_advertised"},
            services={"rp-ylx": {"parsed": {"ActiveState": "active"}}},
        )

        self.assertFalse(summary["closeable"])
        self.assertEqual(summary["blockers"], [])
        self.assertEqual(summary["driver_ap_sta"], "driver_advertised")


if __name__ == "__main__":
    unittest.main()
