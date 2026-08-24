from __future__ import annotations

import configparser
import ctypes
import ctypes.util
import fcntl
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
import uuid
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import rp_ylx.network as network_module
from rp_ylx.cli import main


class FakeNmcli:
    def __init__(self, profile_dir: Path | None = None) -> None:
        self.commands: list[list[str]] = []
        self.devices = "wlan0:wifi:connected\neth0:ethernet:disconnected\n"
        self.status_returncode = 0
        self.status_stderr = ""
        self.scan_output = ""
        self.scan_returncode = 0
        self.profile_dir = profile_dir
        self.active = {"wlan0": "", "eth0": ""}
        self.addresses = {"wlan0": "", "eth0": ""}
        self.routes = {"wlan0": "", "eth0": ""}
        self.fail_modes: dict[str, str] = {}
        self.timeout_modes: set[str] = set()
        self.missing_default_modes: set[str] = set()
        self.reload_failures = 0
        self.dhcp_address_override: str | None = None
        self.static_address_override: str | None = None
        self.timeouts: list[tuple[list[str], float]] = []

    def _load_profile(self, name: str) -> configparser.ConfigParser:
        if self.profile_dir is None:
            raise AssertionError("FakeNmcli 缺少 profile_dir")
        for path in self.profile_dir.glob("*.nmconnection"):
            profile = configparser.ConfigParser(interpolation=None)
            profile.read(path, encoding="utf-8")
            if profile["connection"]["id"] == name:
                return profile
        raise AssertionError(f"找不到 NetworkManager profile：{name}")

    def __call__(
        self,
        command: list[str],
        *,
        check: bool,
        capture_output: bool,
        text: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del check, capture_output, text
        self.commands.append(command)
        self.timeouts.append((command, timeout))
        if command[-4:] == ["--fields", "DEVICE,TYPE,STATE", "device", "status"]:
            return subprocess.CompletedProcess(
                command,
                self.status_returncode,
                self.devices,
                self.status_stderr,
            )
        if command[-2:] == ["connection", "reload"]:
            if self.reload_failures:
                self.reload_failures -= 1
                return subprocess.CompletedProcess(command, 1, "", "reload failed")
            return subprocess.CompletedProcess(command, 0, "", "")
        if "wifi" in command and "list" in command:
            return subprocess.CompletedProcess(
                command,
                self.scan_returncode,
                self.scan_output,
                "scan failed" if self.scan_returncode else "",
            )
        if "--get-values" in command and "connection" in command and "show" in command:
            field = command[command.index("--get-values") + 1]
            name = command[command.index("id") + 1]
            profile = self._load_profile(name)
            if field == "802-11-wireless-security.key-mgmt":
                value = profile.get("wifi-security", "key-mgmt", fallback="")
                return subprocess.CompletedProcess(command, 0, f"{value}\n", "")
            raise AssertionError(f"不支持的 NetworkManager profile 字段：{field}")
        if "connection" in command and "up" in command:
            name = command[command.index("id") + 1]
            interface = command[command.index("ifname") + 1]
            profile = self._load_profile(name)
            mode = next(
                (
                    candidate
                    for candidate in set(self.fail_modes) | self.timeout_modes
                    if f"-{candidate}-" in name
                ),
                None,
            )
            if mode in self.timeout_modes:
                raise subprocess.TimeoutExpired(command, timeout)
            if mode is not None:
                return subprocess.CompletedProcess(command, 4, "", self.fail_modes[mode])
            self.active[interface] = name
            method = profile["ipv4"]["method"]
            if method == "auto":
                self.addresses[interface] = self.dhcp_address_override or "192.168.50.20/24"
                applied_mode = next(
                    (
                        candidate
                        for candidate in [
                            "wifi-client",
                            "ethernet-dhcp",
                        ]
                        if f"-{candidate}-" in name
                    ),
                    None,
                )
                self.routes[interface] = (
                    ""
                    if applied_mode in self.missing_default_modes
                    else "dst = 0.0.0.0/0, nh = 192.168.50.1, mt = 600"
                )
            elif method == "manual":
                self.addresses[interface] = (
                    self.static_address_override or profile["ipv4"]["address1"].split(",", 1)[0]
                )
                self.routes[interface] = (
                    ""
                    if "ethernet-static" in self.missing_default_modes
                    else "dst = 0.0.0.0/0, nh = "
                    + profile["ipv4"]["address1"].split(",", 1)[1]
                    + ", mt = 600"
                    if "," in profile["ipv4"]["address1"]
                    else ""
                )
            else:
                self.addresses[interface] = "10.42.0.1/24"
                self.routes[interface] = "10.42.0.0/24"
            return subprocess.CompletedProcess(command, 0, "successfully activated", "")
        if command[-3:-1] == ["device", "show"]:
            interface = command[-1]
            profile = self.active[interface]
            address = self.addresses[interface]
            route = self.routes[interface]
            rendered = (
                f"GENERAL.STATE:{'100 (connected)' if profile else '30 (disconnected)'}\n"
                f"GENERAL.CONNECTION:{profile}\n"
                f"IP4.ADDRESS[1]:{address}\n"
                f"IP4.ROUTE[1]:{route}\n"
            )
            return subprocess.CompletedProcess(command, 0, rendered, "")
        raise AssertionError(f"未定义的 nmcli 调用：{command!r}")


def _glib_keyfile_string(path: Path, group: str, key: str) -> str:
    library = ctypes.util.find_library("glib-2.0")
    if library is None:
        raise unittest.SkipTest("系统未安装 GLib")
    glib = ctypes.CDLL(library)
    glib.g_key_file_new.restype = ctypes.c_void_p
    glib.g_key_file_load_from_file.argtypes = [
        ctypes.c_void_p,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    glib.g_key_file_load_from_file.restype = ctypes.c_int
    glib.g_key_file_get_string.argtypes = [
        ctypes.c_void_p,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    glib.g_key_file_get_string.restype = ctypes.c_void_p
    glib.g_key_file_free.argtypes = [ctypes.c_void_p]
    glib.g_error_free.argtypes = [ctypes.c_void_p]
    glib.g_free.argtypes = [ctypes.c_void_p]

    keyfile = glib.g_key_file_new()
    error = ctypes.c_void_p()
    try:
        loaded = glib.g_key_file_load_from_file(
            keyfile,
            os.fsencode(path),
            0,
            ctypes.byref(error),
        )
        if not loaded:
            raise AssertionError(f"GLib 无法解析 NetworkManager profile：{path}")
        value = glib.g_key_file_get_string(
            keyfile,
            group.encode(),
            key.encode(),
            ctypes.byref(error),
        )
        if not value:
            raise AssertionError(f"GLib 无法读取 {group}.{key}")
        try:
            return ctypes.string_at(value).decode("utf-8")
        finally:
            glib.g_free(value)
    finally:
        if error.value:
            glib.g_error_free(error)
        glib.g_key_file_free(keyfile)


class NetworkCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.environment = {
            "RP_YLX_NETWORK_STATE_DIR": str(self.root / "state"),
            "RP_YLX_NM_PROFILE_DIR": str(self.root / "profiles"),
            "RP_YLX_AVAHI_SERVICE_DIR": str(self.root / "avahi"),
            "RP_YLX_DEVICE_CONFIG_PATH": str(self.root / "device.json"),
            "RP_YLX_NETWORK_OPERATION_LOCK_PATH": str(self.root / "run" / "network-operation.lock"),
        }
        self._operation_environment = patch.dict(
            os.environ,
            {
                "RP_YLX_NETWORK_OPERATION_LOCK_PATH": self.environment[
                    "RP_YLX_NETWORK_OPERATION_LOCK_PATH"
                ]
            },
            clear=False,
        )
        self._operation_environment.start()

    def tearDown(self) -> None:
        self._operation_environment.stop()
        self.temporary.cleanup()

    def lkg_path(self, interface: str, *, root: Path | None = None) -> Path:
        return (root or self.root) / "state" / f"lkg-{interface}.json"

    def run_cli(
        self,
        arguments: list[str],
        *,
        stdin: str | io.TextIOBase = "",
        nmcli: FakeNmcli | None = None,
    ) -> tuple[int, dict[str, object], str]:
        output = io.StringIO()
        error = io.StringIO()
        runner = nmcli or FakeNmcli(self.root / "profiles")
        input_stream = io.StringIO(stdin) if isinstance(stdin, str) else stdin
        with (
            patch.dict(os.environ, self.environment, clear=False),
            patch("sys.stdin", input_stream),
            patch("rp_ylx.network.subprocess.run", side_effect=runner),
            redirect_stdout(output),
            redirect_stderr(error),
        ):
            code = main(arguments)
        rendered = output.getvalue() or error.getvalue()
        return code, json.loads(rendered), error.getvalue()

    def assert_connection_up_deadlines(self, nmcli: FakeNmcli) -> None:
        up_calls = [
            (command, timeout)
            for command, timeout in nmcli.timeouts
            if "connection" in command and "up" in command
        ]
        self.assertGreater(len(up_calls), 0)
        for command, timeout in up_calls:
            wait_index = command.index("--wait")
            self.assertEqual(
                command[wait_index : wait_index + 2],
                ["--wait", str(network_module.NETWORK_ACTIVATION_WAIT_SECONDS)],
            )
            self.assertEqual(timeout, network_module.NETWORK_ACTIVATION_TIMEOUT_SECONDS)

    def test_legacy_wpa2_lkg_without_security_is_verified_before_retry(self) -> None:
        profile = "rp-ylx-wifi-client-0123456789ab"
        profile_dir = self.root / "profiles"
        profile_dir.mkdir()
        profile_path = profile_dir / f"{profile}.nmconnection"
        profile_path.write_bytes(
            network_module._network_manager_profile(
                profile,
                {
                    "mode": "wifi-client",
                    "ssid": "Legacy LAN",
                    "security": "wpa2-personal",
                    "psk": "legacy-secret-123",
                },
            )
        )
        profile_path.chmod(0o600)
        state_dir = self.root / "state"
        state_dir.mkdir()
        self.lkg_path("wlan0").write_text(
            json.dumps(
                {
                    "format": "ylx.network-lkg.v0",
                    "mode": "wifi-client",
                    "interface": "wlan0",
                    "profile": profile,
                    "config": {"mode": "wifi-client", "ssid": "Legacy LAN"},
                }
            ),
            encoding="utf-8",
        )
        self.lkg_path("wlan0").chmod(0o600)
        nmcli = FakeNmcli(profile_dir)

        with (
            patch.dict(os.environ, self.environment, clear=False),
            patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
        ):
            candidate = network_module.saved_network_candidate("wifi-client")
            result = network_module.activate_network_candidate(candidate)

        self.assertTrue(result["connected"])
        self.assertEqual(result["connection"], profile)
        self.assertEqual(result["addresses"], ["192.168.50.20/24"])
        self.assertIn("0.0.0.0/0", result["routes"][0])
        self.assertEqual(
            candidate["config"],
            {
                "mode": "wifi-client",
                "ssid": "Legacy LAN",
                "security": "wpa2-personal",
            },
        )
        self.assertEqual(nmcli.active["wlan0"], profile)
        self.assertEqual(
            json.loads(self.lkg_path("wlan0").read_text(encoding="utf-8"))["config"],
            {"mode": "wifi-client", "ssid": "Legacy LAN"},
        )
        self.assertNotIn("legacy-secret-123", json.dumps(candidate))
        security_query = next(command for command in nmcli.commands if "--get-values" in command)
        activation = next(
            command for command in nmcli.commands if "connection" in command and "up" in command
        )
        self.assertLess(nmcli.commands.index(security_query), nmcli.commands.index(activation))

    def test_legacy_wifi_lkg_rejects_unverified_security_without_activation(self) -> None:
        profile = "rp-ylx-wifi-client-0123456789ab"
        profile_dir = self.root / "profiles"
        profile_dir.mkdir()
        profile_path = profile_dir / f"{profile}.nmconnection"
        profile_path.write_bytes(
            network_module._network_manager_profile(
                profile,
                {
                    "mode": "wifi-client",
                    "ssid": "Legacy LAN",
                    "security": "open",
                },
            )
        )
        profile_path.chmod(0o600)
        state_dir = self.root / "state"
        state_dir.mkdir()
        self.lkg_path("wlan0").write_text(
            json.dumps(
                {
                    "format": "ylx.network-lkg.v0",
                    "mode": "wifi-client",
                    "interface": "wlan0",
                    "profile": profile,
                    "config": {"mode": "wifi-client", "ssid": "Legacy LAN"},
                }
            ),
            encoding="utf-8",
        )
        self.lkg_path("wlan0").chmod(0o600)
        nmcli = FakeNmcli(profile_dir)

        with (
            patch.dict(os.environ, self.environment, clear=False),
            patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            self.assertRaises(network_module.NetworkError) as raised,
        ):
            network_module.saved_network_candidate("wifi-client")

        self.assertEqual(raised.exception.code, "state_invalid")
        self.assertTrue(any("--get-values" in command for command in nmcli.commands))
        self.assertFalse(
            any("connection" in command and "up" in command for command in nmcli.commands)
        )

    def test_status_reports_fixed_rdk_x5_capabilities_and_mdns(self) -> None:
        code, result, error = self.run_cli(["network", "status"])

        self.assertEqual(code, 0, error)
        self.assertEqual(result["format"], "ylx.network-status.v0")
        self.assertEqual(
            result["capabilities"],
            {
                "modes": [
                    "hotspot",
                    "wifi-client",
                    "ethernet-dhcp",
                    "ethernet-static",
                ],
                "wifi_interface": "wlan0",
                "ethernet_interface": "eth0",
                "second_wifi": False,
            },
        )
        self.assertEqual(
            result["mdns"],
            {
                "hostname": "rp-ylx.local",
                "service": "_ylx-capture._tcp",
                "aliases": ["_http._tcp"],
                "port": 8080,
            },
        )

    def test_status_only_reports_a_detected_second_wifi_as_a_capability(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        nmcli.devices = (
            "wlan0:wifi:connected\nwlan1:wifi:disconnected\neth0:ethernet:disconnected\n"
        )

        code, result, error = self.run_cli(["network", "status"], nmcli=nmcli)

        self.assertEqual(code, 0, error)
        self.assertTrue(result["capabilities"]["second_wifi"])
        self.assertEqual(result["capabilities"]["wifi_interface"], "wlan0")

    def test_status_v1_projects_runtime_state_and_disables_mutation(self) -> None:
        runtime = {
            "observed_at": "2026-08-23T12:58:00+08:00",
            "network": {
                "ap": {
                    "state": "disabled",
                    "interface": "wlan0",
                    "addresses": [],
                    "peer_or_ssid": None,
                },
                "wifi_client": {
                    "state": "connected",
                    "interface": "wlan0",
                    "addresses": ["192.168.110.36/24"],
                    "peer_or_ssid": "studio-wifi",
                },
                "wired": {
                    "state": "disconnected",
                    "interface": "eth0",
                    "addresses": [],
                    "peer_or_ssid": None,
                },
                "default_route": "wifi_client",
            },
        }
        state_dir = self.root / "state"
        state_dir.mkdir()
        self.lkg_path("wlan0").write_text(
            json.dumps(
                {
                    "format": "ylx.network-lkg.v0",
                    "mode": "wifi-client",
                    "interface": "wlan0",
                    "profile": "rp-ylx-wifi-client-0123456789ab",
                    "config": {
                        "mode": "wifi-client",
                        "ssid": "studio-wifi",
                        "psk": "must-not-leak",
                    },
                }
            ),
            encoding="utf-8",
        )

        with (
            patch.dict(os.environ, self.environment, clear=False),
            patch("rp_ylx.network.subprocess.run", side_effect=FakeNmcli(self.root / "profiles")),
        ):
            status = network_module.network_status_v1(runtime)

        self.assertEqual(status["schema"], "ylx.network-status.v1")
        self.assertEqual(uuid.UUID(status["authority_epoch"]).version, 4)
        self.assertEqual(status["source_revision"], 0)
        self.assertTrue(status["saved"])
        self.assertTrue(status["verified"])
        self.assertEqual(status["desired"]["mode"], "wifi-client")
        self.assertEqual(
            status["desired"]["wifi_client"],
            {
                "ssid": "studio-wifi",
                "security": "wpa2-personal",
                "credential_state": "stored",
            },
        )
        self.assertEqual(status["observed"]["default_route"], "wifi_client")
        self.assertEqual(status["observed"]["mdns"]["hostname"], "rp-ylx.local")
        self.assertFalse(status["mutation_capability"]["enabled"])
        self.assertEqual(status["mutation_capability"]["disabled_reason"], "controller_unavailable")
        self.assertEqual(status["mutation_capability"]["operations"], ["apply", "retry", "forget"])
        self.assertEqual(status["concurrency_capability"]["same_phy_ap_sta"], "unverified")
        self.assertEqual(
            status["concurrency_capability"]["exclusive_client_failure_timeout_seconds"],
            network_module.NETWORK_ACTIVATION_WAIT_SECONDS,
        )
        self.assertNotIn("must-not-leak", json.dumps(status, ensure_ascii=False))
        self.assertNotIn("psk", json.dumps(status, ensure_ascii=False).lower())

    def test_status_v1_projects_authoritative_controller_transaction(self) -> None:
        authority_epoch = "b9c5daed-6146-4a0e-8ab4-03aa65722720"
        desired = {
            "mode": "wifi-client",
            "wifi_client": {
                "ssid": "Field LAN",
                "security": "wpa2-personal",
                "credential_state": "pending_input",
            },
            "ethernet": None,
        }
        transaction = {
            "schema": "ylx.network-transaction.v1",
            "authority_epoch": authority_epoch,
            "source_revision": 8,
            "transaction_id": "0198d2a0-41a0-7b7a-a751-0e86a39d4db1",
            "operation": "apply",
            "status": "accepted",
            "stage": "accepted",
            "desired": desired,
            "accepted_at": "2026-08-23T12:57:00Z",
            "updated_at": "2026-08-23T12:57:00Z",
            "deadline": None,
            "recovery_action": "await_device",
            "rescue": {
                "ap_validated": False,
                "fallback_mode": "hotspot",
                "failure_trigger_seconds": 10,
            },
            "error": None,
        }
        capability = {
            "enabled": True,
            "disabled_reason": None,
            "operations": ["apply", "retry", "forget"],
            "idempotency_key_required": True,
            "secret_handling": "opaque_credential_reference_only",
            "active_state_policy": "idle_only",
        }
        controller_response = {
            "schema": "ylx.network-control-response.v1",
            "ok": True,
            "operation": "status",
            "status": 200,
            "body": {
                "schema": "ylx.network-controller-status.v1",
                "authority_epoch": authority_epoch,
                "source_revision": 8,
                "saved": True,
                "verified": False,
                "desired": desired,
                "transaction": {"current": transaction, "latest": None},
                "capability": capability,
            },
        }
        runtime = {
            "observed_at": "2026-08-23T12:58:00Z",
            "network": {
                "ap": {
                    "state": "connected",
                    "interface": "wlan0",
                    "addresses": ["10.42.0.1/24"],
                    "peer_or_ssid": "OpenAria",
                },
                "wifi_client": {
                    "state": "disconnected",
                    "interface": "wlan0",
                    "addresses": [],
                    "peer_or_ssid": None,
                },
                "wired": {
                    "state": "disconnected",
                    "interface": "eth0",
                    "addresses": [],
                    "peer_or_ssid": None,
                },
                "default_route": "none",
            },
        }
        legacy = {
            "capabilities": {"second_wifi": False},
            "mdns": {
                "hostname": "rp-ylx.local",
                "service": "_ylx-capture._tcp",
                "aliases": ["_http._tcp"],
                "port": 8080,
            },
            "devices": [],
        }

        with patch(
            "rp_ylx.network_control.request_control",
            return_value=controller_response,
        ) as request:
            status = network_module.network_status_v1(runtime, legacy_status=legacy)

        self.assertEqual(status["authority_epoch"], authority_epoch)
        self.assertEqual(status["source_revision"], 8)
        self.assertTrue(status["saved"])
        self.assertFalse(status["verified"])
        self.assertEqual(status["desired"], desired)
        self.assertEqual(status["transaction"]["current"], transaction)
        self.assertEqual(status["mutation_capability"], capability)
        request.assert_called_once_with("status", timeout_seconds=15.0)

    def test_status_reports_a_stable_error_when_network_manager_is_missing(self) -> None:
        with patch(
            "rp_ylx.network.subprocess.run",
            side_effect=FileNotFoundError("nmcli is not installed"),
        ):
            output = io.StringIO()
            error = io.StringIO()
            with redirect_stdout(output), redirect_stderr(error):
                code = main(["network", "status"])

        self.assertEqual(code, 3)
        self.assertEqual(output.getvalue(), "")
        self.assertEqual(
            json.loads(error.getvalue())["error"]["code"],
            "network_manager_unavailable",
        )

    def test_status_reports_nmcli_failure_as_machine_readable_error(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        nmcli.status_returncode = 10
        nmcli.status_stderr = "NetworkManager is not running"

        code, result, error = self.run_cli(["network", "status"], nmcli=nmcli)

        self.assertEqual(code, 3)
        self.assertEqual(result["error"]["code"], "network_status_failed")
        self.assertNotIn(nmcli.status_stderr, error)

    def test_controller_creates_one_secret_safe_rescue_ap_per_device(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        rescue_secret = "random-rescue-secret-1234"

        with (
            patch.dict(os.environ, self.environment, clear=False),
            patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
            patch("rp_ylx.network.secrets.token_urlsafe", return_value=rescue_secret) as random_psk,
        ):
            first = network_module.ensure_rescue_ap("device-serial-001")
            second = network_module.ensure_rescue_ap("device-serial-001")

        self.assertEqual(first, second)
        random_psk.assert_called_once()
        self.assertEqual(first["mode"], "hotspot")
        self.assertEqual(first["interface"], "wlan0")
        self.assertNotIn("psk", first)
        self.assertNotIn(rescue_secret, json.dumps(first))

        rescue_path = self.root / "state" / "rescue.json"
        rescue = json.loads(rescue_path.read_text(encoding="utf-8"))
        self.assertEqual(rescue_path.stat().st_mode & 0o777, 0o600)
        self.assertNotIn("psk", rescue["config"])
        self.assertNotIn(rescue_secret, json.dumps(rescue))

        profile_path = self.root / "profiles" / f"{rescue['profile']}.nmconnection"
        self.assertEqual(profile_path.stat().st_mode & 0o777, 0o600)
        profile = profile_path.read_text(encoding="utf-8")
        self.assertIn("mode=ap", profile)
        self.assertIn(f"psk={rescue_secret}", profile)
        self.assertIn("autoconnect=true", profile)

        persisted = b"\n".join(
            path.read_bytes() for path in (self.root / "state").rglob("*") if path.is_file()
        )
        self.assertNotIn(rescue_secret.encode(), persisted)

    def test_wifi_client_profile_never_autoconnects_outside_controller(self) -> None:
        code, _, error = self.run_cli(
            ["network", "apply", "--request-id", "client-no-autoconnect", "--config", "-"],
            stdin='{"mode":"wifi-client","ssid":"Field LAN","psk":"client-secret"}',
        )

        self.assertEqual(code, 0, error)
        profile_path = next((self.root / "profiles").glob("*wifi-client*.nmconnection"))
        profile = profile_path.read_text(encoding="utf-8")
        self.assertIn("autoconnect=false", profile)
        self.assertNotIn("autoconnect=true", profile)

    def test_scan_redacts_and_normalizes_closed_results(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        nmcli.scan_output = (
            "Field\\:LAN:WPA2:80\nField\\:LAN:WPA2:45\nGuest::60\n:WPA3:40\nCorp:WPA2 802.1X:95\n"
        )

        with (
            patch.dict(os.environ, self.environment, clear=False),
            patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
        ):
            networks = network_module.scan_wifi_networks()

        self.assertEqual(
            networks,
            [
                {
                    "ssid": "Field:LAN",
                    "hidden": False,
                    "security": "wpa2-personal",
                    "signal_dbm": -60,
                    "credential_required": True,
                },
                {
                    "ssid": "Guest",
                    "hidden": False,
                    "security": "open",
                    "signal_dbm": -70,
                    "credential_required": False,
                },
                {
                    "ssid": None,
                    "hidden": True,
                    "security": "wpa3-personal",
                    "signal_dbm": -80,
                    "credential_required": True,
                },
            ],
        )
        rendered = json.dumps(networks)
        self.assertNotIn("BSSID", rendered)
        self.assertNotIn("802.1X", rendered)

    def test_apply_rejects_invalid_or_overexposed_config_without_side_effects(self) -> None:
        cases = [
            '{"mode":"wifi-client","ssid":"Lab","psk":"secret123","extra":true}',
            '{"mode":"hotspot","ssid":"RP-YLX"}',
            '{"mode":"ethernet-static","address":"10.42.0.20/24"}',
            '{"mode":"unsupported"}',
        ]
        for index, config in enumerate(cases):
            with self.subTest(config=config):
                nmcli = FakeNmcli(self.root / "profiles")
                code, result, _ = self.run_cli(
                    ["network", "apply", "--request-id", f"invalid-{index}", "--config", "-"],
                    stdin=config,
                    nmcli=nmcli,
                )
                self.assertEqual(code, 2)
                self.assertEqual(result["error"]["code"], "config_invalid")
                self.assertEqual(nmcli.commands, [])

        path = self.root / "network.json"
        path.write_text('{"mode":"ethernet-dhcp"}', encoding="utf-8")
        path.chmod(0o644)
        nmcli = FakeNmcli(self.root / "profiles")
        code, result, _ = self.run_cli(
            ["network", "apply", "--request-id", "bad-permissions", "--config", str(path)],
            nmcli=nmcli,
        )
        self.assertEqual(code, 2)
        self.assertEqual(result["error"]["code"], "config_permissions")
        self.assertEqual(nmcli.commands, [])

        protected = self.root / "protected-network.json"
        protected.write_text('{"mode":"ethernet-dhcp"}', encoding="utf-8")
        protected.chmod(0o600)
        linked = self.root / "linked-network.json"
        linked.symlink_to(protected)
        code, result, _ = self.run_cli(
            ["network", "apply", "--request-id", "symlink-config", "--config", str(linked)],
            nmcli=nmcli,
        )
        self.assertEqual(code, 2)
        self.assertEqual(result["error"]["code"], "config_unreadable")
        self.assertEqual(nmcli.commands, [])

    def test_apply_rejects_invalid_utf8_stdin_without_traceback(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        stream = io.TextIOWrapper(io.BytesIO(b"\xff"), encoding="utf-8")
        try:
            code, result, error = self.run_cli(
                ["network", "apply", "--request-id", "invalid-utf8", "--config", "-"],
                stdin=stream,
                nmcli=nmcli,
            )
        finally:
            stream.close()

        self.assertEqual(code, 2)
        self.assertEqual(result["error"]["code"], "config_unreadable")
        self.assertNotIn("Traceback", error)
        self.assertEqual(nmcli.commands, [])

    def test_apply_maps_first_idempotency_key_write_failure(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        real_write_atomic = network_module._write_atomic

        def fail_key_write(path: Path, data: bytes, mode: int) -> None:
            if path.name == ".idempotency-key":
                raise OSError("simulated key write failure")
            real_write_atomic(path, data, mode)

        with patch("rp_ylx.network._write_atomic", side_effect=fail_key_write):
            code, result, error = self.run_cli(
                ["network", "apply", "--request-id", "key-write-failure", "--config", "-"],
                stdin='{"mode":"ethernet-dhcp"}',
                nmcli=nmcli,
            )

        self.assertEqual(code, 3)
        self.assertEqual(result["error"]["code"], "state_unwritable")
        self.assertNotIn("Traceback", error)
        self.assertEqual(nmcli.commands, [])

    def test_apply_rejects_fifo_config_without_blocking(self) -> None:
        fifo = self.root / "network.fifo"
        os.mkfifo(fifo, 0o600)
        project_root = Path(__file__).resolve().parents[1]
        environment = {**os.environ, **self.environment}
        source_path = str(project_root / "src")
        environment["PYTHONPATH"] = os.pathsep.join(
            filter(None, [source_path, environment.get("PYTHONPATH", "")])
        )

        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "rp_ylx",
                "network",
                "apply",
                "--request-id",
                "fifo-config",
                "--config",
                str(fifo),
            ],
            cwd=project_root,
            env=environment,
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )

        self.assertEqual(completed.returncode, 2, completed.stderr)
        self.assertEqual(
            json.loads(completed.stderr)["error"]["code"],
            "config_unreadable",
        )

    def test_apply_ethernet_dhcp_commits_durable_lkg_and_mdns(self) -> None:
        code, result, error = self.run_cli(
            ["network", "apply", "--request-id", "wired-dhcp-1", "--config", "-"],
            stdin='{ "mode": "ethernet-dhcp" }',
        )

        self.assertEqual(code, 0, error)
        self.assertEqual(
            result,
            {
                "format": "ylx.network-result.v0",
                "ok": True,
                "request_id": "wired-dhcp-1",
                "mode": "ethernet-dhcp",
                "replayed": False,
            },
        )
        journal_path = self.root / "state" / "journal.json"
        lkg_path = self.lkg_path("eth0")
        journal = json.loads(journal_path.read_text(encoding="utf-8"))
        lkg = json.loads(lkg_path.read_text(encoding="utf-8"))
        self.assertEqual(journal["phase"], "commit")
        self.assertEqual(journal["outcome"], "committed")
        self.assertEqual(lkg["mode"], "ethernet-dhcp")
        self.assertEqual(journal_path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(lkg_path.stat().st_mode & 0o777, 0o600)

        profiles = list((self.root / "profiles").glob("*.nmconnection"))
        self.assertEqual(len(profiles), 1)
        self.assertEqual(profiles[0].stat().st_mode & 0o777, 0o600)
        self.assertIn("method=auto", profiles[0].read_text(encoding="utf-8"))

        service = self.root / "avahi" / "rp-ylx.service"
        rendered_service = service.read_text(encoding="utf-8")
        self.assertIn("_ylx-capture._tcp", rendered_service)
        self.assertIn("_http._tcp", rendered_service)
        self.assertEqual(rendered_service.count("<port>8080</port>"), 2)

    def test_customer_network_apply_preserves_https_mdns(self) -> None:
        (self.root / "device.json").write_text(
            json.dumps(
                {
                    "security": {
                        "profile": "customer",
                        "tls_certificate_file": "/etc/rp-ylx/tls/device.crt",
                        "tls_private_key_file": "/etc/rp-ylx/tls/device.key",
                    }
                }
            ),
            encoding="utf-8",
        )

        code, _, error = self.run_cli(
            ["network", "apply", "--request-id", "customer-wired", "--config", "-"],
            stdin='{ "mode": "ethernet-dhcp" }',
        )

        self.assertEqual(code, 0, error)
        rendered_service = (self.root / "avahi" / "rp-ylx.service").read_text(encoding="utf-8")
        self.assertIn("_https._tcp", rendered_service)
        self.assertIn("<txt-record>scheme=https</txt-record>", rendered_service)
        self.assertNotIn("_http._tcp", rendered_service)

    def test_customer_network_apply_rejects_incomplete_tls_mdns_config(self) -> None:
        (self.root / "device.json").write_text(
            json.dumps({"security": {"profile": "customer"}}),
            encoding="utf-8",
        )

        code, result, error = self.run_cli(
            ["network", "apply", "--request-id", "customer-invalid", "--config", "-"],
            stdin='{ "mode": "ethernet-dhcp" }',
        )

        self.assertEqual(code, 3, error)
        self.assertEqual(result["error"]["code"], "mdns_config_invalid")
        self.assertFalse((self.root / "avahi" / "rp-ylx.service").exists())

    def test_apply_supports_four_modes_without_leaking_psk(self) -> None:
        cases = [
            (
                "hotspot",
                {"mode": "hotspot", "ssid": "RP-YLX", "psk": "hotspot-secret"},
                ["mode=ap", "address1=10.42.0.1/24", "autoconnect-priority=100"],
            ),
            (
                "wifi-client",
                {"mode": "wifi-client", "ssid": "Field LAN", "psk": "client-secret"},
                ["mode=infrastructure", "method=auto", "autoconnect-priority=800"],
            ),
            (
                "ethernet-dhcp",
                {"mode": "ethernet-dhcp"},
                ["method=auto", "autoconnect-priority=900"],
            ),
            (
                "ethernet-static",
                {
                    "mode": "ethernet-static",
                    "address": "192.168.88.20/24",
                    "gateway": "192.168.88.1",
                    "dns": ["1.1.1.1", "8.8.8.8"],
                },
                [
                    "method=manual",
                    "address1=192.168.88.20/24,192.168.88.1",
                    "dns=1.1.1.1;8.8.8.8;",
                    "autoconnect-priority=900",
                ],
            ),
        ]
        for mode, config, expected_profile_lines in cases:
            with self.subTest(mode=mode):
                case_root = self.root / mode
                environment = {
                    "RP_YLX_NETWORK_STATE_DIR": str(case_root / "state"),
                    "RP_YLX_NM_PROFILE_DIR": str(case_root / "profiles"),
                    "RP_YLX_AVAHI_SERVICE_DIR": str(case_root / "avahi"),
                }
                nmcli = FakeNmcli(case_root / "profiles")
                output = io.StringIO()
                error = io.StringIO()
                with (
                    patch.dict(os.environ, environment, clear=False),
                    patch("sys.stdin", io.StringIO(json.dumps(config))),
                    patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
                    redirect_stdout(output),
                    redirect_stderr(error),
                ):
                    code = main(
                        ["network", "apply", "--request-id", f"mode-{mode}", "--config", "-"]
                    )

                self.assertEqual(code, 0, error.getvalue())
                self.assert_connection_up_deadlines(nmcli)
                profile_path = next((case_root / "profiles").glob("*.nmconnection"))
                profile = profile_path.read_text(encoding="utf-8")
                for expected in expected_profile_lines:
                    self.assertIn(expected, profile)
                self.assertEqual(profile_path.stat().st_mode & 0o777, 0o600)

                secrets = [str(config.get("psk", ""))]
                observable = "\n".join(
                    [
                        output.getvalue(),
                        error.getvalue(),
                        *(json.dumps(command) for command in nmcli.commands),
                        (case_root / "state" / "journal.json").read_text(encoding="utf-8"),
                        self.lkg_path(
                            "wlan0" if mode in {"hotspot", "wifi-client"} else "eth0",
                            root=case_root,
                        ).read_text(encoding="utf-8"),
                        *(
                            path.read_text(encoding="utf-8")
                            for path in (case_root / "state" / "requests").glob("*.json")
                        ),
                    ]
                )
                for secret in filter(None, secrets):
                    self.assertNotIn(secret, observable)

    def test_wifi_profile_round_trips_escaped_values_through_glib(self) -> None:
        ssid = " leading\\ssid"
        psk = " leading\\password"

        code, _, error = self.run_cli(
            ["network", "apply", "--request-id", "escaped-wifi", "--config", "-"],
            stdin=json.dumps({"mode": "hotspot", "ssid": ssid, "psk": psk}),
        )

        self.assertEqual(code, 0, error)
        profile = next((self.root / "profiles").glob("*.nmconnection"))
        self.assertEqual(_glib_keyfile_string(profile, "wifi", "ssid"), ssid)
        self.assertEqual(_glib_keyfile_string(profile, "wifi-security", "psk"), psk)

    def test_successful_switches_remove_unreferenced_profiles_and_old_passwords(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")

        def apply(request_id: str, config: dict[str, object]) -> None:
            code, _, error = self.run_cli(
                ["network", "apply", "--request-id", request_id, "--config", "-"],
                stdin=json.dumps(config),
                nmcli=nmcli,
            )
            self.assertEqual(code, 0, error)

        apply("hotspot-a", {"mode": "hotspot", "ssid": "AP A", "psk": "old-secret-a"})
        hotspot_a = json.loads(self.lkg_path("wlan0").read_text())["profile"]
        apply("hotspot-b", {"mode": "hotspot", "ssid": "AP B", "psk": "rescue-secret-b"})
        hotspot_b = json.loads(self.lkg_path("wlan0").read_text())["profile"]
        apply(
            "wifi-client-c",
            {"mode": "wifi-client", "ssid": "Field LAN", "psk": "client-secret-c"},
        )
        wifi_client = json.loads(self.lkg_path("wlan0").read_text())["profile"]
        apply("wired-a", {"mode": "ethernet-dhcp"})
        wired_a = json.loads(self.lkg_path("eth0").read_text())["profile"]
        apply(
            "wired-b",
            {
                "mode": "ethernet-static",
                "address": "192.168.88.20/24",
                "gateway": "192.168.88.1",
            },
        )
        wired_b = json.loads(self.lkg_path("eth0").read_text())["profile"]

        profiles = {path.stem for path in (self.root / "profiles").glob("*.nmconnection")}
        self.assertEqual(profiles, {hotspot_b, wifi_client, wired_b})
        self.assertNotIn(hotspot_a, profiles)
        self.assertNotIn(wired_a, profiles)
        profile_bytes = b"\n".join(
            path.read_bytes() for path in (self.root / "profiles").glob("*.nmconnection")
        )
        self.assertNotIn(b"old-secret-a", profile_bytes)

    def test_reconcile_retries_profile_directory_sync_after_old_profile_unlink(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        first_code, _, first_error = self.run_cli(
            ["network", "apply", "--request-id", "old-hotspot", "--config", "-"],
            stdin='{"mode":"hotspot","ssid":"Old AP","psk":"old-secret"}',
            nmcli=nmcli,
        )
        self.assertEqual(first_code, 0, first_error)
        old_profile = json.loads(self.lkg_path("wlan0").read_text())["profile"]
        old_path = self.root / "profiles" / f"{old_profile}.nmconnection"
        real_fsync_directory = network_module._fsync_directory
        failed = False

        def fail_profile_sync_once(path: Path) -> None:
            nonlocal failed
            if path == self.root / "profiles" and not failed and not old_path.exists():
                failed = True
                raise OSError("simulated profile directory sync failure")
            real_fsync_directory(path)

        with patch(
            "rp_ylx.network._fsync_directory",
            side_effect=fail_profile_sync_once,
        ):
            second_code, second, second_error = self.run_cli(
                ["network", "apply", "--request-id", "new-hotspot", "--config", "-"],
                stdin='{"mode":"hotspot","ssid":"New AP","psk":"new-secret"}',
                nmcli=nmcli,
            )

        self.assertEqual(second_code, 3, second_error)
        self.assertEqual(second["error"]["code"], "cleanup_pending")
        self.assertEqual(second["recovery"], "reconcile")
        self.assertFalse(old_path.exists())
        journal_path = self.root / "state" / "journal.json"
        self.assertEqual(json.loads(journal_path.read_text())["cleanup"], "failed")
        nmcli.commands.clear()
        synced: list[Path] = []

        def track_directory_sync(path: Path) -> None:
            synced.append(path)
            real_fsync_directory(path)

        with patch("rp_ylx.network._fsync_directory", side_effect=track_directory_sync):
            reconcile_code, _, reconcile_error = self.run_cli(
                ["network", "reconcile"],
                nmcli=nmcli,
            )

        self.assertEqual(reconcile_code, 0, reconcile_error)
        self.assertIn(self.root / "profiles", synced)
        self.assertTrue(any(command[-2:] == ["connection", "reload"] for command in nmcli.commands))
        self.assertEqual(json.loads(journal_path.read_text())["cleanup"], "complete")

    def test_apply_is_idempotent_and_rejects_request_id_reuse(self) -> None:
        config = '{"mode":"ethernet-dhcp"}'
        first_nmcli = FakeNmcli(self.root / "profiles")
        first_code, first, first_error = self.run_cli(
            ["network", "apply", "--request-id", "same-request", "--config", "-"],
            stdin=config,
            nmcli=first_nmcli,
        )
        self.assertEqual(first_code, 0, first_error)
        state_before = {
            path.relative_to(self.root).as_posix(): path.read_bytes()
            for path in self.root.rglob("*")
            if path.is_file()
        }

        replay_nmcli = FakeNmcli(self.root / "profiles")
        replay_code, replay, replay_error = self.run_cli(
            ["network", "apply", "--request-id", "same-request", "--config", "-"],
            stdin=config,
            nmcli=replay_nmcli,
        )
        self.assertEqual(replay_code, 0, replay_error)
        self.assertEqual(replay, {**first, "replayed": True})
        self.assertEqual(replay_nmcli.commands, [])

        conflict_nmcli = FakeNmcli(self.root / "profiles")
        conflict_code, conflict, _ = self.run_cli(
            ["network", "apply", "--request-id", "same-request", "--config", "-"],
            stdin=(
                '{"mode":"ethernet-static","address":"192.168.90.20/24","gateway":"192.168.90.1"}'
            ),
            nmcli=conflict_nmcli,
        )
        self.assertEqual(conflict_code, 2)
        self.assertEqual(conflict["error"]["code"], "request_conflict")
        self.assertEqual(conflict_nmcli.commands, [])
        self.assertEqual(
            state_before,
            {
                path.relative_to(self.root).as_posix(): path.read_bytes()
                for path in self.root.rglob("*")
                if path.is_file()
            },
        )

    def test_receipt_replay_settles_a_newer_interrupted_transaction_first(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        config = '{"mode":"hotspot","ssid":"YLX-A1B2C3D4","psk":"primary-secret"}'
        first_code, first, first_error = self.run_cli(
            ["network", "apply", "--request-id", "primary-ap", "--config", "-"],
            stdin=config,
            nmcli=nmcli,
        )
        self.assertEqual(first_code, 0, first_error)
        lkg = json.loads(self.lkg_path("wlan0").read_text(encoding="utf-8"))
        candidate = "rp-ylx-wifi-client-deadbeef1234"
        candidate_path = self.root / "profiles" / f"{candidate}.nmconnection"
        candidate_path.write_text(
            f"""[connection]
id={candidate}
type=wifi
interface-name=wlan0

[wifi]
mode=infrastructure
ssid=Interrupted

[wifi-security]
key-mgmt=wpa-psk
psk=interrupted-secret

[ipv4]
method=auto

[ipv6]
method=disabled
""",
            encoding="utf-8",
        )
        candidate_path.chmod(0o600)
        journal_path = self.root / "state" / "journal.json"
        journal_path.write_text(
            json.dumps(
                {
                    "format": "ylx.network-journal.v0",
                    "phase": "verifying",
                    "request_id": "interrupted-client",
                    "request_fingerprint": "b" * 64,
                    "mode": "wifi-client",
                    "interface": "wlan0",
                    "profile": candidate,
                    "previous_profile": lkg["profile"],
                }
            ),
            encoding="utf-8",
        )
        journal_path.chmod(0o600)
        nmcli.active["wlan0"] = candidate
        nmcli.addresses["wlan0"] = "192.168.50.20/24"
        nmcli.routes["wlan0"] = "dst = 0.0.0.0/0, nh = 192.168.50.1, mt = 600"
        nmcli.commands.clear()

        replay_code, replay, replay_error = self.run_cli(
            ["network", "apply", "--request-id", "primary-ap", "--config", "-"],
            stdin=config,
            nmcli=nmcli,
        )

        self.assertEqual(replay_code, 0, replay_error)
        self.assertEqual(replay, {**first, "replayed": True})
        self.assertEqual(nmcli.active["wlan0"], lkg["profile"])
        self.assertFalse(candidate_path.exists())
        self.assertEqual(json.loads(journal_path.read_text())["outcome"], "rolled_back")

    def test_failed_request_id_cannot_be_reused_with_a_changed_body(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        primary_code, _, primary_error = self.run_cli(
            ["network", "apply", "--request-id", "primary-ap", "--config", "-"],
            stdin='{"mode":"hotspot","ssid":"YLX-A1B2C3D4","psk":"primary-secret"}',
            nmcli=nmcli,
        )
        self.assertEqual(primary_code, 0, primary_error)
        failed_config = '{"mode":"wifi-client","ssid":"Field LAN","psk":"wrong-secret"}'
        nmcli.fail_modes["wifi-client"] = "Secrets were required, but not provided"
        failed_code, _, failed_error = self.run_cli(
            ["network", "apply", "--request-id", "failed-reuse", "--config", "-"],
            stdin=failed_config,
            nmcli=nmcli,
        )
        self.assertEqual(failed_code, 3, failed_error)
        nmcli.fail_modes.clear()
        failure_path = (
            self.root / "state" / "requests" / f"{hashlib.sha256(b'failed-reuse').hexdigest()}.json"
        )
        failure_path.unlink()
        between_code, _, between_error = self.run_cli(
            ["network", "apply", "--request-id", "between-failures", "--config", "-"],
            stdin='{"mode":"ethernet-dhcp"}',
            nmcli=nmcli,
        )
        self.assertEqual(between_code, 0, between_error)
        nmcli.commands.clear()

        conflict_code, conflict, conflict_error = self.run_cli(
            ["network", "apply", "--request-id", "failed-reuse", "--config", "-"],
            stdin='{"mode":"ethernet-dhcp"}',
            nmcli=nmcli,
        )

        self.assertEqual(conflict_code, 2, conflict_error)
        self.assertEqual(conflict["error"]["code"], "request_conflict")
        self.assertEqual(nmcli.commands, [])

        retry_code, retry, retry_error = self.run_cli(
            ["network", "apply", "--request-id", "failed-reuse", "--config", "-"],
            stdin=failed_config,
            nmcli=nmcli,
        )
        self.assertEqual(retry_code, 0, retry_error)
        self.assertEqual(retry["mode"], "wifi-client")

    def test_noop_profile_cleanup_write_failure_is_machine_readable(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        journal_path = self.root / "state" / "journal.json"
        real_write_json = network_module._write_json
        failed = False

        def fail_cleanup_complete_once(path: Path, value: dict[str, object]) -> None:
            nonlocal failed
            if (
                not failed
                and path == journal_path
                and value.get("phase") == "commit"
                and value.get("cleanup") == "complete"
            ):
                failed = True
                raise OSError("simulated cleanup completion write failure")
            real_write_json(path, value)

        with patch("rp_ylx.network._write_json", side_effect=fail_cleanup_complete_once):
            code, result, error = self.run_cli(
                ["network", "apply", "--request-id", "noop-cleanup", "--config", "-"],
                stdin='{"mode":"ethernet-dhcp"}',
                nmcli=nmcli,
            )

        self.assertEqual(code, 3)
        self.assertEqual(result["error"]["code"], "cleanup_pending")
        self.assertEqual(result["recovery"], "reconcile")
        self.assertNotIn("Traceback", error)

    def test_explicit_apply_repairs_a_terminal_recovery_failure(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        nmcli.fail_modes["wifi-client"] = "Secrets were required, but not provided"
        failed_code, failed, failed_error = self.run_cli(
            ["network", "apply", "--request-id", "first-broken-client", "--config", "-"],
            stdin='{"mode":"wifi-client","ssid":"Field LAN","psk":"wrong-secret"}',
            nmcli=nmcli,
        )
        self.assertEqual(failed_code, 3, failed_error)
        self.assertEqual(failed["recovery"], "unavailable")
        journal = json.loads((self.root / "state" / "journal.json").read_text())
        self.assertEqual(journal["outcome"], "recovery_failed")
        self.assertEqual(journal["cleanup"], "complete")
        nmcli.fail_modes.clear()

        reconcile_code, reconcile, reconcile_error = self.run_cli(
            ["network", "reconcile"],
            nmcli=nmcli,
        )
        self.assertEqual(reconcile_code, 3, reconcile_error)
        self.assertEqual(reconcile["error"]["code"], "reconcile_failed")
        self.assertEqual(reconcile["recovery"], "unavailable")

        repair_code, repair, repair_error = self.run_cli(
            ["network", "apply", "--request-id", "repair-hotspot", "--config", "-"],
            stdin='{"mode":"hotspot","ssid":"RP-YLX","psk":"repair-secret"}',
            nmcli=nmcli,
        )

        self.assertEqual(repair_code, 0, repair_error)
        self.assertEqual(repair["mode"], "hotspot")

    def test_explicit_apply_replaces_an_unrecoverable_committed_lkg(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        first_code, _, first_error = self.run_cli(
            ["network", "apply", "--request-id", "old-hotspot", "--config", "-"],
            stdin='{"mode":"hotspot","ssid":"Old AP","psk":"old-secret"}',
            nmcli=nmcli,
        )
        self.assertEqual(first_code, 0, first_error)
        old_profile = json.loads(self.lkg_path("wlan0").read_text())["profile"]
        (self.root / "profiles" / f"{old_profile}.nmconnection").unlink()
        nmcli.active["wlan0"] = ""
        nmcli.addresses["wlan0"] = ""
        nmcli.routes["wlan0"] = ""

        repair_code, repair, repair_error = self.run_cli(
            ["network", "apply", "--request-id", "new-hotspot", "--config", "-"],
            stdin='{"mode":"hotspot","ssid":"New AP","psk":"new-secret"}',
            nmcli=nmcli,
        )

        self.assertEqual(repair_code, 0, repair_error)
        self.assertEqual(repair["mode"], "hotspot")
        new_profile = json.loads(self.lkg_path("wlan0").read_text())["profile"]
        self.assertNotEqual(new_profile, old_profile)

    def test_apply_recovers_a_committed_request_when_its_receipt_was_not_written(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        config = '{"mode":"ethernet-dhcp"}'
        first_code, first, first_error = self.run_cli(
            ["network", "apply", "--request-id", "commit-before-receipt", "--config", "-"],
            stdin=config,
            nmcli=nmcli,
        )
        self.assertEqual(first_code, 0, first_error)
        receipt = next((self.root / "state" / "requests").glob("*.json"))
        receipt.unlink()
        nmcli.commands.clear()

        replay_code, replay, replay_error = self.run_cli(
            ["network", "apply", "--request-id", "commit-before-receipt", "--config", "-"],
            stdin=config,
            nmcli=nmcli,
        )

        self.assertEqual(replay_code, 0, replay_error)
        self.assertEqual(replay, {**first, "replayed": True})
        self.assertEqual(nmcli.commands, [])
        self.assertTrue(receipt.exists())

    def test_commit_journal_write_failure_rolls_back_the_active_candidate(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        first_code, _, first_error = self.run_cli(
            ["network", "apply", "--request-id", "primary-ap", "--config", "-"],
            stdin='{"mode":"hotspot","ssid":"YLX-A1B2C3D4","psk":"primary-secret"}',
            nmcli=nmcli,
        )
        self.assertEqual(first_code, 0, first_error)
        lkg_path = self.lkg_path("wlan0")
        lkg_before = lkg_path.read_bytes()
        primary_profile = json.loads(lkg_before)["profile"]
        request_id = "commit-write-failure"
        candidate = f"rp-ylx-wifi-client-{hashlib.sha256(request_id.encode()).hexdigest()[:12]}"
        real_write_json = network_module._write_json
        failed = False

        def fail_commit_once(path: Path, value: dict[str, object]) -> None:
            nonlocal failed
            if not failed and path.name == "journal.json" and value.get("phase") == "commit":
                failed = True
                raise OSError("simulated commit journal failure")
            real_write_json(path, value)

        with patch("rp_ylx.network._write_json", side_effect=fail_commit_once):
            code, result, error = self.run_cli(
                ["network", "apply", "--request-id", request_id, "--config", "-"],
                stdin='{"mode":"wifi-client","ssid":"Field LAN","psk":"candidate-secret"}',
                nmcli=nmcli,
            )

        self.assertEqual(code, 3, error)
        self.assertEqual(result["error"]["code"], "state_write_failed")
        self.assertEqual(result["recovery"], "lkg")
        self.assertEqual(nmcli.active["wlan0"], primary_profile)
        self.assert_connection_up_deadlines(nmcli)
        self.assertEqual(lkg_path.read_bytes(), lkg_before)
        self.assertEqual(list((self.root / "profiles").glob(f"*{candidate}*")), [])
        journal = json.loads((self.root / "state" / "journal.json").read_text(encoding="utf-8"))
        self.assertEqual(journal["phase"], "verifying")
        self.assertEqual(journal["outcome"], "rolled_back")
        self.assertNotIn("candidate-secret", error)

    def test_first_transaction_failure_restores_the_preexisting_connection(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        profile_dir = self.root / "profiles"
        profile_dir.mkdir(parents=True)
        previous_profile = profile_dir / "site-managed.nmconnection"
        previous_profile.write_text(
            """[connection]
id=site-managed-uplink
type=wifi
interface-name=wlan0

[wifi]
mode=infrastructure
ssid=Site-LAN

[ipv4]
method=auto

[ipv6]
method=disabled
""",
            encoding="utf-8",
        )
        previous_profile.chmod(0o600)
        nmcli.active["wlan0"] = "site-managed-uplink"
        nmcli.addresses["wlan0"] = "192.168.60.20/24"
        nmcli.routes["wlan0"] = "dst = 0.0.0.0/0, nh = 192.168.60.1, mt = 600"
        real_write_json = network_module._write_json
        failed = False

        def fail_commit_once(path: Path, value: dict[str, object]) -> None:
            nonlocal failed
            if not failed and path.name == "journal.json" and value.get("phase") == "commit":
                failed = True
                raise OSError("simulated first transaction commit failure")
            real_write_json(path, value)

        with patch("rp_ylx.network._write_json", side_effect=fail_commit_once):
            code, result, error = self.run_cli(
                ["network", "apply", "--request-id", "first-transaction", "--config", "-"],
                stdin='{"mode":"wifi-client","ssid":"Field LAN","psk":"candidate-secret"}',
                nmcli=nmcli,
            )

        self.assertEqual(code, 3, error)
        self.assertEqual(result["error"]["code"], "state_write_failed")
        self.assertEqual(result["recovery"], "previous")
        self.assertEqual(nmcli.active["wlan0"], "site-managed-uplink")
        self.assertTrue(nmcli.addresses["wlan0"])
        self.assertIn("dst = 0.0.0.0/0", nmcli.routes["wlan0"])
        self.assertEqual(list(profile_dir.glob("*wifi-client*.nmconnection")), [])
        journal = json.loads((self.root / "state" / "journal.json").read_text(encoding="utf-8"))
        self.assertEqual(journal["outcome"], "rolled_back")
        self.assertEqual(journal["recovery"], "previous")
        self.assertEqual(journal["previous_snapshot"]["connection"], "site-managed-uplink")
        self.assertNotIn("candidate-secret", json.dumps(journal))
        self.assertNotIn("candidate-secret", error)

    def test_reconcile_retries_candidate_cleanup_after_reload_failure(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        first_code, _, first_error = self.run_cli(
            ["network", "apply", "--request-id", "primary-ap", "--config", "-"],
            stdin='{"mode":"hotspot","ssid":"YLX-A1B2C3D4","psk":"primary-secret"}',
            nmcli=nmcli,
        )
        self.assertEqual(first_code, 0, first_error)
        real_write_json = network_module._write_json
        failed = False

        def fail_commit_and_next_reload(path: Path, value: dict[str, object]) -> None:
            nonlocal failed
            if not failed and path.name == "journal.json" and value.get("phase") == "commit":
                failed = True
                nmcli.reload_failures = 1
                raise OSError("simulated commit failure before cleanup reload")
            real_write_json(path, value)

        with patch("rp_ylx.network._write_json", side_effect=fail_commit_and_next_reload):
            code, result, error = self.run_cli(
                ["network", "apply", "--request-id", "cleanup-retry", "--config", "-"],
                stdin='{"mode":"wifi-client","ssid":"Field LAN","psk":"candidate-secret"}',
                nmcli=nmcli,
            )

        self.assertEqual(code, 3, error)
        self.assertEqual(result["recovery"], "lkg")
        journal_path = self.root / "state" / "journal.json"
        self.assertEqual(json.loads(journal_path.read_text())["cleanup"], "failed")

        nmcli.reload_failures = 1
        pending_code, pending, pending_error = self.run_cli(["network", "reconcile"], nmcli=nmcli)
        self.assertEqual(pending_code, 3, pending_error)
        self.assertEqual(pending["error"]["code"], "cleanup_pending")
        self.assertEqual(pending["recovery"], "reconcile")
        self.assertEqual(json.loads(journal_path.read_text())["cleanup"], "failed")

        nmcli.commands.clear()
        reconcile_code, reconciled, reconcile_error = self.run_cli(
            ["network", "reconcile"], nmcli=nmcli
        )
        self.assertEqual(reconcile_code, 0, reconcile_error)
        self.assertEqual(reconciled["recovery"], "unchanged")
        self.assertEqual(json.loads(journal_path.read_text())["cleanup"], "complete")
        self.assertTrue(any(command[-2:] == ["connection", "reload"] for command in nmcli.commands))

    def test_commit_materialization_failure_waits_for_reconcile_without_rollback(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        first_code, _, first_error = self.run_cli(
            ["network", "apply", "--request-id", "primary-ap", "--config", "-"],
            stdin='{"mode":"hotspot","ssid":"YLX-A1B2C3D4","psk":"primary-secret"}',
            nmcli=nmcli,
        )
        self.assertEqual(first_code, 0, first_error)
        lkg_path = self.lkg_path("wlan0")
        lkg_before = lkg_path.read_bytes()
        request_id = "materialization-failure"
        candidate = f"rp-ylx-wifi-client-{hashlib.sha256(request_id.encode()).hexdigest()[:12]}"
        receipt_path = (
            self.root
            / "state"
            / "requests"
            / f"{hashlib.sha256(request_id.encode()).hexdigest()}.json"
        )
        real_write_json = network_module._write_json
        failed = False

        def fail_new_lkg_once(path: Path, value: dict[str, object]) -> None:
            nonlocal failed
            if not failed and path == lkg_path and value.get("mode") == "wifi-client":
                failed = True
                raise OSError("simulated LKG write failure")
            real_write_json(path, value)

        with patch("rp_ylx.network._write_json", side_effect=fail_new_lkg_once):
            code, result, error = self.run_cli(
                ["network", "apply", "--request-id", request_id, "--config", "-"],
                stdin='{"mode":"wifi-client","ssid":"Field LAN","psk":"candidate-secret"}',
                nmcli=nmcli,
            )

        self.assertEqual(code, 3, error)
        self.assertEqual(result["error"]["code"], "commit_pending")
        self.assertEqual(result["recovery"], "reconcile")
        self.assertEqual(nmcli.active["wlan0"], candidate)
        self.assertTrue((self.root / "profiles" / f"{candidate}.nmconnection").exists())
        self.assertEqual(lkg_path.read_bytes(), lkg_before)
        self.assertFalse(receipt_path.exists())
        journal = json.loads((self.root / "state" / "journal.json").read_text(encoding="utf-8"))
        self.assertEqual(journal["phase"], "commit")
        self.assertEqual(journal["outcome"], "committed")
        self.assertNotIn("candidate-secret", json.dumps(journal))
        self.assertNotIn("candidate-secret", error)

        nmcli.commands.clear()
        reconcile_code, reconcile_result, reconcile_error = self.run_cli(
            ["network", "reconcile"], nmcli=nmcli
        )
        self.assertEqual(reconcile_code, 0, reconcile_error)
        self.assertEqual(reconcile_result["recovery"], "unchanged")
        self.assertEqual(json.loads(lkg_path.read_text(encoding="utf-8"))["mode"], "wifi-client")
        self.assertTrue(receipt_path.exists())
        self.assertEqual(len(nmcli.commands), 1)

    def test_commit_journal_directory_sync_failure_preserves_the_published_commit(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        state_dir = self.root / "state"
        request_id = "commit-sync-failure"
        candidate = f"rp-ylx-ethernet-dhcp-{hashlib.sha256(request_id.encode()).hexdigest()[:12]}"
        journal_path = state_dir / "journal.json"
        real_replace = os.replace
        real_fsync_directory = network_module._fsync_directory
        commit_published = False
        failed = False

        def track_commit_replace(source: str | Path, destination: str | Path) -> None:
            nonlocal commit_published
            real_replace(source, destination)
            if Path(destination) == journal_path:
                value = json.loads(journal_path.read_text(encoding="utf-8"))
                if value.get("phase") == "commit":
                    commit_published = True

        def fail_first_commit_directory_sync(path: Path) -> None:
            nonlocal failed
            if commit_published and not failed and path == state_dir:
                failed = True
                raise OSError("simulated post-rename directory sync failure")
            real_fsync_directory(path)

        with (
            patch("rp_ylx.network.os.replace", side_effect=track_commit_replace),
            patch(
                "rp_ylx.network._fsync_directory",
                side_effect=fail_first_commit_directory_sync,
            ),
        ):
            code, result, error = self.run_cli(
                ["network", "apply", "--request-id", request_id, "--config", "-"],
                stdin='{"mode":"ethernet-dhcp"}',
                nmcli=nmcli,
            )

        self.assertEqual(code, 0, error)
        self.assertEqual(result["request_id"], request_id)
        self.assertEqual(nmcli.active["eth0"], candidate)
        journal = json.loads(journal_path.read_text(encoding="utf-8"))
        self.assertEqual(journal["phase"], "commit")
        self.assertEqual(journal["outcome"], "committed")
        self.assertEqual(json.loads(self.lkg_path("eth0").read_text())["profile"], candidate)
        receipt = next((state_dir / "requests").glob("*.json"))
        self.assertEqual(json.loads(receipt.read_text())["result"], result)

    def test_apply_rejects_changed_body_after_commit_before_receipt(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        first_code, _, first_error = self.run_cli(
            ["network", "apply", "--request-id", "committed-request", "--config", "-"],
            stdin='{"mode":"ethernet-dhcp"}',
            nmcli=nmcli,
        )
        self.assertEqual(first_code, 0, first_error)
        next((self.root / "state" / "requests").glob("*.json")).unlink()
        nmcli.commands.clear()

        code, result, error = self.run_cli(
            ["network", "apply", "--request-id", "committed-request", "--config", "-"],
            stdin=(
                '{"mode":"ethernet-static","address":"192.168.88.20/24","gateway":"192.168.88.1"}'
            ),
            nmcli=nmcli,
        )

        self.assertEqual(code, 2, error)
        self.assertEqual(result["error"]["code"], "request_conflict")
        self.assertEqual(nmcli.commands, [])

    def test_wrong_wifi_password_rolls_back_to_lkg_without_leaking_secret(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        first_code, _, first_error = self.run_cli(
            ["network", "apply", "--request-id", "primary-ap", "--config", "-"],
            stdin=('{"mode":"hotspot","ssid":"YLX-A1B2C3D4","psk":"primary-secret"}'),
            nmcli=nmcli,
        )
        self.assertEqual(first_code, 0, first_error)
        lkg_path = self.lkg_path("wlan0")
        lkg_before = lkg_path.read_bytes()
        primary_profile = json.loads(lkg_before)["profile"]

        nmcli.fail_modes["wifi-client"] = "Secrets were required, but not provided"
        code, result, error = self.run_cli(
            ["network", "apply", "--request-id", "bad-client", "--config", "-"],
            stdin=('{"mode":"wifi-client","ssid":"Field LAN","psk":"wrong-client-secret"}'),
            nmcli=nmcli,
        )

        self.assertEqual(code, 3)
        self.assertEqual(result["error"]["code"], "wifi_auth_failed")
        self.assertEqual(result["recovery"], "lkg")
        self.assertEqual(nmcli.active["wlan0"], primary_profile)
        self.assertEqual(lkg_path.read_bytes(), lkg_before)
        journal = json.loads((self.root / "state" / "journal.json").read_text(encoding="utf-8"))
        self.assertEqual(journal["outcome"], "rolled_back")
        profiles = list((self.root / "profiles").glob("*.nmconnection"))
        self.assertEqual(len(profiles), 1)
        self.assertNotIn("wrong-client-secret", error)

    def test_client_dhcp_and_default_route_failures_restore_primary_hotspot(self) -> None:
        for failure, expected_code in [
            ("dhcp", "dhcp_timeout"),
            ("route", "default_route_missing"),
        ]:
            with self.subTest(failure=failure):
                case_root = self.root / failure
                environment = {
                    "RP_YLX_NETWORK_STATE_DIR": str(case_root / "state"),
                    "RP_YLX_NM_PROFILE_DIR": str(case_root / "profiles"),
                    "RP_YLX_AVAHI_SERVICE_DIR": str(case_root / "avahi"),
                }
                nmcli = FakeNmcli(case_root / "profiles")

                def invoke(
                    request_id: str,
                    config: str,
                    environment: dict[str, str] = environment,
                    nmcli: FakeNmcli = nmcli,
                ) -> tuple[int, dict[str, object], str]:
                    output = io.StringIO()
                    error = io.StringIO()
                    with (
                        patch.dict(os.environ, environment, clear=False),
                        patch("sys.stdin", io.StringIO(config)),
                        patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
                        redirect_stdout(output),
                        redirect_stderr(error),
                    ):
                        code = main(
                            ["network", "apply", "--request-id", request_id, "--config", "-"]
                        )
                    rendered = output.getvalue() or error.getvalue()
                    return code, json.loads(rendered), error.getvalue()

                first_code, _, first_error = invoke(
                    "primary-ap",
                    '{"mode":"hotspot","ssid":"YLX-A1B2C3D4","psk":"primary-secret"}',
                )
                self.assertEqual(first_code, 0, first_error)
                primary_profile = json.loads(
                    self.lkg_path("wlan0", root=case_root).read_text(encoding="utf-8")
                )["profile"]
                if failure == "dhcp":
                    nmcli.fail_modes["wifi-client"] = "DHCP request timed out"
                else:
                    nmcli.missing_default_modes.add("wifi-client")

                code, result, error = invoke(
                    "broken-client",
                    '{"mode":"wifi-client","ssid":"Field LAN","psk":"candidate-secret"}',
                )

                self.assertEqual(code, 3)
                self.assertEqual(result["error"]["code"], expected_code)
                self.assertEqual(result["recovery"], "lkg")
                self.assertEqual(nmcli.active["wlan0"], primary_profile)
                self.assertEqual(len(list((case_root / "profiles").glob("*.nmconnection"))), 1)
                self.assertNotIn("candidate-secret", error)

    def test_reload_failure_restores_the_last_known_good_connection(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        first_code, _, first_error = self.run_cli(
            ["network", "apply", "--request-id", "primary-ap", "--config", "-"],
            stdin='{"mode":"hotspot","ssid":"YLX-A1B2C3D4","psk":"primary-secret"}',
            nmcli=nmcli,
        )
        self.assertEqual(first_code, 0, first_error)
        primary_profile = json.loads(self.lkg_path("wlan0").read_text(encoding="utf-8"))["profile"]
        nmcli.reload_failures = 1

        code, result, error = self.run_cli(
            ["network", "apply", "--request-id", "reload-fails", "--config", "-"],
            stdin='{"mode":"wifi-client","ssid":"Field LAN","psk":"client-secret"}',
            nmcli=nmcli,
        )

        self.assertEqual(code, 3, error)
        self.assertEqual(result["error"]["code"], "reload_failed")
        self.assertEqual(result["recovery"], "lkg")
        self.assertEqual(nmcli.active["wlan0"], primary_profile)
        self.assertEqual(len(list((self.root / "profiles").glob("*.nmconnection"))), 1)

    def test_static_address_mismatch_restores_the_last_known_good_connection(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        first_code, _, first_error = self.run_cli(
            ["network", "apply", "--request-id", "wired-dhcp", "--config", "-"],
            stdin='{"mode":"ethernet-dhcp"}',
            nmcli=nmcli,
        )
        self.assertEqual(first_code, 0, first_error)
        primary_profile = json.loads(self.lkg_path("eth0").read_text(encoding="utf-8"))["profile"]
        nmcli.static_address_override = "192.168.88.99/24"

        code, result, error = self.run_cli(
            ["network", "apply", "--request-id", "wrong-static", "--config", "-"],
            stdin=(
                '{"mode":"ethernet-static","address":"192.168.88.20/24","gateway":"192.168.88.1"}'
            ),
            nmcli=nmcli,
        )

        self.assertEqual(code, 3, error)
        self.assertEqual(result["error"]["code"], "static_address_mismatch")
        self.assertEqual(result["recovery"], "lkg")
        self.assertEqual(nmcli.active["eth0"], primary_profile)
        self.assertEqual(len(list((self.root / "profiles").glob("*.nmconnection"))), 1)

    def test_static_gateway_requires_a_default_route_before_commit(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        first_code, _, first_error = self.run_cli(
            ["network", "apply", "--request-id", "wired-dhcp", "--config", "-"],
            stdin='{"mode":"ethernet-dhcp"}',
            nmcli=nmcli,
        )
        self.assertEqual(first_code, 0, first_error)
        primary_profile = json.loads(self.lkg_path("eth0").read_text(encoding="utf-8"))["profile"]
        nmcli.missing_default_modes.add("ethernet-static")

        code, result, error = self.run_cli(
            ["network", "apply", "--request-id", "static-no-route", "--config", "-"],
            stdin=(
                '{"mode":"ethernet-static","address":"192.168.88.20/24","gateway":"192.168.88.1"}'
            ),
            nmcli=nmcli,
        )

        self.assertEqual(code, 3, error)
        self.assertEqual(result["error"]["code"], "default_route_missing")
        self.assertEqual(result["recovery"], "lkg")
        self.assertEqual(nmcli.active["eth0"], primary_profile)

    def test_wifi_and_ethernet_keep_independent_last_known_good_profiles(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        for request_id, config in [
            (
                "primary-ap",
                '{"mode":"hotspot","ssid":"YLX-A1B2C3D4","psk":"primary-secret"}',
            ),
            ("wired-dhcp", '{"mode":"ethernet-dhcp"}'),
        ]:
            code, _, error = self.run_cli(
                ["network", "apply", "--request-id", request_id, "--config", "-"],
                stdin=config,
                nmcli=nmcli,
            )
            self.assertEqual(code, 0, error)

        wifi = json.loads(self.lkg_path("wlan0").read_text(encoding="utf-8"))
        ethernet = json.loads(self.lkg_path("eth0").read_text(encoding="utf-8"))
        self.assertEqual(wifi["mode"], "hotspot")
        self.assertEqual(ethernet["mode"], "ethernet-dhcp")
        nmcli.fail_modes["wifi-client"] = "Secrets were required, but not provided"

        code, result, error = self.run_cli(
            ["network", "apply", "--request-id", "bad-client", "--config", "-"],
            stdin='{"mode":"wifi-client","ssid":"Field LAN","psk":"wrong-secret"}',
            nmcli=nmcli,
        )

        self.assertEqual(code, 3, error)
        self.assertEqual(result["recovery"], "lkg")
        self.assertEqual(nmcli.active["wlan0"], wifi["profile"])
        self.assertEqual(nmcli.active["eth0"], ethernet["profile"])

    def test_file_write_failure_removes_candidate_and_temporary_secrets(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        first_code, _, first_error = self.run_cli(
            ["network", "apply", "--request-id", "primary-ap", "--config", "-"],
            stdin='{"mode":"hotspot","ssid":"YLX-A1B2C3D4","psk":"primary-secret"}',
            nmcli=nmcli,
        )
        self.assertEqual(first_code, 0, first_error)
        request_id = "write-failure"
        profile = f"rp-ylx-wifi-client-{hashlib.sha256(request_id.encode()).hexdigest()[:12]}"
        profile_dir = self.root / "profiles"
        orphan = profile_dir / f".{profile}.nmconnection.interrupted"
        orphan.write_text("psk=candidate-secret\n", encoding="utf-8")
        orphan.chmod(0o600)
        real_replace = os.replace

        def fail_avahi_replace(source: str | Path, destination: str | Path) -> None:
            if Path(destination).name == "rp-ylx.service":
                raise OSError("simulated avahi write failure")
            real_replace(source, destination)

        with patch("rp_ylx.network.os.replace", side_effect=fail_avahi_replace):
            code, result, error = self.run_cli(
                ["network", "apply", "--request-id", request_id, "--config", "-"],
                stdin=('{"mode":"wifi-client","ssid":"Field LAN","psk":"candidate-secret"}'),
                nmcli=nmcli,
            )

        self.assertEqual(code, 3, error)
        self.assertEqual(result["error"]["code"], "state_write_failed")
        self.assertEqual(result["recovery"], "lkg")
        self.assertFalse(orphan.exists())
        candidate_files = list(profile_dir.glob(f"*{profile}*"))
        self.assertEqual(candidate_files, [])
        for path in self.root.rglob("*"):
            if path.is_file():
                self.assertNotIn(b"candidate-secret", path.read_bytes(), path)

    def test_rescue_activates_saved_hotspot_without_replacing_lkg(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        for request_id, config in [
            (
                "primary-ap",
                '{"mode":"hotspot","ssid":"YLX-A1B2C3D4","psk":"primary-secret"}',
            ),
            (
                "field-client",
                '{"mode":"wifi-client","ssid":"Field LAN","psk":"client-secret"}',
            ),
        ]:
            code, _, error = self.run_cli(
                ["network", "apply", "--request-id", request_id, "--config", "-"],
                stdin=config,
                nmcli=nmcli,
            )
            self.assertEqual(code, 0, error)

        lkg_path = self.lkg_path("wlan0")
        lkg_before = lkg_path.read_bytes()
        rescue_profile = json.loads(
            (self.root / "state" / "rescue.json").read_text(encoding="utf-8")
        )["profile"]
        code, result, error = self.run_cli(["network", "rescue"], nmcli=nmcli)

        self.assertEqual(code, 0, error)
        self.assertEqual(
            result,
            {
                "format": "ylx.network-result.v0",
                "ok": True,
                "action": "rescue",
                "mode": "hotspot",
                "recovery": "rescue",
            },
        )
        self.assertEqual(nmcli.active["wlan0"], rescue_profile)
        self.assert_connection_up_deadlines(nmcli)
        self.assertEqual(lkg_path.read_bytes(), lkg_before)
        self.assertNotIn("primary-secret", json.dumps(nmcli.commands))

    def test_rescue_fails_closed_when_no_hotspot_was_registered(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        code, result, _ = self.run_cli(["network", "rescue"], nmcli=nmcli)

        self.assertEqual(code, 3)
        self.assertEqual(result["error"]["code"], "rescue_unconfigured")
        self.assertEqual(nmcli.commands, [])

    def test_forget_resumes_a_profile_quarantined_before_power_loss(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        with (
            patch.dict(os.environ, self.environment, clear=False),
            patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
        ):
            network_module.ensure_rescue_ap("forget-power-loss-device")
            profile_name = "rp-ylx-wifi-client-deadbeef1234.nmconnection"
            quarantine = self.root / "profiles" / f".{profile_name}.forget"
            quarantine.write_text("psk=power-loss-secret\n", encoding="utf-8")
            quarantine.chmod(0o600)

            result = network_module.forget_network_client_profiles()

        self.assertEqual(result["removed_profiles"], [profile_name])
        self.assertFalse(quarantine.exists())
        self.assertFalse((self.root / "profiles" / profile_name).exists())
        self.assertGreaterEqual(nmcli.commands.count(["nmcli", "connection", "reload"]), 2)
        for path in self.root.rglob("*"):
            if path.is_file():
                self.assertNotIn(b"power-loss-secret", path.read_bytes(), path)

    def test_reconcile_discards_an_uncommitted_active_candidate_after_power_loss(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        code, _, error = self.run_cli(
            ["network", "apply", "--request-id", "primary-ap", "--config", "-"],
            stdin=('{"mode":"hotspot","ssid":"YLX-A1B2C3D4","psk":"primary-secret"}'),
            nmcli=nmcli,
        )
        self.assertEqual(code, 0, error)
        lkg = json.loads(self.lkg_path("wlan0").read_text(encoding="utf-8"))
        candidate = "rp-ylx-wifi-client-deadbeef1234"
        candidate_path = self.root / "profiles" / f"{candidate}.nmconnection"
        candidate_path.write_text("interrupted candidate\n", encoding="utf-8")
        candidate_path.chmod(0o600)
        journal_path = self.root / "state" / "journal.json"
        journal_path.write_text(
            json.dumps(
                {
                    "format": "ylx.network-journal.v0",
                    "phase": "verifying",
                    "request_id": "interrupted-client",
                    "request_fingerprint": "a" * 64,
                    "mode": "wifi-client",
                    "interface": "wlan0",
                    "profile": candidate,
                    "previous_profile": lkg["profile"],
                }
            ),
            encoding="utf-8",
        )
        journal_path.chmod(0o600)
        nmcli.active["wlan0"] = candidate
        nmcli.addresses["wlan0"] = "192.168.50.20/24"
        nmcli.routes["wlan0"] = "dst = 0.0.0.0/0, nh = 192.168.50.1, mt = 600"

        reconcile_code, result, reconcile_error = self.run_cli(
            ["network", "reconcile"], nmcli=nmcli
        )

        self.assertEqual(reconcile_code, 0, reconcile_error)
        self.assertEqual(
            result,
            {
                "format": "ylx.network-result.v0",
                "ok": True,
                "action": "reconcile",
                "interrupted_phase": "verifying",
                "recovery": "lkg",
            },
        )
        self.assertEqual(nmcli.active["wlan0"], lkg["profile"])
        self.assertFalse(candidate_path.exists())
        journal = json.loads(journal_path.read_text(encoding="utf-8"))
        self.assertEqual(journal["outcome"], "rolled_back")

        nmcli.commands.clear()
        second_code, second_result, second_error = self.run_cli(
            ["network", "reconcile"], nmcli=nmcli
        )
        self.assertEqual(second_code, 0, second_error)
        self.assertEqual(second_result["recovery"], "unchanged")
        self.assertEqual(len(nmcli.commands), 1)

        nmcli.active["wlan0"] = ""
        nmcli.addresses["wlan0"] = ""
        nmcli.routes["wlan0"] = ""
        reboot_code, reboot_result, reboot_error = self.run_cli(
            ["network", "reconcile"], nmcli=nmcli
        )
        self.assertEqual(reboot_code, 0, reboot_error)
        self.assertEqual(reboot_result["recovery"], "lkg")
        self.assertEqual(nmcli.active["wlan0"], lkg["profile"])

    def test_new_apply_reconciles_an_older_uncommitted_transaction_first(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        first_code, _, first_error = self.run_cli(
            ["network", "apply", "--request-id", "primary-ap", "--config", "-"],
            stdin='{"mode":"hotspot","ssid":"YLX-A1B2C3D4","psk":"primary-secret"}',
            nmcli=nmcli,
        )
        self.assertEqual(first_code, 0, first_error)
        lkg = json.loads(self.lkg_path("wlan0").read_text(encoding="utf-8"))
        candidate = "rp-ylx-wifi-client-deadbeef1234"
        candidate_path = self.root / "profiles" / f"{candidate}.nmconnection"
        candidate_path.write_text("interrupted candidate\n", encoding="utf-8")
        candidate_path.chmod(0o600)
        journal_path = self.root / "state" / "journal.json"
        journal_path.write_text(
            json.dumps(
                {
                    "format": "ylx.network-journal.v0",
                    "phase": "staging",
                    "request_id": "interrupted-client",
                    "request_fingerprint": "a" * 64,
                    "mode": "wifi-client",
                    "interface": "wlan0",
                    "profile": candidate,
                    "previous_profile": lkg["profile"],
                }
            ),
            encoding="utf-8",
        )
        journal_path.chmod(0o600)
        nmcli.active["wlan0"] = candidate

        code, result, error = self.run_cli(
            ["network", "apply", "--request-id", "wired-after-recovery", "--config", "-"],
            stdin='{"mode":"ethernet-dhcp"}',
            nmcli=nmcli,
        )

        self.assertEqual(code, 0, error)
        self.assertEqual(result["mode"], "ethernet-dhcp")
        self.assertFalse(candidate_path.exists())
        self.assertEqual(nmcli.active["wlan0"], lkg["profile"])
        self.assertTrue(nmcli.active["eth0"].startswith("rp-ylx-ethernet-dhcp-"))

    def test_reconcile_reactivates_a_committed_hotspot_after_restart(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        apply_code, _, apply_error = self.run_cli(
            ["network", "apply", "--request-id", "primary-ap", "--config", "-"],
            stdin='{"mode":"hotspot","ssid":"YLX-A1B2C3D4","psk":"primary-secret"}',
            nmcli=nmcli,
        )
        self.assertEqual(apply_code, 0, apply_error)
        profile = json.loads(self.lkg_path("wlan0").read_text(encoding="utf-8"))["profile"]
        nmcli.active["wlan0"] = ""
        nmcli.addresses["wlan0"] = ""
        nmcli.routes["wlan0"] = ""

        code, result, error = self.run_cli(["network", "reconcile"], nmcli=nmcli)

        self.assertEqual(code, 0, error)
        self.assertEqual(result["recovery"], "lkg")
        self.assertEqual(result["reason"], "profile_disconnected")
        self.assertEqual(nmcli.active["wlan0"], profile)

    def test_reconcile_materializes_lkg_rescue_and_receipt_after_commit_power_loss(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        apply_code, expected, apply_error = self.run_cli(
            ["network", "apply", "--request-id", "hotspot-commit", "--config", "-"],
            stdin='{"mode":"hotspot","ssid":"YLX-A1B2C3D4","psk":"primary-secret"}',
            nmcli=nmcli,
        )
        self.assertEqual(apply_code, 0, apply_error)
        state_dir = self.root / "state"
        for path in [
            self.lkg_path("wlan0"),
            state_dir / "rescue.json",
            *list((state_dir / "requests").glob("*.json")),
        ]:
            path.unlink()
        nmcli.commands.clear()

        code, result, error = self.run_cli(["network", "reconcile"], nmcli=nmcli)

        self.assertEqual(code, 0, error)
        self.assertEqual(result["recovery"], "unchanged")
        lkg = json.loads(self.lkg_path("wlan0").read_text(encoding="utf-8"))
        rescue = json.loads((state_dir / "rescue.json").read_text(encoding="utf-8"))
        receipt = json.loads(next((state_dir / "requests").glob("*.json")).read_text())
        self.assertEqual(lkg["profile"], rescue["profile"])
        self.assertEqual(lkg["mode"], "hotspot")
        self.assertNotIn("psk", lkg["config"])
        self.assertEqual(receipt["result"], expected)
        self.assertNotIn("primary-secret", json.dumps([lkg, rescue, receipt]))

    def test_reconcile_restores_rescue_ap_when_committed_client_loses_route(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        for request_id, config in [
            (
                "primary-ap",
                '{"mode":"hotspot","ssid":"YLX-A1B2C3D4","psk":"primary-secret"}',
            ),
            (
                "field-client",
                '{"mode":"wifi-client","ssid":"Field LAN","psk":"client-secret"}',
            ),
        ]:
            code, _, error = self.run_cli(
                ["network", "apply", "--request-id", request_id, "--config", "-"],
                stdin=config,
                nmcli=nmcli,
            )
            self.assertEqual(code, 0, error)
        rescue_profile = json.loads(
            (self.root / "state" / "rescue.json").read_text(encoding="utf-8")
        )["profile"]
        nmcli.routes["wlan0"] = ""

        code, result, error = self.run_cli(["network", "reconcile"], nmcli=nmcli)

        self.assertEqual(code, 0, error)
        self.assertEqual(
            result,
            {
                "format": "ylx.network-result.v0",
                "ok": True,
                "action": "reconcile",
                "interrupted_phase": None,
                "recovery": "rescue",
                "reason": "default_route_missing",
            },
        )
        self.assertEqual(nmcli.active["wlan0"], rescue_profile)

    def test_reconcile_restores_committed_ethernet_after_route_loss(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        apply_code, _, apply_error = self.run_cli(
            ["network", "apply", "--request-id", "wired-dhcp", "--config", "-"],
            stdin='{"mode":"ethernet-dhcp"}',
            nmcli=nmcli,
        )
        self.assertEqual(apply_code, 0, apply_error)
        profile = json.loads(self.lkg_path("eth0").read_text(encoding="utf-8"))["profile"]
        nmcli.routes["eth0"] = ""

        code, result, error = self.run_cli(["network", "reconcile"], nmcli=nmcli)

        self.assertEqual(code, 0, error)
        self.assertEqual(result["recovery"], "lkg")
        self.assertEqual(result["reason"], "default_route_missing")
        self.assertEqual(nmcli.active["eth0"], profile)
        self.assertIn("dst = 0.0.0.0/0", nmcli.routes["eth0"])

    def test_rescue_mutation_waits_for_shared_operation_lock(self) -> None:
        nmcli = FakeNmcli(self.root / "profiles")
        started = threading.Event()
        completed = threading.Event()
        errors: list[BaseException] = []

        def rescue() -> None:
            started.set()
            try:
                network_module.rescue_network()
            except BaseException as exc:  # pragma: no cover - surfaced below
                errors.append(exc)
            finally:
                completed.set()

        with (
            patch.dict(os.environ, self.environment, clear=False),
            patch("rp_ylx.network.subprocess.run", side_effect=nmcli),
        ):
            network_module.ensure_rescue_ap("operation-lock-device")
            with network_module._network_operation_lock():
                thread = threading.Thread(target=rescue)
                thread.start()
                self.assertTrue(started.wait(timeout=1))
                self.assertFalse(completed.wait(timeout=0.05))
            self.assertTrue(completed.wait(timeout=1))
            thread.join(timeout=1)

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])

    def test_operation_lease_contends_with_external_flock(self) -> None:
        path = network_module.network_operation_lock_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o660)
        try:
            os.fchmod(descriptor, 0o660)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with (
                self.assertRaises(network_module.NetworkError) as busy,
                network_module.network_operation_lease(blocking=False),
            ):
                pass
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

        self.assertEqual(busy.exception.code, "capture_active")
        self.assertEqual(path.stat().st_mode & 0o777, 0o660)


if __name__ == "__main__":
    unittest.main()
