"""NetworkManager/nmcli test doubles and NetworkHarness."""

from __future__ import annotations

import configparser
import ctypes
import ctypes.util
import io
import json
import os
import subprocess
import tempfile
import unittest
import uuid
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager, redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import rp_ylx.network as network_module
from rp_ylx.cli import main
from rp_ylx.network_control import NetworkController

from . import ManagedHarness


class FakeNmcli:
    """In-memory stub for nmcli subprocess invocations backed by .nmconnection keyfiles."""

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
            if name in {profile["connection"]["id"], profile["connection"].get("uuid")}:
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
        if command[-5:] in (
            ["-g", "GENERAL.CON-UUID", "device", "show", "wlan0"],
            ["--get-values", "GENERAL.CON-UUID", "device", "show", "wlan0"],
        ):
            profile_name = self.active["wlan0"]
            value = self._load_profile(profile_name)["connection"]["uuid"] if profile_name else "--"
            return subprocess.CompletedProcess(command, 0, value + "\n", "")
        if "-g" in command and "device" in command:
            field = command[command.index("-g") + 1]
            if field == "IP4.GATEWAY":
                return subprocess.CompletedProcess(command, 0, "192.168.50.1\n", "")
            raise AssertionError(f"unsupported bootstrap device field: {field}")
        if "-g" in command and "connection" in command and "show" in command:
            field = command[command.index("-g") + 1]
            profile = self._load_profile(command[-1])
            section, key = field.split(".", 1)
            section = {"802-11-wireless": "wifi", "802-11-wireless-security": "wifi-security"}.get(
                section, section
            )
            value = profile.get(section, key, fallback="")
            return subprocess.CompletedProcess(command, 0, value + "\n", "")
        if "connection" in command and "clone" in command:
            assert self.profile_dir is not None
            profile = self._load_profile(command[-2])
            name = command[-1]
            profile["connection"]["id"] = name
            profile["connection"]["uuid"] = str(uuid.uuid4())
            path = self.profile_dir / (name + ".nmconnection")
            with path.open("w") as stream:
                profile.write(stream)
            path.chmod(0o600)
            return subprocess.CompletedProcess(command, 0, "", "")
        if "connection" in command and "modify" in command:
            assert self.profile_dir is not None
            name_index = command.index("id") + 1
            name = command[name_index]
            profile = self._load_profile(name)
            for field, value in zip(
                command[name_index + 1 :: 2], command[name_index + 2 :: 2], strict=True
            ):
                section, key = field.split(".", 1)
                profile[section][key] = value
            with (self.profile_dir / (name + ".nmconnection")).open("w") as stream:
                profile.write(stream)
            return subprocess.CompletedProcess(command, 0, "", "")
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
            name = command[-1]
            profile = self._load_profile(name)
            if field == "802-11-wireless-security.key-mgmt":
                value = profile.get("wifi-security", "key-mgmt", fallback="")
                return subprocess.CompletedProcess(command, 0, f"{value}\n", "")
            if field == "802-11-wireless.ssid":
                return subprocess.CompletedProcess(command, 0, profile["wifi"]["ssid"] + "\n", "")
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
            profile_name = self.active[interface]
            address = self.addresses[interface]
            route = self.routes[interface]
            rendered = (
                f"GENERAL.STATE:{'100 (connected)' if profile_name else '30 (disconnected)'}\n"
                f"GENERAL.CONNECTION:{profile_name}\n"
                f"IP4.ADDRESS[1]:{address}\n"
                f"IP4.ROUTE[1]:{route}\n"
            )
            return subprocess.CompletedProcess(command, 0, rendered, "")
        raise AssertionError(f"未定义的 nmcli 调用：{command!r}")


def glib_keyfile_string(path: Path, group: str, key: str) -> str:
    """Read a string from a NetworkManager keyfile using GLib GKeyFile."""
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


class NetworkHarness(ManagedHarness):
    """Temporary environment and FakeNmcli harness for network CLI and controller tests."""

    def __init__(self, *, patch_all_env: bool = False, patch_nmcli: bool = False) -> None:
        super().__init__()
        self._patch_all_env = patch_all_env
        self._patch_nmcli = patch_nmcli

    def _configure(self, stack: ExitStack) -> None:
        self.temporary = stack.enter_context(tempfile.TemporaryDirectory())
        self.root = Path(self.temporary)
        self.state_dir = self.root / "state"
        self.profile_dir = self.root / "profiles"
        self.avahi_dir = self.root / "avahi"
        self.device_config_path = self.root / "device.json"
        self.operation_lock_path = self.root / "run" / "network-operation.lock"
        self.environment = {
            "RP_YLX_NETWORK_STATE_DIR": str(self.state_dir),
            "RP_YLX_NM_PROFILE_DIR": str(self.profile_dir),
            "RP_YLX_AVAHI_SERVICE_DIR": str(self.avahi_dir),
            "RP_YLX_DEVICE_CONFIG_PATH": str(self.device_config_path),
            "RP_YLX_NETWORK_OPERATION_LOCK_PATH": str(self.operation_lock_path),
        }
        env_patch = (
            self.environment
            if self._patch_all_env
            else {"RP_YLX_NETWORK_OPERATION_LOCK_PATH": str(self.operation_lock_path)}
        )
        stack.enter_context(patch.dict(os.environ, env_patch, clear=False))
        self.nmcli = FakeNmcli(self.profile_dir)
        if self._patch_nmcli:
            stack.enter_context(patch("rp_ylx.network.subprocess.run", side_effect=self.nmcli))

    def lkg_path(self, interface: str, *, root: Path | None = None) -> Path:
        return (root or self.root) / "state" / f"lkg-{interface}.json"

    def run_cli(
        self,
        arguments: list[str],
        *,
        stdin: str | io.TextIOBase = "",
        nmcli: FakeNmcli | None = None,
    ) -> tuple[int, dict[str, object], str]:
        """Run `rp_ylx.cli.main` with isolated env, stdin, stdout/stderr, and FakeNmcli."""
        output = io.StringIO()
        error = io.StringIO()
        runner = nmcli or FakeNmcli(self.profile_dir)
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

    @contextmanager
    def controller(
        self,
        *,
        device_id: str = "device-test",
        require_root: bool = False,
        nmcli: FakeNmcli | None = None,
    ) -> Iterator[NetworkController]:
        """Yield a NetworkController wired to isolated env dirs and FakeNmcli."""
        runner = nmcli or self.nmcli
        with (
            patch.dict(os.environ, self.environment, clear=False),
            patch("rp_ylx.network.subprocess.run", side_effect=runner),
        ):
            ctrl = NetworkController(device_id=device_id, require_root=require_root)
            try:
                yield ctrl
            finally:
                ctrl.close()

    @staticmethod
    def assert_connection_up_deadlines(nmcli: FakeNmcli) -> None:
        up_calls = [
            (command, timeout)
            for command, timeout in nmcli.timeouts
            if "connection" in command and "up" in command
        ]
        if not up_calls:
            raise AssertionError("expected at least one nmcli connection up call")
        for command, timeout in up_calls:
            wait_index = command.index("--wait")
            if command[wait_index : wait_index + 2] != [
                "--wait",
                str(network_module.NETWORK_ACTIVATION_WAIT_SECONDS),
            ]:
                raise AssertionError(
                    f"unexpected --wait flag in nmcli connection up command: {command!r}"
                )
            if timeout != network_module.NETWORK_ACTIVATION_TIMEOUT_SECONDS:
                raise AssertionError(
                    f"expected timeout {network_module.NETWORK_ACTIVATION_TIMEOUT_SECONDS}, "
                    f"got {timeout}"
                )


class NetworkMixin(unittest.TestCase):
    """unittest.TestCase mixin provisioning a NetworkHarness as self.harness."""

    harness: NetworkHarness

    def setUp(self) -> None:
        super().setUp()
        self.harness = NetworkHarness().setUp()
        self.addCleanup(self.harness.tearDown)


__all__ = [
    "FakeNmcli",
    "NetworkHarness",
    "NetworkMixin",
    "glib_keyfile_string",
]
