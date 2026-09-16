from __future__ import annotations

import json
import threading
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from rp_ylx.api import Principal, ProviderError, SecurityPolicy, create_gateway_server
from rp_ylx.recording_button import (
    ButtonConfig,
    ButtonError,
    CaptureClient,
    DebouncedButton,
    create_capture_client,
    load_button_config,
    main,
    run_button,
)
from tests.test_gateway import DeviceProvider, _active_capture_status


class ButtonInputTest(unittest.TestCase):
    def test_bounce_hold_and_release_each_produce_one_press(self) -> None:
        button = DebouncedButton(0.08, 1.0)
        trace = [
            (False, 0.0),
            (False, 0.1),
            (True, 0.2),
            (False, 0.22),
            (True, 0.24),
            (True, 0.3),
            (True, 0.33),
            (True, 1.5),
            (False, 1.6),
            (True, 1.62),
            (True, 1.8),
            (False, 2.0),
            (False, 2.1),
            (True, 2.2),
            (True, 2.3),
        ]
        presses = [now for pressed, now in trace if button.update(pressed, now)]
        self.assertEqual(presses, [0.33, 2.3])

    def test_startup_held_and_slow_request_input_require_release(self) -> None:
        button = DebouncedButton(0.08, 1.0)
        for now in (0, 0.1, 2, 10):
            self.assertFalse(button.update(True, now))
        button.update(False, 11)
        button.update(False, 11.1)
        button.update(True, 12)
        self.assertTrue(button.update(True, 12.1))
        button.disarm()
        for now in (20, 21, 30):
            self.assertFalse(button.update(True, now))

    def test_cooldown_drops_press_instead_of_delaying_it(self) -> None:
        button = DebouncedButton(0.08, 1.0)
        trace = [
            (False, 0),
            (False, 0.1),
            (True, 0.2),
            (True, 0.3),
            (False, 0.4),
            (False, 0.5),
            (True, 0.6),
            (True, 0.7),
            (True, 2),
        ]
        self.assertEqual([now for pressed, now in trace if button.update(pressed, now)], [0.3])

    def test_disabled_config_does_not_open_gpio_or_api(self) -> None:
        with TemporaryDirectory() as directory:
            config = Path(directory) / "button.json"
            config.write_text("{}")
            with (
                patch("rp_ylx.recording_button.load_gpio") as gpio,
                patch("rp_ylx.recording_button.create_capture_client") as client,
            ):
                self.assertEqual(main(["--config", str(config)]), 0)
                gpio.assert_not_called()
                client.assert_not_called()

    def test_config_rejects_supply_ground_bad_types_and_unknown_fields(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "button.json"
            for value in (
                {"physical_pin": 1},
                {"physical_pin": 2},
                {"physical_pin": 6},
                {"active_low": "false"},
                {"enabled": 1},
                {"poll_ms": True},
                {"poll_ms": 100, "debounce_ms": 20},
                {"arbitrary": 1},
                {"status_led": "../../gpio"},
                {"status_led": "/sys/class/leds/ACT"},
                {"status_led": True},
            ):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    path.write_text(json.dumps(value))
                    load_button_config(path)

    def test_monitor_never_starts_led_worker(self) -> None:
        with TemporaryDirectory() as directory:
            config = Path(directory) / "button.json"
            config.write_text(json.dumps({"status_led": "ACT"}))
            with (
                patch("rp_ylx.recording_button.load_gpio"),
                patch("rp_ylx.recording_button.run_button"),
                patch("rp_ylx.recording_button.create_capture_client") as client,
                patch("rp_ylx.recording_indicator.run_indicator") as indicator,
            ):
                self.assertEqual(main(["--config", str(config), "--monitor"]), 0)
                client.assert_not_called()
                indicator.assert_not_called()

    def run_trace(self, *, monitor=False, active_low=False, failure=False):
        clock = [0.0]
        stop = Mock()
        stop.is_set.side_effect = lambda: clock[0] >= 3.0
        stop.wait.side_effect = lambda _: clock.__setitem__(0, round(clock[0] + 0.02, 2))
        gpio = Mock(BOARD="BOARD", IN="IN")
        gpio.input.side_effect = lambda _: int(
            (0.2 <= clock[0] < 1.2 or clock[0] >= 2) != active_low
        )
        client = Mock()
        if failure:
            client.handle_press.side_effect = [TimeoutError("ambiguous command"), "stop"]
        with patch("rp_ylx.recording_button.time.monotonic", side_effect=lambda: clock[0]):
            run_button(
                ButtonConfig(enabled=True, active_low=active_low),
                gpio,
                client,
                stop,
                monitor=monitor,
            )
        gpio.setup.assert_called_once_with(37, "IN")
        gpio.cleanup.assert_called_once_with(37)
        return client

    def test_both_polarities_toggle_once_per_press_and_recover_from_timeout(self) -> None:
        for active_low in (False, True):
            with self.subTest(active_low=active_low):
                client = self.run_trace(active_low=active_low, failure=True)
                self.assertEqual(client.handle_press.call_count, 2)

    def test_monitor_reports_raw_levels_without_capture_requests(self) -> None:
        output = StringIO()
        with redirect_stdout(output):
            client = self.run_trace(monitor=True)
        client.handle_press.assert_not_called()
        self.assertEqual(
            [json.loads(line)["level"] for line in output.getvalue().splitlines()], [0, 1, 0, 1]
        )


class ButtonGatewayTest(unittest.TestCase):
    def setUp(self) -> None:
        self.provider = DeviceProvider()
        self.clock = {
            "schema": "ylx.clock-status.v1",
            "source": "ntp",
            "unix_time_ms": 1789552192758,
            "challenge": None,
            "expires_in_ms": 0,
            "applied": False,
        }
        self.provider.clock_status = Mock(side_effect=lambda _: self.clock)
        principal = Principal(
            "physical-button",
            permissions={
                "getCaptureStatus": None,
                "getDeviceClock": None,
                "startCapture": None,
                "stopCapture": None,
            },
        )
        policy = SecurityPolicy.customer(
            tokens={"button-test-token": principal}, csrf_token="button-test-token"
        )
        self.server = create_gateway_server("127.0.0.1", 0, self.provider, security=policy)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.client = CaptureClient(
            f"http://127.0.0.1:{self.server.server_port}", token="button-test-token"
        )

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def test_authenticated_start_stop_use_current_contract_and_distinct_keys(self) -> None:
        with patch.dict("os.environ", {"HTTP_PROXY": "http://127.0.0.1:1"}):
            self.assertEqual(self.client.handle_press(), "start")
        self.provider.status = _active_capture_status()
        self.assertEqual(self.client.handle_press(), "stop")
        commands = list(self.provider.commands.items())
        self.assertEqual([key[1] for key, _ in commands], ["start", "stop"])
        self.assertNotEqual(commands[0][0][2], commands[1][0][2])
        self.assertEqual(
            json.loads(commands[0][1][0]),
            {
                "schema": "ylx.capture-start.v2",
                "mode": "production",
                "take": {"kind": "new"},
            },
        )
        self.assertEqual(
            json.loads(commands[1][1][0]),
            {
                "schema": "ylx.capture-stop.v2",
                "reason": "user",
            },
        )
        self.provider.clock_status.assert_called_once_with("physical-button")

    def test_uninitialized_clock_blocks_start_but_never_blocks_stop(self) -> None:
        self.clock.update(source="unsynchronized", challenge="a" * 32, expires_in_ms=5000)
        with self.assertRaisesRegex(ButtonError, "日期尚未校准"):
            self.client.handle_press()
        self.assertFalse(self.provider.commands)
        self.provider.status = _active_capture_status()
        self.assertEqual(self.client.handle_press(), "stop")

    def test_auth_failure_and_camera_failure_do_not_retry_commands(self) -> None:
        self.client.token = "wrong"
        with self.assertRaisesRegex(ButtonError, "401"):
            self.client.handle_press()
        self.assertFalse(self.provider.commands)
        self.client.token = "button-test-token"
        self.provider.camera_error = ProviderError("camera_not_connected", "missing", status=409)
        with patch.object(
            self.provider, "start_capture", wraps=self.provider.start_capture
        ) as start:
            with self.assertRaisesRegex(ButtonError, "409"):
                self.client.handle_press()
            self.assertEqual(start.call_count, 1)

    def test_saving_and_unknown_states_are_ignored_without_clock_or_commands(self) -> None:
        for state in ("finalizing", "starting", "blocked", "failed", "unexpected"):
            with self.subTest(state=state), patch.object(self.client, "request") as request:
                request.return_value = {
                    "schema": "ylx.capture-status.v4",
                    "snapshot": {"schema": "ylx.capture-snapshot-event.v4", "device_state": state},
                }
                self.assertIsNone(self.client.handle_press())
                request.assert_called_once_with("/capture/status")

    def test_client_uses_installed_certificate_and_loopback_for_customer(self) -> None:
        config = Mock(
            host="0.0.0.0",
            port=8080,
            security_profile="customer",
            bearer_token_file=Path("/token"),
            tls_certificate_file=Path("/cert"),
        )
        with (
            patch("rp_ylx.daemon.load_production_config", return_value=config),
            patch("rp_ylx.daemon._read_bearer_token", return_value="button-test-token"),
            patch("rp_ylx.recording_button.ssl.create_default_context") as context,
            patch("rp_ylx.recording_button.CaptureClient") as factory,
        ):
            create_capture_client(Path("/config"), 3)
            context.assert_called_once_with(cafile="/cert")
            self.assertFalse(context.return_value.check_hostname)
            factory.assert_called_once_with(
                "https://127.0.0.1:8080",
                token="button-test-token",
                timeout_seconds=3,
                tls_context=context.return_value,
            )


if __name__ == "__main__":
    unittest.main()
