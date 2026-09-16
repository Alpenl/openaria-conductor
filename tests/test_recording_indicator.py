from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from rp_ylx.recording_indicator import StatusLed, indicator_pattern, run_indicator


class RecordingIndicatorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "ACT"
        self.path.mkdir()
        for name, value in {
            "trigger": "none timer [heartbeat] default-on",
            "brightness": "0",
            "max_brightness": "1",
        }.items():
            (self.path / name).write_text(value + "\n")

    def test_state_uses_actual_recording_including_web_stop_and_failures(self) -> None:
        for state, failure, expected in (
            ("idle", None, "idle"),
            ("recording", None, "recording"),
            ("finalizing", None, "saving"),
            ("idle", {"session_id": "failed"}, "error"),
            ("blocked", None, "error"),
            ("unknown", None, "error"),
        ):
            with self.subTest(state=state, failure=failure):
                self.assertEqual(
                    indicator_pattern({"device_state": state, "retained_unsuccessful": failure}),
                    expected,
                )

    def test_kernel_blink_and_steady_states_restore_original_heartbeat(self) -> None:
        led = StatusLed(self.path)
        for pattern, brightness, trigger, delay in (
            ("idle", "0", "none", None),
            ("recording", "1", "none", None),
            ("saving", "1", "timer", "500"),
            ("error", "1", "timer", "125"),
            ("idle", "0", "none", None),
        ):
            with self.subTest(pattern=pattern):
                led.show(pattern)
                self.assertEqual((self.path / "brightness").read_text().strip(), brightness)
                self.assertEqual((self.path / "trigger").read_text().strip(), trigger)
                if delay is not None:
                    for name in ("delay_on", "delay_off"):
                        self.assertEqual((self.path / name).read_text().strip(), delay)
                with patch.object(led, "_write") as write:
                    led.show(pattern)
                    write.assert_not_called()
        led.restore()
        self.assertEqual((self.path / "trigger").read_text().strip(), "heartbeat")
        self.assertEqual((self.path / "brightness").read_text().strip(), "0")

    def test_preexisting_timer_settings_are_restored(self) -> None:
        (self.path / "trigger").write_text("none [timer] heartbeat\n")
        (self.path / "brightness").write_text("1\n")
        (self.path / "delay_on").write_text("200\n")
        (self.path / "delay_off").write_text("700\n")
        led = StatusLed(self.path)
        led.show("error")
        led.restore()
        self.assertEqual((self.path / "trigger").read_text().strip(), "timer")
        self.assertEqual((self.path / "brightness").read_text().strip(), "1")
        self.assertEqual((self.path / "delay_on").read_text().strip(), "200")
        self.assertEqual((self.path / "delay_off").read_text().strip(), "700")

    def test_status_timeout_signals_error_then_recovers_and_restores_on_exit(self) -> None:
        client = Mock()
        client.capture_snapshot.side_effect = [
            {"device_state": "recording"},
            TimeoutError("unreachable"),
            {"device_state": "idle"},
        ]
        stopped = Mock()
        patterns = []
        stopped.is_set.side_effect = lambda: len(patterns) == 3
        stopped.wait.side_effect = lambda _: patterns.append(
            (
                (self.path / "trigger").read_text().strip(),
                (self.path / "brightness").read_text().strip(),
            )
        )
        run_indicator(self.path, client, stopped)
        self.assertEqual(patterns, [("none", "1"), ("timer", "1"), ("none", "0")])
        self.assertEqual((self.path / "trigger").read_text().strip(), "heartbeat")
        client.handle_press.assert_not_called()

    def test_missing_led_and_failed_write_do_not_disable_recording_button(self) -> None:
        client = Mock()
        run_indicator(self.path / "missing", client, Mock())
        client.capture_snapshot.assert_not_called()
        client.capture_snapshot.return_value = {"device_state": "recording"}
        stopped = Mock()
        stopped.is_set.return_value = False
        led = StatusLed(self.path)
        original_write = led._write

        def write(name, value):
            if name == "brightness" and value == 1:
                raise PermissionError("LED unavailable")
            original_write(name, value)

        with (
            patch("rp_ylx.recording_indicator.StatusLed", return_value=led),
            patch.object(led, "_write", side_effect=write),
        ):
            run_indicator(self.path, client, stopped)
        self.assertEqual((self.path / "trigger").read_text().strip(), "heartbeat")


if __name__ == "__main__":
    unittest.main()
