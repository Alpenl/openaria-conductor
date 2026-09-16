"""Optional Linux status LED reflecting authoritative capture state."""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rp_ylx.recording_button import CaptureClient

LOG = logging.getLogger("rp-ylx.recording-indicator")


def indicator_pattern(snapshot: dict) -> str:
    state = snapshot.get("device_state")
    if state == "recording":
        return "recording"
    if state == "finalizing":
        return "saving"
    if state == "idle" and snapshot.get("retained_unsuccessful") is None:
        return "idle"
    return "error"


class StatusLed:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.maximum = int((path / "max_brightness").read_text())
        self.previous_brightness = int((path / "brightness").read_text())
        triggers = (path / "trigger").read_text().split()
        active = [value[1:-1] for value in triggers if value.startswith("[")]
        supported = {value.strip("[]") for value in triggers}
        if len(active) != 1 or self.maximum < 1 or "timer" not in supported:
            raise ValueError("status LED requires a valid brightness and timer trigger")
        self.previous_trigger = active[0]
        self.previous_delays = {}
        if self.previous_trigger == "timer":
            self.previous_delays = {
                name: (path / name).read_text().strip() for name in ("delay_on", "delay_off")
            }
        self.pattern: str | None = None
        self.owned = False

    def _write(self, name: str, value: str | int) -> None:
        (self.path / name).write_text(str(value) + "\n")

    def show(self, pattern: str) -> None:
        if pattern not in {"idle", "recording", "saving", "error"}:
            raise ValueError("unknown recording indicator pattern")
        if pattern == self.pattern:
            return
        self.owned = True
        self._write("trigger", "none")
        self._write("brightness", 0 if pattern == "idle" else self.maximum)
        if pattern in {"saving", "error"}:
            self._write("trigger", "timer")
            delay = 500 if pattern == "saving" else 125
            self._write("delay_on", delay)
            self._write("delay_off", delay)
        self.pattern = pattern
        LOG.info("recording indicator: %s", pattern)

    def restore(self) -> None:
        if not self.owned:
            return
        self._write("trigger", "none")
        self._write("brightness", self.previous_brightness)
        self._write("trigger", self.previous_trigger)
        for name, value in self.previous_delays.items():
            self._write(name, value)
        self.owned = False


def run_indicator(led_path: Path, client: CaptureClient, stopped: threading.Event) -> None:
    """Poll independently so slow HTTP requests never block physical button sampling."""
    led = None
    unavailable = False
    try:
        led = StatusLed(led_path)
        while not stopped.is_set():
            try:
                pattern = indicator_pattern(client.capture_snapshot())
                if unavailable:
                    LOG.info("recording indicator: capture status available again")
                unavailable = False
            except (OSError, ValueError, RuntimeError) as error:
                if not unavailable:
                    LOG.warning("recording indicator: capture status unavailable: %s", error)
                unavailable = True
                pattern = "error"
            if stopped.is_set():
                break
            led.show(pattern)
            stopped.wait(0.5)
    except (OSError, ValueError) as error:
        # A missing or unavailable LED must not disable the physical recording button.
        LOG.warning("recording indicator unavailable: %s", error)
    finally:
        if led is not None:
            try:
                led.restore()
            except OSError as error:
                LOG.warning("could not restore status LED: %s", error)
