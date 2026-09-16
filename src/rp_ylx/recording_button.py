"""Optional RDK X5 input button, using the same Device API as Echo."""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import re
import signal
import ssl
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path

from rp_ylx.clock_sync import valid_clock_response

LOG = logging.getLogger("rp-ylx.recording-button")
GPIO_PINS = frozenset(
    {
        3,
        5,
        7,
        8,
        10,
        11,
        12,
        13,
        15,
        16,
        18,
        19,
        21,
        22,
        23,
        24,
        26,
        27,
        28,
        29,
        31,
        32,
        33,
        35,
        36,
        37,
        38,
        40,
    }
)
MAX_RESPONSE_BYTES = 1024 * 1024


@dataclass(frozen=True)
class ButtonConfig:
    enabled: bool = False
    physical_pin: int = 37
    active_low: bool = False
    debounce_ms: int = 80
    cooldown_ms: int = 1000
    poll_ms: int = 20
    request_timeout_ms: int = 5000
    status_led: str | None = "ACT"

    def __post_init__(self) -> None:
        if type(self.enabled) is not bool or type(self.active_low) is not bool:
            raise ValueError("enabled and active_low must be booleans")
        if type(self.physical_pin) is not int or self.physical_pin not in GPIO_PINS:
            raise ValueError("physical_pin must be an RDK X5 BOARD GPIO pin, not power or ground")
        if self.status_led is not None and (
            not isinstance(self.status_led, str)
            or re.fullmatch(r"[A-Za-z0-9_:-]{1,64}", self.status_led) is None
        ):
            raise ValueError("status_led must be a Linux LED name or null")
        for name, minimum, maximum in (
            ("debounce_ms", 20, 1000),
            ("cooldown_ms", 100, 10000),
            ("poll_ms", 5, 100),
            ("request_timeout_ms", 100, 10000),
        ):
            value = getattr(self, name)
            if type(value) is not int or not minimum <= value <= maximum:
                raise ValueError(f"{name} must be an integer in [{minimum}, {maximum}]")
        if self.poll_ms > self.debounce_ms or self.cooldown_ms < self.debounce_ms:
            raise ValueError("require poll_ms <= debounce_ms <= cooldown_ms")


def load_button_config(path: Path) -> ButtonConfig:
    raw = path.read_bytes()
    if len(raw) > 8192:
        raise ValueError("button configuration is too large")
    value = json.loads(raw)
    if not isinstance(value, dict) or set(value) - set(ButtonConfig.__dataclass_fields__):
        raise ValueError("invalid button configuration fields")
    return ButtonConfig(**value)


class DebouncedButton:
    """Require a stable release before each press, including after startup or a slow command."""

    def __init__(self, debounce_seconds: float, cooldown_seconds: float) -> None:
        self.debounce_seconds = debounce_seconds
        self.cooldown_seconds = cooldown_seconds
        self.last_press = float("-inf")
        self.disarm()

    def disarm(self) -> None:
        self.candidate: bool | None = None
        self.candidate_since = 0.0
        self.armed = False

    def update(self, pressed: bool, now: float) -> bool:
        if pressed != self.candidate:
            self.candidate = pressed
            self.candidate_since = now
            return False
        if now - self.candidate_since < self.debounce_seconds:
            return False
        if not pressed:
            self.armed = True
        elif self.armed:
            self.armed = False
            if now - self.last_press >= self.cooldown_seconds:
                self.last_press = now
                return True
        return False


class ButtonError(RuntimeError):
    pass


class CaptureClient:
    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        timeout_seconds: float = 5.0,
        tls_context: ssl.SSLContext | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout_seconds = timeout_seconds
        self.opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            urllib.request.HTTPSHandler(context=tls_context),
        )

    def request(self, path: str, body: dict | None = None) -> dict | None:
        headers = {"Accept": "application/json", "Origin": self.base_url}
        if self.token is not None:
            headers["Authorization"] = "Bearer " + self.token
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            headers["Idempotency-Key"] = str(uuid.uuid4())
            if self.token is not None:
                headers["X-CSRF-Token"] = self.token
            data = json.dumps(body, separators=(",", ":")).encode()
        request = urllib.request.Request(
            self.base_url + "/api/v4" + path, data=data, headers=headers
        )
        try:
            with self.opener.open(request, timeout=self.timeout_seconds) as response:
                if response.status == 204:
                    return None
                raw = response.read(MAX_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as error:
            # Never replay an ambiguous start/stop automatically after an error.
            with error:
                raise ButtonError(f"Device API {path} returned HTTP {error.code}") from error
        if len(raw) > MAX_RESPONSE_BYTES:
            raise ButtonError("Device API response is too large")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ButtonError("Device API response is not an object")
        return value

    def capture_snapshot(self) -> dict:
        status = self.request("/capture/status")
        if not isinstance(status, dict) or status.get("schema") != "ylx.capture-status.v4":
            raise ButtonError("invalid capture status")
        snapshot = status.get("snapshot")
        if (
            not isinstance(snapshot, dict)
            or snapshot.get("schema") != "ylx.capture-snapshot-event.v4"
        ):
            raise ButtonError("invalid capture snapshot")
        return snapshot

    def handle_press(self) -> str | None:
        snapshot = self.capture_snapshot()
        state = snapshot.get("device_state")
        if state == "recording":
            self.request("/capture/stop", {"schema": "ylx.capture-stop.v2", "reason": "user"})
            return "stop"
        if state != "idle":
            LOG.info("button ignored while capture state is %s", state)
            return None
        clock = self.request("/clock")
        if not valid_clock_response(clock) or clock["source"] not in {"ntp", "client"}:
            raise ButtonError("设备日期尚未校准：请联网校时或先用手机/电脑打开控制页面")
        # All camera, volume, network-operation and active-recording guards remain in the daemon.
        self.request(
            "/capture/start",
            {
                "schema": "ylx.capture-start.v2",
                "mode": "production",
                "take": {"kind": "new"},
            },
        )
        return "start"


def create_capture_client(config_path: Path, timeout_seconds: float) -> CaptureClient:
    from rp_ylx.daemon import _read_bearer_token, load_production_config

    config = load_production_config(config_path)
    host = {"0.0.0.0": "127.0.0.1", "::": "::1"}.get(config.host, config.host)
    authority = f"[{host}]" if ":" in host else host
    context = None
    token = None
    scheme = "http"
    if config.security_profile == "customer":
        assert config.bearer_token_file is not None and config.tls_certificate_file is not None
        scheme = "https"
        token = _read_bearer_token(config.bearer_token_file)
        context = ssl.create_default_context(cafile=str(config.tls_certificate_file))
        # Trust only the installed device certificate; it need not name the loopback address.
        context.check_hostname = False
    return CaptureClient(
        f"{scheme}://{authority}:{config.port}",
        token=token,
        timeout_seconds=timeout_seconds,
        tls_context=context,
    )


def load_gpio():
    path = "/usr/lib/hobot-gpio/lib/python"
    if path not in sys.path:
        sys.path.append(path)
    gpio = importlib.import_module("Hobot.GPIO")
    if gpio.model != "RDK_X5":
        raise ButtonError("physical recording button requires RDK X5")
    return gpio


def run_button(
    config: ButtonConfig,
    gpio,
    client: CaptureClient | None,
    stopped: threading.Event,
    *,
    monitor: bool = False,
) -> None:
    button = DebouncedButton(config.debounce_ms / 1000, config.cooldown_ms / 1000)
    gpio.setmode(gpio.BOARD)
    configured = False
    try:
        gpio.setup(config.physical_pin, gpio.IN)
        configured = True
        LOG.info(
            "button ready: BOARD pin=%d active_low=%s monitor=%s",
            config.physical_pin,
            config.active_low,
            monitor,
        )
        previous_level = None
        while not stopped.is_set():
            level = gpio.input(config.physical_pin)
            if level not in (0, 1):
                raise ButtonError("invalid GPIO input level")
            now = time.monotonic()
            if monitor and level != previous_level:
                print(
                    json.dumps(
                        {
                            "physical_pin": config.physical_pin,
                            "level": int(level),
                            "pressed": bool(level) != config.active_low,
                        }
                    ),
                    flush=True,
                )
            previous_level = level
            if not monitor and button.update(bool(level) != config.active_low, now):
                try:
                    assert client is not None
                    action = client.handle_press()
                    LOG.info("button action: %s", action or "ignored")
                except (OSError, ValueError, ButtonError) as error:
                    LOG.warning("button command failed: %s", error)
                finally:
                    # Do not turn input accumulated during a slow API request into another press.
                    button.disarm()
            stopped.wait(config.poll_ms / 1000)
    finally:
        if configured:
            gpio.cleanup(config.physical_pin)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="RDK X5 physical recording button")
    parser.add_argument("--config", type=Path, default=Path("/etc/rp-ylx/recording-button.json"))
    parser.add_argument("--device-config", type=Path, default=Path("/etc/rp-ylx/device.json"))
    parser.add_argument(
        "--monitor", action="store_true", help="print input levels without recording"
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config = load_button_config(args.config)
    if not config.enabled and not args.monitor:
        LOG.info("physical recording button disabled in configuration")
        return 0
    client = (
        None
        if args.monitor
        else create_capture_client(
            args.device_config,
            config.request_timeout_ms / 1000,
        )
    )
    stopped = threading.Event()
    previous = {}
    indicator_thread = None
    try:
        for signum in (signal.SIGTERM, signal.SIGINT):
            previous[signum] = signal.signal(signum, lambda *_: stopped.set())
        gpio = load_gpio()
        if config.status_led is not None and not args.monitor:
            from rp_ylx.recording_indicator import run_indicator

            indicator_client = create_capture_client(
                args.device_config, min(config.request_timeout_ms / 1000, 2.0)
            )
            indicator_thread = threading.Thread(
                target=run_indicator,
                args=(Path("/sys/class/leds") / config.status_led, indicator_client, stopped),
                name="recording-indicator",
                daemon=True,
            )
            indicator_thread.start()
        run_button(config, gpio, client, stopped, monitor=args.monitor)
    finally:
        stopped.set()
        if indicator_thread is not None:
            indicator_thread.join(timeout=3)
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
