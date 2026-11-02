"""Optional, bounded offline voice feedback. Never part of capture success."""

from __future__ import annotations

import logging
import os
import queue
import subprocess
import threading
import time
from pathlib import Path

LOG = logging.getLogger(__name__)
EVENTS = {"started", "saved", "start_failed", "recording_failed", "save_failed"}


class VoiceFeedback:
    def __init__(self, device: str):
        self.device = device
        self.pending = queue.Queue(maxsize=4)
        self.lock = threading.Lock()
        self.last = None
        self.worker = threading.Thread(target=self._run, name="capture-voice", daemon=True)
        self.worker.start()

    def notify(self, event: str, identity: str) -> None:
        if event not in EVENTS:
            return
        with self.lock:
            key = (event, identity)
            if (
                self.last is not None
                and self.last[0] == key
                and time.monotonic() - self.last[1] < 5
            ):
                return
            self.last = (key, time.monotonic())
            # Only the latest state is useful; never announce an old queued start after stop.
            while True:
                try:
                    self.pending.get_nowait()
                except queue.Empty:
                    break
            self.pending.put_nowait((event, time.monotonic()))

    def _run(self) -> None:
        while True:
            event, created = self.pending.get()
            if time.monotonic() - created > 4:
                LOG.warning("voice expired: %s", event)
                continue
            try:
                path = Path(__file__).with_name("voice_assets") / (event + ".wav")
                subprocess.run(
                    ["/usr/bin/aplay", "-q", "-D", self.device, str(path)],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=5,
                    check=True,
                )
                LOG.info("voice played: %s", event)
            except Exception:
                LOG.warning("voice playback failed: %s", event, exc_info=True)


_instance = None
_instance_lock = threading.Lock()


def notify(event: str, identity: str = "") -> None:
    """Opt in per device. Errors, missing ALSA and worker failure cannot escape."""
    global _instance
    if os.environ.get("OPENARIA_VOICE_ENABLED") != "1":
        return
    try:
        with _instance_lock:
            if _instance is None:
                _instance = VoiceFeedback(
                    os.environ.get("OPENARIA_VOICE_DEVICE", "plughw:CARD=duplexaudio,DEV=0")
                )
        _instance.notify(event, identity)
    except Exception:
        LOG.warning("voice notification failed", exc_info=True)
