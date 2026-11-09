"""Optional, bounded offline voice feedback. Never part of capture success."""

from __future__ import annotations

import array
import io
import logging
import os
import queue
import subprocess
import sys
import threading
import time
import wave
from pathlib import Path

LOG = logging.getLogger(__name__)
EVENTS = {
    "started",
    "saved",
    "start_failed",
    "recording_failed",
    "save_failed",
    "clock_ready",
    "clock_required",
}


def valid_volume(value: object) -> bool:
    return type(value) is int and 0 <= value <= 100


def scaled_wave(path: Path, volume: int) -> bytes:
    """Scale only the bundled PCM prompt; never touch capture or ALSA mixer gain."""
    if not valid_volume(volume):
        raise ValueError("invalid voice volume")
    with wave.open(str(path), "rb") as source:
        params = source.getparams()
        if params.sampwidth != 2 or params.comptype != "NONE":
            raise ValueError("voice asset must be PCM16")
        samples = array.array("h", source.readframes(params.nframes))
    if sys.byteorder != "little":
        samples.byteswap()
    samples = array.array("h", (int(sample * volume / 100) for sample in samples))
    if sys.byteorder != "little":
        samples.byteswap()
    output = io.BytesIO()
    with wave.open(output, "wb") as target:
        target.setparams(params)
        target.writeframes(samples.tobytes())
    return output.getvalue()


class VoiceFeedback:
    def __init__(self, device: str):
        self.device = device
        self.pending = queue.Queue(maxsize=4)
        self.lock = threading.Lock()
        self.last = None
        self.worker = threading.Thread(target=self._run, name="capture-voice", daemon=True)
        self.worker.start()

    def notify(self, event: str, identity: str, volume: int = 100) -> None:
        if event not in EVENTS or not valid_volume(volume):
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
            if volume > 0:
                self.pending.put_nowait((event, time.monotonic(), volume))

    def _run(self) -> None:
        while True:
            event, created, volume = self.pending.get()
            if time.monotonic() - created > 4:
                LOG.warning("voice expired: %s", event)
                continue
            try:
                path = Path(__file__).with_name("voice_assets") / (event + ".wav")
                subprocess.run(
                    ["/usr/bin/aplay", "-q", "-D", self.device, "-"],
                    input=scaled_wave(path, volume),
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


def notify(event: str, identity: str = "", *, volume: int = 100) -> None:
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
        _instance.notify(event, identity, volume)
    except Exception:
        LOG.warning("voice notification failed", exc_info=True)
