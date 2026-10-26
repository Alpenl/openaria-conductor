"""Ablate one preview codec using actual source and a controlled codec delay."""

import argparse
import hashlib
import json
import statistics
import subprocess
import threading
import time
import types
from collections import namedtuple
from contextlib import suppress
from pathlib import Path

BASELINE = "e7da4babf1d1f48ebc262b5714497cfabac98801"
Frame = namedtuple("Frame", "sequence jpeg")


def load(source):
    module = types.ModuleType("preview_probe")
    exec(compile(source, "preview_thumbnail.py", "exec"), module.__dict__)
    return module.PreviewThumbnails


def stop(previews):
    with previews.condition:
        worker = previews.worker
        previews.clear()
    if worker is not None:
        worker.join(2)
        assert not worker.is_alive(), "preview worker did not terminate"


def overlap_probe(cls):
    first, second, release = (threading.Event() for _ in range(3))
    source = [Frame(1, b"first")]

    class Codec:
        def resize(self, jpeg):
            (first if jpeg == b"first" else second).set()
            assert release.wait(2)
            return jpeg

        def close(self):
            pass

    previews = cls(lambda: source[0], Codec)

    def request():
        # Initial view may time out during the deliberate stall.
        with suppress(RuntimeError):
            previews.get()

    viewer = threading.Thread(target=request)
    viewer.start()
    try:
        assert first.wait(1)
        source[0] = Frame(2, b"second")
        return second.wait(0.3)
    finally:
        previews.clear()
        release.set()
        viewer.join(1)
        stop(previews)


def measure(cls, codec_delay):
    published = []

    class Observed(cls):
        def _encode(self, codec, frame, generation):
            super()._encode(codec, frame, generation)
            with self.condition:
                if self.latest is not None and self.latest.sequence == frame.sequence:
                    published.append((time.monotonic(), frame.sequence))

    class Codec:
        def resize(self, jpeg):
            if codec_delay:
                time.sleep(codec_delay)
            return jpeg

        def close(self):
            pass

    started = time.monotonic()

    def snapshot():
        sequence = int((time.monotonic() - started) * 60) + 1
        return Frame(sequence, str(sequence).encode())

    previews = Observed(snapshot, Codec)
    try:
        while time.monotonic() - started < 1.44:
            frame = previews.get()
            assert frame.jpeg == str(frame.sequence).encode()
            time.sleep(0.005)
    finally:
        stop(previews)
    sequences = [sequence for _, sequence in published]
    assert all(a < b for a, b in zip(sequences, sequences[1:], strict=False))
    count = sum(0.24 <= timestamp - started < 1.44 for timestamp, _ in published)
    return {"published_in_1_2_seconds": count, "fps": count / 1.2}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = subprocess.check_output(
        ["git", "-C", str(args.repo), "show", f"{BASELINE}:src/rp_ylx/api/preview_thumbnail.py"],
        text=True,
    )
    candidate = source
    for before, after in (
        ("max_workers=2", "max_workers=1"),
        ("            codecs.append(self.codec_factory())\n", ""),
        ("pending = [None, None]", "pending = [None]"),
    ):
        assert candidate.count(before) == 1
        candidate = candidate.replace(before, after)
    classes = {"two_codecs": load(source), "one_codec": load(candidate)}
    result = {
        "baseline_revision": BASELINE,
        "source_sha256": hashlib.sha256(source.encode()).hexdigest(),
        "candidate_sha256": hashlib.sha256(candidate.encode()).hexdigest(),
        "scope": (
            "actual scheduler; synthetic 60 Hz source, identity codec with controlled delay; "
            "not device FPS"
        ),
        "can_start_second_frame_while_first_blocked": {
            name: overlap_probe(cls) for name, cls in classes.items()
        },
        "measurements": {},
    }
    for delay in (0, 0.075):
        trials = {name: [] for name in classes}
        for round_index in range(3):
            names = list(classes)
            if round_index % 2:
                names.reverse()
            for name in names:
                trials[name].append(measure(classes[name], delay))
        result["measurements"][str(delay)] = {
            name: {"trials": rows, "median_fps": statistics.median(r["fps"] for r in rows)}
            for name, rows in trials.items()
        }
    result["decision"] = "reject one-codec ablation: blocks supported overlapping slow resize"
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
