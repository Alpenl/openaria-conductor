"""Read the fsynced prefix named by an atomic audio checkpoint v3 header.

The native writer only appends. Live compaction consumes each committed byte
once; recovery creates a fresh reader and verifies the entire prefix. Bytes
past the published boundary are never evidence of durable audio.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path

from rp_ylx.recording.verified import identity, open_regular

SCHEMA = "openaria.audio-checkpoint.v3"
MAX_JOURNAL_BYTES = 256 * 1024 * 1024
MAX_ROW_BYTES = 16 * 1024 * 1024


def _integer(value, name, *, positive=False):
    if type(value) is not int or value < int(positive) or value > 2**64 - 1:
        raise ValueError(f"invalid audio journal {name}")
    return value


def _anchor(value):
    if not isinstance(value, list) or len(value) != 5:
        raise ValueError("invalid audio journal anchor")
    for item in value:
        _integer(item, "anchor scalar")
    return value


class AudioJournalReader:
    def __init__(self, root: Path):
        self.root = root
        self.offset = 0
        self.digest = hashlib.sha256()
        self.sequence = 0
        self.anchor_count = 0
        self.segment_count = 0
        self.sample_count = 0
        self.last_anchor = None
        self.file_id = None

    def read(self, head: dict) -> dict:
        if head.get("schema") != SCHEMA:
            raise ValueError("unsupported audio journal header")
        end = _integer(head.get("journal_bytes"), "length", positive=True)
        if not self.offset <= end <= MAX_JOURNAL_BYTES:
            raise ValueError("audio journal boundary regressed or exceeds limit")
        expected_hash = head.get("journal_sha256")
        if (
            not isinstance(expected_hash, str)
            or re.fullmatch("[0-9a-f]{64}", expected_hash) is None
        ):
            raise ValueError("invalid audio journal digest")
        for key in (
            "journal_records",
            "anchor_count",
            "segment_count",
            "sample_count",
            "thread_started_monotonic_ns",
            "period_frames",
            "buffer_frames",
            "queue_capacity_frames",
            "queue_peak_frames",
            "max_write_ns",
        ):
            _integer(head.get(key), key)
        if self.root.is_symlink() or (self.root / "audio").is_symlink():
            raise ValueError("unsafe audio journal directory")
        path = self.root / "audio/checkpoint.ndjson"
        digest = self.digest.copy()
        sequence, anchor_count, segment_count = self.sequence, self.anchor_count, self.segment_count
        sample_count, previous = self.sample_count, self.last_anchor
        anchors, segments = [], []
        with open_regular(path) as source:
            before = identity(os.fstat(source.fileno()))
            if before[4] < end or (self.file_id is not None and before[:2] != self.file_id):
                raise ValueError("audio journal replaced or truncated")
            source.seek(self.offset)
            remaining = end - self.offset
            while remaining:
                raw = source.readline(min(remaining, MAX_ROW_BYTES) + 1)
                if not raw.endswith(b"\n") or len(raw) > min(remaining, MAX_ROW_BYTES):
                    raise ValueError("incomplete or oversized committed audio journal row")
                remaining -= len(raw)
                digest.update(raw)
                row = json.loads(raw)
                if not isinstance(row, dict) or set(row) != {"sequence", "anchors", "segments"}:
                    raise ValueError("invalid audio journal row")
                if _integer(row["sequence"], "sequence") != sequence:
                    raise ValueError("noncontiguous audio journal sequence")
                sequence += 1
                if not isinstance(row["anchors"], list) or not isinstance(row["segments"], list):
                    raise ValueError("invalid audio journal arrays")
                for point in row["anchors"]:
                    point = _anchor(point)
                    if previous is not None and (
                        point[0] <= previous[0] or point[1] <= previous[1]
                    ):
                        raise ValueError("nonmonotonic audio journal anchors")
                    previous = point
                    anchors.append(point)
                    anchor_count += 1
                for segment in row["segments"]:
                    if not isinstance(segment, dict) or set(segment) != {
                        "index",
                        "path",
                        "start_sample",
                        "end_sample",
                    }:
                        raise ValueError("invalid audio journal segment")
                    index = _integer(segment["index"], "segment index")
                    start = _integer(segment["start_sample"], "segment start")
                    stop = _integer(segment["end_sample"], "segment end")
                    if (
                        index != segment_count
                        or start != sample_count
                        or stop <= start
                        or segment["path"] != f"audio/audio_{index:05d}.wav"
                    ):
                        raise ValueError("noncontiguous or unsafe audio journal segment")
                    segments.append(segment)
                    segment_count += 1
                    sample_count = stop
            after = identity(os.fstat(source.fileno()))
            named = identity(path.stat(follow_symlinks=False))
            if before[:2] != after[:2] or after[:2] != named[:2] or min(after[4], named[4]) < end:
                raise ValueError("audio journal replaced or truncated during read")
        if (
            digest.hexdigest() != expected_hash
            or sequence != head["journal_records"]
            or anchor_count != head["anchor_count"]
            or segment_count != head["segment_count"]
            or sample_count != head["sample_count"]
        ):
            raise ValueError("audio journal committed prefix verification failed")
        last = head.get("last_anchor")
        if last is not None:
            last = _anchor(last)
            if (
                previous is not None
                and last != previous
                and (last[0] <= previous[0] or last[1] <= previous[1])
            ):
                raise ValueError("audio journal final anchor regressed")
            if last != previous:
                anchors.append(last)
        # Commit reader state only after the complete published prefix verifies.
        self.offset, self.digest, self.sequence = end, digest, sequence
        self.anchor_count, self.segment_count = anchor_count, segment_count
        self.sample_count, self.last_anchor, self.file_id = sample_count, previous, before[:2]
        return {
            **head,
            "schema": "openaria.audio-checkpoint.v2",
            "anchors": anchors,
            "segments": segments,
        }
