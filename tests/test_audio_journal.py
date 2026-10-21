import copy
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.recording.audio_journal import AudioJournalReader
from tests import test_live_sealing as live_fixtures


def publish(root, rows, *, last=None):
    payload = b"".join(json.dumps(row).encode() + b"\n" for row in rows)
    segments = [s for row in rows for s in row["segments"]]
    anchors = [a for row in rows for a in row["anchors"]]
    head = {
        "schema": "openaria.audio-checkpoint.v3",
        "journal_bytes": len(payload),
        "journal_sha256": hashlib.sha256(payload).hexdigest(),
        "journal_records": len(rows),
        "anchor_count": len(anchors),
        "segment_count": len(segments),
        "sample_count": segments[-1]["end_sample"] if segments else 0,
        "last_anchor": last,
        "thread_started_monotonic_ns": 100,
        "period_frames": 1024,
        "buffer_frames": 8192,
        "queue_capacity_frames": 786432,
        "queue_peak_frames": 1024,
        "max_write_ns": 5,
    }
    audio = root / "audio"
    audio.mkdir(exist_ok=True)
    path = audio / "checkpoint.ndjson"
    # Model the actual append-only writer, not inode replacement.
    prior = path.read_bytes() if path.exists() else b""
    if not payload.startswith(prior):
        raise ValueError("fixture cannot rewrite journal history")
    with path.open("ab") as f:
        f.write(payload[len(prior) :])
    temporary = audio / "checkpoint.tmp"
    temporary.write_text(json.dumps(head))
    temporary.replace(audio / "checkpoint.json")
    return head


def row(index):
    n = index * 10
    return {
        "sequence": index,
        "anchors": [[n, 100 + n, 100 + n, 100 + n, 100 + n]],
        "segments": [
            {
                "index": index,
                "path": f"audio/audio_{index:05d}.wav",
                "start_sample": n,
                "end_sample": n + 10,
            }
        ],
    }


class AudioJournalTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.head = publish(self.root, [row(0)])
        self.path = self.root / "audio/checkpoint.ndjson"

    def test_unpublished_complete_and_partial_tail_is_ignored(self):
        with self.path.open("ab") as f:
            f.write(json.dumps(row(1)).encode() + b'\n{"incomplete":')
        value = AudioJournalReader(self.root).read(self.head)
        self.assertEqual(value["segments"], row(0)["segments"])
        self.assertEqual(value["sample_count"], 10)

    def test_incremental_reads_only_new_rows_and_last_anchor_is_not_accumulated(self):
        reader = AudioJournalReader(self.root)
        first = reader.read(self.head)
        self.assertEqual(first["anchors"], row(0)["anchors"])
        head = publish(self.root, [row(0), row(1)], last=[11, 111, 111, 111, 111])
        second = reader.read(head)
        self.assertEqual(second["segments"], row(1)["segments"])
        self.assertEqual(second["anchors"], row(1)["anchors"] + [[11, 111, 111, 111, 111]])
        self.assertEqual(reader.read(head)["segments"], [])
        full = AudioJournalReader(self.root).read(head)
        self.assertEqual(full["segments"], first["segments"] + second["segments"])
        with self.assertRaisesRegex(ValueError, "regressed"):
            reader.read(self.head)

    def repeated_point(self):
        first = [4800, 1000000000, 2000000100, 1000000050, 1000000150]
        later = [4800, 1000000000, 2000100100, 1000100050, 1000100150]
        self.path.unlink()
        value = row(0)
        value["anchors"] = [first]
        head = publish(self.root, [value], last=later)
        return value, head

    def test_reobserved_same_sample_and_raw_time_is_one_point(self):
        value, head = self.repeated_point()
        reader = AudioJournalReader(self.root)
        decoded = reader.read(head)
        self.assertEqual(decoded["anchors"], value["anchors"])
        self.assertEqual(reader.read(head)["anchors"], [])
        self.assertEqual(reader.anchor_count, 1)

    def test_reobserved_header_after_incremental_read_keeps_original_point(self):
        value, head = self.repeated_point()
        first_head = {**head, "last_anchor": value["anchors"][0]}
        reader = AudioJournalReader(self.root)
        reader.read(first_head)
        decoded = reader.read(head)
        self.assertEqual(decoded["anchors"], [])
        self.assertEqual(reader.last_anchor, value["anchors"][0])
        second = row(1)
        second["anchors"] = [[9600, 1100000000, 2100000100, 1100000050, 1100000150]]
        end = publish(self.root, [value, second], last=second["anchors"][0])
        self.assertEqual(reader.read(end)["anchors"], second["anchors"])
        self.assertEqual(
            AudioJournalReader(self.root).read(end)["anchors"], value["anchors"] + second["anchors"]
        )

    def test_reobserved_point_requires_valid_and_forward_host_bridge(self):
        value, head = self.repeated_point()
        invalid = [
            [4800, 1000000000, 2000100100, 999999999, 1000100150],
            [4800, 1000000000, 2000100100, 1000100151, 1000100150],
            [4800, 1000000000, 2000100100, 1000100050, 1002100051],
            [4800, 1000000000, 2030100100, 1030100050, 1030100150],
            [4800, 1000000000, 0, 1000100050, 1000100150],
            [4800, 1000000000, 2000000000, 1000000000, 1000000001],
            [4800, 1000000000, 3000100100, 1000100050, 1000100150],
        ]
        for last in invalid:
            with self.subTest(last=last):
                reader = AudioJournalReader(self.root)
                with self.assertRaises(ValueError):
                    reader.read({**head, "last_anchor": last})
                self.assertEqual(reader.offset, 0)
                self.assertEqual(
                    reader.read({**head, "last_anchor": value["anchors"][0]})["anchors"],
                    value["anchors"],
                )

    def test_live_sealing_accepts_reobserved_point_and_final_reverification(self):
        value, head = self.repeated_point()
        fixture = live_fixtures.LiveSealingTests()
        sealing = fixture.audio_sealing(self.root)
        (self.root / "audio/audio_00000.wav").write_bytes(b"fixture")
        with patch(
            "rp_ylx.recording.live_seal.compact_audio", side_effect=fixture.compact_audio
        ) as compact:
            sealing._audio()
            sealing._audio(force=True)
        self.assertEqual(compact.call_count, 1)
        self.assertEqual(set(sealing.prepared), {"audio/audio_00000.wav"})
        self.assertEqual(sealing._audio_journal.last_anchor, value["anchors"][0])

    def test_same_sample_or_raw_time_alone_still_rejects(self):
        value, head = self.repeated_point()
        for sample, raw in [
            (4799, 1000000000),
            (4800, 999999999),
            (4800, 1000000001),
            (4801, 1000000000),
        ]:
            with self.subTest(sample=sample, raw=raw):
                last = [sample, raw, *head["last_anchor"][2:]]
                with self.assertRaisesRegex(ValueError, "regressed"):
                    AudioJournalReader(self.root).read({**head, "last_anchor": last})

    def test_distinct_forward_final_point_is_retained(self):
        value, head = self.repeated_point()
        last = [4801, 1000001000, 2000100100, 1000100050, 1000100150]
        result = AudioJournalReader(self.root).read({**head, "last_anchor": last})
        self.assertEqual(result["anchors"], value["anchors"] + [last])

    def test_replaced_inode_is_rejected_even_with_identical_prefix(self):
        reader = AudioJournalReader(self.root)
        reader.read(self.head)
        replacement = self.path.with_suffix(".replacement")
        replacement.write_bytes(self.path.read_bytes())
        replacement.replace(self.path)
        with self.assertRaisesRegex(ValueError, "replaced"):
            reader.read(self.head)

    def test_corruption_and_truncation_do_not_advance_reader(self):
        for kind in ("hash", "truncate", "row", "counts"):
            with self.subTest(kind=kind):
                reader = AudioJournalReader(self.root)
                original = self.path.read_bytes()
                head = copy.deepcopy(self.head)
                if kind == "hash":
                    head["journal_sha256"] = "0" * 64
                if kind == "truncate":
                    self.path.write_bytes(original[:-1])
                if kind == "row":
                    head["journal_bytes"] -= 1
                if kind == "counts":
                    head["anchor_count"] += 1
                with self.assertRaises(ValueError):
                    reader.read(head)
                self.assertEqual(reader.offset, 0)
                self.path.write_bytes(original)
                self.assertEqual(reader.read(self.head)["sample_count"], 10)

    def test_unsafe_or_noncontiguous_rows_rejected_even_with_valid_hash(self):
        for field, value in (
            ("path", "../outside.wav"),
            ("index", 1),
            ("start_sample", 1),
            ("end_sample", 0),
        ):
            with self.subTest(field=field):
                self.path.unlink()
                bad = row(0)
                bad["segments"][0][field] = value
                head = publish(self.root, [bad])
                with self.assertRaises(ValueError):
                    AudioJournalReader(self.root).read(head)

    def test_symlink_hardlink_and_fifo_are_rejected_without_blocking(self):
        payload = self.path.read_bytes()
        for kind in ("symlink", "hardlink", "fifo"):
            with self.subTest(kind=kind):
                self.path.unlink()
                target = self.root / kind
                target.write_bytes(payload)
                if kind == "symlink":
                    self.path.symlink_to(target)
                elif kind == "hardlink":
                    os.link(target, self.path)
                else:
                    os.mkfifo(self.path)
                with self.assertRaises((ValueError, OSError)):
                    AudioJournalReader(self.root).read(self.head)

    def test_live_compaction_reads_deltas_and_final_pass_reverifies_prefix(self):
        fixture = live_fixtures.LiveSealingTests()
        sealing = fixture.audio_sealing(self.root)
        for index in range(2):
            (self.root / f"audio/audio_{index:05d}.wav").write_bytes(b"fixture")
        with patch(
            "rp_ylx.recording.live_seal.compact_audio", side_effect=fixture.compact_audio
        ) as compact:
            sealing._audio()
            publish(self.root, [row(0), row(1)])
            sealing._audio()
            self.assertEqual(compact.call_count, 2)
            self.assertEqual(len(sealing.prepared), 2)
            sealing._audio(force=True)
            self.assertEqual(compact.call_count, 2)
            payload = self.path.read_bytes().replace(b'"sequence": 0', b'"sequence": 9', 1)
            self.path.write_bytes(payload)
            with self.assertRaises(ValueError):
                sealing._audio(force=True)
