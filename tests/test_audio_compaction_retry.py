"""Failed audio compaction must preserve evidence and leave a usable retry."""

import errno
import hashlib
import shutil
import subprocess
import tempfile
import threading
import unittest
import wave
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from rp_ylx.recording import storage


class AudioCompactionRetryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "source.wav"
        self.pcm = bytes(range(256)) * 40
        with wave.open(str(self.source), "wb") as output:
            output.setparams((1, 2, 48000, 0, "NONE", "not compressed"))
            output.writeframes(self.pcm)
        self.original = self.source.read_bytes()
        self.target = self.source.with_suffix(".flac")
        self.target.write_bytes(b"previous verified output")
        self.previous = self.target.read_bytes()
        self.names = set(self.root.iterdir())
        self.proof = (len(self.pcm), hashlib.sha256(self.pcm).hexdigest())

    def encode(self, command, **kwargs):
        Path(command[-1]).write_bytes(b"test encoded data")

    def assert_preserved(self):
        self.assertEqual(self.source.read_bytes(), self.original)
        self.assertEqual(self.target.read_bytes(), self.previous)
        self.assertEqual(set(self.root.iterdir()), self.names)

    def test_encoder_failure_timeout_and_cancellation_remove_only_owned_temporary(self):
        failures = (
            subprocess.CalledProcessError(1, ["ffmpeg"]),
            subprocess.TimeoutExpired(["ffmpeg"], 0.1),
            KeyboardInterrupt(),
        )
        for error in failures:
            with self.subTest(error=type(error).__name__):

                def fail(command, error=error, **kwargs):
                    self.encode(command, **kwargs)
                    raise error

                with (
                    patch.object(storage.subprocess, "run", side_effect=fail),
                    self.assertRaises(type(error)),
                ):
                    storage.compact_audio(self.source)
                self.assert_preserved()

    def test_failed_or_mismatched_readback_preserves_source_and_published_target(self):
        for result in (
            ValueError("bad decode"),
            subprocess.TimeoutExpired(["decode"], 0.1),
            (0, "bad"),
        ):
            with self.subTest(result=repr(result)):
                kwargs = (
                    {"side_effect": result}
                    if isinstance(result, Exception)
                    else {"return_value": result}
                )
                with (
                    patch.object(storage.subprocess, "run", side_effect=self.encode),
                    patch.object(storage, "_pcm_readback", **kwargs),
                    self.assertRaises((ValueError, subprocess.TimeoutExpired)),
                ):
                    storage.compact_audio(self.source)
                self.assert_preserved()

    def test_fsync_failure_and_publish_failure_leave_no_retry_barrier(self):
        for operation in ("fsync", "replace"):
            with self.subTest(operation=operation):
                with (
                    patch.object(storage.subprocess, "run", side_effect=self.encode),
                    patch.object(storage, "_pcm_readback", return_value=self.proof),
                    patch.object(
                        storage.os, operation, side_effect=OSError(errno.ENOSPC, "disk full")
                    ),
                    self.assertRaises(OSError),
                ):
                    storage.compact_audio(self.source)
                self.assert_preserved()
                with (
                    patch.object(storage.subprocess, "run", side_effect=self.encode),
                    patch.object(storage, "_pcm_readback", return_value=self.proof),
                ):
                    target, compression = storage.compact_audio(self.source)
                self.assertEqual(target, self.target)
                self.assertEqual(compression["pcm_bytes"], len(self.pcm))
                self.assertEqual(set(self.root.iterdir()), self.names)
                self.target.write_bytes(self.previous)

    @unittest.skipUnless(shutil.which("ffmpeg"), "FLAC integration requires ffmpeg")
    def test_real_codec_retries_after_enospc_without_losing_source(self):
        with (
            patch.object(storage.os, "fsync", side_effect=OSError(errno.ENOSPC, "disk full")),
            self.assertRaises(OSError),
        ):
            storage.compact_audio(self.source)
        self.assert_preserved()
        target, compression = storage.compact_audio(self.source)
        decoded = subprocess.check_output(
            ["ffmpeg", "-nostdin", "-v", "error", "-i", str(target), "-f", "s16le", "-"]
        )
        self.assertEqual(decoded, self.pcm)
        self.assertEqual(compression["pcm_sha256"], self.proof[1])
        self.assertEqual(self.source.read_bytes(), self.original)
        self.assertEqual(set(self.root.iterdir()), self.names)

    @unittest.skipUnless(shutil.which("ffmpeg"), "FLAC integration requires ffmpeg")
    def test_stale_legacy_temporary_does_not_block_or_get_deleted_by_retry(self):
        stale = self.target.with_suffix(".flac.tmp")
        stale.write_bytes(b"previous interrupted attempt")
        storage.compact_audio(self.source)
        self.assertEqual(stale.read_bytes(), b"previous interrupted attempt")
        self.assertEqual(self.source.read_bytes(), self.original)
        self.assertEqual(set(self.root.iterdir()), self.names | {stale})

    def test_truncated_wav_is_rejected_before_encoding(self):
        for missing in (1, 2, 200):
            with self.subTest(missing=missing):
                self.source.write_bytes(self.original[:-missing])
                with (
                    patch.object(storage.subprocess, "run") as encode,
                    self.assertRaisesRegex(ValueError, "truncated WAV"),
                ):
                    storage.compact_audio(self.source)
                encode.assert_not_called()
                self.assertEqual(self.source.read_bytes(), self.original[:-missing])
                self.assertEqual(self.target.read_bytes(), self.previous)
                self.assertEqual(set(self.root.iterdir()), self.names)

    def test_concurrent_failed_attempt_does_not_remove_other_attempt_output(self):
        barrier = threading.Barrier(2)
        paths = []
        lock = threading.Lock()

        def encode(command, **kwargs):
            path = Path(command[-1])
            with lock:
                attempt = len(paths)
                paths.append(path)
            path.write_bytes(b"failed" if attempt == 0 else b"winner")
            barrier.wait(timeout=3)
            if attempt == 0:
                raise OSError(errno.ENOSPC, "first attempt failed")

        def invoke():
            try:
                return storage.compact_audio(self.source)
            except OSError:
                return None

        with (
            patch.object(storage.subprocess, "run", side_effect=encode),
            patch.object(storage, "_pcm_readback", return_value=self.proof),
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            futures = [executor.submit(invoke) for _ in range(2)]
            results = [future.result(timeout=5) for future in futures]
        self.assertEqual(sum(value is not None for value in results), 1)
        self.assertNotEqual(paths[0], paths[1])
        self.assertEqual(self.target.read_bytes(), b"winner")
        self.assertEqual(self.source.read_bytes(), self.original)
        self.assertEqual(set(self.root.iterdir()), self.names)

    @unittest.skipUnless(shutil.which("ffmpeg"), "FLAC integration requires ffmpeg")
    def test_stereo_pcm_across_read_blocks_round_trips_exactly(self):
        source = self.root / "双声道 source.wav"
        pcm = bytes(range(256)) * (storage.BLOCK_BYTES // 256 + 3)
        with wave.open(str(source), "wb") as stream:
            stream.setparams((2, 2, 48000, 0, "NONE", "not compressed"))
            stream.writeframes(pcm)
        original = source.read_bytes()
        target, proof = storage.compact_audio(source)
        decoded = subprocess.check_output(
            ["ffmpeg", "-nostdin", "-v", "error", "-i", str(target), "-f", "s16le", "-"]
        )
        self.assertEqual(decoded, pcm)
        self.assertEqual(proof["pcm_bytes"], len(pcm))
        self.assertEqual(proof["pcm_sha256"], hashlib.sha256(pcm).hexdigest())
        self.assertEqual(source.read_bytes(), original)
        self.assertEqual(set(self.root.iterdir()), self.names | {source, target})

    def test_nondurable_preparation_still_verifies_without_fsync(self):
        with (
            patch.object(storage.subprocess, "run", side_effect=self.encode),
            patch.object(storage, "_pcm_readback", return_value=self.proof) as verify,
            patch.object(storage.os, "fsync") as sync,
        ):
            target, _ = storage.compact_audio(self.source, durable=False)
        verify.assert_called_once()
        sync.assert_not_called()
        self.assertEqual(target, self.target)
        self.assertEqual(self.source.read_bytes(), self.original)
        self.assertEqual(set(self.root.iterdir()), self.names)


if __name__ == "__main__":
    unittest.main()
