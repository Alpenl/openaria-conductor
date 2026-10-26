"""Real child-process failure modes; source audio must survive failed readback."""

import os
import signal
import subprocess
import sys
import tempfile
import unittest

CHILD = r"""
import hashlib
import subprocess
import sys
import wave
from pathlib import Path
from unittest.mock import patch
from rp_ylx.recording import storage

mode, directory = sys.argv[1:]
path = Path(directory) / 'source.wav'
with wave.open(str(path), 'wb') as stream:
    stream.setnchannels(1)
    stream.setsampwidth(2)
    stream.setframerate(48000)
    stream.writeframes(b'\0' * 16)
original = path.read_bytes()
real_popen = subprocess.Popen
class ControlledPopen(real_popen):
    def wait(self, timeout=None):
        # Exercise the old explicit 120s wait quickly; __exit__ still waits unbounded.
        return super().wait(timeout=.15 if timeout == 120 else timeout)
children = []
programs = {
    'silent': 'import time; time.sleep(30)',
    'trickle': 'import os,time\nwhile True: os.write(1,b"x"); time.sleep(.05)',
    'eof_stall': 'import os,time; os.close(1); time.sleep(30)',
    'excess': 'import os,time; os.write(1,b"x"*100); time.sleep(30)',
    'failure': 'import sys; sys.exit(1)',
    'success': 'import os; os.write(1,bytes(16))',
}
def encode(command, **kwargs):
    Path(command[-1]).write_bytes(b'test-only FLAC substitute')
def decode(command, **kwargs):
    process = ControlledPopen([sys.executable, '-c', programs[mode]], **kwargs)
    children.append(process)
    return process
try:
    with patch.object(storage, 'AUDIO_CODEC_TIMEOUT_SECONDS', .15, create=True), \
         patch.object(storage.subprocess, 'run', encode), \
         patch.object(storage.subprocess, 'Popen', decode):
        error = None
        try:
            storage.compact_audio(path)
        except Exception as caught:
            error = caught
        if mode in ('silent','trickle','eof_stall'):
            assert isinstance(error, subprocess.TimeoutExpired), repr(error)
        elif mode in ('excess','failure'):
            assert isinstance(error, ValueError), repr(error)
        else:
            assert error is None, repr(error)
        assert path.read_bytes() == original
        assert path.with_suffix('.flac').exists() == (mode == 'success')
        assert children and all(p.poll() is not None for p in children), 'decoder not reaped'
finally:
    for process in children:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=2)
"""


class AudioReadbackDeadlineTests(unittest.TestCase):
    def execute(self, mode):
        with tempfile.TemporaryDirectory() as directory:
            process = subprocess.Popen(
                [sys.executable, "-c", CHILD, mode, directory],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
            try:
                stdout, stderr = process.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.communicate(timeout=2)
                self.fail(f"{mode}: audio readback exceeded its total deadline")
            self.assertEqual(process.returncode, 0, (stdout, stderr))

    def test_no_output_is_bounded_and_decoder_reaped(self):
        self.execute("silent")

    def test_trickle_output_does_not_reset_deadline(self):
        self.execute("trickle")

    def test_eof_without_process_exit_is_bounded(self):
        self.execute("eof_stall")

    def test_excess_pcm_is_rejected_and_source_retained(self):
        self.execute("excess")

    def test_decoder_failure_keeps_source(self):
        self.execute("failure")

    def test_exact_pcm_can_be_published(self):
        self.execute("success")
