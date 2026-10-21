"""Normal stop signals native audio before fallible checkpoint I/O."""

import errno
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rp_ylx.recording.device_session import DeviceRecordingError
from tests import test_split_eye_recording as fixture


class AudioStopSignalTest(unittest.TestCase):
    def test_checkpoint_cannot_delay_capture_stop_or_bypass_failure_cleanup(self):
        for checkpoint_fails in (False, True):
            with (
                self.subTest(checkpoint_fails=checkpoint_fails),
                tempfile.TemporaryDirectory() as directory,
            ):
                store = fixture.FakeSessionStore()
                with (
                    patch(
                        "rp_ylx.recording.device_session.native_session_store", return_value=store
                    ),
                    patch(
                        "rp_ylx.recording.device_session.resolve_executable",
                        return_value=Path("/bin/true"),
                    ),
                ):
                    recorder, _, _ = fixture.SplitEyeRecordingTest().build(
                        Path(directory), native_data_plane=True
                    )
                    recorder.start()
                    transaction = store.transaction
                    transaction.advance(frames=4, imu_samples=2)
                    persist = recorder._persist_state
                    observed = []

                    def checkpoint(
                        state,
                        *,
                        transaction=transaction,
                        observed=observed,
                        persist=persist,
                        checkpoint_fails=checkpoint_fails,
                        **kwargs,
                    ):
                        if state == "finalizing":
                            observed.append(transaction.audio_stop_requested)
                            self.assertTrue(transaction.audio_stop_requested)
                            self.assertFalse(transaction.finished)
                            self.assertFalse(transaction.aborted)
                            if checkpoint_fails:
                                raise OSError(errno.ENOSPC, "checkpoint full")
                        return persist(state, **kwargs)

                    try:
                        with patch.object(recorder, "_persist_state", side_effect=checkpoint):
                            if checkpoint_fails:
                                with self.assertRaises(DeviceRecordingError):
                                    recorder.stop()
                                self.assertTrue(transaction.aborted)
                                self.assertFalse(recorder._final.exists())
                            else:
                                sealed = recorder.stop()
                                self.assertEqual(sealed.manifest["frames"]["count"], 4)
                                self.assertTrue(transaction.finished)
                                self.assertTrue(transaction.sealed)
                        self.assertEqual(observed, [True])
                    finally:
                        if recorder.state in {"recording", "finalizing"}:
                            recorder.abort()


if __name__ == "__main__":
    unittest.main()
