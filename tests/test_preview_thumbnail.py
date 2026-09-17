import io
import threading
import unittest

from PIL import Image

from rp_ylx.api.preview import _Frame
from rp_ylx.api.preview_thumbnail import PreviewThumbnails, ThumbnailCodec


class PreviewThumbnailTests(unittest.TestCase):
    def test_real_jpeg_scaling_preserves_aspect_and_rejects_invalid_payload(self):
        try:
            codec = ThumbnailCodec()
        except OSError:
            self.skipTest("libturbojpeg required for codec integration")
        try:
            source = io.BytesIO()
            Image.new("RGB", (3840, 1080), (100, 50, 200)).save(source, format="JPEG")
            output = codec.resize(source.getvalue())
            with Image.open(io.BytesIO(output)) as decoded:
                self.assertEqual(decoded.size, (960, 270))
                self.assertLess(abs(decoded.getpixel((50, 50))[0] - 100), 5)
            self.assertLess(len(output), len(source.getvalue()))
            with self.assertRaises(ValueError):
                codec.resize(b"broken")
        finally:
            codec.close()

    def test_slow_codec_skips_stale_frames_and_clear_rejects_inflight_result(self):
        started, release, closed = threading.Event(), threading.Event(), threading.Event()
        source = [_Frame(1, b"first")]
        calls = []

        class Codec:
            def resize(self, jpeg):
                calls.append(jpeg)
                started.set()
                release.wait(2)
                return jpeg

            def close(self):
                closed.set()

        previews = PreviewThumbnails(lambda: source[0], Codec)
        errors = []

        def viewer():
            try:
                previews.get()
            except RuntimeError as error:
                errors.append(error)

        request = threading.Thread(target=viewer)
        request.start()
        self.assertTrue(started.wait(1))
        for index in range(2, 100):
            source[0] = _Frame(index, str(index).encode())
        previews.clear()
        release.set()
        request.join(1)
        self.assertTrue(closed.wait(1))
        self.assertEqual(calls, [b"first"])
        self.assertIsNone(previews.latest)
        self.assertEqual(len(errors), 1)

    def test_viewers_share_a_single_encoded_result(self):
        calls = []

        class Codec:
            def resize(self, jpeg):
                calls.append(jpeg)
                return b"small"

            def close(self):
                pass

        previews = PreviewThumbnails(lambda: _Frame(1, b"raw"), Codec)
        try:
            for _ in range(20):
                self.assertEqual(previews.get().jpeg, b"small")
            self.assertEqual(calls, [b"raw"])
        finally:
            previews.clear()
