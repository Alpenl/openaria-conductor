import io
import threading
import unittest

from PIL import Image, ImageDraw

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
                self.assertEqual(decoded.size, (1920, 540))
                self.assertLess(abs(decoded.getpixel((50, 50))[0] - 100), 5)
            self.assertLess(len(output), len(source.getvalue()))
            with self.assertRaises(ValueError):
                codec.resize(b"broken")
        finally:
            codec.close()

    def test_stereo_preview_retains_fine_detail_in_both_eyes(self):
        try:
            codec = ThumbnailCodec()
        except OSError:
            self.skipTest("libturbojpeg required for codec integration")
        try:
            # Two-pixel lines in the source should remain distinct at preview
            # resolution. The old quarter-size preview averages them to grey.
            image = Image.new("RGB", (3840, 1080), "black")
            draw = ImageDraw.Draw(image)
            for x in range(0, image.width, 4):
                draw.rectangle((x, 0, x + 1, image.height - 1), fill="white")
            source = io.BytesIO()
            image.save(source, format="JPEG", quality=95)
            with Image.open(io.BytesIO(codec.resize(source.getvalue()))) as decoded:
                for eye_start in (0, 960):
                    bright = decoded.getpixel((eye_start + 100, 100))[0]
                    dark = decoded.getpixel((eye_start + 101, 100))[0]
                    self.assertGreater(bright - dark, 150)
        finally:
            codec.close()

    def test_smaller_source_is_not_upscaled_or_recompressed(self):
        try:
            codec = ThumbnailCodec()
        except OSError:
            self.skipTest("libturbojpeg required for codec integration")
        try:
            source = io.BytesIO()
            Image.new("RGB", (1280, 360), (100, 50, 200)).save(source, format="JPEG")
            jpeg = source.getvalue()
            self.assertIs(codec.resize(jpeg), jpeg)
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

    def test_parallel_encoding_is_bounded_and_cannot_publish_out_of_order(self):
        first_started, second_started, newest_started = (threading.Event() for _ in range(3))
        first_release, second_release = threading.Event(), threading.Event()
        first_finished = threading.Event()
        source = [_Frame(1, b"first")]
        calls = []

        class Codec:
            def resize(self, jpeg):
                calls.append(jpeg)
                if jpeg == b"first":
                    first_started.set()
                    first_release.wait(2)
                elif jpeg == b"second":
                    second_started.set()
                    second_release.wait(2)
                else:
                    newest_started.set()
                return jpeg

            def close(self):
                pass

        class ObservedPreviews(PreviewThumbnails):
            def _encode(self, codec, frame, generation):
                super()._encode(codec, frame, generation)
                if frame.sequence == 1:
                    first_finished.set()

        previews = ObservedPreviews(lambda: source[0], Codec)
        viewer = threading.Thread(target=previews.get)
        viewer.start()
        try:
            self.assertTrue(first_started.wait(1))
            source[0] = _Frame(2, b"second")
            self.assertTrue(second_started.wait(1))
            for sequence in range(3, 100):
                source[0] = _Frame(sequence, str(sequence).encode())
            self.assertFalse(newest_started.wait(0.12))
            self.assertEqual(calls, [b"first", b"second"])
            second_release.set()
            self.assertTrue(newest_started.wait(1))
            with previews.condition:
                self.assertTrue(
                    previews.condition.wait_for(
                        lambda: previews.latest is not None and previews.latest.sequence == 99,
                        timeout=1,
                    )
                )
            first_release.set()
            self.assertTrue(first_finished.wait(1))
            self.assertEqual(previews.get().sequence, 99)
            previews.clear()
            viewer.join(1)
            with previews.condition:
                self.assertTrue(
                    previews.condition.wait_for(lambda: previews.worker is None, timeout=1)
                )
            self.assertEqual(calls, [b"first", b"second", b"99"])
            self.assertIsNone(previews.latest)
        finally:
            first_release.set()
            second_release.set()
            previews.clear()
            viewer.join(1)
