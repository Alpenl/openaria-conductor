"""Bounded, demand-driven previews; acquisition never calls the JPEG codec.

TurboJPEG performs scaled IDCT and compression in C with the GIL released.
Each viewer shares one latest-only result, with no queue of stale raw frames.
"""

from __future__ import annotations

import ctypes as c
import threading
import time
from concurrent.futures import ThreadPoolExecutor

# A 3840x1080 stereo source becomes 960x540 per eye. The former 960-wide
# stereo thumbnail left only 480x270 per eye, losing detail when displayed.
# Keep scaled IDCT and a bounded frame size instead of decoding full resolution.
PREVIEW_MAX_WIDTH = 1920
PREVIEW_JPEG_QUALITY = 85


class ThumbnailCodec:
    def __init__(self):
        self.library = library = c.CDLL("libturbojpeg.so.0")
        library.tjInitDecompress.restype = c.c_void_p
        library.tjInitCompress.restype = c.c_void_p
        library.tjDestroy.argtypes = [c.c_void_p]
        library.tjFree.argtypes = [c.c_void_p]
        library.tjDecompressHeader3.argtypes = [c.c_void_p, c.c_char_p, c.c_ulong] + [
            c.POINTER(c.c_int)
        ] * 4
        library.tjDecompress2.argtypes = [c.c_void_p, c.c_char_p, c.c_ulong, c.c_void_p] + [
            c.c_int
        ] * 5
        library.tjCompress2.argtypes = (
            [c.c_void_p, c.c_void_p]
            + [c.c_int] * 4
            + [c.POINTER(c.c_void_p), c.POINTER(c.c_ulong), c.c_int, c.c_int, c.c_int]
        )
        self.decoder = library.tjInitDecompress()
        self.encoder = library.tjInitCompress()
        self.pixels = None
        self.shape = None
        if not self.decoder or not self.encoder:
            self.close()
            raise RuntimeError("preview JPEG codec initialization failed")

    def resize(self, jpeg: bytes) -> bytes:
        library = self.library
        width, height, subsampling, color = [c.c_int() for _ in range(4)]
        if library.tjDecompressHeader3(
            self.decoder,
            jpeg,
            len(jpeg),
            c.byref(width),
            c.byref(height),
            c.byref(subsampling),
            c.byref(color),
        ):
            raise ValueError("invalid preview JPEG header")
        if not 0 < width.value <= 8192 or not 0 < height.value <= 8192:
            raise ValueError("preview JPEG dimensions exceed limit")
        if width.value <= PREVIEW_MAX_WIDTH:
            return jpeg
        # Power-of-two IDCT scaling is supported by all deployed TurboJPEG ABIs.
        divisor = 1
        while (width.value + divisor - 1) // divisor > PREVIEW_MAX_WIDTH:
            divisor *= 2
        divisor = min(divisor, 8)
        w, h = (width.value + divisor - 1) // divisor, (height.value + divisor - 1) // divisor
        if self.shape != (w, h):
            self.pixels = c.create_string_buffer(w * h * 3)
            self.shape = (w, h)
        if library.tjDecompress2(self.decoder, jpeg, len(jpeg), self.pixels, w, 0, h, 0, 2304):
            raise ValueError("preview JPEG scaled decode failed")
        output, size = c.c_void_p(), c.c_ulong()
        try:
            if library.tjCompress2(
                self.encoder,
                self.pixels,
                w,
                0,
                h,
                0,
                c.byref(output),
                c.byref(size),
                2,
                PREVIEW_JPEG_QUALITY,
                2304,
            ):
                raise ValueError("preview JPEG encode failed")
            return c.string_at(output, size.value)
        finally:
            if output:
                library.tjFree(output)

    def close(self):
        for name in ("decoder", "encoder"):
            handle = getattr(self, name, None)
            if handle:
                self.library.tjDestroy(handle)
                setattr(self, name, None)


class PreviewThumbnails:
    def __init__(self, snapshot, codec_factory=ThumbnailCodec):
        self.snapshot = snapshot
        self.codec_factory = codec_factory
        self.condition = threading.Condition()
        self.worker = None
        self.latest = None
        self.requested = 0.0
        self.generation = 0

    def get(self):
        with self.condition:
            self.requested = time.monotonic()
            if self.worker is None:
                self.worker = threading.Thread(
                    target=self._run, name="preview-thumbnail", daemon=True
                )
                self.worker.start()
            # Only the first request waits for one thumbnail; capture is independent.
            self.condition.wait_for(
                lambda: self.latest is not None or self.worker is None, timeout=0.25
            )
            if self.latest is None:
                raise RuntimeError("preview thumbnail is not available")
            return self.latest

    def clear(self):
        with self.condition:
            self.generation += 1
            self.latest = None
            self.requested = 0.0
            self.condition.notify_all()

    def _run(self):
        codecs = []
        previous = None
        executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="preview-codec")
        try:
            # HD scaling takes longer than a 40 ms frame period on RDK X5.
            # Two independent codecs overlap work, with at most one frame per
            # codec in flight. There is never a queue of waiting camera frames.
            codecs = [self.codec_factory()]
            codecs.append(self.codec_factory())
            pending = [None, None]
            while True:
                started = time.monotonic()
                with self.condition:
                    if started - self.requested > 2:
                        return
                    generation = self.generation
                for slot, future in enumerate(pending):
                    if future is not None:
                        if not future.done():
                            continue
                        future.result()
                    frame = self.snapshot()
                    if frame.sequence != previous:
                        pending[slot] = executor.submit(
                            self._encode, codecs[slot], frame, generation
                        )
                        previous = frame.sequence
                    break
                deadline = started + 1 / 25
                with self.condition:
                    # Completion notifications wake initial viewers, but must
                    # not cause the producer to exceed its shared 25 FPS cap.
                    self.condition.wait_for(
                        lambda generation=generation: generation != self.generation,
                        timeout=max(0.001, deadline - time.monotonic()),
                    )
        except Exception:
            # Unavailable camera / codec is retried on the next viewer request.
            pass
        finally:
            executor.shutdown(wait=True)
            for codec in codecs:
                codec.close()
            with self.condition:
                self.worker = None
                self.latest = None
                self.condition.notify_all()

    def _encode(self, codec, frame, generation):
        jpeg = codec.resize(frame.jpeg)
        with self.condition:
            if generation == self.generation and (
                self.latest is None or frame.sequence > self.latest.sequence
            ):
                self.latest = type(frame)(frame.sequence, jpeg)
                self.condition.notify_all()
