"""Bounded, demand-driven previews; acquisition never calls the JPEG codec.

Native camera JPEGs pass through without decoding or recompression. Oversized
sources use TurboJPEG scaled IDCT and compression with the GIL released.
Each viewer shares one latest-only result, with no queue of stale raw frames.
"""

from __future__ import annotations

import ctypes as c
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress

from rp_ylx.operational_logging import operational_logger

# Preserve the camera's 3840x1080 stereo JPEG: each eye keeps all 1920x1080
# pixels, with no extra compression loss or codec cost on the normal path.
# Keep a bounded scaled-IDCT fallback for larger sources.
PREVIEW_MAX_WIDTH = 3840
PREVIEW_MAX_HEIGHT = 8192
PREVIEW_JPEG_QUALITY = 85
_SOF_MARKERS = frozenset(
    {
        0xC0,
        0xC1,
        0xC2,
        0xC3,
        0xC5,
        0xC6,
        0xC7,
        0xC9,
        0xCA,
        0xCB,
        0xCD,
        0xCE,
        0xCF,
    }
)


def _jpeg_dimensions(jpeg: bytes) -> tuple[int, int] | None:
    """Parse JPEG width and height from SOF headers without calling libturbojpeg."""
    length = len(jpeg)
    if length < 4 or jpeg[:2] != b"\xff\xd8":
        return None
    offset = 2
    while offset < length:
        while offset < length and jpeg[offset] != 0xFF:
            offset += 1
        while offset < length and jpeg[offset] == 0xFF:
            offset += 1
        if offset >= length:
            return None
        marker = jpeg[offset]
        offset += 1
        if marker in (0x00, 0x01, 0xD8) or 0xD0 <= marker <= 0xD7:
            continue
        if marker in (0xD9, 0xDA) or offset + 2 > length:
            return None
        segment_len = int.from_bytes(jpeg[offset : offset + 2], "big")
        if segment_len < 2 or offset + segment_len > length:
            return None
        if marker in _SOF_MARKERS:
            if segment_len < 7:
                return None
            height = int.from_bytes(jpeg[offset + 3 : offset + 5], "big")
            width = int.from_bytes(jpeg[offset + 5 : offset + 7], "big")
            return (width, height)
        offset += segment_len
    return None


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
            if self.latest is not None and self.worker is not None:
                return self.latest
            need_probe = self.worker is None and self.latest is None
        if need_probe:
            # Probe snapshot before spawning any worker, executor, or JPEG codec.
            try:
                self.snapshot()
            except Exception as error:
                raise RuntimeError("preview thumbnail is not available") from error
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

    def _needs_codec(self, frame) -> bool:
        if self.codec_factory is not ThumbnailCodec:
            return True
        dims = _jpeg_dimensions(frame.jpeg)
        if dims is None:
            return True
        width, height = dims
        return not (0 < width <= PREVIEW_MAX_WIDTH and 0 < height <= PREVIEW_MAX_HEIGHT)

    def _publish_frame(self, frame, jpeg: bytes, generation: int) -> None:
        with self.condition:
            if generation == self.generation and (
                self.latest is None or frame.sequence > self.latest.sequence
            ):
                self.latest = type(frame)(frame.sequence, jpeg)
                self.condition.notify_all()

    def _run(self):
        codecs = []
        previous = None
        executor = None
        pending = [None, None]
        try:
            while True:
                started = time.monotonic()
                with self.condition:
                    if started - self.requested > 2:
                        if self.worker is threading.current_thread():
                            self.worker = None
                            self.latest = None
                            self.condition.notify_all()
                        return
                    generation = self.generation
                frame = self.snapshot()
                if frame.sequence != previous:
                    if not self._needs_codec(frame):
                        self._publish_frame(frame, frame.jpeg, generation)
                        previous = frame.sequence
                    else:
                        if executor is None:
                            # Lazily spin up workers and dual codecs only when
                            # resizing oversized frames or using a custom codec.
                            executor = ThreadPoolExecutor(
                                max_workers=2, thread_name_prefix="preview-codec"
                            )
                            codecs = []
                            for _ in range(2):
                                codecs.append(self.codec_factory())
                        for slot, future in enumerate(pending):
                            if future is not None:
                                if not future.done():
                                    continue
                                future.result()
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
        except Exception as error:
            # Unavailable camera / codec is retried on the next viewer request.
            with suppress(Exception):
                operational_logger("preview-thumbnail").event(
                    "preview_thumbnail_worker_failed",
                    level="warning",
                    exception_type=type(error).__name__,
                )
        finally:
            with self.condition:
                if self.worker is threading.current_thread():
                    self.worker = None
                    self.latest = None
                    self.condition.notify_all()
            if executor is not None:
                executor.shutdown(wait=True)
            for codec in codecs:
                with suppress(Exception):
                    codec.close()

    def _encode(self, codec, frame, generation):
        jpeg = codec.resize(frame.jpeg)
        self._publish_frame(frame, jpeg, generation)
