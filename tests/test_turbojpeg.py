from __future__ import annotations

import ctypes
import unittest
from unittest.mock import patch

from rp_ylx.camera.turbojpeg import (
    TJXOPT_CROP,
    TurboJpegError,
    _configure_library,
    _transform_sbs_jpeg,
    lossless_crop_sbs_jpeg,
)


class _FakeFunction:
    def __init__(self, function) -> None:
        self.function = function
        self.argtypes = None
        self.restype = None

    def __call__(self, *args):
        return self.function(*args)


class _FakeTurboJpeg:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.handle = 17
        self.buffers: list[object] = []
        self.freed: list[int] = []
        self.destroyed: list[int] = []
        self.regions: list[tuple[int, int, int, int, int]] = []
        self.tjInitTransform = _FakeFunction(lambda: self.handle)
        self.tjTransform = _FakeFunction(self._transform)
        self.tjDestroy = _FakeFunction(self._destroy)
        self.tjFree = _FakeFunction(self._free)
        self.tjGetErrorStr2 = _FakeFunction(lambda handle: b"fake transform failure")

    def _transform(self, handle, source, size, count, outputs, sizes, transforms, flags):
        self.regions = [
            (
                transforms[index].r.x,
                transforms[index].r.y,
                transforms[index].r.w,
                transforms[index].r.h,
                transforms[index].options,
            )
            for index in range(count)
        ]
        values = (b"left-jpeg", b"right-jpeg")
        limit = 1 if self.fail else 2
        for index, value in enumerate(values[:limit]):
            buffer = (ctypes.c_ubyte * len(value)).from_buffer_copy(value)
            self.buffers.append(buffer)
            outputs[index] = ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte))
            sizes[index] = len(value)
        return -1 if self.fail else 0

    def _free(self, output) -> None:
        self.freed.append(ctypes.addressof(output.contents))

    def _destroy(self, handle) -> int:
        self.destroyed.append(handle)
        return 0


class TurboJpegTest(unittest.TestCase):
    def test_transform_crops_both_eyes_and_releases_c_resources(self) -> None:
        library = _configure_library(_FakeTurboJpeg())

        self.assertEqual(
            _transform_sbs_jpeg(library, b"source-jpeg", 3840, 1080),
            (b"left-jpeg", b"right-jpeg"),
        )
        self.assertEqual(
            library.regions,
            [
                (0, 0, 1920, 1080, TJXOPT_CROP),
                (1920, 0, 1920, 1080, TJXOPT_CROP),
            ],
        )
        self.assertEqual(len(library.freed), 2)
        self.assertEqual(library.destroyed, [17])

    def test_transform_failure_frees_partial_output_and_destroys_handle(self) -> None:
        library = _configure_library(_FakeTurboJpeg(fail=True))

        with self.assertRaisesRegex(TurboJpegError, "fake transform failure"):
            _transform_sbs_jpeg(library, b"source-jpeg", 3840, 1080)

        self.assertEqual(len(library.freed), 1)
        self.assertEqual(library.destroyed, [17])

    def test_missing_library_returns_none(self) -> None:
        with patch("rp_ylx.camera.turbojpeg._load_library", return_value=None):
            self.assertIsNone(lossless_crop_sbs_jpeg(b"source-jpeg", 3840, 1080))

    def test_transform_error_returns_none_to_allow_pillow_fallback(self) -> None:
        library = _configure_library(_FakeTurboJpeg(fail=True))
        with patch("rp_ylx.camera.turbojpeg._load_library", return_value=library):
            self.assertIsNone(lossless_crop_sbs_jpeg(b"source-jpeg", 3840, 1080))


if __name__ == "__main__":
    unittest.main()
