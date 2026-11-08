"""Download the pinned official RDK X5 image and generate a local SHA-256 manifest."""

from __future__ import annotations

import hashlib
import json
import lzma
import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

NAME = "rdk-x5-ubuntu22-preinstalled-desktop-3.5.0-arm64.img.xz"
BASE = "https://archive.d-robotics.cc/downloads/os_images/rdk_x5/rdk_os_3.5.0-2026-4-9/"
MD5 = "b39cd58ab65e838929063e4f1e184d0b"
ARCHIVE_BYTES = 2768107136


def prepare(root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    archive = root / NAME
    image = root / NAME.removesuffix(".xz")
    manifest = root / "image.json"
    if any(p.exists() for p in (image, manifest)):
        raise RuntimeError("Image output already exists; choose an empty destination")
    if not archive.exists():
        partial = archive.with_suffix(archive.suffix + ".partial")
        print("Downloading official RDK OS 3.5.0 (2.77 GB)...", flush=True)
        size = ARCHIVE_BYTES
        chunk = 32 * 1024**2
        with partial.open("xb") as dst:
            dst.truncate(size)

        def download(bounds):
            start, end = bounds
            request = urllib.request.Request(
                BASE + NAME, headers={"Range": f"bytes={start}-{end - 1}"}
            )
            with urllib.request.urlopen(request, timeout=120) as src, partial.open("r+b") as dst:
                expected = f"bytes {start}-{end - 1}/{size}"
                if src.status != 206 or src.headers.get("Content-Range") != expected:
                    raise RuntimeError("Mirror did not honor the byte range")
                dst.seek(start)
                remaining = end - start
                while remaining:
                    data = src.read(min(4 * 1024**2, remaining))
                    if not data:
                        raise RuntimeError("Incomplete mirror response")
                    dst.write(data)
                    remaining -= len(data)
                dst.flush()
                os.fsync(dst.fileno())
            return end - start

        ranges = [(start, min(start + chunk, size)) for start in range(0, size, chunk)]
        with ThreadPoolExecutor(max_workers=8) as pool:
            done = 0
            for amount in pool.map(download, ranges):
                done += amount
                print(f"Downloaded {done // 1024**2} MiB", flush=True)
        partial.rename(archive)
    checksum = hashlib.md5()
    with archive.open("rb") as src:
        while data := src.read(4 * 1024**2):
            checksum.update(data)
    if checksum.hexdigest() != MD5:
        raise RuntimeError("Official archive checksum mismatch")
    print("Extracting and computing system image SHA-256...", flush=True)
    sha = hashlib.sha256()
    with lzma.open(archive) as src, image.open("xb") as dst:
        while data := src.read(4 * 1024**2):
            dst.write(data)
            sha.update(data)
        dst.flush()
        os.fsync(dst.fileno())
    with manifest.open("x") as out:
        json.dump(
            {
                "source": BASE + NAME,
                "official_md5": MD5,
                "image": str(image.resolve()),
                "bytes": image.stat().st_size,
                "sha256": sha.hexdigest(),
            },
            out,
            indent=2,
        )
    print("Image verified and ready.", flush=True)


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("directory", type=Path)
    prepare(p.parse_args().directory)
