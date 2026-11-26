"""Download the pinned official RDK OS 3.5.0 image and generate a SHA-256 manifest."""

from __future__ import annotations

import hashlib
import json
import lzma
import os
import re
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

NAME = "rdk-x5-ubuntu22-preinstalled-desktop-3.5.0-arm64.img.xz"
BASE = "https://archive.d-robotics.cc/downloads/os_images/rdk_x5/rdk_os_3.5.0-2026-4-9/"
MD5 = "b39cd58ab65e838929063e4f1e184d0b"
SHA256: str | None = None
ARCHIVE_BYTES = 2768107136


def _verify_archive(path: Path, expected_sha256: str | None) -> str:
    checksum = hashlib.md5()
    archive_sha = hashlib.sha256()
    with path.open("rb") as src:
        while data := src.read(4 * 1024**2):
            checksum.update(data)
            archive_sha.update(data)
    if checksum.hexdigest() != MD5:
        raise RuntimeError("Official archive checksum mismatch")
    pinned_sha256 = expected_sha256 or os.environ.get("MIRO_EGO_RDK_IMAGE_ARCHIVE_SHA256") or SHA256
    if pinned_sha256 is not None:
        normalized = pinned_sha256.strip().lower()
        if not re.fullmatch(r"[0-9a-f]{64}", normalized):
            raise RuntimeError("Invalid expected archive SHA-256")
        if archive_sha.hexdigest() != normalized:
            raise RuntimeError("Official archive SHA-256 mismatch")
    return archive_sha.hexdigest()


def prepare(root: str | Path, *, expected_sha256: str | None = None) -> Path:
    """Download/verify the official RDK OS 3.5.0 archive and write ``image.json``."""
    root_path = Path(root)
    root_path.mkdir(parents=True, exist_ok=True)
    archive = root_path / NAME
    image = root_path / NAME.removesuffix(".xz")
    manifest = root_path / "image.json"
    if any(p.exists() for p in (image, manifest)):
        raise RuntimeError("Image output already exists; choose an empty destination")
    if not archive.exists():
        partial = archive.with_suffix(archive.suffix + ".partial")
        print("Downloading official RDK OS 3.5.0 (2.77 GB)...", flush=True)
        size = ARCHIVE_BYTES
        chunk = 32 * 1024**2
        created_partial = False
        try:
            with partial.open("xb") as dst:
                created_partial = True
                dst.truncate(size)

            def download(bounds: tuple[int, int]) -> int:
                start, end = bounds
                request = urllib.request.Request(
                    BASE + NAME, headers={"Range": f"bytes={start}-{end - 1}"}
                )
                with (
                    urllib.request.urlopen(request, timeout=120) as src,
                    partial.open("r+b") as dst,
                ):
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
            archive_sha_hex = _verify_archive(partial, expected_sha256)
            os.replace(partial, archive)
        except BaseException:
            if created_partial:
                partial.unlink(missing_ok=True)
            raise
    else:
        archive_sha_hex = _verify_archive(archive, expected_sha256)

    print("Extracting and computing system image SHA-256...", flush=True)
    sha = hashlib.sha256()
    with lzma.open(archive) as src, image.open("xb") as dst:
        while data := src.read(4 * 1024**2):
            dst.write(data)
            sha.update(data)
        dst.flush()
        os.fsync(dst.fileno())
    with manifest.open("x", encoding="utf-8") as out:
        json.dump(
            {
                "source": BASE + NAME,
                "official_md5": MD5,
                "archive_sha256": archive_sha_hex,
                "image": str(image.resolve()),
                "bytes": image.stat().st_size,
                "sha256": sha.hexdigest(),
            },
            out,
            indent=2,
        )
    print("Image verified and ready.", flush=True)
    return manifest


if __name__ == "__main__":
    import argparse

    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("directory", type=Path)
    cli.add_argument("--archive-sha256", help="Optional pinned SHA-256 for the compressed .img.xz")
    parsed = cli.parse_args()
    prepare(parsed.directory, expected_sha256=parsed.archive_sha256)
