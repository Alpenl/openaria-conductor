"""从未安装 RP-YLX 的 RDK X5 启动 bundle 内部署工具。"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import tarfile
import tempfile
from pathlib import Path, PurePosixPath


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _prepare_runtime(root: Path) -> Path:
    try:
        bundle = json.loads((root / "bundle.json").read_bytes())
        descriptor = bundle["runtime"]
        archive_path = root / descriptor["file"]
    except (KeyError, OSError, TypeError, json.JSONDecodeError) as error:
        raise RuntimeError(f"runtime descriptor is invalid: {error}") from error
    if (
        not isinstance(descriptor, dict)
        or descriptor.get("executable") != "runtime/bin/python3"
        or archive_path.parent != root
        or archive_path.is_symlink()
        or not archive_path.is_file()
        or archive_path.stat().st_size != descriptor.get("bytes")
        or _sha256(archive_path) != descriptor.get("sha256")
    ):
        raise RuntimeError("package-owned runtime digest check failed")
    try:
        with tarfile.open(archive_path, "r:gz") as archive:
            members = archive.getmembers()
            if len(members) > 100_000:
                raise RuntimeError("package-owned runtime contains too many files")
            total = 0
            names: set[str] = set()
            executable_found = False
            for member in members:
                path = PurePosixPath(member.name)
                raw_parts = member.name.split("/")
                total += member.size
                if (
                    path.is_absolute()
                    or not path.parts
                    or path.parts[0] != "runtime"
                    or any(part in {"", ".", ".."} for part in raw_parts)
                    or member.name in names
                    or member.issym()
                    or member.islnk()
                    or not (member.isdir() or member.isfile())
                    or total > 4 * 1024 * 1024 * 1024
                ):
                    raise RuntimeError(f"unsafe package-owned runtime member: {member.name}")
                names.add(member.name)
                if member.name == "runtime/bin/python3" and member.isfile() and member.size > 0:
                    executable_found = True
            if not executable_found:
                raise RuntimeError("package-owned runtime executable is missing")
            with tempfile.TemporaryDirectory(prefix=".runtime-stage-", dir=root) as directory:
                stage = Path(directory)
                for member in members:
                    target = stage.joinpath(*PurePosixPath(member.name).parts)
                    if member.isdir():
                        target.mkdir(parents=True, exist_ok=True)
                        target.chmod(0o755)
                        continue
                    target.parent.mkdir(parents=True, exist_ok=True)
                    source = archive.extractfile(member)
                    if source is None:
                        raise RuntimeError(
                            f"cannot read package-owned runtime member: {member.name}"
                        )
                    with source, target.open("xb") as destination:
                        shutil.copyfileobj(source, destination, length=1024 * 1024)
                    target.chmod(
                        0o755
                        if member.name == "runtime/bin/python3" or member.mode & 0o111
                        else 0o644
                    )
                installed_runtime = root / "runtime"
                if installed_runtime.is_symlink() or (
                    installed_runtime.exists() and not installed_runtime.is_dir()
                ):
                    raise RuntimeError("package-owned runtime destination is unsafe")
                if installed_runtime.exists():
                    shutil.rmtree(installed_runtime)
                os.replace(stage / "runtime", installed_runtime)
    except (OSError, tarfile.TarError) as error:
        raise RuntimeError(f"cannot extract package-owned runtime: {error}") from error
    executable = root / "runtime/bin/python3"
    return executable


def main() -> int:
    root = Path(__file__).resolve().parent
    runtime = root / "runtime/bin/python3"
    try:
        using_managed_runtime = (
            runtime.is_file() and Path(sys.executable).resolve() == runtime.resolve()
        )
    except OSError:
        using_managed_runtime = False
    if not using_managed_runtime:
        try:
            runtime = _prepare_runtime(root)
        except RuntimeError as error:
            print(str(error), file=sys.stderr)
            return 2
        os.execv(str(runtime), [str(runtime), str(Path(__file__).resolve()), *sys.argv[1:]])
    wheels = sorted(root.glob("rp_ylx-*.whl"))
    if len(wheels) != 1:
        print("bundle 必须包含唯一 rp_ylx wheel", file=sys.stderr)
        return 2
    sys.path.insert(0, str(wheels[0]))
    from rp_ylx.deployment import main as deployment_main

    return deployment_main(sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(main())
