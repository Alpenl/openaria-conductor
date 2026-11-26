"""Offline, board-bound Miro-Ego first-boot restoration and p1 CONFIG cleanup."""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
import uuid
from contextlib import suppress
from pathlib import Path, PurePosixPath
from typing import Any

if __package__:
    from .. import disks
    from . import device, partition_layout, provision, storage_firstboot
else:
    _parent = str(Path(__file__).resolve().parents[1])
    if (Path(_parent) / "disks.py").is_file() and _parent not in sys.path:
        sys.path.insert(0, _parent)
    import device
    import disks
    import partition_layout
    import provision
    import storage_firstboot

ROOT = Path("/var/lib/miro-ego-factory/firstboot")
BOOT = Path("/boot/config/miro-ego")
HOBOT_HOOK = Path("/boot/config/hobot_config.sh")
FIRSTBOOT_UNIT = Path("/etc/systemd/system/miro-ego-firstboot.service")
VOICE_STATE = Path("/var/lib/miro-ego/voice.json")

STATUS_SCHEMA = "miro-ego.autoinstall-status.v1"
KEEP_BOOT_FILES = frozenset({"status.json", "DO_NOT_DELETE.txt"})

DO_NOT_DELETE_TEXT = (
    "Miro-Ego RDK X5 CONFIG boot partition.\n"
    "\n"
    "Offline factory provisioning has completed and transient installation files\n"
    "have been cleaned up. Do NOT format this partition or delete boot files.\n"
    "Recordings and diagnostic logs are stored on the separate MIRO-EGO volume.\n"
)

UNIT = """[Unit]
Description=Miro-Ego offline factory recovery
Requires=hobot-resizefs.service
After=local-fs.target hobot-resizefs.service

[Service]
Type=oneshot
UMask=0077
ExecStart=/usr/bin/python3 -B /boot/config/miro-ego/firstboot.py
TimeoutStartSec=30min
StandardOutput=append:/var/lib/miro-ego-factory/firstboot/install.log
StandardError=append:/var/lib/miro-ego-factory/firstboot/install.log
"""

PHASES = ("restoring", "configured", "installed", "complete")


def _atomic_write_text(
    path: Path,
    text: str,
    *,
    mode: int,
    strict_mode: bool = True,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    fd = os.open(temporary, flags, mode)
    replaced = False
    try:
        try:
            stream = os.fdopen(fd, "w", encoding="utf-8")
        except BaseException:
            os.close(fd)
            raise
        with stream:
            try:
                os.fchmod(stream.fileno(), mode)
            except OSError as exc:
                if strict_mode or exc.errno not in (errno.EPERM, errno.EINVAL, errno.EOPNOTSUPP):
                    raise
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        replaced = True
        dir_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except BaseException:
        if not replaced:
            with suppress(OSError):
                temporary.unlink(missing_ok=True)
        raise


def atomic_json(
    path: str | Path, value: Any, *, mode: int = 0o600, strict_mode: bool = True
) -> None:
    """Atomically write JSON to *path* with fsync and parent-dir sync."""
    _atomic_write_text(
        Path(path),
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        mode=mode,
        strict_mode=strict_mode,
    )


def extract_bundle(archive: str | Path, destination: Path) -> None:
    """Validate every tar member before extraction; refuse links or path traversal."""
    with tarfile.open(archive) as tar:
        members = tar.getmembers()
        if (
            len(members) > provision.MAX_BUNDLE_MEMBERS
            or sum(m.size for m in members) > provision.MAX_BUNDLE_TOTAL_BYTES
        ):
            raise RuntimeError("Oversized bundle")
        seen: set[PurePosixPath] = set()
        for member in members:
            if member.name in (".", "./") and member.isdir():
                continue
            rel = PurePosixPath(member.name[2:] if member.name.startswith("./") else member.name)
            if rel.is_absolute() or ".." in rel.parts or not rel.parts or rel in seen:
                raise RuntimeError("Unsafe bundle path")
            if not (member.isfile() or member.isdir()):
                raise RuntimeError("Unsafe bundle member")
            seen.add(rel)
        destination.mkdir(mode=0o700)
        for member in members:
            if member.name in (".", "./") and member.isdir():
                continue
            rel = PurePosixPath(member.name[2:] if member.name.startswith("./") else member.name)
            target = destination / rel
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                extracted = tar.extractfile(member)
                if extracted is None:
                    raise RuntimeError(f"Failed to read bundle file: {rel}")
                with extracted as src, target.open("xb") as dst:
                    shutil.copyfileobj(src, dst)
                target.chmod(0o700 if member.mode & 0o111 else 0o600)


def run(*args: str, **kwargs: Any) -> subprocess.CompletedProcess[Any]:
    """Run a provisioning command and require exit code 0."""
    return subprocess.run(args, check=True, **kwargs)


def install_bundle(bundle: Path) -> None:
    """Install the native Miro-Ego release bundle via ``miro-ego deploy install``."""
    binary = bundle / "bin" / "miro-ego"
    if not binary.is_file() or binary.is_symlink():
        raise RuntimeError("Extracted bundle is missing native bin/miro-ego binary")
    binary.chmod(0o755)
    # Run under umask 0022 so installed binaries and units are readable by miro-ego.
    run(str(binary), "deploy", "install", str(bundle), umask=0o022)


def complete() -> bool:
    """Return True if persistent first-boot state on p2 is already marked complete."""
    path = ROOT / "state.json"
    return path.exists() and json.loads(path.read_bytes()).get("phase") == "complete"


def register() -> None:
    """Register and enqueue the oneshot systemd first-boot unit from ``hobot_config.sh``."""
    ROOT.mkdir(parents=True, mode=0o700, exist_ok=True)
    ROOT.chmod(0o700)
    if complete():
        return
    _atomic_write_text(FIRSTBOOT_UNIT, UNIT, mode=0o644)
    run("systemctl", "daemon-reload")
    # Never block vendor boot scripts waiting on resizefs or package install.
    run("systemctl", "start", "--no-block", FIRSTBOOT_UNIT.name)


def restore_voice(profile: dict[str, Any]) -> None:
    """Persist voice volume setting and restore ALSA mixer state if backed up."""
    volume = int(profile.get("voice", {"volume": 100})["volume"])
    run("systemctl", "stop", "miro-ego.service")
    atomic_json(VOICE_STATE, {"schema": "miro-ego.voice.v1", "volume": volume}, mode=0o640)
    if "var/lib/alsa/asound.state" in profile.get("files", {}):
        run("alsactl", "restore")
    run("systemctl", "start", "miro-ego.service")
    run("systemctl", "is-active", "--quiet", "miro-ego.service")


def cleanup_boot_partition(
    boot: Path,
    *,
    hook_path: Path | None = None,
    unit_path: Path | None = None,
) -> list[str]:
    """Delete transient first-boot payloads from p1 ``CONFIG``, keeping only status & notice.

    Removes ``bundle.tar``, ``profile.bin``, ``manifest.json``, ``*.deb``, ``*.py``,
    and ``__pycache__/`` so only ``status.json`` and ``DO_NOT_DELETE.txt`` remain
    under ``/boot/config/miro-ego/``. Resets ``hobot_config.sh`` to a clean no-op.
    """
    atomic_json(
        boot / "status.json",
        {"schema": STATUS_SCHEMA, "phase": "complete"},
        mode=0o644,
        strict_mode=False,
    )
    _atomic_write_text(
        boot / "DO_NOT_DELETE.txt",
        DO_NOT_DELETE_TEXT,
        mode=0o644,
        strict_mode=False,
    )

    removed: list[str] = []
    for entry in sorted(boot.iterdir(), key=lambda p: p.name):
        if entry.name in KEEP_BOOT_FILES:
            continue
        if entry.is_dir() and not entry.is_symlink():
            shutil.rmtree(entry)
        else:
            entry.unlink(missing_ok=True)
        removed.append(entry.name)

    if hook_path is not None and hook_path.exists():
        _atomic_write_text(hook_path, provision.CLEAN_HOOK, mode=0o755, strict_mode=False)
    if unit_path is not None:
        unit_path.unlink(missing_ok=True)

    return removed


def execute() -> None:
    """Run the idempotent first-boot provisioning transaction and clean up p1 ``CONFIG``."""
    if complete():
        return
    manifest = json.loads((BOOT / "manifest.json").read_bytes())
    if manifest.get("schema") != provision.AUTOINSTALL_SCHEMA:
        raise RuntimeError("Invalid autoinstall manifest")
    if "storage" not in manifest or not isinstance(manifest["storage"], dict):
        raise RuntimeError("Missing MIRO-EGO exFAT storage layout in manifest")

    sealed = (BOOT / "profile.bin").read_bytes()
    if hashlib.sha256(sealed).hexdigest() != manifest["profile_sha256"]:
        raise RuntimeError("Profile digest mismatch")
    profile = provision.open_profile(sealed, device.hardware())

    archive = BOOT / "bundle.tar"
    if disks.sha256_file(archive) != manifest["bundle_sha256"]:
        raise RuntimeError("Bundle digest mismatch")

    job = hashlib.sha256(sealed + manifest["bundle_sha256"].encode("utf-8")).hexdigest()
    state_path = ROOT / "state.json"
    if state_path.exists():
        state = json.loads(state_path.read_bytes())
        if state.get("job") != job or state.get("phase") not in PHASES:
            raise RuntimeError("Unrelated or invalid recovery transaction")
    else:
        device.require_fresh()
        state = {"job": job, "phase": "restoring"}
        atomic_json(state_path, state)

    def advance(phase: str) -> None:
        state["phase"] = phase
        atomic_json(state_path, state)
        atomic_json(
            BOOT / "status.json",
            {"schema": STATUS_SCHEMA, "phase": phase},
            mode=0o644,
            strict_mode=False,
        )

    if state["phase"] == "restoring":
        device.write_files(device.validate(profile))
        advance("configured")

    if state["phase"] == "configured":
        storage_layout = manifest["storage"]
        data_device, _ = storage_firstboot.prepare(storage_layout, BOOT, ROOT, atomic_json)
        if not shutil.which("logrotate"):
            logrotate_pkg = BOOT / partition_layout.LOGROTATE_PACKAGE
            expected_logrotate = storage_layout.get(
                "logrotate_sha256", partition_layout.LOGROTATE_SHA256
            )
            if expected_logrotate != partition_layout.LOGROTATE_SHA256:
                raise RuntimeError("Offline logrotate manifest digest mismatch")
            partition_layout.verify_deb(logrotate_pkg, partition_layout.LOGROTATE_SHA256)
            run("dpkg", "-i", str(logrotate_pkg))

        bundle = ROOT / "bundle"
        if bundle.is_symlink():
            raise RuntimeError("Unsafe staging directory")
        if bundle.exists():
            shutil.rmtree(bundle)
        provision.require_native_bundle(archive)
        extract_bundle(archive, bundle)
        run("sha256sum", "-c", "SHA256SUMS", cwd=bundle)
        install_bundle(bundle)
        storage_firstboot.mount_and_initialize(data_device)
        run("systemctl", "is-active", "--quiet", "miro-ego.service")
        advance("installed")

    if state["phase"] == "installed":
        restore_voice(profile)
        state["phase"] = "complete"
        atomic_json(state_path, state)
        cleanup_boot_partition(BOOT, hook_path=HOBOT_HOOK, unit_path=FIRSTBOOT_UNIT)
        shutil.rmtree(ROOT / "bundle", ignore_errors=True)
        os.sync()
        run("systemctl", "reboot", "--no-block")


def main() -> None:
    if os.geteuid() != 0:
        raise RuntimeError("Root required")
    os.umask(0o077)
    ROOT.mkdir(parents=True, mode=0o700, exist_ok=True)
    if "--register" in sys.argv:
        register()
        return
    with (ROOT / "lock").open("a", encoding="utf-8") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            execute()
        except Exception as error:
            # Never leak tracebacks or configuration secrets onto the readable FAT32 partition.
            atomic_json(
                BOOT / "status.json",
                {"schema": STATUS_SCHEMA, "phase": "failed", "error": type(error).__name__},
                mode=0o644,
                strict_mode=False,
            )
            print("Offline recovery failed: " + type(error).__name__, flush=True)
            raise SystemExit(1) from None


if __name__ == "__main__":
    main()
