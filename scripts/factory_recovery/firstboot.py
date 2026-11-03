"""Offline, board-bound restoration, started by the vendor CONFIG boot hook."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path, PurePosixPath

if __package__:
    from . import device, disks, provision
else:
    import device
    import disks
    import provision

ROOT = Path("/var/lib/openaria-factory/firstboot")
BOOT = Path("/boot/config/openaria")
UNIT = """[Unit]
Description=OpenAria offline factory recovery
Requires=hobot-resizefs.service
After=local-fs.target hobot-resizefs.service

[Service]
Type=oneshot
UMask=0077
ExecStart=/usr/bin/python3 /boot/config/openaria/firstboot.py
TimeoutStartSec=30min
StandardOutput=append:/var/lib/openaria-factory/firstboot/install.log
StandardError=append:/var/lib/openaria-factory/firstboot/install.log
"""
PHASES = ("restoring", "configured", "installed", "complete")


def atomic_json(path, value):
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as out:
        json.dump(value, out)
        out.flush()
        os.fsync(out.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def extract_bundle(archive, destination):
    """Validate every member before writing; never accept links or traversal."""
    with tarfile.open(archive) as tar:
        members = tar.getmembers()
        if len(members) > 10000 or sum(m.size for m in members) > 4 * 1024**3:
            raise RuntimeError("Oversized bundle")
        seen = set()
        for m in members:
            if m.name in (".", "./") and m.isdir():
                continue
            p = PurePosixPath(m.name)
            if p.is_absolute() or ".." in p.parts or not p.parts or p in seen:
                raise RuntimeError("Unsafe bundle path")
            if not (m.isfile() or m.isdir()):
                raise RuntimeError("Unsafe bundle member")
            seen.add(p)
        destination.mkdir(mode=0o700)
        for m in members:
            if m.name in (".", "./") and m.isdir():
                continue
            p = destination / m.name
            if m.isdir():
                p.mkdir(parents=True, exist_ok=True)
            else:
                p.parent.mkdir(parents=True, exist_ok=True)
                with tar.extractfile(m) as src, p.open("xb") as dst:
                    shutil.copyfileobj(src, dst)
                p.chmod(0o700 if m.mode & 0o111 else 0o600)


def run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def complete():
    path = ROOT / "state.json"
    return path.exists() and json.loads(path.read_bytes()).get("phase") == "complete"


def register():
    ROOT.mkdir(parents=True, mode=0o700, exist_ok=True)
    ROOT.chmod(0o700)
    if complete():
        return
    unit = Path("/etc/systemd/system/openaria-firstboot.service")
    unit.write_text(UNIT)
    run("systemctl", "daemon-reload")
    # Never wait for the boot/resize jobs from inside the vendor's boot hook.
    run("systemctl", "start", "--no-block", "openaria-firstboot.service")


def restore_volume(profile):
    volume = profile.get("voice", {"volume": 100})["volume"]
    run("systemctl", "stop", "rp-ylx.service")
    code = (
        "from rp_ylx.persistence import StateDatabase;from pathlib import Path;"
        "db=StateDatabase(Path('/var/lib/rp-ylx/state.sqlite3'));"
        f"db.put('voice_feedback',{{'volume':{volume}}},history=True)"
    )
    run(
        "runuser",
        "-u",
        "rp-ylx",
        "--",
        "/usr/bin/env",
        "PYTHONPATH=/opt/rp-ylx/current/site-packages",
        "/opt/rp-ylx/current/runtime/bin/python3",
        "-c",
        code,
    )
    if "var/lib/alsa/asound.state" in profile["files"]:
        run("alsactl", "restore")
    run("systemctl", "start", "rp-ylx.service")
    run("systemctl", "is-active", "--quiet", "rp-ylx.service")


def execute():
    if complete():
        return
    manifest = json.loads((BOOT / "manifest.json").read_bytes())
    if manifest.get("schema") != "openaria.autoinstall.v1":
        raise RuntimeError("Invalid manifest")
    sealed = (BOOT / "profile.bin").read_bytes()
    if hashlib.sha256(sealed).hexdigest() != manifest["profile_sha256"]:
        raise RuntimeError("Profile digest mismatch")
    profile = provision.open_profile(sealed, device.hardware())
    archive = BOOT / "bundle.tar"
    if disks.digest(archive) != manifest["bundle_sha256"]:
        raise RuntimeError("Bundle digest mismatch")
    job = hashlib.sha256(sealed + manifest["bundle_sha256"].encode()).hexdigest()
    state_path = ROOT / "state.json"
    if state_path.exists():
        state = json.loads(state_path.read_bytes())
        if state.get("job") != job or state.get("phase") not in PHASES:
            raise RuntimeError("Unrelated or invalid recovery transaction")
    else:
        device.require_fresh()
        state = {"job": job, "phase": "restoring"}
        atomic_json(state_path, state)

    def advance(phase):
        state["phase"] = phase
        atomic_json(state_path, state)
        atomic_json(BOOT / "status.json", {"phase": phase})

    if state["phase"] == "restoring":
        # Only an owned transaction can repeat partial configuration writes.
        device.write_files(device.validate(profile))
        advance("configured")
    if state["phase"] == "configured":
        bundle = ROOT / "bundle"
        if bundle.is_symlink():
            raise RuntimeError("Unsafe staging directory")
        if bundle.exists():
            shutil.rmtree(bundle)
        extract_bundle(archive, bundle)
        run("sha256sum", "-c", "SHA256SUMS", cwd=bundle)
        run("/usr/bin/python3", str(bundle / "rdk_x5_install.py"), "install", str(bundle))
        run("systemctl", "is-active", "--quiet", "rp-ylx.service")
        advance("installed")
    if state["phase"] == "installed":
        restore_volume(profile)
        advance("complete")
        (BOOT / "profile.bin").unlink(missing_ok=True)
        os.sync()
        run("systemctl", "reboot", "--no-block")


def main():
    if os.geteuid() != 0:
        raise RuntimeError("Root required")
    os.umask(0o077)
    ROOT.mkdir(parents=True, mode=0o700, exist_ok=True)
    if "--register" in sys.argv:
        register()
        return
    with (ROOT / "lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            execute()
        except Exception as error:
            # No traceback/configuration secrets on the readable FAT partition.
            atomic_json(BOOT / "status.json", {"phase": "failed", "error": type(error).__name__})
            print("Offline recovery failed: " + type(error).__name__, flush=True)
            raise SystemExit(1) from None


if __name__ == "__main__":
    main()
