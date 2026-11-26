# Miro-Ego Factory Recovery Tool

Host-PC utility for composing, flashing, and backing up Miro-Ego RDK X5 TF/SD cards.

## Partition Layout (RDK OS 3.5.0)

`compose` and `auto-flash` produce a 3-partition MBR layout with zero loop-volume fallback:

| Partition | Type | Filesystem | Label | Size | Role |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **p1** | `0x0C` | FAT32 | `CONFIG` | `256 MiB` | Vendor boot hook & one-shot provisioning payload |
| **p2** | `0x83` | ext4 | `rootfs` | Fixed `20 GiB` | Zero-growth appliance OS + native `miro-ego` binary |
| **p3** | `0x07` | exFAT | `MIRO-EGO` | Remaining (`>= 4 GiB`, 1 MiB aligned) | `/data` (`recordings/`, `logs/`, `updates/`, hidden `.miro-ego/`) |

## First-Boot Lifecycle & p1 `CONFIG` Cleanup

On first boot of the original physical RDK X5 board:

1. Vendor `hobot_config.sh` registers `miro-ego-firstboot.service`.
2. `firstboot.py` decrypts the board-bound `MEB1` profile, restores allowlisted `/etc/miro-ego/` and network configs, installs verified offline `exfatprogs` + `logrotate` `.deb` packages, resizes p2 to 20 GiB, formats p3 as exFAT with label `MIRO-EGO`, configures `/etc/default/miro-ego-data-volume`, installs the native AArch64 `miro-ego` bundle (`miro-ego deploy install`), mounts `/data`, creates `/data/{recordings,logs,updates,.miro-ego/...}` with native `ATTR_HIDDEN`, and restores voice volume.
3. Upon completion, all one-shot provisioning files on p1 `CONFIG` (`bundle.tar`, `profile.bin`, `manifest.json`, `.deb` packages, Python scripts, and `__pycache__/`) are deleted, leaving only `/miro-ego/status.json` and `/miro-ego/DO_NOT_DELETE.txt`, and `/hobot_config.sh` is reset to a clean `exit 0` before rebooting once.

## Quick Start

Install host dependencies (`requirements.txt`), then run interactively or via CLI:

```bash
# List eligible removable external disks
python -m miro_ego_host.factory_recovery.recover disks

# Back up an idle device configuration into an encrypted .mer file
python -m miro_ego_host.factory_recovery.recover backup --host 192.168.1.50 --profile board.mer

# Download and verify official RDK OS 3.5.0 image
python -m miro_ego_host.factory_recovery.recover prepare-image

# Compose an offline recovery image for a specific card byte size
python -m miro_ego_host.factory_recovery.recover compose \
  --image images/rdk-x5-ubuntu22-preinstalled-desktop-3.5.0-arm64.img \
  --sha256 <image-sha256> \
  --bundle bundle.tar \
  --bundle-sha256 <bundle-sha256> \
  --profile board.mer \
  --card-bytes 127999672320 \
  --output out/miro-ego-recovery.img

# One-step compose and flash to an external SD card
python -m miro_ego_host.factory_recovery.recover auto-flash \
  --disk /dev/sdb \
  --image images/rdk-x5-ubuntu22-preinstalled-desktop-3.5.0-arm64.img \
  --sha256 <image-sha256> \
  --bundle bundle.tar \
  --bundle-sha256 <bundle-sha256> \
  --profile board.mer \
  --output out/miro-ego-recovery.img
```

Encrypted `.mer` backups use Scrypt + AES-256-GCM (`MER1` envelope). Keep backup passwords off the SD card.
