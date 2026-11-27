# Miro-Ego RDK X5 Storage Layout & Zero-Growth System-Disk Specification

This document defines the canonical 3-partition TF/SD card layout, the `/data` (`MIRO-EGO`) exFAT recording volume directory tree, and the 20 GiB zero-growth system-disk budget enforced by `deploy/miro-ego/` in Miro-Ego 0.3.0.

---

## 1. Three-Partition MBR Layout

Miro-Ego 0.3.0 targets freshly provisioned RDK X5 TF/SD cards (~24.26 GiB–512 GiB actual capacity; 260 MiB boot/CONFIG + 20 GiB rootfs + minimum 4 GiB reserved for recordings, i.e. `MIN_CARD_BYTES = 26,042,433,536` bytes) using an MBR partition table that preserves the official RDK OS 3.5.0 boot code, 32-bit disk signature, and Partition 1/2 start offsets:

| Partition | Device Node | MBR Type | Filesystem | Label | Start Sector | Size | Mountpoint | Role |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **p1** | `/dev/mmcblk0p1` | `0x0C` (FAT32 LBA) | FAT32 | `CONFIG` | `8192` | `256 MiB` (`524,288` sectors) | `/boot/config` | Vendor boot hooks & transient first-boot provisioning staging |
| **p2** | `/dev/mmcblk0p2` | `0x83` (Linux) | ext4 | `rootfs` | `532480` | **Fixed `20 GiB`** (`41,943,040` sectors) | `/` | Zero-growth appliance OS, native Rust binary + C encoder, fixed-size config |
| **p3** | `/dev/mmcblk0p3` | `0x07` (exFAT/NTFS) | exFAT | `MIRO-EGO` | `42475520` | Remaining card capacity (1 MiB aligned, `>= 4 GiB`, `< 1 MiB` tail slack) | `/data` | Dedicated recording, diagnostic log, and internal catalog/staging volume |

### Partition 1 (`CONFIG`) Post-Firstboot Cleanup

- During offline factory provisioning, Partition 1 carries the first-boot bootstrap scripts, offline ARM64 `.deb` tools (`exfatprogs`, `logrotate`), sealed device profile (`profile.bin`), and release archive (`bundle.tar`) under `/boot/config/miro-ego/`.
- Once first-boot installation reaches `phase = "complete"`, the provisioner persists completion state on Partition 2, writes a non-secret status summary (`status.json`), and **unlinks all transient payload and secret artifacts** (`profile.bin`, `bundle.tar`, and staged installer archives) from `/boot/config/miro-ego/`, followed by `sync` and reboot.
- In steady-state operation, Partition 1 is never written by runtime capture, logging, or watchdog services.

---

## 2. Partition 2: 20 GiB Zero-Growth System Disk

### Design Principle

The 20 GiB ext4 root filesystem is treated as an **immutable-capacity appliance disk**:
- **Zero Python runtime on device**: All device-side services (`miro-ego serve`, `miro-ego network-control`, `miro-ego button`, `miro-ego deploy`, `miro-ego volume init`) run from a single native Rust binary (`/opt/miro-ego/current/bin/miro-ego`) plus the standalone C hardware stereo encoder (`/opt/miro-ego/current/bin/miro-ego-encoder`).
- **No loop-backed image**: Loop-volume storage on ext4 is completely removed. Recording writes are strictly confined to the verified exFAT Partition 3 (`/data`).
- **Fixed-size system paths only**:
  - `/opt/miro-ego/` — versioned native release directories (`current` and at most one `previous` rollback slot).
  - `/etc/miro-ego/` — tiny bounded JSON configs (`device.json`, `button.json`, optional TLS material) (`0750 root:miro-ego`).
  - `/etc/default/miro-ego-data-volume` — 3-line exFAT mount identity (`MIRO_EGO_DATA_VOLUME_MODE=exfat`, `MIRO_EGO_DATA_VOLUME_PARTUUID`, `MIRO_EGO_DATA_VOLUME_UUID`).
  - `/etc/default/miro-ego-wifi-watchdog` — bounded watchdog tuning defaults.
  - `/var/lib/miro-ego/` — small bounded appliance state (`0750 miro-ego:miro-ego`), no unbounded journals or recordings.
  - `/var/lib/miro-ego-network/` — bounded authoritative NetworkManager transaction state (`0750 root:root`).
  - `/var/lib/miro-ego-wifi-watchdog/` — strictly two fixed-size epoch timestamp files (`last-recovery-epoch`, `last-automatic-reboot-epoch`, `< 64 bytes` combined).
  - `/run/miro-ego/` (`0750 root:miro-ego`) and `/run/miro-ego-wifi-watchdog/` (`0700 root:root`) — volatile `tmpfs` (RAM-backed) locks, sockets, clock sync state, and bounded fallback watchdog diagnostics.

### System-Disk Growth Budget

Every background subsystem capable of unbounded disk growth on Linux is capped by drop-in configs in `deploy/miro-ego/`:

| Subsystem | Config Asset | Target Install Path | Hard Cap / Enforcement | Max Steady-State Footprint |
| :--- | :--- | :--- | :--- | :--- |
| **systemd-journald** | `miro-ego-journal.conf` | `/etc/systemd/journald.conf.d/60-miro-ego-space.conf` | `SystemMaxUse=64M`, `SystemMaxFileSize=8M`, `SystemKeepFree=1G`, `RuntimeMaxUse=16M` | `<= 64 MiB` on ext4 (`<= 16 MiB` tmpfs) |
| **logrotate rules** | `miro-ego-logrotate.conf` | `/etc/logrotate.d/00-miro-ego-defaults` | Global `maxsize 4M` applied to subsequent vendor rules (`rsyslog`, `dpkg`, etc.) | `<= 32 MiB` across rotated `/var/log/*` |
| **logrotate cadence** | `miro-ego-logrotate-timer.conf` | `/etc/systemd/system/logrotate.timer.d/60-miro-ego-space.conf` | `OnCalendar=*:0/5`, `AccuracySec=30s`, `RandomizedDelaySec=0` (5-minute enforcement) | Prevents burst spikes between daily runs |
| **Core dumps (systemd)** | `miro-ego-core-limits.conf` | `/etc/systemd/system.conf.d/60-miro-ego-space.conf` | `DefaultLimitCORE=0:0` (+ `LimitCORE=0` on every Miro-Ego unit) | `0 B` |
| **Core dumps (coredumpd)** | `miro-ego-coredump.conf` | `/etc/systemd/coredump.conf.d/60-miro-ego-space.conf` | `Storage=none`, `ProcessSizeMax=0` | `0 B` |
| **APT binary caches** | `miro-ego-apt-cache.conf` + `miro-ego.tmpfiles` | `/etc/apt/apt.conf.d/99miro-ego-cache` | `Dir::Cache::pkgcache ""`, `Dir::Cache::srcpkgcache ""`, plus tmpfiles `r` removal | `0 B` |
| **Wi-Fi watchdog logs** | `miro-ego-wifi-watchdog` + `miro-ego-wifi-watchdog.service` | `/usr/local/sbin/miro-ego-wifi-watchdog` | Writes rotated logs (`<= 10` files) to mounted `rw` `/data/logs/wifi-watchdog/`; falls back to tmpfs `/run/miro-ego-wifi-watchdog/diagnostics/` (`<= 3` files); `ProtectSystem=strict` blocks writes to unmounted `/data` | `<= 64 B` on ext4 (2 epoch timestamps only) |
| **Telemetry / audit fallback** | Native Rust daemon (`miro-ego-core`) | In-process RAM ring | When `/data` is unmounted, audit/telemetry buffer stays in RAM (`<= 4 MiB`), never spills to ext4 | `0 B` on ext4 |
| **Idle TRIM** | `miro-ego-idle-fstrim` + `miro-ego-idle-fstrim.conf` | `/usr/local/sbin/miro-ego-idle-fstrim` & `/etc/systemd/system/fstrim.service.d/50-miro-ego-idle.conf` | `Type=exec` (`RuntimeMaxSec=10min`), exclusive non-blocking `flock` on `/run/miro-ego/maintenance.lock` (defers with exit `75`, retries after `15min`), trims `/data` and mounted filesystems when idle | Reclaims flash blocks without stalling capture |
| **Base RDK OS + 2 native release slots** | Vendor OS + `/opt/miro-ego/releases/*` | `/` | RDK OS 3.5.0 base (~9–10 GiB) + 2 native binary releases (`< 200 MiB`) + bounded caps (`< 128 MiB`) | `~10.5 GiB` total (`~9.5 GiB` free headroom in 20 GiB) |

---

## 3. Partition 3: `MIRO-EGO` exFAT Recording Volume (`/data`)

### Strict Mount Verification (`miro-ego-data-volume`)

`miro-ego-data-volume.service` invokes `/usr/local/sbin/miro-ego-data-volume ensure` before `miro-ego.service` starts. The script enforces strict fail-closed verification with **no fallback**:

1. Requires `miro-ego` system user (`getent passwd miro-ego`).
2. Requires `MIRO_EGO_DATA_VOLUME_MODE=exfat`; rejects any other mode.
3. Validates and case-normalizes `MIRO_EGO_DATA_VOLUME_PARTUUID` (`^[[:xdigit:]]{8}-03$`, normalized lowercase) and `MIRO_EGO_DATA_VOLUME_UUID` (`^[[:xdigit:]]{4}-[[:xdigit:]]{4}$`, normalized uppercase).
4. Waits up to 5 seconds for `/dev/disk/by-uuid/<UUID>` (tolerating case) and verifies via `blkid` that `PARTUUID`, `UUID`, and `TYPE=exfat` all match.
5. If `/data` is not mounted:
   - Runs bounded safe repair: `timeout --kill-after=5s 90s fsck.exfat -p "$device"` (accepts exit code `0` clean or `1` repaired; never reformats).
   - Mounts with hardened exFAT options:
     `rw,nosuid,nodev,noexec,noatime,uid=<miro-ego>,gid=<miro-ego>,fmask=0137,dmask=0027,errors=remount-ro`.
6. Verifies the live mount via `findmnt` (`FSTYPE=exfat`, canonical `SOURCE` equals canonical device path, and `OPTIONS` contains `rw`, remounting `rw` if needed and failing closed if still read-only).
7. Ensures the canonical directory tree exists as `miro-ego`, runs `miro-ego volume init /data` if `/data/.miro-ego/volume.json` is absent, and applies the native exFAT hidden attribute (`ATTR_HIDDEN`) to `/data/.miro-ego`.

If Partition 3 is missing, fails identity verification, or remains read-only after I/O error (`errors=remount-ro`), `miro-ego.service` still starts so the web UI and diagnostics remain reachable, while recording admission is blocked and **never falls back to writing on Partition 2**.

### Directory Tree

When the TF card is inserted into a Windows or macOS computer, users see a clean top-level `MIRO-EGO` volume containing only user-facing directories, while internal metadata is hidden natively by exFAT `ATTR_HIDDEN` (and dot-prefix for macOS/Linux):

```text
/data/                              # exFAT volume label: MIRO-EGO
├── recordings/                     # [User-visible] Completed stereo recordings (video MP4, audio WAV, IMU, manifest)
├── logs/                           # [User-visible] Diagnostic & operational logs exported for support (/api/v1/logs)
│   └── wifi-watchdog/              # [User-visible] Rotated Wi-Fi recovery diagnostics (max 10 files)
├── updates/                        # [User-visible] Reserved staging directory for future offline update packages
└── .miro-ego/                      # [Hidden: exFAT ATTR_HIDDEN] Internal appliance metadata & staging
    ├── volume.json                 # Volume identity & schema metadata created by `miro-ego volume init`
    ├── index/                      # Persistent binary recording catalog (`catalog.bin`)
    ├── staging/                    # In-progress capture staging before atomic promotion into `recordings/`
    ├── quarantine/                 # Corrupted or incomplete captures isolated away from user recordings
    ├── audit/                      # Bounded Device API authorization audit logs
    ├── receipts/                   # Capture & transfer verification receipts
    └── telemetry/                  # Bounded operational telemetry records
```

### Applying Native exFAT `ATTR_HIDDEN`

Unix dot-prefixes hide `.miro-ego` on macOS and Linux, but Windows Explorer ignores leading dots unless the FAT/exFAT `ATTR_HIDDEN` bit (`0x02`) is set in the directory entry. `miro-ego-data-volume` applies `ATTR_HIDDEN` on every mount using a fallback-safe sequence:

1. Primary: `fatattr +h /data/.miro-ego` when `fatattr` is installed.
2. Native fallback: `runuser -u miro-ego -- /opt/miro-ego/current/bin/miro-ego volume hide /data/.miro-ego`, which invokes the Linux kernel `FAT_IOCTL_SET_ATTRIBUTES` ioctl directly with zero external package dependency.
3. Non-fatal warning if neither tool is available (for example during early bootstrap before binaries are staged), so booting and web access are never disrupted.

---

## 4. Concurrency Locks & Identity Summary

- **Service account**: `miro-ego:miro-ego` (`SupplementaryGroups=audio video vpu jpu vps misc` on `miro-ego.service`).
- **SSH admin account**: `MiroEgo` (provisioned atomically by `/usr/local/sbin/miro-ego-device-login`; existing user passwords are preserved across upgrades).
- **Rescue hotspot SSID**: `MiroEgo-XXXXXXXX`.
- **mDNS discovery**: `_miro-ego._tcp` (`Miro-Ego on %h`, TXT `api=/api/v1/device`, port `8080`).
- **Runtime locks** (created by `miro-ego.tmpfiles` in `/run/miro-ego` (`0750 root:miro-ego`), lock files `0660 root:miro-ego`, never unlinked):
  - `/run/miro-ego/maintenance.lock`: Shared lock during capture, recovery, and session deletion; exclusive non-blocking lock required by `miro-ego-idle-fstrim` (defers with exit `75` if busy), `miro-ego-deploy`, and `miro-ego-wifi-watchdog` (acquired right before disruptive driver reload or automatic reboot, never during passive probes).
  - `/run/miro-ego/network-operation.lock`: Serializes authoritative network transactions in `miro-ego-network.service` against `miro-ego-wifi-watchdog` disruptive driver reloads and reboots.

---

## 5. Deployment Asset Installation Map

| Source File (`deploy/miro-ego/`) | Target Path on RDK X5 | Mode | Purpose |
| :--- | :--- | :--- | :--- |
| `miro-ego.service` | `/usr/lib/systemd/system/miro-ego.service` | `0644` | Main Rust stereo capture & HTTP `/api/v1` daemon |
| `miro-ego-data-volume` | `/usr/local/sbin/miro-ego-data-volume` | `0755` | Strict exFAT-only `/data` mount, layout, & `ATTR_HIDDEN` script |
| `miro-ego-data-volume.service` | `/usr/lib/systemd/system/miro-ego-data-volume.service` | `0644` | Early oneshot unit mounting `/data` before `miro-ego.service` |
| `miro-ego-network.service` | `/usr/lib/systemd/system/miro-ego-network.service` | `0644` | Hardened socket-activated privileged network & clock worker |
| `miro-ego-network.socket` | `/usr/lib/systemd/system/miro-ego-network.socket` | `0644` | Root/miro-ego fail-closed Unix control socket (`0600`) |
| `miro-ego-deploy.service` | `/usr/lib/systemd/system/miro-ego-deploy.service` | `0644` | Privileged transactional A/B firmware deployment & rollback RPC worker |
| `miro-ego-deploy.socket` | `/usr/lib/systemd/system/miro-ego-deploy.socket` | `0644` | Socket-activated Unix control socket (`/run/miro-ego/deploy-control.sock`, `0660`) |
| `miro-ego-button.service` | `/usr/lib/systemd/system/miro-ego-button.service` | `0644` | 40-pin physical recording button & `ACT` LED daemon |
| `miro-ego-button.json` | `/etc/miro-ego/button.json` | `0644` | Physical pin 37 button & LED default config (preserved on upgrade) |
| `90-miro-ego-leds.rules` | `/etc/udev/rules.d/90-miro-ego-leds.rules` | `0644` | Grants `miro-ego` group write access (`0660`) to LED sysfs attributes |
| `miro-ego-recover.service` | `/usr/lib/systemd/system/miro-ego-recover.service` | `0644` | Boot-time transactional release recovery |
| `miro-ego-wifi-watchdog` | `/usr/local/sbin/miro-ego-wifi-watchdog` | `0755` | POSIX sh Wi-Fi health check, driver reload, & bounded diagnostics |
| `miro-ego-wifi-watchdog.default` | `/etc/default/miro-ego-wifi-watchdog` | `0644` | Watchdog tuning defaults (preserved on upgrade) |
| `miro-ego-wifi-watchdog.service` | `/usr/lib/systemd/system/miro-ego-wifi-watchdog.service` | `0644` | Hardened oneshot watchdog service (`ProtectSystem=strict`) |
| `miro-ego-wifi-watchdog.timer` | `/usr/lib/systemd/system/miro-ego-wifi-watchdog.timer` | `0644` | Periodic timer (`OnBootSec=3min`, `OnUnitActiveSec=1min`) |
| `miro-ego-idle-fstrim` | `/usr/local/sbin/miro-ego-idle-fstrim` | `0755` | Fail-closed maintenance-locked TRIM wrapper |
| `miro-ego-idle-fstrim.conf` | `/etc/systemd/system/fstrim.service.d/50-miro-ego-idle.conf` | `0644` | `fstrim.service` drop-in (`Type=exec`, retry on exit `75`) |
| `miro-ego-device-login` | `/usr/local/sbin/miro-ego-device-login` | `0700` | Provisions `MiroEgo` SSH admin user without resetting existing passwords |
| `miro-ego.sysusers` | `/usr/lib/sysusers.d/miro-ego.conf` | `0644` | Creates `miro-ego` service account & `video`/`misc` memberships |
| `miro-ego.tmpfiles` | `/usr/lib/tmpfiles.d/miro-ego.conf` | `0644` | Creates runtime dirs/locks and purges APT binary cache files |
| `miro-ego.avahi` | `/etc/avahi/services/miro-ego.service` | `0644` | Default HTTP mDNS `_miro-ego._tcp` advertisement |
| `miro-ego-customer.avahi` | `/etc/avahi/services/miro-ego.service` (customer mode) | `0644` | HTTPS customer-mode mDNS `_miro-ego._tcp` advertisement |
| `90-miro-ego-wifi-powersave.conf` | `/etc/NetworkManager/conf.d/90-miro-ego-wifi-powersave.conf` | `0644` | Disables NetworkManager Wi-Fi power-save (`wifi.powersave = 2`) |
| `aic8800-miro-ego.conf` | `/etc/modprobe.d/aic8800-miro-ego.conf` | `0644` | Disables AIC8800 SDIO firmware power-save (`ps_on=0 dpsm=0`) |
| `miro-ego-apt-cache.conf` | `/etc/apt/apt.conf.d/99miro-ego-cache` | `0644` | Keeps APT index caches in RAM |
| `miro-ego-core-limits.conf` | `/etc/systemd/system.conf.d/60-miro-ego-space.conf` | `0644` | Disables process core files globally (`DefaultLimitCORE=0:0`) |
| `miro-ego-coredump.conf` | `/etc/systemd/coredump.conf.d/60-miro-ego-space.conf` | `0644` | Disables systemd-coredump storage (`Storage=none`) |
| `miro-ego-journal.conf` | `/etc/systemd/journald.conf.d/60-miro-ego-space.conf` | `0644` | Bounds journald to 64 MiB disk / 16 MiB RAM / 1 GiB free |
| `miro-ego-logrotate.conf` | `/etc/logrotate.d/00-miro-ego-defaults` | `0644` | Enforces global `maxsize 4M` across logrotate configs |
| `miro-ego-logrotate-timer.conf` | `/etc/systemd/system/logrotate.timer.d/60-miro-ego-space.conf` | `0644` | Runs logrotate every 5 minutes (`*:0/5`) |
