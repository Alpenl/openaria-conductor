# Miro-Ego

**Miro-Ego 0.3.0** is a pure-native stereo video, audio, and IMU capture appliance for **D-Robotics RDK X5 V1.0 + YLX 2UQ2**.

Version **0.3.0** replaces the legacy 0.2.x Python/PyO3/SQLite daemon with a **zero-Python on-device runtime**: a single Rust binary (`miro-ego`) paired with a standalone C hardware stereo encoder (`miro-ego-encoder`), a clean-break `/api/v1` Device API, an exFAT-only `MIRO-EGO` recording volume, and a zero-growth 20 GiB system disk.

---

## Architecture

### On-Device Native Runtime (Zero Python)

- **Unified Rust Binary (`miro-ego`)** — Workspace crates under `crates/`:
  - `crates/miro-ego-core`: config, canonical paths, `flock` locks, strict bounded JSON I/O, atomic writes, Linux exFAT `ATTR_HIDDEN` ioctl, structured telemetry.
  - `crates/miro-ego-capture`: V4L2 stereo camera discovery & capture, IMU reader, ALSA audio clock/capture, `memfd` + `eventfd` `MEGO_SHM` ring IPC with `miro-ego-encoder`, live MJPEG/H.264 preview feeders.
  - `crates/miro-ego-storage`: capture coordinator state machine, exFAT-safe recording staging/sealing, binary `catalog.bin` index (replacing SQLite), interrupted-session recovery, diagnostic logs, and bounded API audit store.
  - `crates/miro-ego-api`: async `tokio` + `axum` + `rustls` HTTP/HTTPS gateway implementing Miro-Ego Device API v1 (`/api/v1/...`), single-range artifact & log downloads, SSE event streams, MJPEG & H.264 fMP4 preview, and CORS/CSRF/Idempotency enforcement.
  - `crates/miro-ego-network`: privileged `nmcli` Wi-Fi/hotspot/Ethernet controller, Unix socket JSON-RPC server (`/run/miro-ego/network-control.sock`), rescue hotspot (`MiroEgo-XXXXXXXX`), and `_miro-ego._tcp` mDNS helper.
  - `crates/miro-ego-hw`: 40-pin GPIO recording button monitor, sysfs `ACT` LED indicator, ALSA voice feedback volume control, and RDK X5 thermal/board inspection.
  - `crates/miro-ego-deploy`: transactional A/B release manager (`/opt/miro-ego/releases/<commit>`, `current`/`previous` symlinks, automatic activation rollback) verifying `miro-ego.rdk-x5-bundle.v1` AArch64 ELF64 bundles.
  - `crates/miro-ego-cli`: single binary entrypoint (`serve`, `network-control`, `button`, `deploy`, `volume`, `status`, `probe`, `validate`).
- **Hardware Stereo Encoder (`miro-ego-encoder`)** — Standalone C11 process under [`csrc/miro-ego-encoder/`](csrc/miro-ego-encoder/Makefile) driving the RDK X5 Hobot VPU/JPU hardware codecs (`hb_mm_mc` / `hb_mm_mx`) over the `MEGO_SHM` zero-copy ring buffer, with host-side `-Wall -Wextra -Werror` + ASan/UBSan test suites.
- **Deployment Assets ([`deploy/miro-ego/`](deploy/miro-ego/))** — Hardened systemd units, `sysusers.d`/`tmpfiles.d`, Avahi `_miro-ego._tcp` service descriptors, exFAT `/data` mount verifier, POSIX `sh` Wi-Fi watchdog, idle TRIM wrapper, and zero-growth system disk caps.

### Host-PC Tooling (`tools/miro_ego_host/`)

Python is used **exclusively on the operator's host PC** and during CI verification:

- **Offline Factory Recovery & SD-Card Flasher**: [`tools/miro_ego_host/factory_recovery/README.md`](tools/miro_ego_host/factory_recovery/README.md) — composes and flashes 3-partition RDK OS 3.5.0 cards with board-bound `MEB1` encrypted profiles and automatic post-firstboot `CONFIG` cleanup.
- **Read-Only exFAT Card Exporter**: [`tools/miro_ego_host/card_export/README.md`](tools/miro_ego_host/card_export/README.md) — strictly read-only SHA-256-verified exporter for `MIRO-EGO` recordings and `/logs/` diagnostics across Windows, macOS, and Linux.

---

## Storage Layout & Zero-Growth 20 GiB System Disk

See [`docs/miro-ego-storage-layout.md`](docs/miro-ego-storage-layout.md) for the complete specification.

| Partition | Filesystem | Label | Size | Mountpoint | Role |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **p1** | FAT32 (`0x0C`) | `CONFIG` | `256 MiB` | `/boot/config` | Vendor boot hook & one-shot provisioning (cleaned after first boot) |
| **p2** | ext4 (`0x83`) | `rootfs` | Fixed `20 GiB` | `/` | Zero-growth appliance OS, native `miro-ego` + `miro-ego-encoder`, bounded configs |
| **p3** | exFAT (`0x07`) | `MIRO-EGO` | Remaining (`>= 4 GiB`) | `/data` | Recordings (`recordings/`), diagnostics (`logs/`), future offline updates (`updates/`), hidden `.miro-ego/` (`ATTR_HIDDEN`) |

- **Zero unbounded growth on p2**: journald (`<= 64 MiB` disk), logrotate (`maxsize 4M` every 5 min), core dumps disabled (`Storage=none`, `DefaultLimitCORE=0:0`), APT disk caches disabled, watchdog logs on `/data/logs/wifi-watchdog/` (tmpfs fallback only), and RAM-ring fallback for telemetry/audit when `/data` is unmounted.
- **Fail-closed recording volume**: `/data` mounts strictly by verified `PARTUUID` + `UUID` + `exfat` with no loop-volume fallback onto p2.

---

## Miro-Ego Device API v1 & Device Access

See [`docs/miro-ego-v1-api-migration.md`](docs/miro-ego-v1-api-migration.md) for the complete endpoint, header, and schema specification.

- **API Prefix**: `/api/v1/...` (`api_version: "1.0"`, fail-closed `supported_device_api_majors: [1]`)
- **OpenAPI 3.1 Contract**: [`contracts/miro-ego-device-v1.openapi.yaml`](contracts/miro-ego-device-v1.openapi.yaml)
- **Consumer Support Manifest**: [`contracts/miro-ego-api-support.json`](contracts/miro-ego-api-support.json)
- **JSON Schemas**:
  - [`miro-ego-device-session-v1.schema.json`](contracts/schemas/miro-ego-device-session-v1.schema.json)
  - [`miro-ego-recording-state-v1.schema.json`](contracts/schemas/miro-ego-recording-state-v1.schema.json)
  - [`miro-ego-api-audit-event-v1.schema.json`](contracts/schemas/miro-ego-api-audit-event-v1.schema.json)
  - [`miro-ego-operational-event-v1.schema.json`](contracts/schemas/miro-ego-operational-event-v1.schema.json)
  - [`miro-ego-performance-report-v1.schema.json`](contracts/schemas/miro-ego-performance-report-v1.schema.json)
- **Device Network Identity**:
  - mDNS discovery: `_miro-ego._tcp` (`Miro-Ego on <host>`, TXT `api=/api/v1/device`, `miro-ego.local`)
  - Rescue hotspot SSID: `MiroEgo-XXXXXXXX` (`http://10.42.0.1:8080/`)
  - SSH admin account: `MiroEgo`
  - Service user: `miro-ego:miro-ego`

---

## Development & Verification

Requires **Rust 1.85+** (edition 2024, `clippy`, `rustfmt`), **GCC + Make** (for C encoder host tests), and **uv** (for host-PC tools and verification scripts):

```bash
# Sync host-PC & verification Python environment
uv sync --frozen --extra dev

# Verify unified 0.3.x version across Cargo.toml, pyproject.toml, and API contracts
uv run --extra dev python scripts/release_version.py

# Run full hardware-free verification gate (static/contracts/lint + C ASan/UBSan + host tools + Rust workspace)
CI=true uv run --extra dev python scripts/check.py

# Fast local iteration mode (refused under CI=true)
uv run --extra dev python scripts/check.py --fast

# Build release native binary
cargo build --release -p miro-ego-cli
./target/release/miro-ego --version

# Run C encoder host test suite directly
make -C csrc/miro-ego-encoder check

# Assemble a pure-native RDK X5 AArch64 bundle (miro-ego.rdk-x5-bundle.v1)
uv run --extra dev python scripts/build_rdk_x5_bundle.py --help
```

> Note: Production on-device `miro-ego-encoder` binaries are compiled on RDK X5 (or against an RDK X5 Hobot SDK sysroot) via `make -C csrc/miro-ego-encoder` linking `/usr/hobot/lib/libmultimedia.so`.

---

## Backlog (Post-0.3.0)

The following release-delivery capabilities are intentionally deferred to the post-0.3.0 backlog, with extension hooks already reserved in `crates/miro-ego-deploy/src/bundle.rs` and `/data/updates/`:

- **Incremental CAS-Layered OTA Updates**: content-addressed object layers (`StoreLayer` / `LocalCasStore`) and signed remote channel publishing (OSS / CDN).
- **Offline Recording-Disk Update Packages**: discovering, verifying detached signatures, and applying bundles placed by operators under `/data/updates/` (`OfflinePackageHook` / `DefaultOfflinePackageHook`).

---

## Branch Maintenance & Archived Engineering History

- Dual-branch policy and cutover gates: [`docs/branch-maintenance-0.3.md`](docs/branch-maintenance-0.3.md)
- Audit & architecture design records:
  - [`docs/reports/2026-11-25-full-audit-and-refactor-plan.md`](docs/reports/2026-11-25-full-audit-and-refactor-plan.md)
  - [`docs/reports/2026-11-26-round2-perfection-audit.md`](docs/reports/2026-11-26-round2-perfection-audit.md)
  - [`docs/reports/2026-09-15-recording-recovery.md`](docs/reports/2026-09-15-recording-recovery.md)
  - [`docs/reports/2026-09-15-recording-ablation.md`](docs/reports/2026-09-15-recording-ablation.md)
- Archived historical benchmarks (`experiments/`, non-production):
  - [`experiments/fixed-trace-20260905-132783d/README.md`](experiments/fixed-trace-20260905-132783d/README.md)
  - [`experiments/code-structure-20260908/README.md`](experiments/code-structure-20260908/README.md)
