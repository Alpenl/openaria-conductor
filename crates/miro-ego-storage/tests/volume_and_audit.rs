use miro_ego_capture::session::schema_validation::SchemaValidator;
use miro_ego_core::paths::{DataVolumePaths, RECORDING_VOLUME_LABEL};
use miro_ego_storage::audit::{ApiAuditEvent, AuditConfig, AuditLogger, AuditWriteTarget};
use miro_ego_storage::volume::{ExfatVolumeManager, FsCapacity, VolumePolicy, VolumeProbe};
use miro_ego_storage::{VOLUME_METADATA_SCHEMA, VolumeManager};
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

struct TempVolume {
    root: PathBuf,
}

impl TempVolume {
    fn new(tag: &str) -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "miro-ego-storage-vol-{tag}-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        Self { root }
    }

    fn paths(&self) -> DataVolumePaths {
        DataVolumePaths::new(&self.root)
    }
}

impl Drop for TempVolume {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[derive(Clone)]
struct MockProbe {
    cap: Arc<Mutex<FsCapacity>>,
    system: Arc<Mutex<(u64, u64)>>,
}

impl MockProbe {
    fn for_dir(dir: &Path, available_bytes: u64) -> Self {
        let dev = fs::metadata(dir).unwrap().dev();
        Self {
            cap: Arc::new(Mutex::new(FsCapacity {
                total_bytes: 64 * 1024 * 1024 * 1024,
                available_bytes,
                available_inodes: 100_000,
                read_only: false,
                writable: true,
                is_exfat: true,
                device_id: dev,
                mount_identity: format!("mount:42:{dev}"),
                mounted: true,
            })),
            system: Arc::new(Mutex::new((4 * 1024 * 1024 * 1024, 100_000))),
        }
    }

    fn update(&self, f: impl FnOnce(&mut FsCapacity)) {
        let mut guard = self.cap.lock().unwrap();
        f(&mut guard);
    }
}

impl VolumeProbe for MockProbe {
    fn probe_volume(&self, _mountpoint: &Path) -> io::Result<FsCapacity> {
        Ok(self.cap.lock().unwrap().clone())
    }

    fn probe_system(&self, _state_root: &Path) -> io::Result<(u64, u64)> {
        Ok(*self.system.lock().unwrap())
    }
}

#[test]
fn volume_init_creates_canonical_layout_and_preserves_volume_id() {
    let tmp = TempVolume::new("init");
    let probe = MockProbe::for_dir(&tmp.root, 16 * 1024 * 1024 * 1024);
    let mut policy = VolumePolicy::production(2 * 1024 * 1024 * 1024, 1024);
    policy.system_state_root = Some(tmp.root.clone());
    policy.expected_uuid = Some("ABCD-1234".to_owned());
    policy.expected_partuuid = Some("12345678-03".to_owned());

    let mgr = ExfatVolumeManager::with_probe(tmp.paths(), policy, Arc::new(probe));
    let _hidden = mgr.ensure_layout().unwrap();

    for expected in [
        tmp.paths().recordings_dir(),
        tmp.paths().logs_dir(),
        tmp.paths().updates_dir(),
        tmp.paths().hidden_meta_dir(),
        tmp.paths().index_dir(),
        tmp.paths().staging_dir(),
        tmp.paths().quarantine_dir(),
        tmp.paths().audit_dir(),
        tmp.paths().receipts_dir(),
        tmp.paths().telemetry_dir(),
    ] {
        assert!(expected.is_dir(), "missing dir: {}", expected.display());
    }

    let meta1 = mgr.read_metadata().unwrap();
    assert_eq!(meta1.schema, VOLUME_METADATA_SCHEMA);
    assert_eq!(meta1.label, RECORDING_VOLUME_LABEL);
    assert!(miro_ego_core::ids::is_valid_uuid_v4(&meta1.volume_id));

    // Re-running ensure_layout must preserve volume_id idempotently.
    let _ = mgr.ensure_layout().unwrap();
    let meta2 = mgr.read_metadata().unwrap();
    assert_eq!(meta1.volume_id, meta2.volume_id);

    let info = mgr.inspect_volume().unwrap();
    assert_eq!(info.label, RECORDING_VOLUME_LABEL);
    assert!(info.is_exfat);
    assert_eq!(info.uuid, "ABCD-1234");
    assert_eq!(info.partuuid, "12345678-03");

    let reserve = mgr.check_reserve().unwrap();
    assert!(reserve.ok);
    assert_eq!(reserve.code, "ready");
    assert!(reserve.recording.is_some());
    assert!(reserve.system.is_some());
}

#[test]
fn volume_fails_closed_when_unmounted_non_exfat_readonly_or_low_space() {
    let tmp = TempVolume::new("failclosed");
    let probe = MockProbe::for_dir(&tmp.root, 16 * 1024 * 1024 * 1024);
    let mut policy = VolumePolicy::production(2 * 1024 * 1024 * 1024, 1024);
    policy.system_state_root = Some(tmp.root.clone());
    let mgr = ExfatVolumeManager::with_probe(tmp.paths(), policy, Arc::new(probe.clone()));
    let _ = mgr.ensure_layout().unwrap();

    // Unmounted -> fail-closed (never falls back to system disk).
    probe.update(|c| c.mounted = false);
    assert!(mgr.inspect_volume().is_err());
    let res = mgr.check_reserve().unwrap();
    assert!(!res.ok);
    assert_eq!(res.code, "storage_unavailable");
    assert!(res.recording.is_none());

    // Non-exFAT -> fail-closed.
    probe.update(|c| {
        c.mounted = true;
        c.is_exfat = false;
    });
    assert_eq!(mgr.inspect_volume().unwrap_err().code(), "volume_invalid");

    // Read-only (errors=remount-ro) -> fail-closed.
    probe.update(|c| {
        c.is_exfat = true;
        c.read_only = true;
        c.writable = false;
    });
    assert!(mgr.inspect_volume().is_err());
    assert!(!mgr.check_reserve().unwrap().ok);

    // Writable exFAT but below minimum_available_bytes -> insufficient_space.
    probe.update(|c| {
        c.read_only = false;
        c.writable = true;
        c.available_bytes = 512 * 1024 * 1024;
    });
    let low = mgr.check_reserve().unwrap();
    assert!(!low.ok);
    assert_eq!(low.code, "insufficient_space");

    // Below minimum_available_inodes -> insufficient_inodes.
    probe.update(|c| {
        c.available_bytes = 10 * 1024 * 1024 * 1024;
        c.available_inodes = 10;
    });
    let low_inodes = mgr.check_reserve().unwrap();
    assert!(!low_inodes.ok);
    assert_eq!(low_inodes.code, "insufficient_inodes");
}

#[test]
fn audit_logger_rotates_bounded_generations_and_validates_against_schema() {
    let tmp = TempVolume::new("audit-rot");
    let _ = tmp.paths().ensure_layout().unwrap();

    let schema_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../contracts/schemas/miro-ego-api-audit-event-v1.schema.json");
    let schema_text = fs::read_to_string(schema_path).unwrap();
    let validator = SchemaValidator::new(&schema_text).unwrap();

    let logger = AuditLogger::new(
        tmp.paths(),
        AuditConfig {
            max_file_bytes: 600,
            max_generations: 2,
            fallback_ring_bytes: 8 * 1024,
        },
        Arc::new(|| true),
    );

    for idx in 0..12 {
        let ev = ApiAuditEvent::new(
            format!("req-{idx:04}"),
            Some("operator-1".to_owned()),
            "startCapture",
            Some(format!("session-{idx}")),
            "allowed",
        );
        let serialized = serde_json::to_string(&ev).unwrap();
        assert!(
            validator.is_valid(&serialized).unwrap(),
            "audit event failed schema validation: {serialized}"
        );
        let target = logger.record(&ev).unwrap();
        assert_eq!(target, AuditWriteTarget::Disk);
        assert!(target.header_status().is_none());
    }

    // Active file + 1 rotated backup exist; generation 2 must NOT exist (strictly <= 2 gens).
    assert!(logger.active_log_path().is_file());
    assert!(logger.rotated_log_path(1).is_file());
    assert!(!logger.rotated_log_path(2).exists());
    assert!(fs::metadata(logger.active_log_path()).unwrap().len() <= 600);
    assert!(fs::metadata(logger.rotated_log_path(1)).unwrap().len() <= 600);
}

#[test]
fn audit_logger_falls_back_to_ram_ring_when_unmounted_with_zero_disk_writes_then_flushes() {
    let tmp = TempVolume::new("audit-unmounted");
    // Deliberately do NOT create `.miro-ego/audit` yet — simulates unmounted `/data`.
    let mounted = Arc::new(AtomicBool::new(false));
    let mounted_flag = Arc::clone(&mounted);

    let logger = AuditLogger::new(
        tmp.paths(),
        AuditConfig {
            max_file_bytes: 16 * 1024,
            max_generations: 2,
            fallback_ring_bytes: 4096,
        },
        Arc::new(move || mounted_flag.load(Ordering::Acquire)),
    );

    for idx in 0..5 {
        let ev = ApiAuditEvent::new(
            format!("offline-req-{idx}"),
            None,
            "getCaptureStatus",
            None,
            "allowed",
        );
        let target = logger.record(&ev).unwrap();
        assert_eq!(target, AuditWriteTarget::MemoryFallback);
        assert_eq!(target.header_status(), Some("unavailable"));
    }

    // ZERO files or directories created on unmounted path!
    assert!(!tmp.paths().hidden_meta_dir().exists());
    assert_eq!(logger.fallback_stats().unwrap().entry_count, 5);

    // Mount `/data` and create layout -> next audit write flushes all 5 buffered entries first.
    let _ = tmp.paths().ensure_layout().unwrap();
    mounted.store(true, Ordering::Release);

    let online_ev = ApiAuditEvent::new("online-req", None, "listSessions", None, "allowed");
    assert_eq!(logger.record(&online_ev).unwrap(), AuditWriteTarget::Disk);
    assert_eq!(logger.fallback_stats().unwrap().entry_count, 0);

    let contents = fs::read_to_string(logger.active_log_path()).unwrap();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 6);
    assert!(lines[0].contains("offline-req-0"));
    assert!(lines[5].contains("online-req"));
}

#[test]
fn util_crc32_base64url_and_rfc3339_helpers_are_exact() {
    use miro_ego_storage::util::{
        base64url_decode, base64url_encode, crc32_ieee, readable_date_time_from_rfc3339,
        unix_secs_to_rfc3339_utc,
    };

    // Standard IEEE 802.3 CRC32 check vector.
    assert_eq!(crc32_ieee(b"123456789"), 0xcbf4_3926);
    assert_eq!(crc32_ieee(b""), 0);

    // Base64url round-trip for all remainder classes + invalid input rejection.
    for sample in [&b""[..], b"a", b"ab", b"abc", b"hello-miro-ego-0.3.0!"] {
        let encoded = base64url_encode(sample);
        if !sample.is_empty() {
            assert_eq!(base64url_decode(&encoded).unwrap(), sample);
        }
    }
    assert!(base64url_decode("").is_err());
    assert!(base64url_decode("a").is_err());
    assert!(base64url_decode("invalid+/=").is_err());

    // Exact Gregorian UTC conversions (epoch, leap day 2024-02-29, 2026-11-26).
    assert_eq!(unix_secs_to_rfc3339_utc(0), "1970-01-01T00:00:00Z");
    assert_eq!(
        unix_secs_to_rfc3339_utc(1_709_208_000),
        "2024-02-29T12:00:00Z"
    );

    assert_eq!(
        readable_date_time_from_rfc3339("2026-11-26T10:25:01Z"),
        Some(("2026-11-26".to_owned(), "10-25-01".to_owned()))
    );
    assert_eq!(
        readable_date_time_from_rfc3339("2026-11-26T18:25:01.123+08:00"),
        Some(("2026-11-26".to_owned(), "18-25-01".to_owned()))
    );
    assert!(readable_date_time_from_rfc3339("2019-01-01T00:00:00Z").is_none());
    assert!(readable_date_time_from_rfc3339("2026-11-26T10:25:01").is_none());
    assert!(readable_date_time_from_rfc3339("2026-02-29T10:25:01Z").is_none());
    assert!(readable_date_time_from_rfc3339("2026-02-31T10:25:01Z").is_none());
}

#[test]
fn volume_admit_enforces_expected_uuid_and_partuuid_and_sweeps_stale_tmps() {
    use miro_ego_storage::util::parse_rfc3339_utc_nanos;
    use miro_ego_storage::volume::PosixVolumeProbe;

    // PosixVolumeProbe on local ext4/tmpfs test directory must NOT claim exFAT.
    let tmp = TempVolume::new("uuid-sweep");
    let real_cap = PosixVolumeProbe.probe_volume(&tmp.root).unwrap();
    assert!(!real_cap.is_exfat);

    // Offset normalization check (`+08:00` vs `Z`).
    assert_eq!(
        parse_rfc3339_utc_nanos("2026-11-26T20:00:00+08:00"),
        parse_rfc3339_utc_nanos("2026-11-26T12:00:00Z")
    );
    assert!(parse_rfc3339_utc_nanos("2024-02-29T23:59:59.123456789Z").is_some());
    assert!(parse_rfc3339_utc_nanos("2025-02-29T00:00:00Z").is_none());

    let probe = MockProbe::for_dir(&tmp.root, 16 * 1024 * 1024 * 1024);
    let mut init_policy = VolumePolicy::production(2 * 1024 * 1024 * 1024, 1024);
    init_policy.system_state_root = Some(tmp.root.clone());
    // Simulate `volume init` creating `volume.json` without uuid/partuuid first.
    let mgr_no_uuid =
        ExfatVolumeManager::with_probe(tmp.paths(), init_policy, Arc::new(probe.clone()));
    let _ = mgr_no_uuid.ensure_layout().unwrap();

    // Direct `admit()` with `expected_uuid` configured MUST fail when `volume.json` lacks it.
    let mut strict_policy = VolumePolicy::production(2 * 1024 * 1024 * 1024, 1024);
    strict_policy.system_state_root = Some(tmp.root.clone());
    strict_policy.expected_uuid = Some("CAFE-9999".to_owned());
    strict_policy.expected_partuuid = Some("deadbeef-01".to_owned());
    let mgr_strict =
        ExfatVolumeManager::with_probe(tmp.paths(), strict_policy.clone(), Arc::new(probe.clone()));
    assert_eq!(mgr_strict.admit(None).unwrap_err().code(), "volume_invalid");

    // Plant orphan `.*.tmp` crash leftovers in `.miro-ego`, `index/`, and `receipts/`.
    let stale1 = tmp.paths().hidden_meta_dir().join(".volume.json.999.0.tmp");
    let stale2 = tmp.paths().index_dir().join(".catalog.bin.999.1.tmp");
    let stale3 = tmp
        .paths()
        .receipts_dir()
        .join(".coordinator-receipts.json.999.2.tmp");
    fs::write(&stale1, b"orphan").unwrap();
    fs::write(&stale2, b"orphan").unwrap();
    fs::write(&stale3, b"orphan").unwrap();

    // Running `ensure_layout()` sweeps stale `.tmp` files and backfills missing uuid/partuuid.
    let _ = mgr_strict.ensure_layout().unwrap();
    assert!(!stale1.exists());
    assert!(!stale2.exists());
    assert!(!stale3.exists());
    let admitted = mgr_strict.admit(None).unwrap();
    assert_eq!(admitted.uuid, "CAFE-9999");
    assert_eq!(admitted.partuuid, "deadbeef-01");

    // Mismatched expected_uuid / expected_partuuid MUST fail closed with VolumeInvalid!
    let mut wrong_uuid_policy = strict_policy.clone();
    wrong_uuid_policy.expected_uuid = Some("WRONG-0000".to_owned());
    let mgr_wrong = ExfatVolumeManager::with_probe(tmp.paths(), wrong_uuid_policy, Arc::new(probe));
    assert_eq!(mgr_wrong.admit(None).unwrap_err().code(), "volume_invalid");
}

#[test]
fn audit_logger_repairs_partial_tail_line_on_reopen_and_skips_truncated_fallback_markers() {
    let tmp = TempVolume::new("audit-repair");
    let _ = tmp.paths().ensure_layout().unwrap();

    let schema_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../contracts/schemas/miro-ego-api-audit-event-v1.schema.json");
    let schema_text = fs::read_to_string(schema_path).unwrap();
    let validator = SchemaValidator::new(&schema_text).unwrap();

    // 1. Write one valid event, then append an unterminated partial line simulating power loss.
    {
        let logger1 = AuditLogger::new(tmp.paths(), AuditConfig::default(), Arc::new(|| true));
        let ev1 = ApiAuditEvent::new("req-clean-1", None, "startCapture", None, "allowed");
        assert_eq!(logger1.record(&ev1).unwrap(), AuditWriteTarget::Disk);
    }
    let active_path = tmp.paths().audit_dir().join("api-audit.ndjson");
    {
        use std::io::Write;
        let mut f = fs::OpenOptions::new()
            .append(true)
            .open(&active_path)
            .unwrap();
        f.write_all(b"{\"schema\":\"miro-ego.api-audit-event.v1\",\"partial_power_loss")
            .unwrap();
    }

    // 2. Reopen AuditLogger and append a new event: partial tail MUST be repaired before append!
    let mounted = Arc::new(AtomicBool::new(true));
    let mounted_flag = Arc::clone(&mounted);
    let logger2 = AuditLogger::new(
        tmp.paths(),
        AuditConfig {
            max_file_bytes: 64 * 1024,
            max_generations: 2,
            // 1024-byte ring so an >1024-byte serialized JSON entry produces `{"truncated":true}`.
            fallback_ring_bytes: 1024,
        },
        Arc::new(move || mounted_flag.load(Ordering::Acquire)),
    );
    let ev2 = ApiAuditEvent::new("req-clean-2", None, "stopCapture", None, "allowed");
    assert_eq!(logger2.record(&ev2).unwrap(), AuditWriteTarget::Disk);

    // 3. Simulate offline oversized event (> 1024 B via `\u0001` JSON escaping) -> truncated marker in ring,
    //    followed by a normal compact offline event.
    mounted.store(false, Ordering::Release);
    let huge_ev = ApiAuditEvent::new(
        "req-huge-offline",
        None,
        "deleteSessions",
        Some("\u{0001}".repeat(300)),
        "allowed",
    );
    let _ = logger2.record(&huge_ev).unwrap();
    assert_eq!(
        logger2.fallback_snapshot().unwrap()[0]
            .payload
            .get("truncated"),
        Some(&serde_json::Value::Bool(true))
    );
    let normal_offline = ApiAuditEvent::new("req-offline-ok", None, "listLogs", None, "allowed");
    let _ = logger2.record(&normal_offline).unwrap();

    // Remount and flush: `{"truncated":true}` marker MUST be skipped, keeping 100% schema-valid lines!
    mounted.store(true, Ordering::Release);
    let _ = logger2.flush_fallback_to_disk().unwrap();

    let final_text = fs::read_to_string(&active_path).unwrap();
    let lines: Vec<&str> = final_text.lines().collect();
    assert_eq!(lines.len(), 3, "lines: {lines:?}");
    for line in &lines {
        assert!(
            validator.is_valid(line).unwrap(),
            "corrupt audit line in api-audit.ndjson: {line}"
        );
    }
    assert!(lines[0].contains("req-clean-1"));
    assert!(lines[1].contains("req-clean-2"));
    assert!(lines[2].contains("req-offline-ok"));
}

#[test]
fn safe_swap_volume_release_flag_handle_leases_and_audit_close_work_cleanly() {
    let tmp = TempVolume::new("vol-release-leases");
    let probe = MockProbe::for_dir(&tmp.root, 16 * 1024 * 1024 * 1024);
    let mut policy = VolumePolicy::production(2 * 1024 * 1024 * 1024, 1024);
    policy.system_state_root = Some(tmp.root.clone());
    let mgr = ExfatVolumeManager::with_probe(tmp.paths(), policy, Arc::new(probe));
    let _ = mgr.ensure_layout().unwrap();

    assert!(!mgr.is_volume_released());
    assert!(mgr.is_mounted_and_writable());
    assert!(mgr.is_raw_mounted_and_writable());
    assert_eq!(mgr.open_handle_count(), 0);

    let lease1 = mgr.acquire_handle_lease();
    let lease2 = lease1.clone();
    assert_eq!(mgr.open_handle_count(), 2);
    drop(lease1);
    assert_eq!(mgr.open_handle_count(), 1);
    drop(lease2);
    assert_eq!(mgr.open_handle_count(), 0);

    // Marking volume released hides writability from normal callers while raw probe still sees mount.
    mgr.set_volume_released(true);
    assert!(mgr.is_volume_released());
    assert!(!mgr.is_mounted_and_writable());
    assert!(mgr.is_raw_mounted_and_writable());

    let logger = AuditLogger::new(tmp.paths(), AuditConfig::default(), Arc::new(|| true));
    let ev = ApiAuditEvent::new("req-open-fd", None, "getCaptureStatus", None, "allowed");
    assert_eq!(logger.record(&ev).unwrap(), AuditWriteTarget::Disk);
    assert!(logger.has_open_file());
    logger.close_active_file();
    assert!(!logger.has_open_file());
}

#[test]
fn audit_logger_validates_recorded_at_resets_fd_on_error_and_peeks_front_batch_on_enospc() {
    use std::time::Duration;

    let tmp = TempVolume::new("audit-enospc-peek");
    let _ = tmp.paths().ensure_layout().unwrap();

    // P2-13: ApiAuditEvent::validate rejects malformed `recorded_at`.
    let mut bad_ev = ApiAuditEvent::new("req-ts", None, "listLogs", None, "allowed");
    bad_ev.recorded_at = "not-a-valid-rfc3339-timestamp".to_owned();
    assert_eq!(bad_ev.validate().unwrap_err().code(), "invalid_audit_event");
    bad_ev.recorded_at = "2026-02-30T10:00:00Z".to_owned();
    assert_eq!(bad_ev.validate().unwrap_err().code(), "invalid_audit_event");

    // P1-11: Non-destructive peek-then-pop fallback flush + ENOSPC backoff + active_file reset on error.
    let writable = Arc::new(AtomicBool::new(false));
    let writable_flag = Arc::clone(&writable);
    let logger = AuditLogger::new(
        tmp.paths(),
        AuditConfig {
            max_file_bytes: 16 * 1024,
            max_generations: 2,
            fallback_ring_bytes: 16 * 1024,
        },
        Arc::new(move || writable_flag.load(Ordering::Acquire)),
    )
    .with_enospc_backoff(Duration::from_millis(40));

    for idx in 0..3 {
        let ev = ApiAuditEvent::new(format!("queued-{idx}"), None, "listLogs", None, "allowed");
        assert_eq!(
            logger.record(&ev).unwrap(),
            AuditWriteTarget::MemoryFallback
        );
    }
    assert_eq!(logger.fallback_stats().unwrap().entry_count, 3);

    // Replace `audit/api-audit.ndjson` with a directory so opening for disk flush fails with EISDIR!
    let active_path = logger.active_log_path();
    fs::create_dir_all(&active_path).unwrap();
    writable.store(true, Ordering::Release);

    // Flush fails, resets `active_file = None`, and preserves exact FIFO order [queued-0, queued-1, queued-2] in ring!
    assert!(logger.flush_fallback_to_disk().is_err());
    assert!(!logger.has_open_file());
    let snap = logger.fallback_snapshot().unwrap();
    assert_eq!(snap.len(), 3);
    assert_eq!(snap[0].payload["request_id"], "queued-0");
    assert_eq!(snap[1].payload["request_id"], "queued-1");
    assert_eq!(snap[2].payload["request_id"], "queued-2");

    // Fix the obstruction -> flush now succeeds in exact original FIFO order!
    fs::remove_dir(&active_path).unwrap();
    assert_eq!(logger.flush_fallback_to_disk().unwrap(), 3);
    assert_eq!(logger.fallback_stats().unwrap().entry_count, 0);
    let disk_lines: Vec<String> = fs::read_to_string(&active_path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(disk_lines.len(), 3);
    assert!(disk_lines[0].contains("queued-0"));
    assert!(disk_lines[1].contains("queued-1"));
    assert!(disk_lines[2].contains("queued-2"));
}

#[test]
fn volume_admit_rejects_subdir_symlinks_reads_env_policy_and_clamps_pre1970_timestamps() {
    use miro_ego_storage::util::{
        parse_rfc3339_utc_nanos, system_time_to_rfc3339_utc, unix_secs_to_rfc3339_utc,
    };
    use std::os::unix::fs::symlink;
    use std::time::Duration;

    // P3-03: Pre-1970 timestamps clamp consistently to `1970-01-01T00:00:00Z` (`0` nanos).
    assert_eq!(unix_secs_to_rfc3339_utc(-86_400), "1970-01-01T00:00:00Z");
    assert_eq!(
        system_time_to_rfc3339_utc(UNIX_EPOCH - Duration::from_secs(500)),
        "1970-01-01T00:00:00Z"
    );
    assert_eq!(parse_rfc3339_utc_nanos("1969-12-31T23:59:00Z"), Some(0));
    assert_eq!(
        parse_rfc3339_utc_nanos("1970-01-01T01:00:00+02:00"),
        Some(0)
    );

    // P2-14: Environment overrides populate expected UUID / PARTUUID cleanly.
    let mut pol = VolumePolicy::production(1024, 10);
    pol.apply_volume_identity_overrides(Some("  DEAD-BEEF "), Some(" part-99 "));
    assert_eq!(pol.expected_uuid.as_deref(), Some("DEAD-BEEF"));
    assert_eq!(pol.expected_partuuid.as_deref(), Some("part-99"));

    // P2-14: `ExfatVolumeManager::admit` rejects symlinks on canonical `.miro-ego/*` & `logs/` subdirs.
    let tmp = TempVolume::new("subdir-symlink");
    let probe = MockProbe::for_dir(&tmp.root, 16 * 1024 * 1024 * 1024);
    let mut policy = VolumePolicy::production(1024 * 1024, 100);
    policy.system_state_root = Some(tmp.root.clone());
    let mgr = ExfatVolumeManager::with_probe(tmp.paths(), policy, Arc::new(probe));
    let _ = mgr.ensure_layout().unwrap();
    assert!(mgr.admit(None).is_ok());

    let outside = std::env::temp_dir().join(format!("miro-outside-{}", std::process::id()));
    fs::create_dir_all(&outside).unwrap();
    fs::remove_dir_all(tmp.paths().logs_dir()).unwrap();
    symlink(&outside, tmp.paths().logs_dir()).unwrap();
    assert_eq!(mgr.admit(None).unwrap_err().code(), "volume_invalid");
    let _ = fs::remove_dir_all(&outside);
}
