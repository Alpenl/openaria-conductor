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
}
