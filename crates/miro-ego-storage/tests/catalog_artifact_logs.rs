use miro_ego_core::ids::{new_uuid_v4, new_uuid_v7};
use miro_ego_core::paths::DataVolumePaths;
use miro_ego_storage::artifact_io::{
    open_manifest, open_verified_artifact, parse_single_byte_range,
};
use miro_ego_storage::catalog::{
    BinarySessionCatalog, DEVICE_SESSION_SCHEMA, DiscoveryDiagnosticCode,
};
use miro_ego_storage::logs::{LOG_LIST_SCHEMA, LogStore};
use miro_ego_storage::util::sha256_hex;
use miro_ego_storage::{CATALOG_BINARY_MAGIC, SessionCatalog, StorageError};
use serde_json::json;
use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

struct TempVol {
    root: PathBuf,
}

impl TempVol {
    fn new(tag: &str) -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "miro-ego-storage-cat-{tag}-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        let paths = DataVolumePaths::new(&root);
        let _ = paths.ensure_layout().unwrap();
        Self { root }
    }

    fn paths(&self) -> DataVolumePaths {
        DataVolumePaths::new(&self.root)
    }
}

impl Drop for TempVol {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn write_sealed_session(
    recordings_root: &Path,
    rel_dir: &str,
    session_id: &str,
    take_id: &str,
    started_at: &str,
    recovered_prefix: bool,
) -> (String, String, Vec<u8>) {
    let dir = recordings_root.join(rel_dir);
    fs::create_dir_all(dir.join("video")).unwrap();

    let left_bytes = b"LEFT-MP4-BYTES-0123456789abcdef".to_vec();
    let right_bytes = b"RIGHT-MP4-BYTES-fedcba9876543210".to_vec();
    let frames_bytes = b"{\"frame\":0}\n".to_vec();
    let imu_bytes = b"{\"seq\":0}\n".to_vec();

    fs::write(dir.join("video/left_00000.mp4"), &left_bytes).unwrap();
    fs::write(dir.join("video/right_00000.mp4"), &right_bytes).unwrap();
    fs::write(dir.join("frames.ndjson"), &frames_bytes).unwrap();
    fs::write(dir.join("imu.ndjson"), &imu_bytes).unwrap();

    let left_sha = sha256_hex(&left_bytes);
    let right_sha = sha256_hex(&right_bytes);
    let frames_sha = sha256_hex(&frames_bytes);
    let imu_sha = sha256_hex(&imu_bytes);

    let mut manifest = json!({
        "schema": DEVICE_SESSION_SCHEMA,
        "manifest_id": new_uuid_v7(),
        "sealed": true,
        "sealed_at": started_at,
        "session_id": session_id,
        "volume_id": new_uuid_v4(),
        "capture_mode": "production",
        "display_name": format!("Take {rel_dir}"),
        "device": {
            "device_id": new_uuid_v4(),
            "device_label": "Miro-Ego-1234ABCD",
            "hardware_fingerprint": format!("sha256:{}", "a".repeat(64)),
            "platform": "rdk-x5",
            "software_version": "0.3.0",
            "commit": "b".repeat(40),
        },
        "time": {
            "started_at": started_at,
            "ended_at": started_at,
            "timezone": "Etc/UTC",
            "duration_seconds": 5.0,
            "duration_clock": "host_monotonic",
        },
        "take": {
            "take_id": take_id,
            "sequence": 1,
            "continuation_of": null,
        },
        "camera": {
            "width": 3840,
            "height": 1080,
            "eye_width": 1920,
            "sensor_fps": 60.0,
            "frame_decimation": 2,
            "effective_fps": 30.0,
            "coordinate_frame": "opencv_optical",
        },
        "video": {
            "layout": "split-eyes",
            "codec": "hevc",
            "container": "mp4",
            "segments": [{
                "index": 0,
                "start_frame": 0,
                "end_frame": 150,
                "start_time_seconds": 0.0,
                "end_time_seconds": 5.0,
                "artifacts": {
                    "left": {
                        "artifact_id": left_sha,
                        "role": "video.left",
                        "path": "video/left_00000.mp4",
                        "media_type": "video/mp4",
                        "bytes": left_bytes.len(),
                        "sha256": left_sha,
                    },
                    "right": {
                        "artifact_id": right_sha,
                        "role": "video.right",
                        "path": "video/right_00000.mp4",
                        "media_type": "video/mp4",
                        "bytes": right_bytes.len(),
                        "sha256": right_sha,
                    }
                }
            }],
            "encoding": {
                "schema": "miro-ego.recording-encoding.v1",
                "codec": "hevc",
                "rate_control": "cbr",
                "profile": "main",
                "pixel_format": "yuv420p",
                "bit_depth": 8,
                "b_frames": 0,
                "bitrate_kbps": 12288,
                "min_qp": 18,
                "max_qp": 32,
                "intra_qp": 20,
                "initial_qp": 22,
                "gop_frames": 30,
                "vbv_ms": 3000,
            }
        },
        "imu": {
            "artifact": {
                "artifact_id": imu_sha,
                "role": "imu.samples",
                "path": "imu.ndjson",
                "media_type": "application/x-ndjson",
                "bytes": imu_bytes.len(),
                "sha256": imu_sha,
            },
            "sample_count": 1,
            "units": "raw_int16",
            "coordinate_frame": "raw_device_axes",
        },
        "frames": {
            "artifact": {
                "artifact_id": frames_sha,
                "role": "frames.index",
                "path": "frames.ndjson",
                "media_type": "application/x-ndjson",
                "bytes": frames_bytes.len(),
                "sha256": frames_sha,
            },
            "count": 1,
        },
        "audio": {
            "state": "not_recorded",
            "requested_mode": "disabled",
            "resolved_mode": "disabled",
            "reason": "user_disabled",
        },
        "logs": [],
        "integrity": {
            "verified_at": started_at,
            "dropped_frames": 0,
            "drop_events": [],
            "fatal_errors": [],
        }
    });

    if recovered_prefix {
        manifest["capture_audit"] = json!({
            "schema": "miro-ego.capture-audit.v1",
            "recovery": {
                "recovered_prefix": true,
                "recovered_segments": 1,
            }
        });
    }

    let raw = serde_json::to_vec_pretty(&manifest).unwrap();
    fs::write(dir.join("manifest.json"), &raw).unwrap();
    (sha256_hex(&raw), left_sha, left_bytes)
}

#[test]
fn catalog_bin_corruption_auto_rebuilds_and_supports_deterministic_pagination() {
    let tmp = TempVol::new("rebuild-page");
    let rec_root = tmp.paths().recordings_dir();

    let s1 = new_uuid_v7();
    let s2 = new_uuid_v7();
    let s3 = new_uuid_v7();
    let t1 = new_uuid_v7();
    let t2 = new_uuid_v7();

    write_sealed_session(
        &rec_root,
        "2026-11-26/10-00-01",
        &s1,
        &t1,
        "2026-11-26T10:00:01Z",
        false,
    );
    write_sealed_session(
        &rec_root,
        "2026-11-26/10-00-02",
        &s2,
        &t2,
        "2026-11-26T10:00:02Z",
        true,
    );
    write_sealed_session(
        &rec_root,
        "2026-11-26/10-00-03",
        &s3,
        &t1,
        "2026-11-26T10:00:03Z",
        false,
    );

    // Also add one corrupt recording to confirm diagnostics pagination alongside normal items.
    let bad_dir = rec_root.join("2026-11-26/10-00-04");
    fs::create_dir_all(&bad_dir).unwrap();
    fs::write(bad_dir.join("manifest.json"), b"{not valid json").unwrap();

    // Missing catalog.bin -> auto-rebuilds on open and persists framed MEGOCAT1 file.
    let cat = BinarySessionCatalog::open(tmp.paths()).unwrap();
    let bin_bytes = fs::read(tmp.paths().catalog_file()).unwrap();
    assert_eq!(&bin_bytes[0..8], &CATALOG_BINARY_MAGIC);
    assert_eq!(cat.list_sessions().unwrap().len(), 3);

    // Corrupt a payload byte (triggers CRC32 mismatch) -> reopen must auto-rebuild cleanly!
    let mut corrupted = bin_bytes.clone();
    let last = corrupted.len() - 1;
    corrupted[last] ^= 0xff;
    fs::write(tmp.paths().catalog_file(), &corrupted).unwrap();
    let rebuilt = BinarySessionCatalog::open(tmp.paths()).unwrap();
    assert_eq!(rebuilt.list_sessions().unwrap().len(), 3);

    // Truncated / bad magic -> also auto-rebuilds cleanly.
    fs::write(tmp.paths().catalog_file(), b"BADMAGIC").unwrap();
    let rebuilt2 = BinarySessionCatalog::open(tmp.paths()).unwrap();
    assert_eq!(rebuilt2.list_sessions().unwrap().len(), 3);

    // Deterministic pagination newest-first across 3 items + 1 diagnostic (page size 2).
    let page1 = rebuilt2
        .list_sessions_page(Some(2), None, false, None)
        .unwrap();
    assert_eq!(page1.items.len(), 2);
    assert!(page1.diagnostics.is_empty());
    assert_eq!(page1.items[0].session_id, s3);
    assert_eq!(page1.items[1].session_id, s2);
    // include_recovery=false MUST omit recovered_prefix even for recovered session s2.
    assert_eq!(page1.items[1].recovered_prefix, None);
    let cursor1 = page1.next_cursor.as_deref().unwrap();

    // Prefer: include-recovery marks recovered_prefix=Some(true) on s2.
    let summary_with_rec = rebuilt2.session_summary(&s2, true).unwrap().unwrap();
    assert_eq!(summary_with_rec.recovered_prefix, Some(true));
    let summary_without_rec = rebuilt2.session_summary(&s2, false).unwrap().unwrap();
    assert_eq!(summary_without_rec.recovered_prefix, None);

    // Mismatched filter with cursor -> 400 invalid_cursor.
    let filter_mismatch = rebuilt2
        .list_sessions_page(Some(2), Some(cursor1), true, None)
        .unwrap_err();
    assert_eq!(filter_mismatch.code(), "invalid_cursor");
    assert_eq!(filter_mismatch.http_status(), 400);

    // Valid page 2 returns remaining item s1 + 1 diagnostic and next_cursor = None.
    let page2 = rebuilt2
        .list_sessions_page(Some(2), Some(cursor1), false, None)
        .unwrap();
    assert_eq!(page2.catalog_revision, page1.catalog_revision);
    assert_eq!(page2.items.len(), 1);
    assert_eq!(page2.items[0].session_id, s1);
    assert_eq!(page2.diagnostics.len(), 1);
    assert_eq!(
        page2.diagnostics[0].code,
        DiscoveryDiagnosticCode::ManifestUnreadable
    );
    assert!(page2.next_cursor.is_none());

    // Mutating catalog (forget_path) monotonically bumps catalog_revision and invalidates old cursor (409).
    let rev_before = rebuilt2.revision_seq().unwrap();
    assert!(rebuilt2.forget_path("2026-11-26/10-00-04").unwrap());
    assert!(rebuilt2.revision_seq().unwrap() > rev_before);
    let stale_err = rebuilt2
        .list_sessions_page(Some(2), Some(cursor1), false, None)
        .unwrap_err();
    assert_eq!(stale_err.code(), "catalog_changed");
    assert_eq!(stale_err.http_status(), 409);

    // take_id filter returns only sessions belonging to t1.
    let filtered = rebuilt2
        .list_sessions_page(Some(10), None, false, Some(&t1))
        .unwrap();
    assert_eq!(filtered.items.len(), 2);
    assert_eq!(filtered.items[0].session_id, s3);
    assert_eq!(filtered.items[1].session_id, s1);
}

#[test]
fn catalog_duplicate_session_id_safety_and_concurrency() {
    let tmp = TempVol::new("dup-conc");
    let rec_root = tmp.paths().recordings_dir();

    let dup_sid = new_uuid_v7();
    let take_id = new_uuid_v7();
    write_sealed_session(
        &rec_root,
        "2026-11-26/11-00-01",
        &dup_sid,
        &take_id,
        "2026-11-26T11:00:01Z",
        false,
    );
    write_sealed_session(
        &rec_root,
        "2026-11-26/11-00-02",
        &dup_sid,
        &take_id,
        "2026-11-26T11:00:02Z",
        false,
    );

    let ok_sid = new_uuid_v7();
    write_sealed_session(
        &rec_root,
        "2026-11-26/11-00-03",
        &ok_sid,
        &new_uuid_v7(),
        "2026-11-26T11:00:03Z",
        false,
    );

    // Rebuild detects duplicate session_id across directories -> excludes ambiguous dup_sid
    // from items and emits 2 ManifestInvalid diagnostics while keeping ok_sid healthy.
    let cat = Arc::new(BinarySessionCatalog::open(tmp.paths()).unwrap());
    let page = cat.list_sessions_page(Some(50), None, false, None).unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].session_id, ok_sid);
    assert_eq!(page.diagnostics.len(), 2);
    assert!(
        page.diagnostics
            .iter()
            .all(|d| d.code == DiscoveryDiagnosticCode::ManifestInvalid)
    );

    // Concurrent readers + admitter stress test on RwLock<CatalogIndex>.
    let mut handles = Vec::new();
    for reader_id in 0..4 {
        let c = Arc::clone(&cat);
        handles.push(thread::spawn(move || {
            for _ in 0..25 {
                let p = c.list_sessions_page(Some(20), None, false, None).unwrap();
                assert!(!p.catalog_revision.is_empty(), "reader {reader_id}");
            }
        }));
    }
    let writer_cat = Arc::clone(&cat);
    handles.push(thread::spawn(move || {
        for idx in 10..18 {
            let sid = new_uuid_v7();
            let rel = format!("2026-11-26/12-00-{idx:02}");
            write_sealed_session(
                &writer_cat.paths().recordings_dir(),
                &rel,
                &sid,
                &new_uuid_v7(),
                &format!("2026-11-26T12:00:{idx:02}Z"),
                false,
            );
            let _ = writer_cat.admit_from_session_dir(&rel).unwrap();
        }
    }));
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(cat.list_sessions().unwrap().len(), 9);
}

#[test]
fn artifact_io_enforces_symlink_traversal_tamper_and_range_bounds() {
    let tmp = TempVol::new("artifact-io");
    let rec_root = tmp.paths().recordings_dir();
    let sid = new_uuid_v7();
    let (manifest_sha, left_sha, left_bytes) = write_sealed_session(
        &rec_root,
        "2026-11-26/13-00-00",
        &sid,
        &new_uuid_v7(),
        "2026-11-26T13:00:00Z",
        false,
    );

    // Exact manifest open + SHA-256 preservation.
    let opened_manifest = open_manifest(&rec_root, "2026-11-26/13-00-00", Some(&sid)).unwrap();
    assert_eq!(opened_manifest.sha256, manifest_sha);
    assert_eq!(opened_manifest.etag, format!("\"{manifest_sha}\""));

    // Verified artifact open + Range reading.
    let verified = open_verified_artifact(
        &rec_root,
        "2026-11-26/13-00-00",
        &left_sha,
        Some(&sid),
        true,
    )
    .unwrap();
    let etag = verified.descriptor.etag();
    let total = left_bytes.len() as u64;

    // Valid inclusive range bytes=0-3.
    let r1 = parse_single_byte_range(Some("bytes=0-3"), Some(&etag), &etag, total)
        .unwrap()
        .unwrap();
    assert_eq!(r1.content_range_header(), format!("bytes 0-3/{total}"));
    assert_eq!(verified.read_range_bytes(Some(r1)).unwrap(), b"LEFT");

    // Suffix range bytes=-6.
    let r_suf = parse_single_byte_range(Some("bytes=-6"), None, &etag, total)
        .unwrap()
        .unwrap();
    assert_eq!(verified.read_range_bytes(Some(r_suf)).unwrap(), b"abcdef");

    // If-Range mismatch -> falls back to full representation (Ok(None)).
    let stale_etag = format!("\"{}\"", "0".repeat(64));
    assert!(
        parse_single_byte_range(Some("bytes=0-3"), Some(&stale_etag), &etag, total)
            .unwrap()
            .is_none()
    );

    // Multipart range, out-of-bounds, inverted range, empty file -> 416 RangeNotSatisfiable.
    for bad in [
        "bytes=0-1,2-3",
        "bytes=999-1000",
        "bytes=5-2",
        "bytes=-0",
        "items=0-5",
    ] {
        let err = parse_single_byte_range(Some(bad), None, &etag, total).unwrap_err();
        assert!(matches!(err, StorageError::RangeNotSatisfiable { .. }));
        assert_eq!(err.http_status(), 416);
    }
    assert!(matches!(
        parse_single_byte_range(Some("bytes=0-0"), None, &etag, 0).unwrap_err(),
        StorageError::RangeNotSatisfiable { total_bytes: 0 }
    ));

    // Path traversal rejection.
    for bad_dir in [
        "../.miro-ego",
        "/etc",
        "2026-11-26/../13-00-00",
        "2026-11-26\\13-00-00",
    ] {
        assert!(open_manifest(&rec_root, bad_dir, None).is_err());
    }

    // Tampered artifact content (same byte length, flipped byte) -> digest mismatch.
    let left_path = rec_root.join("2026-11-26/13-00-00/video/left_00000.mp4");
    let mut tampered = left_bytes.clone();
    tampered[0] ^= 0x01;
    fs::write(&left_path, &tampered).unwrap();
    let tamper_err = open_verified_artifact(
        &rec_root,
        "2026-11-26/13-00-00",
        &left_sha,
        Some(&sid),
        true,
    )
    .err()
    .unwrap();
    assert_eq!(tamper_err.code(), "artifact_digest_mismatch");

    // Symlink artifact rejection.
    let outside = tmp.root.join("outside.mp4");
    fs::write(&outside, &left_bytes).unwrap();
    fs::remove_file(&left_path).unwrap();
    symlink(&outside, &left_path).unwrap();
    let sym_err = open_verified_artifact(
        &rec_root,
        "2026-11-26/13-00-00",
        &left_sha,
        Some(&sid),
        false,
    )
    .err()
    .unwrap();
    assert_eq!(sym_err.code(), "unsafe_path");

    // Symlink session directory rejection.
    let sym_session = rec_root.join("2026-11-26/13-00-01");
    symlink(rec_root.join("2026-11-26/13-00-00"), &sym_session).unwrap();
    assert!(open_manifest(&rec_root, "2026-11-26/13-00-01", None).is_err());
}

#[test]
fn log_store_lists_reads_and_blocks_symlinks_hidden_and_traversal() {
    let tmp = TempVol::new("logs");
    let logs_dir = tmp.paths().logs_dir();
    fs::create_dir_all(logs_dir.join("wifi-watchdog")).unwrap();
    fs::write(
        logs_dir.join("wifi-watchdog/events.ndjson"),
        b"{\"ok\":true}\n",
    )
    .unwrap();
    fs::write(logs_dir.join("system.log"), b"0123456789abcdef").unwrap();
    // Hidden file & symlink MUST be excluded from listing and rejected on open.
    fs::write(logs_dir.join(".secret.log"), b"hidden").unwrap();
    let external = tmp.root.join("external.txt");
    fs::write(&external, b"outside").unwrap();
    symlink(&external, logs_dir.join("linked.log")).unwrap();

    let store = LogStore::new(tmp.paths(), Arc::new(|| true));
    let listed = store
        .list_logs(None, Some(50), Some(new_uuid_v4()))
        .unwrap();
    assert_eq!(listed.schema, LOG_LIST_SCHEMA);
    assert!(!listed.truncated);
    let names: Vec<&str> = listed.items.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(names.len(), 2);
    assert!(names.contains(&"system.log"));
    assert!(names.contains(&"wifi-watchdog/events.ndjson"));

    // Prefix + limit truncation.
    let pref = store
        .list_logs(Some("wifi-watchdog/"), Some(1), None)
        .unwrap();
    assert_eq!(pref.items.len(), 1);
    assert_eq!(pref.items[0].media_type, "application/x-ndjson");

    // Bounded & Range read of regular log.
    let opened = store.open_log("system.log").unwrap();
    assert_eq!(opened.entry.media_type, "text/plain");
    assert_eq!(opened.read_bounded(64).unwrap(), b"0123456789abcdef");
    assert!(opened.read_bounded(4).is_err());
    let (_, etag) = opened.compute_sha256_and_etag().unwrap();
    let (range, slice) = opened
        .read_with_range(Some("bytes=4-7"), Some(&etag), &etag)
        .unwrap();
    assert_eq!(range.unwrap().content_range_header(), "bytes 4-7/16");
    assert_eq!(slice, b"4567");

    // Reject traversal, hidden dot names, leading slash, and symlinks.
    for bad in [
        "../.miro-ego/volume.json",
        ".secret.log",
        "wifi-watchdog/../system.log",
        "/etc/passwd",
        "linked.log",
    ] {
        assert!(store.open_log(bad).is_err(), "expected rejection for {bad}");
    }

    // Unmounted volume returns 503 Unavailable.
    let offline = LogStore::new(tmp.paths(), Arc::new(|| false));
    assert_eq!(
        offline
            .list_logs(None, None, None)
            .unwrap_err()
            .http_status(),
        503
    );
    assert_eq!(
        offline.open_log("system.log").err().unwrap().http_status(),
        503
    );
}

#[test]
fn catalog_utc_sorting_unicode_display_name_schema_rejection_and_rollback_on_write_error() {
    let tmp = TempVol::new("utc-schema-rollback");
    let rec_root = tmp.paths().recordings_dir();

    // Session A: 2026-11-26T20:00:00+08:00 == 12:00:00Z (older in UTC, lexically larger string!)
    // Session B: 2026-11-26T13:00:00Z       == 13:00:00Z (newer in UTC, lexically smaller string!)
    let sid_a = new_uuid_v7();
    let sid_b = new_uuid_v7();
    write_sealed_session(
        &rec_root,
        "2026-11-26/20-00-00",
        &sid_a,
        &new_uuid_v7(),
        "2026-11-26T20:00:00+08:00",
        false,
    );
    write_sealed_session(
        &rec_root,
        "2026-11-26/13-00-00",
        &sid_b,
        &new_uuid_v7(),
        "2026-11-26T13:00:00Z",
        false,
    );
    // Patch sid_b's display_name to 160 multi-byte CJK characters (480 UTF-8 bytes) -> MUST succeed!
    let b_manifest_path = rec_root.join("2026-11-26/13-00-00/manifest.json");
    let mut b_val: serde_json::Value =
        serde_json::from_slice(&fs::read(&b_manifest_path).unwrap()).unwrap();
    b_val["display_name"] = json!("鏡".repeat(160));
    fs::write(&b_manifest_path, serde_json::to_vec_pretty(&b_val).unwrap()).unwrap();

    // Session C: schema-invalid manifest with empty `video.segments: []` -> MUST become a diagnostic!
    let sid_c = new_uuid_v7();
    write_sealed_session(
        &rec_root,
        "2026-11-26/14-00-00",
        &sid_c,
        &new_uuid_v7(),
        "2026-11-26T14:00:00Z",
        false,
    );
    let c_manifest_path = rec_root.join("2026-11-26/14-00-00/manifest.json");
    let mut c_val: serde_json::Value =
        serde_json::from_slice(&fs::read(&c_manifest_path).unwrap()).unwrap();
    c_val["video"]["segments"] = json!([]);
    fs::write(&c_manifest_path, serde_json::to_vec_pretty(&c_val).unwrap()).unwrap();

    // Session D: closed-schema violation (unexpected top-level field) -> MUST become a diagnostic!
    let sid_d = new_uuid_v7();
    write_sealed_session(
        &rec_root,
        "2026-11-26/15-00-00",
        &sid_d,
        &new_uuid_v7(),
        "2026-11-26T15:00:00Z",
        false,
    );
    let d_manifest_path = rec_root.join("2026-11-26/15-00-00/manifest.json");
    let mut d_val: serde_json::Value =
        serde_json::from_slice(&fs::read(&d_manifest_path).unwrap()).unwrap();
    d_val["bogus_extra_field"] = json!(123);
    fs::write(&d_manifest_path, serde_json::to_vec_pretty(&d_val).unwrap()).unwrap();

    let cat = BinarySessionCatalog::open(tmp.paths()).unwrap();
    let page = cat.list_sessions_page(Some(50), None, false, None).unwrap();
    assert_eq!(page.items.len(), 2);
    // True UTC newest-first: sid_b (13:00Z) MUST precede sid_a (20:00+08:00 == 12:00Z)!
    assert_eq!(page.items[0].session_id, sid_b);
    assert_eq!(page.items[0].display_name.chars().count(), 160);
    assert_eq!(page.items[1].session_id, sid_a);
    assert_eq!(page.diagnostics.len(), 2);
    assert!(
        page.diagnostics
            .iter()
            .all(|d| d.code == DiscoveryDiagnosticCode::ManifestInvalid)
    );

    // Commit-after-write guarantee: if `.miro-ego` is missing so `catalog.bin` cannot be written,
    // `remove_session` MUST fail without mutating the in-RAM `CatalogIndex`!
    let rev_before = cat.catalog_revision().unwrap();
    fs::remove_dir_all(tmp.paths().hidden_meta_dir()).unwrap();
    assert!(cat.remove_session(&sid_b).is_err());
    assert!(cat.get_session(&sid_b).unwrap().is_some());
    assert_eq!(cat.catalog_revision().unwrap(), rev_before);
}

#[test]
fn artifact_missing_returns_409_integrity_and_log_truncation_detected() {
    let tmp = TempVol::new("missing-art-trunc-log");
    let rec_root = tmp.paths().recordings_dir();
    let sid = new_uuid_v7();
    let (_, left_sha, _) = write_sealed_session(
        &rec_root,
        "2026-11-26/16-00-00",
        &sid,
        &new_uuid_v7(),
        "2026-11-26T16:00:00Z",
        false,
    );

    // Undeclared artifact_id -> 404 NotFound.
    let unknown_id = "f".repeat(64);
    let err_unknown = open_verified_artifact(
        &rec_root,
        "2026-11-26/16-00-00",
        &unknown_id,
        Some(&sid),
        false,
    )
    .err()
    .unwrap();
    assert_eq!(err_unknown.http_status(), 404);

    // Declared artifact missing on disk -> 409 Integrity `artifact_missing`!
    fs::remove_file(rec_root.join("2026-11-26/16-00-00/video/left_00000.mp4")).unwrap();
    let err_missing = open_verified_artifact(
        &rec_root,
        "2026-11-26/16-00-00",
        &left_sha,
        Some(&sid),
        false,
    )
    .err()
    .unwrap();
    assert_eq!(err_missing.code(), "artifact_missing");
    assert_eq!(err_missing.http_status(), 409);

    // Truncating an opened diagnostic log mid-flight fails closed with 409 `log_truncated`.
    let log_path = tmp.paths().logs_dir().join("truncated.log");
    fs::write(&log_path, b"0123456789abcdef0123456789abcdef").unwrap();
    let store = LogStore::new(tmp.paths(), Arc::new(|| true));
    let opened = store.open_log("truncated.log").unwrap();
    fs::write(&log_path, b"short").unwrap();
    let hash_err = opened.compute_sha256_and_etag().unwrap_err();
    assert_eq!(hash_err.code(), "log_truncated");
    assert_eq!(hash_err.http_status(), 409);
    let read_err = opened.read_bounded(1024).unwrap_err();
    assert_eq!(read_err.code(), "log_truncated");
    assert_eq!(read_err.http_status(), 409);
}
